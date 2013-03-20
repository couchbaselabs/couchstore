#include "config.h"
#include "internal.h"
#include "couch_btree.h"
#include "reduces.h"
#include "bitfield.h"
#include "arena.h"
#include "mergesort.h"
#include "node_types.h"
#include "util.h"

#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>

#define ID_SORT_CHUNK_SIZE (100 * 1024 * 1024) // 100MB. Make tuneable?
#define ID_SORT_MAX_RECORD_SIZE 4196

typedef struct extsort_record {
    sized_buf k;
    sized_buf v;
    char buf[1];
} extsort_record;

typedef struct compact_ctx {
    FILE* id_tmp;
    /* Using this for stuff that doesn't need to live longer than it takes to write
     * out a b-tree node (the k/v pairs) */
    arena *transient_arena;
    const arena_position *transient_zero;
    /* This is for stuff that lasts the duration of the b-tree writing (node pointers) */
    arena *persistent_arena;
    couchfile_modify_result *target_mr;
    couchstore_compact_flags flags;
} compact_ctx;

static couchstore_error_t compact_seq_tree(Db* source, Db* target, compact_ctx *ctx);
static couchstore_error_t compact_localdocs_tree(Db* source, Db* target, compact_ctx *ctx);
static couchstore_error_t write_id_tree(Db* target, compact_ctx *ctx);

couchstore_error_t couchstore_compact_db_ex(Db* source, const char* target_filename,
                                            couchstore_compact_flags flags,
                                            const couch_file_ops *ops)
{
    Db* target = NULL;
    couchstore_error_t errcode;
    compact_ctx ctx;
    ctx.id_tmp = NULL;
    ctx.flags = flags;

    error_pass(couchstore_open_db_ex(target_filename, COUCHSTORE_OPEN_FLAG_CREATE, ops, &target));

    target->file_pos = 1;
    target->header.update_seq = source->header.update_seq;
    if(flags & COUCHSTORE_COMPACT_FLAG_DROP_DELETES) {
        //Count the number of times purge has happened
        target->header.purge_seq = source->header.purge_seq + 1;
    } else {
        target->header.purge_seq = source->header.purge_seq;
    }
    target->header.purge_ptr = source->header.purge_ptr;

    if(source->header.by_seq_root) {
        ctx.id_tmp = tmpfile();
        if(!ctx.id_tmp) {
            error_pass(COUCHSTORE_ERROR_OPEN_FILE);
        }
        error_pass(compact_seq_tree(source, target, &ctx));
        error_pass(write_id_tree(target, &ctx));
        fclose(ctx.id_tmp);
        ctx.id_tmp = NULL;
    }

    if(source->header.local_docs_root) {
        error_pass(compact_localdocs_tree(source, target, &ctx));
    }
    error_pass(couchstore_commit(target));
cleanup:
    if(ctx.id_tmp) {
        fclose(ctx.id_tmp);
    }
    couchstore_close_db(target);
    if(errcode != COUCHSTORE_SUCCESS && target != NULL) {
        unlink(target_filename);
    }
    return errcode;
}

couchstore_error_t couchstore_compact_db(Db* source, const char* target_filename)
{
    return couchstore_compact_db_ex(source, target_filename, 0, couchstore_get_default_file_ops());
}

static int read_id_record(FILE *in, void *buf, void *ctx)
{
    (void) ctx;
    uint16_t klen;
    uint32_t vlen;
    extsort_record *rec = (extsort_record *) buf;
    if(fread(&klen, 2, 1, in) != 1) {
        return 0;
    }
    if(fread(&vlen, 4, 1, in) != 1) {
        return 0;
    }
    if(fread(rec->buf, klen, 1, in) != 1) {
        return 0;
    }
    if(fread(rec->buf + klen, vlen, 1, in) != 1) {
        return 0;
    }
    rec->k.size = klen;
    rec->k.buf = NULL;
    rec->v.size = vlen;
    rec->v.buf = NULL;
    return sizeof(extsort_record) + klen + vlen;
}

static int write_id_record(FILE *out, void *ptr, void *ctx)
{
    (void) ctx;
    extsort_record *rec = (extsort_record *) ptr;
    uint16_t klen = (uint16_t) rec->k.size;
    uint32_t vlen = (uint32_t) rec->v.size;
    if(fwrite(&klen, 2, 1, out) != 1) {
        return 0;
    }
    if(fwrite(&vlen, 4, 1, out) != 1) {
        return 0;
    }
    if(fwrite(rec->buf, vlen + klen, 1, out) != 1) {
        return 0;
    }
    return 1;
}

static int compare_id_record(void* r1, void* r2, void *ctx)
{
    (void) ctx;
    extsort_record *e1 = (extsort_record *) r1, *e2 = (extsort_record *) r2;
    e1->k.buf = e1->buf;
    e1->v.buf = e1->buf + e1->k.size;
    e2->k.buf = e2->buf;
    e2->v.buf = e2->buf + e2->k.size;
    return ebin_cmp(&e1->k, &e2->k);
}

static couchstore_error_t write_id_tree(Db* target, compact_ctx *ctx)
{
    int readerr = 0;
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    ctx->transient_arena = new_arena(0);
    ctx->persistent_arena = new_arena(0);
    ctx->transient_zero = arena_mark(ctx->transient_arena);
    rewind(ctx->id_tmp);
    error_pass(merge_sort(ctx->id_tmp, ctx->id_tmp, read_id_record, write_id_record, compare_id_record,
                          NULL, ID_SORT_MAX_RECORD_SIZE, ID_SORT_CHUNK_SIZE, NULL));

    rewind(ctx->id_tmp);

    compare_info idcmp;
    sized_buf tmp;
    idcmp.compare = ebin_cmp;
    idcmp.arg = &tmp;

    ctx->target_mr = new_btree_modres(ctx->persistent_arena, ctx->transient_arena, target,
            &idcmp, by_id_reduce, by_id_rereduce);
    if(ctx->target_mr == NULL) {
        error_pass(COUCHSTORE_ERROR_ALLOC_FAIL);
    }

    uint16_t klen;
    uint32_t vlen;
    sized_buf k, v;
    while(1) {
        if(fread(&klen, 2, 1, ctx->id_tmp) != 1) {
            break;
        }
        if(fread(&vlen, 4, 1, ctx->id_tmp) != 1) {
            break;
        }
        k.size = klen;
        k.buf = arena_alloc(ctx->transient_arena, klen);
        v.size = vlen;
        v.buf = arena_alloc(ctx->transient_arena, vlen);
        if(fread(k.buf, klen, 1, ctx->id_tmp) != 1) {
            error_pass(COUCHSTORE_ERROR_READ);
        }
        if(fread(v.buf, vlen, 1, ctx->id_tmp) != 1) {
            error_pass(COUCHSTORE_ERROR_READ);
        }
        //printf("K: '%.*s'\n", klen, k.buf);
        mr_push_item(&k, &v, ctx->target_mr);
        if(ctx->target_mr->count == 0) {
            /* No items queued, we must have just flushed. We can safely rewind the transient arena. */
            arena_free_from_mark(ctx->transient_arena, ctx->transient_zero);
        }
    }
    readerr = ferror(ctx->id_tmp);
    if(readerr != 0 && readerr != EOF) {
        error_pass(COUCHSTORE_ERROR_READ);
    }

    target->header.by_id_root = complete_new_btree(ctx->target_mr, &errcode);
cleanup:
    delete_arena(ctx->transient_arena);
    delete_arena(ctx->persistent_arena);
    return errcode;
}

//Copy buffer to arena
static sized_buf* arena_copy_buf(arena* a, sized_buf *src)
{
    sized_buf *nbuf = arena_alloc(a, sizeof(sized_buf));
    if(nbuf == NULL) {
        return NULL;
    }
    nbuf->buf = arena_alloc(a, src->size);
    if(nbuf->buf == NULL) {
        return NULL;
    }
    nbuf->size = src->size;
    memcpy(nbuf->buf, src->buf, src->size);
    return nbuf;
}

static couchstore_error_t output_seqtree_item(sized_buf* k, sized_buf *v, compact_ctx *ctx)
{
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    sized_buf *k_c = arena_copy_buf(ctx->transient_arena, k);
    if(k_c == NULL) {
        error_pass(COUCHSTORE_ERROR_READ);
    }
    sized_buf *v_c = arena_copy_buf(ctx->transient_arena, v);
    if(v_c == NULL) {
        error_pass(COUCHSTORE_ERROR_READ);
    }

    error_pass(mr_push_item(k_c, v_c, ctx->target_mr));
    
    // Decode the by-sequence index value. See the file format doc or
    // assemble_id_index_value in couch_db.c:
    const raw_seq_index_value* rawSeq = (const raw_seq_index_value*)v->buf;
    uint32_t idsize, datasize;
    decode_kv_length(&rawSeq->sizes, &idsize, &datasize);
    uint32_t revMetaSize = (uint32_t)v->size - (sizeof(raw_seq_index_value) + idsize);
    
    // Set up sized_bufs for the ID tree key and value:
    sized_buf id_k, id_v;
    id_k.buf = (char*)(rawSeq + 1);
    id_k.size = idsize;
    id_v.size = sizeof(raw_id_index_value) + revMetaSize;
    id_v.buf = arena_alloc(ctx->transient_arena, id_v.size);

    raw_id_index_value *raw = (raw_id_index_value*)id_v.buf;
    raw->db_seq = *(raw_48*)k->buf;  //Copy db seq from seq tree key
    raw->size = encode_raw32(datasize);
    raw->bp = rawSeq->bp;
    raw->content_meta = rawSeq->content_meta;
    raw->rev_seq = rawSeq->rev_seq;
    memcpy(raw + 1, (uint8_t*)(rawSeq + 1) + idsize, revMetaSize); //Copy rev_meta

    uint16_t klen = (uint16_t) id_k.size;
    uint32_t vlen = (uint32_t) id_v.size;
    error_unless(fwrite(&klen, 2, 1, ctx->id_tmp) == 1, COUCHSTORE_ERROR_WRITE);
    error_unless(fwrite(&vlen, 4, 1, ctx->id_tmp) == 1, COUCHSTORE_ERROR_WRITE);
    error_unless(fwrite(id_k.buf, id_k.size, 1, ctx->id_tmp) == 1, COUCHSTORE_ERROR_WRITE);
    error_unless(fwrite(id_v.buf, id_v.size, 1, ctx->id_tmp) == 1, COUCHSTORE_ERROR_WRITE);

    if(ctx->target_mr->count == 0) {
        /* No items queued, we must have just flushed. We can safely rewind the transient arena. */
        arena_free_from_mark(ctx->transient_arena, ctx->transient_zero);
    }

cleanup:
    return errcode;
}

static couchstore_error_t compact_seq_fetchcb(couchfile_lookup_request *rq, void *k, sized_buf *v)
{
    compact_ctx *ctx = (compact_ctx *) rq->callback_ctx;
    raw_seq_index_value* rawSeq = (raw_seq_index_value*)v->buf;
    uint64_t bpWithDeleted = decode_raw48(rawSeq->bp);
    uint64_t bp = bpWithDeleted & ~BP_DELETED_FLAG;
    if((bpWithDeleted & BP_DELETED_FLAG) &&
       (ctx->flags & COUCHSTORE_COMPACT_FLAG_DROP_DELETES)) {
        return COUCHSTORE_SUCCESS;
    }

    if(bp != 0) {
        cs_off_t new_bp = 0;
        // Copy the document from the old db file to the new one:
        size_t new_size = 0;
        sized_buf item;
        item.buf = NULL;

        int itemsize = pread_bin(rq->db, bp, &item.buf);
        if(itemsize < 0)
        {
            return itemsize;
        }
        item.size = itemsize;

        db_write_buf(ctx->target_mr->rq->db, &item, &new_bp, &new_size);

        bpWithDeleted = (bpWithDeleted & BP_DELETED_FLAG) | new_bp;  //Preserve high bit
        rawSeq->bp = encode_raw48(bpWithDeleted);
        free(item.buf);
    }

    return output_seqtree_item(k, v, ctx);
}

static couchstore_error_t compact_seq_tree(Db* source, Db* target, compact_ctx *ctx)
{
    couchstore_error_t errcode;
    ctx->transient_arena = new_arena(0);
    ctx->persistent_arena = new_arena(0);
    error_unless(ctx->transient_arena, COUCHSTORE_ERROR_ALLOC_FAIL);
    error_unless(ctx->persistent_arena, COUCHSTORE_ERROR_ALLOC_FAIL);
    ctx->transient_zero = arena_mark(ctx->transient_arena);
    compare_info seqcmp;
    sized_buf tmp;
    seqcmp.compare = seq_cmp;
    seqcmp.arg = &tmp;
    couchfile_lookup_request srcfold;
    sized_buf low_key;
    //Keys in seq tree are 48-bit numbers, this is 0, lowest possible key
    low_key.buf = "\0\0\0\0\0\0";
    low_key.size = 6;
    sized_buf *low_key_list = &low_key;

    ctx->target_mr = new_btree_modres(ctx->persistent_arena, ctx->transient_arena, target,
            &seqcmp, by_seq_reduce, by_seq_rereduce);
    if(ctx->target_mr == NULL) {
        error_pass(COUCHSTORE_ERROR_ALLOC_FAIL);
    }

    srcfold.cmp = seqcmp;
    srcfold.db = source;
    srcfold.num_keys = 1;
    srcfold.keys = &low_key_list;
    srcfold.fold = 1;
    srcfold.in_fold = 1;
    srcfold.callback_ctx = ctx;
    srcfold.fetch_callback = compact_seq_fetchcb;
    srcfold.node_callback = NULL;

    errcode = btree_lookup(&srcfold, source->header.by_seq_root->pointer);
    if(errcode == COUCHSTORE_SUCCESS) {
        target->header.by_seq_root = complete_new_btree(ctx->target_mr, &errcode);
    }
cleanup:
    delete_arena(ctx->persistent_arena);
    delete_arena(ctx->transient_arena);
    return errcode;
}

static couchstore_error_t compact_localdocs_fetchcb(couchfile_lookup_request *rq, void *k, sized_buf *v)
{
    compact_ctx *ctx = (compact_ctx *) rq->callback_ctx;
    //printf("V: '%.*s'\n", v->size, v->buf);
    return mr_push_item(arena_copy_buf(ctx->persistent_arena, k),
            arena_copy_buf(ctx->persistent_arena, v), ctx->target_mr);
}

static couchstore_error_t compact_localdocs_tree(Db* source, Db* target, compact_ctx *ctx)
{
    couchstore_error_t errcode;
    ctx->persistent_arena = new_arena(0);
    error_unless(ctx->persistent_arena, COUCHSTORE_ERROR_ALLOC_FAIL);
    compare_info idcmp;
    sized_buf tmp;
    idcmp.compare = ebin_cmp;
    idcmp.arg = &tmp;
    couchfile_lookup_request srcfold;

    sized_buf low_key;
    low_key.buf = NULL;
    low_key.size = 0;
    sized_buf *low_key_list = &low_key;

    ctx->target_mr = new_btree_modres(ctx->persistent_arena, NULL, target,
            &idcmp, NULL, NULL);
    if(ctx->target_mr == NULL) {
        error_pass(COUCHSTORE_ERROR_ALLOC_FAIL);
    }

    srcfold.cmp = idcmp;
    srcfold.db = source;
    srcfold.num_keys = 1;
    srcfold.keys = &low_key_list;
    srcfold.fold = 1;
    srcfold.in_fold = 1;
    srcfold.callback_ctx = ctx;
    srcfold.fetch_callback = compact_localdocs_fetchcb;
    srcfold.node_callback = NULL;

    errcode = btree_lookup(&srcfold, source->header.local_docs_root->pointer);
    if(errcode == COUCHSTORE_SUCCESS) {
        target->header.local_docs_root = complete_new_btree(ctx->target_mr, &errcode);
    }
cleanup:
    delete_arena(ctx->persistent_arena);
    return errcode;
}

