#include "config.h"
#include "internal.h"
#include "couch_btree.h"
#include "reduces.h"
#include "bitfield.h"
#include "arena.h"
#include "tree_writer.h"
#include "node_types.h"
#include "util.h"

#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>

typedef struct compact_ctx {
    TreeWriter* tree_writer;
    /* Using this for stuff that doesn't need to live longer than it takes to write
     * out a b-tree node (the k/v pairs) */
    arena *transient_arena;
    /* This is for stuff that lasts the duration of the b-tree writing (node pointers) */
    arena *persistent_arena;
    couchfile_modify_result *target_mr;
    Db* target;
    couchstore_compact_hook hook;
    void* hook_ctx;
    couchstore_compact_flags flags;
} compact_ctx;

static couchstore_error_t compact_seq_tree(Db* source, Db* target, compact_ctx *ctx);
static couchstore_error_t compact_localdocs_tree(Db* source, Db* target, compact_ctx *ctx);

couchstore_error_t couchstore_compact_db_ex(Db* source, const char* target_filename,
                                            couchstore_compact_flags flags,
                                            couchstore_compact_hook hook,
                                            void* hook_ctx,
                                            const couch_file_ops *ops)
{
    Db* target = NULL;
    couchstore_error_t errcode;
    compact_ctx ctx = {NULL, new_arena(0), new_arena(0), NULL, NULL, hook, hook_ctx, 0};
    ctx.flags = flags;
    error_unless(ctx.transient_arena && ctx.persistent_arena, COUCHSTORE_ERROR_ALLOC_FAIL);

    error_pass(couchstore_open_db_ex(target_filename, COUCHSTORE_OPEN_FLAG_CREATE, ops, &target));

    ctx.target = target;
    target->file.pos = 1;
    target->header.update_seq = source->header.update_seq;
    if(flags & COUCHSTORE_COMPACT_FLAG_DROP_DELETES) {
        //Count the number of times purge has happened
        target->header.purge_seq = source->header.purge_seq + 1;
    } else {
        target->header.purge_seq = source->header.purge_seq;
    }
    target->header.purge_ptr = source->header.purge_ptr;

    if(source->header.by_seq_root) {
        error_pass(TreeWriterOpen(NULL, ebin_cmp, by_id_reduce, by_id_rereduce, &ctx.tree_writer));
        error_pass(compact_seq_tree(source, target, &ctx));
        error_pass(TreeWriterSort(ctx.tree_writer));
        error_pass(TreeWriterWrite(ctx.tree_writer, &target->file, &target->header.by_id_root));
        TreeWriterFree(ctx.tree_writer);
        ctx.tree_writer = NULL;
    }

    if(source->header.local_docs_root) {
        error_pass(compact_localdocs_tree(source, target, &ctx));
    }
    if(ctx.hook != NULL) {
        error_pass(ctx.hook(ctx.target, NULL, ctx.hook_ctx));
    }
    error_pass(couchstore_commit(target));
cleanup:
    TreeWriterFree(ctx.tree_writer);
    delete_arena(ctx.transient_arena);
    delete_arena(ctx.persistent_arena);
    couchstore_close_db(target);
    if(errcode != COUCHSTORE_SUCCESS && target != NULL) {
        unlink(target_filename);
    }
    return errcode;
}

couchstore_error_t couchstore_compact_db(Db* source, const char* target_filename)
{
    return couchstore_compact_db_ex(source, target_filename, 0, NULL, NULL, couchstore_get_default_file_ops());
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

    error_pass(TreeWriterAddItem(ctx->tree_writer, id_k, id_v));

    if(ctx->target_mr->count == 0) {
        /* No items queued, we must have just flushed. We can safely rewind the transient arena. */
        arena_free_all(ctx->transient_arena);
    }

cleanup:
    return errcode;
}

static couchstore_error_t compact_seq_fetchcb(couchfile_lookup_request *rq, void *k, sized_buf *v)
{
    DocInfo* info = NULL;
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    compact_ctx *ctx = (compact_ctx *) rq->callback_ctx;
    raw_seq_index_value* rawSeq = (raw_seq_index_value*)v->buf;
    uint64_t bpWithDeleted = decode_raw48(rawSeq->bp);
    uint64_t bp = bpWithDeleted & ~BP_DELETED_FLAG;
    if((bpWithDeleted & BP_DELETED_FLAG) &&
       (ctx->hook == NULL) &&
       (ctx->flags & COUCHSTORE_COMPACT_FLAG_DROP_DELETES)) {
        return COUCHSTORE_SUCCESS;
    }

    if(ctx->hook) {
        error_pass(by_seq_read_docinfo(&info, k, v));
        int hook_action = ctx->hook(ctx->target, info, ctx->hook_ctx);
        switch(hook_action) {
            case COUCHSTORE_COMPACT_KEEP_ITEM:
                break;
            case COUCHSTORE_COMPACT_DROP_ITEM:
                goto cleanup;
            default:
                error_pass(hook_action);
        }
    }

    if(bp != 0) {
        cs_off_t new_bp = 0;
        // Copy the document from the old db file to the new one:
        size_t new_size = 0;
        sized_buf item;
        item.buf = NULL;

        int itemsize = pread_bin(rq->file, bp, &item.buf);
        if(itemsize < 0)
        {
            return itemsize;
        }
        item.size = itemsize;

        db_write_buf(ctx->target_mr->rq->file, &item, &new_bp, &new_size);

        bpWithDeleted = (bpWithDeleted & BP_DELETED_FLAG) | new_bp;  //Preserve high bit
        rawSeq->bp = encode_raw48(bpWithDeleted);
        free(item.buf);
    }

    error_pass(output_seqtree_item(k, v, ctx));
cleanup:
    couchstore_free_docinfo(info);
    return errcode;
}

static couchstore_error_t compact_seq_tree(Db* source, Db* target, compact_ctx *ctx)
{
    couchstore_error_t errcode;
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

    ctx->target_mr = new_btree_modres(ctx->persistent_arena, ctx->transient_arena, &target->file,
            &seqcmp, by_seq_reduce, by_seq_rereduce);
    if(ctx->target_mr == NULL) {
        error_pass(COUCHSTORE_ERROR_ALLOC_FAIL);
    }

    srcfold.cmp = seqcmp;
    srcfold.file = &source->file;
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
    arena_free_all(ctx->persistent_arena);
    arena_free_all(ctx->transient_arena);
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
    compare_info idcmp;
    sized_buf tmp;
    idcmp.compare = ebin_cmp;
    idcmp.arg = &tmp;
    couchfile_lookup_request srcfold;

    sized_buf low_key;
    low_key.buf = NULL;
    low_key.size = 0;
    sized_buf *low_key_list = &low_key;

    ctx->target_mr = new_btree_modres(ctx->persistent_arena, NULL, &target->file,
                                      &idcmp, NULL, NULL);
    if(ctx->target_mr == NULL) {
        error_pass(COUCHSTORE_ERROR_ALLOC_FAIL);
    }

    srcfold.cmp = idcmp;
    srcfold.file = &source->file;
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
    arena_free_all(ctx->persistent_arena);
    return errcode;
}

