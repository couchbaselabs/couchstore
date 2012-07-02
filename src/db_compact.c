#include "config.h"
#include "internal.h"
#include "couch_btree.h"
#include "reduces.h"
#include "bitfield.h"
#include "arena.h"
#include "tree_writer.h"
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
} compact_ctx;

static couchstore_error_t compact_seq_tree(Db* source, Db* target, compact_ctx *ctx);
static couchstore_error_t compact_localdocs_tree(Db* source, Db* target, compact_ctx *ctx);

couchstore_error_t couchstore_compact_db_ex(Db* source, const char* target_filename, const couch_file_ops *ops)
{
    Db* target = NULL;
    couchstore_error_t errcode;
    compact_ctx ctx = {NULL, new_arena(0), new_arena(0)};
    error_unless(ctx.transient_arena && ctx.persistent_arena, COUCHSTORE_ERROR_ALLOC_FAIL);

    error_pass(couchstore_open_db_ex(target_filename, COUCHSTORE_OPEN_FLAG_CREATE, ops, &target));

    target->file_pos = 1;
    target->header.update_seq = source->header.update_seq;
    target->header.purge_seq = source->header.purge_seq;
    target->header.purge_ptr = source->header.purge_ptr;

    if(source->header.by_seq_root) {
        error_pass(TreeWriterOpen(NULL, &ctx.tree_writer));
        error_pass(compact_seq_tree(source, target, &ctx));
        error_pass(TreeWriterSort(ctx.tree_writer));
        error_pass(TreeWriterWrite(ctx.tree_writer, target));
        TreeWriterFree(ctx.tree_writer);
        ctx.tree_writer = NULL;
    }

    if(source->header.local_docs_root) {
        error_pass(compact_localdocs_tree(source, target, &ctx));
    }
    error_pass(couchstore_commit(target));
cleanup:
    TreeWriterFree(ctx.tree_writer);
    delete_arena(ctx.transient_arena);
    delete_arena(ctx.persistent_arena);
    couchstore_close_db(target);
    if(errcode != COUCHSTORE_SUCCESS) {
        unlink(target_filename);
    }
    return errcode;
}

couchstore_error_t couchstore_compact_db(Db* source, const char* target_filename)
{
    return couchstore_compact_db_ex(source, target_filename, couch_get_default_file_ops());
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

    //Assembling a ID tree k,v pair. See the file format doc or
    //assemble_id_index_value in couch_db.c
    uint32_t idsize, datasize;
    sized_buf id_k, id_v;
    get_kvlen(v->buf, &idsize, &datasize);
    id_k.buf = v->buf + 16;
    id_k.size = idsize;
    id_v.size = (v->size - (16 + idsize)) + 21;
    id_v.buf = arena_alloc(ctx->transient_arena, id_v.size);
    memset(id_v.buf, 0, id_v.size);
    memcpy(id_v.buf, k->buf, 6);//Copy db seq from seq tree key
    set_bits(id_v.buf + 6, 0, 32, datasize);
    memcpy(id_v.buf + 10, v->buf + 5, 6); //Copy bp and deleted flag from seq tree value
    id_v.buf[16] = v->buf[11]; //Copy content_meta
    memcpy(id_v.buf + 17, v->buf + 12, 4); //Copy rev_seq
    memcpy(id_v.buf + 21, v->buf + 16 + idsize, v->size - 16 - idsize); //Copy rev_meta

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
    compact_ctx *ctx = (compact_ctx *) rq->callback_ctx;
    uint64_t bp = get_48(v->buf + 5) &~ 0x800000000000;
    off_t new_bp = 0;
    size_t new_size = 0;
    sized_buf item;
    item.buf = NULL;
    if(bp != 0) {
        int itemsize = pread_bin(rq->db, bp, &item.buf);
        if(itemsize < 0)
        {
            return itemsize;
        }
        item.size = itemsize;

        db_write_buf(ctx->target_mr->rq->db, &item, &new_bp, &new_size);

        //Preserve high bit
        v->buf[5] &= 0x80;
        memset(v->buf + 6, 0, 5);
        set_bits(v->buf + 5, 1, 47, new_bp);
        free(item.buf);
    }

    return output_seqtree_item(k, v, ctx);
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

    errcode = btree_lookup(&srcfold, source->header.local_docs_root->pointer);
    if(errcode == COUCHSTORE_SUCCESS) {
        target->header.local_docs_root = complete_new_btree(ctx->target_mr, &errcode);
    }
cleanup:
    arena_free_all(ctx->persistent_arena);
    return errcode;
}

