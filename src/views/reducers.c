/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "../bitfield.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "bitmap.h"
#include "keys.h"
#include "reductions.h"
#include "values.h"
#include "reducers.h"
#include "../couch_btree.h"

couchstore_error_t view_id_btree_reduce(char *dst,
                                        size_t *size_r,
                                        const nodelist *leaflist,
                                        int count,
                                        void *ctx)
{
    view_id_btree_reduction_t *r = NULL;
    uint64_t subtree_count = 0;
    uint16_t j;
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    const nodelist *i;

    (void) ctx;
    r = (view_id_btree_reduction_t *) malloc(sizeof(view_id_btree_reduction_t));
    if (r == NULL) {
        errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto alloc_error;
    }
    memset(&r->partitions_bitmap, 0, sizeof(bitmap_t));

    for (i = leaflist; i != NULL && count > 0; i = i->next, count--) {
        view_id_btree_value_t *v = NULL;
        errcode = decode_view_id_btree_value(i->data.buf, i->data.size, &v);
        if (errcode != COUCHSTORE_SUCCESS) {
            goto alloc_error;
        }
        set_bit(&r->partitions_bitmap, v->partition);
        for (j = 0; j< v->num_view_keys_map; ++j) {
            subtree_count += v->view_keys_map[j].num_keys;
        }
        free_view_id_btree_value(v);
    }
    r->kv_count = subtree_count;
    errcode = encode_view_id_btree_reductions(r, dst, size_r);

alloc_error:
    free_view_id_btree_reductions(r);

    return errcode;
}

couchstore_error_t view_id_btree_rereduce(char *dst,
                                          size_t *size_r,
                                          const nodelist *itmlist,
                                          int count,
                                          void *ctx)
{
    view_id_btree_reduction_t *r = NULL;
    uint64_t subtree_count = 0;
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    const nodelist *i;

    (void) ctx;
    r = (view_id_btree_reduction_t *) malloc(sizeof(view_id_btree_reduction_t));
    if (r == NULL) {
        errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto alloc_error;
    }
    memset(&r->partitions_bitmap, 0, sizeof(bitmap_t));
    for (i = itmlist; i != NULL && count > 0; i = i->next, count--) {
        view_id_btree_reduction_t *r2 = NULL;
        errcode = decode_view_id_btree_reductions(i->pointer->reduce_value.buf, &r2);
        if (errcode != COUCHSTORE_SUCCESS) {
            goto alloc_error;
        }
        union_bitmaps(&r->partitions_bitmap, &r2->partitions_bitmap);
        subtree_count += r2->kv_count;
        free_view_id_btree_reductions(r2);
    }
    r->kv_count = subtree_count;
    errcode = encode_view_id_btree_reductions(r, dst, size_r);

alloc_error:
    free_view_id_btree_reductions(r);

    return errcode;
}
