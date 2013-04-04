/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "reductions.h"
#include "../bitfield.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <snappy-c.h>

#define BITMASK_BYTE_SIZE      (1024 / CHAR_BIT)

#define dec_uint16(b) (decode_raw16(*((raw_16 *) b)))
#define dec_uint48(b) (decode_raw48(*((raw_48 *) b)))
#define dec_uint40(b) (decode_raw40(*((raw_40 *) b)))

static void enc_uint16(uint16_t u, char **buf);
static void enc_uint48(uint64_t u, char **buf);

static void enc_raw40(uint64_t u, char **buf);


LIBCOUCHSTORE_API
couchstore_error_t decode_view_btree_reductions(const char *bytes, size_t len,
                                                view_btree_reduction_t **reduction)
{
    view_btree_reduction_t *r = NULL;
    uint8_t  i, j;
    uint16_t sz;
    char    *bs;
    size_t length;

    r = (view_btree_reduction_t *) malloc(sizeof(view_btree_reduction_t));
    if (r == NULL) {
        goto alloc_error;
    }

    r->reduce_values = NULL;

    r->kv_count = dec_uint40(bytes);
    bytes += 5;
    len -= 5;

    memcpy(&r->partitions_bitmap, bytes, BITMASK_BYTE_SIZE);
    bytes += BITMASK_BYTE_SIZE;
    len -= BITMASK_BYTE_SIZE;


    bs = bytes;
    length = len;

    i = 0;
    while (len > 0) {

        sz = dec_uint16(bs);
        bs += 2;
        len -= 2;

        bs += sz;
        len -= sz;
        i++;
    }

    r->num_values = i;

    if (len > 0) {
        free_view_btree_reductions(r);
        return COUCHSTORE_ERROR_CORRUPT;
    }

    r->reduce_values = (sized_buf *) malloc(i * sizeof(sized_buf));

    if (r->reduce_values == NULL) {
        goto alloc_error;
    }

    for (j = 0; j< i; ++j) {
        r->reduce_values[j].size = 0;
        r->reduce_values[j].buf = NULL;
    }

    i = 0;
    len = length;
    while (len > 0) {

        sz = dec_uint16(bytes);
        bytes += 2;
        len -= 2;

        r->reduce_values[i].size = sz;
        r->reduce_values[i].buf = (char *) malloc(sz * sizeof(char));

        if (r->reduce_values[i].buf == NULL) {
            goto alloc_error;
        }

        memcpy(r->reduce_values[i].buf, bytes, sz);
        bytes += sz;
        len -= sz;
        i++;
    }

    *reduction = r;

    return COUCHSTORE_SUCCESS;

 alloc_error:
    free_view_btree_reductions(r);
    return COUCHSTORE_ERROR_ALLOC_FAIL;
}


LIBCOUCHSTORE_API
couchstore_error_t encode_view_btree_reductions(const view_btree_reduction_t *reduction,
                                       char **buffer,
                                       size_t *buffer_size)
{
    char *buf = NULL, *b = NULL;
    size_t sz = 0;

    sz += 5;                     /* kv_count */
    sz += BITMASK_BYTE_SIZE; /* partitions bitmap */
    /* reduce values */
    for (int i = 0; i < reduction->num_values; ++i) {
        sz += 2;             /* size_t */
        sz += reduction->reduce_values[i].size;
    }

    b = buf = (char *) malloc(sz);
    if (buf == NULL) {
        goto alloc_error;
    }

    enc_raw40(reduction->kv_count, &b);

    memcpy(b, &reduction->partitions_bitmap, BITMASK_BYTE_SIZE);
    b += BITMASK_BYTE_SIZE;

    for (int i = 0; i < reduction->num_values; ++i) {
        enc_uint16(reduction->reduce_values[i].size, &b);

        memcpy(b, reduction->reduce_values[i].buf, reduction->reduce_values[i].size);
        b += reduction->reduce_values[i].size;
    }

    *buffer = buf;
    *buffer_size = sz;

    return COUCHSTORE_SUCCESS;

 alloc_error:
    free(buf);
    *buffer = NULL;
    *buffer_size = 0;
    return COUCHSTORE_ERROR_ALLOC_FAIL;
}


LIBCOUCHSTORE_API
void free_view_btree_reductions(view_btree_reduction_t *reduction)
{
    if (reduction == NULL) {
        return;
    }

    if (reduction->reduce_values != NULL){
        for (int i = 0; i < reduction->num_values; ++i) {
            if (reduction->reduce_values[i].buf != NULL) {
                free(reduction->reduce_values[i].buf);
            }
        }
        free(reduction->reduce_values);
    }

    free(reduction);
}

LIBCOUCHSTORE_API
couchstore_error_t decode_view_id_btree_reductions(const char *bytes,
                                             view_id_btree_reduction_t **reduction)
{
    view_id_btree_reduction_t *r = NULL;


    r = (view_id_btree_reduction_t *) malloc(sizeof(view_id_btree_reduction_t));
    if (r == NULL) {
        goto alloc_error;
    }

    r->kv_count = dec_uint40(bytes);
    bytes += 5;

    memcpy(&r->partitions_bitmap, bytes, BITMASK_BYTE_SIZE);

    *reduction = r;

    return COUCHSTORE_SUCCESS;

 alloc_error:
    free_view_id_btree_reductions(r);
    return COUCHSTORE_ERROR_ALLOC_FAIL;
}


LIBCOUCHSTORE_API
couchstore_error_t encode_view_id_btree_reductions(const view_id_btree_reduction_t *reduction,
                                       char **buffer,
                                       size_t *buffer_size)
{
    char *buf = NULL, *b = NULL;
    size_t sz = 0;

    sz += 5;                     /* kv_count */
    sz += BITMASK_BYTE_SIZE; /* partitions bitmap */

    b = buf = (char *) malloc(sz);
    if (buf == NULL) {
        goto alloc_error;
    }

    enc_raw40(reduction->kv_count, &b);

    memcpy(b, &reduction->partitions_bitmap, BITMASK_BYTE_SIZE);

    *buffer = buf;
    *buffer_size = sz;

    return COUCHSTORE_SUCCESS;

 alloc_error:
    free(buf);
    *buffer = NULL;
    *buffer_size = 0;
    return COUCHSTORE_ERROR_ALLOC_FAIL;
}


LIBCOUCHSTORE_API
void free_view_id_btree_reductions(view_id_btree_reduction_t *reduction)
{
    if (reduction == NULL) {
        return;
    }

    free(reduction);
}

static void enc_uint16(uint16_t u, char **buf)
{
    raw_16 r = encode_raw16(u);
    memcpy(*buf, &r, 2);
    *buf += 2;
}


static void enc_uint48(uint64_t u, char **buf)
{
    raw_48 r = encode_raw48(u);
    memcpy(*buf, &r, 6);
    *buf += 6;
}

static void enc_raw40(uint64_t u, char **buf)
{
    raw_40 r = encode_raw40(u);
    memcpy(*buf, &r, 5);
    *buf += 5;
}

