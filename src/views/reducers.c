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

#define DOUBLE_FMT "%.15lg"
#define scan_stats(buf, sum, count, min, max, sumsqr) \
        sscanf(buf, "{\"sum\":%lg,\"count\":%llu,\"min\":%lg,\"max\":%lg,\"sumsqr\":%lg}",\
               &sum, &count, &min, &max, &sumsqr)

#define sprint_stats(buf, sum, count, min, max, sumsqr) \
        sprintf(buf, "{\"sum\":%lg,\"count\":%llu,\"min\":%lg,\"max\":%lg,\"sumsqr\":%lg}",\
                sum, count, min, max, sumsqr)

static int buf_to_str(const sized_buf *buf, char str[32])
{
    if (buf->size < 1 || buf->size > 31) {
        return 0;
    }
    memcpy(str, buf->buf, buf->size);
    str[buf->size] = '\0';

    return 1;
}

static int buf_to_double(const sized_buf *buf, double *out_num)
{
    char str[32];
    char *end;

    if (!(buf_to_str(buf, str))) {
        return 0;
    }
    *out_num = strtod(str, &end);

    return ((end > str) && (*end == '\0'));
}

static int buf_to_uint64(const sized_buf *buf, uint64_t *out_num)
{
    char str[32];
    char *end;

    if (!buf_to_str(buf, str)) {
        return 0;
    }
    *out_num = strtoull(str, &end, 10);

    return ((end > str) && (*end == '\0'));
}

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
    errcode = encode_view_id_btree_reduction(r, dst, size_r);

alloc_error:
    free_view_id_btree_reduction(r);

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
        errcode = decode_view_id_btree_reduction(i->pointer->reduce_value.buf, &r2);
        if (errcode != COUCHSTORE_SUCCESS) {
            goto alloc_error;
        }
        union_bitmaps(&r->partitions_bitmap, &r2->partitions_bitmap);
        subtree_count += r2->kv_count;
        free_view_id_btree_reduction(r2);
    }
    r->kv_count = subtree_count;
    errcode = encode_view_id_btree_reduction(r, dst, size_r);

alloc_error:
    free_view_id_btree_reduction(r);

    return errcode;
}

couchstore_error_t view_btree_sum_reduce(char *dst,
                                         size_t *size_r,
                                         const nodelist *leaflist,
                                         int count,
                                         void *ctx)
{
    view_btree_reduction_t *r = NULL;
    uint64_t subtree_count = 0;
    uint16_t j;
    double n, sum;
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    char buf[MAX_REDUCTION_SIZE];
    size_t size = 0;
    const nodelist *i;
    view_reducer_ctx_t *errctx;
    char *doc_id;

    r = (view_btree_reduction_t *) malloc(sizeof(view_btree_reduction_t));
    if (r == NULL) {
        errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto alloc_error;
    }
    r->reduce_values = NULL;
    memset(&r->partitions_bitmap, 0, sizeof(bitmap_t));

    sum = 0;
    for (i = leaflist; i != NULL && count > 0; i = i->next, count--) {
        view_btree_value_t *v = NULL;
        errcode = decode_view_btree_value(i->data.buf, i->data.size, &v);
        if (errcode != COUCHSTORE_SUCCESS) {
            goto alloc_error;
        }
        set_bit(&r->partitions_bitmap, v->partition);
        subtree_count += v->num_values;

        for (j = 0; j< v->num_values; ++j) {
            if (buf_to_double(&(v->values[j]), &n)) {
                sum += n;
            } else {
                view_btree_key_t *k = NULL;
                errcode = decode_view_btree_key(i->key.buf, i->key.size, &k);
                if (errcode != COUCHSTORE_SUCCESS) {
                    goto alloc_error;
                }
                errctx = (view_reducer_ctx_t *) ctx;
                doc_id = (char *) malloc(k->doc_id.size);
                if (doc_id == NULL) {
                    errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
                    free_view_btree_key(k);
                    free_view_btree_value(v);
                    goto alloc_error;
                }
                memcpy(doc_id, k->doc_id.buf, k->doc_id.size);
                errctx->error_doc_id = (const char *) doc_id;
                errctx->error = VIEW_REDUCER_ERROR_NOT_A_NUMBER;
                free_view_btree_key(k);
                free_view_btree_value(v);
                errcode = COUCHSTORE_ERROR_REDUCER_FAILURE;
                goto alloc_error;
            }
        }
        free_view_btree_value(v);
    }

    size = sprintf(buf, DOUBLE_FMT, sum);
    assert(size > 0);
    r->kv_count = subtree_count;
    r->num_values = 1;
    r->reduce_values = (sized_buf *) malloc(r->num_values * sizeof(sized_buf));
    if (r->reduce_values == NULL) {
        errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto alloc_error;
    }

    r->reduce_values[0].size = size;
    r->reduce_values[0].buf = (char *) malloc(size);
    if (r->reduce_values[0].buf == NULL) {
        errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto alloc_error;
    }
    memcpy(r->reduce_values[0].buf, buf, size);
    errcode = encode_view_btree_reduction(r, dst, size_r);

alloc_error:
    free_view_btree_reduction(r);

    return errcode;
}

couchstore_error_t view_btree_sum_rereduce(char *dst,
                                           size_t *size_r,
                                           const nodelist *itmlist,
                                           int count,
                                           void *ctx)
{
    view_btree_reduction_t *r = NULL;
    uint64_t subtree_count = 0;
    uint16_t j;
    double n, sum;
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    char buf[MAX_REDUCTION_SIZE];
    size_t size = 0;
    const nodelist *i;
    view_reducer_ctx_t *errctx;
    char *doc_id;

    r = (view_btree_reduction_t *) malloc(sizeof(view_btree_reduction_t));
    if (r == NULL) {
        errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto alloc_error;
    }
    r->reduce_values = NULL;
    memset(&r->partitions_bitmap, 0, sizeof(bitmap_t));

    sum = 0;
    for (i = itmlist; i != NULL && count > 0; i = i->next, count--) {
        view_btree_reduction_t *r2 = NULL;
        errcode = decode_view_btree_reduction(i->pointer->reduce_value.buf,
                                              i->pointer->reduce_value.size, &r2);
        if (errcode != COUCHSTORE_SUCCESS) {
            goto alloc_error;
        }
        union_bitmaps(&r->partitions_bitmap, &r2->partitions_bitmap);
        subtree_count += r2->kv_count;
        for (j = 0; j< r2->num_values; ++j) {
            if (buf_to_double(&(r2->reduce_values[j]), &n)) {
                sum += n;
            } else {
                view_btree_key_t *k = NULL;
                errcode = decode_view_btree_key(i->key.buf, i->key.size, &k);
                if (errcode != COUCHSTORE_SUCCESS) {
                    goto alloc_error;
                }
                errctx = (view_reducer_ctx_t *) ctx;
                doc_id = (char *) malloc(k->doc_id.size);
                if (doc_id == NULL) {
                    errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
                    free_view_btree_key(k);
                    free_view_btree_reduction(r2);
                    goto alloc_error;
                }
                memcpy(doc_id, k->doc_id.buf, k->doc_id.size);
                errctx->error_doc_id = (const char *) doc_id;
                errctx->error = VIEW_REDUCER_ERROR_NOT_A_NUMBER;
                free_view_btree_key(k);
                free_view_btree_reduction(r2);
                errcode = COUCHSTORE_ERROR_REDUCER_FAILURE;
                goto alloc_error;
            }
        }
        free_view_btree_reduction(r2);
    }
    size = sprintf(buf, DOUBLE_FMT, sum);
    assert(size > 0);
    r->kv_count = subtree_count;
    r->num_values = 1;
    r->reduce_values = (sized_buf *) malloc(r->num_values * sizeof(sized_buf));
    if (r->reduce_values == NULL) {
        errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto alloc_error;
    }

    r->reduce_values[0].size = size;
    r->reduce_values[0].buf = (char *) malloc(size);
    if (r->reduce_values[0].buf == NULL) {
        errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto alloc_error;
    }
    memcpy(r->reduce_values[0].buf, buf, size);
    errcode = encode_view_btree_reduction(r, dst, size_r);

alloc_error:
    free_view_btree_reduction(r);

    return errcode;
}

couchstore_error_t view_btree_count_reduce(char *dst,
                                           size_t *size_r,
                                           const nodelist *leaflist,
                                           int count,
                                           void *ctx)
{
    view_btree_reduction_t *r = NULL;
    uint64_t subtree_count = 0;
    uint64_t cnt;
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    char buf[MAX_REDUCTION_SIZE];
    size_t size = 0;
    const nodelist *i;

    (void) ctx;
    r = (view_btree_reduction_t *) malloc(sizeof(view_btree_reduction_t));
    if (r == NULL) {
        errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto alloc_error;
    }
    r->reduce_values = NULL;
    memset(&r->partitions_bitmap, 0, sizeof(bitmap_t));

    cnt = 0;
    for (i = leaflist; i != NULL && count > 0; i = i->next, count--) {
        view_btree_value_t *v = NULL;
        errcode = decode_view_btree_value(i->data.buf, i->data.size, &v);
        if (errcode != COUCHSTORE_SUCCESS) {
            goto alloc_error;
        }
        set_bit(&r->partitions_bitmap, v->partition);
        subtree_count += v->num_values;
        cnt += v->num_values;
        free_view_btree_value(v);
    }

    size = sprintf(buf, "%llu", (unsigned long long) cnt);
    assert(size > 0);
    r->kv_count = subtree_count;
    r->num_values = 1;
    r->reduce_values = (sized_buf *) malloc(r->num_values * sizeof(sized_buf));
    if (r->reduce_values == NULL) {
        errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto alloc_error;
    }

    r->reduce_values[0].size = size;
    r->reduce_values[0].buf = (char *) malloc(size);
    if (r->reduce_values[0].buf == NULL) {
        errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto alloc_error;
    }
    memcpy(r->reduce_values[0].buf, buf, size);
    errcode = encode_view_btree_reduction(r, dst, size_r);

alloc_error:
    free_view_btree_reduction(r);

    return errcode;
}

couchstore_error_t view_btree_count_rereduce(char *dst,
                                             size_t *size_r,
                                             const nodelist *itmlist,
                                             int count,
                                             void *ctx)
{
    view_btree_reduction_t *r = NULL;
    uint64_t subtree_count = 0;
    uint16_t j;
    uint64_t n, cnt;
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    char buf[MAX_REDUCTION_SIZE];
    size_t size = 0;
    const nodelist *i;
    view_reducer_ctx_t *errctx;
    char *doc_id;

    r = (view_btree_reduction_t *) malloc(sizeof(view_btree_reduction_t));
    if (r == NULL) {
        errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto alloc_error;
    }
    r->reduce_values = NULL;
    memset(&r->partitions_bitmap, 0, sizeof(bitmap_t));

    cnt = 0;
    for (i = itmlist; i != NULL && count > 0; i = i->next, count--) {
        view_btree_reduction_t *r2 = NULL;
        errcode = decode_view_btree_reduction(i->pointer->reduce_value.buf,
                                              i->pointer->reduce_value.size, &r2);
        if (errcode != COUCHSTORE_SUCCESS) {
            goto alloc_error;
        }
        union_bitmaps(&r->partitions_bitmap, &r2->partitions_bitmap);
        subtree_count += r2->kv_count;
        for (j = 0; j< r2->num_values; ++j) {
            if (buf_to_uint64(&(r2->reduce_values[j]), &n)) {
                cnt += n;
            } else {
                view_btree_key_t *k = NULL;
                errcode = decode_view_btree_key(i->key.buf, i->key.size, &k);
                if (errcode != COUCHSTORE_SUCCESS) {
                    goto alloc_error;
                }
                errctx = (view_reducer_ctx_t *) ctx;
                doc_id = (char *) malloc(k->doc_id.size);
                if (doc_id == NULL) {
                    errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
                    free_view_btree_key(k);
                    free_view_btree_reduction(r2);
                    goto alloc_error;
                }
                memcpy(doc_id, k->doc_id.buf, k->doc_id.size);
                errctx->error_doc_id = (const char *) doc_id;
                errctx->error = VIEW_REDUCER_ERROR_NOT_A_NUMBER;
                free_view_btree_key(k);
                free_view_btree_reduction(r2);
                errcode = COUCHSTORE_ERROR_REDUCER_FAILURE;
                goto alloc_error;
            }
        }
        free_view_btree_reduction(r2);
    }
    size = sprintf(buf, "%llu", (unsigned long long) cnt);
    assert(size > 0);
    r->kv_count = subtree_count;
    r->num_values = 1;
    r->reduce_values = (sized_buf *) malloc(r->num_values * sizeof(sized_buf));
    if (r->reduce_values == NULL) {
        errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto alloc_error;
    }

    r->reduce_values[0].size = size;
    r->reduce_values[0].buf = (char *) malloc(size);
    if (r->reduce_values[0].buf == NULL) {
        errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto alloc_error;
    }
    memcpy(r->reduce_values[0].buf, buf, size);
    errcode = encode_view_btree_reduction(r, dst, size_r);

alloc_error:
    free_view_btree_reduction(r);

    return errcode;
}


couchstore_error_t view_btree_stats_reduce(char *dst,
                                           size_t *size_r,
                                           const nodelist *leaflist,
                                           int count,
                                           void *ctx)
{
    view_btree_reduction_t *r = NULL;
    uint64_t subtree_count = 0;
    uint16_t j;
    double n;
    stats_t *s;
    sized_buf reduce_value;
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    char buf[MAX_REDUCTION_SIZE];
    size_t size = 0;
    const nodelist *i;
    view_reducer_ctx_t *errctx;
    char *doc_id;

    reduce_value.buf = NULL;
    r = (view_btree_reduction_t *) malloc(sizeof(view_btree_reduction_t));
    if (r == NULL) {
        errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto alloc_error;
    }
    r->reduce_values = NULL;
    memset(&r->partitions_bitmap, 0, sizeof(bitmap_t));
    reduce_value.size = sizeof(stats_t);
    reduce_value.buf = malloc(sizeof(stats_t));
    if (reduce_value.buf == NULL) {
        errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto alloc_error;
    }
    memset(reduce_value.buf, 0, sizeof(stats_t));
    s = (stats_t *)reduce_value.buf;

    for (i = leaflist; i != NULL && count > 0; i = i->next, count--) {
        view_btree_value_t *v = NULL;
        errcode = decode_view_btree_value(i->data.buf, i->data.size, &v);
        if (errcode != COUCHSTORE_SUCCESS) {
            goto alloc_error;
        }
        set_bit(&r->partitions_bitmap, v->partition);
        subtree_count += v->num_values;

        for (j = 0; j< v->num_values; ++j) {
            if (buf_to_double(&(v->values[j]), &n)) {
                s->sum += n;
                s->sumsqr += n * n;
                if (s->count++ == 0) {
                    s->min = s->max = n;
                } else if (n > s->max) {
                    s->max = n;
                } else if (n < s->min) {
                    s->min = n;
                }
            } else {
                view_btree_key_t *k = NULL;
                errcode = decode_view_btree_key(i->key.buf, i->key.size, &k);
                if (errcode != COUCHSTORE_SUCCESS) {
                    goto alloc_error;
                }
                errctx = (view_reducer_ctx_t *) ctx;
                doc_id = (char *) malloc(k->doc_id.size);
                if (doc_id == NULL) {
                    errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
                    free_view_btree_key(k);
                    free_view_btree_value(v);
                    goto alloc_error;
                }
                memcpy(doc_id, k->doc_id.buf, k->doc_id.size);
                errctx->error_doc_id = (const char *) doc_id;
                errctx->error = VIEW_REDUCER_ERROR_NOT_A_NUMBER;
                free_view_btree_key(k);
                free_view_btree_value(v);
                errcode = COUCHSTORE_ERROR_REDUCER_FAILURE;
                goto alloc_error;
            }
        }
        free_view_btree_value(v);
    }
    size = sprint_stats(buf, s->sum, s->count, s->min, s->max, s->sumsqr);
    assert(size > 0);
    r->kv_count = subtree_count;
    r->num_values = 1;
    r->reduce_values = (sized_buf *) malloc(r->num_values * sizeof(sized_buf));
    if (r->reduce_values == NULL) {
        errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto alloc_error;
    }

    r->reduce_values[0].size = size;
    r->reduce_values[0].buf = (char *) malloc(size);
    if (r->reduce_values[0].buf == NULL) {
        errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto alloc_error;
    }
    memcpy(r->reduce_values[0].buf, buf, size);
    errcode = encode_view_btree_reduction(r, dst, size_r);

alloc_error:
    free(reduce_value.buf);
    free_view_btree_reduction(r);

    return errcode;
}

couchstore_error_t view_btree_stats_rereduce(char *dst,
                                             size_t *size_r,
                                             const nodelist *itmlist,
                                             int count,
                                             void *ctx)
{
    view_btree_reduction_t *r = NULL;
    uint64_t subtree_count = 0;
    uint16_t j;
    sized_buf reduce_value;
    double n;
    stats_t *s, reduced;
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    char buf[MAX_REDUCTION_SIZE];
    size_t size = 0;
    const nodelist *i;
    view_reducer_ctx_t *errctx;
    char *doc_id;

    reduce_value.buf = NULL;
    r = (view_btree_reduction_t *) malloc(sizeof(view_btree_reduction_t));
    if (r == NULL) {
        errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto alloc_error;
    }
    r->reduce_values = NULL;
    memset(&r->partitions_bitmap, 0, sizeof(bitmap_t));
    reduce_value.size = sizeof(stats_t);
    reduce_value.buf = malloc(sizeof(stats_t));
    if (reduce_value.buf == NULL) {
        errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto alloc_error;
    }
    memset(reduce_value.buf, 0, sizeof(stats_t));
    s = (stats_t *)reduce_value.buf;

    for (i = itmlist; i != NULL && count > 0; i = i->next, count--) {
        view_btree_reduction_t *r2 = NULL;
        errcode = decode_view_btree_reduction(i->pointer->reduce_value.buf,
                                              i->pointer->reduce_value.size, &r2);
        if (errcode != COUCHSTORE_SUCCESS) {
            goto alloc_error;
        }
        union_bitmaps(&r->partitions_bitmap, &r2->partitions_bitmap);
        subtree_count += r2->kv_count;
        for (j = 0; j< r2->num_values; ++j) {
            int scanned = scan_stats(r2->reduce_values[j].buf,
                                    reduced.sum, reduced.count, reduced.min,
                                    reduced.max, reduced.sumsqr);
            if (scanned == 5) {
                if (reduced.min < s->min || s->count == 0) {
                    s->min = reduced.min;
                }
                if (reduced.max > s->max || s->count == 0) {
                    s->max = reduced.max;
                }
                s->count += reduced.count;
                s->sum += reduced.sum;
                s->sumsqr += reduced.sumsqr;
            } else {
                view_btree_key_t *k = NULL;
                errcode = decode_view_btree_key(i->key.buf, i->key.size, &k);
                if (errcode != COUCHSTORE_SUCCESS) {
                    goto alloc_error;
                }
                errctx = (view_reducer_ctx_t *) ctx;
                doc_id = (char *) malloc(k->doc_id.size);
                if (doc_id == NULL) {
                    errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
                    free_view_btree_key(k);
                    free_view_btree_reduction(r2);
                    goto alloc_error;
                }
                memcpy(doc_id, k->doc_id.buf, k->doc_id.size);
                errctx->error_doc_id = (const char *) doc_id;
                errctx->error = VIEW_REDUCER_ERROR_NOT_A_NUMBER;
                free_view_btree_key(k);
                free_view_btree_reduction(r2);
                errcode = COUCHSTORE_ERROR_REDUCER_FAILURE;
                goto alloc_error;
            }
        }
        free_view_btree_reduction(r2);
    }
    size = sprint_stats(buf, s->sum, s->count, s->min, s->max, s->sumsqr);
    assert(size > 0);
    r->kv_count = subtree_count;
    r->num_values = 1;
    r->reduce_values = (sized_buf *) malloc(r->num_values * sizeof(sized_buf));
    if (r->reduce_values == NULL) {
        errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto alloc_error;
    }

    r->reduce_values[0].size = size;
    r->reduce_values[0].buf = (char *) malloc(size);
    if (r->reduce_values[0].buf == NULL) {
        errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto alloc_error;
    }
    memcpy(r->reduce_values[0].buf, buf, size);
    errcode = encode_view_btree_reduction(r, dst, size_r);

alloc_error:
    free(reduce_value.buf);
    free_view_btree_reduction(r);

    return errcode;
}
