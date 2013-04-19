/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/**
 * @copyright 2013 Couchbase, Inc.
 *
 * @author Filipe Manana  <filipe@couchbase.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

#include "file_sorter.h"
#include "../mergesort.h"
#include "util.h"
#include "collate_json.h"
#include <string.h>
#include <fcntl.h>

/*
 * *** NOTE ***
 *
 * Maximum number of elements for each buffer the mergesort module keeps in
 * memory for sorting. The value here can be "infinite" because right now this
 * module is used only for sorting small files, which are created by CouchDB
 * (not larger than ~30Mb). Each record from a view file has a variable size,
 * since both keys and values are defined by users in the map functions and can
 * be any JSON with sizes of up to 4Kb and ~16Mb respectively. Number of views
 * in a design document, and document ID sizes, also influence the size of
 * view records.
 *
 * Once this module is used for sorting files with an unnounded size, another
 * mergesort implementation must be used, one that allows us to control the
 * maximum buffer size kept in memory (see MB-8054).
 */
#define SORT_MAX_BUFFER_ELEMENTS   1000000

#define REMOVE_OP 1
#define INSERT_OP 2

typedef struct {
    uint8_t   op;
    sized_buf k;
    sized_buf v;
    char      *buf;
} merge_record_t;

typedef struct {
    FILE *src_f;
    FILE *dst_f;
    int (*key_cmp_fun)(const sized_buf *key1, const sized_buf *key2);
} merge_ctx_t;


static char *alloc_record(void);
static char *duplicate_record(char *rec);
static void free_record(char *rec);

static int compare_op_records(const void *r1, const void *r2, void *ctx);
static int read_op_record(FILE *in, void *buf, void *ctx);
static int write_op_record(FILE *out, void *buf, void *ctx);

static couchstore_error_t sort_file(const char *source_path,
                                    const char *dest_path,
                                    merge_ctx_t *ctx);

LIBCOUCHSTORE_API
couchstore_error_t sort_view_kvs_ops_file(const char *source_path,
                                          const char *dest_path)
{
    merge_ctx_t ctx;

    ctx.key_cmp_fun = view_key_cmp;

    return sort_file(source_path, dest_path, &ctx);
}


LIBCOUCHSTORE_API
couchstore_error_t sort_view_ids_ops_file(const char *source_path,
                                          const char *dest_path)
{
    merge_ctx_t ctx;

    ctx.key_cmp_fun = view_id_cmp;

    return sort_file(source_path, dest_path, &ctx);
}


static couchstore_error_t sort_file(const char *source_path,
                                    const char *dest_path,
                                    merge_ctx_t *ctx)
{
    couchstore_error_t ret;

    ctx->src_f = fopen(source_path, "r+b");
    if (ctx->src_f == NULL) {
        return COUCHSTORE_ERROR_OPEN_FILE;
    }

    if (strcmp(source_path, dest_path) == 0) {
        ctx->dst_f = ctx->src_f;
    } else {
        ctx->dst_f = fopen(dest_path, "r+b");
        if (ctx->dst_f == NULL) {
            fclose(ctx->src_f);
            return COUCHSTORE_ERROR_OPEN_FILE;
        }
    }

    ret = merge_sort(ctx->src_f,
                     ctx->dst_f,
                     read_op_record,
                     write_op_record,
                     compare_op_records,
                     alloc_record,
                     duplicate_record,
                     free_record,
                     ctx,
                     SORT_MAX_BUFFER_ELEMENTS,
                     NULL);

    fclose(ctx->src_f);
    if (ctx->dst_f != ctx->src_f) {
        fclose(ctx->dst_f);
    }

    return ret;
}


static int read_op_record(FILE *in, void *buf, void *ctx)
{
    uint32_t len, vlen;
    uint16_t klen;
    merge_record_t *rec = (merge_record_t *) buf;
    (void) ctx;

    /* On disk format is a bit weird, but it's compatible with what
       Erlang's file_sorter module requires. */

    if (fread(&len, sizeof(len), 1, in) != 1) {
        if (feof(in)) {
            return 0;
        } else {
            return -1;
        }
    }
    if (fread(&rec->op, sizeof(rec->op), 1, in) != 1) {
        return -1;
    }
    if (fread(&klen, sizeof(klen), 1, in) != 1) {
        return -1;
    }

    len = ntohl(len);
    klen = ntohs(klen);
    vlen = len - sizeof(rec->op) - sizeof(klen) - klen;
    rec->buf = (char *) malloc(klen + vlen);
    if (rec->buf == NULL) {
        return -1;
    }
    rec->k.size = klen;
    rec->k.buf = rec->buf;
    rec->v.size = vlen;
    rec->v.buf = rec->buf + klen;

    if (fread(rec->buf, klen + vlen, 1, in) != 1) {
        return -1;
    }

    return sizeof(*rec) + klen + vlen;
}


static int write_op_record(FILE *out, void *buf, void *ctx)
{
    merge_record_t *rec = (merge_record_t *) buf;
    uint16_t klen = htons((uint16_t) rec->k.size);
    uint32_t len;

    (void) ctx;
    len = (uint32_t) sizeof(rec->op) + sizeof(klen) + rec->k.size + rec->v.size;
    len = htonl(len);

    if (fwrite(&len, sizeof(len), 1, out) != 1) {
        return 0;
    }
    if (fwrite(&rec->op, sizeof(rec->op), 1, out) != 1) {
        return 0;
    }
    if (fwrite(&klen, sizeof(klen), 1, out) != 1) {
        return 0;
    }
    if (fwrite(rec->buf, rec->k.size + rec->v.size, 1, out) != 1) {
        return 0;
    }

    /* Once mergesort module writes a record, it reuses it for reading other
       records from a file - it's safe to free rec->buf here. */
    free(rec->buf);
    rec->buf = NULL;

    return 1;
}


static int compare_op_records(const void *r1, const void *r2, void *ctx)
{
    merge_ctx_t *merge_ctx = (merge_ctx_t *) ctx;
    merge_record_t *rec1 = (merge_record_t *) r1;
    merge_record_t *rec2 = (merge_record_t *) r2;
    int res;

    res = merge_ctx->key_cmp_fun(&rec1->k, &rec2->k);

    if (res == 0) {
        return ((int) rec1->op) - ((int) rec2->op);
    }

    return res;
}


static char *alloc_record(void)
{
    merge_record_t *rec = calloc(1, sizeof(*rec));
    rec->buf = NULL;
    return (char *) rec;
}


static char *duplicate_record(char *rec)
{
    merge_record_t *new_rec = (merge_record_t *) malloc(sizeof(merge_record_t));

    if (new_rec != NULL) {
        memcpy(new_rec, rec, sizeof(merge_record_t));
    }

    return (char *) new_rec;
}


static void free_record(char *rec)
{
    if (rec != NULL) {
        merge_record_t *record = (merge_record_t *) rec;
        free(record->buf);
        free(rec);
    }
}
