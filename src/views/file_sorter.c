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
#include "util.h"
#include <string.h>
#include <stdlib.h>

#define SORT_MAX_BUFFER_SIZE       (64 * 1024 * 1024)
#define SORT_MAX_NUM_TMP_FILES     16

#define REMOVE_OP 1
#define INSERT_OP 2

typedef struct {
    uint8_t   op;
    sized_buf k;
    sized_buf v;
} merge_record_t;

typedef struct {
    FILE *src_f;
    FILE *dst_f;
    int (*key_cmp_fun)(const sized_buf *key1, const sized_buf *key2);
} merge_ctx_t;


static int compare_op_records(const void *r1, const void *r2, void *ctx);
static int read_op_record(FILE *in, void **buf, void *ctx);
static file_merger_error_t write_op_record(FILE *out, void *buf, void *ctx);
static void record_free(void *record, void *ctx);

static file_sorter_error_t do_sort_file(const char *file_path,
                                        const char *tmp_dir,
                                        merge_ctx_t *ctx);

LIBCOUCHSTORE_API
file_sorter_error_t sort_view_kvs_ops_file(const char *file_path,
                                           const char *tmp_dir)
{
    merge_ctx_t ctx;

    ctx.key_cmp_fun = view_key_cmp;

    return do_sort_file(file_path, tmp_dir, &ctx);
}


LIBCOUCHSTORE_API
file_sorter_error_t sort_view_ids_ops_file(const char *file_path,
                                           const char *tmp_dir)
{
    merge_ctx_t ctx;

    ctx.key_cmp_fun = view_id_cmp;

    return do_sort_file(file_path, tmp_dir, &ctx);
}


static file_sorter_error_t do_sort_file(const char *file_path,
                                        const char *tmp_dir,
                                        merge_ctx_t *ctx)
{
    return sort_file(file_path,
                     tmp_dir,
                     SORT_MAX_NUM_TMP_FILES,
                     SORT_MAX_BUFFER_SIZE,
                     read_op_record,
                     write_op_record,
                     compare_op_records,
                     record_free,
                     ctx);
}


static int read_op_record(FILE *in, void **buf, void *ctx)
{
    uint32_t len, vlen;
    uint16_t klen;
    uint8_t op;
    merge_record_t *rec;
    (void) ctx;

    /* On disk format is a bit weird, but it's compatible with what
       Erlang's file_sorter module requires. */

    if (fread(&len, sizeof(len), 1, in) != 1) {
        if (feof(in)) {
            return 0;
        } else {
            return FILE_MERGER_ERROR_FILE_READ;
        }
    }
    if (fread(&op, sizeof(rec->op), 1, in) != 1) {
        return FILE_MERGER_ERROR_FILE_READ;
    }
    if (fread(&klen, sizeof(klen), 1, in) != 1) {
        return FILE_MERGER_ERROR_FILE_READ;
    }

    len = ntohl(len);
    klen = ntohs(klen);
    vlen = len - sizeof(op) - sizeof(klen) - klen;

    rec = (merge_record_t *) malloc(sizeof(merge_record_t) + klen + vlen);
    if (rec == NULL) {
        return FILE_MERGER_ERROR_ALLOC;
    }

    rec->op = op;
    rec->k.size = klen;
    rec->k.buf = ((char *) rec) + sizeof(merge_record_t);
    rec->v.size = vlen;
    rec->v.buf = ((char *) rec) + sizeof(merge_record_t) + klen;

    if (fread(rec->k.buf, klen + vlen, 1, in) != 1) {
        free(rec);
        return FILE_MERGER_ERROR_FILE_READ;
    }

    *buf = (void *) rec;

    return klen + vlen;
}


static file_merger_error_t write_op_record(FILE *out, void *buf, void *ctx)
{
    merge_record_t *rec = (merge_record_t *) buf;
    uint16_t klen = htons((uint16_t) rec->k.size);
    uint32_t len;

    (void) ctx;
    len = (uint32_t) sizeof(rec->op) + sizeof(klen) + rec->k.size + rec->v.size;
    len = htonl(len);

    if (fwrite(&len, sizeof(len), 1, out) != 1) {
        return FILE_MERGER_ERROR_FILE_WRITE;
    }
    if (fwrite(&rec->op, sizeof(rec->op), 1, out) != 1) {
        return FILE_MERGER_ERROR_FILE_WRITE;
    }
    if (fwrite(&klen, sizeof(klen), 1, out) != 1) {
        return FILE_MERGER_ERROR_FILE_WRITE;
    }
    if (fwrite(rec->k.buf, rec->k.size + rec->v.size, 1, out) != 1) {
        return FILE_MERGER_ERROR_FILE_WRITE;
    }

    return FILE_MERGER_SUCCESS;
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


static void record_free(void *record, void *ctx)
{
    (void) ctx;
    free(record);
}
