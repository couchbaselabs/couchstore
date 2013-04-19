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

#include "config.h"
#include <stdlib.h>
#include "file_merger.h"
#include "util.h"


typedef struct {
    sized_buf k;
    sized_buf v;
    char buffer[1];
} merge_record_t;

typedef struct {
    int (*key_cmp_fun)(const sized_buf *key1, const sized_buf *key2);
} merge_ctx_t;


static int read_record(FILE *f, void **buffer, void *ctx);
static file_merger_error_t write_record(FILE *f, void *buffer, void *ctx);
static int compare_records(const void *r1, const void *r2, void *ctx);
static void free_record(void *buffer, void *ctx);

static file_merger_error_t merge_view_files(const char *source_files[],
                                            unsigned num_source_files,
                                            const char *dest_path,
                                            merge_ctx_t *ctx);


LIBCOUCHSTORE_API
file_merger_error_t merge_view_kvs_files(const char *source_files[],
                                         unsigned num_source_files,
                                         const char *dest_path)
{
    merge_ctx_t ctx;

    ctx.key_cmp_fun = view_key_cmp;

    return merge_view_files(source_files, num_source_files, dest_path, &ctx);
}


LIBCOUCHSTORE_API
file_merger_error_t merge_view_ids_files(const char *source_files[],
                                         unsigned num_source_files,
                                         const char *dest_path)
{
    merge_ctx_t ctx;

    ctx.key_cmp_fun = view_id_cmp;

    return merge_view_files(source_files, num_source_files, dest_path, &ctx);
}


static file_merger_error_t merge_view_files(const char *source_files[],
                                            unsigned num_source_files,
                                            const char *dest_path,
                                            merge_ctx_t *ctx)
{
    return merge_files(source_files, num_source_files, dest_path,
                       read_record, write_record, compare_records,
                       free_record, ctx);
}



static int read_record(FILE *f, void **buffer, void *ctx)
{
    uint32_t len, vlen;
    uint16_t klen;
    merge_record_t *rec;
    (void) ctx;

    /* On disk format is compatible with what is produced by CouchDB. */

    if (fread(&len, sizeof(len), 1, f) != 1) {
        if (feof(f)) {
            return 0;
        } else {
            return FILE_MERGER_ERROR_FILE_READ;
        }
    }
    if (fread(&klen, sizeof(klen), 1, f) != 1) {
        return FILE_MERGER_ERROR_FILE_READ;
    }

    len = ntohl(len);
    klen = ntohs(klen);
    vlen = len - sizeof(klen) - klen;

    rec = (merge_record_t *) malloc(sizeof(merge_record_t) - 1 + klen + vlen);
    if (rec == NULL) {
        return FILE_MERGER_ERROR_ALLOC;
    }

    rec->k.size = klen;
    rec->k.buf = rec->buffer;
    rec->v.size = vlen;
    rec->v.buf = rec->buffer + klen;

    if (fread(rec->buffer, klen + vlen, 1, f) != 1) {
        free(rec);
        return FILE_MERGER_ERROR_FILE_READ;
    }

    *buffer = rec;

    return sizeof(merge_record_t) - 1 + klen + vlen;
}


static file_merger_error_t write_record(FILE *f, void *buffer, void *ctx)
{
    merge_record_t *rec = (merge_record_t *) buffer;
    uint16_t klen = htons((uint16_t) rec->k.size);
    uint32_t len;
    (void) ctx;

    len = (uint32_t) sizeof(klen) + rec->k.size + rec->v.size;
    len = htonl(len);

    if (fwrite(&len, sizeof(len), 1, f) != 1) {
        return FILE_MERGER_ERROR_FILE_WRITE;
    }
    if (fwrite(&klen, sizeof(klen), 1, f) != 1) {
        return FILE_MERGER_ERROR_FILE_WRITE;
    }
    if (fwrite(rec->buffer, rec->k.size + rec->v.size, 1, f) != 1) {
        return FILE_MERGER_ERROR_FILE_WRITE;
    }

    return FILE_MERGER_SUCCESS;
}


static int compare_records(const void *r1, const void *r2, void *ctx)
{
    merge_ctx_t *merge_ctx = (merge_ctx_t *) ctx;
    merge_record_t *rec1 = (merge_record_t *) r1;
    merge_record_t *rec2 = (merge_record_t *) r2;

    return merge_ctx->key_cmp_fun(&rec1->k, &rec2->k);
}


static void free_record(void *buffer, void *ctx)
{
    (void) ctx;
    free(buffer);
}
