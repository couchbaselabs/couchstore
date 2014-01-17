/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/**
 * @copyright 2013 Couchbase, Inc.
 *
 * @author Filipe Manana  <filipe@couchbase.com>
 * @author Aliaksey Kandratsenka <alk@tut.by> (small optimization)
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

#include "file_merger.h"
#include <stdlib.h>
#include <assert.h>
#include <string.h>


typedef struct {
    void      *data;
    unsigned  file;
} record_t;

#define FREE_RECORD(ctx, rec)                                \
    do {                                                     \
        (*(ctx)->free_record)((rec)->data, (ctx)->user_ctx); \
        free((rec));                                         \
    } while (0)

struct file_merger_ctx_t;

typedef struct {
    struct file_merger_ctx_t  *ctx;
    record_t                  **data;
    unsigned                  count;
} sorted_vector_t;

typedef struct file_merger_ctx_t {
    unsigned                       num_files;
    FILE                           **files;
    FILE                           *dest_file;
    file_merger_read_record_t      read_record;
    file_merger_write_record_t     write_record;
    file_merger_record_free_t      free_record;
    file_merger_compare_records_t  compare_records;
    file_merger_feed_record_t      feed_record;
    void                           *user_ctx;
    sorted_vector_t                sorted_vector;
} file_merger_ctx_t;


static int  init_sorted_vector(sorted_vector_t *sorted_vector, unsigned max_elements, file_merger_ctx_t *ctx);
static void sorted_vector_destroy(sorted_vector_t *sorted_vector);
static record_t *sorted_vector_pop(sorted_vector_t *sorted_vector);
static int  sorted_vector_add(sorted_vector_t *sorted_vector, record_t *record);

static file_merger_error_t do_merge_files(file_merger_ctx_t *ctx);


file_merger_error_t merge_files(const char *source_files[],
                                unsigned num_files,
                                const char *dest_file,
                                file_merger_read_record_t read_record,
                                file_merger_write_record_t write_record,
                                file_merger_feed_record_t feed_record,
                                file_merger_compare_records_t compare_records,
                                file_merger_record_free_t free_record,
                                int skip_writeback,
                                void *user_ctx)
{
    file_merger_ctx_t ctx;
    file_merger_error_t ret;
    unsigned i, j;

    if (num_files == 0) {
        return FILE_MERGER_ERROR_BAD_ARG;
    }

    ctx.num_files = num_files;
    ctx.read_record = read_record;
    ctx.write_record = write_record;
    ctx.free_record = free_record;
    ctx.compare_records = compare_records;
    ctx.user_ctx = user_ctx;
    ctx.feed_record = feed_record;

    if (feed_record && skip_writeback) {
        ctx.dest_file = NULL;
    } else {
        ctx.dest_file = fopen(dest_file, "ab");
    }

    if (!init_sorted_vector(&ctx.sorted_vector, num_files, &ctx)) {
        return FILE_MERGER_ERROR_ALLOC;
    }

    if (feed_record == NULL && ctx.dest_file == NULL) {
        sorted_vector_destroy(&ctx.sorted_vector);
        return FILE_MERGER_ERROR_OPEN_FILE;
    }

    ctx.files = (FILE **) malloc(sizeof(FILE *) * num_files);

    if (ctx.files == NULL) {
        sorted_vector_destroy(&ctx.sorted_vector);
        fclose(ctx.dest_file);
        return FILE_MERGER_ERROR_ALLOC;
    }

    for (i = 0; i < num_files; ++i) {
        ctx.files[i] = fopen(source_files[i], "rb");

        if (ctx.files[i] == NULL) {
            for (j = 0; j < i; ++j) {
                fclose(ctx.files[j]);
            }
            free(ctx.files);
            fclose(ctx.dest_file);
            sorted_vector_destroy(&ctx.sorted_vector);

            return FILE_MERGER_ERROR_OPEN_FILE;
        }
    }

    ret = do_merge_files(&ctx);

    for (i = 0; i < ctx.num_files; ++i) {
        if (ctx.files[i] != NULL) {
            fclose(ctx.files[i]);
        }
    }
    free(ctx.files);
    sorted_vector_destroy(&ctx.sorted_vector);
    if (ctx.dest_file) {
        fclose(ctx.dest_file);
    }

    return ret;
}


static file_merger_error_t do_merge_files(file_merger_ctx_t *ctx)
{
    unsigned i;

    for (i = 0; i < ctx->num_files; ++i) {
        FILE *f = ctx->files[i];
        int record_len;
        void *record_data;
        record_t *record;

        record_len = (*ctx->read_record)(f, &record_data, ctx->user_ctx);

        if (record_len == 0) {
            fclose(f);
            ctx->files[i] = NULL;
        } else if (record_len < 0) {
            return (file_merger_error_t) record_len;
        } else {
            int rv;
            record = (record_t *) malloc(sizeof(*record));
            if (record == NULL) {
                return FILE_MERGER_ERROR_ALLOC;
            }
            record->data = record_data;
            record->file = i;
            rv = sorted_vector_add(&ctx->sorted_vector, record);
            assert(rv);
        }
    }

    while (ctx->sorted_vector.count != 0) {
        record_t *record;
        void *record_data;
        int record_len;
        file_merger_error_t ret;

        record = sorted_vector_pop(&ctx->sorted_vector);
        assert(record != NULL);
        assert(ctx->files[record->file] != NULL);

        if (ctx->feed_record) {
            ret = (*ctx->feed_record)(record->data, ctx->user_ctx);
            if (ret != FILE_MERGER_SUCCESS) {
                FREE_RECORD(ctx, record);
                return ret;
            }
        } else {
            assert(ctx->dest_file != NULL);
        }

        if (ctx->dest_file) {
            ret = (*ctx->write_record)(ctx->dest_file, record->data, ctx->user_ctx);
            if (ret != FILE_MERGER_SUCCESS) {
                FREE_RECORD(ctx, record);
                return ret;
            }
        }

        record_len = (*ctx->read_record)(ctx->files[record->file],
                                         &record_data,
                                         ctx->user_ctx);

        if (record_len == 0) {
            fclose(ctx->files[record->file]);
            ctx->files[record->file] = NULL;
            FREE_RECORD(ctx, record);
        } else if (record_len < 0) {
            FREE_RECORD(ctx, record);
            return (file_merger_error_t) record_len;
        } else {
            int rv;
            (*ctx->free_record)(record->data, ctx->user_ctx);
            record->data = record_data;
            rv = sorted_vector_add(&ctx->sorted_vector, record);
            assert(rv);
        }

    }

    return FILE_MERGER_SUCCESS;
}


static int init_sorted_vector(sorted_vector_t *sorted_vector,
                              unsigned max_elements,
                              file_merger_ctx_t *ctx)
{
    sorted_vector->data = (record_t **) malloc(sizeof(record_t *) * max_elements);
    if (sorted_vector->data == NULL) {
        return 0;
    }

    sorted_vector->count = 0;
    sorted_vector->ctx = ctx;

    return 1;
}


static void sorted_vector_destroy(sorted_vector_t *sorted_vector)
{
    unsigned i;

    for (i = 0; i < sorted_vector->count; ++i) {
        FREE_RECORD(sorted_vector->ctx, sorted_vector->data[i]);
    }

    free(sorted_vector->data);
}


static record_t *sorted_vector_pop(sorted_vector_t *sorted_vector)
{
    record_t *min;

    if (sorted_vector->count == 0) {
        return NULL;
    }

    min = sorted_vector->data[0];
    sorted_vector->count--;
    memmove(sorted_vector->data + 0, sorted_vector->data + 1, sizeof(sorted_vector->data[0])*(sorted_vector->count));

    return min;
}


#define SORTED_VECTOR_LESS(h, a, b)  \
    ((*(h)->ctx->compare_records)((a)->data, (b)->data, (h)->ctx->user_ctx) < 0)

static int sorted_vector_add(sorted_vector_t *sorted_vector, record_t *record)
{
    unsigned l, r;

    if (sorted_vector->count == sorted_vector->ctx->num_files) {
        /* sorted_vector full */
        return 0;
    }

    l = 0;
    r = sorted_vector->count;
    while (r - l > 1) {
        unsigned pos = (l + r) / 2;

        if (SORTED_VECTOR_LESS(sorted_vector, record, sorted_vector->data[pos])) {
            r = pos;
        } else {
            l = pos;
        }
    }

    if (l == 0 && r != 0) {
        if (SORTED_VECTOR_LESS(sorted_vector, record, sorted_vector->data[0])) {
            r = 0;
        }
    }

    if (r < sorted_vector->count) {
        memmove(sorted_vector->data + r + 1,
                sorted_vector->data + r,
                sizeof(sorted_vector->data[0]) * (sorted_vector->count - r));
    }

    sorted_vector->count += 1;

    sorted_vector->data[r] = record;

    return 1;
}
