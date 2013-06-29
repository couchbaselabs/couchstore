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

#include "file_merger.h"
#include <stdlib.h>
#include <assert.h>


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
} heap_t;

#define HEAP_PARENT(i)      (((i) - 1) / 2)
#define HEAP_LEFT(i)        ((2 * (i)) + 1)
#define HEAP_RIGHT(i)       ((2 * (i)) + 2)
#define HEAP_LESS(h, a, b)  \
    ((*(h)->ctx->compare_records)((a)->data, (b)->data, (h)->ctx->user_ctx) < 0)

typedef struct file_merger_ctx_t {
    unsigned                       num_files;
    FILE                           **files;
    FILE                           *dest_file;
    file_merger_read_record_t      read_record;
    file_merger_write_record_t     write_record;
    file_merger_record_free_t      free_record;
    file_merger_compare_records_t  compare_records;
    void                           *user_ctx;
    heap_t                         heap;
} file_merger_ctx_t;


static int  init_heap(heap_t *heap, unsigned max_elements, file_merger_ctx_t *ctx);
static void heap_destroy(heap_t *heap);
static record_t *heap_pop(heap_t *heap);
static int  heap_push(heap_t *heap, record_t *record);
static void min_heapify(heap_t *heap, unsigned i);

static file_merger_error_t do_merge_files(file_merger_ctx_t *ctx);


file_merger_error_t merge_files(const char *source_files[],
                                unsigned num_files,
                                const char *dest_file,
                                file_merger_read_record_t read_record,
                                file_merger_write_record_t write_record,
                                file_merger_compare_records_t compare_records,
                                file_merger_record_free_t free_record,
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

    if (!init_heap(&ctx.heap, num_files, &ctx)) {
        return FILE_MERGER_ERROR_ALLOC;
    }

    ctx.dest_file = fopen(dest_file, "ab");

    if (ctx.dest_file == NULL) {
        heap_destroy(&ctx.heap);
        return FILE_MERGER_ERROR_OPEN_FILE;
    }

    ctx.files = (FILE **) malloc(sizeof(FILE *) * num_files);

    if (ctx.files == NULL) {
        heap_destroy(&ctx.heap);
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
            heap_destroy(&ctx.heap);

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
    heap_destroy(&ctx.heap);
    fclose(ctx.dest_file);

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
            record = (record_t *) malloc(sizeof(*record));
            if (record == NULL) {
                return FILE_MERGER_ERROR_ALLOC;
            }
            record->data = record_data;
            record->file = i;
            assert(heap_push(&ctx->heap, record) != 0);
        }
    }

    while (ctx->heap.count != 0) {
        record_t *record;
        void *record_data;
        int record_len;
        file_merger_error_t ret;

        record = heap_pop(&ctx->heap);
        assert(record != NULL);
        assert(ctx->files[record->file] != NULL);

        ret = (*ctx->write_record)(ctx->dest_file, record->data, ctx->user_ctx);
        if (ret != FILE_MERGER_SUCCESS) {
            FREE_RECORD(ctx, record);
            return ret;
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
            (*ctx->free_record)(record->data, ctx->user_ctx);
            record->data = record_data;
            assert(heap_push(&ctx->heap, record) != 0);
        }

    }

    return FILE_MERGER_SUCCESS;
}


static int init_heap(heap_t *heap,
                     unsigned max_elements,
                     file_merger_ctx_t *ctx)
{
    heap->data = (record_t **) malloc(sizeof(record_t *) * max_elements);
    if (heap->data == NULL) {
        return 0;
    }

    heap->count = 0;
    heap->ctx = ctx;

    return 1;
}


static void heap_destroy(heap_t *heap)
{
    unsigned i;

    for (i = 0; i < heap->count; ++i) {
        FREE_RECORD(heap->ctx, heap->data[i]);
    }

    free(heap->data);
}


static record_t *heap_pop(heap_t *heap)
{
    record_t *min;

    if (heap->count == 0) {
        return NULL;
    }

    min = heap->data[0];
    heap->data[0] = heap->data[heap->count - 1];
    heap->count -= 1;
    min_heapify(heap, 0);

    return min;
}


static int heap_push(heap_t *heap, record_t *record)
{
    unsigned i, parent;

    if (heap->count == heap->ctx->num_files) {
        /* heap full */
        return 0;
    }

    heap->count += 1;

    for (i = heap->count - 1; i > 0; i = parent) {
        parent = HEAP_PARENT(i);
        if (HEAP_LESS(heap, heap->data[parent], record)) {
            break;
        }
        heap->data[i] = heap->data[parent];
    }

    heap->data[i] = record;

    return 1;
}


static void min_heapify(heap_t *heap, unsigned i)
{
    record_t *rec;
    unsigned l, r, min;

    while (1) {
        l = HEAP_LEFT(i);
        r = HEAP_RIGHT(i);
        min = i;

        if ((l < heap->count) && HEAP_LESS(heap, heap->data[l], heap->data[min])) {
            min = l;
        }
        if ((r < heap->count) && HEAP_LESS(heap, heap->data[r], heap->data[min])) {
            min = r;
        }
        if (min == i) {
            break;
        }

        rec = heap->data[i];
        heap->data[i] = heap->data[min];
        heap->data[min] = rec;
        i = min;
    }
}
