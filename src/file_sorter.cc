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

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include "file_sorter.h"
#include "file_name_utils.h"


struct record_node {
    void               *record;
    struct record_node *next;
};

typedef struct {
    char     *name;
    unsigned  level;
} tmp_file_t;

typedef struct {
    const char                   *tmp_dir;
    const char                   *source_file;
    char                         *tmp_file_prefix;
    unsigned                      num_tmp_files;
    unsigned                      max_buffer_size;
    file_merger_read_record_t     read_record;
    file_merger_write_record_t    write_record;
    file_merger_feed_record_t     feed_record;
    file_merger_compare_records_t compare_records;
    file_merger_record_free_t     free_record;
    void                         *user_ctx;
    FILE                         *f;
    tmp_file_t                   *tmp_files;
    unsigned                      active_tmp_files;
    int                           skip_writeback;
} file_sort_ctx_t;


static file_sorter_error_t do_sort_file(file_sort_ctx_t *ctx);

static struct record_node *sort_records(struct record_node *list,
                                        file_sort_ctx_t *ctx);

static tmp_file_t *create_tmp_file(file_sort_ctx_t *ctx);

static file_sorter_error_t write_record_list(struct record_node *list,
                                             file_sort_ctx_t *ctx);

static void pick_merge_files(file_sort_ctx_t *ctx,
                             unsigned *start,
                             unsigned *end,
                             unsigned *next_level);

static file_sorter_error_t merge_tmp_files(file_sort_ctx_t *ctx,
                                           unsigned start,
                                           unsigned end,
                                           unsigned next_level);

static file_sorter_error_t iterate_records_file(file_sort_ctx_t *ctx, const char *file);


file_sorter_error_t sort_file(const char *source_file,
                              const char *tmp_dir,
                              unsigned num_tmp_files,
                              unsigned max_buffer_size,
                              file_merger_read_record_t read_record,
                              file_merger_write_record_t write_record,
                              file_merger_feed_record_t feed_record,
                              file_merger_compare_records_t compare_records,
                              file_merger_record_free_t free_record,
                              int skip_writeback,
                              void *user_ctx)
{
    file_sort_ctx_t ctx;
    unsigned i;
    file_sorter_error_t ret;

    if (num_tmp_files <= 1) {
        return FILE_SORTER_ERROR_BAD_ARG;
    }

    ctx.tmp_file_prefix = file_basename(source_file);
    if (ctx.tmp_file_prefix == NULL) {
        return FILE_SORTER_ERROR_TMP_FILE_BASENAME;
    }

    ctx.tmp_dir = tmp_dir;
    ctx.source_file = source_file;
    ctx.num_tmp_files = num_tmp_files;
    ctx.max_buffer_size = max_buffer_size;
    ctx.read_record = read_record;
    ctx.write_record = write_record;
    ctx.feed_record = feed_record;
    ctx.compare_records = compare_records;
    ctx.free_record = free_record;
    ctx.user_ctx = user_ctx;
    ctx.active_tmp_files = 0;
    ctx.skip_writeback = skip_writeback;

    if (skip_writeback && !feed_record) {
        return FILE_SORTER_ERROR_MISSING_CALLBACK;
    }

    ctx.f = fopen(source_file, "rb");
    if (ctx.f == NULL) {
        free(ctx.tmp_file_prefix);
        return FILE_SORTER_ERROR_OPEN_FILE;
    }

    ctx.tmp_files = (tmp_file_t *) malloc(sizeof(tmp_file_t) * num_tmp_files);

    if (ctx.tmp_files == NULL) {
        fclose(ctx.f);
        free(ctx.tmp_file_prefix);
        return FILE_SORTER_ERROR_ALLOC;
    }

    for (i = 0; i < num_tmp_files; ++i) {
        ctx.tmp_files[i].name = NULL;
        ctx.tmp_files[i].level = 0;
    }

    ret = do_sort_file(&ctx);

    if (ctx.f != NULL) {
        fclose(ctx.f);
    }
    for (i = 0; i < ctx.active_tmp_files; ++i) {
        if (ctx.tmp_files[i].name != NULL) {
            remove(ctx.tmp_files[i].name);
            free(ctx.tmp_files[i].name);
        }
    }
    free(ctx.tmp_files);
    free(ctx.tmp_file_prefix);

    return ret;
}


static file_sorter_error_t do_sort_file(file_sort_ctx_t *ctx)
{
    unsigned buffer_size = 0;
    void *record;
    int record_size;
    struct record_node *buffer, buffer_head;
    file_sorter_error_t ret;
    file_merger_feed_record_t feed_record = ctx->feed_record;

    buffer_head.record = NULL;
    buffer_head.next = NULL;
    buffer = &buffer_head;
    ctx->feed_record = NULL;

    while (1) {
        record_size = (*ctx->read_record)(ctx->f, &record, ctx->user_ctx);

        if (record_size < 0) {
           ret = (file_sorter_error_t) record_size;
           goto failure;
        } else if (record_size == 0) {
            break;
        }

        buffer->next = (struct record_node *) malloc(sizeof(struct record_node));
        if (buffer->next == NULL) {
            (*ctx->free_record)(record, ctx->user_ctx);
            ret = FILE_SORTER_ERROR_ALLOC;
            goto failure;
        }

        buffer = buffer->next;
        buffer->record = record;
        buffer->next = NULL;
        buffer_size += (unsigned) record_size;

        if (buffer_size >= ctx->max_buffer_size) {
            ret = write_record_list(&buffer_head, ctx);
            if (ret != FILE_SORTER_SUCCESS) {
                goto failure;
            }
            buffer_size = 0;
            buffer = &buffer_head;
        }

        if (ctx->active_tmp_files >= ctx->num_tmp_files) {
            unsigned start, end, next_level;

            pick_merge_files(ctx, &start, &end, &next_level);
            assert(next_level > 1);
            ret = merge_tmp_files(ctx, start, end, next_level);
            if (ret != FILE_SORTER_SUCCESS) {
                goto failure;
            }
        }
    }

    fclose(ctx->f);
    ctx->f = NULL;

    if (ctx->active_tmp_files == 0 && buffer_size == 0) {
        /* empty source file */
        return FILE_SORTER_SUCCESS;
    }

    if (buffer_size > 0) {
        ret = write_record_list(&buffer_head, ctx);
        if (ret != FILE_SORTER_SUCCESS) {
            goto failure;
        }
    }

    assert(ctx->active_tmp_files > 0);

    if (!ctx->skip_writeback && remove(ctx->source_file) != 0) {
        ret = FILE_SORTER_ERROR_DELETE_FILE;
        goto failure;
    }

    // Restore feed_record callback for final merge */
    ctx->feed_record = feed_record;
    if (ctx->active_tmp_files == 1) {
        if (ctx->feed_record) {
            ret = iterate_records_file(ctx, ctx->tmp_files[0].name);
            if (ret != FILE_SORTER_SUCCESS) {
                goto failure;
            }

            if (ctx->skip_writeback && remove(ctx->tmp_files[0].name) != 0) {
                ret = FILE_SORTER_ERROR_DELETE_FILE;
                goto failure;
            }
        }

        if (!ctx->skip_writeback &&
                rename(ctx->tmp_files[0].name, ctx->source_file) != 0) {
            ret = FILE_SORTER_ERROR_RENAME_FILE;
            goto failure;
        }
    } else if (ctx->active_tmp_files > 1) {
        ret = merge_tmp_files(ctx, 0, ctx->active_tmp_files, 0);
        if (ret != FILE_SORTER_SUCCESS) {
            goto failure;
        }
    }

    return FILE_SORTER_SUCCESS;

 failure:
    buffer = buffer_head.next;

    while (buffer != NULL) {
        struct record_node *next = buffer->next;
        (*ctx->free_record)(buffer->record, ctx->user_ctx);
        free(buffer);
        buffer = next;
    }

    return ret;
}


static file_sorter_error_t write_record_list(struct record_node *list,
                                             file_sort_ctx_t *ctx)
{
    struct record_node *node;
    FILE *f;
    tmp_file_t *tmp_file;

    list->next = sort_records(list->next, ctx);

    tmp_file = create_tmp_file(ctx);
    if (tmp_file == NULL) {
        return FILE_SORTER_ERROR_MK_TMP_FILE;
    }

    remove(tmp_file->name);
    f = fopen(tmp_file->name, "ab");
    if (f == NULL) {
        return FILE_SORTER_ERROR_MK_TMP_FILE;
    }

    if (ftell(f) != 0) {
        /* File already existed. It's not supposed to exist, and if it
         * exists it means a temporary file name collision happened or
         * some previous sort left temporary files that were never
         * deleted. */
        return FILE_SORTER_ERROR_NOT_EMPTY_TMP_FILE;
    }

    node = list->next;
    while (node != NULL) {
        struct record_node *next = node->next;
        file_sorter_error_t err;

        err = static_cast<file_sorter_error_t>((*ctx->write_record)(f, node->record, ctx->user_ctx));
        (*ctx->free_record)(node->record, ctx->user_ctx);
        free(node);

        if (err != FILE_SORTER_SUCCESS) {
            list->next = next;
            fclose(f);
            return err;
        }

        node = next;
    }

    fclose(f);
    list->next = NULL;

    return FILE_SORTER_SUCCESS;
}


static tmp_file_t *create_tmp_file(file_sort_ctx_t *ctx)
{
    unsigned i = ctx->active_tmp_files;

    assert(ctx->active_tmp_files < ctx->num_tmp_files);
    assert(ctx->tmp_files[i].name == NULL);
    assert(ctx->tmp_files[i].level == 0);

    ctx->tmp_files[i].name = tmp_file_path(ctx->tmp_dir, ctx->tmp_file_prefix);
    if (ctx->tmp_files[i].name == NULL) {
        return NULL;
    }

    ctx->tmp_files[i].level = 1;
    ctx->active_tmp_files += 1;

    return &ctx->tmp_files[i];
}


/*
 * Based on sample and free code from Simon Tatham (of Putty fame).
 * Original code at:
 *
 * http://www.chiark.greenend.org.uk/~sgtatham/algorithms/listsort.c
 */
static struct record_node *sort_records(struct record_node *list,
                                        file_sort_ctx_t *ctx)
{
#define CMP_LTE(a, b) \
    ((*ctx->compare_records)((a)->record, (b)->record, ctx->user_ctx) <= 0)

    struct record_node *p, *q, *e, *tail;
    int insize, nmerges, psize, qsize, i;

    insize = 1;
    while (1) {
        p = list;
        list = NULL;
        tail = NULL;
        nmerges = 0;  /* count number of merges we do in this pass */

        while (p != NULL) {
            nmerges++;  /* there exists a merge to be done */
            /* step `insize' places along from p */
            q = p;
            psize = 0;
            for (i = 0; i < insize; i++) {
                psize++;
                q = q->next;
                if (q == NULL) {
                    break;
                }
            }
            /* if q hasn't fallen off end, we have two lists to merge */
            qsize = insize;
            /* now we have two lists; merge them */
            while (psize > 0 || (qsize > 0 && q != NULL)) {

                /* decide whether next element of merge comes from p or q */
                if (psize == 0) {
                    /* p is empty; e must come from q. */
                    e = q;
                    q = q->next;
                    qsize--;
                } else if (qsize == 0 || q == NULL) {
                    /* q is empty; e must come from p. */
                    e = p;
                    p = p->next;
                    psize--;
                } else if (CMP_LTE(p, q)) {
                    /* First element of p is lower (or same);
                     * e must come from p. */
                    e = p;
                    p = p->next;
                    psize--;
                } else {
                    /* First element of q is lower; e must come from q. */
                    e = q;
                    q = q->next;
                    qsize--;
                }

                /* add the next element to the merged list */
                if (tail != NULL) {
                    tail->next = e;
                } else {
                    list = e;
                }
                tail = e;
            }
            /* now p has stepped `insize' places along, and q has too */
            p = q;
        }
        tail->next = NULL;
        /* If we have done only one merge, we're finished. */
        if (nmerges <= 1) {
            /* allow for nmerges==0, the empty list case */
            return list;
        }
        /* Otherwise repeat, merging lists twice the size */
        insize *= 2;
    }
}


static int tmp_file_cmp(const void *a, const void *b)
{
    unsigned x = ((const tmp_file_t *) a)->level;
    unsigned y = ((const tmp_file_t *) b)->level;

    if (x == 0) {
        return 1;
    }
    if (y == 0) {
        return -1;
    }

    return x - y;
}


static void pick_merge_files(file_sort_ctx_t *ctx,
                             unsigned *start,
                             unsigned *end,
                             unsigned *next_level)
{
    unsigned i, j, level;

    qsort(ctx->tmp_files, ctx->active_tmp_files, sizeof(tmp_file_t), tmp_file_cmp);

    for (i = 0; i < ctx->active_tmp_files; i = j) {
        level = ctx->tmp_files[i].level;
        assert(level > 0);
        j = i + 1;

        while (j < ctx->active_tmp_files) {
            assert(ctx->tmp_files[j].level > 0);
            if (ctx->tmp_files[j].level != level) {
                break;
            }
            j += 1;
        }

        if ((j - i) > 1) {
            *start = i;
            *end = j;
            *next_level = (j - i) * level;
            return;
        }
    }

    /* All files have a different level. */
    assert(ctx->active_tmp_files == ctx->num_tmp_files);
    assert(ctx->active_tmp_files >= 2);
    *start = 0;
    *end = 2;
    *next_level = ctx->tmp_files[0].level + ctx->tmp_files[1].level;
}


static file_sorter_error_t merge_tmp_files(file_sort_ctx_t *ctx,
                                           unsigned start,
                                           unsigned end,
                                           unsigned next_level)
{
    char *dest_tmp_file;
    const char **files;
    unsigned nfiles, i;
    file_sorter_error_t ret;
    file_merger_feed_record_t feed_record = NULL;

    nfiles = end - start;
    files = (const char **) malloc(sizeof(char *) * nfiles);
    if (files == NULL) {
        return FILE_SORTER_ERROR_ALLOC;
    }
    for (i = start; i < end; ++i) {
        files[i - start] = ctx->tmp_files[i].name;
        assert(files[i - start] != NULL);
    }

    if (next_level == 0) {
        /* Final merge iteration. */
        if (ctx->skip_writeback) {
            dest_tmp_file = NULL;
        } else {
            dest_tmp_file = (char *) ctx->source_file;
        }

        feed_record = ctx->feed_record;
    } else {
        dest_tmp_file = tmp_file_path(ctx->tmp_dir, ctx->tmp_file_prefix);
        if (dest_tmp_file == NULL) {
            free(files);
            return FILE_SORTER_ERROR_MK_TMP_FILE;
        }
    }

    ret = (file_sorter_error_t) merge_files(files,
                                            nfiles,
                                            dest_tmp_file,
                                            ctx->read_record,
                                            ctx->write_record,
                                            feed_record,
                                            ctx->compare_records,
                                            ctx->free_record,
                                            ctx->skip_writeback,
                                            ctx->user_ctx);

    free(files);

    if (ret != FILE_SORTER_SUCCESS) {
        if (dest_tmp_file != ctx->source_file) {
            remove(dest_tmp_file);
            free(dest_tmp_file);
        }
        return ret;
    }

    for (i = start; i < end; ++i) {
        if (remove(ctx->tmp_files[i].name) != 0) {
            if (dest_tmp_file != ctx->source_file) {
                free(dest_tmp_file);
            }
            return FILE_SORTER_ERROR_DELETE_FILE;
        }
        free(ctx->tmp_files[i].name);
        ctx->tmp_files[i].name = NULL;
        ctx->tmp_files[i].level = 0;
    }

    qsort(ctx->tmp_files + start, ctx->num_tmp_files - start,
          sizeof(tmp_file_t), tmp_file_cmp);
    ctx->active_tmp_files -= nfiles;

    if (dest_tmp_file != ctx->source_file) {
        i = ctx->active_tmp_files;
        ctx->tmp_files[i].name = dest_tmp_file;
        ctx->tmp_files[i].level = next_level;
        ctx->active_tmp_files += 1;
    }

    return FILE_SORTER_SUCCESS;
}

static file_sorter_error_t iterate_records_file(file_sort_ctx_t *ctx,
                                               const char *file)
{
    void *record_data = NULL;
    int record_len;
    FILE *f = fopen(file, "rb");
    int ret = FILE_SORTER_SUCCESS;

    if (f == NULL) {
        return FILE_SORTER_ERROR_OPEN_FILE;
    }

    while (1) {
        record_len = (*ctx->read_record)(f, &record_data, ctx->user_ctx);
        if (record_len == 0) {
            record_data = NULL;
            break;
        } else if (record_len < 0) {
            ret = record_len;
            goto cleanup;
        } else {
            ret = (*ctx->feed_record)(record_data, ctx->user_ctx);
            if (ret != FILE_SORTER_SUCCESS) {
                goto cleanup;
            }

            free(record_data);
        }
    }

cleanup:
    free(record_data);
    if (f != NULL) {
        fclose(f);
    }

    return (file_sorter_error_t) ret;
}
