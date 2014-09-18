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
#include "quicksort.h"

#define NSORT_RECORDS_INIT 500000
#define NSORT_RECORD_INCR  100000
#define NSORT_THREADS 2
#define SORTER_TMP_FILE_SUFFIX ".XXXXXX"

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

// For parallel sorter
typedef struct {
    void       **records;
    tmp_file_t *tmp_file;
    size_t     n;
} sort_job_t;

typedef struct {
    size_t              nworkers;
    size_t              free_workers;
    cb_cond_t           cond;
    cb_mutex_t          mutex;
    int                 finished;
    file_sorter_error_t error;
    sort_job_t          *job;
    file_sort_ctx_t     *ctx;
    cb_thread_t         *threads;
} parallel_sorter_t;


static file_sorter_error_t do_sort_file(file_sort_ctx_t *ctx);

static void sort_records(void **records, size_t n,
                                          file_sort_ctx_t *ctx);

static tmp_file_t *create_tmp_file(file_sort_ctx_t *ctx);

static file_sorter_error_t write_record_list(void **records,
                                             size_t n,
                                             tmp_file_t *tmp_file,
                                             file_sort_ctx_t *ctx);

static void pick_merge_files(file_sort_ctx_t *ctx,
                             unsigned *start,
                             unsigned *end,
                             unsigned *next_level);

static file_sorter_error_t merge_tmp_files(file_sort_ctx_t *ctx,
                                           unsigned start,
                                           unsigned end,
                                           unsigned next_level);

static file_sorter_error_t iterate_records_file(file_sort_ctx_t *ctx,
                                                const char *file);


static sort_job_t *create_sort_job(void **recs, size_t n, tmp_file_t *t);

static void free_sort_job(sort_job_t *job, parallel_sorter_t *sorter);

static parallel_sorter_t *create_parallel_sorter(size_t workers,
                                                 file_sort_ctx_t *ctx);

static void free_parallel_sorter(parallel_sorter_t *s);

static void sort_worker(void *args);

static file_sorter_error_t parallel_sorter_add_job(parallel_sorter_t *s,
                                                  void **records,
                                                  size_t n);

static file_sorter_error_t parallel_sorter_wait(parallel_sorter_t *s, size_t n);

static file_sorter_error_t parallel_sorter_finish(parallel_sorter_t *s);

static int sorter_random_name(char *tmpl, int totlen, int suffixlen);

static char *sorter_tmp_file_path(const char *tmp_dir, const char *prefix);

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


static sort_job_t *create_sort_job(void **recs, size_t n, tmp_file_t *t)
{
    sort_job_t *job = (sort_job_t *) calloc(1, sizeof(sort_job_t));
    if (job) {
        job->records = recs;
        job->n = n;
        job->tmp_file = t;
    }

    return job;
}


static void free_sort_job(sort_job_t *job, parallel_sorter_t *sorter)
{
    size_t i;

    if (job) {
        for (i = 0; i < job->n; i++) {
            (*sorter->ctx->free_record)(job->records[i], sorter->ctx->user_ctx);
        }

        free(job->records);
        free(job);
    }
}


static parallel_sorter_t *create_parallel_sorter(size_t workers,
                                                 file_sort_ctx_t *ctx)
{
    size_t i, j;
    parallel_sorter_t *s = (parallel_sorter_t *) calloc(1, sizeof(parallel_sorter_t));
    if (!s) {
        return NULL;
    }

    s->nworkers = workers;
    s->free_workers = workers;
    cb_mutex_initialize(&s->mutex);
    cb_cond_initialize(&s->cond);
    s->finished = 0;
    s->error = FILE_SORTER_SUCCESS;
    s->job = NULL;
    s->ctx = ctx;

    s->threads = (cb_thread_t *) calloc(workers, sizeof(cb_thread_t));
    if (!s->threads) {
        goto failure;
    }

    for (i = 0; i < workers; i++) {
        if (cb_create_thread(&s->threads[i], &sort_worker, (void *) s, 0) < 0) {
            cb_mutex_enter(&s->mutex);
            s->finished = 1;
            cb_mutex_exit(&s->mutex);
            cb_cond_broadcast(&s->cond);

            for (j = 0; j < i; j++) {
                cb_join_thread(s->threads[j]);
            }

            goto failure;
        }
    }

    return s;

failure:
    cb_mutex_destroy(&s->mutex);
    cb_cond_destroy(&s->cond);
    free(s->threads);
    free(s);
    return NULL;
}


static void free_parallel_sorter(parallel_sorter_t *s)
{
    if (s) {
        cb_mutex_destroy(&s->mutex);
        cb_cond_destroy(&s->cond);
        free_sort_job(s->job, s);
        free(s->threads);
        free(s);
    }
}


static void sort_worker(void *args)
{
    file_sorter_error_t ret;
    sort_job_t *job;
    parallel_sorter_t *s = (parallel_sorter_t *) args;

    /*
     * If a job is available, pick it up and notify all waiters
     * If job is not available, wait on condition variable
     * Once a job is complete, notify all waiters
     * Loop it over until finished flag becomes true
     */
    while (1) {
        cb_mutex_enter(&s->mutex);
        if (s->finished) {
            cb_mutex_exit(&s->mutex);
            return;
        }

        if (s->job) {
            job = s->job;
            s->job = NULL;
            s->free_workers -= 1;
            cb_mutex_exit(&s->mutex);
            cb_cond_broadcast(&s->cond);

            ret = write_record_list(job->records, job->n, job->tmp_file, s->ctx);
            free_sort_job(job, s);
            if (ret != FILE_SORTER_SUCCESS) {
                cb_mutex_enter(&s->mutex);
                s->finished = 1;
                s->error = ret;
                cb_mutex_exit(&s->mutex);
                cb_cond_broadcast(&s->cond);
                return;
            }

            cb_mutex_enter(&s->mutex);
            s->free_workers += 1;
            cb_mutex_exit(&s->mutex);
            cb_cond_broadcast(&s->cond);
        } else {
            cb_cond_wait(&s->cond, &s->mutex);
            cb_mutex_exit(&s->mutex);
        }
    }
}


 // Add a job and block wait until a worker picks up the job
static file_sorter_error_t parallel_sorter_add_job(parallel_sorter_t *s,
                                                  void **records,
                                                  size_t n)
{
    file_sorter_error_t ret;
    sort_job_t *job;
    tmp_file_t *tmp_file = create_tmp_file(s->ctx);

    if (tmp_file == NULL) {
        return FILE_SORTER_ERROR_MK_TMP_FILE;
    }

    job = create_sort_job(records, n, tmp_file);
    if (!job) {
        return FILE_SORTER_ERROR_ALLOC;
    }

    cb_mutex_enter(&s->mutex);
    if (s->finished) {
        ret = s->error;
        cb_mutex_exit(&s->mutex);
        return ret;
    }

    s->job = job;
    cb_mutex_exit(&s->mutex);
    cb_cond_signal(&s->cond);

    cb_mutex_enter(&s->mutex);
    while (s->job) {
        cb_cond_wait(&s->cond, &s->mutex);
    }
    cb_mutex_exit(&s->mutex);

    return FILE_SORTER_SUCCESS;
}


// Wait until n or more workers become idle after processing current jobs
static file_sorter_error_t parallel_sorter_wait(parallel_sorter_t *s, size_t n)
{
    while (1) {
        cb_mutex_enter(&s->mutex);
        if (s->finished) {
            cb_mutex_exit(&s->mutex);
            return s->error;
        }

        if (s->free_workers >= n) {
            cb_mutex_exit(&s->mutex);
            return FILE_SORTER_SUCCESS;
        }

        cb_cond_wait(&s->cond, &s->mutex);
        cb_mutex_exit(&s->mutex);
    }
}


// Notify all workers that we have no more jobs and wait for them to join
static file_sorter_error_t parallel_sorter_finish(parallel_sorter_t *s)
{
    file_sorter_error_t ret;
    size_t i;

    cb_mutex_enter(&s->mutex);
    s->finished = 1;
    cb_mutex_exit(&s->mutex);
    cb_cond_broadcast(&s->cond);

    for (i = 0; i < s->nworkers; i++) {
        ret = (file_sorter_error_t) cb_join_thread(s->threads[i]);
        if (ret != 0) {
            return ret;
        }
    }

    return FILE_SORTER_SUCCESS;
}


static file_sorter_error_t do_sort_file(file_sort_ctx_t *ctx)
{
    unsigned buffer_size = 0;
    size_t i = 0;
    size_t record_count = NSORT_RECORDS_INIT;
    void *record;
    int record_size;
    file_sorter_error_t ret;
    file_merger_feed_record_t feed_record = ctx->feed_record;
    parallel_sorter_t *sorter;
    void **records = (void **) calloc(record_count, sizeof(void *));

    if (records == NULL) {
        return FILE_SORTER_ERROR_ALLOC;
    }

    sorter = create_parallel_sorter(NSORT_THREADS, ctx);
    if (sorter == NULL) {
        return FILE_SORTER_ERROR_ALLOC;
    }

    ctx->feed_record = NULL;

    i = 0;
    while (1) {
        record_size = (*ctx->read_record)(ctx->f, &record, ctx->user_ctx);
        if (record_size < 0) {
           ret = (file_sorter_error_t) record_size;
           goto failure;
        } else if (record_size == 0) {
            break;
        }

        if (records == NULL) {
            records = (void **) calloc(record_count, sizeof(void *));
            if (records == NULL) {
                ret =  FILE_SORTER_ERROR_ALLOC;
                goto failure;
            }
        }

        records[i++] = record;
        if (i == record_count) {
            record_count += NSORT_RECORD_INCR;
            records = (void **) realloc(records, record_count * sizeof(void *));
            if (records == NULL) {
                ret =  FILE_SORTER_ERROR_ALLOC;
                goto failure;
            }
        }

        buffer_size += (unsigned) record_size;

        if (buffer_size >= ctx->max_buffer_size) {
            ret = parallel_sorter_add_job(sorter, records, i);
            if (ret != FILE_SORTER_SUCCESS) {
                goto failure;
            }

            ret = parallel_sorter_wait(sorter, 1);
            if (ret != FILE_SORTER_SUCCESS) {
                goto failure;
            }

            records = NULL;
            record_count = NSORT_RECORDS_INIT;
            buffer_size = 0;
            i = 0;
        }

        if (ctx->active_tmp_files >= ctx->num_tmp_files) {
            unsigned start, end, next_level;

            ret = parallel_sorter_wait(sorter, NSORT_THREADS);
            if (ret != FILE_SORTER_SUCCESS) {
                goto failure;
            }

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
        ret = parallel_sorter_add_job(sorter, records, i);
        if (ret != FILE_SORTER_SUCCESS) {
            goto failure;
        }
    }

    ret = parallel_sorter_finish(sorter);
    if (ret != FILE_SORTER_SUCCESS) {
        goto failure;
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

    ret = FILE_SORTER_SUCCESS;

 failure:
    free_parallel_sorter(sorter);
    return ret;
}


static file_sorter_error_t write_record_list(void **records,
                                             size_t n,
                                             tmp_file_t *tmp_file,
                                             file_sort_ctx_t *ctx)
{
    size_t i;
    FILE *f;

    sort_records(records, n, ctx);

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


    for (i = 0; i < n; i++) {
        file_sorter_error_t err;
        err = static_cast<file_sorter_error_t>((*ctx->write_record)(f, records[i], ctx->user_ctx));
        (*ctx->free_record)(records[i], ctx->user_ctx);
        records[i] = NULL;

        if (err != FILE_SORTER_SUCCESS) {
            fclose(f);
            return err;
        }
    }

    fclose(f);

    return FILE_SORTER_SUCCESS;
}


static tmp_file_t *create_tmp_file(file_sort_ctx_t *ctx)
{
    unsigned i = ctx->active_tmp_files;

    assert(ctx->active_tmp_files < ctx->num_tmp_files);
    assert(ctx->tmp_files[i].name == NULL);
    assert(ctx->tmp_files[i].level == 0);

    ctx->tmp_files[i].name = sorter_tmp_file_path(ctx->tmp_dir,
        ctx->tmp_file_prefix);
    if (ctx->tmp_files[i].name == NULL) {
        return NULL;
    }

    ctx->tmp_files[i].level = 1;
    ctx->active_tmp_files += 1;

    return &ctx->tmp_files[i];
}


static int qsort_cmp(const void *a, const void *b, void *ctx)
{
    file_sort_ctx_t *sort_ctx = (file_sort_ctx_t *) ctx;
    const void **k1 = (const void **) a, **k2 = (const void **) b;
    return (*sort_ctx->compare_records)(*k1, *k2, sort_ctx->user_ctx);
}


static void sort_records(void **records, size_t n,
                                         file_sort_ctx_t *ctx)
{
    quicksort(records, n, sizeof(void *), &qsort_cmp, ctx);
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
        dest_tmp_file = sorter_tmp_file_path(ctx->tmp_dir,
            ctx->tmp_file_prefix);
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
                                            NULL,
                                            ctx->free_record,
                                            ctx->skip_writeback,
                                            ctx->user_ctx);

    free(files);

    if (ret != FILE_SORTER_SUCCESS) {
        if (dest_tmp_file != NULL && dest_tmp_file != ctx->source_file) {
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

            (*ctx->free_record)(record_data, ctx->user_ctx);
        }
    }

cleanup:
    (*ctx->free_record)(record_data, ctx->user_ctx);
    if (f != NULL) {
        fclose(f);
    }

    return (file_sorter_error_t) ret;
}

int sorter_random_name(char *tmpl, int totlen, int suffixlen) {
    static unsigned int value = 0;
    tmpl = tmpl + totlen - suffixlen;
#ifdef WINDOWS
    _snprintf(tmpl, suffixlen, ".%d", value);
#else
    snprintf(tmpl, suffixlen, ".%d", value);
#endif
    if (++value > 2 << 18) {
        value = 0;
    }
    return 0;
}

char *sorter_tmp_file_path(const char *tmp_dir, const char *prefix) {
    char *file_path;
    size_t tmp_dir_len, prefix_len, total_len;
    tmp_dir_len = strlen(tmp_dir);
    prefix_len = strlen(prefix);
    total_len = tmp_dir_len + 1 + prefix_len + sizeof(SORTER_TMP_FILE_SUFFIX);
    file_path = (char *) malloc(total_len);

    if (file_path == NULL) {
        return NULL;
    }

    memcpy(file_path, tmp_dir, tmp_dir_len);
    /* Windows specific file API functions and stdio file functions on Windows
     * convert forward slashes to back slashes. */
    file_path[tmp_dir_len] = '/';
    memcpy(file_path + tmp_dir_len + 1, prefix, prefix_len);
    sorter_random_name(file_path, total_len - 1, sizeof(SORTER_TMP_FILE_SUFFIX)
        - 1);
    return file_path;
}
