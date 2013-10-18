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

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include "view_group.h"
#include "reducers.h"
#include "util.h"
#include "../arena.h"
#include "../couch_btree.h"
#include "../internal.h"
#include "../util.h"

#define VIEW_KV_CHUNK_THRESHOLD (7 * 1024)
#define VIEW_KP_CHUNK_THRESHOLD (6 * 1024)
#define MAX_HEADER_SIZE         (64 * 1024)

#ifndef HAVE_STRDUP
static char *strdup(const char *s)
{
    size_t len = strlen(s);
    char *c = (char *) malloc(len + 1);

    if (c != NULL) {
        memcpy(c, s, len);
        c[len] = '\0';
    }
    return c;
}
#else
# ifndef _BSD_SOURCE
#  define _BSD_SOURCE
# endif
extern char *strdup(const char *);
#endif


static couchstore_error_t open_view_group_file(const char *path,
                                               couchstore_open_flags open_flags,
                                               tree_file *file);

static couchstore_error_t build_btree(const char *source_file,
                                      tree_file *dest_file,
                                      compare_info *cmp,
                                      reduce_fn reduce_fun,
                                      reduce_fn rereduce_fun,
                                      void *reduce_ctx,
                                      node_pointer **out_root);

static couchstore_error_t build_id_btree(const char *source_file,
                                         tree_file *dest_file,
                                         node_pointer **out_root);

static couchstore_error_t build_view_btree(const char *source_file,
                                           const view_btree_info_t *info,
                                           tree_file *dest_file,
                                           node_pointer **out_root,
                                           view_error_t *error_info);

static void close_view_group_file(view_group_info_t *info);

static int read_record(FILE *f, arena *a, sized_buf *k, sized_buf *v);


LIBCOUCHSTORE_API
view_group_info_t *couchstore_read_view_group_info(FILE *in_stream,
                                                   FILE *error_stream)
{
    view_group_info_t *info;
    char buf[4096];
    char *dup;
    int i, j;
    int reduce_len;

    info = (view_group_info_t *) calloc(1, sizeof(*info));
    if (info == NULL) {
        fprintf(error_stream, "Memory allocation failure\n");
        goto out_error;
    }

    if (couchstore_read_line(in_stream, buf, sizeof(buf)) != buf) {
        fprintf(stderr, "Error reading source index file path\n");
        goto out_error;
    }
    dup = strdup(buf);
    if (dup == NULL) {
        fprintf(error_stream, "Memory allocation failure\n");
        goto out_error;
    }
    info->filepath = (const char *) dup;

    if (fscanf(in_stream, "%" SCNu64 "\n", &info->header_pos) != 1) {
        fprintf(error_stream, "Error reading header position\n");
        goto out_error;
    }

    if (fscanf(in_stream, "%d\n", &info->num_btrees) != 1) {
        fprintf(error_stream, "Error reading number of btrees\n");
        goto out_error;
    }

    info->btree_infos = (view_btree_info_t *)
        calloc(info->num_btrees, sizeof(view_btree_info_t));
    if (info->btree_infos == NULL) {
        fprintf(error_stream, "Memory allocation failure\n");
        info->num_btrees = 0;
        goto out_error;
    }

    for (i = 0; i < info->num_btrees; ++i) {
        view_btree_info_t *bti = &info->btree_infos[i];

        if (fscanf(in_stream, "%d\n", &bti->num_reducers) != 1) {
            fprintf(error_stream,
                    "Error reading number of reducers for btree %d\n", i);
            goto out_error;
        }

        bti->names = (const char **) calloc(bti->num_reducers, sizeof(char *));
        if (bti->names == NULL) {
            fprintf(error_stream, "Memory allocation failure\n");
            bti->num_reducers = 0;
            goto out_error;
        }

        bti->reducers = (const char **) calloc(bti->num_reducers, sizeof(char *));
        if (bti->reducers == NULL) {
            fprintf(error_stream, "Memory allocation failure\n");
            bti->num_reducers = 0;
            free(bti->names);
            goto out_error;
        }

        for (j = 0; j < bti->num_reducers; ++j) {
            if (couchstore_read_line(in_stream, buf, sizeof(buf)) != buf) {
                fprintf(error_stream,
                        "Error reading btree %d view %d name\n", i, j);
                goto out_error;
            }
            dup = strdup(buf);
            if (dup == NULL) {
                fprintf(error_stream, "Memory allocation failure\n");
                goto out_error;
            }
            bti->names[j] = (const char *) dup;

            if (fscanf(in_stream, "%d\n", &reduce_len) != 1) {
                fprintf(error_stream,
                        "Error reading btree %d view %d "
                        "reduce function size\n", i, j);
                goto out_error;
            }

            dup = (char *) malloc(reduce_len + 1);
            if (dup == NULL) {
                fprintf(error_stream, "Memory allocation failure\n");
                goto out_error;
            }

            if (fread(dup, reduce_len, 1, in_stream) != 1) {
                fprintf(error_stream,
                        "Error reading btree %d view %d reducer\n", i, j);
                free(dup);
                goto out_error;
            }
            dup[reduce_len] = '\0';
            bti->reducers[j] = (const char *) dup;
        }
    }

    return info;

out_error:
    couchstore_free_view_group_info(info);

    return NULL;
}


LIBCOUCHSTORE_API
void couchstore_free_view_group_info(view_group_info_t *info)
{
    int i, j;

    if (info == NULL)
        return;

    close_view_group_file(info);

    for (i = 0; i < info->num_btrees; ++i) {
        view_btree_info_t vi = info->btree_infos[i];

        for (j = 0; j < vi.num_reducers; ++j) {
            free((void *) vi.names[j]);
            free((void *) vi.reducers[j]);
        }
        free(vi.names);
        free(vi.reducers);
    }
    free(info->btree_infos);
    free((void *) info->filepath);
    free(info);
}


static void close_view_group_file(view_group_info_t *info)
{
    if (info->file.ops != NULL) {
        info->file.ops->close(info->file.handle);
        info->file.ops->destructor(info->file.handle);
        info->file.ops = NULL;
        info->file.handle = NULL;
    }
    free((void *) info->file.path);
    info->file.path = NULL;
}


static couchstore_error_t open_view_group_file(const char *path,
                                               couchstore_open_flags open_flags,
                                               tree_file *file)
{
    couchstore_error_t ret;
    const couch_file_ops *file_ops = couchstore_get_default_file_ops();
    int flags = 0;

    if ((open_flags & COUCHSTORE_OPEN_FLAG_RDONLY) &&
        (open_flags & COUCHSTORE_OPEN_FLAG_CREATE)) {
        return COUCHSTORE_ERROR_INVALID_ARGUMENTS;
    }

    if (open_flags & COUCHSTORE_OPEN_FLAG_RDONLY) {
        flags |= O_RDONLY;
    } else {
        flags |= O_RDWR;
    }

    if (open_flags & COUCHSTORE_OPEN_FLAG_CREATE) {
        flags |= O_CREAT;
    }

    ret = tree_file_open(file, path, flags, file_ops);

    return ret;
}


LIBCOUCHSTORE_API
couchstore_error_t couchstore_build_view_group(view_group_info_t *info,
                                               const char *id_records_file,
                                               const char *kv_records_files[],
                                               const char *dst_file,
                                               uint64_t *header_pos,
                                               view_error_t *error_info)
{
    couchstore_error_t ret;
    tree_file index_file;
    index_header_t *header = NULL;
    node_pointer *id_root = NULL;
    node_pointer **view_roots = NULL;
    int i;

    error_info->view_name = NULL;
    error_info->error_msg = NULL;
    index_file.handle = NULL;
    index_file.ops = NULL;
    index_file.path = NULL;

    view_roots = (node_pointer **) calloc(
        info->num_btrees, sizeof(node_pointer *));
    if (view_roots == NULL) {
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }

    ret = open_view_group_file(info->filepath,
                               COUCHSTORE_OPEN_FLAG_RDONLY,
                               &info->file);
    if (ret != COUCHSTORE_SUCCESS) {
        goto out;
    }

    ret = read_view_group_header(info, &header);
    if (ret != COUCHSTORE_SUCCESS) {
        goto out;
    }
    assert(info->num_btrees == header->num_views);

    ret = open_view_group_file(dst_file,
                               COUCHSTORE_OPEN_FLAG_CREATE,
                               &index_file);
    if (ret != COUCHSTORE_SUCCESS) {
        goto out;
    }

    ret = build_id_btree(id_records_file, &index_file, &id_root);
    if (ret != COUCHSTORE_SUCCESS) {
        goto out;
    }

    free(header->id_btree_state);
    header->id_btree_state = id_root;
    id_root = NULL;

    for (i = 0; i < info->num_btrees; ++i) {
        ret = build_view_btree(kv_records_files[i],
                               &info->btree_infos[i],
                               &index_file,
                               &view_roots[i],
                               error_info);
        if (ret != COUCHSTORE_SUCCESS) {
            goto out;
        }

        free(header->view_btree_states[i]);
        header->view_btree_states[i] = view_roots[i];
        view_roots[i] = NULL;
    }

    ret = write_view_group_header(&index_file, header_pos, header);
    if (ret != COUCHSTORE_SUCCESS) {
        goto out;
    }

    ret = COUCHSTORE_SUCCESS;

out:
    free_index_header(header);
    close_view_group_file(info);
    tree_file_close(&index_file);
    free(id_root);
    if (view_roots != NULL) {
        for (i = 0; i < info->num_btrees; ++i) {
            free(view_roots[i]);
        }
        free(view_roots);
    }

    return ret;
}


/*
 * Similar to util.c:read_view_record(), but it uses arena allocator, which is
 * required for the existing semantics/api of btree bottom-up build in
 * src/btree_modify.cc.
 */
static int read_record(FILE *f, arena *a, sized_buf *k, sized_buf *v)
{
    uint16_t klen;
    uint32_t vlen, len;

    if (fread(&len, sizeof(len), 1, f) != 1) {
        if (feof(f)) {
            return 0;
        } else {
            return COUCHSTORE_ERROR_READ;
        }
    }

    if (fread(&klen, sizeof(klen), 1, f) != 1) {
        return COUCHSTORE_ERROR_READ;
    }

    klen = ntohs(klen);
    vlen = len - sizeof(klen) - klen;

    k->size = klen;
    k->buf = (char *) arena_alloc(a, k->size);
    if (k->buf == NULL) {
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }

    v->size = vlen;
    v->buf = (char *) arena_alloc(a, v->size);
    if (v->buf == NULL) {
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }

    if (fread(k->buf, k->size, 1, f) != 1) {
        return FILE_MERGER_ERROR_FILE_READ;
    }
    if (fread(v->buf, v->size, 1, f) != 1) {
        return FILE_MERGER_ERROR_FILE_READ;
    }

    return len;
}


static int id_btree_cmp(const sized_buf *key1, const sized_buf *key2)
{
    return view_id_cmp(key1, key2, NULL);
}


static int view_btree_cmp(const sized_buf *key1, const sized_buf *key2)
{
    return view_key_cmp(key1, key2, NULL);
}


static couchstore_error_t build_btree(const char *source_file,
                                      tree_file *dest_file,
                                      compare_info *cmp,
                                      reduce_fn reduce_fun,
                                      reduce_fn rereduce_fun,
                                      void *reduce_ctx,
                                      node_pointer **out_root)
{
    couchstore_error_t ret = COUCHSTORE_SUCCESS;
    arena *transient_arena = new_arena(0);
    arena *persistent_arena = new_arena(0);
    couchfile_modify_result *mr;
    FILE *f = NULL;

    if (transient_arena == NULL || persistent_arena == NULL) {
        ret = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto out;
    }

    mr = new_btree_modres(persistent_arena,
                          transient_arena,
                          dest_file,
                          cmp,
                          reduce_fun,
                          rereduce_fun,
                          reduce_ctx,
                          VIEW_KV_CHUNK_THRESHOLD + (VIEW_KV_CHUNK_THRESHOLD / 3),
                          VIEW_KP_CHUNK_THRESHOLD + (VIEW_KP_CHUNK_THRESHOLD / 3));
    if (mr == NULL) {
        ret = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto out;
    }

    f = fopen(source_file, "rb");
    if (f == NULL) {
        ret = COUCHSTORE_ERROR_OPEN_FILE;
        goto out;
    }

    while (1) {
        sized_buf k, v;
        int read_ret;

        read_ret = read_record(f, transient_arena, &k, &v);
        if (read_ret == 0) {
            break;
        } else if (read_ret < 0) {
            ret = (couchstore_error_t) read_ret;
            goto out;
        }

        ret = mr_push_item(&k, &v, mr);
        if (ret != COUCHSTORE_SUCCESS) {
            goto out;
        }
        if (mr->count == 0) {
            arena_free_all(transient_arena);
        }
    }

    *out_root = complete_new_btree(mr, &ret);
    if (ret != COUCHSTORE_SUCCESS) {
        goto out;
    }

    /* Don't care about success/failure. Erlang side will eventually delete it. */
    remove(source_file);

out:
    if (f != NULL) {
        fclose(f);
    }
    if (transient_arena != NULL) {
        delete_arena(transient_arena);
    }
    if (persistent_arena != NULL) {
        delete_arena(persistent_arena);
    }

    return ret;
}


static couchstore_error_t build_id_btree(const char *source_file,
                                         tree_file *dest_file,
                                         node_pointer **out_root)
{
    couchstore_error_t ret;
    compare_info cmp;

    cmp.compare = id_btree_cmp;

    ret = build_btree(source_file,
                      dest_file,
                      &cmp,
                      view_id_btree_reduce,
                      view_id_btree_rereduce,
                      NULL,
                      out_root);

    return ret;
}


static couchstore_error_t build_view_btree(const char *source_file,
                                           const view_btree_info_t *info,
                                           tree_file *dest_file,
                                           node_pointer **out_root,
                                           view_error_t *error_info)
{
    couchstore_error_t ret;
    compare_info cmp;
    view_reducer_ctx_t *red_ctx = NULL;
    char *error_msg = NULL;

    cmp.compare = view_btree_cmp;
    red_ctx = make_view_reducer_ctx(info->reducers,
                                    info->num_reducers,
                                    &error_msg);
    if (red_ctx == NULL) {
        error_info->error_msg = (const char *) error_msg;
        error_info->view_name = (const char *) strdup(info->names[0]);
        return COUCHSTORE_ERROR_REDUCER_FAILURE;
    }

    ret = build_btree(source_file,
                      dest_file,
                      &cmp,
                      view_btree_reduce,
                      view_btree_rereduce,
                      red_ctx,
                      out_root);

    if (ret != COUCHSTORE_SUCCESS) {
        char *error_msg = NULL;

        if (red_ctx->error != NULL) {
            error_msg = strdup(red_ctx->error);
        } else {
            /* TODO: add more human friendly messages for other error types */
            switch (ret) {
            case COUCHSTORE_ERROR_REDUCTION_TOO_LARGE:
                /* TODO: add reduction byte size information to error message */
                error_msg = strdup("reduction too large");
                break;
            default:
                error_msg = (char *) malloc(64);
                if (error_msg != NULL) {
                    sprintf(error_msg, "%d", ret);
                }
            }
        }
        error_info->error_msg = (const char *) error_msg;
        error_info->view_name = (const char *) strdup(info->names[0]);
    }

    free_view_reducer_ctx(red_ctx);

    return ret;
}


couchstore_error_t read_view_group_header(const view_group_info_t *info,
                                          index_header_t **header)
{
    couchstore_error_t ret;
    cs_off_t pos = info->header_pos;
    const tree_file *file = &info->file;
    char *header_buf = NULL;
    int header_len;
    char buf;

    if (file->handle == NULL) {
        return COUCHSTORE_ERROR_FILE_CLOSED;
    }

    if (info->file.ops->pread(file->handle, &buf, 1, pos) != 1) {
        return COUCHSTORE_ERROR_READ;
    }
    if (buf == 0) {
        return COUCHSTORE_ERROR_NO_HEADER;
    } else if (buf != 1) {
        return COUCHSTORE_ERROR_CORRUPT;
    }

    header_len = pread_header(file, pos, &header_buf, MAX_HEADER_SIZE);
    if (header_len < 0) {
        return (couchstore_error_t) header_len;
    }

    ret = decode_index_header(header_buf, (size_t) header_len, header);
    free(header_buf);

    return ret;
}


couchstore_error_t write_view_group_header(tree_file *file,
                                           uint64_t *pos,
                                           const index_header_t *header)
{
    couchstore_error_t ret;
    sized_buf buf = { NULL, 0 };
    cs_off_t p;

    if (file->handle == NULL) {
        return COUCHSTORE_ERROR_FILE_CLOSED;
    }

    ret = encode_index_header(header, &buf.buf, &buf.size);
    if (ret != COUCHSTORE_SUCCESS) {
        return ret;
    }

    ret = write_header(file, &buf, &p);
    if (ret != COUCHSTORE_SUCCESS) {
        goto out;
    }

    assert(p >= 0);
    *pos = (uint64_t) p;

out:
    free(buf.buf);

    return ret;
}
