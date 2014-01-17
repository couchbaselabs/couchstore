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
#include "spatial.h"

#define SORT_MAX_BUFFER_SIZE       (64 * 1024 * 1024)
#define SORT_MAX_NUM_TMP_FILES     16


static file_sorter_error_t do_sort_file(const char *file_path,
                                        const char *tmp_dir,
                                        view_file_merge_ctx_t *ctx);

LIBCOUCHSTORE_API
file_sorter_error_t sort_view_kvs_ops_file(const char *file_path,
                                           const char *tmp_dir)
{
    view_file_merge_ctx_t ctx;

    ctx.key_cmp_fun = view_key_cmp;
    ctx.type = INCREMENTAL_UPDATE_VIEW_RECORD;

    return do_sort_file(file_path, tmp_dir, &ctx);
}


LIBCOUCHSTORE_API
file_sorter_error_t sort_view_kvs_file(const char *file_path,
                                       const char *tmp_dir)
{
    view_file_merge_ctx_t ctx;

    ctx.key_cmp_fun = view_key_cmp;
    ctx.type = INITIAL_BUILD_VIEW_RECORD;

    return do_sort_file(file_path, tmp_dir, &ctx);
}


LIBCOUCHSTORE_API
file_sorter_error_t sort_view_ids_ops_file(const char *file_path,
                                           const char *tmp_dir)
{
    view_file_merge_ctx_t ctx;

    ctx.key_cmp_fun = view_id_cmp;
    ctx.type = INCREMENTAL_UPDATE_VIEW_RECORD;

    return do_sort_file(file_path, tmp_dir, &ctx);
}


LIBCOUCHSTORE_API
file_sorter_error_t sort_view_ids_file(const char *file_path,
                                       const char *tmp_dir)
{
    view_file_merge_ctx_t ctx;

    ctx.key_cmp_fun = view_id_cmp;
    ctx.type = INITIAL_BUILD_VIEW_RECORD;

    return do_sort_file(file_path, tmp_dir, &ctx);
}


LIBCOUCHSTORE_API
file_sorter_error_t sort_spatial_kvs_file(const char *file_path,
                                          const char *tmp_dir,
                                          const double *mbb,
                                          const uint16_t mbb_num)
{
    file_sorter_error_t ret;
    view_file_merge_ctx_t ctx;
    scale_factor_t *user_ctx = spatial_scale_factor(mbb, mbb_num/2,
                                                    ZCODE_MAX_VALUE);

    ctx.key_cmp_fun = spatial_key_cmp;
    ctx.type = INITIAL_BUILD_SPATIAL_RECORD;
    ctx.user_ctx = (void *)user_ctx;

    ret = do_sort_file(file_path, tmp_dir, &ctx);

    free_spatial_scale_factor(user_ctx);
    return ret;
}


static file_sorter_error_t do_sort_file(const char *file_path,
                                        const char *tmp_dir,
                                        view_file_merge_ctx_t *ctx)
{
    return sort_file(file_path,
                     tmp_dir,
                     SORT_MAX_NUM_TMP_FILES,
                     SORT_MAX_BUFFER_SIZE,
                     read_view_record,
                     write_view_record,
                     NULL,
                     compare_view_records,
                     free_view_record,
                     0,
                     ctx);
}
