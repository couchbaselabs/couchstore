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

#if defined(WIN32) || defined(_WIN32)
# define WINDOWS
# include <io.h>
#else
# ifndef _BSD_SOURCE
/* for mkstemp() */
#  define _BSD_SOURCE
# endif
# include <libgen.h>
# include <unistd.h>
#endif

#include "file_name_utils.h"

#include <platform/cb_malloc.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TMP_FILE_SUFFIX ".XXXXXX"


char *tmp_file_path(const char *tmp_dir, const char *prefix)
{
    char *file_path;
    size_t tmp_dir_len, prefix_len, total_len;
#ifdef WINDOWS
    errno_t err;
#else
    int fd;
#endif


    tmp_dir_len = strlen(tmp_dir);
    prefix_len = strlen(prefix);
    total_len = tmp_dir_len + 1 + prefix_len + sizeof(TMP_FILE_SUFFIX);
    file_path = (char *) cb_malloc(total_len);

    if (file_path == NULL) {
        return NULL;
    }

    memcpy(file_path, tmp_dir, tmp_dir_len);
    /* Windows specific file API functions and stdio file functions on Windows
     * convert forward slashes to back slashes. */
    file_path[tmp_dir_len] = '/';
    memcpy(file_path + tmp_dir_len + 1, prefix, prefix_len);
    memcpy(file_path + tmp_dir_len + 1 + prefix_len,
           TMP_FILE_SUFFIX,
           sizeof(TMP_FILE_SUFFIX));

#ifdef WINDOWS
    err = _mktemp_s(file_path, total_len);
    if (err != 0) {
        cb_free(file_path);
        return NULL;
    }
#else
    fd = mkstemp(file_path);
    if (fd == -1) {
        cb_free(file_path);
        return NULL;
    }
    close(fd);
    remove(file_path);
#endif

    return file_path;
}


char *file_basename(const char *path)
{
    char *ret;
#ifdef WINDOWS
    char drive[_MAX_DRIVE];
    char dir[_MAX_DIR];
    char fname[_MAX_FNAME];
    char ext[_MAX_EXT];

    _splitpath(path, drive, dir, fname, ext);
#else
    char *fname;

    fname = basename((char *) path);
#endif

    ret = (char *) cb_malloc(strlen(fname) + 1);
    if (ret != NULL) {
        strcpy(ret, fname);
    }

    return ret;
}
