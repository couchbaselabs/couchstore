/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/**
 * @copyright 2014 Couchbase, Inc.
 *
 * @author Sarath Lakshman  <sarath@couchbase.com>
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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "../view_group.h"
#include "../util.h"
#include "util.h"

#define BUF_SIZE 8192

int main(int argc, char *argv[])
{
    view_group_info_t *group_info = NULL;
    char buf[BUF_SIZE];
    char *target_file = NULL;
    size_t len;
    int ret = COUCHSTORE_SUCCESS;
    sized_buf header_buf = {NULL, 0};
    sized_buf header_outbuf = {NULL, 0};
    uint64_t inserted = 0;
    view_error_t error_info;
    cb_thread_t exit_thread;

    (void) argc;
    (void) argv;

    /* Read target filepath */
    if (couchstore_read_line(stdin, buf, BUF_SIZE) != buf) {
        fprintf(stderr, "Error reading compaction target filepath\n");
        ret = COUCHSTORE_ERROR_INVALID_ARGUMENTS;
        goto out;
    }

    len = strlen(buf);
    target_file = (char *) malloc(len + 1);
    if (target_file == NULL) {
        fprintf(stderr, "Memory allocation failure\n");
        ret = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto out;
    }

    memcpy(target_file, buf, len);
    target_file[len] = '\0';

    group_info = couchstore_read_view_group_info(stdin, stderr);
    if (group_info == NULL) {
        ret = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto out;
    }

    /* Read group header bin */
    header_buf.size = couchstore_read_int(stdin, buf, sizeof(buf), &ret);
    if (ret != COUCHSTORE_SUCCESS) {
        fprintf(stderr, "Error reading viewgroup header size\n");
        ret = COUCHSTORE_ERROR_INVALID_ARGUMENTS;
        goto out;
    }

    header_buf.buf = malloc(header_buf.size);
    if (header_buf.buf == NULL) {
        fprintf(stderr, "Memory allocation failure\n");
        ret = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto out;
    }

    if (fread(header_buf.buf, header_buf.size, 1, stdin) != 1) {
        fprintf(stderr,
                "Error reading viewgroup header from stdin\n");
        ret = COUCHSTORE_ERROR_INVALID_ARGUMENTS;
        goto out;
    }

    ret = start_exit_listener(&exit_thread);
    if (ret) {
        fprintf(stderr, "Error starting stdin exit listener thread\n");
        goto out;
    }

    ret = couchstore_compact_view_group(group_info,
                                        target_file,
                                        &header_buf,
                                        &inserted,
                                        &header_outbuf,
                                        &error_info);

    if (ret != COUCHSTORE_SUCCESS) {
        if (error_info.error_msg != NULL && error_info.view_name != NULL) {
            fprintf(stderr,
                    "Error compacting index for view `%s`, reason: %s\n",
                    error_info.view_name,
                    error_info.error_msg);
        }
        goto out;
    }

    fprintf(stdout, "Header Len : %lu\n", header_outbuf.size);
    fwrite(header_outbuf.buf, header_outbuf.size, 1, stdout);
    fprintf(stdout, "\n");

    fprintf(stdout, "Results = inserts : %"PRIu64"\n", inserted);

out:
    couchstore_free_view_group_info(group_info);
    free((void *) error_info.error_msg);
    free((void *) error_info.view_name);
    free((void *) header_buf.buf);
    free((void *) header_outbuf.buf);
    free(target_file);

    return (ret < 0) ? (100 + ret) : ret;
}

