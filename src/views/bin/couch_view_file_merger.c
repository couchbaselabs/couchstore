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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <libcouchstore/couch_db.h>
#include "../file_merger.h"

#define LINE_BUF_SIZE (8 * 1024)
#define MERGE_ERROR_CODE(Err) (100 + (Err))

static char *read_line(char *buf, int size);


int main(int argc, char *argv[])
{
    char view_file_type;
    int num_files;
    int i, j;
    char **view_files;
    char dest_file[LINE_BUF_SIZE];
    file_merger_error_t error;
    int status = 0;

    (void) argc;
    (void) argv;

    if (fscanf(stdin, "%c\n", &view_file_type) != 1) {
        fprintf(stderr, "Error reading view file type.\n");
        exit(1);
    }
    if (view_file_type != 'i' && view_file_type != 'v') {
        fprintf(stderr, "View file type must be 'i' or 'v'.\n");
        exit(1);
    }

    if (fscanf(stdin, "%d\n", &num_files) != 1) {
        fprintf(stderr, "Error reading number of files to merge.\n");
        exit(1);
    }
    if (num_files <= 0) {
        fprintf(stderr, "Number of files to merge is negative or zero.\n");
        exit(1);
    }

    view_files = (char **) malloc(sizeof(char *) * num_files);
    if (view_files == NULL) {
        fprintf(stderr, "Memory allocation failure.\n");
        exit(1);
    }

    for (i = 0; i < num_files; ++i) {
        view_files[i] = (char *) malloc(LINE_BUF_SIZE);
        if (view_files[i] == NULL) {
            for (j = 0; j < i; ++j) {
                free(view_files[j]);
            }
            free(view_files);
            fprintf(stderr, "Memory allocation failure.\n");
            exit(1);
        }

        if (read_line(view_files[i], LINE_BUF_SIZE) != view_files[i]) {
            for (j = 0; j <= i; ++j) {
                free(view_files[j]);
            }
            free(view_files);
            fprintf(stderr, "Error reading view file number %d.\n", (i + 1));
            exit(1);
        }
    }

    if (read_line(dest_file, LINE_BUF_SIZE) != dest_file) {
        status = 1;
        goto finished;
    }

    if (num_files > 1) {
        const char **src_files = (const char **) view_files;
        switch (view_file_type) {
        case 'i':
            error = merge_view_ids_files(src_files, num_files, dest_file);
            break;
        case 'v':
            error = merge_view_kvs_files(src_files, num_files, dest_file);
            break;
        default:
            fprintf(stderr, "Unknown view file type: %c\n", view_file_type);
            status = 1;
            goto finished;
        }

        if (error == FILE_MERGER_SUCCESS) {
            for (i = 0; i < num_files; ++i) {
                /* Ignore failures.
                   Erlang side will eventually delete the files later. */
                remove(view_files[i]);
            }
        } else {
            status = MERGE_ERROR_CODE(error);
            goto finished;
        }
    } else {
        if (rename(view_files[0], dest_file) != 0) {
            fprintf(stderr, "Error renaming file %s to %s: %s\n",
                    view_files[0], dest_file, strerror(errno));
            status = 1;
            goto finished;
        }
    }

 finished:
    for (i = 0; i < num_files; ++i) {
        free(view_files[i]);
    }
    free(view_files);

    return status;
}


static char *read_line(char *buf, int size)
{
    size_t len;

    if (fgets(buf, size, stdin) != buf) {
        return NULL;
    }

    len = strlen(buf);
    if ((len >= 1) && (buf[len - 1] == '\n')) {
        buf[len - 1] = '\0';
    }

    return buf;
}
