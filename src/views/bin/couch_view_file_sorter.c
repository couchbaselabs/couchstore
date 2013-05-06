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
#include "../file_sorter.h"

#define LINE_BUF_SIZE (8 * 1024)
#define SORT_ERROR_CODE(Err) (100 + (Err))
#define NIL_FILE "<nil>"

static char *read_line(char *buf, int size);


int main(int argc, char *argv[])
{
    char tmp_dir[LINE_BUF_SIZE];
    char id_file[LINE_BUF_SIZE];
    int num_views;
    char **view_files;
    file_sorter_error_t error;
    int status = 0, i, j;

    (void) argc;
    (void) argv;

    if (read_line(tmp_dir, LINE_BUF_SIZE) != tmp_dir) {
        fprintf(stderr, "Error reading temporary directory path.\n");
        exit(1);
    }

    if (fscanf(stdin, "%d\n", &num_views) != 1) {
        fprintf(stderr, "Error reading number of views.\n");
        exit(1);
    }
    if (num_views <= 0) {
        fprintf(stderr, "Number of views is negative or zero.\n");
        exit(1);
    }

    if (read_line(id_file, LINE_BUF_SIZE) != id_file) {
        fprintf(stderr, "Error reading id file path.\n");
        exit(1);
    }

    view_files = (char **) malloc(sizeof(char *) * num_views);
    if (view_files == NULL) {
        fprintf(stderr, "Memory allocation failure.\n");
        exit(1);
    }

    for (i = 0; i < num_views; ++i) {
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
            fprintf(stderr, "Error reading view %d file.\n", (i + 1));
            exit(1);
        }
    }

    if (strcmp(id_file, NIL_FILE) != 0) {
        error = sort_view_ids_ops_file(id_file, tmp_dir);
        if (error != FILE_SORTER_SUCCESS) {
            fprintf(stderr, "Error sorting id file: %d\n", error);
            status = SORT_ERROR_CODE(error);
            goto finished;
        }
    }

    for (i = 0; i < num_views; ++i) {
        if (strcmp(view_files[i], NIL_FILE) != 0) {
            error = sort_view_kvs_ops_file(view_files[i], tmp_dir);
            if (error != FILE_SORTER_SUCCESS) {
                fprintf(stderr, "Error sorting view %d file: %d\n", (i + 1), error);
                status = SORT_ERROR_CODE(error);
                goto finished;
            }
        }
    }

 finished:
    for (i = 0; i < num_views; ++i) {
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
