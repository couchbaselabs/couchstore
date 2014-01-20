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
#include "../util.h"

#define LINE_BUF_SIZE (8 * 1024)
#define SORT_ERROR_CODE(Err) (100 + (Err))
#define NIL_FILE "<nil>"

#define INITIAL_VIEW_FILE     'b'
#define INCREMENTAL_VIEW_FILE 'u'
#define INITIAL_SPATIAL_FILE  's'


int main(int argc, char *argv[])
{
    char tmp_dir[LINE_BUF_SIZE];
    char id_file[LINE_BUF_SIZE];
    int num_views;
    char **view_files;
    double **extra_data = NULL;
    file_sorter_error_t error;
    int status = 0, i, j;
    char type;
    /* The number of floats the bounding box consists of. It's two per
     * dimension */
    uint16_t *num_doubles = 0;

    (void) argc;
    (void) argv;

    if (couchstore_read_line(stdin, tmp_dir, LINE_BUF_SIZE) != tmp_dir) {
        fprintf(stderr, "Error reading temporary directory path.\n");
        exit(1);
    }

    if (fscanf(stdin, "%c\n", &type) != 1) {
        fprintf(stderr, "Error reading view file type.\n");
        exit(1);
    }
    switch (type) {
    case INITIAL_VIEW_FILE:
    case INCREMENTAL_VIEW_FILE:
    case INITIAL_SPATIAL_FILE:
        break;
    default:
        fprintf(stderr, "Invalid view file type: %c.\n", type);
        exit(1);
    }

    if (fscanf(stdin, "%d\n", &num_views) != 1) {
        fprintf(stderr, "Error reading number of views.\n");
        exit(1);
    }
    if (num_views < 0) {
        fprintf(stderr, "Number of views is negative.\n");
        exit(1);
    }

    if (couchstore_read_line(stdin, id_file, LINE_BUF_SIZE) != id_file) {
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

        if (couchstore_read_line(stdin, view_files[i], LINE_BUF_SIZE) != view_files[i]) {
            for (j = 0; j <= i; ++j) {
                free(view_files[j]);
            }
            free(view_files);
            fprintf(stderr, "Error reading view %d file.\n", (i + 1));
            exit(1);
        }
    }

    /* Spatial views get extra data transmitted */
    if (type == INITIAL_SPATIAL_FILE) {
        extra_data = (double **) calloc(num_views, sizeof(double *));
        if (extra_data == NULL) {
            fprintf(stderr, "Memory allocation failure.\n");
            status = 1;
            goto finished;
        }
        num_doubles = (uint16_t *) malloc(sizeof(uint16_t) * num_views);
        if (num_doubles == NULL) {
            free(extra_data);
            fprintf(stderr, "Memory allocation failure.\n");
            status = 1;
            goto finished;
        }

        for (i = 0; i < num_views; ++i) {
            if (fread(&num_doubles[i], sizeof(uint16_t), 1, stdin) < 1) {
                free(extra_data);
                free(num_doubles);
                fprintf(stderr, "Error reading the number of doubles (%d).\n",
                        (i + 1));
                status = 1;
                goto finished;
            }

            extra_data[i] = (double *) malloc(sizeof(double) * num_doubles[i]);
            if (extra_data[i] == NULL) {
                for (j = 0; j < i; ++j) {
                    free(extra_data[j]);
                }
                free(extra_data);
                free(num_doubles);
                fprintf(stderr, "Memory allocation failure.\n");
                status = 1;
                goto finished;
            }

            if (fread(extra_data[i], sizeof(double), num_doubles[i],
                      stdin) < num_doubles[i]) {
                for (j = 0; j <= i; ++j) {
                    free(extra_data[j]);
                }
                free(extra_data);
                free(num_doubles);
                fprintf(stderr, "Error reading extra data (%d).\n",
                        (i + 1));
                status = 1;
                goto finished;
            }
        }
    }

    if (strcmp(id_file, NIL_FILE) != 0) {
        switch (type) {
        case INITIAL_VIEW_FILE:
        case INITIAL_SPATIAL_FILE:
            error = sort_view_ids_file(id_file, tmp_dir, NULL, NULL);
            break;
        case INCREMENTAL_VIEW_FILE:
            error = sort_view_ids_ops_file(id_file, tmp_dir);
            break;
        }
        if (error != FILE_SORTER_SUCCESS) {
            fprintf(stderr, "Error sorting id file: %d\n", error);
            status = SORT_ERROR_CODE(error);
            goto finished_spatial;
        }
    }

    for (i = 0; i < num_views; ++i) {
        if (strcmp(view_files[i], NIL_FILE) != 0) {
            switch (type) {
            case INITIAL_VIEW_FILE:
                error = sort_view_kvs_file(view_files[i], tmp_dir, NULL, NULL);
                break;
            case INCREMENTAL_VIEW_FILE:
                error = sort_view_kvs_ops_file(view_files[i], tmp_dir);
                break;
            case INITIAL_SPATIAL_FILE:
                /* For sorting the spatial file extra information
                 * (the bounding box) is needed */
                error = sort_spatial_kvs_file(view_files[i], tmp_dir,
                                              extra_data[i], num_doubles[i]);
                break;
            }
            if (error != FILE_SORTER_SUCCESS) {
                fprintf(stderr, "Error sorting view %d file: %d\n", (i + 1), error);
                status = SORT_ERROR_CODE(error);
                goto finished_spatial;
            }
        }
    }

 finished_spatial:
    if (type == INITIAL_SPATIAL_FILE) {
        for (i = 0; i < num_views; ++i) {
            free(extra_data[i]);
        }
        free(extra_data);
        free(num_doubles);
    }
 finished:
    for (i = 0; i < num_views; ++i) {
        free(view_files[i]);
    }
    free(view_files);

    return status;
}
