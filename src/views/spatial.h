/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/**
 * @copyright 2013 Couchbase, Inc.
 *
 * @author Volker Mische <volker@couchbase.com>
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

#ifndef _VIEW_SPATIAL_UTILS_H
#define _VIEW_SPATIAL_UTILS_H

#include "config.h"
#include <libcouchstore/couch_db.h>
#include "../file_merger.h"

#ifdef __cplusplus
extern "C" {
#endif
    #define ZCODE_PRECISION 32
    #define ZCODE_MAX_VALUE UINT32_MAX

    typedef struct {
        double *mbb;
        /* the total number of values (two times the dimension) */
        uint16_t num;
    } sized_mbb_t;

    /* It is used to scale up MBBs to the relative size of an enclosing one */
    typedef struct {
        /* The offset the value needs to be shifted in order to be in the
         * origin of the enclosing MBB */
        double *offsets;
        /* The scale factors for every dimension */
        double *scales;
        /* the total number of values, one per dimension */
        uint8_t dim;
    } scale_factor_t;


    /* compare keys of a spatial index */
    int spatial_key_cmp(const sized_buf *key1, const sized_buf *key2,
                        const void *user_ctx);

    /* Return the scale factor for every dimension that would be needed to
     * scale this MBB to the maximum value `max` (when shifted to the
     * origin)
     * Memory is dynamically allocted within the function, make sure to call
     * free_spatial_scale_factor() afterwards */
    scale_factor_t *spatial_scale_factor(const double *mbb, uint16_t dim,
                                         uint32_t max);

    /* Free the memory that spatial_scale_factor() allocated */
    void free_spatial_scale_factor(scale_factor_t *sf);

    /* Calculate the center of an multi-dimensional bounding box (MBB) */
    double *spatial_center(const sized_mbb_t *mbb);

    /* Scales all dimensions of a (multi-dimensional) point
     * with the given factor and offset */
    uint32_t *spatial_scale_point(const double *point,
                                  const scale_factor_t *sf);

    /* Set a bit on a buffer with a certain size */
    void set_bit_sized(unsigned char *bitmap, uint16_t size, uint16_t bit);

    /* Interleave numbers bitwise. The return value is a unsigned char array
     * with length 4 bytes * number of numbers.
     * The maximum number of numbers is (2^14)-1 (16383). */
    unsigned char *interleave_uint32s(uint32_t *numbers, uint16_t num);

#ifdef __cplusplus
}
#endif

#endif
