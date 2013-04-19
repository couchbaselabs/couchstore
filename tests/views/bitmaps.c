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

#include "view_tests.h"


void test_bitmaps()
{
    TPRINT("Running view bitmap tests\n");

    bitmap_t bm, bm1, bm2;
    uint16_t one_bits[] = {1023, 1013, 500, 401, 1, 7, 666, 69};
    int set;
    uint16_t i, j;

    memset(&bm, 0, sizeof(bitmap_t));
    for (j = 0; j < (sizeof(one_bits) / sizeof(uint16_t)); ++j) {
        set_bit(&bm, one_bits[j]);
    }

    for (i = 0; i < 1024; ++i) {
        set = 0;
        for (j = 0; j < (sizeof(one_bits) / sizeof(uint16_t)); ++j) {
            if (one_bits[j] == i) {
                set = 1;
                break;
            }
        }

        if (set) {
            assert(is_bit_set(&bm, i));
        } else {
            assert(!is_bit_set(&bm, i));
        }

        unset_bit(&bm, i);
        assert(!is_bit_set(&bm, i));
    }

    memset(&bm, 0, sizeof(bitmap_t));
    set_bit(&bm, 1023);
    set_bit(&bm, 514);
    set_bit(&bm, 0);
    for (i = 0; i < (sizeof((bm.chunks)) / sizeof(bm.chunks[0])); ++i) {
        switch (i) {
        case 0:
            assert(bm.chunks[i] == 0x80);
            break;
        case 63:
            assert(bm.chunks[i] == 0x04);
            break;
        case 127:
            assert(bm.chunks[i] == 0x01);
            break;
        default:
            assert(bm.chunks[i] == 0);
        }
    }

    memset(&bm1, 0, sizeof(bitmap_t));
    memset(&bm2, 0, sizeof(bitmap_t));
    set_bit(&bm1, 1023);
    set_bit(&bm2, 0);

    union_bitmaps(&bm1, &bm2);
    assert(bm1.chunks[0] == 0x80);
    assert(bm1.chunks[127] == 0x01);
}
