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

#include "util.h"
#include "../util.h"
#include "../bitfield.h"
#include "collate_json.h"


int view_key_cmp(const sized_buf *key1, const sized_buf *key2)
{
    uint16_t json_key1_len = decode_raw16(*((raw_16 *) key1->buf));
    uint16_t json_key2_len = decode_raw16(*((raw_16 *) key2->buf));
    sized_buf json_key1 = {
        .size = json_key1_len,
        .buf = key1->buf + sizeof(uint16_t)
    };
    sized_buf json_key2 = {
        .size = json_key2_len,
        .buf = key2->buf + sizeof(uint16_t)
    };
    int res;

    res = CollateJSON(&json_key1, &json_key2, kCollateJSON_Unicode);

    if (res == 0) {
        sized_buf doc_id1 = {
            .size = key1->size - sizeof(uint16_t) - json_key1.size,
            .buf = key1->buf + sizeof(uint16_t) + json_key1.size
        };
        sized_buf doc_id2 = {
            .size = key2->size - sizeof(uint16_t) - json_key2.size,
            .buf = key2->buf + sizeof(uint16_t) + json_key2.size
        };

        res = ebin_cmp(&doc_id1, &doc_id2);
    }

    return res;
}


int view_id_cmp(const sized_buf *key1, const sized_buf *key2)
{
    return ebin_cmp(key1, key2);
}
