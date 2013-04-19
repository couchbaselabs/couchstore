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

#ifndef _VIEW_UTILS_H
#define _VIEW_UTILS_H

#include "config.h"
#include <libcouchstore/couch_db.h>

#ifdef __cplusplus
extern "C" {
#endif

    /* compare keys of a view btree */
    int view_key_cmp(const sized_buf *key1, const sized_buf *key2);

    /* compare keys of the id btree of an index */
    int view_id_cmp(const sized_buf *key1, const sized_buf *key2);

#ifdef __cplusplus
}
#endif

#endif
