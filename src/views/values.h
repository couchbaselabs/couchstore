/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _VALUES_H
#define _VALUES_H

#include "config.h"
#include <stdint.h>
#include <libcouchstore/visibility.h>
#include <libcouchstore/couch_db.h>
#include <libcouchstore/couch_common.h>
#include "bitmap.h"
#include "sorted_list.h"
#include "../node_types.h"
#include "../bitfield.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    uint16_t        partition;
    uint16_t        num_values;
    sized_buf         *values;
} view_btree_value_t;


typedef struct {
    uint8_t     view_id;
    uint16_t    num_keys;
    sized_buf   *json_keys;
} view_keys_mapping_t;

typedef struct {
    uint16_t        partition;
    uint16_t        num_view_keys_map;
    view_keys_mapping_t *view_keys_map;
} view_id_btree_value_t;

LIBCOUCHSTORE_API
couchstore_error_t decode_view_btree_value(const char *bytes, size_t len,
                                                view_btree_value_t **value);

LIBCOUCHSTORE_API
couchstore_error_t encode_view_btree_value(const view_btree_value_t *value,
                                                char **buffer, size_t *buffer_size);
LIBCOUCHSTORE_API
void free_view_btree_value(view_btree_value_t *value);

LIBCOUCHSTORE_API
couchstore_error_t decode_view_id_btree_value(const char *bytes, size_t len,
                                                    view_id_btree_value_t **value);

LIBCOUCHSTORE_API
couchstore_error_t encode_view_id_btree_value(const view_id_btree_value_t *value,
                                                    char **buffer, size_t *buffer_size);

LIBCOUCHSTORE_API
void free_view_id_btree_value(view_id_btree_value_t *value);

#ifdef __cplusplus
}
#endif

#endif
