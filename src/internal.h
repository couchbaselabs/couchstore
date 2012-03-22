/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef LIBCOUCHSTORE_INTERNAL_H
#define LIBCOUCHSTORE_INTERNAL_H 1

/*
 * This file contains datastructures and prototypes for functions only to
 * be used by the internal workings of libcoucstore. If you for some reason
 * need access to them from outside the library, you should write a
 * function to give you what you need.
 */

#include <libcouchstore/couch_db.h>

#ifdef __cplusplus
extern "C" {
#endif

    struct _db {
        uint64_t file_pos;
        couch_file_ops *file_ops;
        void *file_ops_cookie;
        db_header header;
        void *userdata;
    };

    couch_file_ops *couch_get_default_file_ops(void);

#ifdef __cplusplus
}
#endif

#endif
