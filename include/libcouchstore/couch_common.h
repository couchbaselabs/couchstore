/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef COUCH_COMMON_H
#define COUCH_COMMON_H
#include <sys/types.h>
#include <stdint.h>

#include <libcouchstore/visibility.h>

#ifdef __cplusplus
extern "C" {
#endif

    /** Document content metadata flags */
    typedef uint8_t couchstore_content_meta_flags;
    enum {
        COUCH_DOC_IS_COMPRESSED = 128,  /**< Document contents compressed via Snappy */
        //Content Type Reasons (content_meta & 0x0F):
        COUCH_DOC_IS_JSON = 0,      /**< Document is valid JSON data */
        COUCH_DOC_INVALID_JSON = 1, /**< Document was checked, and was not valid JSON */
        COUCH_DOC_INVALID_JSON_KEY = 2, /**< Document was checked, and contained reserved keys,
                                             was not inserted as JSON. */
        COUCH_DOC_NON_JSON_MODE = 3 /**< Document was not checked (DB running in non-JSON mode) */
    };

    typedef struct _sized_buf {
        char *buf;
        size_t size;
        /* Size of data on disk - this accounts for eventual compression,
           checksums, block prefixes/padding and any other metadata.
           To be set by file write operations. */
        size_t disk_size;
    } sized_buf;

    typedef struct _doc {
        sized_buf id;
        sized_buf data;
    } Doc;

    typedef struct _docinfo {
        sized_buf id;
        uint64_t db_seq;
        uint64_t rev_seq;
        sized_buf rev_meta;
        int deleted;
        couchstore_content_meta_flags content_meta;
        uint64_t bp;
        size_t size;
    } DocInfo;

#define DOC_INFO_INITIALIZER { {0, 0}, 0, 0, {0, 0}, 0, 0, 0, 0 }

    typedef struct _local_doc {
        sized_buf id;
        sized_buf json;
        int deleted;
    } LocalDoc;


    typedef struct _db Db;

#ifdef __cplusplus
}
#endif

#endif
