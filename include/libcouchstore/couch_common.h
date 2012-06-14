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
        /* Content Type Reasons (content_meta & 0x0F): */
        COUCH_DOC_IS_JSON = 0,      /**< Document is valid JSON data */
        COUCH_DOC_INVALID_JSON = 1, /**< Document was checked, and was not valid JSON */
        COUCH_DOC_INVALID_JSON_KEY = 2, /**< Document was checked, and contained reserved keys,
                                             was not inserted as JSON. */
        COUCH_DOC_NON_JSON_MODE = 3 /**< Document was not checked (DB running in non-JSON mode) */
    };

    /** A generic data blob. Nothing is implied about ownership of the block pointed to. */
    typedef struct _sized_buf {
        char *buf;
        size_t size;
    } sized_buf;

    /** A CouchStore document, consisting of an ID (key) and data, each of which is a blob. */
    typedef struct _doc {
        sized_buf id;
        sized_buf data;
    } Doc;

    /** Metadata of a CouchStore document. */
    typedef struct _docinfo {
        sized_buf id;               /**< Document ID (key) */
        uint64_t db_seq;            /**< Sequence number in database */
        uint64_t rev_seq;           /**< Revision number of document */
        sized_buf rev_meta;         /**< Revision metadata; uninterpreted by CouchStore.
                                         Needs to be kept small enough to fit in a B-tree index.*/
        int deleted;                /**< Is this a deleted revision? */
        couchstore_content_meta_flags content_meta;  /**< Content metadata flags */
        uint64_t bp;                /**< Byte offset of document data in file */
        size_t size;                /**< Data size in bytes */
    } DocInfo;

#define DOC_INFO_INITIALIZER { {0, 0}, 0, 0, {0, 0}, 0, 0, 0, 0 }

    /** Contents of a 'local' (unreplicated) document. */
    typedef struct _local_doc {
        sized_buf id;
        sized_buf json;
        int deleted;
    } LocalDoc;


    /** Opaque reference to an open database. */
    typedef struct _db Db;

#ifdef __cplusplus
}
#endif

#endif
