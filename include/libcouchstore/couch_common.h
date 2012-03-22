/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef COUCH_COMMON_H
#define COUCH_COMMON_H
#include <sys/types.h>
#include <stdint.h>

#include <libcouchstore/visibility.h>

#ifdef __cplusplus
extern "C" {
#endif

    typedef struct _sized_buf {
        char *buf;
        size_t size;
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
        uint8_t content_meta;
        uint64_t bp;
        size_t size;
    } DocInfo;

#define DOC_INFO_INITIALIZER { {0, 0}, 0, 0, {0, 0}, 0, 0, 0, 0 }

    //Content Meta Flags
#define COUCH_DOC_IS_COMPRESSED 128
    //Content Type Reasons (content_meta & 0x0F)
    //Document is valid JSON data
#define COUCH_DOC_IS_JSON 0
    //Document was checked, and was not valid JSON
#define COUCH_DOC_INVALID_JSON 1
    //Document was checked, and contained reserved keys, was not inserted as JSON.
#define COUCH_DOC_INVALID_JSON_KEY 2
    //Document was not checked (DB running in non-JSON mode)
#define COUCH_DOC_NON_JSON_MODE 3

    typedef struct _local_doc {
        sized_buf id;
        sized_buf json;
        int deleted;
    } LocalDoc;


    typedef struct _db Db;

    /* Errors */
    typedef enum {
        COUCHSTORE_SUCCESS = 0,
        COUCHSTORE_ERROR_OPEN_FILE = -1,
        COUCHSTORE_ERROR_PARSE_TERM = -2,
        COUCHSTORE_ERROR_ALLOC_FAIL = -3,
        COUCHSTORE_ERROR_READ = -4,
        COUCHSTORE_ERROR_DOC_NOT_FOUND = -5,
        COUCHSTORE_ERROR_NO_HEADER = -6,
        COUCHSTORE_ERROR_WRITE = -7,
        COUCHSTORE_ERROR_HEADER_VERSION = -8,
        COUCHSTORE_ERROR_CHECKSUM_FAIL = -9,
        COUCHSTORE_ERROR_INVALID_ARGUMENTS = -10,
        COUCHSTORE_ERROR_NO_SUCH_FILE = -11
    } couchstore_error_t;

#ifdef __cplusplus
}
#endif

#endif
