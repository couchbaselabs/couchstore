/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef COUCHSTORE_COUCH_INDEX_H
#define COUCHSTORE_COUCH_INDEX_H

#include <libcouchstore/couch_db.h>

#ifdef __cplusplus
extern "C" {
#endif

    /**
     * Opaque reference to an open index file.
     */
    typedef struct _CouchStoreIndex CouchStoreIndex;

    /**
     * Create a new index file.
     *
     * The file should be closed with couchstore_close_index().
     *
     * @param filename The name of the file containing the index. Any existing file at this path
     *          will be deleted.
     * @param index Pointer to where you want the handle to the index to be
     *           stored.
     * @return COUCHSTORE_SUCCESS for success
     */
    LIBCOUCHSTORE_API
    couchstore_error_t couchstore_create_index(const char *filename,
                                               CouchStoreIndex** index);
    
    /**
     * Close an open index file.
     *
     * @param index Pointer to the index handle to free.
     * @return COUCHSTORE_SUCCESS upon success
     */
    LIBCOUCHSTORE_API
    couchstore_error_t couchstore_close_index(CouchStoreIndex* index);

    /**
     * Read an unsorted key-value file and add its contents to an index file.
     * Each file added will create a new independent index within the file; they are not merged.
     *
     * The key-value file is a sequence of zero or more records, each of which consists of:
     *      key length (16 bits, big-endian)
     *      value length (32 bits, big-endian)
     *      key data
     *      value data
     *
     * @param inputPath The path to the key-value file
     * @param index The index file to write to
     * @return COUCHSTORE_SUCCESS on success, else an error code
     */
    LIBCOUCHSTORE_API
    couchstore_error_t couchstore_index_add(const char *inputPath,
                                            CouchStoreIndex* index);

#ifdef __cplusplus
}
#endif
#endif
