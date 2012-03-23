/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef COUCHSTORE_COUCH_DB_H
#define COUCHSTORE_COUCH_DB_H

#include "couch_common.h"

#include <libcouchstore/error.h>
#include <libcouchstore/file_ops.h>

#ifdef __cplusplus
extern "C" {
#endif

    /**
     * Open a database.
     *
     * The database should be closed with couchstore_close_db().
     *
     * @param filename The name of the file containing the database
     * @param flags Additional flags for how the database should
     *              be opened. See couchstore_open_flags_* for the
     *              available flags.
     * @oaram db Pointer to where you want the handle to the database to be
     *           stored.
     * @return COUCHSTORE_SUCCESS for success
     */
    LIBCOUCHSTORE_API
    couchstore_error_t couchstore_open_db(const char *filename,
                                          uint64_t flags,
                                          Db **db);

    /**
     * Open a database.
     *
     * The database should be closed with couchstore_close_db().
     *
     * @param filename The name of the file containing the database
     * @param flags Additional flags for how the database should
     *              be opened. See couchstore_open_flags_* for the
     *              available flags.
     * @param ops Pointer to a structure containing the file io operations
     *            you want the library to use.
     * @oaram db Pointer to where you want the handle to the database to be
     *           stored.
     * @return COUCHSTORE_SUCCESS for success
     */
    LIBCOUCHSTORE_API
    couchstore_error_t couchstore_open_db_ex(const char *filename,
                                             uint64_t flags,
                                             couch_file_ops *ops,
                                             Db **db);

    /*
     * Flags to pass as the flags parameter to couchstore_open_db
     */
    /**
     * Create a new empty .couch file if file doesn't exist.
     */
#define COUCHSTORE_OPEN_FLAG_CREATE 1
    /**
     * Open the database in read only mode
     */
#define COUCHSTORE_OPEN_FLAG_RDONLY 2


    /**
     * Close an open database and release all allocated resources.
     *
     * @param db Pointer to the database handle to release.
     * @return COUCHSTORE_SUCCESS upon success
     */
    LIBCOUCHSTORE_API
    couchstore_error_t couchstore_close_db(Db *db);


    /**
     * Get the position in the file of the mostly recently written
     * database header.
     */
    LIBCOUCHSTORE_API
    uint64_t couchstore_get_header_position(Db *db);


    /**
     * Save document pointed to by doc and docinfo to db.
     *
     * When saving documents you should only set the id, rev_meta,
     * rev_seq, deleted, and content_meta fields on the DocInfo.
     *
     * To delete a docuemnt, set doc to NULL.
     *
     * @param db database to save the document in
     * @param doc the document to save
     * @param info document info
     * @param options see descrtiption of COMPRESS_DOC_BODIES below
     * @return COUCHSTORE_SUCCESS upon success
     */
    LIBCOUCHSTORE_API
    couchstore_error_t couchstore_save_document(Db *db,
                                                Doc *doc,
                                                DocInfo *info,
                                                uint64_t options);

    /**
     * Save array of docs to db
     *
     * To delete docuemnts, set docs to NULL, the docs referenced by
     * the docinfos will be deleted. To intermix deletes and inserts
     * in a bulk update, pass docinfos with the deleted flag set.
     *
     * @param db the database to save documents in
     * @param docs an array of document pointers
     * @param infos an array of docinfo pointers
     * @param numDocs the number documents to save
     * @param options see descrtiption of COMPRESS_DOC_BODIES below
     * @return COUCHSTORE_SUCCESS upon success
     */
    LIBCOUCHSTORE_API
    couchstore_error_t couchstore_save_documents(Db *db,
                                                 Doc **docs,
                                                 DocInfo **infos,
                                                 long numDocs,
                                                 uint64_t options);
    /*
     * Options used by couchstore_save_document() and
     * couchstore_save_documents():
     */

    /**
     * Snappy compress document data if the high bit of the
     * content_meta field of the DocInfo is set. This is NOT the
     * default, and if this is not set the data field of the Doc will
     * be written to disk as-is, regardless of the content_meta flags.
     */
#define COMPRESS_DOC_BODIES 1

    /**
     * Commit all pending changes and flush buffers to persistent storage.
     *
     * @param db database to perform the commit on
     * @return COUCHSTORE_SUCCESS on success
     */
    LIBCOUCHSTORE_API
    couchstore_error_t couchstore_commit(Db *db);

    /**
     * Retrieve the document info for a given key.
     *
     * The info should be released with couchstore_free_docinfo()
     *
     * @param id the document identifier
     * @param idlen the number of bytes in the identifier
     * @param pInfo where to store the result
     * @return COUCHSTORE_SUCCESS on success.
     */
    LIBCOUCHSTORE_API
    couchstore_error_t couchstore_docinfo_by_id(Db *db,
                                                const void *id,
                                                size_t idlen,
                                                DocInfo **pInfo);

    /**
     * Retrieve a doc from the db.
     *
     * The document should be released with couchstore_free_document()
     *
     * doc.id.buf will be the same buffer as id @@ WHat does this mean? @@
     *
     * @param db database to load document from
     * @param id the identifier to load
     * @param idlen the number of bytes in the id
     * @param pDoc Where to store the result
     * @param options See DECOMPRESS_DOC_BODIES
     * @return COUCHSTORE_SUCCESS if found
     */
    LIBCOUCHSTORE_API
    couchstore_error_t couchstore_open_document(Db *db,
                                                const void *id,
                                                size_t idlen,
                                                Doc **pDoc,
                                                uint64_t options);

    /**
     * Retrieve a doc from the using a docinfo.
     *
     * Do not free the docinfo before freeing the doc.
     * Should be freed with free_doc.
     *
     * @param db database to load document from
     * @param docid the the identified to load
     * @param pDoc Where to store the result
     * @param options See DECOMPRESS_DOC_BODIES
     * @return COUCHSTORE_SUCCESS if found
     */
    LIBCOUCHSTORE_API
    couchstore_error_t couchstore_open_doc_with_docinfo(Db *db,
                                                        DocInfo *docinfo,
                                                        Doc **pDoc,
                                                        uint64_t options);

    //Options flags for open_doc and open_doc_with_docinfo
    /* Snappy decompress document data if the high bit of the content_meta field
     * of the DocInfo is set.
     * This is NOT the default, and if this is not set the data field of the Doc
     * will be read from disk as-is, regardless of the content_meta flags. */
#define DECOMPRESS_DOC_BODIES 1

    /**
     * Release all allocated resources from a document returned from
     * couchstore_open_document()
     *
     * @param doc the document to release
     */
    LIBCOUCHSTORE_API
    void couchstore_free_document(Doc *doc);


    /**
     * Release all allocated resources from a docinfo structure returned by
     * couchstore_docinfo_by_id.
     *
     * @param docinfo the document info to relase
     */
    LIBCOUCHSTORE_API
    void couchstore_free_docinfo(DocInfo *docinfo);

    /**
     * The callback function used by couchstore_changes_since to iterate
     * through the documents.
     *
     * The docinfo structure is automatically released if the callback
     * returns 0. A non-zero return value will preserve the DocInfo
     * for future use (should be released with free_docinfo by the
     * caller)
     *
     * @param db the database being traversed
     * @param docinfo the current document
     * @param ctx user context
     * @return 0 or 1. See description above
     */
    typedef int (*couchstore_changes_callback_fn)(Db *db,
                                                  DocInfo *docinfo,
                                                  void *ctx);

    /**
     * Iterate through the changes since sequence number `since`.
     *
     * @param db the database to iterate through
     * @param since the sequence number to start iterating from
     * @param options Not used
     * @param callback the callback function used to iterate over all changes
     * @param ctx client context (passed to the callback)
     * @return COUCHSTORE_SUCCESS upon success
     */
    LIBCOUCHSTORE_API
    couchstore_error_t couchstore_changes_since(Db *db,
                                                uint64_t since,
                                                uint64_t options,
                                                couchstore_changes_callback_fn callback,
                                                void *ctx);
    /**
     * Get a local doc from the DB.
     *
     * The document should be released with couchstore_free_local_document()
     *
     * @param db database to load document from
     * @param id the identifier to load (must include "_local/" prefix)
     * @param idlen the number of bytes in the id
     * @param lDoc Where to store the result
     * @return COUCHSTORE_SUCCESS if found
     */
    LIBCOUCHSTORE_API
    couchstore_error_t couchstore_open_local_document(Db *db,
                                                      const void *id,
                                                      size_t idlen,
                                                      LocalDoc **lDoc);

    /**
     * Save a local doc to the db. ID must include the _local/ prefix.
     * To delete an existing doc set the deleted flag on the LocalDoc
     * struct. The json buffer will be ignored for a deletion.
     *
     * @param db the database to store the document in
     * @param lDoc the document to store
     * @return COUCHSTORE_SUCCESS on success
     */
    LIBCOUCHSTORE_API
    couchstore_error_t couchstore_save_local_document(Db *db, LocalDoc *lDoc);

    /*
     * Release all allocated resources from a LocalDoc obtained from
     * couchstore_open_local_document().
     *
     * @param lDoc document to release
     */
    LIBCOUCHSTORE_API
    void couchstore_free_local_document(LocalDoc *lDoc);

    /**
     * Convert an error code from couchstore to a textual description. The
     * text is a constant within the library so you should not try to modify
     * or release the pointer.
     *
     * @param errcode The error code to look up
     * @return a textual description of the error
     */
    LIBCOUCHSTORE_API
    const char *couchstore_strerror(couchstore_error_t errcode);
#ifdef __cplusplus
}
#endif
#endif
