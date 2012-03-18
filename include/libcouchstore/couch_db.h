/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef COUCHSTORE_COUCH_DB_H
#define COUCHSTORE_COUCH_DB_H

#include "couch_common.h"
#ifdef __cplusplus
extern "C" {
#endif

    //Open a database, should be closed with close_db
    LIBCOUCHSTORE_API
    int open_db(const char *filename, uint64_t options, couch_file_ops *ops, Db **db);

    //Flags passable to open_db options parameter
    /* Create a new empty .couch file if file doesn't exist. */
#define COUCH_CREATE_FILES 1

    /* Close a database and free resources */
    LIBCOUCHSTORE_API
    int close_db(Db *db);

    /* Get the position in the file of the mostly recently written database header. */
    LIBCOUCHSTORE_API
    uint64_t get_header_position(Db *db);

    /* When saving documents you should only set the
     * id, rev_meta, rev_seq, deleted, and content_meta fields on the
     * DocInfo. */
    /* Save document pointed to by doc and docinfo to db. */
    LIBCOUCHSTORE_API
    int save_doc(Db *db, Doc *doc, DocInfo *info, uint64_t options);
    /* Save array of docs to db, expects arrays of Doc and DocInfo pointers */
    LIBCOUCHSTORE_API
    int save_docs(Db *db, Doc **docs, DocInfo **infos, long numDocs, uint64_t options);

    //Options flags for save_doc and save_docs
    /* Snappy compress document data if the high bit of the content_meta field
     * of the DocInfo is set.
     * This is NOT the default, and if this is not set the data field of the Doc
     * will be written to disk as-is, regardless of the content_meta flags. */
#define COMPRESS_DOC_BODIES 1

    /* To delete docuemnts, call save_doc or save_docs with doc or docs set to NULL,
     * the docs referenced by the docinfos will be deleted.
     * To intermix deletes and inserts in a bulk update, pass docinfos with the deleted flag
     * set to save_docs.
     */

    /* Write header and fsync. */
    LIBCOUCHSTORE_API
    int commit_all(Db *db, uint64_t options);

    /* Retrieve a doc_info record using the by_id index
     * should be freed with free_docinfo.
     */
    LIBCOUCHSTORE_API
    int docinfo_by_id(Db *db, uint8_t *id,  size_t idlen, DocInfo **pInfo);

    /* Retrieve a doc from the db.
     * doc.id.buf will be the same buffer as id
     * Should be freed with free_doc. */
    LIBCOUCHSTORE_API
    int open_doc(Db *db, uint8_t *id, size_t idlen, Doc **pDoc, uint64_t options);

    /* Retrieve a doc from the using a docinfo.
     * Do not free the docinfo before freeing the doc.
     * Should be freed with free_doc. */
    LIBCOUCHSTORE_API
    int open_doc_with_docinfo(Db *db, DocInfo *docinfo, Doc **pDoc, uint64_t options);

    //Options flags for open_doc and open_doc_with_docinfo
    /* Snappy decompress document data if the high bit of the content_meta field
     * of the DocInfo is set.
     * This is NOT the default, and if this is not set the data field of the Doc
     * will be read from disk as-is, regardless of the content_meta flags. */
#define DECOMPRESS_DOC_BODIES 1

    /* Free a doc returned from open_doc. */
    LIBCOUCHSTORE_API
    void free_doc(Doc *doc);

    /* Free a docinfo returned from docinfo_by_id. */
    LIBCOUCHSTORE_API
    void free_docinfo(DocInfo *docinfo);

    /* Get changes since sequence number `since`.
     * the docinfo passed to the callback will be freed after the callback finishes,
     * do not free it. Return NO_FREE_DOCINFO to prevent freeing of the docinfo
     * (free it with free_docinfo when you are done with it)
     * otherwise 0. */
    LIBCOUCHSTORE_API
    int changes_since(Db *db, uint64_t since, uint64_t options,
                      int(*f)(Db *db, DocInfo *docinfo, void *ctx), void *ctx);
#define NO_FREE_DOCINFO 1

    /* Get a local doc from the DB. ID must include the _local/ prefix. */
    LIBCOUCHSTORE_API
    int open_local_doc(Db *db, uint8_t *id, size_t idlen, LocalDoc **lDoc);

    /* Save a local doc to the db. ID must include the _local/ prefix.
     * To delete an existing doc set the deleted flag on the LocalDoc struct. The json buffer
     * will be ignored for a deletion. */
    LIBCOUCHSTORE_API
    int save_local_doc(Db *db, LocalDoc *lDoc);

    /* LocalDoc's obtained with open_local_doc must be freed with free_local_doc */
    LIBCOUCHSTORE_API
    void free_local_doc(LocalDoc *lDoc);

    /* Description of errcodes returned by couchstore functions */
    LIBCOUCHSTORE_API
    const char *describe_error(int errcode);
#ifdef __cplusplus
}
#endif
#endif
