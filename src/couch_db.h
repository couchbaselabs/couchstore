#include "couch_common.h"

//Open a database, should be closed with close_db
int open_db(char* filename, uint64_t options, Db** db);
//Close a database and free resources
int close_db(Db* db);

/* Save document pointed to by pDoc to db.
 * (not implemented) */
int save_doc(Db* db, Doc* doc, uint64_t options);
/* Save array of docs to db
 * (not implemented) */
int save_docs(Db* db, Doc* doc, long numDocs, uint64_t options);
/* Delete doc by ID
 * (not implemented) */
int delete_doc(Db* db, uint8_t* id,  size_t idlen);

/* Retrieve a doc_info record using the by_id index
 * should be freed with free_docinfo.
 */
int docinfo_by_id(Db* db, uint8_t* id,  size_t idlen, DocInfo** pInfo);

/* Retrieve a doc from the db.
 * doc.id.buf will be the same buffer as id
 * Should be freed with free_doc. */
int open_doc(Db* db, uint8_t* id,  size_t idlen, Doc** pDoc, uint64_t options);

/* Retrieve a doc from the using a docinfo.
 * Do not free the docinfo before freeing the doc.
 * Should be freed with free_doc. */
int open_doc_with_docinfo(Db* db, DocInfo* docinfo, Doc** pDoc, uint64_t options);

/* Free a doc returned from open_doc. */
void free_doc(Doc* doc);

/* Free a docinfo returned from docinfo_by_id. */
void free_docinfo(DocInfo* docinfo);

/* Get changes since sequence number `since`.
 * the docinfo passed to the callback will be freed after the callback finishes,
 * do not free it. */
int changes_since(Db* db, uint64_t since, uint64_t options,
        int(*f)(Db* db, DocInfo* docinfo, void *ctx), void *ctx);

