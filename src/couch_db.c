/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <stdio.h>

#include "internal.h"
#include "node_types.h"
#include "couch_btree.h"
#include "bitfield.h"
#include "reduces.h"
#include "util.h"
#include "iobuffer.h"

#define ROOT_BASE_SIZE 12
#define HEADER_BASE_SIZE 25

// Initializes one of the db's root node pointers from data in the file header
static couchstore_error_t read_db_root(Db *db, node_pointer **root,
                                       void *root_data, int root_size)
{
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    if (root_size > 0) {
        error_unless(root_size >= ROOT_BASE_SIZE, COUCHSTORE_ERROR_CORRUPT);
        *root = read_root(root_data, root_size);
        error_unless(*root, COUCHSTORE_ERROR_ALLOC_FAIL);
        error_unless((*root)->pointer < db->header.position, COUCHSTORE_ERROR_CORRUPT);
    } else {
        *root = NULL;
    }
cleanup:
    return errcode;
}

// Attempts to initialize the database from a header at the given file position
static couchstore_error_t find_header_at_pos(Db *db, cs_off_t pos)
{
    int errcode = COUCHSTORE_SUCCESS;
    raw_file_header *header_buf = NULL;
    uint8_t buf[2];
    ssize_t readsize = db->file_ops->pread(db->file_handle, buf, 2, pos);
    error_unless(readsize == 2, COUCHSTORE_ERROR_READ);
    if (buf[0] == 0) {
        return COUCHSTORE_ERROR_NO_HEADER;
    } else if (buf[0] != 1) {
        return COUCHSTORE_ERROR_CORRUPT;
    }

    int header_len = pread_header(db, pos, (char**)&header_buf);
    if (header_len < 0) {
        error_pass(header_len);
    }

    db->header.position = pos;
    db->header.disk_version = decode_raw08(header_buf->version);
    error_unless(db->header.disk_version == COUCH_DISK_VERSION,
                 COUCHSTORE_ERROR_HEADER_VERSION);
    db->header.update_seq = decode_raw48(header_buf->update_seq);
    db->header.purge_seq = decode_raw48(header_buf->purge_seq);
    db->header.purge_ptr = decode_raw48(header_buf->purge_ptr);
    error_unless(db->header.purge_ptr <= db->header.position, COUCHSTORE_ERROR_CORRUPT);
    int seqrootsize = decode_raw16(header_buf->seqrootsize);
    int idrootsize = decode_raw16(header_buf->idrootsize);
    int localrootsize = decode_raw16(header_buf->localrootsize);
    error_unless(header_len == HEADER_BASE_SIZE + seqrootsize + idrootsize + localrootsize,
                 COUCHSTORE_ERROR_CORRUPT);

    char *root_data = (char*) (header_buf + 1);  // i.e. just past *header_buf
    error_pass(read_db_root(db, &db->header.by_seq_root, root_data, seqrootsize));
    root_data += seqrootsize;
    error_pass(read_db_root(db, &db->header.by_id_root, root_data, idrootsize));
    root_data += idrootsize;
    error_pass(read_db_root(db, &db->header.local_docs_root, root_data, localrootsize));

cleanup:
    free(header_buf);
    return errcode;
}

// Finds the database header by scanning back from the end of the file at 4k boundaries
static couchstore_error_t find_header(Db *db)
{
    couchstore_error_t last_header_errcode = COUCHSTORE_ERROR_NO_HEADER;
    int64_t pos = db->file_pos - 2;
    pos -= pos % COUCH_BLOCK_SIZE;
    for (; pos >= 0; pos -= COUCH_BLOCK_SIZE) {
        couchstore_error_t errcode = find_header_at_pos(db, pos);
        switch(errcode) {
            case COUCHSTORE_SUCCESS:
                // Found it!
                return COUCHSTORE_SUCCESS;
            case COUCHSTORE_ERROR_NO_HEADER:
                // No header here, so keep going
                break;
            case COUCHSTORE_ERROR_ALLOC_FAIL:
                // Fatal error
                return errcode;
            default:
                // Invalid header; continue, but remember the last error
                last_header_errcode = errcode;
                break;
        }
    }
    return last_header_errcode;
}

static couchstore_error_t write_header(Db *db)
{
    sized_buf writebuf;
    size_t seqrootsize = 0, idrootsize = 0, localrootsize = 0;
    if (db->header.by_seq_root) {
        seqrootsize = ROOT_BASE_SIZE + db->header.by_seq_root->reduce_value.size;
    }
    if (db->header.by_id_root) {
        idrootsize = ROOT_BASE_SIZE + db->header.by_id_root->reduce_value.size;
    }
    if (db->header.local_docs_root) {
        localrootsize = ROOT_BASE_SIZE + db->header.local_docs_root->reduce_value.size;
    }
    writebuf.size = sizeof(raw_file_header) + seqrootsize + idrootsize + localrootsize;
    writebuf.buf = (char *) calloc(1, writebuf.size);
    raw_file_header* header = (raw_file_header*)writebuf.buf;
    header->version = encode_raw08(COUCH_DISK_VERSION);
    header->update_seq = encode_raw48(db->header.update_seq);
    header->purge_seq = encode_raw48(db->header.purge_seq);
    header->purge_ptr = encode_raw48(db->header.purge_ptr);
    header->seqrootsize = encode_raw16((uint16_t)seqrootsize);
    header->idrootsize = encode_raw16((uint16_t)idrootsize);
    header->localrootsize = encode_raw16((uint16_t)localrootsize);
    uint8_t *root = (uint8_t*)(header + 1);
    encode_root(root, db->header.by_seq_root);
    root += seqrootsize;
    encode_root(root, db->header.by_id_root);
    root += idrootsize;
    encode_root(root, db->header.local_docs_root);
    cs_off_t pos;
    couchstore_error_t errcode = db_write_header(db, &writebuf, &pos);
    if (errcode == COUCHSTORE_SUCCESS) {
        db->header.position = pos;
    }
    free(writebuf.buf);
    return errcode;
}

static couchstore_error_t create_header(Db *db)
{
    db->header.disk_version = COUCH_DISK_VERSION;
    db->header.update_seq = 0;
    db->header.by_id_root = NULL;
    db->header.by_seq_root = NULL;
    db->header.local_docs_root = NULL;
    db->header.purge_seq = 0;
    db->header.purge_ptr = 0;
    db->header.position = 0;
    return write_header(db);
}

LIBCOUCHSTORE_API
uint64_t couchstore_get_header_position(Db *db)
{
    return db->header.position;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_commit(Db *db)
{
    cs_off_t curpos = db->file_pos;
    sized_buf zerobyte = {"\0", 1};
    size_t seqrootsize = 0, idrootsize = 0, localrootsize = 0;
    if (db->header.by_seq_root) {
        seqrootsize = 12 + db->header.by_seq_root->reduce_value.size;
    }
    if (db->header.by_id_root) {
        idrootsize = 12 + db->header.by_id_root->reduce_value.size;
    }
    if (db->header.local_docs_root) {
        localrootsize = 12 + db->header.local_docs_root->reduce_value.size;
    }
    db->file_pos += 25 + seqrootsize + idrootsize + localrootsize;
    //Extend file size to where end of header will land before we do first sync
    db_write_buf(db, &zerobyte, NULL, NULL);

    couchstore_error_t errcode = db->file_ops->sync(db->file_handle);

    //Set the pos back to where it was when we started to write the real header.
    db->file_pos = curpos;
    if (errcode == COUCHSTORE_SUCCESS) {
        errcode = write_header(db);
    }

    if (errcode == COUCHSTORE_SUCCESS) {
        errcode = db->file_ops->sync(db->file_handle);
    }

    return errcode;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_open_db(const char *filename,
                                      couchstore_open_flags flags,
                                      Db **pDb)
{
    return couchstore_open_db_ex(filename, flags,
                                 couchstore_get_default_file_ops(), pDb);
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_open_db_ex(const char *filename,
                                         couchstore_open_flags flags,
                                         const couch_file_ops *ops,
                                         Db **pDb)
{
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    Db *db;
    int openflags;

    /* Sanity check input parameters */
    if (filename == NULL || pDb == NULL || ops == NULL ||
            ops->version != 3 || ops->constructor == NULL || ops->open == NULL ||
            ops->close == NULL || ops->pread == NULL ||
            ops->pwrite == NULL || ops->goto_eof == NULL ||
            ops->sync == NULL || ops->destructor == NULL ||
            ((flags & COUCHSTORE_OPEN_FLAG_RDONLY) &&
             (flags & COUCHSTORE_OPEN_FLAG_CREATE))) {
        return COUCHSTORE_ERROR_INVALID_ARGUMENTS;
    }

    if ((db = calloc(1, sizeof(Db))) == NULL) {
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }

    if (flags & COUCHSTORE_OPEN_FLAG_RDONLY) {
        openflags = O_RDONLY;
    } else {
        openflags = O_RDWR;
    }

    if (flags & COUCHSTORE_OPEN_FLAG_CREATE) {
        openflags |= O_CREAT;
    }

    db->filename = strdup(filename);
    error_unless(db->filename, COUCHSTORE_ERROR_ALLOC_FAIL);

    db->file_ops = couch_get_buffered_file_ops(ops, &db->file_handle);
    error_unless(db->file_ops, COUCHSTORE_ERROR_ALLOC_FAIL);

    error_pass(db->file_ops->open(&db->file_handle, filename, openflags));

    if ((db->file_pos = db->file_ops->goto_eof(db->file_handle)) == 0) {
        /* This is an empty file. Create a new fileheader unless the
         * user wanted a read-only version of the file
         */
        if (flags & COUCHSTORE_OPEN_FLAG_RDONLY) {
            error_pass(COUCHSTORE_ERROR_NO_HEADER);
        } else {
            error_pass(create_header(db));
        }
    } else {
        error_pass(find_header(db));
    }

    *pDb = db;
    return COUCHSTORE_SUCCESS;

cleanup:
    couchstore_close_db(db);
    return errcode;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_close_db(Db *db)
{
    if(db == NULL) {
        return COUCHSTORE_SUCCESS;
    }

    if (db->file_ops) {
        db->file_ops->close(db->file_handle);
        db->file_ops->destructor(db->file_handle);
    }
    free((char*)db->filename);

    free(db->header.by_id_root);
    free(db->header.by_seq_root);
    free(db->header.local_docs_root);

    memset(db, 0xa5, sizeof(*db));
    free(db);

    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
const char* couchstore_get_db_filename(Db *db) {
    return db->filename;
}

static int by_seq_read_docinfo(DocInfo **pInfo, sized_buf *k, sized_buf *v)
{
    const raw_seq_index_value *raw = (const raw_seq_index_value*)v->buf;
    ssize_t extraSize = v->size - sizeof(*raw);
    if (extraSize < 0) {
        return COUCHSTORE_ERROR_CORRUPT;
    }

    uint32_t idsize, datasize;
    decode_kv_length(&raw->sizes, &idsize, &datasize);
    uint64_t bp = decode_raw48(raw->bp);
    int deleted = (bp & BP_DELETED_FLAG) != 0;
    bp &= ~BP_DELETED_FLAG;
    uint8_t content_meta = decode_raw08(raw->content_meta);
    uint64_t rev_seq = decode_raw48(raw->rev_seq);
    uint64_t db_seq = decode_sequence_key(k);

    DocInfo* docInfo = malloc(sizeof(DocInfo) + extraSize);
    if (!docInfo) {
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }
    char *rbuf = (char *) docInfo;
    memcpy(rbuf + sizeof(DocInfo), v->buf + sizeof(*raw), extraSize);
    *pInfo = docInfo;
    docInfo->db_seq = db_seq;
    docInfo->rev_seq = rev_seq;
    docInfo->deleted = deleted;
    docInfo->bp = bp;
    docInfo->size = datasize;
    docInfo->content_meta = content_meta;
    docInfo->id.buf = rbuf + sizeof(DocInfo);
    docInfo->id.size = idsize;
    docInfo->rev_meta.buf = rbuf + sizeof(DocInfo) + idsize;
    docInfo->rev_meta.size = extraSize - idsize;
    return 0;
}

static int by_id_read_docinfo(DocInfo **pInfo, sized_buf *k, sized_buf *v)
{
    const raw_id_index_value *raw = (const raw_id_index_value*)v->buf;
    ssize_t revMetaSize = v->size - sizeof(*raw);
    if (revMetaSize < 0) {
        return COUCHSTORE_ERROR_CORRUPT;
    }

    uint32_t datasize, deleted;
    uint8_t content_meta;
    uint64_t bp, seq, revnum;

    seq = decode_raw48(raw->db_seq);
    datasize = decode_raw32(raw->size);
    bp = decode_raw48(raw->bp);
    deleted = (bp & BP_DELETED_FLAG) != 0;
    bp &= ~BP_DELETED_FLAG;
    content_meta = decode_raw08(raw->content_meta);
    revnum = decode_raw48(raw->rev_seq);

    DocInfo* docInfo = malloc(sizeof(DocInfo) + revMetaSize + k->size);
    if (!docInfo) {
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }
    char *rbuf = (char *) docInfo;
    memcpy(rbuf + sizeof(DocInfo), v->buf + sizeof(*raw), revMetaSize);
    *pInfo = docInfo;
    docInfo->db_seq = seq;
    docInfo->rev_seq = revnum;
    docInfo->deleted = deleted;
    docInfo->bp = bp;
    docInfo->size = datasize;
    docInfo->content_meta = content_meta;
    docInfo->rev_meta.buf = rbuf + sizeof(DocInfo);
    docInfo->rev_meta.size = revMetaSize;
    docInfo->id.buf = docInfo->rev_meta.buf + docInfo->rev_meta.size;
    docInfo->id.size = k->size;
    memcpy(docInfo->id.buf, k->buf, k->size);
    return 0;
}

//Fill in doc from reading file.
static couchstore_error_t bp_to_doc(Doc **pDoc, Db *db, cs_off_t bp, couchstore_open_options options)
{
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    int bodylen = 0;
    char *docbody = NULL;
    fatbuf *docbuf = NULL;

    if (options & DECOMPRESS_DOC_BODIES) {
        bodylen = pread_compressed(db, bp, &docbody);
    } else {
        bodylen = pread_bin(db, bp, &docbody);
    }

    error_unless(bodylen >= 0, bodylen);    // if bodylen is negative it's an error code
    error_unless(docbody || bodylen == 0, COUCHSTORE_ERROR_READ);

    error_unless(docbuf = fatbuf_alloc(sizeof(Doc) + bodylen), COUCHSTORE_ERROR_ALLOC_FAIL);
    *pDoc = (Doc *) fatbuf_get(docbuf, sizeof(Doc));

    if (bodylen == 0) { //Empty doc
        (*pDoc)->data.buf = NULL;
        (*pDoc)->data.size = 0;
        return 0;
    }

    (*pDoc)->data.buf = (char *) fatbuf_get(docbuf, bodylen);
    (*pDoc)->data.size = bodylen;
    memcpy((*pDoc)->data.buf, docbody, bodylen);

cleanup:
    free(docbody);
    if (errcode < 0) {
        fatbuf_free(docbuf);
    }
    return errcode;
}

static couchstore_error_t docinfo_fetch_by_id(couchfile_lookup_request *rq,
                                              void *k,
                                              sized_buf *v)
{
    sized_buf *id = (sized_buf *) k;
    DocInfo **pInfo = (DocInfo **) rq->callback_ctx;
    if (v == NULL) {
        return COUCHSTORE_ERROR_DOC_NOT_FOUND;
    }
    return by_id_read_docinfo(pInfo, id, v);
}

static couchstore_error_t docinfo_fetch_by_seq(couchfile_lookup_request *rq,
                                               void *k,
                                               sized_buf *v)
{
    sized_buf *id = (sized_buf *) k;
    DocInfo **pInfo = (DocInfo **) rq->callback_ctx;
    if (v == NULL) {
        return COUCHSTORE_ERROR_DOC_NOT_FOUND;
    }
    return by_seq_read_docinfo(pInfo, id, v);
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_docinfo_by_id(Db *db,
                                            const void *id,
                                            size_t idlen,
                                            DocInfo **pInfo)
{
    sized_buf key;
    sized_buf *keylist = &key;
    couchfile_lookup_request rq;
    sized_buf cmptmp;
    couchstore_error_t errcode;

    if (db->header.by_id_root == NULL) {
        return COUCHSTORE_ERROR_DOC_NOT_FOUND;
    }

    key.buf = (char *) id;
    key.size = idlen;

    rq.cmp.compare = ebin_cmp;
    rq.cmp.arg = &cmptmp;
    rq.db = db;
    rq.num_keys = 1;
    rq.keys = &keylist;
    rq.callback_ctx = pInfo;
    rq.fetch_callback = docinfo_fetch_by_id;
    rq.node_callback = NULL;
    rq.fold = 0;

    errcode = btree_lookup(&rq, db->header.by_id_root->pointer);
    if (errcode == COUCHSTORE_SUCCESS) {
        if (*pInfo == NULL) {
            errcode = COUCHSTORE_ERROR_DOC_NOT_FOUND;
        }
    }
    return errcode;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_docinfo_by_sequence(Db *db,
                                                  uint64_t sequence,
                                                  DocInfo **pInfo)
{
    sized_buf key;
    sized_buf *keylist = &key;
    couchfile_lookup_request rq;
    sized_buf cmptmp;
    couchstore_error_t errcode;

    if (db->header.by_id_root == NULL) {
        return COUCHSTORE_ERROR_DOC_NOT_FOUND;
    }

    sequence = htonll(sequence);
    key.buf = (char *)&sequence + 2;
    key.size = 6;

    rq.cmp.compare = seq_cmp;
    rq.cmp.arg = &cmptmp;
    rq.db = db;
    rq.num_keys = 1;
    rq.keys = &keylist;
    rq.callback_ctx = pInfo;
    rq.fetch_callback = docinfo_fetch_by_seq;
    rq.node_callback = NULL;
    rq.fold = 0;

    errcode = btree_lookup(&rq, db->header.by_seq_root->pointer);
    if (errcode == COUCHSTORE_SUCCESS) {
        if (*pInfo == NULL) {
            errcode = COUCHSTORE_ERROR_DOC_NOT_FOUND;
        }
    }
    return errcode;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_open_doc_with_docinfo(Db *db,
                                                    DocInfo *docinfo,
                                                    Doc **pDoc,
                                                    couchstore_open_options options)
{
    couchstore_error_t errcode;

    *pDoc = NULL;
    if (docinfo->bp == 0) {
        return COUCHSTORE_ERROR_DOC_NOT_FOUND;
    }

    if (!(docinfo->content_meta & COUCH_DOC_IS_COMPRESSED)) {
        options &= ~DECOMPRESS_DOC_BODIES;
    }

    errcode = bp_to_doc(pDoc, db, docinfo->bp, options);
    if (errcode == COUCHSTORE_SUCCESS) {
        (*pDoc)->id.buf = docinfo->id.buf;
        (*pDoc)->id.size = docinfo->id.size;
    }

    return errcode;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_open_document(Db *db,
                                            const void *id,
                                            size_t idlen,
                                            Doc **pDoc,
                                            couchstore_open_options options)
{
    couchstore_error_t errcode;
    DocInfo *info;

    *pDoc = NULL;
    errcode = couchstore_docinfo_by_id(db, id, idlen, &info);
    if (errcode == COUCHSTORE_SUCCESS) {
        errcode = couchstore_open_doc_with_docinfo(db, info, pDoc, options);
        if (errcode == COUCHSTORE_SUCCESS) {
            (*pDoc)->id.buf = (char *) id;
            (*pDoc)->id.size = idlen;
        }

        couchstore_free_docinfo(info);
    }

    return errcode;
}

// context info passed to lookup_callback via btree_lookup
typedef struct {
    Db *db;
    couchstore_docinfos_options options;
    couchstore_changes_callback_fn callback;
    void* callback_context;
    int by_id;
    int depth;
    couchstore_walk_tree_callback_fn walk_callback;
} lookup_context;

// btree_lookup callback, called while iterating keys
static couchstore_error_t lookup_callback(couchfile_lookup_request *rq,
                                          void *k, sized_buf *v)
{
    if (v == NULL) {
        return COUCHSTORE_SUCCESS;
    }

    const lookup_context *context = rq->callback_ctx;
    sized_buf *seqterm = (sized_buf *) k;
    DocInfo *docinfo = NULL;
    couchstore_error_t errcode;
    if (context->by_id) {
        errcode = by_id_read_docinfo(&docinfo, seqterm, v);
    } else {
        errcode = by_seq_read_docinfo(&docinfo, seqterm, v);
    }
    if (errcode == COUCHSTORE_ERROR_CORRUPT && (context->options & COUCHSTORE_INCLUDE_CORRUPT_DOCS)) {
        // Invoke callback even if doc info is corrupted/unreadable, if magic flag is set
        docinfo = calloc(sizeof(DocInfo), 1);
        docinfo->id = *seqterm;
        docinfo->rev_meta = *v;
    } else if (errcode) {
        return errcode;
    }

    if ((context->options & COUCHSTORE_DELETES_ONLY) && docinfo->deleted == 0) {
        couchstore_free_docinfo(docinfo);
        return COUCHSTORE_SUCCESS;
    }

    if ((context->options & COUCHSTORE_NO_DELETES) && docinfo->deleted == 1) {
        couchstore_free_docinfo(docinfo);
        return COUCHSTORE_SUCCESS;
    }

    if (context->walk_callback) {
        errcode = context->walk_callback(context->db,
                                         context->depth,
                                         docinfo,
                                         0,
                                         NULL,
                                         context->callback_context);
    } else {
        errcode = context->callback(context->db, docinfo, context->callback_context);
    }
    if (errcode <= 0) {
        couchstore_free_docinfo(docinfo);
    } else {
        // User requested docinfo not be freed, don't free it, return success
        return COUCHSTORE_SUCCESS;
    }
    return errcode;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_changes_since(Db *db,
                                            uint64_t since,
                                            couchstore_docinfos_options options,
                                            couchstore_changes_callback_fn callback,
                                            void *ctx)
{
    char since_termbuf[6];
    sized_buf since_term;
    sized_buf *keylist = &since_term;
    lookup_context cbctx = {db, options, callback, ctx, 0, 0, NULL};
    couchfile_lookup_request rq;
    sized_buf cmptmp;
    couchstore_error_t errcode;

    if (db->header.by_seq_root == NULL) {
        return COUCHSTORE_SUCCESS;
    }

    since_term.buf = since_termbuf;
    since_term.size = 6;
    *(raw_48*)since_term.buf = encode_raw48(since);

    rq.cmp.compare = seq_cmp;
    rq.cmp.arg = &cmptmp;
    rq.db = db;
    rq.num_keys = 1;
    rq.keys = &keylist;
    rq.callback_ctx = &cbctx;
    rq.fetch_callback = lookup_callback;
    rq.node_callback = NULL;
    rq.fold = 1;

    errcode = btree_lookup(&rq, db->header.by_seq_root->pointer);
    return errcode;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_all_docs(Db *db,
                                       const sized_buf* startKeyPtr,
                                       couchstore_docinfos_options options,
                                       couchstore_changes_callback_fn callback,
                                       void *ctx)
{
    sized_buf startKey = {NULL, 0};
    sized_buf *keylist = &startKey;
    lookup_context cbctx = {db, options, callback, ctx, 1, 0, NULL};
    couchfile_lookup_request rq;
    sized_buf cmptmp;
    couchstore_error_t errcode;

    if (db->header.by_id_root == NULL) {
        return COUCHSTORE_SUCCESS;
    }

    if (startKeyPtr) {
        startKey = *startKeyPtr;
    }

    rq.cmp.compare = ebin_cmp;
    rq.cmp.arg = &cmptmp;
    rq.db = db;
    rq.num_keys = 1;
    rq.keys = &keylist;
    rq.callback_ctx = &cbctx;
    rq.fetch_callback = lookup_callback;
    rq.node_callback = NULL;
    rq.fold = 1;

    errcode = btree_lookup(&rq, db->header.by_id_root->pointer);
    return errcode;
}

static couchstore_error_t walk_node_callback(struct couchfile_lookup_request *rq,
                                                 uint64_t subtreeSize,
                                                 const sized_buf *reduceValue)
{
    lookup_context* context = rq->callback_ctx;
    if (reduceValue) {
        int result = context->walk_callback(context->db,
                                            context->depth,
                                            NULL,
                                            subtreeSize,
                                            reduceValue,
                                            context->callback_context);
        context->depth++;
        if (result < 0)
            return result;
    } else {
        context->depth--;
    }
    return COUCHSTORE_SUCCESS;
}

static
couchstore_error_t couchstore_walk_tree(Db *db,
                                        int by_id,
                                        const node_pointer* root,
                                        const sized_buf* startKeyPtr,
                                        couchstore_docinfos_options options,
                                        int (*compare)(const sized_buf *k1, const sized_buf *k2),
                                        couchstore_walk_tree_callback_fn callback,
                                        void *ctx)
{
    couchstore_error_t errcode;

    if (root == NULL) {
        return COUCHSTORE_SUCCESS;
    }

    // Invoke the callback on the root node:
    errcode = callback(db, 0, NULL,
                       root->subtreesize,
                       &root->reduce_value,
                       ctx);
    if (errcode < 0) {
        return errcode;
    }

    sized_buf startKey = {NULL, 0};
    if (startKeyPtr) {
        startKey = *startKeyPtr;
    }
    sized_buf *keylist = &startKey;

    lookup_context lookup_ctx = {db, options, NULL, ctx, by_id, 1, callback};
    sized_buf cmptmp;
    couchfile_lookup_request rq;
    
    rq.cmp.compare = compare;
    rq.cmp.arg = &cmptmp;
    rq.db = db;
    rq.num_keys = 1;
    rq.keys = &keylist;
    rq.callback_ctx = &lookup_ctx;
    rq.fetch_callback = lookup_callback;
    rq.node_callback = walk_node_callback;
    rq.fold = 1;

    return btree_lookup(&rq, root->pointer);
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_walk_id_tree(Db *db,
                                           const sized_buf* startDocID,
                                           couchstore_docinfos_options options,
                                           couchstore_walk_tree_callback_fn callback,
                                           void *ctx)
{
    return couchstore_walk_tree(db, 1, db->header.by_id_root, startDocID,
                                options, ebin_cmp, callback, ctx);
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_walk_seq_tree(Db *db,
                                           uint64_t startSequence,
                                           couchstore_docinfos_options options,
                                           couchstore_walk_tree_callback_fn callback,
                                           void *ctx)
{
    raw_48 start_termbuf = encode_raw48(startSequence);
    sized_buf start_term = {(char*)&start_termbuf, 6};

    return couchstore_walk_tree(db, 0, db->header.by_seq_root, &start_term,
                                options, seq_cmp, callback, ctx);
}

static int id_ptr_cmp(const void *a, const void *b)
{
    sized_buf **buf1 = (sized_buf**) a;
    sized_buf **buf2 = (sized_buf**) b;
    return ebin_cmp(*buf1, *buf2);
}

static int seq_ptr_cmp(const void *a, const void *b)
{
    sized_buf **buf1 = (sized_buf**) a;
    sized_buf **buf2 = (sized_buf**) b;
    return seq_cmp(*buf1, *buf2);
}

// Common subroutine of couchstore_docinfos_by_{ids, sequence}
static couchstore_error_t iterate_docinfos(Db *db,
                                           const sized_buf keys[],
                                           unsigned numDocs,
                                           node_pointer *tree,
                                           int (*key_ptr_compare)(const void *, const void *),
                                           int (*key_compare)(const sized_buf *k1, const sized_buf *k2),
                                           couchstore_changes_callback_fn callback,
                                           int fold,
                                           void *ctx)
{
    // Nothing to do if the tree is empty
    if (tree == NULL) {
        return COUCHSTORE_SUCCESS;
    }
    
    // Create an array of *pointers to* sized_bufs, which is what btree_lookup wants:
    const sized_buf **keyptrs = malloc(numDocs * sizeof(sized_buf*));
    if (!keyptrs) {
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }
    unsigned i;
    for (i = 0; i< numDocs; ++i) {
        keyptrs[i] = &keys[i];
    }
    if (!fold) {
        // Sort the key pointers:
        qsort(keyptrs, numDocs, sizeof(keyptrs[0]), key_ptr_compare);
    }

    // Construct the lookup request:
    lookup_context cbctx = {db, 0, callback, ctx, (tree == db->header.by_id_root), 0, NULL};
    couchfile_lookup_request rq;
    sized_buf cmptmp;
    rq.cmp.compare = key_compare;
    rq.cmp.arg = &cmptmp;
    rq.db = db;
    rq.num_keys = numDocs;
    rq.keys = (sized_buf**) keyptrs;
    rq.callback_ctx = &cbctx;
    rq.fetch_callback = lookup_callback;
    rq.node_callback = NULL;
    rq.fold = fold;
    
    // Go!
    couchstore_error_t errcode = btree_lookup(&rq, tree->pointer);
    
    free(keyptrs);
    return errcode;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_docinfos_by_id(Db *db,
                                             const sized_buf ids[],
                                             unsigned numDocs,
                                             couchstore_docinfos_options options,
                                             couchstore_changes_callback_fn callback,
                                             void *ctx)
{
    return iterate_docinfos(db, ids, numDocs,
                            db->header.by_id_root, id_ptr_cmp, ebin_cmp,
                            callback,
                            (options & RANGES) != 0,
                            ctx);
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_docinfos_by_sequence(Db *db,
                                                   const uint64_t sequence[],
                                                   unsigned numDocs,
                                                   couchstore_docinfos_options options,
                                                   couchstore_changes_callback_fn callback,
                                                   void *ctx)
{
    // Create the array of keys:
    sized_buf *keylist = malloc(numDocs * sizeof(sized_buf));
    raw_by_seq_key *keyvalues = malloc(numDocs * sizeof(raw_by_seq_key));
    couchstore_error_t errcode;
    error_unless(keylist && keyvalues, COUCHSTORE_ERROR_ALLOC_FAIL);
    unsigned i;
    for (i = 0; i< numDocs; ++i) {
        keyvalues[i].sequence = encode_raw48(sequence[i]);
        keylist[i].buf = (void*) &keyvalues[i];
        keylist[i].size = sizeof(keyvalues[i]);
    }
    
    error_pass(iterate_docinfos(db, keylist, numDocs,
                                db->header.by_seq_root, seq_ptr_cmp, seq_cmp,
                                callback,
                                (options & RANGES) != 0,
                                ctx));
cleanup:
    free(keylist);
    free(keyvalues);
    return errcode;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_db_info(Db *db, DbInfo* dbinfo) {
    const node_pointer *id_root = db->header.by_id_root;
    const node_pointer *seq_root = db->header.by_seq_root;
    const node_pointer *local_root = db->header.local_docs_root;
    dbinfo->filename = db->filename;
    dbinfo->header_position = db->header.position;
    dbinfo->last_sequence = db->header.update_seq;
    dbinfo->deleted_count = dbinfo->doc_count = dbinfo->space_used = 0;
    if (id_root) {
        raw_by_id_reduce* id_reduce = (raw_by_id_reduce*) id_root->reduce_value.buf;
        dbinfo->doc_count = decode_raw40(id_reduce->notdeleted);
        dbinfo->deleted_count = decode_raw40(id_reduce->deleted);
        dbinfo->space_used = decode_raw48(id_reduce->size);
        dbinfo->space_used += id_root->subtreesize;
    }
    if(seq_root) {
        dbinfo->space_used += seq_root->subtreesize;
    }
    if(local_root) {
        dbinfo->space_used += local_root->subtreesize;
    }
    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
void couchstore_free_document(Doc *doc)
{
    if (doc) {
        char *offset = (char *) (&((fatbuf *) NULL)->buf);
        fatbuf_free((fatbuf *) ((char *)doc - (char *)offset));
    }
}

LIBCOUCHSTORE_API
void couchstore_free_docinfo(DocInfo *docinfo)
{
    free(docinfo);
}

static couchstore_error_t local_doc_fetch(couchfile_lookup_request *rq,
                                          void *k,
                                          sized_buf *v)
{
    sized_buf *id = (sized_buf *) k;
    LocalDoc **lDoc = (LocalDoc **) rq->callback_ctx;
    LocalDoc *dp;

    if (!v) {
        *lDoc = NULL;
        return COUCHSTORE_SUCCESS;
    }
    fatbuf *ldbuf = fatbuf_alloc(sizeof(LocalDoc) + id->size + v->size);
    if (ldbuf == NULL) {
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }

    dp = *lDoc = (LocalDoc *) fatbuf_get(ldbuf, sizeof(LocalDoc));
    dp->id.buf = (char *) fatbuf_get(ldbuf, id->size);
    dp->id.size = id->size;

    dp->json.buf = (char *) fatbuf_get(ldbuf, v->size);
    dp->json.size = v->size;

    dp->deleted = 0;

    memcpy(dp->id.buf, id->buf, id->size);
    memcpy(dp->json.buf, v->buf, v->size);

    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_open_local_document(Db *db,
                                                  const void *id,
                                                  size_t idlen,
                                                  LocalDoc **pDoc)
{
    sized_buf key;
    sized_buf *keylist = &key;
    couchfile_lookup_request rq;
    sized_buf cmptmp;
    couchstore_error_t errcode;

    if (db->header.local_docs_root == NULL) {
        return COUCHSTORE_ERROR_DOC_NOT_FOUND;
    }

    key.buf = (char *) id;
    key.size = idlen;

    rq.cmp.compare = ebin_cmp;
    rq.cmp.arg = &cmptmp;
    rq.db = db;
    rq.num_keys = 1;
    rq.keys = &keylist;
    rq.callback_ctx = pDoc;
    rq.fetch_callback = local_doc_fetch;
    rq.node_callback = NULL;
    rq.fold = 0;

    errcode = btree_lookup(&rq, db->header.local_docs_root->pointer);
    if (errcode == COUCHSTORE_SUCCESS) {
        if (*pDoc == NULL) {
            errcode = COUCHSTORE_ERROR_DOC_NOT_FOUND;
        }
    }
    return errcode;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_save_local_document(Db *db, LocalDoc *lDoc)
{
    couchstore_error_t errcode;
    couchfile_modify_action ldupdate;
    sized_buf cmptmp;
    node_pointer *nroot = NULL;

    if (lDoc->deleted) {
        ldupdate.type = ACTION_REMOVE;
    } else {
        ldupdate.type = ACTION_INSERT;
    }

    ldupdate.key = &lDoc->id;
    ldupdate.value.data = &lDoc->json;

    couchfile_modify_request rq;
    rq.cmp.compare = ebin_cmp;
    rq.cmp.arg = &cmptmp;
    rq.num_actions = 1;
    rq.actions = &ldupdate;
    rq.fetch_callback = NULL;
    rq.reduce = NULL;
    rq.rereduce = NULL;
    rq.db = db;
    rq.compacting = 0;

    nroot = modify_btree(&rq, db->header.local_docs_root, &errcode);
    if (errcode == COUCHSTORE_SUCCESS && nroot != db->header.local_docs_root) {
        free(db->header.local_docs_root);
        db->header.local_docs_root = nroot;
    }

    return errcode;
}

LIBCOUCHSTORE_API
void couchstore_free_local_document(LocalDoc *lDoc)
{
    if (lDoc) {
        char *offset = (char *) (&((fatbuf *) NULL)->buf);
        fatbuf_free((fatbuf *) ((char *)lDoc - (char *)offset));
    }
}

pthread_key_t os_err_key;
pthread_once_t os_err_init = PTHREAD_ONCE_INIT;

static void os_error_destroy(void* err_ptr) {
    free(err_ptr);
}

static void init_os_error_key(void) {
    pthread_key_create(&os_err_key, os_error_destroy);
}

struct _os_error *get_os_error_store(void) {
    int once = pthread_once(&os_err_init, init_os_error_key);
    assert(once == 0);
    void *ptr;
    if((ptr = pthread_getspecific(os_err_key)) == NULL) {
        ptr = calloc(1, sizeof(struct _os_error));
        pthread_setspecific(os_err_key, ptr);
    }
    return ptr;
}

LIBCOUCHSTORE_API
void couchstore_last_os_error(char* buf, size_t size) {
    struct _os_error *err = get_os_error_store();
#ifndef WINDOWS
    snprintf(buf, size, "errno = %d: `%s'", err->errno_err, strerror(err->errno_err));
#else
    char* win_msg = NULL;
    FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER |
                   FORMAT_MESSAGE_FROM_SYSTEM |
                   FORMAT_MESSAGE_IGNORE_INSERTS,
                   NULL, err->win_err,
                   MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                   (LPTSTR) &win_msg,
                   0, NULL);
    snprintf(buf, size, "errno = %d: `%s', WINAPI error = %d: `%s'",
                        err->errno_err, strerror(err->errno_err),
                        err->win_err, win_msg);
    LocalFree(win_msg);
#endif
}
