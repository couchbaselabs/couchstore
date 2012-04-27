/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>

#include "internal.h"
#include "couch_btree.h"
#include "bitfield.h"
#include "util.h"
#include "iobuffer.h"

sized_buf nil_atom = {
    (char *) "\x64\x00\x03nil",
    6
};

static couchstore_error_t find_header(Db *db)
{
    uint64_t block = db->file_pos / COUCH_BLOCK_SIZE;
    int errcode = COUCHSTORE_SUCCESS;
    ssize_t readsize;
    char *header_buf = NULL;
    uint8_t buf[2];

    while (1) {
        readsize = db->file_ops->pread(db->file_handle, buf, 2, block * COUCH_BLOCK_SIZE);
        if (readsize == 2 && buf[0] == 1) {
            //Found a header block.
            int header_len = pread_header(db, block * COUCH_BLOCK_SIZE, &header_buf);
            if (header_len > 0) {
                db->header.disk_version = header_buf[0];
                error_unless(db->header.disk_version == COUCH_DISK_VERSION, COUCHSTORE_ERROR_HEADER_VERSION)
                db->header.update_seq = get_48(header_buf + 1);
                db->header.purge_seq = get_48(header_buf + 7);
                db->header.purge_ptr = get_48(header_buf + 13);
                int seqrootsize = get_16(header_buf + 19);
                int idrootsize = get_16(header_buf + 21);
                int localrootsize = get_16(header_buf + 23);
                int rootpos = 25;
                if (seqrootsize > 0) {
                    db->header.by_seq_root = read_root(header_buf + rootpos, seqrootsize);
                } else {
                    db->header.by_seq_root = NULL;
                }
                rootpos += seqrootsize;
                if (idrootsize > 0) {
                    db->header.by_id_root = read_root(header_buf + rootpos, idrootsize);
                } else {
                    db->header.by_id_root = NULL;
                }
                rootpos += idrootsize;
                if (localrootsize > 0) {
                    db->header.local_docs_root = read_root(header_buf + rootpos, localrootsize);
                } else {
                    db->header.local_docs_root = NULL;
                }
                db->header.position = block * COUCH_BLOCK_SIZE;
                break;
            }
        }

        if (block == 0) {
            /*
             * We've read all of the blocks in the file from the end up to
             * the beginning, and we still haven't found a header.
             */
            return COUCHSTORE_ERROR_NO_HEADER;
        }
        block--;
    }

cleanup:
    free(header_buf);
    return errcode;
}

static couchstore_error_t write_header(Db *db)
{
    sized_buf writebuf;
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
    writebuf.size = 25 + seqrootsize + idrootsize + localrootsize;
    writebuf.buf = (char *) calloc(1, writebuf.size);
    writebuf.buf[0] = COUCH_DISK_VERSION;
    set_bits(writebuf.buf + 1, 0, 48, db->header.update_seq);
    set_bits(writebuf.buf + 7, 0, 48, db->header.purge_seq);
    set_bits(writebuf.buf + 13, 0, 48, db->header.purge_ptr);
    set_bits(writebuf.buf + 19, 0, 16, seqrootsize);
    set_bits(writebuf.buf + 21, 0, 16, idrootsize);
    set_bits(writebuf.buf + 23, 0, 16, localrootsize);
    encode_root(writebuf.buf + 25, db->header.by_seq_root);
    encode_root(writebuf.buf + 25 + seqrootsize, db->header.by_id_root);
    encode_root(writebuf.buf + 25 + seqrootsize + idrootsize,
                db->header.local_docs_root);
    off_t pos;
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
    couchstore_error_t errcode = write_header(db);
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
                                 couch_get_default_file_ops(), pDb);
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
            ops->version != 2 || ops->constructor == NULL || ops->open == NULL ||
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
            error_pass(COUCHSTORE_ERROR_CHECKSUM_FAIL);
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
    uint32_t idsize, datasize, deleted, revnum;
    uint8_t content_meta;
    uint64_t bp, seq;
    get_kvlen(v->buf, &idsize, &datasize);
    deleted = (v->buf[5] & 0x80) != 0;
    bp = get_48(v->buf + 5) &~ 0x800000000000;
    content_meta = v->buf[11];
    revnum = get_32(v->buf + 12);
    seq = get_48(k->buf);
    char *rbuf = (char *) malloc(sizeof(DocInfo) + (v->size - 16));
    if (!rbuf) {
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }
    memcpy(rbuf + sizeof(DocInfo), v->buf + 16, v->size - 16);
    *pInfo = (DocInfo *) rbuf;
    (*pInfo)->db_seq = seq;
    (*pInfo)->rev_seq = revnum;
    (*pInfo)->deleted = deleted;
    (*pInfo)->bp = bp;
    (*pInfo)->size = datasize;
    (*pInfo)->content_meta = content_meta;
    (*pInfo)->id.buf = rbuf + sizeof(DocInfo);
    (*pInfo)->id.size = idsize;
    (*pInfo)->rev_meta.buf = rbuf + sizeof(DocInfo) + idsize;
    (*pInfo)->rev_meta.size = v->size - 16 - idsize;
    return 0;
}

static int by_id_read_docinfo(DocInfo **pInfo, sized_buf *k, sized_buf *v)
{
    uint32_t datasize, deleted, revnum;
    uint8_t content_meta;
    uint64_t bp, seq;
    seq = get_48(v->buf);
    datasize = get_32(v->buf + 6);
    deleted = (v->buf[10] & 0x80) != 0;
    bp = get_48(v->buf + 10) &~ 0x800000000000;
    content_meta = v->buf[16];
    revnum = get_32(v->buf + 17);
    char *rbuf = (char *) malloc(sizeof(DocInfo) + (v->size - 21) + k->size);
    if (!rbuf) {
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }
    memcpy(rbuf + sizeof(DocInfo), v->buf + 21, v->size - 21);
    *pInfo = (DocInfo *) rbuf;
    (*pInfo)->db_seq = seq;
    (*pInfo)->rev_seq = revnum;
    (*pInfo)->deleted = deleted;
    (*pInfo)->bp = bp;
    (*pInfo)->size = datasize;
    (*pInfo)->content_meta = content_meta;
    (*pInfo)->rev_meta.buf = rbuf + sizeof(DocInfo);
    (*pInfo)->rev_meta.size = v->size - 21;
    (*pInfo)->id.buf = (*pInfo)->rev_meta.buf + (*pInfo)->rev_meta.size;
    (*pInfo)->id.size = k->size;
    memcpy((*pInfo)->id.buf, k->buf, k->size);
    return 0;
}

//Fill in doc from reading file.
static couchstore_error_t bp_to_doc(Doc **pDoc, Db *db, off_t bp, couchstore_open_options options)
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

/*
 * ISO C forbids conversion of function pointer to object pointer type, but
 * a union allows us to do this ;-)
 */
union c99hack {
    couchstore_changes_callback_fn callback;
    void *voidptr;
};

static couchstore_error_t byseq_do_callback(couchfile_lookup_request *rq,
                                            void *k, sized_buf *v)
{
    if (v == NULL) {
        return COUCHSTORE_SUCCESS;
    }

    sized_buf *seqterm = (sized_buf *) k;
    DocInfo *docinfo = NULL;
    by_seq_read_docinfo(&docinfo, seqterm, v);

    union c99hack hack;
    hack.voidptr = ((void **)rq->callback_ctx)[0];
    Db *db = ((void **)rq->callback_ctx)[1];
    void *ctx = ((void **)rq->callback_ctx)[2];

    if (hack.callback(db, docinfo, ctx) == 0) {
        couchstore_free_docinfo(docinfo);
    }

    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_changes_since(Db *db,
                                            uint64_t since,
                                            uint64_t options,
                                            couchstore_changes_callback_fn callback,
                                            void *ctx)
{
    (void) options;
    char since_termbuf[6];
    sized_buf since_term;
    sized_buf *keylist = &since_term;
    void *cbctx[3];
    couchfile_lookup_request rq;
    sized_buf cmptmp;
    couchstore_error_t errcode;
    union c99hack hack;
    hack.callback = callback;

    if (db->header.by_seq_root == NULL) {
        return COUCHSTORE_SUCCESS;
    }

    since_term.buf = since_termbuf;
    since_term.size = 6;
    memset(since_term.buf, 0, 6);
    set_bits(since_term.buf, 0, 48, since);

    cbctx[0] = hack.voidptr;
    cbctx[1] = db;
    cbctx[2] = ctx;

    rq.cmp.compare = seq_cmp;
    rq.cmp.arg = &cmptmp;
    rq.db = db;
    rq.num_keys = 1;
    rq.keys = &keylist;
    rq.callback_ctx = cbctx;
    rq.fetch_callback = byseq_do_callback;
    rq.fold = 1;

    errcode = btree_lookup(&rq, db->header.by_seq_root->pointer);
    return errcode;
}

static int seq_ptr_cmp(const void *a, const void *b)
{
    sized_buf **buf1 = (sized_buf**) a;
    sized_buf **buf2 = (sized_buf**) b;
    return seq_cmp(*buf1, *buf2);
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_docinfos_by_sequence(Db *db,
                                                   const uint64_t sequence[],
                                                   unsigned numDocs,
                                                   uint64_t options,
                                                   couchstore_changes_callback_fn callback,
                                                   void *ctx)
{
    (void) options;
    sized_buf *keylist = NULL;
    char *keyvalues = NULL;
    sized_buf **keyptrs = NULL;
    couchfile_lookup_request rq;
    sized_buf cmptmp;
    couchstore_error_t errcode;

    if (db->header.by_id_root == NULL) {
        return COUCHSTORE_ERROR_DOC_NOT_FOUND;
    }

    // Create the array of keys:
    keylist = malloc(numDocs * sizeof(sized_buf));
    keyvalues = calloc(numDocs, 6);
    keyptrs = malloc(numDocs * sizeof(sized_buf*));
    error_unless(keylist && keyvalues && keyptrs, COUCHSTORE_ERROR_ALLOC_FAIL);
    unsigned i;
    for (i = 0; i< numDocs; ++i) {
        keylist[i].buf = keyvalues + 6 * i;
        keylist[i].size = 6;
        set_bits(keylist[i].buf, 0, 48, sequence[i]);
        keyptrs[i] = &keylist[i];
    }
    qsort(keyptrs, numDocs, sizeof(keyptrs[0]), &seq_ptr_cmp);

    union c99hack hack;
    hack.callback = callback;
    void *cbctx[3];
    cbctx[0] = hack.voidptr;
    cbctx[1] = db;
    cbctx[2] = ctx;

    rq.cmp.compare = seq_cmp;
    rq.cmp.arg = &cmptmp;
    rq.db = db;
    rq.num_keys = numDocs;
    rq.keys = keyptrs;
    rq.callback_ctx = cbctx;
    rq.fetch_callback = byseq_do_callback;
    rq.fold = 0;

    error_pass(btree_lookup(&rq, db->header.by_seq_root->pointer));

cleanup:
    free(keylist);
    free(keyvalues);
    return errcode;
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

    ldupdate.cmp_key = &lDoc->id;
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
