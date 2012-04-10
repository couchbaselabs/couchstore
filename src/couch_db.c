/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <snappy-c.h>

#include "internal.h"
#include "couch_btree.h"
#include "bitfield.h"
#include "util.h"
#include "reduces.h"

#define SNAPPY_META_FLAG 128

sized_buf nil_atom = {
    (char *) "\x64\x00\x03nil",
    6
};

int by_id_read_docinfo(DocInfo **pInfo, sized_buf *k, sized_buf *v);
int assemble_id_index_value(DocInfo *docinfo, char *dst);

static couchstore_error_t find_header(Db *db)
{
    uint64_t block = db->file_pos / COUCH_BLOCK_SIZE;
    int errcode = COUCHSTORE_SUCCESS;
    int readsize;
    char *header_buf = NULL;
    uint8_t buf[2];

    while (1) {
        readsize = db->file_ops->pread(db, buf, 2, block * COUCH_BLOCK_SIZE);
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
    int seqrootsize = 0, idrootsize = 0, localrootsize = 0;
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
        errcode = db->file_ops->sync(db);
    }

    return errcode;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_open_db(const char *filename,
                                      uint64_t flags,
                                      Db **pDb)
{
    return couchstore_open_db_ex(filename, flags,
                                 couch_get_default_file_ops(), pDb);
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_open_db_ex(const char *filename,
                                         uint64_t flags,
                                         couch_file_ops *ops,
                                         Db **pDb)
{
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    Db *db;
    int openflags;

    /* Sanity check input parameters */
    if (filename == NULL || pDb == NULL || ops == NULL ||
            ops->version != 1 || ops->open == NULL ||
            ops->close == NULL || ops->pread == NULL ||
            ops->pwrite == NULL || ops->goto_eof == NULL ||
            ops->sync == NULL || ops->destructor == NULL ||
            ((flags & COUCHSTORE_OPEN_FLAG_RDONLY) &&
             (flags & COUCHSTORE_OPEN_FLAG_CREATE))) {
        return COUCHSTORE_ERROR_INVALID_ARGUMENTS;
    }

    if ((db = malloc(sizeof(Db))) == NULL) {
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

    db->file_ops = ops;
    errcode = db->file_ops->open(db, filename, openflags);
    if (errcode != COUCHSTORE_SUCCESS) {
        db->file_ops->destructor(db);
        free(db);
        return errcode;
    }

    if ((db->file_pos = db->file_ops->goto_eof(db)) == 0) {
        /* This is an empty file. Create a new fileheader unless the
         * user wanted a read-only version of the file
         */
        if (flags & COUCHSTORE_OPEN_FLAG_RDONLY) {
            errcode = COUCHSTORE_ERROR_CHECKSUM_FAIL;
        } else {
            errcode = create_header(db);
        }
    } else {
        errcode = find_header(db);
    }

    if (errcode == COUCHSTORE_SUCCESS) {
        *pDb = db;
    } else {
        db->file_ops->close(db);
        db->file_ops->destructor(db);
        free(db);
    }

    return errcode;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_close_db(Db *db)
{
    db->file_ops->close(db);
    db->file_ops->destructor(db);

    free(db->header.by_id_root);
    free(db->header.by_seq_root);
    free(db->header.local_docs_root);

    memset(db, 0xa5, sizeof(*db));
    free(db);

    return COUCHSTORE_SUCCESS;
}

static int ebin_cmp(sized_buf *e1, sized_buf *e2)
{
    size_t size;
    if (e2->size < e1->size) {
        size = e2->size;
    } else {
        size = e1->size;
    }

    int cmp = memcmp(e1->buf, e2->buf, size);
    if (cmp == 0) {
        if (size < e2->size) {
            return -1;
        } else if (size < e1->size) {
            return 1;
        }
    }
    return cmp;
}

static int seq_cmp(sized_buf *k1, sized_buf *k2)
{
    uint64_t e1val = get_48(k1->buf);
    uint64_t e2val = get_48(k2->buf);
    if (e1val == e2val) {
        return 0;
    }
    return (e1val < e2val ? -1 : 1);
}

static int by_seq_read_docinfo(DocInfo **pInfo, sized_buf *k, sized_buf *v)
{
    uint32_t idsize, datasize, deleted, revnum;
    uint8_t content_meta;
    uint64_t bp, seq;
    get_kvlen(v->buf, &idsize, &datasize);
    deleted = v->buf[5] >> 7;
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

int by_id_read_docinfo(DocInfo **pInfo, sized_buf *k, sized_buf *v)
{
    uint32_t datasize, deleted, revnum;
    uint8_t content_meta;
    uint64_t bp, seq;
    seq = get_48(v->buf);
    datasize = get_32(v->buf + 6);
    deleted = v->buf[10] >> 7;
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

#define COMPRESSED_BODY 1
//Fill in doc from reading file.
static couchstore_error_t bp_to_doc(Doc **pDoc, Db *db, off_t bp, uint64_t options)
{
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    int bodylen = 0;
    char *docbody = NULL;
    fatbuf *docbuf = NULL;

    if (options & COMPRESSED_BODY) {
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

static couchstore_error_t docinfo_fetch(couchfile_lookup_request *rq,
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
    rq.fetch_callback = docinfo_fetch;
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
couchstore_error_t couchstore_open_doc_with_docinfo(Db *db,
                                                    DocInfo *docinfo,
                                                    Doc **pDoc,
                                                    uint64_t options)
{
    couchstore_error_t errcode;

    *pDoc = NULL;
    if (docinfo->bp == 0) {
        return COUCHSTORE_ERROR_DOC_NOT_FOUND;
    }

    int readopts = 0;
    if ((options & DECOMPRESS_DOC_BODIES) && (docinfo->content_meta & SNAPPY_META_FLAG)) {
        readopts = COMPRESSED_BODY;
    }

    errcode = bp_to_doc(pDoc, db, docinfo->bp, readopts);
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
                                            uint64_t options)
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

static int assemble_seq_index_value(DocInfo *docinfo, char *dst)
{
    memset(dst, 0, 16);
    set_bits(dst, 0, 12, docinfo->id.size);
    set_bits(dst + 1, 4, 28, docinfo->size);
    set_bits(dst + 5, 0, 1, docinfo->deleted);
    set_bits(dst + 5, 1, 47, docinfo->bp);
    dst[11] = docinfo->content_meta;
    set_bits(dst + 12, 0, 32, docinfo->rev_seq); //16 bytes in.
    memcpy(dst + 16, docinfo->id.buf, docinfo->id.size);
    memcpy(dst + 16 + docinfo->id.size, docinfo->rev_meta.buf,
           docinfo->rev_meta.size);
    return 16 + docinfo->id.size + docinfo->rev_meta.size;
}

int assemble_id_index_value(DocInfo *docinfo, char *dst)
{
    memset(dst, 0, 21);
    set_bits(dst, 0, 48, docinfo->db_seq);
    set_bits(dst + 6, 0, 32, docinfo->size);
    set_bits(dst + 10, 0, 1, docinfo->deleted);
    set_bits(dst + 10, 1, 47, docinfo->bp);
    dst[16] = docinfo->content_meta;
    set_bits(dst + 17, 0, 32, docinfo->rev_seq); //21 bytes in
    memcpy(dst + 21, docinfo->rev_meta.buf, docinfo->rev_meta.size);
    return 21 + docinfo->rev_meta.size;
}

static couchstore_error_t write_doc(Db *db, const Doc *doc, uint64_t *bp,
                                    size_t* disk_size, uint64_t writeopts)
{
    couchstore_error_t errcode;
    if (writeopts & COMPRESSED_BODY) {
        errcode = db_write_buf_compressed(db, &doc->data, (off_t *) bp, disk_size);
    } else {
        errcode = db_write_buf(db, &doc->data, (off_t *) bp, disk_size);
    }

    return errcode;
}

static int id_action_compare(const void *actv1, const void *actv2)
{
    const couchfile_modify_action *act1, *act2;
    act1 = (const couchfile_modify_action *) actv1;
    act2 = (const couchfile_modify_action *) actv2;

    int cmp = ebin_cmp(act1->key, act2->key);
    if (cmp == 0) {
        if (act1->type < act2->type) {
            return -1;
        }
        if (act1->type > act2->type) {
            return 1;
        }
    }
    return cmp;
}

static int seq_action_compare(const void *actv1, const void *actv2)
{
    const couchfile_modify_action *act1, *act2;
    act1 = (const couchfile_modify_action *) actv1;
    act2 = (const couchfile_modify_action *) actv2;

    uint64_t seq1, seq2;

    seq1 = get_48(act1->key->buf);
    seq2 = get_48(act2->key->buf);

    if (seq1 < seq2) {
        return -1;
    }
    if (seq1 == seq2) {
        if (act1->type < act2->type) {
            return -1;
        }
        if (act1->type > act2->type) {
            return 1;
        }
        return 0;
    }
    if (seq1 > seq2) {
        return 1;
    }
    return 0;
}

typedef struct _idxupdatectx {
    couchfile_modify_action *seqacts;
    int actpos;

    sized_buf **seqs;
    sized_buf **seqvals;
    int valpos;

    fatbuf *deltermbuf;
} index_update_ctx;

static void idfetch_update_cb(couchfile_modify_request *rq,
                              sized_buf *k, sized_buf *v, void *arg)
{
    (void)k;
    (void)rq;
    //v contains a seq we need to remove ( {Seq,_,_,_,_} )
    uint64_t oldseq;
    sized_buf *delbuf = NULL;
    index_update_ctx *ctx = (index_update_ctx *) arg;

    if (v == NULL) { //Doc not found
        return;
    }

    oldseq = get_48(v->buf);

    delbuf = (sized_buf *) fatbuf_get(ctx->deltermbuf, sizeof(sized_buf));
    delbuf->buf = (char *) fatbuf_get(ctx->deltermbuf, 6);
    delbuf->size = 6;
    memset(delbuf->buf, 0, 6);
    set_bits(delbuf->buf, 0, 48, oldseq);

    ctx->seqacts[ctx->actpos].type = ACTION_REMOVE;
    ctx->seqacts[ctx->actpos].value.data = NULL;
    ctx->seqacts[ctx->actpos].key = delbuf;
    ctx->seqacts[ctx->actpos].cmp_key = delbuf;

    ctx->actpos++;
}

static couchstore_error_t update_indexes(Db *db,
                                         sized_buf *seqs,
                                         sized_buf *seqvals,
                                         sized_buf *ids,
                                         sized_buf *idvals,
                                         int numdocs)
{
    couchfile_modify_action *idacts;
    couchfile_modify_action *seqacts;
    sized_buf *idcmps;
    size_t size;
    fatbuf *actbuf;
    node_pointer *new_id_root;
    node_pointer *new_seq_root;
    couchstore_error_t errcode;
    couchfile_modify_request seqrq, idrq;
    sized_buf tmpsb;
    int ii;

    /*
    ** Two action list up to numdocs * 2 in size + Compare keys for ids,
    ** and compare keys for removed seqs found from id index +
    ** Max size of a int64 erlang term (for deleted seqs)
    */
    size = 4 * sizeof(couchfile_modify_action) + 2 * sizeof(sized_buf) + 10;

    if ((actbuf = fatbuf_alloc(numdocs * size)) == NULL) {
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }

    idacts = fatbuf_get(actbuf, numdocs * sizeof(couchfile_modify_action) * 2);
    seqacts = fatbuf_get(actbuf, numdocs * sizeof(couchfile_modify_action) * 2);
    idcmps = fatbuf_get(actbuf, numdocs * sizeof(sized_buf));

    if (idacts == NULL || seqacts == NULL || idcmps == NULL) {
        fatbuf_free(actbuf);
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }

    index_update_ctx fetcharg = {
        seqacts, 0, &seqs, &seqvals, 0, actbuf
    };

    for (ii = 0; ii < numdocs; ii++) {
        idcmps[ii].buf = ids[ii].buf + 5;
        idcmps[ii].size = ids[ii].size - 5;

        idacts[ii * 2].type = ACTION_FETCH;
        idacts[ii * 2].value.arg = &fetcharg;
        idacts[ii * 2 + 1].type = ACTION_INSERT;
        idacts[ii * 2 + 1].value.data = &idvals[ii];

        idacts[ii * 2].key = &ids[ii];
        idacts[ii * 2].cmp_key = &idcmps[ii];

        idacts[ii * 2 + 1].key = &ids[ii];
        idacts[ii * 2 + 1].cmp_key = &idcmps[ii];
    }

    qsort(idacts, numdocs * 2, sizeof(couchfile_modify_action),
          id_action_compare);

    idrq.cmp.compare = ebin_cmp;
    idrq.cmp.arg = &tmpsb;
    idrq.db = db;
    idrq.actions = idacts;
    idrq.num_actions = numdocs * 2;
    idrq.reduce = by_id_reduce;
    idrq.rereduce = by_id_rereduce;
    idrq.fetch_callback = idfetch_update_cb;
    idrq.db = db;

    new_id_root = modify_btree(&idrq, db->header.by_id_root, &errcode);
    if (errcode != COUCHSTORE_SUCCESS) {
        fatbuf_free(actbuf);
        return errcode;
    }

    while (fetcharg.valpos < numdocs) {
        seqacts[fetcharg.actpos].type = ACTION_INSERT;
        seqacts[fetcharg.actpos].value.data = &seqvals[fetcharg.valpos];
        seqacts[fetcharg.actpos].key = &seqs[fetcharg.valpos];
        seqacts[fetcharg.actpos].cmp_key = &seqs[fetcharg.valpos];
        fetcharg.valpos++;
        fetcharg.actpos++;
    }

    //printf("Total seq actions: %d\n", fetcharg.actpos);
    qsort(seqacts, fetcharg.actpos, sizeof(couchfile_modify_action),
          seq_action_compare);

    seqrq.cmp.compare = seq_cmp;
    seqrq.cmp.arg = &tmpsb;
    seqrq.actions = seqacts;
    seqrq.num_actions = fetcharg.actpos;
    seqrq.reduce = by_seq_reduce;
    seqrq.rereduce = by_seq_rereduce;
    seqrq.db = db;

    new_seq_root = modify_btree(&seqrq, db->header.by_seq_root, &errcode);
    if (errcode != COUCHSTORE_SUCCESS) {
        fatbuf_free(actbuf);
        return errcode;
    }

    if (db->header.by_id_root != new_id_root) {
        free(db->header.by_id_root);
        db->header.by_id_root = new_id_root;
    }

    if (db->header.by_seq_root != new_seq_root) {
        free(db->header.by_seq_root);
        db->header.by_seq_root = new_seq_root;
    }

    fatbuf_free(actbuf);
    return errcode;
}

static couchstore_error_t add_doc_to_update_list(Db *db,
                                                 const Doc *doc,
                                                 const DocInfo *info,
                                                 fatbuf *fb,
                                                 sized_buf *seqterm,
                                                 sized_buf *idterm,
                                                 sized_buf *seqval,
                                                 sized_buf *idval,
                                                 uint64_t seq,
                                                 uint64_t options)
{
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    DocInfo updated = *info;
    updated.db_seq = seq;

    seqterm->buf = (char *) fatbuf_get(fb, 6);
    seqterm->size = 6;
    error_unless(seqterm->buf, COUCHSTORE_ERROR_ALLOC_FAIL);
    memset(seqterm->buf, 0, 6);
    set_bits(seqterm->buf, 0, 48, seq);

    if (doc) {
        uint64_t writeopts = 0;
        size_t disk_size;
        
        if ((options & COMPRESS_DOC_BODIES) && (info->content_meta & SNAPPY_META_FLAG)) {
            writeopts = COMPRESSED_BODY;
        }
        errcode = write_doc(db, doc, &updated.bp, &disk_size, writeopts);

        if (errcode != COUCHSTORE_SUCCESS) {
            return errcode;
        }
        updated.size = disk_size;
    } else {
        updated.deleted = 1;
        updated.bp = 0;
        updated.size = 0;
    }

    *idterm = updated.id;

    seqval->buf = (char *) fatbuf_get(fb, (44 + updated.id.size + updated.rev_meta.size));
    error_unless(seqval->buf, COUCHSTORE_ERROR_ALLOC_FAIL);
    seqval->size = assemble_seq_index_value(&updated, seqval->buf);

    idval->buf = (char *) fatbuf_get(fb, (44 + 10 + updated.rev_meta.size));
    error_unless(idval->buf, COUCHSTORE_ERROR_ALLOC_FAIL);
    idval->size = assemble_id_index_value(&updated, idval->buf);

    //We use 37 + id.size + 2 * rev_meta.size bytes
cleanup:
    return errcode;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_save_documents(Db *db,
                                             Doc* const *docs,
                                             DocInfo* const *infos,
                                             long numdocs,
                                             uint64_t options)
{
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    int ii;
    sized_buf *seqklist, *idklist, *seqvlist, *idvlist;
    size_t term_meta_size = 0;
    const Doc *curdoc;
    uint64_t seq = db->header.update_seq;

    fatbuf *fb;

    for (ii = 0; ii < numdocs; ii++) {
        // Get additional size for terms to be inserted into indexes
        size_t size = 37 + infos[ii]->id.size + 2 * infos[ii]->rev_meta.size;
        term_meta_size += (2 * size);
    }

    fb = fatbuf_alloc(term_meta_size +
                      numdocs * (sizeof(sized_buf) * 4)); //seq/id key and value lists

    if (fb == NULL) {
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }


    seqklist = fatbuf_get(fb, numdocs * sizeof(sized_buf));
    idklist = fatbuf_get(fb, numdocs * sizeof(sized_buf));
    seqvlist = fatbuf_get(fb, numdocs * sizeof(sized_buf));
    idvlist = fatbuf_get(fb, numdocs * sizeof(sized_buf));

    for (ii = 0; ii < numdocs; ii++) {
        seq++;
        if (docs) {
            curdoc = docs[ii];
        } else {
            curdoc = NULL;
        }

        errcode = add_doc_to_update_list(db, curdoc, infos[ii], fb,
                                         &seqklist[ii], &idklist[ii],
                                         &seqvlist[ii], &idvlist[ii],
                                         seq, options);
        if (errcode != COUCHSTORE_SUCCESS) {
            break;
        }
    }

    if (errcode == COUCHSTORE_SUCCESS) {
        errcode = update_indexes(db, seqklist, seqvlist,
                                 idklist, idvlist, numdocs);
    }

    fatbuf_free(fb);
    if (errcode == COUCHSTORE_SUCCESS) {
        db->header.update_seq = seq;
    }

    return errcode;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_save_document(Db *db, const Doc *doc,
                                            const DocInfo *info, uint64_t options)
{
    return couchstore_save_documents(db, (Doc**)&doc, (DocInfo**)&info, 1, options);
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

