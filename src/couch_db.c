/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include <ei.h>
#include <snappy-c.h>

#include "internal.h"
#include "couch_btree.h"
#include "util.h"
#include "reduces.h"

#define SNAPPY_META_FLAG 128

sized_buf nil_atom = {
    (char *) "\x64\x00\x03nil",
    6
};

static couchstore_error_t find_header(Db *db)
{
    uint64_t block = db->file_pos / COUCH_BLOCK_SIZE;
    int errcode = 0;
    int readsize;
    char *header_buf = NULL;
    uint8_t buf[2];

    while (1) {
        readsize = db->file_ops->pread(db, buf, 2, block * COUCH_BLOCK_SIZE);
        if (readsize == 2 && buf[0] == 1) {
            //Found a header block.
            int header_len = pread_header(db, block * COUCH_BLOCK_SIZE, &header_buf);
            int arity = 0;
            int purged_docs_index = 0;
            if (header_len > 0) {
                int idx = 0;
                if (ei_decode_version(header_buf, &idx, &arity) < 0) {
                    errcode = COUCHSTORE_ERROR_PARSE_TERM;
                    break;
                }
                if (ei_decode_tuple_header(header_buf, &idx, &arity) < 0) {
                    errcode = COUCHSTORE_ERROR_PARSE_TERM;
                    break;
                }
                ei_skip_term(header_buf, &idx); //db_header
                ei_decode_uint64(header_buf, &idx, &db->header.disk_version);

                if (db->header.disk_version != COUCH_DISK_VERSION) {
                    errcode = COUCHSTORE_ERROR_HEADER_VERSION;
                    break;
                }

                ei_decode_uint64(header_buf, &idx, &db->header.update_seq);
                db->header.by_id_root = read_root(header_buf, &idx);
                db->header.by_seq_root = read_root(header_buf, &idx);
                db->header.local_docs_root = read_root(header_buf, &idx);
                ei_decode_uint64(header_buf, &idx, &db->header.purge_seq);

                purged_docs_index = idx;
                ei_skip_term(header_buf, &idx); //purged_docs
                db->header.purged_docs = malloc(sizeof(sized_buf) + (idx - purged_docs_index));
                db->header.purged_docs->buf = ((char *)db->header.purged_docs) + sizeof(sized_buf);
                memcpy(db->header.purged_docs->buf, header_buf + purged_docs_index, idx - purged_docs_index);
                db->header.purged_docs->size = idx - purged_docs_index;

                ei_skip_term(header_buf, &idx); //security ptr
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

    free(header_buf);
    return errcode;
}

static couchstore_error_t write_header(Db *db)
{
    ei_x_buff x_header;
    sized_buf writebuf;

    ei_x_new_with_version(&x_header);
    ei_x_encode_tuple_header(&x_header, 8);
    ei_x_encode_atom(&x_header, "db_header");
    ei_x_encode_ulonglong(&x_header, db->header.disk_version);
    ei_x_encode_ulonglong(&x_header, db->header.update_seq);
    ei_x_encode_nodepointer(&x_header, db->header.by_id_root);
    ei_x_encode_nodepointer(&x_header, db->header.by_seq_root);
    ei_x_encode_nodepointer(&x_header, db->header.local_docs_root);
    ei_x_encode_ulonglong(&x_header, db->header.purge_seq);
    ei_x_append_buf(&x_header, db->header.purged_docs->buf, db->header.purged_docs->size);
    ei_x_encode_atom(&x_header, "nil"); //security_ptr;
    writebuf.buf = x_header.buff;
    writebuf.size = x_header.index;
    off_t pos;
    couchstore_error_t errcode = db_write_header(db, &writebuf, &pos);
    if (errcode == COUCHSTORE_SUCCESS) {
        db->header.position = pos;
    }
    ei_x_free(&x_header);
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
    db->header.purged_docs = &nil_atom;
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

    if (db->header.purged_docs != &nil_atom) {
        free(db->header.purged_docs);
    }
    memset(db, 0xa5, sizeof(*db));
    free(db);

    return COUCHSTORE_SUCCESS;
}

static int ebin_cmp(void *k1, void *k2)
{
    sized_buf *e1 = (sized_buf *)k1;
    sized_buf *e2 = (sized_buf *)k2;
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

static void *ebin_from_ext(compare_info *c, char *buf, int pos)
{
    int binsize;
    int type;
    sized_buf *ebcmp = (sized_buf *) c->arg;
    ei_get_type(buf, &pos, &type, &binsize);
    ebcmp->buf = buf + pos + 5;
    ebcmp->size = binsize;
    return ebcmp;
}

static void *term_from_ext(compare_info *c, char *buf, int pos)
{
    int endpos = pos;
    sized_buf *ebcmp = (sized_buf *) c->arg;
    ei_skip_term(buf, &endpos);
    ebcmp->buf = buf + pos;
    ebcmp->size = endpos - pos;
    return ebcmp;
}

static int long_term_cmp(void *k1, void *k2)
{
    sized_buf *e1 = (sized_buf *)k1;
    sized_buf *e2 = (sized_buf *)k2;
    int pos = 0;
    uint64_t e1val, e2val;
    ei_decode_uint64(e1->buf, &pos, &e1val);
    pos = 0;
    ei_decode_uint64(e2->buf, &pos, &e2val);
    if (e1val == e2val) {
        return 0;
    }
    return (e1val < e2val ? -1 : 1);
}

static couchstore_error_t docinfo_from_buf(DocInfo **pInfo,
                                           sized_buf *v,
                                           int idBytes)
{
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    int term_index = 0, fterm_pos = 0, fterm_size = 0;
    int metabin_pos = 0, metabin_size = 0;
    uint64_t deleted;
    uint64_t content_meta;
    uint64_t seq = 0, rev = 0, bp = 0;
    uint64_t size;
    char *infobuf = NULL;
    *pInfo = NULL;

    if (v == NULL) {
        return COUCHSTORE_ERROR_DOC_NOT_FOUND;
    }

    //Id/Seq
    error_unless(tuple_check(v->buf, &term_index, 6), COUCHSTORE_ERROR_PARSE_TERM);
    fterm_pos = term_index; //Save position of first term
    ei_skip_term(v->buf, &term_index);
    fterm_size = term_index - fterm_pos; //and size.

    //Rev = {RevNum, MetaBin}
    error_unless(tuple_check(v->buf, &term_index, 2), COUCHSTORE_ERROR_PARSE_TERM);
    error_nonzero(ei_decode_uint64(v->buf, &term_index, &rev), COUCHSTORE_ERROR_PARSE_TERM);
    metabin_pos = term_index + 5; //Save position of meta term
    //We know it's an ERL_BINARY_EXT, so the contents are from
    //5 bytes in to the end of the term.
    ei_skip_term(v->buf, &term_index);
    metabin_size = term_index - metabin_pos; //and size.

    error_nonzero(ei_decode_uint64(v->buf, &term_index, &bp), COUCHSTORE_ERROR_PARSE_TERM);
    error_nonzero(ei_decode_uint64(v->buf, &term_index, &deleted), COUCHSTORE_ERROR_PARSE_TERM);
    error_nonzero(ei_decode_uint64(v->buf, &term_index, &content_meta), COUCHSTORE_ERROR_PARSE_TERM);
    error_nonzero(ei_decode_uint64(v->buf, &term_index, &size), COUCHSTORE_ERROR_PARSE_TERM);

    //If first term is seq, we don't need to include it in the buffer
    if (idBytes != 0) {
        fterm_size = 0;
    }
    infobuf = (char *) malloc(sizeof(DocInfo) + metabin_size + fterm_size + idBytes);
    error_unless(infobuf, COUCHSTORE_ERROR_ALLOC_FAIL);
    *pInfo = (DocInfo *) infobuf;

    (*pInfo)->rev_meta.buf = infobuf + sizeof(DocInfo);
    (*pInfo)->rev_meta.size = metabin_size;

    if (metabin_size > 0) {
        memcpy((*pInfo)->rev_meta.buf, v->buf + metabin_pos, metabin_size);
    }

    (*pInfo)->id.buf = infobuf + sizeof(DocInfo) + metabin_size;

    if (idBytes != 0) { //First term is Seq

        (*pInfo)->id.size = idBytes;
        ei_decode_uint64(v->buf, &fterm_pos, &seq);
        //Let the caller fill in the Id.
    } else { //First term is Id
        (*pInfo)->id.size = fterm_size - 5; //Id will be a binary.
        memcpy((*pInfo)->id.buf, v->buf + fterm_pos + 5, fterm_size);
        //Let the caller fill in the Seq
    }

    (*pInfo)->db_seq = seq;
    (*pInfo)->rev_seq = rev;
    (*pInfo)->bp = bp;
    (*pInfo)->size = size;
    (*pInfo)->deleted = deleted;
    (*pInfo)->content_meta = content_meta;

cleanup:
    if (errcode < 0) {
        free(*pInfo);
        *pInfo = NULL;
    }
    return errcode;
}

#define COMPRESSED_BODY 1
//Fill in doc from reading file.
static couchstore_error_t bp_to_doc(Doc **pDoc, Db *db, off_t bp, uint64_t options)
{
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    size_t bodylen = 0;
    char *docbody = NULL;
    fatbuf *docbuf = NULL;

    if (options & COMPRESSED_BODY) {
        bodylen = pread_compressed(db, bp, &docbody);
    } else {
        bodylen = pread_bin(db, bp, &docbody);
    }

    error_unless(docbuf = fatbuf_alloc(sizeof(Doc) + bodylen), COUCHSTORE_ERROR_ALLOC_FAIL);
    *pDoc = (Doc *) fatbuf_get(docbuf, sizeof(Doc));

    if (bodylen == 0) { //Empty doc
        (*pDoc)->data.buf = NULL;
        (*pDoc)->data.size = 0;
        return 0;
    }

    error_unless(bodylen > 0, COUCHSTORE_ERROR_READ);
    error_unless(docbody, COUCHSTORE_ERROR_READ);
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
    int errcode = docinfo_from_buf(pInfo, v, id->size - 5);
    if (errcode == COUCHSTORE_SUCCESS) {
        memcpy((*pInfo)->id.buf, id->buf + 5, id->size - 5);
    }

    return errcode;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_docinfo_by_id(Db *db,
                                            const void *id,
                                            size_t idlen,
                                            DocInfo **pInfo)
{
    sized_buf key;
    void *keylist = &key;
    couchfile_lookup_request rq;
    sized_buf cmptmp;
    couchstore_error_t errcode;

    if (db->header.by_id_root == NULL) {
        return COUCHSTORE_ERROR_DOC_NOT_FOUND;
    }

    key.buf = (char *) id;
    key.size = idlen;

    rq.cmp.compare = ebin_cmp;
    rq.cmp.from_ext = ebin_from_ext;
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
    int seqindex = 0;
    DocInfo *docinfo;
    docinfo_from_buf(&docinfo, v, 0);
    ei_decode_uint64(seqterm->buf, &seqindex, &docinfo->db_seq);

    union c99hack hack;
    hack.voidptr = ((void **)rq->callback_ctx)[0];
    Db *db =  ((void **)rq->callback_ctx)[1];
    void *ctx = ((void **)rq->callback_ctx)[2];

    if (hack.callback(db, docinfo, ctx)  == 0) {
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
    (void)options;
    char since_termbuf[10];
    sized_buf since_term;
    void *keylist = &since_term;
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
    since_term.size = 0;
    ei_encode_ulonglong(since_termbuf, (int *) &since_term.size, since);

    cbctx[0] = hack.voidptr;
    cbctx[1] = db;
    cbctx[2] = ctx;

    rq.cmp.compare = long_term_cmp;
    rq.cmp.from_ext = term_from_ext;
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

static void copy_term(char *dst, int *idx, sized_buf *term)
{
    memcpy(dst + *idx, term->buf, term->size);
    *idx += term->size;
}

static int assemble_index_value(DocInfo *docinfo, char *dst,
                                sized_buf *first_term)
{
    int pos = 0;
    ei_encode_tuple_header(dst, &pos, 6); //2 bytes.

    //Id or Seq (possibly encoded as a binary)
    copy_term(dst, &pos, first_term); //first_term.size
    //Rev
    ei_encode_tuple_header(dst, &pos, 2); //3 bytes.
    ei_encode_ulonglong(dst, &pos, docinfo->rev_seq); //Max 10 bytes
    ei_encode_binary(dst, &pos, docinfo->rev_meta.buf, docinfo->rev_meta.size); //meta.size + 5
    //Bp
    ei_encode_ulonglong(dst, &pos, docinfo->bp); //Max 10 bytes
    //Deleted
    ei_encode_ulonglong(dst, &pos, docinfo->deleted); //2 bytes
    //
    ei_encode_ulonglong(dst, &pos, docinfo->content_meta); //2 bytes
    //Size
    ei_encode_ulonglong(dst, &pos, docinfo->size); //Max 10 bytes

    //Max 44 + first_term.size + meta.size bytes.
    return pos;
}

static couchstore_error_t write_doc(Db *db, Doc *doc, uint64_t *bp,
                                    uint64_t writeopts)
{
    couchstore_error_t errcode;
    if (writeopts & COMPRESSED_BODY) {
        errcode = db_write_buf_compressed(db, &doc->data, (off_t *) bp);
    } else {
        errcode = db_write_buf(db, &doc->data, (off_t *) bp);
    }

    return errcode;
}

static int id_action_compare(const void *actv1, const void *actv2)
{
    const couchfile_modify_action *act1, *act2;
    act1 = (const couchfile_modify_action *) actv1;
    act2 = (const couchfile_modify_action *) actv2;

    int cmp = ebin_cmp(act1->cmp_key, act2->cmp_key);
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
    int pos = 0;

    ei_decode_uint64(act1->key->buf, &pos, &seq1);
    pos = 0;
    ei_decode_uint64(act2->key->buf, &pos, &seq2);

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
    (void)k; (void)rq;
    //v contains a seq we need to remove ( {Seq,_,_,_,_} )
    int termpos = 0;
    uint64_t oldseq;
    sized_buf *delbuf = NULL;
    index_update_ctx *ctx = (index_update_ctx *) arg;

    if (v == NULL) { //Doc not found
        return;
    }

    ei_decode_tuple_header(v->buf, &termpos, NULL);
    ei_decode_uint64(v->buf, &termpos, &oldseq);

    delbuf = (sized_buf *) fatbuf_get(ctx->deltermbuf, sizeof(sized_buf));
    delbuf->buf = (char *) fatbuf_get(ctx->deltermbuf, 10);
    delbuf->size = 0;
    ei_encode_ulonglong(delbuf->buf, (int *) &delbuf->size, oldseq);

    ctx->seqacts[ctx->actpos].type = ACTION_REMOVE;
    ctx->seqacts[ctx->actpos].value.term = NULL;
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
        idacts[ii * 2 + 1].value.term = &idvals[ii];

        idacts[ii * 2].key = &ids[ii];
        idacts[ii * 2].cmp_key = &idcmps[ii];

        idacts[ii * 2 + 1].key = &ids[ii];
        idacts[ii * 2 + 1].cmp_key = &idcmps[ii];
    }

    qsort(idacts, numdocs * 2, sizeof(couchfile_modify_action),
          id_action_compare);

    idrq.cmp.compare = ebin_cmp;
    idrq.cmp.from_ext = ebin_from_ext;
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
        seqacts[fetcharg.actpos].value.term = &seqvals[fetcharg.valpos];
        seqacts[fetcharg.actpos].key = &seqs[fetcharg.valpos];
        seqacts[fetcharg.actpos].cmp_key = &seqs[fetcharg.valpos];
        fetcharg.valpos++;
        fetcharg.actpos++;
    }

    //printf("Total seq actions: %d\n", fetcharg.actpos);
    qsort(seqacts, fetcharg.actpos, sizeof(couchfile_modify_action),
          seq_action_compare);

    seqrq.cmp.compare = long_term_cmp;
    seqrq.cmp.from_ext = term_from_ext;
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
                                                 Doc *doc,
                                                 DocInfo *info,
                                                 fatbuf *fb,
                                                 sized_buf *seqterm,
                                                 sized_buf *idterm,
                                                 sized_buf *seqval,
                                                 sized_buf *idval,
                                                 uint64_t seq,
                                                 uint64_t options)
{
    couchstore_error_t errcode;
    DocInfo updated = *info;
    updated.db_seq = seq;

    seqterm->buf = fatbuf_get(fb, 10);
    idterm->buf = fatbuf_get(fb, updated.id.size + 5);
    seqval->buf = fatbuf_get(fb, (44 + updated.id.size + updated.rev_meta.size));
    idval->buf = fatbuf_get(fb, (44 + 10 + updated.rev_meta.size));

    if (!(seqterm->buf && idterm->buf && seqval->buf && idval->buf)) {
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }
    seqterm->size = 0;
    idterm->size = 0;
    ei_encode_ulonglong(seqterm->buf, (int *) &seqterm->size, seq);

    if (doc) {
        if ((options & COMPRESS_DOC_BODIES) && (info->content_meta & SNAPPY_META_FLAG)) {
            errcode = write_doc(db, doc, &updated.bp, COMPRESSED_BODY);
        } else {
            errcode = write_doc(db, doc, &updated.bp, 0);
        }

        if (errcode != COUCHSTORE_SUCCESS) {
            return errcode;
        }
        updated.size = doc->data.size;
    } else {
        updated.deleted = 1;
        updated.bp = 0;
        updated.size = 0;
    }

    ei_encode_binary(idterm->buf, (int *) &idterm->size, updated.id.buf, updated.id.size);
    seqval->size = assemble_index_value(&updated, seqval->buf, idterm);
    idval->size = assemble_index_value(&updated, idval->buf, seqterm);

    //Use max of 10 +, id.size + 5 +, 42 + rev_meta.size + id.size, + 52 + rev_meta.size
    // == id.size *2 + rev_meta.size *2 + 109 bytes

    return errcode;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_save_documents(Db *db,
                                             Doc **docs,
                                             DocInfo **infos,
                                             long numdocs,
                                             uint64_t options)
{
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    int ii;
    sized_buf *seqklist, *idklist, *seqvlist, *idvlist;
    size_t term_meta_size = 0;
    Doc *curdoc;
    uint64_t seq = db->header.update_seq;

    fatbuf *fb;

    for (ii = 0; ii < numdocs; ii++) {
        // Get additional size for terms to be inserted into indexes
        size_t size = infos[ii]->id.size + infos[ii]->rev_meta.size;
        term_meta_size += 113 + (2 * size);
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
couchstore_error_t couchstore_save_document(Db *db, Doc *doc,
                                            DocInfo *info, uint64_t options)
{
    return couchstore_save_documents(db, &doc, &info, 1, options);
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
    dp->id.buf = (char *) fatbuf_get(ldbuf, id->size - 5);
    dp->id.size = id->size - 5;

    dp->json.buf = (char *) fatbuf_get(ldbuf, v->size - 5);
    dp->json.size = v->size - 5;

    dp->deleted = 0;

    memcpy(dp->id.buf, id->buf + 5, id->size - 5);
    memcpy(dp->json.buf, v->buf + 5, v->size - 5);

    return COUCHSTORE_SUCCESS;
}

LIBCOUCHSTORE_API
couchstore_error_t couchstore_open_local_document(Db *db,
                                                  const void *id,
                                                  size_t idlen,
                                                  LocalDoc **pDoc)
{
    sized_buf key;
    void *keylist = &key;
    couchfile_lookup_request rq;
    sized_buf cmptmp;
    couchstore_error_t errcode;

    if (db->header.local_docs_root == NULL) {
        return COUCHSTORE_ERROR_DOC_NOT_FOUND;
    }

    key.buf = (char *) id;
    key.size = idlen;

    rq.cmp.compare = ebin_cmp;
    rq.cmp.from_ext = ebin_from_ext;
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
    fatbuf *binbufs = fatbuf_alloc(10 + lDoc->id.size + lDoc->json.size);
    sized_buf idterm;
    sized_buf jsonterm;
    sized_buf cmptmp;
    node_pointer *nroot = NULL;

    if (binbufs == NULL) {
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }

    if (lDoc->deleted) {
        ldupdate.type = ACTION_REMOVE;
    } else {
        ldupdate.type = ACTION_INSERT;
    }

    idterm.buf = (char *) fatbuf_get(binbufs, lDoc->id.size + 5);
    idterm.size = 0;
    ei_encode_binary(idterm.buf, (int *) &idterm.size, lDoc->id.buf,
                     lDoc->id.size);

    jsonterm.buf = (char *) fatbuf_get(binbufs, lDoc->json.size + 5);
    jsonterm.size = 0;
    ei_encode_binary(jsonterm.buf, (int *) &jsonterm.size,
                     lDoc->json.buf, lDoc->json.size);

    ldupdate.cmp_key = (void *) &lDoc->id;
    ldupdate.key = &idterm;
    ldupdate.value.term = &jsonterm;

    couchfile_modify_request rq;
    rq.cmp.compare = ebin_cmp;
    rq.cmp.from_ext = ebin_from_ext;
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

    fatbuf_free(binbufs);
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

