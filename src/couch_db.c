#include <fcntl.h>
#include <string.h>
#include <stdlib.h>

#include "couch_db.h"
#include "couch_btree.h"
#include "ei.h"
#include "snappy-c.h"
#include "util.h"

sized_buf nil_atom = {
    "\x64\x00\x03nil",
    6
};

int find_header(Db *db)
{
    int block = db->file_pos / SIZE_BLOCK;
    int errcode = 0;
    int readsize;
    char* header_buf = NULL;
    uint8_t buf[2];

    while(block >= 0)
    {
        readsize = pread(db->fd, buf, 2, block * SIZE_BLOCK);
        if(readsize == 2 && buf[0] == 1)
        {
            //Found a header block.
            int header_len = pread_header(db->fd, block * SIZE_BLOCK, &header_buf);
            int arity = 0;
            int purged_docs_index = 0;
            if(header_len > 0)
            {
                int index = 0;
                if(ei_decode_version(header_buf, &index, &arity) < 0)
                {
                    errcode = ERROR_PARSE_TERM;
                    break;
                }
                if(ei_decode_tuple_header(header_buf, &index, &arity) < 0)
                {
                    errcode = ERROR_PARSE_TERM;
                    break;
                }
                ei_skip_term(header_buf, &index); //db_header
                ei_decode_ulong(header_buf, &index, &db->header.disk_version);
                ei_decode_ulonglong(header_buf, &index, &db->header.update_seq);
                ei_skip_term(header_buf, &index); //unused
                db->header.by_id_root = read_root(header_buf, &index);
                db->header.by_seq_root = read_root(header_buf, &index);
                db->header.local_docs_root = read_root(header_buf, &index);
                ei_decode_ulonglong(header_buf, &index, &db->header.purge_seq);

                purged_docs_index = index;
                ei_skip_term(header_buf, &index); //purged_docs
                db->header.purged_docs = malloc(sizeof(sized_buf) + (index - purged_docs_index));
                db->header.purged_docs->buf = ((char*)db->header.purged_docs) + sizeof(sized_buf);
                memcpy(db->header.purged_docs->buf, header_buf + purged_docs_index, index - purged_docs_index);

                ei_skip_term(header_buf, &index); //security ptr
                ei_skip_term(header_buf, &index); //revs_limit
                break;
            }
        }
        block--;
    }

    if(header_buf != NULL)
        free(header_buf);

    if(block == -1)
    {
        //Didn't find a header block
        //TODO what do we do here?
        return ERROR_NO_HEADER;
    }
    return errcode;
}

int write_header(Db* db)
{
    ei_x_buff x_header;
    sized_buf writebuf;
    int errcode = 0;

    ei_x_new_with_version(&x_header);
    ei_x_encode_tuple_header(&x_header, 10);
    ei_x_encode_atom(&x_header, "db_header");
    ei_x_encode_ulonglong(&x_header, db->header.disk_version);
    ei_x_encode_ulonglong(&x_header, db->header.update_seq);
    ei_x_encode_ulonglong(&x_header, 0); //unused field
    ei_x_encode_nodepointer(&x_header, db->header.by_id_root);
    ei_x_encode_nodepointer(&x_header, db->header.by_seq_root);
    ei_x_encode_nodepointer(&x_header, db->header.local_docs_root);
    ei_x_encode_ulonglong(&x_header, db->header.purge_seq);
    ei_x_append_buf(&x_header, db->header.purged_docs->buf, db->header.purged_docs->size);
    ei_x_encode_atom(&x_header, "nil"); //security_ptr;
    ei_x_encode_ulonglong(&x_header, 1); //revs_limit
    writebuf.buf = x_header.buff;
    writebuf.size = x_header.index;
    errcode = db_write_header(db, &writebuf);
    ei_x_free(&x_header);
    return errcode;
}

int open_db(char* filename, uint64_t options, Db** pDb)
{
    int errcode = 0;
    Db* db = malloc(sizeof(Db));
    *pDb = db;
    db->fd = open(filename, O_CREAT | O_RDWR, 0744);
    error_unless(db->fd, ERROR_OPEN_FILE);
    //TODO Not totally up on how to handle large files.
    //     Should we be using pread64 and the off64_t accepting functions?
    //     They don't seem to be available everywhere.
    db->file_pos = lseek(db->fd, 0, SEEK_END);
    //TODO are there some cases where we should blow up?
    //     such as not finding a header in a file that we didn't
    //     just create? (Possibly not a couch file?)
    if(db->file_pos == 0)
    {
        //Our file is empty, create and write a new empty db header.
        db->header.disk_version = LATEST_DISK_VERSION;
        db->header.update_seq = 0;
        db->header.by_id_root = NULL;
        db->header.by_seq_root = NULL;
        db->header.local_docs_root = NULL;
        db->header.purge_seq = 0;
        db->header.purged_docs = &nil_atom;
        write_header(db);
        return 0;
    }
    else
        try(find_header(db));
cleanup:
    return errcode;
}

int close_db(Db* db)
{
    int errcode = 0;
    if(db->fd)
        close(db->fd);
    db->fd = 0;

    if(db->header.by_id_root)
        free(db->header.by_id_root);
    db->header.by_id_root = NULL;

    if(db->header.by_seq_root)
        free(db->header.by_seq_root);
    db->header.by_seq_root = NULL;

    if(db->header.local_docs_root)
        free(db->header.local_docs_root);
    db->header.local_docs_root = NULL;

    if(db->header.purged_docs && db->header.purged_docs != &nil_atom)
        free(db->header.purged_docs);
    db->header.purged_docs = NULL;
    free(db);
    return errcode;
}

int ebin_cmp(void* k1, void* k2) {
    sized_buf *e1 = (sized_buf*)k1;
    sized_buf *e2 = (sized_buf*)k2;
    int size;
    if(e2->size < e1->size)
    {
        size = e2->size;
    }
    else
    {
        size = e1->size;
    }

    int cmp = memcmp(e1->buf, e2->buf, size);
    if(cmp == 0)
    {
        if(size < e2->size)
        {
            return -1;
        }
        else if (size < e1->size)
        {
            return 1;
        }
    }
    return cmp;
}

void* ebin_from_ext(compare_info* c, char* buf, int pos) {
    int binsize;
    int type;
    sized_buf* ebcmp = c->arg;
    ei_get_type(buf, &pos, &type, &binsize);
    ebcmp->buf = buf + pos + 5;
    ebcmp->size = binsize;
    return ebcmp;
}

void* term_from_ext(compare_info* c, char* buf, int pos) {
    int endpos = pos;
    sized_buf* ebcmp = c->arg;
    ei_skip_term(buf, &endpos);
    ebcmp->buf = buf + pos;
    ebcmp->size = endpos - pos;
    return ebcmp;
}

int long_term_cmp(void *k1, void *k2) {
    sized_buf *e1 = (sized_buf*)k1;
    sized_buf *e2 = (sized_buf*)k2;
    int pos = 0;
    uint64_t e1val, e2val;
    ei_decode_ulonglong(e1->buf, &pos, &e1val);
    pos = 0;
    ei_decode_ulonglong(e2->buf, &pos, &e2val);
    if(e1val == e2val)
    {
        return 0;
    }
    return (e1val < e2val ? -1 : 1);
}

int docinfo_from_buf(DocInfo** pInfo, sized_buf *v, int idBytes)
{
    int errcode = 0,term_index = 0, fterm_pos = 0, fterm_size = 0;
    int metabin_pos = 0, metabin_size = 0;
    unsigned long deleted;
    uint64_t seq = 0, rev = 0, bp = 0;
    uint64_t size;
    *pInfo = NULL;

    if(v == NULL)
    {
        return DOC_NOT_FOUND;
    }

    //Id/Seq
    error_unless(tuple_check(v->buf, &term_index, 5), ERROR_PARSE_TERM);
    fterm_pos = term_index; //Save position of first term
    ei_skip_term(v->buf, &term_index);
    fterm_size = term_index - fterm_pos; //and size.

    //Rev = {RevNum, MetaBin}
    error_unless(tuple_check(v->buf, &term_index, 2), ERROR_PARSE_TERM);
    error_nonzero(ei_decode_ulonglong(v->buf, &term_index, &rev), ERROR_PARSE_TERM);
    metabin_pos = term_index + 5; //Save position of meta term
                                  //We know it's an ERL_BINARY_EXT, so the contents are from
                                  //5 bytes in to the end of the term.
    ei_skip_term(v->buf, &term_index);
    metabin_size = term_index - metabin_pos; //and size.

    error_nonzero(ei_decode_ulonglong(v->buf, &term_index, &bp), ERROR_PARSE_TERM);
    error_nonzero(ei_decode_ulong(v->buf, &term_index, &deleted), ERROR_PARSE_TERM);
    error_nonzero(ei_decode_ulonglong(v->buf, &term_index, &size), ERROR_PARSE_TERM);

    //If first term is seq, we don't need to include it in the buffer
    if(idBytes != 0) fterm_size = 0;
    char* infobuf = malloc(sizeof(DocInfo) + metabin_size + fterm_size + idBytes);
    *pInfo = (DocInfo*) infobuf;

    (*pInfo)->meta.buf = infobuf + sizeof(DocInfo);
    (*pInfo)->meta.size = metabin_size;

    (*pInfo)->id.buf = infobuf + sizeof(DocInfo) + metabin_size;

    if(idBytes != 0) //First term is Seq
    {

        (*pInfo)->id.size = idBytes;
        ei_decode_ulonglong(v->buf, &fterm_pos, &seq);
        //Let the caller fill in the Id.
    }
    else //First term is Id
    {
        (*pInfo)->id.size = fterm_size - 5; //Id will be a binary.
        memcpy((*pInfo)->id.buf, v->buf + fterm_pos + 5, fterm_size);
        //Let the caller fill in the Seq
    }

    (*pInfo)->seq = seq;
    (*pInfo)->rev = rev;
    (*pInfo)->bp = bp;
    (*pInfo)->size = size;
    (*pInfo)->deleted = deleted;

cleanup:
    if(errcode < 0 && (*pInfo))
    {
        free(*pInfo);
        *pInfo = NULL;
    }
    return errcode;
}

//Fill in doc from reading file.
int bp_to_doc(Doc **pDoc, int fd, off_t bp)
{
    int errcode = 0;
    uint32_t jsonlen, hasbin;
    size_t jsonlen_uncompressed = 0;
    size_t bodylen;
    char *docbody;

    bodylen = pread_bin(fd, bp, &docbody);
    error_unless(bodylen > 0, ERROR_READ);

    memcpy(&jsonlen, docbody, 4);
    jsonlen = ntohl(jsonlen);
    hasbin = jsonlen & 0x80000000;
    jsonlen = jsonlen & ~0x80000000;
    if(!hasbin)
        jsonlen = bodylen - 4; //Should be true anyway..

    //couch uncompress
    if(docbody[4] == 1) //Need to unsnappy;
    {
        error_unless(snappy_uncompressed_length(docbody + 5, jsonlen - 1, &jsonlen_uncompressed) != SNAPPY_OK, ERROR_READ)
    }
    //Fill out doc structure.
    char *docbuf = malloc(sizeof(Doc) + (bodylen - 4) + jsonlen_uncompressed); //meta and binary and json
    error_unless(docbuf, ERROR_ALLOC_FAIL);

    *pDoc = (Doc*) docbuf;
    docbuf += sizeof(Doc);

    memcpy(docbuf, docbody + 4, bodylen - 4);
    if(hasbin)
    {
        (*pDoc)->binary.buf = docbuf + jsonlen;
    }
    else
    {
        (*pDoc)->binary.buf = NULL;
    }
    (*pDoc)->binary.size = (bodylen - 4) - jsonlen;

    if(docbody[4] == 1)
    {
        (*pDoc)->json.buf = docbuf + (bodylen -4);
        error_unless(snappy_uncompress(docbody + 5, jsonlen - 1, (*pDoc)->json.buf, &jsonlen_uncompressed), ERROR_READ);
        (*pDoc)->json.buf += 6;
        (*pDoc)->json.size = jsonlen_uncompressed - 6;
    }
    else
    {
        (*pDoc)->binary.size = (bodylen - 4) - jsonlen;
        (*pDoc)->json.buf = docbuf + 6;
        (*pDoc)->json.size = jsonlen - 6;
    }

    free(docbody);
cleanup:
    if(errcode < 0 && (*pDoc))
    {
        free(*pDoc);
        (*pDoc) = NULL;
    }
    return errcode;
}

int docinfo_fetch(couchfile_lookup_request *rq, void *k, sized_buf *v)
{
    int errcode = 0;
    sized_buf *id = k;
    DocInfo** pInfo = rq->callback_ctx;
    try(docinfo_from_buf(pInfo, v, id->size));
    memcpy((*pInfo)->id.buf, id->buf, id->size);
cleanup:
    return errcode;
}

int docinfo_by_id(Db* db, uint8_t* id,  size_t idlen, DocInfo** pInfo)
{
    sized_buf key;
    void *keylist = &key;
    couchfile_lookup_request rq;
    sized_buf cmptmp;
    int errcode = 0;

    if(db->header.by_id_root == NULL)
        return DOC_NOT_FOUND;

    key.buf = id;
    key.size = idlen;

    rq.cmp.compare = ebin_cmp;
    rq.cmp.from_ext = ebin_from_ext;
    rq.cmp.arg = &cmptmp;
    rq.fd = db->fd;
    rq.num_keys = 1;
    rq.keys = &keylist;
    rq.callback_ctx = pInfo;
    rq.fetch_callback = docinfo_fetch;
    rq.fold = 0;

    errcode = btree_lookup(&rq, db->header.by_id_root->pointer);
    if(errcode == 0)
    {
        if(*pInfo == NULL)
            errcode = DOC_NOT_FOUND;
    }
    return errcode;
}

int open_doc_with_docinfo(Db* db, DocInfo* docinfo, Doc** pDoc, uint64_t options)
{
    int errcode = 0;
    *pDoc = NULL;
    try(bp_to_doc(pDoc, db->fd, docinfo->bp));
    (*pDoc)->id.buf = docinfo->id.buf;
    (*pDoc)->id.size = docinfo->id.size;

cleanup:
    return errcode;
}

int open_doc(Db* db, uint8_t* id,  size_t idlen, Doc** pDoc, uint64_t options)
{
    int errcode = 0;
    DocInfo *info;

    *pDoc = NULL;
    try(docinfo_by_id(db, id, idlen, &info));
    try(open_doc_with_docinfo(db, info, pDoc, options));
    (*pDoc)->id.buf = id;
    (*pDoc)->id.size = idlen;

    free_docinfo(info);
cleanup:
    return errcode;
}

int byseq_do_callback(couchfile_lookup_request *rq, void *k, sized_buf *v)
{
    int vpos = 0;
    int(*real_callback)(Db* db, DocInfo* docinfo, void *ctx) = ((void**)rq->callback_ctx)[0];
    if(v == NULL) return 0;
    sized_buf *seqterm = k;
    int seqindex = 0;
    DocInfo* docinfo;
    docinfo_from_buf(&docinfo, v, 0);
    ei_decode_ulonglong(seqterm->buf, &seqindex, &docinfo->seq);
    real_callback(((void**)rq->callback_ctx)[1], docinfo, ((void**)rq->callback_ctx)[2]);
    free_docinfo(docinfo);
    return 0;
}

int changes_since(Db* db, uint64_t since, uint64_t options,
        int(*f)(Db* db, DocInfo* docinfo, void *ctx), void *ctx)
{
    char since_termbuf[10];
    sized_buf since_term;
    void *keylist = &since_term;
    void *cbctx[3];
    couchfile_lookup_request rq;
    sized_buf cmptmp;
    int errcode = 0;

    if(db->header.by_seq_root == NULL)
        return 0;

    since_term.buf = since_termbuf;
    since_term.size = 0;
    ei_encode_ulonglong(since_termbuf, (int*) &since_term.size, since);

    cbctx[0] = f;
    cbctx[1] = db;
    cbctx[2] = ctx;

    rq.cmp.compare = long_term_cmp;
    rq.cmp.from_ext = term_from_ext;
    rq.cmp.arg = &cmptmp;
    rq.fd = db->fd;
    rq.num_keys = 1;
    rq.keys = &keylist;
    rq.callback_ctx = cbctx;
    rq.fetch_callback = byseq_do_callback;
    rq.fold = 1;

    errcode = btree_lookup(&rq, db->header.by_seq_root->pointer);
    return errcode;
}

void free_doc(Doc* doc)
{
    free(doc);
}

void free_docinfo(DocInfo* docinfo)
{
    free(docinfo);
}

void copy_term(char* dst, int *index, sized_buf* term)
{
    memcpy(dst + *index, term->buf, term->size);
    *index += term->size;
}

int assemble_index_value(DocInfo* docinfo, char* dst, sized_buf* first_term)
{
    int pos = 0;
    ei_encode_tuple_header(dst, &pos, 5); //3 bytes.

    //Id or Seq
    copy_term(dst, &pos, first_term); //first_term.size
    //Rev
    ei_encode_tuple_header(dst, &pos, 2); //3 bytes.
    ei_encode_ulonglong(dst, &pos, docinfo->rev); //Max 10 bytes
    copy_term(dst, &pos, &docinfo->meta); //meta.size
    //Bp
    ei_encode_ulonglong(dst, &pos, docinfo->bp); //Max 10 bytes
    //Deleted
    if(docinfo->deleted)                       //Max 7 bytes (SMALL_ATOM_EXT)
        ei_encode_atom(dst, &pos, "false");
    else
        ei_encode_atom(dst, &pos, "true");
    //Size
    ei_encode_ulonglong(dst, &pos, docinfo->rev); //Max 10 bytes

    //Max 43 + first_term.size + meta.size bytes.
    return pos;
}

int save_doc(Db* db, Doc* doc, uint64_t options)
{
    sized_buf seq_btree_value, id_btree_value;
    // Btree values are {Id/Seq, Rev, Bp, Deleted, Size}
    return -99;
}

