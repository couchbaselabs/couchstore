/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <unistd.h>
#include <libcouchstore/couch_db.h>
#include "../src/fatbuf.h"
#include "../src/internal.h"
#include "../src/node_types.h"
#include "../src/reduces.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "macros.h"

#define ZERO(V) memset(&(V), 0, sizeof(V))
//Only use the macro SETDOC with constants!
//it uses sizeof()
#define SETDOC(N, I, D, M)  \
   setdoc(&testdocset.docs[N], &testdocset.infos[N], I, strlen(I), \
         D, sizeof(D) - 1, M, strlen(M)); testdocset.datasize += sizeof(D) - 1;

typedef struct _counterset {
    int totaldocs;
    int deleted;
} counterset;

typedef struct _docset {
    Doc *docs;
    DocInfo *infos;
    int size;
    int pos;
    uint64_t datasize;
    counterset counters;
} docset;

counterset counters;
docset testdocset;
fatbuf *docsetbuf = NULL;
char testfilepath[1024] = "testfile.couch";

static void print_os_err() {
    char emsg[512];
    couchstore_last_os_error(emsg, 512);
    fprintf(stderr, "OS Error: %s\n", emsg);
}

static void test_raw_08(uint8_t value)
{
    raw_08 raw;
    raw = encode_raw08(value);
    assert(decode_raw08(raw) == value);
}

static void test_raw_16(uint16_t value)
{
    raw_16 raw;
    raw = encode_raw16(value);
    assert(decode_raw16(raw) == value);
}

static void test_raw_32(uint32_t value)
{
    raw_32 raw;
    raw = encode_raw32(value);
    assert(decode_raw32(raw) == value);
}

static void test_raw_40(uint64_t value, const uint8_t expected[8])
{
    union {
        raw_40 raw;
        uint8_t bytes[8];
    } data;
    memset(&data, 0, sizeof(data));
    data.raw = encode_raw40(value);
    assert(memcmp(data.bytes, expected, 8) == 0);
    assert(decode_raw40(data.raw) == value);
}

static void test_raw_48(uint64_t value, const uint8_t expected[8])
{
    union {
        raw_48 raw;
        uint8_t bytes[8];
    } data;
    memset(&data, 0, sizeof(data));
    data.raw = encode_raw48(value);
    assert(memcmp(data.bytes, expected, 8) == 0);
    assert(decode_raw48(data.raw) == value);
}

static void test_bitfield_fns(void)
{
    assert(sizeof(cs_off_t) == 8);

    assert(sizeof(raw_08) == 1);
    assert(sizeof(raw_16) == 2);
    assert(sizeof(raw_32) == 4);
    assert(sizeof(raw_40) == 5);
    assert(sizeof(raw_48) == 6);

    struct {
        raw_08 a;
        raw_48 b;
        raw_16 c;
        raw_40 d;
        raw_32 e;
        raw_08 f;
    } packed;
    assert(sizeof(packed) == 19);
    
    raw_kv_length kv;
    assert(sizeof(kv) == 5);
    kv = encode_kv_length(1234, 123456);
    uint32_t klen, vlen;
    decode_kv_length(&kv, &klen, &vlen);
    assert(klen == 1234);
    assert(vlen == 123456);
    
    test_raw_08(0);
    test_raw_08(UINT8_MAX);
    test_raw_16(0);
    test_raw_16(12345);
    test_raw_16(UINT16_MAX);
    test_raw_32(0);
    test_raw_32(12345678);
    test_raw_32(UINT32_MAX);
    
    uint8_t expected1[8] = {0x12, 0x34, 0x56, 0x78, 0x90};
    test_raw_40(0x1234567890LL, expected1);
    uint8_t expected2[8] = {0x09, 0x87, 0x65, 0x43, 0x21};
    test_raw_40(0x0987654321LL, expected2);
    uint8_t expected3[8] = {0x12, 0x34, 0x56, 0x78, 0x90, 0xAB};
    test_raw_48(0x1234567890ABLL, expected3);
    uint8_t expected4[8] = {0xBA, 0x98, 0x76, 0x54, 0x32, 0x10};
    test_raw_48(0xBA9876543210LL, expected4);
}

static void setdoc(Doc *doc, DocInfo *info, char *id, size_t idlen,
                   char *data, size_t datalen, char *meta, size_t metalen)
{
    memset(doc, 0, sizeof(Doc));
    memset(info, 0, sizeof(DocInfo));
    doc->id.buf = id;
    doc->id.size = idlen;
    doc->data.buf = data;
    doc->data.size = datalen;
    info->rev_meta.buf = meta;
    info->rev_meta.size = metalen;
    info->id = doc->id;
}

static void docset_init(int numdocs)
{
    testdocset.size = numdocs;
    testdocset.pos = 0;
    testdocset.datasize = 0;
    if (docsetbuf) {
        fatbuf_free(docsetbuf);
        docsetbuf = NULL;
    }

    docsetbuf = fatbuf_alloc(numdocs * (sizeof(Doc) + sizeof(DocInfo)));
    testdocset.docs = fatbuf_get(docsetbuf, numdocs * sizeof(Doc));
    testdocset.infos = fatbuf_get(docsetbuf, numdocs * sizeof(DocInfo));
    ZERO(testdocset.counters);
}

static int counter_inc(Db *db, DocInfo *info, void *ctx)
{
    (void)db;
    counterset *ctr = ctx;
    ctr->totaldocs++;
    if (info->deleted) {
        ctr->deleted++;
    }
    return 0;
}

#define EQUAL_DOC_BUF(f) assert(memcmp(doc-> f .buf, testdocset.docs[testdocset.pos]. f .buf, doc-> f .size) == 0)
#define EQUAL_INFO_BUF(f) assert(memcmp(info-> f  .buf, testdocset.infos[testdocset.pos]. f .buf, info-> f .size) == 0)
static int docset_check(Db *db, DocInfo *info, void *ctx)
{
    int errcode = 0;
    docset *ds = ctx;
    counterset *ctr = &ds->counters;
    ctr->totaldocs++;
    if (info->deleted) {
        ctr->deleted++;
    }
    EQUAL_INFO_BUF(id);
    EQUAL_INFO_BUF(rev_meta);
    Doc *doc;
    try(couchstore_open_doc_with_docinfo(db, info, &doc, DECOMPRESS_DOC_BODIES));
    if (testdocset.docs[testdocset.pos].data.size > 0) {
        assert(doc);
        EQUAL_DOC_BUF(data);
        EQUAL_DOC_BUF(id);
    }
    testdocset.pos++;
    couchstore_free_document(doc);
cleanup:
    assert(errcode == 0);
    return 0;
}

static int dociter_check(Db *db, DocInfo *info, void *ctx)
{
    int errcode = 0;
    docset *ds = ctx;
    counterset *ctr = &ds->counters;
    ctr->totaldocs++;
    if (info->deleted) {
        ctr->deleted++;
    }
    Doc *doc;
    try(couchstore_open_doc_with_docinfo(db, info, &doc, DECOMPRESS_DOC_BODIES));
    assert(doc);
    couchstore_free_document(doc);
cleanup:
    assert(errcode == 0);
    return 0;
}

static int dump_count(Db *db)
{
    int errcode = 0;
    ZERO(counters);
    try(couchstore_changes_since(db, 0, 0, counter_inc, &counters));
cleanup:
    assert(errcode == 0);
    return errcode;
}

static char zerometa[] = {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3};

static void test_save_docs(int count, const char *doc_tpl)
{
    int errcode = 0;
    int i;
    char *idBuf, *valueBuf;
    Doc **docptrs;
    DocInfo **nfoptrs;
    sized_buf *ids;
    uint64_t idtreesize = 0;
    uint64_t seqtreesize = 0;
    uint64_t docssize = 0;
    uint64_t dbfilesize = 0;
    uint64_t *sequences = NULL;

    fprintf(stderr, "save_docs (doc count %d)... ", count);
    fflush(stderr);

    docset_init(count);
    srandom(0xdeadbeef);  // doc IDs should be consistent across runs
    for (i = 0; i < count; ++i) {
        idBuf = (char *) malloc(sizeof(char) * 32);
        assert(idBuf != NULL);
        int idsize = sprintf(idBuf, "doc%d-%lu", i, (unsigned long)random());
        valueBuf = (char *) malloc(sizeof(char) * (strlen(doc_tpl) + 20));
        assert(valueBuf != NULL);
        int valsize = sprintf(valueBuf, doc_tpl, i + 1);
        setdoc(&testdocset.docs[i], &testdocset.infos[i],
                idBuf, idsize, valueBuf, valsize, zerometa, sizeof(zerometa));
        testdocset.datasize += valsize;
    }

    docptrs = (Doc **) malloc(sizeof(Doc*) * count);
    assert(docptrs != NULL);
    for (i = 0; i < count; ++i) {
        docptrs[i] = &testdocset.docs[i];
    }

    nfoptrs = (DocInfo **) malloc(sizeof(DocInfo*) * count);
    assert(nfoptrs != NULL);
    for (i = 0; i < count; ++i) {
        nfoptrs[i] = &testdocset.infos[i];
    }

    unlink(testfilepath);
    Db *db;
    try(couchstore_open_db(testfilepath, COUCHSTORE_OPEN_FLAG_CREATE, &db));
    assert(strcmp(couchstore_get_db_filename(db), testfilepath) == 0);
    try(couchstore_save_documents(db, docptrs, nfoptrs, count, 0));
    try(couchstore_commit(db));
    couchstore_close_db(db);

    try(couchstore_open_db(testfilepath, 0, &db));

    // Read back by doc ID:
    fprintf(stderr, "get by ID... ");
    testdocset.pos = 0;
    for (i = 0; i < count; ++i) {
        DocInfo* out_info;
        try(couchstore_docinfo_by_id(db, testdocset.docs[i].id.buf, testdocset.docs[i].id.size,
                                     &out_info));
        docset_check(db, out_info, &testdocset);
        couchstore_free_docinfo(out_info);
    }

    // Read back in bulk by doc ID:
    fprintf(stderr, "bulk IDs... ");
    ids = malloc(count * sizeof(sized_buf));
    for (i = 0; i < count; ++i) {
        ids[i] = docptrs[i]->id;
    }
    ZERO(testdocset.counters);
    try(couchstore_docinfos_by_id(db, ids, count, 0, dociter_check, &testdocset));
    assert(testdocset.counters.totaldocs == count);
    assert(testdocset.counters.deleted == 0);

    // Read back by sequence:
    fprintf(stderr, "get by sequence... ");
    sequences = malloc(count * sizeof(*sequences));
    testdocset.pos = 0;
    for (i = 0; i < count; ++i) {
        DocInfo* out_info;
        sequences[i] = testdocset.infos[i].db_seq;
        assert(sequences[i] == (uint64_t)i + 1);
        try(couchstore_docinfo_by_sequence(db, testdocset.infos[i].db_seq, &out_info));
        docset_check(db, out_info, &testdocset);
        couchstore_free_docinfo(out_info);
    }

    // Read back in bulk by sequence:
    fprintf(stderr, "bulk sequences... ");
    testdocset.pos = 0;
    ZERO(testdocset.counters);
    try(couchstore_docinfos_by_sequence(db, sequences, count, 0, docset_check, &testdocset));
    assert(testdocset.counters.totaldocs == count);
    assert(testdocset.counters.deleted == 0);

    // Read back using changes_since:
    fprintf(stderr, "changes_since... ");
    testdocset.pos = 0;
    ZERO(testdocset.counters);
    try(couchstore_changes_since(db, 0, 0, docset_check, &testdocset));
    assert(testdocset.counters.totaldocs == count);
    assert(testdocset.counters.deleted == 0);

    idtreesize = db->header.by_id_root->subtreesize;
    seqtreesize = db->header.by_seq_root->subtreesize;
    const raw_by_id_reduce *reduce = (const raw_by_id_reduce*)db->header.by_id_root->reduce_value.buf;
    docssize = decode_raw48(reduce->size);
    dbfilesize = db->file_pos;

    assert(dbfilesize > 0);
    assert(idtreesize > 0);
    assert(seqtreesize > 0);
    assert(docssize > 0);
    assert(idtreesize < dbfilesize);
    assert(seqtreesize < dbfilesize);
    assert(docssize < dbfilesize);
    assert(db->header.local_docs_root == NULL);
    assert((idtreesize + seqtreesize + docssize) < dbfilesize);

    couchstore_close_db(db);
cleanup:
    free(ids);
    free(sequences);
    for (i = 0; i < count; ++i) {
        free(docptrs[i]->id.buf);
        free(docptrs[i]->data.buf);
    }
    free(docptrs);
    free(nfoptrs);
    assert(errcode == 0);
}

static void test_save_doc(void)
{
    fprintf(stderr, "save_doc... ");
    fflush(stderr);
    int errcode = 0;
    docset_init(4);
    SETDOC(0, "doc1", "{\"test_doc_index\":1}", zerometa);
    SETDOC(1, "doc2", "{\"test_doc_index\":2}", zerometa);
    SETDOC(2, "doc3", "{\"test_doc_index\":3}", zerometa);
    SETDOC(3, "doc4", "{\"test_doc_index\":4}", zerometa);
    unlink(testfilepath);
    Db *db;
    try(couchstore_open_db(testfilepath, COUCHSTORE_OPEN_FLAG_CREATE, &db));
    try(couchstore_save_document(db, &testdocset.docs[0],
                                     &testdocset.infos[0], 0));
    try(couchstore_save_document(db, &testdocset.docs[1],
                                     &testdocset.infos[1], 0));
    try(couchstore_save_document(db, &testdocset.docs[2],
                                     &testdocset.infos[2], 0));
    try(couchstore_save_document(db, &testdocset.docs[3],
                                     &testdocset.infos[3], 0));
    try(couchstore_commit(db));
    couchstore_close_db(db);

    // Check that sequence numbers got filled in
    unsigned i;
    for (i = 0; i < 4; ++i) {
        assert(testdocset.infos[i].db_seq == i + 1);
    }

    //Read back
    try(couchstore_open_db(testfilepath, 0, &db));
    try(couchstore_changes_since(db, 0, 0, docset_check, &testdocset));
    assert(testdocset.counters.totaldocs == 4);
    assert(testdocset.counters.deleted == 0);

    DbInfo info;
    assert(couchstore_db_info(db, &info) == COUCHSTORE_SUCCESS);
    assert(info.last_sequence == 4);
    assert(info.doc_count == 4);
    assert(info.deleted_count == 0);
    assert(info.header_position == 4096);

    couchstore_close_db(db);
cleanup:
    assert(errcode == 0);
}

static void test_compressed_doc_body(void)
{
    fprintf(stderr, "compressed bodies... ");
    fflush(stderr);
    int errcode = 0;
    docset_init(2);
    SETDOC(0, "doc1", "{\"test_doc_index\":1, \"val\":\"blah blah blah blah blah blah\"}", zerometa);
    SETDOC(1, "doc2", "{\"test_doc_index\":2, \"val\":\"blah blah blah blah blah blah\"}", zerometa);
    Doc *docptrs [2] =  { &testdocset.docs[0],
                          &testdocset.docs[1]
                        };
    DocInfo *nfoptrs [2] =  { &testdocset.infos[0],
                              &testdocset.infos[1]
                            };
    testdocset.infos[1].content_meta = COUCH_DOC_IS_COMPRESSED; //Mark doc2 as to be snappied.
    unlink(testfilepath);
    Db *db;
    try(couchstore_open_db(testfilepath, COUCHSTORE_OPEN_FLAG_CREATE, &db));
    try(couchstore_save_documents(db, docptrs, nfoptrs, 2,
                                      COMPRESS_DOC_BODIES));
    try(couchstore_commit(db));
    couchstore_close_db(db);
    //Read back
    try(couchstore_open_db(testfilepath, 0, &db));
    try(couchstore_changes_since(db, 0, 0, docset_check, &testdocset));
    assert(testdocset.counters.totaldocs == 2);
    assert(testdocset.counters.deleted == 0);
    couchstore_close_db(db);
cleanup:
    assert(errcode == 0);
}

static void test_dump_empty_db(void)
{
    fprintf(stderr, "dump empty db... ");
    fflush(stderr);
    unlink(testfilepath);
    Db *db;
    couchstore_error_t errcode;
    
    try(couchstore_open_db(testfilepath, COUCHSTORE_OPEN_FLAG_CREATE, &db));
    try(couchstore_close_db(db));
    try(couchstore_open_db(testfilepath, 0, &db));
    dump_count(db);
    assert(counters.totaldocs == 0);
    assert(counters.deleted == 0);

    DbInfo info;
    assert(couchstore_db_info(db, &info) == COUCHSTORE_SUCCESS);
    assert(strcmp(info.filename, testfilepath) == 0);
    assert(info.last_sequence == 0);
    assert(info.doc_count == 0);
    assert(info.deleted_count == 0);
    assert(info.space_used == 0);
    assert(info.header_position == 0);

    couchstore_close_db(db);
cleanup:
    assert(errcode == 0);
}

static void test_local_docs(void)
{
    fprintf(stderr, "local docs... ");
    fflush(stderr);
    int errcode = 0;
    Db *db;
    LocalDoc lDocWrite;
    LocalDoc *lDocRead = NULL;
    unlink(testfilepath);
    try(couchstore_open_db(testfilepath, COUCHSTORE_OPEN_FLAG_CREATE, &db));
    lDocWrite.id.buf = "_local/testlocal";
    lDocWrite.id.size = 16;
    lDocWrite.json.buf = "{\"test\":true}";
    lDocWrite.json.size = 13;
    lDocWrite.deleted = 0;
    couchstore_save_local_document(db, &lDocWrite);
    couchstore_commit(db);
    couchstore_close_db(db);
    couchstore_open_db(testfilepath, 0, &db);
    couchstore_open_local_document(db, "_local/testlocal", 16, &lDocRead);
    assert(lDocRead);
    assert(lDocRead->json.size == 13);
    assert(memcmp(lDocRead->json.buf, "{\"test\":true}", 13) == 0);
    couchstore_free_local_document(lDocRead);
    couchstore_close_db(db);
cleanup:
    assert(errcode == 0);
}

static void test_open_file_error(void)
{
    fprintf(stderr, "opening nonexistent file errors... ");
    fflush(stderr);
    unlink(testfilepath);
    Db *db;
    int errcode = couchstore_open_db(testfilepath, 0, &db);

    if(errcode != 0) {
        print_os_err();
    }

    assert(errcode == COUCHSTORE_ERROR_NO_SUCH_FILE);

    // make sure os.c didn't accidentally call close(0):
    assert(lseek(0, 0, SEEK_CUR) >= 0 || errno != EBADF);
}

static void shuffle(Doc **docs, DocInfo **docinfos, size_t n)
{
    if (n > 1) {
        size_t i;
        for (i = 0; i < n - 1; i++) {
          size_t j = i + rand() / (RAND_MAX / (n - i) + 1);
          DocInfo *docinfo;
          Doc *doc = docs[j];
          docs[j] = docs[i];
          docs[i] = doc;

          docinfo = docinfos[j];
          docinfos[j] = docinfos[i];
          docinfos[i] = docinfo;
        }
    }
}

static int docmap_check(Db *db, DocInfo *info, void *ctx)
{
    (void)db;
    char* docmap = (char*)ctx;
    int i;
    char buffer[100];
    memcpy(buffer, info->id.buf, info->id.size);
    buffer[info->id.size] = 0; // null terminate
    sscanf(buffer, "doc%d", &i);
    assert(docmap[i] == 0);
    docmap[i] = 1;
    return 0;
}

static void test_changes_no_dups(void)
{
    fprintf(stderr, "changes no dupes... ");
    fflush(stderr);
    int errcode = 0;
    int i;
    const int numdocs = 10000;
    int updatebatch = 1000;
    Doc **docptrs;
    DocInfo **nfoptrs;
    char *docmap;
    Db *db;
    docset_init(numdocs);
    for (i=0; i < numdocs; i++) {
        char* id = malloc(100);
        char* body = malloc(100);
        sprintf(id, "doc%d", i);
        sprintf(body, "{\"test_doc_index\":%d}", i);
        setdoc(&testdocset.docs[i], &testdocset.infos[i],
                id, strlen(id),
                body, strlen(body),
                zerometa, sizeof(zerometa));
    }
    docptrs = malloc(numdocs * sizeof(Doc*));
    nfoptrs = malloc(numdocs * sizeof(DocInfo*));
    docmap = malloc(numdocs);
    for (i=0; i < numdocs; i++) {
        docptrs[i] = &testdocset.docs[i];
        nfoptrs[i] = &testdocset.infos[i];
    }
    unlink(testfilepath);
    try(couchstore_open_db(testfilepath, COUCHSTORE_OPEN_FLAG_CREATE, &db));
    // only save half the docs at first.
    try(couchstore_save_documents(db, docptrs, nfoptrs, numdocs/2, 0));
    try(couchstore_commit(db));
    couchstore_close_db(db);

    for (i=0; i < numdocs/2; i++) {
        // increment the rev for already added docs
        nfoptrs[i]->rev_seq++;
    }
    srand(10); // make deterministic
    // now shuffle so some bulk updates contain previous docs and new docs
    shuffle(docptrs, nfoptrs, numdocs);
    try(couchstore_open_db(testfilepath, 0, &db));
    for (i=0; i < numdocs; i += updatebatch) {
        // now do bulk updates and check the changes for dups
        try(couchstore_save_documents(db, docptrs + i, nfoptrs + i, updatebatch, 0));
        try(couchstore_commit(db));
        memset(docmap, 0, numdocs);
        try(couchstore_changes_since(db, 0, 0, docmap_check, docmap));
    }

    DbInfo info;
    assert(couchstore_db_info(db, &info) == COUCHSTORE_SUCCESS);
    assert(info.last_sequence == (uint64_t)(numdocs + numdocs/2));
    assert(info.doc_count == (uint64_t)numdocs);
    assert(info.deleted_count == 0);

    couchstore_close_db(db);
cleanup:
    for (i=0; i < numdocs; i++) {
        free(docptrs[i]->id.buf);
        free(docptrs[i]->data.buf);
    }
    free(docptrs);
    free(nfoptrs);
    free(docmap);
    assert(errcode == 0);
}


static void mb5086(void)
{
    Db *db;
    Doc d;
    DocInfo i;
    couchstore_error_t err;

    fprintf(stderr, "regression mb-5086.... ");
    fflush(stderr);

    setdoc(&d, &i, "hi", 2, "foo", 3, NULL, 0);
    err = couchstore_open_db("mb5085.couch", COUCHSTORE_OPEN_FLAG_CREATE, &db);
    assert(err == COUCHSTORE_SUCCESS);
    assert(couchstore_save_document(db, &d, &i, 0) == COUCHSTORE_SUCCESS);
    assert(couchstore_commit(db) == COUCHSTORE_SUCCESS);
    assert(couchstore_close_db(db) == COUCHSTORE_SUCCESS);
    assert(remove("mb5085.couch") == 0);
}

static void test_huge_revseq(void)
{
    Db *db;
    Doc d;
    DocInfo i;
    DocInfo *i2;
    couchstore_error_t err;

    fprintf(stderr, "huge rev_seq.... ");
    fflush(stderr);

    setdoc(&d, &i, "hi", 2, "foo", 3, NULL, 0);
    i.rev_seq = 5294967296;

    err = couchstore_open_db("bigrevseq.couch", COUCHSTORE_OPEN_FLAG_CREATE, &db);
    assert(err == COUCHSTORE_SUCCESS);
    assert(couchstore_save_document(db, &d, &i, 0) == COUCHSTORE_SUCCESS);
    assert(couchstore_commit(db) == COUCHSTORE_SUCCESS);
    assert(couchstore_docinfo_by_id(db, "hi", 2, &i2) == COUCHSTORE_SUCCESS);
    assert(i2->rev_seq == 5294967296);
    couchstore_free_docinfo(i2);
    assert(couchstore_close_db(db) == COUCHSTORE_SUCCESS);
    assert(remove("bigrevseq.couch") == 0);
}


int main(int argc, const char *argv[])
{
    int doc_counts[] = { 4, 69, 666, 9090 };
    unsigned i;
    const char *small_doc_tpl = "{\"test_doc_index\":%d}";
    const char *large_doc_tpl =
        "{"
        "\"test_doc_index\":%d,"
        "\"field1\": \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\","
        "\"field2\": \"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\","
        "\"field3\": \"cccccccccccccccccccccccccccccccccccccccccccccccccc"
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc\""
        "}";

    if (argc > 1)
        strcpy(testfilepath, argv[1]);
    printf("Using test database at %s\n", testfilepath);

    test_bitfield_fns();

    test_open_file_error();
    fprintf(stderr, "OK \n");
    test_dump_empty_db();
    fprintf(stderr, " OK\n");
    test_save_doc();
    fprintf(stderr, " OK\n");
    for (i = 0; i < (sizeof(doc_counts) / sizeof(int)); ++i) {
        test_save_docs(doc_counts[i], small_doc_tpl);
        fprintf(stderr, " OK\n");
        test_save_docs(doc_counts[i], large_doc_tpl);
        fprintf(stderr, " OK\n");
    }
    test_local_docs();
    fprintf(stderr, " OK\n");
    test_compressed_doc_body();
    fprintf(stderr, " OK\n");
    test_changes_no_dups();
    fprintf(stderr, " OK\n");
    mb5086();
    fprintf(stderr, " OK\n");
    unlink(testfilepath);
    test_huge_revseq();
    fprintf(stderr, " OK\n");
    unlink(testfilepath);

    // make sure os.c didn't accidentally call close(0):
    assert(lseek(0, 0, SEEK_CUR) >= 0 || errno != EBADF);

    return 0;
}
