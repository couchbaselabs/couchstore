/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <unistd.h>
#include <libcouchstore/couch_db.h>
#include "../src/fatbuf.h"
#include "../src/internal.h"
#include "../src/bitfield.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "macros.h"

#define ZERO(V) memset(&(V), 0, sizeof(V))
#define SETDOC(N, I, D, M)  \
   setdoc(&testdocset.docs[N], &testdocset.infos[N], I, sizeof(I) - 1, \
         D, sizeof(D) - 1, M, sizeof(M)); testdocset.datasize += sizeof(D) - 1;

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

static void setdoc(Doc *doc, DocInfo *info, char *id, int idlen,
                   char *data, int datalen, char *meta, int metalen)
{
    memset(doc, 0, sizeof(Doc));
    memset(info, 0, sizeof(DocInfo));
    doc->id.buf = id;
    doc->id.size = idlen;
    doc->data.buf = data;
    doc->data.size = datalen;
    info->rev_meta.buf = meta;
    info->rev_meta.size = metalen;
    info->content_meta = 0;
    info->rev_seq = 1;
    info->size = 0;
    info->deleted = 0;
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

static int dump_count(Db *db)
{
    int errcode = 0;
    ZERO(counters);
    try(couchstore_changes_since(db, 0, 0, counter_inc, &counters));
cleanup:
    assert(errcode == 0);
    return errcode;
}
char zerometa[] = {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3};
static void test_save_docs(int count, const char *doc_tpl)
{
    int errcode = 0;
    int i;
    char *idBuf, *valueBuf;
    Doc **docptrs;
    DocInfo **nfoptrs;
    uint64_t idtreesize = 0;
    uint64_t seqtreesize = 0;
    uint64_t docssize = 0;
    uint64_t dbfilesize = 0;

    fprintf(stderr, "save_docs (doc count %d)... ", count);
    fflush(stderr);

    docset_init(count);
    for (i = 0; i < count; ++i) {
        idBuf = (char *) malloc(sizeof(char) * 32);
        assert(idBuf != NULL);
        sprintf(idBuf, "doc%d", i + 1);
        valueBuf = (char *) malloc(sizeof(char) * (strlen(doc_tpl) + 20));
        assert(valueBuf != NULL);
        sprintf(valueBuf, doc_tpl, i + 1);
        SETDOC(i, idBuf, valueBuf, zerometa);
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

    unlink("test.couch");
    Db *db;
    try(couchstore_open_db("test.couch", COUCHSTORE_OPEN_FLAG_CREATE, &db));
    assert(strcmp(couchstore_get_db_filename(db), "test.couch") == 0);
    try(couchstore_save_documents(db, docptrs, nfoptrs, count, 0));
    try(couchstore_commit(db));
    couchstore_close_db(db);

    //Read back
    try(couchstore_open_db("test.couch", 0, &db));
    try(couchstore_changes_since(db, 0, 0, docset_check, &testdocset));
    assert(testdocset.counters.totaldocs == count);
    assert(testdocset.counters.deleted == 0);

    idtreesize = db->header.by_id_root->subtreesize;
    seqtreesize = db->header.by_seq_root->subtreesize;
    docssize = get_48(db->header.by_id_root->reduce_value.buf + 10);
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
    unlink("test.couch");
    Db *db;
    try(couchstore_open_db("test.couch", COUCHSTORE_OPEN_FLAG_CREATE, &db));
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
    //Read back
    try(couchstore_open_db("test.couch", 0, &db));
    try(couchstore_changes_since(db, 0, 0, docset_check, &testdocset));
    assert(testdocset.counters.totaldocs == 4);
    assert(testdocset.counters.deleted == 0);
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
    testdocset.infos[1].content_meta = 128; //Mark doc2 as to be snappied.
    unlink("test.couch");
    Db *db;
    try(couchstore_open_db("test.couch", COUCHSTORE_OPEN_FLAG_CREATE, &db));
    try(couchstore_save_documents(db, docptrs, nfoptrs, 2,
                                      COMPRESS_DOC_BODIES));
    try(couchstore_commit(db));
    couchstore_close_db(db);
    //Read back
    try(couchstore_open_db("test.couch", 0, &db));
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
    unlink("test.couch");
    Db *db;
    couchstore_open_db("test.couch", COUCHSTORE_OPEN_FLAG_CREATE, &db);
    couchstore_close_db(db);
    couchstore_open_db("test.couch", 0, &db);
    dump_count(db);
    assert(counters.totaldocs == 0);
    assert(counters.deleted == 0);
    couchstore_close_db(db);
}

static void test_local_docs(void)
{
    fprintf(stderr, "local docs... ");
    fflush(stderr);
    int errcode = 0;
    Db *db;
    LocalDoc lDocWrite;
    LocalDoc *lDocRead = NULL;
    unlink("test.couch");
    try(couchstore_open_db("test.couch", COUCHSTORE_OPEN_FLAG_CREATE, &db));
    lDocWrite.id.buf = "_local/testlocal";
    lDocWrite.id.size = 16;
    lDocWrite.json.buf = "{\"test\":true}";
    lDocWrite.json.size = 13;
    lDocWrite.deleted = 0;
    couchstore_save_local_document(db, &lDocWrite);
    couchstore_commit(db);
    couchstore_close_db(db);
    couchstore_open_db("test.couch", 0, &db);
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
    unlink("test.couch");
    Db *db;
    int errcode = couchstore_open_db("test.couch", 0, &db);
    assert(errcode == COUCHSTORE_ERROR_NO_SUCH_FILE);
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
    memcpy(buffer, info->id.buf, info->size);
    buffer[info->size] = 0; // null terminate
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
    unlink("test.couch");
    try(couchstore_open_db("test.couch", COUCHSTORE_OPEN_FLAG_CREATE, &db));
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
    try(couchstore_open_db("test.couch", 0, &db));
    for (i=0; i < numdocs; i += updatebatch) {
        // now do bulk updates and check the changes for dups
        try(couchstore_save_documents(db, docptrs + i, nfoptrs + i, updatebatch, 0));
        try(couchstore_commit(db));
        memset(docmap, 0, numdocs);
        try(couchstore_changes_since(db, 0, 0, docmap_check, docmap));
    }
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


int main(void)
{
    int doc_counts[] = { 4, 69, 666, 9090, 99999 };
    int i;
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

    return 0;
}
