/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <unistd.h>
#include <libcouchstore/couch_db.h>
#include "../src/fatbuf.h"
#include "../src/internal.h"
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
static void test_save_docs(void)
{
    fprintf(stderr, "save_docs... ");
    fflush(stderr);
    int errcode = 0;
    docset_init(4);
    SETDOC(0, "doc1", "{\"test_doc_index\":1}", zerometa);
    SETDOC(1, "doc2", "{\"test_doc_index\":2}", zerometa);
    SETDOC(2, "doc3", "{\"test_doc_index\":3}", zerometa);
    SETDOC(3, "doc4", "{\"test_doc_index\":4}", zerometa);
    Doc *docptrs [4] =  { &testdocset.docs[0],
                          &testdocset.docs[1],
                          &testdocset.docs[2],
                          &testdocset.docs[3]
                        };
    DocInfo *nfoptrs [4] =  { &testdocset.infos[0],
                              &testdocset.infos[1],
                              &testdocset.infos[2],
                              &testdocset.infos[3]
                            };
    unlink("test.couch");
    Db *db;
    try(couchstore_open_db("test.couch", COUCHSTORE_OPEN_FLAG_CREATE, &db));
    try(couchstore_save_documents(db, docptrs, nfoptrs, 4, 0));
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


int main(void)
{
    test_open_file_error();
    fprintf(stderr, "OK \n");
    test_dump_empty_db();
    fprintf(stderr, " OK\n");
    test_save_doc();
    fprintf(stderr, " OK\n");
    test_save_docs();
    fprintf(stderr, " OK\n");
    test_local_docs();
    fprintf(stderr, " OK\n");
    test_compressed_doc_body();
    fprintf(stderr, " OK\n");

    return 0;
}
