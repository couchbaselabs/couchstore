#include <libcouchstore/couch_db.h>
#include "../src/fatbuf.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "macros.h"

#define ZERO(V) memset(&(V), 0, sizeof(V))
#define SETDOC(N, I, J, B, M) setdoc(&testdocset.docs[N], &testdocset.infos[N], I, sizeof(I) - 1, \
                                  J, sizeof(J) - 1, B, sizeof(B) - 1, M, sizeof(M))

typedef struct _counterset
{
    int totaldocs;
    int deleted;
} counterset;

typedef struct _docset
{
    Doc* docs;
    DocInfo* infos;
    int size;
    int pos;
    counterset counters;
} docset;

counterset counters;
docset testdocset;
fatbuf* docsetbuf = NULL;

void setdoc(Doc* doc, DocInfo* info, char* id, int idlen, char* json, int jsonlen, char* bin, int binlen, char* meta, int metalen)
{
    doc->id.buf = id;
    doc->id.size = idlen;
    doc->json.buf = json;
    doc->json.size = jsonlen;
    doc->binary.buf = bin;
    doc->binary.size = binlen;
    info->meta.buf = meta;
    info->meta.size = metalen;
    info->rev = 1;
    info->size = 0;
    info->deleted = 0;
    info->id = doc->id;
}

void docset_init(int numdocs)
{
    testdocset.size = numdocs;
    testdocset.pos = 0;
    if(docsetbuf)
    {
        fatbuf_free(docsetbuf);
        docsetbuf = NULL;
    }

    docsetbuf = fatbuf_alloc(numdocs * (sizeof(Doc) + sizeof(DocInfo)));
    testdocset.docs = fatbuf_get(docsetbuf, numdocs * sizeof(Doc));
    testdocset.infos = fatbuf_get(docsetbuf, numdocs * sizeof(DocInfo));
    ZERO(testdocset.counters);
}

int counter_inc(Db* db, DocInfo* info, void *ctx)
{
    counterset* ctr = ctx;
    ctr->totaldocs++;
    if(info->deleted)
        ctr->deleted++;
    return 0;
}

#define EQUAL_DOC_BUF(f) assert(memcmp(doc-> f .buf, testdocset.docs[testdocset.pos]. f .buf, doc-> f .size) == 0)
#define EQUAL_INFO_BUF(f) assert(memcmp(info-> f  .buf, testdocset.infos[testdocset.pos]. f .buf, info-> f .size) == 0)
int docset_check(Db* db, DocInfo* info, void *ctx)
{
    int errcode = 0;
    docset* ds = ctx;
    counterset* ctr = &ds->counters;
    ctr->totaldocs++;
    if(info->deleted)
        ctr->deleted++;
    EQUAL_INFO_BUF(id);
    EQUAL_INFO_BUF(meta);
    Doc* doc;
    try(open_doc_with_docinfo(db, info, &doc, 0));
    if(testdocset.docs[testdocset.pos].json.size > 0)
    {
        assert(doc);
        EQUAL_DOC_BUF(json);
        EQUAL_DOC_BUF(id);
        if(testdocset.docs[testdocset.pos].binary.size > 0)
            EQUAL_DOC_BUF(binary);
        else
            assert(doc->binary.size == 0);
    }
    testdocset.pos++;
    free_doc(doc);
cleanup:
    assert(errcode == 0);
    return 0;
}

int dump_count(Db* db)
{
    int errcode = 0;
    ZERO(counters);
    try(changes_since(db, 0, 0, counter_inc, &counters));
cleanup:
    assert(errcode == 0);
    return errcode;
}
char zerometa[] = {1, 2, 3, 4};
void test_save_docs()
{
    fprintf(stderr, "save_docs... "); fflush(stderr);
    int errcode = 0;
    docset_init(4);
    SETDOC(0, "doc1", "{\"test_doc_index\":1}", "test binary 1", zerometa);
    SETDOC(1, "doc2", "{\"test_doc_index\":2}", "test binary 2", zerometa);
    SETDOC(2, "doc3", "{\"test_doc_index\":3}", "test binary 3", zerometa);
    SETDOC(3, "doc4", "{\"test_doc_index\":4}", "test binary 4", zerometa);
    unlink("test.couch");
    Db* db;
    try(open_db("test.couch", COUCH_CREATE_FILES, &db));
    try(save_docs(db, testdocset.docs, testdocset.infos, 4, 0));
    try(commit_all(db, 0));
    close_db(db);
    //Read back
    try(open_db("test.couch", 0, &db));
    try(changes_since(db, 0, 0, docset_check, &testdocset));
    assert(testdocset.counters.totaldocs == 4);
    assert(testdocset.counters.deleted == 0);
    close_db(db);
cleanup:
    assert(errcode == 0);
}

void test_save_doc()
{
    fprintf(stderr, "save_doc... "); fflush(stderr);
    int errcode = 0;
    docset_init(4);
    SETDOC(0, "doc1", "{\"test_doc_index\":1}", "test binary 1", zerometa);
    SETDOC(1, "doc2", "{\"test_doc_index\":2}", "test binary 2", zerometa);
    SETDOC(2, "doc3", "{\"test_doc_index\":3}", "test binary 3", zerometa);
    SETDOC(3, "doc4", "{\"test_doc_index\":4}", "test binary 4", zerometa);
    unlink("test.couch");
    Db* db;
    try(open_db("test.couch", COUCH_CREATE_FILES, &db));
    try(save_doc(db, &testdocset.docs[0], &testdocset.infos[0], 0));
    try(save_doc(db, &testdocset.docs[1], &testdocset.infos[1], 0));
    try(save_doc(db, &testdocset.docs[2], &testdocset.infos[2], 0));
    try(save_doc(db, &testdocset.docs[3], &testdocset.infos[3], 0));
    try(commit_all(db, 0));
    close_db(db);
    //Read back
    try(open_db("test.couch", 0, &db));
    try(changes_since(db, 0, 0, docset_check, &testdocset));
    assert(testdocset.counters.totaldocs == 4);
    assert(testdocset.counters.deleted == 0);
    close_db(db);
cleanup:
    assert(errcode == 0);
}

void test_dump_empty_db()
{
    fprintf(stderr, "dump empty db... "); fflush(stderr);
    unlink("test.couch");
    Db* db;
    open_db("test.couch", COUCH_CREATE_FILES, &db);
    close_db(db);
    open_db("test.couch", 0, &db);
    dump_count(db);
    assert(counters.totaldocs == 0);
    assert(counters.deleted == 0);
    close_db(db);
}

void test_local_docs()
{
    fprintf(stderr, "local docs... "); fflush(stderr);
    int errcode = 0;
    Db* db;
    LocalDoc lDocWrite;
    LocalDoc *lDocRead = NULL;
    unlink("test.couch");
    try(open_db("test.couch", COUCH_CREATE_FILES, &db));
    lDocWrite.id.buf = "_local/testlocal";
    lDocWrite.id.size = 16;
    lDocWrite.json.buf = "{\"test\":true}";
    lDocWrite.json.size = 13;
    lDocWrite.deleted = 0;
    save_local_doc(db, &lDocWrite);
    commit_all(db, 0);
    close_db(db);
    open_db("test.couch", 0, &db);
    open_local_doc(db, "_local/testlocal", 16, &lDocRead);
    assert(lDocRead);
    assert(lDocRead->json.size == 13);
    assert(memcmp(lDocRead->json.buf, "{\"test\":true}", 13) == 0);
    free_local_doc(lDocRead);
    close_db(db);
cleanup:
    assert(errcode == 0);
}

void test_open_file_error()
{
    fprintf(stderr, "opening nonexistent file errors... "); fflush(stderr);
    unlink("test.couch");
    Db* db;
    int errcode = open_db("test.couch", 0, &db);
    assert(errcode == ERROR_OPEN_FILE);
}


int main(void)
{
    test_open_file_error(); fprintf(stderr, "OK \n");
    test_dump_empty_db(); fprintf(stderr," OK\n");
    test_save_doc(); fprintf(stderr," OK\n");
    test_save_docs(); fprintf(stderr," OK\n");
    test_local_docs(); fprintf(stderr," OK\n");

    return 0;
}
