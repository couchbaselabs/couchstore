#include "config.h"
#include <unistd.h>
#include <libcouchstore/couch_db.h>
#include "../src/fatbuf.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ei.h>
#include "macros.h"

#define ZERO(V) memset(&(V), 0, sizeof(V))
#define SETDOC(N, I, D, M) setdoc(&testdocset.docs[N], &testdocset.infos[N], I, sizeof(I) - 1, \
                                  D, sizeof(D) - 1, M, sizeof(M)); testdocset.datasize += sizeof(D) - 1;

//Wrapper in couchstore.
int ei_decode_uint64(char *buf, int *index, uint64_t *val);

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

void setdoc(Doc *doc, DocInfo *info, char *id, int idlen, char *data, int datalen, char *meta, int metalen)
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

void docset_init(int numdocs)
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

int counter_inc(Db *db, DocInfo *info, void *ctx)
{
    counterset *ctr = ctx;
    ctr->totaldocs++;
    if (info->deleted) {
        ctr->deleted++;
    }
    return 0;
}

#define EQUAL_DOC_BUF(f) assert(memcmp(doc-> f .buf, testdocset.docs[testdocset.pos]. f .buf, doc-> f .size) == 0)
#define EQUAL_INFO_BUF(f) assert(memcmp(info-> f  .buf, testdocset.infos[testdocset.pos]. f .buf, info-> f .size) == 0)
int docset_check(Db *db, DocInfo *info, void *ctx)
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
    try(open_doc_with_docinfo(db, info, &doc, DECOMPRESS_DOC_BODIES));
    if (testdocset.docs[testdocset.pos].data.size > 0) {
        assert(doc);
        EQUAL_DOC_BUF(data);
        EQUAL_DOC_BUF(id);
    }
    testdocset.pos++;
    free_doc(doc);
cleanup:
    assert(errcode == 0);
    return 0;
}

void assert_id_rv(char *buf, uint64_t deleted, uint64_t notdeleted, uint64_t size)
{
    uint64_t r_deleted, r_notdeleted, r_size;
    int pos = 0;
    assert(ei_decode_tuple_header(buf, &pos, NULL) == 0);
    ei_decode_uint64(buf, &pos, &r_notdeleted);
    ei_decode_uint64(buf, &pos, &r_deleted);
    ei_decode_uint64(buf, &pos, &r_size);
    //fprintf(stderr,"notdeleted, deleted, size = %llu, %llu, %llu\n", notdeleted, deleted, size);
    //fprintf(stderr,"notdeleted, deleted, size = %llu, %llu, %llu\n", r_notdeleted, r_deleted, r_size);
    assert(notdeleted == r_notdeleted);
    assert(deleted == r_deleted);
    assert(size == r_size);

}
//Check the toplevel reduces on the db headers.
void check_reductions(Db *db)
{
    assert_id_rv(db->header.by_id_root->reduce_value.buf,
                 testdocset.counters.deleted, testdocset.counters.totaldocs - testdocset.counters.deleted, testdocset.datasize);
}

int dump_count(Db *db)
{
    int errcode = 0;
    ZERO(counters);
    try(changes_since(db, 0, 0, counter_inc, &counters));
cleanup:
    assert(errcode == 0);
    return errcode;
}
char zerometa[] = {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3};
void test_save_docs()
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
    try(open_db("test.couch", COUCH_CREATE_FILES, NULL, &db));
    try(save_docs(db, docptrs, nfoptrs, 4, 0));
    try(commit_all(db, 0));
    close_db(db);
    //Read back
    try(open_db("test.couch", 0, NULL, &db));
    try(changes_since(db, 0, 0, docset_check, &testdocset));
    assert(testdocset.counters.totaldocs == 4);
    assert(testdocset.counters.deleted == 0);
    check_reductions(db);
    close_db(db);
cleanup:
    assert(errcode == 0);
}

void test_save_doc()
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
    try(open_db("test.couch", COUCH_CREATE_FILES, NULL, &db));
    try(save_doc(db, &testdocset.docs[0], &testdocset.infos[0], 0));
    try(save_doc(db, &testdocset.docs[1], &testdocset.infos[1], 0));
    try(save_doc(db, &testdocset.docs[2], &testdocset.infos[2], 0));
    try(save_doc(db, &testdocset.docs[3], &testdocset.infos[3], 0));
    try(commit_all(db, 0));
    close_db(db);
    //Read back
    try(open_db("test.couch", 0, NULL, &db));
    try(changes_since(db, 0, 0, docset_check, &testdocset));
    assert(testdocset.counters.totaldocs == 4);
    assert(testdocset.counters.deleted == 0);
    close_db(db);
cleanup:
    assert(errcode == 0);
}

void test_compressed_doc_body()
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
    try(open_db("test.couch", COUCH_CREATE_FILES, NULL, &db));
    try(save_docs(db, docptrs, nfoptrs, 2, COMPRESS_DOC_BODIES));
    try(commit_all(db, 0));
    close_db(db);
    //Read back
    try(open_db("test.couch", 0, NULL, &db));
    try(changes_since(db, 0, 0, docset_check, &testdocset));
    assert(testdocset.counters.totaldocs == 2);
    assert(testdocset.counters.deleted == 0);
    close_db(db);
cleanup:
    assert(errcode == 0);
}

void test_dump_empty_db()
{
    fprintf(stderr, "dump empty db... ");
    fflush(stderr);
    unlink("test.couch");
    Db *db;
    open_db("test.couch", COUCH_CREATE_FILES, NULL, &db);
    close_db(db);
    open_db("test.couch", 0, NULL, &db);
    dump_count(db);
    assert(counters.totaldocs == 0);
    assert(counters.deleted == 0);
    close_db(db);
}

void test_local_docs()
{
    fprintf(stderr, "local docs... ");
    fflush(stderr);
    int errcode = 0;
    Db *db;
    LocalDoc lDocWrite;
    LocalDoc *lDocRead = NULL;
    unlink("test.couch");
    try(open_db("test.couch", COUCH_CREATE_FILES, NULL, &db));
    lDocWrite.id.buf = "_local/testlocal";
    lDocWrite.id.size = 16;
    lDocWrite.json.buf = "{\"test\":true}";
    lDocWrite.json.size = 13;
    lDocWrite.deleted = 0;
    save_local_doc(db, &lDocWrite);
    commit_all(db, 0);
    close_db(db);
    open_db("test.couch", 0, NULL, &db);
    open_local_doc(db, (uint8_t*)"_local/testlocal", 16, &lDocRead);
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
    fprintf(stderr, "opening nonexistent file errors... ");
    fflush(stderr);
    unlink("test.couch");
    Db *db;
    int errcode = open_db("test.couch", 0, NULL, &db);
    assert(errcode == ERROR_OPEN_FILE);
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
