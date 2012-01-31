#include <stdio.h>
#include <string.h>
#include <libcouchstore/couch_db.h>
#include "fatbuf.h"

#define setsb(B, V) (B).buf = V; (B).size = strlen(V);

#ifndef DEBUG
#define try(C) if((errcode = (C)) < 0) { goto cleanup; }
#else
#define try(C) if((errcode = (C)) < 0) { \
                            fprintf(stderr, "Couchstore error `%s' at %s:%d\r\n", \
                            describe_error(errcode), __FILE__, __LINE__); goto cleanup; }
#endif
#define error_unless(C, E) if(!(C)) { try(E); }

int main(int argc, char** argv)
{
    int errcode = 0;
    Db* db;

    if(argc < 3)
    {
        printf("USE %s <key> <bin> [<key> <bin>]+\n", argv[0]);
        return -1;
    }

    int numdocs = (argc - 1) / 2;
    int i;
    fatbuf* fb = fatbuf_alloc(numdocs * (sizeof(Doc) + sizeof(DocInfo)));
    Doc* docs = fatbuf_get(fb, numdocs * sizeof(Doc));
    DocInfo* infos = fatbuf_get(fb, numdocs * sizeof(DocInfo));

    for(i = 0; i < numdocs; i++)
    {
        setsb(docs[i].id, argv[i*2 + 1]);
        setsb(docs[i].json, "{\"test doc\":[1,2,3,4]}");
        setsb(docs[i].binary, argv[i*2 + 2]);

        infos[i].id = docs[i].id;
        setsb(infos[i].meta, "\000\000\000\000");
        infos[i].meta.size = 4;
        infos[i].rev = 0;
        infos[i].size = docs[i].json.size + docs[i].binary.size;
        if(strcmp(argv[i * 2 + 2], "deleted") == 0)
            infos[i].deleted = 1;
        else
            infos[i].deleted = 0;
    }

    try(open_db("testfile.couch", 0, &db));
    try(save_docs(db, docs, infos, numdocs, 0));
    commit_all(db, 0);

cleanup:
    if(errcode < 0)
        fprintf(stderr, "error: %s\n", describe_error(errcode));
    if(db)
        close_db(db);
    return errcode;
}

