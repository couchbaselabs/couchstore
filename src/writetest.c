#include <stdio.h>
#include "couch_db.h"
#include "util.h"

#define setsb(B, V) (B).buf = V; (B).size = strlen(V);

int main(int argc, char** argv)
{
    int errcode = 0;
    Db* db;
    DocInfo di;
    Doc doc;

    if(argc < 3)
    {
        printf("USE %s <key> <bin>\n", argv[0]);
        return -1;
    }

    setsb(doc.id, argv[1]);
    setsb(doc.json, "{\"test doc\":[1,2,3,4]}");
    setsb(doc.binary, argv[2]);
    //doc.binary.buf = NULL;
    //doc.binary.size = 0;

    di.id = doc.id;
    setsb(di.meta, "\000\000\000\000");
    di.rev = 0;
    di.size = doc.json.size + doc.binary.size;
    di.deleted = 0;

    try(open_db("testfile.couch", 0, &db));
    try(save_doc(db, &doc, &di, 0));
    commit_all(db, 0);

cleanup:
    if(errcode < 0)
        fprintf(stderr, "error: %s\n", describe_error(errcode));
    if(db)
        close_db(db);
    return errcode;
}

