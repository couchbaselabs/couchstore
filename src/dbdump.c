#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <libcouchstore/couch_db.h>

#ifndef DEBUG
#define try(C) if((errcode = (C)) < 0) { goto cleanup; }
#else
#define try(C) if((errcode = (C)) < 0) { \
                            fprintf(stderr, "Couchstore error `%s' at %s:%d\r\n", \
                            describe_error(errcode), __FILE__, __LINE__); goto cleanup; }
#endif
#define error_unless(C, E) if(!(C)) { try(E); }

void printsb(sized_buf *sb)
{
    if(sb->buf == NULL)
    {
        printf("null\n");
        return;
    }
    printf("%.*s\n", (int) sb->size, sb->buf);
}

int foldprint(Db* db, DocInfo* docinfo, void *ctx)
{
    int *count = ctx;
    Doc* doc;
    open_doc_with_docinfo(db, docinfo, &doc, 0);
    printf("Doc seq: %llu\n", docinfo->seq);
    printf("     id: "); printsb(&docinfo->id);
    if(docinfo->deleted)
        printf("     doc deleted\n");
    if(doc)
    {
        printf("    bin: "); printsb(&doc->binary);
        printf("   json: "); printsb(&doc->json);
    }
    else
        printf("no doc body\n");
    free_doc(doc);
    (*count)++;
    return 0;
}

int main(int argc, char **argv)
{
    Db *db = NULL;
    int errcode;
    int count = 0;

    int argpos = 1;
    if(argc < 2)
    {
        printf("USAGE: %s <file.couch>\n", argv[0]);
        return -1;
    }
again:
    try(open_db(argv[argpos], 0, &db));
    try(changes_since(db, 0, 0, foldprint, &count));
cleanup:
    if(db)
        close_db(db);
    argpos++;
    if(argpos < argc) goto again;
    printf("\nTotal docs: %d\n", count);
    if(errcode < 0)
        printf("ERROR: %s\n", describe_error(errcode));
    return errcode;
}

