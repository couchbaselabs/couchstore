#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "couch_db.h"
#include "util.h"

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
    printf("    bin: "); printsb(&doc->binary);
    printf("   json: "); printsb(&doc->json);
    free_doc(doc);
    (*count)++;
}

int main(int argc, char **argv)
{
    Db *db = NULL;
    int errcode;
    int count = 0;

    if(argv < 1)
    {
        printf("USAGE: %s <file.couch>\n", argv[0], argv[1]);
        return -1;
    }

    try(open_db(argv[1], 0, &db));
    try(changes_since(db, 0, 0, foldprint, &count));
    printf("\nTotal docs: %d\n", count);
cleanup:
    if(db)
        close_db(db);
    if(errcode < 0)
        printf("ERROR: %s\n", describe_error(errcode));
    return errcode;
}

