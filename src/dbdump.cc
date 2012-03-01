#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <libcouchstore/couch_db.h>
#include <snappy-c.h>
#include "endian.h"

#ifndef DEBUG
#define error_pass(C) if((errcode = (C)) < 0) { goto cleanup; }
#else
#define error_pass(C) if((errcode = (C)) < 0) { \
                            fprintf(stderr, "Couchstore error `%s' at %s:%d\r\n", \
                            describe_error(errcode), __FILE__, __LINE__); goto cleanup; }
#endif
#define error_unless(C, E) if(!(C)) { error_pass(E); }
#define SNAPPY_FLAG 128


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
    int *count = (int*) ctx;
    Doc* doc;
    uint64_t cas;
    uint32_t expiry, flags;
    cas = endianSwap(*((uint64_t*) docinfo->rev_meta.buf));
    expiry = endianSwap(*((uint32_t*) (docinfo->rev_meta.buf + 8)));
    flags = endianSwap(*((uint32_t*) (docinfo->rev_meta.buf + 12)));
    open_doc_with_docinfo(db, docinfo, &doc, 0);
    printf("Doc seq: %"PRIu64"\n", docinfo->db_seq);
    printf("     id: "); printsb(&docinfo->id);
    printf("     content_meta: %d\n", docinfo->content_meta);
    printf("     cas: %"PRIu64", expiry: %"PRIu64", flags: %"PRIu64"\n", (long long unsigned int) cas, (long long unsigned int) expiry, (long long unsigned int) flags);
    if(docinfo->deleted)
        printf("     doc deleted\n");

    if(docinfo->content_meta & SNAPPY_FLAG)
    {
        size_t rlen;
        snappy_uncompressed_length(doc->data.buf, doc->data.size, &rlen);
        char *decbuf = (char*) malloc(rlen);
        size_t uncompr_len;
        snappy_uncompress(doc->data.buf, doc->data.size, decbuf, &uncompr_len);
        printf("     data: (snappy) %.*s\n", (int) uncompr_len, decbuf);
    }
    else
    {
        printf("     data: "); printsb(&doc->data);
    }

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
    error_pass(open_db(argv[argpos], 0, NULL, &db));
    error_pass(changes_since(db, 0, 0, foldprint, &count));
cleanup:
    if(errcode == 0 && db)
        close_db(db);
    argpos++;
    if(errcode == 0 && argpos < argc) goto again;
    printf("\nTotal docs: %d\n", count);
    if(errcode < 0)
        printf("ERROR: %s\n", describe_error(errcode));
    return errcode;
}

