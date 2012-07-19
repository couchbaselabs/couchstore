/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <libcouchstore/couch_db.h>
#include <snappy-c.h>
#include "bitfield.h"

static void printsb(sized_buf *sb)
{
    if (sb->buf == NULL) {
        printf("null\n");
        return;
    }
    printf("%.*s\n", (int) sb->size, sb->buf);
}

static void printsbhex(sized_buf *sb)
{
    if (sb->buf == NULL) {
        printf("null\n");
        return;
    }
    for (size_t i = 0; i < sb->size; ++i) {
        printf("%02x", sb->buf[i]);
        if (i % 4 == 3) {
            printf(" ");
        }
    }
    printf("\n");
}

static int foldprint(Db *db, DocInfo *docinfo, void *ctx)
{
    int *count = (int *) ctx;
    (*count)++;
    Doc *doc = NULL;
    uint64_t cas;
    uint32_t expiry, flags;
    printf("Doc seq: %"PRIu64"\n", docinfo->db_seq);
    printf("     id: ");
    printsb(&docinfo->id);
    if (docinfo->bp == 0) {
        printf("         ** This b-tree node is corrupt; raw node value follows:*\n");
        printf("    raw: ");
        printsbhex(&docinfo->rev_meta);
        return 0;
    }
    printf("     rev: %"PRIu64"\n", docinfo->rev_seq);
    printf("     content_meta: %d\n", docinfo->content_meta);
    if (docinfo->rev_meta.size == 16) {
        cas = ntohll(*((uint64_t *)docinfo->rev_meta.buf));
        expiry = get_32(docinfo->rev_meta.buf + 8);
        flags = get_32(docinfo->rev_meta.buf + 12);
        printf("     cas: %"PRIu64", expiry: %"PRIu32", flags: %"PRIu32"\n", cas, expiry, flags);
    }
    if (docinfo->deleted) {
        printf("     doc deleted\n");
    }

    couchstore_error_t docerr = couchstore_open_doc_with_docinfo(db, docinfo, &doc, 0);
    if(docerr != COUCHSTORE_SUCCESS) {
        printf("     error reading document body: %s\n", couchstore_strerror(docerr));
    } else if (doc && (docinfo->content_meta & COUCH_DOC_IS_COMPRESSED)) {
        size_t rlen;
        snappy_uncompressed_length(doc->data.buf, doc->data.size, &rlen);
        char *decbuf = (char *) malloc(rlen);
        size_t uncompr_len;
        snappy_uncompress(doc->data.buf, doc->data.size, decbuf, &uncompr_len);
        printf("     data: (snappy) %.*s\n", (int) uncompr_len, decbuf);
    } else if(doc) {
        printf("     data: ");
        printsb(&doc->data);
    }

    couchstore_free_document(doc);
    return 0;
}

static int process_file(const char *file, int *total)
{
    Db *db;
    couchstore_error_t errcode;
    errcode = couchstore_open_db(file, COUCHSTORE_OPEN_FLAG_RDONLY, &db);
    if (errcode != COUCHSTORE_SUCCESS) {
        fprintf(stderr, "Failed to open \"%s\": %s\n",
                file, couchstore_strerror(errcode));
        return -1;
    }

    int count = 0;
    errcode = couchstore_changes_since(db, 0, 0, foldprint, &count);
    (void)couchstore_close_db(db);

    if (errcode < 0) {
        fprintf(stderr, "Failed to dump database \"%s\": %s\n",
                file, couchstore_strerror(errcode));
        return -1;
    }

    *total += count;
    return 0;
}

int main(int argc, char **argv)
{
    if (argc < 2) {
        printf("USAGE: %s <file.couch>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int error = 0;
    int count = 0;
    for (int ii = 1; ii < argc; ++ii) {
        error += process_file(argv[ii], &count);
    }

    printf("\nTotal docs: %d\n", count);
    if (error) {
        exit(EXIT_FAILURE);
    } else {
        exit(EXIT_SUCCESS);
    }
}

