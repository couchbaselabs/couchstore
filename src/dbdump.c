/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <string.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <libcouchstore/couch_db.h>
#include <snappy-c.h>
#include "bitfield.h"
#include "internal.h"

typedef enum {
    DumpBySequence,
    DumpByID,
} DumpMode;

static DumpMode mode = DumpBySequence;
static bool dumpTree = false;

typedef struct {
    raw_64 cas;
    raw_32 expiry;
    raw_32 flags;
} CouchbaseRevMeta;

static void printsb(const sized_buf *sb)
{
    if (sb->buf == NULL) {
        printf("null\n");
        return;
    }
    printf("%.*s\n", (int) sb->size, sb->buf);
}

static void printsbhex(const sized_buf *sb, int with_ascii)
{
    if (sb->buf == NULL) {
        printf("null\n");
        return;
    }
    printf("{");
    for (size_t i = 0; i < sb->size; ++i) {
        printf("%.02x", (uint8_t)sb->buf[i]);
        if (i % 4 == 3) {
            printf(" ");
        }
    }
    printf("}");
    if (with_ascii) {
        printf("  (\"");
        for (size_t i = 0; i < sb->size; ++i) {
            uint8_t ch = sb->buf[i];
            if (ch < 32 || ch >= 127) {
                ch = '?';
            }
            printf("%c", ch);
        }
        printf("\")");
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
    if (mode == DumpBySequence) {
        printf("Doc seq: %"PRIu64"\n", docinfo->db_seq);
        printf("     id: ");
        printsb(&docinfo->id);
    } else {
        printf(" Doc ID: ");
        printsb(&docinfo->id);
        if (docinfo->db_seq > 0) {
            printf("    seq: %"PRIu64"\n", docinfo->db_seq);
        }
    }
    if (docinfo->bp == 0 && docinfo->deleted == 0) {
        printf("         ** This b-tree node is corrupt; raw node value follows:*\n");
        printf("    raw: ");
        printsbhex(&docinfo->rev_meta, 1);
        return 0;
    }
    printf("     rev: %"PRIu64"\n", docinfo->rev_seq);
    printf("     content_meta: %d\n", docinfo->content_meta);
    if (docinfo->rev_meta.size == sizeof(CouchbaseRevMeta)) {
        const CouchbaseRevMeta* meta = (const CouchbaseRevMeta*)docinfo->rev_meta.buf;
        cas = decode_raw64(meta->cas);
        expiry = decode_raw32(meta->expiry);
        flags = decode_raw32(meta->flags);
        printf("     cas: %"PRIu64", expiry: %"PRIu32", flags: %"PRIu32"\n", cas, expiry, flags);
    }
    if (docinfo->deleted) {
        printf("     doc deleted\n");
    }

    couchstore_error_t docerr = couchstore_open_doc_with_docinfo(db, docinfo, &doc, 0);
    if(docerr != COUCHSTORE_SUCCESS) {
        printf("     could not read document body: %s\n", couchstore_strerror(docerr));
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


static int visit_node(Db *db,
                      int depth,
                      const DocInfo* docinfo,
                      uint64_t subtreeSize,
                      const sized_buf* reduceValue,
                      void *ctx)
{
    (void) db;
    for (int i = 0; i < depth; ++i)
        printf("  ");
    if (reduceValue) {
        // This is a tree node:
        printf("+ (%llu) ", subtreeSize);
        printsbhex(reduceValue, 0);
    } else if (docinfo->bp > 0) {
        // This is a document:
        printf("%c (%llu) ", (docinfo->deleted ? 'x' : '*'), (uint64_t)docinfo->size);
        if (mode == DumpBySequence) {
            printf("#%lld ", docinfo->db_seq);
        }
        printsb(&docinfo->id);

        int *count = (int *) ctx;
        (*count)++;
    } else {
        // Document, but not in a known format:
        printf("**corrupt?** ");
        printsbhex(&docinfo->rev_meta, 1);
    }
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

    switch (mode) {
        case DumpBySequence:
            if (dumpTree) {
                errcode = couchstore_walk_seq_tree(db, 0, COUCHSTORE_INCLUDE_CORRUPT_DOCS,
                                                   visit_node, &count);
            } else {
                errcode = couchstore_changes_since(db, 0, COUCHSTORE_INCLUDE_CORRUPT_DOCS,
                                                   foldprint, &count);
            }
            break;
        case DumpByID:
            if (dumpTree) {
                errcode = couchstore_walk_id_tree(db, NULL, COUCHSTORE_INCLUDE_CORRUPT_DOCS,
                                                  visit_node, &count);
            } else {
                errcode = couchstore_all_docs(db, NULL, COUCHSTORE_INCLUDE_CORRUPT_DOCS,
                                              foldprint, &count);
            }
            break;
    }
    (void)couchstore_close_db(db);

    if (errcode < 0) {
        fprintf(stderr, "Failed to dump database \"%s\": %s\n",
                file, couchstore_strerror(errcode));
        return -1;
    }

    *total += count;
    return 0;
}

static void usage(void) {
    printf("USAGE: couch_dbdump [--byid | --byseq] [--tree] <file.couch>\n");
    exit(EXIT_FAILURE);
}

int main(int argc, char **argv)
{
    if (argc < 2) {
        usage();
    }

    int ii = 1;
    while (strncmp(argv[ii], "-", 1) == 0) {
        if (strcmp(argv[ii], "--byid") == 0) {
            mode = DumpByID;
        } else if (strcmp(argv[ii], "--byseq") == 0) {
            mode = DumpBySequence;
        } else if (strcmp(argv[ii], "--tree") == 0) {
            dumpTree = true;
        } else {
            usage();
        }
        ++ii;
    }

    if (ii >= argc) {
        usage();
    }

    int error = 0;
    int count = 0;
    for (; ii < argc; ++ii) {
        error += process_file(argv[ii], &count);
    }

    printf("\nTotal docs: %d\n", count);
    if (error) {
        exit(EXIT_FAILURE);
    } else {
        exit(EXIT_SUCCESS);
    }
}

