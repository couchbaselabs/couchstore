#include "config.h"
#include <libcouchstore/couch_db.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bitfield.h"

static void exit_error(couchstore_error_t errcode)
{
    fprintf(stderr, "Couchstore error: %s\n", couchstore_strerror(errcode));
    exit(-1);
}

static void usage(const char* prog) {
    fprintf(stderr, "Usage: %s [--purge-before <timestamp>] [--dropdeletes] <input file> <output file>\n", prog);
    exit(-1);
}

typedef struct {
    uint64_t purge_before;
    uint64_t max_purged_seq;
} time_purge_ctx;

typedef struct {
    raw_64 cas;
    raw_32 expiry;
    raw_32 flags;
} CouchbaseRevMeta;

static int time_purge_hook(Db* target, DocInfo* info, void* ctx_p) {
    time_purge_ctx* ctx = (time_purge_ctx*) ctx_p;

    //Compaction finished
    if(info == NULL) {
        target->header.purge_seq = ctx->max_purged_seq;
        return COUCHSTORE_SUCCESS;
    }

    if(info->deleted && info->rev_meta.size >= 16) {
        const CouchbaseRevMeta* meta = (const CouchbaseRevMeta*)info->rev_meta.buf;
        uint32_t exptime = decode_raw32(meta->expiry);
        if(exptime < ctx->purge_before) {
            if(ctx->max_purged_seq < info->db_seq) {
                ctx->max_purged_seq = info->db_seq;
            }
            return COUCHSTORE_COMPACT_DROP_ITEM;
        }
    }

    return COUCHSTORE_COMPACT_KEEP_ITEM;
}

int main(int argc, char** argv)
{
    Db* source;
    couchstore_error_t errcode;
    time_purge_ctx timepurge = {0, 0};
    couchstore_compact_hook hook = NULL;
    void* hook_ctx = NULL;
    if(argc < 3)
    {
        usage(argv[0]);
    }
    int argp = 1;
    couchstore_compact_flags flags = 0;
    const couch_file_ops* target_io_ops = couchstore_get_default_file_ops();

    while(argv[argp][0] == '-') {
        if(!strcmp(argv[argp],"--purge-before")) {
            argp+=2;
            if(argc + argp < 2) {
                usage(argv[0]);
            }
            hook = time_purge_hook;
            hook_ctx = &timepurge;
            timepurge.purge_before = atoi(argv[argp-1]);
            printf("Purging items before timestamp %"PRIu64"\n", timepurge.purge_before);
        }

        if(!strcmp(argv[argp],"--dropdeletes")) {
            argp++;
            if(argc + argp < 2) {
                usage(argv[0]);
            }
            flags = COUCHSTORE_COMPACT_FLAG_DROP_DELETES;
        }
    }

    errcode = couchstore_open_db(argv[argp++], COUCHSTORE_OPEN_FLAG_RDONLY, &source);
    if(errcode)
    {
        exit_error(errcode);
    }
    errcode = couchstore_compact_db_ex(source, argv[argp], flags, hook, hook_ctx, target_io_ops);
    if(errcode)
    {
        exit_error(errcode);
    }

    printf("Compacted %s -> %s\n", argv[argp - 1], argv[argp]);
    return 0;
}

