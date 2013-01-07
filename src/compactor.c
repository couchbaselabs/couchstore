#include "config.h"
#include <libcouchstore/couch_db.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void exit_error(couchstore_error_t errcode)
{
    fprintf(stderr, "Couchstore error: %s\n", couchstore_strerror(errcode));
    exit(-1);
}

static void usage(const char* prog) {
    fprintf(stderr, "Usage: %s [--dropdeletes] <input file> <output file>\n", prog);
    exit(-1);
}

int main(int argc, char** argv)
{
    Db* source;
    couchstore_error_t errcode;
    if(argc < 3)
    {
        usage(argv[0]);
    }
    int argp = 1;
    couchstore_compact_flags flags = 0;
    const couch_file_ops* target_io_ops = couchstore_get_default_file_ops();

    if(!strcmp(argv[argp],"--dropdeletes")) {
        argp++;
        if(argc < 4) {
            usage(argv[0]);
        }
        flags = COUCHSTORE_COMPACT_FLAG_DROP_DELETES;
    }

    errcode = couchstore_open_db(argv[argp++], COUCHSTORE_OPEN_FLAG_RDONLY, &source);
    if(errcode)
    {
        exit_error(errcode);
    }
    errcode = couchstore_compact_db_ex(source, argv[argp], flags, target_io_ops);
    if(errcode)
    {
        exit_error(errcode);
    }

    printf("Compacted %s -> %s\n", argv[argp - 1], argv[argp]);
    return 0;
}

