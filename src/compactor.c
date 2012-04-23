#include "config.h"
#include <libcouchstore/couch_db.h>
#include <stdio.h>
#include <stdlib.h>

static void exit_error(couchstore_error_t errcode)
{
    fprintf(stderr, "Couchstore error: %s\n", couchstore_strerror(errcode));
    exit(-1);
}

int main(int argc, char** argv)
{
    Db* source;
    couchstore_error_t errcode;
    if(argc < 3)
    {
        fprintf(stderr, "Usage: %s <input file> <output file>\n", argv[0]);
    }

    errcode = couchstore_open_db(argv[1], COUCHSTORE_OPEN_FLAG_RDONLY, &source);
    if(errcode)
    {
        exit_error(errcode);
    }
    errcode = couchstore_compact_db(source, argv[2]);
    if(errcode)
    {
        exit_error(errcode);
    }

    printf("Compacted %s -> %s\n", argv[1], argv[2]);
    return 0;
}

