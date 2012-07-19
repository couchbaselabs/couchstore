#include "config.h"
#include <libcouchstore/couch_db.h>
#include <stdio.h>
#include <stdlib.h>


int main(int argc, char** argv)
{
    if(argc < 3)
    {
        fprintf(stderr, "Usage: %s <input file> [<input file> ...] <output file>\n",
                argv[0]);
        return EXIT_FAILURE;
    }

    couchstore_error_t errcode;
    Db* outputDb = NULL;

    const char* dbPath = argv[argc - 1];
    errcode = couchstore_open_db(dbPath, COUCHSTORE_OPEN_FLAG_CREATE, &outputDb);
    if (errcode) {
        fprintf(stderr, "Couldn't open database %s: %s\n", dbPath, couchstore_strerror(errcode));
        goto cleanup;
    }
    
    for (int i = 1; i < argc - 1; ++i) {
        const char* inputPath = argv[i];
        printf("Adding %s to %s ...\n", inputPath, couchstore_get_db_filename(outputDb));
        errcode = couchstore_index_view(inputPath, outputDb);
        if (errcode < 0) {
            fprintf(stderr, "Error adding %s: %s\n", inputPath, couchstore_strerror(errcode));
            goto cleanup;
        }
    }
    printf("Done!");

cleanup:
    if (outputDb) {
        couchstore_close_db(outputDb);
    }
    return errcode ? EXIT_FAILURE : EXIT_SUCCESS;
}
