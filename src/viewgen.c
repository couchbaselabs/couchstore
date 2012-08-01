#include "config.h"
#include <libcouchstore/couch_index.h>
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
    CouchStoreIndex* index = NULL;

    const char* indexPath = argv[argc - 1];
    errcode = couchstore_create_index(indexPath, &index);
    if (errcode) {
        fprintf(stderr, "Couldn't open database %s: %s\n", indexPath, couchstore_strerror(errcode));
        goto cleanup;
    }
    
    for (int i = 1; i < argc - 1; ++i) {
        const char* inputPath = argv[i];
        printf("Adding %s to %s ...\n", inputPath, indexPath);
        errcode = couchstore_index_add(inputPath, index);
        if (errcode < 0) {
            fprintf(stderr, "Error adding %s: %s\n", inputPath, couchstore_strerror(errcode));
            goto cleanup;
        }
    }
    printf("Done!");

cleanup:
    if (index) {
        couchstore_close_index(index);
        if (errcode < 0) {
            unlink(indexPath);
        }
    }
    return errcode ? EXIT_FAILURE : EXIT_SUCCESS;
}
