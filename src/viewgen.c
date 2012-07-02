#include "config.h"
#include <libcouchstore/couch_db.h>
#include <stdio.h>
#include <stdlib.h>


static int generateView(const char *inputPath, const char*outputPath) {
    printf("Adding %s to %s\n", inputPath, outputPath);
    return 0;
}


int main(int argc, char** argv)
{
    if(argc < 3)
    {
        fprintf(stderr, "Usage: viewgen <input file> [<input file> ...] <output file>\n");
        return 1;
    }

    const char* outPath = argv[argc - 1];
    int result = 0;
    
    for (int i = 1; i < argc - 1; ++i) {
        result = generateView(argv[i], outPath);
        if (result)
            break;
    }

    return result;
}
