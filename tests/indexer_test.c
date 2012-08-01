/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <libcouchstore/couch_index.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include "macros.h"

#define check_read(C)                                                      \
    do {                                                            \
        if((C) == 0) {                                   \
            fprintf(stderr, "fread failed at %s:%d\r\n",   \
            __FILE__, __LINE__);      \
            abort();                                           \
        }                                                           \
    } while (0)



#define KVPATH "/tmp/test.couchkv"
#define INDEXPATH "/tmp/test.couchindex"

void TestCouchIndexer(void);


static void GenerateKVFile(const char* path, unsigned numKeys) {
    FILE* out = fopen(path, "wb");
    for (unsigned i = 0; i < numKeys; ++i) {
        // See "Primary Key Index Values" in view_format.md for the data format.
        int k1 = random() % 100, k2 = random() % 100;
        char docid[100], key[100], value[100];
        sprintf(docid, "doc%u", i);
        sprintf(key, "[%d,%d]", k1, k2);
        sprintf(value, "The value for [%d, %d]", k1, k2);

        // Write key and value lengths:
        uint16_t klen = htons(2 + strlen(key) + strlen(docid));
        uint32_t vlen = htonl(2 + 3 + strlen(value));
        fwrite(&klen, sizeof(klen), 1, out);
        fwrite(&vlen, sizeof(vlen), 1, out);

        // Write key:
        klen = htons(strlen(key));
        fwrite(&klen, sizeof(klen), 1, out);
        fwrite(key, strlen(key), 1, out);
        fwrite(docid, strlen(docid), 1, out);

        // Write value:
        uint16_t partitionID = htons((uint16_t)random() % 1024);
        fwrite(&partitionID, sizeof(partitionID), 1, out);
        uint32_t valueLength = htonl(strlen(value));
        fwrite((char*)&valueLength + 1, 3, 1, out);     // write it as 24-bit
        fwrite(value, strlen(value), 1, out);
    }
    fclose(out);
}


static void IndexKVFile(const char* kvPath, const char* indexPath) {
    couchstore_error_t errcode;
    CouchStoreIndex* index = NULL;

    try(couchstore_create_index(indexPath, &index));
    try(couchstore_index_add(kvPath, index));
    try(couchstore_close_index(index));

cleanup:
    assert(errcode == 0);
}


static void ReadIndexFile(const char *indexPath) {
    // See write_index_header in couch_index.c
    FILE *file = fopen(indexPath, "rb");
    fseek(file, 0, SEEK_END);
    off_t eof = ftell(file);
    off_t headerPos = eof - (eof % 4096);    // go to last 4KB boundary
    printf("Index header is at 0x%llx\n", headerPos);
    fseek(file, headerPos, SEEK_SET);

    // Header starts with a "1" byte, a length, and a CRC checksum:
    uint8_t flag;
    check_read(fread(&flag, 1, 1, file));
    assert_eq(flag, 1);
    uint32_t headerLength, headerChecksum;
    check_read(fread(&headerLength, sizeof(headerLength), 1, file));
    headerLength = ntohl(headerLength);
    check_read(fread(&headerChecksum, sizeof(headerChecksum), 1, file));
    headerChecksum = htonl(headerChecksum);
    assert_eq(headerPos + headerLength + 1 + 4, eof);

    // Next is the root count:
    uint32_t numIndexes;
    check_read(fread(&numIndexes, sizeof(numIndexes), 1, file));
    numIndexes = ntohl(numIndexes);
    assert_eq(numIndexes, 1);

    // The root starts with its size, then the node pointer, subtree size, and reduce data:
    uint16_t rootSize;
    check_read(fread(&rootSize, sizeof(rootSize), 1, file));
    rootSize = ntohs(rootSize);
    assert(rootSize >= 12);
    assert(rootSize < headerLength);
    uint64_t pointer = 0;
    uint64_t subtreesize = 0;
    check_read(fread((char*)&pointer + 2, 6, 1, file));
    check_read(fread((char*)&subtreesize + 2, 6, 1, file));
    pointer = ntohll(pointer);
    subtreesize = ntohll(subtreesize);
    sized_buf reduce = {NULL, rootSize - 12};
    reduce.buf = malloc(reduce.size);
    check_read(fread(reduce.buf, reduce.size, 1, file));
    assert_eq(ftell(file), eof);

    printf("Root: pointer=%llu, subtreesize=%llu, reduce=%llu bytes\n",
           pointer, subtreesize, (uint64_t)reduce.size);

    assert(pointer < (uint64_t)headerPos);

    fclose(file);
}


void TestCouchIndexer(void) {
    srandom(42);  // get a consistent sequence of random numbers
    fprintf(stderr, "Indexer: ");
    GenerateKVFile(KVPATH, 1000);
    IndexKVFile(KVPATH, INDEXPATH);
    ReadIndexFile(INDEXPATH);
    unlink(KVPATH);
    unlink(INDEXPATH);
    fprintf(stderr, "OK\n");
}
