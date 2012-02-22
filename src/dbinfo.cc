#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <libcouchstore/couch_db.h>
#include <snappy-c.h>
#include "util.h"
#include "endian.h"
#include "ei.h"

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
char rfs[256];
char* size_str(double size) {
    int i = 0;
    const char* units[] = {"bytes", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"};
    while (size > 1024) {
        size /= 1024;
        i++;
    }
    sprintf(rfs, "%.*f %s", i, size, units[i]);
    return rfs;
}

uint64_t id_reduce_info(node_pointer* root)
{
    int pos = 0;
    uint64_t total, deleted, size;
    if(root == NULL)
    {
      printf("   no documents\n");
      return 0;
    }
    ei_decode_tuple_header(root->reduce_value.buf, &pos, NULL);
    ei_decode_uint64(root->reduce_value.buf, &pos, &total);
    ei_decode_uint64(root->reduce_value.buf, &pos, &deleted);
    ei_decode_uint64(root->reduce_value.buf, &pos, &size);
    printf("   doc count: %"PRIu64"\n", total);
    printf("   deleted doc count: %"PRIu64"\n", deleted);
    printf("   data size: %s\n", size_str(size));
    return size;
}

int main(int argc, char **argv)
{
    Db *db = NULL;
    int errcode;
    uint64_t datasize, btreesize;

    int argpos = 1;
    if(argc < 2)
    {
        printf("USAGE: %s <file.couch>\n", argv[0]);
        return -1;
    }

again:
    datasize = 0;
    btreesize = 0;
    error_pass(open_db(argv[argpos], 0, &db));
    printf("DB Info (%s)\n", argv[argpos]);
    printf("   file format version: %"PRIu64"\n", db->header.disk_version);
    printf("   update_seq: %"PRIu64"\n", db->header.update_seq);
    datasize = id_reduce_info(db->header.by_id_root);
    if(db->header.by_id_root)
      btreesize += db->header.by_id_root->subtreesize;
    if(db->header.by_seq_root)
      btreesize += db->header.by_seq_root->subtreesize;
    printf("   B-tree size: %s\n", size_str(btreesize));
    printf("   total disk size: %s\n", size_str(db->file_pos));
cleanup:
    if(errcode == 0 && db)
        close_db(db);
    argpos++;
    if(errcode == 0 && argpos < argc) goto again;
    if(errcode < 0)
        printf("ERROR: %s\n", describe_error(errcode));
    return errcode;
}

