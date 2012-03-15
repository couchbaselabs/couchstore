#ifndef COUCHSTORE_UTIL_H
#define COUCHSTORE_UTIL_H

#include <string.h>
#include <signal.h>
#include <libcouchstore/couch_db.h>

#include "ei.h"
#include "fatbuf.h"

#ifndef DEBUG
#define error_pass(C) if((errcode = (C)) < 0) { goto cleanup; }
#else
#define error_pass(C) if((errcode = (C)) < 0) { \
                            fprintf(stderr, "Couchstore error `%s' at %s:%d\r\n", \
                            describe_error(errcode), __FILE__, __LINE__); goto cleanup; }
#endif
#define error_unless(C, E) if(!(C)) { error_pass(E); }
#define error_nonzero(C, E) if((C) != 0) { error_pass(E); }

#define atom_check(B, A) do_atom_check(B, A, sizeof(A) - 1)

static inline int do_atom_check(char *buf, const char *atomname, int len)
{
    //quick atom check for < 255 in len
    if (buf[0] != 100) {
        return 0;
    }
    if (buf[1] != 0) {
        return 0;
    }
    if (buf[2] != len) {
        return 0;
    }
    return !strncmp(atomname, buf + 3, len);
}

static inline int tuple_check(char *buf, int *index, int tuplelen)
{
    int checklen = 0;
    if (ei_decode_tuple_header(buf, index, &checklen) < 0) {
        return 0;
    } else {
        if (checklen != tuplelen) {
            return 0;
        }
    }
    return 1;
}

node_pointer *read_root(char *buf, int *endpos);
void ei_x_encode_nodepointer(ei_x_buff *x, node_pointer *node);
void term_to_buf(sized_buf *dst, char *buf, int *pos);

extern "C" {
    int ei_decode_uint64(char *buf, int *index, uint64_t *val);
    uint32_t hash_crc32(const char *key, size_t key_length);
}
#endif
