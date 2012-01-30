#ifndef COUCHSTORE_UTIL_H
#define COUCHSTORE_UTIL_H

#include <string.h>
#include <signal.h>
#include "couch_common.h"
#include "ei.h"
#ifndef DEBUG
#define try(C) if((errcode = (C)) < 0) { goto cleanup; }
#else
#define try(C) if((errcode = (C)) < 0) { \
                            fprintf(stderr, "Couchstore error `%s' at %s:%d\r\n", \
                            describe_error(errcode), __FILE__, __LINE__); raise(SIGINT); goto cleanup; }
#endif
#define error_unless(C, E) if(!(C)) { try(E); }
#define error_nonzero(C, E) if((C) != 0) { try(E); }

#define atom_check(B, A) do_atom_check(B, A, sizeof(A) - 1)
static inline int do_atom_check(char* buf, char *atomname, int len)
{
    //quick atom check for < 255 in len
    if(buf[0] != 100)
        return 0;
    if(buf[1] != 0)
        return 0;
    if(buf[2] != len)
        return 0;
    return !strncmp(atomname, buf+3, len);
}

static inline int tuple_check(char* buf, int *index, int tuplelen)
{
    int checklen = 0;
    if(ei_decode_tuple_header(buf, index, &checklen) < 0)
    {
        return 0;
    }
    else
    {
        if(checklen != tuplelen)
            return 0;
    }
    return 1;
}

typedef struct _fat_buffer {
    size_t pos;
    size_t size;
    char buf[1];
} fatbuf;

fatbuf* fatbuf_alloc(size_t bytes);
void* fatbuf_get(fatbuf* fb, size_t bytes);
void fatbuf_free(fatbuf* fb);

node_pointer* read_root(char* buf, int* endpos);
void ei_x_encode_nodepointer(ei_x_buff* x, node_pointer* node);
void term_to_buf(sized_buf *dst, char* buf, int *pos);
#endif
