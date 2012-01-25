#ifndef COUCHSTORE_UTIL_H
#define COUCHSTORE_UTIL_H

#include <string.h>
#include "couch_common.h"
#include "ei.h"
#define error_unless(C, E) if(!(C)) { errcode = E; goto cleanup; }
#define error_nonzero(C, E) if((C) != 0) { errcode = E; goto cleanup; }
#define try(C) if((errcode = (C)) < 0) { goto cleanup; }

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

node_pointer* read_root(char* buf, int* endpos);
void ei_x_encode_nodepointer(ei_x_buff* x, node_pointer* node);
void term_to_buf(sized_buf *dst, char* buf, int *pos);
#endif
