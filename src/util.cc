#include <stdlib.h>
#include <libcouchstore/couch_db.h>
#include "util.h"

#define ERR_MIN -9

const char* errordescs[9] =
{
      "error opening file"        // ERROR_OPEN_FILE
    , "error reading erlang term" // ERROR_PARSE_TERM
    , "failed to allocate buffer" // ERROR_ALLOC_FAIL
    , "error reading file"        // ERROR_READ
    , "document not found"        // DOC_NOT_FOUND
    , "no header in non-empty file" // ERROR_NO_HEADER
    , "error writing to file" // ERROR_WRITE
    , "incorrect version in header" // ERROR_HEADER_VERSION
    , "checksum fail" // ERROR_CHECKSUM_FAIL
};

const char* describe_error(int errcode)
{
    if(errcode < 0 && errcode >= ERR_MIN)
        return errordescs[-1-errcode];
    else
        return NULL;
}

void term_to_buf(sized_buf *dst, char* buf, int *pos)
{
    int start = *pos;
    ei_skip_term(buf, pos);
    dst->buf = buf + start;
    dst->size = *pos - start;
}

node_pointer* read_root(char* buf, int* endpos)
{
    //Parse {ptr, reduce_value, subtreesize} into a node_pointer with no key.
    node_pointer* ptr;
    int size, type;
    int pos = *endpos;
    ei_get_type(buf, &pos, &type, &size);
    ei_skip_term(buf, endpos);
    if(type == ERL_ATOM_EXT)
        return NULL;
    size = *endpos - pos;
    //Copy the erlang term into the buffer.
    ptr = (node_pointer*) malloc(sizeof(node_pointer) + size);
    buf = (char*) memcpy(((char*)ptr) + sizeof(node_pointer), buf + pos, size);
    pos = 0;
    ptr->key.buf = NULL;
    ptr->key.size = 0;
    ptr->pointer = 0;
    ptr->subtreesize = 0;
    ei_decode_tuple_header(buf, &pos, NULL); //arity 3
    ei_decode_ulonglong(buf, &pos, (unsigned long long*) &ptr->pointer);
    term_to_buf(&ptr->reduce_value, buf, &pos);
    ei_decode_ulonglong(buf, &pos, (unsigned long long*) &ptr->subtreesize);
    return ptr;
}

void ei_x_encode_nodepointer(ei_x_buff* x, node_pointer* node)
{
    if(node == NULL)
    {
        ei_x_encode_atom(x, "nil");
    }
    else
    {
        ei_x_encode_tuple_header(x, 3);
        ei_x_encode_ulonglong(x, node->pointer);
        ei_x_append_buf(x, node->reduce_value.buf, node->reduce_value.size);
        ei_x_encode_ulonglong(x, node->subtreesize);
    }
}


fatbuf* fatbuf_alloc(size_t bytes)
{
    fatbuf* fb = (fatbuf*) malloc(sizeof(fatbuf) + bytes);
#ifdef DEBUG
    memset(fb->buf, 0x44, bytes);
#endif
    if(!fb)
        return NULL;

    fb->size = bytes;
    fb->pos = 0;
    return fb;
}

void* fatbuf_get(fatbuf* fb, size_t bytes)
{
    if(fb->pos + bytes > fb->size)
    {
        return NULL;
    }
#ifdef DEBUG
    if(fb->buf[fb->pos] != 0x44)
    {
        fprintf(stderr, "Fatbuf space has been written to before it was taken!\n");
    }
#endif
    void* rptr = fb->buf + fb->pos;
    fb->pos += bytes;
    return rptr;
}

void fatbuf_free(fatbuf* fb)
{
    free(fb);
}
