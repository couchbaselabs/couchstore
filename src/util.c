/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <stdlib.h>
#include <libcouchstore/couch_db.h>
#include "util.h"
#include "bitfield.h"

node_pointer *read_root(char *buf, int size)
{
    node_pointer *ptr;
    uint64_t position = get_48(buf);
    uint64_t subtreesize = get_48(buf + 6);
    int redsize = size - 12;
    ptr = (node_pointer *) malloc(sizeof(node_pointer) + redsize);
    buf = (char *) memcpy(((char *)ptr) + sizeof(node_pointer), buf + 12, redsize);
    ptr->key.buf = NULL;
    ptr->key.size = 0;
    ptr->pointer = position;
    ptr->subtreesize = subtreesize;
    ptr->reduce_value.buf = buf;
    ptr->reduce_value.size = redsize;
    return ptr;
}

void encode_root(char *buf, node_pointer *node)
{
    if (node) {
        memset(buf, 0, 12);
        set_bits(buf, 0, 48, node->pointer);
        set_bits(buf + 6, 0, 48, node->subtreesize);
        memcpy(buf + 12, node->reduce_value.buf, node->reduce_value.size);
    }
}

fatbuf *fatbuf_alloc(size_t bytes)
{
    fatbuf *fb = (fatbuf *) malloc(sizeof(fatbuf) + bytes);
#ifdef DEBUG
    memset(fb->buf, 0x44, bytes);
#endif
    if (!fb) {
        return NULL;
    }

    fb->size = bytes;
    fb->pos = 0;
    return fb;
}

void *fatbuf_get(fatbuf *fb, size_t bytes)
{
    if (fb->pos + bytes > fb->size) {
        return NULL;
    }
#ifdef DEBUG
    if (fb->buf[fb->pos] != 0x44) {
        fprintf(stderr, "Fatbuf space has been written to before it was taken!\n");
    }
#endif
    void *rptr = fb->buf + fb->pos;
    fb->pos += bytes;
    return rptr;
}

void fatbuf_free(fatbuf *fb)
{
    free(fb);
}
