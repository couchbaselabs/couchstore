/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <stdlib.h>
#include <libcouchstore/couch_db.h>
#include "util.h"
#include "bitfield.h"

#ifdef DEBUG
#include <stdio.h>
#endif

int ebin_cmp(const sized_buf *e1, const sized_buf *e2)
{
    size_t size;
    if (e2->size < e1->size) {
        size = e2->size;
    } else {
        size = e1->size;
    }
    
    int cmp = memcmp(e1->buf, e2->buf, size);
    if (cmp == 0) {
        if (size < e2->size) {
            return -1;
        } else if (size < e1->size) {
            return 1;
        }
    }
    return cmp;
}

int seq_cmp(const sized_buf *k1, const sized_buf *k2)
{
    uint64_t e1val = get_48(k1->buf);
    uint64_t e2val = get_48(k2->buf);
    if (e1val == e2val) {
        return 0;
    }
    return (e1val < e2val ? -1 : 1);
}

node_pointer *read_root(char *buf, int size)
{
    node_pointer *ptr;
    uint64_t position = get_48(buf);
    uint64_t subtreesize = get_48(buf + 6);
    int redsize = size - 12;
    ptr = (node_pointer *) malloc(sizeof(node_pointer) + redsize);
    if (!ptr) {
        return NULL;
    }
    buf = (char *) memcpy(((char *)ptr) + sizeof(node_pointer), buf + 12, redsize);
    ptr->key.buf = NULL;
    ptr->key.size = 0;
    ptr->pointer = position;
    ptr->subtreesize = subtreesize;
    ptr->reduce_value.buf = buf;
    ptr->reduce_value.size = redsize;
    return ptr;
}

size_t encode_root(char *buf, node_pointer *node)
{
    if (!node)
        return 0;
    if (buf) {
        memset(buf, 0, 12);
        set_bits(buf, 0, 48, node->pointer);
        set_bits(buf + 6, 0, 48, node->subtreesize);
        memcpy(buf + 12, node->reduce_value.buf, node->reduce_value.size);
    }
    return 12 + node->reduce_value.size;
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

#ifdef DEBUG
void report_error(couchstore_error_t errcode, const char* file, int line) {
    fprintf(stderr, "Couchstore error `%s' at %s:%d\r\n", \
            couchstore_strerror(errcode), file, line);
}
#endif
