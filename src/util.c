/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "node_types.h"
#include "util.h"
#include <stdlib.h>

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
    uint64_t e1val = decode_sequence_key(k1);
    uint64_t e2val = decode_sequence_key(k2);
    if (e1val == e2val) {
        return 0;
    }
    return (e1val < e2val ? -1 : 1);
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
