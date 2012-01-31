#ifndef COUCHSTORE_FATBUF_H
#define COUCHSTORE_FATBUF_H

typedef struct _fat_buffer {
    size_t pos;
    size_t size;
    char buf[1];
} fatbuf;

fatbuf* fatbuf_alloc(size_t bytes);
void* fatbuf_get(fatbuf* fb, size_t bytes);
void fatbuf_free(fatbuf* fb);
#endif
