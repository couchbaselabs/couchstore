//
//  iobuffer.c
//  couchstore
//
//  Created by Jens Alfke on 4/12/12.
//  Copyright (c) 2012 Couchbase, Inc. All rights reserved.
//

#include "config.h"
#include "iobuffer.h"
#include "internal.h"
#include <stdlib.h>
#include <string.h>

#define LOG_BUFFER 0 && defined(DEBUG)
#if LOG_BUFFER
#include <stdio.h>
#endif


#define MAX_READ_BUFFERS 8
#define WRITE_BUFFER_CAPACITY (128*1024)
#define READ_BUFFER_CAPACITY (8*1024)

#ifdef min
#undef min
#endif

static inline ssize_t min(ssize_t a, ssize_t b) {return a < b ? a : b;}


typedef struct file_buffer {
    struct file_buffer* prev;
    struct file_buffer* next;
    struct buffered_file_handle *owner;
    size_t capacity;
    size_t length;
    cs_off_t offset;
    uint8_t dirty;
    uint8_t bytes[1];
} file_buffer;


// How I interpret a couch_file_handle:
typedef struct buffered_file_handle {
    const couch_file_ops* raw_ops;
    couch_file_handle raw_ops_handle;
    unsigned nbuffers;
    file_buffer* write_buffer;
    file_buffer* first_buffer;
} buffered_file_handle;


static file_buffer* new_buffer(buffered_file_handle* owner, size_t capacity) {
    file_buffer *buf = malloc(sizeof(file_buffer) + capacity);
    if (buf) {
        buf->prev = buf->next = NULL;
        buf->owner = owner;
        buf->capacity = capacity;
        buf->length = 0;
        buf->offset = 0;
        buf->dirty = 0;
    }
#if LOG_BUFFER
    fprintf(stderr, "BUFFER: %p <- new_buffer(%zu)\n", buf, capacity);
#endif
    return buf;
}

static void free_buffer(file_buffer* buf) {
#if LOG_BUFFER
    fprintf(stderr, "BUFFER: %p freed\n", buf);
#endif
    free(buf);
}


//////// BUFFER WRITES:


// Write as many bytes as possible into the buffer, returning the count
static size_t write_to_buffer(file_buffer* buf, const void *bytes, size_t nbyte, cs_off_t offset)
{
    if (buf->length == 0) {
        // If buffer is empty, align it to start at the current offset:
        buf->offset = offset;
    } else if (offset < buf->offset || offset > buf->offset + (cs_off_t)buf->length) {
        // If it's out of range, don't write anything
        return 0;
    }
    size_t offset_in_buffer = (size_t)(offset - buf->offset);
    size_t buffer_nbyte = min(buf->capacity - offset_in_buffer, nbyte);

    memcpy(buf->bytes + offset_in_buffer, bytes, buffer_nbyte);
    buf->dirty = 1;
    offset_in_buffer += buffer_nbyte;
    if (offset_in_buffer > buf->length)
        buf->length = offset_in_buffer;
    
    return buffer_nbyte;
}

// Write the current buffer to disk and empty it.
static couchstore_error_t flush_buffer(file_buffer* buf) {
    while (buf->length > 0 && buf->dirty) {
        ssize_t raw_written = buf->owner->raw_ops->pwrite(buf->owner->raw_ops_handle, buf->bytes, buf->length, buf->offset);
#if LOG_BUFFER
        fprintf(stderr, "BUFFER: %p flush %zd bytes at %zd --> %zd\n",
                buf, buf->length, buf->offset, raw_written);
#endif
        if (raw_written <= 0)
            return (couchstore_error_t) raw_written;
        buf->length -= raw_written;
        buf->offset += raw_written;
        memmove(buf->bytes, buf->bytes + raw_written, buf->length);
    }
    buf->dirty = 0;
    return COUCHSTORE_SUCCESS;
}


//////// BUFFER READS:


static size_t read_from_buffer(file_buffer* buf, void *bytes, size_t nbyte, cs_off_t offset) {
    if (offset < buf->offset || offset >= buf->offset + (cs_off_t)buf->length) {
        return 0;
    }
    size_t offset_in_buffer = (size_t)(offset - buf->offset);
    size_t buffer_nbyte = min(buf->length - offset_in_buffer, nbyte);

    memcpy(bytes, buf->bytes + offset_in_buffer, buffer_nbyte);
    return buffer_nbyte;
}


static couchstore_error_t load_buffer_from(file_buffer* buf, cs_off_t offset, size_t nbyte) {
    if (buf->dirty) {
        // If buffer contains data to be written, flush it first:
        couchstore_error_t err = flush_buffer(buf);
        if (err < 0) {
            return err;
        }
    }
    
    if (offset < buf->offset || offset + nbyte > buf->offset + buf->capacity) {
        // Reset the buffer to empty if it has to move:
        buf->offset = offset;
        buf->length = 0;
    }

    // Read data to extend the buffer to its capacity (if possible):
    ssize_t bytes_read = buf->owner->raw_ops->pread(buf->owner->raw_ops_handle,
                                           buf->bytes + buf->length,
                                           buf->capacity - buf->length,
                                           buf->offset + buf->length);
#if LOG_BUFFER
    fprintf(stderr, "BUFFER: %p loaded %zd bytes from %zd\n", buf, bytes_read, offset + buf->length);
#endif
    if (bytes_read < 0) {
        return (couchstore_error_t) bytes_read;
    }
    buf->length += bytes_read;
    return COUCHSTORE_SUCCESS;
}


//////// BUFFER MANAGEMENT:


static file_buffer* find_buffer(buffered_file_handle* h, cs_off_t offset) {
    offset = offset - offset % READ_BUFFER_CAPACITY;
    // Find a buffer for this offset, or use the last one:
    file_buffer* buffer = h->first_buffer;
    while (buffer->offset != offset && buffer->next != NULL)
        buffer = buffer->next;
    if (buffer->offset != offset) {
        if (h->nbuffers < MAX_READ_BUFFERS) {
            // Didn't find a matching one, but we can still create another:
            file_buffer* buffer2 = new_buffer(h, READ_BUFFER_CAPACITY);
            if (buffer2) {
                buffer = buffer2;
                ++h->nbuffers;
            }
        } else {
#if LOG_BUFFER
            fprintf(stderr, "BUFFER: %p recycled, from %zd to %zd\n", buffer, buffer->offset, offset);
#endif
        }
    }
    if (buffer != h->first_buffer) {
        // Move the buffer to the start of the list:
        if (buffer->prev) buffer->prev->next = buffer->next;
        if (buffer->next) buffer->next->prev = buffer->prev;
        buffer->prev = NULL;
        h->first_buffer->prev = buffer;
        buffer->next = h->first_buffer;
        h->first_buffer = buffer;
    }
    return buffer;
}


//////// FILE API:


static void buffered_destructor(couch_file_handle handle)
{
    buffered_file_handle *h = (buffered_file_handle*)handle;
    if (!h) {
        return;
    }
    h->raw_ops->destructor(h->raw_ops_handle);
	
    free_buffer(h->write_buffer);
    file_buffer* buffer, *next;
    for (buffer = h->first_buffer; buffer; buffer = next) {
        next = buffer->next;
        free_buffer(buffer);
    }
    free(h);
}

static couch_file_handle buffered_constructor_with_raw_ops(const couch_file_ops* raw_ops)
{
    buffered_file_handle *h = malloc(sizeof(buffered_file_handle));
    if (h) {
        h->raw_ops = raw_ops;
        h->raw_ops_handle = raw_ops->constructor(raw_ops->cookie);
        h->nbuffers = 1;
        h->write_buffer = new_buffer(h, WRITE_BUFFER_CAPACITY);
        h->first_buffer = new_buffer(h, READ_BUFFER_CAPACITY);
        
        if (!h->write_buffer || !h->first_buffer) {
            buffered_destructor((couch_file_handle)h);
            h = NULL;
        }
    }
    return (couch_file_handle) h;
}

static couch_file_handle buffered_constructor(void* cookie)
{
    (void) cookie;
    return buffered_constructor_with_raw_ops(couchstore_get_default_file_ops());
}

static couchstore_error_t buffered_open(couch_file_handle* handle, const char *path, int oflag)
{
    buffered_file_handle *h = (buffered_file_handle*)*handle;
    return h->raw_ops->open(&h->raw_ops_handle, path, oflag);
}

static void buffered_close(couch_file_handle handle)
{
    buffered_file_handle *h = (buffered_file_handle*)handle;
    if (!h) {
        return;
    }
    flush_buffer(h->write_buffer);
    h->raw_ops->close(h->raw_ops_handle);
}

static ssize_t buffered_pread(couch_file_handle handle, void *buf, size_t nbyte, cs_off_t offset)
{
#if LOG_BUFFER
    //fprintf(stderr, "r");
#endif
    buffered_file_handle *h = (buffered_file_handle*)handle;
    // Flush the write buffer before trying to read anything:
    couchstore_error_t err = flush_buffer(h->write_buffer);
    if (err < 0) {
        return err;
    }
    
    ssize_t total_read = 0;
    while (nbyte > 0) {
        file_buffer* buffer = find_buffer(h, offset);
        
        // Read as much as we can from the current buffer:
        ssize_t nbyte_read = read_from_buffer(buffer, buf, nbyte, offset);
        if (nbyte_read == 0) {
            /*if (nbyte > buffer->capacity) {
                // Remainder won't fit in a single buffer, so just read it directly:
                nbyte_read = h->raw_ops->pread(h->raw_ops_handle, buf, nbyte, offset);
                if (nbyte_read < 0) {
                    return nbyte_read;
                }
            } else*/ {
                // Move the buffer to cover the remainder of the data to be read.
                cs_off_t block_start = offset - (offset % READ_BUFFER_CAPACITY);
                err = load_buffer_from(buffer, block_start, (size_t)(offset + nbyte - block_start));
                if (err < 0) {
                    return err;
                }
                nbyte_read = read_from_buffer(buffer, buf, nbyte, offset);
                if (nbyte_read == 0)
                    break;  // must be at EOF
            }
        }
        buf = (char*)buf + nbyte_read;
        nbyte -= nbyte_read;
        offset += nbyte_read;
        total_read += nbyte_read;
    }
    return total_read;
}

static ssize_t buffered_pwrite(couch_file_handle handle, const void *buf, size_t nbyte, cs_off_t offset)
{
#if LOG_BUFFER
    //fprintf(stderr, "w");
#endif
    if (nbyte == 0) {
        return 0;
    }
    
    buffered_file_handle *h = (buffered_file_handle*)handle;
    file_buffer* buffer = h->write_buffer;
    
    // Write data to the current buffer:
    size_t nbyte_written = write_to_buffer(buffer, buf, nbyte, offset);
    if (nbyte_written > 0) {
        buf = (char*)buf + nbyte_written;
        offset += nbyte_written;
        nbyte -= nbyte_written;
    }
        
    // Flush the buffer if it's full, or if it isn't aligned with the current write:
    if (buffer->length == buffer->capacity || nbyte_written == 0) {
        couchstore_error_t error = flush_buffer(buffer);
        if (error < 0)
            return error;
    }
    
    if (nbyte > 0) {
        ssize_t written;
        // If the remaining data will fit into the buffer, write it; else write directly:
        if (nbyte <= (buffer->capacity - buffer->length)) {
            written = write_to_buffer(buffer, buf, nbyte, offset);
        } else {
            written = h->raw_ops->pwrite(h->raw_ops_handle, buf, nbyte, offset);
#if LOG_BUFFER
            fprintf(stderr, "BUFFER: passthru %zd bytes at %zd --> %zd\n",
                    nbyte, offset, written);
#endif
            if (written < 0) {
                return written;
            }
        }
        nbyte_written += written;
    }

    return nbyte_written;
}

static cs_off_t buffered_goto_eof(couch_file_handle handle)
{
    buffered_file_handle *h = (buffered_file_handle*)handle;
    return h->raw_ops->goto_eof(h->raw_ops_handle);
}

static couchstore_error_t buffered_sync(couch_file_handle handle)
{
    buffered_file_handle *h = (buffered_file_handle*)handle;
    couchstore_error_t err = flush_buffer(h->write_buffer);
    if (err == COUCHSTORE_SUCCESS) {
        err = h->raw_ops->sync(h->raw_ops_handle);
    }
    return err;
}

static const couch_file_ops ops = {
    (uint64_t)3,
    buffered_constructor,
    buffered_open,
    buffered_close,
    buffered_pread,
    buffered_pwrite,
    buffered_goto_eof,
    buffered_sync,
    buffered_destructor,
    NULL
};

const couch_file_ops *couch_get_buffered_file_ops(const couch_file_ops* raw_ops,
                                                  couch_file_handle* handle)
{
    *handle = buffered_constructor_with_raw_ops(raw_ops);
    return &ops;
}
