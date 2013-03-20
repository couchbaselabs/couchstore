/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>
#include <snappy-c.h>
#include <libcouchstore/couch_db.h>

#include "rfc1321/global.h"
#include "rfc1321/md5.h"
#include "internal.h"
#include "crc32.h"
#include "util.h"

static ssize_t raw_write(Db *db, const sized_buf *buf, cs_off_t pos)
{
    cs_off_t write_pos = pos;
    size_t buf_pos = 0;
    char blockprefix = 0;
    ssize_t written;
    size_t block_remain;
    while (buf_pos < buf->size) {
        block_remain = COUCH_BLOCK_SIZE - (write_pos % COUCH_BLOCK_SIZE);
        if (block_remain > (buf->size - buf_pos)) {
            block_remain = buf->size - buf_pos;
        }

        if (write_pos % COUCH_BLOCK_SIZE == 0) {
            written = db->file_ops->pwrite(db->file_handle, &blockprefix, 1, write_pos);
            if (written < 0) {
                return written;
            }
            write_pos += 1;
            continue;
        }

        written = db->file_ops->pwrite(db->file_handle, buf->buf + buf_pos, block_remain, write_pos);
        if (written < 0) {
            return written;
        }
        buf_pos += written;
        write_pos += written;
    }

    return (ssize_t)(write_pos - pos);
}

couchstore_error_t db_write_header(Db *db, sized_buf *buf, cs_off_t *pos)
{
    cs_off_t write_pos = db->file_pos;
    ssize_t written;
    uint32_t size = htonl(buf->size + 4); //Len before header includes hash len.
    uint32_t crc32 = htonl(hash_crc32(buf->buf, buf->size));
    char headerbuf[1 + 4 + 4];

    if (write_pos % COUCH_BLOCK_SIZE != 0) {
        write_pos += COUCH_BLOCK_SIZE - (write_pos % COUCH_BLOCK_SIZE);    //Move to next block boundary.
    }
    *pos = write_pos;

    // Write the header's block header
    headerbuf[0] = 1;
    memcpy(&headerbuf[1], &size, 4);
    memcpy(&headerbuf[5], &crc32, 4);

    written = db->file_ops->pwrite(db->file_handle, &headerbuf, sizeof(headerbuf), write_pos);
    if (written < 0) {
        return (couchstore_error_t)written;
    }
    write_pos += written;

    //Write actual header
    written = raw_write(db, buf, write_pos);
    if (written < 0) {
        return (couchstore_error_t)written;
    }
    write_pos += written;
    db->file_pos = write_pos;

    return COUCHSTORE_SUCCESS;
}

int db_write_buf(Db *db, const sized_buf *buf, cs_off_t *pos, size_t *disk_size)
{
    cs_off_t write_pos = db->file_pos;
    cs_off_t end_pos = write_pos;
    ssize_t written;
    uint32_t size = htonl(buf->size | 0x80000000);
    uint32_t crc32 = htonl(hash_crc32(buf->buf, buf->size));
    char headerbuf[4 + 4];

    // Write the buffer's header:
    memcpy(&headerbuf[0], &size, 4);
    memcpy(&headerbuf[4], &crc32, 4);

    sized_buf sized_headerbuf = { headerbuf, 8 };
    written = raw_write(db, &sized_headerbuf, end_pos);
    if (written < 0) {
        return (int)written;
    }
    end_pos += written;

    // Write actual buffer:
    written = raw_write(db, buf, end_pos);
    if (written < 0) {
        return (int)written;
    }
    end_pos += written;

    if (pos) {
        *pos = write_pos;
    }

    db->file_pos = end_pos;
    if (disk_size) {
        *disk_size = (size_t) (end_pos - write_pos);
    }

    return 0;
}

int db_write_buf_compressed(Db *db, const sized_buf *buf, cs_off_t *pos, size_t *disk_size)
{
    int errcode = 0;
    sized_buf to_write;
    size_t max_size = snappy_max_compressed_length(buf->size);

    to_write.buf = (char *) malloc(max_size);
    to_write.size = max_size;
    error_unless(to_write.buf, COUCHSTORE_ERROR_ALLOC_FAIL);
    error_unless(snappy_compress(buf->buf, buf->size, to_write.buf,
                                 &to_write.size) == SNAPPY_OK,
                 COUCHSTORE_ERROR_WRITE);

    error_pass(db_write_buf(db, &to_write, pos, disk_size));
cleanup:
    free(to_write.buf);
    return errcode;
}
