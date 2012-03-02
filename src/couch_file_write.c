/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <snappy-c.h>
#include <libcouchstore/couch_db.h>

#include "rfc1321/global.h"
#include "rfc1321/md5.h"
#include "internal.h"
#include "crc32.h"
#include "util.h"

static ssize_t raw_write(Db *db, sized_buf *buf, off_t pos)
{
    off_t write_pos = pos;
    off_t buf_pos = 0;
    char blockprefix = 0;
    ssize_t written;
    size_t block_remain;
    while (buf_pos < (off_t)buf->size) {
        block_remain = COUCH_BLOCK_SIZE - (write_pos % COUCH_BLOCK_SIZE);
        if (block_remain > (buf->size - buf_pos)) {
            block_remain = buf->size - buf_pos;
        }

        if (write_pos % COUCH_BLOCK_SIZE == 0) {
            written = db->file_ops->pwrite(db, &blockprefix, 1, write_pos);
            if (written < 0) {
                return COUCHSTORE_ERROR_WRITE;
            }
            write_pos += 1;
            continue;
        }

        written = db->file_ops->pwrite(db, buf->buf + buf_pos, block_remain, write_pos);
        if (written < 0) {
            return COUCHSTORE_ERROR_WRITE;
        }
        buf_pos += written;
        write_pos += written;
    }

    return write_pos - pos;
}

couchstore_error_t db_write_header(Db *db, sized_buf *buf, off_t *pos)
{
    off_t write_pos = db->file_pos;
    ssize_t written;
    char blockheader = 1;
    uint32_t size = htonl(buf->size + 4); //Len before header includes hash len.
    uint32_t crc32 = htonl(hash_crc32(buf->buf, buf->size));
    sized_buf lenbuf = { (char *) &size, 4 };
    sized_buf crcbuf = { (char *) &crc32, 4 };
    if (write_pos % COUCH_BLOCK_SIZE != 0) {
        write_pos += COUCH_BLOCK_SIZE - (write_pos % COUCH_BLOCK_SIZE);    //Move to next block boundary.
    }
    *pos = write_pos;

    written = db->file_ops->pwrite(db, &blockheader, 1, write_pos);
    if (written < 0) {
        return COUCHSTORE_ERROR_WRITE;
    }
    write_pos += written;

    //Write length
    written = raw_write(db, &lenbuf, write_pos);
    if (written < 0) {
        return COUCHSTORE_ERROR_WRITE;
    }
    write_pos += written;

    //Write CRC32
    written = raw_write(db, &crcbuf, write_pos);
    if (written < 0) {
        return COUCHSTORE_ERROR_WRITE;
    }
    write_pos += written;

    //Write actual header
    written = raw_write(db, buf, write_pos);
    if (written < 0) {
        return COUCHSTORE_ERROR_WRITE;
    }
    write_pos += written;
    db->file_pos = write_pos;

    return COUCHSTORE_SUCCESS;
}

int db_write_buf(Db *db, sized_buf *buf, off_t *pos)
{
    off_t write_pos = db->file_pos;
    off_t end_pos = write_pos;
    ssize_t written;
    uint32_t size = htonl(buf->size | 0x80000000);
    uint32_t crc32 = htonl(hash_crc32(buf->buf, buf->size));
    sized_buf lenbuf = { (char *) &size, 4 };
    sized_buf crcbuf = { (char *) &crc32, 4 };

    written = raw_write(db, &lenbuf, end_pos);
    if (written < 0) {
        return COUCHSTORE_ERROR_WRITE;
    }
    end_pos += written;

    written = raw_write(db, &crcbuf, end_pos);
    if (written < 0) {
        return COUCHSTORE_ERROR_WRITE;
    }
    end_pos += written;

    written = raw_write(db, buf, end_pos);
    if (written < 0) {
        return COUCHSTORE_ERROR_WRITE;
    }
    end_pos += written;

    if (pos) {
        *pos = write_pos;
    }

    db->file_pos = end_pos;
    return 0;
}

int db_write_buf_compressed(Db *db, sized_buf *buf, off_t *pos)
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

    error_pass(db_write_buf(db, &to_write, pos));
cleanup:
    if (to_write.buf) {
        free(to_write.buf);
    }
    return errcode;
}

