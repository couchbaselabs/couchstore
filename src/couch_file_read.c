/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <netinet/in.h>
#include <snappy-c.h>

#include "internal.h"
#include "bitfield.h"
#include "crc32.h"
#include "util.h"

ssize_t total_read_len(off_t blockoffset, ssize_t finallen)
{
    ssize_t left;
    ssize_t add = 0;
    if (blockoffset == 0) {
        add++;
        blockoffset = 1;
    }

    left = COUCH_BLOCK_SIZE - blockoffset;
    if (left >= finallen) {
        return finallen + add;
    } else {
        if ((finallen - left) % (COUCH_BLOCK_SIZE - 1) != 0) {
            add++;
        }
        return finallen + add + ((finallen - left) / (COUCH_BLOCK_SIZE - 1));
    }
}

/** Edits out the header-detection prefix byte at the start of each file block.
    @param buf Memory buffer containing raw data read from the file
    @param offset offset from a block boundary at which the buffer starts
    @param len Length of the block in bytes
    @return The length of the block after editing out the prefix bytes */
static int remove_block_prefixes(char *buf, off_t offset, ssize_t len)
{
    off_t buf_pos = 0;
    off_t gap = 0;
    ssize_t remain_block;
    while (buf_pos + gap < len) {
        remain_block = COUCH_BLOCK_SIZE - offset;

        if (offset == 0) {
            gap++;
        }

        if (remain_block > (len - gap - buf_pos)) {
            remain_block = len - gap - buf_pos;
        }

        if (offset == 0) {
            //printf("Move %d bytes <-- by %d, landing at %d\r\n", remain_block, gap, buf_pos);
            memmove(buf + buf_pos, buf + buf_pos + gap, remain_block);
            offset = 1;
        } else {
            buf_pos += remain_block;
            offset = 0;
        }
    }
    return len - gap;
}

/** Reads data from the file, skipping block prefix bytes.
    @param db Database to read from
    @param pos Points to file position to read from. On successful return, will be incremented
                by the number of physical bytes read.
    @param len Number of data bytes to read
    @param dst  On success, will be set to point to a malloc'ed buffer containing the bytes
    @return Number of data bytes read, or an error code */
static int raw_read(Db *db, off_t *pos, ssize_t len, char **dst)
{
    off_t blockoffs = *pos % COUCH_BLOCK_SIZE;
    ssize_t total = total_read_len(blockoffs, len);
    *dst = (char *) malloc(total);
    if (!*dst) {
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }
    ssize_t got_bytes = db->file_ops->pread(db, *dst, total, *pos);
    if (got_bytes <= 0) {
        free(*dst);
        *dst = NULL;
        return COUCHSTORE_ERROR_READ;
    }

    *pos += got_bytes;
    return remove_block_prefixes(*dst, blockoffs, got_bytes);
}

/** Common subroutine of pread_bin, pread_compressed and pread_header.
    Parameters and return value are the same as for pread_bin,
    except the 'header' parameter which is 1 if reading a header, 0 otherwise. */
static int pread_bin_internal(Db *db, off_t pos, char **ret_ptr, int header)
{
    char *bufptr = NULL, *bufptr_rest = NULL, *newbufptr = NULL;
    int buf_len;
    uint32_t chunk_len, crc32 = 0;
    int skip = 0;
    int errcode = 0;
    buf_len = raw_read(db, &pos, 2 * COUCH_BLOCK_SIZE - (pos % COUCH_BLOCK_SIZE), &bufptr);
    if (buf_len < 0) {
        buf_len = raw_read(db, &pos, 4, &bufptr);       //??? What is the purpose of this? Shouldn't it at least read 8 bytes? -Jens
        error_unless(buf_len >= 0, buf_len);  // if negative, it's an error code
    }
    error_unless(buf_len >= 8, COUCHSTORE_ERROR_READ);

    chunk_len = get_32(bufptr) & ~0x80000000;
    skip += 4;
    if (header) {
        chunk_len -= 4;    //Header len includes hash len.
    }
    crc32 = get_32(bufptr + 4);
    skip += 4;

    if (chunk_len == 0) {
        free(bufptr);
        *ret_ptr = NULL;
        return 0;
    }

    buf_len -= skip;
    memmove(bufptr, bufptr + skip, buf_len);
    if (chunk_len <= (uint32_t)buf_len) {
        newbufptr = (char *) realloc(bufptr, chunk_len);
        error_unless(newbufptr, COUCHSTORE_ERROR_ALLOC_FAIL);
        bufptr = newbufptr;
    } else {
        int rest_len = raw_read(db, &pos, chunk_len - buf_len, &bufptr_rest);
        error_unless(rest_len >= 0, rest_len);  // if negative, it's an error code
        error_unless((unsigned) rest_len + buf_len == chunk_len, COUCHSTORE_ERROR_READ);

        newbufptr = (char *) realloc(bufptr, buf_len + rest_len);
        error_unless(newbufptr, COUCHSTORE_ERROR_ALLOC_FAIL);
        bufptr = newbufptr;

        memcpy(bufptr + buf_len, bufptr_rest, rest_len);
        free(bufptr_rest);
        bufptr_rest = NULL;
    }
    if (crc32) {
        error_unless(crc32 == hash_crc32(bufptr, chunk_len), COUCHSTORE_ERROR_CHECKSUM_FAIL);
    }
    *ret_ptr = bufptr;
    return chunk_len;

cleanup:
    free(bufptr);
    free(bufptr_rest);
    return errcode;
}

int pread_header(Db *db, off_t pos, char **ret_ptr)
{
    return pread_bin_internal(db, pos + 1, ret_ptr, 1);
}

int pread_compressed(Db *db, off_t pos, char **ret_ptr)
{
    char *compressed_buf;
    char *new_buf;
    int len = pread_bin_internal(db, pos, &compressed_buf, 0);
    if (len < 0) {
        return len;
    }
    size_t uncompressed_len;
    if (snappy_uncompressed_length(compressed_buf, len, &uncompressed_len) != SNAPPY_OK) {
        //should be compressed but snappy doesn't see it as valid.
        free(compressed_buf);
        return COUCHSTORE_ERROR_CORRUPT;
    }

    new_buf = (char *) malloc(uncompressed_len);
    if (!new_buf) {
        free(compressed_buf);
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }
    snappy_status ss = (snappy_uncompress(compressed_buf, len, new_buf, &uncompressed_len));
    free(compressed_buf);
    if (ss != SNAPPY_OK) {
        return COUCHSTORE_ERROR_CORRUPT;
    }

    *ret_ptr = new_buf;
    return uncompressed_len;
}

int pread_bin(Db *db, off_t pos, char **ret_ptr)
{
    return pread_bin_internal(db, pos, ret_ptr, 0);
}
