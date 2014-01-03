/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <snappy.h>

#include "internal.h"
#include "iobuffer.h"
#include "bitfield.h"
#include "crc32.h"
#include "util.h"


couchstore_error_t tree_file_open(tree_file* file,
                                  const char *filename,
                                  int openflags,
                                  const couch_file_ops *ops)
{
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;

    /* Sanity check input parameters */
    if (filename == NULL || file == NULL || ops == NULL ||
            ops->version != 5 ||
            ops->constructor == NULL || ops->open == NULL ||
            ops->close == NULL || ops->pread == NULL ||
            ops->pwrite == NULL || ops->goto_eof == NULL ||
            ops->sync == NULL || ops->destructor == NULL) {
        return COUCHSTORE_ERROR_INVALID_ARGUMENTS;
    }

    memset(file, 0, sizeof(*file));

    file->path = (const char *) strdup(filename);
    error_unless(file->path, COUCHSTORE_ERROR_ALLOC_FAIL);

    file->ops = couch_get_buffered_file_ops(&file->lastError, ops, &file->handle);
    error_unless(file->ops, COUCHSTORE_ERROR_ALLOC_FAIL);

    error_pass(file->ops->open(&file->lastError, &file->handle,
                               filename, openflags));

cleanup:
    if (errcode != COUCHSTORE_SUCCESS) {
        free((char *) file->path);
        file->path = NULL;
        if (file->ops) {
            file->ops->destructor(&file->lastError, file->handle);
            file->ops = NULL;
            file->handle = NULL;
        }
    }
    return errcode;
}

void tree_file_close(tree_file* file)
{
    if (file->ops) {
        file->ops->close(&file->lastError, file->handle);
        file->ops->destructor(&file->lastError, file->handle);
    }
    free((char*)file->path);
}

/** Read bytes from the database file, skipping over the header-detection bytes at every block
    boundary. */
static couchstore_error_t read_skipping_prefixes(tree_file *file,
                                                 cs_off_t *pos,
                                                 ssize_t len,
                                                 void *dst) {
    if (*pos % COUCH_BLOCK_SIZE == 0) {
        ++*pos;
    }
    while (len > 0) {
        ssize_t read_size = COUCH_BLOCK_SIZE - (*pos % COUCH_BLOCK_SIZE);
        if (read_size > len) {
            read_size = len;
        }
        ssize_t got_bytes = file->ops->pread(&file->lastError, file->handle,
                                             dst, read_size, *pos);
        if (got_bytes < 0) {
            return (couchstore_error_t) got_bytes;
        } else if (got_bytes == 0) {
            return COUCHSTORE_ERROR_READ;
        }
        *pos += got_bytes;
        len -= got_bytes;
        dst = (char*)dst + got_bytes;
        if (*pos % COUCH_BLOCK_SIZE == 0) {
            ++*pos;
        }
    }
    return COUCHSTORE_SUCCESS;
}

/*
 * Common subroutine of pread_bin, pread_compressed and pread_header.
 * Parameters and return value are the same as for pread_bin,
 * except the 'max_header_size' parameter which is greater than 0 if
 * reading a header, 0 otherwise.
 */
static int pread_bin_internal(tree_file *file,
                              cs_off_t pos,
                              char **ret_ptr,
                              uint32_t max_header_size)
{
    struct {
        uint32_t chunk_len;
        uint32_t crc32;
    } info;

    couchstore_error_t err = read_skipping_prefixes(file, &pos, sizeof(info), &info);
    if (err < 0) {
        return err;
    }

    info.chunk_len = ntohl(info.chunk_len) & ~0x80000000;
    if (max_header_size) {
        if (info.chunk_len < 4 || info.chunk_len > max_header_size)
            return COUCHSTORE_ERROR_CORRUPT;
        info.chunk_len -= 4;    //Header len includes CRC len.
    }
    info.crc32 = ntohl(info.crc32);

    char* buf = static_cast<char*>(malloc(info.chunk_len));
    if (!buf) {
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }
    err = read_skipping_prefixes(file, &pos, info.chunk_len, buf);
    if (!err && info.crc32 && info.crc32 != hash_crc32(buf, info.chunk_len)) {
        err = COUCHSTORE_ERROR_CHECKSUM_FAIL;
    }
    if (err < 0) {
        free(buf);
        return err;
    }

    *ret_ptr = buf;
    return info.chunk_len;
}

int pread_header(tree_file *file,
                 cs_off_t pos,
                 char **ret_ptr,
                 uint32_t max_header_size)
{
    if (max_header_size == 0) {
        return COUCHSTORE_ERROR_INVALID_ARGUMENTS;
    }

    return pread_bin_internal(file, pos + 1, ret_ptr, max_header_size);
}

int pread_compressed(tree_file *file, cs_off_t pos, char **ret_ptr)
{
    char *compressed_buf;
    char *new_buf;
    int len = pread_bin_internal(file, pos, &compressed_buf, 0);
    if (len < 0) {
        return len;
    }
    size_t uncompressed_len;

    if (!snappy::GetUncompressedLength(compressed_buf, len, &uncompressed_len)) {
        //should be compressed but snappy doesn't see it as valid.
        free(compressed_buf);
        return COUCHSTORE_ERROR_CORRUPT;
    }

    new_buf = static_cast<char *>(malloc(uncompressed_len));
    if (!new_buf) {
        free(compressed_buf);
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }

    if (!snappy::RawUncompress(compressed_buf, len, new_buf)) {
        free(compressed_buf);
        free(new_buf);
        return COUCHSTORE_ERROR_CORRUPT;
    }

    free(compressed_buf);
    *ret_ptr = new_buf;
    return static_cast<int>(uncompressed_len);
}

int pread_bin(tree_file *file, cs_off_t pos, char **ret_ptr)
{
    return pread_bin_internal(file, pos, ret_ptr, 0);
}
