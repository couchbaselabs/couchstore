/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <snappy-c.h>

#include "internal.h"
#include "iobuffer.h"
#include "bitfield.h"
#include "crc32.h"
#include "util.h"

#define MAX_HEADER_SIZE 1024    // Conservative estimate; just for sanity check

couchstore_error_t tree_file_open(tree_file* file,
                                  const char *filename,
                                  int openflags,
                                  const couch_file_ops *ops)
{
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;

    /* Sanity check input parameters */
    if (filename == NULL || file == NULL || ops == NULL ||
            ops->version != 4 || ops->constructor == NULL || ops->open == NULL ||
            ops->close == NULL || ops->pread == NULL ||
            ops->pwrite == NULL || ops->goto_eof == NULL ||
            ops->sync == NULL || ops->destructor == NULL) {
        return COUCHSTORE_ERROR_INVALID_ARGUMENTS;
    }

    memset(file, 0, sizeof(*file));

    file->path = strdup(filename);
    error_unless(file->path, COUCHSTORE_ERROR_ALLOC_FAIL);

    file->ops = couch_get_buffered_file_ops(ops, &file->handle);
    error_unless(file->ops, COUCHSTORE_ERROR_ALLOC_FAIL);

    error_pass(file->ops->open(&file->handle, filename, openflags));

cleanup:
    return errcode;
}

void tree_file_close(tree_file* file)
{
    if (file->ops) {
        file->ops->close(file->handle);
        file->ops->destructor(file->handle);
    }
    free((char*)file->path);
}

/** Read bytes from the database file, skipping over the header-detection bytes at every block
    boundary. */
static couchstore_error_t read_skipping_prefixes(tree_file *file, cs_off_t *pos, ssize_t len, void *dst) {
    if (*pos % COUCH_BLOCK_SIZE == 0) {
        ++*pos;
    }
    while (len > 0) {
        ssize_t read_size = COUCH_BLOCK_SIZE - (*pos % COUCH_BLOCK_SIZE);
        if (read_size > len) {
            read_size = len;
        }
        ssize_t got_bytes = file->ops->pread(file->handle, dst, read_size, *pos);
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

/** Common subroutine of pread_bin, pread_compressed and pread_header.
    Parameters and return value are the same as for pread_bin,
    except the 'header' parameter which is 1 if reading a header, 0 otherwise. */
static int pread_bin_internal(tree_file *file, cs_off_t pos, char **ret_ptr, int header)
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
    if (header) {
        if (info.chunk_len < 4 || info.chunk_len > MAX_HEADER_SIZE)
            return COUCHSTORE_ERROR_CORRUPT;
        info.chunk_len -= 4;    //Header len includes CRC len.
    }
    info.crc32 = ntohl(info.crc32);

    char* buf = malloc(info.chunk_len);
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

int pread_header(tree_file *file, cs_off_t pos, char **ret_ptr)
{
    return pread_bin_internal(file, pos + 1, ret_ptr, 1);
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
    return (int) uncompressed_len;
}

int pread_bin(tree_file *file, cs_off_t pos, char **ret_ptr)
{
    return pread_bin_internal(file, pos, ret_ptr, 0);
}
