#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <netinet/in.h>
#include "snappy-c.h"
#include "ei.h"
#include "util.h"

#define SIZE_BLOCK 4096

ssize_t total_read_len(off_t blockoffset, ssize_t finallen)
{
    ssize_t left;
    ssize_t add = 0;
    if(blockoffset == 0)
    {
        add++;
        blockoffset = 1;
    }

    left = SIZE_BLOCK - blockoffset;
    if(left >= finallen)
    {
        return finallen + add;
    }
    else
    {
        if((finallen - left) % (SIZE_BLOCK - 1) != 0)
        {
            add++;
        }
        return finallen + add + ((finallen - left) / (SIZE_BLOCK - 1));
    }
}

int remove_block_prefixes(char* buf, off_t offset, ssize_t len)
{
    off_t buf_pos = 0;
    off_t gap = 0;
    ssize_t remain_block;
    while(buf_pos + gap < len)
    {
        remain_block = SIZE_BLOCK - offset;

        if(offset == 0) gap++;

        if(remain_block > (len - gap - buf_pos))
        {
            remain_block = len - gap - buf_pos;
        }

        if(offset == 0)
        {
            //printf("Move %d bytes <-- by %d, landing at %d\r\n", remain_block, gap, buf_pos);
            memmove(buf + buf_pos, buf + buf_pos + gap, remain_block);
            offset = 1;
        }
        else
        {
            buf_pos += remain_block;
            offset = 0;
        }
    }
    return len - gap;
}

// Sets *dst to returned buffer, returns end size.
// Increases pos by read len.
int raw_read(int fd, off_t *pos, ssize_t len, char** dst)
{
    off_t blockoffs = *pos % SIZE_BLOCK;
    ssize_t total = total_read_len(blockoffs, len);
    *dst = (char*) malloc(total);
    if(!*dst)
        return -1;
    ssize_t got_bytes = pread(fd, *dst, total, *pos);
    if(got_bytes <= 0)
        goto fail;

    *pos += got_bytes;
    return remove_block_prefixes(*dst, blockoffs, got_bytes);

fail:
    free(*dst);
    *dst = NULL;
    return -1;
}

int pread_bin_int(int fd, off_t pos, char **ret_ptr, int header)
{
   char *bufptr = NULL, *bufptr_rest = NULL, *newbufptr = NULL;
   char prefix;
   int buf_len;
   uint32_t chunk_len, crc32 = 0;
   int skip = 0;
   int errcode = 0;
   buf_len = raw_read(fd, &pos, 2*SIZE_BLOCK - (pos % SIZE_BLOCK), &bufptr);
   if(buf_len == -1)
   {
       buf_len = raw_read(fd, &pos, 4, &bufptr);
       error_unless(buf_len > 0, ERROR_READ);
   }

   prefix = bufptr[0] & 0x80;

   memcpy(&chunk_len, bufptr, 4);
   chunk_len = ntohl(chunk_len) & ~0x80000000;
   skip += 4;
   if(header)
   {
       chunk_len -= 16; //Header len includes hash len.
       skip += 16; //Header is still md5, and the md5 is always present.
   }
   else if(prefix)
   {
       memcpy(&crc32, bufptr + 4, 4);
       crc32 = ntohl(crc32);
       skip += 4;
   }

   buf_len -= skip;
   memmove(bufptr, bufptr+skip, buf_len);
   if(chunk_len <= buf_len)
   {
       newbufptr = (char*) realloc(bufptr, chunk_len);
       error_unless(newbufptr, ERROR_READ);
       bufptr = newbufptr;

       if(crc32) {
           error_unless((crc32) == hash_crc32(bufptr, chunk_len), ERROR_CHECKSUM_FAIL);
       }
       *ret_ptr = bufptr;
       return chunk_len;
   }
   else
   {
       int rest_len = raw_read(fd, &pos, chunk_len - buf_len, &bufptr_rest);
       error_unless(rest_len > 0, ERROR_READ);

       newbufptr = (char*) realloc(bufptr, buf_len + rest_len);
       error_unless(newbufptr, ERROR_READ);
       bufptr = newbufptr;

       memcpy(bufptr + buf_len, bufptr_rest, rest_len);
       free(bufptr_rest);
       if(crc32) {
           error_unless((crc32) == hash_crc32(bufptr, chunk_len), ERROR_CHECKSUM_FAIL);
       }
       *ret_ptr = bufptr;
       return chunk_len;
   }

cleanup:
   if(errcode < 0)
   {
       if(bufptr)
           free(bufptr);
   }
   return errcode;
}

int pread_header(int fd, off_t pos, char **ret_ptr)
{
    return pread_bin_int(fd, pos + 1, ret_ptr, 1);
}

int pread_compressed(int fd, off_t pos, char **ret_ptr)
{
    char *new_buf;
    int len = pread_bin_int(fd, pos, ret_ptr, 0);
    if(len < 0)
    {
        return len;
    }
    size_t new_len;
    if(snappy_uncompressed_length((*ret_ptr), len, &new_len)
            != SNAPPY_OK)
    {
        //should be compressed but snappy doesn't see it as valid.
        free(*ret_ptr);
        return -1;
    }

    new_buf = (char*) malloc(new_len);
    if(!new_buf)
    {
        free(*ret_ptr);
        return -1;
    }
    snappy_status ss = (snappy_uncompress((*ret_ptr), len, new_buf, &new_len));
    if(ss == SNAPPY_OK)
    {
        free(*ret_ptr);
        *ret_ptr = new_buf;
        return new_len;
    }
    else
    {
        free(*ret_ptr);
        return -1;
    }
}

int pread_bin(int fd, off_t pos, char **ret_ptr)
{
    return pread_bin_int(fd, pos, ret_ptr, 0);
}
