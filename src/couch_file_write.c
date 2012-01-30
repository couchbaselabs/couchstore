#include <stdio.h>
#include <stdint.h>
#include <sys/types.h>
#include <netinet/in.h>

#include "couch_db.h"

#include "rfc1321/global.h"
#include "rfc1321/md5.h"

ssize_t raw_write(int fd, sized_buf* buf, off_t pos)
{
    off_t write_pos = pos;
    off_t buf_pos = 0;
    char blockprefix = 0;
    ssize_t written;
    size_t block_remain;
    while(buf_pos < buf->size)
    {
        block_remain = SIZE_BLOCK - (write_pos % SIZE_BLOCK);
        if(block_remain > (buf->size - buf_pos))
            block_remain = buf->size - buf_pos;

        if(write_pos % SIZE_BLOCK == 0)
        {
            written = pwrite(fd, &blockprefix, 1, write_pos);
            if(written < 0) return ERROR_WRITE;
            write_pos += 1;
            continue;
        }

        written = pwrite(fd, buf->buf + buf_pos, block_remain, write_pos);
        if(written < 0) return ERROR_WRITE;
        buf_pos += written;
        write_pos += written;
    }

    return write_pos - pos;
}

int db_write_header(Db* db, sized_buf* buf)
{
    off_t write_pos = db->file_pos;
    ssize_t written;
    char blockheader = 1;
    uint32_t size = htonl(buf->size + 16); //Len before header includes hash len.
    sized_buf lenbuf = { (char*) &size, 4 };
    MD5_CTX hashctx;
    char hash[16];
    sized_buf hashbuf = { hash, 16 };

    if(write_pos % SIZE_BLOCK != 0)
        write_pos += SIZE_BLOCK - (write_pos % SIZE_BLOCK); //Move to next block boundary.

    written = pwrite(db->fd, &blockheader, 1, write_pos);
    if(written < 0) return ERROR_WRITE;
    write_pos += written;

    //Write length
    written = raw_write(db->fd, &lenbuf, write_pos);
    if(written < 0) return ERROR_WRITE;
    write_pos += written;

    //Write MD5
    MD5Init(&hashctx);
    MD5Update(&hashctx, buf->buf, buf->size);
    MD5Final(hash, &hashctx);

    written = raw_write(db->fd, &hashbuf, write_pos);
    if(written < 0) return ERROR_WRITE;
    write_pos += written;

    //Write actual header
    written = raw_write(db->fd, buf, write_pos);
    if(written < 0) return ERROR_WRITE;
    write_pos += written;
    db->file_pos = write_pos;
    return 0;
}

int db_write_buf(Db* db, sized_buf* buf, off_t *pos)
{
    off_t write_pos = db->file_pos;
    off_t end_pos = write_pos;
    ssize_t written;
    uint32_t size = htonl(buf->size);
    sized_buf lenbuf = { (char*) &size, 4 };

    written = raw_write(db->fd, &lenbuf, end_pos);
    if(written < 0) return ERROR_WRITE;
    end_pos += written;

    written = raw_write(db->fd, buf, end_pos);
    if(written < 0) return ERROR_WRITE;
    end_pos += written;

    if(pos)
        *pos = write_pos;

    db->file_pos = end_pos;
    return 0;
}
