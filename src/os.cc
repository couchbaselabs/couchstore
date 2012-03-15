#include "os.h"

ssize_t couch_pread(Db *db, void *buf, size_t nbyte, off_t offset)
{
    return pread(db->fd, buf, nbyte, offset);
}

ssize_t couch_pwrite(Db *db, const void *buf, size_t nbyte, off_t offset)
{
    return pwrite(db->fd, buf, nbyte, offset);
}

int couch_open(const char *path, int flags, int mode)
{
    return open(path, flags, mode);
}

off_t couch_goto_eof(Db *db)
{
    // TODO:  64-bit clean version.
    return lseek(db->fd, 0, SEEK_END);
}

int couch_close(Db *db)
{
    return close(db->fd);
}

int couch_sync(Db *db)
{
    return fsync(db->fd);
}
