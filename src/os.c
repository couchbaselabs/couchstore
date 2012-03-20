/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#include "os.h"

static ssize_t couch_pread(Db *db, void *buf, size_t nbyte, off_t offset)
{
    ssize_t rv;
    do {
        rv = pread(db->fd, buf, nbyte, offset);
    } while (rv == -1 && errno == EINTR);

    return rv;
}

static ssize_t couch_pwrite(Db *db, const void *buf, size_t nbyte, off_t offset)
{
    ssize_t rv;
    do {
        rv = pwrite(db->fd, buf, nbyte, offset);
    } while (rv == -1 && errno == EINTR);

    return rv;
}

static int couch_open(const char *path, int flags, int mode)
{
    int rv;
    do {
        rv = open(path, flags, mode);
    } while (rv == -1 && errno == EINTR);

    return rv;
}

static off_t couch_goto_eof(Db *db)
{
    // TODO:  64-bit clean version.
    return lseek(db->fd, 0, SEEK_END);
}

static int couch_close(Db *db)
{
    int rv;
    do {
        rv = close(db->fd);
    } while (rv == -1 && errno == EINTR);

    if (rv == 0) {
        db->fd = -1;
    }

    return rv;
}

static int couch_sync(Db *db)
{
    int rv;
    do {
        rv = fsync(db->fd);
    } while (rv == -1 && errno == EINTR);

    return rv;
}

static couch_file_ops default_file_ops = {
    couch_pread,
    couch_pwrite,
    couch_open,
    couch_close,
    couch_goto_eof,
    couch_sync
};

couch_file_ops *couch_get_default_file_ops(void)
{
    return &default_file_ops;
}
