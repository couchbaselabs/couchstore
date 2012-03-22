/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#include "internal.h"

LIBCOUCHSTORE_API
void libcouchstore_set_file_ops_cookie(Db *db, void *data)
{
    db->file_ops_cookie = data;
}

LIBCOUCHSTORE_API
void* libcouchstore_get_file_ops_cookie(Db *db)
{
    return db->file_ops_cookie;
}

static ssize_t couch_pread(Db *db, void *buf, size_t nbyte, off_t offset)
{
    intptr_t fd = (intptr_t)libcouchstore_get_file_ops_cookie(db);
    ssize_t rv;
    do {
        rv = pread((int)fd, buf, nbyte, offset);
    } while (rv == -1 && errno == EINTR);

    return rv;
}

static ssize_t couch_pwrite(Db *db, const void *buf, size_t nbyte, off_t offset)
{
    intptr_t fd = (intptr_t)libcouchstore_get_file_ops_cookie(db);
    ssize_t rv;
    do {
        rv = pwrite((int)fd, buf, nbyte, offset);
    } while (rv == -1 && errno == EINTR);

    return rv;
}

static couchstore_error_t couch_open(Db *db, const char *path, int oflag)
{
    int rv;
    intptr_t fd;
    do {
        rv = open(path, oflag, 0666);
    } while (rv == -1 && errno == EINTR);

    if (rv == -1) {
        if (errno == ENOENT) {
            return COUCHSTORE_ERROR_NO_SUCH_FILE;
        } else {
            return COUCHSTORE_ERROR_OPEN_FILE;
        }
    }

    fd = rv;
    libcouchstore_set_file_ops_cookie(db, (void *)fd);
    return COUCHSTORE_SUCCESS;
}

static void couch_close(Db *db)
{
    intptr_t fd = (intptr_t)libcouchstore_get_file_ops_cookie(db);
    int rv;

    if ((int)fd != -1) {
        do {
            rv = close((int)fd);
        } while (rv == -1 && errno == EINTR);
    }

    libcouchstore_set_file_ops_cookie(db, (void *)-1);
}

static off_t couch_goto_eof(Db *db)
{
    intptr_t fd = (intptr_t)libcouchstore_get_file_ops_cookie(db);
    return lseek((int)fd, 0, SEEK_END);
}


static couchstore_error_t couch_sync(Db *db)
{
    intptr_t fd = (intptr_t)libcouchstore_get_file_ops_cookie(db);
    int rv;
    do {
        rv = fsync((int)fd);
    } while (rv == -1 && errno == EINTR);

    if (rv == -1) {
        return COUCHSTORE_ERROR_WRITE;
    }

    return COUCHSTORE_SUCCESS;
}

static void couch_destructor(Db *db)
{
    libcouchstore_set_file_ops_cookie(db, NULL);
}

static couch_file_ops default_file_ops = {
    (uint64_t)1,
    couch_open,
    couch_close,
    couch_pread,
    couch_pwrite,
    couch_goto_eof,
    couch_sync,
    couch_destructor
};

couch_file_ops *couch_get_default_file_ops(void)
{
    return &default_file_ops;
}
