/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <assert.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#include "internal.h"

#undef LOG_IO
#ifdef LOG_IO
#include <stdio.h>
#endif

static inline void save_errno(void) {
    get_os_error_store()->errno_err = errno;
}

static inline int handle_to_fd(couch_file_handle handle)
{
    return (int)(intptr_t)handle;
}

static inline couch_file_handle fd_to_handle(int fd)
{
    return (couch_file_handle)(intptr_t)fd;
}

static ssize_t couch_pread(couch_file_handle handle, void *buf, size_t nbyte, cs_off_t offset)
{
#ifdef LOG_IO
    fprintf(stderr, "PREAD  %8llx -- %8llx  (%6.1f kbytes)\n", offset, offset+nbyte, nbyte/1024.0);
#endif
    int fd = handle_to_fd(handle);
    ssize_t rv;
    do {
        rv = pread(fd, buf, nbyte, offset);
    } while (rv == -1 && errno == EINTR);

    if(rv < 0) {
        save_errno();
        return (ssize_t) COUCHSTORE_ERROR_READ;
    }
    return rv;
}

static ssize_t couch_pwrite(couch_file_handle handle, const void *buf, size_t nbyte, cs_off_t offset)
{
#ifdef LOG_IO
    fprintf(stderr, "PWRITE %8llx -- %8llx  (%6.1f kbytes)\n", offset, offset+nbyte, nbyte/1024.0);
#endif
    int fd = handle_to_fd(handle);
    ssize_t rv;
    do {
        rv = pwrite(fd, buf, nbyte, offset);
    } while (rv == -1 && errno == EINTR);

    if(rv < 0) {
        save_errno();
        return (ssize_t) COUCHSTORE_ERROR_WRITE;
    }
    return rv;
}

static couchstore_error_t couch_open(couch_file_handle* handle, const char *path, int oflag)
{
    int fd;
    do {
        fd = open(path, oflag | O_LARGEFILE, 0666);
    } while (fd == -1 && errno == EINTR);

    if (fd < 0) {
        if (errno == ENOENT) {
            return COUCHSTORE_ERROR_NO_SUCH_FILE;
        } else {
            return COUCHSTORE_ERROR_OPEN_FILE;
        }
    }
    // Tell the caller about the new handle (file descriptor)
    *handle = fd_to_handle(fd);
    return COUCHSTORE_SUCCESS;
}

static void couch_close(couch_file_handle handle)
{
    int fd = handle_to_fd(handle);
    int rv;

    if (fd != -1) {
        do {
            assert(fd >= 3);
            rv = close(fd);
        } while (rv == -1 && errno == EINTR);
    }
    if(rv < 0) {
        save_errno();
    }
}

static cs_off_t couch_goto_eof(couch_file_handle handle)
{
    int fd = handle_to_fd(handle);
    cs_off_t rv = lseek(fd, 0, SEEK_END);
    if(rv < 0) {
        save_errno();
    }
    return rv;
}


static couchstore_error_t couch_sync(couch_file_handle handle)
{
    int fd = handle_to_fd(handle);
    int rv;
    do {
        rv = fdatasync(fd);
    } while (rv == -1 && errno == EINTR);

    if (rv == -1) {
        save_errno();
        return COUCHSTORE_ERROR_WRITE;
    }

    return COUCHSTORE_SUCCESS;
}

static couch_file_handle couch_constructor(void* cookie)
{
    (void) cookie;
    // We don't have a file descriptor till couch_open runs, so return an invalid value for now.
    return fd_to_handle(-1);
}

static void couch_destructor(couch_file_handle handle)
{
    // nothing to do here
    (void)handle;
}

static const couch_file_ops default_file_ops = {
    (uint64_t)3,
    couch_constructor,
    couch_open,
    couch_close,
    couch_pread,
    couch_pwrite,
    couch_goto_eof,
    couch_sync,
    couch_destructor,
    NULL
};

LIBCOUCHSTORE_API
const couch_file_ops *couchstore_get_default_file_ops(void)
{
    return &default_file_ops;
}
