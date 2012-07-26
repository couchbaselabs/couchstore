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

/* Do special cases for windows */
#ifndef _O_BINARY
#define open_int open
#define close_int close
#else
#include <io.h>
#include <share.h>

static int win_open(const char* filename, int oflag, int pmode) {
    int creationflag = OPEN_EXISTING;
    if(oflag & O_CREAT) {
        creationflag = OPEN_ALWAYS;
    }

    HANDLE os_handle = CreateFileA(filename, GENERIC_READ | GENERIC_WRITE,
                                   FILE_SHARE_DELETE | FILE_SHARE_WRITE | FILE_SHARE_READ,
                                   NULL, creationflag, 0, NULL);

    if(os_handle == INVALID_HANDLE_VALUE) {
        return -1;
    }

    int fd = _open_osfhandle(os_handle, oflag | _O_BINARY);
    if(fd < 0) {
        CloseHandle(os_handle);
    }

    return fd;
}

static int win_close(int fd) {
    int os_handle = _get_osfhandle(fd);
    int close_result = close(fd);
    CloseHandle(os_handle);
    return close_result;

}
#define open_int win_open
#define close_int win_close
#endif

static inline int handle_to_fd(couch_file_handle handle)
{
    return (int)(intptr_t)handle;
}

static inline couch_file_handle fd_to_handle(int fd)
{
    return (couch_file_handle)(intptr_t)fd;
}

static ssize_t couch_pread(couch_file_handle handle, void *buf, size_t nbyte, off_t offset)
{
#ifdef LOG_IO
    fprintf(stderr, "PREAD  %8llx -- %8llx  (%6.1f kbytes)\n", offset, offset+nbyte, nbyte/1024.0);
#endif
    int fd = handle_to_fd(handle);
    ssize_t rv;
    do {
        rv = pread(fd, buf, nbyte, offset);
    } while (rv == -1 && errno == EINTR);

    if(rv < 0) return (ssize_t) COUCHSTORE_ERROR_READ;
    return rv;
}

static ssize_t couch_pwrite(couch_file_handle handle, const void *buf, size_t nbyte, off_t offset)
{
#ifdef LOG_IO
    fprintf(stderr, "PWRITE %8llx -- %8llx  (%6.1f kbytes)\n", offset, offset+nbyte, nbyte/1024.0);
#endif
    int fd = handle_to_fd(handle);
    ssize_t rv;
    do {
        rv = pwrite(fd, buf, nbyte, offset);
    } while (rv == -1 && errno == EINTR);

    if(rv < 0) return (ssize_t) COUCHSTORE_ERROR_WRITE;
    return rv;
}

static couchstore_error_t couch_open(couch_file_handle* handle, const char *path, int oflag)
{
    int fd;
    do {
        fd = open_int(path, oflag | O_LARGEFILE, 0666);
    } while (fd == -1 && errno == EINTR);

    if (fd == -1) {
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
            rv = close_int(fd);
        } while (rv == -1 && errno == EINTR);
    }
}

static off_t couch_goto_eof(couch_file_handle handle)
{
    int fd = handle_to_fd(handle);
    return lseek(fd, 0, SEEK_END);
}


static couchstore_error_t couch_sync(couch_file_handle handle)
{
    int fd = handle_to_fd(handle);
    int rv;
    do {
        rv = fdatasync(fd);
    } while (rv == -1 && errno == EINTR);

    if (rv == -1) {
        return COUCHSTORE_ERROR_WRITE;
    }

    return COUCHSTORE_SUCCESS;
}

static couch_file_handle couch_constructor(void)
{
    // We don't have a file descriptor till couch_open runs, so return an invalid value for now.
    return fd_to_handle(-1);
}

static void couch_destructor(couch_file_handle handle)
{
    // nothing to do here
    (void)handle;
}

static const couch_file_ops default_file_ops = {
    (uint64_t)2,
    couch_constructor,
    couch_open,
    couch_close,
    couch_pread,
    couch_pwrite,
    couch_goto_eof,
    couch_sync,
    couch_destructor
};

const couch_file_ops *couch_get_default_file_ops(void)
{
    return &default_file_ops;
}
