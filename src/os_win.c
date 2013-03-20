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

#include <io.h>
#include <share.h>
#include <assert.h>

static inline DWORD save_windows_error() {
    DWORD err = GetLastError();
    get_os_error_store()->win_err = err;
    get_os_error_store()->errno_err = 0;
    return err;
}

static inline HANDLE handle_to_win(couch_file_handle handle)
{
    return (HANDLE)(intptr_t)handle;
}

static inline couch_file_handle win_to_handle(HANDLE hdl)
{
    return (couch_file_handle)(intptr_t)hdl;
}

static ssize_t couch_pread(couch_file_handle handle, void *buf, size_t nbyte, cs_off_t offset)
{
#ifdef LOG_IO
    fprintf(stderr, "PREAD  %8llx -- %8llx  (%6.1f kbytes)\n", offset, offset+nbyte, nbyte/1024.0);
#endif
    HANDLE file = handle_to_win(handle);
    BOOL rv;
    DWORD bytesread;
    OVERLAPPED winoffs;
    memset(&winoffs, 0, sizeof(winoffs));
    winoffs.Offset = offset & 0xFFFFFFFF;
    winoffs.OffsetHigh = offset >> 32;
    rv = ReadFile(file, buf, nbyte, &bytesread, &winoffs);
    if(!rv) {
        return (ssize_t) COUCHSTORE_ERROR_READ;
    }
    return bytesread;
}

static ssize_t couch_pwrite(couch_file_handle handle, const void *buf, size_t nbyte, cs_off_t offset)
{
#ifdef LOG_IO
    fprintf(stderr, "PWRITE %8llx -- %8llx  (%6.1f kbytes)\n", offset, offset+nbyte, nbyte/1024.0);
#endif
    HANDLE file = handle_to_win(handle);
    BOOL rv;
    DWORD byteswritten;
    OVERLAPPED winoffs;
    memset(&winoffs, 0, sizeof(winoffs));
    winoffs.Offset = offset & 0xFFFFFFFF;
    winoffs.OffsetHigh = offset >> 32;
    rv = WriteFile(file, buf, nbyte, &byteswritten, &winoffs);
    if(!rv) {
        save_windows_error();
        return (ssize_t) COUCHSTORE_ERROR_WRITE;
    }
    return byteswritten;
}

static couchstore_error_t couch_open(couch_file_handle* handle, const char *path, int oflag)
{
    int creationflag = OPEN_EXISTING;
    if(oflag & O_CREAT) {
        creationflag = OPEN_ALWAYS;
    }

    HANDLE os_handle = CreateFileA(path, GENERIC_READ | GENERIC_WRITE,
                                   FILE_SHARE_DELETE | FILE_SHARE_WRITE | FILE_SHARE_READ,
                                   NULL, creationflag, 0, NULL);

    if(os_handle == INVALID_HANDLE_VALUE) {
        if(save_windows_error() == ERROR_FILE_NOT_FOUND) {
            return (ssize_t) COUCHSTORE_ERROR_NO_SUCH_FILE;
        };
        return COUCHSTORE_ERROR_OPEN_FILE;
    }
    // Tell the caller about the new handle (file descriptor)
    *handle = win_to_handle(os_handle);
    return COUCHSTORE_SUCCESS;
}

static void couch_close(couch_file_handle handle)
{
    HANDLE file = handle_to_win(handle);
    CloseHandle(handle);
}

static cs_off_t couch_goto_eof(couch_file_handle handle)
{
    HANDLE file = handle_to_win(handle);
    LARGE_INTEGER size;
    if(!GetFileSizeEx(file, &size)) {
        save_windows_error();
        return (cs_off_t) COUCHSTORE_ERROR_READ;
    }
    return size.QuadPart;
}


static couchstore_error_t couch_sync(couch_file_handle handle)
{
    HANDLE file = handle_to_win(handle);

    if (!FlushFileBuffers(file)) {
        save_windows_error();
        return COUCHSTORE_ERROR_WRITE;
    }

    return COUCHSTORE_SUCCESS;
}

static couch_file_handle couch_constructor(void* cookie)
{
    (void) cookie;
    // We don't have a file descriptor till couch_open runs, so return an invalid value for now.
    return handle_to_win(INVALID_HANDLE_VALUE);
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
