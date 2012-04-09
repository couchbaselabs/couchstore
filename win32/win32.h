#ifndef WIN32_H
#define WIN32_H 1

#ifndef __WIN32__
#define __WIN32__
#endif
// Including SDKDDKVer.h defines the highest available Windows platform.

// If you wish to build your application for a previous Windows platform, include WinSDKVer.h and
// set the _WIN32_WINNT macro to the platform you wish to support before including SDKDDKVer.h.

#include <SDKDDKVer.h>

#define WIN32_LEAN_AND_MEAN             // Exclude rarely-used stuff from Windows headers
#define MAXPATHLEN 1024
#include <windows.h>
#include <stdio.h>
#include <stdint.h>
#include <io.h>
#include <Winsock2.h>                 // for ntohl

#define inline __inline

#define ssize_t long
#define off_t   long

static inline ssize_t pread(int fd, void *buf, size_t count, off_t offset)
{
    off_t pos = _lseek(fd, offset, SEEK_SET);
    if (pos < 0) {
        return pos;
    }
    return read(fd, buf, count);
}

static inline ssize_t pwrite(int fd, const void *buf, size_t nbytes, off_t offset)
{
    off_t ret = _lseek(fd, offset, SEEK_SET);

    if (ret < 0) {
        return(ret);
    }
    return(_write(fd, buf, nbytes));
}

static inline int fsync(int fd)
{
    HANDLE h = (HANDLE)_get_osfhandle(fd);
    DWORD err;

    if (h == INVALID_HANDLE_VALUE) {
        errno = EBADF;
        return -1;
    }

    if (!FlushFileBuffers(h)) {
        err = GetLastError();
        switch (err) {
        case ERROR_INVALID_HANDLE:
            errno = EINVAL;
            break;
        default:
            errno = EIO;
        }
        return -1;
    }
    return 0;
}
#endif