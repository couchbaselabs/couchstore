#include "win32/win32.h"

ssize_t pread(int fd, void *buf, size_t count, off_t offset)
{
    off_t pos = _lseek(fd, offset, SEEK_SET);
    if (pos < 0) {
        return pos;
    }
    return read(fd, buf, count);
}

ssize_t pwrite(int fd, const void *buf, size_t nbytes, off_t offset)
{
    off_t ret = _lseek(fd, offset, SEEK_SET);

    if (ret < 0) {
        return(ret);
    }
    return(_write(fd, buf, nbytes));
}

int fsync(int fd)
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
