#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <fcntl.h>

#include <libcouchstore/couch_db.h>

ssize_t couch_pread(Db *db, void *buf, size_t nbyte, off_t offset);
ssize_t couch_pwrite(Db *db, const void *buf, size_t nbyte, off_t offset);

int couch_open(const char *path, int oflag, int mode);
int couch_close(Db *db);

off_t couch_goto_eof(Db *db);

int couch_sync(Db *db);
