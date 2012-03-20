#ifndef COUCHSTORE_OS_H
#define COUCHSTORE_OS_H 1

#include <sys/types.h>
#include <libcouchstore/couch_db.h>

#ifdef __cplusplus
extern "C" {
#endif

    couch_file_ops *couch_get_default_file_ops(void);

#ifdef __cplusplus
}
#endif

#endif
