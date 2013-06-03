#ifndef COUCHSTORE_REDUCES_H
#define COUCHSTORE_REDUCES_H 1

#include <libcouchstore/couch_common.h>
#include <libcouchstore/couch_db.h>
#include "couch_btree.h"
#include "bitfield.h"

typedef struct {
    raw_40 count;
} raw_by_seq_reduce;

typedef struct {
    raw_40 notdeleted;
    raw_40 deleted;
    raw_48 size;
} raw_by_id_reduce;

#ifdef __cplusplus
extern "C" {
#endif

    couchstore_error_t by_seq_reduce(char *dst, size_t *size_r, const nodelist *leaflist, int count, void *ctx);
    couchstore_error_t by_seq_rereduce(char *dst, size_t *size_r, const nodelist *leaflist, int count, void *ctx);

    couchstore_error_t by_id_rereduce(char *dst, size_t *size_r, const nodelist *leaflist, int count, void *ctx);
    couchstore_error_t by_id_reduce(char *dst, size_t *size_r, const nodelist *leaflist, int count, void *ctx);

#ifdef __cplusplus
}
#endif

#endif
