#ifndef COUCHSTORE_REDUCES_H
#define COUCHSTORE_REDUCES_H 1

#include <libcouchstore/couch_common.h>
#include "couch_btree.h"

#ifdef __cplusplus
extern "C" {
#endif

    void by_seq_reduce(char *dst, size_t *size_r, nodelist *leaflist, int count);
    void by_seq_rereduce(char *dst, size_t *size_r, nodelist *leaflist, int count);

    void by_id_rereduce(char *dst, size_t *size_r, nodelist *leaflist, int count);
    void by_id_reduce(char *dst, size_t *size_r, nodelist *leaflist, int count);

#ifdef __cplusplus
}
#endif

#endif
