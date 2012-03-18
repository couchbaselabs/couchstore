#ifndef COUCHSTORE_REDUCES_H
#define COUCHSTORE_REDUCES_H 1

#include <libcouchstore/couch_common.h>
#include <libcouchstore/couch_btree.h>

#ifdef __cplusplus
extern "C" {
#endif

void by_seq_reduce (sized_buf *dst, nodelist *leaflist, int count);
void by_seq_rereduce (sized_buf *dst, nodelist *leaflist, int count);

void by_id_reduce(sized_buf *dst, nodelist *leaflist, int count);
void by_id_rereduce(sized_buf *dst, nodelist *leaflist, int count);

#ifdef __cplusplus
}
#endif

#endif
