#include "couch_common.h"
#include "couch_btree.h"

void by_seq_reduce (sized_buf* dst, nodelist* leaflist, int count);
void by_seq_rereduce (sized_buf* dst, nodelist* leaflist, int count);

void by_id_reduce(sized_buf *dst, nodelist* leaflist, int count);
void by_id_rereduce(sized_buf *dst, nodelist* leaflist, int count);
