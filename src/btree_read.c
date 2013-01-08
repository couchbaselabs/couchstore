/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <stdlib.h>
#include "couch_btree.h"
#include "util.h"
#include "node_types.h"

static couchstore_error_t btree_lookup_inner(couchfile_lookup_request *rq,
                                             uint64_t diskpos,
                                             int current,
                                             int end)
{
    int bufpos = 1, nodebuflen = 0;

    if (current == end) {
        return 0;
    }
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;

    char *nodebuf = NULL;

    nodebuflen = pread_compressed(rq->db, diskpos, &nodebuf);
    error_unless(nodebuflen >= 0, nodebuflen);  // if negative, it's an error code

    if (nodebuf[0] == 0) { //KP Node
        while (bufpos < nodebuflen && current < end) {
            sized_buf cmp_key, val_buf;
            bufpos += read_kv(nodebuf + bufpos, &cmp_key, &val_buf);

            if (rq->cmp.compare(&cmp_key, rq->keys[current]) >= 0) {
                if (rq->fold) {
                    rq->in_fold = 1;
                }

                uint64_t pointer = 0;
                int last_item = current;
                //Descend into the pointed to node.
                //with all keys < item key.
                do {
                    last_item++;
                } while (last_item < end && rq->cmp.compare(&cmp_key, rq->keys[last_item]) >= 0);

                const raw_node_pointer *raw = (const raw_node_pointer*)val_buf.buf;
                if(rq->node_callback) {
                    uint64_t subtreeSize = decode_raw48(raw->subtreesize);
                    sized_buf reduce_value =
                    {val_buf.buf + sizeof(raw_node_pointer), decode_raw16(raw->reduce_value_size)};
                    error_pass(rq->node_callback(rq, subtreeSize, &reduce_value));
                }

                pointer = decode_raw48(raw->pointer);
                error_pass(btree_lookup_inner(rq, pointer, current, last_item));
                if (!rq->in_fold) {
                    current = last_item;
                }
                if(rq->node_callback) {
                    error_pass(rq->node_callback(rq, 0, NULL));
                }
            }
        }
    } else if (nodebuf[0] == 1) { //KV Node
        while (bufpos < nodebuflen && current < end) {
            sized_buf cmp_key, val_buf;
            bufpos += read_kv(nodebuf + bufpos, &cmp_key, &val_buf);
            int cmp_val = rq->cmp.compare(&cmp_key, rq->keys[current]);
            if (cmp_val >= 0 && rq->fold && !rq->in_fold) {
                rq->in_fold = 1;
            } else if (rq->in_fold && (current + 1) < end &&
                       (rq->cmp.compare(&cmp_key, rq->keys[current + 1])) > 0) {
                //We've hit a key past the end of our range.
                rq->in_fold = 0;
                rq->fold = 0;
                current = end;
            }

            if (cmp_val == 0 || (cmp_val > 0 && rq->in_fold)) {
                //Found
                error_pass(rq->fetch_callback(rq, &cmp_key, &val_buf));
                if (!rq->in_fold) {
                    current++;
                }
            }
        }
    }

    //Any remaining items are not found.
    while (current < end && !rq->fold) {
        error_pass(rq->fetch_callback(rq, rq->keys[current], NULL));
        current++;
    }

cleanup:
    free(nodebuf);

    return errcode;
}

couchstore_error_t btree_lookup(couchfile_lookup_request *rq,
                                uint64_t root_pointer)
{
    rq->in_fold = 0;
    return btree_lookup_inner(rq, root_pointer, 0, rq->num_keys);
}

