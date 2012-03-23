/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <stdlib.h>
#include "couch_btree.h"
#include "util.h"

static couchstore_error_t btree_lookup_inner(couchfile_lookup_request *rq,
                                             uint64_t diskpos,
                                             int current,
                                             int end)
{
    int bufpos = 0;
    int type_pos;
    int list_size = 0;
    sized_buf v;

    if (current == end) {
        return 0;
    }
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;

    char *nodebuf = NULL;

    int nodebuflen = pread_compressed(rq->db, diskpos, &nodebuf);
    error_unless(nodebuflen > 0, COUCHSTORE_ERROR_READ);

    bufpos++; //Skip over term version
    error_unless(tuple_check(nodebuf, &bufpos, 2), COUCHSTORE_ERROR_PARSE_TERM);
    type_pos = bufpos;
    ei_skip_term(nodebuf, &bufpos); //node type
    error_nonzero(ei_decode_list_header(nodebuf, &bufpos, &list_size),
                  COUCHSTORE_ERROR_PARSE_TERM);

    if (atom_check(nodebuf + type_pos, "kp_node")) {
        int list_item = 0;
        while (list_item < list_size && current < end) {
            //{K,P}
            error_unless(tuple_check(nodebuf, &bufpos, 2),
                         COUCHSTORE_ERROR_PARSE_TERM);
            void *cmp_key = rq->cmp.from_ext(&rq->cmp, nodebuf, bufpos);
            ei_skip_term(nodebuf, &bufpos); //Skip key

            if (rq->cmp.compare(cmp_key, rq->keys[current]) >= 0) {
                if (rq->fold) {
                    rq->in_fold = 1;
                }
                uint64_t pointer = 0;
                int last_item = current;
                //Descend into the pointed to node.
                //with all keys < item key.
                do {
                    last_item++;
                } while (last_item < end &&
                         rq->cmp.compare(cmp_key, rq->keys[last_item]) >= 0);

                error_unless(tuple_check(nodebuf, &bufpos, 3), COUCHSTORE_ERROR_PARSE_TERM);
                ei_decode_uint64(nodebuf, &bufpos, &pointer);
                ei_skip_term(nodebuf, &bufpos); //Skip reduce
                ei_skip_term(nodebuf, &bufpos); //Skip subtreesize
                error_pass(btree_lookup_inner(rq, pointer, current, last_item));
                if (!rq->in_fold) {
                    current = last_item + 1;
                }
            } else {
                ei_skip_term(nodebuf, &bufpos); //Skip pointer
            }
            list_item++;
        }
    } else if (atom_check(nodebuf + type_pos, "kv_node")) {
        int list_item = 0;
        while (list_item < list_size && current < end) {
            int cmp_val, keypos;
            sized_buf key_term;
            //{K,V}
            error_unless(tuple_check(nodebuf, &bufpos, 2), COUCHSTORE_ERROR_PARSE_TERM);
            void *cmp_key = rq->cmp.from_ext(&rq->cmp, nodebuf, bufpos);
            keypos = bufpos;
            ei_skip_term(nodebuf, &bufpos); //Skip key
            cmp_val = rq->cmp.compare(cmp_key, rq->keys[current]);
            if (cmp_val >= 0 && rq->fold && !rq->in_fold) {
                rq->in_fold = 1;
            } else if (rq->in_fold && (current + 1) < end &&
                       (rq->cmp.compare(cmp_key, rq->keys[current + 1])) > 0) {
                //We've hit a key past the end of our range.
                rq->in_fold = 0;
                rq->fold = 0;
                current = end;
            }

            if (cmp_val == 0 || (cmp_val > 0 && rq->in_fold)) {
                //Found
                term_to_buf(&key_term, nodebuf, &keypos);
                term_to_buf(&v, nodebuf, &bufpos); //Read value
                error_pass(rq->fetch_callback(rq, &key_term, &v));
                if (!rq->in_fold) {
                    current++;
                }
            } else {
                ei_skip_term(nodebuf, &bufpos);    //Skip value
            }
            list_item++;
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

