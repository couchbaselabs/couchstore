/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <stdlib.h>
#include <string.h>
#include "reduces.h"
#include "node_types.h"


void by_seq_reduce (char *dst, size_t *size_r, nodelist *leaflist, int count)
{
    (void)leaflist;
    raw_by_seq_reduce *raw = (raw_by_seq_reduce*)dst;
    raw->count = encode_raw40(count);
    *size_r = sizeof(*raw);
}

void by_seq_rereduce (char *dst, size_t *size_r, nodelist *ptrlist, int count)
{
    uint64_t total = 0;
    nodelist *i = ptrlist;
    while (i != NULL && count > 0) {
        const raw_by_seq_reduce *reduce = (const raw_by_seq_reduce*) i->pointer->reduce_value.buf;
        total += decode_raw40(reduce->count);
        
        i = i->next;
        count--;
    }
    raw_by_seq_reduce *raw = (raw_by_seq_reduce*)dst;
    raw->count = encode_raw40(total);
    *size_r = sizeof(*raw);
}


static size_t encode_by_id_reduce(char *dst, uint64_t notdeleted, uint64_t deleted, uint64_t size)
{
    raw_by_id_reduce *raw = (raw_by_id_reduce*)dst;
    raw->notdeleted = encode_raw40(notdeleted);
    raw->deleted = encode_raw40(deleted);
    raw->size = encode_raw48(size);
    return sizeof(*raw);
}

void by_id_reduce(char *dst, size_t *size_r, nodelist *leaflist, int count)
{
    uint64_t notdeleted = 0, deleted = 0, size = 0;

    nodelist *i = leaflist;
    while (i != NULL && count > 0) {
        const raw_id_index_value *raw = (const raw_id_index_value*)i->data.buf;
        if (decode_raw48(raw->bp) & BP_DELETED_FLAG) {
            deleted++;
        } else {
            notdeleted++;
        }
        size += decode_raw32(raw->size);

        i = i->next;
        count--;
    }

    *size_r = encode_by_id_reduce(dst, notdeleted, deleted, size);
}

void by_id_rereduce(char *dst, size_t *size_r, nodelist *ptrlist, int count)
{
    uint64_t notdeleted = 0, deleted = 0, size = 0;

    nodelist *i = ptrlist;
    while (i != NULL && count > 0) {
        const raw_by_id_reduce *reduce = (const raw_by_id_reduce*) i->pointer->reduce_value.buf;
        notdeleted += decode_raw40(reduce->notdeleted);
        deleted += decode_raw40(reduce->deleted);
        size += decode_raw48(reduce->size);

        i = i->next;
        count--;
    }

    *size_r = encode_by_id_reduce(dst, notdeleted, deleted, size);
}
