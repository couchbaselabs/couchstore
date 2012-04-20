/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <stdlib.h>
#include <string.h>
#include "reduces.h"
#include "bitfield.h"

void by_seq_reduce (char *dst, size_t *size_r, nodelist *leaflist, int count)
{
    (void)leaflist;
    memset(dst, 0, 5);
    set_bits(dst, 0, 40, count);
    *size_r = 5;
}

void by_seq_rereduce (char *dst, size_t *size_r, nodelist *ptrlist, int count)
{
    (void)count;
    uint64_t total = 0;
    nodelist *i = ptrlist;
    while (i != NULL) {
        total += get_40(i->pointer->reduce_value.buf);
        i = i->next;
    }
    memset(dst, 0, 5);
    set_bits(dst, 0, 40, total);
    *size_r = 5;
}

void by_id_reduce(char *dst, size_t *size_r, nodelist *leaflist, int count)
{
    (void)count;
    uint64_t notdeleted = 0, deleted = 0, size = 0;
    nodelist *i = leaflist;
    while (i != NULL) {
        if ((i->data.buf[10] & 0x80) != 0) {
            deleted++;
        } else {
            notdeleted++;
        }
        size += get_32(i->data.buf + 6);

        i = i->next;
    }

    memset(dst, 0, 16);
    set_bits(dst, 0, 40, notdeleted);
    set_bits(dst + 5, 0, 40, deleted);
    set_bits(dst + 10, 0, 48, size);
    *size_r = 16;
}

void by_id_rereduce(char *dst, size_t *size_r, nodelist *ptrlist, int count)
{
    (void)count;
    uint64_t notdeleted = 0, deleted = 0, size = 0;

    nodelist *i = ptrlist;
    while (i != NULL) {
        notdeleted += get_40(i->pointer->reduce_value.buf);
        deleted += get_40(i->pointer->reduce_value.buf + 5);
        size += get_48(i->pointer->reduce_value.buf + 10);

        i = i->next;
    }

    memset(dst, 0, 16);
    set_bits(dst, 0, 40, notdeleted);
    set_bits(dst + 5, 0, 40, deleted);
    set_bits(dst + 10, 0, 48, size);
    *size_r = 16;
}
