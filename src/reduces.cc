#include <stdlib.h>
#include <libcouchstore/couch_btree.h>
#include <ei.h>

void by_seq_reduce (sized_buf* dst, nodelist* leaflist, int count)
{
    //will be freed by flush_mr
    dst->buf = (char*) malloc(12);
    if(!dst->buf)
        return;
    int pos = 0;
    ei_encode_long(dst->buf, &pos, count);
    dst->size = pos;
}

void by_seq_rereduce (sized_buf* dst, nodelist* leaflist, int count)
{
    long total = 0;
    long current = 0;
    int r_pos = 0;
    int pos = 0;

    //will be freed by flush_mr
    dst->buf = (char*) malloc(12);
    if(!dst->buf)
        return;
    nodelist* i = leaflist;
    while(i != NULL)
    {
        r_pos = 0;
        ei_decode_long(i->value.pointer->reduce_value.buf, &r_pos, &current);
        total += current;
        i = i->next;
    }
    ei_encode_long(dst->buf, &pos, total);
    dst->size = pos;
}

void by_id_rereduce(sized_buf *dst, nodelist* leaflist, int count)
{
    //Source term {NotDeleted, Deleted, Size}
    //Result term {NotDeleted, Deleted, Size}
    dst->buf = (char*) malloc(30);
    if(!dst->buf)
        return;
    int dstpos = 0;
    int srcpos = 0;
    long notdeleted = 0, deleted = 0, size = 0;
    nodelist* i = leaflist;
    while(i != NULL)
    {
        srcpos = 0;
        long long src_deleted = 0;
        long long src_notdeleted = 0;
        long long src_size = 0;
        ei_decode_tuple_header(i->value.pointer->reduce_value.buf, &srcpos, NULL);
        ei_decode_longlong(i->value.pointer->reduce_value.buf, &srcpos, &src_notdeleted);
        ei_decode_longlong(i->value.pointer->reduce_value.buf, &srcpos, &src_deleted);
        ei_decode_longlong(i->value.pointer->reduce_value.buf, &srcpos, &src_size);
        size += src_size;
        deleted += src_deleted;
        notdeleted += src_notdeleted;
        i = i->next;
    }

    ei_encode_tuple_header(dst->buf, &dstpos, 3);
    ei_encode_longlong(dst->buf, &dstpos, notdeleted);
    ei_encode_longlong(dst->buf, &dstpos, deleted);
    ei_encode_longlong(dst->buf, &dstpos, size);
    dst->size = dstpos;
}

void by_id_reduce(sized_buf *dst, nodelist* leaflist, int count)
{
    //Source term {Key, {Seq, Rev, Bp, Deleted, ContentMeta, Size}}
    //Result term {NotDeleted, Deleted, Size}
    dst->buf = (char*) malloc(30);
    if(!dst->buf)
        return;
    int dstpos = 0;
    int srcpos = 0;
    long notdeleted = 0, deleted = 0, size = 0;
    nodelist* i = leaflist;
    while(i != NULL)
    {
        srcpos = 0;
        long src_deleted = 0;
        long long src_size = 0;
        ei_decode_tuple_header(i->value.leaf->buf, &srcpos, NULL);
        ei_skip_term(i->value.leaf->buf, &srcpos); //skip key
        ei_decode_tuple_header(i->value.leaf->buf, &srcpos, NULL);
        ei_skip_term(i->value.leaf->buf, &srcpos); //skip seq
        ei_skip_term(i->value.leaf->buf, &srcpos); //skip rev
        ei_skip_term(i->value.leaf->buf, &srcpos); //skip bp
        ei_decode_long(i->value.leaf->buf, &srcpos, &src_deleted);
        ei_skip_term(i->value.leaf->buf, &srcpos); //skip ContentMeta
        ei_decode_longlong(i->value.leaf->buf, &srcpos, &src_size);
        if(src_deleted == 1)
            deleted++;
        else
            notdeleted++;

        size += src_size;
        i = i->next;
    }

    ei_encode_tuple_header(dst->buf, &dstpos, 3);
    ei_encode_long(dst->buf, &dstpos, notdeleted);
    ei_encode_long(dst->buf, &dstpos, deleted);
    ei_encode_longlong(dst->buf, &dstpos, size);
    dst->size = dstpos;
}
