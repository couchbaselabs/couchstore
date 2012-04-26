//
//  node_types.c
//  couchstore
//
//  Created by Jens Alfke on 4/25/12.
//  Copyright (c) 2012 Couchbase, Inc. All rights reserved.
//

#include "node_types.h"
#include <stdlib.h>

size_t read_kv(const void *buf, sized_buf *key, sized_buf *value)
{
    const raw_kv_length* kvlen = buf;
    uint32_t klen, vlen;
    decode_kv_length(kvlen, &klen, &vlen);
    key->size = klen;
    key->buf = (char*)(kvlen + 1);
    value->size = vlen;
    value->buf = key->buf + klen;
    return sizeof(raw_kv_length) + klen + vlen;
}

void* write_kv(void *buf, sized_buf key, sized_buf value)
{
    uint8_t *dst = buf;
    *(raw_kv_length*)dst = encode_kv_length((uint32_t)key.size, (uint32_t)value.size);
    dst += sizeof(raw_kv_length);
    memcpy(dst, key.buf, key.size);
    dst += key.size;
    memcpy(dst, value.buf, value.size);
    dst += value.size;
    return dst;
}

node_pointer *read_root(void *buf, int size)
{
    raw_btree_root *raw = (raw_btree_root*)buf;
    node_pointer *ptr;
    uint64_t position = decode_raw48(raw->pointer);
    uint64_t subtreesize = decode_raw48(raw->subtreesize);
    int redsize = size - sizeof(*raw);
    
    ptr = (node_pointer *) malloc(sizeof(node_pointer) + redsize);
    buf = (char *) memcpy(ptr + 1, raw + 1, redsize);
    ptr->key.buf = NULL;
    ptr->key.size = 0;
    ptr->pointer = position;
    ptr->subtreesize = subtreesize;
    ptr->reduce_value.buf = buf;
    ptr->reduce_value.size = redsize;
    return ptr;
}

size_t encode_root(void *buf, node_pointer *node)
{
    if (!node) {
        return 0;
    }
    if (buf) {
        raw_btree_root *root = buf;
        root->pointer = encode_raw48(node->pointer);
        root->subtreesize = encode_raw48(node->subtreesize);
        memcpy(root + 1, node->reduce_value.buf, node->reduce_value.size);
    }
    return sizeof(raw_btree_root) + node->reduce_value.size;
}
