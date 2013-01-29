//
//  node_types.h
//  couchstore
//
//  Created by Jens Alfke on 4/25/12.
//  Copyright (c) 2012 Couchbase, Inc. All rights reserved.
//

#ifndef couchstore_node_types_h
#define couchstore_node_types_h

#include "bitfield.h"
#include "internal.h"

typedef struct {
    raw_08 version;
    raw_48 update_seq;
    raw_48 purge_seq;
    raw_48 purge_ptr;
    raw_16 seqrootsize;
    raw_16 idrootsize;
    raw_16 localrootsize;
    // Three variable-size raw_btree_root structures follow
} raw_file_header;

typedef struct {
    raw_48 pointer;
    raw_48 subtreesize;
    // Variable-size reduce value follows
} raw_btree_root;

/** Packed key-and-value length type. Key length is 12 bits, value length is 28. */
typedef struct {
    uint8_t raw_kv[5];
} raw_kv_length;

typedef struct {
    raw_48 pointer;
    raw_48 subtreesize;
    raw_16 reduce_value_size;
    // Variable-size reduce value follows
} raw_node_pointer;

typedef struct {
    raw_48 sequence;
} raw_by_seq_key;

typedef struct {
    raw_48 db_seq;
    raw_32 size;
    raw_48 bp;                 // high bit is 'deleted' flag
    raw_48 rev_seq;
    raw_08 content_meta;
    // Variable-size rev_meta data follows
} raw_id_index_value;

typedef struct {
    raw_kv_length sizes;
    raw_48 bp;                 // high bit is 'deleted' flag
    raw_48 rev_seq;
    raw_08 content_meta;
    // Variable-size id follows
    // Variable-size rev_meta data follows
} raw_seq_index_value;

// Mask for the 'deleted' bit in .bp fields
#define BP_DELETED_FLAG 0x800000000000


node_pointer *read_root(void *buf, int size);

size_t encode_root(void *buf, node_pointer *node);


/**
 * Reads a 12-bit key length and 28-bit value length, packed into 5 bytes big-endian.
 */
static inline void decode_kv_length(const raw_kv_length *kv, uint32_t *klen, uint32_t *vlen)
{
    //12, 28 bit
    *klen = (uint16_t) ((kv->raw_kv[0] << 4) | ((kv->raw_kv[1] & 0xf0) >> 4));
    *vlen = ntohl(*(uint32_t*)&kv->raw_kv[1]) & 0x0FFFFFFF;
}

/**
 * Returns an encoded 5-byte key/value length pair.
 */
static inline raw_kv_length encode_kv_length(size_t klen, size_t vlen)
{
    raw_kv_length kv;
    *(uint32_t*)&kv.raw_kv[1] = htonl(vlen);
    kv.raw_kv[0] = (uint8_t)(klen >> 4);    // upper 8 bits of klen
    kv.raw_kv[1] |= (klen & 0xF) << 4;     // lower 4 bits of klen in upper half of byte
    return kv;
}


/**
 * Parses an in-memory buffer containing a 5-byte key/value length followed by key and value data,
 * and fills in sized_bufs to point to the key and data.
 * @return Number of bytes consumed from the buffer
 */
size_t read_kv(const void *buf, sized_buf *key, sized_buf *value);

void* write_kv(void *buf, sized_buf key, sized_buf value);


/**
 * Reads a 48-bit sequence number out of a sized_buf.
 */
static inline uint64_t decode_sequence_key(const sized_buf *buf)
{
    const raw_by_seq_key *key = (const raw_by_seq_key*)buf->buf;
    return decode_raw48(key->sequence);
}

#endif
