#ifndef COUCH_BITFIELD_H
#define COUCH_BITFIELD_H

#include "config.h"
#include "internal.h"
#include <string.h>

// Variable-width types. Since these are made out of chars they will be byte-aligned,
// so structs consisting only of these will be packed.

typedef struct {
    uint8_t raw_bytes[1];
} raw_08;

typedef struct {
    uint8_t raw_bytes[2];
} raw_16;

typedef struct {
    uint8_t raw_bytes[4];
} raw_32;

typedef struct {
    uint8_t raw_bytes[5];
} raw_40;

typedef struct {
    uint8_t raw_bytes[6];
} raw_48;

typedef struct {
    uint8_t raw_bytes[8];
} raw_64;

// Functions for decoding raw_xx types to native integers:

static inline uint8_t decode_raw08(raw_08 raw)
{
    return raw.raw_bytes[0];
}

static inline uint16_t decode_raw16(raw_16 raw)
{
    return ntohs(*(uint16_t*)raw.raw_bytes);
}

static inline uint32_t decode_raw32(raw_32 raw)
{
    return ntohl(*(uint32_t*)raw.raw_bytes);
}

static inline uint64_t decode_raw40(raw_40 raw)
{
    return ((uint64_t)ntohl(*(uint32_t*)raw.raw_bytes) << 8) | raw.raw_bytes[4];
}

static inline uint64_t decode_raw48(raw_48 raw)
{
    return ((uint64_t)ntohl(*(uint32_t*)raw.raw_bytes) << 16) |
           ntohs(*(uint16_t*)(&raw.raw_bytes[4]));
}

static inline uint64_t decode_raw64(raw_64 raw)
{
    return ntohll(*(uint64_t*)raw.raw_bytes);
}

// Functions for encoding native integers to raw_xx types:

static inline raw_08 encode_raw08(uint8_t value)
{
    raw_08 raw;
    raw.raw_bytes[0] = value;
    return raw;
}

static inline raw_16 encode_raw16(uint16_t value)
{
    raw_16 raw;
    *(uint16_t*)&raw.raw_bytes = htons(value);
    return raw;
}

static inline raw_32 encode_raw32(uint32_t value)
{
    raw_32 raw;
    *(uint32_t*)&raw.raw_bytes = htonl(value);
    return raw;
}

static inline raw_40 encode_raw40(uint64_t value)
{
    raw_40 raw;
    *(uint32_t*)&raw.raw_bytes = htonl(value >> 8);
    raw.raw_bytes[4] = (value & 0xFF);
    return raw;
}

static inline raw_48 encode_raw48(uint64_t value)
{
    raw_48 raw;
    *(uint32_t*)&raw.raw_bytes = htonl(value >> 16);
    *(uint16_t*)&raw.raw_bytes[4] = htons(value & 0xFFFF);
    return raw;
}

static inline raw_64 encode_raw64(uint64_t value)
{
    raw_64 raw;
    *(uint64_t*)&raw.raw_bytes = htonll(value);
    return raw;
}

#endif
