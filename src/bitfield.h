#ifndef COUCH_BITFIELD_H
#define COUCH_BITFIELD_H

#include "config.h"
#include "internal.h"
#include <assert.h>
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
    uint8_t raw_bytes[3];
} raw_24;

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

#define DECODE_RAW(DST_TYPE, FLIP_FN) \
    DST_TYPE value = 0; \
    memcpy((char*)&value + sizeof(value) - sizeof(raw), &(raw), sizeof(raw)); \
    return FLIP_FN(value)

static inline uint8_t decode_raw08(raw_08 raw)
{
    return raw.raw_bytes[0];
}

static inline uint16_t decode_raw16(raw_16 raw)
{
    DECODE_RAW(uint16_t, ntohs);
}

static inline uint32_t decode_raw24(raw_24 raw)
{
    DECODE_RAW(uint32_t, ntohl);
}

static inline uint32_t decode_raw32(raw_32 raw)
{
    DECODE_RAW(uint32_t, ntohl);
}

static inline uint64_t decode_raw40(raw_40 raw)
{
    DECODE_RAW(uint64_t, ntohll);
}

static inline uint64_t decode_raw48(raw_48 raw)
{
    DECODE_RAW(uint64_t, ntohll);
}

static inline uint64_t decode_raw64(raw_64 raw)
{
    DECODE_RAW(uint64_t, ntohll);
}


// Functions for encoding native integers to raw_xx types:

#define ENCODE_RAW(FLIP_FN, RAW_TYPE) \
    value = FLIP_FN(value); \
    RAW_TYPE raw; \
    memcpy(&raw, (char*)&value + sizeof(value) - sizeof(raw), sizeof(raw)); \
    return raw


static inline raw_08 encode_raw08(uint8_t value)
{
    ENCODE_RAW(, raw_08);
}

static inline raw_16 encode_raw16(uint16_t value)
{
    ENCODE_RAW(htons, raw_16);
}

static inline raw_32 encode_raw32(uint32_t value)
{
    ENCODE_RAW(htonl, raw_32);
}

static inline raw_40 encode_raw40(uint64_t value)
{
    ENCODE_RAW(htonll, raw_40);
}

static inline raw_48 encode_raw48(uint64_t value)
{
    ENCODE_RAW(htonll, raw_48);
}

static inline raw_64 encode_raw64(uint64_t value)
{
    ENCODE_RAW(htonll, raw_64);
}

#endif
