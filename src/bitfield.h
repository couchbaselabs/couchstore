#ifndef COUCH_BITFIELD_H
#define COUCH_BITFIELD_H

#include "config.h"
#include <string.h>

/** Read a 48-bit (6-byte) big-endian integer from the address pointed to by buf. */
static inline uint64_t get_48(const char *buf)
{
    const uint32_t* longs = (const uint32_t*)buf;
    const uint16_t* shorts = (const uint16_t*)buf;
    return ((uint64_t)ntohl(longs[0]) << 16) | ntohs(shorts[2]);
}

/** Read a 40-bit (5-byte) big-endian integer from the address pointed to by buf. */
static inline uint64_t get_40(const char *buf)
{
    const uint32_t* longs = (const uint32_t*)buf;
    return ((uint64_t)ntohl(longs[0]) << 8) | buf[4];
}

/** Read a 32-bit big-endian integer from the address pointed to by buf. */
static inline uint32_t get_32(const char *buf)
{
    return ntohl(*(const uint32_t*)buf);
}

/** Read a 16-bit big-endian integer from the address pointed to by buf. */
static inline uint32_t get_16(const char *buf)
{
    return ntohs(*(const uint16_t*)buf);
}

/** Read a 12-bit key length and 28-bit value length, packed into 5 bytes big-endian. */
static inline void get_kvlen(const char *buf, uint32_t *klen, uint32_t *vlen)
{
    //12, 28 bit
    *klen = get_16(buf) >> 4;
    *vlen = get_32(buf + 1) & 0x0FFFFFFF;
}

/** Flip on the the bits of num as a numbits-bit number, offset bitpos bits into
    buf. MUST ZERO MEMORY _BEFORE_ WRITING TO IT! */
static inline void set_bits(char *buf, const int bitpos, const int numbits, uint64_t num)
{
    num = num << (64 - (numbits + bitpos));
    num = htonll(num);
    *(uint64_t*)buf |= num;
}
#endif
