#ifndef COUCH_BITFIELD_H
#define COUCH_BITFIELD_H
static inline uint64_t get_48(const char *buf)
{
    uint64_t num = 0;
    char *numbuf = (char *) &num;
    memcpy(numbuf + 2, buf, 6);
    return ntohll(num);
}


static inline uint64_t get_40(const char *buf)
{
    uint64_t num = 0;
    char *numbuf = (char *) &num;
    memcpy(numbuf + 3, buf, 5);
    return ntohll(num);
}

static inline uint32_t get_32(const char *buf)
{
    uint32_t num;
    memcpy(&num, buf, 4);
    return ntohl(num);
}


static inline uint32_t get_16(const char *buf)
{
    uint32_t num = 0;
    char *numbuf = (char *) &num;
    memcpy(numbuf + 2, buf, 2);
    return ntohl(num);
}

static inline void get_kvlen(const char *buf, uint32_t *klen, uint32_t *vlen)
{
    //12, 28 bit
    uint32_t num = 0;
    char *numbuf = (char *) &num;
    memcpy(numbuf + 2, buf, 2);
    *klen = ntohl(num) >> 4;
    num = 0;
    numbuf = (char *) &num;
    memcpy(numbuf, buf + 1, 4);
    *vlen = ntohl(num) & 0x0FFFFFFF;
}

//Flip on the the bits of num as a numbits-bit number, offset bitpos bits into
//buf. MUST ZERO MEMORY _BEFORE_ WRITING TO IT!
static inline void set_bits(char *buf, const int bitpos, const int numbits, uint64_t num)
{
    num = num << (64 - (numbits + bitpos));
    num = htonll(num);
    char *nbuf = (char *) &num;
    int i = 0;
    for (i = 0; i < ((numbits + bitpos + 7) / 8); i++) {
        buf[i] |= nbuf[i];
    }
}
#endif
