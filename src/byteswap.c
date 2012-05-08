/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"

#if !defined(HAVE_HTONLL) && !defined(WORDS_BIGENDIAN)
uint64_t couchstore_byteswap64(uint64_t val)
{
    size_t ii;
    uint64_t ret = 0;
    for (ii = 0; ii < sizeof(uint64_t); ii++) {
        ret <<= 8;
        ret |= val & 0xff;
        val >>= 8;
    }
    return ret;
}
#elif defined(__GNUC__)
// solaris boxes contains a ntohll/htonll method, but
// it seems like the gnu linker doesn't like to use
// an archive without _any_ symbols in it ;)
int unreferenced_symbol_to_satisfy_the_linker;
#endif
