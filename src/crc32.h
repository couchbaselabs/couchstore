#ifndef COUCHSTORE_CRC32_H
#define COUCHSTORE_CRC_H 1

#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

    uint32_t hash_crc32(const char *key, size_t key_length);

#ifdef __cplusplus
}
#endif

#endif
