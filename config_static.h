/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010, 2011 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

/**
 * This file contains the static part of the configure script. Please add
 * all platform specific conditional code to this file.
 */
#ifndef COUCHSTORE_CONFIG_STATIC_H
#define COUCHSTORE_CONFIG_STATIC_H 1

//Large File Support
#define _LARGE_FILE 1
#ifndef _FILE_OFFSET_BITS
#  define _FILE_OFFSET_BITS 64
#elif (_FILE_OFFSET_BITS != 64)
#error "bad things"
#endif
#define _LARGEFILE_SOURCE 1
#ifndef O_LARGEFILE
# define O_LARGEFILE 0
#endif

#include <sys/types.h>

#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>
#endif

#ifdef HAVE_INTTYPES_H
#include <inttypes.h>
#endif

#ifdef __APPLE__
#define fdatasync(FD) fsync(FD)  // autoconf things OS X has fdatasync but it doesn't
#ifndef HAVE_HTONLL
// On Darwin, use built-in functions for 64-bit byte-swap:
#include <libkern/OSByteOrder.h>
#define ntohll(n) OSSwapBigToHostInt64(n)
#define htonll(n) OSSwapHostToBigInt64(n)
#define HAVE_HTONLL
#endif // HAVE_HTONLL
#endif // __APPLE__

#ifndef HAVE_HTONLL
#ifdef WORDS_BIGENDIAN
#define ntohll(a) a
#define htonll(a) a
#elif defined(__GLIBC__)
#define HAVE_HTONLL 1
/* GNU libc does have bswap which is optimized implementation */
#include <byteswap.h>
#define ntohll(a) bswap_64(a)
#define htonll(a) bswap_64(a)
#else
#define ntohll(a) couchstore_byteswap64(a)
#define htonll(a) couchstore_byteswap64(a)

#ifdef __cplusplus
extern "C" {
#endif
   extern uint64_t couchstore_byteswap64(uint64_t val);
#ifdef __cplusplus
}
#endif
#endif /* WORDS_BIGENDIAN */
#endif /* HAVE_HTONLL */


#ifdef linux
#undef ntohs
#undef ntohl
#undef htons
#undef htonl
#endif

#if defined(WIN32) || defined(_WIN32)
#include <windows.h>
#define WINDOWS
#endif

#endif
