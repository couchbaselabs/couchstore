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

#include <sys/types.h>

#ifdef HAVE_NETINET_IN_H
#include <netinet/in.h>
#endif

#ifdef HAVE_INTTYPES_H
#include <inttypes.h>
#endif

#ifndef HAVE_HTONLL
#ifdef WORDS_BIGENDIAN
#define ntohll(a) a
#define htonll(a) a
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

#endif
