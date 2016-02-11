/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc
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

#ifndef LIBCOUCHSTORE_IOBUFFER_H
#define LIBCOUCHSTORE_IOBUFFER_H 1

#include <libcouchstore/couch_db.h>

/**
 * Constructs a set of file ops that buffer the I/O provided by an underlying set of raw ops.
 * @param raw_ops the file ops callbacks to use for the underlying I/O
 * @param handle on output, a constructed (but not opened) couch_file_handle
 * @param whether or not the file is being opened as read only
 * @return the couch_file_ops to use, or NULL on failure
 */

FileOpsInterface* couch_get_buffered_file_ops(couchstore_error_info_t *errinfo,
                                              FileOpsInterface* raw_ops,
                                              couch_file_handle* handle,
                                              bool readOnly);

#endif // LIBCOUCHSTORE_IOBUFFER_H
