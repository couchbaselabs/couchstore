//
//  iobuffer.h
//  couchstore
//
//  Created by Jens Alfke on 4/12/12.
//  Copyright (c) 2012 Couchbase, Inc. All rights reserved.
//

#ifndef LIBCOUCHSTORE_IOBUFFER_H
#define LIBCOUCHSTORE_IOBUFFER_H 1

#include <libcouchstore/couch_db.h>

/**
 * Constructs a set of file ops that buffer the I/O provided by an underlying set of raw ops.
 * @param raw_ops the file ops callbacks to use for the underlying I/O
 * @param handle on output, a constructed (but not opened) couch_file_handle
 * @return the couch_file_ops to use, or NULL on failure
 */

const couch_file_ops *couch_get_buffered_file_ops(const couch_file_ops* raw_ops,
                                                  couch_file_handle* handle);

#endif // LIBCOUCHSTORE_IOBUFFER_H
