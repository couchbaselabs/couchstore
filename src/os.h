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

#pragma once

#include <libcouchstore/couch_db.h>

class FileOps : public FileOpsInterface {
public:
    FileOps() {}

    couch_file_handle constructor(couchstore_error_info_t* errinfo) override ;
    couchstore_error_t open(couchstore_error_info_t* errinfo,
                            couch_file_handle* handle, const char* path,
                            int oflag) override;
    couchstore_error_t close(couchstore_error_info_t* errinfo,
                             couch_file_handle handle) override;
    couchstore_error_t set_periodic_sync(couch_file_handle handle,
                                         uint64_t period_bytes) override;
    ssize_t pread(couchstore_error_info_t* errinfo,
                  couch_file_handle handle, void* buf, size_t nbytes,
                  cs_off_t offset) override;
    ssize_t pwrite(couchstore_error_info_t* errinfo,
                   couch_file_handle handle, const void* buf,
                   size_t nbytes, cs_off_t offset) override;
    cs_off_t goto_eof(couchstore_error_info_t* errinfo,
                      couch_file_handle handle) override;
    couchstore_error_t sync(couchstore_error_info_t* errinfo,
                            couch_file_handle handle) override;
    couchstore_error_t advise(couchstore_error_info_t* errinfo,
                              couch_file_handle handle, cs_off_t offset,
                              cs_off_t len,
                              couchstore_file_advice_t advice) override;
    void destructor(couch_file_handle handle) override;
};
