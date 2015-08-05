/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include "couchstoretest.h"
#include <libcouchstore/couch_db.h>

CouchstoreTest::CouchstoreTest()
    : db(nullptr),
      filePath("testfile.couch") {
}

/**
    Called after each test finishes.
      - Closes db (if non-null)
      - Removes testfile.couch
**/
CouchstoreTest::~CouchstoreTest() {
    if (db) {
        couchstore_close_db(db);
    }
    remove(filePath.c_str());
    /* make sure os.c didn't accidentally call close(0): */
#ifndef WIN32
    EXPECT_TRUE(lseek(0, 0, SEEK_CUR) >= 0 || errno != EBADF);
#endif
}
