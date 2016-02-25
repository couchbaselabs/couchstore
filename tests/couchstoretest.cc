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
#include <src/internal.h>

CouchstoreTest::CouchstoreTest()
    : CouchstoreTest("testfile.couch"){
}

CouchstoreTest::CouchstoreTest(const std::string& _filePath)
    : db(nullptr),
      filePath(_filePath) {
}

/**
    Called after each test finishes.
      - Closes db (if non-null)
      - Removes testfile.couch
**/
CouchstoreTest::~CouchstoreTest() {
    clean_up();
    /* make sure os.c didn't accidentally call close(0): */
#ifndef WIN32
    EXPECT_TRUE(lseek(0, 0, SEEK_CUR) >= 0 || errno != EBADF);
#endif
}

void CouchstoreTest::clean_up() {
    if (db) {
        couchstore_close_file(db);
        couchstore_free_db(db);
        db = nullptr;
    }
    remove(filePath.c_str());
}

CouchstoreInternalTest::CouchstoreInternalTest()
        : CouchstoreTest("testfile_internal.couch"),
          compactPath("testfile_internal.couch.compact"),
          documents(Documents(0)),
          ops(create_default_file_ops()) {
}

CouchstoreInternalTest::~CouchstoreInternalTest() {
    // Destruct db here instead of parent so that ops isn't destructed
    // when we try to destruct the db.
    clean_up();
    remove(compactPath.c_str());
}

couchstore_error_t CouchstoreInternalTest::open_db(couchstore_open_flags extra_flags) {
    return couchstore_open_db_ex(filePath.c_str(),
                                 extra_flags | COUCHSTORE_OPEN_FLAG_UNBUFFERED,
                                 &ops, &db);
}


void CouchstoreInternalTest::open_db_and_populate(couchstore_open_flags extra_flags,
                                                  size_t count) {
    ASSERT_EQ(COUCHSTORE_SUCCESS, open_db(extra_flags));
    documents = Documents(count);
    documents.generateDocs();
    ASSERT_EQ(COUCHSTORE_SUCCESS,
              couchstore_save_documents(db, documents.getDocs(),
                                        documents.getDocInfos(), count, 0));

}

LocalDoc CouchstoreInternalTest::create_local_doc(std::string& id,
                                                  std::string& json) {
    LocalDoc doc;
    doc.id.buf = &id[0];
    doc.id.size = strlen(doc.id.buf);
    doc.json.buf = &json[0];
    doc.json.size = strlen(doc.json.buf);
    doc.deleted = 0;
    return doc;
}
