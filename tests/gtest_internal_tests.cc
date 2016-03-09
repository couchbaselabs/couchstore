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

/*
 * This test file is for GTest tests which test the internal API.
 *
 * This is in contrast to gtest_tests.cc which runs tests using
 * just the external API.
 */

#include <gtest/gtest.h>

#include "couchstoretest.h"
#include "couchstoredoctest.h"
#include "documents.h"

#include <libcouchstore/couch_db.h>
#include "src/internal.h"

/**
 * The commit alignment test checks that the file size following
 * these situations are all the same:
 *
 * - Precommit
 * - Write Header
 * - Commit (= Precommit followed by a Write Header)
 *
 * This is done to verify that the precommit has extended the
 * file long enough to encompass the subsequently written header
 * (which avoids a metadata flush when we sync).
 */
TEST_F(CouchstoreInternalTest, commit_alignment) {

    const size_t docCount = 100;
    couchstore_error_info_t errinfo;
    Documents documents(docCount);
    documents.generateDocs();

    /* Get the size of the precommit */
    ASSERT_EQ(COUCHSTORE_SUCCESS,
            couchstore_open_db(filePath.c_str(),
                               COUCHSTORE_OPEN_FLAG_CREATE,
                               &db));

    ASSERT_EQ(COUCHSTORE_SUCCESS, couchstore_save_documents(db,
                                                            documents.getDocs(),
                                                            documents.getDocInfos(),
                                                            docCount,
                                                            0));

    EXPECT_EQ(COUCHSTORE_SUCCESS, precommit(db));
    cs_off_t precommit_size = db->file.ops->goto_eof(&errinfo, db->file.handle);

    /* Clean-up */
    couchstore_close_db(db);
    remove(filePath.c_str());


    /* Get the size from actually writing a header without a precommit */
    ASSERT_EQ(COUCHSTORE_SUCCESS,
              couchstore_open_db(filePath.c_str(),
                                 COUCHSTORE_OPEN_FLAG_CREATE,
                                 &db));

    ASSERT_EQ(COUCHSTORE_SUCCESS, couchstore_save_documents(db,
                                                            documents.getDocs(),
                                                            documents.getDocInfos(),
                                                            docCount,
                                                            0));

    ASSERT_EQ(COUCHSTORE_SUCCESS, db_write_header(db));
    ASSERT_EQ(COUCHSTORE_SUCCESS, db->file.ops->sync(&db->file.lastError, db->file.handle));

    /* Compare */
    EXPECT_EQ(precommit_size,
              db->file.ops->goto_eof(&errinfo, db->file.handle));

    /* Clean-up */
    couchstore_close_db(db);
    remove(filePath.c_str());

    /* Get the size from actually doing a full commit */
    ASSERT_EQ(COUCHSTORE_SUCCESS,
              couchstore_open_db(filePath.c_str(),
                                 COUCHSTORE_OPEN_FLAG_CREATE,
                                 &db));

    ASSERT_EQ(COUCHSTORE_SUCCESS, couchstore_save_documents(db,
                                                            documents.getDocs(),
                                                            documents.getDocInfos(),
                                                            docCount,
                                                            0));

    ASSERT_EQ(COUCHSTORE_SUCCESS, couchstore_commit(db));
    /* Compare */
    EXPECT_EQ(precommit_size,
              db->file.ops->goto_eof(&errinfo, db->file.handle));
}

/**
 * Tests whether the unbuffered file ops flag actually
 * prevents the buffered file operations from being used.
 */
TEST_F(CouchstoreInternalTest, unbuffered_fileops) {
    ASSERT_EQ(COUCHSTORE_SUCCESS,
              couchstore_open_db_ex(filePath.c_str(),
                                    COUCHSTORE_OPEN_FLAG_CREATE | COUCHSTORE_OPEN_FLAG_UNBUFFERED,
                                    couchstore_get_default_file_ops(),
                                    &db));
    EXPECT_EQ(db->file.ops, couchstore_get_default_file_ops());
}
