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

using namespace testing;

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
    couchstore_error_info_t errinfo;

    open_db_and_populate(COUCHSTORE_OPEN_FLAG_CREATE, 100);

    EXPECT_EQ(COUCHSTORE_SUCCESS, precommit(db));
    cs_off_t precommit_size = db->file.ops->goto_eof(&errinfo, db->file.handle);

    clean_up();

    /* Get the size from actually writing a header without a precommit */
    open_db_and_populate(COUCHSTORE_OPEN_FLAG_CREATE, 100);

    ASSERT_EQ(COUCHSTORE_SUCCESS, db_write_header(db));
    ASSERT_EQ(COUCHSTORE_SUCCESS, db->file.ops->sync(&db->file.lastError, db->file.handle));

    /* Compare */
    EXPECT_EQ(precommit_size,
              db->file.ops->goto_eof(&errinfo, db->file.handle));

    clean_up();

    /* Get the size from actually doing a full commit */
    open_db_and_populate(COUCHSTORE_OPEN_FLAG_CREATE, 100);

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

TEST_F(FileOpsErrorInjectionTest, dbopen_fileopen_fail) {
    EXPECT_CALL(ops, open(_, _, _, _)).WillOnce(Return(COUCHSTORE_ERROR_OPEN_FILE));
    EXPECT_EQ(COUCHSTORE_ERROR_OPEN_FILE, open_db(COUCHSTORE_OPEN_FLAG_CREATE));
}

TEST_F(FileOpsErrorInjectionTest, dbopen_filegoto_eof_fail) {
    EXPECT_CALL(ops, goto_eof(_, _)).WillOnce(Return(-1));
    EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(0);
    EXPECT_EQ(COUCHSTORE_ERROR_OPEN_FILE, open_db(COUCHSTORE_OPEN_FLAG_CREATE));
}

/**
 * This is a parameterised test which injects errors on specific
 * calls to the file ops object.
 *
 * In this example pwrite(..) is set up to pass through to a
 * non-mock implementation of FileOpsInterface for the first
 * `GetParam()` # of calls, then to inject an error on the
 * `GetParam() + 1` call.
 *
 * For this example GetParam() will be the range of values from
 * 0 to 2 (i.e. {0, 1}). Therefore pwrite(..) will return
 * a COUCHSTORE_ERROR_WRITE on the 1st and 2nd calls in each
 * instance.
 */
typedef ParameterisedFileOpsErrorInjectionTest NewOpenWrite;
TEST_P(NewOpenWrite, fail) {
    InSequence s;

    EXPECT_CALL(ops, pwrite(_, _, _, _, _)).Times(GetParam());
    EXPECT_CALL(ops, pwrite(_, _, _, _, _)).WillOnce(Return(COUCHSTORE_ERROR_WRITE));
    EXPECT_EQ(COUCHSTORE_ERROR_WRITE, open_db(COUCHSTORE_OPEN_FLAG_CREATE));
}
INSTANTIATE_TEST_CASE_P(Parameterised, NewOpenWrite,
                        ::testing::Range(0, 2),
                        ::testing::PrintToStringParamName());

TEST_F(FileOpsErrorInjectionTest, dbdropfile_fileclose_fail) {
    ASSERT_EQ(COUCHSTORE_SUCCESS, open_db(COUCHSTORE_OPEN_FLAG_CREATE));
    {
        InSequence s;
        EXPECT_CALL(ops, close(_, _)).WillOnce(Return(COUCHSTORE_ERROR_WRITE));
        EXPECT_EQ(COUCHSTORE_ERROR_WRITE, couchstore_close_file(db));
    }
}

typedef ParameterisedFileOpsErrorInjectionTest SaveDocsWrite;
TEST_P(SaveDocsWrite, fail) {
    ASSERT_EQ(COUCHSTORE_SUCCESS, open_db(COUCHSTORE_OPEN_FLAG_CREATE));
    const size_t docCount = 1;
    documents = Documents(docCount);
    documents.generateDocs();
    {
        InSequence s;
        EXPECT_CALL(ops, pwrite(_, _, _, _, _)).Times(GetParam());
        EXPECT_CALL(ops, pwrite(_, _, _, _, _)).WillOnce(Return(COUCHSTORE_ERROR_WRITE));
        EXPECT_EQ(COUCHSTORE_ERROR_WRITE,
                  couchstore_save_documents(db, documents.getDocs(),
                                            documents.getDocInfos(), docCount, 0));
    }
}
INSTANTIATE_TEST_CASE_P(Parameterised, SaveDocsWrite,
                        ::testing::Range(0, 6),
                        ::testing::PrintToStringParamName());

typedef ParameterisedFileOpsErrorInjectionTest CommitSync;
TEST_P(CommitSync, fail) {
    open_db_and_populate(COUCHSTORE_OPEN_FLAG_CREATE, 1);
    {
        InSequence s;
        EXPECT_CALL(ops, sync(_, _)).Times(GetParam());
        EXPECT_CALL(ops, sync(_, _)).WillOnce(Return(COUCHSTORE_ERROR_WRITE));
        EXPECT_EQ(COUCHSTORE_ERROR_WRITE, couchstore_commit(db));
    }
}
INSTANTIATE_TEST_CASE_P(Parameterised, CommitSync,
                        ::testing::Range(0, 2),
                        ::testing::PrintToStringParamName());

typedef ParameterisedFileOpsErrorInjectionTest CommitWrite;
TEST_P(CommitWrite, fail) {
    open_db_and_populate(COUCHSTORE_OPEN_FLAG_CREATE, 1);
    {
        InSequence s;
        EXPECT_CALL(ops, pwrite(_, _, _, _, _)).Times(GetParam());
        EXPECT_CALL(ops, pwrite(_, _, _, _, _)).WillOnce(Return(COUCHSTORE_ERROR_WRITE));
        EXPECT_EQ(COUCHSTORE_ERROR_WRITE, couchstore_commit(db));
    }
}
INSTANTIATE_TEST_CASE_P(Parameterised, CommitWrite,
                        ::testing::Range(0, 4),
                        ::testing::PrintToStringParamName());

typedef ParameterisedFileOpsErrorInjectionTest SaveDocWrite;
TEST_P(SaveDocWrite, fail) {
    ASSERT_EQ(COUCHSTORE_SUCCESS, open_db(COUCHSTORE_OPEN_FLAG_CREATE));
    Documents documents(1);
    documents.generateDocs();
    {
        InSequence s;
        EXPECT_CALL(ops, pwrite(_, _, _, _, _)).Times(GetParam());
        EXPECT_CALL(ops, pwrite(_, _, _, _, _)).WillOnce(Return(COUCHSTORE_ERROR_WRITE));
        EXPECT_EQ(COUCHSTORE_ERROR_WRITE,
                  couchstore_save_document(db, documents.getDoc(0),
                                           documents.getDocInfo(0), 0));
    }
}
INSTANTIATE_TEST_CASE_P(Parameterised, SaveDocWrite,
                        ::testing::Range(0, 6),
                        ::testing::PrintToStringParamName());

typedef ParameterisedFileOpsErrorInjectionTest DocInfoById;
TEST_P(DocInfoById, fail) {
    open_db_and_populate(COUCHSTORE_OPEN_FLAG_CREATE, 10);
    EXPECT_EQ(COUCHSTORE_SUCCESS, couchstore_commit(db));
    {
        InSequence s;
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(GetParam());
        EXPECT_CALL(ops, pread(_, _, _, _, _)).WillOnce(Return(COUCHSTORE_ERROR_READ));
        EXPECT_EQ(COUCHSTORE_ERROR_READ,
                  couchstore_docinfo_by_id(db, documents.getDocInfo(0)->id.buf,
                                           documents.getDocInfo(0)->id.size,
                                           &info));

    }
}
INSTANTIATE_TEST_CASE_P(Parameterised, DocInfoById,
                        ::testing::Range(0, 2),
                        ::testing::PrintToStringParamName());

typedef ParameterisedFileOpsErrorInjectionTest DocInfoBySeq;
TEST_P(DocInfoBySeq, fail) {
    open_db_and_populate(COUCHSTORE_OPEN_FLAG_CREATE, 10);
    EXPECT_EQ(COUCHSTORE_SUCCESS, couchstore_commit(db));
    couchstore_docinfo_by_id(db, documents.getDocInfo(0)->id.buf,
                             documents.getDocInfo(0)->id.size, &info);
    {
        InSequence s;
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(GetParam());
        EXPECT_CALL(ops, pread(_, _, _, _, _)).WillOnce(Return(COUCHSTORE_ERROR_READ));
        EXPECT_EQ(COUCHSTORE_ERROR_READ,
                  couchstore_docinfo_by_sequence(db, info->db_seq, &info));

    }
    couchstore_free_docinfo(info);
}
INSTANTIATE_TEST_CASE_P(Parameterised, DocInfoBySeq,
                        ::testing::Range(0, 2),
                        ::testing::PrintToStringParamName());

typedef ParameterisedFileOpsErrorInjectionTest OpenDocRead;
TEST_P(OpenDocRead, fail) {
    open_db_and_populate(COUCHSTORE_OPEN_FLAG_CREATE, 10);
    EXPECT_EQ(COUCHSTORE_SUCCESS, couchstore_commit(db));
    {
        InSequence s;
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(GetParam());
        EXPECT_CALL(ops, pread(_, _, _, _, _)).WillOnce(Return(COUCHSTORE_ERROR_READ));
        EXPECT_EQ(COUCHSTORE_ERROR_READ,
                  couchstore_open_document(db, documents.getDocInfo(0)->id.buf,
                                           documents.getDocInfo(0)->id.size,
                                           &doc, DECOMPRESS_DOC_BODIES));
    }
}
INSTANTIATE_TEST_CASE_P(Parameterised, OpenDocRead,
                        ::testing::Range(0, 2),
                        ::testing::PrintToStringParamName());

typedef ParameterisedFileOpsErrorInjectionTest DocByInfoRead;
TEST_P(DocByInfoRead, fail) {
    open_db_and_populate(COUCHSTORE_OPEN_FLAG_CREATE, 10);
    EXPECT_EQ(COUCHSTORE_SUCCESS, couchstore_commit(db));
    couchstore_docinfo_by_id(db, documents.getDocInfo(0)->id.buf,
                             documents.getDocInfo(0)->id.size, &info);
    {
        InSequence s;
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(GetParam());
        EXPECT_CALL(ops, pread(_, _, _, _, _)).WillOnce(Return(COUCHSTORE_ERROR_READ));
        EXPECT_EQ(COUCHSTORE_ERROR_READ,
                  couchstore_open_doc_with_docinfo(db, info, &doc, DECOMPRESS_DOC_BODIES));

    }
    couchstore_free_docinfo(info);
}
INSTANTIATE_TEST_CASE_P(Parameterised, DocByInfoRead,
                        ::testing::Range(0, 2),
                        ::testing::PrintToStringParamName());

static int changes_callback(Db *db, DocInfo *docinfo, void *ctx) {
    return 0;
}

typedef ParameterisedFileOpsErrorInjectionTest ChangesSinceRead;
TEST_P(ChangesSinceRead, fail) {
    open_db_and_populate(COUCHSTORE_OPEN_FLAG_CREATE, 10);
    EXPECT_EQ(COUCHSTORE_SUCCESS, couchstore_commit(db));
    couchstore_docinfo_by_id(db, documents.getDocInfo(0)->id.buf,
                             documents.getDocInfo(0)->id.size, &info);
    {
        InSequence s;
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(GetParam());
        EXPECT_CALL(ops, pread(_, _, _, _, _)).WillOnce(Return(COUCHSTORE_ERROR_READ));
        EXPECT_EQ(COUCHSTORE_ERROR_READ,
                  couchstore_changes_since(db, info->db_seq, 0,
                                           changes_callback, nullptr));

    }
    couchstore_free_docinfo(info);
}
INSTANTIATE_TEST_CASE_P(Parameterised, ChangesSinceRead,
                        ::testing::Range(0, 2),
                        ::testing::PrintToStringParamName());

typedef ParameterisedFileOpsErrorInjectionTest AllDocsRead;
TEST_P(AllDocsRead, fail) {
    open_db_and_populate(COUCHSTORE_OPEN_FLAG_CREATE, 10);
    EXPECT_EQ(COUCHSTORE_SUCCESS, couchstore_commit(db));
    couchstore_docinfo_by_id(db, documents.getDocInfo(0)->id.buf,
                             documents.getDocInfo(0)->id.size, &info);
    {
        InSequence s;
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(GetParam());
        EXPECT_CALL(ops, pread(_, _, _, _, _)).WillOnce(Return(COUCHSTORE_ERROR_READ));
        EXPECT_EQ(COUCHSTORE_ERROR_READ,
                  couchstore_all_docs(db, nullptr, 0,
                                      changes_callback, nullptr));

    }
    couchstore_free_docinfo(info);
}
INSTANTIATE_TEST_CASE_P(Parameterised, AllDocsRead,
                        ::testing::Range(0, 2),
                        ::testing::PrintToStringParamName());

typedef ParameterisedFileOpsErrorInjectionTest DocInfosByIdRead;
TEST_P(DocInfosByIdRead, fail) {
    open_db_and_populate(COUCHSTORE_OPEN_FLAG_CREATE, 10);
    EXPECT_EQ(COUCHSTORE_SUCCESS, couchstore_commit(db));

    {
        InSequence s;
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(GetParam());
        EXPECT_CALL(ops, pread(_, _, _, _, _)).WillOnce(Return(COUCHSTORE_ERROR_READ));
        EXPECT_EQ(COUCHSTORE_ERROR_READ,
                  couchstore_docinfos_by_id(db, &documents.getDocInfo(0)->id,
                                            1, 0, changes_callback, nullptr));

    }
}
INSTANTIATE_TEST_CASE_P(Parameterised, DocInfosByIdRead,
                        ::testing::Range(0, 2),
                        ::testing::PrintToStringParamName());

typedef ParameterisedFileOpsErrorInjectionTest DocInfosBySeqRead;
TEST_P(DocInfosBySeqRead, fail) {
    open_db_and_populate(COUCHSTORE_OPEN_FLAG_CREATE, 10);
    EXPECT_EQ(COUCHSTORE_SUCCESS, couchstore_commit(db));
    couchstore_docinfo_by_id(db, documents.getDocInfo(0)->id.buf,
                             documents.getDocInfo(0)->id.size, &info);
    {
        InSequence s;
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(GetParam());
        EXPECT_CALL(ops, pread(_, _, _, _, _)).WillOnce(Return(COUCHSTORE_ERROR_READ));
        EXPECT_EQ(COUCHSTORE_ERROR_READ,
                  couchstore_docinfos_by_sequence(db, &info->db_seq, 1, 0,
                                                  changes_callback, nullptr));

    }
    couchstore_free_docinfo(info);
}
INSTANTIATE_TEST_CASE_P(Parameterised, DocInfosBySeqRead,
                        ::testing::Range(0, 2),
                        ::testing::PrintToStringParamName());

static int tree_walk_callback(Db *db, int depth, const DocInfo* doc_info,
                       uint64_t subtree_size, const sized_buf* reduce_value,
                       void *ctx) {
    return 0;
}

typedef ParameterisedFileOpsErrorInjectionTest WalkIdTreeRead;
TEST_P(WalkIdTreeRead, fail) {
    open_db_and_populate(COUCHSTORE_OPEN_FLAG_CREATE, 10);
    EXPECT_EQ(COUCHSTORE_SUCCESS, couchstore_commit(db));
    couchstore_docinfo_by_id(db, documents.getDocInfo(0)->id.buf,
                             documents.getDocInfo(0)->id.size, &info);
    {
        InSequence s;
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(GetParam());
        EXPECT_CALL(ops, pread(_, _, _, _, _)).WillOnce(Return(COUCHSTORE_ERROR_READ));
        EXPECT_EQ(COUCHSTORE_ERROR_READ,
                  couchstore_walk_id_tree(db, nullptr, 0,
                                          tree_walk_callback, nullptr));

    }
    couchstore_free_docinfo(info);
}
INSTANTIATE_TEST_CASE_P(Parameterised, WalkIdTreeRead,
                        ::testing::Range(0, 2),
                        ::testing::PrintToStringParamName());

typedef ParameterisedFileOpsErrorInjectionTest WalkSeqTreeRead;
TEST_P(WalkSeqTreeRead, fail) {
    open_db_and_populate(COUCHSTORE_OPEN_FLAG_CREATE, 10);
    EXPECT_EQ(COUCHSTORE_SUCCESS, couchstore_commit(db));
    couchstore_docinfo_by_id(db, documents.getDocInfo(0)->id.buf,
                             documents.getDocInfo(0)->id.size, &info);
    {
        InSequence s;
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(GetParam());
        EXPECT_CALL(ops, pread(_, _, _, _, _)).WillOnce(Return(COUCHSTORE_ERROR_READ));
        EXPECT_EQ(COUCHSTORE_ERROR_READ,
                  couchstore_walk_seq_tree(db, 0, 0,
                                           tree_walk_callback, nullptr));

    }
    couchstore_free_docinfo(info);
}
INSTANTIATE_TEST_CASE_P(Parameterised, WalkSeqTreeRead,
                        ::testing::Range(0, 2),
                        ::testing::PrintToStringParamName());

typedef ParameterisedFileOpsErrorInjectionTest LocalDocFileWrite;
TEST_P(LocalDocFileWrite, fail) {
    ASSERT_EQ(COUCHSTORE_SUCCESS, open_db(COUCHSTORE_OPEN_FLAG_CREATE));
    std::string id("Hello");
    std::string json("\"World\"");
    LocalDoc doc(create_local_doc(id, json));

    {
        InSequence s;
        EXPECT_CALL(ops, pwrite(_, _, _, _, _)).Times(GetParam());
        EXPECT_CALL(ops, pwrite(_, _, _, _, _)).WillOnce(Return(COUCHSTORE_ERROR_WRITE));
        EXPECT_EQ(COUCHSTORE_ERROR_WRITE, couchstore_save_local_document(db, &doc));
    }
}
INSTANTIATE_TEST_CASE_P(Parameterised, LocalDocFileWrite,
                        ::testing::Range(0, 2),
                        ::testing::PrintToStringParamName());

typedef ParameterisedFileOpsErrorInjectionTest LocalDocFileRead;
TEST_P(LocalDocFileRead, fail) {
    ASSERT_EQ(COUCHSTORE_SUCCESS, open_db(COUCHSTORE_OPEN_FLAG_CREATE));

    std::string id("Hello");
    std::string json("\"World\"");
    LocalDoc doc(create_local_doc(id, json));
    ASSERT_EQ(COUCHSTORE_SUCCESS, couchstore_save_local_document(db, &doc));

    {
        InSequence s;
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(GetParam());
        EXPECT_CALL(ops, pread(_, _, _, _, _)).WillOnce(Return(COUCHSTORE_ERROR_READ));
        LocalDoc* ldoc;
        EXPECT_EQ(COUCHSTORE_ERROR_READ,
                  couchstore_open_local_document(db, &id[0],
                                                 strlen(&id[0]), &ldoc));
    }
}
INSTANTIATE_TEST_CASE_P(Parameterised, LocalDocFileRead,
                        ::testing::Range(0, 2),
                        ::testing::PrintToStringParamName());

typedef ParameterisedFileOpsErrorInjectionTest CompactSourceRead;
TEST_P(CompactSourceRead, fail) {
    open_db_and_populate(COUCHSTORE_OPEN_FLAG_CREATE, 1);
    {
        InSequence s;
        EXPECT_CALL(ops, pread(_, _, _, _, _)).Times(GetParam());
        EXPECT_CALL(ops, pread(_, _, _, _, _)).WillOnce(Return(COUCHSTORE_ERROR_READ));
        EXPECT_EQ(COUCHSTORE_ERROR_READ,
                  couchstore_compact_db_ex(db, compactPath.c_str(),
                                           COUCHSTORE_COMPACT_FLAG_UNBUFFERED,
                                           nullptr, nullptr, nullptr, &ops));

    }
}
INSTANTIATE_TEST_CASE_P(Parameterised, CompactSourceRead,
                        ::testing::Range(0, 4),
                        ::testing::PrintToStringParamName());

typedef ParameterisedFileOpsErrorInjectionTest CompactTargetWrite;
TEST_P(CompactTargetWrite, fail) {
    open_db_and_populate(COUCHSTORE_OPEN_FLAG_CREATE, 1);
    {
        InSequence s;
        EXPECT_CALL(ops, pwrite(_, _, _, _, _)).Times(GetParam());
        EXPECT_CALL(ops, pwrite(_, _, _, _, _)).WillOnce(Return(COUCHSTORE_ERROR_WRITE));
        EXPECT_EQ(COUCHSTORE_ERROR_WRITE,
                  couchstore_compact_db_ex(db, compactPath.c_str(),
                                           COUCHSTORE_COMPACT_FLAG_UNBUFFERED,
                                           nullptr, nullptr, nullptr, &ops));

    }
}
INSTANTIATE_TEST_CASE_P(Parameterised, CompactTargetWrite,
                        ::testing::Range(0, 6),
                        ::testing::PrintToStringParamName());
