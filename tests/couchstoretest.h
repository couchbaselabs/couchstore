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
#pragma once

#include <gtest/gtest.h>
#include <libcouchstore/couch_db.h>
#include <string>
/*
    CouchstoreTest
        * Global test class for most of the couchstore tests.
        * Auto-cleans when the test is complete.
          a) If db is not null, closes the db
          b) removes testfile.couch.
*/
class CouchstoreTest : public ::testing::Test {
protected:
    CouchstoreTest();

    virtual ~CouchstoreTest();

    Db* db;
    std::string filePath;
};
