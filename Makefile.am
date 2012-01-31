# libcouchbase
# Copyright (C) 2010, 2011 Couchbase, Inc
# All rights reserved.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
ACLOCAL_AMFLAGS = -I m4 --force

bin_PROGRAMS = couch_dbdump
EXTRA_DIST = LICENSE README.md src/rfc1321

pkginclude_HEADERS = \
                     include/libcouchstore/couch_db.h \
                     include/libcouchstore/couch_common.h

lib_LTLIBRARIES = libcouchstore.la

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = libcouchstore.pc

libcouchstore_la_SOURCES = \
                            src/btree_modify.c \
                            src/btree_read.c \
                            src/couch_btree.h \
                            src/couch_common.h \
                            src/couch_db.c \
                            src/crc32.c \
                            src/couch_db.h \
                            src/couch_file_read.c \
                            src/couch_file_write.c \
                            src/reduces.c \
                            src/reduces.h \
                            src/rfc1321/md5c.c \
                            src/rfc1321/md5.h \
                            src/rfc1321/global.h \
                            src/util.c \
                            src/util.h

libcouchstore_la_LDFLAGS = -version-info $(LIBCOUCHSTORE_API_CURRENT):$(LIBCOUCHSTORE_API_REVISION):$(LIBCOUCHSTORE_API_AGE) -no-undefined $(ERL_INTERFACE_DIR_LIB) -lei -lsnappy
libcouchstore_la_CFLAGS = $(ERL_INTERFACE_DIR_INCLUDE)

couch_dbdump_SOURCES = src/dbdump.c
couch_dbdump_DEPENDENCIES = libcouchstore.la
couch_dbdump_LDADD = libcouchstore.la

#writetest_SOURCES = src/writetest.c
#writetest_DEPENDENCIES = libcouchstore.la
#writetest_LDADD = libcouchstore.la

#tests
check_PROGRAMS = testapp
TESTS = ${check_PROGRAMS}

testapp_SOURCES = tests/testapp.c
testapp_DEPENDENCIES = libcouchstore.la
testapp_LDADD = libcouchstore.la

test: check-TESTS

