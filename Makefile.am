# libcouchstore
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

bin_PROGRAMS = couch_dbdump couch_dbinfo couch_compact
EXTRA_DIST = LICENSE README.md

pkginclude_HEADERS = \
                     include/libcouchstore/couch_db.h \
                     include/libcouchstore/couch_common.h \
                     include/libcouchstore/error.h \
                     include/libcouchstore/file_ops.h \
                     include/libcouchstore/visibility.h

lib_LTLIBRARIES = libcouchstore.la

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = libcouchstore.pc

noinst_LTLIBRARIES = librfc1321.la libbyteswap.la

libbyteswap_la_SOURCES = src/byteswap.c

librfc1321_la_SOURCES = \
                            src/rfc1321/md5c.c \
                            src/rfc1321/md5.h \
                            src/rfc1321/global.h
librfc1321_la_CFLAGS =  $(AM_CFLAGS) ${NO_WERROR}

libcouchstore_la_SOURCES = \
                            config_static.h \
                            src/arena.c \
                            src/arena.h \
                            src/btree_modify.c \
                            src/btree_read.c \
                            src/couch_btree.h \
                            src/couch_db.c \
                            src/couch_save.c \
                            src/crc32.c \
                            src/crc32.h \
                            src/couch_file_read.c \
                            src/couch_file_write.c \
                            src/db_compact.c \
                            src/fatbuf.h \
                            src/internal.h \
                            src/iobuffer.c \
                            src/iobuffer.h \
                            src/llmsort.c \
                            src/mergesort.c \
                            src/mergesort.h \
                            src/reduces.c \
                            src/reduces.h \
                            src/strerror.c \
                            src/os.c \
                            src/util.c \
                            src/util.h


libcouchstore_la_LDFLAGS = $(AM_LDFLAGS) -version-info $(LIBCOUCHSTORE_API_CURRENT):$(LIBCOUCHSTORE_API_REVISION):$(LIBCOUCHSTORE_API_AGE) -no-undefined -lsnappy

if WINDOWS
libcouchstore_la_SOURCES += win32/win32.c
libcouchstore_la_LDFLAGS += -lws2_32
endif

libcouchstore_la_CFLAGS = $(AM_CFLAGS) -DLIBCOUCHSTORE_INTERNAL=1
libcouchstore_la_LIBADD = librfc1321.la libbyteswap.la

couch_dbdump_SOURCES = src/dbdump.c
couch_dbdump_DEPENDENCIES = libcouchstore.la
couch_dbdump_CFLAGS = $(AM_CFLAGS) -D__STDC_FORMAT_MACROS
couch_dbdump_LDADD = libcouchstore.la libbyteswap.la -lsnappy

if WINDOWS
couch_dbdump_SOURCES += win32/win32.c
couch_dbdump_LDADD += -lws2_32
endif

couch_dbinfo_SOURCES = src/dbinfo.c src/util.c
couch_dbinfo_CFLAGS = $(AM_CFLAGS) -D__STDC_FORMAT_MACROS
couch_dbinfo_DEPENDENCIES = libcouchstore.la libbyteswap.la
couch_dbinfo_LDADD = libcouchstore.la libbyteswap.la

couch_compact_SOURCES = src/compactor.c
couch_compact_DEPENDENCIES = libcouchstore.la
couch_compact_CFLAGS = $(AM_CFLAGS) -D__STDC_FORMAT_MACROS
couch_compact_LDADD = libcouchstore.la libbyteswap.la -lsnappy

extra_tests=
slow_tests=

if HAVE_LIBLUA
bin_PROGRAMS += couchscript

couchscript_CXXFLAGS = $(AM_CXXFLAGS) $(lua_CFLAGS)
couchscript_SOURCES = src/couchscript.cc
couchscript_LDFLAGS = $(AM_LDFLAGS) $(AM_CXX_LDFLAGS)
couchscript_LDADD = libcouchstore.la libbyteswap.la $(LTLIBLUA) -ldl
couchscript_DEPENDENCIES = libcouchstore.la libbyteswap.la
extra_tests += lua_tests

slow_tests += slow_lua_tests

lua_tests: couchscript couch_compact tests/localdoc.lua
	@echo "Running lua tests."
	@./couchscript tests/localdoc.lua
	@./couchscript tests/corrupt.lua
	@./couchscript tests/bulk.lua
	@./couchscript tests/compact.lua

slow_lua_tests: couchscript tests/largefile.lua tests/large.lua
	@echo "Running slow lua tests."
	@./couchscript tests/largefile.lua
	@./couchscript tests/large.lua

endif

#tests

CLEANFILES = test.couch
check_PROGRAMS = testapp
TESTS = ${check_PROGRAMS}

testapp_SOURCES = tests/testapp.c src/util.c tests/macros.h
testapp_CFLAGS = $(AM_CFLAGS)
testapp_DEPENDENCIES = libcouchstore.la libbyteswap.la
testapp_LDADD = libcouchstore.la libbyteswap.la

test: check-TESTS $(extra_tests)

slow_test: $(slow_tests)

VALGRIND=valgrind
VALGRIND_OPTIONS=--tool=Memcheck --num-callers=50 \
                 --error-limit=yes --track-origins=yes --malloc-fill=55 \
                 --free-fill=aa --dsymutil=yes -q

VALGRIND_TEST=.libs/testapp

valgrind: ${check_PROGRAMS}
	@for f in $(VALGRIND_TEST); \
        do \
           echo Running Valgrind on $$f; \
           LD_LIBRARY_PATH=`pwd`/.libs \
           DYLD_LIBRARY_PATH=`pwd`/.libs \
           $(VALGRIND) $(VALGRIND_OPTIONS) --log-file=`basename $$f`.log $$f; \
        done

reformat:
	astyle --mode=c \
               --quiet \
               --style=1tbs \
               --indent=spaces=4 \
               --indent-namespaces \
               --indent-col1-comments \
               --max-instatement-indent=78 \
               --pad-oper \
               --pad-header \
               --add-brackets \
               --align-pointer=name \
               --align-reference=name \
               $(top_srcdir)/include/libcouchstore/*.h \
               $(top_srcdir)/src/*.[ch] \
               $(top_srcdir)/src/*.cc \
               $(top_srcdir)/tests/*.[ch]
