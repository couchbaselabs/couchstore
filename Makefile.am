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

bin_PROGRAMS = \
	couch_dbdump \
	couch_dbinfo \
	couch_compact \
	couch_view_file_sorter \
	couch_view_file_merger \
	couch_view_index_builder
EXTRA_DIST = python LICENSE README.md

if WINDOWS
ICU_LOCAL_LIBS=-licuuc -licudt -licuin
else
ICU_LOCAL_LIBS=-licuuc -licudata -licui18n
endif

pkginclude_HEADERS = \
                     include/libcouchstore/couch_db.h \
                     include/libcouchstore/couch_index.h \
                     include/libcouchstore/couch_common.h \
                     include/libcouchstore/error.h \
                     include/libcouchstore/file_ops.h \
                     include/libcouchstore/visibility.h

lib_LTLIBRARIES = libcouchstore.la

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = libcouchstore.pc

noinst_LTLIBRARIES = librfc1321.la libbyteswap.la libcouchstore_tests.la libmapreduce.la libutil.la

libbyteswap_la_SOURCES = src/byteswap.c
libutil_la_SOURCES = src/util.cc src/util.h

librfc1321_la_SOURCES = \
                            src/rfc1321/md5c.c \
                            src/rfc1321/md5.h \
                            src/rfc1321/global.h
librfc1321_la_CFLAGS =  $(AM_CFLAGS) ${NO_WERROR}

libmapreduce_la_CXXFLAGS = $(AM_CXXFLAGS) -DLIBMAPREDUCE_INTERNAL=1
libmapreduce_la_SOURCES = \
	src/views/mapreduce/mapreduce.h \
	src/views/mapreduce/mapreduce_internal.h \
	src/views/mapreduce/visibility.h \
	src/views/mapreduce/mapreduce_c.cc \
	src/views/mapreduce/mapreduce.cc
libmapreduce_la_LDFLAGS = $(AM_LDFLAGS) $(AM_CXX_LDFLAGS) $(V8_LIB_FLAGS) -pthread
libmapreduce_la_LIBADD = $(V8_FLAGS)

BASE_libcouchstore_la_CFLAGS = $(AM_CFLAGS) $(ICU_LOCAL_CFLAGS)
if GCC
BASE_libcouchstore_la_CFLAGS += -Wstrict-aliasing=2 -Werror=strict-aliasing -pedantic
endif

libcouchstore_la_SOURCES = \
                            config_static.h \
                            src/arena.cc \
                            src/arena.h \
                            src/bitfield.h \
                            src/btree_modify.cc \
                            src/btree_read.cc \
                            src/couch_btree.h \
                            src/couch_db.cc \
                            src/couch_file_read.cc \
                            src/couch_file_write.cc \
                            src/couch_save.cc \
                            src/crc32.c \
                            src/crc32.h \
                            src/db_compact.cc \
                            src/fatbuf.h \
                            src/file_merger.cc \
                            src/file_merger.h \
                            src/file_name_utils.c \
                            src/file_name_utils.h \
                            src/file_sorter.cc \
                            src/file_sorter.h \
                            src/internal.h \
                            src/iobuffer.cc \
                            src/iobuffer.h \
                            src/llmsort.cc \
                            src/mergesort.cc \
                            src/mergesort.h \
                            src/node_types.cc \
                            src/node_types.h \
                            src/reduces.cc \
                            src/reduces.h \
                            src/strerror.cc \
                            src/tree_writer.cc \
                            src/tree_writer.h \
                            src/util.h \
                            src/views/collate_json.c \
                            src/views/collate_json.h \
                            src/views/file_sorter.h \
                            src/views/file_sorter.c \
                            src/views/file_merger.h \
                            src/views/file_merger.c \
                            src/views/bitmap.h \
                            src/views/bitmap.c \
                            src/views/index_header.h \
                            src/views/index_header.c \
                            src/views/sorted_list.h \
                            src/views/sorted_list.c \
                            src/views/reducers.h \
                            src/views/reductions.h \
                            src/views/reductions.c \
                            src/views/keys.h \
                            src/views/keys.c \
                            src/views/values.h \
                            src/views/values.c \
                            src/views/util.h \
                            src/views/util.c \
                            src/views/reducers.h \
                            src/views/reducers.c \
                            src/views/view_group.h \
                            src/views/view_group.c \
                            src/views/spatial.h \
                            src/views/spatial.c


libcouchstore_la_LDFLAGS = $(AM_LDFLAGS) $(ICU_LOCAL_LDFLAGS) -version-info $(LIBCOUCHSTORE_API_CURRENT):$(LIBCOUCHSTORE_API_REVISION):$(LIBCOUCHSTORE_API_AGE) -no-undefined

if WINDOWS
libcouchstore_la_SOURCES += src/os_win.c
libcouchstore_la_LDFLAGS += -lws2_32
else
libcouchstore_la_SOURCES += src/os.c
endif

libcouchstore_la_CFLAGS = $(BASE_libcouchstore_la_CFLAGS)
libcouchstore_la_CPPFLAGS = ${AM_CPPFLAGS} -DLIBCOUCHSTORE_INTERNAL=1
libcouchstore_la_DEPENDENCIES =  librfc1321.la libbyteswap.la libmapreduce.la libutil.la
libcouchstore_la_LIBADD = librfc1321.la libbyteswap.la libmapreduce.la libutil.la $(ICU_LOCAL_LIBS) -lstdc++ -lsnappy -lpthread

libcouchstore_tests_la_SOURCES = $(libcouchstore_la_SOURCES)
libcouchstore_tests_la_LDFLAGS =  $(AM_LDFLAGS) $(ICU_LOCAL_LDFLAGS) --no-undefined
libcouchstore_tests_la_CFLAGS = $(BASE_libcouchstore_la_CFLAGS)
libcouchstore_tests_la_LIBADD = $(libcouchstore_la_LIBADD)

couch_dbdump_SOURCES = src/dbdump.c
couch_dbdump_DEPENDENCIES = libcouchstore.la
couch_dbdump_CFLAGS = $(AM_CFLAGS) -D__STDC_FORMAT_MACROS
couch_dbdump_LDADD = libcouchstore.la libbyteswap.la -lsnappy

if WINDOWS
couch_dbdump_LDADD += -lws2_32
endif

couch_dbinfo_SOURCES = src/dbinfo.c
couch_dbinfo_CFLAGS = $(AM_CFLAGS) -D__STDC_FORMAT_MACROS
couch_dbinfo_DEPENDENCIES = libcouchstore.la libbyteswap.la libutil.la
couch_dbinfo_LDADD = libcouchstore.la libbyteswap.la libutil.la

couch_compact_SOURCES = src/compactor.c
couch_compact_DEPENDENCIES = libcouchstore.la
couch_compact_CFLAGS = $(AM_CFLAGS) -D__STDC_FORMAT_MACROS
couch_compact_LDADD = libcouchstore.la libbyteswap.la -lsnappy

couch_view_file_sorter_SOURCES = src/views/bin/couch_view_file_sorter.c
couch_view_file_sorter_DEPENDENCIES = libcouchstore.la
couch_view_file_sorter_CFLAGS = $(AM_CFLAGS) -D__STDC_FORMAT_MACROS
couch_view_file_sorter_LDADD = libcouchstore.la libbyteswap.la -lsnappy

if WINDOWS
couch_view_file_sorter_LDADD += -lws2_32
endif

couch_view_file_merger_SOURCES = src/views/bin/couch_view_file_merger.c
couch_view_file_merger_DEPENDENCIES = libcouchstore.la
couch_view_file_merger_CFLAGS = $(AM_CFLAGS) -D__STDC_FORMAT_MACROS
couch_view_file_merger_LDADD = libcouchstore.la libbyteswap.la -lsnappy

if WINDOWS
couch_view_file_merger_LDADD += -lws2_32
endif

couch_view_index_builder_SOURCES = src/views/bin/couch_view_index_builder.c
couch_view_index_builder_DEPENDENCIES = libcouchstore.la
couch_view_index_builder_CFLAGS = $(AM_CFLAGS) -D__STDC_FORMAT_MACROS
couch_view_index_builder_LDADD = libcouchstore.la libbyteswap.la -lsnappy

if WINDOWS
couch_view_index_builder_LDADD += -lws2_32
endif

extra_tests=
slow_tests=

if HAVE_LIBLUA
bin_PROGRAMS += couchscript

couchscript_CXXFLAGS = $(AM_CXXFLAGS) $(lua_CFLAGS)
couchscript_SOURCES = src/couchscript.cc
couchscript_LDFLAGS = $(AM_LDFLAGS) $(AM_CXX_LDFLAGS)
couchscript_LDADD = libcouchstore.la libbyteswap.la $(LTLIBLUA) -ldl
couchscript_DEPENDENCIES = libcouchstore.la libbyteswap.la
extra_tests += lua_tests python_tests

slow_tests += slow_lua_tests

lua_tests: couchscript couch_compact tests/localdoc.lua
	@echo "Running lua tests."
	@./couchscript tests/localdoc.lua
	@./couchscript tests/corrupt.lua
	@./couchscript tests/bulk.lua
	@./couchscript tests/changessincefilter.lua
	@./couchscript tests/compact.lua
	@./couchscript tests/dropdel.lua

slow_lua_tests: couchscript tests/largefile.lua tests/large.lua
	@echo "Running slow lua tests."
	@./couchscript tests/largefile.lua
	@./couchscript tests/large.lua

endif

python_pythonlibdir = $(libdir)/python
python_pythonlib_DATA = python/couchstore.py


python_tests: python/*.py
	@LD_LIBRARY_PATH=`pwd`/.libs \
		DYLD_LIBRARY_PATH=`pwd`/.libs \
		python python/couchstore_test.py
	@LD_LIBRARY_PATH=`pwd`/.libs \
		DYLD_LIBRARY_PATH=`pwd`/.libs \
                PYTHONPATH=${PYTHONPATH}:`pwd`/python \
		python tests/purge.py

#tests

CLEANFILES = test.couch
check_PROGRAMS = testapp
TESTS = ${check_PROGRAMS}

testapp_SOURCES = tests/testapp.c tests/macros.h
testapp_SOURCES += \
	tests/file_merger_tests.c \
	tests/file_sorter_tests.c \
	tests/mapreduce/mapreduce_tests.h \
	tests/mapreduce/tests.c \
	tests/mapreduce/map.c \
	tests/mapreduce/reduce.c \
	tests/mapreduce/builtin.c \
	tests/views/view_tests.h \
	tests/views/tests.c \
	tests/views/collate_json_test.c \
	tests/views/index_headers.c \
	tests/views/bitmaps.c \
	tests/views/sorted_lists.c \
	tests/views/reductions.c \
	tests/views/keys.c \
	tests/views/values.c \
	tests/views/reducers.c \
	tests/views/spatial_tests.h \
	tests/views/spatial.c \
	tests/btree_purge/purge_tests.h \
	tests/btree_purge/tests.c \
	tests/btree_purge/purge.c


testapp_CFLAGS = $(AM_CFLAGS)
testapp_DEPENDENCIES = libcouchstore_tests.la libbyteswap.la libutil.la
testapp_LDFLAGS = $(AM_LDFLAGS) $(ICU_LOCAL_LDFLAGS)
testapp_LDADD = libcouchstore_tests.la libbyteswap.la libmapreduce.la libutil.la -lstdc++

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

if WINDOWS
install-data-hook:
	$(INSTALL) $(python_pythonlib_DATA) $(bindir)
	$(INSTALL) $(ICU_LIB_DIR)/icuuc50.dll $(bindir)
	$(INSTALL) $(ICU_LIB_DIR)/icudt50.dll $(bindir)
	$(INSTALL) $(ICU_LIB_DIR)/icuin50.dll $(bindir)
endif
