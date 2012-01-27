cmake_minimum_required (VERSION 2.8)

project (COUCHSTORE)

include_directories(${EI_INCLUDE_DIR} ${COUCHSTORE_SOURCE_DIR}/src/snappy)
link_directories(${EI_LIBPATH})

add_library (couchstore
                src/couch_db.c
                src/reduces.c
                src/couch_file_read.c
                src/couch_file_write.c
                src/util.c
                src/btree_modify.c
                src/btree_read.c
                src/rfc1321/md5c.c
                src/snappy/snappy-sinksource.cc
                src/snappy/snappy-stubs-internal.cc
                src/snappy/snappy-c.cc
                src/snappy/snappy.cc)

add_executable (dbdump
                    src/dbdump.c)

add_executable (writetest
                    src/writetest.c)

target_link_libraries (couchstore ei)
target_link_libraries (dbdump couchstore)
target_link_libraries (writetest couchstore)
