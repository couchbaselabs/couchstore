/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef LIBCOUCHSTORE_INTERNAL_H
#define LIBCOUCHSTORE_INTERNAL_H 1

/*
 * This file contains datastructures and prototypes for functions only to
 * be used by the internal workings of libcoucstore. If you for some reason
 * need access to them from outside the library, you should write a
 * function to give you what you need.
 */

#include <libcouchstore/couch_db.h>

#define COUCH_BLOCK_SIZE 4096
#define COUCH_DISK_VERSION 10
#define COUCH_SNAPPY_THRESHOLD 64

#ifdef __cplusplus
extern "C" {
#endif

    typedef struct _nodepointer {
        sized_buf key;
        uint64_t pointer;
        sized_buf reduce_value;
        uint64_t subtreesize;
    } node_pointer;

    typedef struct _db_header {
        uint64_t disk_version;
        uint64_t update_seq;
        node_pointer *by_id_root;
        node_pointer *by_seq_root;
        node_pointer *local_docs_root;
        uint64_t purge_seq;
        uint64_t purge_ptr;
        uint64_t position;
    } db_header;

    struct _db {
        uint64_t file_pos;
        couch_file_ops *file_ops;
        void *file_ops_cookie;
        db_header header;
        void *userdata;
    };

    couch_file_ops *couch_get_default_file_ops(void);

    /* File ops
     *  Read a chunk from file, remove block prefixes, and decompress.
     *  Don't forget to free when done with the returned value.
     *  (If it returns -1 it will not have set ret_ptr, no need to free.)
    */
    int pread_bin(Db *db, off_t pos, char **ret_ptr);
    int pread_compressed(Db *db, off_t pos, char **ret_ptr);

    int pread_header(Db *db, off_t pos, char **ret_ptr);

    ssize_t total_read_len(off_t blockoffset, ssize_t finallen);

    couchstore_error_t db_write_header(Db *db, sized_buf *buf, off_t *pos);
    int db_write_buf(Db *db, sized_buf *buf, off_t *pos);
    int db_write_buf_compressed(Db *db, sized_buf *buf, off_t *pos);

    node_pointer *read_root(char *buf, int size);
    void encode_root(char *buf, node_pointer *node);

#ifdef __cplusplus
}
#endif

#endif
