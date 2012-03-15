#ifndef COUCH_COMMON_H
#define COUCH_COMMON_H
#include <unistd.h>
#include <stdint.h>

#define COUCH_BLOCK_SIZE 4096
#define COUCH_DISK_VERSION 9
#define COUCH_SNAPPY_THRESHOLD 64

typedef struct _sized_buf {
    char *buf;
    size_t size;
} sized_buf;

typedef struct _nodepointer {
    sized_buf key;
    uint64_t pointer;
    sized_buf reduce_value;
    uint64_t subtreesize;
} node_pointer;

typedef struct _doc {
    sized_buf id;
    sized_buf data;
} Doc;

typedef struct _docinfo {
    sized_buf id;
    uint64_t db_seq;
    uint64_t rev_seq;
    sized_buf rev_meta;
    int deleted;
    uint8_t content_meta;
    uint64_t bp;
    size_t size;
} DocInfo;

#define DOC_INFO_INITIALIZER { {0, 0}, 0, 0, {0, 0}, 0, 0, 0, 0 }

//Content Meta Flags
#define COUCH_DOC_IS_COMPRESSED 128
//Content Type Reasons (content_meta & 0x0F)
//Document is valid JSON data
#define COUCH_DOC_IS_JSON 0
//Document was checked, and was not valid JSON
#define COUCH_DOC_INVALID_JSON 1
//Document was checked, and contained reserved keys, was not inserted as JSON.
#define COUCH_DOC_INVALID_JSON_KEY 2
//Document was not checked (DB running in non-JSON mode)
#define COUCH_DOC_NON_JSON_MODE 3

typedef struct _local_doc {
    sized_buf id;
    sized_buf json;
    int deleted;
} LocalDoc;

typedef struct _db_header {
    uint64_t disk_version;
    uint64_t update_seq;
    node_pointer *by_id_root;
    node_pointer *by_seq_root;
    node_pointer *local_docs_root;
    uint64_t purge_seq;
    sized_buf *purged_docs;
    uint64_t position;
} db_header;

typedef struct _db Db;

typedef struct {
    ssize_t (*pread)(Db *db, void *buf, size_t nbyte, off_t offset);
    ssize_t (*pwrite)(Db *db, const void *buf, size_t nbyte, off_t offset);
    int (*open)(const char *path, int oflag, int mode);
    int (*close)(Db *db);

    off_t (*goto_eof)(Db *db);

    int (*sync)(Db *db);
} couch_file_ops;

struct _db {
    int fd;
    uint64_t file_pos;
    couch_file_ops *file_ops;
    db_header header;
    void *userdata;
};

/* File ops

//Read a chunk from file, remove block prefixes, and decompress.
//Don't forget to free when done with the returned value.
//(If it returns -1 it will not have set ret_ptr, no need to free.) */
int pread_bin(Db *db, off_t pos, char **ret_ptr);
int pread_compressed(Db *db, off_t pos, char **ret_ptr);

int pread_header(Db *db, off_t pos, char **ret_ptr);

ssize_t total_read_len(off_t blockoffset, ssize_t finallen);

int db_write_header(Db *db, sized_buf *buf, off_t *pos);
int db_write_buf(Db *db, sized_buf *buf, off_t *pos);
int db_write_buf_compressed(Db *db, sized_buf *buf, off_t *pos);

/* Errors */

#define ERROR_OPEN_FILE -1
#define ERROR_PARSE_TERM -2
#define ERROR_ALLOC_FAIL -3
#define ERROR_READ -4
#define DOC_NOT_FOUND -5
#define ERROR_NO_HEADER -6
#define ERROR_WRITE -7
#define ERROR_HEADER_VERSION -8
#define ERROR_CHECKSUM_FAIL -9

#endif
