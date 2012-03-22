/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef LIBCOUCHSTORE_FILE_OPS_H
#define LIBCOUCHSTORE_FILE_OPS_H

#ifndef COUCHSTORE_COUCH_DB_H
#error "You should include <libcouchstore/couch_db.h> instead"
#endif

#ifdef __cplusplus
extern "C" {
#endif

    typedef struct {
        /**
         * Version number that describes the layout of the structure. Should be set
         * to 1.
         */
        uint64_t version;

        /**
         * Open a file named file.
         *
         * @param db the database instance to open the file
         * @param path the name of the file
         * @param flags flags as specified by open(2)
         * @return COUCHSTORE_SUCCESS upon success.
         */
        couchstore_error_t (*open)(Db *db, const char *path, int oflag);

        /**
         * Close this database handle.
         *
         * @param db database to close
         */
        void (*close)(Db *db);


        /**
         * Read a chunk of data from a given offset in the file.
         *
         * @param db database to read from
         * @param buf where to store data
         * @param nbyte number of bytes to read
         * @param offset where to read from
         * @return number of bytes read
         */
        ssize_t (*pread)(Db *db, void *buf, size_t nbyte, off_t offset);

        /**
         * Write a chunk of data to a given offset in the file.
         * @param db database to write to
         * @param buf where to read data
         * @param nbyte number of bytes to write
         * @param offset where to write to
         * @return number of bytes written
         */
        ssize_t (*pwrite)(Db *db, const void *buf, size_t nbyte, off_t offset);

        /**
         * Move to the end of the file.
         *
         * @param db database to move the filepointer in
         * @return the offset (from beginning of the file)
         */
        off_t (*goto_eof)(Db *db);

        /**
         * Flush the buffers to disk
         *
         * @param db database handle to flush
         * @return COUCHSTORE_SUCCESS upon success
         */
        couchstore_error_t (*sync)(Db *db);

        /**
         * Called as part of shutting down the db instance this instance was
         * passed to. A hook to for releasing allocated resources
         *
         * @param db the instance being killed
         */
        void (*destructor)(Db *db);
    } couch_file_ops;

    LIBCOUCHSTORE_API
    void libcouchstore_set_file_ops_cookie(Db *db, void *data);

    LIBCOUCHSTORE_API
    void* libcouchstore_get_file_ops_cookie(Db *db);

#ifdef __cplusplus
}
#endif

#endif
