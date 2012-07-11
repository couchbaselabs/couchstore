#ifndef LIBCOUCHSTORE_TREE_WRITER_H
#define LIBCOUCHSTORE_TREE_WRITER_H

#include <libcouchstore/couch_db.h>
#include "couch_btree.h"


typedef struct TreeWriter TreeWriter;


/**
 * Creates a new TreeWriter.
 * @param unsortedFilePath If non-NULL, the path to an existing file containing a series of unsorted
 * key/value pairs in TreeWriter format. If NULL, an empty TreeWriter will be created (using a
 * temporary file for the external sorting.)
 * @param key_compare Callback function that compares two keys.
 * @param out_writer The new TreeWriter pointer will be stored here.
 * @return Error code or COUCHSTORE_SUCCESS.
 */
couchstore_error_t TreeWriterOpen(const char* unsortedFilePath,
                                  compare_callback key_compare,
                                  TreeWriter** out_writer);

/**
 * Frees a TreeWriter instance. It is safe to pass a NULL pointer.
 */
void TreeWriterFree(TreeWriter* writer);

/**
 * Adds a key/value pair to a TreeWriter. These can be added in any order.
 */
couchstore_error_t TreeWriterAddItem(TreeWriter* writer, sized_buf key, sized_buf value);

/**
 * Sorts the key/value pairs already added.
 * The keys are sorted by ebin_cmp (basic lexicographic order by byte values).
 * If this TreeWriter was opened on an existing data file, the contents of the file will be sorted.
 */
couchstore_error_t TreeWriterSort(TreeWriter* writer);

/**
 * Writes the key/value pairs to the by-ID tree of the target database.
 * The items should first have been sorted.
 */
couchstore_error_t TreeWriterWrite(TreeWriter* writer, Db* target);


/*
 * The input file format for TreeWriterOpen is as follows:
 * The file is binary and consists of nothing more than a series of records.
 * Each record looks like this:
 *     2 bytes: Key length (native byte order)
 *     4 bytes: Value length (native byte order)
 *     <key bytes>
 *     <value bytes>
 */

#endif // LIBCOUCHSTORE_TREE_WRITER_H
