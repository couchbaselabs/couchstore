/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <assert.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <libcouchstore/couch_index.h>
#include "bitfield.h"
#include "collate_json.h"
#include "couch_btree.h"
#include "internal.h"
#include "tree_writer.h"
#include "util.h"


/* Private data of the CouchStoreIndex structure */
struct _CouchStoreIndex {
    tree_file file;
    uint32_t root_count;
    node_pointer** roots;
};


static void view_reduce(char *dst, size_t *size_r, nodelist *leaflist, int count);
static void view_rereduce(char *dst, size_t *size_r, nodelist *leaflist, int count);


LIBCOUCHSTORE_API
couchstore_error_t couchstore_create_index(const char *filename,
                                           CouchStoreIndex** index)
{
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;

    CouchStoreIndex* file = calloc(1, sizeof(*file));
    error_unless(file != NULL, COUCHSTORE_ERROR_ALLOC_FAIL);
    error_pass(tree_file_open(&file->file, filename, O_CREAT, couch_get_default_file_ops()));
    *index = file;
cleanup:
    return errcode;
}


static couchstore_error_t write_index_header(CouchStoreIndex* index)
{
    // Header consists of a 32-bit root count, followed by the roots.
    // Each root has a 16-bit size followed by its data (pointer, subtree size, reduce value.)
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;

    // Compute header size and allocate a buffer:
    sized_buf writebuf = {NULL, 4};
    for (uint32_t i = 0; i < index->root_count; ++i) {
        writebuf.size += 2 + encode_root(NULL, index->roots[i]);
    }
    writebuf.buf = calloc(1, writebuf.size);
    error_unless(writebuf.buf, COUCHSTORE_ERROR_ALLOC_FAIL);

    // Write the root count:
    char* dst = writebuf.buf;
    set_bits(dst, 0, 32, index->root_count);
    dst += 4;
    
    // Write the roots:
    for (uint32_t i = 0; i < index->root_count; ++i) {
        size_t rootSize = encode_root(dst + 2, index->roots[i]);
        set_bits(dst, 0, 16, rootSize);
        dst += 2 + rootSize;
    }

    // Write the header to the file, on a 4k block boundary:
    off_t pos;
    error_pass(db_write_header(&index->file, &writebuf, &pos));
    
cleanup:
    free(writebuf.buf);
    return errcode;
}


LIBCOUCHSTORE_API
couchstore_error_t couchstore_close_index(CouchStoreIndex* index)
{
    couchstore_error_t errcode = write_index_header(index);

    for (uint32_t i = 0; i < index->root_count; ++i) {
        free(index->roots[i]);
    }
    free(index->roots);
    
    tree_file_close(&index->file);

    memset(index, 0xa5, sizeof(*index));
    free(index);
    
    return errcode;
}


/////// INDEXING:


static inline sized_buf getJSONKey(sized_buf buf) {
    // Primary index key starts with 16bit length followed by JSON key string (see view_format.md)
    assert(buf.size >= 2);
    sized_buf key = {buf.buf + 2, get_16(buf.buf)};
    assert(key.size > 0 && key.size < buf.size - 2);
    return key;
}

static int keyCompare(const sized_buf *k1, const sized_buf *k2) {
    return CollateJSON(getJSONKey(*k1), getJSONKey(*k2), kCollateJSON_Unicode);
}


couchstore_error_t couchstore_index_add(const char *inputPath, CouchStoreIndex* index) {
    couchstore_error_t errcode;
    TreeWriter* treeWriter = NULL;
    node_pointer* rootNode = NULL;

    error_pass(TreeWriterOpen(inputPath, &keyCompare, view_reduce, view_rereduce, &treeWriter));
    error_pass(TreeWriterSort(treeWriter));
    error_pass(TreeWriterWrite(treeWriter, &index->file, &rootNode));

    node_pointer** roots;
    if (index->roots) {
        roots = realloc(index->roots, (index->root_count + 1) * sizeof(node_pointer*));
    } else {
        roots = malloc(sizeof(node_pointer*));
    }
    error_unless(roots, COUCHSTORE_ERROR_ALLOC_FAIL);
    roots[index->root_count++] = rootNode;
    index->roots = roots;
    rootNode = NULL;  // don't free it

cleanup:
    free(rootNode);
    TreeWriterFree(treeWriter);
    return errcode;
}


//////// REDUCE FUNCTIONS:


// See view_format.md for a description of the formats of tree nodes and reduce values.


#define VBUCKETS_MAX 1024

typedef struct {
    uint64_t chunks[VBUCKETS_MAX / 64];
} VBucketMap;

static void VBucketMap_SetBit(VBucketMap* map, unsigned bucketID) {
    assert(bucketID < VBUCKETS_MAX);
    if (bucketID < VBUCKETS_MAX) {
        map->chunks[(VBUCKETS_MAX - 1 - bucketID) / 64] |= (1LLU << (bucketID % 64));
    }
}

static void VBucketMap_Union(VBucketMap* map, const VBucketMap* src) {
    for (unsigned i = 0; i < VBUCKETS_MAX / 64; ++i)
        map->chunks[i] |= src->chunks[i];
}


static void view_reduce (char *dst, size_t *size_r, nodelist *leaflist, int count)
{
    uint64_t subtreeCount = 0;
    VBucketMap* subtreeBitmap = (VBucketMap*)(dst + 5);
    *size_r = 5 + sizeof(VBucketMap);
    memset(dst, 0, *size_r);

    for (nodelist *i = leaflist; i != NULL && count > 0; i = i->next, count--) {
        assert(i->data.size >= 5);
        unsigned bucketID = get_16(i->data.buf);
        VBucketMap_SetBit(subtreeBitmap, bucketID);

        // Count the emitted values. Each is prefixed with its length.
        const char* pos = i->data.buf + sizeof(uint16_t);
        const char* end = i->data.buf + i->data.size;
        while (pos < end) {
            ++subtreeCount;
            pos += 3 + get_24(pos);
        }
        assert(pos == end);
    }

    set_bits(dst, 0, 40, subtreeCount);
}


static void view_rereduce (char *dst, size_t *size_r, nodelist *ptrlist, int count)
{
    uint64_t subtreeCount = 0;
    VBucketMap* subtreeBitmap = (VBucketMap*)(dst + 5);
    *size_r = 5 + sizeof(VBucketMap);
    memset(dst, 0, *size_r);

    for (nodelist *i = ptrlist; i != NULL && count > 0; i = i->next, count--) {
        assert(i->pointer->reduce_value.size == 5 + sizeof(VBucketMap));
        const char* reduce_value = i->pointer->reduce_value.buf;
        subtreeCount += get_40(reduce_value);

        const VBucketMap* srcMap = (const VBucketMap*)(reduce_value + 5);
        VBucketMap_Union(subtreeBitmap, srcMap);
    }

    set_bits(dst, 0, 40, subtreeCount);
}
