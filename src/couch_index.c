/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <assert.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <libcouchstore/couch_index.h>
#include "bitfield.h"
#include "collate_json.h"
#include "couch_btree.h"
#include "internal.h"
#include "json_reduce.h"
#include "node_types.h"
#include "tree_writer.h"
#include "util.h"


#define DST_SIZE 500


// Header consists of a 32-bit root count, followed by the roots.
// Each root has a 1-byte type field (0 for primary, 1 for back-index),
// a 16-bit size, then its data (pointer, subtree size, reduce value.)

typedef struct {
    raw_08 type;
    raw_16 size;
    raw_btree_root root;
} raw_index_file_root;

typedef struct {
    raw_32 rootCount;
    raw_index_file_root firstRoot;
} raw_index_file_header;


/* Private data of the CouchStoreIndex structure */
struct _CouchStoreIndex {
    tree_file file;
    uint32_t back_root_index;
    uint32_t root_count;
    node_pointer** roots;
};


static const JSONReducer* CurrentReducer;
static couchstore_index_type CurrentIndexType;


static void view_reduce(char *dst, size_t *size_r, nodelist *leaflist, int count);
static void view_rereduce(char *dst, size_t *size_r, nodelist *leaflist, int count);


LIBCOUCHSTORE_API
couchstore_error_t couchstore_create_index(const char *filename,
                                           CouchStoreIndex** index)
{
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;

    CouchStoreIndex* file = calloc(1, sizeof(*file));
    error_unless(file != NULL, COUCHSTORE_ERROR_ALLOC_FAIL);
    error_pass(tree_file_open(&file->file, filename, O_RDWR | O_CREAT | O_TRUNC,
                              couchstore_get_default_file_ops()));
    file->back_root_index = UINT32_MAX;
    *index = file;
cleanup:
    return errcode;
}


static couchstore_error_t write_index_header(CouchStoreIndex* index)
{
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;

    // Compute header size and allocate a buffer:
    sized_buf writebuf = {NULL, 4};
    for (uint32_t i = 0; i < index->root_count; ++i) {
        writebuf.size += 3 + encode_root(NULL, index->roots[i]);
    }
    writebuf.buf = calloc(1, writebuf.size);
    error_unless(writebuf.buf, COUCHSTORE_ERROR_ALLOC_FAIL);

    // Write the root count:
    raw_index_file_header* header = (raw_index_file_header*)writebuf.buf;
    header->rootCount = encode_raw32(index->root_count);
    raw_index_file_root* root = &header->firstRoot;
    
    // Write the roots:
    for (uint32_t i = 0; i < index->root_count; ++i) {
        root->type =  encode_raw08((i == index->back_root_index) ? COUCHSTORE_VIEW_BACK_INDEX
                                                                 : COUCHSTORE_VIEW_PRIMARY_INDEX);
        size_t rootSize = encode_root(&root->root, index->roots[i]);
        root->size = encode_raw16((uint16_t)rootSize);
        root = (raw_index_file_root*)((char*)root + 3 + rootSize);
    }
    assert((char*)root - writebuf.buf == (ssize_t)writebuf.size);

    // Write the header to the file, on a 4k block boundary:
    cs_off_t pos;
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


typedef struct {
    raw_16 length;
    char key[1];
} raw_primary_index_key;


typedef struct {
    raw_24 jsonLength;
    char jsonValue[1];
} raw_json_string_value;

typedef struct {
    raw_16 bucketID;
    raw_json_string_value firstValue;
} raw_primary_index_value;


static inline sized_buf getJSONKey(sized_buf buf) {
    // Primary index key starts with 16bit length followed by JSON key string (see view_format.md)
    assert(buf.size >= 2);
    raw_primary_index_key* key_ptr = (raw_primary_index_key*)buf.buf;
    sized_buf key = {key_ptr->key, decode_raw16(key_ptr->length)};
    assert(key.size > 0 && key.size < buf.size - 2);
    return key;
}

static int keyCompare(const sized_buf *k1, const sized_buf *k2) {
    return CollateJSON(getJSONKey(*k1), getJSONKey(*k2), kCollateJSON_Unicode);
}


couchstore_error_t couchstore_index_add(const char *inputPath,
                                        couchstore_index_type index_type,
                                        couchstore_json_reducer reduce_function,
                                        CouchStoreIndex* index)
{
    if (index_type == COUCHSTORE_VIEW_BACK_INDEX && index->back_root_index < UINT32_MAX) {
        return COUCHSTORE_ERROR_INVALID_ARGUMENTS;  // Can only have one back index
    }
    couchstore_error_t errcode;
    TreeWriter* treeWriter = NULL;
    node_pointer* rootNode = NULL;

    CurrentIndexType = index_type;
    CurrentReducer = NULL;
    if (index_type == COUCHSTORE_VIEW_PRIMARY_INDEX) {
        switch (reduce_function) {
            case COUCHSTORE_REDUCE_COUNT:
                CurrentReducer = &JSONCountReducer;
                break;
            case COUCHSTORE_REDUCE_SUM:
                CurrentReducer = &JSONSumReducer;
                break;
            case COUCHSTORE_REDUCE_STATS:
                CurrentReducer = &JSONStatsReducer;
                break;
        }
    }
    
    error_pass(TreeWriterOpen(inputPath,
                              (index_type == COUCHSTORE_VIEW_PRIMARY_INDEX) ? keyCompare : ebin_cmp,
                              view_reduce,
                              view_rereduce,
                              &treeWriter));
    error_pass(TreeWriterSort(treeWriter));
    error_pass(TreeWriterWrite(treeWriter, &index->file, &rootNode));

    // Add new root pointer to the roots array:
    node_pointer** roots;
    if (index->roots) {
        roots = realloc(index->roots, (index->root_count + 1) * sizeof(node_pointer*));
    } else {
        roots = malloc(sizeof(node_pointer*));
    }
    error_unless(roots, COUCHSTORE_ERROR_ALLOC_FAIL);
    if (index_type == COUCHSTORE_VIEW_BACK_INDEX)
        index->back_root_index = index->root_count;
    roots[index->root_count++] = rootNode;
    index->roots = roots;
    rootNode = NULL;  // don't free it in cleanup

cleanup:
    CurrentReducer = NULL;
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


typedef struct {
    raw_16 length;
    char json[1];
} raw_json_reduction;


typedef struct {
    raw_40 subTreeCount;
    char partitionBitmap[sizeof(VBucketMap)];
    raw_json_reduction firstReduction;
} raw_reduce_value;


static void primary_reduce_common(char *dst, size_t *size_r, nodelist *leaflist, int count,
                                  bool rereduce)
{
    // Format of dst is shown in "Primary Index Inner Node Reductions" in view_format.md
    raw_reduce_value* result = (raw_reduce_value*)dst;
    uint64_t subtreeCount = 0;
    VBucketMap* subtreeBitmap = (VBucketMap*)result->partitionBitmap;
    *size_r = 5 + sizeof(VBucketMap);
    memset(dst, 0, *size_r);
    
    sized_buf jsonReduceBuf = {result->firstReduction.json, DST_SIZE - *size_r - 2};
    if (CurrentReducer) {
        CurrentReducer->init(jsonReduceBuf);
    }

    for (nodelist *i = leaflist; i != NULL && count > 0; i = i->next, count--) {
        if (!rereduce) {
            // First-level reduce; i->data is a primary-index Value, as per view_format.md
            assert(i->data.size >= 2);
            const raw_primary_index_value* value = (const raw_primary_index_value*)i->data.buf;
            unsigned bucketID = decode_raw16(value->bucketID);
            VBucketMap_SetBit(subtreeBitmap, bucketID);

            if (CurrentIndexType == COUCHSTORE_VIEW_PRIMARY_INDEX) {
                assert(i->data.size >= 5);
                // i->key is a primary-index key
                sized_buf jsonKey = getJSONKey(i->key);

                // Count the emitted values. Each is prefixed with its length.
                const raw_json_string_value* pos = &value->firstValue;
                const void* end = i->data.buf + i->data.size;
                while ((void*)pos < end) {
                    ++subtreeCount;
                    sized_buf jsonValue = {(char*)pos->jsonValue, decode_raw24(pos->jsonLength)};
                    pos = offsetby(pos, 3 + jsonValue.size);
                    // JSON reduction:
                    if (CurrentReducer) {
                        CurrentReducer->add(jsonReduceBuf, jsonKey, jsonValue);
                    }
                }
                assert(pos == end);
            } else {
                ++subtreeCount;
            }

        } else {
            // Re-reduce:
            const raw_reduce_value* reduce_value = (const raw_reduce_value*) i->pointer->reduce_value.buf;
            subtreeCount += decode_raw40(reduce_value->subTreeCount);

            const VBucketMap* srcMap = (const VBucketMap*)reduce_value->partitionBitmap;
            VBucketMap_Union(subtreeBitmap, srcMap);

            // JSON re-reduction:
            if (CurrentReducer) {
                sized_buf jsonReduceValue = {(char*)reduce_value->firstReduction.json,
                                             decode_raw16(reduce_value->firstReduction.length)};
                CurrentReducer->add_reduced(jsonReduceBuf, jsonReduceValue);
            }
        }
    }

    result->subTreeCount = encode_raw40(subtreeCount);

    if (CurrentReducer) {
        jsonReduceBuf.size = CurrentReducer->finish(jsonReduceBuf);
        result->firstReduction.length = encode_raw16((uint16_t)jsonReduceBuf.size);
        *size_r += 2 + jsonReduceBuf.size;
        assert(dst + *size_r == &jsonReduceBuf.buf[jsonReduceBuf.size]);
    }
}


static void view_reduce (char *dst, size_t *size_r, nodelist *leaflist, int count)
{
    primary_reduce_common(dst, size_r, leaflist, count, false);
}

static void view_rereduce (char *dst, size_t *size_r, nodelist *ptrlist, int count)
{
    primary_reduce_common(dst, size_r, ptrlist, count, true);
}
