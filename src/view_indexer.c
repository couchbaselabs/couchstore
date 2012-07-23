/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include "bitfield.h"
#include "collate_json.h"
#include "couch_btree.h"
#include "tree_writer.h"
#include "util.h"


static void view_reduce(char *dst, size_t *size_r, nodelist *leaflist, int count);
static void view_rereduce(char *dst, size_t *size_r, nodelist *leaflist, int count);


static int keyCompare(const sized_buf *k1, const sized_buf *k2) {
    return CollateJSON(*k1, *k2, kCollateJSON_Unicode);
}


couchstore_error_t couchstore_index_view(const char *inputPath, Db* outputDb) {
    couchstore_error_t errcode;
    TreeWriter* treeWriter = NULL;

    error_pass(TreeWriterOpen(inputPath, &keyCompare, view_reduce, view_rereduce, &treeWriter));
    error_pass(TreeWriterSort(treeWriter));
    error_pass(TreeWriterWrite(treeWriter, outputDb));
    error_pass(couchstore_commit(outputDb));

cleanup:
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
