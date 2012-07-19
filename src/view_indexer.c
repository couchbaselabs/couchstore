/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "collate_json.h"
#include "tree_writer.h"
#include "util.h"


static int keyCompare(const sized_buf *k1, const sized_buf *k2) {
    return CollateJSON(*k1, *k2, kCollateJSON_Unicode);
}


couchstore_error_t couchstore_index_view(const char *inputPath, Db* outputDb) {
    couchstore_error_t errcode;
    TreeWriter* treeWriter = NULL;

    error_pass(TreeWriterOpen(inputPath, &keyCompare, &treeWriter));
    error_pass(TreeWriterSort(treeWriter));
    error_pass(TreeWriterWrite(treeWriter, outputDb));
    error_pass(couchstore_commit(outputDb));

cleanup:
    TreeWriterFree(treeWriter);
    return errcode;
}
