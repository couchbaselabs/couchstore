/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "view_tests.h"
#include "../src/couch_btree.h"

#include <string.h>

#define BITMAP_SIZE 1024
#define DOUBLE_FMT "%.15lg"
#define scan_stats(buf, sum, count, min, max, sumsqr) \
        sscanf(buf, "{\"sum\":%lg,\"count\":%llu,\"min\":%lg,\"max\":%lg,\"sumsqr\":%lg}",\
               &sum, &count, &min, &max, &sumsqr)

static void free_node_list(nodelist *nl);
static void test_view_id_btree_reducer();
static void test_view_btree_sum_reducer();
static void test_view_btree_sum_reducer_errors();
static void test_view_btree_count_reducer();
static void test_view_btree_stats_reducer();
static void test_view_btree_stats_reducer_errors();

void reducer_tests()
{
    TPRINT("Running built-in reducer tests ... \n");
    test_view_id_btree_reducer();
    TPRINT("End of built-in view id btree reducer tests\n");
    test_view_btree_sum_reducer();
    test_view_btree_sum_reducer_errors();
    TPRINT("End of built-in view btree sum reducer tests\n");
    test_view_btree_count_reducer();
    TPRINT("End of built-in view btree count reducer tests\n");
    test_view_btree_stats_reducer();
    test_view_btree_stats_reducer_errors();
    TPRINT("End of built-in view btree stats reducer tests\n");
    TPRINT("End of built-in reducer tests\n");
}

static void test_view_id_btree_reducer()
{
    nodelist *nl = NULL;
    node_pointer *np = NULL;
    node_pointer *np2 = NULL;
    view_id_btree_reduction_t *r;
    char dst[MAX_REDUCTION_SIZE];
    size_t size_r;
    int i, count = 0;
    /* partition ID is 67, two views, three keys in total (2 + 1) */
    char data_bin1[] = {
        0,67,0,0,2,0,14,91,49,50,51,44,34,102,111,111,98,97,114,
        34,93,0,4,45,51,50,49,1,0,1,0,7,91,53,44,54,44,55,93
    };
    /* partition ID is 67, doc_id size is 12 */
    char key_bin1[] = {
        0,67,100,111,99,95,48,48,48,48,48,48,53,55
    };
    char reduce_bin[] = {
        0,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,8,0,0,0,0,0,0,0,0
    };
    nodelist *nl2 = NULL;
    /* partition ID is 57, two views, three keys in total (2 + 1) */
    char data_bin2[] = {
        0,57,0,0,2,0,14,91,49,50,51,44,34,102,111,111,98,97,114,
        34,93,0,4,45,51,50,49,1,0,1,0,7,91,53,44,54,44,55,93
    };
    /* partition ID is 57, doc_id size is 12 */
    char key_bin2[] = {
        0,57,100,111,99,95,48,48,48,48,48,48,53,55
    };
    char reduce_bin2[] = {
        0,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2,0,0,0,0,0,0,0
    };

    nl = (nodelist *) malloc(sizeof(nodelist));
    assert(nl != NULL);

    count++;
    nl->data.buf = data_bin1;
    nl->data.size = sizeof(data_bin1);
    nl->key.buf = key_bin1;
    nl->key.size = sizeof(key_bin1);
    nl->pointer = NULL;
    nl->next = NULL;

    np = (node_pointer *) malloc(sizeof(node_pointer));
    assert(np != NULL);
    np->key.buf = key_bin1;
    np->key.size = sizeof(key_bin1);
    np->reduce_value.buf = reduce_bin;
    np->reduce_value.size = sizeof(reduce_bin);
    np->pointer = 0;
    np->subtreesize = 3;
    nl->pointer = np;

    nl2 = (nodelist *) malloc(sizeof(nodelist));
    assert(nl2 != NULL);

    count++;
    nl2->data.buf = data_bin2;
    nl2->data.size = sizeof(data_bin2);
    nl2->key.buf = key_bin2;
    nl2->key.size = sizeof(key_bin2);
    nl2->pointer = NULL;
    nl2->next = NULL;
    nl->next = nl2;

    assert(view_id_btree_reduce(dst, &size_r, nl, count, NULL) == COUCHSTORE_SUCCESS);
    assert(decode_view_id_btree_reduction(dst, &r) == COUCHSTORE_SUCCESS);
    assert(r->kv_count == 6);
    assert(is_bit_set(&r->partitions_bitmap,57));
    assert(is_bit_set(&r->partitions_bitmap,67));

    for (i = 0; i < BITMAP_SIZE; ++i) {
        if ((i != 57) && (i != 67)) {
            assert (!(is_bit_set(&r->partitions_bitmap, i)));
        }
    }

    /* free it before variables reuse */
    free_view_id_btree_reduction(r);

    np2 = (node_pointer *) malloc(sizeof(node_pointer));
    assert(np2 != NULL);
    np2->key.buf = key_bin2;
    np2->key.size = sizeof(key_bin2);
    np2->reduce_value.buf = reduce_bin2;
    np2->reduce_value.size = sizeof(reduce_bin2);
    np2->pointer = 0;
    np2->subtreesize = 3;
    nl2->pointer = np2;

    assert(view_id_btree_rereduce(dst, &size_r, nl, count, NULL) == COUCHSTORE_SUCCESS);
    assert(decode_view_id_btree_reduction(dst, &r) == COUCHSTORE_SUCCESS);
    assert(r->kv_count == 6);
    assert(is_bit_set(&r->partitions_bitmap,57));
    assert(is_bit_set(&r->partitions_bitmap,67));

    for (i = 0; i < BITMAP_SIZE; ++i) {
        if ((i != 57) && (i != 67)) {
            assert (!(is_bit_set(&r->partitions_bitmap, i)));
        }
    }

    free_view_id_btree_reduction(r);
    free_node_list(nl);
}

static void free_node_list(nodelist *nl)
{
    nodelist *tmp;
    tmp = nl;
    while (tmp != NULL ){
        nl = nl->next;
        free (tmp->pointer);
        free (tmp);
        tmp = nl;
    }
}

static void test_view_btree_sum_reducer()
{
    nodelist *nl = NULL;
    node_pointer *np = NULL;
    node_pointer *np2 = NULL;
    view_btree_reduction_t *r = NULL;
    char dst[MAX_REDUCTION_SIZE];
    size_t size_r;
    int i, count = 0;
    /* partition ID is 1, two values, 5 and 6, ascii encoded */
    char data_bin1[] = {0,1,0,0,1,53,0,0,1,54};
    /*json_key size is 2, doc_id size is 4 */
    char key_bin1[] = {0,2,11,22,11,22,33,44};
    char reduce_bin[] = {
        0,0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2,0,2,49,49
    };
    /* partition ID is 2, two values, 5 and 6 */
    char data_bin2[] = {0,2,0,0,1,53,0,0,1,54};
    /* json_key size is 2, doc_id size is 5 */
    char key_bin2[] = {0,2,33,44,11,22,33,44,55};
    char reduce_bin2[] = {
        0,0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,4,0,2,49,49
    };
    nodelist *nl2 = NULL;

    nl = (nodelist *) malloc(sizeof(nodelist));
    assert(nl != NULL);
    count++;
    nl->data.buf = data_bin1;
    nl->data.size = sizeof(data_bin1);
    nl->key.buf = key_bin1;
    nl->key.size = sizeof(key_bin1);
    nl->pointer = NULL;
    nl->next = NULL;

    np = (node_pointer *) malloc(sizeof(node_pointer));
    assert(np != NULL);
    np->key.buf = key_bin1;
    np->key.size = sizeof(key_bin1);
    np->reduce_value.buf = reduce_bin;
    np->reduce_value.size = sizeof(reduce_bin);
    np->pointer = 0;
    np->subtreesize = 2;
    nl->pointer = np;

    nl2 = (nodelist *) malloc(sizeof(nodelist));
    assert(nl2 != NULL);
    count++;
    nl2->data.buf = data_bin2;
    nl2->data.size = sizeof(data_bin2);
    nl2->key.buf = key_bin2;
    nl2->key.size = sizeof(key_bin2);
    nl2->pointer = NULL;
    nl2->next = NULL;
    nl->next = nl2;

    assert(view_btree_sum_reduce(dst, &size_r, nl, count, NULL) == COUCHSTORE_SUCCESS);
    assert(decode_view_btree_reduction(dst, size_r, &r) == COUCHSTORE_SUCCESS);
    assert(r->kv_count == 4);
    assert(r->num_values == 1);

    assert(memcmp(r->reduce_values[0].buf, "22",2) == 0);
    assert(r->reduce_values[0].size == 2);

    assert(is_bit_set(&r->partitions_bitmap,1));
    assert(is_bit_set(&r->partitions_bitmap,2));

    for (i = 0; i < BITMAP_SIZE; ++i) {
        if ((i != 1) && (i != 2)) {
            assert (!(is_bit_set(&r->partitions_bitmap, i)));
        }
    }
    /* free it before variables reuse */
    free_view_btree_reduction(r);
    np2 = (node_pointer *) malloc(sizeof(node_pointer));
    assert(np2 != NULL);

    np2->key.buf = key_bin2;
    np2->key.size = sizeof(key_bin2);
    np2->reduce_value.buf = reduce_bin2;
    np2->reduce_value.size = sizeof(reduce_bin2);
    np2->pointer = 0;
    np2->subtreesize = 2;
    nl2->pointer = np2;

    assert(view_btree_sum_rereduce(dst, &size_r, nl, count, NULL) == COUCHSTORE_SUCCESS);
    assert(decode_view_btree_reduction(dst, size_r, &r) == COUCHSTORE_SUCCESS);
    assert(r->kv_count == 4);
    assert(r->num_values == 1);

    assert(memcmp(r->reduce_values[0].buf, "22",2) == 0);
    assert(r->reduce_values[0].size == 2);

    assert(is_bit_set(&r->partitions_bitmap,1));
    assert(is_bit_set(&r->partitions_bitmap,2));

    for (i = 0; i < BITMAP_SIZE; ++i) {
        if ((i != 1) && (i != 2)) {
            assert (!(is_bit_set(&r->partitions_bitmap, i)));
        }
    }

    free_view_btree_reduction(r);
    free_node_list(nl);
}

static void test_view_btree_sum_reducer_errors()
{
    nodelist *nl = NULL;
    node_pointer *np = NULL;
    node_pointer *np2 = NULL;
    char dst[MAX_REDUCTION_SIZE];
    size_t size_r;
    int count = 0;
    /* partition ID is 1, two values, f and o, ascii encoded */
    char data_bin1[] = {0,1,0,0,1,102,0,0,1,111};
    /*json_key size is 2, doc_id size is 4 */
    char key_bin1[] = {0,2,11,22,102,111,111,111};
    /* reduction value is fo */
    char reduce_bin[] = {
        0,0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2,0,2,102,111
    };
    /* partition ID is 2, two values, 5 and 6 */
    char data_bin2[] = {0,2,0,0,1,53,0,0,1,54};
    /* json_key size is 2, doc_id size is 5 */
    char key_bin2[] = {0,2,33,44,11,22,33,44,55};
    char reduce_bin2[] = {
        0,0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,4,0,2,49,49
    };
    nodelist *nl2 = NULL;
    couchstore_error_t errcode;
    view_reducer_ctx_t errctx;

    nl = (nodelist *) malloc(sizeof(nodelist));
    assert(nl != NULL);
    count++;
    nl->data.buf = data_bin1;
    nl->data.size = sizeof(data_bin1);
    nl->key.buf = key_bin1;
    nl->key.size = sizeof(key_bin1);

    nl->pointer = NULL;
    nl->next = NULL;

    np = (node_pointer *) malloc(sizeof(node_pointer));
    assert(np != NULL);
    np->key.buf = key_bin1;
    np->key.size = sizeof(key_bin1);
    np->reduce_value.buf = reduce_bin;
    np->reduce_value.size = sizeof(reduce_bin);
    np->pointer = 0;
    np->subtreesize = 2;
    nl->pointer = np;

    nl2 = (nodelist *) malloc(sizeof(nodelist));
    assert(nl2 != NULL);
    count++;
    nl2->data.buf = data_bin2;
    nl2->data.size = sizeof(data_bin2);
    nl2->key.buf = key_bin2;
    nl2->key.size = sizeof(key_bin2);
    nl2->pointer = NULL;
    nl2->next = NULL;
    nl->next = nl2;

    errcode = view_btree_sum_reduce(dst, &size_r, nl, count, &errctx);
    assert(errcode == COUCHSTORE_ERROR_REDUCER_FAILURE);
    assert(errctx.error == VIEW_REDUCER_ERROR_NOT_A_NUMBER);
    assert(memcmp(errctx.error_doc_id,"fooo", 4) == 0);
    free((char *) errctx.error_doc_id);

    np2 = (node_pointer *) malloc(sizeof(node_pointer));
    assert(np2 != NULL);

    np2->key.buf = key_bin2;
    np2->key.size = sizeof(key_bin2);
    np2->reduce_value.buf = reduce_bin2;
    np2->reduce_value.size = sizeof(reduce_bin2);
    np2->pointer = 0;
    np2->subtreesize = 2;
    nl2->pointer = np2;

    assert(view_btree_sum_rereduce(dst, &size_r, nl, count, &errctx) == COUCHSTORE_ERROR_REDUCER_FAILURE);
    assert(errctx.error == VIEW_REDUCER_ERROR_NOT_A_NUMBER);
    assert(memcmp(errctx.error_doc_id,"fooo", 4) == 0);
    free((char *)errctx.error_doc_id);
    free_node_list(nl);
}

static void test_view_btree_count_reducer()
{
    nodelist *nl = NULL;
    node_pointer *np = NULL;
    node_pointer *np2 = NULL;
    view_btree_reduction_t *r = NULL;
    char dst[MAX_REDUCTION_SIZE];
    size_t size_r;
    int count = 0;
    /* partition ID is 1, two values, 5 and 6, ascii encoded */
    char data_bin1[] = {0,1,0,0,1,53,0,0,1,54};
    /*json_key size is 2, doc_id size is 4 */
    char key_bin1[] = {0,2,11,22,11,22,33,44};
    char reduce_bin[] = {
        0,0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2,0,1,50
    };
    /* partition ID is 2, two values, 5 and 6 */
    char data_bin2[] = {0,2,0,0,1,53,0,0,1,54};
    /* json_key size is 2, doc_id size is 5 */
    char key_bin2[] = {0,2,33,44,11,22,33,44,55};
    char reduce_bin2[] = {
        0,0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,4,0,1,50
    };
    nodelist *nl2 = NULL;
    int i;

    nl = (nodelist *) malloc(sizeof(nodelist));
    assert(nl != NULL);
    count++;
    nl->data.buf = data_bin1;
    nl->data.size = sizeof(data_bin1);
    nl->key.buf = key_bin1;
    nl->key.size = sizeof(key_bin1);
    nl->pointer = NULL;
    nl->next = NULL;

    np = (node_pointer *) malloc(sizeof(node_pointer));
    assert(np != NULL);
    np->key.buf = key_bin1;
    np->key.size = sizeof(key_bin1);
    np->reduce_value.buf = reduce_bin;
    np->reduce_value.size = sizeof(reduce_bin);
    np->pointer = 0;
    np->subtreesize = 2;
    nl->pointer = np;

    nl2 = (nodelist *) malloc(sizeof(nodelist));
    assert(nl2 != NULL);
    count++;
    nl2->data.buf = data_bin2;
    nl2->data.size = sizeof(data_bin2);
    nl2->key.buf = key_bin2;
    nl2->key.size = sizeof(key_bin2);
    nl2->pointer = NULL;
    nl2->next = NULL;
    nl->next = nl2;

    assert(view_btree_count_reduce(dst, &size_r, nl, count, NULL) == COUCHSTORE_SUCCESS);
    assert(decode_view_btree_reduction(dst, size_r, &r) == COUCHSTORE_SUCCESS);
    assert(r->kv_count == 4);
    assert(r->num_values == 1);

    assert(memcmp(r->reduce_values[0].buf, "4", 1) == 0);
    assert(r->reduce_values[0].size == 1);

    assert(is_bit_set(&r->partitions_bitmap,1));
    assert(is_bit_set(&r->partitions_bitmap,2));

    for (i = 0; i < BITMAP_SIZE; ++i) {
        if ((i != 1) && (i != 2)) {
            assert (!(is_bit_set(&r->partitions_bitmap, i)));
        }
    }
    /* free it before variables reuse */
    free_view_btree_reduction(r);
    np2 = (node_pointer *) malloc(sizeof(node_pointer));
    assert(np2 != NULL);

    np2->key.buf = key_bin2;
    np2->key.size = sizeof(key_bin2);
    np2->reduce_value.buf = reduce_bin2;
    np2->reduce_value.size = sizeof(reduce_bin2);
    np2->pointer = 0;
    np2->subtreesize = 2;
    nl2->pointer = np2;

    assert(view_btree_count_rereduce(dst, &size_r, nl, count, NULL) == COUCHSTORE_SUCCESS);
    assert(decode_view_btree_reduction(dst, size_r, &r) == COUCHSTORE_SUCCESS);
    assert(r->kv_count == 4);
    assert(r->num_values == 1);

    assert(memcmp(r->reduce_values[0].buf, "4", 1) == 0);
    assert(r->reduce_values[0].size == 1);

    assert(is_bit_set(&r->partitions_bitmap,1));
    assert(is_bit_set(&r->partitions_bitmap,2));

    for (i = 0; i < BITMAP_SIZE; ++i) {
        if ((i != 1) && (i != 2)) {
            assert (!(is_bit_set(&r->partitions_bitmap, i)));
        }
    }

    free_view_btree_reduction(r);
    free_node_list(nl);
}

static void test_view_btree_stats_reducer()
{
    nodelist *nl = NULL;
    node_pointer *np = NULL;
    node_pointer *np2 = NULL;
    view_btree_reduction_t *r;
    char dst[MAX_REDUCTION_SIZE];
    size_t size_r;
    int count = 0;
    /* partition ID is 1, two values, 5 and 6 */
    char data_bin1[] = {0,1,0,0,1,53,0,0,1,54};
    /* json_key size is 2, doc_id size is 4 */
    char key_bin1[] = {0,2,11,22,11,22,33,44};
    char reduce_bin[] = {
        0,0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2,0,48,123,34,115,117,
        109,34,58,49,49,44,34,99,111,117,110,116,34,58,50,44,34,109,105,110,34,
        58,53,44,34,109,97,120,34,58,54,44,34,115,117,109,115,113,114,34,58,54,49,125
    };
    nodelist *nl2 = NULL;
    /* partition ID is 2, two values, 5 and 6 */
    char data_bin2[] = {0,2,0,0,1,53,0,0,1,54};
    /* json_key size is 2, doc_id size is 5 */
    char key_bin2[] = {0,2,33,44,11,22,33,44,55};
    char reduce_bin2[] = {
        0,0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,4,0,48,123,34,115,117,
        109,34,58,49,49,44,34,99,111,117,110,116,34,58,50,44,34,109,105,110,34,
        58,53,44,34,109,97,120,34,58,54,44,34,115,117,109,115,113,114,34,58,54,49,125
    };

    nl = (nodelist *) malloc(sizeof(nodelist));
    assert(nl != NULL);
    count++;
    nl->data.buf = data_bin1;
    nl->data.size = sizeof(data_bin1);
    nl->key.buf = key_bin1;
    nl->key.size = sizeof(key_bin1);
    nl->pointer = NULL;
    nl->next = NULL;

    np = (node_pointer *) malloc(sizeof(node_pointer));
    assert(np != NULL);
    np->key.buf = key_bin1;
    np->key.size = sizeof(key_bin1);
    np->reduce_value.buf = reduce_bin;
    np->reduce_value.size = sizeof(reduce_bin);
    np->pointer = 0;
    np->subtreesize = 2;
    nl->pointer = np;

    nl2 = (nodelist *) malloc(sizeof(nodelist));
    assert(nl2 != NULL);
    count++;

    nl2->data.buf = data_bin2;
    nl2->data.size = sizeof(data_bin2);
    nl2->key.buf = key_bin2;
    nl2->key.size = sizeof(key_bin2);
    nl2->pointer = NULL;
    nl2->next = NULL;
    nl->next = nl2;

    assert(view_btree_stats_reduce(dst, &size_r, nl, count, NULL) == COUCHSTORE_SUCCESS);
    assert(decode_view_btree_reduction(dst, size_r, &r) == COUCHSTORE_SUCCESS);
    assert(r->kv_count == 4);
    assert(r->num_values == 1);

    stats_t *st = malloc(sizeof(stats_t));
    assert(st != NULL);

    int scanned = scan_stats(r->reduce_values[0].buf,
                            st->sum, st->count, st->min, st->max, st->sumsqr);
    assert(scanned == 5);
    assert(st->count == 4);
    assert(st->sum == 22);
    assert(st->sumsqr == 122);
    assert(st->min == 5);
    assert(st->max == 6);
    assert(is_bit_set(&r->partitions_bitmap,1));
    assert(is_bit_set(&r->partitions_bitmap,2));

    for (int i = 0; i < BITMAP_SIZE; ++i) {
        if ((i != 1) && (i != 2)) {
            assert (!(is_bit_set(&r->partitions_bitmap, i)));
        }
    }

    /* free it before variables reuse */
    free_view_btree_reduction(r);
    free(st);
    np2 = (node_pointer *) malloc(sizeof(node_pointer));
    assert(np2 != NULL);
    np2->key.buf = key_bin2;
    np2->key.size = sizeof(key_bin2);
    np2->reduce_value.buf = reduce_bin2;
    np2->reduce_value.size = sizeof(reduce_bin2);
    np2->pointer = 0;
    np2->subtreesize = 2;
    nl2->pointer = np2;

    assert(view_btree_stats_rereduce(dst, &size_r, nl, count, NULL) == COUCHSTORE_SUCCESS);
    assert(decode_view_btree_reduction(dst, size_r, &r) == COUCHSTORE_SUCCESS);
    assert(r->kv_count == 4);
    assert(r->num_values == 1);

    st = malloc(sizeof(stats_t));
    assert(st != NULL);
    scanned = scan_stats(r->reduce_values[0].buf,
                        st->sum, st->count, st->min, st->max, st->sumsqr);
    assert(scanned == 5);
    assert(st->count == 4);
    assert(st->sum == 22);
    assert(st->sumsqr == 122);
    assert(st->min == 5);
    assert(st->max == 6);
    assert(is_bit_set(&r->partitions_bitmap,1));
    assert(is_bit_set(&r->partitions_bitmap,2));

    for (int i = 0; i < BITMAP_SIZE; ++i) {
        if ((i != 1) && (i != 2)) {
            assert (!(is_bit_set(&r->partitions_bitmap, i)));
        }
    }

    free_view_btree_reduction(r);
    free_node_list(nl);
    free(st);
}

static void test_view_btree_stats_reducer_errors()
{
    nodelist *nl = NULL;
    node_pointer *np = NULL;
    node_pointer *np2 = NULL;
    char dst[MAX_REDUCTION_SIZE];
    size_t size_r;
    int count = 0;
    /* partition ID is 1, two values, f and o, ascii encoded */
    char data_bin1[] = {0,1,0,0,1,102,0,0,1,111};
    /*json_key size is 2, doc_id size is 4 */
    char key_bin1[] = {0,2,11,22,102,111,111,111};
    /* sum is fo */
    char reduce_bin[] = {
        0,0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,2,0,48,123,34,115,117,
        109,34,58,102,111,44,34,99,111,117,110,116,34,58,50,44,34,109,105,110,34,
        58,53,44,34,109,97,120,34,58,54,44,34,115,117,109,115,113,114,34,58,54,49,125
    };
    /* partition ID is 2, two values, 5 and 6 */
    char data_bin2[] = {0,2,0,0,1,53,0,0,1,54};
    /* json_key size is 2, doc_id size is 5 */
    char key_bin2[] = {0,2,33,44,11,22,33,44,55};
    char reduce_bin2[] = {
        0,0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,4,0,48,123,34,115,117,
        109,34,58,49,49,44,34,99,111,117,110,116,34,58,50,44,34,109,105,110,34,
        58,53,44,34,109,97,120,34,58,54,44,34,115,117,109,115,113,114,34,58,54,49,125
    };
    nodelist *nl2 = NULL;
    couchstore_error_t errcode;
    view_reducer_ctx_t errctx;

    nl = (nodelist *) malloc(sizeof(nodelist));
    assert(nl != NULL);
    count++;
    nl->data.buf = data_bin1;
    nl->data.size = sizeof(data_bin1);
    nl->key.buf = key_bin1;
    nl->key.size = sizeof(key_bin1);
    nl->pointer = NULL;
    nl->next = NULL;

    np = (node_pointer *) malloc(sizeof(node_pointer));
    assert(np != NULL);
    np->key.buf = key_bin1;
    np->key.size = sizeof(key_bin1);
    np->reduce_value.buf = reduce_bin;
    np->reduce_value.size = sizeof(reduce_bin);
    np->pointer = 0;
    np->subtreesize = 2;
    nl->pointer = np;

    nl2 = (nodelist *) malloc(sizeof(nodelist));
    assert(nl2 != NULL);
    count++;

    nl2->data.buf = data_bin2;
    nl2->data.size = sizeof(data_bin2);
    nl2->key.buf = key_bin2;
    nl2->key.size = sizeof(key_bin2);
    nl2->pointer = NULL;
    nl2->next = NULL;
    nl->next = nl2;

    errcode = view_btree_stats_reduce(dst, &size_r, nl, count, &errctx);
    assert(errcode == COUCHSTORE_ERROR_REDUCER_FAILURE);
    assert(errctx.error == VIEW_REDUCER_ERROR_NOT_A_NUMBER);
    assert(memcmp(errctx.error_doc_id,"fooo", 4) == 0);
    free((char *) errctx.error_doc_id);

    np2 = (node_pointer *) malloc(sizeof(node_pointer));
    assert(np2 != NULL);

    np2->key.buf = key_bin2;
    np2->key.size = sizeof(key_bin2);
    np2->reduce_value.buf = reduce_bin2;
    np2->reduce_value.size = sizeof(reduce_bin2);
    np2->pointer = 0;
    np2->subtreesize = 2;
    nl2->pointer = np2;

    assert(view_btree_stats_rereduce(dst, &size_r, nl, count, &errctx) == COUCHSTORE_ERROR_REDUCER_FAILURE);
    assert(errctx.error == VIEW_REDUCER_ERROR_NOT_A_NUMBER);
    assert(memcmp(errctx.error_doc_id,"fooo", 4) == 0);
    free((char *)errctx.error_doc_id);
    free_node_list(nl);
}
