/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "view_tests.h"


static view_btree_reduction_t *test_view_btree_reduction_decoding(const char *reduction_bin,
                                                                    size_t len)
{
    view_btree_reduction_t *r = NULL;
    uint16_t partition_bitset[] = {0,1023};
    bitmap_t expected_part_bitmap;


    assert(decode_view_btree_reductions(reduction_bin, len, &r) == COUCHSTORE_SUCCESS);


    assert(r != NULL);

    assert(r->kv_count == 1221);

    memset(&expected_part_bitmap, 0, sizeof(expected_part_bitmap));
    for (int i = 0; i < (sizeof(partition_bitset) / sizeof(partition_bitset[0])); ++i) {
        set_bit(&expected_part_bitmap, partition_bitset[i]);
    }

    assert(memcmp(&r->partitions_bitmap, &expected_part_bitmap, sizeof(expected_part_bitmap)) == 0);

    assert(r->num_values == 2);

    assert(r->reduce_values[0].size == 2);
    assert(r->reduce_values[1].size == 4);

    return r;
}

static view_id_btree_reduction_t *test_view_id_btree_reduction_decoding(const char *id_btree_reduction_bin)
{
    view_id_btree_reduction_t *r = NULL;
    uint16_t partition_bitset[] = {0,1023};
    bitmap_t expected_part_bitmap;

    assert(decode_view_id_btree_reductions(id_btree_reduction_bin, &r) == COUCHSTORE_SUCCESS);
    assert(r != NULL);

    assert(r->kv_count == 1221);

    memset(&expected_part_bitmap, 0, sizeof(expected_part_bitmap));
    for (int i = 0; i < (sizeof(partition_bitset) / sizeof(partition_bitset[0])); ++i) {
        set_bit(&expected_part_bitmap, partition_bitset[i]);
    }
    assert(memcmp(&r->partitions_bitmap, &expected_part_bitmap, sizeof(expected_part_bitmap)) == 0);

    return r;
}

static void test_view_btree_reduction_encoding(const view_btree_reduction_t *r,
                                       char **buffer,
                                       size_t *size)
{
    couchstore_error_t res;

    res = encode_view_btree_reductions(r, buffer, size);
    assert(res == COUCHSTORE_SUCCESS);
}


static void test_view_id_btree_reduction_encoding(const view_id_btree_reduction_t *r,
                                       char **buffer,
                                       size_t *size)
{
    couchstore_error_t res;

    res = encode_view_id_btree_reductions(r, buffer, size);
    assert(res == COUCHSTORE_SUCCESS);
}

void test_reductions()
{
    char reduction_bin[] = {
        0,0,0,4,197,128,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,1,0,2,11,22,0,4,11,22,33,44
    };

    char id_btree_reduction_bin[] = {
        0,0,0,4,197,128,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,1
    };

    TPRINT("Decoding a view btree reduction ...\n");
    view_btree_reduction_t *r = test_view_btree_reduction_decoding(reduction_bin,
                                                                    sizeof(reduction_bin));

    TPRINT("Decoding a view id btree reduction ...\n");
    view_id_btree_reduction_t *id_btree_r = test_view_id_btree_reduction_decoding(id_btree_reduction_bin);

    TPRINT("Encoding the previously decoded view btree reduction ...\n");
    char *r_bin2 = NULL;
    size_t r_bin2_size = 0;

    test_view_btree_reduction_encoding(r, &r_bin2, &r_bin2_size);

    assert(r_bin2_size == sizeof(reduction_bin));
    assert(memcmp(r_bin2, reduction_bin, r_bin2_size) == 0);


    TPRINT("Encoding the previously decoded view id btree reduction ...\n");
    char *id_btree_r_bin2 = NULL;
    size_t id_btree_r_bin2_size = 0;

    test_view_id_btree_reduction_encoding(id_btree_r, &id_btree_r_bin2, &id_btree_r_bin2_size);

    assert(id_btree_r_bin2_size == sizeof(id_btree_reduction_bin));
    assert(memcmp(id_btree_r_bin2, id_btree_reduction_bin, id_btree_r_bin2_size) == 0);

    TPRINT("Decoding the previously encoded view btree reduction ...\n");
    view_btree_reduction_t *r2 = test_view_btree_reduction_decoding(r_bin2, r_bin2_size);

    TPRINT("Decoding the previously encoded view id btree reduction ...\n");
    view_id_btree_reduction_t *id_btree_r2 = test_view_id_btree_reduction_decoding(id_btree_r_bin2);

    TPRINT("Encoding the previously decoded view btree reduciton ...\n");
    char *r_bin3 = NULL;
    size_t r_bin3_size = 0;

    test_view_btree_reduction_encoding(r2, &r_bin3, &r_bin3_size);

    assert(r_bin3_size == sizeof(reduction_bin));
    assert(memcmp(r_bin3, reduction_bin, r_bin3_size) == 0);

    TPRINT("Encoding the previously decoded view id btree reduciton ...\n");
    char *id_btree_r_bin3 = NULL;
    size_t id_btree_r_bin3_size = 0;

    test_view_id_btree_reduction_encoding(id_btree_r2, &id_btree_r_bin3, &id_btree_r_bin3_size);

    assert(id_btree_r_bin3_size == sizeof(id_btree_reduction_bin));
    assert(memcmp(id_btree_r_bin3, id_btree_reduction_bin, id_btree_r_bin3_size) == 0);

    free_view_btree_reductions(r);
    free_view_btree_reductions(r2);
    free(r_bin2);
    free(r_bin3);

    free_view_id_btree_reductions(id_btree_r);
    free_view_id_btree_reductions(id_btree_r2);
    free(id_btree_r_bin2);
    free(id_btree_r_bin3);
}
