/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "view_tests.h"


static view_btree_key_t *test_view_btree_key_decoding(const char *key_bin, size_t len)
{
    view_btree_key_t *k = NULL;

    assert(decode_view_btree_key(key_bin, len, &k) == COUCHSTORE_SUCCESS);
    assert(k != NULL);
    assert(k->json_key.size == 4);
    assert(memcmp(k->json_key.buf, "\"23\"", k->json_key.size) == 0);
    assert(k->doc_id.size == 12);
    assert(memcmp(k->doc_id.buf, "doc_00000023", k->doc_id.size) == 0);

    return k;
}

static view_id_btree_key_t *test_view_id_btree_key_decoding(const char *id_btree_key_bin, size_t len)
{
    view_id_btree_key_t *k = NULL;

    assert(decode_view_id_btree_key(id_btree_key_bin, len, &k) == COUCHSTORE_SUCCESS);
    assert(k != NULL);

    assert(k->partition == 57);
    assert(k->doc_id.size == 12);
    assert(memcmp(k->doc_id.buf, "doc_00000057", k->doc_id.size) == 0);

    return k;
}

static void test_view_btree_key_encoding(const view_btree_key_t *k,
                                       char **buffer,
                                       size_t *size)
{
    couchstore_error_t res;

    res = encode_view_btree_key(k, buffer, size);
    assert(res == COUCHSTORE_SUCCESS);
}


static void test_view_id_btree_key_encoding(const view_id_btree_key_t *k,
                                       char **buffer,
                                       size_t *size)
{
    couchstore_error_t res;

    res = encode_view_id_btree_key(k, buffer, size);
    assert(res == COUCHSTORE_SUCCESS);
}

void test_keys()
{
    char key_bin[] = {
        0,4,34,50,51,34,100,111,99,95,48,48,48,48,48,48,50,51
    };
    char id_btree_key_bin[] = {
        0,57,100,111,99,95,48,48,48,48,48,48,53,55
    };
    char *k_bin2 = NULL;
    size_t k_bin2_size = 0;
    char *id_btree_k_bin2 = NULL;
    size_t id_btree_k_bin2_size = 0;
    char *k_bin3 = NULL;
    size_t k_bin3_size = 0;
    char *id_btree_k_bin3 = NULL;
    size_t id_btree_k_bin3_size = 0;
    view_btree_key_t *k;
    view_id_btree_key_t *id_btree_k;
    view_btree_key_t *k2;
    view_id_btree_key_t *id_btree_k2;

    fprintf(stderr, "Decoding a view btree key ...\n");
    k = test_view_btree_key_decoding(key_bin, sizeof(key_bin));

    fprintf(stderr, "Decoding a view id btree key ...\n");
    id_btree_k = test_view_id_btree_key_decoding(id_btree_key_bin, sizeof(id_btree_key_bin));

    fprintf(stderr, "Encoding the previously decoded view btree key ...\n");
    test_view_btree_key_encoding(k, &k_bin2, &k_bin2_size);

    assert(k_bin2_size == sizeof(key_bin));
    assert(memcmp(k_bin2, key_bin, k_bin2_size) == 0);

    fprintf(stderr, "Encoding the previously decoded view id btree key ...\n");
    test_view_id_btree_key_encoding(id_btree_k, &id_btree_k_bin2, &id_btree_k_bin2_size);

    assert(id_btree_k_bin2_size == sizeof(id_btree_key_bin));
    assert(memcmp(id_btree_k_bin2, id_btree_key_bin, id_btree_k_bin2_size) == 0);

    fprintf(stderr, "Decoding the previously encoded view btree key ...\n");
    k2 = test_view_btree_key_decoding(k_bin2, k_bin2_size);

    fprintf(stderr, "Decoding the previously encoded view id btree key ...\n");
    id_btree_k2 = test_view_id_btree_key_decoding(id_btree_k_bin2, id_btree_k_bin2_size);

    fprintf(stderr, "Encoding the previously decoded view btree key ...\n");
    test_view_btree_key_encoding(k2, &k_bin3, &k_bin3_size);

    assert(k_bin3_size == sizeof(key_bin));
    assert(memcmp(k_bin3, key_bin, k_bin3_size) == 0);

    fprintf(stderr, "Encoding the previously decoded view id btree key ...\n");
    test_view_id_btree_key_encoding(id_btree_k2, &id_btree_k_bin3, &id_btree_k_bin3_size);

    assert(id_btree_k_bin3_size == sizeof(id_btree_key_bin));
    assert(memcmp(id_btree_k_bin3, id_btree_key_bin, id_btree_k_bin3_size) == 0);

    free_view_btree_key(k);
    free_view_btree_key(k2);
    free(k_bin2);
    free(k_bin3);

    free_view_id_btree_key(id_btree_k);
    free_view_id_btree_key(id_btree_k2);
    free(id_btree_k_bin2);
    free(id_btree_k_bin3);
}
