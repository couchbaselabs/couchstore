/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "view_tests.h"


static view_btree_value_t *test_view_btree_value_decoding(const char *value_bin,
                                                                    size_t len)
{
    view_btree_value_t *v = NULL;


    assert(decode_view_btree_value(value_bin, len, &v) == COUCHSTORE_SUCCESS);


    assert(v != NULL);

    assert(v->partition == 46);


    assert(v->values[0].size == 2);
    assert(v->values[1].size == 4);
    assert(v->values[2].size == 5);


    assert(memcmp(v->values[0].buf, "ab", v->values[0].size) == 0);

    assert(memcmp(v->values[1].buf, "abcd", v->values[1].size) == 0);

    assert(memcmp(v->values[2].buf, "abcde", v->values[2].size) == 0);

    return v;
}

static view_id_btree_value_t *test_view_id_btree_value_decoding(const char *id_btree_value_bin,
                                                                size_t len)
{
    view_id_btree_value_t *v = NULL;


    assert(decode_view_id_btree_value(id_btree_value_bin, len, &v) == COUCHSTORE_SUCCESS);

    assert(v != NULL);

    assert(v->partition == 45);

    assert(v->view_keys_map[0].view_id == 1);
    assert(v->view_keys_map[0].num_keys == 3);
    assert(v->view_keys_map[0].json_keys[0].size == 2);
    assert(v->view_keys_map[0].json_keys[1].size == 3);
    assert(v->view_keys_map[0].json_keys[2].size == 4);

    assert(v->view_keys_map[1].view_id == 2);
    assert(v->view_keys_map[1].num_keys == 1);
    assert(v->view_keys_map[1].json_keys[0].size == 2);

    assert(memcmp(v->view_keys_map[0].json_keys[0].buf, "ab", v->view_keys_map[0].json_keys[0].size) == 0);

    assert(memcmp(v->view_keys_map[0].json_keys[1].buf, "abc", v->view_keys_map[0].json_keys[1].size) == 0);

    assert(memcmp(v->view_keys_map[0].json_keys[2].buf, "abcd", v->view_keys_map[0].json_keys[2].size) == 0);

    assert(memcmp(v->view_keys_map[1].json_keys[0].buf, "ab", v->view_keys_map[0].json_keys[0].size) == 0);

    return v;
}

static void test_view_btree_value_encoding(const view_btree_value_t *v,
                                            char **buffer,
                                            size_t *size)
{
    couchstore_error_t res;

    res = encode_view_btree_value(v, buffer, size);
    assert(res == COUCHSTORE_SUCCESS);
}


static void test_view_id_btree_value_encoding(const view_id_btree_value_t *v,
                                       char **buffer,
                                       size_t *size)
{
    couchstore_error_t res;

    res = encode_view_id_btree_value(v, buffer, size);
    assert(res == COUCHSTORE_SUCCESS);
}

void test_values()
{
    char value_bin[] = {
        0,46,0,0,2,97,98,0,0,4,97,98,99,100,0,0,5,97,98,99,100,101
    };

    char id_btree_value_bin[] = {
        0,45,1,0,3,0,2,97,98,0,3,97,98,99,0,4,97,98,99,100,2,0,1,0,2,97,98
    };

    TPRINT("Decoding a view btree value ...\n");
    view_btree_value_t *v = test_view_btree_value_decoding(value_bin, sizeof(value_bin));

    TPRINT("Decoding a view id btree value ...\n");
    view_id_btree_value_t *id_btree_v = test_view_id_btree_value_decoding(id_btree_value_bin,
                                                                        sizeof(id_btree_value_bin));

    TPRINT("Encoding the previously decoded view btree value ...\n");
    char *v_bin2 = NULL;
    size_t v_bin2_size = 0;

    test_view_btree_value_encoding(v, &v_bin2, &v_bin2_size);

    assert(v_bin2_size == sizeof(value_bin));
    assert(memcmp(v_bin2, value_bin, v_bin2_size) == 0);


    TPRINT("Encoding the previously decoded view id btree value ...\n");
    char *id_btree_v_bin2 = NULL;
    size_t id_btree_v_bin2_size = 0;

    test_view_id_btree_value_encoding(id_btree_v, &id_btree_v_bin2, &id_btree_v_bin2_size);

    assert(id_btree_v_bin2_size == sizeof(id_btree_value_bin));
    assert(memcmp(id_btree_v_bin2, id_btree_value_bin, id_btree_v_bin2_size) == 0);

    TPRINT("Decoding the previously encoded view btree value ...\n");
    view_btree_value_t *v2 = test_view_btree_value_decoding(v_bin2, v_bin2_size);

    TPRINT("Decoding the previously encoded view id btree value ...\n");
    view_id_btree_value_t *id_btree_v2 = test_view_id_btree_value_decoding(id_btree_v_bin2,
                                                                        id_btree_v_bin2_size);

    TPRINT("Encoding the previously decoded view btree value ...\n");
    char *v_bin3 = NULL;
    size_t v_bin3_size = 0;

    test_view_btree_value_encoding(v2, &v_bin3, &v_bin3_size);

    assert(v_bin3_size == sizeof(value_bin));
    assert(memcmp(v_bin3, value_bin, v_bin3_size) == 0);

    TPRINT("Encoding the previously decoded view id btree value ...\n");
    char *id_btree_v_bin3 = NULL;
    size_t id_btree_v_bin3_size = 0;

    test_view_id_btree_value_encoding(id_btree_v2, &id_btree_v_bin3, &id_btree_v_bin3_size);

    assert(id_btree_v_bin3_size == sizeof(id_btree_value_bin));
    assert(memcmp(id_btree_v_bin3, id_btree_value_bin, id_btree_v_bin3_size) == 0);

    free_view_btree_value(v);
    free_view_btree_value(v2);
    free(v_bin2);
    free(v_bin3);

    free_view_id_btree_value(id_btree_v);
    free_view_id_btree_value(id_btree_v2);
    free(id_btree_v_bin2);
    free(id_btree_v_bin3);
}
