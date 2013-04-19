/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/**
 * @copyright 2013 Couchbase, Inc.
 *
 * @author Filipe Manana  <filipe@couchbase.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

#include "view_tests.h"


static index_header_t *test_index_header_decoding(const char *header_bin, size_t header_bin_size)
{
    uint16_t active[] = { 3,7,19,23,27,31,35,39,43,47,51,55,59,63 };
    uint16_t passive[] = {
        0,1,2,4,5,6,8,9,10,12,13,14,16,17,18,20,21,22,24,25,
        26,28,29,30,32,33,34,36,37,38,40,41,42,44,45,46,48,49,
        50,52,53,54,56,57,60,62
    };
    uint16_t cleanup[] = { 11,15,58,61 };
    uint16_t unindexable[] = { 0,63 };
    uint16_t replicas_on_transfer[] = { 5, 10, 60, 62 };
    uint16_t pending_active[] = { 11,15 };
    uint16_t pending_passive[] = { 58,61 };
    uint16_t pending_unindexable[] = { 15,58 };
    index_header_t *header = NULL;
    bitmap_t expected_active, expected_passive, expected_cleanup;
    unsigned i;

    assert(decode_index_header(header_bin, header_bin_size, &header) == COUCHSTORE_SUCCESS);
    assert(header != NULL);

    assert(header->version == 1);
    assert(memcmp(header->signature, header_bin, 16) == 0);
    assert(header->num_partitions == 64);
    assert(header->num_views == 2);

    memset(&expected_active, 0, sizeof(expected_active));
    for (i = 0; i < (sizeof(active) / sizeof(active[0])); ++i) {
        set_bit(&expected_active, active[i]);
    }
    assert(memcmp(&header->active_bitmask, &expected_active, sizeof(expected_active)) == 0);

    memset(&expected_passive, 0, sizeof(expected_passive));
    for (i = 0; i < (sizeof(passive) / sizeof(passive[0])); ++i) {
        set_bit(&expected_passive, passive[i]);
    }
    assert(memcmp(&header->passive_bitmask, &expected_passive, sizeof(expected_passive)) == 0);

    memset(&expected_cleanup, 0, sizeof(expected_cleanup));
    for (i = 0; i < (sizeof(cleanup) / sizeof(cleanup[0])); ++i) {
        set_bit(&expected_cleanup, cleanup[i]);
    }
    assert(memcmp(&header->cleanup_bitmask, &expected_cleanup, sizeof(expected_cleanup)) == 0);

    assert(sorted_list_size(header->seqs) == 58);
    for (uint16_t i = 0; i < 64; ++i) {

        switch (i) {
            /* unindexable */
        case 0:
        case 63:
            /* cleanup */
        case 11:
        case 15:
        case 58:
        case 61:
            continue;
        default:
            break;
        }

        part_seq_t rs, *pseq;
        rs.part_id = i;

        pseq = (part_seq_t *) sorted_list_get(header->seqs, &rs);
        assert(pseq != NULL);
        assert(pseq->part_id == i);
        assert(pseq->seq == 1221);
    }

    int num_unindexable = sizeof(unindexable) / sizeof(unindexable[0]);
    assert(sorted_list_size(header->unindexable_seqs) == num_unindexable);
    for (int i = 0; i < num_unindexable; ++i) {
        part_seq_t rs, *pseq;
        rs.part_id = unindexable[i];

        pseq = (part_seq_t *) sorted_list_get(header->unindexable_seqs, &rs);
        assert(pseq != NULL);
        assert(pseq->part_id == unindexable[i]);
        assert(pseq->seq == 1221);
    }

    assert(header->id_btree_state->pointer == 1617507);
    assert(header->id_btree_state->subtreesize == 1286028);
    assert(header->id_btree_state->reduce_value.size == 133);
    /* TODO: once view reduction decoding is done, test the exact reduction value. */

    assert(header->view_btree_states[0]->pointer == 2901853);
    assert(header->view_btree_states[0]->subtreesize == 1284202);
    assert(header->view_btree_states[0]->reduce_value.size == 140);
    /* TODO: once view reduction decoding is done, test the exact reduction value. */

    assert(header->view_btree_states[1]->pointer == 4180175);
    assert(header->view_btree_states[1]->subtreesize == 1278451);
    assert(header->view_btree_states[1]->reduce_value.size == 140);
    /* TODO: once view reduction decoding is done, test the exact reduction value. */

    assert(header->has_replica == 1);
    assert(header->replicas_on_transfer != NULL);

    int num_reps = (sizeof(replicas_on_transfer) /
                    sizeof(replicas_on_transfer[0]));

    assert(sorted_list_size(header->replicas_on_transfer) == num_reps);
    for (int i = 0; i < num_reps; ++i) {
        uint16_t *part_id = sorted_list_get(header->replicas_on_transfer,
                                            &replicas_on_transfer[i]);
        assert(part_id != NULL);
        assert(*part_id == replicas_on_transfer[i]);
    }

    int num_pending_active = sizeof(pending_active) / sizeof(pending_active[0]);
    assert(sorted_list_size(header->pending_transition.active) == num_pending_active);
    for (int i = 0; i < num_pending_active; ++i) {
        uint16_t *part_id = sorted_list_get(header->pending_transition.active,
                                            &pending_active[i]);
        assert(part_id != NULL);
        assert(*part_id == pending_active[i]);
    }

    int num_pending_passive = sizeof(pending_passive) / sizeof(pending_passive[0]);
    assert(sorted_list_size(header->pending_transition.passive) == num_pending_passive);
    for (int i = 0; i < num_pending_passive; ++i) {
        uint16_t *part_id = sorted_list_get(header->pending_transition.passive,
                                            &pending_passive[i]);
        assert(part_id != NULL);
        assert(*part_id == pending_passive[i]);
    }

    int num_pending_unindexable = sizeof(pending_unindexable) / sizeof(pending_unindexable[0]);
    assert(sorted_list_size(header->pending_transition.unindexable) == num_pending_unindexable);
    for (int i = 0; i < num_pending_unindexable; ++i) {
        uint16_t *part_id = sorted_list_get(header->pending_transition.unindexable,
                                            &pending_unindexable[i]);
        assert(part_id != NULL);
        assert(*part_id == pending_unindexable[i]);
    }

    return header;
}


static void test_index_header_encoding(const index_header_t *header,
                                       char **buffer,
                                       size_t *size)
{
    couchstore_error_t res;

    res = encode_index_header(header, buffer, size);
    assert(res == COUCHSTORE_SUCCESS);
}


void test_index_headers()
{
    char header_bin[] = {
        5,226,251,160,170,107,207,39,248,218,139,62,137,58,95,46,204,10,12,1,0,64,0,
        254,1,0,218,1,0,0,136,5,1,4,0,136,254,127,0,218,127,0,8,0,83,119,9,1,254,128,
        0,222,128,0,0,36,5,121,20,136,0,0,58,0,1,1,11,12,4,197,0,2,13,8,0,3,13,8,0,4,
        13,8,0,5,13,8,0,6,13,8,0,7,13,8,0,8,13,8,0,9,13,8,0,10,13,8,0,12,13,8,0,13,
        13,8,0,14,13,8,0,16,13,8,0,17,13,8,0,18,13,8,0,19,13,8,0,20,13,8,0,21,13,8,0,
        22,13,8,0,23,13,8,0,24,13,8,0,25,13,8,0,26,13,8,0,27,13,8,0,28,13,8,0,29,13,
        8,0,30,13,8,0,31,13,8,0,32,13,8,0,33,13,8,0,34,13,8,0,35,13,8,37,19,12,4,197,
        0,37,13,16,0,38,13,8,0,39,13,8,0,40,13,8,0,41,13,8,0,42,13,8,0,43,13,8,0,44,
        13,8,0,45,13,8,0,46,13,8,0,47,13,8,0,48,13,8,0,49,13,8,0,50,13,8,0,51,13,8,0,
        52,13,8,0,53,13,8,0,54,13,8,0,55,13,8,0,56,13,8,0,57,13,8,0,59,13,8,0,60,13,
        8,0,62,13,8,64,145,0,0,0,24,174,99,0,0,0,19,159,140,0,0,1,49,254,101,3,226,
        101,3,0,255,13,1,32,2,0,152,0,0,0,44,71,93,1,148,8,152,106,0,254,148,0,254,
        148,0,1,148,24,0,5,55,56,49,52,52,5,154,8,63,200,207,1,154,4,129,243,254,154,
        0,254,154,0,46,154,0,112,1,0,4,0,5,0,10,0,60,0,62,0,2,0,11,0,15,0,2,0,58,0,
        61,0,2,0,15,0,58,105,173,44,0,0,4,197,0,63,0,0,0,0,4,197
    };

    TPRINT("Decoding an index header...\n");
    index_header_t *header = test_index_header_decoding(header_bin, sizeof(header_bin));

    TPRINT("Encoding the previously decoded header...\n");
    char *header_bin2 = NULL;
    size_t header_bin2_size = 0;

    test_index_header_encoding(header, &header_bin2, &header_bin2_size);

    assert(header_bin2_size == sizeof(header_bin));
    assert(memcmp(header_bin2, header_bin, header_bin2_size) == 0);

    TPRINT("Decoding the previously encoded header...\n");
    index_header_t *header2 = test_index_header_decoding(header_bin2, header_bin2_size);

    TPRINT("Encoding the previously decoded header...\n");
    char *header_bin3 = NULL;
    size_t header_bin3_size = 0;

    test_index_header_encoding(header2, &header_bin3, &header_bin3_size);

    assert(header_bin3_size == sizeof(header_bin));
    assert(memcmp(header_bin3, header_bin, header_bin3_size) == 0);

    free_index_header(header);
    free_index_header(header2);
    free(header_bin2);
    free(header_bin3);
}
