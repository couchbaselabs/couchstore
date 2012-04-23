#ifndef COUCH_BTREE_H
#define COUCH_BTREE_H
#include <libcouchstore/couch_common.h>
#include "internal.h"
#include "arena.h"

#ifdef __cplusplus
extern "C" {
#endif

    typedef struct compare_info {
        /* used by find_first_gteq */
        int last_cmp_val;
        sized_buf *last_cmp_key;
        int list_pos;
        /* Compare function */
        int (*compare)(sized_buf *k1, sized_buf *k2);
        void *arg;
    } compare_info;


    /* Lookup */

    typedef struct couchfile_lookup_request {
        compare_info cmp;
        Db *db;
        int num_keys;
        /* If nonzero, calls fetch_callback for all keys between and including key 0 and key 1
           in the keys array, or all keys after key 0 if it contains only one key.
           GIVE KEYS SORTED. */
        int fold;
        /*  v-- Flag used during lookup, do not set. */
        int in_fold;
        sized_buf **keys;
        void *callback_ctx;
        couchstore_error_t (*fetch_callback) (struct couchfile_lookup_request *rq, void *k, sized_buf *v);
    } couchfile_lookup_request;

    couchstore_error_t btree_lookup(couchfile_lookup_request *rq,
                                    uint64_t root_pointer);

    /* Modify */
    typedef struct nodelist {
        sized_buf data;
        sized_buf key;
        node_pointer *pointer;
        struct nodelist *next;
    } nodelist;

    /* Reduce function gets items and places reduce value in dst buffer */
    typedef void (*reduce_fn) (char* dst, size_t* size_r, nodelist* itmlist, int count);

#define ACTION_FETCH  0
#define ACTION_REMOVE 1
#define ACTION_INSERT 2

    typedef struct couchfile_modify_action {
        int type;
        sized_buf *key;
        sized_buf *cmp_key;
        union _act_value {
            sized_buf *data;
            void *arg;
        } value;
    } couchfile_modify_action;

    typedef struct couchfile_modify_request {
        compare_info cmp;
        Db *db;
        int num_actions;
        couchfile_modify_action *actions;
        void (*fetch_callback) (struct couchfile_modify_request *rq, sized_buf *k, sized_buf *v, void *arg);
        reduce_fn reduce;
        reduce_fn rereduce;
    } couchfile_modify_request;

#define KP_NODE 0
#define KV_NODE 1

    /* Used to build and chunk modified nodes */
    typedef struct couchfile_modify_result {
        couchfile_modify_request *rq;
        struct arena *arena;
        /* If this is set, prefer to put items that can be thrown away after a flush in this arena */
        struct arena *arena_transient;
        nodelist *values;
        nodelist *values_end;

        long node_len;
        int count;

        nodelist *pointers;
        nodelist *pointers_end;
        /* If we run over a node and never set this, it can be left as-is on disk. */
        int modified;
        /* 0 - leaf, 1 - ptr */
        int node_type;
        int error_state;
    } couchfile_modify_result;

    node_pointer *modify_btree(couchfile_modify_request *rq,
                               node_pointer *root,
                               couchstore_error_t *errcode);

    couchstore_error_t mr_push_item(sized_buf *k, sized_buf *v, couchfile_modify_result *dst);

    couchfile_modify_result* new_btree_modres(arena* a, arena* transient_arena, Db* db, compare_info* cmp, reduce_fn reduce,
            reduce_fn rereduce);

    node_pointer* complete_new_btree(couchfile_modify_result* mr, couchstore_error_t *errcode);

#ifdef __cplusplus
}
#endif

#endif
