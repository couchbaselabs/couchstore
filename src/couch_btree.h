#ifndef COUCH_BTREE_H
#define COUCH_BTREE_H
#include <libcouchstore/couch_common.h>
#include "internal.h"
#include "arena.h"

#ifdef __cplusplus
extern "C" {
#endif

#define DB_CHUNK_THRESHOLD 1279
#define MAX_REDUCTION_SIZE ((1 << 16) - 1)

    typedef int (*compare_callback)(const sized_buf *k1, const sized_buf *k2);

    typedef struct compare_info {
        /* Compare function */
        compare_callback compare;
    } compare_info;


    /* Lookup */

    typedef struct couchfile_lookup_request {
        compare_info cmp;
        tree_file *file;
        int num_keys;
        /* If nonzero, calls fetch_callback for all keys between and including key 0 and key 1
           in the keys array, or all keys after key 0 if it contains only one key.
           GIVE KEYS SORTED. */
        int fold;
        /*  v-- Flag used during lookup, do not set. */
        int in_fold;
        sized_buf **keys;
        void *callback_ctx;
        couchstore_error_t (*fetch_callback) (struct couchfile_lookup_request *rq,
					      const sized_buf *k,
					      const sized_buf *v);
        couchstore_error_t (*node_callback) (struct couchfile_lookup_request *rq,
                                             uint64_t subtreeSize,
                                             const sized_buf *reduce_value);
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
    typedef couchstore_error_t (*reduce_fn)(char *dst,
                                            size_t *size_r,
                                            const nodelist *itmlist,
                                            int count,
                                            void *ctx);

#define ACTION_FETCH  0
#define ACTION_REMOVE 1
#define ACTION_INSERT 2

    typedef struct couchfile_modify_action {
        int type;
        sized_buf *key;
        union _act_value {
            sized_buf *data;
            void *arg;
        } value;
    } couchfile_modify_action;

    /* Guided purge related constants */
#define PURGE_ITEM    0
#define PURGE_STOP    1
#define PURGE_KEEP    2
#define PURGE_PARTIAL 3

    /* Returns purge action or error code */
    typedef int (*purge_kp_fn)(const node_pointer *nptr, void *ctx);
    typedef int (*purge_kv_fn)(const sized_buf *key, const sized_buf *val, void *ctx);

    typedef struct couchfile_modify_request {
        compare_info cmp;
        tree_file *file;
        int num_actions;
        couchfile_modify_action *actions;
        void (*fetch_callback) (struct couchfile_modify_request *rq, sized_buf *k, sized_buf *v, void *arg);
        reduce_fn reduce;
        reduce_fn rereduce;
        void *user_reduce_ctx;
        /* For guided btree purge */
        purge_kp_fn purge_kp;
        purge_kv_fn purge_kv;
        int enable_purging;
        void *guided_purge_ctx;
        /*  We're in the compactor */
        int compacting;
        int kv_chunk_threshold;
        int kp_chunk_threshold;
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
        /* 1 - leaf, 0 - ptr */
        int node_type;
        int error_state;
    } couchfile_modify_result;

    node_pointer *modify_btree(couchfile_modify_request *rq,
                               node_pointer *root,
                               couchstore_error_t *errcode);

    couchstore_error_t mr_push_item(sized_buf *k, sized_buf *v, couchfile_modify_result *dst);

    couchfile_modify_result* new_btree_modres(arena* a, arena* transient_arena, tree_file *file,
                                              compare_info* cmp, reduce_fn reduce,
                                              reduce_fn rereduce, void *user_reduce_ctx,
                                              int kv_chunk_threshold,
                                              int kp_chunk_threshold);

    node_pointer* complete_new_btree(couchfile_modify_result* mr, couchstore_error_t *errcode);

    node_pointer *guided_purge_btree(couchfile_modify_request *rq,
                                                node_pointer *root,
                                                couchstore_error_t *errcode);

    node_pointer* copy_node_pointer(node_pointer* ptr);

    node_pointer *read_pointer(arena* a, sized_buf *key, char *buf);

    node_pointer *finish_root(couchfile_modify_request *rq,
                              couchfile_modify_result *root_result,
                              couchstore_error_t *errcode);

    couchfile_modify_result *make_modres(arena* a, couchfile_modify_request *rq);
#ifdef __cplusplus
}
#endif

#endif
