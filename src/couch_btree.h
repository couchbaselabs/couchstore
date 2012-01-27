#include "couch_common.h"
#ifndef COUCH_BTREE_H
#define COUCH_BTREE_H
typedef struct compare_info {
    //used by find_first_gteq
    int last_cmp_val;
    void* last_cmp_key;
    int list_pos;
    //Compare function
    int (*compare)(void *k1, void *k2);
    //Given an erlang term, return a pointer accepted by the compare function
    void *(*from_ext)(struct compare_info* ci, char* buf, int pos);
    //Use to set up any context needed by from_ext
    void *arg;
} compare_info;


//Lookup

typedef struct couchfile_lookup_request {
    compare_info cmp;
    int fd;
    int num_keys;
    //If nonzero, calls fetch_callback for all keys between and including key 0 and key 1
    //in the keys array, or all keys after key 0 if it contains only one key.
    //GIVE KEYS SORTED.
    int fold;
    // v-- Flag used during lookup, do not set.
    int in_fold;
    void **keys;
    void *callback_ctx;
    int (*fetch_callback) (struct couchfile_lookup_request* rq, void* k, sized_buf* v);
    node_pointer *root;
} couchfile_lookup_request;

int btree_lookup(couchfile_lookup_request* rq, uint64_t root_pointer);

//Modify

typedef struct nodelist {
    union _nl_value {
        sized_buf *leaf;
        node_pointer *pointer;
        void *mem;
    } value;
    struct nodelist *next;
} nodelist;

#define ACTION_FETCH  0
#define ACTION_INSERT 1
#define ACTION_REMOVE 2

typedef struct couchfile_modify_action {
    int type;
    sized_buf *key;
    void* cmp_key;
    union _act_value
    {
        sized_buf *term;
        void *arg;
    } value;
} couchfile_modify_action;

typedef struct couchfile_modify_request {
    compare_info cmp;
    Db* db;
    int fd;
    int num_actions;
    couchfile_modify_action* actions;
    void (*fetch_callback) (struct couchfile_modify_request *rq, sized_buf* k, sized_buf* v, void *arg);
    //Put result term into the sized_buf.
    void (*reduce) (sized_buf* dst, nodelist* leaflist, int count);
    void (*rereduce) (sized_buf* dst, nodelist* ptrlist, int count);
    node_pointer root;
} couchfile_modify_request;

#define KV_NODE 0
#define KP_NODE 1

//Used to build and chunk modified nodes
typedef struct couchfile_modify_result {
    couchfile_modify_request *rq;
    nodelist* values;
    nodelist* values_end;

    long node_len;
    int count;

    nodelist* pointers;
    nodelist* pointers_end;
    //If we run over a node and never set this, it can be left as-is on disk.
    int modified;
    //0 - leaf, 1 - ptr
    int node_type;
    int error_state;
} couchfile_modify_result;

node_pointer* modify_btree(couchfile_modify_request *rq,
        node_pointer *root, int *errcode);

#endif
