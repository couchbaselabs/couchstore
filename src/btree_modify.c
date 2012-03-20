/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <stdlib.h>
#include <string.h>
#include <signal.h>

#include <libcouchstore/couch_btree.h>
#include <ei.h>
#include "util.h"

#define CHUNK_THRESHOLD 1279

sized_buf empty_root = {
    // {kv_node, []}
    (char *) "\x68\x02\x64\x00\x07kv_node\x6A",
    13
};

static int flush_mr(couchfile_modify_result *res);

static void append_buf(void *dst, int *dstpos, void *src, int len)
{
    memcpy((char *) dst + *dstpos, src, len);
    *dstpos += len;
}

static int find_first_gteq(char *buf, int pos, void *key,
                           compare_info *lu, int at_least)
{
    int list_arity, inner_arity;
    int list_pos = 0, cmp_val;
    off_t pair_pos = 0;
    int errcode = 0;
    error_nonzero(ei_decode_list_header(buf, &pos, &list_arity), COUCHSTORE_ERROR_PARSE_TERM);
    while (list_pos < list_arity) {
        //{<<"key", some other term}
        pair_pos = pos; //Save pos of kv/kp pair tuple

        error_nonzero(ei_decode_tuple_header(buf, &pos, &inner_arity), COUCHSTORE_ERROR_PARSE_TERM)

        lu->last_cmp_key = (*lu->from_ext)(lu, buf, pos);
        cmp_val = (*lu->compare) (lu->last_cmp_key, key);
        lu->last_cmp_val = cmp_val;
        lu->list_pos = list_pos;
        if (cmp_val >= 0 && list_pos >= at_least) {
            break;
        }
        error_nonzero(ei_skip_term(buf, &pos), COUCHSTORE_ERROR_PARSE_TERM); //skip over the key
        error_nonzero(ei_skip_term(buf, &pos), COUCHSTORE_ERROR_PARSE_TERM); //skip over the value
        list_pos++;
    }
cleanup:
    if (errcode < 0) {
        return errcode;
    }
    return pair_pos;
}

static int maybe_flush(couchfile_modify_result *mr)
{
    if (mr->modified && mr->node_len > CHUNK_THRESHOLD) {
        return flush_mr(mr);
    }
    return 0;
}


static void free_nodelist(nodelist *nl)
{
    while (nl) {
        nodelist *next = nl->next;
        free(nl->value.mem);
        free(nl);
        nl = next;
    }
}

static nodelist *make_nodelist(void)
{
    nodelist *r = (nodelist *) malloc(sizeof(nodelist));
    if (!r) {
        return NULL;
    }
    r->next = NULL;
    r->value.mem = NULL;
    return r;
}

static couchfile_modify_result *make_modres(couchfile_modify_request *rq)
{
    couchfile_modify_result *res = (couchfile_modify_result *) malloc(sizeof(couchfile_modify_result));
    if (!res) {
        return NULL;
    }
    res->values = make_nodelist();
    if (!res->values) {
        free(res);
        return NULL;
    }
    res->values_end = res->values;
    res->pointers = make_nodelist();
    if (!res->pointers) {
        free(res);
        return NULL;
    }
    res->pointers_end = res->pointers;
    res->node_len = 0;
    res->count = 0;
    res->modified = 0;
    res->error_state = 0;
    res->rq = rq;
    return res;
}

static void free_modres(couchfile_modify_result *mr)
{
    free_nodelist(mr->values);
    free_nodelist(mr->pointers);
    free(mr);
}

static int mr_push_action(couchfile_modify_action *act,
                          couchfile_modify_result *dst)
{
    //For ACTION_INSERT
    sized_buf *lv = (sized_buf *) malloc(sizeof(sized_buf) +
                                         act->key->size + act->value.term->size + 2);
    if (!lv) {
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }
    //Allocate space for {K,V} term
    lv->buf = ((char *)lv) + sizeof(sized_buf);
    lv->size = (act->key->size + act->value.term->size + 2);
    //tuple of arity 2
    lv->buf[0] = 104;
    lv->buf[1] = 2;
    //copy terms from the action
    memcpy(lv->buf + 2, act->key->buf, act->key->size);
    memcpy(lv->buf + 2 + act->key->size,
           act->value.term->buf, act->value.term->size);

    nodelist *n = make_nodelist();
    if (!n) {
        free(lv);
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }
    dst->values_end->next = n;
    dst->values_end = n;
    n->value.leaf = lv;

    dst->node_len += lv->size;
    dst->count++;
    return maybe_flush(dst);
}

static int mr_push_pointerinfo(node_pointer *ptr, couchfile_modify_result *dst)
{
    nodelist *pel = make_nodelist();
    if (!pel) {
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }
    pel->value.pointer = ptr;
    dst->values_end->next = pel;
    dst->values_end = pel;

    //max len of {key,{pointer, reduce_value, subtreesize}}
    dst->node_len += ptr->key.size + ptr->reduce_value.size + 24;
    dst->count++;
    return maybe_flush(dst);
}

static int mr_push_kv_range(char *buf, int pos, int bound, int end,
                            couchfile_modify_result *dst)
{
    int current = 0;
    int term_begin_pos;
    ei_decode_list_header(buf, &pos, NULL);
    int errcode = 0;
    sized_buf *lv = NULL;

    while (current < end && errcode == 0) {
        term_begin_pos = pos;
        ei_skip_term(buf, &pos);
        if (current >= bound) {
            //Parse KV pair into a leaf_value
            nodelist *n;
            lv = (sized_buf *) malloc(sizeof(sized_buf));
            error_unless(lv, COUCHSTORE_ERROR_ALLOC_FAIL);

            lv->buf = buf + term_begin_pos;
            lv->size = pos - term_begin_pos;

            n = make_nodelist();
            error_unless(n, COUCHSTORE_ERROR_ALLOC_FAIL);
            dst->values_end->next = n;
            dst->values_end = n;
            n->value.leaf = lv;

            dst->node_len += lv->size;
            dst->count++;
            lv = NULL;
            error_pass(maybe_flush(dst));
        }
        current++;
    }
cleanup:
    free(lv);
    return errcode;
}

static node_pointer *read_pointer(char *buf, int pos)
{
    //Parse KP pair into a node_pointer {K, {ptr, reduce_value, subtreesize}}
    node_pointer *p = (node_pointer *) malloc(sizeof(node_pointer));
    if (!p) {
        return NULL;
    }
    ei_decode_tuple_header(buf, &pos, NULL); //arity 2
    term_to_buf(&p->key, buf, &pos);
    ei_decode_tuple_header(buf, &pos, NULL); //arity 3
    ei_decode_uint64(buf, &pos, &p->pointer);
    term_to_buf(&p->reduce_value, buf, &pos);
    ei_decode_uint64(buf, &pos, &p->subtreesize);

    return p;
}

static void mr_push_kp_range(char *buf, int pos, int bound, int end,
                             couchfile_modify_result *dst)
{
    int current = 0;
    ei_decode_list_header(buf, &pos, NULL);
    while (current < end) {
        if (current >= bound) {
            mr_push_pointerinfo(read_pointer(buf, pos), dst);
        }
        ei_skip_term(buf, &pos);
        current++;
    }
}

//Write the current contents of the values list to disk as a node
//and add the resulting pointer to the pointers list.
static int flush_mr(couchfile_modify_result *res)
{
    int nbufpos = 0;
    uint64_t subtreesize = 0;
    sized_buf reduce_value;
    sized_buf writebuf;
    //default reduce value []
    reduce_value.buf = (char *) "\x6A"; //NIL_EXT
    reduce_value.size = 1;
    int reduced = 0;
    int errcode = 0;
    nodelist *pel = NULL;

    if (res->values_end == res->values || !res->modified) {
        //Empty
        return 0;
    }

    res->node_len += 19; //tuple header and node type tuple, list header and tail
    char *nodebuf = (char *) malloc(res->node_len);

    //External term header; tuple header arity 2;
    ei_encode_version(nodebuf, &nbufpos);
    ei_encode_tuple_header(nodebuf, &nbufpos, 2);
    switch (res->node_type) {
    case KV_NODE:
        ei_encode_atom_len(nodebuf, &nbufpos, "kv_node", 7);
        if (res->rq->reduce) {
            (*res->rq->reduce)(&reduce_value, res->values->next, res->count);
            reduced = 1;
        }
        break;
    case KP_NODE:
        ei_encode_atom_len(nodebuf, &nbufpos, "kp_node", 7);
        if (res->rq->rereduce) {
            (*res->rq->rereduce)(&reduce_value, res->values->next, res->count);
            reduced = 1;
        }
        break;
    }

    ei_encode_list_header(nodebuf, &nbufpos, res->count);

    nodelist *i = res->values->next;

    sized_buf last_key = {NULL, 0};
    while (i != NULL) {
        if (res->node_type == KV_NODE) { //writing value in a kv_node
            append_buf(nodebuf, &nbufpos, i->value.leaf->buf, i->value.leaf->size);
            if (i->next == NULL) {
                int pos = 0;
                term_to_buf(&last_key, i->value.leaf->buf + 2, &pos);
            }
        } else if (res->node_type == KP_NODE) { //writing value in a kp_node
            //waitpointer used to live here
            subtreesize += i->value.pointer->subtreesize;
            ei_encode_tuple_header(nodebuf, &nbufpos, 2); //tuple arity 2
            append_buf(nodebuf, &nbufpos, i->value.pointer->key.buf, i->value.pointer->key.size);
            ei_encode_tuple_header(nodebuf, &nbufpos, 3); //tuple arity 3
            //pointer
            // v- between 2 and 10 bytes (ERL_SMALL_INTEGER_EXT to ERL_SMALL_BIG_EXT/8)
            ei_encode_ulonglong(nodebuf, &nbufpos, i->value.pointer->pointer);
            //reduce_value
            append_buf(nodebuf, &nbufpos, i->value.pointer->reduce_value.buf,
                       i->value.pointer->reduce_value.size);
            //subtreesize
            // v- between 2 and 10 bytes (ERL_SMALL_INTEGER_EXT to ERL_SMALL_BIG_EXT/8)
            ei_encode_ulonglong(nodebuf, &nbufpos, i->value.pointer->subtreesize);
            if (i->next == NULL) {
                last_key = i->value.pointer->key;
            }
        }
        i = i->next;
    }

    //NIL_EXT (list tail)
    ei_encode_empty_list(nodebuf, &nbufpos);

    node_pointer *ptr = (node_pointer *) malloc(sizeof(node_pointer) +
                                                last_key.size + reduce_value.size);
    if (!ptr) {
        errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
        goto cleanup;
    }

    ptr->pointer = 0;

    writebuf.buf = nodebuf;
    writebuf.size = nbufpos;
    db_write_buf_compressed(res->rq->db, &writebuf, (off_t *) &ptr->pointer);

    ptr->key.buf = ((char *)ptr) + sizeof(node_pointer);
    ptr->reduce_value.buf = ((char *)ptr) + sizeof(node_pointer) + last_key.size;

    ptr->key.size = last_key.size;
    ptr->reduce_value.size = reduce_value.size;

    memcpy(ptr->key.buf, last_key.buf, last_key.size);
    memcpy(ptr->reduce_value.buf, reduce_value.buf, reduce_value.size);

    ptr->subtreesize = subtreesize + nbufpos;

    pel = make_nodelist();
    if (!pel) {
        errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
        free(ptr);
        goto cleanup;
    }
    pel->value.pointer = ptr;
    res->pointers_end->next = pel;
    res->pointers_end = pel;

    res->node_len = 0;
    res->count = 0;

    res->values_end = res->values;
    free_nodelist(res->values->next);
    res->values->next = NULL;
cleanup:
    free(nodebuf);
    if (errcode < 0) {
        free(ptr);
    }

    if (reduced) {
        free(reduce_value.buf);
    }
    return errcode;
}

//Move this node's pointers list to dst node's values list.
static int mr_move_pointers(couchfile_modify_result *src,
                            couchfile_modify_result *dst)
{
    int errcode = 0;
    if (src->pointers_end == src->pointers) {
        return 0;
    }

    nodelist *ptr = src->pointers->next;
    nodelist *next = ptr;
    while (ptr != NULL && errcode == 0) {
        //max on disk len of a pointer node
        dst->node_len += ptr->value.pointer->key.size +
                         ptr->value.pointer->reduce_value.size + 24;
        dst->count++;

        next = ptr->next;
        ptr->next = NULL;

        dst->values_end->next = ptr;
        dst->values_end = ptr;
        ptr = next;
        error_pass(maybe_flush(dst));
    }

cleanup:
    src->pointers->next = next;
    src->pointers_end = src->pointers;
    return errcode;
}

static int modify_node(couchfile_modify_request *rq, node_pointer *nptr,
                       int start, int end, couchfile_modify_result *dst)
{
    sized_buf current_node;
    int curnode_pos = 0;
    int read_size = 0;
    int list_start_pos = 0;
    int node_len = 0;
    int node_bound = 0;
    int errcode = 0;
    int kpos = 0;
    int node_type_pos = 0;
    couchfile_modify_result *local_result = NULL;

    if (start == end) {
        return 0;
    }

    if (nptr == NULL) {
        current_node = empty_root;
    } else {
        if ((read_size = pread_compressed(rq->db, nptr->pointer, (char **) &current_node.buf)) < 0) {
            error_pass(COUCHSTORE_ERROR_READ);
        }
        current_node.size = read_size;
        curnode_pos++; //Skip over 131.
    }

    local_result = make_modres(rq);
    error_unless(local_result, COUCHSTORE_ERROR_ALLOC_FAIL);

    ei_decode_tuple_header(current_node.buf, &curnode_pos, NULL);
    node_type_pos = curnode_pos;
    ei_skip_term(current_node.buf, &curnode_pos);
    list_start_pos = curnode_pos;
    error_nonzero(ei_decode_list_header(current_node.buf, &curnode_pos, &node_len), COUCHSTORE_ERROR_PARSE_TERM);

    if (atom_check(current_node.buf + node_type_pos, "kv_node")) {
        local_result->node_type = KV_NODE;
        while (start < end) {
            if (node_bound >= node_len) {
                //We're at the end of a leaf node.
                switch (rq->actions[start].type) {
                case ACTION_INSERT:
                    local_result->modified = 1;
                    mr_push_action(&rq->actions[start], local_result);
                    break;

                case ACTION_REMOVE:
                    local_result->modified = 1;
                    break;

                case ACTION_FETCH:
                    if (rq->fetch_callback) {
                        //not found
                        (*rq->fetch_callback)(rq, rq->actions[start].key, NULL, rq->actions[start].value.arg);
                    }
                    break;
                }
                start++;
            } else {
                kpos = find_first_gteq(current_node.buf, list_start_pos,
                                       rq->actions[start].cmp_key,
                                       &rq->cmp, node_bound);

                error_unless(kpos >= 0, COUCHSTORE_ERROR_PARSE_TERM);

                //Add items from node_bound up to but not including the current
                mr_push_kv_range(current_node.buf, list_start_pos, node_bound,
                                 rq->cmp.list_pos, local_result);

                if (rq->cmp.last_cmp_val > 0) { // Node key > action key
                    switch (rq->actions[start].type) {
                    case ACTION_INSERT:
                        local_result->modified = 1;
                        mr_push_action(&rq->actions[start], local_result);
                        break;

                    case ACTION_REMOVE:
                        local_result->modified = 1;
                        break;

                    case ACTION_FETCH:
                        if (rq->fetch_callback) {
                            //not found
                            (*rq->fetch_callback)(rq, rq->actions[start].key, NULL, rq->actions[start].value.arg);
                        }
                        break;
                    }

                    start++;
                    node_bound = rq->cmp.list_pos;
                } else if (rq->cmp.last_cmp_val < 0) { // Node key < action key
                    node_bound = rq->cmp.list_pos + 1;
                    mr_push_kv_range(current_node.buf, list_start_pos, node_bound - 1,
                                     node_bound, local_result);
                } else { //Node key == action key
                    switch (rq->actions[start].type) {
                    case ACTION_INSERT:
                        local_result->modified = 1;
                        mr_push_action(&rq->actions[start], local_result);
                        node_bound = rq->cmp.list_pos + 1;
                        break;

                    case ACTION_REMOVE:
                        local_result->modified = 1;
                        node_bound = rq->cmp.list_pos + 1;
                        break;

                    case ACTION_FETCH:
                        if (rq->fetch_callback) {
                            sized_buf cb_tmp;
                            int cb_vpos = kpos;
                            ei_decode_tuple_header(current_node.buf, &cb_vpos, NULL);
                            ei_skip_term(current_node.buf, &cb_vpos);
                            cb_tmp.buf = current_node.buf + cb_vpos;
                            cb_tmp.size = cb_vpos;
                            ei_skip_term(current_node.buf, &cb_vpos);
                            cb_tmp.size = cb_vpos - cb_tmp.size;
                            (*rq->fetch_callback)(rq, rq->actions[start].key, &cb_tmp, rq->actions[start].value.arg);
                        }
                        node_bound = rq->cmp.list_pos;
                        break;
                    }
                    start++;
                }
            }
        }
        //Push any items past the end of what we dealt with onto result.
        if (node_bound < node_len) {
            mr_push_kv_range(current_node.buf, list_start_pos, node_bound,
                             node_len, local_result);
        }
    } else if (atom_check(current_node.buf + node_type_pos, "kp_node")) {
        local_result->node_type = KP_NODE;
        while (start < end) {
            kpos = find_first_gteq(current_node.buf, list_start_pos,
                                   rq->actions[start].cmp_key,
                                   &rq->cmp, node_bound);

            error_unless(kpos >= 0, COUCHSTORE_ERROR_PARSE_TERM);

            if (rq->cmp.list_pos == (node_len - 1)) { //got last item in kp_node
                //Push all items except last onto mr
                mr_push_kp_range(current_node.buf, list_start_pos, node_bound,
                                 rq->cmp.list_pos, local_result);
                node_pointer *desc = read_pointer(current_node.buf, kpos);
                errcode = modify_node(rq, desc, start, end, local_result);
                if (local_result->values_end->value.pointer != desc) {
                    free(desc);
                }
                error_pass(errcode);
                node_bound = node_len;
                break;
            } else {
                //Get all actions with key <= the key of the current item in the
                //kp_node

                //Push items in node up to but not including current onto mr
                mr_push_kp_range(current_node.buf, list_start_pos, node_bound,
                                 rq->cmp.list_pos, local_result);
                int range_end = start;
                while (range_end < end &&
                        ((*rq->cmp.compare)(rq->actions[range_end].cmp_key, rq->cmp.last_cmp_key) <= 0)) {
                    range_end++;
                }

                node_bound = rq->cmp.list_pos + 1;
                node_pointer *desc = read_pointer(current_node.buf, kpos);
                errcode = modify_node(rq, desc, start, range_end, local_result);
                if (local_result->values_end->value.pointer != desc) {
                    free(desc);
                }
                error_pass(errcode);
                start = range_end;
            }
        }
        if (node_bound < node_len) {
            //Processed all the actions but haven't exhausted this kpnode.
            //push the rest of it onto the mr.
            mr_push_kp_range(current_node.buf, list_start_pos, node_bound, node_len,
                             local_result);
        }
    } else {
        errcode = COUCHSTORE_ERROR_PARSE_TERM;
        goto cleanup;
    }
    //If we've done modifications, write out the last leaf node.
    error_pass(flush_mr(local_result))
    if (!local_result->modified && nptr != NULL) {
        //If we didn't do anything, give back the pointer to the original
        mr_push_pointerinfo(nptr, dst);
    } else {
        //Otherwise, give back the pointers to the nodes we've created.
        dst->modified = 1;
        error_pass(mr_move_pointers(local_result, dst));
    }
cleanup:
    free_modres(local_result);

    if (current_node.buf != empty_root.buf) {
        free(current_node.buf);
    }

    return errcode;
}

static node_pointer *finish_root(couchfile_modify_request *rq,
                                 couchfile_modify_result *root_result,
                                 int *errcode)
{
    node_pointer *ret_ptr = NULL;
    couchfile_modify_result *collector = make_modres(rq);
    if (!collector) {
        *errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
        return NULL;
    }
    collector->modified = 1;
    collector->node_type = KP_NODE;
    flush_mr(root_result);
    while (1) {
        if (root_result->pointers_end == root_result->pointers->next) {
            //The root result split into exactly one kp_node.
            //Return the pointer to it.
            ret_ptr = root_result->pointers_end->value.pointer;
            root_result->pointers_end->value.mem = NULL;
            break;
        } else {
            //The root result split into more than one kp_node.
            //Move the pointer list to the value list and write out the new node.
            *errcode = mr_move_pointers(root_result, collector);
            if (*errcode < 0) {
                goto cleanup;
            }

            *errcode = flush_mr(collector);
            if (*errcode < 0) {
                goto cleanup;
            }
            //Swap root_result and collector mr's.
            couchfile_modify_result *tmp = root_result;
            root_result = collector;
            collector = tmp;
        }
    }
cleanup:
    free_modres(root_result);
    free_modres(collector);
    return ret_ptr;
}

node_pointer *modify_btree(couchfile_modify_request *rq,
                           node_pointer *root, int *errcode)
{
    node_pointer *ret_ptr = root;
    couchfile_modify_result *root_result = make_modres(rq);
    if (!root_result) {
        *errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
        return root;
    }
    root_result->node_type = KP_NODE;
    *errcode = modify_node(rq, root, 0, rq->num_actions, root_result);
    if (*errcode < 0) {
        free_modres(root_result);
        return NULL;
    }

    if (root_result->values_end->value.pointer == root) {
        //If we got the root pointer back, remove it from the list
        //so we don't try to free it.
        root_result->values_end->value.mem = NULL;
    }

    if (!root_result->modified) {
        free_modres(root_result);
    } else {
        if (root_result->count > 1 || root_result->pointers != root_result->pointers_end) {
            //The root was split
            //Write it to disk and return the pointer to it.
            ret_ptr = finish_root(rq, root_result, errcode);
            if (*errcode < 0) {
                ret_ptr = NULL;
            }
        } else {
            ret_ptr = root_result->values_end->value.pointer;
            root_result->values_end->value.mem = NULL;
            free_modres(root_result);
        }
    }
    return ret_ptr;
}

