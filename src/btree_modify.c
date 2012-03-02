/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <stdlib.h>
#include <string.h>
#include <signal.h>

#include "couch_btree.h"
#include "util.h"
#include "bitfield.h"

#define CHUNK_THRESHOLD 1279


static couchstore_error_t flush_mr(couchfile_modify_result *res);

static couchstore_error_t maybe_flush(couchfile_modify_result *mr)
{
    if (mr->modified && mr->node_len > CHUNK_THRESHOLD) {
        return flush_mr(mr);
    }

    return COUCHSTORE_SUCCESS;
}


static void free_nodelist(nodelist *nl)
{
    while (nl) {
        nodelist *next = nl->next;
        free(nl->pointer);
        free(nl);
        nl = next;
    }
}

static nodelist *make_nodelist(int bufsize)
{
    nodelist *r = calloc(1, sizeof(nodelist) + bufsize);
    if (!r) {
        return NULL;
    }
    r->data.size = bufsize;
    r->data.buf = ((char *) r) + (sizeof(nodelist));
    return r;
}

static couchfile_modify_result *make_modres(couchfile_modify_request *rq)
{
    couchfile_modify_result *res = malloc(sizeof(couchfile_modify_result));
    if (!res) {
        return NULL;
    }
    res->values = make_nodelist(0);
    if (!res->values) {
        free(res);
        return NULL;
    }
    res->values_end = res->values;
    res->pointers = make_nodelist(0);
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

static couchstore_error_t mr_push_item(sized_buf *k, sized_buf *v, couchfile_modify_result *dst)
{
    nodelist *itm = make_nodelist(0);
    if (!itm) {
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }
    itm->key = *k;
    itm->data = *v;
    itm->pointer = NULL;
    dst->values_end->next = itm;
    dst->values_end = itm;
    //Encoded size (see flush_mr)
    dst->node_len += k->size + v->size + 5;
    dst->count++;
    return maybe_flush(dst);
}

static nodelist *encode_pointer(node_pointer *ptr)
{
    nodelist *pel = make_nodelist(14 + ptr->reduce_value.size);
    if (!pel) {
        return NULL;
    }
    set_bits(pel->data.buf, 0, 48, ptr->pointer);
    set_bits(pel->data.buf + 6, 0, 48, ptr->subtreesize);
    set_bits(pel->data.buf + 12, 0, 16, ptr->reduce_value.size);
    memcpy(pel->data.buf + 14, ptr->reduce_value.buf, ptr->reduce_value.size);
    pel->pointer = ptr;
    pel->key = ptr->key;
    return pel;
}

static couchstore_error_t mr_push_pointerinfo(node_pointer *ptr,
                                              couchfile_modify_result *dst)
{
    nodelist *pel = encode_pointer(ptr);
    if (!pel) {
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }
    dst->values_end->next = pel;
    dst->values_end = pel;
    dst->node_len += pel->key.size + pel->data.size + 5;
    dst->count++;
    return maybe_flush(dst);
}

static node_pointer *read_pointer(sized_buf *key, char *buf)
{
    //Parse KP pair into a node_pointer {K, {ptr, reduce_value, subtreesize}}
    node_pointer *p = (node_pointer *) malloc(sizeof(node_pointer));
    if (!p) {
        return NULL;
    }
    p->pointer = get_48(buf);
    p->subtreesize = get_48(buf + 6);
    p->reduce_value.size = get_16(buf + 12);
    p->reduce_value.buf = buf + 14;
    p->key = *key;
    return p;
}

//Write the current contents of the values list to disk as a node
//and add the resulting pointer to the pointers list.
static couchstore_error_t flush_mr(couchfile_modify_result *res)
{
    int bufpos = 0;
    int errcode = COUCHSTORE_SUCCESS;
    char *nodebuf = NULL;
    sized_buf writebuf;
    char reducebuf[30];
    size_t reducesize = 0;
    uint64_t subtreesize = 0;
    off_t diskpos;
    sized_buf final_key = {NULL, 0};

    if (res->values_end == res->values || ! res->modified) {
        //Empty
        return COUCHSTORE_SUCCESS;
    }

    nodebuf = malloc(res->node_len + 1);
    if (!nodebuf) {
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }

    writebuf.buf = nodebuf;
    writebuf.size = res->node_len + 1;
    memset(nodebuf, 0, res->node_len + 1);

    nodebuf[0] = res->node_type;
    bufpos = 1;

    nodelist *i = res->values->next;
    while (i != NULL) {
        set_bits(nodebuf + bufpos, 0, 12, i->key.size);
        set_bits(nodebuf + bufpos + 1, 4, 28, i->data.size);
        memcpy(nodebuf + bufpos + 5, i->key.buf, i->key.size);
        memcpy(nodebuf + bufpos + 5 + i->key.size, i->data.buf, i->data.size);
        bufpos = bufpos + 5 + i->data.size + i->key.size;
        if (i->pointer) {
            subtreesize += i->pointer->subtreesize;
        }
        if (i->next == NULL) {
            final_key = i->key;
        }
        i = i->next;
    }

    errcode = db_write_buf_compressed(res->rq->db, &writebuf, &diskpos);
    free(nodebuf);
    if (errcode != COUCHSTORE_SUCCESS) {
        return errcode;
    }

    if (res->node_type == KV_NODE && res->rq->reduce) {
        res->rq->reduce(reducebuf, &reducesize, res->values->next, res->count);
    }

    if (res->node_type == KP_NODE && res->rq->rereduce) {
        res->rq->rereduce(reducebuf, &reducesize, res->values->next, res->count);
    }

    node_pointer *ptr = (node_pointer *) malloc(sizeof(node_pointer) + final_key.size + reducesize);
    if (!ptr) {
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }

    ptr->key.buf = ((char *)ptr) + sizeof(node_pointer);
    ptr->reduce_value.buf = ((char *)ptr) + sizeof(node_pointer) + final_key.size;

    ptr->key.size = final_key.size;
    ptr->reduce_value.size = reducesize;

    memcpy(ptr->key.buf, final_key.buf, final_key.size);
    memcpy(ptr->reduce_value.buf, reducebuf, reducesize);

    ptr->subtreesize = subtreesize + writebuf.size;
    ptr->pointer = diskpos;

    nodelist *pel = encode_pointer(ptr);
    if (!pel) {
        free(ptr);
        return COUCHSTORE_ERROR_ALLOC_FAIL;
    }

    res->pointers_end->next = pel;
    res->pointers_end = pel;

    res->node_len = 0;
    res->count = 0;

    res->values_end = res->values;
    free_nodelist(res->values->next);
    res->values->next = NULL;

    return COUCHSTORE_SUCCESS;
}

//Move this node's pointers list to dst node's values list.
static couchstore_error_t mr_move_pointers(couchfile_modify_result *src,
                                           couchfile_modify_result *dst)
{
    couchstore_error_t errcode = COUCHSTORE_SUCCESS;
    if (src->pointers_end == src->pointers) {
        return 0;
    }

    nodelist *ptr = src->pointers->next;
    nodelist *next = ptr;
    while (ptr != NULL && errcode == 0) {
        dst->node_len += ptr->data.size + ptr->key.size + 5;
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

static couchstore_error_t modify_node(couchfile_modify_request *rq,
                                      node_pointer *nptr,
                                      int start, int end,
                                      couchfile_modify_result *dst)
{
    char *nodebuf = NULL;
    int bufpos = 1;
    int nodebuflen = 0;
    int errcode = 0;
    couchfile_modify_result *local_result = NULL;

    if (start == end) {
        return 0;
    }

    if (nptr) {
        if ((nodebuflen = pread_compressed(rq->db, nptr->pointer, (char **) &nodebuf)) < 0) {
            error_pass(COUCHSTORE_ERROR_READ);
        }
    }

    local_result = make_modres(rq);
    error_unless(local_result, COUCHSTORE_ERROR_ALLOC_FAIL);

    if (nptr == NULL || nodebuf[0] == 1) { //KV Node
        local_result->node_type = KV_NODE;
        while (bufpos < nodebuflen) {
            uint32_t klen, vlen;
            get_kvlen(nodebuf + bufpos, &klen, &vlen);
            sized_buf cmp_key = {nodebuf + bufpos + 5, klen};
            sized_buf val_buf = {nodebuf + bufpos + 5 + klen, vlen};
            bufpos += 5 + klen + vlen;
            int advance = 0;
            while (!advance && start < end) {
                advance = 1;
                int cmp_val = rq->cmp.compare(&cmp_key, rq->actions[start].key);

                if (cmp_val < 0) { //Key less than action key
                    mr_push_item(&cmp_key, &val_buf, local_result);
                } else if (cmp_val > 0) { //Key greater than action key
                    switch (rq->actions[start].type) {
                    case ACTION_INSERT:
                        local_result->modified = 1;
                        mr_push_item(rq->actions[start].key, rq->actions[start].value.data, local_result);
                        break;

                    case ACTION_REMOVE:
                        local_result->modified = 1;
                        break;

                    case ACTION_FETCH:
                        if (rq->fetch_callback) {
                            //not found
                            (*rq->fetch_callback)(rq, rq->actions[start].key, NULL, rq->actions[start].value.arg);
                        }
                    }
                    start++;
                    //Do next action on same item in the node, as our action was
                    //not >= it.
                    advance = 0;
                } else if (cmp_val == 0) { //Node key is equal to action key
                    switch (rq->actions[start].type) {
                    case ACTION_INSERT:
                        local_result->modified = 1;
                        mr_push_item(rq->actions[start].key, rq->actions[start].value.data, local_result);
                        break;

                    case ACTION_REMOVE:
                        local_result->modified = 1;
                        break;

                    case ACTION_FETCH:
                        if (rq->fetch_callback) {
                            (*rq->fetch_callback)(rq, rq->actions[start].key, &val_buf, rq->actions[start].value.arg);
                        }
                        //Do next action on same item in the node, as our action was a fetch
                        //and there may be an equivalent insert or remove
                        //following.
                        advance = 0;
                    }
                    start++;
                }
            }
            if (start == end && !advance) {
                //If we've exhausted actions then just keep this key
                mr_push_item(&cmp_key, &val_buf, local_result);
            }
        }
        while (start < end) {
            //We're at the end of a leaf node.
            switch (rq->actions[start].type) {
            case ACTION_INSERT:
                local_result->modified = 1;
                mr_push_item(rq->actions[start].key, rq->actions[start].value.data, local_result);
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
        }
    } else if (nodebuf[0] == 0) { //KP Node
        local_result->node_type = KP_NODE;
        while (bufpos < nodebuflen && start < end) {
            uint32_t klen, vlen;
            get_kvlen(nodebuf + bufpos, &klen, &vlen);
            sized_buf cmp_key = {nodebuf + bufpos + 5, klen};
            sized_buf val_buf = {nodebuf + bufpos + 5 + klen, vlen};
            bufpos += 5 + klen + vlen;
            int cmp_val = rq->cmp.compare(&cmp_key, rq->actions[start].key);
            if (bufpos == nodebuflen) {
                //We're at the last item in the kpnode, must apply all our
                //actions here.
                node_pointer *desc = read_pointer(&cmp_key, val_buf.buf);
                if (!desc) {
                    errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
                    goto cleanup;
                }

                errcode = modify_node(rq, desc, start, end, local_result);
                if (local_result->values_end->pointer != desc) {
                    free(desc);
                }
                if (errcode != COUCHSTORE_SUCCESS) {
                    goto cleanup;
                }
                break;
            }

            if (cmp_val < 0) {
                //Key in node item less than action item and not at end
                //position, so just add it and continue.
                node_pointer *add = read_pointer(&cmp_key, val_buf.buf);
                if (!add) {
                    errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
                    goto cleanup;
                }

                errcode = mr_push_pointerinfo(add, local_result);
                if (errcode != COUCHSTORE_SUCCESS) {
                    goto cleanup;
                }
            } else if (cmp_val >= 0) {
                //Found a key in the node greater than the one in the current
                //action. Descend into the pointed node with as many actions as
                //are less than the key here.
                int range_end = start;
                while (range_end < end &&
                        rq->cmp.compare(rq->actions[range_end].key, &cmp_key) <= 0) {
                    range_end++;
                }

                node_pointer *desc = read_pointer(&cmp_key, val_buf.buf);
                if (!desc) {
                    errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
                    goto cleanup;
                }

                errcode = modify_node(rq, desc, start, range_end, local_result);
                if (local_result->values_end->pointer != desc) {
                    free(desc);
                }
                start = range_end;
                if (errcode != COUCHSTORE_SUCCESS) {
                    goto cleanup;
                }
            }
        }
        while (bufpos < nodebuflen) {
            uint32_t klen, vlen;
            get_kvlen(nodebuf + bufpos, &klen, &vlen);
            sized_buf cmp_key = {nodebuf + bufpos + 5, klen};
            sized_buf val_buf = {nodebuf + bufpos + 5 + klen, vlen};
            bufpos += 5 + klen + vlen;
            node_pointer *add = read_pointer(&cmp_key, val_buf.buf);
            if (!add) {
                errcode = COUCHSTORE_ERROR_ALLOC_FAIL;
                goto cleanup;
            }

            errcode = mr_push_pointerinfo(add, local_result);
            if (errcode != COUCHSTORE_SUCCESS) {
                goto cleanup;
            }
        }
    } else {
        errcode = COUCHSTORE_ERROR_CORRUPT;
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

    if (nodebuf) {
        free(nodebuf);
    }

    return errcode;
}

static node_pointer *finish_root(couchfile_modify_request *rq,
                                 couchfile_modify_result *root_result,
                                 couchstore_error_t *errcode)
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
            ret_ptr = root_result->pointers_end->pointer;
            root_result->pointers_end->pointer = NULL;
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
                           node_pointer *root,
                           couchstore_error_t *errcode)
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

    if (root_result->values_end->pointer == root) {
        //If we got the root pointer back, remove it from the list
        //so we don't try to free it.
        root_result->values_end->pointer = NULL;
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
            ret_ptr = root_result->values_end->pointer;
            root_result->values_end->pointer = NULL;
            free_modres(root_result);
        }
    }
    return ret_ptr;
}

