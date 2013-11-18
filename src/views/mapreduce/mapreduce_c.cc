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

/**
 * Implementation of all exported (public) functions, pure C.
 **/
#include "mapreduce.h"
#include "mapreduce_internal.h"
#include <iostream>
#include <map>
#include <cstring>
#include <assert.h>

#if defined(WIN32) || defined(_WIN32)
#define doSleep(Secs) Sleep(Secs * 1000)
#else
#include <unistd.h>
#define doSleep(Secs) sleep(Secs)
#endif

static const char *MEM_ALLOC_ERROR_MSG = "memory allocation failure";

static cb_thread_t terminator_thread;
static bool terminator_thread_created = false;
static volatile unsigned int terminator_timeout = 5;

static std::map<uintptr_t, mapreduce_ctx_t *> ctx_registry;

class RegistryMutex {
public:
    RegistryMutex() {
        cb_mutex_initialize(&mutex);
    }
    ~RegistryMutex() {
        cb_mutex_destroy(&mutex);
    }
    void lock() {
        cb_mutex_enter(&mutex);
    }
    void unlock() {
        cb_mutex_exit(&mutex);
    }
private:
    cb_mutex_t mutex;
};

static RegistryMutex registryMutex;


static mapreduce_error_t start_context(const char *functions[],
                                       int num_functions,
                                       void **context,
                                       char **error_msg);

static void make_function_list(const char *sources[],
                               int num_sources,
                               std::list<std::string> &list);

static void copy_error_msg(const std::string &msg, char **to);

static void register_ctx(mapreduce_ctx_t *ctx);
static void unregister_ctx(mapreduce_ctx_t *ctx);
static void terminator_loop(void *);


LIBMAPREDUCE_API
mapreduce_error_t mapreduce_start_map_context(const char *map_functions[],
                                              int num_functions,
                                              void **context,
                                              char **error_msg)
{
    return start_context(map_functions, num_functions, context, error_msg);
}


LIBMAPREDUCE_API
mapreduce_error_t mapreduce_map(void *context,
                                const mapreduce_json_t *doc,
                                const mapreduce_json_t *meta,
                                mapreduce_map_result_list_t **result)
{
    mapreduce_ctx_t *ctx = (mapreduce_ctx_t *) context;

    *result = (mapreduce_map_result_list_t *) malloc(sizeof(**result));
    if (*result == NULL) {
        return MAPREDUCE_ALLOC_ERROR;
    }

    int num_funs = ctx->functions->size();
    size_t sz = sizeof(mapreduce_map_result_t) * num_funs;
    (*result)->list = (mapreduce_map_result_t *) malloc(sz);

    if ((*result)->list == NULL) {
        free(*result);
        *result = NULL;
        return MAPREDUCE_ALLOC_ERROR;
    }

    (*result)->length = 0;
    try {
        mapDoc(ctx, *doc, *meta, *result);
    } catch (MapReduceError &e) {
        mapreduce_free_map_result_list(*result);
        *result = NULL;
        return e.getError();
    } catch (std::bad_alloc &) {
        mapreduce_free_map_result_list(*result);
        *result = NULL;
        return MAPREDUCE_ALLOC_ERROR;
    }

    assert((*result)->length == num_funs);
    return MAPREDUCE_SUCCESS;
}


LIBMAPREDUCE_API
mapreduce_error_t mapreduce_start_reduce_context(const char *reduce_functions[],
                                                 int num_functions,
                                                 void **context,
                                                 char **error_msg)
{
    return start_context(reduce_functions, num_functions, context, error_msg);
}


LIBMAPREDUCE_API
mapreduce_error_t mapreduce_reduce_all(void *context,
                                       const mapreduce_json_list_t *keys,
                                       const mapreduce_json_list_t *values,
                                       mapreduce_json_list_t **result,
                                       char **error_msg)
{
    mapreduce_ctx_t *ctx = (mapreduce_ctx_t *) context;

    try {
        json_results_list_t list = runReduce(ctx, *keys, *values);
        size_t sz = list.size();
        json_results_list_t::iterator it = list.begin();

        assert(sz == ctx->functions->size());

        *result = (mapreduce_json_list_t *) malloc(sizeof(**result));
        if (*result == NULL) {
            for ( ; it != list.end(); ++it) {
                free((*it).json);
            }
            throw std::bad_alloc();
        }

        (*result)->length = sz;
        (*result)->values = (mapreduce_json_t *) malloc(sizeof(mapreduce_json_t) * sz);
        if ((*result)->values == NULL) {
            free(*result);
            for ( ; it != list.end(); ++it) {
                free((*it).json);
            }
            throw std::bad_alloc();
        }
        for (int i = 0; it != list.end(); ++it, ++i) {
            (*result)->values[i] = *it;
        }
    } catch (MapReduceError &e) {
        copy_error_msg(e.getMsg(), error_msg);
        *result = NULL;
        return e.getError();
    } catch (std::bad_alloc &) {
        copy_error_msg(MEM_ALLOC_ERROR_MSG, error_msg);
        *result = NULL;
        return MAPREDUCE_ALLOC_ERROR;
    }

    *error_msg = NULL;
    return MAPREDUCE_SUCCESS;
}


LIBMAPREDUCE_API
mapreduce_error_t mapreduce_reduce(void *context,
                                   int reduceFunNum,
                                   const mapreduce_json_list_t *keys,
                                   const mapreduce_json_list_t *values,
                                   mapreduce_json_t **result,
                                   char **error_msg)
{
    mapreduce_ctx_t *ctx = (mapreduce_ctx_t *) context;

    try {
        mapreduce_json_t red = runReduce(ctx, reduceFunNum, *keys, *values);

        *result = (mapreduce_json_t *) malloc(sizeof(**result));
        if (*result == NULL) {
            free(red.json);
            throw std::bad_alloc();
        }
        **result = red;
    } catch (MapReduceError &e) {
        copy_error_msg(e.getMsg(), error_msg);
        *result = NULL;
        return e.getError();
    } catch (std::bad_alloc &) {
        copy_error_msg(MEM_ALLOC_ERROR_MSG, error_msg);
        *result = NULL;
        return MAPREDUCE_ALLOC_ERROR;
    }

    *error_msg = NULL;
    return MAPREDUCE_SUCCESS;
}


LIBMAPREDUCE_API
mapreduce_error_t mapreduce_rereduce(void *context,
                                     int reduceFunNum,
                                     const mapreduce_json_list_t *reductions,
                                     mapreduce_json_t **result,
                                     char **error_msg)
{
    mapreduce_ctx_t *ctx = (mapreduce_ctx_t *) context;

    try {
        mapreduce_json_t red = runRereduce(ctx, reduceFunNum, *reductions);

        *result = (mapreduce_json_t *) malloc(sizeof(**result));
        if (*result == NULL) {
            free(red.json);
            throw std::bad_alloc();
        }
        **result = red;
    } catch (MapReduceError &e) {
        copy_error_msg(e.getMsg(), error_msg);
        *result = NULL;
        return e.getError();
    } catch (std::bad_alloc &) {
        copy_error_msg(MEM_ALLOC_ERROR_MSG, error_msg);
        *result = NULL;
        return MAPREDUCE_ALLOC_ERROR;
    }

    *error_msg = NULL;
    return MAPREDUCE_SUCCESS;
}


LIBMAPREDUCE_API
void mapreduce_free_context(void *context)
{
    if (context != NULL) {
        mapreduce_ctx_t *ctx = (mapreduce_ctx_t *) context;

        unregister_ctx(ctx);
        destroyContext(ctx);
        delete ctx;
    }
}


LIBMAPREDUCE_API
void mapreduce_free_json(mapreduce_json_t *value)
{
    if (value != NULL) {
        free(value->json);
        free(value);
    }
}


LIBMAPREDUCE_API
void mapreduce_free_json_list(mapreduce_json_list_t *list)
{
    if (list != NULL) {
        for (int i = 0; i < list->length; ++i) {
            free(list->values[i].json);
        }
        free(list->values);
        free(list);
    }
}


LIBMAPREDUCE_API
void mapreduce_free_map_result_list(mapreduce_map_result_list_t *list)
{
    if (list == NULL) {
        return;
    }

    for (int i = 0; i < list->length; ++i) {
        mapreduce_map_result_t mr = list->list[i];

        switch (mr.error) {
        case MAPREDUCE_SUCCESS:
            {
                mapreduce_kv_list_t kvs = mr.result.kvs;

                for (int j = 0; j < kvs.length; ++j) {
                    mapreduce_kv_t kv = kvs.kvs[j];
                    free(kv.key.json);
                    free(kv.value.json);
                }
                free(kvs.kvs);
            }
            break;
        default:
            free(mr.result.error_msg);
            break;
        }
    }

    free(list->list);
    free(list);
}


LIBMAPREDUCE_API
void mapreduce_free_error_msg(char *error_msg)
{
    free(error_msg);
}


LIBMAPREDUCE_API
void mapreduce_set_timeout(unsigned int seconds)
{
    terminator_timeout = seconds;
}


static mapreduce_error_t start_context(const char *functions[],
                                       int num_functions,
                                       void **context,
                                       char **error_msg)
{
    mapreduce_ctx_t *ctx = NULL;
    mapreduce_error_t ret = MAPREDUCE_SUCCESS;

    try {
        ctx = new mapreduce_ctx_t();
        std::list<std::string> functions_list;

        make_function_list(functions, num_functions, functions_list);
        initContext(ctx, functions_list);
    } catch (MapReduceError &e) {
        copy_error_msg(e.getMsg(), error_msg);
        ret = e.getError();
    } catch (std::bad_alloc &) {
        copy_error_msg(MEM_ALLOC_ERROR_MSG, error_msg);
        ret = MAPREDUCE_ALLOC_ERROR;
    }

    if (ret == MAPREDUCE_SUCCESS) {
        register_ctx(ctx);
        *context = (void *) ctx;
        *error_msg = NULL;
    } else {
        delete ctx;
    }
    return ret;
}


static void make_function_list(const char *sources[],
                               int num_sources,
                               std::list<std::string> &list)
{
    for (int i = 0; i < num_sources; ++i) {
        std::string source;
        size_t len = strlen(sources[i]);

        source.reserve(1 + len + 1);
        source += '(';
        source.append(sources[i], len);
        source += ')';

        list.push_back(source);
    }
}


static void copy_error_msg(const std::string &msg, char **to)
{
    if (to != NULL) {
        size_t len = msg.length();

        *to = (char *) malloc(len + 1);
        if (*to != NULL) {
            msg.copy(*to, len);
            (*to)[len] = '\0';
        }
    }
}


static void register_ctx(mapreduce_ctx_t *ctx)
{
    uintptr_t key = reinterpret_cast<uintptr_t>(ctx);
    registryMutex.lock();

    if (!terminator_thread_created) {
        int ret = cb_create_thread(&terminator_thread, terminator_loop, NULL, 1);
        if (ret != 0) {
            std::cerr << "Error creating terminator thread: " << ret << std::endl;
            exit(1);
        }
        terminator_thread_created = true;
    }

    ctx_registry[key] = ctx;
    registryMutex.unlock();
}


static void unregister_ctx(mapreduce_ctx_t *ctx)
{
    uintptr_t key = reinterpret_cast<uintptr_t>(ctx);

    registryMutex.lock();
    ctx_registry.erase(key);
    registryMutex.unlock();
}


static void terminator_loop(void *)
{
    std::map<uintptr_t, mapreduce_ctx_t *>::iterator it;
    time_t now;

    while (true) {
        registryMutex.lock();
        now = time(NULL);
        for (it = ctx_registry.begin(); it != ctx_registry.end(); ++it) {
            mapreduce_ctx_t *ctx = (*it).second;

            if (ctx->taskStartTime >= 0) {
                if (ctx->taskStartTime + terminator_timeout < now) {
                    terminateTask(ctx);
                }
            }
        }

        registryMutex.unlock();
        doSleep(terminator_timeout);
    }
}
