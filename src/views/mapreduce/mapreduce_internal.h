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
 * This is a private header, do not include it in other applications/lirbaries.
 **/

#ifndef _MAPREDUCE_INTERNAL_H
#define _MAPREDUCE_INTERNAL_H

#include "mapreduce.h"
#include <iostream>
#include <string>
#include <list>
#include <vector>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <v8.h>


class MapReduceError;

typedef std::list<mapreduce_json_t>                    json_results_list_t;
typedef std::list<mapreduce_kv_t>                      kv_list_int_t;
#ifdef V8_POST_3_19_API
typedef std::vector< v8::Persistent<v8::Function>* >   function_vector_t;
#else
typedef std::vector< v8::Persistent<v8::Function> >    function_vector_t;
#endif

typedef struct {
    v8::Persistent<v8::Context> jsContext;
    v8::Isolate                 *isolate;
    function_vector_t           *functions;
    kv_list_int_t               *kvs;
    volatile time_t             taskStartTime;
} mapreduce_ctx_t;


void initContext(mapreduce_ctx_t *ctx,
                 const std::list<std::string> &function_sources);

void destroyContext(mapreduce_ctx_t *ctx);

void mapDoc(mapreduce_ctx_t *ctx,
            const mapreduce_json_t &doc,
            const mapreduce_json_t &meta,
            mapreduce_map_result_list_t *result);

json_results_list_t runReduce(mapreduce_ctx_t *ctx,
                              const mapreduce_json_list_t &keys,
                              const mapreduce_json_list_t &values);

mapreduce_json_t runReduce(mapreduce_ctx_t *ctx,
                           int reduceFunNum,
                           const mapreduce_json_list_t &keys,
                           const mapreduce_json_list_t &values);

mapreduce_json_t runRereduce(mapreduce_ctx_t *ctx,
                             int reduceFunNum,
                             const mapreduce_json_list_t &reductions);

void terminateTask(mapreduce_ctx_t *ctx);



class MapReduceError {
public:
    MapReduceError(const mapreduce_error_t error, const char *msg)
        : _error(error), _msg(msg) {
    }

    MapReduceError(const mapreduce_error_t error, const std::string &msg)
        : _error(error), _msg(msg) {
    }

    mapreduce_error_t getError() const {
        return _error;
    }

    const std::string& getMsg() const {
        return _msg;
    }

private:
    const mapreduce_error_t _error;
    const std::string _msg;
};


#endif
