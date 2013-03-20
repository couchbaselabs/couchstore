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

#include "mapreduce.h"
#include "mapreduce_internal.h"
#include <iostream>
#include <cstring>
#include <stdlib.h>
#include <v8.h>


using namespace v8;

typedef struct {
    Persistent<Object>    jsonObject;
    Persistent<Function>  jsonParseFun;
    Persistent<Function>  stringifyFun;
    mapreduce_ctx_t       *ctx;
} isolate_data_t;


static const char *SUM_FUNCTION_STRING =
    "(function(values) {"
    "    var sum = 0;"
    "    for (var i = 0; i < values.length; ++i) {"
    "        sum += values[i];"
    "    }"
    "    return sum;"
    "})";

static const char *DATE_FUNCTION_STRING =
    // I wish it was on the prototype, but that will require bigger
    // C changes as adding to the date prototype should be done on
    // process launch. The code you see here may be faster, but it
    // is less JavaScripty.
    // "Date.prototype.toArray = (function() {"
    "(function(date) {"
    "    date = date.getUTCDate ? date : new Date(date);"
    "    return isFinite(date.valueOf()) ?"
    "      [date.getUTCFullYear(),"
    "      (date.getUTCMonth() + 1),"
    "       date.getUTCDate(),"
    "       date.getUTCHours(),"
    "       date.getUTCMinutes(),"
    "       date.getUTCSeconds()] : null;"
    "})";

static const char *BASE64_FUNCTION_STRING =
    "(function(b64) {"
    "    var i, j, l, tmp, scratch, arr = [];"
    "    var lookup = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';"
    "    if (typeof b64 !== 'string') {"
    "        throw 'Input is not a string';"
    "    }"
    "    if (b64.length % 4 > 0) {"
    "        throw 'Invalid base64 source.';"
    "    }"
    "    scratch = b64.indexOf('=');"
    "    scratch = scratch > 0 ? b64.length - scratch : 0;"
    "    l = scratch > 0 ? b64.length - 4 : b64.length;"
    "    for (i = 0, j = 0; i < l; i += 4, j += 3) {"
    "        tmp = (lookup.indexOf(b64[i]) << 18) | (lookup.indexOf(b64[i + 1]) << 12);"
    "        tmp |= (lookup.indexOf(b64[i + 2]) << 6) | lookup.indexOf(b64[i + 3]);"
    "        arr.push((tmp & 0xFF0000) >> 16);"
    "        arr.push((tmp & 0xFF00) >> 8);"
    "        arr.push(tmp & 0xFF);"
    "    }"
    "    if (scratch === 2) {"
    "        tmp = (lookup.indexOf(b64[i]) << 2) | (lookup.indexOf(b64[i + 1]) >> 4);"
    "        arr.push(tmp & 0xFF);"
    "    } else if (scratch === 1) {"
    "        tmp = (lookup.indexOf(b64[i]) << 10) | (lookup.indexOf(b64[i + 1]) << 4);"
    "        tmp |= (lookup.indexOf(b64[i + 2]) >> 2);"
    "        arr.push((tmp >> 8) & 0xFF);"
    "        arr.push(tmp & 0xFF);"
    "    }"
    "    return arr;"
    "})";


static void doInitContext(mapreduce_ctx_t *ctx);
static Persistent<Context> createJsContext();
static Handle<Function> compileFunction(const std::string &function);
static std::string exceptionString(const TryCatch &tryCatch);
static void loadFunctions(mapreduce_ctx_t *ctx,
                          const std::list<std::string> &function_sources);
static Handle<Value> emit(const Arguments &args);
static inline isolate_data_t *getIsolateData();
static inline mapreduce_json_t jsonStringify(const Handle<Value> &obj);
static inline Handle<Value> jsonParse(const mapreduce_json_t &thing);
static inline void taskStarted(mapreduce_ctx_t *ctx);
static inline void taskFinished(mapreduce_ctx_t *ctx);
static void freeKvListEntries(kv_list_int_t &kvs);
static void freeJsonListEntries(json_results_list_t &list);
static inline Handle<Array> jsonListToJsArray(const mapreduce_json_list_t &list);


void initContext(mapreduce_ctx_t *ctx,
                 const std::list<std::string> &function_sources)
{
    doInitContext(ctx);

    try {
        Locker locker(ctx->isolate);
        Isolate::Scope isolateScope(ctx->isolate);
        HandleScope handleScope;
        Context::Scope contextScope(ctx->jsContext);

        loadFunctions(ctx, function_sources);
    } catch (...) {
        destroyContext(ctx);
        throw;
    }
}


void destroyContext(mapreduce_ctx_t *ctx)
{
    {
        Locker locker(ctx->isolate);
        Isolate::Scope isolateScope(ctx->isolate);
        HandleScope handleScope;
        Context::Scope contextScope(ctx->jsContext);

        for (unsigned int i = 0; i < ctx->functions->size(); ++i) {
            (*ctx->functions)[i].Dispose();
        }
        delete ctx->functions;

        isolate_data_t *isoData = getIsolateData();
        isoData->jsonObject.Dispose();
        isoData->jsonObject.Clear();
        isoData->jsonParseFun.Dispose();
        isoData->jsonParseFun.Clear();
        isoData->stringifyFun.Dispose();
        isoData->stringifyFun.Clear();
        delete isoData;

        ctx->jsContext.Dispose();
        ctx->jsContext.Clear();
    }

    ctx->isolate->Dispose();
}


static void doInitContext(mapreduce_ctx_t *ctx)
{
    ctx->isolate = Isolate::New();
    Locker locker(ctx->isolate);
    Isolate::Scope isolateScope(ctx->isolate);
    HandleScope handleScope;

    ctx->jsContext = createJsContext();
    Context::Scope contextScope(ctx->jsContext);

    Handle<Object> jsonObject = Local<Object>::Cast(ctx->jsContext->Global()->Get(String::New("JSON")));
    Handle<Function> parseFun = Local<Function>::Cast(jsonObject->Get(String::New("parse")));
    Handle<Function> stringifyFun = Local<Function>::Cast(jsonObject->Get(String::New("stringify")));

    isolate_data_t *isoData = new isolate_data_t();
    isoData->jsonObject = Persistent<Object>::New(jsonObject);
    isoData->jsonParseFun = Persistent<Function>::New(parseFun);
    isoData->stringifyFun = Persistent<Function>::New(stringifyFun);
    isoData->ctx = ctx;

    ctx->isolate->SetData(isoData);
    ctx->taskStartTime = -1;
}


static Persistent<Context> createJsContext()
{
    HandleScope handleScope;
    Handle<ObjectTemplate> global = ObjectTemplate::New();

    global->Set(String::New("emit"), FunctionTemplate::New(emit));

    Persistent<Context> context = Context::New(NULL, global);
    Context::Scope contextScope(context);

    Handle<Function> sumFun = compileFunction(SUM_FUNCTION_STRING);
    context->Global()->Set(String::New("sum"), sumFun);

    Handle<Function> decodeBase64Fun = compileFunction(BASE64_FUNCTION_STRING);
    context->Global()->Set(String::New("decodeBase64"), decodeBase64Fun);

    Handle<Function> dateToArrayFun = compileFunction(DATE_FUNCTION_STRING);
    context->Global()->Set(String::New("dateToArray"), dateToArrayFun);

    return context;
}


void mapDoc(mapreduce_ctx_t *ctx,
            const mapreduce_json_t &doc,
            const mapreduce_json_t &meta,
            mapreduce_map_result_list_t *results)
{
    Locker locker(ctx->isolate);
    Isolate::Scope isolateScope(ctx->isolate);
    HandleScope handleScope;
    Context::Scope contextScope(ctx->jsContext);
    Handle<Value> docObject = jsonParse(doc);
    Handle<Value> metaObject = jsonParse(meta);

    if (!metaObject->IsObject()) {
        throw MapReduceError(MAPREDUCE_INVALID_ARG, "metadata is not a JSON object");
    }

    Handle<Value> funArgs[] = { docObject, metaObject };

    taskStarted(ctx);
    kv_list_int_t kvs;
    ctx->kvs = &kvs;

    for (unsigned int i = 0; i < ctx->functions->size(); ++i) {
        mapreduce_map_result_t mapResult;
        Handle<Function> fun = (*ctx->functions)[i];
        TryCatch trycatch;
        Handle<Value> result = fun->Call(fun, 2, funArgs);

        if (!result.IsEmpty()) {
            mapResult.error = MAPREDUCE_SUCCESS;
            mapResult.result.kvs.length = kvs.size();
            size_t sz = sizeof(mapreduce_kv_t) * mapResult.result.kvs.length;
            mapResult.result.kvs.kvs = (mapreduce_kv_t *) malloc(sz);
            if (mapResult.result.kvs.kvs == NULL) {
                freeKvListEntries(kvs);
                throw std::bad_alloc();
            }
            kv_list_int_t::iterator it = kvs.begin();
            for (int j = 0; it != kvs.end(); ++it, ++j) {
                mapResult.result.kvs.kvs[j] = *it;
            }
        } else {
            freeKvListEntries(kvs);

            if (!trycatch.CanContinue()) {
                throw MapReduceError(MAPREDUCE_TIMEOUT, "timeout");
            }

            mapResult.error = MAPREDUCE_RUNTIME_ERROR;
            std::string exceptString = exceptionString(trycatch);
            size_t len = exceptString.length();

            mapResult.result.error_msg = (char *) malloc(len + 1);
            if (mapResult.result.error_msg == NULL) {
                throw std::bad_alloc();
            }
            memcpy(mapResult.result.error_msg, exceptString.data(), len);
            mapResult.result.error_msg[len] = '\0';
        }

        results->list[i] = mapResult;
        results->length += 1;
        kvs.clear();
    }

    taskFinished(ctx);
}


json_results_list_t runReduce(mapreduce_ctx_t *ctx,
                              const mapreduce_json_list_t &keys,
                              const mapreduce_json_list_t &values)
{
    Locker locker(ctx->isolate);
    Isolate::Scope isolateScope(ctx->isolate);
    HandleScope handleScope;
    Context::Scope contextScope(ctx->jsContext);
    Handle<Array> keysArray = jsonListToJsArray(keys);
    Handle<Array> valuesArray = jsonListToJsArray(values);
    json_results_list_t results;

    Handle<Value> args[] = { keysArray, valuesArray, Boolean::New(false) };

    taskStarted(ctx);

    for (unsigned int i = 0; i < ctx->functions->size(); ++i) {
        Handle<Function> fun = (*ctx->functions)[i];
        TryCatch trycatch;
        Handle<Value> result = fun->Call(fun, 3, args);

        if (result.IsEmpty()) {
            freeJsonListEntries(results);

            if (!trycatch.CanContinue()) {
                throw MapReduceError(MAPREDUCE_TIMEOUT, "timeout");
            }

            throw MapReduceError(MAPREDUCE_RUNTIME_ERROR, exceptionString(trycatch));
        }

        try {
            mapreduce_json_t jsonResult = jsonStringify(result);
            results.push_back(jsonResult);
        } catch(...) {
            freeJsonListEntries(results);
            throw;
        }
    }

    taskFinished(ctx);

    return results;
}


mapreduce_json_t runReduce(mapreduce_ctx_t *ctx,
                           int reduceFunNum,
                           const mapreduce_json_list_t &keys,
                           const mapreduce_json_list_t &values)
{
    Locker locker(ctx->isolate);
    Isolate::Scope isolateScope(ctx->isolate);
    HandleScope handleScope;
    Context::Scope contextScope(ctx->jsContext);

    reduceFunNum -= 1;
    if (reduceFunNum < 0 ||
        static_cast<unsigned int>(reduceFunNum) >= ctx->functions->size()) {
        throw MapReduceError(MAPREDUCE_INVALID_ARG, "invalid reduce function number");
    }

    Handle<Function> fun = (*ctx->functions)[reduceFunNum];
    Handle<Array> keysArray = jsonListToJsArray(keys);
    Handle<Array> valuesArray = jsonListToJsArray(values);
    Handle<Value> args[] = { keysArray, valuesArray, Boolean::New(false) };

    taskStarted(ctx);

    TryCatch trycatch;
    Handle<Value> result = fun->Call(fun, 3, args);

    taskFinished(ctx);

    if (result.IsEmpty()) {
        if (!trycatch.CanContinue()) {
            throw MapReduceError(MAPREDUCE_TIMEOUT, "timeout");
        }

        throw MapReduceError(MAPREDUCE_RUNTIME_ERROR, exceptionString(trycatch));
    }

    return jsonStringify(result);
}


mapreduce_json_t runRereduce(mapreduce_ctx_t *ctx,
                             int reduceFunNum,
                             const mapreduce_json_list_t &reductions)
{
    Locker locker(ctx->isolate);
    Isolate::Scope isolateScope(ctx->isolate);
    HandleScope handleScope;
    Context::Scope contextScope(ctx->jsContext);

    reduceFunNum -= 1;
    if (reduceFunNum < 0 ||
        static_cast<unsigned int>(reduceFunNum) >= ctx->functions->size()) {
        throw MapReduceError(MAPREDUCE_INVALID_ARG, "invalid reduce function number");
    }

    Handle<Function> fun = (*ctx->functions)[reduceFunNum];
    Handle<Array> valuesArray = jsonListToJsArray(reductions);
    Handle<Value> args[] = { Null(), valuesArray, Boolean::New(true) };

    taskStarted(ctx);

    TryCatch trycatch;
    Handle<Value> result = fun->Call(fun, 3, args);

    taskFinished(ctx);

    if (result.IsEmpty()) {
        if (!trycatch.CanContinue()) {
            throw MapReduceError(MAPREDUCE_TIMEOUT, "timeout");
        }

        throw MapReduceError(MAPREDUCE_RUNTIME_ERROR, exceptionString(trycatch));
    }

    return jsonStringify(result);
}


void terminateTask(mapreduce_ctx_t *ctx)
{
    V8::TerminateExecution(ctx->isolate);
    taskFinished(ctx);
}


static void freeKvListEntries(kv_list_int_t &kvs)
{
    kv_list_int_t::iterator it = kvs.begin();

    for ( ; it != kvs.end(); ++it) {
        mapreduce_kv_t kv = *it;
        free(kv.key.json);
        free(kv.value.json);
    }
    kvs.clear();
}


static void freeJsonListEntries(json_results_list_t &list)
{
    json_results_list_t::iterator it = list.begin();

    for ( ; it != list.end(); ++it) {
        free((*it).json);
    }
    list.clear();
}


static Handle<Function> compileFunction(const std::string &funSource)
{
    HandleScope handleScope;
    TryCatch trycatch;
    Handle<String> source = String::New(funSource.data(), funSource.length());
    Handle<Script> script = Script::Compile(source);

    if (script.IsEmpty()) {
        throw MapReduceError(MAPREDUCE_SYNTAX_ERROR, exceptionString(trycatch));
    }

    Handle<Value> result = script->Run();

    if (result.IsEmpty()) {
        throw MapReduceError(MAPREDUCE_SYNTAX_ERROR, exceptionString(trycatch));
    }

    if (!result->IsFunction()) {
        throw MapReduceError(MAPREDUCE_SYNTAX_ERROR,
                             std::string("Invalid function: ") + funSource.c_str());
    }

    return handleScope.Close(Handle<Function>::Cast(result));
}


static std::string exceptionString(const TryCatch &tryCatch)
{
    HandleScope handleScope;
    String::Utf8Value exception(tryCatch.Exception());
    const char *exceptionString = (*exception);

    if (exceptionString) {
        return std::string(exceptionString);
    }

    return std::string("runtime error");
}


static void loadFunctions(mapreduce_ctx_t *ctx,
                          const std::list<std::string> &function_sources)
{
    HandleScope handleScope;

    ctx->functions = new function_vector_t();

    std::list<std::string>::const_iterator it = function_sources.begin();

    for ( ; it != function_sources.end(); ++it) {
        Handle<Function> fun = compileFunction(*it);

        ctx->functions->push_back(Persistent<Function>::New(fun));
    }
}


static Handle<Value> emit(const Arguments &args)
{
    isolate_data_t *isoData = getIsolateData();

    if (isoData->ctx->kvs == NULL) {
        return Undefined();
    }

    try {
        mapreduce_kv_t result;

        result.key   = jsonStringify(args[0]);
        result.value = jsonStringify(args[1]);
        isoData->ctx->kvs->push_back(result);

        return Undefined();
    } catch(Handle<Value> &ex) {
        return ThrowException(ex);
    }
}


static inline isolate_data_t *getIsolateData()
{
    Isolate *isolate = Isolate::GetCurrent();
    return reinterpret_cast<isolate_data_t*>(isolate->GetData());
}


static inline mapreduce_json_t jsonStringify(const Handle<Value> &obj)
{
    isolate_data_t *isoData = getIsolateData();
    Handle<Value> args[] = { obj };
    TryCatch trycatch;
    Handle<Value> result = isoData->stringifyFun->Call(isoData->jsonObject, 1, args);

    if (result.IsEmpty()) {
        throw trycatch.Exception();
    }

    mapreduce_json_t jsonResult;

    if (!result->IsUndefined()) {
        Handle<String> str = Handle<String>::Cast(result);
        jsonResult.length = str->Utf8Length();
        jsonResult.json = (char *) malloc(jsonResult.length);
        if (jsonResult.json == NULL) {
            throw std::bad_alloc();
        }
        str->WriteUtf8(jsonResult.json, jsonResult.length,
                       NULL, String::NO_NULL_TERMINATION);
    } else {
        jsonResult.length = sizeof("null") - 1;
        jsonResult.json = (char *) malloc(jsonResult.length);
        if (jsonResult.json == NULL) {
            throw std::bad_alloc();
        }
        memcpy(jsonResult.json, "null", jsonResult.length);
    }

    // Caller responsible for freeing jsonResult.json
    return jsonResult;
}


static inline Handle<Value> jsonParse(const mapreduce_json_t &thing)
{
    isolate_data_t *isoData = getIsolateData();
    Handle<Value> args[] = { String::New(thing.json, thing.length) };
    TryCatch trycatch;
    Handle<Value> result = isoData->jsonParseFun->Call(isoData->jsonObject, 1, args);

    if (result.IsEmpty()) {
        throw MapReduceError(MAPREDUCE_RUNTIME_ERROR, exceptionString(trycatch));
    }

    return result;
}


static inline void taskStarted(mapreduce_ctx_t *ctx)
{
    ctx->taskStartTime = time(NULL);
    ctx->kvs = NULL;
}


static inline void taskFinished(mapreduce_ctx_t *ctx)
{
    ctx->taskStartTime = -1;
}


static inline Handle<Array> jsonListToJsArray(const mapreduce_json_list_t &list)
{
    Handle<Array> array = Array::New(list.length);

    for (int i = 0 ; i < list.length; ++i) {
        Handle<Value> v = jsonParse(list.values[i]);
        array->Set(Number::New(i), v);
    }

    return array;
}
