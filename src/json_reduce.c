#include "config.h"
#include "json_reduce.h"
#include <assert.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// See <http://wiki.apache.org/couchdb/Built-In_Reduce_Functions>


// Format string to use for outputting doubles. This prints as many significant figures as
// possible without quickly getting into rounding errors (like "14.99999999999999".)
#define DOUBLE_FMT "%.15lg"


static bool buf_to_str(sized_buf buf, char str[32]) {
    if (buf.size < 1 || buf.size > 31)
        return false;
    memcpy(str, buf.buf, buf.size);
    str[buf.size] = '\0';
    return true;
}

static bool buf_to_uint64(sized_buf buf, uint64_t* out_num) {
    char str[32];
    if (!buf_to_str(buf, str))
        return false;
    char* end;
    *out_num = strtoull(str, &end, 10);
    return (end > str);
}

static bool buf_to_double(sized_buf buf, double* out_num) {
    char str[32];
    if (!buf_to_str(buf, str))
        return false;
    char* end;
    *out_num = strtod(str, &end);
    return (end > str);
}


//// COUNT:

static void init_count(sized_buf buffer)
{
    buffer.size = sizeof(uint64_t);
    *(uint64_t*)buffer.buf = 0;
}

static void add_count(sized_buf buffer, const sized_buf key, const sized_buf value)
{
    ++ *(uint64_t*)buffer.buf;
}

static void add_reduced_count(sized_buf buffer, const sized_buf reduced_value)
{
    uint64_t reduced_count;
    bool parsed = buf_to_uint64(reduced_value, &reduced_count);
    assert(parsed);
    *(uint64_t*)buffer.buf += reduced_count;
}

static size_t finish_count(sized_buf buffer)
{
    uint64_t count = *(uint64_t*)buffer.buf;
    return sprintf(buffer.buf, "%llu", count);
}

const JSONReducer JSONCountReducer = {&init_count, &add_count, &add_reduced_count, &finish_count};


//// SUM:

static void init_sum(sized_buf buffer)
{
    buffer.size = sizeof(double);
    *(double*)buffer.buf = 0.0;
}

static void add_sum(sized_buf buffer, const sized_buf key, const sized_buf value)
{
    double n;
    if (buf_to_double(value, &n))
        *(double*)buffer.buf += n;
}

static void add_reduced_sum(sized_buf buffer, const sized_buf reduced_value)
{
    double reduced_sum;
    bool parsed = buf_to_double(reduced_value, &reduced_sum);
    assert(parsed);
    *(double*)buffer.buf += reduced_sum;
}

static size_t finish_sum(sized_buf buffer)
{
    double sum = *(double*)buffer.buf;
    return sprintf(buffer.buf, DOUBLE_FMT, sum);
}

const JSONReducer JSONSumReducer = {&init_sum, &add_sum, &add_reduced_sum, &finish_sum};


//// STATS:

typedef struct {
    uint64_t count;
    double sum, min, max, sumsqr;
} stats;

static void init_stats(sized_buf buffer)
{
    buffer.size = sizeof(stats);
    memset(buffer.buf, 0, sizeof(stats));
}

static void add_stats(sized_buf buffer, const sized_buf key, const sized_buf value)
{
    double n;
    if (buf_to_double(value, &n)) {
        stats* s = (stats*)buffer.buf;
        s->sum += n;
        s->sumsqr += n * n;
        if (s->count++ == 0) {
            s->min = s->max = n;
        } else if (n > s->max) {
            s->max = n;
        } else if (n < s->min) {
            s->min = n;
        }
    }
}

static void add_reduced_stats(sized_buf buffer, const sized_buf reduced_value)
{
    stats reduced;
    int scanned = sscanf(reduced_value.buf,
                         "{\"count\":%llu,\"max\":%lg,\"min\":%lg,\"sum\":%lg,\"sumsqr\":%lg}",
                         &reduced.count, &reduced.max, &reduced.min, &reduced.sum, &reduced.sumsqr);
    assert(scanned == 5);
    stats* s = (stats*)buffer.buf;
    if (reduced.min < s->min || s->count == 0)
        s->min = reduced.min;
    if (reduced.max > s->max || s->count == 0)
        s->max = reduced.max;
    s->count += reduced.count;
    s->sum += reduced.sum;
    s->sumsqr += reduced.sumsqr;
}

static size_t finish_stats(sized_buf buffer)
{
    stats* s = (stats*)buffer.buf;
    size_t size = sprintf(buffer.buf,
                   "{\"count\":%llu,\"max\":"DOUBLE_FMT",\"min\":"DOUBLE_FMT",\"sum\":"DOUBLE_FMT
                          ",\"sumsqr\":"DOUBLE_FMT"}",
                   s->count, s->max, s->min, s->sum, s->sumsqr);
    assert(size <= buffer.size);
    return size;
}

const JSONReducer JSONStatsReducer = {&init_stats, &add_stats, &add_reduced_stats, &finish_stats};
