#ifndef COUCHSTORE_JSON_REDUCE_H
#define COUCHSTORE_JSON_REDUCE_H

#include <libcouchstore/couch_common.h>

typedef struct JSONReducer {
    void (*init)(sized_buf buffer);
    void (*add)(sized_buf buffer, const sized_buf key, const sized_buf value);
    void (*add_reduced)(sized_buf buffer, const sized_buf reduced);
    size_t (*finish)(sized_buf buffer);
} JSONReducer;


extern const JSONReducer JSONCountReducer;
extern const JSONReducer JSONSumReducer;
extern const JSONReducer JSONStatsReducer;

#endif
