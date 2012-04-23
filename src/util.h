#ifndef COUCHSTORE_UTIL_H
#define COUCHSTORE_UTIL_H

#include <string.h>
#include <signal.h>
#include <libcouchstore/couch_db.h>

#include "internal.h"
#include "fatbuf.h"

int ebin_cmp(sized_buf *e1, sized_buf *e2);
int seq_cmp(sized_buf *k1, sized_buf *k2);

#ifndef DEBUG
#define error_pass(C) if((errcode = (C)) < 0) { goto cleanup; }
#else
void report_error(couchstore_error_t, const char* file, int line);
#define error_pass(C) if((errcode = (C)) < 0) { \
                            report_error(errcode, __FILE__, __LINE__); goto cleanup; }
#endif
#define error_unless(C, E) if(!(C)) { error_pass(E); }
#define error_nonzero(C, E) if((C) != 0) { error_pass(E); }
#endif
