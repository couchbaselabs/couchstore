#ifndef TESTS_MACROS_H
#define TESTS_MACROS_H

/** Custom assert() macro -- can't use the one in <assert.h> because it's disabled when
    NDEBUG is defined. */
#undef assert
#define assert(expr)                                        \
    do {                                                    \
        if (!(expr)) {                                      \
            fprintf(stderr, "%s:%d: assertion failed\n",    \
                    __FILE__, __LINE__);                    \
            fflush(stderr);                                 \
            abort();                                        \
        }                                                   \
    }  while (0)

#define try(C)                                                      \
    do {                                                            \
        if((errcode = (C)) < 0) {                                   \
            fprintf(stderr, "Couchstore error `%s' at %s:%d\r\n",   \
            couchstore_strerror(errcode), __FILE__, __LINE__);      \
            goto cleanup;                                           \
        }                                                           \
    } while (0)

#define error_unless(C, E)                                  \
    do {                                                    \
        if(!(C)) { try(E); }                                \
    } while (0)

#endif
