#ifndef TESTS_MACROS_H
#define TESTS_MACROS_H

#define assert(expr)                                        \
    do {                                                    \
        if (!(expr)) {                                      \
            fprintf(stderr, "%s:%d: assertion failed\n",    \
                    __FILE__, __LINE__);                    \
            fflush(stderr);                                 \
            abort();                                        \
        }                                                   \
    }  while (0)

#define try(C) if((errcode = (C)) < 0) { \
                            fprintf(stderr, "Couchstore error `%s' at %s:%d\r\n", \
                            couchstore_strerror(errcode), __FILE__, __LINE__); goto cleanup; }

#define error_unless(C, E) if(!(C)) { try(E); }

#endif
