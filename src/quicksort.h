#ifndef QUICKSORT_H
#define QUICKSORT_H

typedef int sort_cmp_t(const void *, const void *, void *);

#ifdef __cplusplus
extern "C" {
#endif
void quicksort(void *a, size_t n, size_t es, sort_cmp_t *cmp, void *thunk);
#ifdef __cplusplus
}
#endif

#endif
