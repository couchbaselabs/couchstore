#ifndef MERGESORT_H
#define MERGESORT_H
#include <stdio.h>
void *sort_linked_list(void *, unsigned, int (*)(void *, void *, void *), void *, unsigned long *);

int merge_sort(FILE *unsorted_file, FILE *sorted_file,
               int (*read)(FILE *, void *, void *),
               int (*write)(FILE *, void *, void *),
               int (*compare)(void *, void *, void *), void *pointer,
               unsigned max_record_size, unsigned long block_size, unsigned long *pcount);
#endif
