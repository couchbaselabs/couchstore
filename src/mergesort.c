/* Merge Sort
   by Philip J. Erdelsky
   pje@efgh.com
   http://www.efgh.com/software/mergesor.htm
*/
#include "config.h"
#include "internal.h"
#include "mergesort.h"

#include <stdlib.h>
#include <string.h>
struct record_in_memory {
    struct record_in_memory *next;
    char record[1];
};

struct compare_info {
    int (*compare)(void *, void *, void *);
    void *pointer;
};

static void free_memory_blocks(struct record_in_memory *first)
{
    while (first != NULL) {
        struct record_in_memory *next = first->next;
        free(first);
        first = next;
    }
}

static int compare_records(void *p, void *q, void *pointer)
{
#define pp ((struct record_in_memory *) p)
#define qq ((struct record_in_memory *) q)
#define point ((struct compare_info *) pointer)
    return (*point->compare)(pp->record, qq->record, point->pointer);
}

#define OK                   COUCHSTORE_SUCCESS
#define INSUFFICIENT_MEMORY  COUCHSTORE_ERROR_ALLOC_FAIL
#define FILE_CREATION_ERROR  COUCHSTORE_ERROR_OPEN_FILE
#define FILE_WRITE_ERROR     COUCHSTORE_ERROR_WRITE

int merge_sort(FILE *unsorted_file, FILE *sorted_file,
               int (*read)(FILE *, void *, void *),
               int (*write)(FILE *, void *, void *),
               int (*compare)(void *, void *, void *), void *pointer,
               unsigned max_record_size, unsigned long block_size, unsigned long *pcount)
{
    struct tape {
        FILE *fp;
        unsigned long count;
    };
    struct tape source_tape[2];
    char *record[2];
    /* allocate memory */
    if ((record[0] = malloc(max_record_size)) == NULL) {
        return INSUFFICIENT_MEMORY;
    }
    if ((record[1] = malloc(max_record_size)) == NULL) {
        free(record[0]);
        return INSUFFICIENT_MEMORY;
    }
    /* create temporary files source_tape[0] and source_tape[1] */
    source_tape[0].fp = tmpfile();
    source_tape[0].count = 0L;
    if (source_tape[0].fp == NULL) {
        free(record[0]);
        free(record[1]);
        return FILE_CREATION_ERROR;
    }
    source_tape[1].fp = tmpfile();
    source_tape[1].count = 0L;
    if (source_tape[1].fp == NULL) {
        fclose(source_tape[0].fp);
        free(record[0]);
        free(record[1]);
        return FILE_CREATION_ERROR;
    }
    /* read blocks, sort them in memory, and write the alternately to */
    /* tapes 0 and 1 */
    {
        struct record_in_memory *first = NULL;
        unsigned long block_count = 0;
        unsigned destination = 0;
        struct compare_info comp;
        comp.compare = compare;
        comp.pointer = pointer;
        while (1) {
            int record_size = (*read)(unsorted_file, record[0], pointer);
            if (record_size > 0) {
                struct record_in_memory *p = (struct record_in_memory *)
                                             malloc(sizeof(struct record_in_memory *) + record_size);
                if (p == NULL) {
                    fclose(source_tape[0].fp);
                    fclose(source_tape[1].fp);
                    free(record[0]);
                    free(record[1]);
                    free_memory_blocks(first);
                    return INSUFFICIENT_MEMORY;
                }
                p->next = first;
                memcpy(p->record, record[0], record_size);
                first = p;
                block_count++;
            }
            if (block_count == block_size || (record_size == 0 && block_count != 0)) {
                first = sort_linked_list(first, 0, compare_records, &comp, NULL);
                while (first != NULL) {
                    struct record_in_memory *next = first->next;
                    if ((*write)(source_tape[destination].fp, first->record,
                                 pointer) == 0) {
                        fclose(source_tape[0].fp);
                        fclose(source_tape[1].fp);
                        free(record[0]);
                        free(record[1]);
                        free_memory_blocks(first);
                        return FILE_WRITE_ERROR;
                    }
                    source_tape[destination].count++;
                    free(first);
                    first = next;
                }
                destination ^= 1;
                block_count = 0;
            }
            if (record_size == 0) {
                break;
            }
        }
    }
    if (sorted_file == unsorted_file) {
        rewind(unsorted_file);
    }
    rewind(source_tape[0].fp);
    rewind(source_tape[1].fp);
    /* delete the unsorted file here, if required (see instructions) */
    /* handle case where memory sort is all that is required */
    if (source_tape[1].count == 0L) {
        fclose(source_tape[1].fp);
        source_tape[1] = source_tape[0];
        source_tape[0].fp = sorted_file;
        while (source_tape[1].count-- != 0L) {
            (*read)(source_tape[1].fp, record[0], pointer);
            if ((*write)(source_tape[0].fp, record[0], pointer) == 0) {
                fclose(source_tape[1].fp);
                free(record[0]);
                free(record[1]);
                return FILE_WRITE_ERROR;
            }
        }
    } else {
        /* merge tapes, two by two, until every record is in source_tape[0] */
        while (source_tape[1].count != 0L) {
            unsigned destination = 0;
            struct tape destination_tape[2];
            destination_tape[0].fp = source_tape[0].count <= block_size ?
                                     sorted_file : tmpfile();
            destination_tape[0].count = 0L;
            if (destination_tape[0].fp == NULL) {
                fclose(source_tape[0].fp);
                fclose(source_tape[1].fp);
                free(record[0]);
                free(record[1]);
                return FILE_CREATION_ERROR;
            }
            destination_tape[1].fp = tmpfile();
            destination_tape[1].count = 0L;
            if (destination_tape[1].fp == NULL) {
                if (destination_tape[0].fp != sorted_file) {
                    fclose(destination_tape[0].fp);
                }
                fclose(source_tape[0].fp);
                fclose(source_tape[1].fp);
                free(record[0]);
                free(record[1]);
                return FILE_CREATION_ERROR;
            }
            (*read)(source_tape[0].fp, record[0], pointer);
            (*read)(source_tape[1].fp, record[1], pointer);
            while (source_tape[0].count != 0L) {
                unsigned long count[2];
                count[0] = source_tape[0].count;
                if (count[0] > block_size) {
                    count[0] = block_size;
                }
                count[1] = source_tape[1].count;
                if (count[1] > block_size) {
                    count[1] = block_size;
                }
                while (count[0] + count[1] != 0) {
                    unsigned select = count[0] == 0 ? 1 : count[1] == 0 ? 0 :
                                      compare(record[0], record[1], pointer) < 0 ? 0 : 1;
                    if ((*write)(destination_tape[destination].fp, record[select],
                                 pointer) == 0) {
                        if (destination_tape[0].fp != sorted_file) {
                            fclose(destination_tape[0].fp);
                        }
                        fclose(destination_tape[1].fp);
                        fclose(source_tape[0].fp);
                        fclose(source_tape[1].fp);
                        free(record[0]);
                        free(record[1]);
                        return FILE_WRITE_ERROR;
                    }
                    if (source_tape[select].count > 1L) {
                        (*read)(source_tape[select].fp, record[select], pointer);
                    }
                    source_tape[select].count--;
                    count[select]--;
                    destination_tape[destination].count++;
                }
                destination ^= 1;
            }
            fclose(source_tape[0].fp);
            fclose(source_tape[1].fp);
            if (fflush(destination_tape[0].fp) == EOF ||
                    fflush(destination_tape[1].fp) == EOF) {
                if (destination_tape[0].fp != sorted_file) {
                    fclose(destination_tape[0].fp);
                }
                fclose(destination_tape[1].fp);
                free(record[0]);
                free(record[1]);
                return FILE_WRITE_ERROR;
            }
            rewind(destination_tape[0].fp);
            rewind(destination_tape[1].fp);
            memcpy(source_tape, destination_tape, sizeof(source_tape));
            block_size <<= 1;
        }
    }
    fclose(source_tape[1].fp);
    if (pcount != NULL) {
        *pcount = source_tape[0].count;
    }
    free(record[0]);
    free(record[1]);
    return OK;
}

