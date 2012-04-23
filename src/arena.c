//
//  arena.c
//  couchstore
//
//  Created by Jens Alfke on 4/13/12.
//  Copyright (c) 2012 Couchbase, Inc. All rights reserved.
//

#include "arena.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#define LOG_STATS 0

#if LOG_STATS
#include <stdio.h>
#endif


typedef struct arena_chunk {
    struct arena_chunk* prev_chunk;
} arena_chunk;


struct arena {
    char* next_block;           // Next block to be allocated in cur_chunk (if there's room)
    char* end;                  // End of the current chunk; can't allocate past here
    arena_chunk* cur_chunk;     // The current chunk
    size_t chunk_size;          // The size of chunks to allocate, as passed to new_arena
#ifdef DEBUG
    int blocks_allocated;       // Number of blocks allocated
    size_t bytes_allocated;     // Number of bytes allocated
#endif
};


// Returns a pointer to the first byte of a chunk's contents.
static inline void* chunk_start(arena_chunk* chunk) {
    return chunk + 1;
}

// Returns a pointer to the byte past the end of a chunk's contents.
static inline void* chunk_end(arena* a, arena_chunk* chunk) {
    return (char*)chunk_start(chunk) + a->chunk_size;
}

// Allocates a new chunk and attaches it to the arena.
static arena_chunk* add_chunk(arena* a)
{
    arena_chunk* chunk = malloc(sizeof(arena_chunk) + a->chunk_size);
    if (chunk) {
        a->next_block = chunk_start(chunk);
        a->end = chunk_end(a, chunk);
        chunk->prev_chunk = a->cur_chunk;
        a->cur_chunk = chunk;
    }
    return chunk;
}


arena* new_arena(size_t chunk_size)
{
    arena* a = malloc(sizeof(arena));
    if (a) {
        a->chunk_size = chunk_size;
        a->cur_chunk = NULL;
        a->next_block = a->end = NULL;
#ifdef DEBUG
        a->blocks_allocated = 0;
        a->bytes_allocated = 0;
#endif
    }
    return a;
}

void delete_arena(arena* a)
{
#ifdef DEBUG
    //assert(a->nblocks == 0);
    size_t total_allocated = 0;
#endif
    arena_chunk* chunk = a->cur_chunk;
    while (chunk) {
#ifdef DEBUG
        total_allocated += a->chunk_size;
#endif
        void* to_free = chunk;
        chunk = chunk->prev_chunk;
        free(to_free);
    }
#if LOG_STATS
    fprintf(stderr, "delete_arena: %zd bytes malloced for %zd bytes of data in %d blocks (%.0f%%)\n",
            total_allocated, a->bytes_allocated, a->blocks_allocated,
            a->bytes_allocated*100.0/total_allocated);
#endif
    free(a);
}

void* arena_alloc(arena* a, size_t size)
{
    assert(size <= a->chunk_size);
    void* result = a->next_block;
    a->next_block += size;
    if (a->next_block > a->end) {
        if (!add_chunk(a)) {
            return NULL;
        }
        result = a->next_block;
        a->next_block += size;
    }
#ifdef DEBUG
    ++a->blocks_allocated;
    a->bytes_allocated += size;
#endif
    return result;
}

#ifdef DEBUG
void arena_free(arena* a, void* block)
{
    if (block) {
        arena_chunk* chunk;
        for (chunk = a->cur_chunk; chunk; chunk = chunk->prev_chunk) {
            if (block >= chunk_start(chunk) && block < chunk_end(a, chunk)) {
                break;
            }
        }
        assert(chunk);
        assert(a->blocks_allocated > 0);
        --a->blocks_allocated;
    }
}
#endif
