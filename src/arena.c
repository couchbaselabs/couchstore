//
//  arena.c
//  couchstore
//
//  Created by Jens Alfke on 4/13/12.
//  Copyright (c) 2012 Couchbase, Inc. All rights reserved.
//
#include "config.h"
#include "arena.h"
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdint.h>
#include <stdio.h>

#define ALIGNMENT 4                 // Byte alignment of blocks; must be a power of 2
#define PAGE_SIZE 4096              // Chunk allocation will be rounded to a multiple of this
#define DEFAULT_CHUNK_SIZE 32768    // Used if 0 is passed to new_arena
#define LOG_STATS 0                 // Set to 1 to log info about allocations when arenas are freed

typedef struct arena_chunk {
    struct arena_chunk* prev_chunk; // Link to previous chunk
    size_t size;                    // Size of available bytes after this header
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
static inline void* chunk_start(arena_chunk* chunk)
{
    return (char*)chunk + sizeof(arena_chunk);
}

// Returns a pointer to the byte past the end of a chunk's contents.
static inline void* chunk_end(arena_chunk* chunk)
{
    return (char*)chunk_start(chunk) + chunk->size;
}

// Allocates a new chunk, attaches it to the arena, and allocates 'size' bytes from it.
static void* add_chunk(arena* a, size_t size)
{
    size_t chunk_size = a->chunk_size;
    if (size > chunk_size) {
        chunk_size = size;  // make sure the new chunk is big enough to fit 'size' bytes
    }
    arena_chunk* chunk = malloc(sizeof(arena_chunk) + chunk_size);
    if (!chunk) {
        return NULL;
    }
    chunk->prev_chunk = a->cur_chunk;
    chunk->size = chunk_size;

    void* result = chunk_start(chunk);
    a->next_block = (char*)result + size;
    a->end = chunk_end(chunk);
    a->cur_chunk = chunk;
    return result;
}


arena* new_arena(size_t chunk_size)
{
    arena* a = calloc(1, sizeof(arena));
    if (a) {
        if (chunk_size == 0) {
            chunk_size = DEFAULT_CHUNK_SIZE;
        } else {
            chunk_size += sizeof(arena_chunk);
            chunk_size = (chunk_size + PAGE_SIZE) & ~(PAGE_SIZE - 1);   // round up to multiple
        }
        a->chunk_size = chunk_size - sizeof(arena_chunk);
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
        total_allocated += chunk->size;
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

void* arena_alloc_unaligned(arena* a, size_t size)
{
    void* result = a->next_block;
    a->next_block += size;
    if (a->next_block > a->end || size > a->chunk_size) {
        result = add_chunk(a, size);
    }
#ifdef DEBUG
    if (result) {
        ++a->blocks_allocated;
        a->bytes_allocated += size;
    }
#endif
    return result;
}

void* arena_alloc(arena* a, size_t size)
{
    int padding = ((intptr_t)a->next_block & (ALIGNMENT - 1));
    if (padding == 0) {
        return arena_alloc_unaligned(a, size);
    }
    padding = ALIGNMENT - padding;
    return (char*)arena_alloc_unaligned(a, size + padding) + padding;
}

#ifdef DEBUG
void arena_free(arena* a, void* block)
{
    if (block) {
        arena_chunk* chunk;
        for (chunk = a->cur_chunk; chunk; chunk = chunk->prev_chunk) {
            if (block >= chunk_start(chunk) && block < chunk_end(chunk)) {
                break;
            }
        }
        assert(chunk);
        assert(a->blocks_allocated > 0);
        --a->blocks_allocated;
    }
}
#endif

const arena_position* arena_mark(arena *a)
{
    return (const arena_position*) a->next_block;
}

void arena_free_from_mark(arena *a, const arena_position *mark)
{
    arena_chunk* chunk = a->cur_chunk;
    while (chunk && ((void*)mark < chunk_start(chunk) || (void*)mark > chunk_end(chunk))) {
        a->cur_chunk = chunk->prev_chunk;
        free(chunk);
        chunk = a->cur_chunk;
    }
    assert(chunk != NULL || mark == NULL);   // If this fails, mark was bogus

    a->next_block = (void*)mark;
    a->end = chunk ? chunk_end(chunk) : NULL;
#ifdef DEBUG
    memset(a->next_block, 0x55, (char*)a->end - (char*)a->next_block);
    // TODO: Somehow roll back blocks_allocated and bytes_allocated (how?)
#endif
}
