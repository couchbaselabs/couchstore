//
//  arena.h
//  couchstore
//
//  Created by Jens Alfke on 4/13/12.
//  Copyright (c) 2012 Couchbase, Inc. All rights reserved.
//

#ifndef COUCH_ARENA_H
#define COUCH_ARENA_H

#include <sys/types.h>

/** Opaque arena-allocator object. */
typedef struct arena arena;

/**
 * Creates a new arena allocator.
 * @param chunk_size The size of the memory blocks the allocator sub-allocates from malloc.
 *                   Also the maximum size of allocation allowed from this arena.
 * @return The new arena object.
 */
arena* new_arena(size_t chunk_size);

/**
 * Deletes an arena and all of its memory allocations.
 */
void delete_arena(arena*);

/**
 * Allocates memory from an arena.
 * @param arena The arena to allocate from
 * @param size The number of bytes to allocate
 * @return A pointer to the allocated block, or NULL on failure.
 */
void* arena_alloc(arena*, size_t size);

/**
 * "Frees" a block allocated from an arena. This actually doesn't do anything.
 * In a debug build it at least checks that the pointers was (probably) allocated from the arena.
 * In a release build it's explicitly a no-op and there's no need to call it at all.
 */
#if DEBUG
void arena_free(arena*, void*);
#else
static inline void arena_free(arena* a, void* block) { }
#endif

#endif // COUCH_ARENA_H
