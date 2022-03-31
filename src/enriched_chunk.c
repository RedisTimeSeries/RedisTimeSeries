/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "rmutil/alloc.h"
#include "enriched_chunk.h"

static inline void _init_enriched_chunk(EnrichedChunk *chunk) {
    chunk->num_samples = 0;
    chunk->rev = false;
    chunk->samples.size = 0;
    chunk->samples.timestamps = NULL;
    chunk->samples.values = NULL;
}

EnrichedChunk *allocateEnrichedChunk() {
    EnrichedChunk *chunk = (EnrichedChunk *)malloc(sizeof(EnrichedChunk));
    _init_enriched_chunk(chunk);
    return chunk;
}

void ReallocEnrichedChunk(EnrichedChunk *chunk, size_t n_samples) {
    chunk->samples.size = n_samples;
    chunk->samples.timestamps =
        (timestamp_t *)realloc(chunk->samples.timestamps, n_samples * sizeof(timestamp_t));
    chunk->samples.values = (double *)realloc(chunk->samples.values, n_samples * sizeof(double));
}

void FreeEnrichedChunk(EnrichedChunk *chunk, bool free_samples) {
    if (free_samples) {
        free(chunk->samples.timestamps);
        free(chunk->samples.values);
    }

    free(chunk);
}
