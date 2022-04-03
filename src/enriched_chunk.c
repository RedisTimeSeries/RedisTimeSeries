/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "rmutil/alloc.h"
#include "enriched_chunk.h"

void ResetEnrichedChunk(EnrichedChunk *chunk) {
    chunk->num_samples = 0;
    chunk->rev = false;
    chunk->samples.timestamps = chunk->samples.og_timestamps;
    chunk->samples.values = chunk->samples.og_values;
}

EnrichedChunk *NewEnrichedChunk() {
    EnrichedChunk *chunk = (EnrichedChunk *)malloc(sizeof(EnrichedChunk));
    chunk->num_samples = 0;
    chunk->rev = false;
    chunk->samples.size = 0;
    chunk->samples.og_timestamps = NULL;
    chunk->samples.og_values = NULL;
    return chunk;
}

void ReallocEnrichedChunk(EnrichedChunk *chunk, size_t n_samples) {
    chunk->samples.size = n_samples;
    chunk->samples.og_timestamps =
        (timestamp_t *)realloc(chunk->samples.og_timestamps, n_samples * sizeof(timestamp_t));
    chunk->samples.og_values =
        (double *)realloc(chunk->samples.og_values, n_samples * sizeof(double));
}

void FreeEnrichedChunk(EnrichedChunk *chunk) {
    free(chunk->samples.og_timestamps);
    free(chunk->samples.og_values);
    free(chunk);
}
