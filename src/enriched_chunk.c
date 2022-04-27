/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "enriched_chunk.h"
#include "rmutil/alloc.h"

void ResetEnrichedChunk(EnrichedChunk *chunk) {
    chunk->rev = false;
    chunk->samples.num_samples = 0;
    chunk->samples.timestamps = chunk->samples.og_timestamps;
    chunk->samples.values = chunk->samples.og_values;
}

EnrichedChunk *NewEnrichedChunk() {
    EnrichedChunk *chunk = (EnrichedChunk *)malloc(sizeof(EnrichedChunk));
    chunk->rev = false;
    chunk->samples.num_samples = 0;
    chunk->samples.size = 0;
    chunk->samples.og_timestamps = NULL;
    chunk->samples.og_values = NULL;
    return chunk;
}

void ReallocSamplesArray(Samples *samples, size_t n_samples) {
    samples->size = n_samples;
    samples->og_timestamps =
        (timestamp_t *)realloc(samples->og_timestamps, n_samples * sizeof(timestamp_t));
    samples->og_values = (double *)realloc(samples->og_values, n_samples * sizeof(double));
    samples->timestamps = samples->og_timestamps;
    samples->values = samples->og_values;
}

void FreeEnrichedChunk(EnrichedChunk *chunk) {
    free(chunk->samples.og_timestamps);
    free(chunk->samples.og_values);
    free(chunk);
}
