/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "enriched_chunk.h"
#include "rmutil/alloc.h"

void ResetEnrichedChunk(EnrichedChunk *chunk) {
    chunk->rev = false;
    chunk->samples.num_samples = 0;
    chunk->samples.timestamps = chunk->samples.og_timestamps;
    chunk->samples._values = chunk->samples._og_values;
}

EnrichedChunk *NewEnrichedChunk() {
    EnrichedChunk *chunk = (EnrichedChunk *)malloc(sizeof(EnrichedChunk));
    chunk->rev = false;
    chunk->samples.num_samples = 0;
    chunk->samples.size = 0;
    chunk->samples.values_per_sample = 1;
    chunk->samples.og_timestamps = NULL;
    chunk->samples._og_values = NULL;
    return chunk;
}

void ReallocSamplesArray(Samples *samples, size_t n_samples) {
    samples->size = n_samples;
    samples->og_timestamps =
        (timestamp_t *)realloc(samples->og_timestamps, n_samples * sizeof(timestamp_t));
    size_t total_values = n_samples * samples->values_per_sample;
    samples->_og_values = (double *)realloc(samples->_og_values, total_values * sizeof(double));
    samples->timestamps = samples->og_timestamps;
    samples->_values = samples->_og_values;
}

void FreeEnrichedChunk(EnrichedChunk *chunk) {
    free(chunk->samples.og_timestamps);
    free(chunk->samples._og_values);
    free(chunk);
}
