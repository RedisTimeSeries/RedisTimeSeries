/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "rmutil/alloc.h"
#include "domain_chunk.h"

static inline void _init_domain_chunk(DomainChunk *chunk) {
    chunk->num_samples = 0;
    chunk->rev = false;
    chunk->size = 0;
    chunk->samples.timestamps = NULL;
    chunk->samples.values = NULL;
}

DomainChunk *allocateDomainChunk() {
    DomainChunk *chunk = (DomainChunk *)malloc(sizeof(DomainChunk));
    _init_domain_chunk(chunk);
    return chunk;
}

void ReallocDomainChunk(DomainChunk *chunk, size_t n_samples) {
    _init_domain_chunk(chunk);
    chunk->size = n_samples;
    chunk->samples.timestamps =
        (timestamp_t *)realloc(chunk->samples.timestamps, n_samples * sizeof(timestamp_t));
    chunk->samples.values = (double *)realloc(chunk->samples.values, n_samples * sizeof(double));
}

void FreeDomainChunk(DomainChunk *chunk, bool free_samples) {
    if (free_samples && chunk->samples.timestamps) {
        free(chunk->samples.timestamps);
    }
    if (free_samples && chunk->samples.values) {
        free(chunk->samples.values);
    }
    free(chunk);
}
