/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "consts.h"

#ifndef ENRICHED_CHUNK_H
#define ENRICHED_CHUNK_H

typedef struct Samples
{
    timestamp_t *timestamps; // array of timestamps
    double *values;          // array of values
    size_t size;             // num of maximal samples which can be contained
} Samples;

typedef struct EnrichedChunk
{
    Samples samples;
    unsigned int num_samples;
    bool rev;
} EnrichedChunk;

EnrichedChunk *allocateEnrichedChunk();
void FreeEnrichedChunk(EnrichedChunk *chunk, bool free_samples);
void ReallocEnrichedChunk(EnrichedChunk *chunk, size_t n_samples);

#endif // ENRICHED_CHUNK_H
