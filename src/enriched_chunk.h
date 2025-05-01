/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef ENRICHED_CHUNK_H
#define ENRICHED_CHUNK_H

#include "consts.h"

typedef struct Samples
{
    timestamp_t *timestamps;    // array of timestamps
    double *values;             // array of values
    timestamp_t *og_timestamps; // The original buffer of timestamps
    double *og_values;          // The original buffer of values
    unsigned int num_samples;   // num of samples contained in the array
    size_t size;                // num of maximal samples which can be contained
} Samples;

typedef struct EnrichedChunk
{
    Samples samples;
    bool rev;
} EnrichedChunk;

EnrichedChunk *NewEnrichedChunk();
void ResetEnrichedChunk(EnrichedChunk *chunk);
void FreeEnrichedChunk(EnrichedChunk *chunk);
void ReallocSamplesArray(Samples *samples, size_t n_samples);

#endif // ENRICHED_CHUNK_H
