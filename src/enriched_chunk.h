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

#include <stddef.h>

typedef struct Samples
{
    timestamp_t *timestamps; // array of timestamps
    double *_values; // logical layout: sample s, agg a -> flat index s * values_per_sample + a
    timestamp_t *og_timestamps; // The original buffer of timestamps
    double *_og_values;         // The original buffer of values (same layout as _values)
    unsigned int num_samples;   // num of samples contained in the array
    size_t size;                // num of maximal samples which can be contained
    size_t values_per_sample;   // number of values per sample (1 for single agg, N for multi-agg)
} Samples;

/* sample_index: row; agg_index: column in [0, values_per_sample). Usable as lvalue. */
#define Samples_value_at(s, sample_index, agg_index)                                               \
    ((s)->_values[(size_t)(sample_index) * (s)->values_per_sample + (agg_index)])

#define Samples_og_value_at(s, sample_index, agg_index)                                            \
    ((s)->_og_values[(size_t)(sample_index) * (s)->values_per_sample + (agg_index)])

/* Pointer to first of values_per_sample doubles for row sample_index */
#define Samples_values_row_ptr(s, sample_index)                                                    \
    ((s)->_values + (size_t)(sample_index) * (s)->values_per_sample)
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
