/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
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
