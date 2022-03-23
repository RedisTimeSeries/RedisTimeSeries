/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "consts.h"

#ifndef DOMAIN_CHUNK_H
#define DOMAIN_CHUNK_H

typedef struct Samples
{
    timestamp_t *timestamps; // array of timestamps
    double *values;          // array of values
} Samples;

typedef struct DomainChunk
{
    Samples samples;
    unsigned int num_samples;
    size_t size; // num of maximal samples which can be contained
    bool rev;
} DomainChunk;

DomainChunk *allocateDomainChunk();
void FreeDomainChunk(DomainChunk *chunk, bool free_samples);
void ReallocDomainChunk(DomainChunk *chunk, size_t n_samples);

#endif // DOMAIN_CHUNK_H
