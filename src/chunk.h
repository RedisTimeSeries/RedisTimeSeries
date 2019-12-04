/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/
#ifndef CHUNK_H
#define CHUNK_H

#include "consts.h"
#include "generic_chunk.h"
#include <sys/types.h>

typedef struct Chunk {
    timestamp_t base_timestamp;
    void * samples;
    short num_samples;
    short max_samples;
    struct Chunk *nextChunk;
    // struct Chunk *prevChunk;
} Chunk;

typedef struct ChunkIterator {
    Chunk *chunk;
    int currentIndex;
    timestamp_t lastTimestamp;
    int lastValue;
} ChunkIterator;

Chunk_t *NewChunk(size_t sampleCount);
void FreeChunk(Chunk_t *chunk);
size_t GetChunkSize(Chunk_t *chunk);

// 0 for failure, 1 for success
int ChunkAddSample(Chunk_t *chunk, Sample *sample);
u_int64_t ChunkNumOfSample(Chunk_t *chunk);
timestamp_t ChunkGetLastTimestamp(Chunk_t *chunk);
timestamp_t ChunkGetFirstTimestamp(Chunk_t *chunk);

ChunkIter_t *NewChunkIterator(Chunk_t *chunk);
int ChunkIteratorGetNext(ChunkIter_t *iterator, Sample *sample);

#endif