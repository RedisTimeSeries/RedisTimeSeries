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
    Sample *samples;
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

Chunk_t *U_NewChunk(size_t sampleCount);
void U_FreeChunk(Chunk_t *chunk);
size_t U_GetChunkSize(Chunk_t *chunk);

// 0 for failure, 1 for success
ChunkResult U_AddSample(Chunk_t *chunk, Sample *sample);
u_int64_t U_NumOfSample(Chunk_t *chunk);
timestamp_t U_GetLastTimestamp(Chunk_t *chunk);
timestamp_t U_GetFirstTimestamp(Chunk_t *chunk);

ChunkIter_t *U_NewChunkIterator(Chunk_t *chunk);
ChunkResult U_ChunkIteratorGetNext(ChunkIter_t *iterator, Sample *sample);

#endif