/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/
#ifndef CHUNK_H
#define CHUNK_H

#include "chunk.h"
#include "consts.h"
#include <sys/types.h>

typedef struct Sample {
    timestamp_t timestamp;
    double data;
} Sample;

typedef struct Chunk
{
    timestamp_t base_timestamp;
    void * samples;
    short num_samples;
    short max_samples;
    struct Chunk *nextChunk;
    // struct Chunk *prevChunk;
} Chunk;

typedef struct ChunkIterator
{
    Chunk *chunk;
    int currentIndex;
    timestamp_t lastTimestamp;
    int lastValue;
} ChunkIterator;

Chunk * NewChunk(size_t sampleCount);
void FreeChunk(Chunk *chunk);

// 0 for failure, 1 for success
int ChunkAddSample(Chunk *chunk, Sample sample);
int IsChunkFull(Chunk *chunk);
int ChunkNumOfSample(Chunk *chunk);
timestamp_t ChunkGetLastTimestamp(Chunk *chunk);
timestamp_t ChunkGetFirstTimestamp(Chunk *chunk);

ChunkIterator NewChunkIterator(Chunk* chunk, int initIndex);
int ChunkIteratorGetNext(ChunkIterator *iter, Sample* sample);
int ChunkIteratorGetPrev(ChunkIterator *iter, Sample* sample);
#endif