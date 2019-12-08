/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#ifndef GENERIC__CHUNK_H
#define GENERIC__CHUNK_H

#include <sys/types.h>
#include <stdlib.h>         // malloc
#include <stdio.h>          // printf
#include "consts.h"

typedef void Chunk_t;
typedef void ChunkIter_t;

typedef struct Sample {
    u_int64_t timestamp;
    double value;
} Sample;

#define CHUNK_REGULAR 0
#define CHUNK_COMPRESSED 1

typedef struct ChunkFuncs {
    Chunk_t *(*NewChunk)(size_t sampleCount);
    void(*FreeChunk)(Chunk_t *chunk);

    ChunkResult(*AddSample)(Chunk_t *chunk, Sample *sample);

    ChunkIter_t *(*NewChunkIterator)(Chunk_t *chunk);
    ChunkResult(*ChunkIteratorGetNext)(ChunkIter_t *iter, Sample *sample);

    size_t(*GetChunkSize)(Chunk_t *chunk);
    u_int64_t(*GetNumOfSample)(Chunk_t *chunk);
    u_int64_t(*GetLastTimestamp)(Chunk_t *chunk);
    u_int64_t(*GetFirstTimestamp)(Chunk_t *chunk);
} ChunkFuncs;

ChunkFuncs *GetChunkClass(int chunkClass);

#endif //GENERIC__CHUNK_H