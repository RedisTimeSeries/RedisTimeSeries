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
#include <string.h>         // memcpy, memmove
#include "consts.h"

typedef struct Sample {
    timestamp_t timestamp;
    double value;
} Sample;

typedef void Chunk_t;
typedef void ChunkIter_t;

typedef enum {
    CHUNK_REGULAR,
    CHUNK_COMPRESSED 
} CHUNK_TYPES_T;

typedef struct UpsertCtx {
    Sample sample;
    Chunk_t *inChunk;       // original chunk  
    short maxSamples;       // used for split
    bool latestChunk;       // used for split
} UpsertCtx;

typedef struct ChunkFuncs {
    Chunk_t *(*NewChunk)(size_t sampleCount);
    void(*FreeChunk)(Chunk_t *chunk);
    Chunk_t *(*SplitChunk)(Chunk_t *chunk);

    ChunkResult(*AddSample)(Chunk_t *chunk, Sample *sample);
    ChunkResult(*UpsertSample)(UpsertCtx *uCtx, int *size);

    ChunkIter_t *(*NewChunkIterator)(Chunk_t *chunk, bool rev);
    void(*FreeChunkIterator)(ChunkIter_t *iter, bool rev);
    ChunkResult(*ChunkIteratorGetNext)(ChunkIter_t *iter, Sample *sample);
    ChunkResult(*ChunkIteratorGetPrev)(ChunkIter_t *iter, Sample *sample);

    size_t(*GetChunkSize)(Chunk_t *chunk);
    u_int64_t(*GetNumOfSample)(Chunk_t *chunk);
    u_int64_t(*GetLastTimestamp)(Chunk_t *chunk);
    u_int64_t(*GetFirstTimestamp)(Chunk_t *chunk);
} ChunkFuncs;

ChunkFuncs *GetChunkClass(CHUNK_TYPES_T chunkClass);

#endif //GENERIC__CHUNK_H
