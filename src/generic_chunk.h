/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef GENERIC__CHUNK_H
#define GENERIC__CHUNK_H

#include "consts.h"
#include "load_io_error_macros.h"
#include "enriched_chunk.h"

#include "LibMR/src/mr.h"
#include "RedisModulesSDK/rmutil/strings.h"

#include <stdio.h>  // printf
#include <stdlib.h> // malloc
#include <string.h> // memcpy, memmove
#include <stdint.h>

struct RedisModuleIO;

typedef struct Sample
{
    timestamp_t timestamp;
    double value;
} Sample;

typedef void Chunk_t;
typedef void ChunkIter_t;

typedef enum CHUNK_TYPES_T
{
    CHUNK_REGULAR,
    CHUNK_COMPRESSED
} CHUNK_TYPES_T;

typedef struct UpsertCtx
{
    Sample sample;
    Chunk_t *inChunk; // original chunk
} UpsertCtx;

typedef struct ChunkFuncs
{
    Chunk_t *(*NewChunk)(size_t sampleCount);
    void (*FreeChunk)(Chunk_t *chunk);
    Chunk_t *(*CloneChunk)(const Chunk_t *chunk);
    Chunk_t *(*SplitChunk)(Chunk_t *chunk);
    RedisModuleDefragDictValueCallback DefragChunk;

    size_t (*DelRange)(Chunk_t *chunk, timestamp_t startTs, timestamp_t endTs);
    ChunkResult (*AddSample)(Chunk_t *chunk, Sample *sample);
    ChunkResult (*UpsertSample)(UpsertCtx *uCtx, int *size, DuplicatePolicy duplicatePolicy);

    void (*ProcessChunk)(const Chunk_t *chunk,
                         uint64_t start,
                         uint64_t end,
                         EnrichedChunk *enrichedChunk,
                         bool reverse);

    size_t (*GetChunkSize)(const Chunk_t *chunk, bool includeStruct);
    uint64_t (*GetNumOfSample)(Chunk_t *chunk);
    uint64_t (*GetLastTimestamp)(Chunk_t *chunk);
    double (*GetLastValue)(Chunk_t *chunk);
    uint64_t (*GetFirstTimestamp)(Chunk_t *chunk);

    void (*SaveToRDB)(Chunk_t *chunk, struct RedisModuleIO *io);
    int (*LoadFromRDB)(Chunk_t **chunk, struct RedisModuleIO *io);
    void (*MRSerialize)(Chunk_t *chunk, WriteSerializationCtx *sctx);
    int (*MRDeserialize)(Chunk_t **chunk, ReaderSerializationCtx *sctx);
} ChunkFuncs;

ChunkResult handleDuplicateSample(DuplicatePolicy policy, Sample oldSample, Sample *newSample);
const char *DuplicatePolicyToString(DuplicatePolicy policy);
int RMStringLenDuplicationPolicyToEnum(RedisModuleString *aggTypeStr);
DuplicatePolicy DuplicatePolicyFromString(const char *input, size_t len);

const ChunkFuncs *GetChunkClass(CHUNK_TYPES_T chunkClass);

#endif // GENERIC__CHUNK_H
