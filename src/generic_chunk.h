/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#ifndef GENERIC__CHUNK_H
#define GENERIC__CHUNK_H

#include "LibMR/src/mr.h"
#include "consts.h"
#include "load_io_error_macros.h"
#include "domain_chunk.h"

#include <stdio.h>  // printf
#include <stdlib.h> // malloc
#include <string.h> // memcpy, memmove
#include <sys/types.h>
#include <rmutil/strings.h>

struct RedisModuleIO;

typedef struct Sample
{
    timestamp_t timestamp;
    double value;
} Sample;

typedef struct Chunk
{
    timestamp_t base_timestamp;
    Sample *samples;
    unsigned int num_samples;
    size_t size;
} Chunk;

typedef void Chunk_t;
typedef void ChunkIter_t;

#define CHUNK_ITER_OP_NONE 0
#define CHUNK_ITER_OP_REVERSE 1

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

typedef struct ChunkIterFuncs
{
    void (*Free)(ChunkIter_t *iter);
    ChunkResult (*GetNext)(ChunkIter_t *iter, Sample *sample);
    ChunkResult (*GetPrev)(ChunkIter_t *iter, Sample *sample);
    void (*Reset)(ChunkIter_t *iter, const Chunk_t *chunk);
} ChunkIterFuncs;

typedef struct ChunkFuncs
{
    Chunk_t *(*NewChunk)(size_t sampleCount);
    void (*FreeChunk)(Chunk_t *chunk);
    Chunk_t *(*CloneChunk)(const Chunk_t *chunk);
    Chunk_t *(*SplitChunk)(Chunk_t *chunk);

    size_t (*DelRange)(Chunk_t *chunk, timestamp_t startTs, timestamp_t endTs);
    ChunkResult (*AddSample)(Chunk_t *chunk, Sample *sample);
    ChunkResult (*UpsertSample)(UpsertCtx *uCtx, int *size, DuplicatePolicy duplicatePolicy);

    DomainChunk *(*ProcessChunk)(const Chunk_t *chunk,
                                 uint64_t start,
                                 uint64_t end,
                                 DomainChunk *domainChunk,
                                 DomainChunk *domainChunkAux,
                                 bool reverse);

    size_t (*GetChunkSize)(Chunk_t *chunk, bool includeStruct);
    u_int64_t (*GetNumOfSample)(Chunk_t *chunk);
    u_int64_t (*GetLastTimestamp)(Chunk_t *chunk);
    u_int64_t (*GetFirstTimestamp)(Chunk_t *chunk);

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
ChunkIterFuncs *GetChunkIteratorClass(CHUNK_TYPES_T chunkType);

#endif // GENERIC__CHUNK_H
