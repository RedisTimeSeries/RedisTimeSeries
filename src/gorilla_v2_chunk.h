/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#ifndef TURBOGORILLA_CHUNK_H
#define TURBOGORILLA_CHUNK_H

#include "consts.h"
#include "generic_chunk.h"

#include <sys/types.h>

#define TURBOGORILLA_UNCOMPRESSED_SIZE sizeof(uint64_t) + sizeof(double)

typedef struct Gorilla_v2_Chunk Gorilla_v2_Chunk;

typedef struct Gorilla_v2_ChunkIterator Gorilla_v2_ChunkIterator;

Chunk_t *Gorilla_v2_NewChunk(size_t sampleCount);
void Gorilla_v2_FreeChunk(Chunk_t *chunk);
Chunk_t *Gorilla_v2_CloneChunk(Chunk_t *chunk);

/**
 * TODO: describe me
 * @param chunk
 * @return
 */
Chunk_t *Gorilla_v2_SplitChunk(Chunk_t *chunk);
size_t Gorilla_v2_GetChunkSize(Chunk_t *chunk, bool includeStruct);

ChunkResult Gorilla_v2_AddSampleOptimized(Chunk_t *chunk, u_int64_t timestamp, double value);

/**
 * TODO: describe me
 * 0 for failure, 1 for success
 * @param chunk
 * @param sample
 * @return
 */
ChunkResult Gorilla_v2_AddSample(Chunk_t *chunk, Sample *sample);

/**
 * TODO: describe me
 * @param uCtx
 * @param size
 * @return
 */
ChunkResult Gorilla_v2_UpsertSample(UpsertCtx *uCtx, int *size, DuplicatePolicy duplicatePolicy);
size_t Gorilla_v2_DelRange(Chunk_t *chunk, timestamp_t startTs, timestamp_t endTs);

u_int64_t Gorilla_v2_NumOfSample(Chunk_t *chunk);
timestamp_t Gorilla_v2_GetLastTimestamp(Chunk_t *chunk);
timestamp_t Gorilla_v2_GetFirstTimestamp(Chunk_t *chunk);

ChunkIter_t *Gorilla_v2_NewChunkIterator(Chunk_t *chunk,
                                         int options,
                                         ChunkIterFuncs *retChunkIterClass);
void Gorilla_v2_ResetChunkIterator(ChunkIter_t *iterator, Chunk_t *chunk);
ChunkResult Gorilla_v2_ChunkIteratorGetNext(ChunkIter_t *iterator, Sample *sample);
ChunkResult Gorilla_v2_ChunkIteratorGetPrev(ChunkIter_t *iterator, Sample *sample);
void Gorilla_v2_FreeChunkIterator(ChunkIter_t *iter);

// RDB
void Gorilla_v2_SaveToRDB(Chunk_t *chunk, struct RedisModuleIO *io);
void Gorilla_v2_LoadFromRDB(Chunk_t **chunk, struct RedisModuleIO *io);

// Gears
void Gorilla_v2_GearsSerialize(Chunk_t *chunk, Gears_BufferWriter *bw);
void Gorilla_v2_GearsDeserialize(Chunk_t *chunk, Gears_BufferReader *br);

//----------- delta of delta encoding ------------------
size_t _ddelta_enc64(uint64_t *in, size_t n, unsigned char *out);
size_t _ddelta_dec64(unsigned char *in, size_t n, uint64_t *out);
#endif
