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

typedef struct Chunk
{
    timestamp_t base_timestamp;
    Sample *samples;
    unsigned int num_samples;
    size_t size;
} Chunk;

Chunk_t *Uncompressed_NewChunk(size_t size);
void Uncompressed_FreeChunk(Chunk_t *chunk);

/**
 * TODO: describe me
 * @param chunk
 * @return
 */
Chunk_t *Uncompressed_SplitChunk(Chunk_t *chunk);
Chunk_t *Uncompressed_CloneChunk(const Chunk_t *src);
size_t Uncompressed_GetChunkSize(Chunk_t *chunk, bool includeStruct);

/**
 * TODO: describe me
 * 0 for failure, 1 for success
 * @param chunk
 * @param sample
 * @return
 */
ChunkResult Uncompressed_AddSample(Chunk_t *chunk, Sample *sample);

/**
 * TODO: describe me
 * @param uCtx
 * @param size
 * @return
 */
ChunkResult Uncompressed_UpsertSample(UpsertCtx *uCtx, int *size, DuplicatePolicy duplicatePolicy);
size_t Uncompressed_DelRange(Chunk_t *chunk, timestamp_t startTs, timestamp_t endTs);

u_int64_t Uncompressed_NumOfSample(Chunk_t *chunk);
timestamp_t Uncompressed_GetLastTimestamp(Chunk_t *chunk);
timestamp_t Uncompressed_GetFirstTimestamp(Chunk_t *chunk);

void reverseEnrichedChunk(EnrichedChunk *enrichedChunk);
void Uncompressed_ProcessChunk(const Chunk_t *chunk,
                               uint64_t start,
                               uint64_t end,
                               EnrichedChunk *enrichedChunk,
                               bool reverse);

// RDB
void Uncompressed_SaveToRDB(Chunk_t *chunk, struct RedisModuleIO *io);
int Uncompressed_LoadFromRDB(Chunk_t **chunk, struct RedisModuleIO *io);

// LibMR
void Uncompressed_MRSerialize(Chunk_t *chunk, WriteSerializationCtx *sctx);
int Uncompressed_MRDeserialize(Chunk_t **chunk, ReaderSerializationCtx *sctx);

// this is just a temporary wrapper function that ignores error in order to preserve the common api
void MR_SerializationCtxWriteLongLongWrapper(WriteSerializationCtx *sctx, long long val);

// this is just a temporary wrapper function that ignores error in order to preserve the common api
void MR_SerializationCtxWriteBufferWrapper(WriteSerializationCtx *sctx,
                                           const char *buff,
                                           size_t len);

long long MR_SerializationCtxReadeLongLongWrapper(ReaderSerializationCtx *sctx);
char *MR_ownedBufferFrom(ReaderSerializationCtx *sctx, size_t *len);

#endif
