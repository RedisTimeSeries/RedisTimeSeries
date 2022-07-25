/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#ifndef HEADER_CHUNK_H
#define HEADER_CHUNK_H

#include "generic_chunk.h"

#include <stdbool.h>   // bool
#include <sys/types.h> // u_int_t

// Initialize compressed chunk
Chunk_t *Header_NewChunk(size_t size);
void Header_FreeChunk(Chunk_t *chunk);
Chunk_t *Header_CloneChunk(const Chunk_t *chunk);
Chunk_t *Header_SplitChunk(Chunk_t *chunk);

// Append a sample to a compressed chunk
ChunkResult Header_AddSample(Chunk_t *chunk, Sample *sample);
ChunkResult Header_UpsertSample(UpsertCtx *uCtx, int *size, DuplicatePolicy duplicatePolicy);
size_t Header_DelRange(Chunk_t *chunk, timestamp_t startTs, timestamp_t endTs);

void Header_ProcessChunk(const Chunk_t *chunk,
                             uint64_t start,
                             uint64_t end,
                             EnrichedChunk *enrichedChunk,
                             bool reverse);

// Read from compressed chunk using an iterator
ChunkIter_t *Header_NewChunkIterator(const Chunk_t *chunk);
void Header_ResetChunkIterator(ChunkIter_t *iterator, const Chunk_t *chunk);
void Header_FreeChunkIterator(ChunkIter_t *iter);

// Miscellaneous
size_t Header_GetChunkSize(Chunk_t *chunk, bool includeStruct);
u_int64_t Header_ChunkNumOfSample(Chunk_t *chunk);
timestamp_t Header_GetFirstTimestamp(Chunk_t *chunk);
timestamp_t Header_GetLastTimestamp(Chunk_t *chunk);
double Header_GetLastValue(Chunk_t *chunk);

// RDB
void Header_SaveToRDB(Chunk_t *chunk, struct RedisModuleIO *io);
int Header_LoadFromRDB(Chunk_t **chunk, struct RedisModuleIO *io);

// LibMR
void Header_MRSerialize(Chunk_t *chunk, WriteSerializationCtx *sctx);
int Header_MRDeserialize(Chunk_t **chunk, ReaderSerializationCtx *sctx);

#endif // HEADER_CHUNK_H
