/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef COMPRESSED_CHUNK_H
#define COMPRESSED_CHUNK_H

#include "generic_chunk.h"
#include "gorilla.h"

#include <stdbool.h> // bool
#include <stdint.h>

// Initialize compressed chunk
Chunk_t *Compressed_NewChunk(size_t size);
void Compressed_FreeChunk(Chunk_t *chunk);
Chunk_t *Compressed_CloneChunk(const Chunk_t *chunk);
Chunk_t *Compressed_SplitChunk(Chunk_t *chunk);
int Compressed_DefragChunk(RedisModuleDefragCtx *ctx,
                           void *data,
                           unsigned char *key,
                           size_t keylen,
                           void **newptr);

// Append a sample to a compressed chunk
ChunkResult Compressed_AddSample(Chunk_t *chunk, Sample *sample);
ChunkResult Compressed_UpsertSample(UpsertCtx *uCtx, int *size, DuplicatePolicy duplicatePolicy);
size_t Compressed_DelRange(Chunk_t *chunk, timestamp_t startTs, timestamp_t endTs);

void Compressed_ProcessChunk(const Chunk_t *chunk,
                             uint64_t start,
                             uint64_t end,
                             EnrichedChunk *enrichedChunk,
                             bool reverse);

// Read from compressed chunk using an iterator
ChunkIter_t *Compressed_NewChunkIterator(const Chunk_t *chunk);
void Compressed_ResetChunkIterator(ChunkIter_t *iterator, const Chunk_t *chunk);
void Compressed_FreeChunkIterator(ChunkIter_t *iter);

// Miscellaneous
size_t Compressed_GetChunkSize(const Chunk_t *chunk, bool includeStruct);
uint64_t Compressed_ChunkNumOfSample(Chunk_t *chunk);
timestamp_t Compressed_GetFirstTimestamp(Chunk_t *chunk);
timestamp_t Compressed_GetLastTimestamp(Chunk_t *chunk);
double Compressed_GetLastValue(Chunk_t *chunk);

// RDB
void Compressed_SaveToRDB(Chunk_t *chunk, struct RedisModuleIO *io);
int Compressed_LoadFromRDB(Chunk_t **chunk, struct RedisModuleIO *io);

// LibMR
void Compressed_MRSerialize(Chunk_t *chunk, WriteSerializationCtx *sctx);
int Compressed_MRDeserialize(Chunk_t **chunk, ReaderSerializationCtx *sctx);

/* Used in tests */
uint64_t getIterIdx(ChunkIter_t *iter);

#endif // COMPRESSED_CHUNK_H
