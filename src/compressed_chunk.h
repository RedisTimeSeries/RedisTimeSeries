/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#ifndef COMPRESSED_CHUNK_H
#define COMPRESSED_CHUNK_H

#include <sys/types.h>      // u_int_t
#include <stdbool.h>        // bool
#include "gorilla.h"
#include "generic_chunk.h"

// Initialize compressed chunk
Chunk_t *Compressed_NewChunk(size_t size);
void Compressed_FreeChunk(Chunk_t *chunk);
Chunk_t *Compressed_SplitChunk(Chunk_t *chunk);

// Append a sample to a compressed chunk
ChunkResult Compressed_AddSample(Chunk_t *chunk, Sample *sample);
ChunkResult Compressed_UpsertSample(UpsertCtx *uCtx, int *size);

// Read from compressed chunk using an iterator
ChunkIter_t *Compressed_NewChunkIterator(Chunk_t *chunk, bool rev);
ChunkResult Compressed_ChunkIteratorGetNext(ChunkIter_t *iter, Sample* sample);
void Compressed_FreeChunkIterator(ChunkIter_t *iter, bool freeChunk);

// Miscellaneous
size_t Compressed_GetChunkSize(Chunk_t *chunk);
u_int64_t Compressed_ChunkNumOfSample (Chunk_t *chunk);
timestamp_t Compressed_GetFirstTimestamp(Chunk_t *chunk);
timestamp_t Compressed_GetLastTimestamp (Chunk_t *chunk);

/* Used in tests */
u_int64_t getIterIdx(ChunkIter_t *iter);

#endif // COMPRESSED_CHUNK_H
