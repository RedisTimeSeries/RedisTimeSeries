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
Chunk_t *CChunk_NewChunk(u_int64_t size);
void CChunk_FreeChunk(Chunk_t *chunk);

// Append a sample to a compressed chunk
int CChunk_AddSample(Chunk_t *chunk, Sample *sample);

// Read from compressed chunk using an iterator
ChunkIter_t *CChunk_NewChunkIterator(Chunk_t *chunk);
void CChunk_FreeIter(ChunkIter_t *iter);
int CChunk_ChunkIteratorGetNext(ChunkIter_t *iter, Sample* sample);

// Miscellaneous
size_t CChunk_GetChunkSize(Chunk_t *chunk);
u_int64_t CChunk_ChunkNumOfSample (Chunk_t *chunk);
timestamp_t CChunk_GetFirstTimestamp(Chunk_t *chunk);
timestamp_t CChunk_GetLastTimestamp (Chunk_t *chunk);

/* Used in tests */
u_int64_t getIterIdx(Chunk_t *iter);

#endif // COMPRESSED_CHUNK_H