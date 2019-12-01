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

typedef struct Sample {
    u_int64_t timestamp;
    double value;
} Sample;

// Initialize compressed chunk
CompressedChunk *CChunk_NewChunk(u_int64_t size);
void CChunk_FreeChunk(CompressedChunk *chunk);

// Append a sample to a compressed chunk
int CChunk_AddSample(CompressedChunk *chunk, Sample sample);

// Read from compressed chunk using an iterator
CChunk_Iterator *CChunk_NewChunkIterator(CompressedChunk *chunk);
void CChunk_FreeIter(CChunk_Iterator *iter);
int CChunk_ChunkIteratorGetNext(CChunk_Iterator *iter, Sample* sample);

// Miscellaneous
u_int64_t CChunk_ChunkNumOfSample (CompressedChunk *chunk);
u_int64_t CChunk_GetFirstTimestamp(CompressedChunk *chunk);
u_int64_t CChunk_GetLastTimestamp (CompressedChunk *chunk);
size_t CChunk_GetChunkSize(CompressedChunk *chunk);

/* Used in tests */
u_int64_t getIterIdx(CChunk_Iterator *iter);

#endif // COMPRESSED_CHUNK_H