/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include <assert.h>         // assert
#include <limits.h>
#include <stdlib.h>         // malloc
#include <stdio.h>          // printf

#include "rmutil/alloc.h"
#include "compressed_chunk.h"

/*********************
 *  Chunk functions  *
 *********************/
Chunk_t *Compressed_NewChunk(u_int64_t size) {
  CompressedChunk *chunk = (CompressedChunk *)calloc(1, sizeof(CompressedChunk));
  chunk->size = size * sizeof(Sample);
  chunk->data = (u_int64_t *)calloc(chunk->size, sizeof(char));
  chunk->prevLeading = 32;
  chunk->prevTrailing = 32;
  return chunk;
}

void Compressed_FreeChunk(Chunk_t *chunk) {
  CompressedChunk *cmpChunk = chunk;
  free(cmpChunk->data);
  cmpChunk->data = NULL;
  free(chunk);
}

ChunkResult Compressed_AddSample(Chunk_t *chunk, Sample *sample) {
  return Compressed_Append((CompressedChunk *)chunk, sample->timestamp, sample->value);
}

u_int64_t Compressed_ChunkNumOfSample(Chunk_t *chunk) {
  return ((CompressedChunk *)chunk)->count;
}

timestamp_t Compressed_GetFirstTimestamp(Chunk_t *chunk) {
  return ((CompressedChunk *)chunk)->baseTimestamp;
}

timestamp_t Compressed_GetLastTimestamp(Chunk_t *chunk) {
  return ((CompressedChunk *)chunk)->prevTimestamp;
}

size_t Compressed_GetChunkSize(Chunk_t *chunk) {
  CompressedChunk *cmpChunk = chunk;
  return sizeof(*cmpChunk) + cmpChunk->size * sizeof(char);
}

/************************
 *  Iterator functions  *
 ************************/ 
// LCOV_EXCL_START - used for debug
u_int64_t getIterIdx(ChunkIter_t *iter) {
  return ((Compressed_Iterator *)iter)->idx;
}
// LCOV_EXCL_STOP

ChunkIter_t *Compressed_NewChunkIterator(Chunk_t *chunk, Sample *sample) {
  CompressedChunk *compChunk = chunk;
  Compressed_Iterator *iter = (Compressed_Iterator *)calloc(1, sizeof(Compressed_Iterator));

  iter->chunk = compChunk;
  iter->idx = 0;
  iter->count = 1;    // first sample is returned

  iter->prevTS = compChunk->baseTimestamp;
  iter->prevDelta = 0;

  iter->prevValue.d = compChunk->baseValue.d;  
  iter->prevLeading = 32;
  iter->prevTrailing = 32;

  sample->timestamp = compChunk->baseTimestamp;
  sample->value     = compChunk->baseValue.d;

  return (ChunkIter_t *)iter;
}

ChunkResult Compressed_ChunkIteratorGetNext(ChunkIter_t *iter, Sample* sample) {
  return Compressed_ReadNext((Compressed_Iterator *)iter, &sample->timestamp, &sample->value);
}