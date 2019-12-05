/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include <assert.h>         // assert
#include <limits.h>
#include <stdlib.h>         // malloc
#include <stdio.h>          // printf

#include "compressed_chunk.h"

/*********************
 *  Chunk functions  *
 *********************/
Chunk_t *CChunk_NewChunk(u_int64_t size) {
  CompressedChunk *chunk = (CompressedChunk *)calloc(1, sizeof(CompressedChunk));
  chunk->size = size * sizeof(Sample);
  chunk->data = (u_int64_t *)calloc(chunk->size, sizeof(char));
  chunk->prevLeading = 32;
  chunk->prevTrailing = 32;
  return chunk;
}

void CChunk_FreeChunk(Chunk_t *chunk) {
  CompressedChunk *cmpChunk = chunk;
  free(cmpChunk->data);
  cmpChunk->data = NULL;
  free(chunk);
}

int CChunk_AddSample(Chunk_t *chunk, Sample *sample) {
  return CChunk_Append((CompressedChunk *)chunk, sample->timestamp, sample->value);
}

u_int64_t CChunk_ChunkNumOfSample(Chunk_t *chunk) {
  return ((CompressedChunk *)chunk)->count;
}

timestamp_t CChunk_GetFirstTimestamp(Chunk_t *chunk) {
  return ((CompressedChunk *)chunk)->baseTimestamp;
}

timestamp_t CChunk_GetLastTimestamp(Chunk_t *chunk) {
  return ((CompressedChunk *)chunk)->prevTimestamp;
}

size_t CChunk_GetChunkSize(Chunk_t *chunk) {
  CompressedChunk *cmpChunk = chunk;
  return sizeof(*cmpChunk) + cmpChunk->size * sizeof(char);
}

/************************
 *  Iterator functions  *
 ************************/ 
u_int64_t getIterIdx(ChunkIter_t *iter) {
  return ((CChunk_Iterator *)iter)->idx;
}

ChunkIter_t *CChunk_NewChunkIterator(Chunk_t *chunk) {
  CompressedChunk *compChunk = chunk;
  CChunk_Iterator *iter = (CChunk_Iterator *)calloc(1, sizeof(CChunk_Iterator));

  iter->chunk = compChunk;
  iter->idx = 0;
  iter->count = 0;

  iter->prevTS = compChunk->baseTimestamp;
  iter->prevDelta = 0;

  iter->prevValue.d = compChunk->baseValue.d;  
  iter->prevLeading = 32;
  iter->prevTrailing = 32;

  return (ChunkIter_t *)iter;
}

void CChunk_FreeIter(ChunkIter_t *iter) {
  free(iter);
}

int CChunk_ChunkIteratorGetNext(ChunkIter_t *iter, Sample* sample) {
  return CChunk_ReadNext((CChunk_Iterator *)iter, &sample->timestamp, &sample->value);
}