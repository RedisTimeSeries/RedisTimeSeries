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
CompressedChunk *CChunk_NewChunk(u_int64_t size) {
  CompressedChunk *chunk = (CompressedChunk *)calloc(1, sizeof(CompressedChunk));
  chunk->size = size;
  chunk->data = (u_int64_t *)calloc(size, sizeof(char));
  chunk->prevLeading = 32;
  chunk->prevTrailing = 32;
  return chunk;
}

void CChunk_FreeChunk(CompressedChunk *chunk) {
  free(chunk->data);
  chunk->data = NULL;
  free(chunk);
}

int CChunk_AddSample(CompressedChunk *chunk, Sample sample) {
  return CChunk_Append(chunk, sample.timestamp, sample.value);
}

u_int64_t CChunk_ChunkNumOfSample(CompressedChunk *chunk) {
  return chunk->count;
}

u_int64_t CChunk_GetFirstTimestamp(CompressedChunk *chunk) {
  return chunk->baseTimestamp;
}

u_int64_t CChunk_GetLastTimestamp (CompressedChunk *chunk) {
  return chunk->prevTimestamp;
}

size_t CChunk_GetChunkSize(CompressedChunk *chunk) {
  return sizeof(*chunk) + chunk->size * sizeof(char);
}

/************************
 *  Iterator functions  *
 ************************/ 
u_int64_t getIterIdx(CChunk_Iterator *iter) {
  return iter->idx;
}

CChunk_Iterator *CChunk_NewChunkIterator(CompressedChunk *chunk) {
  CChunk_Iterator *iter = (CChunk_Iterator *)calloc(1, sizeof(CChunk_Iterator));

  iter->chunk = chunk;
  iter->idx = 0;
  iter->count = 0;

  iter->prevTS = chunk->baseTimestamp;
  iter->prevDelta = 0;

  iter->prevValue.d = chunk->baseValue.d;  
  iter->prevLeading = 32;
  iter->prevTrailing = 32;

  return iter;
}

void CChunk_FreeIter(CChunk_Iterator *iter) {
  free(iter);
}

int CChunk_ChunkIteratorGetNext(CChunk_Iterator *iter, Sample* sample) {
  return CChunk_ReadNext(iter, &sample->timestamp, &sample->value);
}