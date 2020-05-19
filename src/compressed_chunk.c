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
#include "chunk.h"

#define EXTRA_SPACE 1024

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

static void swapChunks(CompressedChunk *a, CompressedChunk *b) {
  CompressedChunk tmp = *a;
  *a = *b;
  *b = tmp;
}

ChunkResult Compressed_UpsertSample(AddCtx *aCtx) {
  ChunkResult res;
  ChunkResult rv = CR_OK;
  CompressedChunk *oldChunk = (CompressedChunk *)aCtx->chunk;

  short newSize = oldChunk->size / sizeof(Sample);
  // extend size if approaching end
  if (((oldChunk->idx) / 8) >= oldChunk->size - EXTRA_SPACE) {
    newSize += EXTRA_SPACE / sizeof(Sample); // excessive
  }; // TODO: ensure

  CompressedChunk *newChunk = Compressed_NewChunk(newSize);
  Compressed_Iterator *iter = Compressed_NewChunkIterator(oldChunk, false);
  timestamp_t ts = aCtx->sample.timestamp;
  int numSamples = oldChunk->count;

  // assert(ts >= Compressed_GetFirstTimestamp(oldChunk));
  // find sample location
  size_t i = 0;
  Sample iterSample;
  for (; i < numSamples; ++i) {
    res = Compressed_ChunkIteratorGetNext(iter, &iterSample);
    assert(res == CR_OK);
    if (ts <= iterSample.timestamp) {
        break;
    }
    res = Compressed_AddSample(newChunk, &iterSample);
    assert(res == CR_OK);
  }

  // TODO: TS.UPSERT vs TS.ADD
  if (ts == iterSample.timestamp) {
    if (aCtx->type == UPSERT_NOT_ADD) {
      rv = CR_OCCUPIED;
      goto clean;
    } else /*if (type == UPSERT_ADD || type == UPSERT_DEL)*/ {
      // skip previous sample
      res = Compressed_ChunkIteratorGetNext(iter, &iterSample);
      aCtx->sz = -1;
    }    
  } else if (aCtx->type == UPSERT_DEL) {
    rv = CR_ERR;
    goto clean;
  }

  if (aCtx->type != UPSERT_DEL) {
    ChunkResult resSample = Compressed_AddSample(newChunk, &aCtx->sample);
    assert(resSample == CR_OK);
    aCtx->sz = 1;
  }
  // TODO: split chunk (or provide additional API)
  
  if (i != numSamples) { // sample is not last
    while (res == CR_OK) {
      res = Compressed_AddSample(newChunk, &iterSample);
      assert(res == CR_OK);
      res = Compressed_ChunkIteratorGetNext(iter, &iterSample);
   }
  }
  //assert(type != UPSERT_ADD || oldChunk->count + 1 == newChunk->count);
  //assert(type != UPSERT_DEL || oldChunk->count - 1 == newChunk->count);
  // trim data
  int excess = newChunk->size - (newChunk->idx + 8) / 8;
  assert(excess >= 0);
  if (excess > 0) { // && (newChunk->size - excess) > oldChunk->size
    newSize = (newChunk->size - excess) > oldChunk->size ? newChunk->size - excess : oldChunk->size;
    newChunk->data = realloc(newChunk->data, newSize);
    newChunk->size = newSize;
  }
  swapChunks (newChunk, oldChunk);
clean:
  Compressed_FreeChunkIterator(iter, false);
  Compressed_FreeChunk(newChunk);
  return rv;
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

static Chunk *decompressChunk(CompressedChunk *compressedChunk) {
  Sample sample;
  uint64_t numSamples = compressedChunk->count;
  Chunk *uncompressedChunk = Uncompressed_NewChunk(numSamples);

  ChunkIter_t *iter = Compressed_NewChunkIterator(compressedChunk, 0);
  for(uint64_t i = 0; i < numSamples; ++i) {
    Compressed_ChunkIteratorGetNext(iter, &sample);
    Uncompressed_AddSample(uncompressedChunk, &sample);
  }
  Compressed_FreeChunkIterator(iter, false);
  return uncompressedChunk;
}

/************************
 *  Iterator functions  *
 ************************/ 
// LCOV_EXCL_START - used for debug
u_int64_t getIterIdx(ChunkIter_t *iter) {
  return ((Compressed_Iterator *)iter)->idx;
}
// LCOV_EXCL_STOP

ChunkIter_t *Compressed_NewChunkIterator(Chunk_t *chunk, bool rev) {
  CompressedChunk *compressedChunk = chunk;

  // for reverse iterator of compressed chunks 
  if (rev == true) {
    Chunk *uncompressedChunk = decompressChunk(compressedChunk);
    return Uncompressed_NewChunkIterator(uncompressedChunk, true);
  }

  Compressed_Iterator *iter = (Compressed_Iterator *)calloc(1, sizeof(Compressed_Iterator));

  iter->chunk = compressedChunk;
  iter->idx = 0;
  iter->count = 0;

  iter->prevTS = compressedChunk->baseTimestamp;
  iter->prevDelta = 0;

  iter->prevValue.d = compressedChunk->baseValue.d;  
  iter->prevLeading = 32;
  iter->prevTrailing = 32;

  return (ChunkIter_t *)iter;
}

ChunkResult Compressed_ChunkIteratorGetNext(ChunkIter_t *iter, Sample* sample) {
  return Compressed_ReadNext((Compressed_Iterator *)iter, &sample->timestamp, &sample->value);
}

void Compressed_FreeChunkIterator(ChunkIter_t *iter, bool rev) {
  // compressed iterator on reverse query has to release decompressed chunk
  if (rev) {
    free(((ChunkIterator *)iter)->chunk);
  }
  free(iter);
}