#include "generic_chunk.h"
#include "chunk.h"
#include "compressed_chunk.h"
#include "rmutil/alloc.h"

void FreeChunkIterator(ChunkIter_t *iter) {
  free(iter);
}

static ChunkFuncs regChunk = {
    .NewChunk = U_NewChunk,
    .FreeChunk = U_FreeChunk,

    .AddSample = U_AddSample,

    .NewChunkIterator = U_NewChunkIterator,
    .FreeChunkIterator = FreeChunkIterator,
    .ChunkIteratorGetNext = U_ChunkIteratorGetNext,

    .GetChunkSize = U_GetChunkSize,
    .GetNumOfSample = U_NumOfSample,
    .GetLastTimestamp = U_GetLastTimestamp,
    .GetFirstTimestamp = U_GetFirstTimestamp
};

static ChunkFuncs comprChunk = {
    .NewChunk = CChunk_NewChunk,
    .FreeChunk = CChunk_FreeChunk,

    .AddSample = CChunk_AddSample,

    .NewChunkIterator = CChunk_NewChunkIterator,
    .FreeChunkIterator = FreeChunkIterator,
    .ChunkIteratorGetNext = CChunk_ChunkIteratorGetNext,

    .GetChunkSize = CChunk_GetChunkSize,
    .GetNumOfSample = CChunk_ChunkNumOfSample,
    .GetLastTimestamp = CChunk_GetLastTimestamp,
    .GetFirstTimestamp = CChunk_GetFirstTimestamp
};

ChunkFuncs *GetChunkClass(int chunkType) {
  switch (chunkType) {
  case CHUNK_REGULAR:     return &regChunk;
  case CHUNK_COMPRESSED:  return &comprChunk;
  default:                return NULL;
  }
} 