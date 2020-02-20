#include "generic_chunk.h"
#include "chunk.h"
#include "compressed_chunk.h"
#include "rmutil/alloc.h"

void FreeChunkIterator(ChunkIter_t *iter) {
  free(iter);
}

static ChunkFuncs regChunk = {
    .NewChunk = Uncompressed_NewChunk,
    .FreeChunk = Uncompressed_FreeChunk,

    .AddSample = Uncompressed_AddSample,

    .NewChunkIterator = Uncompressed_NewChunkIterator,
    .FreeChunkIterator = FreeChunkIterator,
    .ChunkIteratorGetNext = Uncompressed_ChunkIteratorGetNext,

    .GetChunkSize = Uncompressed_GetChunkSize,
    .GetNumOfSample = Uncompressed_NumOfSample,
    .GetLastTimestamp = Uncompressed_GetLastTimestamp,
    .GetFirstTimestamp = Uncompressed_GetFirstTimestamp
};

static ChunkFuncs comprChunk = {
    .NewChunk = Compressed_NewChunk,
    .FreeChunk = Compressed_FreeChunk,

    .AddSample = Compressed_AddSample,

    .NewChunkIterator = Compressed_NewChunkIterator,
    .FreeChunkIterator = FreeChunkIterator,
    .ChunkIteratorGetNext = Compressed_ChunkIteratorGetNext,

    .GetChunkSize = Compressed_GetChunkSize,
    .GetNumOfSample = Compressed_ChunkNumOfSample,
    .GetLastTimestamp = Compressed_GetLastTimestamp,
    .GetFirstTimestamp = Compressed_GetFirstTimestamp
};

ChunkFuncs *GetChunkClass(int chunkType) {
  switch (chunkType) {
  case CHUNK_REGULAR:     return &regChunk;
  case CHUNK_COMPRESSED:  return &comprChunk;
  }
  return NULL;
} 