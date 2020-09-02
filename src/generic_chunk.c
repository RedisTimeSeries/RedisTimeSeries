#include "generic_chunk.h"

#include "chunk.h"
#include "compressed_chunk.h"

#include "rmutil/alloc.h"

static ChunkFuncs regChunk = {
    .NewChunk = Uncompressed_NewChunk,
    .FreeChunk = Uncompressed_FreeChunk,
    .SplitChunk = Uncompressed_SplitChunk,

    .AddSample = Uncompressed_AddSample,
    .UpsertSample = Uncompressed_UpsertSample,

    .NewChunkIterator = Uncompressed_NewChunkIterator,

    .GetChunkSize = Uncompressed_GetChunkSize,
    .GetNumOfSample = Uncompressed_NumOfSample,
    .GetLastTimestamp = Uncompressed_GetLastTimestamp,
    .GetFirstTimestamp = Uncompressed_GetFirstTimestamp,

    .SaveToRDB = Uncompressed_SaveToRDB,
    .LoadFromRDB = Uncompressed_LoadFromRDB,
};

ChunkIterFuncs uncompressedChunkIteratorClass = {
    .Free = Uncompressed_FreeChunkIterator,
    .GetNext = Uncompressed_ChunkIteratorGetNext,
    .GetPrev = Uncompressed_ChunkIteratorGetPrev,
};

static ChunkFuncs comprChunk = {
    .NewChunk = Compressed_NewChunk,
    .FreeChunk = Compressed_FreeChunk,
    .SplitChunk = Compressed_SplitChunk,

    .AddSample = Compressed_AddSample,
    .UpsertSample = Compressed_UpsertSample,

    .NewChunkIterator = Compressed_NewChunkIterator,

    .GetChunkSize = Compressed_GetChunkSize,
    .GetNumOfSample = Compressed_ChunkNumOfSample,
    .GetLastTimestamp = Compressed_GetLastTimestamp,
    .GetFirstTimestamp = Compressed_GetFirstTimestamp,

    .SaveToRDB = Compressed_SaveToRDB,
    .LoadFromRDB = Compressed_LoadFromRDB,
};

static ChunkIterFuncs compressedChunkIteratorClass = {
    .Free = Compressed_FreeChunkIterator,
    .GetNext = Compressed_ChunkIteratorGetNext,
    /*** Reverse iteration is on temporary decompressed chunk ***/
    .GetPrev = NULL,
};

ChunkFuncs *GetChunkClass(CHUNK_TYPES_T chunkType) {
    switch (chunkType) {
        case CHUNK_REGULAR:
            return &regChunk;
        case CHUNK_COMPRESSED:
            return &comprChunk;
    }
    return NULL;
}

ChunkIterFuncs *GetChunkIteratorClass(CHUNK_TYPES_T chunkType) {
    switch (chunkType) {
        case CHUNK_REGULAR:
            return &uncompressedChunkIteratorClass;
        case CHUNK_COMPRESSED:
            return &compressedChunkIteratorClass;
    }
    return NULL;
}
