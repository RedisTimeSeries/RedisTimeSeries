#include "generic_chunk.h"

#include "chunk.h"
#include "compressed_chunk.h"

#include <ctype.h>
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

ChunkResult handleDuplicateSample(DuplicatePolicy policy, Sample oldSample, Sample *newSample) {
    switch (policy) {
        case DP_BLOCK:
            return CR_ERR;
            break;
        case DP_FIRST:
            *newSample = oldSample;
            return CR_OK;
            break;
        case DP_LAST:
            return CR_OK;
        default:
            return CR_ERR;
    }
}

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

const char *DuplicatePolicyToString(DuplicatePolicy policy) {
    switch (policy) {
        case DP_NONE:
            return "none";
        case DP_BLOCK:
            return "block";
        case DP_LAST:
            return "last";
        case DP_FIRST:
            return "first";
        default:
            return "invalid";
    }
}

int RMStringLenDuplicationPolicyToEnum(RedisModuleString *aggTypeStr) {
    size_t str_len;
    const char *aggTypeCStr = RedisModule_StringPtrLen(aggTypeStr, &str_len);
    return DuplicatePolicyFromString(aggTypeCStr, str_len);
}

DuplicatePolicy DuplicatePolicyFromString(const char *input, size_t len) {
    char input_lower[len];
    for (int i = 0; i < len; i++) {
        input_lower[i] = tolower(input[i]);
    }
    if (len == 4) {
        if (strncmp(input_lower, "last", len) == 0) {
            return DP_LAST;
        }
    } else if (len == 5) {
        if (strncmp(input_lower, "block", len) == 0) {
            return DP_BLOCK;
        } else if (strncmp(input_lower, "first", len) == 0) {
            return DP_FIRST;
        }
    }
    return DP_INVALID;
}
