#include "generic_chunk.h"

#include "chunk.h"
#include "compressed_chunk.h"

#include <ctype.h>
#include <math.h>
#include "rmutil/alloc.h"

static const ChunkFuncs regChunk = {
    .NewChunk = Uncompressed_NewChunk,
    .FreeChunk = Uncompressed_FreeChunk,
    .SplitChunk = Uncompressed_SplitChunk,
    .CloneChunk = Uncompressed_CloneChunk,
    .DefragChunk = Uncompressed_DefragChunk,

    .AddSample = Uncompressed_AddSample,
    .UpsertSample = Uncompressed_UpsertSample,
    .DelRange = Uncompressed_DelRange,

    .ProcessChunk = Uncompressed_ProcessChunk,

    .GetChunkSize = Uncompressed_GetChunkSize,
    .GetNumOfSample = Uncompressed_NumOfSample,
    .GetLastTimestamp = Uncompressed_GetLastTimestamp,
    .GetLastValue = Uncompressed_GetLastValue,
    .GetFirstTimestamp = Uncompressed_GetFirstTimestamp,

    .SaveToRDB = Uncompressed_SaveToRDB,
    .LoadFromRDB = Uncompressed_LoadFromRDB,
    .MRSerialize = Uncompressed_MRSerialize,
    .MRDeserialize = Uncompressed_MRDeserialize,
};

static const ChunkFuncs comprChunk = {
    .NewChunk = Compressed_NewChunk,
    .FreeChunk = Compressed_FreeChunk,
    .CloneChunk = Compressed_CloneChunk,
    .SplitChunk = Compressed_SplitChunk,
    .DefragChunk = Compressed_DefragChunk,

    .AddSample = Compressed_AddSample,
    .UpsertSample = Compressed_UpsertSample,
    .DelRange = Compressed_DelRange,

    .ProcessChunk = Compressed_ProcessChunk,

    .GetChunkSize = Compressed_GetChunkSize,
    .GetNumOfSample = Compressed_ChunkNumOfSample,
    .GetLastTimestamp = Compressed_GetLastTimestamp,
    .GetLastValue = Compressed_GetLastValue,
    .GetFirstTimestamp = Compressed_GetFirstTimestamp,

    .SaveToRDB = Compressed_SaveToRDB,
    .LoadFromRDB = Compressed_LoadFromRDB,
    .MRSerialize = Compressed_MRSerialize,
    .MRDeserialize = Compressed_MRDeserialize,
};

// This function will decide according to the policy how to handle duplicate sample, the `newSample`
// will contain the data that will be kept in the database.
ChunkResult handleDuplicateSample(DuplicatePolicy policy, Sample oldSample, Sample *newSample) {
    bool has_NAN = isnan(oldSample.value) || isnan(newSample->value);
    if (has_NAN && policy != DP_BLOCK) {
        // take the valid sample regardless of policy
        if (isnan(newSample->value)) {
            newSample->value = oldSample.value;
        }
        return CR_OK;
    }

    switch (policy) {
        case DP_BLOCK:
            return CR_ERR;
        case DP_FIRST:
            *newSample = oldSample;
            return CR_OK;
        case DP_LAST:
            return CR_OK;
        case DP_MIN:
            if (oldSample.value < newSample->value)
                newSample->value = oldSample.value;
            return CR_OK;
        case DP_MAX:
            if (oldSample.value > newSample->value)
                newSample->value = oldSample.value;
            return CR_OK;
        case DP_SUM:
            newSample->value += oldSample.value;
            return CR_OK;
        default:
            return CR_ERR;
    }
}

const ChunkFuncs *GetChunkClass(CHUNK_TYPES_T chunkType) {
    switch (chunkType) {
        case CHUNK_REGULAR:
            return &regChunk;
        case CHUNK_COMPRESSED:
            return &comprChunk;
    }
    return NULL;
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
    if (len == 3) {
        if (strncmp(input_lower, "min", len) == 0) {
            return DP_MIN;
        } else if (strncmp(input_lower, "max", len) == 0) {
            return DP_MAX;
        } else if (strncmp(input_lower, "sum", len) == 0) {
            return DP_SUM;
        }
    } else if (len == 4) {
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

// this is just a temporary wrapper function that ignores error in order to preserve the common api
long long MR_SerializationCtxReadLongLongWrapper(ReaderSerializationCtx *sctx) {
    MRError *err;
    return MR_SerializationCtxReadLongLong(sctx, &err);
}

char *MR_ownedBufferFrom(ReaderSerializationCtx *sctx, size_t *len) {
    MRError *err;
    size_t size = 0;
    const char *temp = MR_SerializationCtxReadBuffer(sctx, &size, &err);
    char *ret = malloc(size);
    memcpy(ret, temp, size);
    if (len != NULL) {
        *len = size;
    }
    return ret;
}

// this is just a temporary wrapper function that ignores error in order to preserve the common api
void MR_SerializationCtxWriteLongLongWrapper(WriteSerializationCtx *sctx, long long val) {
    MRError *err;
    MR_SerializationCtxWriteLongLong(sctx, val, &err);
}

// this is just a temporary wrapper function that ignores error in order to preserve the common api
void MR_SerializationCtxWriteBufferWrapper(WriteSerializationCtx *sctx,
                                           const char *buff,
                                           size_t len) {
    MRError *err;
    MR_SerializationCtxWriteBuffer(sctx, buff, len, &err);
}
