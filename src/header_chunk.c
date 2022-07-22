/*
 * Copyright 2018-2020 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "header_chunk.h"

#include "LibMR/src/mr.h"
#include "chunk.h"
#include "generic_chunk.h"
/*
 * Copyright 2018-2020 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "compressed_chunk.h"

#include "LibMR/src/mr.h"
#include "chunk.h"
#include "generic_chunk.h"

#include <assert.h> // assert
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>  // printf
#include <stdlib.h> // malloc
#include "rmutil/alloc.h"

typedef struct HeaderChunk
{
    u_int64_t size;
    u_int64_t count;
    u_int64_t idx;
    u_int64_t *data;

    timestamp_t baseTimestamp;
    int64_t baseValue;

    timestamp_t prevTimestamp;
    int64_t prevTimestampDelta;

    int64_t prevValue;
} HeaderChunk;

typedef struct Header_Iterator
{
    HeaderChunk *chunk;
    u_int64_t idx;
    u_int64_t count;

    // timestamp vars
    timestamp_t prevTS;
    int64_t prevDeltaTs;

    // value vars
    int64_t prevValue;
    int64_t prevValueDelta;
} Header_Iterator;

#define BIT 8
#define CHUNK_RESIZE_STEP 32

/*********************
 *  Chunk functions  *
 *********************/
Chunk_t *Header_NewChunk(size_t size) {
    HeaderChunk *chunk = (HeaderChunk *)calloc(1, sizeof(HeaderChunk));
    chunk->size = size;
    chunk->data = (u_int64_t *)calloc(chunk->size, sizeof(char));
#ifdef DEBUG
    memset(chunk->data, 0, chunk->size);
#endif
    return chunk;
}

void Header_FreeChunk(Chunk_t *chunk) {
    HeaderChunk *headerChunk = chunk;
    if (headerChunk->data) {
        free(headerChunk->data);
    }
    headerChunk->data = NULL;
    free(chunk);
}

Chunk_t *Header_CloneChunk(const Chunk_t *chunk) {
    const HeaderChunk *oldChunk = chunk;
    HeaderChunk *newChunk = malloc(sizeof(HeaderChunk));
    memcpy(newChunk, oldChunk, sizeof(HeaderChunk));
    newChunk->data = malloc(newChunk->size);
    memcpy(newChunk->data, oldChunk->data, oldChunk->size);
    return newChunk;
}

static void swapChunks(HeaderChunk *a, HeaderChunk *b) {
    HeaderChunk tmp = *a;
    *a = *b;
    *b = tmp;
}

static void ensureAddSample(HeaderChunk *chunk, Sample *sample) {
    ChunkResult res = Header_AddSample(chunk, sample);
    if (res != CR_OK) {
        int oldsize = chunk->size;
        chunk->size += CHUNK_RESIZE_STEP;
        chunk->data = (u_int64_t *)realloc(chunk->data, chunk->size * sizeof(char));
        memset((char *)chunk->data + oldsize, 0, CHUNK_RESIZE_STEP);
        // printf("Chunk extended to %lu \n", chunk->size);
        res = Header_AddSample(chunk, sample);
        assert(res == CR_OK);
    }
}

static void trimChunk(HeaderChunk *chunk) {
    int excess = (chunk->size * BIT - chunk->idx) / BIT;

    assert(excess >= 0); // else we have written beyond allocated memory

    if (excess > 1) {
        size_t newSize = chunk->size - excess + 1;
        // align to 8 bytes (u_int64_t) otherwise we will have an heap overflow in gorilla.c because
        // each write happens in 8 bytes blocks.
        newSize += sizeof(binary_t) - (newSize % sizeof(binary_t));
        chunk->data = realloc(chunk->data, newSize);
        chunk->size = newSize;
    }
}

Chunk_t *Header_SplitChunk(Chunk_t *chunk) {
    HeaderChunk *curChunk = chunk;
    size_t split = curChunk->count / 2;
    size_t curNumSamples = curChunk->count - split;

    // add samples in new chunks
    size_t i = 0;
    Sample sample;
    ChunkIter_t *iter = Header_NewChunkIterator(curChunk);
    HeaderChunk *newChunk1 = Header_NewChunk(curChunk->size);
    HeaderChunk *newChunk2 = Header_NewChunk(curChunk->size);
    for (; i < curNumSamples; ++i) {
        Header_ChunkIteratorGetNext(iter, &sample);
        ensureAddSample(newChunk1, &sample);
    }
    for (; i < curChunk->count; ++i) {
        Header_ChunkIteratorGetNext(iter, &sample);
        ensureAddSample(newChunk2, &sample);
    }

    trimChunk(newChunk1);
    trimChunk(newChunk2);
    swapChunks(curChunk, newChunk1);

    Header_FreeChunkIterator(iter);
    Header_FreeChunk(newChunk1);

    return newChunk2;
}

ChunkResult Header_UpsertSample(UpsertCtx *uCtx, int *size, DuplicatePolicy duplicatePolicy) {
    *size = 0;
    ChunkResult rv = CR_OK;
    ChunkResult nextRes = CR_OK;
    HeaderChunk *oldChunk = (HeaderChunk *)uCtx->inChunk;

    size_t newSize = oldChunk->size;

    HeaderChunk *newChunk = Header_NewChunk(newSize);
    Header_Iterator *iter = Header_NewChunkIterator(oldChunk);
    timestamp_t ts = uCtx->sample.timestamp;
    int numSamples = oldChunk->count;

    size_t i = 0;
    Sample iterSample;
    for (; i < numSamples; ++i) {
        nextRes = Header_ChunkIteratorGetNext(iter, &iterSample);
        if (iterSample.timestamp >= ts) {
            break;
        }
        ensureAddSample(newChunk, &iterSample);
    }

    if (ts == iterSample.timestamp) {
        ChunkResult cr = handleDuplicateSample(duplicatePolicy, iterSample, &uCtx->sample);
        if (cr != CR_OK) {
            Header_FreeChunkIterator(iter);
            Header_FreeChunk(newChunk);
            return CR_ERR;
        }
        nextRes = Header_ChunkIteratorGetNext(iter, &iterSample);
        *size = -1; // we skipped a sample
    }
    // upsert the sample
    ensureAddSample(newChunk, &uCtx->sample);
    *size += 1;

    if (i < numSamples) {
        while (nextRes == CR_OK) {
            ensureAddSample(newChunk, &iterSample);
            nextRes = Header_ChunkIteratorGetNext(iter, &iterSample);
        }
    }

    swapChunks(newChunk, oldChunk);

    Header_FreeChunkIterator(iter);
    Header_FreeChunk(newChunk);
    return rv;
}

ChunkResult Header_AddSample(Chunk_t *chunk, Sample *sample) {
    return Header_Append((HeaderChunk *)chunk, sample->timestamp, sample->value);
}

u_int64_t Header_ChunkNumOfSample(Chunk_t *chunk) {
    return ((HeaderChunk *)chunk)->count;
}

timestamp_t Header_GetFirstTimestamp(Chunk_t *chunk) {
    if (((HeaderChunk *)chunk)->count ==
        0) { // When the chunk is empty it first TS is used for the chunk dict key
        return 0;
    }
    return ((HeaderChunk *)chunk)->baseTimestamp;
}

timestamp_t Header_GetLastTimestamp(Chunk_t *chunk) {
    if (unlikely(((HeaderChunk *)chunk)->count == 0)) { // empty chunks are being removed
        RedisModule_Log(mr_staticCtx, "error", "Trying to get the last timestamp of empty chunk");
    }
    return ((HeaderChunk *)chunk)->prevTimestamp;
}

double Header_GetLastValue(Chunk_t *chunk) {
    if (unlikely(((HeaderChunk *)chunk)->count == 0)) { // empty chunks are being removed
        RedisModule_Log(mr_staticCtx, "error", "Trying to get the last value of empty chunk");
    }
    return ((HeaderChunk *)chunk)->prevValue.d;
}

size_t Header_GetChunkSize(Chunk_t *chunk, bool includeStruct) {
    HeaderChunk *headerChunk = chunk;
    size_t size = headerChunk->size * sizeof(char);
    size += includeStruct ? sizeof(*headerChunk) : 0;
    return size;
}

size_t Header_DelRange(Chunk_t *chunk, timestamp_t startTs, timestamp_t endTs) {
    HeaderChunk *oldChunk = (HeaderChunk *)chunk;
    size_t newSize = oldChunk->size; // mem size
    HeaderChunk *newChunk = Header_NewChunk(newSize);
    Header_Iterator *iter = Header_NewChunkIterator(oldChunk);
    size_t i = 0;
    size_t deleted_count = 0;
    Sample iterSample;
    int numSamples = oldChunk->count; // sample size
    for (; i < numSamples; ++i) {
        Header_ChunkIteratorGetNext(iter, &iterSample);
        if (iterSample.timestamp >= startTs && iterSample.timestamp <= endTs) {
            // in delete range, skip adding to the new chunk
            deleted_count++;
            continue;
        }
        ensureAddSample(newChunk, &iterSample);
    }
    swapChunks(newChunk, oldChunk);
    Header_FreeChunkIterator(iter);
    Header_FreeChunk(newChunk);
    return deleted_count;
}

// TODO: convert to template and unify with decompressChunk when moving to RUST
// decompress chunk reverse
static inline void decompressChunkReverse(const HeaderChunk *headerChunk,
                                          uint64_t start,
                                          uint64_t end,
                                          EnrichedChunk *enrichedChunk) {
    uint64_t numSamples = headerChunk->count;
    uint64_t lastTS = headerChunk->prevTimestamp;
    Sample sample;
    ResetEnrichedChunk(enrichedChunk);
    if (unlikely(numSamples == 0 || end < start || headerChunk->baseTimestamp > end ||
                 lastTS < start)) {
        return;
    }

    Header_Iterator *iter = Header_NewChunkIterator(headerChunk);
    timestamp_t *timestamps_ptr = enrichedChunk->samples.timestamps + numSamples - 1;
    double *values_ptr = enrichedChunk->samples.values + numSamples - 1;

    // find the first sample which is greater than start
    Header_ChunkIteratorGetNext(iter, &sample);
    while (sample.timestamp < start && iter->count < numSamples) {
        Header_ChunkIteratorGetNext(iter, &sample);
    }

    if (unlikely(sample.timestamp > end)) {
        // occurs when the are TS smaller than start and larger than end but nothing in the range.
        Header_FreeChunkIterator(iter);
        return;
    }
    *timestamps_ptr-- = sample.timestamp;
    *values_ptr-- = sample.value;

    if (lastTS > end) { // the range not include the whole chunk
        // 4 samples per iteration
        const size_t n = numSamples >= 4 ? numSamples - 4 : 0;
        while (iter->count < n) {
            Header_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr-- = sample.timestamp;
            *values_ptr-- = sample.value;
            Header_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr-- = sample.timestamp;
            *values_ptr-- = sample.value;
            Header_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr-- = sample.timestamp;
            *values_ptr-- = sample.value;
            Header_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr-- = sample.timestamp;
            *values_ptr-- = sample.value;
            if (unlikely(*(timestamps_ptr + 1) > end)) {
                while (*(timestamps_ptr + 1) > end) {
                    ++timestamps_ptr;
                    ++values_ptr;
                }
                goto _done;
            }
        }

        // left-overs
        while (iter->count < numSamples) {
            Header_ChunkIteratorGetNext(iter, &sample);
            if (sample.timestamp > end) {
                goto _done;
            }
            *timestamps_ptr-- = sample.timestamp;
            *values_ptr-- = sample.value;
        }
    } else {
        while (iter->count < numSamples) {
            Header_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr-- = sample.timestamp;
            *values_ptr-- = sample.value;
        }
    }

_done:
    enrichedChunk->samples.timestamps = timestamps_ptr + 1;
    enrichedChunk->samples.values = values_ptr + 1;
    enrichedChunk->samples.num_samples =
        enrichedChunk->samples.og_timestamps + numSamples - enrichedChunk->samples.timestamps;
    enrichedChunk->rev = true;

    Header_FreeChunkIterator(iter);

    return;
}

// decompress chunk
static inline void decompressChunk(const HeaderChunk *headerChunk,
                                   uint64_t start,
                                   uint64_t end,
                                   EnrichedChunk *enrichedChunk) {
    uint64_t numSamples = headerChunk->count;
    uint64_t lastTS = headerChunk->prevTimestamp;
    Sample sample;
    ChunkResult res;
    ResetEnrichedChunk(enrichedChunk);
    if (unlikely(numSamples == 0 || end < start || headerChunk->baseTimestamp > end ||
                 lastTS < start)) {
        return;
    }

    Header_Iterator *iter = Header_NewChunkIterator(headerChunk);
    timestamp_t *timestamps_ptr = enrichedChunk->samples.timestamps;
    double *values_ptr = enrichedChunk->samples.values;

    // find the first sample which is greater than start
    res = Header_ChunkIteratorGetNext(iter, &sample);
    while (sample.timestamp < start && res == CR_OK) {
        res = Header_ChunkIteratorGetNext(iter, &sample);
    }

    if (unlikely(sample.timestamp > end)) {
        // occurs when the are TS smaller than start and larger than end but nothing in the range.
        Header_FreeChunkIterator(iter);
        return;
    }
    *timestamps_ptr++ = sample.timestamp;
    *values_ptr++ = sample.value;

    if (lastTS > end) { // the range not include the whole chunk
        // 4 samples per iteration
        const size_t n = numSamples >= 4 ? numSamples - 4 : 0;
        while (iter->count < n) {
            Header_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr++ = sample.timestamp;
            *values_ptr++ = sample.value;
            Header_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr++ = sample.timestamp;
            *values_ptr++ = sample.value;
            Header_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr++ = sample.timestamp;
            *values_ptr++ = sample.value;
            Header_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr++ = sample.timestamp;
            *values_ptr++ = sample.value;
            if (unlikely(*(timestamps_ptr - 1) > end)) {
                while (*(timestamps_ptr - 1) > end) {
                    --timestamps_ptr;
                    --values_ptr;
                }
                goto _done;
            }
        }

        // left-overs
        while (iter->count < numSamples) {
            Header_ChunkIteratorGetNext(iter, &sample);
            if (sample.timestamp > end) {
                goto _done;
            }
            *timestamps_ptr++ = sample.timestamp;
            *values_ptr++ = sample.value;
        }
    } else {
        while (iter->count < numSamples) {
            Header_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr++ = sample.timestamp;
            *values_ptr++ = sample.value;
        }
    }

_done:
    enrichedChunk->samples.num_samples = timestamps_ptr - enrichedChunk->samples.timestamps;

    Header_FreeChunkIterator(iter);

    return;
}

/************************
 *  Iterator functions  *
 ************************/
// LCOV_EXCL_START - used for debug
u_int64_t getIterIdx(ChunkIter_t *iter) {
    return ((Header_Iterator *)iter)->idx;
}
// LCOV_EXCL_STOP

void Header_ResetChunkIterator(ChunkIter_t *iterator, const Chunk_t *chunk) {
    const HeaderChunk *headerChunk = chunk;
    Header_Iterator *iter = (Header_Iterator *)iterator;
    iter->chunk = (HeaderChunk *)headerChunk;
    iter->idx = 0;
    iter->count = 0;

    iter->prevDelta = 0;
    iter->prevTS = headerChunk->baseTimestamp;
    iter->prevValue.d = headerChunk->baseValue.d;
    iter->leading = 32;
    iter->trailing = 32;
    iter->blocksize = 0;
    iterator = (ChunkIter_t *)iter;
}

ChunkIter_t *Header_NewChunkIterator(const Chunk_t *chunk) {
    const HeaderChunk *headerChunk = chunk;
    Header_Iterator *iter = (Header_Iterator *)calloc(1, sizeof(Header_Iterator));
    Header_ResetChunkIterator(iter, headerChunk);
    return (ChunkIter_t *)iter;
}

void Header_FreeChunkIterator(ChunkIter_t *iter) {
    free(iter);
}

void Header_ProcessChunk(const Chunk_t *chunk,
                             uint64_t start,
                             uint64_t end,
                             EnrichedChunk *enrichedChunk,
                             bool reverse) {
    if (unlikely(!chunk)) {
        return;
    }
    const HeaderChunk *headerChunk = chunk;

    if (unlikely(reverse)) {
        decompressChunkReverse(headerChunk, start, end, enrichedChunk);
    } else {
        decompressChunk(headerChunk, start, end, enrichedChunk);
    }

    return;
}

typedef void (*SaveUnsignedFunc)(void *, uint64_t);
typedef void (*SaveStringBufferFunc)(void *, const char *str, size_t len);
typedef uint64_t (*ReadUnsignedFunc)(void *);
typedef char *(*ReadStringBufferFunc)(void *, size_t *);

static void Header_Serialize(Chunk_t *chunk,
                                 void *ctx,
                                 SaveUnsignedFunc saveUnsigned,
                                 SaveStringBufferFunc saveStringBuffer) {
    HeaderChunk *compchunk = chunk;

    saveUnsigned(ctx, compchunk->size);
    saveUnsigned(ctx, compchunk->count);
    saveUnsigned(ctx, compchunk->idx);
    saveUnsigned(ctx, compchunk->baseValue.u);
    saveUnsigned(ctx, compchunk->baseTimestamp);
    saveUnsigned(ctx, compchunk->prevTimestamp);
    saveUnsigned(ctx, compchunk->prevTimestampDelta);
    saveUnsigned(ctx, compchunk->prevValue.u);
    saveUnsigned(ctx, compchunk->prevLeading);
    saveUnsigned(ctx, compchunk->prevTrailing);
    saveStringBuffer(ctx, (char *)compchunk->data, compchunk->size);
}

#define COMPRESSED_DESERIALIZE(chunk, ctx, readUnsigned, readStringBuffer, ...)                    \
    do {                                                                                           \
        HeaderChunk *compchunk = (HeaderChunk *)malloc(sizeof(*compchunk));                \
                                                                                                   \
        compchunk->data = NULL;                                                                    \
        compchunk->size = readUnsigned(ctx, ##__VA_ARGS__);                                        \
        compchunk->count = readUnsigned(ctx, ##__VA_ARGS__);                                       \
        compchunk->idx = readUnsigned(ctx, ##__VA_ARGS__);                                         \
        compchunk->baseValue.u = readUnsigned(ctx, ##__VA_ARGS__);                                 \
        compchunk->baseTimestamp = readUnsigned(ctx, ##__VA_ARGS__);                               \
        compchunk->prevTimestamp = readUnsigned(ctx, ##__VA_ARGS__);                               \
        compchunk->prevTimestampDelta = (int64_t)readUnsigned(ctx, ##__VA_ARGS__);                 \
        compchunk->prevValue.u = readUnsigned(ctx, ##__VA_ARGS__);                                 \
        compchunk->prevLeading = readUnsigned(ctx, ##__VA_ARGS__);                                 \
        compchunk->prevTrailing = readUnsigned(ctx, ##__VA_ARGS__);                                \
                                                                                                   \
        size_t len;                                                                                \
        compchunk->data = (uint64_t *)readStringBuffer(ctx, &len, ##__VA_ARGS__);                  \
        *chunk = (Chunk_t *)compchunk;                                                             \
        return TSDB_OK;                                                                            \
                                                                                                   \
err:                                                                                               \
        __attribute__((cold, unused));                                                             \
        *chunk = NULL;                                                                             \
        Header_FreeChunk(compchunk);                                                           \
                                                                                                   \
        return TSDB_ERROR;                                                                         \
    } while (0)

void Header_SaveToRDB(Chunk_t *chunk, struct RedisModuleIO *io) {
    Header_Serialize(chunk,
                         io,
                         (SaveUnsignedFunc)RedisModule_SaveUnsigned,
                         (SaveStringBufferFunc)RedisModule_SaveStringBuffer);
}

int Header_LoadFromRDB(Chunk_t **chunk, struct RedisModuleIO *io) {
    COMPRESSED_DESERIALIZE(chunk, io, LoadUnsigned_IOError, LoadStringBuffer_IOError, goto err);
}

void Header_MRSerialize(Chunk_t *chunk, WriteSerializationCtx *sctx) {
    Header_Serialize(chunk,
                         sctx,
                         (SaveUnsignedFunc)MR_SerializationCtxWriteLongLongWrapper,
                         (SaveStringBufferFunc)MR_SerializationCtxWriteBufferWrapper);
}

int Header_MRDeserialize(Chunk_t **chunk, ReaderSerializationCtx *sctx) {
    COMPRESSED_DESERIALIZE(
        chunk, sctx, MR_SerializationCtxReadeLongLongWrapper, MR_ownedBufferFrom);
}

#include <assert.h> // assert
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>  // printf
#include <stdlib.h> // malloc
#include "rmutil/alloc.h"

#define BIT 8
#define CHUNK_RESIZE_STEP 32

/*********************
 *  Chunk functions  *
 *********************/
Chunk_t *Header_NewChunk(size_t size) {
    HeaderChunk *chunk = (HeaderChunk *)calloc(1, sizeof(HeaderChunk));
    chunk->size = size;
    chunk->data = (u_int64_t *)calloc(chunk->size, sizeof(char));
#ifdef DEBUG
    memset(chunk->data, 0, chunk->size);
#endif
    chunk->prevLeading = 32;
    chunk->prevTrailing = 32;
    chunk->prevTimestamp = 0;
    return chunk;
}

void Header_FreeChunk(Chunk_t *chunk) {
    HeaderChunk *headerChunk = chunk;
    if (headerChunk->data) {
        free(headerChunk->data);
    }
    headerChunk->data = NULL;
    free(chunk);
}

Chunk_t *Header_CloneChunk(const Chunk_t *chunk) {
    const HeaderChunk *oldChunk = chunk;
    HeaderChunk *newChunk = malloc(sizeof(HeaderChunk));
    memcpy(newChunk, oldChunk, sizeof(HeaderChunk));
    newChunk->data = malloc(newChunk->size);
    memcpy(newChunk->data, oldChunk->data, oldChunk->size);
    return newChunk;
}

static void swapChunks(HeaderChunk *a, HeaderChunk *b) {
    HeaderChunk tmp = *a;
    *a = *b;
    *b = tmp;
}

static void ensureAddSample(HeaderChunk *chunk, Sample *sample) {
    ChunkResult res = Header_AddSample(chunk, sample);
    if (res != CR_OK) {
        int oldsize = chunk->size;
        chunk->size += CHUNK_RESIZE_STEP;
        chunk->data = (u_int64_t *)realloc(chunk->data, chunk->size * sizeof(char));
        memset((char *)chunk->data + oldsize, 0, CHUNK_RESIZE_STEP);
        // printf("Chunk extended to %lu \n", chunk->size);
        res = Header_AddSample(chunk, sample);
        assert(res == CR_OK);
    }
}

static void trimChunk(HeaderChunk *chunk) {
    int excess = (chunk->size * BIT - chunk->idx) / BIT;

    assert(excess >= 0); // else we have written beyond allocated memory

    if (excess > 1) {
        size_t newSize = chunk->size - excess + 1;
        // align to 8 bytes (u_int64_t) otherwise we will have an heap overflow in gorilla.c because
        // each write happens in 8 bytes blocks.
        newSize += sizeof(binary_t) - (newSize % sizeof(binary_t));
        chunk->data = realloc(chunk->data, newSize);
        chunk->size = newSize;
    }
}

Chunk_t *Header_SplitChunk(Chunk_t *chunk) {
    HeaderChunk *curChunk = chunk;
    size_t split = curChunk->count / 2;
    size_t curNumSamples = curChunk->count - split;

    // add samples in new chunks
    size_t i = 0;
    Sample sample;
    ChunkIter_t *iter = Header_NewChunkIterator(curChunk);
    HeaderChunk *newChunk1 = Header_NewChunk(curChunk->size);
    HeaderChunk *newChunk2 = Header_NewChunk(curChunk->size);
    for (; i < curNumSamples; ++i) {
        Header_ChunkIteratorGetNext(iter, &sample);
        ensureAddSample(newChunk1, &sample);
    }
    for (; i < curChunk->count; ++i) {
        Header_ChunkIteratorGetNext(iter, &sample);
        ensureAddSample(newChunk2, &sample);
    }

    trimChunk(newChunk1);
    trimChunk(newChunk2);
    swapChunks(curChunk, newChunk1);

    Header_FreeChunkIterator(iter);
    Header_FreeChunk(newChunk1);

    return newChunk2;
}

ChunkResult Header_UpsertSample(UpsertCtx *uCtx, int *size, DuplicatePolicy duplicatePolicy) {
    *size = 0;
    ChunkResult rv = CR_OK;
    ChunkResult nextRes = CR_OK;
    HeaderChunk *oldChunk = (HeaderChunk *)uCtx->inChunk;

    size_t newSize = oldChunk->size;

    HeaderChunk *newChunk = Header_NewChunk(newSize);
    Header_Iterator *iter = Header_NewChunkIterator(oldChunk);
    timestamp_t ts = uCtx->sample.timestamp;
    int numSamples = oldChunk->count;

    size_t i = 0;
    Sample iterSample;
    for (; i < numSamples; ++i) {
        nextRes = Header_ChunkIteratorGetNext(iter, &iterSample);
        if (iterSample.timestamp >= ts) {
            break;
        }
        ensureAddSample(newChunk, &iterSample);
    }

    if (ts == iterSample.timestamp) {
        ChunkResult cr = handleDuplicateSample(duplicatePolicy, iterSample, &uCtx->sample);
        if (cr != CR_OK) {
            Header_FreeChunkIterator(iter);
            Header_FreeChunk(newChunk);
            return CR_ERR;
        }
        nextRes = Header_ChunkIteratorGetNext(iter, &iterSample);
        *size = -1; // we skipped a sample
    }
    // upsert the sample
    ensureAddSample(newChunk, &uCtx->sample);
    *size += 1;

    if (i < numSamples) {
        while (nextRes == CR_OK) {
            ensureAddSample(newChunk, &iterSample);
            nextRes = Header_ChunkIteratorGetNext(iter, &iterSample);
        }
    }

    swapChunks(newChunk, oldChunk);

    Header_FreeChunkIterator(iter);
    Header_FreeChunk(newChunk);
    return rv;
}

ChunkResult Header_AddSample(Chunk_t *chunk, Sample *sample) {
    return Header_Append((HeaderChunk *)chunk, sample->timestamp, sample->value);
}

u_int64_t Header_ChunkNumOfSample(Chunk_t *chunk) {
    return ((HeaderChunk *)chunk)->count;
}

timestamp_t Header_GetFirstTimestamp(Chunk_t *chunk) {
    if (((HeaderChunk *)chunk)->count ==
        0) { // When the chunk is empty it first TS is used for the chunk dict key
        return 0;
    }
    return ((HeaderChunk *)chunk)->baseTimestamp;
}

timestamp_t Header_GetLastTimestamp(Chunk_t *chunk) {
    if (unlikely(((HeaderChunk *)chunk)->count == 0)) { // empty chunks are being removed
        RedisModule_Log(mr_staticCtx, "error", "Trying to get the last timestamp of empty chunk");
    }
    return ((HeaderChunk *)chunk)->prevTimestamp;
}

double Header_GetLastValue(Chunk_t *chunk) {
    if (unlikely(((HeaderChunk *)chunk)->count == 0)) { // empty chunks are being removed
        RedisModule_Log(mr_staticCtx, "error", "Trying to get the last value of empty chunk");
    }
    return ((HeaderChunk *)chunk)->prevValue.d;
}

size_t Header_GetChunkSize(Chunk_t *chunk, bool includeStruct) {
    HeaderChunk *headerChunk = chunk;
    size_t size = headerChunk->size * sizeof(char);
    size += includeStruct ? sizeof(*headerChunk) : 0;
    return size;
}

size_t Header_DelRange(Chunk_t *chunk, timestamp_t startTs, timestamp_t endTs) {
    HeaderChunk *oldChunk = (HeaderChunk *)chunk;
    size_t newSize = oldChunk->size; // mem size
    HeaderChunk *newChunk = Header_NewChunk(newSize);
    Header_Iterator *iter = Header_NewChunkIterator(oldChunk);
    size_t i = 0;
    size_t deleted_count = 0;
    Sample iterSample;
    int numSamples = oldChunk->count; // sample size
    for (; i < numSamples; ++i) {
        Header_ChunkIteratorGetNext(iter, &iterSample);
        if (iterSample.timestamp >= startTs && iterSample.timestamp <= endTs) {
            // in delete range, skip adding to the new chunk
            deleted_count++;
            continue;
        }
        ensureAddSample(newChunk, &iterSample);
    }
    swapChunks(newChunk, oldChunk);
    Header_FreeChunkIterator(iter);
    Header_FreeChunk(newChunk);
    return deleted_count;
}

// TODO: convert to template and unify with decompressChunk when moving to RUST
// decompress chunk reverse
static inline void decompressChunkReverse(const HeaderChunk *headerChunk,
                                          uint64_t start,
                                          uint64_t end,
                                          EnrichedChunk *enrichedChunk) {
    uint64_t numSamples = headerChunk->count;
    uint64_t lastTS = headerChunk->prevTimestamp;
    Sample sample;
    ResetEnrichedChunk(enrichedChunk);
    if (unlikely(numSamples == 0 || end < start || headerChunk->baseTimestamp > end ||
                 lastTS < start)) {
        return;
    }

    Header_Iterator *iter = Header_NewChunkIterator(headerChunk);
    timestamp_t *timestamps_ptr = enrichedChunk->samples.timestamps + numSamples - 1;
    double *values_ptr = enrichedChunk->samples.values + numSamples - 1;

    // find the first sample which is greater than start
    Header_ChunkIteratorGetNext(iter, &sample);
    while (sample.timestamp < start && iter->count < numSamples) {
        Header_ChunkIteratorGetNext(iter, &sample);
    }

    if (unlikely(sample.timestamp > end)) {
        // occurs when the are TS smaller than start and larger than end but nothing in the range.
        Header_FreeChunkIterator(iter);
        return;
    }
    *timestamps_ptr-- = sample.timestamp;
    *values_ptr-- = sample.value;

    if (lastTS > end) { // the range not include the whole chunk
        // 4 samples per iteration
        const size_t n = numSamples >= 4 ? numSamples - 4 : 0;
        while (iter->count < n) {
            Header_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr-- = sample.timestamp;
            *values_ptr-- = sample.value;
            Header_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr-- = sample.timestamp;
            *values_ptr-- = sample.value;
            Header_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr-- = sample.timestamp;
            *values_ptr-- = sample.value;
            Header_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr-- = sample.timestamp;
            *values_ptr-- = sample.value;
            if (unlikely(*(timestamps_ptr + 1) > end)) {
                while (*(timestamps_ptr + 1) > end) {
                    ++timestamps_ptr;
                    ++values_ptr;
                }
                goto _done;
            }
        }

        // left-overs
        while (iter->count < numSamples) {
            Header_ChunkIteratorGetNext(iter, &sample);
            if (sample.timestamp > end) {
                goto _done;
            }
            *timestamps_ptr-- = sample.timestamp;
            *values_ptr-- = sample.value;
        }
    } else {
        while (iter->count < numSamples) {
            Header_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr-- = sample.timestamp;
            *values_ptr-- = sample.value;
        }
    }

_done:
    enrichedChunk->samples.timestamps = timestamps_ptr + 1;
    enrichedChunk->samples.values = values_ptr + 1;
    enrichedChunk->samples.num_samples =
        enrichedChunk->samples.og_timestamps + numSamples - enrichedChunk->samples.timestamps;
    enrichedChunk->rev = true;

    Header_FreeChunkIterator(iter);

    return;
}

// decompress chunk
static inline void decompressChunk(const HeaderChunk *headerChunk,
                                   uint64_t start,
                                   uint64_t end,
                                   EnrichedChunk *enrichedChunk) {
    uint64_t numSamples = headerChunk->count;
    uint64_t lastTS = headerChunk->prevTimestamp;
    Sample sample;
    ChunkResult res;
    ResetEnrichedChunk(enrichedChunk);
    if (unlikely(numSamples == 0 || end < start || headerChunk->baseTimestamp > end ||
                 lastTS < start)) {
        return;
    }

    Header_Iterator *iter = Header_NewChunkIterator(headerChunk);
    timestamp_t *timestamps_ptr = enrichedChunk->samples.timestamps;
    double *values_ptr = enrichedChunk->samples.values;

    // find the first sample which is greater than start
    res = Header_ChunkIteratorGetNext(iter, &sample);
    while (sample.timestamp < start && res == CR_OK) {
        res = Header_ChunkIteratorGetNext(iter, &sample);
    }

    if (unlikely(sample.timestamp > end)) {
        // occurs when the are TS smaller than start and larger than end but nothing in the range.
        Header_FreeChunkIterator(iter);
        return;
    }
    *timestamps_ptr++ = sample.timestamp;
    *values_ptr++ = sample.value;

    if (lastTS > end) { // the range not include the whole chunk
        // 4 samples per iteration
        const size_t n = numSamples >= 4 ? numSamples - 4 : 0;
        while (iter->count < n) {
            Header_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr++ = sample.timestamp;
            *values_ptr++ = sample.value;
            Header_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr++ = sample.timestamp;
            *values_ptr++ = sample.value;
            Header_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr++ = sample.timestamp;
            *values_ptr++ = sample.value;
            Header_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr++ = sample.timestamp;
            *values_ptr++ = sample.value;
            if (unlikely(*(timestamps_ptr - 1) > end)) {
                while (*(timestamps_ptr - 1) > end) {
                    --timestamps_ptr;
                    --values_ptr;
                }
                goto _done;
            }
        }

        // left-overs
        while (iter->count < numSamples) {
            Header_ChunkIteratorGetNext(iter, &sample);
            if (sample.timestamp > end) {
                goto _done;
            }
            *timestamps_ptr++ = sample.timestamp;
            *values_ptr++ = sample.value;
        }
    } else {
        while (iter->count < numSamples) {
            Header_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr++ = sample.timestamp;
            *values_ptr++ = sample.value;
        }
    }

_done:
    enrichedChunk->samples.num_samples = timestamps_ptr - enrichedChunk->samples.timestamps;

    Header_FreeChunkIterator(iter);

    return;
}

/************************
 *  Iterator functions  *
 ************************/
// LCOV_EXCL_START - used for debug
u_int64_t getIterIdx(ChunkIter_t *iter) {
    return ((Header_Iterator *)iter)->idx;
}
// LCOV_EXCL_STOP

void Header_ResetChunkIterator(ChunkIter_t *iterator, const Chunk_t *chunk) {
    const HeaderChunk *headerChunk = chunk;
    Header_Iterator *iter = (Header_Iterator *)iterator;
    iter->chunk = (HeaderChunk *)headerChunk;
    iter->idx = 0;
    iter->count = 0;

    iter->prevDelta = 0;
    iter->prevTS = headerChunk->baseTimestamp;
    iter->prevDeltaTs = 0;
    iter->prevValue = headerChunk->baseValue;
    iter->prevValueDelta = 0;
    iterator = (ChunkIter_t *)iter;
}

ChunkIter_t *Header_NewChunkIterator(const Chunk_t *chunk) {
    const HeaderChunk *headerChunk = chunk;
    Header_Iterator *iter = (Header_Iterator *)calloc(1, sizeof(Header_Iterator));
    Header_ResetChunkIterator(iter, headerChunk);
    return (ChunkIter_t *)iter;
}

void Header_FreeChunkIterator(ChunkIter_t *iter) {
    free(iter);
}

void Header_ProcessChunk(const Chunk_t *chunk,
                             uint64_t start,
                             uint64_t end,
                             EnrichedChunk *enrichedChunk,
                             bool reverse) {
    if (unlikely(!chunk)) {
        return;
    }
    const HeaderChunk *headerChunk = chunk;

    if (unlikely(reverse)) {
        decompressChunkReverse(headerChunk, start, end, enrichedChunk);
    } else {
        decompressChunk(headerChunk, start, end, enrichedChunk);
    }

    return;
}

typedef void (*SaveUnsignedFunc)(void *, uint64_t);
typedef void (*SaveStringBufferFunc)(void *, const char *str, size_t len);
typedef uint64_t (*ReadUnsignedFunc)(void *);
typedef char *(*ReadStringBufferFunc)(void *, size_t *);

static void Header_Serialize(Chunk_t *chunk,
                                 void *ctx,
                                 SaveUnsignedFunc saveUnsigned,
                                 SaveStringBufferFunc saveStringBuffer) {
    HeaderChunk *compchunk = chunk;

    saveUnsigned(ctx, compchunk->size);
    saveUnsigned(ctx, compchunk->count);
    saveUnsigned(ctx, compchunk->idx);
    saveUnsigned(ctx, compchunk->baseValue.u);
    saveUnsigned(ctx, compchunk->baseTimestamp);
    saveUnsigned(ctx, compchunk->prevTimestamp);
    saveUnsigned(ctx, compchunk->prevTimestampDelta);
    saveUnsigned(ctx, compchunk->prevValue.u);
    saveUnsigned(ctx, compchunk->prevLeading);
    saveUnsigned(ctx, compchunk->prevTrailing);
    saveStringBuffer(ctx, (char *)compchunk->data, compchunk->size);
}

#define COMPRESSED_DESERIALIZE(chunk, ctx, readUnsigned, readStringBuffer, ...)                    \
    do {                                                                                           \
        HeaderChunk *compchunk = (HeaderChunk *)malloc(sizeof(*compchunk));                \
                                                                                                   \
        compchunk->data = NULL;                                                                    \
        compchunk->size = readUnsigned(ctx, ##__VA_ARGS__);                                        \
        compchunk->count = readUnsigned(ctx, ##__VA_ARGS__);                                       \
        compchunk->idx = readUnsigned(ctx, ##__VA_ARGS__);                                         \
        compchunk->baseValue.u = readUnsigned(ctx, ##__VA_ARGS__);                                 \
        compchunk->baseTimestamp = readUnsigned(ctx, ##__VA_ARGS__);                               \
        compchunk->prevTimestamp = readUnsigned(ctx, ##__VA_ARGS__);                               \
        compchunk->prevTimestampDelta = (int64_t)readUnsigned(ctx, ##__VA_ARGS__);                 \
        compchunk->prevValue.u = readUnsigned(ctx, ##__VA_ARGS__);                                 \
        compchunk->prevLeading = readUnsigned(ctx, ##__VA_ARGS__);                                 \
        compchunk->prevTrailing = readUnsigned(ctx, ##__VA_ARGS__);                                \
                                                                                                   \
        size_t len;                                                                                \
        compchunk->data = (uint64_t *)readStringBuffer(ctx, &len, ##__VA_ARGS__);                  \
        *chunk = (Chunk_t *)compchunk;                                                             \
        return TSDB_OK;                                                                            \
                                                                                                   \
err:                                                                                               \
        __attribute__((cold, unused));                                                             \
        *chunk = NULL;                                                                             \
        Header_FreeChunk(compchunk);                                                           \
                                                                                                   \
        return TSDB_ERROR;                                                                         \
    } while (0)

void Header_SaveToRDB(Chunk_t *chunk, struct RedisModuleIO *io) {
    Header_Serialize(chunk,
                         io,
                         (SaveUnsignedFunc)RedisModule_SaveUnsigned,
                         (SaveStringBufferFunc)RedisModule_SaveStringBuffer);
}

int Header_LoadFromRDB(Chunk_t **chunk, struct RedisModuleIO *io) {
    COMPRESSED_DESERIALIZE(chunk, io, LoadUnsigned_IOError, LoadStringBuffer_IOError, goto err);
}

void Header_MRSerialize(Chunk_t *chunk, WriteSerializationCtx *sctx) {
    Header_Serialize(chunk,
                         sctx,
                         (SaveUnsignedFunc)MR_SerializationCtxWriteLongLongWrapper,
                         (SaveStringBufferFunc)MR_SerializationCtxWriteBufferWrapper);
}

int Header_MRDeserialize(Chunk_t **chunk, ReaderSerializationCtx *sctx) {
    COMPRESSED_DESERIALIZE(
        chunk, sctx, MR_SerializationCtxReadeLongLongWrapper, MR_ownedBufferFrom);
}
