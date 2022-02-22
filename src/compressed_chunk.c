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

#define BIT 8
#define CHUNK_RESIZE_STEP 32

/*********************
 *  Chunk functions  *
 *********************/
Chunk_t *Compressed_NewChunk(size_t size) {
    CompressedChunk *chunk = (CompressedChunk *)calloc(1, sizeof(CompressedChunk));
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

void Compressed_FreeChunk(Chunk_t *chunk) {
    CompressedChunk *cmpChunk = chunk;
    if (cmpChunk->data) {
        free(cmpChunk->data);
    }
    cmpChunk->data = NULL;
    free(chunk);
}

Chunk_t *Compressed_CloneChunk(const Chunk_t *chunk) {
    const CompressedChunk *oldChunk = chunk;
    CompressedChunk *newChunk = malloc(sizeof(CompressedChunk));
    memcpy(newChunk, oldChunk, sizeof(CompressedChunk));
    newChunk->data = malloc(newChunk->size);
    memcpy(newChunk->data, oldChunk->data, oldChunk->size);
    return newChunk;
}

static void swapChunks(CompressedChunk *a, CompressedChunk *b) {
    CompressedChunk tmp = *a;
    *a = *b;
    *b = tmp;
}

static void ensureAddSample(CompressedChunk *chunk, Sample *sample) {
    ChunkResult res = Compressed_AddSample(chunk, sample);
    if (res != CR_OK) {
        int oldsize = chunk->size;
        chunk->size += CHUNK_RESIZE_STEP;
        chunk->data = (u_int64_t *)realloc(chunk->data, chunk->size * sizeof(char));
        memset((char *)chunk->data + oldsize, 0, CHUNK_RESIZE_STEP);
        // printf("Chunk extended to %lu \n", chunk->size);
        res = Compressed_AddSample(chunk, sample);
        assert(res == CR_OK);
    }
}

static void trimChunk(CompressedChunk *chunk) {
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

Chunk_t *Compressed_SplitChunk(Chunk_t *chunk) {
    CompressedChunk *curChunk = chunk;
    size_t split = curChunk->count / 2;
    size_t curNumSamples = curChunk->count - split;

    // add samples in new chunks
    size_t i = 0;
    Sample sample;
    ChunkIter_t *iter = Compressed_NewChunkIterator(curChunk);
    CompressedChunk *newChunk1 = Compressed_NewChunk(curChunk->size);
    CompressedChunk *newChunk2 = Compressed_NewChunk(curChunk->size);
    for (; i < curNumSamples; ++i) {
        Compressed_ChunkIteratorGetNext(iter, &sample);
        ensureAddSample(newChunk1, &sample);
    }
    for (; i < curChunk->count; ++i) {
        Compressed_ChunkIteratorGetNext(iter, &sample);
        ensureAddSample(newChunk2, &sample);
    }

    trimChunk(newChunk1);
    trimChunk(newChunk2);
    swapChunks(curChunk, newChunk1);

    Compressed_FreeChunkIterator(iter);
    Compressed_FreeChunk(newChunk1);

    return newChunk2;
}

ChunkResult Compressed_UpsertSample(UpsertCtx *uCtx, int *size, DuplicatePolicy duplicatePolicy) {
    *size = 0;
    ChunkResult rv = CR_OK;
    ChunkResult nextRes = CR_OK;
    CompressedChunk *oldChunk = (CompressedChunk *)uCtx->inChunk;

    size_t newSize = oldChunk->size;

    CompressedChunk *newChunk = Compressed_NewChunk(newSize);
    Compressed_Iterator *iter = Compressed_NewChunkIterator(oldChunk);
    timestamp_t ts = uCtx->sample.timestamp;
    int numSamples = oldChunk->count;

    size_t i = 0;
    Sample iterSample;
    for (; i < numSamples; ++i) {
        nextRes = Compressed_ChunkIteratorGetNext(iter, &iterSample);
        if (iterSample.timestamp >= ts) {
            break;
        }
        ensureAddSample(newChunk, &iterSample);
    }

    if (ts == iterSample.timestamp) {
        ChunkResult cr = handleDuplicateSample(duplicatePolicy, iterSample, &uCtx->sample);
        if (cr != CR_OK) {
            Compressed_FreeChunkIterator(iter);
            Compressed_FreeChunk(newChunk);
            return CR_ERR;
        }
        nextRes = Compressed_ChunkIteratorGetNext(iter, &iterSample);
        *size = -1; // we skipped a sample
    }
    // upsert the sample
    ensureAddSample(newChunk, &uCtx->sample);
    *size += 1;

    if (i < numSamples) {
        while (nextRes == CR_OK) {
            ensureAddSample(newChunk, &iterSample);
            nextRes = Compressed_ChunkIteratorGetNext(iter, &iterSample);
        }
    }

    swapChunks(newChunk, oldChunk);

    Compressed_FreeChunkIterator(iter);
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

size_t Compressed_GetChunkSize(Chunk_t *chunk, bool includeStruct) {
    CompressedChunk *cmpChunk = chunk;
    size_t size = cmpChunk->size * sizeof(char);
    size += includeStruct ? sizeof(*cmpChunk) : 0;
    return size;
}

size_t Compressed_DelRange(Chunk_t *chunk, timestamp_t startTs, timestamp_t endTs) {
    CompressedChunk *oldChunk = (CompressedChunk *)chunk;
    size_t newSize = oldChunk->size; // mem size
    CompressedChunk *newChunk = Compressed_NewChunk(newSize);
    Compressed_Iterator *iter = Compressed_NewChunkIterator(oldChunk);
    size_t i = 0;
    size_t deleted_count = 0;
    Sample iterSample;
    int numSamples = oldChunk->count; // sample size
    for (; i < numSamples; ++i) {
        Compressed_ChunkIteratorGetNext(iter, &iterSample);
        if (iterSample.timestamp >= startTs && iterSample.timestamp <= endTs) {
            // in delete range, skip adding to the new chunk
            deleted_count++;
            continue;
        }
        ensureAddSample(newChunk, &iterSample);
    }
    swapChunks(newChunk, oldChunk);
    Compressed_FreeChunkIterator(iter);
    Compressed_FreeChunk(newChunk);
    return deleted_count;
}

// TODO: convert to template and unify with decompressChunk when moving to RUST
// decompress chunk reverse
static inline DomainChunk *decompressChunkReverse(const CompressedChunk *compressedChunk,
                                                  uint64_t start,
                                                  uint64_t end,
                                                  DomainChunk *domainChunk,
                                                  DomainChunk *domainChunkAux) {
    uint64_t numSamples = compressedChunk->count;
    uint64_t lastTS = compressedChunk->prevTimestamp;
    Sample sample;
    domainChunk->num_samples = 0;
    domainChunk->rev = true;
    if (unlikely(numSamples == 0 || end < start || compressedChunk->baseTimestamp > end ||
                 lastTS < start)) {
        return domainChunk;
    }

    Compressed_Iterator *iter = Compressed_NewChunkIterator(compressedChunk);
    timestamp_t *timestamps_ptr = domainChunk->samples.timestamps + numSamples - 1;
    double *values_ptr = domainChunk->samples.values + numSamples - 1;

    // find the first sample which is greater than start
    Compressed_ChunkIteratorGetNext(iter, &sample);
    while (sample.timestamp < start && iter->count < numSamples) {
        Compressed_ChunkIteratorGetNext(iter, &sample);
    }

    if (unlikely(sample.timestamp > end)) {
        // occurs when the are TS smaller than start and larger than end but nothing in the range.
        return domainChunk;
    }
    *timestamps_ptr-- = sample.timestamp;
    *values_ptr-- = sample.value;

    if (lastTS > end) { // the range not include the whole chunk
        // 4 samples per iteration
        const size_t n = numSamples >= 4 ? numSamples - 4 : 0;
        while (iter->count < n) {
            Compressed_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr-- = sample.timestamp;
            *values_ptr-- = sample.value;
            Compressed_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr-- = sample.timestamp;
            *values_ptr-- = sample.value;
            Compressed_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr-- = sample.timestamp;
            *values_ptr-- = sample.value;
            Compressed_ChunkIteratorGetNext(iter, &sample);
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
            Compressed_ChunkIteratorGetNext(iter, &sample);
            if (sample.timestamp > end) {
                goto _done;
            }
            *timestamps_ptr-- = sample.timestamp;
            *values_ptr-- = sample.value;
        }
    } else {
        while (iter->count < numSamples) {
            Compressed_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr-- = sample.timestamp;
            *values_ptr-- = sample.value;
        }
    }

_done:
    domainChunkAux->samples.timestamps = timestamps_ptr + 1;
    domainChunkAux->samples.values = values_ptr + 1;
    domainChunkAux->num_samples =
        domainChunk->samples.timestamps + numSamples - domainChunkAux->samples.timestamps;
    domainChunkAux->rev = true;

    Compressed_FreeChunkIterator(iter);

    return domainChunkAux;
}

// decompress chunk
static inline DomainChunk *decompressChunk(const CompressedChunk *compressedChunk,
                                           uint64_t start,
                                           uint64_t end,
                                           DomainChunk *domainChunk) {
    uint64_t numSamples = compressedChunk->count;
    uint64_t lastTS = compressedChunk->prevTimestamp;
    Sample sample;
    domainChunk->num_samples = 0;
    domainChunk->rev = false;
    if (unlikely(numSamples == 0 || end < start || compressedChunk->baseTimestamp > end ||
                 lastTS < start)) {
        return domainChunk;
    }

    Compressed_Iterator *iter = Compressed_NewChunkIterator(compressedChunk);
    timestamp_t *timestamps_ptr = domainChunk->samples.timestamps;
    double *values_ptr = domainChunk->samples.values;

    // find the first sample which is greater than start
    Compressed_ChunkIteratorGetNext(iter, &sample);
    while (sample.timestamp < start && iter->count < numSamples) {
        Compressed_ChunkIteratorGetNext(iter, &sample);
    }

    if (unlikely(sample.timestamp > end)) {
        // occurs when the are TS smaller than start and larger than end but nothing in the range.
        return domainChunk;
    }
    *timestamps_ptr++ = sample.timestamp;
    *values_ptr++ = sample.value;

    if (lastTS > end) { // the range not include the whole chunk
        // 4 samples per iteration
        const size_t n = numSamples >= 4 ? numSamples - 4 : 0;
        while (iter->count < n) {
            Compressed_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr++ = sample.timestamp;
            *values_ptr++ = sample.value;
            Compressed_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr++ = sample.timestamp;
            *values_ptr++ = sample.value;
            Compressed_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr++ = sample.timestamp;
            *values_ptr++ = sample.value;
            Compressed_ChunkIteratorGetNext(iter, &sample);
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
            Compressed_ChunkIteratorGetNext(iter, &sample);
            if (sample.timestamp > end) {
                goto _done;
            }
            *timestamps_ptr++ = sample.timestamp;
            *values_ptr++ = sample.value;
        }
    } else {
        while (iter->count < numSamples) {
            Compressed_ChunkIteratorGetNext(iter, &sample);
            *timestamps_ptr++ = sample.timestamp;
            *values_ptr++ = sample.value;
        }
    }

_done:
    domainChunk->num_samples = timestamps_ptr - domainChunk->samples.timestamps;

    Compressed_FreeChunkIterator(iter);

    return domainChunk;
}

/************************
 *  Iterator functions  *
 ************************/
// LCOV_EXCL_START - used for debug
u_int64_t getIterIdx(ChunkIter_t *iter) {
    return ((Compressed_Iterator *)iter)->idx;
}
// LCOV_EXCL_STOP

void Compressed_ResetChunkIterator(ChunkIter_t *iterator, const Chunk_t *chunk) {
    const CompressedChunk *compressedChunk = chunk;
    Compressed_Iterator *iter = (Compressed_Iterator *)iterator;
    iter->chunk = (CompressedChunk *)compressedChunk;
    iter->idx = 0;
    iter->count = 0;

    iter->prevDelta = 0;
    iter->prevTS = compressedChunk->baseTimestamp;
    iter->prevValue.d = compressedChunk->baseValue.d;
    iter->leading = 32;
    iter->trailing = 32;
    iter->blocksize = 0;
    iterator = (ChunkIter_t *)iter;
}

ChunkIter_t *Compressed_NewChunkIterator(const Chunk_t *chunk) {
    const CompressedChunk *compressedChunk = chunk;
    Compressed_Iterator *iter = (Compressed_Iterator *)calloc(1, sizeof(Compressed_Iterator));
    Compressed_ResetChunkIterator(iter, compressedChunk);
    return (ChunkIter_t *)iter;
}

void Compressed_FreeChunkIterator(ChunkIter_t *iter) {
    free(iter);
}

DomainChunk *Compressed_ProcessChunk(const Chunk_t *chunk,
                                     uint64_t start,
                                     uint64_t end,
                                     DomainChunk *domainChunk,
                                     DomainChunk *domainChunkAux,
                                     bool reverse) {
    if (unlikely(!chunk)) {
        return NULL;
    }
    const CompressedChunk *compressedChunk = chunk;
    DomainChunk *ret_domainChunk;

    if (unlikely(reverse)) {
        ret_domainChunk =
            decompressChunkReverse(compressedChunk, start, end, domainChunk, domainChunkAux);
    } else {
        ret_domainChunk = decompressChunk(compressedChunk, start, end, domainChunk);
    }
    if (unlikely(ret_domainChunk->num_samples == 0)) {
        return NULL;
    }
    return ret_domainChunk;
}

typedef void (*SaveUnsignedFunc)(void *, uint64_t);
typedef void (*SaveStringBufferFunc)(void *, const char *str, size_t len);
typedef uint64_t (*ReadUnsignedFunc)(void *);
typedef char *(*ReadStringBufferFunc)(void *, size_t *);

static void Compressed_Serialize(Chunk_t *chunk,
                                 void *ctx,
                                 SaveUnsignedFunc saveUnsigned,
                                 SaveStringBufferFunc saveStringBuffer) {
    CompressedChunk *compchunk = chunk;

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
        CompressedChunk *compchunk = (CompressedChunk *)malloc(sizeof(*compchunk));                \
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
        Compressed_FreeChunk(compchunk);                                                           \
                                                                                                   \
        return TSDB_ERROR;                                                                         \
    } while (0)

void Compressed_SaveToRDB(Chunk_t *chunk, struct RedisModuleIO *io) {
    Compressed_Serialize(chunk,
                         io,
                         (SaveUnsignedFunc)RedisModule_SaveUnsigned,
                         (SaveStringBufferFunc)RedisModule_SaveStringBuffer);
}

int Compressed_LoadFromRDB(Chunk_t **chunk, struct RedisModuleIO *io) {
    COMPRESSED_DESERIALIZE(chunk, io, LoadUnsigned_IOError, LoadStringBuffer_IOError, goto err);
}

void Compressed_MRSerialize(Chunk_t *chunk, WriteSerializationCtx *sctx) {
    Compressed_Serialize(chunk,
                         sctx,
                         (SaveUnsignedFunc)MR_SerializationCtxWriteLongLongWrapper,
                         (SaveStringBufferFunc)MR_SerializationCtxWriteBufferWrapper);
}

int Compressed_MRDeserialize(Chunk_t **chunk, ReaderSerializationCtx *sctx) {
    COMPRESSED_DESERIALIZE(
        chunk, sctx, MR_SerializationCtxReadeLongLongWrapper, MR_ownedBufferFrom);
}
