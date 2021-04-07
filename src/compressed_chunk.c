/*
 * Copyright 2018-2020 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "compressed_chunk.h"

#include "chunk.h"
#include "generic_chunk.h"

#include <assert.h> // assert
#include <limits.h>
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
    ChunkIter_t *iter = Compressed_NewChunkIterator(curChunk, CHUNK_ITER_OP_NONE, NULL);
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
    Compressed_Iterator *iter = Compressed_NewChunkIterator(oldChunk, CHUNK_ITER_OP_NONE, NULL);
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

static Chunk *decompressChunk(CompressedChunk *compressedChunk) {
    Sample sample;
    uint64_t numSamples = compressedChunk->count;
    Chunk *uncompressedChunk = Uncompressed_NewChunk(numSamples * SAMPLE_SIZE);

    ChunkIter_t *iter = Compressed_NewChunkIterator(compressedChunk, CHUNK_ITER_OP_NONE, NULL);
    for (uint64_t i = 0; i < numSamples; ++i) {
        Compressed_ChunkIteratorGetNext(iter, &sample);
        Uncompressed_AddSample(uncompressedChunk, &sample);
    }
    Compressed_FreeChunkIterator(iter);
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

ChunkIter_t *Compressed_NewChunkIterator(Chunk_t *chunk,
                                         int options,
                                         ChunkIterFuncs *retChunkIterClass) {
    CompressedChunk *compressedChunk = chunk;

    // for reverse iterator of compressed chunks
    if (options & CHUNK_ITER_OP_REVERSE) {
        int uncompressed_options = CHUNK_ITER_OP_REVERSE | CHUNK_ITER_OP_FREE_CHUNK;
        Chunk *uncompressedChunk = decompressChunk(compressedChunk);
        return Uncompressed_NewChunkIterator(
            uncompressedChunk, uncompressed_options, retChunkIterClass);
    }

    if (retChunkIterClass != NULL) {
        *retChunkIterClass = *GetChunkIteratorClass(CHUNK_COMPRESSED);
        ;
    }

    Compressed_Iterator *iter = (Compressed_Iterator *)calloc(1, sizeof(Compressed_Iterator));

    iter->chunk = compressedChunk;
    iter->idx = 0;
    iter->count = 0;

    iter->prevTS = compressedChunk->baseTimestamp;
    iter->prevDelta = 0;

    iter->prevValue.d = compressedChunk->baseValue.d;
    iter->leading = 32;
    iter->trailing = 32;
    iter->blocksize = 0;

    return (ChunkIter_t *)iter;
}

ChunkResult Compressed_ChunkIteratorGetNext(ChunkIter_t *iter, Sample *sample) {
    return Compressed_ReadNext((Compressed_Iterator *)iter, &sample->timestamp, &sample->value);
}

void Compressed_FreeChunkIterator(ChunkIter_t *iter) {
    free(iter);
}

void Compressed_SaveToRDB(Chunk_t *chunk, struct RedisModuleIO *io) {
    CompressedChunk *compchunk = chunk;

    RedisModule_SaveUnsigned(io, compchunk->size);
    RedisModule_SaveUnsigned(io, compchunk->count);
    RedisModule_SaveUnsigned(io, compchunk->idx);
    RedisModule_SaveUnsigned(io, compchunk->baseValue.u);
    RedisModule_SaveUnsigned(io, compchunk->baseTimestamp);
    RedisModule_SaveUnsigned(io, compchunk->prevTimestamp);
    RedisModule_SaveSigned(io, compchunk->prevTimestampDelta);
    RedisModule_SaveUnsigned(io, compchunk->prevValue.u);
    RedisModule_SaveUnsigned(io, compchunk->prevLeading);
    RedisModule_SaveUnsigned(io, compchunk->prevTrailing);
    RedisModule_SaveStringBuffer(io, (char *)compchunk->data, compchunk->size);
}

void Compressed_LoadFromRDB(Chunk_t **chunk, struct RedisModuleIO *io) {
    CompressedChunk *compchunk = (CompressedChunk *)malloc(sizeof(*compchunk));

    compchunk->size = RedisModule_LoadUnsigned(io);
    compchunk->count = RedisModule_LoadUnsigned(io);
    compchunk->idx = RedisModule_LoadUnsigned(io);
    compchunk->baseValue.u = RedisModule_LoadUnsigned(io);
    compchunk->baseTimestamp = RedisModule_LoadUnsigned(io);
    compchunk->prevTimestamp = RedisModule_LoadUnsigned(io);
    compchunk->prevTimestampDelta = RedisModule_LoadSigned(io);
    compchunk->prevValue.u = RedisModule_LoadUnsigned(io);
    compchunk->prevLeading = RedisModule_LoadUnsigned(io);
    compchunk->prevTrailing = RedisModule_LoadUnsigned(io);

    compchunk->data = (uint64_t *)RedisModule_LoadStringBuffer(io, NULL);
    *chunk = (Chunk_t *)compchunk;
}
