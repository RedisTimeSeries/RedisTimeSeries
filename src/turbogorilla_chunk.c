/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "turbogorilla_chunk.h"

#include "fp.h"
#include "gears_integration.h"

#include <assert.h>
#include "rmutil/alloc.h"
#undef USIZE
#define USIZE 8

Chunk_t *TurboGorilla_NewChunk(size_t size) {
    TurboGorilla_Chunk *newChunk = (TurboGorilla_Chunk *)malloc(sizeof(TurboGorilla_Chunk));
    newChunk->num_samples = 0;
    newChunk->size = size;
    newChunk->timestamps_cpos = 0;
    newChunk->values_cpos = 0;
    newChunk->buffer_in_use = 1;
    newChunk->buffer_timestamps = (uint64_t *)malloc(size / (2 * sizeof(uint64_t)));
    newChunk->buffer_values = (double *)malloc(size / (2 * sizeof(double)));
    newChunk->timestamps = NULL;
    newChunk->values = NULL;
    return newChunk;
}

void TurboGorilla_FreeChunk(Chunk_t *chunk) {
    TurboGorilla_Chunk *g_chunk = (TurboGorilla_Chunk *)chunk;
    if (g_chunk->timestamps)
        free(g_chunk->timestamps);
    if (g_chunk->values)
        free(g_chunk->values);
    if (g_chunk->buffer_timestamps)
        free(g_chunk->buffer_timestamps);
    if (g_chunk->buffer_values)
        free(g_chunk->buffer_values);
    free(chunk);
}

/**
 * TODO: describe me
 * @param chunk
 * @return
 */
Chunk_t *TurboGorilla_SplitChunk(Chunk_t *chunk) {
    // TurboGorilla_Chunk *curChunk = (TurboGorilla_Chunk *)chunk;
    // size_t split = curChunk->num_samples / 2;
    // size_t curNumSamples = curChunk->num_samples - split;

    // // create chunk and copy samples
    // TurboGorilla_Chunk *newChunk = TurboGorilla_NewChunk(split * SAMPLE_SIZE);
    // for (size_t i = 0; i < split; ++i) {
    //     Sample *sample = &curChunk->samples[curNumSamples + i];
    //     TurboGorilla_AddSample(newChunk, sample);
    // }

    // // update current chunk
    // curChunk->num_samples = curNumSamples;
    // curChunk->size = curNumSamples * SAMPLE_SIZE;
    // curChunk->samples = realloc(curChunk->samples, curChunk->size);

    return chunk;
}

static inline int TurboGorilla_IsChunkFull(TurboGorilla_Chunk *chunk) {
    return (chunk->num_samples * 16) >= chunk->size;
}

u_int64_t TurboGorilla_NumOfSample(Chunk_t *chunk) {
    return ((TurboGorilla_Chunk *)chunk)->num_samples;
}

timestamp_t TurboGorilla_GetLastTimestamp(Chunk_t *chunk) {
    TurboGorilla_Chunk *g_chunk = (TurboGorilla_Chunk *)chunk;
    if (g_chunk->num_samples == 0) {
        return -1;
    }
    return g_chunk->end_timestamp;
}

timestamp_t TurboGorilla_GetFirstTimestamp(Chunk_t *chunk) {
    TurboGorilla_Chunk *g_chunk = (TurboGorilla_Chunk *)chunk;
    if (g_chunk->num_samples == 0) {
        return -1;
    }
    return g_chunk->start_timestamp;
}

ChunkResult TurboGorilla_AddSample(Chunk_t *chunk, Sample *sample) {
    TurboGorilla_Chunk *g_chunk = (TurboGorilla_Chunk *)chunk;
    if (TurboGorilla_IsChunkFull(g_chunk)) {
        return CR_END;
    }
    if (g_chunk->num_samples == 0) {
        // initialize base_timestamp
        g_chunk->start_timestamp = sample->timestamp;
        // g_chunk->timestamps_startptr =
    }
    g_chunk->end_timestamp = sample->timestamp;
    g_chunk->buffer_timestamps[g_chunk->num_samples] = sample->timestamp;
    g_chunk->buffer_values[g_chunk->num_samples] = sample->value;
    g_chunk->num_samples++;
    if (TurboGorilla_IsChunkFull(g_chunk)) {
        g_chunk->timestamps = malloc(g_chunk->size / 2 * sizeof(unsigned char));
        g_chunk->values = malloc(g_chunk->size / 2 * sizeof(unsigned char));
        g_chunk->timestamps_cpos +=
            fpgenc64(g_chunk->buffer_timestamps, g_chunk->num_samples, g_chunk->timestamps, 0);
        g_chunk->values_cpos +=
            fpgenc64(g_chunk->buffer_values, g_chunk->num_samples, g_chunk->values, 0);
        printf("enconded size timestamps_cpos %d values_cpos %d\n",
               g_chunk->timestamps_cpos,
               g_chunk->values_cpos);
        free(g_chunk->buffer_timestamps);
        free(g_chunk->buffer_values);
        g_chunk->buffer_in_use = 0;
    }
    return CR_OK;
}

/**
 * TODO: describe me
 * @param uCtx
 * @param size
 * @return
 */
ChunkResult TurboGorilla_UpsertSample(UpsertCtx *uCtx, int *size, DuplicatePolicy duplicatePolicy) {
    return CR_ERR;
}

size_t TurboGorilla_DelRange(Chunk_t *chunk, timestamp_t startTs, timestamp_t endTs) {
    return 0;
}

ChunkIter_t *TurboGorilla_NewChunkIterator(Chunk_t *chunk,
                                           int options,
                                           ChunkIterFuncs *retChunkIterClass) {
    TurboGorilla_ChunkIterator *iter =
        (TurboGorilla_ChunkIterator *)calloc(1, sizeof(TurboGorilla_ChunkIterator));
    iter->chunk = chunk;
    iter->options = options;
    if (options & CHUNK_ITER_OP_REVERSE) { // iterate from last to first
        iter->currentIndex = iter->chunk->num_samples - 1;
    } else { // iterate from first to last
        iter->currentIndex = 0;
    }
    if (iter->chunk->num_samples > 0) {
        iter->timestamps = malloc((iter->chunk->num_samples) * sizeof(uint64_t));
        iter->values = malloc((iter->chunk->num_samples) * sizeof(double));
        if (iter->chunk->buffer_in_use == 1) {
            memcpy(iter->timestamps,
                   iter->chunk->buffer_timestamps,
                   (iter->chunk->num_samples) * sizeof(uint64_t));
            memcpy(iter->values,
                   iter->chunk->buffer_values,
                   (iter->chunk->num_samples) * sizeof(double));
        } else {
            fpgdec64(iter->chunk->timestamps, iter->chunk->num_samples, iter->timestamps, 0);
            fpgdec64(iter->chunk->values, iter->chunk->num_samples, iter->values, 0);
        }
    }
    if (retChunkIterClass != NULL) {
        *retChunkIterClass = *GetChunkIteratorClass(CHUNK_COMPRESSED_TURBOGORILLA);
    }

    return (ChunkIter_t *)iter;
}

ChunkResult TurboGorilla_ChunkIteratorGetNext(ChunkIter_t *iterator, Sample *sample) {
    TurboGorilla_ChunkIterator *iter = (TurboGorilla_ChunkIterator *)iterator;
    if (iter->currentIndex < iter->chunk->num_samples) {
        (*sample).timestamp = iter->timestamps[iter->currentIndex];
        (*sample).value = iter->values[iter->currentIndex];
        iter->currentIndex++;
        return CR_OK;
    } else {
        return CR_END;
    }
}

ChunkResult TurboGorilla_ChunkIteratorGetPrev(ChunkIter_t *iterator, Sample *sample) {
    TurboGorilla_ChunkIterator *iter = (TurboGorilla_ChunkIterator *)iterator;
    if (iter->currentIndex >= 0) {
        (*sample).timestamp = iter->timestamps[iter->currentIndex];
        (*sample).value = iter->values[iter->currentIndex];
        iter->currentIndex--;
        return CR_OK;
    } else {
        return CR_END;
    }
}

void TurboGorilla_FreeChunkIterator(ChunkIter_t *iterator) {
    TurboGorilla_ChunkIterator *iter = (TurboGorilla_ChunkIterator *)iterator;
    if (iter->options & CHUNK_ITER_OP_FREE_CHUNK) {
        TurboGorilla_FreeChunk(iter->chunk);
    }
    free(iter);
}

size_t TurboGorilla_GetChunkSize(Chunk_t *chunk, bool includeStruct) {
    TurboGorilla_Chunk *g_chunck = chunk;
    size_t size = g_chunck->buffer_in_use ? g_chunck->size
                                          : g_chunck->timestamps_cpos + g_chunck->values_cpos;
    size += includeStruct ? sizeof(*g_chunck) : 0;
    return size;
}

typedef void (*SaveUnsignedFunc)(void *, uint64_t);
typedef void (*SaveStringBufferFunc)(void *, const char *str, size_t len);
typedef uint64_t (*ReadUnsignedFunc)(void *);
typedef char *(*ReadStringBufferFunc)(void *, size_t *);

static void TurboGorilla_GenericSerialize(Chunk_t *chunk,
                                          void *ctx,
                                          SaveUnsignedFunc saveUnsigned,
                                          SaveStringBufferFunc saveString) {
    TurboGorilla_Chunk *g_chunck = chunk;
    saveUnsigned(ctx, g_chunck->start_timestamp);
    saveUnsigned(ctx, g_chunck->end_timestamp);
    saveUnsigned(ctx, g_chunck->num_samples);
    saveUnsigned(ctx, g_chunck->size);
    saveUnsigned(ctx, g_chunck->buffer_in_use);
    if (g_chunck->buffer_in_use == 1) {
        saveString(ctx, g_chunck->buffer_timestamps, g_chunck->size / 2);
        saveString(ctx, g_chunck->buffer_values, g_chunck->size / 2);
    } else {
        saveUnsigned(ctx, g_chunck->timestamps_cpos);
        saveUnsigned(ctx, g_chunck->values_cpos);
        saveString(ctx, g_chunck->timestamps, g_chunck->timestamps_cpos / 8);
        saveString(ctx, g_chunck->values, g_chunck->values_cpos / 8);
    }
}

static void TurboGorilla_Deserialize(Chunk_t **chunk,
                                     void *ctx,
                                     ReadUnsignedFunc readUnsigned,
                                     ReadStringBufferFunc readStringBuffer) {
    TurboGorilla_Chunk *g_chunck = (TurboGorilla_Chunk *)malloc(sizeof(*g_chunck));
    g_chunck->start_timestamp = readUnsigned(ctx);
    g_chunck->end_timestamp = readUnsigned(ctx);
    g_chunck->num_samples = readUnsigned(ctx);
    g_chunck->size = readUnsigned(ctx);
    g_chunck->buffer_in_use = readUnsigned(ctx);
    if (g_chunck->buffer_in_use == 1) {
        size_t string_buffer_size;
        g_chunck->buffer_timestamps = (uint64_t *)readStringBuffer(ctx, &string_buffer_size);
        g_chunck->buffer_values = (double *)readStringBuffer(ctx, &string_buffer_size);
        g_chunck->timestamps_cpos = 0;
        g_chunck->values_cpos = 0;
    } else {
        g_chunck->timestamps_cpos = readUnsigned(ctx);
        g_chunck->values_cpos = readUnsigned(ctx);
        size_t string_buffer_size;
        g_chunck->timestamps = (unsigned char *)readStringBuffer(ctx, &string_buffer_size);
        g_chunck->values = (unsigned char *)readStringBuffer(ctx, &string_buffer_size);
    }
    *chunk = (Chunk_t *)g_chunck;
}

void TurboGorilla_SaveToRDB(Chunk_t *chunk, struct RedisModuleIO *io) {
    TurboGorilla_GenericSerialize(chunk,
                                  io,
                                  (SaveUnsignedFunc)RedisModule_SaveUnsigned,
                                  (SaveStringBufferFunc)RedisModule_SaveStringBuffer);
}

void TurboGorilla_LoadFromRDB(Chunk_t **chunk, struct RedisModuleIO *io) {
    TurboGorilla_Deserialize(chunk,
                             io,
                             (ReadUnsignedFunc)RedisModule_LoadUnsigned,
                             (ReadStringBufferFunc)RedisModule_LoadStringBuffer);
}

void TurboGorilla_GearsSerialize(Chunk_t *chunk, Gears_BufferWriter *bw) {}

void TurboGorilla_GearsDeserialize(Chunk_t *chunk, Gears_BufferReader *br) {}
