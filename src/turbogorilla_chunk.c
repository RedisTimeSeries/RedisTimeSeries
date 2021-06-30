/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "turbogorilla_chunk.h"

#include "gears_integration.h"

#include "rmutil/alloc.h"

struct TurboGorilla_Chunk
{
    timestamp_t start_ts;
    unsigned int num_samples;
    size_t size;
    bool buffer_in_use;
    u_int64_t *samples_ts;
    double *samples_values;
};

Chunk_t *TurboGorilla_NewChunk(size_t size) {
    TurboGorilla_Chunk *newChunk = (TurboGorilla_Chunk *)malloc(sizeof(TurboGorilla_Chunk));
    newChunk->num_samples = 0;
    newChunk->size = size;
    const size_t array_size = size / 2;
    newChunk->samples_ts = (u_int64_t *)malloc(array_size);
    newChunk->samples_values = (double *)malloc(array_size);
    newChunk->buffer_in_use = true;
#ifdef DEBUG
    memset(newChunk->samples_ts, 0, array_size);
    memset(newChunk->samples_values, 0, array_size);
#endif

    return newChunk;
}

void TurboGorilla_FreeChunk(Chunk_t *chunk) {
    TurboGorilla_Chunk *curChunk = (TurboGorilla_Chunk *)chunk;
    free(curChunk->samples_ts);
    free(curChunk->samples_values);
    free(curChunk);
}

/**
 * Split the chunk in half, returning a new chunk with the right-side of the current chunk
 * The input chunk is trimmed to retain the left-most part
 * @param chunk
 * @return new chunk with the right-most splited in half samples
 */
Chunk_t *TurboGorilla_SplitChunk(Chunk_t *chunk) {
    TurboGorilla_Chunk *curChunk = (TurboGorilla_Chunk *)chunk;
    const size_t newChunkNumSamples = curChunk->num_samples / 2;
    const size_t currentChunkNumSamples = curChunk->num_samples - newChunkNumSamples;

    // create chunk and copy samples
    TurboGorilla_Chunk *newChunk = TurboGorilla_NewChunk(newChunkNumSamples * SAMPLE_SIZE);
    for (size_t i = 0; i < newChunkNumSamples; i++) {
        const u_int64_t ts = curChunk->samples_ts[currentChunkNumSamples + i];
        const double v = curChunk->samples_values[currentChunkNumSamples + i];
        TurboGorilla_AddSampleOptimized(newChunk, ts, v);
    }

    // update current chunk
    const size_t old_ts_size = (currentChunkNumSamples * sizeof(u_int64_t));
    const size_t old_values_size = (currentChunkNumSamples * sizeof(double));
    curChunk->num_samples = currentChunkNumSamples;
    curChunk->size = currentChunkNumSamples * SAMPLE_SIZE;
    curChunk->samples_ts = realloc(curChunk->samples_ts, old_ts_size);
    curChunk->samples_values = realloc(curChunk->samples_values, old_values_size);
    return newChunk;
}

static int IsChunkFull(TurboGorilla_Chunk *chunk) {
    return chunk->num_samples == chunk->size / SAMPLE_SIZE;
}

u_int64_t TurboGorilla_NumOfSample(Chunk_t *chunk) {
    return ((TurboGorilla_Chunk *)chunk)->num_samples;
}

timestamp_t TurboGorilla_GetLastTimestamp(Chunk_t *chunk) {
    TurboGorilla_Chunk *uChunk = (TurboGorilla_Chunk *)chunk;
    if (uChunk->num_samples == 0) {
        return -1;
    }
    return uChunk->samples_ts[uChunk->num_samples - 1];
}

timestamp_t TurboGorilla_GetFirstTimestamp(Chunk_t *chunk) {
    TurboGorilla_Chunk *uChunk = (TurboGorilla_Chunk *)chunk;
    if (uChunk->num_samples == 0) {
        return -1;
    }
    return uChunk->samples_ts[0];
}

int TurboGorilla_GetSampleValueAtPos(Chunk_t *chunk, size_t pos, double *value) {
    int result = CR_ERR;
    TurboGorilla_Chunk *uChunk = (TurboGorilla_Chunk *)chunk;
    if (uChunk->num_samples > pos) {
        *value = uChunk->samples_values[pos];
        result = CR_OK;
    }

    return result;
}
int TurboGorilla_GetSampleTimestampAtPos(Chunk_t *chunk, size_t pos, u_int64_t *timestamp) {
    int result = CR_ERR;
    TurboGorilla_Chunk *uChunk = (TurboGorilla_Chunk *)chunk;
    if (uChunk->num_samples > pos) {
        *timestamp = uChunk->samples_ts[pos];
        result = CR_OK;
    }
    return result;
}

ChunkResult TurboGorilla_AddSampleOptimized(Chunk_t *chunk, u_int64_t timestamp, double value) {
    TurboGorilla_Chunk *regChunk = (TurboGorilla_Chunk *)chunk;
    if (IsChunkFull(regChunk)) {
        return CR_END;
    }

    if (regChunk->num_samples == 0) {
        // initialize start_ts
        regChunk->start_ts = timestamp;
    }
    const size_t pos = regChunk->num_samples;
    regChunk->samples_ts[pos] = timestamp;
    regChunk->samples_values[pos] = value;
    regChunk->num_samples++;
    return CR_OK;
}

ChunkResult TurboGorilla_AddSample(Chunk_t *chunk, Sample *sample) {
    return TurboGorilla_AddSampleOptimized(chunk, sample->timestamp, sample->value);
}

/**
 * upsertTurboGorilla_Chunk will insert the sample in the chunk no matter the position of insertion.
 * In the case of the chunk being at max capacity we allocate space for one more sample
 * @param chunk
 * @param idx
 * @param sample
 */
static void upsertChunk(TurboGorilla_Chunk *chunk, size_t idx, u_int64_t ts, double value) {
    if (IsChunkFull(chunk)) {
        chunk->size += SAMPLE_SIZE;
        const size_t new_ts_size = chunk->size / 2 + sizeof(u_int64_t);
        const size_t new_values_size = chunk->size / 2 + sizeof(double);
        chunk->samples_ts = realloc(chunk->samples_ts, new_ts_size);
        chunk->samples_values = realloc(chunk->samples_values, new_values_size);
    }
    if (idx < chunk->num_samples) { // sample is not last
        memmove(&chunk->samples_ts[idx + 1],
                &chunk->samples_ts[idx],
                (chunk->num_samples - idx) * sizeof(u_int64_t));
        memmove(&chunk->samples_values[idx + 1],
                &chunk->samples_values[idx],
                (chunk->num_samples - idx) * sizeof(double));
    }
    chunk->samples_ts[idx] = ts;
    chunk->samples_values[idx] = value;
    chunk->num_samples++;
}

/**
 * TODO: describe me
 * @param uCtx
 * @param size
 * @return
 */
ChunkResult TurboGorilla_UpsertSample(UpsertCtx *uCtx, int *size, DuplicatePolicy duplicatePolicy) {
    *size = 0;
    TurboGorilla_Chunk *regChunk = (TurboGorilla_Chunk *)uCtx->inChunk;
    const u_int64_t ts = uCtx->sample.timestamp;
    const u_int64_t *ts_array = regChunk->samples_ts;
    const size_t numSamples = regChunk->num_samples;
    size_t sample_pos = 0;
    bool found = false;

    // find the number of elements in the array that are less than the timestamp you search for
    for (int i = 0; i < numSamples; i++)
        sample_pos += (ts_array[i] < ts);

    // check if timestamp right after is the one we're searching for
    if (sample_pos < numSamples && ts_array[sample_pos] == ts)
        found = true;

    // update value in case timestamp exists
    if (found == true) {
        ChunkResult cr = handleDuplicateSample(
            duplicatePolicy, regChunk->samples_values[sample_pos], &(uCtx->sample.value));
        if (cr != CR_OK) {
            return CR_ERR;
        }
        regChunk->samples_values[sample_pos] = uCtx->sample.value;
        return CR_OK;
    }

    if (sample_pos == 0) {
        regChunk->start_ts = ts;
    }

    upsertChunk(regChunk, sample_pos, ts, uCtx->sample.value);
    *size = 1;
    return CR_OK;
}

size_t TurboGorilla_DelRange(Chunk_t *chunk, timestamp_t startTs, timestamp_t endTs) {
    TurboGorilla_Chunk *regChunk = (TurboGorilla_Chunk *)chunk;
    const u_int64_t *timestamps = regChunk->samples_ts;
    const double *values = regChunk->samples_values;

    // create two new arrays and copy samples that don't match the delete range
    // TODO: use memove that should be much faster
    const size_t array_size = regChunk->size / 2;
    u_int64_t *new_samples_ts = (u_int64_t *)malloc(array_size);
    double *new_samples_values = (double *)malloc(array_size);
    size_t i = 0;
    size_t new_count = 0;
    for (; i < regChunk->num_samples; ++i) {
        if (timestamps[i] >= startTs && timestamps[i] <= endTs) {
            continue;
        }
        new_samples_ts[new_count] = timestamps[i];
        new_samples_values[new_count] = values[i];
        new_count++;
    }
    size_t deleted_count = regChunk->num_samples - new_count;
    free(regChunk->samples_ts);
    free(regChunk->samples_values);
    regChunk->samples_ts = new_samples_ts;
    regChunk->samples_values = new_samples_values;
    regChunk->num_samples = new_count;
    regChunk->start_ts = new_samples_ts[0];
    return deleted_count;
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

    if (retChunkIterClass != NULL) {
        *retChunkIterClass = *GetChunkIteratorClass(CHUNK_COMPRESSED_TURBOGORILLA);
    }

    return (ChunkIter_t *)iter;
}

ChunkResult TurboGorilla_ChunkIteratorGetNext(ChunkIter_t *iterator, Sample *sample) {
    TurboGorilla_ChunkIterator *iter = (TurboGorilla_ChunkIterator *)iterator;
    if (iter->currentIndex < iter->chunk->num_samples) {
        sample->value = iter->chunk->samples_values[iter->currentIndex];
        sample->timestamp = iter->chunk->samples_ts[iter->currentIndex];
        iter->currentIndex++;
        return CR_OK;
    } else {
        return CR_END;
    }
}

ChunkResult TurboGorilla_ChunkIteratorGetPrev(ChunkIter_t *iterator, Sample *sample) {
    TurboGorilla_ChunkIterator *iter = (TurboGorilla_ChunkIterator *)iterator;
    if (iter->currentIndex >= 0) {
        sample->value = iter->chunk->samples_values[iter->currentIndex];
        sample->timestamp = iter->chunk->samples_ts[iter->currentIndex];
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
    TurboGorilla_Chunk *uncompChunk = chunk;
    size_t size = uncompChunk->size;
    size += includeStruct ? sizeof(*uncompChunk) : 0;
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
    TurboGorilla_Chunk *uncompchunk = chunk;
    saveUnsigned(ctx, uncompchunk->size);
    saveUnsigned(ctx, uncompchunk->start_ts);
    saveUnsigned(ctx, uncompchunk->num_samples);
    saveUnsigned(ctx, uncompchunk->buffer_in_use);
    saveString(ctx, (char *)uncompchunk->samples_ts, uncompchunk->num_samples * sizeof(uint64_t));
    saveString(ctx, (char *)uncompchunk->samples_values, uncompchunk->num_samples * sizeof(double));
}

static void TurboGorilla_Deserialize(Chunk_t **chunk,
                                     void *ctx,
                                     ReadUnsignedFunc readUnsigned,
                                     ReadStringBufferFunc readStringBuffer) {
    const size_t size = readUnsigned(ctx);
    TurboGorilla_Chunk *uncompchunk = TurboGorilla_NewChunk(size);
    uncompchunk->start_ts = readUnsigned(ctx);
    uncompchunk->num_samples = readUnsigned(ctx);
    uncompchunk->buffer_in_use = readUnsigned(ctx);
    size_t loadsize;
    uncompchunk->samples_ts = (uint64_t *)readStringBuffer(ctx, &loadsize);
    uncompchunk->samples_values = (double *)readStringBuffer(ctx, &loadsize);
    *chunk = (Chunk_t *)uncompchunk;
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
