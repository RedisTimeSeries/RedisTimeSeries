/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "chunk.h"

#include "libmr_integration.h"

#include "rmutil/alloc.h"

Chunk_t *Uncompressed_NewChunk(size_t size) {
    Chunk *newChunk = (Chunk *)malloc(sizeof(Chunk));
    newChunk->base_timestamp = 0;
    newChunk->num_samples = 0;
    newChunk->size = size;
    newChunk->samples = (Sample *)malloc(size);
#ifdef DEBUG
    memset(newChunk->samples, 0, size);
#endif

    return newChunk;
}

void Uncompressed_FreeChunk(Chunk_t *chunk) {
    if (((Chunk *)chunk)->samples) {
        free(((Chunk *)chunk)->samples);
    }
    free(chunk);
}

/**
 * TODO: describe me
 * @param chunk
 * @return
 */
Chunk_t *Uncompressed_SplitChunk(Chunk_t *chunk) {
    Chunk *curChunk = (Chunk *)chunk;
    size_t split = curChunk->num_samples / 2;
    size_t curNumSamples = curChunk->num_samples - split;

    // create chunk and copy samples
    Chunk *newChunk = Uncompressed_NewChunk(split * SAMPLE_SIZE);
    for (size_t i = 0; i < split; ++i) {
        Sample *sample = &curChunk->samples[curNumSamples + i];
        Uncompressed_AddSample(newChunk, sample);
    }

    // update current chunk
    curChunk->num_samples = curNumSamples;
    curChunk->size = curNumSamples * SAMPLE_SIZE;
    curChunk->samples = realloc(curChunk->samples, curChunk->size);

    return newChunk;
}

/**
 * Deep copy of src chunk to dst
 * @param src: src chunk
 * @return the copied chunk
 */
Chunk_t *Uncompressed_CloneChunk(const Chunk_t *src) {
    const Chunk *_src = src;
    Chunk *dst = (Chunk *)malloc(sizeof(Chunk));
    memcpy(dst, _src, sizeof(Chunk));
    dst->samples = (Sample *)malloc(dst->size);
    memcpy(dst->samples, _src->samples, dst->size);
    return dst;
}

static int IsChunkFull(Chunk *chunk) {
    return chunk->num_samples == chunk->size / SAMPLE_SIZE;
}

u_int64_t Uncompressed_NumOfSample(Chunk_t *chunk) {
    return ((Chunk *)chunk)->num_samples;
}

static Sample *ChunkGetSample(Chunk *chunk, int index) {
    return &chunk->samples[index];
}

timestamp_t Uncompressed_GetLastTimestamp(Chunk_t *chunk) {
    if (unlikely(((Chunk *)chunk)->num_samples == 0)) { // empty chunks are being removed
        RedisModule_Log(mr_staticCtx, "error", "Trying to get the last timestamp of empty chunk");
    }
    return ChunkGetSample(chunk, ((Chunk *)chunk)->num_samples - 1)->timestamp;
}

double Uncompressed_GetLastValue(Chunk_t *chunk) {
    if (unlikely(((Chunk *)chunk)->num_samples == 0)) { // empty chunks are being removed
        RedisModule_Log(mr_staticCtx, "error", "Trying to get the last value of empty chunk");
    }
    return ChunkGetSample(chunk, ((Chunk *)chunk)->num_samples - 1)->value;
}

timestamp_t Uncompressed_GetFirstTimestamp(Chunk_t *chunk) {
    if (((Chunk *)chunk)->num_samples == 0) {
        // When the chunk is empty it first TS is used for the chunk dict key
        // Only the first chunk can be empty since we delete empty chunks
        return 0;
    }
    return ChunkGetSample(chunk, 0)->timestamp;
}

ChunkResult Uncompressed_AddSample(Chunk_t *chunk, Sample *sample) {
    Chunk *regChunk = (Chunk *)chunk;
    if (IsChunkFull(regChunk)) {
        return CR_END;
    }

    if (Uncompressed_NumOfSample(regChunk) == 0) {
        // initialize base_timestamp
        regChunk->base_timestamp = sample->timestamp;
    }

    regChunk->samples[regChunk->num_samples] = *sample;
    regChunk->num_samples++;

    return CR_OK;
}

/**
 * TODO: describe me
 * @param chunk
 * @param idx
 * @param sample
 */
static void upsertChunk(Chunk *chunk, size_t idx, Sample *sample) {
    if (chunk->num_samples == chunk->size / SAMPLE_SIZE) {
        chunk->size += sizeof(Sample);
        chunk->samples = realloc(chunk->samples, chunk->size);
    }
    if (idx < chunk->num_samples) { // sample is not last
        memmove(&chunk->samples[idx + 1],
                &chunk->samples[idx],
                (chunk->num_samples - idx) * sizeof(Sample));
    }
    chunk->samples[idx] = *sample;
    chunk->num_samples++;
}

/**
 * TODO: describe me
 * @param uCtx
 * @param size
 * @return
 */
ChunkResult Uncompressed_UpsertSample(UpsertCtx *uCtx, int *size, DuplicatePolicy duplicatePolicy) {
    *size = 0;
    Chunk *regChunk = (Chunk *)uCtx->inChunk;
    timestamp_t ts = uCtx->sample.timestamp;
    short numSamples = regChunk->num_samples;
    // find sample location
    size_t i = 0;
    Sample *sample = NULL;
    for (; i < numSamples; ++i) {
        sample = ChunkGetSample(regChunk, i);
        if (ts <= sample->timestamp) {
            break;
        }
    }
    // update value in case timestamp exists
    if (sample != NULL && ts == sample->timestamp) {
        ChunkResult cr = handleDuplicateSample(duplicatePolicy, *sample, &uCtx->sample);
        if (cr != CR_OK) {
            return CR_ERR;
        }
        regChunk->samples[i].value = uCtx->sample.value;
        return CR_OK;
    }

    if (i == 0) {
        regChunk->base_timestamp = ts;
    }

    upsertChunk(regChunk, i, &uCtx->sample);
    *size = 1;
    return CR_OK;
}

size_t Uncompressed_DelRange(Chunk_t *chunk, timestamp_t startTs, timestamp_t endTs) {
    Chunk *regChunk = (Chunk *)chunk;
    Sample *newSamples = (Sample *)malloc(regChunk->size);
    size_t i = 0;
    size_t new_count = 0;
    for (; i < regChunk->num_samples; ++i) {
        if (regChunk->samples[i].timestamp >= startTs && regChunk->samples[i].timestamp <= endTs) {
            continue;
        }
        newSamples[new_count++] = regChunk->samples[i];
    }
    size_t deleted_count = regChunk->num_samples - new_count;
    free(regChunk->samples);
    regChunk->samples = newSamples;
    regChunk->num_samples = new_count;
    regChunk->base_timestamp = newSamples[0].timestamp;
    return deleted_count;
}

#define __array_reverse_inplace(arr, len)                                                          \
    __extension__({                                                                                \
        const size_t ei = len - 1;                                                                 \
        __typeof__(*arr) tmp;                                                                      \
        for (size_t i = 0; i < len / 2; ++i) {                                                     \
            tmp = arr[i];                                                                          \
            arr[i] = arr[ei - i];                                                                  \
            arr[ei - i] = tmp;                                                                     \
        }                                                                                          \
    })

void reverseEnrichedChunk(EnrichedChunk *enrichedChunk) {
    __array_reverse_inplace(enrichedChunk->samples.timestamps, enrichedChunk->samples.num_samples);
    __array_reverse_inplace(enrichedChunk->samples.values, enrichedChunk->samples.num_samples);
    enrichedChunk->rev = true;
}

// TODO: can be optimized further using binary search
void Uncompressed_ProcessChunk(const Chunk_t *chunk,
                               uint64_t start,
                               uint64_t end,
                               EnrichedChunk *enrichedChunk,
                               bool reverse) {
    const Chunk *_chunk = chunk;
    ResetEnrichedChunk(enrichedChunk);
    if (unlikely(!_chunk || _chunk->num_samples == 0 || end < start ||
                 _chunk->base_timestamp > end ||
                 _chunk->samples[_chunk->num_samples - 1].timestamp < start)) {
        return;
    }

    size_t si = _chunk->num_samples, ei = _chunk->num_samples - 1, i = 0;

    // find start index
    for (; i < _chunk->num_samples; i++) {
        if (_chunk->samples[i].timestamp >= start) {
            si = i;
            break;
        }
    }

    if (si == _chunk->num_samples) { // all TS are smaller than start
        return;
    }

    // find end index
    for (; i < _chunk->num_samples; i++) {
        if (_chunk->samples[i].timestamp > end) {
            ei = i - 1;
            break;
        }
    }

    enrichedChunk->samples.num_samples = ei - si + 1;
    if (enrichedChunk->samples.num_samples == 0) {
        return;
    }

    if (unlikely(reverse)) {
        for (i = 0; i < enrichedChunk->samples.num_samples; ++i) {
            enrichedChunk->samples.timestamps[i] = _chunk->samples[ei - i].timestamp;
            enrichedChunk->samples.values[i] = _chunk->samples[ei - i].value;
        }
        enrichedChunk->rev = true;
    } else {
        for (i = 0; i < enrichedChunk->samples.num_samples;
             ++i) { // use memcpy once chunk becomes columned
            enrichedChunk->samples.timestamps[i] = _chunk->samples[i + si].timestamp;
            enrichedChunk->samples.values[i] = _chunk->samples[i + si].value;
        }
        enrichedChunk->rev = false;
    }
    return;
}

size_t Uncompressed_GetChunkSize(Chunk_t *chunk, bool includeStruct) {
    Chunk *uncompChunk = chunk;
    size_t size = uncompChunk->size;
    size += includeStruct ? sizeof(*uncompChunk) : 0;
    return size;
}

typedef void (*SaveUnsignedFunc)(void *, uint64_t);
typedef void (*SaveStringBufferFunc)(void *, const char *str, size_t len);

static void Uncompressed_GenericSerialize(Chunk_t *chunk,
                                          void *ctx,
                                          SaveUnsignedFunc saveUnsigned,
                                          SaveStringBufferFunc saveStringBuffer) {
    Chunk *uncompchunk = chunk;

    saveUnsigned(ctx, uncompchunk->base_timestamp);
    saveUnsigned(ctx, uncompchunk->num_samples);
    saveUnsigned(ctx, uncompchunk->size);

    saveStringBuffer(ctx, (char *)uncompchunk->samples, uncompchunk->size);
}

#define UNCOMPRESSED_DESERIALIZE(chunk, ctx, load_unsigned, loadStringBuffer, ...)                 \
    do {                                                                                           \
        Chunk *uncompchunk = (Chunk *)calloc(1, sizeof(*uncompchunk));                             \
                                                                                                   \
        uncompchunk->base_timestamp = load_unsigned(ctx, ##__VA_ARGS__);                           \
        uncompchunk->num_samples = load_unsigned(ctx, ##__VA_ARGS__);                              \
        uncompchunk->size = load_unsigned(ctx, ##__VA_ARGS__);                                     \
        size_t string_buffer_size;                                                                 \
        uncompchunk->samples =                                                                     \
            (Sample *)loadStringBuffer(ctx, &string_buffer_size, ##__VA_ARGS__);                   \
        *chunk = (Chunk_t *)uncompchunk;                                                           \
        return TSDB_OK;                                                                            \
                                                                                                   \
err:                                                                                               \
        __attribute__((cold, unused));                                                             \
        *chunk = NULL;                                                                             \
        Uncompressed_FreeChunk(uncompchunk);                                                       \
        return TSDB_ERROR;                                                                         \
    } while (0)

void Uncompressed_SaveToRDB(Chunk_t *chunk, struct RedisModuleIO *io) {
    Uncompressed_GenericSerialize(chunk,
                                  io,
                                  (SaveUnsignedFunc)RedisModule_SaveUnsigned,
                                  (SaveStringBufferFunc)RedisModule_SaveStringBuffer);
}

int Uncompressed_LoadFromRDB(Chunk_t **chunk, struct RedisModuleIO *io) {
    UNCOMPRESSED_DESERIALIZE(chunk, io, LoadUnsigned_IOError, LoadStringBuffer_IOError, goto err);
}

void Uncompressed_MRSerialize(Chunk_t *chunk, WriteSerializationCtx *sctx) {
    Uncompressed_GenericSerialize(chunk,
                                  sctx,
                                  (SaveUnsignedFunc)MR_SerializationCtxWriteLongLongWrapper,
                                  (SaveStringBufferFunc)MR_SerializationCtxWriteBufferWrapper);
}

int Uncompressed_MRDeserialize(Chunk_t **chunk, ReaderSerializationCtx *sctx) {
    UNCOMPRESSED_DESERIALIZE(
        chunk, sctx, MR_SerializationCtxReadeLongLongWrapper, MR_ownedBufferFrom);
}
