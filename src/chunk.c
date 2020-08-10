/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "chunk.h"

#include "rmutil/alloc.h"

Chunk_t *Uncompressed_NewChunk(size_t sampleCount) {
    Chunk *newChunk = (Chunk *)malloc(sizeof(Chunk));
    newChunk->num_samples = 0;
    newChunk->max_samples = sampleCount;
    newChunk->samples = (Sample *)malloc(sizeof(Sample) * sampleCount);

    return newChunk;
}

void Uncompressed_FreeChunk(Chunk_t *chunk) {
    free(((Chunk *)chunk)->samples);
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
    Chunk *newChunk = Uncompressed_NewChunk(split);
    for (size_t i = 0; i < split; ++i) {
        Sample *sample = &curChunk->samples[curNumSamples + i];
        Uncompressed_AddSample(newChunk, sample);
    }

    // update current chunk
    curChunk->max_samples = curChunk->num_samples = curNumSamples;
    curChunk->samples = realloc(curChunk->samples, curNumSamples * sizeof(Sample));

    return newChunk;
}

static int IsChunkFull(Chunk *chunk) {
    return chunk->num_samples == chunk->max_samples;
}

u_int64_t Uncompressed_NumOfSample(Chunk_t *chunk) {
    return ((Chunk *)chunk)->num_samples;
}

static Sample *ChunkGetSample(Chunk *chunk, int index) {
    return &chunk->samples[index];
}

timestamp_t Uncompressed_GetLastTimestamp(Chunk_t *chunk) {
    if (((Chunk *)chunk)->num_samples == 0) {
        return -1;
    }
    return ChunkGetSample(chunk, ((Chunk *)chunk)->num_samples - 1)->timestamp;
}

timestamp_t Uncompressed_GetFirstTimestamp(Chunk_t *chunk) {
    if (((Chunk *)chunk)->num_samples == 0) {
        return -1;
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
    if (chunk->num_samples == chunk->max_samples) {
        chunk->samples = realloc(chunk->samples, ++chunk->max_samples * sizeof(Sample));
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
ChunkResult Uncompressed_UpsertSample(UpsertCtx *uCtx, int *size) {
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
    if (ts == sample->timestamp) {
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

ChunkIter_t *Uncompressed_NewChunkIterator(Chunk_t *chunk, bool rev) {
    ChunkIterator *iter = (ChunkIterator *)calloc(1, sizeof(ChunkIterator));
    iter->chunk = chunk;
    if (rev == false) { // iterate from first to last
        iter->currentIndex = 0;
    } else { // iterate from last to first
        iter->currentIndex = iter->chunk->num_samples - 1;
    }
    return (ChunkIter_t *)iter;
}

ChunkResult Uncompressed_ChunkIteratorGetNext(ChunkIter_t *iterator, Sample *sample) {
    ChunkIterator *iter = (ChunkIterator *)iterator;
    if (iter->currentIndex < iter->chunk->num_samples) {
        *sample = *ChunkGetSample(iter->chunk, iter->currentIndex);
        iter->currentIndex++;
        return CR_OK;
    } else {
        return CR_END;
    }
}

ChunkResult Uncompressed_ChunkIteratorGetPrev(ChunkIter_t *iterator, Sample *sample) {
    ChunkIterator *iter = (ChunkIterator *)iterator;
    if (iter->currentIndex >= 0) {
        *sample = *ChunkGetSample(iter->chunk, iter->currentIndex);
        iter->currentIndex--;
        return CR_OK;
    } else {
        return CR_END;
    }
}

void Uncompressed_FreeChunkIterator(ChunkIter_t *iter, bool rev) {
    (void)rev; // only used with compressed chunk but signature must be similar
    free(iter);
}

size_t Uncompressed_GetChunkSize(Chunk_t *chunk, bool includeStruct) {
    Chunk *uncompChunk = chunk;
    size_t size = uncompChunk->max_samples * sizeof(Sample);
    size += includeStruct ? sizeof(*uncompChunk) : 0;
    return size;
}
