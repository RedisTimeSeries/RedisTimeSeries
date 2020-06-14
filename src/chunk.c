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

Chunk_t *Uncompressed_SplitChunk(Chunk_t *chunk) {
    Chunk *curChunk = (Chunk *)chunk;
    size_t split = curChunk->num_samples / 2;
    size_t curNumSamples = curChunk->num_samples - split;

    // create chunk and copy samples
    Chunk *newChunk = Uncompressed_NewChunk(split);
    for (size_t i = 0; i < split; ++i) {
        Sample *sample = &curChunk->samples[curNumSamples + i];
        ChunkResult res = Uncompressed_AddSample(newChunk, sample);
        assert(res == CR_OK);
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

static ChunkResult operateOccupiedSample(AddCtx *aCtx, size_t idx) {
    Chunk *regChunk = (Chunk *)aCtx->inChunk;
    short numSamples = regChunk->num_samples;
    // printf("cur %lu vs sample %lu, %f\n", ChunkGetSample(regChunk, i)->timestamp,
    // sample->timestamp, sample->value);
    switch (aCtx->type) {
        case UPSERT_ADD:
            regChunk->samples[idx] = aCtx->sample;
            return CR_OK;
        case UPSERT_DEL: {
            memmove(&regChunk->samples[idx],
                    &regChunk->samples[idx + 1],
                    (numSamples - idx) * sizeof(Sample));
            if (numSamples == regChunk->max_samples) {
                // TODO: adjust memory
            }
            regChunk->num_samples--;
            aCtx->sz = -1;
            return CR_OK;
        }
    }
    return CR_ERR;
}

static void copyChunk(Chunk *dest, Chunk *src, size_t srcIdx, short qty) {
    memcpy(dest->samples, src->samples + srcIdx, qty);
    dest->base_timestamp = dest->samples[0].timestamp;
    dest->num_samples = qty;
}

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

ChunkResult Uncompressed_UpsertSample(AddCtx *aCtx) {
    Chunk *regChunk = (Chunk *)aCtx->inChunk;
    timestamp_t ts = aCtx->sample.timestamp;
    short numSamples = regChunk->num_samples;
    // find sample location
    size_t i = 0;
    for (; i < numSamples; ++i) {
        if (ts <= ChunkGetSample(regChunk, i)->timestamp) {
            break;
        }
    }
    // TODO: TS.UPSERT vs TS.ADD
    if (ts == ChunkGetSample(regChunk, i)->timestamp) {
        return operateOccupiedSample(aCtx, i);
    } else if (aCtx->type == UPSERT_DEL) {
        return CR_DEL_FAIL;
    }

    if (i == 0) {
        regChunk->base_timestamp = ts;
        aCtx->reindex = true;
    }

    bool shouldSplit =
        (numSamples == regChunk->max_samples && numSamples > aCtx->maxSamples * SPLIT_FACTOR);
    if (!shouldSplit || shouldSplit) {
        upsertChunk(regChunk, i, &aCtx->sample);
    } else { // split - unused
        short split = numSamples / 2;
        Chunk *newChunk = Uncompressed_NewChunk(split + SPLIT_EXTRA);
        copyChunk(newChunk, regChunk, split, numSamples - split);
        regChunk->max_samples = regChunk->num_samples = split;
        if (i < split) {
            upsertChunk(regChunk, i, &aCtx->sample);
        } else {
            upsertChunk(newChunk, i - split, &aCtx->sample);
            regChunk->max_samples += SPLIT_EXTRA;
            regChunk->samples = realloc(regChunk->samples, regChunk->max_samples * sizeof(Sample));
        }
        aCtx->outChunk_1 = newChunk;
    }
    aCtx->sz = 1;
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

size_t Uncompressed_GetChunkSize(Chunk_t *chunk) {
    return sizeof(Chunk) + ((Chunk *)chunk)->max_samples * sizeof(Sample);
}
