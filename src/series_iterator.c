/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "series_iterator.h"

#include "abstract_iterator.h"
#include "filter_iterator.h"
#include "tsdb.h"
#include "enriched_chunk.h"

EnrichedChunk *SeriesIteratorGetNextChunk(AbstractIterator *iterator);

void SeriesIteratorClose(AbstractIterator *iterator);

// Initiates SeriesIterator, find the correct chunk and initiate a ChunkIterator
AbstractIterator *SeriesIterator_New(Series *series,
                                     timestamp_t start_ts,
                                     timestamp_t end_ts,
                                     bool rev,
                                     bool rev_chunk,
                                     bool latest) {
    SeriesIterator *iter = malloc(sizeof(SeriesIterator));
    iter->base.Close = SeriesIteratorClose;
    iter->base.GetNext = SeriesIteratorGetNextChunk;
    iter->base.input = NULL;
    iter->currentChunk = NULL;
    iter->enrichedChunk = NewEnrichedChunk();
    iter->series = series;
    iter->minTimestamp = start_ts;
    iter->maxTimestamp = end_ts;
    iter->reverse = rev;
    iter->reverse_chunk = rev_chunk;
    iter->latest = latest;

    timestamp_t rax_key;

    if (!rev) {
        iter->DictGetNext = RedisModule_DictNextC;
        seriesEncodeTimestamp(&rax_key, iter->minTimestamp);
    } else {
        iter->DictGetNext = RedisModule_DictPrevC;
        seriesEncodeTimestamp(&rax_key, iter->maxTimestamp);
    }

    // get first chunk within query range
    iter->dictIter =
        RedisModule_DictIteratorStartC(series->chunks, "<=", &rax_key, sizeof(rax_key));
    if (!iter->DictGetNext(iter->dictIter, NULL, (void *)&iter->currentChunk)) {
        RedisModule_DictIteratorReseekC(iter->dictIter, "^", NULL, 0);
        iter->DictGetNext(iter->dictIter, NULL, (void *)&iter->currentChunk);
    }

    return (AbstractIterator *)iter;
}

void SeriesIteratorClose(AbstractIterator *iterator) {
    SeriesIterator *self = (SeriesIterator *)iterator;
    RedisModule_DictIteratorStop(self->dictIter);
    FreeEnrichedChunk(self->enrichedChunk);
    free(iterator);
}

extern RedisModuleCtx *rts_staticCtx; // global redis ctx

// LATEST is ignored for a series that is not a compaction.
#define should_finalize_last_bucket(iter)                                                          \
    ((iter)->latest && (iter)->series->srcKey &&                                                   \
     (iter)->maxTimestamp > (iter)->series->lastTimestamp)

// Fills sample from chunk. If all samples were extracted from the chunk, we
// move to the next chunk.
EnrichedChunk *SeriesIteratorGetNextChunk(AbstractIterator *abstractIterator) {
    Sample sample;
    Sample *sample_ptr = &sample;
    SeriesIterator *iter = (SeriesIterator *)abstractIterator;
    Chunk_t *curChunk = iter->currentChunk;

    if (unlikely(iter->reverse && should_finalize_last_bucket(iter))) {
        goto _handle_latest;
    }

    if (!curChunk || iter->series->funcs->GetNumOfSample(curChunk) == 0) {
        if (unlikely(curChunk && iter->series->funcs->GetNumOfSample(curChunk) > 0 &&
                     iter->series->totalSamples == 0)) { // empty chunks are being removed
            RedisModule_Log(rts_staticCtx, "error", "Empty chunk in a non empty series is invalid");
        }
        if (should_finalize_last_bucket(iter)) {
            iter->enrichedChunk->samples.num_samples = 0;
            goto _handle_latest;
        }
        return NULL;
    }

    u_int64_t n_samples = iter->series->funcs->GetNumOfSample(curChunk);
    if (n_samples > iter->enrichedChunk->samples.size) {
        ReallocSamplesArray(&iter->enrichedChunk->samples, n_samples);
    }
    iter->series->funcs->ProcessChunk(
        curChunk, iter->minTimestamp, iter->maxTimestamp, iter->enrichedChunk, iter->reverse_chunk);
    if (!iter->DictGetNext(iter->dictIter, NULL, (void *)&iter->currentChunk)) {
        iter->currentChunk = NULL;
    }

    if (unlikely(!iter->reverse &&
                 iter->series->funcs->GetLastTimestamp(curChunk) < iter->minTimestamp)) {
        // In forward iterator it's possible that the minTimestamp is located between the 1st chunk
        // and the 2nd in this case the first proces chunk will result in an empty result and we
        // need to continue to process the 2nd chunk
        return SeriesIteratorGetNextChunk(abstractIterator);
    }

    if (iter->enrichedChunk->samples.num_samples > 0 || (!should_finalize_last_bucket(iter))) {
        goto _out;
    }

_handle_latest:
    calculate_latest_sample(&sample_ptr, iter->series);
    if (sample_ptr && (sample.timestamp <= iter->maxTimestamp)) {
        if (iter->enrichedChunk->samples.size == 0) {
            ReallocSamplesArray(&iter->enrichedChunk->samples, 1);
        }
        ResetEnrichedChunk(iter->enrichedChunk);
        iter->enrichedChunk->rev = iter->reverse_chunk;
        iter->enrichedChunk->samples.num_samples = 1;
        *iter->enrichedChunk->samples.timestamps = sample.timestamp;
        *iter->enrichedChunk->samples.values = sample.value;
    }
    iter->latest = false;

_out:
    return iter->enrichedChunk;
}
