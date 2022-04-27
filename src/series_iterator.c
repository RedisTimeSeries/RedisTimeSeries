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
                                     bool rev_chunk) {
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

// Fills sample from chunk. If all samples were extracted from the chunk, we
// move to the next chunk.
EnrichedChunk *SeriesIteratorGetNextChunk(AbstractIterator *abstractIterator) {
    SeriesIterator *iter = (SeriesIterator *)abstractIterator;
    Chunk_t *curChunk = iter->currentChunk;
    if (!curChunk) {
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

    return iter->enrichedChunk;
}
