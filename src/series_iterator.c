/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "series_iterator.h"

#include "abstract_iterator.h"
#include "filter_iterator.h"
#include "tsdb.h"

// Initiates SeriesIterator, find the correct chunk and initiate a ChunkIterator
AbstractIterator *SeriesIterator_New(Series *series,
                                     timestamp_t start_ts,
                                     timestamp_t end_ts,
                                     bool rev) {
    SeriesIterator *iter = malloc(sizeof(SeriesIterator));
    iter->base.Close = SeriesIteratorClose;
    iter->base.GetNext = SeriesIteratorGetNextChunk;
    iter->base.input = NULL;
    iter->currentChunk = NULL;

    iter->series = series;
    iter->minTimestamp = start_ts;
    iter->maxTimestamp = end_ts;
    iter->reverse = rev;

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
    free(iterator);
}

// Fills sample from chunk. If all samples were extracted from the chunk, we
// move to the next chunk.
Chunk *SeriesIteratorGetNextChunk(AbstractIterator *abstractIterator) {
    SeriesIterator *iter = (SeriesIterator *)abstractIterator;
    Chunk *ret = iter->series->funcs->ProcessChunk(
        iter->currentChunk, iter->minTimestamp, iter->maxTimestamp, iter->reverse);
    iter->DictGetNext(iter->dictIter, NULL, (void *)&iter->currentChunk);
    if (!iter->DictGetNext(iter->dictIter, NULL, (void *)&iter->currentChunk)) {
        iter->currentChunk = NULL;
    }

    return ret;
}
