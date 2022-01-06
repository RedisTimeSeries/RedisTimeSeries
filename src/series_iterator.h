/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "abstract_iterator.h"
#include "query_language.h"
#include "tsdb.h"

#ifndef REDIS_TIMESERIES_CLEAN_SERIES_ITERATOR_H
#define REDIS_TIMESERIES_CLEAN_SERIES_ITERATOR_H

typedef struct SeriesIterator
{
    AbstractIterator base;
    Series *series;
    RedisModuleDictIter *dictIter;
    Chunk_t *currentChunk;
    ChunkIter_t *chunkIterator;
    ChunkIterFuncs chunkIteratorFuncs;
    api_timestamp_t maxTimestamp;
    api_timestamp_t minTimestamp;
    bool reverse;
    void *(*DictGetNext)(RedisModuleDictIter *di, size_t *keylen, void **dataptr);
} SeriesIterator;

struct AbstractIterator *SeriesIterator_New(Series *series,
                                            timestamp_t start_ts,
                                            timestamp_t end_ts,
                                            bool rev);

ChunkResult SeriesIteratorGetNext(AbstractIterator *iterator, Sample *currentSample);

void SeriesIteratorClose(AbstractIterator *iterator);

bool SeriesIteratorGetNextBoundaryAggValue(AbstractIterator *iterator,
                                           const timestamp_t boundaryStart,
                                           const timestamp_t boundaryEnd,
                                           Sample *sample);

#endif // REDIS_TIMESERIES_CLEAN_SERIES_ITERATOR_H
