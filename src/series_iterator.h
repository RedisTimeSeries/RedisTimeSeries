/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "tsdb.h"

#ifndef REDIS_TIMESERIES_CLEAN_SERIES_ITERATOR_H
#define REDIS_TIMESERIES_CLEAN_SERIES_ITERATOR_H

typedef struct SeriesIterator
{
    Series *series;
    RedisModuleDictIter *dictIter;
    Chunk_t *currentChunk;
    ChunkIter_t *chunkIterator;
    ChunkIterFuncs chunkIteratorFuncs;
    api_timestamp_t maxTimestamp;
    api_timestamp_t minTimestamp;
    bool reverse;
    void *(*DictGetNext)(RedisModuleDictIter *di, size_t *keylen, void **dataptr);
    AggregationClass *aggregation;
    void *aggregationContext;
    timestamp_t aggregationLastTimestamp;
    int64_t aggregationTimeDelta;
    bool aggregationIsFirstSample;
    bool aggregationIsFinalized;
} SeriesIterator;

int SeriesQuery(Series *series,
                SeriesIterator *iter,
                timestamp_t start_ts,
                timestamp_t end_ts,
                bool rev,
                AggregationClass *aggregation,
                int64_t time_delta);

ChunkResult SeriesIteratorGetNext(SeriesIterator *iterator, Sample *currentSample);

void SeriesIteratorClose(SeriesIterator *iterator);

#endif // REDIS_TIMESERIES_CLEAN_SERIES_ITERATOR_H
