/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "abstract_iterator.h"
#include "query_language.h"
#include "tsdb.h"
#include "chunk.h"

#ifndef REDIS_TIMESERIES_CLEAN_SERIES_ITERATOR_H
#define REDIS_TIMESERIES_CLEAN_SERIES_ITERATOR_H

typedef struct SeriesIterator
{
    AbstractIterator base;
    Series *series;
    RedisModuleDictIter *dictIter; // iterator over chunks
    Chunk_t *currentChunk;
    DomainChunk *domainChunk;
    DomainChunk *domainChunkAux; // auxiliary chunk to represent reverse chunk
    api_timestamp_t maxTimestamp;
    api_timestamp_t minTimestamp;
    bool reverse;
    bool reverse_chunk;
    bool isFirstIteration;
    void *(*DictGetNext)(RedisModuleDictIter *di, size_t *keylen, void **dataptr);
} SeriesIterator;

struct AbstractIterator *SeriesIterator_New(Series *series,
                                            timestamp_t start_ts,
                                            timestamp_t end_ts,
                                            bool rev,
                                            bool rev_chunk);

#endif // REDIS_TIMESERIES_CLEAN_SERIES_ITERATOR_H
