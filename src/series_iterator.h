/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
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
    EnrichedChunk *enrichedChunk;
    EnrichedChunk *enrichedChunkAux; // auxiliary chunk to represent reverse chunk
    api_timestamp_t maxTimestamp;
    api_timestamp_t minTimestamp;
    bool reverse;
    bool reverse_chunk;
    bool latest;
    void *(*DictGetNext)(RedisModuleDictIter *di, size_t *keylen, void **dataptr);
} SeriesIterator;

struct AbstractIterator *SeriesIterator_New(Series *series,
                                            timestamp_t start_ts,
                                            timestamp_t end_ts,
                                            bool rev,
                                            bool rev_chunk,
                                            bool latest);

#endif // REDIS_TIMESERIES_CLEAN_SERIES_ITERATOR_H
