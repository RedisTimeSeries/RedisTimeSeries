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
#include "series_iterator.h"

#ifndef FILTER_ITERATOR_H
#define FILTER_ITERATOR_H

typedef struct SeriesFilterTSIterator
{
    AbstractIterator base;
    FilterByTSArgs ByTsArgs;
    size_t tsFilterIndex; // the index in the TS filter array in ByTsArgs
    bool reverse;
} SeriesFilterTSIterator;

SeriesFilterTSIterator *SeriesFilterTSIterator_New(AbstractIterator *input,
                                                   FilterByTSArgs ByTsArgs,
                                                   bool rev);

EnrichedChunk *SeriesFilterTSIterator_GetNextChunk(struct AbstractIterator *base);

void SeriesFilterIterator_Close(struct AbstractIterator *iterator);

typedef struct SeriesFilterValIterator
{
    AbstractIterator base;
    FilterByValueArgs byValueArgs;
} SeriesFilterValIterator;

SeriesFilterValIterator *SeriesFilterValIterator_New(AbstractIterator *input,
                                                     FilterByValueArgs byValue);

EnrichedChunk *SeriesFilterValIterator_GetNextChunk(struct AbstractIterator *base);

typedef struct AggregationIterator
{
    AbstractIterator base;
    size_t numAggregations;
    AggregationClass aggregations[TS_AGG_TYPES_MAX];
    void *aggregationContexts[TS_AGG_TYPES_MAX];
    int64_t aggregationTimeDelta;
    timestamp_t timestampAlignment;
    timestamp_t aggregationLastTimestamp;
    bool hasUnFinalizedContext;
    bool reverse;
    bool initialized;
    bool empty; // should report empty buckets
    BucketTimestamp bucketTS;
    EnrichedChunk *aux_chunk; // auxiliary chunk for containing the final bucket
    Series *series;
    api_timestamp_t startTimestamp;
    api_timestamp_t endTimestamp;
    bool hasTwa; // precomputed: any aggregation is TWA
    bool handled_twa_empty_prefix;
    bool handled_twa_empty_suffix;
    // No points in [start,end] but EMPTY is on: we already returned the "all buckets empty" result once.
    bool handled_non_twa_empty_full_range;
    // We already inserted EMPTY buckets between range start and the first real point; do not repeat.
    bool handled_non_twa_empty_prefix;
    // LAST+EMPTY: we already copied the last raw value from before `startTimestamp` into LAST (or LAST not used).
    bool last_locf_seed_ok;
    timestamp_t prev_ts;
    bool validSamplesInBucket; // are there any valid samples in current bucket (any aggregation)
    bool validPerAgg[TS_AGG_TYPES_MAX]; // per-aggregation validity tracking for current bucket
} AggregationIterator;

AggregationIterator *AggregationIterator_New(struct AbstractIterator *input,
                                             size_t numAggregations,
                                             AggregationClass **aggregations,
                                             int64_t aggregationTimeDelta,
                                             timestamp_t timestampAlignment,
                                             bool reverse,
                                             bool empty,
                                             BucketTimestamp bucketTS,
                                             Series *series,
                                             api_timestamp_t startTimestamp,
                                             api_timestamp_t endTimestamp);
EnrichedChunk *AggregationIterator_GetNextChunk(struct AbstractIterator *iter);
void AggregationIterator_Close(struct AbstractIterator *iterator);

#endif // FILTER_ITERATOR_H
