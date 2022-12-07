/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "abstract_iterator.h"
#include "query_language.h"
#include "series_iterator.h"

#ifndef ROLLING_AGGREGATION_ITERATOR_H
#define ROLLING_AGGREGATION_ITERATOR_H

typedef struct RollingAggregationIterator
{
    AbstractIterator base;
    AggregationClass *aggregation;
    int64_t windowSize;
    void *aggregationContext;
    Series *series;
    api_timestamp_t startTimestamp;
    api_timestamp_t endTimestamp;
    uint64_t count;
} RollingAggregationIterator;

RollingAggregationIterator *RollingAggregationIterator_New(struct AbstractIterator *input,
                                                           AggregationClass *aggregation,
                                                           uint64_t windowSize,
                                                           Series *series,
                                                           api_timestamp_t startTimestamp,
                                                           api_timestamp_t endTimestamp);
EnrichedChunk *RollingAggregationIterator_GetNextChunk(struct AbstractIterator *iter);
void RollingAggregationIterator_Close(struct AbstractIterator *iterator);

#endif // ROLLING_AGGREGATION_ITERATOR_H
