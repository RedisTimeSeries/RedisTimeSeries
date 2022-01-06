/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "abstract_iterator.h"
#include "query_language.h"
#include "series_iterator.h"

#ifndef FILTER_ITERATOR_H
#define FILTER_ITERATOR_H

typedef struct SeriesFilterIterator
{
    AbstractIterator base;
    FilterByValueArgs byValueArgs;
    FilterByTSArgs ByTsArgs;
} SeriesFilterIterator;

SeriesFilterIterator *SeriesFilterIterator_New(AbstractIterator *input,
                                               FilterByValueArgs byValue,
                                               FilterByTSArgs ByTsArgs);

ChunkResult SeriesFilterIterator_GetNext(struct AbstractIterator *iter, Sample *currentSample);

void SeriesFilterIterator_Close(struct AbstractIterator *iterator);

typedef struct AggregationIterator
{
    AbstractIterator base;
    AggregationClass *aggregation;
    int aggregationType;
    int64_t aggregationTimeDelta;
    timestamp_t timestampAlignment;
    void *aggregationContext;
    timestamp_t aggregationLastTimestamp;
    bool aggregationIsFirstSample;
    bool aggregationIsFinalized;
    bool reverse;
    bool initilized;
} AggregationIterator;

AggregationIterator *AggregationIterator_New(struct AbstractIterator *input,
                                             int aggregationType,
                                             AggregationClass *aggregation,
                                             int64_t aggregationTimeDelta,
                                             timestamp_t timestampAlignment,
                                             bool reverse);
ChunkResult AggregationIterator_GetNext(struct AbstractIterator *iter, Sample *currentSample);
void AggregationIterator_Close(struct AbstractIterator *iterator);

#endif // FILTER_ITERATOR_H
