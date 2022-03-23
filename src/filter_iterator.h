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

DomainChunk *SeriesFilterTSIterator_GetNextChunk(struct AbstractIterator *base);

void SeriesFilterIterator_Close(struct AbstractIterator *iterator);

typedef struct SeriesFilterValIterator
{
    AbstractIterator base;
    FilterByValueArgs byValueArgs;
} SeriesFilterValIterator;

SeriesFilterValIterator *SeriesFilterValIterator_New(AbstractIterator *input,
                                                     FilterByValueArgs byValue);

DomainChunk *SeriesFilterValIterator_GetNextChunk(struct AbstractIterator *base);

typedef struct AggregationIterator
{
    AbstractIterator base;
    AggregationClass *aggregation;
    int64_t aggregationTimeDelta;
    timestamp_t timestampAlignment;
    void *aggregationContext;
    timestamp_t aggregationLastTimestamp;
    bool hasUnFinalizedContext;
    bool reverse;
    bool initilized;
    DomainChunk *aux_chunk; // auxiliary chunk for containing the final bucket
} AggregationIterator;

AggregationIterator *AggregationIterator_New(struct AbstractIterator *input,
                                             AggregationClass *aggregation,
                                             int64_t aggregationTimeDelta,
                                             timestamp_t timestampAlignment,
                                             bool reverse);
DomainChunk *AggregationIterator_GetNextChunk(struct AbstractIterator *iter);
void AggregationIterator_Close(struct AbstractIterator *iterator);

#endif // FILTER_ITERATOR_H
