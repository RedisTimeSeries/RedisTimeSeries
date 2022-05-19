/*
 * Copyright 2018-2022 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#ifndef REDISTIMESERIES_MULTISERIES_AGG_DUP_SAMPLE_ITERATOR_H
#define REDISTIMESERIES_MULTISERIES_AGG_DUP_SAMPLE_ITERATOR_H

#include "abstract_iterator.h"
#include "query_language.h"

typedef struct MultiSeriesAggDupSampleIterator
{
    AbstractMultiSeriesAggDupSampleIterator base;
    void *aggregationContext;
    AggregationClass *aggregation;
    Sample next_sample;
    bool has_next_sample;
} MultiSeriesAggDupSampleIterator;

MultiSeriesAggDupSampleIterator *MultiSeriesAggDupSampleIterator_New(
    AbstractMultiSeriesSampleIterator *input,
    const ReducerArgs *reducerArgs);

#endif // REDISTIMESERIES_MULTISERIES_AGG_DUP_SAMPLE_ITERATOR_H
