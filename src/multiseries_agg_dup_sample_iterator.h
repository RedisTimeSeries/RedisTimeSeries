/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
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
