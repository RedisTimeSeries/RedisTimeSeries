/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
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
