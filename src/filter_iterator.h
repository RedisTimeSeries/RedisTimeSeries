/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
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
    AggregationClass *aggregation;
    int64_t aggregationTimeDelta;
    timestamp_t timestampAlignment;
    void *aggregationContext;
    timestamp_t aggregationLastTimestamp;
    bool hasUnFinalizedContext;
    bool reverse;
    bool initilized;
    bool empty; // should report empty buckets
    BucketTimestamp bucketTS;
    EnrichedChunk *aux_chunk; // auxiliary chunk for containing the final bucket
    Series *series;
    api_timestamp_t startTimestamp;
    api_timestamp_t endTimestamp;
    bool handled_twa_empty_prefix;
    bool handled_twa_empty_suffix;
    timestamp_t prev_ts;
} AggregationIterator;

AggregationIterator *AggregationIterator_New(struct AbstractIterator *input,
                                             AggregationClass *aggregation,
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
