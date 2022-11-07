/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "rolling_aggregation_iterator.h"
#include "consts.h"

void RollingAggregationIterator_Close(struct AbstractIterator *iterator) {
    RollingAggregationIterator *self = (RollingAggregationIterator *)iterator;
    iterator->input->Close(iterator->input);
    self->aggregation->freeContext(self->aggregationContext);
    free(iterator);
}

RollingAggregationIterator *RollingAggregationIterator_New(struct AbstractIterator *input,
                                                           AggregationClass *aggregation,
                                                           uint64_t windowSize,
                                                           Series *series,
                                                           api_timestamp_t startTimestamp,
                                                           api_timestamp_t endTimestamp) {
    RollingAggregationIterator *iter = malloc(sizeof(RollingAggregationIterator));
    iter->base.GetNext = RollingAggregationIterator_GetNextChunk;
    iter->base.Close = RollingAggregationIterator_Close;
    iter->base.input = input;
    iter->aggregation = aggregation;
    iter->windowSize = windowSize;
    iter->aggregationContext = iter->aggregation->createContext(DC, windowSize);
    iter->series = series;
    iter->startTimestamp = startTimestamp;
    iter->endTimestamp = endTimestamp;
    iter->count = 0;
    return iter;
}

EnrichedChunk *RollingAggregationIterator_GetNextChunk(struct AbstractIterator *iter) {
    RollingAggregationIterator *self = (RollingAggregationIterator *)iter;
    AggregationClass *aggregation = self->aggregation;
    void *context = self->aggregationContext;
    u_int64_t windowSize = self->windowSize;

    AbstractIterator *input = iter->input;
    EnrichedChunk *enrichedChunk = NULL;
    unsigned int n_samples;
    unsigned int i = 0;
    unsigned int stop_cond;

    while ((self->count < (windowSize - 1)) && (enrichedChunk = input->GetNext(input))) {
        n_samples = enrichedChunk->samples.num_samples;
        stop_cond = min(windowSize - self->count - 1, n_samples);
        for (i = 0; i < stop_cond; ++i, ++self->count) {
            aggregation->appendValue(
                context, enrichedChunk->samples.values[i], enrichedChunk->samples.timestamps[i]);
        }
    }

    unsigned int j = 0;
    if (!enrichedChunk) {
        enrichedChunk = input->GetNext(input);
        if (!enrichedChunk) {
            return NULL;
        }
        n_samples = enrichedChunk->samples.num_samples;
    }

    double val;
    for (; i < n_samples; ++i, ++j) {
        aggregation->appendValue(
            context, enrichedChunk->samples.values[i], enrichedChunk->samples.timestamps[i]);
        aggregation->finalize(context, &val);

        // insert the values inplace
        enrichedChunk->samples.timestamps[j] = enrichedChunk->samples.timestamps[i];
        enrichedChunk->samples.values[j] = val;
    }

    enrichedChunk->samples.num_samples = j;
    return enrichedChunk;
}
