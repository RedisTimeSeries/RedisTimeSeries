/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "filter_iterator.h"

#include "abstract_iterator.h"
#include "series_iterator.h"

static bool check_sample_value(Sample sample, FilterByValueArgs byValueArgs) {
    if (!byValueArgs.hasValue) {
        return true;
    }

    if (sample.value >= byValueArgs.min && sample.value <= byValueArgs.max) {
        return true;
    } else {
        return false;
    }
}

static bool check_sample_timestamp(Sample sample, FilterByTSArgs byTsArgs) {
    if (!byTsArgs.hasValue) {
        return true;
    }
    return timestamp_binary_search(byTsArgs.values, byTsArgs.count, sample.timestamp) == -1 ? false
                                                                                            : true;
}

Chunk *SeriesFilterIterator_GetNextChunk(struct AbstractIterator *base) {
    SeriesFilterIterator *self = (SeriesFilterIterator *)base;
    Chunk *chunk;
    size_t i, count = 0;
    while ((chunk = self->base.input->GetNext(self->base.input))) {
        for (i = 0; i < chunk->num_samples; ++i) {
            if (check_sample_value(chunk->samples[i], self->byValueArgs) &&
                check_sample_timestamp(chunk->samples[i], self->ByTsArgs)) {
                chunk->samples[count++] = chunk->samples[i];
            }
        }

        if (count > 0) {
            chunk->num_samples = count;
            chunk->base_timestamp = chunk->samples[0].timestamp;
            return chunk;
        }
    }

    return NULL;
}

SeriesFilterIterator *SeriesFilterIterator_New(AbstractIterator *input,
                                               FilterByValueArgs byValue,
                                               FilterByTSArgs ByTsArgs) {
    SeriesFilterIterator *newIter = malloc(sizeof(SeriesFilterIterator));
    newIter->base.input = input;
    newIter->base.GetNext = SeriesFilterIterator_GetNextChunk;
    newIter->base.Close = SeriesFilterIterator_Close;
    newIter->byValueArgs = byValue;
    newIter->ByTsArgs = ByTsArgs;
    return newIter;
}

void SeriesFilterIterator_Close(struct AbstractIterator *iterator) {
    iterator->input->Close(iterator->input);
    free(iterator);
}

AggregationIterator *AggregationIterator_New(struct AbstractIterator *input,
                                             AggregationClass *aggregation,
                                             int64_t aggregationTimeDelta,
                                             timestamp_t timestampAlignment,
                                             bool reverse) {
    AggregationIterator *iter = malloc(sizeof(AggregationIterator));
    iter->base.GetNext = AggregationIterator_GetNextChunk;
    iter->base.Close = AggregationIterator_Close;
    iter->base.input = input;
    iter->aggregation = aggregation;
    iter->timestampAlignment = timestampAlignment;
    iter->aggregationTimeDelta = aggregationTimeDelta;
    iter->aggregationContext = iter->aggregation->createContext();
    iter->aggregationLastTimestamp = 0;
    iter->hasUnFinalizedContext = false;
    iter->reverse = reverse;
    iter->initilized = false;

    return iter;
}

static inline void finalizeBucket(Sample *currentSample, const AggregationIterator *self) {
    double value;
    self->aggregation->finalize(self->aggregationContext, &value);
    currentSample->timestamp = self->aggregationLastTimestamp;
    currentSample->value = value;
    self->aggregation->resetContext(self->aggregationContext);
}

// process C's modulo result to translate from a negative modulo to a positive
#define modulo(x, N) ((x % N + N) % N)

static timestamp_t calc_ts_bucket(timestamp_t ts,
                                  u_int64_t timedelta,
                                  timestamp_t timestampAlignment) {
    const int64_t timestamp_diff = ts - timestampAlignment;
    return ts - modulo(timestamp_diff, (int64_t)timedelta);
}

Chunk *AggregationIterator_GetNextChunk(struct AbstractIterator *iter) {
    AggregationIterator *self = (AggregationIterator *)iter;
    Chunk *aux_chunk;
    AggregationClass *aggregation = self->aggregation;
    void *aggregationContext = self->aggregationContext;

    AbstractIterator *input = iter->input;
    Chunk *chunk = input->GetNext(input);
    double value;
    if (!chunk || chunk->num_samples == 0) {
        if (self->hasUnFinalizedContext) {
            goto _finalize;
        } else {
            return NULL;
        }
    }

    self->hasUnFinalizedContext = true;

    size_t n_samples;
    Sample sample;

    u_int64_t aggregationTimeDelta = self->aggregationTimeDelta;
    bool is_reserved = self->reverse;
    if (!self->initilized) {
        timestamp_t init_ts = chunk->samples[0].timestamp;
        self->aggregationLastTimestamp =
            calc_ts_bucket(init_ts, aggregationTimeDelta, self->timestampAlignment);
        self->initilized = true;
    }

    size_t agg_n_samples = 0;
    void (*appendValue)(void *, double) = aggregation->appendValue;
    u_int64_t contextScope = self->aggregationLastTimestamp + aggregationTimeDelta;
    while (chunk) {
        n_samples = chunk->num_samples;
        for (size_t i = 0; i < n_samples; ++i) {
            sample = chunk->samples[i]; // store sample cause we aggregate in place
            // (1) aggregationTimeDelta > 0,
            // (2) self->aggregationLastTimestamp > chunk->samples[0].timestamp -
            // aggregationTimeDelta (3) self->aggregationLastTimestamp = chunk->samples[0].timestamp
            // - mod where 0 <= mod from (1)+(2) contextScope > chunk->samples[0].timestamp from (3)
            // chunk->samples[0].timestamp >= self->aggregationLastTimestamp so the following
            // condition should always be false on the first iteration
            if ((is_reserved == FALSE && sample.timestamp >= contextScope) ||
                (is_reserved == TRUE && sample.timestamp < self->aggregationLastTimestamp)) {
                finalizeBucket(&chunk->samples[agg_n_samples++], self);
                self->aggregationLastTimestamp = calc_ts_bucket(
                    sample.timestamp, aggregationTimeDelta, self->timestampAlignment);
                contextScope = self->aggregationLastTimestamp + aggregationTimeDelta;
            }

            appendValue(aggregationContext, sample.value);
        }

        if (agg_n_samples > 0) {
            chunk->num_samples = agg_n_samples;
            chunk->base_timestamp = chunk->samples[0].timestamp;
            return chunk;
        }
        chunk = input->GetNext(input);
    }

_finalize:
    self->hasUnFinalizedContext = false;
    aux_chunk = GetTemporaryUncompressedChunk();
    aggregation->finalize(aggregationContext, &value);
    aux_chunk->samples[0].timestamp = self->aggregationLastTimestamp;
    aux_chunk->samples[0].value = value;
    aux_chunk->num_samples = 1;
    aux_chunk->base_timestamp = aux_chunk->samples[0].timestamp;
    return aux_chunk;
}

void AggregationIterator_Close(struct AbstractIterator *iterator) {
    AggregationIterator *self = (AggregationIterator *)iterator;
    iterator->input->Close(iterator->input);
    self->aggregation->freeContext(self->aggregationContext);
    free(iterator);
}
