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

ChunkResult SeriesFilterIterator_GetNext(struct AbstractIterator *base, Sample *currentSample) {
    SeriesFilterIterator *self = (SeriesFilterIterator *)base;
    Sample sample = { 0 };
    ChunkResult cr = CR_ERR;
    while (true) {
        cr = self->base.input->GetNext(self->base.input, &sample);

        if (cr == CR_OK) {
            if (check_sample_value(sample, self->byValueArgs) &&
                check_sample_timestamp(sample, self->ByTsArgs)) {
                *currentSample = sample;
                return cr;
            }
            continue;
        } else {
            return cr;
        }
    }
}

SeriesFilterIterator *SeriesFilterIterator_New(AbstractIterator *input,
                                               FilterByValueArgs byValue,
                                               FilterByTSArgs ByTsArgs) {
    SeriesFilterIterator *newIter = malloc(sizeof(SeriesFilterIterator));
    newIter->base.input = input;
    newIter->base.GetNext = SeriesFilterIterator_GetNext;
    newIter->base.Close = SeriesFilterIterator_Close;
    newIter->base.GetNextBoundaryAggValue = NULL;
    newIter->byValueArgs = byValue;
    newIter->ByTsArgs = ByTsArgs;
    return newIter;
}

void SeriesFilterIterator_Close(struct AbstractIterator *iterator) {
    iterator->input->Close(iterator->input);
    free(iterator);
}

AggregationIterator *AggregationIterator_New(struct AbstractIterator *input,
                                             int aggregationType,
                                             AggregationClass *aggregation,
                                             int64_t aggregationTimeDelta,
                                             timestamp_t timestampAlignment,
                                             bool reverse) {
    AggregationIterator *iter = malloc(sizeof(AggregationIterator));
    iter->base.GetNext = AggregationIterator_GetNext;
    iter->base.Close = AggregationIterator_Close;
    iter->base.input = input;
    iter->aggregation = aggregation;
    iter->timestampAlignment = timestampAlignment;
    iter->aggregationType = aggregationType;
    iter->aggregationTimeDelta = aggregationTimeDelta;
    iter->aggregationContext = iter->aggregation->createContext();
    iter->aggregationLastTimestamp = 0;

    iter->aggregationIsFirstSample = true;
    iter->aggregationIsFinalized = false;
    iter->reverse = reverse;
    iter->initilized = false;

    return iter;
}

static bool finalizeBucket(Sample *currentSample, const AggregationIterator *self) {
    bool hasSample = false;
    double value;
    if (self->aggregation->finalize(self->aggregationContext, &value) == TSDB_OK) {
        currentSample->timestamp = self->aggregationLastTimestamp;
        currentSample->value = value;
        hasSample = TRUE;
        self->aggregation->resetContext(self->aggregationContext);
    }
    return hasSample;
}

// process C's modulo result to translate from a negative modulo to a positive
#define modulo(x, N) ((x % N + N) % N)

static timestamp_t calc_ts_bucket(timestamp_t ts,
                                  u_int64_t timedelta,
                                  timestamp_t timestampAlignment) {
    const int64_t timestamp_diff = ts - timestampAlignment;
    return ts - modulo(timestamp_diff, (int64_t)timedelta);
}

ChunkResult AggregationIterator_GetNext(struct AbstractIterator *iter, Sample *currentSample) {
    AggregationIterator *self = (AggregationIterator *)iter;

    Sample internalSample = { 0 };
    AbstractIterator *baseIterator = iter->input;
    ChunkResult (*getNextInput)(struct AbstractIterator *, Sample *) = baseIterator->GetNext;
    bool (*getNextBoundaryAggValue)(
        struct AbstractIterator *, const timestamp_t, const timestamp_t, int, Sample *) =
        baseIterator->GetNextBoundaryAggValue;
    const u_int64_t aggregationTimeDelta = self->aggregationTimeDelta;
    bool is_reversed = self->reverse;

    ChunkResult result = getNextInput(baseIterator, &internalSample);

    if (result == CR_OK && !self->initilized) {
        self->aggregationLastTimestamp = calc_ts_bucket(
            internalSample.timestamp, aggregationTimeDelta, self->timestampAlignment);
        self->initilized = true;
    }
    bool hasSample = FALSE;
    AggregationClass *aggregation = self->aggregation;
    void (*appendValue)(void *, double) = aggregation->appendValue;
    void *aggregationContext = self->aggregationContext;
    u_int64_t timestampBoundary = self->aggregationLastTimestamp + aggregationTimeDelta;
    while (result == CR_OK) {
        if ((is_reversed == FALSE && internalSample.timestamp >= timestampBoundary) ||
            (is_reversed == TRUE && internalSample.timestamp < self->aggregationLastTimestamp)) {
            // update the last timestamp before because its relevant for first sample and others
            if (self->aggregationIsFirstSample == FALSE) {
                hasSample = finalizeBucket(currentSample, self);
            }
            self->aggregationLastTimestamp = calc_ts_bucket(
                internalSample.timestamp, aggregationTimeDelta, self->timestampAlignment);
            timestampBoundary = self->aggregationLastTimestamp + aggregationTimeDelta;
        }
        self->aggregationIsFirstSample = FALSE;

        appendValue(aggregationContext, internalSample.value);
        if (hasSample) {
            return CR_OK;
        }
        // Check if we can use boundary aggregation optimizations
        if (!is_reversed && getNextBoundaryAggValue != NULL) {
            const bool boundaryOptUsed = getNextBoundaryAggValue(baseIterator,
                                                                 internalSample.timestamp,
                                                                 timestampBoundary,
                                                                 self->aggregationType,
                                                                 &internalSample);
            if (!boundaryOptUsed)
                result = getNextInput(baseIterator, &internalSample);
        } else {
            result = getNextInput(baseIterator, &internalSample);
        }
    }

    if (result == CR_END) {
        if (self->aggregationIsFinalized || self->aggregationIsFirstSample) {
            return CR_END;
        } else {
            double value;
            if (aggregation->finalize(aggregationContext, &value) == TSDB_OK) {
                currentSample->timestamp = self->aggregationLastTimestamp;
                currentSample->value = value;
            }
            self->aggregationIsFinalized = TRUE;
            return CR_OK;
        }
    } else {
        return CR_ERR;
    }
}

void AggregationIterator_Close(struct AbstractIterator *iterator) {
    AggregationIterator *self = (AggregationIterator *)iterator;
    iterator->input->Close(iterator->input);
    self->aggregation->freeContext(self->aggregationContext);
    free(iterator);
}
