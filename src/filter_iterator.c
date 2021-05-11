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

    if (sample.value >= byValueArgs.min &&
            sample.value <= byValueArgs.max) {
        return true;
    } else {
        return false;
    }
}

static bool check_sample_timestamp(Sample sample, FilterByTSArgs byTsArgs) {
    if (!byTsArgs.hasValue) {
        return true;
    }

    for (int i=0; i < byTsArgs.count; i++) {
        if (sample.timestamp == byTsArgs.values[i]) {
            return true;
        }
    }
    return false;
}

ChunkResult SeriesFilterIterator_GetNext(struct AbstractIterator *base, Sample *currentSample) {
    SeriesFilterIterator *self = (SeriesFilterIterator *)base;
    Sample sample = { 0 };
    ChunkResult cr = CR_ERR;
    while (true) {
        cr = self->base.input->GetNext(self->base.input, &sample);

        if (cr == CR_OK) {
            if (check_sample_value(sample, self->byValueArgs) && check_sample_timestamp(sample, self->ByTsArgs)) {
                *currentSample = sample;
                return cr;
            }
            continue;
        } else {
            return cr;
        }
    }
}

SeriesFilterIterator *SeriesFilterIterator_New(AbstractIterator *input, FilterByValueArgs byValue, FilterByTSArgs ByTsArgs) {
    SeriesFilterIterator *newIter = malloc(sizeof(SeriesFilterIterator));
    newIter->base.input = input;
    newIter->base.GetNext = SeriesFilterIterator_GetNext;
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
                                             bool reverse) {
    AggregationIterator *iter = malloc(sizeof(AggregationIterator));
    iter->base.GetNext = AggregationIterator_GetNext;
    iter->base.Close = AggregationIterator_Close;
    iter->base.input = input;
    iter->aggregation = aggregation;
    iter->aggregationTimeDelta = aggregationTimeDelta;
    iter->aggregationContext = iter->aggregation->createContext();
    iter->aggregationLastTimestamp = 0;

    iter->aggregationIsFirstSample = true;
    iter->aggregationIsFinalized = false;
    iter->reverse = reverse;
    iter->initilized = false;

    return iter;
}

bool finalizeBucket(Sample *currentSample, const AggregationIterator *self) {
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

ChunkResult AggregationIterator_GetNext(struct AbstractIterator *iter, Sample *currentSample) {
    AggregationIterator *self = (AggregationIterator *)iter;

    Sample internalSample = { 0 };
    ChunkResult result = iter->input->GetNext(iter->input, &internalSample);

    if (result == CR_OK && !self->initilized) {
        timestamp_t init_ts = internalSample.timestamp;
        self->aggregationLastTimestamp = init_ts - (init_ts % self->aggregationTimeDelta);
        self->initilized = true;
    }

    bool hasSample = FALSE;
    while (result == CR_OK) {
        if ((self->reverse == FALSE &&
             internalSample.timestamp >=
                 self->aggregationLastTimestamp + self->aggregationTimeDelta) ||
            (self->reverse == TRUE && internalSample.timestamp < self->aggregationLastTimestamp)) {
            // update the last timestamp before because its relevant for first sample and others
            if (self->aggregationIsFirstSample == FALSE) {
                hasSample = finalizeBucket(currentSample, self);
            }
            self->aggregationLastTimestamp =
                internalSample.timestamp - (internalSample.timestamp % self->aggregationTimeDelta);
        }
        self->aggregationIsFirstSample = FALSE;
        self->aggregation->appendValue(self->aggregationContext, internalSample.value);
        if (hasSample) {
            return CR_OK;
        }
        result = self->base.input->GetNext(self->base.input, &internalSample);
    }

    if (result == CR_END) {
        if (self->aggregationIsFinalized || self->aggregationIsFirstSample) {
            return CR_END;
        } else {
            double value;
            if (self->aggregation->finalize(self->aggregationContext, &value) == TSDB_OK) {
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
