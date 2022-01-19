/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "filter_iterator.h"

#include "abstract_iterator.h"
#include "series_iterator.h"
#include <assert.h>

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

void filterSamples(Sample *samples,
                   size_t si,
                   size_t ei,
                   const timestamp_t *tsVals,
                   size_t values_si,
                   size_t values_ei,
                   const FilterByValueArgs *byValArgs,
                   size_t *count) {
    if (si == ei) {
        for (size_t i = values_si; i <= values_ei; ++i) {
            const Sample *sample = &samples[si];
            if (sample->timestamp == tsVals[i]) {
                if (!byValArgs ||
                    (sample->value >= byValArgs->min && sample->value <= byValArgs->max)) {
                    samples[(*count)++] = *sample;
                }
                return;
            }
        }
        return;
    }

    const size_t mid = (si + ei) / 2;

    // find tsVals that fit into left bucket
    bool found = false;
    size_t _siVals = values_si, _eiVals;

    while (_siVals <= values_ei && tsVals[_siVals] < samples[si].timestamp) {
        ++_siVals;
    }

    _eiVals = _siVals;
    while (_eiVals <= values_ei && tsVals[_eiVals] <= samples[mid].timestamp) {
        found = true;
        ++_eiVals;
    }

    if (found) {
        filterSamples(samples, si, mid, tsVals, _siVals, _eiVals - 1, byValArgs, count);
    }

    // find tsVals that fit into right bucket
    found = false;
    _siVals = _eiVals;
    while (_siVals <= values_ei && tsVals[_siVals] < samples[mid + 1].timestamp) {
        ++_siVals;
    }

    _eiVals = _siVals;
    while (_eiVals <= values_ei && tsVals[_eiVals] <= samples[ei].timestamp) {
        found = true;
        ++_eiVals;
    }

    if (found) {
        filterSamples(samples, mid + 1, ei, tsVals, _siVals, _eiVals - 1, byValArgs, count);
    }

    return;
}

DomainChunk *SeriesFilterIterator_GetNextChunk(struct AbstractIterator *base) {
    SeriesFilterIterator *self = (SeriesFilterIterator *)base;
    DomainChunk *domainChunk;
    Chunk *chunk;
    size_t i, count = 0;

    if (self->ByTsArgs.hasValue) {
        if (self->tsFilterIndex == self->ByTsArgs.count) {
            return NULL;
        }
        while ((domainChunk = self->base.input->GetNext(self->base.input))) {
            assert(!domainChunk->rev); // the impl assumes that the chunk isn't reversed
            chunk = &domainChunk->chunk;
            filterSamples(chunk->samples,
                          0,
                          chunk->num_samples - 1,
                          self->ByTsArgs.values,
                          self->tsFilterIndex,
                          self->ByTsArgs.count - 1,
                          self->byValueArgs.hasValue ? &self->byValueArgs : NULL,
                          &count);
            if (count > 0) {
                self->tsFilterIndex += count; // at least count samples consumed
                chunk->num_samples = count;
                if (unlikely(self->reverse)) {
                    reverseDomainChunk(domainChunk);
                }
                return domainChunk;
            }
        }
    } else {
        while ((domainChunk = self->base.input->GetNext(self->base.input))) {
            // currently if the query reversed the chunk will be already reversed here
            assert(self->reverse == domainChunk->rev);
            chunk = &domainChunk->chunk;
            for (i = 0; i < chunk->num_samples; ++i) {
                if (check_sample_value(chunk->samples[i], self->byValueArgs)) {
                    chunk->samples[count++] = chunk->samples[i];
                }
            }
            if (count > 0) {
                chunk->num_samples = count;
                return domainChunk;
            }
        }
    }

    return NULL;
}

SeriesFilterIterator *SeriesFilterIterator_New(AbstractIterator *input,
                                               FilterByValueArgs byValue,
                                               FilterByTSArgs ByTsArgs,
                                               bool rev) {
    SeriesFilterIterator *newIter = malloc(sizeof(SeriesFilterIterator));
    newIter->base.input = input;
    newIter->base.GetNext = SeriesFilterIterator_GetNextChunk;
    newIter->base.Close = SeriesFilterIterator_Close;
    newIter->byValueArgs = byValue;
    newIter->ByTsArgs = ByTsArgs;
    newIter->tsFilterIndex = 0;
    newIter->reverse = rev;
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

DomainChunk *AggregationIterator_GetNextChunk(struct AbstractIterator *iter) {
    AggregationIterator *self = (AggregationIterator *)iter;
    DomainChunk *aux_chunk;
    AggregationClass *aggregation = self->aggregation;
    void *aggregationContext = self->aggregationContext;

    AbstractIterator *input = iter->input;
    DomainChunk *domainChunk = input->GetNext(input);
    Chunk *chunk;
    double value;
    if (!domainChunk || domainChunk->chunk.num_samples == 0) {
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
        timestamp_t init_ts = domainChunk->chunk.samples[0].timestamp;
        self->aggregationLastTimestamp =
            calc_ts_bucket(init_ts, aggregationTimeDelta, self->timestampAlignment);
        self->initilized = true;
    }

    size_t agg_n_samples = 0;
    void (*appendValue)(void *, double) = aggregation->appendValue;
    u_int64_t contextScope = self->aggregationLastTimestamp + aggregationTimeDelta;
    while (domainChunk) {
        // currently if the query reversed the chunk will be already revered here
        assert(self->reverse == domainChunk->rev);
        chunk = &domainChunk->chunk;
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
            return domainChunk;
        }
        domainChunk = input->GetNext(input);
    }

_finalize:
    self->hasUnFinalizedContext = false;
    aux_chunk = GetTemporaryDomainChunk();
    aggregation->finalize(aggregationContext, &value);
    aux_chunk->chunk.samples[0].timestamp = self->aggregationLastTimestamp;
    aux_chunk->chunk.samples[0].value = value;
    aux_chunk->chunk.num_samples = 1;
    return aux_chunk;
}

void AggregationIterator_Close(struct AbstractIterator *iterator) {
    AggregationIterator *self = (AggregationIterator *)iterator;
    iterator->input->Close(iterator->input);
    self->aggregation->freeContext(self->aggregationContext);
    free(iterator);
}
