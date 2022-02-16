/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "filter_iterator.h"

#include "abstract_iterator.h"
#include "series_iterator.h"
#include "utils/arr.h"
#include <assert.h>
#include <math.h> /* ceil */

static inline bool check_sample_value(double value, FilterByValueArgs *byValueArgs) {
    if (value >= byValueArgs->min && value <= byValueArgs->max) {
        return true;
    } else {
        return false;
    }
}

typedef struct dfs_stack_val
{
    size_t si;
    size_t ei;
    size_t values_si;
    size_t values_ei;
} dfs_stack_val;

#define dfs_stack_val_init(_val, _si, _ei, _values_si, _values_ei)                                 \
    do {                                                                                           \
        (_val).si = (_si);                                                                         \
        (_val).ei = (_ei);                                                                         \
        (_val).values_si = (_values_si);                                                           \
        (_val).values_ei = (_values_ei);                                                           \
    } while (0)

// returns the number of matches
static size_t filterSamples(Samples *samples,
                            size_t num_samples,
                            const timestamp_t *tsVals,
                            size_t values_si,
                            size_t values_ei) {
    size_t count = 0;
    dfs_stack_val *dfs_stack =
        array_new(dfs_stack_val, ceil(log(num_samples)) + 1); // + 1 is for one left child
    dfs_stack_val first_frame = {
        .si = 0, .ei = num_samples - 1, .values_si = values_si, .values_ei = values_ei
    };
    dfs_stack_val left_frame, right_frame;
    array_append(dfs_stack, first_frame);
    dfs_stack_val cur_frame;
    bool found_left, found_right;
    while (array_len(dfs_stack) > 0) {
        cur_frame = array_pop(dfs_stack);
        if (cur_frame.si == cur_frame.ei) {
            assert((num_samples <= 1) || cur_frame.values_ei == cur_frame.values_si);
            for (size_t i = cur_frame.values_si; i <= cur_frame.values_ei; ++i) {
                const timestamp_t sample_ts = samples->timestamps[cur_frame.si];
                if (sample_ts == tsVals[i]) {
                    double value = samples->values[cur_frame.si];
                    samples->timestamps[count] = sample_ts;
                    samples->values[count] = value;
                    ++(count);
                    break;
                }
            }
            continue;
        }

        const size_t mid = (cur_frame.si + cur_frame.ei) / 2;

        // find tsVals that fit into left bucket
        found_left = false;
        size_t _siVals = cur_frame.values_si, _eiVals;

        while (_siVals <= cur_frame.values_ei &&
               tsVals[_siVals] < samples->timestamps[cur_frame.si]) {
            ++_siVals;
        }

        _eiVals = _siVals;
        while (_eiVals <= cur_frame.values_ei && tsVals[_eiVals] <= samples->timestamps[mid]) {
            found_left = true;
            ++_eiVals;
        }

        if (found_left) {
            dfs_stack_val_init(left_frame, cur_frame.si, mid, _siVals, _eiVals - 1);
        }

        // find tsVals that fit into right bucket
        found_right = false;
        _siVals = _eiVals;
        while (_siVals <= cur_frame.values_ei && tsVals[_siVals] < samples->timestamps[mid + 1]) {
            ++_siVals;
        }

        _eiVals = _siVals;
        while (_eiVals <= cur_frame.values_ei &&
               tsVals[_eiVals] <= samples->timestamps[cur_frame.ei]) {
            found_right = true;
            ++_eiVals;
        }

        if (found_right) {
            dfs_stack_val_init(right_frame, mid + 1, cur_frame.ei, _siVals, _eiVals - 1);
        }

        if (found_right) {
            array_append(dfs_stack, right_frame);
        }

        if (found_left) {
            array_append(dfs_stack, left_frame);
        }
    }

    return count;
}

DomainChunk *SeriesFilterTSIterator_GetNextChunk(struct AbstractIterator *base) {
    SeriesFilterTSIterator *self = (SeriesFilterTSIterator *)base;
    DomainChunk *domainChunk;
    size_t count = 0;
    assert(self->ByTsArgs.hasValue);

    if (self->tsFilterIndex == self->ByTsArgs.count) {
        return NULL;
    }
    while ((domainChunk = self->base.input->GetNext(self->base.input))) {
        assert(!domainChunk->rev); // the impl assumes that the chunk isn't reversed
        count = filterSamples(&domainChunk->samples,
                              domainChunk->num_samples,
                              self->ByTsArgs.values,
                              self->tsFilterIndex,
                              self->ByTsArgs.count - 1);
        if (count > 0) {
            domainChunk->num_samples = count;
            if (unlikely(self->reverse)) {
                reverseDomainChunk(domainChunk);
                self->ByTsArgs.count -= count;
            } else {
                self->tsFilterIndex += count; // at least count samples consumed
            }
            return domainChunk;
        }
    }

    return NULL;
}

SeriesFilterTSIterator *SeriesFilterTSIterator_New(AbstractIterator *input,
                                                   FilterByTSArgs ByTsArgs,
                                                   bool rev) {
    SeriesFilterTSIterator *newIter = malloc(sizeof(SeriesFilterTSIterator));
    newIter->base.input = input;
    newIter->base.GetNext = SeriesFilterTSIterator_GetNextChunk;
    newIter->base.Close = SeriesFilterIterator_Close;
    newIter->ByTsArgs = ByTsArgs;
    newIter->tsFilterIndex = 0;
    newIter->reverse = rev;
    return newIter;
}

void SeriesFilterIterator_Close(struct AbstractIterator *iterator) {
    iterator->input->Close(iterator->input);
    free(iterator);
}

DomainChunk *SeriesFilterValIterator_GetNextChunk(struct AbstractIterator *base) {
    SeriesFilterValIterator *self = (SeriesFilterValIterator *)base;
    DomainChunk *domainChunk;
    size_t i, count = 0;
    assert(self->byValueArgs.hasValue);

    while ((domainChunk = self->base.input->GetNext(self->base.input))) {
        // currently if the query reversed the chunk will be already reversed here
        // assert(self->reverse == domainChunk->rev);
        for (i = 0; i < domainChunk->num_samples; ++i) {
            if (check_sample_value(domainChunk->samples.values[i], &self->byValueArgs)) {
                domainChunk->samples.timestamps[count] = domainChunk->samples.timestamps[i];
                domainChunk->samples.values[count] = domainChunk->samples.values[i];
                ++count;
            }
        }
        if (count > 0) {
            domainChunk->num_samples = count;
            return domainChunk;
        }
    }

    return NULL;
}

SeriesFilterValIterator *SeriesFilterValIterator_New(AbstractIterator *input,
                                                     FilterByValueArgs byValue) {
    SeriesFilterValIterator *newIter = malloc(sizeof(SeriesFilterValIterator));
    newIter->base.input = input;
    newIter->base.GetNext = SeriesFilterValIterator_GetNextChunk;
    newIter->base.Close = SeriesFilterIterator_Close;
    newIter->byValueArgs = byValue;
    return newIter;
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
    iter->aux_chunk = allocateDomainChunk();
    ReallocDomainChunk(iter->aux_chunk, 1);
    return iter;
}

static inline void finalizeBucket(Samples *samples, size_t index, const AggregationIterator *self) {
    self->aggregation->finalize(self->aggregationContext, &samples->values[index]);
    samples->timestamps[index] = self->aggregationLastTimestamp;
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

// assumes num of samples > si + 1, returns -1 when no such an index
static int64_t findLastIndexbeforeTS(const DomainChunk *chunk, timestamp_t timestamp, int64_t si) {
    timestamp_t *timestamps = chunk->samples.timestamps;
    int64_t h = chunk->num_samples - 1;
    if (unlikely(timestamps[si] >= timestamp)) {
        // the first sample of the current chunk closes prev chunk bucket
        assert(si == 0);
        return -1;
    }
    if (timestamps[h] < timestamp) { // range of size 1 will be handled here
        return h;
    }

    int64_t m, l = si;
    assert(h > l);      // from here h > l
    while (l < h - 1) { // if l == h-1 it means l has the result
        m = (l + h) / 2;
        if (timestamps[m] < timestamp) {
            l = m;
        } else {
            h = m;
        }
    }

    return l;
}

DomainChunk *AggregationIterator_GetNextChunk(struct AbstractIterator *iter) {
    AggregationIterator *self = (AggregationIterator *)iter;
    AggregationClass *aggregation = self->aggregation;
    void *aggregationContext = self->aggregationContext;

    AbstractIterator *input = iter->input;
    DomainChunk *domainChunk = input->GetNext(input);
    double value;
    if (!domainChunk || domainChunk->num_samples == 0) {
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
        timestamp_t init_ts = domainChunk->samples.timestamps[0];
        self->aggregationLastTimestamp =
            calc_ts_bucket(init_ts, aggregationTimeDelta, self->timestampAlignment);
        self->initilized = true;
    }

    extern AggregationClass aggMax;
    size_t agg_n_samples = 0;
    void (*appendValue)(void *, double) = aggregation->appendValue;
    u_int64_t contextScope = self->aggregationLastTimestamp + aggregationTimeDelta;
    int64_t si, ei;
    while (domainChunk) {
        // currently if the query reversed the chunk will be already revered here
        assert(self->reverse == domainChunk->rev);
        n_samples = domainChunk->num_samples;
        if (self->aggregation == &aggMax &&
            !is_reserved) { // Currently only implemented vectorization for specific case
            si = 0;
            while (si < n_samples) {
                ei = findLastIndexbeforeTS(domainChunk, contextScope, si);
                if (likely(ei >= 0)) {
                    aggregation->appendValueVec(
                        aggregationContext, domainChunk->samples.values, si, ei);
                    si = ei + 1;
                }
                sample.timestamp =
                    domainChunk->samples.timestamps[si]; // store sample cause we aggregate in place
                sample.value =
                    domainChunk->samples.values[si]; // store sample cause we aggregate in place
                if (si <
                    n_samples) { // if si == n_samples need to check next chunk for more samples
                    assert(domainChunk->samples.timestamps[si] >= contextScope);
                    finalizeBucket(&domainChunk->samples, agg_n_samples++, self);
                    self->aggregationLastTimestamp = calc_ts_bucket(
                        sample.timestamp, aggregationTimeDelta, self->timestampAlignment);
                    contextScope = self->aggregationLastTimestamp + aggregationTimeDelta;
                }
                if (unlikely(ei < 0)) {
                    appendValue(aggregationContext, sample.value);
                    si++;
                }
            }
        } else {
            for (size_t i = 0; i < n_samples; ++i) {
                sample.timestamp =
                    domainChunk->samples.timestamps[i]; // store sample cause we aggregate in place
                sample.value =
                    domainChunk->samples.values[i]; // store sample cause we aggregate in place
                // (1) aggregationTimeDelta > 0,
                // (2) self->aggregationLastTimestamp > chunk->samples.timestamp[0] -
                // aggregationTimeDelta (3) self->aggregationLastTimestamp = samples.timestamps[0]
                // - mod where 0 <= mod from (1)+(2) contextScope > chunk->samples.timestamps[0]
                // from (3) chunk->samples.timestamps[0] >= self->aggregationLastTimestamp so the
                // following condition should always be false on the first iteration
                if ((is_reserved == FALSE && sample.timestamp >= contextScope) ||
                    (is_reserved == TRUE && sample.timestamp < self->aggregationLastTimestamp)) {
                    finalizeBucket(&domainChunk->samples, agg_n_samples++, self);
                    self->aggregationLastTimestamp = calc_ts_bucket(
                        sample.timestamp, aggregationTimeDelta, self->timestampAlignment);
                    contextScope = self->aggregationLastTimestamp + aggregationTimeDelta;
                }

                appendValue(aggregationContext, sample.value);
            }
        }

        if (agg_n_samples > 0) {
            domainChunk->num_samples = agg_n_samples;
            return domainChunk;
        }
        domainChunk = input->GetNext(input);
    }

_finalize:
    self->hasUnFinalizedContext = false;
    aggregation->finalize(aggregationContext, &value);
    self->aux_chunk->samples.timestamps[0] = self->aggregationLastTimestamp;
    self->aux_chunk->samples.values[0] = value;
    self->aux_chunk->num_samples = 1;
    return self->aux_chunk;
}

void AggregationIterator_Close(struct AbstractIterator *iterator) {
    AggregationIterator *self = (AggregationIterator *)iterator;
    iterator->input->Close(iterator->input);
    self->aggregation->freeContext(self->aggregationContext);
    FreeDomainChunk(self->aux_chunk, true);
    free(iterator);
}
