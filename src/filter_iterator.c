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

static inline timestamp_t calc_bucket_ts(BucketTimestamp bucketTS,
                                         timestamp_t ts,
                                         int64_t TimeDelta) {
    switch (bucketTS) {
        case BucketStartTimestamp:
            return ts;
        case BucketMidTimestamp:
            return ts + TimeDelta / 2;
        case BucketEndTimestamp:
            return ts + TimeDelta;
        default:
            assert(false);
    }
}

// returns the number of matches
static size_t filterSamples(Samples *samples,
                            const timestamp_t *tsVals,
                            size_t values_si,
                            size_t values_ei) {
    size_t count = 0;
    size_t num_samples = samples->num_samples;
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

    array_free(dfs_stack);
    return count;
}

EnrichedChunk *SeriesFilterTSIterator_GetNextChunk(struct AbstractIterator *base) {
    SeriesFilterTSIterator *self = (SeriesFilterTSIterator *)base;
    EnrichedChunk *enrichedChunk;
    size_t count = 0;
    assert(self->ByTsArgs.hasValue);

    if (self->tsFilterIndex == self->ByTsArgs.count) {
        return NULL;
    }
    while ((enrichedChunk = self->base.input->GetNext(self->base.input))) {
        assert(!enrichedChunk->rev); // the impl assumes that the chunk isn't reversed
        count = filterSamples(&enrichedChunk->samples,
                              self->ByTsArgs.values,
                              self->tsFilterIndex,
                              self->ByTsArgs.count - 1);
        if (count > 0) {
            enrichedChunk->samples.num_samples = count;
            if (unlikely(self->reverse)) {
                reverseEnrichedChunk(enrichedChunk);
                self->ByTsArgs.count -= count;
            } else {
                self->tsFilterIndex += count; // at least count samples consumed
            }
            return enrichedChunk;
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

EnrichedChunk *SeriesFilterValIterator_GetNextChunk(struct AbstractIterator *base) {
    SeriesFilterValIterator *self = (SeriesFilterValIterator *)base;
    EnrichedChunk *enrichedChunk;
    size_t i, count = 0;
    assert(self->byValueArgs.hasValue);

    while ((enrichedChunk = self->base.input->GetNext(self->base.input))) {
        // currently if the query reversed the chunk will be already reversed here
        // assert(self->reverse == enrichedChunk->rev);
        for (i = 0; i < enrichedChunk->samples.num_samples; ++i) {
            if (check_sample_value(enrichedChunk->samples.values[i], &self->byValueArgs)) {
                enrichedChunk->samples.timestamps[count] = enrichedChunk->samples.timestamps[i];
                enrichedChunk->samples.values[count] = enrichedChunk->samples.values[i];
                ++count;
            }
        }
        if (count > 0) {
            enrichedChunk->samples.num_samples = count;
            return enrichedChunk;
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
                                             bool reverse,
                                             bool empty,
                                             BucketTimestamp bucketTS,
                                             Series *series) {
    AggregationIterator *iter = malloc(sizeof(AggregationIterator));
    iter->base.GetNext = AggregationIterator_GetNextChunk;
    iter->base.Close = AggregationIterator_Close;
    iter->base.input = input;
    iter->aggregation = aggregation;
    iter->timestampAlignment = timestampAlignment;
    iter->aggregationTimeDelta = aggregationTimeDelta;
    iter->aggregationContext = iter->aggregation->createContext(reverse);
    iter->aggregationLastTimestamp = 0;
    iter->hasUnFinalizedContext = false;
    iter->reverse = reverse;
    iter->series = series;
    iter->initilized = false;
    iter->empty = empty;
    iter->bucketTS = bucketTS;
    iter->aux_chunk = NewEnrichedChunk();
    ReallocSamplesArray(&iter->aux_chunk->samples, 1);
    ResetEnrichedChunk(iter->aux_chunk);
    return iter;
}

static inline void finalizeBucket(Samples *samples, size_t index, const AggregationIterator *self) {
    self->aggregation->finalize(self->aggregationContext, &samples->values[index]);
    samples->timestamps[index] =
        calc_bucket_ts(self->bucketTS, self->aggregationLastTimestamp, self->aggregationTimeDelta);
    self->aggregation->resetContext(self->aggregationContext);
}

// assumes num of samples > si + 1, returns -1 when no such an index
static int64_t findLastIndexbeforeTS(const EnrichedChunk *chunk,
                                     timestamp_t timestamp,
                                     int64_t si) {
    timestamp_t *timestamps = chunk->samples.timestamps;
    int64_t h = chunk->samples.num_samples - 1;
    if (unlikely(timestamps[si] >= timestamp)) {
        // the first sample of the current range finalize prev chunk bucket
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

static void fillEmptyBuckets(Samples *samples,
                             size_t *write_index,
                             timestamp_t first_bucket_ts,
                             timestamp_t end_bucket_ts,
                             timestamp_t agg_time_delta,
                             const AggregationIterator *self,
                             bool reversed,
                             int64_t *read_index) {
    int64_t _read_index = *read_index + 1; // Cause we already stored the sample in read_index
    if (reversed) {
        first_bucket_ts -=
            2 * agg_time_delta; // the first bucket in case of reversed is 2 deltas ahead
        __SWAP(end_bucket_ts, first_bucket_ts);
    }
    assert((end_bucket_ts - first_bucket_ts) % agg_time_delta == 0);
    size_t n_empty_buckets = (end_bucket_ts - first_bucket_ts) / agg_time_delta;
    if (n_empty_buckets == 0) {
        return;
    }

    // We are aggregating in-place, make sure not to override unprocessed data
    if (*write_index + n_empty_buckets - 1 >= _read_index) {
        timestamp_t n_read_samples_left = samples->num_samples - _read_index;
        timestamp_t n_samples_needed = *write_index + n_empty_buckets + n_read_samples_left;
        size_t padding = samples->timestamps - samples->og_timestamps;

        // ensure we have space in the samples array
        if (n_samples_needed > samples->size) {
            ReallocSamplesArray(samples, n_samples_needed);
        }

        // copy the already aggregated and the yet to be read samples to the new place
        // even when reallocation didn't take place we need to utilize the full array
        memmove(samples->og_timestamps,
                samples->og_timestamps + padding,
                *write_index * sizeof(samples->og_timestamps[0]));
        memmove(samples->og_values,
                samples->og_values + padding,
                *write_index * sizeof(samples->og_values[0]));
        memmove(samples->og_timestamps + *write_index + n_empty_buckets,
                samples->og_timestamps + padding + _read_index,
                n_read_samples_left * sizeof(samples->og_timestamps[0]));
        memmove(samples->og_values + *write_index + n_empty_buckets,
                samples->og_values + padding + _read_index,
                n_read_samples_left * sizeof(samples->og_values[0]));

        // update the read index and num of samples
        _read_index = *write_index + n_empty_buckets;
        *read_index = _read_index - 1; // - 1 cause outside this function we inc by 1
        samples->num_samples = _read_index + n_read_samples_left;
    }

    timestamp_t cur_ts = (reversed) ? end_bucket_ts : first_bucket_ts;
    for (size_t i = 0; i < n_empty_buckets; ++i) {
        self->aggregation->finalizeEmpty(&samples->values[*write_index]);
        samples->timestamps[*write_index] =
            calc_bucket_ts(self->bucketTS, cur_ts, self->aggregationTimeDelta);
        if (reversed) {
            cur_ts -= agg_time_delta;
        } else {
            cur_ts += agg_time_delta;
        }
        (*write_index)++;
    }

    return;
}

EnrichedChunk *AggregationIterator_GetNextChunk(struct AbstractIterator *iter) {
    AggregationIterator *self = (AggregationIterator *)iter;
    AggregationClass *aggregation = self->aggregation;
    void *aggregationContext = self->aggregationContext;
    u_int64_t aggregationTimeDelta = self->aggregationTimeDelta;
    bool is_reserved = self->reverse;
    Sample sample;

    AbstractIterator *input = iter->input;
    EnrichedChunk *enrichedChunk = input->GetNext(input);
    double value;

    if (!enrichedChunk || enrichedChunk->samples.num_samples == 0) {
        if (self->hasUnFinalizedContext) {
            goto _finalize;
        } else {
            return NULL;
        }
    }

    self->hasUnFinalizedContext = true;

    if (!self->initilized) {
        timestamp_t init_ts = enrichedChunk->samples.timestamps[0];
        self->aggregationLastTimestamp =
            CalcBucketStart(init_ts, aggregationTimeDelta, self->timestampAlignment);
        self->initilized = true;
        if (aggregation->addBucketParams) {
            aggregation->addBucketParams(aggregationContext,
                                         BucketStartNormalize(self->aggregationLastTimestamp),
                                         self->aggregationLastTimestamp + aggregationTimeDelta);
        }

        if (aggregation->addPrevBucketLastSample && !((!is_reserved) && init_ts == 0)) {
            RangeArgs args = { .aggregationArgs = { 0 },
                               .filterByValueArgs = { 0 },
                               .filterByTSArgs = { 0 },
                               .startTimestamp = is_reserved ? init_ts + 1 : 0,
                               .endTimestamp = is_reserved ? UINT64_MAX : init_ts - 1,
                               .latest = false };
            AbstractSampleIterator *sample_iterator =
                SeriesCreateSampleIterator(self->series, &args, !is_reserved, true);
            if (sample_iterator->GetNext(sample_iterator, &sample) == CR_OK) {
                aggregation->addPrevBucketLastSample(
                    aggregationContext, sample.value, sample.timestamp);
            }
            sample_iterator->Close(sample_iterator);
        }
    }

    extern AggregationClass aggMax;
    size_t agg_n_samples = 0;
    void (*appendValue)(void *, double, timestamp_t) = aggregation->appendValue;
    u_int64_t contextScope = self->aggregationLastTimestamp + aggregationTimeDelta;
    self->aggregationLastTimestamp = BucketStartNormalize(self->aggregationLastTimestamp);
    int64_t si, ei;
    while (enrichedChunk) {
        // currently if the query reversed the chunk will be already revered here
        assert(self->reverse == enrichedChunk->rev);
        Samples *samples = &enrichedChunk->samples;
        si = 0;
        if (self->aggregation == &aggMax &&
            !is_reserved) { // Currently only implemented vectorization for specific case
            while (si < samples->num_samples) {
                ei = findLastIndexbeforeTS(enrichedChunk, contextScope, si);
                if (likely(ei >= 0)) {
                    aggregation->appendValueVec(
                        aggregationContext, enrichedChunk->samples.values, si, ei);
                    si = ei + 1;
                } // else ei < 0: only need to finalize the previous bucket and update contextScope
                if (si < samples->num_samples) { // if si == num_samples need to check next chunk
                                                 // for more samples
                    sample.timestamp =
                        enrichedChunk->samples
                            .timestamps[si]; // store sample cause we aggregate in place
                    sample.value = enrichedChunk->samples
                                       .values[si]; // store sample cause we aggregate in place
                    assert(enrichedChunk->samples.timestamps[si] >= contextScope);
                    finalizeBucket(&enrichedChunk->samples, agg_n_samples++, self);
                    self->aggregationLastTimestamp = CalcBucketStart(
                        sample.timestamp, aggregationTimeDelta, self->timestampAlignment);
                    if (self->empty) {
                        fillEmptyBuckets(&enrichedChunk->samples,
                                         &agg_n_samples,
                                         contextScope,
                                         self->aggregationLastTimestamp,
                                         aggregationTimeDelta,
                                         self,
                                         is_reserved,
                                         &si);
                    }
                    contextScope = self->aggregationLastTimestamp + aggregationTimeDelta;
                    self->aggregationLastTimestamp =
                        BucketStartNormalize(self->aggregationLastTimestamp);

                    // append sample and inc si cause we aggregate in place
                    appendValue(aggregationContext, sample.value, sample.timestamp);
                    si++;
                }
            }
        } else {
            while (si < samples->num_samples) {
                sample.timestamp = enrichedChunk->samples
                                       .timestamps[si]; // store sample cause we aggregate in place
                sample.value =
                    enrichedChunk->samples.values[si]; // store sample cause we aggregate in place
                // (1) aggregationTimeDelta > 0,
                // (2) self->aggregationLastTimestamp > chunk->samples.timestamp[0] -
                // aggregationTimeDelta (3) self->aggregationLastTimestamp = samples.timestamps[0]
                // - mod where 0 <= mod from (1)+(2) contextScope > chunk->samples.timestamps[0]
                // from (3) chunk->samples.timestamps[0] >= self->aggregationLastTimestamp so the
                // following condition should always be false on the first iteration
                if ((is_reserved == FALSE && sample.timestamp >= contextScope) ||
                    (is_reserved == TRUE && sample.timestamp < self->aggregationLastTimestamp)) {
                    if (aggregation->addNextBucketFirstSample) {
                        aggregation->addNextBucketFirstSample(
                            aggregationContext, sample.value, sample.timestamp);
                    }

                    Sample last_sample;
                    if (aggregation->addPrevBucketLastSample) {
                        aggregation->getLastSample(aggregationContext, &last_sample);
                    }
                    finalizeBucket(&enrichedChunk->samples, agg_n_samples++, self);
                    self->aggregationLastTimestamp = CalcBucketStart(
                        sample.timestamp, aggregationTimeDelta, self->timestampAlignment);
                    if (self->empty) {
                        fillEmptyBuckets(&enrichedChunk->samples,
                                         &agg_n_samples,
                                         contextScope,
                                         self->aggregationLastTimestamp,
                                         aggregationTimeDelta,
                                         self,
                                         is_reserved,
                                         &si);
                    }
                    contextScope = self->aggregationLastTimestamp + aggregationTimeDelta;
                    self->aggregationLastTimestamp =
                        BucketStartNormalize(self->aggregationLastTimestamp);
                    if (aggregation->addPrevBucketLastSample) {
                        aggregation->addPrevBucketLastSample(
                            aggregationContext, last_sample.value, last_sample.timestamp);
                    }

                    if (aggregation->addBucketParams) {
                        aggregation->addBucketParams(
                            aggregationContext, self->aggregationLastTimestamp, contextScope);
                    }
                }

                appendValue(aggregationContext, sample.value, sample.timestamp);
                si++;
            }
        }

        if (agg_n_samples > 0) {
            enrichedChunk->samples.num_samples = agg_n_samples;
            return enrichedChunk;
        }
        enrichedChunk = input->GetNext(input);
    }

_finalize:
    self->hasUnFinalizedContext = false;
    if (aggregation->addNextBucketFirstSample) {
        Sample last_sample;
        aggregation->getLastSample(aggregationContext, &last_sample);
        if (!(is_reserved && last_sample.timestamp == 0)) {
            RangeArgs args = { .aggregationArgs = { 0 },
                               .filterByValueArgs = { 0 },
                               .filterByTSArgs = { 0 },
                               .startTimestamp = is_reserved ? 0 : last_sample.timestamp + 1,
                               .endTimestamp = is_reserved ? last_sample.timestamp - 1 : UINT64_MAX,
                               .latest = false };
            AbstractSampleIterator *sample_iterator =
                SeriesCreateSampleIterator(self->series, &args, is_reserved, true);
            if (sample_iterator->GetNext(sample_iterator, &sample) == CR_OK) {
                aggregation->addNextBucketFirstSample(
                    aggregationContext, sample.value, sample.timestamp);
            }
            sample_iterator->Close(sample_iterator);
        }
    }
    aggregation->finalize(aggregationContext, &value); // last bucket, no need to addBucketParams
    self->aux_chunk->samples.timestamps[0] =
        calc_bucket_ts(self->bucketTS, self->aggregationLastTimestamp, self->aggregationTimeDelta);
    self->aux_chunk->samples.values[0] = value;
    self->aux_chunk->samples.num_samples = 1;
    return self->aux_chunk;
}

void AggregationIterator_Close(struct AbstractIterator *iterator) {
    AggregationIterator *self = (AggregationIterator *)iterator;
    iterator->input->Close(iterator->input);
    self->aggregation->freeContext(self->aggregationContext);
    FreeEnrichedChunk(self->aux_chunk);
    free(iterator);
}
