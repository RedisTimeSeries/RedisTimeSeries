/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
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
    size_t filter_si;
    size_t filter_ei;
} dfs_stack_val;

#define dfs_stack_val_init(_val, _si, _ei, _filter_si, _filter_ei)                                 \
    do {                                                                                           \
        (_val).si = (_si);                                                                         \
        (_val).ei = (_ei);                                                                         \
        (_val).filter_si = (_filter_si);                                                           \
        (_val).filter_ei = (_filter_ei);                                                           \
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
                            const timestamp_t *filter,
                            size_t filter_si,
                            size_t filter_ei) {
    size_t count = 0;
    size_t num_samples = samples->num_samples;
    dfs_stack_val *dfs_stack =
        array_new(dfs_stack_val, ceil(log(num_samples)) + 1); // + 1 is for one left child
    dfs_stack_val first_frame = {
        .si = 0, .ei = num_samples - 1, .filter_si = filter_si, .filter_ei = filter_ei
    };
    dfs_stack_val left_frame, right_frame;
    array_append(dfs_stack, first_frame);
    dfs_stack_val cur_frame;
    bool found_left, found_right;
    while (array_len(dfs_stack) > 0) {
        cur_frame = array_pop(dfs_stack);
        if (cur_frame.si == cur_frame.ei) {
            assert((num_samples <= 1) || cur_frame.filter_ei == cur_frame.filter_si);
            for (size_t i = cur_frame.filter_si; i <= cur_frame.filter_ei; ++i) {
                const timestamp_t sample_ts = samples->timestamps[cur_frame.si];
                if (sample_ts == filter[i]) {
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

        // find filter values that fit into left bucket
        found_left = false;
        size_t _filter_si = cur_frame.filter_si, _filter_ei;

        while (_filter_si <= cur_frame.filter_ei &&
               filter[_filter_si] < samples->timestamps[cur_frame.si]) {
            ++_filter_si;
        }

        _filter_ei = _filter_si;
        while (_filter_ei <= cur_frame.filter_ei &&
               filter[_filter_ei] <= samples->timestamps[mid]) {
            found_left = true;
            ++_filter_ei;
        }

        if (found_left) {
            dfs_stack_val_init(left_frame, cur_frame.si, mid, _filter_si, _filter_ei - 1);
        }

        // find filter that fit into right bucket
        found_right = false;
        _filter_si = _filter_ei;
        while (_filter_si <= cur_frame.filter_ei &&
               filter[_filter_si] < samples->timestamps[mid + 1]) {
            ++_filter_si;
        }

        _filter_ei = _filter_si;
        while (_filter_ei <= cur_frame.filter_ei &&
               filter[_filter_ei] <= samples->timestamps[cur_frame.ei]) {
            found_right = true;
            ++_filter_ei;
        }

        if (found_right) {
            dfs_stack_val_init(right_frame, mid + 1, cur_frame.ei, _filter_si, _filter_ei - 1);
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
    while ((enrichedChunk = self->base.input->GetNext(self->base.input)) &&
           enrichedChunk->samples.num_samples > 0) {
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
                                             size_t numAggregations,
                                             AggregationClass **aggregations,
                                             int64_t aggregationTimeDelta,
                                             timestamp_t timestampAlignment,
                                             bool reverse,
                                             bool empty,
                                             BucketTimestamp bucketTS,
                                             Series *series,
                                             api_timestamp_t startTimestamp,
                                             api_timestamp_t endTimestamp) {
    AggregationIterator *iter = malloc(sizeof(AggregationIterator));
    iter->base.GetNext = AggregationIterator_GetNextChunk;
    iter->base.Close = AggregationIterator_Close;
    iter->base.input = input;
    iter->numAggregations = numAggregations;
    iter->aggregations = malloc(numAggregations * sizeof(AggregationClass *));
    iter->aggregationContexts = malloc(numAggregations * sizeof(void *));
    for (size_t i = 0; i < numAggregations; i++) {
        iter->aggregations[i] = aggregations[i];
        iter->aggregationContexts[i] = aggregations[i]->createContext(reverse);
    }
    iter->timestampAlignment = timestampAlignment;
    iter->aggregationTimeDelta = aggregationTimeDelta;
    iter->aggregationLastTimestamp = 0;
    iter->hasUnFinalizedContext = false;
    iter->reverse = reverse;
    iter->series = series;
    iter->initialized = false;
    iter->empty = empty;
    iter->bucketTS = bucketTS;
    iter->aux_chunk = NewEnrichedChunk();
    iter->aux_chunk->samples.values_per_sample = numAggregations;
    iter->startTimestamp = startTimestamp;
    iter->endTimestamp = endTimestamp;
    iter->hasTwa = false;
    for (size_t i = 0; i < numAggregations; i++) {
        if (aggregations[i]->type == TS_AGG_TWA) {
            iter->hasTwa = true;
            break;
        }
    }
    iter->handled_twa_empty_prefix = false;
    iter->handled_twa_empty_suffix = false;
    iter->prev_ts = DC;
    iter->validSamplesInBucket = false;
    ReallocSamplesArray(&iter->aux_chunk->samples, 1);
    ResetEnrichedChunk(iter->aux_chunk);
    return iter;
}

static size_t twa_get_samples_from_left(timestamp_t cur_ts,
                                        const AggregationIterator *self,
                                        Sample *sample_left,
                                        Sample *sample_leftLeft);
static size_t twa_get_samples_from_right(timestamp_t cur_ts,
                                         const AggregationIterator *self,
                                         Sample *sample_right,
                                         Sample *sample_rightRight);
timestamp_t twa_calc_ta(bool reverse,
                        timestamp_t bucketStartTS,
                        timestamp_t bucketEndTS,
                        timestamp_t rangeStart,
                        timestamp_t rangeEnd);
timestamp_t twa_calc_tb(bool reverse,
                        timestamp_t bucketStartTS,
                        timestamp_t bucketEndTS,
                        timestamp_t rangeStart,
                        timestamp_t rangeEnd);

static void twa_calc_empty_bucket_val(timestamp_t ta,
                                      timestamp_t tb,
                                      const Sample *sample_before,
                                      const Sample *sample_befBefore,
                                      const Sample *sample_after,
                                      const Sample *sample_afAfter,
                                      size_t n_samples_before,
                                      size_t n_samples_after,
                                      double *value) {
    bool is_empty_bucket = true;
    bool has_before_and_after = false;

    if (n_samples_before > 1) {
        timestamp_t delta = sample_before->timestamp - sample_befBefore->timestamp;
        if (sample_before->timestamp + delta > ta) {
            is_empty_bucket = false;
        }
    }
    if (n_samples_after > 1) {
        timestamp_t delta = sample_afAfter->timestamp - sample_after->timestamp;
        if (tb + delta > sample_after->timestamp) {
            is_empty_bucket = false;
        }
    }
    if (n_samples_after != 0 && n_samples_before != 0) {
        is_empty_bucket = false;
        has_before_and_after = true;
    }

    if (is_empty_bucket) {
        *value = NAN;
    } else if (has_before_and_after) {
        double delta_val = (sample_after->value - sample_before->value);
        double delta_ts = (sample_after->timestamp - sample_before->timestamp);
        double va = sample_before->value + ((ta - sample_before->timestamp) * delta_val) / delta_ts;
        double vb = sample_before->value + ((tb - sample_before->timestamp) * delta_val) / delta_ts;
        *value = (va + vb) / 2.0;
    } else if (n_samples_after > 1) {
        timestamp_t delta = sample_afAfter->timestamp - sample_after->timestamp;
        if (tb + (delta / 2) <= sample_after->timestamp) {
            *value = NAN;
        } else {
            *value = sample_after->value;
        }
    } else { // n_samples_before > 1
        timestamp_t delta = sample_before->timestamp - sample_befBefore->timestamp;
        if (sample_before->timestamp + (delta / 2) <= ta) {
            *value = NAN;
        } else {
            *value = sample_before->value;
        }
    }
}

// Compute TWA value for a single empty/NaN-only bucket using surrounding samples.
static void twa_compute_empty_bucket_value(const AggregationIterator *self,
                                           timestamp_t bucket_ts,
                                           double *value) {
    Sample sample_before, sample_befBefore, sample_after, sample_afAfter;
    int64_t agg_time_delta = self->aggregationTimeDelta;

    timestamp_t ta = twa_calc_ta(
        false, bucket_ts, bucket_ts + agg_time_delta, self->startTimestamp, self->endTimestamp);
    timestamp_t tb = twa_calc_tb(
        false, bucket_ts, bucket_ts + agg_time_delta, self->startTimestamp, self->endTimestamp);

    size_t n_samples_before =
        twa_get_samples_from_left(ta, self, &sample_before, &sample_befBefore);
    size_t n_samples_after = twa_get_samples_from_right(tb, self, &sample_after, &sample_afAfter);

    twa_calc_empty_bucket_val(ta,
                              tb,
                              &sample_before,
                              &sample_befBefore,
                              &sample_after,
                              &sample_afAfter,
                              n_samples_before,
                              n_samples_after,
                              value);
}

// Returns true if bucket should be output, false if it should be skipped.
static inline bool finalizeBucket(Samples *samples, size_t index, AggregationIterator *self) {
    bool shouldBucketIgnored = !self->validSamplesInBucket;
    size_t numAggs = self->numAggregations;
    size_t vps = samples->values_per_sample;
    if (shouldBucketIgnored) {
        if (!self->empty) {
            for (size_t a = 0; a < numAggs; a++) {
                self->aggregations[a]->resetContext(self->aggregationContexts[a]);
            }
            self->validSamplesInBucket = false;
            return false;
        }
        for (size_t a = 0; a < numAggs; a++) {
            if (self->aggregations[a]->type == TS_AGG_TWA) {
                twa_compute_empty_bucket_value(
                    self, self->aggregationLastTimestamp, &samples->values[index * vps + a]);
            } else {
                self->aggregations[a]->finalizeEmpty(self->aggregationContexts[a],
                                                     &samples->values[index * vps + a]);
            }
        }
    } else {
        for (size_t a = 0; a < numAggs; a++) {
            self->aggregations[a]->finalize(self->aggregationContexts[a],
                                            &samples->values[index * vps + a]);
        }
    }
    samples->timestamps[index] =
        calc_bucket_ts(self->bucketTS, self->aggregationLastTimestamp, self->aggregationTimeDelta);
    for (size_t a = 0; a < numAggs; a++) {
        self->aggregations[a]->resetContext(self->aggregationContexts[a]);
    }
    self->validSamplesInBucket = false;
    return true;
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

static void fillEmptyBucketWithValueIncIter(size_t *write_index,
                                            timestamp_t *cur_ts,
                                            Samples *samples,
                                            timestamp_t ts,
                                            double value,
                                            const AggregationIterator *self,
                                            bool reversed) {
    size_t vps = samples->values_per_sample;
    samples->values[(*write_index) * vps] = value;
    samples->timestamps[*write_index] = ts;
    if (reversed) {
        (*cur_ts) -= self->aggregationTimeDelta;
    } else {
        (*cur_ts) += self->aggregationTimeDelta;
    }
    (*write_index)++;
}

static void fillEmptyBucketsWithDefaultVals(size_t *write_index,
                                            timestamp_t cur_ts,
                                            Samples *samples,
                                            const AggregationIterator *self,
                                            size_t n_empty_buckets,
                                            bool reversed) {
    size_t vps = samples->values_per_sample;
    for (size_t i = 0; i < n_empty_buckets; ++i) {
        timestamp_t ts = calc_bucket_ts(self->bucketTS, cur_ts, self->aggregationTimeDelta);
        for (size_t a = 0; a < self->numAggregations; a++) {
            self->aggregations[a]->finalizeEmpty(self->aggregationContexts[a],
                                                 &samples->values[(*write_index) * vps + a]);
        }
        samples->timestamps[*write_index] = ts;
        if (reversed) {
            cur_ts -= self->aggregationTimeDelta;
        } else {
            cur_ts += self->aggregationTimeDelta;
        }
        (*write_index)++;
    }
}

static size_t twa_get_samples_from_right(timestamp_t cur_ts,
                                         const AggregationIterator *self,
                                         Sample *sample_right,
                                         Sample *sample_rightRight) {
    size_t n_samples_right = 0;
    if (cur_ts < UINT64_MAX) {
        RangeArgs args = {
            .aggregationArgs = { 0 },
            .filterByValueArgs = { 0 },
            .filterByTSArgs = { 0 },
            .startTimestamp = cur_ts,
            .endTimestamp = UINT64_MAX,
            .latest = false,
        };
        AbstractSampleIterator *sample_iterator =
            SeriesCreateSampleIterator(self->series, &args, false, true);
        Sample sample;
        // Skip NaN samples - they shouldn't be used for interpolation
        while (sample_iterator->GetNext(sample_iterator, &sample) == CR_OK) {
            if (!isnan(sample.value)) {
                if (n_samples_right == 0) {
                    *sample_right = sample;
                    n_samples_right++;
                } else if (n_samples_right == 1) {
                    *sample_rightRight = sample;
                    n_samples_right++;
                    break;
                }
            }
        }
        sample_iterator->Close(sample_iterator);
    }
    return n_samples_right;
}

static size_t twa_get_samples_from_left(timestamp_t cur_ts,
                                        const AggregationIterator *self,
                                        Sample *sample_left,
                                        Sample *sample_leftLeft) {
    size_t n_samples_left = 0;
    if (cur_ts > 0) {
        RangeArgs args = {
            .aggregationArgs = { 0 },
            .filterByValueArgs = { 0 },
            .filterByTSArgs = { 0 },
            .startTimestamp = 0,
            .endTimestamp = cur_ts - 1,
            .latest = false,
        };
        AbstractSampleIterator *sample_iterator =
            SeriesCreateSampleIterator(self->series, &args, true, true);
        Sample sample;
        // Skip NaN samples - they shouldn't be used for interpolation
        // Note: iterator is reversed, so we get samples in descending timestamp order
        while (sample_iterator->GetNext(sample_iterator, &sample) == CR_OK) {
            if (!isnan(sample.value)) {
                if (n_samples_left == 0) {
                    *sample_left = sample;
                    n_samples_left++;
                } else if (n_samples_left == 1) {
                    *sample_leftLeft = sample;
                    n_samples_left++;
                    break;
                }
            }
        }
        sample_iterator->Close(sample_iterator);
    }
    return n_samples_left;
}

timestamp_t twa_calc_ta(bool reverse,
                        timestamp_t bucketStartTS,
                        timestamp_t bucketEndTS,
                        timestamp_t rangeStart,
                        timestamp_t rangeEnd) {
    if (!reverse) {
        return max(bucketStartTS, rangeStart);
    } else {
        return min(bucketEndTS, rangeEnd);
    }
}

timestamp_t twa_calc_tb(bool reverse,
                        timestamp_t bucketStartTS,
                        timestamp_t bucketEndTS,
                        timestamp_t rangeStart,
                        timestamp_t rangeEnd) {
    if (!reverse) {
        return min(bucketEndTS, rangeEnd);
    } else {
        return max(bucketStartTS, rangeStart);
    }
}

static void twa_fillEmptyBuckets(size_t *write_index,
                                 timestamp_t cur_ts,
                                 Samples *samples,
                                 const AggregationIterator *self,
                                 size_t n_empty_buckets,
                                 bool reversed) {
    Sample sample_before, sample_befBefore, sample_after, sample_afAfter;
    size_t n_samples_before = 0, n_samples_after = 0;
    timestamp_t ta, tb;
    int64_t agg_time_delta = self->aggregationTimeDelta;
    size_t vps = samples->values_per_sample;
    ta = twa_calc_ta(
        false, cur_ts, cur_ts + agg_time_delta, self->startTimestamp, self->endTimestamp);
    n_samples_before = twa_get_samples_from_left(ta, self, &sample_before, &sample_befBefore);
    n_samples_after = twa_get_samples_from_right(ta, self, &sample_after, &sample_afAfter);

    for (size_t i = 0; i < n_empty_buckets; ++i) {
        ta = twa_calc_ta(
            false, cur_ts, cur_ts + agg_time_delta, self->startTimestamp, self->endTimestamp);
        tb = twa_calc_tb(
            false, cur_ts, cur_ts + agg_time_delta, self->startTimestamp, self->endTimestamp);

        timestamp_t ts = calc_bucket_ts(self->bucketTS, cur_ts, self->aggregationTimeDelta);
        for (size_t a = 0; a < self->numAggregations; a++) {
            if (self->aggregations[a]->type == TS_AGG_TWA) {
                double val;
                twa_calc_empty_bucket_val(ta,
                                          tb,
                                          &sample_before,
                                          &sample_befBefore,
                                          &sample_after,
                                          &sample_afAfter,
                                          n_samples_before,
                                          n_samples_after,
                                          &val);
                samples->values[(*write_index) * vps + a] = val;
            } else {
                self->aggregations[a]->finalizeEmpty(self->aggregationContexts[a],
                                                     &samples->values[(*write_index) * vps + a]);
            }
        }
        samples->timestamps[*write_index] = ts;
        if (reversed) {
            cur_ts -= self->aggregationTimeDelta;
        } else {
            cur_ts += self->aggregationTimeDelta;
        }
        (*write_index)++;
    }
}

static int fillEmptyBuckets(Samples *samples,
                            size_t *write_index,
                            timestamp_t first_bucket_ts,
                            timestamp_t end_bucket_ts,
                            const AggregationIterator *self,
                            bool reversed,
                            int64_t *read_index) {
    int64_t agg_time_delta = self->aggregationTimeDelta;
    int64_t _read_index = *read_index + 1; // Cause we already stored the sample in read_index
    size_t vps = samples->values_per_sample;
    if (reversed) {
        __SWAP(end_bucket_ts, first_bucket_ts);
    }

    if (end_bucket_ts < first_bucket_ts) {
        return -1;
    }

    if ((end_bucket_ts - first_bucket_ts) % agg_time_delta != 0) {
        return -1;
    }

    size_t n_empty_buckets = ((end_bucket_ts - first_bucket_ts) / agg_time_delta) + 1;
    if (n_empty_buckets == 0) {
        return -1;
    }

    timestamp_t cur_ts = (reversed) ? end_bucket_ts : first_bucket_ts;

#ifndef PREFIX_SUFFIX_IMPL // The PM decided to disable it, as it might cause OOM and complicates
                           // users
    if (self->hasTwa) {
        Sample sample_before, sample_befBefore, sample_after, sample_afAfter;
        timestamp_t ta;
        int64_t agg_time_delta = self->aggregationTimeDelta;
        ta = twa_calc_ta(
            false, cur_ts, cur_ts + agg_time_delta, self->startTimestamp, self->endTimestamp);
        size_t n_samples_before =
            twa_get_samples_from_left(ta, self, &sample_before, &sample_befBefore);
        size_t n_samples_after =
            twa_get_samples_from_right(ta, self, &sample_after, &sample_afAfter);
        if (n_samples_before == 0 || n_samples_after == 0) { // the PM canceled cases 6 and 7
            return 0;
        }
    }
#endif // PREFIX_SUFFIX_IMPL

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
                samples->og_values + padding * vps,
                *write_index * vps * sizeof(samples->og_values[0]));
        memmove(samples->og_timestamps + *write_index + n_empty_buckets,
                samples->og_timestamps + padding + _read_index,
                n_read_samples_left * sizeof(samples->og_timestamps[0]));
        memmove(samples->og_values + (*write_index + n_empty_buckets) * vps,
                samples->og_values + (padding + _read_index) * vps,
                n_read_samples_left * vps * sizeof(samples->og_values[0]));

        // use compacted buffer so fillEmptyBuckets* write at correct indices (avoid overflow)
        samples->timestamps = samples->og_timestamps;
        samples->values = samples->og_values;

        _read_index = *write_index + n_empty_buckets;
        *read_index = _read_index - 1;
        samples->num_samples = _read_index + n_read_samples_left;
    }

    if (self->hasTwa) {
        twa_fillEmptyBuckets(write_index, cur_ts, samples, self, n_empty_buckets, reversed);
    } else {
        fillEmptyBucketsWithDefaultVals(
            write_index, cur_ts, samples, self, n_empty_buckets, reversed);
    }

    return 0;
}

#define TWA_EMPTY_RANGE(iter) (((iter)->empty) && ((iter)->hasTwa))

static inline void appendValueToAllContexts(AggregationIterator *self,
                                            double value,
                                            timestamp_t ts) {
    for (size_t a = 0; a < self->numAggregations; a++) {
        self->aggregations[a]->appendValue(self->aggregationContexts[a], value, ts);
    }
}

static inline Samples *ensureOutputSamples(AggregationIterator *self,
                                           EnrichedChunk *enrichedChunk,
                                           size_t needed_samples) {
    if (self->numAggregations > 1) {
        if (needed_samples >= self->aux_chunk->samples.size) {
            ReallocSamplesArray(&self->aux_chunk->samples, needed_samples + 16);
        }
        return &self->aux_chunk->samples;
    }
    return &enrichedChunk->samples;
}

EnrichedChunk *AggregationIterator_GetNextChunk(struct AbstractIterator *iter) {
    AggregationIterator *self = (AggregationIterator *)iter;
    AggregationClass *aggregation = self->aggregations[0];
    void *aggregationContext = self->aggregationContexts[0];
    uint64_t aggregationTimeDelta = self->aggregationTimeDelta;
    bool is_reversed = self->reverse;
    bool multiAgg = self->numAggregations > 1;
    Sample sample;

    AbstractIterator *input = iter->input;
    EnrichedChunk *enrichedChunk = input->GetNext(input);
    double value;
    size_t agg_n_samples = 0;
    int64_t si = 0, ei;

    if (!enrichedChunk || enrichedChunk->samples.num_samples == 0) {
        if (self->hasUnFinalizedContext) {
            goto _finalize;
        } else if (TWA_EMPTY_RANGE(self)) {
            if (!self->handled_twa_empty_prefix) {
                self->handled_twa_empty_prefix = true;
                self->handled_twa_empty_suffix = true; // The prefix in this case is also the suffix
                timestamp_t first_bucket = CalcBucketStart(
                    self->startTimestamp, aggregationTimeDelta, self->timestampAlignment);
                timestamp_t last_bucket = CalcBucketStart(
                    self->endTimestamp, aggregationTimeDelta, self->timestampAlignment);
                if (is_reversed) {
                    __SWAP(first_bucket, last_bucket);
                }
                si = -1;
                self->aux_chunk->samples.num_samples = 0;
                int err = fillEmptyBuckets(&self->aux_chunk->samples,
                                           &agg_n_samples,
                                           first_bucket,
                                           last_bucket,
                                           self,
                                           is_reversed,
                                           &si);
                if (err != 0) {
                    return NULL;
                }
                si++;
                self->aux_chunk->samples.num_samples = agg_n_samples;
                return self->aux_chunk;
            } else if (!self->handled_twa_empty_suffix) {
                self->handled_twa_empty_suffix = true;
                timestamp_t last_bucket =
                    CalcBucketStart(is_reversed ? self->startTimestamp : self->endTimestamp,
                                    aggregationTimeDelta,
                                    self->timestampAlignment);
                timestamp_t first_bucket =
                    CalcBucketStart(self->prev_ts, aggregationTimeDelta, self->timestampAlignment);
                if (!is_reversed) {
                    first_bucket += aggregationTimeDelta;
                    if (first_bucket > last_bucket) {
                        return NULL;
                    }
                } else {
                    if (first_bucket <= last_bucket) {
                        return NULL;
                    }
                    first_bucket =
                        max(0, (int64_t)((int64_t)first_bucket - (int64_t)aggregationTimeDelta));
                }
                si = -1;
                self->aux_chunk->samples.num_samples = 0;
                int err = fillEmptyBuckets(&self->aux_chunk->samples,
                                           &agg_n_samples,
                                           first_bucket,
                                           last_bucket,
                                           self,
                                           is_reversed,
                                           &si);
                if (err != 0) {
                    return NULL;
                }
                si++;
                self->aux_chunk->samples.num_samples = agg_n_samples;
                return self->aux_chunk;
            } else {
                return NULL;
            }
        } else {
            return NULL;
        }
    }

    if (TWA_EMPTY_RANGE(self) && !self->handled_twa_empty_prefix) {
        self->handled_twa_empty_prefix = true;
        timestamp_t first_bucket =
            CalcBucketStart(is_reversed ? self->endTimestamp : self->startTimestamp,
                            aggregationTimeDelta,
                            self->timestampAlignment);
        timestamp_t first_sample_ts = enrichedChunk->samples.timestamps[0];
        timestamp_t last_bucket =
            CalcBucketStart(first_sample_ts, aggregationTimeDelta, self->timestampAlignment);
        bool has_empty_buckets = true;
        if (!is_reversed) {
            if (first_bucket >= last_bucket) {
                has_empty_buckets = false;
            }
            last_bucket = max(0, (int64_t)((int64_t)last_bucket - (int64_t)aggregationTimeDelta));
        } else {
            last_bucket += aggregationTimeDelta;
            if (first_bucket < last_bucket) {
                has_empty_buckets = false;
            }
        }
        if (has_empty_buckets) {
            si = -1;
            int err = fillEmptyBuckets(&enrichedChunk->samples,
                                       &agg_n_samples,
                                       first_bucket,
                                       last_bucket,
                                       self,
                                       is_reversed,
                                       &si);
            if (err != 0) {
                return NULL;
            }
            si++;
        }
    }
    self->hasUnFinalizedContext = true;

    if (!self->initialized) {
        timestamp_t init_ts = enrichedChunk->samples.timestamps[si];
        self->aggregationLastTimestamp =
            CalcBucketStart(init_ts, aggregationTimeDelta, self->timestampAlignment);
        self->initialized = true;

        for (size_t a = 0; a < self->numAggregations; a++) {
            if (self->aggregations[a]->type == TS_AGG_TWA) {
                timestamp_t ta = twa_calc_ta(self->reverse,
                                             BucketStartNormalize(self->aggregationLastTimestamp),
                                             self->aggregationLastTimestamp + aggregationTimeDelta,
                                             self->startTimestamp,
                                             self->endTimestamp);
                timestamp_t tb = twa_calc_tb(self->reverse,
                                             BucketStartNormalize(self->aggregationLastTimestamp),
                                             self->aggregationLastTimestamp + aggregationTimeDelta,
                                             self->startTimestamp,
                                             self->endTimestamp);
                self->aggregations[a]->addBucketParams(self->aggregationContexts[a],
                                                       (!self->reverse) ? ta : tb,
                                                       (!self->reverse) ? tb : ta);
            }
        }

        if (self->hasTwa && !((!is_reversed) && init_ts == 0)) {
            RangeArgs args = {
                .aggregationArgs = { 0 },
                .filterByValueArgs = { 0 },
                .filterByTSArgs = { 0 },
                .startTimestamp = is_reversed ? init_ts + 1 : 0,
                .endTimestamp = is_reversed ? UINT64_MAX : init_ts - 1,
                .latest = false,
            };
            AbstractSampleIterator *sample_iterator =
                SeriesCreateSampleIterator(self->series, &args, !is_reversed, true);
            // Skip non valid samples - they shouldn't be used for interpolation
            while (sample_iterator->GetNext(sample_iterator, &sample) == CR_OK) {
                if (aggregation->isValueValid(sample.value)) {
                    for (size_t a = 0; a < self->numAggregations; a++) {
                        if (self->aggregations[a]->type == TS_AGG_TWA) {
                            self->aggregations[a]->addPrevBucketLastSample(
                                self->aggregationContexts[a], sample.value, sample.timestamp);
                        }
                    }
                    break;
                }
            }
            sample_iterator->Close(sample_iterator);
        }
    }

    void (*appendValue)(void *, double, timestamp_t) = aggregation->appendValue;
    uint64_t contextScope = self->aggregationLastTimestamp + aggregationTimeDelta;
    self->aggregationLastTimestamp = BucketStartNormalize(self->aggregationLastTimestamp);
    while (enrichedChunk) {
        // currently if the query reversed the chunk will be already revered here
        assert(self->reverse == enrichedChunk->rev || enrichedChunk->samples.num_samples == 0);
        Samples *samples = &enrichedChunk->samples;
        if (self->numAggregations == 1 && aggregation->type == TS_AGG_MAX && !is_reversed) {
            while (si < samples->num_samples) {
                ei = findLastIndexbeforeTS(enrichedChunk, contextScope, si);
                if (likely(ei >= 0)) {
                    aggregation->appendValueVec(
                        aggregationContext, enrichedChunk->samples.values, si, ei);
                    for (int64_t idx = si; idx <= ei; idx++) {
                        if (aggregation->isValueValid(enrichedChunk->samples.values[idx])) {
                            self->validSamplesInBucket = true;
                        }
                    }
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
                    if (finalizeBucket(&enrichedChunk->samples, agg_n_samples, self)) {
                        agg_n_samples++;
                    }
                    self->aggregationLastTimestamp = CalcBucketStart(
                        sample.timestamp, aggregationTimeDelta, self->timestampAlignment);
                    if (self->empty) {
                        bool has_empty_buckets = true;
                        timestamp_t first_bucket, last_bucket;
                        if (is_reversed) {
                            first_bucket = max(0,
                                               (int64_t)((int64_t)contextScope -
                                                         (int64_t)(2 * aggregationTimeDelta)));
                            last_bucket = self->aggregationLastTimestamp + aggregationTimeDelta;
                            if (contextScope > last_bucket + (2 * aggregationTimeDelta)) {
                                has_empty_buckets = false;
                            }
                        } else {
                            first_bucket = contextScope;
                            if (first_bucket >= self->aggregationLastTimestamp) {
                                has_empty_buckets = false;
                            }
                            last_bucket = max(0,
                                              (int64_t)((int64_t)self->aggregationLastTimestamp -
                                                        (int64_t)aggregationTimeDelta));
                        }
                        if (has_empty_buckets) {
                            int err = fillEmptyBuckets(&enrichedChunk->samples,
                                                       &agg_n_samples,
                                                       first_bucket,
                                                       last_bucket,
                                                       self,
                                                       is_reversed,
                                                       &si);
                            if (err != 0) {
                                return NULL;
                            }
                        }
                    }
                    contextScope = self->aggregationLastTimestamp + aggregationTimeDelta;
                    self->aggregationLastTimestamp =
                        BucketStartNormalize(self->aggregationLastTimestamp);

                    // append sample and inc si cause we aggregate in place
                    if (aggregation->isValueValid(sample.value)) {
                        appendValue(aggregationContext, sample.value, sample.timestamp);
                        self->validSamplesInBucket = true;
                    }
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
                if ((!is_reversed && sample.timestamp >= contextScope) ||
                    (is_reversed && sample.timestamp < self->aggregationLastTimestamp)) {
                    for (size_t a = 0; a < self->numAggregations; a++) {
                        if (self->aggregations[a]->type == TS_AGG_TWA &&
                            self->aggregations[a]->isValueValid(sample.value)) {
                            self->aggregations[a]->addNextBucketFirstSample(
                                self->aggregationContexts[a], sample.value, sample.timestamp);
                        }
                    }

                    bool hadValidSamples = self->validSamplesInBucket;
                    // Collect last samples from each TWA context before finalizeBucket resets them
                    Sample twa_last_samples[self->numAggregations];
                    if (self->hasTwa) {
                        for (size_t a = 0; a < self->numAggregations; a++) {
                            if (self->aggregations[a]->type == TS_AGG_TWA) {
                                self->aggregations[a]->getLastSample(self->aggregationContexts[a],
                                                                     &twa_last_samples[a]);
                            }
                        }
                    }

                    Samples *outSamples =
                        ensureOutputSamples(self, enrichedChunk, agg_n_samples + 1);
                    if (finalizeBucket(outSamples, agg_n_samples, self)) {
                        agg_n_samples++;
                    }
                    self->aggregationLastTimestamp = CalcBucketStart(
                        sample.timestamp, aggregationTimeDelta, self->timestampAlignment);
                    if (self->empty) {
                        bool has_empty_buckets = true;
                        timestamp_t first_bucket, last_bucket;
                        if (is_reversed) {
                            first_bucket = max(0,
                                               (int64_t)((int64_t)contextScope -
                                                         (int64_t)(2 * aggregationTimeDelta)));
                            last_bucket = self->aggregationLastTimestamp + aggregationTimeDelta;
                            if (contextScope < last_bucket + (2 * aggregationTimeDelta)) {
                                has_empty_buckets = false;
                            }
                        } else {
                            first_bucket = contextScope;
                            if (first_bucket >= self->aggregationLastTimestamp) {
                                has_empty_buckets = false;
                            }
                            last_bucket = max(0,
                                              (int64_t)((int64_t)self->aggregationLastTimestamp -
                                                        (int64_t)aggregationTimeDelta));
                        }
                        if (has_empty_buckets) {
                            Samples *emptySamples =
                                ensureOutputSamples(self, enrichedChunk, agg_n_samples + 256);
                            int64_t read_idx = -1;
                            int err = fillEmptyBuckets(emptySamples,
                                                       &agg_n_samples,
                                                       first_bucket,
                                                       last_bucket,
                                                       self,
                                                       is_reversed,
                                                       multiAgg ? &read_idx : &si);
                            if (err != 0) {
                                return NULL;
                            }
                        }
                    }
                    contextScope = self->aggregationLastTimestamp + aggregationTimeDelta;
                    self->aggregationLastTimestamp =
                        BucketStartNormalize(self->aggregationLastTimestamp);

                    for (size_t a = 0; a < self->numAggregations; a++) {
                        if (self->aggregations[a]->type == TS_AGG_TWA) {
                            if (hadValidSamples &&
                                self->aggregations[a]->isValueValid(twa_last_samples[a].value)) {
                                self->aggregations[a]->addPrevBucketLastSample(
                                    self->aggregationContexts[a],
                                    twa_last_samples[a].value,
                                    twa_last_samples[a].timestamp);
                            }
                            timestamp_t tb = twa_calc_tb(self->reverse,
                                                         self->aggregationLastTimestamp,
                                                         contextScope,
                                                         self->startTimestamp,
                                                         self->endTimestamp);
                            self->aggregations[a]->addBucketParams(
                                self->aggregationContexts[a],
                                (!self->reverse) ? self->aggregationLastTimestamp : tb,
                                (!self->reverse) ? tb : contextScope);
                        }
                    }
                }

                if (aggregation->isValueValid(sample.value)) {
                    if (self->numAggregations == 1) {
                        appendValue(aggregationContext, sample.value, sample.timestamp);
                    } else {
                        appendValueToAllContexts(self, sample.value, sample.timestamp);
                    }
                    self->validSamplesInBucket = true;
                }
                si++;
            }
        }

        if (agg_n_samples > 0) {
            if (multiAgg) {
                self->prev_ts = self->aux_chunk->samples.timestamps[agg_n_samples - 1];
                self->aux_chunk->samples.num_samples = agg_n_samples;
                return self->aux_chunk;
            } else {
                self->prev_ts = enrichedChunk->samples.timestamps[agg_n_samples - 1];
                enrichedChunk->samples.num_samples = agg_n_samples;
                return enrichedChunk;
            }
        }
        enrichedChunk = input->GetNext(input);
        si = 0;
    }

_finalize:
    self->hasUnFinalizedContext = false;
    for (size_t a = 0; a < self->numAggregations; a++) {
        if (self->aggregations[a]->type == TS_AGG_TWA) {
            Sample last_sample;
            self->aggregations[a]->getLastSample(self->aggregationContexts[a], &last_sample);
            if (!(is_reversed && last_sample.timestamp == 0)) {
                RangeArgs args = {
                    .aggregationArgs = { 0 },
                    .filterByValueArgs = { 0 },
                    .filterByTSArgs = { 0 },
                    .startTimestamp = is_reversed ? 0 : last_sample.timestamp + 1,
                    .endTimestamp = is_reversed ? last_sample.timestamp - 1 : UINT64_MAX,
                    .latest = false,
                };
                AbstractSampleIterator *sample_iterator =
                    SeriesCreateSampleIterator(self->series, &args, is_reversed, true);
                // Skip non valid samples - they shouldn't be used for interpolation
                while (sample_iterator->GetNext(sample_iterator, &sample) == CR_OK) {
                    if (self->aggregations[a]->isValueValid(sample.value)) {
                        self->aggregations[a]->addNextBucketFirstSample(
                            self->aggregationContexts[a], sample.value, sample.timestamp);
                        break;
                    }
                }
                sample_iterator->Close(sample_iterator);
            }
        }
    }

    size_t numAggs = self->numAggregations;
    size_t vps = self->aux_chunk->samples.values_per_sample;
    bool shouldBucketIgnored = self->validSamplesInBucket == 0;
    if (shouldBucketIgnored) {
        if (!self->empty) {
            for (size_t a = 0; a < numAggs; a++) {
                self->aggregations[a]->resetContext(self->aggregationContexts[a]);
            }
            self->validSamplesInBucket = false;
            self->aux_chunk->samples.num_samples = 0;
            return self->aux_chunk;
        }
        for (size_t a = 0; a < numAggs; a++) {
            // For TWA, compute value using surrounding samples (same as truly empty buckets)
            if (self->aggregations[a]->type == TS_AGG_TWA) {
                twa_compute_empty_bucket_value(
                    self, self->aggregationLastTimestamp, &self->aux_chunk->samples.values[a]);
            } else {
                self->aggregations[a]->finalizeEmpty(self->aggregationContexts[a],
                                                     &self->aux_chunk->samples.values[a]);
            }
        }
    } else {
        for (size_t a = 0; a < numAggs; a++) {
            self->aggregations[a]->finalize(self->aggregationContexts[a],
                                            &self->aux_chunk->samples.values[a]);
        }
    }
    self->validSamplesInBucket = false;

    self->aux_chunk->samples.timestamps[0] =
        calc_bucket_ts(self->bucketTS, self->aggregationLastTimestamp, self->aggregationTimeDelta);
    size_t n_samples = 1;
    if (TWA_EMPTY_RANGE(self) && !self->handled_twa_empty_suffix) {
        self->handled_twa_empty_suffix = true;
        timestamp_t last_bucket =
            CalcBucketStart(is_reversed ? self->startTimestamp : self->endTimestamp,
                            aggregationTimeDelta,
                            self->timestampAlignment);
        timestamp_t first_bucket = CalcBucketStart(
            self->aux_chunk->samples.timestamps[0], aggregationTimeDelta, self->timestampAlignment);
        bool has_empty_buckets = true;
        if (is_reversed) {
            if (first_bucket <= last_bucket) {
                has_empty_buckets = false;
            }
            first_bucket = (int64_t)((int64_t)first_bucket - (int64_t)aggregationTimeDelta);
        } else {
            if (first_bucket >= last_bucket) {
                has_empty_buckets = false;
            }
            first_bucket += aggregationTimeDelta;
        }
        int64_t read_index = 0;
        if (has_empty_buckets) {
            self->aux_chunk->samples.num_samples = 1;
            int err = fillEmptyBuckets(&self->aux_chunk->samples,
                                       &n_samples,
                                       first_bucket,
                                       last_bucket,
                                       self,
                                       is_reversed,
                                       &read_index);
            if (err != 0) {
                return NULL;
            }
        }
    }
    self->aux_chunk->samples.num_samples = n_samples;
    return self->aux_chunk;
}

void AggregationIterator_Close(struct AbstractIterator *iterator) {
    AggregationIterator *self = (AggregationIterator *)iterator;
    iterator->input->Close(iterator->input);
    for (size_t a = 0; a < self->numAggregations; a++) {
        self->aggregations[a]->freeContext(self->aggregationContexts[a]);
    }
    free(self->aggregations);
    free(self->aggregationContexts);
    FreeEnrichedChunk(self->aux_chunk);
    free(iterator);
}
