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
#include "compaction.h"
#include "series_iterator.h"
#include "utils/arr.h"
#include <assert.h>
#include <math.h> /* ceil */
#include <string.h>

static void agg_iter_load_last_pre_window(AggregationIterator *self, Sample *sample);
static EnrichedChunk *agg_iter_empty_range_no_samples(AggregationIterator *self,
                                                        uint64_t aggregationTimeDelta,
                                                        bool is_reversed,
                                                        size_t *agg_n_samples,
                                                        int64_t *si);

/**
 * @brief True if @p agg uses LOCF for `EMPTY` (last context / `finalize_empty_last_value`; @c LAST).
 * @param[in] agg aggregation class.
 */
static inline bool agg_class_needs_locf_empty_seed(const AggregationClass *agg) {
    return agg->type == TS_AGG_LAST;
}

/**
 * @brief True if @p self needs a sample before the first bucket (`addPrevBucketLastSample` on any agg).
 * @param[in] self aggregation iterator (e.g. TWA).
 */
static bool agg_iter_needs_prev_bucket_sample(const AggregationIterator *self) {
    for (size_t a = 0; a < self->numAggregations; a++) {
        if (self->aggregations[a].addPrevBucketLastSample) {
            return true;
        }
    }
    return false;
}

/**
 * @brief True if any aggregation in @p self uses LOCF for `EMPTY` (see agg_class_needs_locf_empty_seed).
 * @param[in] self aggregation iterator.
 */
static bool agg_iter_any_locf_empty_agg(const AggregationIterator *self) {
    for (size_t a = 0; a < self->numAggregations; a++) {
        if (agg_class_needs_locf_empty_seed(&self->aggregations[a])) {
            return true;
        }
    }
    return false;
}

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
                    samples->timestamps[count] = sample_ts;
                    for (size_t a = 0; a < samples->values_per_sample; ++a) {
                        Samples_value_at(samples, count, a) =
                            Samples_value_at(samples, cur_frame.si, a);
                    }
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
            if (check_sample_value(Samples_value_at(&enrichedChunk->samples, i, 0),
                                   &self->byValueArgs)) {
                enrichedChunk->samples.timestamps[count] = enrichedChunk->samples.timestamps[i];
                for (size_t a = 0; a < enrichedChunk->samples.values_per_sample; ++a) {
                    Samples_value_at(&enrichedChunk->samples, count, a) =
                        Samples_value_at(&enrichedChunk->samples, i, a);
                }
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
    for (size_t i = 0; i < numAggregations; i++) {
        iter->aggregations[i] = *aggregations[i];
        iter->aggregationContexts[i] = iter->aggregations[i].createContext(reverse);
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
    iter->handled_non_twa_empty_full_range = false;
    iter->handled_non_twa_empty_prefix = false;
    iter->last_locf_seed_ok = true;
    iter->prev_ts = DC;
    iter->validSamplesInBucket = false;
    memset(iter->validPerAgg, 0, sizeof(iter->validPerAgg));
    ReallocSamplesArray(&iter->aux_chunk->samples, 1);
    ResetEnrichedChunk(iter->aux_chunk);
    return iter;
}

static size_t get_samples_from_side(timestamp_t cur_ts,
                                    bool from_left,
                                    const AggregationIterator *self,
                                    Sample *sample_1,
                                    Sample *sample_2);
static timestamp_t calc_ta(bool reverse,
                           timestamp_t bucketStartTS,
                           timestamp_t bucketEndTS,
                           timestamp_t rangeStart,
                           timestamp_t rangeEnd);
static timestamp_t calc_tb(bool reverse,
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

    timestamp_t ta = calc_ta(
        false, bucket_ts, bucket_ts + agg_time_delta, self->startTimestamp, self->endTimestamp);
    timestamp_t tb = calc_tb(
        false, bucket_ts, bucket_ts + agg_time_delta, self->startTimestamp, self->endTimestamp);

    size_t n_samples_before =
        get_samples_from_side(ta, true, self, &sample_before, &sample_befBefore);
    size_t n_samples_after = get_samples_from_side(tb, false, self, &sample_after, &sample_afAfter);

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
    if (shouldBucketIgnored) {
        // Bucket has only NaN samples - treat as empty bucket
        if (!self->empty) {
            for (size_t a = 0; a < numAggs; a++) {
                self->aggregations[a].resetContext(self->aggregationContexts[a]);
                self->validPerAgg[a] = false;
            }
            self->validSamplesInBucket = false;
            return false;
        }
        // For TWA, compute value using surrounding samples (same as truly empty buckets)
        double twa_empty_val = 0;
        if (self->hasTwa) {
            twa_compute_empty_bucket_value(self, self->aggregationLastTimestamp, &twa_empty_val);
        }
        for (size_t a = 0; a < numAggs; a++) {
            if (self->aggregations[a].type == TS_AGG_TWA) {
                Samples_value_at(samples, index, a) = twa_empty_val;
            } else {
                self->aggregations[a].finalizeEmpty(self->aggregationContexts[a],
                                                    &Samples_value_at(samples, index, a));
            }
        }
    } else {
        double twa_empty_val;
        bool twa_empty_computed = false;
        for (size_t a = 0; a < numAggs; a++) {
            if (self->validPerAgg[a]) {
                self->aggregations[a].finalize(self->aggregationContexts[a],
                                               &Samples_value_at(samples, index, a));
            } else if (self->aggregations[a].type == TS_AGG_TWA) {
                if (!twa_empty_computed) {
                    twa_compute_empty_bucket_value(
                        self, self->aggregationLastTimestamp, &twa_empty_val);
                    twa_empty_computed = true;
                }
                Samples_value_at(samples, index, a) = twa_empty_val;
            } else {
                self->aggregations[a].finalizeEmpty(self->aggregationContexts[a],
                                                    &Samples_value_at(samples, index, a));
            }
        }
    }
    samples->timestamps[index] =
        calc_bucket_ts(self->bucketTS, self->aggregationLastTimestamp, self->aggregationTimeDelta);
    for (size_t a = 0; a < numAggs; a++) {
        self->aggregations[a].resetContext(self->aggregationContexts[a]);
        self->validPerAgg[a] = false;
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

// Empty bucket with no samples from left or right at all.
static void fillEmptyBucketsWithDefaultVals(size_t *write_index,
                                            timestamp_t cur_ts,
                                            Samples *samples,
                                            const AggregationIterator *self,
                                            size_t n_empty_buckets,
                                            bool reversed) {
    for (size_t i = 0; i < n_empty_buckets; ++i) {
        timestamp_t ts = calc_bucket_ts(self->bucketTS, cur_ts, self->aggregationTimeDelta);
        for (size_t a = 0; a < self->numAggregations; a++) {
            self->aggregations[a].finalizeEmpty(self->aggregationContexts[a],
                                                &Samples_value_at(samples, *write_index, a));
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

/**
 * @brief Return up to two nearest non-NaN samples on one side of @p cur_ts.
 *
 * Unifies the previous `..._from_left` / `..._from_right` pair. NaN samples are skipped
 * because they can't serve as interpolation/carry sources.
 *
 * @param[in]  cur_ts    pivot timestamp.
 * @param[in]  from_left true: scan `[0, cur_ts - 1]` newest-first (samples strictly before).
 *                       false: scan `[cur_ts, UINT64_MAX]` oldest-first (samples at or after).
 * @param[out] sample_1  closest sample to @p cur_ts on the chosen side.
 * @param[out] sample_2  second-closest sample on the chosen side (written only when 2 found).
 * @return number of samples written (0, 1, or 2).
 */
static size_t get_samples_from_side(timestamp_t cur_ts,
                                    bool from_left,
                                    const AggregationIterator *self,
                                    Sample *sample_1,
                                    Sample *sample_2) {
    if (from_left ? (cur_ts == 0) : (cur_ts == UINT64_MAX)) {
        return 0;
    }
    RangeArgs args = {
        .aggregationArgs = { 0 },
        .filterByValueArgs = { 0 },
        .filterByTSArgs = { 0 },
        .startTimestamp = from_left ? 0 : cur_ts,
        .endTimestamp = from_left ? cur_ts - 1 : UINT64_MAX,
        .latest = false,
    };
    AbstractSampleIterator *sample_iterator =
        SeriesCreateSampleIterator(self->series, &args, from_left, true);
    Sample sample;
    size_t n = 0;
    while (sample_iterator->GetNext(sample_iterator, &sample) == CR_OK) {
        if (isnan(sample.value)) {
            continue;
        }
        if (n == 0) {
            *sample_1 = sample;
            n = 1;
        } else {
            *sample_2 = sample;
            n = 2;
            break;
        }
    }
    sample_iterator->Close(sample_iterator);
    return n;
}

/** @brief Clipped leading edge of a bucket in iteration order (forward: start, reverse: end). */
static timestamp_t calc_ta(bool reverse,
                           timestamp_t bucketStartTS,
                           timestamp_t bucketEndTS,
                           timestamp_t rangeStart,
                           timestamp_t rangeEnd) {
    return reverse ? min(bucketEndTS, rangeEnd) : max(bucketStartTS, rangeStart);
}

/** @brief Clipped trailing edge of a bucket in iteration order (forward: end, reverse: start). */
static timestamp_t calc_tb(bool reverse,
                           timestamp_t bucketStartTS,
                           timestamp_t bucketEndTS,
                           timestamp_t rangeStart,
                           timestamp_t rangeEnd) {
    return reverse ? max(bucketStartTS, rangeStart) : min(bucketEndTS, rangeEnd);
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
    ta = calc_ta(
        false, cur_ts, cur_ts + agg_time_delta, self->startTimestamp, self->endTimestamp);
    n_samples_before = get_samples_from_side(ta, true, self, &sample_before, &sample_befBefore);
    n_samples_after = get_samples_from_side(ta, false, self, &sample_after, &sample_afAfter);

    for (size_t i = 0; i < n_empty_buckets; ++i) {
        ta = calc_ta(
            false, cur_ts, cur_ts + agg_time_delta, self->startTimestamp, self->endTimestamp);
        tb = calc_tb(
            false, cur_ts, cur_ts + agg_time_delta, self->startTimestamp, self->endTimestamp);

        timestamp_t ts = calc_bucket_ts(self->bucketTS, cur_ts, self->aggregationTimeDelta);
        double twa_val = 0;
        if (self->hasTwa) {
            twa_calc_empty_bucket_val(ta,
                                      tb,
                                      &sample_before,
                                      &sample_befBefore,
                                      &sample_after,
                                      &sample_afAfter,
                                      n_samples_before,
                                      n_samples_after,
                                      &twa_val);
        }
        for (size_t a = 0; a < self->numAggregations; a++) {
            if (self->aggregations[a].type == TS_AGG_TWA) {
                Samples_value_at(samples, *write_index, a) = twa_val;
            } else {
                self->aggregations[a].finalizeEmpty(self->aggregationContexts[a],
                                                    &Samples_value_at(samples, *write_index, a));
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

#ifndef PREFIX_SUFFIX_IMPL

/**
 * @brief True if a run of empty buckets lies outside the data span and should be skipped.
 *
 * Aggregation-agnostic: at @p first_bucket_of_gap, asks the series whether raw samples exist on
 * both sides of the bucket's leading edge. If either side has no samples, the gap is outside the
 * data span and the caller omits it. Used identically by:
 *   - TWA  : interpolation is undefined without left/right neighbors.
 *   - LOCF (e.g. `LAST + EMPTY`) : no carrier value exists outside the data span.
 *   - Plain `EMPTY` (`SUM`, `COUNT`, ...) : avoid synthesizing values past the data.
 */
static bool should_skip_empty_gap(const AggregationIterator *self,
                                  timestamp_t first_bucket_of_gap,
                                  uint64_t aggregationTimeDelta) {
    Sample sample_before, sample_befBefore, sample_after, sample_afAfter;
    timestamp_t ta = calc_ta(
        false,
        first_bucket_of_gap,
        first_bucket_of_gap + aggregationTimeDelta,
        self->startTimestamp,
        self->endTimestamp);
    size_t n_samples_before =
        get_samples_from_side(ta, true, self, &sample_before, &sample_befBefore);
    size_t n_samples_after =
        get_samples_from_side(ta, false, self, &sample_after, &sample_afAfter);
    return n_samples_before == 0 || n_samples_after == 0;
}

#endif // PREFIX_SUFFIX_IMPL

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

    // Check timestamp ordering
    if (end_bucket_ts < first_bucket_ts || agg_time_delta == 0) {
        return 0; /* skip gap; -1 would abort the entire TS.RANGE */
    }

    // Check alignment with aggregation time delta
    if ((uint64_t)(end_bucket_ts - first_bucket_ts) % (uint64_t)agg_time_delta != 0) {
        return 0;
    }

    size_t n_empty_buckets = ((end_bucket_ts - first_bucket_ts) / agg_time_delta) + 1;
    if (n_empty_buckets == 0) {
        return 0;
    }

    timestamp_t cur_ts = (reversed) ? end_bucket_ts : first_bucket_ts;

#ifndef PREFIX_SUFFIX_IMPL // The PM decided to disable it, as it might cause OOM and complicates
                           // users
    // Skip empty buckets that lie outside the data span (no raw sample on one side). Applies
    // uniformly to TWA, LOCF, and plain EMPTY so forward and reverse return the same buckets.
    if (should_skip_empty_gap(self, cur_ts, self->aggregationTimeDelta)) {
        return 0;
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
        memmove(samples->_og_values,
                samples->_og_values + padding * vps,
                *write_index * vps * sizeof(double));
        memmove(samples->og_timestamps + *write_index + n_empty_buckets,
                samples->og_timestamps + padding + _read_index,
                n_read_samples_left * sizeof(samples->og_timestamps[0]));
        memmove(samples->_og_values + (*write_index + n_empty_buckets) * vps,
                samples->_og_values + (padding + _read_index) * vps,
                n_read_samples_left * vps * sizeof(double));

        // use compacted buffer so fillEmptyBuckets* write at correct indices (avoid overflow)
        samples->timestamps = samples->og_timestamps;
        samples->_values = samples->_og_values;

        // update the read index and num of samples
        _read_index = *write_index + n_empty_buckets;
        *read_index = _read_index - 1; // - 1 cause outside this function we inc by 1
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

/**
 * @brief Computes bucket bounds for `EMPTY` **prefix**: query edge through bucket before @p first_sample_ts.
 *
 * Forward: first bucket aligns to range start, last bucket is the one immediately before the bucket
 * containing @p first_sample_ts. Reversed: uses range end and adjusts bounds symmetrically.
 *
 * @param[in] self iterator (range, alignment).
 * @param[in] aggregationTimeDelta bucket width.
 * @param[in] is_reversed forward vs reverse aggregation direction.
 * @param[in] first_sample_ts timestamp of the first in-range raw sample (chunk start).
 * @param[out] out_first_bucket first empty-prefix bucket start.
 * @param[out] out_last_bucket last empty-prefix bucket start.
 * @param[out] out_has_span false if there is no non-empty span to fill (degenerate / no gap).
 */
static void agg_iter_compute_empty_prefix_bounds(const AggregationIterator *self,
                                              uint64_t aggregationTimeDelta,
                                              bool is_reversed,
                                              timestamp_t first_sample_ts,
                                              timestamp_t *out_first_bucket,
                                              timestamp_t *out_last_bucket,
                                              bool *out_has_span) {
    *out_first_bucket =
        CalcBucketStart(is_reversed ? self->endTimestamp : self->startTimestamp,
                        aggregationTimeDelta,
                        self->timestampAlignment);
    *out_last_bucket =
        CalcBucketStart(first_sample_ts, aggregationTimeDelta, self->timestampAlignment);
    *out_has_span = true;
    if (!is_reversed) {
        if (*out_first_bucket >= *out_last_bucket) {
            *out_has_span = false;
        }
        *out_last_bucket =
            max(0, (int64_t)((int64_t)*out_last_bucket - (int64_t)aggregationTimeDelta));
    } else {
        *out_last_bucket += aggregationTimeDelta;
        if (*out_first_bucket < *out_last_bucket) {
            *out_has_span = false;
        }
    }
}

/**
 * @brief Appends `EMPTY` buckets for aligned span [@p first_bucket, @p last_bucket] into the output chunk.
 *
 * Delegates to `fillEmptyBuckets`. Single-aggregation path appends into @p enrichedChunk and advances
 * @p si past the filled span on success. Multi-aggregation path appends after existing prefix samples
 * in `aux_chunk` (via `ensureOutputSamples`) and returns `fillEmptyBuckets`'s status unchanged.
 *
 * @param[in,out] self aggregation iterator (contexts, `EMPTY` policy).
 * @param[in,out] enrichedChunk chunk being built; single-agg writes `samples`, multi-agg uses aux buffer.
 * @param[in] first_bucket first bucket start in the span (inclusive).
 * @param[in] last_bucket last bucket start in the span (inclusive).
 * @param[in] is_reversed forward vs reverse bucket order for `fillEmptyBuckets`.
 * @param[in] multiAgg if true, write into scratch samples; if false, write into @p enrichedChunk.
 * @param[in,out] agg_n_samples sample count: prefix length for multi-agg; updated by `fillEmptyBuckets`.
 * @param[in,out] si read index for `fillEmptyBuckets` on single-agg path; reset to `-1`, then incremented on success.
 * @return `0` on success (single-agg), or `-1` on failure; multi-agg returns `fillEmptyBuckets`'s return code.
 */
static int agg_iter_append_empty_buckets(AggregationIterator *self,
                                             EnrichedChunk *enrichedChunk,
                                             timestamp_t first_bucket,
                                             timestamp_t last_bucket,
                                             bool is_reversed,
                                             bool multiAgg,
                                             size_t *agg_n_samples,
                                             int64_t *si) {
    if (multiAgg) {
        Samples *prefixSamples = ensureOutputSamples(self, enrichedChunk, *agg_n_samples + 256);
        prefixSamples->num_samples = *agg_n_samples;
        int64_t read_idx = -1;
        return fillEmptyBuckets(
            prefixSamples, agg_n_samples, first_bucket, last_bucket, self, is_reversed, &read_idx);
    }
    *si = -1;
    int err = fillEmptyBuckets(&enrichedChunk->samples,
                               agg_n_samples,
                               first_bucket,
                               last_bucket,
                               self,
                               is_reversed,
                               si);
    if (err != 0) {
        return -1;
    }
    (*si)++;
    return 0;
}

/**
 * @brief Emit `EMPTY` buckets for the full query range into `aux_chunk` via `fillEmptyBuckets`.
 *
 * Used when there is no in-range data to merge into an `EnrichedChunk` but `EMPTY` still needs
 * every bucket (TWA empty range; non-TWA empty full range after LOCF). `aux_chunk` is the scratch
 * buffer because there is no upstream chunk to extend.
 *
 * @param[in,out] self iterator (range, alignment, contexts).
 * @param[in] aggregationTimeDelta bucket width.
 * @param[in] is_reversed forward vs reverse (`fillEmptyBuckets`; may swap aligned bounds).
 * @param[out] agg_n_samples emitted row count; @p si set to `-1` then passed to `fillEmptyBuckets`.
 * @return `fillEmptyBuckets` return value.
 */
static int agg_iter_fill_aux_query_window(AggregationIterator *self,
                                          uint64_t aggregationTimeDelta,
                                          bool is_reversed,
                                          size_t *agg_n_samples,
                                          int64_t *si) {
    timestamp_t fb =
        CalcBucketStart(self->startTimestamp, aggregationTimeDelta, self->timestampAlignment);
    timestamp_t lb =
        CalcBucketStart(self->endTimestamp, aggregationTimeDelta, self->timestampAlignment);
    if (is_reversed) {
        __SWAP(fb, lb);
    }
    *si = -1;
    self->aux_chunk->samples.num_samples = 0;
    return fillEmptyBuckets(&self->aux_chunk->samples,
                            agg_n_samples,
                            fb,
                            lb,
                            self,
                            is_reversed,
                            si);
}

/**
 * @brief Handles the "no more samples from upstream" case for an aggregation iterator.
 *
 * Decides between three terminal paths and signals the caller via @p enter_finalize:
 * - If a partially-built bucket is still pending (`hasUnFinalizedContext`), sets
 *   @p enter_finalize to @c true so the caller runs `agg_iter_finalize()`.
 * - If TWA `EMPTY` range mode is active, emits the empty prefix on the first call and
 *   the empty suffix on the second call (each via the aux chunk), then ends the stream.
 * - If a non-TWA `EMPTY` policy has a fully-empty range that hasn't been emitted yet,
 *   delegates to `agg_iter_empty_range_no_samples()`.
 * - Otherwise returns @c NULL to signal end of stream.
 *
 * @param[in,out] self            aggregation iterator (TWA flags, aux chunk, prev_ts).
 * @param[in]     aggregationTimeDelta bucket duration.
 * @param[in]     is_reversed     forward vs reverse aggregation direction.
 * @param[in,out] agg_n_samples   running count of samples written to the aux chunk.
 * @param[in,out] si              current sample index into the aux chunk; reset to -1
 *                                before filling and post-incremented after.
 * @param[out]    enter_finalize  set to @c true iff the caller must run `agg_iter_finalize()`;
 *                                @c false otherwise.
 * @return The aux `EnrichedChunk` populated with empty buckets, or @c NULL when the stream
 *         has ended or the caller must finalize first.
 */
static EnrichedChunk *agg_iter_on_empty_chunk(AggregationIterator *self,
                                              uint64_t aggregationTimeDelta,
                                              bool is_reversed,
                                              size_t *agg_n_samples,
                                              int64_t *si,
                                              bool *enter_finalize) {
    *enter_finalize = false;
    if (self->hasUnFinalizedContext) {
        *enter_finalize = true;
        return NULL;
    }
    if (TWA_EMPTY_RANGE(self)) {
        if (!self->handled_twa_empty_prefix) {
            self->handled_twa_empty_prefix = true;
            self->handled_twa_empty_suffix = true; // The prefix in this case is also the suffix
            int err = agg_iter_fill_aux_query_window(
                self, aggregationTimeDelta, is_reversed, agg_n_samples, si);
            if (err != 0) {
                return NULL;
            }
            (*si)++;
            self->aux_chunk->samples.num_samples = *agg_n_samples;
            return self->aux_chunk;
        }
        if (!self->handled_twa_empty_suffix) {
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
            *si = -1;
            self->aux_chunk->samples.num_samples = 0;
            int err = fillEmptyBuckets(&self->aux_chunk->samples,
                                       agg_n_samples,
                                       first_bucket,
                                       last_bucket,
                                       self,
                                       is_reversed,
                                       si);
            if (err != 0) {
                return NULL;
            }
            (*si)++;
            self->aux_chunk->samples.num_samples = *agg_n_samples;
            return self->aux_chunk;
        }
        return NULL;
    } else if (self->empty && !self->initialized && !self->handled_non_twa_empty_full_range) {
        return agg_iter_empty_range_no_samples(
            self, aggregationTimeDelta, is_reversed, agg_n_samples, si);
    }
    return NULL;
}


/**
 * @brief Emits the `EMPTY` prefix once per range, before the first non-empty bucket.
 *
 * Gated by the per-range `handled_*_empty_prefix` flags; no-ops if `EMPTY` is inactive,
 * the span is degenerate, or (non-TWA forward LOCF) no valid pre-range seed exists.
 *
 * @param[in,out] self                 aggregation iterator (`EMPTY` flags, range bounds).
 * @param[in,out] enrichedChunk        output chunk receiving prefix buckets.
 * @param[in]     aggregationTimeDelta bucket duration.
 * @param[in]     is_reversed          forward vs reverse direction.
 * @param[in]     multiAgg             true if more than one aggregation is active.
 * @param[in,out] agg_n_samples        running count of samples written.
 * @param[in,out] si                   current sample index into the output buffer.
 * @return 0 on success/no-op; error code propagated from `agg_iter_append_empty_buckets()`.
 */
static int agg_iter_apply_empty_prefix(AggregationIterator *self,
                                       EnrichedChunk *enrichedChunk,
                                       uint64_t aggregationTimeDelta,
                                       bool is_reversed,
                                       bool multiAgg,
                                       size_t *agg_n_samples,
                                       int64_t *si) {
    if (TWA_EMPTY_RANGE(self)) {
        if (self->handled_twa_empty_prefix) {
            return 0;
        }
        self->handled_twa_empty_prefix = true;
    } else {
        if (!self->empty || self->handled_non_twa_empty_prefix) {
            return 0;
        }
        self->handled_non_twa_empty_prefix = true;
    }
    timestamp_t first_bucket, last_bucket;
    bool has_span;
    agg_iter_compute_empty_prefix_bounds(self,
                                      aggregationTimeDelta,
                                      is_reversed,
                                      enrichedChunk->samples.timestamps[0],
                                      &first_bucket,
                                      &last_bucket,
                                      &has_span);
    if (!has_span) {
        return 0;
    }
    if (!TWA_EMPTY_RANGE(self) && !is_reversed && self->startTimestamp > 0 &&
        !self->last_locf_seed_ok) {
        return 0;
    }
    return agg_iter_append_empty_buckets(self,
                                             enrichedChunk,
                                             first_bucket,
                                             last_bucket,
                                             is_reversed,
                                             multiAgg,
                                             agg_n_samples,
                                             si);
}

/**
 * @brief Seeds all applicable aggregations from a single raw `sample`.
 *
 * Three mutually-exclusive seed modes (the first true flag wins):
 * - `seed_prev_bucket_neighbors`: TWA prev-bucket seeding via `addPrevBucketLastSample`.
 * - `seed_next_bucket_neighbors`: TWA next-bucket seeding via `addNextBucketFirstSample`.
 * - otherwise: LOCF empty-bucket seeding via `appendValue` for aggs reported by
 *   `agg_class_needs_locf_empty_seed` (e.g. `TS_AGG_LAST`).
 *
 * Aggs are skipped when the relevant hook is absent or `isValueValid(sample->value)` is false.
 *
 * @return true if at least one aggregation was seeded.
 */
static bool agg_iter_try_seed_sample(AggregationIterator *self,
                                     const Sample *sample,
                                     bool seed_prev_bucket_neighbors,
                                     bool seed_next_bucket_neighbors) {
    bool seeded = false;
    for (size_t a = 0; a < self->numAggregations; a++) {
        AggregationClass *agg = &self->aggregations[a];
        bool applies;
        if (seed_prev_bucket_neighbors) {
            applies = (agg->addPrevBucketLastSample != NULL);
        } else if (seed_next_bucket_neighbors) {
            applies = (agg->addNextBucketFirstSample != NULL);
        } else {
            applies = agg_class_needs_locf_empty_seed(agg);
        }
        if (!applies || !agg->isValueValid(sample->value)) {
            continue;
        }
        if (seed_prev_bucket_neighbors) {
            agg->addPrevBucketLastSample(
                self->aggregationContexts[a], sample->value, sample->timestamp);
        } else if (seed_next_bucket_neighbors) {
            agg->addNextBucketFirstSample(
                self->aggregationContexts[a], sample->value, sample->timestamp);
        } else {
            agg->appendValue(self->aggregationContexts[a], sample->value, sample->timestamp);
        }
        seeded = true;
    }
    return seeded;
}

/**
 * @brief Seeds aggregation contexts from a raw sample in `[range_start, range_end]`.
 *
 * Iterates the series via `SeriesCreateSampleIterator` and stops at the first sample that
 * successfully seeds at least one aggregation. Modes (mutually exclusive):
 * - `seed_prev_bucket_neighbors`: TWA `addPrevBucketLastSample`.
 * - `seed_next_bucket_neighbors`: TWA `addNextBucketFirstSample`.
 * - `seed_last_locf`: LOCF `appendValue` for aggs needing LOCF empty seeding (e.g. `LAST`);
 *   on success also sets `self->last_locf_seed_ok`.
 */
static void agg_iter_neighbor_sample_scan(AggregationIterator *self,
                                          Sample *sample,
                                          timestamp_t range_start,
                                          timestamp_t range_end,
                                          bool reverse_iteration,
                                          bool seed_prev_bucket_neighbors,
                                          bool seed_next_bucket_neighbors,
                                          bool seed_last_locf) {
    RangeArgs args = {
        .aggregationArgs = { 0 },
        .filterByValueArgs = { 0 },
        .filterByTSArgs = { 0 },
        .startTimestamp = range_start,
        .endTimestamp = range_end,
        .latest = false,
    };
    AbstractSampleIterator *sample_iterator =
        SeriesCreateSampleIterator(self->series, &args, reverse_iteration, true);
    while (sample_iterator->GetNext(sample_iterator, sample) == CR_OK) {
        if (seed_prev_bucket_neighbors &&
            agg_iter_try_seed_sample(self, sample, true, false)) {
            sample_iterator->Close(sample_iterator);
            return;
        }
        if (seed_next_bucket_neighbors &&
            agg_iter_try_seed_sample(self, sample, false, true)) {
            sample_iterator->Close(sample_iterator);
            return;
        }
        if (seed_last_locf && agg_iter_try_seed_sample(self, sample, false, false)) {
            self->last_locf_seed_ok = true;
            sample_iterator->Close(sample_iterator);
            return;
        }
    }
    sample_iterator->Close(sample_iterator);
}

/**
 * @brief Pre-loads LOCF empty seed from the latest raw sample chronologically just outside the
 *        query window in the iteration direction (forward: before `startTimestamp`; reverse:
 *        after `endTimestamp`).
 *
 * Used by `TS.RANGE`/`TS.REVRANGE ... AGGREGATION LAST ... EMPTY` so empty buckets at the
 * leading edge of the window (in iteration order) can produce a value via
 * `finalize_empty_last_value`. No-ops if `EMPTY` is inactive, the relevant edge is at the
 * timestamp domain boundary, or no aggregation needs an LOCF empty seed. Resets
 * `last_locf_seed_ok` and delegates the scan to `agg_iter_neighbor_sample_scan`, which sets
 * the flag on success.
 */
static void agg_iter_load_last_pre_window(AggregationIterator *self, Sample *sample) {
    if (!self->empty) {
        return;
    }
    if (!self->reverse && self->startTimestamp == 0) {
        return;
    }
    if (self->reverse && self->endTimestamp == UINT64_MAX) {
        return;
    }
    if (!agg_iter_any_locf_empty_agg(self)) {
        return;
    }
    self->last_locf_seed_ok = false;
    agg_iter_neighbor_sample_scan(self,
                                  sample,
                                  self->reverse ? self->endTimestamp + 1 : 0,
                                  self->reverse ? UINT64_MAX : self->startTimestamp - 1,
                                  !self->reverse,
                                  false,
                                  false,
                                  true);
}

/**
 * @brief Fills `[startTimestamp, endTimestamp]` with empty buckets when no in-range samples exist (non-TWA `EMPTY`).
 *
 * Marks the full empty range as handled, pre-loads an LOCF seed, then writes empty buckets into
 * the aux chunk via `agg_iter_fill_aux_query_window`. Returns @c NULL when forward LOCF has no
 * pre-window seed, the fill fails, or no buckets were written. Caller must ensure `self->empty`,
 * `!initialized`, `!handled_non_twa_empty_full_range`, and `!TWA_EMPTY_RANGE(self)`.
 *
 * @param[in,out] self                 aggregation iterator (`handled_non_twa_empty_full_range`, aux chunk).
 * @param[in]     aggregationTimeDelta bucket duration.
 * @param[in]     is_reversed          forward vs reverse direction.
 * @param[out]    agg_n_samples        number of samples written to the aux chunk.
 * @param[in,out] si                   current sample index into the aux chunk.
 * @return The aux `EnrichedChunk` populated with empty buckets, or @c NULL on failure / no output.
 */
static EnrichedChunk *agg_iter_empty_range_no_samples(AggregationIterator *self,
                                                        uint64_t aggregationTimeDelta,
                                                        bool is_reversed,
                                                        size_t *agg_n_samples,
                                                        int64_t *si) {
    self->handled_non_twa_empty_full_range = true;
    *agg_n_samples = 0;
    Sample locf;
    agg_iter_load_last_pre_window(self, &locf);
    if (!is_reversed && self->startTimestamp > 0 && !self->last_locf_seed_ok) {
        return NULL;
    }
    int err = agg_iter_fill_aux_query_window(
        self, aggregationTimeDelta, is_reversed, agg_n_samples, si);
    if (err != 0) {
        return NULL;
    }
    if (*agg_n_samples == 0) {
        return NULL;
    }
    (*si)++;
    self->aux_chunk->samples.num_samples = *agg_n_samples;
    return self->aux_chunk;
}

// First chunk only: bucket start from first sample, TWA params, optional prior sample for TWA.
static void agg_iter_init_if_needed(AggregationIterator *self,
                                    EnrichedChunk *enrichedChunk,
                                    int64_t si,
                                    uint64_t aggregationTimeDelta,
                                    bool is_reversed,
                                    Sample *sample) {
    if (self->initialized) {
        return;
    }
    timestamp_t init_ts = enrichedChunk->samples.timestamps[si];
    self->aggregationLastTimestamp =
        CalcBucketStart(init_ts, aggregationTimeDelta, self->timestampAlignment);
    self->initialized = true;

    if (self->hasTwa) {
        timestamp_t ta = calc_ta(self->reverse,
                                     BucketStartNormalize(self->aggregationLastTimestamp),
                                     self->aggregationLastTimestamp + aggregationTimeDelta,
                                     self->startTimestamp,
                                     self->endTimestamp);
        timestamp_t tb = calc_tb(self->reverse,
                                     BucketStartNormalize(self->aggregationLastTimestamp),
                                     self->aggregationLastTimestamp + aggregationTimeDelta,
                                     self->startTimestamp,
                                     self->endTimestamp);
        for (size_t a = 0; a < self->numAggregations; a++) {
            if (self->aggregations[a].type == TS_AGG_TWA) {
                self->aggregations[a].addBucketParams(self->aggregationContexts[a],
                                                      (!self->reverse) ? ta : tb,
                                                      (!self->reverse) ? tb : ta);
            }
        }
    }

    // Guard against `init_ts - 1` underflow in forward direction when init_ts == 0
    // (no samples can exist before timestamp 0, so nothing to scan).
    if (agg_iter_needs_prev_bucket_sample(self) && !((!is_reversed) && init_ts == 0)) {
        agg_iter_neighbor_sample_scan(self,
                                      sample,
                                      is_reversed ? init_ts + 1 : 0,
                                      is_reversed ? UINT64_MAX : init_ts - 1,
                                      !is_reversed,
                                      true,
                                      false,
                                      false);
    }
}

/*
 * After finalizeBucket while aggregating in-place: decide if we must insert empty buckets between
 * the previous contextScope and the new bucket start (aggregationLastTimestamp).
 */
static bool agg_iter_empty_gap_after_finalize(uint64_t contextScope,
                                              timestamp_t aggregationLastTimestamp,
                                              uint64_t aggregationTimeDelta,
                                              bool is_reversed,
                                              bool reversed_gap_max_style,
                                              timestamp_t *out_first_bucket,
                                              timestamp_t *out_last_bucket) {
    bool has_empty_buckets = true;
    if (is_reversed) {
        *out_first_bucket =
            max(0, (int64_t)((int64_t)contextScope - (int64_t)(2 * aggregationTimeDelta)));
        *out_last_bucket = aggregationLastTimestamp + aggregationTimeDelta;
        if (reversed_gap_max_style) {
            if (contextScope > *out_last_bucket + (2 * aggregationTimeDelta)) {
                has_empty_buckets = false;
            }
        } else {
            if (contextScope < *out_last_bucket + (2 * aggregationTimeDelta)) {
                has_empty_buckets = false;
            }
        }
    } else {
        *out_first_bucket = contextScope;
        if (*out_first_bucket >= aggregationLastTimestamp) {
            has_empty_buckets = false;
        }
        *out_last_bucket =
            max(0, (int64_t)((int64_t)aggregationLastTimestamp - (int64_t)aggregationTimeDelta));
        /* fillEmptyBuckets rejects end < first or (end-first) % delta != 0 with -1, which aborts
         * the whole range query. Skip synthetic gaps that would fail those checks. */
        if (has_empty_buckets) {
            if (*out_last_bucket < *out_first_bucket) {
                has_empty_buckets = false;
            } else if (aggregationTimeDelta != 0) {
                uint64_t span = *out_last_bucket - *out_first_bucket;
                if (span % aggregationTimeDelta != 0) {
                    has_empty_buckets = false;
                }
            }
        }
    }
    return has_empty_buckets;
}

// Advance *contextScope to the next bucket boundary; normalize aggregationLastTimestamp.
static void agg_iter_advance_context_scope(AggregationIterator *self,
                                           uint64_t aggregationTimeDelta,
                                           uint64_t *contextScope) {
    *contextScope = self->aggregationLastTimestamp + aggregationTimeDelta;
    self->aggregationLastTimestamp = BucketStartNormalize(self->aggregationLastTimestamp);
}

// MAX fast path: vector-append samples with timestamp < *contextScope; advance si past the run.
static void agg_iter_max_drain_vec_segment(AggregationIterator *self,
                                           EnrichedChunk *enrichedChunk,
                                           AggregationClass *aggregation,
                                           void *aggregationContext,
                                           uint64_t *contextScope,
                                           int64_t *si,
                                           int64_t *ei) {
    *ei = findLastIndexbeforeTS(enrichedChunk, *contextScope, *si);
    if (likely(*ei >= 0)) {
        aggregation->appendValueVec(aggregationContext, enrichedChunk->samples._values, *si, *ei);
        for (int64_t idx = *si; idx <= *ei; idx++) {
            if (aggregation->isValueValid(Samples_value_at(&enrichedChunk->samples, idx, 0))) {
                self->validSamplesInBucket = true;
                self->validPerAgg[0] = true;
            }
        }
        *si = *ei + 1;
    }
}

/* Opening sample after vec drain: finalize prior bucket, optional empty gap, advance scope, append.
 * Returns 0 or -1 on fillEmptyBuckets error. Caller ensures *si < num_samples. */
static int agg_iter_max_emit_opening_sample(AggregationIterator *self,
                                            EnrichedChunk *enrichedChunk,
                                            AggregationClass *aggregation,
                                            void *aggregationContext,
                                            uint64_t aggregationTimeDelta,
                                            bool is_reversed,
                                            void (*appendValue)(void *, double, timestamp_t),
                                            uint64_t *contextScope,
                                            size_t *agg_n_samples,
                                            int64_t *si,
                                            Sample *sample) {
    sample->timestamp = enrichedChunk->samples.timestamps[*si];
    sample->value = Samples_value_at(&enrichedChunk->samples, *si, 0);
    assert(enrichedChunk->samples.timestamps[*si] >= *contextScope);
    if (finalizeBucket(&enrichedChunk->samples, *agg_n_samples, self)) {
        (*agg_n_samples)++;
    }
    self->aggregationLastTimestamp =
        CalcBucketStart(sample->timestamp, aggregationTimeDelta, self->timestampAlignment);
    if (self->empty) {
        timestamp_t first_bucket, last_bucket;
        if (agg_iter_empty_gap_after_finalize(*contextScope,
                                              self->aggregationLastTimestamp,
                                              aggregationTimeDelta,
                                              is_reversed,
                                              true,
                                              &first_bucket,
                                              &last_bucket)) {
            int err = fillEmptyBuckets(&enrichedChunk->samples,
                                       agg_n_samples,
                                       first_bucket,
                                       last_bucket,
                                       self,
                                       is_reversed,
                                       si);
            if (err != 0) {
                return -1;
            }
        }
    }
    agg_iter_advance_context_scope(self, aggregationTimeDelta, contextScope);

    if (aggregation->isValueValid(sample->value)) {
        appendValue(aggregationContext, sample->value, sample->timestamp);
        self->validSamplesInBucket = true;
        self->validPerAgg[0] = true;
    }
    (*si)++;
    return 0;
}

// Single agg MAX forward: vectorized append per bucket. Returns 0 or -1 on fillEmptyBuckets error.
static int agg_iter_process_chunk_max_fast_path(AggregationIterator *self,
                                                EnrichedChunk *enrichedChunk,
                                                AggregationClass *aggregation,
                                                void *aggregationContext,
                                                uint64_t aggregationTimeDelta,
                                                bool is_reversed,
                                                void (*appendValue)(void *, double, timestamp_t),
                                                uint64_t *contextScope,
                                                size_t *agg_n_samples,
                                                int64_t *si,
                                                int64_t *ei,
                                                Sample *sample) {
    Samples *samples = &enrichedChunk->samples;
    while (*si < (int64_t)samples->num_samples) {
        agg_iter_max_drain_vec_segment(
            self, enrichedChunk, aggregation, aggregationContext, contextScope, si, ei);
        if (*si >= (int64_t)samples->num_samples) {
            break;
        }
        if (agg_iter_max_emit_opening_sample(self,
                                             enrichedChunk,
                                             aggregation,
                                             aggregationContext,
                                             aggregationTimeDelta,
                                             is_reversed,
                                             appendValue,
                                             contextScope,
                                             agg_n_samples,
                                             si,
                                             sample) != 0) {
            return -1;
        }
    }
    return 0;
}

static int agg_iter_general_on_bucket_boundary(AggregationIterator *self,
                                               EnrichedChunk *enrichedChunk,
                                               Sample *sample,
                                               uint64_t aggregationTimeDelta,
                                               bool is_reversed,
                                               bool multiAgg,
                                               uint64_t *contextScope,
                                               size_t *agg_n_samples,
                                               int64_t *si,
                                               Sample *twa_last_samples,
                                               bool *twaHadValid) {
    for (size_t a = 0; a < self->numAggregations; a++) {
        if (self->aggregations[a].type == TS_AGG_TWA &&
            self->aggregations[a].isValueValid(sample->value)) {
            self->aggregations[a].addNextBucketFirstSample(
                self->aggregationContexts[a], sample->value, sample->timestamp);
        }
    }

    if (self->hasTwa) {
        for (size_t a = 0; a < self->numAggregations; a++) {
            if (self->aggregations[a].type == TS_AGG_TWA) {
                twaHadValid[a] = self->validPerAgg[a];
                self->aggregations[a].getLastSample(self->aggregationContexts[a],
                                                    &twa_last_samples[a]);
            }
        }
    }

    Samples *outSamples = ensureOutputSamples(self, enrichedChunk, *agg_n_samples + 1);
    if (finalizeBucket(outSamples, *agg_n_samples, self)) {
        (*agg_n_samples)++;
    }
    self->aggregationLastTimestamp =
        CalcBucketStart(sample->timestamp, aggregationTimeDelta, self->timestampAlignment);
    if (self->empty) {
        // In reverse iteration, the chronologically-previous-in-time sample relative to the
        // empty gap is the upcoming one (sample arg here), not the bucket we just finalized.
        // Pre-load LOCF aggregations' contexts with it so finalize_empty_*_value emits the
        // correct value during fillEmptyBuckets. The subsequent appendValue for the same
        // sample (in process_chunk_general) is idempotent for LOCF aggs (e.g. LAST).
        if (is_reversed) {
            agg_iter_try_seed_sample(self, sample, false, false);
        }
        timestamp_t first_bucket, last_bucket;
        if (agg_iter_empty_gap_after_finalize(*contextScope,
                                              self->aggregationLastTimestamp,
                                              aggregationTimeDelta,
                                              is_reversed,
                                              false,
                                              &first_bucket,
                                              &last_bucket)) {
            Samples *emptySamples = ensureOutputSamples(self, enrichedChunk, *agg_n_samples + 256);
            if (multiAgg) {
                emptySamples->num_samples = *agg_n_samples;
            }
            int64_t read_idx = -1;
            int err = fillEmptyBuckets(emptySamples,
                                       agg_n_samples,
                                       first_bucket,
                                       last_bucket,
                                       self,
                                       is_reversed,
                                       multiAgg ? &read_idx : si);
            if (err != 0) {
                return -1;
            }
        }
    }
    agg_iter_advance_context_scope(self, aggregationTimeDelta, contextScope);

    if (self->hasTwa) {
        timestamp_t tb = calc_tb(self->reverse,
                                     self->aggregationLastTimestamp,
                                     *contextScope,
                                     self->startTimestamp,
                                     self->endTimestamp);
        for (size_t a = 0; a < self->numAggregations; a++) {
            if (self->aggregations[a].type == TS_AGG_TWA) {
                if (twaHadValid[a] &&
                    self->aggregations[a].isValueValid(twa_last_samples[a].value)) {
                    self->aggregations[a].addPrevBucketLastSample(self->aggregationContexts[a],
                                                                  twa_last_samples[a].value,
                                                                  twa_last_samples[a].timestamp);
                }
                self->aggregations[a].addBucketParams(
                    self->aggregationContexts[a],
                    (!self->reverse) ? self->aggregationLastTimestamp : tb,
                    (!self->reverse) ? tb : *contextScope);
            }
        }
    }
    return 0;
}

static void agg_iter_append_sample_to_all_aggs(AggregationIterator *self,
                                               AggregationClass *aggregation,
                                               void *aggregationContext,
                                               void (*appendValue)(void *, double, timestamp_t),
                                               Sample *sample) {
    if (self->numAggregations == 1) {
        if (aggregation->isValueValid(sample->value)) {
            appendValue(aggregationContext, sample->value, sample->timestamp);
            self->validSamplesInBucket = true;
            self->validPerAgg[0] = true;
        }
    } else {
        for (size_t a = 0; a < self->numAggregations; a++) {
            if (self->aggregations[a].isValueValid(sample->value)) {
                self->aggregations[a].appendValue(
                    self->aggregationContexts[a], sample->value, sample->timestamp);
                self->validSamplesInBucket = true;
                self->validPerAgg[a] = true;
            }
        }
    }
}

static int agg_iter_process_chunk_general(AggregationIterator *self,
                                          EnrichedChunk *enrichedChunk,
                                          AggregationClass *aggregation,
                                          void *aggregationContext,
                                          uint64_t aggregationTimeDelta,
                                          bool is_reversed,
                                          bool multiAgg,
                                          void (*appendValue)(void *, double, timestamp_t),
                                          uint64_t *contextScope,
                                          size_t *agg_n_samples,
                                          int64_t *si,
                                          Sample *sample) {
    Samples *samples = &enrichedChunk->samples;
    Sample twa_last_samples[self->numAggregations];
    bool twaHadValid[self->numAggregations];

    while (*si < (int64_t)samples->num_samples) {
        sample->timestamp = enrichedChunk->samples.timestamps[*si];
        sample->value = Samples_value_at(&enrichedChunk->samples, *si, 0);
        if ((!is_reversed && sample->timestamp >= *contextScope) ||
            (is_reversed && sample->timestamp < self->aggregationLastTimestamp)) {
            if (agg_iter_general_on_bucket_boundary(self,
                                                    enrichedChunk,
                                                    sample,
                                                    aggregationTimeDelta,
                                                    is_reversed,
                                                    multiAgg,
                                                    contextScope,
                                                    agg_n_samples,
                                                    si,
                                                    twa_last_samples,
                                                    twaHadValid) != 0) {
                return -1;
            }
        }

        agg_iter_append_sample_to_all_aggs(
            self, aggregation, aggregationContext, appendValue, sample);
        (*si)++;
    }
    return 0;
}

static EnrichedChunk *agg_iter_try_emit_partial(AggregationIterator *self,
                                                EnrichedChunk *enrichedChunk,
                                                bool multiAgg,
                                                size_t agg_n_samples) {
    if (agg_n_samples == 0) {
        return NULL;
    }
    if (multiAgg) {
        self->prev_ts = self->aux_chunk->samples.timestamps[agg_n_samples - 1];
        self->aux_chunk->samples.num_samples = agg_n_samples;
        return self->aux_chunk;
    }
    self->prev_ts = enrichedChunk->samples.timestamps[agg_n_samples - 1];
    enrichedChunk->samples.num_samples = agg_n_samples;
    return enrichedChunk;
}

static EnrichedChunk *agg_iter_finalize(AggregationIterator *self,
                                        uint64_t aggregationTimeDelta,
                                        bool is_reversed,
                                        Sample *sample) {
    self->hasUnFinalizedContext = false;
    if (self->hasTwa) {
        // All TWA aggs in a single query share the same chronology, so use any TWA agg's
        // last sample to compute the scan range and seed all TWA aggs with one series scan.
        Sample last_sample;
        bool found = false;
        for (size_t a = 0; a < self->numAggregations && !found; a++) {
            if (self->aggregations[a].type == TS_AGG_TWA) {
                self->aggregations[a].getLastSample(self->aggregationContexts[a], &last_sample);
                found = true;
            }
        }
        if (found && !(is_reversed && last_sample.timestamp == 0)) {
            agg_iter_neighbor_sample_scan(
                self,
                sample,
                is_reversed ? 0 : last_sample.timestamp + 1,
                is_reversed ? last_sample.timestamp - 1 : UINT64_MAX,
                is_reversed,
                false,
                true,
                false);
        }
    }

    size_t numAggs = self->numAggregations;
    bool shouldBucketIgnored = self->validSamplesInBucket == 0;
    if (shouldBucketIgnored) {
        // Bucket has only NaN samples - treat as empty bucket
        if (!self->empty) {
            for (size_t a = 0; a < numAggs; a++) {
                self->aggregations[a].resetContext(self->aggregationContexts[a]);
                self->validPerAgg[a] = false;
            }
            self->validSamplesInBucket = false;
            self->aux_chunk->samples.num_samples = 0;
            return self->aux_chunk;
        }
        // For TWA, compute value using surrounding samples (same as truly empty buckets)
        double twa_empty_val = 0;
        if (self->hasTwa) {
            twa_compute_empty_bucket_value(self, self->aggregationLastTimestamp, &twa_empty_val);
        }
        for (size_t a = 0; a < numAggs; a++) {
            if (self->aggregations[a].type == TS_AGG_TWA) {
                Samples_value_at(&self->aux_chunk->samples, 0, a) = twa_empty_val;
            } else {
                self->aggregations[a].finalizeEmpty(
                    self->aggregationContexts[a],
                    &Samples_value_at(&self->aux_chunk->samples, 0, a));
            }
        }
    } else {
        double twa_empty_val;
        bool twa_empty_computed = false;
        for (size_t a = 0; a < numAggs; a++) {
            if (self->validPerAgg[a]) {
                self->aggregations[a].finalize(self->aggregationContexts[a],
                                               &Samples_value_at(&self->aux_chunk->samples, 0, a));
            } else if (self->aggregations[a].type == TS_AGG_TWA) {
                if (!twa_empty_computed) {
                    twa_compute_empty_bucket_value(
                        self, self->aggregationLastTimestamp, &twa_empty_val);
                    twa_empty_computed = true;
                }
                Samples_value_at(&self->aux_chunk->samples, 0, a) = twa_empty_val;
            } else {
                self->aggregations[a].finalizeEmpty(
                    self->aggregationContexts[a],
                    &Samples_value_at(&self->aux_chunk->samples, 0, a));
            }
        }
    }
    for (size_t a = 0; a < numAggs; a++) {
        self->validPerAgg[a] = false;
    }
    self->validSamplesInBucket = false;

    self->aux_chunk->samples.timestamps[0] =
        calc_bucket_ts(self->bucketTS, self->aggregationLastTimestamp, self->aggregationTimeDelta);
    size_t n_samples = 1;
    /* Emit trailing empty buckets from the last output bucket through endTimestamp. TWA fillEmptyBuckets
     * may insert nothing for a gap (PM edge rule); LAST+EMPTY uses the same rule for the suffix so
     * bucket counts match TWA+EMPTY. */
    if (self->empty && !self->handled_twa_empty_suffix) {
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

EnrichedChunk *AggregationIterator_GetNextChunk(struct AbstractIterator *iter) {
    AggregationIterator *self = (AggregationIterator *)iter;
    AggregationClass *aggregation = &self->aggregations[0];
    void *aggregationContext = self->aggregationContexts[0];
    uint64_t aggregationTimeDelta = self->aggregationTimeDelta;
    bool is_reversed = self->reverse;
    bool multiAgg = self->numAggregations > 1;
    Sample sample;

    AbstractIterator *input = iter->input;
    EnrichedChunk *enrichedChunk = input->GetNext(input);
    size_t agg_n_samples = 0;
    int64_t si = 0, ei;

    if (!enrichedChunk || enrichedChunk->samples.num_samples == 0) {
        bool enter_finalize;
        EnrichedChunk *r = agg_iter_on_empty_chunk(
            self, aggregationTimeDelta, is_reversed, &agg_n_samples, &si, &enter_finalize);
        if (enter_finalize) {
            return agg_iter_finalize(self, aggregationTimeDelta, is_reversed, &sample);
        }
        return r;
    }

    if (self->empty && !self->initialized) {
        agg_iter_load_last_pre_window(self, &sample);
    }

    if (agg_iter_apply_empty_prefix(self,
                                      enrichedChunk,
                                      aggregationTimeDelta,
                                      is_reversed,
                                      multiAgg,
                                      &agg_n_samples,
                                      &si) != 0) {
        return NULL;
    }
    self->hasUnFinalizedContext = true;

    agg_iter_init_if_needed(self, enrichedChunk, si, aggregationTimeDelta, is_reversed, &sample);

    void (*appendValue)(void *, double, timestamp_t) = aggregation->appendValue;
    uint64_t contextScope = self->aggregationLastTimestamp + aggregationTimeDelta;
    self->aggregationLastTimestamp = BucketStartNormalize(self->aggregationLastTimestamp);
    while (enrichedChunk) {
        assert(self->reverse == enrichedChunk->rev || enrichedChunk->samples.num_samples == 0);
        if (self->numAggregations == 1 && aggregation->type == TS_AGG_MAX && !is_reversed) {
            if (agg_iter_process_chunk_max_fast_path(self,
                                                     enrichedChunk,
                                                     aggregation,
                                                     aggregationContext,
                                                     aggregationTimeDelta,
                                                     is_reversed,
                                                     appendValue,
                                                     &contextScope,
                                                     &agg_n_samples,
                                                     &si,
                                                     &ei,
                                                     &sample) != 0) {
                return NULL;
            }
        } else {
            if (agg_iter_process_chunk_general(self,
                                               enrichedChunk,
                                               aggregation,
                                               aggregationContext,
                                               aggregationTimeDelta,
                                               is_reversed,
                                               multiAgg,
                                               appendValue,
                                               &contextScope,
                                               &agg_n_samples,
                                               &si,
                                               &sample) != 0) {
                return NULL;
            }
        }

        EnrichedChunk *out =
            agg_iter_try_emit_partial(self, enrichedChunk, multiAgg, agg_n_samples);
        if (out) {
            return out;
        }
        enrichedChunk = input->GetNext(input);
        si = 0;
    }

    return agg_iter_finalize(self, aggregationTimeDelta, is_reversed, &sample);
}

void AggregationIterator_Close(struct AbstractIterator *iterator) {
    AggregationIterator *self = (AggregationIterator *)iterator;
    iterator->input->Close(iterator->input);
    for (size_t a = 0; a < self->numAggregations; a++) {
        self->aggregations[a].freeContext(self->aggregationContexts[a]);
    }
    FreeEnrichedChunk(self->aux_chunk);
    free(iterator);
}
