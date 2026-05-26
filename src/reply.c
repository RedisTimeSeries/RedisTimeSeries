/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "reply.h"

#include "enriched_chunk.h"
#include "query_language.h"
#include "series_iterator.h"
#include "tsdb.h"

#include "dragonbox/dragonbox.h"
#include "RedisModulesSDK/redismodule.h"
#include "utils/arr.h"
#include "rmutil/alloc.h"

// double string presentation requires 15 digit integers +
// '.' + "e+" or "e-" + 3 digits of exponent
#define MAX_VAL_LEN 24
int ReplyWithDoubleOrString(RedisModuleCtx *ctx, double d) {
    if (_is_resp3(ctx)) {
        return RedisModule_ReplyWithDouble(ctx, d);
    } else {
        char buf[MAX_VAL_LEN + 1];
        dragonbox_double_to_chars(d, buf);
        return RedisModule_ReplyWithSimpleString(ctx, buf);
    }
}

void ReplySetMapOrArrayLength(RedisModuleCtx *ctx, long len, bool divide_by_two) {
    if (_ReplyMap(ctx)) {
        RedisModule_ReplySetMapLength(ctx, divide_by_two ? len / 2 : len);
    } else {
        RedisModule_ReplySetArrayLength(ctx, len);
    }
}

void ReplyWithMapOrArray(RedisModuleCtx *ctx, long len, bool divide_by_two) {
    if (_ReplyMap(ctx)) {
        RedisModule_ReplyWithMap(ctx, divide_by_two ? len / 2 : len);
    } else {
        RedisModule_ReplyWithArray(ctx, len);
    }
}

void ReplySetSetOrArrayLength(RedisModuleCtx *ctx, long len) {
    if (_ReplySet(ctx)) {
        RedisModule_ReplySetSetLength(ctx, len);
    } else {
        RedisModule_ReplySetArrayLength(ctx, len);
    }
}

void ReplyWithSetOrArray(RedisModuleCtx *ctx, long len) {
    if (_ReplySet(ctx)) {
        RedisModule_ReplyWithSet(ctx, len);
    } else {
        RedisModule_ReplyWithArray(ctx, len);
    }
}

int ReplySeriesArrayPos(RedisModuleCtx *ctx,
                        Series *s,
                        bool withlabels,
                        RedisModuleString *limitLabels[],
                        uint16_t limitLabelsSize,
                        const RangeArgs *args,
                        bool rev,
                        bool print_reduced) {
    if (!_ReplyMap(ctx)) {
        RedisModule_ReplyWithArray(ctx, 3);
    }
    RedisModule_ReplyWithString(ctx, s->keyName);
    if (_ReplyMap(ctx)) {
        RedisModule_ReplyWithArray(ctx, print_reduced ? 4 : 3);
    }
    if (withlabels) {
        if (_ReplyMap(ctx) && print_reduced) {
            s->labelsCount -= 2; // Inorder to not print the __reducer__ etc
        }
        ReplyWithSeriesLabels(ctx, s);
        if (_ReplyMap(ctx) && print_reduced) {
            s->labelsCount += 2;
        }
    } else if (limitLabelsSize > 0) {
        ReplyWithSeriesLabelsWithLimit(ctx, s, limitLabels, limitLabelsSize);
    } else {
        ReplyWithMapOrArray(ctx, 0, false);
    }

    if (_ReplyMap(ctx)) {
        if (print_reduced) {
            // reply reducers
            RedisModule_ReplyWithMap(ctx, 1);
            RedisModule_ReplyWithCString(ctx, "reducers");
            RedisModule_ReplyWithArray(ctx, 1);
            RedisModule_ReplyWithString(ctx, s->labels[s->labelsCount - 2].value);

            // reply sources
            RedisModule_ReplyWithMap(ctx, 1);
            RedisModule_ReplyWithCString(ctx, "sources");
            RedisModule_ReplyWithArray(ctx, array_len((RedisModuleString **)s->srcKey));
            for (uint32_t i = 0; i < array_len((RedisModuleString **)s->srcKey); i++) {
                RedisModule_ReplyWithString(ctx, ((RedisModuleString **)s->srcKey)[i]);
            }
        } else {
            // reply aggregators
            RedisModule_ReplyWithMap(ctx, 1);
            RedisModule_ReplyWithCString(ctx, "aggregators");
            if (args->aggregationArgs.numClasses == 0) {
                RedisModule_ReplyWithArray(ctx, 0);
            } else {
                RedisModule_ReplyWithArray(ctx, args->aggregationArgs.numClasses);
                for (size_t i = 0; i < args->aggregationArgs.numClasses; i++) {
                    RedisModule_ReplyWithCString(
                        ctx, AggTypeEnumToStringLowerCase(args->aggregationArgs.classes[i]->type));
                }
            }
        }
    }
    ReplySeriesRange(ctx, s, args, rev);
    return REDISMODULE_OK;
}

/* Decide buffer size and starting timestamp for the forward agg pass that backs REVRANGE.
 * COUNT N: budget = N, narrow start to the analytical window of the last N buckets.
 * no COUNT: budget = analytical max bucket count over the full range; start unchanged.
 * Returns false (and sets *budget=0) when the range is provably empty. */
static bool plan_reverse_agg_window(const Series *series,
                                    const RangeArgs *args,
                                    size_t *budget,
                                    timestamp_t *narrowed_start) {
    *budget = 0;
    *narrowed_start = args->startTimestamp;
    if (series->totalSamples == 0) return false;

    timestamp_t effEnd = (args->endTimestamp > series->lastTimestamp) ? series->lastTimestamp
                                                                     : args->endTimestamp;
    if (effEnd < args->startTimestamp) return false;

    timestamp_t aln = 0;
    switch (args->alignment) {
        case StartAlignment: aln = args->startTimestamp; break;
        case EndAlignment: aln = args->endTimestamp; break;
        case TimestampAlignment: aln = args->timestampAlignment; break;
        default: break;
    }
    const timestamp_t bucketDuration = args->aggregationArgs.timeDelta;
    const timestamp_t lastBucketStart = CalcBucketStart(effEnd, bucketDuration, aln);

    if (args->count != -1) {
        if (args->count == 0) return false;
        *budget = (size_t)args->count;
        timestamp_t windowSize = (timestamp_t)(*budget - 1) * bucketDuration;
        if (lastBucketStart >= windowSize &&
            lastBucketStart - windowSize > args->startTimestamp) {
            *narrowed_start = lastBucketStart - windowSize;
        }
    } else {
        timestamp_t firstBucketStart =
            CalcBucketStart(args->startTimestamp, bucketDuration, aln);
        *budget = (size_t)((lastBucketStart - firstBucketStart) / bucketDuration) + 1;
        if (!args->aggregationArgs.empty && *budget > series->totalSamples) {
            *budget = series->totalSamples;
        }
    }
    return *budget > 0;
}

/* Run one forward query starting at `start_ts` and append every sample into the ring at
 * (written % budget). Allocates val_buf lazily once vps is known. If `early_stop`, halts as soon
 * as `budget` samples have been written. Returns the total number of samples written (may
 * exceed budget on a full pass). */
static size_t fill_ring(Series *series,
                        const RangeArgs *args,
                        timestamp_t start_ts,
                        size_t budget,
                        bool early_stop,
                        timestamp_t *ts_buf,
                        double **val_buf,
                        size_t *vps) {
    RangeArgs q = *args;
    q.startTimestamp = start_ts;
    q.count = -1;
    AbstractIterator *iter = SeriesQuery(series, &q, false, true);
    EnrichedChunk *chunk;
    size_t written = 0;
    while ((chunk = iter->GetNext(iter))) {
        size_t n = chunk->samples.num_samples;
        if (n == 0) continue;
        if (!*val_buf) {
            *vps = chunk->samples.values_per_sample;
            *val_buf = malloc(budget * (*vps) * sizeof(double));
        }
        for (size_t i = 0; i < n; ++i) {
            size_t slot = written % budget;
            ts_buf[slot] = chunk->samples.timestamps[i];
            memcpy(*val_buf + slot * (*vps),
                   Samples_values_row_ptr(&chunk->samples, i),
                   (*vps) * sizeof(double));
            written++;
        }
        if (early_stop && written >= budget) break;
    }
    iter->Close(iter);
    return written;
}

/* Emit the ring backward from the newest slot. Newest is at (written - 1) % budget. */
static void reply_ring_reverse(RedisModuleCtx *ctx,
                               const timestamp_t *ts_buf,
                               double *val_buf,
                               size_t budget,
                               size_t written,
                               size_t vps) {
    size_t total = (written < budget) ? written : budget;
    RedisModule_ReplyWithArray(ctx, total);
    for (size_t k = 0; k < total; ++k) {
        size_t idx = (written - 1 - k) % budget;
        if (vps > 1) {
            ReplyWithMultiAggSample(ctx, ts_buf[idx], &val_buf[idx * vps], vps);
        } else {
            ReplyWithSample(ctx, ts_buf[idx], val_buf ? val_buf[idx] : 0.0);
        }
    }
}

/* REVRANGE + aggregation: aggregation pipeline must run forward (raw aggregators were wrong in
 * reverse), so iterate forward into a ring sized by `plan_reverse_agg_window`, then emit the
 * ring backward. If a count-limited narrow window under-fills (filters dropped buckets), retry
 * once over the full range. */
static int ReplyReverseAgg(RedisModuleCtx *ctx, Series *series, const RangeArgs *args) {
    size_t budget;
    timestamp_t narrowed_start;
    if (!plan_reverse_agg_window(series, args, &budget, &narrowed_start)) {
        RedisModule_ReplyWithArray(ctx, 0);
        return REDISMODULE_OK;
    }

    timestamp_t *ts_buf = malloc(budget * sizeof(timestamp_t));
    double *val_buf = NULL;
    size_t vps = 1;

    size_t written = fill_ring(series, args, narrowed_start, budget, true, ts_buf, &val_buf, &vps);
    // The narrow window guessed "the last N buckets fit in [narrowed_start, end]". That guess
    // can be wrong when buckets inside the window get erased — FILTER_BY_VALUE / FILTER_BY_TS
    // can drop samples that would otherwise have anchored a bucket, and without EMPTY a bucket
    // with no surviving samples isn't emitted at all. When that happens we end up with fewer
    // than N buckets and the real "last N" lives earlier in time than the narrow window saw.
    // Fall back to a full forward scan and let the ring keep the latest N as iteration advances.
    if (args->count != -1 && written < budget && narrowed_start > args->startTimestamp) {
        written = fill_ring(series, args, args->startTimestamp, budget, false,
                            ts_buf, &val_buf, &vps);
    }

    reply_ring_reverse(ctx, ts_buf, val_buf, budget, written, vps);
    free(ts_buf);
    free(val_buf);
    return REDISMODULE_OK;
}

int ReplySeriesRange(RedisModuleCtx *ctx, Series *series, const RangeArgs *args, bool reverse) {
    if (reverse && args->aggregationArgs.numClasses > 0) {
        return ReplyReverseAgg(ctx, series, args);
    }

    long long arraylen = 0;
    long long _count = LLONG_MAX;
    unsigned int n;
    if (args->count != -1) {
        _count = args->count;
    }

    AbstractIterator *iter = SeriesQuery(series, args, reverse, true);
    EnrichedChunk *enrichedChunk;
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    while ((arraylen < _count) && (enrichedChunk = iter->GetNext(iter))) {
        n = (unsigned int)min(_count - arraylen, enrichedChunk->samples.num_samples);
        size_t vps = enrichedChunk->samples.values_per_sample;
        for (size_t i = 0; i < n; ++i) {
            if (vps > 1) {
                ReplyWithMultiAggSample(ctx,
                                        enrichedChunk->samples.timestamps[i],
                                        Samples_values_row_ptr(&enrichedChunk->samples, i),
                                        vps);
            } else {
                ReplyWithSample(ctx,
                                enrichedChunk->samples.timestamps[i],
                                Samples_value_at(&enrichedChunk->samples, i, 0));
            }
        }
        arraylen += n;
    }
    iter->Close(iter);

    RedisModule_ReplySetArrayLength(ctx, arraylen);
    return REDISMODULE_OK;
}

void ReplyWithSeriesLabelsWithLimit(RedisModuleCtx *ctx,
                                    const Series *series,
                                    RedisModuleString **limitLabels,
                                    uint16_t limitLabelsSize) {
    const char **limitLabelsStr = malloc(sizeof(char *) * limitLabelsSize);
    for (int i = 0; i < limitLabelsSize; i++) {
        limitLabelsStr[i] = RedisModule_StringPtrLen(limitLabels[i], NULL);
    }
    ReplyWithSeriesLabelsWithLimitC(ctx, series, limitLabelsStr, limitLabelsSize);
    free(limitLabelsStr);
}

void ReplyWithSeriesLabelsWithLimitC(RedisModuleCtx *ctx,
                                     const Series *series,
                                     const char **limitLabels,
                                     uint16_t limitLabelsSize) {
    ReplyWithMapOrArray(ctx, limitLabelsSize, false);
    for (int i = 0; i < limitLabelsSize; i++) {
        bool found = false;
        for (int j = 0; j < series->labelsCount; ++j) {
            const char *key = RedisModule_StringPtrLen(series->labels[j].key, NULL);
            if (strcasecmp(key, limitLabels[i]) == 0) {
                if (!_ReplyMap(ctx)) {
                    RedisModule_ReplyWithArray(ctx, 2);
                }
                RedisModule_ReplyWithString(ctx, series->labels[j].key);
                RedisModule_ReplyWithString(ctx, series->labels[j].value);
                found = true;
                break;
            }
        }
        if (!found) {
            if (!_ReplyMap(ctx)) {
                RedisModule_ReplyWithArray(ctx, 2);
            }
            RedisModule_ReplyWithCString(ctx, limitLabels[i]);
            RedisModule_ReplyWithNull(ctx);
        }
    }
}

void ReplyWithSeriesLabels(RedisModuleCtx *ctx, const Series *series) {
    ReplyWithMapOrArray(ctx, series->labelsCount, false);
    for (int i = 0; i < series->labelsCount; i++) {
        if (!_ReplyMap(ctx)) {
            RedisModule_ReplyWithArray(ctx, 2);
        }
        RedisModule_ReplyWithString(ctx, series->labels[i].key);
        if (likely(series->labels[i].value != NULL))
            RedisModule_ReplyWithString(ctx, series->labels[i].value);
        else
            RedisModule_ReplyWithNull(ctx);
    }
}

void ReplyWithSample(RedisModuleCtx *ctx, uint64_t timestamp, double value) {
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithLongLong(ctx, timestamp);
    ReplyWithDoubleOrString(ctx, value);
}

void ReplyWithMultiAggSample(RedisModuleCtx *ctx,
                             uint64_t timestamp,
                             double *values,
                             size_t num_values) {
    RedisModule_ReplyWithArray(ctx, 1 + num_values);
    RedisModule_ReplyWithLongLong(ctx, timestamp);
    for (size_t i = 0; i < num_values; i++) {
        ReplyWithDoubleOrString(ctx, values[i]);
    }
}

void ReplyWithSeriesLastDatapoint(RedisModuleCtx *ctx, const Series *series) {
    if (SeriesGetNumSamples(series) == 0) {
        RedisModule_ReplyWithArray(ctx, 0);
    } else {
        ReplyWithSample(ctx, series->lastTimestamp, series->lastValue);
    }
}
