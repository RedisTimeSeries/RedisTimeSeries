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

/* TS.RANGE / TS.REVRANGE reply.
 *
 * REVRANGE is a mirror image of RANGE: produce the regular forward result into a temporary
 * buffer, then walk it backwards at reply time when reverse is requested. This keeps reverse
 * out of all downstream iterators. COUNT is applied at reply time so that REVRANGE COUNT N
 * returns the latest N samples chronologically, in reverse order.
 *
 * Forward path with COUNT exits the iterator loop as soon as the first N samples are
 * buffered (and caps the last chunk's copy to fit), so it never reads or allocates beyond
 * the user-visible limit. Reverse path must drain the iterator because the latest N
 * samples are only known once the full window has been seen. */
int ReplySeriesRange(RedisModuleCtx *ctx, Series *series, const RangeArgs *args, bool reverse) {
    AbstractIterator *iter = SeriesQuery(series, args, false, true);
    EnrichedChunk *enrichedChunk;

    timestamp_t *timestamps = NULL;
    double *values = NULL;
    size_t total = 0;
    size_t cap = 0;
    size_t vps = 1;
    const bool forward_count_limited = (!reverse && args->count != -1);
    const size_t forward_budget = forward_count_limited ? (size_t)args->count : 0;

    while ((enrichedChunk = iter->GetNext(iter))) {
        size_t n = enrichedChunk->samples.num_samples;
        if (n == 0) {
            continue;
        }
        vps = enrichedChunk->samples.values_per_sample;

        if (forward_count_limited) {
            size_t remaining = forward_budget - total;
            if (n > remaining) {
                n = remaining;
            }
        }

        if (total + n > cap) {
            size_t new_cap = cap == 0 ? 64 : cap * 2;
            while (new_cap < total + n) {
                new_cap *= 2;
            }
            timestamps = realloc(timestamps, new_cap * sizeof(timestamp_t));
            values = realloc(values, new_cap * vps * sizeof(double));
            cap = new_cap;
        }

        memcpy(timestamps + total,
               enrichedChunk->samples.timestamps,
               n * sizeof(timestamp_t));
        memcpy(values + total * vps,
               enrichedChunk->samples._values,
               n * vps * sizeof(double));
        total += n;

        if (forward_count_limited && total >= forward_budget) {
            break;
        }
    }
    iter->Close(iter);

    long long reply_count = (long long)total;
    if (args->count != -1 && args->count < reply_count) {
        reply_count = args->count;
    }

    RedisModule_ReplyWithArray(ctx, reply_count);
    for (long long k = 0; k < reply_count; ++k) {
        size_t i = reverse ? (total - 1 - (size_t)k) : (size_t)k;
        if (vps > 1) {
            ReplyWithMultiAggSample(ctx, timestamps[i], &values[i * vps], vps);
        } else {
            ReplyWithSample(ctx, timestamps[i], values[i * vps]);
        }
    }

    free(timestamps);
    free(values);
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
