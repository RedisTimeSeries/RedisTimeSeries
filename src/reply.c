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

/* Growable buffer used by the reverse (TS.REVRANGE) path to collect samples
 * chronologically before they get flipped at reply time. */
typedef struct {
    timestamp_t *timestamps;
    double *values;
    size_t total;
    size_t cap;
} ReverseBuffer;

/* Forward path per-chunk action: emit n samples of `chunk` straight to ctx. */
static inline void emit_chunk_forward(RedisModuleCtx *ctx,
                                      const EnrichedChunk *chunk,
                                      size_t n,
                                      size_t vps) {
    for (size_t i = 0; i < n; ++i) {
        if (vps > 1) {
            ReplyWithMultiAggSample(ctx, chunk->samples.timestamps[i],
                                    Samples_values_row_ptr(&chunk->samples, i), vps);
        } else {
            ReplyWithSample(ctx, chunk->samples.timestamps[i],
                            Samples_value_at(&chunk->samples, i, 0));
        }
    }
}

/* Reverse path final emit: walk `buf` newest-to-oldest into ctx, then release
 * the buffer storage. Caller must not touch `buf` after this. */
static inline void flush_buffer_reverse(RedisModuleCtx *ctx,
                                        ReverseBuffer *buf,
                                        size_t vps) {
    RedisModule_ReplyWithArray(ctx, buf->total);
    for (size_t k = 0; k < buf->total; ++k) {
        size_t i = buf->total - 1 - k;
        if (vps > 1) {
            ReplyWithMultiAggSample(ctx, buf->timestamps[i], &buf->values[i * vps], vps);
        } else {
            ReplyWithSample(ctx, buf->timestamps[i], buf->values[i * vps]);
        }
    }
    free(buf->timestamps);
    free(buf->values);
}

/* Reverse path per-chunk action: append n samples of `chunk` to `buf`, then
 * drop the oldest excess so the buffer never grows past ~COUNT + chunk_size. */
static inline void append_chunk_reverse(ReverseBuffer *buf,
                                        const EnrichedChunk *chunk,
                                        size_t n,
                                        size_t vps,
                                        bool count_limited,
                                        size_t budget) {
    if (buf->total + n > buf->cap) {
        size_t new_cap = buf->cap == 0 ? 64 : buf->cap * 2;
        while (new_cap < buf->total + n) {
            new_cap *= 2;
        }
        buf->timestamps = realloc(buf->timestamps, new_cap * sizeof(timestamp_t));
        buf->values = realloc(buf->values, new_cap * vps * sizeof(double));
        buf->cap = new_cap;
    }
    memcpy(buf->timestamps + buf->total, chunk->samples.timestamps, n * sizeof(timestamp_t));
    memcpy(buf->values + buf->total * vps, chunk->samples._values, n * vps * sizeof(double));
    buf->total += n;
    if (count_limited && buf->total > budget) {
        size_t drop = buf->total - budget;
        memmove(buf->timestamps, buf->timestamps + drop, budget * sizeof(timestamp_t));
        memmove(buf->values, buf->values + drop * vps, budget * vps * sizeof(double));
        buf->total = budget;
    }
}

/* TS.RANGE / TS.REVRANGE reply.
 *
 * Forward (TS.RANGE) streams the result via POSTPONED_ARRAY_LEN with per-sample emit and
 * an early break once COUNT samples have been sent — O(min(N,total)) work, no intermediate
 * buffer.
 *
 * Reverse (TS.REVRANGE) cannot stream because downstream iterators are forward-only;
 * collect samples into a temporary buffer, then emit in reverse at reply time. COUNT on
 * the reverse path is enforced by dropping the oldest excess after each chunk so the
 * buffer never grows beyond ~COUNT + chunk_size. */
int ReplySeriesRange(RedisModuleCtx *ctx, Series *series, const RangeArgs *args, bool reverse) {
    AbstractIterator *iter = SeriesQuery(series, args, false, true);
    EnrichedChunk *chunk;
    const bool count_limited = (args->count != -1);
    const long long budget = count_limited ? args->count : 0;

    // Forward streams straight to ctx via POSTPONED array length tracked in
    // `arraylen`. Reverse accumulates into `buf` and walks it backwards at the
    // end (downstream iterators are forward-only).
    long long arraylen = 0;
    ReverseBuffer buf = { .timestamps = NULL, .values = NULL, .total = 0, .cap = 0 };
    size_t vps = 1;

    if (!reverse) {
        RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    }

    // Loop condition encodes both behaviors: reverse must walk the entire window
    // (`reverse ||` short-circuits the budget check); forward stops as soon as
    // it has emitted COUNT samples.
    while ((reverse || !count_limited || arraylen < budget) &&
           (chunk = iter->GetNext(iter))) {
        size_t n = chunk->samples.num_samples;
        if (n == 0) {
            continue;
        }
        vps = chunk->samples.values_per_sample;

        if (!reverse) {
            // Forward + COUNT: truncate this chunk to the remaining budget so
            // the outer condition exits the loop next iteration.
            if (count_limited && (long long)n > budget - arraylen) {
                n = (size_t)(budget - arraylen);
            }
            emit_chunk_forward(ctx, chunk, n, vps);
            arraylen += n;
        } else {
            append_chunk_reverse(&buf, chunk, n, vps, count_limited, (size_t)budget);
        }
    }
    iter->Close(iter);

    if (!reverse) {
        RedisModule_ReplySetArrayLength(ctx, arraylen);
    } else {
        flush_buffer_reverse(ctx, &buf, vps);
    }
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
