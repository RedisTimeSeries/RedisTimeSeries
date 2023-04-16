/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
 */

#include "reply.h"

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
int RedisModule_ReplyWithDoubleOrString(RedisModuleCtx *ctx, double d) {
    if (_is_resp3(ctx)) {
        return RedisModule_ReplyWithDouble(ctx, d);
    } else {
        char buf[MAX_VAL_LEN + 1];
        dragonbox_double_to_chars(d, buf);
        return RedisModule_ReplyWithSimpleString(ctx, buf);
    }
}

void RedisModule_ReplySetMapOrArrayLength(RedisModuleCtx *ctx, long len, bool devide_by_two) {
    if (_ReplyMap(ctx)) {
        RedisModule_ReplySetMapLength(ctx, devide_by_two ? len / 2 : len);
    } else {
        RedisModule_ReplySetArrayLength(ctx, len);
    }
}

void RedisModule_ReplyWithMapOrArray(RedisModuleCtx *ctx, long len, bool devide_by_two) {
    if (_ReplyMap(ctx)) {
        RedisModule_ReplyWithMap(ctx, devide_by_two ? len / 2 : len);
    } else {
        RedisModule_ReplyWithArray(ctx, len);
    }
}

void RedisModule_ReplySetSetOrArrayLength(RedisModuleCtx *ctx, long len) {
    if (_ReplySet(ctx)) {
        RedisModule_ReplySetSetLength(ctx, len);
    } else {
        RedisModule_ReplySetArrayLength(ctx, len);
    }
}

void RedisModule_ReplyWithSetOrArray(RedisModuleCtx *ctx, long len) {
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
                        ushort limitLabelsSize,
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
        RedisModule_ReplyWithMapOrArray(ctx, 0, false);
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
            if (!args->aggregationArgs.aggregationClass) {
                RedisModule_ReplyWithArray(ctx, 0);
            } else {
                RedisModule_ReplyWithArray(ctx, 1);
                RedisModule_ReplyWithCString(
                    ctx,
                    AggTypeEnumToStringLowerCase(args->aggregationArgs.aggregationClass->type));
            }
        }
    }
    ReplySeriesRange(ctx, s, args, rev);
    return REDISMODULE_OK;
}

int ReplySeriesRange(RedisModuleCtx *ctx, Series *series, const RangeArgs *args, bool reverse) {
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
        for (size_t i = 0; i < n; ++i) {
            ReplyWithSample(
                ctx, enrichedChunk->samples.timestamps[i], enrichedChunk->samples.values[i]);
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
                                    ushort limitLabelsSize) {
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
                                     ushort limitLabelsSize) {
    RedisModule_ReplyWithMapOrArray(ctx, limitLabelsSize, false);
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
    RedisModule_ReplyWithMapOrArray(ctx, series->labelsCount, false);
    for (int i = 0; i < series->labelsCount; i++) {
        if (!_ReplyMap(ctx)) {
            RedisModule_ReplyWithArray(ctx, 2);
        }
        RedisModule_ReplyWithString(ctx, series->labels[i].key);
        RedisModule_ReplyWithString(ctx, series->labels[i].value);
    }
}

void ReplyWithSample(RedisModuleCtx *ctx, u_int64_t timestamp, double value) {
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithLongLong(ctx, timestamp);
    RedisModule_ReplyWithDoubleOrString(ctx, value);
}

void ReplyWithSeriesLastDatapoint(RedisModuleCtx *ctx, const Series *series) {
    if (SeriesGetNumSamples(series) == 0) {
        RedisModule_ReplyWithArray(ctx, 0);
    } else {
        ReplyWithSample(ctx, series->lastTimestamp, series->lastValue);
    }
}
