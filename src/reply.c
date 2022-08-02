/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "reply.h"

#include "dragonbox/dragonbox.h"
#include "query_language.h"
#include "redismodule.h"
#include "series_iterator.h"
#include "tsdb.h"
#include "ll2string.h"

#include "rmutil/alloc.h"

int ReplySeriesArrayPos(RedisModuleCtx *ctx,
                        Series *s,
                        bool withlabels,
                        RedisModuleString *limitLabels[],
                        ushort limitLabelsSize,
                        const RangeArgs *args,
                        bool rev) {
    RedisModule_ReplyWithArray(ctx, 3);
    RedisModule_ReplyWithString(ctx, s->keyName);
    if (withlabels) {
        ReplyWithSeriesLabels(ctx, s);
    } else if (limitLabelsSize > 0) {
        ReplyWithSeriesLabelsWithLimit(ctx, s, limitLabels, limitLabelsSize);
    } else {
        RedisModule_ReplyWithArray(ctx, 0);
    }
    ReplySeriesRange(ctx, s, args, rev);
    return REDISMODULE_OK;
}

#define PAGE 4096
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

    char buf[PAGE];
    int bufCount = 0;

    while ((arraylen < _count) && (enrichedChunk = iter->GetNext(iter))) {
        n = enrichedChunk->samples.num_samples;
        for (size_t i = 0; i < n; ++i) {
            if (bufCount > PAGE - 30) {
                RedisModule_ReplyWithStringBuffer(ctx, buf, bufCount);
                bufCount = 0;
                arraylen++;
            }
            buf[bufCount++] = '+';  // array type
            buf[bufCount++] = '2';  // array len
            buf[bufCount++] = '$';  // int type
            bufCount += ll2string(buf + bufCount, PAGE - bufCount, enrichedChunk->samples.timestamps[i]);
            buf[bufCount++] = '\n\r';

            buf[bufCount++] = ',';  // double type
            char dblbuf[20];
            dragonbox_double_to_chars(enrichedChunk->samples.values[i], dblbuf);
            int len = strlen(dblbuf);
            memcpy(buf + bufCount, dblbuf, len);
            bufCount += len;
            //bufCount += snprintf(buf + bufCount, PAGE - bufCount, "%.17g",enrichedChunk->samples.values[i]);
            buf[bufCount++] = '\n\r';
            buf[bufCount++] = '^';  // on the house
            buf[bufCount++] = '~';  // feeling good
        }
    }
    iter->Close(iter);

    if (bufCount > 0) {
        RedisModule_ReplyWithStringBuffer(ctx, buf, bufCount);
        arraylen++;
    }

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
    RedisModule_ReplyWithArray(ctx, limitLabelsSize);
    for (int i = 0; i < limitLabelsSize; i++) {
        bool found = false;
        for (int j = 0; j < series->labelsCount; ++j) {
            const char *key = RedisModule_StringPtrLen(series->labels[j].key, NULL);
            if (strcasecmp(key, limitLabels[i]) == 0) {
                RedisModule_ReplyWithArray(ctx, 2);
                RedisModule_ReplyWithString(ctx, series->labels[j].key);
                RedisModule_ReplyWithString(ctx, series->labels[j].value);
                found = true;
                break;
            }
        }
        if (!found) {
            RedisModule_ReplyWithArray(ctx, 2);
            RedisModule_ReplyWithCString(ctx, limitLabels[i]);
            RedisModule_ReplyWithNull(ctx);
        }
    }
}

void ReplyWithSeriesLabels(RedisModuleCtx *ctx, const Series *series) {
    RedisModule_ReplyWithArray(ctx, series->labelsCount);
    for (int i = 0; i < series->labelsCount; i++) {
        RedisModule_ReplyWithArray(ctx, 2);
        RedisModule_ReplyWithString(ctx, series->labels[i].key);
        RedisModule_ReplyWithString(ctx, series->labels[i].value);
    }
}

// double string presentation requires 15 digit integers +
// '.' + "e+" or "e-" + 3 digits of exponent
#define MAX_VAL_LEN 24
void ReplyWithSample(RedisModuleCtx *ctx, u_int64_t timestamp, double value) {
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithLongLong(ctx, timestamp);
    char buf[MAX_VAL_LEN + 1];
    dragonbox_double_to_chars(value, buf);
    RedisModule_ReplyWithSimpleString(ctx, buf);
}

void ReplyWithSeriesLastDatapoint(RedisModuleCtx *ctx, const Series *series) {
    if (SeriesGetNumSamples(series) == 0) {
        RedisModule_ReplyWithArray(ctx, 0);
    } else {
        ReplyWithSample(ctx, series->lastTimestamp, series->lastValue);
    }
}
