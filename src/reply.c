/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "reply.h"

#include "fpconv.h"
#include "redismodule.h"
#include "series_iterator.h"
#include "tsdb.h"

#include "rmutil/alloc.h"

int ReplySeriesArrayPos(RedisModuleCtx *ctx,
                        Series *s,
                        bool withlabels,
                        api_timestamp_t start_ts,
                        api_timestamp_t end_ts,
                        AggregationClass *aggObject,
                        int64_t time_delta,
                        long long maxResults,
                        bool rev) {
    RedisModule_ReplyWithArray(ctx, 3);
    RedisModule_ReplyWithString(ctx, s->keyName);
    if (withlabels) {
        ReplyWithSeriesLabels(ctx, s);
    } else {
        RedisModule_ReplyWithArray(ctx, 0);
    }
    ReplySeriesRange(ctx, s, start_ts, end_ts, aggObject, time_delta, maxResults, rev);
    return REDISMODULE_OK;
}

int ReplySeriesRange(RedisModuleCtx *ctx,
                     Series *series,
                     api_timestamp_t start_ts,
                     api_timestamp_t end_ts,
                     AggregationClass *aggObject,
                     int64_t time_delta,
                     long long maxResults,
                     bool rev) {
    Sample sample;
    long long arraylen = 0;

    // In case a retention is set shouldn't return chunks older than the retention
    // TODO: move to parseRangeArguments(?)
    if (series->retentionTime) {
        start_ts = series->lastTimestamp > series->retentionTime
                       ? max(start_ts, series->lastTimestamp - series->retentionTime)
                       : start_ts;
        // if new start_ts > end_ts, there are no results to return
        if (start_ts > end_ts) {
            return RedisModule_ReplyWithArray(ctx, 0);
        }
    }

    SeriesIterator iterator;
    if (SeriesQuery(series, &iterator, start_ts, end_ts, rev, aggObject, time_delta) != TSDB_OK) {
        return RedisModule_ReplyWithArray(ctx, 0);
    }

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    while (SeriesIteratorGetNext(&iterator, &sample) == CR_OK &&
           (maxResults == -1 || arraylen < maxResults)) {
        ReplyWithSample(ctx, sample.timestamp, sample.value);
        arraylen++;
    }
    SeriesIteratorClose(&iterator);

    RedisModule_ReplySetArrayLength(ctx, arraylen);
    return REDISMODULE_OK;
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
    int str_len = fpconv_dtoa(value, buf);
    buf[str_len] = '\0';
    RedisModule_ReplyWithSimpleString(ctx, buf);
}

void ReplyWithSeriesLastDatapoint(RedisModuleCtx *ctx, const Series *series) {
    if (SeriesGetNumSamples(series) == 0) {
        RedisModule_ReplyWithArray(ctx, 0);
    } else {
        ReplyWithSample(ctx, series->lastTimestamp, series->lastValue);
    }
}
