/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "reply.h"

#include "redismodule.h"
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
    void *context = NULL;
    long long arraylen = 0;
    timestamp_t last_agg_timestamp;

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

    SeriesIterator iterator = SeriesQuery(series, start_ts, end_ts, rev);
    if (iterator.series == NULL) {
        return RedisModule_ReplyWithArray(ctx, 0);
    }

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    if (aggObject == NULL) {
        // No aggregation
        while (SeriesIteratorGetNext(&iterator, &sample) == CR_OK &&
               (maxResults == -1 || arraylen < maxResults)) {
            ReplyWithSample(ctx, sample.timestamp, sample.value);
            arraylen++;
        }
    } else {
        bool firstSample = TRUE;
        context = aggObject->createContext();
        // setting the first timestamp of the aggregation
        timestamp_t init_ts = (rev == false)
                                  ? series->funcs->GetFirstTimestamp(iterator.currentChunk)
                                  : series->funcs->GetLastTimestamp(iterator.currentChunk);
        last_agg_timestamp = init_ts - (init_ts % time_delta);

        while (SeriesIteratorGetNext(&iterator, &sample) == CR_OK &&
               (maxResults == -1 || arraylen < maxResults)) {
            if ((iterator.reverse == false &&
                 sample.timestamp >= last_agg_timestamp + time_delta) ||
                (iterator.reverse == true && sample.timestamp < last_agg_timestamp)) {
                if (firstSample == FALSE) {
                    double value;
                    if (aggObject->finalize(context, &value) == TSDB_OK) {
                        ReplyWithSample(ctx, last_agg_timestamp, value);
                        aggObject->resetContext(context);
                        arraylen++;
                    }
                }
                last_agg_timestamp = sample.timestamp - (sample.timestamp % time_delta);
            }
            firstSample = FALSE;
            aggObject->appendValue(context, sample.value);
        }
    }
    SeriesIteratorClose(&iterator);

    if (aggObject != NULL) {
        if (arraylen != maxResults) {
            // reply last bucket of data
            double value;
            if (aggObject->finalize(context, &value) == TSDB_OK) {
                ReplyWithSample(ctx, last_agg_timestamp, value);
                aggObject->resetContext(context);
                arraylen++;
            }
        }
        aggObject->freeContext(context);
    }

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
    char buf[MAX_VAL_LEN];
    snprintf(buf, MAX_VAL_LEN, "%.15g", value);
    RedisModule_ReplyWithSimpleString(ctx, buf);
}

void ReplyWithSeriesLastDatapoint(RedisModuleCtx *ctx, const Series *series) {
    if (SeriesGetNumSamples(series) == 0) {
        RedisModule_ReplyWithArray(ctx, 0);
    } else {
        ReplyWithSample(ctx, series->lastTimestamp, series->lastValue);
    }
}
