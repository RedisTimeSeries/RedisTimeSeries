/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "generic_chunk.h"
#include "redismodule.h"
#include "tsdb.h"

#ifndef REDISTIMESERIES_REPLY_H
#define REDISTIMESERIES_REPLY_H

int ReplySeriesArrayPos(RedisModuleCtx *ctx,
                        Series *series,
                        bool withlabels,
                        api_timestamp_t start_ts,
                        api_timestamp_t end_ts,
                        AggregationClass *aggObject,
                        int64_t time_delta,
                        long long maxResults,
                        bool rev);

int ReplySeriesRange(RedisModuleCtx *ctx,
                     Series *series,
                     api_timestamp_t start_ts,
                     api_timestamp_t end_ts,
                     AggregationClass *aggObject,
                     int64_t time_delta,
                     long long maxResults,
                     bool rev);

void ReplyWithSeriesLabels(RedisModuleCtx *ctx, const Series *series);

void ReplyWithSample(RedisModuleCtx *ctx, u_int64_t timestamp, double value);

void ReplyWithSeriesLastDatapoint(RedisModuleCtx *ctx, const Series *series);

#endif // REDISTIMESERIES_REPLY_H
