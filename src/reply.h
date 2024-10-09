/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
 */

#include "generic_chunk.h"
#include "query_language.h"
#include "redismodule.h"
#include "tsdb.h"

#ifndef REDISTIMESERIES_REPLY_H
#define REDISTIMESERIES_REPLY_H

int ReplySeriesArrayPos(RedisModuleCtx *ctx,
                        Series *series,
                        bool withlabels,
                        RedisModuleString *limitLabels[],
                        ushort limitLabelsSize,
                        const RangeArgs *args,
                        bool rev);

int ReplySeriesRange(RedisModuleCtx *ctx, Series *series, const RangeArgs *args, bool rev);

void ReplyWithSeriesLabels(RedisModuleCtx *ctx, const Series *series);
void ReplyWithSeriesLabelsWithLimit(RedisModuleCtx *ctx,
                                    const Series *series,
                                    RedisModuleString **limitLabels,
                                    ushort limitLabelsSize);
void ReplyWithSeriesLabelsWithLimitC(RedisModuleCtx *ctx,
                                     const Series *series,
                                     const char **limitLabels,
                                     ushort limitLabelsSize);

void ReplyWithSample(RedisModuleCtx *ctx, uint64_t timestamp, double value);

void ReplyWithSeriesLastDatapoint(RedisModuleCtx *ctx, const Series *series);

#endif // REDISTIMESERIES_REPLY_H
