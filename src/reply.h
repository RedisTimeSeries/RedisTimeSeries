/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
 */

#ifndef REDISTIMESERIES_REPLY_H
#define REDISTIMESERIES_REPLY_H

#include "generic_chunk.h"
#include "query_language.h"
#include "tsdb.h"

#include "RedisModulesSDK/redismodule.h"

void RedisModule_ReplySetMapOrArrayLength(RedisModuleCtx *ctx, long len, bool devide_by_two);
void RedisModule_ReplyWithMapOrArray(RedisModuleCtx *ctx, long len, bool devide_by_two);
void RedisModule_ReplySetSetOrArrayLength(RedisModuleCtx *ctx, long len);
void RedisModule_ReplyWithSetOrArray(RedisModuleCtx *ctx, long len);

static inline bool _is_resp3(RedisModuleCtx *ctx) {
    int ctxFlags = RedisModule_GetContextFlags(ctx);
    if (ctxFlags & REDISMODULE_CTX_FLAGS_RESP3) {
        return true;
    }
    return false;
}

#define _ReplyMap(ctx) (RedisModule_ReplyWithMap != NULL && _is_resp3(ctx))
#define _ReplySet(ctx) (RedisModule_ReplyWithSet != NULL && _is_resp3(ctx))

int ReplySeriesArrayPos(RedisModuleCtx *ctx,
                        Series *series,
                        bool withlabels,
                        RedisModuleString *limitLabels[],
                        ushort limitLabelsSize,
                        const RangeArgs *args,
                        bool rev,
                        bool print_reduced);

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

void ReplyWithSample(RedisModuleCtx *ctx, u_int64_t timestamp, double value);

void ReplyWithSeriesLastDatapoint(RedisModuleCtx *ctx, const Series *series);

#endif // REDISTIMESERIES_REPLY_H
