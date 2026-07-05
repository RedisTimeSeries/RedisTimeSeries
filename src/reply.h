/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef REDISTIMESERIES_REPLY_H
#define REDISTIMESERIES_REPLY_H

#include "generic_chunk.h"
#include "query_language.h"
#include "tsdb.h"

#include "RedisModulesSDK/redismodule.h"

void ReplySetMapOrArrayLength(RedisModuleCtx *ctx, long len, bool divide_by_two);
void ReplyWithMapOrArray(RedisModuleCtx *ctx, long len, bool divide_by_two);
void ReplySetSetOrArrayLength(RedisModuleCtx *ctx, long len);
void ReplyWithSetOrArray(RedisModuleCtx *ctx, long len);

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
                        uint16_t limitLabelsSize,
                        const RangeArgs *args,
                        bool rev,
                        bool print_reduced);

int ReplySeriesRange(RedisModuleCtx *ctx, Series *series, const RangeArgs *args, bool rev);

// Reply one pivoted row: [timestamp, [value_0, value_1, ..., value_{num_values-1}]].
void ReplyWithPivotSample(RedisModuleCtx *ctx,
                          uint64_t timestamp,
                          const double *values,
                          size_t num_values);

// Merge num_keys time-ordered per-key chunk iterators into a timestamp-major reply (one row per
// distinct timestamp, NaN where a key has no sample). aggs_per_key[i] is the number of aggregation
// values key i contributes per row. Each row is flat: [ts, [v0, v1, ...]].
// Consumes and closes each iterator.
int ReplySeriesNRange(RedisModuleCtx *ctx,
                      AbstractIterator **iters,
                      size_t num_keys,
                      const size_t *aggs_per_key,
                      long long count,
                      bool reverse);

void ReplyWithSeriesLabels(RedisModuleCtx *ctx, const Series *series);
void ReplyWithSeriesLabelsWithLimit(RedisModuleCtx *ctx,
                                    const Series *series,
                                    RedisModuleString **limitLabels,
                                    uint16_t limitLabelsSize);
void ReplyWithSeriesLabelsWithLimitC(RedisModuleCtx *ctx,
                                     const Series *series,
                                     const char **limitLabels,
                                     uint16_t limitLabelsSize);

void ReplyWithSample(RedisModuleCtx *ctx, uint64_t timestamp, double value);
void ReplyWithMultiAggSample(RedisModuleCtx *ctx,
                             uint64_t timestamp,
                             double *values,
                             size_t num_values);

void ReplyWithSeriesLastDatapoint(RedisModuleCtx *ctx, const Series *series);

// Reply for one key's pre-aggregated group in the multi-agg cluster path.
// group[0] provides the key name and labels; group[0..numAggTypes-1] each hold one agg type's
// values.
int ReplyMultiAggSeriesGroup(RedisModuleCtx *ctx,
                             Series **group,
                             size_t numAggTypes,
                             bool withLabels,
                             RedisModuleString *limitLabels[],
                             uint16_t limitLabelsSize,
                             const RangeArgs *args,
                             bool rev);

#endif // REDISTIMESERIES_REPLY_H
