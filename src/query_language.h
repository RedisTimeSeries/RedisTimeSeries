/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "config.h"
#include "generic_chunk.h"
#include "indexer.h"
#include "redismodule.h"
#include "tsdb.h"

#include "rmutil/alloc.h"
#include "rmutil/strings.h"
#include "rmutil/util.h"

#ifndef REDISTIMESERIES_QUERY_LANGUAGE_H
#define REDISTIMESERIES_QUERY_LANGUAGE_H

typedef struct AggregationArgs
{
    api_timestamp_t timeDelta;
    AggregationClass *aggregationClass;
} AggregationArgs;

typedef struct RangeArgs
{
    api_timestamp_t startTimestamp;
    api_timestamp_t endTimestamp;
    long long count; // AKA limit
    AggregationArgs aggregationArgs;

} RangeArgs;

typedef struct MRangeArgs
{
    RangeArgs rangeArgs;
    bool withLabels;
    QueryPredicateList *queryPredicates;
    const char *groupByLabel;
    MultiSeriesReduceOp gropuByReducerOp;
    bool reverse;
} MRangeArgs;

int parseLabelsFromArgs(RedisModuleString **argv, int argc, size_t *label_count, Label **labels);

int ParseDuplicatePolicy(RedisModuleCtx *ctx,
                         RedisModuleString **argv,
                         int argc,
                         const char *arg_prefix,
                         DuplicatePolicy *policy);

int parseCreateArgs(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, CreateCtx *cCtx);

int _parseAggregationArgs(RedisModuleCtx *ctx,
                          RedisModuleString **argv,
                          int argc,
                          api_timestamp_t *time_delta,
                          int *agg_type);

int parseAggregationArgs(RedisModuleCtx *ctx,
                         RedisModuleString **argv,
                         int argc,
                         AggregationArgs *out);

int parseRangeArguments(RedisModuleCtx *ctx,
                        Series *series,
                        int start_index,
                        RedisModuleString **argv,
                        int argc,
                        RangeArgs *out);


QueryPredicateList *parseLabelListFromArgs(RedisModuleCtx *ctx,
                                           RedisModuleString **argv,
                                           int start,
                                           int query_count,
                                           int *response);

int parseMRangeCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, MRangeArgs *out);
void MRangeArgs_Free(MRangeArgs *args);

#endif // REDISTIMESERIES_QUERY_LANGUAGE_H
