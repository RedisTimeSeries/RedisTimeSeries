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

typedef struct MRangeArgs
{
    api_timestamp_t startTimestamp;
    api_timestamp_t endTimestamp;
    AggregationArgs aggregationArgs;
    bool withLabels;
    long long count; // AKA limit
    QueryPredicate *queryPredicates;
    size_t queryPredicatesCount;
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
                         api_timestamp_t *time_delta,
                         AggregationClass **agg_object);

int parseRangeArguments(RedisModuleCtx *ctx,
                        Series *series,
                        int start_index,
                        RedisModuleString **argv,
                        api_timestamp_t *start_ts,
                        api_timestamp_t *end_ts);

int parseCountArgument(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, long long *count);

QueryPredicateList *parseLabelListFromArgs(RedisModuleCtx *ctx,
                                           RedisModuleString **argv,
                                           int start,
                                           int query_count,
                                           int *response);

int parseMRangeCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, MRangeArgs *out);
void MRangeArgs_Free(MRangeArgs *args);

#endif // REDISTIMESERIES_QUERY_LANGUAGE_H
