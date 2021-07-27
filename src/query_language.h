/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "compaction.h"
#include "config.h"
#include "generic_chunk.h"
#include "indexer.h"
#include "redismodule.h"

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

typedef struct FilterByValueArgs
{
    bool hasValue;
    double min;
    double max;
} FilterByValueArgs;

#define MAX_TS_VALUES_FILTER 128

typedef struct FilterByTSArgs
{
    bool hasValue;
    size_t count;
    timestamp_t values[MAX_TS_VALUES_FILTER];
} FilterByTSArgs;

typedef enum RangeAlignment
{
    DefaultAlignment,
    StartAlignment,
    EndAlignment,
    TimestampAlignment
} RangeAlignment;

typedef struct RangeArgs
{
    api_timestamp_t startTimestamp;
    api_timestamp_t endTimestamp;
    long long count; // AKA limit
    AggregationArgs aggregationArgs;
    FilterByValueArgs filterByValueArgs;
    FilterByTSArgs filterByTSArgs;
    RangeAlignment alignment;
    timestamp_t timestampAlignment;
} RangeArgs;

typedef enum MultiSeriesReduceOp
{
    MultiSeriesReduceOp_Min,
    MultiSeriesReduceOp_Max,
    MultiSeriesReduceOp_Sum,
} MultiSeriesReduceOp;

#define LIMIT_LABELS_SIZE 50
typedef struct MRangeArgs
{
    RangeArgs rangeArgs;
    bool withLabels;
    unsigned short numLimitLabels;
    RedisModuleString *limitLabels[LIMIT_LABELS_SIZE];
    QueryPredicateList *queryPredicates;
    const char *groupByLabel;
    MultiSeriesReduceOp gropuByReducerOp;
    bool reverse;
} MRangeArgs;

typedef struct MGetArgs
{
    bool withLabels;
    unsigned short numLimitLabels;
    RedisModuleString *limitLabels[LIMIT_LABELS_SIZE];
    QueryPredicateList *queryPredicates;
} MGetArgs;

typedef struct CreateCtx
{
    long long retentionTime;
    long long chunkSizeBytes;
    size_t labelsCount;
    Label *labels;
    int options;
    DuplicatePolicy duplicatePolicy;
    bool isTemporary;
    bool skipChunkCreation;
} CreateCtx;

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

int parseLabelQuery(RedisModuleCtx *ctx,
                    RedisModuleString **argv,
                    int argc,
                    bool *withLabels,
                    RedisModuleString **limitLabels,
                    unsigned short *limitLabelsSize);

int parseAggregationArgs(RedisModuleCtx *ctx,
                         RedisModuleString **argv,
                         int argc,
                         AggregationArgs *out);

int parseRangeArguments(RedisModuleCtx *ctx,
                        int start_index,
                        RedisModuleString **argv,
                        int argc,
                        timestamp_t maxTimestamp,
                        RangeArgs *out);

QueryPredicateList *parseLabelListFromArgs(RedisModuleCtx *ctx,
                                           RedisModuleString **argv,
                                           int start,
                                           int query_count,
                                           int *response);

int parseMRangeCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, MRangeArgs *out);
void MRangeArgs_Free(MRangeArgs *args);

int parseMGetCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, MGetArgs *out);
void MGetArgs_Free(MGetArgs *args);

#endif // REDISTIMESERIES_QUERY_LANGUAGE_H
