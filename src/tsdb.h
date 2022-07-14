/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#ifndef TSDB_H
#define TSDB_H

#include "abstract_iterator.h"
#include "compaction.h"
#include "consts.h"
#include "generic_chunk.h"
#include "indexer.h"
#include "query_language.h"
#include "redismodule.h"

typedef struct CompactionRule
{
    RedisModuleString *destKey;
    timestamp_t bucketDuration;
    timestamp_t timestampAlignment;
    AggregationClass *aggClass;
    TS_AGG_TYPES_T aggType;
    void *aggContext;
    struct CompactionRule *nextRule;
    timestamp_t startCurrentTimeBucket; // Beware that the first bucket is alway starting in 0 no
                                        // matter the alignment
} CompactionRule;

typedef struct Series
{
    RedisModuleDict *chunks;
    Chunk_t *lastChunk;
    uint64_t retentionTime;
    long long chunkSizeBytes;
    short options;
    CompactionRule *rules;
    timestamp_t lastTimestamp;
    double lastValue;
    Label *labels;
    RedisModuleString *keyName;
    size_t labelsCount;
    RedisModuleString *srcKey;
    const ChunkFuncs *funcs;
    size_t totalSamples;
    DuplicatePolicy duplicatePolicy;
} Series;

// process C's modulo result to translate from a negative modulo to a positive
#define modulo(x, N) ((x % N + N) % N)

// Calculate the begining of aggregation bucket
static inline timestamp_t CalcBucketStart(timestamp_t ts,
                                          timestamp_t bucketDuration,
                                          timestamp_t timestampAlignment) {
    const int64_t timestamp_diff = ts - timestampAlignment;
    return ts - modulo(timestamp_diff, (int64_t)bucketDuration);
}

// If bucketTS is negative converts it to 0
static inline timestamp_t BucketStartNormalize(timestamp_t bucketTS) {
    return max(0, (int64_t)bucketTS);
}

Series *NewSeries(RedisModuleString *keyName, CreateCtx *cCtx);
void FreeSeries(void *value);
void *CopySeries(RedisModuleString *fromkey, RedisModuleString *tokey, const void *value);
void RenameSeriesFrom(RedisModuleCtx *ctx, RedisModuleString *key);
void IndexMetricFromName(RedisModuleCtx *ctx, RedisModuleString *keyname);
void RenameSeriesTo(RedisModuleCtx *ctx, RedisModuleString *key);
void RestoreKey(RedisModuleCtx *ctx, RedisModuleString *keyname);

CompactionRule *GetRule(CompactionRule *rules, RedisModuleString *keyName);
void deleteReferenceToDeletedSeries(RedisModuleCtx *ctx, Series *series);

// Deletes the reference if the series deleted, watch out of rules iterator invalidation
int GetSeries(RedisModuleCtx *ctx,
              RedisModuleString *keyName,
              RedisModuleKey **key,
              Series **series,
              int mode,
              bool shouldDeleteRefs,
              bool isSilent);

AbstractIterator *SeriesQuery(Series *series,
                              const RangeArgs *args,
                              bool reserve,
                              bool check_retention);
AbstractSampleIterator *SeriesCreateSampleIterator(Series *series,
                                                   const RangeArgs *args,
                                                   bool reverse,
                                                   bool check_retention);

AbstractMultiSeriesSampleIterator *MultiSeriesCreateSampleIterator(Series **series,
                                                                   size_t n_series,
                                                                   const RangeArgs *args,
                                                                   bool reverse,
                                                                   bool check_retention);

AbstractSampleIterator *MultiSeriesCreateAggDupSampleIterator(Series **series,
                                                              size_t n_series,
                                                              const RangeArgs *args,
                                                              bool reverse,
                                                              bool check_retention,
                                                              const ReducerArgs *reducerArgs);

void FreeCompactionRule(void *value);
size_t SeriesMemUsage(const void *value);

int SeriesAddSample(Series *series, api_timestamp_t timestamp, double value);
int SeriesUpsertSample(Series *series,
                       api_timestamp_t timestamp,
                       double value,
                       DuplicatePolicy dp_override);

int SeriesDeleteRule(Series *series, RedisModuleString *destKey);
void SeriesSetSrcRule(RedisModuleCtx *ctx, Series *series, RedisModuleString *srcKeyName);
int SeriesDeleteSrcRule(Series *series, RedisModuleString *srctKey);

CompactionRule *SeriesAddRule(RedisModuleCtx *ctx,
                              Series *series,
                              Series *destSeries,
                              int aggType,
                              uint64_t bucketDuration,
                              timestamp_t timestampAlignment);
int SeriesCreateRulesFromGlobalConfig(RedisModuleCtx *ctx,
                                      RedisModuleString *keyName,
                                      Series *series,
                                      Label *labels,
                                      size_t labelsCount);
size_t SeriesGetNumSamples(const Series *series);

char *SeriesGetCStringLabelValue(const Series *series, const char *labelKey);
size_t SeriesDelRange(Series *series, timestamp_t start_ts, timestamp_t end_ts);
const char *SeriesChunkTypeToString(const Series *series);

int SeriesCalcRange(Series *series,
                    timestamp_t start_ts,
                    timestamp_t end_ts,
                    CompactionRule *rule,
                    double *val,
                    bool *is_empty);

// return first timestamp in retention window, and set `skipped` to number of samples outside of
// retention
timestamp_t getFirstValidTimestamp(Series *series, long long *skipped);

CompactionRule *NewRule(RedisModuleString *destKey,
                        int aggType,
                        uint64_t bucketDuration,
                        timestamp_t timestampAlignment);

// set/delete/replace a chunk in a dictionary
typedef enum
{
    DICT_OP_SET = 0,
    DICT_OP_REPLACE = 1,
    DICT_OP_DEL = 2
} DictOp;
int dictOperator(RedisModuleDict *d, void *chunk, timestamp_t ts, DictOp op);

void seriesEncodeTimestamp(void *buf, timestamp_t timestamp);

CompactionRule *find_rule(CompactionRule *rules, RedisModuleString *keyName);

#define should_finalize_last_bucket_get(latest, series) ((latest) && (series)->srcKey)

void calculate_latest_sample(Sample **sample, const Series *series);

#endif /* TSDB_H */
