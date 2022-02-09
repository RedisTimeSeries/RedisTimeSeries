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
    timestamp_t timeBucket;
    AggregationClass *aggClass;
    TS_AGG_TYPES_T aggType;
    void *aggContext;
    struct CompactionRule *nextRule;
    timestamp_t startCurrentTimeBucket;
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
                              uint64_t timeBucket);
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

// Calculate the begining of  aggregation window
timestamp_t CalcWindowStart(timestamp_t timestamp, size_t window);

// return first timestamp in retention window, and set `skipped` to number of samples outside of
// retention
timestamp_t getFirstValidTimestamp(Series *series, long long *skipped);

CompactionRule *NewRule(RedisModuleString *destKey, int aggType, uint64_t timeBucket);

// set/delete/replace a chunk in a dictionary
typedef enum
{
    DICT_OP_SET = 0,
    DICT_OP_REPLACE = 1,
    DICT_OP_DEL = 2
} DictOp;
int dictOperator(RedisModuleDict *d, void *chunk, timestamp_t ts, DictOp op);

void seriesEncodeTimestamp(void *buf, timestamp_t timestamp);

#endif /* TSDB_H */
