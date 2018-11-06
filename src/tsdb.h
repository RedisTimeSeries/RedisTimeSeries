#ifndef TSDB_H
#define TSDB_H

#include "redismodule.h"
#include "compaction.h"
#include "consts.h"
#include "chunk.h"

typedef struct CompactionRule {
    RedisModuleString *destKey;
    int32_t bucketSizeSec;
    AggregationClass *aggClass;
    int aggType;
    void *aggContext;
    struct CompactionRule *nextRule;
} CompactionRule;

typedef struct Series {
    RedisModuleDict* chunks;
    int32_t retentionSecs;
    short maxSamplesPerChunk;
    CompactionRule *rules;
    timestamp_t lastTimestamp;
    double lastValue;
    RedisModuleCtx *ctx;
} Series;

typedef struct SeriesIterator {
    Series *series;
    RedisModuleDictIter *dictIter;
    Chunk *currentChunk;
    int chunkIteratorInitialized;
    ChunkIterator chunkIterator;
    api_timestamp_t maxTimestamp;
    api_timestamp_t minTimestamp;
} SeriesIterator;

Series * NewSeries(int32_t retentionSecs, short maxSamplesPerChunk);
void FreeSeries(void *value);
size_t SeriesMemUsage(const void *value);
int SeriesAddSample(Series *series, api_timestamp_t timestamp, double value);
int SeriesHasRule(Series *series, RedisModuleString *destKey);
CompactionRule *SeriesAddRule(Series *series, RedisModuleString *destKeyStr, int aggType, long long bucketSize);
int SeriesCreateRulesFromGlobalConfig(RedisModuleCtx *ctx, RedisModuleString *keyName, Series *series);
size_t SeriesGetNumSamples(Series *series);
Chunk *SeriesGetLastChunk(Series *series);

// Iterator over the series
SeriesIterator SeriesQuery(Series *series, api_timestamp_t minTimestamp, api_timestamp_t maxTimestamp);
int SeriesIteratorGetNext(SeriesIterator *iterator, Sample *currentSample);


CompactionRule *NewRule(RedisModuleString *destKey, int aggType, int bucketSizeSec);
#endif /* TSDB_H */