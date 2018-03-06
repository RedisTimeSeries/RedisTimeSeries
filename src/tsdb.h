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
    Chunk *firstChunk;
    Chunk *lastChunk;
    size_t chunkCount;
    int32_t retentionSecs;
    short maxSamplesPerChunk;
    CompactionRule *rules;
    timestamp_t lastTimestamp;
    double lastValue;
} Series;

typedef struct SeriesItertor {
    Series *series;
    Chunk *currentChunk;
    int chunkIteratorInitilized;
    ChunkIterator chunkIterator;
    api_timestamp_t maxTimestamp;
    api_timestamp_t minTimestamp;
} SeriesItertor;

Series * NewSeries(int32_t retentionSecs, short maxSamplesPerChunk);
void FreeSeries(void *value);
size_t SeriesMemUsage(const void *value);
int SeriesAddSample(Series *series, api_timestamp_t timestamp, double value);
int SeriesHasRule(Series *series, RedisModuleString *destKey);
CompactionRule *SeriesAddRule(Series *series, RedisModuleString *destKeyStr, int aggType, long long bucketSize);
int SeriesCreateRulesFromGloalConfig(RedisModuleCtx *ctx, RedisModuleString *keyName, Series *series);

// Iterator over the series
SeriesItertor SeriesQuery(Series *series, api_timestamp_t minTimestamp, api_timestamp_t maxTimestamp);
int SeriesItertorGetNext(SeriesItertor *iterator, Sample *currentSample);


CompactionRule *NewRule(RedisModuleString *destKey, int aggType, int bucketSizeSec);
#endif /* TSDB_H */