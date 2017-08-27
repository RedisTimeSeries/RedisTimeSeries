#ifndef TSDB_H
#define TSDB_H

#include "redismodule.h"
#include "compaction.h"
#include "chunk.h"
#include "consts.h"

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
    timestamp_t lastTimestamp;
    int32_t retentionSecs;
    short maxSamplesPerChunk;
    CompactionRule *rules;
} Series;

typedef struct SeriesItertor {
    Series *series;
    Chunk *currentChunk;
    int chunkIteratorInitilized;
    ChunkIterator chunkIterator;
    unsigned int currentSampleIndex;
    api_timestamp_t maxTimestamp;
    api_timestamp_t minTimestamp;
} SeriesItertor;

Series * NewSeries(int32_t retentionSecs, short maxSamplesPerChunk);
void FreeSeries(void *value);
size_t SeriesMemUsage(const void *value);
int SeriesAddSample(Series *series, api_timestamp_t timestamp, double value);

SeriesItertor SeriesQuery(Series *series, api_timestamp_t minTimestamp, api_timestamp_t maxTimestamp);
Sample * SeriesItertorGetNext(SeriesItertor *iterator);


CompactionRule *NewRule(RedisModuleString *destKey, int aggType, int bucketSizeSec);
#endif /* TSDB_H */