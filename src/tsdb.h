#ifndef TSDB_H
#define TSDB_H

#include <sys/types.h>
#include "redismodule.h"
#include "compaction.h"

#define timestamp_t int32_t
#define api_timestamp_t int32_t
#define TSDB_ERR_TIMESTAMP_TOO_OLD -1
#define TSDB_OK 0

typedef struct Sample {
    timestamp_t timestamp;
    double data;
} Sample;

typedef struct Chunk
{
    timestamp_t base_timestamp;
    Sample * samples;
    short num_samples;
    struct Chunk *nextChunk;
    // struct Chunk *prevChunk;
} Chunk;

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
    unsigned int currentSampleIndex;
    api_timestamp_t maxTimestamp;
    api_timestamp_t minTimestamp;
} SeriesItertor;

Chunk * NewChunk();
Series * NewSeries(int32_t retentionSecs, short maxSamplesPerChunk);
void SeriesFree(void *value);
size_t SeriesMemUsage(void *value);
int SeriesAddSample(Series *series, api_timestamp_t timestamp, double value);

SeriesItertor SeriesQuery(Series *series, api_timestamp_t minTimestamp, api_timestamp_t maxTimestamp);
Sample * SeriesItertorGetNext(SeriesItertor *iterator);


CompactionRule *NewRule(RedisModuleString *destKey, int aggType, int bucketSizeSec);
#endif /* TSDB_H */