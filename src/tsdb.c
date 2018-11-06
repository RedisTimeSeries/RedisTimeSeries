#include <time.h>
#include <string.h>
#include "rmutil/logging.h"
#include "rmutil/strings.h"
#include "rmutil/alloc.h"
#include "tsdb.h"
#include "module.h"
#include "config.h"

Series * NewSeries(int32_t retentionSecs, short maxSamplesPerChunk)
{
    Series *newSeries = (Series *)malloc(sizeof(Series));
    newSeries->chunks = RedisModule_CreateDict(NULL);
    newSeries->maxSamplesPerChunk = maxSamplesPerChunk;
    newSeries->retentionSecs = retentionSecs;
    newSeries->rules = NULL;
    newSeries->lastTimestamp = 0;
    newSeries->lastValue = 0;
    Chunk* newChunk = NewChunk(newSeries->maxSamplesPerChunk);
    RedisModule_DictSetC(newSeries->chunks, (void*)&newSeries->lastTimestamp, sizeof(newSeries->lastTimestamp),
                        (void*)newChunk);
    newSeries->lastChunk = newChunk;
    return newSeries;
}

void SeriesTrim(Series * series) {
    if (series->retentionSecs == 0) {
        return;
    }

    // start iterator from smallest key
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(series->chunks, "^", NULL, 0);
    Chunk *currentChunk;
    void *currentKey;
    timestamp_t minTimestamp = time(NULL) - series->retentionSecs;
    while ((currentKey=RedisModule_DictNextC(iter, NULL, (void*)&currentChunk)))
    {
        if (ChunkGetLastTimestamp(currentChunk) < minTimestamp)
        {
            RedisModule_DictDelC(series->chunks, currentKey, sizeof(timestamp_t), NULL);
            // reseek iterator since we modified the dict, go to first element that is bigger than current key
            RedisModule_DictIteratorReseekC(iter, ">", currentKey, sizeof(timestamp_t));
            FreeChunk(currentChunk);
        } else {
            break;
        }
    }
    RedisModule_DictIteratorStop(iter);
}

void FreeSeries(void *value) {
    Series *currentSeries = (Series *) value;
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(currentSeries->chunks, "^", NULL, 0);
    Chunk *currentChunk;
    void *currentKey;
    while ((currentKey=RedisModule_DictNextC(iter, NULL, (void*)&currentChunk)))
    {
        FreeChunk(currentChunk);
        RedisModule_DictIteratorReseekC(iter, ">", currentKey, sizeof(currentKey));
    }
    RedisModule_DictIteratorStop(iter);
    RedisModule_FreeDict(NULL, currentSeries->chunks);
}

size_t SeriesMemUsage(const void *value) {
    Series *series = (Series *)value;
    return sizeof(series) + sizeof(Chunk) * RedisModule_DictSize(series->chunks);
}

size_t SeriesGetNumSamples(Series *series)
{
    size_t numSamples = 0;
    Chunk *currentChunk;
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(series->chunks, "^", NULL, 0);
    while (RedisModule_DictNextC(iter, NULL, (void*)&currentChunk))
    {
        numSamples += ChunkNumOfSample(currentChunk);
    }
    RedisModule_DictIteratorStop(iter);
    return numSamples;
}

int SeriesAddSample(Series *series, api_timestamp_t timestamp, double value) {
    if (timestamp < series->lastTimestamp) {
        return TSDB_ERR_TIMESTAMP_TOO_OLD;
    } else if (timestamp == series->lastTimestamp && series->lastChunk->num_samples > 0) {
        // this is a hack, we want to override the last sample, so lets ignore it first
        series->lastChunk->num_samples--;
    }
    Sample sample = {.timestamp = timestamp, .data = value};
    int ret = ChunkAddSample(series->lastChunk, sample);
    if (ret == 0 ) {
        // When a new chunk is created trim the series
        SeriesTrim(series);

        Chunk *newChunk = NewChunk(series->maxSamplesPerChunk);
        RedisModule_DictSetC(series->chunks, (void*)&timestamp, sizeof(timestamp), (void*)newChunk);
        // re-add the sample
        ChunkAddSample(newChunk, sample);
        series->lastChunk = newChunk;
    }
    series->lastTimestamp = timestamp;
    series->lastValue = value;
    return TSDB_OK;
}

SeriesIterator SeriesQuery(Series *series, api_timestamp_t minTimestamp, api_timestamp_t maxTimestamp) {
    SeriesIterator iter;
    iter.series = series;
    // get the rightmost chunk whose base timestamp is smaller or equal to minTimestamp
    iter.dictIter = RedisModule_DictIteratorStartC(series->chunks, "<=", (void*)&minTimestamp, sizeof(minTimestamp));

    // if no such chunk exists, we will start the search from the first chunk
    if (!RedisModule_DictNextC(iter.dictIter, NULL, (void*)&iter.currentChunk))
    {
        RedisModule_DictIteratorReseekC(iter.dictIter, "^", NULL, 0);
        RedisModule_DictNextC(iter.dictIter, NULL, (void*)&iter.currentChunk);
    }
    iter.chunkIteratorInitialized = FALSE;
    iter.minTimestamp = minTimestamp;
    iter.maxTimestamp = maxTimestamp;
    return iter;
}

int SeriesIteratorGetNext(SeriesIterator *iterator, Sample *currentSample) {
    Sample internalSample;
    while (iterator->currentChunk != NULL)
    {
        Chunk *currentChunk = iterator->currentChunk;
        if (ChunkGetLastTimestamp(currentChunk) < iterator->minTimestamp)
        {
            if (!RedisModule_DictNextC(iterator->dictIter, NULL, (void*)&iterator->currentChunk)) {
                iterator->currentChunk = NULL;
            }
            iterator->chunkIteratorInitialized = FALSE;
            continue;
        }
        else if (ChunkGetFirstTimestamp(currentChunk) > iterator->maxTimestamp)
        {
            break;
        }

        if (!iterator->chunkIteratorInitialized) 
        {
            iterator->chunkIterator = NewChunkIterator(iterator->currentChunk);
            iterator->chunkIteratorInitialized = TRUE;
        }

        if (ChunkIteratorGetNext(&iterator->chunkIterator, &internalSample) == 0) { // reached the end of the chunk
            if (!RedisModule_DictNextC(iterator->dictIter, NULL, (void*)&iterator->currentChunk)) {
                iterator->currentChunk = NULL;
            }
            iterator->chunkIteratorInitialized = FALSE;
            continue;
        }

        if (internalSample.timestamp < iterator->minTimestamp) {
            continue;
        } else if (internalSample.timestamp > iterator->maxTimestamp) {
            break;
        } else {
            memcpy(currentSample, &internalSample, sizeof(Sample));
            return 1;
        }
    }
    return 0;
}

CompactionRule * SeriesAddRule(Series *series, RedisModuleString *destKeyStr, int aggType, long long bucketSize) {
    CompactionRule *rule = NewRule(destKeyStr, aggType, bucketSize);
    if (rule == NULL ) {
        return NULL;
    }
    if (series->rules == NULL){
        series->rules = rule;
    } else {
        CompactionRule *last = series->rules;
        while(last->nextRule != NULL) last = last->nextRule;
        last->nextRule = rule;
    }
    return rule;
}

int SeriesCreateRulesFromGlobalConfig(RedisModuleCtx *ctx, RedisModuleString *keyName, Series *series) {
    size_t len;
    int i;
    Series *compactedSeries;
    RedisModuleKey *compactedKey;

    for (i=0; i<TSGlobalConfig.compactionRulesCount; i++) {
        SimpleCompactionRule* rule = TSGlobalConfig.compactionRules + i;
        RedisModuleString* destKey = RedisModule_CreateStringPrintf(ctx, "%s_%s_%ld",
                                            RedisModule_StringPtrLen(keyName, &len),
                                            AggTypeEnumToString(rule->aggType),
                                            rule->bucketSizeSec);
        RedisModule_RetainString(ctx, destKey);
        SeriesAddRule(series, destKey, rule->aggType, rule->bucketSizeSec);

        compactedKey = RedisModule_OpenKey(ctx, destKey, REDISMODULE_READ|REDISMODULE_WRITE);

        if (RedisModule_KeyType(compactedKey) != REDISMODULE_KEYTYPE_EMPTY) {
            RM_LOG_WARNING(ctx, "Cannot create compacted key, key '%s' already exists", destKey);
            RedisModule_CloseKey(compactedKey);
            continue;
        }

        CreateTsKey(ctx, destKey, rule->retentionSizeSec, TSGlobalConfig.maxSamplesPerChunk, &compactedSeries, &compactedKey);
        RedisModule_CloseKey(compactedKey);
    }
    return TSDB_OK;
}

CompactionRule *NewRule(RedisModuleString *destKey, int aggType, int bucketSizeSec) {
    if (bucketSizeSec <= 0) {
        return NULL;
    }

    CompactionRule *rule = (CompactionRule *)malloc(sizeof(CompactionRule));
    rule->aggClass = GetAggClass(aggType);;
    rule->aggType = aggType;
    rule->aggContext = rule->aggClass->createContext();
    rule->bucketSizeSec = bucketSizeSec;
    rule->destKey = destKey;

    rule->nextRule = NULL;

    return rule;
}

int SeriesHasRule(Series *series, RedisModuleString *destKey) {
    CompactionRule *rule = series->rules;
    while (rule != NULL) {
        if (RMUtil_StringEquals(rule->destKey, destKey)) {
            return TRUE;
        }
        rule = rule->nextRule;
    }
    return FALSE;
}
