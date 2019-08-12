/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/
#include <time.h>
#include <string.h>
#include <redismodule.h>
#include "rmutil/logging.h"
#include "rmutil/strings.h"
#include "rmutil/alloc.h"
#include "tsdb.h"
#include "module.h"
#include "config.h"
#include "indexer.h"
#include "endianconv.h"

static Series* lastDeletedSeries = NULL;

Series *NewSeries(RedisModuleString *keyName, RSLabel *labels, size_t labelsCount, int32_t retentionTime,
        short maxSamplesPerChunk)
{
    Series *newSeries = (Series *)malloc(sizeof(Series));
    newSeries->keyName = keyName;
    newSeries->chunks = RedisModule_CreateDict(NULL);
    newSeries->maxSamplesPerChunk = maxSamplesPerChunk;
    newSeries->retentionTime = retentionTime;
    newSeries->srcKey = NULL;
    newSeries->rules = NULL;
    newSeries->lastTimestamp = 0;
    newSeries->lastValue = 0;
    newSeries->labels = RSLabelToLabels(NULL, labels, labelsCount);
    newSeries->labelsCount = labelsCount;
    Chunk* newChunk = NewChunk(newSeries->maxSamplesPerChunk);
    RedisModule_DictSetC(newSeries->chunks, (void*)&newSeries->lastTimestamp, sizeof(newSeries->lastTimestamp),
                        (void*)newChunk);
    newSeries->lastChunk = newChunk;
    return newSeries;
}

void SeriesTrim(Series * series) {
    if (series->retentionTime == 0) {
        return;
    }

    // start iterator from smallest key
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(series->chunks, "^", NULL, 0);
    Chunk *currentChunk;
    void *currentKey;
    size_t keyLen;
    timestamp_t minTimestamp = series->lastTimestamp > series->retentionTime ?
    		series->lastTimestamp - series->retentionTime : 0;

    while ((currentKey=RedisModule_DictNextC(iter, &keyLen, (void*)&currentChunk)))
    {
        if (ChunkGetLastTimestamp(currentChunk) < minTimestamp)
        {
            RedisModule_DictDelC(series->chunks, currentKey, keyLen, NULL);
            // reseek iterator since we modified the dict, go to first element that is bigger than current key
            RedisModule_DictIteratorReseekC(iter, ">", currentKey, keyLen);
            FreeChunk(currentChunk);
        } else {
            break;
        }
    }
    RedisModule_DictIteratorStop(iter);
}

// Encode timestamps as bigendian to allow correct lexical sorting
static void seriesEncodeTimestamp(void *buf, timestamp_t timestamp) {
    uint64_t e;
    e = htonu64(timestamp);
    memcpy(buf, &e, sizeof(e));
}

void freeLastDeletedSeries() {
    if(lastDeletedSeries == NULL){
        return;
    }
    CompactionRule *rule = lastDeletedSeries->rules;
    while (rule != NULL) {
        CompactionRule *nextRule = rule->nextRule;
        FreeCompactionRule(rule);
        rule = nextRule;
    }
    if(lastDeletedSeries->srcKey != NULL) {
        RedisModule_FreeString(NULL, lastDeletedSeries->srcKey);
    }
    RedisModule_FreeString(NULL, lastDeletedSeries->keyName);
    free(lastDeletedSeries);
    lastDeletedSeries = NULL;
}

void CleanLastDeletedSeries(RedisModuleCtx *ctx, RedisModuleString *key){
    if(lastDeletedSeries != NULL && RedisModule_StringCompare(lastDeletedSeries->keyName, key) == 0) {
        CompactionRule *rule = lastDeletedSeries->rules;
        while (rule != NULL) {
            Series *dstSeries;
            int status = GetSeries(ctx, rule->destKey, &dstSeries);
            if (status) {
                SeriesDeleteSrcRule(dstSeries, lastDeletedSeries->keyName);
            }
            rule = rule->nextRule;
        }
        if (lastDeletedSeries->srcKey) {
            Series *srcSeries;
            int status = GetSeries(ctx, lastDeletedSeries->srcKey, &srcSeries);
            if (status) {
                SeriesDeleteRule(srcSeries, lastDeletedSeries->keyName);
            }
        }
    }
    freeLastDeletedSeries();
}

// Releases Series and all its compaction rules
void FreeSeries(void *value) {
    Series *currentSeries = (Series *) value;
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(currentSeries->chunks, "^", NULL, 0);
    Chunk *currentChunk;
    while (RedisModule_DictNextC(iter, NULL, (void*)&currentChunk) != NULL){
        FreeChunk(currentChunk);
    }
    RedisModule_DictIteratorStop(iter);

    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
    RedisModule_AutoMemory(ctx);
    RemoveIndexedMetric(ctx, currentSeries->keyName, currentSeries->labels, currentSeries->labelsCount);

    FreeLabels(currentSeries->labels, currentSeries->labelsCount);

    RedisModule_FreeThreadSafeContext(ctx);
    RedisModule_FreeDict(NULL, currentSeries->chunks);

    freeLastDeletedSeries();
    lastDeletedSeries = currentSeries;
}

void FreeCompactionRule(void *value) {
	CompactionRule *rule = (CompactionRule *) value;
	RedisModule_FreeString(NULL, rule->destKey);
	free(rule->aggContext);
	free(rule);
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
    timestamp_t rax_key;
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
        seriesEncodeTimestamp(&rax_key, timestamp);
        RedisModule_DictSetC(series->chunks, &rax_key, sizeof(rax_key), (void*)newChunk);
        ChunkAddSample(newChunk, sample);
        series->lastChunk = newChunk;
    }
    series->lastTimestamp = timestamp;
    series->lastValue = value;
    return TSDB_OK;
}

SeriesIterator SeriesQuery(Series *series, api_timestamp_t minTimestamp, api_timestamp_t maxTimestamp) {
    SeriesIterator iter;
    timestamp_t rax_key;
    iter.series = series;
    // get the rightmost chunk whose base timestamp is smaller or equal to minTimestamp
    seriesEncodeTimestamp(&rax_key, minTimestamp);
    iter.dictIter = RedisModule_DictIteratorStartC(series->chunks, "<=", &rax_key, sizeof(rax_key));

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

void SeriesIteratorClose(SeriesIterator *iterator) {
    RedisModule_DictIteratorStop(iterator->dictIter);
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

CompactionRule * SeriesAddRule(Series *series, RedisModuleString *destKeyStr, int aggType, long long timeBucket) {
    CompactionRule *rule = NewRule(destKeyStr, aggType, timeBucket);
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

int SeriesCreateRulesFromGlobalConfig(RedisModuleCtx *ctx, RedisModuleString *keyName, Series *series,
        RSLabel *labels, size_t labelsCount) {
    size_t len;
    int i;
    Series *compactedSeries;
    RedisModuleKey *compactedKey;
    size_t compactedRuleLabelCount = labelsCount + 2;

    for (i=0; i<TSGlobalConfig.compactionRulesCount; i++) {
        SimpleCompactionRule* rule = TSGlobalConfig.compactionRules + i;
        const char *aggString = AggTypeEnumToString(rule->aggType);
        RedisModuleString* destKey = RedisModule_CreateStringPrintf(ctx, "%s_%s_%ld",
                                            RedisModule_StringPtrLen(keyName, &len),
                                            aggString,
                                            rule->timeBucket);
        RedisModule_RetainString(ctx, destKey);
        SeriesAddRule(series, destKey, rule->aggType, rule->timeBucket);

        compactedKey = RedisModule_OpenKey(ctx, destKey, REDISMODULE_READ|REDISMODULE_WRITE);

        if (RedisModule_KeyType(compactedKey) != REDISMODULE_KEYTYPE_EMPTY) {
            RM_LOG_WARNING(ctx, "Cannot create compacted key, key '%s' already exists", destKey);
            RedisModule_CloseKey(compactedKey);
            continue;
        }
        Label *compactedLabels = calloc(compactedRuleLabelCount, sizeof(RSLabel));
        RSLabelToLabels(compactedLabels, labels, labelsCount);

        // For every aggregated key create 2 labels: `aggregation` and `time_bucket`.
        compactedLabels[labelsCount].key = RedisModule_CreateStringPrintf(NULL, "aggregation");
        compactedLabels[labelsCount].value = RedisModule_CreateString(NULL, aggString, strlen(aggString));
        compactedLabels[labelsCount+1].key = RedisModule_CreateStringPrintf(NULL, "time_bucket");
        compactedLabels[labelsCount+1].value = RedisModule_CreateStringPrintf(NULL, "%ld", rule->timeBucket);

        CreateTsKey(ctx, destKey, labels, compactedRuleLabelCount, rule->retentionSizeSec, TSGlobalConfig.maxSamplesPerChunk, &compactedSeries, &compactedKey);
        RedisModule_CloseKey(compactedKey);
    }
    return TSDB_OK;
}

CompactionRule *NewRule(RedisModuleString *destKey, int aggType, int timeBucket) {
    if (timeBucket <= 0) {
        return NULL;
    }

    CompactionRule *rule = (CompactionRule *)malloc(sizeof(CompactionRule));
    rule->aggClass = GetAggClass(aggType);;
    rule->aggType = aggType;
    rule->aggContext = rule->aggClass->createContext();
    rule->timeBucket = timeBucket;
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

int SeriesDeleteRule(Series *series, RedisModuleString *destKey) {
	CompactionRule *rule = series->rules;
	CompactionRule *prev_rule = NULL;
	while (rule != NULL) {
		if (RMUtil_StringEquals(rule->destKey, destKey)) {
            CompactionRule *next = rule->nextRule;
            FreeCompactionRule(rule);
			if (prev_rule != NULL) {
			     // cut off the current rule from the linked list
			     prev_rule->nextRule = next;
			}  else {
			    // make the next one to be the first rule
			    series->rules = next;
			}
			return TRUE;
		}
		prev_rule = rule;
		rule = rule->nextRule;
	}
	return FALSE;
}

int SeriesSetSrcRule(Series *series, RedisModuleString *srctKey) {
	if(series->srcKey){
		return FALSE;
	}
	series->srcKey = srctKey;
	return TRUE;
}

int SeriesDeleteSrcRule(Series *series, RedisModuleString *srctKey) {
	if(RMUtil_StringEquals(series->srcKey, srctKey)){
		RedisModule_FreeString(NULL, series->srcKey);
		series->srcKey = NULL;
		return TRUE;
	}
	return FALSE;
}
