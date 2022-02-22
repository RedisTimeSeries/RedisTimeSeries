/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "tsdb.h"

#include "config.h"
#include "consts.h"
#include "endianconv.h"
#include "filter_iterator.h"
#include "indexer.h"
#include "module.h"
#include "series_iterator.h"
#include "sample_iterator.h"

#include <inttypes.h>
#include <math.h>
#include <stdlib.h>
#include <assert.h> // assert
#include "rmutil/alloc.h"
#include "rmutil/logging.h"
#include "rmutil/strings.h"

static RedisModuleString *renameFromKey = NULL;

void deleteReferenceToDeletedSeries(RedisModuleCtx *ctx, Series *series) {
    Series *_series;
    RedisModuleKey *_key;
    int status;

    if (series->srcKey) {
        status = GetSeries(ctx, series->srcKey, &_key, &_series, REDISMODULE_READ, false, true);
        if (!status || (!GetRule(_series->rules, series->keyName))) {
            SeriesDeleteSrcRule(series, series->srcKey);
        }
        if (status) {
            RedisModule_CloseKey(_key);
        }
    }

    CompactionRule *rule = series->rules;
    while (rule) {
        CompactionRule *nextRule = rule->nextRule;
        status = GetSeries(ctx, rule->destKey, &_key, &_series, REDISMODULE_READ, false, true);
        if (!status || !_series->srcKey ||
            (RedisModule_StringCompare(_series->srcKey, series->keyName) != 0)) {
            SeriesDeleteRule(series, rule->destKey);
        }
        if (status) {
            RedisModule_CloseKey(_key);
        }
        rule = nextRule;
    }
}

CompactionRule *GetRule(CompactionRule *rules, RedisModuleString *keyName) {
    CompactionRule *rule = rules;
    while (rule != NULL) {
        if (RedisModule_StringCompare(rule->destKey, keyName) == 0) {
            return rule;
        }
        rule = rule->nextRule;
    }
    return NULL;
}

int GetSeries(RedisModuleCtx *ctx,
              RedisModuleString *keyName,
              RedisModuleKey **key,
              Series **series,
              int mode,
              bool shouldDeleteRefs,
              bool isSilent) {
    if (shouldDeleteRefs) {
        mode = mode | REDISMODULE_WRITE;
    }
    RedisModuleKey *new_key = RedisModule_OpenKey(ctx, keyName, mode);
    if (RedisModule_KeyType(new_key) == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(new_key);
        if (!isSilent) {
            RTS_ReplyGeneralError(ctx, "TSDB: the key does not exist");
        }
        return FALSE;
    }
    if (RedisModule_ModuleTypeGetType(new_key) != SeriesType) {
        RedisModule_CloseKey(new_key);
        if (!isSilent) {
            RTS_ReplyGeneralError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        }
        return FALSE;
    }

    *series = RedisModule_ModuleTypeGetValue(new_key);
    *key = new_key;

    if (shouldDeleteRefs) {
        deleteReferenceToDeletedSeries(ctx, *series);
    }

    return TRUE;
}

int dictOperator(RedisModuleDict *d, void *chunk, timestamp_t ts, DictOp op) {
    timestamp_t rax_key = htonu64(ts);
    switch (op) {
        case DICT_OP_SET:
            return RedisModule_DictSetC(d, &rax_key, sizeof(rax_key), chunk);
        case DICT_OP_REPLACE:
            return RedisModule_DictReplaceC(d, &rax_key, sizeof(rax_key), chunk);
        case DICT_OP_DEL:
            return RedisModule_DictDelC(d, &rax_key, sizeof(rax_key), NULL);
    }
    chunk = NULL;
    return REDISMODULE_OK; // silence compiler
}

Series *NewSeries(RedisModuleString *keyName, CreateCtx *cCtx) {
    Series *newSeries = (Series *)calloc(1, sizeof(Series));
    newSeries->keyName = keyName;
    newSeries->chunks = RedisModule_CreateDict(NULL);
    newSeries->chunkSizeBytes = cCtx->chunkSizeBytes;
    newSeries->retentionTime = cCtx->retentionTime;
    newSeries->srcKey = NULL;
    newSeries->rules = NULL;
    newSeries->lastTimestamp = 0;
    newSeries->lastValue = 0;
    newSeries->totalSamples = 0;
    newSeries->labels = cCtx->labels;
    newSeries->labelsCount = cCtx->labelsCount;
    newSeries->options = cCtx->options;
    newSeries->duplicatePolicy = cCtx->duplicatePolicy;

    if (newSeries->options & SERIES_OPT_UNCOMPRESSED) {
        newSeries->options |= SERIES_OPT_UNCOMPRESSED;
        newSeries->funcs = GetChunkClass(CHUNK_REGULAR);
    } else {
        newSeries->options |= SERIES_OPT_COMPRESSED_GORILLA;
        newSeries->funcs = GetChunkClass(CHUNK_COMPRESSED);
    }

    if (!cCtx->skipChunkCreation) {
        Chunk_t *newChunk = newSeries->funcs->NewChunk(newSeries->chunkSizeBytes);
        dictOperator(newSeries->chunks, newChunk, 0, DICT_OP_SET);
        newSeries->lastChunk = newChunk;
    } else {
        newSeries->lastChunk = NULL;
    }

    return newSeries;
}

void SeriesTrim(Series *series, timestamp_t startTs, timestamp_t endTs) {
    // if not causedByRetention, caused by ts.del
    if (series->retentionTime == 0) {
        return;
    }

    // start iterator from smallest key
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(series->chunks, "^", NULL, 0);

    Chunk_t *currentChunk;
    void *currentKey;
    size_t keyLen;
    timestamp_t minTimestamp = series->lastTimestamp > series->retentionTime
                                   ? series->lastTimestamp - series->retentionTime
                                   : 0;

    const ChunkFuncs *funcs = series->funcs;
    while ((currentKey = RedisModule_DictNextC(iter, &keyLen, (void *)&currentChunk))) {
        if (funcs->GetLastTimestamp(currentChunk) >= minTimestamp) {
            break;
        }

        RedisModule_DictDelC(series->chunks, currentKey, keyLen, NULL);
        // reseek iterator since we modified the dict,
        // go to first element that is bigger than current key
        RedisModule_DictIteratorReseekC(iter, ">", currentKey, keyLen);

        series->totalSamples -= funcs->GetNumOfSample(currentChunk);
        funcs->FreeChunk(currentChunk);
    }

    RedisModule_DictIteratorStop(iter);
}

// Encode timestamps as bigendian to allow correct lexical sorting
void seriesEncodeTimestamp(void *buf, timestamp_t timestamp) {
    uint64_t e;
    e = htonu64(timestamp);
    memcpy(buf, &e, sizeof(e));
}

void RestoreKey(RedisModuleCtx *ctx, RedisModuleString *keyname) {
    Series *series;
    RedisModuleKey *key = NULL;
    if (GetSeries(ctx, keyname, &key, &series, REDISMODULE_READ | REDISMODULE_WRITE, false, true) !=
        TRUE) {
        return;
    }

    // update self keyname cause on rdb_load we don't have the
    // key name and the key might be loaded with different name
    RedisModule_FreeString(NULL, series->keyName); // free old name allocated on rdb_load()
    RedisModule_RetainString(NULL, keyname);
    series->keyName = keyname;

    if (IsKeyIndexed(keyname)) {
        // Key is still in the index cause only free series being called, remove it for safety
        RemoveIndexedMetric(keyname);
    }
    IndexMetric(keyname, series->labels, series->labelsCount);

    // Remove references to other keys
    if (series->srcKey) {
        RedisModule_FreeString(NULL, series->srcKey);
        series->srcKey = NULL;
    }

    CompactionRule *rule = series->rules;
    while (rule != NULL) {
        CompactionRule *nextRule = rule->nextRule;
        FreeCompactionRule(rule);
        rule = nextRule;
    }
    series->rules = NULL;

    RedisModule_CloseKey(key);
}

void IndexMetricFromName(RedisModuleCtx *ctx, RedisModuleString *keyname) {
    // Try to open the series
    Series *series;
    RedisModuleKey *key = NULL;
    RedisModuleString *_keyname = RedisModule_HoldString(ctx, keyname);
    const int status = GetSeries(ctx, _keyname, &key, &series, REDISMODULE_READ, false, true);
    if (!status) { // Not a timeseries key
        goto cleanup;
    }

    if (unlikely(IsKeyIndexed(_keyname))) {
        // when loading from rdb file the key shouldn't exist.
        size_t len;
        const char *str = RedisModule_StringPtrLen(_keyname, &len);
        RedisModule_Log(
            ctx, "warning", "Trying to load rdb a key=%s, which is already in index", str);
        RemoveIndexedMetric(_keyname); // for safety
    }

    IndexMetric(_keyname, series->labels, series->labelsCount);

cleanup:
    if (key) {
        RedisModule_CloseKey(key);
    }
    RedisModule_FreeString(ctx, _keyname);
}

void RenameSeriesFrom(RedisModuleCtx *ctx, RedisModuleString *key) {
    // keep in global variable for RenameSeriesTo() and increase recount
    RedisModule_RetainString(NULL, key);
    renameFromKey = key;
}

static void UpdateReferencesToRenamedSeries(RedisModuleCtx *ctx,
                                            Series *series,
                                            RedisModuleString *keyTo) {
    // A destination key was renamed
    if (series->srcKey) {
        Series *srcSeries;
        RedisModuleKey *srcKey;
        const int status =
            GetSeries(ctx, series->srcKey, &srcKey, &srcSeries, REDISMODULE_WRITE, false, false);
        if (status) {
            // Find the rule in the source key and rename the its destKey
            CompactionRule *rule = srcSeries->rules;
            while (rule) {
                if (RedisModule_StringCompare(renameFromKey, rule->destKey) == 0) {
                    RedisModule_FreeString(NULL, rule->destKey);
                    RedisModule_RetainString(NULL, keyTo);
                    rule->destKey = keyTo;
                    break; // Only one src can point back to destKey
                }
                rule = rule->nextRule;
            }
            RedisModule_CloseKey(srcKey);
        }
    }

    // A source key was renamed need to rename the srcKey on all the destKeys
    CompactionRule *rule = series->rules;
    while (rule) {
        Series *destSeries;
        RedisModuleKey *destKey;
        CompactionRule *nextRule = rule->nextRule; // avoid iterator invalidation
        const int status =
            GetSeries(ctx, rule->destKey, &destKey, &destSeries, REDISMODULE_WRITE, false, false);
        if (status) {
            // rename the srcKey in the destKey
            RedisModule_FreeString(NULL, destSeries->srcKey);
            RedisModule_RetainString(NULL, keyTo);
            destSeries->srcKey = keyTo;

            RedisModule_CloseKey(destKey);
        }
        rule = nextRule;
    }
}

void RenameSeriesTo(RedisModuleCtx *ctx, RedisModuleString *keyTo) {
    // Try to open the series
    Series *series;
    RedisModuleKey *key = NULL;
    const int status =
        GetSeries(ctx, keyTo, &key, &series, REDISMODULE_READ | REDISMODULE_WRITE, true, true);
    if (!status) { // Not a timeseries key
        goto cleanup;
    }

    // Reindex key by the new name
    RemoveIndexedMetric(renameFromKey);
    IndexMetric(keyTo, series->labels, series->labelsCount);

    UpdateReferencesToRenamedSeries(ctx, series, keyTo);

    RedisModule_FreeString(NULL, series->keyName);
    RedisModule_RetainString(NULL, keyTo);
    series->keyName = keyTo;

cleanup:
    if (key) {
        RedisModule_CloseKey(key);
    }
    RedisModule_FreeString(NULL, renameFromKey);
    renameFromKey = NULL;
}

void *CopySeries(RedisModuleString *fromkey, RedisModuleString *tokey, const void *value) {
    Series *src = (Series *)value;
    Series *dst = (Series *)calloc(1, sizeof(Series));
    memcpy(dst, src, sizeof(Series));
    RedisModule_RetainString(NULL, tokey);
    dst->keyName = tokey;

    // Copy labels
    if (src->labelsCount > 0) {
        dst->labels = calloc(src->labelsCount, sizeof(Label));
        for (size_t i = 0; i < dst->labelsCount; i++) {
            dst->labels[i].key = RedisModule_CreateStringFromString(NULL, src->labels[i].key);
            dst->labels[i].value = RedisModule_CreateStringFromString(NULL, src->labels[i].value);
        }
    }

    // Copy chunks
    dst->chunks = RedisModule_CreateDict(NULL);
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(src->chunks, "^", NULL, 0);
    Chunk_t *curChunk;
    char *curKey;
    size_t keylen;
    while ((curKey = RedisModule_DictNextC(iter, &keylen, &curChunk)) != NULL) {
        Chunk_t *newChunk = src->funcs->CloneChunk(curChunk);
        RedisModule_DictSetC(dst->chunks, curKey, keylen, newChunk);
        if (src->lastChunk == curChunk) {
            dst->lastChunk = newChunk;
        }
    }

    RedisModule_DictIteratorStop(iter);

    dst->srcKey = NULL;
    dst->rules = NULL;

    RemoveIndexedMetric(tokey); // in case of replace
    if (dst->labelsCount > 0) {
        IndexMetric(tokey, dst->labels, dst->labelsCount);
    }
    return dst;
}

// Releases Series and all its compaction rules
// Doesn't free the cross reference between rules, only on "del" keyspace notification,
// since Flush anyway will free all series.
// Doesn't free the index just on "del" keyspace notification since RoF might delete the key while
// it's only on the disk, in this case FreeSeries won't be called just the "del" keyspace
// notification.
void FreeSeries(void *value) {
    Series *series = (Series *)value;
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(series->chunks, "^", NULL, 0);
    Chunk_t *currentChunk;
    while (RedisModule_DictNextC(iter, NULL, (void *)&currentChunk) != NULL) {
        series->funcs->FreeChunk(currentChunk);
    }
    RedisModule_DictIteratorStop(iter);

    FreeLabels(series->labels, series->labelsCount);

    RedisModule_FreeDict(NULL, series->chunks);

    CompactionRule *rule = series->rules;
    while (rule != NULL) {
        CompactionRule *nextRule = rule->nextRule;
        FreeCompactionRule(rule);
        rule = nextRule;
    }

    if (series->srcKey != NULL) {
        RedisModule_FreeString(NULL, series->srcKey);
    }
    if (series->keyName) {
        RedisModule_FreeString(NULL, series->keyName);
    }

    free(series);
}

void FreeCompactionRule(void *value) {
    CompactionRule *rule = (CompactionRule *)value;
    RedisModule_FreeString(NULL, rule->destKey);
    ((AggregationClass *)rule->aggClass)->freeContext(rule->aggContext);
    free(rule);
}

size_t SeriesGetChunksSize(Series *series) {
    size_t size = 0;
    Chunk_t *currentChunk;
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(series->chunks, "^", NULL, 0);
    while (RedisModule_DictNextC(iter, NULL, (void *)&currentChunk)) {
        size += series->funcs->GetChunkSize(currentChunk, true);
    }
    RedisModule_DictIteratorStop(iter);
    return size;
}

char *SeriesGetCStringLabelValue(const Series *series, const char *labelKey) {
    char *result = NULL;
    for (int i = 0; i < series->labelsCount; i++) {
        const char *currLabel = RedisModule_StringPtrLen(series->labels[i].key, NULL);
        if (strcmp(currLabel, labelKey) == 0) {
            result = strdup(RedisModule_StringPtrLen(series->labels[i].value, NULL));
            break;
        }
    }
    return result;
}

size_t SeriesMemUsage(const void *value) {
    Series *series = (Series *)value;

    size_t labelLen = 0;
    uint32_t labelsLen = 0;
    for (int i = 0; i < series->labelsCount; i++) {
        RedisModule_StringPtrLen(series->labels[i].key, &labelLen);
        labelsLen += (labelLen + 1);
        RedisModule_StringPtrLen(series->labels[i].value, &labelLen);
        labelsLen += (labelLen + 1);
    }

    size_t rulesSize = 0;
    CompactionRule *rule = series->rules;
    while (rule != NULL) {
        rulesSize += sizeof(CompactionRule);
        rule = rule->nextRule;
    }

    return sizeof(series) + rulesSize + labelsLen + sizeof(Label) * series->labelsCount +
           SeriesGetChunksSize(series);
}

size_t SeriesGetNumSamples(const Series *series) {
    size_t numSamples = 0;
    if (series != NULL) {
        numSamples = series->totalSamples;
    }
    return numSamples;
}

int MultiSerieReduce(Series *dest,
                     Series *source,
                     MultiSeriesReduceOp op,
                     const RangeArgs *args,
                     bool reverse) {
    Sample sample;
    AbstractSampleIterator *iterator = SeriesCreateSampleIterator(source, args, reverse, true);
    DuplicatePolicy dp = DP_INVALID;
    switch (op) {
        case MultiSeriesReduceOp_Max:
            dp = DP_MAX;
            break;
        case MultiSeriesReduceOp_Min:
            dp = DP_MIN;
            break;
        case MultiSeriesReduceOp_Sum:
            dp = DP_SUM;
            break;
    }
    while (iterator->GetNext(iterator, &sample) == CR_OK) {
        SeriesUpsertSample(dest, sample.timestamp, sample.value, dp);
    }
    iterator->Close(iterator);
    return 1;
}

static bool RuleSeriesUpsertSample(RedisModuleCtx *ctx,
                                   Series *series,
                                   CompactionRule *rule,
                                   timestamp_t start,
                                   double val) {
    RedisModuleKey *key;
    Series *destSeries;
    if (!GetSeries(ctx,
                   rule->destKey,
                   &key,
                   &destSeries,
                   REDISMODULE_READ | REDISMODULE_WRITE,
                   false,
                   false)) {
        RedisModule_Log(ctx, "verbose", "%s", "Failed to retrieve downsample series");
        return false;
    }

    if (destSeries->totalSamples == 0) {
        SeriesAddSample(destSeries, start, val);
    } else {
        SeriesUpsertSample(destSeries, start, val, DP_LAST);
    }
    RedisModule_CloseKey(key);

    return true;
}

static void upsertCompaction(Series *series, UpsertCtx *uCtx) {
    if (series->rules == NULL) {
        return;
    }
    deleteReferenceToDeletedSeries(rts_staticCtx, series);
    CompactionRule *rule = series->rules;
    const timestamp_t upsertTimestamp = uCtx->sample.timestamp;
    const timestamp_t seriesLastTimestamp = series->lastTimestamp;
    while (rule != NULL) {
        const timestamp_t ruleTimebucket = rule->timeBucket;
        const timestamp_t curAggWindowStart = CalcWindowStart(seriesLastTimestamp, ruleTimebucket);
        if (upsertTimestamp >= curAggWindowStart) {
            // upsert in latest timebucket
            const int rv = SeriesCalcRange(series, curAggWindowStart, UINT64_MAX, rule, NULL, NULL);
            if (rv == TSDB_ERROR) {
                RedisModule_Log(
                    rts_staticCtx, "verbose", "%s", "Failed to calculate range for downsample");
                continue;
            }
        } else {
            const timestamp_t start = CalcWindowStart(upsertTimestamp, ruleTimebucket);
            // ensure last include/exclude
            double val = 0;
            const int rv =
                SeriesCalcRange(series, start, start + ruleTimebucket - 1, rule, &val, NULL);
            if (rv == TSDB_ERROR) {
                RedisModule_Log(
                    rts_staticCtx, "verbose", "%s", "Failed to calculate range for downsample");
                continue;
            }

            if (!RuleSeriesUpsertSample(rts_staticCtx, series, rule, start, val)) {
                continue;
            }
        }
        rule = rule->nextRule;
    }
}

int SeriesUpsertSample(Series *series,
                       api_timestamp_t timestamp,
                       double value,
                       DuplicatePolicy dp_override) {
    bool latestChunk = true;
    void *chunkKey = NULL;
    const ChunkFuncs *funcs = series->funcs;
    Chunk_t *chunk = series->lastChunk;
    timestamp_t chunkFirstTS = funcs->GetFirstTimestamp(series->lastChunk);

    if (timestamp < chunkFirstTS && RedisModule_DictSize(series->chunks) > 1) {
        // Upsert in an older chunk
        latestChunk = false;
        timestamp_t rax_key;
        seriesEncodeTimestamp(&rax_key, timestamp);
        RedisModuleDictIter *dictIter =
            RedisModule_DictIteratorStartC(series->chunks, "<=", &rax_key, sizeof(rax_key));
        chunkKey = RedisModule_DictNextC(dictIter, NULL, (void *)&chunk);
        if (chunkKey == NULL) {
            RedisModule_DictIteratorReseekC(dictIter, "^", NULL, 0);
            chunkKey = RedisModule_DictNextC(dictIter, NULL, (void *)&chunk);
        }
        RedisModule_DictIteratorStop(dictIter);
        if (chunkKey == NULL) {
            return REDISMODULE_ERR;
        }
        chunkFirstTS = funcs->GetFirstTimestamp(chunk);
    }

    // Split chunks
    if (funcs->GetChunkSize(chunk, false) > series->chunkSizeBytes * SPLIT_FACTOR) {
        Chunk_t *newChunk = funcs->SplitChunk(chunk);
        if (newChunk == NULL) {
            return REDISMODULE_ERR;
        }
        timestamp_t newChunkFirstTS = funcs->GetFirstTimestamp(newChunk);
        dictOperator(series->chunks, newChunk, newChunkFirstTS, DICT_OP_SET);
        if (timestamp >= newChunkFirstTS) {
            chunk = newChunk;
            chunkFirstTS = newChunkFirstTS;
        }
        if (latestChunk) { // split of latest chunk
            series->lastChunk = newChunk;
        }
    }

    UpsertCtx uCtx = {
        .inChunk = chunk,
        .sample = { .timestamp = timestamp, .value = value },
    };

    int size = 0;

    // Use module level configuration if key level configuration doesn't exists
    DuplicatePolicy dp_policy;
    if (dp_override != DP_NONE) {
        dp_policy = dp_override;
    } else if (series->duplicatePolicy != DP_NONE) {
        dp_policy = series->duplicatePolicy;
    } else {
        dp_policy = TSGlobalConfig.duplicatePolicy;
    }

    ChunkResult rv = funcs->UpsertSample(&uCtx, &size, dp_policy);
    if (rv == CR_OK) {
        series->totalSamples += size;
        if (timestamp == series->lastTimestamp) {
            series->lastValue = uCtx.sample.value;
        }
        timestamp_t chunkFirstTSAfterOp = funcs->GetFirstTimestamp(uCtx.inChunk);
        if (chunkFirstTSAfterOp != chunkFirstTS) {
            // update chunk in dictionary if first timestamp changed
            if (dictOperator(series->chunks, NULL, chunkFirstTS, DICT_OP_DEL) == REDISMODULE_ERR) {
                dictOperator(series->chunks, NULL, 0, DICT_OP_DEL);
            }
            dictOperator(series->chunks, uCtx.inChunk, chunkFirstTSAfterOp, DICT_OP_SET);
        }

        upsertCompaction(series, &uCtx);
    }
    return rv;
}

int SeriesAddSample(Series *series, api_timestamp_t timestamp, double value) {
    // backfilling or update
    Sample sample = { .timestamp = timestamp, .value = value };
    ChunkResult ret = series->funcs->AddSample(series->lastChunk, &sample);

    if (ret == CR_END) {
        // When a new chunk is created trim the series
        SeriesTrim(series, 0, 0);

        Chunk_t *newChunk = series->funcs->NewChunk(series->chunkSizeBytes);
        dictOperator(series->chunks, newChunk, timestamp, DICT_OP_SET);
        ret = series->funcs->AddSample(newChunk, &sample);
        series->lastChunk = newChunk;
    }
    series->lastTimestamp = timestamp;
    series->lastValue = value;
    series->totalSamples++;
    return TSDB_OK;
}

static int ContinuousDeletion(RedisModuleCtx *ctx,
                              Series *series,
                              CompactionRule *rule,
                              timestamp_t start,
                              timestamp_t end) {
    RedisModuleKey *key;
    Series *destSeries;
    if (!GetSeries(ctx,
                   rule->destKey,
                   &key,
                   &destSeries,
                   REDISMODULE_READ | REDISMODULE_WRITE,
                   false,
                   false)) {
        RedisModule_Log(ctx, "verbose", "%s", "Failed to retrieve downsample series");
        return TSDB_ERROR;
    }

    SeriesDelRange(destSeries, start, end);

    RedisModule_CloseKey(key);
    return TSDB_OK;
}

void CompactionDelRange(Series *series, timestamp_t start_ts, timestamp_t end_ts) {
    if (!series->rules)
        return;

    deleteReferenceToDeletedSeries(rts_staticCtx, series);
    CompactionRule *rule = series->rules;

    while (rule) {
        const timestamp_t ruleTimebucket = rule->timeBucket;
        const timestamp_t curAggWindowStart =
            CalcWindowStart(series->lastTimestamp, ruleTimebucket);

        if (start_ts >= curAggWindowStart) {
            // All deletion range in latest timebucket - only update the context on the rule
            const int rv = SeriesCalcRange(series, curAggWindowStart, UINT64_MAX, rule, NULL, NULL);
            if (rv == TSDB_ERROR) {
                RedisModule_Log(
                    rts_staticCtx, "verbose", "%s", "Failed to calculate range for downsample");
                continue;
            }
        } else {
            const timestamp_t startTSWindowStart = CalcWindowStart(start_ts, ruleTimebucket);
            const timestamp_t endTSWindowStart = CalcWindowStart(end_ts, ruleTimebucket);
            timestamp_t continuous_deletion_start;
            timestamp_t continuous_deletion_end;
            double val = 0;
            bool is_empty;
            int rv;

            // ---- handle start bucket ----

            rv = SeriesCalcRange(series,
                                 startTSWindowStart,
                                 startTSWindowStart + ruleTimebucket - 1,
                                 rule,
                                 &val,
                                 &is_empty);
            if (unlikely(rv == TSDB_ERROR)) {
                RedisModule_Log(
                    rts_staticCtx, "verbose", "%s", "Failed to calculate range for downsample");
                continue;
            }

            if (is_empty) {
                // first bucket should be deleted
                continuous_deletion_start = startTSWindowStart;
            } else { // first bucket needs update
                // continuous deletion starts one bucket after startTSWindowStart
                continuous_deletion_start = startTSWindowStart + ruleTimebucket;
                if (!RuleSeriesUpsertSample(rts_staticCtx, series, rule, startTSWindowStart, val)) {
                    continue;
                }
            }

            // ---- handle end bucket ----

            if (end_ts >= curAggWindowStart) {
                // deletion in latest timebucket
                const int rv =
                    SeriesCalcRange(series, curAggWindowStart, UINT64_MAX, rule, NULL, NULL);
                if (rv == TSDB_ERROR) {
                    RedisModule_Log(
                        rts_staticCtx, "verbose", "%s", "Failed to calculate range for downsample");
                    continue;
                }
                // continuous deletion ends one bucket before endTSWindowStart
                continuous_deletion_end = endTSWindowStart - ruleTimebucket;
            } else {
                // deletion in old timebucket
                rv = SeriesCalcRange(series,
                                     endTSWindowStart,
                                     endTSWindowStart + ruleTimebucket - 1,
                                     rule,
                                     &val,
                                     &is_empty);
                if (unlikely(rv == TSDB_ERROR)) {
                    RedisModule_Log(
                        rts_staticCtx, "verbose", "%s", "Failed to calculate range for downsample");
                    continue;
                }

                if (is_empty) {
                    // continuous deletion ends in end timebucket
                    continuous_deletion_end = endTSWindowStart;
                } else { // update in end timebucket
                    // continuous deletion ends one bucket before endTSWindowStart
                    continuous_deletion_end = endTSWindowStart - ruleTimebucket;
                    if (!RuleSeriesUpsertSample(
                            rts_staticCtx, series, rule, endTSWindowStart, val)) {
                        continue;
                    }
                }
            }

            // ---- handle continuous deletion ----

            if (continuous_deletion_end >= continuous_deletion_start) {
                ContinuousDeletion(rts_staticCtx,
                                   series,
                                   rule,
                                   continuous_deletion_start,
                                   continuous_deletion_end);
            }
        }

        rule = rule->nextRule;
    }
}

size_t SeriesDelRange(Series *series, timestamp_t start_ts, timestamp_t end_ts) {
    // start iterator from smallest key
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(series->chunks, "^", NULL, 0);

    Chunk_t *currentChunk;
    void *currentKey;
    size_t keyLen;
    size_t deletedSamples = 0;
    const ChunkFuncs *funcs = series->funcs;
    while ((currentKey = RedisModule_DictNextC(iter, &keyLen, (void *)&currentChunk))) {
        // We deleted the latest samples, no more chunks/samples to delete or cur chunk start_ts is
        // larger than end_ts
        if (!currentKey || funcs->GetFirstTimestamp(currentChunk) > end_ts)
            break;

        // Should we delete the all chunk?
        bool ts_delCondition = (funcs->GetFirstTimestamp(currentChunk) >= start_ts &&
                                funcs->GetLastTimestamp(currentChunk) <= end_ts) &&
                               currentChunk != series->lastChunk;

        if (!ts_delCondition) {
            deletedSamples += funcs->DelRange(currentChunk, start_ts, end_ts);
            continue;
        }

        RedisModule_DictDelC(series->chunks, currentKey, keyLen, NULL);
        deletedSamples += funcs->GetNumOfSample(currentChunk);
        funcs->FreeChunk(currentChunk);

        // reseek iterator since we modified the dict,
        // go to first element that is bigger than current key
        RedisModule_DictIteratorReseekC(iter, ">", currentKey, keyLen);
    }
    series->totalSamples -= deletedSamples;

    RedisModule_DictIteratorStop(iter);

    CompactionDelRange(series, start_ts, end_ts);
    return deletedSamples;
}

CompactionRule *SeriesAddRule(RedisModuleCtx *ctx,
                              Series *series,
                              Series *destSeries,
                              int aggType,
                              uint64_t timeBucket) {
    CompactionRule *rule = NewRule(destSeries->keyName, aggType, timeBucket);
    if (rule == NULL) {
        return NULL;
    }
    RedisModule_RetainString(ctx, destSeries->keyName);
    if (series->rules == NULL) {
        series->rules = rule;
    } else {
        CompactionRule *last = series->rules;
        while (last->nextRule != NULL)
            last = last->nextRule;
        last->nextRule = rule;
    }
    return rule;
}

int SeriesCreateRulesFromGlobalConfig(RedisModuleCtx *ctx,
                                      RedisModuleString *keyName,
                                      Series *series,
                                      Label *labels,
                                      size_t labelsCount) {
    size_t len;
    int i;
    Series *compactedSeries;
    RedisModuleKey *compactedKey;
    size_t compactedRuleLabelCount = labelsCount + 2;

    for (i = 0; i < TSGlobalConfig.compactionRulesCount; i++) {
        SimpleCompactionRule *rule = TSGlobalConfig.compactionRules + i;
        const char *aggString = AggTypeEnumToString(rule->aggType);
        RedisModuleString *destKey =
            RedisModule_CreateStringPrintf(ctx,
                                           "%s_%s_%" PRIu64,
                                           RedisModule_StringPtrLen(keyName, &len),
                                           aggString,
                                           rule->timeBucket);
        compactedKey = RedisModule_OpenKey(ctx, destKey, REDISMODULE_READ | REDISMODULE_WRITE);
        if (RedisModule_KeyType(compactedKey) != REDISMODULE_KEYTYPE_EMPTY) {
            // TODO: should we break here? Is log enough?
            RM_LOG_WARNING(ctx,
                           "Cannot create compacted key, key '%s' already exists",
                           RedisModule_StringPtrLen(destKey, NULL));
            RedisModule_FreeString(ctx, destKey);
            RedisModule_CloseKey(compactedKey);
            continue;
        }

        Label *compactedLabels = calloc(compactedRuleLabelCount, sizeof(Label));
        // todo: deep copy labels function
        for (int l = 0; l < labelsCount; l++) {
            compactedLabels[l].key = RedisModule_CreateStringFromString(NULL, labels[l].key);
            compactedLabels[l].value = RedisModule_CreateStringFromString(NULL, labels[l].value);
        }

        // For every aggregated key create 2 labels: `aggregation` and `time_bucket`.
        compactedLabels[labelsCount].key = RedisModule_CreateStringPrintf(NULL, "aggregation");
        compactedLabels[labelsCount].value =
            RedisModule_CreateString(NULL, aggString, strlen(aggString));
        compactedLabels[labelsCount + 1].key = RedisModule_CreateStringPrintf(NULL, "time_bucket");
        compactedLabels[labelsCount + 1].value =
            RedisModule_CreateStringPrintf(NULL, "%" PRIu64, rule->timeBucket);

        int rules_options = TSGlobalConfig.options;
        rules_options &= ~SERIES_OPT_DEFAULT_COMPRESSION;
        rules_options &= SERIES_OPT_UNCOMPRESSED;

        CreateCtx cCtx = {
            .retentionTime = rule->retentionSizeMillisec,
            .chunkSizeBytes = TSGlobalConfig.chunkSizeBytes,
            .labelsCount = compactedRuleLabelCount,
            .labels = compactedLabels,
            .options = rules_options,
        };
        CreateTsKey(ctx, destKey, &cCtx, &compactedSeries, &compactedKey);
        SeriesSetSrcRule(ctx, compactedSeries, series->keyName);
        SeriesAddRule(ctx, series, compactedSeries, rule->aggType, rule->timeBucket);
        RedisModule_CloseKey(compactedKey);
    }
    return TSDB_OK;
}

CompactionRule *NewRule(RedisModuleString *destKey, int aggType, uint64_t timeBucket) {
    if (timeBucket == 0ULL) {
        return NULL;
    }

    CompactionRule *rule = (CompactionRule *)malloc(sizeof(CompactionRule));
    rule->aggClass = GetAggClass(aggType);
    rule->aggType = aggType;
    rule->aggContext = rule->aggClass->createContext();
    rule->timeBucket = timeBucket;
    rule->destKey = destKey;
    rule->startCurrentTimeBucket = -1LL;
    rule->nextRule = NULL;

    return rule;
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
            } else {
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

void SeriesSetSrcRule(RedisModuleCtx *ctx, Series *series, RedisModuleString *srcKeyName) {
    RedisModule_RetainString(ctx, srcKeyName);
    series->srcKey = srcKeyName;
}

int SeriesDeleteSrcRule(Series *series, RedisModuleString *srctKey) {
    if (RMUtil_StringEquals(series->srcKey, srctKey)) {
        RedisModule_FreeString(NULL, series->srcKey);
        series->srcKey = NULL;
        return TRUE;
    }
    return FALSE;
}

/*
 * This function calculate aggregation value of a range.
 *
 * If `val` is NULL, the function will update the context of `rule`.
 */
int SeriesCalcRange(Series *series,
                    timestamp_t start_ts,
                    timestamp_t end_ts,
                    CompactionRule *rule,
                    double *val,
                    bool *is_empty) {
    Sample sample;
    AggregationClass *aggObject = rule->aggClass;
    const RangeArgs args = {
        .startTimestamp = start_ts,
        .endTimestamp = end_ts,
        .aggregationArgs = { 0 },
        .filterByValueArgs = { 0 },
    };

    AbstractSampleIterator *iterator = SeriesCreateSampleIterator(series, &args, false, true);

    void *context = aggObject->createContext();
    bool _is_empty = true;
    ChunkResult res;

    res = iterator->GetNext(iterator, &sample);
    if (res == CR_OK) {
        _is_empty = false;
    }
    while (res == CR_OK) {
        aggObject->appendValue(context, sample.value);
        res = iterator->GetNext(iterator, &sample);
    }

    if (is_empty) {
        *is_empty = _is_empty;
    }

    iterator->Close(iterator);
    if (val == NULL) { // just update context for current window
        aggObject->freeContext(rule->aggContext);
        rule->aggContext = context;
    } else {
        if (!_is_empty) {
            aggObject->finalize(context, val);
        }
        aggObject->freeContext(context);
    }
    return TSDB_OK;
}

timestamp_t CalcWindowStart(timestamp_t timestamp, size_t window) {
    return timestamp - (timestamp % window);
}

timestamp_t getFirstValidTimestamp(Series *series, long long *skipped) {
    if (skipped != NULL) {
        *skipped = 0;
    }
    if (series->totalSamples == 0) {
        return 0;
    }

    size_t count = 0;
    Sample sample = { 0 };

    timestamp_t minTimestamp = 0;
    if (series->retentionTime && series->retentionTime < series->lastTimestamp) {
        minTimestamp = series->lastTimestamp - series->retentionTime;
    }

    const RangeArgs args = {
        .startTimestamp = 0,
        .endTimestamp = series->lastTimestamp,
        .aggregationArgs = { 0 },
        .filterByValueArgs = { 0 },
    };
    AbstractSampleIterator *iterator = SeriesCreateSampleIterator(series, &args, false, false);

    while (iterator->GetNext(iterator, &sample) == CR_OK) {
        if (sample.timestamp >= minTimestamp) {
            break;
        }
        ++count;
    }

    if (skipped != NULL) {
        *skipped = count;
    }
    iterator->Close(iterator);
    return sample.timestamp;
}

AbstractIterator *SeriesQuery(Series *series,
                              const RangeArgs *args,
                              bool reverse,
                              bool check_retention) {
    // In case a retention is set shouldn't return chunks older than the retention
    timestamp_t startTimestamp = args->startTimestamp;
    if (check_retention && series->retentionTime > 0) {
        startTimestamp =
            series->lastTimestamp > series->retentionTime
                ? max(args->startTimestamp, series->lastTimestamp - series->retentionTime)
                : args->startTimestamp;
    }

    // When there is a TS filter the filter itself will reverse if needed
    bool should_reverse_chunk = reverse && (!args->filterByTSArgs.hasValue);
    AbstractIterator *chain = SeriesIterator_New(
        series, startTimestamp, args->endTimestamp, reverse, should_reverse_chunk);

    if (args->filterByTSArgs.hasValue) {
        chain =
            (AbstractIterator *)SeriesFilterTSIterator_New(chain, args->filterByTSArgs, reverse);
    }

    if (args->filterByValueArgs.hasValue) {
        chain = (AbstractIterator *)SeriesFilterValIterator_New(chain, args->filterByValueArgs);
    }

    timestamp_t timestampAlignment;
    switch (args->alignment) {
        case StartAlignment:
            // args-startTimestamp can hold an older timestamp than what we currently have or just 0
            timestampAlignment = args->startTimestamp;
            break;
        case EndAlignment:
            timestampAlignment = args->endTimestamp;
            break;
        case TimestampAlignment:
            timestampAlignment = args->timestampAlignment;
            break;
        default:
            timestampAlignment = 0;
            break;
    }

    if (args->aggregationArgs.aggregationClass != NULL) {
        chain = (AbstractIterator *)AggregationIterator_New(chain,
                                                            args->aggregationArgs.aggregationClass,
                                                            args->aggregationArgs.timeDelta,
                                                            timestampAlignment,
                                                            reverse);
    }

    return chain;
}

AbstractSampleIterator *SeriesCreateSampleIterator(Series *series,
                                                   const RangeArgs *args,
                                                   bool reverse,
                                                   bool check_retention) {
    AbstractIterator *chain = SeriesQuery(series, args, reverse, check_retention);
    return (AbstractSampleIterator *)SeriesSampleIterator_New(chain);
}
