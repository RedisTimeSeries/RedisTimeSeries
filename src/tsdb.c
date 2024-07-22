/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
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
#include "multiseries_sample_iterator.h"
#include "multiseries_agg_dup_sample_iterator.h"
#include "rdb.h"

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
    newSeries->ignoreMaxTimeDiff = cCtx->ignoreMaxTimeDiff;
    newSeries->ignoreMaxValDiff = cCtx->ignoreMaxValDiff;
    newSeries->in_ram = true;

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

    if (last_rdb_load_version < TS_REPLICAOF_SUPPORT_VER) {
        // In versions greater than TS_REPLICAOF_SUPPORT_VER we delete the reference on the dump
        // stage

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
    }

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

    dst->in_ram = src->in_ram;
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
                     Series **series,
                     size_t n_series,
                     const ReducerArgs *gropuByReducerArgs,
                     RangeArgs *args) {
    Sample sample;
    AbstractSampleIterator *iterator = MultiSeriesCreateAggDupSampleIterator(
        series, n_series, args, false, true, gropuByReducerArgs);
    while (iterator->GetNext(iterator, &sample) == CR_OK) {
        SeriesAddSample(dest, sample.timestamp, sample.value);
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
        const timestamp_t ruleTimebucket = rule->bucketDuration;
        const timestamp_t curAggWindowStart =
            CalcBucketStart(seriesLastTimestamp, ruleTimebucket, rule->timestampAlignment);
        const timestamp_t curAggWindowStartNormalized = BucketStartNormalize(curAggWindowStart);
        if (upsertTimestamp >= curAggWindowStartNormalized) {
            // upsert in latest timebucket
            const int rv = SeriesCalcRange(series,
                                           curAggWindowStartNormalized,
                                           curAggWindowStart + ruleTimebucket - 1,
                                           rule,
                                           NULL,
                                           NULL);
            if (rv == TSDB_ERROR) {
                RedisModule_Log(
                    rts_staticCtx, "verbose", "%s", "Failed to calculate range for downsample");
                continue;
            }
        } else {
            const timestamp_t start =
                CalcBucketStart(upsertTimestamp, ruleTimebucket, rule->timestampAlignment);
            const timestamp_t startNormalized = BucketStartNormalize(start);
            // ensure last include/exclude
            double val = 0;
            const int rv = SeriesCalcRange(
                series, startNormalized, start + ruleTimebucket - 1, rule, &val, NULL);
            if (rv == TSDB_ERROR) {
                RedisModule_Log(
                    rts_staticCtx, "verbose", "%s", "Failed to calculate range for downsample");
                continue;
            }

            if (!RuleSeriesUpsertSample(rts_staticCtx, series, rule, startNormalized, val)) {
                continue;
            }
        }
        rule = rule->nextRule;
    }
}

// update chunk in dictionary if first timestamp changed
static inline void update_chunk_in_dict(RedisModuleDict *chunks,
                                        Chunk_t *chunk,
                                        timestamp_t chunkOrigFirstTS,
                                        timestamp_t chunkFirstTSAfterOp) {
    if (dictOperator(chunks, NULL, chunkOrigFirstTS, DICT_OP_DEL) == REDISMODULE_ERR) {
        dictOperator(chunks, NULL, 0, DICT_OP_DEL); // The first chunk is a special case
    }
    dictOperator(chunks, chunk, chunkFirstTSAfterOp, DICT_OP_SET);
}

int SeriesUpsertSample(Series *series,
                       api_timestamp_t timestamp,
                       double value,
                       DuplicatePolicy dp_policy) {
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

    ChunkResult rv = funcs->UpsertSample(&uCtx, &size, dp_policy);
    if (rv == CR_OK) {
        series->totalSamples += size;
        if (timestamp == series->lastTimestamp) {
            series->lastValue = uCtx.sample.value;
        }
        timestamp_t chunkFirstTSAfterOp = funcs->GetFirstTimestamp(uCtx.inChunk);
        if (chunkFirstTSAfterOp != chunkFirstTS) {
            update_chunk_in_dict(series->chunks, uCtx.inChunk, chunkFirstTS, chunkFirstTSAfterOp);
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

static bool delete_sample_before(RedisModuleCtx *ctx,
                                 RedisModuleString *series_name,
                                 timestamp_t ts,
                                 timestamp_t *deleted) {
    RedisModuleKey *key;
    Series *series;
    bool rv = true;
    if (!GetSeries(
            ctx, series_name, &key, &series, REDISMODULE_READ | REDISMODULE_WRITE, false, false)) {
        RedisModule_Log(ctx, "verbose", "%s", "Failed to retrieve downsample series");
        return false;
    }

    timestamp_t rax_key;
    Chunk_t *chunk;
    seriesEncodeTimestamp(&rax_key, ts);
    RedisModuleDictIter *dictIter =
        RedisModule_DictIteratorStartC(series->chunks, "<", &rax_key, sizeof(rax_key));
    void *chunkKey = RedisModule_DictNextC(dictIter, NULL, (void *)&chunk);
    if (chunkKey == NULL || series->funcs->GetNumOfSample(chunk) == 0) {
        rv = false;
        goto _out;
    }

    *deleted = Uncompressed_GetLastTimestamp(chunk);
    SeriesDelRange(series, *deleted, *deleted);

_out:
    RedisModule_CloseKey(key);
    RedisModule_DictIteratorStop(dictIter);
    return rv;
}

static void CompactionDelRange(Series *series,
                               timestamp_t start_ts,
                               timestamp_t end_ts,
                               timestamp_t last_ts_before_deletion) {
    if (!series->rules)
        return;

    deleteReferenceToDeletedSeries(rts_staticCtx, series);
    CompactionRule *rule = series->rules;
    bool is_empty;

    while (rule) {
        const timestamp_t ruleTimebucket = rule->bucketDuration;
        const timestamp_t curAggWindowStart =
            CalcBucketStart(last_ts_before_deletion, ruleTimebucket, rule->timestampAlignment);
        const timestamp_t curAggWindowStartNormalized = BucketStartNormalize(curAggWindowStart);
        bool latest_timebucket_deleted = false;

        if (start_ts >= curAggWindowStartNormalized) {
            // All deletion range in latest timebucket - only update the context on the rule
            const int rv = SeriesCalcRange(series,
                                           curAggWindowStartNormalized,
                                           curAggWindowStart + ruleTimebucket - 1,
                                           rule,
                                           NULL,
                                           &is_empty);
            if (rv == TSDB_ERROR) {
                RedisModule_Log(
                    rts_staticCtx, "verbose", "%s", "Failed to calculate range for downsample");
                continue;
            }

            if (is_empty) {
                latest_timebucket_deleted = true;
            }
        } else {
            const timestamp_t startTSWindowStart =
                CalcBucketStart(start_ts, ruleTimebucket, rule->timestampAlignment);
            const timestamp_t startTSWindowStartNormalized =
                BucketStartNormalize(startTSWindowStart);
            const timestamp_t endTSWindowStart =
                CalcBucketStart(end_ts, ruleTimebucket, rule->timestampAlignment);
            const timestamp_t endTSWindowStartNormalized = BucketStartNormalize(endTSWindowStart);
            timestamp_t continuous_deletion_start;
            timestamp_t continuous_deletion_end;
            double val = 0;
            int rv;

            // ---- handle start bucket ----

            rv = SeriesCalcRange(series,
                                 startTSWindowStartNormalized,
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
                continuous_deletion_start = startTSWindowStartNormalized;
            } else { // first bucket needs update
                // continuous deletion starts one bucket after startTSWindowStart
                continuous_deletion_start = startTSWindowStart + ruleTimebucket;
                if (!RuleSeriesUpsertSample(
                        rts_staticCtx, series, rule, startTSWindowStartNormalized, val)) {
                    continue;
                }
            }

            // ---- handle end bucket ----

            if (end_ts >= curAggWindowStartNormalized) {
                // deletion in latest timebucket
                const int rv = SeriesCalcRange(
                    series, curAggWindowStartNormalized, UINT64_MAX, rule, NULL, &is_empty);
                if (rv == TSDB_ERROR) {
                    RedisModule_Log(
                        rts_staticCtx, "verbose", "%s", "Failed to calculate range for downsample");
                    continue;
                }

                if (is_empty) {
                    latest_timebucket_deleted = true;
                }

                // continuous deletion ends one bucket before endTSWindowStart
                continuous_deletion_end = BucketStartNormalize(endTSWindowStart - ruleTimebucket);
            } else {
                // deletion in old timebucket
                rv = SeriesCalcRange(series,
                                     endTSWindowStartNormalized,
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
                    continuous_deletion_end = endTSWindowStartNormalized;
                } else { // update in end timebucket
                    // continuous deletion ends one bucket before endTSWindowStart
                    continuous_deletion_end =
                        BucketStartNormalize(endTSWindowStart - ruleTimebucket);
                    if (!RuleSeriesUpsertSample(
                            rts_staticCtx, series, rule, endTSWindowStartNormalized, val)) {
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

        // If the latest timebucket was deleted, we need to update the context to be the
        // previous timebucket
        if (latest_timebucket_deleted) {
            timestamp_t last_ts;
            // delete the last sample in the downsampled series
            if (!delete_sample_before(
                    rts_staticCtx, rule->destKey, last_ts_before_deletion, &last_ts)) {
                // The compaction series is empty
                last_ts = 0;
            }

            const timestamp_t wind_start =
                CalcBucketStart(last_ts, ruleTimebucket, rule->timestampAlignment);
            const timestamp_t wind_start_normalized = BucketStartNormalize(wind_start);

            // update the context to be the latest timebucket
            SeriesCalcRange(series,
                            wind_start_normalized,
                            wind_start_normalized + ruleTimebucket - 1,
                            rule,
                            NULL,
                            &is_empty);

            // if is_empty, it means that series is empty and we need to set startCurrentTimeBucket
            // to -1
            rule->startCurrentTimeBucket = is_empty ? -1LL : wind_start_normalized;
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
        if (!currentKey || (funcs->GetNumOfSample(currentChunk) == 0) ||
            funcs->GetFirstTimestamp(currentChunk) > end_ts) {
            // Having empty chunk means the series is empty
            break;
        }

        if (funcs->GetLastTimestamp(currentChunk) < start_ts) {
            continue;
        }

        bool is_only_chunk =
            ((funcs->GetNumOfSample(currentChunk) + deletedSamples) == series->totalSamples);
        // Should we delete the all chunk?
        bool ts_delCondition =
            (funcs->GetFirstTimestamp(currentChunk) >= start_ts &&
             funcs->GetLastTimestamp(currentChunk) <= end_ts) &&
            (!is_only_chunk); // We assume at least one allocated chunk in the series

        if (!ts_delCondition) {
            timestamp_t chunkFirstTS = funcs->GetFirstTimestamp(currentChunk);
            deletedSamples += funcs->DelRange(currentChunk, start_ts, end_ts);
            timestamp_t chunkFirstTSAfterOp = funcs->GetFirstTimestamp(currentChunk);
            if (chunkFirstTSAfterOp != chunkFirstTS) {
                update_chunk_in_dict(
                    series->chunks, currentChunk, chunkFirstTS, chunkFirstTSAfterOp);
                // reseek iterator since we modified the dict,
                // go to first element that is bigger than current key
                timestamp_t rax_key;
                seriesEncodeTimestamp(&rax_key, chunkFirstTSAfterOp);
                RedisModule_DictIteratorReseekC(iter, ">", &rax_key, sizeof(rax_key));
            }
            continue;
        }

        bool isLastChunkDeleted = (currentChunk == series->lastChunk);
        RedisModule_DictDelC(series->chunks, currentKey, keyLen, NULL);
        deletedSamples += funcs->GetNumOfSample(currentChunk);
        funcs->FreeChunk(currentChunk);

        if (isLastChunkDeleted) {
            Chunk_t *lastChunk;
            RedisModuleDictIter *lastChunkIter =
                RedisModule_DictIteratorStartC(series->chunks, "$", NULL, 0);
            RedisModule_DictNextC(lastChunkIter, NULL, (void *)&lastChunk);
            series->lastChunk = lastChunk;
            RedisModule_DictIteratorStop(lastChunkIter);
        }

        // reseek iterator since we modified the dict,
        // go to first element that is bigger than current key
        RedisModule_DictIteratorReseekC(iter, ">", currentKey, keyLen);
    }
    series->totalSamples -= deletedSamples;

    RedisModule_DictIteratorStop(iter);

    timestamp_t last_ts_before_deletion = series->lastTimestamp;

    // Check if last timestamp deleted
    if (end_ts >= series->lastTimestamp && start_ts <= series->lastTimestamp) {
        iter = RedisModule_DictIteratorStartC(series->chunks, "$", NULL, 0);
        currentKey = RedisModule_DictNextC(iter, &keyLen, (void *)&currentChunk);
        if (!currentKey || (funcs->GetNumOfSample(currentChunk) == 0)) {
            // No samples in the series
            series->lastTimestamp = 0;
            series->lastValue = 0;
        } else {
            series->lastTimestamp = funcs->GetLastTimestamp(currentChunk);
            series->lastValue = funcs->GetLastValue(currentChunk);
        }
        RedisModule_DictIteratorStop(iter);
    }

    CompactionDelRange(series, start_ts, end_ts, last_ts_before_deletion);

    return deletedSamples;
}

CompactionRule *SeriesAddRule(RedisModuleCtx *ctx,
                              Series *series,
                              Series *destSeries,
                              int aggType,
                              uint64_t bucketDuration,
                              timestamp_t timestampAlignment) {
    CompactionRule *rule =
        NewRule(destSeries->keyName, aggType, bucketDuration, timestampAlignment);
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
        RedisModuleString *destKey;
        if (rule->timestampAlignment != 0) {
            destKey = RedisModule_CreateStringPrintf(ctx,
                                                     "%s_%s_%" PRIu64 "_%" PRIu64,
                                                     RedisModule_StringPtrLen(keyName, &len),
                                                     aggString,
                                                     rule->bucketDuration,
                                                     rule->timestampAlignment);
        } else {
            destKey = RedisModule_CreateStringPrintf(ctx,
                                                     "%s_%s_%" PRIu64,
                                                     RedisModule_StringPtrLen(keyName, &len),
                                                     aggString,
                                                     rule->bucketDuration);
        }

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
            RedisModule_CreateStringPrintf(NULL, "%" PRIu64, rule->bucketDuration);

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
        SeriesAddRule(ctx,
                      series,
                      compactedSeries,
                      rule->aggType,
                      rule->bucketDuration,
                      rule->timestampAlignment);
        RedisModule_CloseKey(compactedKey);
    }
    return TSDB_OK;
}

CompactionRule *NewRule(RedisModuleString *destKey,
                        int aggType,
                        uint64_t bucketDuration,
                        uint64_t timestampAlignment) {
    if (bucketDuration == 0ULL) {
        return NULL;
    }

    CompactionRule *rule = (CompactionRule *)malloc(sizeof(CompactionRule));
    rule->aggClass = GetAggClass(aggType);
    rule->aggType = aggType;
    rule->aggContext = rule->aggClass->createContext(false);
    rule->bucketDuration = bucketDuration;
    rule->timestampAlignment = timestampAlignment;
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
    void *context = aggObject->createContext(false);
    bool _is_empty = true;
    AbstractSampleIterator *iterator;
    RangeArgs args = { .aggregationArgs = { 0 },
                       .filterByValueArgs = { 0 },
                       .filterByTSArgs = { 0 } };

    if (aggObject->type == TS_AGG_TWA) {
        aggObject->addBucketParams(context, start_ts, end_ts + 1);
    }

    if (aggObject->type == TS_AGG_TWA && start_ts > 0) {
        args.startTimestamp = 0, args.endTimestamp = start_ts - 1,
        iterator = SeriesCreateSampleIterator(series, &args, true, true);
        if (iterator->GetNext(iterator, &sample) == CR_OK) {
            aggObject->addPrevBucketLastSample(context, sample.value, sample.timestamp);
        }
        iterator->Close(iterator);
    }

    args.startTimestamp = start_ts;
    args.endTimestamp = end_ts;
    iterator = SeriesCreateSampleIterator(series, &args, false, true);

    while (iterator->GetNext(iterator, &sample) == CR_OK) {
        aggObject->appendValue(context, sample.value, sample.timestamp);
        _is_empty = false;
    }
    iterator->Close(iterator);

    if (aggObject->type == TS_AGG_TWA) {
        args.startTimestamp = end_ts + 1, args.endTimestamp = UINT64_MAX,
        iterator = SeriesCreateSampleIterator(series, &args, false, true);
        if (iterator->GetNext(iterator, &sample) == CR_OK) {
            aggObject->addNextBucketFirstSample(context, sample.value, sample.timestamp);
        }
        iterator->Close(iterator);
    }

    if (is_empty) {
        *is_empty = _is_empty;
    }

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

    const RangeArgs args = { .startTimestamp = 0,
                             .endTimestamp = series->lastTimestamp,
                             .aggregationArgs = { 0 },
                             .filterByValueArgs = { 0 },
                             .filterByTSArgs = { 0 } };
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

    // When there is a TS filter because we wanted the logic to be one for both reverse and non
    // reverse chunk, if the requested range should be reverse, we reverse it after the filter, and
    // should_reverse_chunk point it out.
    bool should_reverse_chunk = reverse && (!args->filterByTSArgs.hasValue);
    AbstractIterator *chain = SeriesIterator_New(
        series, startTimestamp, args->endTimestamp, reverse, should_reverse_chunk, args->latest);

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
                                                            reverse,
                                                            args->aggregationArgs.empty,
                                                            args->aggregationArgs.bucketTS,
                                                            series,
                                                            args->startTimestamp,
                                                            args->endTimestamp);
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

// returns sample iterator over multiple series
AbstractMultiSeriesSampleIterator *MultiSeriesCreateSampleIterator(Series **series,
                                                                   size_t n_series,
                                                                   const RangeArgs *args,
                                                                   bool reverse,
                                                                   bool check_retention) {
    size_t i;
    AbstractSampleIterator **iters = malloc(n_series * sizeof(AbstractSampleIterator *));
    for (i = 0; i < n_series; ++i) {
        iters[i] = SeriesCreateSampleIterator(series[i], args, reverse, check_retention);
    }

    AbstractMultiSeriesSampleIterator *res =
        (AbstractMultiSeriesSampleIterator *)MultiSeriesSampleIterator_New(
            iters, n_series, reverse);

    free(iters);
    return res;
}

// returns sample iterator over multiple series
AbstractSampleIterator *MultiSeriesCreateAggDupSampleIterator(Series **series,
                                                              size_t n_series,
                                                              const RangeArgs *args,
                                                              bool reverse,
                                                              bool check_retention,
                                                              const ReducerArgs *reducerArgs) {
    AbstractMultiSeriesSampleIterator *chain =
        MultiSeriesCreateSampleIterator(series, n_series, args, reverse, check_retention);
    return (AbstractSampleIterator *)MultiSeriesAggDupSampleIterator_New(chain, reducerArgs);
}

void calculate_latest_sample(Sample **sample, const Series *series) {
    RedisModuleKey *srcKey = NULL;
    Series *srcSeries;
    const int status = GetSeries(
        rts_staticCtx, series->srcKey, &srcKey, &srcSeries, REDISMODULE_READ, false, true);
    if (!status || srcSeries->totalSamples == 0) {
        // LATEST is ignored for a series that is not a compaction.
        *sample = NULL;
    } else {
        /* If src key was deleted and recreated again, there is a chance that
         * it may not have a rule to the destination key anymore. */
        CompactionRule *rule = GetRule(srcSeries->rules, series->keyName);
        if (!rule || rule->startCurrentTimeBucket == -1LL) {
            // when srcSeries->totalSamples != 0 and rule->startCurrentTimeBucket == -1LL it means
            // that on ts.createrule the src wasn't empty This means that the rule context is empty
            *sample = NULL;
            goto __out;
        }
        void *clonedContext = rule->aggClass->cloneContext(rule->aggContext);

        double aggVal;
        rule->aggClass->finalize(clonedContext, &aggVal);
        (*sample)->timestamp = rule->startCurrentTimeBucket;
        (*sample)->value = aggVal;

        free(clonedContext);
    }

__out:
    if (srcKey) {
        RedisModule_CloseKey(srcKey);
    }
}
