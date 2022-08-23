/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "rdb.h"

#include "consts.h"
#include "endianconv.h"
#include "load_io_error_macros.h"
#include "module.h"

#include <inttypes.h>
#include <string.h>
#include <rmutil/alloc.h>

int last_rdb_load_version;

void *series_rdb_load(RedisModuleIO *io, int encver) {
    last_rdb_load_version = encver;
    if (encver < TS_ENC_VER || encver > TS_LATEST_ENCVER) {
        RedisModule_LogIOError(io, "error", "data is not in the correct encoding");
        return NULL;
    }
    double lastValue;
    timestamp_t lastTimestamp;
    uint64_t totalSamples;
    DuplicatePolicy duplicatePolicy = DP_NONE;
    RedisModuleString *srcKey = NULL;
    Series *series = NULL;
    RedisModuleString *destKey = NULL;

    CreateCtx cCtx = { 0 };
    RedisModuleString *keyName = NULL;
    keyName = LoadString_IOError(io, goto err);

    cCtx.retentionTime = LoadUnsigned_IOError(io, goto err);
    cCtx.chunkSizeBytes = LoadUnsigned_IOError(io, goto err);
    if (encver < TS_SIZE_RDB_VER) {
        cCtx.chunkSizeBytes *= SAMPLE_SIZE;
    }

    if (encver >= TS_UNCOMPRESSED_VER) {
        cCtx.options = LoadUnsigned_IOError(io, goto err);
    } else {
        cCtx.options |= SERIES_OPT_UNCOMPRESSED;
    }

    if (encver >= TS_SIZE_RDB_VER) {
        lastTimestamp = LoadUnsigned_IOError(io, goto err);
        lastValue = LoadDouble_IOError(io, goto err);
        totalSamples = LoadUnsigned_IOError(io, goto err);
        if (encver >= TS_IS_RESSETED_DUP_POLICY_RDB_VER) {
            duplicatePolicy = LoadUnsigned_IOError(io, goto err);
        }
        uint64_t hasSrcKey = LoadUnsigned_IOError(io, goto err);
        if (hasSrcKey) {
            srcKey = LoadString_IOError(io, goto err);
        }
    }

    cCtx.labelsCount = LoadUnsigned_IOError(io, goto err);
    cCtx.labels = calloc(cCtx.labelsCount, sizeof(Label));
    for (int i = 0; i < cCtx.labelsCount; i++) {
        cCtx.labels[i].key = LoadString_IOError(io, goto err);
        cCtx.labels[i].value = LoadString_IOError(io, goto err);
    }

    uint64_t rulesCount = LoadUnsigned_IOError(io, goto err);

    series = NewSeries(keyName, &cCtx);

    CompactionRule *lastRule = NULL;

    for (int i = 0; i < rulesCount; i++) {
        destKey = LoadString_IOError(io, goto err);
        uint64_t bucketDuration = LoadUnsigned_IOError(io, goto err);
        uint64_t timestampAlignment = 0;
        if (encver >= TS_ALIGNMENT_TS_VER) {
            timestampAlignment = LoadUnsigned_IOError(io, goto err);
        }
        uint64_t aggType = LoadUnsigned_IOError(io, goto err);
        timestamp_t startCurrentTimeBucket = LoadUnsigned_IOError(io, goto err);

        CompactionRule *rule = NewRule(destKey, aggType, bucketDuration, timestampAlignment);
        destKey = NULL;
        rule->startCurrentTimeBucket = startCurrentTimeBucket;

        if (i == 0) {
            series->rules = rule;
        } else {
            lastRule->nextRule = rule;
        }
        if (rule->aggClass->readContext(rule->aggContext, io, encver)) {
            goto err;
        }
        lastRule = rule;
    }

    if (encver < TS_SIZE_RDB_VER) {
        uint64_t samplesCount = LoadUnsigned_IOError(io, goto err);
        for (size_t sampleIndex = 0; sampleIndex < samplesCount; sampleIndex++) {
            timestamp_t ts = LoadUnsigned_IOError(io, goto err);
            double val = LoadDouble_IOError(io, goto err);
            int result = SeriesAddSample(series, ts, val);
            if (result != TSDB_OK) {
                RedisModule_LogIOError(
                    io, "warning", "couldn't load sample: %" PRIu64 " %lf", ts, val);
            }
        }
    } else {
        Chunk_t *chunk = NULL;
        // Free the default allocated chunk given LoadFromRDB will allocate a proper sized chunk
        timestamp_t rax_key = htonu64(0);
        chunk = (Chunk_t *)RedisModule_DictGetC(series->chunks, &rax_key, sizeof(rax_key), NULL);
        if (chunk != NULL) {
            series->funcs->FreeChunk(chunk);
        }
        dictOperator(series->chunks, NULL, 0, DICT_OP_DEL);
        uint64_t numChunks = LoadUnsigned_IOError(io, goto err);
        for (int i = 0; i < numChunks; ++i) {
            if (series->funcs->LoadFromRDB(&chunk, io)) {
                goto err;
            }
            dictOperator(
                series->chunks, chunk, series->funcs->GetFirstTimestamp(chunk), DICT_OP_SET);
        }
        series->totalSamples = totalSamples;
        series->duplicatePolicy = duplicatePolicy;
        series->srcKey = srcKey;
        srcKey = NULL;
        series->lastTimestamp = lastTimestamp;
        series->lastValue = lastValue;
        series->lastChunk = chunk;
    }

    return series;

err:
    if (destKey) { // clean if there is a key name which been alloced but not added to series yet
        RedisModule_FreeString(NULL, destKey);
    }
    if (srcKey) { // clean if there is a key name which been alloced but not added to series yet
        RedisModule_FreeString(NULL, srcKey);
    }
    if (series) {
        // Note that we aren't calling RemoveIndexedMetric(series->keyName) since
        // the series only being indexed on loaded notification
        FreeSeries(series);
    } else {
        if (keyName) {
            RedisModule_FreeString(NULL, keyName);
        }
        if (cCtx.labels) {
            for (int i = 0; i < cCtx.labelsCount; i++) {
                if (cCtx.labels[i].key) {
                    RedisModule_FreeString(NULL, cCtx.labels[i].key);
                }
                if (cCtx.labels[i].value) {
                    RedisModule_FreeString(NULL, cCtx.labels[i].value);
                }
            }
            free(cCtx.labels);
        }
    }

    return NULL;
}

unsigned int countRules(Series *series) {
    unsigned int count = 0;
    CompactionRule *rule = series->rules;
    while (rule != NULL) {
        count++;
        rule = rule->nextRule;
    }
    return count;
}

#define should_save_cross_references(series)                                                       \
    ((persistence_in_progress > 0) || (TSGlobalConfig.forceSaveCrossRef) || (!(series)->in_ram))

void series_rdb_save(RedisModuleIO *io, void *value) {
    Series *series = value;
    RedisModule_SaveString(io, series->keyName);
    RedisModule_SaveUnsigned(io, series->retentionTime);
    RedisModule_SaveUnsigned(io, series->chunkSizeBytes);
    RedisModule_SaveUnsigned(io, series->options);
    RedisModule_SaveUnsigned(io, series->lastTimestamp);
    RedisModule_SaveDouble(io, series->lastValue);
    RedisModule_SaveUnsigned(io, series->totalSamples);
    RedisModule_SaveUnsigned(io, series->duplicatePolicy);
    if ((series->srcKey != NULL) && (should_save_cross_references(series))) {
        // on dump command (restore) we don't keep the cross references
        RedisModule_SaveUnsigned(io, TRUE);
        RedisModule_SaveString(io, series->srcKey);
    } else {
        RedisModule_SaveUnsigned(io, FALSE);
    }

    RedisModule_SaveUnsigned(io, series->labelsCount);
    for (int i = 0; i < series->labelsCount; i++) {
        RedisModule_SaveString(io, series->labels[i].key);
        RedisModule_SaveString(io, series->labels[i].value);
    }

    if (should_save_cross_references(series)) {
        RedisModule_SaveUnsigned(io, countRules(series));

        CompactionRule *rule = series->rules;
        while (rule != NULL) {
            RedisModule_SaveString(io, rule->destKey);
            RedisModule_SaveUnsigned(io, rule->bucketDuration);
            RedisModule_SaveUnsigned(io, rule->timestampAlignment);
            RedisModule_SaveUnsigned(io, rule->aggType);
            RedisModule_SaveUnsigned(io, rule->startCurrentTimeBucket);
            rule->aggClass->writeContext(rule->aggContext, io);
            rule = rule->nextRule;
        }
    } else {
        // on dump command (restore) we don't keep the cross references
        RedisModule_SaveUnsigned(io, 0);
    }

    Chunk_t *chunk;
    uint64_t numChunks = RedisModule_DictSize(series->chunks);
    RedisModule_SaveUnsigned(io, numChunks);
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(series->chunks, "^", NULL, 0);
    while (RedisModule_DictNextC(iter, NULL, &chunk)) {
        series->funcs->SaveToRDB(chunk, io);
        numChunks--;
    }
    RedisModule_DictIteratorStop(iter);
}
