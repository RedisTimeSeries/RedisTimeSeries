/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "rdb.h"

#include "consts.h"
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

    bool err = false;
    Series *series = NULL;

    RedisModuleString *keyName = LoadString_IOError(io, err, NULL);
    errdefer(err, if (!series) RedisModule_FreeString(NULL, keyName));

    CreateCtx cCtx = { 0 };
    cCtx.retentionTime = LoadUnsigned_IOError(io, err, NULL);
    cCtx.chunkSizeBytes = LoadUnsigned_IOError(io, err, NULL);
    if (encver < TS_SIZE_RDB_VER) {
        cCtx.chunkSizeBytes *= SAMPLE_SIZE;
    }
    cCtx.options = Load_IOError_OrDefault(
        io, err, NULL, encver >= TS_UNCOMPRESSED_VER, SERIES_OPT_UNCOMPRESSED);

    const timestamp_t lastTimestamp =
        Load_IOError_OrDefault(io, err, NULL, encver >= TS_SIZE_RDB_VER, 0);
    const double lastValue = Load_IOError_OrDefault(io, err, NULL, encver >= TS_SIZE_RDB_VER, 0.0);
    const uint64_t totalSamples =
        Load_IOError_OrDefault(io, err, NULL, encver >= TS_SIZE_RDB_VER, 0);
    const DuplicatePolicy duplicatePolicy =
        Load_IOError_OrDefault(io, err, NULL, encver >= TS_IS_RESSETED_DUP_POLICY_RDB_VER, DP_NONE);

    const bool hasSrcKey = Load_IOError_OrDefault(io, err, NULL, encver >= TS_SIZE_RDB_VER, false);
    RedisModuleString *srcKey = Load_IOError_OrDefault(io, err, NULL, hasSrcKey, NULL);
    // clean if there is a key name which been alloced but not added to series yet
    errdefer(err, if (srcKey) RedisModule_FreeString(NULL, srcKey));

    const long long ignoreMaxTimeDiff =
        Load_IOError_OrDefault(io, err, NULL, encver >= TS_CREATE_IGNORE_VER, 0);
    const double ignoreMaxValDiff =
        Load_IOError_OrDefault(io, err, NULL, encver >= TS_CREATE_IGNORE_VER, 0.0);

    cCtx.labelsCount = LoadUnsigned_IOError(io, err, NULL);
    cCtx.labels = calloc(cCtx.labelsCount, sizeof *cCtx.labels);
    errdefer(err, if (!series) FreeLabels(cCtx.labels, cCtx.labelsCount));
    for (int i = 0; i < cCtx.labelsCount; i++) {
        cCtx.labels[i].key = LoadString_IOError(io, err, NULL);
        cCtx.labels[i].value = LoadString_IOError(io, err, NULL);
    }

    series = NewSeries(keyName, &cCtx);
    // Note that we aren't calling RemoveIndexedMetric(series->keyName) since
    // the series only being indexed on loaded notification
    errdefer(err, FreeSeries(series));

    const uint64_t rulesCount = LoadUnsigned_IOError(io, err, NULL);
    for (int i = 0; i < rulesCount; i++) {
        RedisModuleString *destKey = LoadString_IOError(io, err, NULL);
        // clean if there is a key name which been alloced but not added to series yet
        errdefer(err, if (destKey) RedisModule_FreeString(NULL, destKey));

        const uint64_t bucketDuration = LoadUnsigned_IOError(io, err, NULL);
        const uint64_t timestampAlignment =
            Load_IOError_OrDefault(io, err, NULL, encver >= TS_ALIGNMENT_TS_VER, 0);
        const uint64_t aggType = LoadUnsigned_IOError(io, err, NULL);
        const timestamp_t startCurrentTimeBucket = LoadUnsigned_IOError(io, err, NULL);

        CompactionRule *rule = NewRule(destKey, aggType, bucketDuration, timestampAlignment);
        destKey = NULL;

        rule->startCurrentTimeBucket = startCurrentTimeBucket;
        rule->nextRule = series->rules;
        series->rules = rule;

        if (rule->aggClass->readContext(rule->aggContext, io, encver)) {
            err = true;
            return NULL;
        }
    }

    if (encver < TS_SIZE_RDB_VER) {
        const uint64_t samplesCount = LoadUnsigned_IOError(io, err, NULL);
        for (size_t sampleIndex = 0; sampleIndex < samplesCount; sampleIndex++) {
            const timestamp_t ts = LoadUnsigned_IOError(io, err, NULL);
            const double val = LoadDouble_IOError(io, err, NULL);
            SeriesAddSample(series, ts, val);
        }
    } else {
        // Free the default allocated chunk given LoadFromRDB will allocate a proper sized chunk
        timestamp_t rax_key = 0;
        Chunk_t *chunk = RedisModule_DictGetC(series->chunks, &rax_key, sizeof(rax_key), NULL);
        if (chunk != NULL) {
            series->funcs->FreeChunk(chunk);
        }
        dictOperator(series->chunks, NULL, 0, DICT_OP_DEL);
        const uint64_t numChunks = LoadUnsigned_IOError(io, err, NULL);
        for (int i = 0; i < numChunks; ++i) {
            if (series->funcs->LoadFromRDB(&chunk, io)) {
                err = true;
                return NULL;
            }
            dictOperator(
                series->chunks, chunk, series->funcs->GetFirstTimestamp(chunk), DICT_OP_SET);
        }
        series->totalSamples = totalSamples;
        series->duplicatePolicy = duplicatePolicy;
        series->srcKey = srcKey;
        series->lastTimestamp = lastTimestamp;
        series->lastValue = lastValue;
        series->lastChunk = chunk;
        series->ignoreMaxTimeDiff = ignoreMaxTimeDiff;
        series->ignoreMaxValDiff = ignoreMaxValDiff;
    }

    return series;
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
        RedisModule_SaveUnsigned(io, true);
        RedisModule_SaveString(io, series->srcKey);
    } else {
        RedisModule_SaveUnsigned(io, false);
    }

    RedisModule_SaveUnsigned(io, series->ignoreMaxTimeDiff);
    RedisModule_SaveDouble(io, series->ignoreMaxValDiff);

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
