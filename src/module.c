/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/
#include <time.h>
#include <string.h>
#include <limits.h>
#include <ctype.h>

#include "redismodule.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"
#include "rmutil/alloc.h"

#include "tsdb.h"
#include "compaction.h"
#include "rdb.h"
#include "config.h"
#include "module.h"
#include "indexer.h"
#include "version.h"

RedisModuleType *SeriesType;
static time_t timer;

static int ReplySeriesRange(RedisModuleCtx *ctx, Series *series, api_timestamp_t start_ts, api_timestamp_t end_ts,
                     AggregationClass *aggObject, int64_t time_delta);

static void ReplyWithSeriesLabels(RedisModuleCtx *ctx, const Series *series);

static Label *parseLabelsFromArgs(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, size_t *label_count) {
    int pos = RMUtil_ArgIndex("LABELS", argv, argc);
    int first_label_pos = pos + 1;
    Label *labels = NULL;
    *label_count = 0;
    if (pos < 0) {
        return NULL;
    }
    *label_count = (size_t)(max(0, (argc - first_label_pos) / 2 ));
    if (label_count > 0) {
        labels = malloc(sizeof(Label) * (*label_count));
        for (int i=0; i < *label_count; i++) {
            labels[i].key = RedisModule_CreateStringFromString(NULL, argv[first_label_pos + i*2]);
            labels[i].value = RedisModule_CreateStringFromString(NULL, argv[first_label_pos + i*2 + 1]);
        };
    }
    return labels;
}

static int parseCreateArgs(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                    long long *retentionSecs, long long *maxSamplesPerChunk, size_t *labelsCount, Label **labels) {
    *retentionSecs = TSGlobalConfig.retentionPolicy;
    *maxSamplesPerChunk = TSGlobalConfig.maxSamplesPerChunk;
    *labelsCount = 0;
    *labels = parseLabelsFromArgs(ctx, argv, argc, labelsCount);

    if (RMUtil_ArgIndex("RETENTION", argv, argc) > 0 && RMUtil_ParseArgsAfter("RETENTION", argv, argc, "l", retentionSecs) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, "TSDB: Couldn't parse RETENTION");
        return REDISMODULE_ERR;
    }

    if (retentionSecs < 0) {
        RedisModule_ReplyWithError(ctx, "TSDB: Couldn't parse RETENTION");
        return REDISMODULE_ERR;
    }

    if (RMUtil_ArgIndex("CHUNK_SIZE", argv, argc) > 0 && RMUtil_ParseArgsAfter("CHUNK_SIZE", argv, argc, "l", maxSamplesPerChunk) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, "TSDB: Couldn't parse CHUNK_SIZE");
        return REDISMODULE_ERR;
    }

    if (maxSamplesPerChunk <= 0) {
        RedisModule_ReplyWithError(ctx, "TSDB: Couldn't parse CHUNK_SIZE");
        return REDISMODULE_ERR;
    }
    return REDISMODULE_OK;
}

static int _parseAggregationArgs(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, api_timestamp_t *time_delta,
                         int *agg_type) {
    RedisModuleString * aggTypeStr = NULL;
    int offset = RMUtil_ArgIndex("AGGREGATION", argv, argc);
    if (offset > 0) {
        if (RMUtil_ParseArgs(argv, argc, offset + 1, "sl", &aggTypeStr, time_delta) != REDISMODULE_OK) {
            RedisModule_ReplyWithError(ctx, "TSDB: Couldn't parse AGGREGATION");
            return TSDB_ERROR;
        }

        if (!aggTypeStr){
            RedisModule_ReplyWithError(ctx, "TSDB: Unknown aggregation type");
            return TSDB_ERROR;
        }

        *agg_type = RMStringLenAggTypeToEnum(aggTypeStr);

        if (*agg_type < 0 || *agg_type >= TS_AGG_TYPES_MAX) {
            RedisModule_ReplyWithError(ctx, "TSDB: Unknown aggregation type");
            return TSDB_ERROR;
        }

        if (*time_delta <= 0) {
            RedisModule_ReplyWithError(ctx, "TSDB: bucketSizeSeconds must be greater than zero");
            return TSDB_ERROR;
        }

        return TSDB_OK;
    }

    return TSDB_NOTEXISTS;

}

static int parseAggregationArgs(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, api_timestamp_t *time_delta,
                         AggregationClass **agg_object) {
    int agg_type;
    int result = _parseAggregationArgs(ctx, argv, argc, time_delta, &agg_type);
    if (result == TSDB_OK) {
        *agg_object = GetAggClass(agg_type);
        if (*agg_object == NULL) {
            RedisModule_ReplyWithError(ctx, "TSDB: Failed to retrieve aggregation class");
            return TSDB_ERROR;
        }
        return TSDB_OK;
    } else {
        return result;
    }
}

static int parseRangeArguments(RedisModuleCtx *ctx, Series *series, int start_index, RedisModuleString **argv,
        api_timestamp_t *start_ts, api_timestamp_t *end_ts) {
    size_t start_len;
    const char *start = RedisModule_StringPtrLen(argv[start_index], &start_len);
    if (strcmp(start, "-") == 0) {
        *start_ts = 0;
    } else {
        if (RedisModule_StringToLongLong(argv[start_index], (long long int *) start_ts) != REDISMODULE_OK) {
            RedisModule_ReplyWithError(ctx, "TSDB: wrong fromTimestamp");
            return REDISMODULE_ERR;
        }
    }

    size_t end_len;
    const char *end = RedisModule_StringPtrLen(argv[start_index + 1], &end_len);
    if (strcmp(end, "+") == 0) {
        *end_ts = series->lastTimestamp;
    } else {
        if (RedisModule_StringToLongLong(argv[start_index + 1], (long long int *) end_ts) != REDISMODULE_OK) {
            RedisModule_ReplyWithError(ctx, "TSDB: wrong toTimestamp");
            return REDISMODULE_ERR;
        }
    }

    return REDISMODULE_OK;
}

int TSDB_info(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    
    if (argc != 2) return RedisModule_WrongArity(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    Series *series;
    
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY){
        return RedisModule_ReplyWithError(ctx, "TSDB: key does not exist");
    } else if (RedisModule_ModuleTypeGetType(key) != SeriesType){
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    } else {
        series = RedisModule_ModuleTypeGetValue(key);
    }

    RedisModule_ReplyWithArray(ctx, 6*2);

    RedisModule_ReplyWithSimpleString(ctx, "lastTimestamp");
    RedisModule_ReplyWithLongLong(ctx, series->lastTimestamp);
    RedisModule_ReplyWithSimpleString(ctx, "retentionSecs");
    RedisModule_ReplyWithLongLong(ctx, series->retentionSecs);
    RedisModule_ReplyWithSimpleString(ctx, "chunkCount");
    RedisModule_ReplyWithLongLong(ctx, RedisModule_DictSize(series->chunks));
    RedisModule_ReplyWithSimpleString(ctx, "maxSamplesPerChunk");
    RedisModule_ReplyWithLongLong(ctx, series->maxSamplesPerChunk);

    RedisModule_ReplyWithSimpleString(ctx, "labels");
    ReplyWithSeriesLabels(ctx, series);

    RedisModule_ReplyWithSimpleString(ctx, "rules");
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    CompactionRule *rule = series->rules;
    int ruleCount = 0;
    while (rule != NULL) {
        RedisModule_ReplyWithArray(ctx, 3);
        RedisModule_ReplyWithString(ctx, rule->destKey);
        RedisModule_ReplyWithLongLong(ctx, rule->bucketSizeSec);
        RedisModule_ReplyWithSimpleString(ctx, AggTypeEnumToString(rule->aggType));
        
        rule = rule->nextRule;
        ruleCount++;
    }
    RedisModule_ReplySetArrayLength(ctx, ruleCount);
    RedisModule_CloseKey(key);

    return REDISMODULE_OK;
}

void ReplyWithSeriesLabels(RedisModuleCtx *ctx, const Series *series) {
    RedisModule_ReplyWithArray(ctx, series->labelsCount);
    for (int i=0; i < series->labelsCount; i++) {
        RedisModule_ReplyWithArray(ctx, 2);
        RedisModule_ReplyWithString(ctx, series->labels[i].key);
        RedisModule_ReplyWithString(ctx, series->labels[i].value);
    }
}

void ReplyWithAggValue(RedisModuleCtx *ctx, timestamp_t last_agg_timestamp, AggregationClass *aggObject, void *context) {
    RedisModule_ReplyWithArray(ctx, 2);

    RedisModule_ReplyWithLongLong(ctx, last_agg_timestamp);
    RedisModule_ReplyWithDouble(ctx, aggObject->finalize(context));

    aggObject->resetContext(context);
}

int parseLabelListFromArgs(RedisModuleCtx *ctx, RedisModuleString **argv, int start, int query_count,
        QueryPredicate *queries) {
    QueryPredicate *query = queries;
    for (int i=start; i < start + query_count; i++) {
        size_t _s;
        const char *str2 = RedisModule_StringPtrLen(argv[i], &_s);
        if (strstr(str2, "!=") != NULL) {
            query->type = NEQ;
            if (parseLabel(ctx, argv[i], &query->label, "!=") == TSDB_ERROR) {
                return TSDB_ERROR;
            }
            if (query->label.value == NULL) {
                query->type = CONTAINS;
            }
        } else if (strstr(str2, "=") != NULL) {
            query->type = EQ;
            if (parseLabel(ctx, argv[i], &query->label, "=") == TSDB_ERROR) {
                return TSDB_ERROR;
            }
            if (query->label.value == NULL) {
                query->type = NCONTAINS;
            }
        } else {
            return TSDB_ERROR;
        }
        query++;
    }
    return TSDB_OK;
}

int TSDB_queryindex(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    int query_count = argc - 1;

    QueryPredicate *queries = RedisModule_PoolAlloc(ctx, sizeof(QueryPredicate) * query_count);
    if (parseLabelListFromArgs(ctx, argv, 1, query_count, queries) == TSDB_ERROR) {
        return RedisModule_ReplyWithError(ctx, "TSDB: failed parsing labels");
    }

    if (CountPredicateType(queries, (size_t) query_count, EQ) == 0) {
        return RedisModule_ReplyWithError(ctx, "TSDB: please provide at least one matcher");
    }

    RedisModuleDict *result = QueryIndex(ctx, queries, query_count);

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(result, "^", NULL, 0);
    char *currentKey;
    size_t currentKeyLen;
    long long replylen = 0;
    while((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        RedisModule_ReplyWithStringBuffer(ctx, currentKey, currentKeyLen);
        replylen++;
    }
    RedisModule_DictIteratorStop(iter);
    RedisModule_ReplySetArrayLength(ctx, replylen);

    return REDISMODULE_OK;
}

int TSDB_mrange(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    api_timestamp_t start_ts, end_ts;
    api_timestamp_t time_delta = 0;

    if (argc < 4)
        return RedisModule_WrongArity(ctx);
    Series fake_series = {0};
    fake_series.lastTimestamp = LLONG_MAX;
    if (parseRangeArguments(ctx, &fake_series, 1, argv, &start_ts, &end_ts) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    AggregationClass *aggObject = NULL;

    int aggregationResult = parseAggregationArgs(ctx, argv, argc, &time_delta, &aggObject);
    if (aggregationResult == TSDB_ERROR) {
        return REDISMODULE_ERR;
    }

    int filter_location = RMUtil_ArgIndex("FILTER", argv, argc);
    if (filter_location == -1) {
        return RedisModule_WrongArity(ctx);
    }

    size_t query_count = argc - 1 - filter_location;
    QueryPredicate *queries = RedisModule_PoolAlloc(ctx, sizeof(QueryPredicate) * query_count);
    if (parseLabelListFromArgs(ctx, argv, filter_location + 1, query_count, queries) == TSDB_ERROR) {
        return RedisModule_ReplyWithError(ctx, "TSDB: failed parsing labels");
    }

    if (CountPredicateType(queries, (size_t) query_count, EQ) == 0) {
        return RedisModule_ReplyWithError(ctx, "TSDB: please provide at least one matcher");
    }

    RedisModuleDict *result = QueryIndex(ctx, queries, query_count);

    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(result, "^", NULL, 0);
    char *currentKey;
    size_t currentKeyLen;
    long long replylen = 0;
    Series *series;
    while((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        RedisModuleKey *key = RedisModule_OpenKey(ctx, RedisModule_CreateString(ctx, currentKey, currentKeyLen),
                REDISMODULE_READ);
        if (key == NULL || RedisModule_ModuleTypeGetType(key) != SeriesType){
            RedisModule_Log(ctx, "warning", "couldn't open key or key is not a Timeseries. key=%s", currentKey);
            continue;
        }
        series = RedisModule_ModuleTypeGetValue(key);
        RedisModule_ReplyWithArray(ctx, 3);
        RedisModule_ReplyWithStringBuffer(ctx, currentKey, currentKeyLen);
        ReplyWithSeriesLabels(ctx, series);
        ReplySeriesRange(ctx, series, start_ts, end_ts, aggObject, time_delta);
        replylen++;
    }
    RedisModule_DictIteratorStop(iter);
    RedisModule_ReplySetArrayLength(ctx, replylen);

    return REDISMODULE_OK;
}

int TSDB_range(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    Series *series;
    RedisModuleKey *key;
    key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);

    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY){
        return RedisModule_ReplyWithError(ctx, "TSDB: key does not exist");
    } else if (RedisModule_ModuleTypeGetType(key) != SeriesType){
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    } else {
        series = RedisModule_ModuleTypeGetValue(key);
    }

    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    api_timestamp_t start_ts, end_ts;
    api_timestamp_t time_delta = 0;

    if (parseRangeArguments(ctx, series, 2, argv, &start_ts, &end_ts) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    AggregationClass *aggObject = NULL;
    int aggregationResult = parseAggregationArgs(ctx, argv, argc, &time_delta, &aggObject);
    if (aggregationResult == TSDB_ERROR) {
        return REDISMODULE_ERR;
    }

    ReplySeriesRange(ctx, series, start_ts, end_ts, aggObject, time_delta);
    return REDISMODULE_OK;

}

int ReplySeriesRange(RedisModuleCtx *ctx, Series *series, api_timestamp_t start_ts, api_timestamp_t end_ts,
        AggregationClass *aggObject, int64_t time_delta) {
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    long long arraylen = 0;
    SeriesIterator iterator = SeriesQuery(series, start_ts, end_ts);
    Sample sample;
    void *context = NULL;
    if (aggObject != NULL)
        context = aggObject->createContext();
    timestamp_t last_agg_timestamp = 0;
    while (SeriesIteratorGetNext(&iterator, &sample) != 0 ) {
        if (aggObject == NULL) { // No aggregation whats so ever
            RedisModule_ReplyWithArray(ctx, 2);

            RedisModule_ReplyWithLongLong(ctx, sample.timestamp);
            RedisModule_ReplyWithDouble(ctx, sample.data);
            arraylen++;
        } else {
            timestamp_t current_timestamp = sample.timestamp - (sample.timestamp % time_delta);
            if (current_timestamp > last_agg_timestamp) {
                if (last_agg_timestamp != 0) {
                    ReplyWithAggValue(ctx, last_agg_timestamp, aggObject, context);
                    arraylen++;
                }

                last_agg_timestamp = current_timestamp;
            }
            aggObject->appendValue(context, sample.data);
        }
    }

    if (aggObject != AGG_NONE) {
        // reply last bucket of data
        ReplyWithAggValue(ctx, last_agg_timestamp, aggObject, context);
        arraylen++;
    }

    RedisModule_ReplySetArrayLength(ctx,arraylen);
    return REDISMODULE_OK;
}

void handleCompaction(RedisModuleCtx *ctx, CompactionRule *rule, api_timestamp_t timestamp, double value) {
    RedisModuleKey *key = RedisModule_OpenKey(ctx, rule->destKey, REDISMODULE_READ|REDISMODULE_WRITE);
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY){
        // key doesn't exist anymore and we don't do anything
        return;
    }
    Series *destSeries = RedisModule_ModuleTypeGetValue(key);

    timestamp_t currentTimestamp = timestamp - timestamp % rule->bucketSizeSec;
    if (currentTimestamp > destSeries->lastTimestamp) {
        rule->aggClass->resetContext(rule->aggContext);
    }
    rule->aggClass->appendValue(rule->aggContext, value);
    SeriesAddSample(destSeries, currentTimestamp, rule->aggClass->finalize(rule->aggContext));
    RedisModule_CloseKey(key);
}

int TSDB_add(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    
    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleString *keyName = argv[1];
    RedisModuleString *timestampStr = argv[2];
    RedisModuleString *valueStr = argv[3];
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ|REDISMODULE_WRITE);

    double value;
    api_timestamp_t timestamp;
    if ((RedisModule_StringToDouble(valueStr, &value) != REDISMODULE_OK))
        return RedisModule_ReplyWithError(ctx, "TSDB: invalid value");

    if ((RedisModule_StringToLongLong(timestampStr, (long long int *) &timestamp) != REDISMODULE_OK)) {
        // if timestamp is "*", take current time (automatic timestamp)
        if(RMUtil_StringEqualsC(timestampStr, "*"))
            timestamp = (u_int64_t) time(NULL);
        else
            return RedisModule_ReplyWithError(ctx, "TSDB: invalid timestamp");
    }

    Series *series = NULL;
    
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        // the key doesn't exist, lets check we have enough information to create one
        long long retentionSecs;
        long long maxSamplesPerChunk;
        size_t labelsCount;
        Label *labels;
        if (parseCreateArgs(ctx, argv, argc, &retentionSecs, &maxSamplesPerChunk, &labelsCount, &labels) != REDISMODULE_OK) {
            return REDISMODULE_ERR;
        }

        CreateTsKey(ctx, keyName, labels, labelsCount, retentionSecs, maxSamplesPerChunk, &series, &key);
        SeriesCreateRulesFromGlobalConfig(ctx, keyName, series, labels, labelsCount);
    } else if (RedisModule_ModuleTypeGetType(key) != SeriesType){
        return RedisModule_ReplyWithError(ctx, "TSDB: the key is not a TSDB key");
    } else {
        series = RedisModule_ModuleTypeGetValue(key);
    }

    int retval = SeriesAddSample(series, timestamp, value);
    int result = 0;
    if (retval == TSDB_ERR_TIMESTAMP_TOO_OLD) {
        RedisModule_ReplyWithError(ctx, "TSDB: timestamp is too old");
        result = REDISMODULE_ERR;
    } else if (retval != TSDB_OK) {
        RedisModule_ReplyWithError(ctx, "TSDB: Unknown Error");
        result = REDISMODULE_ERR;
    } else {
        // handle compaction rules
        CompactionRule *rule = series->rules;
        while (rule != NULL) {
            handleCompaction(ctx, rule, timestamp, value);
            rule = rule->nextRule;
        }
        
        RedisModule_ReplyWithSimpleString(ctx, "OK");
        RedisModule_ReplicateVerbatim(ctx);
        result = REDISMODULE_OK;
    }
    RedisModule_CloseKey(key);
    return result;
}

int CreateTsKey(RedisModuleCtx *ctx, RedisModuleString *keyName, Label *labels, size_t labelsCounts, long long retentionSecs,
                long long maxSamplesPerChunk, Series **series, RedisModuleKey **key) {
    if (*key == NULL) {
        *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ|REDISMODULE_WRITE);
    }

    RedisModule_RetainString(ctx, keyName);
    *series = NewSeries(keyName, labels, labelsCounts, retentionSecs, maxSamplesPerChunk);
    if (RedisModule_ModuleTypeSetValue(*key, SeriesType, *series) == REDISMODULE_ERR) {
        return TSDB_ERROR;
    }

    IndexMetric(ctx, keyName, (*series)->labels, (*series)->labelsCount);

    return TSDB_OK;
}

int TSDB_create(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 2)
        return RedisModule_WrongArity(ctx);

    Series *series;
    RedisModuleString *keyName = argv[1];
    long long retentionSecs;
    long long maxSamplesPerChunk;
    size_t labelsCount;
    Label *labels;

    if (parseCreateArgs(ctx, argv, argc, &retentionSecs, &maxSamplesPerChunk, &labelsCount, &labels) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ|REDISMODULE_WRITE);

    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx,"TSDB: key already exists");
    }

    CreateTsKey(ctx, keyName, labels, labelsCount, retentionSecs, maxSamplesPerChunk, &series, &key);
    RedisModule_CloseKey(key);

    RedisModule_Log(ctx, "info", "created new series");
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

int TSDB_alter(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    if (argc < 2)
        return RedisModule_WrongArity(ctx);

    Series *series;
    RedisModuleString *keyName = argv[1];
    long long retentionSecs;
    long long maxSamplesPerChunk;
    size_t labelsCount;
    Label *newLabels;

    if (parseCreateArgs(ctx, argv, argc, &retentionSecs, &maxSamplesPerChunk, &labelsCount, &newLabels) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ|REDISMODULE_WRITE);
    if (RedisModule_ModuleTypeGetType(key) != SeriesType) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    series = RedisModule_ModuleTypeGetValue(key);
    if (RMUtil_ArgIndex("RETENTION", argv, argc) > 0) {
        series->retentionSecs = retentionSecs;
    }

    if (RMUtil_ArgIndex("CHUNK_SIZE", argv, argc) > 0) {
        series->maxSamplesPerChunk = maxSamplesPerChunk;
    }

    if (RMUtil_ArgIndex("LABELS", argv, argc) > 0) {
        RemoveIndexedMetric(ctx, keyName, series->labels, series->labelsCount);
        // free current labels
        if (series->labelsCount > 0) {
            for (int i = 0; i < series->labelsCount; i++) {
                RedisModule_FreeString(ctx, series->labels[i].key);
                RedisModule_FreeString(ctx, series->labels[i].value);
            }
            free(series->labels);
        }

        // set new newLabels
        series->labels = newLabels;
        series->labelsCount = labelsCount;
        IndexMetric(ctx, keyName, series->labels, series->labelsCount);
    }

    RedisModule_CloseKey(key);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

//TS.DELETERULE SOURCE_KEY DEST_KEY
int TSDB_deleteRule(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3)
        return RedisModule_WrongArity(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithError(ctx, "TSDB: the key does not exist");
    } else if (RedisModule_ModuleTypeGetType(key) != SeriesType) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    Series *series = RedisModule_ModuleTypeGetValue(key);

    RedisModuleString *destKey = argv[2];
    if (SeriesHasRule(series, destKey)) {
        CompactionRule *rule = series->rules;
        CompactionRule *prev_rule = NULL;
        while (rule != NULL) {
            if (RMUtil_StringEquals(rule->destKey, destKey)) {
                if (prev_rule == NULL) {
                    series->rules = rule->nextRule;
                } else {
                    prev_rule->nextRule = rule->nextRule;
                }
            }

            prev_rule = rule;
            rule = rule->nextRule;
        }
    } else {
        return RedisModule_ReplyWithError(ctx, "TSDB: compaction rule does not exist");
    }

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}

/*
TS.CREATERULE sourceKey destKey AGGREGATION aggregationType bucketSizeSeconds
*/
int TSDB_createRule(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 6)
        return RedisModule_WrongArity(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithError(ctx, "TSDB: the key does not exist");
    }
    api_timestamp_t bucketSize;
    int aggType;
    int result = _parseAggregationArgs(ctx, argv, argc, &bucketSize, &aggType);
    if (result == TSDB_NOTEXISTS) {
        return RedisModule_WrongArity(ctx);
    } else if (result == TSDB_ERROR) {
        return REDISMODULE_ERR;
    }

    RedisModuleString *destKeyName = argv[2];
    RedisModuleKey *destKey = RedisModule_OpenKey(ctx, destKeyName, REDISMODULE_READ);
    if (RedisModule_KeyType(destKey) == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithError(ctx, "TSDB: the destination key does not exist");
    } else if (RedisModule_ModuleTypeGetType(key) != SeriesType) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    Series *series = RedisModule_ModuleTypeGetValue(key);
    if (SeriesHasRule(series, destKeyName)) {
        return RedisModule_ReplyWithError(ctx, "TSDB: the destination key already has a rule");
    }

    RedisModuleString *destKeyStr = RedisModule_CreateStringFromString(ctx, destKeyName);
    if (SeriesAddRule(series, destKeyStr, aggType, bucketSize) != NULL) {
        RedisModule_RetainString(ctx, destKeyStr);
    } else {
        RedisModule_ReplyWithSimpleString(ctx, "ERROR creating rule");
        return REDISMODULE_ERR;
    }

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_CloseKey(destKey);
    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}


/*
TS.INCRBY ts_key NUMBER [RESET time-bucket]
*/
int TSDB_incrby(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 3)
        return RedisModule_WrongArity(ctx);

    RedisModuleString *keyName = argv[1];
    Series *series;

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        // the key doesn't exist, lets check we have enough information to create one
        long long retentionSecs;
        long long maxSamplesPerChunk;
        size_t labelsCount;
        Label *labels;
        if (parseCreateArgs(ctx, argv, argc, &retentionSecs, &maxSamplesPerChunk, &labelsCount, &labels) != REDISMODULE_OK) {
            return REDISMODULE_ERR;
        }

        CreateTsKey(ctx, keyName, labels, labelsCount, retentionSecs, maxSamplesPerChunk, &series, &key);
        SeriesCreateRulesFromGlobalConfig(ctx, keyName, series, labels, labelsCount);
    }

    series = RedisModule_ModuleTypeGetValue(key);
    double incrby = 0;
    if (RMUtil_ParseArgs(argv, argc, 2, "d", &incrby) != REDISMODULE_OK)
        return RedisModule_WrongArity(ctx);
    time(&timer);

    double result;
    long long resetSeconds = 1;
    time_t currentUpdatedTime = timer;
    if (RMUtil_ArgIndex("RESET", argv, argc) > 0) {
        if (RMUtil_ParseArgsAfter("RESET", argv, argc, "l", &resetSeconds) != 0) {
            return RedisModule_WrongArity(ctx);
        }

        currentUpdatedTime = timer - ((int)timer % resetSeconds);
        if (series->lastTimestamp != 0) {
            u_int64_t lastTS = series->lastTimestamp;
            if (lastTS - (lastTS % resetSeconds) !=  currentUpdatedTime) {
                series->lastValue = 0;
            }
        }
    }

    RMUtil_StringToLower(argv[0]);
    if (RMUtil_StringEqualsC(argv[0], "ts.incrby")) {
        result = series->lastValue + incrby;
    } else {
        result = series->lastValue - incrby;
    }

    if (SeriesAddSample(series, max(currentUpdatedTime, series->lastTimestamp), result) != TSDB_OK) {
        RedisModule_ReplyWithSimpleString(ctx, "TSDB: couldn't add sample");
        return REDISMODULE_OK;
    }

    // handle compaction rules
    CompactionRule *rule = series->rules;
    while (rule != NULL) {
        handleCompaction(ctx, rule, currentUpdatedTime, result);
        rule = rule->nextRule;
    }

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}


int TSDB_get(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 2) return RedisModule_WrongArity(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    Series *series;

    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithError(ctx, "TSDB: key does not exist");
    } else if (RedisModule_ModuleTypeGetType(key) != SeriesType) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    } else {
        series = RedisModule_ModuleTypeGetValue(key);
    }
    RedisModule_ReplyWithArray(ctx, 2);
    RedisModule_ReplyWithLongLong(ctx, series->lastTimestamp);
    RedisModule_ReplyWithDouble(ctx, series->lastValue);

    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}

int TSDB_mget(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    int filter_location = RMUtil_ArgIndex("FILTER", argv, argc);
    if (filter_location == -1) {
        return RedisModule_WrongArity(ctx);
    }
    size_t query_count = argc - 1 - filter_location;
    QueryPredicate *queries = RedisModule_PoolAlloc(ctx, sizeof(QueryPredicate) * query_count);
    if (parseLabelListFromArgs(ctx, argv, filter_location + 1, query_count, queries) == TSDB_ERROR) {
        return RedisModule_ReplyWithError(ctx, "TSDB: failed parsing labels");
    }

    if (CountPredicateType(queries, (size_t) query_count, EQ) == 0) {
        return RedisModule_ReplyWithError(ctx, "TSDB: please provide at least one matcher");
    }

    RedisModuleDict *result = QueryIndex(ctx, queries, query_count);
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(result, "^", NULL, 0);
    char *currentKey;
    size_t currentKeyLen;
    long long replylen = 0;
    Series *series;
    while((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        RedisModuleKey *key = RedisModule_OpenKey(ctx, RedisModule_CreateString(ctx, currentKey, currentKeyLen),
                REDISMODULE_READ);
        if (key == NULL || RedisModule_ModuleTypeGetType(key) != SeriesType){
            RedisModule_Log(ctx, "warning", "couldn't open key or key is not a Timeseries. key=%s", currentKey);
                continue;
            }
        series = RedisModule_ModuleTypeGetValue(key);

        RedisModule_ReplyWithArray(ctx, 4);
        RedisModule_ReplyWithStringBuffer(ctx, currentKey, currentKeyLen);
        ReplyWithSeriesLabels(ctx, series);
        RedisModule_ReplyWithLongLong(ctx, series->lastTimestamp);
        RedisModule_ReplyWithDouble(ctx, series->lastValue);
        replylen++;
        RedisModule_CloseKey(key);
    }
    RedisModule_DictIteratorStop(iter);
    RedisModule_ReplySetArrayLength(ctx, replylen);

    return REDISMODULE_OK;
}

/*
module loading function, possible arguments:
COMPACTION_POLICY - compaction policy from parse_policies,h
RETENTION_POLICY - integer that represents the retention in seconds
MAX_SAMPLE_PER_CHUNK - how many samples per chunk
example:
redis-server --loadmodule ./redistimeseries.so COMPACTION_POLICY "max:1m:1d;min:10s:1h;avg:2h:10d;avg:3d:100d" RETENTION_POLICY 3600 MAX_SAMPLE_PER_CHUNK 1024
*/
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, "timeseries", REDISTIMESERIES_MODULE_VERSION, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (ReadConfig(argv, argc) == TSDB_ERROR) {
        return REDISMODULE_ERR;
    }

    RedisModuleTypeMethods tm = {
            .version = REDISMODULE_TYPE_METHOD_VERSION,
            .rdb_load = series_rdb_load,
            .rdb_save = series_rdb_save,
            .aof_rewrite = RMUtil_DefaultAofRewrite,
            .mem_usage = SeriesMemUsage,
            .free = FreeSeries
        };

    SeriesType = RedisModule_CreateDataType(ctx, "TSDB-TYPE", TS_ENC_VER, &tm);
    if (SeriesType == NULL) return REDISMODULE_ERR;
    IndexInit();
    RMUtil_RegisterWriteCmd(ctx, "ts.create", TSDB_create);
    RMUtil_RegisterWriteCmd(ctx, "ts.alter", TSDB_alter);
    RMUtil_RegisterWriteCmd(ctx, "ts.createrule", TSDB_createRule);
    RMUtil_RegisterWriteCmd(ctx, "ts.deleterule", TSDB_deleteRule);
    RMUtil_RegisterWriteCmd(ctx, "ts.add", TSDB_add);
    RMUtil_RegisterWriteCmd(ctx, "ts.incrby", TSDB_incrby);
    RMUtil_RegisterWriteCmd(ctx, "ts.decrby", TSDB_incrby);
    RMUtil_RegisterReadCmd(ctx, "ts.range", TSDB_range);
    RMUtil_RegisterReadCmd(ctx, "ts.mrange", TSDB_mrange);
    RMUtil_RegisterReadCmd(ctx, "ts.queryindex", TSDB_queryindex);
    RMUtil_RegisterReadCmd(ctx, "ts.info", TSDB_info);
    RMUtil_RegisterReadCmd(ctx, "ts.get", TSDB_get);
    RMUtil_RegisterReadCmd(ctx, "ts.mget", TSDB_mget);

    return REDISMODULE_OK;
}
