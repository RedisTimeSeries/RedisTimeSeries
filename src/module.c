
/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/
#include <time.h>
#include <string.h>
#include <limits.h>
#include <ctype.h>
#include <strings.h>

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
#include "search.h"

RedisModuleType *SeriesType;

static int ReplySeriesRange(RedisModuleCtx *ctx, Series *series, api_timestamp_t start_ts, api_timestamp_t end_ts,
                     AggregationClass *aggObject, int64_t time_delta, long long maxResults);

static void ReplyWithSeriesLabels(RedisModuleCtx *ctx, const Series *series);

static int parseRSLabelsFromArgs(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, size_t *label_count, RSLabel **rsLabels) {
    int pos = RMUtil_ArgIndex("LABELS", argv, argc);
    if (pos < 0) {
        *rsLabels = NULL;
        return REDISMODULE_OK;
    }

    int first_label_pos = pos + 1;
    if ((argc - pos) % 2 == 0) { return REDISMODULE_ERR; }

    RSLabel *labelsResult = NULL;
    *label_count = (size_t)(max(0, (argc - first_label_pos) / 2 ));
    if (label_count > 0) {
    	labelsResult = calloc(*label_count, sizeof(RSLabel));
        for (int i = 0; i < *label_count; i++) {
        	size_t keyLen = 0;
            size_t valueLen = 0;
        	RedisModuleString *key = argv[first_label_pos + i * 2];
        	RedisModuleString *value = argv[first_label_pos + i * 2 + 1];

        	// Processing Label Key into Field
        	const char *fieldStr = RedisModule_StringPtrLen(key, &keyLen);
        	if(fieldStr == NULL || keyLen == 0) { 
                goto exitOnError;
            }

        	const char *valueStr = RedisModule_StringPtrLen(value, &valueLen);
        	if(valueStr == NULL || valueLen == 0 || strpbrk(valueStr, "=(),")) {
        		goto exitOnError;
        	}
            labelsResult[i].RSFieldType = RSFLDTYPE_FULLTEXT;

            // These RM_strings are for Series->labels
            labelsResult[i].RTS_Label.key = 
                RedisModule_CreateStringFromString(NULL, key);
        	labelsResult[i].RTS_Label.value =
                RedisModule_CreateStringFromString(NULL, value);
        }
        *rsLabels = labelsResult;
    }
    return REDISMODULE_OK;

exitOnError:
    FreeRSLabels(labelsResult, *label_count, TRUE); // need to release prior key values too
    labelsResult = NULL;
    return REDISMODULE_ERR;  
}

int GetSeries(RedisModuleCtx *ctx, RedisModuleString *keyName, Series **series){
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ|REDISMODULE_WRITE);
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithError(ctx, "TSDB: the key does not exist"); // TODO add keyName
        return FALSE;
    }
    if (RedisModule_ModuleTypeGetType(key) != SeriesType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return FALSE;
    }
    *series = RedisModule_ModuleTypeGetValue(key);
    return TRUE;
}

static int parseCreateArgs(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                    long long *retentionTime, long long *maxSamplesPerChunk, size_t *labelsCount, RSLabel **labels) {
    *retentionTime = TSGlobalConfig.retentionPolicy;
    *maxSamplesPerChunk = TSGlobalConfig.maxSamplesPerChunk;
    *labelsCount = 0;
    if(parseRSLabelsFromArgs(ctx, argv, argc, labelsCount, labels) == REDISMODULE_ERR){
        RedisModule_ReplyWithError(ctx, "TSDB: Couldn't parse LABELS");
        return REDISMODULE_ERR;
    }

    if(parseFieldsFromArgs(ctx, argv, argc, labelsCount, labels) == REDISMODULE_ERR){
        RedisModule_ReplyWithError(ctx, "TSDB: Couldn't parse FIELDS");
        return REDISMODULE_ERR;
    }

    if (RMUtil_ArgIndex("RETENTION", argv, argc) > 0 && RMUtil_ParseArgsAfter("RETENTION", argv, argc, "l", retentionTime) != REDISMODULE_OK) {
        RedisModule_ReplyWithError(ctx, "TSDB: Couldn't parse RETENTION");
        return REDISMODULE_ERR;
    }

    if (retentionTime < 0) {
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
    *time_delta = 0;
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
            RedisModule_ReplyWithError(ctx, "TSDB: timeBucket must be greater than zero");
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

static int parseCountArgument(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, long long *count) {
    int offset = RMUtil_ArgIndex("COUNT", argv, argc);
    if (offset > 0) {
        if (strcasecmp(RedisModule_StringPtrLen(argv[offset - 1], NULL), "AGGREGATION") == 0) {
            int second_offset = offset + 1 + RMUtil_ArgIndex("COUNT", argv + offset + 1, argc - offset - 1);
            if (offset == second_offset) { return TSDB_OK; }
            offset = second_offset;
        }
        if (RedisModule_StringToLongLong(argv[offset + 1], count) != REDISMODULE_OK) {
            RedisModule_ReplyWithError(ctx, "TSDB: Couldn't parse COUNT");
            return TSDB_ERROR;
        }
    }
    return TSDB_OK;
}

int TSDB_info(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    
    if (argc != 2) {
    	return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ);
    Series *series;
    
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY){
        return RedisModule_ReplyWithError(ctx, "TSDB: key does not exist");
    } else if (RedisModule_ModuleTypeGetType(key) != SeriesType){
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    } else {
        series = RedisModule_ModuleTypeGetValue(key);
    }

    RedisModule_ReplyWithArray(ctx, 7*2);

    RedisModule_ReplyWithSimpleString(ctx, "lastTimestamp");
    RedisModule_ReplyWithLongLong(ctx, series->lastTimestamp);
    RedisModule_ReplyWithSimpleString(ctx, "retentionTime");
    RedisModule_ReplyWithLongLong(ctx, series->retentionTime);
    RedisModule_ReplyWithSimpleString(ctx, "chunkCount");
    RedisModule_ReplyWithLongLong(ctx, RedisModule_DictSize(series->chunks));
    RedisModule_ReplyWithSimpleString(ctx, "maxSamplesPerChunk");
    RedisModule_ReplyWithLongLong(ctx, series->maxSamplesPerChunk);

    RedisModule_ReplyWithSimpleString(ctx, "labels");
    ReplyWithSeriesLabels(ctx, series);


    RedisModule_ReplyWithSimpleString(ctx, "sourceKey");
    if(series->srcKey == NULL){
    	RedisModule_ReplyWithNull(ctx);
    } else {
    	RedisModule_ReplyWithString(ctx, series->srcKey);
    }

    RedisModule_ReplyWithSimpleString(ctx, "rules");
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    CompactionRule *rule = series->rules;
    int ruleCount = 0;
    while (rule != NULL) {
        RedisModule_ReplyWithArray(ctx, 3);
        RedisModule_ReplyWithString(ctx, rule->destKey);
        RedisModule_ReplyWithLongLong(ctx, rule->timeBucket);
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
        if (strstr(str2, "!=(") != NULL) {  // order is important! Must be before "!=".
            query->type = LIST_NOTMATCH;
            if (parsePredicate(ctx, argv[i], query, "!=(") == TSDB_ERROR) {
                return TSDB_ERROR;
            }
        } else if (strstr(str2, "!=") != NULL) {
            query->type = NEQ;
            if (parsePredicate(ctx, argv[i], query, "!=") == TSDB_ERROR) {
                return TSDB_ERROR;
            }
            if (query->valueListCount == 0) {
                query->type = CONTAINS;
            }
        } else if (strstr(str2, "=(") != NULL) {  // order is important! Must be before "=".
            query->type = LIST_MATCH;
            if (parsePredicate(ctx, argv[i], query, "=(") == TSDB_ERROR) {
                return TSDB_ERROR;
            }
        } else if (strstr(str2, "=") != NULL) {
            query->type = EQ;
            if (parsePredicate(ctx, argv[i], query, "=") == TSDB_ERROR) {
                return TSDB_ERROR;
            }
            if (query->valueListCount == 0) {
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
    if (argc < 2) {
        return RedisModule_WrongArity(ctx);
    }

    char *err = NULL;
    RSResultsIterator *resIter = GetRSIter(argv + 1, argc - 1, &err);
    if (err != NULL) {
        RedisModule_ReplyWithError(ctx, err);
        return REDISMODULE_OK;
    }
    if(resIter == NULL) { 
        return RedisModule_ReplyWithNull(ctx);
    }

    size_t keylen;
    long replylen = 0;
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    
    char *tsKey = (char *)RSL_IterateResults(resIter, &keylen);
    while(tsKey != NULL) {  // INDEXREAD_EOF == NULL
        RedisModule_ReplyWithStringBuffer(ctx, tsKey, keylen);
        ++replylen;
        tsKey = (char *)RSL_IterateResults(resIter, &keylen);
    }
    
    RedisModule_ReplySetArrayLength(ctx, replylen);
    RediSearch_ResultsIteratorFree(resIter);  

    return REDISMODULE_OK;
}

static int seriesFromKey(RedisModuleCtx *ctx, RedisModuleString *keyStr, Series **series) {
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyStr, REDISMODULE_READ);

    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_ReplyWithError(ctx, "TSDB: key does not exist");
        return REDISMODULE_ERR;
    } else if (RedisModule_ModuleTypeGetType(key) != SeriesType) {
        RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
        return REDISMODULE_ERR;
    }
    *series = RedisModule_ModuleTypeGetValue(key);

    return REDISMODULE_OK;
}

int TSDB_mrange(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    api_timestamp_t start_ts, end_ts;
    api_timestamp_t time_delta = 0;
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

    long long count = -1;
    if (parseCountArgument(ctx, argv, argc, &count) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    size_t query_count = argc - 1 - filter_location;
    QueryPredicate *queries = RedisModule_PoolAlloc(ctx, sizeof(QueryPredicate) * query_count);
    if (parseLabelListFromArgs(ctx, argv, filter_location + 1, query_count, queries) == TSDB_ERROR) {
        return RedisModule_ReplyWithError(ctx, "TSDB: failed parsing labels");
    }

    char *err = NULL;
    RSResultsIterator *resIter = GetRSIter(argv + filter_location + 1, argc - filter_location - 1, &err);
    if (err != NULL) {
        return RedisModule_ReplyWithError(ctx, "TSDB: failed parsing filters");
    }
    if(resIter == NULL) { 
        return RedisModule_ReplyWithNull(ctx);
    }

    size_t keylen;
    Series *series;
    long long replylen = 0;
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    char *tsKey = (char *)RSL_IterateResults(resIter, &keylen);
    while(tsKey != NULL) {  // INDEXREAD_EOF == NULL
        RedisModuleString *rm_key = RedisModule_CreateString(NULL, tsKey, keylen);
        if (seriesFromKey(ctx, rm_key, &series) != REDISMODULE_OK) {
            RedisModule_Log(ctx, "warning", "couldn't open key or key is not a Timeseries. key=%s", tsKey);
            continue;
        }
        RedisModule_ReplyWithArray(ctx, 3);
        RedisModule_ReplyWithString(ctx,rm_key);
        ReplyWithSeriesLabels(ctx, series);
        ReplySeriesRange(ctx, series, start_ts, end_ts, aggObject, time_delta, count);
        replylen++;
        RedisModule_FreeString(NULL, rm_key);
        tsKey = (char *)RSL_IterateResults(resIter, &keylen);
    }
    RedisModule_ReplySetArrayLength(ctx, replylen);

    return REDISMODULE_OK;
}

int TSDB_range(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    Series *series;
    if (seriesFromKey(ctx, argv[1], &series) != REDISMODULE_OK) {
        return REDISMODULE_OK;
    }

    api_timestamp_t start_ts, end_ts;
    api_timestamp_t time_delta = 0;
    if (parseRangeArguments(ctx, series, 2, argv, &start_ts, &end_ts) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    long long count = -1;
    if (parseCountArgument(ctx, argv, argc, &count) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    AggregationClass *aggObject = NULL;
    int aggregationResult = parseAggregationArgs(ctx, argv, argc, &time_delta, &aggObject);
    if (aggregationResult == TSDB_ERROR) {
        return REDISMODULE_ERR;
    }

    ReplySeriesRange(ctx, series, start_ts, end_ts, aggObject, time_delta, count);
    return REDISMODULE_OK;
}

int ReplySeriesRange(RedisModuleCtx *ctx, Series *series, api_timestamp_t start_ts, api_timestamp_t end_ts,
        AggregationClass *aggObject, int64_t time_delta, long long maxResults) {
    Sample sample;
    long long arraylen = 0;
    timestamp_t last_agg_timestamp = 0;

    // In case a retention is set shouldn't return chunks older than the retention 
    if(series->retentionTime){
    	start_ts = series->lastTimestamp > series->retentionTime ?
    			max(start_ts, series->lastTimestamp - series->retentionTime) : start_ts;
    }
    SeriesIterator iterator = SeriesQuery(series, start_ts, end_ts);

    void *context = NULL;
    if (aggObject != NULL)
        context = aggObject->createContext();
    
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    while (SeriesIteratorGetNext(&iterator, &sample) != 0 &&
                    (maxResults == -1 || arraylen < maxResults)) {
        if (aggObject == NULL) { // No aggregation whatssoever
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
    SeriesIteratorClose(&iterator);

    if (aggObject != AGG_NONE && arraylen != maxResults) {
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

    timestamp_t currentTimestamp = timestamp - timestamp % rule->timeBucket;
    if (currentTimestamp > destSeries->lastTimestamp) {
        rule->aggClass->resetContext(rule->aggContext);
    }
    rule->aggClass->appendValue(rule->aggContext, value);
    SeriesAddSample(destSeries, currentTimestamp, rule->aggClass->finalize(rule->aggContext));
    RedisModule_CloseKey(key);
}

static inline int add(RedisModuleCtx *ctx, RedisModuleString *keyName, RedisModuleString *timestampStr, RedisModuleString *valueStr, RedisModuleString **argv, int argc){
    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ|REDISMODULE_WRITE);

    double value;
    api_timestamp_t timestamp;
    if ((RedisModule_StringToDouble(valueStr, &value) != REDISMODULE_OK))
        return RedisModule_ReplyWithError(ctx, "TSDB: invalid value");

    if ((RedisModule_StringToLongLong(timestampStr, (long long int *) &timestamp) != REDISMODULE_OK)) {
        // if timestamp is "*", take current time (automatic timestamp)
        if(RMUtil_StringEqualsC(timestampStr, "*"))
            timestamp = (u_int64_t) RedisModule_Milliseconds();
        else
            return RedisModule_ReplyWithError(ctx, "TSDB: invalid timestamp");
    }

    Series *series = NULL;

    if (argv != NULL && RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        // the key doesn't exist, lets check we have enough information to create one
        long long retentionTime;
        long long maxSamplesPerChunk;
        size_t labelsCount;
        RSLabel *labels;
        if (parseCreateArgs(ctx, argv, argc, &retentionTime, &maxSamplesPerChunk, &labelsCount, &labels) != REDISMODULE_OK) {
            return REDISMODULE_ERR;
        }

        CreateTsKey(ctx, keyName, labels, labelsCount, retentionTime, maxSamplesPerChunk, &series, &key);
        SeriesCreateRulesFromGlobalConfig(ctx, keyName, series, labels, labelsCount);
    } else if (RedisModule_ModuleTypeGetType(key) != SeriesType){
        return RedisModule_ReplyWithError(ctx, "TSDB: the key is not a TSDB key");
    } else {
        series = RedisModule_ModuleTypeGetValue(key);
    }

    int retval = SeriesAddSample(series, timestamp, value);
    int result = 0;
    if (retval == TSDB_ERR_TIMESTAMP_TOO_OLD) {
        RedisModule_ReplyWithError(ctx, "TSDB: Timestamp cannot be older than the latest timestamp in the time series");
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
        RedisModule_ReplyWithLongLong(ctx, timestamp);
        result = REDISMODULE_OK;
    }
    RedisModule_CloseKey(key);
    return result;
}

int TSDB_madd(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 4 || (argc-1)%3 != 0) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModule_ReplyWithArray(ctx, (argc-1)/3);
    for(int i=1; i<argc ; i+=3){
        RedisModuleString *keyName = argv[i];
        RedisModuleString *timestampStr = argv[i+1];
        RedisModuleString *valueStr = argv[i+2];
        add(ctx, keyName, timestampStr, valueStr, NULL, -1);
    }
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

int TSDB_add(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 4) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleString *keyName = argv[1];
    RedisModuleString *timestampStr = argv[2];
    RedisModuleString *valueStr = argv[3];

    int result = add(ctx, keyName, timestampStr, valueStr, argv, argc);
    RedisModule_ReplicateVerbatim(ctx);
    return result;
}

int CreateTsKey(RedisModuleCtx *ctx, RedisModuleString *keyName, RSLabel *labels, size_t labelsCount, long long retentionTime,
                long long maxSamplesPerChunk, Series **series, RedisModuleKey **key) {
    if (*key == NULL) {
        *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ|REDISMODULE_WRITE);
    }

    RedisModule_RetainString(ctx, keyName);
    *series = NewSeries(keyName, labels, labelsCount, retentionTime, maxSamplesPerChunk);
    if (RedisModule_ModuleTypeSetValue(*key, SeriesType, *series) == REDISMODULE_ERR) {
        return TSDB_ERROR;
    }

    RSL_Index(TSGlobalConfig.globalRSIndex, keyName, labels, labelsCount);
    FreeRSLabels(labels, labelsCount, FALSE);

    return TSDB_OK;
}

int TSDB_create(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 2) {
        return RedisModule_WrongArity(ctx);
    }

    Series *series;
    RedisModuleString *keyName = argv[1];
    long long retentionTime;
    long long maxSamplesPerChunk;
    size_t labelsCount;
    RSLabel *labels = NULL;

    if (parseCreateArgs(ctx, argv, argc, &retentionTime, &maxSamplesPerChunk, &labelsCount, &labels) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ|REDISMODULE_WRITE);

    if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx,"TSDB: key already exists");
    }

    CreateTsKey(ctx, keyName, labels, labelsCount, retentionTime, maxSamplesPerChunk, &series, &key);
    RedisModule_CloseKey(key);

    RedisModule_Log(ctx, "verbose", "created new series");
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

int TSDB_alter(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
    RedisModule_AutoMemory(ctx);

    if (argc < 2) {
        return RedisModule_WrongArity(ctx);
    }

    Series *series;
    RedisModuleString *keyName = argv[1];
    long long retentionTime;
    long long maxSamplesPerChunk;
    size_t labelsCount = 0;
    RSLabel *newLabels = NULL;

    if (parseCreateArgs(ctx, argv, argc, &retentionTime, &maxSamplesPerChunk, &labelsCount, &newLabels) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, keyName, REDISMODULE_READ|REDISMODULE_WRITE);
    if (RedisModule_ModuleTypeGetType(key) != SeriesType) {
        RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    series = RedisModule_ModuleTypeGetValue(key);
    if (RMUtil_ArgIndex("RETENTION", argv, argc) > 0) {
        series->retentionTime = retentionTime;
    }

    if (RMUtil_ArgIndex("CHUNK_SIZE", argv, argc) > 0) {
        series->maxSamplesPerChunk = maxSamplesPerChunk;
    }

    if (newLabels != NULL) {
        RSL_Index(TSGlobalConfig.globalRSIndex, keyName, newLabels, labelsCount);
        free(series->labels);
        series->labels = (Label *)calloc(labelsCount, sizeof(Label));
        series->labels = RSLabelToLabels(NULL, newLabels, labelsCount);
        series->labelsCount = labelsCount;
        FreeRSLabels(newLabels, labelsCount, FALSE);
    }

    RedisModule_CloseKey(key);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

/*
TS.DELETERULE SOURCE_KEY DEST_KEY
 */
int TSDB_deleteRule(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 3) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleString *srcKeyName = argv[1];

    // First try to remove the rule from the source key
    Series *srcSeries;
    int status = GetSeries(ctx, srcKeyName, &srcSeries);
    if(!status){
    	return REDISMODULE_ERR;
    }

    RedisModuleString *destKeyName = argv[2];
    if (!SeriesDeleteRule(srcSeries, destKeyName)) {
    	return RedisModule_ReplyWithError(ctx, "TSDB: compaction rule does not exist");
    }

    // If succeed to remove the rule from the source key remove from the destination too
    Series *destSeries;
    status = GetSeries(ctx, destKeyName, &destSeries);
    if(!status){
    	return REDISMODULE_ERR;
    }
    SeriesDeleteSrcRule(destSeries, srcKeyName);

    RedisModule_ReplyWithSimpleString(ctx, "OK");
    RedisModule_ReplicateVerbatim(ctx);
    return REDISMODULE_OK;
}

/*
TS.CREATERULE sourceKey destKey AGGREGATION aggregationType timeBucket
*/
int TSDB_createRule(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc != 6){
        return RedisModule_WrongArity(ctx);
    }

    // Validate aggregation arguments
    api_timestamp_t timeBucket;
    int aggType;
    int result = _parseAggregationArgs(ctx, argv, argc, &timeBucket, &aggType);
    if (result == TSDB_NOTEXISTS) {
        return RedisModule_WrongArity(ctx);
    }
    if (result == TSDB_ERROR) {
        return REDISMODULE_ERR;
    }

    RedisModuleString *srcKeyName = argv[1];
    RedisModuleString *destKeyName = argv[2];
    if(!RedisModule_StringCompare(srcKeyName, destKeyName)){
    	return RedisModule_ReplyWithError(ctx, "TSDB: the source key and destination key should be different");
    }

    // First we verify the source is not a destination
    Series *srcSeries;
    int status = GetSeries(ctx, srcKeyName, &srcSeries);
    if(!status){
    	return REDISMODULE_ERR;
    }
    if(srcSeries->srcKey){
    	return RedisModule_ReplyWithError(ctx, "TSDB: the source key already has a source rule");
    }

    // Second verify the destination doesn't have other rule
    Series *destSeries;
    status = GetSeries(ctx, destKeyName, &destSeries);
    if(!status){
    	return REDISMODULE_ERR;
    }
    srcKeyName = RedisModule_CreateStringFromString(ctx, srcKeyName);
    if(!SeriesSetSrcRule(destSeries, srcKeyName)){
    	return RedisModule_ReplyWithError(ctx, "TSDB: the destination key already has a rule");
    }
    RedisModule_RetainString(ctx, srcKeyName);

    // Last add the rule to source
    destKeyName = RedisModule_CreateStringFromString(ctx, destKeyName);
    if (SeriesAddRule(srcSeries, destKeyName, aggType, timeBucket) == NULL) {
        RedisModule_ReplyWithSimpleString(ctx, "TSDB: ERROR creating rule");
        return REDISMODULE_ERR;
    }
    RedisModule_RetainString(ctx, destKeyName);

    RedisModule_ReplicateVerbatim(ctx);
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}


/*
TS.INCRBY ts_key NUMBER [RESET time-bucket]
*/
int TSDB_incrby(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);

    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleString *keyName = argv[1];
    Series *series;

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY) {
        // the key doesn't exist, lets check we have enough information to create one
        long long retentionTime;
        long long maxSamplesPerChunk;
        size_t labelsCount;
        RSLabel *labels;
        if (parseCreateArgs(ctx, argv, argc, &retentionTime, &maxSamplesPerChunk, &labelsCount, &labels) != REDISMODULE_OK) {
            return REDISMODULE_ERR;
        }

        CreateTsKey(ctx, keyName, labels, labelsCount, retentionTime, maxSamplesPerChunk, &series, &key);
        SeriesCreateRulesFromGlobalConfig(ctx, keyName, series, labels, labelsCount);
    }

    series = RedisModule_ModuleTypeGetValue(key);
    double incrby = 0;
    if (RMUtil_ParseArgs(argv, argc, 2, "d", &incrby) != REDISMODULE_OK)
        return RedisModule_WrongArity(ctx);

    double result;
    long long resetMilliSeconds = 1;
    time_t currentUpdatedTime = RedisModule_Milliseconds();
    if (RMUtil_ArgIndex("RESET", argv, argc) > 0) {
        if (RMUtil_ParseArgsAfter("RESET", argv, argc, "l", &resetMilliSeconds) != 0) {
            return RedisModule_WrongArity(ctx);
        }

        currentUpdatedTime = currentUpdatedTime - (currentUpdatedTime % resetMilliSeconds);
        if (series->lastTimestamp != 0) {
            u_int64_t lastTS = series->lastTimestamp;
            if (lastTS - (lastTS % resetMilliSeconds) !=  currentUpdatedTime) {
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

static inline void get(RedisModuleCtx *ctx, Series *series) {
    RedisModule_ReplyWithLongLong(ctx, series->lastTimestamp);
    RedisModule_ReplyWithDouble(ctx, series->lastValue);
}

int TSDB_get(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc != 2) {
    	return RedisModule_WrongArity(ctx);
    }

    Series *series;
    if (seriesFromKey(ctx, argv[1], &series) != REDISMODULE_OK) {
        return REDISMODULE_OK;
    }

    RedisModule_ReplyWithArray(ctx, 2);
    get(ctx, series);
    return REDISMODULE_OK;
}

int TSDB_mget(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    if (argc < 3 || RMUtil_ArgIndex("FILTER", argv, argc) != 1) {
        return RedisModule_WrongArity(ctx);
    }

    char *err = NULL;
    RSResultsIterator *resIter = GetRSIter(argv + 2, argc - 2, &err);
    if (err != NULL) {
        return RedisModule_ReplyWithError(ctx, "TSDB: failed parsing filters");
    }
    if(resIter == NULL) { 
        return RedisModule_ReplyWithNull(ctx);
    }

    size_t keylen;
    Series *series;
    long long replylen = 0;
    RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    char *tsKey = (char *)RSL_IterateResults(resIter, &keylen);
    while(tsKey != NULL) {  // INDEXREAD_EOF == NULL
        RedisModuleString *rm_key = RedisModule_CreateString(NULL, tsKey, keylen);
        if (seriesFromKey(ctx, rm_key, &series) != REDISMODULE_OK) {
            return REDISMODULE_OK;
        }

        RedisModule_ReplyWithArray(ctx, 4);
        RedisModule_ReplyWithStringBuffer(ctx, tsKey, keylen);
        ReplyWithSeriesLabels(ctx, series);
        RedisModule_ReplyWithLongLong(ctx, series->lastTimestamp);
        RedisModule_ReplyWithDouble(ctx, series->lastValue);

        ++replylen;
        RedisModule_FreeString(NULL, rm_key);
        tsKey = (char *)RSL_IterateResults(resIter, &keylen);
    }
    RedisModule_ReplySetArrayLength(ctx, replylen);

    return REDISMODULE_OK;
}

int NotifyCallback(RedisModuleCtx *original_ctx, int type, const char *event, RedisModuleString *key) {
    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
    RedisModule_AutoMemory(ctx);

    if (strcasecmp(event, "del")==0) {
        CleanLastDeletedSeries(ctx, key);
    }

    RedisModule_FreeThreadSafeContext(ctx);

    return REDISMODULE_OK;
}

/*
module loading function, possible arguments:
COMPACTION_POLICY - compaction policy from parse_policies,h
RETENTION_POLICY - long that represents the retention in milliseconds
MAX_SAMPLE_PER_CHUNK - how many samples per chunk
example:
redis-server --loadmodule ./redistimeseries.so COMPACTION_POLICY "max:1m:1d;min:10s:1h;avg:2h:10d;avg:3d:100d" RETENTION_POLICY 3600 MAX_SAMPLE_PER_CHUNK 1024
*/
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx, "timeseries", REDISTIMESERIES_MODULE_VERSION, REDISMODULE_APIVER_1) == REDISMODULE_ERR) {
        return REDISMODULE_ERR;
    }

    if (RediSearch_Init(ctx, REDISEARCH_INIT_LIBRARY) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }
    
    if (ReadTSConfig(ctx, argv, argc) == TSDB_ERROR) {
        return REDISMODULE_ERR;
    }

    TSGlobalConfig.globalRSIndex = RSLiteCreate("RedisTimeSeries_RSIndex");

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
    //IndexInit();
    RMUtil_RegisterWriteCmd(ctx, "ts.create", TSDB_create);
    RMUtil_RegisterWriteCmd(ctx, "ts.alter", TSDB_alter);
    RMUtil_RegisterWriteCmd(ctx, "ts.createrule", TSDB_createRule);
    RMUtil_RegisterWriteCmd(ctx, "ts.deleterule", TSDB_deleteRule);
    RMUtil_RegisterWriteCmd(ctx, "ts.add", TSDB_add);
    RMUtil_RegisterWriteCmd(ctx, "ts.incrby", TSDB_incrby);
    RMUtil_RegisterWriteCmd(ctx, "ts.decrby", TSDB_incrby);
    RMUtil_RegisterReadCmd(ctx, "ts.range", TSDB_range);
    RMUtil_RegisterReadCmd(ctx, "ts.queryindex", TSDB_queryindex);
    RMUtil_RegisterReadCmd(ctx, "ts.info", TSDB_info);
    RMUtil_RegisterReadCmd(ctx, "ts.get", TSDB_get);

    if (RedisModule_CreateCommand(ctx, "ts.madd", TSDB_madd, "write", 1, -1, 3) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "ts.mrange", TSDB_mrange, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx, "ts.mget", TSDB_mget, "readonly", 0, 0, 0) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    RedisModule_SubscribeToKeyspaceEvents(ctx, REDISMODULE_NOTIFY_GENERIC, NotifyCallback);

    return REDISMODULE_OK;
}
