
#include "libmr_integration.h"

#include "LibMR/src/mr.h"
#include "LibMR/src/record.h"
#include "LibMR/src/utils/arr.h"
#include "consts.h"
#include "generic_chunk.h"
#include "indexer.h"
#include "module.h"
#include "query_language.h"
#include "tsdb.h"

#include "RedisModulesSDK/redismodule.h"
#include "rmutil/alloc.h"

#define SeriesRecordName "SeriesRecord"

static Record NullRecord;
static MRRecordType *nullRecordType = NULL;
static MRRecordType *stringRecordType = NULL;
static MRRecordType *listRecordType = NULL;
static MRRecordType *SeriesRecordType = NULL;
static MRRecordType *LongRecordType = NULL;
static MRRecordType *DoubleRecordType = NULL;
static MRRecordType *mapRecordType = NULL;

static Record *GetNullRecord() {
    return &NullRecord;
}

MRRecordType *GetMapRecordType() {
    return mapRecordType;
}

MRRecordType *GetListRecordType() {
    return listRecordType;
}

MRRecordType *GetSeriesRecordType() {
    return SeriesRecordType;
}

static void QueryPredicates_ObjectFree(void *arg) {
    QueryPredicates_Arg *predicate_list = arg;

    if (__atomic_sub_fetch(&predicate_list->refCount, 1, __ATOMIC_RELAXED) > 0) {
        return;
    }

    QueryPredicateList_Free(predicate_list->predicates);
    for (int i = 0; i < predicate_list->limitLabelsSize; i++) {
        RedisModule_FreeString(NULL, predicate_list->limitLabels[i]);
    }
    free(predicate_list->limitLabels);
    free(predicate_list);
}

static void *QueryPredicates_Duplicate(void *arg) {
    QueryPredicates_Arg *queryArg = (QueryPredicates_Arg *)arg;
    __atomic_add_fetch(&queryArg->refCount, 1, __ATOMIC_RELAXED);
    return arg;
}

static char *QueryPredicates_ToString(void *arg) {
    QueryPredicates_Arg *predicate_list = arg;
    char out[250];
    int index = 0;
    index += sprintf(out, "QueryPredicates: len: %lu; ", predicate_list->predicates->count);
    for (int i = 0; i < predicate_list->predicates->count; i++) {
        QueryPredicate *predicate = predicate_list->predicates->list + i;
        size_t len;
        index += sprintf(out + index,
                         "'%s=%s' ",
                         RedisModule_StringPtrLen(predicate->key, &len),
                         RedisModule_StringPtrLen(predicate->valuesList[0], &len));
    }
    return strdup(out);
}

static Record *DoubleRecord_Create(double val);
static void DoubleRecord_Free(void *base);
static void DoubleRecord_Add(Record *base, Record *element);
static void *DoubleRecord_Deserialize(ReaderSerializationCtx *sctx, MRError **error);
static void DoubleRecord_Serialize(WriteSerializationCtx *sctx, void *arg, MRError **error);
static void DoubleRecord_SendReply(RedisModuleCtx *rctx, void *record);
static Record *LongRecord_Create(long val);
static void ListRecord_Free(void *base);
static void ListRecord_Add(Record *base, Record *element);
static void *ListRecord_Deserialize(ReaderSerializationCtx *sctx, MRError **error);
static void ListRecord_Serialize(WriteSerializationCtx *sctx, void *arg, MRError **error);
static void ListRecord_SendReply(RedisModuleCtx *rctx, void *record);
static void MapRecord_Free(void *base);
static void MapRecord_Add(Record *base, Record *element);
static void *MapRecord_Deserialize(ReaderSerializationCtx *sctx, MRError **error);
static void MapRecord_Serialize(WriteSerializationCtx *sctx, void *arg, MRError **error);
static void MapRecord_SendReply(RedisModuleCtx *rctx, void *record);
static void StringRecord_Free(void *base);
static void StringRecord_Serialize(WriteSerializationCtx *sctx, void *base, MRError **error);
static void *StringRecord_Deserialize(ReaderSerializationCtx *sctx, MRError **error);
static void StringRecord_SendReply(RedisModuleCtx *rctx, void *r);
static void NullRecord_SendReply(RedisModuleCtx *rctx, void *base);
static void NullRecord_Serialize(WriteSerializationCtx *sctx, void *arg, MRError **error);
static void *NullRecord_Deserialize(ReaderSerializationCtx *sctx, MRError **error);
static void NullRecord_Free(void *base);
static void DoubleRecord_Free(void *arg);
static void DoubleRecord_Serialize(WriteSerializationCtx *sctx, void *arg, MRError **error);
static void *DoubleRecord_Deserialize(ReaderSerializationCtx *sctx, MRError **error);
static void DoubleRecord_SendReply(RedisModuleCtx *rctx, void *r);
static void LongRecord_Free(void *arg);
static void LongRecord_Serialize(WriteSerializationCtx *sctx, void *arg, MRError **error);
static void *LongRecord_Deserialize(ReaderSerializationCtx *sctx, MRError **error);
static void LongRecord_SendReply(RedisModuleCtx *rctx, void *r);
static Record *RedisStringRecord_Create(RedisModuleString *str);

static void SerializationCtxWriteRedisString(WriteSerializationCtx *sctx,
                                             const RedisModuleString *arg,
                                             MRError **error);

static void QueryPredicates_ArgSerialize(WriteSerializationCtx *sctx, void *arg, MRError **error) {
    QueryPredicates_Arg *predicate_list = arg;
    MR_SerializationCtxWriteLongLong(sctx, predicate_list->predicates->count, error);
    MR_SerializationCtxWriteLongLong(sctx, predicate_list->withLabels, error);
    MR_SerializationCtxWriteLongLong(sctx, predicate_list->limitLabelsSize, error);
    MR_SerializationCtxWriteLongLong(sctx, predicate_list->startTimestamp, error);
    MR_SerializationCtxWriteLongLong(sctx, predicate_list->endTimestamp, error);
    MR_SerializationCtxWriteLongLong(sctx, predicate_list->latest, error);
    MR_SerializationCtxWriteLongLong(sctx, predicate_list->resp3, error);

    for (int i = 0; i < predicate_list->limitLabelsSize; i++) {
        SerializationCtxWriteRedisString(sctx, predicate_list->limitLabels[i], error);
    }
    for (int i = 0; i < predicate_list->predicates->count; i++) {
        // encode type
        QueryPredicate *predicate = predicate_list->predicates->list + i;
        MR_SerializationCtxWriteLongLong(sctx, predicate->type, error);

        // encode key
        SerializationCtxWriteRedisString(sctx, predicate->key, error);

        // encode values
        MR_SerializationCtxWriteLongLong(sctx, predicate->valueListCount, error);
        for (int value_index = 0; value_index < predicate->valueListCount; value_index++) {
            SerializationCtxWriteRedisString(sctx, predicate->valuesList[value_index], error);
        }
    }
}

static void SerializationCtxWriteRedisString(WriteSerializationCtx *sctx,
                                             const RedisModuleString *arg,
                                             MRError **error) {
    size_t value_len = 0;
    const char *value = RedisModule_StringPtrLen(arg, &value_len);
    MR_SerializationCtxWriteBuffer(sctx, value, value_len + 1, error);
}

static RedisModuleString *SerializationCtxReadeRedisString(ReaderSerializationCtx *sctx,
                                                           MRError **error) {
    size_t len;
    const char *temp = MR_SerializationCtxReadBuffer(sctx, &len, error);
    return RedisModule_CreateString(NULL, temp, len - 1);
}

static void QueryPredicates_CleanupFailedDeserialization(QueryPredicates_Arg *predicates) {
    if (predicates->predicates->list) {
        for (int i = 0; i < predicates->predicates->count; i++) {
            QueryPredicate *predicate = &predicates->predicates->list[i];
            if (!predicate->key)
                break;

            if (predicate->valuesList) {
                for (int j = 0; j < predicate->valueListCount && predicate->valuesList[j]; j++) {
                    RedisModule_FreeString(NULL, predicate->valuesList[j]);
                }
                free(predicate->valuesList);
            }
            RedisModule_FreeString(NULL, predicate->key);
        }
        free(predicates->predicates->list);
    }
    free(predicates->predicates);
    if (predicates->limitLabels) {
        for (int i = 0; i < predicates->limitLabelsSize && predicates->limitLabels[i]; ++i) {
            RedisModule_FreeString(NULL, predicates->limitLabels[i]);
        }
        free(predicates->limitLabels);
    }
    free(predicates);
}

static void *QueryPredicates_ArgDeserialize_impl(ReaderSerializationCtx *sctx,
                                                 MRError **error,
                                                 bool expect_resp) {
    QueryPredicates_Arg *predicates = calloc(1, sizeof *predicates);
    predicates->shouldReturnNull = false;
    predicates->refCount = 1;
    predicates->predicates = calloc(1, sizeof *predicates->predicates);
    predicates->predicates->count = MR_SerializationCtxReadLongLong(sctx, error);
    predicates->predicates->ref = 1;
    predicates->withLabels = MR_SerializationCtxReadLongLong(sctx, error);
    predicates->limitLabelsSize = MR_SerializationCtxReadLongLong(sctx, error);
    predicates->startTimestamp = MR_SerializationCtxReadLongLong(sctx, error);
    predicates->endTimestamp = MR_SerializationCtxReadLongLong(sctx, error);
    predicates->latest = MR_SerializationCtxReadLongLong(sctx, error);
    predicates->resp3 = expect_resp ? MR_SerializationCtxReadLongLong(sctx, error) : false;
    // check that the value read is a boolean.
    // *error must be NULL here, as ReadLongLong checks that we do not exceed the buffer.
    if (unlikely(expect_resp && (bool)predicates->resp3 != predicates->resp3)) {
        goto err;
    }

    predicates->limitLabels = calloc(predicates->limitLabelsSize, sizeof *predicates->limitLabels);
    for (int i = 0; i < predicates->limitLabelsSize; ++i) {
        predicates->limitLabels[i] = SerializationCtxReadeRedisString(sctx, error);
        if (unlikely(expect_resp && *error)) {
            goto err;
        }
    }

    predicates->predicates->list =
        calloc(predicates->predicates->count, sizeof *predicates->predicates->list);
    for (int i = 0; i < predicates->predicates->count; i++) {
        QueryPredicate *predicate = &predicates->predicates->list[i];
        // decode type
        predicate->type = MR_SerializationCtxReadLongLong(sctx, error);
        if (unlikely(expect_resp && *error)) {
            goto err;
        }

        // decode key
        predicate->key = SerializationCtxReadeRedisString(sctx, error);
        if (unlikely(expect_resp && *error)) {
            goto err;
        }

        // decode values
        predicate->valueListCount = MR_SerializationCtxReadLongLong(sctx, error);
        if (unlikely(expect_resp && *error)) {
            goto err;
        }

        predicate->valuesList = calloc(predicate->valueListCount, sizeof *predicate->valuesList);
        for (int value_index = 0; value_index < predicate->valueListCount; value_index++) {
            predicate->valuesList[value_index] = SerializationCtxReadeRedisString(sctx, error);
            if (unlikely(expect_resp && *error)) {
                goto err;
            }
        }
    }

    return predicates;

err:
    *error = NULL;
    MR_SerializationCtxReaderRewind(sctx);
    QueryPredicates_CleanupFailedDeserialization(predicates);
    return NULL;
}

static void *QueryPredicates_ArgDeserialize(ReaderSerializationCtx *sctx, MRError **error) {
    return QueryPredicates_ArgDeserialize_impl(sctx, error, true)
               ?: QueryPredicates_ArgDeserialize_impl(sctx, error, false);
}

static Record *StringRecord_Create(char *val, size_t len);
static Record *ListRecord_Create(size_t initSize);
static Record *MapRecord_Create(size_t initSize);

static Record *RedisStringRecord_Create(RedisModuleString *str) {
    size_t len = 0;
    const char *cstr = RedisModule_StringPtrLen(str, &len);
    return StringRecord_Create(strndup(cstr, len), len);
}

Record *ListSeriesLabels_resp3(const Series *series) {
    Record *r = MapRecord_Create(series->labelsCount);

    for (int i = 0; i < series->labelsCount; i++) {
        MapRecord_Add(r, RedisStringRecord_Create(series->labels[i].key));
        MapRecord_Add(r, RedisStringRecord_Create(series->labels[i].value));
    }
    return r;
}

Record *ListSeriesLabels(const Series *series) {
    Record *r = ListRecord_Create(series->labelsCount);
    for (int i = 0; i < series->labelsCount; i++) {
        Record *internal_list = ListRecord_Create(series->labelsCount);
        ListRecord_Add(internal_list, RedisStringRecord_Create(series->labels[i].key));
        ListRecord_Add(internal_list, RedisStringRecord_Create(series->labels[i].value));
        ListRecord_Add(r, internal_list);
    }
    return r;
}

Record *ListSeriesLabelsWithLimit_rep3(const Series *series,
                                       const char *limitLabels[],
                                       RedisModuleString **rLimitLabels,
                                       ushort limitLabelsSize) {
    Record *r = MapRecord_Create(series->labelsCount);
    for (int i = 0; i < limitLabelsSize; i++) {
        bool found = false;
        for (int j = 0; j < series->labelsCount; ++j) {
            const char *key = RedisModule_StringPtrLen(series->labels[j].key, NULL);
            if (strcasecmp(key, limitLabels[i]) == 0) {
                MapRecord_Add(r, RedisStringRecord_Create(series->labels[j].key));
                MapRecord_Add(r, RedisStringRecord_Create(series->labels[j].value));
                found = true;
                break;
            }
        }
        if (!found) {
            MapRecord_Add(r, RedisStringRecord_Create(rLimitLabels[i]));
            MapRecord_Add(r, GetNullRecord());
        }
    }
    return r;
}

Record *ListSeriesLabelsWithLimit(const Series *series,
                                  const char *limitLabels[],
                                  RedisModuleString **rLimitLabels,
                                  ushort limitLabelsSize) {
    Record *r = ListRecord_Create(series->labelsCount);
    for (int i = 0; i < limitLabelsSize; i++) {
        bool found = false;
        for (int j = 0; j < series->labelsCount; ++j) {
            const char *key = RedisModule_StringPtrLen(series->labels[j].key, NULL);
            if (strcasecmp(key, limitLabels[i]) == 0) {
                Record *internal_list = ListRecord_Create(series->labelsCount);
                ListRecord_Add(internal_list, RedisStringRecord_Create(series->labels[j].key));
                ListRecord_Add(internal_list, RedisStringRecord_Create(series->labels[j].value));
                ListRecord_Add(r, internal_list);
                found = true;
                break;
            }
        }
        if (!found) {
            Record *internal_list = ListRecord_Create(series->labelsCount);
            ListRecord_Add(internal_list, RedisStringRecord_Create(rLimitLabels[i]));
            ListRecord_Add(internal_list, GetNullRecord());
            ListRecord_Add(r, internal_list);
        }
    }
    return r;
}

#define MAX_VAL_LEN 24
Record *ListWithSample(uint64_t timestamp, double value, bool resp3) {
    Record *r = ListRecord_Create(2);
    ListRecord_Add(r, LongRecord_Create(timestamp));
    if (resp3) {
        ListRecord_Add(r, DoubleRecord_Create(value));
        return r;
    } else {
        char buf[MAX_VAL_LEN];
        snprintf(buf, MAX_VAL_LEN, "%.15g", value);
        ListRecord_Add(r, StringRecord_Create(strdup(buf), strlen(buf)));
    }
    return r;
}

Record *ListWithSeriesLastDatapoint(const Series *series, bool latest, bool resp3) {
    if (should_finalize_last_bucket_get(latest, series)) {
        Sample sample;
        Sample *sample_ptr = &sample;
        calculate_latest_sample(&sample_ptr, series);
        if (sample_ptr) {
            return ListWithSample(sample.timestamp, sample.value, resp3);
        }
    }

    if (SeriesGetNumSamples(series) == 0) {
        return ListRecord_Create(0);
    } else {
        return ListWithSample(series->lastTimestamp, series->lastValue, resp3);
    }
}

// LATEST is ignored for a series that is not a compaction.
#define should_finalize_last_bucket(pred, series)                                                  \
    ((pred)->latest && (series)->srcKey && (pred)->endTimestamp > (series)->lastTimestamp)

Record *ShardSeriesMapper(ExecutionCtx *rctx, void *arg) {
    QueryPredicates_Arg *predicates = arg;

    if (predicates->shouldReturnNull) {
        return NULL;
    }
    predicates->shouldReturnNull = true;

    RedisModule_ThreadSafeContextLock(rts_staticCtx);

    // The permission error is ignored.
    RedisModuleDict *result = QueryIndex(
        rts_staticCtx, predicates->predicates->list, predicates->predicates->count, NULL);

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(result, "^", NULL, 0);
    char *currentKey;
    size_t currentKeyLen;

    Series *series;
    Record *series_list = ListRecord_Create(0);
    const GetSeriesFlags flags = GetSeriesFlags_SilentOperation | GetSeriesFlags_CheckForAcls;

    while ((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        RedisModuleKey *key;
        RedisModuleString *keyName =
            RedisModule_CreateString(rts_staticCtx, currentKey, currentKeyLen);
        const GetSeriesResult status =
            GetSeries(rts_staticCtx, keyName, &key, &series, REDISMODULE_READ, flags);

        RedisModule_FreeString(rts_staticCtx, keyName);

        if (status != GetSeriesResult_Success) {
            RedisModule_Log(rts_staticCtx,
                            "warning",
                            "couldn't open key or key is not a Timeseries. key=%.*s",
                            (int)currentKeyLen,
                            currentKey);
            continue;
        }

        ListRecord_Add(
            series_list,
            SeriesRecord_New(
                series, predicates->startTimestamp, predicates->endTimestamp, predicates));

        RedisModule_CloseKey(key);
    }

    RedisModule_DictIteratorStop(iter);
    RedisModule_FreeDict(rts_staticCtx, result);
    RedisModule_ThreadSafeContextUnlock(rts_staticCtx);

    return series_list;
}

Record *ShardMgetMapper(ExecutionCtx *rctx, void *arg) {
    QueryPredicates_Arg *predicates = arg;

    if (predicates->shouldReturnNull) {
        return NULL;
    }
    predicates->shouldReturnNull = true;

    const char **limitLabelsStr = calloc(predicates->limitLabelsSize, sizeof(char *));
    for (int i = 0; i < predicates->limitLabelsSize; i++) {
        limitLabelsStr[i] = RedisModule_StringPtrLen(predicates->limitLabels[i], NULL);
    }

    RedisModule_ThreadSafeContextLock(rts_staticCtx);

    // The permission error is ignored.
    RedisModuleDict *result = QueryIndex(
        rts_staticCtx, predicates->predicates->list, predicates->predicates->count, NULL);

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(result, "^", NULL, 0);
    char *currentKey;
    size_t currentKeyLen;

    Series *series;
    Record *series_listOrMap;
    if (predicates->resp3) {
        series_listOrMap = MapRecord_Create(0);
    } else {
        series_listOrMap = ListRecord_Create(0);
    }

    const GetSeriesFlags flags = GetSeriesFlags_SilentOperation | GetSeriesFlags_CheckForAcls;

    while ((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        RedisModuleKey *key;
        RedisModuleString *keyName =
            RedisModule_CreateString(rts_staticCtx, currentKey, currentKeyLen);
        const GetSeriesResult status =
            GetSeries(rts_staticCtx, keyName, &key, &series, REDISMODULE_READ, flags);
        RedisModule_FreeString(rts_staticCtx, keyName);

        if (status != GetSeriesResult_Success) {
            RedisModule_Log(rts_staticCtx,
                            "warning",
                            "couldn't open key or key is not a Timeseries. key=%.*s",
                            (int)currentKeyLen,
                            currentKey);
            continue;
        }

        if (predicates->resp3) {
            MapRecord_Add(series_listOrMap,
                          StringRecord_Create(strndup(currentKey, currentKeyLen), currentKeyLen));
            Record *list_record = ListRecord_Create(2);
            if (predicates->withLabels) {
                ListRecord_Add(list_record, ListSeriesLabels_resp3(series));
            } else if (predicates->limitLabelsSize > 0) {
                ListRecord_Add(list_record,
                               ListSeriesLabelsWithLimit_rep3(series,
                                                              limitLabelsStr,
                                                              predicates->limitLabels,
                                                              predicates->limitLabelsSize));
            } else {
                ListRecord_Add(list_record, MapRecord_Create(0));
            }

            ListRecord_Add(
                list_record,
                ListWithSeriesLastDatapoint(series, predicates->latest, predicates->resp3));

            RedisModule_CloseKey(key);
            ListRecord_Add(series_listOrMap, list_record);
        } else {
            Record *key_record = ListRecord_Create(3);
            ListRecord_Add(key_record,
                           StringRecord_Create(strndup(currentKey, currentKeyLen), currentKeyLen));
            if (predicates->withLabels) {
                ListRecord_Add(key_record, ListSeriesLabels(series));
            } else if (predicates->limitLabelsSize > 0) {
                ListRecord_Add(key_record,
                               ListSeriesLabelsWithLimit(series,
                                                         limitLabelsStr,
                                                         predicates->limitLabels,
                                                         predicates->limitLabelsSize));
            } else {
                ListRecord_Add(key_record, ListRecord_Create(0));
            }

            ListRecord_Add(
                key_record,
                ListWithSeriesLastDatapoint(series, predicates->latest, predicates->resp3));

            RedisModule_CloseKey(key);
            ListRecord_Add(series_listOrMap, key_record);
        }
    }
    RedisModule_DictIteratorStop(iter);
    RedisModule_FreeDict(rts_staticCtx, result);
    free(limitLabelsStr);
    RedisModule_ThreadSafeContextUnlock(rts_staticCtx);

    return series_listOrMap;
}

Record *ShardQueryindexMapper(ExecutionCtx *rctx, void *arg) {
    QueryPredicates_Arg *predicates = arg;

    if (predicates->shouldReturnNull) {
        return NULL;
    }
    predicates->shouldReturnNull = true;

    RedisModule_ThreadSafeContextLock(rts_staticCtx);

    // The permission error is ignored.
    RedisModuleDict *result = QueryIndex(
        rts_staticCtx, predicates->predicates->list, predicates->predicates->count, NULL);

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(result, "^", NULL, 0);
    char *currentKey;
    size_t currentKeyLen;

    Record *series_list = ListRecord_Create(0);

    while ((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        ListRecord_Add(series_list,
                       StringRecord_Create(strndup(currentKey, currentKeyLen), currentKeyLen));
    }
    RedisModule_DictIteratorStop(iter);
    RedisModule_FreeDict(rts_staticCtx, result);
    RedisModule_ThreadSafeContextUnlock(rts_staticCtx);

    return series_list;
}

static MRObjectType *MR_CreateType(char *type,
                                   ObjectFree free,
                                   ObjectDuplicate dup,
                                   ObjectSerialize serialize,
                                   ObjectDeserialize deserialize,
                                   ObjectToString tostring) {
    MRObjectType *ret = malloc(sizeof(*ret));
    *ret = (MRObjectType){
        .type = strdup(type),
        .free = free,
        .dup = dup,
        .serialize = serialize,
        .deserialize = deserialize,
        .tostring = tostring,
    };
    return ret;
}

static MRRecordType *MR_RecordTypeCreate(char *type,
                                         ObjectFree free,
                                         ObjectDuplicate dup,
                                         ObjectSerialize serialize,
                                         ObjectDeserialize deserialize,
                                         ObjectToString tostring,
                                         SendAsRedisReply sendReply,
                                         HashTag hashTag) {
    MRRecordType *ret = malloc(sizeof *ret);
    *ret = (MRRecordType){
        .type = {
            .type = strdup(type),
            .free = free,
            .dup = dup,
            .serialize = serialize,
            .deserialize = deserialize,
            .tostring = tostring,
        },
        .sendReply = sendReply,
        .hashTag = hashTag,
    };
    return ret;
}

static Record *MR_RecordCreate(MRRecordType *type, size_t size) {
    Record *ret = malloc(size);
    ret->recordType = type;
    return ret;
}

int register_rg(RedisModuleCtx *ctx, long long numThreads) {
    if (MR_Init(ctx, numThreads, TSGlobalConfig.password) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "Failed to init LibMR. aborting...");
        return REDISMODULE_ERR;
    }

    // TODO: free the types later.

    MRObjectType *QueryPredicatesType = MR_CreateType("QueryPredicatesType",
                                                      QueryPredicates_ObjectFree,
                                                      QueryPredicates_Duplicate,
                                                      QueryPredicates_ArgSerialize,
                                                      QueryPredicates_ArgDeserialize,
                                                      QueryPredicates_ToString);

    if (MR_RegisterObject(QueryPredicatesType) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    listRecordType = MR_RecordTypeCreate("ListRecord",
                                         ListRecord_Free,
                                         NULL,
                                         ListRecord_Serialize,
                                         ListRecord_Deserialize,
                                         NULL,
                                         ListRecord_SendReply,
                                         NULL);

    if (MR_RegisterRecord(listRecordType) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    mapRecordType = MR_RecordTypeCreate("MapRecord",
                                        MapRecord_Free,
                                        NULL,
                                        MapRecord_Serialize,
                                        MapRecord_Deserialize,
                                        NULL,
                                        MapRecord_SendReply,
                                        NULL);

    if (MR_RegisterRecord(mapRecordType) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    stringRecordType = MR_RecordTypeCreate("StringRecord",
                                           StringRecord_Free,
                                           NULL,
                                           StringRecord_Serialize,
                                           StringRecord_Deserialize,
                                           NULL,
                                           StringRecord_SendReply,
                                           NULL);

    if (MR_RegisterRecord(stringRecordType) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    nullRecordType = MR_RecordTypeCreate("NullRecord",
                                         NullRecord_Free,
                                         NULL,
                                         NullRecord_Serialize,
                                         NullRecord_Deserialize,
                                         NULL,
                                         NullRecord_SendReply,
                                         NULL);

    if (MR_RegisterRecord(nullRecordType) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    NullRecord.recordType = nullRecordType;

    SeriesRecordType = MR_RecordTypeCreate(SeriesRecordName,
                                           SeriesRecord_ObjectFree,
                                           NULL,
                                           SeriesRecord_Serialize,
                                           SeriesRecord_Deserialize,
                                           NULL,
                                           SeriesRecord_SendReply,
                                           NULL);

    if (MR_RegisterRecord(SeriesRecordType) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    LongRecordType = MR_RecordTypeCreate("LongRecord",
                                         LongRecord_Free,
                                         NULL,
                                         LongRecord_Serialize,
                                         LongRecord_Deserialize,
                                         NULL,
                                         LongRecord_SendReply,
                                         NULL);

    if (MR_RegisterRecord(LongRecordType) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    DoubleRecordType = MR_RecordTypeCreate("DoubleRecord",
                                           DoubleRecord_Free,
                                           NULL,
                                           DoubleRecord_Serialize,
                                           DoubleRecord_Deserialize,
                                           NULL,
                                           DoubleRecord_SendReply,
                                           NULL);

    if (MR_RegisterRecord(DoubleRecordType) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    MR_RegisterReader("ShardSeriesMapper", ShardSeriesMapper, QueryPredicatesType);

    MR_RegisterReader("ShardMgetMapper", ShardMgetMapper, QueryPredicatesType);

    MR_RegisterReader("ShardQueryindexMapper", ShardQueryindexMapper, QueryPredicatesType);

    return REDISMODULE_OK;
}

bool IsMRCluster() {
    return MR_ClusterIsInClusterMode();
}

static void StringRecord_Free(void *base) {
    StringRecord *record = (StringRecord *)base;
    free(record->str);
    free(record);
}

static void StringRecord_Serialize(WriteSerializationCtx *sctx, void *base, MRError **error) {
    StringRecord *r = (StringRecord *)base;
    MR_SerializationCtxWriteBuffer(sctx, r->str, r->len, error);
}

static void *StringRecord_Deserialize(ReaderSerializationCtx *sctx, MRError **error) {
    size_t size;
    const char *temp = MR_SerializationCtxReadBuffer(sctx, &size, error);
    char *temp1 = malloc(size);
    memcpy(temp1, temp, size);
    return StringRecord_Create(temp1, size);
}

static char *StringRecordGet(Record *base, size_t *len) {
    StringRecord *r = (StringRecord *)base;
    if (len) {
        *len = r->len;
    }
    return r->str;
}

static void StringRecord_SendReply(RedisModuleCtx *rctx, void *r) {
    size_t listLen;
    char *str = StringRecordGet(r, &listLen);
    RedisModule_ReplyWithStringBuffer(rctx, str, listLen);
}

static Record *StringRecord_Create(char *val, size_t len) {
    StringRecord *ret = (StringRecord *)MR_RecordCreate(stringRecordType, sizeof(*ret));
    ret->str = val;
    ret->len = len;
    return &ret->base;
}

static size_t MapRecord_Len(Record *base) {
    MapRecord *r = (MapRecord *)base;
    return array_len(r->records);
}

static Record *MapRecord_Get(Record *base, size_t index) {
    RedisModule_Assert(MapRecord_Len(base) > index);
    MapRecord *r = (MapRecord *)base;
    return r->records[index];
}

static void MapRecord_SendReply(RedisModuleCtx *rctx, void *record) {
    size_t mapLen = MapRecord_Len(record);
    RedisModule_ReplyWithMap(rctx, mapLen / 2);
    for (int i = 0; i < mapLen; ++i) {
        Record *r = MapRecord_Get(record, i);
        r->recordType->sendReply(rctx, r);
    }
}

Record *MapRecord_GetRecord(MapRecord *record, size_t index) {
    return array_elem(record->records, index);
}

size_t MapRecord_GetLen(MapRecord *record) {
    return array_len(record->records);
}

static Record *MapRecord_Create(size_t initSize) {
    MapRecord *ret = (MapRecord *)MR_RecordCreate(mapRecordType, sizeof(*ret));
    ret->records = array_new(Record *, initSize);
    return &ret->base;
}

static void MapRecord_Free(void *base) {
    MapRecord *record = (MapRecord *)base;
    for (size_t i = 0; i < MapRecord_Len(base); ++i) {
        MR_RecordFree(record->records[i]);
    }
    array_free(record->records);
    free(record);
}

static void MapRecord_Add(Record *base, Record *element) {
    MapRecord *r = (MapRecord *)base;
    r->records = array_append(r->records, element);
}

static void MapRecord_Serialize(WriteSerializationCtx *sctx, void *arg, MRError **error) {
    MapRecord *r = (MapRecord *)arg;
    MR_SerializationCtxWriteLongLong(sctx, MapRecord_Len(arg), error);
    for (size_t i = 0; i < MapRecord_Len(arg); ++i) {
        MR_RecordSerialize(r->records[i], sctx);
    }
}

static void *MapRecord_Deserialize(ReaderSerializationCtx *sctx, MRError **error) {
    size_t size = (size_t)MR_SerializationCtxReadLongLong(sctx, error);
    Record *r = MapRecord_Create(size);
    for (size_t i = 0; i < size; ++i) {
        MapRecord_Add(r, MR_RecordDeSerialize(sctx));
    }
    return r;
}

static size_t ListRecord_Len(Record *base) {
    ListRecord *r = (ListRecord *)base;
    return array_len(r->records);
}

static Record *ListRecord_Get(Record *base, size_t index) {
    RedisModule_Assert(ListRecord_Len(base) > index);
    ListRecord *r = (ListRecord *)base;
    return r->records[index];
}

static void ListRecord_SendReply(RedisModuleCtx *rctx, void *record) {
    size_t listLen = ListRecord_Len(record);
    RedisModule_ReplyWithArray(rctx, listLen);
    for (int i = 0; i < listLen; ++i) {
        Record *r = ListRecord_Get(record, i);
        r->recordType->sendReply(rctx, r);
    }
}

Record *ListRecord_GetRecord(ListRecord *record, size_t index) {
    return array_elem(record->records, index);
}

size_t ListRecord_GetLen(ListRecord *record) {
    return array_len(record->records);
}

static Record *ListRecord_Create(size_t initSize) {
    ListRecord *ret = (ListRecord *)MR_RecordCreate(listRecordType, sizeof(*ret));
    ret->records = array_new(Record *, initSize);
    return &ret->base;
}

static void ListRecord_Free(void *base) {
    ListRecord *record = (ListRecord *)base;
    for (size_t i = 0; i < ListRecord_Len(base); ++i) {
        MR_RecordFree(record->records[i]);
    }
    array_free(record->records);
    free(record);
}

static void ListRecord_Add(Record *base, Record *element) {
    ListRecord *r = (ListRecord *)base;
    r->records = array_append(r->records, element);
}

static void ListRecord_Serialize(WriteSerializationCtx *sctx, void *arg, MRError **error) {
    ListRecord *r = (ListRecord *)arg;
    MR_SerializationCtxWriteLongLong(sctx, ListRecord_Len(arg), error);
    for (size_t i = 0; i < ListRecord_Len(arg); ++i) {
        MR_RecordSerialize(r->records[i], sctx);
    }
}

static void *ListRecord_Deserialize(ReaderSerializationCtx *sctx, MRError **error) {
    size_t size = (size_t)MR_SerializationCtxReadLongLong(sctx, error);
    Record *r = ListRecord_Create(size);
    for (size_t i = 0; i < size; ++i) {
        ListRecord_Add(r, MR_RecordDeSerialize(sctx));
    }
    return r;
}

Record *SeriesRecord_New(Series *series,
                         timestamp_t startTimestamp,
                         timestamp_t endTimestamp,
                         const QueryPredicates_Arg *predicates) {
    SeriesRecord *out = (SeriesRecord *)MR_RecordCreate(SeriesRecordType, sizeof(*out));
    out->keyName = RedisModule_CreateStringFromString(NULL, series->keyName);
    if (series->options & SERIES_OPT_UNCOMPRESSED) {
        out->chunkType = CHUNK_REGULAR;
    } else {
        out->chunkType = CHUNK_COMPRESSED;
    }
    out->funcs = series->funcs;
    out->labelsCount = series->labelsCount;
    out->labels = calloc(series->labelsCount, sizeof(Label));
    for (int i = 0; i < series->labelsCount; i++) {
        out->labels[i].key = RedisModule_CreateStringFromString(NULL, series->labels[i].key);
        out->labels[i].value = RedisModule_CreateStringFromString(NULL, series->labels[i].value);
    }

    // clone chunks
    out->chunks = calloc(RedisModule_DictSize(series->chunks) + 1,
                         sizeof(Chunk_t *)); // + 1 in case of latest flag
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(series->chunks, "^", NULL, 0);
    Chunk_t *chunk = NULL;
    int index = 0;
    while (RedisModule_DictNextC(iter, NULL, &chunk)) {
        if (series->funcs->GetNumOfSample(chunk) == 0) {
            if (unlikely(series->totalSamples != 0)) { // empty chunks are being removed
                RedisModule_Log(
                    mr_staticCtx, "error", "Empty chunk in a non empty series is invalid");
            }
            break;
        }
        if (series->funcs->GetLastTimestamp(chunk) >= startTimestamp) {
            if (series->funcs->GetFirstTimestamp(chunk) > endTimestamp) {
                break;
            }

            out->chunks[index] = out->funcs->CloneChunk(chunk);
            index++;
        }
    }

    if (should_finalize_last_bucket(predicates, series)) {
        Sample sample;
        Sample *sample_ptr = &sample;
        calculate_latest_sample(&sample_ptr, series);
        if (sample_ptr && (sample.timestamp <= endTimestamp)) {
            out->chunks[index] = out->funcs->NewChunk(128);
            series->funcs->AddSample(out->chunks[index], &sample);
            index++;
        }
    }
    out->chunkCount = index;
    RedisModule_DictIteratorStop(iter);
    return &out->base;
}

void SeriesRecord_ObjectFree(void *record) {
    SeriesRecord *series = record;
    for (int i = 0; i < series->labelsCount; i++) {
        RedisModule_FreeString(NULL, series->labels[i].key);
        RedisModule_FreeString(NULL, series->labels[i].value);
    }
    free(series->labels);

    for (int i = 0; i < series->chunkCount; i++) {
        series->funcs->FreeChunk(series->chunks[i]);
    }

    free(series->chunks);
    RedisModule_FreeString(NULL, series->keyName);
    free(series);
}

void SeriesRecord_Serialize(WriteSerializationCtx *sctx, void *arg, MRError **error) {
    SeriesRecord *series = (SeriesRecord *)arg;
    MR_SerializationCtxWriteLongLong(sctx, series->chunkType, error);
    SerializationCtxWriteRedisString(sctx, series->keyName, error);
    MR_SerializationCtxWriteLongLong(sctx, series->labelsCount, error);
    for (int i = 0; i < series->labelsCount; i++) {
        SerializationCtxWriteRedisString(sctx, series->labels[i].key, error);
        SerializationCtxWriteRedisString(sctx, series->labels[i].value, error);
    }

    MR_SerializationCtxWriteLongLong(sctx, series->chunkCount, error);
    for (int i = 0; i < series->chunkCount; i++) {
        series->funcs->MRSerialize(series->chunks[i], sctx);
    }
}

void *SeriesRecord_Deserialize(ReaderSerializationCtx *sctx, MRError **error) {
    SeriesRecord *series = (SeriesRecord *)MR_RecordCreate(SeriesRecordType, sizeof(*series));
    series->chunkType = MR_SerializationCtxReadLongLong(sctx, error);
    series->funcs = GetChunkClass(series->chunkType);
    series->keyName = SerializationCtxReadeRedisString(sctx, error);
    series->labelsCount = MR_SerializationCtxReadLongLong(sctx, error);
    series->labels = calloc(series->labelsCount, sizeof(Label));
    for (int i = 0; i < series->labelsCount; i++) {
        series->labels[i].key = SerializationCtxReadeRedisString(sctx, error);
        series->labels[i].value = SerializationCtxReadeRedisString(sctx, error);
    }

    series->chunkCount = MR_SerializationCtxReadLongLong(sctx, error);
    series->chunks = calloc(series->chunkCount, sizeof(Chunk_t *));
    for (int i = 0; i < series->chunkCount; i++) {
        series->funcs->MRDeserialize(&series->chunks[i], sctx);
    }
    return &series->base;
}

void SeriesRecord_SendReply(RedisModuleCtx *rctx, void *record) {
    SeriesRecord *series = (SeriesRecord *)record;
    RedisModule_ReplyWithArray(rctx, 3);
    RedisModule_ReplyWithString(rctx, series->keyName);
    RedisModule_ReplyWithLongLong(rctx, series->chunkCount);
    RedisModule_ReplyWithLongLong(rctx, series->labelsCount);
}

Series *SeriesRecord_IntoSeries(SeriesRecord *record) {
    CreateCtx createArgs = { 0 };
    createArgs.skipChunkCreation = true;
    Series *s = NewSeries(RedisModule_CreateStringFromString(NULL, record->keyName), &createArgs);
    s->labelsCount = record->labelsCount;
    s->labels = calloc(s->labelsCount, sizeof(Label));
    for (int i = 0; i < s->labelsCount; i++) {
        s->labels[i].key = RedisModule_CreateStringFromString(NULL, record->labels[i].key);
        s->labels[i].value = RedisModule_CreateStringFromString(NULL, record->labels[i].value);
    }
    s->funcs = record->funcs;

    Chunk_t *chunk = NULL;
    for (int chunk_index = 0; chunk_index < record->chunkCount; chunk_index++) {
        chunk = record->chunks[chunk_index];
        s->totalSamples += s->funcs->GetNumOfSample(chunk);
        dictOperator(s->chunks,
                     s->funcs->CloneChunk(chunk),
                     record->funcs->GetFirstTimestamp(chunk),
                     DICT_OP_SET);
    }
    if (chunk != NULL) {
        s->lastTimestamp = s->funcs->GetLastTimestamp(chunk);
    }
    return s;
}

static void DoubleRecord_Free(void *arg) {
    free(arg);
}

static void DoubleRecord_Serialize(WriteSerializationCtx *sctx, void *arg, MRError **error) {
    DoubleRecord *r = (DoubleRecord *)arg;
    MR_SerializationCtxWriteDouble(sctx, r->num, error);
}

static void *DoubleRecord_Deserialize(ReaderSerializationCtx *sctx, MRError **error) {
    return DoubleRecord_Create(MR_SerializationCtxReadDouble(sctx, error));
}

static double DoubleRecordGet(Record *base) {
    RedisModule_Assert(base->recordType == DoubleRecordType);
    DoubleRecord *r = (DoubleRecord *)base;
    return r->num;
}

static void DoubleRecord_SendReply(RedisModuleCtx *rctx, void *r) {
    RedisModule_ReplyWithDouble(rctx, DoubleRecordGet(r));
}

static Record *DoubleRecord_Create(double val) {
    DoubleRecord *ret = (DoubleRecord *)MR_RecordCreate(DoubleRecordType, sizeof(*ret));
    ret->num = val;
    return &ret->base;
}

static void LongRecord_Free(void *arg) {
    free(arg);
}

static void LongRecord_Serialize(WriteSerializationCtx *sctx, void *arg, MRError **error) {
    LongRecord *r = (LongRecord *)arg;
    MR_SerializationCtxWriteLongLong(sctx, r->num, error);
}

static void *LongRecord_Deserialize(ReaderSerializationCtx *sctx, MRError **error) {
    return LongRecord_Create(MR_SerializationCtxReadLongLong(sctx, error));
}

static long LongRecordGet(Record *base) {
    RedisModule_Assert(base->recordType == LongRecordType);
    LongRecord *r = (LongRecord *)base;
    return r->num;
}

static void LongRecord_SendReply(RedisModuleCtx *rctx, void *r) {
    RedisModule_ReplyWithLongLong(rctx, LongRecordGet(r));
}

static Record *LongRecord_Create(long val) {
    LongRecord *ret = (LongRecord *)MR_RecordCreate(LongRecordType, sizeof(*ret));
    ret->num = val;
    return &ret->base;
}

static void NullRecord_SendReply(RedisModuleCtx *rctx, void *base) {
    RedisModule_ReplyWithNull(rctx);
}

static void NullRecord_Serialize(WriteSerializationCtx *sctx, void *arg, MRError **error) {
    return;
}

static void *NullRecord_Deserialize(ReaderSerializationCtx *sctx, MRError **error) {
    return &NullRecord;
}

static void NullRecord_Free(void *base) {}
