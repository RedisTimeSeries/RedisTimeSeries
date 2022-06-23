
#include "libmr_integration.h"

#include "LibMR/src/mr.h"
#include "LibMR/src/record.h"
#include "LibMR/src/utils/arr.h"
#include "RedisModulesSDK/redismodule.h"
#include "consts.h"
#include "generic_chunk.h"
#include "indexer.h"
#include "module.h"
#include "query_language.h"
#include "tsdb.h"

#include "rmutil/alloc.h"

#define SeriesRecordName "SeriesRecord"

static Record NullRecord;
static MRRecordType *nullRecordType = NULL;
static MRRecordType *stringRecordType = NULL;
static MRRecordType *listRecordType = NULL;
static MRRecordType *SeriesRecordType = NULL;
static MRRecordType *LongRecordType = NULL;

static Record *GetNullRecord() {
    return &NullRecord;
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

static Record *LongRecord_Create(long val);
static void ListRecord_Free(void *base);
static void ListRecord_Add(Record *base, Record *element);
static void *ListRecord_Deserialize(ReaderSerializationCtx *sctx, MRError **error);
static void ListRecord_Serialize(WriteSerializationCtx *sctx, void *arg, MRError **error);
static void ListRecord_SendReply(RedisModuleCtx *rctx, void *record);
static void StringRecord_Free(void *base);
static void StringRecord_Serialize(WriteSerializationCtx *sctx, void *base, MRError **error);
static void *StringRecord_Deserialize(ReaderSerializationCtx *sctx, MRError **error);
static void StringRecord_SendReply(RedisModuleCtx *rctx, void *r);
static void NullRecord_SendReply(RedisModuleCtx *rctx, void *base);
static void NullRecord_Serialize(WriteSerializationCtx *sctx, void *arg, MRError **error);
static void *NullRecord_Deserialize(ReaderSerializationCtx *sctx, MRError **error);
static void NullRecord_Free(void *base);
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
    const char *temp = MR_SerializationCtxReadeBuffer(sctx, &len, error);
    return RedisModule_CreateString(NULL, temp, len - 1);
}

static void *QueryPredicates_ArgDeserialize(ReaderSerializationCtx *sctx, MRError **error) {
    QueryPredicates_Arg *predicates = malloc(sizeof(*predicates));
    predicates->shouldReturnNull = false;
    predicates->refCount = 1;
    predicates->predicates = malloc(sizeof(QueryPredicateList));
    predicates->predicates->count = MR_SerializationCtxReadeLongLong(sctx, error);
    predicates->predicates->ref = 1;
    predicates->withLabels = MR_SerializationCtxReadeLongLong(sctx, error);
    predicates->limitLabelsSize = MR_SerializationCtxReadeLongLong(sctx, error);
    predicates->startTimestamp = MR_SerializationCtxReadeLongLong(sctx, error);
    predicates->endTimestamp = MR_SerializationCtxReadeLongLong(sctx, error);

    predicates->limitLabels = calloc(predicates->limitLabelsSize, sizeof(char **));
    for (int i = 0; i < predicates->limitLabelsSize; ++i) {
        predicates->limitLabels[i] = SerializationCtxReadeRedisString(sctx, error);
    }

    predicates->predicates->list = calloc(predicates->predicates->count, sizeof(QueryPredicate));
    for (int i = 0; i < predicates->predicates->count; i++) {
        QueryPredicate *predicate = predicates->predicates->list + i;
        // decode type
        predicate->type = MR_SerializationCtxReadeLongLong(sctx, error);

        // decode key
        predicate->key = SerializationCtxReadeRedisString(sctx, error);

        // decode values
        predicate->valueListCount = MR_SerializationCtxReadeLongLong(sctx, error);
        predicate->valuesList = calloc(predicate->valueListCount, sizeof(RedisModuleString *));

        for (int value_index = 0; value_index < predicate->valueListCount; value_index++) {
            predicate->valuesList[value_index] = SerializationCtxReadeRedisString(sctx, error);
        }
    }
    return predicates;
}

static Record *StringRecord_Create(char *val, size_t len);
static Record *ListRecord_Create(size_t initSize);

static Record *RedisStringRecord_Create(RedisModuleString *str) {
    size_t len = 0;
    const char *cstr = RedisModule_StringPtrLen(str, &len);
    return StringRecord_Create(strndup(cstr, len), len);
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
Record *ListWithSample(u_int64_t timestamp, double value) {
    Record *r = ListRecord_Create(2);
    ListRecord_Add(r, LongRecord_Create(timestamp));
    char buf[MAX_VAL_LEN];
    snprintf(buf, MAX_VAL_LEN, "%.15g", value);
    ListRecord_Add(r, StringRecord_Create(strdup(buf), strlen(buf)));
    return r;
}

Record *ListWithSeriesLastDatapoint(const Series *series) {
    if (SeriesGetNumSamples(series) == 0) {
        return ListRecord_Create(0);
    } else {
        return ListWithSample(series->lastTimestamp, series->lastValue);
    }
}

Record *ShardSeriesMapper(ExecutionCtx *rctx, void *arg) {
    QueryPredicates_Arg *predicates = arg;

    if (predicates->shouldReturnNull) {
        return NULL;
    }
    predicates->shouldReturnNull = true;

    RedisModule_ThreadSafeContextLock(rts_staticCtx);

    RedisModuleDict *result =
        QueryIndex(rts_staticCtx, predicates->predicates->list, predicates->predicates->count);

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(result, "^", NULL, 0);
    char *currentKey;
    size_t currentKeyLen;

    Series *series;
    Record *series_list = ListRecord_Create(0);

    while ((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        RedisModuleKey *key;
        RedisModuleString *keyName =
            RedisModule_CreateString(rts_staticCtx, currentKey, currentKeyLen);
        const int status =
            GetSeries(rts_staticCtx, keyName, &key, &series, REDISMODULE_READ, false, true);

        RedisModule_FreeString(rts_staticCtx, keyName);

        if (!status) {
            RedisModule_Log(rts_staticCtx,
                            "warning",
                            "couldn't open key or key is not a Timeseries. key=%.*s",
                            (int)currentKeyLen,
                            currentKey);
            continue;
        }

        bool should_finalize_last_bucket = false;
        RedisModuleKey *srcKey;
        Series *srcSeries;

        // LATEST is ignored for a series that is not a compaction.
        should_finalize_last_bucket = predicates->latest && series->srcKey &&
                                      predicates->endTimestamp > series->lastTimestamp;
        if (should_finalize_last_bucket) {
            // temporarily close the last bucket of the src series and write it to dest
            const int status = GetSeries(
                rts_staticCtx, series->srcKey, &srcKey, &srcSeries, REDISMODULE_READ, false, true);
            if (!status) {
                // LATEST is ignored for a series that is not a compaction.
                should_finalize_last_bucket = false;
            } else {
                finalize_last_bucket(srcSeries, series);
            }
        }

        ListRecord_Add(
            series_list,
            SeriesRecord_New(series, predicates->startTimestamp, predicates->endTimestamp));

        if (should_finalize_last_bucket) {
            if (srcSeries->totalSamples > 0) {
                CompactionRule *rule = find_rule(srcSeries->rules, series->keyName);
                SeriesDelRange(series, rule->startCurrentTimeBucket, rule->startCurrentTimeBucket);
            }
            RedisModule_CloseKey(srcKey);
        }

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

    RedisModuleDict *result =
        QueryIndex(rts_staticCtx, predicates->predicates->list, predicates->predicates->count);

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(result, "^", NULL, 0);
    char *currentKey;
    size_t currentKeyLen;

    Series *series;
    Record *series_list = ListRecord_Create(0);

    while ((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        RedisModuleKey *key;
        RedisModuleString *keyName =
            RedisModule_CreateString(rts_staticCtx, currentKey, currentKeyLen);
        const int status =
            GetSeries(rts_staticCtx, keyName, &key, &series, REDISMODULE_READ, false, true);
        RedisModule_FreeString(rts_staticCtx, keyName);

        if (!status) {
            RedisModule_Log(rts_staticCtx,
                            "warning",
                            "couldn't open key or key is not a Timeseries. key=%.*s",
                            (int)currentKeyLen,
                            currentKey);
            continue;
        }

        Record *key_record = ListRecord_Create(3);
        ListRecord_Add(key_record,
                       StringRecord_Create(strndup(currentKey, currentKeyLen), currentKeyLen));
        if (predicates->withLabels) {
            ListRecord_Add(key_record, ListSeriesLabels(series));
        } else if (predicates->limitLabelsSize > 0) {
            ListRecord_Add(
                key_record,
                ListSeriesLabelsWithLimit(
                    series, limitLabelsStr, predicates->limitLabels, predicates->limitLabelsSize));
        } else {
            ListRecord_Add(key_record, ListRecord_Create(0));
        }
        ListRecord_Add(key_record, ListWithSeriesLastDatapoint(series));
        RedisModule_CloseKey(key);
        ListRecord_Add(series_list, key_record);
    }
    RedisModule_DictIteratorStop(iter);
    RedisModule_FreeDict(rts_staticCtx, result);
    free(limitLabelsStr);
    RedisModule_ThreadSafeContextUnlock(rts_staticCtx);

    return series_list;
}

Record *ShardQueryindexMapper(ExecutionCtx *rctx, void *arg) {
    QueryPredicates_Arg *predicates = arg;

    if (predicates->shouldReturnNull) {
        return NULL;
    }
    predicates->shouldReturnNull = true;

    RedisModule_ThreadSafeContextLock(rts_staticCtx);

    RedisModuleDict *result =
        QueryIndex(rts_staticCtx, predicates->predicates->list, predicates->predicates->count);

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
    MRRecordType *ret = malloc(sizeof(MRRecordType));
    *ret = (MRRecordType){ .type =
                               (MRObjectType){
                                   .type = strdup(type),
                                   .free = free,
                                   .dup = dup,
                                   .serialize = serialize,
                                   .deserialize = deserialize,
                                   .tostring = tostring,
                               },
                           .sendReply = sendReply,
                           .hashTag = hashTag };
    return ret;
}

static Record *MR_RecordCreate(MRRecordType *type, size_t size) {
    Record *ret = malloc(size);
    ret->recordType = type;
    return ret;
}

int register_rg(RedisModuleCtx *ctx, long long numThreads) {
    if (MR_Init(ctx, numThreads) != REDISMODULE_OK) {
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
    const char *temp = MR_SerializationCtxReadeBuffer(sctx, &size, error);
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
    size_t size = (size_t)MR_SerializationCtxReadeLongLong(sctx, error);
    Record *r = ListRecord_Create(size);
    for (size_t i = 0; i < size; ++i) {
        ListRecord_Add(r, MR_RecordDeSerialize(sctx));
    }
    return r;
}

Record *SeriesRecord_New(Series *series, timestamp_t startTimestamp, timestamp_t endTimestamp) {
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
    out->chunks = calloc(RedisModule_DictSize(series->chunks), sizeof(Chunk_t *));
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
        if (series->funcs->GetLastTimestamp(chunk) > startTimestamp) {
            if (series->funcs->GetFirstTimestamp(chunk) > endTimestamp) {
                break;
            }

            out->chunks[index] = out->funcs->CloneChunk(chunk);
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
    series->chunkType = MR_SerializationCtxReadeLongLong(sctx, error);
    series->funcs = GetChunkClass(series->chunkType);
    series->keyName = SerializationCtxReadeRedisString(sctx, error);
    series->labelsCount = MR_SerializationCtxReadeLongLong(sctx, error);
    series->labels = calloc(series->labelsCount, sizeof(Label));
    for (int i = 0; i < series->labelsCount; i++) {
        series->labels[i].key = SerializationCtxReadeRedisString(sctx, error);
        series->labels[i].value = SerializationCtxReadeRedisString(sctx, error);
    }

    series->chunkCount = MR_SerializationCtxReadeLongLong(sctx, error);
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

static void LongRecord_Free(void *arg) {
    free(arg);
}

static void LongRecord_Serialize(WriteSerializationCtx *sctx, void *arg, MRError **error) {
    LongRecord *r = (LongRecord *)arg;
    MR_SerializationCtxWriteLongLong(sctx, r->num, error);
}

static void *LongRecord_Deserialize(ReaderSerializationCtx *sctx, MRError **error) {
    return LongRecord_Create(MR_SerializationCtxReadeLongLong(sctx, error));
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
