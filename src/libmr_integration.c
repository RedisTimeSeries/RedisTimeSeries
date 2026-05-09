#include "libmr_integration.h"

#include "LibMR/src/mr.h"
#include "LibMR/src/record.h"
#include "common.h"
#include "consts.h"
#include "generic_chunk.h"
#include "indexer.h"
#include "module.h"
#include "query_language.h"
#include "tsdb.h"
#include <math.h>
#include "reply.h"

#include "RedisModulesSDK/redismodule.h"
#include "rmutil/alloc.h"

#include "LibMR/deps/hiredis/hiredis.h"
#include "LibMR/src/utils/buffer.h"
#include "enriched_chunk.h"
#include "series_iterator.h"

// Initial capacities for the binary buffers used in internal-command responses.
// These are only starting hints — the buffer auto-grows via realloc as we write.
#define INTERNAL_REPLY_BUF_INITIAL_CAP 1024 // mrange/mget top-level reply
#define INTERNAL_REPLY_BUF_SMALL_INITIAL_CAP                                                       \
    256 // queryindex reply / per-series sample staging// Binary serialization helpers for internal
        // command responses.
// These replace the RESP encoding/decoding with direct binary blob transfers
#define SeriesRecordName "SeriesRecord"

/* All four must be present: Apply sets user from name; Release reads and clears via the same API
 * set. */
#define API_USER_CONTEXT_SUPPORTED                                                                 \
    (RedisModule_SetContextUser && RedisModule_GetContextUser &&                                   \
     RedisModule_GetModuleUserFromUserName && RedisModule_GetUserUsername)

// Per-series visitor state for TS_INTERNAL_MGET: writes name + (optional) labels +
// last/finalized sample to the binary buffer being built for the coordinator.
typedef struct
{
    mr_BufferWriter *bw;
    const MGetArgs *args;
} MgetBufCtx;

// Per-series visitor state for TS_INTERNAL_MRANGE: writes name + labels + range
// samples to the binary buffer being built for the coordinator.
typedef struct
{
    mr_BufferWriter *bw;
    const RangeArgs *rangeArgs;
    bool reverse;
} MrangeBufCtx;

static Record NullRecord;
static MRRecordType *NullRecordType = NULL;
static MRRecordType *StringRecordType = NULL;
static MRRecordType *ListRecordType = NULL;
static MRRecordType *SeriesRecordType = NULL;
static MRRecordType *LongRecordType = NULL;
static MRRecordType *DoubleRecordType = NULL;
static MRRecordType *MapRecordType = NULL;
static MRRecordType *SlotRangesRecordType = NULL;
static MRRecordType *SeriesListRecordType = NULL;
static MRRecordType *StringListRecordType = NULL;

static void QueryPredicates_FreeUserName(QueryPredicates_Arg *predicate_list) {
    if (predicate_list->userName) {
        RedisModule_FreeString(NULL, predicate_list->userName);
        predicate_list->userName = NULL;
    }
}

static void QueryPredicates_FreeLimitLabels(QueryPredicates_Arg *predicate_list) {
    if (!predicate_list->limitLabels) {
        return;
    }
    for (int i = 0; i < predicate_list->limitLabelsSize; i++) {
        if (predicate_list->limitLabels[i]) {
            RedisModule_FreeString(NULL, predicate_list->limitLabels[i]);
        }
    }
    free(predicate_list->limitLabels);
    predicate_list->limitLabels = NULL;
    predicate_list->limitLabelsSize = 0;
}

static Record *GetNullRecord() {
    return &NullRecord;
}

MRRecordType *GetMapRecordType() {
    return MapRecordType;
}

MRRecordType *GetListRecordType() {
    return ListRecordType;
}

MRRecordType *GetSeriesRecordType() {
    return SeriesRecordType;
}

MRRecordType *GetSlotRangesRecordType() {
    return SlotRangesRecordType;
}

MRRecordType *GetSeriesListRecordType() {
    return SeriesListRecordType;
}

MRRecordType *GetStringListRecordType() {
    return StringListRecordType;
}

static void QueryPredicates_ObjectFree(void *arg) {
    QueryPredicates_Arg *predicate_list = arg;

    if (__atomic_sub_fetch(&predicate_list->refCount, 1, __ATOMIC_RELAXED) > 0) {
        return;
    }

    QueryPredicateList_Free(predicate_list->predicates);
    QueryPredicates_FreeLimitLabels(predicate_list);
    QueryPredicates_FreeUserName(predicate_list);
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

// Internal command records
static Record *SlotRangesRecord_Create(RedisModuleSlotRangeArray *slotRanges);
static void SlotRangesRecord_Free(void *base);
static Record *SeriesListRecord_Create(ARR(Series *) seriesList);
static void SeriesListRecord_Free(void *base);
static Record *StringListRecord_Create(ARR(RedisModuleString *) stringList);
static void StringListRecord_Free(void *base);

static Record *RedisStringRecord_Create(RedisModuleString *str);

static void SerializationCtxWriteRedisString(WriteSerializationCtx *sctx,
                                             const RedisModuleString *arg,
                                             MRError **error);

static void QueryPredicates_ArgSerialize(WriteSerializationCtx *sctx, void *arg, MRError **error) {
    QueryPredicates_Arg *predicate_list = arg;
    /* Client username for ACL (empty string = default user). Retained on main thread like
     * limitLabels. */
    if (predicate_list->userName) {
        SerializationCtxWriteRedisString(sctx, predicate_list->userName, error);
    } else {
        /* For empty string, we write a single byte of 0. */
        MR_SerializationCtxWriteBuffer(sctx, "", 1, error);
    }
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

static RedisModuleString *SerializationCtxReadRedisString(ReaderSerializationCtx *sctx,
                                                          MRError **error) {
    size_t len;
    const char *temp = MR_SerializationCtxReadBuffer(sctx, &len, error);
    return RedisModule_CreateString(NULL, temp, len - 1);
}

static void QueryPredicates_CleanupFailedDeserialization(QueryPredicates_Arg *predicates) {
    QueryPredicates_FreeUserName(predicates);
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
    QueryPredicates_FreeLimitLabels(predicates);
    free(predicates);
}

static void *QueryPredicates_ArgDeserialize_impl(ReaderSerializationCtx *sctx,
                                                 MRError **error,
                                                 bool expect_resp) {
    QueryPredicates_Arg *predicates = calloc(1, sizeof *predicates);
    predicates->shouldReturnNull = false;
    predicates->refCount = 1;
    /* Username from wire as stored; empty vs NULL is interpreted only in ApplyCtxUser(). */
    predicates->userName = SerializationCtxReadRedisString(sctx, error);
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
        predicates->limitLabels[i] = SerializationCtxReadRedisString(sctx, error);
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
        predicate->key = SerializationCtxReadRedisString(sctx, error);
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
            predicate->valuesList[value_index] = SerializationCtxReadRedisString(sctx, error);
            if (unlikely(expect_resp && *error)) {
                goto err;
            }
        }
    }
    if (unlikely(expect_resp && *error)) {
        goto err;
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
                                       uint16_t limitLabelsSize) {
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
                                  uint16_t limitLabelsSize) {
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
        if (isnan(value)) {
            strcpy(buf, "NaN");
        } else {
            snprintf(buf, MAX_VAL_LEN, "%.15g", value);
        }
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

// Set the context user for ACL checks. Skips allocation if the context
// already has the same user set, to avoid redundant alloc+free cycles.
static void ApplyCtxUser(RedisModuleCtx *ctx, RedisModuleString *userName) {
    if (!API_USER_CONTEXT_SUPPORTED)
        return;

    size_t len = 0;

    RedisModule_Assert(userName != NULL);
    RedisModule_StringPtrLen(userName, &len);
    RedisModule_Assert(len > 0);

    // Check if the requested user is already set on the context
    const RedisModuleUser *currentUser = RedisModule_GetContextUser(ctx);
    if (currentUser) {
        RedisModuleString *currentName = RedisModule_GetUserUsername(ctx, currentUser);
        if (currentName) {
            const int cmp = RedisModule_StringCompare(currentName, userName);
            RedisModule_FreeString(ctx, currentName);
            if (cmp == 0) {
                return; // Same user already set, nothing to do
            }
        }
        RedisModule_FreeModuleUser((RedisModuleUser *)currentUser);
        RedisModule_SetContextUser(ctx, NULL);
    }

    // Allocate and set the new user on the context
    const RedisModuleUser *user = RedisModule_GetModuleUserFromUserName(userName);
    if (user) {
        RedisModule_SetContextUser(ctx, user);
    }
}

static void ReleaseCtxUser(RedisModuleCtx *ctx) {
    if (!API_USER_CONTEXT_SUPPORTED)
        return;
    RedisModuleUser *user = (RedisModuleUser *)RedisModule_GetContextUser(ctx);
    if (user) {
        RedisModule_FreeModuleUser(user);
        RedisModule_SetContextUser(ctx, NULL);
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
    ApplyCtxUser(rts_staticCtx, predicates->userName);

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
            RedisModule_Log(
                rts_staticCtx, "warning", "couldn't open key or key is not a Timeseries.");
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
    ReleaseCtxUser(rts_staticCtx);
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
    ApplyCtxUser(rts_staticCtx, predicates->userName);

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
            RedisModule_Log(
                rts_staticCtx, "warning", "couldn't open key or key is not a Timeseries.");
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
    ReleaseCtxUser(rts_staticCtx);
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

static void TS_INTERNAL_SLOT_RANGES(RedisModuleCtx *ctx, void *args) {
    RedisModuleSlotRangeArray *sra = RedisModule_ClusterGetLocalSlotRanges(ctx);
    if (sra == NULL) {
        // Should never happen, because this function is only called in clustered environment.
        // But to be on the safe side:
        RedisModule_ReplyWithArray(ctx, 0);
        return;
    }
    RedisModule_ReplyWithArray(ctx, sra->num_ranges);
    for (int i = 0; i < sra->num_ranges; i++) {
        RedisModule_ReplyWithArray(ctx, 2);
        RedisModule_ReplyWithLongLong(ctx, sra->ranges[i].start);
        RedisModule_ReplyWithLongLong(ctx, sra->ranges[i].end);
    }
    RedisModule_ClusterFreeSlotRanges(ctx, sra);
}

static Record *SlotRangesReplyParser(const redisReply *reply) {
    RedisModule_Assert(reply->type == REDIS_REPLY_ARRAY);
    size_t size =
        sizeof(RedisModuleSlotRangeArray) + reply->elements * sizeof(RedisModuleSlotRange);
    RedisModuleSlotRangeArray *slotRanges = malloc(size);
    slotRanges->num_ranges = reply->elements;
    for (size_t i = 0; i < slotRanges->num_ranges; i++) {
        const redisReply *range = reply->element[i];
        RedisModule_Assert(range->type == REDIS_REPLY_ARRAY && range->elements == 2);
        RedisModule_Assert(range->element[0]->type == REDIS_REPLY_INTEGER &&
                           range->element[1]->type == REDIS_REPLY_INTEGER);
        slotRanges->ranges[i].start = range->element[0]->integer;
        slotRanges->ranges[i].end = range->element[1]->integer;
    }

    return SlotRangesRecord_Create(slotRanges);
}

static InternalCommandCallbacks SlotRangesCallbacks = { .command = TS_INTERNAL_SLOT_RANGES,
                                                        .replyParser = SlotRangesReplyParser };

/**
 * @brief Serialize one (timestamp, value) sample into the shard's reply buffer.
 *
 * Used by ::BufWriteSeriesRange (TS_INTERNAL_MRANGE) and ::BufWriteMGetSample
 * (TS_INTERNAL_MGET). Format must match ::BufReadSample on the coordinator.
 */
static inline void BufWriteSample(mr_BufferWriter *bw, long long timestamp, double value) {
    mr_BufferWriterWriteLongLong(bw, timestamp);
    mr_BufferWriterWriteBuff(bw, (const char *)&value, sizeof(double));
}

/**
 * @brief Deserialize one (timestamp, value) sample from a shard's reply buffer.
 *
 * Used by ::DeserializeSeries on the coordinator. Format must match
 * ::BufWriteSample emitted by the shard.
 */
static inline void BufReadSample(mr_BufferReader *br,
                                 long long *timestamp,
                                 double *value,
                                 int *err) {
    *timestamp = mr_BufferReaderReadLongLong(br, err);
    size_t dlen;
    const char *dptr = mr_BufferReaderReadBuff(br, &dlen, err);
    if (dptr)
        memcpy(value, dptr, sizeof(double));
}

/**
 * @brief Serialize one label (key + optional value) into the shard's reply buffer.
 *
 * Used by ::BufWriteSeriesLabel and ::BufWriteSeriesLabelsWithLimit. Writes the
 * key, a hasValue flag, and the value when present; matched on the coordinator
 * by the label-reading loop in ::DeserializeSeries.
 */
static inline void BufWriteLabel(mr_BufferWriter *bw,
                                 const char *key,
                                 size_t klen,
                                 const char *value,
                                 size_t vlen,
                                 bool hasValue) {
    mr_BufferWriterWriteBuff(bw, key, klen);
    mr_BufferWriterWriteLongLong(bw, hasValue ? 1 : 0);
    if (hasValue)
        mr_BufferWriterWriteBuff(bw, value, vlen);
}

/**
 * @brief Serialize a series ::Label into the shard's reply buffer.
 *
 * Thin wrapper that extracts the key/value strings and forwards to ::BufWriteLabel.
 * Used by ::BufWriteSeriesLabels (WITHLABELS) and ::BufWriteSeriesLabelsWithLimit
 * (SELECTED_LABELS, when the requested label exists on the series).
 */
static inline void BufWriteSeriesLabel(mr_BufferWriter *bw, const Label *label) {
    size_t klen, vlen = 0;
    const char *k = RedisModule_StringPtrLen(label->key, &klen);
    bool hasValue = (label->value != NULL);
    const char *v = hasValue ? RedisModule_StringPtrLen(label->value, &vlen) : NULL;
    BufWriteLabel(bw, k, klen, v, vlen, hasValue);
}

/**
 * @brief Begin a shard reply whose first field is the total entry count.
 *
 * Allocates a buffer, writes a zero placeholder for the count, and stores its
 * offset in @p countOffset so ::CountedReplyFinish can patch it once the loop
 * has counted all entries. Used by TS_INTERNAL_MRANGE / MGET / QUERYINDEX.
 */
static mr_Buffer *CountedReplyBegin(mr_BufferWriter *bw, size_t initialCap, size_t *countOffset) {
    mr_Buffer *buf = mr_BufferNew(initialCap);
    mr_BufferWriterInit(bw, buf);
    *countOffset = buf->size;
    mr_BufferWriterWriteLongLong(bw, 0); // placeholder, patched in CountedReplyFinish
    return buf;
}

/**
 * @brief Finalize a counted reply: patch the entry count, send, and free.
 *
 * Overwrites the placeholder reserved by ::CountedReplyBegin at @p countOffset
 * with @p count, ships the buffer as one RESP bulk-string, and releases it.
 */
static void CountedReplyFinish(RedisModuleCtx *ctx,
                               mr_Buffer *buf,
                               size_t countOffset,
                               long long count) {
    long countPatch = (long)count;
    memcpy(buf->buff + countOffset, &countPatch, sizeof(long));
    RedisModule_ReplyWithStringBuffer(ctx, buf->buff, buf->size);
    mr_BufferFree(buf);
}

/**
 * @brief Wrap a hiredis bulk-string reply as an ::mr_BufferReader (zero-copy).
 *
 * Used by the coordinator-side reply parsers (::SeriesListReplyParser,
 * ::StringListReplyParser) to read the binary buffer the shard built. The
 * reader only borrows @p reply->str, so the returned reader is valid for the
 * lifetime of the hiredis reply.
 */
static inline void BufReaderInitFromReply(mr_BufferReader *br,
                                          mr_Buffer *buf,
                                          const redisReply *reply) {
    buf->cap = reply->len;
    buf->size = reply->len;
    buf->buff = reply->str;
    mr_BufferReaderInit(br, buf);
}

/**
 * @brief Serialize all of a series' labels into the shard's reply buffer.
 *
 * Writes a length-prefixed sequence: the label count followed by each label
 * via ::BufWriteSeriesLabel. Used for the WITHLABELS path of TS_INTERNAL_MGET
 * and unconditionally on the TS_INTERNAL_MRANGE path (labels are always sent
 * so the coordinator can group by them in mrange_done).
 */
static void BufWriteSeriesLabels(mr_BufferWriter *bw, const Series *series) {
    mr_BufferWriterWriteLongLong(bw, series->labelsCount);
    for (size_t i = 0; i < series->labelsCount; i++)
        BufWriteSeriesLabel(bw, &series->labels[i]);
}

/**
 * @brief Serialize the SELECTED_LABELS subset for a series into the reply buffer.
 *
 * Writes exactly @p numLimitLabels entries in the order requested by the client,
 * looking each name up case-insensitively in the series. If the series has the
 * label, it's emitted with its value; otherwise the name is emitted with
 * hasValue=false so the coordinator can report `(name, nil)` to the client.
 *
 * Used by TS_INTERNAL_MGET when the client passed SELECTED_LABELS.
 */
static void BufWriteSeriesLabelsWithLimit(mr_BufferWriter *bw,
                                          const Series *series,
                                          const char **limitLabels,
                                          uint16_t numLimitLabels) {
    mr_BufferWriterWriteLongLong(bw, numLimitLabels);
    for (int i = 0; i < numLimitLabels; i++) {
        const Label *match = NULL;
        for (size_t j = 0; j < series->labelsCount; j++) {
            const char *key = RedisModule_StringPtrLen(series->labels[j].key, NULL);
            if (strcasecmp(key, limitLabels[i]) == 0) {
                match = &series->labels[j];
                break;
            }
        }
        if (match) {
            BufWriteSeriesLabel(bw, match);
        } else {
            BufWriteLabel(bw, limitLabels[i], strlen(limitLabels[i]), NULL, 0, false);
        }
    }
}

/**
 * @brief Serialize all samples a range query yields for a series into the reply buffer.
 *
 * Writes a length-prefixed sample list: the sample count (known only after the
 * iterator is drained), followed by each sample via ::BufWriteSample. Samples
 * are staged in a side buffer first so the count can be written before the body
 * — this avoids the placeholder/patch dance used at the top level.
 *
 * Used by TS_INTERNAL_MRANGE per series in the dict.
 */
static void BufWriteSeriesRange(mr_BufferWriter *bw,
                                Series *series,
                                const RangeArgs *args,
                                bool reverse) {
    mr_Buffer *samplesBuf = mr_BufferNew(INTERNAL_REPLY_BUF_SMALL_INITIAL_CAP);
    mr_BufferWriter sw;
    mr_BufferWriterInit(&sw, samplesBuf);
    long long sampleCount = 0;
    long long maxCount = (args->count != -1) ? args->count : LLONG_MAX;

    AbstractIterator *iter = SeriesQuery(series, args, reverse, true);
    EnrichedChunk *enrichedChunk;
    while ((sampleCount < maxCount) && (enrichedChunk = iter->GetNext(iter))) {
        long long n = min(maxCount - sampleCount, (long long)enrichedChunk->samples.num_samples);
        for (long long i = 0; i < n; i++) {
            BufWriteSample(&sw,
                           enrichedChunk->samples.timestamps[i],
                           Samples_value_at(&enrichedChunk->samples, i, 0));
        }
        sampleCount += n;
    }
    iter->Close(iter);

    mr_BufferWriterWriteLongLong(bw, sampleCount);
    if (samplesBuf->size > 0)
        mr_BufferAdd(bw->buff, samplesBuf->buff, samplesBuf->size);
    mr_BufferFree(samplesBuf);
}

/**
 * @brief Serialize the single sample TS.MGET should return for a series.
 *
 * Picks the finalized last-bucket sample when LATEST is set on a compaction,
 * otherwise the series' last datapoint; writes nothing if the series is empty.
 * Format: a hasSample flag (0/1), followed by one ::BufWriteSample if present.
 *
 * Used by TS_INTERNAL_MGET per series.
 */
static void BufWriteMGetSample(mr_BufferWriter *bw, Series *series, bool latest) {
    long long ts;
    double val;
    bool hasSample = false;

    if (should_finalize_last_bucket_get(latest, series)) {
        Sample sample;
        Sample *sample_ptr = &sample;
        calculate_latest_sample(&sample_ptr, series);
        if (sample_ptr) {
            ts = sample.timestamp;
            val = sample.value;
            hasSample = true;
        }
    }
    if (!hasSample && SeriesGetNumSamples(series) > 0) {
        ts = series->lastTimestamp;
        val = series->lastValue;
        hasSample = true;
    }

    mr_BufferWriterWriteLongLong(bw, hasSample ? 1 : 0);
    if (hasSample)
        BufWriteSample(bw, ts, val);
}

/**
 * @brief Reconstruct one ::Series from a shard's binary reply on the coordinator.
 *
 * Reads the series in the order written by the shard: name, labels (count +
 * key/hasValue/value triples — see ::BufWriteLabel), then samples (count +
 * timestamp/value pairs — see ::BufWriteSample). Allocates fresh
 * ::RedisModuleString and chunk storage so the returned Series is independent
 * of the hiredis reply buffer (which is freed when the parser returns).
 *
 * Used by ::SeriesListReplyParser, the per-shard parser registered for
 * TS_INTERNAL_MRANGE / TS_INTERNAL_MGET.
 */
static Series *DeserializeSeries(mr_BufferReader *br) {
    int err = 0;

    size_t nameLen;
    const char *nameData = mr_BufferReaderReadBuff(br, &nameLen, &err);
    RedisModuleString *name = RedisModule_CreateString(rts_staticCtx, nameData, nameLen);

    long long labelsCount = mr_BufferReaderReadLongLong(br, &err);

    CreateCtx cCtx = { 0 };
    cCtx.chunkSizeBytes = TSGlobalConfig.chunkSizeBytes;
    cCtx.options = SERIES_OPT_COMPRESSED_GORILLA;
    cCtx.duplicatePolicy = DP_NONE;
    cCtx.labelsCount = labelsCount;
    cCtx.labels = labelsCount > 0 ? calloc(labelsCount, sizeof(Label)) : NULL;

    for (long long i = 0; i < labelsCount; i++) {
        size_t klen;
        const char *k = mr_BufferReaderReadBuff(br, &klen, &err);
        cCtx.labels[i].key = RedisModule_CreateString(rts_staticCtx, k, klen);

        long long hasValue = mr_BufferReaderReadLongLong(br, &err);
        if (hasValue) {
            size_t vlen;
            const char *v = mr_BufferReaderReadBuff(br, &vlen, &err);
            cCtx.labels[i].value = RedisModule_CreateString(rts_staticCtx, v, vlen);
        } else {
            cCtx.labels[i].value = NULL;
        }
    }

    Series *result = NewSeries(name, &cCtx);

    long long sampleCount = mr_BufferReaderReadLongLong(br, &err);
    for (long long i = 0; i < sampleCount; i++) {
        long long timestamp;
        double value;
        BufReadSample(br, &timestamp, &value, &err);
        SeriesAddSample(result, timestamp, value);
    }

    return result;
}

static void mrangeBufCb(RedisModuleCtx *ctx, Series *series, void *userData) {
    (void)ctx;
    MrangeBufCtx *bc = userData;
    size_t nameLen;
    const char *name = RedisModule_StringPtrLen(series->keyName, &nameLen);
    mr_BufferWriterWriteBuff(bc->bw, name, nameLen);
    BufWriteSeriesLabels(bc->bw, series);
    BufWriteSeriesRange(bc->bw, series, bc->rangeArgs, bc->reverse);
}

static void TS_INTERNAL_MRANGE(RedisModuleCtx *ctx, void *args) {
    QueryPredicates_Arg *queryArg = args;

    ApplyCtxUser(ctx, queryArg->userName);
    MRangeArgs mrangeArgs;
    mrangeArgs.rangeArgs.startTimestamp = queryArg->startTimestamp;
    mrangeArgs.rangeArgs.endTimestamp = queryArg->endTimestamp;
    mrangeArgs.rangeArgs.latest = queryArg->latest;
    mrangeArgs.rangeArgs.count = -1LL;
    mrangeArgs.rangeArgs.aggregationArgs.empty = false;
    mrangeArgs.rangeArgs.aggregationArgs.timeDelta = 0;
    mrangeArgs.rangeArgs.aggregationArgs.bucketTS = BucketStartTimestamp;
    mrangeArgs.rangeArgs.aggregationArgs.numClasses = 0;
    mrangeArgs.rangeArgs.aggregationArgs.classes = NULL;
    mrangeArgs.rangeArgs.filterByValueArgs.hasValue = false;
    mrangeArgs.rangeArgs.filterByTSArgs.hasValue = false;
    mrangeArgs.rangeArgs.alignment = DefaultAlignment;
    mrangeArgs.rangeArgs.timestampAlignment = 0;
    // Include all the labels because the aggregated result might be grouped by a label (in
    // mrange_done)
    mrangeArgs.withLabels = true;
    mrangeArgs.numLimitLabels = 0;
    mrangeArgs.queryPredicates = queryArg->predicates;
    mrangeArgs.groupByLabel = NULL;
    mrangeArgs.groupByReducerArgs.aggregationClass = NULL;
    mrangeArgs.groupByReducerArgs.agg_type = TS_AGG_NONE;
    mrangeArgs.reverse = false;

    RedisModuleDict *qi =
        QueryIndex(ctx, mrangeArgs.queryPredicates->list, mrangeArgs.queryPredicates->count, NULL);

    if (CheckDictSeriesPermissions(
            ctx, qi, GetSeriesFlags_CheckForAcls | GetSeriesFlags_SilentOperation) ==
        GetSeriesResult_PermissionError) {
        RTS_ReplyKeyPermissionsError(ctx);
        RedisModule_FreeDict(ctx, qi);
        ReleaseCtxUser(ctx);
        return;
    }

    mr_BufferWriter bw;
    size_t countOffset;
    mr_Buffer *buf = CountedReplyBegin(&bw, INTERNAL_REPLY_BUF_INITIAL_CAP, &countOffset);

    MrangeBufCtx bc = { .bw = &bw,
                        .rangeArgs = &mrangeArgs.rangeArgs,
                        .reverse = mrangeArgs.reverse };
    long long seriesCount = ForEachDictSeries(ctx, qi, mrangeBufCb, &bc);

    CountedReplyFinish(ctx, buf, countOffset, seriesCount);
    RedisModule_FreeDict(ctx, qi);
    ReleaseCtxUser(ctx);
}

static Record *SeriesListReplyParser(const redisReply *reply) {
    RedisModule_Assert(reply->type == REDIS_REPLY_STRING);

    mr_Buffer buf;
    mr_BufferReader br;
    BufReaderInitFromReply(&br, &buf, reply);
    int err = 0;

    long long numSeries = mr_BufferReaderReadLongLong(&br, &err);
    ARR(Series *) seriesList = array_new(Series *, numSeries);
    for (long long i = 0; i < numSeries; i++) {
        Series *s = DeserializeSeries(&br);
        seriesList = array_append(seriesList, s);
    }

    return SeriesListRecord_Create(seriesList);
}

static InternalCommandCallbacks MrangeCallbacks = { .command = TS_INTERNAL_MRANGE,
                                                    .replyParser = SeriesListReplyParser };

/**
 * @brief Per-series visitor invoked by ::ForEachDictSeries inside TS_INTERNAL_MGET.
 *
 * Emits one entry into the shard's reply buffer: the series key name, the
 * labels block (full / SELECTED_LABELS / empty depending on @p args), and the
 * single MGET sample via ::BufWriteMGetSample.
 */
static void mgetBufCb(RedisModuleCtx *ctx, Series *series, void *userData) {
    (void)ctx;
    MgetBufCtx *bc = userData;
    const MGetArgs *args = bc->args;

    size_t nameLen;
    const char *name = RedisModule_StringPtrLen(series->keyName, &nameLen);
    mr_BufferWriterWriteBuff(bc->bw, name, nameLen);

    if (args->withLabels) {
        BufWriteSeriesLabels(bc->bw, series);
    } else if (args->numLimitLabels > 0) {
        const char *limitLabelsStr[args->numLimitLabels];
        for (int i = 0; i < args->numLimitLabels; i++)
            limitLabelsStr[i] = RedisModule_StringPtrLen(args->limitLabels[i], NULL);
        BufWriteSeriesLabelsWithLimit(bc->bw, series, limitLabelsStr, args->numLimitLabels);
    } else {
        mr_BufferWriterWriteLongLong(bc->bw, 0);
    }

    BufWriteMGetSample(bc->bw, series, args->latest);
}

static void TS_INTERNAL_MGET(RedisModuleCtx *ctx, void *args) {
    QueryPredicates_Arg *queryArg = args;
    ApplyCtxUser(ctx, queryArg->userName);
    MGetArgs mgetArgs;
    mgetArgs.withLabels = queryArg->withLabels;
    mgetArgs.numLimitLabels = queryArg->limitLabelsSize;
    for (int i = 0; i < mgetArgs.numLimitLabels; i++)
        mgetArgs.limitLabels[i] = queryArg->limitLabels[i];
    mgetArgs.queryPredicates = queryArg->predicates;
    mgetArgs.latest = queryArg->latest;

    RedisModuleDict *qi =
        QueryIndex(ctx, mgetArgs.queryPredicates->list, mgetArgs.queryPredicates->count, NULL);

    if (CheckDictSeriesPermissions(
            ctx, qi, GetSeriesFlags_CheckForAcls | GetSeriesFlags_SilentOperation) ==
        GetSeriesResult_PermissionError) {
        RTS_ReplyKeyPermissionsError(ctx);
        RedisModule_FreeDict(ctx, qi);
        ReleaseCtxUser(ctx);
        return;
    }

    mr_BufferWriter bw;
    size_t countOffset;
    mr_Buffer *buf = CountedReplyBegin(&bw, INTERNAL_REPLY_BUF_INITIAL_CAP, &countOffset);

    MgetBufCtx bc = { .bw = &bw, .args = &mgetArgs };
    long long seriesCount = ForEachDictSeries(ctx, qi, mgetBufCb, &bc);

    CountedReplyFinish(ctx, buf, countOffset, seriesCount);
    RedisModule_FreeDict(ctx, qi);
    ReleaseCtxUser(ctx);
}

static InternalCommandCallbacks MgetCallbacks = { .command = TS_INTERNAL_MGET,
                                                  .replyParser = SeriesListReplyParser };

static void TS_INTERNAL_QUERYINDEX(RedisModuleCtx *ctx, void *args) {
    QueryPredicates_Arg *queryArg = args;
    ApplyCtxUser(ctx, queryArg->userName);
    RedisModuleDict *qi =
        QueryIndex(ctx, queryArg->predicates->list, queryArg->predicates->count, NULL);
    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(qi, "^", NULL, 0);

    mr_BufferWriter bw;
    size_t countOffset;
    mr_Buffer *buf = CountedReplyBegin(&bw, INTERNAL_REPLY_BUF_SMALL_INITIAL_CAP, &countOffset);
    long long count = 0;

    const char *keyName;
    size_t keyNameLen;
    while ((keyName = RedisModule_DictNextC(iter, &keyNameLen, NULL)) != NULL) {
        mr_BufferWriterWriteBuff(&bw, keyName, keyNameLen);
        count++;
    }

    CountedReplyFinish(ctx, buf, countOffset, count);
    RedisModule_DictIteratorStop(iter);
    RedisModule_FreeDict(ctx, qi);
    ReleaseCtxUser(ctx);
}

static Record *StringListReplyParser(const redisReply *reply) {
    RedisModule_Assert(reply->type == REDIS_REPLY_STRING);

    mr_Buffer buf;
    mr_BufferReader br;
    BufReaderInitFromReply(&br, &buf, reply);
    int err = 0;

    long long numStrings = mr_BufferReaderReadLongLong(&br, &err);
    ARR(RedisModuleString *) stringList = array_new(RedisModuleString *, numStrings);
    for (long long i = 0; i < numStrings; i++) {
        size_t len;
        const char *s = mr_BufferReaderReadBuff(&br, &len, &err);
        RedisModuleString *rms = RedisModule_CreateString(rts_staticCtx, s, len);
        stringList = array_append(stringList, rms);
    }

    return StringListRecord_Create(stringList);
}
static InternalCommandCallbacks QueryIndexCallbacks = { .command = TS_INTERNAL_QUERYINDEX,
                                                        .replyParser = StringListReplyParser };

static bool mr_initialized = false;

bool LibMR_IsInitialized() {
    return mr_initialized;
}

int LibMR_ResizeExecutionThreadPoolIfUnstarted(long long numThreads) {
    return MR_ResizeExecutionThreadPoolIfUnstarted(numThreads);
}

int register_mr(RedisModuleCtx *ctx, long long numThreads) {
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

    ListRecordType = MR_RecordTypeCreate("ListRecord",
                                         ListRecord_Free,
                                         NULL,
                                         ListRecord_Serialize,
                                         ListRecord_Deserialize,
                                         NULL,
                                         ListRecord_SendReply,
                                         NULL);

    if (MR_RegisterRecord(ListRecordType) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    MapRecordType = MR_RecordTypeCreate("MapRecord",
                                        MapRecord_Free,
                                        NULL,
                                        MapRecord_Serialize,
                                        MapRecord_Deserialize,
                                        NULL,
                                        MapRecord_SendReply,
                                        NULL);

    if (MR_RegisterRecord(MapRecordType) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    StringRecordType = MR_RecordTypeCreate("StringRecord",
                                           StringRecord_Free,
                                           NULL,
                                           StringRecord_Serialize,
                                           StringRecord_Deserialize,
                                           NULL,
                                           StringRecord_SendReply,
                                           NULL);

    if (MR_RegisterRecord(StringRecordType) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    NullRecordType = MR_RecordTypeCreate("NullRecord",
                                         NullRecord_Free,
                                         NULL,
                                         NullRecord_Serialize,
                                         NullRecord_Deserialize,
                                         NULL,
                                         NullRecord_SendReply,
                                         NULL);

    if (MR_RegisterRecord(NullRecordType) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    NullRecord.recordType = NullRecordType;

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

    SlotRangesRecordType = MR_RecordTypeCreate(
        "SlotRangesRecord", SlotRangesRecord_Free, NULL, NULL, NULL, NULL, NULL, NULL);
    if (MR_RegisterRecord(SlotRangesRecordType) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    SeriesListRecordType = MR_RecordTypeCreate(
        "SeriesListRecord", SeriesListRecord_Free, NULL, NULL, NULL, NULL, NULL, NULL);
    if (MR_RegisterRecord(SeriesListRecordType) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    StringListRecordType = MR_RecordTypeCreate(
        "StringListRecord", StringListRecord_Free, NULL, NULL, NULL, NULL, NULL, NULL);
    if (MR_RegisterRecord(StringListRecordType) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    MR_RegisterReader("ShardSeriesMapper", ShardSeriesMapper, QueryPredicatesType);
    MR_RegisterInternalCommand(
        "TS.INTERNAL_SLOT_RANGES", &SlotRangesCallbacks, QueryPredicatesType);
    MR_RegisterInternalCommand("TS.INTERNAL_MRANGE", &MrangeCallbacks, QueryPredicatesType);
    MR_RegisterInternalCommand("TS.INTERNAL_MGET", &MgetCallbacks, QueryPredicatesType);
    MR_RegisterInternalCommand("TS.INTERNAL_QUERYINDEX", &QueryIndexCallbacks, QueryPredicatesType);

    MR_RegisterReader("ShardMgetMapper", ShardMgetMapper, QueryPredicatesType);

    MR_RegisterReader("ShardQueryindexMapper", ShardQueryindexMapper, QueryPredicatesType);
    mr_initialized = true;

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
    StringRecord *ret = (StringRecord *)MR_RecordCreate(StringRecordType, sizeof(*ret));
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
    MapRecord *ret = (MapRecord *)MR_RecordCreate(MapRecordType, sizeof(*ret));
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
    ListRecord *ret = (ListRecord *)MR_RecordCreate(ListRecordType, sizeof(*ret));
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
    series->keyName = SerializationCtxReadRedisString(sctx, error);
    series->labelsCount = MR_SerializationCtxReadLongLong(sctx, error);
    series->labels = calloc(series->labelsCount, sizeof(Label));
    for (int i = 0; i < series->labelsCount; i++) {
        series->labels[i].key = SerializationCtxReadRedisString(sctx, error);
        series->labels[i].value = SerializationCtxReadRedisString(sctx, error);
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

static Record *SlotRangesRecord_Create(RedisModuleSlotRangeArray *slotRanges) {
    SlotRangesRecord *result =
        (SlotRangesRecord *)MR_RecordCreate(SlotRangesRecordType, sizeof(*result));
    result->slotRanges = slotRanges;
    return &result->base;
}

static void SlotRangesRecord_Free(void *base) {
    SlotRangesRecord *record = base;
    free(record->slotRanges);
    free(record);
}

static Record *SeriesListRecord_Create(ARR(Series *) seriesList) {
    SeriesListRecord *result =
        (SeriesListRecord *)MR_RecordCreate(SeriesListRecordType, sizeof(*result));
    result->seriesList = seriesList;
    return &result->base;
}

static void SeriesListRecord_Free(void *base) {
    SeriesListRecord *record = base;
    array_free_ex(record->seriesList, FreeSeries(*(Series **)ptr));
    free(record);
}

static Record *StringListRecord_Create(ARR(RedisModuleString *) stringList) {
    StringListRecord *result =
        (StringListRecord *)MR_RecordCreate(StringListRecordType, sizeof(*result));
    result->stringList = stringList;
    return &result->base;
}

static void StringListRecord_Free(void *base) {
    StringListRecord *record = base;
    array_free_ex(record->stringList,
                  RedisModule_FreeString(rts_staticCtx, *(RedisModuleString **)ptr));
    free(record);
}
