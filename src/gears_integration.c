
#include "gears_integration.h"
#include "indexer.h"
#include "consts.h"

#include "RedisModulesSDK/redismodule.h"
#include "RedisGears/src/redisgears.h"
#include "query_language.h"

#define QueryPredicatesVersion 1

typedef struct QueryPredicates_Arg {
    QueryPredicate *predicates;
    size_t count;
    bool withLabels;
} QueryPredicates_Arg;

static void QueryPredicates_ObjectFree(void* arg){
    QueryPredicates_Arg* predicate_list = arg;

    for (int i=0; i < predicate_list->count; i++) {
        QueryPredicate *predicate = predicate_list->predicates + i;
        RedisModule_FreeString(NULL, predicate->key);
        for (int value_index=0; value_index < predicate->valueListCount; value_index++) {
            RedisModule_FreeString(NULL, predicate->valuesList[value_index]);
        }
    }
    free(predicate_list->predicates);

    free(predicate_list);
}

static void* QueryPredicates_Duplicate(void *arg) {
    assert(FALSE);
}

static char* QueryPredicates_ToString(void *arg) {
    QueryPredicates_Arg* predicate_list = arg;
    char out[250];
    int index = 0;
    index += sprintf(out, "QueryPredicates: len: %lu; ", predicate_list->count);
    for (int i=0; i < predicate_list->count; i++) {
        QueryPredicate *predicate = predicate_list->predicates + i;
        size_t len;
        index += sprintf(out +index, "'%s=%s' ",
                         RedisModule_StringPtrLen(predicate->key, &len),
                         RedisModule_StringPtrLen(predicate->valuesList[0], &len));
    }
    return strdup(out);
}

static int QueryPredicates_ArgSerialize(FlatExecutionPlan* fep, void* arg, Gears_BufferWriter* bw, char** err){
    QueryPredicates_Arg* predicate_list = arg;
    RedisGears_BWWriteLong(bw, predicate_list->count);
    RedisGears_BWWriteLong(bw, predicate_list->withLabels);
    for (int i=0; i < predicate_list->count; i++) {
        // encode type
        QueryPredicate *predicate = predicate_list->predicates + i;
        RedisGears_BWWriteLong(bw, predicate->type);

        // encode key
        size_t len = 0;
        const char *keyC = RedisModule_StringPtrLen(predicate->key, &len);
        RedisGears_BWWriteBuffer(bw, keyC, len + 1);

        //encode values
        RedisGears_BWWriteLong(bw, predicate->valueListCount);
        for (int value_index=0; value_index < predicate->valueListCount; value_index++) {
            size_t value_len = 0;
            const char *value = RedisModule_StringPtrLen(predicate->valuesList[value_index], &value_len);
            RedisGears_BWWriteBuffer(bw, value, value_len + 1);
        }

    }
    return REDISMODULE_OK;
}

static void* QueryPredicates_ArgDeserialize(FlatExecutionPlan* fep, Gears_BufferReader* br, int version, char** err){
    QueryPredicates_Arg* predicates = RG_ALLOC(sizeof(*predicates));
    predicates->count = RedisGears_BRReadLong(br);
    predicates->withLabels = RedisGears_BRReadLong(br);
    predicates->predicates = calloc(predicates->count, sizeof(QueryPredicate));
    for (int i=0; i < predicates->count; i++) {
        QueryPredicate *predicate = predicates->predicates + i;
        // decode type
        predicate->type = RedisGears_BRReadLong(br);

        // decode key
        char *key_c = RedisGears_BRReadString(br);
        size_t len = strlen(key_c);
        predicate->key = RedisModule_CreateString(NULL, key_c, len);

        // decode values
        predicate->valueListCount = RedisGears_BRReadLong(br);
        predicate->valuesList = calloc(predicate->valueListCount, sizeof(RedisModuleString*));

        for (int value_index=0; value_index < predicate->valueListCount; value_index++) {
            char *key_c = RedisGears_BRReadString(br);
            predicate->valuesList[value_index] = RedisModule_CreateString(NULL, key_c, strlen(key_c));
        }
    }
    return predicates;
}

Record *RedisGears_RedisStringRecordCreate(RedisModuleString *str) {
    size_t len = 0;
    return RedisGears_StringRecordCreate(strdup(RedisModule_StringPtrLen(str, &len)), len);
}

Record *ListSeriesLabels(const Series *series) {
    Record *r = RedisGears_ListRecordCreate(series->labelsCount);
    for (int i = 0; i < series->labelsCount; i++) {
        Record *internal_list = RedisGears_ListRecordCreate(series->labelsCount);
        RedisGears_ListRecordAdd(internal_list, RedisGears_RedisStringRecordCreate(series->labels[i].key));
        RedisGears_ListRecordAdd(internal_list, RedisGears_RedisStringRecordCreate(series->labels[i].value));
        RedisGears_ListRecordAdd(r, internal_list);
    }
    return r;
}

#define MAX_VAL_LEN 24
Record* ListWithSample(u_int64_t timestamp, double value) {
    Record *r = RedisGears_ListRecordCreate(2);
    RedisGears_ListRecordAdd(r, RedisGears_LongRecordCreate(timestamp));
    char buf[MAX_VAL_LEN];
    snprintf(buf, MAX_VAL_LEN, "%.15g", value);
    RedisGears_ListRecordAdd(r, RedisGears_StringRecordCreate(strdup(buf), strlen(buf)));
    return r;
}


Record* ListWithSeriesLastDatapoint(const Series *series) {
    if (SeriesGetNumSamples(series) == 0) {
        return RedisGears_ListRecordCreate(0);
    } else {
        return ListWithSample(series->lastTimestamp, series->lastValue);
    }
}

Record* ShardMgetMapper(ExecutionCtx* rctx, Record *data, void* arg) {
    RedisModuleCtx *ctx = RedisGears_GetRedisModuleCtx(rctx);
    QueryPredicates_Arg* predicates = arg;

    RedisModuleDict *result = QueryIndex(ctx, predicates->predicates, predicates->count);

    RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(result, "^", NULL, 0);
    char *currentKey;
    size_t currentKeyLen;

    Series *series;
    Record *series_list = RedisGears_ListRecordCreate(0);
    while ((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
        RedisModuleKey *key;
        const int status = SilentGetSeries(ctx,
                                           RedisModule_CreateString(ctx, currentKey, currentKeyLen),
                                           &key,
                                           &series,
                                           REDISMODULE_READ);
        if (!status) {
            RedisModule_Log(ctx,
                            "warning",
                            "couldn't open key or key is not a Timeseries. key=%.*s",
                            currentKeyLen,
                            currentKey);
            continue;
        }

        Record *r = RedisGears_ListRecordCreate(3);
        RedisGears_ListRecordAdd(r, RedisGears_StringRecordCreate(strdup(currentKey), currentKeyLen));
        if (predicates->withLabels) {
            RedisGears_ListRecordAdd(r, ListSeriesLabels(series));
        } else {
            RedisGears_ListRecordAdd(r, RedisGears_ListRecordCreate(0));
        }
        RedisGears_ListRecordAdd(r, ListWithSeriesLastDatapoint(series));
        RedisModule_CloseKey(key);

        RedisGears_ListRecordAdd(series_list, r);
        RedisGears_ListRecordAdd(series_list, RedisGears_StringRecordCreate(strdup(currentKey), currentKeyLen));
    }
    RedisModule_DictIteratorStop(iter);

    return series_list;
}

int register_rg(RedisModuleCtx *ctx) {
    if(RedisGears_InitAsRedisModule(ctx, "timeseries", REDISMODULE_TYPE_METHOD_VERSION) != REDISMODULE_OK){
        RedisModule_Log(ctx, "warning", "Failed initialize RedisGears API");
        return REDISMODULE_ERR;
    }

    ArgType* QueryPredicatesType = RedisGears_CreateType("QueryPredicatesType",
                                                         QueryPredicatesVersion,
                                                         QueryPredicates_ObjectFree,
                                                         QueryPredicates_Duplicate,
                                                         QueryPredicates_ArgSerialize,
                                                         QueryPredicates_ArgDeserialize,
                                                         QueryPredicates_ToString);

    return RedisGears_RegisterMap("ShardMgetMapper", ShardMgetMapper, QueryPredicatesType);
}

static void on_done(ExecutionPlan* ctx, void* privateData) {
    RedisModuleBlockedClient *bc = privateData;
    RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(bc);
    RedisGears_ReturnResultsAndErrors(ctx, rctx);
    RedisModule_UnblockClient(bc, NULL);
    RedisGears_DropExecution(ctx);
    RedisModule_FreeThreadSafeContext(rctx);
}

int TSDB_mget_RG(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 3) {
        return RedisModule_WrongArity(ctx);
    }

    int filter_location = RMUtil_ArgIndex("FILTER", argv, argc);
    if (filter_location == -1) {
        return RedisModule_WrongArity(ctx);
    }
    size_t query_count = argc - 1 - filter_location;
    const int withlabels_location = RMUtil_ArgIndex("WITHLABELS", argv, argc);
    QueryPredicate *queries = calloc(query_count, sizeof(QueryPredicate));
    if (parseLabelListFromArgs(ctx, argv, filter_location + 1, query_count, queries) ==
        TSDB_ERROR) {
        return RTS_ReplyGeneralError(ctx, "TSDB: failed parsing labels");
    }

    if (CountPredicateType(queries, (size_t)query_count, EQ) +
        CountPredicateType(queries, (size_t)query_count, LIST_MATCH) ==
        0) {
        return RTS_ReplyGeneralError(ctx, "TSDB: please provide at least one matcher");
    }


    char *err = NULL;
    FlatExecutionPlan* rg_ctx = RedisGears_CreateCtx("ShardIDReader", &err);
    if (err) {
        RedisModule_ReplyWithError(ctx, err);
    }
    QueryPredicates_Arg *queryArg = malloc(sizeof(QueryPredicate));
    queryArg->count = query_count;
    queryArg->predicates = queries;
    queryArg->withLabels = (withlabels_location > 0);
    RedisGears_Map(rg_ctx, "ShardMgetMapper", queryArg);

    RGM_Collect(rg_ctx);
    ExecutionPlan* ep = RGM_Run(rg_ctx, ExecutionModeAsync, NULL, NULL, NULL, &err);
    if(!ep){
        RedisGears_FreeFlatExecution(rg_ctx);
        RedisModule_ReplyWithError(ctx, err);
        return REDISMODULE_OK;
    }

    RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, NULL, NULL, NULL, 0);
    RedisGears_AddOnDoneCallback(ep, on_done, bc);
    RedisGears_FreeFlatExecution(rg_ctx);
    return REDISMODULE_OK;
}
