#include "redismodule.h"
#include "module.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"
#include "rmutil/alloc.h"
// #include "rmutil/test_util.h"

#include "tsdb.h"

#define MYTYPE_ENCODING_VERSION 0 

static RedisModuleType *SeriesType;

int TSDB_Meta(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    
    if (argc != 2) return RedisModule_WrongArity(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
    Series *series;
    
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY){
        return RedisModule_ReplyWithError(ctx, "TSDB: key does not exist");
    } else if (RedisModule_ModuleTypeGetType(key) != SeriesType){
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    } else {
        series = RedisModule_ModuleTypeGetValue(key);
    }

    RedisModule_ReplyWithArray(ctx, 4*2);

    RedisModule_ReplyWithSimpleString(ctx, "lastTimestamp");
    RedisModule_ReplyWithLongLong(ctx, series->lastTimestamp);
    RedisModule_ReplyWithSimpleString(ctx, "retentionSecs");
    RedisModule_ReplyWithLongLong(ctx, series->retentionSecs);
    RedisModule_ReplyWithSimpleString(ctx, "chunkCount");
    RedisModule_ReplyWithLongLong(ctx, series->chunkCount);
    RedisModule_ReplyWithSimpleString(ctx, "maxSamplesPerChunk");
    RedisModule_ReplyWithLongLong(ctx, series->maxSamplesPerChunk);

    return REDISMODULE_OK;
}

int TSDB_Query(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    
    if (argc != 4) return RedisModule_WrongArity(ctx);

    long long start_ts, end_ts;
    
    if (RedisModule_StringToLongLong(argv[2], &start_ts) != REDISMODULE_OK)
        RedisModule_ReplyWithError(ctx, "TSDB: start-timestamp is invalid");
    if (RedisModule_StringToLongLong(argv[3], &end_ts) != REDISMODULE_OK)
        RedisModule_ReplyWithError(ctx, "TSDB: end-timestamp is invalid");

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
    Series *series;
    
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY){
        return RedisModule_ReplyWithError(ctx, "TSDB: key does not exist");
    } else if (RedisModule_ModuleTypeGetType(key) != SeriesType){
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    } else {
        series = RedisModule_ModuleTypeGetValue(key);
    }

    long long arraylen = 0;
    RedisModule_ReplyWithArray(ctx,REDISMODULE_POSTPONED_ARRAY_LEN);
    SeriesItertor iterator = SeriesQuery(series, start_ts, end_ts);
    Sample *sample;
    while ((sample = SeriesItertorGetNext(&iterator)) != NULL ) {
            RedisModule_ReplyWithLongLong(ctx, sample->timestamp);
            RedisModule_ReplyWithLongLong(ctx, sample->data);
            arraylen++;
    }

    RedisModule_ReplySetArrayLength(ctx,arraylen*2);
    return REDISMODULE_OK;
}

int TSDB_Insert(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    
    if (argc != 4) {
        return RedisModule_WrongArity(ctx);
    }

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE);

    double timestamp, value;
    if ((RedisModule_StringToDouble(argv[3], &value) != REDISMODULE_OK))
        return RedisModule_ReplyWithError(ctx,"TSDB: invalid value");
    
    if ((RedisModule_StringToDouble(argv[2], &timestamp) != REDISMODULE_OK))
        return RedisModule_ReplyWithError(ctx,"TSDB: invalid timestamp");

    Series *series;
    
    if (RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY){
        series = TSDB_Create(ctx, argv[1]);
    } else if (RedisModule_ModuleTypeGetType(key) != SeriesType){
        return RedisModule_ReplyWithError(ctx, "TSDB: the key is not a TSDB key");
    } else {
        series = RedisModule_ModuleTypeGetValue(key);
    }

    int retval = SeriesAddSample(series, timestamp, value);
    if (retval == TSDB_ERR_TIMESTAMP_TOO_OLD) {
        RedisModule_ReplyWithError(ctx, "TSDB: timestamp is too old");
        return REDISMODULE_ERR;
    } else if (retval != TSDB_OK) {
        RedisModule_ReplyWithError(ctx, "TSDB: Unknown Error");
        return REDISMODULE_ERR;
    } else {
        RedisModule_ReplyWithSimpleString(ctx, "OK");
        RedisModule_ReplicateVerbatim(ctx);
        return REDISMODULE_OK;
    }
}

Series* TSDB_Create(RedisModuleCtx *ctx, RedisModuleString *key) {
    RedisModuleKey *series = RedisModule_OpenKey(ctx, key, REDISMODULE_READ|REDISMODULE_WRITE);

    if (RedisModule_KeyType(series) != REDISMODULE_KEYTYPE_EMPTY) {
        return NULL;
    }

    Series *newSeries = NewSeries();
    RedisModule_ModuleTypeSetValue(series, SeriesType, newSeries);

    RedisModule_Log(ctx, "info", "created new series");
    return newSeries;
}
void series_aof_rewrite(RedisModuleIO *aof, RedisModuleString *key, void *value)
{}

void series_rdb_save(RedisModuleIO *rdb, void *value)
{}

void *series_rdb_load(RedisModuleIO *rdb, int encver)
{
    return NULL;
}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {
  if (RedisModule_Init(ctx, "tsdb", 1, REDISMODULE_APIVER_1) ==
      REDISMODULE_ERR) {
    return REDISMODULE_ERR;
  }

  RedisModuleTypeMethods tm = {
        .version = REDISMODULE_TYPE_METHOD_VERSION,
        .rdb_load = series_rdb_load,
        .rdb_save = series_rdb_save,
        .aof_rewrite = series_aof_rewrite,
        .mem_usage = SeriesMemUsage,
        .free = SeriesFree
    };

    SeriesType = RedisModule_CreateDataType(ctx, "TSDB-TYPE", 0, &tm);
    if (SeriesType == NULL) return REDISMODULE_ERR;
    RMUtil_RegisterWriteCmd(ctx, "ts.insert", TSDB_Insert);
    RMUtil_RegisterWriteCmd(ctx, "ts.query", TSDB_Query);
    RMUtil_RegisterWriteCmd(ctx, "ts.meta", TSDB_Meta);

    return REDISMODULE_OK;
}
