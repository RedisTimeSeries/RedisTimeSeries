#include "redismodule.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"
#include "rmutil/alloc.h"
// #include "rmutil/test_util.h"

#include "tsdb.h"

#define TS_ENC_VER 0

#define AGG_NONE 0
#define AGG_MIN 1
#define AGG_MAX 2
#define AGG_SUM 3
#define AGG_AVG 4

static RedisModuleType *SeriesType;

typedef struct agg_values_t {
    double sum_agg, avg_count, min_val, max_val;
} agg_values_t;

int TSDB_info(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
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

void print_agg_values(RedisModuleCtx *ctx, int agg_type, timestamp_t last_agg_timestamp, agg_values_t *agg_values) {
    RedisModule_ReplyWithLongLong(ctx, last_agg_timestamp);

    if (agg_type == AGG_SUM) {
        RedisModule_ReplyWithDouble(ctx, agg_values->sum_agg);
    } else if (agg_type == AGG_AVG) {
        RedisModule_ReplyWithDouble(ctx, agg_values->sum_agg/agg_values->avg_count);
    } else if (agg_type == AGG_MAX) {
        RedisModule_ReplyWithDouble(ctx, agg_values->max_val);
    } else if (agg_type == AGG_MIN) {
        RedisModule_ReplyWithDouble(ctx, agg_values->min_val);
    }
}

int TSDB_range(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx);
    
    if (argc < 4 || argc > 6) return RedisModule_WrongArity(ctx);

    long long start_ts, end_ts;
    long long agg_type = 0;
    long long dt = 0;
    agg_values_t agg_values;
    
    if (RedisModule_StringToLongLong(argv[2], &start_ts) != REDISMODULE_OK)
        RedisModule_ReplyWithError(ctx, "TSDB: start-timestamp is invalid");
    if (RedisModule_StringToLongLong(argv[3], &end_ts) != REDISMODULE_OK)
        RedisModule_ReplyWithError(ctx, "TSDB: end-timestamp is invalid");
    if (argc > 4) {
        if (RMUtil_ArgExists("SUM", argv, argc, 3)) {
            agg_type = AGG_SUM;
        } else if (RMUtil_ArgExists("AVG", argv, argc, 3)) {
            agg_type = AGG_AVG;
        } else if (RMUtil_ArgExists("MIN", argv, argc, 3)) {
            agg_type = AGG_MIN;
        } else if (RMUtil_ArgExists("MAX", argv, argc, 3)) {
            agg_type = AGG_MAX;
        } else {
            RedisModule_ReplyWithError(ctx, "TSDB: Unkown aggregation type");
        }

        if (RedisModule_StringToLongLong(argv[5], &dt) != REDISMODULE_OK)
            RedisModule_ReplyWithError(ctx, "TSDB: time delta is invalid");
    }

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
    timestamp_t last_agg_timestamp = 0;
    while ((sample = SeriesItertorGetNext(&iterator)) != NULL ) {
        if (agg_type == AGG_NONE) { // No aggregation whats so ever
            RedisModule_ReplyWithLongLong(ctx, sample->timestamp);
            RedisModule_ReplyWithDouble(ctx, sample->data);
            arraylen++;
        } else {
            timestamp_t current_timestamp = sample->timestamp - sample->timestamp%dt;
            if (current_timestamp > last_agg_timestamp) {
                if (last_agg_timestamp != 0) {
                    print_agg_values(ctx, agg_type, last_agg_timestamp, &agg_values);
                    arraylen++;
                }

                last_agg_timestamp = current_timestamp;
                agg_values.sum_agg = 0;
                agg_values.avg_count = 0;
                agg_values.max_val,agg_values.min_val = sample->data;
            }

            if (agg_type == AGG_AVG || agg_type == AGG_SUM)
            {
                agg_values.sum_agg += sample->data;
                agg_values.avg_count++;
            }

            if (agg_type == AGG_MAX && sample->data > agg_values.max_val) {
                agg_values.max_val = sample->data;
            }
            if (agg_type == AGG_MIN && sample->data < agg_values.min_val) {
                agg_values.min_val = sample->data;
            }
        }
    }

    if (agg_type != AGG_NONE && agg_values.avg_count > 0) {
        // reply last bucket of data
        print_agg_values(ctx, agg_type, last_agg_timestamp, &agg_values);
        arraylen++;
    }

    RedisModule_ReplySetArrayLength(ctx,arraylen*2);
    return REDISMODULE_OK;
}

int TSDB_add(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
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
        return RedisModule_ReplyWithError(ctx, "TSDB: the key does not exists");
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

int TSDB_create(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc < 2 || argc > 4)
        return RedisModule_WrongArity(ctx);

    RedisModuleString *key = argv[1];
    long long retentionSecs = 0;
    long long maxSamplesPerChunk = 360;

    if (argc > 2) {
        if ((RedisModule_StringToLongLong(argv[2], &retentionSecs) != REDISMODULE_OK))
            return RedisModule_ReplyWithError(ctx,"TSDB: invalid retentionSecs");
    }

    if (argc > 3) {
        if ((RedisModule_StringToLongLong(argv[3], &maxSamplesPerChunk) != REDISMODULE_OK))
            return RedisModule_ReplyWithError(ctx,"TSDB: invalid maxSamplesPerChunk");
    }

    RedisModuleKey *series = RedisModule_OpenKey(ctx, key, REDISMODULE_READ|REDISMODULE_WRITE);

    if (RedisModule_KeyType(series) != REDISMODULE_KEYTYPE_EMPTY) {
        return RedisModule_ReplyWithError(ctx,"TSDB: key already exists");
    }

    Series *newSeries = NewSeries(retentionSecs, maxSamplesPerChunk);
    RedisModule_ModuleTypeSetValue(series, SeriesType, newSeries);

    RedisModule_Log(ctx, "info", "created new series");
    RedisModule_ReplyWithSimpleString(ctx, "OK");
    return REDISMODULE_OK;
}
void series_aof_rewrite(RedisModuleIO *aof, RedisModuleString *key, void *value)
{}

void series_rdb_save(RedisModuleIO *io, void *value)
{
    Series *series = value;
    RedisModule_SaveUnsigned(io, series->retentionSecs);
    RedisModule_SaveUnsigned(io, series->maxSamplesPerChunk);

    Chunk *chunk = series->firstChunk;
    size_t numSamples =0;
    while (chunk != NULL) {
        numSamples += chunk->num_samples;
        chunk = chunk->nextChunk;
    }
    RedisModule_SaveUnsigned(io, numSamples);

    SeriesItertor iter = SeriesQuery(series, 0, series->lastTimestamp);
    Sample *sample;
    while ((sample = SeriesItertorGetNext(&iter)) != NULL) {
        RedisModule_SaveUnsigned(io, sample->timestamp);
        RedisModule_SaveDouble(io, sample->data);
    }
}

void *series_rdb_load(RedisModuleIO *io, int encver)
{
    if (encver != TS_ENC_VER) {
        RedisModule_LogIOError(io, "error", "data is not in the correct encoding");
        return NULL;
    }
    uint64_t retentionSecs = RedisModule_LoadUnsigned(io);
    uint64_t maxSamplesPerChunk = RedisModule_LoadUnsigned(io);
    uint64_t samplesCount = RedisModule_LoadUnsigned(io);

    Series *series = NewSeries(retentionSecs, maxSamplesPerChunk);
    for (size_t sampleIndex = 0; sampleIndex < samplesCount; sampleIndex++) {
        timestamp_t ts = RedisModule_LoadUnsigned(io);
        double val = RedisModule_LoadDouble(io);
        SeriesAddSample(series, ts, val);
    }
    return series;
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

    SeriesType = RedisModule_CreateDataType(ctx, "TSDB-TYPE", TS_ENC_VER, &tm);
    if (SeriesType == NULL) return REDISMODULE_ERR;
    RMUtil_RegisterWriteCmd(ctx, "ts.create", TSDB_create);
    RMUtil_RegisterWriteCmd(ctx, "ts.add", TSDB_add);
    RMUtil_RegisterWriteCmd(ctx, "ts.range", TSDB_range);
    RMUtil_RegisterWriteCmd(ctx, "ts.info", TSDB_info);

    return REDISMODULE_OK;
}
