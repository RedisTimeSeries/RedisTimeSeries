
#include "generic_chunk.h"
#include "tsdb.h"
#include "indexer.h"

#include "RedisModulesSDK/redismodule.h"
#include "RedisGears/src/redisgears.h"

#ifndef REDIS_TIMESERIES_CLEAN_GEARS_INTEGRATION_H
#define REDIS_TIMESERIES_CLEAN_GEARS_INTEGRATION_H

typedef struct QueryPredicates_Arg {
    QueryPredicate *predicates;
    size_t count;
    bool withLabels;
} QueryPredicates_Arg;

typedef struct SeriesRecord {
    Record base;
    CHUNK_TYPES_T chunkType;
    ChunkFuncs *funcs;
    RedisModuleString *keyName;
    Label *labels;
    size_t labelsCount;
    Chunk_t **chunks;
    size_t chunkCount;
} SeriesRecord;

RecordType *GetSeriesRecordType();
Record* SeriesRecord_New(Series *series);
void SeriesRecord_ObjectFree(void *series);
int SeriesRecord_Serialize(ExecutionCtx* ctx, Gears_BufferWriter* bw, Record* base);
Record* SeriesRecord_Deserialize(ExecutionCtx* ctx, Gears_BufferReader* br);
int SeriesRecord_SendReply(Record* record, RedisModuleCtx* rctx);
Series* SeriesRecord_IntoSeries(SeriesRecord *record);

int register_rg(RedisModuleCtx *ctx);

#endif //REDIS_TIMESERIES_CLEAN_GEARS_INTEGRATION_H
