#include "RedisModulesSDK/redismodule.h"
#include "generic_chunk.h"
#include "indexer.h"
#include "tsdb.h"

#ifndef REDIS_TIMESERIES_CLEAN_MR_INTEGRATION_H
#define REDIS_TIMESERIES_CLEAN_MR_INTEGRATION_H

typedef struct QueryPredicates_Arg
{
    bool shouldReturnNull;
    size_t refCount;
    QueryPredicateList *predicates;
    timestamp_t startTimestamp;
    timestamp_t endTimestamp;
    size_t count;
    bool withLabels;
    unsigned short limitLabelsSize;
    RedisModuleString **limitLabels;
    bool latest;
} QueryPredicates_Arg;

typedef struct StringRecord
{
    Record base;
    size_t len;
    char *str;
} StringRecord;

typedef struct ListRecord
{
    Record base;
    Record **records;
} ListRecord;

typedef struct SeriesRecord
{
    Record base;
    CHUNK_TYPES_T chunkType;
    const ChunkFuncs *funcs;
    RedisModuleString *keyName;
    Label *labels;
    size_t labelsCount;
    Chunk_t **chunks;
    size_t chunkCount;
} SeriesRecord;

typedef struct LongRecord
{
    Record base;
    long num;
} LongRecord;

MRRecordType *GetListRecordType();
MRRecordType *GetSeriesRecordType();
Record *ListRecord_GetRecord(ListRecord *record, size_t index);
size_t ListRecord_GetLen(ListRecord *record);
Record *SeriesRecord_New(Series *series, timestamp_t startTimestamp, timestamp_t endTimestamp);
void SeriesRecord_ObjectFree(void *series);
void SeriesRecord_Serialize(WriteSerializationCtx *sctx, void *arg, MRError **error);
void *SeriesRecord_Deserialize(ReaderSerializationCtx *sctx, MRError **error);
void SeriesRecord_SendReply(RedisModuleCtx *rctx, void *record);
Series *SeriesRecord_IntoSeries(SeriesRecord *record);

int register_rg(RedisModuleCtx *ctx, long long numThreads);
bool IsMRCluster();

#endif // REDIS_TIMESERIES_CLEAN_MR_INTEGRATION_H
