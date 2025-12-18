#include "RedisModulesSDK/redismodule.h"
#include "generic_chunk.h"
#include "indexer.h"
#include "query_language.h"
#include "tsdb.h"

#include <stdint.h>

#ifndef REDIS_TIMESERIES_CLEAN_MR_INTEGRATION_H
#define REDIS_TIMESERIES_CLEAN_MR_INTEGRATION_H

typedef enum QueryType
{
    QUERY_MGET,
    QUERY_QUERYINDEX,
    QUERY_MRANGE,
} QueryType;

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
    bool resp3;

    // Optional extra data for specialized multi-shard execution paths.
    // Kept inside this shared object so shards can run pushed-down logic.
    uint64_t flags;
    RangeArgs rangeArgs;
    TS_AGG_TYPES_T rangeAggType; // TS_AGG_NONE when no aggregation
    ReducerArgs reducerArgs;     // used for GROUPBY/REDUCE pushdown
    RedisModuleString *groupByLabel;
} QueryPredicates_Arg;

// QueryPredicates_Arg.flags values
#define QP_FLAG_MRANGE_GROUPBY_REDUCE_PUSHDOWN (1ULL << 0)

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

typedef struct MapRecord
{
    Record base;
    Record **records;
} MapRecord;

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

typedef struct DoubleRecord
{
    Record base;
    double num;
} DoubleRecord;

typedef struct LongRecord
{
    Record base;
    long num;
} LongRecord;

MRRecordType *GetMapRecordType();
MRRecordType *GetListRecordType();
MRRecordType *GetSeriesRecordType();
Record *MapRecord_GetRecord(MapRecord *record, size_t index);
size_t MapRecord_GetLen(MapRecord *record);
Record *ListRecord_GetRecord(ListRecord *record, size_t index);
size_t ListRecord_GetLen(ListRecord *record);
Record *SeriesRecord_New(Series *series,
                         timestamp_t startTimestamp,
                         timestamp_t endTimestamp,
                         const QueryPredicates_Arg *predicates);
void SeriesRecord_ObjectFree(void *series);
void SeriesRecord_Serialize(WriteSerializationCtx *sctx, void *arg, MRError **error);
void *SeriesRecord_Deserialize(ReaderSerializationCtx *sctx, MRError **error);
void SeriesRecord_SendReply(RedisModuleCtx *rctx, void *record);
Series *SeriesRecord_IntoSeries(SeriesRecord *record);

int register_rg(RedisModuleCtx *ctx, long long numThreads);
bool IsMRCluster();

#endif // REDIS_TIMESERIES_CLEAN_MR_INTEGRATION_H
