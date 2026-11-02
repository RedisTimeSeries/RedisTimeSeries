#include "RedisModulesSDK/redismodule.h"
#include "LibMR/src/utils/arr.h"
#include "generic_chunk.h"
#include "indexer.h"
#include "query_language.h"
#include "tsdb.h"

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
    /** Username from client (for other shards ACL).
     * NULL or empty string means no explicit user when ACL context is applied. */
    RedisModuleString *userName;
    QueryPredicateList *predicates;
    timestamp_t startTimestamp;
    timestamp_t endTimestamp;
    size_t count;
    bool withLabels;
    unsigned short limitLabelsSize;
    RedisModuleString **limitLabels;
    bool latest;
    bool resp3;
    // Per-shard aggregation fields — applied on shards for both GROUPBY and non-GROUPBY paths
    size_t numAggClasses;
    TS_AGG_TYPES_T aggTypes[TS_AGG_TYPES_MAX];
    api_timestamp_t aggTimeDelta;
    BucketTimestamp aggBucketTS;
    bool aggEmpty;
    RangeAlignment alignment;
    timestamp_t timestampAlignment;
    FilterByValueArgs filterByValueArgs;
    FilterByTSArgs filterByTSArgs;
    bool excludeEmpty;
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

// The following record structs are for internal commands.
// They are a bit more lightweight than the above collection records because
// their elements do not need to be "derived from" Record objects.
// We create them in the `replyParser`s and use them in the `..._done()` callbacks.

typedef struct SlotRangesRecord
{
    Record base;
    RedisModuleSlotRangeArray *slotRanges;
} SlotRangesRecord;

typedef struct SeriesListRecord
{
    Record base;
    ARR(Series *) seriesList;
    size_t numAggClasses; // >1: seriesList has numAggClasses Series per key (multi-agg pre-agg)
} SeriesListRecord;

typedef struct StringListRecord
{
    Record base;
    ARR(RedisModuleString *) stringList;
} StringListRecord;

// Arg for TS.INTERNAL_QUERYLABELS. Kept separate from QueryPredicates_Arg (used by
// mget/mrange/queryindex) rather than adding a subtype field to it, since that type's
// (de)serialization already carries a two-variant rolling-upgrade fallback that a third
// unrelated variant would only make more fragile.
typedef struct QueryLabelsArg
{
    bool shouldReturnNull; // GEARS reader one-shot guard; unused by the INTERNAL protocol path
    size_t refCount;
    QueryLabelsSubtype subtype;
    RedisModuleString *userName;
    RedisModuleString *label; // target label name for VALUES; NULL for LABELS
    bool hasFilter;
    QueryPredicateList *predicates; // NULL when hasFilter is false
} QueryLabelsArg;

// Drops a reference; frees the arg (and its predicates/strings) once refCount hits 0.
void QueryLabelsArg_ObjectFree(void *arg);

MRRecordType *GetMapRecordType();
MRRecordType *GetListRecordType();
MRRecordType *GetSeriesRecordType();
MRRecordType *GetSlotRangesRecordType();
MRRecordType *GetSeriesListRecordType();
MRRecordType *GetStringListRecordType();
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

int register_mr(RedisModuleCtx *ctx, long long numThreads);
int LibMR_ResizeExecutionThreadPoolIfUnstarted(long long numThreads);
bool LibMR_IsInitialized();
bool IsMRCluster();

#endif // REDIS_TIMESERIES_CLEAN_MR_INTEGRATION_H
