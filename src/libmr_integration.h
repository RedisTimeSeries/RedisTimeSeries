#include "RedisModulesSDK/redismodule.h"
#include "LibMR/src/utils/arr.h"
#include "generic_chunk.h"
#include "indexer.h"
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
    QueryPredicateList *predicates;
    timestamp_t startTimestamp;
    timestamp_t endTimestamp;
    size_t count;
    bool withLabels;
    unsigned short limitLabelsSize;
    RedisModuleString **limitLabels;
    bool latest;
    bool resp3;
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
} SeriesListRecord;

typedef struct StringListRecord
{
    Record base;
    ARR(RedisModuleString *) stringList;
} StringListRecord;

// Lightweight sample pair for raw forwarding (no compression/chunks)
typedef struct RawSample {
    int64_t timestamp;
    double value;
} RawSample;

// Lightweight label pair for raw forwarding
typedef struct RawLabel {
    char *key;
    size_t keyLen;
    char *value;
    size_t valueLen;
} RawLabel;

// Lightweight series representation for non-GROUPBY forwarding
typedef struct RawSeriesEntry {
    char *name;
    size_t nameLen;
    RawLabel *labels;
    size_t labelsCount;
    RawSample *samples;
    size_t samplesCount;
} RawSeriesEntry;

// Record that holds lightweight series data (avoids full Series object creation)
typedef struct RawSeriesListRecord {
    Record base;
    RawSeriesEntry *entries;
    size_t count;
} RawSeriesListRecord;

MRRecordType *GetMapRecordType();
MRRecordType *GetListRecordType();
MRRecordType *GetSeriesRecordType();
MRRecordType *GetSlotRangesRecordType();
MRRecordType *GetSeriesListRecordType();
MRRecordType *GetStringListRecordType();
MRRecordType *GetRawSeriesListRecordType();
void RawSeriesListRecord_Free(void *base);
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
bool IsMRCluster();

#endif // REDIS_TIMESERIES_CLEAN_MR_INTEGRATION_H
