
#include "libmr_commands.h"

#include "LibMR/src/mr.h"
#include "LibMR/src/utils/arr.h"
#include "consts.h"
#include "libmr_integration.h"
#include "query_language.h"
#include "reply.h"
#include "resultset.h"
#include "utils/blocked_client.h"

#include "rmutil/alloc.h"

static inline bool check_and_reply_on_error(ExecutionCtx *eCtx, RedisModuleCtx *rctx) {
    size_t len = MR_ExecutionCtxGetErrorsLen(eCtx);
    if (likely(len == 0))
        return false;

    RedisModule_Log(rctx, "warning", "got libmr error:");
    bool max_idle_reached = false;
    for (size_t i = 0; i < len; ++i) {
        RedisModule_Log(rctx, "warning", "%s", MR_ExecutionCtxGetError(eCtx, i));
        if (!strcmp("execution max idle reached", MR_ExecutionCtxGetError(eCtx, i))) {
            max_idle_reached = true;
        }
    }

    if (max_idle_reached) {
        RedisModule_ReplyWithError(rctx,
                                   "A multi-shard command failed because at least one shard "
                                   "did not reply within the given timeframe.");
    } else {
        char buf[512] = { 0 };
        snprintf(
            buf, sizeof(buf), "Multi-shard command failed. %s", MR_ExecutionCtxGetError(eCtx, 0));

        RedisModule_ReplyWithError(rctx, buf);
    }
    return true;
}

// This function used for calling freeing the blocked client context
// in the main thread. It's needed cause there is a bug in RoF when calling
// RedisModule_FreeThreadSafeContext from thread which is not the main one, see:
// https://redislabs.atlassian.net/browse/RED-68772 . It should be fixed in redis 7
void rts_free_rctx(RedisModuleCtx *rctx, void *privateData) {
    RedisModuleCtx *_rctx = privateData;
    RedisModule_FreeThreadSafeContext(_rctx);
}

static void queryindex_done_resp3(ExecutionCtx *eCtx, void *privateData) {
    RedisModuleBlockedClient *bc = privateData;
    RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(bc);

    if (unlikely(check_and_reply_on_error(eCtx, rctx))) {
        goto __done;
    }

    size_t len = MR_ExecutionCtxGetResultsLen(eCtx);
    size_t total_len = 0;
    for (int i = 0; i < len; i++) {
        Record *raw_listRecord = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_listRecord->recordType != GetListRecordType()) {
            RedisModule_Log(rctx,
                            "warning",
                            "Unexpected record type: %s",
                            raw_listRecord->recordType->type.type);
            continue;
        }
        total_len += ListRecord_GetLen((ListRecord *)raw_listRecord);
    }
    RedisModule_ReplyWithSet(rctx, total_len);

    for (int i = 0; i < len; i++) {
        Record *raw_listRecord = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_listRecord->recordType != GetListRecordType()) {
            RedisModule_Log(rctx,
                            "warning",
                            "Unexpected record type: %s",
                            raw_listRecord->recordType->type.type);
            continue;
        }

        size_t list_len = ListRecord_GetLen((ListRecord *)raw_listRecord);
        for (size_t j = 0; j < list_len; j++) {
            Record *r = ListRecord_GetRecord((ListRecord *)raw_listRecord, j);
            r->recordType->sendReply(rctx, r);
        }
    }

__done:
    RTS_UnblockClient(bc, rctx);
}

static void mget_done_resp3(ExecutionCtx *eCtx, void *privateData) {
    RedisModuleBlockedClient *bc = privateData;
    RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(bc);

    if (unlikely(check_and_reply_on_error(eCtx, rctx))) {
        goto __done;
    }

    size_t len = MR_ExecutionCtxGetResultsLen(eCtx);
    size_t total_len = 0;
    for (int i = 0; i < len; i++) {
        Record *raw_mapRecord = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_mapRecord->recordType != GetMapRecordType()) {
            RedisModule_Log(rctx,
                            "warning",
                            "Unexpected record type: %s",
                            raw_mapRecord->recordType->type.type);
            continue;
        }
        total_len += MapRecord_GetLen((MapRecord *)raw_mapRecord);
    }

    RedisModule_ReplyWithMap(rctx, total_len / 2);

    for (int i = 0; i < len; i++) {
        Record *raw_mapRecord = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_mapRecord->recordType != GetMapRecordType()) {
            RedisModule_Log(rctx,
                            "warning",
                            "Unexpected record type: %s",
                            raw_mapRecord->recordType->type.type);
            continue;
        }

        size_t map_len = MapRecord_GetLen((MapRecord *)raw_mapRecord);
        for (size_t j = 0; j < map_len; j++) {
            Record *r = MapRecord_GetRecord((MapRecord *)raw_mapRecord, j);
            r->recordType->sendReply(rctx, r);
        }
    }

__done:
    RTS_UnblockClient(bc, rctx);
}

static void mget_done(ExecutionCtx *eCtx, void *privateData) {
    RedisModuleBlockedClient *bc = privateData;
    RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(bc);

    if (unlikely(check_and_reply_on_error(eCtx, rctx))) {
        goto __done;
    }

    size_t len = MR_ExecutionCtxGetResultsLen(eCtx);
    size_t total_len = 0;
    for (int i = 0; i < len; i++) {
        Record *raw_listRecord = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_listRecord->recordType != GetListRecordType()) {
            RedisModule_Log(rctx,
                            "warning",
                            "Unexpected record type: %s",
                            raw_listRecord->recordType->type.type);
            continue;
        }
        total_len += ListRecord_GetLen((ListRecord *)raw_listRecord);
    }
    RedisModule_ReplyWithArray(rctx, total_len);

    for (int i = 0; i < len; i++) {
        Record *raw_listRecord = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_listRecord->recordType != GetListRecordType()) {
            RedisModule_Log(rctx,
                            "warning",
                            "Unexpected record type: %s",
                            raw_listRecord->recordType->type.type);
            continue;
        }

        size_t list_len = ListRecord_GetLen((ListRecord *)raw_listRecord);
        for (size_t j = 0; j < list_len; j++) {
            Record *r = ListRecord_GetRecord((ListRecord *)raw_listRecord, j);
            r->recordType->sendReply(rctx, r);
        }
    }

__done:
    RTS_UnblockClient(bc, rctx);
}

static void queryindex_resp3_done(ExecutionCtx *eCtx, void *privateData) {
    RedisModuleBlockedClient *bc = privateData;
    RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(bc);

    if (unlikely(check_and_reply_on_error(eCtx, rctx))) {
        goto __done;
    }

    size_t len = MR_ExecutionCtxGetResultsLen(eCtx);
    size_t total_len = 0;
    for (int i = 0; i < len; i++) {
        Record *raw_listRecord = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_listRecord->recordType != GetListRecordType()) {
            RedisModule_Log(rctx,
                            "warning",
                            "Unexpected record type: %s",
                            raw_listRecord->recordType->type.type);
            continue;
        }
        total_len += ListRecord_GetLen((ListRecord *)raw_listRecord);
    }
    RedisModule_ReplyWithSet(rctx, total_len);

    for (int i = 0; i < len; i++) {
        Record *raw_listRecord = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_listRecord->recordType != GetListRecordType()) {
            RedisModule_Log(rctx,
                            "warning",
                            "Unexpected record type: %s",
                            raw_listRecord->recordType->type.type);
            continue;
        }

        size_t list_len = ListRecord_GetLen((ListRecord *)raw_listRecord);
        for (size_t j = 0; j < list_len; j++) {
            Record *r = ListRecord_GetRecord((ListRecord *)raw_listRecord, j);
            r->recordType->sendReply(rctx, r);
        }
    }

__done:
    RTS_UnblockClient(bc, rctx);
}

static int compare_slot_ranges(const void *a, const void *b) {
    const RedisModuleSlotRange *ra = *(const RedisModuleSlotRange **)a;
    const RedisModuleSlotRange *rb = *(const RedisModuleSlotRange **)b;
    return (int)ra->start - (int)rb->start;
}

static bool valid_slot_ranges(ARR(RedisModuleSlotRange *) slotRanges) {
    size_t len = array_len(slotRanges);
    if (len == 0)
        return false;
    qsort(slotRanges, len, sizeof(*slotRanges), compare_slot_ranges);
    uint16_t slot = 0;
    for (size_t i = 0; i < len; i++) {
        if (slot != slotRanges[i]->start)
            return false;
        slot = 1 + slotRanges[i]->end;
    }
    return slot == (1 << 14);
}

#define SLOT_RANGES_ERROR "Query requires unavailable slots"

static void mrange_done(ExecutionCtx *eCtx, void *privateData) {
    MRangeData *data = privateData;
    RedisModuleBlockedClient *bc = data->bc;
    MRangeArgs *args = &data->args;
    RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(bc);
    ARR(RedisModuleSlotRange *) slotRanges = NULL;
    ARR(ARR(Series *)) nodesResults = NULL;

    if (unlikely(check_and_reply_on_error(eCtx, rctx))) {
        goto __done;
    }

    // Collect results
    size_t len = MR_ExecutionCtxGetResultsLen(eCtx);
    if (len % 2 != 0 || len == 0) {
        // There should be 2 results from each node: slot ranges and internal mrange
        RedisModule_Log(rctx, "warning", "Unexpected results from nodes");
        RedisModule_ReplyWithError(rctx, SLOT_RANGES_ERROR);
        goto __done;
    }
    slotRanges = array_new(RedisModuleSlotRange *,
                           len / 2); // Minimal capacity (in case each node has one range)
    nodesResults = array_new(ARR(Series *), len / 2);
    for (size_t i = 0; i < len; i++) {
        Record *r = MR_ExecutionCtxGetResult(eCtx, i);
        if (r->recordType == GetSlotRangesRecordType()) {
            RedisModuleSlotRangeArray *sra = ((SlotRangesRecord *)r)->slotRanges;
            for (size_t j = 0; j < sra->num_ranges; j++)
                slotRanges = array_append(slotRanges, sra->ranges + j);
            continue;
        }
        if (r->recordType == GetSeriesListRecordType()) {
            SeriesListRecord *record = (SeriesListRecord *)r;
            nodesResults = array_append(nodesResults, record->seriesList);
            continue;
        }
        RedisModule_Log(rctx, "warning", "Unexpected record type: %s", r->recordType->type.type);
        RedisModule_ReplyWithError(rctx, SLOT_RANGES_ERROR);
        goto __done;
    }

    bool redisClusterEnabled =
        (RedisModule_GetContextFlags(rctx) & REDISMODULE_CTX_FLAGS_CLUSTER) != 0;
    if (redisClusterEnabled && !valid_slot_ranges(slotRanges)) {
        RedisModule_Log(rctx, "warning", "Invalid slot ranges");
        RedisModule_ReplyWithError(rctx, SLOT_RANGES_ERROR);
        goto __done;
    }

    TS_ResultSet *resultset = NULL;
    if (args->groupByLabel) {
        resultset = ResultSet_Create();
        ResultSet_GroupbyLabel(resultset, args->groupByLabel);
    } else {
        size_t totalLen = 0;
        array_foreach(nodesResults, seriesList, totalLen += array_len(seriesList));
        RedisModule_ReplyWithMapOrArray(rctx, totalLen, false);
    }

    array_foreach(nodesResults, seriesList, {
        array_foreach(seriesList, s, {
            if (args->groupByLabel)
                ResultSet_AddSeries(resultset, s, RedisModule_StringPtrLen(s->keyName, NULL));
            else
                ReplySeriesArrayPos(rctx,
                                    s,
                                    args->withLabels,
                                    args->limitLabels,
                                    args->numLimitLabels,
                                    &args->rangeArgs,
                                    args->reverse,
                                    false);
        });
    });

    if (args->groupByLabel) {
        // Apply the reducer
        RangeArgs rangeArgs = args->rangeArgs;
        rangeArgs.latest = false; // we already handled the latest flag in the client side
        ResultSet_ApplyReducer(rctx, resultset, &rangeArgs, &args->groupByReducerArgs);

        // Do not apply the aggregation on the resultset, do apply max results on the final result
        RangeArgs minimizedArgs = args->rangeArgs;
        minimizedArgs.startTimestamp = 0;
        minimizedArgs.endTimestamp = UINT64_MAX;
        minimizedArgs.aggregationArgs.aggregationClass = NULL;
        minimizedArgs.aggregationArgs.timeDelta = 0;
        minimizedArgs.filterByTSArgs.hasValue = false;
        minimizedArgs.filterByValueArgs.hasValue = false;
        minimizedArgs.latest = false;

        replyResultSet(rctx,
                       resultset,
                       args->withLabels,
                       args->limitLabels,
                       args->numLimitLabels,
                       &minimizedArgs,
                       args->reverse);
        ResultSet_Free(resultset);
    }

__done:
    array_free(slotRanges);
    array_free(nodesResults);
    MRangeArgs_Free(&data->args);
    free(data);
    MR_ExecutionCtxSetDone(eCtx);

    RTS_UnblockClient(bc, rctx);
}

int TSDB_mget_MR(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    MGetArgs args;
    if (parseMGetCommand(ctx, argv, argc, &args) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    QueryPredicates_Arg *queryArg = malloc(sizeof *queryArg);
    queryArg->shouldReturnNull = false;
    queryArg->refCount = 1;
    queryArg->count = args.queryPredicates->count;
    queryArg->startTimestamp = 0;
    queryArg->endTimestamp = 0;
    queryArg->latest = args.latest;
    // moving ownership of queries to QueryPredicates_Arg
    queryArg->predicates = args.queryPredicates;
    queryArg->withLabels = args.withLabels;
    queryArg->limitLabelsSize = args.numLimitLabels;
    queryArg->limitLabels = calloc(args.numLimitLabels, sizeof *queryArg->limitLabels);
    memcpy(queryArg->limitLabels,
           args.limitLabels,
           args.numLimitLabels * sizeof *queryArg->limitLabels);
    for (int i = 0; i < queryArg->limitLabelsSize; i++) {
        RedisModule_RetainString(ctx, queryArg->limitLabels[i]);
    }
    queryArg->resp3 = _ReplyMap(ctx);
    MRError *err = NULL;
    ExecutionBuilder *builder = MR_CreateExecutionBuilder("ShardMgetMapper", queryArg);

    MR_ExecutionBuilderCollect(builder);

    Execution *exec = MR_CreateExecution(builder, &err);
    if (err) {
        RedisModule_ReplyWithError(ctx, MR_ErrorGetMessage(err));
        MR_FreeExecutionBuilder(builder);
        return REDISMODULE_OK;
    }

    RedisModuleBlockedClient *bc = RTS_BlockClient(ctx, rts_free_rctx);
    MR_ExecutionSetOnDoneHandler(exec, queryArg->resp3 ? mget_done_resp3 : mget_done, bc);

    MR_Run(exec);

    MR_FreeExecution(exec);
    MR_FreeExecutionBuilder(builder);
    return REDISMODULE_OK;
}

int TSDB_mrange_MR(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool reverse) {
    MRangeArgs args;
    if (parseMRangeCommand(ctx, argv, argc, &args) != REDISMODULE_OK) {
        return REDISMODULE_OK;
    }
    args.reverse = reverse;

    QueryPredicates_Arg *queryArg = malloc(sizeof *queryArg);
    queryArg->shouldReturnNull = false;
    queryArg->refCount = 1;
    queryArg->count = args.queryPredicates->count;
    queryArg->startTimestamp = args.rangeArgs.startTimestamp;
    queryArg->endTimestamp = args.rangeArgs.endTimestamp;
    queryArg->latest = args.rangeArgs.latest;
    args.queryPredicates->ref++;
    queryArg->predicates = args.queryPredicates;
    queryArg->withLabels = args.withLabels;
    queryArg->limitLabelsSize = args.numLimitLabels;
    queryArg->limitLabels = calloc(args.numLimitLabels, sizeof *queryArg->limitLabels);
    memcpy(queryArg->limitLabels,
           args.limitLabels,
           args.numLimitLabels * sizeof *queryArg->limitLabels);
    for (int i = 0; i < queryArg->limitLabelsSize; i++) {
        RedisModule_RetainString(ctx, queryArg->limitLabels[i]);
    }

    MRError *err = NULL;

    ExecutionBuilder *builder = MR_CreateEmptyExecutionBuilder();
    MR_ExecutionBuilderInternalCommand(builder, "TS.INTERNAL_SLOT_RANGES", NULL);
    MR_ExecutionBuilderInternalCommand(builder, "TS.INTERNAL_MRANGE", queryArg);
    Execution *exec = MR_CreateExecution(builder, &err);
    if (err) {
        RedisModule_ReplyWithError(ctx, MR_ErrorGetMessage(err));
        MR_FreeExecutionBuilder(builder);
        return REDISMODULE_OK;
    }

    RedisModuleBlockedClient *bc = RTS_BlockClient(ctx, rts_free_rctx);
    MRangeData *data = malloc(sizeof(struct MRangeData));
    data->bc = bc;
    data->args = args;
    MR_ExecutionSetOnDoneHandler(exec, mrange_done, data);

    MR_Run(exec);
    MR_FreeExecution(exec);
    MR_FreeExecutionBuilder(builder);
    return REDISMODULE_OK;
}

int TSDB_queryindex_MR(RedisModuleCtx *ctx, QueryPredicateList *queries) {
    MRError *err = NULL;

    QueryPredicates_Arg *queryArg = malloc(sizeof(QueryPredicates_Arg));
    queryArg->shouldReturnNull = false;
    queryArg->refCount = 1;
    queryArg->count = queries->count;
    queryArg->startTimestamp = 0;
    queryArg->endTimestamp = 0;
    queries->ref++;
    queryArg->predicates = queries;
    queryArg->withLabels = false;
    queryArg->limitLabelsSize = 0;
    queryArg->limitLabels = NULL;
    queryArg->resp3 = _ReplySet(ctx);

    ExecutionBuilder *builder = MR_CreateExecutionBuilder("ShardQueryindexMapper", queryArg);

    MR_ExecutionBuilderCollect(builder);

    Execution *exec = MR_CreateExecution(builder, &err);
    if (err) {
        RedisModule_ReplyWithError(ctx, MR_ErrorGetMessage(err));
        MR_FreeExecutionBuilder(builder);
        return REDISMODULE_OK;
    }

    RedisModuleBlockedClient *bc = RTS_BlockClient(ctx, rts_free_rctx);
    MR_ExecutionSetOnDoneHandler(exec, queryArg->resp3 ? queryindex_resp3_done : mget_done, bc);

    MR_Run(exec);

    MR_FreeExecution(exec);
    MR_FreeExecutionBuilder(builder);
    return REDISMODULE_OK;
}
