#include "libmr_commands.h"

#include "LibMR/src/utils/arr.h"
#include "LibMR/src/mr.h"
#include "LibMR/src/cluster.h"
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
static void rts_free_rctx(RedisModuleCtx *rctx, void *privateData) {
    RedisModuleCtx *_rctx = privateData;
    RedisModule_FreeThreadSafeContext(_rctx);
}

static int compare_slot_ranges(const void *a, const void *b) {
    const RedisModuleSlotRange *ra = *(const RedisModuleSlotRange **)a;
    const RedisModuleSlotRange *rb = *(const RedisModuleSlotRange **)b;
    return (int)ra->start - (int)rb->start;
}

#define SLOT_RANGES_ERROR "Query requires unavailable slots"

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

static void *collect_node_results(ExecutionCtx *eCtx, RedisModuleCtx *ctx) {
    if (unlikely(check_and_reply_on_error(eCtx, ctx)))
        return NULL;

    size_t len = MR_ExecutionCtxGetResultsLen(eCtx);
    if (len == 0 || len % MR_ClusterGetSize() != 0) {
        // Each node should return the same number of results because they were all ran the same
        // internal commands
        RedisModule_Log(ctx, "warning", "Unexpected results from nodes");
        RedisModule_ReplyWithError(ctx, SLOT_RANGES_ERROR);
        return NULL;
    }

    // Note that there could be more than one slot range per node, in which case the
    // array_len(slotRanges) will expand and become larger than the cluster size, but this is a good
    // initial capacity.
    ARR(RedisModuleSlotRange *) slotRanges = array_new(RedisModuleSlotRange *, MR_ClusterGetSize());
    // The actual type of the nodesResult will be determined dynamically (below).
    // Each entry will hold the full collection of results from a node's reply to an internal
    // command.
    ARR(void *) nodesResults = array_new(void *, MR_ClusterGetSize());
    // We keep track of the type to ensure different nodes don't reply with different types.
    MRRecordType *nodesResultsType = NULL;

    for (size_t i = 0; i < len; i++) {
        Record *r = MR_ExecutionCtxGetResult(eCtx, i);
        if (r->recordType == GetSlotRangesRecordType()) {
            RedisModuleSlotRangeArray *sra = ((SlotRangesRecord *)r)->slotRanges;
            for (size_t j = 0; j < sra->num_ranges; j++)
                slotRanges = array_append(slotRanges, sra->ranges + j);
            continue;
        }

        if (nodesResultsType && nodesResultsType != r->recordType) {
            RedisModule_Log(ctx, "warning", "Mixed node result types");
            RedisModule_ReplyWithError(ctx, SLOT_RANGES_ERROR);
            goto __error;
        }
        nodesResultsType = r->recordType;

        if (r->recordType == GetSeriesListRecordType()) {
            SeriesListRecord *record = (SeriesListRecord *)r;
            nodesResults = array_append(nodesResults, record->seriesList);
            continue;
        }
        if (r->recordType == GetStringListRecordType()) {
            StringListRecord *record = (StringListRecord *)r;
            nodesResults = array_append(nodesResults, record->stringList);
            continue;
        }

        RedisModule_Log(ctx, "warning", "Unexpected record type: %s", r->recordType->type.type);
        RedisModule_ReplyWithError(ctx, SLOT_RANGES_ERROR);
        goto __error;
    }

    bool redisClusterEnabled =
        (RedisModule_GetContextFlags(ctx) & REDISMODULE_CTX_FLAGS_CLUSTER) != 0;
    if (redisClusterEnabled && !valid_slot_ranges(slotRanges)) {
        RedisModule_Log(ctx, "warning", "Invalid slot ranges");
        RedisModule_ReplyWithError(ctx, SLOT_RANGES_ERROR);
        goto __error;
    }

    array_free(slotRanges);
    return nodesResults;

__error:
    array_free(slotRanges);
    array_free(nodesResults);
    return NULL;
}

static void mrange_done_internal(ExecutionCtx *eCtx, RedisModuleCtx *ctx, MRangeData *data) {
    MRangeArgs *args = &data->args;
    RedisModuleBlockedClient *bc = data->bc;

    ARR(ARR(Series *)) nodesResults = collect_node_results(eCtx, ctx);
    if (!nodesResults)
        goto __done;

    TS_ResultSet *resultset = NULL;
    if (args->groupByLabel) {
        resultset = ResultSet_Create();
        ResultSet_GroupbyLabel(resultset, args->groupByLabel);
    } else {
        size_t totalLen = 0;
        array_foreach(nodesResults, seriesList, totalLen += array_len(seriesList));
        RedisModule_ReplyWithArray(ctx, totalLen);
    }

    array_foreach(nodesResults, seriesList, {
        array_foreach(seriesList, s, {
            if (args->groupByLabel)
                ResultSet_AddSeries(resultset, s, RedisModule_StringPtrLen(s->keyName, NULL));
            else
                ReplySeriesArrayPos(ctx,
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
        ResultSet_ApplyReducer(ctx, resultset, &rangeArgs, &args->groupByReducerArgs);

        // Do not apply the aggregation on the resultset, do apply max results on the final result
        RangeArgs minimizedArgs = args->rangeArgs;
        minimizedArgs.startTimestamp = 0;
        minimizedArgs.endTimestamp = UINT64_MAX;
        minimizedArgs.aggregationArgs.aggregationClass = NULL;
        minimizedArgs.aggregationArgs.timeDelta = 0;
        minimizedArgs.filterByTSArgs.hasValue = false;
        minimizedArgs.filterByValueArgs.hasValue = false;
        minimizedArgs.latest = false;

        replyResultSet(ctx,
                       resultset,
                       args->withLabels,
                       args->limitLabels,
                       args->numLimitLabels,
                       &minimizedArgs,
                       args->reverse);
        ResultSet_Free(resultset);
    }

__done:
    array_free(nodesResults);
    MRangeArgs_Free(&data->args);
    free(data);
    MR_ExecutionCtxSetDone(eCtx);
}

static void mrange_done_gears(ExecutionCtx *eCtx, RedisModuleCtx *ctx, MRangeData *data) {
    RedisModuleBlockedClient *bc = data->bc;
    RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(bc);

    if (unlikely(check_and_reply_on_error(eCtx, ctx))) {
        goto __done;
    }

    long long len = MR_ExecutionCtxGetResultsLen(eCtx);

    TS_ResultSet *resultset = NULL;

    if (data->args.groupByLabel) {
        resultset = ResultSet_Create();
        ResultSet_GroupbyLabel(resultset, data->args.groupByLabel);
    } else {
        size_t total_len = 0;
        for (int i = 0; i < len; i++) {
            Record *raw_listRecord = MR_ExecutionCtxGetResult(eCtx, i);
            if (raw_listRecord->recordType != GetListRecordType()) {
                RedisModule_Log(ctx,
                                "warning",
                                "Unexpected record type: %s",
                                raw_listRecord->recordType->type.type);
                continue;
            }
            total_len += ListRecord_GetLen((ListRecord *)raw_listRecord);
        }
        ReplyWithMapOrArray(ctx, total_len, false);
    }

    Series **tempSeries = array_new(Record *, len); // calloc(len, sizeof(Series *));
    for (int i = 0; i < len; i++) {
        Record *raw_listRecord = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_listRecord->recordType != GetListRecordType()) {
            RedisModule_Log(ctx,
                            "warning",
                            "Unexpected record type: %s",
                            raw_listRecord->recordType->type.type);
            continue;
        }

        size_t list_len = ListRecord_GetLen((ListRecord *)raw_listRecord);
        for (size_t j = 0; j < list_len; j++) {
            Record *raw_record = ListRecord_GetRecord((ListRecord *)raw_listRecord, j);
            if (raw_record->recordType != GetSeriesRecordType()) {
                continue;
            }
            Series *s = SeriesRecord_IntoSeries((SeriesRecord *)raw_record);
            tempSeries = array_append(tempSeries, s);

            if (data->args.groupByLabel) {
                ResultSet_AddSeries(resultset, s, RedisModule_StringPtrLen(s->keyName, NULL));
            } else {
                ReplySeriesArrayPos(ctx,
                                    s,
                                    data->args.withLabels,
                                    data->args.limitLabels,
                                    data->args.numLimitLabels,
                                    &data->args.rangeArgs,
                                    data->args.reverse,
                                    false);
            }
        }
    }

    if (data->args.groupByLabel) {
        // Apply the reducer
        RangeArgs args = data->args.rangeArgs;
        args.latest = false; // we already handled the latest flag in the client side
        ResultSet_ApplyReducer(ctx, resultset, &args, &data->args.groupByReducerArgs);

        // Do not apply the aggregation on the resultset, do apply max results on the final result
        RangeArgs minimizedArgs = data->args.rangeArgs;
        minimizedArgs.startTimestamp = 0;
        minimizedArgs.endTimestamp = UINT64_MAX;
        minimizedArgs.aggregationArgs.aggregationClass = NULL;
        minimizedArgs.aggregationArgs.timeDelta = 0;
        minimizedArgs.filterByTSArgs.hasValue = false;
        minimizedArgs.filterByValueArgs.hasValue = false;
        minimizedArgs.latest = false;

        replyResultSet(ctx,
                       resultset,
                       data->args.withLabels,
                       data->args.limitLabels,
                       data->args.numLimitLabels,
                       &minimizedArgs,
                       data->args.reverse);

        ResultSet_Free(resultset);
    }
    array_foreach(tempSeries, x, FreeSeries(x));
    array_free(tempSeries);

__done:
    MRangeArgs_Free(&data->args);
    free(data);
}

static void mrange_done(ExecutionCtx *eCtx, void *privateData) {
    MRangeData *data = privateData;
    RedisModuleBlockedClient *bc = data->bc;
    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(bc);

    switch (TSGlobalConfig.libmrProtocol) {
        case LIBMR_PROTOCOL_GEARS:
            mrange_done_gears(eCtx, ctx, data);
            break;
        case LIBMR_PROTOCOL_INTERNAL:
            mrange_done_internal(eCtx, ctx, data);
            break;
        default:
            RedisModule_ReplyWithError(ctx, "Unknown LibMR protocol");
    }

    RTS_UnblockClient(bc, ctx);
}

static void mget_done_internal(ExecutionCtx *eCtx,
                               RedisModuleCtx *ctx,
                               RedisModuleBlockedClient *bc) {
    ARR(ARR(Series *)) nodesResults = collect_node_results(eCtx, ctx);
    if (!nodesResults)
        goto __done;

    ReplyWithMapOrArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN, false);
    size_t len = 0;
    array_foreach(nodesResults, seriesList, {
        array_foreach(seriesList, s, {
            if (!_ReplyMap(ctx))
                RedisModule_ReplyWithArray(ctx, 3); // name, labels, sample
            RedisModule_ReplyWithString(ctx, s->keyName);
            if (_ReplyMap(ctx))
                RedisModule_ReplyWithArray(ctx, 2);
            ReplyWithSeriesLabels(ctx, s);
            ReplyWithSeriesLastDatapoint(ctx, s);
            len++;
        });
    });
    ReplySetMapOrArrayLength(ctx, len, false);

__done:
    array_free(nodesResults);
    MR_ExecutionCtxSetDone(eCtx);
}

static void mget_done_gears(ExecutionCtx *eCtx, RedisModuleCtx *ctx, RedisModuleBlockedClient *bc) {
    if (unlikely(check_and_reply_on_error(eCtx, ctx)))
        return;

    size_t len = MR_ExecutionCtxGetResultsLen(eCtx);
    size_t total_len = 0;
    for (int i = 0; i < len; i++) {
        Record *raw_listRecord = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_listRecord->recordType != GetListRecordType()) {
            RedisModule_Log(ctx,
                            "warning",
                            "Unexpected record type: %s",
                            raw_listRecord->recordType->type.type);
            continue;
        }
        total_len += ListRecord_GetLen((ListRecord *)raw_listRecord);
    }
    RedisModule_ReplyWithArray(ctx, total_len);

    for (int i = 0; i < len; i++) {
        Record *raw_listRecord = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_listRecord->recordType != GetListRecordType()) {
            RedisModule_Log(ctx,
                            "warning",
                            "Unexpected record type: %s",
                            raw_listRecord->recordType->type.type);
            continue;
        }

        size_t list_len = ListRecord_GetLen((ListRecord *)raw_listRecord);
        for (size_t j = 0; j < list_len; j++) {
            Record *r = ListRecord_GetRecord((ListRecord *)raw_listRecord, j);
            r->recordType->sendReply(ctx, r);
        }
    }
}

static void mget_done(ExecutionCtx *eCtx, void *privateData) {
    RedisModuleBlockedClient *bc = privateData;
    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(bc);

    switch (TSGlobalConfig.libmrProtocol) {
        case LIBMR_PROTOCOL_GEARS:
            mget_done_gears(eCtx, ctx, bc);
            break;
        case LIBMR_PROTOCOL_INTERNAL:
            mget_done_internal(eCtx, ctx, bc);
            break;
        default:
            RedisModule_ReplyWithError(ctx, "Unknown LibMR protocol");
    }

    RTS_UnblockClient(bc, ctx);
}

static void queryindex_done_internal(ExecutionCtx *eCtx,
                                     RedisModuleCtx *ctx,
                                     RedisModuleBlockedClient *bc) {
    ARR(ARR(RedisModuleString *)) nodesResults = collect_node_results(eCtx, ctx);
    if (!nodesResults)
        goto __done;

    ReplyWithSetOrArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
    size_t len = 0;
    array_foreach(nodesResults, stringList, {
        array_foreach(stringList, keyName, {
            RedisModule_ReplyWithString(ctx, keyName);
            len++;
        });
    });
    ReplySetSetOrArrayLength(ctx, len);

__done:
    array_free(nodesResults);
    MR_ExecutionCtxSetDone(eCtx);
}

static void queryindex_done_gears(ExecutionCtx *eCtx,
                                  RedisModuleCtx *ctx,
                                  RedisModuleBlockedClient *bc) {
    if (unlikely(check_and_reply_on_error(eCtx, ctx)))
        return;

    size_t len = MR_ExecutionCtxGetResultsLen(eCtx);
    size_t total_len = 0;
    for (int i = 0; i < len; i++) {
        Record *raw_listRecord = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_listRecord->recordType != GetListRecordType()) {
            RedisModule_Log(ctx,
                            "warning",
                            "Unexpected record type: %s",
                            raw_listRecord->recordType->type.type);
            continue;
        }
        total_len += ListRecord_GetLen((ListRecord *)raw_listRecord);
    }
    RedisModule_ReplyWithSet(ctx, total_len);

    for (int i = 0; i < len; i++) {
        Record *raw_listRecord = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_listRecord->recordType != GetListRecordType()) {
            RedisModule_Log(ctx,
                            "warning",
                            "Unexpected record type: %s",
                            raw_listRecord->recordType->type.type);
            continue;
        }

        size_t list_len = ListRecord_GetLen((ListRecord *)raw_listRecord);
        for (size_t j = 0; j < list_len; j++) {
            Record *r = ListRecord_GetRecord((ListRecord *)raw_listRecord, j);
            r->recordType->sendReply(ctx, r);
        }
    }
}

static void queryindex_done(ExecutionCtx *eCtx, void *privateData) {
    RedisModuleBlockedClient *bc = privateData;
    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(bc);

    switch (TSGlobalConfig.libmrProtocol) {
        case LIBMR_PROTOCOL_GEARS:
            queryindex_done_gears(eCtx, ctx, bc);
            break;
        case LIBMR_PROTOCOL_INTERNAL:
            queryindex_done_internal(eCtx, ctx, bc);
            break;
        default:
            RedisModule_ReplyWithError(ctx, "Unknown LibMR protocol");
    }

    RTS_UnblockClient(bc, ctx);
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

    ExecutionBuilder *builder = NULL;
    switch (TSGlobalConfig.libmrProtocol) {
        case LIBMR_PROTOCOL_GEARS: {
            builder = MR_CreateExecutionBuilder("ShardMgetMapper", queryArg);
            MR_ExecutionBuilderCollect(builder);
            break;
        }
        case LIBMR_PROTOCOL_INTERNAL: {
            builder = MR_CreateEmptyExecutionBuilder();
            MR_ExecutionBuilderInternalCommand(builder, "TS.INTERNAL_SLOT_RANGES", NULL);
            MR_ExecutionBuilderInternalCommand(builder, "TS.INTERNAL_MGET", queryArg);
            break;
        }
        default: {
            RedisModule_ReplyWithError(ctx, "Unknown LibMR protocol");
            return REDISMODULE_OK;
        }
    }
    Execution *exec = MR_CreateExecution(builder, &err);
    if (err) {
        RedisModule_ReplyWithError(ctx, MR_ErrorGetMessage(err));
        MR_FreeExecutionBuilder(builder);
        return REDISMODULE_OK;
    }

    RedisModuleBlockedClient *bc = RTS_BlockClient(ctx, rts_free_rctx);
    MR_ExecutionSetOnDoneHandler(exec, mget_done, bc);

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

    ExecutionBuilder *builder = NULL;
    switch (TSGlobalConfig.libmrProtocol) {
        case LIBMR_PROTOCOL_GEARS: {
            builder = MR_CreateExecutionBuilder("ShardSeriesMapper", queryArg);
            MR_ExecutionBuilderCollect(builder);
            break;
        }
        case LIBMR_PROTOCOL_INTERNAL: {
            builder = MR_CreateEmptyExecutionBuilder();
            MR_ExecutionBuilderInternalCommand(builder, "TS.INTERNAL_SLOT_RANGES", NULL);
            MR_ExecutionBuilderInternalCommand(builder, "TS.INTERNAL_MRANGE", queryArg);
            break;
        }
        default: {
            RedisModule_ReplyWithError(ctx, "Unknown LibMR protocol");
            return REDISMODULE_OK;
        }
    }
    Execution *exec = MR_CreateExecution(builder, &err);
    if (err) {
        RedisModule_ReplyWithError(ctx, MR_ErrorGetMessage(err));
        MR_FreeExecutionBuilder(builder);
        return REDISMODULE_OK;
    }

    RedisModuleBlockedClient *bc = RTS_BlockClient(ctx, rts_free_rctx);
    MRangeData *data = malloc(sizeof(struct MRangeData)); // freed by mrange_done
    data->bc = bc;
    data->args = args;
    MR_ExecutionSetOnDoneHandler(exec, mrange_done, data);

    MR_Run(exec);
    MR_FreeExecution(exec);
    MR_FreeExecutionBuilder(builder);
    return REDISMODULE_OK;
}

int TSDB_queryindex_MR(RedisModuleCtx *ctx, QueryPredicateList *queries) {
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

    MRError *err = NULL;

    ExecutionBuilder *builder = NULL;
    switch (TSGlobalConfig.libmrProtocol) {
        case LIBMR_PROTOCOL_GEARS: {
            builder = MR_CreateExecutionBuilder("ShardQueryindexMapper", queryArg);
            MR_ExecutionBuilderCollect(builder);
            break;
        }
        case LIBMR_PROTOCOL_INTERNAL: {
            builder = MR_CreateEmptyExecutionBuilder();
            MR_ExecutionBuilderInternalCommand(builder, "TS.INTERNAL_SLOT_RANGES", NULL);
            MR_ExecutionBuilderInternalCommand(builder, "TS.INTERNAL_QUERYINDEX", queryArg);
            break;
        }
        default: {
            RedisModule_ReplyWithError(ctx, "Unknown LibMR protocol");
            return REDISMODULE_OK;
        }
    }
    Execution *exec = MR_CreateExecution(builder, &err);
    if (err) {
        RedisModule_ReplyWithError(ctx, MR_ErrorGetMessage(err));
        MR_FreeExecutionBuilder(builder);
        return REDISMODULE_OK;
    }

    RedisModuleBlockedClient *bc = RTS_BlockClient(ctx, rts_free_rctx);
    MR_ExecutionSetOnDoneHandler(exec, queryindex_done, bc);

    MR_Run(exec);
    MR_FreeExecution(exec);
    MR_FreeExecutionBuilder(builder);
    return REDISMODULE_OK;
}
