
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
    if (unlikely(len > 0)) {
        RedisModule_ReplyWithError(rctx, "multi shard cmd failed");
        RedisModule_Log(rctx, "warning", "got libmr error:");
        for (size_t i = 0; i < len; ++i) {
            RedisModule_Log(rctx, "warning", MR_ExecutionCtxGetError(eCtx, i));
        }
        return true;
    }

    return false;
}

// This function used for calling freeing the blocked client context
// in the main thread. It's needed cause there is a bug in RoF when calling
// RedisModule_FreeThreadSafeContext from thread which is not the main one, see:
// https://redislabs.atlassian.net/browse/RED-68772 . It should be fixed in redis 7
void rts_free_rctx(RedisModuleCtx *rctx, void *privateData) {
    RedisModuleCtx *_rctx = privateData;
    RedisModule_FreeThreadSafeContext(_rctx);
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

static void mrange_done(ExecutionCtx *eCtx, void *privateData) {
    MRangeData *data = privateData;
    RedisModuleBlockedClient *bc = data->bc;
    RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(bc);

    if (unlikely(check_and_reply_on_error(eCtx, rctx))) {
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
                RedisModule_Log(rctx,
                                "warning",
                                "Unexpected record type: %s",
                                raw_listRecord->recordType->type.type);
                continue;
            }
            total_len += ListRecord_GetLen((ListRecord *)raw_listRecord);
        }
        RedisModule_ReplyWithArray(rctx, total_len);
    }

    Series **tempSeries = array_new(Record *, len); // calloc(len, sizeof(Series *));
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
            Record *raw_record = ListRecord_GetRecord((ListRecord *)raw_listRecord, j);
            if (raw_record->recordType != GetSeriesRecordType()) {
                continue;
            }
            Series *s = SeriesRecord_IntoSeries((SeriesRecord *)raw_record);
            tempSeries = array_append(tempSeries, s);

            if (data->args.groupByLabel) {
                ResultSet_AddSerie(resultset, s, RedisModule_StringPtrLen(s->keyName, NULL));
            } else {
                ReplySeriesArrayPos(rctx,
                                    s,
                                    data->args.withLabels,
                                    data->args.limitLabels,
                                    data->args.numLimitLabels,
                                    &data->args.rangeArgs,
                                    data->args.reverse);
            }
        }
    }

    if (data->args.groupByLabel) {
        // Apply the reducer
        RangeArgs args = data->args.rangeArgs;
        ResultSet_ApplyReducer(resultset, &args, &data->args.gropuByReducerArgs);

        // Do not apply the aggregation on the resultset, do apply max results on the final result
        RangeArgs minimizedArgs = data->args.rangeArgs;
        minimizedArgs.startTimestamp = 0;
        minimizedArgs.endTimestamp = UINT64_MAX;
        minimizedArgs.aggregationArgs.aggregationClass = NULL;
        minimizedArgs.aggregationArgs.timeDelta = 0;
        minimizedArgs.filterByTSArgs.hasValue = false;
        minimizedArgs.filterByValueArgs.hasValue = false;

        replyResultSet(rctx,
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
    RTS_UnblockClient(bc, rctx);
}

int TSDB_mget_RG(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    MGetArgs args;
    if (parseMGetCommand(ctx, argv, argc, &args) != REDISMODULE_OK) {
        return REDISMODULE_ERR;
    }

    QueryPredicates_Arg *queryArg = malloc(sizeof(QueryPredicates_Arg));
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
    queryArg->limitLabels = calloc(args.numLimitLabels, sizeof(RedisModuleString *));
    memcpy(
        queryArg->limitLabels, args.limitLabels, sizeof(RedisModuleString *) * args.numLimitLabels);
    for (int i = 0; i < queryArg->limitLabelsSize; i++) {
        RedisModule_RetainString(ctx, queryArg->limitLabels[i]);
    }

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
    MR_ExecutionSetOnDoneHandler(exec, mget_done, bc);

    MR_Run(exec);

    MR_FreeExecution(exec);
    MR_FreeExecutionBuilder(builder);
    return REDISMODULE_OK;
}

int TSDB_mrange_RG(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool reverse) {
    MRangeArgs args;
    if (parseMRangeCommand(ctx, argv, argc, &args) != REDISMODULE_OK) {
        return REDISMODULE_OK;
    }
    args.reverse = reverse;

    QueryPredicates_Arg *queryArg = malloc(sizeof(QueryPredicates_Arg));
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
    queryArg->limitLabels = calloc(args.numLimitLabels, sizeof(RedisModuleString *));
    memcpy(
        queryArg->limitLabels, args.limitLabels, sizeof(RedisModuleString *) * args.numLimitLabels);
    for (int i = 0; i < queryArg->limitLabelsSize; i++) {
        RedisModule_RetainString(ctx, queryArg->limitLabels[i]);
    }

    MRError *err = NULL;

    ExecutionBuilder *builder = MR_CreateExecutionBuilder("ShardSeriesMapper", queryArg);

    MR_ExecutionBuilderCollect(builder);

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

int TSDB_queryindex_RG(RedisModuleCtx *ctx, QueryPredicateList *queries) {
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

    ExecutionBuilder *builder = MR_CreateExecutionBuilder("ShardQueryindexMapper", queryArg);

    MR_ExecutionBuilderCollect(builder);

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
