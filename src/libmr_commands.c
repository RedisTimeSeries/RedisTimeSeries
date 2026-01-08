
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

#include <string.h>

static inline bool check_and_reply_on_error(ExecutionCtx *eCtx, RedisModuleCtx *rctx) {
    size_t len = MR_ExecutionCtxGetErrorsLen(eCtx);
    if (unlikely(len > 0)) {
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
            snprintf(buf,
                     sizeof(buf),
                     "Multi-shard command failed. %s",
                     MR_ExecutionCtxGetError(eCtx, 0));

            RedisModule_ReplyWithError(rctx, buf);
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

static void mrange_done(ExecutionCtx *eCtx, void *privateData) {
    MRangeData *data = privateData;
    RedisModuleBlockedClient *bc = data->bc;
    RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(bc);

    if (unlikely(check_and_reply_on_error(eCtx, rctx))) {
        goto __done;
    }

    long long len = MR_ExecutionCtxGetResultsLen(eCtx);

    const bool groupby_reduce_count_pushdown =
        data->args.groupByLabel && data->args.gropuByReducerArgs.agg_type == TS_AGG_COUNT;

    TS_ResultSet *resultset = NULL;

    if (groupby_reduce_count_pushdown) {
        // Shards already returned reduced "COUNT" series per group.
        // We only need to merge per-group partial results by summing counts per timestamp.
        RedisModuleDict *groups = RedisModule_CreateDict(NULL);
        Series **tempSeries = array_new(Series *, len);

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

                size_t klen = 0;
                const char *k = RedisModule_StringPtrLen(s->keyName, &klen);
                int nokey = 0;
                Series **partials = (Series **)RedisModule_DictGetC(groups, (void *)k, klen, &nokey);
                if (nokey) {
                    partials = array_new(Series *, 1);
                }
                partials = array_append(partials, s);
                RedisModule_DictSetC(groups, (void *)k, klen, partials);
            }
        }

        // Prepare the reply args: don't re-apply aggregation/filters, but keep COUNT limit.
        RangeArgs minimizedArgs = data->args.rangeArgs;
        minimizedArgs.startTimestamp = 0;
        minimizedArgs.endTimestamp = UINT64_MAX;
        minimizedArgs.aggregationArgs.aggregationClass = NULL;
        minimizedArgs.aggregationArgs.timeDelta = 0;
        minimizedArgs.filterByTSArgs.hasValue = false;
        minimizedArgs.filterByValueArgs.hasValue = false;
        minimizedArgs.latest = false;

        RedisModule_ReplyWithMapOrArray(rctx, RedisModule_DictSize(groups), false);

        // Merge reducer output across shards by summing per-timestamp counts.
        ReducerArgs sumReducer = { .agg_type = TS_AGG_SUM, .aggregationClass = GetAggClass(TS_AGG_SUM) };
        RangeArgs rawArgs = { 0 };
        rawArgs.startTimestamp = 0;
        rawArgs.endTimestamp = UINT64_MAX;
        rawArgs.count = -1;
        rawArgs.latest = false;
        rawArgs.alignment = DefaultAlignment;
        rawArgs.aggregationArgs.aggregationClass = NULL;
        rawArgs.filterByTSArgs.hasValue = false;
        rawArgs.filterByValueArgs.hasValue = false;

        RedisModuleDictIter *giter = RedisModule_DictIteratorStartC(groups, "^", NULL, 0);
        char *groupKey = NULL;
        size_t groupKeyLen = 0;
        Series **partials = NULL;

        // Use the same chunk size used by the existing group-by reducer path.
        CreateCtx cCtx = {
            .labels = NULL, .labelsCount = 0, .chunkSizeBytes = Chunk_SIZE_BYTES_SECS, .options = 0
        };
        cCtx.options |= SERIES_OPT_UNCOMPRESSED;

        while ((groupKey = RedisModule_DictNextC(giter, &groupKeyLen, (void **)&partials)) != NULL) {
            if (!partials || array_len(partials) == 0) {
                continue;
            }

            Series *first = partials[0];
            Series *merged = NewSeries(RedisModule_CreateStringFromString(NULL, first->keyName), &cCtx);

            // Labels: <label>=<value>, __reducer__=count, __source__=<comma-separated keys>.
            Label *labels = calloc(3, sizeof(Label));
            labels[0].key = RedisModule_CreateStringFromString(NULL, first->labels[0].key);
            labels[0].value = RedisModule_CreateStringFromString(NULL, first->labels[0].value);
            labels[1].key = RedisModule_CreateString(NULL, "__reducer__", strlen("__reducer__"));
            labels[1].value = RedisModule_CreateString(NULL, "count", strlen("count"));
            labels[2].key = RedisModule_CreateString(NULL, "__source__", strlen("__source__"));
            labels[2].value = RedisModule_CreateString(NULL, "", 0);

            bool first_src = true;
            for (uint32_t i = 0; i < array_len(partials); i++) {
                Series *p = partials[i];
                // "__source__" is always the last label in reduced series.
                RedisModuleString *srcVal = p->labels[p->labelsCount - 1].value;
                size_t srcLen = 0;
                const char *src = RedisModule_StringPtrLen(srcVal, &srcLen);
                if (srcLen == 0) {
                    continue;
                }
                if (!first_src) {
                    RedisModule_StringAppendBuffer(NULL, labels[2].value, ",", 1);
                }
                RedisModule_StringAppendBuffer(NULL, labels[2].value, src, srcLen);
                first_src = false;
            }

            merged->labels = labels;
            merged->labelsCount = 3;

            // Merge values: SUM(partial counts) for each timestamp.
            MultiSerieReduce(merged, partials, array_len(partials), &sumReducer, &rawArgs);

            ReplySeriesArrayPos(rctx,
                                merged,
                                data->args.withLabels,
                                data->args.limitLabels,
                                data->args.numLimitLabels,
                                &minimizedArgs,
                                data->args.reverse,
                                true);

            FreeSeries(merged);
            array_free(partials);
        }

        RedisModule_DictIteratorStop(giter);
        RedisModule_FreeDict(NULL, groups);

        array_foreach(tempSeries, x, FreeSeries(x));
        array_free(tempSeries);

        goto __done;
    }

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
        RedisModule_ReplyWithMapOrArray(rctx, total_len, false);
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
                                    data->args.reverse,
                                    false);
            }
        }
    }

    if (data->args.groupByLabel) {
        // Apply the reducer
        RangeArgs args = data->args.rangeArgs;
        args.latest = false; // we already handled the latest flag in the client side
        ResultSet_ApplyReducer(rctx, resultset, &args, &data->args.gropuByReducerArgs);

        // Do not apply the aggregation on the resultset, do apply max results on the final result
        RangeArgs minimizedArgs = data->args.rangeArgs;
        minimizedArgs.startTimestamp = 0;
        minimizedArgs.endTimestamp = UINT64_MAX;
        minimizedArgs.aggregationArgs.aggregationClass = NULL;
        minimizedArgs.aggregationArgs.timeDelta = 0;
        minimizedArgs.filterByTSArgs.hasValue = false;
        minimizedArgs.filterByValueArgs.hasValue = false;
        minimizedArgs.latest = false;

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

    // No push-down for mget.
    queryArg->flags = 0;
    queryArg->groupByLabel = NULL;
    memset(&queryArg->rangeArgs, 0, sizeof(queryArg->rangeArgs));
    queryArg->rangeAggType = TS_AGG_NONE;
    memset(&queryArg->reducerArgs, 0, sizeof(queryArg->reducerArgs));

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

int TSDB_mrange_RG(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool reverse) {
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
    queryArg->resp3 = _ReplyMap(ctx);

    // Default: no push-down.
    queryArg->flags = 0;
    queryArg->groupByLabel = NULL;
    memset(&queryArg->rangeArgs, 0, sizeof(queryArg->rangeArgs));
    queryArg->rangeAggType = TS_AGG_NONE;
    memset(&queryArg->reducerArgs, 0, sizeof(queryArg->reducerArgs));

    // Optimization: for GROUPBY <label> REDUCE COUNT in cluster mode, push the reduction to shards.
    if (args.groupByLabel && args.gropuByReducerArgs.agg_type == TS_AGG_COUNT) {
        queryArg->flags |= QP_FLAG_MRANGE_GROUPBY_REDUCE_PUSHDOWN;
        queryArg->groupByLabel =
            RedisModule_CreateString(ctx, args.groupByLabel, strlen(args.groupByLabel));
        queryArg->rangeArgs = args.rangeArgs;
        queryArg->rangeAggType =
            args.rangeArgs.aggregationArgs.aggregationClass
                ? args.rangeArgs.aggregationArgs.aggregationClass->type
                : TS_AGG_NONE;
        queryArg->reducerArgs.agg_type = args.gropuByReducerArgs.agg_type;
        queryArg->reducerArgs.aggregationClass = args.gropuByReducerArgs.aggregationClass;
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
    queryArg->resp3 = _ReplySet(ctx);

    // No push-down for queryindex.
    queryArg->latest = false;
    queryArg->flags = 0;
    queryArg->groupByLabel = NULL;
    memset(&queryArg->rangeArgs, 0, sizeof(queryArg->rangeArgs));
    queryArg->rangeAggType = TS_AGG_NONE;
    memset(&queryArg->reducerArgs, 0, sizeof(queryArg->reducerArgs));

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
