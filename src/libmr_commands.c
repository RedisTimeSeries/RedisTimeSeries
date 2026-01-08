
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

#include <stdio.h>
#include <string.h>

// Keep behavior similar to LibMR's default max idle (see deps/LibMR/src/mr.c).
#define RTS_LIBMR_REMOTE_TASK_TIMEOUT_MS 5000

static inline bool check_and_reply_on_remote_errors(MRError **errs,
                                                    size_t nErrs,
                                                    RedisModuleCtx *rctx) {
    if (unlikely(nErrs > 0)) {
        RedisModule_Log(rctx, "warning", "got libmr error:");
        bool timeout_reached = false;
        for (size_t i = 0; i < nErrs; ++i) {
            const char *msg = MR_ErrorGetMessage(errs[i]);
            RedisModule_Log(rctx, "warning", "%s", msg);
            if (msg && strstr(msg, "timeout")) {
                timeout_reached = true;
            }
        }

        if (timeout_reached) {
            RedisModule_ReplyWithError(rctx,
                                      "A multi-shard command failed because at least one shard "
                                      "did not reply within the given timeframe.");
        } else {
            char buf[512] = { 0 };
            snprintf(buf,
                     sizeof(buf),
                     "Multi-shard command failed. %s",
                     MR_ErrorGetMessage(errs[0]));
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

typedef struct {
    RedisModuleBlockedClient *bc;
    bool resp3;
} MRRunOnShardsBlockedCtx;

static void mget_done_onshards(void *privateData,
                              Record **results,
                              size_t nResults,
                              MRError **errs,
                              size_t nErrs) {
    MRRunOnShardsBlockedCtx *pd = privateData;
    RedisModuleBlockedClient *bc = pd->bc;
    RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(bc);

    if (unlikely(check_and_reply_on_remote_errors(errs, nErrs, rctx))) {
        goto __done;
    }

    size_t total_len = 0;
    for (size_t i = 0; i < nResults; i++) {
        Record *raw = results[i];
        if (!raw) {
            continue;
        }
        if (pd->resp3) {
            if (raw->recordType != GetMapRecordType()) {
                RedisModule_Log(rctx,
                                "warning",
                                "Unexpected record type: %s",
                                raw->recordType->type.type);
                continue;
            }
            total_len += MapRecord_GetLen((MapRecord *)raw);
        } else {
            if (raw->recordType != GetListRecordType()) {
                RedisModule_Log(rctx,
                                "warning",
                                "Unexpected record type: %s",
                                raw->recordType->type.type);
                continue;
            }
            total_len += ListRecord_GetLen((ListRecord *)raw);
        }
    }

    if (pd->resp3) {
        RedisModule_ReplyWithMap(rctx, total_len / 2);
        for (size_t i = 0; i < nResults; i++) {
            Record *raw = results[i];
            if (!raw || raw->recordType != GetMapRecordType()) {
                continue;
            }
            size_t map_len = MapRecord_GetLen((MapRecord *)raw);
            for (size_t j = 0; j < map_len; j++) {
                Record *r = MapRecord_GetRecord((MapRecord *)raw, j);
                r->recordType->sendReply(rctx, r);
            }
        }
    } else {
        RedisModule_ReplyWithArray(rctx, total_len);
        for (size_t i = 0; i < nResults; i++) {
            Record *raw = results[i];
            if (!raw || raw->recordType != GetListRecordType()) {
                continue;
            }
            size_t list_len = ListRecord_GetLen((ListRecord *)raw);
            for (size_t j = 0; j < list_len; j++) {
                Record *r = ListRecord_GetRecord((ListRecord *)raw, j);
                r->recordType->sendReply(rctx, r);
            }
        }
    }

__done:
    for (size_t i = 0; i < nResults; ++i) {
        if (results[i]) {
            MR_RecordFree(results[i]);
        }
    }
    for (size_t i = 0; i < nErrs; ++i) {
        if (errs[i]) {
            MR_ErrorFree(errs[i]);
        }
    }
    free(pd);
    RTS_UnblockClient(bc, rctx);
}

static void queryindex_done_onshards(void *privateData,
                                   Record **results,
                                   size_t nResults,
                                   MRError **errs,
                                   size_t nErrs) {
    MRRunOnShardsBlockedCtx *pd = privateData;
    RedisModuleBlockedClient *bc = pd->bc;
    RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(bc);

    if (unlikely(check_and_reply_on_remote_errors(errs, nErrs, rctx))) {
        goto __done;
    }

    size_t total_len = 0;
    for (size_t i = 0; i < nResults; i++) {
        Record *raw = results[i];
        if (!raw) {
            continue;
        }
        if (raw->recordType != GetListRecordType()) {
            RedisModule_Log(rctx,
                            "warning",
                            "Unexpected record type: %s",
                            raw->recordType->type.type);
            continue;
        }
        total_len += ListRecord_GetLen((ListRecord *)raw);
    }

    if (pd->resp3) {
        RedisModule_ReplyWithSet(rctx, total_len);
    } else {
        RedisModule_ReplyWithArray(rctx, total_len);
    }

    for (size_t i = 0; i < nResults; i++) {
        Record *raw = results[i];
        if (!raw || raw->recordType != GetListRecordType()) {
            continue;
        }
        size_t list_len = ListRecord_GetLen((ListRecord *)raw);
        for (size_t j = 0; j < list_len; j++) {
            Record *r = ListRecord_GetRecord((ListRecord *)raw, j);
            r->recordType->sendReply(rctx, r);
        }
    }

__done:
    for (size_t i = 0; i < nResults; ++i) {
        if (results[i]) {
            MR_RecordFree(results[i]);
        }
    }
    for (size_t i = 0; i < nErrs; ++i) {
        if (errs[i]) {
            MR_ErrorFree(errs[i]);
        }
    }
    free(pd);
    RTS_UnblockClient(bc, rctx);
}

static void mrange_done_onshards(void *privateData,
                                Record **results,
                                size_t nResults,
                                MRError **errs,
                                size_t nErrs) {
    MRangeData *data = privateData;
    RedisModuleBlockedClient *bc = data->bc;
    RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(bc);

    if (unlikely(check_and_reply_on_remote_errors(errs, nErrs, rctx))) {
        goto __done;
    }

    TS_ResultSet *resultset = NULL;
    if (data->args.groupByLabel) {
        resultset = ResultSet_Create();
        ResultSet_GroupbyLabel(resultset, data->args.groupByLabel);
    } else {
        size_t total_len = 0;
        for (size_t i = 0; i < nResults; i++) {
            Record *raw_listRecord = results[i];
            if (!raw_listRecord) {
                continue;
            }
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

    Series **tempSeries = array_new(Record *, nResults);
    for (size_t i = 0; i < nResults; i++) {
        Record *raw_listRecord = results[i];
        if (!raw_listRecord || raw_listRecord->recordType != GetListRecordType()) {
            continue;
        }

        size_t list_len = ListRecord_GetLen((ListRecord *)raw_listRecord);
        for (size_t j = 0; j < list_len; j++) {
            Record *raw_record = ListRecord_GetRecord((ListRecord *)raw_listRecord, j);
            if (!raw_record || raw_record->recordType != GetSeriesRecordType()) {
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
    for (size_t i = 0; i < nResults; ++i) {
        if (results[i]) {
            MR_RecordFree(results[i]);
        }
    }
    for (size_t i = 0; i < nErrs; ++i) {
        if (errs[i]) {
            MR_ErrorFree(errs[i]);
        }
    }
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

    RedisModuleBlockedClient *bc = RTS_BlockClient(ctx, rts_free_rctx);
    MRRunOnShardsBlockedCtx *pd = malloc(sizeof(*pd));
    pd->bc = bc;
    pd->resp3 = queryArg->resp3;

    // Note: ownership of `queryArg` is transferred to remote tasks (local+remote).
    MR_RunOnAllShards("TSDB_MGET_REMOTE_TASK",
                      queryArg,
                      GetNullRecord(),
                      mget_done_onshards,
                      pd,
                      RTS_LIBMR_REMOTE_TASK_TIMEOUT_MS);
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
    RedisModuleBlockedClient *bc = RTS_BlockClient(ctx, rts_free_rctx);
    MRangeData *data = malloc(sizeof(struct MRangeData));
    data->bc = bc;
    data->args = args;

    // Note: ownership of `queryArg` is transferred to remote tasks (local+remote).
    MR_RunOnAllShards("TSDB_MRANGE_REMOTE_TASK",
                      queryArg,
                      GetNullRecord(),
                      mrange_done_onshards,
                      data,
                      RTS_LIBMR_REMOTE_TASK_TIMEOUT_MS);
    return REDISMODULE_OK;
}

int TSDB_queryindex_RG(RedisModuleCtx *ctx, QueryPredicateList *queries) {
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

    RedisModuleBlockedClient *bc = RTS_BlockClient(ctx, rts_free_rctx);
    MRRunOnShardsBlockedCtx *pd = malloc(sizeof(*pd));
    pd->bc = bc;
    pd->resp3 = queryArg->resp3;

    // Note: ownership of `queryArg` is transferred to remote tasks (local+remote).
    MR_RunOnAllShards("TSDB_QUERYINDEX_REMOTE_TASK",
                      queryArg,
                      GetNullRecord(),
                      queryindex_done_onshards,
                      pd,
                      RTS_LIBMR_REMOTE_TASK_TIMEOUT_MS);
    return REDISMODULE_OK;
}
