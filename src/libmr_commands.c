
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

typedef struct SlotRangeAccum
{
    SlotRangeRecord *ranges;
    size_t count;
    uint64_t epoch;
    bool epoch_set;
} SlotRangeAccum;

static int cmp_slotrange_by_start(const void *a, const void *b) {
    const SlotRangeRecord *ra = a;
    const SlotRangeRecord *rb = b;
    return (int)ra->start - (int)rb->start;
}

static void SlotRangeAccum_Free(SlotRangeAccum *acc) {
    free(acc->ranges);
    acc->ranges = NULL;
    acc->count = 0;
    acc->epoch = 0;
    acc->epoch_set = false;
}

static bool validate_and_accumulate_envelope(RedisModuleCtx *rctx,
                                             SlotRangeAccum *acc,
                                             const ShardEnvelopeRecord *env) {
    const uint64_t epoch = ShardEnvelopeRecord_GetEpoch(env);
    if (!acc->epoch_set) {
        acc->epoch = epoch;
        acc->epoch_set = true;
    } else if (acc->epoch != epoch) {
        RedisModule_ReplyWithError(rctx,
                                   "Multi-shard command failed due to cluster topology change "
                                   "during execution. Please retry.");
        return false;
    }

    const size_t n = ShardEnvelopeRecord_GetSlotRangesCount(env);
    const SlotRangeRecord *ranges = ShardEnvelopeRecord_GetSlotRanges(env);
    if (n == 0 || ranges == NULL) {
        RedisModule_ReplyWithError(
            rctx,
            "Multi-shard command failed due to missing slot ownership metadata. Please retry.");
        return false;
    }

    acc->ranges = realloc(acc->ranges, sizeof(*acc->ranges) * (acc->count + n));
    memcpy(acc->ranges + acc->count, ranges, sizeof(*ranges) * n);
    acc->count += n;
    return true;
}

static bool validate_slot_coverage_or_reply(RedisModuleCtx *rctx, const SlotRangeAccum *acc) {
    if (!acc->epoch_set || acc->count == 0) {
        RedisModule_ReplyWithError(
            rctx,
            "Multi-shard command failed due to missing slot ownership metadata. Please retry.");
        return false;
    }

    // Validate that shard-reported ownership covers all cluster slots exactly once.
    // Overlap or gaps mean shards replied under inconsistent views.
    SlotRangeRecord *tmp = malloc(sizeof(*tmp) * acc->count);
    memcpy(tmp, acc->ranges, sizeof(*tmp) * acc->count);
    qsort(tmp, acc->count, sizeof(*tmp), cmp_slotrange_by_start);

    int expected = 0;
    for (size_t i = 0; i < acc->count; i++) {
        const int start = (int)tmp[i].start;
        const int end = (int)tmp[i].end;
        if (start != expected) {
            free(tmp);
            RedisModule_ReplyWithError(rctx,
                                       "Multi-shard command failed due to cluster topology change "
                                       "during execution. Please retry.");
            return false;
        }
        if (end < start) {
            free(tmp);
            RedisModule_ReplyWithError(
                rctx,
                "Multi-shard command failed due to invalid slot ownership metadata. Please retry.");
            return false;
        }
        expected = end + 1;
    }
    free(tmp);
    if (expected != (1 << 14)) {
        RedisModule_ReplyWithError(rctx,
                                   "Multi-shard command failed due to cluster topology change "
                                   "during execution. Please retry.");
        return false;
    }

    return true;
}

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
    SlotRangeAccum acc = (SlotRangeAccum){ 0 };

    if (unlikely(check_and_reply_on_error(eCtx, rctx))) {
        goto __done;
    }

    size_t len = MR_ExecutionCtxGetResultsLen(eCtx);
    size_t total_len = 0;
    for (int i = 0; i < len; i++) {
        Record *raw_env = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_env->recordType != GetShardEnvelopeRecordType()) {
            RedisModule_Log(
                rctx, "warning", "Unexpected record type: %s", raw_env->recordType->type.type);
            continue;
        }
        ShardEnvelopeRecord *env = (ShardEnvelopeRecord *)raw_env;
        if (!validate_and_accumulate_envelope(rctx, &acc, env)) {
            SlotRangeAccum_Free(&acc);
            goto __done;
        }
        Record *payload = ShardEnvelopeRecord_GetPayload(env);
        if (payload->recordType != GetListRecordType()) {
            RedisModule_Log(rctx,
                            "warning",
                            "Unexpected payload record type: %s",
                            payload->recordType->type.type);
            continue;
        }
        total_len += ListRecord_GetLen((ListRecord *)payload);
    }
    if (!validate_slot_coverage_or_reply(rctx, &acc)) {
        SlotRangeAccum_Free(&acc);
        goto __done;
    }
    RedisModule_ReplyWithSet(rctx, total_len);

    for (int i = 0; i < len; i++) {
        Record *raw_env = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_env->recordType != GetShardEnvelopeRecordType()) {
            RedisModule_Log(
                rctx, "warning", "Unexpected record type: %s", raw_env->recordType->type.type);
            continue;
        }

        Record *payload = ShardEnvelopeRecord_GetPayload((ShardEnvelopeRecord *)raw_env);
        if (payload->recordType != GetListRecordType()) {
            continue;
        }
        size_t list_len = ListRecord_GetLen((ListRecord *)payload);
        for (size_t j = 0; j < list_len; j++) {
            Record *r = ListRecord_GetRecord((ListRecord *)payload, j);
            r->recordType->sendReply(rctx, r);
        }
    }

__done:
    SlotRangeAccum_Free(&acc);
    RTS_UnblockClient(bc, rctx);
}

static void mget_done_resp3(ExecutionCtx *eCtx, void *privateData) {
    RedisModuleBlockedClient *bc = privateData;
    RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(bc);
    SlotRangeAccum acc = (SlotRangeAccum){ 0 };

    if (unlikely(check_and_reply_on_error(eCtx, rctx))) {
        goto __done;
    }

    size_t len = MR_ExecutionCtxGetResultsLen(eCtx);
    size_t total_len = 0;
    for (int i = 0; i < len; i++) {
        Record *raw_env = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_env->recordType != GetShardEnvelopeRecordType()) {
            RedisModule_Log(
                rctx, "warning", "Unexpected record type: %s", raw_env->recordType->type.type);
            continue;
        }
        ShardEnvelopeRecord *env = (ShardEnvelopeRecord *)raw_env;
        if (!validate_and_accumulate_envelope(rctx, &acc, env)) {
            SlotRangeAccum_Free(&acc);
            goto __done;
        }
        Record *payload = ShardEnvelopeRecord_GetPayload(env);
        if (payload->recordType != GetMapRecordType()) {
            RedisModule_Log(rctx,
                            "warning",
                            "Unexpected payload record type: %s",
                            payload->recordType->type.type);
            continue;
        }
        total_len += MapRecord_GetLen((MapRecord *)payload);
    }

    if (!validate_slot_coverage_or_reply(rctx, &acc)) {
        SlotRangeAccum_Free(&acc);
        goto __done;
    }

    RedisModule_ReplyWithMap(rctx, total_len / 2);

    for (int i = 0; i < len; i++) {
        Record *raw_env = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_env->recordType != GetShardEnvelopeRecordType()) {
            continue;
        }
        Record *payload = ShardEnvelopeRecord_GetPayload((ShardEnvelopeRecord *)raw_env);
        if (payload->recordType != GetMapRecordType()) {
            continue;
        }
        size_t map_len = MapRecord_GetLen((MapRecord *)payload);
        for (size_t j = 0; j < map_len; j++) {
            Record *r = MapRecord_GetRecord((MapRecord *)payload, j);
            r->recordType->sendReply(rctx, r);
        }
    }

__done:
    SlotRangeAccum_Free(&acc);
    RTS_UnblockClient(bc, rctx);
}

static void mget_done(ExecutionCtx *eCtx, void *privateData) {
    RedisModuleBlockedClient *bc = privateData;
    RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(bc);
    SlotRangeAccum acc = (SlotRangeAccum){ 0 };

    if (unlikely(check_and_reply_on_error(eCtx, rctx))) {
        goto __done;
    }

    size_t len = MR_ExecutionCtxGetResultsLen(eCtx);
    size_t total_len = 0;
    for (int i = 0; i < len; i++) {
        Record *raw_env = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_env->recordType != GetShardEnvelopeRecordType()) {
            RedisModule_Log(
                rctx, "warning", "Unexpected record type: %s", raw_env->recordType->type.type);
            continue;
        }
        ShardEnvelopeRecord *env = (ShardEnvelopeRecord *)raw_env;
        if (!validate_and_accumulate_envelope(rctx, &acc, env)) {
            SlotRangeAccum_Free(&acc);
            goto __done;
        }
        Record *payload = ShardEnvelopeRecord_GetPayload(env);
        if (payload->recordType != GetListRecordType()) {
            RedisModule_Log(rctx,
                            "warning",
                            "Unexpected payload record type: %s",
                            payload->recordType->type.type);
            continue;
        }
        total_len += ListRecord_GetLen((ListRecord *)payload);
    }
    if (!validate_slot_coverage_or_reply(rctx, &acc)) {
        SlotRangeAccum_Free(&acc);
        goto __done;
    }
    RedisModule_ReplyWithArray(rctx, total_len);

    for (int i = 0; i < len; i++) {
        Record *raw_env = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_env->recordType != GetShardEnvelopeRecordType()) {
            RedisModule_Log(
                rctx, "warning", "Unexpected record type: %s", raw_env->recordType->type.type);
            continue;
        }

        Record *payload = ShardEnvelopeRecord_GetPayload((ShardEnvelopeRecord *)raw_env);
        if (payload->recordType != GetListRecordType()) {
            continue;
        }
        size_t list_len = ListRecord_GetLen((ListRecord *)payload);
        for (size_t j = 0; j < list_len; j++) {
            Record *r = ListRecord_GetRecord((ListRecord *)payload, j);
            r->recordType->sendReply(rctx, r);
        }
    }

__done:
    SlotRangeAccum_Free(&acc);
    RTS_UnblockClient(bc, rctx);
}

static void queryindex_resp3_done(ExecutionCtx *eCtx, void *privateData) {
    RedisModuleBlockedClient *bc = privateData;
    RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(bc);
    SlotRangeAccum acc = (SlotRangeAccum){ 0 };

    if (unlikely(check_and_reply_on_error(eCtx, rctx))) {
        goto __done;
    }

    size_t len = MR_ExecutionCtxGetResultsLen(eCtx);
    size_t total_len = 0;
    for (int i = 0; i < len; i++) {
        Record *raw_env = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_env->recordType != GetShardEnvelopeRecordType()) {
            RedisModule_Log(
                rctx, "warning", "Unexpected record type: %s", raw_env->recordType->type.type);
            continue;
        }
        ShardEnvelopeRecord *env = (ShardEnvelopeRecord *)raw_env;
        if (!validate_and_accumulate_envelope(rctx, &acc, env)) {
            SlotRangeAccum_Free(&acc);
            goto __done;
        }
        Record *payload = ShardEnvelopeRecord_GetPayload(env);
        if (payload->recordType != GetListRecordType()) {
            continue;
        }
        total_len += ListRecord_GetLen((ListRecord *)payload);
    }
    if (!validate_slot_coverage_or_reply(rctx, &acc)) {
        SlotRangeAccum_Free(&acc);
        goto __done;
    }
    RedisModule_ReplyWithSet(rctx, total_len);

    for (int i = 0; i < len; i++) {
        Record *raw_env = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_env->recordType != GetShardEnvelopeRecordType()) {
            RedisModule_Log(
                rctx, "warning", "Unexpected record type: %s", raw_env->recordType->type.type);
            continue;
        }

        Record *payload = ShardEnvelopeRecord_GetPayload((ShardEnvelopeRecord *)raw_env);
        if (payload->recordType != GetListRecordType()) {
            continue;
        }
        size_t list_len = ListRecord_GetLen((ListRecord *)payload);
        for (size_t j = 0; j < list_len; j++) {
            Record *r = ListRecord_GetRecord((ListRecord *)payload, j);
            r->recordType->sendReply(rctx, r);
        }
    }

__done:
    SlotRangeAccum_Free(&acc);
    RTS_UnblockClient(bc, rctx);
}

static void mrange_done(ExecutionCtx *eCtx, void *privateData) {
    MRangeData *data = privateData;
    RedisModuleBlockedClient *bc = data->bc;
    RedisModuleCtx *rctx = RedisModule_GetThreadSafeContext(bc);
    SlotRangeAccum acc = (SlotRangeAccum){ 0 };

    if (unlikely(check_and_reply_on_error(eCtx, rctx))) {
        goto __done;
    }

    long long len = MR_ExecutionCtxGetResultsLen(eCtx);

    TS_ResultSet *resultset = NULL;

    // First pass: validate metadata (epoch + slot coverage) and compute total length if needed.
    size_t total_len = 0;
    for (int i = 0; i < len; i++) {
        Record *raw_env = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_env->recordType != GetShardEnvelopeRecordType()) {
            RedisModule_Log(
                rctx, "warning", "Unexpected record type: %s", raw_env->recordType->type.type);
            continue;
        }
        ShardEnvelopeRecord *env = (ShardEnvelopeRecord *)raw_env;
        if (!validate_and_accumulate_envelope(rctx, &acc, env)) {
            SlotRangeAccum_Free(&acc);
            goto __done;
        }
        Record *payload = ShardEnvelopeRecord_GetPayload(env);
        if (payload->recordType != GetListRecordType()) {
            RedisModule_Log(rctx,
                            "warning",
                            "Unexpected payload record type: %s",
                            payload->recordType->type.type);
            continue;
        }
        if (!data->args.groupByLabel) {
            total_len += ListRecord_GetLen((ListRecord *)payload);
        }
    }
    if (!validate_slot_coverage_or_reply(rctx, &acc)) {
        SlotRangeAccum_Free(&acc);
        goto __done;
    }

    if (data->args.groupByLabel) {
        resultset = ResultSet_Create();
        ResultSet_GroupbyLabel(resultset, data->args.groupByLabel);
    } else {
        RedisModule_ReplyWithMapOrArray(rctx, total_len, false);
    }

    Series **tempSeries = array_new(Record *, len); // calloc(len, sizeof(Series *));
    for (int i = 0; i < len; i++) {
        Record *raw_env = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_env->recordType != GetShardEnvelopeRecordType()) {
            RedisModule_Log(
                rctx, "warning", "Unexpected record type: %s", raw_env->recordType->type.type);
            continue;
        }
        Record *raw_listRecord = ShardEnvelopeRecord_GetPayload((ShardEnvelopeRecord *)raw_env);
        if (raw_listRecord->recordType != GetListRecordType()) {
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
    SlotRangeAccum_Free(&acc);
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
