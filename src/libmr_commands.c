#include "libmr_commands.h"

#include "LibMR/src/utils/arr.h"
#include "LibMR/src/mr.h"
#include "LibMR/src/cluster.h"
#include "consts.h"
#include "libmr_integration.h"
#include "module.h"
#include "query_language.h"
#include "reply.h"
#include "resultset.h"
#include "streaming_resultset.h"
#include "utils/blocked_client.h"

#include "rmutil/alloc.h"

typedef struct SlotRangeAccum
{
    SlotRangeRecord *ranges;
    size_t count;
} SlotRangeAccum;

#define RTS_ERR_QUERY_REQUIRES_UNAVAILABLE_SLOTS "Query requires unavailable slots"

static inline bool should_validate_slot_ranges(RedisModuleCtx *rctx) {
    // Redis core note: RM_ClusterGetLocalSlotRanges() can be incorrect when cluster-enabled=no.
    // Only validate slot coverage when Redis runs in OSS Cluster mode (cluster-enabled=yes).
    const int flags = RedisModule_GetContextFlags(rctx);
    return (flags & REDISMODULE_CTX_FLAGS_CLUSTER) != 0;
}

static int cmp_slotrange_by_start(const void *a, const void *b) {
    const SlotRangeRecord *ra = a;
    const SlotRangeRecord *rb = b;
    return (int)ra->start - (int)rb->start;
}

static void SlotRangeAccum_Free(SlotRangeAccum *acc) {
    free(acc->ranges);
    acc->ranges = NULL;
    acc->count = 0;
}

static bool validate_and_accumulate_shard_slots(RedisModuleCtx *rctx,
                                                SlotRangeAccum *acc,
                                                const ShardEnvelopeRecord *shardResult) {
    if (!should_validate_slot_ranges(rctx)) {
        return true;
    }
    size_t n = 0;
    const SlotRangeRecord *ranges = ShardEnvelopeRecord_SlotRanges(shardResult, &n);
    if (n == 0 || ranges == NULL) {
        RedisModule_ReplyWithError(rctx, RTS_ERR_QUERY_REQUIRES_UNAVAILABLE_SLOTS);
        return false;
    }

    acc->ranges = realloc(acc->ranges, sizeof(*acc->ranges) * (acc->count + n));
    memcpy(acc->ranges + acc->count, ranges, sizeof(*ranges) * n);
    acc->count += n;
    return true;
}

static bool validate_slot_coverage_or_reply(RedisModuleCtx *rctx, SlotRangeAccum *acc) {
    if (!should_validate_slot_ranges(rctx)) {
        return true;
    }
    if (acc->count == 0) {
        RedisModule_ReplyWithError(rctx, RTS_ERR_QUERY_REQUIRES_UNAVAILABLE_SLOTS);
        return false;
    }

    // Validate that shard-reported ownership covers all cluster slots exactly once.
    // Overlap or gaps mean shards replied under inconsistent views.
    qsort(acc->ranges, acc->count, sizeof(*acc->ranges), cmp_slotrange_by_start);

    int expected = 0;
    for (size_t i = 0; i < acc->count; i++) {
        const int start = (int)acc->ranges[i].start;
        const int end = (int)acc->ranges[i].end;
        if (start != expected) {
            RedisModule_ReplyWithError(rctx, RTS_ERR_QUERY_REQUIRES_UNAVAILABLE_SLOTS);
            return false;
        }
        if (end < start) {
            RedisModule_ReplyWithError(rctx, RTS_ERR_QUERY_REQUIRES_UNAVAILABLE_SLOTS);
            return false;
        }
        expected = end + 1;
    }
    if (expected != (1 << 14)) {
        RedisModule_ReplyWithError(rctx, RTS_ERR_QUERY_REQUIRES_UNAVAILABLE_SLOTS);
        return false;
    }

    return true;
}

// RedisModule_GetCurrentUserName allocates a copy but registers it on the context's auto-memory,
// so it gets freed when the context ends. We re-copy with NULL ctx to detach from auto-memory,
// since the string must survive serialization to other shards via LibMR.
static RedisModuleString *CopyCurrentUserName(RedisModuleCtx *ctx) {
    const RedisModuleString *userName = RedisModule_GetCurrentUserName(ctx);
    if (!userName)
        return NULL;

    return RedisModule_CreateStringFromString(NULL, userName);
}

static inline bool check_and_reply_on_error(ExecutionCtx *eCtx, RedisModuleCtx *rctx) {
    size_t len = MR_ExecutionCtxGetErrorsLen(eCtx);
    if (likely(len == 0))
        return false;

    RedisModule_Log(rctx, "warning", "got libmr error:");
    bool max_idle_reached = false, cluster_topology_changed = false;
    for (size_t i = 0; i < len; ++i) {
        const char *execution_error = MR_ExecutionCtxGetError(eCtx, i);
        RedisModule_Log(rctx, "warning", "%s", execution_error);
        if (strcmp("execution max idle reached", execution_error) == 0)
            max_idle_reached = true;
        if (strcmp("cluster topology changed", execution_error) == 0)
            cluster_topology_changed = true;
    }

    if (max_idle_reached) {
        RedisModule_ReplyWithError(rctx,
                                   "A multi-keys command failed because at least one shard "
                                   "did not reply within the given timeframe.");
    } else if (cluster_topology_changed) {
        RedisModule_ReplyWithError(
            rctx, "A multi-shard command failed because the cluster topology has changed");
    } else {
        char buf[512] = { 0 };
        const char *err_msg = MR_ExecutionCtxGetError(eCtx, 0);
        if (strncmp(err_msg, "NOPERM ", 7) == 0) {
            snprintf(buf, sizeof(buf), "NOPERM Multi-keys command failed. %s", err_msg + 7);
        } else {
            snprintf(buf, sizeof(buf), "Multi-keys command failed. %s", err_msg);
        }
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
            nodesResults = array_append(nodesResults, r); // keep full record for numAggClasses
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

// Build coordinator RangeArgs from the original args: open the time window so the coordinator
// accepts all pre-aggregated buckets from shards, skip re-aggregation, and clear FILTERBY
// (shards already applied it per-series before reducing).
static RangeArgs RangeArgsSkipReAggregation(const RangeArgs *src) {
    RangeArgs a = *src;
    a.skipAggregation = true;
    a.filterByValueArgs.hasValue = false;
    a.filterByTSArgs.hasValue = false;
    a.startTimestamp = 0;
    a.endTimestamp = UINT64_MAX;
    return a;
}

static void mrange_done_internal(ExecutionCtx *eCtx, RedisModuleCtx *ctx, MRangeData *data) {
    MRangeArgs *args = &data->args;
    RedisModuleBlockedClient *bc = data->bc;

    ARR(SeriesListRecord *) nodesResults = collect_node_results(eCtx, ctx);
    if (!nodesResults)
        goto __done;

    // Shards always apply FILTERBY (aggregation or not); the coordinator must not re-apply it.
    RangeArgs coordArgs = RangeArgsSkipReAggregation(&args->rangeArgs);
    const RangeArgs *replyArgs = &coordArgs;

    // MERGE (public->private, MOD-15896 Flex): public/master fed the buffered TS_ResultSet here
    // (ResultSet_Create/ResultSet_AddSeries/ResultSet_ApplyReducer/replyResultSet). That buffered
    // grouped reducer was deleted from this fork (see resultset.c breadcrumb) in favor of the
    // streaming reducer, which also fixes a wide-GROUPBY OOM. Use it here too, same as
    // mrange_done_gears above.
    TS_StreamingResultSet *streaming_rs = NULL;
    if (args->groupByLabel) {
        RangeArgs rargs = coordArgs;
        rargs.latest = false; // we already handled the latest flag in the client side
        streaming_rs =
            StreamingResultSet_Create(args->groupByLabel, &args->groupByReducerArgs, &rargs);
    } else {
        size_t totalLen = 0;
        array_foreach(nodesResults, record, {
            size_t N = (record->numAggClasses > 1) ? record->numAggClasses : 1;
            totalLen += array_len(record->seriesList) / N;
        });
        ReplyWithMapOrArray(ctx, totalLen, false);
    }

    array_foreach(nodesResults, record, {
        ARR(Series *) sl = record->seriesList;
        size_t numAggTypes = (record->numAggClasses > 1) ? record->numAggClasses : 1;
        size_t numKeys = array_len(sl) / numAggTypes;
        for (size_t k = 0; k < numKeys; k++) {
            Series **group = &sl[k * numAggTypes];
            if (args->groupByLabel) {
                StreamingResultSet_FeedSeries(streaming_rs, group[0]);
            } else if (numAggTypes > 1) {
                ReplyMultiAggSeriesGroup(ctx,
                                         group,
                                         numAggTypes,
                                         args->withLabels,
                                         args->limitLabels,
                                         args->numLimitLabels,
                                         replyArgs,
                                         args->reverse);
            } else {
                ReplySeriesArrayPos(ctx,
                                    group[0],
                                    args->withLabels,
                                    args->limitLabels,
                                    args->numLimitLabels,
                                    replyArgs,
                                    args->reverse,
                                    false,
                                    NULL,
                                    NULL);
            }
        }
    });

    if (args->groupByLabel) {
        StreamingResultSet_FinalizeAndReply(ctx,
                                            streaming_rs,
                                            args->withLabels,
                                            args->limitLabels,
                                            args->numLimitLabels,
                                            args->reverse);
        // FinalizeAndReply freed streaming_rs.
    }

__done:
    if (nodesResults)
        array_free(nodesResults);
    MRangeArgs_Free(&data->args);
    free(data);
}

static void mrange_done_gears(ExecutionCtx *eCtx, RedisModuleCtx *ctx, MRangeData *data) {
    RedisModuleBlockedClient *bc = data->bc;
    RedisModuleCtx *rctx = ctx; // mrange_done (dispatcher) already created/owns this ctx.
    SlotRangeAccum acc = (SlotRangeAccum){ 0 };

    if (unlikely(check_and_reply_on_error(eCtx, ctx))) {
        goto __done;
    }

    long long len = MR_ExecutionCtxGetResultsLen(eCtx);

    TS_StreamingResultSet *streaming_rs = NULL;

    // First pass: validate slot ownership metadata across shard replies. (Reply length for
    // the non-groupby case is no longer precomputed here — EXCLUDEEMPTY may skip series, so
    // the real count is only known after emission; see the postponed-length reply below.)
    for (int i = 0; i < len; i++) {
        Record *raw_env = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_env->recordType != GetShardEnvelopeRecordType()) {
            RedisModule_Log(
                rctx, "warning", "Unexpected record type: %s", raw_env->recordType->type.type);
            continue;
        }
        ShardEnvelopeRecord *env = (ShardEnvelopeRecord *)raw_env;
        if (!validate_and_accumulate_shard_slots(rctx, &acc, env)) {
            SlotRangeAccum_Free(&acc);
            goto __done;
        }
    }
    if (!validate_slot_coverage_or_reply(rctx, &acc)) {
        SlotRangeAccum_Free(&acc);
        goto __done;
    }

    // Grouped queries stream each shard's Series into a per-group accumulator
    // and free it immediately, so coordinator-side heap stays bounded for wide
    // cluster queries. Only streamable reducers can reach here —
    // parseMultiSeriesReduceArgs (query_language.c) rejects TWA/FIRST/LAST up
    // front — so no non-streaming fallback is needed.
    if (data->args.groupByLabel) {
        RangeArgs rargs = data->args.rangeArgs;
        rargs.latest = false; // already handled on client side
        streaming_rs = StreamingResultSet_Create(
            data->args.groupByLabel, &data->args.groupByReducerArgs, &rargs);
    } else {
        // ponytail: postponed length — EXCLUDEEMPTY may skip series, so the real
        // count is only known after emission (see ReplySetMapOrArrayLength below).
        ReplyWithMapOrArray(rctx, REDISMODULE_POSTPONED_ARRAY_LEN, false);
    }

    long long replylen = 0;

    // tempSeries keeps ungrouped Series alive across the reply iteration.
    // Grouped (streaming) frees each Series immediately after feeding.
    Series **tempSeries = NULL;
    if (!data->args.groupByLabel)
        tempSeries = array_new(Record *, len);

    for (int i = 0; i < len; i++) {
        Record *raw_env = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_env->recordType != GetShardEnvelopeRecordType()) {
            RedisModule_Log(
                rctx, "warning", "Unexpected record type: %s", raw_env->recordType->type.type);
            continue;
        }
        Record *raw_listRecord = ShardEnvelopeRecord_GetPayload((ShardEnvelopeRecord *)raw_env);
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

            EnrichedChunk *first_chunk = NULL;
            AbstractIterator *probe = NULL;
            if (data->args.excludeEmpty) {
                probe = SeriesQueryIfNonEmpty(
                    s, &data->args.rangeArgs, data->args.reverse, &first_chunk);
                if (!probe)
                    continue;
            }

            if (data->args.groupByLabel) {
                StreamingResultSet_FeedSeries(streaming_rs, s);
                FreeSeries(s); // Done with this series; release coordinator heap.
            } else {
                tempSeries = array_append(tempSeries, s);
                ReplySeriesArrayPos(rctx,
                                    s,
                                    data->args.withLabels,
                                    data->args.limitLabels,
                                    data->args.numLimitLabels,
                                    &data->args.rangeArgs,
                                    data->args.reverse,
                                    false,
                                    probe,
                                    first_chunk);
                replylen++;
            }
        }
    }

    if (!data->args.groupByLabel) {
        ReplySetMapOrArrayLength(ctx, replylen, false);
    }

    if (data->args.groupByLabel) {
        StreamingResultSet_FinalizeAndReply(rctx,
                                            streaming_rs,
                                            data->args.withLabels,
                                            data->args.limitLabels,
                                            data->args.numLimitLabels,
                                            data->args.reverse);
        // FinalizeAndReply freed streaming_rs.
    }
    if (tempSeries) {
        array_foreach(tempSeries, x, FreeSeries(x));
        array_free(tempSeries);
    }

__done:
    MRangeArgs_Free(&data->args);
    free(data);
    SlotRangeAccum_Free(&acc);
    // NOTE: no RTS_UnblockClient here — mrange_done (the gears/internal dispatcher) unblocks
    // the client once, after this function returns, using the ctx it created.
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
    ARR(SeriesListRecord *) nodesResults = collect_node_results(eCtx, ctx);
    if (!nodesResults)
        goto __done;

    ReplyWithMapOrArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN, false);
    size_t len = 0;
    array_foreach(nodesResults, record, {
        array_foreach(record->seriesList, s, {
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
    if (nodesResults)
        array_free(nodesResults);
}

// MERGE (public->private, MOD-15896 Flex): the public/master "gears" done-handler for MGET
// unwraps plain ListRecord results directly. That doesn't match this fork's shard-side mapper
// (ShardMgetMapper in libmr_integration.c), which always wraps its payload in a
// ShardEnvelopeRecord carrying slot ownership metadata, and — mirroring mget_done_resp3/mget_done
// from before this merge — builds a MapRecord instead of a ListRecord when the querying client is
// RESP3. Restored that ShardEnvelopeRecord/SlotRangeAccum unwrap + slot-coverage validation here
// (previously two separate functions), dispatched on protocol/reply-shape exactly like the
// original queryArg->resp3 flag (recomputed via _ReplyMap(ctx), same expression used to set that
// flag in TSDB_mget_MR).
static void mget_done_gears_resp3(ExecutionCtx *eCtx,
                                  RedisModuleCtx *ctx,
                                  RedisModuleBlockedClient *bc) {
    SlotRangeAccum acc = (SlotRangeAccum){ 0 };

    if (unlikely(check_and_reply_on_error(eCtx, ctx))) {
        goto __done;
    }

    size_t len = MR_ExecutionCtxGetResultsLen(eCtx);
    size_t total_len = 0;
    for (int i = 0; i < len; i++) {
        Record *raw_env = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_env->recordType != GetShardEnvelopeRecordType()) {
            RedisModule_Log(
                ctx, "warning", "Unexpected record type: %s", raw_env->recordType->type.type);
            continue;
        }
        ShardEnvelopeRecord *env = (ShardEnvelopeRecord *)raw_env;
        if (!validate_and_accumulate_shard_slots(ctx, &acc, env)) {
            SlotRangeAccum_Free(&acc);
            goto __done;
        }
        Record *payload = ShardEnvelopeRecord_GetPayload(env);
        if (payload->recordType != GetMapRecordType()) {
            RedisModule_Log(ctx,
                            "warning",
                            "Unexpected payload record type: %s",
                            payload->recordType->type.type);
            continue;
        }
        total_len += MapRecord_GetLen((MapRecord *)payload);
    }

    if (!validate_slot_coverage_or_reply(ctx, &acc)) {
        SlotRangeAccum_Free(&acc);
        goto __done;
    }

    RedisModule_ReplyWithMap(ctx, total_len / 2);

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
            r->recordType->sendReply(ctx, r);
        }
    }

__done:
    SlotRangeAccum_Free(&acc);
}

static void mget_done_gears(ExecutionCtx *eCtx, RedisModuleCtx *ctx, RedisModuleBlockedClient *bc) {
    if (_ReplyMap(ctx)) {
        mget_done_gears_resp3(eCtx, ctx, bc);
        return;
    }

    SlotRangeAccum acc = (SlotRangeAccum){ 0 };

    if (unlikely(check_and_reply_on_error(eCtx, ctx))) {
        goto __done;
    }

    size_t len = MR_ExecutionCtxGetResultsLen(eCtx);
    size_t total_len = 0;
    for (int i = 0; i < len; i++) {
        Record *raw_env = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_env->recordType != GetShardEnvelopeRecordType()) {
            RedisModule_Log(
                ctx, "warning", "Unexpected record type: %s", raw_env->recordType->type.type);
            continue;
        }
        ShardEnvelopeRecord *env = (ShardEnvelopeRecord *)raw_env;
        if (!validate_and_accumulate_shard_slots(ctx, &acc, env)) {
            SlotRangeAccum_Free(&acc);
            goto __done;
        }
        Record *payload = ShardEnvelopeRecord_GetPayload(env);
        if (payload->recordType != GetListRecordType()) {
            RedisModule_Log(ctx,
                            "warning",
                            "Unexpected payload record type: %s",
                            payload->recordType->type.type);
            continue;
        }
        total_len += ListRecord_GetLen((ListRecord *)payload);
    }
    if (!validate_slot_coverage_or_reply(ctx, &acc)) {
        SlotRangeAccum_Free(&acc);
        goto __done;
    }
    RedisModule_ReplyWithArray(ctx, total_len);

    for (int i = 0; i < len; i++) {
        Record *raw_env = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_env->recordType != GetShardEnvelopeRecordType()) {
            RedisModule_Log(
                ctx, "warning", "Unexpected record type: %s", raw_env->recordType->type.type);
            continue;
        }

        Record *payload = ShardEnvelopeRecord_GetPayload((ShardEnvelopeRecord *)raw_env);
        if (payload->recordType != GetListRecordType()) {
            continue;
        }
        size_t list_len = ListRecord_GetLen((ListRecord *)payload);
        for (size_t j = 0; j < list_len; j++) {
            Record *r = ListRecord_GetRecord((ListRecord *)payload, j);
            r->recordType->sendReply(ctx, r);
        }
    }

__done:
    SlotRangeAccum_Free(&acc);
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
    if (nodesResults)
        array_free(nodesResults);
}

// MERGE (public->private, MOD-15896 Flex): same ShardEnvelopeRecord/SlotRangeAccum restoration
// as mget_done_gears above — ShardQueryindexMapper (libmr_integration.c) always wraps its
// ListRecord payload in a ShardEnvelopeRecord, and (unlike mget) never varies the payload shape
// on resp3, so this just needs the envelope unwrap + slot-coverage validation, no dual dispatch.
static void queryindex_done_gears(ExecutionCtx *eCtx,
                                  RedisModuleCtx *ctx,
                                  RedisModuleBlockedClient *bc) {
    SlotRangeAccum acc = (SlotRangeAccum){ 0 };

    if (unlikely(check_and_reply_on_error(eCtx, ctx))) {
        goto __done;
    }

    size_t len = MR_ExecutionCtxGetResultsLen(eCtx);
    size_t total_len = 0;
    for (int i = 0; i < len; i++) {
        Record *raw_env = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_env->recordType != GetShardEnvelopeRecordType()) {
            RedisModule_Log(
                ctx, "warning", "Unexpected record type: %s", raw_env->recordType->type.type);
            continue;
        }
        ShardEnvelopeRecord *env = (ShardEnvelopeRecord *)raw_env;
        if (!validate_and_accumulate_shard_slots(ctx, &acc, env)) {
            SlotRangeAccum_Free(&acc);
            goto __done;
        }
        Record *payload = ShardEnvelopeRecord_GetPayload(env);
        if (payload->recordType != GetListRecordType()) {
            continue;
        }
        total_len += ListRecord_GetLen((ListRecord *)payload);
    }
    if (!validate_slot_coverage_or_reply(ctx, &acc)) {
        SlotRangeAccum_Free(&acc);
        goto __done;
    }
    RedisModule_ReplyWithSet(ctx, total_len);

    for (int i = 0; i < len; i++) {
        Record *raw_env = MR_ExecutionCtxGetResult(eCtx, i);
        if (raw_env->recordType != GetShardEnvelopeRecordType()) {
            RedisModule_Log(
                ctx, "warning", "Unexpected record type: %s", raw_env->recordType->type.type);
            continue;
        }

        Record *payload = ShardEnvelopeRecord_GetPayload((ShardEnvelopeRecord *)raw_env);
        if (payload->recordType != GetListRecordType()) {
            continue;
        }
        size_t list_len = ListRecord_GetLen((ListRecord *)payload);
        for (size_t j = 0; j < list_len; j++) {
            Record *r = ListRecord_GetRecord((ListRecord *)payload, j);
            r->recordType->sendReply(ctx, r);
        }
    }

__done:
    SlotRangeAccum_Free(&acc);
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

    QueryPredicates_Arg *queryArg = calloc(1, sizeof *queryArg);
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
    queryArg->userName = CopyCurrentUserName(ctx);
    queryArg->numAggClasses = 0;

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

    QueryPredicates_Arg *queryArg = calloc(1, sizeof *queryArg);
    queryArg->shouldReturnNull = false;
    queryArg->refCount = 1;
    queryArg->count = args.queryPredicates->count;
    queryArg->startTimestamp = args.rangeArgs.startTimestamp;
    queryArg->endTimestamp = args.rangeArgs.endTimestamp;
    queryArg->latest = args.rangeArgs.latest;
    // Atomic even though this call site is main-thread-only: LibMR's own Duplicate/ObjectFree
    // step-arg callbacks (QueryPredicates_Duplicate/QueryPredicates_ObjectFree) touch this same
    // ref from their own execution threads over the object's life, so every mutation site has
    // to stay atomic for the count to be consistent (see QueryPredicateList_Free in indexer.c).
    __atomic_add_fetch(&args.queryPredicates->ref, 1, __ATOMIC_RELAXED);
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

    queryArg->userName = CopyCurrentUserName(ctx);
    queryArg->excludeEmpty = args.excludeEmpty;
    // Always send FILTERBY to shards; they apply it regardless of aggregation.
    queryArg->filterByValueArgs = args.rangeArgs.filterByValueArgs;
    queryArg->filterByTSArgs = args.rangeArgs.filterByTSArgs;
    // Push aggregation to every shard for all cases (single-agg and multi-agg).
    // Multi-agg + GROUPBY is rejected at parse time, so no special case is needed.
    if (args.rangeArgs.aggregationArgs.numClasses > 0) {
        queryArg->numAggClasses = args.rangeArgs.aggregationArgs.numClasses;
        for (size_t i = 0; i < args.rangeArgs.aggregationArgs.numClasses; i++)
            queryArg->aggTypes[i] = args.rangeArgs.aggregationArgs.classes[i]->type;
        queryArg->aggTimeDelta = args.rangeArgs.aggregationArgs.timeDelta;
        queryArg->aggBucketTS = args.rangeArgs.aggregationArgs.bucketTS;
        queryArg->aggEmpty = args.rangeArgs.aggregationArgs.empty;
        queryArg->alignment = args.rangeArgs.alignment;
        queryArg->timestampAlignment = args.rangeArgs.timestampAlignment;
    } else {
        queryArg->numAggClasses = 0;
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
    QueryPredicates_Arg *queryArg = calloc(1, sizeof(QueryPredicates_Arg));
    queryArg->shouldReturnNull = false;
    queryArg->refCount = 1;
    queryArg->count = queries->count;
    queryArg->startTimestamp = 0;
    queryArg->endTimestamp = 0;
    __atomic_add_fetch(&queries->ref, 1, __ATOMIC_RELAXED); // see rationale in TSDB_mrange_MR above
    queryArg->predicates = queries;
    queryArg->withLabels = false;
    queryArg->limitLabelsSize = 0;
    queryArg->limitLabels = NULL;
    queryArg->resp3 = _ReplySet(ctx);
    queryArg->userName = CopyCurrentUserName(ctx);
    queryArg->numAggClasses = 0;

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

static void querylabels_done_gears(ExecutionCtx *eCtx, RedisModuleCtx *ctx) {
    if (unlikely(check_and_reply_on_error(eCtx, ctx))) {
        return;
    }

    RedisModuleDict *agg = RedisModule_CreateDict(NULL);
    size_t len = MR_ExecutionCtxGetResultsLen(eCtx);
    for (size_t i = 0; i < len; i++) {
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
            StringRecord *sr =
                (StringRecord *)ListRecord_GetRecord((ListRecord *)raw_listRecord, j);
            RedisModule_DictSetC(agg, sr->str, sr->len, NULL);
        }
    }

    ReplyWithKeySetFromDict(ctx, agg);
    RedisModule_FreeDict(NULL, agg);
}

static void querylabels_done_internal(ExecutionCtx *eCtx, RedisModuleCtx *ctx) {
    ARR(ARR(RedisModuleString *)) nodesResults = collect_node_results(eCtx, ctx);
    if (!nodesResults) {
        return;
    }

    RedisModuleDict *agg = RedisModule_CreateDict(NULL);
    array_foreach(nodesResults, stringList, {
        array_foreach(stringList, s, { RedisModule_DictSet(agg, s, NULL); });
    });
    array_free(nodesResults);

    ReplyWithKeySetFromDict(ctx, agg);
    RedisModule_FreeDict(NULL, agg);
}

static void querylabels_done(ExecutionCtx *eCtx, void *privateData) {
    RedisModuleBlockedClient *bc = privateData;
    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(bc);

    switch (TSGlobalConfig.libmrProtocol) {
        case LIBMR_PROTOCOL_GEARS:
            querylabels_done_gears(eCtx, ctx);
            break;
        case LIBMR_PROTOCOL_INTERNAL:
            querylabels_done_internal(eCtx, ctx);
            break;
        default:
            RedisModule_ReplyWithError(ctx, "Unknown LibMR protocol");
    }

    RTS_UnblockClient(bc, ctx);
}

int TSDB_querylabels_MR(RedisModuleCtx *ctx,
                        QueryLabelsSubtype subtype,
                        RedisModuleString *label,
                        QueryPredicateList *queries) {
    QueryLabelsArg *queryArg = calloc(1, sizeof(QueryLabelsArg));
    queryArg->shouldReturnNull = false;
    queryArg->refCount = 1;
    queryArg->subtype = subtype;
    queryArg->userName = CopyCurrentUserName(ctx);
    if (label != NULL) {
        queryArg->label = RedisModule_CreateStringFromString(NULL, label);
    }
    if (queries != NULL) {
        __atomic_add_fetch(
            &queries->ref, 1, __ATOMIC_RELAXED); // see rationale in TSDB_mrange_MR above
        queryArg->predicates = queries;
        queryArg->hasFilter = true;
    }

    ExecutionBuilder *builder = NULL;
    switch (TSGlobalConfig.libmrProtocol) {
        case LIBMR_PROTOCOL_GEARS: {
            builder = MR_CreateExecutionBuilder("ShardQuerylabelsMapper", queryArg);
            MR_ExecutionBuilderCollect(builder);
            break;
        }
        case LIBMR_PROTOCOL_INTERNAL: {
            builder = MR_CreateEmptyExecutionBuilder();
            MR_ExecutionBuilderInternalCommand(builder, "TS.INTERNAL_SLOT_RANGES", NULL);
            MR_ExecutionBuilderInternalCommand(builder, "TS.INTERNAL_QUERYLABELS", queryArg);
            break;
        }
        default: {
            RedisModule_ReplyWithError(ctx, "Unknown LibMR protocol");
            return REDISMODULE_OK;
        }
    }

    MRError *err = NULL;
    Execution *exec = MR_CreateExecution(builder, &err);
    if (err) {
        RedisModule_ReplyWithError(ctx, MR_ErrorGetMessage(err));
        // MR_CreateExecution always allocates and returns exec, even on error (it copies
        // and dups every step's args before checking the error) - free it or exec plus its
        // duplicated QueryLabelsArg reference leak on every failed call. That only drops
        // the execution's dup'd reference though - also drop our own original reference
        // (which in turn releases the extra QueryPredicateList ref taken above for FILTER).
        MR_FreeExecution(exec);
        QueryLabelsArg_ObjectFree(queryArg);
        MR_FreeExecutionBuilder(builder);
        return REDISMODULE_OK;
    }

    RedisModuleBlockedClient *bc = RTS_BlockClient(ctx, rts_free_rctx);
    MR_ExecutionSetOnDoneHandler(exec, querylabels_done, bc);

    MR_Run(exec);
    MR_FreeExecution(exec);
    MR_FreeExecutionBuilder(builder);
    return REDISMODULE_OK;
}
