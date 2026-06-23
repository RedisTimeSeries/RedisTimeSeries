/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "prefetch_commands.h"

#include "common.h" // RTS_ReplyKeyPermissionsError
#include "config.h" // TSGlobalConfig
#include "module.h" // CheckKeyIsAllowedByAclsForUser, mget_emit_for_key
#include "prefetch.h"
#include "query_language.h"
#include "reply.h" // ReplySeriesArrayPos
#include "streaming_resultset.h"
#include "tsdb.h" // GetSeries
#include "utils/blocked_client.h"

#include "RedisModulesSDK/redismodule.h"
#include "rmutil/alloc.h"

#include <stdlib.h>
#include <string.h>

// State carried across the optional asynchronous prefetch hop in
// TSDB_generic_mrange. All allocations stored here are detached from the
// command-handler ctx so they survive past RedisModule_BlockClient until the
// continuation runs.
typedef struct MRangeAsyncCtx
{
    RTS_AsyncGuard guard;     // MUST be first — async_prefetch_timeout casts privdata to it
    MRangeArgs args;          // limitLabels detached in-place during setup
    char *groupByLabel_owned; // strdup'd; NULL if not grouped
    RedisModuleString **keys; // CreateString(NULL, ...) — manual lifetime
    size_t nkeys;
    long long replylen;           // running ungrouped reply count, finalised at end
    RedisModuleBlockedClient *bc; // NULL when running synchronously
    RedisModuleCtx *reply_ctx;    // thread-safe ctx, allocated lazily
    RedisModuleString *user_name; // user that issued the command — async ACL
    // Non-NULL on the grouped async path (NULL when ungrouped). Ownership is
    // transferred to the all-done callback (set to NULL there);
    // mrange_async_ctx_destroy frees it if still set (error-before-emit path).
    TS_StreamingResultSet *streaming_rs;
} MRangeAsyncCtx;

static int async_prefetch_timeout(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    (void)argv;
    (void)argc;
    RTS_AsyncGuard *g = RedisModule_GetBlockedClientPrivateData(ctx);
    if (g && RTS_AsyncGuard_Settle(g)) {
        RedisModule_Log(ctx,
                        "warning",
                        "async prefetch did not complete within %dms; aborting blocked "
                        "client (possible lost SwapPrefetchKey completion). Late completion "
                        "still drives cleanup; the command context leaks only if the "
                        "completion never fires.",
                        PREFETCH_ASYNC_TIMEOUT_MS);
        RedisModule_ReplyWithError(ctx, "ERR TSDB: timed out waiting for keys to load from flash");
    }
    return REDISMODULE_OK;
}

// Tear down a fully-detached MRangeAsyncCtx. Called once, when the last guard
// reference is dropped (see mrange_free_privdata / the all-done callbacks), or
// directly on the setup error path before the guard is armed.
static void mrange_async_ctx_destroy(MRangeAsyncCtx *m) {
    if (m->keys) {
        for (size_t i = 0; i < m->nkeys; i++) {
            RedisModule_FreeString(NULL, m->keys[i]);
        }
        free(m->keys);
    }
    for (int i = 0; i < m->args.numLimitLabels; i++) {
        if (m->args.limitLabels[i]) {
            RedisModule_FreeString(NULL, m->args.limitLabels[i]);
        }
    }
    free(m->groupByLabel_owned);
    // m->args.groupByLabel aliases m->groupByLabel_owned (set during async-path
    // setup). Clear the alias so MRangeArgs_Free can't double-free it if it
    // ever starts taking ownership of groupByLabel.
    m->args.groupByLabel = NULL;
    if (m->streaming_rs)
        StreamingResultSet_Free(m->streaming_rs);
    if (m->user_name)
        RedisModule_FreeString(NULL, m->user_name);
    if (m->reply_ctx)
        RedisModule_FreeThreadSafeContext(m->reply_ctx);
    MRangeArgs_Free(&m->args);
    free(m);
}

// Sole destroyer of the async MRangeAsyncCtx. Redis invokes this once per blocked
// client — only when RTS_UnblockClient is called (the all-done callback always
// calls it). It is NOT called on timeout for a non-keys client, so a
// genuinely-lost prefetch completion (all-done never fires) leaks m; that is
// the documented, bounded leak the timeout exists to bound.
static void mrange_free_privdata(RedisModuleCtx *ctx, void *privdata) {
    (void)ctx;
    MRangeAsyncCtx *m = privdata;
    if (m)
        mrange_async_ctx_destroy(m);
}

// Per-batch callback for the STREAMING grouped path. After each slice's
// keys have been prefetched into RAM, feed them into the StreamingResultSet
// and close them immediately. Bigredis can evict slice K's keys before
// slice K+1 prefetches — matches the per-slice eviction pattern of the
// ungrouped path.
static void mrange_grouped_stream_feed(RedisModuleCtx *cb_ctx,
                                       size_t start,
                                       size_t count,
                                       void *user_data) {
    (void)cb_ctx;
    MRangeAsyncCtx *m = user_data;
    if (!m->streaming_rs)
        return;
    for (size_t i = start; i < start + count; i++) {
        if (!CheckKeyIsAllowedByAclsForUser(m->user_name, m->keys[i], REDISMODULE_CMD_KEY_ACCESS)) {
            RedisModule_Log(m->reply_ctx,
                            "warning",
                            "The user lacks the required permissions for the key, skipping.");
            continue;
        }
        RedisModuleKey *key;
        Series *series;
        const GetSeriesResult status = GetSeries(m->reply_ctx,
                                                 m->keys[i],
                                                 &key,
                                                 &series,
                                                 REDISMODULE_READ,
                                                 GetSeriesFlags_SilentOperation);
        if (status != GetSeriesResult_Success) {
            continue;
        }
        StreamingResultSet_FeedSeries(m->streaming_rs, series);
        RedisModule_CloseKey(key);
    }
}

// All-done callback for the streaming grouped path: just emit. Series have
// already been fed slice-by-slice in mrange_grouped_stream_feed; finalize
// walks the per-group accumulators, builds reduced Series, and writes the
// reply. Then unblock.
static void mrange_grouped_stream_done(RedisModuleCtx *ctx, void *user_data) {
    (void)ctx;
    MRangeAsyncCtx *m = user_data;
    // Settle gates only the reply: on the timeout-lost path the client was
    // already replied and bc->client nulled, so skip the emit (the leftover
    // streaming_rs is freed by mrange_async_ctx_destroy). But ALWAYS unblock — Redis
    // does not free a non-keys blocked client's privdata on timeout (see
    // unblockClientFromModule); this RTS_UnblockClient is the only thing that
    // drives free_privdata, so skipping it would leak m + the blocked client.
    if (RTS_AsyncGuard_Settle(&m->guard)) {
        if (m->streaming_rs) {
            StreamingResultSet_FinalizeAndReply(m->reply_ctx,
                                                m->streaming_rs,
                                                m->args.withLabels,
                                                (RedisModuleString **)m->args.limitLabels,
                                                m->args.numLimitLabels,
                                                m->args.reverse);
            // FinalizeAndReply freed the streaming_rs; clear the pointer so
            // mrange_async_ctx_destroy doesn't double-free.
            m->streaming_rs = NULL;
        }
    }
    // Always unblock — this drives free_privdata, the sole destroyer of m.
    RTS_UnblockClient(m->bc, m);
}

// Ungrouped per-key emit — body extracted from `replyUngroupedMultiRange`'s
// loop body so the batched path can reuse it without going through the
// dict-iterator-restart machinery (we already have a stable keys array).
static void mrange_emit_ungrouped_one(RedisModuleCtx *ctx, MRangeAsyncCtx *m, size_t i) {
    // Re-check ACLs on the async emit ctx (which has no client → no current
    // user). QueryIndex already gated the keyset on the command ctx; this
    // catches the rare case where the user's permissions change between command
    // entry and emit. Skip the per-key GetSeries ACL flag — without a current
    // user it would warn-and-allow.
    if (!CheckKeyIsAllowedByAclsForUser(m->user_name, m->keys[i], REDISMODULE_CMD_KEY_ACCESS)) {
        RedisModule_Log(
            ctx, "warning", "The user lacks the required permissions for the key, skipping.");
        return;
    }
    RedisModuleKey *key;
    Series *series;
    const GetSeriesResult status =
        GetSeries(ctx, m->keys[i], &key, &series, REDISMODULE_READ, GetSeriesFlags_SilentOperation);
    if (status != GetSeriesResult_Success)
        return;
    ReplySeriesArrayPos(ctx,
                        series,
                        m->args.withLabels,
                        (RedisModuleString **)m->args.limitLabels,
                        m->args.numLimitLabels,
                        &m->args.rangeArgs,
                        m->args.reverse,
                        false);
    m->replylen++;
    RedisModule_CloseKey(key);
}

// PrefetchKeysBatched per-slice callback for the ungrouped path.
static void mrange_emit_batch_ungrouped(RedisModuleCtx *cb_ctx,
                                        size_t start,
                                        size_t count,
                                        void *user_data) {
    (void)cb_ctx;
    MRangeAsyncCtx *m = user_data;
    for (size_t i = start; i < start + count; i++) {
        mrange_emit_ungrouped_one(m->reply_ctx, m, i);
    }
}

// PrefetchKeysBatched final callback for the ungrouped path — close the
// outer array and unblock. `mrange_free_privdata` runs after the unblock
// flush, so we don't free `m` here.
static void mrange_all_done_ungrouped(RedisModuleCtx *cb_ctx, void *user_data) {
    (void)cb_ctx;
    MRangeAsyncCtx *m = user_data;
    // Settle gates only the reply: on the timeout-lost path the client was
    // already replied (bc->client nulled), so skip finalizing the array — the
    // partial POSTPONED reply on reply_ctx is discarded on unblock since
    // bc->client is NULL. But ALWAYS unblock: Redis doesn't free a non-keys
    // blocked client's privdata on timeout, so this is the only driver of
    // free_privdata; skipping it would leak m + the blocked client.
    if (RTS_AsyncGuard_Settle(&m->guard)) {
        RedisModule_ReplySetMapOrArrayLength(m->reply_ctx, m->replylen, false);
    }
    // Always unblock — this drives free_privdata, the sole destroyer of m.
    RTS_UnblockClient(m->bc, m);
}

// ASYNC mrange — build heap-owned state that survives the BlockClient hop, then
// drive batched prefetch. Takes ownership of `resultSeries` and the contents of
// `args` (moved into the heap MRangeAsyncCtx). ACL filtering for the keyset
// already happened inside QueryIndex via the `&hasPermissionError` channel at
// the call site.
int MRange_ReplyAsync(RedisModuleCtx *ctx, RedisModuleDict *resultSeries, MRangeArgs *args) {
    MRangeAsyncCtx *m = calloc(1, sizeof(*m));

    // Capture the user-name on the original ctx so the async emit can resolve
    // ACLs without a client. NULL is acceptable here (no ACL user → emit will
    // treat every key as denied), but in practice an authenticated client is
    // always present on this path.
    RedisModuleString *uname = RedisModule_GetCurrentUserName(ctx);
    if (uname) {
        size_t ulen;
        const char *up = RedisModule_StringPtrLen(uname, &ulen);
        m->user_name = RedisModule_CreateString(NULL, up, ulen);
        RedisModule_FreeString(ctx, uname);
    }

    for (int i = 0; i < args->numLimitLabels; i++) {
        size_t len;
        const char *p = RedisModule_StringPtrLen(args->limitLabels[i], &len);
        args->limitLabels[i] = RedisModule_CreateString(NULL, p, len);
    }
    if (args->groupByLabel) {
        m->groupByLabel_owned = strdup(args->groupByLabel);
        args->groupByLabel = m->groupByLabel_owned;
    }
    m->args = *args;

    const bool grouped_streaming = (m->args.groupByLabel != NULL);

    // Materialize keys. The streaming path consumes via m->keys[] only — no
    // result-dict clone needed (unlike the legacy buffered reducer).
    {
        m->keys = malloc(RedisModule_DictSize(resultSeries) * sizeof(*m->keys));
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(resultSeries, "^", NULL, 0);
        char *currentKey;
        size_t currentKeyLen;
        while ((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
            m->keys[m->nkeys++] = RedisModule_CreateString(NULL, currentKey, currentKeyLen);
        }
        RedisModule_DictIteratorStop(iter);
        RedisModule_FreeDict(ctx, resultSeries);
    }

    // Create the streaming resultset BEFORE BlockClient so any failure can be
    // returned synchronously. A NULL here (unknown reducer / bad args) must not
    // reach BlockClient: the stream_done callback skips the reply when
    // streaming_rs is NULL, which would unblock the client with no reply.
    if (grouped_streaming) {
        m->streaming_rs = StreamingResultSet_Create(
            m->args.groupByLabel, &m->args.gropuByReducerArgs, &m->args.rangeArgs);
        if (!m->streaming_rs) {
            // Guard not armed yet — destroy directly rather than via the unref path.
            mrange_async_ctx_destroy(m);
            return RedisModule_ReplyWithError(ctx, "ERR TSDB: failed to create grouped result set");
        }
    }

    m->bc = RTS_BlockClientTimeout(
        ctx, mrange_free_privdata, async_prefetch_timeout, PREFETCH_ASYNC_TIMEOUT_MS);
    if (!m->bc) {
        // Guard not armed yet — destroy directly rather than via the unref path.
        mrange_async_ctx_destroy(m);
        return RedisModule_ReplyWithError(ctx, "ERR failed to block client");
    }
    RTS_AsyncGuard_Init(&m->guard);
    // Make m reachable via GetBlockedClientPrivateData so the timeout path frees
    // it through free_privdata even though that path never calls UnblockClient.
    RedisModule_BlockClientSetPrivateData(m->bc, m);
    m->reply_ctx = RedisModule_GetThreadSafeContext(m->bc);
    RedisModule_AutoMemory(m->reply_ctx);
    if (grouped_streaming) {
        // Streaming grouped: feed each batch into the accumulator, evict
        // between slices, finalize+emit at end.
        PrefetchKeysBatched(ctx,
                            m->keys,
                            m->nkeys,
                            TSGlobalConfig.prefetchBatchSize,
                            mrange_grouped_stream_feed,
                            mrange_grouped_stream_done,
                            m);
    } else {
        // Ungrouped: per-slice emit bounds the prefetch fan-in.
        RedisModule_ReplyWithMapOrArray(m->reply_ctx, REDISMODULE_POSTPONED_ARRAY_LEN, false);
        PrefetchKeysBatched(ctx,
                            m->keys,
                            m->nkeys,
                            TSGlobalConfig.prefetchBatchSize,
                            mrange_emit_batch_ungrouped,
                            mrange_all_done_ungrouped,
                            m);
    }
    return REDISMODULE_OK;
}

// State carried across the optional asynchronous prefetch hop in TSDB_mget.
// All allocations stored here are detached from the command-handler ctx so
// they survive past RedisModule_BlockClient until the continuation runs.
typedef struct MGetAsyncCtx
{
    RTS_AsyncGuard guard; // MUST be first — async_prefetch_timeout casts privdata to it
    MGetArgs args;
    RedisModuleString **keys; // CreateString(NULL, ...) — manual lifetime
    size_t nkeys;
    long long replylen;           // running reply count, finalised at the end
    RedisModuleBlockedClient *bc; // NULL when running synchronously
    RedisModuleCtx *reply_ctx;    // thread-safe ctx, allocated lazily
    // Name of the user that issued the command, captured before the async hop.
    // The thread-safe reply_ctx has no client → no current user, so the async
    // emit path resolves ACLs via this stashed name instead. Retained on the
    // global pool (CreateString(NULL,...)) so it survives the hop.
    RedisModuleString *user_name;
} MGetAsyncCtx;

// Tear down a fully-detached MGetAsyncCtx. Called once, when the last guard
// reference is dropped, or directly on the setup error path before the guard
// is armed.
static void mget_async_ctx_destroy(MGetAsyncCtx *m) {
    if (m->keys) {
        for (size_t i = 0; i < m->nkeys; i++) {
            RedisModule_FreeString(NULL, m->keys[i]);
        }
        free(m->keys);
    }
    for (int i = 0; i < m->args.numLimitLabels; i++) {
        if (m->args.limitLabels[i]) {
            RedisModule_FreeString(NULL, m->args.limitLabels[i]);
        }
    }
    MGetArgs_Free(&m->args);
    if (m->user_name)
        RedisModule_FreeString(NULL, m->user_name);
    if (m->reply_ctx)
        RedisModule_FreeThreadSafeContext(m->reply_ctx);
    free(m);
}

// Sole destroyer of the async MGetAsyncCtx. See mrange_free_privdata for the
// invocation contract (driven by RTS_UnblockClient; not called on non-keys
// timeout, so a lost completion leaks — bounded by the timeout).
static void mget_free_privdata(RedisModuleCtx *ctx, void *privdata) {
    (void)ctx;
    MGetAsyncCtx *m = privdata;
    if (m)
        mget_async_ctx_destroy(m);
}

// PrefetchKeysBatched per-slice callback — emits replies for keys[start..count]
// against the pre-allocated thread-safe reply ctx.
static void mget_emit_batch(RedisModuleCtx *cb_ctx, size_t start, size_t count, void *user_data) {
    (void)cb_ctx;
    MGetAsyncCtx *m = user_data;
    for (size_t i = start; i < start + count; i++) {
        mget_emit_for_key(m->reply_ctx, m->keys[i], &m->args, &m->replylen, m->user_name);
    }
}

// PrefetchKeysBatched final callback — close the array, unblock the client.
// `mget_free_privdata` is fired by Redis after the unblock flush, so we do
// not free `m` here.
static void mget_all_done(RedisModuleCtx *cb_ctx, void *user_data) {
    (void)cb_ctx;
    MGetAsyncCtx *m = user_data;
    // Settle gates only the reply: on the timeout-lost path the client was
    // already replied (bc->client nulled), so skip finalizing the array. But
    // ALWAYS unblock: Redis doesn't free a non-keys blocked client's privdata
    // on timeout, so this RTS_UnblockClient is the only driver of
    // free_privdata; skipping it would leak m + the blocked client.
    if (RTS_AsyncGuard_Settle(&m->guard)) {
        RedisModule_ReplySetMapOrArrayLength(m->reply_ctx, m->replylen, false);
    }
    // Always unblock — this drives free_privdata, the sole destroyer of m.
    RTS_UnblockClient(m->bc, m);
}

// ASYNC mget — build heap-owned state that survives the BlockClient hop, then
// drive batched prefetch. Takes ownership of `result` and the contents of
// `args` (moved into the heap MGetAsyncCtx). ACL filtering for the keyset
// already happened inside QueryIndex via the `&hasPermissionError` channel at
// the call site.
int MGet_ReplyAsync(RedisModuleCtx *ctx, RedisModuleDict *result, MGetArgs *args) {
    MGetAsyncCtx *m = calloc(1, sizeof(*m));

    // Capture the user-name on the original ctx so the async emit can resolve
    // ACLs without a client. See MRangeAsyncCtx comment for details.
    RedisModuleString *uname = RedisModule_GetCurrentUserName(ctx);
    if (uname) {
        size_t ulen;
        const char *up = RedisModule_StringPtrLen(uname, &ulen);
        m->user_name = RedisModule_CreateString(NULL, up, ulen);
        RedisModule_FreeString(ctx, uname);
    }

    for (int i = 0; i < args->numLimitLabels; i++) {
        size_t len;
        const char *p = RedisModule_StringPtrLen(args->limitLabels[i], &len);
        args->limitLabels[i] = RedisModule_CreateString(NULL, p, len);
    }
    m->args = *args;

    {
        m->keys = malloc(RedisModule_DictSize(result) * sizeof(*m->keys));
        RedisModuleDictIter *iter = RedisModule_DictIteratorStartC(result, "^", NULL, 0);
        char *currentKey;
        size_t currentKeyLen;
        while ((currentKey = RedisModule_DictNextC(iter, &currentKeyLen, NULL)) != NULL) {
            m->keys[m->nkeys++] = RedisModule_CreateString(NULL, currentKey, currentKeyLen);
        }
        RedisModule_DictIteratorStop(iter);
        RedisModule_FreeDict(ctx, result);
    }

    m->bc = RTS_BlockClientTimeout(
        ctx, mget_free_privdata, async_prefetch_timeout, PREFETCH_ASYNC_TIMEOUT_MS);
    if (!m->bc) {
        // Guard not armed yet — destroy directly rather than via the unref path.
        mget_async_ctx_destroy(m);
        return RedisModule_ReplyWithError(ctx, "ERR failed to block client");
    }
    RTS_AsyncGuard_Init(&m->guard);
    // Make m reachable via GetBlockedClientPrivateData so the timeout path frees
    // it through free_privdata even though that path never calls UnblockClient.
    RedisModule_BlockClientSetPrivateData(m->bc, m);
    m->reply_ctx = RedisModule_GetThreadSafeContext(m->bc);
    // Defensive parity with the mrange continuation: any reply helper that
    // inlines RedisModule_CreateString(ctx, ...) without explicit FreeString
    // would leak on this ctx without AutoMemory.
    RedisModule_AutoMemory(m->reply_ctx);
    RedisModule_ReplyWithMapOrArray(m->reply_ctx, REDISMODULE_POSTPONED_ARRAY_LEN, false);
    PrefetchKeysBatched(ctx,
                        m->keys,
                        m->nkeys,
                        TSGlobalConfig.prefetchBatchSize,
                        mget_emit_batch,
                        mget_all_done,
                        m);
    return REDISMODULE_OK;
}
