/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "RedisModulesSDK/redismodule.h"

#include <stdatomic.h>
#include <stdbool.h>

// create blocked client and report start time
RedisModuleBlockedClient *RTS_BlockClient(RedisModuleCtx *ctx,
                                          void (*free_privdata)(RedisModuleCtx *, void *));

// Like RTS_BlockClient but arms a timeout: if the client is not unblocked
// within timeout_ms, Redis fires `timeout_cb` and aborts the client. Used by
// the async prefetch paths so a lost SwapPrefetchKey completion can't leave a
// client blocked forever. Pair with RTS_AsyncGuard on the privdata.
RedisModuleBlockedClient *RTS_BlockClientTimeout(RedisModuleCtx *ctx,
                                                 void (*free_privdata)(RedisModuleCtx *, void *),
                                                 RedisModuleCmdFunc timeout_cb,
                                                 long long timeout_ms);

// unblock blocked client and report end time
void RTS_UnblockClient(RedisModuleBlockedClient *bc, void *privdata);

// Picks the single finisher that replies, when the prefetch all-done callback
// and the block-client timeout callback race on the main thread. Exactly one
// wins `Settle` and emits the reply; the loser stays silent. The all-done
// callback then ALWAYS calls RTS_UnblockClient regardless of who won, which
// drives free_privdata (the sole destroyer of the carrying context) — Redis
// does not free a non-keys blocked client's privdata on timeout, so the
// unblock is mandatory. Embed as the FIRST member of the per-command context
// so a type-agnostic timeout callback can reach it through
// RedisModule_GetBlockedClientPrivateData.
typedef struct
{
    _Atomic int settled; // 0 until a finisher wins the CAS
} RTS_AsyncGuard;

static inline void RTS_AsyncGuard_Init(RTS_AsyncGuard *g) {
    atomic_init(&g->settled, 0);
}

// Returns true to exactly one caller — the one that should emit the reply.
// The loser must not touch the reply ctx.
static inline bool RTS_AsyncGuard_Settle(RTS_AsyncGuard *g) {
    int expected = 0;
    return atomic_compare_exchange_strong_explicit(
        &g->settled, &expected, 1, memory_order_acq_rel, memory_order_acquire);
}

/**
 * @brief Block the calling client on a single key.
 *
 * Thin wrapper over RedisModule_BlockClientOnKeys() that always parks on
 * exactly one key. @p reply_callback is invoked on every
 * RedisModule_SignalKeyAsReady() on @p key (and once at setup);
 * @p timeout_callback fires after @p timeout_ms. @p privdata is owned by
 * the blocked client and freed via @p free_privdata once it unblocks.
 *
 * Also starts BlockedClientMeasureTime when the linked Redis supports it.
 *
 * @param ctx               Module context bound to the calling client.
 * @param reply_callback    Wake-up handler (return OK to commit, ERR to stay).
 * @param timeout_callback  Deadline handler.
 * @param free_privdata     Releases @p privdata after unblock; NULL-safe.
 * @param timeout_ms        Max wait in ms (0 = no timeout).
 * @param key               Single key to block on.
 * @param privdata          Caller-owned context handed to the callbacks.
 * @return Handle to the blocked client, or NULL if Redis refused to block.
 */
RedisModuleBlockedClient *RTS_BlockClientOnKey(RedisModuleCtx *ctx,
                                               RedisModuleCmdFunc reply_callback,
                                               RedisModuleCmdFunc timeout_callback,
                                               void (*free_privdata)(RedisModuleCtx *, void *),
                                               long long timeout_ms,
                                               RedisModuleString *key,
                                               void *privdata);
