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
