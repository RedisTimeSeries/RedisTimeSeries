/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include <stdbool.h>
#include <stddef.h>

#include "RedisModulesSDK/redismodule.h"

// Result of a PrefetchKeys / PrefetchKeysBatched call.
typedef enum
{
    PrefetchResult_Completed, // finished inline; no callback will fire
    PrefetchResult_Pending,   // async; callback fires when prefetches drain
} PrefetchResult;

// Async-only completion callback. Fired once all pending prefetches in a
// batch have drained. (Used by the file-local PrefetchKeys in prefetch.c.)
typedef void (*PrefetchDoneFn)(RedisModuleCtx *ctx, void *user_data);

// Per-slice callback fired by PrefetchKeysBatched once each batch of keys is
// resident in RAM. `start`/`count` describe the slice within the original
// `keys` array. The caller typically does GetSeries + reply emission for
// keys[start..start+count) here.
typedef void (*PrefetchBatchFn)(RedisModuleCtx *ctx, size_t start, size_t count, void *user_data);

// Final callback fired by PrefetchKeysBatched after every slice's batch_fn
// has run. The caller usually finalises the reply (set array length) and
// calls RedisModule_UnblockClient here.
typedef void (*PrefetchAllDoneFn)(RedisModuleCtx *ctx, void *user_data);

// Bounded-fan-in version of PrefetchKeys. Iterates the `keys` array in
// slices of `batch_size`; for each slice, issues SwapPrefetchKey for the
// cold subset, waits for those to complete, fires `batch_fn(start,count)`
// so the caller can do its per-slice work, then advances. After the last
// slice, fires `all_done_fn`.
//
// `batch_size == 0` or `batch_size >= nkeys` collapses to single-slice
// behaviour (equivalent to PrefetchKeys + a one-shot batch_fn).
//
// On OSS Redis or non Flex Enterprise builds (prefetch APIs unavailable),
// fires `batch_fn(0, nkeys)` then `all_done_fn` synchronously and returns
// PrefetchResult_Completed.
//
// Returns PrefetchResult_Completed if `all_done_fn` ran synchronously on the
// calling thread, PrefetchResult_Pending if it will fire later via the
// chained prefetch callbacks.
//
PrefetchResult PrefetchKeysBatched(RedisModuleCtx *ctx,
                                   RedisModuleString **keys,
                                   size_t nkeys,
                                   size_t batch_size,
                                   PrefetchBatchFn batch_fn,
                                   PrefetchAllDoneFn all_done_fn,
                                   void *user_data);

// Synchronous prefetch — issues async prefetches and blocks the caller until
// every key is in RAM. Intended for use inside LibMR mapper callbacks where
// the surrounding execution model is synchronous (no BlockClient available).
//
// PRECONDITION: `ctx` must be a thread-safe context that the caller currently
// holds via RedisModule_ThreadSafeContextLock. PrefetchKeysSync calls
// RedisModule_ThreadSafeContextUnlock internally to release the GIL across the
// cond-var wait so the prefetch completion callback (which runs on the main
// thread and needs the same lock) can make progress; the lock is reacquired
// before this function returns. Calling without holding the lock is undefined
// behaviour.
//
// On OSS Redis or older Enterprise builds (prefetch APIs unavailable) this is
// a no-op that returns immediately.
void PrefetchKeysSync(RedisModuleCtx *ctx, RedisModuleString **keys, size_t nkeys);

// Per-key callback for ForEachKeyWithSyncPrefetch.
typedef void (*PrefetchedKeyFn)(RedisModuleCtx *ctx, RedisModuleString *key, void *user_data);

// SYNCHRONOUS batched key iteration for LibMR mapper callbacks. Walks `keys` in
// slices of `batch_size`, BLOCKING on PrefetchKeysSync for each slice (only when
// bigredis is enabled) before invoking `fn` per key in that slice. Blocks the
// calling thread; `ctx` must be a thread-safe context the caller holds via
// RedisModule_ThreadSafeContextLock. `batch_size == 0` collapses to one slice.
void ForEachKeyWithSyncPrefetch(RedisModuleCtx *ctx,
                                RedisModuleString **keys,
                                size_t nkeys,
                                size_t batch_size,
                                PrefetchedKeyFn fn,
                                void *user_data);

#define PREFETCH_SYNC_TIMEOUT_SECONDS 60
#define PREFETCH_ASYNC_TIMEOUT_MS (PREFETCH_SYNC_TIMEOUT_SECONDS * 1000)
