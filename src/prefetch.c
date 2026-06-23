/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "prefetch.h"

#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <time.h>

#include "rmutil/alloc.h"

extern int (*RedisModule_SwapPrefetchKey)(RedisModuleCtx *ctx,
                                          RedisModuleString *keyname,
                                          RedisModuleSwapPrefetchCB fn,
                                          void *user_data,
                                          int flags);
extern int (*RedisModule_IsKeyInRam)(RedisModuleCtx *ctx, RedisModuleString *key);
extern bool bigredis_enabled;

typedef struct PrefetchBatch
{
    _Atomic int pending;
    PrefetchDoneFn done;
    void *user_data;
} PrefetchBatch;

// Warn-once latch for the in-callback residency probe below.
static atomic_flag g_prefetch_evicted_warned = ATOMIC_FLAG_INIT;

static void prefetch_callback(RedisModuleCtx *ctx, RedisModuleString *key, void *user_data) {
    PrefetchBatch *batch = user_data;

    // Defensive residency probe: a Flex key can in principle be re-evicted
    // between SwapPrefetchKey completion and this callback running
    // (concurrent memory pressure / TTL / DEL). Diagnostic only — the
    // downstream GetSeries absorbs a cold re-read silently. Surface via
    // warn-once so the race shows up in logs if it ever happens.
    if (!RedisModule_IsKeyInRam(ctx, key)) {
        if (!atomic_flag_test_and_set_explicit(&g_prefetch_evicted_warned, memory_order_relaxed)) {
            RedisModule_Log(ctx,
                            "warning",
                            "Prefetch callback fired but key is no longer in "
                            "RAM (eviction race). Downstream open will pay a "
                            "cold-read penalty. Logged once per process.");
        }
    }

    // acq_rel: the last decrement synchronizes with all earlier completers
    // so `done` sees their writes to user_data state.
    if (atomic_fetch_sub_explicit(&batch->pending, 1, memory_order_acq_rel) == 1) {
        batch->done(ctx, batch->user_data);
        free(batch);
    }
}

static PrefetchResult PrefetchKeys(RedisModuleCtx *ctx,
                                   RedisModuleString **keys,
                                   size_t nkeys,
                                   PrefetchDoneFn done,
                                   void *user_data) {
    if (RedisModule_SwapPrefetchKey == NULL || RedisModule_IsKeyInRam == NULL) {
        return PrefetchResult_Completed;
    }

    // Fast path: skip allocation when every key is already hot.
    bool any_cold = false;
    for (size_t i = 0; i < nkeys; i++) {
        if (!RedisModule_IsKeyInRam(ctx, keys[i])) {
            any_cold = true;
            break;
        }
    }

    if (!any_cold) {
        return PrefetchResult_Completed;
    }

    PrefetchBatch *batch = malloc(sizeof(*batch));
    batch->done = done;
    batch->user_data = user_data;
    atomic_init(&batch->pending, 1);

    for (size_t i = 0; i < nkeys; i++) {
        if (RedisModule_IsKeyInRam(ctx, keys[i]))
            continue;
        atomic_fetch_add_explicit(&batch->pending, 1, memory_order_relaxed);
        int rc = RedisModule_SwapPrefetchKey(ctx, keys[i], prefetch_callback, batch, 0);
        if (rc != REDISMODULE_OK) {
            // Dispatch failed: the callback will never fire for this key, so
            // balance the bump we just made.
            atomic_fetch_sub_explicit(&batch->pending, 1, memory_order_relaxed);
        }
    }

    // Release the sentinel. If every prefetch completed inline we are the
    // last decrement — free and return Completed without invoking `done`.
    if (atomic_fetch_sub_explicit(&batch->pending, 1, memory_order_acq_rel) == 1) {
        free(batch);
        return PrefetchResult_Completed;
    }

    return PrefetchResult_Pending;
}

// ---------------------------------------------------------------------------
// PrefetchKeysBatched: bounded fan-in via slice iteration.
// ---------------------------------------------------------------------------
//
// Two completion paths per slice, distinguished by PrefetchKeys's return:
//
//   sync  — keys hot / all prefetches drained inline. PrefetchKeys returns
//           PrefetchResult_Completed without invoking the callback. drive_slices emits the slice
//           via batch_fn, advances slice_start, and loops to the next slice.
//
//   async — at least one prefetch is pending. PrefetchKeys returns PrefetchResult_Pending;
//           prefetch_callback eventually fires on_slice_async_done on a clean
//           stack. That callback emits the slice, advances, and re-enters
//           drive_slices.
typedef struct PrefetchBatchedState
{
    RedisModuleCtx *ctx;
    RedisModuleString **keys;
    size_t nkeys;
    size_t batch_size;
    size_t slice_start; // index of next slice's first key in `keys`
    size_t slice_count; // length of the slice currently in flight; on_slice_
                        // async_done reads this to know which slice completed
    PrefetchBatchFn batch_fn;
    PrefetchAllDoneFn all_done_fn;
    void *user_data;
} PrefetchBatchedState;

static void drive_slices(PrefetchBatchedState *st);

// Async-only callback: fires from prefetch_callback once a slice's pending
// prefetches drain. Never invoked inline by PrefetchKeys.
static void on_slice_async_done(RedisModuleCtx *ctx, void *user_data) {
    PrefetchBatchedState *st = user_data;
    st->ctx = ctx;
    st->batch_fn(ctx, st->slice_start, st->slice_count, st->user_data);
    st->slice_start += st->slice_count;
    drive_slices(st);
}

static void drive_slices(PrefetchBatchedState *st) {
    while (st->slice_start < st->nkeys) {
        size_t count = st->batch_size;
        if (st->slice_start + count > st->nkeys) {
            count = st->nkeys - st->slice_start;
        }
        st->slice_count = count;
        PrefetchResult r =
            PrefetchKeys(st->ctx, st->keys + st->slice_start, count, on_slice_async_done, st);
        if (r == PrefetchResult_Pending) {
            // Async wait — on_slice_async_done resumes the loop later.
            return;
        }
        // Completed: slice keys are in RAM right now. Emit, advance, loop.
        st->batch_fn(st->ctx, st->slice_start, count, st->user_data);
        st->slice_start += count;
    }
    // All slices emitted. Snapshot fields before free() so we don't reach
    // into dead memory after the final callback.
    const PrefetchAllDoneFn done = st->all_done_fn;
    RedisModuleCtx *ctx = st->ctx;
    void *user_data = st->user_data;
    free(st);
    done(ctx, user_data);
}

PrefetchResult PrefetchKeysBatched(RedisModuleCtx *ctx,
                                   RedisModuleString **keys,
                                   size_t nkeys,
                                   size_t batch_size,
                                   PrefetchBatchFn batch_fn,
                                   PrefetchAllDoneFn all_done_fn,
                                   void *user_data) {
    if (nkeys == 0) {
        // Match the SwapPrefetchKey==NULL no-op path's callback shape: always
        // fire batch_fn first (with a zero-length slice here) then all_done_fn,
        // so callers see one uniform contract regardless of which fast-path
        // triggered. batch_fn must be empty-tolerant.
        batch_fn(ctx, 0, 0, user_data);
        all_done_fn(ctx, user_data);
        return PrefetchResult_Completed;
    }

    if (batch_size == 0 || batch_size >= nkeys) {
        batch_size = nkeys;
    }

    if (RedisModule_SwapPrefetchKey == NULL || RedisModule_IsKeyInRam == NULL) {
        batch_fn(ctx, 0, nkeys, user_data);
        all_done_fn(ctx, user_data);
        return PrefetchResult_Completed;
    }

    PrefetchBatchedState *st = malloc(sizeof(*st));
    *st = (PrefetchBatchedState){
        .ctx = ctx,
        .keys = keys,
        .nkeys = nkeys,
        .batch_size = batch_size,
        .batch_fn = batch_fn,
        .all_done_fn = all_done_fn,
        .user_data = user_data,
    };

    // Even if every slice completes inline (and so `all_done_fn` already
    // fired by the time drive_slices returns), we conservatively report
    // Pending so callers always rely on the callback for completion ordering.
    drive_slices(st);
    return PrefetchResult_Pending;
}

// ---------------------------------------------------------------------------
// PrefetchKeysSync: synchronous wrapper for use inside LibMR mappers.
// ---------------------------------------------------------------------------

typedef struct PrefetchSyncWait
{
    pthread_mutex_t mu;
    pthread_cond_t cv;
    bool done;
    _Atomic int refcount;
} PrefetchSyncWait;

static void prefetch_sync_release(PrefetchSyncWait *w) {
    if (atomic_fetch_sub_explicit(&w->refcount, 1, memory_order_acq_rel) == 1) {
        pthread_mutex_destroy(&w->mu);
        pthread_cond_destroy(&w->cv);
        free(w);
    }
}

static void prefetch_sync_done(RedisModuleCtx *ctx, void *user_data) {
    (void)ctx;
    PrefetchSyncWait *w = user_data;
    pthread_mutex_lock(&w->mu);
    w->done = true;
    pthread_cond_signal(&w->cv);
    pthread_mutex_unlock(&w->mu);
    prefetch_sync_release(w);
}

void PrefetchKeysSync(RedisModuleCtx *ctx, RedisModuleString **keys, size_t nkeys) {
    if (RedisModule_SwapPrefetchKey == NULL || RedisModule_IsKeyInRam == NULL) {
        return;
    }

    PrefetchSyncWait *w = malloc(sizeof(*w));
    pthread_mutex_init(&w->mu, NULL);
#if defined(__linux__)
    pthread_condattr_t cv_attr;
    pthread_condattr_init(&cv_attr);
    pthread_condattr_setclock(&cv_attr, CLOCK_MONOTONIC);
    pthread_cond_init(&w->cv, &cv_attr);
    pthread_condattr_destroy(&cv_attr);
#else
    pthread_cond_init(&w->cv, NULL);
#endif
    w->done = false;
    atomic_init(&w->refcount, 2); // one for this caller, one for the callback

    PrefetchResult r = PrefetchKeys(ctx, keys, nkeys, prefetch_sync_done, w);

    if (r == PrefetchResult_Completed) {
        // No callback will fire — drop the callback's refcount on its behalf.
        prefetch_sync_release(w);
        prefetch_sync_release(w);
        return;
    }

    // Release the thread-safe ctx lock for the wait so the prefetch completion
    // callback (which may run on the main thread and need the same lock) can
    // make progress. Re-acquire before returning so the caller's invariant is
    // restored.
    RedisModule_ThreadSafeContextUnlock(ctx);

    // On timeout: log, drop our refcount, and return.
    // The caller's GetSeries will pay a synchronous flash read, same
    // degraded path as a SwapPrefetchKey dispatch failure, no crash.
    struct timespec deadline;
#if defined(__linux__)
    clock_gettime(CLOCK_MONOTONIC, &deadline); // matches the cv's CLOCK_MONOTONIC attr
#else
    clock_gettime(CLOCK_REALTIME, &deadline); // macOS cv uses the realtime clock
#endif
    deadline.tv_sec += PREFETCH_SYNC_TIMEOUT_SECONDS;

    pthread_mutex_lock(&w->mu);
    bool timed_out = false;
    while (!w->done) {
        int rc = pthread_cond_timedwait(&w->cv, &w->mu, &deadline);
        if (rc == ETIMEDOUT) {
            timed_out = true;
            break;
        }
    }
    pthread_mutex_unlock(&w->mu);
    RedisModule_ThreadSafeContextLock(ctx);

    if (timed_out) {
        RedisModule_Log(ctx,
                        "warning",
                        "PrefetchKeysSync timed out after %ds waiting for "
                        "%zu key(s); falling back to synchronous GetSeries",
                        PREFETCH_SYNC_TIMEOUT_SECONDS,
                        nkeys);
    }

    prefetch_sync_release(w);
}

void ForEachKeyWithSyncPrefetch(RedisModuleCtx *ctx,
                                RedisModuleString **keys,
                                size_t nkeys,
                                size_t batch_size,
                                PrefetchedKeyFn fn,
                                void *user_data) {
    if (batch_size == 0) {
        batch_size = nkeys;
    }
    for (size_t batch_start = 0; batch_start < nkeys; batch_start += batch_size) {
        size_t batch_end = batch_start + batch_size;
        if (batch_end > nkeys) {
            batch_end = nkeys;
        }
        if (bigredis_enabled) {
            PrefetchKeysSync(ctx, keys + batch_start, batch_end - batch_start);
        }
        for (size_t i = batch_start; i < batch_end; i++) {
            fn(ctx, keys[i], user_data);
        }
    }
}
