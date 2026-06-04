/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

/*
 * These tests exercise the RDB chunk-load functions' allocation-failure path.
 *
 * The loaders live in chunk.c / compressed_chunk.c and allocate via the
 * rts_try_calloc / rts_try_alloc macros (RedisModule_Try* with a fallback).
 * To force that allocation to fail deterministically we compile the loaders
 * straight into this translation unit so their allocations resolve to *this*
 * binary's RedisModule_Try* pointers, which we override below. (Overriding the
 * pre-built module .so's allocator from the test binary is not possible.)
 *
 * The allocation is the first thing each loader does, before reading the RDB
 * stream, so the RedisModuleIO argument is never dereferenced and can be NULL.
 *
 * NOTE: the test binary also links redistimeseries.so, which defines these same
 * loaders (compiled WITH REDIS_MODULE_TARGET). We rely on symbol interposition:
 * the executable's in-TU copies (libc allocators) win, including for the other
 * chunk suites. If a linker/visibility change ever breaks that, an allocator
 * crossing would surface here under ASan (run by the sanitizer unit job).
 */

#include "minunit.h"

#include "chunk.c"
#include "compressed_chunk.c"

static void *rdb_oom_calloc(size_t nmemb, size_t size) {
    (void)nmemb;
    (void)size;
    return NULL; // simulate an exhausted allocator
}

static void *rdb_oom_alloc(size_t size) {
    (void)size;
    return NULL; // simulate an exhausted allocator
}

static void rdb_oom_noop_log(RedisModuleIO *io, const char *levelstr, const char *fmt, ...) {
    (void)io;
    (void)levelstr;
    (void)fmt; // no-op; RedisModule_LogIOError is unset in unit builds
}

MU_TEST(test_Uncompressed_LoadFromRDB_oom_returns_error) {
    void *(*saved_try_calloc)(size_t, size_t) = RedisModule_TryCalloc;
    void (*saved_log)(RedisModuleIO *, const char *, const char *, ...) = RedisModule_LogIOError;
    RedisModule_TryCalloc = rdb_oom_calloc;
    RedisModule_LogIOError = rdb_oom_noop_log;

    Chunk_t *chunk = (Chunk_t *)0xDEAD; // sentinel; must be cleared to NULL on error
    int rc = Uncompressed_LoadFromRDB(&chunk, NULL);

    RedisModule_TryCalloc = saved_try_calloc;
    RedisModule_LogIOError = saved_log;

    mu_assert_int_eq(TSDB_ERROR, rc);
    mu_assert(chunk == NULL, "chunk must be cleared to NULL when allocation fails");
}

MU_TEST(test_Compressed_LoadFromRDB_oom_returns_error) {
    void *(*saved_try_alloc)(size_t) = RedisModule_TryAlloc;
    void (*saved_log)(RedisModuleIO *, const char *, const char *, ...) = RedisModule_LogIOError;
    RedisModule_TryAlloc = rdb_oom_alloc;
    RedisModule_LogIOError = rdb_oom_noop_log;

    Chunk_t *chunk = (Chunk_t *)0xDEAD; // sentinel; must be cleared to NULL on error
    int rc = Compressed_LoadFromRDB(&chunk, NULL);

    RedisModule_TryAlloc = saved_try_alloc;
    RedisModule_LogIOError = saved_log;

    mu_assert_int_eq(TSDB_ERROR, rc);
    mu_assert(chunk == NULL, "chunk must be cleared to NULL when allocation fails");
}

MU_TEST_SUITE(rdb_load_oom_test_suite) {
    MU_RUN_TEST(test_Uncompressed_LoadFromRDB_oom_returns_error);
    MU_RUN_TEST(test_Compressed_LoadFromRDB_oom_returns_error);
}
