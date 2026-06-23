/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include "query_language.h" // MRangeArgs, MGetArgs (+ RedisModule types)

// Each takes ownership of the result dict and the contents of `args` (moved
// into a heap context that survives the BlockClient hop).
int MRange_ReplyAsync(RedisModuleCtx *ctx, RedisModuleDict *resultSeries, MRangeArgs *args);
int MGet_ReplyAsync(RedisModuleCtx *ctx, RedisModuleDict *result, MGetArgs *args);

// Emit one MGET reply element. Defined in module.c alongside the other reply
// helpers; shared by the sync mget path (module.c) and the async batch path
// (prefetch_commands.c). `async_user_name` is non-NULL only on the async path,
// where the thread-safe reply ctx has no current user for ACL resolution.
void mget_emit_for_key(RedisModuleCtx *ctx,
                       RedisModuleString *key,
                       const MGetArgs *args,
                       long long *replylen,
                       RedisModuleString *async_user_name);
