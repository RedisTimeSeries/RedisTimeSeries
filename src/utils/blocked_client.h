/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "RedisModulesSDK/redismodule.h"

// create blocked client and report start time
RedisModuleBlockedClient *RTS_BlockClient(RedisModuleCtx *ctx,
                                          void (*free_privdata)(RedisModuleCtx *, void *));

// unblock blocked client and report end time
void RTS_UnblockClient(RedisModuleBlockedClient *bc, void *privdata);

// Block the client on a single key. The reply_cb is invoked on every
// RedisModule_SignalKeyAsReady on `key` (and once at setup); the timeout_cb
// fires after `timeout_ms`. `privdata` is owned by the blocked client and
// freed via `free_privdata` once the client is unblocked.
RedisModuleBlockedClient *RTS_BlockClientOnKey(RedisModuleCtx *ctx,
                                               RedisModuleCmdFunc reply_callback,
                                               RedisModuleCmdFunc timeout_callback,
                                               void (*free_privdata)(RedisModuleCtx *, void *),
                                               long long timeout_ms,
                                               RedisModuleString *key,
                                               void *privdata);
