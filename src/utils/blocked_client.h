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
