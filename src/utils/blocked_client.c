/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "blocked_client.h"
#include "../module.h"

#include <assert.h>

RedisModuleBlockedClient *RTS_BlockClient(RedisModuleCtx *ctx,
                                          void (*free_privdata)(RedisModuleCtx *, void *)) {
    assert(ctx != NULL);

    RedisModuleBlockedClient *bc = RedisModule_BlockClient(ctx, NULL, NULL, free_privdata, 0);
    if (CheckVersionForBlockedClientMeasureTime()) {
        // report block client start time
        RedisModule_BlockedClientMeasureTimeStart(bc);
    }
    return bc;
}

void RTS_UnblockClient(RedisModuleBlockedClient *bc, void *privdata) {
    assert(bc != NULL);

    if (CheckVersionForBlockedClientMeasureTime()) {
        // report block client end time
        RedisModule_BlockedClientMeasureTimeEnd(bc);
    }
    RedisModule_UnblockClient(bc, privdata);
}
