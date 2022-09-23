/*
 * Copyright 2018-2022 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
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
