
#pragma once

#include "redismodule.h"

#define RTS_ERR "ERR"
#define RTS_ReplyError(ctx, err_type, msg) RedisModule_ReplyWithError(ctx, err_type " " msg);
#define RTS_ReplyGeneralError(ctx, msg) RTS_ReplyError(ctx, RTS_ERR, msg);

// Rerturns the current user of the context.
static inline RedisModuleUser* GetCurrentUser(RedisModuleCtx *ctx) {
    RedisModuleString *username = RedisModule_GetCurrentUserName(ctx);

    if (!username) {
        return NULL;
    }

    return RedisModule_GetModuleUserFromUserName(username);
}

#if (defined(DEBUG) || defined(_DEBUG)) && !defined(NDEBUG)
#include "readies/cetara/diag/gdb.h"
#endif
