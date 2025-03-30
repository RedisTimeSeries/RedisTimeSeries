#pragma once

struct RedisModuleUser;
struct RedisModuleCtx;
struct RedisModuleString;
struct RedisModuleUser *(*RedisModule_GetModuleUserFromUserName)(struct RedisModuleString *name);
struct RedisModuleString *(*RedisModule_GetCurrentUserName)(struct RedisModuleCtx *ctx);
void (*RedisModule_FreeString)(struct RedisModuleCtx *ctx, struct RedisModuleString *str);
int (*RedisModule_ACLCheckKeyPrefixPermissions)(struct RedisModuleUser *user,
                                                struct RedisModuleString *prefix,
                                                int flags);

#include <stdlib.h>
#include "RedisModulesSDK/redismodule.h"

#define RTS_ERR "ERR"
#define RTS_NOPERM "NOPERM"
#define RTS_ReplyError(ctx, err_type, msg) RedisModule_ReplyWithError(ctx, err_type " " msg)
#define RTS_ReplyGeneralError(ctx, msg) RTS_ReplyError(ctx, RTS_ERR, msg)
#define RTS_ReplyPermissionError(ctx, msg) RTS_ReplyError(ctx, RTS_NOPERM, msg)
#define RTS_ReplyKeyPermissionsError(ctx)                                                          \
    RTS_ReplyPermissionError(ctx,                                                                  \
                             "TSDB: current user doesn't have read permission to one or more "     \
                             "keys that match the specified filter")

// Returns the current user of the context.
static inline struct RedisModuleUser *GetCurrentUser(struct RedisModuleCtx *ctx) {
    struct RedisModuleString *username = RedisModule_GetCurrentUserName(ctx);

    if (!username) {
        return NULL;
    }

    struct RedisModuleUser *user = RedisModule_GetModuleUserFromUserName(username);
    RedisModule_FreeString(ctx, username);

    return user;
}

enum {
    DefragStatus_Finished = 0,
    DefragStatus_Paused = 1,
};

static inline void *defragPtr(RedisModuleDefragCtx *ctx, void *ptr) {
    if (ptr == NULL) {
        return NULL;
    }
    return RedisModule_DefragAlloc(ctx, ptr) ?: ptr;
}
static inline RedisModuleString *defragString(RedisModuleDefragCtx *ctx, RedisModuleString *str) {
    if (str == NULL) {
        return NULL;
    }
    return RedisModule_DefragRedisModuleString(ctx, str) ?: str;
}
static inline RedisModuleDict *defragDict(RedisModuleDefragCtx *ctx, RedisModuleDict *dict, RedisModuleDefragDictValueCallback valueCB, RedisModuleString **seekTo) {
    if (dict == NULL) {
        return NULL;
    }
    return RedisModule_DefragRedisModuleDict(ctx, dict, valueCB, seekTo) ?: dict;
}

#if (defined(DEBUG) || defined(_DEBUG)) && !defined(NDEBUG)
#include "readies/cetara/diag/gdb.h"
#endif
