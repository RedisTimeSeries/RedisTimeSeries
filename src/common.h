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
#include <string.h>
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

static inline int stringEqualsC(const RedisModuleString *s1, const char *s2) {
    size_t len;
    const char *s1Str = RedisModule_StringPtrLen(s1, &len);
    return len == strlen(s2) && strncmp(s1Str, s2, len) == 0;
}

enum
{
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
static inline RedisModuleDict *defragDict(RedisModuleDefragCtx *ctx,
                                          RedisModuleDict *dict,
                                          RedisModuleDefragDictValueCallback valueCB,
                                          RedisModuleString **seekTo) {
    if (dict == NULL) {
        return NULL;
    }
    return RedisModule_DefragRedisModuleDict(ctx, dict, valueCB, seekTo) ?: dict;
}

int NotifyCallback(RedisModuleCtx *ctx, int type, const char *event, RedisModuleString *key);

static inline void lazyModuleInitialize(RedisModuleCtx *ctx) {
    static int lazy_initialized = 0;
    if (!lazy_initialized) {
        RedisModule_SubscribeToKeyspaceEvents(
            ctx,
            REDISMODULE_NOTIFY_GENERIC | REDISMODULE_NOTIFY_SET | REDISMODULE_NOTIFY_STRING |
                REDISMODULE_NOTIFY_EVICTED | REDISMODULE_NOTIFY_EXPIRED |
                REDISMODULE_NOTIFY_LOADED | REDISMODULE_NOTIFY_TRIMMED,
            NotifyCallback);
        lazy_initialized = 1;
    }
}

#if (defined(DEBUG) || defined(_DEBUG)) && !defined(NDEBUG)
#include "readies/cetara/diag/gdb.h"
#endif
