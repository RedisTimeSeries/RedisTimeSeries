/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef MODULE_H
#define MODULE_H

#include <stdbool.h>
#include <math.h>

#include "tsdb.h"

#include "RedisModulesSDK/redismodule.h"

#include "fast_double_parser_c/fast_double_parser_c.h"

/* A RedisModuleCtx carries the connection's authenticated "client user" plus an
 * optional "attached user" set by the module: SetContextUser attaches one,
 * GetContextUser reads it back (borrowed), GetModuleUserFromUserName allocates
 * one by name (owned), GetUserUsername returns a user's name. GetUserFromContext()
 * below picks the attached user if present, else allocates the client user. */
#define API_USER_CONTEXT_SUPPORTED                                                                 \
    (RedisModule_SetContextUser && RedisModule_GetContextUser &&                                   \
     RedisModule_GetModuleUserFromUserName && RedisModule_GetUserUsername)

/* Thin wrapper around RedisModuleUser*. `is_owned` tells the caller whether the user must be
 * released with RedisModule_FreeModuleUser when done:
 *   - is_owned == true  : the user was freshly allocated for this call; caller must release.
 *   - is_owned == false : borrowed reference (e.g. attached to the ctx); caller MUST NOT release.
 * Always release via FreeUser(), which makes the decision based on the flag. */
typedef struct {
    RedisModuleUser *user;
    bool is_owned;
} User_Ctx_t;

User_Ctx_t GetUserFromContext(RedisModuleCtx *ctx);

/* Release a User_Ctx_t. Frees the underlying RedisModuleUser iff the wrapper actually owns it
 * (i.e. user != NULL && is_owned). Always clears the wrapper to a safe empty state afterwards,
 * making double-FreeUser calls a no-op. */
static inline void FreeUser(User_Ctx_t *userCtx) {
    if (userCtx == NULL)
        return;
    if (userCtx->user != NULL && userCtx->is_owned) {
        RedisModule_FreeModuleUser(userCtx->user);
    }
    userCtx->user = NULL;
    userCtx->is_owned = false;
}

static inline bool is_nan_string(const char *str, size_t len) {
    if (len == 3 && strncasecmp(str, "nan", 3) == 0) {
        return true;
    }
    if (len == 4 && (strncasecmp(str, "-nan", 4) == 0 || strncasecmp(str, "+nan", 4) == 0)) {
        return true;
    }
    return false;
}

static inline bool parse_double_cstr(const char *str, size_t len, double *outValue) {
    double value;
    char const *const endptr = fast_double_parser_c_parse_number(str, &value);
    if (unlikely(endptr > str + len)) {
        // Unlikely, but could be that str[len] is a digit (or a dot)
        // In such cases we copy, null-terminate and try again
        char buf[1 + len];
        strncpy(buf, str, len);
        buf[len] = '\0';
        return parse_double_cstr(buf, len, outValue);
    }
    if (unlikely(endptr == NULL || endptr - str != len)) {
        if (likely(is_nan_string(str, len)))
            value = NAN;
        else
            return false;
    }
    if (likely(outValue != NULL))
        *outValue = value;
    return true;
}

static inline bool parse_double(const RedisModuleString *valueStr, double *outValue) {
    size_t len;
    char const *const valueCStr = RedisModule_StringPtrLen(valueStr, &len);
    return parse_double_cstr(valueCStr, len, outValue);
}

/// @brief Check if the key is allowed by the ACLs for the given user.
/// @param user The user to check against. Callers should hoist
///             GetUserFromContext() once per request and pass userCtx.user
///             here so multi-key loops don't pay per-key alloc/free.
///             user==NULL means "no user resolvable" and default-allows.
/// @param keyName The name of the key to check the ACLs for.
/// @param permissionFlags The permissions to check for.
/// @return true if the key is allowed by the ACLs, false otherwise.
static inline bool CheckKeyIsAllowedByAcls(RedisModuleUser *user,
                                           RedisModuleString *keyName,
                                           const int permissionFlags) {
    if (user == NULL) {
        return true;
    }
    return RedisModule_ACLCheckKeyPermissions(user, keyName, permissionFlags) == REDISMODULE_OK;
}

/// @brief Same as CheckKeyIsAllowedByAcls but with a C-string key. The
///        RedisModuleString is allocated/freed internally; the user is
///        taken as-is so loops can hoist it once.
static inline bool CheckKeyIsAllowedByAclsC(RedisModuleCtx *ctx,
                                            RedisModuleUser *user,
                                            const char *keyName,
                                            const size_t keyNameLength,
                                            const int permissionFlags) {
    if (user == NULL) {
        return true;
    }
    RedisModuleString *key = RedisModule_CreateString(ctx, keyName, keyNameLength);
    const bool isAllowed = CheckKeyIsAllowedByAcls(user, key, permissionFlags);
    RedisModule_FreeString(ctx, key);
    return isAllowed;
}

static inline bool CheckKeyIsAllowedToRead(RedisModuleUser *user, RedisModuleString *keyName) {
    return CheckKeyIsAllowedByAcls(user, keyName, REDISMODULE_CMD_KEY_ACCESS);
}

static inline bool CheckKeyIsAllowedToReadC(RedisModuleCtx *ctx,
                                            RedisModuleUser *user,
                                            const char *keyName,
                                            const size_t keyNameLength) {
    return CheckKeyIsAllowedByAclsC(ctx, user, keyName, keyNameLength, REDISMODULE_CMD_KEY_ACCESS);
}

static inline bool CheckKeyIsAllowedToWrite(RedisModuleUser *user, RedisModuleString *keyName) {
    return CheckKeyIsAllowedByAcls(user, keyName, REDISMODULE_CMD_KEY_UPDATE);
}

static inline bool CheckKeyIsAllowedToWriteC(RedisModuleCtx *ctx,
                                             RedisModuleUser *user,
                                             const char *keyName,
                                             const size_t keyNameLength) {
    return CheckKeyIsAllowedByAclsC(ctx, user, keyName, keyNameLength, REDISMODULE_CMD_KEY_UPDATE);
}

static inline bool CheckKeyIsAllowedToReadWrite(RedisModuleUser *user,
                                                RedisModuleString *keyName) {
    return CheckKeyIsAllowedByAcls(
        user, keyName, REDISMODULE_CMD_KEY_ACCESS | REDISMODULE_CMD_KEY_UPDATE);
}

static inline bool CheckKeyIsAllowedToReadWriteC(RedisModuleCtx *ctx,
                                                 RedisModuleUser *user,
                                                 const char *keyName,
                                                 const size_t keyNameLength) {
    return CheckKeyIsAllowedByAclsC(
        ctx, user, keyName, keyNameLength, REDISMODULE_CMD_KEY_ACCESS | REDISMODULE_CMD_KEY_UPDATE);
}

// Returns true if the user is allowed to read all the keys.
static inline bool IsUserAllowedToReadAllTheKeys(struct RedisModuleCtx *ctx,
                                                 struct RedisModuleUser *user) {
    struct RedisModuleString *prefix = RedisModule_CreateString(ctx, "*", 1);

    if (!prefix) {
        return false;
    }

    const bool ret = RedisModule_ACLCheckKeyPermissions(user, prefix, REDISMODULE_CMD_KEY_ACCESS) ==
                     REDISMODULE_OK;

    RedisModule_FreeString(ctx, prefix);

    return ret;
}

static inline bool IsCurrentUserAllowedToReadAllTheKeys(struct RedisModuleCtx *ctx) {
    User_Ctx_t userCtx = GetUserFromContext(ctx);

    if (!userCtx.user) {
        return false;
    }

    const bool ret = IsUserAllowedToReadAllTheKeys(ctx, userCtx.user);

    FreeUser(&userCtx);

    return ret;
}

extern RedisModuleType *SeriesType;
extern RedisModuleCtx *rts_staticCtx;

// Create a new TS key, if key is NULL the function will open the key, the user must call to
// RedisModule_CloseKey The function assumes the key doesn't exists
int CreateTsKey(RedisModuleCtx *ctx,
                RedisModuleString *keyName,
                const CreateCtx *cCtx,
                Series **series,
                RedisModuleKey **key);

bool CheckVersionForBlockedClientMeasureTime();

GetSeriesResult CheckDictSeriesPermissions(RedisModuleCtx *ctx,
                                           RedisModuleDict *dict,
                                           const GetSeriesFlags flags);

int replyUngroupedMultiRange(RedisModuleCtx *ctx, RedisModuleDict *result, const MRangeArgs *args);

extern int persistence_in_progress;

#endif // MODULE_H
