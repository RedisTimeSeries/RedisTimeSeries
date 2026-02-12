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
        *outValue;
    return true;
}

static inline bool parse_double(const RedisModuleString *valueStr, double *outValue) {
    size_t len;
    char const *const valueCStr = RedisModule_StringPtrLen(valueStr, &len);
    return parse_double_cstr(valueCStr, len, outValue);
}

/// @brief Check if the key is allowed by the ACLs for the current user.
/// @param ctx The redis module context.
/// @param keyName The name of the key to check the ACLs for.
/// @param permissionFlags The permissions to check for.
/// @return true if the key is allowed by the ACLs, false otherwise.
static inline bool CheckKeyIsAllowedByAcls(RedisModuleCtx *ctx,
                                           RedisModuleString *keyName,
                                           const int permissionFlags) {
    if (ctx != NULL) {
        RedisModuleUser *user = GetCurrentUser(ctx);

        if (!user) {
            size_t len = 0;
            const char *currentKeyStr = RedisModule_StringPtrLen(keyName, &len);
            RedisModule_Log(ctx,
                            "warning",
                            "No context user set, can't check for the ACLs for key %s",
                            currentKeyStr);

            return true;
        }

        const int allowed = RedisModule_ACLCheckKeyPermissions(user, keyName, permissionFlags);

        RedisModule_FreeModuleUser(user);

        if (allowed != REDISMODULE_OK) {
            return false;
        }
    } else {
        RedisModule_Log(
            NULL, "warning", "Can't check for the ACLs: redis module context is not set.");
    }

    return true;
}

/// @brief Check if the key is allowed by the ACLs for the current user.
/// @param ctx The redis module context.
/// @param keyName The name of the key to check the ACLs for (C String).
/// @param permissionFlags The permissions to check for.
/// @return true if the key is allowed by the ACLs, false otherwise.
static inline bool CheckKeyIsAllowedByAclsC(RedisModuleCtx *ctx,
                                            const char *keyName,
                                            const size_t keyNameLength,
                                            const int permissionFlags) {
    RedisModuleString *key = RedisModule_CreateString(ctx, keyName, keyNameLength);
    const bool isAllowed = CheckKeyIsAllowedByAcls(ctx, key, permissionFlags);

    RedisModule_FreeString(ctx, key);

    return isAllowed;
}

static inline bool CheckKeyIsAllowedToRead(RedisModuleCtx *ctx, RedisModuleString *keyName) {
    return CheckKeyIsAllowedByAcls(ctx, keyName, REDISMODULE_CMD_KEY_ACCESS);
}

static inline bool CheckKeyIsAllowedToReadC(RedisModuleCtx *ctx,
                                            const char *keyName,
                                            const size_t keyNameLength) {
    return CheckKeyIsAllowedByAclsC(ctx, keyName, keyNameLength, REDISMODULE_CMD_KEY_ACCESS);
}

static inline bool CheckKeyIsAllowedToWrite(RedisModuleCtx *ctx, RedisModuleString *keyName) {
    return CheckKeyIsAllowedByAcls(ctx, keyName, REDISMODULE_CMD_KEY_UPDATE);
}

static inline bool CheckKeyIsAllowedToWriteC(RedisModuleCtx *ctx,
                                             const char *keyName,
                                             const size_t keyNameLength) {
    return CheckKeyIsAllowedByAclsC(ctx, keyName, keyNameLength, REDISMODULE_CMD_KEY_UPDATE);
}

static inline bool CheckKeyIsAllowedToReadWrite(RedisModuleCtx *ctx, RedisModuleString *keyName) {
    return CheckKeyIsAllowedByAcls(
        ctx, keyName, REDISMODULE_CMD_KEY_ACCESS | REDISMODULE_CMD_KEY_UPDATE);
}

static inline bool CheckKeyIsAllowedToReadWriteC(RedisModuleCtx *ctx,
                                                 const char *keyName,
                                                 const size_t keyNameLength) {
    return CheckKeyIsAllowedByAclsC(
        ctx, keyName, keyNameLength, REDISMODULE_CMD_KEY_ACCESS | REDISMODULE_CMD_KEY_UPDATE);
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
    struct RedisModuleUser *user = GetCurrentUser(ctx);

    if (!user) {
        return false;
    }

    const bool ret = IsUserAllowedToReadAllTheKeys(ctx, user);

    RedisModule_FreeModuleUser(user);

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

int replyUngroupedMultiRange(RedisModuleCtx *ctx, RedisModuleDict *result, const MRangeArgs *args);

extern int persistence_in_progress;

#endif // MODULE_H
