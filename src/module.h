/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
 */
#ifndef MODULE_H
#define MODULE_H

#include "common.h"
#include "tsdb.h"

#include "RedisModulesSDK/redismodule.h"

#define CheckKeyIsAllowedByAcls(ctx, keyName, permissionFlags) \
    { \
        if (ctx != NULL) { \
            RedisModuleUser *user = GetCurrentUser(ctx); \
            \
            if (!user) { \
                size_t len = 0; \
                const char *currentKeyStr = RedisModule_StringPtrLen(keyName, &len); \
                RedisModule_Log(ctx, \
                                "warning", \
                                "No context user set, can't check for the ACLs for key %s", \
                                currentKeyStr); \
            } else if (RedisModule_ACLCheckKeyPermissions(user, keyName, permissionFlags) != REDISMODULE_OK) { \
                return RTS_ReplyGeneralError(ctx, "ERR operation not permitted"); \
            } \
        } else { \
            fprintf(stderr, "Cannot check for the ACLs: redis module context is not set."); \
        } \
    }

/// Checks if the key with the key name passed is
#define CheckKeyIsAllowedToRead(ctx, keyName) CheckKeyIsAllowedByAcls(ctx, keyName, REDISMODULE_CMD_KEY_ACCESS | REDISMODULE_CMD_KEY_RO)
#define CheckKeyIsAllowedToUpdate(ctx, keyName) CheckKeyIsAllowedByAcls(ctx, keyName, REDISMODULE_CMD_KEY_UPDATE | REDISMODULE_CMD_KEY_RW | REDISMODULE_CMD_KEY_OW)
#define CheckKeyIsAllowedToInsert(ctx, keyName) CheckKeyIsAllowedByAcls(ctx, keyName, REDISMODULE_CMD_KEY_INSERT | REDISMODULE_CMD_KEY_RW | REDISMODULE_CMD_KEY_OW)
#define CheckKeyIsAllowedToDelete(ctx, keyName) CheckKeyIsAllowedByAcls(ctx, keyName, REDISMODULE_CMD_KEY_DELETE | REDISMODULE_CMD_KEY_RM)
#define CheckKeyIsAllowedToWrite(ctx, keyName) CheckKeyIsAllowedByAcls(ctx, keyName, REDISMODULE_CMD_KEY_DELETE | REDISMODULE_CMD_KEY_INSERT | REDISMODULE_CMD_KEY_UPDATE | REDISMODULE_CMD_KEY_RW | REDISMODULE_CMD_KEY_OW | REDISMODULE_CMD_KEY_RM)

extern RedisModuleType *SeriesType;
extern RedisModuleCtx *rts_staticCtx;

// Create a new TS key, if key is NULL the function will open the key, the user must call to
// RedisModule_CloseKey The function assumes the key doesn't exists
int CreateTsKey(RedisModuleCtx *ctx,
                RedisModuleString *keyName,
                CreateCtx *cCtx,
                Series **series,
                RedisModuleKey **key);

bool CheckVersionForBlockedClientMeasureTime();

extern int persistence_in_progress;

#endif // MODULE_H
