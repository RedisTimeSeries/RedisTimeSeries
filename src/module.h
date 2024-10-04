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
        RedisModuleUser *user = GetCurrentUser(ctx); \
        if (RedisModule_ACLCheckKeyPermissions(user, keyName, permissionFlags) != REDISMODULE_OK) { \
            return RTS_ReplyGeneralError(ctx, "ERR operation not permitted"); \
        } \
    }

/// Checks if the key with the key name passed is
#define CheckKeyIsAllowedToRead(ctx, keyName) CheckKeyIsAllowedByAcls(ctx, keyName, REDISMODULE_CMD_KEY_ACCESS)
#define CheckKeyIsAllowedToUpdate(ctx, keyName) CheckKeyIsAllowedByAcls(ctx, keyName, REDISMODULE_CMD_KEY_UPDATE)
#define CheckKeyIsAllowedToInsert(ctx, keyName) CheckKeyIsAllowedByAcls(ctx, keyName, REDISMODULE_CMD_KEY_INSERT)
#define CheckKeyIsAllowedToDelete(ctx, keyName) CheckKeyIsAllowedByAcls(ctx, keyName, REDISMODULE_CMD_KEY_DELETE)
#define CheckKeyIsAllowedToWrite(ctx, keyName) CheckKeyIsAllowedByAcls(ctx, keyName, REDISMODULE_CMD_KEY_DELETE | REDISMODULE_CMD_KEY_INSERT | REDISMODULE_CMD_KEY_UPDATE)

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

#endif
