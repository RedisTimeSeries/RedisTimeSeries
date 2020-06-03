/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "config.h"

#include "common.h"
#include "consts.h"
#include "redismodule.h"

#include <assert.h>
#include <string.h>
#include "rmutil/strings.h"
#include "rmutil/util.h"

TSConfig TSGlobalConfig;

int ReadConfig(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    TSGlobalConfig.hasGlobalConfig = FALSE;
    TSGlobalConfig.options = SERIES_OPT_UNCOMPRESSED;

    if (argc > 1 && RMUtil_ArgIndex("COMPACTION_POLICY", argv, argc) >= 0) {
        RedisModuleString *policy;
        const char *policy_cstr;
        size_t len;

        if (RMUtil_ParseArgsAfter("COMPACTION_POLICY", argv, argc, "s", &policy) !=
            REDISMODULE_OK) {
            return TSDB_ERROR;
        }
        policy_cstr = RedisModule_StringPtrLen(policy, &len);
        if (ParseCompactionPolicy(policy_cstr,
                                  &TSGlobalConfig.compactionRules,
                                  &TSGlobalConfig.compactionRulesCount) != TRUE) {
            return TSDB_ERROR;
        }

        RedisModule_Log(ctx, "verbose", "loaded default compaction policy: %s\n\r", policy_cstr);
        TSGlobalConfig.hasGlobalConfig = TRUE;
    }

    if (argc > 1 && RMUtil_ArgIndex("RETENTION_POLICY", argv, argc) >= 0) {
        if (RMUtil_ParseArgsAfter(
                "RETENTION_POLICY", argv, argc, "l", &TSGlobalConfig.retentionPolicy) !=
            REDISMODULE_OK) {
            return TSDB_ERROR;
        }

        RedisModule_Log(ctx,
                        "verbose",
                        "loaded default retention policy: %lld \n",
                        TSGlobalConfig.retentionPolicy);
        TSGlobalConfig.hasGlobalConfig = TRUE;
    } else {
        TSGlobalConfig.retentionPolicy = RETENTION_TIME_DEFAULT;
    }

    if (argc > 1 && RMUtil_ArgIndex("MAX_SAMPLE_PER_CHUNK", argv, argc) >= 0) {
        if (RMUtil_ParseArgsAfter(
                "MAX_SAMPLE_PER_CHUNK", argv, argc, "l", &TSGlobalConfig.maxSamplesPerChunk) !=
            REDISMODULE_OK) {
            return TSDB_ERROR;
        }
    } else {
        TSGlobalConfig.maxSamplesPerChunk = SAMPLES_PER_CHUNK_DEFAULT_SECS;
    }
    RedisModule_Log(ctx,
                    "verbose",
                    "loaded default MAX_SAMPLE_PER_CHUNK policy: %lld \n",
                    TSGlobalConfig.maxSamplesPerChunk);
    return TSDB_OK;
}

RTS_RedisVersion RTS_currVersion;

RTS_RedisVersion RTS_minSupportedVersion = {
    .redisMajorVersion = 5,
    .redisMinorVersion = 0,
    .redisPatchVersion = 0,
};

int RTS_RlecMajorVersion;
int RTS_RlecMinorVersion;
int RTS_RlecPatchVersion;
int RTS_RlecBuild;

int RTS_CheckSupportedVestion()
{
    if (RTS_currVersion.redisMajorVersion < RTS_minSupportedVersion.redisMajorVersion) {
        return REDISMODULE_ERR;
    }

    if (RTS_currVersion.redisMajorVersion == RTS_minSupportedVersion.redisMajorVersion) {
        if (RTS_currVersion.redisMinorVersion < RTS_minSupportedVersion.redisMinorVersion) {
            return REDISMODULE_ERR;
        }

        if (RTS_currVersion.redisMinorVersion == RTS_minSupportedVersion.redisMinorVersion) {
            if (RTS_currVersion.redisPatchVersion < RTS_minSupportedVersion.redisPatchVersion) {
                return REDISMODULE_ERR;
            }
        }
    }

    return REDISMODULE_OK;
}

void RTS_GetRedisVersion()
{
    RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
    RedisModuleCallReply *reply = RedisModule_Call(ctx, "info", "c", "server");
    assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_STRING);
    size_t len;
    const char *replyStr = RedisModule_CallReplyStringPtr(reply, &len);

    int n = sscanf(replyStr,
                   "# Server\nredis_version:%d.%d.%d",
                   &RTS_currVersion.redisMajorVersion,
                   &RTS_currVersion.redisMinorVersion,
                   &RTS_currVersion.redisPatchVersion);
    if (n != 3) {
        RedisModule_Log(NULL, "warning", "Could not extract redis version");
    }

    RTS_RlecMajorVersion = -1;
    RTS_RlecMinorVersion = -1;
    RTS_RlecPatchVersion = -1;
    RTS_RlecBuild = -1;
    const char *enterpriseStr = strstr(replyStr, "rlec_version:");
    if (enterpriseStr) {
        n = sscanf(enterpriseStr,
                   "rlec_version:%d.%d.%d-%d",
                   &RTS_RlecMajorVersion,
                   &RTS_RlecMinorVersion,
                   &RTS_RlecPatchVersion,
                   &RTS_RlecBuild);
        if (n != 4) {
            RedisModule_Log(NULL, "warning", "Could not extract enterprise version");
        }
    }

    RedisModule_FreeCallReply(reply);
    RedisModule_FreeThreadSafeContext(ctx);
}
