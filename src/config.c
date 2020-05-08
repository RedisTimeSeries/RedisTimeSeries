/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/
#include "redismodule.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"
#include "config.h"
#include "consts.h"
#include "common.h"
#include <string.h>
#include <stdbool.h>
#include <stdarg.h>
#include <assert.h>

TSConfig TSGlobalConfig;

int ReadConfig(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    TSGlobalConfig.hasGlobalConfig = FALSE;
    TSGlobalConfig.options = SERIES_OPT_UNCOMPRESSED;

    if (argc > 1 && RMUtil_ArgIndex("COMPACTION_POLICY", argv, argc) >= 0) {
        RedisModuleString *policy;
        const char *policy_cstr;
        size_t len;

        if (RMUtil_ParseArgsAfter("COMPACTION_POLICY", argv, argc, "s", &policy) != REDISMODULE_OK) {
            return TSDB_ERROR;
        }
        policy_cstr = RedisModule_StringPtrLen(policy, &len);
        if (ParseCompactionPolicy(policy_cstr, &TSGlobalConfig.compactionRules, &TSGlobalConfig.compactionRulesCount) != TRUE ) {
            return TSDB_ERROR;
        }

        RedisModule_Log(ctx, "verbose", "loaded default compaction policy: %s\n\r", policy_cstr);
        TSGlobalConfig.hasGlobalConfig = TRUE;
    }

    if (argc > 1 && RMUtil_ArgIndex("RETENTION_POLICY", argv, argc) >= 0) {
        if (RMUtil_ParseArgsAfter("RETENTION_POLICY", argv, argc, "l", &TSGlobalConfig.retentionPolicy) != REDISMODULE_OK) {
            return TSDB_ERROR;
        }

        RedisModule_Log(ctx, "verbose", "loaded default retention policy: %lld \n", TSGlobalConfig.retentionPolicy);
        TSGlobalConfig.hasGlobalConfig = TRUE;
    } else {
        TSGlobalConfig.retentionPolicy = RETENTION_TIME_DEFAULT;
    }

    if (argc > 1 && RMUtil_ArgIndex("MAX_SAMPLE_PER_CHUNK", argv, argc) >= 0) {
        if (RMUtil_ParseArgsAfter("MAX_SAMPLE_PER_CHUNK", argv, argc, "l", &TSGlobalConfig.maxSamplesPerChunk) != REDISMODULE_OK) {
            return TSDB_ERROR;
        }
    } else {
        TSGlobalConfig.maxSamplesPerChunk = SAMPLES_PER_CHUNK_DEFAULT_SECS;
    }
    RedisModule_Log(ctx, "verbose", "loaded default MAX_SAMPLE_PER_CHUNK policy: %lld \n", TSGlobalConfig.maxSamplesPerChunk);
    return TSDB_OK;
}

RedisVersion currVersion;

RedisVersion supportedVersion = {
    .redisMajorVersion = 5,
    .redisMinorVersion = 0,
    .redisPatchVersion = 0,
};

int timeseriesRlecMajorVersion;
int timeseriesRlecMinorVersion;
int timeseriesRlecPatchVersion;
int timeseriesRlecBuild;

bool timeseriesIsCrdt;

int TimeSeriesCheckSupportedVestion() {
  if (currVersion.redisMajorVersion < supportedVersion.redisMajorVersion) {
    return REDISMODULE_ERR;
  }

  if (currVersion.redisMajorVersion == supportedVersion.redisMajorVersion) {
    if (currVersion.redisMinorVersion < supportedVersion.redisMinorVersion) {
      return REDISMODULE_ERR;
    }

    if (currVersion.redisMinorVersion == supportedVersion.redisMinorVersion) {
      if (currVersion.redisPatchVersion < supportedVersion.redisPatchVersion) {
        return REDISMODULE_ERR;
      }
    }
  }

  return REDISMODULE_OK;
}

void TimeSeriesGetRedisVersion() {
  RedisModuleCtx *ctx = RedisModule_GetThreadSafeContext(NULL);
  RedisModuleCallReply *reply = RedisModule_Call(ctx, "info", "c", "server");
  assert(RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_STRING);
  size_t len;
  const char *replyStr = RedisModule_CallReplyStringPtr(reply, &len);

  int n = sscanf(replyStr, "# Server\nredis_version:%d.%d.%d",
                 &currVersion.redisMajorVersion, &currVersion.redisMinorVersion,
                 &currVersion.redisPatchVersion);

  assert(n == 3);

  timeseriesRlecMajorVersion = -1;
  timeseriesRlecMinorVersion = -1;
  timeseriesRlecPatchVersion = -1;
  timeseriesRlecBuild = -1;
  char *enterpriseStr = strstr(replyStr, "rlec_version:");
  if (enterpriseStr) {
    n = sscanf(enterpriseStr, "rlec_version:%d.%d.%d-%d",
               &timeseriesRlecMajorVersion, &timeseriesRlecMinorVersion,
               &timeseriesRlecPatchVersion, &timeseriesRlecBuild);
    if (n != 4) {
      RedisModule_Log(NULL, "warning", "Could not extract enterprise version");
    }
  }

  RedisModule_FreeCallReply(reply);

  timeseriesIsCrdt = true;
  reply = RedisModule_Call(ctx, "CRDT.CONFIG", "cc", "GET", "active-gc");
  if (!reply || RedisModule_CallReplyType(reply) == REDISMODULE_REPLY_ERROR) {
    timeseriesIsCrdt = false;
  }

  if (reply) {
    RedisModule_FreeCallReply(reply);
  }

  RedisModule_FreeThreadSafeContext(ctx);
}