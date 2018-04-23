#include "redismodule.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"
#include "config.h"
#include "consts.h"

TSConfig TSGlobalConfig;

int ReadConfig(RedisModuleString **argv, int argc) {
    TSGlobalConfig.hasGlobalConfig = FALSE;

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

        printf("loaded default compation policy: %s\n\r", policy_cstr);
        TSGlobalConfig.hasGlobalConfig = TRUE;
    }

    if (argc > 1 && RMUtil_ArgIndex("RETENTION_POLICY", argv, argc) >= 0) {
        if (RMUtil_ParseArgsAfter("RETENTION_POLICY", argv, argc, "l", &TSGlobalConfig.retentionPolicy) != REDISMODULE_OK) {
            return TSDB_ERROR;
        }

        printf("loaded default retention policy: %lld \n", TSGlobalConfig.retentionPolicy);
    }

    if (argc > 1 && RMUtil_ArgIndex("MAX_SAMPLE_PER_CHUNK", argv, argc) >= 0) {
        if (RMUtil_ParseArgsAfter("MAX_SAMPLE_PER_CHUNK", argv, argc, "l", &TSGlobalConfig.maxSamplesPerChunk) != REDISMODULE_OK) {
            return TSDB_ERROR;
        }

        printf("loaded default MAX_SAMPLE_PER_CHUNK policy: %lld \n", TSGlobalConfig.maxSamplesPerChunk);
    } else {
        TSGlobalConfig.maxSamplesPerChunk = SAMPLES_PER_CHUNK_DEFAULT_SECS;
    }
    return TSDB_OK;
}