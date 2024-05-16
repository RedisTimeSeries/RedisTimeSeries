/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
 */
#include "config.h"

#include "consts.h"
#include "module.h"
#include "query_language.h"
#include "RedisModulesSDK/redismodule.h"

#include <assert.h>
#include <string.h>
#include "rmutil/strings.h"
#include "rmutil/util.h"

TSConfig TSGlobalConfig;

int ParseDuplicatePolicy(RedisModuleCtx *ctx,
                         RedisModuleString **argv,
                         int argc,
                         const char *arg_prefix,
                         DuplicatePolicy *policy);

const char *ChunkTypeToString(int options) {
    if (options & SERIES_OPT_UNCOMPRESSED) {
        return UNCOMPRESSED_ARG_STR;
    }
    if (options & SERIES_OPT_COMPRESSED_GORILLA) {
        return COMPRESSED_GORILLA_ARG_STR;
    }
    return "invalid";
}

int ReadConfig(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    TSGlobalConfig.hasGlobalConfig = FALSE;
    TSGlobalConfig.options = 0;
    // default serie encoding
    TSGlobalConfig.options |= SERIES_OPT_DEFAULT_COMPRESSION;

    if (argc > 1 && RMUtil_ArgIndex("COMPACTION_POLICY", argv, argc) >= 0) {
        RedisModuleString *policy;
        const char *policy_cstr;
        size_t len;

        if (RMUtil_ParseArgsAfter("COMPACTION_POLICY", argv, argc, "s", &policy) !=
            REDISMODULE_OK) {
            RedisModule_Log(ctx, "warning", "Unable to parse argument after COMPACTION_POLICY");
            return TSDB_ERROR;
        }
        policy_cstr = RedisModule_StringPtrLen(policy, &len);
        if (ParseCompactionPolicy(policy_cstr,
                                  &TSGlobalConfig.compactionRules,
                                  &TSGlobalConfig.compactionRulesCount) != TRUE) {
            RedisModule_Log(ctx, "warning", "Unable to parse argument after COMPACTION_POLICY");
            return TSDB_ERROR;
        }

        RedisModule_Log(ctx, "notice", "loaded default compaction policy: %s", policy_cstr);
        TSGlobalConfig.hasGlobalConfig = TRUE;
    }

    if (argc > 1 && RMUtil_ArgIndex("OSS_GLOBAL_PASSWORD", argv, argc) >= 0) {
        RedisModuleString *password;
        size_t len;
        if (RMUtil_ParseArgsAfter("OSS_GLOBAL_PASSWORD", argv, argc, "s", &password) !=
            REDISMODULE_OK) {
            RedisModule_Log(ctx, "warning", "Unable to parse argument after OSS_GLOBAL_PASSWORD");
            return TSDB_ERROR;
        }

        TSGlobalConfig.password = (char *)RedisModule_StringPtrLen(password, &len);
        RedisModule_Log(ctx, "notice", "loaded tls password");
        TSGlobalConfig.hasGlobalConfig = TRUE;
    } else {
        TSGlobalConfig.password = NULL;
    }

    if (argc > 1 && RMUtil_ArgIndex("RETENTION_POLICY", argv, argc) >= 0) {
        if (RMUtil_ParseArgsAfter(
                "RETENTION_POLICY", argv, argc, "l", &TSGlobalConfig.retentionPolicy) !=
            REDISMODULE_OK) {
            RedisModule_Log(ctx, "warning", "Unable to parse argument after RETENTION_POLICY");
            return TSDB_ERROR;
        }

        RedisModule_Log(
            ctx, "notice", "loaded default retention policy: %lld", TSGlobalConfig.retentionPolicy);
        TSGlobalConfig.hasGlobalConfig = TRUE;
    } else {
        TSGlobalConfig.retentionPolicy = RETENTION_TIME_DEFAULT;
    }

    if (!ValidateChunkSize(ctx, Chunk_SIZE_BYTES_SECS)) {
        return TSDB_ERROR;
    }
    TSGlobalConfig.chunkSizeBytes = Chunk_SIZE_BYTES_SECS;
    if (ParseChunkSize(ctx, argv, argc, "CHUNK_SIZE_BYTES", &TSGlobalConfig.chunkSizeBytes) !=
        REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "Unable to parse argument after CHUNK_SIZE_BYTES");
        return TSDB_ERROR;
    }
    RedisModule_Log(ctx,
                    "notice",
                    "loaded default CHUNK_SIZE_BYTES policy: %lld",
                    TSGlobalConfig.chunkSizeBytes);

    TSGlobalConfig.duplicatePolicy = DEFAULT_DUPLICATE_POLICY;
    if (ParseDuplicatePolicy(
            ctx, argv, argc, DUPLICATE_POLICY_ARG, &TSGlobalConfig.duplicatePolicy) != TSDB_OK) {
        RedisModule_Log(ctx, "warning", "Unable to parse argument after DUPLICATE_POLICY");
        return TSDB_ERROR;
    }
    RedisModule_Log(ctx,
                    "notice",
                    "loaded server DUPLICATE_POLICY: %s",
                    DuplicatePolicyToString(TSGlobalConfig.duplicatePolicy));

    if (argc > 1 && (RMUtil_ArgIndex("ENCODING", argv, argc) >= 0 ||
                     RMUtil_ArgIndex("CHUNK_TYPE", argv, argc) >= 0)) {
        if (RMUtil_ArgIndex("CHUNK_TYPE", argv, argc) >= 0) {
            RedisModule_Log(
                ctx,
                "warning",
                "CHUNK_TYPE configuration was deprecated and will be removed in future "
                "versions of RedisTimeSeries. Please use ENCODING configuration instead.");
        }
        RedisModuleString *chunk_type;
        size_t len;
        const char *chunk_type_cstr;
        if (RMUtil_ArgIndex("CHUNK_TYPE", argv, argc) >= 0 &&
            RMUtil_ParseArgsAfter("CHUNK_TYPE", argv, argc, "s", &chunk_type) != REDISMODULE_OK) {
            RedisModule_Log(ctx, "warning", "Unable to parse argument after CHUNK_TYPE");
            return TSDB_ERROR;
        }
        if (RMUtil_ArgIndex("ENCODING", argv, argc) >= 0 &&
            RMUtil_ParseArgsAfter("ENCODING", argv, argc, "s", &chunk_type) != REDISMODULE_OK) {
            RedisModule_Log(ctx, "warning", "Unable to parse argument after ENCODING");
            return TSDB_ERROR;
        }
        RMUtil_StringToLower(chunk_type);
        chunk_type_cstr = RedisModule_StringPtrLen(chunk_type, &len);

        if (strncmp(chunk_type_cstr, COMPRESSED_GORILLA_ARG_STR, len) == 0) {
            TSGlobalConfig.options &= ~SERIES_OPT_DEFAULT_COMPRESSION;
            TSGlobalConfig.options |= SERIES_OPT_COMPRESSED_GORILLA;
        } else if (strncmp(chunk_type_cstr, UNCOMPRESSED_ARG_STR, len) == 0) {
            TSGlobalConfig.options &= ~SERIES_OPT_DEFAULT_COMPRESSION;
            TSGlobalConfig.options |= SERIES_OPT_UNCOMPRESSED;
        } else {
            RedisModule_Log(ctx, "warning", "unknown series ENCODING type: %s\n", chunk_type_cstr);
            return TSDB_ERROR;
        }
    }
    if (argc > 1 && RMUtil_ArgIndex("NUM_THREADS", argv, argc) >= 0) {
        if (RMUtil_ParseArgsAfter("NUM_THREADS", argv, argc, "l", &TSGlobalConfig.numThreads) !=
            REDISMODULE_OK) {
            RedisModule_Log(ctx, "warning", "Unable to parse argument after COMPACTION_POLICY");
            return TSDB_ERROR;
        }
    } else {
        TSGlobalConfig.numThreads = 3;
    }
    TSGlobalConfig.forceSaveCrossRef = false;
    if (argc > 1 && RMUtil_ArgIndex("DEUBG_FORCE_RULE_DUMP", argv, argc) >= 0) {
        RedisModuleString *forceSaveCrossRef;
        if (RMUtil_ParseArgsAfter("DEUBG_FORCE_RULE_DUMP", argv, argc, "s", &forceSaveCrossRef) !=
            REDISMODULE_OK) {
            RedisModule_Log(ctx, "warning", "Unable to parse argument after DEUBG_FORCE_RULE_DUMP");
            return TSDB_ERROR;
        }
        size_t forceSaveCrossRef_len;
        const char *forceSaveCrossRef_cstr =
            RedisModule_StringPtrLen(forceSaveCrossRef, &forceSaveCrossRef_len);
        if (!strcasecmp(forceSaveCrossRef_cstr, "enable")) {
            TSGlobalConfig.forceSaveCrossRef = true;
        } else if (!strcasecmp(forceSaveCrossRef_cstr, "disable")) {
            TSGlobalConfig.forceSaveCrossRef = false;
        }
    }
    TSGlobalConfig.dontAssertOnFailiure = false;
    if (argc > 1 && RMUtil_ArgIndex("DONT_ASSERT_ON_FAILIURE", argv, argc) >= 0) {
        RedisModuleString *dontAssertOnFailiure;
        if (RMUtil_ParseArgsAfter(
                "DONT_ASSERT_ON_FAILIURE", argv, argc, "s", &dontAssertOnFailiure) !=
            REDISMODULE_OK) {
            RedisModule_Log(
                ctx, "warning", "Unable to parse argument after DONT_ASSERT_ON_FAILIURE");
            return TSDB_ERROR;
        }
        size_t dontAssertOnFailiure_len;
        const char *dontAssertOnFailiure_cstr =
            RedisModule_StringPtrLen(dontAssertOnFailiure, &dontAssertOnFailiure_len);
        if (!strcasecmp(dontAssertOnFailiure_cstr, "enable")) {
            TSGlobalConfig.dontAssertOnFailiure = true;
        } else if (!strcasecmp(dontAssertOnFailiure_cstr, "disable")) {
            TSGlobalConfig.dontAssertOnFailiure = false;
        }

        extern bool _dontAssertOnFailiure;
        _dontAssertOnFailiure = TSGlobalConfig.dontAssertOnFailiure;
    }

    TSGlobalConfig.ignoreMaxTimeDiff = 0;
    if (argc > 1 && RMUtil_ArgIndex("IGNORE_MAX_TIME_DIFF", argv, argc) >= 0) {
        long long ignoreMaxTimeDiff = 0;
        if (RMUtil_ParseArgsAfter("IGNORE_MAX_TIME_DIFF", argv, argc, "l", &ignoreMaxTimeDiff) !=
            REDISMODULE_OK) {
            RedisModule_Log(ctx, "warning", "Unable to parse argument after IGNORE_MAX_TIME_DIFF");
            return TSDB_ERROR;
        }
        if (ignoreMaxTimeDiff < 0) {
            RedisModule_Log(ctx, "warning", "IGNORE_MAX_TIME_DIFF config cannot be negative!");
            return TSDB_ERROR;
        }
        TSGlobalConfig.ignoreMaxTimeDiff = ignoreMaxTimeDiff;
    }
    RedisModule_Log(ctx,
                    "notice",
                    "loaded default IGNORE_MAX_TIME_DIFF: %lld",
                    TSGlobalConfig.ignoreMaxTimeDiff);

    TSGlobalConfig.ignoreMaxValDiff = 0.0;
    if (argc > 1 && RMUtil_ArgIndex("IGNORE_MAX_VAL_DIFF", argv, argc) >= 0) {
        double ignoreMaxValDiff = 0;
        if (RMUtil_ParseArgsAfter("IGNORE_MAX_VAL_DIFF", argv, argc, "d", &ignoreMaxValDiff) !=
            REDISMODULE_OK) {
            RedisModule_Log(ctx, "warning", "Unable to parse argument after IGNORE_MAX_VAL_DIFF");
            return TSDB_ERROR;
        }
        if (ignoreMaxValDiff < 0) {
            RedisModule_Log(ctx, "warning", "IGNORE_MAX_VAL_DIFF config cannot be negative!");
            return TSDB_ERROR;
        }
        TSGlobalConfig.ignoreMaxValDiff = ignoreMaxValDiff;
    }
    RedisModule_Log(
        ctx, "notice", "loaded default IGNORE_MAX_VAL_DIFF: %f", TSGlobalConfig.ignoreMaxValDiff);

    RedisModule_Log(ctx,
                    "notice",
                    "Setting default series ENCODING to: %s",
                    ChunkTypeToString(TSGlobalConfig.options));
    return TSDB_OK;
}

RTS_RedisVersion RTS_currVersion;

RTS_RedisVersion RTS_minSupportedVersion = {
    .redisMajorVersion = 6,
    .redisMinorVersion = 0,
    .redisPatchVersion = 0,
};

int RTS_RlecMajorVersion;
int RTS_RlecMinorVersion;
int RTS_RlecPatchVersion;
int RTS_RlecBuild;

int RTS_CheckSupportedVestion() {
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

void RTS_GetRedisVersion() {
    RedisModuleCallReply *reply = RedisModule_Call(rts_staticCtx, "info", "c", "server");
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
}
