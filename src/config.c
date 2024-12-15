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
#include <float.h>
#include "rmutil/strings.h"
#include "rmutil/util.h"

#define DEFAULT_NUM_THREADS 3
#define NUM_THREADS_MIN 1
#define NUM_THREADS_MAX 16
#define RETENTION_POLICY_MIN 0
#define RETENTION_POLICY_MAX LLONG_MAX
#define CHUNK_SIZE_BYTES_MIN 48
#define CHUNK_SIZE_BYTES_MAX 1048576
#define IGNORE_MAX_TIME_DIFF_DEFAULT 0
#define IGNORE_MAX_TIME_DIFF_MIN 0
#define IGNORE_MAX_TIME_DIFF_MAX LLONG_MAX
#define IGNORE_MAX_VAL_DIFF_DEFAULT 0.0
#define IGNORE_MAX_VAL_DIFF_MIN 0.0
#define IGNORE_MAX_VAL_DIFF_MAX DBL_MAX
#define DEFAULT_ENCODING_STRING COMPRESSED_GORILLA_ARG_STR
#define DEFAULT_DUPLICATE_POLICY_STRING "block"

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

static RedisModuleString *getStringConfigValue(const char *name, void *privdata) {
    if (!strcasecmp("ts-compaction-policy", name)) {
        char *rulesAsString = CompactionRulesToString(TSGlobalConfig.compactionRules,
                                                      TSGlobalConfig.compactionRulesCount);
        RedisModuleString *out =
            RedisModule_CreateString(rts_staticCtx, rulesAsString, strlen(rulesAsString));
        free(rulesAsString);

        return out;
    } else if (!strcasecmp("OSS_GLOBAL_PASSWORD", name)) {
        RedisModule_Log(rts_staticCtx,
                        "warning",
                        "OSS_GLOBAL_PASSWORD is deprecated, please use the 'global-password' "
                        "password instead.");

        return RedisModule_CreateString(
            rts_staticCtx, TSGlobalConfig.password, strlen(TSGlobalConfig.password));
    } else if (!strcasecmp("global-password", name)) {
        return RedisModule_CreateString(
            rts_staticCtx, TSGlobalConfig.password, strlen(TSGlobalConfig.password));
    } else if (!strcasecmp("global-user", name)) {
        return RedisModule_CreateString(
            rts_staticCtx, TSGlobalConfig.username, strlen(TSGlobalConfig.password));
    } else if (!strcasecmp("ts-duplicate-policy", name)) {
        const char *value = DuplicatePolicyToString(TSGlobalConfig.duplicatePolicy);
        return RedisModule_CreateString(rts_staticCtx, value, strlen(value));
    } else if (!strcasecmp("ts-encoding", name)) {
        const char *value = ChunkTypeToString(TSGlobalConfig.options);
        return RedisModule_CreateString(rts_staticCtx, value, strlen(value));
    } else if (!strcasecmp("ts-ignore-max-val-diff", name)) {
        return RedisModule_CreateStringPrintf(
            rts_staticCtx, "%lf", TSGlobalConfig.ignoreMaxValDiff);
    }

    return NULL;
}

/*
 * Parses a string value and validate it is a valid double in the range
 * [min, max].
 *
 * If the value is invalid, set `err` to an error message.
 * Otherwise, set `value` to the parsed value and return it without
 * setting the error string.
 *
 * The success of the operation can be determined by checking if `err`
 * is NULL.
 */
static double stringToDouble(const char *name,
                             RedisModuleString *string,
                             const double min,
                             const double max,
                             RedisModuleString **err) {
    double value = 0.0;

    if (RedisModule_StringToDouble(string, &value) != REDISMODULE_OK) {
        *err = RedisModule_CreateStringPrintf(rts_staticCtx, "Invalid value for `%s`", name);
    } else if (value < min || value > max) {
        *err = RedisModule_CreateStringPrintf(
            rts_staticCtx,
            "Invalid value for `%s`. Value must be in the range [%f .. %f]",
            name,
            min,
            max);
    }

    return value;
}

static int setStringConfigValue(const char *name,
                                RedisModuleString *value,
                                void *data,
                                RedisModuleString **err) {
    if (!strcasecmp("ts-compaction-policy", name)) {
        // TODO: deallocate old compaction rules?
        size_t len;
        const char *policy_cstr = RedisModule_StringPtrLen(value, &len);
        if (ParseCompactionPolicy(policy_cstr,
                                  &TSGlobalConfig.compactionRules,
                                  &TSGlobalConfig.compactionRulesCount) != TRUE) {
            *err =
                RedisModule_CreateStringPrintf(NULL, "Invalid compaction policy: %s", policy_cstr);
            return REDISMODULE_ERR;
        }
    } else if (!strcasecmp("OSS_GLOBAL_PASSWORD", name)) {
        *err = RedisModule_CreateStringPrintf(NULL,
                                              "OSS_GLOBAL_PASSWORD is deprecated, please use the "
                                              "\"global-password\" password instead.");
        return REDISMODULE_ERR;
    } else if (!strcasecmp("ts-duplicate-policy", name)) {
        const DuplicatePolicy newValue = RMStringLenDuplicationPolicyToEnum(value);

        if (newValue == DP_INVALID) {
            *err = RedisModule_CreateStringPrintf(
                NULL, "Invalid duplicate policy: %s", RedisModule_StringPtrLen(value, NULL));
            return REDISMODULE_ERR;
        }

        TSGlobalConfig.duplicatePolicy = newValue;
        return REDISMODULE_OK;
    } else if (!strcasecmp("ts-ignore-max-val-diff", name)) {
        const double newValue = stringToDouble(
            "ts-ignore-max-val-diff", value, IGNORE_MAX_VAL_DIFF_MIN, IGNORE_MAX_VAL_DIFF_MAX, err);

        if (err) {
            return REDISMODULE_ERR;
        }

        TSGlobalConfig.ignoreMaxValDiff = newValue;
        return REDISMODULE_OK;
    } else if (!strcasecmp("global-password", name)) {
        size_t len = 0;
        const char *str = RedisModule_StringPtrLen(value, &len);
        TSGlobalConfig.password = strndup(str, len);
        return REDISMODULE_OK;
    } else if (!strcasecmp("global-user", name)) {
        size_t len = 0;
        const char *str = RedisModule_StringPtrLen(value, &len);
        TSGlobalConfig.username = strndup(str, len);
        return REDISMODULE_OK;
    } else if (!strcasecmp("ts-duplicate-policy", name)) {
        const DuplicatePolicy newValue = RMStringLenDuplicationPolicyToEnum(value);

        if (newValue == DP_INVALID) {
            *err = RedisModule_CreateStringPrintf(
                NULL, "Invalid duplicate policy: %s", RedisModule_StringPtrLen(value, NULL));
            return REDISMODULE_ERR;
        }

        TSGlobalConfig.duplicatePolicy = newValue;
        return REDISMODULE_OK;
    } else if (!strcasecmp("ts-encoding", name)) {
        size_t len;
        const char *encoding = RedisModule_StringPtrLen(value, &len);
        if (!strcasecmp(encoding, UNCOMPRESSED_ARG_STR)) {
            TSGlobalConfig.options &= ~SERIES_OPT_DEFAULT_COMPRESSION;
            TSGlobalConfig.options |= SERIES_OPT_UNCOMPRESSED;
        } else if (!strcasecmp(encoding, COMPRESSED_GORILLA_ARG_STR)) {
            TSGlobalConfig.options &= ~SERIES_OPT_DEFAULT_COMPRESSION;
            TSGlobalConfig.options |= SERIES_OPT_COMPRESSED_GORILLA;
        } else {
            *err = RedisModule_CreateStringPrintf(NULL, "Invalid encoding: %s", encoding);
            return REDISMODULE_ERR;
        }

        return REDISMODULE_OK;
    }

    return REDISMODULE_ERR;
}

static long long getIntegerConfigValue(const char *name, void *privdata) {
    if (!strcasecmp("ts-num-threads", name)) {
        return TSGlobalConfig.numThreads;
    } else if (!strcasecmp("ts-retention-policy", name)) {
        return TSGlobalConfig.retentionPolicy;
    } else if (!strcasecmp("ts-chunk-size-bytes", name)) {
        return TSGlobalConfig.chunkSizeBytes;
    } else if (!strcasecmp("ts-ignore-max-time-diff", name)) {
        return TSGlobalConfig.ignoreMaxTimeDiff;
    }

    return 0;
}

static int setIntegerConfigValue(const char *name,
                                 long long value,
                                 void *data,
                                 RedisModuleString **err) {
    if (!strcasecmp("ts-num-threads", name)) {
        TSGlobalConfig.numThreads = value;

        return REDISMODULE_OK;
    } else if (!strcasecmp("ts-retention-policy", name)) {
        TSGlobalConfig.retentionPolicy = value;

        return REDISMODULE_OK;
    } else if (!strcasecmp("ts-chunk-size-bytes", name)) {
        if (value % 8 != 0) {
            // Currently the gorilla algorithm implementation can only handle chunks of size
            // multiplication of 8
            *err = RedisModule_CreateStringPrintf(NULL,
                                                  "Invalid chunk size: %lld. CHUNK_SIZE value must "
                                                  "be a multiple of 8 in the range [%d .. %d]",
                                                  value,
                                                  CHUNK_SIZE_BYTES_MIN,
                                                  CHUNK_SIZE_BYTES_MAX);
            return REDISMODULE_ERR;
        }

        TSGlobalConfig.chunkSizeBytes = value;

        return REDISMODULE_OK;
    } else if (!strcasecmp("ts-ignore-max-time-diff", name)) {
        TSGlobalConfig.ignoreMaxTimeDiff = value;

        return REDISMODULE_OK;
    }

    return REDISMODULE_ERR;
}

bool RegisterConfigurationOptions(RedisModuleCtx *ctx) {
    if (RedisModule_RegisterStringConfig(ctx,
                                         "ts-compaction-policy",
                                         NULL,
                                         REDISMODULE_CONFIG_DEFAULT,
                                         getStringConfigValue,
                                         setStringConfigValue,
                                         NULL,
                                         NULL)) {
        return false;
    }

    if (RedisModule_RegisterStringConfig(ctx,
                                         "OSS_GLOBAL_PASSWORD",
                                         NULL,
                                         REDISMODULE_CONFIG_IMMUTABLE |
                                             REDISMODULE_CONFIG_SENSITIVE,
                                         getStringConfigValue,
                                         setStringConfigValue,
                                         NULL,
                                         NULL)) {
        return false;
    }

    if (RedisModule_RegisterStringConfig(ctx,
                                         "global-password",
                                         NULL,
                                         REDISMODULE_CONFIG_IMMUTABLE |
                                             REDISMODULE_CONFIG_SENSITIVE,
                                         getStringConfigValue,
                                         setStringConfigValue,
                                         NULL,
                                         NULL)) {
        return false;
    }

    if (RedisModule_RegisterStringConfig(ctx,
                                         "global-user",
                                         NULL,
                                         REDISMODULE_CONFIG_IMMUTABLE |
                                             REDISMODULE_CONFIG_SENSITIVE,
                                         getStringConfigValue,
                                         setStringConfigValue,
                                         NULL,
                                         NULL)) {
        return false;
    }

    if (RedisModule_RegisterNumericConfig(ctx,
                                          "ts-num-threads",
                                          DEFAULT_NUM_THREADS,
                                          REDISMODULE_CONFIG_IMMUTABLE,
                                          NUM_THREADS_MIN,
                                          NUM_THREADS_MAX,
                                          getIntegerConfigValue,
                                          setIntegerConfigValue,
                                          NULL,
                                          NULL)) {
        return false;
    }

    if (RedisModule_RegisterNumericConfig(ctx,
                                          "ts-retention-policy",
                                          RETENTION_TIME_DEFAULT,
                                          REDISMODULE_CONFIG_DEFAULT,
                                          RETENTION_POLICY_MIN,
                                          RETENTION_POLICY_MAX,
                                          getIntegerConfigValue,
                                          setIntegerConfigValue,
                                          NULL,
                                          NULL)) {
        return false;
    }

    if (RedisModule_RegisterStringConfig(ctx,
                                         "ts-duplicate-policy",
                                         DEFAULT_DUPLICATE_POLICY_STRING,
                                         REDISMODULE_CONFIG_DEFAULT,
                                         getStringConfigValue,
                                         setStringConfigValue,
                                         NULL,
                                         NULL)) {
        return false;
    }

    if (RedisModule_RegisterNumericConfig(ctx,
                                          "ts-chunk-size-bytes",
                                          Chunk_SIZE_BYTES_SECS,
                                          REDISMODULE_CONFIG_DEFAULT,
                                          CHUNK_SIZE_BYTES_MIN,
                                          CHUNK_SIZE_BYTES_MAX,
                                          getIntegerConfigValue,
                                          setIntegerConfigValue,
                                          NULL,
                                          NULL)) {
        return false;
    }

    if (RedisModule_RegisterStringConfig(ctx,
                                         "ts-encoding",
                                         DEFAULT_ENCODING_STRING,
                                         REDISMODULE_CONFIG_DEFAULT,
                                         getStringConfigValue,
                                         setStringConfigValue,
                                         NULL,
                                         NULL)) {
        return false;
    }

    if (RedisModule_RegisterNumericConfig(ctx,
                                          "ts-ignore-max-time-diff",
                                          IGNORE_MAX_TIME_DIFF_DEFAULT,
                                          REDISMODULE_CONFIG_DEFAULT,
                                          IGNORE_MAX_TIME_DIFF_MIN,
                                          IGNORE_MAX_TIME_DIFF_MAX,
                                          getIntegerConfigValue,
                                          setIntegerConfigValue,
                                          NULL,
                                          NULL)) {
        return false;
    }

    if (RedisModule_RegisterStringConfig(ctx,
                                         "ts-ignore-max-val-diff",
                                         "0.0",
                                         REDISMODULE_CONFIG_DEFAULT,
                                         getStringConfigValue,
                                         setStringConfigValue,
                                         NULL,
                                         NULL)) {
        return false;
    }

    return true;
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
