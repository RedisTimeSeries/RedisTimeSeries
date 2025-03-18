/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
 */
#include "config.h"

#include "consts.h"
#include "module.h"
#include "parse_policies.h"
#include "query_language.h"
#include "RedisModulesSDK/redismodule.h"

#include <assert.h>
#include <string.h>
#include <float.h>
#include "rmutil/strings.h"
#include "rmutil/util.h"

/*
 * Logs that a deprecated configuration option was used.
 */
#define LOG_DEPRECATED_OPTION(deprecatedName, modernName, show)                                    \
    if (show) {                                                                                    \
        if (modernName) {                                                                          \
            RedisModule_Log(rts_staticCtx,                                                         \
                            "warning",                                                             \
                            "%s is deprecated, please use the '%s' instead",                       \
                            deprecatedName,                                                        \
                            modernName);                                                           \
        } else {                                                                                   \
            RedisModule_Log(rts_staticCtx, "warning", "%s is deprecated.", deprecatedName);        \
        }                                                                                          \
    }

#define LOG_DEPRECATED_OPTION_ALWAYS(deprecatedName, modernName)                                   \
    LOG_DEPRECATED_OPTION(deprecatedName, modernName, true)

TSConfig TSGlobalConfig;

static RedisModuleString *getConfigStringCache = NULL;

void InitConfig(void) {
    TSGlobalConfig.options = SERIES_OPT_DEFAULT_COMPRESSION;
    TSGlobalConfig.password = NULL;

    if (getConfigStringCache) {
        RedisModule_FreeString(rts_staticCtx, getConfigStringCache);
        getConfigStringCache = NULL;
    }
}

static inline void ClearCompactionRules(void) {
    if (TSGlobalConfig.compactionRules) {
        free(TSGlobalConfig.compactionRules);
        TSGlobalConfig.compactionRules = NULL;
        TSGlobalConfig.compactionRulesCount = 0;
    }
}

void FreeConfig(void) {
    if (TSGlobalConfig.password) {
        free(TSGlobalConfig.password);
        TSGlobalConfig.password = NULL;
    }

    if (getConfigStringCache) {
        RedisModule_FreeString(rts_staticCtx, getConfigStringCache);
        getConfigStringCache = NULL;
    }

    ClearCompactionRules();
}

RedisModuleString *GlobalConfigToString(RedisModuleCtx *ctx) {
    return RedisModule_CreateStringPrintf(
        ctx,
        "COMPACTION_POLICY %s\n"
        "RETENTION_POLICY %lld\n"
        "CHUNK_SIZE % lld\n"
        "ENCODING %s\n"
        "DUPLICATE_POLICY %s\n"
        "NUM_THREADS %lld\n"
        "IGNORE_MAX_VAL_DIFF %lf\n"
        "IGNORE_MAX_TIME_DIFF %lld\n",
        CompactionRulesToString(TSGlobalConfig.compactionRules,
                                TSGlobalConfig.compactionRulesCount),
        TSGlobalConfig.retentionPolicy,
        TSGlobalConfig.chunkSizeBytes,
        ChunkTypeToString(TSGlobalConfig.options),
        DuplicatePolicyToString(TSGlobalConfig.duplicatePolicy),
        TSGlobalConfig.numThreads,
        TSGlobalConfig.ignoreMaxValDiff,
        TSGlobalConfig.ignoreMaxTimeDiff);
}

int ParseDuplicatePolicy(RedisModuleCtx *ctx,
                         RedisModuleString **argv,
                         int argc,
                         const char *arg_prefix,
                         DuplicatePolicy *policy,
                         bool *found);

const char *ChunkTypeToString(int options) {
    if (options & SERIES_OPT_UNCOMPRESSED) {
        return UNCOMPRESSED_ARG_STR;
    }
    if (options & SERIES_OPT_COMPRESSED_GORILLA) {
        return COMPRESSED_GORILLA_ARG_STR;
    }
    return "invalid";
}

static RedisModuleString *getModernStringConfigValue(const char *name, void *privdata) {
    if (!strcasecmp("ts-compaction-policy", name)) {
        char *rulesAsString = CompactionRulesToString(TSGlobalConfig.compactionRules,
                                                      TSGlobalConfig.compactionRulesCount);

        if (!rulesAsString) {
            return NULL;
        }

        if (getConfigStringCache) {
            RedisModule_FreeString(rts_staticCtx, getConfigStringCache);
        }

        getConfigStringCache =
            RedisModule_CreateString(rts_staticCtx, rulesAsString, strlen(rulesAsString));

        free(rulesAsString);

        return getConfigStringCache;
    } else if (!strcasecmp("ts-duplicate-policy", name)) {
        const char *value = DuplicatePolicyToString(TSGlobalConfig.duplicatePolicy);

        if (!value) {
            return NULL;
        }

        if (getConfigStringCache) {
            RedisModule_FreeString(rts_staticCtx, getConfigStringCache);
        }

        getConfigStringCache = RedisModule_CreateString(rts_staticCtx, value, strlen(value));

        return getConfigStringCache;
    } else if (!strcasecmp("ts-encoding", name)) {
        const char *value = ChunkTypeToString(TSGlobalConfig.options);

        if (!value) {
            return NULL;
        }

        if (getConfigStringCache) {
            RedisModule_FreeString(rts_staticCtx, getConfigStringCache);
        }

        getConfigStringCache = RedisModule_CreateString(rts_staticCtx, value, strlen(value));

        return getConfigStringCache;
    } else if (!strcasecmp("ts-ignore-max-val-diff", name)) {
        if (getConfigStringCache) {
            RedisModule_FreeString(rts_staticCtx, getConfigStringCache);
        }

        getConfigStringCache =
            RedisModule_CreateStringPrintf(rts_staticCtx, "%lf", TSGlobalConfig.ignoreMaxValDiff);

        return getConfigStringCache;
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
    long long longValue = 0;

    if (RedisModule_StringToLongLong(string, &longValue) == REDISMODULE_OK) {
        value = (double)longValue;
    } else if (RedisModule_StringToDouble(string, &value) != REDISMODULE_OK) {
        *err = RedisModule_CreateStringPrintf(rts_staticCtx, "Invalid value for `%s`", name);
    }

    if (value < min || value > max) {
        *err = RedisModule_CreateStringPrintf(
            rts_staticCtx,
            "Invalid value for `%s`. Value must be in the range [%f .. %f]",
            name,
            min,
            max);
    }

    return value;
}

static bool Config_SetCompactionPolicyFromCStr(const char *policyString, RedisModuleString **err) {
    if (!policyString || strlen(policyString) == 0) {
        ClearCompactionRules();

        return true;
    }

    SimpleCompactionRule *compactionRules = NULL;
    uint64_t compactionRulesCount = 0;

    if (ParseCompactionPolicy(policyString, &compactionRules, &compactionRulesCount) != TRUE) {
        *err = RedisModule_CreateStringPrintf(NULL, "Invalid compaction policy: %s", policyString);
        return false;
    }

    ClearCompactionRules();

    TSGlobalConfig.compactionRules = compactionRules;
    TSGlobalConfig.compactionRulesCount = compactionRulesCount;

    return true;
}

static bool Config_SetDuplicationPolicyFromRedisString(RedisModuleString *value,
                                                       RedisModuleString **err) {
    const DuplicatePolicy newValue = RMStringLenDuplicationPolicyToEnum(value);

    if (newValue == DP_INVALID) {
        *err = RedisModule_CreateStringPrintf(
            NULL, "Invalid duplicate policy: %s", RedisModule_StringPtrLen(value, NULL));
        return false;
    }

    TSGlobalConfig.duplicatePolicy = newValue;
    return true;
}

static bool Config_SetIgnoreMaxValDiffFromRedisString(RedisModuleString *value,
                                                      RedisModuleString **err) {
    const double newValue = stringToDouble(
        "ts-ignore-max-val-diff", value, IGNORE_MAX_VAL_DIFF_MIN, IGNORE_MAX_VAL_DIFF_MAX, err);

    if (err && *err) {
        return false;
    }

    TSGlobalConfig.ignoreMaxValDiff = newValue;
    return true;
}

static bool Config_SetGlobalPasswordFromRedisString(RedisModuleString *value) {
    if (TSGlobalConfig.password) {
        free(TSGlobalConfig.password);
        TSGlobalConfig.password = NULL;
    }

    size_t len = 0;
    const char *str = RedisModule_StringPtrLen(value, &len);

    if (!str || len == 0) {
        return true;
    }

    TSGlobalConfig.password = strndup(str, len);
    return true;
}

static bool Config_SetEncodingFromRedisString(RedisModuleString *value, RedisModuleString **err) {
    size_t len = 0;
    const char *encoding = RedisModule_StringPtrLen(value, &len);

    if (!strcasecmp(encoding, UNCOMPRESSED_ARG_STR)) {
        TSGlobalConfig.options &= ~SERIES_OPT_DEFAULT_COMPRESSION;
        TSGlobalConfig.options |= SERIES_OPT_UNCOMPRESSED;
    } else if (!strcasecmp(encoding, COMPRESSED_GORILLA_ARG_STR)) {
        TSGlobalConfig.options &= ~SERIES_OPT_DEFAULT_COMPRESSION;
        TSGlobalConfig.options |= SERIES_OPT_COMPRESSED_GORILLA;
    } else {
        *err = RedisModule_CreateStringPrintf(NULL, "Invalid encoding: %s", encoding);
        return false;
    }

    return true;
}

static int setModernStringConfigValue(const char *name,
                                      RedisModuleString *value,
                                      void *data,
                                      RedisModuleString **err) {
    if (!strcasecmp("ts-compaction-policy", name)) {
        size_t len = 0;
        const char *policyString = RedisModule_StringPtrLen(value, &len);

        return Config_SetCompactionPolicyFromCStr(policyString, err) ? REDISMODULE_OK
                                                                     : REDISMODULE_ERR;
    } else if (!strcasecmp("ts-duplicate-policy", name)) {
        return Config_SetDuplicationPolicyFromRedisString(value, err) ? REDISMODULE_OK
                                                                      : REDISMODULE_ERR;
    } else if (!strcasecmp("ts-ignore-max-val-diff", name)) {
        return Config_SetIgnoreMaxValDiffFromRedisString(value, err) ? REDISMODULE_OK
                                                                     : REDISMODULE_ERR;
    } else if (!strcasecmp("ts-encoding", name)) {
        return Config_SetEncodingFromRedisString(value, err) ? REDISMODULE_OK : REDISMODULE_ERR;
    }

    return REDISMODULE_ERR;
}

static long long getModernIntegerConfigValue(const char *name, void *privdata) {
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

static int setModernIntegerConfigValue(const char *name,
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
        if (!ValidateChunkSize(NULL, value, err)) {
            return REDISMODULE_ERR;
        }

        TSGlobalConfig.chunkSizeBytes = value;

        return REDISMODULE_OK;
    } else if (!strcasecmp("ts-ignore-max-time-diff", name)) {
        if (value < 0) {
            *err = RedisModule_CreateStringPrintf(
                NULL, "Invalid value for `ts-ignore-max-time-diff`. Value must be non-negative");
            return REDISMODULE_ERR;
        }

        TSGlobalConfig.ignoreMaxTimeDiff = value;

        return REDISMODULE_OK;
    }

    return REDISMODULE_ERR;
}

bool RegisterModernConfigurationOptions(RedisModuleCtx *ctx) {
    RedisModule_Log(ctx, "notice", "Registering configuration options: [");
    {
        char *oldValue = CompactionRulesToString(TSGlobalConfig.compactionRules,
                                                 TSGlobalConfig.compactionRulesCount);
        if (!oldValue) {
            oldValue = strdup("");
        }

        if (RedisModule_RegisterStringConfig(ctx,
                                             "ts-compaction-policy",
                                             oldValue,
                                             REDISMODULE_CONFIG_UNPREFIXED,
                                             getModernStringConfigValue,
                                             setModernStringConfigValue,
                                             NULL,
                                             NULL)) {
            free(oldValue);

            return false;
        }

        RedisModule_Log(ctx, "notice", "\t{ %-*s: %*s }", 23, "ts-compaction-policy", 12, oldValue);

        free(oldValue);
    }

    if (RedisModule_RegisterNumericConfig(ctx,
                                          "ts-num-threads",
                                          TSGlobalConfig.numThreads,
                                          REDISMODULE_CONFIG_IMMUTABLE |
                                              REDISMODULE_CONFIG_UNPREFIXED,
                                          NUM_THREADS_MIN,
                                          NUM_THREADS_MAX,
                                          getModernIntegerConfigValue,
                                          setModernIntegerConfigValue,
                                          NULL,
                                          NULL)) {
        return false;
    }

    RedisModule_Log(ctx, "notice", "\t{ %-*s: %*lld }", 23, "ts-num-threads", 12, TSGlobalConfig.numThreads);

    if (RedisModule_RegisterNumericConfig(ctx,
                                          "ts-retention-policy",
                                          TSGlobalConfig.retentionPolicy,
                                          REDISMODULE_CONFIG_UNPREFIXED,
                                          RETENTION_POLICY_MIN,
                                          RETENTION_POLICY_MAX,
                                          getModernIntegerConfigValue,
                                          setModernIntegerConfigValue,
                                          NULL,
                                          NULL)) {
        return false;
    }

    RedisModule_Log(ctx, "notice", "\t{ %-*s: %*lld }", 23, "ts-retention-policy", 12, TSGlobalConfig.retentionPolicy);

    if (RedisModule_RegisterStringConfig(ctx,
                                         "ts-duplicate-policy",
                                         DuplicatePolicyToString(TSGlobalConfig.duplicatePolicy),
                                         REDISMODULE_CONFIG_UNPREFIXED,
                                         getModernStringConfigValue,
                                         setModernStringConfigValue,
                                         NULL,
                                         NULL)) {
        return false;
    }

    RedisModule_Log(ctx, "notice", "\t{ %-*s: %*s }", 23, "ts-duplicate-policy", 12, DuplicatePolicyToString(TSGlobalConfig.duplicatePolicy));

    if (RedisModule_RegisterNumericConfig(ctx,
                                          "ts-chunk-size-bytes",
                                          TSGlobalConfig.chunkSizeBytes,
                                          REDISMODULE_CONFIG_UNPREFIXED,
                                          CHUNK_SIZE_BYTES_MIN,
                                          CHUNK_SIZE_BYTES_MAX,
                                          getModernIntegerConfigValue,
                                          setModernIntegerConfigValue,
                                          NULL,
                                          NULL)) {
        return false;
    }

    RedisModule_Log(ctx, "notice", "\t{ %-*s: %*lld }", 23, "ts-chunk-size-bytes", 12, TSGlobalConfig.chunkSizeBytes);

    if (RedisModule_RegisterStringConfig(ctx,
                                         "ts-encoding",
                                         ChunkTypeToString(TSGlobalConfig.options),
                                         REDISMODULE_CONFIG_UNPREFIXED,
                                         getModernStringConfigValue,
                                         setModernStringConfigValue,
                                         NULL,
                                         NULL)) {
        return false;
    }

    RedisModule_Log(ctx, "notice", "\t{ %-*s: %*s }", 23, "ts-encoding", 12, ChunkTypeToString(TSGlobalConfig.options));

    if (RedisModule_RegisterNumericConfig(ctx,
                                          "ts-ignore-max-time-diff",
                                          TSGlobalConfig.ignoreMaxTimeDiff,
                                          REDISMODULE_CONFIG_UNPREFIXED,
                                          IGNORE_MAX_TIME_DIFF_MIN,
                                          IGNORE_MAX_TIME_DIFF_MAX,
                                          getModernIntegerConfigValue,
                                          setModernIntegerConfigValue,
                                          NULL,
                                          NULL)) {
        return false;
    }

    RedisModule_Log(ctx, "notice", "\t{ %-*s: %*lld }", 23, "ts-ignore-max-time-diff", 12, TSGlobalConfig.ignoreMaxTimeDiff);

    {
        char oldValue[32] = { 0 };
        snprintf(oldValue, sizeof(oldValue), "%lf", TSGlobalConfig.ignoreMaxValDiff);

        if (RedisModule_RegisterStringConfig(ctx,
                                             "ts-ignore-max-val-diff",
                                             oldValue,
                                             REDISMODULE_CONFIG_UNPREFIXED,
                                             getModernStringConfigValue,
                                             setModernStringConfigValue,
                                             NULL,
                                             NULL)) {
            return false;
        }

        RedisModule_Log(ctx, "notice", "\t{ %-*s: %*s }", 23, "ts-ignore-max-val-diff", 12, oldValue);
    }

    RedisModule_Log(ctx, "notice", "]");

    return true;
}

bool RegisterConfigurationOptions(RedisModuleCtx *ctx) {
    return RegisterModernConfigurationOptions(ctx);
}

int ReadDeprecatedLoadTimeConfig(RedisModuleCtx *ctx,
                                 RedisModuleString **argv,
                                 int argc,
                                 const bool showDeprecationWarning) {
    bool isDeprecated = false;

    if (argc > 1 && RMUtil_ArgIndex("COMPACTION_POLICY", argv, argc) >= 0) {
        LOG_DEPRECATED_OPTION("COMPACTION_POLICY", "ts-compaction-policy", showDeprecationWarning);

        RedisModuleString *policy;
        const char *policy_cstr;
        size_t len;

        if (RMUtil_ParseArgsAfter("COMPACTION_POLICY", argv, argc, "s", &policy) !=
            REDISMODULE_OK) {
            RedisModule_Log(ctx, "warning", "Unable to parse argument after COMPACTION_POLICY");
            return TSDB_ERROR;
        }
        policy_cstr = RedisModule_StringPtrLen(policy, &len);
        SimpleCompactionRule *compactionRules = NULL;
        uint64_t compactionRulesCount = 0;
        if (ParseCompactionPolicy(policy_cstr, &compactionRules, &compactionRulesCount) != TRUE) {
            RedisModule_Log(ctx, "warning", "Unable to parse argument after COMPACTION_POLICY");
            free(compactionRules);
            return TSDB_ERROR;
        }

        if (TSGlobalConfig.compactionRules) {
            free(TSGlobalConfig.compactionRules);
        }

        TSGlobalConfig.compactionRules = compactionRules;
        TSGlobalConfig.compactionRulesCount = compactionRulesCount;
        isDeprecated = true;
    }

    if (argc > 1 && RMUtil_ArgIndex("OSS_GLOBAL_PASSWORD", argv, argc) >= 0) {
        LOG_DEPRECATED_OPTION("OSS_GLOBAL_PASSWORD", NULL, showDeprecationWarning);

        RedisModuleString *password;
        size_t len;
        if (RMUtil_ParseArgsAfter("OSS_GLOBAL_PASSWORD", argv, argc, "s", &password) !=
            REDISMODULE_OK) {
            RedisModule_Log(ctx, "warning", "Unable to parse argument after OSS_GLOBAL_PASSWORD");
            return TSDB_ERROR;
        }

        const char *temp = (char *)RedisModule_StringPtrLen(password, &len);
        TSGlobalConfig.password = strndup(temp, len);
        RedisModule_Log(ctx, "notice", "loaded tls password");
        isDeprecated = true;
    }

    if (argc > 1 && RMUtil_ArgIndex("global-password", argv, argc) >= 0) {
        LOG_DEPRECATED_OPTION("global-password", NULL, showDeprecationWarning);
        RedisModuleString *password;
        size_t len;
        if (RMUtil_ParseArgsAfter("global-password", argv, argc, "s", &password) !=
            REDISMODULE_OK) {
            RedisModule_Log(ctx, "warning", "Unable to parse argument after global-password");
            return TSDB_ERROR;
        }

        const char *temp = (char *)RedisModule_StringPtrLen(password, &len);
        TSGlobalConfig.password = strndup(temp, len);
        RedisModule_Log(ctx, "notice", "loaded global-password");
        isDeprecated = true;
    }

    if (argc > 1 && RMUtil_ArgIndex("RETENTION_POLICY", argv, argc) >= 0) {
        LOG_DEPRECATED_OPTION("RETENTION_POLICY", "ts-retention-policy", showDeprecationWarning);

        if (RMUtil_ParseArgsAfter(
                "RETENTION_POLICY", argv, argc, "l", &TSGlobalConfig.retentionPolicy) !=
            REDISMODULE_OK) {
            RedisModule_Log(ctx, "warning", "Unable to parse argument after RETENTION_POLICY");
            return TSDB_ERROR;
        }

        isDeprecated = true;
    }

    if (!ValidateChunkSize(ctx, Chunk_SIZE_BYTES_SECS, NULL)) {
        return TSDB_ERROR;
    }
    TSGlobalConfig.chunkSizeBytes = Chunk_SIZE_BYTES_SECS;
    bool chunkSizeBytesFound = false;
    if (ParseChunkSize(ctx,
                       argv,
                       argc,
                       "CHUNK_SIZE_BYTES",
                       &TSGlobalConfig.chunkSizeBytes,
                       &chunkSizeBytesFound) != REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "Unable to parse argument after CHUNK_SIZE_BYTES");
        return TSDB_ERROR;
    }

    LOG_DEPRECATED_OPTION(
        "CHUNK_SIZE_BYTES", "ts-chunk-size-bytes", chunkSizeBytesFound && showDeprecationWarning);

    isDeprecated = isDeprecated || chunkSizeBytesFound;

    TSGlobalConfig.duplicatePolicy = DEFAULT_DUPLICATE_POLICY;
    bool duplicatePolicyFound = false;
    if (ParseDuplicatePolicy(ctx,
                             argv,
                             argc,
                             DUPLICATE_POLICY_ARG,
                             &TSGlobalConfig.duplicatePolicy,
                             &duplicatePolicyFound) != TSDB_OK) {
        RedisModule_Log(ctx, "warning", "Unable to parse argument after DUPLICATE_POLICY");
        return TSDB_ERROR;
    }

    isDeprecated = isDeprecated || duplicatePolicyFound;

    LOG_DEPRECATED_OPTION(
        "DUPLICATE_POLICY", "ts-duplicate-policy", duplicatePolicyFound && showDeprecationWarning);

    if (argc > 1 && (RMUtil_ArgIndex("ENCODING", argv, argc) >= 0 ||
                     RMUtil_ArgIndex("CHUNK_TYPE", argv, argc) >= 0)) {
        isDeprecated = true;

        if (RMUtil_ArgIndex("ENCODING", argv, argc) >= 0) {
            LOG_DEPRECATED_OPTION("ENCODING", "ts-encoding", showDeprecationWarning);
        }

        if (RMUtil_ArgIndex("CHUNK_TYPE", argv, argc) >= 0) {
            LOG_DEPRECATED_OPTION("CHUNK_TYPE", "ts-encoding", showDeprecationWarning);
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
            RedisModule_Log(ctx, "warning", "Unable to parse argument after NUM_THREADS");
            return TSDB_ERROR;
        }
        LOG_DEPRECATED_OPTION("NUM_THREADS", "ts-num-threads", showDeprecationWarning);
        isDeprecated = true;
    } else {
        TSGlobalConfig.numThreads = DEFAULT_NUM_THREADS;
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

        isDeprecated = true;
    }
    TSGlobalConfig.dontAssertOnFailure = false;
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
            TSGlobalConfig.dontAssertOnFailure = true;
        } else if (!strcasecmp(dontAssertOnFailiure_cstr, "disable")) {
            TSGlobalConfig.dontAssertOnFailure = false;
        }

        extern bool _dontAssertOnFailiure;
        _dontAssertOnFailiure = TSGlobalConfig.dontAssertOnFailure;

        isDeprecated = true;
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
        LOG_DEPRECATED_OPTION(
            "IGNORE_MAX_TIME_DIFF", "ts-ignore-max-time-diff", showDeprecationWarning);
        isDeprecated = true;
    }

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

        LOG_DEPRECATED_OPTION(
            "IGNORE_MAX_VAL_DIFF", "ts-ignore-max-val-diff", showDeprecationWarning);
        isDeprecated = true;
    }

    if (isDeprecated && showDeprecationWarning) {
        RedisModule_Log(ctx,
                        "warning",
                        "Deprecated load-time configuration options were used. These will be "
                        "removed in future versions of RedisTimeSeries.");
    }

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

int RTS_CheckSupportedVestion(void) {
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

void RTS_GetRedisVersion(void) {
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
