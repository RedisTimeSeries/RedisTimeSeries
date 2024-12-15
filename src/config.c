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

#define LOG_DEPRECATED_OPTION(deprecatedName, modernName)                                          \
    RedisModule_Log(rts_staticCtx,                                                                 \
                    "warning",                                                                     \
                    "%s is deprecated, please use the '%s' instead",                               \
                    deprecatedName,                                                                \
                    modernName);

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

static RedisModuleString *getModernStringConfigValue(const char *name, void *privdata) {
    if (!strcasecmp("ts-compaction-policy", name)) {
        char *rulesAsString = CompactionRulesToString(TSGlobalConfig.compactionRules,
                                                      TSGlobalConfig.compactionRulesCount);
        RedisModuleString *out =
            RedisModule_CreateString(rts_staticCtx, rulesAsString, strlen(rulesAsString));
        free(rulesAsString);

        return out;
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

static RedisModuleString *getDeprecatedStringConfigValue(const char *deprecatedName,
                                                         void *modernName) {
    const char *modernNameStr = (const char *)modernName;

    LOG_DEPRECATED_OPTION(deprecatedName, modernNameStr);

    return getModernStringConfigValue(modernNameStr, NULL);
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

static int setModernStringConfigValue(const char *name,
                                      RedisModuleString *value,
                                      void *data,
                                      RedisModuleString **err) {
    if (!strcasecmp("ts-compaction-policy", name) || !strcasecmp("COMPACTION_POLICY", name)) {
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

static int setDeprecatedStringConfigValue(const char *deprecatedName,
                                          RedisModuleString *value,
                                          void *data,
                                          RedisModuleString **err) {
    const char *modernNameStr = (const char *)data;

    LOG_DEPRECATED_OPTION(deprecatedName, modernNameStr);

    return setModernStringConfigValue(modernNameStr, value, NULL, err);
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

static long long getDeprecatedIntegerConfigValue(const char *deprecatedName, void *privdata) {
    const char *modernNameStr = (const char *)privdata;

    LOG_DEPRECATED_OPTION(deprecatedName, modernNameStr);

    return getModernIntegerConfigValue(modernNameStr, NULL);
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

static int setDeprecatedIntegerConfigValue(const char *deprecatedName,
                                           long long value,
                                           void *data,
                                           RedisModuleString **err) {
    const char *modernNameStr = (const char *)data;

    LOG_DEPRECATED_OPTION(deprecatedName, modernNameStr);

    return setModernIntegerConfigValue(modernNameStr, value, NULL, err);
}

// Registers the options deprecated in 8.0.
bool RegisterDeprecatedConfigurationOptions(RedisModuleCtx *ctx) {
    if (RedisModule_RegisterStringConfig(ctx,
                                         "OSS_GLOBAL_PASSWORD",
                                         NULL,
                                         REDISMODULE_CONFIG_IMMUTABLE |
                                             REDISMODULE_CONFIG_SENSITIVE,
                                         getDeprecatedStringConfigValue,
                                         setDeprecatedStringConfigValue,
                                         NULL,
                                         "global-password")) {
        return false;
    }

    if (RedisModule_RegisterStringConfig(ctx,
                                         "COMPACTION_POLICY",
                                         NULL,
                                         REDISMODULE_CONFIG_DEFAULT,
                                         getDeprecatedStringConfigValue,
                                         setDeprecatedStringConfigValue,
                                         NULL,
                                         "ts-compaction-policy")) {
        return false;
    }

    if (RedisModule_RegisterNumericConfig(ctx,
                                          "NUM_THREADS",
                                          DEFAULT_NUM_THREADS,
                                          REDISMODULE_CONFIG_IMMUTABLE,
                                          NUM_THREADS_MIN,
                                          NUM_THREADS_MAX,
                                          getDeprecatedIntegerConfigValue,
                                          setDeprecatedIntegerConfigValue,
                                          NULL,
                                          "ts-num-threads")) {
        return false;
    }

    if (RedisModule_RegisterNumericConfig(ctx,
                                          "RETENTION_POLICY",
                                          RETENTION_TIME_DEFAULT,
                                          REDISMODULE_CONFIG_DEFAULT,
                                          RETENTION_POLICY_MIN,
                                          RETENTION_POLICY_MAX,
                                          getDeprecatedIntegerConfigValue,
                                          setDeprecatedIntegerConfigValue,
                                          NULL,
                                          "ts-retention-policy")) {
        return false;
    }

    if (RedisModule_RegisterStringConfig(ctx,
                                         "DUPLICATE_POLICY",
                                         DEFAULT_DUPLICATE_POLICY_STRING,
                                         REDISMODULE_CONFIG_DEFAULT,
                                         getDeprecatedStringConfigValue,
                                         setDeprecatedStringConfigValue,
                                         NULL,
                                         "ts-duplicate-policy")) {
        return false;
    }

    if (RedisModule_RegisterNumericConfig(ctx,
                                          "CHUNK_SIZE_BYTES",
                                          Chunk_SIZE_BYTES_SECS,
                                          REDISMODULE_CONFIG_DEFAULT,
                                          CHUNK_SIZE_BYTES_MIN,
                                          CHUNK_SIZE_BYTES_MAX,
                                          getDeprecatedIntegerConfigValue,
                                          setDeprecatedIntegerConfigValue,
                                          NULL,
                                          "ts-chunk-size-bytes")) {
        return false;
    }

    if (RedisModule_RegisterStringConfig(ctx,
                                         "ENCODING",
                                         DEFAULT_ENCODING_STRING,
                                         REDISMODULE_CONFIG_DEFAULT,
                                         getDeprecatedStringConfigValue,
                                         setDeprecatedStringConfigValue,
                                         NULL,
                                         "ts-encoding")) {
        return false;
    }

    if (RedisModule_RegisterNumericConfig(ctx,
                                          "IGNORE_MAX_TIME_DIFF",
                                          IGNORE_MAX_TIME_DIFF_DEFAULT,
                                          REDISMODULE_CONFIG_DEFAULT,
                                          IGNORE_MAX_TIME_DIFF_MIN,
                                          IGNORE_MAX_TIME_DIFF_MAX,
                                          getDeprecatedIntegerConfigValue,
                                          setDeprecatedIntegerConfigValue,
                                          NULL,
                                          "ts-ignore-max-time-diff")) {
        return false;
    }

    if (RedisModule_RegisterStringConfig(ctx,
                                         "IGNORE_MAX_VAL_DIFF",
                                         "0.0",
                                         REDISMODULE_CONFIG_DEFAULT,
                                         getDeprecatedStringConfigValue,
                                         setDeprecatedStringConfigValue,
                                         NULL,
                                         "ts-ignore-max-val-diff")) {
        return false;
    }

    return true;
}

bool RegisterModernConfigurationOptions(RedisModuleCtx *ctx) {
    if (RedisModule_RegisterStringConfig(ctx,
                                         "ts-compaction-policy",
                                         NULL,
                                         REDISMODULE_CONFIG_DEFAULT,
                                         getModernStringConfigValue,
                                         setModernStringConfigValue,
                                         NULL,
                                         NULL)) {
        return false;
    }

    if (RedisModule_RegisterStringConfig(ctx,
                                         "global-password",
                                         NULL,
                                         REDISMODULE_CONFIG_IMMUTABLE |
                                             REDISMODULE_CONFIG_SENSITIVE,
                                         getModernStringConfigValue,
                                         setModernStringConfigValue,
                                         NULL,
                                         NULL)) {
        return false;
    }

    if (RedisModule_RegisterStringConfig(ctx,
                                         "global-user",
                                         NULL,
                                         REDISMODULE_CONFIG_IMMUTABLE |
                                             REDISMODULE_CONFIG_SENSITIVE,
                                         getModernStringConfigValue,
                                         setModernStringConfigValue,
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
                                          getModernIntegerConfigValue,
                                          setModernIntegerConfigValue,
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
                                          getModernIntegerConfigValue,
                                          setModernIntegerConfigValue,
                                          NULL,
                                          NULL)) {
        return false;
    }

    if (RedisModule_RegisterStringConfig(ctx,
                                         "ts-duplicate-policy",
                                         DEFAULT_DUPLICATE_POLICY_STRING,
                                         REDISMODULE_CONFIG_DEFAULT,
                                         getModernStringConfigValue,
                                         setModernStringConfigValue,
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
                                          getModernIntegerConfigValue,
                                          setModernIntegerConfigValue,
                                          NULL,
                                          NULL)) {
        return false;
    }

    if (RedisModule_RegisterStringConfig(ctx,
                                         "ts-encoding",
                                         DEFAULT_ENCODING_STRING,
                                         REDISMODULE_CONFIG_DEFAULT,
                                         getModernStringConfigValue,
                                         setModernStringConfigValue,
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
                                          getModernIntegerConfigValue,
                                          setModernIntegerConfigValue,
                                          NULL,
                                          NULL)) {
        return false;
    }

    if (RedisModule_RegisterStringConfig(ctx,
                                         "ts-ignore-max-val-diff",
                                         "0.0",
                                         REDISMODULE_CONFIG_DEFAULT,
                                         getModernStringConfigValue,
                                         setModernStringConfigValue,
                                         NULL,
                                         NULL)) {
        return false;
    }

    return true;
}

bool RegisterConfigurationOptions(RedisModuleCtx *ctx) {
    return RegisterDeprecatedConfigurationOptions(ctx) && RegisterModernConfigurationOptions(ctx);
}

int ReadDeprecatedConfig(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    TSGlobalConfig.options = SERIES_OPT_DEFAULT_COMPRESSION;
    bool hasDeprecatedConfig = false;

    if (argc > 1 && RMUtil_ArgIndex("COMPACTION_POLICY", argv, argc) >= 0) {
        LOG_DEPRECATED_OPTION("COMPACTION_POLICY", "ts-compaction-policy");

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
        hasDeprecatedConfig = true;
    }

    if (argc > 1 && RMUtil_ArgIndex("OSS_GLOBAL_PASSWORD", argv, argc) >= 0) {
        LOG_DEPRECATED_OPTION("OSS_GLOBAL_PASSWORD", "global-password");

        RedisModuleString *password;
        size_t len;
        if (RMUtil_ParseArgsAfter("OSS_GLOBAL_PASSWORD", argv, argc, "s", &password) !=
            REDISMODULE_OK) {
            RedisModule_Log(ctx, "warning", "Unable to parse argument after OSS_GLOBAL_PASSWORD");
            return TSDB_ERROR;
        }

        TSGlobalConfig.password = (char *)RedisModule_StringPtrLen(password, &len);
        RedisModule_Log(ctx, "notice", "loaded tls password");
        hasDeprecatedConfig = true;
    } else {
        TSGlobalConfig.password = NULL;
    }

    if (argc > 1 && RMUtil_ArgIndex("RETENTION_POLICY", argv, argc) >= 0) {
        LOG_DEPRECATED_OPTION("RETENTION_POLICY", "ts-retention-policy");

        if (RMUtil_ParseArgsAfter(
                "RETENTION_POLICY", argv, argc, "l", &TSGlobalConfig.retentionPolicy) !=
            REDISMODULE_OK) {
            RedisModule_Log(ctx, "warning", "Unable to parse argument after RETENTION_POLICY");
            return TSDB_ERROR;
        }

        RedisModule_Log(
            ctx, "notice", "loaded default retention policy: %lld", TSGlobalConfig.retentionPolicy);
        hasDeprecatedConfig = true;
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
        hasDeprecatedConfig = true;

        if (RMUtil_ArgIndex("CHUNK_TYPE", argv, argc) >= 0) {
            RedisModule_Log(ctx,
                            "warning",
                            "CHUNK_TYPE and ENCODING configuration options were deprecated "
                            "and will be removed in future versions of RedisTimeSeries. "
                            "Please use the 'ts-encoding' configuration instead.");
        }
        RedisModuleString *chunk_type;
        size_t len;
        const char *chunk_type_cstr;
        if (RMUtil_ArgIndex("CHUNK_TYPE", argv, argc) >= 0 &&
            RMUtil_ParseArgsAfter("CHUNK_TYPE", argv, argc, "s", &chunk_type) != REDISMODULE_OK) {
            RedisModule_Log(ctx, "warning", "Unable to parse argument after CHUNK_TYPE");
            return TSDB_ERROR;
        } else {
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
        LOG_DEPRECATED_OPTION("NUM_THREADS", "ts-num-threads");
        hasDeprecatedConfig = true;
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

        hasDeprecatedConfig = true;
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
        hasDeprecatedConfig = true;
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
        LOG_DEPRECATED_OPTION("IGNORE_MAX_TIME_DIFF", "ts-ignore-max-time-diff");
        hasDeprecatedConfig = true;
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
        LOG_DEPRECATED_OPTION("IGNORE_MAX_VAL_DIFF", "ts-ignore-max-val-diff");
        hasDeprecatedConfig = true;
    }
    RedisModule_Log(
        ctx, "notice", "loaded default IGNORE_MAX_VAL_DIFF: %f", TSGlobalConfig.ignoreMaxValDiff);

    RedisModule_Log(ctx,
                    "notice",
                    "Setting default series ENCODING to: %s",
                    ChunkTypeToString(TSGlobalConfig.options));

    if (hasDeprecatedConfig) {
        RedisModule_Log(ctx,
                        "warning",
                        "Deprecated configuration options were used. These will be "
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
