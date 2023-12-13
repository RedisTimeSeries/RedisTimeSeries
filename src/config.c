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

static const char *duplicate_policy_names[] = {
    "block", "last", "first", "min", "max", "sum",
};

static const int duplicate_policy_flags[] = {
    DP_BLOCK, DP_LAST, DP_FIRST, DP_MIN, DP_MAX, DP_SUM,
};

#define DUPLICATE_POLICY_FLAG_COUNT (sizeof(duplicate_policy_flags) / sizeof(int))

static const char *encoding_names[] = {
    "uncompressed",
    "compressed",
};

static const int encoding_flags[] = { SERIES_OPT_UNCOMPRESSED, SERIES_OPT_COMPRESSED_GORILLA };

#define ENCODING_FLAG_COUNT (sizeof(encoding_flags) / sizeof(int))

static const char *conf_compaction_policy = "compaction_policy";
static const char *conf_oss_global_password = "oss_global_password";
static const char *conf_retention_policy = "retention_policy";
static const char *conf_chunk_size_bytes = "chunk_size_bytes";
static const char *conf_duplicate_policy = "duplicate_policy";
static const char *conf_encoding = "encoding";
static const char *conf_num_threads = "num_threads";
static const char *conf_debug_force_rule_dump = "debug_force_rule_dump";
static const char *conf_debug_dont_assert_on_failure = "debug_dont_assert_on_failure";

static RedisModuleString *getString(const char *name, void *privdata) {
    TSConfig *c = privdata;

    if (strcasecmp(name, conf_compaction_policy) == 0) {
        return c->compactionPolicyStr;
    } else if (strcasecmp(name, conf_oss_global_password) == 0) {
        return c->passwordStr;
    }

    return NULL;
}

static int setString(const char *name,
                     RedisModuleString *val,
                     void *privdata,
                     RedisModuleString **err) {
    size_t len;
    const char *value = RedisModule_StringPtrLen(val, &len);

    if (strcasecmp(name, conf_compaction_policy) == 0) {
        SimpleCompactionRule *compactionRules = NULL;
        uint64_t compactionRulesCount = 0;

        if (len != 0) {
            if (ParseCompactionPolicy(value, &compactionRules, &compactionRulesCount) != TRUE) {
                *err = RedisModule_CreateStringPrintf(NULL,
                                                      "Unable to parse compaction policy config");
                return REDISMODULE_ERR;
            }
        }

        SetCompactionRulesConfig(val, compactionRules, compactionRulesCount);
    } else if (strcasecmp(name, conf_oss_global_password) == 0) {
        SetOSSGlobalPasswordConfig(val);
    } else {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}

static int getEnum(const char *name, void *privdata) {
    TSConfig *c = privdata;

    if (strcasecmp(name, conf_duplicate_policy) == 0) {
        return c->duplicatePolicy;
    } else if (strcasecmp(name, conf_encoding) == 0) {
        return c->options;
    }

    return REDISMODULE_ERR;
}

static int setEnum(const char *name, int val, void *privdata, RedisModuleString **err) {
    TSConfig *c = privdata;

    if (strcasecmp(name, conf_duplicate_policy) == 0) {
        c->duplicatePolicy = val;
    } else if (strcasecmp(name, conf_encoding) == 0) {
        c->options = val;
    } else {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}

static int getBool(const char *name, void *privdata) {
    TSConfig *c = privdata;

    if (strcasecmp(name, conf_debug_force_rule_dump) == 0) {
        return c->forceSaveCrossRef;
    } else if (strcasecmp(name, conf_debug_dont_assert_on_failure) == 0) {
        return c->dontAssertOnFailure;
    }

    return REDISMODULE_ERR;
}

static int setBool(const char *name, int val, void *privdata, RedisModuleString **err) {
    TSConfig *c = privdata;

    if (strcasecmp(name, conf_debug_force_rule_dump) == 0) {
        c->forceSaveCrossRef = val;
    } else if (strcasecmp(name, conf_debug_dont_assert_on_failure) == 0) {
        c->dontAssertOnFailure = val;
        _dontAssertOnFailiure = val;
    } else {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}

static long long getNumeric(const char *name, void *privdata) {
    TSConfig *c = privdata;

    if (strcasecmp(name, conf_retention_policy) == 0) {
        return c->retentionPolicy;
    } else if (strcasecmp(name, conf_chunk_size_bytes) == 0) {
        return c->chunkSizeBytes;
    } else if (strcasecmp(name, conf_num_threads) == 0) {
        return c->numThreads;
    }

    return REDISMODULE_ERR;
}

static int setNumeric(const char *name, long long val, void *priv, RedisModuleString **err) {
    TSConfig *c = priv;

    if (strcasecmp(name, conf_retention_policy) == 0) {
        c->retentionPolicy = val;
    } else if (strcasecmp(name, conf_chunk_size_bytes) == 0) {
        if (val % 8 != 0) {
            // Currently the gorilla algorithm implementation can only handle chunks of size
            // multiplication of 8
            *err = RedisModule_CreateStringPrintf(
                NULL,
                "TSDB: CHUNK_SIZE value must be a multiple of 8 in the range [48 .. 1048576]");
            return REDISMODULE_ERR;
        }

        c->chunkSizeBytes = val;
    } else if (strcasecmp(name, conf_num_threads) == 0) {
        c->numThreads = val;
    } else {
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}

void FreeGlobalConfig(void) {
    if (TSGlobalConfig.passwordStr) {
        RedisModule_FreeString(NULL, TSGlobalConfig.passwordStr);
    }
    free(TSGlobalConfig.password);

    if (TSGlobalConfig.compactionPolicyStr) {
        RedisModule_FreeString(NULL, TSGlobalConfig.compactionPolicyStr);
    }
    free(TSGlobalConfig.compactionRules);
}

// Configuration via module config API
int ModuleConfigInit(RedisModuleCtx *ctx) {
    int ret = 0;
    /* clang-format off */
                                                 /* name */                         /* default-value */                 /* flags */                   /* min - max value */
   ret |= RedisModule_RegisterNumericConfig(ctx, conf_retention_policy,             RETENTION_TIME_DEFAULT,         REDISMODULE_CONFIG_IMMUTABLE,       0,  LONG_MAX,            getNumeric, setNumeric, NULL, &TSGlobalConfig);
   ret |= RedisModule_RegisterNumericConfig(ctx, conf_chunk_size_bytes,             Chunk_SIZE_BYTES_SECS,          REDISMODULE_CONFIG_IMMUTABLE,       48, 1048576,             getNumeric, setNumeric, NULL, &TSGlobalConfig);
   ret |= RedisModule_RegisterNumericConfig(ctx, conf_num_threads,                  3,                              REDISMODULE_CONFIG_IMMUTABLE,       0,  LONG_MAX,            getNumeric, setNumeric, NULL, &TSGlobalConfig);

                                                 /* name */                         /* default-value */                 /* flags */
   ret |= RedisModule_RegisterBoolConfig(ctx,    conf_debug_force_rule_dump,        false,                          REDISMODULE_CONFIG_IMMUTABLE | REDISMODULE_CONFIG_HIDDEN,    getBool,    setBool,    NULL, &TSGlobalConfig);
   ret |= RedisModule_RegisterBoolConfig(ctx,    conf_debug_dont_assert_on_failure, false,                          REDISMODULE_CONFIG_IMMUTABLE | REDISMODULE_CONFIG_HIDDEN,    getBool,    setBool,    NULL, &TSGlobalConfig);

                                                 /* name */                         /* default-value */                 /* flags */
   ret |= RedisModule_RegisterStringConfig(ctx,  conf_compaction_policy,            "",                             REDISMODULE_CONFIG_IMMUTABLE,                                getString,  setString,  NULL, &TSGlobalConfig);
   ret |= RedisModule_RegisterStringConfig(ctx,  conf_oss_global_password,          "",                             REDISMODULE_CONFIG_IMMUTABLE,                                getString,  setString,  NULL, &TSGlobalConfig);

                                                  /* name */                        /* default-value */                 /* flags */
   ret |= RedisModule_RegisterEnumConfig(ctx,    conf_duplicate_policy,             DEFAULT_DUPLICATE_POLICY,       REDISMODULE_CONFIG_IMMUTABLE,                                duplicate_policy_names,   duplicate_policy_flags, DUPLICATE_POLICY_FLAG_COUNT, getEnum, setEnum, NULL, &TSGlobalConfig);
   ret |= RedisModule_RegisterEnumConfig(ctx,    conf_encoding,                     SERIES_OPT_COMPRESSED_GORILLA,  REDISMODULE_CONFIG_IMMUTABLE | REDISMODULE_CONFIG_BITFLAGS,  encoding_names,           encoding_flags,         ENCODING_FLAG_COUNT,         getEnum, setEnum, NULL, &TSGlobalConfig);

    /* clang-format on */

    ret |= RedisModule_LoadConfigs(ctx);
    if (ret != REDISMODULE_OK) {
        return TSDB_ERROR;
    }

    return TSDB_OK;
}

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

void SetCompactionRulesConfig(RedisModuleString *policyStr,
                              SimpleCompactionRule *compactionRules,
                              uint64_t compactionRulesCount) {
    if (TSGlobalConfig.compactionPolicyStr) {
        RedisModule_FreeString(NULL, TSGlobalConfig.compactionPolicyStr);
    }
    free(TSGlobalConfig.compactionRules);

    TSGlobalConfig.compactionPolicyStr = RedisModule_CreateStringFromString(NULL, policyStr);
    TSGlobalConfig.compactionRules = compactionRules;
    TSGlobalConfig.compactionRulesCount = compactionRulesCount;
}

void SetOSSGlobalPasswordConfig(RedisModuleString *password) {
    size_t len;
    const char *ptr;

    if (TSGlobalConfig.passwordStr) {
        RedisModule_FreeString(NULL, TSGlobalConfig.passwordStr);
    }
    free(TSGlobalConfig.password);

    TSGlobalConfig.passwordStr = RedisModule_CreateStringFromString(NULL, password);
    ptr = RedisModule_StringPtrLen(TSGlobalConfig.passwordStr, &len);
    if (len == 0) {
        TSGlobalConfig.password = NULL;
    } else {
        TSGlobalConfig.password = rmalloc_strndup(ptr, len);
    }
}

// Legacy configuration via module arguments
int ReadConfig(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc > 1 && RMUtil_ArgIndex("COMPACTION_POLICY", argv, argc) >= 0) {
        RedisModuleString *policy;
        const char *policy_cstr;
        size_t len;
        SimpleCompactionRule *compactionRules;
        uint64_t compactionRulesCount;

        if (RMUtil_ParseArgsAfter("COMPACTION_POLICY", argv, argc, "s", &policy) !=
            REDISMODULE_OK) {
            RedisModule_Log(ctx, "warning", "Unable to parse argument after COMPACTION_POLICY");
            return TSDB_ERROR;
        }
        policy_cstr = RedisModule_StringPtrLen(policy, &len);
        if (ParseCompactionPolicy(policy_cstr, &compactionRules, &compactionRulesCount) != TRUE) {
            RedisModule_Log(ctx, "warning", "Unable to parse argument after COMPACTION_POLICY");
            return TSDB_ERROR;
        }

        SetCompactionRulesConfig(policy, compactionRules, compactionRulesCount);
        RedisModule_Log(ctx, "notice", "loaded default compaction policy: %s", policy_cstr);
    }

    if (argc > 1 && RMUtil_ArgIndex("OSS_GLOBAL_PASSWORD", argv, argc) >= 0) {
        RedisModuleString *password;
        if (RMUtil_ParseArgsAfter("OSS_GLOBAL_PASSWORD", argv, argc, "s", &password) !=
            REDISMODULE_OK) {
            RedisModule_Log(ctx, "warning", "Unable to parse argument after OSS_GLOBAL_PASSWORD");
            return TSDB_ERROR;
        }

        SetOSSGlobalPasswordConfig(password);
        RedisModule_Log(ctx, "notice", "loaded tls password");
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
    }

    if (!ValidateChunkSize(ctx, Chunk_SIZE_BYTES_SECS)) {
        return TSDB_ERROR;
    }
    if (ParseChunkSize(ctx, argv, argc, "CHUNK_SIZE_BYTES", &TSGlobalConfig.chunkSizeBytes) !=
        REDISMODULE_OK) {
        RedisModule_Log(ctx, "warning", "Unable to parse argument after CHUNK_SIZE_BYTES");
        return TSDB_ERROR;
    }
    RedisModule_Log(ctx,
                    "notice",
                    "loaded default CHUNK_SIZE_BYTES policy: %lld",
                    TSGlobalConfig.chunkSizeBytes);

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
    }

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
