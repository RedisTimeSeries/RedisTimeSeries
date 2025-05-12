/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef CONFIG_H
#define CONFIG_H

#include "parse_policies.h"
#include "RedisModulesSDK/redismodule.h"

#include <stdbool.h>

#define DEFAULT_NUM_THREADS 3
#define NUM_THREADS_MIN 1
#define NUM_THREADS_MAX 16
#define RETENTION_POLICY_MIN 0
#define RETENTION_POLICY_MAX LLONG_MAX
#define CHUNK_SIZE_BYTES_MIN 48
#define CHUNK_SIZE_BYTES_MAX 1048576
#define IGNORE_MAX_TIME_DIFF_MIN 0
#define IGNORE_MAX_TIME_DIFF_MAX LLONG_MAX
#define IGNORE_MAX_VAL_DIFF_MIN 0.0
#define IGNORE_MAX_VAL_DIFF_MAX DBL_MAX

typedef struct
{
    SimpleCompactionRule *compactionRules;
    uint64_t compactionRulesCount;
    long long retentionPolicy;
    long long chunkSizeBytes;
    short options;
    DuplicatePolicy duplicatePolicy;
    long long numThreads;        // number of threads used by libMR
    bool forceSaveCrossRef;      // Internal debug configuration param
    char *password;              // tls password which used by libmr
    bool dontAssertOnFailure;    // Internal debug configuration param
    long long ignoreMaxTimeDiff; // Insert filter max time diff with the last sample
    double ignoreMaxValDiff;     // Insert filter max value diff with the last sample
} TSConfig;

extern TSConfig TSGlobalConfig;

void InitConfig(void);
void FreeConfig(void);
RedisModuleString *GlobalConfigToString(RedisModuleCtx *ctx);
bool RegisterConfigurationOptions(RedisModuleCtx *ctx);
int ReadDeprecatedLoadTimeConfig(RedisModuleCtx *ctx,
                                 RedisModuleString **argv,
                                 int argc,
                                 const bool showDeprecationWarning);
const char *ChunkTypeToString(int options);
typedef struct RTS_RedisVersion
{
    int redisMajorVersion;
    int redisMinorVersion;
    int redisPatchVersion;
} RTS_RedisVersion;

extern RTS_RedisVersion RTS_currVersion;
extern RTS_RedisVersion RTS_minSupportedVersion;

extern int RTS_RlecMajorVersion;
extern int RTS_RlecMinorVersion;
extern int RTS_RlecPatchVersion;
extern int RTS_RlecBuild;

static inline int RTS_IsEnterprise(void) {
    return RTS_RlecMajorVersion != -1;
}

/*
 * Returns true if the current version of Redis supports the module
 * configuration API.
 */
static inline bool RTS_RedisSupportsModuleConfigApi(void) {
    return (RTS_currVersion.redisMajorVersion > 7 ||
            (RTS_currVersion.redisMajorVersion == 7 && RTS_currVersion.redisMinorVersion >= 9)) &&
           RedisModule_RegisterEnumConfig && RedisModule_RegisterBoolConfig &&
           RedisModule_RegisterStringConfig && RedisModule_RegisterNumericConfig &&
           RedisModule_LoadConfigs;
}

int RTS_CheckSupportedVestion(void);
void RTS_GetRedisVersion(void);

#endif
