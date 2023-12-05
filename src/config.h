/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
 */
#ifndef CONFIG_H
#define CONFIG_H

#include "parse_policies.h"
#include "RedisModulesSDK/redismodule.h"

#include <stdbool.h>

typedef struct
{
    SimpleCompactionRule *compactionRules;
    uint64_t compactionRulesCount;
    long long retentionPolicy;
    long long chunkSizeBytes;
    short options;
    int hasGlobalConfig;
    DuplicatePolicy duplicatePolicy;
    long long numThreads;        // number of threads used by libMR
    bool forceSaveCrossRef;      // Internal debug configuration param
    char *password;              // tls password which used by libmr
    bool dontAssertOnFailiure;   // Internal debug configuration param
    long long ignoreMaxTimeDiff; // Insert filter max time diff with the last sample
    double ignoreMaxValDiff;     // Insert filter max value diff with the last sample
} TSConfig;

extern TSConfig TSGlobalConfig;

int ReadConfig(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
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

static inline int RTS_IsEnterprise() {
    return RTS_RlecMajorVersion != -1;
}

int RTS_CheckSupportedVestion();
void RTS_GetRedisVersion();
#endif
