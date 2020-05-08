/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/
#ifndef CONFIG_H
#define CONFIG_H

#include "redismodule.h"
#include "parse_policies.h"
#include <stdbool.h>

typedef struct {
    SimpleCompactionRule *compactionRules;
    size_t compactionRulesCount;
    long long retentionPolicy;
    long long maxSamplesPerChunk;
    short options;
    int hasGlobalConfig;
} TSConfig;

extern TSConfig TSGlobalConfig;

int ReadConfig(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

typedef struct RedisVersion {
  int redisMajorVersion;
  int redisMinorVersion;
  int redisPatchVersion;
} RedisVersion;

extern RedisVersion currVersion;
extern RedisVersion supportedVersion;

extern int timeseriesRlecMajorVersion;
extern int timeseriesRlecMinorVersion;
extern int timeseriesRlecPatchVersion;
extern int timeseriesRlecBuild;

extern bool timeseriesIsCrdt;

static inline int IsEnterprise() { return timeseriesRlecMajorVersion != -1; }

int TimeSeriesCheckSupportedVestion();
void TimeSeriesGetRedisVersion();
#endif
