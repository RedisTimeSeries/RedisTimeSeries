/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/
#ifndef CONFIG_H
#define CONFIG_H

#include "redismodule.h"
#include "parse_policies.h"
#include "search.h"

typedef struct {
    SimpleCompactionRule *compactionRules;
    size_t compactionRulesCount;
    long long retentionPolicy;
    long long maxSamplesPerChunk;
    int hasGlobalConfig;
    RSLiteIndex *globalRSIndex;
} TSConfig;

extern TSConfig TSGlobalConfig;

int ReadTSConfig(RedisModuleString **argv, int argc);
#endif
