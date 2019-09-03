/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#pragma once

#include "RedisModulesSDK/redismodule.h"
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

int ReadTSConfig(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
