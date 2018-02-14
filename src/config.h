#ifndef CONFIG_H
#define CONFIG_H

#include "redismodule.h"
#include "parse_policies.h"

typedef struct {
    SimpleCompactionRule *compactionRules;
    size_t compactionRulesCount;
    long long retentionPolicy;
    long long maxSamplesPerChunk;
    int hasGlobalConfig;
} TSConfig;

extern TSConfig TSGlobalConfig;

int ReadConfig(RedisModuleString **argv, int argc);
#endif
