
#include "RedisModulesSDK/redismodule.h"
#include "query_language.h"

#ifndef REDIS_TIMESERIES_CLEAN_GEARS_COMMANDS_H
#define REDIS_TIMESERIES_CLEAN_GEARS_COMMANDS_H

typedef struct MRangeData
{
    RedisModuleBlockedClient *bc;
    MRangeArgs args;
} MRangeData;

int TSDB_mget_RG(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int TSDB_queryindex_RG(RedisModuleCtx *ctx, QueryPredicateList *queries);
int TSDB_mrange_RG(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool reverse);

#endif // REDIS_TIMESERIES_CLEAN_GEARS_COMMANDS_H
