
#include "RedisModulesSDK/redismodule.h"

#ifndef REDIS_TIMESERIES_CLEAN_GEARS_INTEGRATION_H
#define REDIS_TIMESERIES_CLEAN_GEARS_INTEGRATION_H

int TSDB_mget_RG(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int register_rg(RedisModuleCtx *ctx);

#endif //REDIS_TIMESERIES_CLEAN_GEARS_INTEGRATION_H
