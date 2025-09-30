#pragma once

#include "redismodule.h"
/* Register documentation/metadata for Timeseries commands */
int RegisterTSCommandInfos(RedisModuleCtx *ctx);