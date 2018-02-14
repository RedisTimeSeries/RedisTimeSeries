#ifndef MODULE_H
#define MODULE_H

#include "redismodule.h"
#include "tsdb.h"

extern RedisModuleType *SeriesType;

// Create a new TS key, if key is NULL the function will open the key, the user must call to RedisModule_CloseKey
// The function assumes the key doesn't exists
int CreateTsKey(RedisModuleCtx *ctx, RedisModuleString *keyName, long long retentionSecs,
                long long maxSamplesPerChunk, Series **series, RedisModuleKey **key);

#endif