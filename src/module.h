/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/
#ifndef MODULE_H
#define MODULE_H

#include "redismodule.h"
#include "tsdb.h"

extern RedisModuleType *SeriesType;

// Create a new TS key, if key is NULL the function will open the key, the user must call to RedisModule_CloseKey
// The function assumes the key doesn't exists
int CreateTsKey(RedisModuleCtx *ctx, RedisModuleString *keyName,
                CreateCtx *cCtx, Series **series, RedisModuleKey **key);

// This method provides the same logic as GetSeries, without replying to the client in case of error
// The caller method should check the result for TRUE/FALSE and update the client accordingly if required
int SilentGetSeries(RedisModuleCtx *ctx, RedisModuleString *keyName, RedisModuleKey **key, Series **series, int mode);

int GetSeries(RedisModuleCtx *ctx, RedisModuleString *keyName, RedisModuleKey **key, Series **series, int mode);

#endif
