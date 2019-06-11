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
int CreateTsKey(RedisModuleCtx *ctx, RedisModuleString *keyName, Label *labels, size_t labelsCounts,
                long long retentionSecs, long long maxSamplesPerChunk, Series **series, RedisModuleKey **key);

int GetSeries(RedisModuleCtx *ctx, RedisModuleString *keyName, Series **series);


#endif
