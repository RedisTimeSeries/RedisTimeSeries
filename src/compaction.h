/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/
#ifndef COMPACTION_H
#define COMPACTION_H
#include <sys/types.h>
#include "redismodule.h"
#include "consts.h"
#include <rmutil/util.h>

typedef struct AggregationClass
{
    void *(*createContext)();
    void(*freeContext)(void *context);
    void(*appendValue)(void *context, double value);
    void(*resetContext)(void *context);
    void(*writeContext)(void *context, RedisModuleIO *io);
    void(*readContext)(void *context, RedisModuleIO *io);
    int(*finalize)(void *context, double *value);
} AggregationClass;

AggregationClass *GetAggClass(TS_AGG_TYPES_T aggType);
int StringAggTypeToEnum(const char *agg_type);
int RMStringLenAggTypeToEnum(RedisModuleString *aggTypeStr);
int StringLenAggTypeToEnum(const char *agg_type, size_t len);
const char *AggTypeEnumToString(TS_AGG_TYPES_T aggType);

#endif
