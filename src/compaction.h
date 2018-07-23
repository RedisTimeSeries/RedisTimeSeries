#ifndef COMPACTION_H
#define COMPACTION_H
#include <sys/types.h>
#include "redismodule.h"
#include "consts.h"
#include <rmutil/util.h>


#define AGG_NONE 0
#define AGG_MIN 1
#define AGG_MAX 2
#define AGG_SUM 3
#define AGG_AVG 4
#define AGG_COUNT 5
#define AGG_FIRST 6
#define AGG_LAST 7


typedef struct AggregationClass
{
    void *(*createContext)();
    void(*freeContext)(void *context);
    void(*appendValue)(void *context, double value);
    void(*resetContext)(void *context);
    void(*writeContext)(void *context, RedisModuleIO * io);
    void(*readContext)(void *context, RedisModuleIO *io);
    double(*finalize)(void *context);
} AggregationClass;

AggregationClass* GetAggClass(int aggType);
int StringAggTypeToEnum(const char *agg_type);
int RMStringLenAggTypeToEnum(RedisModuleString *aggTypeStr);
int StringLenAggTypeToEnum(const char *agg_type, size_t len);
const char * AggTypeEnumToString(int aggType);

#endif