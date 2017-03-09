#ifndef COMPACTION_H
#define COMPACTION_H
#include <sys/types.h>


#define AGG_NONE 0
#define AGG_MIN 1
#define AGG_MAX 2
#define AGG_SUM 3
#define AGG_AVG 4
#define AGG_COUNT 5

#define TRUE 1
#define FALSE 0

typedef struct AggregationClass
{
    void *(*createContext)();
    void(*freeContext)(void *context);
    void(*appendValue)(void *context, double value);
    void(*resetContext)(void *context);
    double(*finalize)(void *context);
} AggregationClass;

AggregationClass* GetAggClass(int aggType);

#endif