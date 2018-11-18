#include <ctype.h>
#include <string.h>
#include "compaction.h"
#include "rmutil/alloc.h"

typedef struct MaxMinContext {
    double minValue;
    double maxValue;
    char isResetted;
} MaxMinContext;

typedef struct SingleValueContext {
    double value;
    char isResetted;
} SingleValueContext;

typedef struct AvgContext {
    double val;
    double cnt;
} AvgContext;

void *SingleValueCreateContext() {
    SingleValueContext *context = (SingleValueContext*)malloc(sizeof(SingleValueContext));
    context->value = 0;
    context->isResetted = TRUE;
    return context;
}

void SingleValueReset(void *contextPtr) {
    SingleValueContext *context = (SingleValueContext*)contextPtr;
    context->value = 0;
    context->isResetted = TRUE;
}

double SingleValueFinlize(void *contextPtr) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    return context->value;
}

void SingleValueWriteContext(void *contextPtr, RedisModuleIO * io) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    RedisModule_SaveDouble(io, context->value);
}

void SingleValueReadContext(void *contextPtr, RedisModuleIO * io){
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    context->value = RedisModule_LoadDouble(io);
}

void *AvgCreateContext() {
    AvgContext *context = (AvgContext*)malloc(sizeof(AvgContext));
    context->cnt = 0;
    context->val =0;
    return context;
}

void AvgAddValue(void *contextPtr, double value){
    AvgContext *context = (AvgContext *)contextPtr;
    context->val += value;
    context->cnt++;
}

double AvgFinalize(void *contextPtr) {
    AvgContext *context = (AvgContext *)contextPtr;
    return context->val / context->cnt;
}

void AvgReset(void *contextPtr) {
    AvgContext *context = (AvgContext *)contextPtr;
    context->val = 0;
    context->cnt = 0;
}

void AvgWriteContext(void *contextPtr, RedisModuleIO * io) {
    AvgContext *context = (AvgContext *)contextPtr;
    RedisModule_SaveDouble(io, context->val);
    RedisModule_SaveDouble(io, context->cnt);
}

void AvgReadContext(void *contextPtr, RedisModuleIO * io){
    AvgContext *context = (AvgContext *)contextPtr;
    context->val = RedisModule_LoadDouble(io);
    context->cnt = RedisModule_LoadDouble(io);
}

void rm_free(void* ptr) {
    free(ptr);
}

static AggregationClass aggAvg = {
    .createContext = AvgCreateContext,
    .appendValue = AvgAddValue,
    .freeContext = rm_free,
    .finalize = AvgFinalize,
    .writeContext = AvgWriteContext,
    .readContext = AvgReadContext,
    .resetContext = AvgReset
};

void *MaxMinCreateContext() {
    MaxMinContext *context = (MaxMinContext *)malloc(sizeof(MaxMinContext));
    context->minValue = 0;
    context->maxValue = 0;
    context->isResetted = TRUE;
    return context;
}

void MaxMinAppendValue(void *contextPtr, double value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    if (context->isResetted) {
        context->isResetted = FALSE;
        context->maxValue = value;
        context->minValue = value;
    } else {
        if (value > context->maxValue) {
            context->maxValue = value;
        }
        if (value < context->minValue) {
            context->minValue = value;
        }
    }
}

double MaxFinalize(void *contextPtr) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    return context->maxValue;
}

double MinFinalize(void *contextPtr) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    return context->minValue;
}

double RangeFinalize(void *contextPtr) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    return context->maxValue - context->minValue;
}

void MaxMinReset(void *contextPtr) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    context->maxValue = 0;
    context->minValue = 0;
    context->isResetted = TRUE;
}

void MaxMinWriteContext(void *contextPtr, RedisModuleIO * io) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    RedisModule_SaveDouble(io, context->maxValue);
    RedisModule_SaveDouble(io, context->minValue);
    RedisModule_SaveStringBuffer(io, &context->isResetted, 1);
}

void MaxMinReadContext(void *contextPtr, RedisModuleIO * io) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    size_t len = 1;
    context->maxValue = RedisModule_LoadDouble(io);
    context->minValue = RedisModule_LoadDouble(io);
    context->isResetted = RedisModule_LoadStringBuffer(io, &len)[0];
}

void SumAppendValue(void *contextPtr, double value) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    context->value += value;
}

void CountAppendValue(void *contextPtr, double value) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    context->value++;
}

void FirstAppendValue(void *contextPtr, double value) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    if (context->isResetted) {
        context->isResetted = FALSE;
        context->value = value;
    }
}

void LastAppendValue(void *contextPtr, double value) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    context->value = value;
}

static AggregationClass aggMax = {
    .createContext = MaxMinCreateContext,
    .appendValue = MaxMinAppendValue,
    .freeContext = rm_free,
    .finalize = MaxFinalize,
    .writeContext = MaxMinWriteContext,
    .readContext = MaxMinReadContext,
    .resetContext = MaxMinReset
};

static AggregationClass aggMin = {
    .createContext = MaxMinCreateContext,
    .appendValue = MaxMinAppendValue,
    .freeContext = rm_free,
    .finalize = MinFinalize,
    .writeContext = MaxMinWriteContext,
    .readContext = MaxMinReadContext,
    .resetContext = MaxMinReset
};

static AggregationClass aggSum = {
    .createContext = SingleValueCreateContext,
    .appendValue = SumAppendValue,
    .freeContext = rm_free,
    .finalize = SingleValueFinlize,
    .writeContext =  SingleValueWriteContext,
    .readContext = SingleValueReadContext,
    .resetContext = SingleValueReset
};

static AggregationClass aggCount = {
    .createContext = SingleValueCreateContext,
    .appendValue = CountAppendValue,
    .freeContext = rm_free,
    .finalize = SingleValueFinlize,
    .writeContext =  SingleValueWriteContext,
    .readContext = SingleValueReadContext,
    .resetContext = SingleValueReset
};

static AggregationClass aggFirst = {
    .createContext = SingleValueCreateContext,
    .appendValue = FirstAppendValue,
    .freeContext = rm_free,
    .finalize = SingleValueFinlize,
    .writeContext =  SingleValueWriteContext,
    .readContext = SingleValueReadContext,
    .resetContext = SingleValueReset
};

static AggregationClass aggLast = {
    .createContext = SingleValueCreateContext,
    .appendValue = LastAppendValue,
    .freeContext = rm_free,
    .finalize = SingleValueFinlize,
    .writeContext =  SingleValueWriteContext,
    .readContext = SingleValueReadContext,
    .resetContext = SingleValueReset
};

static AggregationClass aggRange = {
    .createContext = MaxMinCreateContext,
    .appendValue = MaxMinAppendValue,
    .freeContext = rm_free,
    .finalize = RangeFinalize,
    .writeContext = MaxMinWriteContext,
    .readContext = MaxMinReadContext,
    .resetContext = MaxMinReset
};

int StringAggTypeToEnum(const char *agg_type) {
    return StringLenAggTypeToEnum(agg_type, strlen(agg_type));
}

int RMStringLenAggTypeToEnum(RedisModuleString *aggTypeStr) {
    size_t str_len;
    const char *aggTypeCStr = RedisModule_StringPtrLen(aggTypeStr, &str_len);
    return StringLenAggTypeToEnum(aggTypeCStr, str_len);
}

int StringLenAggTypeToEnum(const char *agg_type, size_t len) {
    char agg_type_lower[10];
    int result;

    for(int i = 0; i < len; i++){
        agg_type_lower[i] = tolower(agg_type[i]);
    }
    if (strncmp(agg_type_lower, "min", len) == 0){
        result = TS_AGG_MIN;
    } else if (strncmp(agg_type_lower, "max", len) == 0) {
        result =  TS_AGG_MAX;
    } else if (strncmp(agg_type_lower, "sum", len) == 0) {
        result =  TS_AGG_SUM;
    } else if (strncmp(agg_type_lower, "avg", len) == 0) {
        result =  TS_AGG_AVG;
    } else if (strncmp(agg_type_lower, "count", len) == 0) {
        result =  TS_AGG_COUNT;
    } else if (strncmp(agg_type_lower, "first", len) == 0) {
        result =  TS_AGG_FIRST;
    } else if (strncmp(agg_type_lower, "last", len) == 0) {
        result =  TS_AGG_LAST;
    } else if (strncmp(agg_type_lower, "range", len) == 0) {
        result =  TS_AGG_RANGE;
    } else {
        result =  TS_AGG_INVALID;
    }

    return result;
}

const char * AggTypeEnumToString(int aggType) {
    switch (aggType) {
        case TS_AGG_MIN:
            return "MIN";
        case TS_AGG_MAX:
            return "MAX";
        case TS_AGG_SUM:
            return "SUM";
        case TS_AGG_AVG:
            return "AVG";
        case TS_AGG_COUNT:
            return "COUNT";
        case TS_AGG_FIRST:
            return "FIRST";
        case TS_AGG_LAST:
            return "LAST";
        case TS_AGG_RANGE:
            return "RANGE";
        default:
            return "Unknown";
    }
}

AggregationClass* GetAggClass(int aggType) {
    switch (aggType) {
        case AGG_NONE:
            return NULL;
            break;
        case AGG_MIN:
            return &aggMin;
            break;
        case AGG_MAX:
            return &aggMax;
        case AGG_AVG:
            return &aggAvg;
            break;
        case AGG_SUM:
            return &aggSum;
            break;
        case AGG_COUNT:
            return &aggCount;
            break;
        case AGG_FIRST:
            return &aggFirst;
            break;
        case AGG_LAST:
            return &aggLast;
            break;
        case TS_AGG_RANGE:
            return &aggRange;
        default:
            return NULL;
    }
}