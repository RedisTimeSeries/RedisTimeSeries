#include <ctype.h>
#include <string.h>
#include "compaction.h"
#include "rmutil/alloc.h"

typedef struct MaxMinContext {
    double value;
    char isResetted;
} MaxMinContext;

typedef struct AvgContext {
    double val;
    double cnt;
} AvgContext;

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
    context->value = 0;
    context->isResetted = TRUE;
    return context;
}

void MaxAppendValue(void *contextPtr, double value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    if (context->isResetted) {
        context->isResetted = FALSE;
        context->value = value;
    } else if (value > context->value) {
        context->value = value;
    }
}

double MaxMinFinalize(void *contextPtr) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    return context->value;
}

void MaxMinReset(void *contextPtr) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    context->value = 0;
    context->isResetted = TRUE;
}

void MinAppendValue(void *contextPtr, double value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    if (context->isResetted) {
        context->isResetted = FALSE;
        context->value = value;
    } else if (value < context->value) {
        context->value = value;
    }
}

void MaxMinWriteContext(void *contextPtr, RedisModuleIO * io) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    RedisModule_SaveDouble(io, context->value);
    RedisModule_SaveStringBuffer(io, &context->isResetted, 1);
}

void MaxMinReadContext(void *contextPtr, RedisModuleIO * io) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    size_t len = 1;
    context->value = RedisModule_LoadDouble(io);
    context->isResetted = RedisModule_LoadStringBuffer(io, &len)[0];
}
void SumAppendValue(void *contextPtr, double value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    context->value += value;
}

void CountAppendValue(void *contextPtr, double value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    context->value++;
}

void FirstAppendValue(void *contextPtr, double value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    if (context->isResetted) {
        context->isResetted = FALSE;
        context->value = value;
    }
}

void LastAppendValue(void *contextPtr, double value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    context->value = value;
}

static AggregationClass aggMax = {
    .createContext = MaxMinCreateContext,
    .appendValue = MaxAppendValue,
    .freeContext = rm_free,
    .finalize = MaxMinFinalize,
    .writeContext = MaxMinWriteContext,
    .readContext = MaxMinReadContext,
    .resetContext = MaxMinReset
};

static AggregationClass aggMin = {
    .createContext = MaxMinCreateContext,
    .appendValue = MinAppendValue,
    .freeContext = rm_free,
    .finalize = MaxMinFinalize,
    .writeContext = MaxMinWriteContext,
    .readContext = MaxMinReadContext,
    .resetContext = MaxMinReset
};

static AggregationClass aggSum = {
    .createContext = MaxMinCreateContext,
    .appendValue = SumAppendValue,
    .freeContext = rm_free,
    .finalize = MaxMinFinalize,
    .writeContext =  MaxMinWriteContext,
    .readContext = MaxMinReadContext,
    .resetContext = MaxMinReset
};

static AggregationClass aggCount = {
    .createContext = MaxMinCreateContext,
    .appendValue = CountAppendValue,
    .freeContext = rm_free,
    .finalize = MaxMinFinalize,
    .writeContext = MaxMinWriteContext,
    .readContext = MaxMinReadContext,
    .resetContext = MaxMinReset
};

static AggregationClass aggFirst = {
    .createContext = MaxMinCreateContext,
    .appendValue = FirstAppendValue,
    .freeContext = rm_free,
    .finalize = MaxMinFinalize,
    .writeContext = MaxMinWriteContext,
    .readContext = MaxMinReadContext,
    .resetContext = MaxMinReset
};

static AggregationClass aggLast = {
    .createContext = MaxMinCreateContext,
    .appendValue = LastAppendValue,
    .freeContext = rm_free,
    .finalize = MaxMinFinalize,
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
        default:
            return NULL;
    }
}