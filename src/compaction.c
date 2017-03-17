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

static AggregationClass aggAvg = {
    .createContext = AvgCreateContext,
    .appendValue = AvgAddValue,
    .freeContext = free,
    .finalize = AvgFinalize,
    .resetContext = AvgReset
};

void *MaxMinCreateContext() {
    MaxMinContext *context = (MaxMinContext *)malloc(sizeof(MaxMinContext));
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

void SumAppendValue(void *contextPtr, double value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    context->value += value;
}

void CountAppendValue(void *contextPtr, double value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    context->value++;
}

static AggregationClass aggMax = {
    .createContext = MaxMinCreateContext,
    .appendValue = MaxAppendValue,
    .freeContext = free,
    .finalize = MaxMinFinalize,
    .resetContext = MaxMinReset
};

static AggregationClass aggMin = {
    .createContext = MaxMinCreateContext,
    .appendValue = MinAppendValue,
    .freeContext = free,
    .finalize = MaxMinFinalize,
    .resetContext = MaxMinReset
};

static AggregationClass aggSum = {
    .createContext = MaxMinCreateContext,
    .appendValue = SumAppendValue,
    .freeContext = free,
    .finalize = MaxMinFinalize,
    .resetContext = MaxMinReset
};

static AggregationClass aggCount = {
    .createContext = MaxMinCreateContext,
    .appendValue = CountAppendValue,
    .freeContext = free,
    .finalize = MaxMinFinalize,
    .resetContext = MaxMinReset
};

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
        default:
            return NULL;
    }
}