/*
 * Copyright 2018-2019 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#include "compaction.h"

#include "load_io_error_macros.h"
#include "rdb.h"

#include "rmutil/alloc.h"
#include "compactions/compaction_common.h"

#include <ctype.h>
#include <float.h>
#include <math.h> // sqrt
#include <string.h>

#ifdef _DEBUG
#include "valgrind/valgrind.h"
#endif

typedef struct SingleValueContext
{
    double value;
    char isResetted;
} SingleValueContext;

typedef struct AvgContext
{
    double val;
    double cnt;
    bool isOverflow;
} AvgContext;

typedef struct StdContext
{
    double sum;
    double sum_2; // sum of (values^2)
    u_int64_t cnt;
} StdContext;

void *SingleValueCreateContext() {
    SingleValueContext *context = (SingleValueContext *)malloc(sizeof(SingleValueContext));
    context->value = 0;
    context->isResetted = TRUE;
    return context;
}

void SingleValueReset(void *contextPtr) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    context->value = 0;
    context->isResetted = TRUE;
}

void SingleValueFinalize(void *contextPtr, double *val) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    *val = context->value;
}

void SingleValueWriteContext(void *contextPtr, RedisModuleIO *io) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    RedisModule_SaveDouble(io, context->value);
    RedisModule_SaveUnsigned(io, context->isResetted);
}

int SingleValueReadContext(void *contextPtr, RedisModuleIO *io, int encver) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    context->value = LoadDouble_IOError(io, goto err);
    if (encver >= TS_IS_RESSETED_DUP_POLICY_RDB_VER) {
        context->isResetted = LoadUnsigned_IOError(io, goto err);
    }
    return TSDB_OK;
err:
    return TSDB_ERROR;
}

void *AvgCreateContext() {
    AvgContext *context = (AvgContext *)malloc(sizeof(AvgContext));
    context->cnt = 0;
    context->val = 0;
    context->isOverflow = false;
    return context;
}

// Except valgrind it's equivalent to sizeof(long double) > 8
#if !defined(_DEBUG) && !defined(_VALGRIND)
bool hasLongDouble = sizeof(long double) > 8;
#else
bool hasLongDouble = false;
#endif

void AvgAddValue(void *contextPtr, double value) {
    AvgContext *context = (AvgContext *)contextPtr;
    context->cnt++;

    // Test for overflow
    if (unlikely(((context->val < 0.0) == (value < 0.0) &&
                  (fabs(context->val) > (DBL_MAX - fabs(value)))) ||
                 context->isOverflow)) {
        // calculating: avg(t+1) = t*avg(t)/(t+1) + val/(t+1)

        long double ld_val = context->val;
        long double ld_value = value;
        if (likely(hasLongDouble)) { // better accuracy
            ld_val /= context->cnt;
            if (context->isOverflow) {
                ld_val *= (long double)(context->cnt - 1);
            }
        } else {
            if (context->isOverflow) {
                ld_val *= ((long double)(context->cnt - 1) / context->cnt);
            } else {
                ld_val /= context->cnt;
            }
        }
        ld_val += (ld_value / (long double)context->cnt);
        context->val = ld_val;
        context->isOverflow = true;
    } else { // No Overflow
        context->val += value;
    }
}

void AvgFinalize(void *contextPtr, double *value) {
    AvgContext *context = (AvgContext *)contextPtr;
    if (unlikely(context->isOverflow)) {
        *value = context->val;
    } else {
        *value = context->val / context->cnt;
    }
}

void AvgReset(void *contextPtr) {
    AvgContext *context = (AvgContext *)contextPtr;
    context->val = 0;
    context->cnt = 0;
    context->isOverflow = false;
}

void AvgWriteContext(void *contextPtr, RedisModuleIO *io) {
    AvgContext *context = (AvgContext *)contextPtr;
    RedisModule_SaveDouble(io, context->val);
    RedisModule_SaveDouble(io, context->cnt);
    RedisModule_SaveUnsigned(io, context->isOverflow);
}

int AvgReadContext(void *contextPtr, RedisModuleIO *io, int encver) {
    AvgContext *context = (AvgContext *)contextPtr;
    context->val = LoadDouble_IOError(io, goto err);
    context->cnt = LoadDouble_IOError(io, goto err);
    context->isOverflow = false;
    if (encver >= TS_OVERFLOW_RDB_VER) {
        context->isOverflow = !!(LoadUnsigned_IOError(io, goto err));
    }
    return TSDB_OK;
err:
    return TSDB_ERROR;
}

void *StdCreateContext() {
    StdContext *context = (StdContext *)malloc(sizeof(StdContext));
    context->cnt = 0;
    context->sum = 0;
    context->sum_2 = 0;
    return context;
}

void StdAddValue(void *contextPtr, double value) {
    StdContext *context = (StdContext *)contextPtr;
    ++context->cnt;
    context->sum += value;
    context->sum_2 += value * value;
}

static inline double variance(double sum, double sum_2, double count) {
    if (count == 0) {
        return 0;
    }

    /*  var(X) = sum((x_i - E[X])^2)
     *  = sum(x_i^2) - 2 * sum(x_i) * E[X] + E^2[X] */
    return (sum_2 - 2 * sum * sum / count + pow(sum / count, 2) * count) / count;
}

void VarPopulationFinalize(void *contextPtr, double *value) {
    StdContext *context = (StdContext *)contextPtr;
    uint64_t count = context->cnt;
    *value = variance(context->sum, context->sum_2, count);
}

void VarSamplesFinalize(void *contextPtr, double *value) {
    StdContext *context = (StdContext *)contextPtr;
    uint64_t count = context->cnt;
    if (count == 1) {
        *value = 0;
    } else {
        *value = variance(context->sum, context->sum_2, count) * count / (count - 1);
    }
}

void StdPopulationFinalize(void *contextPtr, double *value) {
    double val;
    VarPopulationFinalize(contextPtr, &val);
    *value = sqrt(val);
}

void StdSamplesFinalize(void *contextPtr, double *value) {
    double val;
    VarSamplesFinalize(contextPtr, &val);
    *value = sqrt(val);
}

void StdReset(void *contextPtr) {
    StdContext *context = (StdContext *)contextPtr;
    context->cnt = 0;
    context->sum = 0;
    context->sum_2 = 0;
}

void StdWriteContext(void *contextPtr, RedisModuleIO *io) {
    StdContext *context = (StdContext *)contextPtr;
    RedisModule_SaveDouble(io, context->sum);
    RedisModule_SaveDouble(io, context->sum_2);
    RedisModule_SaveUnsigned(io, context->cnt);
}

int StdReadContext(void *contextPtr, RedisModuleIO *io, REDISMODULE_ATTR_UNUSED int encver) {
    StdContext *context = (StdContext *)contextPtr;
    context->sum = LoadDouble_IOError(io, goto err);
    context->sum_2 = LoadDouble_IOError(io, goto err);
    context->cnt = LoadUnsigned_IOError(io, goto err);
    return TSDB_OK;
err:
    return TSDB_ERROR;
}

void rm_free(void *ptr) {
    free(ptr);
}

static AggregationClass aggAvg = { .createContext = AvgCreateContext,
                                   .appendValue = AvgAddValue,
                                   .appendValueVec = NULL, /* determined on run time */
                                   .freeContext = rm_free,
                                   .finalize = AvgFinalize,
                                   .writeContext = AvgWriteContext,
                                   .readContext = AvgReadContext,
                                   .resetContext = AvgReset };

static AggregationClass aggStdP = { .createContext = StdCreateContext,
                                    .appendValue = StdAddValue,
                                    .appendValueVec = NULL, /* determined on run time */
                                    .freeContext = rm_free,
                                    .finalize = StdPopulationFinalize,
                                    .writeContext = StdWriteContext,
                                    .readContext = StdReadContext,
                                    .resetContext = StdReset };

static AggregationClass aggStdS = { .createContext = StdCreateContext,
                                    .appendValue = StdAddValue,
                                    .appendValueVec = NULL, /* determined on run time */
                                    .freeContext = rm_free,
                                    .finalize = StdSamplesFinalize,
                                    .writeContext = StdWriteContext,
                                    .readContext = StdReadContext,
                                    .resetContext = StdReset };

static AggregationClass aggVarP = { .createContext = StdCreateContext,
                                    .appendValue = StdAddValue,
                                    .appendValueVec = NULL, /* determined on run time */
                                    .freeContext = rm_free,
                                    .finalize = VarPopulationFinalize,
                                    .writeContext = StdWriteContext,
                                    .readContext = StdReadContext,
                                    .resetContext = StdReset };

static AggregationClass aggVarS = { .createContext = StdCreateContext,
                                    .appendValue = StdAddValue,
                                    .appendValueVec = NULL, /* determined on run time */
                                    .freeContext = rm_free,
                                    .finalize = VarSamplesFinalize,
                                    .writeContext = StdWriteContext,
                                    .readContext = StdReadContext,
                                    .resetContext = StdReset };

void *MaxMinCreateContext() {
    MaxMinContext *context = (MaxMinContext *)malloc(sizeof(MaxMinContext));
    context->minValue = DBL_MAX;
    context->maxValue = ((double)-1.0) * DBL_MAX;
    return context;
}

void MaxAppendValue(void *context, double value) {
    _AssignIfGreater(&((MaxMinContext *)context)->maxValue, &value);
}

void MaxAppendValuesVec(void *__restrict__ context,
                        double *__restrict__ values,
                        size_t si,
                        size_t ei) {
    for (int i = si; i <= ei; ++i) {
        _AssignIfGreater(&((MaxMinContext *)context)->maxValue, &values[i]);
    }
}

void MinAppendValue(void *contextPtr, double value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    if (value < context->minValue) {
        context->minValue = value;
    }
}

void MaxMinAppendValue(void *contextPtr, double value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    if (value > context->maxValue) {
        context->maxValue = value;
    }
    if (value < context->minValue) {
        context->minValue = value;
    }
}

void MaxFinalize(void *contextPtr, double *value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    *value = context->maxValue;
}

void MinFinalize(void *contextPtr, double *value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    *value = context->minValue;
}

void RangeFinalize(void *contextPtr, double *value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    *value = context->maxValue - context->minValue;
}

void MaxMinReset(void *contextPtr) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    context->minValue = DBL_MAX;
    context->maxValue = ((double)-1.0) * DBL_MAX;
}

void MaxMinWriteContext(void *contextPtr, RedisModuleIO *io) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    RedisModule_SaveDouble(io, context->maxValue);
    RedisModule_SaveDouble(io, context->minValue);
}

int MaxMinReadContext(void *contextPtr, RedisModuleIO *io, REDISMODULE_ATTR_UNUSED int encver) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    context->maxValue = LoadDouble_IOError(io, goto err);
    context->minValue = LoadDouble_IOError(io, goto err);
    return TSDB_OK;
err:
    return TSDB_ERROR;
}

void SumAppendValue(void *contextPtr, double value) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    context->value += value;
}

void CountAppendValue(void *contextPtr, double value) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    context->value++;
}

void CountFinalize(void *contextPtr, double *val) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    *val = context->value;
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

AggregationClass aggMax = { .createContext = MaxMinCreateContext,
                            .appendValue = MaxAppendValue,
                            .appendValueVec = NULL, /* determined on run time */
                            .freeContext = rm_free,
                            .finalize = MaxFinalize,
                            .writeContext = MaxMinWriteContext,
                            .readContext = MaxMinReadContext,
                            .resetContext = MaxMinReset };

static AggregationClass aggMin = { .createContext = MaxMinCreateContext,
                                   .appendValue = MinAppendValue,
                                   .appendValueVec = NULL, /* determined on run time */
                                   .freeContext = rm_free,
                                   .finalize = MinFinalize,
                                   .writeContext = MaxMinWriteContext,
                                   .readContext = MaxMinReadContext,
                                   .resetContext = MaxMinReset };

static AggregationClass aggSum = { .createContext = SingleValueCreateContext,
                                   .appendValue = SumAppendValue,
                                   .appendValueVec = NULL, /* determined on run time */
                                   .freeContext = rm_free,
                                   .finalize = SingleValueFinalize,
                                   .writeContext = SingleValueWriteContext,
                                   .readContext = SingleValueReadContext,
                                   .resetContext = SingleValueReset };

static AggregationClass aggCount = { .createContext = SingleValueCreateContext,
                                     .appendValue = CountAppendValue,
                                     .appendValueVec = NULL, /* determined on run time */
                                     .freeContext = rm_free,
                                     .finalize = CountFinalize,
                                     .writeContext = SingleValueWriteContext,
                                     .readContext = SingleValueReadContext,
                                     .resetContext = SingleValueReset };

static AggregationClass aggFirst = { .createContext = SingleValueCreateContext,
                                     .appendValue = FirstAppendValue,
                                     .appendValueVec = NULL, /* determined on run time */
                                     .freeContext = rm_free,
                                     .finalize = SingleValueFinalize,
                                     .writeContext = SingleValueWriteContext,
                                     .readContext = SingleValueReadContext,
                                     .resetContext = SingleValueReset };

static AggregationClass aggLast = { .createContext = SingleValueCreateContext,
                                    .appendValue = LastAppendValue,
                                    .appendValueVec = NULL, /* determined on run time */
                                    .freeContext = rm_free,
                                    .finalize = SingleValueFinalize,
                                    .writeContext = SingleValueWriteContext,
                                    .readContext = SingleValueReadContext,
                                    .resetContext = SingleValueReset };

static AggregationClass aggRange = { .createContext = MaxMinCreateContext,
                                     .appendValue = MaxMinAppendValue,
                                     .appendValueVec = NULL, /* determined on run time */
                                     .freeContext = rm_free,
                                     .finalize = RangeFinalize,
                                     .writeContext = MaxMinWriteContext,
                                     .readContext = MaxMinReadContext,
                                     .resetContext = MaxMinReset };

void linkAppendValueVecFuncs() {
    aggMax.appendValueVec = MaxAppendValuesVec;
    return;
}

int StringAggTypeToEnum(const char *agg_type) {
    return StringLenAggTypeToEnum(agg_type, strlen(agg_type));
}

int RMStringLenAggTypeToEnum(RedisModuleString *aggTypeStr) {
    size_t str_len;
    const char *aggTypeCStr = RedisModule_StringPtrLen(aggTypeStr, &str_len);
    return StringLenAggTypeToEnum(aggTypeCStr, str_len);
}

int StringLenAggTypeToEnum(const char *agg_type, size_t len) {
    int result = TS_AGG_INVALID;
    char agg_type_lower[len];
    for (int i = 0; i < len; i++) {
        agg_type_lower[i] = tolower(agg_type[i]);
    }
    if (len == 3) {
        if (strncmp(agg_type_lower, "min", len) == 0 && len == 3) {
            result = TS_AGG_MIN;
        } else if (strncmp(agg_type_lower, "max", len) == 0) {
            result = TS_AGG_MAX;
        } else if (strncmp(agg_type_lower, "sum", len) == 0) {
            result = TS_AGG_SUM;
        } else if (strncmp(agg_type_lower, "avg", len) == 0) {
            result = TS_AGG_AVG;
        }
    } else if (len == 4) {
        if (strncmp(agg_type_lower, "last", len) == 0) {
            result = TS_AGG_LAST;
        }
    } else if (len == 5) {
        if (strncmp(agg_type_lower, "count", len) == 0) {
            result = TS_AGG_COUNT;
        } else if (strncmp(agg_type_lower, "range", len) == 0) {
            result = TS_AGG_RANGE;
        } else if (strncmp(agg_type_lower, "first", len) == 0) {
            result = TS_AGG_FIRST;
        } else if (strncmp(agg_type_lower, "std.p", len) == 0) {
            result = TS_AGG_STD_P;
        } else if (strncmp(agg_type_lower, "std.s", len) == 0) {
            result = TS_AGG_STD_S;
        } else if (strncmp(agg_type_lower, "var.p", len) == 0) {
            result = TS_AGG_VAR_P;
        } else if (strncmp(agg_type_lower, "var.s", len) == 0) {
            result = TS_AGG_VAR_S;
        }
    }
    return result;
}

const char *AggTypeEnumToString(TS_AGG_TYPES_T aggType) {
    switch (aggType) {
        case TS_AGG_MIN:
            return "MIN";
        case TS_AGG_MAX:
            return "MAX";
        case TS_AGG_SUM:
            return "SUM";
        case TS_AGG_AVG:
            return "AVG";
        case TS_AGG_STD_P:
            return "STD.P";
        case TS_AGG_STD_S:
            return "STD.S";
        case TS_AGG_VAR_P:
            return "VAR.P";
        case TS_AGG_VAR_S:
            return "VAR.S";
        case TS_AGG_COUNT:
            return "COUNT";
        case TS_AGG_FIRST:
            return "FIRST";
        case TS_AGG_LAST:
            return "LAST";
        case TS_AGG_RANGE:
            return "RANGE";
        case TS_AGG_NONE:
        case TS_AGG_INVALID:
        case TS_AGG_TYPES_MAX:
            break;
    }
    return "Unknown";
}

AggregationClass *GetAggClass(TS_AGG_TYPES_T aggType) {
    switch (aggType) {
        case TS_AGG_MIN:
            return &aggMin;
        case TS_AGG_MAX:
            return &aggMax;
        case TS_AGG_AVG:
            return &aggAvg;
        case TS_AGG_STD_P:
            return &aggStdP;
        case TS_AGG_STD_S:
            return &aggStdS;
        case TS_AGG_VAR_P:
            return &aggVarP;
        case TS_AGG_VAR_S:
            return &aggVarS;
        case TS_AGG_SUM:
            return &aggSum;
        case TS_AGG_COUNT:
            return &aggCount;
        case TS_AGG_FIRST:
            return &aggFirst;
        case TS_AGG_LAST:
            return &aggLast;
        case TS_AGG_RANGE:
            return &aggRange;
        case TS_AGG_NONE:
        case TS_AGG_INVALID:
        case TS_AGG_TYPES_MAX:
            break;
    }
    return NULL;
}
