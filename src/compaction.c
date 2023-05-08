/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
 */
#include "compaction.h"

#include "load_io_error_macros.h"
#include "rdb.h"

#include "rmutil/alloc.h"
#include "compactions/compaction_common.h"
#include "compactions/compaction_avx512f.h"
#include "compactions/compaction_avx2.h"
#include "utils/arch_features.h"

#include <ctype.h>
#include <float.h>
#include <math.h> // sqrt
#include <string.h>
#include <assert.h>

#if defined(_DEBUG) && defined(_VALGRIND)
#include "valgrind/valgrind.h"
#endif

typedef struct FirstValueContext
{
    double value;
    char isResetted;
} FirstValueContext;

typedef struct SingleValueContext
{
    double value;
} SingleValueContext;

typedef struct AvgContext
{
    double val;
    double cnt;
    bool isOverflow;
} AvgContext;

typedef struct TwaContext
{
    double res;
    timestamp_t prevTS;
    double prevValue;
    timestamp_t bucketStartTS;
    timestamp_t bucketEndTS;
    timestamp_t first_ts;
    timestamp_t last_ts;
    bool is_first_bucket;
    bool reverse;
    int64_t iteration;
} TwaContext;

typedef struct StdContext
{
    double sum;
    double sum_2; // sum of (values^2)
    u_int64_t cnt;
} StdContext;

void finalize_empty_with_NAN(__unused void *contextPtr, double *value) {
    *value = NAN;
}

void finalize_empty_with_ZERO(__unused void *contextPtr, double *value) {
    *value = 0;
}

void finalize_empty_last_value(void *contextPtr, double *value) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    *value = context->value;
}

void *SingleValueCreateContext(__unused bool reverse) {
    SingleValueContext *context = (SingleValueContext *)malloc(sizeof(SingleValueContext));
    context->value = 0;
    return context;
}

void *SingleValueCloneContext(void *contextPtr) {
    SingleValueContext *buf = (SingleValueContext *)malloc(sizeof(SingleValueContext));
    memcpy(buf, contextPtr, sizeof(SingleValueContext));
    return buf;
}

void SingleValueReset(void *contextPtr) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    context->value = 0;
}

void LastValueReset(void *contextPtr) {
    // Don't do anything cause with EMPTY flag we would like to use the last value
    return;
}

int SingleValueFinalize(void *contextPtr, double *val) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    *val = context->value;
    return TSDB_OK;
}

void SingleValueWriteContext(void *contextPtr, RedisModuleIO *io) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    RedisModule_SaveDouble(io, context->value);
}

int SingleValueReadContext(void *contextPtr, RedisModuleIO *io, int encver) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    context->value = LoadDouble_IOError(io, goto err);
    if (encver >= TS_IS_RESSETED_DUP_POLICY_RDB_VER && encver < TS_LAST_AGGREGATION_EMPTY) {
        // In old rdbs there was is_resetted flag
        LoadUnsigned_IOError(io, goto err);
    }
    return TSDB_OK;
err:
    return TSDB_ERROR;
}

void *FirstValueCreateContext(__unused bool reverse) {
    FirstValueContext *context = (FirstValueContext *)malloc(sizeof(FirstValueContext));
    context->value = 0;
    context->isResetted = TRUE;
    return context;
}

void *FirstValueCloneContext(void *contextPtr) {
    FirstValueContext *buf = (FirstValueContext *)malloc(sizeof(FirstValueContext));
    memcpy(buf, contextPtr, sizeof(FirstValueContext));
    return buf;
}

void FirstValueReset(void *contextPtr) {
    FirstValueContext *context = (FirstValueContext *)contextPtr;
    context->value = 0;
    context->isResetted = TRUE;
}

int FirstValueFinalize(void *contextPtr, double *val) {
    FirstValueContext *context = (FirstValueContext *)contextPtr;
    *val = context->value;
    return TSDB_OK;
}

void FirstValueWriteContext(void *contextPtr, RedisModuleIO *io) {
    FirstValueContext *context = (FirstValueContext *)contextPtr;
    RedisModule_SaveDouble(io, context->value);
    RedisModule_SaveUnsigned(io, context->isResetted);
}

int FirstValueReadContext(void *contextPtr, RedisModuleIO *io, int encver) {
    FirstValueContext *context = (FirstValueContext *)contextPtr;
    context->value = LoadDouble_IOError(io, goto err);
    if (encver >= TS_IS_RESSETED_DUP_POLICY_RDB_VER) {
        context->isResetted = LoadUnsigned_IOError(io, goto err);
    }
    return TSDB_OK;
err:
    return TSDB_ERROR;
}

static inline void _AvgInitContext(AvgContext *context) {
    context->cnt = 0;
    context->val = 0;
    context->isOverflow = false;
}

void *AvgCreateContext(__unused bool reverse) {
    AvgContext *context = (AvgContext *)malloc(sizeof(AvgContext));
    _AvgInitContext(context);
    return context;
}

void *AvgCloneContext(void *contextPtr) {
    AvgContext *buf = (AvgContext *)malloc(sizeof(AvgContext));
    memcpy(buf, contextPtr, sizeof(AvgContext));
    return buf;
}

// Except valgrind it's equivalent to sizeof(long double) > 8
#if !defined(_DEBUG) && !defined(_VALGRIND)
bool hasLongDouble = sizeof(long double) > 8;
#else
bool hasLongDouble = false;
#endif

void AvgAddValue(void *contextPtr, double value, __attribute__((unused)) timestamp_t ts) {
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

int AvgFinalize(void *contextPtr, double *value) {
    AvgContext *context = (AvgContext *)contextPtr;
    if (unlikely(context->cnt == 0)) {
        _log_if(context->cnt == 0, "AvgFinalize: context->cnt is 0");
        return TSDB_ERROR;
    }

    if (unlikely(context->isOverflow)) {
        *value = context->val;
    } else {
        *value = context->val / context->cnt;
    }
    return TSDB_OK;
}

void AvgReset(void *contextPtr) {
    _AvgInitContext(contextPtr);
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

static inline void _TwainitContext(TwaContext *context, bool reverse) {
    context->res = 0;
    context->prevTS = DC;        // arbitrary value
    context->prevValue = DC;     // arbitrary value
    context->bucketStartTS = DC; // arbitrary value
    context->bucketEndTS = DC;   // arbitrary value
    context->first_ts = DC;
    context->last_ts = DC;
    context->is_first_bucket = true;
    context->iteration = 0;
    context->reverse = reverse;
}

void *TwaCloneContext(void *contextPtr) {
    TwaContext *buf = (TwaContext *)malloc(sizeof(TwaContext));
    memcpy(buf, contextPtr, sizeof(TwaContext));
    return buf;
}

void *TwaCreateContext(bool reverse) {
    TwaContext *context = (TwaContext *)malloc(sizeof(TwaContext));
    _TwainitContext(context, reverse);
    return context;
}

static inline void _update_twaContext(TwaContext *wcontext,
                                      const double *value,
                                      const timestamp_t *ts) {
    wcontext->prevValue = *value;
    wcontext->prevTS = *ts;
}

void TwaAddBucketParams(void *contextPtr, timestamp_t bucketStartTS, timestamp_t bucketEndTS) {
    TwaContext *context = (TwaContext *)contextPtr;
    if (context->reverse) {
        __SWAP(bucketStartTS, bucketEndTS);
    }
    context->bucketStartTS = bucketStartTS;
    context->bucketEndTS = bucketEndTS;
}

void TwaAddPrevBucketLastSample(void *contextPtr, double value, timestamp_t ts) {
    TwaContext *wcontext = (TwaContext *)contextPtr;
    _update_twaContext(wcontext, &value, &ts);
    wcontext->is_first_bucket = false;
}

void TwaAddValue(void *contextPtr, double value, timestamp_t ts) {
    TwaContext *context = (TwaContext *)contextPtr;
    int64_t *iter = &context->iteration;
    timestamp_t t1 = context->prevTS, t2 = ts;
    double v1 = context->prevValue, v2 = value;
    if (context->reverse) {
        __SWAP(t1, t2);
        __SWAP(v1, v2);
    }
    const double delta_time = t2 - t1;
    const double delta_val = v2 - v1;
    const bool *is_first_bucket = &context->is_first_bucket;
    const timestamp_t ta = context->bucketStartTS;

    if ((*iter) == 0) { // First sample in bucket
        if (!(*is_first_bucket)) {
            context->first_ts = ta;
            double vab = v1 + ((double)((ta - t1) * delta_val)) / delta_time;
            if (!context->reverse) {
                context->res += ((vab + v2) * (t2 - ta)) / 2.0;
            } else {
                context->res += ((vab + v1) * (ta - t1)) / 2.0;
            }
        } else {
            // else: cur sample is the first in the series, so just store it
            context->first_ts = ts;
        }
    } else {
        context->res += ((v1 + v2) * (t2 - t1)) / 2.0;
    }

    // store this sample for next iteration
    context->last_ts = context->prevTS = ts;
    context->prevValue = value;
    ++(*iter);
}

void TwaAddNextBucketFirstSample(void *contextPtr, double value, timestamp_t ts) {
    TwaContext *context = (TwaContext *)contextPtr;
    timestamp_t t1 = context->prevTS, t2 = ts;
    double v1 = context->prevValue, v2 = value;
    if (context->reverse) {
        __SWAP(t1, t2);
        __SWAP(v1, v2);
    }
    const double delta_time = t2 - t1;
    const double delta_val = v2 - v1;
    const timestamp_t tb = context->bucketEndTS;

    double vab = v1 + ((double)((tb - t1) * delta_val)) / delta_time;
    if (!context->reverse) {
        context->res += ((vab + v1) * (tb - t1)) / 2.0;
    } else {
        context->res += ((vab + v2) * (t2 - tb)) / 2.0;
    }

    context->last_ts = tb;
}

int TwaFinalize(void *contextPtr, double *value) {
    TwaContext *context = (TwaContext *)contextPtr;
    if (context->last_ts == context->first_ts) {
        // Or the size of the bucket is 0 with one sample in it,
        // or there is only one sample in the series.
        *value = context->prevValue;
    } else {
        *value = context->res / llabs((int64_t)(context->last_ts - context->first_ts));
    }
    return TSDB_OK;
}

void TwaGetLastSample(void *contextPtr, Sample *sample) {
    TwaContext *wcontext = (TwaContext *)contextPtr;
    sample->timestamp = wcontext->prevTS;
    sample->value = wcontext->prevValue;
}

void TwaReset(void *contextPtr) {
    TwaContext *wcontext = (TwaContext *)contextPtr;
    _TwainitContext(contextPtr, wcontext->reverse);
}

void TwaWriteContext(void *contextPtr, RedisModuleIO *io) {
    TwaContext *context = (TwaContext *)contextPtr;
    RedisModule_SaveDouble(io, context->res);
    RedisModule_SaveUnsigned(io, context->prevTS);
    RedisModule_SaveDouble(io, context->prevValue);
    RedisModule_SaveUnsigned(io, context->bucketStartTS);
    RedisModule_SaveUnsigned(io, context->bucketEndTS);
    RedisModule_SaveUnsigned(io, context->first_ts);
    RedisModule_SaveUnsigned(io, context->last_ts);
    RedisModule_SaveUnsigned(io, context->is_first_bucket);
    RedisModule_SaveUnsigned(io, context->iteration);
    RedisModule_SaveUnsigned(io, context->reverse);
}

int TwaReadContext(void *contextPtr, RedisModuleIO *io, int encver) {
    TwaContext *context = (TwaContext *)contextPtr;
    context->res = LoadDouble_IOError(io, goto err);
    context->prevTS = LoadUnsigned_IOError(io, goto err);
    context->prevValue = LoadDouble_IOError(io, goto err);
    context->bucketStartTS = LoadUnsigned_IOError(io, goto err);
    context->bucketEndTS = LoadUnsigned_IOError(io, goto err);
    context->first_ts = LoadUnsigned_IOError(io, goto err);
    context->last_ts = LoadUnsigned_IOError(io, goto err);
    context->is_first_bucket = LoadUnsigned_IOError(io, goto err);
    context->iteration = LoadUnsigned_IOError(io, goto err);
    context->reverse = LoadUnsigned_IOError(io, goto err);
    return TSDB_OK;
err:
    return TSDB_ERROR;
}

void *StdCreateContext(__unused bool reverse) {
    StdContext *context = (StdContext *)malloc(sizeof(StdContext));
    context->cnt = 0;
    context->sum = 0;
    context->sum_2 = 0;
    return context;
}

void *StdCloneContext(void *contextPtr) {
    StdContext *buf = (StdContext *)malloc(sizeof(StdContext));
    memcpy(buf, contextPtr, sizeof(StdContext));
    return buf;
}

void StdAddValue(void *contextPtr, double value, __attribute__((unused)) timestamp_t ts) {
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

int VarPopulationFinalize(void *contextPtr, double *value) {
    StdContext *context = (StdContext *)contextPtr;
    uint64_t count = context->cnt;
    if (unlikely(count == 0)) {
        *value = 0;
        return TSDB_ERROR;
    }
    *value = variance(context->sum, context->sum_2, count);
    return TSDB_OK;
}

int VarSamplesFinalize(void *contextPtr, double *value) {
    StdContext *context = (StdContext *)contextPtr;
    uint64_t count = context->cnt;
    if (unlikely(count == 0)) {
        *value = 0;
        return TSDB_ERROR;
    }
    if (count == 1) {
        *value = 0;
    } else {
        *value = variance(context->sum, context->sum_2, count) * count / (count - 1);
    }
    return TSDB_OK;
}

int StdPopulationFinalize(void *contextPtr, double *value) {
    double val;
    int ret = VarPopulationFinalize(contextPtr, &val);
    *value = sqrt(val);
    return ret;
}

int StdSamplesFinalize(void *contextPtr, double *value) {
    double val;
    int ret = VarSamplesFinalize(contextPtr, &val);
    *value = sqrt(val);
    return ret;
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

// time weighted avg
static AggregationClass aggWAvg = { .type = TS_AGG_TWA,
                                    .createContext = TwaCreateContext,
                                    .appendValue = TwaAddValue,
                                    .freeContext = rm_free,
                                    .finalize = TwaFinalize,
                                    .finalizeEmpty = finalize_empty_with_NAN,
                                    .writeContext = TwaWriteContext,
                                    .readContext = TwaReadContext,
                                    .addBucketParams = TwaAddBucketParams,
                                    .addPrevBucketLastSample = TwaAddPrevBucketLastSample,
                                    .addNextBucketFirstSample = TwaAddNextBucketFirstSample,
                                    .getLastSample = TwaGetLastSample,
                                    .resetContext = TwaReset,
                                    .cloneContext = TwaCloneContext };

static AggregationClass aggAvg = { .type = TS_AGG_AVG,
                                   .createContext = AvgCreateContext,
                                   .appendValue = AvgAddValue,
                                   .appendValueVec = NULL, /* determined on run time */
                                   .freeContext = rm_free,
                                   .finalize = AvgFinalize,
                                   .finalizeEmpty = finalize_empty_with_NAN,
                                   .writeContext = AvgWriteContext,
                                   .readContext = AvgReadContext,
                                   .addBucketParams = NULL,
                                   .addPrevBucketLastSample = NULL,
                                   .addNextBucketFirstSample = NULL,
                                   .getLastSample = NULL,
                                   .resetContext = AvgReset,
                                   .cloneContext = AvgCloneContext };

static AggregationClass aggStdP = { .type = TS_AGG_STD_P,
                                    .createContext = StdCreateContext,
                                    .appendValue = StdAddValue,
                                    .appendValueVec = NULL, /* determined on run time */
                                    .freeContext = rm_free,
                                    .finalize = StdPopulationFinalize,
                                    .finalizeEmpty = finalize_empty_with_NAN,
                                    .writeContext = StdWriteContext,
                                    .readContext = StdReadContext,
                                    .addBucketParams = NULL,
                                    .addPrevBucketLastSample = NULL,
                                    .addNextBucketFirstSample = NULL,
                                    .getLastSample = NULL,
                                    .resetContext = StdReset,
                                    .cloneContext = StdCloneContext };

static AggregationClass aggStdS = { .type = TS_AGG_STD_S,
                                    .createContext = StdCreateContext,
                                    .appendValue = StdAddValue,
                                    .appendValueVec = NULL, /* determined on run time */
                                    .freeContext = rm_free,
                                    .finalize = StdSamplesFinalize,
                                    .finalizeEmpty = finalize_empty_with_NAN,
                                    .writeContext = StdWriteContext,
                                    .readContext = StdReadContext,
                                    .addBucketParams = NULL,
                                    .addPrevBucketLastSample = NULL,
                                    .addNextBucketFirstSample = NULL,
                                    .getLastSample = NULL,
                                    .resetContext = StdReset,
                                    .cloneContext = StdCloneContext };

static AggregationClass aggVarP = { .type = TS_AGG_VAR_P,
                                    .createContext = StdCreateContext,
                                    .appendValue = StdAddValue,
                                    .appendValueVec = NULL, /* determined on run time */
                                    .freeContext = rm_free,
                                    .finalize = VarPopulationFinalize,
                                    .finalizeEmpty = finalize_empty_with_NAN,
                                    .writeContext = StdWriteContext,
                                    .readContext = StdReadContext,
                                    .addBucketParams = NULL,
                                    .addPrevBucketLastSample = NULL,
                                    .addNextBucketFirstSample = NULL,
                                    .getLastSample = NULL,
                                    .resetContext = StdReset,
                                    .cloneContext = StdCloneContext };

static AggregationClass aggVarS = { .type = TS_AGG_VAR_S,
                                    .createContext = StdCreateContext,
                                    .appendValue = StdAddValue,
                                    .appendValueVec = NULL, /* determined on run time */
                                    .freeContext = rm_free,
                                    .finalize = VarSamplesFinalize,
                                    .finalizeEmpty = finalize_empty_with_NAN,
                                    .writeContext = StdWriteContext,
                                    .readContext = StdReadContext,
                                    .addBucketParams = NULL,
                                    .addPrevBucketLastSample = NULL,
                                    .addNextBucketFirstSample = NULL,
                                    .getLastSample = NULL,
                                    .resetContext = StdReset,
                                    .cloneContext = StdCloneContext };

void *MaxMinCreateContext(__unused bool reverse) {
    MaxMinContext *context = (MaxMinContext *)malloc(sizeof(MaxMinContext));
    context->minValue = DBL_MAX;
    context->maxValue = _DOUBLE_MIN;
    return context;
}

void *MaxMinCloneContext(void *contextPtr) {
    MaxMinContext *buf = (MaxMinContext *)malloc(sizeof(MaxMinContext));
    memcpy(buf, contextPtr, sizeof(MaxMinContext));
    return buf;
}

void MaxAppendValue(void *context, double value, __attribute__((unused)) timestamp_t ts) {
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

void MinAppendValue(void *contextPtr, double value, __attribute__((unused)) timestamp_t ts) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    if (value < context->minValue) {
        context->minValue = value;
    }
}

void MaxMinAppendValue(void *contextPtr, double value, __attribute__((unused)) timestamp_t ts) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    if (value > context->maxValue) {
        context->maxValue = value;
    }
    if (value < context->minValue) {
        context->minValue = value;
    }
}

int MaxFinalize(void *contextPtr, double *value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    *value = context->maxValue;
    return TSDB_OK;
}

int MinFinalize(void *contextPtr, double *value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    *value = context->minValue;
    return TSDB_OK;
}

int RangeFinalize(void *contextPtr, double *value) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    *value = context->maxValue - context->minValue;
    return TSDB_OK;
}

void MaxMinReset(void *contextPtr) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    context->minValue = DBL_MAX;
    context->maxValue = _DOUBLE_MIN;
}

void MaxMinWriteContext(void *contextPtr, RedisModuleIO *io) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    RedisModule_SaveDouble(io, context->maxValue);
    RedisModule_SaveDouble(io, context->minValue);
}

int MaxMinReadContext(void *contextPtr, RedisModuleIO *io, int encver) {
    MaxMinContext *context = (MaxMinContext *)contextPtr;
    context->maxValue = LoadDouble_IOError(io, goto err);
    context->minValue = LoadDouble_IOError(io, goto err);
    if (encver < TS_ALIGNMENT_TS_VER) {
        size_t len = 1;
        char *sb = LoadStringBuffer_IOError(io, &len, goto err);
        RedisModule_Free(sb);
    }
    return TSDB_OK;
err:
    return TSDB_ERROR;
}

void SumAppendValue(void *contextPtr, double value, __attribute__((unused)) timestamp_t ts) {
    FirstValueContext *context = (FirstValueContext *)contextPtr;
    context->value += value;
}

void CountAppendValue(void *contextPtr, double value, __attribute__((unused)) timestamp_t ts) {
    FirstValueContext *context = (FirstValueContext *)contextPtr;
    context->value++;
}

int CountFinalize(void *contextPtr, double *val) {
    FirstValueContext *context = (FirstValueContext *)contextPtr;
    *val = context->value;
    return TSDB_OK;
}

void FirstAppendValue(void *contextPtr, double value, __attribute__((unused)) timestamp_t ts) {
    FirstValueContext *context = (FirstValueContext *)contextPtr;
    if (context->isResetted) {
        context->isResetted = FALSE;
        context->value = value;
    }
}

void LastAppendValue(void *contextPtr, double value, __attribute__((unused)) timestamp_t ts) {
    SingleValueContext *context = (SingleValueContext *)contextPtr;
    context->value = value;
}

static AggregationClass aggMax = { .type = TS_AGG_MAX,
                                   .createContext = MaxMinCreateContext,
                                   .appendValue = MaxAppendValue,
                                   .appendValueVec = NULL, /* determined on run time */
                                   .freeContext = rm_free,
                                   .finalize = MaxFinalize,
                                   .finalizeEmpty = finalize_empty_with_NAN,
                                   .writeContext = MaxMinWriteContext,
                                   .readContext = MaxMinReadContext,
                                   .addBucketParams = NULL,
                                   .addPrevBucketLastSample = NULL,
                                   .addNextBucketFirstSample = NULL,
                                   .getLastSample = NULL,
                                   .resetContext = MaxMinReset,
                                   .cloneContext = MaxMinCloneContext };

static AggregationClass aggMin = { .type = TS_AGG_MIN,
                                   .createContext = MaxMinCreateContext,
                                   .appendValue = MinAppendValue,
                                   .appendValueVec = NULL, /* determined on run time */
                                   .freeContext = rm_free,
                                   .finalize = MinFinalize,
                                   .finalizeEmpty = finalize_empty_with_NAN,
                                   .writeContext = MaxMinWriteContext,
                                   .readContext = MaxMinReadContext,
                                   .addBucketParams = NULL,
                                   .addPrevBucketLastSample = NULL,
                                   .addNextBucketFirstSample = NULL,
                                   .getLastSample = NULL,
                                   .resetContext = MaxMinReset,
                                   .cloneContext = MaxMinCloneContext };

static AggregationClass aggSum = { .type = TS_AGG_SUM,
                                   .createContext = SingleValueCreateContext,
                                   .appendValue = SumAppendValue,
                                   .appendValueVec = NULL, /* determined on run time */
                                   .freeContext = rm_free,
                                   .finalize = SingleValueFinalize,
                                   .finalizeEmpty = finalize_empty_with_ZERO,
                                   .writeContext = SingleValueWriteContext,
                                   .readContext = SingleValueReadContext,
                                   .addBucketParams = NULL,
                                   .addPrevBucketLastSample = NULL,
                                   .addNextBucketFirstSample = NULL,
                                   .getLastSample = NULL,
                                   .resetContext = SingleValueReset,
                                   .cloneContext = SingleValueCloneContext };

static AggregationClass aggCount = { .type = TS_AGG_COUNT,
                                     .createContext = SingleValueCreateContext,
                                     .appendValue = CountAppendValue,
                                     .appendValueVec = NULL, /* determined on run time */
                                     .freeContext = rm_free,
                                     .finalize = CountFinalize,
                                     .finalizeEmpty = finalize_empty_with_ZERO,
                                     .writeContext = SingleValueWriteContext,
                                     .readContext = SingleValueReadContext,
                                     .addBucketParams = NULL,
                                     .addPrevBucketLastSample = NULL,
                                     .addNextBucketFirstSample = NULL,
                                     .getLastSample = NULL,
                                     .resetContext = SingleValueReset,
                                     .cloneContext = SingleValueCloneContext };

static AggregationClass aggFirst = { .type = TS_AGG_FIRST,
                                     .createContext = FirstValueCreateContext,
                                     .appendValue = FirstAppendValue,
                                     .appendValueVec = NULL, /* determined on run time */
                                     .freeContext = rm_free,
                                     .finalize = FirstValueFinalize,
                                     .finalizeEmpty = finalize_empty_with_NAN,
                                     .writeContext = FirstValueWriteContext,
                                     .readContext = FirstValueReadContext,
                                     .addBucketParams = NULL,
                                     .addPrevBucketLastSample = NULL,
                                     .addNextBucketFirstSample = NULL,
                                     .getLastSample = NULL,
                                     .resetContext = FirstValueReset,
                                     .cloneContext = FirstValueCloneContext };

static AggregationClass aggLast = { .type = TS_AGG_LAST,
                                    .createContext = SingleValueCreateContext,
                                    .appendValue = LastAppendValue,
                                    .appendValueVec = NULL, /* determined on run time */
                                    .freeContext = rm_free,
                                    .finalize = SingleValueFinalize,
                                    .finalizeEmpty = finalize_empty_last_value,
                                    .writeContext = SingleValueWriteContext,
                                    .readContext = SingleValueReadContext,
                                    .addBucketParams = NULL,
                                    .addPrevBucketLastSample = NULL,
                                    .addNextBucketFirstSample = NULL,
                                    .getLastSample = NULL,
                                    .resetContext = LastValueReset,
                                    .cloneContext = SingleValueCloneContext };

static AggregationClass aggRange = { .type = TS_AGG_RANGE,
                                     .createContext = MaxMinCreateContext,
                                     .appendValue = MaxMinAppendValue,
                                     .appendValueVec = NULL, /* determined on run time */
                                     .freeContext = rm_free,
                                     .finalize = RangeFinalize,
                                     .finalizeEmpty = finalize_empty_with_NAN,
                                     .writeContext = MaxMinWriteContext,
                                     .readContext = MaxMinReadContext,
                                     .addBucketParams = NULL,
                                     .addPrevBucketLastSample = NULL,
                                     .addNextBucketFirstSample = NULL,
                                     .getLastSample = NULL,
                                     .resetContext = MaxMinReset,
                                     .cloneContext = MaxMinCloneContext };

void initGlobalCompactionFunctions() {
    const X86Features *features = getArchitectureOptimization();
    aggMax.appendValueVec = MaxAppendValuesVec;

#if defined(__x86_64__)
    if (!features) {
        return;
        /* remove this comment to enable avx512
     } else if (features->avx512f) {
            aggMax.appendValueVec = MaxAppendValuesAVX512F;
            return;
        }*/
    } else if (features->avx2) {
        aggMax.appendValueVec = MaxAppendValuesAVX2;
        return;
    }
#endif // __x86_64__
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
        } else if (strncmp(agg_type_lower, "twa", len) == 0) {
            result = TS_AGG_TWA;
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
        case TS_AGG_TWA:
            return "TWA";
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

const char *AggTypeEnumToStringLowerCase(TS_AGG_TYPES_T aggType) {
    switch (aggType) {
        case TS_AGG_MIN:
            return "min";
        case TS_AGG_MAX:
            return "max";
        case TS_AGG_SUM:
            return "sum";
        case TS_AGG_AVG:
            return "avg";
        case TS_AGG_TWA:
            return "twa";
        case TS_AGG_STD_P:
            return "std.p";
        case TS_AGG_STD_S:
            return "std.s";
        case TS_AGG_VAR_P:
            return "var.p";
        case TS_AGG_VAR_S:
            return "var.s";
        case TS_AGG_COUNT:
            return "count";
        case TS_AGG_FIRST:
            return "first";
        case TS_AGG_LAST:
            return "last";
        case TS_AGG_RANGE:
            return "range";
        case TS_AGG_NONE:
        case TS_AGG_INVALID:
        case TS_AGG_TYPES_MAX:
            break;
    }
    return "unknown";
}

AggregationClass *GetAggClass(TS_AGG_TYPES_T aggType) {
    switch (aggType) {
        case TS_AGG_MIN:
            return &aggMin;
        case TS_AGG_MAX:
            return &aggMax;
        case TS_AGG_AVG:
            return &aggAvg;
        case TS_AGG_TWA:
            return &aggWAvg;
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
