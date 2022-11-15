/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
 */
#ifndef COMPACTION_COMMON_H
#define COMPACTION_COMMON_H

#include <stdbool.h>
#include <stdio.h>
#include <float.h>
#include <stdint.h>
#include "../consts.h"
#include "compaction_avx512f.h"

#define CACHE_LINE_SIZE 64
#define ALIGN_SIZE_AVX2 32
#define VECTOR_SIZE (CACHE_LINE_SIZE/sizeof(double))
#define VECTOR_SIZE_AVX2 (ALIGN_SIZE_AVX2/sizeof(double))

#define _DOUBLE_MIN (((double)-1.0) * DBL_MAX)

typedef struct MaxMinContext
{
    double minValue;
    double maxValue;
} MaxMinContext;

static really_inline void _AssignIfGreater(double *__restrict__ value, double *__restrict__ newValues)
{
    if(*newValues > *value) {
        *value = *newValues;
    }
}

void MaxAppendValuesVec(void *__restrict__ context,
                        double *__restrict__ values,
                        size_t si,
                        size_t ei);

static really_inline bool is_aligned(void *p, int N)
{
    return (uintptr_t)p % N == 0;
}

#endif
