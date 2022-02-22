/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#ifndef COMPACTION_COMMON_H
#define COMPACTION_COMMON_H

#include <stdbool.h>
#include <stdio.h>
#include <float.h>
#include <stdint.h>

#define CACHE_LINE_SIZE 64
#define VECTOR_SIZE (CACHE_LINE_SIZE/sizeof(double))

#define _DOUBLE_MOST_NEG (((double)-1.0) * DBL_MAX)

typedef struct MaxMinContext
{
    double minValue;
    double maxValue;
} MaxMinContext;

static inline void _AssignIfGreater(double *__restrict__ value, double *__restrict__ newValues) {
    if(*newValues > *value) {
        *value = *newValues;
    }
}

void MaxAppendValuesVec(void *__restrict__ context,
                        double *__restrict__ values,
                        size_t si,
                        size_t ei);

static inline bool is_aligned(void *p, int N)
{
    return (uintptr_t)p % N == 0;
}

#endif
