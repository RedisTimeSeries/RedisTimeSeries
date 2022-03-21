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

#define _DOUBLE_MIN (((double)-1.0) * DBL_MAX)

static inline void _AssignIfGreater(double *__restrict__ value, double *__restrict__ newValues) {
    if(*newValues > *value) {
        *value = *newValues;
    }
}

#endif
