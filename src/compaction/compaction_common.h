#ifndef COMPACTION_COMMON_H
#define COMPACTION_COMMON_H

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

static inline bool is_aligned(void *p, int N)
{
    return (int)p % N == 0;
}

#endif
