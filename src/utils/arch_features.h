/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#ifndef ARCH_FEATURES_H
#define ARCH_FEATURES_H

#include "cpu_features/include/cpu_features_macros.h"

#ifdef CPU_FEATURES_ARCH_X86_64
#include "cpu_features/include/cpuinfo_x86.h"
#else
typedef struct X86Features
{
    int avx512f;
} X86Features;

#endif

const X86Features *getArchitectureOptimization();

#endif // ARCH_FEATURES_H
