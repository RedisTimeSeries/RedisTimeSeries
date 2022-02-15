/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */

#include "cpu_features/include/cpu_features_macros.h"
#include "arch_features.h"

static X86Features g_features = { 0 };

const X86Features *getArchitectureOptimization() {
#ifdef CPU_FEATURES_ARCH_X86_64
    g_features = GetX86Info().features;
    return &g_features;
#endif // CPU_FEATURES_ARCH_X86_64
    return (X86Features *)0;
}
