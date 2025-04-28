/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#ifndef ARCH_FEATURES_H
#define ARCH_FEATURES_H

#include "cpu_features/include/cpu_features_macros.h"

#ifdef CPU_FEATURES_ARCH_X86_64
#include "cpu_features/include/cpuinfo_x86.h"
#else
typedef struct X86Features
{
    int avx2;
    int avx512f;
} X86Features;

#endif

const X86Features *getArchitectureOptimization();

#endif // ARCH_FEATURES_H
