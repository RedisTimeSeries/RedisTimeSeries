/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
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
