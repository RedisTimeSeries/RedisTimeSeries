/*
 * Copyright 2018-2021 Redis Labs Ltd. and Contributors
 *
 * This file is available under the Redis Labs Source Available License Agreement
 */
#ifndef ARCH_FEATURES_H
#define ARCH_FEATURES_H
#include "cpu_features/include/cpuinfo_x86.h"

const X86Features *getArchitectureOptimization();

#endif
