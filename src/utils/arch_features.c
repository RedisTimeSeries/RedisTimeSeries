/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "arch_features.h"

static X86Features g_features = { 0, 0 };

const X86Features *getArchitectureOptimization() {
#ifdef CPU_FEATURES_ARCH_X86_64
    g_features = GetX86Info().features;
    return &g_features;
#endif // CPU_FEATURES_ARCH_X86_64
    return (X86Features *)0;
}
