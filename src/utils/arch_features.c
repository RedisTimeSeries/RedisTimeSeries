/*
 *copyright redis ltd. 2017 - present
 *licensed under your choice of the redis source available license 2.0 (rsalv2) or
 *the server side public license v1 (ssplv1).
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
