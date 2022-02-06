#include "arch_features.h"
#include "cpu_features/include/cpu_features_macros.h"

static X86Features g_features = { 0 };

const X86Features *getArchitectureOptimization() {
#ifdef CPU_FEATURES_ARCH_X86_64
    g_features = GetX86Info().features;
    return &g_features;
#endif // CPU_FEATURES_ARCH_X86_64
    return (X86Features *)0;
}
