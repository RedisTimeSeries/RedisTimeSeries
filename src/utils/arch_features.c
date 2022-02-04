#include "arch_features.h"
#include "cpu_features_macros.h"
#include "cpuinfo_x86.h"

static X86Features g_features = {0};

X86Features *getArchitectureOptimization() {
#ifdef CPU_FEATURES_ARCH_X86_64
    g_features = GetX86Info().features;
    if (features.avx512f) {
        return ARCH_OPT_AVX512;
    }
    if (features.avx || features.avx2) {
        return ARCH_OPT_AVX;
    }
    if (features.sse || features.sse2 || features.sse3 || features.sse4_1 ||
               features.sse4_2 || features.sse4a) {
        return ARCH_OPT_SSE;
    }
    return &g_features;
#endif // CPU_FEATURES_ARCH_X86_64
    return NULL;
}