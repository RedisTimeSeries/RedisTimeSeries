//
// Copyright © 2020 Maxim Morozov. All rights reserved.
//
// Created by Maxim Morozov on 30/10/2020.
//
// cpu-cache-line-size
//
// main.cpp
//
// The utility prints the size of the cache line of CPU in bytes.
//

#if defined(__APPLE__)

#include <sys/sysctl.h>

size_t getCPUCacheLineSize() {
    size_t lineSize = 0;
    size_t sizeOfLineSize = sizeof(lineSize);
    sysctlbyname("hw.cachelinesize", &lineSize, &sizeOfLineSize, 0, 0);
    return lineSize;
}

#elif defined(__linux__)

#include <stdio.h>

size_t getCPUCacheLineSize() {
    unsigned int lineSize = 0;

    FILE *const f = fopen("/sys/devices/system/cpu/cpu0/cache/index0/coherency_line_size", "r");
    if (f) {
        fscanf(f, "%ul", &lineSize);
        fclose(f);
    }

    return (size_t)lineSize;
}

#else
#error Unsupported platform
#endif
