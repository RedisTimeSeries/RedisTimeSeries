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
    static size_t chacheLineSize = 0;
    if(chacheLineSize != 0) {
        return chacheLineSize;
    } 
    size_t sizeOfLineSize = sizeof(chacheLineSize);
    sysctlbyname("hw.cachelinesize", &chacheLineSize, &sizeOfLineSize, 0, 0);
    return chacheLineSize;
}

#elif defined(__linux__)

#include <stdio.h>

size_t getCPUCacheLineSize() {
    static size_t chacheLineSize = 0;
    if(chacheLineSize != 0) {
        return chacheLineSize;
    } 
    
    unsigned int lineSize = 0;
    FILE *const f = fopen("/sys/devices/system/cpu/cpu0/cache/index0/coherency_line_size", "r");
    if (f) {
        fscanf(f, "%ul", &lineSize);
        fclose(f);
    }

    chacheLineSize = (size_t)lineSize;
    return chacheLineSize;
}

#else
#error Unsupported platform
#endif
