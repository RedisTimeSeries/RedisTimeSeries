
#pragma once

#include <stdbool.h>

extern bool __via_gdb;

#define BB do { if (__via_gdb) { __asm__("int $3"); } } while(0);
