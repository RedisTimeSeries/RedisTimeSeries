#ifndef OVERFLOW_H
#define OVERFLOW_H

#include <stdbool.h>

// Check if the addition of two numbers will overflow. Returns true if it will
// overflow, false otherwise.
static inline  __attribute__((always_inline)) bool check_mul_overflow(const size_t a, const size_t b) {
    size_t result;
    return __builtin_mul_overflow(a, b, &result);
}


#endif // OVERFLOW_H
