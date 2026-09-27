#include "config.h"

#include "checked.h"

#include <stdint.h>

bool vsr_size_add(size_t a, size_t b, size_t *out)
{
    if (a > SIZE_MAX - b) {
        return false;
    }
    *out = a + b;
    return true;
}

bool vsr_size_mul(size_t a, size_t b, size_t *out)
{
    if (b != 0 && a > SIZE_MAX / b) {
        return false;
    }
    *out = a * b;
    return true;
}
