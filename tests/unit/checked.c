#include "config.h"

#include "checked.h"
#include "lib/check.h"

#include <stdint.h>

int main(void)
{
    size_t out = 0;

    CHECK(vsr_size_add(0, 0, &out) && out == 0);
    CHECK(vsr_size_add(12, 30, &out) && out == 42);
    CHECK(vsr_size_add(SIZE_MAX, 0, &out) && out == SIZE_MAX);
    CHECK(vsr_size_add(SIZE_MAX - 1, 1, &out) && out == SIZE_MAX);
    out = 42;
    CHECK(!vsr_size_add(SIZE_MAX, 1, &out) && out == 42);
    CHECK(!vsr_size_add(1, SIZE_MAX, &out) && out == 42);
    CHECK(!vsr_size_add(SIZE_MAX, SIZE_MAX, &out) && out == 42);

    CHECK(vsr_size_mul(0, SIZE_MAX, &out) && out == 0);
    CHECK(vsr_size_mul(SIZE_MAX, 0, &out) && out == 0);
    CHECK(vsr_size_mul(6, 7, &out) && out == 42);
    CHECK(vsr_size_mul(SIZE_MAX, 1, &out) && out == SIZE_MAX);
    CHECK(vsr_size_mul(SIZE_MAX / 2, 2, &out) && out == SIZE_MAX - 1);
    out = 42;
    CHECK(!vsr_size_mul(SIZE_MAX / 2 + 1, 2, &out) && out == 42);
    CHECK(!vsr_size_mul(SIZE_MAX, SIZE_MAX, &out) && out == 42);
    return 0;
}
