#include "config.h"

#include "lib/check.h"
#include "lib/random.h"

#include <stddef.h>

int main(void)
{
    /* Published PCG32 reference output for initial state 42, stream 54. */
    static const uint32_t expected[] = {
        UINT32_C(0xa15c02b7), UINT32_C(0x7b47f409), UINT32_C(0xba1d3330),
        UINT32_C(0x83d2f293), UINT32_C(0xbfa4784b), UINT32_C(0xcbed606e)};
    static const uint64_t seeds[] = {0, 1, UINT64_MAX, UINT64_MAX - 1,
                                     UINT64_C(0x123456789abcdef0)};
    static const uint32_t bounds[] = {1,         2, 3, 17, UINT32_C(0x80000001),
                                      UINT32_MAX};
    struct test_random a;
    struct test_random b;

    test_random_seed(&a, 42, 54);
    for (size_t i = 0; i < sizeof(expected) / sizeof(expected[0]); ++i) {
        CHECK(test_random_next(&a) == expected[i]);
    }
    for (size_t i = 0; i < sizeof(seeds) / sizeof(seeds[0]); ++i) {
        test_random_seed(&a, seeds[i], UINT64_MAX);
        test_random_seed(&b, seeds[i], UINT64_MAX);
        for (size_t j = 0; j < 10000; ++j) {
            uint32_t bound = bounds[j % (sizeof(bounds) / sizeof(bounds[0]))];
            uint32_t value = test_random_bounded(&a, bound);
            CHECK(value < bound);
            CHECK(test_random_bounded(&b, bound) == value);
        }
    }
    return 0;
}
