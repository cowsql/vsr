#ifndef VSR_TEST_RANDOM_H
#define VSR_TEST_RANDOM_H

#include <stdint.h>

/* PCG XSH-RR 64/32 with fixed-width, explicit modular arithmetic. The stream
 * and seed fully determine output on every supported C11 target. This generator
 * belongs to the host simulator; no randomness enters the replication core.
 * Algorithm and reference vectors: https://www.pcg-random.org/ */
struct test_random {
    uint64_t state;
    uint64_t increment;
};

void test_random_seed(struct test_random *random, uint64_t seed,
                      uint64_t stream);
uint32_t test_random_next(struct test_random *random);
/* Uniform in [0, bound), including non-power-of-two bounds; bound must be > 0. */
uint32_t test_random_bounded(struct test_random *random, uint32_t bound);

#endif
