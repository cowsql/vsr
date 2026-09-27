#include "config.h"

#include "lib/random.h"

#include "lib/check.h"

/* Two limbs make the intentional modulo-2^64 arithmetic explicit, so integer
 * sanitizers remain enabled without exemptions anywhere in the simulator. */
static uint64_t add_mod64(uint64_t a, uint64_t b)
{
    uint64_t low = (uint64_t)(uint32_t)a + (uint32_t)b;
    uint32_t high = (uint32_t)((a >> 32) + (b >> 32) + (low >> 32));
    return ((uint64_t)high << 32) | (uint32_t)low;
}

static uint64_t advance(uint64_t state, uint64_t increment)
{
    const uint32_t multiplier_low = UINT32_C(0x4c957f2d);
    const uint32_t multiplier_high = UINT32_C(0x5851f42d);
    uint32_t low = (uint32_t)state;
    uint32_t high = (uint32_t)(state >> 32);
    uint64_t product = (uint64_t)low * multiplier_low;
    uint32_t upper = (uint32_t)((product >> 32) +
                                (uint32_t)((uint64_t)low * multiplier_high) +
                                (uint32_t)((uint64_t)high * multiplier_low));
    return add_mod64(((uint64_t)upper << 32) | (uint32_t)product, increment);
}

uint32_t test_random_next(struct test_random *random)
{
    uint64_t previous = random->state;
    uint32_t bits = (uint32_t)(((previous >> 18) ^ previous) >> 27);
    uint32_t rotation = (uint32_t)(previous >> 59);
    uint32_t left = (32u - rotation) & 31u;

    random->state = advance(previous, random->increment);
    return (bits >> rotation) | (uint32_t)((uint64_t)bits << left);
}

void test_random_seed(struct test_random *random, uint64_t seed,
                      uint64_t stream)
{
    random->state = 0;
    random->increment = (stream & (UINT64_MAX >> 1)) * 2 + 1;
    (void)test_random_next(random);
    random->state = add_mod64(random->state, seed);
    (void)test_random_next(random);
}

uint32_t test_random_bounded(struct test_random *random, uint32_t bound)
{
    uint32_t threshold;
    CHECK(bound != 0);
    threshold = (uint32_t)((UINT64_C(1) << 32) % bound);
    for (;;) {
        uint32_t value = test_random_next(random);
        if (value >= threshold) {
            return value % bound;
        }
    }
}
