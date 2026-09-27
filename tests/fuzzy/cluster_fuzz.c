#include "config.h"

#include "fuzzy/scenario.h"

#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

/* The scenario engine driven by a byte stream instead of the seeded PCG: one
 * byte selects each small choice, two bytes a large one, and an exhausted
 * stream keeps choosing zero. Coverage feedback then steers the schedule. */
struct bytes {
    const uint8_t *data;
    size_t size;
    size_t offset;
};

static uint32_t choose_bytes(void *context, uint32_t limit)
{
    struct bytes *bytes = context;
    uint32_t value = 0;
    const unsigned width = limit <= 256 ? 1 : 2;
    for (unsigned i = 0; i < width; ++i) {
        value <<= 8;
        if (bytes->offset < bytes->size)
            value |= bytes->data[bytes->offset++];
    }
    return value % limit;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    struct bytes bytes = {data, size, 2};
    const struct scenario_source source = {choose_bytes, &bytes};
    struct scenario_options options = {0};
    if (size < 2)
        return 0;
    options.seed = data[0];
    options.profile = data[1] & SCENARIO_PROFILE_MAX;
    options.steps = (uint32_t)((size - 2) / 3);
    options.trace = getenv("VSR_CLUSTER_TRACE") != NULL;
    options.quiet = !options.trace;
    scenario_run(&options, &source);
    return 0;
}
