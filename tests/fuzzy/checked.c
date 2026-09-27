#include "config.h"

#include "checked.h"
#include "lib/check.h"

#include <stdint.h>
#include <string.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    size_t a;
    size_t b;
    size_t sum = 17;
    size_t product = 17;
    size_t reversed = 17;

    if (size != sizeof(a) + sizeof(b)) {
        return 0;
    }
    memcpy(&a, data, sizeof(a));
    memcpy(&b, data + sizeof(a), sizeof(b));

    const bool added = vsr_size_add(a, b, &sum);
    CHECK(added == vsr_size_add(b, a, &reversed));
    CHECK(sum == reversed);
    if (added) {
        CHECK(sum >= a && sum - a == b);
    } else {
        CHECK(sum == 17 && a > SIZE_MAX - b);
    }

    reversed = 17;
    const bool multiplied = vsr_size_mul(a, b, &product);
    CHECK(multiplied == vsr_size_mul(b, a, &reversed));
    CHECK(product == reversed);
    if (multiplied) {
        CHECK(a == 0 ? product == 0 : product / a == b && product % a == 0);
    } else {
        CHECK(product == 17 && b != 0 && a > SIZE_MAX / b);
    }
    return 0;
}
