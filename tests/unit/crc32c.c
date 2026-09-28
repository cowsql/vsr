#include "config.h"

#include "io/crc32c.h"
#include "lib/check.h"
#include "lib/random.h"

#include <inttypes.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Published check values: the CRC catalogue's "123456789" and the four
 * 32-byte vectors of RFC 3720 appendix B.4. */
static void test_vectors(void)
{
    unsigned char bytes[32];

    CHECK(vsr_io_crc32c(0, NULL, 0) == 0);
    CHECK(vsr_io_crc32c(0, "a", 1) == UINT32_C(0xc1d04330));
    CHECK(vsr_io_crc32c(0, "123456789", 9) == UINT32_C(0xe3069283));
    CHECK(vsr_io_crc32c_portable(0, "123456789", 9) == UINT32_C(0xe3069283));

    memset(bytes, 0, sizeof(bytes));
    CHECK(vsr_io_crc32c(0, bytes, sizeof(bytes)) == UINT32_C(0x8a9136aa));
    memset(bytes, 0xff, sizeof(bytes));
    CHECK(vsr_io_crc32c(0, bytes, sizeof(bytes)) == UINT32_C(0x62a8ab43));
    for (size_t i = 0; i < sizeof(bytes); ++i) {
        bytes[i] = (unsigned char)i;
    }
    CHECK(vsr_io_crc32c(0, bytes, sizeof(bytes)) == UINT32_C(0x46dd794e));
    for (size_t i = 0; i < sizeof(bytes); ++i) {
        bytes[i] = (unsigned char)(sizeof(bytes) - 1 - i);
    }
    CHECK(vsr_io_crc32c(0, bytes, sizeof(bytes)) == UINT32_C(0x113fdb5c));
    CHECK(vsr_io_crc32c_portable(0, bytes, sizeof(bytes)) ==
          UINT32_C(0x113fdb5c));
}

/* Chaining at every split point equals the one-shot checksum. */
static void test_chaining(void)
{
    unsigned char bytes[257];
    uint32_t whole;

    for (size_t i = 0; i < sizeof(bytes); ++i) {
        bytes[i] = (unsigned char)(i * 7u + 3u);
    }
    whole = vsr_io_crc32c(0, bytes, sizeof(bytes));
    CHECK(vsr_io_crc32c_portable(0, bytes, sizeof(bytes)) == whole);
    for (size_t split = 0; split <= sizeof(bytes); ++split) {
        uint32_t head = vsr_io_crc32c(0, bytes, split);
        uint32_t portable = vsr_io_crc32c_portable(0, bytes, split);

        CHECK(vsr_io_crc32c(head, bytes + split, sizeof(bytes) - split) ==
              whole);
        CHECK(vsr_io_crc32c_portable(portable, bytes + split,
                                     sizeof(bytes) - split) == whole);
    }
}

/* The dispatched path agrees with the portable one at every alignment and
 * length, from any chained starting value. */
static void test_paths_agree(uint64_t seed)
{
    static unsigned char bytes[4096 + 16];
    struct test_random random;

    test_random_seed(&random, seed, 1);
    for (size_t i = 0; i < sizeof(bytes); ++i) {
        bytes[i] = (unsigned char)test_random_next(&random);
    }
    for (uint32_t round = 0; round < 2000; ++round) {
        size_t offset = test_random_bounded(&random, 16);
        size_t size = test_random_bounded(&random, 4097);
        uint32_t start = round % 2 == 0 ? 0 : test_random_next(&random);

        CHECK(vsr_io_crc32c(start, bytes + offset, size) ==
              vsr_io_crc32c_portable(start, bytes + offset, size));
    }
}

int main(int argc, char **argv)
{
    uint64_t seed = 1;

    if (argc > 1) {
        seed = strtoull(argv[1], NULL, 0);
    }
    printf("crc32c seed %" PRIu64 "\n", seed);
    test_vectors();
    test_chaining();
    test_paths_agree(seed);
    return 0;
}
