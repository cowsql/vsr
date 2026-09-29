/*
 * The store's recovery under libFuzzer: the random walk of the unit
 * harness (tests/unit/store.c), driven by the input bytes instead of a
 * seeded generator. Each input builds a log image through a script of
 * STOREs, SYNCs, RECLAIMs, LOADs and crashes with torn writes, lost
 * unflushed blocks or flipped bytes, recovers it after every crash and
 * compares the verdict with the harness's independent reading of the raw
 * bytes (checked_image), the recovered indexes with the pre-crash
 * snapshot and the segment table with the checker's; the model's log and
 * client records must read back after every recovery; nothing may crash
 * or read out of bounds (ASan). Every failure is a bug in the store or in
 * the checker. The seed corpus is what the unit test's default seeds
 * write under VSR_RECOVERY_CORPUS (make fuzz does that).
 */

#define VSR_STORE_TESTS_MAIN store_tests_main
int store_tests_main(int argc, char **argv);
#include "../unit/store.c" /* NOLINT(bugprone-suspicious-include) */

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    if (size == 0) {
        return 0;
    }
    walk_run(data, size);
    return 0;
}
