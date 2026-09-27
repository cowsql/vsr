#ifndef VSR_TEST_SCENARIO_H
#define VSR_TEST_SCENARIO_H

#include <stdbool.h>
#include <stdint.h>

/* Seeded fault scenario shared by the PCG-driven scheduler and the libFuzzer
 * harness. Every nondeterministic decision is one bounded choice taken from the
 * source, so a run is a pure function of the options and the choice sequence:
 * no pointer hashing, no wall clock, and trace output uses stable indices. */

/* Profile flags combine by addition. Profiles 0..7 keep their event schedules
 * stable for existing seeds; each higher flag consumes choices only when set. */
enum scenario_flags {
    SCENARIO_WARM = 1,        /* Begin with committed commands/checkpoints. */
    SCENARIO_MINIMAL = 2,     /* Exact minimum lease and payload budgets. */
    SCENARIO_PARTITION = 4,   /* Changing network partitions. */
    SCENARIO_HARSH = 8,       /* Crash in any state, up to f unavailable. */
    SCENARIO_IO_FAULTS = 16,  /* Transient RETRY/NOT_FOUND on retryable I/O. */
    SCENARIO_MEMBERSHIP = 32, /* Learners, RECONFIGURE, CHECK_EPOCH. */
    SCENARIO_READS = 64,      /* Read barriers issued right before faults. */
    SCENARIO_PROFILE_MAX = 127
};

struct scenario_source {
    /* Uniform-looking choice in [0, limit); limit > 0. Deterministic. */
    uint32_t (*choose)(void *context, uint32_t limit);
    void *context;
};

struct scenario_options {
    uint64_t seed; /* Run identity; also selects warm-up checkpoints. */
    uint32_t steps;
    uint32_t profile;
    bool trace; /* Print every action to stderr. */
    bool quiet; /* Suppress the configuration header. */
};

/* Runs one scenario to completion, including the liveness check after faults
 * cease. Any violation fails through CHECK (abort). */
void scenario_run(const struct scenario_options *options,
                  const struct scenario_source *source);

/* Configuration header of the most recent or current run, for failure reports
 * when the header itself was suppressed. */
const char *scenario_header(void);

#endif /* VSR_TEST_SCENARIO_H */
