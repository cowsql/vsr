/* sigaction, sigsetjmp, and _exit for campaign mode under -std=c11. */
#define _POSIX_C_SOURCE 200809L

#include "config.h"

#include "fuzzy/scenario.h"
#include "lib/check.h"
#include "lib/random.h"

#include <errno.h>
#include <inttypes.h>
#include <setjmp.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#ifndef CLUSTER_DEFAULT_COUNT
#define CLUSTER_DEFAULT_COUNT 32
#endif
#ifndef CLUSTER_DEFAULT_PROFILE
#define CLUSTER_DEFAULT_PROFILE 0
#endif

static uint32_t choose_pcg(void *context, uint32_t limit)
{
    return test_random_bounded(context, limit);
}

static void run(uint64_t seed, uint32_t steps, bool trace, bool quiet,
                uint32_t profile)
{
    struct test_random random;
    const struct scenario_source source = {choose_pcg, &random};
    const struct scenario_options options = {seed, steps, profile, trace,
                                             quiet};
    test_random_seed(&random, seed, 54);
    scenario_run(&options, &source);
}

/* Campaign mode: a failed CHECK aborts; the handler resumes the campaign at
 * the next seed instead of ending the process. The abandoned cluster leaks
 * deliberately, so a failing campaign exits without the leak checker. */
static sigjmp_buf campaign_resume;
static volatile sig_atomic_t campaign_active;

static void on_abort(int signal)
{
    (void)signal;
    if (campaign_active)
        siglongjmp(campaign_resume, 1);
}

static int campaign(uint64_t seed, uint64_t seeds, uint32_t steps,
                    uint32_t profile)
{
    struct sigaction action = {.sa_handler = on_abort};
    uint64_t failures = 0;
    sigemptyset(&action.sa_mask);
    CHECK(sigaction(SIGABRT, &action, NULL) == 0);
    for (uint64_t i = 0; i < seeds; ++i) {
        if (sigsetjmp(campaign_resume, 1) == 0) {
            campaign_active = 1;
            run(seed + i, steps, false, true, profile);
            campaign_active = 0;
        } else {
            campaign_active = 0;
            ++failures;
            fprintf(stderr, "FAIL %s\n", scenario_header());
            fprintf(stderr, "replay: cluster %" PRIu64 " 1 %u trace %u\n",
                    seed + i, steps, profile);
        }
    }
    fprintf(stderr,
            "campaign profile=%u steps=%u seeds=%" PRIu64 "..%" PRIu64
            " passed=%" PRIu64 " failed=%" PRIu64 "\n",
            profile, steps, seed, seed + seeds - 1, seeds - failures, failures);
    if (failures != 0)
        _exit(1);
    return 0;
}

static uint64_t number(const char *text)
{
    char *end;
    errno = 0;
    uintmax_t result = strtoumax(text, &end, 10);
    CHECK(errno == 0 && *text != '\0' && *text != '-' && *end == '\0' &&
          result <= UINT64_MAX);
    return (uint64_t)result;
}

int main(int argc, char **argv)
{
    CHECK(argc <= 7);
    uint64_t seed = argc > 1 ? number(argv[1]) : 1;
    uint64_t count = argc > 2 ? number(argv[2]) : CLUSTER_DEFAULT_COUNT;
    uint64_t steps = argc > 3 ? number(argv[3]) : 600;
    uint64_t profile = argc > 5 ? number(argv[5]) : CLUSTER_DEFAULT_PROFILE;
    uint64_t seeds = argc > 6 ? number(argv[6]) : 0;
    CHECK(profile <= SCENARIO_PROFILE_MAX);
    CHECK(steps <= UINT32_MAX && count != 0 && count - 1 <= UINT64_MAX - seed);
    if (argc > 6) {
        CHECK(seeds != 0 && seeds - 1 <= UINT64_MAX - seed);
        CHECK(strcmp(argv[4], "quiet") == 0);
        return campaign(seed, seeds, (uint32_t)steps, (uint32_t)profile);
    }
    for (uint64_t i = 0; i < count; ++i)
        run(seed + i, (uint32_t)steps,
            argc > 4 && strcmp(argv[4], "trace") == 0, false,
            (uint32_t)profile);
    return 0;
}
