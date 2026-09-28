#include "config.h"

#include "io/deadline.h"
#include "lib/check.h"
#include "lib/random.h"
#include "vsr.h"

#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define NONE UINT32_MAX
#define CAPACITY 48u

/* Handles are dense per kind: base[kind] + index. */
static const uint32_t kind_count[VSR_IO_DEADLINE_KINDS] = {12, 8,  4, 4,
                                                           4,  12, 4};
static uint32_t kind_base[VSR_IO_DEADLINE_KINDS];

static uint64_t memory[CAPACITY * 4];
static uint64_t model[CAPACITY]; /* Independent record of each handle. */

static void open_set(struct vsr_io_deadlines *set)
{
    size_t bytes = 0;
    uint32_t base = 0;

    CHECK(vsr_io_deadlines_size(CAPACITY, &bytes) == VSR_OK);
    CHECK(bytes <= sizeof(memory));
    memset(memory, 0xEE, sizeof(memory));
    vsr_io_deadlines_init(set, memory, CAPACITY);
    for (uint32_t kind = 0; kind < VSR_IO_DEADLINE_KINDS; ++kind) {
        kind_base[kind] = base;
        for (uint32_t index = 0; index < kind_count[kind]; ++index) {
            vsr_io_deadlines_bind(set, base + index, (uint16_t)kind, index);
        }
        base += kind_count[kind];
    }
    CHECK(base == CAPACITY);
    for (uint32_t i = 0; i < CAPACITY; ++i) {
        model[i] = VSR_NO_DEADLINE;
    }
    CHECK(vsr_io_deadlines_earliest(set) == VSR_NO_DEADLINE);
}

static uint32_t handle_of(uint16_t kind, uint32_t index)
{
    CHECK(kind < VSR_IO_DEADLINE_KINDS && index < kind_count[kind]);
    return kind_base[kind] + index;
}

/* The heap agrees with the model: same armed set, positions mirror the
 * heap, the heap order holds, and earliest is the linear minimum. */
static void check_model(const struct vsr_io_deadlines *set)
{
    uint64_t minimum = VSR_NO_DEADLINE;
    uint32_t armed = 0;

    for (uint32_t h = 0; h < CAPACITY; ++h) {
        const struct vsr_io_deadline_entry *entry = &set->entries[h];

        CHECK(entry->when == model[h]);
        if (model[h] == VSR_NO_DEADLINE) {
            CHECK(entry->position == NONE);
            continue;
        }
        armed++;
        if (model[h] < minimum) {
            minimum = model[h];
        }
        CHECK(entry->position < set->count);
        CHECK(set->heap[entry->position] == h);
    }
    CHECK(armed == set->count);
    for (uint32_t p = 1; p < set->count; ++p) {
        CHECK(set->entries[set->heap[(p - 1) / 2]].when <=
              set->entries[set->heap[p]].when);
    }
    CHECK(vsr_io_deadlines_earliest(set) == minimum);
}

/* The handle the model says pops next at now: least (when, handle). */
static uint32_t model_next(uint64_t now)
{
    uint32_t best = NONE;

    for (uint32_t h = 0; h < CAPACITY; ++h) {
        if (model[h] <= now && (best == NONE || model[h] < model[best])) {
            best = h;
        }
    }
    return best;
}

static void check_pop(struct vsr_io_deadlines *set, uint64_t now)
{
    uint32_t expect = model_next(now);
    uint16_t kind = UINT16_MAX;
    uint32_t index = NONE;

    if (expect == NONE) {
        CHECK(!vsr_io_deadlines_pop(set, now, &kind, &index));
        CHECK(kind == UINT16_MAX && index == NONE);
        return;
    }
    CHECK(vsr_io_deadlines_pop(set, now, &kind, &index));
    CHECK(handle_of(kind, index) == expect);
    model[expect] = VSR_NO_DEADLINE;
}

static void test_random(uint64_t seed)
{
    struct vsr_io_deadlines set;
    struct test_random random;

    test_random_seed(&random, seed, 2);
    open_set(&set);
    for (uint32_t step = 0; step < 50000; ++step) {
        uint32_t handle = test_random_bounded(&random, CAPACITY);
        uint32_t action = test_random_bounded(&random, 10);
        /* A narrow range forces ties; the far end tests large values. */
        uint64_t when = test_random_bounded(&random, 64);

        if (test_random_bounded(&random, 16) == 0) {
            when = VSR_NO_DEADLINE - 1 - when;
        }
        if (action < 5) {
            vsr_io_deadlines_arm(&set, handle, when);
            model[handle] = when;
        } else if (action < 8) {
            vsr_io_deadlines_arm(&set, handle, VSR_NO_DEADLINE);
            model[handle] = VSR_NO_DEADLINE;
        } else {
            check_pop(&set, test_random_bounded(&random, 80));
        }
        check_model(&set);
    }
}

/* Popping everything yields (when, handle) order, then nothing. */
static void test_pop_order(uint64_t seed)
{
    struct vsr_io_deadlines set;
    struct test_random random;
    uint64_t last_when = 0;
    uint32_t last_handle = 0;
    uint32_t popped = 0;
    uint16_t kind;
    uint32_t index;

    test_random_seed(&random, seed, 3);
    open_set(&set);
    /* Arm in a permuted handle order; 7 is coprime with CAPACITY. */
    for (uint32_t i = 0; i < CAPACITY; ++i) {
        uint32_t h = (i * 7u) % CAPACITY;

        model[h] = 1000 + test_random_bounded(&random, 16);
        vsr_io_deadlines_arm(&set, h, model[h]);
    }
    check_model(&set);
    /* Nothing is due before the earliest. */
    CHECK(!vsr_io_deadlines_pop(&set, vsr_io_deadlines_earliest(&set) - 1,
                                &kind, &index));
    while (vsr_io_deadlines_pop(&set, UINT64_MAX - 1, &kind, &index)) {
        uint32_t handle = handle_of(kind, index);
        uint64_t when = model[handle];

        CHECK(when != VSR_NO_DEADLINE);
        CHECK(popped == 0 || when > last_when ||
              (when == last_when && handle > last_handle));
        last_when = when;
        last_handle = handle;
        model[handle] = VSR_NO_DEADLINE;
        popped++;
        check_model(&set);
    }
    CHECK(popped == CAPACITY);
    CHECK(set.count == 0);
    CHECK(vsr_io_deadlines_earliest(&set) == VSR_NO_DEADLINE);
}

/* A periodic owner re-arms its popped entry; moving an armed entry both
 * earlier and later keeps the order. */
static void test_rearm(void)
{
    struct vsr_io_deadlines set;
    uint32_t flush = 0;
    uint16_t kind;
    uint32_t index;

    open_set(&set);
    vsr_io_deadlines_arm(&set, handle_of(VSR_IO_DEADLINE_FLUSH, 2), 100);
    model[handle_of(VSR_IO_DEADLINE_FLUSH, 2)] = 100;
    vsr_io_deadlines_arm(&set, handle_of(VSR_IO_DEADLINE_LINK, 5), 250);
    model[handle_of(VSR_IO_DEADLINE_LINK, 5)] = 250;
    for (uint64_t now = 0; now <= 1000; now += 10) {
        while (vsr_io_deadlines_pop(&set, now, &kind, &index)) {
            uint32_t handle = handle_of(kind, index);

            CHECK(model[handle] <= now);
            model[handle] = VSR_NO_DEADLINE;
            if (kind == VSR_IO_DEADLINE_FLUSH) {
                CHECK(index == 2);
                flush++;
                vsr_io_deadlines_arm(&set, handle, now + 100);
                model[handle] = now + 100;
            } else {
                CHECK(kind == VSR_IO_DEADLINE_LINK && index == 5);
                CHECK(now == 250);
            }
            check_model(&set);
        }
    }
    CHECK(flush == 10);
    CHECK(model[handle_of(VSR_IO_DEADLINE_LINK, 5)] == VSR_NO_DEADLINE);

    /* Move the flush earlier and later than the link, and back. */
    vsr_io_deadlines_arm(&set, handle_of(VSR_IO_DEADLINE_LINK, 5), 2000);
    model[handle_of(VSR_IO_DEADLINE_LINK, 5)] = 2000;
    vsr_io_deadlines_arm(&set, handle_of(VSR_IO_DEADLINE_FLUSH, 2), 3000);
    model[handle_of(VSR_IO_DEADLINE_FLUSH, 2)] = 3000;
    check_model(&set);
    CHECK(vsr_io_deadlines_earliest(&set) == 2000);
    vsr_io_deadlines_arm(&set, handle_of(VSR_IO_DEADLINE_FLUSH, 2), 1500);
    model[handle_of(VSR_IO_DEADLINE_FLUSH, 2)] = 1500;
    check_model(&set);
    CHECK(vsr_io_deadlines_earliest(&set) == 1500);
}

/* VSR_NO_DEADLINE disarms, an extreme finite deadline is armed, and an
 * empty set reports no deadline. */
static void test_no_deadline(void)
{
    struct vsr_io_deadlines set;
    uint32_t core = handle_of(VSR_IO_DEADLINE_CORE, 3);
    uint16_t kind;
    uint32_t index;

    open_set(&set);
    CHECK(!vsr_io_deadlines_pop(&set, UINT64_MAX, &kind, &index));
    vsr_io_deadlines_arm(&set, core, VSR_NO_DEADLINE);
    CHECK(set.count == 0);
    vsr_io_deadlines_arm(&set, core, VSR_NO_DEADLINE - 1);
    model[core] = VSR_NO_DEADLINE - 1;
    check_model(&set);
    CHECK(!vsr_io_deadlines_pop(&set, VSR_NO_DEADLINE - 2, &kind, &index));
    vsr_io_deadlines_arm(&set, core, VSR_NO_DEADLINE);
    model[core] = VSR_NO_DEADLINE;
    check_model(&set);
    CHECK(vsr_io_deadlines_earliest(&set) == VSR_NO_DEADLINE);
    vsr_io_deadlines_arm(&set, core, VSR_NO_DEADLINE - 1);
    CHECK(vsr_io_deadlines_pop(&set, VSR_NO_DEADLINE - 1, &kind, &index));
    CHECK(kind == VSR_IO_DEADLINE_CORE && index == 3);
    CHECK(set.entries[core].when == VSR_NO_DEADLINE);
    CHECK(set.count == 0);
}

static void test_size(void)
{
    struct vsr_io_deadlines set;
    size_t bytes = 1;

    CHECK(vsr_io_deadlines_size(0, &bytes) == VSR_OK);
    CHECK(bytes == 0);
    vsr_io_deadlines_init(&set, NULL, 0);
    CHECK(vsr_io_deadlines_earliest(&set) == VSR_NO_DEADLINE);
    CHECK(vsr_io_deadlines_size(10, &bytes) == VSR_OK);
    CHECK(bytes ==
          10 * (sizeof(struct vsr_io_deadline_entry) + sizeof(uint32_t)));
    bytes = 7;
    CHECK(vsr_io_deadlines_size(UINT32_MAX, &bytes) == VSR_ELIMIT);
    CHECK(bytes == 7);
}

int main(int argc, char **argv)
{
    uint64_t seed = 1;

    if (argc > 1) {
        seed = strtoull(argv[1], NULL, 0);
    }
    printf("deadline seed %" PRIu64 "\n", seed);
    test_size();
    test_random(seed);
    test_pop_order(seed);
    test_rearm();
    test_no_deadline();
    return 0;
}
