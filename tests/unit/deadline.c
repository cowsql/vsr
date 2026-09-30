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

/* The heap agrees with want[capacity]: same armed set, positions mirror
 * the heap, slots past count hold no handle, the heap order holds on
 * (when, handle), and earliest is the linear minimum. */
static void check_state(const struct vsr_io_deadlines *set, uint32_t capacity,
                        const uint64_t *want)
{
    uint64_t minimum = VSR_NO_DEADLINE;
    uint32_t armed = 0;

    CHECK(set->capacity == capacity);
    for (uint32_t h = 0; h < capacity; ++h) {
        const struct vsr_io_deadline_entry *entry = &set->entries[h];

        CHECK(entry->when == want[h]);
        if (want[h] == VSR_NO_DEADLINE) {
            CHECK(entry->position == NONE);
            continue;
        }
        armed++;
        if (want[h] < minimum) {
            minimum = want[h];
        }
        CHECK(entry->position < set->count);
        CHECK(set->heap[entry->position] == h);
    }
    CHECK(armed == set->count);
    for (uint32_t p = set->count; p < capacity; ++p) {
        CHECK(set->heap[p] == NONE);
    }
    for (uint32_t p = 1; p < set->count; ++p) {
        uint32_t parent = set->heap[(p - 1) / 2];
        uint32_t child = set->heap[p];
        uint64_t parent_when = set->entries[parent].when;
        uint64_t child_when = set->entries[child].when;

        CHECK(parent_when < child_when ||
              (parent_when == child_when && parent < child));
    }
    CHECK(vsr_io_deadlines_earliest(set) == minimum);
}

/* check_state on the model, and every binding is intact. */
static void check_model(const struct vsr_io_deadlines *set)
{
    check_state(set, CAPACITY, model);
    for (uint16_t kind = 0; kind < (uint16_t)VSR_IO_DEADLINE_KINDS; ++kind) {
        for (uint32_t index = 0; index < kind_count[kind]; ++index) {
            const struct vsr_io_deadline_entry *entry =
                &set->entries[kind_base[kind] + index];

            CHECK(entry->kind == kind && entry->index == index);
        }
    }
}

/* The armed handle of want[capacity] that pops next at now: least (when,
 * handle) among those due. */
static uint32_t next_due(uint32_t capacity, const uint64_t *want, uint64_t now)
{
    uint32_t best = NONE;

    for (uint32_t h = 0; h < capacity; ++h) {
        if (want[h] != VSR_NO_DEADLINE && want[h] <= now &&
            (best == NONE || want[h] < want[best])) {
            best = h;
        }
    }
    return best;
}

/* The handle the model says pops next at now: least (when, handle). */
static uint32_t model_next(uint64_t now)
{
    return next_due(CAPACITY, model, now);
}

/* Pops once at now and checks the handle against the model; true when an
 * entry popped. */
static bool check_pop(struct vsr_io_deadlines *set, uint64_t now)
{
    uint32_t expect = model_next(now);
    uint16_t kind = UINT16_MAX;
    uint32_t index = NONE;

    if (expect == NONE) {
        CHECK(!vsr_io_deadlines_pop(set, now, &kind, &index));
        CHECK(kind == UINT16_MAX && index == NONE);
        return false;
    }
    CHECK(vsr_io_deadlines_pop(set, now, &kind, &index));
    CHECK(handle_of(kind, index) == expect);
    model[expect] = VSR_NO_DEADLINE;
    return true;
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

/* Arms (or disarms) a handle and records it in the model. */
static void model_arm(struct vsr_io_deadlines *set, uint32_t handle,
                      uint64_t when)
{
    vsr_io_deadlines_arm(set, handle, when);
    model[handle] = when;
}

/* The first handle at or after start, wrapping, that is armed (or
 * disarmed); NONE when there is none. */
static uint32_t model_find(uint32_t start, bool armed)
{
    for (uint32_t i = 0; i < CAPACITY; ++i) {
        uint32_t h = (start + i) % CAPACITY;

        if ((model[h] != VSR_NO_DEADLINE) == armed) {
            return h;
        }
    }
    return NONE;
}

/* Near zero so that entries tie; now and then at the far end. */
static uint64_t walk_when(struct test_random *random)
{
    uint64_t when = test_random_bounded(random, 24);

    if (test_random_bounded(random, 16) == 0) {
        when = VSR_NO_DEADLINE - 1 - when;
    }
    return when;
}

/* A walk that names every move: insert, arm at a tie with an armed entry,
 * move earlier, later and to the same deadline, disarm armed and disarmed
 * entries, pop exactly at the earliest deadline and one before it, pop and
 * drain at a random now, and drain everything. Each move is taken often
 * enough to be counted. */
static void test_walk(uint64_t seed)
{
    struct vsr_io_deadlines set;
    struct test_random random;
    uint32_t taken[16] = {0};
    uint32_t tie_pops = 0;
    uint32_t full = 0;

    test_random_seed(&random, seed, 4);
    open_set(&set);
    for (uint32_t step = 0; step < 200000; ++step) {
        uint32_t move = test_random_bounded(&random, 16);
        uint32_t start = test_random_bounded(&random, CAPACITY);
        uint32_t armed = model_find(start, true);
        uint32_t disarmed = model_find(start, false);
        uint32_t count = set.count;
        uint64_t delta = 1 + test_random_bounded(&random, 8);
        uint64_t earliest = vsr_io_deadlines_earliest(&set);
        uint64_t now = test_random_bounded(&random, 32);

        if ((move >= 3 && move <= 7 && armed == NONE) ||
            ((move == 8 || move >= 14) && disarmed == NONE) ||
            ((move == 9 || move == 10) && earliest == VSR_NO_DEADLINE)) {
            move = 0;
        }
        taken[move]++;
        switch (move) {
        case 0:
        case 1:
        case 2: /* Arm any handle: insert or move. */
            model_arm(&set, start, walk_when(&random));
            break;
        case 3: /* Any handle at an armed entry's deadline: a tie. */
            model_arm(&set, test_random_bounded(&random, CAPACITY),
                      model[armed]);
            break;
        case 4: /* Earlier, down to zero. */
            model_arm(&set, armed,
                      model[armed] -
                          (delta < model[armed] ? delta : model[armed]));
            break;
        case 5: /* Later, up to the last finite deadline. */
            model_arm(&set, armed,
                      model[armed] +
                          (delta < VSR_NO_DEADLINE - 1 - model[armed]
                               ? delta
                               : VSR_NO_DEADLINE - 1 - model[armed]));
            break;
        case 6: /* The same deadline again. */
            model_arm(&set, armed, model[armed]);
            CHECK(set.count == count);
            break;
        case 7: /* Disarm an armed entry. */
            model_arm(&set, armed, VSR_NO_DEADLINE);
            CHECK(set.count == count - 1);
            break;
        case 8: /* Disarm a disarmed entry: nothing changes. */
            model_arm(&set, disarmed, VSR_NO_DEADLINE);
            CHECK(set.count == count);
            break;
        case 9:
        case 10: /* Exactly at the earliest: not before, then due. */
            if (earliest > 0) {
                CHECK(!check_pop(&set, earliest - 1));
            }
            CHECK(model_next(earliest) != NONE);
            CHECK(model[model_next(earliest)] == earliest);
            CHECK(check_pop(&set, earliest));
            if (vsr_io_deadlines_earliest(&set) == earliest) {
                tie_pops++;
            }
            break;
        case 11:
        case 12: /* One pop at a random now. */
            check_pop(&set, now);
            break;
        case 13: /* Drain at a random now; everything due goes. */
            while (check_pop(&set, now)) {
                check_model(&set);
            }
            CHECK(model_next(now) == NONE);
            CHECK(vsr_io_deadlines_earliest(&set) == VSR_NO_DEADLINE ||
                  vsr_io_deadlines_earliest(&set) > now);
            break;
        default: /* Insert a disarmed entry; rarely, drain everything or
                  * fill every disarmed entry. */
            if (test_random_bounded(&random, 64) == 0) {
                while (check_pop(&set, VSR_NO_DEADLINE - 1)) {
                }
                CHECK(set.count == 0);
                break;
            }
            if (test_random_bounded(&random, 64) == 0) {
                for (uint32_t h = 0; h < CAPACITY; ++h) {
                    if (model[h] == VSR_NO_DEADLINE) {
                        model_arm(&set, h, walk_when(&random));
                    }
                }
                CHECK(set.count == CAPACITY);
                break;
            }
            model_arm(&set, disarmed, walk_when(&random));
            CHECK(set.count == count + 1);
            break;
        }
        check_model(&set);
        if (set.count == CAPACITY) {
            full++;
        }
    }
    for (uint32_t move = 0; move < 16; ++move) {
        CHECK(taken[move] > 1000);
    }
    CHECK(tie_pops > 100);
    CHECK(full > 100);
}

#define SMALL 4u

static uint64_t small_memory[SMALL * 4];

/* Pops everything in want[n] one now at a time and checks each pop against
 * the least (when, handle) that is due; want ends all disarmed. */
static void small_drain(struct vsr_io_deadlines *set, uint32_t n,
                        uint64_t *want)
{
    uint16_t kind;
    uint32_t index;

    for (uint64_t now = 0; now < n; ++now) {
        for (;;) {
            uint32_t expect = next_due(n, want, now);

            if (expect == NONE) {
                CHECK(!vsr_io_deadlines_pop(set, now, &kind, &index));
                break;
            }
            CHECK(vsr_io_deadlines_earliest(set) == want[expect]);
            CHECK(vsr_io_deadlines_pop(set, now, &kind, &index));
            CHECK(kind == VSR_IO_DEADLINE_CORE && index == expect);
            want[expect] = VSR_NO_DEADLINE;
            check_state(set, n, want);
        }
    }
    CHECK(set->count == 0);
    CHECK(vsr_io_deadlines_earliest(set) == VSR_NO_DEADLINE);
}

/* Every capacity up to SMALL, every assignment of deadlines in [0, n),
 * armed in every order, then optionally shifted (which moves entries both
 * earlier and later) and optionally with one entry disarmed: the pops at
 * each now are exactly the due entries in (when, handle) order, whatever
 * the order of arming. */
static void test_exhaustive(void)
{
    struct vsr_io_deadlines set;
    uint32_t cases = 0;

    for (uint32_t n = 1; n <= SMALL; ++n) {
        uint32_t total = 1;
        size_t bytes = 0;

        for (uint32_t i = 0; i < n; ++i) {
            total *= n;
        }
        CHECK(vsr_io_deadlines_size(n, &bytes) == VSR_OK);
        CHECK(bytes <= sizeof(small_memory));
        for (uint32_t values = 0; values < total; ++values) {
            for (uint32_t order = 0; order < total; ++order) {
                uint32_t perm[SMALL];
                uint32_t seen = 0;
                uint32_t v = values;
                uint32_t o = order;
                uint64_t at[SMALL];

                /* Decode base n digits; order must be a permutation. */
                for (uint32_t i = 0; i < n; ++i) {
                    at[i] = v % n;
                    v /= n;
                    perm[i] = o % n;
                    o /= n;
                    seen |= 1u << perm[i];
                }
                if (seen != (1u << n) - 1) {
                    continue;
                }
                for (uint32_t shift = 0; shift < n; ++shift) {
                    for (uint32_t drop = 0; drop < 2; ++drop) {
                        uint64_t want[SMALL];

                        memset(small_memory, 0xEE, sizeof(small_memory));
                        vsr_io_deadlines_init(&set, small_memory, n);
                        for (uint32_t h = 0; h < n; ++h) {
                            vsr_io_deadlines_bind(&set, h, VSR_IO_DEADLINE_CORE,
                                                  h);
                            want[h] = VSR_NO_DEADLINE;
                        }
                        for (uint32_t i = 0; i < n; ++i) {
                            want[perm[i]] = at[perm[i]];
                            vsr_io_deadlines_arm(&set, perm[i], at[perm[i]]);
                            check_state(&set, n, want);
                        }
                        for (uint32_t i = 0; shift > 0 && i < n; ++i) {
                            uint32_t h = perm[i];

                            want[h] = (at[h] + shift) % n;
                            vsr_io_deadlines_arm(&set, h, want[h]);
                            check_state(&set, n, want);
                        }
                        if (drop) {
                            want[perm[n - 1]] = VSR_NO_DEADLINE;
                            vsr_io_deadlines_arm(&set, perm[n - 1],
                                                 VSR_NO_DEADLINE);
                            check_state(&set, n, want);
                        }
                        small_drain(&set, n, want);
                        cases++;
                    }
                }
            }
        }
    }
    /* Sum over n of n^n * n! * n * 2. */
    CHECK(cases ==
          1 * 1 * 1 * 2 + 4 * 2 * 2 * 2 + 27 * 6 * 3 * 2 + 256 * 24 * 4 * 2);
}

/* Every handle at one deadline, armed from the highest handle down: they
 * pop in handle order, and none before the deadline. */
static void test_all_ties(void)
{
    struct vsr_io_deadlines set;

    open_set(&set);
    for (uint32_t h = CAPACITY; h-- > 0;) {
        model_arm(&set, h, 500);
    }
    check_model(&set);
    CHECK(set.count == CAPACITY);
    CHECK(!check_pop(&set, 499));
    for (uint32_t h = 0; h < CAPACITY; ++h) {
        CHECK(model_next(500) == h);
        CHECK(check_pop(&set, 500));
        check_model(&set);
    }
    CHECK(!check_pop(&set, 500));
    CHECK(set.count == 0);
}

/* A pop at UINT64_MAX takes every armed entry, the last finite deadline
 * included, and never a disarmed one; an entry moved to the root and
 * then past every other one leaves in the right place; a periodic owner
 * that re-arms at now is due again at once. */
static void test_edges(void)
{
    struct vsr_io_deadlines set;
    uint32_t low = handle_of(VSR_IO_DEADLINE_LINK, 0);
    uint32_t mid = handle_of(VSR_IO_DEADLINE_STREAM, 4);
    uint32_t high = handle_of(VSR_IO_DEADLINE_CAPTURE, 3);

    open_set(&set);
    model_arm(&set, high, VSR_NO_DEADLINE - 1);
    model_arm(&set, mid, VSR_NO_DEADLINE - 1);
    model_arm(&set, low, 7);
    for (uint32_t h = 0; h < CAPACITY; h += 3) {
        if (model[h] == VSR_NO_DEADLINE) {
            model_arm(&set, h, 100 + h);
        }
    }
    check_model(&set);
    /* To the root, then past everything, then back into the middle. */
    model_arm(&set, mid, 0);
    check_model(&set);
    CHECK(vsr_io_deadlines_earliest(&set) == 0);
    model_arm(&set, mid, VSR_NO_DEADLINE - 1);
    check_model(&set);
    model_arm(&set, mid, 120);
    check_model(&set);
    /* The last finite deadline ties with high and is not due before
     * itself; at UINT64_MAX the two go in handle order and nothing
     * disarmed follows. */
    model_arm(&set, mid, VSR_NO_DEADLINE - 1);
    while (check_pop(&set, VSR_NO_DEADLINE - 2)) {
        check_model(&set);
    }
    CHECK(set.count == 2);
    CHECK(vsr_io_deadlines_earliest(&set) == VSR_NO_DEADLINE - 1);
    CHECK(mid < high && model_next(UINT64_MAX) == mid);
    CHECK(check_pop(&set, UINT64_MAX));
    CHECK(check_pop(&set, UINT64_MAX));
    CHECK(!check_pop(&set, UINT64_MAX));
    check_model(&set);
    CHECK(set.count == 0);
    CHECK(model[mid] == VSR_NO_DEADLINE && model[high] == VSR_NO_DEADLINE);

    /* Re-armed at or before now, a popped entry is due again at once;
     * after now, it is not. */
    model_arm(&set, low, 50);
    CHECK(check_pop(&set, 60));
    model_arm(&set, low, 60);
    CHECK(check_pop(&set, 60));
    model_arm(&set, low, 61);
    CHECK(!check_pop(&set, 60));
    check_model(&set);
}

/* Decision G2: entries armed while `rebasing` is set move by the delta of
 * the next rebase, once; entries armed outside it, re-armed outside it
 * since, or disarmed, do not; a delta of zero only clears the marks; the
 * move saturates below VSR_NO_DEADLINE; the heap order holds after. */
static void test_rebase(uint64_t seed)
{
    struct vsr_io_deadlines set;
    struct test_random random;
    uint32_t a = handle_of(VSR_IO_DEADLINE_LINK, 1);
    uint32_t b = handle_of(VSR_IO_DEADLINE_LINK, 2);
    uint32_t c = handle_of(VSR_IO_DEADLINE_STREAM, 0);
    uint32_t d = handle_of(VSR_IO_DEADLINE_DIAL, 3);
    uint32_t e = handle_of(VSR_IO_DEADLINE_CORE, 0);

    open_set(&set);
    CHECK(set.rebasing == 0 && set.marked == 0);
    model_arm(&set, e, 500); /* Armed at a poll: stays. */
    set.rebasing = 1;
    model_arm(&set, a, 100);
    model_arm(&set, b, 200);
    model_arm(&set, c, 300);
    model_arm(&set, d, VSR_NO_DEADLINE - 10);
    model_arm(&set, c, VSR_NO_DEADLINE); /* Disarmed: unmarked. */
    set.rebasing = 0;
    model_arm(&set, b, 250); /* Re-armed at a poll: unmarked. */
    CHECK(set.marked == 2);
    vsr_io_deadlines_rebase(&set, 1000);
    model[a] = 1100;
    model[d] = VSR_NO_DEADLINE - 1;
    check_model(&set);
    CHECK(set.marked == 0);
    /* Once only. */
    vsr_io_deadlines_rebase(&set, 1000);
    check_model(&set);
    /* Zero clears the marks and moves nothing. */
    set.rebasing = 1;
    model_arm(&set, a, 42);
    set.rebasing = 0;
    CHECK(set.marked == 1);
    vsr_io_deadlines_rebase(&set, 0);
    CHECK(set.marked == 0);
    check_model(&set);
    vsr_io_deadlines_rebase(&set, 77);
    check_model(&set);
    /* Random marks and deltas against the model. */
    test_random_seed(&random, seed, 0x5EBA5E);
    for (uint32_t round = 0; round < 200; ++round) {
        uint64_t delta = test_random_bounded(&random, 1000);
        bool marked[CAPACITY];

        memset(marked, 0, sizeof(marked));
        for (uint32_t i = 0; i < 12; ++i) {
            uint32_t h = test_random_bounded(&random, CAPACITY);
            bool mark = test_random_bounded(&random, 2) == 0;
            uint64_t when = test_random_bounded(&random, 8) == 0
                                ? VSR_NO_DEADLINE
                                : 1000u + test_random_bounded(&random, 5000);

            set.rebasing = mark ? 1u : 0u;
            model_arm(&set, h, when);
            marked[h] = mark && when != VSR_NO_DEADLINE;
        }
        set.rebasing = 0;
        vsr_io_deadlines_rebase(&set, delta);
        for (uint32_t h = 0; h < CAPACITY; ++h) {
            if (marked[h]) {
                model[h] += delta;
            }
        }
        check_model(&set);
        CHECK(set.marked == 0);
    }
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
    test_walk(seed);
    test_exhaustive();
    test_all_ties();
    test_edges();
    test_rebase(seed);
    return 0;
}
