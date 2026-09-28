#include "config.h"

#include "io/link.h"
#include "io/slots.h"
#include "lib/check.h"
#include "vsr-io.h"

#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define NONE UINT32_MAX
#define OWNER 0x5Au
#define CAPACITY 16u

static struct vsr_io_slot memory[CAPACITY];

/* user_data fields below the owner tag, per the layout in slots.h. */
static uint32_t ud_kind(uint64_t user_data)
{
    return (uint32_t)(user_data >> 48) & 0xFFu;
}

static uint32_t ud_index(uint64_t user_data)
{
    return (uint32_t)(user_data >> 24) & 0xFFFFFFu;
}

static uint32_t ud_generation(uint64_t user_data)
{
    return (uint32_t)user_data & 0xFFFFFFu;
}

static void open_table(struct vsr_io_slots *table, uint32_t count)
{
    memset(memory, 0xEE, sizeof(memory));
    vsr_io_slots_init(table, memory, count, OWNER);
    CHECK(table->count == count);
    CHECK(table->free_count == count);
    CHECK(table->rejected == 0);
}

static void test_size(void)
{
    struct vsr_io_limits limits;
    size_t bytes = 0;
    uint32_t count = 0;

    memset(&limits, 0, sizeof(limits));
    CHECK(vsr_io_slots_size(&limits, 0, 0, &bytes, &count) == VSR_OK);
    CHECK(count == 8 && bytes == 8 * sizeof(struct vsr_io_slot));

    /* listeners + 7 links + streams (window + 2) + replicas (inflight + 8)
     * + 8 = 1 + 14 + 5 + 2 * 12 + 8. */
    limits.links = 2;
    limits.streams = 1;
    limits.stream_window = 3;
    limits.replicas = 2;
    CHECK(vsr_io_slots_size(&limits, 1, 4, &bytes, &count) == VSR_OK);
    CHECK(count == 52 && bytes == 52 * sizeof(struct vsr_io_slot));

    /* The 24-bit index bounds the table exactly. */
    memset(&limits, 0, sizeof(limits));
    CHECK(vsr_io_slots_size(&limits, VSR_IO_SLOTS_MAX - 8, 0, &bytes, &count) ==
          VSR_OK);
    CHECK(count == VSR_IO_SLOTS_MAX);
    bytes = 1;
    count = 1;
    CHECK(vsr_io_slots_size(&limits, VSR_IO_SLOTS_MAX - 7, 0, &bytes, &count) ==
          VSR_ELIMIT);
    CHECK(bytes == 1 && count == 1);

    /* Products that overflow 32 bits, or even size_t, are ELIMIT. */
    limits.links = UINT32_MAX;
    CHECK(vsr_io_slots_size(&limits, 0, 0, &bytes, &count) == VSR_ELIMIT);
    memset(&limits, 0, sizeof(limits));
    limits.streams = UINT32_MAX;
    limits.stream_window = UINT32_MAX;
    limits.replicas = UINT32_MAX;
    CHECK(vsr_io_slots_size(&limits, UINT32_MAX, UINT32_MAX, &bytes, &count) ==
          VSR_ELIMIT);
    memset(&limits, 0, sizeof(limits));
    limits.replicas = 1;
    CHECK(vsr_io_slots_size(&limits, 0, UINT32_MAX, &bytes, &count) ==
          VSR_ELIMIT);
}

static void test_exhaustion(void)
{
    struct vsr_io_slots table;
    uint64_t user_data[CAPACITY];
    bool seen[CAPACITY];

    open_table(&table, CAPACITY);
    memset(seen, 0, sizeof(seen));
    for (uint32_t i = 0; i < CAPACITY; ++i) {
        uint32_t index = vsr_io_slots_alloc(&table, VSR_IO_SLOT_WRITE, 1, i,
                                            i * 3u, (uint64_t)i << 40);

        CHECK(index < CAPACITY && !seen[index]);
        seen[index] = true;
        user_data[i] = vsr_io_slots_user_data(&table, index);
        CHECK(VSR_IO_OWNER(user_data[i]) == OWNER);
        CHECK(ud_kind(user_data[i]) == VSR_IO_SLOT_WRITE);
        CHECK(ud_index(user_data[i]) == index);
        CHECK(table.free_count == CAPACITY - 1 - i);
    }
    CHECK(vsr_io_slots_alloc(&table, VSR_IO_SLOT_WRITE, 1, 0, 0, 0) == NONE);
    CHECK(vsr_io_slots_alloc(&table, VSR_IO_SLOT_FLUSH, 1, 0, 0, 0) == NONE);
    CHECK(table.free_count == 0);

    /* Every slot resolves to itself with its payload intact. */
    for (uint32_t i = 0; i < CAPACITY; ++i) {
        uint32_t index = NONE;
        struct vsr_io_slot *slot =
            vsr_io_slots_resolve(&table, user_data[i], &index);

        CHECK(slot != NULL && index == ud_index(user_data[i]));
        CHECK(slot == &table.slots[index]);
        CHECK(slot->kind == VSR_IO_SLOT_WRITE && slot->expected == 1);
        CHECK(slot->owner == i && slot->sub == i * 3u);
        CHECK(slot->cookie == (uint64_t)i << 40);
    }
    CHECK(table.rejected == 0);

    /* Consuming the single expected completion frees each slot. */
    for (uint32_t i = 0; i < CAPACITY; ++i) {
        vsr_io_slots_consumed(&table, ud_index(user_data[i]), false);
    }
    CHECK(table.free_count == CAPACITY);
    for (uint32_t i = 0; i < CAPACITY; ++i) {
        uint32_t index = NONE;

        CHECK(vsr_io_slots_resolve(&table, user_data[i], &index) == NULL);
        CHECK(index == NONE);
    }
    CHECK(table.rejected == CAPACITY);
}

static void test_reuse_generation(void)
{
    struct vsr_io_slots table;
    uint32_t index;
    uint32_t again;
    uint32_t resolved = NONE;
    uint64_t stale;
    uint64_t fresh;

    open_table(&table, 4);
    index = vsr_io_slots_alloc(&table, VSR_IO_SLOT_RECV, 1, 7, 0, 0);
    stale = vsr_io_slots_user_data(&table, index);
    vsr_io_slots_free(&table, index);
    CHECK(table.free_count == 4);
    /* A freed slot is left alone by a second free or a late consume. */
    vsr_io_slots_free(&table, index);
    vsr_io_slots_consumed(&table, index, false);
    CHECK(table.free_count == 4);

    again = vsr_io_slots_alloc(&table, VSR_IO_SLOT_RECV, 1, 7, 0, 0);
    CHECK(again == index); /* The free list is LIFO. */
    fresh = vsr_io_slots_user_data(&table, again);
    CHECK(fresh != stale);
    CHECK(ud_index(fresh) == ud_index(stale));
    CHECK(ud_generation(fresh) == ud_generation(stale) + 1);

    /* The recycled slot's late completion is stale and counted. */
    CHECK(vsr_io_slots_resolve(&table, stale, &resolved) == NULL);
    CHECK(table.rejected == 1);
    CHECK(vsr_io_slots_resolve(&table, fresh, &resolved) != NULL);
    CHECK(resolved == again);
    CHECK(table.rejected == 1);

    /* The generation is 24 bits and wraps. */
    table.slots[again].generation = 0xFFFFFFu;
    vsr_io_slots_free(&table, again);
    CHECK(table.slots[again].generation == 0);
    again = vsr_io_slots_alloc(&table, VSR_IO_SLOT_SEND, 2, 0, 0, 0);
    CHECK(ud_generation(vsr_io_slots_user_data(&table, again)) == 0);
}

static void test_foreign_and_forged(void)
{
    struct vsr_io_slots table;
    uint32_t index;
    uint32_t resolved = NONE;
    uint64_t user_data;
    uint64_t below;

    open_table(&table, 4);
    index = vsr_io_slots_alloc(&table, VSR_IO_SLOT_CONNECT, 1, 0, 0, 0);
    user_data = vsr_io_slots_user_data(&table, index);
    below = user_data & ((UINT64_C(1) << VSR_IO_OWNER_SHIFT) - 1);

    /* Another owner's record with bits identical below the tag. */
    CHECK(vsr_io_slots_resolve(&table, VSR_IO_USER_DATA(OWNER + 1, below),
                               &resolved) == NULL);
    CHECK(vsr_io_slots_resolve(&table, VSR_IO_USER_DATA(0, below), &resolved) ==
          NULL);
    CHECK(table.rejected == 2);
    /* A kind that is not the slot's. */
    CHECK(vsr_io_slots_resolve(&table,
                               (user_data & ~(UINT64_C(0xFF) << 48)) |
                                   (uint64_t)VSR_IO_SLOT_LISTEN << 48,
                               &resolved) == NULL);
    /* An index past the table and a FREE slot. */
    CHECK(vsr_io_slots_resolve(&table,
                               (user_data & ~(UINT64_C(0xFFFFFF) << 24)) |
                                   UINT64_C(4) << 24,
                               &resolved) == NULL);
    CHECK(vsr_io_slots_resolve(&table,
                               (user_data & ~(UINT64_C(0xFFFFFF) << 24)) |
                                   (uint64_t)((index + 1) % 4) << 24,
                               &resolved) == NULL);
    CHECK(table.rejected == 5);
    CHECK(resolved == NONE);
    CHECK(vsr_io_slots_resolve(&table, user_data, &resolved) != NULL);
    CHECK(resolved == index);
}

static void test_multishot(void)
{
    struct vsr_io_slots table;
    uint32_t index;
    uint32_t resolved;
    uint64_t user_data;

    open_table(&table, 2);
    index = vsr_io_slots_alloc(&table, VSR_IO_SLOT_RECV, 1, 3, 0, 0);
    user_data = vsr_io_slots_user_data(&table, index);
    for (int i = 0; i < 100; ++i) {
        CHECK(vsr_io_slots_resolve(&table, user_data, &resolved) != NULL);
        vsr_io_slots_consumed(&table, resolved, true);
        CHECK(table.slots[index].kind == VSR_IO_SLOT_RECV);
        CHECK(table.slots[index].expected == 1);
        CHECK(table.free_count == 1);
    }
    /* The completion without MORE ends the record. */
    CHECK(vsr_io_slots_resolve(&table, user_data, &resolved) != NULL);
    vsr_io_slots_consumed(&table, resolved, false);
    CHECK(table.free_count == 2);
    CHECK(vsr_io_slots_resolve(&table, user_data, &resolved) == NULL);
}

static void test_zero_copy(void)
{
    struct vsr_io_slots table;
    uint32_t index;
    uint32_t resolved;
    uint64_t user_data;

    open_table(&table, 2);
    /* Result with MORE, then NOTIF without it: the flags verbatim. */
    index = vsr_io_slots_alloc(&table, VSR_IO_SLOT_SEND, 2, 1, 0, 0);
    user_data = vsr_io_slots_user_data(&table, index);
    CHECK(table.slots[index].expected == 2);
    CHECK(vsr_io_slots_resolve(&table, user_data, &resolved) != NULL);
    vsr_io_slots_consumed(&table, resolved, true);
    CHECK(table.slots[index].expected == 1);
    CHECK(table.slots[index].kind == VSR_IO_SLOT_SEND);
    CHECK(vsr_io_slots_resolve(&table, user_data, &resolved) != NULL);
    vsr_io_slots_consumed(&table, resolved, false);
    CHECK(table.free_count == 2);
    CHECK(vsr_io_slots_resolve(&table, user_data, &resolved) == NULL);

    /* Counting both without MORE also frees on the second. */
    index = vsr_io_slots_alloc(&table, VSR_IO_SLOT_SEND, 2, 1, 0, 0);
    user_data = vsr_io_slots_user_data(&table, index);
    vsr_io_slots_consumed(&table, index, false);
    CHECK(vsr_io_slots_resolve(&table, user_data, &resolved) != NULL);
    vsr_io_slots_consumed(&table, index, false);
    CHECK(table.free_count == 2);
    CHECK(vsr_io_slots_resolve(&table, user_data, &resolved) == NULL);

    /* An ordinary send expects one completion. */
    index = vsr_io_slots_alloc(&table, VSR_IO_SLOT_SEND, 1, 1, 0, 0);
    vsr_io_slots_consumed(&table, index, false);
    CHECK(table.free_count == 2);
}

/* Per link the engine holds a receive, a shutdown, a connect and every send
 * that may await its NOTIF (VSR_IO_LINK_SENDS in src/io/link.h), each in a
 * slot of its own. */
static void test_link_capacity(void)
{
    struct vsr_io_limits limits;
    size_t bytes = 0;
    uint32_t count = 0;

    memset(&limits, 0, sizeof(limits));
    limits.links = 5;
    CHECK(vsr_io_slots_size(&limits, 0, 0, &bytes, &count) == VSR_OK);
    CHECK(count == 8 + 5 * (3 + VSR_IO_LINK_SENDS));
    CHECK(bytes == count * sizeof(struct vsr_io_slot));
}

/* The index bound is exact: an entry just past the table, even one shaped
 * like a live slot, is never resolved. */
static void test_index_bound(void)
{
    struct vsr_io_slots table;
    uint32_t resolved = NONE;
    uint32_t index;
    uint64_t user_data;

    open_table(&table, 2);
    index = vsr_io_slots_alloc(&table, VSR_IO_SLOT_FLUSH, 1, 0, 0, 0);
    user_data = vsr_io_slots_user_data(&table, index);
    memory[2] = memory[index];
    user_data = (user_data & ~(UINT64_C(0xFFFFFF) << 24)) | UINT64_C(2) << 24;
    CHECK(vsr_io_slots_resolve(&table, user_data, &resolved) == NULL);
    CHECK(resolved == NONE && table.rejected == 1);
}

/* A zero-copy send refused before the kernel took it (SKIP_SUCCESS is
 * -EINVAL, per the executor contract) completes once, without MORE, and no
 * NOTIF follows. The table cannot tell that completion from the first of
 * two counted ones, so the caller frees the slot. */
static void test_zero_copy_refused(void)
{
    struct vsr_io_slots table;
    uint32_t index;
    uint32_t resolved = NONE;
    uint64_t user_data;

    open_table(&table, 2);
    index = vsr_io_slots_alloc(&table, VSR_IO_SLOT_SEND, 2, 1, 0, 0);
    user_data = vsr_io_slots_user_data(&table, index);
    CHECK(vsr_io_slots_resolve(&table, user_data, &resolved) != NULL);
    vsr_io_slots_consumed(&table, resolved, false);
    CHECK(table.slots[index].kind == VSR_IO_SLOT_SEND);
    CHECK(table.slots[index].expected == 1);
    CHECK(table.free_count == 1);
    vsr_io_slots_free(&table, index);
    CHECK(table.free_count == 2);
    CHECK(vsr_io_slots_resolve(&table, user_data, &resolved) == NULL);
    CHECK(table.rejected == 1);
}

/* The executor contract orders a zero-copy send's result before its NOTIF
 * (docs/io-implementation.md, section 8; the kernel posts the notification
 * after the request's completion). That order, flags verbatim, frees the
 * slot on the NOTIF (test_zero_copy). The reverse order is outside the
 * contract: after the NOTIF one completion is expected, and a MORE
 * completion then is indistinguishable from a multishot record's, so the
 * slot stays live until freed. */
static void test_zero_copy_order(void)
{
    struct vsr_io_slots table;
    uint32_t index;
    uint32_t resolved = NONE;
    uint64_t user_data;

    open_table(&table, 2);
    index = vsr_io_slots_alloc(&table, VSR_IO_SLOT_SEND, 2, 1, 0, 0);
    user_data = vsr_io_slots_user_data(&table, index);
    CHECK(vsr_io_slots_resolve(&table, user_data, &resolved) != NULL);
    vsr_io_slots_consumed(&table, resolved, false); /* NOTIF */
    CHECK(vsr_io_slots_resolve(&table, user_data, &resolved) != NULL);
    vsr_io_slots_consumed(&table, resolved, true); /* Result with MORE. */
    CHECK(table.slots[index].kind == VSR_IO_SLOT_SEND);
    CHECK(table.slots[index].expected == 1);
    CHECK(table.free_count == 1);
    vsr_io_slots_free(&table, index);
    CHECK(table.free_count == 2);
    CHECK(table.rejected == 0);
}

/* A LINK chain sharing one slot expects one completion per record that
 * completes on success; a SKIP_SUCCESS record counts none. After a failure
 * the count can reach zero before the chain's -ECANCELED rest, which is
 * then stale and counted. */
static void test_chain(void)
{
    struct vsr_io_slots table;
    uint32_t index;
    uint32_t resolved = NONE;
    uint64_t user_data;

    open_table(&table, 2);
    /* SOCKET then CONNECT: two completions without MORE. */
    index = vsr_io_slots_alloc(&table, VSR_IO_SLOT_CONNECT, 2, 4, 0, 0);
    user_data = vsr_io_slots_user_data(&table, index);
    CHECK(vsr_io_slots_resolve(&table, user_data, &resolved) != NULL);
    vsr_io_slots_consumed(&table, resolved, false);
    CHECK(table.slots[index].kind == VSR_IO_SLOT_CONNECT);
    CHECK(vsr_io_slots_resolve(&table, user_data, &resolved) != NULL);
    vsr_io_slots_consumed(&table, resolved, false);
    CHECK(table.free_count == 2);
    CHECK(vsr_io_slots_resolve(&table, user_data, &resolved) == NULL);
    CHECK(table.rejected == 1);

    /* SOCKET and BIND with SKIP_SUCCESS, LISTEN, then multishot ACCEPT:
     * LISTEN and ACCEPT count. */
    index = vsr_io_slots_alloc(&table, VSR_IO_SLOT_LISTEN, 2, 0, 0, 0);
    user_data = vsr_io_slots_user_data(&table, index);
    CHECK(vsr_io_slots_resolve(&table, user_data, &resolved) != NULL);
    vsr_io_slots_consumed(&table, resolved, false); /* LISTEN */
    for (int i = 0; i < 3; ++i) {
        CHECK(vsr_io_slots_resolve(&table, user_data, &resolved) != NULL);
        vsr_io_slots_consumed(&table, resolved, true); /* Accepted. */
        CHECK(table.slots[index].expected == 1);
    }
    CHECK(vsr_io_slots_resolve(&table, user_data, &resolved) != NULL);
    vsr_io_slots_consumed(&table, resolved, false); /* -ECANCELED */
    CHECK(table.free_count == 2);

    /* The same chain failing at SOCKET: SOCKET's error and BIND's
     * -ECANCELED end the slot; LISTEN's and ACCEPT's are stale. */
    index = vsr_io_slots_alloc(&table, VSR_IO_SLOT_LISTEN, 2, 0, 0, 0);
    user_data = vsr_io_slots_user_data(&table, index);
    for (int i = 0; i < 2; ++i) {
        CHECK(vsr_io_slots_resolve(&table, user_data, &resolved) != NULL);
        vsr_io_slots_consumed(&table, resolved, false);
    }
    CHECK(table.free_count == 2);
    CHECK(vsr_io_slots_resolve(&table, user_data, &resolved) == NULL);
    CHECK(vsr_io_slots_resolve(&table, user_data, &resolved) == NULL);
    CHECK(table.rejected == 3);
}

/* A multishot record freed while it still delivers (a cancelled receive
 * given up on): its late completions, with MORE or without, are stale even
 * once the slot is reused, and never touch the new record. */
static void test_freed_multishot(void)
{
    struct vsr_io_slots table;
    uint32_t index;
    uint32_t again;
    uint32_t resolved = NONE;
    uint64_t late;
    uint64_t fresh;
    struct vsr_io_slot *slot;

    open_table(&table, 1);
    index = vsr_io_slots_alloc(&table, VSR_IO_SLOT_RECV, 1, 9, 1, 11);
    late = vsr_io_slots_user_data(&table, index);
    CHECK(vsr_io_slots_resolve(&table, late, &resolved) != NULL);
    vsr_io_slots_consumed(&table, resolved, true);
    vsr_io_slots_free(&table, index);
    again = vsr_io_slots_alloc(&table, VSR_IO_SLOT_RECV, 1, 10, 2, 12);
    CHECK(again == index);
    fresh = vsr_io_slots_user_data(&table, again);
    CHECK(vsr_io_slots_resolve(&table, late, &resolved) == NULL);
    CHECK(vsr_io_slots_resolve(&table, late, &resolved) == NULL);
    CHECK(table.rejected == 2);
    slot = vsr_io_slots_resolve(&table, fresh, &resolved);
    CHECK(slot != NULL && resolved == again);
    CHECK(slot->expected == 1 && slot->owner == 10 && slot->sub == 2 &&
          slot->cookie == 12);
    CHECK(table.free_count == 0);
}

/* The generation has 24 bits, so a stale user_data is rejected through
 * 2^24 - 1 recycles of its slot and matches again at the 2^24th. No
 * completion stays in flight that long: a slot is freed after its last
 * completion, or at once for a record never submitted, and a chain's stale
 * rest is already in the completion queue, which holds far fewer than 2^24
 * entries (io_uring: at most 65536), when the slot is freed. */
static void test_generation_period(void)
{
    struct vsr_io_slots table;
    uint32_t index;
    uint32_t resolved = NONE;
    uint64_t stale;
    uint64_t user_data = 0;
    uint32_t matches = 0;

    open_table(&table, 1);
    index = vsr_io_slots_alloc(&table, VSR_IO_SLOT_WRITE, 1, 0, 0, 0);
    stale = vsr_io_slots_user_data(&table, index);
    for (uint32_t i = 1; i < VSR_IO_SLOTS_MAX; ++i) {
        vsr_io_slots_free(&table, index);
        CHECK(vsr_io_slots_alloc(&table, VSR_IO_SLOT_WRITE, 1, 0, 0, 0) ==
              index);
        user_data = vsr_io_slots_user_data(&table, index);
        matches += user_data == stale;
    }
    CHECK(matches == 0);
    CHECK(ud_generation(user_data) == 0xFFFFFFu);
    CHECK(vsr_io_slots_resolve(&table, stale, &resolved) == NULL);
    vsr_io_slots_free(&table, index);
    CHECK(vsr_io_slots_alloc(&table, VSR_IO_SLOT_WRITE, 1, 0, 0, 0) == index);
    CHECK(vsr_io_slots_user_data(&table, index) == stale);
    CHECK(vsr_io_slots_resolve(&table, stale, &resolved) != NULL);
    CHECK(table.rejected == 1);
}

/* ------------------------------------------------------------------------
 * Randomized model check: records of every completion shape share a small
 * table; each carries the script of MORE flags its completions will have,
 * so the model says when its slot must free without restating the counting
 * rule. Stale, forged and late completions, double frees and consumes of
 * free slots are mixed in. A user_data resolves exactly when it is a live
 * record's. Replay with ./tests/unit/slots SEED.
 * --------------------------------------------------------------------- */

#define MODEL_SLOTS 12u
#define MODEL_STEPS 200000u
#define MODEL_STALE 32u
#define MODEL_SCRIPT 16u

enum model_shape {
    SHAPE_SINGLE,         /* One completion. */
    SHAPE_MULTISHOT,      /* MORE..., then the final one. */
    SHAPE_ZERO_COPY,      /* Result with MORE, then NOTIF. */
    SHAPE_REFUSED,        /* Zero-copy refused: one completion, then free. */
    SHAPE_CHAIN,          /* Records of a chain, none multishot. */
    SHAPE_CHAIN_MULTI,    /* A chain ending in a multishot record. */
    SHAPE_CHAIN_ZERO_COPY /* A chain with a zero-copy send inside. */
};
#define MODEL_SHAPES 7u

struct model_record {
    bool live;
    uint8_t kind;
    uint8_t shape;
    uint32_t owner;
    uint32_t sub;
    uint64_t cookie;
    uint64_t user_data;
    uint8_t script[MODEL_SCRIPT]; /* MORE flag per completion. */
    uint32_t length;
    uint32_t next;
};

struct model {
    struct vsr_io_slots table;
    struct model_record records[MODEL_SLOTS];
    uint64_t stale[MODEL_STALE];
    uint32_t stale_count;
    uint32_t live;
    uint64_t rejected;
    uint64_t random;
};

/* splitmix64: deterministic on every target. */
static uint64_t model_next(struct model *m)
{
    uint64_t z = (m->random += UINT64_C(0x9E3779B97F4A7C15));

    z = (z ^ (z >> 30)) * UINT64_C(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)) * UINT64_C(0x94D049BB133111EB);
    return z ^ (z >> 31);
}

static uint32_t model_below(struct model *m, uint32_t bound)
{
    return (uint32_t)(model_next(m) % bound);
}

/* Builds a record's script; returns the completions to allocate. */
static uint8_t model_script(struct model *m, struct model_record *r)
{
    uint32_t n = 0;
    uint32_t before;
    uint32_t after;
    uint8_t completions = 1;

    switch (r->shape) {
    case SHAPE_SINGLE:
        r->script[n++] = 0;
        break;
    case SHAPE_MULTISHOT:
        for (uint32_t i = model_below(m, 7); i > 0; --i) {
            r->script[n++] = 1;
        }
        r->script[n++] = 0;
        break;
    case SHAPE_ZERO_COPY:
        r->script[n++] = 1;
        r->script[n++] = 0;
        completions = 2;
        break;
    case SHAPE_REFUSED:
        r->script[n++] = 0;
        completions = 2;
        break;
    case SHAPE_CHAIN:
        completions = (uint8_t)(2 + model_below(m, 3));
        for (uint32_t i = 0; i < completions; ++i) {
            r->script[n++] = 0;
        }
        break;
    case SHAPE_CHAIN_MULTI:
        completions = (uint8_t)(2 + model_below(m, 2));
        for (uint32_t i = 1; i < completions; ++i) {
            r->script[n++] = 0;
        }
        for (uint32_t i = model_below(m, 5); i > 0; --i) {
            r->script[n++] = 1;
        }
        r->script[n++] = 0;
        break;
    default: /* SHAPE_CHAIN_ZERO_COPY */
        before = model_below(m, 3);
        after = model_below(m, 3);
        completions = (uint8_t)(before + 2 + after);
        for (uint32_t i = 0; i < before; ++i) {
            r->script[n++] = 0;
        }
        r->script[n++] = 1;
        for (uint32_t i = 0; i <= after; ++i) {
            r->script[n++] = 0;
        }
        break;
    }
    CHECK(n <= MODEL_SCRIPT);
    r->length = n;
    r->next = 0;
    return completions;
}

static void model_bury(struct model *m, uint32_t index)
{
    struct model_record *r = &m->records[index];

    CHECK(r->live);
    r->live = false;
    m->live--;
    m->stale[m->stale_count % MODEL_STALE] = r->user_data;
    m->stale_count++;
}

static struct model_record *model_owner(struct model *m, uint64_t user_data,
                                        uint32_t *index)
{
    for (uint32_t i = 0; i < MODEL_SLOTS; ++i) {
        if (m->records[i].live && m->records[i].user_data == user_data) {
            *index = i;
            return &m->records[i];
        }
    }
    return NULL;
}

/* Resolves against the table and the model; returns the slot or NULL. */
static struct vsr_io_slot *model_resolve(struct model *m, uint64_t user_data,
                                         uint32_t *index)
{
    uint32_t expected = NONE;
    uint32_t resolved = NONE;
    struct model_record *r = model_owner(m, user_data, &expected);
    struct vsr_io_slot *slot =
        vsr_io_slots_resolve(&m->table, user_data, &resolved);

    if (r == NULL) {
        CHECK(slot == NULL && resolved == NONE);
        m->rejected++;
        return NULL;
    }
    CHECK(slot != NULL && resolved == expected);
    CHECK(slot == &m->table.slots[expected]);
    CHECK(slot->kind == r->kind && slot->owner == r->owner &&
          slot->sub == r->sub && slot->cookie == r->cookie);
    *index = resolved;
    return slot;
}

static void model_alloc(struct model *m)
{
    struct model_record probe;
    uint8_t kind = (uint8_t)(1 + model_below(m, VSR_IO_SLOT_KINDS - 1));
    uint32_t owner = model_below(m, 1000);
    uint32_t sub = model_below(m, 1000);
    uint64_t cookie = model_next(m);
    uint8_t completions;
    uint32_t index;
    struct model_record *r;

    memset(&probe, 0, sizeof(probe));
    probe.shape = (uint8_t)model_below(m, MODEL_SHAPES);
    completions = model_script(m, &probe);
    index =
        vsr_io_slots_alloc(&m->table, kind, completions, owner, sub, cookie);
    if (m->live == MODEL_SLOTS) {
        CHECK(index == NONE);
        return;
    }
    CHECK(index < MODEL_SLOTS && !m->records[index].live);
    r = &m->records[index];
    *r = probe;
    r->live = true;
    r->kind = kind;
    r->owner = owner;
    r->sub = sub;
    r->cookie = cookie;
    r->user_data = vsr_io_slots_user_data(&m->table, index);
    m->live++;
    CHECK(VSR_IO_OWNER(r->user_data) == OWNER);
    CHECK(ud_kind(r->user_data) == kind && ud_index(r->user_data) == index);
    for (uint32_t i = 0; i < MODEL_SLOTS; ++i) {
        CHECK(i == index || !m->records[i].live ||
              m->records[i].user_data != r->user_data);
    }
    for (uint32_t i = 0; i < MODEL_STALE && i < m->stale_count; ++i) {
        CHECK(m->stale[i] != r->user_data);
    }
}

/* The next scripted completion of a live record. */
static void model_complete(struct model *m)
{
    uint32_t pick = model_below(m, MODEL_SLOTS);
    uint32_t index = NONE;
    struct model_record *r;
    bool more;

    for (uint32_t i = 0; i < MODEL_SLOTS; ++i) {
        if (m->records[(pick + i) % MODEL_SLOTS].live) {
            pick = (pick + i) % MODEL_SLOTS;
            break;
        }
    }
    r = &m->records[pick];
    if (!r->live) {
        return;
    }
    CHECK(model_resolve(m, r->user_data, &index) != NULL && index == pick);
    more = r->script[r->next++] != 0;
    vsr_io_slots_consumed(&m->table, index, more);
    if (r->next < r->length) {
        CHECK(m->table.slots[index].kind == r->kind);
        return;
    }
    if (r->shape == SHAPE_REFUSED) {
        /* No NOTIF follows: the caller frees the slot. */
        CHECK(m->table.slots[index].kind == r->kind);
        vsr_io_slots_free(&m->table, index);
    }
    CHECK(m->table.slots[index].kind == VSR_IO_SLOT_FREE);
    model_bury(m, index);
}

/* A completion that may belong to nobody: stale, forged or random. */
static void model_stray(struct model *m)
{
    uint64_t user_data;
    uint32_t index = NONE;
    uint32_t choice = model_below(m, 4);

    if (choice == 0 && m->stale_count > 0) {
        uint32_t held =
            m->stale_count < MODEL_STALE ? m->stale_count : MODEL_STALE;

        user_data = m->stale[model_below(m, held)];
        CHECK(model_resolve(m, user_data, &index) == NULL);
        return;
    }
    if (choice == 1) {
        user_data = model_next(m);
    } else {
        struct model_record *r = &m->records[model_below(m, MODEL_SLOTS)];

        user_data = r->live || m->stale_count == 0
                        ? r->user_data
                        : m->stale[model_below(m, m->stale_count < MODEL_STALE
                                                      ? m->stale_count
                                                      : MODEL_STALE)];
        if (choice == 2) {
            user_data ^= UINT64_C(1) << model_below(m, 64);
        } else {
            /* Push the index past the table. */
            user_data |= (uint64_t)(MODEL_SLOTS + model_below(m, 1000)) << 24;
        }
    }
    (void)model_resolve(m, user_data, &index);
}

/* Frees and consumes by index, live or not. */
static void model_free(struct model *m)
{
    uint32_t index = model_below(m, MODEL_SLOTS);
    struct vsr_io_slot before = m->table.slots[index];
    uint32_t free_count = m->table.free_count;

    if (m->records[index].live) {
        /* Given up on (never submitted, or cancelled): its remaining
         * completions are stale from now on. */
        vsr_io_slots_free(&m->table, index);
        CHECK(m->table.slots[index].kind == VSR_IO_SLOT_FREE);
        CHECK(m->table.slots[index].generation ==
              ((before.generation + 1) & 0xFFFFFFu));
        model_bury(m, index);
        return;
    }
    if (model_below(m, 2) == 0) {
        vsr_io_slots_free(&m->table, index);
    } else {
        vsr_io_slots_consumed(&m->table, index, model_below(m, 2) == 0);
    }
    CHECK(m->table.free_count == free_count);
    CHECK(m->table.slots[index].generation == before.generation);
    CHECK(m->table.slots[index].kind == VSR_IO_SLOT_FREE);
}

static void model_verify(struct model *m)
{
    bool seen[MODEL_SLOTS];
    uint32_t walked = 0;

    CHECK(m->table.free_count == MODEL_SLOTS - m->live);
    CHECK(m->table.rejected == m->rejected);
    for (uint32_t i = 0; i < MODEL_SLOTS; ++i) {
        const struct model_record *r = &m->records[i];
        const struct vsr_io_slot *slot = &m->table.slots[i];

        CHECK(slot->generation <= 0xFFFFFFu);
        if (r->live) {
            CHECK(slot->kind == r->kind);
            CHECK(vsr_io_slots_user_data(&m->table, i) == r->user_data);
        } else {
            CHECK(slot->kind == VSR_IO_SLOT_FREE);
        }
    }
    /* The free list holds exactly the free slots, once each. */
    memset(seen, 0, sizeof(seen));
    for (uint32_t i = m->table.free_head; i != NONE;
         i = m->table.slots[i].next) {
        CHECK(i < MODEL_SLOTS && !seen[i] && !m->records[i].live);
        seen[i] = true;
        walked++;
    }
    CHECK(walked == m->table.free_count);
}

static void test_model(uint64_t seed, bool near_wrap)
{
    static struct model m;

    memset(&m, 0, sizeof(m));
    m.random = seed;
    open_table(&m.table, MODEL_SLOTS);
    if (near_wrap) {
        /* Start every generation just below the 24-bit wrap. */
        for (uint32_t i = 0; i < MODEL_SLOTS; ++i) {
            m.table.slots[i].generation = 0xFFFFFFu - model_below(&m, 4);
        }
    }
    for (uint32_t step = 0; step < MODEL_STEPS; ++step) {
        uint32_t action = model_below(&m, 16);

        if (action < 4) {
            model_alloc(&m);
        } else if (action < 11) {
            model_complete(&m);
        } else if (action < 14) {
            model_stray(&m);
        } else {
            model_free(&m);
        }
        model_verify(&m);
    }
    /* Drain: every record's scripted completions free every slot. */
    while (m.live > 0) {
        model_complete(&m);
        model_verify(&m);
    }
    CHECK(m.table.free_count == MODEL_SLOTS);
}

int main(int argc, char **argv)
{
    uint64_t seed = 1;

    if (argc > 1) {
        seed = strtoull(argv[1], NULL, 0);
    }
    printf("slots seed %" PRIu64 "\n", seed);
    test_size();
    test_exhaustion();
    test_reuse_generation();
    test_foreign_and_forged();
    test_multishot();
    test_zero_copy();
    test_link_capacity();
    test_index_bound();
    test_zero_copy_refused();
    test_zero_copy_order();
    test_chain();
    test_freed_multishot();
    test_generation_period();
    test_model(seed, false);
    test_model(seed + 1, true);
    printf("slots ok\n");
    return 0;
}
