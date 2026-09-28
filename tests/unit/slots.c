#include "config.h"

#include "io/slots.h"
#include "lib/check.h"
#include "vsr-io.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
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

    /* listeners + 4 links + streams (window + 2) + replicas (inflight + 8)
     * + 8 = 1 + 8 + 5 + 2 * 12 + 8. */
    limits.links = 2;
    limits.streams = 1;
    limits.stream_window = 3;
    limits.replicas = 2;
    CHECK(vsr_io_slots_size(&limits, 1, 4, &bytes, &count) == VSR_OK);
    CHECK(count == 46 && bytes == 46 * sizeof(struct vsr_io_slot));

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

int main(void)
{
    test_size();
    test_exhaustion();
    test_reuse_generation();
    test_foreign_and_forged();
    test_multishot();
    test_zero_copy();
    printf("slots ok\n");
    return 0;
}
