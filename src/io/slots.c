#include "config.h"

#include "io/slots.h"

#include "checked.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* user_data below the owner tag: kind (8) | index (24) | generation (24). */
#define SLOT_NONE UINT32_MAX /* VSR_IO_INDEX_NONE */
#define SLOT_KIND_SHIFT 48
#define SLOT_INDEX_SHIFT 24
#define SLOT_FIELD_MASK UINT32_C(0xFFFFFF)

/* Invariant checks in debug builds; a violation traps (see pool.c). */
#ifdef NDEBUG
#define SLOTS_ASSERT(condition) ((void)sizeof(condition))
#else
#define SLOTS_ASSERT(condition) ((condition) ? (void)0 : __builtin_trap())
#endif

/* Fixed per-record counts of docs/io-implementation.md, "Slots". A link
 * holds a receive, a shutdown, a connect and up to VSR_IO_LINK_SENDS (4,
 * src/io/link.h, a level above) sends, each until its NOTIF. */
#define SLOTS_LINK_SENDS 4u
#define SLOTS_PER_LINK (3u + SLOTS_LINK_SENDS)
#define SLOTS_STREAM_EXTRA 2u
#define SLOTS_REPLICA_EXTRA 8u
#define SLOTS_SPARE 8u

int vsr_io_slots_size(const struct vsr_io_limits *limits, uint32_t listeners,
                      uint32_t inflight_writes, size_t *bytes, uint32_t *count)
{
    size_t total = SLOTS_SPARE;
    size_t term;

    if (!vsr_size_add(total, listeners, &total) ||
        !vsr_size_mul(limits->links, SLOTS_PER_LINK, &term) ||
        !vsr_size_add(total, term, &total) ||
        !vsr_size_add(limits->stream_window, SLOTS_STREAM_EXTRA, &term) ||
        !vsr_size_mul(limits->streams, term, &term) ||
        !vsr_size_add(total, term, &total) ||
        !vsr_size_add(inflight_writes, SLOTS_REPLICA_EXTRA, &term) ||
        !vsr_size_mul(limits->replicas, term, &term) ||
        !vsr_size_add(total, term, &total) || total > VSR_IO_SLOTS_MAX ||
        !vsr_size_mul(total, sizeof(struct vsr_io_slot), bytes)) {
        return VSR_ELIMIT;
    }
    *count = (uint32_t)total;
    return VSR_OK;
}

void vsr_io_slots_init(struct vsr_io_slots *table, void *memory, uint32_t count,
                       uint8_t owner)
{
    /* A larger table would alias index bits in user_data. */
    SLOTS_ASSERT(count <= VSR_IO_SLOTS_MAX);
    table->slots = memory;
    table->count = count;
    table->free_head = count > 0 ? 0 : SLOT_NONE;
    table->free_count = count;
    table->owner = owner;
    table->rejected = 0;
    for (uint32_t i = 0; i < count; ++i) {
        struct vsr_io_slot *slot = &table->slots[i];

        slot->generation = 0;
        slot->next = i + 1 < count ? i + 1 : SLOT_NONE;
        slot->kind = VSR_IO_SLOT_FREE;
        slot->expected = 0;
        slot->reserved = 0;
        slot->owner = 0;
        slot->sub = 0;
        slot->cookie = 0;
    }
}

uint32_t vsr_io_slots_alloc(struct vsr_io_slots *table, uint8_t kind,
                            uint8_t completions, uint32_t owner, uint32_t sub,
                            uint64_t cookie)
{
    uint32_t index = table->free_head;
    struct vsr_io_slot *slot;

    /* A FREE kind would drop the slot from the free list for good. */
    SLOTS_ASSERT(kind != VSR_IO_SLOT_FREE);
    if (index == SLOT_NONE) {
        return SLOT_NONE;
    }
    slot = &table->slots[index];
    table->free_head = slot->next;
    table->free_count--;
    slot->next = SLOT_NONE;
    slot->kind = kind;
    slot->expected = completions;
    slot->owner = owner;
    slot->sub = sub;
    slot->cookie = cookie;
    return index;
}

uint64_t vsr_io_slots_user_data(const struct vsr_io_slots *table,
                                uint32_t index)
{
    const struct vsr_io_slot *slot = &table->slots[index];
    uint64_t value = (uint64_t)slot->kind << SLOT_KIND_SHIFT |
                     (uint64_t)(index & SLOT_FIELD_MASK) << SLOT_INDEX_SHIFT |
                     (uint64_t)(slot->generation & SLOT_FIELD_MASK);

    return VSR_IO_USER_DATA(table->owner, value);
}

struct vsr_io_slot *vsr_io_slots_resolve(struct vsr_io_slots *table,
                                         uint64_t user_data, uint32_t *index)
{
    uint32_t kind = (uint32_t)(user_data >> SLOT_KIND_SHIFT) & 0xFFu;
    uint32_t position =
        (uint32_t)(user_data >> SLOT_INDEX_SHIFT) & SLOT_FIELD_MASK;
    uint32_t generation = (uint32_t)user_data & SLOT_FIELD_MASK;
    struct vsr_io_slot *slot;

    if (VSR_IO_OWNER(user_data) != table->owner || position >= table->count) {
        table->rejected++;
        return NULL;
    }
    slot = &table->slots[position];
    if (slot->kind == VSR_IO_SLOT_FREE || slot->kind != kind ||
        (slot->generation & SLOT_FIELD_MASK) != generation) {
        table->rejected++;
        return NULL;
    }
    *index = position;
    return slot;
}

void vsr_io_slots_consumed(struct vsr_io_slots *table, uint32_t index,
                           bool more)
{
    struct vsr_io_slot *slot = &table->slots[index];

    if (slot->kind == VSR_IO_SLOT_FREE) {
        return;
    }
    if (more) {
        if (slot->expected > 1) {
            slot->expected--;
        }
        return;
    }
    if (slot->expected > 0) {
        slot->expected--;
    }
    if (slot->expected == 0) {
        vsr_io_slots_free(table, index);
    }
}

void vsr_io_slots_free(struct vsr_io_slots *table, uint32_t index)
{
    struct vsr_io_slot *slot = &table->slots[index];

    if (slot->kind == VSR_IO_SLOT_FREE) {
        return;
    }
    slot->generation = (slot->generation + 1) & SLOT_FIELD_MASK;
    slot->kind = VSR_IO_SLOT_FREE;
    slot->expected = 0;
    slot->owner = 0;
    slot->sub = 0;
    slot->cookie = 0;
    slot->next = table->free_head;
    table->free_head = index;
    table->free_count++;
}
