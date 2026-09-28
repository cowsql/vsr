#include "config.h"

#include "io/deadline.h"

#include "checked.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define DEADLINE_NONE UINT32_MAX /* VSR_IO_INDEX_NONE */

/*
 * A binary min-heap of handles keyed by (when, handle): ties break by
 * handle, so the pop order of equal deadlines does not depend on the order
 * in which they were armed. entries[h].position mirrors heap[position] == h.
 */

int vsr_io_deadlines_size(uint32_t capacity, size_t *bytes)
{
    size_t entries;
    size_t heap;

    if (capacity == DEADLINE_NONE ||
        !vsr_size_mul(capacity, sizeof(struct vsr_io_deadline_entry),
                      &entries) ||
        !vsr_size_mul(capacity, sizeof(uint32_t), &heap) ||
        !vsr_size_add(entries, heap, bytes)) {
        return VSR_ELIMIT;
    }
    return VSR_OK;
}

void vsr_io_deadlines_init(struct vsr_io_deadlines *set, void *memory,
                           uint32_t capacity)
{
    set->capacity = capacity;
    set->count = 0;
    if (capacity == 0) {
        set->entries = NULL;
        set->heap = NULL;
        return;
    }
    set->entries = memory;
    set->heap = (uint32_t *)(void *)(set->entries + capacity);
    for (uint32_t i = 0; i < capacity; ++i) {
        struct vsr_io_deadline_entry *entry = &set->entries[i];

        entry->when = VSR_NO_DEADLINE;
        entry->position = DEADLINE_NONE;
        entry->kind = VSR_IO_DEADLINE_KINDS;
        entry->reserved = 0;
        entry->index = DEADLINE_NONE;
        entry->reserved2 = 0;
        set->heap[i] = DEADLINE_NONE;
    }
}

void vsr_io_deadlines_bind(struct vsr_io_deadlines *set, uint32_t handle,
                           uint16_t kind, uint32_t index)
{
    set->entries[handle].kind = kind;
    set->entries[handle].index = index;
}

static bool deadline_before(const struct vsr_io_deadlines *set, uint32_t a,
                            uint32_t b)
{
    uint64_t left = set->entries[a].when;
    uint64_t right = set->entries[b].when;

    return left < right || (left == right && a < b);
}

static void deadline_place(struct vsr_io_deadlines *set, uint32_t position,
                           uint32_t handle)
{
    set->heap[position] = handle;
    set->entries[handle].position = position;
}

static void deadline_up(struct vsr_io_deadlines *set, uint32_t position)
{
    uint32_t handle = set->heap[position];

    while (position > 0) {
        uint32_t parent = (position - 1) / 2;

        if (!deadline_before(set, handle, set->heap[parent])) {
            break;
        }
        deadline_place(set, position, set->heap[parent]);
        position = parent;
    }
    deadline_place(set, position, handle);
}

static void deadline_down(struct vsr_io_deadlines *set, uint32_t position)
{
    uint32_t handle = set->heap[position];

    for (;;) {
        uint32_t child;

        /* The left child exists iff position < count / 2; computing
         * 2 * position + 1 first would wrap past 2^31 entries. */
        if (position >= set->count / 2) {
            break;
        }
        child = 2 * position + 1;
        if (child + 1 < set->count &&
            deadline_before(set, set->heap[child + 1], set->heap[child])) {
            child++;
        }
        if (!deadline_before(set, set->heap[child], handle)) {
            break;
        }
        deadline_place(set, position, set->heap[child]);
        position = child;
    }
    deadline_place(set, position, handle);
}

/* Removes an armed handle from the heap and disarms it. */
static void deadline_remove(struct vsr_io_deadlines *set, uint32_t handle)
{
    struct vsr_io_deadline_entry *entry = &set->entries[handle];
    uint32_t position = entry->position;
    uint32_t last = set->heap[set->count - 1];

    set->count--;
    set->heap[set->count] = DEADLINE_NONE;
    entry->when = VSR_NO_DEADLINE;
    entry->position = DEADLINE_NONE;
    if (last == handle) {
        return;
    }
    deadline_place(set, position, last);
    deadline_up(set, position);
    deadline_down(set, set->entries[last].position);
}

void vsr_io_deadlines_arm(struct vsr_io_deadlines *set, uint32_t handle,
                          uint64_t when)
{
    struct vsr_io_deadline_entry *entry = &set->entries[handle];

    if (when == VSR_NO_DEADLINE) {
        if (entry->position != DEADLINE_NONE) {
            deadline_remove(set, handle);
        }
        return;
    }
    if (entry->position == DEADLINE_NONE) {
        entry->when = when;
        deadline_place(set, set->count, handle);
        set->count++;
        deadline_up(set, entry->position);
        return;
    }
    entry->when = when;
    deadline_up(set, entry->position);
    deadline_down(set, entry->position);
}

uint64_t vsr_io_deadlines_earliest(const struct vsr_io_deadlines *set)
{
    if (set->count == 0) {
        return VSR_NO_DEADLINE;
    }
    return set->entries[set->heap[0]].when;
}

bool vsr_io_deadlines_pop(struct vsr_io_deadlines *set, uint64_t now,
                          uint16_t *kind, uint32_t *index)
{
    uint32_t handle;

    if (set->count == 0) {
        return false;
    }
    handle = set->heap[0];
    if (set->entries[handle].when > now) {
        return false;
    }
    *kind = set->entries[handle].kind;
    *index = set->entries[handle].index;
    deadline_remove(set, handle);
    return true;
}
