#ifndef VSR_IO_DEADLINE_H
#define VSR_IO_DEADLINE_H

#include "vsr.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/*
 * Deadline set (docs/io-implementation.md, "Deadlines"). The engine arms no
 * TIMEOUT record for itself (docs/io-design.md decision 40): every engine
 * deadline is an entry here, the earliest is what vsr_io_prepare reports in
 * *deadline_ns, and the loop's submit_and_wait returns by then. On the next
 * poll the engine asks which entries are due and dispatches them by kind.
 *
 * Entries have fixed handles, one per possible timer (a link's handshake or
 * idle timer, a node's dial backoff, a replica's core deadline, flush
 * interval and sync delay, a stream's inactivity), so arming and cancelling
 * never allocate. A binary heap keyed by deadline with a position index per
 * handle gives O(log n) arm/cancel and O(1) earliest.
 */

enum vsr_io_deadline_kind {
    VSR_IO_DEADLINE_LINK,    /* Handshake, then idle timeout; index = link. */
    VSR_IO_DEADLINE_DIAL,    /* Redial backoff; index = node. */
    VSR_IO_DEADLINE_CORE,    /* vsr_update.deadline_ns; index = replica. */
    VSR_IO_DEADLINE_FLUSH,   /* Write-behind flush; index = replica. */
    VSR_IO_DEADLINE_SYNC,    /* sync_delay_ns hold; index = replica. */
    VSR_IO_DEADLINE_STREAM,  /* Stream inactivity; index = stream. */
    VSR_IO_DEADLINE_CAPTURE, /* Clients-file step retry; index = replica. */
    VSR_IO_DEADLINE_KINDS
};

struct vsr_io_deadline_entry {
    uint64_t when;     /* VSR_NO_DEADLINE when disarmed. */
    uint32_t position; /* Heap index or INDEX_NONE. */
    uint16_t kind;     /* enum vsr_io_deadline_kind */
    uint16_t rebase;   /* Armed while `rebasing`: moved by the next rebase. */
    uint32_t index;    /* Owner index within its kind. */
    uint32_t reserved2;
};

struct vsr_io_deadlines {
    struct vsr_io_deadline_entry *entries; /* [capacity], by handle. */
    uint32_t *heap;                        /* [capacity] handles. */
    uint32_t capacity;
    uint32_t count;
    uint32_t rebasing; /* 1 while the engine processes completions: arms
                          are marked for the next rebase (decision G2). */
    uint32_t marked;   /* Entries marked. */
};

/* Bytes for capacity handles (entries, then heap), in memory aligned for
 * uint64_t; checked arithmetic, ELIMIT on overflow or capacity UINT32_MAX
 * (the NONE position). */
int vsr_io_deadlines_size(uint32_t capacity, size_t *bytes);
/* Every handle starts disarmed and unbound. */
void vsr_io_deadlines_init(struct vsr_io_deadlines *set, void *memory,
                           uint32_t capacity);
/* Binds a handle to its owner once; handles are dense per kind and the
 * engine computes them as base[kind] + index. */
void vsr_io_deadlines_bind(struct vsr_io_deadlines *set, uint32_t handle,
                           uint16_t kind, uint32_t index);
/* Arms or moves a handle; VSR_NO_DEADLINE disarms it. */
void vsr_io_deadlines_arm(struct vsr_io_deadlines *set, uint32_t handle,
                          uint64_t when);
uint64_t vsr_io_deadlines_earliest(const struct vsr_io_deadlines *set);
/* Moves every entry armed while `rebasing` was set (and not re-armed or
 * disarmed since) `delta` later, and clears the marks. Completions carry
 * no clock: a timer armed while one is processed counts from the engine's
 * time of the previous poll, which an idle engine left behind by as long
 * as it slept, so the next poll (the first to bring the clock) moves such
 * timers by the time that passed (decision G2). */
void vsr_io_deadlines_rebase(struct vsr_io_deadlines *set, uint64_t delta);
/* Pops the earliest entry due at or before now; false when none. Equal
 * deadlines pop in handle order. The entry is disarmed; a periodic owner
 * re-arms it after now, since an entry armed at or before now is due at
 * once and pops again in the same drain. */
bool vsr_io_deadlines_pop(struct vsr_io_deadlines *set, uint64_t now,
                          uint16_t *kind, uint32_t *index);

#endif /* VSR_IO_DEADLINE_H */
