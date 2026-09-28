#ifndef VSR_IO_SLOTS_H
#define VSR_IO_SLOTS_H

#include "vsr-io.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/*
 * Slot table: the engine side of user_data (docs/io-implementation.md,
 * "Slots"). Every executor record the engine submits carries
 *
 *   user_data = owner (8) | kind (8) | index (24) | generation (24)
 *
 * where owner is vsr_io_options.owner, kind says which module handles the
 * completion, index names a slot of this table and generation rejects a
 * stale completion after the slot was recycled. A slot is allocated when a
 * record is prepared and freed when its last completion has been consumed
 * (a multishot or zero-copy record produces several). The table is a fixed
 * array with a free list; dispatch is an array index, never a lookup.
 */

enum vsr_io_slot_kind {
    VSR_IO_SLOT_FREE = 0,
    VSR_IO_SLOT_LISTEN,   /* SOCKET/BIND/LISTEN chain and multishot ACCEPT */
    VSR_IO_SLOT_CONNECT,  /* SOCKET then CONNECT of a dialed link */
    VSR_IO_SLOT_RECV,     /* Multishot receive of a link */
    VSR_IO_SLOT_PEEK,     /* Adopt-time peek is the caller's; unused */
    VSR_IO_SLOT_SEND,     /* One coalesced send; NOTIF shares the slot */
    VSR_IO_SLOT_SHUTDOWN, /* SHUTDOWN, CLOSE or CANCEL of a link */
    VSR_IO_SLOT_WRITE,    /* One store record write */
    VSR_IO_SLOT_FLUSH,    /* Store fdatasync */
    VSR_IO_SLOT_SUPER,    /* Superblock write */
    VSR_IO_SLOT_LOAD,     /* Cold LOAD read */
    VSR_IO_SLOT_FILE,     /* OPENAT/CLOSE/FALLOCATE/STATX/UNLINK/RENAME */
    VSR_IO_SLOT_CLIENTS,  /* Clients file read or write chunk */
    VSR_IO_SLOT_STREAM,   /* Stream file read chunk */
    VSR_IO_SLOT_KINDS
};

struct vsr_io_slot {
    uint32_t generation; /* 24 significant bits; incremented on free. */
    uint32_t next;       /* Free-list link or INDEX_NONE. */
    uint8_t kind;        /* enum vsr_io_slot_kind */
    uint8_t expected;    /* Completions still expected before free. */
    uint16_t reserved;
    uint32_t owner;  /* Link, replica or stream index. */
    uint32_t sub;    /* Per kind: write index, chunk index, ... */
    uint64_t cookie; /* Per kind: op id, sequence, byte offset. */
};

struct vsr_io_slots {
    struct vsr_io_slot *slots;
    uint32_t count;
    uint32_t free_head;
    uint32_t free_count;
    uint8_t owner; /* Owner tag of every user_data. */
};

/* Slots the engine needs for the limits: listeners, per link recv + send
 * + shutdown + connect, per stream its window plus two, per replica
 * inflight_writes + flush + superblock + load + four file operations, plus
 * a small fixed spare. Checked arithmetic; ELIMIT on overflow. */
int vsr_io_slots_size(const struct vsr_io_limits *limits, uint32_t listeners,
                      uint32_t inflight_writes, size_t *bytes, uint32_t *count);
void vsr_io_slots_init(struct vsr_io_slots *table, void *memory, uint32_t count,
                       uint8_t owner);

/* Allocates a slot expecting `completions` completions; INDEX_NONE when
 * the table is full, which prepare treats as "stop preparing". */
uint32_t vsr_io_slots_alloc(struct vsr_io_slots *table, uint8_t kind,
                            uint8_t completions, uint32_t owner, uint32_t sub,
                            uint64_t cookie);
uint64_t vsr_io_slots_user_data(const struct vsr_io_slots *table,
                                uint32_t index);
/* Resolves a completion: NULL for a foreign owner tag or a stale
 * generation (the completion is dropped). */
struct vsr_io_slot *vsr_io_slots_resolve(struct vsr_io_slots *table,
                                         uint64_t user_data, uint32_t *index);
/* One completion consumed; frees the slot when none is expected. Multishot
 * records call `more` to keep the slot alive while MORE is set. */
void vsr_io_slots_consumed(struct vsr_io_slots *table, uint32_t index,
                           bool more);
void vsr_io_slots_free(struct vsr_io_slots *table, uint32_t index);

#endif /* VSR_IO_SLOTS_H */
