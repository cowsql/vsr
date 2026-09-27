#ifndef VSR_INTERNAL_H
#define VSR_INTERNAL_H

#include "vsr.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define VSR_INDEX_NONE UINT32_MAX

/* Hot descriptors are separate from operation-owned cold metadata storage. */
enum vsr_slot_state {
    VSR_SLOT_FREE,
    VSR_SLOT_BUILDING,
    VSR_SLOT_READY,
    VSR_SLOT_ACTIVE
};

struct vsr_operation {
    struct vsr_op output;
    uint64_t generation;
    uint64_t reserved_bytes;
    uint64_t tag; /* Protocol-owned completion discriminator. */
    unsigned char *memory;
    size_t capacity;
    size_t used;
    uint32_t *leases;
    uint32_t lease_count;
    uint32_t lease_capacity;
    uint32_t index;
    uint32_t next;
    uint32_t state;
    bool reserves_lease;
};

struct vsr_lease {
    uint64_t id;
    uint64_t bytes;
    uint32_t references;
    uint32_t next;
    uint32_t hash_next;
    bool releasing;
};

struct vsr {
    struct vsr_options options; /* seed points into this arena. */
    struct vsr_status status; /* Protocol owns configuration/progress fields. */
    void *protocol;
    size_t arena_size;
    struct vsr_operation *operations;
    struct vsr_lease *leases;
    uint32_t *lease_buckets;
    uint64_t now;
    uint64_t payload_bytes;
    uint64_t reserved_bytes;
    uint64_t progress_bytes;
    uint32_t operation_free;
    uint32_t operation_free_count;
    uint32_t operation_ready_first;
    uint32_t operation_ready_last;
    uint32_t lease_free;
    uint32_t lease_free_count;
    uint32_t release_first;
    uint32_t release_last;
    uint32_t reserved_leases;
    bool time_set;
    bool started;
    bool initialized;
    bool stopping;
    bool protocol_quiesced;
    bool state_changed;
    /* A blocked input asked for a readable cache pin to be released; the
     * release waits until the protocol poll is idle. */
    bool relief_pending;
};

/* Protocol-private arena is laid out with checked arithmetic; no allocation. */
int vsr_protocol_size(const struct vsr_options *options, size_t *size,
                      size_t *alignment);
void vsr_protocol_init(struct vsr *v, void *memory, size_t size);
/* start/event return AGAIN only before retaining/mutating the submitted input. */
int vsr_protocol_start(struct vsr *v);
int vsr_protocol_event(struct vsr *v, const struct vsr_event *event,
                       uint32_t lease);
/* The completed operation and completion lease remain alive during this call.
 * Completion cannot refuse admission: record intent, then issue work in poll. */
void vsr_protocol_complete(struct vsr *v, struct vsr_operation *operation,
                           const struct vsr_event *event, uint32_t lease);
/* Perform at most one bounded transition; true means a transition was made. */
bool vsr_protocol_poll(struct vsr *v);
/* Release one reconstructible cache pin after an input's resource admission
 * failed. The unconsumed input remains entirely caller-owned. */
bool vsr_protocol_relieve_pressure(struct vsr *v);
uint64_t vsr_protocol_deadline(const struct vsr *v);
/* Release every protocol-owned lease/reference. Already issued effects drain. */
void vsr_protocol_stop(struct vsr *v);

/* Operation construction is private until publish. Each successful acquire must
 * be followed by publish or abort. completion_bytes reserves the maximum total
 * blob bytes a successful completion can return, not bytes in the output graph.
 * LOAD/APPLY/CAPTURE/FETCH also reserve one input lease, even with zero bytes. */
struct vsr_operation *vsr_operation_acquire(struct vsr *v, uint32_t type,
                                            uint64_t arg, uint64_t tag,
                                            uint64_t completion_bytes);
void *vsr_operation_alloc(struct vsr_operation *operation, size_t size,
                          size_t alignment);
/* Copy descriptor graphs/spans only; all payload bytes remain referenced.
 * The caller holds every input lease backing those bytes using hold below.
 * Internal STORE builders use each change type at most once and at most one
 * PUBLISH/RESTORE change combined; the arena planner relies on this bound. */
int vsr_operation_copy(struct vsr_operation *operation, const void *data);
bool vsr_operation_hold(struct vsr *v, struct vsr_operation *operation,
                        uint32_t lease);
void vsr_operation_publish(struct vsr *v, struct vsr_operation *operation);
void vsr_operation_abort(struct vsr *v, struct vsr_operation *operation);
struct vsr_operation *vsr_operation_find(struct vsr *v, uint64_t id);

/* Event callbacks borrow one reference. Retain anything needed after return.
 * Index NONE represents an event with no data and retain/release are no-ops. */
bool vsr_lease_retain(struct vsr *v, uint32_t lease);
void vsr_lease_release(struct vsr *v, uint32_t lease);
uint64_t vsr_lease_id(const struct vsr *v, uint32_t lease);

/* First fatal failure wins. Polling/protocol admission stops; effects and leases
 * can still be drained. stop releases protocol-held references exactly once. */
void vsr_fail(struct vsr *v, uint32_t code,
              const struct vsr_operation *operation, int32_t status);
void vsr_changed(struct vsr *v);
/* Checked absolute deadline; overflow fences with EXHAUSTED. */
uint64_t vsr_after(struct vsr *v, uint64_t delay);

#endif
