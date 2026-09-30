#ifndef VSR_IO_ENGINE_H
#define VSR_IO_ENGINE_H

#include "io/codec.h"
#include "io/deadline.h"
#include "io/link.h"
#include "io/pool.h"
#include "io/slots.h"
#include "io/snapshot.h"
#include "io/store.h"
#include "io/stream.h"
#include "vsr-io.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/*
 * Engine: struct vsr_io and struct vsr_io_replica, the loop entry points
 * and the op routing (docs/io-implementation.md, "Engine"). The engine
 * owns the executor calls; every other module under src/io is a planner
 * that the engine drives through poll, prepare and complete.
 *
 * Memory: the engine metadata region holds struct vsr_io and the tables of
 * every engine-level module; the payload region is the pool. A replica's
 * metadata region holds struct vsr_io_replica, the core arena, the event
 * and message queues, the decode regions and lease table, the store
 * bookkeeping and the snapshot module; its tail region is the store's
 * ring and superblocks. Both are laid out by vsr_io_layout and
 * vsr_io_replica_layout with checked arithmetic and never grown.
 *
 * Leases: an engine lease id is VSR_IO_LEASE_ENGINE | replica << 40 |
 * region << 16 | generation, so RELEASE routes by replica and region
 * without a lookup; a stale generation is an API error the engine reports
 * as EINVAL. Every lease holds one decode region and either one slab
 * reference (MESSAGE, cold LOAD) or one ring pin (hot LOAD).
 */

#define VSR_IO_LEASE_REPLICA_SHIFT 40
#define VSR_IO_LEASE_REGION_SHIFT 16
#define VSR_IO_LEASE_GENERATION_MASK UINT64_C(0xFFFF)

enum vsr_io_replica_state {
    VSR_IO_REPLICA_FREE,
    VSR_IO_REPLICA_OPENING, /* Store open/recovery in progress. */
    VSR_IO_REPLICA_RUNNING,
    VSR_IO_REPLICA_STOPPED /* STATUS STOPPED emitted; detach allowed. */
};

/* A decode region and its lease. */
struct vsr_io_lease {
    uint32_t state; /* 0 free, 1 queued (event not yet accepted), 2 leased */
    uint32_t slab;  /* Pool slab referenced, or NONE. */
    uint32_t pin;   /* Ring pin, or NONE. */
    uint16_t generation;
    uint16_t reserved;
    struct vsr_io_bump region;
};

/* A queued input event for the core: MESSAGE from a link, or a caller
 * event held under the caller's lease. */
struct vsr_io_queued_event {
    struct vsr_event event;
    uint32_t lease; /* Engine lease index for MESSAGE, else NONE. */
    uint32_t kind;  /* enum vsr_io_event_kind of the wrapper. */
};

/* An in-flight incarnation admitted by vsr_io_submit lives in the store's
 * client table (inflight flag); the engine only routes. */

struct vsr_io_replica {
    struct vsr_io *io;
    uint32_t index;
    uint32_t state; /* enum vsr_io_replica_state */
    struct vsr *core;
    struct vsr_options options; /* Copy; seed points into `seed_region`. */
    unsigned char *seed_region;
    struct vsr_io_store store;
    struct vsr_io_snapshots snapshots;
    /* Event queues, in the order vsr_io_poll feeds them: internal
     * completions, then caller events, then MESSAGEs, then TIME. */
    struct vsr_io_queued_event *completions; /* [core.limits.operations]
                                                ring of internal COMPLETEs
                                                (SEND, snapshot); the store
                                                drains its own queue. */
    uint32_t completions_head;
    uint32_t completions_count;
    struct vsr_io_queued_event *events; /* [limits.events] caller ring */
    uint32_t events_head;
    uint32_t events_count;
    struct vsr_io_queued_event *messages; /* [regions] ring */
    uint32_t messages_head;
    uint32_t messages_count;
    struct vsr_event *step_events; /* [regions + events + operations + 1]
                                      scratch array for vsr_step_many */
    struct vsr_op *step_ops;       /* [step_capacity] */
    uint32_t step_capacity;
    uint32_t regions_count;
    struct vsr_io_lease *leases; /* [regions_count] */
    uint32_t leases_free;
    uint32_t status_pending;  /* 1: STATUS op to emit on the next poll. */
    struct vsr_status status; /* Borrowed by the STATUS op. */
    uint64_t core_deadline;
    uint32_t deadline_core; /* Deadline handles. */
    uint32_t deadline_flush;
    uint32_t deadline_sync;
    uint32_t deadline_capture;
    struct vsr_io_region metadata; /* The caller's regions, for detach. */
    struct vsr_io_region tail;
};

/* Forwarded op queue entry: a CORE op verbatim or an engine rail with its
 * descriptor stored inline. */
struct vsr_io_forwarded {
    struct vsr_io_op op;
    union {
        struct vsr_io_handshake handshake;
        struct vsr_io_stream_serve serve;
        struct vsr_io_stream_data data;
        struct vsr_io_stream_end end;
        struct vsr_io_stream_written written;
        struct vsr_io_link_wanted wanted;
    } rail;
};

struct vsr_io {
    struct vsr_io_options options; /* Copy; listen points into listen_copy. */
    struct vsr_io_address *listen_copy;
    struct vsr_io_executor ex;
    uint32_t state; /* 0 running, 1 closing, 2 closed */
    uint32_t page_bytes;
    struct vsr_io_pool pool;
    struct vsr_io_slots slots;
    struct vsr_io_deadlines deadlines;
    struct vsr_io_links links;
    struct vsr_io_streams streams;
    struct vsr_io_replica *replicas; /* [limits.replicas] */
    uint32_t replicas_count;
    uint32_t file_slot_next;   /* Next unallocated engine file slot. */
    uint32_t *file_slots_free; /* Free list of engine file slots. */
    uint32_t file_slots_free_count;
    /* Forwarded ops queued for vsr_io_poll. */
    struct vsr_io_forwarded *forwarded; /* [limits.ops] ring */
    uint32_t forwarded_head;
    uint32_t forwarded_count;
    uint32_t forwarded_overflow; /* 1 when a producer found it full; poll
                                    reports MORE. */
    uint64_t now;                /* Last now_ns given to poll. */
    uint32_t wake_pending;       /* Set by vsr_io_wake; cleared by poll. */
    uint32_t reserved;
    struct vsr_io_stats stats;
    struct vsr_io_uring *uring; /* Non-NULL when the executor is ours. */
};

/* Layout of the engine metadata region: bytes and alignment (the page,
 * because of the provided-ring memory) for the options. */
int vsr_io_engine_size(const struct vsr_io_options *options, size_t *bytes,
                       size_t *alignment);
/* Layout of a replica's regions: core arena (vsr_layout), queues, regions
 * (vsr_io_codec_message_region / load_region, times input_leases + events),
 * store and snapshot bookkeeping; tail from vsr_io_store_size. */
int vsr_io_replica_size(const struct vsr_io *io,
                        const struct vsr_io_replica_options *options,
                        size_t *metadata_bytes, size_t *metadata_alignment,
                        size_t *tail_bytes, size_t *tail_alignment);

/* Op routing helpers used by the modules. */
struct vsr_io_forwarded *vsr_io_forward(struct vsr_io *io,
                                        struct vsr_io_replica *replica,
                                        uint32_t kind);
/* Queues a COMPLETE event for the core of `replica` from an internal
 * completion; the event is fed on the next poll. */
void vsr_io_engine_complete_core(struct vsr_io_replica *replica,
                                 const struct vsr_io_completion *completion);
/* Engine leases. */
uint32_t vsr_io_lease_alloc(struct vsr_io_replica *replica, uint32_t slab,
                            uint32_t pin);
uint64_t vsr_io_lease_id(const struct vsr_io_replica *replica, uint32_t lease);
/* Resolves an engine lease id to its replica and index; EINVAL when
 * stale. */
int vsr_io_lease_resolve(struct vsr_io *io, uint64_t lease,
                         struct vsr_io_replica **replica, uint32_t *index);
void vsr_io_lease_release(struct vsr_io_replica *replica, uint32_t lease);
/* MESSAGE delivery from the link module: decodes the body into a fresh
 * region and queues the event for the replica of message.cluster; false
 * when no region is free (the link keeps the bytes and retries). */
bool vsr_io_engine_deliver(struct vsr_io *io, struct vsr_io_replica *replica,
                           const struct vsr_io_cursor *body, uint32_t slab);
struct vsr_io_replica *vsr_io_engine_replica(struct vsr_io *io,
                                             struct vsr_id cluster);
/* Engine file slots (the executor range reserved for the engine). */
uint32_t vsr_io_engine_slot_alloc(struct vsr_io *io);
void vsr_io_engine_slot_free(struct vsr_io *io, uint32_t slot);
/* Random bytes from the executor. */
void vsr_io_engine_random(struct vsr_io *io, void *bytes, size_t size);
/* Installs a raw descriptor into an engine file slot (the executor's
 * update_file, which takes the descriptor over); 0 or a negative errno. The
 * link module calls it when it takes a socket over (decision 72). */
int vsr_io_engine_install(struct vsr_io *io, uint32_t slot, int fd);

/* The pool's reserve for the limits (decision E1): one send slab per
 * link, the chunk reads of every stream window, per replica a cold-load
 * or recovery read slab and the snapshot module's writer, writer-cold and
 * reader slabs, and one reassembly slab. */
uint32_t vsr_io_engine_reserve(const struct vsr_io_limits *limits);

#endif /* VSR_IO_ENGINE_H */
