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
 * without a lookup; a RELEASE of a stale generation, or of a lease not
 * LEASED, is counted in releases_rejected and ignored (decision 131). Every
 * lease holds one decode region and either one slab
 * reference (MESSAGE, cold LOAD) or one ring pin (hot LOAD).
 */

#define VSR_IO_LEASE_REPLICA_SHIFT 40
#define VSR_IO_LEASE_REGION_SHIFT 16
#define VSR_IO_LEASE_GENERATION_MASK UINT64_C(0xFFFF)
/* Lease ids carry the region index in 24 bits. */
#define VSR_IO_LEASE_REGIONS_MAX (UINT32_C(1) << 24)

/* The slot table is engine-wide and sized before any replica attaches, so
 * it reserves this many record writes per replica; vsr_io_attach refuses a
 * store configured with more (decision 67). */
#define VSR_IO_ENGINE_INFLIGHT_WRITES_MAX 8u
/* The longest LINK chain the engine prepares (listener setup: SOCKET,
 * BIND, LISTEN, ACCEPT; a chain never spans batches) plus the PROVIDE
 * record of the pool's provision: vsr_io_prepare needs this capacity. */
#define VSR_IO_ENGINE_BATCH_MIN 5u
/* A store directory, NUL included, like every path the modules build. */
#define VSR_IO_ENGINE_PATH_BYTES 4096u

enum vsr_io_replica_state {
    VSR_IO_REPLICA_FREE,
    VSR_IO_REPLICA_OPENING, /* Attached; no STATUS emitted yet. */
    VSR_IO_REPLICA_RUNNING, /* A STATUS was emitted. */
    VSR_IO_REPLICA_STOPPED  /* STATUS STOPPED emitted; detach allowed. */
};

enum vsr_io_engine_state {
    VSR_IO_ENGINE_RUNNING,
    VSR_IO_ENGINE_CLOSING, /* vsr_io_close called; links and streams shut. */
    VSR_IO_ENGINE_CLOSED
};

enum vsr_io_lease_state {
    VSR_IO_LEASE_STATE_FREE,
    VSR_IO_LEASE_STATE_QUEUED, /* Its event is not accepted by the core yet. */
    VSR_IO_LEASE_STATE_LEASED
};

/* A decode region and its lease. */
struct vsr_io_lease {
    uint32_t state; /* enum vsr_io_lease_state */
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

/* A completion the routing decided at once (a SEND the link module
 * refused, a snapshot op's status, a malformed op), fed to the core once
 * `due` passed: the replica's retry_ns after the routing (decision 129). */
struct vsr_io_deferred {
    uint64_t due;
    uint64_t op;
    int32_t status;
    uint32_t reserved;
};

/* An in-flight incarnation admitted by vsr_io_submit lives in the store's
 * client table (inflight flag); the engine only routes. */

struct vsr_io_replica {
    struct vsr_io *io;
    uint32_t index;
    uint32_t state;             /* enum vsr_io_replica_state */
    struct vsr *core;           /* Kept after detach for vsr_deinit. */
    struct vsr_options options; /* Copy; seed points at seed_copy. */
    struct vsr_membership seed_copy;
    struct vsr_member *seed_members; /* [seed count] in the region. */
    char *path;                      /* The store directory, copied. */
    struct vsr_io_store store;
    struct vsr_io_snapshots snapshots;
    /* Event queues, in the order vsr_io_poll feeds them: internal
     * completions, then the caller's COMPLETE and STOP events, then
     * MESSAGEs, then the caller's other events, then TIME. */
    struct vsr_io_queued_event *completions; /* [core.limits.operations]
                                                ring of internal COMPLETEs
                                                (store, SEND, snapshot):
                                                one per outstanding op. */
    uint32_t completions_head;
    uint32_t completions_count;
    struct vsr_io_deferred *deferred; /* [operations] ring, due in order. */
    uint32_t deferred_head;
    uint32_t deferred_count;
    struct vsr_io_queued_event *priority; /* [operations + 1] caller
                                             COMPLETE and STOP ring. */
    uint32_t priority_head;
    uint32_t priority_count;
    uint32_t priority_capacity;
    uint32_t stopping; /* A STOP was submitted: other core events are
                          refused, MESSAGEs no longer delivered. */
    struct vsr_io_queued_event *events; /* [limits.events] ring of the
                                           caller's other events. */
    uint32_t events_head;
    uint32_t events_count;
    struct vsr_io_queued_event *messages; /* [regions] ring */
    uint32_t messages_head;
    uint32_t messages_count;
    struct vsr_event *step_events; /* [step_events_capacity] scratch array
                                      for vsr_step_many: every queue plus
                                      TIME. */
    uint32_t step_events_capacity;
    struct vsr_op *step_ops; /* [step_capacity] */
    uint32_t step_capacity;
    uint32_t regions_count;
    struct vsr_io_lease *leases; /* [regions_count] */
    uint32_t leases_free;
    uint32_t status_pending;  /* 1: STATUS op to emit on the next poll. */
    struct vsr_status status; /* Last status read for a STATUS op. */
    uint64_t core_deadline;   /* Last update.deadline_ns. */
    uint32_t deadline_core;   /* Deadline handles, bound at init. */
    uint32_t deadline_flush;
    uint32_t deadline_sync;
    uint32_t deadline_capture;
    uint64_t rejected;             /* Events the core refused (EINVAL) that the
                              engine dropped. */
    struct vsr_io_region metadata; /* The caller's regions, for detach. */
    struct vsr_io_region tail;
    uint32_t tail_region; /* Executor buffer region of the tail. */
    uint32_t reserved;
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
        struct vsr_status status; /* STATUS: the replica's, at emission. */
    } rail;
};

struct vsr_io {
    struct vsr_io_options options; /* Copy; listen points into listen_copy. */
    struct vsr_io_address *listen_copy;
    struct vsr_io_executor ex;
    uint32_t state; /* enum vsr_io_engine_state */
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
    uint64_t now;                /* Last now_ns given to poll or prepare,
                                    never decreasing. */
    uint32_t wake_pending;       /* Set by vsr_io_wake; cleared by poll. */
    uint32_t reserved;
    struct vsr_io_stats stats;
    struct vsr_io_uring *uring; /* Non-NULL when the executor is ours. */
    uint64_t random_state[4];   /* xoshiro256**, seeded at init (134). */
    uint32_t *clears;           /* [limits.file_slots] engine file slots whose
                               FILES_UPDATE (-1) is still to be issued. */
    uint32_t clears_count;
    uint32_t reserved3;
    struct vsr_io_buffer *provide_buffers; /* [limits.slabs] the PROVIDE
                                              record's buffers, read when
                                              the batch is submitted. */
    uint64_t releases_rejected; /* RELEASE ops of a stale engine lease. */
    uint64_t events_rejected;   /* Queued events the core refused. */
    /* vsr_io_run's scratch, from the metadata region. */
    struct vsr_io_sqe *run_sqes;     /* [limits.batch] */
    struct vsr_io_cqe *run_cqes;     /* [limits.batch] */
    struct vsr_io_op *run_ops;       /* [limits.ops] */
    struct vsr_io_event *run_events; /* [run_events_capacity] */
    uint32_t run_events_capacity;    /* limits.ops + limits.events */
    uint32_t reserved2;
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
/* A completion decided at once while an op is routed (a SEND the link
 * module refuses, or one it evicts from a full node queue for a newer
 * SEND, a snapshot op's status, a malformed op's FAILED), with no data and
 * no lease: queued in the replica's deferred ring and fed retry_ns later
 * (decisions 129 and 136), since the core answers a failed SEND by sending
 * again and a completion fed within the same poll loops. */
void vsr_io_engine_complete_later(struct vsr_io_replica *replica, uint64_t op,
                                  int32_t status);
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
/* Engine file slots (the executor range reserved for the engine). free
 * returns an index that holds no file (never installed, or emptied by a
 * CLOSE with FIXED_FILE or a FILES_UPDATE); clear returns one that may
 * still hold a file: the engine empties it with a FILES_UPDATE of -1 from
 * its next prepare and frees the index at that record's completion, so a
 * new file never lands in it before (decision 135). */
uint32_t vsr_io_engine_slot_alloc(struct vsr_io *io);
void vsr_io_engine_slot_free(struct vsr_io *io, uint32_t slot);
void vsr_io_engine_slot_clear(struct vsr_io *io, uint32_t slot);
/* True while slot's clear is queued or in flight. */
bool vsr_io_engine_slot_clearing(const struct vsr_io *io, uint32_t slot);
/* The engine's own records: the FILES_UPDATE of every queued clear
 * (continuing *count), and the completion of one (slot kind FILES).
 * vsr_io_prepare and vsr_io_complete call them; a test that plays the
 * engine over the modules calls them too. */
void vsr_io_engine_prepare_files(struct vsr_io *io, struct vsr_io_sqe *sqes,
                                 uint32_t capacity, uint32_t *count);
void vsr_io_engine_files_complete(struct vsr_io *io, uint32_t slot,
                                  const struct vsr_io_cqe *cqe);
/* user_data of the PROVIDE record (kind PROVIDE, no slot). */
uint64_t vsr_io_engine_provide_user_data(const struct vsr_io *io);
/* Bytes from the engine's generator, seeded from the executor's entropy
 * at vsr_io_init and never calling the executor after (decision 134):
 * unique, not secret. */
void vsr_io_engine_random(struct vsr_io *io, void *bytes, size_t size);

/* Brings the engine's clock to now_ns (never back) at a poll or prepare,
 * first moving the timers armed while completions were processed by the
 * time that passed since the last poll (decision 137). */
void vsr_io_engine_advance(struct vsr_io *io, uint64_t now_ns);

/* The pool's reserve for the limits (decision 127): one send slab per
 * link, the chunk reads of every stream window, per replica a cold-load
 * or recovery read slab and the snapshot module's writer, writer-cold and
 * reader slabs, and one reassembly slab. */
uint32_t vsr_io_engine_reserve(const struct vsr_io_limits *limits);
/* Closing completes once every slot is free, every link FREE and no
 * stream active (decisions 49, 96, 101); run after every completion batch,
 * poll and prepare. */
void vsr_io_engine_check_closed(struct vsr_io *io);

/* The routing of one core update (docs/io-implementation.md 7.3), exposed
 * for the unit tests: every LOAD, then every STORE, then SYNC and RECLAIM,
 * then the other ops in emission order (decision 51). The forwarded ring
 * must have room for every forwarded op (vsr_io_poll bounds the update's
 * capacity by it). */
void vsr_io_engine_route(struct vsr_io_replica *replica,
                         const struct vsr_op *ops, uint32_t count);

#endif /* VSR_IO_ENGINE_H */
