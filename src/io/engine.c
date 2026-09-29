#include "config.h"

#include "io/engine.h"

#include "checked.h"

#include <stdalign.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

/*
 * Engine kernel: the metadata layout, initialization and executor
 * registration of struct vsr_io, and the helpers every planner module
 * calls (docs/io-implementation.md, section 7). The loop entry points, the
 * replica life cycle and the op routing follow in this file once the
 * store, link, stream and snapshot modules exist (see the end of the
 * file).
 *
 * Metadata region, in offset order (docs/io-implementation.md, section 2):
 *   1. pool bookkeeping: the provided-ring memory first, page aligned at
 *      the region base (which is why the region's alignment is the page),
 *      then the slab entries (vsr_io_pool_size)
 *   2. struct vsr_io
 *   3. listen address copies [listen_count]
 *   4. slot table (vsr_io_slots_size)
 *   5. deadline set (vsr_io_deadlines_size): links + nodes + 4 * replicas
 *      + streams handles, dense per kind in enum vsr_io_deadline_kind
 *      order (decision 60)
 *   6. link tables (vsr_io_links_size)
 *   7. stream tables (vsr_io_streams_size)
 *   8. forwarded-op ring [limits.ops]
 *   9. replica table [limits.replicas]
 *  10. engine file-slot free list [limits.file_slots]
 * Every offset is computed with checked arithmetic; the same plan carves
 * the region at init.
 */

#define NONE VSR_IO_INDEX_NONE

/* The page the provided-ring memory and the pool are aligned to. The engine
 * is a planner and reads no system value; 4 KiB pages are the baseline
 * (io_uring rejects a ring whose memory is not page aligned, so a larger
 * page makes vsr_io_init fail with the executor's -EINVAL). */
#define ENGINE_PAGE_BYTES 4096u
/* Provided-ring entry bytes, per the executor contract (buffer_ring). */
#define ENGINE_RING_ENTRY_BYTES 16u
/* The slot table is engine-wide and sized before any replica attaches, so
 * it reserves this many record writes per replica; vsr_io_attach refuses
 * a store configured with more (vsr_io_store_options.inflight_writes). */
#define ENGINE_INFLIGHT_WRITES_MAX 8u
/* Longest LINK chain the engine prepares (listener setup: SOCKET, BIND,
 * LISTEN, ACCEPT); a chain never spans batches. */
#define ENGINE_BATCH_MIN 4u
/* Frame header plus stream chunk header around one chunk payload. */
#define ENGINE_STREAM_FRAMING                                                  \
    (VSR_IO_FRAME_HEADER_BYTES + sizeof(struct vsr_io_wire_stream_chunk))
/* Lease ids carry the replica index in 23 bits above the region. */
#define ENGINE_REPLICAS_MAX (UINT32_C(1) << 23)
#define ENGINE_LEASE_REPLICA_MASK (ENGINE_REPLICAS_MAX - 1)
#define ENGINE_LEASE_REGION_MASK ((UINT64_C(1) << 24) - 1)
/* Registered file slots are int32_t descriptors; regions are uint16_t. */
#define ENGINE_FILE_SLOTS_END ((uint64_t)INT32_MAX + 1)
#define ENGINE_BUFFER_REGIONS_END (UINT64_C(1) << 16)

enum engine_state { ENGINE_RUNNING, ENGINE_CLOSING, ENGINE_CLOSED };

enum lease_state { LEASE_FREE, LEASE_QUEUED, LEASE_LEASED };

/* Invariant checks in debug builds; a violation traps (see pool.c). */
#ifdef NDEBUG
#define ENGINE_ASSERT(condition) ((void)sizeof(condition))
#else
#define ENGINE_ASSERT(condition) ((condition) ? (void)0 : __builtin_trap())
#endif

/* -------------------------------------------------------------------------
 * Layout
 * ---------------------------------------------------------------------- */

struct engine_plan {
    size_t pool;
    size_t pool_bytes;
    size_t io;
    size_t listen;
    size_t slots;
    uint32_t slots_count;
    uint32_t deadlines_count;
    size_t deadlines;
    size_t links;
    size_t links_bytes;
    size_t streams;
    size_t streams_bytes;
    size_t forwarded;
    size_t replicas;
    size_t file_slots;
    size_t total;
    size_t alignment;
    size_t payload;
};

static bool place(size_t *offset, size_t bytes, size_t alignment, size_t *out)
{
    size_t aligned;

    if (!vsr_size_add(*offset, alignment - 1, &aligned)) {
        return false;
    }
    aligned &= ~(alignment - 1);
    *out = aligned;
    return vsr_size_add(aligned, bytes, offset);
}

static bool power_of_two(uint64_t value)
{
    return value != 0 && (value & (value - 1)) == 0;
}

static bool duration_valid(uint64_t ns)
{
    return ns < VSR_NO_DEADLINE;
}

/* Deadline handles per kind, in enum order (decision 60). */
static uint32_t deadline_count(const struct vsr_io_limits *limits,
                               uint16_t kind)
{
    switch ((enum vsr_io_deadline_kind)kind) {
    case VSR_IO_DEADLINE_LINK:
        return limits->links;
    case VSR_IO_DEADLINE_DIAL:
        return limits->nodes;
    case VSR_IO_DEADLINE_CORE:
    case VSR_IO_DEADLINE_FLUSH:
    case VSR_IO_DEADLINE_SYNC:
    case VSR_IO_DEADLINE_CAPTURE:
        return limits->replicas;
    case VSR_IO_DEADLINE_STREAM:
        return limits->streams;
    case VSR_IO_DEADLINE_KINDS:
    default:
        return 0;
    }
}

static uint32_t deadline_base(const struct vsr_io_limits *limits, uint16_t kind)
{
    uint32_t base = 0;

    for (uint16_t k = 0; k < kind; ++k) {
        base += deadline_count(limits, k);
    }
    return base;
}

/* The option checks of vsr-io.h (sizing rules, "positive except
 * caller_slabs") and of the capacity table: EINVAL for a malformed option,
 * ELIMIT for a capacity that can never be met. */
static int check_options(const struct vsr_io_options *options)
{
    const struct vsr_io_limits *limits = &options->limits;
    size_t minimum;
    size_t term;
    uint64_t end;

    if (options->node == VSR_IO_NO_NODE || options->reserved != 0 ||
        (options->listen_count > 0 && options->listen == NULL) ||
        (options->handshake != VSR_IO_HANDSHAKE_TRUSTED &&
         options->handshake != VSR_IO_HANDSHAKE_EXTERNAL) ||
        !duration_valid(options->connect_backoff_ns) ||
        !duration_valid(options->handshake_timeout_ns) ||
        options->handshake_timeout_ns == 0 ||
        !duration_valid(options->idle_timeout_ns) ||
        !duration_valid(options->wait_min_ns) ||
        options->send_coalesce_bytes == 0 || options->stream_chunk_bytes == 0 ||
        (options->cache_line_bytes != 0 &&
         !power_of_two(options->cache_line_bytes))) {
        return VSR_EINVAL;
    }
    for (uint32_t i = 0; i < options->listen_count; ++i) {
        const struct vsr_io_address *address = &options->listen[i];

        if (address->length == 0 ||
            address->length > sizeof(address->sockaddr) ||
            address->reserved != 0) {
            return VSR_EINVAL;
        }
    }
    if (limits->replicas == 0 || limits->nodes == 0 ||
        limits->authorizations == 0 || limits->links == 0 ||
        limits->link_queue == 0 || limits->streams == 0 ||
        limits->stream_window == 0 || limits->events == 0 || limits->ops == 0 ||
        limits->batch == 0 || limits->slabs == 0 || limits->slab_bytes == 0 ||
        limits->file_slots == 0 || limits->buffer_regions == 0 ||
        limits->slab_bytes % ENGINE_PAGE_BYTES != 0) {
        return VSR_EINVAL;
    }
    /* The link module keeps listener state in a fixed table (decision L1). */
    if (options->listen_count > VSR_IO_LISTENERS_MAX) {
        return VSR_ELIMIT;
    }
    /* Minimum slabs: links + streams * (stream_window + 1) + 2 * replicas
     * + 4 + caller_slabs (decisions 42 and 54). */
    if (!vsr_size_add(limits->stream_window, 1, &term) ||
        !vsr_size_mul(limits->streams, term, &term) ||
        !vsr_size_add(limits->links, term, &minimum) ||
        !vsr_size_mul(limits->replicas, 2, &term) ||
        !vsr_size_add(minimum, term, &minimum) ||
        !vsr_size_add(minimum, 4, &minimum) ||
        !vsr_size_add(minimum, limits->caller_slabs, &minimum) ||
        minimum > limits->slabs || limits->replicas > ENGINE_REPLICAS_MAX ||
        limits->batch < ENGINE_BATCH_MIN) {
        return VSR_ELIMIT;
    }
    /* A stream chunk and its framing fit one slab. */
    if ((uint64_t)options->stream_chunk_bytes + ENGINE_STREAM_FRAMING >
        limits->slab_bytes) {
        return VSR_ELIMIT;
    }
    /* File slots: listeners, links, one per stream, per replica the log
     * and one transient clients file; the range must stay a descriptor. */
    if (!vsr_size_mul(limits->replicas, 2, &term) ||
        !vsr_size_add(term, options->listen_count, &minimum) ||
        !vsr_size_add(minimum, limits->links, &minimum) ||
        !vsr_size_add(minimum, limits->streams, &minimum) ||
        minimum > limits->file_slots) {
        return VSR_ELIMIT;
    }
    end = (uint64_t)options->file_slot_base + limits->file_slots;
    if (end > ENGINE_FILE_SLOTS_END) {
        return VSR_ELIMIT;
    }
    /* Buffer regions: the pool plus one per replica for its tail. */
    if (!vsr_size_add(limits->replicas, 1, &minimum) ||
        minimum > limits->buffer_regions) {
        return VSR_ELIMIT;
    }
    end = (uint64_t)options->buffer_region_base + limits->buffer_regions;
    if (end > ENGINE_BUFFER_REGIONS_END) {
        return VSR_ELIMIT;
    }
    return VSR_OK;
}

static int engine_plan(const struct vsr_io_options *options,
                       struct engine_plan *plan)
{
    const struct vsr_io_limits *limits = &options->limits;
    size_t offset = 0;
    size_t alignment;
    size_t bytes;
    size_t capacity;
    int rc;

    memset(plan, 0, sizeof(*plan));
    rc = check_options(options);
    if (rc != VSR_OK) {
        return rc;
    }
    rc = vsr_io_pool_size(limits, ENGINE_PAGE_BYTES, &plan->pool_bytes,
                          &alignment);
    if (rc != VSR_OK) {
        return rc;
    }
    if (!vsr_size_mul(limits->slabs, limits->slab_bytes, &plan->payload)) {
        return VSR_ELIMIT;
    }
    /* Deadline handles: links + nodes + 4 * replicas + streams. */
    capacity = 0;
    for (uint16_t kind = 0; kind < VSR_IO_DEADLINE_KINDS; ++kind) {
        if (!vsr_size_add(capacity, deadline_count(limits, kind), &capacity)) {
            return VSR_ELIMIT;
        }
    }
    if (capacity >= UINT32_MAX) {
        return VSR_ELIMIT;
    }
    plan->deadlines_count = (uint32_t)capacity;
    if (vsr_io_slots_size(limits, options->listen_count,
                          ENGINE_INFLIGHT_WRITES_MAX, &bytes,
                          &plan->slots_count) != VSR_OK) {
        return VSR_ELIMIT;
    }
    if (!place(&offset, plan->pool_bytes, alignment, &plan->pool) ||
        !place(&offset, sizeof(struct vsr_io), alignof(struct vsr_io),
               &plan->io) ||
        !vsr_size_mul(options->listen_count, sizeof(struct vsr_io_address),
                      &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_address), &plan->listen) ||
        !vsr_size_mul(plan->slots_count, sizeof(struct vsr_io_slot), &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_slot), &plan->slots) ||
        vsr_io_deadlines_size(plan->deadlines_count, &bytes) != VSR_OK ||
        !place(&offset, bytes, alignof(struct vsr_io_deadline_entry),
               &plan->deadlines) ||
        vsr_io_links_size(limits, &plan->links_bytes, &alignment) != VSR_OK ||
        !place(&offset, plan->links_bytes, alignment, &plan->links) ||
        vsr_io_streams_size(limits, &plan->streams_bytes, &alignment) !=
            VSR_OK ||
        !place(&offset, plan->streams_bytes, alignment, &plan->streams) ||
        !vsr_size_mul(limits->ops, sizeof(struct vsr_io_forwarded), &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_forwarded),
               &plan->forwarded) ||
        !vsr_size_mul(limits->replicas, sizeof(struct vsr_io_replica),
                      &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_replica),
               &plan->replicas) ||
        !vsr_size_mul(limits->file_slots, sizeof(uint32_t), &bytes) ||
        !place(&offset, bytes, alignof(uint32_t), &plan->file_slots)) {
        return VSR_ELIMIT;
    }
    plan->total = offset;
    plan->alignment = ENGINE_PAGE_BYTES;
    return VSR_OK;
}

int vsr_io_engine_size(const struct vsr_io_options *options, size_t *bytes,
                       size_t *alignment)
{
    struct engine_plan plan;
    int rc;

    if (options == NULL || bytes == NULL || alignment == NULL) {
        return VSR_EINVAL;
    }
    rc = engine_plan(options, &plan);
    if (rc != VSR_OK) {
        return rc;
    }
    *bytes = plan.total;
    *alignment = plan.alignment;
    return VSR_OK;
}

int vsr_io_layout(const struct vsr_io_options *options,
                  struct vsr_io_layout *layout)
{
    struct engine_plan plan;
    int rc;

    if (layout == NULL) {
        return VSR_EINVAL;
    }
    memset(layout, 0, sizeof(*layout));
    if (options == NULL) {
        return VSR_EINVAL;
    }
    rc = engine_plan(options, &plan);
    if (rc != VSR_OK) {
        return rc;
    }
    layout->metadata.size = plan.total;
    layout->metadata.alignment = plan.alignment;
    layout->payload.size = plan.payload;
    layout->payload.alignment = ENGINE_PAGE_BYTES;
    return VSR_OK;
}

/* -------------------------------------------------------------------------
 * Init, close, deinit
 * ---------------------------------------------------------------------- */

static bool executor_complete(const struct vsr_io_executor *executor)
{
    const struct vsr_io_executor_ops *ops = executor->ops;

    return ops != NULL && ops->now != NULL && ops->random != NULL &&
           ops->submit_and_wait != NULL && ops->reap != NULL &&
           ops->register_files != NULL && ops->update_file != NULL &&
           ops->register_buffers != NULL && ops->update_buffer != NULL &&
           ops->buffer_ring != NULL && ops->provide != NULL &&
           ops->wake != NULL;
}

static bool misaligned(const void *pointer, size_t alignment)
{
    return ((uintptr_t)pointer & (alignment - 1)) != 0;
}

/* Binds every deadline handle to its owner: dense per kind, kinds in enum
 * order, so that the link and stream tables carry their handles and a
 * replica's handles are base + index (decision 60). */
static void bind_deadlines(struct vsr_io *io)
{
    const struct vsr_io_limits *limits = &io->options.limits;

    for (uint16_t kind = 0; kind < VSR_IO_DEADLINE_KINDS; ++kind) {
        uint32_t base = deadline_base(limits, kind);
        uint32_t count = deadline_count(limits, kind);

        for (uint32_t index = 0; index < count; ++index) {
            vsr_io_deadlines_bind(&io->deadlines, base + index, kind, index);
        }
    }
    for (uint32_t i = 0; i < io->links.links_count; ++i) {
        io->links.links[i].deadline =
            deadline_base(limits, VSR_IO_DEADLINE_LINK) + i;
    }
    for (uint32_t i = 0; i < io->streams.count; ++i) {
        io->streams.streams[i].deadline =
            deadline_base(limits, VSR_IO_DEADLINE_STREAM) + i;
    }
}

/* The pool region and its provided-buffer ring, in that order; a failed
 * ring registration undoes the region. Listener setup waits for the first
 * prepare (decision 45). */
static int register_pool(struct vsr_io *io)
{
    const struct vsr_io_executor *ex = &io->ex;
    struct vsr_io_region region;
    struct vsr_io_region ring;
    int rc;

    region.base = io->pool.base;
    region.size = io->pool.size;
    rc = ex->ops->update_buffer(ex->ctx, io->pool.region_index, &region);
    if (rc < 0) {
        return rc;
    }
    ring.base = io->pool.ring_memory;
    ring.size = (size_t)io->pool.ring_entries * ENGINE_RING_ENTRY_BYTES;
    rc = ex->ops->buffer_ring(ex->ctx, io->pool.group, io->pool.ring_entries,
                              VSR_IO_BUFFER_RING_INCREMENTAL, &ring);
    if (rc < 0) {
        (void)ex->ops->update_buffer(ex->ctx, io->pool.region_index, NULL);
        return rc;
    }
    io->pool.ring_registered = true;
    return VSR_OK;
}

int vsr_io_init(const struct vsr_io_options *options,
                const struct vsr_io_region *metadata,
                const struct vsr_io_region *payload, struct vsr_io **out)
{
    struct engine_plan plan;
    unsigned char *base;
    struct vsr_io *io;
    int rc;

    if (out == NULL) {
        return VSR_EINVAL;
    }
    *out = NULL;
    if (options == NULL || metadata == NULL || payload == NULL) {
        return VSR_EINVAL;
    }
    rc = engine_plan(options, &plan);
    if (rc != VSR_OK) {
        return rc;
    }
    if (!executor_complete(&options->executor) || metadata->base == NULL ||
        misaligned(metadata->base, plan.alignment) || payload->base == NULL ||
        misaligned(payload->base, ENGINE_PAGE_BYTES)) {
        return VSR_EINVAL;
    }
    if (metadata->size < plan.total || payload->size < plan.payload) {
        return VSR_ELIMIT;
    }
    base = metadata->base;
    io = (struct vsr_io *)(void *)(base + plan.io);
    memset(io, 0, sizeof(*io));
    io->options = *options;
    io->listen_copy = NULL;
    if (options->listen_count > 0) {
        io->listen_copy = (struct vsr_io_address *)(void *)(base + plan.listen);
        for (uint32_t i = 0; i < options->listen_count; ++i) {
            io->listen_copy[i] = options->listen[i];
        }
    }
    io->options.listen = io->listen_copy;
    io->ex = options->executor;
    io->state = ENGINE_RUNNING;
    io->page_bytes = ENGINE_PAGE_BYTES;
    vsr_io_pool_init(&io->pool, payload->base, plan.payload, &options->limits,
                     options->limits.replicas + 1, options->buffer_region_base,
                     options->buffer_group, base + plan.pool, plan.pool_bytes);
    vsr_io_slots_init(&io->slots, base + plan.slots, plan.slots_count,
                      options->owner);
    vsr_io_deadlines_init(&io->deadlines, base + plan.deadlines,
                          plan.deadlines_count);
    vsr_io_links_init(&io->links, base + plan.links, plan.links_bytes,
                      &options->limits, options->limits.slab_bytes);
    vsr_io_streams_init(&io->streams, base + plan.streams, plan.streams_bytes,
                        &options->limits, options->stream_chunk_bytes);
    bind_deadlines(io);
    io->replicas = (struct vsr_io_replica *)(void *)(base + plan.replicas);
    memset(io->replicas, 0,
           (size_t)options->limits.replicas * sizeof(*io->replicas));
    for (uint32_t i = 0; i < options->limits.replicas; ++i) {
        io->replicas[i].io = io;
        io->replicas[i].index = i;
        io->replicas[i].state = VSR_IO_REPLICA_FREE;
    }
    io->replicas_count = 0;
    io->file_slot_next = options->file_slot_base;
    io->file_slots_free = (uint32_t *)(void *)(base + plan.file_slots);
    io->file_slots_free_count = 0;
    io->forwarded = (struct vsr_io_forwarded *)(void *)(base + plan.forwarded);
    io->forwarded_head = 0;
    io->forwarded_count = 0;
    io->forwarded_overflow = 0;
    io->now = 0;
    io->wake_pending = 0;
    memset(&io->stats, 0, sizeof(io->stats));
    /* The executor is opaque to the engine; nothing in it needs the ring. */
    io->uring = NULL;
    rc = register_pool(io);
    if (rc != VSR_OK) {
        return rc;
    }
    *out = io;
    return VSR_OK;
}

/* Closing completes once every slot is free and every link is FREE
 * (decision 49); the loop re-checks after each completion. */
static void check_closed(struct vsr_io *io)
{
    if (io->state != ENGINE_CLOSING ||
        io->slots.free_count != io->slots.count) {
        return;
    }
    for (uint32_t i = 0; i < io->links.links_count; ++i) {
        if (io->links.links[i].state != VSR_IO_LINK_FREE) {
            return;
        }
    }
    io->state = ENGINE_CLOSED;
    io->stats.closed = 1;
}

int vsr_io_close(struct vsr_io *io)
{
    if (io == NULL) {
        return VSR_EINVAL;
    }
    if (io->replicas_count > 0) {
        return VSR_EBUSY;
    }
    if (io->state == ENGINE_RUNNING) {
        io->state = ENGINE_CLOSING;
    }
    check_closed(io);
    return VSR_OK;
}

int vsr_io_deinit(struct vsr_io *io)
{
    const struct vsr_io_executor *ex;
    int rc = VSR_OK;
    int result;

    if (io == NULL) {
        return VSR_EINVAL;
    }
    if (io->state != ENGINE_CLOSED) {
        return VSR_EBUSY;
    }
    ex = &io->ex;
    if (io->pool.ring_registered) {
        result = ex->ops->buffer_ring(ex->ctx, io->pool.group, 0, 0, NULL);
        if (result < 0) {
            rc = result;
        }
        vsr_io_pool_ring_lost(&io->pool);
    }
    result = ex->ops->update_buffer(ex->ctx, io->pool.region_index, NULL);
    if (result < 0 && rc == VSR_OK) {
        rc = result;
    }
    return rc;
}

void vsr_io_wake(struct vsr_io *io)
{
    if (io == NULL) {
        return;
    }
    /* The only cross-thread write into the engine; poll clears it with the
     * matching acquire exchange. */
    __atomic_store_n(&io->wake_pending, 1, __ATOMIC_RELEASE);
    io->ex.ops->wake(io->ex.ctx);
}

void vsr_io_get_stats(const struct vsr_io *io, struct vsr_io_stats *stats)
{
    if (io == NULL || stats == NULL) {
        return;
    }
    *stats = io->stats;
    stats->replicas = io->replicas_count;
    stats->links = io->links.established;
    stats->links_pending = io->links.pending;
    stats->streams = io->streams.active;
    stats->slabs_free = io->pool.free_count;
    stats->closed = io->state == ENGINE_CLOSED ? 1 : 0;
}

/* -------------------------------------------------------------------------
 * Caller slabs (decisions 54 and 61)
 * ---------------------------------------------------------------------- */

int vsr_io_slab_acquire(struct vsr_io *io, struct vsr_io_slab *slab)
{
    uint32_t id;

    if (io == NULL || slab == NULL || io->state != ENGINE_RUNNING) {
        return VSR_EINVAL;
    }
    id = vsr_io_pool_acquire(&io->pool, true);
    if (id == NONE) {
        return VSR_ELIMIT;
    }
    slab->base = vsr_io_pool_slab(&io->pool, id);
    slab->length = io->pool.slab_bytes;
    slab->id = (uint16_t)id;
    slab->region = (uint16_t)io->pool.region_index;
    return VSR_OK;
}

int vsr_io_slab_release(struct vsr_io *io, uint16_t id)
{
    if (io == NULL || !vsr_io_pool_caller_release(&io->pool, id)) {
        return VSR_EINVAL;
    }
    return VSR_OK;
}

/* -------------------------------------------------------------------------
 * Helpers for the planner modules
 * ---------------------------------------------------------------------- */

static uint32_t ring_slot(uint32_t head, uint32_t count, uint32_t capacity)
{
    return (uint32_t)(((uint64_t)head + count) % capacity);
}

/* A rail's descriptor lives in the ring entry, so op.data of a rail op
 * points into it; the entry is reused only after the poll that dequeues
 * it, which is the lifetime vsr-io.h gives such descriptors. */
struct vsr_io_forwarded *
vsr_io_forward(struct vsr_io *io, struct vsr_io_replica *replica, uint32_t kind)
{
    uint32_t capacity = io->options.limits.ops;
    struct vsr_io_forwarded *entry;

    if (io->forwarded_count == capacity) {
        io->forwarded_overflow = 1;
        return NULL;
    }
    entry = &io->forwarded[ring_slot(io->forwarded_head, io->forwarded_count,
                                     capacity)];
    io->forwarded_count++;
    memset(entry, 0, sizeof(*entry));
    entry->op.replica = replica;
    entry->op.kind = kind;
    return entry;
}

/* Internal completions queue in arrival order and are fed before every
 * other event of the next poll (section 7.1). The ring holds one entry per
 * outstanding core op, so it never fills. */
void vsr_io_engine_complete_core(struct vsr_io_replica *replica,
                                 const struct vsr_io_completion *completion)
{
    uint32_t capacity = replica->options.limits.operations;
    struct vsr_io_queued_event *queued;

    ENGINE_ASSERT(replica->completions_count < capacity);
    queued = &replica->completions[ring_slot(
        replica->completions_head, replica->completions_count, capacity)];
    replica->completions_count++;
    memset(queued, 0, sizeof(*queued));
    queued->event.type = VSR_EVENT_COMPLETE;
    queued->event.status = completion->status;
    queued->event.id = completion->op;
    queued->event.data = completion->data;
    queued->event.lease = completion->lease != NONE
                              ? vsr_io_lease_id(replica, completion->lease)
                              : 0;
    queued->lease = completion->lease;
    queued->kind = VSR_IO_EVENT_CORE;
}

/* Leases (section 7.6): an entry is a decode region plus the slab
 * reference or ring pin recorded for it. alloc records; the caller takes
 * the reference (a MESSAGE retains its receive slab, a cold LOAD hands
 * over the slab it acquired) and the store pins its ring range. release
 * drops the slab reference and frees the region; a ring pin is the
 * store's, released through vsr_io_store_release by the routing before
 * this call. */
uint32_t vsr_io_lease_alloc(struct vsr_io_replica *replica, uint32_t slab,
                            uint32_t pin)
{
    if (replica->leases_free == 0) {
        return NONE;
    }
    for (uint32_t i = 0; i < replica->regions_count; ++i) {
        struct vsr_io_lease *lease = &replica->leases[i];

        if (lease->state != LEASE_FREE) {
            continue;
        }
        lease->state = LEASE_QUEUED;
        lease->slab = slab;
        lease->pin = pin;
        lease->generation = (uint16_t)(lease->generation + 1u);
        lease->region.used = 0;
        replica->leases_free--;
        return i;
    }
    ENGINE_ASSERT(false);
    return NONE;
}

uint64_t vsr_io_lease_id(const struct vsr_io_replica *replica, uint32_t lease)
{
    return VSR_IO_LEASE_ENGINE |
           (uint64_t)replica->index << VSR_IO_LEASE_REPLICA_SHIFT |
           (uint64_t)lease << VSR_IO_LEASE_REGION_SHIFT |
           (uint64_t)replica->leases[lease].generation;
}

int vsr_io_lease_resolve(struct vsr_io *io, uint64_t lease,
                         struct vsr_io_replica **replica, uint32_t *index)
{
    uint32_t r = (uint32_t)((lease >> VSR_IO_LEASE_REPLICA_SHIFT) &
                            ENGINE_LEASE_REPLICA_MASK);
    uint32_t region = (uint32_t)((lease >> VSR_IO_LEASE_REGION_SHIFT) &
                                 ENGINE_LEASE_REGION_MASK);
    uint64_t generation = lease & VSR_IO_LEASE_GENERATION_MASK;
    struct vsr_io_replica *found;
    const struct vsr_io_lease *entry;

    if ((lease & VSR_IO_LEASE_ENGINE) == 0 ||
        r >= io->options.limits.replicas) {
        return VSR_EINVAL;
    }
    found = &io->replicas[r];
    if (found->state == VSR_IO_REPLICA_FREE || region >= found->regions_count) {
        return VSR_EINVAL;
    }
    entry = &found->leases[region];
    if (entry->state == LEASE_FREE || entry->generation != generation) {
        return VSR_EINVAL;
    }
    *replica = found;
    *index = region;
    return VSR_OK;
}

void vsr_io_lease_release(struct vsr_io_replica *replica, uint32_t lease)
{
    struct vsr_io_lease *entry = &replica->leases[lease];

    ENGINE_ASSERT(entry->state != LEASE_FREE);
    if (entry->slab != NONE) {
        vsr_io_pool_release(&replica->io->pool, entry->slab);
    }
    entry->slab = NONE;
    entry->pin = NONE;
    entry->region.used = 0;
    entry->state = LEASE_FREE;
    replica->leases_free++;
}

/* The replica is the one the link module resolved from the envelope's
 * cluster; the body is decoded into a fresh region with one reference on
 * its slab and queued as a MESSAGE event. A body the codec rejects, or one
 * naming another cluster, is dropped and counted; the link module checks
 * the sender's authorization for its node before delivering. */
bool vsr_io_engine_deliver(struct vsr_io *io, struct vsr_io_replica *replica,
                           const struct vsr_io_cursor *body, uint32_t slab)
{
    struct vsr_io_cursor cursor = *body;
    struct vsr_message *message = NULL;
    struct vsr_io_lease *lease;
    struct vsr_io_queued_event *queued;
    uint32_t index;
    int rc;

    if (replica->leases_free == 0) {
        return false;
    }
    index = vsr_io_lease_alloc(replica, slab, NONE);
    ENGINE_ASSERT(index != NONE);
    lease = &replica->leases[index];
    vsr_io_pool_retain(&io->pool, slab);
    rc = vsr_io_codec_decode_message(&cursor, &replica->options.limits,
                                     &lease->region, &message);
    if (rc == VSR_OK && (message->cluster.hi != replica->options.cluster.hi ||
                         message->cluster.lo != replica->options.cluster.lo)) {
        rc = VSR_EINVAL;
    }
    if (rc != VSR_OK) {
        vsr_io_lease_release(replica, index);
        io->stats.frames_rejected++;
        return true;
    }
    ENGINE_ASSERT(replica->messages_count < replica->regions_count);
    queued = &replica->messages[ring_slot(replica->messages_head,
                                          replica->messages_count,
                                          replica->regions_count)];
    replica->messages_count++;
    memset(queued, 0, sizeof(*queued));
    queued->event.type = VSR_EVENT_MESSAGE;
    queued->event.status = 0;
    queued->event.id = 0;
    queued->event.data = message;
    queued->event.lease = vsr_io_lease_id(replica, index);
    queued->lease = index;
    queued->kind = VSR_IO_EVENT_CORE;
    return true;
}

struct vsr_io_replica *vsr_io_engine_replica(struct vsr_io *io,
                                             struct vsr_id cluster)
{
    for (uint32_t i = 0; i < io->options.limits.replicas; ++i) {
        struct vsr_io_replica *replica = &io->replicas[i];

        if (replica->state != VSR_IO_REPLICA_FREE &&
            replica->options.cluster.hi == cluster.hi &&
            replica->options.cluster.lo == cluster.lo) {
            return replica;
        }
    }
    return NULL;
}

/* Engine file slots come from [file_slot_base, file_slot_base +
 * file_slots): freed slots are reused first, most recently freed first,
 * then the high-water mark advances. */
uint32_t vsr_io_engine_slot_alloc(struct vsr_io *io)
{
    uint32_t end = io->options.file_slot_base + io->options.limits.file_slots;

    if (io->file_slots_free_count > 0) {
        io->file_slots_free_count--;
        return io->file_slots_free[io->file_slots_free_count];
    }
    if (io->file_slot_next < end) {
        return io->file_slot_next++;
    }
    return NONE;
}

void vsr_io_engine_slot_free(struct vsr_io *io, uint32_t slot)
{
    ENGINE_ASSERT(slot >= io->options.file_slot_base &&
                  slot < io->file_slot_next);
    ENGINE_ASSERT(io->file_slots_free_count < io->options.limits.file_slots);
    io->file_slots_free[io->file_slots_free_count] = slot;
    io->file_slots_free_count++;
}

void vsr_io_engine_random(struct vsr_io *io, void *bytes, size_t size)
{
    io->ex.ops->random(io->ex.ctx, bytes, size);
}

int vsr_io_engine_install(struct vsr_io *io, uint32_t slot, int fd)
{
    return io->ex.ops->update_file(io->ex.ctx, slot, fd);
}

/*
 * Part 2 of this file, once the store, link, stream and snapshot modules
 * exist, adds on top of the structures above:
 *   - vsr_io_replica_size and vsr_io_replica_layout: the core arena, the
 *     event, message and completion rings, the step scratch arrays, the
 *     lease table and decode regions (input_leases + events, each
 *     max(message_region, load_region) bytes), the store and snapshot
 *     bookkeeping, and the tail region;
 *   - vsr_io_attach, vsr_io_detach, vsr_io_replica_status,
 *     vsr_io_replica_core, vsr_io_replica_find, with the replica deadline
 *     handles bound as base[kind] + index and the store's inflight_writes
 *     checked against ENGINE_INFLIGHT_WRITES_MAX;
 *   - vsr_io_node_set, vsr_io_node_clear, vsr_io_authorize, vsr_io_adopt
 *     and vsr_io_node_status over the link module;
 *   - vsr_io_complete, vsr_io_poll (with the event order of section 7.1,
 *     the op routing of 7.3 and the drain of the forwarded ring),
 *     vsr_io_submit, vsr_io_prepare (provision, links, streams, per
 *     replica store and snapshots, the earliest deadline) and vsr_io_run;
 *   - close completion: vsr_io_close calls the link and stream shutdowns
 *     and check_closed runs after every completion;
 *   - STATUS emission once per poll with STATE_CHANGED and once at
 *     STOPPED, and the replica state transitions of 7.7.
 */
