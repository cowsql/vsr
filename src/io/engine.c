#include "config.h"

#include "io/engine.h"

#include "checked.h"

#include <stdalign.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

/*
 * Engine kernel: the metadata layout, initialization, close and executor
 * registration of struct vsr_io, and the helpers every planner module
 * calls (docs/io-implementation.md, section 7). The replica life cycle
 * lives in replica.c and the loop entry points with the op routing in
 * loop.c: this file references no store or snapshot symbol, so a program
 * that only drives the link and stream modules (tests/unit/stream.c with
 * its snapshot doubles) links without snapshot.o.
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
 *  11. vsr_io_run's scratch: records [batch], completions [batch], ops
 *      [ops], events [ops + events]
 *  12. the queued slot clears [file_slots] and the PROVIDE record's
 *      buffers [slabs]
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
#define ENGINE_INFLIGHT_WRITES_MAX VSR_IO_ENGINE_INFLIGHT_WRITES_MAX
#define ENGINE_BATCH_MIN VSR_IO_ENGINE_BATCH_MIN
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

#define ENGINE_RUNNING VSR_IO_ENGINE_RUNNING
#define ENGINE_CLOSING VSR_IO_ENGINE_CLOSING
#define ENGINE_CLOSED VSR_IO_ENGINE_CLOSED
#define LEASE_FREE VSR_IO_LEASE_STATE_FREE
#define LEASE_QUEUED VSR_IO_LEASE_STATE_QUEUED
/* Pool reserve per replica: a cold-load or recovery read slab, and the
 * snapshot module's writer, writer-cold and reader slabs (decision 127). */
#define ENGINE_RESERVE_PER_REPLICA 4u
/* One reassembly slab, and the slabs the ring keeps beyond one per link
 * and one per stream in the minimum-slabs rule. */
#define ENGINE_RESERVE_REASSEMBLY 1u
#define ENGINE_RING_MIN 4u

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
    size_t run_sqes;
    size_t run_cqes;
    size_t run_ops;
    size_t run_events;
    uint32_t run_events_count;
    size_t clears;
    size_t provide;
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
    /* The link module keeps listener state in a fixed table (decision 72). */
    if (options->listen_count > VSR_IO_LISTENERS_MAX) {
        return VSR_ELIMIT;
    }
    /* Minimum slabs: the reserve (links + streams * stream_window + 4 *
     * replicas + 1), the ring's links + streams + 4 (a slab per link
     * receiving, one per stream, four more) and caller_slabs, that is
     * 2 * links + streams * (stream_window + 1) + 4 * replicas + 5 +
     * caller_slabs (decisions 42, 54 and 127). */
    if (!vsr_size_add(limits->stream_window, 1, &term) ||
        !vsr_size_mul(limits->streams, term, &term) ||
        !vsr_size_add(limits->links, term, &minimum) ||
        !vsr_size_add(minimum, limits->links, &minimum) ||
        !vsr_size_mul(limits->replicas, ENGINE_RESERVE_PER_REPLICA, &term) ||
        !vsr_size_add(minimum, term, &minimum) ||
        !vsr_size_add(minimum, ENGINE_RESERVE_REASSEMBLY + ENGINE_RING_MIN,
                      &minimum) ||
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
    for (uint16_t kind = 0; kind < (uint16_t)VSR_IO_DEADLINE_KINDS; ++kind) {
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
        !place(&offset, bytes, alignof(uint32_t), &plan->file_slots) ||
        !vsr_size_mul(limits->batch, sizeof(struct vsr_io_sqe), &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_sqe), &plan->run_sqes) ||
        !vsr_size_mul(limits->batch, sizeof(struct vsr_io_cqe), &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_cqe), &plan->run_cqes) ||
        !vsr_size_mul(limits->ops, sizeof(struct vsr_io_op), &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_op), &plan->run_ops) ||
        !vsr_size_add(limits->ops, limits->events, &capacity) ||
        capacity > UINT32_MAX ||
        !vsr_size_mul(capacity, sizeof(struct vsr_io_event), &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_event),
               &plan->run_events) ||
        !vsr_size_mul(limits->file_slots, sizeof(uint32_t), &bytes) ||
        !place(&offset, bytes, alignof(uint32_t), &plan->clears) ||
        !vsr_size_mul(limits->slabs, sizeof(struct vsr_io_buffer), &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_buffer), &plan->provide)) {
        return VSR_ELIMIT;
    }
    plan->run_events_count = (uint32_t)capacity;
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

    for (uint16_t kind = 0; kind < (uint16_t)VSR_IO_DEADLINE_KINDS; ++kind) {
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
    for (uint32_t i = 0; i < limits->replicas; ++i) {
        struct vsr_io_replica *replica = &io->replicas[i];

        replica->deadline_core =
            deadline_base(limits, VSR_IO_DEADLINE_CORE) + i;
        replica->deadline_flush =
            deadline_base(limits, VSR_IO_DEADLINE_FLUSH) + i;
        replica->deadline_sync =
            deadline_base(limits, VSR_IO_DEADLINE_SYNC) + i;
        replica->deadline_capture =
            deadline_base(limits, VSR_IO_DEADLINE_CAPTURE) + i;
    }
}

uint32_t vsr_io_engine_reserve(const struct vsr_io_limits *limits)
{
    /* check_options bounded the sum by limits->slabs (<= 32768). */
    return limits->links + limits->streams * limits->stream_window +
           ENGINE_RESERVE_PER_REPLICA * limits->replicas +
           ENGINE_RESERVE_REASSEMBLY;
}

/* The generator (decision 134): xoshiro256**, seeded once from the
 * executor's entropy at init, so that nothing the primitives reach calls
 * the executor; a simulation stays deterministic by its seed. The values
 * are unique, not secret: a keyed handshake needing secret nonces would
 * draw them from an entropy pool the loop refills. */
static uint64_t rotate(uint64_t x, int k)
{
    return (x << k) | (x >> (64 - k));
}

static uint64_t random_next(struct vsr_io *io)
{
    uint64_t *s = io->random_state;
    uint64_t result = rotate(s[1] * 5u, 7) * 9u;
    uint64_t t = s[1] << 17;

    s[2] ^= s[0];
    s[3] ^= s[1];
    s[1] ^= s[2];
    s[0] ^= s[3];
    s[2] ^= t;
    s[3] = rotate(s[3], 45);
    return result;
}

static void random_seed(struct vsr_io *io)
{
    uint64_t *s = io->random_state;

    io->ex.ops->random(io->ex.ctx, s, sizeof(io->random_state));
    if ((s[0] | s[1] | s[2] | s[3]) == 0) {
        s[0] = UINT64_C(0x9E3779B97F4A7C15); /* The all-zero state sticks. */
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
                     vsr_io_engine_reserve(&options->limits),
                     options->buffer_region_base, options->buffer_group,
                     base + plan.pool, plan.pool_bytes);
    vsr_io_slots_init(&io->slots, base + plan.slots, plan.slots_count,
                      options->owner);
    vsr_io_deadlines_init(&io->deadlines, base + plan.deadlines,
                          plan.deadlines_count);
    vsr_io_links_init(&io->links, base + plan.links, plan.links_bytes,
                      &options->limits, options->limits.slab_bytes);
    vsr_io_streams_init(&io->streams, base + plan.streams, plan.streams_bytes,
                        &options->limits, options->stream_chunk_bytes);
    io->replicas = (struct vsr_io_replica *)(void *)(base + plan.replicas);
    memset(io->replicas, 0,
           (size_t)options->limits.replicas * sizeof(*io->replicas));
    for (uint32_t i = 0; i < options->limits.replicas; ++i) {
        io->replicas[i].io = io;
        io->replicas[i].index = i;
        io->replicas[i].state = VSR_IO_REPLICA_FREE;
    }
    bind_deadlines(io);
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
    io->releases_rejected = 0;
    io->events_rejected = 0;
    io->run_sqes = (struct vsr_io_sqe *)(void *)(base + plan.run_sqes);
    io->run_cqes = (struct vsr_io_cqe *)(void *)(base + plan.run_cqes);
    io->run_ops = (struct vsr_io_op *)(void *)(base + plan.run_ops);
    io->run_events = (struct vsr_io_event *)(void *)(base + plan.run_events);
    io->run_events_capacity = plan.run_events_count;
    io->clears = (uint32_t *)(void *)(base + plan.clears);
    io->clears_count = 0;
    io->provide_buffers = (struct vsr_io_buffer *)(void *)(base + plan.provide);
    random_seed(io);
    rc = register_pool(io);
    if (rc != VSR_OK) {
        return rc;
    }
    *out = io;
    return VSR_OK;
}

/* Closing completes once every slot is free, every link FREE and no
 * stream active (decisions 49 and 97); the loop re-checks after each
 * completion batch, poll and prepare. */
void vsr_io_engine_check_closed(struct vsr_io *io)
{
    if (io->state != ENGINE_CLOSING ||
        io->slots.free_count != io->slots.count || io->streams.active != 0 ||
        io->clears_count != 0) {
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

/* The link and stream shutdowns may come in either order (decision 101):
 * every stream ends CANCELLED on this engine whichever runs first. The
 * links go first, as section 7.7 lists them. */
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
        vsr_io_links_shutdown(io);
        vsr_io_streams_shutdown(io);
    }
    vsr_io_engine_check_closed(io);
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
    if (io->state != ENGINE_CLOSED || io->replicas_count > 0) {
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
 * points into it; the entry is reused by the first producer after the
 * poll that dequeues it, which vsr-io.h bounds by the caller's next call
 * into the engine (decision 132). */
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

/* The deferred ring is due in order: io->now never decreases and retry_ns
 * is the replica's. It holds one entry per outstanding core op at most,
 * like the completion ring (an op is in one of them or in neither). */
void vsr_io_engine_complete_later(struct vsr_io_replica *replica, uint64_t op,
                                  int32_t status)
{
    uint32_t capacity = replica->options.limits.operations;
    struct vsr_io_deferred *deferred;

    ENGINE_ASSERT(replica->deferred_count < capacity);
    deferred = &replica->deferred[ring_slot(replica->deferred_head,
                                            replica->deferred_count, capacity)];
    deferred->due = replica->io->now + replica->options.retry_ns;
    deferred->op = op;
    deferred->status = status;
    deferred->reserved = 0;
    replica->deferred_count++;
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
        if (slab != NONE) {
            /* The bytes are the lease's now (a cold LOAD result, a
             * reassembled MESSAGE): the reserve is free again for the
             * internal user that acquired the slab (decision 127). */
            vsr_io_pool_handoff(&replica->io->pool, slab);
        }
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

/* A replica whose STOP was submitted is not resolved: its core refuses
 * MESSAGEs (they are dropped and counted, decision 75) and its files are
 * not served any more (NOT_FOUND), so the served streams that would hold
 * its detach off end (decision 130). */
struct vsr_io_replica *vsr_io_engine_replica(struct vsr_io *io,
                                             struct vsr_id cluster)
{
    for (uint32_t i = 0; i < io->options.limits.replicas; ++i) {
        struct vsr_io_replica *replica = &io->replicas[i];

        if (replica->state != VSR_IO_REPLICA_FREE && replica->stopping == 0 &&
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

void vsr_io_engine_slot_clear(struct vsr_io *io, uint32_t slot)
{
    ENGINE_ASSERT(slot >= io->options.file_slot_base &&
                  slot < io->file_slot_next);
    ENGINE_ASSERT(io->clears_count < io->options.limits.file_slots);
    io->clears[io->clears_count] = slot;
    io->clears_count++;
}

bool vsr_io_engine_slot_clearing(const struct vsr_io *io, uint32_t slot)
{
    for (uint32_t i = 0; i < io->clears_count; ++i) {
        if (io->clears[i] == slot) {
            return true;
        }
    }
    for (uint32_t i = 0; i < io->slots.count; ++i) {
        if (io->slots.slots[i].kind == VSR_IO_SLOT_FILES &&
            io->slots.slots[i].owner == slot) {
            return true;
        }
    }
    return false;
}

/* The descriptor a clear installs: none. */
static const int32_t engine_empty_fd = -1;

void vsr_io_engine_prepare_files(struct vsr_io *io, struct vsr_io_sqe *sqes,
                                 uint32_t capacity, uint32_t *count)
{
    while (io->clears_count > 0 && *count < capacity) {
        uint32_t file = io->clears[io->clears_count - 1];
        uint32_t slot =
            vsr_io_slots_alloc(&io->slots, VSR_IO_SLOT_FILES, 1, file, 0, 0);
        struct vsr_io_sqe *sqe;

        if (slot == NONE) {
            return;
        }
        io->clears_count--;
        sqe = &sqes[*count];
        memset(sqe, 0, sizeof(*sqe));
        sqe->opcode = VSR_IO_SQE_FILES_UPDATE;
        sqe->fd = -1;
        sqe->offset = file;
        sqe->addr = &engine_empty_fd;
        sqe->length = 1;
        sqe->user_data = vsr_io_slots_user_data(&io->slots, slot);
        (*count)++;
    }
}

/* Whatever the result, the slot holds no file the engine still wants: a
 * failed clear leaves at most a file a later install replaces. */
void vsr_io_engine_files_complete(struct vsr_io *io, uint32_t slot,
                                  const struct vsr_io_cqe *cqe)
{
    uint32_t file = io->slots.slots[slot].owner;

    (void)cqe;
    vsr_io_slots_consumed(&io->slots, slot, false);
    vsr_io_engine_slot_free(io, file);
}

uint64_t vsr_io_engine_provide_user_data(const struct vsr_io *io)
{
    return VSR_IO_USER_DATA(io->options.owner,
                            (uint64_t)VSR_IO_SLOT_PROVIDE << 48);
}

void vsr_io_engine_random(struct vsr_io *io, void *bytes, size_t size)
{
    unsigned char *out = bytes;

    while (size > 0) {
        uint64_t value = random_next(io);
        size_t take = size < sizeof(value) ? size : sizeof(value);

        memcpy(out, &value, take);
        out += take;
        size -= take;
    }
}

/* -------------------------------------------------------------------------
 * Nodes, authorizations and adopted sockets: thin over the link module,
 * which holds the tables and every rule (decisions 72, 73 and 86).
 * ---------------------------------------------------------------------- */

int vsr_io_node_set(struct vsr_io *io, uint64_t node,
                    const struct vsr_io_address *address)
{
    if (io == NULL) {
        return VSR_EINVAL;
    }
    return vsr_io_links_node_set(io, node, address);
}

int vsr_io_node_clear(struct vsr_io *io, uint64_t node)
{
    if (io == NULL) {
        return VSR_EINVAL;
    }
    return vsr_io_links_node_clear(io, node);
}

int vsr_io_authorize(struct vsr_io *io, struct vsr_id cluster, uint64_t replica,
                     uint64_t node)
{
    if (io == NULL) {
        return VSR_EINVAL;
    }
    return vsr_io_links_authorize(io, cluster, replica, node);
}

int vsr_io_adopt(struct vsr_io *io, int fd, uint64_t node, uint32_t flags)
{
    if (io == NULL) {
        return VSR_EINVAL;
    }
    return vsr_io_links_adopt(io, fd, node, flags);
}

int vsr_io_node_status(const struct vsr_io *io, uint64_t node,
                       struct vsr_io_node_status *status)
{
    if (io == NULL || status == NULL) {
        return VSR_EINVAL;
    }
    return vsr_io_links_node_status(io, node, status);
}
