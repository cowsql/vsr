/* The GNU declarations (AT_FDCWD) precede every system header. */
#define _GNU_SOURCE
#include "config.h"

#include "io/engine.h"

#include "checked.h"

#include <fcntl.h>
#include <stdalign.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

/*
 * Replica life cycle (docs/io-implementation.md 7.7): the layout of a
 * replica's two regions, attach, detach and the status accessors.
 *
 * Metadata region, in offset order, every offset with checked arithmetic:
 *   1. the core arena (vsr_layout), first so that its alignment is the
 *      region's at offset 0
 *   2. the seed membership's members, copied
 *   3. the store directory, copied (VSR_IO_ENGINE_PATH_BYTES)
 *   4. the internal completion ring [operations] and the deferred
 *      completions [operations]
 *   5. the caller's COMPLETE/STOP ring [operations + 1]
 *   6. the caller's other events [limits.events]
 *   7. the MESSAGE ring [regions]
 *   8. the step scratch: events [every ring + TIME], ops [operations]
 *   9. the lease table [regions] and the decode regions [regions] of
 *      max(message region, load region, snapshot checkpoint copy) bytes
 *  10. the store's bookkeeping (vsr_io_store_size)
 *  11. the snapshot module's (vsr_io_snapshots_size)
 * regions = core input_leases + engine events (decision 42). The tail
 * region is the store's ring and superblocks, registered as the executor
 * buffer region buffer_region_base + 1 + replica index.
 */

#define NONE VSR_IO_INDEX_NONE
#define REGION_ALIGNMENT 16u

/* Invariant checks in debug builds; a violation traps (see pool.c). */
#ifdef NDEBUG
#define REPLICA_ASSERT(condition) ((void)sizeof(condition))
#else
#define REPLICA_ASSERT(condition) ((condition) ? (void)0 : __builtin_trap())
#endif

struct replica_plan {
    size_t core;
    size_t core_bytes;
    size_t members;
    size_t path;
    size_t completions;
    size_t deferred;
    size_t priority;
    size_t events;
    size_t messages;
    size_t step_events;
    size_t step_ops;
    size_t leases;
    size_t regions_memory;
    size_t region_bytes;
    size_t store;
    size_t store_bytes;
    size_t snapshots;
    size_t snapshots_bytes;
    size_t total;
    size_t alignment;
    size_t tail_bytes;
    size_t tail_alignment;
    uint32_t operations;
    uint32_t priority_capacity;
    uint32_t regions;
    uint32_t step_events_count;
    uint32_t step_capacity;
    uint32_t members_count;
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

static size_t larger(size_t a, size_t b)
{
    return a > b ? a : b;
}

static bool misaligned(const void *pointer, size_t alignment)
{
    return ((uintptr_t)pointer & (alignment - 1)) != 0;
}

static bool power_of_two(size_t value)
{
    return value != 0 && (value & (value - 1)) == 0;
}

/* Length of a NUL-terminated path within the bound, or the bound when it
 * does not end within it. */
static size_t path_length(const char *path)
{
    const char *end = memchr(path, 0, VSR_IO_ENGINE_PATH_BYTES);

    return end == NULL ? VSR_IO_ENGINE_PATH_BYTES : (size_t)(end - path);
}

/* The attach-time rules and the region plan: EINVAL for a malformed
 * option, ELIMIT for a capacity the engine cannot meet (vsr-io.h's sizing
 * rules), and the core's and the store's own verdicts. */
static int replica_plan(const struct vsr_io *io,
                        const struct vsr_io_replica_options *options,
                        struct replica_plan *plan)
{
    const struct vsr_limits *limits = &options->core.limits;
    struct vsr_layout core;
    uint64_t frame = 0;
    size_t offset = 0;
    size_t bytes;
    size_t alignment;
    size_t region = 0;
    size_t count;
    int rc;

    memset(plan, 0, sizeof(*plan));
    if (options->reserved != 0 || options->path == NULL ||
        options->path[0] == 0 ||
        path_length(options->path) == VSR_IO_ENGINE_PATH_BYTES) {
        return VSR_EINVAL;
    }
    rc = vsr_layout(&options->core, &core);
    if (rc != VSR_OK) {
        return rc;
    }
    if (!power_of_two(core.alignment)) {
        return VSR_EINVAL;
    }
    rc = vsr_io_store_check(&options->store, limits,
                            io->options.limits.slab_bytes);
    if (rc != VSR_OK) {
        return rc;
    }
    /* The slot table reserves this many writes per replica (decision 67). */
    if (options->store.inflight_writes > VSR_IO_ENGINE_INFLIGHT_WRITES_MAX) {
        return VSR_ELIMIT;
    }
    /* A frame of this replica fits one slab (decision 24). */
    if (vsr_io_codec_frame_limit(limits, &frame) != VSR_OK ||
        frame > io->options.limits.slab_bytes) {
        return VSR_ELIMIT;
    }
    plan->operations = limits->operations;
    if (limits->operations == UINT32_MAX ||
        !vsr_size_add(limits->input_leases, io->options.limits.events,
                      &count) ||
        count > VSR_IO_LEASE_REGIONS_MAX) {
        return VSR_ELIMIT;
    }
    plan->regions = (uint32_t)count;
    plan->priority_capacity = limits->operations + 1;
    /* Every queue plus TIME, so one step can take all of them. */
    if (!vsr_size_add(plan->operations, plan->priority_capacity, &count) ||
        !vsr_size_add(count, io->options.limits.events, &count) ||
        !vsr_size_add(count, plan->regions, &count) ||
        !vsr_size_add(count, 1, &count) || count > UINT32_MAX) {
        return VSR_ELIMIT;
    }
    plan->step_events_count = (uint32_t)count;
    plan->step_capacity = limits->operations;
    plan->members_count = options->core.seed->count;
    /* Decode regions: a MESSAGE, a LOAD result or a snapshot checkpoint. */
    if (vsr_io_codec_message_region(limits, &bytes) != VSR_OK) {
        return VSR_ELIMIT;
    }
    region = larger(region, bytes);
    if (vsr_io_codec_load_region(limits, &bytes) != VSR_OK) {
        return VSR_ELIMIT;
    }
    region = larger(region, bytes);
    if (vsr_io_snapshots_region_bytes(limits, &bytes) != VSR_OK) {
        return VSR_ELIMIT;
    }
    region = larger(region, bytes);
    if (!vsr_size_add(region, REGION_ALIGNMENT - 1, &region)) {
        return VSR_ELIMIT;
    }
    plan->region_bytes = region & ~(size_t)(REGION_ALIGNMENT - 1);
    rc = vsr_io_store_size(&options->store, limits, plan->regions,
                           &plan->store_bytes, &alignment, &plan->tail_bytes,
                           &plan->tail_alignment);
    if (rc != VSR_OK) {
        return rc;
    }
    plan->alignment = larger(core.alignment, alignment);
    plan->core_bytes = core.size;
    if (!place(&offset, core.size, core.alignment, &plan->core) ||
        !vsr_size_mul(plan->members_count, sizeof(struct vsr_member), &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_member), &plan->members) ||
        !place(&offset, VSR_IO_ENGINE_PATH_BYTES, 1, &plan->path) ||
        !vsr_size_mul(plan->operations, sizeof(struct vsr_io_queued_event),
                      &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_queued_event),
               &plan->completions) ||
        !vsr_size_mul(plan->operations, sizeof(struct vsr_io_deferred),
                      &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_deferred),
               &plan->deferred) ||
        !vsr_size_mul(plan->priority_capacity,
                      sizeof(struct vsr_io_queued_event), &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_queued_event),
               &plan->priority) ||
        !vsr_size_mul(io->options.limits.events,
                      sizeof(struct vsr_io_queued_event), &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_queued_event),
               &plan->events) ||
        !vsr_size_mul(plan->regions, sizeof(struct vsr_io_queued_event),
                      &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_queued_event),
               &plan->messages) ||
        !vsr_size_mul(plan->step_events_count, sizeof(struct vsr_event),
                      &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_event), &plan->step_events) ||
        !vsr_size_mul(plan->step_capacity, sizeof(struct vsr_op), &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_op), &plan->step_ops) ||
        !vsr_size_mul(plan->regions, sizeof(struct vsr_io_lease), &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_lease), &plan->leases) ||
        !vsr_size_mul(plan->regions, plan->region_bytes, &bytes) ||
        !place(&offset, bytes, REGION_ALIGNMENT, &plan->regions_memory) ||
        !place(&offset, plan->store_bytes, alignment, &plan->store)) {
        return VSR_ELIMIT;
    }
    rc = vsr_io_snapshots_size(limits, &io->options.limits,
                               options->store.max_clients,
                               &plan->snapshots_bytes, &alignment);
    if (rc != VSR_OK) {
        return rc;
    }
    plan->alignment = larger(plan->alignment, alignment);
    if (!place(&offset, plan->snapshots_bytes, alignment, &plan->snapshots)) {
        return VSR_ELIMIT;
    }
    plan->total = offset;
    return VSR_OK;
}

int vsr_io_replica_size(const struct vsr_io *io,
                        const struct vsr_io_replica_options *options,
                        size_t *metadata_bytes, size_t *metadata_alignment,
                        size_t *tail_bytes, size_t *tail_alignment)
{
    struct replica_plan plan;
    int rc;

    if (io == NULL || options == NULL || metadata_bytes == NULL ||
        metadata_alignment == NULL || tail_bytes == NULL ||
        tail_alignment == NULL) {
        return VSR_EINVAL;
    }
    rc = replica_plan(io, options, &plan);
    if (rc != VSR_OK) {
        return rc;
    }
    *metadata_bytes = plan.total;
    *metadata_alignment = plan.alignment;
    *tail_bytes = plan.tail_bytes;
    *tail_alignment = plan.tail_alignment;
    return VSR_OK;
}

int vsr_io_replica_layout(const struct vsr_io *io,
                          const struct vsr_io_replica_options *options,
                          struct vsr_io_replica_layout *layout)
{
    if (layout == NULL) {
        return VSR_EINVAL;
    }
    memset(layout, 0, sizeof(*layout));
    if (io == NULL || options == NULL) {
        return VSR_EINVAL;
    }
    return vsr_io_replica_size(io, options, &layout->metadata.size,
                               &layout->metadata.alignment, &layout->tail.size,
                               &layout->tail.alignment);
}

/* -------------------------------------------------------------------------
 * Attach
 * ---------------------------------------------------------------------- */

static bool cluster_attached(const struct vsr_io *io, struct vsr_id cluster)
{
    for (uint32_t i = 0; i < io->options.limits.replicas; ++i) {
        const struct vsr_io_replica *replica = &io->replicas[i];

        if (replica->state != VSR_IO_REPLICA_FREE &&
            replica->options.cluster.hi == cluster.hi &&
            replica->options.cluster.lo == cluster.lo) {
            return true;
        }
    }
    return false;
}

/* Carves the queues, leases and decode regions out of the metadata
 * region and empties them. */
static void replica_tables(struct vsr_io_replica *replica,
                           const struct replica_plan *plan, unsigned char *base)
{
    replica->completions =
        (struct vsr_io_queued_event *)(void *)(base + plan->completions);
    replica->completions_head = 0;
    replica->completions_count = 0;
    replica->deferred =
        (struct vsr_io_deferred *)(void *)(base + plan->deferred);
    replica->deferred_head = 0;
    replica->deferred_count = 0;
    replica->priority =
        (struct vsr_io_queued_event *)(void *)(base + plan->priority);
    replica->priority_head = 0;
    replica->priority_count = 0;
    replica->priority_capacity = plan->priority_capacity;
    replica->events =
        (struct vsr_io_queued_event *)(void *)(base + plan->events);
    replica->events_head = 0;
    replica->events_count = 0;
    replica->messages =
        (struct vsr_io_queued_event *)(void *)(base + plan->messages);
    replica->messages_head = 0;
    replica->messages_count = 0;
    replica->step_events =
        (struct vsr_event *)(void *)(base + plan->step_events);
    replica->step_events_capacity = plan->step_events_count;
    replica->step_ops = (struct vsr_op *)(void *)(base + plan->step_ops);
    replica->step_capacity = plan->step_capacity;
    replica->regions_count = plan->regions;
    replica->leases = (struct vsr_io_lease *)(void *)(base + plan->leases);
    replica->leases_free = plan->regions;
    for (uint32_t i = 0; i < plan->regions; ++i) {
        struct vsr_io_lease *lease = &replica->leases[i];

        memset(lease, 0, sizeof(*lease));
        lease->state = VSR_IO_LEASE_STATE_FREE;
        lease->slab = NONE;
        lease->pin = NONE;
        vsr_io_bump_init(&lease->region,
                         base + plan->regions_memory +
                             (size_t)i * plan->region_bytes,
                         plan->region_bytes);
    }
}

int vsr_io_attach(struct vsr_io *io,
                  const struct vsr_io_replica_options *options,
                  const struct vsr_io_region *metadata,
                  const struct vsr_io_region *tail, struct vsr_io_replica **out)
{
    const struct vsr_io_executor *ex;
    struct vsr_io_replica *replica = NULL;
    struct replica_plan plan;
    struct vsr_io_region region;
    unsigned char *base;
    struct vsr *core = NULL;
    uint32_t region_index;
    int rc;

    if (out == NULL) {
        return VSR_EINVAL;
    }
    *out = NULL;
    if (io == NULL || options == NULL || metadata == NULL || tail == NULL ||
        io->state != VSR_IO_ENGINE_RUNNING) {
        return VSR_EINVAL;
    }
    rc = replica_plan(io, options, &plan);
    if (rc != VSR_OK) {
        return rc;
    }
    if (metadata->base == NULL || misaligned(metadata->base, plan.alignment) ||
        tail->base == NULL || misaligned(tail->base, plan.tail_alignment) ||
        cluster_attached(io, options->core.cluster)) {
        return VSR_EINVAL;
    }
    if (metadata->size < plan.total || tail->size < plan.tail_bytes) {
        return VSR_ELIMIT;
    }
    for (uint32_t i = 0; i < io->options.limits.replicas; ++i) {
        if (io->replicas[i].state == VSR_IO_REPLICA_FREE) {
            replica = &io->replicas[i];
            break;
        }
    }
    if (replica == NULL) {
        return VSR_EBUSY;
    }
    /* The tail first: nothing else changed yet if the executor refuses. */
    ex = &io->ex;
    region_index = io->options.buffer_region_base + 1 + replica->index;
    region.base = tail->base;
    region.size = plan.tail_bytes;
    rc = ex->ops->update_buffer(ex->ctx, region_index, &region);
    if (rc < 0) {
        return rc;
    }
    base = metadata->base;
    rc = vsr_init(base + plan.core, plan.core_bytes, &options->core, &core);
    if (rc != VSR_OK) {
        (void)ex->ops->update_buffer(ex->ctx, region_index, NULL);
        return rc;
    }
    replica->core = core;
    replica->options = options->core;
    replica->seed_copy = *options->core.seed;
    replica->seed_members = (struct vsr_member *)(void *)(base + plan.members);
    if (plan.members_count > 0) {
        memcpy(replica->seed_members, options->core.seed->members,
               (size_t)plan.members_count * sizeof(struct vsr_member));
    }
    replica->seed_copy.members =
        plan.members_count > 0 ? replica->seed_members : NULL;
    replica->options.seed = &replica->seed_copy;
    replica->path = (char *)(base + plan.path);
    memcpy(replica->path, options->path, path_length(options->path) + 1);
    replica_tables(replica, &plan, base);
    replica->stopping = 0;
    replica->status_pending = 0;
    memset(&replica->status, 0, sizeof(replica->status));
    replica->core_deadline = VSR_NO_DEADLINE;
    replica->rejected = 0;
    replica->metadata = *metadata;
    replica->tail = *tail;
    replica->tail_region = region_index;
    vsr_io_store_init(&replica->store, base + plan.store, plan.store_bytes,
                      tail->base, plan.tail_bytes, &options->store,
                      &options->core.limits, plan.regions, region_index,
                      replica->path, AT_FDCWD);
    vsr_io_snapshots_init(&replica->snapshots, base + plan.snapshots,
                          plan.snapshots_bytes, &options->core.limits,
                          &io->options.limits, options->store.max_clients);
    vsr_io_deadlines_arm(&io->deadlines, replica->deadline_core,
                         VSR_NO_DEADLINE);
    replica->state = VSR_IO_REPLICA_OPENING;
    io->replicas_count++;
    *out = replica;
    return VSR_OK;
}

/* -------------------------------------------------------------------------
 * Detach
 * ---------------------------------------------------------------------- */

static bool replica_valid(const struct vsr_io_replica *replica)
{
    const struct vsr_io *io;

    if (replica == NULL || replica->io == NULL) {
        return false;
    }
    io = replica->io;
    return replica->index < io->options.limits.replicas &&
           replica == &io->replicas[replica->index];
}

/* A forwarded op that names the replica is still to reach the caller (a
 * RELEASE of one of its leases, say): the entry must outlive it. */
static bool forwarded_names(const struct vsr_io *io,
                            const struct vsr_io_replica *replica)
{
    uint32_t capacity = io->options.limits.ops;

    for (uint32_t i = 0; i < io->forwarded_count; ++i) {
        uint32_t at = (uint32_t)(((uint64_t)io->forwarded_head + i) % capacity);

        if (io->forwarded[at].op.replica == replica) {
            return true;
        }
    }
    return false;
}

/* Write-behind bytes packed but not written yet: the next prepare issues
 * them, so detach waits unless the store no longer writes (fenced,
 * memory-only). */
static bool store_unwritten(const struct vsr_io_store *store)
{
    struct vsr_io_store_status status;

    if (store->state != VSR_IO_STORE_READY || store->error != 0) {
        return false;
    }
    vsr_io_store_status(store, &status);
    return status.unwritten_bytes > 0;
}

/* A queued event's engine lease (a MESSAGE not taken by the stopped core)
 * goes back with its slab reference. */
static void release_queued(struct vsr_io_replica *replica,
                           struct vsr_io_queued_event *ring, uint32_t *head,
                           uint32_t *count, uint32_t capacity)
{
    while (*count > 0) {
        struct vsr_io_queued_event *queued = &ring[*head];

        if (queued->lease != NONE &&
            replica->leases[queued->lease].state != VSR_IO_LEASE_STATE_FREE) {
            vsr_io_lease_release(replica, queued->lease);
        }
        *head = (*head + 1) % capacity;
        (*count)--;
    }
}

int vsr_io_detach(struct vsr_io_replica *replica)
{
    struct vsr_io *io;
    uint32_t file_slot;
    int rc;

    if (!replica_valid(replica) || replica->state == VSR_IO_REPLICA_FREE) {
        return VSR_EINVAL;
    }
    io = replica->io;
    if (replica->state != VSR_IO_REPLICA_STOPPED ||
        forwarded_names(io, replica) || store_unwritten(&replica->store) ||
        replica->deferred_count > 0) {
        return VSR_EBUSY;
    }
    rc = vsr_io_snapshots_close(io, replica->index);
    if (rc != VSR_OK) {
        return rc;
    }
    file_slot = replica->store.file_slot;
    rc = vsr_io_store_close(&replica->store);
    if (rc != VSR_OK) {
        return rc;
    }
    if (file_slot != NONE) {
        /* The log's slot (store-phase-1: read before the close clears it);
         * clearing an empty slot is harmless. */
        (void)vsr_io_engine_install(io, file_slot, -1);
        vsr_io_engine_slot_free(io, file_slot);
    }
    release_queued(replica, replica->messages, &replica->messages_head,
                   &replica->messages_count, replica->regions_count);
    release_queued(replica, replica->completions, &replica->completions_head,
                   &replica->completions_count,
                   replica->options.limits.operations);
    replica->priority_head = replica->priority_count = 0;
    replica->events_head = replica->events_count = 0;
    /* The stopped core holds no lease (vsr.h); the module released its
     * reserved ones at close. What is left was the engine's own. */
    for (uint32_t i = 0; i < replica->regions_count; ++i) {
        if (replica->leases[i].state != VSR_IO_LEASE_STATE_FREE) {
            vsr_io_lease_release(replica, i);
        }
    }
    (void)io->ex.ops->update_buffer(io->ex.ctx, replica->tail_region, NULL);
    vsr_io_deadlines_arm(&io->deadlines, replica->deadline_core,
                         VSR_NO_DEADLINE);
    vsr_io_deadlines_arm(&io->deadlines, replica->deadline_flush,
                         VSR_NO_DEADLINE);
    vsr_io_deadlines_arm(&io->deadlines, replica->deadline_sync,
                         VSR_NO_DEADLINE);
    vsr_io_deadlines_arm(&io->deadlines, replica->deadline_capture,
                         VSR_NO_DEADLINE);
    replica->state = VSR_IO_REPLICA_FREE;
    replica->stopping = 0;
    replica->status_pending = 0;
    REPLICA_ASSERT(io->replicas_count > 0);
    io->replicas_count--;
    return VSR_OK;
}

/* -------------------------------------------------------------------------
 * Accessors
 * ---------------------------------------------------------------------- */

void vsr_io_replica_status(const struct vsr_io_replica *replica,
                           struct vsr_status *core,
                           struct vsr_io_store_status *store)
{
    bool attached =
        replica_valid(replica) && replica->state != VSR_IO_REPLICA_FREE;

    if (core != NULL) {
        memset(core, 0, sizeof(*core));
        if (attached) {
            vsr_get_status(replica->core, core);
        }
    }
    if (store != NULL) {
        memset(store, 0, sizeof(*store));
        if (attached) {
            vsr_io_store_status(&replica->store, store);
        }
    }
}

struct vsr *vsr_io_replica_core(struct vsr_io_replica *replica)
{
    return replica_valid(replica) ? replica->core : NULL;
}

struct vsr_io_replica *
vsr_io_replica_find(struct vsr_io *io, struct vsr_id cluster, uint64_t replica)
{
    if (io == NULL) {
        return NULL;
    }
    for (uint32_t i = 0; i < io->options.limits.replicas; ++i) {
        struct vsr_io_replica *entry = &io->replicas[i];

        if (entry->state != VSR_IO_REPLICA_FREE &&
            entry->options.cluster.hi == cluster.hi &&
            entry->options.cluster.lo == cluster.lo &&
            entry->options.replica == replica) {
            return entry;
        }
    }
    return NULL;
}
