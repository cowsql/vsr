#include "config.h"

#include "io/engine.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

/*
 * Loop entry points (docs/io-implementation.md, section 7): vsr_io_complete
 * dispatches completion records by slot kind to the owning module (7.5),
 * vsr_io_poll steps every core with its queued events and routes the ops
 * of every update (7.1, 7.3), vsr_io_submit takes the caller's events
 * (7.2), vsr_io_prepare collects every module's records (7.4) and
 * vsr_io_run is the ready-made loop over the four.
 *
 * The step loop of one replica feeds, in this order, the internal
 * completions (store, SEND, snapshot), the caller's COMPLETE and STOP
 * events, the MESSAGEs, the caller's other events and TIME(now), all in
 * one vsr_step_many, and consumes the accepted prefix. An update's
 * capacity is bounded by the room left in the forwarded ring, so every op
 * of it can be routed at once (decision 129). A head event the core finds
 * INPUT_BLOCKED keeps its queue (MESSAGEs, or the caller's other events)
 * out of the rest of the poll, so TIME and the queues before it still
 * reach the core; an event the core refuses with EINVAL is dropped (the
 * caller's lease returned through a RELEASE op, a MESSAGE's engine lease
 * freed) and counted.
 */

#define NONE VSR_IO_INDEX_NONE

/* Step calls per replica and poll before the poll reports MORE instead:
 * the core's MORE drains are bounded, this only caps one poll. */
#define LOOP_STEPS_MAX 256u

/* Invariant checks in debug builds; a violation traps (see pool.c). */
#ifdef NDEBUG
#define LOOP_ASSERT(condition) ((void)sizeof(condition))
#else
#define LOOP_ASSERT(condition) ((condition) ? (void)0 : __builtin_trap())
#endif

/* The segments of a step's event array, in feeding order. */
enum segment {
    SEG_INTERNAL,
    SEG_PRIORITY,
    SEG_MESSAGE,
    SEG_NORMAL,
    SEG_TIME,
    SEG_COUNT
};

struct step_build {
    uint32_t counts[SEG_COUNT];
    uint32_t total;
};

static uint32_t ring_at(uint32_t head, uint32_t offset, uint32_t capacity)
{
    return (uint32_t)(((uint64_t)head + offset) % capacity);
}

static bool replica_attached(const struct vsr_io *io,
                             const struct vsr_io_replica *replica)
{
    return replica != NULL && replica->io == io &&
           replica->index < io->options.limits.replicas &&
           replica == &io->replicas[replica->index] &&
           replica->state != VSR_IO_REPLICA_FREE;
}

/* -------------------------------------------------------------------------
 * vsr_io_complete (7.5)
 * ---------------------------------------------------------------------- */

static bool replica_owner(const struct vsr_io *io, uint32_t owner)
{
    return owner < io->options.limits.replicas &&
           io->replicas[owner].state != VSR_IO_REPLICA_FREE;
}

static void complete_one(struct vsr_io *io, const struct vsr_io_cqe *cqe)
{
    struct vsr_io_slot *slot;
    uint32_t index;

    /* The PROVIDE record completes only when it failed, and holds no slot:
     * the ring is sized for every slab, so that is fatal (decision 61). */
    if (cqe->user_data == vsr_io_engine_provide_user_data(io)) {
        if (cqe->result < 0 && io->stats.failure == 0) {
            io->stats.failure = cqe->result;
        }
        return;
    }
    /* A foreign owner tag, a stale generation or a freed slot: dropped and
     * counted in the slot table (decision 74's module never sees it). */
    slot = vsr_io_slots_resolve(&io->slots, cqe->user_data, &index);
    if (slot == NULL) {
        return;
    }
    switch ((enum vsr_io_slot_kind)slot->kind) {
    case VSR_IO_SLOT_LISTEN:
    case VSR_IO_SLOT_CONNECT:
    case VSR_IO_SLOT_RECV:
    case VSR_IO_SLOT_SEND:
    case VSR_IO_SLOT_SHUTDOWN:
        vsr_io_links_complete(io, index, cqe);
        return;
    case VSR_IO_SLOT_WRITE:
    case VSR_IO_SLOT_FLUSH:
    case VSR_IO_SLOT_SUPER:
    case VSR_IO_SLOT_LOAD:
    case VSR_IO_SLOT_FILE:
        if (replica_owner(io, slot->owner)) {
            vsr_io_store_complete(io, slot->owner, index, cqe);
            return;
        }
        break;
    case VSR_IO_SLOT_CLIENTS:
        if (replica_owner(io, slot->owner)) {
            vsr_io_snapshots_complete(io, slot->owner, index, cqe);
            return;
        }
        break;
    case VSR_IO_SLOT_STREAM:
        vsr_io_streams_complete(io, index, cqe);
        return;
    case VSR_IO_SLOT_FILES:
        vsr_io_engine_files_complete(io, index, cqe);
        return;
    case VSR_IO_SLOT_FREE:
    case VSR_IO_SLOT_PEEK:
    case VSR_IO_SLOT_PROVIDE:
    case VSR_IO_SLOT_KINDS:
    default:
        break;
    }
    /* A kind the engine never issues, or a record of a detached replica
     * (detach waits for every record, so neither happens): consumed. */
    vsr_io_slots_consumed(&io->slots, index,
                          (cqe->flags & VSR_IO_CQE_MORE) != 0);
}

int vsr_io_complete(struct vsr_io *io, const struct vsr_io_cqe *cqes,
                    uint32_t count)
{
    if (io == NULL || (cqes == NULL && count > 0)) {
        return VSR_EINVAL;
    }
    /* A completion carries no clock: what the modules arm now counts from
     * io->now, the last poll's time, and moves with the next poll's
     * (decision G2). */
    io->deadlines.rebasing = 1;
    for (uint32_t i = 0; i < count; ++i) {
        complete_one(io, &cqes[i]);
    }
    io->deadlines.rebasing = 0;
    vsr_io_engine_check_closed(io);
    return VSR_OK;
}

/* -------------------------------------------------------------------------
 * Op routing (7.3)
 * ---------------------------------------------------------------------- */

/* A completion decided at once is fed to the core the replica's retry_ns
 * later (decision 129): the core re-sends at once on a failed SEND (its
 * peer is marked for a heartbeat), so feeding a refused SEND's RETRY in
 * the same poll turned an unauthorized destination into a hot loop; the
 * delay makes it one attempt per retry_ns, the cadence the core retries
 * its protocol work at anyway. */
static void complete_now(struct vsr_io_replica *replica, uint64_t op,
                         int32_t status)
{
    vsr_io_engine_complete_later(replica, op, status);
}

/* Moves the deferred completions that are due into the internal ring. */
static void release_deferred(struct vsr_io_replica *replica, uint64_t now)
{
    uint32_t capacity = replica->options.limits.operations;

    while (replica->deferred_count > 0) {
        const struct vsr_io_deferred *deferred =
            &replica->deferred[replica->deferred_head];
        struct vsr_io_completion completion;

        if (deferred->due > now) {
            return;
        }
        completion.op = deferred->op;
        completion.status = deferred->status;
        completion.lease = NONE;
        completion.data = NULL;
        vsr_io_engine_complete_core(replica, &completion);
        replica->deferred_head = (replica->deferred_head + 1) % capacity;
        replica->deferred_count--;
    }
}

/* Forwarded verbatim: the core's pin rules hold, the engine never copies
 * a forwarded op's data. The caller of the routing guaranteed the room. */
static void forward_core(struct vsr_io_replica *replica,
                         const struct vsr_op *op)
{
    struct vsr_io_forwarded *entry =
        vsr_io_forward(replica->io, replica, VSR_IO_OP_CORE);

    LOOP_ASSERT(entry != NULL);
    if (entry != NULL) {
        entry->op.op = *op;
    }
}

static void route_load(struct vsr_io_replica *replica, const struct vsr_op *op)
{
    struct vsr_io_store *store = &replica->store;
    const struct vsr_store_read *read = op->data;

    if (read == NULL) {
        complete_now(replica, op->id, VSR_IO_FAILED);
        return;
    }
    if (read->type == VSR_LOAD_RECOVERY) {
        /* The core's first op (7.7): the store opens or creates the log
         * and answers it; a second one would find the store open. */
        if (store->state != VSR_IO_STORE_CLOSED) {
            complete_now(replica, op->id, VSR_IO_FAILED);
            return;
        }
        vsr_io_store_open(store, replica->options.start_mode, op->id, read);
        return;
    }
    if (vsr_io_store_load(store, op->id, read) != VSR_OK) {
        complete_now(replica, op->id, VSR_IO_FAILED);
    }
}

static void route_store(struct vsr_io_replica *replica, const struct vsr_op *op)
{
    int rc;

    switch (op->type) {
    case VSR_OP_STORE:
        rc = vsr_io_store_store(&replica->store, op->id, op->data);
        break;
    case VSR_OP_SYNC:
        rc = vsr_io_store_sync(&replica->store, op->id, op->arg);
        break;
    case VSR_OP_RECLAIM:
    default:
        rc = vsr_io_store_reclaim(&replica->store, op->id, op->arg);
        break;
    }
    if (rc != VSR_OK) {
        /* A malformed op (a core bug): completed, never left hanging. */
        complete_now(replica, op->id, VSR_IO_FAILED);
    }
}

static void route_snapshot(struct vsr_io_replica *replica,
                           const struct vsr_op *op)
{
    struct vsr_io *io = replica->io;
    const struct vsr_snapshot_task *task = op->data;
    int rc;

    switch (op->type) {
    case VSR_OP_SNAPSHOT_CAPTURE:
        rc = vsr_io_snapshots_capture(io, replica->index, op->id, task);
        break;
    case VSR_OP_SNAPSHOT_FETCH:
        rc = vsr_io_snapshots_fetch(io, replica->index, op->id, task);
        break;
    case VSR_OP_SNAPSHOT_SYNC:
        rc = vsr_io_snapshots_sync(io, replica->index, op->id, task);
        break;
    case VSR_OP_SNAPSHOT_DROP:
    default:
        rc = vsr_io_snapshots_drop(io, replica->index, op->id, task);
        break;
    }
    if (rc == VSR_OK) {
        return; /* Taken: forwarded from the module's poll. */
    }
    /* A status is a completion at once (no data, no lease); EINVAL a
     * malformed op. */
    complete_now(replica, op->id, rc == VSR_EINVAL ? VSR_IO_FAILED : rc);
}

/* RELEASE of an engine lease: the region, the slab reference or the ring
 * pin go back (7.6); a stale id is counted and ignored. A caller lease is
 * the caller's. */
static void route_release(struct vsr_io_replica *replica,
                          const struct vsr_op *op)
{
    struct vsr_io *io = replica->io;
    struct vsr_io_replica *owner = NULL;
    uint32_t index = NONE;

    if ((op->arg & VSR_IO_LEASE_ENGINE) == 0) {
        forward_core(replica, op);
        return;
    }
    if (vsr_io_lease_resolve(io, op->arg, &owner, &index) != VSR_OK ||
        owner->leases[index].state != VSR_IO_LEASE_STATE_LEASED) {
        io->releases_rejected++;
        return;
    }
    if (owner->leases[index].pin != NONE) {
        vsr_io_store_release(&owner->store, index);
    }
    vsr_io_lease_release(owner, index);
}

/* A REPLY other than OK ends the admission of an incarnation the store
 * does not index (decision 44): replied clears the in-flight flag and
 * forgets an entry with nothing else in it. */
static void route_reply(struct vsr_io_replica *replica, const struct vsr_op *op)
{
    const struct vsr_reply *reply = op->data;

    forward_core(replica, op);
    if (reply != NULL && reply->status != VSR_REPLY_OK) {
        vsr_io_store_replied(&replica->store, reply->request.client);
    }
}

static void route_one(struct vsr_io_replica *replica, const struct vsr_op *op)
{
    struct vsr_io *io = replica->io;

    switch ((enum vsr_op_type)op->type) {
    case VSR_OP_SEND:
        /* RETRY means complete at once; OK completes later through the
         * replica's completion ring (decision 83). */
        if (vsr_io_links_send(io, replica->index, op->id, op->data, op->arg) ==
            VSR_IO_RETRY) {
            complete_now(replica, op->id, VSR_IO_RETRY);
        }
        return;
    case VSR_OP_LOAD:
        route_load(replica, op);
        return;
    case VSR_OP_STORE:
    case VSR_OP_SYNC:
    case VSR_OP_RECLAIM:
        route_store(replica, op);
        return;
    case VSR_OP_SNAPSHOT_CAPTURE:
    case VSR_OP_SNAPSHOT_FETCH:
    case VSR_OP_SNAPSHOT_SYNC:
    case VSR_OP_SNAPSHOT_DROP:
        route_snapshot(replica, op);
        return;
    case VSR_OP_RELEASE:
        route_release(replica, op);
        return;
    case VSR_OP_REPLY:
        route_reply(replica, op);
        return;
    case VSR_OP_APPLY:
    case VSR_OP_READ_READY:
    case VSR_OP_SNAPSHOT_INSTALL:
    default:
        forward_core(replica, op);
        return;
    }
}

/* Decision 51: every LOAD, then every STORE, then SYNC and RECLAIM, then
 * the rest in emission order. A LOAD is answered against the indexes as
 * they were before the update's STOREs; a CAPTURE snapshots the client
 * table after them. */
static uint32_t route_pass(uint32_t type)
{
    switch (type) {
    case VSR_OP_LOAD:
        return 0;
    case VSR_OP_STORE:
        return 1;
    case VSR_OP_SYNC:
    case VSR_OP_RECLAIM:
        return 2;
    default:
        return 3;
    }
}

void vsr_io_engine_route(struct vsr_io_replica *replica,
                         const struct vsr_op *ops, uint32_t count)
{
    for (uint32_t pass = 0; pass < 4; ++pass) {
        for (uint32_t i = 0; i < count; ++i) {
            if (route_pass(ops[i].type) == pass) {
                route_one(replica, &ops[i]);
            }
        }
    }
}

/* -------------------------------------------------------------------------
 * The step loop (7.1)
 * ---------------------------------------------------------------------- */

/* The store's completions join the replica's internal ring, which holds one
 * entry per outstanding core op: the store's queue never waits for a step. */
static void drain_store(struct vsr_io_replica *replica)
{
    struct vsr_io_completion completion;

    while (vsr_io_store_next_completion(&replica->store, &completion)) {
        vsr_io_engine_complete_core(replica, &completion);
    }
}

static uint32_t copy_ring(struct vsr_event *out,
                          const struct vsr_io_queued_event *ring, uint32_t head,
                          uint32_t count, uint32_t capacity)
{
    for (uint32_t i = 0; i < count; ++i) {
        out[i] = ring[ring_at(head, i, capacity)].event;
    }
    return count;
}

static uint32_t build(struct vsr_io_replica *replica, uint64_t now,
                      bool skip_messages, bool skip_normal, bool time,
                      struct step_build *b)
{
    struct vsr_event *events = replica->step_events;
    uint32_t n = 0;

    memset(b, 0, sizeof(*b));
    b->counts[SEG_INTERNAL] = copy_ring(
        events + n, replica->completions, replica->completions_head,
        replica->completions_count, replica->options.limits.operations);
    n += b->counts[SEG_INTERNAL];
    b->counts[SEG_PRIORITY] =
        copy_ring(events + n, replica->priority, replica->priority_head,
                  replica->priority_count, replica->priority_capacity);
    n += b->counts[SEG_PRIORITY];
    if (!skip_messages) {
        b->counts[SEG_MESSAGE] =
            copy_ring(events + n, replica->messages, replica->messages_head,
                      replica->messages_count, replica->regions_count);
        n += b->counts[SEG_MESSAGE];
    }
    if (!skip_normal) {
        b->counts[SEG_NORMAL] = copy_ring(
            events + n, replica->events, replica->events_head,
            replica->events_count, replica->io->options.limits.events);
        n += b->counts[SEG_NORMAL];
    }
    if (time) {
        memset(&events[n], 0, sizeof(events[n]));
        events[n].type = VSR_EVENT_TIME;
        events[n].id = now;
        b->counts[SEG_TIME] = 1;
        n++;
    }
    LOOP_ASSERT(n <= replica->step_events_capacity);
    b->total = n;
    return n;
}

static enum segment segment_of(const struct step_build *b, uint32_t index)
{
    for (uint32_t s = 0; s < SEG_COUNT; ++s) {
        if (index < b->counts[s]) {
            return (enum segment)s;
        }
        index -= b->counts[s];
    }
    return SEG_COUNT;
}

/* Dequeues `take` accepted events of a ring; a queued engine lease (a
 * MESSAGE, a LOAD result, a snapshot checkpoint) is the core's now. */
static void accept_ring(struct vsr_io_replica *replica,
                        const struct vsr_io_queued_event *ring, uint32_t *head,
                        uint32_t *count, uint32_t capacity, uint32_t take)
{
    for (uint32_t i = 0; i < take; ++i) {
        const struct vsr_io_queued_event *queued = &ring[*head];

        if (queued->lease != NONE) {
            replica->leases[queued->lease].state = VSR_IO_LEASE_STATE_LEASED;
        }
        *head = (*head + 1) % capacity;
        (*count)--;
    }
}

static uint32_t take_of(uint32_t *left, uint32_t count)
{
    uint32_t take = *left < count ? *left : count;

    *left -= take;
    return take;
}

/* Consumes the accepted prefix; true when TIME was part of it. */
static bool accept_prefix(struct vsr_io_replica *replica,
                          const struct step_build *b, uint32_t consumed)
{
    uint32_t left = consumed;

    accept_ring(replica, replica->completions, &replica->completions_head,
                &replica->completions_count, replica->options.limits.operations,
                take_of(&left, b->counts[SEG_INTERNAL]));
    accept_ring(replica, replica->priority, &replica->priority_head,
                &replica->priority_count, replica->priority_capacity,
                take_of(&left, b->counts[SEG_PRIORITY]));
    accept_ring(replica, replica->messages, &replica->messages_head,
                &replica->messages_count, replica->regions_count,
                take_of(&left, b->counts[SEG_MESSAGE]));
    accept_ring(replica, replica->events, &replica->events_head,
                &replica->events_count, replica->io->options.limits.events,
                take_of(&left, b->counts[SEG_NORMAL]));
    return take_of(&left, b->counts[SEG_TIME]) > 0;
}

static void drop_lease(struct vsr_io_replica *replica, uint32_t lease)
{
    if (lease == NONE ||
        replica->leases[lease].state == VSR_IO_LEASE_STATE_FREE) {
        return;
    }
    if (replica->leases[lease].pin != NONE) {
        vsr_io_store_release(&replica->store, lease);
    }
    vsr_io_lease_release(replica, lease);
}

/* The head event of `segment` was refused with EINVAL: dropped. The
 * caller's lease goes back through a RELEASE op as one from the core
 * would; false (the event stays) when the forwarded ring has no room for
 * it. */
static bool drop_invalid(struct vsr_io_replica *replica, enum segment segment)
{
    struct vsr_io *io = replica->io;
    struct vsr_io_queued_event *queued;

    switch (segment) {
    case SEG_INTERNAL:
        /* A completion the core does not know: an engine or module bug. */
        queued = &replica->completions[replica->completions_head];
        drop_lease(replica, queued->lease);
        replica->completions_head = (replica->completions_head + 1) %
                                    replica->options.limits.operations;
        replica->completions_count--;
        break;
    case SEG_MESSAGE:
        queued = &replica->messages[replica->messages_head];
        drop_lease(replica, queued->lease);
        replica->messages_head =
            (replica->messages_head + 1) % replica->regions_count;
        replica->messages_count--;
        io->stats.frames_rejected++;
        break;
    case SEG_PRIORITY:
    case SEG_NORMAL:
        queued = segment == SEG_PRIORITY
                     ? &replica->priority[replica->priority_head]
                     : &replica->events[replica->events_head];
        if (queued->event.lease != 0) {
            struct vsr_io_forwarded *entry =
                vsr_io_forward(io, replica, VSR_IO_OP_CORE);

            if (entry == NULL) {
                return false;
            }
            entry->op.op.type = VSR_OP_RELEASE;
            entry->op.op.arg = queued->event.lease;
        }
        if (segment == SEG_PRIORITY) {
            replica->priority_head =
                (replica->priority_head + 1) % replica->priority_capacity;
            replica->priority_count--;
        } else {
            replica->events_head =
                (replica->events_head + 1) % io->options.limits.events;
            replica->events_count--;
        }
        break;
    case SEG_TIME:
    case SEG_COUNT:
    default:
        return true;
    }
    replica->rejected++;
    io->events_rejected++;
    return true;
}

/* Steps the replica's core until it has nothing runnable; true when the
 * poll must report MORE (the forwarded ring filled, or the step bound). */
static bool replica_step(struct vsr_io *io, struct vsr_io_replica *replica,
                         uint64_t now)
{
    bool time_done = false;
    bool skip_messages = false;
    bool skip_normal = false;
    bool more = false;

    for (uint32_t steps = 0;; ++steps) {
        struct step_build b;
        struct vsr_update update;
        uint32_t room;
        uint32_t n;
        uint32_t consumed;
        bool progress;
        int rc;

        drain_store(replica);
        room = io->options.limits.ops - io->forwarded_count;
        if (room == 0) {
            io->forwarded_overflow = 1;
            return true;
        }
        if (steps == LOOP_STEPS_MAX) {
            return true;
        }
        n = build(replica, now, skip_messages, skip_normal, !time_done, &b);
        if (n == 0 && !more) {
            return false;
        }
        memset(&update, 0, sizeof(update));
        update.ops = replica->step_ops;
        update.capacity =
            replica->step_capacity < room ? replica->step_capacity : room;
        rc = vsr_step_many(replica->core, n > 0 ? replica->step_events : NULL,
                           n, &update);
        consumed = update.consumed <= n ? update.consumed : n;
        if (accept_prefix(replica, &b, consumed)) {
            time_done = true;
        }
        LOOP_ASSERT(update.count <= update.capacity);
        vsr_io_engine_route(replica, update.ops, update.count);
        if ((update.flags & VSR_UPDATE_STATE_CHANGED) != 0) {
            replica->status_pending = 1;
        }
        replica->core_deadline = update.deadline_ns;
        vsr_io_deadlines_arm(&io->deadlines, replica->deadline_core,
                             update.deadline_ns);
        progress = consumed > 0 || update.count > 0;
        if (rc < 0 && consumed < n) {
            enum segment segment = segment_of(&b, consumed);

            if (segment == SEG_TIME) {
                time_done = true; /* The clock never goes back (poll). */
            } else if (!drop_invalid(replica, segment)) {
                io->forwarded_overflow = 1;
                return true;
            }
            progress = true;
        } else if ((update.flags & VSR_UPDATE_INPUT_BLOCKED) != 0 &&
                   consumed < n) {
            /* The head event waits for resources the core frees as it
             * consumes completions: its queue sits out the rest of this
             * poll, the others (TIME included) go on. */
            switch (segment_of(&b, consumed)) {
            case SEG_MESSAGE:
                skip_messages = true;
                progress = true;
                break;
            case SEG_NORMAL:
                skip_normal = true;
                progress = true;
                break;
            case SEG_INTERNAL:
            case SEG_PRIORITY:
            case SEG_TIME:
            case SEG_COUNT:
            default:
                /* Completions are always admissible eventually (vsr.h):
                 * retried at the next poll. */
                return false;
            }
        }
        more = (update.flags & VSR_UPDATE_MORE) != 0;
        if (!progress && !more) {
            return false;
        }
    }
}

/* STATUS once per poll in which STATE_CHANGED was reported, and at
 * STOPPED (7.8); the status is copied into the ring entry (decision 130),
 * valid as long as any rail descriptor (decision 132). */
static bool emit_status(struct vsr_io *io, struct vsr_io_replica *replica)
{
    struct vsr_io_forwarded *entry;

    if (replica->status_pending == 0) {
        return false;
    }
    entry = vsr_io_forward(io, replica, VSR_IO_OP_STATUS);
    if (entry == NULL) {
        return true;
    }
    vsr_get_status(replica->core, &replica->status);
    entry->rail.status = replica->status;
    entry->op.op.data = &entry->rail.status;
    replica->status_pending = 0;
    if (replica->status.state == VSR_STATE_STOPPED) {
        replica->state = VSR_IO_REPLICA_STOPPED;
    } else if (replica->state == VSR_IO_REPLICA_OPENING) {
        replica->state = VSR_IO_REPLICA_RUNNING;
    }
    return false;
}

/* One replica's share of a poll. The store and snapshot polls run before
 * the step loop (held STOREs and base loads may complete) and after it
 * (ops just routed to them start at once: a CAPTURE forwards in the same
 * poll); completions they queue afterwards make the poll report MORE. */
static bool poll_replica(struct vsr_io *io, struct vsr_io_replica *replica,
                         uint64_t now)
{
    bool more = false;
    uint32_t before;

    vsr_io_store_poll(io, replica->index, now);
    vsr_io_snapshots_poll(io, replica->index, now);
    release_deferred(replica, now);
    if (replica->state == VSR_IO_REPLICA_OPENING ||
        replica->state == VSR_IO_REPLICA_RUNNING) {
        more = replica_step(io, replica, now);
    }
    before = replica->completions_count;
    vsr_io_store_poll(io, replica->index, now);
    vsr_io_snapshots_poll(io, replica->index, now);
    drain_store(replica);
    if (replica->completions_count > before &&
        replica->state != VSR_IO_REPLICA_STOPPED) {
        more = true;
    }
    if (emit_status(io, replica)) {
        more = true;
    }
    return more;
}

static void dispatch_deadlines(struct vsr_io *io, uint64_t now)
{
    uint16_t kind;
    uint32_t index;

    while (vsr_io_deadlines_pop(&io->deadlines, now, &kind, &index)) {
        switch ((enum vsr_io_deadline_kind)kind) {
        case VSR_IO_DEADLINE_LINK:
        case VSR_IO_DEADLINE_DIAL:
            vsr_io_links_deadline(io, kind, index, now);
            break;
        case VSR_IO_DEADLINE_STREAM:
            vsr_io_streams_deadline(io, index, now);
            break;
        case VSR_IO_DEADLINE_CORE:    /* TIME(now) below. */
        case VSR_IO_DEADLINE_FLUSH:   /* The store's poll checks its own. */
        case VSR_IO_DEADLINE_SYNC:    /* Likewise. */
        case VSR_IO_DEADLINE_CAPTURE: /* The module's poll retries. */
        case VSR_IO_DEADLINE_KINDS:
        default:
            break;
        }
    }
}

int vsr_io_poll(struct vsr_io *io, uint64_t now_ns, struct vsr_io_op *ops,
                uint32_t capacity, uint32_t *count, uint32_t *flags)
{
    uint32_t ring;
    uint32_t n;
    bool more = false;
    uint64_t now;

    if (count != NULL) {
        *count = 0;
    }
    if (flags != NULL) {
        *flags = 0;
    }
    if (io == NULL || count == NULL || flags == NULL ||
        (ops == NULL && capacity > 0) || now_ns >= VSR_NO_DEADLINE) {
        return VSR_EINVAL;
    }
    /* TIME ids never decrease (vsr.h), whatever the caller's clock does. */
    vsr_io_engine_advance(io, now_ns);
    now = io->now;
    (void)__atomic_exchange_n(&io->wake_pending, 0, __ATOMIC_ACQUIRE);
    io->forwarded_overflow = 0;
    dispatch_deadlines(io, now);
    vsr_io_links_poll(io, now);
    vsr_io_streams_poll(io, now);
    for (uint32_t i = 0; i < io->options.limits.replicas; ++i) {
        struct vsr_io_replica *replica = &io->replicas[i];

        if (replica->state != VSR_IO_REPLICA_FREE &&
            poll_replica(io, replica, now)) {
            more = true;
        }
    }
    ring = io->options.limits.ops;
    n = capacity < io->forwarded_count ? capacity : io->forwarded_count;
    for (uint32_t i = 0; i < n; ++i) {
        ops[i] = io->forwarded[io->forwarded_head].op;
        io->forwarded_head = (io->forwarded_head + 1) % ring;
        io->forwarded_count--;
    }
    *count = n;
    if (io->forwarded_count > 0) {
        /* Ops left in the ring: ops filled (n == capacity). */
        more = true;
        *flags |= VSR_IO_POLL_OUTPUT_FULL;
    }
    if (more || io->forwarded_overflow != 0) {
        *flags |= VSR_IO_POLL_MORE;
    }
    vsr_io_engine_check_closed(io);
    return VSR_OK;
}

/* -------------------------------------------------------------------------
 * vsr_io_submit (7.2)
 * ---------------------------------------------------------------------- */

static int queue_event(struct vsr_io_queued_event *ring, uint32_t head,
                       uint32_t *count, uint32_t capacity,
                       const struct vsr_io_event *event)
{
    struct vsr_io_queued_event *queued;

    if (*count == capacity) {
        return VSR_AGAIN;
    }
    queued = &ring[ring_at(head, *count, capacity)];
    memset(queued, 0, sizeof(*queued));
    queued->event = event->event;
    queued->lease = NONE;
    queued->kind = event->kind;
    (*count)++;
    return VSR_OK;
}

static bool id_zero(struct vsr_id id)
{
    return id.hi == 0 && id.lo == 0;
}

/* The caller completed an op the snapshot module forwarded: the module
 * copies the checkpoint during the call, then the caller's lease goes back
 * at once through a RELEASE op (so the ring must have room first). */
static int submit_forwarded_done(struct vsr_io *io,
                                 struct vsr_io_replica *replica,
                                 const struct vsr_event *event)
{
    if (event->lease != 0 && io->forwarded_count == io->options.limits.ops) {
        return VSR_AGAIN;
    }
    if (vsr_io_snapshots_forwarded_done(io, replica->index, event->id,
                                        event->status, event->data) != VSR_OK) {
        return VSR_EINVAL;
    }
    if (event->lease != 0) {
        struct vsr_io_forwarded *entry =
            vsr_io_forward(io, replica, VSR_IO_OP_CORE);

        LOOP_ASSERT(entry != NULL);
        if (entry != NULL) {
            entry->op.op.type = VSR_OP_RELEASE;
            entry->op.op.arg = event->lease;
        }
    }
    return VSR_OK;
}

static int submit_core(struct vsr_io *io, const struct vsr_io_event *wrapper)
{
    struct vsr_io_replica *replica = wrapper->replica;
    const struct vsr_event *event = &wrapper->event;
    const struct vsr_request *request;
    int rc;

    if (!replica_attached(io, replica) ||
        replica->state == VSR_IO_REPLICA_STOPPED ||
        (event->lease & VSR_IO_LEASE_ENGINE) != 0 ||
        (event->data == NULL) != (event->lease == 0)) {
        return VSR_EINVAL;
    }
    switch ((enum vsr_event_type)event->type) {
    case VSR_EVENT_COMPLETE:
        if (vsr_io_snapshots_owns(&replica->snapshots, event->id)) {
            return submit_forwarded_done(io, replica, event);
        }
        return queue_event(replica->priority, replica->priority_head,
                           &replica->priority_count, replica->priority_capacity,
                           wrapper);
    case VSR_EVENT_STOP:
        rc = queue_event(replica->priority, replica->priority_head,
                         &replica->priority_count, replica->priority_capacity,
                         wrapper);
        if (rc == VSR_OK) {
            replica->stopping = 1;
        }
        return rc;
    case VSR_EVENT_REQUEST:
        request = event->data;
        if (replica->stopping != 0 || request == NULL ||
            id_zero(request->id.client)) {
            return VSR_EINVAL;
        }
        if (replica->events_count == io->options.limits.events) {
            return VSR_AGAIN;
        }
        /* max_clients admission (decisions 27 and 44). */
        if (!vsr_io_store_admit(&replica->store, request->id.client)) {
            return VSR_ELIMIT;
        }
        return queue_event(replica->events, replica->events_head,
                           &replica->events_count, io->options.limits.events,
                           wrapper);
    case VSR_EVENT_CLIENT_QUERY:
    case VSR_EVENT_READ:
    case VSR_EVENT_CHECKPOINT:
        if (replica->stopping != 0) {
            return VSR_EINVAL;
        }
        return queue_event(replica->events, replica->events_head,
                           &replica->events_count, io->options.limits.events,
                           wrapper);
    case VSR_EVENT_TIME:
    case VSR_EVENT_MESSAGE:
    default:
        return VSR_EINVAL;
    }
}

/* A rail COMPLETE is routed by its op id alone (decision 93). */
static int submit_rail_complete(struct vsr_io *io,
                                const struct vsr_io_event *wrapper)
{
    const struct vsr_event *event = &wrapper->event;
    int rc;

    switch (vsr_io_streams_op_kind(event->id)) {
    case VSR_IO_STREAM_OP_SERVE:
        rc = vsr_io_streams_served(io, event->id, event->status);
        break;
    case VSR_IO_STREAM_OP_DATA:
        rc = vsr_io_streams_data_done(io, event->id);
        break;
    default:
        rc = vsr_io_links_handshake_done(io, event->id, event->status,
                                         event->data);
        break;
    }
    return rc == VSR_OK ? VSR_OK : VSR_EINVAL;
}

static int submit_one(struct vsr_io *io, const struct vsr_io_event *wrapper)
{
    const struct vsr_event *event = &wrapper->event;
    uint32_t index;

    switch ((enum vsr_io_event_kind)wrapper->kind) {
    case VSR_IO_EVENT_CORE:
        return submit_core(io, wrapper);
    case VSR_IO_EVENT_COMPLETE:
        return submit_rail_complete(io, wrapper);
    case VSR_IO_EVENT_STREAM_OPEN:
        if (event->data == NULL) {
            return VSR_EINVAL;
        }
        return vsr_io_streams_open(io, event->id, event->data, event->lease,
                                   VSR_IO_STREAM_CALLER, &index);
    case VSR_IO_EVENT_STREAM_WRITE:
        if (event->data == NULL) {
            return VSR_EINVAL;
        }
        return vsr_io_streams_write(io, event->data, event->lease);
    case VSR_IO_EVENT_STREAM_CLOSE:
        return vsr_io_streams_close(io, event->id, event->status);
    default:
        return VSR_EINVAL;
    }
}

int vsr_io_submit(struct vsr_io *io, const struct vsr_io_event *events,
                  uint32_t count, uint32_t *consumed)
{
    if (consumed == NULL) {
        return VSR_EINVAL;
    }
    *consumed = 0;
    if (io == NULL || (events == NULL && count > 0)) {
        return VSR_EINVAL;
    }
    for (uint32_t i = 0; i < count; ++i) {
        int rc = submit_one(io, &events[i]);

        if (rc != VSR_OK) {
            return rc;
        }
        (*consumed)++;
    }
    return VSR_OK;
}

/* -------------------------------------------------------------------------
 * vsr_io_prepare (7.4)
 * ---------------------------------------------------------------------- */

/* Pool provision (decision 133): the slabs above the provision floor go to
 * the ring through one PROVIDE record at the end of the batch, which the
 * executor runs before the batch's other records, so a receive armed or
 * re-armed in this batch already sees them. The pool counts them KERNEL
 * from here; the record's place in the batch is set aside first. */
static void provide_record(struct vsr_io *io, struct vsr_io_sqe *sqe,
                           uint32_t provided)
{
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode = VSR_IO_SQE_PROVIDE;
    sqe->fd = -1;
    sqe->buffer_group = io->pool.group;
    sqe->addr = io->provide_buffers;
    sqe->length = provided;
    sqe->user_data = vsr_io_engine_provide_user_data(io);
}

int vsr_io_prepare(struct vsr_io *io, uint64_t now_ns, struct vsr_io_sqe *sqes,
                   uint32_t capacity, uint32_t *count, uint64_t *deadline_ns)
{
    uint32_t provided;
    uint32_t room;

    if (count != NULL) {
        *count = 0;
    }
    if (deadline_ns != NULL) {
        *deadline_ns = VSR_NO_DEADLINE;
    }
    if (io == NULL || sqes == NULL || count == NULL || deadline_ns == NULL ||
        capacity < VSR_IO_ENGINE_BATCH_MIN || now_ns >= VSR_NO_DEADLINE) {
        return VSR_EINVAL;
    }
    vsr_io_engine_advance(io, now_ns);
    provided = vsr_io_pool_provide(&io->pool, io->provide_buffers,
                                   io->options.limits.slabs);
    room = provided > 0 ? capacity - 1 : capacity;
    vsr_io_links_prepare(io, sqes, room, count);
    vsr_io_streams_prepare(io, sqes, room, count);
    for (uint32_t i = 0; i < io->options.limits.replicas; ++i) {
        if (io->replicas[i].state == VSR_IO_REPLICA_FREE) {
            continue;
        }
        vsr_io_store_prepare(io, i, sqes, room, count);
        vsr_io_snapshots_prepare(io, i, sqes, room, count);
    }
    vsr_io_engine_prepare_files(io, sqes, room, count);
    if (provided > 0) {
        provide_record(io, &sqes[*count], provided);
        (*count)++;
    }
    LOOP_ASSERT(*count <= capacity);
    *deadline_ns = vsr_io_deadlines_earliest(&io->deadlines);
    for (uint32_t i = 0; i < io->options.limits.replicas; ++i) {
        const struct vsr_io_replica *replica = &io->replicas[i];

        /* Deferred completions are due in order: the head is the
         * earliest. */
        if (replica->state != VSR_IO_REPLICA_FREE &&
            replica->deferred_count > 0 &&
            replica->deferred[replica->deferred_head].due < *deadline_ns) {
            *deadline_ns = replica->deferred[replica->deferred_head].due;
        }
    }
    if (*count == capacity) {
        /* Records may be left over: the loop comes straight back. */
        *deadline_ns = io->now;
    }
    vsr_io_engine_check_closed(io);
    return VSR_OK;
}

/* -------------------------------------------------------------------------
 * vsr_io_run
 * ---------------------------------------------------------------------- */

/* Submits the pending events; an event the engine refuses for good (EINVAL,
 * ELIMIT) is dropped and counted, AGAIN keeps the rest for after the next
 * poll. Returns the events left, moved to the front. */
static uint32_t run_submit(struct vsr_io *io, uint32_t pending)
{
    struct vsr_io_event *events = io->run_events;
    uint32_t done = 0;

    while (done < pending) {
        uint32_t consumed = 0;
        int rc = vsr_io_submit(io, events + done, pending - done, &consumed);

        done += consumed;
        if (rc == VSR_OK || rc == VSR_AGAIN) {
            break;
        }
        io->events_rejected++;
        done++;
    }
    if (done > 0 && done < pending) {
        memmove(events, events + done,
                (size_t)(pending - done) * sizeof(*events));
    }
    return pending - done;
}

int vsr_io_run(struct vsr_io *io, const struct vsr_io_hooks *hooks)
{
    const struct vsr_io_executor *ex;
    uint32_t batch;
    uint32_t pending = 0;

    if (io == NULL) {
        return VSR_EINVAL;
    }
    ex = &io->ex;
    batch = io->options.limits.batch;
    for (;;) {
        uint64_t now = ex->ops->now(ex->ctx);
        uint32_t reaped = ex->ops->reap(ex->ctx, io->run_cqes, batch);
        uint32_t count = 0;
        uint64_t deadline = VSR_NO_DEADLINE;
        uint32_t want;
        int rc;

        for (uint32_t i = 0; i < reaped; ++i) {
            const struct vsr_io_cqe *cqe = &io->run_cqes[i];

            if (VSR_IO_OWNER(cqe->user_data) == io->options.owner) {
                (void)vsr_io_complete(io, cqe, 1);
            } else if (hooks != NULL && hooks->complete != NULL) {
                hooks->complete(hooks->ctx, cqe);
            }
        }
        for (;;) {
            uint32_t k = 0;
            uint32_t flags = 0;
            uint32_t m = 0;
            uint32_t left = pending;

            (void)vsr_io_poll(io, now, io->run_ops, io->options.limits.ops, &k,
                              &flags);
            if (hooks != NULL && hooks->step != NULL) {
                uint32_t room = io->run_events_capacity - pending;

                m = hooks->step(hooks->ctx, io->run_ops, k,
                                io->run_events + pending, room);
                if (m > room) {
                    m = room;
                }
            }
            pending = run_submit(io, pending + m);
            if (k == 0 && (m == 0 || pending == left + m) &&
                (flags & VSR_IO_POLL_MORE) == 0) {
                break;
            }
        }
        rc = vsr_io_prepare(io, now, io->run_sqes, batch, &count, &deadline);
        LOOP_ASSERT(rc == VSR_OK);
        if (hooks != NULL && hooks->prepare != NULL && count < batch) {
            uint32_t added = 0;
            uint64_t caller = hooks->prepare(hooks->ctx, io->run_sqes + count,
                                             batch - count, &added);

            count += added < batch - count ? added : batch - count;
            if (caller < deadline) {
                deadline = caller;
            }
        }
        if (pending > 0 && deadline > now) {
            deadline = now; /* Events wait for the next poll. */
        }
        if (io->stats.failure != 0) {
            return io->stats.failure;
        }
        if (io->state == VSR_IO_ENGINE_CLOSED && io->replicas_count == 0 &&
            count == 0) {
            return VSR_OK;
        }
        want = io->options.wait_min_complete > 0 ? io->options.wait_min_complete
                                                 : 1;
        rc = ex->ops->submit_and_wait(ex->ctx, io->run_sqes, count, want,
                                      io->options.wait_min_ns, deadline);
        if (rc < 0) {
            return rc;
        }
    }
}
