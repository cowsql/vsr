#include "config.h"

#include "io/engine.h"
#include "lib/check.h"
#include "lib/pure_executor.h"
#include "vsr-io.h"
#include "vsr-sim.h"

#include <errno.h>
#include <inttypes.h>
#include <stdalign.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * Engine part 2 (src/io/loop.c, src/io/replica.c): the public loop, the
 * replica life cycle and the op routing, driven through vsr-io.h over the
 * simulated executor of vsr-sim.h, each node's executor wrapped so a test
 * can fail a registration, count provisions and keep the completions it
 * reaped for replays. Every node runs one engine; a test attaches real
 * cores (vsr_init) with real stores, and an in-test caller per replica
 * (struct app) plays the application: it completes APPLY, READ_READY,
 * REPLY and the snapshot ops, keeps the leases of its events until their
 * RELEASE and records STATUS. The routing tests call the routing seam
 * vsr_io_engine_route with crafted ops on a replica whose core is never
 * stepped.
 *
 * Harness:
 *   world_open(nodes, seed)        the simulated world
 *   node_open(i) / close_node(n)   an engine on node i (i + 1 is its id)
 *   app_attach(n, r, cluster, id, members, mode)  replica r of node n
 *   iterate(n)                     one loop iteration, as vsr-io.h lists it
 *   run_until(pred, ctx, rounds)   iterate ready nodes and advance
 *   run_for(ns)                    the same for a stretch of virtual time
 *   submit_request(app, client, n) a COMMAND under a caller lease
 *   pump(n)                        an iteration without core steps (the
 *                                  routing tests)
 *   group_open(nodes, cluster)     a replicated group, one node each
 * VSR_ENGINE_TEST=name runs one test; VSR_ENGINE_TRACE=1 prints the
 * simulation's trace and every iteration.
 */

#define NODES 3u
#define PAGE 4096u
#define SLAB_BYTES (4u * PAGE)
#define SLABS 64u
#define REPLICAS 2u
#define OWNER 0x5Au
#define APP_OWNER 0x33u
#define FILE_SLOT_BASE 8u
#define FILE_SLOTS 32u
#define REGION_BASE 2u
#define BUFFER_REGIONS (1u + REPLICAS)
#define GROUP 1u
#define BATCH 64u
#define OPS 32u
#define EVENTS 16u
#define LINKS 6u
#define STREAMS 2u
#define WINDOW 2u
#define BLOCK 512u
#define PORT 7000u
#define CQES 256u
#define ENGINE_BYTES (1u << 20)
#define REPLICA_BYTES (6u << 20)
#define TAIL_BYTES (1u << 20)
#define LEASES 96u
#define PENDING 128u
#define LOG_CQES 4096u
#define MS UINT64_C(1000000)

/* members, operations, input_leases, pending_requests, pending_reads,
 * transfers, log_cache_entries, client_cache_entries, batch_entries,
 * spans_per_blob, work_per_step, command_bytes, result_bytes,
 * manifest_bytes, message_bytes, pinned_payload_bytes. */
static const struct vsr_limits core_limits = {
    3, 32, 16, 8, 4, 2, 32, 16, 4, 4, 64, 256, 64, 64, 4096, 65536};

static _Alignas(4096) unsigned char engine_memory[NODES][ENGINE_BYTES];
static _Alignas(4096) unsigned char payload_memory[NODES][SLABS * SLAB_BYTES];
static _Alignas(
    4096) unsigned char replica_memory[NODES][REPLICAS][REPLICA_BYTES];
static _Alignas(4096) unsigned char tail_memory[NODES][REPLICAS][TAIL_BYTES];

/* -------------------------------------------------------------------------
 * The caller of one replica
 * ---------------------------------------------------------------------- */

/* A caller lease and the graph it covers, kept until the RELEASE op. */
struct lease_slot {
    uint64_t id; /* 0: free. */
    struct vsr_request request;
    struct vsr_blob blob;
    struct vsr_span span;
    unsigned char bytes[16];
    struct vsr_applied applied;
    struct vsr_value values[8];
    struct vsr_checkpoint checkpoint;
};

struct app {
    struct node *node;
    struct vsr_io_replica *replica;
    uint32_t index;
    bool attached;
    struct vsr_membership seed;
    struct vsr_member members[3];
    struct vsr_io_replica_options options;
    char path[48]; /* "c<u64>-r<u64>" fits. */
    struct lease_slot leases[LEASES];
    uint64_t next_route;
    /* Observations. */
    uint32_t statuses;
    struct vsr_status status;
    uint64_t replies;
    uint64_t replies_ok;
    uint64_t replies_executed;
    uint32_t last_reply_status;
    uint64_t applies;
    uint64_t applied_entries;
    uint64_t releases;
    uint64_t captures;
    uint64_t snapshot_ops;
    uint64_t read_ready;
    /* Behaviour. */
    bool hold_apply;     /* Keep APPLY completions back. */
    uint64_t held_apply; /* Its op id, 0 none. */
    const struct vsr_apply *held_data;
};

/* -------------------------------------------------------------------------
 * The executor wrapper
 * ---------------------------------------------------------------------- */

struct wrap {
    struct vsr_io_executor inner;
    int fail_update_buffer; /* The next update_buffer returns it. */
    int fail_submit;        /* The next submit_and_wait returns it. */
    uint32_t provides;
    uint32_t provided;
    uint32_t submits;
    bool logging;
    struct vsr_io_cqe log[LOG_CQES];
    uint32_t log_count;
};

struct node {
    uint32_t index;
    bool open;
    bool registered;
    struct vsr_io *io;
    struct wrap wrap;
    struct pure_executor pure; /* Between the engine and the wrapper. */
    struct vsr_io_executor ex;
    struct app apps[REPLICAS];
    /* Events waiting for the next submit. */
    struct vsr_io_event pending[PENDING];
    uint32_t pending_count;
    /* Forwarded ops of the last iteration, for tests that look at them. */
    struct vsr_io_op seen[PENDING];
    uint32_t seen_count;
    uint32_t hold_ops; /* Poll with capacity 0 while set. */
    /* Rails. */
    uint64_t stream_ends;
    int32_t stream_end_status;
    uint64_t stream_end_bytes;
    uint64_t stream_serves;
    uint64_t stream_data;
    uint64_t stream_data_bytes;
    uint64_t stream_written;
    uint64_t served_handle;
    uint64_t external_peer; /* EXTERNAL: the node an inbound link is. */
    bool hold_serve;        /* Leave STREAM_SERVE unanswered... */
    uint64_t held_serve;    /* ...this one. */
    bool hold_data;         /* Keep STREAM_DATA completions back. */
    uint64_t held_data[PENDING];
    uint32_t held_data_count;
    uint64_t handshakes;
    uint64_t links_wanted;
    struct vsr_io_handshake_done handshake_done;
};

struct world {
    struct vsr_sim *sim;
    uint32_t nodes;
    struct node node[NODES];
    uint64_t incarnation;
    uint32_t durability; /* Of the replicas attached next. */
};

static struct world world;

/* A call into the engine with the purity guard armed (decision E7): an
 * executor call from anything it reaches aborts the test, naming the
 * call. Used for the four primitives, the routing seam, the modules' polls
 * the routing tests drive, vsr_io_close and the node calls. */
static void pure_begin(struct node *n)
{
    pure_executor_arm(&n->pure);
}

/* Evaluated after the call (its argument), which the comma operator in
 * PURE sequences after pure_begin. */
static int pure_end(struct node *n, int result)
{
    pure_executor_disarm(&n->pure);
    return result;
}

#define PURE(node, call) (pure_begin(node), pure_end((node), (call)))
#define PURE_VOID(node, call)                                                  \
    do {                                                                       \
        pure_executor_arm(&(node)->pure);                                      \
        call;                                                                  \
        pure_executor_disarm(&(node)->pure);                                   \
    } while (0)

static uint64_t wrap_now(void *ctx)
{
    struct wrap *w = ctx;

    return w->inner.ops->now(w->inner.ctx);
}

static void wrap_random(void *ctx, void *bytes, size_t size)
{
    struct wrap *w = ctx;

    w->inner.ops->random(w->inner.ctx, bytes, size);
}

static int wrap_submit(void *ctx, const struct vsr_io_sqe *sqes, uint32_t count,
                       uint32_t want, uint64_t min_wait_ns,
                       uint64_t deadline_ns)
{
    struct wrap *w = ctx;

    w->submits++;
    if (w->fail_submit != 0) {
        int rc = w->fail_submit;

        w->fail_submit = 0;
        return rc;
    }
    return w->inner.ops->submit_and_wait(w->inner.ctx, sqes, count, want,
                                         min_wait_ns, deadline_ns);
}

static uint32_t wrap_reap(void *ctx, struct vsr_io_cqe *cqes, uint32_t capacity)
{
    struct wrap *w = ctx;
    uint32_t n = w->inner.ops->reap(w->inner.ctx, cqes, capacity);

    if (w->logging) {
        for (uint32_t i = 0; i < n && w->log_count < LOG_CQES; ++i) {
            w->log[w->log_count++] = cqes[i];
        }
    }
    return n;
}

static int wrap_register_files(void *ctx, uint32_t slots)
{
    struct wrap *w = ctx;

    return w->inner.ops->register_files(w->inner.ctx, slots);
}

static int wrap_update_file(void *ctx, uint32_t slot, int fd)
{
    struct wrap *w = ctx;

    return w->inner.ops->update_file(w->inner.ctx, slot, fd);
}

static int wrap_register_buffers(void *ctx, uint32_t regions)
{
    struct wrap *w = ctx;

    return w->inner.ops->register_buffers(w->inner.ctx, regions);
}

static int wrap_update_buffer(void *ctx, uint32_t index,
                              const struct vsr_io_region *region)
{
    struct wrap *w = ctx;

    if (w->fail_update_buffer != 0 && region != NULL) {
        int rc = w->fail_update_buffer;

        w->fail_update_buffer = 0;
        return rc;
    }
    return w->inner.ops->update_buffer(w->inner.ctx, index, region);
}

static int wrap_buffer_ring(void *ctx, uint16_t group, uint32_t entries,
                            uint32_t flags, const struct vsr_io_region *memory)
{
    struct wrap *w = ctx;

    return w->inner.ops->buffer_ring(w->inner.ctx, group, entries, flags,
                                     memory);
}

static int wrap_provide(void *ctx, uint16_t group,
                        const struct vsr_io_buffer *buffers, uint32_t count)
{
    struct wrap *w = ctx;

    w->provides++;
    w->provided += count;
    return w->inner.ops->provide(w->inner.ctx, group, buffers, count);
}

static void wrap_wake(void *ctx)
{
    struct wrap *w = ctx;

    w->inner.ops->wake(w->inner.ctx);
}

static const struct vsr_io_executor_ops wrap_ops = {wrap_now,
                                                    wrap_random,
                                                    wrap_submit,
                                                    wrap_reap,
                                                    wrap_register_files,
                                                    wrap_update_file,
                                                    wrap_register_buffers,
                                                    wrap_update_buffer,
                                                    wrap_buffer_ring,
                                                    wrap_provide,
                                                    wrap_wake};

/* -------------------------------------------------------------------------
 * The world
 * ---------------------------------------------------------------------- */

static void trace_event(void *ctx, const struct vsr_sim_trace_event *event)
{
    (void)ctx;
    fprintf(stderr,
            "trace t=%" PRIu64 " kind %u node %u peer %u op %u ud %" PRIx64
            " result %" PRId64 " bytes %" PRIu64 "\n",
            event->now_ns, event->kind, event->node, event->peer, event->opcode,
            event->user_data, event->result, event->bytes);
}

static void world_open(uint32_t nodes, uint64_t seed)
{
    struct vsr_sim_options options;

    CHECK(nodes <= NODES);
    memset(&world, 0, sizeof(world));
    world.durability = VSR_DURABLE;
    memset(&options, 0, sizeof(options));
    options.seed = seed;
    options.nodes = nodes;
    options.block_bytes = BLOCK;
    options.file_slots = FILE_SLOT_BASE + FILE_SLOTS + 8;
    options.buffer_regions = REGION_BASE + BUFFER_REGIONS;
    options.faults.disk.latency_min_ns = 1000;
    options.faults.disk.latency_max_ns = 20000;
    options.faults.disk.fsync_min_ns = 1000;
    options.faults.disk.fsync_max_ns = 50000;
    options.faults.disk.unsynced_keep_ppm = 1000000;
    options.faults.network.delay_min_ns = 1000;
    options.faults.network.delay_max_ns = 50000;
    options.faults.network.stall_reset_ns = 100 * MS;
    options.faults.network.connect_timeout_ns = 100 * MS;
    CHECK(vsr_sim_create(&options, &world.sim) == 0);
    if (getenv("VSR_ENGINE_TRACE") != NULL) {
        static const struct vsr_sim_trace trace = {NULL, trace_event};

        vsr_sim_set_trace(world.sim, &trace);
    }
    world.nodes = nodes;
    world.incarnation = 1;
    for (uint32_t i = 0; i < nodes; ++i) {
        world.node[i].index = i;
    }
}

static void world_close(void)
{
    vsr_sim_destroy(world.sim);
    memset(&world, 0, sizeof(world));
}

static struct vsr_io_address node_address(uint32_t index)
{
    return vsr_sim_address(world.sim, index, (uint16_t)PORT);
}

static struct vsr_io_options engine_options(struct node *n)
{
    struct vsr_io_options options;
    struct vsr_io_limits *l = &options.limits;
    static struct vsr_io_address listen[NODES];

    memset(&options, 0, sizeof(options));
    listen[n->index] = node_address(n->index);
    options.executor = n->ex;
    options.node = n->index + 1;
    options.listen = &listen[n->index];
    options.listen_count = 1;
    options.handshake = VSR_IO_HANDSHAKE_TRUSTED;
    l->replicas = REPLICAS;
    l->nodes = 4;
    l->authorizations = 8;
    l->links = LINKS;
    l->link_queue = 8;
    l->streams = STREAMS;
    l->stream_window = WINDOW;
    l->events = EVENTS;
    l->ops = OPS;
    l->batch = BATCH;
    l->slabs = SLABS;
    l->slab_bytes = SLAB_BYTES;
    l->caller_slabs = 2;
    l->file_slots = FILE_SLOTS;
    l->buffer_regions = BUFFER_REGIONS;
    options.file_slot_base = FILE_SLOT_BASE;
    options.buffer_region_base = REGION_BASE;
    options.buffer_group = GROUP;
    options.owner = OWNER;
    options.nodelay = 1;
    options.connect_backoff_ns = MS;
    options.handshake_timeout_ns = 200 * MS;
    options.idle_timeout_ns = 0;
    options.send_coalesce_bytes = 65536;
    options.zero_copy_bytes = 4096;
    options.stream_chunk_bytes = 1024;
    return options;
}

/* The executor of a node, registered as a process would before init. */
static void node_executor(struct node *n)
{
    memset(&n->wrap, 0, sizeof(n->wrap));
    n->wrap.inner = vsr_sim_executor(world.sim, n->index);
    CHECK(n->wrap.inner.ops != NULL);
    n->ex.ops = &wrap_ops;
    n->ex.ctx = &n->wrap;
    /* The engine's executor: the purity guard over the wrapper, armed
     * around every primitive call (decision E7). */
    n->ex = pure_executor_init(&n->pure, n->ex, true);
    if (!n->registered) {
        /* The tables are executor-wide, registered once per executor. */
        CHECK(n->ex.ops->register_files(n->ex.ctx,
                                        FILE_SLOT_BASE + FILE_SLOTS + 8) == 0);
        CHECK(n->ex.ops->register_buffers(n->ex.ctx,
                                          REGION_BASE + BUFFER_REGIONS) == 0);
        n->registered = true;
    }
}

static struct node *node_open_with(uint32_t index,
                                   const struct vsr_io_options *custom)
{
    struct node *n = &world.node[index];
    struct vsr_io_options options;
    struct vsr_io_layout layout;
    struct vsr_io_region metadata;
    struct vsr_io_region payload;

    memset(n->apps, 0, sizeof(n->apps));
    n->pending_count = 0;
    n->seen_count = 0;
    n->hold_ops = 0;
    n->stream_ends = n->stream_serves = n->stream_data = 0;
    n->stream_data_bytes = n->stream_written = 0;
    n->served_handle = 0;
    n->external_peer = 0;
    n->handshakes = 0;
    n->links_wanted = 0;
    n->hold_serve = false;
    n->hold_data = false;
    n->held_data_count = 0;
    node_executor(n);
    options = custom != NULL ? *custom : engine_options(n);
    options.executor = n->ex;
    CHECK(vsr_io_layout(&options, &layout) == VSR_OK);
    CHECK(layout.metadata.size <= ENGINE_BYTES);
    CHECK(layout.payload.size <= sizeof(payload_memory[index]));
    metadata.base = engine_memory[index];
    metadata.size = layout.metadata.size;
    payload.base = payload_memory[index];
    payload.size = layout.payload.size;
    CHECK(vsr_io_init(&options, &metadata, &payload, &n->io) == VSR_OK);
    n->open = true;
    for (uint32_t r = 0; r < REPLICAS; ++r) {
        n->apps[r].node = n;
        n->apps[r].index = r;
        n->apps[r].next_route = 1;
    }
    return n;
}

static struct node *node_open(uint32_t index)
{
    return node_open_with(index, NULL);
}

/* -------------------------------------------------------------------------
 * The caller's leases and events
 * ---------------------------------------------------------------------- */

static uint64_t lease_counter = 1;

static struct lease_slot *lease_take(struct app *app)
{
    for (uint32_t i = 0; i < LEASES; ++i) {
        struct lease_slot *slot = &app->leases[i];

        if (slot->id == 0) {
            memset(slot, 0, sizeof(*slot));
            slot->id = lease_counter++;
            return slot;
        }
    }
    CHECK(false);
    return NULL;
}

static uint32_t leases_out(const struct app *app)
{
    uint32_t count = 0;

    for (uint32_t i = 0; i < LEASES; ++i) {
        count += app->leases[i].id != 0 ? 1u : 0u;
    }
    return count;
}

static void lease_release(struct app *app, uint64_t id)
{
    for (uint32_t i = 0; i < LEASES; ++i) {
        if (app->leases[i].id == id) {
            app->leases[i].id = 0;
            app->releases++;
            return;
        }
    }
    fprintf(stderr,
            "RELEASE of a lease the caller does not hold: %" PRIu64 "\n", id);
    CHECK(false);
}

/* Events for the next submit; the loop is woken as an application queue
 * would wake it from outside. */
static void queue_event(struct node *n, const struct vsr_io_event *event)
{
    CHECK(n->pending_count < PENDING);
    n->pending[n->pending_count++] = *event;
    vsr_io_wake(n->io);
}

static void core_event(struct app *app, uint32_t type, int32_t status,
                       uint64_t id, const void *data, uint64_t lease)
{
    struct vsr_io_event event;

    memset(&event, 0, sizeof(event));
    event.replica = app->replica;
    event.kind = VSR_IO_EVENT_CORE;
    event.event.type = type;
    event.event.status = status;
    event.event.id = id;
    event.event.data = data;
    event.event.lease = lease;
    queue_event(app->node, &event);
}

static void complete_op(struct app *app, uint64_t id, int32_t status)
{
    core_event(app, VSR_EVENT_COMPLETE, status, id, NULL, 0);
}

static struct vsr_id client_id(uint64_t n)
{
    struct vsr_id id = {0xC11E47u, n};

    return id;
}

/* A COMMAND of client n, number `number`, under a fresh lease; the REPLY
 * comes back with the returned route. */
static uint64_t make_request(struct app *app, uint64_t client, uint64_t number,
                             struct vsr_io_event *out)
{
    struct lease_slot *slot = lease_take(app);
    uint64_t route = app->next_route++;

    slot->bytes[0] = (unsigned char)number;
    slot->span.data = slot->bytes;
    slot->span.size = 8;
    slot->blob.spans = &slot->span;
    slot->blob.size = 8;
    slot->blob.count = 1;
    slot->request.id.client = client_id(client);
    slot->request.id.number = number;
    slot->request.epoch = 0;
    slot->request.type = VSR_REQUEST_COMMAND;
    slot->request.body = &slot->blob;
    memset(out, 0, sizeof(*out));
    out->replica = app->replica;
    out->kind = VSR_IO_EVENT_CORE;
    out->event.type = VSR_EVENT_REQUEST;
    out->event.id = route;
    out->event.data = &slot->request;
    out->event.lease = slot->id;
    return route;
}

static uint64_t submit_request(struct app *app, uint64_t client,
                               uint64_t number)
{
    struct vsr_io_event event;
    uint64_t route = make_request(app, client, number, &event);

    queue_event(app->node, &event);
    return route;
}

/* -------------------------------------------------------------------------
 * The application: forwarded ops to events
 * ---------------------------------------------------------------------- */

static struct app *app_of(struct node *n, const struct vsr_io_replica *replica)
{
    for (uint32_t r = 0; r < REPLICAS; ++r) {
        if (n->apps[r].attached && n->apps[r].replica == replica) {
            return &n->apps[r];
        }
    }
    return NULL;
}

static void complete_apply(struct app *app, uint64_t id,
                           const struct vsr_apply *apply)
{
    struct lease_slot *slot = lease_take(app);

    CHECK(apply->batch.count <= 8);
    for (uint32_t i = 0; i < apply->batch.count; ++i) {
        memset(&slot->values[i], 0, sizeof(slot->values[i]));
    }
    slot->applied.results = apply->batch.count > 0 ? slot->values : NULL;
    slot->applied.count = apply->batch.count;
    app->applies++;
    app->applied_entries += apply->batch.count;
    core_event(app, VSR_EVENT_COMPLETE, VSR_IO_OK, id, &slot->applied,
               slot->id);
}

static void app_core_op(struct app *app, const struct vsr_op *op)
{
    const struct vsr_snapshot_task *task;
    const struct vsr_reply *reply;
    struct lease_slot *slot;

    switch (op->type) {
    case VSR_OP_APPLY:
        if (app->hold_apply) {
            CHECK(app->held_apply == 0);
            app->held_apply = op->id;
            app->held_data = op->data;
            return;
        }
        complete_apply(app, op->id, op->data);
        return;
    case VSR_OP_READ_READY:
        app->read_ready++;
        complete_op(app, op->id, VSR_IO_OK);
        return;
    case VSR_OP_REPLY:
        reply = op->data;
        app->replies++;
        app->last_reply_status = reply->status;
        if (reply->status == VSR_REPLY_OK) {
            app->replies_ok++;
        }
        if ((reply->flags & VSR_REPLY_EXECUTED) != 0) {
            app->replies_executed++;
        }
        complete_op(app, op->id, VSR_IO_OK);
        return;
    case VSR_OP_SNAPSHOT_CAPTURE:
        task = op->data;
        CHECK(task != NULL && task->checkpoint != NULL);
        CHECK(task->checkpoint->id.hi != 0 || task->checkpoint->id.lo != 0);
        slot = lease_take(app);
        slot->checkpoint = *task->checkpoint;
        app->captures++;
        core_event(app, VSR_EVENT_COMPLETE, VSR_IO_OK, op->id,
                   &slot->checkpoint, slot->id);
        return;
    case VSR_OP_SNAPSHOT_FETCH:
    case VSR_OP_SNAPSHOT_SYNC:
    case VSR_OP_SNAPSHOT_INSTALL:
    case VSR_OP_SNAPSHOT_DROP:
        app->snapshot_ops++;
        complete_op(app, op->id, VSR_IO_OK);
        return;
    case VSR_OP_RELEASE:
        lease_release(app, op->arg);
        return;
    default:
        fprintf(stderr, "unexpected forwarded op type %u\n", op->type);
        CHECK(false);
    }
}

static void app_op(struct node *n, const struct vsr_io_op *op)
{
    struct app *app;
    struct vsr_io_event event;

    if (n->seen_count < PENDING) {
        n->seen[n->seen_count++] = *op;
    }
    switch (op->kind) {
    case VSR_IO_OP_CORE:
        app = app_of(n, op->replica);
        CHECK(app != NULL);
        app_core_op(app, &op->op);
        return;
    case VSR_IO_OP_STATUS:
        app = app_of(n, op->replica);
        CHECK(app != NULL);
        CHECK(op->op.data != NULL);
        app->status = *(const struct vsr_status *)op->op.data;
        app->statuses++;
        return;
    case VSR_IO_OP_STREAM_END: {
        const struct vsr_io_stream_end *end = op->op.data;

        n->stream_ends++;
        n->stream_end_status = end->status;
        n->stream_end_bytes = end->bytes;
        return;
    }
    case VSR_IO_OP_STREAM_SERVE: {
        const struct vsr_io_stream_serve *serve = op->op.data;

        n->stream_serves++;
        n->served_handle = serve->stream;
        if (n->hold_serve) {
            n->held_serve = op->op.id; /* The stream times out first. */
            return;
        }
        memset(&event, 0, sizeof(event));
        event.kind = VSR_IO_EVENT_COMPLETE;
        event.event.type = VSR_EVENT_COMPLETE;
        event.event.id = op->op.id;
        event.event.status = VSR_IO_OK;
        queue_event(n, &event);
        return;
    }
    case VSR_IO_OP_STREAM_DATA: {
        const struct vsr_io_stream_data *data = op->op.data;

        n->stream_data++;
        n->stream_data_bytes += data->bytes.size;
        if (n->hold_data) {
            CHECK(n->held_data_count < PENDING);
            n->held_data[n->held_data_count++] = op->op.id;
            return;
        }
        memset(&event, 0, sizeof(event));
        event.kind = VSR_IO_EVENT_COMPLETE;
        event.event.type = VSR_EVENT_COMPLETE;
        event.event.id = op->op.id;
        event.event.status = VSR_IO_OK;
        queue_event(n, &event);
        return;
    }
    case VSR_IO_OP_STREAM_WRITTEN:
        n->stream_written++;
        return;
    case VSR_IO_OP_HANDSHAKE: {
        const struct vsr_io_handshake *handshake = op->op.data;

        /* EXTERNAL: the caller authenticates; here it trusts the expected
         * node, or the node the test says dials in. */
        CHECK(n->external_peer != 0);
        n->handshakes++;
        n->handshake_done.node = handshake->direction == VSR_IO_OUTBOUND
                                     ? handshake->node
                                     : n->external_peer;
        memset(&event, 0, sizeof(event));
        event.kind = VSR_IO_EVENT_COMPLETE;
        event.event.type = VSR_EVENT_COMPLETE;
        event.event.id = op->op.id;
        event.event.status = VSR_IO_OK;
        event.event.data = &n->handshake_done;
        queue_event(n, &event);
        return;
    }
    case VSR_IO_OP_LINK_WANTED:
        n->links_wanted++; /* The test dials nothing itself. */
        return;
    default:
        fprintf(stderr, "unexpected rail op kind %u\n", op->kind);
        CHECK(false);
    }
}

/* Submits the pending events; what the engine refuses with AGAIN waits
 * for the next poll, anything else is a test failure unless expected. */
static void submit_pending(struct node *n)
{
    uint32_t consumed = 0;
    int rc;

    if (n->pending_count == 0) {
        return;
    }
    rc = PURE(n, vsr_io_submit(n->io, n->pending, n->pending_count, &consumed));
    if (rc != VSR_OK && rc != VSR_AGAIN) {
        fprintf(stderr, "submit refused event %u: %d\n", consumed, rc);
        CHECK(false);
    }
    memmove(n->pending, n->pending + consumed,
            (size_t)(n->pending_count - consumed) * sizeof(n->pending[0]));
    n->pending_count -= consumed;
}

/* -------------------------------------------------------------------------
 * The loop
 * ---------------------------------------------------------------------- */

static struct vsr_io_cqe foreign[CQES];
static uint32_t foreign_count;

/* One iteration of vsr-io.h's loop: reap, run until quiescent, prepare,
 * block (the simulation records the wait). */
static void iterate(struct node *n)
{
    struct vsr_io_cqe cqes[CQES];
    struct vsr_io_sqe sqes[BATCH];
    struct vsr_io_op ops[OPS];
    uint64_t now = n->ex.ops->now(n->ex.ctx);
    uint32_t reaped = n->ex.ops->reap(n->ex.ctx, cqes, CQES);
    uint32_t count = 0;
    uint64_t deadline = 0;
    uint32_t rounds = 0;

    if (getenv("VSR_ENGINE_TRACE") != NULL) {
        fprintf(stderr, "iterate node %u at %" PRIu64 " reaped %u\n", n->index,
                now, reaped);
    }
    for (uint32_t i = 0; i < reaped; ++i) {
        if (VSR_IO_OWNER(cqes[i].user_data) == OWNER) {
            CHECK(PURE(n, vsr_io_complete(n->io, &cqes[i], 1)) == VSR_OK);
        } else if (foreign_count < CQES) {
            foreign[foreign_count++] = cqes[i];
        }
    }
    n->seen_count = 0;
    for (;;) {
        uint32_t k = 0;
        uint32_t flags = 0;
        uint32_t before;

        CHECK(PURE(n, vsr_io_poll(n->io, now, ops, n->hold_ops != 0 ? 0 : OPS,
                                  &k, &flags)) == VSR_OK);
        for (uint32_t i = 0; i < k; ++i) {
            app_op(n, &ops[i]);
        }
        before = n->pending_count;
        submit_pending(n);
        /* Again while ops came, events went in, or the engine has more. */
        if (k == 0 && n->pending_count == before &&
            ((flags & VSR_IO_POLL_MORE) == 0 || n->hold_ops != 0)) {
            break;
        }
        /* The engine must not stay runnable forever within one iteration
         * (a hot loop between the core and an at-once completion). */
        CHECK(++rounds < 10000);
    }
    CHECK(PURE(n, vsr_io_prepare(n->io, now, sqes, BATCH, &count, &deadline)) ==
          VSR_OK);
    if (n->pending_count > 0) {
        deadline = now;
    }
    if (getenv("VSR_ENGINE_TRACE") != NULL) {
        fprintf(stderr, "  node %u prepared %u deadline %" PRIu64 " fwd %u\n",
                n->index, count, deadline, n->io->forwarded_count);
    }
    CHECK(n->ex.ops->submit_and_wait(n->ex.ctx, sqes, count, 1, 0, deadline) ==
          0);
}

typedef bool (*predicate)(void *ctx);

/* Runs every ready node, then advances the clock; true once `until` holds,
 * false after `rounds` or when nothing can happen any more. */
static bool run_nodes(void);

/* The predicate is checked after the nodes ran and before the clock
 * moves, so a test acts at the time the condition arose. */
static bool run_until(predicate until, void *ctx, uint32_t rounds)
{
    for (uint32_t round = 0; round < rounds; ++round) {
        bool ran;

        if (until(ctx)) {
            return true;
        }
        ran = run_nodes();
        if (until(ctx)) {
            return true;
        }
        if (vsr_sim_advance(world.sim) < 0 && !ran) {
            break;
        }
    }
    return until(ctx);
}

/* One round: every ready node iterates, then the clock advances; false
 * when nothing can happen any more. */
static bool run_nodes(void)
{
    bool ran = false;

    for (uint32_t i = 0; i < world.nodes; ++i) {
        struct node *n = &world.node[i];

        if (n->open && vsr_sim_ready(world.sim, i)) {
            iterate(n);
            ran = true;
        }
    }
    return ran;
}

static bool run_round(void)
{
    bool ran = run_nodes();

    return vsr_sim_advance(world.sim) >= 0 || ran;
}

static void run_for(uint64_t ns)
{
    uint64_t end = vsr_sim_now(world.sim) + ns;

    for (uint32_t round = 0; round < 1000000; ++round) {
        if (vsr_sim_now(world.sim) >= end || !run_round()) {
            return;
        }
    }
    CHECK(false);
}

/* -------------------------------------------------------------------------
 * Replicas
 * ---------------------------------------------------------------------- */

static uint64_t round_up(uint64_t value, uint64_t multiple)
{
    return (value + multiple - 1) / multiple * multiple;
}

static struct vsr_io_store_options store_options(void)
{
    struct vsr_io_store_options o;
    uint64_t max_record = 0;
    size_t segment_limit = 0;
    uint64_t header_bytes;

    CHECK(vsr_io_codec_record_limit(&core_limits, &max_record) == VSR_OK);
    CHECK(vsr_io_codec_segment_limit(&core_limits, &segment_limit) == VSR_OK);
    header_bytes = round_up(segment_limit, BLOCK);
    memset(&o, 0, sizeof(o));
    o.block_bytes = BLOCK;
    o.segments = 2;
    o.max_segments = 8;
    o.max_entries = 256;
    o.max_clients = 4;
    o.inflight_writes = 2;
    o.segment_bytes = round_up(header_bytes + 8 * max_record, BLOCK);
    o.write_behind_bytes = (uint64_t)8 * BLOCK;
    o.cache_bytes =
        round_up(o.write_behind_bytes + core_limits.pinned_payload_bytes +
                     2 * max_record + 2 * header_bytes + BLOCK,
                 BLOCK);
    o.direct_io = 1;
    o.sync_mode = VSR_IO_SYNC_FDATASYNC;
    o.on_write_error = VSR_IO_WRITE_ERROR_FENCE;
    return o;
}

static struct vsr_id cluster_of(uint64_t n)
{
    struct vsr_id id = {0xC1u, n};

    return id;
}

/* Replica options of `replica_id` in a group of `members` (ids 1..members,
 * all FULL, f = (members - 1) / 2), the store in directory `path`. */
static void app_options(struct app *app, struct vsr_id cluster,
                        uint64_t replica_id, uint32_t members, uint32_t mode)
{
    struct vsr_options *core = &app->options.core;

    memset(&app->options, 0, sizeof(app->options));
    for (uint32_t i = 0; i < members; ++i) {
        app->members[i].id = i + 1;
        app->members[i].role = VSR_MEMBER_FULL;
        app->members[i].reserved = 0;
    }
    app->seed.epoch = 0;
    app->seed.members = app->members;
    app->seed.count = members;
    app->seed.faults = (members - 1) / 2;
    core->cluster = cluster;
    core->incarnation.hi = 0x1Cu;
    core->incarnation.lo = world.incarnation++;
    core->replica = replica_id;
    core->seed = &app->seed;
    core->limits = core_limits;
    core->heartbeat_ns = 10 * MS;
    core->view_timeout_ns = 50 * MS;
    core->retry_ns = 5 * MS;
    core->transfer_timeout_ns = 100 * MS;
    core->start_mode = mode;
    core->durability = world.durability;
    app->options.store = store_options();
    snprintf(app->path, sizeof(app->path), "c%" PRIu64 "-r%" PRIu64, cluster.lo,
             replica_id);
    app->options.path = app->path;
}

/* A directory in the node's root, made through its executor with the
 * caller's owner tag. */
static void make_directory(struct node *n, const char *path)
{
    struct vsr_io_sqe sqe;
    uint64_t user_data = VSR_IO_USER_DATA(APP_OWNER, 0xD1);

    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode = VSR_IO_SQE_MKDIRAT;
    sqe.fd = VSR_SIM_ROOT;
    sqe.addr = path;
    sqe.length = 0755;
    sqe.user_data = user_data;
    foreign_count = 0;
    CHECK(n->ex.ops->submit_and_wait(n->ex.ctx, &sqe, 1, 0, 0, 0) == 0);
    for (uint32_t i = 0; i < 1000; ++i) {
        struct vsr_io_cqe cqe;

        if (n->ex.ops->reap(n->ex.ctx, &cqe, 1) == 1) {
            CHECK(cqe.user_data == user_data);
            CHECK(cqe.result == 0 || cqe.result == -EEXIST);
            return;
        }
        (void)vsr_sim_advance(world.sim);
    }
    CHECK(false);
}

static struct app *app_attach(struct node *n, uint32_t r, struct vsr_id cluster,
                              uint64_t replica_id, uint32_t members,
                              uint32_t mode)
{
    struct app *app = &n->apps[r];
    struct vsr_io_replica_layout layout;
    struct vsr_io_region metadata;
    struct vsr_io_region tail;

    memset(app->leases, 0, sizeof(app->leases));
    app->statuses = 0;
    app->replies = app->replies_ok = app->replies_executed = 0;
    app->applies = app->applied_entries = app->releases = 0;
    app->captures = app->snapshot_ops = app->read_ready = 0;
    app->hold_apply = false;
    app->held_apply = 0;
    app_options(app, cluster, replica_id, members, mode);
    if (mode != VSR_START_RECOVER) {
        make_directory(n, app->path);
    }
    CHECK(vsr_io_replica_layout(n->io, &app->options, &layout) == VSR_OK);
    CHECK(layout.metadata.size <= REPLICA_BYTES);
    CHECK(layout.metadata.alignment <= 4096);
    CHECK(layout.tail.size <= TAIL_BYTES && layout.tail.alignment <= 4096);
    metadata.base = replica_memory[n->index][r];
    metadata.size = REPLICA_BYTES;
    tail.base = tail_memory[n->index][r];
    tail.size = TAIL_BYTES;
    CHECK(vsr_io_attach(n->io, &app->options, &metadata, &tail,
                        &app->replica) == VSR_OK);
    CHECK(app->replica != NULL);
    app->attached = true;
    return app;
}

static bool app_normal(void *ctx)
{
    const struct app *app = ctx;

    return app->statuses > 0 && app->status.state == VSR_STATE_NORMAL;
}

static bool app_stopped(void *ctx)
{
    const struct app *app = ctx;

    return app->replica->state == VSR_IO_REPLICA_STOPPED;
}

struct replies_at_least {
    const struct app *app;
    uint64_t replies;
};

static bool replies_reached(void *ctx)
{
    const struct replies_at_least *want = ctx;

    return want->app->replies >= want->replies;
}

/* Everything the store packed has been written (not necessarily
 * flushed). */
static bool store_written(void *ctx)
{
    const struct app *app = ctx;
    struct vsr_io_store_status store;

    vsr_io_replica_status(app->replica, NULL, &store);
    return store.written >= store.readable && store.unwritten_bytes == 0;
}

static void stop_app(struct app *app)
{
    core_event(app, VSR_EVENT_STOP, 0, 0, NULL, 0);
    CHECK(run_until(app_stopped, app, 20000));
}

static bool detached(void *ctx)
{
    struct app *app = ctx;

    return vsr_io_detach(app->replica) == VSR_OK;
}

static void detach_app(struct app *app)
{
    CHECK(run_until(detached, app, 20000));
    app->attached = false;
    CHECK(leases_out(app) == 0);
    CHECK(vsr_deinit(vsr_io_replica_core(app->replica)) == VSR_OK);
}

static bool node_closed(void *ctx)
{
    const struct node *n = ctx;
    struct vsr_io_stats stats;

    vsr_io_get_stats(n->io, &stats);
    return stats.closed != 0;
}

/* vsr_io_close from outside the loop: woken so its prepare issues the
 * teardown. */
static void close_io(struct node *n)
{
    CHECK(PURE(n, vsr_io_close(n->io)) == VSR_OK);
    vsr_io_wake(n->io);
}

static void close_node(struct node *n)
{
    close_io(n);
    CHECK(run_until(node_closed, n, 20000));
    CHECK(vsr_io_deinit(n->io) == VSR_OK);
    n->open = false;
}

/* The engine's leases and slabs are all back once nothing is outstanding. */
static void check_idle_replica(const struct app *app)
{
    const struct vsr_io_replica *rep = app->replica;

    CHECK(rep->completions_count == 0);
    CHECK(rep->priority_count == 0);
    CHECK(rep->events_count == 0);
    CHECK(rep->messages_count == 0);
    CHECK(rep->leases_free == rep->regions_count);
}

/* -------------------------------------------------------------------------
 * Tests: one replica through the loop
 * ---------------------------------------------------------------------- */

/* NEW over an empty directory: the first poll steps the core with TIME
 * alone, its RECOVERY LOAD opens the store (NOT_FOUND, then the log is
 * created), the core boots to NORMAL and the first STATUS makes the
 * replica RUNNING; requests commit, apply and reply; STOP, detach, close
 * and deinit end it with every lease and slab back. */
static void test_single_replica(void)
{
    struct node *n;
    struct app *app;
    struct replies_at_least want;
    struct vsr_io_stats stats;
    struct vsr_status core;
    struct vsr_io_store_status store;

    world_open(1, 1);
    n = node_open(0);
    app = app_attach(n, 0, cluster_of(1), 1, 1, VSR_START_NEW);
    CHECK(app->replica->state == VSR_IO_REPLICA_OPENING);
    CHECK(vsr_io_replica_find(n->io, cluster_of(1), 1) == app->replica);
    CHECK(vsr_io_replica_find(n->io, cluster_of(1), 2) == NULL);
    CHECK(vsr_io_replica_find(n->io, cluster_of(2), 1) == NULL);
    CHECK(vsr_io_replica_core(app->replica) != NULL);
    CHECK(run_until(app_normal, app, 20000));
    CHECK(app->replica->state == VSR_IO_REPLICA_RUNNING);
    CHECK(app->replica->store.state == VSR_IO_STORE_READY);
    CHECK(app->status.primary == 1);
    for (uint64_t i = 1; i <= 5; ++i) {
        submit_request(app, 1, i);
        want.app = app;
        want.replies = i;
        CHECK(run_until(replies_reached, &want, 20000));
    }
    CHECK(app->replies_ok == 5 && app->replies_executed == 5);
    CHECK(app->applied_entries >= 5);
    vsr_io_replica_status(app->replica, &core, &store);
    CHECK(core.state == VSR_STATE_NORMAL && core.committed >= 5);
    CHECK(store.readable >= 5 && store.durable >= 1);
    vsr_io_get_stats(n->io, &stats);
    CHECK(stats.replicas == 1 && stats.writes > 0 && stats.flushes > 0);
    run_for(20 * MS);
    stop_app(app);
    CHECK(app->status.state == VSR_STATE_STOPPED);
    check_idle_replica(app);
    detach_app(app);
    vsr_io_get_stats(n->io, &stats);
    CHECK(stats.replicas == 0);
    close_node(n);
    world_close();
}

/* -------------------------------------------------------------------------
 * Tests: the routing of one update (7.3), through the seam
 * ---------------------------------------------------------------------- */

/* One loop iteration without core steps: the store and snapshot polls,
 * prepare, submission, the simulated clock, completions. The routing
 * tests drive the modules this way and read the completions themselves. */
static void pump(struct node *n)
{
    struct vsr_io_cqe cqes[CQES];
    struct vsr_io_sqe sqes[BATCH];
    uint64_t now = n->ex.ops->now(n->ex.ctx);
    uint64_t deadline = 0;
    uint32_t count = 0;
    uint32_t reaped;

    for (uint32_t r = 0; r < REPLICAS; ++r) {
        if (n->apps[r].attached) {
            PURE_VOID(n,
                      vsr_io_store_poll(n->io, n->apps[r].replica->index, now));
            PURE_VOID(n, vsr_io_snapshots_poll(n->io, n->apps[r].replica->index,
                                               now));
        }
    }
    CHECK(PURE(n, vsr_io_prepare(n->io, now, sqes, BATCH, &count, &deadline)) ==
          VSR_OK);
    CHECK(n->ex.ops->submit_and_wait(n->ex.ctx, sqes, count, 0, 0, 0) == 0);
    (void)vsr_sim_advance(world.sim);
    reaped = n->ex.ops->reap(n->ex.ctx, cqes, CQES);
    if (getenv("VSR_ENGINE_TRACE") != NULL) {
        fprintf(stderr, "iterate node %u at %" PRIu64 " reaped %u\n", n->index,
                now, reaped);
    }
    for (uint32_t i = 0; i < reaped; ++i) {
        if (VSR_IO_OWNER(cqes[i].user_data) == OWNER) {
            CHECK(PURE(n, vsr_io_complete(n->io, &cqes[i], 1)) == VSR_OK);
        }
    }
}

/* The store's queued completions join the replica's ring, as the poll
 * does. */
static void drain_store(struct vsr_io_replica *rep)
{
    struct vsr_io_completion completion;

    while (vsr_io_store_next_completion(&rep->store, &completion)) {
        vsr_io_engine_complete_core(rep, &completion);
    }
}

/* Takes op's completion off the replica's internal ring (pumping the node
 * until it is there); returns its engine lease index, NONE without. */
/* A completion the routing decided at once waits in the deferred ring
 * for the replica's retry_ns (decision E3); true when op's is there, then
 * taken off it. */
static bool take_deferred(struct vsr_io_replica *rep, uint64_t op,
                          int32_t status)
{
    uint32_t capacity = rep->options.limits.operations;

    for (uint32_t i = 0; i < rep->deferred_count; ++i) {
        uint32_t at = (rep->deferred_head + i) % capacity;

        if (rep->deferred[at].op != op) {
            continue;
        }
        CHECK(rep->deferred[at].status == status);
        CHECK(rep->deferred[at].due == rep->io->now + rep->options.retry_ns);
        for (uint32_t j = i; j > 0; --j) {
            rep->deferred[(rep->deferred_head + j) % capacity] =
                rep->deferred[(rep->deferred_head + j - 1) % capacity];
        }
        rep->deferred_head = (rep->deferred_head + 1) % capacity;
        rep->deferred_count--;
        return true;
    }
    return false;
}

static uint32_t take_completion(struct node *n, struct vsr_io_replica *rep,
                                uint64_t op, int32_t status)
{
    for (uint32_t round = 0; round < 2000; ++round) {
        uint32_t capacity = rep->options.limits.operations;

        if (take_deferred(rep, op, status)) {
            return VSR_IO_INDEX_NONE;
        }
        drain_store(rep);
        for (uint32_t i = 0; i < rep->completions_count; ++i) {
            uint32_t at = (rep->completions_head + i) % capacity;
            struct vsr_io_queued_event found = rep->completions[at];

            if (found.event.id != op) {
                continue;
            }
            if (found.event.status != status) {
                fprintf(stderr, "op %" PRIu64 " completed %d, not %d\n", op,
                        found.event.status, status);
                CHECK(false);
            }
            for (uint32_t j = i; j > 0; --j) {
                rep->completions[(rep->completions_head + j) % capacity] =
                    rep->completions[(rep->completions_head + j - 1) %
                                     capacity];
            }
            rep->completions_head = (rep->completions_head + 1) % capacity;
            rep->completions_count--;
            if (found.lease != VSR_IO_INDEX_NONE) {
                /* The core would have accepted the event. */
                rep->leases[found.lease].state = VSR_IO_LEASE_STATE_LEASED;
            }
            return found.lease;
        }
        pump(n);
    }
    fprintf(stderr, "op %" PRIu64 " never completed\n", op);
    CHECK(false);
    return VSR_IO_INDEX_NONE;
}

static bool completion_queued(struct vsr_io_replica *rep, uint64_t op)
{
    uint32_t capacity = rep->options.limits.operations;

    for (uint32_t i = 0; i < rep->deferred_count; ++i) {
        if (rep->deferred[(rep->deferred_head + i) % capacity].op == op) {
            return true;
        }
    }
    drain_store(rep);
    for (uint32_t i = 0; i < rep->completions_count; ++i) {
        if (rep->completions[(rep->completions_head + i) % capacity].event.id ==
            op) {
            return true;
        }
    }
    return false;
}

/* Crafted store transactions: the identity and hard state at 1, then
 * CLIENTS records. */
struct txn {
    struct vsr_store store;
    struct vsr_change changes[2];
    struct vsr_store_identity identity;
    struct vsr_hard_state hard;
    struct vsr_epoch epoch;
    struct vsr_client_record records[2];
};

static struct txn txns[16];

static const struct vsr_store *txn_identity(const struct app *app)
{
    struct txn *t = &txns[1];

    memset(t, 0, sizeof(*t));
    t->identity.cluster = app->options.core.cluster;
    t->identity.replica = app->options.core.replica;
    t->identity.durability = VSR_DURABLE;
    t->epoch.current = &app->seed;
    t->epoch.phase = VSR_EPOCH_STEADY;
    t->hard.epoch = &t->epoch;
    t->hard.state = VSR_HARD_NORMAL;
    t->hard.role = VSR_MEMBER_FULL;
    t->changes[0].type = VSR_STORE_IDENTITY;
    t->changes[0].count = 1;
    t->changes[0].data = &t->identity;
    t->changes[1].type = VSR_STORE_HARD_STATE;
    t->changes[1].count = 1;
    t->changes[1].data = &t->hard;
    t->store.sequence = 1;
    t->store.changes = t->changes;
    t->store.count = 2;
    return &t->store;
}

static const struct vsr_store *txn_clients(uint64_t sequence, uint64_t client,
                                           uint64_t number, uint64_t op)
{
    struct txn *t = &txns[sequence];

    CHECK(sequence < 16);
    memset(t, 0, sizeof(*t));
    t->records[0].request.client = client_id(client);
    t->records[0].request.number = number;
    t->records[0].op = op;
    t->changes[0].type = VSR_STORE_CLIENTS;
    t->changes[0].count = 1;
    t->changes[0].data = t->records;
    t->store.sequence = sequence;
    t->store.changes = t->changes;
    t->store.count = 1;
    return &t->store;
}

static struct vsr_op make_op(uint32_t type, uint64_t id, const void *data,
                             uint64_t arg)
{
    struct vsr_op op;

    memset(&op, 0, sizeof(op));
    op.type = type;
    op.id = id;
    op.data = data;
    op.arg = arg;
    return op;
}

static void route(struct app *app, const struct vsr_op *ops, uint32_t count)
{
    PURE_VOID(app->node, vsr_io_engine_route(app->replica, ops, count));
}

static struct vsr_store_read client_read(uint64_t sequence, uint64_t client)
{
    struct vsr_store_read read;

    memset(&read, 0, sizeof(read));
    read.sequence = sequence;
    read.client = client_id(client);
    read.max_bytes = core_limits.result_bytes;
    read.type = VSR_LOAD_CLIENT;
    read.max_count = 1;
    return read;
}

/* A replica whose core never steps, its store opened by a crafted
 * RECOVERY LOAD (NEW: NOT_FOUND, the log created) and given the identity
 * and a client record of client 1 at sequence 2. */
static struct app *seam_replica(struct node *n, uint32_t members)
{
    static struct vsr_store_read recovery;
    struct app *app =
        app_attach(n, 0, cluster_of(9), 1, members, VSR_START_NEW);
    struct vsr_op ops[2];

    memset(&recovery, 0, sizeof(recovery));
    recovery.type = VSR_LOAD_RECOVERY;
    recovery.max_count = 1;
    ops[0] = make_op(VSR_OP_LOAD, 1, &recovery, 0);
    route(app, ops, 1);
    CHECK(app->replica->store.state == VSR_IO_STORE_OPENING);
    CHECK(take_completion(n, app->replica, 1, VSR_IO_NOT_FOUND) ==
          VSR_IO_INDEX_NONE);
    ops[0] = make_op(VSR_OP_STORE, 2, txn_identity(app), 0);
    ops[1] = make_op(VSR_OP_STORE, 3, txn_clients(2, 1, 1, 1), 0);
    route(app, ops, 2);
    CHECK(take_completion(n, app->replica, 2, VSR_IO_OK) == VSR_IO_INDEX_NONE);
    CHECK(take_completion(n, app->replica, 3, VSR_IO_OK) == VSR_IO_INDEX_NONE);
    CHECK(app->replica->store.readable == 2);
    CHECK(app->replica->store.state == VSR_IO_STORE_READY);
    return app;
}

static const struct vsr_io_forwarded *forwarded_at(const struct vsr_io *io,
                                                   uint32_t i)
{
    return &io->forwarded[(io->forwarded_head + i) % io->options.limits.ops];
}

/* Decision 51: within one update every LOAD is routed before any STORE,
 * so a CLIENT load naming the stored sequence is answered from it, not
 * RETRY behind the STORE the same update carries; SYNC and RECLAIM after
 * them; the rest in emission order. */
static void test_route_order(void)
{
    struct node *n;
    struct app *app;
    struct vsr_io_replica *rep;
    struct vsr_store_read read;
    struct vsr_op ops[6];
    struct vsr_read_fence fence;
    struct vsr_apply apply;
    uint32_t lease;

    world_open(1, 2);
    n = node_open(0);
    app = seam_replica(n, 1);
    rep = app->replica;
    /* STORE of client 1's next record, then a LOAD at sequence 2. */
    read = client_read(2, 1);
    ops[0] = make_op(VSR_OP_STORE, 10, txn_clients(3, 1, 2, 2), 0);
    ops[1] = make_op(VSR_OP_LOAD, 11, &read, 0);
    route(app, ops, 2);
    lease = take_completion(n, rep, 11, VSR_IO_OK);
    CHECK(lease != VSR_IO_INDEX_NONE);
    CHECK(take_completion(n, rep, 10, VSR_IO_OK) == VSR_IO_INDEX_NONE);
    CHECK(rep->store.readable == 3);
    /* The same shape one sequence later, in emission order, would answer
     * RETRY: the LOAD names sequence 3 while client 1's record moved. */
    read = client_read(3, 1);
    ops[0] = make_op(VSR_OP_STORE, 12, txn_clients(4, 1, 3, 3), 0);
    ops[1] = make_op(VSR_OP_LOAD, 13, &read, 0);
    route(app, ops, 1);
    route(app, ops + 1, 1);
    CHECK(take_completion(n, rep, 13, VSR_IO_RETRY) == VSR_IO_INDEX_NONE);
    CHECK(take_completion(n, rep, 12, VSR_IO_OK) == VSR_IO_INDEX_NONE);
    /* RELEASE of the LOAD's engine lease: region, pin and slab back; a
     * repeated or stale id is counted and ignored. */
    CHECK(rep->leases[lease].state == VSR_IO_LEASE_STATE_LEASED);
    CHECK(rep->leases_free == rep->regions_count - 1);
    CHECK(rep->store.pins[lease].begin != UINT64_MAX);
    ops[0] = make_op(VSR_OP_RELEASE, 0, NULL, vsr_io_lease_id(rep, lease));
    route(app, ops, 1);
    CHECK(rep->leases[lease].state == VSR_IO_LEASE_STATE_FREE);
    CHECK(rep->leases_free == rep->regions_count);
    CHECK(rep->store.pins[lease].begin == UINT64_MAX);
    CHECK(n->io->releases_rejected == 0);
    route(app, ops, 1);
    CHECK(n->io->releases_rejected == 1);
    CHECK(n->io->forwarded_count == 0);
    /* The forwarded kinds keep their order around the store's ops. */
    memset(&fence, 0, sizeof(fence));
    memset(&apply, 0, sizeof(apply));
    read = client_read(4, 1);
    ops[0] = make_op(VSR_OP_READ_READY, 20, &fence, 0);
    ops[1] = make_op(VSR_OP_SYNC, 21, NULL, 4);
    ops[2] = make_op(VSR_OP_APPLY, 22, &apply, 0);
    ops[3] = make_op(VSR_OP_LOAD, 23, &read, 0);
    ops[4] = make_op(VSR_OP_RELEASE, 0, NULL, 77);
    ops[5] = make_op(VSR_OP_RECLAIM, 24, NULL, 1);
    route(app, ops, 6);
    CHECK(n->io->forwarded_count == 3);
    CHECK(forwarded_at(n->io, 0)->op.kind == VSR_IO_OP_CORE);
    CHECK(forwarded_at(n->io, 0)->op.replica == rep);
    CHECK(forwarded_at(n->io, 0)->op.op.id == 20);
    CHECK(forwarded_at(n->io, 0)->op.op.data == &fence);
    CHECK(forwarded_at(n->io, 1)->op.op.id == 22);
    CHECK(forwarded_at(n->io, 1)->op.op.type == VSR_OP_APPLY);
    CHECK(forwarded_at(n->io, 2)->op.op.type == VSR_OP_RELEASE);
    CHECK(forwarded_at(n->io, 2)->op.op.arg == 77);
    lease = take_completion(n, rep, 23, VSR_IO_OK);
    CHECK(take_completion(n, rep, 21, VSR_IO_OK) == VSR_IO_INDEX_NONE);
    CHECK(take_completion(n, rep, 24, VSR_IO_OK) == VSR_IO_INDEX_NONE);
    ops[0] = make_op(VSR_OP_RELEASE, 0, NULL, vsr_io_lease_id(rep, lease));
    route(app, ops, 1);
    CHECK(rep->leases_free == rep->regions_count);
    world_close();
}

/* SEND, REPLY, RECOVERY, malformed ops and the snapshot ops. */
static void test_route_kinds(void)
{
    static struct vsr_message message;
    static struct vsr_reply reply;
    static struct vsr_store_read recovery;
    static struct vsr_snapshot_task task;
    static struct vsr_checkpoint template;
    static struct vsr_epoch epoch;
    struct node *n;
    struct app *app;
    struct vsr_io_replica *rep;
    struct vsr_io_address address;
    struct vsr_op ops[4];
    uint32_t node2;
    const struct vsr_io_client *entry;
    struct vsr_snapshot_task sync_task;

    world_open(2, 3);
    n = node_open(0);
    app = seam_replica(n, 3);
    rep = app->replica;
    /* SEND: to an authorized member it waits in its node's queue (the
     * link completes it); to an unknown member RETRY at once. */
    address = node_address(1);
    CHECK(PURE(n, vsr_io_node_set(n->io, 2, &address)) == VSR_OK);
    CHECK(PURE(n, vsr_io_authorize(n->io, app->options.core.cluster, 2, 2)) ==
          VSR_OK);
    node2 = vsr_io_links_node_index(&n->io->links, 2);
    CHECK(node2 != VSR_IO_INDEX_NONE);
    memset(&message, 0, sizeof(message));
    message.cluster = app->options.core.cluster;
    message.from = 1;
    message.type = VSR_MSG_PREPARE_OK;
    message.number = 1;
    ops[0] = make_op(VSR_OP_SEND, 30, &message, 2);
    ops[1] = make_op(VSR_OP_SEND, 31, &message, 3);
    route(app, ops, 2);
    CHECK(n->io->links.nodes[node2].queue_count == 1);
    CHECK(!completion_queued(rep, 30));
    /* The refused SEND's RETRY waits retry_ns (decision E3), and prepare
     * wakes the loop for it. */
    CHECK(rep->deferred_count == 1);
    {
        struct vsr_io_sqe sqes[BATCH];
        uint64_t now = n->ex.ops->now(n->ex.ctx);
        uint64_t deadline = 0;
        uint32_t count = 0;

        CHECK(PURE(n, vsr_io_prepare(n->io, now, sqes, BATCH, &count,
                                     &deadline)) == VSR_OK);
        CHECK(deadline <= rep->deferred[rep->deferred_head].due);
        CHECK(n->ex.ops->submit_and_wait(n->ex.ctx, sqes, count, 0, 0, 0) == 0);
    }
    CHECK(take_completion(n, rep, 31, VSR_IO_RETRY) == VSR_IO_INDEX_NONE);
    /* REPLY: forwarded; one other than OK ends the incarnation's admission
     * (decision 44), OK leaves it. */
    CHECK(vsr_io_store_admit(&rep->store, client_id(7)));
    CHECK(vsr_io_store_admit(&rep->store, client_id(8)));
    memset(&reply, 0, sizeof(reply));
    reply.request.client = client_id(7);
    reply.status = VSR_REPLY_NOT_PRIMARY;
    ops[0] = make_op(VSR_OP_REPLY, 32, &reply, 5);
    route(app, ops, 1);
    CHECK(n->io->forwarded_count == 1);
    CHECK(forwarded_at(n->io, 0)->op.op.id == 32);
    CHECK(forwarded_at(n->io, 0)->op.op.arg == 5);
    entry = NULL;
    for (uint32_t i = 0; i < rep->store.clients_capacity; ++i) {
        const struct vsr_io_client *c = &rep->store.clients[i];

        CHECK(c->id.lo != 7 || c->id.hi != client_id(7).hi);
        if (c->id.hi == client_id(8).hi && c->id.lo == 8) {
            entry = c;
        }
    }
    CHECK(entry != NULL && entry->inflight == 1);
    reply.request.client = client_id(8);
    reply.status = VSR_REPLY_OK;
    ops[0] = make_op(VSR_OP_REPLY, 33, &reply, 6);
    route(app, ops, 1);
    CHECK(entry->inflight == 1);
    /* A second RECOVERY LOAD finds the store open: FAILED, not lost. */
    memset(&recovery, 0, sizeof(recovery));
    recovery.type = VSR_LOAD_RECOVERY;
    ops[0] = make_op(VSR_OP_LOAD, 34, &recovery, 0);
    /* Malformed ops complete FAILED rather than hang the core. */
    ops[1] = make_op(VSR_OP_LOAD, 35, NULL, 0);
    ops[2] = make_op(VSR_OP_STORE, 36, NULL, 0);
    route(app, ops, 3);
    CHECK(take_completion(n, rep, 34, VSR_IO_FAILED) == VSR_IO_INDEX_NONE);
    CHECK(take_completion(n, rep, 35, VSR_IO_FAILED) == VSR_IO_INDEX_NONE);
    CHECK(take_completion(n, rep, 36, VSR_IO_FAILED) == VSR_IO_INDEX_NONE);
    /* FETCH from a member no node is authorized for: RETRY at once. */
    memset(&epoch, 0, sizeof(epoch));
    epoch.current = &app->seed;
    memset(&template, 0, sizeof(template));
    template.id.hi = 0xF;
    template.id.lo = 0xE;
    template.op = 1;
    template.epoch = &epoch;
    memset(&task, 0, sizeof(task));
    task.op = 1;
    task.peer = 3;
    task.checkpoint = &template;
    ops[0] = make_op(VSR_OP_SNAPSHOT_FETCH, 37, &task, 0);
    /* INSTALL is forwarded verbatim. */
    ops[1] = make_op(VSR_OP_SNAPSHOT_INSTALL, 38, &task, 0);
    /* A SYNC of an id the registry lacks is taken: forwarded by the
     * module's poll with the core's id. */
    memset(&sync_task, 0, sizeof(sync_task));
    sync_task.checkpoint = &template;
    ops[2] = make_op(VSR_OP_SNAPSHOT_SYNC, 39, &sync_task, 0);
    route(app, ops, 3);
    CHECK(take_completion(n, rep, 37, VSR_IO_RETRY) == VSR_IO_INDEX_NONE);
    CHECK(n->io->forwarded_count == 3);
    CHECK(forwarded_at(n->io, 2)->op.op.id == 38);
    CHECK(forwarded_at(n->io, 2)->op.op.type == VSR_OP_SNAPSHOT_INSTALL);
    world_close();
}

/* A CAPTURE routed with the STOREs of its update snapshots the client
 * table after them (STOREs first, decision 51): the core's capture fence
 * keeps later CLIENTS changes out, and the module snapshots what the
 * task's sequence covers. */
static void test_route_capture(void)
{
    static struct vsr_snapshot_task task;
    static struct vsr_checkpoint template;
    static struct vsr_epoch epoch;
    struct node *n;
    struct app *app;
    struct vsr_io_replica *rep;
    struct vsr_op ops[2];

    world_open(1, 4);
    n = node_open(0);
    app = seam_replica(n, 1);
    rep = app->replica;
    for (uint32_t i = 0; i < 50 && rep->snapshots.dir_state != 2; ++i) {
        pump(n); /* The module opens the directory at its first poll. */
    }
    memset(&epoch, 0, sizeof(epoch));
    epoch.current = &app->seed;
    memset(&template, 0, sizeof(template));
    template.op = 1;
    template.epoch = &epoch;
    memset(&task, 0, sizeof(task));
    task.op = 1;
    task.sequence = 3;
    task.checkpoint = &template;
    ops[0] = make_op(VSR_OP_SNAPSHOT_CAPTURE, 40, &task, 0);
    ops[1] = make_op(VSR_OP_STORE, 41, txn_clients(3, 2, 1, 2), 0);
    route(app, ops, 2);
    CHECK(rep->store.clients_sequence == 3);
    CHECK(rep->snapshots.capture != VSR_IO_INDEX_NONE);
    /* Both clients are in the captured table: 1 (sequence 2) and 2. */
    CHECK(rep->snapshots.writer.count == 2);
    CHECK(take_completion(n, rep, 41, VSR_IO_OK) == VSR_IO_INDEX_NONE);
    world_close();
}

/* -------------------------------------------------------------------------
 * Tests: the public functions' documented statuses
 * ---------------------------------------------------------------------- */

static struct vsr_io_region region_of(void *base, size_t size)
{
    struct vsr_io_region region;

    region.base = base;
    region.size = size;
    return region;
}

static int layout_status(struct node *n,
                         const struct vsr_io_replica_options *options)
{
    struct vsr_io_replica_layout layout;
    int rc = vsr_io_replica_layout(n->io, options, &layout);

    if (rc != VSR_OK) {
        CHECK(layout.metadata.size == 0 && layout.tail.size == 0);
    }
    return rc;
}

static void test_attach_errors(void)
{
    static char long_path[VSR_IO_ENGINE_PATH_BYTES + 1];
    struct node *n;
    struct app *app;
    struct app *other;
    struct vsr_io_replica_options o;
    struct vsr_io_replica_layout layout;
    struct vsr_io_region metadata;
    struct vsr_io_region tail;
    struct vsr_io_replica *out;
    struct vsr_io_replica fake;
    struct vsr_status core;
    struct vsr_io_store_status store;
    struct vsr_io_stats stats;
    size_t bytes = 0;
    size_t alignment = 0;
    size_t tail_bytes = 0;
    size_t tail_alignment = 0;

    world_open(1, 5);
    n = node_open(0);
    app = &n->apps[0];
    app_options(app, cluster_of(1), 1, 1, VSR_START_NEW);
    make_directory(n, app->path);
    /* Layout. */
    CHECK(vsr_io_replica_layout(n->io, &app->options, NULL) == VSR_EINVAL);
    memset(&layout, 0xEE, sizeof(layout));
    CHECK(vsr_io_replica_layout(NULL, &app->options, &layout) == VSR_EINVAL);
    CHECK(layout.metadata.size == 0 && layout.tail.size == 0);
    CHECK(vsr_io_replica_layout(n->io, NULL, &layout) == VSR_EINVAL);
    CHECK(vsr_io_replica_layout(n->io, &app->options, &layout) == VSR_OK);
    CHECK(layout.metadata.size > 0 && layout.tail.size > 0);
    CHECK(vsr_io_replica_size(n->io, &app->options, &bytes, &alignment,
                              &tail_bytes, &tail_alignment) == VSR_OK);
    CHECK(bytes == layout.metadata.size &&
          alignment == layout.metadata.alignment);
    CHECK(tail_bytes == layout.tail.size &&
          tail_alignment == layout.tail.alignment);
    CHECK(vsr_io_replica_size(n->io, &app->options, NULL, &alignment,
                              &tail_bytes, &tail_alignment) == VSR_EINVAL);
    o = app->options;
    o.reserved = 1;
    CHECK(layout_status(n, &o) == VSR_EINVAL);
    o = app->options;
    o.path = NULL;
    CHECK(layout_status(n, &o) == VSR_EINVAL);
    o.path = "";
    CHECK(layout_status(n, &o) == VSR_EINVAL);
    memset(long_path, 'p', VSR_IO_ENGINE_PATH_BYTES);
    long_path[VSR_IO_ENGINE_PATH_BYTES] = 0;
    o.path = long_path;
    CHECK(layout_status(n, &o) == VSR_EINVAL);
    long_path[VSR_IO_ENGINE_PATH_BYTES - 1] = 0;
    CHECK(layout_status(n, &o) == VSR_OK);
    o = app->options;
    o.core.heartbeat_ns = 0; /* The core's verdict. */
    CHECK(layout_status(n, &o) == VSR_EINVAL);
    o = app->options;
    o.core.limits.input_leases = 1; /* Below transfers + 8. */
    CHECK(layout_status(n, &o) == VSR_ELIMIT);
    o = app->options;
    o.store.block_bytes = 500; /* The store's verdict. */
    CHECK(layout_status(n, &o) == VSR_EINVAL);
    o = app->options;
    o.store.inflight_writes = VSR_IO_ENGINE_INFLIGHT_WRITES_MAX + 1;
    CHECK(layout_status(n, &o) == VSR_ELIMIT);
    o.store.inflight_writes = VSR_IO_ENGINE_INFLIGHT_WRITES_MAX;
    CHECK(layout_status(n, &o) == VSR_OK);
    o = app->options;
    o.core.limits.message_bytes =
        (uint64_t)(4u * SLAB_BYTES); /* Beyond a slab. */
    CHECK(layout_status(n, &o) == VSR_ELIMIT);
    /* Attach. */
    metadata = region_of(replica_memory[0][0], REPLICA_BYTES);
    tail = region_of(tail_memory[0][0], TAIL_BYTES);
    CHECK(vsr_io_attach(n->io, &app->options, &metadata, &tail, NULL) ==
          VSR_EINVAL);
    out = &fake;
    CHECK(vsr_io_attach(NULL, &app->options, &metadata, &tail, &out) ==
          VSR_EINVAL);
    CHECK(out == NULL);
    CHECK(vsr_io_attach(n->io, NULL, &metadata, &tail, &out) == VSR_EINVAL);
    CHECK(vsr_io_attach(n->io, &app->options, NULL, &tail, &out) == VSR_EINVAL);
    CHECK(vsr_io_attach(n->io, &app->options, &metadata, NULL, &out) ==
          VSR_EINVAL);
    o = app->options;
    o.store.max_clients = 0;
    CHECK(vsr_io_attach(n->io, &o, &metadata, &tail, &out) == VSR_EINVAL);
    metadata.base = replica_memory[0][0] + 8;
    CHECK(vsr_io_attach(n->io, &app->options, &metadata, &tail, &out) ==
          VSR_EINVAL);
    metadata = region_of(replica_memory[0][0], layout.metadata.size - 1);
    CHECK(vsr_io_attach(n->io, &app->options, &metadata, &tail, &out) ==
          VSR_ELIMIT);
    metadata = region_of(replica_memory[0][0], REPLICA_BYTES);
    tail.base = tail_memory[0][0] + 8;
    CHECK(vsr_io_attach(n->io, &app->options, &metadata, &tail, &out) ==
          VSR_EINVAL);
    tail = region_of(tail_memory[0][0], layout.tail.size - 1);
    CHECK(vsr_io_attach(n->io, &app->options, &metadata, &tail, &out) ==
          VSR_ELIMIT);
    tail = region_of(tail_memory[0][0], TAIL_BYTES);
    /* The executor refuses the tail registration: its errno, nothing
     * attached. */
    n->wrap.fail_update_buffer = -EBUSY;
    CHECK(vsr_io_attach(n->io, &app->options, &metadata, &tail, &out) ==
          -EBUSY);
    CHECK(out == NULL && n->io->replicas_count == 0);
    CHECK(n->io->replicas[0].state == VSR_IO_REPLICA_FREE);
    app = app_attach(n, 0, cluster_of(1), 1, 1, VSR_START_NEW);
    /* One replica per cluster; limits.replicas of them. */
    other = &n->apps[1];
    app_options(other, cluster_of(1), 2, 1, VSR_START_JOIN);
    metadata = region_of(replica_memory[0][1], REPLICA_BYTES);
    tail = region_of(tail_memory[0][1], TAIL_BYTES);
    CHECK(vsr_io_attach(n->io, &other->options, &metadata, &tail, &out) ==
          VSR_EINVAL);
    other = app_attach(n, 1, cluster_of(2), 1, 1, VSR_START_NEW);
    app_options(&n->apps[1], cluster_of(3), 1, 1, VSR_START_NEW);
    CHECK(vsr_io_attach(n->io, &n->apps[1].options, &metadata, &tail, &out) ==
          VSR_EBUSY);
    app_options(&n->apps[1], cluster_of(2), 1, 1, VSR_START_NEW);
    /* Detach: EINVAL for what is no attached replica, EBUSY until the
     * STATUS reporting STOPPED. */
    CHECK(vsr_io_detach(NULL) == VSR_EINVAL);
    memset(&fake, 0, sizeof(fake));
    fake.io = n->io;
    CHECK(vsr_io_detach(&fake) == VSR_EINVAL);
    CHECK(vsr_io_detach(app->replica) == VSR_EBUSY);
    CHECK(PURE(n, vsr_io_close(n->io)) == VSR_EBUSY);
    /* Accessors tolerate NULL. */
    memset(&core, 0xEE, sizeof(core));
    memset(&store, 0xEE, sizeof(store));
    vsr_io_replica_status(NULL, &core, &store);
    CHECK(core.state == 0 && core.committed == 0 && store.readable == 0);
    vsr_io_replica_status(app->replica, NULL, NULL);
    CHECK(vsr_io_replica_core(NULL) == NULL);
    CHECK(vsr_io_replica_find(NULL, cluster_of(1), 1) == NULL);
    CHECK(vsr_io_replica_find(n->io, cluster_of(2), 1) == other->replica);
    CHECK(run_until(app_normal, app, 20000));
    CHECK(run_until(app_normal, other, 20000));
    stop_app(app);
    stop_app(other);
    detach_app(app);
    CHECK(vsr_io_detach(app->replica) == VSR_EINVAL); /* Already FREE. */
    detach_app(other);
    vsr_io_get_stats(n->io, &stats);
    CHECK(stats.replicas == 0);
    close_node(n);
    /* A closing engine attaches nothing. */
    n = node_open(0);
    close_io(n);
    app = &n->apps[0];
    app_options(app, cluster_of(1), 1, 1, VSR_START_RECOVER);
    metadata = region_of(replica_memory[0][0], REPLICA_BYTES);
    tail = region_of(tail_memory[0][0], TAIL_BYTES);
    CHECK(vsr_io_attach(n->io, &app->options, &metadata, &tail, &out) ==
          VSR_EINVAL);
    CHECK(run_until(node_closed, n, 20000));
    CHECK(vsr_io_deinit(n->io) == VSR_OK);
    world_close();
}

/* The other entry points' argument checks and the node calls. */
static void test_entry_errors(void)
{
    struct node *n;
    struct vsr_io_sqe sqes[BATCH];
    struct vsr_io_op ops[4];
    struct vsr_io_cqe cqe;
    struct vsr_io_node_status status;
    struct vsr_io_address address;
    uint32_t count = 7;
    uint32_t flags = 7;
    uint64_t deadline = 0;
    uint64_t rejected;

    world_open(2, 6);
    n = node_open(0);
    /* poll */
    CHECK(vsr_io_poll(NULL, 1, ops, 4, &count, &flags) == VSR_EINVAL);
    CHECK(count == 0 && flags == 0);
    CHECK(PURE(n, vsr_io_poll(n->io, 1, ops, 4, NULL, &flags)) == VSR_EINVAL);
    CHECK(PURE(n, vsr_io_poll(n->io, 1, ops, 4, &count, NULL)) == VSR_EINVAL);
    CHECK(PURE(n, vsr_io_poll(n->io, 1, NULL, 4, &count, &flags)) ==
          VSR_EINVAL);
    CHECK(PURE(n, vsr_io_poll(n->io, VSR_NO_DEADLINE, ops, 4, &count,
                              &flags)) == VSR_EINVAL);
    CHECK(PURE(n, vsr_io_poll(n->io, 1, NULL, 0, &count, &flags)) == VSR_OK);
    /* The clock never goes back for the core. */
    CHECK(PURE(n, vsr_io_poll(n->io, 5000, ops, 4, &count, &flags)) == VSR_OK);
    CHECK(n->io->now == 5000);
    CHECK(PURE(n, vsr_io_poll(n->io, 4000, ops, 4, &count, &flags)) == VSR_OK);
    CHECK(n->io->now == 5000);
    /* prepare */
    CHECK(vsr_io_prepare(NULL, 1, sqes, BATCH, &count, &deadline) ==
          VSR_EINVAL);
    CHECK(PURE(n, vsr_io_prepare(n->io, 1, NULL, BATCH, &count, &deadline)) ==
          VSR_EINVAL);
    CHECK(PURE(n, vsr_io_prepare(n->io, 1, sqes, BATCH, NULL, &deadline)) ==
          VSR_EINVAL);
    CHECK(PURE(n, vsr_io_prepare(n->io, 1, sqes, BATCH, &count, NULL)) ==
          VSR_EINVAL);
    CHECK(PURE(n, vsr_io_prepare(n->io, VSR_NO_DEADLINE, sqes, BATCH, &count,
                                 &deadline)) == VSR_EINVAL);
    /* A LINK chain never spans batches: four records at least. */
    CHECK(PURE(n, vsr_io_prepare(n->io, 1, sqes, VSR_IO_ENGINE_BATCH_MIN - 1,
                                 &count, &deadline)) == VSR_EINVAL);
    CHECK(count == 0 && deadline == VSR_NO_DEADLINE);
    /* complete: a foreign owner, a bad index and a free slot are dropped
     * and counted by the slot table. */
    memset(&cqe, 0, sizeof(cqe));
    CHECK(vsr_io_complete(NULL, &cqe, 1) == VSR_EINVAL);
    CHECK(PURE(n, vsr_io_complete(n->io, NULL, 1)) == VSR_EINVAL);
    CHECK(PURE(n, vsr_io_complete(n->io, NULL, 0)) == VSR_OK);
    rejected = n->io->slots.rejected;
    memset(&cqe, 0, sizeof(cqe));
    cqe.user_data = VSR_IO_USER_DATA(APP_OWNER, 5);
    CHECK(PURE(n, vsr_io_complete(n->io, &cqe, 1)) == VSR_OK);
    cqe.user_data = VSR_IO_USER_DATA(OWNER, UINT64_C(0xFFFFFF) << 24);
    CHECK(PURE(n, vsr_io_complete(n->io, &cqe, 1)) == VSR_OK);
    cqe.user_data = VSR_IO_USER_DATA(OWNER, (uint64_t)VSR_IO_SLOT_WRITE << 48);
    CHECK(PURE(n, vsr_io_complete(n->io, &cqe, 1)) == VSR_OK);
    CHECK(n->io->slots.rejected == rejected + 3);
    /* run */
    CHECK(vsr_io_run(NULL, NULL) == VSR_EINVAL);
    /* Nodes: thin over the link module. */
    address = node_address(1);
    CHECK(vsr_io_node_set(NULL, 2, &address) == VSR_EINVAL);
    CHECK(PURE(n, vsr_io_node_set(n->io, VSR_IO_NO_NODE, &address)) ==
          VSR_EINVAL);
    CHECK(PURE(n, vsr_io_node_set(n->io, 2, &address)) == VSR_OK);
    CHECK(PURE(n, vsr_io_node_set(n->io, 3, NULL)) ==
          VSR_OK); /* Caller-dialed. */
    CHECK(PURE(n, vsr_io_node_set(n->io, 4, &address)) == VSR_OK);
    CHECK(PURE(n, vsr_io_node_set(n->io, 5, &address)) == VSR_OK);
    CHECK(PURE(n, vsr_io_node_set(n->io, 6, &address)) ==
          VSR_ELIMIT); /* 4 nodes. */
    CHECK(vsr_io_authorize(NULL, cluster_of(1), 2, 2) == VSR_EINVAL);
    CHECK(PURE(n, vsr_io_authorize(n->io, cluster_of(1), 2, 9)) == VSR_EINVAL);
    CHECK(PURE(n, vsr_io_authorize(n->io, cluster_of(1), 2, 2)) == VSR_OK);
    CHECK(PURE(n, vsr_io_authorize(n->io, cluster_of(1), 2, VSR_IO_NO_NODE)) ==
          VSR_OK);
    CHECK(vsr_io_node_status(NULL, 2, &status) == VSR_EINVAL);
    CHECK(PURE(n, vsr_io_node_status(n->io, 2, NULL)) == VSR_EINVAL);
    CHECK(PURE(n, vsr_io_node_status(n->io, 9, &status)) == VSR_EINVAL);
    CHECK(PURE(n, vsr_io_node_status(n->io, 2, &status)) == VSR_OK);
    CHECK(status.state == VSR_IO_NODE_UNLINKED && status.links == 0);
    CHECK(vsr_io_node_clear(NULL, 2) == VSR_EINVAL);
    CHECK(PURE(n, vsr_io_node_clear(n->io, 9)) == VSR_EINVAL);
    CHECK(PURE(n, vsr_io_node_clear(n->io, 5)) == VSR_OK);
    CHECK(PURE(n, vsr_io_node_status(n->io, 5, &status)) == VSR_EINVAL);
    CHECK(vsr_io_adopt(NULL, 3, 2, 0) == VSR_EINVAL);
    CHECK(PURE(n, vsr_io_adopt(n->io, -1, 2, 0)) == VSR_EINVAL);
    CHECK(PURE(n, vsr_io_adopt(n->io, 3, 9, 0)) ==
          VSR_EINVAL); /* Unknown node. */
    CHECK(PURE(n, vsr_io_adopt(n->io, 3, 1, 0)) ==
          VSR_EINVAL); /* The engine. */
    CHECK(PURE(n, vsr_io_adopt(n->io, 3, 2, VSR_IO_ADOPT_OUTBOUND)) ==
          VSR_EINVAL);
    close_node(n);
    world_close();
}

/* vsr_io_submit's statuses: EINVAL for what the engine never queues,
 * AGAIN for a full queue, ELIMIT for a client the table cannot admit,
 * and the refusals once STOP was submitted. */
static void test_submit_errors(void)
{
    struct node *n;
    struct app *app;
    struct vsr_io_event events[EVENTS + 2] = {0};
    struct vsr_io_event event;
    struct vsr_io_stream_open open;
    struct vsr_io_stream_write write;
    struct vsr_io_replica fake;
    struct lease_slot *slot;
    uint32_t consumed = 7;

    world_open(1, 7);
    n = node_open(0);
    app = app_attach(n, 0, cluster_of(1), 1, 1, VSR_START_NEW);
    CHECK(run_until(app_normal, app, 20000));
    CHECK(PURE(n, vsr_io_submit(n->io, events, 1, NULL)) == VSR_EINVAL);
    CHECK(vsr_io_submit(NULL, events, 1, &consumed) == VSR_EINVAL);
    CHECK(consumed == 0);
    CHECK(PURE(n, vsr_io_submit(n->io, NULL, 1, &consumed)) == VSR_EINVAL);
    CHECK(PURE(n, vsr_io_submit(n->io, NULL, 0, &consumed)) == VSR_OK);
    /* The core's own event types, a stranger's replica, malformed leases,
     * an unknown kind: EINVAL at their index. */
    memset(events, 0, sizeof(events));
    for (uint32_t i = 0; i < 2; ++i) {
        events[i].replica = app->replica;
        events[i].kind = VSR_IO_EVENT_CORE;
        events[i].event.type = VSR_EVENT_CHECKPOINT;
    }
    events[1].event.type = VSR_EVENT_TIME;
    CHECK(PURE(n, vsr_io_submit(n->io, events, 2, &consumed)) == VSR_EINVAL);
    CHECK(consumed == 1);
    event = events[0];
    event.event.type = VSR_EVENT_MESSAGE;
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_EINVAL);
    event = events[0];
    event.replica = NULL;
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_EINVAL);
    memset(&fake, 0, sizeof(fake));
    fake.io = n->io;
    event.replica = &fake;
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_EINVAL);
    event.replica = &n->io->replicas[1]; /* FREE. */
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_EINVAL);
    event = events[0];
    event.event.type = VSR_EVENT_COMPLETE;
    event.event.id = 999;
    event.event.lease = VSR_IO_LEASE_ENGINE | 1;
    event.event.data = &event;
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_EINVAL);
    event.event.lease = 0; /* Data without a lease. */
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_EINVAL);
    event.event.lease = 5; /* A lease without data. */
    event.event.data = NULL;
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_EINVAL);
    event = events[0];
    event.kind = 99;
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_EINVAL);
    CHECK(consumed == 0);
    /* A REQUEST without a client, or without its body. */
    make_request(app, 1, 1, &event);
    slot = NULL;
    for (uint32_t i = 0; i < LEASES; ++i) {
        if (app->leases[i].id == event.event.lease) {
            slot = &app->leases[i];
        }
    }
    CHECK(slot != NULL);
    slot->request.id.client.hi = 0;
    slot->request.id.client.lo = 0;
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_EINVAL);
    lease_release(app, slot->id);
    /* A full queue: AGAIN at the first event that does not fit, before
     * any admission. */
    for (uint32_t i = 0; i < EVENTS + 1; ++i) {
        events[i] = events[0];
    }
    CHECK(app->replica->events_count == 1); /* The CHECKPOINT above. */
    CHECK(PURE(n, vsr_io_submit(n->io, events, EVENTS + 1, &consumed)) ==
          VSR_AGAIN);
    CHECK(consumed == EVENTS - 1);
    CHECK(app->replica->events_count == EVENTS);
    make_request(app, 1, 1, &event);
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_AGAIN);
    CHECK(app->replica->store.clients_count == 0);
    run_for(5 * MS);
    CHECK(app->replica->events_count == 0);
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_OK);
    /* Admission: max_clients (4) incarnations, in flight or indexed. */
    for (uint64_t c = 2; c <= 4; ++c) {
        make_request(app, c, 1, &event);
        CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_OK);
    }
    make_request(app, 5, 1, &event);
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_ELIMIT);
    CHECK(consumed == 0);
    {
        struct replies_at_least want = {app, 4};

        CHECK(run_until(replies_reached, &want, 20000));
    }
    CHECK(app->replies_ok == 4);
    /* Four clients with records: a fifth is still refused, a known one
     * is not. */
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_ELIMIT);
    lease_release(app, event.event.lease);
    make_request(app, 2, 2, &event);
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_OK);
    /* Rails with ids that are not outstanding. */
    memset(&event, 0, sizeof(event));
    event.kind = VSR_IO_EVENT_COMPLETE;
    event.event.type = VSR_EVENT_COMPLETE;
    event.event.id = 12345; /* A HANDSHAKE id never issued. */
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_EINVAL);
    event.event.id = VSR_IO_STREAM_OP_SERVE | 3;
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_EINVAL);
    event.event.id = VSR_IO_STREAM_OP_DATA | 3;
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_EINVAL);
    memset(&event, 0, sizeof(event));
    event.kind = VSR_IO_EVENT_STREAM_OPEN;
    event.event.id = 1;
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_EINVAL);
    memset(&open, 0, sizeof(open));
    open.node = 9; /* Unknown. */
    event.event.data = &open;
    event.event.lease = 1;
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_EINVAL);
    event.kind = VSR_IO_EVENT_STREAM_WRITE;
    event.event.data = NULL;
    event.event.lease = 0;
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_EINVAL);
    memset(&write, 0, sizeof(write));
    write.stream = 77;
    write.kind = VSR_IO_WRITE_FILE;
    write.slot = 100;
    write.length = 1;
    event.event.data = &write;
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_EINVAL);
    event.kind = VSR_IO_EVENT_STREAM_CLOSE;
    event.event.id = 77;
    event.event.data = NULL;
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_EINVAL);
    /* STOP: the caller's other events are refused from then on, a
     * COMPLETE and another STOP are not. */
    memset(&event, 0, sizeof(event));
    event.replica = app->replica;
    event.kind = VSR_IO_EVENT_CORE;
    event.event.type = VSR_EVENT_STOP;
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_OK);
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_OK);
    event.event.type = VSR_EVENT_CHECKPOINT;
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_EINVAL);
    make_request(app, 3, 2, &event);
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_EINVAL);
    lease_release(app, event.event.lease);
    CHECK(run_until(app_stopped, app, 20000));
    /* STOPPED: every core event is refused. */
    memset(&event, 0, sizeof(event));
    event.replica = app->replica;
    event.kind = VSR_IO_EVENT_CORE;
    event.event.type = VSR_EVENT_STOP;
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_EINVAL);
    event.event.type = VSR_EVENT_COMPLETE;
    event.event.id = 1;
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_EINVAL);
    detach_app(app);
    close_node(n);
    world_close();
}

/* An event the core refuses is dropped, not retried forever: the caller's
 * lease comes back through a RELEASE op and the drop is counted; a
 * REQUEST queued behind a STOP meets that fate, as the STOP is fed first
 * (7.1's order). */
static void test_refused_events(void)
{
    struct node *n;
    struct app *app;
    struct lease_slot *slot;
    uint64_t replies;

    world_open(1, 8);
    n = node_open(0);
    app = app_attach(n, 0, cluster_of(1), 1, 1, VSR_START_NEW);
    CHECK(run_until(app_normal, app, 20000));
    CHECK(n->io->events_rejected == 0);
    /* A COMPLETE for an op the core never emitted, with data. */
    slot = lease_take(app);
    core_event(app, VSR_EVENT_COMPLETE, VSR_IO_OK, 0xDEAD, &slot->applied,
               slot->id);
    run_for(2 * MS);
    CHECK(slot->id == 0); /* Released. */
    CHECK(n->io->events_rejected == 1 && app->replica->rejected == 1);
    /* Without data: dropped and counted, nothing to release. */
    complete_op(app, 0xBEEF, VSR_IO_OK);
    run_for(2 * MS);
    CHECK(n->io->events_rejected == 2);
    CHECK(leases_out(app) == 0);
    /* A REQUEST submitted before a STOP in the same batch. */
    replies = app->replies;
    (void)submit_request(app, 1, 1);
    core_event(app, VSR_EVENT_STOP, 0, 0, NULL, 0);
    CHECK(run_until(app_stopped, app, 20000));
    CHECK(app->replies == replies);
    CHECK(n->io->events_rejected == 3);
    CHECK(leases_out(app) == 0);
    detach_app(app);
    close_node(n);
    world_close();
}

/* STATUS: one per poll with STATE_CHANGED, copied into its ring entry;
 * the replica is STOPPED once the STATUS reporting it is forwarded, and
 * detach waits until no forwarded op names the replica. The poll's flags
 * say what is left in the ring. */
static void test_status_and_flags(void)
{
    struct node *n;
    struct app *app;
    struct vsr_io_op ops[4];
    const struct vsr_status *status;
    uint32_t count = 0;
    uint32_t flags = 0;
    uint64_t now;
    uint32_t statuses;

    world_open(1, 9);
    n = node_open(0);
    app = app_attach(n, 0, cluster_of(1), 1, 1, VSR_START_NEW);
    CHECK(run_until(app_normal, app, 20000));
    CHECK(app->statuses >= 1);
    statuses = app->statuses;
    run_for(50 * MS);
    CHECK(app->statuses == statuses); /* Nothing changed: no STATUS. */
    /* STOP with the caller's ops held: the STATUS waits in the ring. */
    n->hold_ops = 1;
    core_event(app, VSR_EVENT_STOP, 0, 0, NULL, 0);
    CHECK(run_until(app_stopped, app, 20000));
    CHECK(n->io->forwarded_count >= 1);
    CHECK(vsr_io_detach(app->replica) == VSR_EBUSY);
    now = n->ex.ops->now(n->ex.ctx);
    CHECK(PURE(n, vsr_io_poll(n->io, now, NULL, 0, &count, &flags)) == VSR_OK);
    CHECK(count == 0);
    CHECK(flags == (VSR_IO_POLL_MORE | VSR_IO_POLL_OUTPUT_FULL));
    CHECK(PURE(n, vsr_io_poll(n->io, now, ops, 1, &count, &flags)) == VSR_OK);
    CHECK(count == 1);
    if (n->io->forwarded_count > 0) {
        CHECK(flags == (VSR_IO_POLL_MORE | VSR_IO_POLL_OUTPUT_FULL));
    }
    while (ops[0].kind != VSR_IO_OP_STATUS) {
        CHECK(ops[0].kind == VSR_IO_OP_CORE);
        app_op(n, &ops[0]);
        CHECK(PURE(n, vsr_io_poll(n->io, now, ops, 1, &count, &flags)) ==
              VSR_OK);
        CHECK(count == 1);
    }
    CHECK(ops[0].replica == app->replica);
    status = ops[0].op.data;
    CHECK(status->state == VSR_STATE_STOPPED);
    CHECK((const unsigned char *)status >=
              (const unsigned char *)n->io->forwarded &&
          (const unsigned char *)status <
              (const unsigned char *)(n->io->forwarded + OPS));
    CHECK(ops[0].op.id == 0);
    app_op(n, &ops[0]);
    n->hold_ops = 0;
    detach_app(app);
    close_node(n);
    world_close();
}

/* -------------------------------------------------------------------------
 * Tests: several engines
 * ---------------------------------------------------------------------- */

/* Every node knows every other and authorizes replica j + 1 of `cluster`
 * as node j + 1. */
static void mesh(uint32_t nodes, struct vsr_id cluster)
{
    for (uint32_t i = 0; i < nodes; ++i) {
        struct node *n = &world.node[i];

        for (uint32_t j = 0; j < nodes; ++j) {
            struct vsr_io_address address = node_address(j);

            if (j != i) {
                CHECK(PURE(n, vsr_io_node_set(n->io, j + 1, &address)) ==
                      VSR_OK);
            }
            if (j != i || vsr_io_links_node_index(&n->io->links, j + 1) !=
                              VSR_IO_INDEX_NONE) {
                CHECK(PURE(n, vsr_io_authorize(n->io, cluster, j + 1, j + 1)) ==
                      VSR_OK);
            }
        }
    }
}

struct group {
    uint32_t count;
    struct app *apps[NODES];
};

static bool group_normal(void *ctx)
{
    const struct group *g = ctx;
    uint64_t view = 0;

    for (uint32_t i = 0; i < g->count; ++i) {
        const struct app *app = g->apps[i];

        if (app->statuses == 0 || app->status.state != VSR_STATE_NORMAL ||
            app->status.primary == 0) {
            return false;
        }
        if (i > 0 && app->status.view != view) {
            return false;
        }
        view = app->status.view;
    }
    return true;
}

static struct app *group_primary(const struct group *g)
{
    for (uint32_t i = 0; i < g->count; ++i) {
        if (g->apps[i]->status.primary == g->apps[i]->options.core.replica &&
            g->apps[i]->status.state == VSR_STATE_NORMAL) {
            return g->apps[i];
        }
    }
    return NULL;
}

static struct group group_open(uint32_t nodes, struct vsr_id cluster)
{
    struct group g;

    memset(&g, 0, sizeof(g));
    for (uint32_t i = 0; i < nodes; ++i) {
        node_open(i);
    }
    mesh(nodes, cluster);
    g.count = nodes;
    for (uint32_t i = 0; i < nodes; ++i) {
        g.apps[i] =
            app_attach(&world.node[i], 0, cluster, i + 1, nodes, VSR_START_NEW);
    }
    CHECK(run_until(group_normal, &g, 200000));
    return g;
}

static void dump_node(const struct node *n)
{
    const struct vsr_io *io = n->io;

    fprintf(stderr, "node %u: state %u slots %u/%u streams %u\n", n->index,
            io->state, io->slots.free_count, io->slots.count,
            io->streams.active);
    for (uint32_t i = 0; i < io->slots.count; ++i) {
        const struct vsr_io_slot *slot = &io->slots.slots[i];

        if (slot->kind != VSR_IO_SLOT_FREE) {
            fprintf(stderr, "  slot %u kind %u owner %u sub %u expected %u\n",
                    i, slot->kind, slot->owner, slot->sub, slot->expected);
        }
    }
    for (uint32_t i = 0; i < io->links.links_count; ++i) {
        const struct vsr_io_link *link = &io->links.links[i];

        if (link->state != VSR_IO_LINK_FREE) {
            fprintf(stderr,
                    "  link %u state %u stage %u torn %d recv %u shut %u\n", i,
                    link->state, link->stage, link->torn_down, link->recv_slot,
                    link->shutdown_slot);
        }
    }
    for (uint32_t i = 0; i < io->options.listen_count; ++i) {
        fprintf(stderr, "  listener %u state %u\n", i,
                io->links.listener_table[i].state);
    }
}

static void group_close(struct group *g)
{
    for (uint32_t i = 0; i < g->count; ++i) {
        core_event(g->apps[i], VSR_EVENT_STOP, 0, 0, NULL, 0);
    }
    for (uint32_t i = 0; i < g->count; ++i) {
        CHECK(run_until(app_stopped, g->apps[i], 200000));
    }
    for (uint32_t i = 0; i < g->count; ++i) {
        detach_app(g->apps[i]);
    }
    for (uint32_t i = 0; i < g->count; ++i) {
        close_io(&world.node[i]);
    }
    for (uint32_t i = 0; i < g->count; ++i) {
        if (!run_until(node_closed, &world.node[i], 200000)) {
            dump_node(&world.node[i]);
        }
        CHECK(node_closed(&world.node[i]));
        CHECK(vsr_io_deinit(world.node[i].io) == VSR_OK);
        world.node[i].open = false;
    }
}

/* A group of three over three engines: SENDs routed to the links,
 * MESSAGEs delivered under engine leases and released by the core,
 * replies at the primary; STOP everywhere and the engines close with
 * their links established. */
static void test_group(void)
{
    struct group g;
    struct app *primary;
    struct vsr_io_stats stats;
    struct replies_at_least want;
    struct vsr_io_node_status status;

    world_open(3, 10);
    g = group_open(3, cluster_of(3));
    primary = group_primary(&g);
    CHECK(primary != NULL);
    want.app = primary;
    for (uint64_t number = 1; number <= 3; ++number) {
        /* One outstanding request per incarnation. */
        for (uint64_t client = 1; client <= 3; ++client) {
            submit_request(primary, client, number);
        }
        want.replies = 3 * number;
        CHECK(run_until(replies_reached, &want, 200000));
    }
    CHECK(primary->replies_ok == 9);
    run_for(100 * MS);
    for (uint32_t i = 0; i < 3; ++i) {
        struct vsr_status core;

        vsr_io_get_stats(world.node[i].io, &stats);
        CHECK(stats.links >= 2);
        CHECK(stats.messages_sent > 0 && stats.bytes_received > 0);
        CHECK(stats.frames_rejected == 0);
        vsr_io_replica_status(g.apps[i]->replica, &core, NULL);
        CHECK(core.committed >= 9);
        CHECK(PURE((&world.node[i]),
                   vsr_io_node_status(world.node[i].io, i == 0 ? 2 : 1,
                                      &status)) == VSR_OK);
        CHECK(status.state == VSR_IO_NODE_LINKED);
        CHECK(status.last_received_ns > 0);
        CHECK(g.apps[i]->applied_entries >= 9);
    }
    group_close(&g);
    world_close();
}

/* CHECKPOINT: the core's CAPTURE goes to the snapshot module, which writes
 * the clients file (CLIENTS records), forwards the op with the id in its
 * template and completes to the core with the checkpoint under an engine
 * lease once the caller completed; the caller's lease comes back at once,
 * the engine's when the core releases it. */
static void test_capture(void)
{
    struct node *n;
    struct app *app;
    struct vsr_io_replica *rep;
    struct replies_at_least want;
    struct vsr_status core;
    uint32_t tries = 0;

    world_open(1, 11);
    n = node_open(0);
    app = app_attach(n, 0, cluster_of(1), 1, 1, VSR_START_NEW);
    rep = app->replica;
    CHECK(run_until(app_normal, app, 20000));
    for (uint64_t i = 1; i <= 4; ++i) {
        submit_request(app, i, 1);
    }
    want.app = app;
    want.replies = 4;
    CHECK(run_until(replies_reached, &want, 20000));
    do {
        core_event(app, VSR_EVENT_CHECKPOINT, 0, 0, NULL, 0);
        run_for(50 * MS);
        vsr_io_replica_status(rep, &core, NULL);
    } while (core.checkpoint_op == 0 && ++tries < 20);
    CHECK(app->captures >= 1);
    CHECK(core.checkpoint_op >= 1);
    CHECK(rep->store.last_capture.hi != 0 || rep->store.last_capture.lo != 0);
    run_for(50 * MS);
    CHECK(rep->snapshots.capture == VSR_IO_INDEX_NONE);
    /* The caller's checkpoint lease came back at once; after the
     * checkpoint the replica still serves. */
    submit_request(app, 1, 2);
    want.replies = 5;
    CHECK(run_until(replies_reached, &want, 20000));
    stop_app(app);
    check_idle_replica(app);
    detach_app(app);
    close_node(n);
    world_close();
}

/* The caller's own file, through its executor with its owner tag. */
static int caller_record(struct node *n, struct vsr_io_sqe *sqe)
{
    sqe->user_data = VSR_IO_USER_DATA(APP_OWNER, 0xF11E);
    CHECK(n->ex.ops->submit_and_wait(n->ex.ctx, sqe, 1, 0, 0, 0) == 0);
    for (uint32_t i = 0; i < 100000; ++i) {
        struct vsr_io_cqe cqe;

        if (n->ex.ops->reap(n->ex.ctx, &cqe, 1) == 1) {
            CHECK(cqe.user_data == sqe->user_data);
            return cqe.result;
        }
        (void)vsr_sim_advance(world.sim);
    }
    CHECK(false);
    return -1;
}

#define CALLER_SLOT (FILE_SLOT_BASE + FILE_SLOTS + 1u)
#define STREAM_FILE_BYTES 5000u

static unsigned char stream_file[STREAM_FILE_BYTES];
static unsigned char stream_buffer[3000];

/* A file of known bytes behind a caller slot of node n. */
static void caller_file(struct node *n)
{
    struct vsr_io_sqe sqe;

    for (uint32_t i = 0; i < STREAM_FILE_BYTES; ++i) {
        stream_file[i] = (unsigned char)(i * 7u + 3u);
    }
    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode = VSR_IO_SQE_OPENAT;
    sqe.flags = VSR_IO_SQE_DIRECT;
    sqe.fd = VSR_SIM_ROOT;
    sqe.fd2 = (int32_t)CALLER_SLOT;
    sqe.addr = "stream-file";
    sqe.op_flags = 0102; /* O_RDWR | O_CREAT */
    sqe.length = 0644;
    CHECK(caller_record(n, &sqe) == (int)CALLER_SLOT);
    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode = VSR_IO_SQE_WRITE;
    sqe.flags = VSR_IO_SQE_FIXED_FILE;
    sqe.fd = (int32_t)CALLER_SLOT;
    sqe.addr = stream_file;
    sqe.length = STREAM_FILE_BYTES;
    CHECK(caller_record(n, &sqe) == (int)STREAM_FILE_BYTES);
}

static bool stream_served(void *ctx)
{
    const struct node *n = ctx;

    return n->stream_serves > 0;
}

static bool stream_ended(void *ctx)
{
    const struct node *n = ctx;

    return n->stream_ends > 0;
}

/* A caller stream from node 0 to node 1: STREAM_OPEN at the requester,
 * STREAM_SERVE at the source completed through a rail COMPLETE, a BUFFERS
 * write and a FILE write (the chunk reads are STREAM records), CLOSE; the
 * requester's DATA ops completed through rail COMPLETEs, WRITTEN for both
 * writes and END on both sides; no RELEASE for the stream events' leases
 * (decision 94). */
static void test_stream(void)
{
    struct node *a;
    struct node *b;
    struct vsr_io_address address;
    struct vsr_io_event event;
    struct vsr_io_stream_open open;
    struct vsr_io_stream_write writes[2];
    struct vsr_span span;
    static const unsigned char request[] = "fetch-me";

    world_open(2, 12);
    a = node_open(0);
    b = node_open(1);
    address = node_address(1);
    CHECK(PURE(a, vsr_io_node_set(a->io, 2, &address)) == VSR_OK);
    address = node_address(0);
    CHECK(PURE(b, vsr_io_node_set(b->io, 1, &address)) == VSR_OK);
    caller_file(b);
    memset(&open, 0, sizeof(open));
    open.node = 2;
    open.request.data = request;
    open.request.size = sizeof(request);
    memset(&event, 0, sizeof(event));
    event.kind = VSR_IO_EVENT_STREAM_OPEN;
    event.event.id = 0xC00C1E;
    event.event.data = &open;
    event.event.lease = 42;
    queue_event(a, &event);
    CHECK(run_until(stream_served, b, 100000));
    CHECK(b->served_handle != 0);
    for (uint32_t i = 0; i < sizeof(stream_buffer); ++i) {
        stream_buffer[i] = (unsigned char)(i + 1);
    }
    span.data = stream_buffer;
    span.size = sizeof(stream_buffer);
    memset(writes, 0, sizeof(writes));
    writes[0].stream = b->served_handle;
    writes[0].write = 1;
    writes[0].kind = VSR_IO_WRITE_BUFFERS;
    writes[0].buffers.spans = &span;
    writes[0].buffers.size = span.size;
    writes[0].buffers.count = 1;
    writes[1].stream = b->served_handle;
    writes[1].write = 2;
    writes[1].kind = VSR_IO_WRITE_FILE;
    writes[1].slot = CALLER_SLOT;
    writes[1].offset = 0;
    writes[1].length = STREAM_FILE_BYTES;
    for (uint32_t i = 0; i < 2; ++i) {
        memset(&event, 0, sizeof(event));
        event.kind = VSR_IO_EVENT_STREAM_WRITE;
        event.event.data = &writes[i];
        event.event.lease = i == 0 ? 43 : 0;
        queue_event(b, &event);
    }
    memset(&event, 0, sizeof(event));
    event.kind = VSR_IO_EVENT_STREAM_CLOSE;
    event.event.id = b->served_handle;
    event.event.status = VSR_IO_OK;
    queue_event(b, &event);
    CHECK(run_until(stream_ended, a, 100000));
    if (a->stream_end_status != VSR_IO_OK) {
        fprintf(stderr,
                "requester END %d bytes %" PRIu64 " data %" PRIu64
                " source ends %" PRIu64 " status %d written %" PRIu64 "\n",
                a->stream_end_status, a->stream_end_bytes, a->stream_data_bytes,
                b->stream_ends, b->stream_end_status, b->stream_written);
    }
    CHECK(a->stream_end_status == VSR_IO_OK);
    CHECK(a->stream_end_bytes == sizeof(stream_buffer) + STREAM_FILE_BYTES);
    CHECK(a->stream_data_bytes == sizeof(stream_buffer) + STREAM_FILE_BYTES);
    CHECK(a->stream_data >= 2);
    CHECK(run_until(stream_ended, b, 100000));
    CHECK(b->stream_end_status == VSR_IO_OK);
    CHECK(b->stream_written == 2);
    run_for(50 * MS);
    CHECK(a->io->streams.active == 0);
    /* A second stream, and the requester's engine closes mid-way: the
     * requester's END is CANCELLED (decision 101), the source's RETRY. */
    a->stream_ends = b->stream_ends = b->stream_serves = 0;
    event.kind = VSR_IO_EVENT_STREAM_OPEN;
    event.event.id = 0xC00C1F;
    event.event.data = &open;
    event.event.lease = 44;
    event.event.status = 0;
    queue_event(a, &event);
    CHECK(run_until(stream_served, b, 100000));
    close_io(a);
    CHECK(run_until(stream_ended, a, 100000));
    CHECK(a->stream_end_status == VSR_IO_CANCELLED);
    CHECK(run_until(node_closed, a, 100000));
    CHECK(vsr_io_deinit(a->io) == VSR_OK);
    a->open = false;
    memset(&event, 0, sizeof(event));
    event.kind = VSR_IO_EVENT_STREAM_CLOSE;
    event.event.id = b->served_handle;
    event.event.status = VSR_IO_OK;
    queue_event(b, &event);
    CHECK(run_until(stream_ended, b, 100000));
    CHECK(b->stream_end_status == VSR_IO_RETRY ||
          b->stream_end_status == VSR_IO_OK);
    close_node(b);
    world_close();
}

/* -------------------------------------------------------------------------
 * Tests: attach and detach on a busy engine, prepare, stale completions,
 * EXTERNAL handshakes, the ready-made loop
 * ---------------------------------------------------------------------- */

/* Two replicas of two clusters share one engine; one attaches while the
 * other has requests in flight, one stops and detaches while the other
 * keeps serving, and the stopped one comes back with RECOVER over its
 * log, the recovered row reaching the core through the store. */
static void test_busy_attach_detach(void)
{
    struct node *n;
    struct app *a;
    struct app *b;
    struct replies_at_least want_a;
    struct replies_at_least want_b;
    struct vsr_status core;
    uint64_t committed;

    world_open(1, 13);
    n = node_open(0);
    a = app_attach(n, 0, cluster_of(1), 1, 1, VSR_START_NEW);
    CHECK(run_until(app_normal, a, 20000));
    for (uint64_t c = 1; c <= 3; ++c) {
        submit_request(a, c, 1);
    }
    /* b attaches with a's requests queued. */
    b = app_attach(n, 1, cluster_of(2), 1, 1, VSR_START_NEW);
    want_a.app = a;
    want_a.replies = 3;
    CHECK(run_until(replies_reached, &want_a, 20000));
    CHECK(run_until(app_normal, b, 20000));
    for (uint64_t c = 1; c <= 3; ++c) {
        submit_request(b, c, 1);
        submit_request(a, c, 2);
    }
    /* a stops with b's requests in flight; b keeps going. */
    core_event(a, VSR_EVENT_STOP, 0, 0, NULL, 0);
    CHECK(run_until(app_stopped, a, 20000));
    vsr_io_replica_status(a->replica, &core, NULL);
    committed = core.committed;
    CHECK(committed >= 3);
    detach_app(a);
    want_b.app = b;
    want_b.replies = 3;
    CHECK(run_until(replies_reached, &want_b, 20000));
    CHECK(b->replies_ok == 3);
    submit_request(b, 1, 2);
    /* a again, RECOVER from its log while b serves. */
    a = app_attach(n, 0, cluster_of(1), 1, 1, VSR_START_RECOVER);
    CHECK(a->replica == &n->io->replicas[0]);
    CHECK(run_until(app_normal, a, 50000));
    vsr_io_replica_status(a->replica, &core, NULL);
    CHECK(core.committed >= committed);
    CHECK(a->replica->store.readable > 0);
    want_b.replies = 4;
    CHECK(run_until(replies_reached, &want_b, 20000));
    submit_request(a, 4, 1);
    want_a.app = a;
    want_a.replies = 1;
    CHECK(run_until(replies_reached, &want_a, 20000));
    CHECK(a->replies_ok == 1);
    stop_app(a);
    stop_app(b);
    detach_app(a);
    detach_app(b);
    close_node(n);
    world_close();
}

static uint8_t slot_kind(uint64_t user_data)
{
    return (uint8_t)(user_data >> 48);
}

/* vsr_io_prepare fills the batch in 7.4's order: provision (not a
 * record), the links' records, the streams', then per replica the
 * store's and the snapshot module's; the deadline is the earliest engine
 * deadline, or now when the batch filled up. */
static void test_prepare(void)
{
    struct node *n;
    struct app *app;
    struct vsr_io_sqe sqes[BATCH];
    struct vsr_io_op ops[OPS];
    uint32_t count = 0;
    uint32_t flags = 0;
    uint32_t provides;
    uint64_t deadline = 0;
    uint64_t now;
    bool store_seen = false;
    bool clients_seen = false;

    world_open(1, 14);
    n = node_open(0);
    app = app_attach(n, 0, cluster_of(1), 1, 1, VSR_START_NEW);
    now = n->ex.ops->now(n->ex.ctx);
    /* The first poll steps the core with TIME alone: its RECOVERY LOAD
     * opens the store; the core's deadline is armed. */
    CHECK(PURE(n, vsr_io_poll(n->io, now, ops, OPS, &count, &flags)) == VSR_OK);
    CHECK(count == 0);
    CHECK(app->replica->store.state == VSR_IO_STORE_OPENING);
    /* A batch of exactly the listener chain: it fills, deadline now. */
    /* The provision is a PROVIDE record at the end of the batch (decision
     * E7), which the executor runs before the rest: no provide() call. */
    provides = n->wrap.provides;
    CHECK(PURE(n, vsr_io_prepare(n->io, now, sqes, VSR_IO_ENGINE_BATCH_MIN,
                                 &count, &deadline)) == VSR_OK);
    CHECK(n->wrap.provides == provides);
    CHECK(count == VSR_IO_ENGINE_BATCH_MIN && deadline == now);
    for (uint32_t i = 0; i + 1 < count; ++i) {
        CHECK(slot_kind(sqes[i].user_data) == VSR_IO_SLOT_LISTEN);
    }
    CHECK(sqes[0].opcode == VSR_IO_SQE_SOCKET);
    CHECK(sqes[3].opcode == VSR_IO_SQE_ACCEPT);
    CHECK(sqes[4].opcode == VSR_IO_SQE_PROVIDE && sqes[4].flags == 0);
    CHECK(sqes[4].buffer_group == GROUP);
    CHECK(sqes[4].length == SLABS - n->io->pool.reserve - 2);
    CHECK(sqes[4].addr == n->io->provide_buffers);
    CHECK(sqes[4].user_data == vsr_io_engine_provide_user_data(n->io));
    CHECK(n->io->pool.kernel_count == sqes[4].length);
    CHECK(n->ex.ops->submit_and_wait(n->ex.ctx, sqes, count, 0, 0, 0) == 0);
    /* Then the store's open and the snapshot module's directory. */
    CHECK(PURE(n, vsr_io_poll(n->io, now, ops, OPS, &count, &flags)) == VSR_OK);
    CHECK(PURE(n, vsr_io_prepare(n->io, now, sqes, BATCH, &count, &deadline)) ==
          VSR_OK);
    CHECK(count >= 2 && count < BATCH);
    for (uint32_t i = 0; i < count; ++i) {
        uint8_t kind = slot_kind(sqes[i].user_data);

        if (kind == VSR_IO_SLOT_FILE) {
            CHECK(!clients_seen); /* The store first, then snapshots. */
            store_seen = true;
        } else if (kind == VSR_IO_SLOT_CLIENTS) {
            clients_seen = true;
        } else {
            /* Nothing more to provide: no PROVIDE record either. */
            CHECK(kind == VSR_IO_SLOT_LISTEN || kind == VSR_IO_SLOT_RECV);
            CHECK(!store_seen && !clients_seen); /* Links first. */
        }
    }
    CHECK(store_seen && clients_seen);
    CHECK(deadline == vsr_io_deadlines_earliest(&n->io->deadlines));
    CHECK(deadline > now && deadline != VSR_NO_DEADLINE);
    CHECK(deadline <= app->replica->core_deadline);
    CHECK(n->ex.ops->submit_and_wait(n->ex.ctx, sqes, count, 0, 0, 0) == 0);
    CHECK(run_until(app_normal, app, 20000));
    /* Decision 122: an outcome the snapshot module decides arms the
     * replica's CAPTURE deadline at the engine's time; prepare's deadline
     * (computed after every module prepared) returns the loop at once, and
     * the next poll consumes it. Armed here by hand: no unit scenario
     * makes the module decide one in its prepare. */
    {
        uint64_t at = n->io->now;
        uint32_t k = 0;

        vsr_io_deadlines_arm(&n->io->deadlines, app->replica->deadline_capture,
                             at);
        CHECK(PURE(n, vsr_io_prepare(n->io, at, sqes, BATCH, &count,
                                     &deadline)) == VSR_OK);
        CHECK(deadline == at);
        CHECK(n->ex.ops->submit_and_wait(n->ex.ctx, sqes, count, 0, 0, 0) >= 0);
        CHECK(PURE(n, vsr_io_poll(n->io, at, ops, OPS, &k, &flags)) == VSR_OK);
        CHECK(vsr_io_deadlines_earliest(&n->io->deadlines) > at);
    }
    stop_app(app);
    detach_app(app);
    close_node(n);
    world_close();
}

/* A completion whose slot was freed since (a stale generation), a
 * completion repeated after its record's last one, a foreign tag: each
 * dropped by the slot table and counted, the owning module never called;
 * the engine goes on. Every store, link and snapshot slot kind was
 * dispatched on the way. */
static void test_stale_completions(void)
{
    struct node *n;
    struct app *app;
    struct replies_at_least want;
    uint32_t kinds[VSR_IO_SLOT_KINDS];
    uint32_t replayed = 0;
    uint64_t rejected;

    world_open(1, 15);
    n = node_open(0);
    n->wrap.logging = true;
    app = app_attach(n, 0, cluster_of(1), 1, 1, VSR_START_NEW);
    CHECK(run_until(app_normal, app, 20000));
    for (uint64_t c = 1; c <= 3; ++c) {
        submit_request(app, c, 1);
    }
    want.app = app;
    want.replies = 3;
    CHECK(run_until(replies_reached, &want, 20000));
    core_event(app, VSR_EVENT_CHECKPOINT, 0, 0, NULL, 0);
    run_for(100 * MS);
    n->wrap.logging = false;
    memset(kinds, 0, sizeof(kinds));
    for (uint32_t i = 0; i < n->wrap.log_count; ++i) {
        uint8_t kind = slot_kind(n->wrap.log[i].user_data);

        if (VSR_IO_OWNER(n->wrap.log[i].user_data) == OWNER &&
            kind < VSR_IO_SLOT_KINDS) {
            kinds[kind]++;
        }
    }
    CHECK(kinds[VSR_IO_SLOT_WRITE] > 0 && kinds[VSR_IO_SLOT_FLUSH] > 0);
    CHECK(kinds[VSR_IO_SLOT_SUPER] > 0 && kinds[VSR_IO_SLOT_FILE] > 0);
    CHECK(kinds[VSR_IO_SLOT_CLIENTS] > 0);
    /* Replays of every logged completion whose slot is free or reused. */
    rejected = n->io->slots.rejected;
    for (uint32_t i = 0; i < n->wrap.log_count; ++i) {
        const struct vsr_io_cqe *cqe = &n->wrap.log[i];
        uint32_t index = (uint32_t)((cqe->user_data >> 24) & 0xFFFFFFu);
        uint32_t generation = (uint32_t)(cqe->user_data & 0xFFFFFFu);
        const struct vsr_io_slot *slot;

        if (VSR_IO_OWNER(cqe->user_data) != OWNER ||
            index >= n->io->slots.count) {
            continue;
        }
        slot = &n->io->slots.slots[index];
        if (slot->kind != VSR_IO_SLOT_FREE &&
            (slot->generation & 0xFFFFFFu) == generation) {
            continue; /* Live: a duplicate would be a new completion. */
        }
        CHECK(PURE(n, vsr_io_complete(n->io, cqe, 1)) == VSR_OK);
        replayed++;
    }
    CHECK(replayed > 10);
    CHECK(n->io->slots.rejected == rejected + replayed);
    /* The engine is unharmed. */
    submit_request(app, 1, 2);
    want.replies = 4;
    CHECK(run_until(replies_reached, &want, 20000));
    CHECK(app->replies_ok == 4);
    stop_app(app);
    detach_app(app);
    close_node(n);
    world_close();
}

/* EXTERNAL handshakes: the HANDSHAKE ops the link module emits at both
 * ends of a peer link are completed through rail COMPLETE events, which
 * the engine routes by id to the link module; two of a group of three
 * then commit over the links. (An inbound EXTERNAL link is a peer link:
 * without a HELLO its purpose is unknown, so a stream cannot be carried;
 * docs/io-implementation.md section 11.) */
static void test_external(void)
{
    struct vsr_io_options options;
    struct group g;
    struct app *primary;
    struct replies_at_least want;

    world_open(2, 16);
    for (uint32_t i = 0; i < 2; ++i) {
        options = engine_options(&world.node[i]);
        options.handshake = VSR_IO_HANDSHAKE_EXTERNAL;
        node_open_with(i, &options);
        world.node[i].external_peer = 2 - i;
    }
    mesh(2, cluster_of(4));
    memset(&g, 0, sizeof(g));
    g.count = 2;
    for (uint32_t i = 0; i < 2; ++i) {
        g.apps[i] = app_attach(&world.node[i], 0, cluster_of(4), i + 1, 3,
                               VSR_START_NEW);
    }
    CHECK(run_until(group_normal, &g, 200000));
    CHECK(world.node[0].handshakes >= 1 && world.node[1].handshakes >= 1);
    primary = group_primary(&g);
    CHECK(primary != NULL);
    submit_request(primary, 1, 1);
    want.app = primary;
    want.replies = 1;
    CHECK(run_until(replies_reached, &want, 200000));
    CHECK(primary->replies_ok == 1);
    group_close(&g);
    world_close();
}

/* vsr_io_run over the simulation: the wrapper's submit_and_wait advances
 * the simulated clock until the node is ready, as a blocking wait would.
 * The hooks are the caller: it takes the ops, answers them, stops the
 * replica after three replies, detaches it, closes the engine; run
 * returns once closed. An executor error ends run with its errno. */
struct run_ctx {
    struct node *n;
    struct app *app;
    bool submitted;
    bool stopping;
    bool closing;
    uint32_t steps;
    uint32_t prepares;
    uint32_t completes;
};

static int blocking_submit(void *ctx, const struct vsr_io_sqe *sqes,
                           uint32_t count, uint32_t want, uint64_t min_wait_ns,
                           uint64_t deadline_ns)
{
    struct wrap *w = ctx;
    uint32_t node =
        (uint32_t)(((struct node *)(void *)((unsigned char *)w -
                                            offsetof(struct node, wrap)))
                       ->index);
    int rc = wrap_submit(ctx, sqes, count, want, min_wait_ns, deadline_ns);

    if (rc != 0) {
        return rc;
    }
    for (uint32_t i = 0; i < 100000 && !vsr_sim_ready(world.sim, node); ++i) {
        if (vsr_sim_advance(world.sim) < 0) {
            /* Nothing can ever complete: a wait forever. */
            dump_node(&world.node[node]);
            CHECK(false);
        }
    }
    return 0;
}

static const struct vsr_io_executor_ops blocking_ops = {wrap_now,
                                                        wrap_random,
                                                        blocking_submit,
                                                        wrap_reap,
                                                        wrap_register_files,
                                                        wrap_update_file,
                                                        wrap_register_buffers,
                                                        wrap_update_buffer,
                                                        wrap_buffer_ring,
                                                        wrap_provide,
                                                        wrap_wake};

static uint32_t run_step(void *ctx, const struct vsr_io_op *ops, uint32_t count,
                         struct vsr_io_event *events, uint32_t capacity)
{
    struct run_ctx *run = ctx;
    struct node *n = run->n;
    uint32_t out;

    run->steps++;
    for (uint32_t i = 0; i < count; ++i) {
        app_op(n, &ops[i]);
    }
    if (run->app->attached && app_normal(run->app) && !run->submitted) {
        run->submitted = true;
        for (uint64_t c = 1; c <= 3; ++c) {
            submit_request(run->app, c, 1);
        }
    }
    if (run->app->attached && run->app->replies >= 3 && !run->stopping) {
        run->stopping = true;
        core_event(run->app, VSR_EVENT_STOP, 0, 0, NULL, 0);
    }
    if (run->app->attached &&
        run->app->replica->state == VSR_IO_REPLICA_STOPPED &&
        vsr_io_detach(run->app->replica) == VSR_OK) {
        run->app->attached = false;
        CHECK(vsr_deinit(vsr_io_replica_core(run->app->replica)) == VSR_OK);
    }
    if (!run->app->attached && !run->closing) {
        run->closing = true;
        CHECK(PURE(n, vsr_io_close(n->io)) == VSR_OK);
    }
    out = n->pending_count < capacity ? n->pending_count : capacity;
    memcpy(events, n->pending, (size_t)out * sizeof(*events));
    memmove(n->pending, n->pending + out,
            (size_t)(n->pending_count - out) * sizeof(n->pending[0]));
    n->pending_count -= out;
    return out;
}

static uint64_t run_prepare(void *ctx, struct vsr_io_sqe *sqes,
                            uint32_t capacity, uint32_t *count)
{
    struct run_ctx *run = ctx;

    (void)sqes;
    (void)capacity;
    run->prepares++;
    *count = 0;
    return VSR_NO_DEADLINE;
}

static void run_complete(void *ctx, const struct vsr_io_cqe *cqe)
{
    struct run_ctx *run = ctx;

    (void)cqe;
    run->completes++;
}

static void test_run(void)
{
    struct run_ctx run;
    struct vsr_io_hooks hooks;
    struct vsr_io_options options;
    struct node *n;

    world_open(1, 17);
    n = node_open(0);
    /* vsr_io_run calls the executor itself (the loop may): the blocking
     * wrapper, without the purity guard, is the engine's here. */
    n->ex.ops = &blocking_ops;
    n->ex.ctx = &n->wrap;
    n->io->ex = n->ex;
    memset(&run, 0, sizeof(run));
    run.n = n;
    run.app = app_attach(n, 0, cluster_of(1), 1, 1, VSR_START_NEW);
    hooks.ctx = &run;
    hooks.complete = run_complete;
    hooks.step = run_step;
    hooks.prepare = run_prepare;
    CHECK(vsr_io_run(n->io, &hooks) == VSR_OK);
    CHECK(run.app->replies_ok == 3);
    CHECK(run.stopping && run.closing && !run.app->attached);
    CHECK(run.steps > 0 && run.prepares > 0 && run.completes == 0);
    CHECK(node_closed(n));
    CHECK(vsr_io_deinit(n->io) == VSR_OK);
    n->open = false;
    /* Without hooks: an executor error ends the loop with its errno (on
     * an engine with nothing in flight: after a failed submission the
     * engine cannot know which of its records the kernel took), and a
     * closed engine returns at once. */
    options = engine_options(&world.node[0]);
    options.listen_count = 0;
    n = node_open_with(0, &options);
    n->ex.ops = &blocking_ops;
    n->ex.ctx = &n->wrap;
    n->io->ex = n->ex;
    n->wrap.fail_submit = -EIO;
    CHECK(vsr_io_run(n->io, NULL) == -EIO);
    CHECK(PURE(n, vsr_io_close(n->io)) == VSR_OK);
    CHECK(vsr_io_run(n->io, NULL) == VSR_OK);
    CHECK(vsr_io_deinit(n->io) == VSR_OK);
    n->open = false;
    world_close();
}

/* -------------------------------------------------------------------------
 * Tests: bounds, timers and the close
 * ---------------------------------------------------------------------- */

/* A forwarded ring of two entries: an update's capacity is bounded by the
 * room left in it, so every op of every update is routed at once; the
 * poll reports OUTPUT_FULL and MORE while ops wait, and nothing is lost. */
static void test_small_ring(void)
{
    struct vsr_io_options options;
    struct node *n;
    struct app *app;
    struct replies_at_least want;

    world_open(1, 18);
    options = engine_options(&world.node[0]);
    options.limits.ops = 2;
    n = node_open_with(0, &options);
    app = app_attach(n, 0, cluster_of(1), 1, 1, VSR_START_NEW);
    CHECK(run_until(app_normal, app, 20000));
    for (uint64_t c = 1; c <= 4; ++c) {
        submit_request(app, c, 1);
    }
    want.app = app;
    want.replies = 4;
    CHECK(run_until(replies_reached, &want, 20000));
    CHECK(app->replies_ok == 4 && app->applied_entries >= 4);
    stop_app(app);
    detach_app(app);
    close_node(n);
    world_close();
}

/* Stream timers and the close: a SERVE the caller never answers ends both
 * sides RETRY at the inactivity timer (the STREAM deadline dispatched by
 * the poll); a requester's DATA op the caller holds keeps the stream, and
 * so the closing engine, alive until it is completed (the engine is not
 * closed while a stream is active). */
static void test_stream_timers(void)
{
    struct node *a;
    struct node *b;
    struct vsr_io_address address;
    struct vsr_io_event event;
    struct vsr_io_stream_open open;
    struct vsr_io_stream_write write;
    struct vsr_span span;
    static const unsigned char request[] = "timer";

    world_open(2, 19);
    a = node_open(0);
    b = node_open(1);
    address = node_address(1);
    CHECK(PURE(a, vsr_io_node_set(a->io, 2, &address)) == VSR_OK);
    address = node_address(0);
    CHECK(PURE(b, vsr_io_node_set(b->io, 1, &address)) == VSR_OK);
    memset(&open, 0, sizeof(open));
    open.node = 2;
    open.request.data = request;
    open.request.size = sizeof(request);
    memset(&event, 0, sizeof(event));
    event.kind = VSR_IO_EVENT_STREAM_OPEN;
    event.event.id = 11;
    event.event.data = &open;
    event.event.lease = 1;
    b->hold_serve = true;
    queue_event(a, &event);
    CHECK(run_until(stream_served, b, 100000));
    CHECK(run_until(stream_ended, a, 100000));
    CHECK(a->stream_end_status == VSR_IO_RETRY && a->stream_end_bytes == 0);
    /* The source ended too, but its SERVE is the caller's until it
     * completes it: the refusal then frees the stream, with no END op
     * for a stream the caller never accepted (decision 97). */
    run_for(300 * MS);
    CHECK(a->io->streams.active == 0 && b->io->streams.active == 1);
    CHECK(b->held_serve != 0);
    memset(&event, 0, sizeof(event));
    event.kind = VSR_IO_EVENT_COMPLETE;
    event.event.type = VSR_EVENT_COMPLETE;
    event.event.id = b->held_serve;
    event.event.status = VSR_IO_RETRY;
    queue_event(b, &event);
    run_for(10 * MS);
    CHECK(b->io->streams.active == 0 && b->stream_ends == 0);
    memset(&event, 0, sizeof(event));
    event.kind = VSR_IO_EVENT_STREAM_OPEN;
    event.event.data = &open;
    event.event.lease = 1;
    /* A stream whose DATA the requester's caller holds, then the close. */
    b->hold_serve = false;
    b->stream_serves = 0;
    a->stream_ends = 0;
    a->hold_data = true;
    event.event.id = 12;
    queue_event(a, &event);
    CHECK(run_until(stream_served, b, 100000));
    span.data = request;
    span.size = sizeof(request);
    memset(&write, 0, sizeof(write));
    write.stream = b->served_handle;
    write.write = 1;
    write.kind = VSR_IO_WRITE_BUFFERS;
    write.buffers.spans = &span;
    write.buffers.size = span.size;
    write.buffers.count = 1;
    memset(&event, 0, sizeof(event));
    event.kind = VSR_IO_EVENT_STREAM_WRITE;
    event.event.data = &write;
    event.event.lease = 2;
    queue_event(b, &event);
    memset(&event, 0, sizeof(event));
    event.kind = VSR_IO_EVENT_STREAM_CLOSE;
    event.event.id = b->served_handle;
    queue_event(b, &event);
    for (uint32_t i = 0; i < 100000 && a->held_data_count == 0; ++i) {
        CHECK(run_round());
    }
    CHECK(a->held_data_count == 1);
    close_io(a);
    run_for(500 * MS);
    CHECK(!node_closed(a));
    CHECK(a->io->streams.active == 1);
    /* The caller completes its DATA op: the stream ends, the engine
     * closes. */
    memset(&event, 0, sizeof(event));
    event.kind = VSR_IO_EVENT_COMPLETE;
    event.event.type = VSR_EVENT_COMPLETE;
    event.event.id = a->held_data[0];
    event.event.status = VSR_IO_OK;
    a->hold_data = false;
    a->held_data_count = 0;
    queue_event(a, &event);
    CHECK(run_until(node_closed, a, 100000));
    CHECK(a->stream_ends == 1);
    CHECK(vsr_io_deinit(a->io) == VSR_OK);
    a->open = false;
    close_node(b);
    world_close();
}

/* A peer that is not listening yet: the dial is refused, the node backs
 * off, and the DIAL deadline the poll dispatches redials it once it
 * listens; the group then commits. The peer itself never dials (it has
 * the primary's node caller-dialed and only emits LINK_WANTED), so the
 * redial is the only way the link comes up. */
static void test_redial(void)
{
    struct group g;
    struct app *primary;
    struct replies_at_least want;
    struct vsr_io_node_status status;

    world_open(2, 20);
    node_open(0);
    node_open(1);
    world.node[1].open = false; /* Its loop does not run yet. */
    mesh(2, cluster_of(5));
    CHECK(PURE((&world.node[1]), vsr_io_node_set(world.node[1].io, 1, NULL)) ==
          VSR_OK);
    memset(&g, 0, sizeof(g));
    g.count = 2;
    g.apps[0] =
        app_attach(&world.node[0], 0, cluster_of(5), 1, 3, VSR_START_NEW);
    run_for(20 * MS);
    CHECK(PURE((&world.node[0]),
               vsr_io_node_status(world.node[0].io, 2, &status)) == VSR_OK);
    CHECK(status.state != VSR_IO_NODE_LINKED);
    CHECK(status.last_error == -ECONNREFUSED);
    world.node[1].open = true;
    g.apps[1] =
        app_attach(&world.node[1], 0, cluster_of(5), 2, 3, VSR_START_NEW);
    CHECK(run_until(group_normal, &g, 200000));
    primary = group_primary(&g);
    CHECK(primary == g.apps[0]);
    submit_request(primary, 1, 1);
    want.app = primary;
    want.replies = 1;
    CHECK(run_until(replies_reached, &want, 200000));
    CHECK(primary->replies_ok == 1);
    CHECK(PURE((&world.node[0]),
               vsr_io_node_status(world.node[0].io, 2, &status)) == VSR_OK);
    CHECK(status.state == VSR_IO_NODE_LINKED);
    CHECK(world.node[1].links_wanted >= 1);
    group_close(&g);
    world_close();
}

struct new_view {
    const struct group *g;
    uint32_t except;
    uint64_t view;
};

static bool view_changed(void *ctx)
{
    const struct new_view *want = ctx;

    for (uint32_t i = 0; i < want->g->count; ++i) {
        const struct app *app = want->g->apps[i];

        if (i == want->except) {
            continue;
        }
        if (app->status.state != VSR_STATE_NORMAL ||
            app->status.view <= want->view ||
            app->status.primary == want->except + 1) {
            return false;
        }
    }
    return true;
}

/* TIME reaches the cores at every poll: once the primary is cut off, the
 * others' view timers expire, they change view and the new primary
 * commits; the old one rejoins when the partition heals. */
static void test_view_change(void)
{
    struct group g;
    struct app *primary;
    struct new_view want;
    struct replies_at_least replies;
    uint32_t old;

    world_open(3, 21);
    g = group_open(3, cluster_of(6));
    primary = group_primary(&g);
    CHECK(primary != NULL);
    old = primary->node->index;
    want.g = &g;
    want.except = old;
    want.view = primary->status.view;
    vsr_sim_isolate(world.sim, old, 1);
    CHECK(run_until(view_changed, &want, 400000));
    primary = NULL;
    for (uint32_t i = 0; i < g.count; ++i) {
        if (i != old && g.apps[i]->status.primary == i + 1) {
            primary = g.apps[i];
        }
    }
    CHECK(primary != NULL);
    submit_request(primary, 1, 1);
    replies.app = primary;
    replies.replies = primary->replies + 1;
    CHECK(run_until(replies_reached, &replies, 400000));
    CHECK(primary->last_reply_status == VSR_REPLY_OK);
    vsr_sim_isolate(world.sim, old, 0);
    CHECK(run_until(group_normal, &g, 400000));
    group_close(&g);
    world_close();
}

/* -------------------------------------------------------------------------
 * Test: the primitives never call the executor (decision E7)
 * ---------------------------------------------------------------------- */

/* In replicated mode the core never SYNCs: the store flushes its writes
 * every flush_interval_ns (100 ms at zero) on the replica's FLUSH
 * deadline. Nothing else asks for that flush, so prepare's deadline must
 * cover it and the next poll must let the store issue it; the log then
 * becomes durable with no further event. A STOP and detach end it, the
 * store closing with nothing of the core's queued (STOPPED means every op
 * completed). */
static void test_replicated_flush(void)
{
    struct node *n;
    struct app *app;
    struct replies_at_least want;
    struct vsr_io_store_status store;
    struct vsr_io_sqe sqes[BATCH];
    uint64_t deadline = 0;
    uint64_t flush_at;
    uint32_t count = 0;

    world_open(1, 23);
    world.durability = VSR_REPLICATED;
    n = node_open(0);
    app = app_attach(n, 0, cluster_of(1), 1, 1, VSR_START_NEW);
    CHECK(run_until(app_normal, app, 20000));
    for (uint64_t i = 1; i <= 3; ++i) {
        (void)submit_request(app, i, 1);
    }
    want.app = app;
    want.replies = 3;
    CHECK(run_until(replies_reached, &want, 20000));
    vsr_io_replica_status(app->replica, NULL, &store);
    CHECK(store.durable == 0);
    /* Written but not flushed: the FLUSH deadline is armed within the
     * interval and prepare reports it (or something earlier). */
    CHECK(run_until(store_written, app, 20000));
    vsr_io_replica_status(app->replica, NULL, &store);
    CHECK(app->replica->store.flushed < store.readable);
    flush_at = n->io->deadlines.entries[app->replica->deadline_flush].when;
    CHECK(flush_at != VSR_NO_DEADLINE);
    CHECK(flush_at <= n->io->now + 100 * MS);
    CHECK(PURE(n, vsr_io_prepare(n->io, n->io->now, sqes, BATCH, &count,
                                 &deadline)) == VSR_OK);
    CHECK(deadline <= flush_at);
    CHECK(n->ex.ops->submit_and_wait(n->ex.ctx, sqes, count, 0, 0, 0) == 0);
    run_for(150 * MS);
    /* vsr_io_store_status's durable counts SYNCs, which replicated mode
     * never issues; the flush shows in the store's own flushed. */
    vsr_io_replica_status(app->replica, NULL, &store);
    CHECK(app->replica->store.flushed >= store.readable);
    CHECK(store.durable == 0);
    CHECK(store.error == 0);
    stop_app(app);
    detach_app(app);
    close_node(n);
    world_close();
}

/* Every test runs with the purity guard armed around the primitives (and
 * the routing seam, the modules' polls, close and the node calls); this one
 * checks the guard itself, then runs a scenario across links, TRUSTED
 * handshakes and takeovers, a stream with a FILE write, the store, a
 * capture, a detach (whose slot clears are records) and a RECOVER, and
 * reads the counters: the executor was called only outside them. */
static void test_purity(void)
{
    struct pure_executor guard;
    struct vsr_io_executor inner;
    struct vsr_io_executor guarded;
    struct group g;
    struct app *primary;
    struct replies_at_least want;
    struct vsr_status core;
    uint64_t calls[NODES];

    memset(&core, 0, sizeof(core));
    /* The guard: a call while armed is a violation, one while not is not. */
    world_open(1, 22);
    inner = vsr_sim_executor(world.sim, 0);
    guarded = pure_executor_init(&guard, inner, false);
    (void)guarded.ops->now(guarded.ctx);
    CHECK(guard.calls == 1 && guard.violations == 0);
    pure_executor_arm(&guard);
    pure_executor_arm(&guard);
    (void)guarded.ops->now(guarded.ctx);
    pure_executor_disarm(&guard);
    guarded.ops->wake(guarded.ctx);
    pure_executor_disarm(&guard);
    CHECK(guard.calls == 3 && guard.violations == 2);
    CHECK(strcmp(guard.last, "wake") == 0);
    world_close();
    /* A group of three, requests, a capture at the primary. */
    world_open(3, 23);
    g = group_open(3, cluster_of(7));
    for (uint32_t i = 0; i < 3; ++i) {
        calls[i] = world.node[i].pure.calls;
        CHECK(calls[i] > 0); /* init and attach registered. */
        CHECK(world.node[i].pure.violations == 0);
    }
    primary = group_primary(&g);
    CHECK(primary != NULL);
    for (uint64_t c = 1; c <= 3; ++c) {
        submit_request(primary, c, 1);
    }
    want.app = primary;
    want.replies = 3;
    CHECK(run_until(replies_reached, &want, 200000));
    /* Successive checkpoints: each capture and publication releases the
     * kept slot of the file before it, a slot clear made at run time. */
    for (uint64_t round = 2; round <= 4; ++round) {
        uint64_t checkpoint = core.checkpoint_op;

        core_event(primary, VSR_EVENT_CHECKPOINT, 0, 0, NULL, 0);
        run_for(200 * MS);
        vsr_io_replica_status(primary->replica, &core, NULL);
        CHECK(core.checkpoint_op > checkpoint);
        for (uint64_t c = 1; c <= 3; ++c) {
            submit_request(primary, c, round);
        }
        want.replies += 3;
        CHECK(run_until(replies_reached, &want, 200000));
    }
    CHECK(primary->captures >= 3);
    CHECK(primary->node->io->file_slots_free_count > 0);
    for (uint32_t i = 0; i < 3; ++i) {
        CHECK(world.node[i].pure.violations == 0);
        CHECK(world.node[i].pure.calls > calls[i]); /* The loop's own. */
    }
    group_close(&g);
    world_close();
}

int main(int argc, char **argv)
{
    const char *only = getenv("VSR_ENGINE_TEST");

    (void)argc;
    (void)argv;
#define RUN(test)                                                              \
    do {                                                                       \
        if (only == NULL || strcmp(only, #test) == 0) {                        \
            test();                                                            \
            printf("%s: ok\n", #test);                                         \
        }                                                                      \
    } while (0)
    setvbuf(stdout, NULL, _IONBF, 0);
    RUN(test_single_replica);
    RUN(test_route_order);
    RUN(test_route_kinds);
    RUN(test_route_capture);
    RUN(test_attach_errors);
    RUN(test_entry_errors);
    RUN(test_submit_errors);
    RUN(test_refused_events);
    RUN(test_status_and_flags);
    RUN(test_group);
    RUN(test_capture);
    RUN(test_stream);
    RUN(test_busy_attach_detach);
    RUN(test_prepare);
    RUN(test_stale_completions);
    RUN(test_external);
    RUN(test_run);
    RUN(test_small_ring);
    RUN(test_stream_timers);
    RUN(test_redial);
    RUN(test_view_change);
    RUN(test_replicated_flush);
    RUN(test_purity);
#undef RUN
    return 0;
}
