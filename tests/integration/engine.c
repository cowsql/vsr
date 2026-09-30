#include "config.h"

#include "io/slots.h"
#include "lib/check.h"
#include "lib/io_world.h"
#include "vsr-io.h"
#include "vsr-sim.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * The engine end to end over the simulation (docs/io-implementation.md
 * section 9, row `engine`): real engines, cores and stores on simulated
 * nodes, driven through vsr-io.h's loop by tests/lib/io_world, with the
 * purity guard armed around every primitive. Every application checks its
 * digest against the cluster's history at every APPLY and INSTALL, and
 * every EXECUTED reply against the first one for its request (exactly
 * once), so each scenario below is also a convergence check.
 *
 * IOW_TEST=name runs one test; IOW_SEED=n changes the simulation's seed
 * of every test (the default is the test's own); IOW_TRACE=1 prints the
 * simulation's trace and every iteration.
 */

static uint64_t seed_base;

static uint64_t seed(uint64_t own)
{
    return seed_base != 0 ? seed_base * 1000u + own : own;
}

static struct iow_group single(struct iow_app *app)
{
    struct iow_group g;

    memset(&g, 0, sizeof(g));
    g.count = 1;
    g.apps[0] = app;
    return g;
}

static uint32_t failure_code(const struct iow_app *app)
{
    struct vsr_status core;

    vsr_io_replica_status(app->replica, &core, NULL);
    return core.failure.code;
}

static bool app_failed(void *ctx)
{
    const struct iow_app *app = ctx;

    return failure_code(app) != VSR_FAILURE_NONE;
}

static bool app_recovering(void *ctx)
{
    const struct iow_app *app = ctx;
    struct vsr_status core;

    vsr_io_replica_status(app->replica, &core, NULL);
    return app->statuses > 0 && core.state == VSR_STATE_RECOVERING;
}

struct caught_up {
    const struct iow_app *app;
    uint64_t op;
};

static bool app_caught_up(void *ctx)
{
    const struct caught_up *want = ctx;

    return want->app->attached && want->app->applied_op >= want->op;
}

static void wait_caught_up(struct iow_app *app, uint64_t op)
{
    struct caught_up want = {app, op};

    if (!iow_run_until(app_caught_up, &want, 20000 * IOW_MS)) {
        iow_dump_node(app->node);
        CHECK(false);
    }
}

struct replies_seen {
    const struct iow_app *app;
    uint64_t replies;
};

static bool replies_seen(void *ctx)
{
    const struct replies_seen *want = ctx;

    return want->app->replies >= want->replies;
}

/* -------------------------------------------------------------------------
 * Attach modes
 * ---------------------------------------------------------------------- */

/* NEW over an empty directory creates the log; RECOVER over it brings the
 * committed prefix back (the application rebuilt by replay, its digests
 * equal to the first run's), and a retried request is answered from the
 * recovered client table, not executed again; NEW over that used log is
 * refused by the core (IDENTITY) from the recovered row; RECOVER over an
 * empty directory reports NOT_FOUND and, alone, waits for a quorum that
 * never comes. Each ends with STOP, detach and every lease back. */
static void test_attach_modes(void)
{
    struct iow_node *n;
    struct iow_app *app;
    struct iow_group g;
    struct vsr_status core;
    struct vsr_io_store_status store;
    uint64_t committed;
    uint64_t replies;
    uint64_t commands;

    iow_open_sim(1, seed(1), NULL);
    n = iow_node_open(0);
    app = iow_attach(n, 0, iow_cluster(1), 1, 1, VSR_START_NEW);
    CHECK(app->replica != NULL);
    CHECK(vsr_io_replica_find(n->io, iow_cluster(1), 1) == app->replica);
    CHECK(iow_run_until(iow_app_normal, app, 1000 * IOW_MS));
    g = single(app);
    iow_group_commit(&g, 6);
    vsr_io_replica_status(app->replica, &core, &store);
    committed = core.committed;
    CHECK(committed >= 6 && app->applied_op == committed);
    CHECK(store.readable > 0 && store.durable > 0 && store.clients == 4);
    iow_stop(app);
    CHECK(app->stopped_statuses == 1);
    iow_detach(app);
    CHECK(vsr_io_replica_find(n->io, iow_cluster(1), 1) == NULL);

    /* RECOVER: the committed prefix, replayed from genesis. */
    app = iow_attach(n, 0, iow_cluster(1), 1, 1, VSR_START_RECOVER);
    CHECK(iow_run_until(iow_app_normal, app, 1000 * IOW_MS));
    vsr_io_replica_status(app->replica, &core, &store);
    CHECK(core.committed >= committed);
    CHECK(store.clients == 4);
    wait_caught_up(app, committed);
    /* A retry of client 1's last request: the recorded reply again (the
     * reply table checks it), nothing executed. */
    replies = app->replies;
    commands = app->commands;
    (void)iow_request(app, 1, iow_last_number(1));
    iow_run_for(100 * IOW_MS);
    CHECK(app->replies == replies + 1);
    CHECK(app->last_reply_status == VSR_REPLY_OK);
    CHECK(app->replies_executed > 0 && app->commands == commands);
    g = single(app);
    iow_group_commit(&g, 2);
    CHECK(app->applied_op > committed);
    iow_stop(app);
    iow_detach(app);

    /* NEW over the used log: the recovered row names a store NEW must not
     * reuse. */
    app = iow_attach(n, 0, iow_cluster(1), 1, 1, VSR_START_NEW);
    CHECK(iow_run_until(app_failed, app, 1000 * IOW_MS));
    CHECK(failure_code(app) == VSR_FAILURE_IDENTITY);
    iow_stop(app);
    iow_detach(app);

    /* RECOVER over an empty directory: NOT_FOUND, then recovery from a
     * quorum a group of one does not have. */
    iow_replica_options(&n->apps[0], iow_cluster(1), 1, 1, VSR_START_RECOVER,
                        1);
    iow_make_directory(n, n->apps[0].path);
    app = iow_attach_with(n, 0);
    CHECK(iow_run_until(app_recovering, app, 1000 * IOW_MS));
    iow_run_for(200 * IOW_MS);
    vsr_io_replica_status(app->replica, &core, &store);
    CHECK(core.state == VSR_STATE_RECOVERING && core.committed == 0);
    CHECK(failure_code(app) == VSR_FAILURE_NONE);
    iow_stop(app);
    iow_detach(app);
    iow_close_node(n);
    iow_close();
}

/* -------------------------------------------------------------------------
 * A group's life
 * ---------------------------------------------------------------------- */

static void checkpoint_all(struct iow_group *g)
{
    for (uint32_t i = 0; i < g->count; ++i) {
        if (g->apps[i] != NULL && g->apps[i]->attached) {
            iow_core_event(g->apps[i], VSR_EVENT_CHECKPOINT, 0, 0, NULL, 0);
        }
    }
}

struct anchored {
    struct iow_group *g;
    uint64_t op;
};

/* Every attached member published an anchor at or past `op`. */
static bool group_anchored(void *ctx)
{
    const struct anchored *want = ctx;

    for (uint32_t i = 0; i < want->g->count; ++i) {
        const struct iow_app *app = want->g->apps[i];
        struct vsr_status core;

        if (app == NULL || !app->attached) {
            continue;
        }
        vsr_io_replica_status(app->replica, &core, NULL);
        if (core.checkpoint_op < want->op) {
            return false;
        }
    }
    return true;
}

static uint64_t committed_of(const struct iow_app *app)
{
    struct vsr_status core;

    vsr_io_replica_status(app->replica, &core, NULL);
    return core.committed;
}

/* Every member applies the primary's commitment, then takes the hint; the
 * anchors reach that op. Returns it. */
static uint64_t checkpoint_group(struct iow_group *g)
{
    struct anchored anchor;
    struct iow_app *primary = iow_group_primary(g);
    uint64_t op;

    CHECK(primary != NULL);
    op = committed_of(primary);
    for (uint32_t i = 0; i < g->count; ++i) {
        if (g->apps[i] != NULL && g->apps[i]->attached) {
            wait_caught_up(g->apps[i], op);
        }
    }
    checkpoint_all(g);
    anchor.g = g;
    anchor.op = op;
    if (!iow_run_until(group_anchored, &anchor, 5000 * IOW_MS)) {
        for (uint32_t i = 0; i < iow.nodes; ++i) {
            if (iow.node[i].open) {
                iow_dump_node(&iow.node[i]);
            }
        }
        CHECK(false);
    }
    return op;
}

static struct iow_app *recover(struct iow_node *n, struct vsr_id cluster,
                               uint64_t replica, uint32_t members,
                               uint32_t generation)
{
    struct iow_app *app = &n->apps[0];

    iow_replica_options(app, cluster, replica, members, VSR_START_RECOVER,
                        generation);
    if (generation > 0) {
        /* A disk that lost everything: an empty directory, no images. */
        memset(app->snapshots, 0, sizeof(app->snapshots));
        app->options.core.join_role = VSR_MEMBER_FULL;
        iow_make_directory(n, app->path);
    }
    return iow_attach_with(n, 0);
}

static uint32_t index_of(const struct iow_group *g, const struct iow_app *app)
{
    for (uint32_t i = 0; i < g->count; ++i) {
        if (g->apps[i] == app) {
            return i;
        }
    }
    CHECK(false);
    return 0;
}

/* Three replicas on three engines commit, reply and checkpoint; a backup
 * crashes (its engine abandoned after vsr_sim_crash) and recovers from
 * its log with RECOVER; the primary crashes, the others change view and
 * go on, and it recovers as a backup; a backup loses its disk and
 * recovers through the group (NOT_FOUND, then the anchor fetched from a
 * peer); a fourth node JOINs as a learner (fetching the anchor too), and a
 * RECONFIGURE admits it. Every replica applies the same history, every
 * retried request gets its first reply, and the group closes. */
static void test_group_lifecycle(void)
{
    struct iow_group g;
    struct iow_app *primary;
    struct iow_app *backup;
    struct iow_app *learner;
    struct replies_seen seen;
    uint64_t committed;
    uint32_t index;
    struct iow_node *n;

    iow_open_sim(4, seed(2), NULL);
    g = iow_group_open(3, iow_cluster(2));
    iow_group_commit(&g, 8);
    (void)checkpoint_group(&g);
    iow_group_commit(&g, 4);

    /* A backup crashes and recovers from its own log. */
    primary = iow_group_primary(&g);
    CHECK(primary != NULL);
    backup = g.apps[(index_of(&g, primary) + 1) % 3];
    n = backup->node;
    iow_crash(n);
    iow_group_commit(&g, 4);
    iow_restart(n);
    g.apps[n->index] = recover(n, iow_cluster(2), n->index + 1, 3, 0);
    committed = committed_of(primary);
    CHECK(iow_run_until(iow_group_normal, &g, 20000 * IOW_MS));
    wait_caught_up(g.apps[n->index], committed);
    iow_group_commit(&g, 3);

    /* The primary crashes: a view change, then it recovers as a backup. */
    primary = iow_group_primary(&g);
    CHECK(primary != NULL);
    index = index_of(&g, primary);
    n = primary->node;
    iow_crash(n);
    g.apps[index] = NULL;
    iow_group_commit(&g, 3);
    CHECK(iow_group_primary(&g) != NULL);
    iow_restart(n);
    g.apps[index] = recover(n, iow_cluster(2), index + 1, 3, 0);
    CHECK(iow_run_until(iow_group_normal, &g, 20000 * IOW_MS));
    iow_group_commit(&g, 2);
    committed = committed_of(iow_group_primary(&g));
    wait_caught_up(g.apps[index], committed);

    /* A checkpoint every member publishes, then a backup loses its disk:
     * NOT_FOUND, recovery from the group, the anchor fetched. */
    (void)checkpoint_group(&g);
    primary = iow_group_primary(&g);
    index = (index_of(&g, primary) + 2) % 3;
    n = g.apps[index]->node;
    iow_crash(n);
    iow_group_commit(&g, 2);
    iow_restart(n);
    g.apps[index] = recover(n, iow_cluster(2), index + 1, 3, 1);
    committed = committed_of(iow_group_primary(&g));
    wait_caught_up(g.apps[index], committed);
    CHECK(g.apps[index]->installs >= 1);
    CHECK(g.apps[index]->fetches_ok >= 1);
    CHECK(iow_run_until(iow_group_normal, &g, 20000 * IOW_MS));
    iow_group_commit(&g, 2);

    /* A learner joins, warms up from the anchor, and is admitted. */
    iow_authorize(iow_cluster(2), 4, 4);
    n = iow_node_open(3);
    learner = iow_attach(n, 0, iow_cluster(2), 4, 3, VSR_START_JOIN);
    committed = committed_of(iow_group_primary(&g));
    wait_caught_up(learner, committed);
    CHECK(learner->status.state == VSR_STATE_WARMING);
    CHECK(learner->fetches_ok >= 1 && learner->installs >= 1);
    primary = iow_group_primary(&g);
    seen.app = primary;
    seen.replies = primary->replies + 1;
    (void)iow_reconfigure(primary, 9, 1, primary->status.epoch + 1, 4);
    if (!iow_run_until(replies_seen, &seen, 5000 * IOW_MS)) {
        for (uint32_t i = 0; i < iow.nodes; ++i) {
            iow_dump_node(&iow.node[i]);
        }
        CHECK(false);
    }
    CHECK(primary->last_reply_status == VSR_REPLY_OK);
    g.apps[3] = learner;
    g.count = 4;
    CHECK(iow_run_until(iow_group_normal, &g, 20000 * IOW_MS));
    CHECK(learner->status.epoch == 1);
    iow_group_commit(&g, 4);
    committed = committed_of(iow_group_primary(&g));
    for (uint32_t i = 0; i < 4; ++i) {
        wait_caught_up(g.apps[i], committed);
    }
    iow_group_close(&g);
    iow_close();
}

/* -------------------------------------------------------------------------
 * Admission
 * ---------------------------------------------------------------------- */

static int submit_one(struct iow_app *app, uint64_t client, uint64_t number,
                      uint64_t *lease)
{
    struct vsr_io_event event;
    uint32_t consumed = 0;
    int rc;

    iow_make_request(app, client, number, &event);
    *lease = event.event.lease;
    rc = iow_submit(app->node, &event, 1, &consumed);
    CHECK(consumed == (rc == VSR_OK ? 1u : 0u));
    if (rc != VSR_OK) {
        iow_lease_forget(app, event.event.lease);
    }
    return rc;
}

static uint32_t clients_of(const struct iow_app *app)
{
    struct vsr_io_store_status store;

    vsr_io_replica_status(app->replica, NULL, &store);
    return store.clients;
}

/* max_clients (4) is enforced at vsr_io_submit: at the primary a fifth
 * client incarnation is ELIMIT (the caller answers LIMIT), while known
 * clients, retries included, go on; a backup admits a new client while
 * its table has room, and the redirect it answers (a REPLY other than OK)
 * ends that admission, so backups hold only what the group replicated;
 * once the table is full everywhere, every replica refuses the fifth. */
static void test_admission(void)
{
    struct iow_group g;
    struct iow_app *primary;
    struct iow_app *backup;
    struct replies_seen want;
    uint64_t lease;

    iow_open_sim(3, seed(3), NULL);
    iow.store.max_clients = 4;
    g = iow_group_open(3, iow_cluster(3));
    primary = iow_group_primary(&g);
    CHECK(primary != NULL);
    backup = g.apps[(index_of(&g, primary) + 1) % 3];
    iow_group_commit(&g, 3); /* Clients 1, 2 and 3. */
    CHECK(iow_run_until(iow_group_normal, &g, 1000 * IOW_MS));
    iow_run_for(50 * IOW_MS);
    CHECK(clients_of(primary) == 3 && clients_of(backup) == 3);

    /* A backup admits client 7 and redirects it: the admission ends. */
    want.app = backup;
    want.replies = backup->replies + 1;
    CHECK(submit_one(backup, 7, 1, &lease) == VSR_OK);
    CHECK(clients_of(backup) == 4);
    CHECK(iow_run_until(replies_seen, &want, 1000 * IOW_MS));
    CHECK(backup->last_reply_status == VSR_REPLY_NOT_PRIMARY);
    CHECK(clients_of(backup) == 3);

    /* The fourth client fills every table. */
    iow_group_commit(&g, 4); /* Clients 4, 1, 2, 3. */
    iow_run_for(50 * IOW_MS);
    for (uint32_t i = 0; i < 3; ++i) {
        CHECK(clients_of(g.apps[i]) == 4);
    }
    primary = iow_group_primary(&g);
    CHECK(submit_one(primary, 5, 1, &lease) == VSR_ELIMIT);
    CHECK(submit_one(backup, 6, 1, &lease) == VSR_ELIMIT);
    /* Known clients go on: a retry of client 2's last request is the
     * recorded reply, a new request of client 2 is executed. */
    want.app = primary;
    want.replies = primary->replies + 1;
    CHECK(submit_one(primary, 2, iow_last_number(2), &lease) == VSR_OK);
    CHECK(iow_run_until(replies_seen, &want, 1000 * IOW_MS));
    CHECK(primary->last_reply_status == VSR_REPLY_OK);
    want.replies = primary->replies + 1;
    CHECK(submit_one(primary, 2, 100, &lease) == VSR_OK);
    CHECK(iow_run_until(replies_seen, &want, 1000 * IOW_MS));
    CHECK(primary->last_reply_status == VSR_REPLY_OK);
    CHECK(clients_of(primary) == 4);
    iow_group_close(&g);
    iow_close();
}

/* -------------------------------------------------------------------------
 * Stop, detach and close with work in flight
 * ---------------------------------------------------------------------- */

static bool replica_stopped_state(void *ctx)
{
    const struct iow_app *app = ctx;
    struct vsr_status core;

    vsr_io_replica_status(app->replica, &core, NULL);
    return core.state == VSR_STATE_STOPPED;
}

static bool write_match(void *ctx, const struct vsr_io_sqe *sqe)
{
    (void)ctx;
    return VSR_IO_OWNER(sqe->user_data) == IOW_OWNER &&
           (sqe->opcode == VSR_IO_SQE_WRITE ||
            sqe->opcode == VSR_IO_SQE_WRITEV) &&
           iow_slot_kind(sqe->user_data) == VSR_IO_SLOT_WRITE;
}

/* Everything the store packed has been written. */
static bool store_written(void *ctx)
{
    const struct iow_app *app = ctx;
    struct vsr_io_store_status store;

    vsr_io_replica_status(app->replica, NULL, &store);
    return store.written >= store.readable && store.unwritten_bytes == 0;
}

static bool held_something(void *ctx)
{
    const struct iow_node *n = ctx;

    return n->hook.held_count > 0;
}

/* Requests queued before a STOP are dropped by the stopping core with their
 * leases returned; an APPLY the application holds keeps the replica from
 * STOPPED, and detach is EBUSY until it completes; the STATUS that reports
 * STOPPED sitting in the forwarded ring (the caller not polling) keeps
 * detach EBUSY; a record write in flight at STOPPED does too, until it
 * completes; vsr_io_close is EBUSY while a replica is attached and deinit
 * until the engine closed; a caller stream cut by the source engine's
 * close ends CANCELLED there and RETRY at the requester. */
static void test_close_ordering(void)
{
    struct iow_node *a;
    struct iow_node *b;
    struct iow_app *app;
    struct iow_group g;
    struct vsr_io_event events[4];
    struct iow_rstream *r;
    struct iow_stream_request request;
    uint32_t consumed = 0;
    uint64_t releases;

    iow_open_sim(2, seed(4), NULL);
    a = iow_node_open(0);
    b = iow_node_open(1);
    app = iow_attach(a, 0, iow_cluster(4), 1, 1, VSR_START_NEW);
    CHECK(iow_run_until(iow_app_normal, app, 1000 * IOW_MS));
    g = single(app);
    iow_group_commit(&g, 2);
    CHECK(vsr_io_close(a->io) == VSR_EBUSY);

    /* The application holds an APPLY; the STOP cannot finish. */
    app->hold_apply = true;
    (void)iow_request(app, 10, 1);
    iow_run_for(20 * IOW_MS);
    CHECK(app->held_apply != 0);
    /* Three more requests and the STOP in one submission. */
    releases = app->releases;
    for (uint32_t i = 0; i < 3; ++i) {
        iow_make_request(app, 11 + i, 1, &events[i]);
    }
    memset(&events[3], 0, sizeof(events[3]));
    events[3].replica = app->replica;
    events[3].kind = VSR_IO_EVENT_CORE;
    events[3].event.type = VSR_EVENT_STOP;
    CHECK(iow_submit(a, events, 4, &consumed) == VSR_OK && consumed == 4);
    iow_run_for(200 * IOW_MS);
    CHECK(!replica_stopped_state(app));
    CHECK(vsr_io_detach(app->replica) == VSR_EBUSY);
    /* The three queued requests were dropped, their leases returned. */
    CHECK(app->releases >= releases + 3);
    /* The APPLY completes; the STATUS STOPPED waits in the ring. */
    a->hold_ops = 1;
    app->hold_apply = false;
    iow_complete_held_apply(app);
    iow_run_for(100 * IOW_MS);
    CHECK(vsr_io_detach(app->replica) == VSR_EBUSY);
    a->hold_ops = 0;
    vsr_io_wake(a->io); /* The caller polls again. */
    if (!iow_run_until(iow_app_stopped, app, 1000 * IOW_MS)) {
        iow_dump_node(a);
        CHECK(false);
    }
    CHECK(app->stopped_statuses == 1);
    iow_detach(app);

    /* The durable replica recovers and goes on. */
    app = iow_attach(a, 0, iow_cluster(4), 1, 1, VSR_START_RECOVER);
    if (!iow_run_until(iow_app_normal, app, 1000 * IOW_MS)) {
        iow_dump_node(a);
        CHECK(false);
    }
    g = single(app);
    iow_group_commit(&g, 1);
    iow_stop(app);
    iow_detach(app);

    /* A replicated replica (write-behind: its commits wait for no write)
     * reaches STOPPED with a record write in flight: detach is EBUSY until
     * the write completes. */
    iow.durability = VSR_REPLICATED;
    app = iow_attach(a, 0, iow_cluster(8), 1, 1, VSR_START_NEW);
    iow.durability = VSR_DURABLE;
    CHECK(iow_run_until(iow_app_normal, app, 1000 * IOW_MS));
    g = single(app);
    iow_group_commit(&g, 1);
    CHECK(iow_run_until(store_written, app, 1000 * IOW_MS));
    iow_rule(a, write_match, NULL, IOW_HOLD, 0, 1);
    iow_group_commit(&g, 1);
    CHECK(iow_run_until(held_something, a, 1000 * IOW_MS));
    iow_stop(app);
    CHECK(a->hook.held_count > 0);
    iow_run_for(100 * IOW_MS);
    CHECK(vsr_io_detach(app->replica) == VSR_EBUSY);
    iow_release_held(a);
    iow_detach(app);

    /* A caller stream from b to a; a's engine closes under it. */
    request = iow_pattern(77, 200000, 4096);
    request.hold = 1; /* The source is mid-stream when its engine closes. */
    b->hold_data = true;
    r = iow_stream_open(b, 1, UINT64_C(0x8000000000000001), &request);
    for (uint32_t i = 0; i < 100000 && b->held_data_count == 0; ++i) {
        CHECK(iow_round());
    }
    CHECK(b->held_data_count > 0);
    iow_close_io(a);
    CHECK(vsr_io_deinit(a->io) == VSR_EBUSY);
    CHECK(iow_run_until(iow_node_closed, a, 5000 * IOW_MS));
    CHECK(vsr_io_deinit(a->io) == VSR_OK);
    a->open = false;
    CHECK(a->sstreams[0].ended && a->sstreams[0].status == VSR_IO_CANCELLED);
    iow_release_data(b);
    for (uint32_t i = 0; i < 100000 && !r->ended; ++i) {
        CHECK(iow_round());
    }
    CHECK(r->ended && r->status == VSR_IO_RETRY && !r->mismatch);
    iow_close_node(b);
    iow_close();
}

/* -------------------------------------------------------------------------
 * The minimum configuration
 * ---------------------------------------------------------------------- */

/* A pool at vsr-io.h's minimum slab count with no caller share
 * (caller_slabs = 0) and file slots at the documented minimum: a group of
 * three commits and checkpoints, and a learner fetches the anchor (links,
 * a library stream, the snapshot module's staging slabs and its kept
 * files all at once), then everyone closes. */
static void test_minimum(void)
{
    struct iow_group g;
    struct iow_app *learner;
    struct vsr_io_options options;
    struct vsr_io_layout layout;
    struct iow_node *n;
    struct vsr_io_limits *l = &iow.io_limits;
    uint64_t committed;

    iow_open_sim(4, seed(5), NULL);
    l->caller_slabs = 0;
    l->slabs = 2 * l->links + l->streams * (l->stream_window + 1) +
               4 * l->replicas + 5;
    l->file_slots = 1 + l->links + l->streams + 2 * l->replicas;
    options = iow_engine_options(&iow.node[0]);
    options.limits.slabs--;
    CHECK(vsr_io_layout(&options, &layout) == VSR_ELIMIT);
    options.limits.slabs++;
    CHECK(vsr_io_layout(&options, &layout) == VSR_OK);
    g = iow_group_open(3, iow_cluster(5));
    iow_group_commit(&g, 6);
    (void)checkpoint_group(&g);
    iow_group_commit(&g, 3);
    (void)checkpoint_group(&g);
    iow_authorize(iow_cluster(5), 4, 4);
    n = iow_node_open(3);
    learner = iow_attach(n, 0, iow_cluster(5), 4, 3, VSR_START_JOIN);
    committed = committed_of(iow_group_primary(&g));
    wait_caught_up(learner, committed);
    CHECK(learner->fetches_ok >= 1);
    iow_group_commit(&g, 3);
    g.apps[3] = learner;
    g.count = 4;
    iow_group_close(&g);
    iow_close();
}

/* -------------------------------------------------------------------------
 * A full send queue
 * ---------------------------------------------------------------------- */

/* A RECONFIGURE adds a fourth member whose node is not running yet: the
 * epoch transition broadcasts START_EPOCH to both groups, and every SEND
 * to the missing node fails. With its node's send queue one message deep,
 * each rebroadcast evicts the queued START_EPOCH (not on the wire) with
 * RETRY; the epochs extension restarts its broadcast at any failed SEND,
 * so an eviction completed within the same poll fed the RETRY back at
 * once and the poll never ended. Evicted SENDs wait retry_ns like refused
 * ones (decision 129): the members poll finitely, and once the fourth
 * node runs (JOIN) it completes the transition and the group of four
 * commits. */
static void test_queue_full(void)
{
    struct iow_group g;
    struct iow_app *primary;
    struct iow_app *fourth;
    struct replies_seen seen;
    struct vsr_io_stats before;
    struct vsr_io_stats after;
    uint64_t committed;

    iow_open_sim(4, seed(8), NULL);
    iow.io_limits.link_queue = 1;
    g = iow_group_open(3, iow_cluster(10));
    iow_group_commit(&g, 2);
    iow_authorize(iow_cluster(10), 4, 4);
    primary = iow_group_primary(&g);
    CHECK(primary != NULL);
    vsr_io_get_stats(primary->node->io, &before);
    seen.app = primary;
    seen.replies = primary->replies + 1;
    (void)iow_reconfigure(primary, 9, 1, 1, 4);
    CHECK(iow_run_until(replies_seen, &seen, 5000 * IOW_MS));
    CHECK(primary->last_reply_status == VSR_REPLY_OK);
    iow_run_for(1000 * IOW_MS);
    vsr_io_get_stats(primary->node->io, &after);
    /* Retries paced by retry_ns: at most a few hundred per second. */
    CHECK(after.messages_retried - before.messages_retried < 2000);
    fourth =
        iow_attach(iow_node_open(3), 0, iow_cluster(10), 4, 3, VSR_START_JOIN);
    g.apps[3] = fourth;
    g.count = 4;
    CHECK(iow_run_until(iow_group_normal, &g, 20000 * IOW_MS));
    iow_group_commit(&g, 3);
    committed = committed_of(iow_group_primary(&g));
    for (uint32_t i = 0; i < 4; ++i) {
        wait_caught_up(g.apps[i], committed);
    }
    iow_group_close(&g);
    iow_close();
}

/* -------------------------------------------------------------------------
 * A peer that stays away
 * ---------------------------------------------------------------------- */

/* A learner warms up, then its node crashes: the members keep sending it
 * epoch messages (EPOCH_STARTED goes to an authenticated learner), which
 * queue on its node while every dial fails, until the queue (link_queue 2
 * here) is full; each later SEND then evicts the oldest one not on the
 * wire with RETRY. The core answers a failed epoch SEND by sending it
 * again, so an eviction completed within the same poll fed that RETRY
 * back at once and the poll never ended. Evicted SENDs wait retry_ns like
 * refused ones (decision 129); the members poll finitely, the group
 * commits, and the learner catches up once it returns. */
static void test_learner_down(void)
{
    struct iow_group g;
    struct iow_app *learner;
    struct iow_node *n;
    struct vsr_io_stats before;
    struct vsr_io_stats after;
    struct iow_app *primary;
    uint64_t committed;

    iow_open_sim(4, seed(7), NULL);
    iow.io_limits.link_queue = 2;
    g = iow_group_open(3, iow_cluster(9));
    iow_group_commit(&g, 2);
    iow_authorize(iow_cluster(9), 4, 4);
    n = iow_node_open(3);
    learner = iow_attach(n, 0, iow_cluster(9), 4, 3, VSR_START_JOIN);
    committed = committed_of(iow_group_primary(&g));
    wait_caught_up(learner, committed);
    iow_crash(n);
    primary = iow_group_primary(&g);
    CHECK(primary != NULL);
    vsr_io_get_stats(primary->node->io, &before);
    iow_run_for(1000 * IOW_MS);
    iow_group_commit(&g, 4);
    vsr_io_get_stats(primary->node->io, &after);
    /* Retries are paced by retry_ns (5 ms): hundreds in a second at most,
     * not an unbounded number within one poll. */
    CHECK(after.messages_retried - before.messages_retried < 2000);
    iow_restart(n);
    iow_replica_options(&n->apps[0], iow_cluster(9), 4, 3, VSR_START_RECOVER,
                        0);
    learner = iow_attach_with(n, 0);
    committed = committed_of(iow_group_primary(&g));
    wait_caught_up(learner, committed);
    g.apps[3] = learner;
    g.count = 4;
    iow_group_close(&g);
    iow_close();
}

static bool write_record(void *ctx, const struct vsr_io_sqe *sqe)
{
    (void)ctx;
    return VSR_IO_OWNER(sqe->user_data) == IOW_OWNER &&
           iow_slot_kind(sqe->user_data) == VSR_IO_SLOT_WRITE;
}

/* A replicated group whose members keep serving from memory after a failed
 * write (VSR_IO_WRITE_ERROR_CONTINUE): a backup's record writes fail; it
 * reports the error, applies everything the group commits, and the group
 * closes. */
static void test_write_errors(void)
{
    struct iow_group g;
    struct iow_app *primary;
    struct iow_node *n;
    struct vsr_io_store_status store;
    uint64_t committed;
    uint32_t index;

    iow_open_sim(3, seed(9), NULL);
    iow.durability = VSR_REPLICATED;
    iow.store.on_write_error = VSR_IO_WRITE_ERROR_CONTINUE;
    g = iow_group_open(3, iow_cluster(12));
    iow_group_commit(&g, 4);
    primary = iow_group_primary(&g);
    CHECK(primary != NULL);
    index = (index_of(&g, primary) + 1) % 3;
    n = g.apps[index]->node;
    iow_rule(n, write_record, NULL, IOW_FAIL, -EIO, 3);
    iow_group_commit(&g, 6);
    CHECK(iow_rule_hits(n) >= 1);
    vsr_io_replica_status(g.apps[index]->replica, NULL, &store);
    CHECK(store.error == -EIO);
    committed = committed_of(iow_group_primary(&g));
    wait_caught_up(g.apps[index], committed);
    iow_group_close(&g);
    iow_close();
}

/* -------------------------------------------------------------------------
 * Two groups on the same engines
 * ---------------------------------------------------------------------- */

/* Each of three engines hosts a replica of two groups: both commit over
 * the same links, one engine crashes and both of its replicas recover
 * with RECOVER while the other groups' members go on. */
static void test_two_groups(void)
{
    struct iow_group g1;
    struct iow_group g2;
    uint64_t committed;
    struct iow_node *n;

    iow_open_sim(3, seed(6), NULL);
    for (uint32_t i = 0; i < 3; ++i) {
        iow_node_open(i);
    }
    iow_mesh(3, iow_cluster(6));
    iow_mesh(3, iow_cluster(7));
    memset(&g1, 0, sizeof(g1));
    memset(&g2, 0, sizeof(g2));
    g1.count = g2.count = 3;
    for (uint32_t i = 0; i < 3; ++i) {
        g1.apps[i] = iow_attach(&iow.node[i], 0, iow_cluster(6), i + 1, 3,
                                VSR_START_NEW);
        g2.apps[i] = iow_attach(&iow.node[i], 1, iow_cluster(7), i + 1, 3,
                                VSR_START_NEW);
    }
    CHECK(iow_run_until(iow_group_normal, &g1, 20000 * IOW_MS));
    CHECK(iow_run_until(iow_group_normal, &g2, 20000 * IOW_MS));
    iow_group_commit(&g1, 4);
    iow_group_commit(&g2, 4);
    n = &iow.node[2];
    iow_crash(n);
    iow_group_commit(&g1, 2);
    iow_group_commit(&g2, 2);
    iow_restart(n);
    iow_replica_options(&n->apps[0], iow_cluster(6), 3, 3, VSR_START_RECOVER,
                        0);
    g1.apps[2] = iow_attach_with(n, 0);
    iow_replica_options(&n->apps[1], iow_cluster(7), 3, 3, VSR_START_RECOVER,
                        0);
    g2.apps[2] = iow_attach_with(n, 1);
    committed = committed_of(iow_group_primary(&g1));
    wait_caught_up(g1.apps[2], committed);
    committed = committed_of(iow_group_primary(&g2));
    wait_caught_up(g2.apps[2], committed);
    iow_group_commit(&g1, 2);
    iow_group_commit(&g2, 2);
    for (uint32_t i = 0; i < 3; ++i) {
        iow_core_event(g2.apps[i], VSR_EVENT_STOP, 0, 0, NULL, 0);
    }
    for (uint32_t i = 0; i < 3; ++i) {
        CHECK(iow_run_until(iow_app_stopped, g2.apps[i], 5000 * IOW_MS));
        iow_detach(g2.apps[i]);
    }
    iow_group_close(&g1);
    iow_close();
}

int main(int argc, char **argv)
{
    const char *only = getenv("IOW_TEST");
    const char *seed_env = getenv("IOW_SEED");

    (void)argc;
    (void)argv;
    seed_base = seed_env != NULL ? strtoull(seed_env, NULL, 10) : 0;
#define RUN(test)                                                              \
    do {                                                                       \
        if (only == NULL || strcmp(only, #test) == 0) {                        \
            test();                                                            \
            printf("%s: ok\n", #test);                                         \
        }                                                                      \
    } while (0)
    setvbuf(stdout, NULL, _IONBF, 0);
    RUN(test_attach_modes);
    RUN(test_group_lifecycle);
    RUN(test_admission);
    RUN(test_close_ordering);
    RUN(test_minimum);
    RUN(test_two_groups);
    RUN(test_learner_down);
    RUN(test_queue_full);
    RUN(test_write_errors);
#undef RUN
    return 0;
}
