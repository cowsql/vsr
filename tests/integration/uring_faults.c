#include "config.h"

#include "io/slots.h"
#include "lib/check.h"
#include "lib/faulty_executor.h"
#include "lib/io_world.h"
#include "vsr-io.h"

#include <dirent.h>
#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * A replicated group over real rings (docs/io-implementation.md section 9,
 * row `uring_faults`): three engines in this thread, each on its own
 * io_uring, linked over abstract AF_UNIX sockets, their stores in a
 * temporary directory under the build tree, with the purity guard armed
 * around every primitive over the ring (tests/lib/io_world). Faults come
 * from the seeded fault-injecting wrapper (delayed completions, cancelled
 * receives) and from the harness's targeted rules (a failed takeover, a
 * failed slot clear, a failed write during the log's creation, held
 * record writes). Exits 77 when no ring can be made. Pools are small:
 * registered memory counts against RLIMIT_MEMLOCK, per user.
 *
 * IOW_TEST=name runs one test; IOW_SEED=n changes the wrapper's seed.
 */

static uint64_t seed_base = 1;

/* Small pools and slower timers than the simulation's: real time, and a
 * sanitized build. */
static bool world(uint32_t nodes)
{
    struct vsr_io_limits *l = &iow.io_limits;

    if (!iow_open_uring(nodes, seed_base)) {
        return false;
    }
    l->replicas = 1;
    l->links = 4;
    l->streams = 2;
    l->stream_window = 2;
    l->caller_slabs = 0;
    l->slabs = 2 * l->links + l->streams * (l->stream_window + 1) +
               4 * l->replicas + 5 + 2;
    l->buffer_regions = 2;
    iow.limits.pinned_payload_bytes = 32768;
    iow.heartbeat_ns = 20 * IOW_MS;
    iow.view_timeout_ns = 400 * IOW_MS;
    iow.retry_ns = 10 * IOW_MS;
    iow.transfer_timeout_ns = 400 * IOW_MS;
    iow.handshake_timeout_ns = 1000 * IOW_MS;
    return true;
}

static uint64_t committed_of(const struct iow_app *app)
{
    struct vsr_status core;

    vsr_io_replica_status(app->replica, &core, NULL);
    return core.committed;
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

/* Descriptors of this process: a test that closes every engine and ring
 * must leave as many as it found (a failed takeover must still close the
 * raw socket it could not install). */
static uint32_t open_descriptors(void)
{
    DIR *dir = opendir("/proc/self/fd");
    uint32_t count = 0;

    CHECK(dir != NULL);
    while (readdir(dir) != NULL) {
        count++;
    }
    CHECK(closedir(dir) == 0);
    return count;
}

/* -------------------------------------------------------------------------
 * A group under the fault wrapper
 * ---------------------------------------------------------------------- */

/* Three replicas on three rings commit, checkpoint and commit again while
 * the fault wrapper delays completions (by up to four reaps) and cancels
 * receives under every engine: links are lost and redialed, SENDs retried,
 * and every replica still applies the same history; a backup then crashes
 * (its ring closed, its engine abandoned) and recovers from its log over a
 * new ring. */
static void test_group_faults(void)
{
    struct faulty_executor_options faults;
    struct faulty_executor_stats stats;
    struct iow_group g;
    struct iow_app *primary;
    struct iow_node *n;
    uint64_t committed;
    uint32_t index;

    if (!world(3)) {
        exit(77);
    }
    memset(&faults, 0, sizeof(faults));
    faults.delay_ppm = 100000;
    faults.delay_reaps_max = 4;
    faults.cancel_recv_ppm = 100000;
    for (uint32_t i = 0; i < 3; ++i) {
        faults.seed = seed_base * 10u + i;
        iow_node_faulty(i, &faults);
    }
    g = iow_group_open(3, iow_cluster(1));
    iow_group_commit(&g, 12);
    primary = iow_group_primary(&g);
    CHECK(primary != NULL);
    committed = committed_of(primary);
    for (uint32_t i = 0; i < 3; ++i) {
        wait_caught_up(g.apps[i], committed);
        iow_core_event(g.apps[i], VSR_EVENT_CHECKPOINT, 0, 0, NULL, 0);
    }
    iow_group_commit(&g, 6);
    for (uint32_t i = 0; i < 3; ++i) {
        faulty_executor_stats(iow.node[i].faulty, &stats);
        CHECK(stats.delayed > 0);
    }
    /* A backup crashes and recovers from its log. */
    primary = iow_group_primary(&g);
    CHECK(primary != NULL);
    index = (primary->node->index + 1) % 3;
    n = &iow.node[index];
    iow_crash(n);
    iow_group_commit(&g, 3);
    iow_restart(n);
    iow_replica_options(&n->apps[0], iow_cluster(1), index + 1, 3,
                        VSR_START_RECOVER, 0);
    g.apps[index] = iow_attach_with(n, 0);
    committed = committed_of(iow_group_primary(&g));
    wait_caught_up(g.apps[index], committed);
    iow_group_commit(&g, 3);
    for (uint32_t i = 0; i < 3; ++i) {
        faulty_executor_stats(iow.node[i].faulty, &stats);
        printf("  node %u: delayed %" PRIu64 ", cancelled %" PRIu64 "\n", i,
               stats.delayed, stats.cancelled);
    }
    iow_group_close(&g);
    iow_close();
}

struct store_match {
    uint8_t kind;
};

/* A store record of one slot kind (WRITE, SUPER, FLUSH, ...). */
static bool store_record(void *ctx, const struct vsr_io_sqe *sqe)
{
    const struct store_match *m = ctx;

    return VSR_IO_OWNER(sqe->user_data) == IOW_OWNER &&
           iow_slot_kind(sqe->user_data) == m->kind;
}

/* A replicated group whose members keep serving from memory when a write
 * fails (VSR_IO_WRITE_ERROR_CONTINUE): a backup's record writes fail for a
 * while; it goes on applying and the group commits, and after a crash it
 * recovers what reached its disk and catches up through the group. */
static void test_write_errors(void)
{
    struct iow_group g;
    struct iow_app *primary;
    struct iow_node *n;
    struct store_match write = {VSR_IO_SLOT_WRITE};
    struct vsr_io_store_status store;
    uint64_t committed;
    uint32_t index;

    if (!world(3)) {
        exit(77);
    }
    iow.durability = VSR_REPLICATED;
    iow.store.on_write_error = VSR_IO_WRITE_ERROR_CONTINUE;
    g = iow_group_open(3, iow_cluster(5));
    iow_group_commit(&g, 4);
    primary = iow_group_primary(&g);
    CHECK(primary != NULL);
    index = (primary->node->index + 1) % 3;
    n = &iow.node[index];
    iow_rule(n, store_record, &write, IOW_FAIL, -EIO, 3);
    iow_group_commit(&g, 6);
    CHECK(iow_rule_hits(n) >= 1);
    vsr_io_replica_status(g.apps[index]->replica, NULL, &store);
    CHECK(store.error == -EIO);
    committed = committed_of(iow_group_primary(&g));
    wait_caught_up(g.apps[index], committed);
    iow_crash(n);
    iow_restart(n);
    iow_replica_options(&n->apps[0], iow_cluster(5), index + 1, 3,
                        VSR_START_RECOVER, 0);
    g.apps[index] = iow_attach_with(n, 0);
    iow_group_commit(&g, 2);
    committed = committed_of(iow_group_primary(&g));
    wait_caught_up(g.apps[index], committed);
    iow_group_close(&g);
    iow_close();
}

/* -------------------------------------------------------------------------
 * Failed registrations
 * ---------------------------------------------------------------------- */

/* A link's takeover FILES_UPDATE, LINKed to the CLOSE of its raw socket. */
static bool takeover(void *ctx, const struct vsr_io_sqe *sqe)
{
    (void)ctx;
    return VSR_IO_OWNER(sqe->user_data) == IOW_OWNER &&
           sqe->opcode == VSR_IO_SQE_FILES_UPDATE &&
           iow_slot_kind(sqe->user_data) == VSR_IO_SLOT_CONNECT;
}

/* An engine slot's clear, a FILES_UPDATE of -1. */
static bool clear(void *ctx, const struct vsr_io_sqe *sqe)
{
    (void)ctx;
    return VSR_IO_OWNER(sqe->user_data) == IOW_OWNER &&
           sqe->opcode == VSR_IO_SQE_FILES_UPDATE &&
           iow_slot_kind(sqe->user_data) == VSR_IO_SLOT_FILES;
}

/* The first six takeovers on every node fail with -EIO in the kernel (the
 * chained CLOSE of the raw socket is cancelled, as a real failure's is):
 * each link closes, its raw descriptor closed by the link itself, and
 * redials until a takeover succeeds; the group then commits. After every
 * replica detached, every clear of the log's and the directory's slots
 * fails too: each slot still returns to the free list at its completion,
 * the stale file staying registered until the next direct open into the
 * slot replaces it, which the replicas' RECOVER does. No descriptor
 * leaks. */
static void test_registrations(void)
{
    struct iow_group g;
    uint32_t before = open_descriptors();
    uint64_t committed;

    if (!world(3)) {
        exit(77);
    }
    for (uint32_t i = 0; i < 3; ++i) {
        iow_node_open(i);
        iow_rule(&iow.node[i], takeover, NULL, IOW_FAIL_CHAINED, -EIO, 6);
    }
    iow_mesh(3, iow_cluster(2));
    memset(&g, 0, sizeof(g));
    g.count = 3;
    for (uint32_t i = 0; i < 3; ++i) {
        g.apps[i] = iow_attach(&iow.node[i], 0, iow_cluster(2), i + 1, 3,
                               VSR_START_NEW);
    }
    CHECK(iow_run_until(iow_group_normal, &g, 20000 * IOW_MS));
    for (uint32_t i = 0; i < 3; ++i) {
        CHECK(iow_rule_hits(&iow.node[i]) >= 1);
    }
    iow_group_commit(&g, 5);
    /* Stop and detach everyone with the clears failing, then RECOVER. */
    for (uint32_t i = 0; i < 3; ++i) {
        iow_rules_clear(&iow.node[i]);
        iow_rule(&iow.node[i], clear, NULL, IOW_FAIL, -EIO, 1000);
        iow_core_event(g.apps[i], VSR_EVENT_STOP, 0, 0, NULL, 0);
    }
    for (uint32_t i = 0; i < 3; ++i) {
        CHECK(iow_run_until(iow_app_stopped, g.apps[i], 20000 * IOW_MS));
        iow_detach(g.apps[i]);
    }
    iow_run_for(100 * IOW_MS);
    for (uint32_t i = 0; i < 3; ++i) {
        struct iow_node *n = &iow.node[i];

        CHECK(iow_rule_hits(n) >= 2);
        iow_replica_options(&n->apps[0], iow_cluster(2), i + 1, 3,
                            VSR_START_RECOVER, 0);
        g.apps[i] = iow_attach_with(n, 0);
    }
    CHECK(iow_run_until(iow_group_normal, &g, 20000 * IOW_MS));
    iow_group_commit(&g, 3);
    committed = committed_of(iow_group_primary(&g));
    for (uint32_t i = 0; i < 3; ++i) {
        wait_caught_up(g.apps[i], committed);
        iow_rules_clear(&iow.node[i]);
    }
    iow_group_close(&g);
    iow_close();
    CHECK(open_descriptors() == before);
}

/* -------------------------------------------------------------------------
 * Store faults
 * ---------------------------------------------------------------------- */

static bool app_failed(void *ctx)
{
    const struct iow_app *app = ctx;
    struct vsr_status core;

    vsr_io_replica_status(app->replica, &core, NULL);
    return core.failure.code != VSR_FAILURE_NONE;
}

static bool held_something(void *ctx)
{
    const struct iow_node *n = ctx;

    return n->hook.held_count > 0;
}

/* Decision 125: a write error while the log is created fences the store
 * under CONTINUE as under FENCE, the RECOVERY load completing FAILED, so
 * the core fails (STORAGE); STOP and detach still finish, the engine
 * closes. */
static void test_creation_error(void)
{
    struct iow_node *n;
    struct iow_app *app;
    struct store_match super = {VSR_IO_SLOT_SUPER};
    struct vsr_status core;

    if (!world(1)) {
        exit(77);
    }
    iow.durability = VSR_REPLICATED;
    iow.store.on_write_error = VSR_IO_WRITE_ERROR_CONTINUE;
    n = iow_node_open(0);
    iow_rule(n, store_record, &super, IOW_FAIL, -EIO, 1);
    app = iow_attach(n, 0, iow_cluster(3), 1, 1, VSR_START_NEW);
    CHECK(iow_run_until(app_failed, app, 10000 * IOW_MS));
    CHECK(iow_rule_hits(n) == 1);
    vsr_io_replica_status(app->replica, &core, NULL);
    CHECK(core.failure.code == VSR_FAILURE_STORAGE);
    iow_stop(app);
    iow_detach(app);
    iow_close_node(n);
    iow_close();
}

/* A STOP while the store holds STOREs: every record write is held (the
 * write-behind fills and STORE completions wait for it) and a client's
 * retried request loads its record (a hot LOAD whose lease pins the
 * ring). The stopping core waits for its STOREs, which wait for the
 * writes; once those complete, the replica reaches STOPPED and detaches:
 * every hold vsr-io.h lists ends without the core. */
static void test_stop_held(void)
{
    struct iow_node *n;
    struct iow_app *app;
    struct iow_group g;
    struct store_match write = {VSR_IO_SLOT_WRITE};
    struct vsr_io_store_status store;

    if (!world(1)) {
        exit(77);
    }
    iow.durability = VSR_REPLICATED;
    iow.store.write_behind_bytes = 2 * (uint64_t)iow.block_bytes;
    n = iow_node_open(0);
    app = iow_attach(n, 0, iow_cluster(4), 1, 1, VSR_START_NEW);
    CHECK(iow_run_until(iow_app_normal, app, 10000 * IOW_MS));
    memset(&g, 0, sizeof(g));
    g.count = 1;
    g.apps[0] = app;
    iow_group_commit(&g, 4);
    iow_rule(n, store_record, &write, IOW_HOLD, 0, 100000);
    for (uint64_t c = 1; c <= 4; ++c) {
        (void)iow_request(app, 20 + c, 1);
    }
    /* The retry of a known client's last request: its record is loaded. */
    (void)iow_request(app, 1, iow_last_number(1));
    CHECK(iow_run_until(held_something, n, 5000 * IOW_MS));
    iow_run_for(200 * IOW_MS);
    vsr_io_replica_status(app->replica, NULL, &store);
    CHECK(store.unwritten_bytes > 0);
    iow_core_event(app, VSR_EVENT_STOP, 0, 0, NULL, 0);
    iow_run_for(200 * IOW_MS);
    CHECK(vsr_io_detach(app->replica) == VSR_EBUSY);
    iow_rules_clear(n);
    iow_release_held(n);
    CHECK(iow_run_until(iow_app_stopped, app, 10000 * IOW_MS));
    iow_detach(app);
    iow_close_node(n);
    iow_close();
}

int main(int argc, char **argv)
{
    const char *only = getenv("IOW_TEST");
    const char *seed_env = getenv("IOW_SEED");

    (void)argc;
    (void)argv;
    if (seed_env != NULL) {
        seed_base = strtoull(seed_env, NULL, 10);
    }
#define RUN(test)                                                              \
    do {                                                                       \
        if (only == NULL || strcmp(only, #test) == 0) {                        \
            test();                                                            \
            printf("%s: ok\n", #test);                                         \
        }                                                                      \
    } while (0)
    setvbuf(stdout, NULL, _IONBF, 0);
    RUN(test_group_faults);
    RUN(test_write_errors);
    RUN(test_registrations);
    RUN(test_creation_error);
    RUN(test_stop_held);
#undef RUN
    return 0;
}
