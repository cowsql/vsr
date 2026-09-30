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
 * Snapshots end to end over the simulation (docs/io-implementation.md
 * section 9, row `snapshots`): the joint ops with engines on both sides of
 * a fetch. Each application puts its digest in the manifest at CAPTURE and
 * serves the image over a caller stream (fetch_by_stream), while the
 * engine's snapshot module writes, syncs, serves, fetches, verifies,
 * renames, loads and drops the clients files over library streams; the
 * harness checks every APPLY, INSTALL and reply against the cluster's
 * history (tests/lib/io_world), with the purity guard armed.
 *
 * IOW_TEST=name runs one test; IOW_SEED=n shifts every test's seed;
 * IOW_TRACE=1 prints the simulation's trace.
 */

static uint64_t seed_base;

static uint64_t seed(uint64_t own)
{
    return seed_base != 0 ? seed_base * 1000u + own : own;
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

struct anchored {
    struct iow_group *g;
    uint64_t op;
};

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

/* Every member applies the primary's commitment and checkpoints there. */
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
    for (uint32_t i = 0; i < g->count; ++i) {
        if (g->apps[i] != NULL && g->apps[i]->attached) {
            iow_core_event(g->apps[i], VSR_EVENT_CHECKPOINT, 0, 0, NULL, 0);
        }
    }
    anchor.g = g;
    anchor.op = op;
    CHECK(iow_run_until(group_anchored, &anchor, 5000 * IOW_MS));
    return op;
}

/* The clients files of a replica's directory on its node. */
struct listing {
    uint32_t count;
    char names[16][VSR_SIM_NAME_BYTES];
    uint64_t sizes[16];
    uint32_t temporary; /* Names ending in .tmp. */
};

static void list_clients(const struct iow_app *app, struct listing *out)
{
    uint32_t count = 0;

    memset(out, 0, sizeof(*out));
    CHECK(vsr_sim_dir_count(iow.sim, app->node->index, app->path, &count) == 0);
    for (uint32_t i = 0; i < count; ++i) {
        char name[VSR_SIM_NAME_BYTES];
        uint64_t size = 0;
        size_t length;

        CHECK(vsr_sim_dir_entry(iow.sim, app->node->index, app->path, i, name,
                                sizeof(name), &size) == 0);
        if (strncmp(name, "clients-", 8) != 0) {
            continue;
        }
        length = strlen(name);
        if (length > 4 && strcmp(name + length - 4, ".tmp") == 0) {
            out->temporary++;
            continue;
        }
        CHECK(out->count < 16);
        memcpy(out->names[out->count], name, length + 1);
        out->sizes[out->count] = size;
        out->count++;
    }
}

static void read_file(const struct iow_app *app, const char *name,
                      unsigned char *bytes, size_t size, bool durable)
{
    char path[512];
    size_t read = 0;

    snprintf(path, sizeof(path), "%s/%s", app->path, name);
    if (durable) {
        CHECK(vsr_sim_file_read_durable(iow.sim, app->node->index, path, 0,
                                        bytes, size, &read) == 0);
    } else {
        CHECK(vsr_sim_file_read(iow.sim, app->node->index, path, 0, bytes, size,
                                &read) == 0);
    }
    CHECK(read == size);
}

static uint32_t clients_of(const struct iow_app *app)
{
    struct vsr_io_store_status store;

    vsr_io_replica_status(app->replica, NULL, &store);
    return store.clients;
}

static void fetch_by_stream(struct iow_group *g)
{
    for (uint32_t i = 0; i < g->count; ++i) {
        if (g->apps[i] != NULL) {
            g->apps[i]->fetch_by_stream = true;
        }
    }
}

/* -------------------------------------------------------------------------
 * Capture, fetch, restore
 * ---------------------------------------------------------------------- */

/* Three members commit for four clients and checkpoint: each captures its
 * own clients file (the module's library half) and the application's
 * digest (the caller's half), then syncs and publishes it. A JOIN learner
 * adopts a member's anchor: its engine pulls the clients file over a
 * library stream from the member's node, verifies it as it arrives,
 * renames it into place and forwards the FETCH, whose caller pulls the
 * application's image over a caller stream beside it; the RESTORE loads
 * the file as the learner's client base and INSTALL takes the digest.
 * The fetched file is byte for byte the member's, the learner's store
 * knows every client, and after a crash the learner recovers the anchor's
 * clients file with its log (decision 90). */
static void test_fetch_restore(void)
{
    struct iow_group g;
    struct iow_app *learner;
    struct iow_node *n;
    struct listing mine;
    uint64_t committed;
    bool matched = false;

    iow_open_sim(4, seed(1), NULL);
    g = iow_group_open(3, iow_cluster(1));
    fetch_by_stream(&g);
    iow_group_commit(&g, 8);
    (void)checkpoint_group(&g);
    iow_group_commit(&g, 2);
    for (uint32_t i = 0; i < 3; ++i) {
        CHECK(g.apps[i]->captures >= 1 && g.apps[i]->syncs >= 1);
        CHECK(clients_of(g.apps[i]) == 4);
    }
    iow_authorize(iow_cluster(1), 4, 4);
    n = iow_node_open(3);
    learner = iow_attach(n, 0, iow_cluster(1), 4, 3, VSR_START_JOIN);
    learner->fetch_by_stream = true;
    committed = committed_of(iow_group_primary(&g));
    wait_caught_up(learner, committed);
    CHECK(learner->fetches_ok >= 1 && learner->installs >= 1);
    CHECK(clients_of(learner) == 4);
    /* The fetched clients file is a member's, byte for byte, and no
     * temporary file is left. */
    list_clients(learner, &mine);
    CHECK(mine.count >= 1 && mine.temporary == 0);
    for (uint32_t i = 0; i < 3 && !matched; ++i) {
        struct listing theirs;

        list_clients(g.apps[i], &theirs);
        for (uint32_t k = 0; k < theirs.count; ++k) {
            for (uint32_t m = 0; m < mine.count; ++m) {
                if (strcmp(theirs.names[k], mine.names[m]) == 0) {
                    static unsigned char a[65536];
                    static unsigned char b[65536];

                    CHECK(theirs.sizes[k] == mine.sizes[m]);
                    CHECK(mine.sizes[m] <= sizeof(a));
                    read_file(g.apps[i], theirs.names[k], a, theirs.sizes[k],
                              false);
                    read_file(learner, mine.names[m], b, mine.sizes[m], false);
                    CHECK(memcmp(a, b, mine.sizes[m]) == 0);
                    matched = true;
                }
            }
        }
    }
    CHECK(matched);
    /* A crash and RECOVER: the anchor's clients file comes back with the
     * log. */
    iow_group_commit(&g, 2);
    iow_crash(n);
    iow_restart(n);
    iow_replica_options(&n->apps[0], iow_cluster(1), 4, 3, VSR_START_RECOVER,
                        0);
    learner = iow_attach_with(n, 0);
    learner->fetch_by_stream = true;
    committed = committed_of(iow_group_primary(&g));
    wait_caught_up(learner, committed);
    CHECK(clients_of(learner) == 4);
    g.apps[3] = learner;
    g.count = 4;
    iow_group_close(&g);
    iow_close();
}

/* -------------------------------------------------------------------------
 * Sync and drop
 * ---------------------------------------------------------------------- */

/* Durable replicas checkpoint repeatedly: every clients file a published
 * anchor names is on the medium (the durable prefix the crash model keeps
 * equals the file), and the files of anchors the core moved past are
 * dropped, so a directory never holds more than a few. */
static void test_sync_drop(void)
{
    struct iow_group g;

    iow_open_sim(3, seed(2), NULL);
    g = iow_group_open(3, iow_cluster(2));
    for (uint32_t round = 0; round < 5; ++round) {
        iow_group_commit(&g, 3);
        (void)checkpoint_group(&g);
        iow_run_for(50 * IOW_MS);
        for (uint32_t i = 0; i < 3; ++i) {
            struct listing files;

            list_clients(g.apps[i], &files);
            CHECK(files.count >= 1 && files.count <= 3);
            CHECK(files.temporary == 0);
            for (uint32_t k = 0; k < files.count; ++k) {
                static unsigned char now[65536];
                static unsigned char durable[65536];

                CHECK(files.sizes[k] <= sizeof(now));
                read_file(g.apps[i], files.names[k], now, files.sizes[k],
                          false);
                read_file(g.apps[i], files.names[k], durable, files.sizes[k],
                          true);
                CHECK(memcmp(now, durable, files.sizes[k]) == 0);
            }
        }
    }
    for (uint32_t i = 0; i < 3; ++i) {
        CHECK(g.apps[i]->captures >= 5 && g.apps[i]->drops >= 2);
    }
    iow_group_close(&g);
    iow_close();
}

/* -------------------------------------------------------------------------
 * A corrupt source (decision 123)
 * ---------------------------------------------------------------------- */

static void corrupt_files(const struct iow_app *app)
{
    struct listing files;

    list_clients(app, &files);
    for (uint32_t k = 0; k < files.count; ++k) {
        char path[512];

        snprintf(path, sizeof(path), "%s/%s", app->path, files.names[k]);
        CHECK(vsr_sim_file_corrupt(iow.sim, app->node->index, path,
                                   files.sizes[k] / 2, 1) == 0);
    }
}

struct fetch_failures {
    const struct iow_node *n;
    uint64_t closes;
};

/* Every member's clients file is damaged on its disk: a learner's fetch
 * receives bytes that fail verification and the FETCH completes FAILED,
 * never CORRUPT (that would latch the learner's snapshot failure for the
 * source's damage): the learner is not fenced, its caller never sees the
 * FETCH, and no file stays behind. Once the members checkpoint again
 * (fresh files), the learner's next discovery fetches the new anchor. */
static void test_corrupt_source(void)
{
    struct iow_group g;
    struct iow_app *learner;
    struct iow_node *n;
    struct listing mine;
    struct vsr_status core;
    uint64_t committed;

    iow_open_sim(4, seed(3), NULL);
    g = iow_group_open(3, iow_cluster(3));
    iow_group_commit(&g, 6);
    (void)checkpoint_group(&g);
    for (uint32_t i = 0; i < 3; ++i) {
        corrupt_files(g.apps[i]);
    }
    iow_authorize(iow_cluster(3), 4, 4);
    n = iow_node_open(3);
    learner = iow_attach(n, 0, iow_cluster(3), 4, 3, VSR_START_JOIN);
    iow_run_for(2000 * IOW_MS);
    vsr_io_replica_status(learner->replica, &core, NULL);
    CHECK(core.failure.code == VSR_FAILURE_NONE);
    CHECK(learner->fetches == 0 && learner->installs == 0);
    list_clients(learner, &mine);
    CHECK(mine.count == 0);
    {
        struct vsr_io_stats stats;

        vsr_io_get_stats(n->io, &stats);
        CHECK(stats.streams <= 1);
    }
    /* A fresh anchor. */
    iow_group_commit(&g, 3);
    (void)checkpoint_group(&g);
    committed = committed_of(iow_group_primary(&g));
    wait_caught_up(learner, committed);
    vsr_io_replica_status(learner->replica, &core, NULL);
    CHECK(core.failure.code == VSR_FAILURE_NONE);
    CHECK(learner->fetches_ok >= 1 && learner->installs >= 1);
    list_clients(learner, &mine);
    CHECK(mine.count >= 1 && mine.temporary == 0);
    g.apps[3] = learner;
    g.count = 4;
    iow_group_close(&g);
    iow_close();
}

/* -------------------------------------------------------------------------
 * An outcome decided in the module's prepare (decision 122)
 * ---------------------------------------------------------------------- */

struct dir_match {
    const char *path;
};

/* The snapshot module's open of the store directory. */
static bool dir_open(void *ctx, const struct vsr_io_sqe *sqe)
{
    const struct dir_match *m = ctx;

    return VSR_IO_OWNER(sqe->user_data) == IOW_OWNER &&
           sqe->opcode == VSR_IO_SQE_OPENAT &&
           iow_slot_kind(sqe->user_data) == VSR_IO_SLOT_CLIENTS &&
           sqe->addr != NULL && strcmp(sqe->addr, m->path) == 0;
}

static bool app_failed(void *ctx)
{
    const struct iow_app *app = ctx;

    return app->failed_ns != 0;
}

/* A durable replica whose directory cannot be opened (twice: at attach and
 * the one retry a SNAPSHOT_SYNC earns, decision 119) checkpoints: the
 * clients file's fsync succeeds, the directory's cannot happen, and the
 * module decides the SYNC FAILED in its prepare, after the core's poll
 * and the links' prepare ran. It arms the replica's CAPTURE deadline at
 * the engine's time (122) so the loop comes straight back and the core
 * hears the failure (every non-OK SNAPSHOT_SYNC fences) within the same
 * instant, not at its next heartbeat, a second away here. */
static void test_prepare_wake(void)
{
    struct iow_node *n;
    struct iow_app *app;
    struct iow_group g;
    struct dir_match match;
    struct vsr_status core;

    iow_open_sim(1, seed(4), NULL);
    iow.heartbeat_ns = 1000 * IOW_MS;
    iow.view_timeout_ns = 5000 * IOW_MS;
    n = iow_node_open(0);
    iow_replica_options(&n->apps[0], iow_cluster(4), 1, 1, VSR_START_NEW, 0);
    match.path = n->apps[0].path;
    iow_rule(n, dir_open, &match, IOW_FAIL, -EMFILE, 2);
    app = iow_attach_with(n, 0);
    CHECK(iow_run_until(iow_app_normal, app, 5000 * IOW_MS));
    memset(&g, 0, sizeof(g));
    g.count = 1;
    g.apps[0] = app;
    iow_group_commit(&g, 3);
    CHECK(iow_rule_hits(n) == 1);
    iow_core_event(app, VSR_EVENT_CHECKPOINT, 0, 0, NULL, 0);
    CHECK(iow_run_until(app_failed, app, 5000 * IOW_MS));
    CHECK(iow_rule_hits(n) == 2);
    CHECK(app->syncs == 1 && app->sync_ns != 0);
    vsr_io_replica_status(app->replica, &core, NULL);
    CHECK(core.failure.code != VSR_FAILURE_NONE);
    CHECK(core.failure.operation_type == VSR_OP_SNAPSHOT_SYNC);
    /* Well within the heartbeat: the wake, not the next core deadline. */
    CHECK(app->failed_ns - app->sync_ns < 10 * IOW_MS);
    iow_stop(app);
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
    seed_base = seed_env != NULL ? strtoull(seed_env, NULL, 10) : 0;
#define RUN(test)                                                              \
    do {                                                                       \
        if (only == NULL || strcmp(only, #test) == 0) {                        \
            test();                                                            \
            printf("%s: ok\n", #test);                                         \
        }                                                                      \
    } while (0)
    setvbuf(stdout, NULL, _IONBF, 0);
    RUN(test_fetch_restore);
    RUN(test_sync_drop);
    RUN(test_corrupt_source);
    RUN(test_prepare_wake);
#undef RUN
    return 0;
}
