#include "config.h"

#include "io/slots.h"
#include "lib/check.h"
#include "lib/io_world.h"
#include "vsr-io.h"
#include "vsr-sim.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Reproduction of a core bug found by tests/integration/uring_faults
 * (test_write_errors) over real rings and fixed in transition.c's
 * compare_chunk (docs/handover/engine-phase-2.md), here over the
 * simulation with real engines (tests/lib/io_world).
 *
 * A backup of a replicated group crashes with 20 entries in its log, the
 * group commits 6 more (fewer than batch_entries: dead_backup_apply), and
 * the backup restarts with RECOVER. It replays its committed prefix
 * through log LOADs of one batch each while its transition compares the
 * primary's chunks with its log: a chunk's entries are walked through the
 * log cache, and a comparison LOAD is issued for the first one the cache
 * lacks. With the replay's LOAD of [9, 17) outstanding when the chunk
 * [9, 17) arrived, the comparison LOAD of the same entries went out after
 * it; the replay's completed first and cached them, the next poll walked
 * the chunk through the cache to its end with the comparison still out,
 * the chunk was appended and replaced by [17, 25), and the stale
 * comparison, still matching compare_id, was compared at the new chunk's
 * offset: committed op 9 "differed" from op 17 and the replica fenced
 * itself with VSR_FAILURE_INVARIANT. On a disk the reads complete in
 * either order and in batches; the simulation completes each one after a
 * short latency, before the chunk arrives.
 *
 * So the restarted node's cold LOAD reads are held and complete in
 * batches, every `batch_ms` of simulated time, oldest first, the ring's
 * order. Every replica must stay unfenced and the restarted one catch up
 * and go on committing. Without the fix every batch period from 1 to 20
 * ms fences the backup. BATCH_MS=n runs one period instead of 1 to 8;
 * IOW_SEED=n changes the simulation's seed (1). */

static bool cold_load(void *ctx, const struct vsr_io_sqe *sqe)
{
    (void)ctx;
    return VSR_IO_OWNER(sqe->user_data) == IOW_OWNER &&
           iow_slot_kind(sqe->user_data) == VSR_IO_SLOT_LOAD;
}

static struct vsr_status core_status(const struct iow_app *app)
{
    struct vsr_status core;

    vsr_io_replica_status(app->replica, &core, NULL);
    return core;
}

static void check_unfenced(const struct iow_group *g)
{
    for (uint32_t i = 0; i < g->count; ++i) {
        struct vsr_status core = core_status(g->apps[i]);

        if (core.failure.code != VSR_FAILURE_NONE) {
            fprintf(stderr, "replica %u fenced: failure %u\n", i + 1,
                    (unsigned)core.failure.code);
            iow_dump_node(g->apps[i]->node);
            CHECK(false);
        }
    }
}

static void run(uint64_t seed, uint64_t batch_ms)
{
    struct vsr_io_limits *l = &iow.io_limits;
    struct iow_group g;
    struct iow_app *primary;
    struct iow_node *n;
    uint64_t committed;
    uint32_t index;
    uint32_t batches = 0;
    uint32_t held;

    iow_open_sim(3, seed, NULL);
    /* tests/integration/uring_faults' configuration. */
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
    iow.durability = VSR_REPLICATED;
    g = iow_group_open(3, iow_cluster(1));
    iow_group_commit(&g, 20);
    primary = iow_group_primary(&g);
    CHECK(primary != NULL);
    index = (primary->node->index + 1) % 3;
    n = g.apps[index]->node;
    committed = core_status(primary).committed;
    while (g.apps[index]->applied_op < committed) {
        iow_run_for(IOW_MS);
    }
    iow_crash(n);
    iow_group_commit(&g, 6);
    iow_restart(n);
    iow_replica_options(&n->apps[0], iow_cluster(1), index + 1, 3,
                        VSR_START_RECOVER, 0);
    iow_rule(n, cold_load, NULL, IOW_HOLD, 0, UINT32_MAX);
    g.apps[index] = iow_attach_with(n, 0);
    committed = core_status(primary).committed;
    while (g.apps[index]->applied_op < committed) {
        iow_run_for(batch_ms * IOW_MS);
        check_unfenced(&g);
        /* The batch completes at the node's next reap, oldest first; the
         * reads issued after it are held again. */
        iow_release_held(n);
        iow_iterate(n);
        iow_hold(n);
        CHECK(++batches < 20000);
    }
    held = iow_rule_hits(n);
    CHECK(held > 0);
    iow_rules_clear(n);
    iow_release_held(n);
    iow_group_commit(&g, 2);
    check_unfenced(&g);
    printf("seed %" PRIu64 ", batch %" PRIu64 " ms: %u batches, %u reads\n",
           seed, batch_ms, batches, held);
    iow_group_close(&g);
    iow_close();
}

int main(void)
{
    const char *seed_env = getenv("IOW_SEED");
    const char *batch_env = getenv("BATCH_MS");
    uint64_t seed = seed_env != NULL ? strtoull(seed_env, NULL, 10) : 1;

    setvbuf(stdout, NULL, _IONBF, 0);
    if (batch_env != NULL) {
        run(seed, strtoull(batch_env, NULL, 10));
        return 0;
    }
    for (uint64_t batch_ms = 1; batch_ms <= 8; ++batch_ms) {
        run(seed, batch_ms);
    }
    return 0;
}
