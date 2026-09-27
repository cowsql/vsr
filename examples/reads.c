/*
 * reads: serving reads without logging them.
 *
 * A linearizable read is a barrier, not a log entry. The primary confirms
 * with a fresh quorum that it still leads, then hands the host a READ_READY
 * effect naming the applied position the read may observe. The host reads its
 * application state and completes the effect. Until it does, the core will
 * not APPLY anything newer, so the state the host is reading cannot move.
 * A causal read on a backup only waits until that backup has applied the
 * caller's minimum op.
 *
 * Scenario:
 *   1. Client A commits "set counter 1".
 *   2. Replica 1 receives a linearizable read barrier. The host runs every
 *      other effect but deliberately keeps READ_READY pending: it is "reading".
 *   3. Client A submits "incr counter". It commits (op 2) but is not applied
 *      while the fence is held: the store still says counter=1.
 *   4. The host completes the fence; op 2 is applied and counter becomes 2.
 *   5. Backup replica 2 serves a causal read with minimum op 2.
 *
 * What to look for: committed runs ahead of applied while the fence is held,
 * and the store on replica 1 does not change until the fence is released.
 */
#include "common.h"
#include "config.h"

enum { REPLICAS = 3, FAULTS = 1 };
enum { COOKIE_LINEARIZABLE = 10, COOKIE_CAUSAL = 11 }; /* Local read IDs. */

static void read_barrier(struct mem_node *node, uint64_t cookie,
                         uint32_t consistency, uint64_t min_op)
{
    const struct vsr_read_barrier barrier = {min_op, VSR_NO_DEADLINE,
                                             consistency, 0};
    const struct vsr_event event = {VSR_EVENT_READ, VSR_IO_OK, cookie, &barrier,
                                    1};
    CHECK(mem_node_event(node, &event).consumed == 1);
}

/* The pending READ_READY effect on a replica, or SIZE_MAX. Completing other
 * effects shifts queue positions, so the search is repeated when needed. */
static size_t pending_fence(struct mem_node *node)
{
    for (size_t i = 0; i < mem_node_effects(node); ++i)
        if (mem_node_effect(node, i)->type == VSR_OP_READ_READY)
            return i;
    return SIZE_MAX;
}

int main(void)
{
    struct example_cluster cluster =
        example_start("reads", REPLICAS, FAULTS, VSR_DURABLE, NULL);
    struct mem_node *primary = example_primary(&cluster);
    struct mem_node *backup = example_replica(&cluster, 2);
    struct example_client client_a = example_client("client A", 1);

    CHECK(example_call(&cluster, primary, &client_a, "set counter 1")->op == 1);

    say("linearizable read barrier on replica %" PRIu64 " (min op 1)",
        mem_node_id(primary));
    read_barrier(primary, COOKIE_LINEARIZABLE, VSR_READ_LINEARIZABLE, 1);
    example_run_holding(&cluster, primary, VSR_OP_READ_READY);
    size_t held = pending_fence(primary);
    CHECK(held != SIZE_MAX);
    const struct vsr_read_fence fence =
        *(const struct vsr_read_fence *)mem_node_effect(primary, held)->data;
    CHECK(fence.cookie == COOKIE_LINEARIZABLE && fence.applied == 1);
    say("-> READ_READY at applied op %" PRIu64
        "; the host keeps it pending while it reads counter=%s",
        fence.applied, kv_get(example_store(primary), "counter"));

    say("client A sends \"incr counter\" while the fence is held");
    example_submit(&cluster, primary, &client_a, "incr counter");
    example_run_holding(&cluster, primary, VSR_OP_READ_READY);
    struct vsr_status status = example_status(primary);
    CHECK(status.committed == 2 && status.applied == 1);
    CHECK(strcmp(kv_get(example_store(primary), "counter"), "1") == 0);
    say("-> op 2 is committed but not applied: counter is still %s",
        kv_get(example_store(primary), "counter"));

    say("the host completes the fence");
    held = pending_fence(primary);
    CHECK(held != SIZE_MAX);
    CHECK(mem_node_complete(primary, held, VSR_IO_OK));
    example_run(&cluster);
    status = example_status(primary);
    CHECK(status.applied == 2);
    CHECK(strcmp(kv_get(example_store(primary), "counter"), "2") == 0);
    say("-> op 2 applied: counter is now %s",
        kv_get(example_store(primary), "counter"));

    say("causal read barrier on backup replica %" PRIu64 " (min op 2)",
        mem_node_id(backup));
    read_barrier(backup, COOKIE_CAUSAL, VSR_READ_CAUSAL, 2);
    example_run(&cluster);
    CHECK(mem_node_reads(backup) == 1);
    CHECK(mem_node_read(backup, 0)->cookie == COOKIE_CAUSAL);
    CHECK(mem_node_read(backup, 0)->applied >= 2);
    say("-> READ_READY at applied op %" PRIu64
        " without contacting the primary",
        mem_node_read(backup, 0)->applied);
    example_show_store(backup);
    example_finish(&cluster);
    return 0;
}
