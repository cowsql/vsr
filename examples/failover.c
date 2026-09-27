/*
 * failover: the primary crashes, the group elects a new one, and the crashed
 * replica later restarts and catches up.
 *
 * Scenario:
 *   1. Replica 1 leads view 0 and commits "set leader alice".
 *   2. Replica 1 crashes. Nothing happens until logical time passes: once the
 *      backups miss the primary's heartbeats for a view timeout they start a
 *      view change, and replica 2 becomes primary of view 1.
 *   3. Client B commits "set leader bob" through the new primary.
 *   4. Replica 1 restarts with a fresh incarnation. Its durable storage
 *      survived but its memory did not; it recovers, learns about view 1, and
 *      re-executes the log to rebuild its store.
 *
 * What to look for: the view number increases exactly once, the new primary
 * assigns op 2, and after the restart replica 1's store matches (two commands
 * executed here again, because the store was rebuilt by replay).
 */
#include "common.h"
#include "config.h"

enum { REPLICAS = 3, FAULTS = 1 };

int main(void)
{
    struct example_cluster cluster =
        example_start("failover", REPLICAS, FAULTS, VSR_DURABLE, NULL);
    const struct vsr_membership seed = example_seed(&cluster);
    const uint64_t view_timeout = mem_options(1, &seed).view_timeout_ns;
    struct mem_node *replica_1 = example_replica(&cluster, 1);
    struct mem_node *replica_2 = example_replica(&cluster, 2);
    struct example_client client_a = example_client("client A", 1);
    struct example_client client_b = example_client("client B", 2);

    CHECK(example_primary(&cluster) == replica_1);
    CHECK(
        example_call(&cluster, replica_1, &client_a, "set leader alice")->op ==
        1);

    example_crash(&cluster, replica_1);
    say("let %" PRIu64 " ns pass, more than the view timeout of %" PRIu64 " ns",
        view_timeout + 10, view_timeout);
    example_elapse(&cluster, view_timeout + 10);
    struct vsr_status status = example_status(replica_2);
    CHECK(status.state == VSR_STATE_NORMAL && status.primary == 2);
    CHECK(status.view == 1);
    say("view change -> replica %" PRIu64 " is primary in view %" PRIu64,
        status.primary, status.view);
    CHECK(example_primary(&cluster) == replica_2);

    CHECK(example_call(&cluster, replica_2, &client_b, "set leader bob")->op ==
          2);

    example_restart(&cluster, replica_1);
    say("let another view timeout pass so recovery and catch-up complete");
    example_elapse(&cluster, view_timeout);
    status = example_status(replica_1);
    CHECK(status.state == VSR_STATE_NORMAL && status.view == 1);
    CHECK(status.primary == 2 && status.applied == 2);
    say("replica 1 is back in view %" PRIu64 " as a backup, applied through op "
        "%" PRIu64,
        status.view, status.applied);
    CHECK(strcmp(kv_get(example_store(replica_1), "leader"), "bob") == 0);
    CHECK(example_store(replica_1)->commands == 2);
    example_show_store(replica_1);
    example_show_store(replica_2);
    example_finish(&cluster);
    return 0;
}
