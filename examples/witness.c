/*
 * witness: a group of two full replicas and one witness.
 *
 * A witness votes and stores the log but never executes commands, so it needs
 * no application state. The group still tolerates one failure: with n = 3 and
 * f = 1, any two members form a quorum, and the two full members guarantee
 * that at least f + 1 = 2 replicas carry the application state.
 *
 * Scenario:
 *   1. Client A commits "set leader alice". Both full replicas execute it;
 *      the witness only records the log entry.
 *   2. Full replica 2 crashes. Client A commits "set leader bob" anyway: the
 *      primary's own vote plus the witness's PREPARE_OK form the quorum.
 *   3. Replica 2 restarts and catches up from the primary.
 *
 * What to look for: the witness's PREPARE_OK count is nonzero, its applied
 * position stays 0, and its store never executes a command.
 */
#include "common.h"
#include "config.h"

enum { REPLICAS = 3, FAULTS = 1, WITNESS_ID = 3 };

static const uint32_t ROLES[] = {VSR_MEMBER_FULL, VSR_MEMBER_FULL,
                                 VSR_MEMBER_WITNESS};

static void check_witness_applied_nothing(struct mem_node *witness)
{
    struct vsr_status status = example_status(witness);
    CHECK(status.role == VSR_MEMBER_WITNESS);
    CHECK(status.applied == 0);
    CHECK(mem_node_applied(witness) == 0);
    CHECK(example_store(witness)->commands == 0);
    say("witness replica %" PRIu64 ": committed op %" PRIu64
        ", applied op %" PRIu64 ", never received APPLY",
        mem_node_id(witness), status.committed, status.applied);
}

int main(void)
{
    struct example_cluster cluster =
        example_start("witness", REPLICAS, FAULTS, VSR_DURABLE, ROLES);
    const uint64_t view_timeout = mem_options(1, &cluster.seed).view_timeout_ns;
    struct mem_node *primary = example_primary(&cluster);
    struct mem_node *backup = example_replica(&cluster, 2);
    struct mem_node *witness = example_replica(&cluster, WITNESS_ID);
    struct example_client client_a = example_client("client A", 1);

    CHECK(example_call(&cluster, primary, &client_a, "set leader alice")->op ==
          1);
    CHECK(example_status(backup).applied == 1);
    check_witness_applied_nothing(witness);

    example_crash(&cluster, backup);
    say("client A sends \"set leader bob\" with only the primary and the "
        "witness alive");
    const uint64_t route =
        example_submit(&cluster, primary, &client_a, "set leader bob");
    const size_t votes =
        example_run_counting(&cluster, WITNESS_ID, VSR_MSG_PREPARE_OK);
    const struct vsr_reply *reply = example_reply(primary, route);
    CHECK(reply->status == VSR_REPLY_OK && reply->op == 2);
    CHECK(votes >= 1);
    say("-> OK (op %" PRIu64 "): the witness sent %zu PREPARE_OK vote%s to "
        "complete the quorum",
        reply->op, votes, votes == 1 ? "" : "s");
    check_witness_applied_nothing(witness);

    example_restart(&cluster, backup);
    example_elapse(&cluster, view_timeout);
    struct vsr_status status = example_status(backup);
    CHECK(status.state == VSR_STATE_NORMAL && status.applied == 2);
    CHECK(strcmp(kv_get(example_store(backup), "leader"), "bob") == 0);
    say("replica 2 caught up through op %" PRIu64, status.applied);
    example_show_store(primary);
    example_show_store(backup);
    check_witness_applied_nothing(witness);
    example_finish(&cluster);
    return 0;
}
