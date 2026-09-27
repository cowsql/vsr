/*
 * checkpoint: a replica captures a snapshot of its application state; a new
 * learner bootstraps from that image, and the replica itself restarts from it.
 *
 * Scenario:
 *   1. Client A increments "counter" five times; the log holds ops 1..5.
 *   2. The primary is asked to checkpoint. The core issues SNAPSHOT_CAPTURE;
 *      the host serializes the key-value map ("counter=5") as the image and
 *      the core publishes the checkpoint at op 5.
 *   3. Replica 4 joins as a nonvoting learner. It has no history, so it
 *      fetches the published image from a peer, installs it, and is caught
 *      up through op 5 without replaying anything.
 *   4. Replica 1 crashes and restarts. Its memory is gone but the checkpoint
 *      is durable, so recovery installs the local image instead of
 *      re-executing the log from genesis.
 *
 * What to look for: both the learner and the restarted replica hold
 * counter=5 while reporting zero commands executed: the state came from the
 * image, not from replay.
 */
#include "common.h"
#include "config.h"

enum { REPLICAS = 3, FAULTS = 1, INCREMENTS = 5 };

static void check_rebuilt_from_image(struct mem_node *node)
{
    struct vsr_status status = example_status(node);
    CHECK(status.checkpoint_op == INCREMENTS && status.applied == INCREMENTS);
    CHECK(strcmp(kv_get(example_store(node), "counter"), "5") == 0);
    CHECK(example_store(node)->commands == 0);
    say("replica %" PRIu64 " (%s) installed the checkpoint image: applied op "
        "%" PRIu64 " without executing any command",
        mem_node_id(node), example_state_name(status.state), status.applied);
    example_show_store(node);
}

int main(void)
{
    struct example_cluster cluster =
        example_start("checkpoint", REPLICAS, FAULTS, VSR_DURABLE, NULL);
    struct mem_node *primary = example_primary(&cluster);
    struct example_client client_a = example_client("client A", 1);
    char text[EXAMPLE_TEXT_BYTES];

    for (uint32_t i = 1; i <= INCREMENTS; ++i) {
        const struct vsr_reply *reply =
            example_call(&cluster, primary, &client_a, "incr counter");
        CHECK(reply->op == i);
        CHECK((uint32_t)atoi(example_result(reply, text, sizeof(text))) == i);
    }
    CHECK(example_store(primary)->commands == INCREMENTS);

    example_checkpoint(&cluster, primary);
    CHECK(example_status(primary).checkpoint_op == INCREMENTS);
    say("the checkpoint image is the serialized store: \"counter=%d\"",
        INCREMENTS);

    struct mem_node *learner = example_join(&cluster, VSR_MEMBER_FULL);
    example_run(&cluster);
    CHECK(example_status(learner).state == VSR_STATE_WARMING);
    check_rebuilt_from_image(learner);

    example_crash(&cluster, primary);
    example_restart(&cluster, primary);
    CHECK(example_status(primary).state == VSR_STATE_NORMAL);
    check_rebuilt_from_image(primary);
    example_finish(&cluster);
    return 0;
}
