/*
 * epochs: reconfiguration hands the whole history to a disjoint new group.
 *
 * Scenario:
 *   1. Epoch 0 is replicas 1 and 2 (full) plus replica 3 (witness). Client A
 *      commits "set owner old-group".
 *   2. Replicas 4, 5 (full) and 6 (witness) join as nonvoting learners and
 *      warm up from the current group.
 *   3. Client A submits a RECONFIGURE request naming {4, 5, 6} as epoch 1.
 *      The request commits as a log entry; the old group transfers the
 *      history through that boundary and the new group takes over.
 *   4. Once the new group has promised the handoff, the old replicas retire.
 *      A CHECK_EPOCH request logged in epoch 1 certifies that readiness.
 *   5. Client B reads "owner" through the new primary: state survived.
 *
 * What to look for: every replica reports epoch 1 in the steady phase; the
 * old three are RETIRED and the new three are NORMAL; the witness in the new
 * group never applies anything.
 */
#include "common.h"
#include "config.h"

enum { OLD_REPLICAS = 3, FAULTS = 1, NEW_EPOCH = 1 };

static const uint32_t ROLES[] = {VSR_MEMBER_FULL, VSR_MEMBER_FULL,
                                 VSR_MEMBER_WITNESS};

int main(void)
{
    struct example_cluster cluster =
        example_start("epochs", OLD_REPLICAS, FAULTS, VSR_DURABLE, ROLES);
    struct mem_node *old_primary = example_primary(&cluster);
    struct example_client client_a = example_client("client A", 1);
    struct example_client client_b = example_client("client B", 2);
    char text[EXAMPLE_TEXT_BYTES];

    CHECK(example_call(&cluster, old_primary, &client_a, "set owner old-group")
              ->op == 1);

    struct mem_node *new_members[OLD_REPLICAS];
    for (uint32_t i = 0; i < OLD_REPLICAS; ++i)
        new_members[i] = example_join(&cluster, ROLES[i]);
    example_run(&cluster);

    const struct vsr_member next_members[] = {{4, VSR_MEMBER_FULL, 0},
                                              {5, VSR_MEMBER_FULL, 0},
                                              {6, VSR_MEMBER_WITNESS, 0}};
    const struct vsr_membership next_group = {NEW_EPOCH, next_members,
                                              OLD_REPLICAS, FAULTS};
    say("client A asks replica %" PRIu64 " to reconfigure into epoch %d = "
        "{replicas 4, 5 (full), 6 (witness)}",
        mem_node_id(old_primary), NEW_EPOCH);
    const uint64_t reconfigure = example_submit_request(
        &cluster, old_primary, &client_a, VSR_REQUEST_RECONFIGURE, &next_group);
    example_run(&cluster);
    CHECK(example_reply(old_primary, reconfigure)->status == VSR_REPLY_OK);
    say("-> OK: the reconfiguration committed as op %" PRIu64,
        example_reply(old_primary, reconfigure)->op);

    for (uint32_t i = 0; i < cluster.count; ++i) {
        struct vsr_status status = example_status(cluster.nodes[i]);
        const bool old = i < OLD_REPLICAS;
        CHECK(status.epoch == NEW_EPOCH);
        CHECK(status.configuration->phase == VSR_EPOCH_STEADY);
        CHECK(status.state == (old ? VSR_STATE_RETIRED : VSR_STATE_NORMAL));
        say("replica %" PRIu64 ": epoch %" PRIu64 ", %s%s",
            mem_node_id(cluster.nodes[i]), status.epoch,
            example_state_name(status.state),
            old ? " (donor, handoff promised by the new group)" : "");
    }

    struct mem_node *new_primary = example_primary(&cluster);
    CHECK(new_primary == new_members[0]);
    say("replica %" PRIu64 " is primary of epoch %d in view %" PRIu64,
        mem_node_id(new_primary), NEW_EPOCH, example_status(new_primary).view);

    /* Clients route by epoch; a CHECK_EPOCH fence proves the handoff is done. */
    client_b.epoch = NEW_EPOCH;
    const struct vsr_check_epoch check = {NEW_EPOCH};
    const uint64_t route = example_submit_request(
        &cluster, new_primary, &client_b, VSR_REQUEST_CHECK_EPOCH, &check);
    example_run(&cluster);
    const struct vsr_reply *certified = example_reply(new_primary, route);
    CHECK(certified->status == VSR_REPLY_OK);
    say("client B's CHECK_EPOCH %d -> OK (op %" PRIu64 "): the old donors may "
        "stop",
        NEW_EPOCH, certified->op);

    const struct vsr_reply *read =
        example_call(&cluster, new_primary, &client_b, "get owner");
    CHECK(strcmp(example_result(read, text, sizeof(text)), "old-group") == 0);
    CHECK(example_status(new_members[2]).applied == 0);
    CHECK(example_store(new_members[2])->commands == 0);
    say("witness replica 6 voted but applied nothing");
    example_show_store(new_primary);
    example_finish(&cluster);
    return 0;
}
