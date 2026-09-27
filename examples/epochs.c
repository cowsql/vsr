#include "common.h"
#include "config.h"

int main(void)
{
    const uint32_t roles[] = {VSR_MEMBER_FULL, VSR_MEMBER_FULL,
                              VSR_MEMBER_WITNESS};
    struct example_cluster cluster = example_create(3, 1, VSR_DURABLE, roles);
    struct vsr_member old[] = {{1, VSR_MEMBER_FULL, 0},
                               {2, VSR_MEMBER_FULL, 0},
                               {3, VSR_MEMBER_WITNESS, 0}};
    struct vsr_membership seed = {0, old, 3, 1};
    for (uint32_t i = 3; i < 6; ++i) {
        struct vsr_options options = mem_options(i + 1, &seed);
        options.start_mode = VSR_START_JOIN;
        options.join_role = roles[i - 3];
        cluster.nodes[i] = mem_cluster_add(cluster.host, &options);
        CHECK(mem_node_time(cluster.nodes[i], 0).consumed == 1);
    }
    cluster.count = 6;
    example_run(&cluster);

    struct vsr_member members[] = {{4, VSR_MEMBER_FULL, 0},
                                   {5, VSR_MEMBER_FULL, 0},
                                   {6, VSR_MEMBER_WITNESS, 0}};
    struct vsr_membership next = {1, members, 3, 1};
    const struct vsr_request request = {
        {{100, 1}, 1}, 0, VSR_REQUEST_RECONFIGURE, 0, &next};
    example_request(cluster.nodes[0], &request, 1);
    example_run(&cluster);
    for (uint32_t i = 0; i < 6; ++i) {
        struct vsr_status status = example_status(cluster.nodes[i]);
        CHECK(status.epoch == 1);
        CHECK(status.configuration->phase == VSR_EPOCH_STEADY);
        CHECK(status.state == (i < 3 ? VSR_STATE_RETIRED : VSR_STATE_NORMAL));
    }

    const struct vsr_check_epoch check = {1};
    const struct vsr_request barrier = {
        {{100, 2}, 1}, 1, VSR_REQUEST_CHECK_EPOCH, 0, &check};
    example_request(cluster.nodes[3], &barrier, 2);
    example_run(&cluster);
    CHECK(example_reply(cluster.nodes[3], 2)->status == VSR_REPLY_OK);
    CHECK(example_status(cluster.nodes[5]).applied == 0);
    puts("epoch 1 ready: replicas 4 and 5 execute, replica 6 witnesses; "
         "old replicas 1..3 retired after handoff promises");
    mem_cluster_destroy(cluster.host);
    return 0;
}
