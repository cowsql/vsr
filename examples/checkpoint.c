#include "common.h"
#include "config.h"

int main(void)
{
    struct example_cluster cluster = example_create(3, 1, VSR_DURABLE, NULL);
    for (uint64_t number = 1; number <= 5; ++number) {
        example_command(cluster.nodes[0], 1, number, 0, number, "increment");
        example_run(&cluster);
    }
    const struct vsr_event hint = {VSR_EVENT_CHECKPOINT, VSR_IO_OK, 0, NULL, 0};
    CHECK(mem_node_event(cluster.nodes[0], &hint).consumed == 1);
    example_run(&cluster);
    CHECK(example_status(cluster.nodes[0]).checkpoint_op == 5);

    struct vsr_member members[] = {{1, VSR_MEMBER_FULL, 0},
                                   {2, VSR_MEMBER_FULL, 0},
                                   {3, VSR_MEMBER_FULL, 0}};
    struct vsr_membership seed = {0, members, 3, 1};
    struct vsr_options options = mem_options(4, &seed);
    options.start_mode = VSR_START_JOIN;
    options.join_role = VSR_MEMBER_FULL;
    struct mem_node *learner = mem_cluster_add(cluster.host, &options);
    CHECK(mem_node_time(learner, 0).consumed == 1);
    example_run(&cluster);
    struct vsr_status status = example_status(learner);
    CHECK(status.state == VSR_STATE_WARMING && status.applied == 5);
    CHECK(status.checkpoint_op == 5);
    printf("checkpoint 5 published; nonvoting replica 4 fetched and installed "
           "it, reaching op %" PRIu64 "\n",
           status.applied);
    mem_cluster_destroy(cluster.host);
    return 0;
}
