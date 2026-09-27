#include "common.h"
#include "config.h"

int main(void)
{
    struct example_cluster cluster = example_create(3, 1, VSR_DURABLE, NULL);
    example_command(cluster.nodes[0], 1, 1, 0, 1, "before failover");
    example_run(&cluster);
    mem_node_crash(cluster.nodes[0]);
    example_time(&cluster, 60);

    struct vsr_status leader = example_status(cluster.nodes[1]);
    CHECK(leader.state == VSR_STATE_NORMAL && leader.primary == 2);
    example_command(cluster.nodes[1], 2, 1, 0, 2, "after failover");
    example_run(&cluster);
    CHECK(example_reply(cluster.nodes[1], 2)->op == 2);
    printf("replica 1 crashed; replica 2 committed op 2 in view %" PRIu64 "\n",
           leader.view);

    CHECK(mem_node_restart(cluster.nodes[0], (struct vsr_id){1, 2}) == VSR_OK);
    CHECK(mem_node_time(cluster.nodes[0], 60).consumed == 1);
    example_run(&cluster);
    for (uint64_t time = 65; time <= 100; time += 5)
        example_time(&cluster, time);
    struct vsr_status recovered = example_status(cluster.nodes[0]);
    CHECK(recovered.state == VSR_STATE_NORMAL && recovered.applied == 2);
    printf("replica 1 rebuilt and caught up through op %" PRIu64 "\n",
           recovered.applied);
    mem_cluster_destroy(cluster.host);
    return 0;
}
