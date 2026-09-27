#include "common.h"
#include "config.h"

/* Finish network/storage work while the host retains READ_READY. Production
 * hosts complete that effect after capturing their application snapshot. */
static void pump_with_held_snapshot(struct example_cluster *cluster)
{
    for (uint32_t steps = 0; steps < 100000; ++steps) {
        bool progress = false;
        for (uint32_t i = 0; i < cluster->count; ++i) {
            struct mem_node *node = cluster->nodes[i];
            struct mem_step step = mem_node_event(node, NULL);
            progress |= step.emitted != 0 || (step.flags & VSR_UPDATE_MORE);
            for (size_t j = 0; j < mem_node_effects(node); ++j) {
                if (mem_node_effect(node, j)->type == VSR_OP_READ_READY)
                    continue;
                if (mem_node_complete(node, j, VSR_IO_OK)) {
                    progress = true;
                    break;
                }
            }
        }
        for (size_t i = 0; i < mem_cluster_messages(cluster->host); ++i) {
            if (mem_cluster_deliver(cluster->host, i)) {
                progress = true;
                break;
            }
        }
        if (!progress)
            return;
    }
    CHECK(false);
}

static void read_barrier(struct mem_node *node, uint64_t cookie,
                         uint32_t consistency, uint64_t min_op)
{
    const struct vsr_read_barrier barrier = {min_op, VSR_NO_DEADLINE,
                                             consistency, 0};
    const struct vsr_event event = {VSR_EVENT_READ, VSR_IO_OK, cookie, &barrier,
                                    1};
    CHECK(mem_node_event(node, &event).consumed == 1);
}

int main(void)
{
    struct example_cluster cluster = example_create(3, 1, VSR_DURABLE, NULL);
    example_command(cluster.nodes[0], 1, 1, 0, 1, "initial value");
    example_run(&cluster);
    read_barrier(cluster.nodes[0], 10, VSR_READ_LINEARIZABLE, 1);
    pump_with_held_snapshot(&cluster);
    size_t held = SIZE_MAX;
    for (size_t i = 0; i < mem_node_effects(cluster.nodes[0]); ++i)
        if (mem_node_effect(cluster.nodes[0], i)->type == VSR_OP_READ_READY)
            held = i;
    CHECK(held != SIZE_MAX);
    const struct vsr_read_fence fence =
        *(const struct vsr_read_fence *)mem_node_effect(cluster.nodes[0], held)
             ->data;
    CHECK(fence.applied == 1);

    example_command(cluster.nodes[0], 1, 2, 0, 2, "next value");
    pump_with_held_snapshot(&cluster);
    struct vsr_status status = example_status(cluster.nodes[0]);
    CHECK(status.committed == 2 && status.applied == 1);
    /* Other completions can move array positions; identify the retained effect. */
    for (size_t i = 0; i < mem_node_effects(cluster.nodes[0]); ++i)
        if (mem_node_effect(cluster.nodes[0], i)->type == VSR_OP_READ_READY) {
            CHECK(mem_node_complete(cluster.nodes[0], i, VSR_IO_OK));
            break;
        }
    example_run(&cluster);
    CHECK(example_status(cluster.nodes[0]).applied == 2);

    read_barrier(cluster.nodes[1], 11, VSR_READ_CAUSAL, 2);
    example_run(&cluster);
    CHECK(mem_node_reads(cluster.nodes[1]) == 1);
    CHECK(mem_node_read(cluster.nodes[1], 0)->applied >= 2);
    printf("linear read captured op %" PRIu64 "; held fence delayed APPLY "
           "of committed op 2; causal backup read reached op 2\n",
           fence.applied);
    mem_cluster_destroy(cluster.host);
    return 0;
}
