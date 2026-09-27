#include "common.h"
#include "config.h"

int main(void)
{
    for (uint32_t policy = VSR_DURABLE; policy <= VSR_REPLICATED; ++policy) {
        struct example_cluster cluster = example_create(3, 1, policy, NULL);
        example_command(cluster.nodes[0], 1, 1, 0, 1, "set counter 7");
        example_run(&cluster);
        uint64_t op = example_reply(cluster.nodes[0], 1)->op;
        CHECK(op == 1);

        /* The same client incarnation/number and bytes identify a retry. */
        example_command(cluster.nodes[0], 1, 1, 0, 2, "set counter 7");
        example_run(&cluster);
        CHECK(example_reply(cluster.nodes[0], 2)->op == op);
        CHECK(example_status(cluster.nodes[0]).applied == op);
        struct vsr_status status = example_status(cluster.nodes[0]);
        printf("%s: request and retry both return op %" PRIu64
               "; readable revision %" PRIu64 ", durable revision %" PRIu64
               "\n",
               policy == VSR_DURABLE ? "durable" : "replicated", op,
               status.stored_sequence, status.durable_sequence);
        mem_cluster_destroy(cluster.host);
    }
    return 0;
}
