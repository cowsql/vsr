/*
 * replication: three replicas agree on one ordered history of commands.
 *
 * Scenario, run once per durability policy:
 *   1. Client A sends "set counter 7" to the primary. The primary assigns the
 *      command log position 1, replicates it to a quorum, and every full
 *      replica executes it against its key-value store.
 *   2. Client A retries the very same request (same client ID and request
 *      number). The primary recognizes the duplicate and resends the cached
 *      reply: same op, same result, and the store executes nothing new.
 *   3. Client A continues with "incr counter" and "get counter", which the log
 *      orders after the first command.
 *
 * What to look for: the retry reports op 1 again and the primary's command
 * count stays where it was; the readable/durable storage frontiers differ
 * between the two policies (VSR_REPLICATED never syncs before replying).
 */
#include "common.h"
#include "config.h"

enum { REPLICAS = 3, FAULTS = 1 };

static void replicate_with_policy(uint32_t durability)
{
    struct example_cluster cluster = example_start(
        durability == VSR_DURABLE ? "replication (durable storage)"
                                  : "replication (replicated storage)",
        REPLICAS, FAULTS, durability, NULL);
    struct mem_node *primary = example_primary(&cluster);
    struct example_client client_a = example_client("client A", 1);
    char text[EXAMPLE_TEXT_BYTES];

    const struct vsr_reply *first =
        example_call(&cluster, primary, &client_a, "set counter 7");
    CHECK(first->status == VSR_REPLY_OK && first->op == 1);
    CHECK(strcmp(example_result(first, text, sizeof(text)), "7") == 0);
    for (uint32_t i = 0; i < cluster.count; ++i)
        CHECK(strcmp(kv_get(example_store(cluster.nodes[i]), "counter"), "7") ==
              0);
    say("all %" PRIu32 " replicas now hold counter=7", cluster.count);

    /* A retry keeps the client ID, request number, and command bytes. */
    const uint64_t executed_before = example_store(primary)->commands;
    const struct vsr_reply *again =
        example_retry(&cluster, primary, &client_a, "set counter 7");
    CHECK(again->status == VSR_REPLY_OK && again->op == first->op);
    CHECK(strcmp(example_result(again, text, sizeof(text)), "7") == 0);
    CHECK(example_store(primary)->commands == executed_before);
    say("the retry was answered from the client table: op %" PRIu64
        " again, nothing re-executed",
        again->op);

    const struct vsr_reply *incremented =
        example_call(&cluster, primary, &client_a, "incr counter");
    CHECK(incremented->op == 2);
    CHECK(strcmp(example_result(incremented, text, sizeof(text)), "8") == 0);
    const struct vsr_reply *read =
        example_call(&cluster, primary, &client_a, "get counter");
    CHECK(read->op == 3);
    CHECK(strcmp(example_result(read, text, sizeof(text)), "8") == 0);

    struct vsr_status status = example_status(primary);
    CHECK(status.committed == 3 && status.applied == 3);
    say("primary frontiers: committed op %" PRIu64 ", applied op %" PRIu64
        ", readable storage revision %" PRIu64 ", durable revision %" PRIu64,
        status.committed, status.applied, status.stored_sequence,
        status.durable_sequence);
    if (durability == VSR_DURABLE)
        CHECK(status.durable_sequence == status.stored_sequence);
    else
        CHECK(status.durable_sequence < status.stored_sequence);
    example_show_store(primary);
    example_finish(&cluster);
}

int main(void)
{
    replicate_with_policy(VSR_DURABLE);
    replicate_with_policy(VSR_REPLICATED);
    return 0;
}
