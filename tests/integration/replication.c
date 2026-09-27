#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <stddef.h>
#include <stdint.h>

static void run(struct mem_cluster *cluster)
{
    CHECK(mem_cluster_run(cluster, 100000) < 100000);
    mem_cluster_check(cluster);
}

static void submit(struct mem_node *node, struct vsr_id client, uint64_t number,
                   uint64_t route, const struct vsr_blob *body)
{
    const struct vsr_request request = {
        {client, number}, 0, VSR_REQUEST_COMMAND, 0, body};
    const struct vsr_event event = {VSR_EVENT_REQUEST, VSR_IO_OK, route,
                                    &request, 1};
    struct mem_step step = mem_node_event(node, &event);
    CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
    CHECK(step.consumed == 1);
}

static const struct vsr_reply *reply_for(const struct mem_node *node,
                                         uint64_t route)
{
    for (size_t i = mem_node_replies(node); i > 0; --i) {
        uint64_t found = 0;
        const struct vsr_reply *reply = mem_node_reply(node, i - 1, &found);
        if (found == route) {
            return reply;
        }
    }
    CHECK(false);
    return NULL;
}

static void normal_group(uint32_t count, uint32_t faults, bool witness,
                         uint32_t durability, bool small_cache)
{
    struct vsr_member members[5];
    struct mem_node *nodes[5];
    struct mem_cluster *cluster = mem_cluster_create();
    struct vsr_membership membership = {0, members, count, faults};
    const struct vsr_span first_spans[] = {{"abc", 3}, {"defgh", 5}};
    const struct vsr_span retry_spans[] = {{"abcdefgh", 8}};
    const struct vsr_blob first = {first_spans, 8, 2, 0};
    const struct vsr_blob retry = {retry_spans, 8, 1, 0};
    const struct vsr_id client = {100, 200};
    uint64_t last_op;

    CHECK(count <= 5);
    for (uint32_t i = 0; i < count; ++i) {
        members[i] = (struct vsr_member){
            (uint64_t)i + 1,
            witness && i == count - 1 ? VSR_MEMBER_WITNESS : VSR_MEMBER_FULL,
            0};
    }
    for (uint32_t i = 0; i < count; ++i) {
        struct vsr_options options = mem_options((uint64_t)i + 1, &membership);
        options.durability = durability;
        if (small_cache) {
            options.limits.log_cache_entries = 4;
            options.limits.batch_entries = 2;
            options.limits.client_cache_entries = 1;
        }
        nodes[i] = mem_cluster_add(cluster, &options);
        if (small_cache) {
            mem_node_output_capacity(nodes[i], 1);
        }
        CHECK(mem_node_time(nodes[i], 0).consumed == 1);
    }
    run(cluster);
    for (uint32_t i = 0; i < count; ++i) {
        struct vsr_status status;
        vsr_get_status(mem_node_core(nodes[i]), &status);
        CHECK(status.state == VSR_STATE_NORMAL);
        CHECK(status.primary == 1 && status.committed == 0);
        CHECK(status.role == members[i].role);
        CHECK(status.failure.code == VSR_FAILURE_NONE);
    }

    /* Different span boundaries do not turn a retry into a new operation. */
    submit(nodes[0], client, 1, 1, &first);
    run(cluster);
    CHECK(reply_for(nodes[0], 1)->status == VSR_REPLY_OK);
    CHECK(reply_for(nodes[0], 1)->flags == VSR_REPLY_EXECUTED);
    CHECK(reply_for(nodes[0], 1)->op == 1);
    submit(nodes[0], client, 1, 2, &retry);
    run(cluster);
    CHECK(reply_for(nodes[0], 2)->status == VSR_REPLY_OK);
    CHECK(reply_for(nodes[0], 2)->op == 1);
    CHECK(mem_blob_equal(&reply_for(nodes[0], 1)->result.data,
                         &reply_for(nodes[0], 2)->result.data));

    /* History and client population both exceed the configured small caches. */
    for (uint64_t i = 0; i < 20; ++i) {
        submit(nodes[0], (struct vsr_id){300, i + 1}, 1, 10 + i, &first);
        run(cluster);
        CHECK(reply_for(nodes[0], 10 + i)->status == VSR_REPLY_OK);
        CHECK(reply_for(nodes[0], 10 + i)->op == i + 2);
    }
    submit(nodes[0], client, 1, 50, &retry);
    run(cluster);
    CHECK(reply_for(nodes[0], 50)->status == VSR_REPLY_OK);
    CHECK(reply_for(nodes[0], 50)->op == 1);
    submit(nodes[0], client, 2, 51, &first);
    run(cluster);
    CHECK(reply_for(nodes[0], 51)->status == VSR_REPLY_OK);
    last_op = reply_for(nodes[0], 51)->op;
    CHECK(last_op == 22);
    submit(nodes[0], client, 1, 52, &retry);
    run(cluster);
    CHECK(reply_for(nodes[0], 52)->status == VSR_REPLY_STALE_REQUEST);
    CHECK(reply_for(nodes[0], 52)->flags == 0);

    if (count > 1) {
        submit(nodes[1], (struct vsr_id){500, 600}, 1, 60, &first);
        run(cluster);
        CHECK(reply_for(nodes[1], 60)->status == VSR_REPLY_NOT_PRIMARY);
        CHECK(reply_for(nodes[1], 60)->primary == 1);
    }
    /* A primary may piggyback commits until its idle heartbeat is due. */
    for (uint32_t i = 0; i < count; ++i) {
        struct vsr_options timing = mem_options((uint64_t)i + 1, &membership);
        CHECK(mem_node_time(nodes[i], timing.heartbeat_ns).consumed == 1);
    }
    run(cluster);
    for (uint32_t i = 0; i < count; ++i) {
        struct vsr_status status;
        vsr_get_status(mem_node_core(nodes[i]), &status);
        CHECK(status.committed == last_op);
        CHECK(status.applied ==
              (members[i].role == VSR_MEMBER_FULL ? last_op : 0));
        CHECK(status.failure.code == VSR_FAILURE_NONE);
        if (members[i].role == VSR_MEMBER_FULL) {
            for (uint64_t op = 1; op <= last_op; ++op) {
                const struct vsr_entry *entry = mem_node_history(nodes[i], op);
                CHECK(entry != NULL && entry->op == op);
                CHECK(entry->type == VSR_REQUEST_COMMAND);
                CHECK(mem_blob_equal(entry->body, &first));
            }
        }
    }
    mem_cluster_destroy(cluster);
}

int main(void)
{
    normal_group(1, 0, false, VSR_DURABLE, false);
    normal_group(1, 0, false, VSR_REPLICATED, true);
    normal_group(3, 1, false, VSR_DURABLE, true);
    normal_group(3, 1, true, VSR_REPLICATED, true);
    normal_group(4, 1, false, VSR_DURABLE, false);
    normal_group(5, 2, true, VSR_REPLICATED, false);
    return 0;
}
