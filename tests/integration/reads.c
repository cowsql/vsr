#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <stddef.h>

struct fixture {
    struct mem_cluster *cluster;
    struct mem_node *nodes[4];
    uint32_t count;
};

static struct fixture create(uint32_t count, bool witness)
{
    struct fixture f = {.cluster = mem_cluster_create(), .count = count};
    struct vsr_member members[4];
    struct vsr_membership group = {0, members, count, count == 1 ? 0 : 1};
    for (uint32_t i = 0; i < count; ++i) {
        members[i] = (struct vsr_member){
            i + 1,
            witness && i + 1 == count ? VSR_MEMBER_WITNESS : VSR_MEMBER_FULL,
            0};
    }
    for (uint32_t i = 0; i < count; ++i) {
        struct vsr_options options = mem_options(i + 1, &group);
        f.nodes[i] = mem_cluster_add(f.cluster, &options);
        mem_node_output_capacity(f.nodes[i], 1);
        CHECK(mem_node_time(f.nodes[i], 0).consumed == 1);
    }
    CHECK(mem_cluster_run(f.cluster, 100000) < 100000);
    return f;
}

static void read_event(struct mem_node *node, uint64_t cookie,
                       uint32_t consistency, uint64_t minimum,
                       uint64_t deadline)
{
    const struct vsr_read_barrier barrier = {minimum, deadline, consistency, 0};
    const struct vsr_event event = {VSR_EVENT_READ, 0, cookie, &barrier, 1};
    CHECK(mem_node_event(node, &event).consumed == 1);
}

static void command(struct mem_node *node, uint64_t number)
{
    const struct vsr_blob body = {0};
    const struct vsr_request request = {
        {{10, 20}, number}, 0, VSR_REQUEST_COMMAND, 0, &body};
    const struct vsr_event event = {VSR_EVENT_REQUEST, 0, number, &request, 1};
    CHECK(mem_node_event(node, &event).consumed == 1);
}

static struct vsr_status status(struct mem_node *node)
{
    struct vsr_status result;
    vsr_get_status(mem_node_core(node), &result);
    CHECK(result.failure.code == VSR_FAILURE_NONE);
    return result;
}

/* Run all dependencies while deliberately withholding read acknowledgments
 * and/or application fence completion. This is a deterministic adversarial
 * schedule; unrelated traffic and persistence continue to make progress. */
static void pump(struct fixture *f, bool hold_acks, bool hold_fences)
{
    for (size_t action = 0; action < 100000; ++action) {
        bool progress = false;
        for (uint32_t i = 0; i < f->count; ++i) {
            struct mem_node *node = f->nodes[i];
            struct mem_step step = mem_node_event(node, NULL);
            CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
            progress |=
                step.emitted != 0 || (step.flags & VSR_UPDATE_MORE) != 0;
            for (size_t j = 0; j < mem_node_effects(node); ++j) {
                if (hold_fences &&
                    mem_node_effect(node, j)->type == VSR_OP_READ_READY)
                    continue;
                if (mem_node_complete(node, j, VSR_IO_OK)) {
                    progress = true;
                    break;
                }
            }
        }
        for (size_t i = 0; i < mem_cluster_messages(f->cluster); ++i) {
            if (hold_acks && mem_cluster_message(f->cluster, i, NULL)->type ==
                                 VSR_MSG_READ_ACK)
                continue;
            if (mem_cluster_deliver(f->cluster, i)) {
                progress = true;
                break;
            }
        }
        mem_cluster_check(f->cluster);
        if (!progress)
            return;
    }
    CHECK(false);
}

static size_t find_ack(struct fixture *f, uint64_t sender)
{
    for (size_t i = 0; i < mem_cluster_messages(f->cluster); ++i) {
        const struct vsr_message *message =
            mem_cluster_message(f->cluster, i, NULL);
        if (message->type == VSR_MSG_READ_ACK && message->from == sender)
            return i;
    }
    CHECK(false);
    return 0;
}

static size_t fence_index(struct mem_node *node)
{
    for (size_t i = 0; i < mem_node_effects(node); ++i)
        if (mem_node_effect(node, i)->type == VSR_OP_READ_READY)
            return i;
    CHECK(false);
    return 0;
}

static void empty_log_establishment(void)
{
    for (uint32_t count = 1; count <= 3; count += 2) {
        struct fixture f = create(count, count == 3);
        read_event(f.nodes[0], 101, VSR_READ_LINEARIZABLE, 0, VSR_NO_DEADLINE);
        CHECK(mem_cluster_run(f.cluster, 100000) < 100000);
        CHECK(mem_node_reads(f.nodes[0]) == 1);
        CHECK(mem_node_read(f.nodes[0], 0)->cookie == 101);
        CHECK(mem_node_read(f.nodes[0], 0)->applied == 1);
        CHECK(mem_node_history(f.nodes[0], 1)->type == VSR_REQUEST_NOOP);
        CHECK(status(f.nodes[0]).committed == 1);
        /* Once established, repeated read barriers do not append NOOPs. */
        for (uint64_t cookie = 102; cookie < 110; ++cookie) {
            read_event(f.nodes[0], cookie, VSR_READ_LINEARIZABLE, 0,
                       VSR_NO_DEADLINE);
            CHECK(mem_cluster_run(f.cluster, 100000) < 100000);
            CHECK(mem_node_read(f.nodes[0], (size_t)(cookie - 101))->applied ==
                  1);
        }
        CHECK(status(f.nodes[0]).committed == 1);
        mem_cluster_destroy(f.cluster);
    }
}

static void quorum_and_application_fence(void)
{
    struct fixture f = create(4, false);
    command(f.nodes[0], 1);
    CHECK(mem_cluster_run(f.cluster, 100000) < 100000);
    read_event(f.nodes[0], 100, VSR_READ_LINEARIZABLE, 1, VSR_NO_DEADLINE);
    pump(&f, true, true);
    /* n=4, f=1 means three votes, not a majority formula tied to 2f+1. */
    for (unsigned i = 0; i < 4; ++i) {
        size_t index = find_ack(&f, 2);
        mem_cluster_duplicate(f.cluster, index);
        CHECK(mem_cluster_deliver(f.cluster, index));
        pump(&f, true, true);
        for (size_t j = 0; j < mem_node_effects(f.nodes[0]); ++j)
            CHECK(mem_node_effect(f.nodes[0], j)->type != VSR_OP_READ_READY);
    }
    CHECK(mem_cluster_deliver(f.cluster, find_ack(&f, 3)));
    pump(&f, true, true);
    const struct vsr_read_fence *fence =
        mem_node_effect(f.nodes[0], fence_index(f.nodes[0]))->data;
    CHECK(fence->cookie == 100 && fence->applied == 1);
    command(f.nodes[0], 2);
    pump(&f, true, true);
    CHECK(status(f.nodes[0]).committed == 2);
    CHECK(status(f.nodes[0]).applied == 1);
    for (size_t j = 0; j < mem_node_effects(f.nodes[0]); ++j) {
        uint32_t type = mem_node_effect(f.nodes[0], j)->type;
        CHECK(type != VSR_OP_APPLY && type != VSR_OP_SNAPSHOT_INSTALL &&
              type != VSR_OP_SNAPSHOT_CAPTURE);
    }
    CHECK(mem_node_complete(f.nodes[0], fence_index(f.nodes[0]), VSR_IO_OK));
    pump(&f, true, false);
    CHECK(status(f.nodes[0]).applied == 2);
    CHECK(mem_node_reads(f.nodes[0]) == 1);
    /* Deliver old-round duplicates before any new-round ACK can exist. */
    read_event(f.nodes[0], 101, VSR_READ_LINEARIZABLE, 2, VSR_NO_DEADLINE);
    while (mem_cluster_messages(f.cluster) != 0)
        CHECK(mem_cluster_deliver(f.cluster, 0));
    for (size_t j = 0; j < mem_node_effects(f.nodes[0]); ++j)
        CHECK(mem_node_effect(f.nodes[0], j)->type != VSR_OP_READ_READY);
    CHECK(mem_cluster_run(f.cluster, 100000) < 100000);
    CHECK(mem_node_reads(f.nodes[0]) == 2);
    CHECK(mem_node_read(f.nodes[0], 1)->applied == 2);
    mem_cluster_destroy(f.cluster);
}

static void backup_causal_and_deadline(void)
{
    struct fixture f = create(3, true);
    command(f.nodes[0], 1);
    CHECK(mem_cluster_run(f.cluster, 100000) < 100000);
    for (uint32_t i = 0; i < f.count; ++i)
        CHECK(mem_node_time(f.nodes[i], 10).consumed == 1);
    CHECK(mem_cluster_run(f.cluster, 100000) < 100000);
    read_event(f.nodes[1], 100, VSR_READ_CAUSAL, 1, VSR_NO_DEADLINE);
    read_event(f.nodes[1], 101, VSR_READ_LINEARIZABLE, 0, VSR_NO_DEADLINE);
    read_event(f.nodes[2], 102, VSR_READ_CAUSAL, 0, VSR_NO_DEADLINE);
    CHECK(mem_cluster_run(f.cluster, 100000) < 100000);
    CHECK(mem_node_reads(f.nodes[1]) == 1);
    CHECK(mem_node_read(f.nodes[1], 0)->cookie == 100);
    CHECK(mem_node_reads(f.nodes[2]) == 0);
    CHECK(mem_node_reply(f.nodes[1], 0, NULL)->status == VSR_REPLY_NOT_PRIMARY);
    CHECK(mem_node_reply(f.nodes[2], 0, NULL)->status == VSR_REPLY_NOT_PRIMARY);
    read_event(f.nodes[1], 103, VSR_READ_CAUSAL, 2, 20);
    CHECK(mem_cluster_run(f.cluster, 100000) < 100000);
    CHECK(mem_node_reads(f.nodes[1]) == 1);
    CHECK(mem_node_time(f.nodes[1], 20).consumed == 1);
    CHECK(mem_cluster_run(f.cluster, 100000) < 100000);
    uint64_t route = 0;
    const struct vsr_reply *reply = mem_node_reply(f.nodes[1], 1, &route);
    CHECK(route == 103 && reply->status == VSR_REPLY_TIMEOUT);
    mem_cluster_destroy(f.cluster);
}

int main(void)
{
    empty_log_establishment();
    quorum_and_application_fence();
    backup_causal_and_deadline();
    return 0;
}
