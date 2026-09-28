#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <stdint.h>

struct group {
    struct mem_cluster *cluster;
    struct mem_node *nodes[5];
    uint32_t count;
    uint32_t fetches; /* GET_LOG and range GET_STATE messages delivered. */
};

enum schedule { FAIR, NO_ACKS, GAP, ONE_COPY, HOLD_LOG, LAG };

static struct vsr_status status(struct mem_node *node)
{
    struct vsr_status result;
    vsr_get_status(mem_node_core(node), &result);
    CHECK(result.failure.code == VSR_FAILURE_NONE);
    return result;
}

static void drive(struct group *group, enum schedule schedule)
{
    for (uint32_t turn = 0; turn < 100000; turn++) {
        bool progress = false;
        for (uint32_t i = 0; i < group->count; i++) {
            struct mem_node *node = group->nodes[i];
            if (!mem_node_alive(node))
                continue;
            struct mem_step step = mem_node_event(node, NULL);
            CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
            progress |=
                step.emitted != 0 || (step.flags & VSR_UPDATE_MORE) != 0;
            for (size_t j = 0; j < mem_node_effects(node); j++) {
                const struct vsr_op *operation = mem_node_effect(node, j);
                if (schedule == HOLD_LOG && operation->type == VSR_OP_LOAD &&
                    ((const struct vsr_store_read *)operation->data)->type ==
                        VSR_LOAD_LOG)
                    continue;
                if (mem_node_complete(node, j, VSR_IO_OK)) {
                    progress = true;
                    break;
                }
            }
        }
        if (mem_cluster_messages(group->cluster) != 0) {
            uint64_t to;
            const struct vsr_message *message =
                mem_cluster_message(group->cluster, 0, &to);
            bool drop = !mem_node_alive(mem_cluster_find(group->cluster, to));
            drop |= (schedule == NO_ACKS || schedule == ONE_COPY) &&
                    message->type == VSR_MSG_PREPARE_OK;
            drop |= schedule == ONE_COPY && to == 3 &&
                    message->type == VSR_MSG_PREPARE;
            drop |= schedule == GAP &&
                    ((to >= 4 && message->type == VSR_MSG_PREPARE) ||
                     message->type == VSR_MSG_GET_STATE ||
                     message->type == VSR_MSG_NEW_STATE);
            drop |= schedule == LAG && to == 3 &&
                    (message->type == VSR_MSG_PREPARE ||
                     message->type == VSR_MSG_COMMIT);
            bool fetch =
                message->type == VSR_MSG_GET_LOG ||
                (message->type == VSR_MSG_GET_STATE &&
                 ((const struct vsr_fetch *)message->body)->first != 0);
            if (drop) {
                mem_cluster_drop(group->cluster, 0);
                progress = true;
            } else if (mem_cluster_deliver(group->cluster, 0)) {
                group->fetches += fetch ? 1 : 0;
                progress = true;
            }
        }
        mem_cluster_check(group->cluster);
        if (!progress)
            return;
    }
    CHECK(false);
}

static struct group create(uint32_t count, uint32_t durability,
                           uint32_t operations, bool minimum, uint32_t batch)
{
    struct group result = {.cluster = mem_cluster_create(), .count = count};
    struct vsr_member members[5];
    struct vsr_membership membership = {0, members, count, (count - 1) / 2};
    for (uint32_t i = 0; i < count; i++)
        members[i] = (struct vsr_member){i + 1, VSR_MEMBER_FULL, 0};
    for (uint32_t i = 0; i < count; i++) {
        struct vsr_options options = mem_options(i + 1, &membership);
        options.durability = durability;
        options.limits.operations = operations;
        if (minimum) {
            options.limits.transfers = 1;
            options.limits.input_leases = 9;
            options.limits.command_bytes = 32;
            options.limits.result_bytes = 18;
            options.limits.manifest_bytes = 8;
            options.limits.message_bytes = 40;
            options.limits.pinned_payload_bytes = 154;
        }
        options.limits.log_cache_entries = batch;
        options.limits.batch_entries = batch;
        result.nodes[i] = mem_cluster_add(result.cluster, &options);
        mem_node_output_capacity(result.nodes[i], 1);
        CHECK(mem_node_time(result.nodes[i], 0).consumed == 1);
    }
    drive(&result, FAIR);
    for (uint32_t i = 0; i < count; i++)
        CHECK(status(result.nodes[i]).state == VSR_STATE_NORMAL);
    return result;
}

static void command(struct mem_node *node, uint64_t number)
{
    const struct vsr_span span = {"transition-history", 18};
    const struct vsr_blob body = {&span, 18, 1, 0};
    const struct vsr_request request = {
        {{100, number}, 1}, 0, VSR_REQUEST_COMMAND, 0, &body};
    const struct vsr_event event = {VSR_EVENT_REQUEST, VSR_IO_OK, number,
                                    &request, 1};
    CHECK(mem_node_event(node, &event).consumed == 1);
}

static void time_all(struct group *group, uint64_t now)
{
    for (uint32_t i = 0; i < group->count; i++)
        if (mem_node_alive(group->nodes[i]))
            CHECK(mem_node_time(group->nodes[i], now).consumed == 1);
}

static void converge(struct group *group, uint64_t first, uint64_t committed)
{
    for (uint64_t round = 0; round < 200; round++) {
        time_all(group, first + round * 5);
        drive(group, FAIR);
        bool ready = true;
        for (uint32_t i = 0; i < group->count; i++) {
            if (!mem_node_alive(group->nodes[i]))
                continue;
            struct vsr_status current = status(group->nodes[i]);
            ready &= current.state == VSR_STATE_NORMAL &&
                     current.committed >= committed &&
                     current.applied >= committed;
        }
        if (ready)
            return;
    }
    CHECK(false);
}

static void inherited_suffix(uint32_t durability, uint32_t operations,
                             bool minimum)
{
    struct group group = create(3, durability, operations, minimum, 1);
    command(group.nodes[0], 1);
    drive(&group, NO_ACKS);
    CHECK(status(group.nodes[0]).committed == 0);
    CHECK(mem_node_replies(group.nodes[0]) == 0);
    mem_node_crash(group.nodes[0]);
    converge(&group, 55, 1);
    CHECK(status(group.nodes[1]).view == 1);
    CHECK(status(group.nodes[1]).primary == 2);
    CHECK(mem_node_history(group.nodes[1], 1)->request.client.lo == 1);
    CHECK(mem_node_restart(group.nodes[0], (struct vsr_id){99, 1}) == VSR_OK);
    converge(&group, 100, 1);
    /* A pre-crash request retry uses the installed indexed client result. */
    command(group.nodes[1], 1);
    drive(&group, FAIR);
    CHECK(mem_node_reply(group.nodes[1], 0, NULL)->status == VSR_REPLY_OK);
    CHECK(mem_node_reply(group.nodes[1], 0, NULL)->op == 1);
    mem_cluster_destroy(group.cluster);
}

static void intact_gap_keeps_vote(void)
{
    struct group group = create(5, VSR_REPLICATED, 2, false, 1);
    command(group.nodes[0], 1);
    drive(&group, GAP);
    time_all(&group, 10);
    drive(&group, GAP);
    CHECK(status(group.nodes[0]).committed == 1);
    CHECK(status(group.nodes[3]).state == VSR_STATE_VIEW_CHANGE);
    CHECK(status(group.nodes[4]).state == VSR_STATE_VIEW_CHANGE);
    /* Only this replica lost volatile replicated storage. The two intact
     * lagging replicas must still help the remaining quorum elect a primary. */
    mem_node_crash(group.nodes[2]);
    CHECK(mem_node_restart(group.nodes[2], (struct vsr_id){99, 3}) == VSR_OK);
    drive(&group, GAP);
    CHECK(status(group.nodes[2]).state == VSR_STATE_RECOVERING);
    converge(&group, 65, 1);
    CHECK(status(group.nodes[0]).view > 0);
    mem_cluster_destroy(group.cluster);
}

static void interrupted_transfer_preserves_suffix(void)
{
    struct group group = create(3, VSR_DURABLE, 4, false, 1);
    command(group.nodes[0], 1);
    drive(&group, ONE_COPY);
    mem_node_crash(group.nodes[0]);
    time_all(&group, 55);
    drive(&group, HOLD_LOG);
    struct mem_store *store = mem_node_store(group.nodes[1]);
    CHECK(mem_store_recovered(store, mem_store_readable(store))->log_end == 2);
    /* Abandon a selected transfer before its indexed LOAD completes. The old
     * endorsed entry must survive both this interruption and another view. */
    time_all(&group, 110);
    drive(&group, HOLD_LOG);
    time_all(&group, 165);
    drive(&group, HOLD_LOG);
    CHECK(mem_store_recovered(store, mem_store_readable(store))->log_end == 2);
    converge(&group, 170, 1);
    mem_cluster_destroy(group.cluster);
}

static void recovered_reconfiguration_fence(void)
{
    struct group group = create(3, VSR_DURABLE, 4, false, 1);
    const struct vsr_member members[] = {{1, VSR_MEMBER_FULL, 0},
                                         {2, VSR_MEMBER_FULL, 0},
                                         {3, VSR_MEMBER_FULL, 0}};
    const struct vsr_membership next = {1, members, 3, 1};
    const struct vsr_request request = {
        {{101, 1}, 1}, 0, VSR_REQUEST_RECONFIGURE, 0, &next};
    const struct vsr_event input = {VSR_EVENT_REQUEST, VSR_IO_OK, 1, &request,
                                    1};
    CHECK(mem_node_event(group.nodes[0], &input).consumed == 1);
    drive(&group, NO_ACKS);
    CHECK(status(group.nodes[0]).committed == 0);
    mem_node_crash(group.nodes[0]);
    CHECK(mem_node_restart(group.nodes[0], (struct vsr_id){99, 1}) == VSR_OK);
    /* Messages racing LOAD_RECOVERY cannot cause a STORE against unknown
     * identity/frontiers or advance a view before durable recovery finishes. */
    const struct vsr_message early = {
        {1, 1}, 0, 9, 2, VSR_MSG_START_VIEW_CHANGE, 0, 0, NULL};
    const struct vsr_event message = {VSR_EVENT_MESSAGE, 0, 0, &early, 1};
    CHECK(mem_node_event(group.nodes[0], &message).consumed == 1);
    drive(&group, NO_ACKS);
    CHECK(status(group.nodes[0]).state == VSR_STATE_NORMAL);
    CHECK(status(group.nodes[0]).view == 0);
    command(group.nodes[0], 2);
    drive(&group, NO_ACKS);
    CHECK(mem_node_replies(group.nodes[0]) == 1);
    CHECK(mem_node_reply(group.nodes[0], 0, NULL)->status == VSR_REPLY_BUSY);
    struct mem_store *store = mem_node_store(group.nodes[0]);
    CHECK(mem_store_recovered(store, mem_store_readable(store))->log_end == 2);
    mem_cluster_destroy(group.cluster);
}

/* "A range fetch asks for as many entries as its byte budget and
 * batch_entries allow." A backup that missed fifty committed entries
 * installs the new primary's log through GET_LOG in batches, not one entry
 * per round trip. */
static void batched_log_transfer(void)
{
    struct group group = create(3, VSR_DURABLE, 8, false, 8);
    for (uint64_t number = 1; number <= 50; number++) {
        command(group.nodes[0], number);
        drive(&group, LAG);
    }
    CHECK(status(group.nodes[0]).committed == 50);
    CHECK(status(group.nodes[2]).committed == 0);
    mem_node_crash(group.nodes[0]);
    group.fetches = 0;
    converge(&group, 55, 50);
    CHECK(status(group.nodes[2]).primary == 2);
    CHECK(mem_node_history(group.nodes[2], 50)->request.client.lo == 50);
    CHECK(group.fetches >= 1 && group.fetches <= 8);
    mem_cluster_destroy(group.cluster);
}

int main(void)
{
    inherited_suffix(VSR_DURABLE, 1, false);
    inherited_suffix(VSR_DURABLE, 8, false);
    inherited_suffix(VSR_REPLICATED, 1, false);
    inherited_suffix(VSR_REPLICATED, 8, false);
    inherited_suffix(VSR_DURABLE, 1, true);
    inherited_suffix(VSR_REPLICATED, 1, true);
    intact_gap_keeps_vote();
    interrupted_transfer_preserves_suffix();
    recovered_reconfiguration_fence();
    batched_log_transfer();
    return 0;
}
