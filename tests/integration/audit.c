#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

/* Keep an indexed request lookup alive across view selection. Its immutable
 * revision remains readable, even after the selected history discards its tail. */
struct fixture {
    struct mem_cluster *cluster;
    struct mem_node *nodes[3];
    uint64_t held;
    bool hold_lookup;
    bool drop_old_primary;
};

static struct vsr_status status(struct mem_node *node)
{
    struct vsr_status result;
    vsr_get_status(mem_node_core(node), &result);
    CHECK(result.failure.code == VSR_FAILURE_NONE);
    return result;
}

static void drive(struct fixture *fixture)
{
    for (uint32_t turn = 0; turn < 100000; turn++) {
        bool progress = false;
        for (uint32_t n = 0; n < 3; n++) {
            struct mem_node *node = fixture->nodes[n];
            struct mem_step step = mem_node_event(node, NULL);
            CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
            progress |=
                step.emitted != 0 || (step.flags & VSR_UPDATE_MORE) != 0;
            for (size_t i = 0; i < mem_node_effects(node); i++) {
                const struct vsr_op *operation = mem_node_effect(node, i);
                if (n == 0 && fixture->hold_lookup &&
                    operation->type == VSR_OP_LOAD &&
                    (fixture->held == 0 || operation->id == fixture->held) &&
                    ((const struct vsr_store_read *)operation->data)->type ==
                        VSR_LOAD_REQUEST) {
                    fixture->held = operation->id;
                    continue;
                }
                if (mem_node_complete(node, i, VSR_IO_OK)) {
                    progress = true;
                    break;
                }
            }
            (void)status(node);
        }
        for (size_t i = 0; i < mem_cluster_messages(fixture->cluster); i++) {
            const struct vsr_message *message =
                mem_cluster_message(fixture->cluster, i, NULL);
            if (fixture->drop_old_primary && message->from == 1) {
                mem_cluster_drop(fixture->cluster, i);
                progress = true;
                break;
            }
            if (mem_cluster_deliver(fixture->cluster, i)) {
                progress = true;
                break;
            }
        }
        mem_cluster_check(fixture->cluster);
        if (!progress)
            return;
    }
    CHECK(false);
}

static void command(struct mem_node *node, uint64_t client, uint64_t route)
{
    const struct vsr_blob body = {0};
    const struct vsr_request request = {
        {{12, client}, 1}, 0, VSR_REQUEST_COMMAND, 0, &body};
    const struct vsr_event event = {VSR_EVENT_REQUEST, VSR_IO_OK, route,
                                    &request, 1};
    CHECK(mem_node_event(node, &event).consumed == 1);
}

static const struct vsr_reply *reply(struct mem_node *node, uint64_t route)
{
    for (size_t i = 0; i < mem_node_replies(node); i++) {
        uint64_t found;
        const struct vsr_reply *result = mem_node_reply(node, i, &found);
        if (found == route)
            return result;
    }
    CHECK(false);
    return NULL;
}

static void stale_request_lookup(uint32_t durability, bool replace)
{
    struct vsr_member members[] = {{1, VSR_MEMBER_FULL, 0},
                                   {2, VSR_MEMBER_FULL, 0},
                                   {3, VSR_MEMBER_FULL, 0}};
    struct vsr_membership group = {0, members, 3, 1};
    struct fixture fixture = {.cluster = mem_cluster_create()};
    for (uint32_t n = 0; n < 3; n++) {
        struct vsr_options options = mem_options(n + 1, &group);
        options.durability = durability;
        fixture.nodes[n] = mem_cluster_add(fixture.cluster, &options);
        CHECK(mem_node_time(fixture.nodes[n], 0).consumed == 1);
    }
    drive(&fixture);
    fixture.drop_old_primary = true;
    command(fixture.nodes[0], 34, 1);
    drive(&fixture);
    CHECK(status(fixture.nodes[0]).committed == 0);

    fixture.hold_lookup = true;
    const struct vsr_id client = {12, 34};
    const struct vsr_event query = {VSR_EVENT_CLIENT_QUERY, VSR_IO_OK, 2,
                                    &client, 1};
    CHECK(mem_node_event(fixture.nodes[0], &query).consumed == 1);
    drive(&fixture);
    CHECK(fixture.held != 0);

    /* The other two replicas select their empty history without the isolated
     * primary's offer. START_VIEW makes that primary discard its local tail. */
    CHECK(mem_node_time(fixture.nodes[1], 55).consumed == 1);
    CHECK(mem_node_time(fixture.nodes[2], 55).consumed == 1);
    drive(&fixture);
    fixture.drop_old_primary = false;
    drive(&fixture);
    for (uint32_t view = 2; view <= 3; view++) {
        for (uint32_t n = 0; n < 3; n++)
            CHECK(
                mem_node_time(fixture.nodes[n], 55 * (uint64_t)view).consumed ==
                1);
        drive(&fixture);
    }
    struct vsr_status current = status(fixture.nodes[0]);
    CHECK(current.view == 3 && current.state == VSR_STATE_NORMAL);
    CHECK(current.primary == 1 && current.committed == 0);

    /* The short-log case used to report STORAGE failure on the valid old
     * result. Once op 1 was reused, it instead cached a phantom pending request
     * and prevented this client's retry from ever being proposed. */
    if (replace) {
        command(fixture.nodes[0], 35, 4);
        drive(&fixture);
        CHECK(status(fixture.nodes[0]).committed == 1);
    }
    fixture.hold_lookup = false;
    drive(&fixture);
    CHECK(reply(fixture.nodes[0], 2)->status == VSR_REPLY_CLIENT_UNKNOWN);
    command(fixture.nodes[0], 34, 3);
    drive(&fixture);
    const struct vsr_reply *result = reply(fixture.nodes[0], 3);
    CHECK(result->status == VSR_REPLY_OK);
    CHECK(result->op == (replace ? 2u : 1u));
    mem_cluster_check(fixture.cluster);
    mem_cluster_destroy(fixture.cluster);
}

static void exact_apply_reservation(void)
{
    struct vsr_member member = {1, VSR_MEMBER_FULL, 0};
    struct vsr_membership group = {0, &member, 1, 0};
    struct mem_cluster *cluster = mem_cluster_create();
    struct vsr_options options = mem_options(1, &group);
    options.limits.batch_entries = 32;
    options.limits.pinned_payload_bytes = 11968;
    struct mem_node *node = mem_cluster_add(cluster, &options);
    CHECK(mem_node_time(node, 0).consumed == 1);
    CHECK(mem_cluster_run(cluster, 100000) < 100000);
    command(node, 34, 1);
    uint64_t apply_id = 0;
    for (uint32_t turn = 0; turn < 10000; turn++) {
        bool progress = false;
        (void)mem_node_event(node, NULL);
        for (size_t i = 0; i < mem_node_effects(node); i++) {
            const struct vsr_op *operation = mem_node_effect(node, i);
            if (operation->type == VSR_OP_APPLY) {
                CHECK(
                    ((const struct vsr_apply *)operation->data)->batch.count ==
                    1);
                apply_id = operation->id;
                continue;
            }
            if (mem_node_complete(node, i, VSR_IO_OK)) {
                progress = true;
                break;
            }
        }
        if (!progress)
            break;
    }
    CHECK(apply_id != 0);
    CHECK(status(node).committed == 1 && status(node).applied == 0);
    /* Only one result can arrive for this APPLY. Reserving the configured
     * 32-entry batch unnecessarily consumes the input progress reserve and
     * blocks even one legal maximum-size command from an independent client. */
    unsigned char bytes[1024] = {0};
    const struct vsr_span span = {bytes, sizeof(bytes)};
    const struct vsr_blob body = {&span, sizeof(bytes), 1, 0};
    const struct vsr_request request = {
        {{12, 35}, 1}, 0, VSR_REQUEST_COMMAND, 0, &body};
    const struct vsr_event event = {VSR_EVENT_REQUEST, VSR_IO_OK, 2, &request,
                                    1};
    CHECK(mem_node_event(node, &event).consumed == 1);
    CHECK(mem_cluster_run(cluster, 100000) < 100000);
    CHECK(reply(node, 1)->status == VSR_REPLY_OK);
    CHECK(reply(node, 2)->status == VSR_REPLY_OK);
    mem_cluster_check(cluster);
    mem_cluster_destroy(cluster);
}

int main(void)
{
    stale_request_lookup(VSR_DURABLE, false);
    stale_request_lookup(VSR_DURABLE, true);
    stale_request_lookup(VSR_REPLICATED, false);
    stale_request_lookup(VSR_REPLICATED, true);
    exact_apply_reservation();
    return 0;
}
