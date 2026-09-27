#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <stdint.h>
#include <string.h>

struct trace {
    size_t captures;
    size_t fetches;
    size_t syncs;
    size_t installs;
    size_t drops;
};

struct schedule {
    uint64_t blocked_types;
    bool block_publication;
    bool block_advertisements;
    bool duplicate_transfer;
};

static struct vsr_status status(struct mem_node *node)
{
    struct vsr_status result;
    vsr_get_status(mem_node_core(node), &result);
    CHECK(result.failure.code == VSR_FAILURE_NONE);
    return result;
}

static bool publication(const struct vsr_op *operation)
{
    if (operation->type == VSR_OP_STORE) {
        const struct vsr_store *store = operation->data;
        for (uint32_t i = 0; i < store->count; i++) {
            if (store->changes[i].type == VSR_STORE_PUBLISH_CHECKPOINT ||
                store->changes[i].type == VSR_STORE_RESTORE_CHECKPOINT)
                return true;
        }
    }
    return false;
}

static void record(struct trace *trace, uint32_t type)
{
    switch (type) {
    case VSR_OP_SNAPSHOT_CAPTURE:
        trace->captures++;
        break;
    case VSR_OP_SNAPSHOT_FETCH:
        trace->fetches++;
        break;
    case VSR_OP_SNAPSHOT_SYNC:
        trace->syncs++;
        break;
    case VSR_OP_SNAPSHOT_INSTALL:
        trace->installs++;
        break;
    case VSR_OP_SNAPSHOT_DROP:
        trace->drops++;
        break;
    default:
        break;
    }
}

static void drive(struct mem_cluster *cluster, struct mem_node **nodes,
                  size_t count, const struct schedule *schedule,
                  struct trace *traces)
{
    for (size_t turn = 0; turn < 100000; turn++) {
        bool progress = false;
        for (size_t i = 0; i < count; i++) {
            if (!mem_node_alive(nodes[i]))
                continue;
            const struct mem_step step = mem_node_event(nodes[i], NULL);
            CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
            if (step.emitted != 0 || (step.flags & VSR_UPDATE_MORE) != 0)
                progress = true;
            for (size_t j = 0; j < mem_node_effects(nodes[i]); j++) {
                const struct vsr_op *operation = mem_node_effect(nodes[i], j);
                const uint32_t type = operation->type;
                if ((schedule->blocked_types & (UINT64_C(1) << type)) != 0 ||
                    (schedule->block_publication && publication(operation)))
                    continue;
                if (mem_node_complete(nodes[i], j, VSR_IO_OK)) {
                    record(&traces[i], type);
                    progress = true;
                    break;
                }
            }
        }
        for (size_t i = 0; i < mem_cluster_messages(cluster); i++) {
            const struct vsr_message *message =
                mem_cluster_message(cluster, i, NULL);
            if (schedule->block_advertisements &&
                message->type == VSR_MSG_CHECKPOINT)
                continue;
            if (schedule->duplicate_transfer &&
                (message->type == VSR_MSG_RECOVERY_RESPONSE ||
                 message->type == VSR_MSG_NEW_STATE ||
                 message->type == VSR_MSG_LOG)) {
                /* Deliver the copy immediately, so it cannot be recursively
                 * duplicated by a later scheduling iteration. */
                mem_cluster_duplicate(cluster, i);
                const size_t duplicate = mem_cluster_messages(cluster) - 1;
                if (!mem_cluster_deliver(cluster, duplicate))
                    mem_cluster_drop(cluster, duplicate);
            }
            if (mem_cluster_deliver(cluster, i)) {
                progress = true;
                break;
            }
        }
        mem_cluster_check(cluster);
        if (!progress)
            return;
    }
    CHECK(false);
}

static void run(struct mem_cluster *cluster, struct mem_node **nodes,
                size_t count, struct trace *traces)
{
    const struct schedule schedule = {0};
    drive(cluster, nodes, count, &schedule, traces);
}

static void hint(struct mem_node *node)
{
    const struct vsr_event event = {VSR_EVENT_CHECKPOINT, 0, 0, NULL, 0};
    CHECK(mem_node_event(node, &event).consumed == 1);
}

static void submit(struct mem_node *node, uint64_t client, uint64_t route)
{
    const struct vsr_span spans[] = {{"immutable", 9}, {" snapshot", 9}};
    const struct vsr_blob command = {spans, 18, 2, 0};
    const struct vsr_request request = {
        {{100, client}, 1}, 0, VSR_REQUEST_COMMAND, 0, &command};
    const struct vsr_event event = {VSR_EVENT_REQUEST, 0, route, &request, 1};
    const struct mem_step step = mem_node_event(node, &event);
    CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
    CHECK(step.consumed == 1);
}

static const struct vsr_reply *reply(struct mem_node *node, uint64_t route)
{
    for (size_t i = mem_node_replies(node); i > 0; i--) {
        uint64_t found;
        const struct vsr_reply *answer = mem_node_reply(node, i - 1, &found);
        if (found == route)
            return answer;
    }
    CHECK(false);
    return NULL;
}

static size_t effect(struct mem_node *node, uint32_t type)
{
    for (size_t i = 0; i < mem_node_effects(node); i++) {
        if (mem_node_effect(node, i)->type == type)
            return i;
    }
    CHECK(false);
    return 0;
}

static void assert_checkpoint(struct mem_node *node, uint64_t op)
{
    const struct vsr_status current = status(node);
    const struct vsr_recovered *stored = mem_store_recovered(
        mem_node_store(node), mem_store_readable(mem_node_store(node)));
    CHECK(current.checkpoint_op == op);
    CHECK(stored != NULL && stored->checkpoint != NULL);
    CHECK(stored->checkpoint->op == op);
    CHECK(stored->checkpoint->view == mem_node_history(node, op)->view);
    CHECK(stored->log_begin == op + 1);
}

static struct mem_node *single(struct mem_cluster *cluster, uint32_t durability,
                               bool minimum)
{
    const struct vsr_member member = {1, VSR_MEMBER_FULL, 0};
    const struct vsr_membership membership = {0, &member, 1, 0};
    struct vsr_options options = mem_options(1, &membership);
    options.durability = durability;
    options.limits.log_cache_entries = 4;
    options.limits.batch_entries = 2;
    options.limits.client_cache_entries = 1;
    if (minimum) {
        options.limits.transfers = 1;
        options.limits.input_leases = options.limits.transfers + 8u;
        options.limits.command_bytes = 18;
        options.limits.result_bytes = 18;
        options.limits.manifest_bytes = 8;
        options.limits.message_bytes = 26;
        options.limits.pinned_payload_bytes = 112;
        {
            struct vsr_options insufficient = options;
            struct vsr_layout layout;
            insufficient.limits.input_leases--;
            CHECK(vsr_layout(&insufficient, &layout) == VSR_ELIMIT);
            insufficient = options;
            insufficient.limits.pinned_payload_bytes--;
            CHECK(vsr_layout(&insufficient, &layout) == VSR_ELIMIT);
        }
    }
    struct mem_node *node = mem_cluster_add(cluster, &options);
    mem_node_output_capacity(node, 1);
    CHECK(mem_node_time(node, 0).consumed == 1);
    return node;
}

static void capture_trim_and_restart(uint32_t durability, bool minimum)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[] = {single(cluster, durability, minimum)};
    struct trace traces[] = {{0}};
    run(cluster, nodes, 1, traces);
    CHECK(status(nodes[0]).state == VSR_STATE_NORMAL);
    for (uint64_t i = 1; i <= 4; i++) {
        submit(nodes[0], i, i);
        run(cluster, nodes, 1, traces);
        CHECK(reply(nodes[0], i)->status == VSR_REPLY_OK);
    }
    hint(nodes[0]);
    run(cluster, nodes, 1, traces);
    assert_checkpoint(nodes[0], 4);
    CHECK(traces[0].captures == 1);
    CHECK(traces[0].syncs == (durability == VSR_DURABLE ? 1u : 0u));
    CHECK(mem_store_entry(mem_node_store(nodes[0]),
                          mem_store_readable(mem_node_store(nodes[0])),
                          1) == NULL);
    submit(nodes[0], 1, 100);
    run(cluster, nodes, 1, traces);
    CHECK(reply(nodes[0], 100)->status == VSR_REPLY_OK);
    CHECK(reply(nodes[0], 100)->op == 1);
    CHECK(status(nodes[0]).applied == 4);
    /* The retained suffix exceeds both cache and storage transaction size. */
    for (uint64_t i = 5; i <= 9; i++) {
        submit(nodes[0], i, i);
        run(cluster, nodes, 1, traces);
        CHECK(reply(nodes[0], i)->status == VSR_REPLY_OK);
    }
    if (durability == VSR_DURABLE) {
        const uint64_t checksum = mem_node_checksum(nodes[0]);
        mem_node_crash(nodes[0]);
        CHECK(mem_node_restart(nodes[0], (struct vsr_id){1, 2}) == VSR_OK);
        CHECK(mem_node_time(nodes[0], 0).consumed == 1);
        run(cluster, nodes, 1, traces);
        CHECK(status(nodes[0]).state == VSR_STATE_NORMAL);
        CHECK(status(nodes[0]).applied == 9);
        CHECK(mem_node_checksum(nodes[0]) == checksum);
        CHECK(traces[0].installs == 2);
        submit(nodes[0], 1, 101);
        run(cluster, nodes, 1, traces);
        CHECK(reply(nodes[0], 101)->op == 1);
        CHECK(status(nodes[0]).applied == 9);
    }
    hint(nodes[0]);
    run(cluster, nodes, 1, traces);
    assert_checkpoint(nodes[0], 9);
    CHECK(traces[0].drops == 1);
    submit(nodes[0], 10, 10);
    run(cluster, nodes, 1, traces);
    CHECK(reply(nodes[0], 10)->status == VSR_REPLY_OK);
    CHECK(status(nodes[0]).applied == 10);
    mem_cluster_destroy(cluster);
}

static void delayed_snapshot_does_not_rewind(void)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[] = {single(cluster, VSR_DURABLE, false)};
    struct trace traces[] = {{0}};
    struct schedule schedule = {UINT64_C(1) << VSR_OP_SNAPSHOT_CAPTURE, false,
                                false, false};
    run(cluster, nodes, 1, traces);
    submit(nodes[0], 1, 1);
    run(cluster, nodes, 1, traces);
    hint(nodes[0]);
    drive(cluster, nodes, 1, &schedule, traces);
    {
        const size_t capture = effect(nodes[0], VSR_OP_SNAPSHOT_CAPTURE);
        const struct vsr_snapshot_task *task =
            mem_node_effect(nodes[0], capture)->data;
        CHECK(task->op == 1 && task->checkpoint->view == 0);
        CHECK(mem_node_execute(nodes[0], capture, VSR_IO_OK));
    }
    submit(nodes[0], 2, 2);
    drive(cluster, nodes, 1, &schedule, traces);
    CHECK(status(nodes[0]).applied == 1);
    CHECK(status(nodes[0]).checkpoint_op == 0);
    CHECK(mem_node_complete(nodes[0], effect(nodes[0], VSR_OP_SNAPSHOT_CAPTURE),
                            VSR_IO_OK));
    traces[0].captures++;
    schedule.blocked_types = UINT64_C(1) << VSR_OP_SNAPSHOT_SYNC;
    drive(cluster, nodes, 1, &schedule, traces);
    CHECK(status(nodes[0]).applied == 2);
    submit(nodes[0], 3, 3);
    drive(cluster, nodes, 1, &schedule, traces);
    CHECK(status(nodes[0]).applied == 3);
    CHECK(status(nodes[0]).checkpoint_op == 0);
    CHECK(mem_node_complete(nodes[0], effect(nodes[0], VSR_OP_SNAPSHOT_SYNC),
                            VSR_IO_OK));
    traces[0].syncs++;
    schedule.blocked_types = 0;
    schedule.block_publication = true;
    drive(cluster, nodes, 1, &schedule, traces);
    CHECK(status(nodes[0]).checkpoint_op == 0);
    CHECK(status(nodes[0]).applied == 3);
    run(cluster, nodes, 1, traces);
    assert_checkpoint(nodes[0], 1);
    CHECK(status(nodes[0]).applied == 3);
    CHECK(reply(nodes[0], 2)->op == 2 && reply(nodes[0], 3)->op == 3);
    mem_cluster_destroy(cluster);
}

static void recovered_anchor_at_log_tip(void)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[] = {single(cluster, VSR_DURABLE, false)};
    struct trace traces[] = {{0}};
    run(cluster, nodes, 1, traces);
    submit(nodes[0], 1, 1);
    run(cluster, nodes, 1, traces);
    hint(nodes[0]);
    run(cluster, nodes, 1, traces);
    assert_checkpoint(nodes[0], 1);
    mem_node_crash(nodes[0]);
    CHECK(mem_node_restart(nodes[0], (struct vsr_id){1, 2}) == VSR_OK);
    CHECK(mem_node_time(nodes[0], 0).consumed == 1);
    run(cluster, nodes, 1, traces);
    CHECK(status(nodes[0]).state == VSR_STATE_NORMAL);
    CHECK(status(nodes[0]).applied == 1);
    CHECK(traces[0].installs == 2);
    submit(nodes[0], 2, 2);
    run(cluster, nodes, 1, traces);
    CHECK(reply(nodes[0], 2)->status == VSR_REPLY_OK);
    CHECK(status(nodes[0]).applied == 2);
    mem_cluster_destroy(cluster);
}

static void distinct_full_coverage_for_witness(void)
{
    const struct vsr_member members[] = {{1, VSR_MEMBER_FULL, 0},
                                         {2, VSR_MEMBER_FULL, 0},
                                         {3, VSR_MEMBER_WITNESS, 0}};
    const struct vsr_membership membership = {0, members, 3, 1};
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[3];
    struct trace traces[3] = {{0}};
    struct schedule schedule = {0, false, true, false};
    for (uint32_t i = 0; i < 3; i++) {
        const struct vsr_options options =
            mem_options((uint64_t)i + 1, &membership);
        nodes[i] = mem_cluster_add(cluster, &options);
        CHECK(mem_node_time(nodes[i], 0).consumed == 1);
    }
    run(cluster, nodes, 3, traces);
    for (uint64_t i = 1; i <= 6; i++) {
        submit(nodes[0], i, i);
        run(cluster, nodes, 3, traces);
    }
    for (uint32_t i = 0; i < 3; i++)
        CHECK(mem_node_time(nodes[i], 10).consumed == 1);
    run(cluster, nodes, 3, traces);
    CHECK(status(nodes[2]).committed == 6);
    hint(nodes[0]);
    drive(cluster, nodes, 3, &schedule, traces);
    {
        bool duplicated = false;
        for (size_t i = 0; i < mem_cluster_messages(cluster); i++) {
            uint64_t to;
            const struct vsr_message *message =
                mem_cluster_message(cluster, i, &to);
            if (message->type == VSR_MSG_CHECKPOINT && to == 3) {
                mem_cluster_duplicate(cluster, i);
                mem_cluster_duplicate(cluster, i);
                duplicated = true;
                break;
            }
        }
        CHECK(duplicated);
    }
    run(cluster, nodes, 3, traces);
    CHECK(status(nodes[2]).checkpoint_op == 0);
    CHECK(mem_store_recovered(mem_node_store(nodes[2]),
                              mem_store_readable(mem_node_store(nodes[2])))
              ->log_begin == 1);
    hint(nodes[1]);
    run(cluster, nodes, 3, traces);
    CHECK(status(nodes[2]).checkpoint_op == 6);
    CHECK(mem_store_recovered(mem_node_store(nodes[2]),
                              mem_store_readable(mem_node_store(nodes[2])))
              ->log_begin == 7);
    CHECK(traces[2].captures == 0 && traces[2].fetches == 0 &&
          traces[2].syncs == 0);
    CHECK(traces[2].installs == 0 && traces[2].drops == 0);
    mem_cluster_destroy(cluster);
}

static void capture_retry_and_stop(void)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[] = {single(cluster, VSR_DURABLE, false)};
    struct trace traces[] = {{0}};
    const struct schedule held = {UINT64_C(1) << VSR_OP_SNAPSHOT_CAPTURE, false,
                                  false, false};
    run(cluster, nodes, 1, traces);
    submit(nodes[0], 1, 1);
    run(cluster, nodes, 1, traces);
    hint(nodes[0]);
    drive(cluster, nodes, 1, &held, traces);
    CHECK(mem_node_complete(nodes[0], effect(nodes[0], VSR_OP_SNAPSHOT_CAPTURE),
                            VSR_IO_RETRY));
    run(cluster, nodes, 1, traces);
    CHECK(status(nodes[0]).checkpoint_op == 0);
    CHECK(traces[0].captures == 0);
    CHECK(mem_node_time(nodes[0], 4).consumed == 1);
    run(cluster, nodes, 1, traces);
    CHECK(traces[0].captures == 0);
    CHECK(mem_node_time(nodes[0], 5).consumed == 1);
    run(cluster, nodes, 1, traces);
    assert_checkpoint(nodes[0], 1);
    submit(nodes[0], 2, 2);
    run(cluster, nodes, 1, traces);
    hint(nodes[0]);
    drive(cluster, nodes, 1, &held, traces);
    CHECK(mem_node_execute(nodes[0], effect(nodes[0], VSR_OP_SNAPSHOT_CAPTURE),
                           VSR_IO_OK));
    {
        const struct vsr_event stop = {VSR_EVENT_STOP, 0, 0, NULL, 0};
        CHECK(mem_node_event(nodes[0], &stop).consumed == 1);
    }
    CHECK(status(nodes[0]).state == VSR_STATE_STOPPING);
    run(cluster, nodes, 1, traces);
    CHECK(status(nodes[0]).state == VSR_STATE_STOPPED);
    CHECK(mem_node_leases(nodes[0]) == 0 && mem_node_effects(nodes[0]) == 0);
    CHECK(vsr_deinit(mem_node_core(nodes[0])) == VSR_OK);
    mem_cluster_destroy(cluster);
}

static void recovery_fetch_with_duplicates(void)
{
    const struct vsr_member members[] = {{1, VSR_MEMBER_FULL, 0},
                                         {2, VSR_MEMBER_FULL, 0},
                                         {3, VSR_MEMBER_FULL, 0}};
    const struct vsr_membership membership = {0, members, 3, 1};
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[3];
    struct trace traces[3] = {{0}};
    const struct schedule duplicates = {0, false, false, true};
    for (uint32_t i = 0; i < 2; i++) {
        struct vsr_options options = mem_options((uint64_t)i + 1, &membership);
        options.limits.log_cache_entries = 4;
        options.limits.batch_entries = 2;
        nodes[i] = mem_cluster_add(cluster, &options);
        CHECK(mem_node_time(nodes[i], 0).consumed == 1);
    }
    run(cluster, nodes, 2, traces);
    for (uint64_t i = 1; i <= 4; i++) {
        submit(nodes[0], i, i);
        run(cluster, nodes, 2, traces);
    }
    for (uint32_t i = 0; i < 2; i++)
        CHECK(mem_node_time(nodes[i], 10).consumed == 1);
    run(cluster, nodes, 2, traces);
    hint(nodes[0]);
    hint(nodes[1]);
    run(cluster, nodes, 2, traces);
    assert_checkpoint(nodes[0], 4);
    assert_checkpoint(nodes[1], 4);
    for (uint64_t i = 5; i <= 9; i++) {
        submit(nodes[0], i, i);
        run(cluster, nodes, 2, traces);
    }
    {
        struct vsr_options options = mem_options(3, &membership);
        options.start_mode = VSR_START_RECOVER;
        options.limits.log_cache_entries = 4;
        options.limits.batch_entries = 2;
        nodes[2] = mem_cluster_add(cluster, &options);
        CHECK(mem_node_time(nodes[2], 0).consumed == 1);
    }
    drive(cluster, nodes, 3, &duplicates, traces);
    CHECK(status(nodes[2]).state == VSR_STATE_NORMAL);
    CHECK(status(nodes[2]).applied == 9);
    CHECK(mem_node_checksum(nodes[2]) == mem_node_checksum(nodes[0]));
    CHECK(traces[2].fetches >= 1 && traces[2].installs >= 1);
    CHECK(status(nodes[2]).checkpoint_op == 4);
    CHECK(traces[2].drops == 0);
    /* Duplicate messages/fetches share the one local snapshot hold. Replacing
     * that image must issue exactly one DROP for it. */
    hint(nodes[2]);
    run(cluster, nodes, 3, traces);
    assert_checkpoint(nodes[2], 9);
    CHECK(traces[2].drops == 1);
    mem_cluster_destroy(cluster);
}

static bool selected(int argc, char **argv, const char *name)
{
    return argc == 1 || strcmp(argv[1], name) == 0;
}

int main(int argc, char **argv)
{
    unsigned int count = 0;
    CHECK(argc <= 2);
    if (selected(argc, argv, "durable")) {
        capture_trim_and_restart(VSR_DURABLE, false);
        count++;
    }
    if (selected(argc, argv, "replicated")) {
        capture_trim_and_restart(VSR_REPLICATED, false);
        count++;
    }
    if (selected(argc, argv, "minimum")) {
        capture_trim_and_restart(VSR_DURABLE, true);
        count++;
    }
    if (selected(argc, argv, "delayed")) {
        delayed_snapshot_does_not_rewind();
        count++;
    }
    if (selected(argc, argv, "tip-restart")) {
        recovered_anchor_at_log_tip();
        count++;
    }
    if (selected(argc, argv, "witness")) {
        distinct_full_coverage_for_witness();
        count++;
    }
    if (selected(argc, argv, "retry-stop")) {
        capture_retry_and_stop();
        count++;
    }
    if (selected(argc, argv, "fetch")) {
        recovery_fetch_with_duplicates();
        count++;
    }
    CHECK(count != 0);
    return 0;
}
