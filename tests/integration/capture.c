#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <stdint.h>
#include <string.h>

/* Contract tests for checkpoint capture scheduling and CAPTURE completion
 * consistency (docs/vsr-api.md "Log and checkpoint transfer" and "Application
 * operations and failures"). Every scenario cites the sentence it derives
 * from; none restates the implementation. */

struct trace {
    size_t captures;
    size_t syncs;
    size_t installs;
    size_t drops;
};

struct schedule {
    uint64_t blocked_types;
};

/* Every CAPTURE template must name an op at or beyond its epoch boundary. */
static bool check_boundary;

static struct vsr_status status(struct mem_node *node)
{
    struct vsr_status result;
    vsr_get_status(mem_node_core(node), &result);
    return result;
}

static struct vsr_status healthy(struct mem_node *node)
{
    const struct vsr_status result = status(node);
    CHECK(result.failure.code == VSR_FAILURE_NONE);
    return result;
}

static void record(struct trace *trace, uint32_t type)
{
    switch (type) {
    case VSR_OP_SNAPSHOT_CAPTURE:
        trace->captures++;
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

static size_t find(struct mem_node *node, uint32_t type)
{
    for (size_t i = 0; i < mem_node_effects(node); i++) {
        if (mem_node_effect(node, i)->type == type)
            return i;
    }
    return SIZE_MAX;
}

static void audit_captures(struct mem_node *node)
{
    if (!check_boundary)
        return;
    for (size_t i = 0; i < mem_node_effects(node); i++) {
        const struct vsr_op *operation = mem_node_effect(node, i);
        if (operation->type != VSR_OP_SNAPSHOT_CAPTURE)
            continue;
        const struct vsr_snapshot_task *task = operation->data;
        CHECK(task->checkpoint != NULL && task->checkpoint->op == task->op);
        CHECK(task->checkpoint->epoch != NULL);
        CHECK(task->checkpoint->epoch->boundary <= task->op);
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
            audit_captures(nodes[i]);
            for (size_t j = 0; j < mem_node_effects(nodes[i]); j++) {
                const uint32_t type = mem_node_effect(nodes[i], j)->type;
                if ((schedule->blocked_types & (UINT64_C(1) << type)) != 0)
                    continue;
                if (mem_node_complete(nodes[i], j, VSR_IO_OK)) {
                    record(&traces[i], type);
                    progress = true;
                    break;
                }
            }
        }
        for (size_t i = 0; i < mem_cluster_messages(cluster); i++) {
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

static struct mem_step request(struct mem_node *node, uint64_t number,
                               uint64_t route, uint64_t epoch)
{
    const struct vsr_span spans[] = {{"capture", 7}, {" contract", 9}};
    const struct vsr_blob command = {spans, 16, 2, 0};
    const struct vsr_request request = {
        {{300, number}, 1}, epoch, VSR_REQUEST_COMMAND, 0, &command};
    const struct vsr_event event = {VSR_EVENT_REQUEST, 0, route, &request, 1};
    const struct mem_step step = mem_node_event(node, &event);
    CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
    return step;
}

static void submit(struct mem_node *node, uint64_t number)
{
    CHECK(request(node, number, number, 0).consumed == 1);
}

static void time_all(struct mem_node **nodes, size_t count, uint64_t now)
{
    for (size_t i = 0; i < count; i++)
        if (mem_node_alive(nodes[i]))
            CHECK(mem_node_time(nodes[i], now).consumed == 1);
}

static void assert_checkpoint(struct mem_node *node, uint64_t op)
{
    const struct vsr_status current = healthy(node);
    const struct vsr_recovered *stored = mem_store_recovered(
        mem_node_store(node), mem_store_readable(mem_node_store(node)));
    CHECK(current.checkpoint_op == op);
    CHECK(stored != NULL && stored->checkpoint != NULL);
    CHECK(stored->checkpoint->op == op);
}

static struct mem_node *single(struct mem_cluster *cluster,
                               uint64_t checkpoint_interval, bool minimum)
{
    const struct vsr_member member = {1, VSR_MEMBER_FULL, 0};
    const struct vsr_membership membership = {0, &member, 1, 0};
    struct vsr_options options = mem_options(1, &membership);
    options.durability = VSR_DURABLE;
    options.checkpoint_interval = checkpoint_interval;
    options.limits.log_cache_entries = 4;
    options.limits.batch_entries = 2;
    options.limits.client_cache_entries = 1;
    if (minimum) {
        options.limits.transfers = 1;
        options.limits.input_leases = options.limits.transfers + 8u;
        options.limits.command_bytes = 16;
        options.limits.result_bytes = 16;
        options.limits.manifest_bytes = 8;
        options.limits.message_bytes = 24;
        options.limits.pinned_payload_bytes = 104;
    }
    struct mem_node *node = mem_cluster_add(cluster, &options);
    mem_node_output_capacity(node, 1);
    CHECK(mem_node_time(node, 0).consumed == 1);
    return node;
}

static void triple(struct mem_cluster *cluster, struct mem_node **nodes,
                   bool witness)
{
    const struct vsr_member members[] = {
        {1, VSR_MEMBER_FULL, 0},
        {2, VSR_MEMBER_FULL, 0},
        {3, witness ? VSR_MEMBER_WITNESS : VSR_MEMBER_FULL, 0}};
    const struct vsr_membership membership = {0, members, 3, 1};
    for (uint32_t i = 0; i < 3; i++) {
        const struct vsr_options options =
            mem_options((uint64_t)i + 1, &membership);
        nodes[i] = mem_cluster_add(cluster, &options);
        CHECK(mem_node_time(nodes[i], 0).consumed == 1);
    }
}

/* "CHECKPOINT events and checkpoint_interval are coalescible scheduling
 * hints ... and may be deferred while replay, transfer, or another
 * application fence is active"; "No APPLY, INSTALL, or CAPTURE overlaps the
 * fence." A hint issued while an APPLY, a read fence, or replay is
 * outstanding produces its single CAPTURE only once that fence ends. */
enum fence { FENCE_APPLY, FENCE_READ, FENCE_REPLAY };

static void hint_deferred_by_fence(enum fence fence)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[] = {single(cluster, 0, false)};
    struct trace traces[] = {{0}};
    struct schedule held = {0};
    uint32_t type = VSR_OP_APPLY;
    run(cluster, nodes, 1, traces);
    CHECK(healthy(nodes[0]).state == VSR_STATE_NORMAL);
    if (fence == FENCE_APPLY) {
        submit(nodes[0], 1);
    } else if (fence == FENCE_READ) {
        submit(nodes[0], 1);
        run(cluster, nodes, 1, traces);
        CHECK(healthy(nodes[0]).applied == 1);
        const struct vsr_read_barrier read = {1, VSR_NO_DEADLINE,
                                              VSR_READ_CAUSAL, 0};
        const struct vsr_event event = {VSR_EVENT_READ, 0, 77, &read, 1};
        CHECK(mem_node_event(nodes[0], &event).consumed == 1);
        type = VSR_OP_READ_READY;
    } else {
        for (uint64_t i = 1; i <= 3; i++) {
            submit(nodes[0], i);
            run(cluster, nodes, 1, traces);
        }
        CHECK(healthy(nodes[0]).applied == 3);
        mem_node_crash(nodes[0]);
        CHECK(mem_node_restart(nodes[0], (struct vsr_id){1, 2}) == VSR_OK);
        CHECK(mem_node_time(nodes[0], 0).consumed == 1);
    }
    held.blocked_types = UINT64_C(1) << type;
    drive(cluster, nodes, 1, &held, traces);
    const size_t index = find(nodes[0], type);
    CHECK(index != SIZE_MAX);
    if (fence == FENCE_REPLAY) {
        const struct vsr_apply *apply = mem_node_effect(nodes[0], index)->data;
        CHECK(apply->replay == 1);
        CHECK(traces[0].installs == 2);
    }
    hint(nodes[0]);
    drive(cluster, nodes, 1, &held, traces);
    CHECK(find(nodes[0], VSR_OP_SNAPSHOT_CAPTURE) == SIZE_MAX);
    CHECK(traces[0].captures == 0);
    CHECK(healthy(nodes[0]).checkpoint_op == 0);
    CHECK(mem_node_complete(nodes[0], find(nodes[0], type), VSR_IO_OK));
    run(cluster, nodes, 1, traces);
    CHECK(traces[0].captures == 1);
    assert_checkpoint(nodes[0], fence == FENCE_REPLAY ? 3u : 1u);
    CHECK(healthy(nodes[0]).applied == (fence == FENCE_REPLAY ? 3u : 1u));
    mem_cluster_destroy(cluster);
}

/* "CHECKPOINT events and checkpoint_interval are coalescible scheduling
 * hints, not completion promises." Repeated hints, including one while the
 * capture is outstanding, produce one capture; a hint at the published
 * position produces none. */
static void hints_coalesce(void)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[] = {single(cluster, 0, false)};
    struct trace traces[] = {{0}};
    const struct schedule held = {UINT64_C(1) << VSR_OP_SNAPSHOT_CAPTURE};
    run(cluster, nodes, 1, traces);
    submit(nodes[0], 1);
    run(cluster, nodes, 1, traces);
    hint(nodes[0]);
    hint(nodes[0]);
    hint(nodes[0]);
    drive(cluster, nodes, 1, &held, traces);
    CHECK(find(nodes[0], VSR_OP_SNAPSHOT_CAPTURE) != SIZE_MAX);
    hint(nodes[0]);
    drive(cluster, nodes, 1, &held, traces);
    CHECK(mem_node_effects(nodes[0]) == 1);
    run(cluster, nodes, 1, traces);
    CHECK(traces[0].captures == 1 && traces[0].syncs == 1);
    assert_checkpoint(nodes[0], 1);
    hint(nodes[0]);
    hint(nodes[0]);
    run(cluster, nodes, 1, traces);
    CHECK(traces[0].captures == 1 && traces[0].syncs == 1);
    assert_checkpoint(nodes[0], 1);
    mem_cluster_destroy(cluster);
}

/* "checkpoint_interval ... In operations; 0: explicit requests only."
 * With an interval of two, capture follows every second applied operation
 * without any explicit hint. */
static void interval_capture(void)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[] = {single(cluster, 2, false)};
    struct trace traces[] = {{0}};
    run(cluster, nodes, 1, traces);
    CHECK(traces[0].captures == 0);
    submit(nodes[0], 1);
    run(cluster, nodes, 1, traces);
    CHECK(traces[0].captures == 0 && healthy(nodes[0]).checkpoint_op == 0);
    submit(nodes[0], 2);
    run(cluster, nodes, 1, traces);
    CHECK(traces[0].captures == 1);
    assert_checkpoint(nodes[0], 2);
    submit(nodes[0], 3);
    run(cluster, nodes, 1, traces);
    CHECK(traces[0].captures == 1);
    assert_checkpoint(nodes[0], 2);
    submit(nodes[0], 4);
    run(cluster, nodes, 1, traces);
    CHECK(traces[0].captures == 2);
    assert_checkpoint(nodes[0], 4);
    mem_cluster_destroy(cluster);
}

/* "Witnesses ignore local capture hints." A witness never issues CAPTURE,
 * SNAPSHOT_SYNC, INSTALL, or DROP; its checkpoint_op advances only through
 * advertisements from full members. */
static void witness_ignores_hints(void)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[3];
    struct trace traces[3] = {{0}};
    triple(cluster, nodes, true);
    run(cluster, nodes, 3, traces);
    for (uint64_t i = 1; i <= 4; i++) {
        submit(nodes[0], i);
        run(cluster, nodes, 3, traces);
    }
    time_all(nodes, 3, 10);
    run(cluster, nodes, 3, traces);
    CHECK(healthy(nodes[2]).committed == 4);
    CHECK(healthy(nodes[2]).role == VSR_MEMBER_WITNESS);
    hint(nodes[2]);
    hint(nodes[2]);
    run(cluster, nodes, 3, traces);
    CHECK(traces[2].captures == 0 && traces[2].syncs == 0);
    CHECK(traces[2].installs == 0 && traces[2].drops == 0);
    CHECK(healthy(nodes[2]).checkpoint_op == 0);
    hint(nodes[0]);
    hint(nodes[1]);
    run(cluster, nodes, 3, traces);
    CHECK(traces[0].captures == 1 && traces[1].captures == 1);
    CHECK(healthy(nodes[2]).checkpoint_op == 4);
    CHECK(traces[2].captures == 0 && traces[2].syncs == 0);
    CHECK(traces[2].installs == 0 && traces[2].drops == 0);
    mem_cluster_destroy(cluster);
}

/* "At genesis the operation and view are zero." A hint before any command
 * captures the genesis boundary with a template naming op 0, view 0, and the
 * epoch-0 descriptor, and publishes checkpoint_op 0. */
static void genesis_capture(void)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[] = {single(cluster, 0, false)};
    struct trace traces[] = {{0}};
    const struct schedule held = {UINT64_C(1) << VSR_OP_SNAPSHOT_CAPTURE};
    run(cluster, nodes, 1, traces);
    CHECK(healthy(nodes[0]).state == VSR_STATE_NORMAL);
    hint(nodes[0]);
    drive(cluster, nodes, 1, &held, traces);
    const size_t index = find(nodes[0], VSR_OP_SNAPSHOT_CAPTURE);
    CHECK(index != SIZE_MAX);
    const struct vsr_snapshot_task *task =
        mem_node_effect(nodes[0], index)->data;
    CHECK(task->op == 0 && task->peer == 0 && task->basis == NULL);
    CHECK(task->checkpoint != NULL);
    CHECK(task->checkpoint->op == 0 && task->checkpoint->view == 0);
    CHECK(task->checkpoint->id.hi == 0 && task->checkpoint->id.lo == 0);
    CHECK(task->checkpoint->manifest.count == 0);
    CHECK(task->checkpoint->epoch->current->epoch == 0);
    CHECK(task->checkpoint->epoch->previous == NULL);
    CHECK(task->checkpoint->epoch->boundary == 0);
    run(cluster, nodes, 1, traces);
    CHECK(traces[0].captures == 1 && traces[0].syncs == 1);
    assert_checkpoint(nodes[0], 0);
    submit(nodes[0], 1);
    run(cluster, nodes, 1, traces);
    CHECK(healthy(nodes[0]).applied == 1);
    hint(nodes[0]);
    run(cluster, nodes, 1, traces);
    assert_checkpoint(nodes[0], 1);
    CHECK(traces[0].drops == 1);
    mem_cluster_destroy(cluster);
}

/* "CAPTURE | Retry ordinary failures; corruption fences snapshot state; no
 * partial object is adopted." RETRY and FAILED are retried after retry_ns;
 * CORRUPT latches VSR_FAILURE_SNAPSHOT with the operation and status. */
static void capture_retry(int code)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[] = {single(cluster, 0, false)};
    struct trace traces[] = {{0}};
    const struct schedule held = {UINT64_C(1) << VSR_OP_SNAPSHOT_CAPTURE};
    run(cluster, nodes, 1, traces);
    submit(nodes[0], 1);
    run(cluster, nodes, 1, traces);
    hint(nodes[0]);
    drive(cluster, nodes, 1, &held, traces);
    const size_t index = find(nodes[0], VSR_OP_SNAPSHOT_CAPTURE);
    CHECK(index != SIZE_MAX);
    const uint64_t id = mem_node_effect(nodes[0], index)->id;
    CHECK(mem_node_complete(nodes[0], index, code));
    if (code == VSR_IO_CORRUPT) {
        const struct vsr_status current = status(nodes[0]);
        CHECK(current.state == VSR_STATE_FAILED);
        CHECK(current.failure.code == VSR_FAILURE_SNAPSHOT);
        CHECK(current.failure.operation == id);
        CHECK(current.failure.operation_type == VSR_OP_SNAPSHOT_CAPTURE);
        CHECK(current.failure.status == VSR_IO_CORRUPT);
        mem_cluster_destroy(cluster);
        return;
    }
    run(cluster, nodes, 1, traces);
    CHECK(traces[0].captures == 0);
    CHECK(healthy(nodes[0]).checkpoint_op == 0);
    CHECK(find(nodes[0], VSR_OP_SNAPSHOT_CAPTURE) == SIZE_MAX);
    CHECK(mem_node_time(nodes[0], 4).consumed == 1);
    run(cluster, nodes, 1, traces);
    CHECK(traces[0].captures == 0);
    CHECK(mem_node_time(nodes[0], 5).consumed == 1);
    run(cluster, nodes, 1, traces);
    CHECK(traces[0].captures == 1);
    assert_checkpoint(nodes[0], 1);
    mem_cluster_destroy(cluster);
}

/* "Return the same logical boundary with a fresh snapshot ID"; "structurally
 * valid but inconsistent storage/application results fence the instance";
 * "Snapshot IDs are cluster-unique and never reused for different contents."
 * A CAPTURE result whose epoch, op, or view differ from the template, or
 * whose ID names an already published snapshot, fences with
 * VSR_FAILURE_SNAPSHOT and latches the operation. */
enum variant {
    VARIANT_COUNT,
    VARIANT_ID,
    VARIANT_EPOCH,
    VARIANT_OP,
    VARIANT_VIEW,
    VARIANT_FAULTS,
    VARIANT_ROLE,
    VARIANT_DUPLICATE
};

static void inconsistent_capture(enum variant variant)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[3];
    struct trace traces[3] = {{0}};
    const struct schedule held = {UINT64_C(1) << VSR_OP_SNAPSHOT_CAPTURE};
    const bool cluster3 = variant == VARIANT_FAULTS || variant == VARIANT_ROLE;
    size_t count = 1;
    uint64_t expected = 1;
    if (cluster3) {
        triple(cluster, nodes, false);
        count = 3;
    } else {
        nodes[0] = single(cluster, 0, false);
    }
    run(cluster, nodes, count, traces);
    submit(nodes[0], 1);
    run(cluster, nodes, count, traces);
    if (variant == VARIANT_DUPLICATE) {
        hint(nodes[0]);
        run(cluster, nodes, count, traces);
        assert_checkpoint(nodes[0], 1);
        submit(nodes[0], 2);
        run(cluster, nodes, count, traces);
        expected = 2;
    }
    hint(nodes[0]);
    drive(cluster, nodes, count, &held, traces);
    const size_t index = find(nodes[0], VSR_OP_SNAPSHOT_CAPTURE);
    CHECK(index != SIZE_MAX);
    const struct vsr_op *operation = mem_node_effect(nodes[0], index);
    const struct vsr_snapshot_task *task = operation->data;
    const uint64_t id = operation->id;
    CHECK(task->op == expected);
    /* The host registers the real image under {replica, capture number}. */
    CHECK(mem_node_execute(nodes[0], index, VSR_IO_OK));
    struct vsr_checkpoint crafted = *task->checkpoint;
    struct vsr_epoch epoch = *task->checkpoint->epoch;
    struct vsr_membership current = *epoch.current;
    struct vsr_membership previous = *epoch.current;
    struct vsr_member members[3];
    memcpy(members, current.members, current.count * sizeof(*members));
    current.members = members;
    epoch.current = &current;
    crafted.epoch = &epoch;
    crafted.id = (struct vsr_id){1, 1};
    switch (variant) {
    case VARIANT_COUNT:
        members[1] = (struct vsr_member){2, VSR_MEMBER_FULL, 0};
        current.count = 2;
        break;
    case VARIANT_ID:
        members[0].id = 2;
        break;
    case VARIANT_EPOCH:
        current.epoch = 1;
        epoch.previous = &previous;
        epoch.boundary = 1;
        break;
    case VARIANT_OP:
        crafted.op = expected + 1;
        break;
    case VARIANT_VIEW:
        crafted.view = 5;
        break;
    case VARIANT_FAULTS:
        current.faults = 0;
        break;
    case VARIANT_ROLE:
        members[2].role = VSR_MEMBER_WITNESS;
        break;
    case VARIANT_DUPLICATE:
        break;
    }
    const struct mem_step step =
        mem_node_notify(nodes[0], index, VSR_IO_OK, &crafted);
    CHECK(step.result >= VSR_OK && step.consumed == 1);
    const struct vsr_status current_status = status(nodes[0]);
    CHECK(current_status.state == VSR_STATE_FAILED);
    CHECK(current_status.failure.code == VSR_FAILURE_SNAPSHOT);
    CHECK(current_status.failure.operation == id);
    CHECK(current_status.failure.operation_type == VSR_OP_SNAPSHOT_CAPTURE);
    CHECK(current_status.failure.status ==
          (variant == VARIANT_DUPLICATE ? VSR_IO_CORRUPT : VSR_IO_OK));
    CHECK(current_status.checkpoint_op == expected - 1);
    mem_cluster_destroy(cluster);
}

static bool same_bytes(const struct vsr_blob *blob, const unsigned char *bytes,
                       size_t size)
{
    const struct vsr_span span = {bytes, size};
    const struct vsr_blob expected = {&span, size, 1, 0};
    return mem_blob_equal(blob, &expected);
}

/* "Return the same logical boundary with a fresh snapshot ID and local
 * manifest"; "manifest_bytes bound individual objects." A manifest of exactly
 * manifest_bytes across spans_per_blob spans is published, offered, and
 * recovered unchanged; one byte more is an API error that leaves the
 * operation active. */
static void manifest_at_limits(void)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[] = {single(cluster, 0, false)};
    struct trace traces[] = {{0}};
    const struct schedule held = {UINT64_C(1) << VSR_OP_SNAPSHOT_CAPTURE};
    unsigned char bytes[65];
    struct vsr_span spans[8];
    for (size_t i = 0; i < sizeof(bytes); i++)
        bytes[i] = (unsigned char)(i * 7 + 1);
    for (size_t i = 0; i < 8; i++)
        spans[i] = (struct vsr_span){bytes + i * 8, 8};
    run(cluster, nodes, 1, traces);
    submit(nodes[0], 1);
    run(cluster, nodes, 1, traces);
    hint(nodes[0]);
    drive(cluster, nodes, 1, &held, traces);
    const size_t index = find(nodes[0], VSR_OP_SNAPSHOT_CAPTURE);
    CHECK(index != SIZE_MAX);
    const uint64_t id = mem_node_effect(nodes[0], index)->id;
    const struct vsr_snapshot_task *task =
        mem_node_effect(nodes[0], index)->data;
    CHECK(mem_node_execute(nodes[0], index, VSR_IO_OK));
    struct vsr_checkpoint crafted = *task->checkpoint;
    crafted.id = (struct vsr_id){1, 1};
    spans[7].size = 9;
    crafted.manifest = (struct vsr_blob){spans, 65, 8, 0};
    struct mem_step step =
        mem_node_notify(nodes[0], index, VSR_IO_OK, &crafted);
    CHECK(step.result == VSR_ELIMIT && step.consumed == 0);
    CHECK(healthy(nodes[0]).failure.code == VSR_FAILURE_NONE);
    CHECK(find(nodes[0], VSR_OP_SNAPSHOT_CAPTURE) == index);
    CHECK(mem_node_effect(nodes[0], index)->id == id);
    spans[7].size = 8;
    crafted.manifest = (struct vsr_blob){spans, 64, 8, 0};
    step = mem_node_notify(nodes[0], index, VSR_IO_OK, &crafted);
    CHECK(step.result >= VSR_OK && step.consumed == 1);
    traces[0].captures++;
    run(cluster, nodes, 1, traces);
    assert_checkpoint(nodes[0], 1);
    struct mem_store *store = mem_node_store(nodes[0]);
    const struct vsr_recovered *stored =
        mem_store_recovered(store, mem_store_readable(store));
    CHECK(stored->checkpoint->manifest.size == 64);
    CHECK(same_bytes(&stored->checkpoint->manifest, bytes, 64));
    /* A discovery offer carries the published manifest. */
    const struct vsr_fetch fetch = {{{2, 1}, 1}, {{0, 0}, 0}, 0, 0,
                                    1024 + 64,   1,           0};
    const struct vsr_message discovery = {
        {1, 1}, 0, 0, 2, VSR_MSG_GET_STATE, 0, 0, &fetch};
    const struct vsr_event event = {VSR_EVENT_MESSAGE, 0, 0, &discovery, 1};
    CHECK(mem_node_event(nodes[0], &event).consumed == 1);
    for (size_t attempt = 0; attempt < 100; attempt++) {
        if (find(nodes[0], VSR_OP_SEND) != SIZE_MAX)
            break;
        const struct mem_step drain = mem_node_event(nodes[0], NULL);
        CHECK(drain.result == VSR_OK || drain.result == VSR_AGAIN);
    }
    const size_t send = find(nodes[0], VSR_OP_SEND);
    CHECK(send != SIZE_MAX);
    const struct vsr_message *response = mem_node_effect(nodes[0], send)->data;
    CHECK(response->type == VSR_MSG_NEW_STATE);
    const struct vsr_state_chunk *chunk = response->body;
    CHECK(chunk->state.checkpoint != NULL);
    CHECK(chunk->state.checkpoint->op == 1);
    CHECK(same_bytes(&chunk->state.checkpoint->manifest, bytes, 64));
    run(cluster, nodes, 1, traces);
    /* A durable restart recovers the same manifest and application state. */
    const uint64_t checksum = mem_node_checksum(nodes[0]);
    mem_node_crash(nodes[0]);
    CHECK(mem_node_restart(nodes[0], (struct vsr_id){1, 2}) == VSR_OK);
    CHECK(mem_node_time(nodes[0], 0).consumed == 1);
    run(cluster, nodes, 1, traces);
    CHECK(healthy(nodes[0]).state == VSR_STATE_NORMAL);
    CHECK(healthy(nodes[0]).applied == 1);
    CHECK(mem_node_checksum(nodes[0]) == checksum);
    assert_checkpoint(nodes[0], 1);
    stored = mem_store_recovered(store, mem_store_readable(store));
    CHECK(same_bytes(&stored->checkpoint->manifest, bytes, 64));
    submit(nodes[0], 2);
    run(cluster, nodes, 1, traces);
    CHECK(healthy(nodes[0]).applied == 2);
    mem_cluster_destroy(cluster);
}

/* CAPTURE "supplies a checkpoint template containing the exact op, view, and
 * epoch." Across a reconfiguration boundary no template names the new epoch
 * with an operation before its boundary, and capture still completes. */
static void capture_across_boundary(void)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[] = {single(cluster, 1, false)};
    struct trace traces[] = {{0}};
    const struct vsr_member member = {1, VSR_MEMBER_FULL, 0};
    const struct vsr_membership next = {1, &member, 1, 0};
    check_boundary = true;
    run(cluster, nodes, 1, traces);
    submit(nodes[0], 1);
    run(cluster, nodes, 1, traces);
    assert_checkpoint(nodes[0], 1);
    const struct vsr_request reconfigure = {
        {{301, 1}, 1}, 0, VSR_REQUEST_RECONFIGURE, 0, &next};
    const struct vsr_event event = {VSR_EVENT_REQUEST, 0, 50, &reconfigure, 1};
    CHECK(mem_node_event(nodes[0], &event).consumed == 1);
    run(cluster, nodes, 1, traces);
    struct vsr_status current = healthy(nodes[0]);
    CHECK(current.epoch == 1 && current.state == VSR_STATE_NORMAL);
    CHECK(current.applied == 2);
    CHECK(request(nodes[0], 2, 2, 1).consumed == 1);
    run(cluster, nodes, 1, traces);
    CHECK(healthy(nodes[0]).applied == 3);
    CHECK(request(nodes[0], 3, 3, 1).consumed == 1);
    run(cluster, nodes, 1, traces);
    current = healthy(nodes[0]);
    CHECK(current.applied == 4);
    CHECK(current.checkpoint_op >= 3);
    CHECK(traces[0].captures >= 2);
    check_boundary = false;
    mem_cluster_destroy(cluster);
}

/* Capture "may be deferred ... while retained inputs leave less than the
 * capture baseline; deferral releases only applied cache entries whose
 * release restores that baseline and never blocks a transition." With the
 * minimum payload budget consumed by admitted requests, a pending hint still
 * yields a checkpoint once those requests are applied and stored. */
static void capacity_deferral(void)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[] = {single(cluster, 0, true)};
    struct trace traces[] = {{0}};
    const struct schedule held = {UINT64_C(1) << VSR_OP_APPLY};
    run(cluster, nodes, 1, traces);
    submit(nodes[0], 1);
    drive(cluster, nodes, 1, &held, traces);
    CHECK(find(nodes[0], VSR_OP_APPLY) != SIZE_MAX);
    hint(nodes[0]);
    uint64_t accepted = 1;
    for (uint64_t number = 2; number <= 100; number++) {
        const struct mem_step step = request(nodes[0], number, number, 0);
        if (step.consumed == 0) {
            CHECK(step.result == VSR_AGAIN);
            break;
        }
        accepted = number;
        drive(cluster, nodes, 1, &held, traces);
    }
    CHECK(accepted > 1 && accepted < 100);
    CHECK(find(nodes[0], VSR_OP_SNAPSHOT_CAPTURE) == SIZE_MAX);
    CHECK(traces[0].captures == 0);
    run(cluster, nodes, 1, traces);
    const struct vsr_status current = healthy(nodes[0]);
    CHECK(current.applied == accepted);
    CHECK(traces[0].captures >= 1);
    CHECK(current.checkpoint_op >= 1);
    CHECK(mem_node_replies(nodes[0]) == accepted);
    mem_cluster_destroy(cluster);
}

int main(void)
{
    hint_deferred_by_fence(FENCE_APPLY);
    hint_deferred_by_fence(FENCE_READ);
    hint_deferred_by_fence(FENCE_REPLAY);
    hints_coalesce();
    interval_capture();
    witness_ignores_hints();
    genesis_capture();
    capture_retry(VSR_IO_RETRY);
    capture_retry(VSR_IO_FAILED);
    capture_retry(VSR_IO_CORRUPT);
    for (enum variant variant = VARIANT_COUNT; variant <= VARIANT_DUPLICATE;
         variant++)
        inconsistent_capture(variant);
    manifest_at_limits();
    capture_across_boundary();
    capacity_deferral();
    return 0;
}
