#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

struct fixture {
    struct mem_cluster *cluster;
    struct mem_node *nodes[3];
    size_t count;
    struct mem_node *target;
    uint64_t disconnected;
};

static struct vsr_status status(struct mem_node *node)
{
    struct vsr_status result;
    vsr_get_status(mem_node_core(node), &result);
    return result;
}

static size_t find(struct mem_node *node, uint32_t type)
{
    for (size_t i = 0; i < mem_node_effects(node); i++) {
        if (mem_node_effect(node, i)->type == type)
            return i;
    }
    return SIZE_MAX;
}

static void drive(struct fixture *fixture, uint32_t until)
{
    for (size_t turn = 0; turn < 20000; turn++) {
        bool progress = false;
        if (find(fixture->target, until) != SIZE_MAX)
            return;
        for (size_t i = 0; i < fixture->count; i++) {
            struct mem_node *node = fixture->nodes[i];
            const struct mem_step step = mem_node_event(node, NULL);
            CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
            progress = progress || step.emitted != 0 ||
                       (step.flags & VSR_UPDATE_MORE) != 0;
            if (find(fixture->target, until) != SIZE_MAX)
                return;
            for (size_t j = 0; j < mem_node_effects(node); j++) {
                if (mem_node_complete(node, j, VSR_IO_OK)) {
                    progress = true;
                    break;
                }
            }
            if (find(fixture->target, until) != SIZE_MAX)
                return;
        }
        for (size_t i = 0; i < mem_cluster_messages(fixture->cluster); i++) {
            uint64_t to;
            (void)mem_cluster_message(fixture->cluster, i, &to);
            if (to == fixture->disconnected) {
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
        if (!progress) {
            if (until != UINT32_MAX) {
                const struct vsr_status current = status(fixture->target);
                fprintf(
                    stderr,
                    "waiting type%u node%llu state%u view%llu applied%llu\n",
                    until, (unsigned long long)mem_node_id(fixture->target),
                    current.state, (unsigned long long)current.view,
                    (unsigned long long)current.applied);
            }
            CHECK(until == UINT32_MAX);
            return;
        }
    }
    CHECK(false);
}

static struct fixture create(size_t count, uint32_t operations)
{
    const struct vsr_member members[] = {{1, VSR_MEMBER_FULL, 0},
                                         {2, VSR_MEMBER_FULL, 0},
                                         {3, VSR_MEMBER_FULL, 0}};
    const struct vsr_membership membership = {0, members, (uint32_t)count,
                                              count == 1 ? 0u : 1u};
    struct fixture fixture = {.cluster = mem_cluster_create(), .count = count};
    for (size_t i = 0; i < count; i++) {
        struct vsr_options options = mem_options(i + 1, &membership);
        options.limits.operations = operations;
        options.limits.work_per_step = 128;
        options.limits.log_cache_entries = 2;
        options.limits.batch_entries = 1;
        fixture.nodes[i] = mem_cluster_add(fixture.cluster, &options);
        mem_node_output_capacity(fixture.nodes[i], 1);
        CHECK(mem_node_time(fixture.nodes[i], 0).consumed == 1);
    }
    fixture.target = fixture.nodes[0];
    return fixture;
}

static struct mem_step request(struct mem_node *node, uint64_t number)
{
    const struct vsr_span spans[] = {{"alpha", 5}, {"beta", 4}};
    const struct vsr_blob command = {spans, 9, 2, 0};
    const struct vsr_request request = {
        {{700, number}, 1}, 0, VSR_REQUEST_COMMAND, 0, &command};
    const struct vsr_event event = {VSR_EVENT_REQUEST, 0, number, &request, 1};
    return mem_node_event(node, &event);
}

static void submit(struct mem_node *node, uint64_t number)
{
    CHECK(request(node, number).consumed == 1);
}

static void hint(struct mem_node *node)
{
    const struct vsr_event event = {VSR_EVENT_CHECKPOINT, 0, 0, NULL, 0};
    CHECK(mem_node_event(node, &event).consumed == 1);
}

/* Reach each operation through actual public protocol work, including FETCH
 * from a compacted quorum into an empty RECOVER replica. */
static struct fixture pending(uint32_t type)
{
    struct fixture fixture =
        create(type == VSR_OP_SEND || type == VSR_OP_SNAPSHOT_FETCH ? 3 : 1, 4);
    if (type == VSR_OP_LOAD || type == VSR_OP_STORE || type == VSR_OP_SYNC ||
        type == VSR_OP_RECLAIM || type == VSR_OP_SNAPSHOT_INSTALL ||
        type == VSR_OP_SEND) {
        drive(&fixture, type);
        return fixture;
    }
    if (type == VSR_OP_SNAPSHOT_FETCH) {
        /* The absent third member has never participated in this history. */
        mem_node_crash(fixture.nodes[2]);
        fixture.count = 2;
    }
    drive(&fixture, UINT32_MAX);
    CHECK(status(fixture.target).state == VSR_STATE_NORMAL);
    submit(fixture.target, 1);
    if (type == VSR_OP_APPLY || type == VSR_OP_REPLY) {
        drive(&fixture, type);
        return fixture;
    }
    drive(&fixture, UINT32_MAX);
    CHECK(status(fixture.target).applied == 1);
    if (type == VSR_OP_READ_READY) {
        const struct vsr_read_barrier read = {1, VSR_NO_DEADLINE,
                                              VSR_READ_CAUSAL, 0};
        const struct vsr_event event = {VSR_EVENT_READ, 0, 19, &read, 1};
        CHECK(mem_node_event(fixture.target, &event).consumed == 1);
        drive(&fixture, type);
        return fixture;
    }
    hint(fixture.target);
    if (type == VSR_OP_SNAPSHOT_CAPTURE || type == VSR_OP_SNAPSHOT_SYNC) {
        drive(&fixture, type);
        return fixture;
    }
    drive(&fixture, UINT32_MAX);
    CHECK(status(fixture.target).checkpoint_op == 1);
    if (type == VSR_OP_SNAPSHOT_FETCH) {
        CHECK(mem_node_restart(fixture.nodes[2], (struct vsr_id){3, 2}) ==
              VSR_OK);
        fixture.count = 3;
        fixture.target = fixture.nodes[2];
        CHECK(mem_node_time(fixture.target, 0).consumed == 1);
        drive(&fixture, type);
        return fixture;
    }
    CHECK(type == VSR_OP_SNAPSHOT_DROP);
    submit(fixture.target, 2);
    drive(&fixture, UINT32_MAX);
    hint(fixture.target);
    drive(&fixture, type);
    return fixture;
}

static void stop_and_drain(struct fixture *fixture, bool success)
{
    struct mem_node *node = fixture->target;
    const struct vsr_event stop = {VSR_EVENT_STOP, 0, 0, NULL, 0};
    const struct vsr_failure first = status(node).failure;
    const size_t outstanding = mem_node_effects(node);
    struct mem_step step = {0};
    for (unsigned int repeat = 0; repeat < 2; repeat++) {
        for (size_t attempt = 0; attempt < 1000; attempt++) {
            step = mem_node_event(node, &stop);
            CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
            if (step.consumed != 0)
                break;
        }
        CHECK(step.consumed == 1 && step.deadline == VSR_NO_DEADLINE);
    }
    if (outstanding != 0) {
        CHECK(status(node).state == VSR_STATE_STOPPING);
        CHECK(vsr_deinit(mem_node_core(node)) == VSR_EBUSY);
    }
    for (size_t turn = 0; turn < 20000; turn++) {
        step = mem_node_event(node, NULL);
        CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
        CHECK(step.deadline == VSR_NO_DEADLINE);
        if (mem_node_effects(node) != 0) {
            bool completed = false;
            for (size_t i = 0; i < mem_node_effects(node); i++) {
                if (mem_node_complete(node, i,
                                      success ? VSR_IO_OK : VSR_IO_CANCELLED)) {
                    completed = true;
                    break;
                }
            }
            CHECK(completed);
        }
        if (status(node).state == VSR_STATE_STOPPED)
            break;
    }
    const struct vsr_status final = status(node);
    CHECK(final.state == VSR_STATE_STOPPED);
    CHECK(final.outstanding_ops == 0 && final.outstanding_leases == 0);
    CHECK(mem_node_effects(node) == 0 && mem_node_leases(node) == 0);
    if (first.code != VSR_FAILURE_NONE)
        CHECK(memcmp(&first, &final.failure, sizeof(first)) == 0);
    mem_cluster_check(fixture->cluster);
    CHECK(vsr_deinit(mem_node_core(node)) == VSR_OK);
}

static void stop_matrix(void)
{
    for (uint32_t type = VSR_OP_SEND; type < VSR_OP_RELEASE; type++) {
        for (unsigned int success = 0; success < 2; success++) {
            struct fixture fixture = pending(type);
            CHECK(find(fixture.target, type) != SIZE_MAX);
            stop_and_drain(&fixture, success != 0);
            mem_cluster_destroy(fixture.cluster);
        }
    }
}

static uint32_t failure_for(uint32_t type, int code)
{
    switch (type) {
    case VSR_OP_STORE:
    case VSR_OP_SYNC:
        return VSR_FAILURE_STORAGE;
    case VSR_OP_APPLY:
        return VSR_FAILURE_APPLICATION;
    case VSR_OP_SNAPSHOT_INSTALL:
        return code == VSR_IO_CORRUPT ? VSR_FAILURE_SNAPSHOT
                                      : VSR_FAILURE_APPLICATION;
    case VSR_OP_SNAPSHOT_SYNC:
        return VSR_FAILURE_SNAPSHOT;
    case VSR_OP_LOAD:
        return code >= VSR_IO_CORRUPT ? VSR_FAILURE_STORAGE : VSR_FAILURE_NONE;
    case VSR_OP_RECLAIM:
        return code == VSR_IO_CORRUPT ? VSR_FAILURE_STORAGE : VSR_FAILURE_NONE;
    case VSR_OP_SNAPSHOT_CAPTURE:
    case VSR_OP_SNAPSHOT_FETCH:
    case VSR_OP_SNAPSHOT_DROP:
        return code == VSR_IO_CORRUPT ? VSR_FAILURE_SNAPSHOT : VSR_FAILURE_NONE;
    default:
        return VSR_FAILURE_NONE;
    }
}

static void failure_matrix(void)
{
    for (uint32_t type = VSR_OP_SEND; type < VSR_OP_RELEASE; type++) {
        for (int code = VSR_IO_RETRY; code <= VSR_IO_CANCELLED; code++) {
            struct fixture fixture = pending(type);
            const size_t index = find(fixture.target, type);
            const uint64_t id = mem_node_effect(fixture.target, index)->id;
            CHECK(mem_node_complete(fixture.target, index, code));
            const struct vsr_status current = status(fixture.target);
            const uint32_t expected = failure_for(type, code);
            if (current.failure.code != expected)
                fprintf(stderr, "type=%u io=%d failure=%u expected=%u\n", type,
                        code, current.failure.code, expected);
            CHECK(current.failure.code == expected);
            if (expected != VSR_FAILURE_NONE) {
                CHECK(current.state == VSR_STATE_FAILED);
                CHECK(current.failure.operation == id);
                CHECK(current.failure.operation_type == type);
                CHECK(current.failure.status == code);
            }
            stop_and_drain(&fixture, false);
            mem_cluster_destroy(fixture.cluster);
        }
    }
}

static void completion_validation(void)
{
    struct fixture fixture = pending(VSR_OP_LOAD);
    size_t index = find(fixture.target, VSR_OP_LOAD);
    const uint64_t id = mem_node_effect(fixture.target, index)->id;
    struct mem_step step =
        mem_node_notify(fixture.target, index, VSR_IO_OK, NULL);
    CHECK(step.result == VSR_EINVAL && step.consumed == 0);
    CHECK(status(fixture.target).failure.code == VSR_FAILURE_NONE);
    CHECK(mem_node_effect(fixture.target, index)->id == id);
    CHECK(mem_node_complete(fixture.target, index, VSR_IO_OK));
    const struct vsr_event duplicate = {VSR_EVENT_COMPLETE, VSR_IO_CANCELLED,
                                        id, NULL, 0};
    step = mem_node_event(fixture.target, &duplicate);
    CHECK(step.result == VSR_EINVAL && step.consumed == 0);
    stop_and_drain(&fixture, false);
    mem_cluster_destroy(fixture.cluster);

    fixture = pending(VSR_OP_APPLY);
    index = find(fixture.target, VSR_OP_APPLY);
    const struct vsr_applied inconsistent = {NULL, 0, 0};
    step = mem_node_notify(fixture.target, index, VSR_IO_OK, &inconsistent);
    CHECK(step.consumed == 1);
    CHECK(status(fixture.target).state == VSR_STATE_FAILED);
    CHECK(status(fixture.target).failure.code == VSR_FAILURE_APPLICATION);
    stop_and_drain(&fixture, false);
    mem_cluster_destroy(fixture.cluster);
}

static bool store_has(const struct vsr_op *operation, uint32_t type)
{
    CHECK(operation->type == VSR_OP_STORE);
    const struct vsr_store *store = operation->data;
    for (uint32_t i = 0; i < store->count; i++) {
        if (store->changes[i].type == type)
            return true;
    }
    return false;
}

/* The adapter may flush a physically completed transaction before either
 * completion is notified. Recovery must use that actual durable prefix. */
static void flush_without_notifications(struct mem_node *node,
                                        uint64_t sequence)
{
    size_t index = SIZE_MAX;
    for (size_t attempt = 0; attempt < 1000; attempt++) {
        index = find(node, VSR_OP_SYNC);
        if (index != SIZE_MAX)
            break;
        const struct mem_step step = mem_node_event(node, NULL);
        CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
    }
    CHECK(index != SIZE_MAX);
    CHECK(mem_node_effect(node, index)->arg >= sequence);
    CHECK(mem_node_execute(node, index, VSR_IO_OK));
    CHECK(mem_store_durable(mem_node_store(node)) >= sequence);
    CHECK(status(node).stored_sequence < sequence);
    CHECK(status(node).durable_sequence < sequence);
}

static void append_crashes(void)
{
    for (unsigned int stage = 0; stage < 3; stage++) {
        struct fixture fixture = create(1, 8);
        drive(&fixture, UINT32_MAX);
        const uint64_t previous =
            mem_store_durable(mem_node_store(fixture.target));
        submit(fixture.target, 1);
        drive(&fixture, VSR_OP_STORE);
        size_t index = find(fixture.target, VSR_OP_STORE);
        CHECK(store_has(mem_node_effect(fixture.target, index),
                        VSR_STORE_APPEND));
        const uint64_t sequence =
            ((const struct vsr_store *)mem_node_effect(fixture.target, index)
                 ->data)
                ->sequence;
        CHECK(sequence > previous);
        if (stage >= 1) {
            CHECK(mem_node_execute(fixture.target, index, VSR_IO_OK));
            CHECK(mem_store_readable(mem_node_store(fixture.target)) ==
                  sequence);
            CHECK(status(fixture.target).stored_sequence < sequence);
        }
        if (stage == 2)
            flush_without_notifications(fixture.target, sequence);
        CHECK(status(fixture.target).committed == 0);
        CHECK(mem_node_applied(fixture.target) == 0);
        mem_node_crash(fixture.target);
        const struct vsr_recovered *recovered = mem_store_recovered(
            mem_node_store(fixture.target),
            mem_store_durable(mem_node_store(fixture.target)));
        CHECK(recovered != NULL);
        CHECK(recovered->log_end == (stage == 2 ? 2u : 1u));
        CHECK(recovered->hard.committed == 0);
        CHECK(mem_node_restart(fixture.target, (struct vsr_id){1, 2}) ==
              VSR_OK);
        CHECK(mem_node_time(fixture.target, 0).consumed == 1);
        drive(&fixture, UINT32_MAX);
        submit(fixture.target, 1);
        drive(&fixture, UINT32_MAX);
        CHECK(status(fixture.target).state == VSR_STATE_NORMAL);
        CHECK(status(fixture.target).applied == 1);
        CHECK(mem_node_history(fixture.target, 1) != NULL);
        CHECK(mem_node_history(fixture.target, 2) == NULL);
        CHECK(mem_node_replies(fixture.target) == 1);
        stop_and_drain(&fixture, true);
        mem_cluster_destroy(fixture.cluster);
    }
}

static void snapshot_crashes(void)
{
    for (unsigned int stage = 0; stage < 4; stage++) {
        struct fixture fixture = pending(stage == 0 ? VSR_OP_SNAPSHOT_CAPTURE
                                                    : VSR_OP_SNAPSHOT_SYNC);
        struct mem_node *node = fixture.target;
        if (stage == 0) {
            /* A completed external capture whose success was never notified. */
            CHECK(mem_node_execute(node, find(node, VSR_OP_SNAPSHOT_CAPTURE),
                                   VSR_IO_OK));
        } else {
            size_t index = find(node, VSR_OP_SNAPSHOT_SYNC);
            CHECK(mem_node_execute(node, index, VSR_IO_OK));
            if (stage >= 2) {
                CHECK(mem_node_complete(node, index, VSR_IO_OK));
                drive(&fixture, VSR_OP_STORE);
                index = find(node, VSR_OP_STORE);
                CHECK(store_has(mem_node_effect(node, index),
                                VSR_STORE_PUBLISH_CHECKPOINT));
                const uint64_t sequence =
                    ((const struct vsr_store *)mem_node_effect(node, index)
                         ->data)
                        ->sequence;
                CHECK(mem_node_execute(node, index, VSR_IO_OK));
                if (stage == 3)
                    flush_without_notifications(node, sequence);
            }
        }
        /* Neither capture nor an unnotified publication moves public status. */
        CHECK(status(node).checkpoint_op == 0);
        mem_node_crash(node);
        const struct vsr_recovered *recovered = mem_store_recovered(
            mem_node_store(node), mem_store_durable(mem_node_store(node)));
        CHECK(recovered != NULL);
        CHECK((recovered->checkpoint != NULL) == (stage == 3));
        CHECK(mem_node_restart(node, (struct vsr_id){1, 2}) == VSR_OK);
        CHECK(mem_node_time(node, 0).consumed == 1);
        drive(&fixture, UINT32_MAX);
        CHECK(status(node).state == VSR_STATE_NORMAL);
        CHECK(status(node).applied == 1);
        CHECK(status(node).checkpoint_op == (stage == 3 ? 1u : 0u));
        submit(node, 2);
        drive(&fixture, UINT32_MAX);
        CHECK(status(node).applied == 2);
        stop_and_drain(&fixture, true);
        mem_cluster_destroy(fixture.cluster);
    }
}

static void stop_under_saturation(void)
{
    struct fixture fixture = create(1, 1);
    drive(&fixture, UINT32_MAX);
    submit(fixture.target, 1);
    drive(&fixture, VSR_OP_APPLY);
    CHECK(mem_node_effects(fixture.target) == 1);
    size_t accepted = 0;
    bool full = false;
    for (uint64_t number = 2; number <= 1000; number++) {
        const struct mem_step step = request(fixture.target, number);
        CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
        if (step.consumed == 0) {
            CHECK(step.result == VSR_AGAIN);
            full = true;
            break;
        }
        accepted++;
    }
    CHECK(full && accepted != 0);
    CHECK(mem_node_leases(fixture.target) > 1);
    CHECK(status(fixture.target).applied == 0);
    /* The single effect slot and pending queue are full. STOP still consumes,
     * then the successful late APPLY brings a new result lease and releases
     * every abandoned request lease through one output descriptor. */
    stop_and_drain(&fixture, true);
    CHECK(mem_node_applied(fixture.target) == 1);
    CHECK(mem_node_replies(fixture.target) == 0);
    mem_cluster_destroy(fixture.cluster);
}

static void late_fetch_after_source_timeout(void)
{
    struct fixture fixture = create(3, 4);
    drive(&fixture, UINT32_MAX);
    fixture.count = 2;
    fixture.disconnected = 3;
    submit(fixture.nodes[0], 1);
    drive(&fixture, UINT32_MAX);
    hint(fixture.nodes[0]);
    drive(&fixture, UINT32_MAX);
    CHECK(status(fixture.nodes[0]).checkpoint_op == 1);
    CHECK(status(fixture.nodes[2]).applied == 0);
    fixture.count = 3;
    fixture.disconnected = 0;
    fixture.target = fixture.nodes[2];
    const struct vsr_message commit = {{1, 1},         0, 0, 1,
                                       VSR_MSG_COMMIT, 0, 1, NULL};
    const struct vsr_event announcement = {VSR_EVENT_MESSAGE, 0, 0, &commit, 1};
    CHECK(mem_node_event(fixture.target, &announcement).consumed == 1);
    drive(&fixture, VSR_OP_SNAPSHOT_FETCH);
    struct mem_node *node = fixture.target;
    const uint64_t fetch =
        mem_node_effect(node, find(node, VSR_OP_SNAPSHOT_FETCH))->id;
    /* A successful transfer can arrive after discovery has abandoned its
     * selected offer. Its immutable input/output graphs remain pinned until
     * completion, and the obsolete adoption must release its app fence. */
    for (size_t attempt = 0; attempt < 1000; attempt++) {
        const struct mem_step step = mem_node_time(node, 200);
        CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
        if (step.consumed != 0)
            break;
    }
    const size_t index = find(node, VSR_OP_SNAPSHOT_FETCH);
    CHECK(index != SIZE_MAX && mem_node_effect(node, index)->id == fetch);
    CHECK(mem_node_complete(node, index, VSR_IO_OK));
    /* Before any new offer is delivered, the obsolete FETCH result must not
     * be flushed, published, or installed. Its hold may only be dropped. */
    for (size_t turn = 0; turn < 1000; turn++) {
        const struct mem_step step = mem_node_event(node, NULL);
        bool progress =
            step.emitted != 0 || (step.flags & VSR_UPDATE_MORE) != 0;
        for (size_t i = 0; i < mem_node_effects(node); i++) {
            const struct vsr_op *operation = mem_node_effect(node, i);
            CHECK(operation->type != VSR_OP_SNAPSHOT_SYNC);
            CHECK(operation->type != VSR_OP_SNAPSHOT_INSTALL);
            if (operation->type == VSR_OP_STORE)
                CHECK(!store_has(operation, VSR_STORE_RESTORE_CHECKPOINT));
            if (mem_node_complete(node, i, VSR_IO_OK)) {
                progress = true;
                break;
            }
        }
        if (!progress)
            break;
    }
    drive(&fixture, UINT32_MAX);
    for (uint64_t time = 210;
         time <= 1000 && status(node).state != VSR_STATE_NORMAL; time += 10) {
        for (size_t i = 0; i < fixture.count; i++) {
            struct mem_step step = mem_node_time(fixture.nodes[i], time);
            if (step.consumed == 0) {
                drive(&fixture, UINT32_MAX);
                step = mem_node_time(fixture.nodes[i], time);
            }
            CHECK(step.consumed == 1);
        }
        drive(&fixture, UINT32_MAX);
    }
    CHECK(status(node).failure.code == VSR_FAILURE_NONE);
    CHECK(status(node).state == VSR_STATE_NORMAL);
    CHECK(status(node).applied == 1);
    CHECK(mem_node_checksum(node) == mem_node_checksum(fixture.nodes[0]));
    stop_and_drain(&fixture, true);
    mem_cluster_destroy(fixture.cluster);
}

int main(int argc, char **argv)
{
    CHECK(argc <= 2);
    if (argc == 1 || strcmp(argv[1], "stop") == 0)
        stop_matrix();
    if (argc == 1 || strcmp(argv[1], "failure") == 0)
        failure_matrix();
    if (argc == 1 || strcmp(argv[1], "completion") == 0)
        completion_validation();
    if (argc == 1 || strcmp(argv[1], "append-crash") == 0)
        append_crashes();
    if (argc == 1 || strcmp(argv[1], "snapshot-crash") == 0)
        snapshot_crashes();
    if (argc == 1 || strcmp(argv[1], "saturation") == 0)
        stop_under_saturation();
    if (argc == 1 || strcmp(argv[1], "late-fetch") == 0)
        late_fetch_after_source_timeout();
    return 0;
}
