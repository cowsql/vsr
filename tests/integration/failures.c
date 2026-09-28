#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

/* Contract tests for the "Application operations and failures" table and the
 * "Indexed storage" load rules of docs/vsr-api.md. Every scenario reaches an
 * operation through ordinary protocol work, then completes it with a chosen
 * vsr_io_status or with a structurally valid but inconsistent result, and
 * checks the documented outcome: retry without fencing, expected absence, or
 * a latched fatal failure that survives through STOPPED. */

struct fixture {
    struct mem_cluster *cluster;
    struct mem_node *nodes[3];
    size_t count;
};

/* Effects matching a hold stay outstanding; UINT32_MAX matches any load type. */
struct hold {
    uint32_t type;
    uint32_t load;
};

static const struct hold none = {UINT32_MAX, UINT32_MAX};

static struct vsr_status status(struct mem_node *node)
{
    struct vsr_status result;
    vsr_get_status(mem_node_core(node), &result);
    return result;
}

static uint32_t load_type(const struct vsr_op *op)
{
    if (op->type != VSR_OP_LOAD)
        return UINT32_MAX;
    return ((const struct vsr_store_read *)op->data)->type;
}

static bool matches(const struct vsr_op *op, struct hold hold)
{
    return op->type == hold.type &&
           (hold.load == UINT32_MAX || load_type(op) == hold.load);
}

static size_t find(struct mem_node *node, struct hold hold)
{
    for (size_t i = 0; i < mem_node_effects(node); i++) {
        if (matches(mem_node_effect(node, i), hold))
            return i;
    }
    return SIZE_MAX;
}

/* Run all nodes until nothing progresses, completing every effect except the
 * held ones with OK and delivering every message. The first `failing` SEND
 * completions use send_status instead; failed sends are retried at once, so
 * an unbounded failure schedule would never idle. */
static void drive_with(struct fixture *f, struct hold hold, int send_status,
                       size_t failing)
{
    for (size_t turn = 0; turn < 20000; turn++) {
        bool progress = false;
        for (size_t i = 0; i < f->count; i++) {
            struct mem_node *node = f->nodes[i];
            if (!mem_node_alive(node))
                continue;
            const struct mem_step step = mem_node_event(node, NULL);
            CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
            progress = progress || step.emitted != 0 ||
                       (step.flags & VSR_UPDATE_MORE) != 0;
            for (size_t j = 0; j < mem_node_effects(node); j++) {
                const struct vsr_op *op = mem_node_effect(node, j);
                if (matches(op, hold))
                    continue;
                int code = VSR_IO_OK;
                if (op->type == VSR_OP_SEND && failing != 0) {
                    code = send_status;
                    failing--;
                }
                if (mem_node_complete(node, j, code)) {
                    progress = true;
                    break;
                }
            }
        }
        for (size_t i = 0; i < mem_cluster_messages(f->cluster); i++) {
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

static void drive(struct fixture *f, struct hold hold)
{
    drive_with(f, hold, VSR_IO_OK, 0);
}

static void time_all(struct fixture *f, uint64_t now)
{
    for (size_t i = 0; i < f->count; i++) {
        if (mem_node_alive(f->nodes[i]))
            CHECK(mem_node_time(f->nodes[i], now).consumed == 1);
    }
}

static struct vsr_options options_for(uint64_t replica,
                                      const struct vsr_membership *membership)
{
    struct vsr_options options = mem_options(replica, membership);
    options.limits.batch_entries = 1;
    options.limits.log_cache_entries = 2;
    options.limits.client_cache_entries = 1;
    return options;
}

static const struct vsr_member members[] = {
    {1, VSR_MEMBER_FULL, 0}, {2, VSR_MEMBER_FULL, 0}, {3, VSR_MEMBER_FULL, 0}};

static struct vsr_membership membership_of(uint32_t count)
{
    return (struct vsr_membership){0, members, count, count == 1 ? 0u : 1u};
}

static struct fixture create(size_t count, uint32_t start_mode)
{
    const struct vsr_membership membership = membership_of((uint32_t)count);
    struct fixture f = {.cluster = mem_cluster_create(), .count = count};
    for (size_t i = 0; i < count; i++) {
        struct vsr_options options = options_for(i + 1, &membership);
        options.start_mode = start_mode;
        f.nodes[i] = mem_cluster_add(f.cluster, &options);
        CHECK(mem_node_time(f.nodes[i], 0).consumed == 1);
    }
    return f;
}

static struct mem_step request(struct mem_node *node, uint64_t client,
                               uint64_t number, uint64_t route)
{
    const struct vsr_span span = {"failure-command", 15};
    const struct vsr_blob command = {&span, 15, 1, 0};
    const struct vsr_request request = {
        {{client, 1}, number}, 0, VSR_REQUEST_COMMAND, 0, &command};
    const struct vsr_event event = {VSR_EVENT_REQUEST, 0, route, &request, 1};
    return mem_node_event(node, &event);
}

static void submit(struct mem_node *node, uint64_t client, uint64_t number,
                   uint64_t route)
{
    CHECK(request(node, client, number, route).consumed == 1);
}

static void hint(struct mem_node *node)
{
    const struct vsr_event event = {VSR_EVENT_CHECKPOINT, 0, 0, NULL, 0};
    CHECK(mem_node_event(node, &event).consumed == 1);
}

static const struct vsr_reply *reply_for(struct mem_node *node, uint64_t route)
{
    for (size_t i = mem_node_replies(node); i > 0; i--) {
        uint64_t found;
        const struct vsr_reply *reply = mem_node_reply(node, i - 1, &found);
        if (found == route)
            return reply;
    }
    return NULL;
}

/* "vsr_get_status exposes the first fatal failure, its triggering operation
 * and completion status" and "Diagnostic information remains available
 * through STOPPED." */
static void check_failed(struct mem_node *node, uint32_t code, uint64_t op,
                         uint32_t type, int32_t io)
{
    const struct vsr_status current = status(node);
    CHECK(current.state == VSR_STATE_FAILED);
    CHECK(current.failure.code == code);
    CHECK(current.failure.operation == op);
    CHECK(current.failure.operation_type == type);
    CHECK(current.failure.status == io);
}

static void stop_and_drain(struct fixture *f, struct mem_node *node)
{
    const struct vsr_event event = {VSR_EVENT_STOP, 0, 0, NULL, 0};
    const struct vsr_failure first = status(node).failure;
    struct mem_step step = {0};
    for (size_t attempt = 0; attempt < 1000 && step.consumed == 0; attempt++) {
        step = mem_node_event(node, &event);
        CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
    }
    CHECK(step.consumed == 1 && step.deadline == VSR_NO_DEADLINE);
    for (size_t turn = 0; turn < 20000; turn++) {
        step = mem_node_event(node, NULL);
        CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
        for (size_t i = 0; i < mem_node_effects(node); i++) {
            if (mem_node_complete(node, i, VSR_IO_CANCELLED))
                break;
        }
        if (status(node).state == VSR_STATE_STOPPED)
            break;
    }
    const struct vsr_status final = status(node);
    CHECK(final.state == VSR_STATE_STOPPED);
    CHECK(final.outstanding_ops == 0 && final.outstanding_leases == 0);
    CHECK(memcmp(&first, &final.failure, sizeof(first)) == 0);
    mem_cluster_check(f->cluster);
    CHECK(vsr_deinit(mem_node_core(node)) == VSR_OK);
}

static void finish(struct fixture *f)
{
    mem_cluster_destroy(f->cluster);
}

/* A valid genesis recovery row for replica 1 of a one-member cluster. */
struct recovered_row {
    struct vsr_membership membership;
    struct vsr_epoch epoch;
    struct vsr_recovered recovered;
    struct vsr_loaded loaded;
};

static void recovered_row(struct recovered_row *row, uint64_t replica,
                          uint32_t durability)
{
    memset(row, 0, sizeof(*row));
    row->membership = membership_of(1);
    row->epoch =
        (struct vsr_epoch){&row->membership, NULL, 0, VSR_EPOCH_STEADY, 0};
    row->recovered = (struct vsr_recovered){
        .identity = {{1, 1}, replica, durability, 0},
        .sequence = 1,
        .log_begin = 1,
        .log_end = 1,
        .hard = {0, 0, 0, &row->epoch, VSR_HARD_NORMAL, VSR_MEMBER_FULL},
        .checkpoint = NULL};
    row->loaded = (struct vsr_loaded){&row->recovered, 1, 0, 1, 0};
}

/* "LOAD | Retry transient unavailability": a RETRY on the startup load must
 * be followed by a fresh LOAD_RECOVERY, never by a failure. */
static void recovery_load_retry(void)
{
    const struct hold load = {VSR_OP_LOAD, VSR_LOAD_RECOVERY};
    struct fixture f = create(1, VSR_START_NEW);
    struct mem_node *node = f.nodes[0];
    drive(&f, load);
    size_t index = find(node, load);
    CHECK(index != SIZE_MAX);
    const uint64_t first = mem_node_effect(node, index)->id;
    CHECK(mem_node_complete(node, index, VSR_IO_RETRY));
    CHECK(status(node).failure.code == VSR_FAILURE_NONE);
    CHECK(status(node).state == VSR_STATE_STARTING);
    drive(&f, load);
    index = find(node, load);
    CHECK(index != SIZE_MAX && mem_node_effect(node, index)->id != first);
    CHECK(mem_node_complete(node, index, VSR_IO_OK));
    drive(&f, none);
    CHECK(status(node).state == VSR_STATE_NORMAL);
    CHECK(status(node).failure.code == VSR_FAILURE_NONE);
    finish(&f);
}

/* "RECOVER ... an absent store requires quorum recovery, never automatic
 * bootstrap" and "If f=0, an absent ... store cannot obtain n responses from
 * n-1 peers": a lone RECOVER replica whose store is NOT_FOUND stays
 * RECOVERING without failing, and answers requests with BUSY. */
static void recovery_absent_without_quorum(void)
{
    struct fixture f = create(1, VSR_START_RECOVER);
    struct mem_node *node = f.nodes[0];
    drive(&f, none);
    CHECK(status(node).state == VSR_STATE_RECOVERING);
    CHECK(status(node).failure.code == VSR_FAILURE_NONE);
    CHECK(status(node).committed == 0 && status(node).applied == 0);
    CHECK(find(node, (struct hold){VSR_OP_APPLY, UINT32_MAX}) == SIZE_MAX);
    submit(node, 700, 1, 1);
    drive(&f, none);
    const struct vsr_reply *reply = reply_for(node, 1);
    CHECK(reply != NULL);
    CHECK(reply->status == VSR_REPLY_BUSY);
    time_all(&f, 500);
    drive(&f, none);
    CHECK(status(node).state == VSR_STATE_RECOVERING);
    CHECK(status(node).failure.code == VSR_FAILURE_NONE);
    finish(&f);
}

enum identity_case {
    NEW_ON_EXISTING_STORE,
    WRONG_CLUSTER,
    WRONG_REPLICA,
    WRONG_DURABILITY,
    SEQUENCE_EXHAUSTED
};

/* "NEW and JOIN fail on an existing store. RECOVER fails on identity/policy
 * mismatch" and "Identity mismatch / exhausted counter ... Fence immediately
 * and latch the corresponding diagnostic". The load itself succeeded, so the
 * latched completion status is OK and the operation is the LOAD. */
static void recovery_identity(enum identity_case which)
{
    const struct hold load = {VSR_OP_LOAD, VSR_LOAD_RECOVERY};
    struct fixture f = create(1, VSR_START_NEW);
    struct mem_node *node = f.nodes[0];
    struct recovered_row row;
    recovered_row(&row, 1, VSR_DURABLE);
    uint32_t code = VSR_FAILURE_IDENTITY;
    if (which != NEW_ON_EXISTING_STORE) {
        drive(&f, none);
        CHECK(status(node).state == VSR_STATE_NORMAL);
        mem_node_crash(node);
        CHECK(mem_node_restart(node, (struct vsr_id){1, 2}) == VSR_OK);
        CHECK(mem_node_time(node, 0).consumed == 1);
    }
    switch (which) {
    case WRONG_CLUSTER:
        row.recovered.identity.cluster = (struct vsr_id){9, 9};
        break;
    case WRONG_REPLICA:
        row.recovered.identity.replica = 2;
        break;
    case WRONG_DURABILITY:
        row.recovered.identity.durability = VSR_REPLICATED;
        break;
    case SEQUENCE_EXHAUSTED:
        row.recovered.sequence = UINT64_MAX - 1;
        row.loaded.sequence = UINT64_MAX - 1;
        code = VSR_FAILURE_EXHAUSTED;
        break;
    case NEW_ON_EXISTING_STORE:
    default:
        break;
    }
    drive(&f, load);
    const size_t index = find(node, load);
    CHECK(index != SIZE_MAX);
    const uint64_t id = mem_node_effect(node, index)->id;
    const struct mem_step step =
        mem_node_notify(node, index, VSR_IO_OK, &row.loaded);
    CHECK(step.consumed == 1);
    check_failed(node, code, id, VSR_OP_LOAD, VSR_IO_OK);
    stop_and_drain(&f, node);
    finish(&f);
}

/* Restart a durable single node with three committed commands so replay has
 * to read the log back from indexed storage. */
static struct fixture replaying(void)
{
    struct fixture f = create(1, VSR_START_NEW);
    struct mem_node *node = f.nodes[0];
    drive(&f, none);
    for (uint64_t i = 1; i <= 3; i++) {
        submit(node, 700, i, i);
        drive(&f, none);
    }
    CHECK(status(node).applied == 3);
    mem_node_crash(node);
    CHECK(mem_node_restart(node, (struct vsr_id){1, 2}) == VSR_OK);
    CHECK(mem_node_time(node, 0).consumed == 1);
    return f;
}

/* "LOAD | Retry transient unavailability; ... unexpected absence, or
 * permanent failure fences storage": the retained log named by the replay
 * LOAD_LOG cannot be absent, so NOT_FOUND is fatal like CORRUPT, FAILED and
 * CANCELLED, while RETRY is re-issued and replay completes. */
static void log_load_status(int code)
{
    const struct hold load = {VSR_OP_LOAD, VSR_LOAD_LOG};
    struct fixture f = replaying();
    struct mem_node *node = f.nodes[0];
    drive(&f, load);
    size_t index = find(node, load);
    CHECK(index != SIZE_MAX);
    const uint64_t first = mem_node_effect(node, index)->id;
    const struct vsr_store_read read =
        *(const struct vsr_store_read *)mem_node_effect(node, index)->data;
    CHECK(read.first >= 1 && read.end <= 4 && read.first < read.end);
    CHECK(mem_node_complete(node, index, code));
    if (code == VSR_IO_RETRY) {
        CHECK(status(node).failure.code == VSR_FAILURE_NONE);
        drive(&f, load);
        index = find(node, load);
        CHECK(index != SIZE_MAX);
        const struct vsr_op *again = mem_node_effect(node, index);
        const struct vsr_store_read *reissued = again->data;
        CHECK(again->id != first);
        CHECK(reissued->sequence == read.sequence);
        CHECK(reissued->first == read.first && reissued->end == read.end);
        CHECK(mem_node_complete(node, index, VSR_IO_OK));
        drive(&f, none);
        CHECK(status(node).state == VSR_STATE_NORMAL);
        CHECK(status(node).applied == 3);
        CHECK(status(node).failure.code == VSR_FAILURE_NONE);
    } else {
        check_failed(node, VSR_FAILURE_STORAGE, first, VSR_OP_LOAD, code);
        stop_and_drain(&f, node);
    }
    finish(&f);
}

/* "LOG | Consecutive vsr_entry[] beginning at first": a successful result
 * whose entries do not start at first is structurally valid but inconsistent
 * and fences storage with the operation latched and status OK. */
static void log_load_inconsistent(void)
{
    const struct hold load = {VSR_OP_LOAD, VSR_LOAD_LOG};
    struct fixture f = replaying();
    struct mem_node *node = f.nodes[0];
    drive(&f, load);
    const size_t index = find(node, load);
    CHECK(index != SIZE_MAX);
    const struct vsr_op *op = mem_node_effect(node, index);
    const struct vsr_store_read *read = op->data;
    const struct vsr_span span = {"failure-command", 15};
    const struct vsr_blob command = {&span, 15, 1, 0};
    const struct vsr_entry entry = {
        read->first + 1, 0, 0, {{700, 1}, 1}, VSR_REQUEST_COMMAND, 0, &command};
    const struct vsr_loaded loaded = {&entry, read->sequence, read->first + 1,
                                      1, 0};
    const uint64_t id = op->id;
    CHECK(mem_node_notify(node, index, VSR_IO_OK, &loaded).consumed == 1);
    check_failed(node, VSR_FAILURE_STORAGE, id, VSR_OP_LOAD, VSR_IO_OK);
    stop_and_drain(&f, node);
    finish(&f);
}

/* Every request first consults the indexed client and request records. */
static struct fixture requesting(void)
{
    struct fixture f = create(1, VSR_START_NEW);
    drive(&f, none);
    CHECK(status(f.nodes[0]).state == VSR_STATE_NORMAL);
    submit(f.nodes[0], 700, 1, 1);
    return f;
}

/* "CLIENT | Zero or one latest completed vsr_client_record" and "REQUEST |
 * Zero or one latest retained vsr_entry": RETRY re-issues the same read and
 * the request still completes; absence of the live revision (NOT_FOUND) or
 * any other failure fences storage. */
static void route_load_status(uint32_t type, int code)
{
    const struct hold load = {VSR_OP_LOAD, type};
    struct fixture f = requesting();
    struct mem_node *node = f.nodes[0];
    drive(&f, load);
    size_t index = find(node, load);
    CHECK(index != SIZE_MAX);
    const struct vsr_op *op = mem_node_effect(node, index);
    const uint64_t first = op->id;
    const struct vsr_store_read read = *(const struct vsr_store_read *)op->data;
    CHECK(read.client.hi == 700 && read.client.lo == 1);
    CHECK(read.first == 0 && read.end == 0 && read.max_count == 1);
    CHECK(mem_node_complete(node, index, code));
    if (code == VSR_IO_RETRY) {
        CHECK(status(node).failure.code == VSR_FAILURE_NONE);
        drive(&f, load);
        index = find(node, load);
        CHECK(index != SIZE_MAX);
        const struct vsr_op *again = mem_node_effect(node, index);
        const struct vsr_store_read *reissued = again->data;
        CHECK(again->id != first && reissued->type == type);
        CHECK(reissued->sequence == read.sequence);
        CHECK(mem_node_complete(node, index, VSR_IO_OK));
        drive(&f, none);
        CHECK(reply_for(node, 1) != NULL);
        CHECK(reply_for(node, 1)->status == VSR_REPLY_OK);
        CHECK(status(node).applied == 1);
    } else {
        check_failed(node, VSR_FAILURE_STORAGE, first, VSR_OP_LOAD, code);
        stop_and_drain(&f, node);
    }
    finish(&f);
}

/* "structurally valid but inconsistent storage/application results fence
 * the instance": a completed client record beyond commitment, or a retained
 * request entry beyond the log end, cannot come from this replica's store. */
static void route_load_inconsistent(uint32_t type)
{
    const struct hold load = {VSR_OP_LOAD, type};
    struct fixture f = requesting();
    struct mem_node *node = f.nodes[0];
    drive(&f, load);
    const size_t index = find(node, load);
    CHECK(index != SIZE_MAX);
    const struct vsr_op *op = mem_node_effect(node, index);
    const struct vsr_store_read *read = op->data;
    const uint64_t id = op->id;
    const struct vsr_span span = {"failure-command", 15};
    const struct vsr_blob command = {&span, 15, 1, 0};
    const struct vsr_client_record record = {
        {{700, 1}, 1}, 7, {{NULL, 0, 0, 0}, 0, 0}};
    const struct vsr_entry entry = {
        5, 0, 0, {{700, 1}, 1}, VSR_REQUEST_COMMAND, 0, &command};
    const struct vsr_loaded loaded = {
        type == VSR_LOAD_CLIENT ? (const void *)&record : (const void *)&entry,
        read->sequence, 0, 1, 0};
    CHECK(mem_node_notify(node, index, VSR_IO_OK, &loaded).consumed == 1);
    check_failed(node, VSR_FAILURE_STORAGE, id, VSR_OP_LOAD, VSR_IO_OK);
    stop_and_drain(&f, node);
    finish(&f);
}

/* "SEND | Retry/coalesce protocol work": failed sends never fence and never
 * count as delivery; the protocol keeps retrying and the command still
 * commits on every member once sends succeed again. */
static void send_failures(int code)
{
    const struct hold sends = {VSR_OP_SEND, UINT32_MAX};
    struct fixture f = create(3, VSR_START_NEW);
    drive(&f, none);
    for (size_t i = 0; i < 3; i++)
        CHECK(status(f.nodes[i]).state == VSR_STATE_NORMAL);
    submit(f.nodes[0], 700, 1, 1);
    /* Fail every send until the local append is durable, then the next 32. */
    drive(&f, sends);
    CHECK(status(f.nodes[0]).committed == 0 &&
          mem_cluster_messages(f.cluster) == 0);
    drive_with(&f, none, code, 32);
    for (size_t i = 0; i < 3; i++)
        CHECK(status(f.nodes[i]).failure.code == VSR_FAILURE_NONE);
    for (uint64_t now = 10; now <= 100 && status(f.nodes[2]).committed == 0;
         now += 10) {
        time_all(&f, now);
        drive(&f, none);
    }
    for (size_t i = 0; i < 3; i++) {
        CHECK(status(f.nodes[i]).failure.code == VSR_FAILURE_NONE);
        CHECK(status(f.nodes[i]).committed == 1);
    }
    CHECK(reply_for(f.nodes[0], 1) != NULL);
    CHECK(reply_for(f.nodes[0], 1)->status == VSR_REPLY_OK);
    finish(&f);
}

/* "REPLY / READ_READY | Abandon local delivery; client may retry": the
 * failed reply is never delivered, nothing is fenced, and a retry of the
 * same request is answered from the completed record. */
static void reply_failure(int code)
{
    const struct hold reply = {VSR_OP_REPLY, UINT32_MAX};
    struct fixture f = requesting();
    struct mem_node *node = f.nodes[0];
    drive(&f, reply);
    const size_t index = find(node, reply);
    CHECK(index != SIZE_MAX);
    CHECK(mem_node_effect(node, index)->arg == 1);
    CHECK(mem_node_complete(node, index, code));
    drive(&f, none);
    CHECK(status(node).failure.code == VSR_FAILURE_NONE);
    CHECK(status(node).applied == 1);
    CHECK(mem_node_replies(node) == 0);
    submit(node, 700, 1, 2);
    drive(&f, none);
    const struct vsr_reply *answer = reply_for(node, 2);
    CHECK(answer != NULL && answer->status == VSR_REPLY_OK);
    CHECK((answer->flags & VSR_REPLY_EXECUTED) != 0 && answer->op == 1);
    CHECK(status(node).applied == 1);
    finish(&f);
}

static void read(struct mem_node *node, uint64_t cookie)
{
    const struct vsr_read_barrier barrier = {1, VSR_NO_DEADLINE,
                                             VSR_READ_CAUSAL, 0};
    const struct vsr_event event = {VSR_EVENT_READ, 0, cookie, &barrier, 1};
    CHECK(mem_node_event(node, &event).consumed == 1);
}

/* "Failed read delivery abandons the local route": a failed fence releases
 * the application without fencing the replica, and a later read is served. */
static void read_ready_failure(int code)
{
    const struct hold fence = {VSR_OP_READ_READY, UINT32_MAX};
    struct fixture f = requesting();
    struct mem_node *node = f.nodes[0];
    drive(&f, none);
    CHECK(status(node).applied == 1);
    read(node, 19);
    drive(&f, fence);
    size_t index = find(node, fence);
    CHECK(index != SIZE_MAX);
    CHECK(mem_node_complete(node, index, code));
    drive(&f, none);
    CHECK(status(node).failure.code == VSR_FAILURE_NONE);
    CHECK(mem_node_reads(node) == 0);
    submit(node, 700, 2, 2);
    drive(&f, none);
    CHECK(status(node).applied == 2);
    read(node, 20);
    drive(&f, fence);
    index = find(node, fence);
    CHECK(index != SIZE_MAX);
    const struct vsr_read_fence *ready = mem_node_effect(node, index)->data;
    CHECK(ready->cookie == 20 && ready->applied == 2);
    CHECK(mem_node_complete(node, index, VSR_IO_OK));
    drive(&f, none);
    CHECK(mem_node_reads(node) == 1);
    CHECK(status(node).failure.code == VSR_FAILURE_NONE);
    finish(&f);
}

/* "RECLAIM / DROP | Retry cleanup": any non-corrupt failure leaves the
 * retention obligation in place and the hint is issued again. */
static void reclaim_retry(int code)
{
    const struct hold reclaim = {VSR_OP_RECLAIM, UINT32_MAX};
    struct fixture f = requesting();
    struct mem_node *node = f.nodes[0];
    drive(&f, reclaim);
    size_t index = find(node, reclaim);
    CHECK(index != SIZE_MAX);
    const struct vsr_op first = *mem_node_effect(node, index);
    CHECK(mem_node_complete(node, index, code));
    CHECK(status(node).failure.code == VSR_FAILURE_NONE);
    drive(&f, reclaim);
    index = find(node, reclaim);
    CHECK(index != SIZE_MAX);
    const struct vsr_op *again = mem_node_effect(node, index);
    CHECK(again->id != first.id && again->arg >= first.arg);
    CHECK(mem_node_complete(node, index, VSR_IO_OK));
    drive(&f, none);
    CHECK(status(node).state == VSR_STATE_NORMAL);
    CHECK(status(node).failure.code == VSR_FAILURE_NONE);
    finish(&f);
}

/* A replaced local image is dropped; the drop is retried after retry_ns. */
static void drop_retry(int code)
{
    const struct hold drop = {VSR_OP_SNAPSHOT_DROP, UINT32_MAX};
    struct fixture f = requesting();
    struct mem_node *node = f.nodes[0];
    drive(&f, none);
    hint(node);
    drive(&f, none);
    CHECK(status(node).checkpoint_op == 1);
    submit(node, 700, 2, 2);
    drive(&f, none);
    hint(node);
    drive(&f, drop);
    size_t index = find(node, drop);
    CHECK(index != SIZE_MAX);
    const uint64_t first = mem_node_effect(node, index)->id;
    CHECK(mem_node_complete(node, index, code));
    drive(&f, drop);
    CHECK(status(node).failure.code == VSR_FAILURE_NONE);
    CHECK(status(node).checkpoint_op == 2);
    CHECK(find(node, drop) == SIZE_MAX);
    time_all(&f, 5);
    drive(&f, drop);
    index = find(node, drop);
    CHECK(index != SIZE_MAX && mem_node_effect(node, index)->id != first);
    CHECK(mem_node_complete(node, index, VSR_IO_OK));
    drive(&f, none);
    CHECK(status(node).failure.code == VSR_FAILURE_NONE);
    finish(&f);
}

/* "FETCH | Retry unavailability or discover another valid offer": a
 * recovering member whose snapshot fetch fails with NOT_FOUND or RETRY
 * restarts discovery, fetches again and still rebuilds the full state. */
static void fetch_unavailable_rediscovers(void)
{
    const struct hold fetch = {VSR_OP_SNAPSHOT_FETCH, UINT32_MAX};
    const struct vsr_membership membership = membership_of(3);
    struct fixture f = {.cluster = mem_cluster_create(), .count = 2};
    for (size_t i = 0; i < 2; i++) {
        const struct vsr_options options = options_for(i + 1, &membership);
        f.nodes[i] = mem_cluster_add(f.cluster, &options);
        CHECK(mem_node_time(f.nodes[i], 0).consumed == 1);
    }
    drive(&f, none);
    for (uint64_t i = 1; i <= 2; i++) {
        submit(f.nodes[0], 700, i, i);
        drive(&f, none);
    }
    time_all(&f, 10);
    drive(&f, none);
    hint(f.nodes[0]);
    hint(f.nodes[1]);
    drive(&f, none);
    CHECK(status(f.nodes[0]).checkpoint_op == 2);
    CHECK(status(f.nodes[1]).checkpoint_op == 2);
    {
        struct vsr_options options = options_for(3, &membership);
        options.start_mode = VSR_START_RECOVER;
        f.nodes[2] = mem_cluster_add(f.cluster, &options);
        f.count = 3;
        CHECK(mem_node_time(f.nodes[2], 0).consumed == 1);
    }
    struct mem_node *node = f.nodes[2];
    const int codes[] = {VSR_IO_NOT_FOUND, VSR_IO_RETRY};
    uint64_t previous = 0;
    for (size_t i = 0; i < 2; i++) {
        drive(&f, fetch);
        const size_t index = find(node, fetch);
        CHECK(index != SIZE_MAX);
        const struct vsr_op *op = mem_node_effect(node, index);
        const struct vsr_snapshot_task *task = op->data;
        CHECK(op->id != previous && task->checkpoint->op == 2);
        previous = op->id;
        CHECK(mem_node_complete(node, index, codes[i]));
        CHECK(status(node).failure.code == VSR_FAILURE_NONE);
        CHECK(status(node).state != VSR_STATE_NORMAL);
    }
    for (uint64_t now = 10; now <= 200 && find(node, fetch) == SIZE_MAX;
         now += 5) {
        drive(&f, fetch);
        if (find(node, fetch) == SIZE_MAX)
            time_all(&f, now);
    }
    const size_t index = find(node, fetch);
    CHECK(index != SIZE_MAX && mem_node_effect(node, index)->id != previous);
    CHECK(mem_node_complete(node, index, VSR_IO_OK));
    drive(&f, none);
    CHECK(status(node).failure.code == VSR_FAILURE_NONE);
    CHECK(status(node).state == VSR_STATE_NORMAL);
    CHECK(status(node).applied == 2);
    CHECK(mem_node_checksum(node) == mem_node_checksum(f.nodes[0]));
    finish(&f);
}

static bool selected(int argc, char **argv, const char *name)
{
    if (argc == 1 || strcmp(argv[1], name) == 0) {
        fprintf(stderr, "failures: %s\n", name);
        return true;
    }
    return false;
}

int main(int argc, char **argv)
{
    CHECK(argc <= 2);
    if (selected(argc, argv, "recovery-retry"))
        recovery_load_retry();
    if (selected(argc, argv, "recovery-absent"))
        recovery_absent_without_quorum();
    if (selected(argc, argv, "identity")) {
        for (int which = NEW_ON_EXISTING_STORE; which <= SEQUENCE_EXHAUSTED;
             which++)
            recovery_identity((enum identity_case)which);
    }
    if (selected(argc, argv, "log")) {
        for (int code = VSR_IO_RETRY; code <= VSR_IO_CANCELLED; code++)
            log_load_status(code);
        log_load_inconsistent();
    }
    if (selected(argc, argv, "route")) {
        for (int code = VSR_IO_RETRY; code <= VSR_IO_CANCELLED; code++) {
            route_load_status(VSR_LOAD_CLIENT, code);
            route_load_status(VSR_LOAD_REQUEST, code);
        }
        route_load_inconsistent(VSR_LOAD_CLIENT);
        route_load_inconsistent(VSR_LOAD_REQUEST);
    }
    if (selected(argc, argv, "send")) {
        send_failures(VSR_IO_RETRY);
        send_failures(VSR_IO_FAILED);
        send_failures(VSR_IO_CANCELLED);
    }
    if (selected(argc, argv, "reply")) {
        reply_failure(VSR_IO_FAILED);
        reply_failure(VSR_IO_CANCELLED);
    }
    if (selected(argc, argv, "read")) {
        read_ready_failure(VSR_IO_FAILED);
        read_ready_failure(VSR_IO_CANCELLED);
    }
    if (selected(argc, argv, "reclaim")) {
        reclaim_retry(VSR_IO_RETRY);
        reclaim_retry(VSR_IO_FAILED);
        reclaim_retry(VSR_IO_CANCELLED);
    }
    if (selected(argc, argv, "drop")) {
        drop_retry(VSR_IO_RETRY);
        drop_retry(VSR_IO_FAILED);
    }
    if (selected(argc, argv, "fetch"))
        fetch_unavailable_rediscovers();
    return 0;
}
