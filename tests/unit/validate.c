#include "config.h"

#include "lib/check.h"
#include "validate.h"

#include <stddef.h>
#include <stdint.h>
#include <string.h>

struct fixture {
    struct vsr_member members[3];
    struct vsr_membership membership;
    struct vsr_epoch epoch;
    struct vsr_options options;
    unsigned char bytes[8];
    struct vsr_span spans[2];
    struct vsr_blob blob;
    struct vsr_request request;
    struct vsr_entry entries[2];
    struct vsr_prepare prepare;
    struct vsr_checkpoint checkpoint;
    struct vsr_log_state state;
    struct vsr_nonce nonce;
    struct vsr_recovered recovered;
};

static void setup(struct fixture *f)
{
    memset(f, 0, sizeof(*f));
    f->members[0] = (struct vsr_member){1, VSR_MEMBER_FULL, 0};
    f->members[1] = (struct vsr_member){2, VSR_MEMBER_FULL, 0};
    f->members[2] = (struct vsr_member){UINT64_MAX, VSR_MEMBER_WITNESS, 0};
    f->membership = (struct vsr_membership){0, f->members, 3, 1};
    f->epoch.current = &f->membership;
    f->options.cluster.lo = 1;
    f->options.incarnation.lo = 2;
    f->options.replica = 1;
    f->options.seed = &f->membership;
    f->options.limits = (struct vsr_limits){
        .members = 5,
        .operations = 32,
        .input_leases = 32,
        .pending_requests = 8,
        .pending_reads = 8,
        .transfers = 4,
        .log_cache_entries = 32,
        .client_cache_entries = 16,
        .batch_entries = 4,
        .spans_per_blob = 4,
        .work_per_step = 64,
        .command_bytes = 16,
        .result_bytes = 16,
        .manifest_bytes = 16,
        .message_bytes = 64,
        .pinned_payload_bytes = 256,
    };
    f->options.heartbeat_ns = 10;
    f->options.view_timeout_ns = 100;
    f->options.retry_ns = 10;
    f->options.transfer_timeout_ns = 1000;
    f->spans[0] = (struct vsr_span){f->bytes, 3};
    f->spans[1] = (struct vsr_span){f->bytes + 3, 2};
    f->blob = (struct vsr_blob){f->spans, 5, 2, 0};
    f->request.id = (struct vsr_request_id){{0, 3}, 1};
    f->request.body = &f->blob;
    f->entries[0] =
        (struct vsr_entry){.op = 1, .request = f->request.id, .body = &f->blob};
    f->entries[1] = f->entries[0];
    f->entries[1].op = 2;
    f->entries[1].request.number = 2;
    f->prepare.batch = (struct vsr_entries){f->entries, 2, 0};
    f->prepare.committed = 1;
    f->checkpoint.id.lo = 4;
    f->checkpoint.epoch = &f->epoch;
    f->checkpoint.manifest = f->blob;
    f->state = (struct vsr_log_state){
        .revision = {{0, 5}, 1},
        .committed = 2,
        .log_begin = 1,
        .log_end = 3,
        .epoch = &f->epoch,
        .entries = f->prepare.batch,
    };
    f->nonce = (struct vsr_nonce){{0, 6}, 1};
    f->recovered = (struct vsr_recovered){
        .identity = {f->options.cluster, 1, VSR_DURABLE, 0},
        .sequence = 1,
        .log_begin = 1,
        .log_end = 3,
        .hard = {.committed = 2, .epoch = &f->epoch, .role = VSR_MEMBER_FULL},
    };
}

static struct vsr_validation context(const struct fixture *f)
{
    return (struct vsr_validation){.limits = &f->options.limits};
}

static int event_result(const struct fixture *f, const struct vsr_event *event,
                        const struct vsr_op *operation, uint64_t *bytes)
{
    struct vsr_validation v = context(f);
    int result = vsr_validate_event(&v, event, operation);
    CHECK(result != VSR_OK || v.failure_code == VSR_FAILURE_NONE);
    if (bytes != NULL && result == VSR_OK) {
        *bytes = v.payload_bytes;
    }
    return result;
}

static void completion_failure(const struct fixture *f,
                               const struct vsr_event *event,
                               const struct vsr_op *operation, uint32_t failure,
                               uint64_t bytes)
{
    struct vsr_validation v = context(f);
    CHECK(vsr_validate_event(&v, event, operation) == VSR_OK);
    CHECK(v.failure_code == failure);
    CHECK(v.payload_bytes == bytes);
}

static int message_result(const struct fixture *f,
                          const struct vsr_message *message)
{
    struct vsr_event event = {VSR_EVENT_MESSAGE, 0, 0, message, UINT64_MAX};
    return event_result(f, &event, NULL, NULL);
}

static void test_options(void)
{
    struct fixture f;
    struct vsr_options changed;
    const size_t positive32[] = {
        offsetof(struct vsr_limits, members),
        offsetof(struct vsr_limits, operations),
        offsetof(struct vsr_limits, input_leases),
        offsetof(struct vsr_limits, pending_requests),
        offsetof(struct vsr_limits, pending_reads),
        offsetof(struct vsr_limits, transfers),
        offsetof(struct vsr_limits, log_cache_entries),
        offsetof(struct vsr_limits, client_cache_entries),
        offsetof(struct vsr_limits, batch_entries),
        offsetof(struct vsr_limits, spans_per_blob),
        offsetof(struct vsr_limits, work_per_step),
    };
    const size_t positive64[] = {
        offsetof(struct vsr_limits, command_bytes),
        offsetof(struct vsr_limits, result_bytes),
        offsetof(struct vsr_limits, manifest_bytes),
        offsetof(struct vsr_limits, message_bytes),
        offsetof(struct vsr_limits, pinned_payload_bytes),
    };

    setup(&f);
    CHECK(vsr_validate_options(&f.options) == VSR_OK);
    CHECK(vsr_validate_options(NULL) == VSR_EINVAL);
    for (size_t i = 0; i < sizeof(positive32) / sizeof(positive32[0]); ++i) {
        uint32_t zero = 0;
        changed = f.options;
        memcpy((unsigned char *)&changed.limits + positive32[i], &zero,
               sizeof(zero));
        CHECK(vsr_validate_options(&changed) == VSR_EINVAL);
    }
    for (size_t i = 0; i < sizeof(positive64) / sizeof(positive64[0]); ++i) {
        uint64_t zero = 0;
        changed = f.options;
        memcpy((unsigned char *)&changed.limits + positive64[i], &zero,
               sizeof(zero));
        CHECK(vsr_validate_options(&changed) == VSR_EINVAL);
    }

#define BAD_OPTION(field, value, error)                                        \
    do {                                                                       \
        changed = f.options;                                                   \
        changed.field = (value);                                               \
        CHECK(vsr_validate_options(&changed) == (error));                      \
    } while (0)
    BAD_OPTION(cluster.lo, 0, VSR_EINVAL);
    BAD_OPTION(incarnation.lo, 0, VSR_EINVAL);
    BAD_OPTION(replica, 0, VSR_EINVAL);
    BAD_OPTION(replica, 99, VSR_EINVAL);
    BAD_OPTION(seed, NULL, VSR_EINVAL);
    BAD_OPTION(heartbeat_ns, 0, VSR_EINVAL);
    BAD_OPTION(view_timeout_ns, f.options.heartbeat_ns, VSR_EINVAL);
    BAD_OPTION(view_timeout_ns, UINT64_MAX, VSR_EINVAL);
    BAD_OPTION(retry_ns, UINT64_MAX, VSR_EINVAL);
    BAD_OPTION(transfer_timeout_ns, 0, VSR_EINVAL);
    BAD_OPTION(batch_delay_ns, UINT64_MAX, VSR_EINVAL);
    BAD_OPTION(checkpoint_interval, UINT64_MAX, VSR_EINVAL);
    BAD_OPTION(start_mode, UINT32_MAX, VSR_EINVAL);
    BAD_OPTION(durability, UINT32_MAX, VSR_EINVAL);
    BAD_OPTION(join_role, VSR_MEMBER_FULL, VSR_EINVAL);
    BAD_OPTION(cache_line_bytes, 3, VSR_EINVAL);
    BAD_OPTION(reserved, 1, VSR_EINVAL);
    BAD_OPTION(limits.batch_entries, 33, VSR_ELIMIT);
    BAD_OPTION(limits.command_bytes, UINT64_MAX, VSR_ELIMIT);
    BAD_OPTION(limits.result_bytes, UINT64_MAX / 4 + 1, VSR_ELIMIT);
    BAD_OPTION(limits.message_bytes, 31, VSR_ELIMIT);
    BAD_OPTION(limits.pinned_payload_bytes, 47, VSR_ELIMIT);
#undef BAD_OPTION

    changed = f.options;
    changed.limits.pinned_payload_bytes = 48;
    changed.cache_line_bytes = 128;
    CHECK(vsr_validate_options(&changed) == VSR_OK);
    changed.start_mode = VSR_START_JOIN;
    changed.replica = 99;
    changed.join_role = VSR_MEMBER_WITNESS;
    CHECK(vsr_validate_options(&changed) == VSR_OK);
    changed.replica = 1;
    CHECK(vsr_validate_options(&changed) == VSR_EINVAL);
    changed.start_mode = VSR_START_RECOVER;
    changed.join_role = VSR_MEMBER_NONE;
    changed.replica = 99;
    CHECK(vsr_validate_options(&changed) == VSR_OK);
}

static void test_membership_epoch(void)
{
    struct fixture f;
    struct vsr_validation v;
    struct vsr_membership next;

    setup(&f);
    v = context(&f);
    CHECK(vsr_validate_membership(&v, &f.membership) == VSR_OK);
    f.membership.faults = UINT32_MAX;
    CHECK(vsr_validate_membership(&v, &f.membership) == VSR_EINVAL);
    f.membership.faults = 1;
    f.membership.count = 6;
    CHECK(vsr_validate_membership(&v, &f.membership) == VSR_ELIMIT);
    f.membership.count = 3;
    f.members[1].id = f.members[0].id;
    CHECK(vsr_validate_membership(&v, &f.membership) == VSR_EINVAL);
    f.members[1].id = 2;
    f.members[1].role = VSR_MEMBER_WITNESS;
    CHECK(vsr_validate_membership(&v, &f.membership) == VSR_EINVAL);
    f.members[1].role = VSR_MEMBER_FULL;
    f.members[2].reserved = 1;
    CHECK(vsr_validate_membership(&v, &f.membership) == VSR_EINVAL);
    f.members[2].reserved = 0;
    f.membership.count = 1;
    f.membership.faults = 0;
    CHECK(vsr_validate_membership(&v, &f.membership) == VSR_OK);
    CHECK(vsr_validate_epoch(&v, &f.epoch) == VSR_OK);
    f.epoch.phase = VSR_EPOCH_INSTALLED;
    CHECK(vsr_validate_epoch(&v, &f.epoch) == VSR_EINVAL);
    next = f.membership;
    next.epoch = 1;
    f.epoch.current = &next;
    f.epoch.previous = &f.membership;
    f.epoch.boundary = 2;
    CHECK(vsr_validate_epoch(&v, &f.epoch) == VSR_OK);
    f.epoch.previous = &next;
    CHECK(vsr_validate_epoch(&v, &f.epoch) == VSR_EINVAL);
    f.epoch.previous = &f.membership;
    f.epoch.boundary = UINT64_MAX;
    CHECK(vsr_validate_epoch(&v, &f.epoch) == VSR_EINVAL);
}

static void test_blobs_requests_and_overlap(void)
{
    struct fixture f;
    struct vsr_validation v;
    struct vsr_blob empty = {0};
    struct vsr_check_epoch check = {1};
    struct vsr_event event;
    uint64_t bytes = 0;
    _Alignas(
        struct vsr_blob) unsigned char misaligned[sizeof(struct vsr_blob) + 1];

    setup(&f);
    v = context(&f);
    CHECK(vsr_validate_blob(&v, &empty, 0) == VSR_OK);
    CHECK(vsr_validate_blob(&v, &f.blob, 5) == VSR_OK);
    CHECK(v.payload_bytes == 5);
    CHECK(vsr_validate_blob(&v, &f.blob, 5) == VSR_OK);
    CHECK(v.payload_bytes == 10); /* Shared bytes count once per occurrence. */
    CHECK(vsr_validate_blob(&v, &f.blob, 4) == VSR_ELIMIT);
    f.blob.reserved = 1;
    CHECK(vsr_validate_blob(&v, &f.blob, 5) == VSR_EINVAL);
    f.blob.reserved = 0;
    f.blob.size = 4;
    CHECK(vsr_validate_blob(&v, &f.blob, 5) == VSR_EINVAL);
    f.blob.size = 5;
    f.spans[0].size = 0;
    CHECK(vsr_validate_blob(&v, &f.blob, 5) == VSR_EINVAL);
    f.spans[0].size = 3;
    f.spans[0].data = NULL;
    CHECK(vsr_validate_blob(&v, &f.blob, 5) == VSR_EINVAL);
    f.spans[0].data = f.bytes;
    empty.spans = f.spans;
    CHECK(vsr_validate_blob(&v, &empty, 0) == VSR_EINVAL);
    v.payload_bytes = UINT64_MAX - 4;
    CHECK(vsr_validate_blob(&v, &f.blob, 5) == VSR_ELIMIT);
    v = context(&f);
    CHECK(vsr_validate_blob(&v, (const void *)(misaligned + 1), 5) ==
          VSR_EINVAL);

    /* Every level of the leased graph is excluded from the arena. */
    v.arena = &f.request;
    v.arena_size = sizeof(f.request);
    CHECK(vsr_validate_request(&v, &f.request) == VSR_EINVAL);
    v.arena = &f.blob;
    v.arena_size = sizeof(f.blob);
    CHECK(vsr_validate_request(&v, &f.request) == VSR_EINVAL);
    v.arena = f.spans;
    v.arena_size = sizeof(f.spans);
    CHECK(vsr_validate_request(&v, &f.request) == VSR_EINVAL);
    v.arena = f.bytes + 2;
    v.arena_size = 1;
    CHECK(vsr_validate_request(&v, &f.request) == VSR_EINVAL);
    v.arena = f.bytes + 5; /* Half-open ranges that merely touch are legal. */
    CHECK(vsr_validate_request(&v, &f.request) == VSR_OK);

    event = (struct vsr_event){VSR_EVENT_REQUEST, 0, UINT64_MAX, &f.request, 1};
    CHECK(event_result(&f, &event, NULL, &bytes) == VSR_OK && bytes == 5);
    f.request.type = VSR_REQUEST_CHECK_EPOCH;
    f.request.body = &check;
    CHECK(event_result(&f, &event, NULL, NULL) == VSR_OK);
    /* Future client targets are rejected by an ordinary protocol reply. */
    f.entries[0].type = VSR_REQUEST_CHECK_EPOCH;
    f.entries[0].body = &check;
    v = context(&f);
    CHECK(vsr_validate_entry(&v, &f.entries[0]) == VSR_EINVAL);
    check.epoch = 0;
    CHECK(vsr_validate_entry(&v, &f.entries[0]) == VSR_OK);
    f.request.type = VSR_REQUEST_NOOP;
    f.request.body = NULL;
    f.request.id = (struct vsr_request_id){0};
    CHECK(event_result(&f, &event, NULL, NULL) == VSR_EINVAL);
    f.entries[0].type = VSR_REQUEST_NOOP;
    f.entries[0].body = NULL;
    f.entries[0].request = (struct vsr_request_id){0};
    CHECK(vsr_validate_entry(&v, &f.entries[0]) == VSR_OK);
    f.entries[0].op = UINT64_MAX;
    CHECK(vsr_validate_entry(&v, &f.entries[0]) == VSR_EINVAL);
}

static void test_message_types(void)
{
    struct fixture f;
    struct vsr_message message;
    struct vsr_recovery recovery;
    struct vsr_fetch fetch;
    struct vsr_state_chunk chunk;
    struct vsr_membership next;
    struct vsr_epoch epoch;

    setup(&f);
    recovery = (struct vsr_recovery){f.nonce, NULL};
    fetch =
        (struct vsr_fetch){.nonce = f.nonce, .max_bytes = 32, .max_entries = 4};
    chunk = (struct vsr_state_chunk){f.nonce, f.state, 1, 3};
    next = f.membership;
    next.epoch = 1;
    epoch =
        (struct vsr_epoch){&next, &f.membership, 2, VSR_EPOCH_TRANSFERRING, 0};

    for (uint32_t type = VSR_MSG_PREPARE; type <= VSR_MSG_READ_ACK; ++type) {
        message = (struct vsr_message){
            .cluster = f.options.cluster, .from = 2, .type = type};
        switch (type) {
        case VSR_MSG_PREPARE:
            message.body = &f.prepare;
            message.number = 2;
            break;
        case VSR_MSG_DO_VIEW_CHANGE:
        case VSR_MSG_START_VIEW:
            message.body = &f.state;
            message.number = 2;
            break;
        case VSR_MSG_RECOVERY:
        case VSR_MSG_RECOVERY_RESPONSE:
            message.body = &recovery;
            break;
        case VSR_MSG_GET_STATE:
            fetch.revision = (struct vsr_revision){0};
            fetch.first = fetch.end = 0;
            message.body = &fetch;
            break;
        case VSR_MSG_GET_LOG:
            fetch.revision = f.state.revision;
            fetch.first = 1;
            fetch.end = 3;
            message.body = &fetch;
            break;
        case VSR_MSG_NEW_STATE:
        case VSR_MSG_LOG:
            message.body = &chunk;
            message.number = 2;
            break;
        case VSR_MSG_STATE_UNAVAILABLE:
            chunk.next = chunk.first;
            chunk.state.entries = (struct vsr_entries){0};
            message.body = &chunk;
            message.number = 2;
            break;
        case VSR_MSG_START_EPOCH:
        case VSR_MSG_NEW_EPOCH:
            message.body = &epoch;
            message.number = 2;
            break;
        case VSR_MSG_EPOCH_STARTED:
            message.number = 2;
            break;
        case VSR_MSG_CHECKPOINT:
            message.body = &f.checkpoint;
            break;
        case VSR_MSG_READ_PROBE:
        case VSR_MSG_READ_ACK:
            message.body = &f.nonce;
            break;
        default:
            break;
        }
        CHECK(message_result(&f, &message) == VSR_OK);
        message.flags = 1;
        CHECK(message_result(&f, &message) == VSR_EINVAL);
        message.flags = 0;
        message.from = 0;
        CHECK(message_result(&f, &message) == VSR_EINVAL);
        message.from = UINT64_MAX;
        message.view = UINT64_MAX;
        CHECK(message_result(&f, &message) == VSR_EINVAL);
    }
    message.type = UINT32_MAX;
    message.view = 0;
    CHECK(message_result(&f, &message) == VSR_EINVAL);
}

static void test_log_and_message_bounds(void)
{
    struct fixture f;
    struct vsr_message message;
    struct vsr_validation v;
    struct vsr_fetch fetch;
    struct vsr_state_chunk chunk;

    setup(&f);
    message = (struct vsr_message){.cluster = f.options.cluster,
                                   .from = 2,
                                   .type = VSR_MSG_PREPARE,
                                   .number = 2,
                                   .body = &f.prepare};
    f.prepare.committed = 3;
    CHECK(message_result(&f, &message) == VSR_EINVAL);
    f.prepare.committed = 0;
    f.entries[1].op = 3;
    CHECK(message_result(&f, &message) == VSR_EINVAL);
    f.entries[1].op = 2;
    f.options.limits.message_bytes = 9;
    CHECK(message_result(&f, &message) == VSR_ELIMIT);
    f.options.limits.message_bytes = 64;
    message.type = VSR_MSG_START_VIEW;
    message.body = &f.state;
    CHECK(message_result(&f, &message) == VSR_OK);
    f.state.log_begin = 2;
    CHECK(message_result(&f, &message) == VSR_EINVAL);
    f.state.checkpoint = &f.checkpoint;
    f.checkpoint.op = 1;
    f.state.entries.entries = f.entries + 1;
    f.state.entries.count = 1;
    CHECK(message_result(&f, &message) == VSR_OK);
    f.checkpoint.op = 3;
    CHECK(message_result(&f, &message) == VSR_EINVAL);
    f.checkpoint.op = 1;
    f.state.committed = 3;
    CHECK(message_result(&f, &message) == VSR_EINVAL);

    setup(&f);
    v = context(&f);
    f.recovered.log_begin = 2;
    CHECK(vsr_validate_recovered(&v, &f.recovered) == VSR_OK);
    CHECK(v.failure_code == VSR_FAILURE_STORAGE);
    v = context(&f);
    f.recovered.checkpoint = &f.checkpoint;
    f.checkpoint.op = 1;
    CHECK(vsr_validate_recovered(&v, &f.recovered) == VSR_OK);
    CHECK(v.failure_code == VSR_FAILURE_NONE);
    f.recovered.hard.view = 2;
    CHECK(vsr_validate_recovered(&v, &f.recovered) == VSR_OK);
    CHECK(v.failure_code == VSR_FAILURE_STORAGE);
    v = context(&f);
    f.recovered.hard.state = VSR_HARD_VIEW_CHANGE;
    CHECK(vsr_validate_recovered(&v, &f.recovered) == VSR_OK);
    f.recovered.sequence = UINT64_MAX;
    CHECK(vsr_validate_recovered(&v, &f.recovered) == VSR_EINVAL);

    fetch = (struct vsr_fetch){.nonce = f.nonce,
                               .revision = f.state.revision,
                               .first = UINT64_MAX - 1,
                               .end = UINT64_MAX,
                               .max_bytes = 32,
                               .max_entries = 1};
    message.type = VSR_MSG_GET_LOG;
    message.number = 0;
    message.body = &fetch;
    CHECK(message_result(&f, &message) == VSR_OK);
    fetch.max_bytes = 31;
    CHECK(message_result(&f, &message) == VSR_ELIMIT);
    fetch.max_bytes = 32;
    fetch.revision = (struct vsr_revision){0};
    fetch.first = fetch.end = 0;
    CHECK(message_result(&f, &message) == VSR_EINVAL);
    message.type = VSR_MSG_GET_STATE;
    CHECK(message_result(&f, &message) == VSR_OK);

    chunk = (struct vsr_state_chunk){f.nonce, f.state, 1, 3};
    message.type = VSR_MSG_LOG;
    message.number = 2;
    message.body = &chunk;
    CHECK(message_result(&f, &message) == VSR_OK);
    chunk.next = 2;
    CHECK(message_result(&f, &message) == VSR_EINVAL);
    chunk.first = chunk.next = 0;
    chunk.state.entries = (struct vsr_entries){0};
    message.type = VSR_MSG_NEW_STATE;
    CHECK(message_result(&f, &message) == VSR_OK);
}

static void test_event_envelopes(void)
{
    struct fixture f;
    struct vsr_event event;
    struct vsr_read_barrier read = {0, VSR_NO_DEADLINE, VSR_READ_CAUSAL, 0};
    const struct vsr_event valid[] = {
        {VSR_EVENT_TIME, 0, 0, NULL, 0},
        {VSR_EVENT_TIME, 0, UINT64_MAX - 1, NULL, 0},
        {VSR_EVENT_CHECKPOINT, 0, 0, NULL, 0},
        {VSR_EVENT_STOP, 0, 0, NULL, 0},
    };

    setup(&f);
    for (size_t i = 0; i < sizeof(valid) / sizeof(valid[0]); ++i) {
        CHECK(event_result(&f, &valid[i], NULL, NULL) == VSR_OK);
        event = valid[i];
        event.lease = 1;
        CHECK(event_result(&f, &event, NULL, NULL) == VSR_EINVAL);
        event.lease = 0;
        event.status = VSR_IO_RETRY;
        CHECK(event_result(&f, &event, NULL, NULL) == VSR_EINVAL);
    }
    event = (struct vsr_event){VSR_EVENT_TIME, 0, UINT64_MAX, NULL, 0};
    CHECK(event_result(&f, &event, NULL, NULL) == VSR_EINVAL);
    event = (struct vsr_event){VSR_EVENT_REQUEST, 0, 1, &f.request, 0};
    CHECK(event_result(&f, &event, NULL, NULL) == VSR_EINVAL);
    event.lease = 1;
    CHECK(event_result(&f, &event, NULL, NULL) == VSR_OK);
    event.id = 0;
    CHECK(event_result(&f, &event, NULL, NULL) == VSR_EINVAL);
    event = (struct vsr_event){VSR_EVENT_READ, 0, 1, &read, 1};
    CHECK(event_result(&f, &event, NULL, NULL) == VSR_OK);
    read.min_op = UINT64_MAX;
    CHECK(event_result(&f, &event, NULL, NULL) == VSR_EINVAL);
    read.min_op = 0;
    read.consistency = UINT32_MAX;
    CHECK(event_result(&f, &event, NULL, NULL) == VSR_EINVAL);
    event =
        (struct vsr_event){VSR_EVENT_CLIENT_QUERY, 0, 1, &f.options.cluster, 1};
    CHECK(event_result(&f, &event, NULL, NULL) == VSR_OK);
    f.options.cluster.lo = 0;
    CHECK(event_result(&f, &event, NULL, NULL) == VSR_EINVAL);
    event.type = UINT32_MAX;
    CHECK(event_result(&f, &event, NULL, NULL) == VSR_EINVAL);
}

static void test_load_completions(void)
{
    struct fixture f;
    struct vsr_store_read read;
    struct vsr_loaded loaded;
    struct vsr_op operation;
    struct vsr_event event;
    struct vsr_client_record record;
    uint64_t bytes = 0;

    setup(&f);
    read = (struct vsr_store_read){
        .max_bytes = 64, .type = VSR_LOAD_RECOVERY, .max_count = 1};
    loaded = (struct vsr_loaded){&f.recovered, 1, 0, 1, 0};
    operation = (struct vsr_op){VSR_OP_LOAD, 0, 1, &read, 0};
    event = (struct vsr_event){VSR_EVENT_COMPLETE, 0, 1, &loaded, 1};
    CHECK(event_result(&f, &event, &operation, &bytes) == VSR_OK && bytes == 0);
    loaded.sequence = 2;
    completion_failure(&f, &event, &operation, VSR_FAILURE_STORAGE, 0);
    loaded.sequence = 1;
    loaded.next = 1;
    completion_failure(&f, &event, &operation, VSR_FAILURE_STORAGE, 0);

    read = (struct vsr_store_read){.sequence = 3,
                                   .first = 1,
                                   .end = 3,
                                   .max_bytes = 10,
                                   .type = VSR_LOAD_LOG,
                                   .max_count = 2};
    loaded = (struct vsr_loaded){f.entries, 3, 3, 2, 0};
    CHECK(event_result(&f, &event, &operation, &bytes) == VSR_OK &&
          bytes == 10);
    read.max_bytes = 9;
    CHECK(event_result(&f, &event, &operation, NULL) == VSR_ELIMIT);
    read.max_bytes = 10;
    loaded.sequence = 2;
    completion_failure(&f, &event, &operation, VSR_FAILURE_STORAGE, 10);
    loaded.sequence = 3;
    loaded.next = 2;
    completion_failure(&f, &event, &operation, VSR_FAILURE_STORAGE, 10);
    loaded.count = 1;
    CHECK(event_result(&f, &event, &operation, &bytes) == VSR_OK && bytes == 5);
    loaded = (struct vsr_loaded){NULL, 3, 1, 0, 0};
    completion_failure(&f, &event, &operation, VSR_FAILURE_STORAGE, 0);
    read.end = 1;
    CHECK(event_result(&f, &event, &operation, NULL) == VSR_OK);

    read.type = VSR_LOAD_CLIENT;
    read.first = read.end = 0;
    read.client = f.request.id.client;
    read.max_count = 1;
    loaded.next = 0;
    CHECK(event_result(&f, &event, &operation, NULL) == VSR_OK);
    record = (struct vsr_client_record){f.request.id, 1, {f.blob, 17, 0}};
    loaded.items = &record;
    loaded.count = 1;
    CHECK(event_result(&f, &event, &operation, &bytes) == VSR_OK && bytes == 5);
    record.request.client.lo = 999;
    completion_failure(&f, &event, &operation, VSR_FAILURE_STORAGE, 5);
    read.type = VSR_LOAD_REQUEST;
    loaded.items = f.entries;
    CHECK(event_result(&f, &event, &operation, NULL) == VSR_OK);
    read.client.lo = 999;
    completion_failure(&f, &event, &operation, VSR_FAILURE_STORAGE, 5);

    /* A semantic mismatch must not skip validation of later leased objects. */
    f.blob.reserved = 1;
    CHECK(event_result(&f, &event, &operation, NULL) == VSR_EINVAL);
    f.blob.reserved = 0;
    loaded.count = 2;
    CHECK(event_result(&f, &event, &operation, NULL) == VSR_ELIMIT);
    loaded = (struct vsr_loaded){NULL, 1, 0, 0, 0};
    read.type = VSR_LOAD_RECOVERY;
    completion_failure(&f, &event, &operation, VSR_FAILURE_STORAGE, 0);
}

static void test_apply_snapshot_and_plain_completions(void)
{
    struct fixture f;
    struct vsr_value results[3];
    struct vsr_apply apply;
    struct vsr_applied applied;
    struct vsr_op operation;
    struct vsr_event event;
    struct vsr_snapshot_task task;
    struct vsr_checkpoint template;
    uint64_t bytes = 0;

    setup(&f);
    results[0] = (struct vsr_value){f.blob, INT32_MIN, 0};
    results[1] = (struct vsr_value){f.blob, INT32_MAX, 0};
    apply = (struct vsr_apply){f.prepare.batch, 2, 0, 0};
    applied = (struct vsr_applied){results, 2, 0};
    operation = (struct vsr_op){VSR_OP_APPLY, 0, 7, &apply, 0};
    event = (struct vsr_event){VSR_EVENT_COMPLETE, 0, 7, &applied, 1};
    CHECK(event_result(&f, &event, &operation, &bytes) == VSR_OK &&
          bytes == 10);
    applied.count = 1;
    completion_failure(&f, &event, &operation, VSR_FAILURE_APPLICATION, 5);
    results[2] = results[0];
    applied.count = 3;
    completion_failure(&f, &event, &operation, VSR_FAILURE_APPLICATION, 15);
    results[2].reserved = 1;
    CHECK(event_result(&f, &event, &operation, NULL) == VSR_EINVAL);
    results[2].reserved = 0;
    applied.count = 5;
    CHECK(event_result(&f, &event, &operation, NULL) == VSR_ELIMIT);
    applied.count = 0;
    applied.results = NULL;
    completion_failure(&f, &event, &operation, VSR_FAILURE_APPLICATION, 0);
    applied.results = results;
    applied.count = 2;
    f.entries[1].type = VSR_REQUEST_NOOP;
    completion_failure(&f, &event, &operation, VSR_FAILURE_APPLICATION, 10);
    results[1] = (struct vsr_value){0};
    CHECK(event_result(&f, &event, &operation, &bytes) == VSR_OK && bytes == 5);

    template = f.checkpoint;
    template.id = (struct vsr_id){0};
    template.manifest = (struct vsr_blob){0};
    task = (struct vsr_snapshot_task){.checkpoint = &template};
    operation.type = VSR_OP_SNAPSHOT_CAPTURE;
    operation.data = &task;
    event.data = &f.checkpoint;
    CHECK(event_result(&f, &event, &operation, &bytes) == VSR_OK && bytes == 5);
    f.checkpoint.view = 1;
    completion_failure(&f, &event, &operation, VSR_FAILURE_SNAPSHOT, 5);
    f.checkpoint.view = 0;
    operation.type = VSR_OP_SNAPSHOT_FETCH;
    completion_failure(&f, &event, &operation, VSR_FAILURE_SNAPSHOT, 5);
    template.id = f.checkpoint.id;
    CHECK(event_result(&f, &event, &operation, NULL) == VSR_OK);
    /* Local manifests may differ on fetch: only logical snapshot identity is fixed. */
    f.checkpoint.manifest = (struct vsr_blob){0};
    CHECK(event_result(&f, &event, &operation, &bytes) == VSR_OK && bytes == 0);

    for (uint32_t type = VSR_OP_SEND; type < VSR_OP_RELEASE; ++type) {
        operation.type = type;
        for (int32_t status = VSR_IO_RETRY; status <= VSR_IO_CANCELLED;
             ++status) {
            event.status = status;
            event.data = NULL;
            event.lease = 0;
            CHECK(event_result(&f, &event, &operation, NULL) == VSR_OK);
            event.data = &f.blob;
            event.lease = 1;
            CHECK(event_result(&f, &event, &operation, NULL) == VSR_EINVAL);
        }
        if (type != VSR_OP_LOAD && type != VSR_OP_APPLY &&
            type != VSR_OP_SNAPSHOT_CAPTURE && type != VSR_OP_SNAPSHOT_FETCH) {
            event.status = VSR_IO_OK;
            event.data = NULL;
            event.lease = 0;
            CHECK(event_result(&f, &event, &operation, NULL) == VSR_OK);
            event.data = &f.blob;
            event.lease = 1;
            CHECK(event_result(&f, &event, &operation, NULL) == VSR_EINVAL);
        }
    }
    event.data = NULL;
    event.lease = 0;
    operation.type = VSR_OP_SEND;
    event.status = VSR_IO_CANCELLED + 1;
    CHECK(event_result(&f, &event, &operation, NULL) == VSR_EINVAL);
    event.status = -1;
    CHECK(event_result(&f, &event, &operation, NULL) == VSR_EINVAL);
    event.status = VSR_IO_OK;
    event.id = 8;
    CHECK(event_result(&f, &event, &operation, NULL) == VSR_EINVAL);
    event.id = 7;
    CHECK(event_result(&f, &event, NULL, NULL) == VSR_EINVAL);
    operation.type = VSR_OP_RELEASE;
    CHECK(event_result(&f, &event, &operation, NULL) == VSR_EINVAL);
}

static void test_transferring_metadata_precedes_history(void)
{
    struct fixture f;
    setup(&f);
    struct vsr_membership next = f.membership;
    next.epoch = 1;
    struct vsr_epoch epoch = {&next, &f.membership, 9, VSR_EPOCH_TRANSFERRING,
                              0};
    f.recovered.hard.epoch = &epoch;
    const uint32_t allowed[] = {VSR_HARD_RECOVERING, VSR_HARD_TRANSITIONING};
    for (size_t i = 0; i < sizeof(allowed) / sizeof(allowed[0]); i++) {
        struct vsr_validation validation = context(&f);
        f.recovered.hard.state = allowed[i];
        CHECK(vsr_validate_recovered(&validation, &f.recovered) == VSR_OK);
        CHECK(validation.failure_code == VSR_FAILURE_NONE);
    }
    const uint32_t denied[] = {VSR_HARD_NORMAL, VSR_HARD_VIEW_CHANGE,
                               VSR_HARD_RETIRED};
    for (size_t i = 0; i < sizeof(denied) / sizeof(denied[0]); i++) {
        struct vsr_validation validation = context(&f);
        f.recovered.hard.state = denied[i];
        CHECK(vsr_validate_recovered(&validation, &f.recovered) == VSR_OK);
        CHECK(validation.failure_code == VSR_FAILURE_STORAGE);
    }
    f.recovered.hard.state = VSR_HARD_RECOVERING;
    const uint32_t phases[] = {VSR_EPOCH_INSTALLED, VSR_EPOCH_STEADY};
    for (size_t i = 0; i < sizeof(phases) / sizeof(phases[0]); i++) {
        struct vsr_validation validation = context(&f);
        epoch.phase = phases[i];
        CHECK(vsr_validate_recovered(&validation, &f.recovered) == VSR_OK);
        CHECK(validation.failure_code == VSR_FAILURE_STORAGE);
    }
    epoch.phase = VSR_EPOCH_TRANSFERRING;
    f.state.epoch = &epoch;
    struct vsr_message message = {.cluster = f.options.cluster,
                                  .epoch = 1,
                                  .from = 1,
                                  .type = VSR_MSG_START_VIEW,
                                  .number = 2,
                                  .body = &f.state};
    CHECK(message_result(&f, &message) == VSR_EINVAL);
}

int main(void)
{
    test_transferring_metadata_precedes_history();
    test_options();
    test_membership_epoch();
    test_blobs_requests_and_overlap();
    test_message_types();
    test_log_and_message_bounds();
    test_event_envelopes();
    test_load_completions();
    test_apply_snapshot_and_plain_completions();
    return 0;
}
