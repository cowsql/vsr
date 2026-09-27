#include "config.h"

#include "internal.h"
#include "lib/check.h"

#include <stdalign.h>
#include <stdlib.h>
#include <string.h>

/* The runtime is tested against a deliberately small protocol double. The real
 * protocol links through the same hooks; this double exercises ownership and
 * scheduling independently of the consensus state machine. */
struct mock_protocol {
    struct vsr_epoch epoch;
    uint64_t deadline;
    uint32_t requests;
    uint32_t completions;
    uint32_t stops;
};

int vsr_protocol_size(const struct vsr_options *options, size_t *size,
                      size_t *alignment)
{
    (void)options;
    *size = sizeof(struct mock_protocol);
    *alignment = alignof(struct mock_protocol);
    return VSR_OK;
}

void vsr_protocol_init(struct vsr *v, void *memory, size_t size)
{
    CHECK(size == sizeof(struct mock_protocol));
    struct mock_protocol *mock = memory;
    mock->epoch.current = v->options.seed;
    mock->epoch.phase = VSR_EPOCH_STEADY;
    mock->deadline = VSR_NO_DEADLINE;
    v->status.configuration = &mock->epoch;
    v->status.role = VSR_MEMBER_FULL;
    v->status.primary = v->options.replica;
}

int vsr_protocol_start(struct vsr *v)
{
    v->status.state = VSR_STATE_NORMAL;
    vsr_changed(v);
    return VSR_OK;
}

int vsr_protocol_event(struct vsr *v, const struct vsr_event *event,
                       uint32_t lease)
{
    if (event->type != VSR_EVENT_REQUEST) {
        return VSR_OK;
    }
    const struct vsr_request *request = event->data;
    struct vsr_operation *operation =
        vsr_operation_acquire(v, VSR_OP_SEND, 2, 1, 0);
    if (operation == NULL) {
        return VSR_AGAIN;
    }
    struct vsr_entry entry = {
        .op = request->id.number,
        .epoch = 0,
        .view = 0,
        .request = request->id,
        .type = request->type,
        .body = request->body,
    };
    struct vsr_prepare prepare = {
        .batch = {&entry, 1, 0},
        .committed = 0,
    };
    struct vsr_message message = {
        .cluster = v->options.cluster,
        .epoch = 0,
        .view = 0,
        .from = v->options.replica,
        .type = VSR_MSG_PREPARE,
        .number = entry.op,
        .body = &prepare,
    };
    CHECK(vsr_operation_copy(operation, &message) == VSR_OK);
    CHECK(vsr_operation_hold(v, operation, lease));
    vsr_operation_publish(v, operation);
    struct mock_protocol *mock = v->protocol;
    mock->requests++;
    return VSR_OK;
}

void vsr_protocol_complete(struct vsr *v, struct vsr_operation *operation,
                           const struct vsr_event *event, uint32_t lease)
{
    (void)operation;
    (void)event;
    (void)lease;
    struct mock_protocol *mock = v->protocol;
    mock->completions++;
}

bool vsr_protocol_poll(struct vsr *v)
{
    (void)v;
    return false;
}

bool vsr_protocol_relieve_pressure(struct vsr *v)
{
    (void)v;
    return false; /* This isolated runtime model owns no reconstructible cache. */
}

uint64_t vsr_protocol_deadline(const struct vsr *v)
{
    const struct mock_protocol *mock = v->protocol;
    return mock->deadline;
}

void vsr_protocol_stop(struct vsr *v)
{
    struct mock_protocol *mock = v->protocol;
    mock->stops++;
}

struct fixture {
    struct vsr_member member;
    struct vsr_membership membership;
    struct vsr_options options;
    struct vsr_layout layout;
    void *memory;
    struct vsr *v;
};

static void options_init(struct fixture *fixture)
{
    memset(fixture, 0, sizeof(*fixture));
    fixture->member = (struct vsr_member){1, VSR_MEMBER_FULL, 0};
    fixture->membership = (struct vsr_membership){0, &fixture->member, 1, 0};
    fixture->options = (struct vsr_options){
        .cluster = {1, 1},
        .incarnation = {2, 1},
        .replica = 1,
        .seed = &fixture->membership,
        .limits =
            {
                .members = 4,
                .operations = 8,
                .input_leases = 16,
                .pending_requests = 4,
                .pending_reads = 4,
                .transfers = 4,
                .log_cache_entries = 8,
                .client_cache_entries = 8,
                .batch_entries = 4,
                .spans_per_blob = 4,
                .work_per_step = 64,
                .command_bytes = 64,
                .result_bytes = 32,
                .manifest_bytes = 64,
                .message_bytes = 256,
                .pinned_payload_bytes = 2048,
            },
        .heartbeat_ns = 10,
        .view_timeout_ns = 30,
        .retry_ns = 5,
        .transfer_timeout_ns = 100,
        .start_mode = VSR_START_NEW,
        .durability = VSR_DURABLE,
    };
}

static void fixture_start(struct fixture *fixture)
{
    CHECK(vsr_layout(&fixture->options, &fixture->layout) == VSR_OK);
    fixture->memory =
        aligned_alloc(fixture->layout.alignment, fixture->layout.size);
    CHECK(fixture->memory != NULL);
    CHECK(vsr_init(fixture->memory, fixture->layout.size, &fixture->options,
                   &fixture->v) == VSR_OK);
}

static void fixture_stop(struct fixture *fixture)
{
    struct vsr_event stop = {.type = VSR_EVENT_STOP};
    struct vsr_op outputs[32];
    struct vsr_update update = {.ops = outputs, .capacity = 32};
    bool submitted = false;
    for (uint32_t iteration = 0; iteration < 1000; iteration++) {
        int result = vsr_step(fixture->v, submitted ? NULL : &stop, &update);
        CHECK(result >= VSR_OK);
        submitted = submitted || update.consumed != 0;
        for (uint32_t i = 0; i < fixture->v->options.limits.operations; i++) {
            struct vsr_operation *operation = &fixture->v->operations[i];
            if (operation->state == VSR_SLOT_ACTIVE) {
                struct vsr_event complete = {
                    .type = VSR_EVENT_COMPLETE,
                    .status = VSR_IO_CANCELLED,
                    .id = operation->output.id,
                };
                do {
                    result = vsr_step(fixture->v, &complete, &update);
                    CHECK(result >= VSR_OK);
                } while (update.consumed == 0);
            }
        }
        if (fixture->v->status.state == VSR_STATE_STOPPED) {
            break;
        }
    }
    CHECK(fixture->v->status.state == VSR_STATE_STOPPED);
    CHECK(vsr_deinit(fixture->v) == VSR_OK);
    free(fixture->memory);
}

struct request_input {
    unsigned char bytes[4];
    struct vsr_span span;
    struct vsr_blob blob;
    struct vsr_request request;
    struct vsr_event event;
};

static void request_init(struct request_input *input, uint64_t number,
                         uint64_t lease)
{
    memset(input, 0, sizeof(*input));
    input->bytes[0] = (unsigned char)number;
    input->span = (struct vsr_span){input->bytes, sizeof(input->bytes)};
    input->blob = (struct vsr_blob){&input->span, sizeof(input->bytes), 1, 0};
    input->request = (struct vsr_request){
        .id = {{3, 1}, number},
        .epoch = 0,
        .type = VSR_REQUEST_COMMAND,
        .body = &input->blob,
    };
    input->event = (struct vsr_event){
        .type = VSR_EVENT_REQUEST,
        .id = number,
        .data = &input->request,
        .lease = lease,
    };
}

static void test_layout_and_init(void)
{
    struct fixture fixture;
    options_init(&fixture);
    fixture_start(&fixture);
    CHECK(fixture.v->options.seed != &fixture.membership);
    CHECK(fixture.v->options.seed->members != &fixture.member);
    CHECK(fixture.v->options.seed->members[0].id == 1);
    fixture.member.id = 99;
    CHECK(fixture.v->options.seed->members[0].id == 1);
    CHECK(vsr_deinit(fixture.v) == VSR_EBUSY);
    struct vsr_layout invalid = {123, 456};
    CHECK(vsr_layout(NULL, &invalid) == VSR_EINVAL);
    CHECK(invalid.size == 0 && invalid.alignment == 0);
    fixture.options.limits.pinned_payload_bytes = 1;
    CHECK(vsr_layout(&fixture.options, &invalid) == VSR_ELIMIT);
    CHECK(invalid.size == 0 && invalid.alignment == 0);
    fixture_stop(&fixture);
}

static void test_prefix_and_time(void)
{
    struct fixture fixture;
    options_init(&fixture);
    fixture_start(&fixture);
    struct vsr_event events[3] = {
        {.type = VSR_EVENT_TIME, .id = 10},
        {.type = VSR_EVENT_TIME, .id = 9},
        {.type = VSR_EVENT_TIME, .id = 20},
    };
    struct vsr_op output[8];
    struct vsr_update update = {.ops = output, .capacity = 8};
    CHECK(vsr_step_many(fixture.v, events, 3, &update) == VSR_EINVAL);
    CHECK(update.consumed == 1);
    CHECK(fixture.v->now == 10);
    CHECK((update.flags & VSR_UPDATE_STATE_CHANGED) != 0);
    CHECK(vsr_step(fixture.v, &events[2], &update) == VSR_OK);
    CHECK(update.consumed == 1 && fixture.v->now == 20);
    CHECK(vsr_after(fixture.v, 5) == 25);
    CHECK(vsr_after(fixture.v, UINT64_MAX - 20u) == VSR_NO_DEADLINE);
    CHECK(fixture.v->status.failure.code == VSR_FAILURE_EXHAUSTED);
    fixture_stop(&fixture);
}

static void test_pin_lifetime_and_ids(void)
{
    struct fixture fixture;
    options_init(&fixture);
    fixture.options.limits.operations = 1;
    fixture_start(&fixture);
    struct request_input request;
    request_init(&request, 1, 77);
    struct vsr_op output[8];
    struct vsr_update update = {.ops = output, .capacity = 8};
    CHECK(vsr_step(fixture.v, &request.event, &update) == VSR_OK);
    CHECK(update.consumed == 1 && update.count == 1);
    CHECK(output[0].type == VSR_OP_SEND);
    uint64_t first = output[0].id;
    const struct vsr_message *message = output[0].data;
    const struct vsr_prepare *prepare = message->body;
    const struct vsr_blob *blob = prepare->batch.entries[0].body;
    CHECK(message->number == 1 && prepare->batch.count == 1);
    CHECK(blob != &request.blob && blob->spans != &request.span);
    CHECK(blob->spans[0].data == request.bytes);
    memset(output, 0, sizeof(output));
    CHECK(vsr_step(fixture.v, NULL, &update) == VSR_OK);
    CHECK(update.count == 0);
    CHECK(message->number == 1 && blob->spans[0].data == request.bytes);
    CHECK(fixture.v->status.outstanding_leases == 1);
    CHECK(vsr_step(fixture.v, &request.event, &update) == VSR_EINVAL);
    CHECK(update.consumed == 0);
    struct vsr_event complete = {
        .type = VSR_EVENT_COMPLETE,
        .status = VSR_IO_OK,
        .id = first,
    };
    CHECK(vsr_step(fixture.v, &complete, &update) == VSR_OK);
    CHECK(update.count == 1 && output[0].type == VSR_OP_RELEASE &&
          output[0].arg == 77);
    CHECK(fixture.v->status.outstanding_leases == 0);
    CHECK(vsr_step(fixture.v, &complete, &update) == VSR_EINVAL);
    CHECK(update.consumed == 0);
    request_init(&request, 2, 77);
    CHECK(vsr_step(fixture.v, &request.event, &update) == VSR_OK);
    CHECK(update.count == 1 && output[0].id != first);
    complete.id = output[0].id;
    CHECK(vsr_step(fixture.v, &complete, &update) == VSR_OK);
    CHECK(fixture.v->status.outstanding_ops == 0);
    fixture_stop(&fixture);
}

static void test_backpressure_and_bounded_steps(void)
{
    struct fixture fixture;
    options_init(&fixture);
    fixture.options.limits.operations = 1;
    fixture.options.limits.work_per_step = 1;
    fixture_start(&fixture);
    struct request_input requests[2];
    request_init(&requests[0], 1, 81);
    request_init(&requests[1], 2, 82);
    struct vsr_op output[1];
    struct vsr_update update = {.ops = output, .capacity = 1};
    uint64_t operation = 0;
    bool consumed = false;
    for (uint32_t i = 0; i < 10 && operation == 0; i++) {
        int result =
            vsr_step(fixture.v, consumed ? NULL : &requests[0].event, &update);
        CHECK(result >= VSR_OK);
        consumed = consumed || update.consumed != 0;
        if (update.count != 0) {
            CHECK(output[0].type == VSR_OP_SEND);
            operation = output[0].id;
        }
    }
    CHECK(consumed && operation != 0);
    CHECK(vsr_step(fixture.v, &requests[1].event, &update) == VSR_AGAIN);
    CHECK(update.consumed == 0);
    CHECK((update.flags & VSR_UPDATE_INPUT_BLOCKED) != 0);
    CHECK(fixture.v->status.outstanding_leases == 1);
    struct vsr_event complete = {
        .type = VSR_EVENT_COMPLETE,
        .id = operation,
        .status = VSR_IO_OK,
    };
    CHECK(vsr_step(fixture.v, &complete, &update) == VSR_AGAIN);
    CHECK(update.consumed == 1);
    CHECK((update.flags & VSR_UPDATE_MORE) != 0);
    CHECK(vsr_step(fixture.v, NULL, &update) >= VSR_OK);
    CHECK(update.count == 1 && output[0].type == VSR_OP_RELEASE);
    CHECK(fixture.v->status.outstanding_leases == 0);
    fixture_stop(&fixture);
}

static void test_completion_reservation_and_fatal_failure(void)
{
    struct fixture fixture;
    options_init(&fixture);
    fixture_start(&fixture);
    struct vsr_store_read read = {
        .sequence = 1,
        .client = {3, 1},
        .max_bytes = fixture.options.limits.result_bytes,
        .type = VSR_LOAD_CLIENT,
        .max_count = 1,
    };
    struct vsr_operation *operation = vsr_operation_acquire(
        fixture.v, VSR_OP_LOAD, 0, 123, fixture.options.limits.result_bytes);
    CHECK(operation != NULL);
    CHECK(vsr_operation_copy(operation, &read) == VSR_OK);
    vsr_operation_publish(fixture.v, operation);
    CHECK(fixture.v->reserved_leases == 1);
    CHECK(fixture.v->reserved_bytes == fixture.options.limits.result_bytes);
    struct vsr_op output[8];
    struct vsr_update update = {.ops = output, .capacity = 8};
    CHECK(vsr_step(fixture.v, NULL, &update) == VSR_OK);
    CHECK(update.count == 1 && output[0].type == VSR_OP_LOAD);
    struct vsr_loaded loaded = {.items = NULL, .sequence = 1, .count = 0};
    struct vsr_event complete = {
        .type = VSR_EVENT_COMPLETE,
        .id = output[0].id,
        .data = &loaded,
        .lease = 88,
    };
    CHECK(vsr_step(fixture.v, &complete, &update) == VSR_OK);
    CHECK(update.count == 1 && output[0].type == VSR_OP_RELEASE &&
          output[0].arg == 88);
    CHECK(fixture.v->reserved_leases == 0 && fixture.v->reserved_bytes == 0);
    operation = vsr_operation_acquire(fixture.v, VSR_OP_SYNC, 1, 456, 0);
    CHECK(operation != NULL);
    CHECK(vsr_operation_copy(operation, NULL) == VSR_OK);
    vsr_operation_publish(fixture.v, operation);
    CHECK(vsr_step(fixture.v, NULL, &update) == VSR_OK);
    CHECK(update.count == 1 && output[0].type == VSR_OP_SYNC);
    uint64_t failed_id = output[0].id;
    complete = (struct vsr_event){
        .type = VSR_EVENT_COMPLETE,
        .status = VSR_IO_RETRY,
        .id = failed_id,
    };
    CHECK(vsr_step(fixture.v, &complete, &update) == VSR_OK);
    CHECK(update.consumed == 1);
    CHECK(fixture.v->status.state == VSR_STATE_FAILED);
    CHECK(fixture.v->status.failure.code == VSR_FAILURE_STORAGE);
    CHECK(fixture.v->status.failure.operation == failed_id);
    CHECK(fixture.v->status.failure.operation_type == VSR_OP_SYNC);
    CHECK(fixture.v->status.failure.status == VSR_IO_RETRY);
    vsr_fail(fixture.v, VSR_FAILURE_EXHAUSTED, NULL, VSR_IO_OK);
    CHECK(fixture.v->status.failure.code == VSR_FAILURE_STORAGE);
    fixture_stop(&fixture);
}

static void test_inconsistent_completion_is_consumed(void)
{
    struct fixture fixture;
    options_init(&fixture);
    fixture_start(&fixture);
    struct vsr_blob command = {0};
    struct vsr_entry entry = {
        .op = 1,
        .request = {{4, 1}, 1},
        .type = VSR_REQUEST_COMMAND,
        .body = &command,
    };
    struct vsr_apply apply = {.batch = {&entry, 1, 0}, .through = 1};
    struct vsr_operation *operation =
        vsr_operation_acquire(fixture.v, VSR_OP_APPLY, 0, 0, 32);
    CHECK(operation != NULL);
    CHECK(vsr_operation_copy(operation, &apply) == VSR_OK);
    vsr_operation_publish(fixture.v, operation);
    struct vsr_op outputs[8];
    struct vsr_update update = {.ops = outputs, .capacity = 8};
    CHECK(vsr_step(fixture.v, NULL, &update) == VSR_OK);
    CHECK(update.count == 1 && outputs[0].type == VSR_OP_APPLY);
    unsigned char bytes[32] = {0};
    struct vsr_span span = {bytes, sizeof(bytes)};
    struct vsr_value values[2] = {
        {.data = {&span, sizeof(bytes), 1, 0}},
        {.data = {&span, sizeof(bytes), 1, 0}},
    };
    struct vsr_applied applied = {.results = values, .count = 2};
    struct vsr_event complete = {
        .type = VSR_EVENT_COMPLETE,
        .id = outputs[0].id,
        .data = &applied,
        .lease = 98,
    };
    CHECK(vsr_step(fixture.v, &complete, &update) == VSR_OK);
    CHECK(update.consumed == 1);
    CHECK(fixture.v->status.state == VSR_STATE_FAILED);
    CHECK(fixture.v->status.failure.code == VSR_FAILURE_APPLICATION);
    CHECK(update.count == 1 && outputs[0].type == VSR_OP_RELEASE &&
          outputs[0].arg == 98);
    CHECK(fixture.v->payload_bytes == 0 && fixture.v->reserved_bytes == 0);
    CHECK(fixture.v->status.outstanding_ops == 0);
    fixture_stop(&fixture);
}

static void test_stop_preserves_emitted_pins(void)
{
    struct fixture fixture;
    options_init(&fixture);
    fixture_start(&fixture);
    struct request_input request;
    request_init(&request, 1, 101);
    struct vsr_op output[8];
    struct vsr_update update = {.ops = output, .capacity = 8};
    CHECK(vsr_step(fixture.v, &request.event, &update) == VSR_OK);
    CHECK(update.count == 1);
    uint64_t id = output[0].id;
    const struct vsr_message *message = output[0].data;
    struct vsr_event stop = {.type = VSR_EVENT_STOP};
    CHECK(vsr_step(fixture.v, &stop, &update) == VSR_OK);
    CHECK(fixture.v->status.state == VSR_STATE_STOPPING);
    CHECK(fixture.v->status.outstanding_ops == 1);
    CHECK(fixture.v->status.outstanding_leases == 1);
    CHECK(message->number == 1);
    CHECK(vsr_deinit(fixture.v) == VSR_EBUSY);
    CHECK(vsr_step(fixture.v, &request.event, &update) == VSR_EINVAL);
    struct vsr_event complete = {
        .type = VSR_EVENT_COMPLETE,
        .status = VSR_IO_CANCELLED,
        .id = id,
    };
    CHECK(vsr_step(fixture.v, &complete, &update) == VSR_OK);
    CHECK(update.count == 1 && output[0].type == VSR_OP_RELEASE);
    CHECK(fixture.v->status.state == VSR_STATE_STOPPED);
    struct mock_protocol *mock = fixture.v->protocol;
    CHECK(mock->stops == 1 && mock->completions == 0);
    fixture_stop(&fixture);
}

int main(void)
{
    test_layout_and_init();
    test_prefix_and_time();
    test_pin_lifetime_and_ids();
    test_backpressure_and_bounded_steps();
    test_completion_reservation_and_fatal_failure();
    test_inconsistent_completion_is_consumed();
    test_stop_preserves_emitted_pins();
    return 0;
}
