#include "config.h"

#include "internal.h"

#include "checked.h"
#include "validate.h"

#include <stdalign.h>
#include <string.h>

struct arena_plan {
    size_t size;
    size_t alignment;
    size_t operations;
    size_t leases;
    size_t lease_buckets;
    size_t seed;
    size_t members;
    size_t holds;
    size_t metadata;
    size_t metadata_stride;
    size_t protocol;
    size_t protocol_size;
    uint32_t hold_capacity;
    uint64_t progress_bytes;
};

static bool power_of_two(size_t value)
{
    return value != 0 && (value & (value - 1)) == 0;
}

static bool aligned_size(size_t value, size_t alignment, size_t *result)
{
    size_t extra = (alignment - value % alignment) % alignment;
    return vsr_size_add(value, extra, result);
}

static bool add_slice(struct arena_plan *plan, size_t count, size_t size,
                      size_t alignment, size_t *offset)
{
    size_t bytes;
    if (!aligned_size(plan->size, alignment, offset) ||
        !vsr_size_mul(count, size, &bytes) ||
        !vsr_size_add(*offset, bytes, &plan->size)) {
        return false;
    }
    return true;
}

static bool metadata_size(const struct vsr_limits *limits, size_t *result)
{
    /* Internal transaction construction permits one APPEND, CLIENTS, HARD_STATE
     * and checkpoint change each, plus IDENTITY and range changes. owned.c
     * enforces this restriction before copying. APPEND and CLIENTS may coexist.
     *
     * Sum all alternative root descriptors conservatively. The largest nested
     * fixed graph has two checkpoints, two epochs, four memberships and two
     * manifest span arrays. Every separately allocated record is rounded to
     * max_align_t, so this also accounts for allocator alignment gaps. Payload
     * bytes are excluded: only their adapter-owned addresses are stored here. */
    const size_t fixed_sizes[] = {
        sizeof(struct vsr_message),        sizeof(struct vsr_reply),
        sizeof(struct vsr_store),          sizeof(struct vsr_prepare),
        sizeof(struct vsr_log_state),      sizeof(struct vsr_state_chunk),
        sizeof(struct vsr_recovery),       sizeof(struct vsr_fetch),
        sizeof(struct vsr_apply),          sizeof(struct vsr_read_fence),
        sizeof(struct vsr_store_read),     sizeof(struct vsr_snapshot_task),
        sizeof(struct vsr_store_identity), sizeof(struct vsr_hard_state),
        sizeof(struct vsr_nonce),          sizeof(struct vsr_epoch),
        sizeof(struct vsr_epoch),          sizeof(struct vsr_checkpoint),
        sizeof(struct vsr_checkpoint),
    };
    size_t fixed = 0;
    for (size_t i = 0; i < sizeof(fixed_sizes) / sizeof(fixed_sizes[0]); i++) {
        size_t rounded;
        if (!aligned_size(fixed_sizes[i], alignof(max_align_t), &rounded) ||
            !vsr_size_add(fixed, rounded, &fixed)) {
            return false;
        }
    }
    size_t spans;
    size_t members;
    size_t command;
    size_t configuration;
    size_t entry;
    size_t client;
    size_t batch;
    size_t nested;
    size_t changes;
    if (!vsr_size_mul(limits->spans_per_blob, sizeof(struct vsr_span),
                      &spans) ||
        !vsr_size_mul(limits->members, sizeof(struct vsr_member), &members) ||
        !vsr_size_add(sizeof(struct vsr_blob), spans, &command) ||
        !vsr_size_add(sizeof(struct vsr_membership), members, &configuration) ||
        !aligned_size(configuration, alignof(max_align_t), &configuration)) {
        return false;
    }
    size_t body = command > configuration ? command : configuration;
    if (!vsr_size_add(sizeof(struct vsr_entry), body, &entry) ||
        !vsr_size_add(entry, alignof(max_align_t), &entry) ||
        !vsr_size_add(sizeof(struct vsr_client_record), spans, &client) ||
        !vsr_size_add(client, alignof(max_align_t), &client) ||
        !vsr_size_add(entry, client, &batch) ||
        !vsr_size_mul(batch, limits->batch_entries, &batch) ||
        !vsr_size_mul(configuration, 4, &nested) ||
        !vsr_size_add(nested, spans, &nested) ||
        !vsr_size_add(nested, spans, &nested) ||
        !vsr_size_mul(VSR_MAX_STORE_CHANGES, sizeof(struct vsr_change),
                      &changes) ||
        !vsr_size_add(fixed, changes, &fixed) ||
        !vsr_size_add(batch, nested, &batch) ||
        !vsr_size_add(batch, fixed, &batch)) {
        return false;
    }
    return aligned_size(batch, alignof(max_align_t), result);
}

static int plan_arena(const struct vsr_options *options,
                      struct arena_plan *plan)
{
    int result = vsr_validate_options(options);
    if (result != VSR_OK) {
        return result;
    }
    memset(plan, 0, sizeof(*plan));
    const struct vsr_limits *limits = &options->limits;
    if (limits->transfers > UINT32_MAX - 8u ||
        limits->input_leases < limits->transfers + 8u ||
        limits->batch_entries > UINT32_MAX - 4u) {
        return VSR_ELIMIT;
    }
    uint64_t maximum = limits->command_bytes > limits->result_bytes
                           ? limits->command_bytes
                           : limits->result_bytes;
    if (maximum > UINT64_MAX - limits->manifest_bytes) {
        return VSR_ELIMIT;
    }
    plan->progress_bytes = maximum + limits->manifest_bytes;
    /* Simultaneous transfer holds: the selected full input graph, one fetched
     * command and one comparison command, retained offer/current/replacement
     * manifests, and a result-progress allowance. Issued operation results are
     * additionally reserved dynamically, and batches may be shortened. */
    uint64_t minimum = limits->message_bytes;
    uint64_t manifest_count = (uint64_t)limits->transfers + 3u;
    if (limits->manifest_bytes > UINT64_MAX / manifest_count)
        return VSR_ELIMIT;
    uint64_t manifests = limits->manifest_bytes * manifest_count;
    const uint64_t components[] = {limits->command_bytes, limits->command_bytes,
                                   manifests, limits->result_bytes};
    for (size_t i = 0; i < sizeof(components) / sizeof(components[0]); i++) {
        if (components[i] > UINT64_MAX - minimum)
            return VSR_ELIMIT;
        minimum += components[i];
    }
    if (limits->pinned_payload_bytes < minimum)
        return VSR_ELIMIT;
    plan->hold_capacity = limits->batch_entries + 4u;
    if (plan->hold_capacity > limits->input_leases) {
        plan->hold_capacity = limits->input_leases;
    }
    size_t protocol_alignment = 0;
    result =
        vsr_protocol_size(options, &plan->protocol_size, &protocol_alignment);
    if (result != VSR_OK) {
        return result;
    }
    if (!power_of_two(protocol_alignment)) {
        return VSR_EINVAL;
    }
    plan->alignment =
        options->cache_line_bytes == 0 ? 64 : options->cache_line_bytes;
    if (plan->alignment < alignof(max_align_t)) {
        plan->alignment = alignof(max_align_t);
    }
    if (plan->alignment < protocol_alignment) {
        plan->alignment = protocol_alignment;
    }
    plan->size = sizeof(struct vsr);
    size_t hold_count;
    if (!metadata_size(limits, &plan->metadata_stride) ||
        !vsr_size_mul(limits->operations, plan->hold_capacity, &hold_count) ||
        !add_slice(plan, limits->operations, sizeof(struct vsr_operation),
                   plan->alignment, &plan->operations) ||
        !add_slice(plan, limits->input_leases, sizeof(struct vsr_lease),
                   plan->alignment, &plan->leases) ||
        !add_slice(plan, limits->input_leases, sizeof(uint32_t),
                   plan->alignment, &plan->lease_buckets) ||
        !add_slice(plan, 1, sizeof(struct vsr_membership),
                   alignof(struct vsr_membership), &plan->seed) ||
        !add_slice(plan, options->seed->count, sizeof(struct vsr_member),
                   alignof(struct vsr_member), &plan->members) ||
        !add_slice(plan, hold_count, sizeof(uint32_t), plan->alignment,
                   &plan->holds) ||
        !add_slice(plan, limits->operations, plan->metadata_stride,
                   plan->alignment, &plan->metadata) ||
        !add_slice(plan, 1, plan->protocol_size, protocol_alignment,
                   &plan->protocol) ||
        !aligned_size(plan->size, plan->alignment, &plan->size)) {
        return VSR_ELIMIT;
    }
    return VSR_OK;
}

int vsr_layout(const struct vsr_options *options, struct vsr_layout *layout)
{
    if (layout == NULL) {
        return VSR_EINVAL;
    }
    *layout = (struct vsr_layout){0, 0};
    if (options == NULL) {
        return VSR_EINVAL;
    }
    struct arena_plan plan;
    int result = plan_arena(options, &plan);
    if (result == VSR_OK) {
        layout->size = plan.size;
        layout->alignment = plan.alignment;
    }
    return result;
}

static bool overlaps(const void *left, size_t left_size, const void *right,
                     size_t right_size)
{
    if (left_size == 0 || right_size == 0) {
        return false;
    }
    uintptr_t a = (uintptr_t)left;
    uintptr_t b = (uintptr_t)right;
    return a <= b ? b - a < left_size : a - b < right_size;
}

int vsr_init(void *memory, size_t size, const struct vsr_options *options,
             struct vsr **out)
{
    if (out == NULL) {
        return VSR_EINVAL;
    }
    if (memory == NULL || options == NULL) {
        *out = NULL;
        return VSR_EINVAL;
    }
    /* An out pointer inside the arena is never written; every other error
     * clears it as promised, before any other overlap is rejected. */
    if (overlaps(memory, size, out, sizeof(*out))) {
        return VSR_EINVAL;
    }
    *out = NULL;
    if (overlaps(memory, size, options, sizeof(*options))) {
        return VSR_EINVAL;
    }
    struct arena_plan plan;
    int result = plan_arena(options, &plan);
    if (result != VSR_OK) {
        return result;
    }
    if ((uintptr_t)memory % plan.alignment != 0) {
        return VSR_EINVAL;
    }
    if (size < plan.size) {
        return VSR_ELIMIT;
    }
    size_t member_bytes;
    if (!vsr_size_mul(options->seed->count, sizeof(struct vsr_member),
                      &member_bytes)) {
        return VSR_ELIMIT;
    }
    if (overlaps(memory, size, options->seed, sizeof(*options->seed)) ||
        overlaps(memory, size, options->seed->members, member_bytes)) {
        return VSR_EINVAL;
    }
    memset(memory, 0, plan.size);
    struct vsr *v = memory;
    unsigned char *base = memory;
    v->options = *options;
    v->arena_size = size;
    v->operations = (void *)(base + plan.operations);
    v->leases = (void *)(base + plan.leases);
    v->lease_buckets = (void *)(base + plan.lease_buckets);
    struct vsr_membership *seed = (void *)(base + plan.seed);
    struct vsr_member *members = (void *)(base + plan.members);
    *seed = *options->seed;
    memcpy(members, options->seed->members, member_bytes);
    seed->members = members;
    v->options.seed = seed;
    v->protocol = base + plan.protocol;
    v->progress_bytes = plan.progress_bytes;
    v->operation_free = 0;
    v->operation_free_count = options->limits.operations;
    v->operation_ready_first = VSR_INDEX_NONE;
    v->operation_ready_last = VSR_INDEX_NONE;
    v->lease_free = 0;
    v->lease_free_count = options->limits.input_leases;
    v->release_first = VSR_INDEX_NONE;
    v->release_last = VSR_INDEX_NONE;
    uint32_t *holds = (void *)(base + plan.holds);
    for (uint32_t i = 0; i < options->limits.operations; i++) {
        struct vsr_operation *operation = &v->operations[i];
        operation->index = i;
        operation->next =
            i + 1u == options->limits.operations ? VSR_INDEX_NONE : i + 1u;
        operation->memory =
            base + plan.metadata + (size_t)i * plan.metadata_stride;
        operation->capacity = plan.metadata_stride;
        operation->leases = holds + (size_t)i * plan.hold_capacity;
        operation->lease_capacity = plan.hold_capacity;
    }
    for (uint32_t i = 0; i < options->limits.input_leases; i++) {
        v->lease_buckets[i] = VSR_INDEX_NONE;
        v->leases[i].hash_next = VSR_INDEX_NONE;
        v->leases[i].next =
            i + 1u == options->limits.input_leases ? VSR_INDEX_NONE : i + 1u;
    }
    v->status.state = VSR_STATE_STARTING;
    v->initialized = true;
    vsr_protocol_init(v, v->protocol, plan.protocol_size);
    *out = v;
    return VSR_OK;
}

void vsr_changed(struct vsr *v)
{
    v->state_changed = true;
}

static bool operation_has_data_completion(uint32_t type)
{
    return type == VSR_OP_LOAD || type == VSR_OP_APPLY ||
           type == VSR_OP_SNAPSHOT_CAPTURE || type == VSR_OP_SNAPSHOT_FETCH;
}

struct vsr_operation *vsr_operation_acquire(struct vsr *v, uint32_t type,
                                            uint64_t arg, uint64_t tag,
                                            uint64_t completion_bytes)
{
    if (type >= VSR_OP_RELEASE || v->stopping ||
        v->status.state == VSR_STATE_FAILED ||
        v->status.state == VSR_STATE_STOPPED ||
        v->operation_free == VSR_INDEX_NONE) {
        return NULL;
    }
    bool reserve_lease = operation_has_data_completion(type);
    if ((!reserve_lease && completion_bytes != 0) ||
        completion_bytes >
            v->options.limits.pinned_payload_bytes - v->payload_bytes ||
        v->reserved_bytes > v->options.limits.pinned_payload_bytes -
                                v->payload_bytes - completion_bytes ||
        (reserve_lease && v->lease_free_count <= v->reserved_leases)) {
        return NULL;
    }
    struct vsr_operation *operation = &v->operations[v->operation_free];
    uint64_t capacity = v->options.limits.operations;
    uint64_t tail = (uint64_t)operation->index + 1u;
    if (operation->generation > (UINT64_MAX - 1u - tail) / capacity) {
        vsr_fail(v, VSR_FAILURE_EXHAUSTED, NULL, VSR_IO_OK);
        return NULL;
    }
    uint64_t id = operation->generation * capacity + tail;
    operation->generation++;
    v->operation_free = operation->next;
    v->operation_free_count--;
    operation->output = (struct vsr_op){type, 0, id, NULL, arg};
    operation->tag = tag;
    operation->reserved_bytes = completion_bytes;
    operation->reserves_lease = reserve_lease;
    operation->used = 0;
    operation->lease_count = 0;
    operation->next = VSR_INDEX_NONE;
    operation->state = VSR_SLOT_BUILDING;
    v->reserved_bytes += completion_bytes;
    if (reserve_lease) {
        v->reserved_leases++;
    }
    return operation;
}

void *vsr_operation_alloc(struct vsr_operation *operation, size_t size,
                          size_t alignment)
{
    if (operation == NULL || operation->state != VSR_SLOT_BUILDING ||
        !power_of_two(alignment) || alignment > alignof(max_align_t)) {
        return NULL;
    }
    size_t offset;
    if (!aligned_size(operation->used, alignment, &offset) ||
        offset > operation->capacity || size > operation->capacity - offset) {
        return NULL;
    }
    operation->used = offset + size;
    return operation->memory + offset;
}

bool vsr_lease_retain(struct vsr *v, uint32_t lease)
{
    if (lease == VSR_INDEX_NONE) {
        return true;
    }
    if (lease >= v->options.limits.input_leases || v->leases[lease].id == 0 ||
        v->leases[lease].releasing ||
        v->leases[lease].references == UINT32_MAX) {
        vsr_fail(v, VSR_FAILURE_INVARIANT, NULL, VSR_IO_OK);
        return false;
    }
    v->leases[lease].references++;
    return true;
}

void vsr_lease_release(struct vsr *v, uint32_t lease)
{
    if (lease == VSR_INDEX_NONE) {
        return;
    }
    if (lease >= v->options.limits.input_leases || v->leases[lease].id == 0 ||
        v->leases[lease].references == 0) {
        vsr_fail(v, VSR_FAILURE_INVARIANT, NULL, VSR_IO_OK);
        return;
    }
    struct vsr_lease *pin = &v->leases[lease];
    pin->references--;
    if (pin->references != 0) {
        return;
    }
    pin->releasing = true;
    pin->next = VSR_INDEX_NONE;
    if (v->release_last == VSR_INDEX_NONE) {
        v->release_first = lease;
    } else {
        v->leases[v->release_last].next = lease;
    }
    v->release_last = lease;
}

uint64_t vsr_lease_id(const struct vsr *v, uint32_t lease)
{
    return lease == VSR_INDEX_NONE ? 0 : v->leases[lease].id;
}

bool vsr_operation_hold(struct vsr *v, struct vsr_operation *operation,
                        uint32_t lease)
{
    if (operation->state != VSR_SLOT_BUILDING) {
        return false;
    }
    if (lease == VSR_INDEX_NONE) {
        return true;
    }
    for (uint32_t i = 0; i < operation->lease_count; i++) {
        if (operation->leases[i] == lease) {
            return true;
        }
    }
    if (operation->lease_count == operation->lease_capacity ||
        !vsr_lease_retain(v, lease)) {
        return false;
    }
    operation->leases[operation->lease_count++] = lease;
    return true;
}

void vsr_operation_publish(struct vsr *v, struct vsr_operation *operation)
{
    if (operation->state != VSR_SLOT_BUILDING) {
        vsr_fail(v, VSR_FAILURE_INVARIANT, NULL, VSR_IO_OK);
        return;
    }
    operation->state = VSR_SLOT_READY;
    operation->next = VSR_INDEX_NONE;
    if (v->operation_ready_last == VSR_INDEX_NONE) {
        v->operation_ready_first = operation->index;
    } else {
        v->operations[v->operation_ready_last].next = operation->index;
    }
    v->operation_ready_last = operation->index;
}

static void unreserve_operation(struct vsr *v, struct vsr_operation *operation)
{
    v->reserved_bytes -= operation->reserved_bytes;
    operation->reserved_bytes = 0;
    if (operation->reserves_lease) {
        v->reserved_leases--;
        operation->reserves_lease = false;
    }
}

static void recycle_operation(struct vsr *v, struct vsr_operation *operation)
{
    unreserve_operation(v, operation);
    for (uint32_t i = 0; i < operation->lease_count; i++) {
        vsr_lease_release(v, operation->leases[i]);
    }
    operation->lease_count = 0;
    operation->output.id = 0;
    operation->output.data = NULL;
    operation->state = VSR_SLOT_FREE;
    operation->next = v->operation_free;
    v->operation_free = operation->index;
    v->operation_free_count++;
}

void vsr_operation_abort(struct vsr *v, struct vsr_operation *operation)
{
    if (operation->state != VSR_SLOT_BUILDING) {
        vsr_fail(v, VSR_FAILURE_INVARIANT, NULL, VSR_IO_OK);
        return;
    }
    recycle_operation(v, operation);
}

struct vsr_operation *vsr_operation_find(struct vsr *v, uint64_t id)
{
    if (id == 0 || id == UINT64_MAX) {
        return NULL;
    }
    uint32_t index = (uint32_t)((id - 1u) % v->options.limits.operations);
    struct vsr_operation *operation = &v->operations[index];
    return operation->state == VSR_SLOT_ACTIVE && operation->output.id == id
               ? operation
               : NULL;
}

static void quiesce(struct vsr *v)
{
    if (v->protocol_quiesced) {
        return;
    }
    v->protocol_quiesced = true;
    vsr_protocol_stop(v);
    v->operation_ready_first = VSR_INDEX_NONE;
    v->operation_ready_last = VSR_INDEX_NONE;
    for (uint32_t i = 0; i < v->options.limits.operations; i++) {
        struct vsr_operation *operation = &v->operations[i];
        if (operation->state == VSR_SLOT_BUILDING ||
            operation->state == VSR_SLOT_READY) {
            recycle_operation(v, operation);
        }
    }
}

void vsr_fail(struct vsr *v, uint32_t code,
              const struct vsr_operation *operation, int32_t status)
{
    if (v->status.failure.code == VSR_FAILURE_NONE) {
        v->status.failure.code = code;
        if (operation != NULL) {
            v->status.failure.operation = operation->output.id;
            v->status.failure.operation_type = operation->output.type;
            v->status.failure.status = status;
        }
        vsr_changed(v);
    }
    if (!v->stopping && v->status.state != VSR_STATE_STOPPED) {
        v->status.state = VSR_STATE_FAILED;
        vsr_changed(v);
    }
    quiesce(v);
}

uint64_t vsr_after(struct vsr *v, uint64_t delay)
{
    if (!v->time_set) {
        return VSR_NO_DEADLINE;
    }
    if (delay >= UINT64_MAX - v->now) {
        vsr_fail(v, VSR_FAILURE_EXHAUSTED, NULL, VSR_IO_OK);
        return VSR_NO_DEADLINE;
    }
    return v->now + delay;
}

static uint64_t deadline(const struct vsr *v)
{
    if (!v->time_set || v->protocol_quiesced || v->stopping ||
        v->status.state == VSR_STATE_RETIRED ||
        v->status.state == VSR_STATE_STOPPED) {
        return VSR_NO_DEADLINE;
    }
    return vsr_protocol_deadline(v);
}

static uint32_t lease_bucket(const struct vsr *v, uint64_t id)
{
    uint64_t folded = id ^ (id >> 32) ^ (id >> 16);
    return (uint32_t)(folded % v->options.limits.input_leases);
}

static bool lease_exists(const struct vsr *v, uint64_t id)
{
    uint32_t index = v->lease_buckets[lease_bucket(v, id)];
    while (index != VSR_INDEX_NONE) {
        if (v->leases[index].id == id) {
            return true;
        }
        index = v->leases[index].hash_next;
    }
    return false;
}

static void unindex_lease(struct vsr *v, uint32_t index)
{
    struct vsr_lease *lease = &v->leases[index];
    uint32_t *link = &v->lease_buckets[lease_bucket(v, lease->id)];
    while (*link != index) {
        link = &v->leases[*link].hash_next;
    }
    *link = lease->hash_next;
    lease->hash_next = VSR_INDEX_NONE;
}

static uint32_t take_lease(struct vsr *v, uint64_t id, uint64_t bytes)
{
    uint32_t index = v->lease_free;
    struct vsr_lease *lease = &v->leases[index];
    v->lease_free = lease->next;
    v->lease_free_count--;
    uint32_t bucket = lease_bucket(v, id);
    lease->hash_next = v->lease_buckets[bucket];
    v->lease_buckets[bucket] = index;
    lease->id = id;
    lease->bytes = bytes;
    lease->references = 1;
    lease->next = VSR_INDEX_NONE;
    lease->releasing = false;
    v->payload_bytes += bytes;
    v->status.outstanding_leases++;
    return index;
}

/* Unconsumed input transfers no pin and therefore must not produce RELEASE. */
static void discard_lease(struct vsr *v, uint32_t index)
{
    if (index == VSR_INDEX_NONE) {
        return;
    }
    struct vsr_lease *lease = &v->leases[index];
    if (lease->references != 1 || lease->releasing) {
        vsr_fail(v, VSR_FAILURE_INVARIANT, NULL, VSR_IO_OK);
        return;
    }
    v->payload_bytes -= lease->bytes;
    unindex_lease(v, index);
    lease->id = 0;
    lease->bytes = 0;
    lease->references = 0;
    lease->next = v->lease_free;
    v->lease_free = index;
    v->lease_free_count++;
    v->status.outstanding_leases--;
}

static bool has_output(const struct vsr *v)
{
    return v->release_first != VSR_INDEX_NONE ||
           v->operation_ready_first != VSR_INDEX_NONE;
}

static void emit_output(struct vsr *v, struct vsr_update *update)
{
    struct vsr_op *output = &update->ops[update->count++];
    if (v->release_first != VSR_INDEX_NONE) {
        uint32_t index = v->release_first;
        struct vsr_lease *lease = &v->leases[index];
        *output = (struct vsr_op){VSR_OP_RELEASE, 0, 0, NULL, lease->id};
        v->release_first = lease->next;
        if (v->release_first == VSR_INDEX_NONE) {
            v->release_last = VSR_INDEX_NONE;
        }
        v->payload_bytes -= lease->bytes;
        unindex_lease(v, index);
        lease->id = 0;
        lease->bytes = 0;
        lease->releasing = false;
        lease->next = v->lease_free;
        v->lease_free = index;
        v->lease_free_count++;
        v->status.outstanding_leases--;
        return;
    }
    struct vsr_operation *operation = &v->operations[v->operation_ready_first];
    *output = operation->output;
    v->operation_ready_first = operation->next;
    if (v->operation_ready_first == VSR_INDEX_NONE) {
        v->operation_ready_last = VSR_INDEX_NONE;
    }
    operation->state = VSR_SLOT_ACTIVE;
    operation->next = VSR_INDEX_NONE;
    v->status.outstanding_ops++;
}

static void maybe_stopped(struct vsr *v)
{
    if (v->stopping && v->status.state != VSR_STATE_STOPPED &&
        v->operation_free_count == v->options.limits.operations &&
        v->status.outstanding_leases == 0 && !has_output(v)) {
        v->status.state = VSR_STATE_STOPPED;
        vsr_changed(v);
    }
}

static uint32_t completion_failure(const struct vsr_operation *operation,
                                   int32_t status)
{
    if (status == VSR_IO_OK) {
        return VSR_FAILURE_NONE;
    }
    uint32_t type = operation->output.type;
    if (type >= VSR_OP_SNAPSHOT_CAPTURE && type <= VSR_OP_SNAPSHOT_DROP &&
        status == VSR_IO_CORRUPT) {
        return VSR_FAILURE_SNAPSHOT;
    }
    switch (type) {
    case VSR_OP_STORE:
    case VSR_OP_SYNC:
        return VSR_FAILURE_STORAGE;
    case VSR_OP_APPLY:
    case VSR_OP_SNAPSHOT_INSTALL:
        return VSR_FAILURE_APPLICATION;
    case VSR_OP_SNAPSHOT_SYNC:
        return VSR_FAILURE_SNAPSHOT;
    case VSR_OP_LOAD:
        return status == VSR_IO_CORRUPT || status == VSR_IO_FAILED ||
                       status == VSR_IO_CANCELLED
                   ? VSR_FAILURE_STORAGE
                   : VSR_FAILURE_NONE;
    case VSR_OP_RECLAIM:
        return status == VSR_IO_CORRUPT ? VSR_FAILURE_STORAGE
                                        : VSR_FAILURE_NONE;
    default:
        return VSR_FAILURE_NONE;
    }
}

static bool terminal(const struct vsr *v)
{
    return v->status.state == VSR_STATE_FAILED ||
           v->status.state == VSR_STATE_RETIRED ||
           v->status.state == VSR_STATE_STOPPED;
}

static int admit_event(struct vsr *v, const struct vsr_event *event,
                       bool *blocked)
{
    struct vsr_operation *operation = NULL;
    if (event->type == VSR_EVENT_COMPLETE) {
        operation = vsr_operation_find(v, event->id);
        if (operation == NULL) {
            return VSR_EINVAL;
        }
    }
    struct vsr_validation validation = {
        .limits = &v->options.limits,
        .arena = v,
        .arena_size = v->arena_size,
        .payload_bytes = 0,
    };
    int result = vsr_validate_event(
        &validation, event, operation == NULL ? NULL : &operation->output);
    if (result != VSR_OK) {
        return result;
    }
    if (event->type == VSR_EVENT_TIME && v->time_set && event->id < v->now) {
        return VSR_EINVAL;
    }
    if (v->stopping && event->type != VSR_EVENT_TIME &&
        event->type != VSR_EVENT_COMPLETE && event->type != VSR_EVENT_STOP) {
        return VSR_EINVAL;
    }
    if (event->lease != 0 && lease_exists(v, event->lease)) {
        return VSR_EINVAL;
    }
    uint32_t lease = VSR_INDEX_NONE;
    bool invalid_effect = validation.failure_code != VSR_FAILURE_NONE;
    if (event->data != NULL) {
        if (operation != NULL) {
            if (!operation->reserves_lease || v->lease_free_count == 0) {
                /* This is an internal reservation error, not a host shape error. */
                vsr_fail(v, VSR_FAILURE_INVARIANT, operation, event->status);
                return VSR_EINVAL;
            }
            if (!invalid_effect &&
                validation.payload_bytes > operation->reserved_bytes) {
                validation.failure_code = VSR_FAILURE_INVARIANT;
                invalid_effect = true;
            }
            unreserve_operation(v, operation);
        } else {
            bool inactive = terminal(v) || v->stopping;
            bool control = event->type == VSR_EVENT_MESSAGE;
            uint32_t lease_reserve = inactive ? 0u : control ? 1u : 2u;
            uint64_t byte_reserve = inactive ? 0u : v->progress_bytes;
            uint64_t limit = v->options.limits.pinned_payload_bytes;
            if (validation.payload_bytes > limit - byte_reserve) {
                return VSR_ELIMIT;
            }
            if (v->lease_free_count - v->reserved_leases <= lease_reserve ||
                validation.payload_bytes > limit - v->payload_bytes ||
                v->reserved_bytes >
                    limit - v->payload_bytes - validation.payload_bytes ||
                byte_reserve > limit - v->payload_bytes -
                                   validation.payload_bytes -
                                   v->reserved_bytes) {
                /* Available space can exceed the standing reserve and still
                 * be too small for this specific input plus that reserve.
                 * Release a readable cache pin only after actual pressure,
                 * and only once the protocol poll is idle: the next poll may
                 * consume the pin (a commit notification, an application
                 * batch head, a lagging peer's resend) and would otherwise
                 * reload it before this input is retried, evicting and
                 * reloading the same entry forever. The queued RELEASE
                 * output still precedes retrying this input. */
                v->relief_pending = true;
                *blocked = true;
                return VSR_AGAIN;
            }
        }
        /* An inconsistent effect is never adopted. Its reserved lease slot
         * carries only the RELEASE token, so even an oversized invalid result
         * cannot consume or overflow the retained-payload accounting. */
        lease = take_lease(v, event->lease,
                           invalid_effect ? 0 : validation.payload_bytes);
    }
    uint64_t previous_time = v->now;
    bool previous_time_set = v->time_set;
    if (operation != NULL) {
        unreserve_operation(v, operation);
        uint32_t failure = validation.failure_code != VSR_FAILURE_NONE
                               ? validation.failure_code
                               : completion_failure(operation, event->status);
        if (failure != VSR_FAILURE_NONE) {
            vsr_fail(v, failure, operation, event->status);
        } else if (!v->protocol_quiesced) {
            vsr_protocol_complete(v, operation, event, lease);
        }
        v->status.outstanding_ops--;
        recycle_operation(v, operation);
    } else if (event->type == VSR_EVENT_STOP) {
        if (!v->stopping) {
            v->stopping = true;
            v->status.state = VSR_STATE_STOPPING;
            vsr_changed(v);
            quiesce(v);
        }
    } else if (event->type == VSR_EVENT_TIME) {
        v->now = event->id;
        v->time_set = true;
        if (!terminal(v) && !v->protocol_quiesced) {
            result = vsr_protocol_event(v, event, lease);
        }
    } else if (!terminal(v) && !v->protocol_quiesced) {
        result = vsr_protocol_event(v, event, lease);
    }
    if (result != VSR_OK) {
        v->now = previous_time;
        v->time_set = previous_time_set;
        discard_lease(v, lease);
        if (result == VSR_AGAIN) {
            *blocked = true;
        }
        return result;
    }
    vsr_lease_release(v, lease);
    return VSR_OK;
}

static bool observed_change(const struct vsr_status *before,
                            uint32_t before_phase,
                            const struct vsr_status *after)
{
    uint32_t after_phase =
        after->configuration == NULL ? UINT32_MAX : after->configuration->phase;
    return before->state != after->state || before->role != after->role ||
           before->epoch != after->epoch || before->view != after->view ||
           before->failure.code != after->failure.code ||
           before_phase != after_phase;
}

int vsr_step_many(struct vsr *v, const struct vsr_event *events, uint32_t count,
                  struct vsr_update *update)
{
    if (v == NULL || update == NULL || !v->initialized ||
        (uintptr_t)update % alignof(struct vsr_update) != 0 ||
        overlaps(update, sizeof(*update), v, v->arena_size)) {
        return VSR_EINVAL;
    }
    update->count = 0;
    update->consumed = 0;
    update->flags = 0;
    update->deadline_ns = deadline(v);
    size_t input_bytes;
    size_t output_bytes;
    if (update->capacity == 0 || update->ops == NULL ||
        (count != 0 && events == NULL) ||
        (uintptr_t)update->ops % alignof(struct vsr_op) != 0 ||
        (events != NULL &&
         (uintptr_t)events % alignof(struct vsr_event) != 0) ||
        !vsr_size_mul(count, sizeof(*events), &input_bytes) ||
        !vsr_size_mul(update->capacity, sizeof(*update->ops), &output_bytes) ||
        overlaps(events, input_bytes, update->ops, output_bytes) ||
        overlaps(events, input_bytes, v, v->arena_size) ||
        overlaps(update->ops, output_bytes, v, v->arena_size) ||
        overlaps(update, sizeof(*update), v, v->arena_size) ||
        overlaps(update, sizeof(*update), update->ops, output_bytes) ||
        overlaps(update, sizeof(*update), events, input_bytes)) {
        return VSR_EINVAL;
    }
    struct vsr_status before = v->status;
    uint32_t before_phase =
        before.configuration == NULL ? UINT32_MAX : before.configuration->phase;
    v->state_changed = false;
    uint32_t budget = v->options.limits.work_per_step;
    int result = VSR_OK;
    bool blocked = false;
    bool exhausted = false;
    bool idle = false;
    while (budget != 0) {
        if (has_output(v)) {
            if (update->count == update->capacity) {
                update->flags |= VSR_UPDATE_MORE | VSR_UPDATE_OUTPUT_FULL;
                result = VSR_AGAIN;
                break;
            }
            emit_output(v, update);
            budget--;
            continue;
        }
        if (!v->started && !v->protocol_quiesced) {
            budget--;
            int start = vsr_protocol_start(v);
            if (start == VSR_OK) {
                v->started = true;
                continue;
            }
            if (start != VSR_AGAIN) {
                vsr_fail(v, VSR_FAILURE_INVARIANT, NULL, VSR_IO_OK);
            }
            if (budget == 0) {
                exhausted = true;
                result = VSR_AGAIN;
                break;
            }
        }
        if (!blocked && update->consumed < count) {
            budget--;
            result = admit_event(v, &events[update->consumed], &blocked);
            if (result == VSR_AGAIN) {
                continue;
            }
            if (result != VSR_OK) {
                break;
            }
            v->relief_pending = false;
            update->consumed++;
            continue;
        }
        if (v->protocol_quiesced || terminal(v)) {
            break;
        }
        budget--;
        if (!vsr_protocol_poll(v)) {
            /* Idle: no poll will consume a readable cache pin before the
             * blocked input is retried, so one may be released for it. */
            if (v->relief_pending) {
                v->relief_pending = false;
                if (vsr_protocol_relieve_pressure(v))
                    continue;
            }
            idle = true;
            break;
        }
    }
    if (budget == 0 && result >= VSR_OK && !idle &&
        (!v->protocol_quiesced || update->consumed < count || has_output(v))) {
        exhausted = true;
        result = VSR_AGAIN;
    }
    if (blocked) {
        update->flags |= VSR_UPDATE_INPUT_BLOCKED;
    }
    if (has_output(v)) {
        update->flags |= VSR_UPDATE_MORE;
        if (update->count == update->capacity) {
            update->flags |= VSR_UPDATE_OUTPUT_FULL;
        }
    } else if (exhausted) {
        update->flags |= VSR_UPDATE_MORE;
    }
    maybe_stopped(v);
    if (v->state_changed ||
        observed_change(&before, before_phase, &v->status)) {
        update->flags |= VSR_UPDATE_STATE_CHANGED;
    }
    update->deadline_ns = deadline(v);
    return result;
}

int vsr_step(struct vsr *v, const struct vsr_event *event,
             struct vsr_update *update)
{
    return vsr_step_many(v, event, event == NULL ? 0u : 1u, update);
}

void vsr_get_status(const struct vsr *v, struct vsr_status *status)
{
    *status = v->status;
}

int vsr_deinit(struct vsr *v)
{
    if (v == NULL || !v->initialized) {
        return VSR_EINVAL;
    }
    if (v->status.state != VSR_STATE_STOPPED ||
        v->status.outstanding_ops != 0 || v->status.outstanding_leases != 0) {
        return VSR_EBUSY;
    }
    v->initialized = false;
    return VSR_OK;
}
