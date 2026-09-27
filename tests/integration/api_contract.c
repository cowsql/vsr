#define _DEFAULT_SOURCE 1

#include "config.h"

#include "lib/check.h"
#include "vsr.h"

#include <stdalign.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

/* Conformance checks for the driver-facing contract of include/vsr.h and
 * docs/vsr-api.md ("Initialization", "Driving the core", "Ownership and
 * resource accounting", "Application operations and failures"). One instance
 * is driven directly through vsr_step_many with hand-built events, so every
 * expectation follows from the documented semantics, not from a host. */

#define CAPACITY 8u
#define SLOTS 512u
#define IDS 4096u
#define BODY_BYTES 64u
#define LOG_ENTRIES 64u
#define CLIENT_RECORDS 32u

/* Graphs offered to the core stay immutable until their RELEASE. Every lease
 * owns one slot, so a released graph is never rewritten under a pin. */
struct graph {
    unsigned char bytes[8];
    struct vsr_span span;
    struct vsr_blob blob;
    struct vsr_request request;
    struct vsr_value values[CAPACITY];
    struct vsr_applied applied;
    struct vsr_loaded loaded;
    struct vsr_entry entries[CAPACITY];
    struct vsr_blob blobs[CAPACITY];
    struct vsr_span spans[CAPACITY];
    unsigned char body[CAPACITY][BODY_BYTES];
    struct vsr_client_record record;
    bool accepted;
    bool released;
};

/* A truthful logical store: completed transactions are readable exactly. */
struct stored_entry {
    bool used;
    bool has_body;
    uint64_t op;
    uint64_t epoch;
    uint64_t view;
    struct vsr_request_id request;
    uint32_t type;
    uint64_t size;
    unsigned char bytes[BODY_BYTES];
};

struct stored_client {
    bool used;
    struct vsr_request_id request;
    uint64_t op;
    int32_t code;
    uint64_t size;
    unsigned char bytes[BODY_BYTES];
};

struct instance {
    struct vsr_member members[3];
    struct vsr_membership seed;
    struct vsr_options options;
    struct vsr_layout layout;
    void *memory;
    struct vsr *v;
    struct vsr_op ops[CAPACITY];
    struct vsr_update update;
    uint32_t capacity;
    struct vsr_op pending[SLOTS];
    bool active[SLOTS];
    uint64_t ids[IDS];
    size_t id_count;
    struct graph graphs[SLOTS];
    uint64_t lease_base;
    uint64_t next_lease;
    bool time_set;
    bool stopped;
    bool output_full_seen;
    struct stored_entry log[LOG_ENTRIES];
    struct stored_client clients[CLIENT_RECORDS];
    struct vsr_store_identity identity;
    bool has_identity;
    uint64_t stored;
};

static struct vsr_status status(const struct instance *in)
{
    struct vsr_status current;
    vsr_get_status(in->v, &current);
    return current;
}

static struct instance *instance_new(uint32_t count)
{
    struct instance *in = calloc(1, sizeof(*in));
    CHECK(in != NULL);
    in->members[0] = (struct vsr_member){1, VSR_MEMBER_FULL, 0};
    in->members[1] = (struct vsr_member){2, VSR_MEMBER_FULL, 0};
    in->members[2] = (struct vsr_member){3, VSR_MEMBER_WITNESS, 0};
    in->seed =
        (struct vsr_membership){0, in->members, count, count == 1 ? 0u : 1u};
    in->options = (struct vsr_options){
        .cluster = {7, 7},
        .incarnation = {1, 1},
        .replica = 1,
        .seed = &in->seed,
        .limits =
            {
                .members = 4,
                .operations = 16,
                .input_leases = 32,
                .pending_requests = 8,
                .pending_reads = 4,
                .transfers = 2,
                .log_cache_entries = 16,
                .client_cache_entries = 8,
                .batch_entries = 4,
                .spans_per_blob = 4,
                .work_per_step = 64,
                .command_bytes = 64,
                .result_bytes = 32,
                .manifest_bytes = 64,
                .message_bytes = 256,
                .pinned_payload_bytes = 4096,
            },
        .heartbeat_ns = 100,
        .view_timeout_ns = 1000,
        .retry_ns = 50,
        .transfer_timeout_ns = 5000,
        .start_mode = VSR_START_NEW,
        .durability = VSR_DURABLE,
    };
    in->capacity = CAPACITY;
    in->lease_base = 1000;
    in->next_lease = in->lease_base;
    return in;
}

static void start(struct instance *in)
{
    CHECK(vsr_layout(&in->options, &in->layout) == VSR_OK);
    CHECK(in->layout.size != 0 && in->layout.alignment != 0);
    in->memory = aligned_alloc(in->layout.alignment, in->layout.size);
    CHECK(in->memory != NULL);
    CHECK(vsr_init(in->memory, in->layout.size, &in->options, &in->v) ==
          VSR_OK);
    CHECK(in->v != NULL);
}

static void finish(struct instance *in)
{
    CHECK(vsr_deinit(in->v) == VSR_OK);
    free(in->memory);
    free(in);
}

static size_t outstanding_ops(const struct instance *in)
{
    size_t count = 0;
    for (size_t i = 0; i < SLOTS; i++)
        count += in->active[i] ? 1u : 0u;
    return count;
}

static size_t outstanding_leases(const struct instance *in)
{
    size_t count = 0;
    for (uint64_t lease = in->lease_base; lease < in->next_lease; lease++) {
        const struct graph *graph = &in->graphs[lease - in->lease_base];
        count += graph->accepted && !graph->released ? 1u : 0u;
    }
    return count;
}

static struct graph *graph_of(struct instance *in, uint64_t lease)
{
    CHECK(lease >= in->lease_base && lease < in->next_lease);
    return &in->graphs[lease - in->lease_base];
}

static uint64_t take_lease(struct instance *in)
{
    CHECK(in->next_lease - in->lease_base < SLOTS);
    return in->next_lease++;
}

static size_t find_op(const struct instance *in, uint64_t id)
{
    for (size_t i = 0; i < SLOTS; i++) {
        if (in->active[i] && in->pending[i].id == id)
            return i;
    }
    return SIZE_MAX;
}

static size_t find_type(const struct instance *in, uint32_t type)
{
    for (size_t i = 0; i < SLOTS; i++) {
        if (in->active[i] && in->pending[i].type == type)
            return i;
    }
    return SIZE_MAX;
}

static void copy_blob(const struct vsr_blob *blob, unsigned char *bytes,
                      uint64_t *size)
{
    uint64_t total = 0;
    for (uint32_t i = 0; i < blob->count; i++) {
        CHECK(total + blob->spans[i].size <= BODY_BYTES);
        memcpy(bytes + total, blob->spans[i].data, blob->spans[i].size);
        total += blob->spans[i].size;
    }
    CHECK(total == blob->size);
    *size = total;
}

static void store_apply(struct instance *in, const struct vsr_store *store)
{
    CHECK(store->count >= 1 && store->count <= VSR_MAX_STORE_CHANGES);
    for (uint32_t c = 0; c < store->count; c++) {
        const struct vsr_change *change = &store->changes[c];
        switch (change->type) {
        case VSR_STORE_IDENTITY:
            CHECK(!in->has_identity && store->sequence == 1);
            in->identity = *(const struct vsr_store_identity *)change->data;
            in->has_identity = true;
            break;
        case VSR_STORE_APPEND: {
            const struct vsr_entry *entries = change->data;
            for (uint32_t i = 0; i < change->count; i++) {
                size_t slot = 0;
                while (slot < LOG_ENTRIES && in->log[slot].used)
                    slot++;
                CHECK(slot < LOG_ENTRIES);
                struct stored_entry *stored = &in->log[slot];
                memset(stored, 0, sizeof(*stored));
                stored->used = true;
                stored->op = entries[i].op;
                stored->epoch = entries[i].epoch;
                stored->view = entries[i].view;
                stored->request = entries[i].request;
                stored->type = entries[i].type;
                CHECK(stored->op == change->first + i);
                if (entries[i].type == VSR_REQUEST_COMMAND) {
                    stored->has_body = true;
                    copy_blob(entries[i].body, stored->bytes, &stored->size);
                } else {
                    CHECK(entries[i].type == VSR_REQUEST_NOOP);
                }
            }
            break;
        }
        case VSR_STORE_TRUNCATE:
            for (size_t i = 0; i < LOG_ENTRIES; i++) {
                if (in->log[i].used && in->log[i].op >= change->first)
                    in->log[i].used = false;
            }
            break;
        case VSR_STORE_TRIM:
            for (size_t i = 0; i < LOG_ENTRIES; i++) {
                if (in->log[i].used && in->log[i].op < change->first)
                    in->log[i].used = false;
            }
            break;
        case VSR_STORE_CLIENTS: {
            const struct vsr_client_record *records = change->data;
            for (uint32_t i = 0; i < change->count; i++) {
                size_t slot = SIZE_MAX;
                for (size_t j = 0; j < CLIENT_RECORDS; j++) {
                    if (in->clients[j].used &&
                        in->clients[j].request.client.hi ==
                            records[i].request.client.hi &&
                        in->clients[j].request.client.lo ==
                            records[i].request.client.lo)
                        slot = j;
                }
                if (slot == SIZE_MAX) {
                    slot = 0;
                    while (slot < CLIENT_RECORDS && in->clients[slot].used)
                        slot++;
                    CHECK(slot < CLIENT_RECORDS);
                } else if (in->clients[slot].request.number >=
                           records[i].request.number) {
                    continue;
                }
                struct stored_client *stored = &in->clients[slot];
                memset(stored, 0, sizeof(*stored));
                stored->used = true;
                stored->request = records[i].request;
                stored->op = records[i].op;
                stored->code = records[i].result.code;
                copy_blob(&records[i].result.data, stored->bytes,
                          &stored->size);
            }
            break;
        }
        case VSR_STORE_HARD_STATE:
        case VSR_STORE_PUBLISH_CHECKPOINT:
            break;
        default:
            CHECK(false);
        }
    }
    if (store->sequence > in->stored)
        in->stored = store->sequence;
}

static void record_op(struct instance *in, const struct vsr_op *op)
{
    /* flags must be zero; every op except RELEASE carries a nonzero ID that
     * is never reused within the instance. */
    CHECK(op->flags == 0);
    CHECK(op->type <= VSR_OP_RELEASE);
    if (op->type == VSR_OP_RELEASE) {
        CHECK(op->id == 0 && op->data == NULL);
        struct graph *graph = graph_of(in, op->arg);
        CHECK(graph->accepted && !graph->released);
        graph->released = true;
        return;
    }
    CHECK(op->id != 0 && op->id != UINT64_MAX);
    for (size_t i = 0; i < in->id_count; i++)
        CHECK(in->ids[i] != op->id);
    CHECK(in->id_count < IDS);
    in->ids[in->id_count++] = op->id;
    if (op->type == VSR_OP_SYNC || op->type == VSR_OP_RECLAIM)
        CHECK(op->data == NULL);
    else
        CHECK(op->data != NULL);
    if (op->type == VSR_OP_LOAD || op->type == VSR_OP_STORE ||
        op->type == VSR_OP_APPLY || op->type == VSR_OP_READ_READY ||
        op->type >= VSR_OP_SNAPSHOT_CAPTURE)
        CHECK(op->arg == 0);
    size_t slot = 0;
    while (slot < SLOTS && in->active[slot])
        slot++;
    CHECK(slot < SLOTS);
    in->pending[slot] = *op;
    in->active[slot] = true;
}

/* Every call goes through here so the documented update invariants and the
 * status counters are checked on each return. */
static int step(struct instance *in, const struct vsr_event *events,
                uint32_t count)
{
    struct vsr_update *u = &in->update;
    u->ops = in->ops;
    u->capacity = in->capacity;
    u->count = u->consumed = u->flags = 0xdeadbeefu;
    u->deadline_ns = 12345;
    const int result = vsr_step_many(in->v, events, count, u);
    CHECK(u->count <= in->capacity);
    CHECK(u->consumed <= count);
    const uint32_t progress =
        VSR_UPDATE_MORE | VSR_UPDATE_OUTPUT_FULL | VSR_UPDATE_INPUT_BLOCKED;
    if (result == VSR_OK) {
        CHECK(u->consumed == count);
        CHECK((u->flags & progress) == 0);
    } else if (result == VSR_AGAIN) {
        CHECK((u->flags & progress) != 0);
    } else {
        CHECK(u->consumed < count);
    }
    if ((u->flags & VSR_UPDATE_OUTPUT_FULL) != 0) {
        CHECK((u->flags & VSR_UPDATE_MORE) != 0);
        CHECK(u->count == in->capacity);
        in->output_full_seen = true;
    }
    CHECK((u->flags & ~(progress | VSR_UPDATE_STATE_CHANGED)) == 0);
    for (uint32_t i = 0; i < u->consumed; i++) {
        const struct vsr_event *event = &events[i];
        if (event->type == VSR_EVENT_TIME)
            in->time_set = true;
        if (event->type == VSR_EVENT_COMPLETE) {
            const size_t slot = find_op(in, event->id);
            CHECK(slot != SIZE_MAX);
            in->active[slot] = false;
        }
        if (event->lease != 0) {
            struct graph *graph = graph_of(in, event->lease);
            CHECK(!graph->accepted && !graph->released);
            graph->accepted = true;
        }
    }
    for (uint32_t i = 0; i < u->count; i++)
        record_op(in, &in->ops[i]);
    if (!in->time_set || in->stopped)
        CHECK(u->deadline_ns == VSR_NO_DEADLINE);
    const struct vsr_status current = status(in);
    CHECK(current.outstanding_ops == outstanding_ops(in));
    CHECK(current.outstanding_leases == outstanding_leases(in));
    if (current.state == VSR_STATE_STOPPED) {
        CHECK(current.outstanding_ops == 0 && current.outstanding_leases == 0);
        CHECK((u->flags & VSR_UPDATE_MORE) == 0);
        in->stopped = true;
    }
    return result;
}

static int step_one(struct instance *in, const struct vsr_event *event)
{
    return step(in, event, 1);
}

/* Drain runnable work until the core reports none. */
static void drain(struct instance *in)
{
    for (unsigned attempt = 0; attempt < 10000; attempt++) {
        const int result = step(in, NULL, 0);
        CHECK(result == VSR_OK || result == VSR_AGAIN);
        if ((in->update.flags & VSR_UPDATE_MORE) == 0)
            return;
    }
    CHECK(false);
}

static struct vsr_event time_event(uint64_t now)
{
    return (struct vsr_event){VSR_EVENT_TIME, 0, now, NULL, 0};
}

static void advance(struct instance *in, uint64_t now)
{
    const struct vsr_event event = time_event(now);
    const int result = step_one(in, &event);
    CHECK(result == VSR_OK || result == VSR_AGAIN);
    CHECK(in->update.consumed == 1);
    drain(in);
}

static void serve_entry(struct graph *graph, uint32_t index,
                        const struct stored_entry *stored)
{
    struct vsr_entry *entry = &graph->entries[index];
    *entry = (struct vsr_entry){
        stored->op, stored->epoch, stored->view, stored->request, stored->type,
        0,          NULL};
    if (!stored->has_body)
        return;
    memcpy(graph->body[index], stored->bytes, stored->size);
    graph->spans[index] = (struct vsr_span){graph->body[index], stored->size};
    graph->blobs[index] =
        stored->size == 0
            ? (struct vsr_blob){NULL, 0, 0, 0}
            : (struct vsr_blob){&graph->spans[index], stored->size, 1, 0};
    entry->body = &graph->blobs[index];
}

static const struct vsr_loaded *serve_load(struct instance *in,
                                           const struct vsr_store_read *read,
                                           uint64_t lease)
{
    struct graph *graph = graph_of(in, lease);
    graph->loaded = (struct vsr_loaded){NULL, read->sequence, 0, 0, 0};
    switch (read->type) {
    case VSR_LOAD_CLIENT: {
        const struct stored_client *found = NULL;
        for (size_t i = 0; i < CLIENT_RECORDS; i++) {
            const struct stored_client *c = &in->clients[i];
            if (c->used && c->request.client.hi == read->client.hi &&
                c->request.client.lo == read->client.lo)
                found = c;
        }
        if (found == NULL)
            break;
        memcpy(graph->body[0], found->bytes, found->size);
        graph->spans[0] = (struct vsr_span){graph->body[0], found->size};
        graph->record = (struct vsr_client_record){
            found->request,
            found->op,
            {found->size == 0
                 ? (struct vsr_blob){NULL, 0, 0, 0}
                 : (struct vsr_blob){&graph->spans[0], found->size, 1, 0},
             found->code, 0}};
        graph->loaded.items = &graph->record;
        graph->loaded.count = 1;
        break;
    }
    case VSR_LOAD_REQUEST: {
        const struct stored_entry *found = NULL;
        for (size_t i = 0; i < LOG_ENTRIES; i++) {
            const struct stored_entry *e = &in->log[i];
            if (e->used && e->request.client.hi == read->client.hi &&
                e->request.client.lo == read->client.lo &&
                (found == NULL || e->op > found->op))
                found = e;
        }
        if (found == NULL)
            break;
        serve_entry(graph, 0, found);
        graph->loaded.items = &graph->entries[0];
        graph->loaded.count = 1;
        break;
    }
    case VSR_LOAD_LOG: {
        uint32_t count = 0;
        uint64_t bytes = 0;
        for (uint64_t op = read->first; op < read->end; op++) {
            if (count == read->max_count || count == CAPACITY)
                break;
            const struct stored_entry *found = NULL;
            for (size_t i = 0; i < LOG_ENTRIES; i++) {
                if (in->log[i].used && in->log[i].op == op)
                    found = &in->log[i];
            }
            CHECK(found != NULL);
            if (count != 0 && bytes + found->size > read->max_bytes)
                break;
            bytes += found->size;
            serve_entry(graph, count, found);
            count++;
        }
        graph->loaded.items = count == 0 ? NULL : graph->entries;
        graph->loaded.count = count;
        graph->loaded.next = read->first + count;
        break;
    }
    default:
        CHECK(false);
    }
    return &graph->loaded;
}

/* A structurally valid completion for a pending operation. LOAD_RECOVERY on
 * the fresh store reports NOT_FOUND; other loads answer from the store; APPLY
 * returns one empty result per entry; every other success has NULL data. */
static struct vsr_event completion(struct instance *in, size_t slot,
                                   int io_status)
{
    const struct vsr_op *op = &in->pending[slot];
    struct vsr_event event = {VSR_EVENT_COMPLETE, io_status, op->id, NULL, 0};
    if (io_status != VSR_IO_OK)
        return event;
    if (op->type == VSR_OP_LOAD) {
        const struct vsr_store_read *read = op->data;
        if (read->type == VSR_LOAD_RECOVERY) {
            CHECK(!in->has_identity);
            event.status = VSR_IO_NOT_FOUND;
            return event;
        }
        const uint64_t lease = take_lease(in);
        event.data = serve_load(in, read, lease);
        event.lease = lease;
        return event;
    }
    if (op->type == VSR_OP_APPLY) {
        const struct vsr_apply *apply = op->data;
        CHECK(apply->batch.count >= 1 && apply->batch.count <= CAPACITY);
        const uint64_t lease = take_lease(in);
        struct graph *graph = graph_of(in, lease);
        memset(graph->values, 0, sizeof(graph->values));
        graph->applied =
            (struct vsr_applied){graph->values, apply->batch.count, 0};
        event.data = &graph->applied;
        event.lease = lease;
        return event;
    }
    CHECK(op->type != VSR_OP_SNAPSHOT_CAPTURE &&
          op->type != VSR_OP_SNAPSHOT_FETCH);
    return event;
}

/* The operation's graph is pinned only until its COMPLETE is accepted, so a
 * successful STORE is made readable before the completion is submitted. */
static int complete(struct instance *in, size_t slot, int io_status)
{
    const struct vsr_event event = completion(in, slot, io_status);
    if (io_status == VSR_IO_OK && in->pending[slot].type == VSR_OP_STORE)
        store_apply(in, in->pending[slot].data);
    const int result = step_one(in, &event);
    CHECK(result >= VSR_OK);
    CHECK(in->update.consumed == 1);
    return result;
}

static size_t first_pending(const struct instance *in)
{
    size_t best = SIZE_MAX;
    for (size_t i = 0; i < SLOTS; i++) {
        if (in->active[i] &&
            (best == SIZE_MAX || in->pending[i].id < in->pending[best].id))
            best = i;
    }
    return best;
}

/* Complete every operation successfully, in issue order, until idle. */
static void settle(struct instance *in)
{
    for (unsigned attempt = 0; attempt < 10000; attempt++) {
        drain(in);
        const size_t slot = first_pending(in);
        if (slot == SIZE_MAX)
            return;
        const int result = complete(in, slot, VSR_IO_OK);
        CHECK(result == VSR_OK || result == VSR_AGAIN);
    }
    CHECK(false);
}

/* Complete operations until one of the given type is pending. */
static size_t reach(struct instance *in, uint32_t type)
{
    for (unsigned attempt = 0; attempt < 10000; attempt++) {
        drain(in);
        size_t slot = find_type(in, type);
        if (slot != SIZE_MAX)
            return slot;
        slot = first_pending(in);
        CHECK(slot != SIZE_MAX);
        const int result = complete(in, slot, VSR_IO_OK);
        CHECK(result == VSR_OK || result == VSR_AGAIN);
    }
    CHECK(false);
    return SIZE_MAX;
}

static void build_request(struct graph *graph, uint64_t client, uint64_t number)
{
    memset(graph->bytes, (int)(number & 0xff), sizeof(graph->bytes));
    graph->span = (struct vsr_span){graph->bytes, sizeof(graph->bytes)};
    graph->blob = (struct vsr_blob){&graph->span, sizeof(graph->bytes), 1, 0};
    graph->request = (struct vsr_request){
        {{client, 9}, number}, 0, VSR_REQUEST_COMMAND, 0, &graph->blob};
}

static struct vsr_event request_event(struct instance *in, uint64_t client,
                                      uint64_t number, uint64_t route)
{
    const uint64_t lease = take_lease(in);
    struct graph *graph = graph_of(in, lease);
    build_request(graph, client, number);
    return (struct vsr_event){VSR_EVENT_REQUEST, 0, route, &graph->request,
                              lease};
}

/* An unconsumed event transferred nothing: its slot is handed back. */
static void untake_lease(struct instance *in, uint64_t lease)
{
    CHECK(lease + 1 == in->next_lease);
    CHECK(!in->graphs[lease - in->lease_base].accepted);
    in->next_lease--;
}

static void submit(struct instance *in, uint64_t client, uint64_t number,
                   uint64_t route)
{
    const struct vsr_event event = request_event(in, client, number, route);
    const int result = step_one(in, &event);
    CHECK(result == VSR_OK || result == VSR_AGAIN);
    CHECK(in->update.consumed == 1);
}

static struct instance *boot(uint32_t members)
{
    struct instance *in = instance_new(members);
    start(in);
    advance(in, 1);
    settle(in);
    CHECK(status(in).state == VSR_STATE_NORMAL);
    return in;
}

static void stop(struct instance *in)
{
    const struct vsr_event event = {VSR_EVENT_STOP, 0, 0, NULL, 0};
    for (unsigned attempt = 0;; attempt++) {
        CHECK(attempt < 1000);
        const int result = step_one(in, &event);
        CHECK(result == VSR_OK || result == VSR_AGAIN);
        if (in->update.consumed == 1)
            break;
    }
    CHECK(in->update.deadline_ns == VSR_NO_DEADLINE);
}

/* STOPPED is reached only once every operation completed and every RELEASE
 * was drained; deinit reports EBUSY until then. */
static void stop_and_drain(struct instance *in)
{
    stop(in);
    for (unsigned attempt = 0; attempt < 10000; attempt++) {
        drain(in);
        const size_t slot = first_pending(in);
        if (slot == SIZE_MAX)
            break;
        CHECK(status(in).state != VSR_STATE_STOPPED);
        CHECK(vsr_deinit(in->v) == VSR_EBUSY);
        const int result = complete(in, slot, VSR_IO_CANCELLED);
        CHECK(result == VSR_OK || result == VSR_AGAIN);
    }
    drain(in);
    CHECK(status(in).state == VSR_STATE_STOPPED);
    CHECK(outstanding_ops(in) == 0 && outstanding_leases(in) == 0);
    for (uint64_t lease = in->lease_base; lease < in->next_lease; lease++) {
        const struct graph *graph = &in->graphs[lease - in->lease_base];
        CHECK(!graph->accepted || graph->released);
    }
}

static bool power_of_two(size_t value)
{
    return value != 0 && (value & (value - 1)) == 0;
}

static int layout_of(const struct vsr_options *options,
                     struct vsr_layout *layout)
{
    *layout = (struct vsr_layout){123, 456};
    const int result = vsr_layout(options, layout);
    if (result != VSR_OK)
        CHECK(layout->size == 0 && layout->alignment == 0);
    else
        CHECK(layout->size != 0 && power_of_two(layout->alignment));
    return result;
}

static void test_layout_validation(void)
{
    struct instance *in = instance_new(3);
    struct vsr_layout layout = {123, 456};
    CHECK(vsr_layout(NULL, &layout) == VSR_EINVAL);
    CHECK(layout.size == 0 && layout.alignment == 0);
    CHECK(vsr_layout(&in->options, NULL) == VSR_EINVAL);
    CHECK(layout_of(&in->options, &layout) == VSR_OK);
    CHECK(layout.alignment == 64);
    CHECK(layout.alignment >= alignof(max_align_t));
    const struct vsr_options base = in->options;
    struct vsr_options o;

#define EXPECT(field, value, expected)                                         \
    do {                                                                       \
        o = base;                                                              \
        o.field = (value);                                                     \
        CHECK(layout_of(&o, &layout) == (expected));                           \
    } while (0)
    /* Every capacity and byte limit is positive. */
    EXPECT(limits.members, 0, VSR_EINVAL);
    EXPECT(limits.operations, 0, VSR_EINVAL);
    EXPECT(limits.input_leases, 0, VSR_EINVAL);
    EXPECT(limits.pending_requests, 0, VSR_EINVAL);
    EXPECT(limits.pending_reads, 0, VSR_EINVAL);
    EXPECT(limits.transfers, 0, VSR_EINVAL);
    EXPECT(limits.log_cache_entries, 0, VSR_EINVAL);
    EXPECT(limits.client_cache_entries, 0, VSR_EINVAL);
    EXPECT(limits.batch_entries, 0, VSR_EINVAL);
    EXPECT(limits.spans_per_blob, 0, VSR_EINVAL);
    EXPECT(limits.work_per_step, 0, VSR_EINVAL);
    EXPECT(limits.command_bytes, 0, VSR_EINVAL);
    EXPECT(limits.result_bytes, 0, VSR_EINVAL);
    EXPECT(limits.manifest_bytes, 0, VSR_EINVAL);
    EXPECT(limits.message_bytes, 0, VSR_EINVAL);
    EXPECT(limits.pinned_payload_bytes, 0, VSR_EINVAL);
    /* Identities, reserved fields, enums, and timeouts. */
    EXPECT(cluster, ((struct vsr_id){0, 0}), VSR_EINVAL);
    EXPECT(incarnation, ((struct vsr_id){0, 0}), VSR_EINVAL);
    EXPECT(replica, 0, VSR_EINVAL);
    EXPECT(seed, NULL, VSR_EINVAL);
    EXPECT(reserved, 1, VSR_EINVAL);
    EXPECT(start_mode, VSR_START_JOIN + 1, VSR_EINVAL);
    EXPECT(durability, VSR_REPLICATED + 1, VSR_EINVAL);
    EXPECT(heartbeat_ns, 0, VSR_EINVAL);
    EXPECT(heartbeat_ns, base.view_timeout_ns, VSR_EINVAL);
    EXPECT(view_timeout_ns, base.heartbeat_ns, VSR_EINVAL);
    EXPECT(view_timeout_ns, base.heartbeat_ns - 1, VSR_EINVAL);
    EXPECT(view_timeout_ns, VSR_NO_DEADLINE, VSR_EINVAL);
    EXPECT(retry_ns, 0, VSR_EINVAL);
    EXPECT(transfer_timeout_ns, 0, VSR_EINVAL);
    EXPECT(cache_line_bytes, 3, VSR_EINVAL);
    EXPECT(cache_line_bytes, 96, VSR_EINVAL);
    EXPECT(cache_line_bytes, 128, VSR_OK);
    CHECK(layout.alignment == 128);
    EXPECT(cache_line_bytes, 1, VSR_OK);
    CHECK(layout.alignment >= alignof(max_align_t));
    /* Size relations are permanent limit violations. */
    EXPECT(limits.batch_entries, base.limits.log_cache_entries + 1, VSR_ELIMIT);
    EXPECT(limits.batch_entries, base.limits.log_cache_entries, VSR_OK);
    EXPECT(limits.message_bytes,
           base.limits.command_bytes + base.limits.manifest_bytes - 1,
           VSR_ELIMIT);
    EXPECT(limits.message_bytes,
           base.limits.command_bytes + base.limits.manifest_bytes, VSR_OK);
    EXPECT(limits.command_bytes, UINT64_MAX, VSR_ELIMIT);
    EXPECT(limits.manifest_bytes, UINT64_MAX, VSR_ELIMIT);
    EXPECT(limits.result_bytes, UINT64_MAX, VSR_ELIMIT);
    EXPECT(limits.message_bytes, UINT64_MAX, VSR_ELIMIT);
    EXPECT(limits.pinned_payload_bytes, 1, VSR_ELIMIT);
    /* Documented implementation minima: input_leases >= transfers + 8 and
     * the payload budget formula, both checked exactly at the boundary. */
    EXPECT(limits.input_leases, base.limits.transfers + 7, VSR_ELIMIT);
    EXPECT(limits.input_leases, base.limits.transfers + 8, VSR_OK);
    const uint64_t minimum_payload =
        base.limits.message_bytes + 2 * base.limits.command_bytes +
        (base.limits.transfers + 3) * base.limits.manifest_bytes +
        base.limits.result_bytes;
    EXPECT(limits.pinned_payload_bytes, minimum_payload, VSR_OK);
    EXPECT(limits.pinned_payload_bytes, minimum_payload - 1, VSR_ELIMIT);
    /* Start mode and seed relations. */
    EXPECT(replica, 9, VSR_EINVAL); /* NEW requires replica in seed. */
    EXPECT(join_role, VSR_MEMBER_FULL, VSR_EINVAL);
    o = base;
    o.start_mode = VSR_START_JOIN;
    o.join_role = VSR_MEMBER_FULL;
    CHECK(layout_of(&o, &layout) == VSR_EINVAL); /* Replica in seed. */
    o.replica = 9;
    CHECK(layout_of(&o, &layout) == VSR_OK);
    o.join_role = VSR_MEMBER_WITNESS;
    CHECK(layout_of(&o, &layout) == VSR_OK);
    o.join_role = VSR_MEMBER_NONE;
    CHECK(layout_of(&o, &layout) == VSR_EINVAL);
    o.join_role = VSR_MEMBER_WITNESS + 1;
    CHECK(layout_of(&o, &layout) == VSR_EINVAL);
    o = base;
    o.start_mode = VSR_START_RECOVER;
    o.replica = 9; /* A discovery seed need not contain the replica. */
    CHECK(layout_of(&o, &layout) == VSR_OK);
    o.join_role = VSR_MEMBER_WITNESS;
    CHECK(layout_of(&o, &layout) == VSR_EINVAL);
    o = base;
    in->seed.epoch = 1; /* NEW requires an epoch-0 seed. */
    CHECK(layout_of(&o, &layout) == VSR_EINVAL);
    o.start_mode = VSR_START_RECOVER;
    CHECK(layout_of(&o, &layout) == VSR_OK);
    in->seed.epoch = 0;
    /* Membership shape. */
    in->seed.count = 2; /* count < 2f + 1 */
    CHECK(layout_of(&base, &layout) == VSR_EINVAL);
    in->seed.count = 3;
    in->seed.faults = 2;
    CHECK(layout_of(&base, &layout) == VSR_EINVAL);
    in->seed.faults = 1;
    in->members[1].role = VSR_MEMBER_WITNESS; /* Fewer than f + 1 FULL. */
    CHECK(layout_of(&base, &layout) == VSR_EINVAL);
    in->members[1].role = VSR_MEMBER_NONE;
    CHECK(layout_of(&base, &layout) == VSR_EINVAL);
    in->members[1].role = VSR_MEMBER_FULL;
    in->members[1].reserved = 1;
    CHECK(layout_of(&base, &layout) == VSR_EINVAL);
    in->members[1].reserved = 0;
    in->members[1].id = 1; /* Duplicate. */
    CHECK(layout_of(&base, &layout) == VSR_EINVAL);
    in->members[1].id = 4; /* Unsorted. */
    CHECK(layout_of(&base, &layout) == VSR_EINVAL);
    in->members[1].id = 2;
    in->members[2].id = 0;
    CHECK(layout_of(&base, &layout) == VSR_EINVAL);
    in->members[2].id = 3;
    in->seed.count = 0;
    CHECK(layout_of(&base, &layout) == VSR_EINVAL);
    in->seed.members = NULL;
    CHECK(layout_of(&base, &layout) == VSR_EINVAL);
    in->seed.members = in->members;
    in->seed.count = 3;
    in->seed.faults = 0;
    CHECK(layout_of(&base, &layout) == VSR_OK);
    in->seed.faults = 1;
    const struct vsr_member five[] = {{1, VSR_MEMBER_FULL, 0},
                                      {2, VSR_MEMBER_FULL, 0},
                                      {3, VSR_MEMBER_FULL, 0},
                                      {4, VSR_MEMBER_FULL, 0},
                                      {5, VSR_MEMBER_FULL, 0}};
    in->seed = (struct vsr_membership){0, five, 5, 2};
    CHECK(layout_of(&base, &layout) == VSR_ELIMIT); /* members limit 4. */
    o = base;
    o.limits.members = 5;
    CHECK(layout_of(&o, &layout) == VSR_OK);
    in->seed = (struct vsr_membership){0, in->members, 1, 0};
    CHECK(layout_of(&base, &layout) == VSR_OK);
#undef EXPECT
    free(in);
}

static void test_init_validation(void)
{
    struct instance *in = instance_new(3);
    struct vsr_layout layout;
    CHECK(vsr_layout(&in->options, &layout) == VSR_OK);
    const size_t size = layout.size;
    unsigned char *memory = aligned_alloc(layout.alignment, size * 2);
    CHECK(memory != NULL);
    struct vsr *out = (struct vsr *)memory;
    CHECK(vsr_init(NULL, size, &in->options, &out) == VSR_EINVAL);
    CHECK(out == NULL);
    out = (struct vsr *)memory;
    CHECK(vsr_init(memory, size, NULL, &out) == VSR_EINVAL);
    CHECK(out == NULL);
    CHECK(vsr_init(memory, size, &in->options, NULL) == VSR_EINVAL);
    out = (struct vsr *)memory;
    CHECK(vsr_init(memory + 1, size, &in->options, &out) == VSR_EINVAL);
    CHECK(out == NULL);
    out = (struct vsr *)memory;
    CHECK(vsr_init(memory, size - 1, &in->options, &out) == VSR_ELIMIT);
    CHECK(out == NULL);
    out = (struct vsr *)memory;
    CHECK(vsr_init(memory, 0, &in->options, &out) == VSR_ELIMIT);
    CHECK(out == NULL);
    struct vsr_options invalid = in->options;
    invalid.heartbeat_ns = 0;
    out = (struct vsr *)memory;
    CHECK(vsr_init(memory, size, &invalid, &out) == VSR_EINVAL);
    CHECK(out == NULL);
    invalid = in->options;
    invalid.limits.batch_entries = invalid.limits.log_cache_entries + 1;
    out = (struct vsr *)memory;
    CHECK(vsr_init(memory, size, &invalid, &out) == VSR_ELIMIT);
    CHECK(out == NULL);
    /* The arena must not overlap options, seed metadata, or out. */
    struct vsr_options *inside = (struct vsr_options *)(void *)memory;
    *inside = in->options;
    CHECK(vsr_init(memory, size, inside, &out) == VSR_EINVAL);
    struct vsr_membership *seed = (struct vsr_membership *)(void *)memory;
    *seed = in->seed;
    struct vsr_options options = in->options;
    options.seed = seed;
    out = (struct vsr *)memory;
    CHECK(vsr_init(memory, size, &options, &out) == VSR_EINVAL);
    CHECK(out == NULL);
    struct vsr_member *members = (struct vsr_member *)(void *)memory;
    memcpy(members, in->members, sizeof(in->members));
    in->seed.members = members;
    out = (struct vsr *)memory;
    CHECK(vsr_init(memory, size, &in->options, &out) == VSR_EINVAL);
    CHECK(out == NULL);
    in->seed.members = in->members;
    struct vsr **out_inside = (struct vsr **)(void *)(memory + 64);
    CHECK(vsr_init(memory, size, &in->options, out_inside) == VSR_EINVAL);
    /* A larger, aligned arena is accepted; the instance starts STARTING. */
    memset(memory, 0xa5, size * 2);
    CHECK(vsr_init(memory, size * 2, &in->options, &out) == VSR_OK);
    CHECK(out != NULL);
    struct vsr_status s;
    vsr_get_status(out, &s);
    CHECK(s.state == VSR_STATE_STARTING);
    CHECK(vsr_deinit(out) == VSR_EBUSY);
    struct vsr_op ops[2];
    struct vsr_update update = {ops, 2, 0, 0, 0, 0};
    const struct vsr_event stop_event = {VSR_EVENT_STOP, 0, 0, NULL, 0};
    CHECK(vsr_step(out, &stop_event, &update) == VSR_OK);
    CHECK(update.consumed == 1 && update.count == 0);
    CHECK(update.deadline_ns == VSR_NO_DEADLINE);
    vsr_get_status(out, &s);
    CHECK(s.state == VSR_STATE_STOPPED);
    CHECK(s.outstanding_ops == 0 && s.outstanding_leases == 0);
    CHECK(vsr_deinit(out) == VSR_OK);
    free(memory);
    free(in);
}

static void test_step_arguments(void)
{
    struct instance *in = instance_new(1);
    start(in);
    struct vsr_op ops[2];
    struct vsr_update u = {ops, 0, 5, 5, 5, 5};
    /* Invalid call arguments: EINVAL, zero counts/flags, current deadline. */
    CHECK(vsr_step_many(in->v, NULL, 0, &u) == VSR_EINVAL);
    CHECK(u.count == 0 && u.consumed == 0 && u.flags == 0);
    CHECK(u.deadline_ns == VSR_NO_DEADLINE);
    u = (struct vsr_update){NULL, 2, 5, 5, 5, 5};
    CHECK(vsr_step_many(in->v, NULL, 0, &u) == VSR_EINVAL);
    CHECK(u.count == 0 && u.consumed == 0 && u.flags == 0);
    u = (struct vsr_update){ops, 2, 5, 5, 5, 5};
    CHECK(vsr_step_many(in->v, NULL, 1, &u) == VSR_EINVAL);
    CHECK(u.count == 0 && u.consumed == 0 && u.flags == 0);
    CHECK(u.deadline_ns == VSR_NO_DEADLINE);
    /* Input and output arrays must not alias each other. */
    union {
        struct vsr_op ops[2];
        struct vsr_event events[2];
    } shared;
    memset(&shared, 0, sizeof(shared));
    shared.events[0] = time_event(1);
    u = (struct vsr_update){shared.ops, 2, 5, 5, 5, 5};
    CHECK(vsr_step_many(in->v, shared.events, 1, &u) == VSR_EINVAL);
    CHECK(u.count == 0 && u.consumed == 0 && u.flags == 0);
    /* events=NULL with count=0 is a drain, and it starts boot work. */
    CHECK(step(in, NULL, 0) == VSR_OK);
    CHECK(in->update.count == 1 && in->ops[0].type == VSR_OP_LOAD);
    CHECK(in->update.deadline_ns == VSR_NO_DEADLINE);
    advance(in, 5);
    const uint64_t deadline = in->update.deadline_ns;
    CHECK(deadline != VSR_NO_DEADLINE && deadline > 5);
    u = (struct vsr_update){ops, 0, 5, 5, 5, 5};
    CHECK(vsr_step_many(in->v, NULL, 0, &u) == VSR_EINVAL);
    CHECK(u.count == 0 && u.consumed == 0 && u.flags == 0);
    CHECK(u.deadline_ns == deadline);
    stop_and_drain(in);
    finish(in);
}

static bool membership_matches_seed(const struct vsr_membership *m,
                                    const struct instance *in)
{
    if (m == NULL || m->epoch != 0 || m->count != in->seed.count ||
        m->faults != in->seed.faults)
        return false;
    for (uint32_t i = 0; i < m->count; i++) {
        if (m->members[i].id != in->members[i].id ||
            m->members[i].role != in->members[i].role)
            return false;
    }
    return true;
}

static void test_status_and_boot(void)
{
    struct instance *in = instance_new(1);
    start(in);
    struct vsr_status s = status(in);
    CHECK(s.state == VSR_STATE_STARTING);
    CHECK(s.epoch == 0 && s.view == 0);
    CHECK(s.primary == VSR_NO_REPLICA || s.primary == 1);
    CHECK(s.committed == 0 && s.applied == 0);
    CHECK(s.stored_sequence == 0 && s.durable_sequence == 0);
    CHECK(s.checkpoint_op == 0 && s.transition_op == 0);
    CHECK(s.outstanding_ops == 0 && s.outstanding_leases == 0);
    CHECK(s.failure.code == VSR_FAILURE_NONE && s.failure.operation == 0 &&
          s.failure.operation_type == 0 && s.failure.status == 0);
    if (s.configuration != NULL) {
        CHECK(membership_matches_seed(s.configuration->current, in));
        CHECK(s.configuration->previous == NULL);
        CHECK(s.configuration->boundary == 0);
        CHECK(s.configuration->phase == VSR_EPOCH_STEADY);
    }
    /* Options and seed metadata are copied: later caller mutation is inert. */
    in->members[0].id = 99;
    in->seed.count = 0;
    /* The first step starts LOAD_RECOVERY; no deadline exists before TIME. */
    CHECK(step(in, NULL, 0) == VSR_OK);
    CHECK(in->update.count == 1);
    CHECK(in->ops[0].type == VSR_OP_LOAD && in->ops[0].arg == 0);
    const struct vsr_store_read *read = in->ops[0].data;
    CHECK(read->type == VSR_LOAD_RECOVERY);
    CHECK(read->sequence == 0 && read->first == 0 && read->end == 0);
    CHECK(read->client.hi == 0 && read->client.lo == 0);
    CHECK(read->max_count == 1);
    CHECK(read->max_bytes >= in->options.limits.manifest_bytes);
    CHECK(in->update.deadline_ns == VSR_NO_DEADLINE);
    CHECK(status(in).state == VSR_STATE_STARTING);
    /* NOT_FOUND on NEW initializes storage: transaction 1 carries IDENTITY. */
    advance(in, 10);
    CHECK(in->update.deadline_ns != VSR_NO_DEADLINE);
    CHECK(in->update.deadline_ns > 10);
    CHECK(in->update.deadline_ns <= 10 + in->options.view_timeout_ns);
    const size_t load = find_type(in, VSR_OP_LOAD);
    CHECK(load != SIZE_MAX);
    CHECK(complete(in, load, VSR_IO_NOT_FOUND) >= VSR_OK);
    const size_t store = reach(in, VSR_OP_STORE);
    const struct vsr_store *transaction = in->pending[store].data;
    CHECK(transaction->sequence == 1);
    bool identity = false;
    for (uint32_t i = 0; i < transaction->count; i++) {
        const struct vsr_change *change = &transaction->changes[i];
        if (change->type != VSR_STORE_IDENTITY)
            continue;
        CHECK(change->count == 1 && change->first == 0);
        const struct vsr_store_identity *id = change->data;
        CHECK(id->cluster.hi == 7 && id->cluster.lo == 7);
        CHECK(id->replica == 1 && id->durability == VSR_DURABLE);
        CHECK(id->reserved == 0);
        identity = true;
    }
    CHECK(identity);
    settle(in);
    s = status(in);
    CHECK(s.state == VSR_STATE_NORMAL);
    CHECK(s.epoch == 0 && s.view == 0 && s.primary == 1);
    CHECK(s.role == VSR_MEMBER_FULL);
    CHECK(s.stored_sequence >= 1 && s.durable_sequence <= s.stored_sequence);
    CHECK(s.committed == 0 && s.applied == 0);
    CHECK(s.configuration != NULL);
    in->members[0].id = 1;
    in->seed.count = 1;
    CHECK(membership_matches_seed(s.configuration->current, in));
    CHECK(s.configuration->previous == NULL);
    CHECK(s.configuration->boundary == 0);
    CHECK(s.configuration->phase == VSR_EPOCH_STEADY);
    /* One command commits, applies, and is answered on its reply route. */
    submit(in, 1, 1, 77);
    const size_t apply = reach(in, VSR_OP_APPLY);
    const struct vsr_apply *batch = in->pending[apply].data;
    CHECK(batch->batch.count == 1 && batch->through == 1);
    CHECK(batch->replay == 0 && batch->reserved == 0);
    CHECK(batch->batch.entries[0].op == 1);
    CHECK(batch->batch.entries[0].type == VSR_REQUEST_COMMAND);
    CHECK(batch->batch.entries[0].request.number == 1);
    CHECK(status(in).committed == 1 && status(in).applied == 0);
    const size_t reply = reach(in, VSR_OP_REPLY);
    CHECK(in->pending[reply].arg == 77);
    const struct vsr_reply *answer = in->pending[reply].data;
    CHECK(answer->status == VSR_REPLY_OK);
    CHECK((answer->flags & VSR_REPLY_EXECUTED) != 0);
    CHECK(answer->request.number == 1 && answer->request.client.hi == 1);
    CHECK(answer->op == 1 && answer->primary == 1);
    CHECK(answer->result.data.size == 0 && answer->result.code == 0);
    settle(in);
    s = status(in);
    CHECK(s.committed == 1 && s.applied == 1);
    CHECK(s.failure.code == VSR_FAILURE_NONE);
    stop_and_drain(in);
    finish(in);
}

/* NEW requires an empty store: a recovered row latches an identity failure,
 * and the failing LOAD is named in the status. */
static void test_new_on_existing_store(void)
{
    struct instance *in = instance_new(1);
    start(in);
    advance(in, 1);
    const size_t load = find_type(in, VSR_OP_LOAD);
    CHECK(load != SIZE_MAX);
    const uint64_t id = in->pending[load].id;
    const uint64_t lease = take_lease(in);
    struct graph *graph = graph_of(in, lease);
    struct vsr_membership membership = {0, in->members, 1, 0};
    struct vsr_epoch epoch = {&membership, NULL, 0, VSR_EPOCH_STEADY, 0};
    struct vsr_recovered recovered = {
        .identity = {{7, 7}, 1, VSR_DURABLE, 0},
        .sequence = 1,
        .log_begin = 1,
        .log_end = 1,
        .hard = {0, 0, 0, &epoch, VSR_HARD_NORMAL, VSR_MEMBER_FULL},
        .checkpoint = NULL,
    };
    graph->loaded = (struct vsr_loaded){&recovered, 1, 0, 1, 0};
    const struct vsr_event event = {VSR_EVENT_COMPLETE, VSR_IO_OK, id,
                                    &graph->loaded, lease};
    CHECK(step_one(in, &event) == VSR_OK);
    CHECK((in->update.flags & VSR_UPDATE_STATE_CHANGED) != 0);
    struct vsr_status s = status(in);
    CHECK(s.state == VSR_STATE_FAILED);
    CHECK(s.failure.code == VSR_FAILURE_IDENTITY);
    CHECK(s.failure.operation == id);
    CHECK(s.failure.operation_type == VSR_OP_LOAD);
    CHECK(s.failure.status == VSR_IO_OK);
    CHECK(in->update.deadline_ns == VSR_NO_DEADLINE);
    drain(in);
    CHECK(graph->released);
    stop_and_drain(in);
    CHECK(status(in).failure.code == VSR_FAILURE_IDENTITY);
    finish(in);
}

static void test_prefix_and_time(void)
{
    struct instance *in = boot(1);
    /* TIME must be nondecreasing; the invalid event is left at consumed. */
    struct vsr_event times[3] = {time_event(10), time_event(9), time_event(20)};
    CHECK(step(in, times, 3) == VSR_EINVAL);
    CHECK(in->update.consumed == 1);
    CHECK(in->update.deadline_ns != VSR_NO_DEADLINE);
    CHECK(step(in, &times[2], 1) == VSR_OK);
    times[1] = time_event(20); /* Equal values may repeat. */
    CHECK(step(in, &times[1], 2) == VSR_OK);
    CHECK(in->update.consumed == 2);
    const struct vsr_event bad[] = {
        time_event(VSR_NO_DEADLINE),
        {VSR_EVENT_TIME, 0, 30, in->members, 0},
        {VSR_EVENT_TIME, 0, 30, NULL, 5},
        {VSR_EVENT_TIME, VSR_IO_RETRY, 30, NULL, 0},
        {VSR_EVENT_STOP + 1, 0, 0, NULL, 0},
        {VSR_EVENT_CHECKPOINT, 0, 1, NULL, 0},
        {VSR_EVENT_STOP, 0, 1, NULL, 0},
        {VSR_EVENT_REQUEST, 0, 0, NULL, 0},
        {VSR_EVENT_MESSAGE, 0, 0, NULL, 0},
        {VSR_EVENT_CLIENT_QUERY, 0, 1, NULL, 0},
        {VSR_EVENT_READ, 0, 1, NULL, 0},
    };
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        CHECK(step_one(in, &bad[i]) == VSR_EINVAL);
        CHECK(in->update.consumed == 0);
        CHECK(in->update.count == 0);
    }
    CHECK(status(in).state == VSR_STATE_NORMAL);
    /* A valid event before an invalid one is accepted; the invalid one
     * transfers nothing, and only the suffix is resubmitted. */
    struct vsr_event events[3] = {request_event(in, 1, 1, 11),
                                  request_event(in, 2, 1, 12),
                                  request_event(in, 3, 1, 13)};
    const uint64_t rejected = events[1].lease;
    events[1].lease = 0;
    const size_t leases = outstanding_leases(in);
    CHECK(step(in, events, 3) == VSR_EINVAL);
    CHECK(in->update.consumed == 1);
    CHECK(status(in).outstanding_leases == leases + 1);
    drain(in);
    CHECK(outstanding_ops(in) >= 1); /* Work for the accepted request. */
    events[1].lease = rejected;
    CHECK(step(in, &events[1], 2) >= VSR_OK);
    CHECK(in->update.consumed == 2);
    CHECK(status(in).outstanding_leases == leases + 3);
    settle(in);
    CHECK(status(in).applied == 3);
    /* Output emitted before the invalid event stays in the update. */
    submit(in, 4, 1, 14);
    const size_t load = reach(in, VSR_OP_LOAD);
    const struct vsr_event mixed[3] = {completion(in, load, VSR_IO_OK),
                                       time_event(19), time_event(25)};
    CHECK(step(in, mixed, 3) == VSR_EINVAL);
    CHECK(in->update.consumed == 1);
    CHECK(in->update.count >= 1);
    CHECK(in->ops[0].type == VSR_OP_RELEASE &&
          in->ops[0].arg == mixed[0].lease);
    CHECK(step(in, &mixed[2], 1) >= VSR_OK);
    settle(in);
    CHECK(status(in).applied == 4);
    /* An oversized command is permanently inadmissible. */
    struct vsr_event big = request_event(in, 5, 1, 15);
    struct graph *graph = graph_of(in, big.lease);
    graph->blob.size = in->options.limits.command_bytes + 1;
    graph->span.size = graph->blob.size;
    CHECK(step_one(in, &big) == VSR_ELIMIT);
    CHECK(in->update.consumed == 0);
    untake_lease(in, big.lease);
    stop_and_drain(in);
    finish(in);
}

/* With one output slot the core reports OUTPUT_FULL and MORE until every
 * queued descriptor has been drained with zero-input calls. */
static void test_output_capacity(void)
{
    struct instance *in = instance_new(1);
    in->capacity = 1;
    start(in);
    advance(in, 1);
    settle(in);
    CHECK(status(in).state == VSR_STATE_NORMAL);
    submit(in, 1, 1, 1);
    settle(in);
    CHECK(status(in).applied == 1);
    CHECK(in->output_full_seen);
    CHECK(step(in, NULL, 0) == VSR_OK);
    CHECK(in->update.count == 0);
    stop_and_drain(in);
    finish(in);
}

static void *page_alloc(size_t *size)
{
    const long page = sysconf(_SC_PAGESIZE);
    CHECK(page > 0);
    *size = (size_t)page;
    void *memory = mmap(NULL, *size, PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    CHECK(memory != MAP_FAILED);
    return memory;
}

/* Pinned graphs live in read-only pages: any core write faults. */
struct readonly_request {
    struct vsr_request request;
    struct vsr_blob blob;
    struct vsr_span span;
    unsigned char bytes[16];
};

struct readonly_applied {
    struct vsr_applied applied;
    struct vsr_value values[CAPACITY];
};

static void test_readonly_graphs(void)
{
    struct instance *in = boot(1);
    size_t size;
    struct readonly_request *request = page_alloc(&size);
    memset(request->bytes, 0x5a, sizeof(request->bytes));
    request->span = (struct vsr_span){request->bytes, sizeof(request->bytes)};
    request->blob =
        (struct vsr_blob){&request->span, sizeof(request->bytes), 1, 0};
    request->request = (struct vsr_request){
        {{5, 5}, 1}, 0, VSR_REQUEST_COMMAND, 0, &request->blob};
    CHECK(mprotect(request, size, PROT_READ) == 0);
    const uint64_t lease = take_lease(in);
    const struct vsr_event event = {VSR_EVENT_REQUEST, 0, 21, &request->request,
                                    lease};
    CHECK(step_one(in, &event) >= VSR_OK);
    CHECK(in->update.consumed == 1);
    const size_t apply = reach(in, VSR_OP_APPLY);
    const struct vsr_apply *batch = in->pending[apply].data;
    CHECK(batch->batch.count == 1);
    CHECK(batch->batch.entries[0].request.client.hi == 5);
    const struct vsr_blob *body = batch->batch.entries[0].body;
    CHECK(body->size == sizeof(request->bytes));
    CHECK(body->spans[0].data == request->bytes);
    size_t applied_size;
    struct readonly_applied *applied = page_alloc(&applied_size);
    memset(applied, 0, sizeof(*applied));
    applied->applied = (struct vsr_applied){applied->values, 1, 0};
    CHECK(mprotect(applied, applied_size, PROT_READ) == 0);
    const uint64_t result_lease = take_lease(in);
    const struct vsr_event done = {VSR_EVENT_COMPLETE, VSR_IO_OK,
                                   in->pending[apply].id, &applied->applied,
                                   result_lease};
    CHECK(step_one(in, &done) >= VSR_OK);
    CHECK(in->update.consumed == 1);
    settle(in);
    CHECK(status(in).applied == 1);
    CHECK(graph_of(in, lease)->accepted &&
          graph_of(in, result_lease)->accepted);
    /* Both pins may outlive execution; STOPPED guarantees their RELEASE. */
    stop_and_drain(in);
    CHECK(munmap(request, size) == 0);
    CHECK(munmap(applied, applied_size) == 0);
    finish(in);
}

static void test_ownership(void)
{
    struct instance *in = boot(1);
    /* Lease/data pairing and uniqueness among outstanding pins. */
    struct vsr_event first = request_event(in, 1, 1, 31);
    struct vsr_event second = request_event(in, 2, 1, 32);
    const uint64_t spare = second.lease;
    second.lease = first.lease;
    CHECK(step_one(in, &first) >= VSR_OK);
    CHECK(in->update.consumed == 1);
    CHECK(step_one(in, &second) == VSR_EINVAL);
    CHECK(in->update.consumed == 0);
    second.lease = spare;
    CHECK(step_one(in, &second) >= VSR_OK);
    CHECK(in->update.consumed == 1);
    settle(in);
    /* A released ID may be reused once its RELEASE has been processed. */
    uint64_t released = 0;
    for (uint64_t lease = in->lease_base; lease < in->next_lease; lease++) {
        if (graph_of(in, lease)->released)
            released = lease;
    }
    CHECK(released != 0);
    struct graph *graph = graph_of(in, released);
    build_request(graph, 3, 1);
    graph->accepted = graph->released = false;
    const struct vsr_event third = {VSR_EVENT_REQUEST, 0, 33, &graph->request,
                                    released};
    CHECK(step_one(in, &third) >= VSR_OK);
    CHECK(in->update.consumed == 1);
    settle(in);
    CHECK(status(in).applied == 3);
    /* Unknown, zero, and already completed operation IDs are rejected. */
    submit(in, 4, 1, 34);
    const size_t store = reach(in, VSR_OP_STORE);
    const uint64_t id = in->pending[store].id;
    struct vsr_event bogus = {VSR_EVENT_COMPLETE, VSR_IO_OK, id + 1000000, NULL,
                              0};
    CHECK(step_one(in, &bogus) == VSR_EINVAL && in->update.consumed == 0);
    bogus.id = 0;
    CHECK(step_one(in, &bogus) == VSR_EINVAL && in->update.consumed == 0);
    bogus.id = UINT64_MAX;
    CHECK(step_one(in, &bogus) == VSR_EINVAL && in->update.consumed == 0);
    /* Completion shape: STORE carries no data, success or failure. */
    const uint64_t lease = take_lease(in);
    graph = graph_of(in, lease);
    graph->loaded = (struct vsr_loaded){NULL, 1, 0, 0, 0};
    struct vsr_event shaped = {VSR_EVENT_COMPLETE, VSR_IO_OK, id,
                               &graph->loaded, lease};
    CHECK(step_one(in, &shaped) == VSR_EINVAL && in->update.consumed == 0);
    shaped.status = VSR_IO_FAILED;
    CHECK(step_one(in, &shaped) == VSR_EINVAL && in->update.consumed == 0);
    shaped = (struct vsr_event){VSR_EVENT_COMPLETE, VSR_IO_OK, id, NULL, lease};
    CHECK(step_one(in, &shaped) == VSR_EINVAL && in->update.consumed == 0);
    shaped.status = VSR_IO_CANCELLED + 1;
    shaped.lease = 0;
    CHECK(step_one(in, &shaped) == VSR_EINVAL && in->update.consumed == 0);
    CHECK(status(in).state == VSR_STATE_NORMAL);
    CHECK(status(in).failure.code == VSR_FAILURE_NONE);
    /* The rejected completions kept the pin: the real one is still valid. */
    CHECK(find_op(in, id) == store);
    CHECK(complete(in, store, VSR_IO_OK) >= VSR_OK);
    CHECK(step_one(in, &bogus) == VSR_EINVAL);
    bogus.id = id; /* Duplicate completion of a finished operation. */
    CHECK(step_one(in, &bogus) == VSR_EINVAL && in->update.consumed == 0);
    /* LOAD success requires vsr_loaded with its own lease; APPLY success
     * requires vsr_applied. A failure never carries data. */
    settle(in);
    submit(in, 5, 1, 35);
    const size_t load = reach(in, VSR_OP_LOAD);
    const uint64_t load_id = in->pending[load].id;
    shaped =
        (struct vsr_event){VSR_EVENT_COMPLETE, VSR_IO_OK, load_id, NULL, 0};
    CHECK(step_one(in, &shaped) == VSR_EINVAL && in->update.consumed == 0);
    const uint64_t extra = take_lease(in);
    graph = graph_of(in, extra);
    graph->loaded = (struct vsr_loaded){NULL, 1, 0, 0, 0};
    shaped = (struct vsr_event){VSR_EVENT_COMPLETE, VSR_IO_NOT_FOUND, load_id,
                                &graph->loaded, extra};
    CHECK(step_one(in, &shaped) == VSR_EINVAL && in->update.consumed == 0);
    CHECK(find_op(in, load_id) == load);
    const size_t apply = reach(in, VSR_OP_APPLY);
    const uint64_t apply_id = in->pending[apply].id;
    shaped =
        (struct vsr_event){VSR_EVENT_COMPLETE, VSR_IO_OK, apply_id, NULL, 0};
    CHECK(step_one(in, &shaped) == VSR_EINVAL && in->update.consumed == 0);
    CHECK(status(in).state == VSR_STATE_NORMAL);
    settle(in);
    CHECK(status(in).applied == 5);
    stop_and_drain(in);
    finish(in);
}

/* A structurally valid but inconsistent APPLY result (wrong count) is
 * consumed and fences the replica with an application failure. */
static void test_inconsistent_apply(void)
{
    struct instance *in = boot(1);
    submit(in, 1, 1, 41);
    const size_t apply = reach(in, VSR_OP_APPLY);
    const uint64_t id = in->pending[apply].id;
    const uint64_t lease = take_lease(in);
    struct graph *graph = graph_of(in, lease);
    memset(graph->values, 0, sizeof(graph->values));
    graph->applied = (struct vsr_applied){graph->values, 2, 0};
    const struct vsr_event event = {VSR_EVENT_COMPLETE, VSR_IO_OK, id,
                                    &graph->applied, lease};
    CHECK(step_one(in, &event) >= VSR_OK);
    CHECK(in->update.consumed == 1);
    const struct vsr_status s = status(in);
    CHECK(s.state == VSR_STATE_FAILED);
    CHECK(s.failure.code == VSR_FAILURE_APPLICATION);
    CHECK(s.failure.operation == id);
    CHECK(s.failure.operation_type == VSR_OP_APPLY);
    CHECK(s.applied == 0);
    drain(in);
    CHECK(graph->released);
    stop_and_drain(in);
    finish(in);
}

/* With the minimum lease budget and every lease held by unfinished work, a
 * new REQUEST is refused with AGAIN/INPUT_BLOCKED and transfers nothing,
 * while TIME, COMPLETE, and STOP still go through reserved capacity. */
static struct instance *saturated(uint64_t *blocked_client)
{
    struct instance *in = instance_new(1);
    in->options.limits.transfers = 1;
    in->options.limits.input_leases = 9;
    in->options.limits.operations = 32;
    in->options.limits.pending_requests = 32;
    in->options.limits.log_cache_entries = 32;
    in->options.limits.client_cache_entries = 32;
    in->options.limits.pinned_payload_bytes = 1u << 16;
    start(in);
    advance(in, 1);
    settle(in);
    CHECK(status(in).state == VSR_STATE_NORMAL);
    uint64_t client = 1;
    for (;; client++) {
        CHECK(client < 64);
        struct vsr_event event = request_event(in, client, 1, client);
        const size_t leases = outstanding_leases(in);
        int result = step_one(in, &event);
        CHECK(result == VSR_OK || result == VSR_AGAIN);
        if (in->update.consumed == 1)
            continue;
        CHECK(result == VSR_AGAIN);
        CHECK((in->update.flags & VSR_UPDATE_INPUT_BLOCKED) != 0);
        CHECK(status(in).outstanding_leases == leases);
        if ((in->update.flags & VSR_UPDATE_MORE) != 0) {
            /* Queued output (such as a RELEASE) precedes the retry. */
            drain(in);
            result = step_one(in, &event);
            CHECK(result == VSR_OK || result == VSR_AGAIN);
            if (in->update.consumed == 1)
                continue;
            CHECK((in->update.flags & VSR_UPDATE_INPUT_BLOCKED) != 0);
        }
        CHECK((in->update.flags & VSR_UPDATE_MORE) == 0);
        untake_lease(in, event.lease);
        break;
    }
    CHECK(client > 1);
    CHECK(status(in).outstanding_leases >= 2);
    *blocked_client = client;
    return in;
}

static void test_backpressure_release(void)
{
    uint64_t client;
    struct instance *in = saturated(&client);
    const struct vsr_event tick = time_event(2);
    CHECK(step_one(in, &tick) >= VSR_OK && in->update.consumed == 1);
    /* Still blocked after time alone. */
    struct vsr_event retry = request_event(in, client, 1, client);
    CHECK(step_one(in, &retry) == VSR_AGAIN && in->update.consumed == 0);
    CHECK((in->update.flags & VSR_UPDATE_INPUT_BLOCKED) != 0);
    /* Completions are admitted through reserved capacity. */
    const size_t slot = first_pending(in);
    CHECK(slot != SIZE_MAX);
    const int result = complete(in, slot, VSR_IO_OK);
    CHECK(result == VSR_OK || result == VSR_AGAIN);
    settle(in);
    CHECK(status(in).applied == client - 1);
    CHECK(step_one(in, &retry) >= VSR_OK && in->update.consumed == 1);
    settle(in);
    CHECK(status(in).applied == client);
    stop_and_drain(in);
    finish(in);
}

static void test_backpressure_stop(void)
{
    uint64_t client;
    struct instance *in = saturated(&client);
    stop(in);
    CHECK(status(in).state == VSR_STATE_STOPPING);
    stop_and_drain(in);
    finish(in);
}

/* The failure a cancelled operation of this type latches, per the header. */
static uint32_t fencing_failure(uint32_t type)
{
    switch (type) {
    case VSR_OP_STORE:
    case VSR_OP_SYNC:
        return VSR_FAILURE_STORAGE;
    case VSR_OP_APPLY:
    case VSR_OP_SNAPSHOT_INSTALL:
        return VSR_FAILURE_APPLICATION;
    case VSR_OP_SNAPSHOT_SYNC:
        return VSR_FAILURE_SNAPSHOT;
    default:
        return VSR_FAILURE_NONE;
    }
}

static void test_stop_lifecycle(void)
{
    struct instance *in = boot(1);
    submit(in, 1, 1, 51);
    drain(in);
    CHECK(outstanding_ops(in) >= 1);
    const struct vsr_event stop_event = {VSR_EVENT_STOP, 0, 0, NULL, 0};
    CHECK(step_one(in, &stop_event) >= VSR_OK);
    CHECK(in->update.consumed == 1);
    CHECK((in->update.flags & VSR_UPDATE_STATE_CHANGED) != 0);
    CHECK(in->update.deadline_ns == VSR_NO_DEADLINE);
    CHECK(status(in).state == VSR_STATE_STOPPING);
    CHECK(vsr_deinit(in->v) == VSR_EBUSY);
    /* Idempotent: a second STOP is consumed without effect. */
    CHECK(step_one(in, &stop_event) >= VSR_OK);
    CHECK(in->update.consumed == 1);
    CHECK(status(in).state == VSR_STATE_STOPPING);
    /* After STOP only TIME, COMPLETE, STOP, and drains are accepted. */
    const struct vsr_event tick = time_event(2);
    CHECK(step_one(in, &tick) >= VSR_OK && in->update.consumed == 1);
    CHECK(in->update.deadline_ns == VSR_NO_DEADLINE);
    struct vsr_event request = request_event(in, 2, 1, 52);
    CHECK(step_one(in, &request) == VSR_EINVAL);
    CHECK(in->update.consumed == 0);
    untake_lease(in, request.lease);
    const struct vsr_id client = {2, 9};
    const struct vsr_read_barrier barrier = {0, VSR_NO_DEADLINE,
                                             VSR_READ_CAUSAL, 0};
    const struct vsr_message message = {{7, 7},         0, 0, 2,
                                        VSR_MSG_COMMIT, 0, 0, NULL};
    const uint64_t leases = in->next_lease;
    in->next_lease += 3;
    const struct vsr_event others[] = {
        {VSR_EVENT_CHECKPOINT, 0, 0, NULL, 0},
        {VSR_EVENT_CLIENT_QUERY, 0, 53, &client, leases},
        {VSR_EVENT_READ, 0, 54, &barrier, leases + 1},
        {VSR_EVENT_MESSAGE, 0, 0, &message, leases + 2},
    };
    for (size_t i = 0; i < sizeof(others) / sizeof(others[0]); i++) {
        CHECK(step_one(in, &others[i]) == VSR_EINVAL);
        CHECK(in->update.consumed == 0);
    }
    /* Loads still complete successfully; every other pending operation is
     * cancelled. STOPPED follows the last completion once its RELEASE
     * outputs have been drained. Cancelling a fencing operation latches its
     * failure even while STOPPING. */
    uint32_t expected_failure = VSR_FAILURE_NONE;
    for (unsigned attempt = 0; attempt < 1000; attempt++) {
        drain(in);
        const size_t slot = first_pending(in);
        if (slot == SIZE_MAX)
            break;
        CHECK(status(in).state == VSR_STATE_STOPPING);
        CHECK(vsr_deinit(in->v) == VSR_EBUSY);
        const bool load = in->pending[slot].type == VSR_OP_LOAD;
        if (!load && expected_failure == VSR_FAILURE_NONE)
            expected_failure = fencing_failure(in->pending[slot].type);
        in->capacity = 1;
        CHECK(complete(in, slot, load ? VSR_IO_OK : VSR_IO_CANCELLED) >=
              VSR_OK);
        CHECK(status(in).failure.code == expected_failure);
        if (outstanding_ops(in) == 0 && outstanding_leases(in) != 0) {
            CHECK(status(in).state == VSR_STATE_STOPPING);
            CHECK(vsr_deinit(in->v) == VSR_EBUSY);
        }
        in->capacity = CAPACITY;
    }
    drain(in);
    struct vsr_status s = status(in);
    CHECK(s.state == VSR_STATE_STOPPED);
    CHECK(s.outstanding_ops == 0 && s.outstanding_leases == 0);
    CHECK(s.failure.code == expected_failure);
    /* STOPPED still accepts STOP, TIME, and drains; nothing else. */
    CHECK(step_one(in, &stop_event) == VSR_OK && in->update.consumed == 1);
    const struct vsr_event later = time_event(3);
    CHECK(step_one(in, &later) == VSR_OK && in->update.consumed == 1);
    CHECK(step(in, NULL, 0) == VSR_OK && in->update.count == 0);
    request = request_event(in, 3, 1, 55);
    CHECK(step_one(in, &request) == VSR_EINVAL && in->update.consumed == 0);
    untake_lease(in, request.lease);
    CHECK(status(in).state == VSR_STATE_STOPPED);
    finish(in);
}

/* A failed STORE latches VSR_FAILURE_STORAGE with the operation's identity,
 * stops protocol work, keeps consuming well-formed input without admitting
 * work, and still drains to STOPPED with the first failure preserved. */
static void test_failure_latch(void)
{
    struct instance *in = boot(1);
    submit(in, 1, 1, 61);
    const size_t store = reach(in, VSR_OP_STORE);
    const uint64_t id = in->pending[store].id;
    CHECK(complete(in, store, VSR_IO_FAILED) >= VSR_OK);
    CHECK((in->update.flags & VSR_UPDATE_STATE_CHANGED) != 0);
    CHECK(in->update.deadline_ns == VSR_NO_DEADLINE);
    struct vsr_status s = status(in);
    CHECK(s.state == VSR_STATE_FAILED);
    CHECK(s.failure.code == VSR_FAILURE_STORAGE);
    CHECK(s.failure.operation == id);
    CHECK(s.failure.operation_type == VSR_OP_STORE);
    CHECK(s.failure.status == VSR_IO_FAILED);
    const struct vsr_failure first = s.failure;
    const size_t ids_before = in->id_count;
    drain(in);
    CHECK(in->id_count == ids_before);
    /* FAILED consumes other well-formed events without new work. */
    const struct vsr_event request = request_event(in, 2, 1, 62);
    CHECK(step_one(in, &request) >= VSR_OK);
    CHECK(in->update.consumed == 1);
    drain(in);
    CHECK(in->id_count == ids_before);
    CHECK(graph_of(in, request.lease)->released);
    const struct vsr_event hint = {VSR_EVENT_CHECKPOINT, 0, 0, NULL, 0};
    CHECK(step_one(in, &hint) == VSR_OK && in->update.consumed == 1);
    const struct vsr_event tick = time_event(2);
    CHECK(step_one(in, &tick) == VSR_OK && in->update.consumed == 1);
    /* A later unsuccessful completion does not replace the first failure. */
    const size_t next = first_pending(in);
    if (next != SIZE_MAX) {
        CHECK(complete(in, next, VSR_IO_CANCELLED) >= VSR_OK);
        s = status(in);
        CHECK(memcmp(&first, &s.failure, sizeof(first)) == 0);
    }
    CHECK(vsr_deinit(in->v) == VSR_EBUSY);
    stop_and_drain(in);
    s = status(in);
    CHECK(s.state == VSR_STATE_STOPPED);
    CHECK(memcmp(&first, &s.failure, sizeof(first)) == 0);
    CHECK(in->id_count == ids_before);
    finish(in);
}

/* LOAD corruption during boot fences storage with the completion status. */
static void test_load_corruption(void)
{
    struct instance *in = instance_new(1);
    start(in);
    advance(in, 1);
    const size_t load = find_type(in, VSR_OP_LOAD);
    CHECK(load != SIZE_MAX);
    const uint64_t id = in->pending[load].id;
    CHECK(complete(in, load, VSR_IO_CORRUPT) >= VSR_OK);
    const struct vsr_status s = status(in);
    CHECK(s.state == VSR_STATE_FAILED);
    CHECK(s.failure.code == VSR_FAILURE_STORAGE);
    CHECK(s.failure.operation == id);
    CHECK(s.failure.operation_type == VSR_OP_LOAD);
    CHECK(s.failure.status == VSR_IO_CORRUPT);
    stop_and_drain(in);
    finish(in);
}

int main(void)
{
    test_layout_validation();
    test_init_validation();
    test_step_arguments();
    test_status_and_boot();
    test_new_on_existing_store();
    test_prefix_and_time();
    test_output_capacity();
    test_readonly_graphs();
    test_ownership();
    test_inconsistent_apply();
    test_backpressure_release();
    test_backpressure_stop();
    test_stop_lifecycle();
    test_failure_latch();
    test_load_corruption();
    return 0;
}
