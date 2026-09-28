#include "config.h"

#include "internal.h"
#include "lib/check.h"
#include "objects.h"
#include "validate.h"

#include <stdalign.h>
#include <stdlib.h>
#include <string.h>

/* Descriptor copies (src/owned.c) and logical equality (src/objects.c,
 * src/validate.c) are exercised on graphs shaped exactly like the configured
 * limits. The runtime is driven through the same protocol double as
 * tests/unit/runtime.c so that operation construction is observed in
 * isolation from the consensus state machine. */
struct mock_protocol {
    struct vsr_epoch epoch;
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
    (void)v;
    (void)event;
    (void)lease;
    return VSR_OK;
}

void vsr_protocol_complete(struct vsr *v, struct vsr_operation *operation,
                           const struct vsr_event *event, uint32_t lease)
{
    (void)v;
    (void)operation;
    (void)event;
    (void)lease;
}

bool vsr_protocol_poll(struct vsr *v)
{
    (void)v;
    return false;
}

bool vsr_protocol_relieve_pressure(struct vsr *v)
{
    (void)v;
    return false;
}

uint64_t vsr_protocol_deadline(const struct vsr *v)
{
    (void)v;
    return VSR_NO_DEADLINE;
}

void vsr_protocol_stop(struct vsr *v)
{
    (void)v;
}

enum { MEMBERS = 4, BATCH = 4, SPANS = 4, MANIFEST = 64, COMMAND = 64 };

/* One complete input graph at every configured limit: memberships with
 * limits.members members, blobs of exactly manifest_bytes/command_bytes across
 * spans_per_blob spans, batches of batch_entries entries and records. */
struct graph {
    struct vsr_member current_members[MEMBERS];
    struct vsr_member previous_members[MEMBERS];
    struct vsr_membership current;
    struct vsr_membership previous;
    struct vsr_epoch epoch;
    unsigned char manifest_bytes[MANIFEST];
    struct vsr_span manifest_spans[SPANS];
    struct vsr_checkpoint checkpoint;
    struct vsr_checkpoint basis;
    unsigned char command_bytes[COMMAND];
    struct vsr_span command_spans[SPANS];
    struct vsr_blob command;
    struct vsr_check_epoch check;
    struct vsr_entry entries[BATCH];
    struct vsr_client_record clients[BATCH];
    struct vsr_hard_state hard;
    struct vsr_store_identity identity;
};

static void spans_init(struct vsr_span *spans, unsigned char *bytes,
                       size_t total, uint32_t count, unsigned char seed)
{
    const size_t each = total / count;
    for (size_t i = 0; i < total; i++)
        bytes[i] = (unsigned char)(seed + i);
    for (uint32_t i = 0; i < count; i++)
        spans[i] = (struct vsr_span){bytes + (size_t)i * each, each};
}

static void graph_init(struct graph *g)
{
    memset(g, 0, sizeof(*g));
    for (uint32_t i = 0; i < MEMBERS; i++) {
        g->current_members[i] = (struct vsr_member){i + 1, VSR_MEMBER_FULL, 0};
        g->previous_members[i] = g->current_members[i];
    }
    g->current = (struct vsr_membership){1, g->current_members, MEMBERS, 1};
    g->previous = (struct vsr_membership){0, g->previous_members, MEMBERS, 1};
    g->epoch =
        (struct vsr_epoch){&g->current, &g->previous, 5, VSR_EPOCH_STEADY, 0};
    spans_init(g->manifest_spans, g->manifest_bytes, MANIFEST, SPANS, 1);
    g->checkpoint = (struct vsr_checkpoint){
        {1, 1}, 10, 2, &g->epoch, {g->manifest_spans, MANIFEST, SPANS, 0}};
    g->basis = g->checkpoint;
    g->basis.id = (struct vsr_id){1, 2};
    g->basis.op = 8;
    spans_init(g->command_spans, g->command_bytes, COMMAND, SPANS, 100);
    g->command = (struct vsr_blob){g->command_spans, COMMAND, SPANS, 0};
    g->check.epoch = 1;
    g->entries[0] = (struct vsr_entry){
        11, 1, 2, {{7, 1}, 1}, VSR_REQUEST_COMMAND, 0, &g->command};
    g->entries[1] = (struct vsr_entry){
        12, 1, 2, {{7, 2}, 2}, VSR_REQUEST_RECONFIGURE, 0, &g->current};
    g->entries[2] = (struct vsr_entry){
        13, 1, 2, {{7, 3}, 3}, VSR_REQUEST_CHECK_EPOCH, 0, &g->check};
    g->entries[3] =
        (struct vsr_entry){14, 1, 2, {{0, 0}, 0}, VSR_REQUEST_NOOP, 0, NULL};
    for (uint32_t i = 0; i < BATCH; i++)
        g->clients[i] = (struct vsr_client_record){
            {{9, i + 1}, i + 1}, 11 + i, {g->command, (int32_t)i, 0}};
    g->hard = (struct vsr_hard_state){
        2, 2, 10, &g->epoch, VSR_HARD_NORMAL, VSR_MEMBER_FULL};
    g->identity = (struct vsr_store_identity){{1, 1}, 1, VSR_DURABLE, 0};
}

struct fixture {
    struct vsr_member member;
    struct vsr_membership membership;
    struct vsr_options options;
    struct vsr_layout layout;
    void *memory;
    struct vsr *v;
    struct graph graph;
};

static void fixture_start(struct fixture *f)
{
    memset(f, 0, sizeof(*f));
    f->member = (struct vsr_member){1, VSR_MEMBER_FULL, 0};
    f->membership = (struct vsr_membership){0, &f->member, 1, 0};
    f->options = (struct vsr_options){
        .cluster = {1, 1},
        .incarnation = {2, 1},
        .replica = 1,
        .seed = &f->membership,
        .limits =
            {
                .members = MEMBERS,
                .operations = 8,
                .input_leases = 16,
                .pending_requests = 4,
                .pending_reads = 4,
                .transfers = 4,
                .log_cache_entries = 8,
                .client_cache_entries = 8,
                .batch_entries = BATCH,
                .spans_per_blob = SPANS,
                .work_per_step = 64,
                .command_bytes = COMMAND,
                .result_bytes = 32,
                .manifest_bytes = MANIFEST,
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
    CHECK(vsr_layout(&f->options, &f->layout) == VSR_OK);
    f->memory = aligned_alloc(f->layout.alignment, f->layout.size);
    CHECK(f->memory != NULL);
    CHECK(vsr_init(f->memory, f->layout.size, &f->options, &f->v) == VSR_OK);
    graph_init(&f->graph);
}

/* Published operations are drained and cancelled so that STOPPED is reached
 * with nothing outstanding, as in tests/unit/runtime.c. */
static void fixture_stop(struct fixture *f)
{
    const struct vsr_event stop = {.type = VSR_EVENT_STOP};
    struct vsr_op outputs[32];
    struct vsr_update update = {.ops = outputs, .capacity = 32};
    bool submitted = false;
    for (uint32_t iteration = 0; iteration < 1000; iteration++) {
        int result = vsr_step(f->v, submitted ? NULL : &stop, &update);
        CHECK(result >= VSR_OK);
        submitted = submitted || update.consumed != 0;
        for (uint32_t i = 0; i < f->v->options.limits.operations; i++) {
            const struct vsr_operation *operation = &f->v->operations[i];
            if (operation->state != VSR_SLOT_ACTIVE)
                continue;
            const struct vsr_event complete = {
                .type = VSR_EVENT_COMPLETE,
                .status = VSR_IO_CANCELLED,
                .id = operation->output.id,
            };
            do {
                result = vsr_step(f->v, &complete, &update);
                CHECK(result >= VSR_OK);
            } while (update.consumed == 0);
        }
        if (f->v->status.state == VSR_STATE_STOPPED)
            break;
    }
    CHECK(f->v->status.state == VSR_STATE_STOPPED);
    CHECK(vsr_deinit(f->v) == VSR_OK);
    free(f->memory);
}

/* Every copied descriptor lives in the operation's own metadata slice. */
static bool owned(const struct vsr_operation *op, const void *pointer)
{
    const unsigned char *p = pointer;
    return p >= op->memory && p < op->memory + op->capacity;
}

static struct vsr_operation *acquire(struct fixture *f, uint32_t type)
{
    struct vsr_operation *op = vsr_operation_acquire(f->v, type, 0, 0, 0);
    CHECK(op != NULL && op->state == VSR_SLOT_BUILDING && op->used == 0);
    return op;
}

/* Copy, then check that the root and every array moved into the operation
 * while payload bytes stayed where the adapter put them. */
static const void *copy(struct vsr_operation *op, const void *data)
{
    CHECK(vsr_operation_copy(op, data) == VSR_OK);
    CHECK(op->output.data != data && owned(op, op->output.data));
    CHECK(op->used != 0);
    return op->output.data;
}

static void check_blob(const struct vsr_operation *op,
                       const struct vsr_blob *copied,
                       const struct vsr_blob *source)
{
    CHECK(vsr_blob_equal(copied, source));
    CHECK(copied->count == source->count && copied->size == source->size);
    if (source->count == 0) {
        CHECK(copied->spans == NULL);
        return;
    }
    CHECK(copied->spans != source->spans && owned(op, copied->spans));
    for (uint32_t i = 0; i < source->count; i++) {
        CHECK(copied->spans[i].data == source->spans[i].data);
        CHECK(copied->spans[i].size == source->spans[i].size);
    }
}

static void check_membership(const struct vsr_operation *op,
                             const struct vsr_membership *copied,
                             const struct vsr_membership *source)
{
    CHECK(copied != source && owned(op, copied));
    CHECK(copied->members != source->members && owned(op, copied->members));
    CHECK(vsr_membership_equal(copied, source));
}

static void check_epoch(const struct vsr_operation *op,
                        const struct vsr_epoch *copied,
                        const struct vsr_epoch *source)
{
    CHECK(copied != source && owned(op, copied));
    CHECK(vsr_epoch_equal(copied, source));
    check_membership(op, copied->current, source->current);
    if (source->previous == NULL)
        CHECK(copied->previous == NULL);
    else
        check_membership(op, copied->previous, source->previous);
}

static void check_checkpoint(const struct vsr_operation *op,
                             const struct vsr_checkpoint *copied,
                             const struct vsr_checkpoint *source)
{
    CHECK(copied != source && owned(op, copied));
    CHECK(vsr_id_equal(copied->id, source->id) && copied->op == source->op &&
          copied->view == source->view);
    check_epoch(op, copied->epoch, source->epoch);
    check_blob(op, &copied->manifest, &source->manifest);
}

static void check_entries(const struct vsr_operation *op,
                          const struct vsr_entries *copied,
                          const struct vsr_entries *source)
{
    CHECK(copied->count == source->count);
    if (source->count == 0) {
        CHECK(copied->entries == NULL);
        return;
    }
    CHECK(copied->entries != source->entries && owned(op, copied->entries));
    for (uint32_t i = 0; i < source->count; i++) {
        const struct vsr_entry *a = &copied->entries[i];
        const struct vsr_entry *b = &source->entries[i];
        CHECK(vsr_entry_equal(a, b));
        switch (b->type) {
        case VSR_REQUEST_COMMAND:
            CHECK(a->body != b->body && owned(op, a->body));
            check_blob(op, a->body, b->body);
            break;
        case VSR_REQUEST_RECONFIGURE:
            check_membership(op, a->body, b->body);
            break;
        case VSR_REQUEST_CHECK_EPOCH:
            CHECK(a->body != b->body && owned(op, a->body));
            CHECK(((const struct vsr_check_epoch *)a->body)->epoch ==
                  ((const struct vsr_check_epoch *)b->body)->epoch);
            break;
        default:
            CHECK(a->body == NULL);
            break;
        }
    }
}

static void check_log_state(const struct vsr_operation *op,
                            const struct vsr_log_state *copied,
                            const struct vsr_log_state *source)
{
    CHECK(vsr_revision_equal(copied->revision, source->revision));
    CHECK(copied->view == source->view &&
          copied->last_normal_view == source->last_normal_view &&
          copied->committed == source->committed &&
          copied->log_begin == source->log_begin &&
          copied->log_end == source->log_end);
    check_epoch(op, copied->epoch, source->epoch);
    check_entries(op, &copied->entries, &source->entries);
    if (source->checkpoint == NULL)
        CHECK(copied->checkpoint == NULL);
    else
        check_checkpoint(op, copied->checkpoint, source->checkpoint);
}

/* CAPTURE/FETCH/SYNC/INSTALL/DROP tasks: the template and the differential
 * basis each carry a full epoch (current and previous membership at
 * limits.members) and a manifest of exactly manifest_bytes across
 * spans_per_blob spans. A genesis INSTALL carries no checkpoint at all. */
static void snapshot_tasks(void)
{
    struct fixture f;
    fixture_start(&f);
    struct graph *g = &f.graph;
    const struct vsr_snapshot_task fetch = {8, 0, 2, &g->checkpoint, &g->basis};
    const uint32_t types[] = {VSR_OP_SNAPSHOT_CAPTURE, VSR_OP_SNAPSHOT_FETCH,
                              VSR_OP_SNAPSHOT_SYNC, VSR_OP_SNAPSHOT_INSTALL,
                              VSR_OP_SNAPSHOT_DROP};
    for (size_t i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        struct vsr_operation *op = acquire(&f, types[i]);
        const struct vsr_snapshot_task *task = copy(op, &fetch);
        CHECK(task->op == 8 && task->sequence == 0 && task->peer == 2);
        check_checkpoint(op, task->checkpoint, &g->checkpoint);
        check_checkpoint(op, task->basis, &g->basis);
        /* The checkpoint and basis graphs are distinct copies. */
        CHECK(task->checkpoint->epoch != task->basis->epoch);
        CHECK(task->checkpoint->manifest.spans != task->basis->manifest.spans);
        vsr_operation_publish(f.v, op);
    }
    {
        const struct vsr_snapshot_task genesis = {0, 0, 0, NULL, NULL};
        struct vsr_operation *op = acquire(&f, VSR_OP_SNAPSHOT_INSTALL);
        const struct vsr_snapshot_task *task = copy(op, &genesis);
        CHECK(task->checkpoint == NULL && task->basis == NULL);
        vsr_operation_abort(f.v, op);
    }
    {
        /* A checkpoint without an epoch descriptor is not a copyable graph. */
        struct vsr_checkpoint bare = g->checkpoint;
        bare.epoch = NULL;
        const struct vsr_snapshot_task task = {10, 0, 0, &bare, NULL};
        struct vsr_operation *op = acquire(&f, VSR_OP_SNAPSHOT_CAPTURE);
        CHECK(vsr_operation_copy(op, &task) == VSR_ELIMIT);
        CHECK(op->used == 0 && op->state == VSR_SLOT_BUILDING);
        vsr_operation_abort(f.v, op);
    }
    fixture_stop(&f);
}

/* An internal STORE uses each change type at most once, with at most one of
 * PUBLISH/RESTORE, and batch_entries entries and records. */
static void store_changes(void)
{
    struct fixture f;
    fixture_start(&f);
    struct graph *g = &f.graph;
    const struct vsr_change changes[] = {
        {VSR_STORE_IDENTITY, 1, 0, &g->identity},
        {VSR_STORE_TRUNCATE, 0, 11, (void *)g},
        {VSR_STORE_APPEND, BATCH, 11, g->entries},
        {VSR_STORE_CLIENTS, BATCH, 0, g->clients},
        {VSR_STORE_HARD_STATE, 1, 0, &g->hard},
        {VSR_STORE_PUBLISH_CHECKPOINT, 1, 0, &g->checkpoint},
        {VSR_STORE_TRIM, 0, 9, (void *)g},
    };
    const uint32_t count = sizeof(changes) / sizeof(changes[0]);
    const struct vsr_store store = {3, changes, count, 0};
    struct vsr_operation *op = acquire(&f, VSR_OP_STORE);
    const struct vsr_store *copied = copy(op, &store);
    CHECK(copied->sequence == 3 && copied->count == count);
    CHECK(copied->changes != changes && owned(op, copied->changes));
    for (uint32_t i = 0; i < count; i++) {
        const struct vsr_change *c = &copied->changes[i];
        CHECK(c->type == changes[i].type && c->count == changes[i].count &&
              c->first == changes[i].first);
        switch (c->type) {
        case VSR_STORE_IDENTITY:
            CHECK(c->data != &g->identity && owned(op, c->data));
            CHECK(memcmp(c->data, &g->identity, sizeof(g->identity)) == 0);
            break;
        case VSR_STORE_APPEND: {
            const struct vsr_entries copied_batch = {c->data, c->count, 0};
            const struct vsr_entries batch = {g->entries, BATCH, 0};
            check_entries(op, &copied_batch, &batch);
            break;
        }
        case VSR_STORE_CLIENTS: {
            const struct vsr_client_record *records = c->data;
            CHECK(records != g->clients && owned(op, records));
            for (uint32_t j = 0; j < BATCH; j++) {
                CHECK(vsr_request_id_equal(records[j].request,
                                           g->clients[j].request));
                CHECK(records[j].op == g->clients[j].op);
                CHECK(
                    vsr_value_equal(&records[j].result, &g->clients[j].result));
                check_blob(op, &records[j].result.data,
                           &g->clients[j].result.data);
            }
            break;
        }
        case VSR_STORE_HARD_STATE: {
            const struct vsr_hard_state *hard = c->data;
            CHECK(hard != &g->hard && owned(op, hard));
            CHECK(hard->view == g->hard.view &&
                  hard->committed == g->hard.committed &&
                  hard->state == g->hard.state && hard->role == g->hard.role);
            check_epoch(op, hard->epoch, &g->epoch);
            break;
        }
        case VSR_STORE_PUBLISH_CHECKPOINT:
            check_checkpoint(op, c->data, &g->checkpoint);
            break;
        default:
            /* Range changes carry no data, whatever the caller passed. */
            CHECK(c->data == NULL);
            break;
        }
    }
    vsr_operation_publish(f.v, op);

    /* RESTORE shares the single checkpoint descriptor slot with PUBLISH. */
    {
        const struct vsr_change restore[] = {
            {VSR_STORE_RESTORE_CHECKPOINT, 1, 0, &g->checkpoint},
            {VSR_STORE_HARD_STATE, 1, 0, &g->hard},
        };
        const struct vsr_store transaction = {4, restore, 2, 0};
        struct vsr_operation *o = acquire(&f, VSR_OP_STORE);
        const struct vsr_store *r = copy(o, &transaction);
        check_checkpoint(o, r->changes[0].data, &g->checkpoint);
        vsr_operation_abort(f.v, o);
    }

    /* Rejected shapes leave the operation untouched and reusable. */
    struct vsr_operation *o = acquire(&f, VSR_OP_STORE);
    {
        const struct vsr_change twice[] = {{VSR_STORE_TRIM, 0, 9, NULL},
                                           {VSR_STORE_TRIM, 0, 10, NULL}};
        const struct vsr_store transaction = {5, twice, 2, 0};
        CHECK(vsr_operation_copy(o, &transaction) == VSR_ELIMIT);
        CHECK(o->used == 0 && o->state == VSR_SLOT_BUILDING);
    }
    {
        const struct vsr_change both[] = {
            {VSR_STORE_PUBLISH_CHECKPOINT, 1, 0, &g->checkpoint},
            {VSR_STORE_RESTORE_CHECKPOINT, 1, 0, &g->checkpoint}};
        const struct vsr_store transaction = {5, both, 2, 0};
        CHECK(vsr_operation_copy(o, &transaction) == VSR_ELIMIT);
        CHECK(o->used == 0);
    }
    {
        const struct vsr_change unknown = {VSR_STORE_IDENTITY + 1, 0, 9, NULL};
        const struct vsr_store transaction = {5, &unknown, 1, 0};
        CHECK(vsr_operation_copy(o, &transaction) == VSR_ELIMIT);
        CHECK(o->used == 0);
    }
    {
        struct vsr_entry bad = g->entries[0];
        bad.type = VSR_REQUEST_NOOP + 1;
        const struct vsr_change append = {VSR_STORE_APPEND, 1, 11, &bad};
        const struct vsr_store transaction = {5, &append, 1, 0};
        CHECK(vsr_operation_copy(o, &transaction) == VSR_ELIMIT);
        CHECK(o->used == 0);
    }
    {
        /* The same slot then accepts a valid transaction. */
        const struct vsr_change trim = {VSR_STORE_TRIM, 0, 9, NULL};
        const struct vsr_store transaction = {5, &trim, 1, 0};
        CHECK(vsr_operation_copy(o, &transaction) == VSR_OK);
        CHECK(o->used != 0);
    }
    vsr_operation_abort(f.v, o);
    fixture_stop(&f);
}

/* Every message body shape: log offers with a checkpoint and an entry chunk,
 * recovery bodies with and without an offer, fetches, chunks, epoch
 * descriptors, checkpoint advertisements, nonces, and bodiless envelopes. */
static void send_messages(void)
{
    struct fixture f;
    fixture_start(&f);
    struct graph *g = &f.graph;
    const struct vsr_prepare prepare = {{g->entries, BATCH, 0}, 12};
    const struct vsr_log_state state = {
        {{2, 1}, 7},   2, 2, 12, 11, 15, &g->epoch, {g->entries, BATCH, 0},
        &g->checkpoint};
    const struct vsr_recovery response = {{{2, 1}, 3}, &state};
    const struct vsr_recovery request = {{{2, 1}, 4}, NULL};
    const struct vsr_fetch fetch = {{{2, 1}, 5}, {{2, 1}, 7}, 11, 15,
                                    128,         4,           0};
    const struct vsr_state_chunk chunk = {{{2, 1}, 5}, state, 11, 15};
    const struct vsr_nonce nonce = {{2, 1}, 6};
    const struct {
        uint32_t type;
        const void *body;
    } cases[] = {
        {VSR_MSG_PREPARE, &prepare},
        {VSR_MSG_PREPARE_OK, NULL},
        {VSR_MSG_COMMIT, NULL},
        {VSR_MSG_START_VIEW_CHANGE, NULL},
        {VSR_MSG_DO_VIEW_CHANGE, &state},
        {VSR_MSG_START_VIEW, &state},
        {VSR_MSG_RECOVERY, &request},
        {VSR_MSG_RECOVERY_RESPONSE, &response},
        {VSR_MSG_GET_STATE, &fetch},
        {VSR_MSG_NEW_STATE, &chunk},
        {VSR_MSG_GET_LOG, &fetch},
        {VSR_MSG_LOG, &chunk},
        {VSR_MSG_STATE_UNAVAILABLE, &chunk},
        {VSR_MSG_START_EPOCH, &g->epoch},
        {VSR_MSG_EPOCH_STARTED, NULL},
        {VSR_MSG_NEW_EPOCH, &g->epoch},
        {VSR_MSG_CHECKPOINT, &g->checkpoint},
        {VSR_MSG_READ_PROBE, &nonce},
        {VSR_MSG_READ_ACK, &nonce},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        const struct vsr_message message = {
            {1, 1}, 1, 2, 1, cases[i].type, 0, 14, cases[i].body};
        struct vsr_operation *op = acquire(&f, VSR_OP_SEND);
        const struct vsr_message *m = copy(op, &message);
        CHECK(m->type == cases[i].type && m->number == 14 && m->view == 2);
        if (cases[i].body == NULL) {
            CHECK(m->body == NULL);
        } else {
            CHECK(m->body != cases[i].body && owned(op, m->body));
        }
        switch (cases[i].type) {
        case VSR_MSG_PREPARE: {
            const struct vsr_prepare *p = m->body;
            CHECK(p->committed == 12);
            check_entries(op, &p->batch, &prepare.batch);
            break;
        }
        case VSR_MSG_DO_VIEW_CHANGE:
        case VSR_MSG_START_VIEW:
            check_log_state(op, m->body, &state);
            break;
        case VSR_MSG_RECOVERY: {
            const struct vsr_recovery *r = m->body;
            CHECK(vsr_nonce_equal(r->nonce, request.nonce) && r->state == NULL);
            break;
        }
        case VSR_MSG_RECOVERY_RESPONSE: {
            const struct vsr_recovery *r = m->body;
            CHECK(vsr_nonce_equal(r->nonce, response.nonce));
            CHECK(r->state != &state && owned(op, r->state));
            check_log_state(op, r->state, &state);
            break;
        }
        case VSR_MSG_GET_STATE:
        case VSR_MSG_GET_LOG:
            CHECK(memcmp(m->body, &fetch, sizeof(fetch)) == 0);
            break;
        case VSR_MSG_NEW_STATE:
        case VSR_MSG_LOG:
        case VSR_MSG_STATE_UNAVAILABLE: {
            const struct vsr_state_chunk *c = m->body;
            CHECK(vsr_nonce_equal(c->nonce, chunk.nonce) && c->first == 11 &&
                  c->next == 15);
            check_log_state(op, &c->state, &state);
            break;
        }
        case VSR_MSG_START_EPOCH:
        case VSR_MSG_NEW_EPOCH:
            check_epoch(op, m->body, &g->epoch);
            break;
        case VSR_MSG_CHECKPOINT:
            check_checkpoint(op, m->body, &g->checkpoint);
            break;
        case VSR_MSG_READ_PROBE:
        case VSR_MSG_READ_ACK:
            CHECK(vsr_nonce_equal(*(const struct vsr_nonce *)m->body, nonce));
            break;
        default:
            break;
        }
        vsr_operation_abort(f.v, op);
    }
    {
        const struct vsr_message unknown = {
            {1, 1}, 1, 2, 1, VSR_MSG_READ_ACK + 1, 0, 0, NULL};
        struct vsr_operation *op = acquire(&f, VSR_OP_SEND);
        CHECK(vsr_operation_copy(op, &unknown) == VSR_ELIMIT);
        CHECK(op->used == 0);
        vsr_operation_abort(f.v, op);
    }
    fixture_stop(&f);
}

/* REPLY, APPLY, READ_READY and LOAD roots, and the data-less operations. */
static void other_operations(void)
{
    struct fixture f;
    fixture_start(&f);
    struct graph *g = &f.graph;
    {
        const struct vsr_reply reply = {{{7, 1}, 1},
                                        11,
                                        2,
                                        1,
                                        &g->current,
                                        {g->command, 3, 0},
                                        VSR_REPLY_OK,
                                        VSR_REPLY_EXECUTED};
        struct vsr_operation *op = acquire(&f, VSR_OP_REPLY);
        const struct vsr_reply *r = copy(op, &reply);
        CHECK(r->status == VSR_REPLY_OK && r->flags == VSR_REPLY_EXECUTED &&
              r->op == 11 && r->result.code == 3);
        check_membership(op, r->membership, &g->current);
        check_blob(op, &r->result.data, &g->command);
        vsr_operation_abort(f.v, op);
        /* Absent membership and an empty result stay NULL. */
        const struct vsr_reply bare = {
            {{7, 1}, 1},    0, 2, 1, NULL, {{NULL, 0, 0, 0}, 0, 0},
            VSR_REPLY_BUSY, 0};
        op = acquire(&f, VSR_OP_REPLY);
        r = copy(op, &bare);
        CHECK(r->membership == NULL && r->result.data.spans == NULL);
        vsr_operation_abort(f.v, op);
    }
    {
        const struct vsr_apply apply = {{g->entries, BATCH, 0}, 14, 1, 0};
        struct vsr_operation *op = acquire(&f, VSR_OP_APPLY);
        const struct vsr_apply *a = copy(op, &apply);
        CHECK(a->through == 14 && a->replay == 1);
        check_entries(op, &a->batch, &apply.batch);
        vsr_operation_abort(f.v, op);
    }
    {
        const struct vsr_read_fence fence = {5, 9, 1, 2};
        struct vsr_operation *op = acquire(&f, VSR_OP_READ_READY);
        const struct vsr_read_fence *copied = copy(op, &fence);
        CHECK(memcmp(copied, &fence, sizeof(fence)) == 0);
        vsr_operation_abort(f.v, op);
    }
    {
        const struct vsr_store_read read = {7,   11,           15, {0, 0},
                                            128, VSR_LOAD_LOG, 4};
        struct vsr_operation *op = acquire(&f, VSR_OP_LOAD);
        const struct vsr_store_read *copied = copy(op, &read);
        CHECK(memcmp(copied, &read, sizeof(read)) == 0);
        vsr_operation_abort(f.v, op);
    }
    {
        /* SYNC/RECLAIM carry no descriptor; other types require one. */
        struct vsr_operation *op = acquire(&f, VSR_OP_SYNC);
        CHECK(vsr_operation_copy(op, NULL) == VSR_OK);
        CHECK(op->output.data == NULL && op->used == 0);
        CHECK(vsr_operation_copy(op, g) == VSR_ELIMIT);
        vsr_operation_abort(f.v, op);
        op = acquire(&f, VSR_OP_RECLAIM);
        CHECK(vsr_operation_copy(op, NULL) == VSR_OK);
        CHECK(vsr_operation_copy(op, g) == VSR_ELIMIT);
        vsr_operation_abort(f.v, op);
        op = acquire(&f, VSR_OP_READ_READY);
        CHECK(vsr_operation_copy(op, NULL) == VSR_EINVAL);
        /* RELEASE never carries a copied graph. */
        op->output.type = VSR_OP_RELEASE;
        CHECK(vsr_operation_copy(op, g) == VSR_ELIMIT);
        op->output.type = VSR_OP_READ_READY;
        vsr_operation_abort(f.v, op);
    }
    {
        /* Only a BUILDING operation accepts a copy. */
        const struct vsr_read_fence fence = {5, 9, 1, 2};
        CHECK(vsr_operation_copy(NULL, &fence) == VSR_EINVAL);
        struct vsr_operation *op = acquire(&f, VSR_OP_READ_READY);
        CHECK(vsr_operation_copy(op, &fence) == VSR_OK);
        vsr_operation_publish(f.v, op);
        CHECK(vsr_operation_copy(op, &fence) == VSR_EINVAL);
        struct vsr_operation *freed = acquire(&f, VSR_OP_READ_READY);
        vsr_operation_abort(f.v, freed);
        CHECK(vsr_operation_copy(freed, &fence) == VSR_EINVAL);
    }
    fixture_stop(&f);
}

/* Graphs beyond the planner's bound are refused as a whole: the operation's
 * metadata offset is restored, the slot stays BUILDING, and after an abort the
 * same slot copies a graph at the limit again. */
static void oversized_graphs(void)
{
    static struct vsr_member many[1024];
    static struct vsr_span many_spans[1024];
    static unsigned char many_bytes[1024];
    struct fixture f;
    fixture_start(&f);
    struct graph *g = &f.graph;
    for (uint32_t i = 0; i < 1024; i++)
        many[i] = (struct vsr_member){i + 1, VSR_MEMBER_FULL, 0};
    spans_init(many_spans, many_bytes, 1024, 1024, 0);
    struct vsr_membership huge = {1, many, 1024, 1};
    struct vsr_epoch epoch = g->epoch;
    epoch.current = &huge;
    struct vsr_checkpoint checkpoint = g->checkpoint;
    checkpoint.epoch = &epoch;
    const struct vsr_snapshot_task task = {10, 0, 2, &checkpoint, &g->basis};
    struct vsr_operation *op = acquire(&f, VSR_OP_SNAPSHOT_FETCH);
    CHECK(vsr_operation_copy(op, &task) == VSR_ELIMIT);
    CHECK(op->used == 0 && op->state == VSR_SLOT_BUILDING);
    struct vsr_checkpoint wide = g->checkpoint;
    wide.manifest = (struct vsr_blob){many_spans, 1024, 1024, 0};
    const struct vsr_snapshot_task spans = {10, 0, 2, &wide, NULL};
    CHECK(vsr_operation_copy(op, &spans) == VSR_ELIMIT);
    CHECK(op->used == 0);
    /* A blob whose span count cannot even be sized is refused the same way. */
    wide.manifest.count = UINT32_MAX;
    CHECK(vsr_operation_copy(op, &spans) == VSR_ELIMIT);
    CHECK(op->used == 0);
    const struct vsr_snapshot_task fits = {8, 0, 2, &g->checkpoint, &g->basis};
    const struct vsr_snapshot_task *copied = copy(op, &fits);
    check_checkpoint(op, copied->checkpoint, &g->checkpoint);
    vsr_operation_abort(f.v, op);
    op = acquire(&f, VSR_OP_SNAPSHOT_FETCH);
    copied = copy(op, &fits);
    check_checkpoint(op, copied->basis, &g->basis);
    vsr_operation_abort(f.v, op);
    fixture_stop(&f);
}

/* Object equality is fieldwise and logical: never memcmp over padding or
 * pointers, and span boundaries do not affect blob equality. */
static void equality(void)
{
    struct graph g;
    graph_init(&g);
    unsigned char other_bytes[MANIFEST];
    struct vsr_span one = {other_bytes, MANIFEST};
    struct vsr_span three[3] = {{other_bytes, 10},
                                {other_bytes + 10, 30},
                                {other_bytes + 40, MANIFEST - 40}};
    memcpy(other_bytes, g.manifest_bytes, MANIFEST);
    const struct vsr_blob single = {&one, MANIFEST, 1, 0};
    const struct vsr_blob split = {three, MANIFEST, 3, 0};
    const struct vsr_blob empty = {NULL, 0, 0, 0};
    CHECK(vsr_blob_equal(&g.checkpoint.manifest, &single));
    CHECK(vsr_blob_equal(&single, &split));
    CHECK(vsr_blob_equal(&split, &g.checkpoint.manifest));
    CHECK(vsr_blob_equal(&empty, &empty));
    CHECK(!vsr_blob_equal(&empty, &single));
    other_bytes[MANIFEST - 1] ^= 1;
    CHECK(!vsr_blob_equal(&single, &g.checkpoint.manifest));
    CHECK(!vsr_blob_equal(&split, &g.checkpoint.manifest));
    other_bytes[MANIFEST - 1] ^= 1;
    struct vsr_blob shorter = single;
    shorter.size = MANIFEST - 1;
    one.size = MANIFEST - 1;
    CHECK(!vsr_blob_equal(&shorter, &g.checkpoint.manifest));
    one.size = MANIFEST;

    struct vsr_member members[MEMBERS];
    memcpy(members, g.current_members, sizeof(members));
    struct vsr_membership same = {1, members, MEMBERS, 1};
    CHECK(vsr_membership_equal(&same, &g.current));
    CHECK(vsr_membership_equal(NULL, NULL));
    CHECK(!vsr_membership_equal(NULL, &same));
    CHECK(!vsr_membership_equal(&same, NULL));
    same.epoch = 2;
    CHECK(!vsr_membership_equal(&same, &g.current));
    same.epoch = 1;
    same.count = MEMBERS - 1;
    CHECK(!vsr_membership_equal(&same, &g.current));
    same.count = MEMBERS;
    same.faults = 0;
    CHECK(!vsr_membership_equal(&same, &g.current));
    same.faults = 1;
    members[MEMBERS - 1].id = 99;
    CHECK(!vsr_membership_equal(&same, &g.current));
    members[MEMBERS - 1].id = MEMBERS;
    members[MEMBERS - 1].role = VSR_MEMBER_WITNESS;
    CHECK(!vsr_membership_equal(&same, &g.current));
    members[MEMBERS - 1].role = VSR_MEMBER_FULL;
    members[MEMBERS - 1].reserved = 7; /* Padding never matters. */
    CHECK(vsr_membership_equal(&same, &g.current));
    members[MEMBERS - 1].reserved = 0;

    struct vsr_membership previous = g.previous;
    struct vsr_epoch epoch = {&same, &previous, 5, VSR_EPOCH_STEADY, 0};
    CHECK(vsr_epoch_equal(&epoch, &g.epoch));
    CHECK(vsr_epoch_equal(NULL, NULL));
    CHECK(!vsr_epoch_equal(NULL, &epoch) && !vsr_epoch_equal(&epoch, NULL));
    epoch.previous = NULL;
    CHECK(!vsr_epoch_equal(&epoch, &g.epoch));
    CHECK(!vsr_epoch_equal(&g.epoch, &epoch));
    epoch.previous = &previous;
    epoch.boundary = 6;
    CHECK(!vsr_epoch_equal(&epoch, &g.epoch));
    epoch.boundary = 5;
    epoch.phase = VSR_EPOCH_INSTALLED;
    CHECK(!vsr_epoch_equal(&epoch, &g.epoch));
    epoch.phase = VSR_EPOCH_STEADY;
    previous.faults = 0;
    CHECK(!vsr_epoch_equal(&epoch, &g.epoch));
    previous.faults = 1;
    epoch.reserved = 3; /* Reserved fields are not identity either. */
    CHECK(vsr_epoch_equal(&epoch, &g.epoch));

    /* Requests and entries: the routing epoch is not identity, bodies are. */
    struct vsr_request a = {{{7, 1}, 1}, 0, VSR_REQUEST_COMMAND, 0, &split};
    struct vsr_request b = {{{7, 1}, 1}, 9, VSR_REQUEST_COMMAND, 0, &single};
    CHECK(vsr_request_equal(&a, &b));
    b.id.number = 2;
    CHECK(!vsr_request_equal(&a, &b));
    b.id.number = 1;
    b.type = VSR_REQUEST_RECONFIGURE;
    b.body = &same;
    CHECK(!vsr_request_equal(&a, &b));
    a.type = VSR_REQUEST_RECONFIGURE;
    a.body = &g.current;
    CHECK(vsr_request_equal(&a, &b));
    struct vsr_entry x = g.entries[0];
    struct vsr_entry y = g.entries[0];
    y.body = &single;
    CHECK(!vsr_entry_equal(&x, &y));
    y.body = &g.command;
    CHECK(vsr_entry_equal(&x, &y));
    y.view = 3;
    CHECK(!vsr_entry_equal(&x, &y));
    struct vsr_value p = {split, 4, 0};
    struct vsr_value q = {single, 4, 1};
    CHECK(vsr_value_equal(&p, &q));
    q.code = 5;
    CHECK(!vsr_value_equal(&p, &q));
}

/* A CAPTURE result must describe the same logical boundary as its template:
 * "structurally valid but inconsistent storage/application results fence the
 * instance". Each way an otherwise valid epoch can differ is an inconsistent
 * snapshot, while an equal graph at different addresses is consistent. */
static void capture_epoch_consistency(void)
{
    struct fixture f;
    fixture_start(&f);
    struct graph *g = &f.graph;
    struct vsr_checkpoint template = {
        {0, 0}, 10, 2, &g->epoch, {NULL, 0, 0, 0}};
    const struct vsr_snapshot_task task = {10, 3, 0, &template, NULL};
    const struct vsr_op operation = {VSR_OP_SNAPSHOT_CAPTURE, 0, 7, &task, 0};
    struct vsr_member members[MEMBERS + 1];
    struct vsr_member previous_members[MEMBERS];
    memcpy(members, g->current_members, sizeof(g->current_members));
    members[MEMBERS] = (struct vsr_member){MEMBERS + 1, VSR_MEMBER_FULL, 0};
    memcpy(previous_members, g->previous_members, sizeof(previous_members));
    struct vsr_membership current = {1, members, MEMBERS, 1};
    struct vsr_membership previous = {0, previous_members, MEMBERS, 1};
    struct vsr_membership older = {0, previous_members, MEMBERS, 1};
    struct vsr_epoch epoch = {&current, &previous, 5, VSR_EPOCH_STEADY, 0};
    struct vsr_checkpoint result = {{5, 5}, 10, 2, &epoch, {NULL, 0, 0, 0}};
    const struct vsr_event event = {VSR_EVENT_COMPLETE, VSR_IO_OK, 7, &result,
                                    9};
    struct vsr_validation v;
#define VALIDATE(expected)                                                     \
    do {                                                                       \
        v = (struct vsr_validation){.limits = &f.options.limits};              \
        CHECK(vsr_validate_event(&v, &event, &operation) == VSR_OK);           \
        CHECK(v.failure_code == (expected));                                   \
    } while (0)
    VALIDATE(VSR_FAILURE_NONE);
    current.epoch = 2;
    previous.epoch = 1;
    VALIDATE(VSR_FAILURE_SNAPSHOT);
    current.epoch = 1;
    previous.epoch = 0;
    current.count = MEMBERS - 1;
    VALIDATE(VSR_FAILURE_SNAPSHOT);
    current.count = MEMBERS;
    current.faults = 0;
    VALIDATE(VSR_FAILURE_SNAPSHOT);
    current.faults = 1;
    members[MEMBERS - 1].id = MEMBERS + 1;
    VALIDATE(VSR_FAILURE_SNAPSHOT);
    members[MEMBERS - 1].id = MEMBERS;
    members[MEMBERS - 1].role = VSR_MEMBER_WITNESS;
    VALIDATE(VSR_FAILURE_SNAPSHOT);
    members[MEMBERS - 1].role = VSR_MEMBER_FULL;
    epoch.boundary = 6;
    VALIDATE(VSR_FAILURE_SNAPSHOT);
    epoch.boundary = 5;
    epoch.phase = VSR_EPOCH_INSTALLED;
    VALIDATE(VSR_FAILURE_SNAPSHOT);
    epoch.phase = VSR_EPOCH_STEADY;
    previous_members[0].role = VSR_MEMBER_WITNESS;
    VALIDATE(VSR_FAILURE_SNAPSHOT);
    previous_members[0].role = VSR_MEMBER_FULL;
    older.faults = 0;
    epoch.previous = &older;
    VALIDATE(VSR_FAILURE_SNAPSHOT);
    epoch.previous = &previous;
    VALIDATE(VSR_FAILURE_NONE);
    /* A boundary or view mismatch against the template is inconsistent too. */
    result.view = 3;
    VALIDATE(VSR_FAILURE_SNAPSHOT);
    result.view = 2;
    result.op = 11;
    VALIDATE(VSR_FAILURE_SNAPSHOT);
    result.op = 10;
    VALIDATE(VSR_FAILURE_NONE);
#undef VALIDATE
    fixture_stop(&f);
}

int main(void)
{
    snapshot_tasks();
    store_changes();
    send_messages();
    other_operations();
    oversized_graphs();
    equality();
    capture_epoch_consistency();
    return 0;
}
