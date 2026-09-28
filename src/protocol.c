#include "protocol.h"
#include "checked.h"
#include "config.h"
#include "epochs.h"
#include "extension.h"

#include <stdalign.h>
#include <string.h>

/* No state here grows with the durable history. The store is the log and client
 * index; these arrays are bounded working sets and completion scoreboards. */
enum {
    BOOT_LOAD,
    BOOT_LOADING,
    BOOT_CREATE,
    BOOT_PERSIST,
    BOOT_INSTALL,
    BOOT_INSTALLING,
    BOOT_READY,
    BOOT_RECOVER
};

struct protocol_plan {
    size_t size, alignment, current, previous, peers, log, clients, routes;
    size_t transactions, batch, results, extension, extension_size;
};

static bool slice(struct protocol_plan *p, size_t n, size_t size, size_t *at)
{
    size_t pad = (p->alignment - p->size % p->alignment) % p->alignment;
    size_t bytes;
    return vsr_size_add(p->size, pad, at) && vsr_size_mul(n, size, &bytes) &&
           vsr_size_add(*at, bytes, &p->size);
}

static bool plan(const struct vsr_options *o, struct protocol_plan *p)
{
    memset(p, 0, sizeof(*p));
    p->alignment = o->cache_line_bytes == 0 ? 64 : o->cache_line_bytes;
    if (p->alignment < alignof(max_align_t))
        p->alignment = alignof(max_align_t);
    p->size = sizeof(struct vsr_protocol);
    size_t extension_alignment;
    if (vsr_extension_size(o, &p->extension_size, &extension_alignment) !=
        VSR_OK)
        return false;
    if (extension_alignment > p->alignment)
        p->alignment = extension_alignment;
    return o->limits.operations != UINT32_MAX &&
           slice(p, o->limits.members, sizeof(struct vsr_member),
                 &p->current) &&
           slice(p, o->limits.members, sizeof(struct vsr_member),
                 &p->previous) &&
           slice(p, o->limits.members, sizeof(struct vsr_peer), &p->peers) &&
           slice(p, o->limits.log_cache_entries, sizeof(struct vsr_log_slot),
                 &p->log) &&
           slice(p, o->limits.client_cache_entries,
                 sizeof(struct vsr_client_slot), &p->clients) &&
           slice(p, o->limits.pending_requests, sizeof(struct vsr_route),
                 &p->routes) &&
           slice(p, (size_t)o->limits.operations + 1,
                 sizeof(struct vsr_transaction), &p->transactions) &&
           slice(p, o->limits.batch_entries, sizeof(struct vsr_entry),
                 &p->batch) &&
           slice(p, o->limits.batch_entries, sizeof(struct vsr_client_record),
                 &p->results) &&
           slice(p, 1, p->extension_size, &p->extension);
}

int vsr_protocol_size(const struct vsr_options *o, size_t *size,
                      size_t *alignment)
{
    struct protocol_plan p;
    if (!plan(o, &p))
        return VSR_ELIMIT;
    *size = p.size;
    *alignment = p.alignment;
    return VSR_OK;
}

void vsr_protocol_history_changed(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    if (p->history_generation == UINT64_MAX) {
        vsr_fail(v, VSR_FAILURE_EXHAUSTED, NULL, VSR_IO_OK);
        return;
    }
    p->history_generation++;
    memset(p->clients, 0,
           (size_t)v->options.limits.client_cache_entries *
               sizeof(*p->clients));
}

void vsr_protocol_configuration(struct vsr *v, const struct vsr_epoch *epoch)
{
    struct vsr_protocol *p = vsr_protocol(v);
    if (p->current.epoch != epoch->current->epoch) {
        p->proposed_boundary = 0;
        vsr_protocol_history_changed(v);
    }
    p->current = *epoch->current;
    memmove(p->current_members, epoch->current->members,
            (size_t)p->current.count * sizeof(*p->current_members));
    p->current.members = p->current_members;
    p->epoch = *epoch;
    p->epoch.current = &p->current;
    if (epoch->previous != NULL) {
        p->previous = *epoch->previous;
        memmove(p->previous_members, epoch->previous->members,
                (size_t)p->previous.count * sizeof(*p->previous_members));
        p->previous.members = p->previous_members;
        p->epoch.previous = &p->previous;
    }
    p->self = vsr_member_index(&p->current, v->options.replica);
    v->status.configuration = &p->epoch;
    v->status.epoch = p->current.epoch;
    v->status.transition_op = p->epoch.boundary;
    v->status.primary = vsr_primary(&p->current, v->status.view);
    p->peer_cursor = 0;
    memset(p->peers, 0, (size_t)v->options.limits.members * sizeof(*p->peers));
    for (uint32_t i = 0; i < p->current.count; ++i) {
        p->peers[i].id = p->current.members[i].id;
        p->peers[i].heartbeat = true;
        p->peers[i].retry_at = VSR_NO_DEADLINE;
    }
    vsr_changed(v);
}

void vsr_protocol_init(struct vsr *v, void *memory, size_t size)
{
    (void)size;
    struct protocol_plan a;
    if (!plan(&v->options, &a))
        return;
    struct vsr_protocol *p = memory;
    unsigned char *b = memory;
    p->current_members = (void *)(b + a.current);
    p->previous_members = (void *)(b + a.previous);
    p->peers = (void *)(b + a.peers);
    p->log = (void *)(b + a.log);
    p->clients = (void *)(b + a.clients);
    p->routes = (void *)(b + a.routes);
    p->transactions = (void *)(b + a.transactions);
    p->batch = (void *)(b + a.batch);
    p->results = (void *)(b + a.results);
    p->transaction_capacity = v->options.limits.operations + 1u;
    p->next_sequence = p->log_begin = p->readable_begin = p->log_end =
        p->written_end = p->stable_end = 1;
    p->results_lease = VSR_INDEX_NONE;
    p->heartbeat_at = p->election_at = p->retry_at = p->append_at =
        VSR_NO_DEADLINE;
    for (uint32_t i = 0; i < v->options.limits.log_cache_entries; ++i)
        p->log[i].lease = VSR_INDEX_NONE;
    for (uint32_t i = 0; i < v->options.limits.pending_requests; ++i) {
        p->routes[i].lease = p->routes[i].result_lease = VSR_INDEX_NONE;
    }
    const struct vsr_epoch epoch = {v->options.seed, NULL, 0, VSR_EPOCH_STEADY,
                                    0};
    vsr_protocol_configuration(v, &epoch);
    /* A replica absent from its seed warms as a learner. RECOVER may name
     * the role to resume with when no store survives; a persisted role
     * replaces it once loaded, and a witness maintains no application state. */
    v->status.role =
        p->self != VSR_INDEX_NONE ? p->current.members[p->self].role
        : v->options.join_role != VSR_MEMBER_NONE ? v->options.join_role
                                                  : VSR_MEMBER_WITNESS;
    vsr_extension_init(v, b + a.extension, a.extension_size);
}

uint64_t vsr_protocol_available_bytes(const struct vsr *v)
{
    return v->options.limits.pinned_payload_bytes - v->payload_bytes -
           v->reserved_bytes;
}

bool vsr_protocol_emit(struct vsr *v, uint32_t type, uint64_t arg, uint64_t tag,
                       const void *data, const uint32_t *leases, uint32_t count,
                       uint64_t completion_bytes)
{
    struct vsr_operation *op =
        vsr_operation_acquire(v, type, arg, tag, completion_bytes);
    if (op == NULL)
        return false;
    if (vsr_operation_copy(op, data) != VSR_OK) {
        vsr_operation_abort(v, op);
        vsr_fail(v, VSR_FAILURE_INVARIANT, NULL, VSR_IO_OK);
        return false;
    }
    for (uint32_t i = 0; i < count; ++i) {
        if (!vsr_operation_hold(v, op, leases[i])) {
            vsr_operation_abort(v, op);
            return false;
        }
    }
    vsr_operation_publish(v, op);
    return true;
}

bool vsr_protocol_send(struct vsr *v, uint64_t peer, uint32_t type,
                       uint64_t number, const void *body, uint32_t lease)
{
    struct vsr_message message = {v->options.cluster,
                                  v->status.epoch,
                                  v->status.view,
                                  v->options.replica,
                                  type,
                                  0,
                                  number,
                                  body};
    return vsr_protocol_emit(v, VSR_OP_SEND, peer, VSR_TAG_SEND, &message,
                             &lease, 1, 0);
}

void vsr_protocol_hard(struct vsr *v, struct vsr_hard_state *h)
{
    struct vsr_protocol *p = vsr_protocol(v);
    uint32_t state = VSR_HARD_NORMAL;
    switch (v->status.state) {
    case VSR_STATE_VIEW_CHANGE:
        state = VSR_HARD_VIEW_CHANGE;
        break;
    case VSR_STATE_RECOVERING:
    case VSR_STATE_WARMING:
        state = VSR_HARD_RECOVERING;
        break;
    case VSR_STATE_TRANSITIONING:
        state = VSR_HARD_TRANSITIONING;
        break;
    case VSR_STATE_RETIRED:
        state = VSR_HARD_RETIRED;
        break;
    default:
        break;
    }
    *h = (struct vsr_hard_state){
        v->status.view, p->last_normal_view, p->desired_commit, &p->epoch,
        state,          v->status.role};
}

struct vsr_log_slot *vsr_protocol_log_find(struct vsr *v, uint64_t op)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_log_slot *s =
        &p->log[(op - 1u) % v->options.limits.log_cache_entries];
    return s->used && s->entry.op == op ? s : NULL;
}

static void log_release(struct vsr *v, struct vsr_log_slot *s)
{
    if (!s->used)
        return;
    vsr_lease_release(v, s->lease);
    s->lease = VSR_INDEX_NONE;
    s->used = false;
}

bool vsr_protocol_relieve_pressure(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    for (uint32_t i = 0; i < v->options.limits.log_cache_entries; ++i) {
        struct vsr_log_slot *slot = &p->log[i];
        if (slot->used && slot->sequence != 0 &&
            slot->sequence <= v->status.stored_sequence) {
            log_release(v, slot);
            return true;
        }
    }
    return false;
}

bool vsr_protocol_log_put(struct vsr *v, const struct vsr_entry *entry,
                          uint32_t lease, uint64_t sequence)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_log_slot *s =
        &p->log[(entry->op - 1u) % v->options.limits.log_cache_entries];
    if (s->used) {
        if (s->entry.op == entry->op) {
            if (!vsr_entry_equal(&s->entry, entry))
                vsr_fail(v, VSR_FAILURE_INVARIANT, NULL, VSR_IO_OK);
            return v->status.failure.code == VSR_FAILURE_NONE;
        }
        if (s->sequence == 0 || s->sequence > v->status.stored_sequence)
            return false;
        log_release(v, s);
    }
    if (!vsr_lease_retain(v, lease))
        return false;
    s->entry = *entry;
    s->sequence = sequence;
    s->lease = lease;
    s->used = true;
    if (entry->type == VSR_REQUEST_RECONFIGURE &&
        entry->epoch == v->status.epoch)
        p->proposed_boundary = entry->op;
    return true;
}

void vsr_protocol_log_clear(struct vsr *v, uint64_t first)
{
    struct vsr_protocol *p = vsr_protocol(v);
    if (p->proposed_boundary >= first)
        p->proposed_boundary = 0;
    for (uint32_t i = 0; i < v->options.limits.log_cache_entries; ++i)
        if (p->log[i].used && p->log[i].entry.op >= first)
            log_release(v, &p->log[i]);
}

static struct vsr_client_slot *client_slot(struct vsr *v, struct vsr_id id)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_client_slot *oldest = &p->clients[0];
    for (uint32_t i = 0; i < v->options.limits.client_cache_entries; ++i) {
        struct vsr_client_slot *s = &p->clients[i];
        if (vsr_id_equal(s->id, id))
            return s;
        if (s->stamp < oldest->stamp)
            oldest = s;
    }
    memset(oldest, 0, sizeof(*oldest));
    oldest->id = id;
    return oldest;
}

static void remember(struct vsr *v, struct vsr_request_id id, uint64_t op,
                     bool completed)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_client_slot *s = client_slot(v, id.client);
    if (p->cache_stamp == UINT64_MAX) {
        for (uint32_t i = 0; i < v->options.limits.client_cache_entries; ++i)
            p->clients[i].stamp = 0;
        p->cache_stamp = 0;
    }
    ++p->cache_stamp;
    s->stamp = p->cache_stamp;
    if (s->request < id.number) {
        s->request = id.number;
        s->request_op = op;
    }
    if (completed && s->completed < id.number) {
        s->completed = id.number;
        s->completed_op = op;
    }
}

static void route_release(struct vsr *v, struct vsr_route *r)
{
    vsr_lease_release(v, r->lease);
    vsr_lease_release(v, r->result_lease);
    memset(r, 0, sizeof(*r));
    r->lease = r->result_lease = VSR_INDEX_NONE;
}

static void route_reply(struct vsr_route *r, uint32_t status)
{
    r->reply = status;
    r->state = VSR_ROUTE_REPLY;
}

static bool send_reply(struct vsr *v, struct vsr_route *r)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_reply reply;
    memset(&reply, 0, sizeof(reply));
    reply.request = r->request.id;
    reply.op = r->op;
    reply.view = v->status.view;
    reply.primary = v->status.primary;
    reply.membership = &p->current;
    reply.status = r->reply;
    if (r->executed) {
        reply.flags = VSR_REPLY_EXECUTED;
        reply.result = r->completed.result;
    }
    if (r->query) {
        reply.request.number = r->latest;
        reply.op = r->latest_op;
    }
    uint32_t leases[] = {r->lease, r->result_lease};
    if (!vsr_protocol_emit(v, VSR_OP_REPLY, r->route, VSR_TAG_REPLY, &reply,
                           leases, 2, 0))
        return false;
    route_release(v, r);
    return true;
}

void vsr_protocol_normal(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    p->boot = BOOT_READY;
    v->status.state = VSR_STATE_NORMAL;
    p->last_normal_view = v->status.view;
    v->status.primary = vsr_primary(&p->current, v->status.view);
    p->heartbeat_at = vsr_after(v, v->options.heartbeat_ns);
    p->election_at = vsr_after(v, v->options.view_timeout_ns);
    for (uint32_t i = 0; i < p->current.count; ++i) {
        p->peers[i].prepared = 0;
        p->peers[i].sent = 0;
        p->peers[i].commit_sent = 0;
        p->peers[i].sending = false;
        p->peers[i].heartbeat = true;
        p->peers[i].retry_at = vsr_after(v, v->options.retry_ns);
    }
    if (p->self != VSR_INDEX_NONE)
        p->peers[p->self].prepared = p->stable_end - 1;
    vsr_extension_normal(v);
    vsr_changed(v);
}

int vsr_protocol_start(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    p->boot = BOOT_LOAD;
    return VSR_OK;
}

static bool boot_poll(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    if (p->boot == BOOT_LOAD) {
        uint64_t bytes = v->options.limits.manifest_bytes;
        struct vsr_store_read read = {0, 0, 0, {0, 0}, bytes, VSR_LOAD_RECOVERY,
                                      1};
        if (!vsr_protocol_emit(v, VSR_OP_LOAD, 0, VSR_TAG_BOOT, &read, NULL, 0,
                               bytes))
            return false;
        p->boot = BOOT_LOADING;
        return true;
    }
    if (p->boot == BOOT_PERSIST && p->hard_sequence != 0 &&
        p->safe_sequence >= p->hard_sequence) {
        p->boot =
            v->status.role == VSR_MEMBER_WITNESS ? BOOT_READY : BOOT_INSTALL;
        if (p->boot == BOOT_READY)
            vsr_protocol_normal(v);
        return true;
    }
    if (p->boot == BOOT_INSTALL) {
        struct vsr_snapshot_task task = {0, 0, 0, NULL, NULL};
        if (!vsr_protocol_emit(v, VSR_OP_SNAPSHOT_INSTALL, 0, VSR_TAG_INSTALL,
                               &task, NULL, 0, 0))
            return false;
        p->boot = BOOT_INSTALLING;
        p->application_busy = true;
        return true;
    }
    return false;
}

static struct vsr_transaction *transaction(struct vsr_protocol *p,
                                           uint64_t sequence)
{
    return &p->transactions[sequence % p->transaction_capacity];
}

static void advance_transactions(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    for (;;) {
        struct vsr_transaction *t =
            transaction(p, v->status.stored_sequence + 1);
        if (t->sequence != v->status.stored_sequence + 1 || !t->completed)
            break;
        v->status.stored_sequence++;
        if (t->begin > p->readable_begin)
            p->readable_begin = t->begin;
        if (t->clients_through > p->clients_stored) {
            p->clients_stored = t->clients_through;
            p->clients_sequence = t->sequence;
        }
    }
    v->status.durable_sequence = p->sync_completed < v->status.stored_sequence
                                     ? p->sync_completed
                                     : v->status.stored_sequence;
    uint64_t safe = v->options.durability == VSR_DURABLE
                        ? v->status.durable_sequence
                        : v->status.stored_sequence;
    while (p->safe_sequence < safe) {
        struct vsr_transaction *t = transaction(p, ++p->safe_sequence);
        p->stable_end = t->append_end;
        if (t->committed > p->stable_commit)
            p->stable_commit = t->committed;
        memset(t, 0, sizeof(*t));
    }
    v->status.committed = p->stable_commit;
    if (p->self != VSR_INDEX_NONE && v->status.state == VSR_STATE_NORMAL)
        p->peers[p->self].prepared = p->stable_end - 1;
}

static bool store_poll(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    bool append = p->written_end < p->log_end;
    bool results = p->results_pending && p->results_sequence == 0;
    if (!p->identity_pending && !p->hard_dirty && !append && !results)
        return false;
    if (append && !p->identity_pending && !p->hard_dirty && !results &&
        p->log_end - p->written_end < v->options.limits.batch_entries &&
        p->append_at != VSR_NO_DEADLINE && v->now < p->append_at)
        return false;
    if (p->next_sequence == UINT64_MAX) {
        vsr_fail(v, VSR_FAILURE_EXHAUSTED, NULL, VSR_IO_OK);
        return true;
    }
    struct vsr_transaction *t = transaction(p, p->next_sequence);
    if (t->sequence != 0)
        return false;
    struct vsr_operation *op =
        vsr_operation_acquire(v, VSR_OP_STORE, 0, VSR_TAG_STORE, 0);
    if (op == NULL)
        return false;
    struct vsr_change changes[VSR_MAX_STORE_CHANGES];
    uint32_t count = 0, appended = 0;
    struct vsr_store_identity identity = {
        v->options.cluster, v->options.replica, v->options.durability, 0};
    struct vsr_hard_state hard;
    if (p->identity_pending)
        changes[count++] =
            (struct vsr_change){VSR_STORE_IDENTITY, 1, 0, &identity};
    for (uint64_t n = p->written_end;
         n < p->log_end && appended < v->options.limits.batch_entries; ++n) {
        struct vsr_log_slot *s = vsr_protocol_log_find(v, n);
        if (s == NULL || !vsr_operation_hold(v, op, s->lease))
            break;
        p->batch[appended++] = s->entry;
    }
    if (append && appended == 0) {
        vsr_operation_abort(v, op);
        return false;
    }
    if (appended != 0)
        changes[count++] = (struct vsr_change){VSR_STORE_APPEND, appended,
                                               p->written_end, p->batch};
    if (results) {
        if (!vsr_operation_hold(v, op, p->results_lease)) {
            vsr_operation_abort(v, op);
            return false;
        }
        if (p->results_count != 0)
            changes[count++] = (struct vsr_change){
                VSR_STORE_CLIENTS, p->results_count, 0, p->results};
    }
    bool hard_changed = p->hard_dirty || p->identity_pending;
    if (hard_changed) {
        vsr_protocol_hard(v, &hard);
        if (hard.committed >= p->written_end + appended)
            hard.committed = p->written_end + appended - 1;
        changes[count++] =
            (struct vsr_change){VSR_STORE_HARD_STATE, 1, 0, &hard};
    }
    if (count == 0) {
        /* A NOOP-only result batch requires no result row. */
        vsr_operation_abort(v, op);
        p->clients_stored = p->results_through;
        p->results_pending = false;
        vsr_lease_release(v, p->results_lease);
        p->results_lease = VSR_INDEX_NONE;
        return true;
    }
    struct vsr_store store = {p->next_sequence, changes, count, 0};
    if (vsr_operation_copy(op, &store) != VSR_OK) {
        vsr_operation_abort(v, op);
        vsr_fail(v, VSR_FAILURE_INVARIANT, NULL, VSR_IO_OK);
        return true;
    }
    *t = (struct vsr_transaction){p->next_sequence,
                                  p->written_end + appended,
                                  hard_changed ? hard.committed : 0,
                                  results ? p->results_through : 0,
                                  0,
                                  false,
                                  false};
    for (uint32_t i = 0; i < appended; ++i)
        vsr_protocol_log_find(v, p->written_end + i)->sequence =
            p->next_sequence;
    p->written_end += appended;
    if (p->written_end == p->log_end)
        p->append_at = VSR_NO_DEADLINE;
    if (results)
        p->results_sequence = p->next_sequence;
    if (hard_changed) {
        p->hard_sequence = p->next_sequence;
        p->hard_dirty = hard.committed < p->desired_commit;
    }
    p->identity_pending = false;
    p->next_sequence++;
    if (p->boot == BOOT_CREATE)
        p->boot = BOOT_PERSIST;
    vsr_operation_publish(v, op);
    return true;
}

static bool sync_poll(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    if (v->options.durability != VSR_DURABLE || p->sync_busy ||
        p->sync_requested >= p->next_sequence - 1)
        return false;
    uint64_t through = p->next_sequence - 1;
    if (!vsr_protocol_emit(v, VSR_OP_SYNC, through, VSR_TAG_SYNC, NULL, NULL, 0,
                           0))
        return false;
    p->sync_busy = true;
    p->sync_requested = through;
    return true;
}

bool vsr_protocol_load_log(struct vsr *v, uint64_t first, uint64_t end)
{
    struct vsr_protocol *p = vsr_protocol(v);
    /* Every LOAD names stored_sequence, so it may only address the range that
     * revision retains; a compacted prefix is expected absence, never fatal. */
    if (p->log_loading || first >= end || first < p->readable_begin ||
        first >= p->written_end)
        return false;
    uint64_t bytes = vsr_protocol_available_bytes(v);
    if (bytes < v->options.limits.command_bytes)
        return false;
    if (bytes > v->options.limits.message_bytes)
        bytes = v->options.limits.message_bytes;
    uint32_t count = v->options.limits.batch_entries;
    if (end - first < count)
        count = (uint32_t)(end - first);
    /* Modulo cache placement must never replace an unstored append. */
    for (uint32_t i = 0; i < count; ++i) {
        struct vsr_log_slot *s =
            &p->log[(first + i - 1) % v->options.limits.log_cache_entries];
        if (s->used && s->entry.op != first + i &&
            (s->sequence == 0 || s->sequence > v->status.stored_sequence)) {
            count = i;
            break;
        }
    }
    if (count == 0)
        return false;
    end = first + count;
    struct vsr_store_read read = {v->status.stored_sequence,
                                  first,
                                  end,
                                  {0, 0},
                                  bytes,
                                  VSR_LOAD_LOG,
                                  count};
    if (!vsr_protocol_emit(v, VSR_OP_LOAD, 0, VSR_TAG_LOG, &read, NULL, 0,
                           bytes))
        return false;
    p->log_loading = true;
    p->load_first = first;
    p->load_end = end;
    return true;
}

bool vsr_protocol_store(struct vsr *v, const struct vsr_change *changes,
                        uint32_t count, const uint32_t *leases,
                        uint32_t lease_count, uint64_t append_end,
                        uint64_t committed, uint64_t clients_through,
                        uint64_t *sequence)
{
    struct vsr_protocol *p = vsr_protocol(v);
    if (p->next_sequence == UINT64_MAX) {
        vsr_fail(v, VSR_FAILURE_EXHAUSTED, NULL, VSR_IO_OK);
        return false;
    }
    struct vsr_transaction *t = transaction(p, p->next_sequence);
    if (t->sequence != 0)
        return false;
    struct vsr_store store = {p->next_sequence, changes, count, 0};
    /* Mirror the store's retained range so the readable frontier can follow
     * this transaction the moment it is readable, before it is durable. */
    uint64_t begin = 0;
    bool restores = false;
    for (uint32_t i = 0; i < count; ++i) {
        if (changes[i].type == VSR_STORE_TRIM) {
            begin = changes[i].first;
        } else if (changes[i].type == VSR_STORE_RESTORE_CHECKPOINT) {
            const struct vsr_checkpoint *checkpoint = changes[i].data;
            begin = checkpoint->op + 1;
            restores = true;
        }
    }
    if (!vsr_protocol_emit(v, VSR_OP_STORE, 0, VSR_TAG_STORE, &store, leases,
                           lease_count, 0))
        return false;
    *sequence = p->next_sequence++;
    *t = (struct vsr_transaction){
        *sequence, append_end, committed, clients_through,
        begin,     restores,   false};
    return true;
}

/* Whether a transaction replacing the indexed client base is still
 * outstanding: the host may already read past it. */
static bool restore_pending(const struct vsr *v)
{
    const struct vsr_protocol *p = vsr_protocol_const(v);
    for (uint64_t s = v->status.stored_sequence + 1; s < p->next_sequence;
         ++s) {
        const struct vsr_transaction *t =
            &p->transactions[s % p->transaction_capacity];
        if (t->sequence == s && t->restores)
            return true;
    }
    return false;
}

bool vsr_protocol_nonce(struct vsr *v, struct vsr_nonce *nonce)
{
    struct vsr_protocol *p = vsr_protocol(v);
    if (p->nonce_counter >= UINT64_MAX - 1) {
        vsr_fail(v, VSR_FAILURE_EXHAUSTED, NULL, VSR_IO_OK);
        return false;
    }
    *nonce = (struct vsr_nonce){v->options.incarnation, ++p->nonce_counter};
    return true;
}

bool vsr_protocol_ready(const struct vsr *v)
{
    const struct vsr_protocol *p = vsr_protocol_const(v);
    return v->status.state == VSR_STATE_NORMAL && p->boot == BOOT_READY &&
           p->self != VSR_INDEX_NONE &&
           p->epoch.phase != VSR_EPOCH_TRANSFERRING && !p->hard_dirty &&
           p->safe_sequence >= p->hard_sequence;
}

static bool proposal_room(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    if (!vsr_protocol_ready(v) || v->status.primary != v->options.replica ||
        v->status.role != VSR_MEMBER_FULL)
        return false;
    if (p->log_end >= UINT64_MAX - 1) {
        vsr_fail(v, VSR_FAILURE_EXHAUSTED, NULL, VSR_IO_OK);
        return false;
    }
    const uint32_t entries = v->options.limits.log_cache_entries;
    if (entries == 0) /* Rejected by validation; keeps analyzers honest. */
        return false;
    struct vsr_log_slot *s = &p->log[(p->log_end - 1) % entries];
    if (s->used &&
        (s->sequence == 0 || s->sequence > v->status.stored_sequence))
        return false;
    /* This fence survives cache eviction of an uncommitted boundary. */
    if (p->proposed_boundary != 0)
        return false;
    return true;
}

bool vsr_protocol_noop(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    if (!proposal_room(v))
        return false;
    struct vsr_entry entry = {p->log_end,  v->status.epoch,  v->status.view,
                              {{0, 0}, 0}, VSR_REQUEST_NOOP, 0,
                              NULL};
    if (!vsr_protocol_log_put(v, &entry, VSR_INDEX_NONE, 0))
        return false;
    if (p->log_end == p->written_end && v->options.batch_delay_ns != 0)
        p->append_at = vsr_after(v, v->options.batch_delay_ns);
    p->log_end++;
    return true;
}

static int request_event(struct vsr *v, const struct vsr_event *event,
                         uint32_t lease)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_route *free_route = NULL;
    uint32_t immediate_reply = UINT32_MAX;
    bool query = event->type == VSR_EVENT_CLIENT_QUERY;
    struct vsr_request query_request = {0};
    if (query)
        query_request.id.client = *(const struct vsr_id *)event->data;
    const struct vsr_request *request = query ? &query_request : event->data;
    struct vsr_id client = request->id.client;
    for (uint32_t i = 0; i < v->options.limits.pending_requests; ++i) {
        struct vsr_route *r = &p->routes[i];
        if (r->state == VSR_ROUTE_FREE && free_route == NULL)
            free_route = r;
        if (!query && r->state != VSR_ROUTE_FREE && !r->query &&
            vsr_request_id_equal(r->request.id, request->id)) {
            struct vsr_log_slot *original =
                r->op == 0 ? NULL : vsr_protocol_log_find(v, r->op);
            struct vsr_request known = r->request;
            if (r->lease == VSR_INDEX_NONE && original != NULL)
                known = (struct vsr_request){
                    original->entry.request, original->entry.epoch,
                    original->entry.type, 0, original->entry.body};
            if ((r->lease != VSR_INDEX_NONE || original != NULL) &&
                !vsr_request_equal(&known, request)) {
                immediate_reply = VSR_REPLY_INVALID;
                continue;
            }
            r->route = event->id;
            return VSR_OK;
        }
        if (!query && r->state != VSR_ROUTE_FREE &&
            r->state != VSR_ROUTE_REPLY && !r->query &&
            vsr_id_equal(r->request.id.client, client) &&
            r->request.id.number != request->id.number)
            immediate_reply = r->request.id.number < request->id.number
                                  ? VSR_REPLY_BUSY
                                  : VSR_REPLY_STALE_REQUEST;
    }
    if (free_route == NULL)
        return VSR_AGAIN;
    struct vsr_route *r = free_route;
    r->query = query;
    r->route = event->id;
    r->request = query ? (struct vsr_request){{client, 0},
                                              v->status.epoch,
                                              VSR_REQUEST_COMMAND,
                                              0,
                                              NULL}
                       : *request;
    r->check_epoch =
        !query && request->type == VSR_REQUEST_CHECK_EPOCH
            ? ((const struct vsr_check_epoch *)request->body)->epoch
            : 0;
    r->lease = r->result_lease = VSR_INDEX_NONE;
    if (!query) {
        vsr_lease_retain(v, lease);
        r->lease = lease;
    }
    r->state = VSR_ROUTE_CLIENT;
    if (immediate_reply != UINT32_MAX)
        route_reply(r, immediate_reply);
    else if (v->status.role == VSR_MEMBER_WITNESS ||
             (!query && v->status.primary != v->options.replica))
        route_reply(r, VSR_REPLY_NOT_PRIMARY);
    else if (v->status.state != VSR_STATE_NORMAL)
        route_reply(r, VSR_REPLY_BUSY);
    return VSR_OK;
}

static void refresh_route(struct vsr *v, struct vsr_route *r)
{
    vsr_lease_release(v, r->result_lease);
    r->result_lease = VSR_INDEX_NONE;
    memset(&r->completed, 0, sizeof(r->completed));
    r->latest = r->latest_op = r->op = 0;
    r->executed = false;
    r->history_generation = vsr_protocol(v)->history_generation;
    r->state = VSR_ROUTE_CLIENT;
}

static bool load_route(struct vsr *v, struct vsr_route *r, uint32_t type)
{
    struct vsr_protocol *p = vsr_protocol(v);
    if (r->history_generation != p->history_generation) {
        refresh_route(v, r);
        type = VSR_LOAD_CLIENT;
    }
    uint64_t bytes = type == VSR_LOAD_CLIENT ? v->options.limits.result_bytes
                                             : v->options.limits.command_bytes;
    struct vsr_store_read read = {
        v->status.stored_sequence, 0, 0, r->request.id.client, bytes, type, 1};
    uint64_t tag = type == VSR_LOAD_CLIENT ? VSR_TAG_CLIENT : VSR_TAG_REQUEST;
    tag |= (uint64_t)(r - p->routes) << 32;
    if (!vsr_protocol_emit(v, VSR_OP_LOAD, 0, tag, &read, NULL, 0, bytes))
        return false;
    r->read_sequence = read.sequence;
    r->state = type == VSR_LOAD_CLIENT ? VSR_ROUTE_CLIENT_LOADING
                                       : VSR_ROUTE_REQUEST_LOADING;
    return true;
}

static bool decide_route(struct vsr *v, struct vsr_route *r)
{
    struct vsr_protocol *p = vsr_protocol(v);
    if (r->history_generation != p->history_generation) {
        refresh_route(v, r);
        return true;
    }
    const struct vsr_entry *known = NULL;
    uint64_t latest = r->latest, latest_op = r->latest_op;
    /* Merge the immutable read revision with proposals and result transactions
     * submitted while those reads were outstanding. */
    for (uint32_t i = 0; i < v->options.limits.log_cache_entries; ++i) {
        struct vsr_log_slot *s = &p->log[i];
        if (s->used &&
            vsr_id_equal(s->entry.request.client, r->request.id.client) &&
            s->entry.request.number >= latest) {
            latest = s->entry.request.number;
            latest_op = s->entry.op;
            known = &s->entry;
        }
    }
    for (uint32_t i = 0; i < v->options.limits.pending_requests; ++i) {
        struct vsr_route *other = &p->routes[i];
        if (other != r && !other->query && other->state == VSR_ROUTE_WAIT &&
            vsr_id_equal(other->request.id.client, r->request.id.client) &&
            other->request.id.number > latest) {
            latest = other->request.id.number;
            latest_op = other->op;
        }
    }
    struct vsr_client_slot *cached = client_slot(v, r->request.id.client);
    if (cached->request > latest) {
        latest = cached->request;
        latest_op = cached->request_op;
    }
    if (cached->completed > r->completed.request.number &&
        cached->completed_op <= p->clients_stored) {
        vsr_lease_release(v, r->result_lease);
        r->result_lease = VSR_INDEX_NONE;
        r->executed = false;
        r->state = VSR_ROUTE_CLIENT;
        return true;
    }
    r->latest = latest;
    r->latest_op = latest_op;
    if (r->query) {
        r->executed = latest != 0 && latest == r->completed.request.number;
        route_reply(r, latest == 0 ? VSR_REPLY_CLIENT_UNKNOWN
                                   : VSR_REPLY_CLIENT_STATE);
        return true;
    }
    uint64_t number = r->request.id.number;
    if (r->completed.request.number == number &&
        r->request.type == VSR_REQUEST_CHECK_EPOCH &&
        !vsr_epochs_ready(v, r->check_epoch))
        return false;
    if (r->completed.request.number == number) {
        r->op = r->completed.op;
        r->executed = true;
        route_reply(r, VSR_REPLY_OK);
    } else if (latest > number || r->completed.request.number > number) {
        route_reply(r, VSR_REPLY_STALE_REQUEST);
    } else if (latest == number && latest_op != 0) {
        if (known != NULL) {
            struct vsr_request old = {known->request, known->epoch, known->type,
                                      0, known->body};
            if (!vsr_request_equal(&old, &r->request)) {
                route_reply(r, VSR_REPLY_INVALID);
                return true;
            }
        }
        r->op = latest_op;
        r->state = VSR_ROUTE_WAIT;
    } else if (latest > r->completed.request.number) {
        route_reply(r, VSR_REPLY_BUSY);
    } else if (r->request.epoch != v->status.epoch) {
        route_reply(r, VSR_REPLY_NEW_EPOCH);
    } else if (v->status.state != VSR_STATE_NORMAL ||
               v->status.primary != v->options.replica) {
        route_reply(r, VSR_REPLY_NOT_PRIMARY);
    } else if (r->request.type == VSR_REQUEST_RECONFIGURE &&
               ((const struct vsr_membership *)r->request.body)->epoch !=
                   v->status.epoch + 1) {
        route_reply(r, VSR_REPLY_INVALID);
    } else if (p->proposed_boundary != 0 ||
               (r->request.type == VSR_REQUEST_RECONFIGURE &&
                p->epoch.phase != VSR_EPOCH_STEADY)) {
        route_reply(r, VSR_REPLY_BUSY);
    } else if (r->request.type == VSR_REQUEST_CHECK_EPOCH &&
               ((const struct vsr_check_epoch *)r->request.body)->epoch >
                   r->request.epoch) {
        route_reply(r, VSR_REPLY_INVALID);
    } else {
        if (!proposal_room(v))
            return false;
        struct vsr_entry entry = {p->log_end,      v->status.epoch,
                                  v->status.view,  r->request.id,
                                  r->request.type, 0,
                                  r->request.body};
        if (!vsr_protocol_log_put(v, &entry, r->lease, 0))
            return false;
        if (p->log_end == p->written_end && v->options.batch_delay_ns != 0)
            p->append_at = vsr_after(v, v->options.batch_delay_ns);
        p->log_end++;
        r->op = entry.op;
        r->state = VSR_ROUTE_WAIT;
        remember(v, entry.request, entry.op, false);
    }
    return true;
}

static bool routes_poll(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    for (uint32_t n = 0; n < v->options.limits.pending_requests; ++n) {
        uint32_t i = p->route_cursor;
        p->route_cursor =
            i + 1 == v->options.limits.pending_requests ? 0 : i + 1;
        struct vsr_route *r = &p->routes[i];
        switch (r->state) {
        case VSR_ROUTE_CLIENT:
            if (load_route(v, r, VSR_LOAD_CLIENT))
                return true;
            break;
        case VSR_ROUTE_REQUEST:
            if (load_route(v, r, VSR_LOAD_REQUEST))
                return true;
            break;
        case VSR_ROUTE_DECIDE:
            if (decide_route(v, r))
                return true;
            break;
        case VSR_ROUTE_REPLY:
            /* Restoring a checkpoint replaces the indexed client base, so an
             * executed reply decided from the previous revision is no longer
             * backed by a stored record. Decide it again from the restored
             * revision, as a load completing after restoration would, and
             * hold it while a restoration is outstanding: the host may
             * execute the reply after that store. */
            if (r->executed) {
                if (r->history_generation != p->history_generation) {
                    refresh_route(v, r);
                    return true;
                }
                if (restore_pending(v))
                    break;
            }
            if (send_reply(v, r))
                return true;
            break;
        case VSR_ROUTE_WAIT: {
            const struct vsr_log_slot *slot = vsr_protocol_log_find(v, r->op);
            if (r->lease != VSR_INDEX_NONE && r->op < p->written_end &&
                slot != NULL && slot->sequence <= v->status.stored_sequence) {
                vsr_lease_release(v, r->lease);
                r->lease = VSR_INDEX_NONE;
                r->request.body = NULL;
                return true;
            }
            if (r->op <= p->clients_stored) {
                r->state = VSR_ROUTE_CLIENT;
                return true;
            }
            break;
        }
        default:
            break;
        }
    }
    return false;
}

static bool commit_poll(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    if (!vsr_protocol_ready(v) || v->status.primary != v->options.replica)
        return false;
    uint64_t chosen = p->desired_commit;
    uint32_t quorum = p->current.count - p->current.faults;
    for (uint32_t i = 0; i < p->current.count; ++i) {
        uint64_t candidate = p->peers[i].prepared;
        if (candidate <= chosen || candidate >= p->stable_end)
            continue;
        uint32_t votes = 0;
        for (uint32_t j = 0; j < p->current.count; ++j)
            if (p->peers[j].prepared >= candidate)
                votes++;
        if (votes >= quorum)
            chosen = candidate;
    }
    if (chosen == p->desired_commit)
        return false;
    p->desired_commit = chosen;
    p->hard_dirty = true;
    return true;
}

static bool network_poll(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    if (!vsr_protocol_ready(v))
        return false;
    if (v->status.primary != v->options.replica) {
        struct vsr_peer *self = &p->peers[p->self];
        if (!self->sending &&
            (self->sent < p->stable_end - 1 || self->heartbeat)) {
            if (!vsr_protocol_send(v, v->status.primary, VSR_MSG_PREPARE_OK,
                                   p->stable_end - 1, NULL, VSR_INDEX_NONE))
                return false;
            self->sent = p->stable_end - 1;
            self->heartbeat = false;
            self->sending = true;
            return true;
        }
        return false;
    }
    for (uint32_t n = 0; n < p->current.count; ++n) {
        uint32_t i = p->peer_cursor;
        p->peer_cursor = i + 1 == p->current.count ? 0 : i + 1;
        struct vsr_peer *peer = &p->peers[i];
        if (i == p->self || peer->sending)
            continue;
        uint64_t first =
            (peer->sent > peer->prepared ? peer->sent : peer->prepared) + 1;
        /* PREPARE can only resume from the retained log. A peer acknowledged
         * below it falls through to COMMIT: the commit number is beyond its
         * history, so it discovers an offer and fetches state instead. */
        if (first < p->stable_end && first >= p->readable_begin) {
            struct vsr_log_slot *s = vsr_protocol_log_find(v, first);
            if (s == NULL) {
                if (vsr_protocol_load_log(v, first, p->stable_end)) {
                    /* Serve this peer first once its entries arrive. A small
                     * cache cannot hold every lagging peer's next entry at
                     * once; rotating past it would evict the loaded entry
                     * for another peer's load before it is ever sent. */
                    p->peer_cursor = i;
                    return true;
                }
                continue;
            }
            struct vsr_operation *op = vsr_operation_acquire(
                v, VSR_OP_SEND, peer->id, VSR_TAG_SEND, 0);
            if (op == NULL)
                return false;
            uint32_t count = 0;
            uint64_t bytes = 0;
            while (first + count < p->stable_end &&
                   count < v->options.limits.batch_entries) {
                s = vsr_protocol_log_find(v, first + count);
                if (s == NULL)
                    break;
                uint64_t payload =
                    s->entry.type == VSR_REQUEST_COMMAND
                        ? ((const struct vsr_blob *)s->entry.body)->size
                        : 0;
                if (payload > v->options.limits.message_bytes - bytes ||
                    !vsr_operation_hold(v, op, s->lease))
                    break;
                bytes += payload;
                p->batch[count++] = s->entry;
            }
            struct vsr_prepare prepare = {{p->batch, count, 0},
                                          p->stable_commit};
            if (prepare.committed >= first + count)
                prepare.committed = first + count - 1;
            struct vsr_message message = {
                v->options.cluster, v->status.epoch, v->status.view,
                v->options.replica, VSR_MSG_PREPARE, 0,
                first + count - 1,  &prepare};
            if (count == 0 || vsr_operation_copy(op, &message) != VSR_OK) {
                vsr_operation_abort(v, op);
                return false;
            }
            peer->sent = message.number;
            peer->commit_sent = prepare.committed;
            peer->sending = true;
            peer->retry_at = vsr_after(v, v->options.retry_ns);
            vsr_operation_publish(v, op);
            return true;
        }
        if (peer->heartbeat || peer->commit_sent < p->stable_commit) {
            if (!vsr_protocol_send(v, peer->id, VSR_MSG_COMMIT,
                                   p->stable_commit, NULL, VSR_INDEX_NONE))
                return false;
            peer->heartbeat = false;
            peer->commit_sent = p->stable_commit;
            peer->sending = true;
            return true;
        }
    }
    return false;
}

static bool apply_poll(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    if (p->application_busy || p->results_pending ||
        v->status.role != VSR_MEMBER_FULL ||
        v->status.applied >= p->stable_commit ||
        (p->boot != BOOT_READY && !p->replay))
        return false;
    uint64_t first = v->status.applied + 1;
    /* A transition that forbids application must not load entries it cannot
     * use: the pinned cache entry would only be evicted under input pressure
     * and reloaded here, starving the input that ends the transition. */
    if (first > p->notified_commit || !vsr_extension_apply_ready(v))
        return false;
    if (vsr_protocol_log_find(v, first) == NULL)
        return vsr_protocol_load_log(v, first, p->stable_commit + 1);
    uint64_t available = vsr_protocol_available_bytes(v);
    uint64_t per = v->options.limits.result_bytes;
    uint32_t maximum = v->options.limits.batch_entries;
    if (per != 0 && available / per < maximum)
        maximum = (uint32_t)(available / per);
    if (maximum == 0)
        return false;
    uint32_t count = 0;
    for (uint64_t n = first; n <= p->stable_commit && count < maximum; ++n) {
        struct vsr_log_slot *s = vsr_protocol_log_find(v, n);
        if (s == NULL || n > p->notified_commit ||
            !vsr_extension_apply_allowed(v, &s->entry))
            break;
        p->batch[count++] = s->entry;
    }
    if (count == 0)
        return false;
    /* Reserve only the eligible batch's possible results. The remaining
     * completion budget stays available to concurrent storage and inputs. */
    struct vsr_operation *op = vsr_operation_acquire(
        v, VSR_OP_APPLY, 0, VSR_TAG_APPLY, (uint64_t)count * per);
    if (op == NULL)
        return false;
    for (uint32_t i = 0; i < count; ++i) {
        struct vsr_log_slot *s =
            &p->log[(first + i - 1) % v->options.limits.log_cache_entries];
        if (!vsr_operation_hold(v, op, s->lease)) {
            vsr_operation_abort(v, op);
            return false;
        }
    }
    struct vsr_apply apply = {
        {p->batch, count, 0}, first + count - 1, p->replay ? 1u : 0u, 0};
    if (vsr_operation_copy(op, &apply) != VSR_OK) {
        vsr_operation_abort(v, op);
        vsr_fail(v, VSR_FAILURE_INVARIANT, NULL, VSR_IO_OK);
        return true;
    }
    p->application_busy = true;
    vsr_operation_publish(v, op);
    return true;
}

static int normal_message(struct vsr *v, const struct vsr_message *m,
                          uint32_t lease)
{
    struct vsr_protocol *p = vsr_protocol(v);
    if (!vsr_id_equal(m->cluster, v->options.cluster) ||
        m->epoch != v->status.epoch || m->view != v->status.view ||
        v->status.state != VSR_STATE_NORMAL)
        return VSR_OK;
    uint32_t sender = vsr_member_index(&p->current, m->from);
    if (sender == VSR_INDEX_NONE || sender == p->self)
        return VSR_OK;
    if (m->type == VSR_MSG_PREPARE_OK &&
        v->status.primary == v->options.replica) {
        if (m->number < p->stable_end && m->number > p->peers[sender].prepared)
            p->peers[sender].prepared = m->number;
        return VSR_OK;
    }
    if (m->from != v->status.primary || v->status.primary == v->options.replica)
        return VSR_OK;
    if (m->type == VSR_MSG_COMMIT) {
        p->election_at = vsr_after(v, v->options.view_timeout_ns);
        if (m->number > p->desired_commit) {
            p->desired_commit =
                m->number < p->log_end ? m->number : p->log_end - 1;
            p->hard_dirty = p->desired_commit > p->stable_commit;
        }
        return VSR_OK;
    }
    if (m->type != VSR_MSG_PREPARE)
        return VSR_OK;
    const struct vsr_prepare *prepare = m->body;
    uint64_t first = prepare->batch.entries[0].op;
    if (first > p->log_end)
        return VSR_OK;
    /* Admission is atomic: verify every replacement and cache slot before pins. */
    for (uint32_t i = 0; i < prepare->batch.count; ++i) {
        const struct vsr_entry *entry = &prepare->batch.entries[i];
        struct vsr_log_slot *s = vsr_protocol_log_find(v, entry->op);
        if (s != NULL && !vsr_entry_equal(&s->entry, entry)) {
            vsr_fail(v, VSR_FAILURE_INVARIANT, NULL, VSR_IO_OK);
            return VSR_OK;
        }
        if (entry->op < p->log_end)
            continue;
        s = &p->log[(entry->op - 1) % v->options.limits.log_cache_entries];
        if (s->used &&
            (s->sequence == 0 || s->sequence > v->status.stored_sequence))
            return VSR_AGAIN;
    }
    for (uint32_t i = 0; i < prepare->batch.count; ++i) {
        const struct vsr_entry *entry = &prepare->batch.entries[i];
        if (entry->op < p->log_end)
            continue;
        if (!vsr_protocol_log_put(v, entry, lease, 0)) {
            vsr_fail(v, VSR_FAILURE_INVARIANT, NULL, VSR_IO_OK);
            return VSR_OK;
        }
        p->log_end++;
        if (entry->type != VSR_REQUEST_NOOP)
            remember(v, entry->request, entry->op, false);
    }
    if (prepare->committed > p->desired_commit) {
        p->desired_commit = prepare->committed;
        p->hard_dirty = true;
    }
    p->peers[p->self].heartbeat = true;
    p->election_at = vsr_after(v, v->options.view_timeout_ns);
    return VSR_OK;
}

int vsr_protocol_event(struct vsr *v, const struct vsr_event *event,
                       uint32_t lease)
{
    struct vsr_protocol *p = vsr_protocol(v);
    bool handled = false;
    int result = vsr_extension_event(v, event, lease, &handled);
    if (handled || result != VSR_OK)
        return result;
    switch (event->type) {
    case VSR_EVENT_TIME:
        if (p->heartbeat_at == VSR_NO_DEADLINE)
            p->heartbeat_at = vsr_after(v, v->options.heartbeat_ns);
        if (p->election_at == VSR_NO_DEADLINE)
            p->election_at = vsr_after(v, v->options.view_timeout_ns);
        if (v->now >= p->heartbeat_at) {
            for (uint32_t i = 0; i < p->current.count; ++i)
                p->peers[i].heartbeat = true;
            p->heartbeat_at = vsr_after(v, v->options.heartbeat_ns);
        }
        for (uint32_t i = 0; i < p->current.count; ++i) {
            struct vsr_peer *peer = &p->peers[i];
            if (peer->retry_at == VSR_NO_DEADLINE)
                peer->retry_at = vsr_after(v, v->options.retry_ns);
            if (v->now >= peer->retry_at) {
                peer->sent = peer->prepared;
                peer->retry_at = vsr_after(v, v->options.retry_ns);
            }
        }
        return VSR_OK;
    case VSR_EVENT_REQUEST:
    case VSR_EVENT_CLIENT_QUERY:
        return request_event(v, event, lease);
    case VSR_EVENT_MESSAGE:
        return normal_message(v, event->data, lease);
    case VSR_EVENT_CHECKPOINT:
        p->checkpoint_requested = true;
        return VSR_OK;
    default:
        return VSR_OK;
    }
}

static void load_failure(struct vsr *v, const struct vsr_operation *op,
                         const struct vsr_event *event)
{
    vsr_fail(v, VSR_FAILURE_STORAGE, op, event->status);
}

static void boot_complete(struct vsr *v, struct vsr_operation *op,
                          const struct vsr_event *event, uint32_t lease)
{
    struct vsr_protocol *p = vsr_protocol(v);
    if (event->status == VSR_IO_RETRY) {
        p->boot = BOOT_LOAD;
        return;
    }
    if (event->status == VSR_IO_NOT_FOUND) {
        if (v->options.start_mode == VSR_START_NEW) {
            p->identity_pending = p->hard_dirty = true;
            p->boot = BOOT_CREATE;
        } else {
            p->boot = BOOT_RECOVER;
            vsr_extension_boot_missing(v);
        }
        return;
    }
    if (event->status != VSR_IO_OK) {
        load_failure(v, op, event);
        return;
    }
    const struct vsr_loaded *loaded = event->data;
    const struct vsr_recovered *r = loaded->items;
    if (v->options.start_mode != VSR_START_RECOVER ||
        !vsr_id_equal(r->identity.cluster, v->options.cluster) ||
        r->identity.replica != v->options.replica ||
        r->identity.durability != v->options.durability) {
        vsr_fail(v, VSR_FAILURE_IDENTITY, op, VSR_IO_OK);
        return;
    }
    if (r->sequence >= UINT64_MAX - 1) {
        vsr_fail(v, VSR_FAILURE_EXHAUSTED, op, VSR_IO_OK);
        return;
    }
    v->status.view = r->hard.view;
    v->status.role = r->hard.role;
    vsr_protocol_configuration(v, r->hard.epoch);
    p->next_sequence = r->sequence + 1;
    p->safe_sequence = p->sync_requested = p->sync_completed = r->sequence;
    v->status.stored_sequence = v->status.durable_sequence = r->sequence;
    p->hard_sequence = r->sequence;
    p->log_begin = p->readable_begin = r->log_begin;
    p->log_end = p->written_end = p->stable_end = r->log_end;
    p->desired_commit = p->stable_commit = v->status.committed =
        r->hard.committed;
    p->last_normal_view = r->hard.last_normal_view;
    p->boot = BOOT_RECOVER;
    if (!vsr_extension_boot_loaded(v, r, lease)) {
        p->boot = BOOT_INSTALL;
        p->replay = true;
    }
}

static void route_load_complete(struct vsr *v, struct vsr_operation *op,
                                const struct vsr_event *event, uint32_t lease)
{
    struct vsr_protocol *p = vsr_protocol(v);
    uint32_t index = (uint32_t)(op->tag >> 32);
    if (index >= v->options.limits.pending_requests) {
        vsr_fail(v, VSR_FAILURE_INVARIANT, op, VSR_IO_OK);
        return;
    }
    struct vsr_route *r = &p->routes[index];
    bool client = (uint32_t)op->tag == VSR_TAG_CLIENT;
    /* Immutable reads can finish after log selection or snapshot restoration.
     * Their former request index remains valid for that storage revision, but
     * it cannot repopulate the current client cache or decide a new proposal. */
    if (r->history_generation != p->history_generation) {
        refresh_route(v, r);
        return;
    }
    if (event->status == VSR_IO_RETRY) {
        r->state = client ? VSR_ROUTE_CLIENT : VSR_ROUTE_REQUEST;
        return;
    }
    if (event->status != VSR_IO_OK) {
        load_failure(v, op, event);
        return;
    }
    const struct vsr_store_read *read = op->output.data;
    const struct vsr_loaded *loaded = event->data;
    if (loaded->sequence != read->sequence) {
        load_failure(v, op, event);
        return;
    }
    if (client) {
        vsr_lease_release(v, r->result_lease);
        r->result_lease = VSR_INDEX_NONE;
        r->executed = false;
        memset(&r->completed, 0, sizeof(r->completed));
        if (loaded->count != 0) {
            const struct vsr_client_record *record = loaded->items;
            if (!vsr_id_equal(record->request.client, read->client) ||
                record->op > v->status.committed) {
                load_failure(v, op, event);
                return;
            }
            r->completed = *record;
            if (record->request.number > r->latest) {
                r->latest = record->request.number;
                r->latest_op = record->op;
            }
            remember(v, record->request, record->op, true);
            if (r->query || record->request.number == r->request.id.number) {
                vsr_lease_retain(v, lease);
                r->result_lease = lease;
            } else
                memset(&r->completed.result, 0, sizeof(r->completed.result));
            if (!r->query && record->request.number >= r->request.id.number) {
                r->state = VSR_ROUTE_DECIDE;
                return;
            }
        }
        r->state = VSR_ROUTE_REQUEST;
    } else {
        if (loaded->count != 0) {
            const struct vsr_entry *entry = loaded->items;
            if (!vsr_id_equal(entry->request.client, read->client) ||
                entry->op >= p->written_end) {
                load_failure(v, op, event);
                return;
            }
            if (entry->request.number > r->latest) {
                r->latest = entry->request.number;
                r->latest_op = entry->op;
            }
            remember(v, entry->request, entry->op, false);
            if (!r->query &&
                vsr_request_id_equal(entry->request, r->request.id) &&
                r->lease != VSR_INDEX_NONE) {
                struct vsr_request old = {entry->request, entry->epoch,
                                          entry->type, 0, entry->body};
                if (!vsr_request_equal(&old, &r->request)) {
                    route_reply(r, VSR_REPLY_INVALID);
                    return;
                }
            }
        }
        r->state = VSR_ROUTE_DECIDE;
    }
}

void vsr_protocol_complete(struct vsr *v, struct vsr_operation *op,
                           const struct vsr_event *event, uint32_t lease)
{
    struct vsr_protocol *p = vsr_protocol(v);
    uint32_t tag = (uint32_t)op->tag;
    if (v->status.state == VSR_STATE_RETIRED)
        return;
    if (tag >= VSR_TAG_EXTENSION) {
        vsr_extension_complete(v, op, event, lease);
        return;
    }
    switch (tag) {
    case VSR_TAG_BOOT:
        boot_complete(v, op, event, lease);
        break;
    case VSR_TAG_STORE: {
        const struct vsr_store *store = op->output.data;
        struct vsr_transaction *t = transaction(p, store->sequence);
        if (t->sequence != store->sequence) {
            vsr_fail(v, VSR_FAILURE_INVARIANT, op, VSR_IO_OK);
            return;
        }
        t->completed = true;
        advance_transactions(v);
        break;
    }
    case VSR_TAG_SYNC:
        p->sync_busy = false;
        if (op->output.arg > p->sync_completed)
            p->sync_completed = op->output.arg;
        advance_transactions(v);
        break;
    case VSR_TAG_INSTALL:
        p->application_busy = false;
        if (p->replay && p->stable_commit != 0)
            p->boot = BOOT_READY;
        else {
            p->replay = false;
            vsr_protocol_normal(v);
        }
        break;
    case VSR_TAG_CLIENT:
    case VSR_TAG_REQUEST:
        route_load_complete(v, op, event, lease);
        break;
    case VSR_TAG_LOG: {
        p->log_loading = false;
        if (event->status == VSR_IO_RETRY)
            break;
        if (event->status != VSR_IO_OK) {
            load_failure(v, op, event);
            break;
        }
        const struct vsr_loaded *loaded = event->data;
        const struct vsr_store_read *read = op->output.data;
        if (loaded->sequence != read->sequence) {
            load_failure(v, op, event);
            break;
        }
        const struct vsr_entry *entries = loaded->items;
        for (uint32_t i = 0; i < loaded->count; ++i)
            if (!vsr_protocol_log_put(v, &entries[i], lease, read->sequence)) {
                /* Concurrent appends may occupy a now-stale cache placement;
                 * the immutable storage range remains reloadable. */
                break;
            }
        break;
    }
    case VSR_TAG_APPLY: {
        const struct vsr_apply *apply = op->output.data;
        const struct vsr_applied *applied = event->data;
        p->application_busy = false;
        p->results_count = 0;
        p->results_through = apply->through;
        p->results_sequence = 0;
        p->results_pending = true;
        vsr_lease_retain(v, lease);
        p->results_lease = lease;
        for (uint32_t i = 0; i < apply->batch.count; ++i) {
            const struct vsr_entry *entry = &apply->batch.entries[i];
            if (entry->type != VSR_REQUEST_NOOP) {
                p->results[p->results_count++] = (struct vsr_client_record){
                    entry->request, entry->op, applied->results[i]};
                remember(v, entry->request, entry->op, true);
            }
        }
        p->applied_view = apply->batch.entries[apply->batch.count - 1].view;
        v->status.applied = apply->through;
        break;
    }
    case VSR_TAG_RECLAIM:
        p->reclaim_busy = false;
        if (event->status == VSR_IO_OK)
            p->reclaimed_sequence = op->output.arg;
        else
            p->reclaim_sequence = p->reclaimed_sequence;
        break;
    case VSR_TAG_SEND: {
        const struct vsr_message *message = op->output.data;
        uint32_t i = message->type == VSR_MSG_PREPARE_OK
                         ? p->self
                         : vsr_member_index(&p->current, op->output.arg);
        if (i != VSR_INDEX_NONE) {
            p->peers[i].sending = false;
            if (event->status != VSR_IO_OK) {
                p->peers[i].sent = p->peers[i].prepared;
                p->peers[i].heartbeat = true;
            }
        }
        break;
    }
    default:
        break;
    }
}

static bool reclaim_poll(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    if (p->reclaim_busy)
        return false;
    uint64_t floor = v->options.durability == VSR_DURABLE
                         ? v->status.durable_sequence
                         : v->status.stored_sequence;
    uint64_t extension = vsr_extension_min_sequence(v);
    if (extension < floor)
        floor = extension;
    for (uint32_t i = 0; i < v->options.limits.operations; ++i) {
        const struct vsr_operation *op = &v->operations[i];
        if (op->state == VSR_SLOT_FREE || op->state == VSR_SLOT_BUILDING)
            continue;
        uint64_t sequence = UINT64_MAX;
        if (op->output.type == VSR_OP_LOAD) {
            const struct vsr_store_read *read = op->output.data;
            if (read->type != VSR_LOAD_RECOVERY)
                sequence = read->sequence;
        } else if (op->output.type == VSR_OP_SNAPSHOT_CAPTURE) {
            sequence =
                ((const struct vsr_snapshot_task *)op->output.data)->sequence;
        }
        if (sequence < floor)
            floor = sequence;
    }
    if (floor <= p->reclaim_sequence)
        return false;
    if (!vsr_protocol_emit(v, VSR_OP_RECLAIM, floor, VSR_TAG_RECLAIM, NULL,
                           NULL, 0, 0))
        return false;
    p->reclaim_sequence = floor;
    p->reclaim_busy = true;
    return true;
}

bool vsr_protocol_poll(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    if (boot_poll(v))
        return true;
    if (p->results_pending && p->results_sequence != 0 &&
        p->results_sequence <= v->status.stored_sequence) {
        p->results_pending = false;
        vsr_lease_release(v, p->results_lease);
        p->results_lease = VSR_INDEX_NONE;
        return true;
    }
    if (p->log_end - p->written_end < v->options.limits.batch_entries) {
        for (uint32_t i = 0; i < v->options.limits.pending_requests; ++i)
            if (p->routes[i].state == VSR_ROUTE_DECIDE &&
                decide_route(v, &p->routes[i]))
                return true;
    }
    if (store_poll(v))
        return true;
    if (sync_poll(v))
        return true;
    if (vsr_extension_poll(v))
        return true;
    if (commit_poll(v))
        return true;
    if (p->notified_commit < p->stable_commit) {
        /* A boundary announcement changes the membership immediately. Issue
         * the final old-group commit first, while its envelope is still valid. */
        bool commits_sent = true;
        if (v->status.state == VSR_STATE_NORMAL &&
            v->status.primary == v->options.replica) {
            for (uint32_t i = 0; i < p->current.count; ++i)
                if (i != p->self && p->peers[i].commit_sent < p->stable_commit)
                    commits_sent = false;
        }
        if (!commits_sent) {
            if (network_poll(v))
                return true;
        } else {
            uint64_t next = p->notified_commit + 1;
            if (next < p->readable_begin) {
                /* A durably published checkpoint already covers the
                 * compacted prefix, whether or not its trim is durable yet. */
                p->notified_commit = p->readable_begin - 1;
                return true;
            }
            struct vsr_log_slot *s = vsr_protocol_log_find(v, next);
            if (s == NULL) {
                if (vsr_protocol_load_log(v, next, p->stable_commit + 1))
                    return true;
            } else {
                p->notified_commit = next;
                vsr_extension_committed(v, &s->entry);
                return true;
            }
        }
    }
    if (routes_poll(v))
        return true;
    if (apply_poll(v))
        return true;
    if (network_poll(v))
        return true;
    /* Cache pressure cannot consume the completion/proposal progress reserve.
     * Eviction is safe once STORE owns the durable index, even before APPLY. */
    if (v->lease_free_count <= v->reserved_leases + 2u ||
        vsr_protocol_available_bytes(v) < v->progress_bytes) {
        for (uint32_t i = 0; i < v->options.limits.log_cache_entries; ++i) {
            struct vsr_log_slot *s = &p->log[i];
            if (s->used && s->sequence != 0 &&
                s->sequence <= v->status.stored_sequence) {
                log_release(v, s);
                return true;
            }
        }
    }
    return reclaim_poll(v);
}

uint64_t vsr_protocol_deadline(const struct vsr *v)
{
    const struct vsr_protocol *p = vsr_protocol_const(v);
    uint64_t next = p->heartbeat_at;
    if (p->append_at < next)
        next = p->append_at;
    for (uint32_t i = 0; i < p->current.count; ++i)
        if (p->peers[i].retry_at < next)
            next = p->peers[i].retry_at;
    uint64_t extension = vsr_extension_deadline(v);
    return extension < next ? extension : next;
}

void vsr_protocol_stop(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    if (p->stopped)
        return;
    p->stopped = true;
    vsr_extension_stop(v);
    for (uint32_t i = 0; i < v->options.limits.pending_requests; ++i)
        route_release(v, &p->routes[i]);
    for (uint32_t i = 0; i < v->options.limits.log_cache_entries; ++i)
        log_release(v, &p->log[i]);
    vsr_lease_release(v, p->results_lease);
    p->results_lease = VSR_INDEX_NONE;
}
