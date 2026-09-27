#include "config.h"

#include "checkpoint.h"

#include "checked.h"
#include "extension.h"
#include "transition.h"

#include <stdalign.h>
#include <string.h>

enum checkpoint_tag {
    CHECKPOINT_CAPTURE = VSR_TAG_CHECKPOINT_FIRST,
    CHECKPOINT_FETCH,
    CHECKPOINT_SYNC,
    CHECKPOINT_INSTALL,
    CHECKPOINT_DROP,
    CHECKPOINT_ADVERTISE
};

enum checkpoint_stage {
    CHECKPOINT_IDLE,
    CHECKPOINT_CAPTURE_PENDING,
    CHECKPOINT_CAPTURE_ACTIVE,
    CHECKPOINT_FETCH_PENDING,
    CHECKPOINT_FETCH_ACTIVE,
    CHECKPOINT_SYNC_PENDING,
    CHECKPOINT_SYNC_ACTIVE,
    CHECKPOINT_STORE_PENDING,
    CHECKPOINT_STORE_ACTIVE,
    CHECKPOINT_INSTALL_PENDING,
    CHECKPOINT_INSTALL_ACTIVE
};

struct checkpoint_slot {
    struct vsr_checkpoint checkpoint;
    uint32_t lease;
    uint32_t pins;
    bool used;
    bool local;
    bool durable;
    bool dropping;
};

struct checkpoint_coverage {
    uint64_t replica;
    uint64_t op;
};

struct vsr_checkpoint_state {
    struct checkpoint_slot *slots;
    struct checkpoint_coverage *coverage;
    struct vsr_checkpoint remote;
    uint64_t coverage_epoch;
    uint64_t publication_sequence;
    uint64_t trim_sequence;
    uint64_t trim_first;
    uint64_t retry_at;
    uint64_t advertise_at;
    uint64_t peer;
    uint64_t capture_sequence;
    uint32_t slot_count;
    uint32_t coverage_count;
    uint32_t candidate;
    uint32_t published;
    uint32_t remote_lease;
    uint32_t advertise_cursor;
    uint32_t target_role;
    uint32_t stage;
    int adoption_status;
    bool adopting;
    bool abandon_requested;
    bool restoring;
    bool fence_owned;
    bool guarded;
    bool remote_present;
    bool advertise;
    bool retry_pending;
    bool stopped;
};

struct checkpoint_plan {
    size_t size;
    size_t alignment;
    size_t slots;
    size_t coverage;
    uint32_t count;
};

static struct vsr_checkpoint_state *state(struct vsr *v)
{
    return ((struct vsr_extension *)vsr_protocol(v)->extension)->checkpoint;
}

static const struct vsr_checkpoint_state *const_state(const struct vsr *v)
{
    return ((const struct vsr_extension *)vsr_protocol_const(v)->extension)
        ->checkpoint;
}

static bool slice(struct checkpoint_plan *plan, size_t count, size_t size,
                  size_t *offset)
{
    size_t bytes;
    const size_t padding =
        (plan->alignment - plan->size % plan->alignment) % plan->alignment;
    return vsr_size_add(plan->size, padding, offset) &&
           vsr_size_mul(count, size, &bytes) &&
           vsr_size_add(*offset, bytes, &plan->size);
}

static bool make_plan(const struct vsr_options *options,
                      struct checkpoint_plan *plan)
{
    memset(plan, 0, sizeof(*plan));
    if (options->limits.transfers > UINT32_MAX - 4u)
        return false;
    plan->count = options->limits.transfers + 4u;
    plan->size = sizeof(struct vsr_checkpoint_state);
    plan->alignment =
        options->cache_line_bytes == 0 ? 64 : options->cache_line_bytes;
    if (plan->alignment < alignof(max_align_t))
        plan->alignment = alignof(max_align_t);
    return slice(plan, plan->count, sizeof(struct checkpoint_slot),
                 &plan->slots) &&
           slice(plan, options->limits.members,
                 sizeof(struct checkpoint_coverage), &plan->coverage);
}

int vsr_checkpoint_size(const struct vsr_options *options, size_t *size,
                        size_t *alignment)
{
    struct checkpoint_plan plan;
    if (!make_plan(options, &plan))
        return VSR_ELIMIT;
    *size = plan.size;
    *alignment = plan.alignment;
    return VSR_OK;
}

void vsr_checkpoint_init(struct vsr *v, void *memory, size_t size)
{
    struct checkpoint_plan plan;
    struct vsr_checkpoint_state *checkpoint = memory;
    unsigned char *bytes = memory;
    if (!make_plan(&v->options, &plan) || size < plan.size)
        return;
    memset(memory, 0, plan.size);
    checkpoint->slots = (void *)(bytes + plan.slots);
    checkpoint->coverage = (void *)(bytes + plan.coverage);
    checkpoint->slot_count = plan.count;
    checkpoint->candidate = VSR_INDEX_NONE;
    checkpoint->published = VSR_INDEX_NONE;
    checkpoint->remote_lease = VSR_INDEX_NONE;
    checkpoint->coverage_epoch = UINT64_MAX;
    checkpoint->retry_at = VSR_NO_DEADLINE;
    checkpoint->advertise_at = VSR_NO_DEADLINE;
    checkpoint->adoption_status = VSR_IO_OK;
    for (uint32_t i = 0; i < plan.count; i++)
        checkpoint->slots[i].lease = VSR_INDEX_NONE;
    ((struct vsr_extension *)vsr_protocol(v)->extension)->checkpoint =
        checkpoint;
}

static uint32_t find_slot(const struct vsr_checkpoint_state *checkpoint,
                          struct vsr_id id)
{
    for (uint32_t i = 0; i < checkpoint->slot_count; i++) {
        if (checkpoint->slots[i].used &&
            vsr_id_equal(checkpoint->slots[i].checkpoint.id, id))
            return i;
    }
    return VSR_INDEX_NONE;
}

static uint32_t free_slot(const struct vsr_checkpoint_state *checkpoint)
{
    for (uint32_t i = 0; i < checkpoint->slot_count; i++) {
        if (!checkpoint->slots[i].used)
            return i;
    }
    return VSR_INDEX_NONE;
}

static bool same_snapshot(const struct vsr_checkpoint *a,
                          const struct vsr_checkpoint *b)
{
    return vsr_id_equal(a->id, b->id) && a->op == b->op && a->view == b->view &&
           vsr_epoch_equal(a->epoch, b->epoch);
}

static bool retain_slot(struct vsr *v, uint32_t index,
                        const struct vsr_checkpoint *object, uint32_t lease)
{
    struct checkpoint_slot *slot = &state(v)->slots[index];
    if (!vsr_lease_retain(v, lease))
        return false;
    vsr_lease_release(v, slot->lease);
    slot->checkpoint = *object;
    slot->lease = lease;
    slot->used = true;
    return true;
}

static void release_slot(struct vsr *v, uint32_t index)
{
    struct checkpoint_slot *slot = &state(v)->slots[index];
    vsr_lease_release(v, slot->lease);
    memset(slot, 0, sizeof(*slot));
    slot->lease = VSR_INDEX_NONE;
}

static void release_remote(struct vsr *v)
{
    struct vsr_checkpoint_state *checkpoint = state(v);
    vsr_lease_release(v, checkpoint->remote_lease);
    checkpoint->remote_lease = VSR_INDEX_NONE;
    checkpoint->remote_present = false;
    memset(&checkpoint->remote, 0, sizeof(checkpoint->remote));
}

static void release_fence(struct vsr *v)
{
    struct vsr_checkpoint_state *checkpoint = state(v);
    if (checkpoint->fence_owned) {
        vsr_protocol(v)->application_busy = false;
        checkpoint->fence_owned = false;
    }
}

static void finish(struct vsr *v, int status)
{
    struct vsr_checkpoint_state *checkpoint = state(v);
    release_fence(v);
    checkpoint->stage = CHECKPOINT_IDLE;
    checkpoint->candidate = VSR_INDEX_NONE;
    checkpoint->publication_sequence = 0;
    checkpoint->restoring = false;
    checkpoint->adopting = false;
    checkpoint->abandon_requested = false;
    checkpoint->adoption_status = status;
}

static void abandon_adoption(struct vsr *v)
{
    struct vsr_checkpoint_state *checkpoint = state(v);
    const uint32_t index = checkpoint->candidate;
    /* A successful FETCH acquired a local hold. Ordinary DROP releases it
     * after operation pins and donor obligations end. A remote descriptor
     * alone carries no adapter hold and can be released immediately. */
    if (index != VSR_INDEX_NONE && index != checkpoint->published &&
        !checkpoint->slots[index].local && checkpoint->slots[index].pins == 0)
        release_slot(v, index);
    finish(v, VSR_IO_CANCELLED);
}

void vsr_checkpoint_cancel_adoption(struct vsr *v)
{
    struct vsr_checkpoint_state *checkpoint = state(v);
    if (!checkpoint->adopting)
        return;
    switch (checkpoint->stage) {
    case CHECKPOINT_FETCH_PENDING:
    case CHECKPOINT_SYNC_PENDING:
    case CHECKPOINT_STORE_PENDING:
        abandon_adoption(v);
        break;
    case CHECKPOINT_FETCH_ACTIVE:
    case CHECKPOINT_SYNC_ACTIVE:
        checkpoint->abandon_requested = true;
        break;
    case CHECKPOINT_INSTALL_PENDING:
        if (checkpoint->candidate == VSR_INDEX_NONE)
            abandon_adoption(v);
        break;
    case CHECKPOINT_STORE_ACTIVE:
    case CHECKPOINT_INSTALL_ACTIVE:
    case CHECKPOINT_IDLE:
    case CHECKPOINT_CAPTURE_PENDING:
    case CHECKPOINT_CAPTURE_ACTIVE:
        break;
    default:
        vsr_fail(v, VSR_FAILURE_INVARIANT, NULL, VSR_IO_OK);
        break;
    }
}

static void retry_later(struct vsr *v)
{
    struct vsr_checkpoint_state *checkpoint = state(v);
    checkpoint->retry_pending = true;
    checkpoint->retry_at = vsr_after(v, v->options.retry_ns);
}

const struct vsr_checkpoint *vsr_checkpoint_published(const struct vsr *v,
                                                      uint32_t *lease)
{
    const struct vsr_checkpoint_state *checkpoint = const_state(v);
    if (lease != NULL)
        *lease = VSR_INDEX_NONE;
    if (checkpoint == NULL || checkpoint->published == VSR_INDEX_NONE)
        return NULL;
    if (lease != NULL)
        *lease = checkpoint->slots[checkpoint->published].lease;
    return &checkpoint->slots[checkpoint->published].checkpoint;
}

bool vsr_checkpoint_pin(struct vsr *v, struct vsr_id id)
{
    struct vsr_checkpoint_state *checkpoint = state(v);
    const uint32_t index = find_slot(checkpoint, id);
    if (index == VSR_INDEX_NONE || checkpoint->slots[index].dropping ||
        checkpoint->slots[index].pins == UINT32_MAX)
        return false;
    checkpoint->slots[index].pins++;
    return true;
}

void vsr_checkpoint_unpin(struct vsr *v, struct vsr_id id)
{
    struct vsr_checkpoint_state *checkpoint = state(v);
    const uint32_t index = find_slot(checkpoint, id);
    if (index == VSR_INDEX_NONE || checkpoint->slots[index].pins == 0) {
        vsr_fail(v, VSR_FAILURE_INVARIANT, NULL, VSR_IO_OK);
        return;
    }
    checkpoint->slots[index].pins--;
}

void vsr_checkpoint_guard(struct vsr *v, bool protected)
{
    state(v)->guarded = protected;
}

bool vsr_checkpoint_busy(const struct vsr *v)
{
    const struct vsr_checkpoint_state *checkpoint = const_state(v);
    return checkpoint != NULL && checkpoint->stage != CHECKPOINT_IDLE;
}

int vsr_checkpoint_adoption_status(const struct vsr *v)
{
    return const_state(v)->adoption_status;
}

bool vsr_checkpoint_revision_ready(const struct vsr *v)
{
    const struct vsr_checkpoint_state *checkpoint = const_state(v);
    const uint64_t safe = vsr_protocol_const(v)->safe_sequence;
    return !(checkpoint->stage == CHECKPOINT_STORE_ACTIVE &&
             checkpoint->publication_sequence <= safe) &&
           !(checkpoint->trim_sequence != 0 &&
             checkpoint->trim_sequence <= safe);
}

bool vsr_checkpoint_recover(struct vsr *v, const struct vsr_checkpoint *object,
                            uint32_t lease, uint32_t role)
{
    struct vsr_checkpoint_state *checkpoint = state(v);
    uint32_t index;
    if (checkpoint->stage != CHECKPOINT_IDLE || object == NULL)
        return false;
    index = free_slot(checkpoint);
    if (index == VSR_INDEX_NONE || !retain_slot(v, index, object, lease))
        return false;
    checkpoint->slots[index].local = role == VSR_MEMBER_FULL;
    checkpoint->slots[index].durable = true;
    checkpoint->published = index;
    v->status.checkpoint_op = object->op;
    checkpoint->advertise = role == VSR_MEMBER_FULL;
    checkpoint->advertise_cursor = 0;
    if (role == VSR_MEMBER_FULL) {
        checkpoint->candidate = index;
        checkpoint->target_role = role;
        checkpoint->publication_sequence = vsr_protocol(v)->safe_sequence;
        checkpoint->adoption_status = VSR_IO_RETRY;
        checkpoint->stage = CHECKPOINT_INSTALL_PENDING;
    }
    return true;
}

int vsr_checkpoint_adopt(struct vsr *v, const struct vsr_checkpoint *object,
                         uint32_t lease, uint64_t peer, uint32_t role)
{
    struct vsr_checkpoint_state *checkpoint = state(v);
    struct vsr_protocol *protocol = vsr_protocol(v);
    uint32_t index = VSR_INDEX_NONE;
    bool existing = false;
    if (checkpoint->stage != CHECKPOINT_IDLE || protocol->application_busy ||
        protocol->results_pending || checkpoint->trim_sequence != 0)
        return VSR_AGAIN;
    if (role != VSR_MEMBER_FULL && role != VSR_MEMBER_WITNESS)
        return VSR_EINVAL;
    if (object != NULL) {
        index = find_slot(checkpoint, object->id);
        existing = index != VSR_INDEX_NONE;
        if (existing) {
            if (checkpoint->slots[index].dropping)
                return VSR_AGAIN;
            if (!same_snapshot(&checkpoint->slots[index].checkpoint, object))
                return VSR_EINVAL;
        } else {
            index = free_slot(checkpoint);
            if (index == VSR_INDEX_NONE)
                return VSR_AGAIN;
            if (!retain_slot(v, index, object, lease))
                return VSR_AGAIN;
        }
    }
    checkpoint->candidate = index;
    checkpoint->target_role = role;
    checkpoint->peer = peer;
    checkpoint->adopting = true;
    checkpoint->abandon_requested = false;
    checkpoint->restoring = object != NULL;
    checkpoint->adoption_status = VSR_IO_RETRY;
    protocol->application_busy = true;
    checkpoint->fence_owned = true;
    if (object == NULL) {
        if (role == VSR_MEMBER_WITNESS) {
            finish(v, VSR_IO_OK);
            return VSR_OK;
        }
        checkpoint->stage = CHECKPOINT_INSTALL_PENDING;
    } else if (role == VSR_MEMBER_WITNESS) {
        checkpoint->stage = CHECKPOINT_STORE_PENDING;
    } else if (!existing || !checkpoint->slots[index].local) {
        checkpoint->stage = CHECKPOINT_FETCH_PENDING;
    } else {
        checkpoint->stage = v->options.durability == VSR_DURABLE &&
                                    !checkpoint->slots[index].durable
                                ? CHECKPOINT_SYNC_PENDING
                                : CHECKPOINT_STORE_PENDING;
    }
    return VSR_OK;
}

static void reset_coverage(struct vsr *v)
{
    struct vsr_checkpoint_state *checkpoint = state(v);
    const struct vsr_protocol *protocol = vsr_protocol(v);
    if (checkpoint->coverage_epoch == protocol->current.epoch)
        return;
    checkpoint->coverage_epoch = protocol->current.epoch;
    checkpoint->coverage_count = protocol->current.count;
    for (uint32_t i = 0; i < protocol->current.count; i++) {
        checkpoint->coverage[i].replica = protocol->current.members[i].id;
        checkpoint->coverage[i].op = 0;
    }
    release_remote(v);
}

static uint32_t covered(const struct vsr *v, uint64_t op)
{
    const struct vsr_checkpoint_state *checkpoint = const_state(v);
    const struct vsr_protocol *protocol = vsr_protocol_const(v);
    uint32_t count = 0;
    for (uint32_t i = 0; i < checkpoint->coverage_count; i++) {
        if (protocol->current.members[i].role == VSR_MEMBER_FULL &&
            checkpoint->coverage[i].op >= op)
            count++;
    }
    return count;
}

/* A witness may discard retained entries through op only once f + 1 full
 * members advertise coverage of them. Entries it never retained, such as an
 * empty log adopting a donor's newer anchor, need no coverage. */
static bool prefix_covered(const struct vsr *v, uint64_t op)
{
    const struct vsr_protocol *protocol = vsr_protocol_const(v);
    if (protocol->written_end <= protocol->log_begin)
        return true;
    uint64_t last = op < protocol->written_end ? op : protocol->written_end - 1;
    return last < protocol->log_begin ||
           covered(v, last) >= protocol->current.faults + 1u;
}

/* Optional maintenance must not turn a completed snapshot into a permanent
 * consumer of the resources needed to admit the next maximal command. The
 * planner supplies the baseline; old pinned images can temporarily defer hints.
 * The extra arguments credit capacity that the caller could release first. */
static bool maintenance_capacity(const struct vsr *v, bool new_completion,
                                 uint32_t extra_leases, uint64_t extra_bytes)
{
    uint64_t bytes = vsr_protocol_available_bytes(v) + extra_bytes;
    uint32_t leases = v->lease_free_count - v->reserved_leases + extra_leases;
    /* Three transient transfer leases plus the ordinary three-slot reserve. */
    if (leases < (new_completion ? 7u : 6u))
        return false;
    if (new_completion) {
        if (bytes < v->options.limits.manifest_bytes)
            return false;
        bytes -= v->options.limits.manifest_bytes;
    }
    if (bytes < v->options.limits.command_bytes)
        return false;
    bytes -= v->options.limits.command_bytes;
    if (bytes < v->options.limits.command_bytes)
        return false;
    bytes -= v->options.limits.command_bytes;
    if (bytes < v->options.limits.result_bytes)
        return false;
    bytes -= v->options.limits.result_bytes;
    if (bytes < v->options.limits.manifest_bytes)
        return false;
    bytes -= v->options.limits.manifest_bytes;
    return bytes >= v->options.limits.message_bytes;
}

/* A retained input graph frees its lease and bytes only when nothing else
 * still references it. */
static bool lease_credit(const struct vsr *v, uint32_t lease, uint32_t *leases,
                         uint64_t *bytes)
{
    if (lease == VSR_INDEX_NONE || v->leases[lease].references != 1)
        return false;
    (*leases)++;
    *bytes += v->leases[lease].bytes;
    return true;
}

/* Cache entries that are applied, stored, and already announced to the
 * extensions are optional: no pending notification or replay reloads them. */
static bool maintenance_optional(const struct vsr *v,
                                 const struct vsr_log_slot *slot)
{
    const struct vsr_protocol *protocol = vsr_protocol_const(v);
    return slot->used && slot->entry.op <= v->status.applied &&
           slot->entry.op <= protocol->notified_commit && slot->sequence != 0 &&
           slot->sequence <= v->status.stored_sequence;
}

/* Release one optional cache entry, but only when releasing every optional
 * entry restores the capture baseline. Evicting an entry the protocol is about
 * to reload, or evicting while capture stays deferred anyway, would trade a
 * deferred hint for an endless load/evict cycle instead of idling. */
static bool maintenance_evict(struct vsr *v)
{
    struct vsr_protocol *protocol = vsr_protocol(v);
    uint32_t leases = 0;
    uint64_t bytes = 0;
    uint32_t chosen = VSR_INDEX_NONE;
    for (uint32_t i = 0; i < v->options.limits.log_cache_entries; i++) {
        const struct vsr_log_slot *slot = &protocol->log[i];
        if (maintenance_optional(v, slot) &&
            lease_credit(v, slot->lease, &leases, &bytes) &&
            chosen == VSR_INDEX_NONE)
            chosen = i;
    }
    if (chosen == VSR_INDEX_NONE ||
        !maintenance_capacity(v, true, leases, bytes))
        return false;
    struct vsr_log_slot *slot = &protocol->log[chosen];
    vsr_lease_release(v, slot->lease);
    slot->lease = VSR_INDEX_NONE;
    slot->used = false;
    return true;
}

int vsr_checkpoint_event(struct vsr *v, const struct vsr_event *event,
                         uint32_t lease)
{
    struct vsr_checkpoint_state *checkpoint = state(v);
    struct vsr_protocol *protocol = vsr_protocol(v);
    if (event->type == VSR_EVENT_CHECKPOINT) {
        if (v->status.role == VSR_MEMBER_FULL)
            protocol->checkpoint_requested = true;
        return VSR_OK;
    }
    if (event->type == VSR_EVENT_MESSAGE) {
        const struct vsr_message *message = event->data;
        const struct vsr_checkpoint *object;
        uint32_t member;
        if (message->type != VSR_MSG_CHECKPOINT ||
            message->epoch != protocol->current.epoch ||
            !vsr_id_equal(message->cluster, v->options.cluster))
            return VSR_OK;
        member = vsr_member_index(&protocol->current, message->from);
        if (member == VSR_INDEX_NONE ||
            protocol->current.members[member].role != VSR_MEMBER_FULL)
            return VSR_OK;
        reset_coverage(v);
        object = message->body;
        if (object->op < checkpoint->coverage[member].op)
            return VSR_OK;
        /* Retain at most one manifest: the smallest newer candidate can become
         * covered without pinning one input payload graph for every member. */
        uint32_t leases = 0;
        uint64_t bytes = 0;
        (void)lease_credit(v, checkpoint->remote_lease, &leases, &bytes);
        if (v->status.role == VSR_MEMBER_WITNESS &&
            object->op > v->status.checkpoint_op &&
            (!checkpoint->remote_present ||
             object->op < checkpoint->remote.op) &&
            maintenance_capacity(v, false, leases, bytes)) {
            if (!vsr_lease_retain(v, lease))
                return VSR_AGAIN;
            release_remote(v);
            checkpoint->remote = *object;
            checkpoint->remote_lease = lease;
            checkpoint->remote_present = true;
        }
        checkpoint->coverage[member].op = object->op;
    }
    return VSR_OK;
}

static bool emit_snapshot(struct vsr *v, uint32_t type, uint64_t tag,
                          const struct vsr_snapshot_task *task, uint32_t lease,
                          uint64_t completion_bytes)
{
    return vsr_protocol_emit(v, type, 0, tag, task, &lease, 1,
                             completion_bytes);
}

static bool capture_poll(struct vsr *v)
{
    struct vsr_checkpoint_state *checkpoint = state(v);
    struct vsr_protocol *protocol = vsr_protocol(v);
    if (v->status.state != VSR_STATE_NORMAL ||
        v->status.role != VSR_MEMBER_FULL ||
        v->status.applied < protocol->epoch.boundary) {
        release_slot(v, checkpoint->candidate);
        finish(v, VSR_IO_OK);
        return true;
    }
    const struct vsr_checkpoint object = {{0, 0},
                                          v->status.applied,
                                          protocol->applied_view,
                                          &protocol->epoch,
                                          {NULL, 0, 0, 0}};
    const struct vsr_snapshot_task task = {object.op, v->status.stored_sequence,
                                           0, &object, NULL};
    if (protocol->application_busy || protocol->results_pending ||
        protocol->clients_stored < v->status.applied)
        return false;
    if (!maintenance_capacity(v, true, 0, 0)) {
        if (maintenance_evict(v))
            return true;
        /* A pending capture counts as busy for transitions and replay. Defer
         * the hint itself instead of holding them until capacity returns. */
        release_slot(v, checkpoint->candidate);
        finish(v, VSR_IO_OK);
        return true;
    }
    if (!emit_snapshot(v, VSR_OP_SNAPSHOT_CAPTURE, CHECKPOINT_CAPTURE, &task,
                       VSR_INDEX_NONE, v->options.limits.manifest_bytes))
        return false;
    checkpoint->capture_sequence = task.sequence;
    checkpoint->stage = CHECKPOINT_CAPTURE_ACTIVE;
    checkpoint->fence_owned = true;
    protocol->application_busy = true;
    protocol->checkpoint_requested = false;
    return true;
}

static bool fetch_poll(struct vsr *v)
{
    struct vsr_checkpoint_state *checkpoint = state(v);
    struct checkpoint_slot *slot = &checkpoint->slots[checkpoint->candidate];
    const struct vsr_checkpoint *basis = vsr_checkpoint_published(v, NULL);
    struct vsr_snapshot_task task = {slot->checkpoint.op, 0, checkpoint->peer,
                                     &slot->checkpoint, basis};
    uint32_t leases[2] = {slot->lease, VSR_INDEX_NONE};
    if (checkpoint->published != VSR_INDEX_NONE)
        leases[1] = checkpoint->slots[checkpoint->published].lease;
    if (!vsr_protocol_emit(v, VSR_OP_SNAPSHOT_FETCH, 0, CHECKPOINT_FETCH, &task,
                           leases, 2, v->options.limits.manifest_bytes))
        return false;
    checkpoint->stage = CHECKPOINT_FETCH_ACTIVE;
    return true;
}

static bool sync_poll(struct vsr *v)
{
    struct vsr_checkpoint_state *checkpoint = state(v);
    struct checkpoint_slot *slot = &checkpoint->slots[checkpoint->candidate];
    const struct vsr_snapshot_task task = {slot->checkpoint.op, 0, 0,
                                           &slot->checkpoint, NULL};
    if (!emit_snapshot(v, VSR_OP_SNAPSHOT_SYNC, CHECKPOINT_SYNC, &task,
                       slot->lease, 0))
        return false;
    checkpoint->stage = CHECKPOINT_SYNC_ACTIVE;
    return true;
}

static bool store_poll(struct vsr *v)
{
    struct vsr_checkpoint_state *checkpoint = state(v);
    struct vsr_protocol *protocol = vsr_protocol(v);
    struct checkpoint_slot *slot = &checkpoint->slots[checkpoint->candidate];
    struct vsr_hard_state hard;
    struct vsr_change changes[2];
    uint32_t count = 1;
    uint64_t end = protocol->written_end;
    uint64_t committed = 0;
    /* RESTORE removes a prefix just as TRIM does. A remote anchor does not
     * replace the independent full-member retention promises. */
    if (checkpoint->restoring &&
        checkpoint->target_role == VSR_MEMBER_WITNESS &&
        slot->checkpoint.op + 1 > protocol->log_begin &&
        !prefix_covered(v, slot->checkpoint.op))
        return false;
    changes[0] = (struct vsr_change){checkpoint->restoring
                                         ? VSR_STORE_RESTORE_CHECKPOINT
                                         : VSR_STORE_PUBLISH_CHECKPOINT,
                                     1, 0, &slot->checkpoint};
    if (checkpoint->restoring) {
        vsr_protocol_hard(v, &hard);
        hard.role = checkpoint->target_role;
        hard.committed = protocol->stable_commit > slot->checkpoint.op
                             ? protocol->stable_commit
                             : slot->checkpoint.op;
        vsr_transition_hard(v, &hard);
        committed = hard.committed;
        if (end <= slot->checkpoint.op)
            end = slot->checkpoint.op + 1;
        changes[count++] =
            (struct vsr_change){VSR_STORE_HARD_STATE, 1, 0, &hard};
    }
    if (!vsr_protocol_store(v, changes, count, &slot->lease, 1, end, committed,
                            0, &checkpoint->publication_sequence))
        return false;
    if (checkpoint->restoring) {
        protocol->hard_sequence = checkpoint->publication_sequence;
        if (protocol->written_end < end)
            protocol->written_end = end;
        if (protocol->log_end < end)
            protocol->log_end = end;
        if (protocol->desired_commit < committed)
            protocol->desired_commit = committed;
    }
    checkpoint->stage = CHECKPOINT_STORE_ACTIVE;
    return true;
}

static void forget_prefix(struct vsr *v, uint64_t first)
{
    struct vsr_protocol *protocol = vsr_protocol(v);
    for (uint32_t i = 0; i < v->options.limits.log_cache_entries; i++) {
        struct vsr_log_slot *slot = &protocol->log[i];
        if (slot->used && slot->entry.op < first) {
            vsr_lease_release(v, slot->lease);
            slot->lease = VSR_INDEX_NONE;
            slot->used = false;
        }
    }
    protocol->log_begin = first;
}

static bool publication_poll(struct vsr *v)
{
    struct vsr_checkpoint_state *checkpoint = state(v);
    struct vsr_protocol *protocol = vsr_protocol(v);
    struct checkpoint_slot *slot = &checkpoint->slots[checkpoint->candidate];
    if (protocol->safe_sequence < checkpoint->publication_sequence)
        return false;
    checkpoint->published = checkpoint->candidate;
    v->status.checkpoint_op = slot->checkpoint.op;
    checkpoint->advertise = slot->local;
    checkpoint->advertise_cursor = 0;
    checkpoint->advertise_at = VSR_NO_DEADLINE;
    if (checkpoint->restoring) {
        forget_prefix(v, slot->checkpoint.op + 1);
        vsr_protocol_history_changed(v);
        protocol->clients_stored = slot->checkpoint.op;
        protocol->clients_sequence = checkpoint->publication_sequence;
        if (checkpoint->target_role == VSR_MEMBER_FULL) {
            checkpoint->stage = CHECKPOINT_INSTALL_PENDING;
        } else {
            v->status.role = VSR_MEMBER_WITNESS;
            v->status.applied = 0;
            finish(v, VSR_IO_OK);
        }
    } else {
        finish(v, VSR_IO_OK);
    }
    vsr_changed(v);
    return true;
}

static bool install_poll(struct vsr *v)
{
    struct vsr_checkpoint_state *checkpoint = state(v);
    struct vsr_protocol *protocol = vsr_protocol(v);
    const struct checkpoint_slot *slot =
        checkpoint->candidate == VSR_INDEX_NONE
            ? NULL
            : &checkpoint->slots[checkpoint->candidate];
    const struct vsr_snapshot_task task = {
        slot == NULL ? 0 : slot->checkpoint.op,
        slot == NULL ? 0 : checkpoint->publication_sequence, 0,
        slot == NULL ? NULL : &slot->checkpoint, NULL};
    if (protocol->application_busy && !checkpoint->fence_owned)
        return false;
    if (!emit_snapshot(v, VSR_OP_SNAPSHOT_INSTALL, CHECKPOINT_INSTALL, &task,
                       slot == NULL ? VSR_INDEX_NONE : slot->lease, 0))
        return false;
    protocol->application_busy = true;
    checkpoint->fence_owned = true;
    checkpoint->stage = CHECKPOINT_INSTALL_ACTIVE;
    return true;
}

static bool trim_poll(struct vsr *v)
{
    struct vsr_checkpoint_state *checkpoint = state(v);
    struct vsr_protocol *protocol = vsr_protocol(v);
    const struct vsr_checkpoint *published = vsr_checkpoint_published(v, NULL);
    if (checkpoint->trim_sequence != 0) {
        if (protocol->safe_sequence < checkpoint->trim_sequence)
            return false;
        forget_prefix(v, checkpoint->trim_first);
        checkpoint->trim_sequence = 0;
        return true;
    }
    if (published == NULL || published->op + 1 <= protocol->log_begin ||
        checkpoint->stage != CHECKPOINT_IDLE || protocol->log_loading)
        return false;
    if (v->status.role == VSR_MEMBER_WITNESS &&
        (checkpoint->guarded || !prefix_covered(v, published->op)))
        return false;
    {
        const struct vsr_change change = {VSR_STORE_TRIM, 0, published->op + 1,
                                          NULL};
        if (!vsr_protocol_store(v, &change, 1, NULL, 0, protocol->written_end,
                                0, 0, &checkpoint->trim_sequence))
            return false;
    }
    checkpoint->trim_first = published->op + 1;
    return true;
}

static bool drop_poll(struct vsr *v)
{
    struct vsr_checkpoint_state *checkpoint = state(v);
    if (checkpoint->guarded)
        return false;
    for (uint32_t i = 0; i < checkpoint->slot_count; i++) {
        struct checkpoint_slot *slot = &checkpoint->slots[i];
        const bool demoted_anchor = i == checkpoint->published && slot->local &&
                                    v->status.role == VSR_MEMBER_WITNESS &&
                                    covered(v, slot->checkpoint.op) >=
                                        vsr_protocol(v)->current.faults + 1u;
        if (!slot->used || i == checkpoint->candidate ||
            (i == checkpoint->published && !demoted_anchor) ||
            slot->pins != 0 || slot->dropping)
            continue;
        if (!slot->local) {
            release_slot(v, i);
            return true;
        }
        {
            const struct vsr_snapshot_task task = {slot->checkpoint.op, 0, 0,
                                                   &slot->checkpoint, NULL};
            if (!emit_snapshot(v, VSR_OP_SNAPSHOT_DROP, CHECKPOINT_DROP, &task,
                               slot->lease, 0))
                return false;
        }
        slot->dropping = true;
        return true;
    }
    return false;
}

static bool advertisement_poll(struct vsr *v)
{
    struct vsr_checkpoint_state *checkpoint = state(v);
    struct vsr_protocol *protocol = vsr_protocol(v);
    struct checkpoint_slot *slot;
    if (checkpoint->published == VSR_INDEX_NONE ||
        v->status.state != VSR_STATE_NORMAL ||
        v->status.role != VSR_MEMBER_FULL)
        return false;
    slot = &checkpoint->slots[checkpoint->published];
    if (!slot->local)
        return false;
    if (!checkpoint->advertise && v->time_set &&
        checkpoint->advertise_at == VSR_NO_DEADLINE) {
        checkpoint->advertise_at = vsr_after(v, v->options.heartbeat_ns);
        return true;
    }
    if (!checkpoint->advertise && v->time_set &&
        checkpoint->advertise_at <= v->now) {
        checkpoint->advertise = true;
        checkpoint->advertise_cursor = 0;
    }
    if (!checkpoint->advertise)
        return false;
    while (checkpoint->advertise_cursor < protocol->current.count) {
        const uint64_t peer =
            protocol->current.members[checkpoint->advertise_cursor].id;
        if (peer == v->options.replica) {
            checkpoint->advertise_cursor++;
            continue;
        }
        {
            const struct vsr_message message = {
                v->options.cluster,  v->status.epoch,    v->status.view,
                v->options.replica,  VSR_MSG_CHECKPOINT, 0,
                slot->checkpoint.op, &slot->checkpoint};
            if (!vsr_protocol_emit(v, VSR_OP_SEND, peer, CHECKPOINT_ADVERTISE,
                                   &message, &slot->lease, 1, 0))
                return false;
        }
        checkpoint->advertise_cursor++;
        return true;
    }
    checkpoint->advertise = false;
    checkpoint->advertise_at = vsr_after(v, v->options.heartbeat_ns);
    return true;
}

bool vsr_checkpoint_poll(struct vsr *v)
{
    struct vsr_checkpoint_state *checkpoint = state(v);
    struct vsr_protocol *protocol = vsr_protocol(v);
    if (checkpoint == NULL || checkpoint->stopped)
        return false;
    reset_coverage(v);
    if (checkpoint->retry_pending) {
        if (!v->time_set)
            return false;
        if (checkpoint->retry_at == VSR_NO_DEADLINE) {
            checkpoint->retry_at = vsr_after(v, v->options.retry_ns);
            return true;
        }
        if (v->now < checkpoint->retry_at)
            return false;
        checkpoint->retry_pending = false;
        checkpoint->retry_at = VSR_NO_DEADLINE;
    }
    switch (checkpoint->stage) {
    case CHECKPOINT_CAPTURE_PENDING:
        return capture_poll(v);
    case CHECKPOINT_FETCH_PENDING:
        return fetch_poll(v);
    case CHECKPOINT_SYNC_PENDING:
        return sync_poll(v);
    case CHECKPOINT_STORE_PENDING:
        return store_poll(v);
    case CHECKPOINT_STORE_ACTIVE:
        return publication_poll(v);
    case CHECKPOINT_INSTALL_PENDING:
        return install_poll(v);
    case CHECKPOINT_CAPTURE_ACTIVE:
    case CHECKPOINT_FETCH_ACTIVE:
    case CHECKPOINT_SYNC_ACTIVE:
    case CHECKPOINT_INSTALL_ACTIVE:
        return false;
    case CHECKPOINT_IDLE:
        break;
    default:
        vsr_fail(v, VSR_FAILURE_INVARIANT, NULL, VSR_IO_OK);
        return true;
    }
    if (trim_poll(v) || drop_poll(v))
        return true;
    if (checkpoint->remote_present && v->status.role == VSR_MEMBER_WITNESS &&
        !checkpoint->guarded &&
        checkpoint->remote.op <= protocol->stable_commit &&
        checkpoint->remote.op > v->status.checkpoint_op &&
        covered(v, checkpoint->remote.op) >= protocol->current.faults + 1u) {
        const uint32_t index = free_slot(checkpoint);
        if (index != VSR_INDEX_NONE &&
            retain_slot(v, index, &checkpoint->remote,
                        checkpoint->remote_lease)) {
            checkpoint->candidate = index;
            checkpoint->target_role = VSR_MEMBER_WITNESS;
            checkpoint->restoring = false;
            checkpoint->stage = CHECKPOINT_STORE_PENDING;
            release_remote(v);
            return true;
        }
    }
    if (v->status.state == VSR_STATE_NORMAL &&
        v->status.role == VSR_MEMBER_FULL && !protocol->application_busy &&
        !protocol->results_pending &&
        protocol->clients_stored >= v->status.applied &&
        (protocol->checkpoint_requested ||
         (v->options.checkpoint_interval != 0 &&
          v->status.applied >= v->status.checkpoint_op &&
          v->status.applied - v->status.checkpoint_op >=
              v->options.checkpoint_interval))) {
        if (checkpoint->published != VSR_INDEX_NONE &&
            v->status.applied <= v->status.checkpoint_op) {
            protocol->checkpoint_requested = false;
        } else {
            if (!maintenance_capacity(v, true, 0, 0))
                return maintenance_evict(v);
            const uint32_t index = free_slot(checkpoint);
            if (index != VSR_INDEX_NONE) {
                checkpoint->slots[index].used = true;
                checkpoint->candidate = index;
                checkpoint->target_role = VSR_MEMBER_FULL;
                checkpoint->restoring = false;
                checkpoint->stage = CHECKPOINT_CAPTURE_PENDING;
                return true;
            }
        }
    }
    return advertisement_poll(v);
}

void vsr_checkpoint_complete(struct vsr *v, struct vsr_operation *operation,
                             const struct vsr_event *event, uint32_t lease)
{
    struct vsr_checkpoint_state *checkpoint = state(v);
    struct vsr_protocol *protocol = vsr_protocol(v);
    const struct vsr_snapshot_task *task = operation->output.data;
    if (checkpoint->stopped)
        return;
    if (operation->tag == CHECKPOINT_ADVERTISE) {
        if (event->status != VSR_IO_OK)
            checkpoint->advertise_at = vsr_after(v, v->options.retry_ns);
        return;
    }
    if (operation->tag == CHECKPOINT_DROP) {
        const uint32_t index = find_slot(checkpoint, task->checkpoint->id);
        if (index == VSR_INDEX_NONE) {
            vsr_fail(v, VSR_FAILURE_INVARIANT, operation, event->status);
            return;
        }
        checkpoint->slots[index].dropping = false;
        if (event->status == VSR_IO_OK) {
            if (index == checkpoint->published) {
                checkpoint->slots[index].local = false;
                checkpoint->slots[index].durable = false;
            } else {
                release_slot(v, index);
            }
        } else {
            retry_later(v);
        }
        return;
    }
    if (operation->tag == CHECKPOINT_INSTALL) {
        v->status.applied = task->op;
        protocol->applied_view =
            task->checkpoint == NULL ? 0 : task->checkpoint->view;
        /* An intact recovered anchor already owns its indexed client base;
         * unlike adoption it does not pass through RESTORE publication. */
        if (task->checkpoint != NULL && protocol->clients_stored < task->op) {
            protocol->clients_stored = task->op;
            protocol->clients_sequence = task->sequence;
        }
        protocol->replay = task->op < protocol->stable_commit;
        v->status.role = checkpoint->target_role;
        finish(v, VSR_IO_OK);
        vsr_changed(v);
        return;
    }
    if (checkpoint->candidate == VSR_INDEX_NONE) {
        vsr_fail(v, VSR_FAILURE_INVARIANT, operation, event->status);
        return;
    }
    if (operation->tag == CHECKPOINT_SYNC) {
        checkpoint->slots[checkpoint->candidate].durable = true;
        if (checkpoint->abandon_requested) {
            abandon_adoption(v);
            return;
        }
        checkpoint->stage = CHECKPOINT_STORE_PENDING;
        return;
    }
    if (operation->tag == CHECKPOINT_CAPTURE ||
        operation->tag == CHECKPOINT_FETCH) {
        struct checkpoint_slot *slot =
            &checkpoint->slots[checkpoint->candidate];
        const struct vsr_checkpoint *object = event->data;
        if (event->status != VSR_IO_OK) {
            const bool capture = operation->tag == CHECKPOINT_CAPTURE;
            if (!slot->local && checkpoint->candidate != checkpoint->published)
                release_slot(v, checkpoint->candidate);
            finish(v, event->status);
            if (capture) {
                protocol->checkpoint_requested = true;
                retry_later(v);
            }
            return;
        }
        if (object == NULL ||
            (operation->tag == CHECKPOINT_FETCH &&
             !same_snapshot(task->checkpoint, object)) ||
            (operation->tag == CHECKPOINT_CAPTURE &&
             (task->op != object->op ||
              task->checkpoint->view != object->view ||
              !vsr_epoch_equal(task->checkpoint->epoch, object->epoch)))) {
            vsr_fail(v, VSR_FAILURE_SNAPSHOT, operation, VSR_IO_CORRUPT);
            return;
        }
        if (operation->tag == CHECKPOINT_CAPTURE) {
            const uint32_t existing = find_slot(checkpoint, object->id);
            if (existing != VSR_INDEX_NONE &&
                existing != checkpoint->candidate) {
                vsr_fail(v, VSR_FAILURE_SNAPSHOT, operation, VSR_IO_CORRUPT);
                return;
            }
            release_fence(v);
        }
        if (!retain_slot(v, checkpoint->candidate, object, lease)) {
            vsr_fail(v, VSR_FAILURE_INVARIANT, operation, VSR_IO_OK);
            return;
        }
        slot->local = true;
        slot->durable = false;
        if (checkpoint->abandon_requested) {
            abandon_adoption(v);
            return;
        }
        checkpoint->stage = v->options.durability == VSR_DURABLE
                                ? CHECKPOINT_SYNC_PENDING
                                : CHECKPOINT_STORE_PENDING;
        return;
    }
    vsr_fail(v, VSR_FAILURE_INVARIANT, operation, event->status);
}

uint64_t vsr_checkpoint_deadline(const struct vsr *v)
{
    const struct vsr_checkpoint_state *checkpoint = const_state(v);
    if (checkpoint == NULL || checkpoint->stopped)
        return VSR_NO_DEADLINE;
    uint64_t result = checkpoint->retry_at;
    if (checkpoint->stage == CHECKPOINT_IDLE &&
        v->status.state == VSR_STATE_NORMAL &&
        v->status.role == VSR_MEMBER_FULL && checkpoint->advertise_at < result)
        result = checkpoint->advertise_at;
    return result;
}

uint64_t vsr_checkpoint_min_sequence(const struct vsr *v)
{
    const struct vsr_checkpoint_state *checkpoint = const_state(v);
    if (checkpoint == NULL || checkpoint->stopped)
        return UINT64_MAX;
    if (checkpoint->stage == CHECKPOINT_CAPTURE_ACTIVE)
        return checkpoint->capture_sequence;
    if (checkpoint->stage == CHECKPOINT_INSTALL_PENDING ||
        checkpoint->stage == CHECKPOINT_INSTALL_ACTIVE)
        return checkpoint->publication_sequence == 0
                   ? UINT64_MAX
                   : checkpoint->publication_sequence;
    return UINT64_MAX;
}

void vsr_checkpoint_stop(struct vsr *v)
{
    struct vsr_checkpoint_state *checkpoint = state(v);
    if (checkpoint == NULL || checkpoint->stopped)
        return;
    checkpoint->stopped = true;
    release_fence(v);
    release_remote(v);
    for (uint32_t i = 0; i < checkpoint->slot_count; i++) {
        if (checkpoint->slots[i].used)
            release_slot(v, i);
    }
    checkpoint->candidate = VSR_INDEX_NONE;
    checkpoint->published = VSR_INDEX_NONE;
    checkpoint->retry_at = VSR_NO_DEADLINE;
    checkpoint->advertise_at = VSR_NO_DEADLINE;
}
