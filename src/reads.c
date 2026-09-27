#include "config.h"

#include "reads.h"

#include "checked.h"
#include "extension.h"
#include "protocol.h"

#include <stdalign.h>
#include <string.h>

enum read_state {
    READ_FREE,
    READ_WAITING,
    READ_ROUND,
    READ_CONFIRMED,
    READ_REJECTED,
    READ_FENCED
};

enum read_tag { READ_SEND = VSR_TAG_READ_FIRST, READ_READY, READ_REPLY };

struct read_slot {
    struct vsr_read_barrier barrier;
    uint64_t cookie;
    uint64_t floor;
    uint32_t state;
    uint32_t rejection;
};

struct vsr_reads {
    struct read_slot *slots;
    unsigned char *acknowledged;
    unsigned char *sent;
    struct vsr_nonce nonce;
    uint64_t epoch;
    uint64_t view;
    uint64_t inherited_end;
    uint64_t floor;
    uint64_t retry_at;
    uint64_t noop_op;
    uint32_t confirmations;
    uint32_t cursor;
    bool established;
    bool active;
};

static struct vsr_reads *reads(struct vsr *v)
{
    struct vsr_extension *extension = vsr_protocol(v)->extension;
    return extension->reads;
}

static const struct vsr_reads *reads_const(const struct vsr *v)
{
    const struct vsr_extension *extension = vsr_protocol_const(v)->extension;
    return extension->reads;
}

static bool padded(size_t size, size_t alignment, size_t *out)
{
    size_t extra = (alignment - size % alignment) % alignment;
    return vsr_size_add(size, extra, out);
}

int vsr_reads_size(const struct vsr_options *options, size_t *size,
                   size_t *alignment)
{
    size_t slots;
    size_t bytes;
    size_t base;
    *size = 0;
    *alignment =
        options->cache_line_bytes == 0 ? 64 : options->cache_line_bytes;
    if (*alignment < alignof(max_align_t)) {
        *alignment = alignof(max_align_t);
    }
    if (!padded(sizeof(struct vsr_reads), *alignment, &base) ||
        !vsr_size_mul(options->limits.pending_reads, sizeof(struct read_slot),
                      &slots) ||
        !vsr_size_mul(options->limits.members, 2, &bytes) ||
        !vsr_size_add(base, slots, &base) ||
        !vsr_size_add(base, bytes, &base) || !padded(base, *alignment, size)) {
        return VSR_ELIMIT;
    }
    return VSR_OK;
}

void vsr_reads_init(struct vsr *v, void *memory, size_t size)
{
    struct vsr_extension *extension = vsr_protocol(v)->extension;
    struct vsr_reads *state = memory;
    unsigned char *base = memory;
    size_t offset = 0;
    size_t alignment =
        v->options.cache_line_bytes == 0 ? 64 : v->options.cache_line_bytes;
    if (alignment < alignof(max_align_t)) {
        alignment = alignof(max_align_t);
    }
    memset(memory, 0, size);
    if (!padded(sizeof(*state), alignment, &offset)) {
        vsr_fail(v, VSR_FAILURE_INVARIANT, NULL, VSR_IO_OK);
        return;
    }
    state->slots = (void *)(base + offset);
    state->acknowledged =
        base + offset +
        (size_t)v->options.limits.pending_reads * sizeof(struct read_slot);
    state->sent = state->acknowledged + v->options.limits.members;
    state->retry_at = VSR_NO_DEADLINE;
    extension->reads = state;
}

static bool full_normal(const struct vsr *v)
{
    const struct vsr_protocol *p = vsr_protocol_const(v);
    return v->status.state == VSR_STATE_NORMAL &&
           v->status.role == VSR_MEMBER_FULL &&
           p->epoch.phase != VSR_EPOCH_TRANSFERRING &&
           p->self < p->current.count &&
           p->current.members[p->self].role == VSR_MEMBER_FULL;
}

static bool primary(const struct vsr *v)
{
    return full_normal(v) && v->status.primary == v->options.replica;
}

static uint32_t rejection(const struct vsr *v)
{
    return v->status.state == VSR_STATE_NORMAL ? VSR_REPLY_NOT_PRIMARY
                                               : VSR_REPLY_BUSY;
}

static bool reject_pending(struct vsr *v, uint32_t reason)
{
    struct vsr_reads *state = reads(v);
    bool changed = state->active;
    state->active = false;
    state->retry_at = VSR_NO_DEADLINE;
    for (uint32_t i = 0; i < v->options.limits.pending_reads; ++i) {
        struct read_slot *slot = &state->slots[i];
        if (slot->state == READ_WAITING || slot->state == READ_ROUND ||
            slot->state == READ_CONFIRMED) {
            slot->state = READ_REJECTED;
            slot->rejection = reason;
            changed = true;
        }
    }
    return changed;
}

void vsr_reads_normal(struct vsr *v)
{
    struct vsr_reads *state = reads(v);
    struct vsr_protocol *p = vsr_protocol(v);
    (void)reject_pending(v, state->epoch == v->status.epoch
                                ? VSR_REPLY_BUSY
                                : VSR_REPLY_NEW_EPOCH);
    state->epoch = v->status.epoch;
    state->view = v->status.view;
    state->inherited_end = p->log_end - 1;
    state->noop_op = 0;
    state->established = false;
}

static uint64_t slot_tag(uint32_t kind, uint32_t index)
{
    return ((uint64_t)index << 32) | kind;
}

static bool send_nonce(struct vsr *v, uint64_t peer, uint32_t type,
                       uint64_t floor, const struct vsr_nonce *nonce,
                       uint32_t index)
{
    const struct vsr_message message = {v->options.cluster,
                                        v->status.epoch,
                                        v->status.view,
                                        v->options.replica,
                                        type,
                                        0,
                                        floor,
                                        nonce};
    return vsr_protocol_emit(v, VSR_OP_SEND, peer, slot_tag(READ_SEND, index),
                             &message, NULL, 0, 0);
}

static void confirm_round(struct vsr *v)
{
    struct vsr_reads *state = reads(v);
    for (uint32_t i = 0; i < v->options.limits.pending_reads; ++i) {
        if (state->slots[i].state == READ_ROUND) {
            state->slots[i].state = READ_CONFIRMED;
        }
    }
    state->active = false;
    state->retry_at = VSR_NO_DEADLINE;
}

int vsr_reads_event(struct vsr *v, const struct vsr_event *event,
                    uint32_t lease, bool *handled)
{
    struct vsr_reads *state = reads(v);
    struct vsr_protocol *p = vsr_protocol(v);
    (void)lease;
    *handled = false;
    if (event->type == VSR_EVENT_READ) {
        const struct vsr_read_barrier *barrier = event->data;
        uint32_t free_slot = VSR_INDEX_NONE;
        *handled = true;
        for (uint32_t i = 0; i < v->options.limits.pending_reads; ++i) {
            if (state->slots[i].state != READ_FREE &&
                state->slots[i].cookie == event->id) {
                return VSR_EINVAL;
            }
            if (state->slots[i].state == READ_FREE &&
                free_slot == VSR_INDEX_NONE) {
                free_slot = i;
            }
        }
        if (free_slot == VSR_INDEX_NONE) {
            return VSR_AGAIN;
        }
        struct read_slot *slot = &state->slots[free_slot];
        *slot = (struct read_slot){
            .barrier = *barrier, .cookie = event->id, .state = READ_WAITING};
        if (!full_normal(v) ||
            (barrier->consistency == VSR_READ_LINEARIZABLE && !primary(v))) {
            slot->state = READ_REJECTED;
            slot->rejection = rejection(v);
        }
        return VSR_OK;
    }
    if (event->type != VSR_EVENT_MESSAGE) {
        return VSR_OK;
    }
    const struct vsr_message *message = event->data;
    if (message->type != VSR_MSG_READ_PROBE &&
        message->type != VSR_MSG_READ_ACK) {
        return VSR_OK;
    }
    *handled = true;
    if (!vsr_id_equal(message->cluster, v->options.cluster) ||
        message->epoch != v->status.epoch || message->view != v->status.view ||
        !vsr_protocol_ready(v)) {
        return VSR_OK;
    }
    const struct vsr_nonce *nonce = message->body;
    if (message->type == VSR_MSG_READ_PROBE) {
        if (message->from != v->status.primary ||
            message->from == v->options.replica ||
            message->number >= p->stable_end) {
            return VSR_OK;
        }
        return send_nonce(v, message->from, VSR_MSG_READ_ACK, message->number,
                          nonce, VSR_INDEX_NONE)
                   ? VSR_OK
                   : VSR_AGAIN;
    }
    if (!primary(v) || !state->active || state->epoch != message->epoch ||
        state->view != message->view || message->number != state->floor ||
        !vsr_nonce_equal(*nonce, state->nonce)) {
        return VSR_OK;
    }
    uint32_t index = vsr_member_index(&p->current, message->from);
    if (index == VSR_INDEX_NONE || state->acknowledged[index] != 0) {
        return VSR_OK;
    }
    state->acknowledged[index] = 1;
    ++state->confirmations;
    if (state->confirmations >= p->current.count - p->current.faults) {
        confirm_round(v);
    }
    return VSR_OK;
}

static bool emit_rejection(struct vsr *v, struct read_slot *slot)
{
    const struct vsr_protocol *p = vsr_protocol_const(v);
    const struct vsr_reply reply = {.view = v->status.view,
                                    .primary = v->status.primary,
                                    .membership = &p->current,
                                    .status = slot->rejection};
    if (!vsr_protocol_emit(v, VSR_OP_REPLY, slot->cookie, READ_REPLY, &reply,
                           NULL, 0, 0)) {
        return false;
    }
    slot->state = READ_FREE;
    return true;
}

static bool emit_fence(struct vsr *v, struct read_slot *slot, uint32_t index)
{
    struct vsr_protocol *p = vsr_protocol(v);
    if (p->application_busy || v->status.applied < slot->barrier.min_op ||
        v->status.applied < slot->floor) {
        return false;
    }
    const struct vsr_read_fence fence = {slot->cookie, v->status.applied,
                                         v->status.epoch, v->status.view};
    if (!vsr_protocol_emit(v, VSR_OP_READ_READY, 0, slot_tag(READ_READY, index),
                           &fence, NULL, 0, 0)) {
        return false;
    }
    p->application_busy = true;
    slot->state = READ_FENCED;
    return true;
}

static bool start_round(struct vsr *v)
{
    struct vsr_reads *state = reads(v);
    struct vsr_protocol *p = vsr_protocol(v);
    bool waiting = false;
    if (state->active || !primary(v) || !vsr_protocol_ready(v)) {
        return false;
    }
    for (uint32_t i = 0; i < v->options.limits.pending_reads; ++i) {
        if (state->slots[i].state == READ_WAITING &&
            state->slots[i].barrier.consistency == VSR_READ_LINEARIZABLE) {
            waiting = true;
            break;
        }
    }
    if (!waiting) {
        return false;
    }
    if (!state->established) {
        if (p->stable_commit > state->inherited_end) {
            state->established = true;
        } else if (state->noop_op == 0 && vsr_protocol_noop(v)) {
            state->noop_op = p->log_end - 1;
            return true;
        } else {
            return false;
        }
    }
    if (!vsr_protocol_nonce(v, &state->nonce)) {
        return false;
    }
    state->floor = v->status.committed;
    state->epoch = v->status.epoch;
    state->view = v->status.view;
    state->confirmations = 1;
    state->active = true;
    state->retry_at = vsr_after(v, v->options.retry_ns);
    memset(state->acknowledged, 0, v->options.limits.members);
    memset(state->sent, 0, v->options.limits.members);
    state->acknowledged[p->self] = 1;
    state->sent[p->self] = 1;
    for (uint32_t i = 0; i < v->options.limits.pending_reads; ++i) {
        struct read_slot *slot = &state->slots[i];
        if (slot->state == READ_WAITING &&
            slot->barrier.consistency == VSR_READ_LINEARIZABLE) {
            slot->state = READ_ROUND;
            slot->floor = state->floor;
        }
    }
    if (p->current.count - p->current.faults == 1) {
        confirm_round(v);
    }
    return true;
}

static bool poll_round(struct vsr *v)
{
    struct vsr_reads *state = reads(v);
    struct vsr_protocol *p = vsr_protocol(v);
    bool needed = false;
    if (!state->active) {
        return false;
    }
    for (uint32_t i = 0; i < v->options.limits.pending_reads; ++i) {
        needed = needed || state->slots[i].state == READ_ROUND;
    }
    if (!needed) {
        state->active = false;
        state->retry_at = VSR_NO_DEADLINE;
        return true;
    }
    if (v->time_set &&
        (state->retry_at == VSR_NO_DEADLINE || state->retry_at <= v->now)) {
        for (uint32_t i = 0; i < p->current.count; ++i) {
            state->sent[i] = state->acknowledged[i];
        }
        state->retry_at = vsr_after(v, v->options.retry_ns);
        return true;
    }
    for (uint32_t i = 0; i < p->current.count; ++i) {
        if (state->sent[i] == 0 &&
            send_nonce(v, p->current.members[i].id, VSR_MSG_READ_PROBE,
                       state->floor, &state->nonce, i)) {
            state->sent[i] = 1;
            return true;
        }
    }
    return false;
}

bool vsr_reads_poll(struct vsr *v)
{
    struct vsr_reads *state = reads(v);
    if (state->epoch != v->status.epoch || state->view != v->status.view ||
        v->status.state != VSR_STATE_NORMAL) {
        bool changed = reject_pending(v, state->epoch != v->status.epoch
                                             ? VSR_REPLY_NEW_EPOCH
                                             : rejection(v));
        state->epoch = v->status.epoch;
        state->view = v->status.view;
        state->established = false;
        state->noop_op = 0;
        if (changed) {
            return true;
        }
    }
    for (uint32_t n = 0; n < v->options.limits.pending_reads; ++n) {
        uint32_t i = state->cursor;
        state->cursor = i + 1 == v->options.limits.pending_reads ? 0 : i + 1;
        struct read_slot *slot = &state->slots[i];
        if (slot->state == READ_FREE || slot->state == READ_FENCED) {
            continue;
        }
        if (slot->state != READ_REJECTED && v->time_set &&
            slot->barrier.deadline_ns != VSR_NO_DEADLINE &&
            slot->barrier.deadline_ns <= v->now) {
            slot->state = READ_REJECTED;
            slot->rejection = VSR_REPLY_TIMEOUT;
            return true;
        }
        if (slot->state == READ_REJECTED && emit_rejection(v, slot)) {
            return true;
        }
        if ((slot->state == READ_CONFIRMED ||
             (slot->state == READ_WAITING &&
              slot->barrier.consistency == VSR_READ_CAUSAL)) &&
            full_normal(v) && emit_fence(v, slot, i)) {
            return true;
        }
    }
    return poll_round(v) || start_round(v);
}

void vsr_reads_complete(struct vsr *v, struct vsr_operation *operation,
                        const struct vsr_event *event, uint32_t lease)
{
    struct vsr_reads *state = reads(v);
    uint32_t kind = (uint32_t)operation->tag;
    uint32_t index = (uint32_t)(operation->tag >> 32);
    (void)lease;
    if (kind == READ_READY) {
        if (index >= v->options.limits.pending_reads ||
            state->slots[index].state != READ_FENCED) {
            vsr_fail(v, VSR_FAILURE_INVARIANT, operation, event->status);
            return;
        }
        state->slots[index].state = READ_FREE;
        vsr_protocol(v)->application_busy = false;
    }
    /* SEND failures use the round's existing retry deadline. Immediate retry
     * here could spin forever when an adapter rejects every submission. */
}

uint64_t vsr_reads_deadline(const struct vsr *v)
{
    const struct vsr_reads *state = reads_const(v);
    uint64_t next = state->active ? state->retry_at : VSR_NO_DEADLINE;
    for (uint32_t i = 0; i < v->options.limits.pending_reads; ++i) {
        const struct read_slot *slot = &state->slots[i];
        if ((slot->state == READ_WAITING || slot->state == READ_ROUND ||
             slot->state == READ_CONFIRMED) &&
            slot->barrier.deadline_ns < next) {
            next = slot->barrier.deadline_ns;
        }
    }
    return next;
}

void vsr_reads_stop(struct vsr *v)
{
    struct vsr_reads *state = reads(v);
    state->active = false;
    state->retry_at = VSR_NO_DEADLINE;
    for (uint32_t i = 0; i < v->options.limits.pending_reads; ++i) {
        state->slots[i].state = READ_FREE;
    }
}
