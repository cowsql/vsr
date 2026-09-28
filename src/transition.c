#include "config.h"

#include "transition.h"

#include "checked.h"
#include "checkpoint.h"
#include "extension.h"

#include <stdalign.h>
#include <string.h>

enum transition_tag {
    TAG_CONTROL = VSR_TAG_TRANSITION_FIRST,
    TAG_SERVE_LOAD,
    TAG_TARGET_LOAD,
    TAG_COMPARE_LOAD,
    TAG_ANCHOR_LOAD,
    TAG_OFFER_SEND
};

enum round_kind {
    ROUND_NONE,
    ROUND_VIEW,
    ROUND_RECOVERY,
    ROUND_CATCHUP,
    ROUND_WARM,
    ROUND_EPOCH
};
enum target_goal {
    TARGET_PRIMARY,
    TARGET_BACKUP,
    TARGET_RECOVERY,
    TARGET_CATCHUP,
    TARGET_WARM,
    TARGET_EPOCH
};
enum target_phase {
    TARGET_DRAIN,
    TARGET_SNAPSHOT,
    TARGET_FETCH,
    TARGET_COMPARE,
    TARGET_APPEND,
    TARGET_REPLAY,
    TARGET_NORMAL
};
enum server_phase { SERVER_FREE, SERVER_LOAD, SERVER_LOADING, SERVER_REPLY };
/* Whether a witness retains the entry a selected remote anchor closes with. */
enum anchor_state { ANCHOR_UNKNOWN, ANCHOR_RETAINED, ANCHOR_MISSING };

struct round_peer {
    struct vsr_nonce reply_nonce;
    bool svc;
    bool dvc;
    bool recovered;
    bool svc_sent;
    bool recovery_sent;
    bool start_sent;
    bool reply_pending;
};

struct retained_offer {
    struct vsr_log_state state;
    struct vsr_epoch epoch;
    struct vsr_membership current, previous;
    struct vsr_checkpoint checkpoint;
    struct vsr_epoch checkpoint_epoch;
    struct vsr_membership checkpoint_current, checkpoint_previous;
    struct vsr_member *members;
    struct vsr_span *spans;
    uint64_t expires;
    uint32_t lease;
    uint32_t references;
    bool used;
    bool snapshot_pin;
};

struct state_server {
    struct vsr_fetch request;
    const struct vsr_loaded *loaded;
    uint64_t peer;
    uint64_t load_id;
    uint64_t retry_at;
    uint32_t offer;
    uint32_t lease;
    uint32_t phase;
    uint32_t response;
};

struct selected_target {
    struct vsr_log_state state;
    struct vsr_fetch request;
    const struct vsr_entry *entries;
    uint64_t source;
    uint64_t next;
    uint64_t sequence;
    uint64_t load_id;
    uint64_t compare_id;
    uint64_t anchor_id;
    uint64_t retry_at;
    uint32_t lease;
    uint32_t offer;
    uint32_t entries_lease;
    uint32_t entry_count;
    uint32_t append_offset;
    uint32_t goal;
    uint32_t phase;
    uint32_t anchor;
    uint32_t unanswered; /* Consecutive chunk fetches without a reply. */
    bool active;
    bool waiting;
    bool snapshot_started;
    bool snapshot_adopted;
    bool rebuild;
};

/* Unanswered chunk fetches after which a transfer that may choose its donor
 * gives the source up: a removed donor retires once the new group is ready,
 * and a crashed one may never return, while every NORMAL full member of
 * either group can serve the same committed history. */
enum { TARGET_PATIENCE = 3 };

struct vsr_transition {
    struct round_peer *peers;
    struct retained_offer *offers;
    struct state_server *servers;
    struct selected_target target;
    struct vsr_log_state best;
    struct vsr_nonce recovery_nonce;
    struct vsr_nonce discovery_nonce;
    uint64_t best_source;
    uint64_t committed_floor;
    uint64_t highest_view;
    uint64_t retry_at;
    uint64_t election_at;
    uint64_t discovery_peer;
    uint64_t redirect_peer;
    uint64_t warm_at;
    uint32_t warm_cursor;
    uint32_t epoch_cursor;
    uint32_t best_lease;
    uint32_t best_offer;
    uint32_t local_offer;
    uint32_t round;
    uint32_t restore_state;
    struct vsr_hard_state old_hard;
    struct vsr_epoch old_epoch;
    struct vsr_membership old_current, old_previous;
    struct vsr_member *old_members;
    uint64_t epoch_fence_sequence;
    uint64_t epoch_boundary;
    bool best_present;
    bool dvc_sent;
    bool discovery_waiting;
    bool start_pending;
    bool ack_pending;
    bool boot_restore;
    bool boot_snapshot_started;
    bool enabled;
    bool uninitialized_hint;
    /* The local state does not qualify this replica to vote: its store was
     * missing or restarted under replicated durability, or a RECOVERING
     * record was restored. Only a completed quorum recovery clears it; a
     * later epoch learned meanwhile restarts that recovery in the learned
     * epoch instead of installing one donor's history. */
    bool quorum_recovery;
    bool redirect_pending;
    bool warmed;
};

struct transition_plan {
    size_t size, alignment, peers, offers, servers, members, spans, old_members;
};

static struct vsr_transition *transition(struct vsr *v)
{
    return ((struct vsr_extension *)vsr_protocol(v)->extension)->transition;
}

static const struct vsr_transition *transition_const(const struct vsr *v)
{
    return ((const struct vsr_extension *)vsr_protocol_const(v)->extension)
        ->transition;
}

static bool slice(struct transition_plan *plan, size_t count, size_t size,
                  size_t *offset)
{
    size_t padding =
        (plan->alignment - plan->size % plan->alignment) % plan->alignment;
    size_t bytes;
    return vsr_size_add(plan->size, padding, offset) &&
           vsr_size_mul(count, size, &bytes) &&
           vsr_size_add(*offset, bytes, &plan->size);
}

static bool transition_plan(const struct vsr_options *options,
                            struct transition_plan *plan)
{
    memset(plan, 0, sizeof(*plan));
    plan->size = sizeof(struct vsr_transition);
    plan->alignment =
        options->cache_line_bytes == 0 ? 64 : options->cache_line_bytes;
    if (plan->alignment < alignof(max_align_t))
        plan->alignment = alignof(max_align_t);
    size_t members, spans;
    return vsr_size_mul(options->limits.transfers, options->limits.members,
                        &members) &&
           vsr_size_mul(members, 4, &members) &&
           vsr_size_mul(options->limits.transfers,
                        options->limits.spans_per_blob, &spans) &&
           slice(plan, options->limits.members, sizeof(struct round_peer),
                 &plan->peers) &&
           slice(plan, options->limits.transfers, sizeof(struct retained_offer),
                 &plan->offers) &&
           slice(plan, options->limits.transfers, sizeof(struct state_server),
                 &plan->servers) &&
           slice(plan, members, sizeof(struct vsr_member), &plan->members) &&
           slice(plan, spans, sizeof(struct vsr_span), &plan->spans) &&
           slice(plan, (size_t)options->limits.members * 2,
                 sizeof(struct vsr_member), &plan->old_members);
}

int vsr_transition_size(const struct vsr_options *options, size_t *size,
                        size_t *alignment)
{
    struct transition_plan plan;
    if (!transition_plan(options, &plan))
        return VSR_ELIMIT;
    *size = plan.size;
    *alignment = plan.alignment;
    return VSR_OK;
}

void vsr_transition_init(struct vsr *v, void *memory, size_t size)
{
    (void)size;
    struct transition_plan plan;
    if (!transition_plan(&v->options, &plan))
        return;
    struct vsr_transition *t = memory;
    unsigned char *base = memory;
    t->old_members = (void *)(base + plan.old_members);
    t->peers = (void *)(base + plan.peers);
    t->offers = (void *)(base + plan.offers);
    t->servers = (void *)(base + plan.servers);
    struct vsr_member *members = (void *)(base + plan.members);
    struct vsr_span *spans = (void *)(base + plan.spans);
    for (uint32_t i = 0; i < v->options.limits.transfers; i++) {
        t->offers[i].members =
            members + (size_t)i * v->options.limits.members * 4;
        t->offers[i].spans =
            spans + (size_t)i * v->options.limits.spans_per_blob;
        t->offers[i].lease = VSR_INDEX_NONE;
        t->servers[i].lease = t->servers[i].offer = VSR_INDEX_NONE;
        t->servers[i].retry_at = VSR_NO_DEADLINE;
    }
    t->best_lease = t->best_offer = t->local_offer = VSR_INDEX_NONE;
    t->target.lease = t->target.offer = t->target.entries_lease =
        VSR_INDEX_NONE;
    t->retry_at = t->election_at = t->warm_at = VSR_NO_DEADLINE;
}

static bool expired(const struct vsr *v, uint64_t at)
{
    return v->time_set && at != VSR_NO_DEADLINE && v->now >= at;
}

/* A replica in neither group of its current configuration: a JOIN learner,
 * warming without a vote, or one restarted during warm-up. */
static bool learner(const struct vsr *v)
{
    const struct vsr_protocol *p = vsr_protocol_const(v);
    return p->self == VSR_INDEX_NONE &&
           (p->epoch.previous == NULL ||
            vsr_member_index(p->epoch.previous, v->options.replica) ==
                VSR_INDEX_NONE);
}

static bool hard_safe(const struct vsr *v)
{
    const struct vsr_protocol *p = vsr_protocol_const(v);
    return p->hard_sequence != 0 && !p->hard_dirty &&
           p->safe_sequence >= p->hard_sequence;
}

/* The committed position a full replica must have applied before a transfer
 * or restart completes. Application beyond the boundary belongs to the new
 * epoch's full members: a removed or demoted member stops there (the epoch
 * module refuses later entries), and a donor from the new group may offer a
 * committed position past it. */
static uint64_t applied_needed(const struct vsr *v)
{
    const struct vsr_protocol *p = vsr_protocol_const(v);
    uint32_t role = p->self == VSR_INDEX_NONE
                        ? VSR_MEMBER_NONE
                        : p->current.members[p->self].role;
    uint32_t previous = VSR_MEMBER_NONE;
    if (p->epoch.previous != NULL) {
        uint32_t index =
            vsr_member_index(p->epoch.previous, v->options.replica);
        if (index != VSR_INDEX_NONE)
            previous = p->epoch.previous->members[index].role;
    }
    if (role != VSR_MEMBER_FULL &&
        (role == VSR_MEMBER_WITNESS || previous == VSR_MEMBER_FULL) &&
        p->epoch.boundary < p->stable_commit)
        return p->epoch.boundary;
    return p->stable_commit;
}

static bool send_tagged(struct vsr *v, uint64_t peer, uint32_t type,
                        uint64_t number, const void *body,
                        const uint32_t *leases, uint32_t count, uint64_t tag)
{
    struct vsr_message message = {v->options.cluster,
                                  v->status.epoch,
                                  v->status.view,
                                  v->options.replica,
                                  type,
                                  0,
                                  number,
                                  body};
    return vsr_protocol_emit(v, VSR_OP_SEND, peer, tag, &message, leases, count,
                             0);
}

static bool send_control(struct vsr *v, uint64_t peer, uint32_t type,
                         uint64_t number, const void *body,
                         const uint32_t *leases, uint32_t count)
{
    return send_tagged(v, peer, type, number, body, leases, count, TAG_CONTROL);
}

static void copy_membership(struct vsr_membership *target,
                            struct vsr_member *members,
                            const struct vsr_membership *source)
{
    *target = *source;
    memcpy(members, source->members, (size_t)source->count * sizeof(*members));
    target->members = members;
}

static void copy_epoch(struct vsr *v, struct vsr_epoch *target,
                       struct vsr_membership *current,
                       struct vsr_membership *previous,
                       struct vsr_member *members,
                       const struct vsr_epoch *source)
{
    *target = *source;
    copy_membership(current, members, source->current);
    target->current = current;
    if (source->previous != NULL) {
        copy_membership(previous, members + v->options.limits.members,
                        source->previous);
        target->previous = previous;
    }
}

static void release_offer(struct vsr *v, uint32_t index)
{
    struct retained_offer *offer = &transition(v)->offers[index];
    if (!offer->used)
        return;
    if (offer->snapshot_pin)
        vsr_checkpoint_unpin(v, offer->checkpoint.id);
    vsr_lease_release(v, offer->lease);
    offer->lease = VSR_INDEX_NONE;
    offer->used = false;
    offer->snapshot_pin = false;
}

static void offer_reference(struct vsr *v, uint32_t index)
{
    if (index != VSR_INDEX_NONE)
        transition(v)->offers[index].references++;
}

static void offer_unreference(struct vsr *v, uint32_t index)
{
    if (index == VSR_INDEX_NONE)
        return;
    struct retained_offer *offer = &transition(v)->offers[index];
    if (offer->references == 0) {
        vsr_fail(v, VSR_FAILURE_INVARIANT, NULL, VSR_IO_OK);
        return;
    }
    offer->references--;
}

/* The output graph owns copied metadata, while the indexed revision and
 * snapshot image retain their separate pin until SEND completion. */
static bool send_offer(struct vsr *v, uint64_t peer, uint32_t type,
                       uint64_t number, const void *body,
                       const uint32_t *leases, uint32_t count, uint32_t index)
{
    uint64_t tag = index == VSR_INDEX_NONE
                       ? TAG_CONTROL
                       : TAG_OFFER_SEND | ((uint64_t)index << 32);
    if (!send_tagged(v, peer, type, number, body, leases, count, tag))
        return false;
    offer_reference(v, index);
    return true;
}

static uint32_t offer_current(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_transition *t = transition(v);
    if (p->epoch.boundary > p->stable_commit || !hard_safe(v) ||
        !vsr_checkpoint_revision_ready(v) || p->stable_end != p->written_end ||
        p->written_end != p->log_end)
        return VSR_INDEX_NONE;
    uint32_t free_index = VSR_INDEX_NONE;
    uint32_t victim = VSR_INDEX_NONE;
    for (uint32_t i = 0; i < v->options.limits.transfers; i++) {
        struct retained_offer *offer = &t->offers[i];
        if (offer->used && offer->state.revision.sequence == p->safe_sequence &&
            !expired(v, offer->expires)) {
            offer->expires = vsr_after(v, v->options.transfer_timeout_ns);
            return i;
        }
        if (!offer->used ||
            (offer->references == 0 && expired(v, offer->expires)))
            free_index = i;
        if (offer->used && offer->references == 0 &&
            (victim == VSR_INDEX_NONE ||
             offer->expires < t->offers[victim].expires))
            victim = i;
    }
    /* Idle historical advertisements cannot prevent a current fenced vote or
     * START_VIEW when transfers=1. Eviction makes future requests explicitly
     * revalidate via STATE_UNAVAILABLE; in-flight indexed reads stay pinned. */
    if (free_index == VSR_INDEX_NONE)
        free_index = victim;
    if (free_index == VSR_INDEX_NONE)
        return free_index;
    struct retained_offer *offer = &t->offers[free_index];
    release_offer(v, free_index);
    copy_epoch(v, &offer->epoch, &offer->current, &offer->previous,
               offer->members, &p->epoch);
    offer->state = (struct vsr_log_state){
        .revision = {v->options.incarnation, p->safe_sequence},
        .view = v->status.view,
        .last_normal_view = p->last_normal_view,
        .committed = p->stable_commit,
        .log_begin = p->log_begin,
        .log_end = p->stable_end,
        .epoch = &offer->epoch,
    };
    uint32_t lease = VSR_INDEX_NONE;
    const struct vsr_checkpoint *checkpoint =
        vsr_checkpoint_published(v, &lease);
    if (checkpoint != NULL) {
        offer->checkpoint = *checkpoint;
        copy_epoch(v, &offer->checkpoint_epoch, &offer->checkpoint_current,
                   &offer->checkpoint_previous,
                   offer->members + (size_t)v->options.limits.members * 2,
                   checkpoint->epoch);
        offer->checkpoint.epoch = &offer->checkpoint_epoch;
        if (checkpoint->manifest.count != 0) {
            memcpy(offer->spans, checkpoint->manifest.spans,
                   (size_t)checkpoint->manifest.count * sizeof(*offer->spans));
            offer->checkpoint.manifest.spans = offer->spans;
        }
        /* A failed retain has failed the engine; leave the slot unused. */
        if (!vsr_lease_retain(v, lease))
            return VSR_INDEX_NONE;
        offer->lease = lease;
        if (v->status.role == VSR_MEMBER_FULL) {
            if (!vsr_checkpoint_pin(v, checkpoint->id)) {
                vsr_lease_release(v, lease);
                offer->lease = VSR_INDEX_NONE;
                return VSR_INDEX_NONE;
            }
            offer->snapshot_pin = true;
        }
        offer->state.checkpoint = &offer->checkpoint;
    }
    if (p->log_begin > 1 && checkpoint == NULL) {
        vsr_fail(v, VSR_FAILURE_INVARIANT, NULL, VSR_IO_OK);
        return VSR_INDEX_NONE;
    }
    offer->references = 0;
    offer->expires = vsr_after(v, v->options.transfer_timeout_ns);
    offer->used = true;
    return free_index;
}

static uint32_t offer_find(struct vsr *v, struct vsr_revision revision)
{
    if (!vsr_id_equal(revision.incarnation, v->options.incarnation))
        return VSR_INDEX_NONE;
    struct vsr_transition *t = transition(v);
    for (uint32_t i = 0; i < v->options.limits.transfers; i++) {
        if (t->offers[i].used &&
            t->offers[i].state.revision.sequence == revision.sequence &&
            !expired(v, t->offers[i].expires))
            return i;
    }
    return VSR_INDEX_NONE;
}

static void clear_best(struct vsr *v)
{
    struct vsr_transition *t = transition(v);
    vsr_lease_release(v, t->best_lease);
    offer_unreference(v, t->best_offer);
    t->best_lease = t->best_offer = VSR_INDEX_NONE;
    t->best_present = false;
}

static void clear_target(struct vsr *v)
{
    struct selected_target *target = &transition(v)->target;
    vsr_lease_release(v, target->entries_lease);
    vsr_lease_release(v, target->lease);
    offer_unreference(v, target->offer);
    memset(target, 0, sizeof(*target));
    target->entries_lease = target->lease = target->offer = VSR_INDEX_NONE;
}

static void reset_round(struct vsr *v)
{
    struct vsr_transition *t = transition(v);
    if (t->target.active && t->target.snapshot_adopted)
        vsr_checkpoint_cancel_adoption(v);
    clear_best(v);
    clear_target(v);
    offer_unreference(v, t->local_offer);
    t->local_offer = VSR_INDEX_NONE;
    memset(t->peers, 0, (size_t)v->options.limits.members * sizeof(*t->peers));
    t->dvc_sent = false;
    t->discovery_waiting = false;
    t->start_pending = false;
    t->ack_pending = false;
    t->warm_at = VSR_NO_DEADLINE;
    if (t->committed_floor < vsr_protocol(v)->desired_commit)
        t->committed_floor = vsr_protocol(v)->desired_commit;
    if (t->committed_floor < v->status.committed)
        t->committed_floor = v->status.committed;
}

static void enter_view(struct vsr *v, uint64_t view)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_transition *t = transition(v);
    if (view == UINT64_MAX || view < v->status.view ||
        p->self == VSR_INDEX_NONE)
        return;
    reset_round(v);
    t->enabled = true;
    t->round = ROUND_VIEW;
    v->status.view = view;
    v->status.primary = vsr_primary(&p->current, view);
    v->status.state = VSR_STATE_VIEW_CHANGE;
    p->hard_dirty = true;
    t->retry_at = v->time_set ? v->now : VSR_NO_DEADLINE;
    t->election_at = vsr_after(v, v->options.view_timeout_ns);
    vsr_changed(v);
}

static bool next_view(struct vsr *v)
{
    if (v->status.view >= UINT64_MAX - 1) {
        vsr_fail(v, VSR_FAILURE_EXHAUSTED, NULL, VSR_IO_OK);
        return false;
    }
    enter_view(v, v->status.view + 1);
    return true;
}

static bool compatible_offer(const struct vsr *v,
                             const struct vsr_log_state *state)
{
    const struct vsr_protocol *p = vsr_protocol_const(v);
    return vsr_membership_equal(&p->current, state->epoch->current) &&
           state->epoch->boundary == p->epoch.boundary &&
           ((state->epoch->previous == NULL && p->epoch.previous == NULL) ||
            (state->epoch->previous != NULL && p->epoch.previous != NULL &&
             vsr_membership_equal(state->epoch->previous, p->epoch.previous)));
}

static bool same_checkpoint(const struct vsr_checkpoint *left,
                            const struct vsr_checkpoint *right)
{
    if (left == NULL || right == NULL)
        return left == right;
    return vsr_id_equal(left->id, right->id) && left->op == right->op &&
           left->view == right->view &&
           vsr_epoch_equal(left->epoch, right->epoch) &&
           vsr_blob_equal(&left->manifest, &right->manifest);
}

static bool same_offer(const struct vsr_log_state *left,
                       const struct vsr_log_state *right)
{
    return vsr_revision_equal(left->revision, right->revision) &&
           left->view == right->view &&
           left->last_normal_view == right->last_normal_view &&
           left->committed == right->committed &&
           left->log_begin == right->log_begin &&
           left->log_end == right->log_end &&
           vsr_epoch_equal(left->epoch, right->epoch) &&
           same_checkpoint(left->checkpoint, right->checkpoint);
}

static void consider_offer(struct vsr *v, uint64_t source,
                           const struct vsr_log_state *state, uint32_t lease,
                           uint32_t local_offer)
{
    struct vsr_transition *t = transition(v);
    if (state->committed > t->committed_floor)
        t->committed_floor = state->committed;
    bool better =
        !t->best_present ||
        state->last_normal_view > t->best.last_normal_view ||
        (state->last_normal_view == t->best.last_normal_view &&
         state->log_end > t->best.log_end) ||
        (state->last_normal_view == t->best.last_normal_view &&
         state->log_end == t->best.log_end && source < t->best_source);
    if (!better)
        return;
    /* A failed retain has failed the engine; the previous best stays. */
    if (!vsr_lease_retain(v, lease))
        return;
    offer_reference(v, local_offer);
    clear_best(v);
    t->best = *state;
    t->best.entries = (struct vsr_entries){0};
    t->best_source = source;
    t->best_lease = lease;
    t->best_offer = local_offer;
    t->best_present = true;
}

static void select_best(struct vsr *v, uint32_t goal, bool rebuild)
{
    struct vsr_transition *t = transition(v);
    if (!t->best_present || t->best.log_end <= t->committed_floor) {
        vsr_fail(v, VSR_FAILURE_INVARIANT, NULL, VSR_IO_OK);
        return;
    }
    clear_target(v);
    struct selected_target *target = &t->target;
    target->state = t->best;
    target->source = t->best_source;
    target->lease = t->best_lease;
    target->offer = t->best_offer;
    target->goal = goal;
    target->phase = TARGET_DRAIN;
    target->active = true;
    target->rebuild = rebuild;
    t->best_lease = t->best_offer = VSR_INDEX_NONE;
    t->best_present = false;
    target->retry_at = v->time_set ? v->now : VSR_NO_DEADLINE;
}

static void begin_recovery(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_transition *t = transition(v);
    reset_round(v);
    if (!vsr_protocol_nonce(v, &t->recovery_nonce))
        return;
    t->enabled = true;
    t->round = ROUND_RECOVERY;
    t->quorum_recovery = true;
    t->highest_view = v->status.view;
    v->status.state = VSR_STATE_RECOVERING;
    p->hard_dirty = true;
    t->retry_at = v->time_set ? v->now : VSR_NO_DEADLINE;
    t->election_at = VSR_NO_DEADLINE;
    vsr_changed(v);
}

/* A learner is not bound to one donor: any NORMAL full member can answer
 * its discovery. Rotate through the membership it knows so a crashed peer
 * cannot stall warm-up indefinitely. */
static uint64_t warm_peer(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_transition *t = transition(v);
    for (uint32_t i = 0; i < p->current.count; i++) {
        uint32_t index = (t->warm_cursor + i) % p->current.count;
        const struct vsr_member *member = &p->current.members[index];
        if (member->role == VSR_MEMBER_FULL &&
            member->id != v->options.replica) {
            t->warm_cursor = index + 1;
            return member->id;
        }
    }
    return v->status.primary;
}

/* An epoch installation accepts any NORMAL full member of either group as
 * its donor, so it is not bound to the one that announced the epoch: a
 * donor that retired, crashed, or is itself still transferring never
 * answers, and the next retry moves on to another member. */
static uint64_t epoch_peer(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_transition *t = transition(v);
    uint32_t previous = p->epoch.previous == NULL ? 0 : p->previous.count;
    uint32_t total = p->current.count + previous;
    for (uint32_t i = 0; i < total; i++) {
        uint32_t index = (t->epoch_cursor + i) % total;
        const struct vsr_member *member =
            index < p->current.count
                ? &p->current.members[index]
                : &p->previous.members[index - p->current.count];
        if (member->role == VSR_MEMBER_FULL &&
            member->id != v->options.replica) {
            t->epoch_cursor = index + 1;
            return member->id;
        }
    }
    return t->discovery_peer;
}

static void begin_discovery(struct vsr *v, uint64_t peer, bool warming)
{
    struct vsr_transition *t = transition(v);
    uint32_t state = warming ? VSR_STATE_WARMING : VSR_STATE_VIEW_CHANGE;
    reset_round(v);
    t->enabled = true;
    t->round = warming ? ROUND_WARM : ROUND_CATCHUP;
    t->discovery_peer = peer;
    t->retry_at = v->time_set ? v->now : VSR_NO_DEADLINE;
    t->election_at = vsr_after(v, v->options.view_timeout_ns);
    /* Missing messages do not invalidate an intact voting history. Persist a
     * view-change fence so a later durable restart retains that eligibility;
     * only state-loss recovery requires a quorum of other normal replicas.
     * A learner has no vote to fence: its warm-up record was persisted when
     * its store was created, and repeated discovery must not rewrite it. */
    bool changed = !warming || v->status.state != state;
    if (!warming)
        vsr_protocol(v)->hard_dirty = true;
    v->status.state = state;
    if (changed)
        vsr_changed(v);
}

static void restart_selection(struct vsr *v)
{
    struct vsr_transition *t = transition(v);
    uint32_t goal = t->target.goal;
    if (goal == TARGET_EPOCH) {
        uint64_t boundary = t->epoch_boundary;
        (void)vsr_transition_epoch(v, epoch_peer(v), boundary);
    } else if (goal == TARGET_PRIMARY) {
        (void)next_view(v);
    } else if (goal == TARGET_RECOVERY) {
        begin_recovery(v);
    } else {
        begin_discovery(v, v->status.primary, goal == TARGET_WARM);
    }
}

static int receive_fetch(struct vsr *v, const struct vsr_message *message)
{
    struct vsr_transition *t = transition(v);
    const struct vsr_fetch *request = message->body;
    uint32_t slot = VSR_INDEX_NONE;
    for (uint32_t i = 0; i < v->options.limits.transfers; i++) {
        if (t->servers[i].phase != SERVER_FREE &&
            t->servers[i].peer == message->from &&
            vsr_nonce_equal(t->servers[i].request.nonce, request->nonce))
            return VSR_OK;
        if (t->servers[i].phase == SERVER_FREE)
            slot = i;
    }
    if (slot == VSR_INDEX_NONE)
        return VSR_AGAIN;
    bool discovery = request->revision.sequence == 0;
    /* A discovery offer endorses the sender's history as a NORMAL one in the
     * offered view: a catch-up, warm-up, or epoch transfer installs it and
     * enters that view. A recovering replica's view and last normal view
     * are pre-crash metadata it has not validated with a quorum, so it does
     * not offer them, just as it answers no recovery request. A range fetch
     * names a revision this incarnation already published; that revision's
     * metadata and contents are fixed, so serving it endorses nothing new.
     * Liveness is unaffected: discovery targets a NORMAL primary (catch-up),
     * the NEW_EPOCH sender or a full member of the old group (handoff), or
     * the seed primary (warm-up), and the requester retries on its timer. */
    if (discovery && v->status.state == VSR_STATE_RECOVERING)
        return VSR_OK;
    uint32_t offer =
        discovery ? offer_current(v) : offer_find(v, request->revision);
    bool unavailable = offer == VSR_INDEX_NONE;
    if (!unavailable && !discovery) {
        const struct vsr_log_state *state = &t->offers[offer].state;
        unavailable =
            request->first < state->log_begin || request->end > state->log_end;
    }
    if (unavailable)
        offer = offer_current(v);
    if (offer == VSR_INDEX_NONE)
        return VSR_AGAIN;
    t->enabled = true;
    struct state_server *server = &t->servers[slot];
    server->request = *request;
    server->peer = message->from;
    server->offer = offer;
    server->lease = VSR_INDEX_NONE;
    server->loaded = NULL;
    server->response = unavailable ? VSR_MSG_STATE_UNAVAILABLE
                       : message->type == VSR_MSG_GET_LOG ? VSR_MSG_LOG
                                                          : VSR_MSG_NEW_STATE;
    server->phase = unavailable || discovery || request->first == request->end
                        ? SERVER_REPLY
                        : SERVER_LOAD;
    server->retry_at = VSR_NO_DEADLINE;
    offer_reference(v, offer);
    t->offers[offer].expires = vsr_after(v, v->options.transfer_timeout_ns);
    return VSR_OK;
}

static bool server_poll(struct vsr *v)
{
    struct vsr_transition *t = transition(v);
    for (uint32_t i = 0; i < v->options.limits.transfers; i++) {
        struct state_server *server = &t->servers[i];
        if (server->phase == SERVER_FREE || server->phase == SERVER_LOADING)
            continue;
        struct retained_offer *offer = &t->offers[server->offer];
        if (server->phase == SERVER_LOAD) {
            if (server->retry_at != VSR_NO_DEADLINE &&
                !expired(v, server->retry_at))
                continue;
            uint64_t manifest = offer->state.checkpoint == NULL
                                    ? 0
                                    : offer->checkpoint.manifest.size;
            uint64_t bytes = server->request.max_bytes - manifest;
            uint64_t available = vsr_protocol_available_bytes(v);
            if (available < bytes)
                bytes = available;
            if (bytes < v->options.limits.command_bytes)
                continue;
            struct vsr_store_read read = {offer->state.revision.sequence,
                                          server->request.first,
                                          server->request.end,
                                          {0, 0},
                                          bytes,
                                          VSR_LOAD_LOG,
                                          server->request.max_entries};
            uint64_t tag = TAG_SERVE_LOAD | ((uint64_t)i << 32);
            if (!vsr_protocol_emit(v, VSR_OP_LOAD, 0, tag, &read, NULL, 0,
                                   bytes))
                continue;
            server->load_id = vsr_operation_last_id(v);
            server->phase = SERVER_LOADING;
            return true;
        }
        struct vsr_state_chunk chunk = {server->request.nonce, offer->state,
                                        server->request.first,
                                        server->request.first};
        chunk.state.entries = (struct vsr_entries){0};
        if (server->loaded != NULL) {
            chunk.state.entries = (struct vsr_entries){
                server->loaded->items, server->loaded->count, 0};
            chunk.next = server->loaded->next;
        } else if (server->response != VSR_MSG_STATE_UNAVAILABLE) {
            chunk.next = server->request.end;
        }
        uint32_t leases[2] = {offer->lease, server->lease};
        if (!send_offer(v, server->peer, server->response,
                        offer->state.log_end - 1, &chunk, leases, 2,
                        server->offer))
            continue;
        vsr_lease_release(v, server->lease);
        offer_unreference(v, server->offer);
        server->lease = server->offer = VSR_INDEX_NONE;
        server->phase = SERVER_FREE;
        server->loaded = NULL;
        return true;
    }
    return false;
}

/* How many entries a range fetch may ask for, and the byte limit that goes
 * with them: as many as batch_entries and the payload budget allow. A chunk
 * is an input this replica must admit with its standing progress reserve
 * intact, so the request is sized from the budget free now, never below the
 * one entry every fetch may reserve and never above message_bytes. A chunk
 * that no longer fits when it arrives is refused, and the retry timer
 * fetches it again, sized to the budget free then. */
static uint64_t fetch_budget(const struct vsr *v, uint32_t *entries)
{
    const struct vsr_limits *limits = &v->options.limits;
    uint64_t budget = vsr_protocol_available_bytes(v);
    budget = budget > v->progress_bytes ? budget - v->progress_bytes : 0;
    uint64_t least = limits->command_bytes + limits->manifest_bytes;
    if (budget < least)
        budget = least;
    if (budget > limits->message_bytes)
        budget = limits->message_bytes;
    uint64_t count = (budget - limits->manifest_bytes) / limits->command_bytes;
    if (count > limits->batch_entries)
        count = limits->batch_entries;
    *entries = (uint32_t)count;
    return limits->manifest_bytes + count * limits->command_bytes;
}

/* Load up to count entries of [first, end) from the named revision, fewer
 * when the payload budget or message_bytes holds less; at least one. */
static bool target_load(struct vsr *v, uint64_t sequence, uint64_t first,
                        uint64_t end, uint32_t count, uint32_t tag,
                        uint64_t *id)
{
    const struct vsr_limits *limits = &v->options.limits;
    uint64_t bytes = vsr_protocol_available_bytes(v);
    if (bytes < limits->command_bytes)
        return false;
    if (bytes > limits->message_bytes)
        bytes = limits->message_bytes;
    if (end - first < count)
        count = (uint32_t)(end - first);
    if (bytes / limits->command_bytes < count)
        count = (uint32_t)(bytes / limits->command_bytes);
    bytes = (uint64_t)count * limits->command_bytes;
    struct vsr_store_read read = {sequence, first,        end,  {0, 0},
                                  bytes,    VSR_LOAD_LOG, count};
    if (!vsr_protocol_emit(v, VSR_OP_LOAD, 0, tag, &read, NULL, 0, bytes))
        return false;
    *id = vsr_operation_last_id(v);
    return true;
}

/* A proposal is identified by its epoch, view, and op: one primary assigns
 * each op once per view, and a boundary entry belongs to the epoch it ends.
 * A witness holding that exact entry accepted it after installing the
 * proposing view's history, so its whole retained prefix is the committed one
 * and needs no anchor. Anything else at that op was never part of the quorum
 * that committed it, and such an unverifiable prefix must be replaced. */
static bool anchor_retained(const struct vsr *v, const struct vsr_entry *entry,
                            const struct vsr_checkpoint *checkpoint)
{
    const struct vsr_protocol *p = vsr_protocol_const(v);
    uint64_t epoch = p->current.epoch;
    if (checkpoint->op != 0 && checkpoint->op == p->epoch.boundary &&
        p->epoch.previous != NULL)
        epoch = p->epoch.previous->epoch;
    return entry->op == checkpoint->op && entry->view == checkpoint->view &&
           entry->epoch == epoch;
}

static void reject_discarded_routes(struct vsr *v, uint64_t first)
{
    struct vsr_protocol *p = vsr_protocol(v);
    vsr_protocol_log_clear(v, first);
    vsr_protocol_history_changed(v);
    for (uint32_t i = 0; i < v->options.limits.pending_requests; i++) {
        struct vsr_route *route = &p->routes[i];
        if (route->state == VSR_ROUTE_WAIT && route->op >= first) {
            route->state = VSR_ROUTE_REPLY;
            route->reply = VSR_REPLY_NOT_PRIMARY;
            route->executed = false;
        }
    }
}

static void take_chunk(struct vsr *v, const struct vsr_entry *entries,
                       uint32_t count, uint32_t lease)
{
    struct selected_target *target = &transition(v)->target;
    /* A failed retain has failed the engine; the chunk is not adopted. */
    if (!vsr_lease_retain(v, lease))
        return;
    vsr_lease_release(v, target->entries_lease);
    target->entries = entries;
    target->entry_count = count;
    target->entries_lease = lease;
    target->append_offset = 0;
    target->waiting = false;
    target->unanswered = 0;
    target->load_id = 0;
    target->phase = TARGET_COMPARE;
}

static bool compare_chunk(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct selected_target *target = &transition(v)->target;
    uint64_t overlap_end = target->next + target->entry_count;
    if (overlap_end > p->written_end)
        overlap_end = p->written_end;
    if (target->next >= overlap_end) {
        target->phase = TARGET_APPEND;
        return true;
    }
    /* append_offset counts the entries proven equal so far, from the cache
     * or from earlier comparison loads; the divergence, if any, is the first
     * entry past it. The overlap is bounded by the chunk, which is bounded
     * by batch_entries, so the count below fits. */
    uint32_t overlap = (uint32_t)(overlap_end - target->next);
    while (target->append_offset < overlap) {
        const struct vsr_entry *entry = &target->entries[target->append_offset];
        struct vsr_log_slot *local = vsr_protocol_log_find(v, entry->op);
        if (local == NULL)
            break;
        if (!vsr_entry_equal(&local->entry, entry)) {
            if (entry->op <= p->stable_commit) {
                vsr_fail(v, VSR_FAILURE_INVARIANT, NULL, VSR_IO_OK);
                return true;
            }
            target->phase = TARGET_APPEND;
            return true;
        }
        target->append_offset++;
    }
    if (target->append_offset == overlap) {
        target->phase = TARGET_APPEND;
        return true;
    }
    if (target->compare_id != 0)
        return false;
    return target_load(v, v->status.stored_sequence,
                       target->next + target->append_offset, overlap_end,
                       overlap - target->append_offset, TAG_COMPARE_LOAD,
                       &target->compare_id);
}

static bool append_chunk(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct selected_target *target = &transition(v)->target;
    if (target->sequence != 0 && p->safe_sequence < target->sequence)
        return false;
    target->sequence = 0;
    uint32_t count = target->entry_count - target->append_offset;
    uint64_t next = target->next + target->entry_count;
    if (count != 0) {
        const struct vsr_entry *entries =
            target->entries + target->append_offset;
        if (entries[0].op > p->written_end ||
            entries[0].op <= p->stable_commit) {
            vsr_fail(v, VSR_FAILURE_INVARIANT, NULL, VSR_IO_OK);
            return true;
        }
        bool replace = entries[0].op < p->written_end;
        struct vsr_hard_state hard;
        vsr_protocol_hard(v, &hard);
        uint64_t committed = target->state.committed;
        if (committed >= next)
            committed = next - 1;
        if (committed < p->stable_commit)
            committed = p->stable_commit;
        hard.committed = committed;
        vsr_transition_hard(v, &hard);
        struct vsr_change changes[3] = {
            {VSR_STORE_TRUNCATE, 0, entries[0].op, NULL},
            {VSR_STORE_APPEND, count, entries[0].op, entries},
            {VSR_STORE_HARD_STATE, 1, 0, &hard},
        };
        uint64_t sequence;
        if (!vsr_protocol_store(v, changes + (replace ? 0 : 1), replace ? 3 : 2,
                                &target->entries_lease, 1, next, committed, 0,
                                &sequence))
            return false;
        if (replace)
            reject_discarded_routes(v, entries[0].op);
        p->written_end = p->log_end = next;
        p->desired_commit = committed;
        p->hard_sequence = sequence;
        p->hard_dirty = false;
        target->sequence = sequence;
        /* Cache insertion is optional: the indexed store is authoritative and
         * the source lease is already pinned by STORE until it is readable. */
        for (uint32_t i = 0; i < count; i++)
            (void)vsr_protocol_log_put(v, &entries[i], target->entries_lease,
                                       sequence);
    }
    for (uint32_t i = 0; i < target->entry_count; i++)
        if (target->entries[i].type == VSR_REQUEST_RECONFIGURE &&
            target->entries[i].epoch == v->status.epoch)
            p->proposed_boundary = target->entries[i].op;
    target->next = next;
    vsr_lease_release(v, target->entries_lease);
    target->entries_lease = VSR_INDEX_NONE;
    target->entries = NULL;
    target->entry_count = target->append_offset = 0;
    target->phase = TARGET_FETCH;
    return true;
}

static bool drain_phase(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct selected_target *target = &transition(v)->target;
    if (p->safe_sequence + 1 != p->next_sequence || p->application_busy ||
        p->results_pending || p->log_loading)
        return false;
    if (p->log_end > p->written_end) {
        /* Unissued old-view proposals were never endorsed and may go. */
        reject_discarded_routes(v, p->written_end);
        p->log_end = p->written_end;
    }
    /* No truncation happens here: the previous suffix is kept until the
     * comparison proves a divergence. A transfer can be interrupted by
     * another view, and truncating a matching quorum-endorsed prefix now
     * would then advertise an incomplete log with its previous last normal
     * view. The divergent uncommitted tail goes in two places instead:
     * append_chunk replaces the suffix from the first differing uncommitted
     * entry in one TRUNCATE+APPEND transaction, and fetch_phase truncates
     * whatever extends past the selected log end once the fetch cursor
     * reaches it. A RESTORE issued by snapshot_phase precedes both, so the
     * suffix it retains is validated by that same comparison; the retained
     * entries below the fetch cursor are committed or covered by an anchor
     * this replica holds and are never divergent. */
    target->phase = TARGET_SNAPSHOT;
    return true;
}

static bool snapshot_phase(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct selected_target *target = &transition(v)->target;
    if (!target->snapshot_started) {
        uint32_t existing_lease;
        const struct vsr_checkpoint *existing =
            vsr_checkpoint_published(v, &existing_lease);
        const struct vsr_checkpoint *checkpoint = target->state.checkpoint;
        uint32_t role = v->status.role;
        /* A handoff or a recovery in a handoff rebuilds the role the
         * learned membership assigns: a promoted witness reconstructs
         * application state before it can vote as a full member. */
        if ((target->goal == TARGET_EPOCH || target->goal == TARGET_RECOVERY) &&
            p->self != VSR_INDEX_NONE &&
            p->current.members[p->self].role == VSR_MEMBER_FULL)
            role = VSR_MEMBER_FULL;
        bool adopt = target->rebuild ||
                     (checkpoint != NULL &&
                      (existing == NULL || checkpoint->op > existing->op));
        /* A full member whose application already reached the selected
         * checkpoint keeps its state: INSTALL establishes the application
         * boundary before suffix replay, never behind entries a running
         * application has applied. The fetch cursor still starts at the
         * donor's retained log, which covers everything past its anchor. */
        if (adopt && !target->rebuild && role == VSR_MEMBER_FULL &&
            checkpoint->op <= v->status.applied)
            adopt = false;
        /* A witness keeps a prefix it can vouch for instead of copying
         * the donor's newer anchor: a known-committed prefix, or one that
         * ends in the very entry the anchor closes with. The donor cannot
         * resend entries behind its anchor, so that check reads the local
         * copy here rather than in the ordinary chunk comparison. */
        if (adopt && role == VSR_MEMBER_WITNESS && !target->rebuild &&
            checkpoint->op > p->stable_commit &&
            checkpoint->op >= p->readable_begin &&
            checkpoint->op < p->written_end &&
            target->anchor == ANCHOR_UNKNOWN) {
            const struct vsr_log_slot *slot =
                vsr_protocol_log_find(v, checkpoint->op);
            if (slot == NULL) {
                if (target->anchor_id != 0)
                    return false;
                return target_load(v, v->status.stored_sequence, checkpoint->op,
                                   checkpoint->op + 1, 1, TAG_ANCHOR_LOAD,
                                   &target->anchor_id);
            }
            target->anchor = anchor_retained(v, &slot->entry, checkpoint)
                                 ? ANCHOR_RETAINED
                                 : ANCHOR_MISSING;
            return true;
        }
        if (role == VSR_MEMBER_WITNESS &&
            (checkpoint == NULL || checkpoint->op <= p->stable_commit ||
             target->anchor == ANCHOR_RETAINED))
            adopt = false;
        if (adopt) {
            /* A newer local anchor may cover an older selected checkpoint,
             * but never beyond the selected committed floor. */
            uint32_t lease = target->lease;
            uint64_t peer = target->source;
            if (existing != NULL && v->status.role == VSR_MEMBER_FULL &&
                (checkpoint == NULL || existing->op > checkpoint->op) &&
                existing->op <= target->state.committed) {
                checkpoint = existing;
                lease = existing_lease;
                peer = v->options.replica;
            }
            int result = vsr_checkpoint_adopt(v, checkpoint, lease, peer, role);
            if (result == VSR_AGAIN)
                return false;
            if (result != VSR_OK) {
                restart_selection(v);
                return true;
            }
            target->snapshot_adopted = true;
        }
        target->snapshot_started = true;
        if (adopt)
            return true;
    }
    if (vsr_checkpoint_busy(v))
        return false;
    if (target->snapshot_adopted &&
        vsr_checkpoint_adoption_status(v) != VSR_OK) {
        restart_selection(v);
        return true;
    }
    target->next = p->log_begin > target->state.log_begin
                       ? p->log_begin
                       : target->state.log_begin;
    uint32_t lease;
    const struct vsr_checkpoint *checkpoint =
        vsr_checkpoint_published(v, &lease);
    if (checkpoint != NULL && target->next <= checkpoint->op)
        target->next = checkpoint->op + 1;
    /* A warmed learner already holds the committed prefix, which every
     * offer shares; only its suffix can differ from the donor's. */
    if (target->goal == TARGET_WARM && !target->rebuild &&
        target->next <= p->stable_commit)
        target->next = p->stable_commit + 1;
    if (target->next > target->state.log_end) {
        vsr_fail(v, VSR_FAILURE_INVARIANT, NULL, VSR_IO_OK);
        return true;
    }
    target->phase = TARGET_FETCH;
    return true;
}

static bool fetch_phase(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_transition *t = transition(v);
    struct selected_target *target = &t->target;
    if (target->sequence != 0 && p->safe_sequence < target->sequence)
        return false;
    target->sequence = 0;
    if (target->next == target->state.log_end) {
        if (p->written_end > target->state.log_end) {
            struct vsr_hard_state hard;
            vsr_protocol_hard(v, &hard);
            hard.committed = target->state.committed;
            if (hard.committed < t->committed_floor)
                hard.committed = t->committed_floor;
            vsr_transition_hard(v, &hard);
            struct vsr_change changes[2] = {
                {VSR_STORE_TRUNCATE, 0, target->state.log_end, NULL},
                {VSR_STORE_HARD_STATE, 1, 0, &hard},
            };
            if (!vsr_protocol_store(v, changes, 2, NULL, 0,
                                    target->state.log_end, hard.committed, 0,
                                    &target->sequence))
                return false;
            reject_discarded_routes(v, target->state.log_end);
            p->written_end = p->log_end = target->state.log_end;
            p->desired_commit = hard.committed;
            p->hard_sequence = target->sequence;
            p->hard_dirty = false;
            return true;
        }
        if (p->written_end != target->state.log_end) {
            vsr_fail(v, VSR_FAILURE_INVARIANT, NULL, VSR_IO_OK);
            return true;
        }
        uint64_t committed = target->state.committed;
        if (committed < t->committed_floor)
            committed = t->committed_floor;
        p->desired_commit = committed;
        if (p->stable_commit < committed)
            p->hard_dirty = true;
        p->replay = true;
        target->phase = TARGET_REPLAY;
        return true;
    }
    if (target->load_id != 0)
        return false;
    if (target->waiting && !expired(v, target->retry_at))
        return false;
    if (target->waiting && target->source != v->options.replica &&
        (target->goal == TARGET_EPOCH || target->goal == TARGET_WARM)) {
        if (target->unanswered < TARGET_PATIENCE)
            target->unanswered++;
        if (target->unanswered >= TARGET_PATIENCE) {
            restart_selection(v);
            return true;
        }
    }
    uint32_t entries;
    uint64_t bytes = fetch_budget(v, &entries);
    if (target->source == v->options.replica) {
        return target_load(v, target->state.revision.sequence, target->next,
                           target->state.log_end, entries, TAG_TARGET_LOAD,
                           &target->load_id);
    }
    struct vsr_fetch request = {
        .revision = target->state.revision,
        .first = target->next,
        .end = target->state.log_end,
        .max_bytes = bytes,
        .max_entries = entries,
    };
    if (!vsr_protocol_nonce(v, &request.nonce))
        return true;
    uint32_t type =
        target->goal == TARGET_PRIMARY || target->goal == TARGET_BACKUP
            ? VSR_MSG_GET_LOG
            : VSR_MSG_GET_STATE;
    if (!send_control(v, target->source, type, 0, &request, NULL, 0))
        return false;
    target->request = request;
    target->waiting = true;
    target->retry_at = vsr_after(v, v->options.retry_ns);
    return true;
}

static bool replay_phase(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_transition *t = transition(v);
    struct selected_target *target = &t->target;
    if (!hard_safe(v) || p->stable_end != target->state.log_end ||
        p->stable_commit < t->committed_floor || p->results_pending ||
        p->application_busy || vsr_checkpoint_busy(v))
        return false;
    if (v->status.role == VSR_MEMBER_FULL &&
        (v->status.applied < applied_needed(v) ||
         p->clients_stored < applied_needed(v)))
        return false;
    bool handoff = target->goal == TARGET_EPOCH ||
                   (target->goal == TARGET_RECOVERY &&
                    p->epoch.phase == VSR_EPOCH_TRANSFERRING);
    if (target->goal == TARGET_WARM || handoff) {
        /* A quorum recovery run inside a handoff installed the highest
         * view primary's complete history: the replica is now qualified
         * to vote and hands the epoch module the installation, which
         * persists NORMAL together with phase INSTALLED. */
        if (target->goal == TARGET_RECOVERY)
            t->quorum_recovery = false;
        p->replay = false;
        clear_target(v);
        t->round = ROUND_NONE;
        t->warmed = true;
        v->status.state = handoff ? VSR_STATE_TRANSITIONING : VSR_STATE_WARMING;
        return true;
    }
    struct vsr_hard_state hard;
    vsr_protocol_hard(v, &hard);
    hard.state = VSR_HARD_NORMAL;
    hard.last_normal_view = v->status.view;
    hard.committed = p->stable_commit;
    struct vsr_change change = {VSR_STORE_HARD_STATE, 1, 0, &hard};
    if (!vsr_protocol_store(v, &change, 1, NULL, 0, p->written_end,
                            p->stable_commit, 0, &target->sequence))
        return false;
    p->last_normal_view = v->status.view;
    p->hard_sequence = target->sequence;
    p->hard_dirty = false;
    target->phase = TARGET_NORMAL;
    return true;
}

static bool normal_phase(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_transition *t = transition(v);
    struct selected_target *target = &t->target;
    if (p->safe_sequence < target->sequence)
        return false;
    bool primary = target->goal == TARGET_PRIMARY;
    if (target->goal == TARGET_RECOVERY)
        t->quorum_recovery = false;
    p->replay = false;
    clear_target(v);
    clear_best(v);
    offer_unreference(v, t->local_offer);
    t->local_offer = VSR_INDEX_NONE;
    t->round = ROUND_NONE;
    t->start_pending = primary;
    t->ack_pending = !primary;
    for (uint32_t i = 0; i < p->current.count; i++)
        t->peers[i].start_sent = false;
    vsr_protocol_normal(v);
    return true;
}

static bool target_poll(struct vsr *v)
{
    struct selected_target *target = &transition(v)->target;
    if (!target->active)
        return false;
    switch (target->phase) {
    case TARGET_DRAIN:
        return drain_phase(v);
    case TARGET_SNAPSHOT:
        return snapshot_phase(v);
    case TARGET_COMPARE:
        return compare_chunk(v);
    case TARGET_APPEND:
        return append_chunk(v);
    case TARGET_FETCH:
        return fetch_phase(v);
    case TARGET_REPLAY:
        return replay_phase(v);
    case TARGET_NORMAL:
        return normal_phase(v);
    default:
        return false;
    }
}

static bool receive_chunk(struct vsr *v, const struct vsr_message *message,
                          const struct vsr_state_chunk *chunk, uint32_t lease)
{
    struct selected_target *target = &transition(v)->target;
    if (!target->active || target->phase != TARGET_FETCH || !target->waiting ||
        message->from != target->source ||
        !vsr_nonce_equal(chunk->nonce, target->request.nonce) ||
        chunk->first != target->request.first)
        return false;
    if (message->type == VSR_MSG_STATE_UNAVAILABLE) {
        if (chunk->next != chunk->first || chunk->state.entries.count != 0)
            return false;
        restart_selection(v);
        return true;
    }
    if (!same_offer(&target->state, &chunk->state) ||
        chunk->next <= chunk->first || chunk->next > target->request.end ||
        chunk->next - chunk->first != chunk->state.entries.count ||
        chunk->state.entries.count > target->request.max_entries)
        return false;
    uint64_t remaining = target->request.max_bytes;
    uint64_t manifest = chunk->state.checkpoint == NULL
                            ? 0
                            : chunk->state.checkpoint->manifest.size;
    if (manifest > remaining)
        return false;
    remaining -= manifest;
    for (uint32_t i = 0; i < chunk->state.entries.count; i++) {
        const struct vsr_entry *entry = &chunk->state.entries.entries[i];
        if (entry->op != chunk->first + i)
            return false;
        if (entry->type == VSR_REQUEST_COMMAND) {
            uint64_t bytes = ((const struct vsr_blob *)entry->body)->size;
            if (bytes > remaining)
                return false;
            remaining -= bytes;
        }
    }
    take_chunk(v, chunk->state.entries.entries, chunk->state.entries.count,
               lease);
    return true;
}

static bool recover_response_poll(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_transition *t = transition(v);
    if (!vsr_protocol_ready(v))
        return false;
    for (uint32_t i = 0; i < p->current.count; i++) {
        struct round_peer *peer = &t->peers[i];
        if (!peer->reply_pending || i == p->self)
            continue;
        struct vsr_recovery recovery = {peer->reply_nonce, NULL};
        uint32_t lease = VSR_INDEX_NONE;
        uint32_t index = VSR_INDEX_NONE;
        uint64_t number = 0;
        if (v->status.primary == v->options.replica) {
            index = offer_current(v);
            if (index == VSR_INDEX_NONE)
                return false;
            recovery.state = &t->offers[index].state;
            lease = t->offers[index].lease;
            number = recovery.state->log_end - 1;
        }
        if (!send_offer(v, p->current.members[i].id, VSR_MSG_RECOVERY_RESPONSE,
                        number, &recovery, &lease, 1, index))
            return false;
        peer->reply_pending = false;
        return true;
    }
    return false;
}

static bool view_poll(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_transition *t = transition(v);
    if (t->target.active || t->round != ROUND_VIEW || !hard_safe(v))
        return false;
    if (expired(v, t->election_at))
        return next_view(v);
    if (t->retry_at == VSR_NO_DEADLINE && v->time_set)
        t->retry_at = v->now;
    if (expired(v, t->retry_at)) {
        for (uint32_t i = 0; i < p->current.count; i++)
            t->peers[i].svc_sent = false;
        t->dvc_sent = false;
        t->retry_at = vsr_after(v, v->options.retry_ns);
    }
    if (p->self == VSR_INDEX_NONE)
        return false;
    t->peers[p->self].svc = true;
    for (uint32_t i = 0; i < p->current.count; i++) {
        if (i == p->self || t->peers[i].svc_sent)
            continue;
        if (!send_control(v, p->current.members[i].id,
                          VSR_MSG_START_VIEW_CHANGE, 0, NULL, NULL, 0))
            return false;
        t->peers[i].svc_sent = true;
        return true;
    }
    uint32_t svc = 0, dvc = 0;
    for (uint32_t i = 0; i < p->current.count; i++) {
        if (t->peers[i].svc)
            svc++;
        if (t->peers[i].dvc)
            dvc++;
    }
    uint32_t quorum = p->current.count - p->current.faults;
    if (svc < quorum)
        return false;
    if (t->local_offer == VSR_INDEX_NONE) {
        uint32_t index = offer_current(v);
        if (index == VSR_INDEX_NONE)
            return false;
        offer_reference(v, index);
        t->local_offer = index;
        return true;
    }
    struct retained_offer *offer = &t->offers[t->local_offer];
    if (v->status.primary == v->options.replica) {
        if (!t->peers[p->self].dvc) {
            t->peers[p->self].dvc = true;
            consider_offer(v, v->options.replica, &offer->state, VSR_INDEX_NONE,
                           t->local_offer);
            dvc++;
        }
        if (dvc >= quorum) {
            select_best(v, TARGET_PRIMARY, false);
            return true;
        }
    } else if (!t->dvc_sent) {
        if (!send_offer(v, v->status.primary, VSR_MSG_DO_VIEW_CHANGE,
                        offer->state.log_end - 1, &offer->state, &offer->lease,
                        1, t->local_offer))
            return false;
        t->dvc_sent = true;
        return true;
    }
    return false;
}

static bool recovery_poll(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_transition *t = transition(v);
    if (t->target.active || t->round != ROUND_RECOVERY || !hard_safe(v))
        return false;
    uint32_t count = 0;
    for (uint32_t i = 0; i < p->current.count; i++)
        if (i != p->self && t->peers[i].recovered)
            count++;
    uint64_t primary = vsr_primary(&p->current, t->highest_view);
    uint32_t index = vsr_member_index(&p->current, primary);
    if (count >= p->current.count - p->current.faults &&
        index != VSR_INDEX_NONE && index != p->self &&
        t->peers[index].recovered && t->best_present &&
        t->best_source == primary && t->best.view == t->highest_view) {
        select_best(v, TARGET_RECOVERY, true);
        return true;
    }
    if (t->retry_at == VSR_NO_DEADLINE && v->time_set)
        t->retry_at = v->now;
    if (expired(v, t->retry_at)) {
        for (uint32_t i = 0; i < p->current.count; i++)
            t->peers[i].recovery_sent = false;
        t->retry_at = vsr_after(v, v->options.retry_ns);
    }
    for (uint32_t i = 0; i < p->current.count; i++) {
        if (i == p->self || t->peers[i].recovery_sent)
            continue;
        struct vsr_recovery request = {t->recovery_nonce, NULL};
        if (!send_control(v, p->current.members[i].id, VSR_MSG_RECOVERY, 0,
                          &request, NULL, 0))
            return false;
        t->peers[i].recovery_sent = true;
        return true;
    }
    return false;
}

static bool discovery_poll(struct vsr *v)
{
    struct vsr_transition *t = transition(v);
    if (t->target.active ||
        (t->round != ROUND_CATCHUP && t->round != ROUND_WARM &&
         t->round != ROUND_EPOCH) ||
        (!hard_safe(v) && !t->uninitialized_hint))
        return false;
    if (t->discovery_waiting && !expired(v, t->retry_at))
        return false;
    if (t->discovery_waiting && t->round == ROUND_WARM)
        t->discovery_peer = warm_peer(v);
    if (t->discovery_waiting && t->round == ROUND_EPOCH)
        t->discovery_peer = epoch_peer(v);
    struct vsr_fetch request = {
        .max_bytes =
            v->options.limits.command_bytes + v->options.limits.manifest_bytes,
        .max_entries = 1,
    };
    if (!vsr_protocol_nonce(v, &request.nonce))
        return true;
    if (!send_control(v, t->discovery_peer, VSR_MSG_GET_STATE, 0, &request,
                      NULL, 0))
        return false;
    t->discovery_nonce = request.nonce;
    t->discovery_waiting = true;
    t->retry_at = vsr_after(v, v->options.retry_ns);
    return true;
}

/* Warm-up is continuous: an idle learner periodically rediscovers the
 * membership it knows and catches up to the latest committed position, or
 * is redirected to a later epoch, so the transfer at admission stays small.
 * One discovery per heartbeat, never while a transfer, an epoch handoff, or
 * a hard-state transaction is in progress. */
static bool warm_poll(struct vsr *v)
{
    struct vsr_transition *t = transition(v);
    if (v->status.state != VSR_STATE_WARMING || t->round != ROUND_NONE ||
        t->target.active || t->boot_restore || !v->time_set)
        return false;
    if (t->warm_at == VSR_NO_DEADLINE) {
        t->warm_at = vsr_after(v, v->options.heartbeat_ns);
        return false;
    }
    if (!expired(v, t->warm_at) || !hard_safe(v))
        return false;
    begin_discovery(v, v->status.primary, true);
    return true;
}

static bool start_view_poll(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_transition *t = transition(v);
    if (!vsr_protocol_ready(v))
        return false;
    if (t->ack_pending) {
        if (!send_control(v, v->status.primary, VSR_MSG_PREPARE_OK,
                          p->stable_end - 1, NULL, NULL, 0))
            return false;
        t->ack_pending = false;
        return true;
    }
    if (!t->start_pending)
        return false;
    uint32_t offer = offer_current(v);
    if (offer == VSR_INDEX_NONE)
        return false;
    for (uint32_t i = 0; i < p->current.count; i++) {
        if (i == p->self || t->peers[i].start_sent)
            continue;
        const struct retained_offer *state = &t->offers[offer];
        if (!send_offer(v, p->current.members[i].id, VSR_MSG_START_VIEW,
                        state->state.log_end - 1, &state->state, &state->lease,
                        1, offer))
            return false;
        t->peers[i].start_sent = true;
        return true;
    }
    t->start_pending = false;
    return false;
}

static bool boot_restore_poll(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_transition *t = transition(v);
    if (!t->boot_restore)
        return false;
    if (!t->boot_snapshot_started) {
        int result = vsr_checkpoint_adopt(v, NULL, VSR_INDEX_NONE,
                                          v->options.replica, v->status.role);
        if (result != VSR_OK)
            return false;
        t->boot_snapshot_started = true;
        return true;
    }
    if (vsr_checkpoint_busy(v) || p->application_busy || p->results_pending)
        return false;
    if (vsr_checkpoint_adoption_status(v) != VSR_OK)
        return false;
    if (v->status.role == VSR_MEMBER_FULL &&
        (v->status.applied < applied_needed(v) ||
         p->clients_stored < applied_needed(v)))
        return false;
    /* A durable restart preserves an uncommitted suffix. Its final entry can
     * fence all later proposals even though committed replay never loads it.
     * Inspect that tail before any normal request or read-fence NOOP can run. */
    if (p->log_end > p->stable_commit + 1 &&
        vsr_protocol_log_find(v, p->log_end - 1) == NULL)
        return vsr_protocol_load_log(v, p->log_end - 1, p->log_end);
    t->boot_restore = false;
    t->warmed = true;
    p->replay = false;
    if (t->restore_state == VSR_HARD_NORMAL)
        vsr_protocol_normal(v);
    else if (t->restore_state == VSR_HARD_VIEW_CHANGE)
        enter_view(v, v->status.view);
    else if (t->restore_state == VSR_HARD_TRANSITIONING) {
        v->status.state = VSR_STATE_TRANSITIONING;
        vsr_changed(v);
    }
    return true;
}

bool vsr_transition_poll(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_transition *t = transition(v);
    if (!t->enabled && !vsr_protocol_ready(v))
        return false;
    t->enabled = true;
    /* Protocol input can precede the first TIME. Arm previously untimed
     * intents when the host supplies its clock, including already sent fetches. */
    if (v->time_set) {
        if (t->round != ROUND_NONE && t->retry_at == VSR_NO_DEADLINE)
            t->retry_at = v->now;
        if ((t->round == ROUND_VIEW || t->round == ROUND_CATCHUP) &&
            t->election_at == VSR_NO_DEADLINE)
            t->election_at = vsr_after(v, v->options.view_timeout_ns);
        if (t->target.active && t->target.retry_at == VSR_NO_DEADLINE)
            t->target.retry_at = v->now;
        for (uint32_t i = 0; i < v->options.limits.transfers; i++)
            if (t->offers[i].used && t->offers[i].expires == VSR_NO_DEADLINE)
                t->offers[i].expires =
                    vsr_after(v, v->options.transfer_timeout_ns);
    }
    if (t->redirect_pending && hard_safe(v) &&
        p->epoch.boundary <= p->stable_commit &&
        send_control(v, t->redirect_peer, VSR_MSG_NEW_EPOCH, p->epoch.boundary,
                     &p->epoch, NULL, 0)) {
        t->redirect_pending = false;
        return true;
    }
    /* Drop idle offers before trying to allocate replacements. Active indexed
     * reads and selected local histories keep independent references. */
    for (uint32_t i = 0; i < v->options.limits.transfers; i++) {
        if (t->offers[i].used && t->offers[i].references == 0 &&
            expired(v, t->offers[i].expires)) {
            release_offer(v, i);
            return true;
        }
    }
    if (boot_restore_poll(v) || target_poll(v))
        return true;
    if (expired(v, t->election_at) &&
        (t->round == ROUND_CATCHUP ||
         (t->target.active && (t->target.goal == TARGET_PRIMARY ||
                               t->target.goal == TARGET_BACKUP))))
        return next_view(v);
    if (v->status.state == VSR_STATE_NORMAL && !t->target.active &&
        p->self != VSR_INDEX_NONE && v->status.primary != v->options.replica &&
        expired(v, p->election_at))
        return next_view(v);
    return view_poll(v) || recovery_poll(v) || discovery_poll(v) ||
           warm_poll(v) || start_view_poll(v) || recover_response_poll(v) ||
           server_poll(v);
}

/* A discovery reply that is not a chunk for a selected transfer: an offer
 * from the peer this replica asked, in the view it is entering. */
static void receive_discovery(struct vsr *v, const struct vsr_message *message,
                              const struct vsr_state_chunk *chunk,
                              uint32_t lease)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_transition *t = transition(v);
    if (!t->target.active && t->discovery_waiting &&
        message->from == t->discovery_peer &&
        message->type == VSR_MSG_NEW_STATE && chunk->first == 0 &&
        chunk->next == 0 && chunk->state.entries.count == 0 &&
        vsr_nonce_equal(chunk->nonce, t->discovery_nonce) &&
        (t->uninitialized_hint
             ? vsr_membership_equal(&p->current, chunk->state.epoch->current)
             : compatible_offer(v, &chunk->state)) &&
        chunk->state.committed >= t->committed_floor &&
        chunk->state.view == message->view &&
        chunk->state.last_normal_view == message->view) {
        if (message->view < v->status.view)
            return;
        if (t->uninitialized_hint) {
            bool recover = t->quorum_recovery;
            vsr_protocol_configuration(v, chunk->state.epoch);
            p->epoch.phase = VSR_EPOCH_TRANSFERRING;
            t->uninitialized_hint = false;
            p->identity_pending = true;
            p->hard_dirty = true;
            v->status.view = message->view;
            p->last_normal_view = 0;
            /* A replica absent from the discovered membership cannot
             * recover from it; it resumes warm-up as a learner. */
            if (recover && p->self != VSR_INDEX_NONE) {
                begin_recovery(v);
                return;
            }
        }
        v->status.view = message->view;
        v->status.primary = vsr_primary(&p->current, message->view);
        if (message->from != v->status.primary && t->round != ROUND_WARM &&
            t->round != ROUND_EPOCH)
            return;
        consider_offer(v, message->from, &chunk->state, lease, VSR_INDEX_NONE);
        /* A learner rebuilds once per incarnation; later warm-ups only
         * extend the state it already materialized. */
        select_best(v,
                    t->round == ROUND_EPOCH  ? TARGET_EPOCH
                    : t->round == ROUND_WARM ? TARGET_WARM
                                             : TARGET_CATCHUP,
                    t->round == ROUND_EPOCH ||
                        (t->round == ROUND_WARM && !t->warmed));
        t->discovery_waiting = false;
        if (t->round != ROUND_WARM)
            p->hard_dirty = true;
    }
}

static void receive_recovery_response(struct vsr *v,
                                      const struct vsr_message *message,
                                      uint32_t sender, uint32_t lease)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_transition *t = transition(v);
    const struct vsr_recovery *response = message->body;
    if (t->round != ROUND_RECOVERY || t->target.active ||
        !vsr_nonce_equal(response->nonce, t->recovery_nonce) ||
        message->view < v->status.view)
        return;
    uint64_t primary = vsr_primary(&p->current, message->view);
    if ((message->from == primary) != (response->state != NULL))
        return;
    if (response->state != NULL &&
        (!compatible_offer(v, response->state) ||
         response->state->view != message->view ||
         response->state->last_normal_view != message->view))
        return;
    if (message->view > t->highest_view) {
        v->status.view = message->view;
        v->status.primary = primary;
        begin_recovery(v);
        return;
    }
    t->peers[sender].recovered = true;
    if (response->state != NULL)
        consider_offer(v, message->from, response->state, lease,
                       VSR_INDEX_NONE);
}

static void receive_start_view_change(struct vsr *v,
                                      const struct vsr_message *message,
                                      uint32_t sender)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_transition *t = transition(v);
    if (v->status.state == VSR_STATE_RECOVERING ||
        v->status.state == VSR_STATE_WARMING ||
        p->epoch.phase == VSR_EPOCH_TRANSFERRING)
        return;
    if (message->view > v->status.view || t->round == ROUND_CATCHUP)
        enter_view(v, message->view);
    if (t->round == ROUND_VIEW && !t->target.active)
        t->peers[sender].svc = true;
}

static void receive_do_view_change(struct vsr *v,
                                   const struct vsr_message *message,
                                   uint32_t sender, uint32_t lease)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_transition *t = transition(v);
    if (v->status.state == VSR_STATE_RECOVERING ||
        v->status.state == VSR_STATE_WARMING ||
        p->epoch.phase == VSR_EPOCH_TRANSFERRING ||
        vsr_primary(&p->current, message->view) != v->options.replica)
        return;
    const struct vsr_log_state *state = message->body;
    if (!compatible_offer(v, state) || state->view != message->view)
        return;
    if (message->view > v->status.view || t->round == ROUND_CATCHUP)
        enter_view(v, message->view);
    if (t->round == ROUND_VIEW && !t->target.active) {
        t->peers[sender].dvc = true;
        consider_offer(v, message->from, state, lease, VSR_INDEX_NONE);
    }
}

static void receive_start_view(struct vsr *v, const struct vsr_message *message,
                               uint32_t lease)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_transition *t = transition(v);
    const struct vsr_log_state *state = message->body;
    if (t->round == ROUND_RECOVERY ||
        p->epoch.phase == VSR_EPOCH_TRANSFERRING ||
        !compatible_offer(v, state) || state->view != message->view ||
        state->last_normal_view != message->view)
        return;
    if (message->view == v->status.view && vsr_protocol_ready(v) &&
        state->log_end <= p->stable_end &&
        state->committed <= p->stable_commit) {
        t->ack_pending = true;
        return;
    }
    if (message->view > v->status.view)
        enter_view(v, message->view);
    if (t->target.active)
        return;
    if (t->round != ROUND_VIEW)
        enter_view(v, message->view);
    consider_offer(v, message->from, state, lease, VSR_INDEX_NONE);
    select_best(v, TARGET_BACKUP, false);
}

/* A PREPARE or COMMIT this replica cannot follow in place: a later view, a
 * gap in its log, or a state that is not NORMAL starts a catch-up. True
 * means the message was consumed here. */
static bool receive_normal_gap(struct vsr *v, const struct vsr_message *message)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_transition *t = transition(v);
    if (t->round == ROUND_RECOVERY || t->target.active ||
        v->status.state == VSR_STATE_WARMING ||
        p->epoch.phase == VSR_EPOCH_TRANSFERRING) {
        return true;
    }
    bool gap =
        message->type == VSR_MSG_COMMIT
            ? message->number >= p->log_end
            : ((const struct vsr_prepare *)message->body)->batch.entries[0].op >
                  p->log_end;
    if (message->view > v->status.view || gap ||
        v->status.state != VSR_STATE_NORMAL) {
        bool new_round =
            t->round != ROUND_CATCHUP || message->view > v->status.view;
        v->status.view = message->view;
        v->status.primary = message->from;
        if (new_round)
            begin_discovery(v, message->from, false);
        uint64_t known =
            message->type == VSR_MSG_COMMIT
                ? message->number
                : ((const struct vsr_prepare *)message->body)->committed;
        if (known > t->committed_floor)
            t->committed_floor = known;
        return true;
    }
    return false;
}

int vsr_transition_event(struct vsr *v, const struct vsr_event *event,
                         uint32_t lease, bool *handled)
{
    *handled = false;
    if (event->type != VSR_EVENT_MESSAGE)
        return VSR_OK;
    const struct vsr_message *message = event->data;
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_transition *t = transition(v);
    if (!vsr_id_equal(message->cluster, v->options.cluster)) {
        *handled = true;
        return VSR_OK;
    }
    if (message->epoch != v->status.epoch) {
        /* Announcements can be lost after handoff finishes. A surviving seed
         * member remains a discovery route for an older authenticated peer. */
        if (message->epoch < v->status.epoch && p->epoch.previous != NULL &&
            message->from != v->options.replica &&
            (message->type == VSR_MSG_GET_STATE ||
             message->type == VSR_MSG_GET_LOG ||
             vsr_member_index(&p->current, message->from) != VSR_INDEX_NONE ||
             vsr_member_index(p->epoch.previous, message->from) !=
                 VSR_INDEX_NONE)) {
            t->redirect_peer = message->from;
            t->redirect_pending = true;
            t->enabled = true;
        }
        *handled = true;
        return VSR_OK;
    }
    /* Retention advertisements are epoch-scoped, never view-scoped. A member
     * recovering or changing views still needs the promises that RESTORE and
     * TRIM count; dropping them below keeps a wiped witness waiting forever. */
    if (message->type == VSR_MSG_CHECKPOINT)
        return VSR_OK;
    if (message->type == VSR_MSG_GET_STATE ||
        message->type == VSR_MSG_GET_LOG) {
        *handled = true;
        return t->enabled || vsr_protocol_ready(v) ||
                       (v->status.state == VSR_STATE_TRANSITIONING &&
                        hard_safe(v))
                   ? receive_fetch(v, message)
                   : VSR_OK;
    }
    uint32_t sender = vsr_member_index(&p->current, message->from);
    if (message->type == VSR_MSG_NEW_STATE || message->type == VSR_MSG_LOG ||
        message->type == VSR_MSG_STATE_UNAVAILABLE) {
        *handled = true;
        const struct vsr_state_chunk *chunk = message->body;
        if (!receive_chunk(v, message, chunk, lease))
            receive_discovery(v, message, chunk, lease);
        return VSR_OK;
    }
    if (sender == VSR_INDEX_NONE || message->from == v->options.replica) {
        *handled = true;
        return VSR_OK;
    }
    if (message->type == VSR_MSG_RECOVERY) {
        *handled = true;
        if (vsr_protocol_ready(v)) {
            t->peers[sender].reply_nonce =
                ((const struct vsr_recovery *)message->body)->nonce;
            t->peers[sender].reply_pending = true;
        }
        return VSR_OK;
    }
    if (message->type == VSR_MSG_RECOVERY_RESPONSE) {
        *handled = true;
        receive_recovery_response(v, message, sender, lease);
        return VSR_OK;
    }
    bool primary_message = message->type == VSR_MSG_PREPARE ||
                           message->type == VSR_MSG_COMMIT ||
                           message->type == VSR_MSG_START_VIEW ||
                           message->type == VSR_MSG_READ_PROBE;
    if (primary_message &&
        message->from != vsr_primary(&p->current, message->view)) {
        *handled = true;
        return VSR_OK;
    }
    if (message->view < v->status.view) {
        *handled = true;
        return VSR_OK;
    }
    if (message->type == VSR_MSG_START_VIEW_CHANGE) {
        *handled = true;
        receive_start_view_change(v, message, sender);
        return VSR_OK;
    }
    if (message->type == VSR_MSG_DO_VIEW_CHANGE) {
        *handled = true;
        receive_do_view_change(v, message, sender, lease);
        return VSR_OK;
    }
    if (message->type == VSR_MSG_START_VIEW) {
        *handled = true;
        receive_start_view(v, message, lease);
        return VSR_OK;
    }
    if ((message->type == VSR_MSG_PREPARE || message->type == VSR_MSG_COMMIT) &&
        receive_normal_gap(v, message)) {
        *handled = true;
        return VSR_OK;
    }
    if (message->view > v->status.view)
        *handled = true;
    return VSR_OK;
}

void vsr_transition_complete(struct vsr *v, struct vsr_operation *operation,
                             const struct vsr_event *event, uint32_t lease)
{
    struct vsr_transition *t = transition(v);
    uint32_t tag = (uint32_t)operation->tag;
    if (tag == TAG_CONTROL)
        return;
    if (tag == TAG_OFFER_SEND) {
        uint32_t index = (uint32_t)(operation->tag >> 32);
        if (index < v->options.limits.transfers)
            offer_unreference(v, index);
        return;
    }
    if (tag == TAG_SERVE_LOAD) {
        uint32_t index = (uint32_t)(operation->tag >> 32);
        if (index >= v->options.limits.transfers)
            return;
        struct state_server *server = &t->servers[index];
        if (server->phase != SERVER_LOADING ||
            server->load_id != operation->output.id)
            return;
        if (event->status == VSR_IO_RETRY) {
            server->phase = SERVER_LOAD;
            server->retry_at = vsr_after(v, v->options.retry_ns);
        } else if (event->status != VSR_IO_OK) {
            vsr_fail(v, VSR_FAILURE_STORAGE, operation, event->status);
        } else {
            /* A failed retain has failed the engine; nothing is served. */
            if (!vsr_lease_retain(v, lease))
                return;
            server->lease = lease;
            server->loaded = event->data;
            server->phase = SERVER_REPLY;
        }
        return;
    }
    struct selected_target *target = &t->target;
    if (!target->active)
        return;
    if (tag == TAG_TARGET_LOAD && target->load_id == operation->output.id) {
        target->load_id = 0;
        if (event->status == VSR_IO_RETRY)
            return;
        if (event->status != VSR_IO_OK) {
            vsr_fail(v, VSR_FAILURE_STORAGE, operation, event->status);
            return;
        }
        const struct vsr_loaded *loaded = event->data;
        if (loaded->count == 0 || loaded->next > target->state.log_end) {
            vsr_fail(v, VSR_FAILURE_STORAGE, operation, event->status);
            return;
        }
        take_chunk(v, loaded->items, loaded->count, lease);
    } else if (tag == TAG_ANCHOR_LOAD &&
               target->anchor_id == operation->output.id) {
        target->anchor_id = 0;
        if (event->status == VSR_IO_RETRY)
            return;
        /* Absence means the prefix is not retained readably: replace it. */
        if (event->status == VSR_IO_NOT_FOUND) {
            target->anchor = ANCHOR_MISSING;
            return;
        }
        if (event->status != VSR_IO_OK) {
            vsr_fail(v, VSR_FAILURE_STORAGE, operation, event->status);
            return;
        }
        const struct vsr_loaded *loaded = event->data;
        const struct vsr_entry *entries = loaded->items;
        target->anchor =
            loaded->count == 1 &&
                    anchor_retained(v, &entries[0], target->state.checkpoint)
                ? ANCHOR_RETAINED
                : ANCHOR_MISSING;
    } else if (tag == TAG_COMPARE_LOAD &&
               target->compare_id == operation->output.id) {
        target->compare_id = 0;
        if (event->status == VSR_IO_RETRY)
            return;
        if (event->status != VSR_IO_OK) {
            vsr_fail(v, VSR_FAILURE_STORAGE, operation, event->status);
            return;
        }
        const struct vsr_loaded *loaded = event->data;
        const struct vsr_entry *entries = loaded->items;
        uint32_t offset = target->append_offset;
        if (loaded->count == 0 ||
            loaded->count > target->entry_count - offset) {
            vsr_fail(v, VSR_FAILURE_STORAGE, operation, event->status);
            return;
        }
        for (uint32_t i = 0; i < loaded->count; i++) {
            if (!vsr_entry_equal(&entries[i], &target->entries[offset + i])) {
                if (entries[i].op <= vsr_protocol(v)->stable_commit) {
                    vsr_fail(v, VSR_FAILURE_INVARIANT, operation,
                             event->status);
                    return;
                }
                target->append_offset = offset + i;
                target->phase = TARGET_APPEND;
                return;
            }
        }
        /* A load bounded by the payload budget may cover only part of the
         * overlap; the comparison resumes past the verified prefix. */
        target->append_offset = offset + loaded->count;
    }
}

uint64_t vsr_transition_deadline(const struct vsr *v)
{
    const struct vsr_transition *t = transition_const(v);
    uint64_t result = VSR_NO_DEADLINE;
    if (!t->enabled)
        return result;
    if (t->round != ROUND_NONE)
        result = t->retry_at;
    if ((t->round == ROUND_VIEW || t->round == ROUND_CATCHUP ||
         t->target.active) &&
        t->election_at < result)
        result = t->election_at;
    if (t->target.active && t->target.waiting && t->target.retry_at < result)
        result = t->target.retry_at;
    if (t->warm_at < result)
        result = t->warm_at;
    for (uint32_t i = 0; i < v->options.limits.transfers; i++) {
        if (t->offers[i].used && t->offers[i].references == 0 &&
            t->offers[i].expires < result)
            result = t->offers[i].expires;
        if (t->servers[i].phase == SERVER_LOAD &&
            t->servers[i].retry_at < result)
            result = t->servers[i].retry_at;
    }
    return result;
}

void vsr_transition_stop(struct vsr *v)
{
    struct vsr_transition *t = transition(v);
    reset_round(v);
    for (uint32_t i = 0; i < v->options.limits.transfers; i++) {
        vsr_lease_release(v, t->servers[i].lease);
        t->servers[i].lease = VSR_INDEX_NONE;
        if (t->servers[i].phase != SERVER_FREE)
            offer_unreference(v, t->servers[i].offer);
        t->servers[i].phase = SERVER_FREE;
        release_offer(v, i);
    }
    t->enabled = false;
}

bool vsr_transition_boot_loaded(struct vsr *v,
                                const struct vsr_recovered *recovered,
                                uint32_t lease)
{
    struct vsr_transition *t = transition(v);
    struct vsr_protocol *p = vsr_protocol(v);
    t->enabled = true;
    if (recovered->checkpoint != NULL) {
        if (!vsr_checkpoint_recover(v, recovered->checkpoint, lease,
                                    v->status.role)) {
            vsr_fail(v, VSR_FAILURE_INVARIANT, NULL, VSR_IO_OK);
            return true;
        }
        t->boot_snapshot_started = true;
    }
    if (recovered->hard.state == VSR_HARD_RETIRED) {
        v->status.state = VSR_STATE_RETIRED;
        p->replay = false;
        vsr_changed(v);
        return true;
    }
    /* A learner belongs to neither group of its persisted configuration.
     * Quorum recovery is for members; a learner resumes the nonvoting
     * warm-up its hard state records, in the role it recorded. In a STEADY
     * epoch it rediscovers the group; an unfinished epoch installation
     * restores like a member's and the handoff resumes. */
    if (learner(v)) {
        if (p->epoch.phase == VSR_EPOCH_STEADY) {
            begin_discovery(v, v->status.primary, true);
            return true;
        }
        t->restore_state = VSR_HARD_TRANSITIONING;
        t->boot_restore = true;
        p->replay = true;
        return true;
    }
    if (v->options.durability == VSR_REPLICATED ||
        recovered->hard.state == VSR_HARD_RECOVERING) {
        begin_recovery(v);
        return true;
    }
    t->restore_state = recovered->hard.state;
    t->boot_restore = recovered->hard.state != VSR_HARD_RETIRED;
    p->replay = t->boot_restore;
    return true;
}

bool vsr_transition_boot_missing(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    p->identity_pending = true;
    p->hard_dirty = true;
    if (v->options.seed->epoch != 0) {
        begin_discovery(v, v->status.primary, true);
        struct vsr_transition *t = transition(v);
        t->uninitialized_hint = true;
        t->quorum_recovery = v->options.start_mode == VSR_START_RECOVER;
        p->identity_pending = false;
        p->hard_dirty = false;
    } else if (v->options.start_mode == VSR_START_JOIN ||
               p->self == VSR_INDEX_NONE) {
        /* Nothing to restore and no group to recover from: a replica absent
         * from its seed can only warm up from scratch as a learner. */
        begin_discovery(v, v->status.primary, true);
    } else {
        begin_recovery(v);
    }
    return true;
}

bool vsr_transition_apply_allowed(const struct vsr *v)
{
    const struct vsr_transition *t = transition_const(v);
    if (t->target.active)
        return t->target.phase >= TARGET_REPLAY;
    if (t->boot_restore)
        return t->boot_snapshot_started && !vsr_checkpoint_busy(v);
    return true;
}

uint64_t vsr_transition_min_sequence(const struct vsr *v)
{
    const struct vsr_transition *t = transition_const(v);
    uint64_t minimum = vsr_protocol_const(v)->safe_sequence;
    for (uint32_t i = 0; i < v->options.limits.transfers; i++) {
        if (t->offers[i].used && t->offers[i].state.revision.sequence < minimum)
            minimum = t->offers[i].state.revision.sequence;
    }
    if (t->target.active && t->target.source == v->options.replica &&
        t->target.state.revision.sequence < minimum)
        minimum = t->target.state.revision.sequence;
    return minimum;
}

bool vsr_transition_epoch_fence(struct vsr *v)
{
    struct vsr_transition *t = transition(v);
    struct vsr_protocol *p = vsr_protocol(v);
    if (t->uninitialized_hint && p->next_sequence == 1 &&
        v->status.stored_sequence == 0) {
        t->uninitialized_hint = false;
        return true;
    }
    if (t->epoch_fence_sequence != 0) {
        return p->safe_sequence >= t->epoch_fence_sequence;
    }
    v->status.state = VSR_STATE_RECOVERING;
    if (p->safe_sequence + 1 != p->next_sequence || p->application_busy ||
        p->results_pending)
        return false;
    vsr_protocol_hard(v, &t->old_hard);
    t->old_hard.state = VSR_HARD_RECOVERING;
    t->old_hard.committed = p->stable_commit;
    copy_epoch(v, &t->old_epoch, &t->old_current, &t->old_previous,
               t->old_members, &p->epoch);
    t->old_hard.epoch = &t->old_epoch;
    struct vsr_change change = {VSR_STORE_HARD_STATE, 1, 0, &t->old_hard};
    if (!vsr_protocol_store(v, &change, 1, NULL, 0, p->written_end,
                            p->stable_commit, 0, &t->epoch_fence_sequence))
        return false;
    p->hard_sequence = t->epoch_fence_sequence;
    p->hard_dirty = false;
    return false;
}

bool vsr_transition_epoch(struct vsr *v, uint64_t peer, uint64_t boundary)
{
    struct vsr_transition *t = transition(v);
    reset_round(v);
    t->enabled = true;
    t->round = ROUND_EPOCH;
    /* The caller crossed the old-epoch fence. A later handoff must establish
     * its own fence rather than reuse this completed transaction forever. */
    t->epoch_fence_sequence = 0;
    t->epoch_boundary = boundary;
    if (t->committed_floor < boundary)
        t->committed_floor = boundary;
    t->discovery_peer = peer;
    t->retry_at = v->time_set ? v->now : VSR_NO_DEADLINE;
    t->election_at = VSR_NO_DEADLINE;
    vsr_protocol(v)->hard_dirty = true;
    if (vsr_protocol(v)->next_sequence == 1 && v->status.stored_sequence == 0)
        vsr_protocol(v)->identity_pending = true;
    vsr_protocol(v)->replay = false;
    v->status.state = VSR_STATE_TRANSITIONING;
    return true;
}

bool vsr_transition_busy(const struct vsr *v)
{
    const struct vsr_transition *t = transition_const(v);
    return t->target.active || t->boot_restore || t->round != ROUND_NONE;
}

void vsr_transition_epoch_entered(struct vsr *v)
{
    struct vsr_transition *t = transition(v);
    /* The committed boundary closes the old epoch. A view change or
     * catch-up still running for it has nothing left to select: its offers,
     * votes, and pending acknowledgments describe old-epoch views and must
     * not carry into the new epoch, which starts at view zero. The committed
     * floor survives because positions continue across the boundary. */
    reset_round(v);
    t->round = ROUND_NONE;
    t->highest_view = 0;
    t->retry_at = VSR_NO_DEADLINE;
    t->election_at = VSR_NO_DEADLINE;
    /* The fence that preceded this entry, if any, is consumed: a later
     * handoff must establish its own rather than reuse a completed one. */
    t->epoch_fence_sequence = 0;
}

bool vsr_transition_epoch_recover(struct vsr *v)
{
    struct vsr_transition *t = transition(v);
    struct vsr_protocol *p = vsr_protocol(v);
    /* Membership knowledge never establishes voting readiness. A replica
     * that owes a quorum recovery, and would vote in the learned epoch,
     * restarts that recovery there: responses now come from NORMAL members
     * of the learned membership, its quorum applies, and the highest-view
     * primary's complete history replaces whatever one donor could offer.
     * Every further epoch learned meanwhile restarts it the same way. The
     * single-donor transfer stays reserved for a replica whose own state is
     * authoritative through its committed prefix and that cast no vote in
     * the learned epoch: an old-epoch member learning the boundary, an
     * unfinished durable installation, or a learner, which never votes. A
     * removed member that lost its state cannot vote either way and only
     * needs the history through the boundary in order to retire.
     *
     * With f = 0 the quorum of n responses from n - 1 peers can never form,
     * and every quorum of the learned epoch is its whole membership: any
     * NORMAL member took part in every decision this replica could have
     * voted in, so one such member's history is authoritative and the
     * single-donor transfer is the recovery. */
    if (!t->quorum_recovery || p->self == VSR_INDEX_NONE ||
        p->current.faults == 0)
        return false;
    t->epoch_boundary = p->epoch.boundary;
    if (p->next_sequence == 1 && v->status.stored_sequence == 0)
        p->identity_pending = true;
    p->replay = false;
    begin_recovery(v);
    if (t->committed_floor < p->epoch.boundary)
        t->committed_floor = p->epoch.boundary;
    return true;
}

void vsr_transition_hard(struct vsr *v, struct vsr_hard_state *hard)
{
    /* A known later boundary is explicit nonvoting metadata until its history
     * arrives. Never downgrade the persisted epoch to make local bounds fit.
     * A quorum recovery in progress keeps its RECOVERING record, which the
     * validator accepts with such a boundary, so a durable restart resumes
     * the recovery rather than an installation it never qualified for. */
    if (hard->state == VSR_HARD_RECOVERING &&
        v->status.state == VSR_STATE_RECOVERING)
        return;
    if (hard->epoch->boundary > hard->committed &&
        hard->epoch->phase == VSR_EPOCH_TRANSFERRING)
        hard->state = VSR_HARD_TRANSITIONING;
}
