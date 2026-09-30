#include "config.h"

#include "checked.h"
#include "checkpoint.h"
#include "epochs.h"
#include "extension.h"
#include "transition.h"

#include <stdalign.h>
#include <string.h>

enum { EPOCH_SEND = VSR_TAG_EPOCH_FIRST };
enum epoch_stage {
    EPOCH_IDLE,
    EPOCH_NEW_FENCE,
    EPOCH_FENCE,
    EPOCH_CATCHUP,
    EPOCH_INSTALL_WAIT,
    EPOCH_INSTALLED,
    EPOCH_STEADY_WAIT
};

struct vsr_epochs {
    unsigned char *started;
    const struct vsr_epoch *pending;
    struct vsr_epoch pending_epoch;
    struct vsr_membership pending_current;
    struct vsr_membership pending_previous;
    struct vsr_member *pending_members;
    struct vsr_member *pending_previous_members;
    uint64_t pending_peer;
    uint64_t learner;
    uint64_t sequence;
    uint64_t peer;
    uint64_t retry_at;
    uint64_t epoch;
    uint64_t cursor;
    uint32_t stage;
    uint32_t sends;
    bool installed;
    bool transferring;
    bool announce;
    bool promise;
    bool guard;
    /* The next epoch's boundary committed in this one: evidence that this
     * handoff completed, standing in for promises still outstanding. */
    bool successor;
};

static struct vsr_epochs *epochs(struct vsr *v)
{
    return ((struct vsr_extension *)vsr_protocol(v)->extension)->epochs;
}
static const struct vsr_epochs *epochs_const(const struct vsr *v)
{
    return ((const struct vsr_extension *)vsr_protocol_const(v)->extension)
        ->epochs;
}

int vsr_epochs_size(const struct vsr_options *o, size_t *size,
                    size_t *alignment)
{
    *alignment = alignof(max_align_t);
    size_t members;
    return vsr_size_mul(o->limits.members, 2 * sizeof(struct vsr_member),
                        &members) &&
                   vsr_size_add(sizeof(struct vsr_epochs), members, size) &&
                   vsr_size_add(*size, o->limits.members, size)
               ? VSR_OK
               : VSR_ELIMIT;
}

void vsr_epochs_init(struct vsr *v, void *memory, size_t size)
{
    (void)size;
    struct vsr_epochs *e = memory;
    ((struct vsr_extension *)vsr_protocol(v)->extension)->epochs = e;
    e->pending_members = (void *)(e + 1);
    e->pending_previous_members =
        e->pending_members + v->options.limits.members;
    e->started =
        (void *)(e->pending_previous_members + v->options.limits.members);
    e->retry_at = VSR_NO_DEADLINE;
}

static uint32_t member_role(const struct vsr_membership *membership,
                            uint64_t id)
{
    if (membership == NULL)
        return VSR_MEMBER_NONE;
    uint32_t index = vsr_member_index(membership, id);
    return index == VSR_INDEX_NONE ? VSR_MEMBER_NONE
                                   : membership->members[index].role;
}

/* A JOIN replica that belongs to neither group of the current handoff. It
 * warms without a vote or a retention obligation and is never a donor. */
static bool learner(const struct vsr *v)
{
    const struct vsr_protocol *p = vsr_protocol_const(v);
    return p->self == VSR_INDEX_NONE &&
           member_role(p->epoch.previous, v->options.replica) ==
               VSR_MEMBER_NONE;
}

/* The local replica has sent EPOCH_STARTED for the current epoch, so its
 * promised coverage is already recoverable and retained. */
static bool promised(const struct vsr *v)
{
    const struct vsr_protocol *p = vsr_protocol_const(v);
    const struct vsr_epochs *e = epochs_const(v);
    return e->installed && p->self != VSR_INDEX_NONE &&
           e->epoch == p->current.epoch;
}

static void protect(struct vsr *v)
{
    struct vsr_epochs *e = epochs(v);
    if (!e->guard) {
        vsr_checkpoint_guard(v, true);
        e->guard = true;
    }
}

static uint64_t donor(const struct vsr *v)
{
    const struct vsr_protocol *p = vsr_protocol_const(v);
    if (p->epoch.previous != NULL) {
        for (uint32_t i = 0; i < p->previous.count; ++i)
            if (p->previous.members[i].role == VSR_MEMBER_FULL &&
                p->previous.members[i].id != v->options.replica)
                return p->previous.members[i].id;
    }
    for (uint32_t i = 0; i < p->current.count; ++i)
        if (p->current.members[i].role == VSR_MEMBER_FULL &&
            p->current.members[i].id != v->options.replica)
            return p->current.members[i].id;
    return 0;
}

static bool stable_configuration(const struct vsr *v)
{
    const struct vsr_protocol *p = vsr_protocol_const(v);
    return !p->hard_dirty && p->safe_sequence >= p->hard_sequence &&
           p->stable_commit >= p->epoch.boundary &&
           p->stable_end > p->epoch.boundary;
}

static bool locally_installed(const struct vsr *v)
{
    const struct vsr_protocol *p = vsr_protocol_const(v);
    if (!stable_configuration(v) || vsr_transition_busy(v))
        return false;
    uint32_t role = member_role(&p->current, v->options.replica);
    uint32_t previous = member_role(p->epoch.previous, v->options.replica);
    if (role == VSR_MEMBER_FULL && v->status.role != VSR_MEMBER_FULL)
        return false;
    if (role == VSR_MEMBER_FULL || previous == VSR_MEMBER_FULL ||
        (learner(v) && v->status.role == VSR_MEMBER_FULL))
        return v->status.applied >= p->epoch.boundary &&
               p->clients_stored >= p->epoch.boundary && !p->results_pending;
    return true;
}

static bool quorum_ready(const struct vsr *v)
{
    const struct vsr_protocol *p = vsr_protocol_const(v);
    const struct vsr_epochs *e = epochs_const(v);
    uint32_t votes = 0, full = 0;
    for (uint32_t i = 0; i < p->current.count; ++i) {
        if (!e->started[i])
            continue;
        votes++;
        if (p->current.members[i].role == VSR_MEMBER_FULL)
            full++;
    }
    return votes >= p->current.count - p->current.faults &&
           full >= p->current.faults + 1;
}

static void arm(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_epochs *e = epochs(v);
    memset(e->started, 0, v->options.limits.members);
    e->epoch = p->current.epoch;
    e->sequence = 0;
    e->installed = false;
    e->transferring = false;
    e->announce = true;
    e->promise = false;
    e->learner = 0;
    e->cursor = 0;
    e->peer = donor(v);
    e->stage = EPOCH_FENCE;
    e->successor = false;
    /* A descriptor still waiting for its fence describes at most this
     * epoch; a later one is learned again from the peers' redirects. */
    e->pending = NULL;
    e->retry_at = vsr_after(v, v->options.retry_ns);
    protect(v);
}

static void begin(struct vsr *v, const struct vsr_epoch *epoch, uint64_t peer)
{
    struct vsr_protocol *p = vsr_protocol(v);
    /* The boundary has been authenticated as committed, but local catch-up
     * is separate. No voter/leader path remains enabled after this fence.
     * Commitment can be learned from any state, including VIEW_CHANGE: the
     * quorum was collected in the old epoch, whose rounds end here. A replica
     * that already holds the complete history through the boundary then
     * installs directly, exactly as it would from NORMAL. */
    v->status.state = VSR_STATE_TRANSITIONING;
    v->status.view = 0;
    p->last_normal_view = 0;
    vsr_transition_epoch_entered(v);
    vsr_protocol_configuration(v, epoch);
    p->epoch.phase = VSR_EPOCH_TRANSFERRING;
    p->replay = v->status.role == VSR_MEMBER_FULL;
    arm(v);
    if (peer != 0 && (member_role(&p->current, peer) == VSR_MEMBER_FULL ||
                      member_role(p->epoch.previous, peer) == VSR_MEMBER_FULL))
        epochs(v)->peer = peer;
    if (p->stable_commit >= epoch->boundary && p->stable_end > epoch->boundary)
        p->hard_dirty = true;
    vsr_changed(v);
}

void vsr_epochs_committed(struct vsr *v, const struct vsr_entry *entry)
{
    struct vsr_protocol *p = vsr_protocol(v);
    if (entry->type != VSR_REQUEST_RECONFIGURE ||
        entry->epoch != p->current.epoch)
        return;
    const struct vsr_membership *next = entry->body;
    if (next->epoch != p->current.epoch + 1) {
        vsr_fail(v, VSR_FAILURE_INVARIANT, NULL, VSR_IO_OK);
        return;
    }
    if (p->epoch.phase != VSR_EPOCH_STEADY) {
        /* The primary admitted this boundary only after its own STEADY, so
         * the handoff into the current epoch necessarily completed: a later
         * committed epoch establishes STEADY without the promises still
         * outstanding here. A backup commits it before collecting them
         * because promise collection is independent of the view, and a
         * transfer can copy it from a donor whose notification lags. A
         * member or learner finishes the handoff locally and enters the
         * next epoch; a removed donor retires through this evidence rather
         * than following a group it does not belong to. */
        if (p->self == VSR_INDEX_NONE && !learner(v)) {
            epochs(v)->successor = true;
            return;
        }
        uint32_t role = member_role(&p->current, v->options.replica);
        if (role != VSR_MEMBER_NONE) {
            v->status.role = role;
            if (role == VSR_MEMBER_WITNESS)
                v->status.applied = 0;
        }
    }
    /* Configuration copying must preserve the old current array before it is
     * overwritten. The caller's entry graph remains pinned through this hook. */
    p->previous = p->current;
    memcpy(p->previous_members, p->current.members,
           (size_t)p->current.count * sizeof(*p->previous_members));
    p->previous.members = p->previous_members;
    struct vsr_epoch epoch = {next, &p->previous, entry->op,
                              VSR_EPOCH_TRANSFERRING, 0};
    begin(v, &epoch, 0);
}

bool vsr_epochs_ready(const struct vsr *v, uint64_t target)
{
    const struct vsr_protocol *p = vsr_protocol_const(v);
    return target == 0 || target < p->current.epoch ||
           (target == p->current.epoch && p->epoch.phase == VSR_EPOCH_STEADY &&
            stable_configuration(v));
}

bool vsr_epochs_apply_allowed(const struct vsr *v,
                              const struct vsr_entry *entry)
{
    const struct vsr_protocol *p = vsr_protocol_const(v);
    uint32_t role = member_role(&p->current, v->options.replica);
    uint32_t previous = member_role(p->epoch.previous, v->options.replica);
    if (role != VSR_MEMBER_FULL &&
        (role == VSR_MEMBER_WITNESS || previous == VSR_MEMBER_FULL) &&
        entry->op > p->epoch.boundary)
        return false;
    if (entry->type == VSR_REQUEST_CHECK_EPOCH &&
        !vsr_epochs_ready(v,
                          ((const struct vsr_check_epoch *)entry->body)->epoch))
        return false;
    return true;
}

void vsr_epochs_recovered(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    if (p->current.epoch == 0 || p->epoch.phase == VSR_EPOCH_STEADY)
        return;
    arm(v);
    if (p->epoch.phase == VSR_EPOCH_INSTALLED)
        epochs(v)->stage = EPOCH_CATCHUP;
}

void vsr_epochs_normal(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_epochs *e = epochs(v);
    if (p->current.epoch == 0)
        return;
    if (p->epoch.phase == VSR_EPOCH_STEADY) {
        /* Recovered STEADY members retain their handoff promise and must answer
         * an installed peer that lost the earlier EPOCH_STARTED delivery. */
        e->epoch = p->current.epoch;
        e->installed = p->self != VSR_INDEX_NONE;
        e->stage = EPOCH_IDLE;
        e->retry_at = VSR_NO_DEADLINE;
        return;
    }
    if (e->epoch != p->current.epoch)
        arm(v);
    if (p->epoch.phase == VSR_EPOCH_INSTALLED && locally_installed(v)) {
        e->installed = true;
        e->stage = EPOCH_INSTALLED;
        e->promise = p->self != VSR_INDEX_NONE;
        e->announce = true;
        e->cursor = 0;
        if (p->self != VSR_INDEX_NONE)
            e->started[p->self] = 1;
    }
}

int vsr_epochs_event(struct vsr *v, const struct vsr_event *event,
                     uint32_t lease, bool *handled)
{
    (void)lease;
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_epochs *e = epochs(v);
    *handled = false;
    if (event->type == VSR_EVENT_TIME) {
        if (e->stage != EPOCH_IDLE &&
            (e->retry_at == VSR_NO_DEADLINE || v->now >= e->retry_at)) {
            e->announce = true;
            e->promise = e->installed && p->self != VSR_INDEX_NONE;
            e->cursor = 0;
            e->retry_at = vsr_after(v, v->options.retry_ns);
        }
        return VSR_OK;
    }
    if (event->type != VSR_EVENT_MESSAGE)
        return VSR_OK;
    const struct vsr_message *message = event->data;
    if (message->type != VSR_MSG_START_EPOCH &&
        message->type != VSR_MSG_NEW_EPOCH &&
        message->type != VSR_MSG_EPOCH_STARTED)
        return VSR_OK;
    *handled = true;
    if (!vsr_id_equal(message->cluster, v->options.cluster) ||
        message->from == v->options.replica)
        return VSR_OK;
    if (message->type == VSR_MSG_EPOCH_STARTED) {
        if (message->epoch != p->current.epoch ||
            message->number != p->epoch.boundary ||
            e->epoch != p->current.epoch)
            return VSR_OK;
        uint32_t i = vsr_member_index(&p->current, message->from);
        if (i != VSR_INDEX_NONE && i != p->self)
            e->started[i] = 1;
        return VSR_OK;
    }
    const struct vsr_epoch *epoch = message->body;
    if (epoch->current->epoch < p->current.epoch)
        return VSR_OK;
    if (epoch->current->epoch == p->current.epoch) {
        if (!vsr_membership_equal(epoch->current, &p->current) ||
            epoch->boundary != p->epoch.boundary)
            return VSR_OK;
        if (member_role(&p->current, message->from) == VSR_MEMBER_NONE &&
            member_role(p->epoch.previous, message->from) == VSR_MEMBER_NONE) {
            /* An authenticated learner outside both groups collects the
             * same promises as a member. Answering it only retransmits a
             * promise this replica already made to both groups; it creates
             * no vote and no further retention obligation. */
            if (promised(v))
                e->learner = message->from;
            return VSR_OK;
        }
        if (promised(v))
            e->promise = true;
        return VSR_OK;
    }
    /* Under the authenticated crash-only model a known current/seed member
     * can report a later committed configuration even after intermediate
     * descriptors have been compacted. This grants metadata knowledge only;
     * the receiver remains TRANSFERRING until the complete prefix is checked. */
    if (epoch->previous == NULL ||
        (member_role(&p->current, message->from) == VSR_MEMBER_NONE &&
         member_role(v->options.seed, message->from) == VSR_MEMBER_NONE) ||
        (epoch->current->epoch == p->current.epoch + 1 &&
         !vsr_membership_equal(epoch->previous, &p->current)))
        return VSR_OK;
    if (epoch->boundary < p->stable_commit) {
        vsr_fail(v, VSR_FAILURE_INVARIANT, NULL, VSR_IO_OK);
        return VSR_OK;
    }
    if (p->stable_commit < epoch->boundary ||
        p->stable_end <= epoch->boundary) {
        if (e->pending != NULL)
            return VSR_OK;
        e->pending_current = *epoch->current;
        memcpy(e->pending_members, epoch->current->members,
               (size_t)epoch->current->count * sizeof(struct vsr_member));
        e->pending_current.members = e->pending_members;
        e->pending_previous = *epoch->previous;
        memcpy(e->pending_previous_members, epoch->previous->members,
               (size_t)epoch->previous->count * sizeof(struct vsr_member));
        e->pending_previous.members = e->pending_previous_members;
        e->pending_epoch = *epoch;
        e->pending_epoch.current = &e->pending_current;
        e->pending_epoch.previous = &e->pending_previous;
        e->pending = &e->pending_epoch;
        e->pending_peer = message->from;
        e->stage = EPOCH_NEW_FENCE;
        v->status.state = VSR_STATE_RECOVERING;
        protect(v);
        vsr_changed(v);
    } else
        begin(v, epoch, message->from);
    return VSR_OK;
}

/* A learner that discovered its epoch while the boundary was still ahead of
 * it recorded the descriptor as TRANSFERRING. Once warm-up has carried it
 * through the boundary it installs like a member and collects the group's
 * promises, since only STEADY makes it admissible. */
static bool learner_installs(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    if (v->status.state != VSR_STATE_WARMING ||
        p->epoch.phase != VSR_EPOCH_TRANSFERRING || !learner(v) ||
        vsr_transition_busy(v) || !stable_configuration(v))
        return false;
    v->status.state = VSR_STATE_TRANSITIONING;
    arm(v);
    vsr_changed(v);
    return true;
}

static bool persist_phase(struct vsr *v, bool steady)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_epochs *e = epochs(v);
    if (p->next_sequence != p->safe_sequence + 1 || p->hard_dirty)
        return false;
    struct vsr_epoch epoch = p->epoch;
    epoch.phase = steady ? VSR_EPOCH_STEADY : VSR_EPOCH_INSTALLED;
    struct vsr_hard_state hard;
    vsr_protocol_hard(v, &hard);
    hard.epoch = &epoch;
    if (!steady && p->self != VSR_INDEX_NONE) {
        hard.state = VSR_HARD_NORMAL;
        hard.last_normal_view = hard.view;
    }
    /* Only a removed donor retires. A learner records the nonvoting warm-up
     * state so a restart resumes discovery instead of a removal tombstone. */
    if (steady && p->self == VSR_INDEX_NONE)
        hard.state = learner(v) ? VSR_HARD_RECOVERING : VSR_HARD_RETIRED;
    uint32_t role = member_role(&p->current, v->options.replica);
    if (steady && role != VSR_MEMBER_NONE)
        hard.role = role;
    struct vsr_change change = {VSR_STORE_HARD_STATE, 1, 0, &hard};
    if (!vsr_protocol_store(v, &change, 1, NULL, 0, p->written_end,
                            p->stable_commit, 0, &e->sequence))
        return false;
    p->hard_sequence = e->sequence;
    e->stage = steady ? EPOCH_STEADY_WAIT : EPOCH_INSTALL_WAIT;
    return true;
}

static bool send_epoch(struct vsr *v, uint64_t peer, uint32_t type)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_epochs *e = epochs(v);
    bool announce = type == VSR_MSG_START_EPOCH;
    struct vsr_message message = {v->options.cluster,
                                  p->current.epoch,
                                  v->status.view,
                                  v->options.replica,
                                  type,
                                  0,
                                  p->epoch.boundary,
                                  announce ? &p->epoch : NULL};
    if (!vsr_protocol_emit(v, VSR_OP_SEND, peer, EPOCH_SEND, &message, NULL, 0,
                           0))
        return false;
    e->sends++;
    return true;
}

static bool send_announcements(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_epochs *e = epochs(v);
    if (!stable_configuration(v))
        return false;
    if (e->learner != 0) {
        /* One slot suffices: a learner retransmits START_EPOCH until STEADY,
         * so a reply lost to a concurrent learner is requested again. */
        if (promised(v) && !send_epoch(v, e->learner, VSR_MSG_EPOCH_STARTED))
            return false;
        e->learner = 0;
        return true;
    }
    if (!e->announce && !e->promise)
        return false;
    uint64_t total = (uint64_t)p->current.count + p->previous.count;
    while (e->cursor < total) {
        uint64_t index = e->cursor;
        uint64_t peer = index < p->current.count
                            ? p->current.members[index].id
                            : p->previous.members[index - p->current.count].id;
        if (peer == v->options.replica) {
            e->cursor++;
            continue;
        }
        if (!send_epoch(v, peer,
                        e->announce ? VSR_MSG_START_EPOCH
                                    : VSR_MSG_EPOCH_STARTED))
            return false;
        e->cursor++;
        return true;
    }
    e->cursor = 0;
    if (e->announce)
        e->announce = false;
    else
        e->promise = false;
    return true;
}

bool vsr_epochs_poll(struct vsr *v)
{
    struct vsr_protocol *p = vsr_protocol(v);
    struct vsr_epochs *e = epochs(v);
    if (v->status.state == VSR_STATE_RETIRED)
        return false;
    if (e->stage == EPOCH_IDLE)
        return learner_installs(v) || send_announcements(v);
    if (e->stage == EPOCH_NEW_FENCE) {
        if (p->stable_commit < e->pending->boundary &&
            !vsr_transition_epoch_fence(v))
            return false;
        const struct vsr_epoch *epoch = e->pending;
        uint64_t peer = e->pending_peer;
        e->pending = NULL;
        begin(v, epoch, peer);
        return true;
    }
    if (e->stage == EPOCH_FENCE) {
        /* Missing state requires quorum recovery, and a membership learned
         * meanwhile does not change that: the recovery restarts in this
         * epoch, whatever history is locally present, and hands over the
         * installation like the single-donor transfer below. */
        if (vsr_transition_epoch_recover(v)) {
            e->transferring = true;
            e->stage = EPOCH_CATCHUP;
            return true;
        }
        if (p->stable_commit < p->epoch.boundary ||
            p->stable_end <= p->epoch.boundary ||
            (member_role(&p->current, v->options.replica) == VSR_MEMBER_FULL &&
             v->status.role != VSR_MEMBER_FULL)) {
            if (!e->transferring && e->peer != 0 &&
                vsr_transition_epoch(v, e->peer, p->epoch.boundary)) {
                e->transferring = true;
                e->stage = EPOCH_CATCHUP;
                return true;
            }
        } else if (stable_configuration(v)) {
            e->stage = EPOCH_CATCHUP;
            return true;
        }
    }
    if (e->stage == EPOCH_CATCHUP && locally_installed(v)) {
        if (persist_phase(v, false))
            return true;
    }
    if (e->stage == EPOCH_INSTALL_WAIT && p->safe_sequence >= e->sequence) {
        p->epoch.phase = VSR_EPOCH_INSTALLED;
        e->installed = true;
        e->stage = EPOCH_INSTALLED;
        e->promise = p->self != VSR_INDEX_NONE;
        e->announce = true;
        e->cursor = 0;
        if (p->self != VSR_INDEX_NONE) {
            e->started[p->self] = 1;
            p->replay = false;
            vsr_protocol_normal(v);
        }
        vsr_changed(v);
        return true;
    }
    if (e->stage == EPOCH_INSTALLED && (quorum_ready(v) || e->successor) &&
        stable_configuration(v)) {
        if (persist_phase(v, true))
            return true;
    }
    if (e->stage == EPOCH_STEADY_WAIT && p->safe_sequence >= e->sequence) {
        p->epoch.phase = VSR_EPOCH_STEADY;
        uint32_t role = member_role(&p->current, v->options.replica);
        if (role == VSR_MEMBER_NONE)
            v->status.state =
                learner(v) ? VSR_STATE_WARMING : VSR_STATE_RETIRED;
        else {
            v->status.role = role;
            if (role == VSR_MEMBER_WITNESS)
                v->status.applied = 0;
        }
        p->replay = false;
        e->stage = EPOCH_IDLE;
        e->retry_at = VSR_NO_DEADLINE;
        if (e->guard) {
            vsr_checkpoint_guard(v, false);
            e->guard = false;
        }
        vsr_changed(v);
        return true;
    }
    return send_announcements(v);
}

void vsr_epochs_complete(struct vsr *v, struct vsr_operation *operation,
                         const struct vsr_event *event, uint32_t lease)
{
    (void)lease;
    struct vsr_epochs *e = epochs(v);
    if ((uint32_t)operation->tag == EPOCH_SEND) {
        if (e->sends != 0)
            e->sends--;
        if (event->status != VSR_IO_OK && e->stage != EPOCH_IDLE) {
            e->announce = true;
            e->promise =
                e->installed && vsr_protocol(v)->self != VSR_INDEX_NONE;
            e->cursor = 0;
        }
    }
}

uint64_t vsr_epochs_deadline(const struct vsr *v)
{
    /* Only a handoff in progress retransmits; the timer is re-armed at
     * every TIME event that finds it expired. */
    const struct vsr_epochs *e = epochs_const(v);
    return e->stage == EPOCH_IDLE ? VSR_NO_DEADLINE : e->retry_at;
}

void vsr_epochs_stop(struct vsr *v)
{
    struct vsr_epochs *e = epochs(v);
    e->stage = EPOCH_IDLE;
    e->retry_at = VSR_NO_DEADLINE;
    e->pending = NULL;
    e->learner = 0;
}
