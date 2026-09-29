#include "config.h"

#include "vsr-client.h"

#include "checked.h"
#include "io/crc32c.h"
#include "objects.h"
#include "validate.h"

#include <stdalign.h>
#include <stdbool.h>
#include <string.h>

/*
 * Sans-IO client bookkeeping. The arena holds struct vsr_client, then the
 * lane array, then the membership copy; nothing else is retained, except the
 * caller-owned request bodies of lanes with a request in flight.
 *
 * Topology knowledge is (epoch, view, primary, membership). A newer epoch
 * always replaces it. Within one epoch a reply's (view, primary) replaces an
 * older view, and a hint (vsr_client_learn, the seed) only fills a primary
 * that no reply has reported yet in that epoch ("witnessed").
 */

struct client_lane {
    struct vsr_id incarnation;
    uint64_t next_number;
    uint64_t pending_number; /* Zero when IDLE. */
    uint64_t deadline;       /* PENDING/WAITING only; else NO_DEADLINE. */
    uint64_t replica;        /* Last target; kept across requests. */
    uint64_t epoch;          /* Routing epoch of the last attempt. */
    const void *body;        /* NULL when IDLE or DETACHED. */
    uint32_t type;
    uint32_t state;
    uint32_t attempts;
    uint32_t busy_streak;
    uint32_t stale; /* 1 after FAILED with STALE_REQUEST: begin refused
                       until the lane is closed (decision 64). */
};

struct vsr_client {
    struct vsr_client_options options; /* seed is NULL after init. */
    size_t arena_size;
    struct client_lane *lanes;
    struct vsr_member *members;
    struct vsr_membership membership; /* Valid iff known. */
    uint64_t epoch;
    uint64_t view;
    uint64_t primary;
    uint64_t min_op;
    uint64_t read_cursor; /* ID of the last causal read target. */
    uint32_t open;
    bool known;
    bool witnessed;
};

struct client_plan {
    size_t size;
    size_t alignment;
    size_t lanes;
    size_t members;
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

static bool add_slice(struct client_plan *plan, size_t count, size_t size,
                      size_t alignment, size_t *offset)
{
    size_t bytes;
    return aligned_size(plan->size, alignment, offset) &&
           vsr_size_mul(count, size, &bytes) &&
           vsr_size_add(*offset, bytes, &plan->size);
}

/* Structural membership check against a capacity. arena, when non-NULL,
 * must not overlap the input. */
static int check_membership(const void *arena, size_t arena_size,
                            uint32_t capacity,
                            const struct vsr_membership *membership)
{
    struct vsr_limits limits;
    memset(&limits, 0, sizeof(limits));
    limits.members = capacity;
    struct vsr_validation validation = {&limits, arena, arena_size, 0, 0};
    return vsr_validate_membership(&validation, membership);
}

static int plan_arena(const struct vsr_client_options *options,
                      struct client_plan *plan)
{
    memset(plan, 0, sizeof(*plan));
    if (options->lanes == 0 || options->members == 0 ||
        options->request_timeout_ns == 0 ||
        options->request_timeout_ns == VSR_NO_DEADLINE ||
        options->backoff_ns == 0 ||
        options->backoff_max_ns < options->backoff_ns ||
        options->backoff_max_ns == VSR_NO_DEADLINE || options->reserved != 0) {
        return VSR_EINVAL;
    }
    plan->alignment =
        options->cache_line_bytes == 0 ? 64 : options->cache_line_bytes;
    if (!power_of_two(plan->alignment)) {
        return VSR_EINVAL;
    }
    if (plan->alignment < alignof(max_align_t)) {
        plan->alignment = alignof(max_align_t);
    }
    if (options->seed != NULL) {
        int result = check_membership(NULL, 0, options->members, options->seed);
        if (result != VSR_OK) {
            return result;
        }
    }
    plan->size = sizeof(struct vsr_client);
    if (!add_slice(plan, options->lanes, sizeof(struct client_lane),
                   plan->alignment, &plan->lanes) ||
        !add_slice(plan, options->members, sizeof(struct vsr_member),
                   alignof(struct vsr_member), &plan->members) ||
        !aligned_size(plan->size, plan->alignment, &plan->size)) {
        return VSR_ELIMIT;
    }
    return VSR_OK;
}

int vsr_client_layout(const struct vsr_client_options *options,
                      struct vsr_layout *layout)
{
    if (layout == NULL) {
        return VSR_EINVAL;
    }
    *layout = (struct vsr_layout){0, 0};
    if (options == NULL) {
        return VSR_EINVAL;
    }
    struct client_plan plan;
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

static void copy_membership(struct vsr_client *client,
                            const struct vsr_membership *membership)
{
    memcpy(client->members, membership->members,
           (size_t)membership->count * sizeof(struct vsr_member));
    client->membership = *membership;
    client->membership.members = client->members;
    client->known = true;
}

int vsr_client_init(void *memory, size_t size,
                    const struct vsr_client_options *options,
                    struct vsr_client **out)
{
    if (out == NULL) {
        return VSR_EINVAL;
    }
    if (memory == NULL || options == NULL) {
        *out = NULL;
        return VSR_EINVAL;
    }
    if (overlaps(memory, size, out, sizeof(struct vsr_client *))) {
        return VSR_EINVAL;
    }
    *out = NULL;
    if (overlaps(memory, size, options, sizeof(*options))) {
        return VSR_EINVAL;
    }
    struct client_plan plan;
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
    if (options->seed != NULL &&
        check_membership(memory, size, options->members, options->seed) !=
            VSR_OK) {
        return VSR_EINVAL; /* The seed overlaps the arena. */
    }
    const struct vsr_membership *seed = options->seed;
    memset(memory, 0, plan.size);
    struct vsr_client *client = memory;
    unsigned char *base = memory;
    client->options = *options;
    client->options.seed = NULL;
    client->arena_size = size;
    client->lanes = (void *)(base + plan.lanes);
    client->members = (void *)(base + plan.members);
    client->primary = VSR_NO_REPLICA;
    for (uint32_t i = 0; i < options->lanes; i++) {
        client->lanes[i].state = VSR_CLIENT_LANE_CLOSED;
        client->lanes[i].deadline = VSR_NO_DEADLINE;
    }
    if (seed != NULL) {
        copy_membership(client, seed);
        client->epoch = seed->epoch;
    }
    *out = client;
    return VSR_OK;
}

/* -------------------------------------------------------------------------
 * Lanes
 * ---------------------------------------------------------------------- */

static struct client_lane *open_lane(struct vsr_client *client, uint32_t lane)
{
    if (client == NULL || lane >= client->options.lanes ||
        client->lanes[lane].state == VSR_CLIENT_LANE_CLOSED) {
        return NULL;
    }
    return &client->lanes[lane];
}

static bool incarnation_open(const struct vsr_client *client,
                             struct vsr_id incarnation)
{
    for (uint32_t i = 0; i < client->options.lanes; i++) {
        const struct client_lane *lane = &client->lanes[i];
        if (lane->state != VSR_CLIENT_LANE_CLOSED &&
            vsr_id_equal(lane->incarnation, incarnation)) {
            return true;
        }
    }
    return false;
}

int vsr_client_open(struct vsr_client *client, struct vsr_id incarnation,
                    uint64_t next_number, uint32_t *lane)
{
    if (client == NULL || lane == NULL || !vsr_id_present(incarnation) ||
        next_number == 0 || next_number == UINT64_MAX ||
        incarnation_open(client, incarnation)) {
        return VSR_EINVAL;
    }
    for (uint32_t i = 0; i < client->options.lanes; i++) {
        struct client_lane *slot = &client->lanes[i];
        if (slot->state == VSR_CLIENT_LANE_CLOSED) {
            memset(slot, 0, sizeof(*slot));
            slot->incarnation = incarnation;
            slot->next_number = next_number;
            slot->deadline = VSR_NO_DEADLINE;
            slot->state = VSR_CLIENT_LANE_IDLE;
            client->open++;
            *lane = i;
            return VSR_OK;
        }
    }
    return VSR_ELIMIT;
}

int vsr_client_close(struct vsr_client *client, uint32_t lane)
{
    struct client_lane *slot = open_lane(client, lane);
    if (slot == NULL) {
        return VSR_EINVAL;
    }
    if (slot->state != VSR_CLIENT_LANE_IDLE) {
        return VSR_EBUSY;
    }
    memset(slot, 0, sizeof(*slot));
    slot->state = VSR_CLIENT_LANE_CLOSED;
    slot->deadline = VSR_NO_DEADLINE;
    client->open--;
    return VSR_OK;
}

void vsr_client_lane_status(const struct vsr_client *client, uint32_t lane,
                            struct vsr_client_lane_status *status)
{
    if (status == NULL) {
        return;
    }
    memset(status, 0, sizeof(*status));
    status->deadline_ns = VSR_NO_DEADLINE;
    status->replica = VSR_NO_REPLICA;
    status->state = VSR_CLIENT_LANE_CLOSED;
    if (client == NULL || lane >= client->options.lanes) {
        return;
    }
    const struct client_lane *slot = &client->lanes[lane];
    if (slot->state == VSR_CLIENT_LANE_CLOSED) {
        return;
    }
    status->incarnation = slot->incarnation;
    status->next_number = slot->next_number;
    status->pending_number = slot->pending_number;
    status->deadline_ns = slot->deadline;
    status->state = slot->state;
    status->attempts = slot->attempts;
    status->busy_streak = slot->busy_streak;
    if (slot->state == VSR_CLIENT_LANE_PENDING ||
        slot->state == VSR_CLIENT_LANE_WAITING ||
        slot->state == VSR_CLIENT_LANE_QUERYING) {
        status->replica = slot->replica;
    }
}

/* -------------------------------------------------------------------------
 * Routing and attempts
 * ---------------------------------------------------------------------- */

/* The first member after `after` in ID order, wrapping; NO_REPLICA when no
 * membership (or no FULL member, for full_only) is known. */
static uint64_t next_member(const struct vsr_client *client, uint64_t after,
                            bool full_only)
{
    uint64_t first = VSR_NO_REPLICA;
    if (!client->known) {
        return VSR_NO_REPLICA;
    }
    for (uint32_t i = 0; i < client->membership.count; i++) {
        const struct vsr_member *member = &client->membership.members[i];
        if (full_only && member->role != VSR_MEMBER_FULL) {
            continue;
        }
        if (first == VSR_NO_REPLICA) {
            first = member->id;
        }
        if (member->id > after) {
            return member->id;
        }
    }
    return first;
}

static uint64_t route(const struct vsr_client *client,
                      const struct client_lane *lane)
{
    return client->primary != VSR_NO_REPLICA
               ? client->primary
               : next_member(client, lane->replica, false);
}

/* Absolute deadline, saturated below VSR_NO_DEADLINE. */
static uint64_t after(uint64_t now, uint64_t delay)
{
    return now >= VSR_NO_DEADLINE - 1 - delay ? VSR_NO_DEADLINE - 1
                                              : now + delay;
}

static uint64_t backoff(const struct vsr_client *client, uint32_t streak)
{
    uint64_t delay = client->options.backoff_ns;
    uint64_t cap = client->options.backoff_max_ns;
    for (uint32_t i = 1; i < streak && delay < cap; i++) {
        delay = delay > cap / 2 ? cap : delay * 2;
    }
    return delay < cap ? delay : cap;
}

static void ignore(struct vsr_client_outcome *outcome, uint32_t lane)
{
    memset(outcome, 0, sizeof(*outcome));
    outcome->action = VSR_CLIENT_IGNORE;
    outcome->lane = lane;
    outcome->deadline_ns = VSR_NO_DEADLINE;
    outcome->attempt.replica = VSR_NO_REPLICA;
    outcome->attempt.deadline_ns = VSR_NO_DEADLINE;
    outcome->attempt.lane = lane;
}

/* Starts the next attempt of the lane's pending request at its route. */
static void attempt(struct vsr_client *client, uint32_t index, uint64_t now,
                    struct vsr_client_attempt *out)
{
    struct client_lane *lane = &client->lanes[index];
    lane->replica = route(client, lane);
    lane->epoch = client->epoch;
    lane->state = VSR_CLIENT_LANE_PENDING;
    lane->deadline = after(now, client->options.request_timeout_ns);
    if (lane->attempts < UINT32_MAX) {
        lane->attempts++;
    }
    memset(out, 0, sizeof(*out));
    out->request.id.client = lane->incarnation;
    out->request.id.number = lane->pending_number;
    out->request.epoch = lane->epoch;
    out->request.type = lane->type;
    out->request.body = lane->body;
    out->replica = lane->replica;
    out->deadline_ns = lane->deadline;
    out->lane = index;
    out->attempt = lane->attempts;
}

static void retry(struct vsr_client *client, uint32_t index, uint64_t now,
                  struct vsr_client_outcome *outcome)
{
    ignore(outcome, index);
    outcome->action = VSR_CLIENT_RETRY;
    attempt(client, index, now, &outcome->attempt);
}

static void back_off(struct vsr_client *client, uint32_t index, uint64_t delay,
                     uint64_t now, struct vsr_client_outcome *outcome)
{
    struct client_lane *lane = &client->lanes[index];
    lane->state = VSR_CLIENT_LANE_WAITING;
    lane->deadline = after(now, delay);
    ignore(outcome, index);
    outcome->action = VSR_CLIENT_WAIT;
    outcome->deadline_ns = lane->deadline;
}

/* Ends the lane's request with a terminal action. */
static void finish(struct vsr_client *client, uint32_t index, uint32_t action,
                   uint32_t status, struct vsr_client_outcome *outcome)
{
    struct client_lane *lane = &client->lanes[index];
    lane->state = VSR_CLIENT_LANE_IDLE;
    lane->pending_number = 0;
    lane->deadline = VSR_NO_DEADLINE;
    lane->body = NULL;
    lane->type = 0;
    lane->attempts = 0;
    lane->busy_streak = 0;
    if (action == VSR_CLIENT_FAILED && status == VSR_REPLY_STALE_REQUEST) {
        lane->stale = 1;
    }
    ignore(outcome, index);
    outcome->action = action;
    outcome->status = status;
}

static void raise_min_op(struct vsr_client *client, uint64_t op)
{
    if (op > client->min_op && op != UINT64_MAX) {
        client->min_op = op;
    }
}

/* Validates a body as vsr_validate_request would, without admission limits. */
static int check_body(const struct vsr_client *client,
                      const struct client_lane *lane, uint32_t type,
                      const void *body)
{
    struct vsr_limits limits;
    memset(&limits, 0, sizeof(limits));
    limits.members = UINT32_MAX;
    limits.spans_per_blob = UINT32_MAX;
    limits.command_bytes = UINT64_MAX;
    struct vsr_validation validation = {&limits, client, client->arena_size, 0,
                                        0};
    struct vsr_request request = {{lane->incarnation, 1}, 0, type, 0, body};
    if (type == VSR_REQUEST_NOOP) {
        return VSR_EINVAL;
    }
    return vsr_validate_request(&validation, &request) == VSR_OK ? VSR_OK
                                                                 : VSR_EINVAL;
}

int vsr_client_begin(struct vsr_client *client, uint32_t lane, uint32_t type,
                     const void *body, uint64_t now_ns,
                     struct vsr_client_attempt *attempt_out)
{
    struct client_lane *slot = open_lane(client, lane);
    if (slot == NULL || attempt_out == NULL || now_ns == VSR_NO_DEADLINE) {
        return VSR_EINVAL;
    }
    if (slot->state != VSR_CLIENT_LANE_IDLE) {
        return VSR_EBUSY;
    }
    if (slot->stale != 0) {
        return VSR_EINVAL; /* Outcome unknown: close and open a fresh one. */
    }
    if (check_body(client, slot, type, body) != VSR_OK) {
        return VSR_EINVAL;
    }
    if (slot->next_number == UINT64_MAX) {
        return VSR_ELIMIT; /* UINT64_MAX - 1 was the last request number. */
    }
    slot->pending_number = slot->next_number++;
    slot->type = type;
    slot->body = body;
    slot->attempts = 0;
    slot->busy_streak = 0;
    attempt(client, lane, now_ns, attempt_out);
    return VSR_OK;
}

/* -------------------------------------------------------------------------
 * Replies
 * ---------------------------------------------------------------------- */

struct topology {
    uint64_t epoch;
    uint64_t view;
    uint64_t primary;
    bool witnessed;
    bool copy; /* Copy the reply's membership. */
};

/* What adopting a reply's topology would yield, without mutating. */
static struct topology decide(const struct vsr_client *client,
                              const struct vsr_reply *reply, bool fits)
{
    struct topology next = {client->epoch, client->view, client->primary,
                            client->witnessed, false};
    const struct vsr_membership *membership = reply->membership;
    uint64_t epoch = membership != NULL ? membership->epoch : client->epoch;
    if (epoch < client->epoch) {
        return next;
    }
    next.copy = membership != NULL && fits;
    if (epoch > client->epoch || !client->witnessed ||
        reply->view > client->view ||
        (reply->view == client->view && reply->primary != VSR_NO_REPLICA)) {
        next.epoch = epoch;
        next.view = reply->view;
        next.primary = reply->primary;
        next.witnessed = true;
    }
    return next;
}

static void apply(struct vsr_client *client, const struct vsr_reply *reply,
                  const struct topology *next)
{
    if (next->copy) {
        copy_membership(client, reply->membership);
    }
    client->epoch = next->epoch;
    client->view = next->view;
    client->primary = next->primary;
    client->witnessed = next->witnessed;
}

/* OK, or EINVAL/ELIMIT for the reply's shape; ELIMIT means the membership
 * exceeds capacity and is otherwise well formed. */
static int check_reply(const struct vsr_client *client,
                       const struct vsr_reply *reply)
{
    if (reply->status > VSR_REPLY_TIMEOUT ||
        (reply->flags & ~(uint32_t)VSR_REPLY_EXECUTED) != 0 ||
        reply->view == UINT64_MAX || reply->op == UINT64_MAX) {
        return VSR_EINVAL;
    }
    if (reply->membership == NULL) {
        return VSR_OK;
    }
    int result = check_membership(client, client->arena_size,
                                  client->options.members, reply->membership);
    if (result == VSR_ELIMIT) {
        /* Only the count exceeds capacity when every other check passes. */
        result = check_membership(client, client->arena_size, UINT32_MAX,
                                  reply->membership) == VSR_OK
                     ? VSR_ELIMIT
                     : VSR_EINVAL;
    }
    return result;
}

int vsr_client_reply(struct vsr_client *client, uint32_t lane,
                     const struct vsr_reply *reply, uint64_t now_ns,
                     struct vsr_client_outcome *outcome)
{
    if (outcome == NULL) {
        return VSR_EINVAL;
    }
    ignore(outcome, lane);
    struct client_lane *slot = open_lane(client, lane);
    if (slot == NULL || reply == NULL || now_ns == VSR_NO_DEADLINE) {
        return VSR_EINVAL;
    }
    int shape = check_reply(client, reply);
    if (shape == VSR_EINVAL || (reply->status == VSR_REPLY_OK &&
                                (reply->flags & VSR_REPLY_EXECUTED) == 0)) {
        return VSR_EINVAL;
    }
    bool pending = slot->state == VSR_CLIENT_LANE_PENDING;
    if ((!pending && slot->state != VSR_CLIENT_LANE_WAITING) ||
        !vsr_id_equal(reply->request.client, slot->incarnation) ||
        reply->request.number != slot->pending_number) {
        return VSR_OK;
    }
    struct topology next = decide(client, reply, shape == VSR_OK);
    switch (reply->status) {
    case VSR_REPLY_OK:
        apply(client, reply, &next);
        raise_min_op(client, reply->op);
        finish(client, lane, VSR_CLIENT_DONE, reply->status, outcome);
        break;
    case VSR_REPLY_INVALID:
    case VSR_REPLY_LIMIT:
    case VSR_REPLY_STALE_REQUEST:
        apply(client, reply, &next);
        finish(client, lane, VSR_CLIENT_FAILED, reply->status, outcome);
        break;
    case VSR_REPLY_NOT_PRIMARY:
        /* A redirect to where the attempt already went, in the same epoch, is
         * a duplicate or a primary that is not ready: its timeout covers it. */
        if (!pending ||
            (next.primary != VSR_NO_REPLICA && next.primary == slot->replica &&
             next.epoch == slot->epoch)) {
            return VSR_OK;
        }
        apply(client, reply, &next);
        slot->busy_streak = 0;
        if (client->primary == VSR_NO_REPLICA) {
            back_off(client, lane, client->options.backoff_ns, now_ns, outcome);
        } else {
            retry(client, lane, now_ns, outcome);
        }
        break;
    case VSR_REPLY_NEW_EPOCH:
        /* A replica in the attempt's routing epoch never answers it with
         * NEW_EPOCH: this one answered an earlier attempt, late or twice. */
        if (!pending || (reply->membership != NULL &&
                         reply->membership->epoch == slot->epoch)) {
            return VSR_OK;
        }
        apply(client, reply, &next);
        slot->busy_streak = 0;
        if (client->epoch != slot->epoch) {
            retry(client, lane, now_ns, outcome);
        } else {
            /* The replica lags behind the routing epoch: it cannot be the
             * primary there. Back off and try elsewhere. */
            if (slot->replica == client->primary) {
                client->primary = VSR_NO_REPLICA;
            }
            back_off(client, lane, client->options.backoff_ns, now_ns, outcome);
        }
        break;
    case VSR_REPLY_BUSY:
        if (!pending) {
            return VSR_OK;
        }
        apply(client, reply, &next);
        if (slot->busy_streak < UINT32_MAX) {
            slot->busy_streak++;
        }
        back_off(client, lane, backoff(client, slot->busy_streak), now_ns,
                 outcome);
        break;
    default: /* CLIENT_STATE, CLIENT_UNKNOWN, TIMEOUT. */
        return VSR_OK;
    }
    return shape;
}

/* -------------------------------------------------------------------------
 * Time
 * ---------------------------------------------------------------------- */

static bool timed(const struct client_lane *lane)
{
    return lane->state == VSR_CLIENT_LANE_PENDING ||
           lane->state == VSR_CLIENT_LANE_WAITING;
}

int vsr_client_time(struct vsr_client *client, uint64_t now_ns,
                    struct vsr_client_outcome *outcome)
{
    if (client == NULL || outcome == NULL || now_ns == VSR_NO_DEADLINE) {
        return VSR_EINVAL;
    }
    ignore(outcome, VSR_CLIENT_NO_LANE);
    uint32_t due = VSR_CLIENT_NO_LANE;
    for (uint32_t i = 0; i < client->options.lanes; i++) {
        const struct client_lane *lane = &client->lanes[i];
        if (timed(lane) && lane->deadline <= now_ns &&
            (due == VSR_CLIENT_NO_LANE ||
             lane->deadline < client->lanes[due].deadline)) {
            due = i;
        }
    }
    if (due == VSR_CLIENT_NO_LANE) {
        return 0;
    }
    struct client_lane *lane = &client->lanes[due];
    if (lane->state == VSR_CLIENT_LANE_PENDING &&
        lane->replica == client->primary) {
        /* The advertised primary did not answer in time: route around it
         * until a reply names the primary again. */
        client->primary = VSR_NO_REPLICA;
    }
    retry(client, due, now_ns, outcome);
    return 1;
}

uint64_t vsr_client_deadline(const struct vsr_client *client)
{
    uint64_t deadline = VSR_NO_DEADLINE;
    if (client == NULL) {
        return deadline;
    }
    for (uint32_t i = 0; i < client->options.lanes; i++) {
        const struct client_lane *lane = &client->lanes[i];
        if (timed(lane) && lane->deadline < deadline) {
            deadline = lane->deadline;
        }
    }
    return deadline;
}

/* -------------------------------------------------------------------------
 * Detached requests
 * ---------------------------------------------------------------------- */

static bool detached(const struct client_lane *lane)
{
    return lane->state == VSR_CLIENT_LANE_DETACHED ||
           lane->state == VSR_CLIENT_LANE_QUERYING;
}

int vsr_client_resume(struct vsr_client *client, uint32_t lane,
                      const void *body, uint64_t now_ns,
                      struct vsr_client_attempt *attempt_out)
{
    struct client_lane *slot = open_lane(client, lane);
    if (slot == NULL || attempt_out == NULL || now_ns == VSR_NO_DEADLINE ||
        !detached(slot) ||
        check_body(client, slot, slot->type, body) != VSR_OK) {
        return VSR_EINVAL;
    }
    slot->body = body;
    slot->busy_streak = 0;
    attempt(client, lane, now_ns, attempt_out);
    return VSR_OK;
}

int vsr_client_query(struct vsr_client *client, uint32_t lane,
                     struct vsr_id *incarnation, uint64_t *replica)
{
    struct client_lane *slot = open_lane(client, lane);
    if (slot == NULL || incarnation == NULL || replica == NULL ||
        !detached(slot)) {
        return VSR_EINVAL;
    }
    slot->replica = route(client, slot);
    slot->state = VSR_CLIENT_LANE_QUERYING;
    slot->deadline = VSR_NO_DEADLINE;
    *incarnation = slot->incarnation;
    *replica = slot->replica;
    return VSR_OK;
}

int vsr_client_queried(struct vsr_client *client, uint32_t lane,
                       const struct vsr_reply *reply, uint64_t now_ns,
                       struct vsr_client_outcome *outcome)
{
    if (outcome == NULL) {
        return VSR_EINVAL;
    }
    ignore(outcome, lane);
    struct client_lane *slot = open_lane(client, lane);
    if (slot == NULL || reply == NULL || now_ns == VSR_NO_DEADLINE ||
        check_reply(client, reply) == VSR_EINVAL) {
        return VSR_EINVAL;
    }
    if (!detached(slot) ||
        !vsr_id_equal(reply->request.client, slot->incarnation)) {
        return VSR_OK;
    }
    if (reply->status == VSR_REPLY_CLIENT_STATE &&
        reply->request.number == slot->pending_number &&
        (reply->flags & VSR_REPLY_EXECUTED) != 0) {
        raise_min_op(client, reply->op);
        finish(client, lane, VSR_CLIENT_DONE, reply->status, outcome);
    } else if (reply->status == VSR_REPLY_CLIENT_STATE &&
               reply->request.number > slot->pending_number) {
        finish(client, lane, VSR_CLIENT_FAILED, VSR_REPLY_STALE_REQUEST,
               outcome);
    } else {
        /* Local knowledge cannot prove the request never ran. */
        slot->state = VSR_CLIENT_LANE_DETACHED;
    }
    return VSR_OK;
}

/* -------------------------------------------------------------------------
 * Topology and reads
 * ---------------------------------------------------------------------- */

void vsr_client_get_status(const struct vsr_client *client,
                           struct vsr_client_status *status)
{
    if (status == NULL) {
        return;
    }
    memset(status, 0, sizeof(*status));
    status->primary = VSR_NO_REPLICA;
    if (client == NULL) {
        return;
    }
    status->epoch = client->epoch;
    status->primary = client->primary;
    status->min_op = client->min_op;
    status->lanes = client->open;
    for (uint32_t i = 0; i < client->options.lanes; i++) {
        uint32_t state = client->lanes[i].state;
        if (state != VSR_CLIENT_LANE_CLOSED && state != VSR_CLIENT_LANE_IDLE) {
            status->busy++;
        }
    }
    status->membership = client->known ? &client->membership : NULL;
}

static bool full_member(const struct vsr_membership *membership, uint64_t id)
{
    uint32_t index = vsr_member_index(membership, id);
    return index != UINT32_MAX &&
           membership->members[index].role == VSR_MEMBER_FULL;
}

int vsr_client_learn(struct vsr_client *client,
                     const struct vsr_membership *membership, uint64_t primary)
{
    if (client == NULL) {
        return VSR_EINVAL;
    }
    if (membership != NULL) {
        int result = check_membership(client, client->arena_size,
                                      client->options.members, membership);
        if (result != VSR_OK) {
            return result;
        }
        if (primary != VSR_NO_REPLICA && !full_member(membership, primary)) {
            return VSR_EINVAL;
        }
        if (membership->epoch < client->epoch) {
            return VSR_OK;
        }
        if (membership->epoch > client->epoch) {
            copy_membership(client, membership);
            client->epoch = membership->epoch;
            client->view = 0;
            client->primary = primary;
            client->witnessed = false;
            return VSR_OK;
        }
        copy_membership(client, membership);
    } else if (primary != VSR_NO_REPLICA && client->known &&
               !full_member(&client->membership, primary)) {
        return VSR_EINVAL;
    }
    if (primary != VSR_NO_REPLICA && !client->witnessed) {
        client->primary = primary;
    }
    return VSR_OK;
}

int vsr_client_read(struct vsr_client *client, uint32_t consistency,
                    uint64_t deadline_ns, struct vsr_read_barrier *barrier,
                    uint64_t *replica)
{
    if (client == NULL || barrier == NULL || replica == NULL ||
        (consistency != VSR_READ_LINEARIZABLE &&
         consistency != VSR_READ_CAUSAL)) {
        return VSR_EINVAL;
    }
    memset(barrier, 0, sizeof(*barrier));
    barrier->deadline_ns = deadline_ns;
    barrier->consistency = consistency;
    if (consistency == VSR_READ_LINEARIZABLE) {
        *replica = client->primary;
        return VSR_OK;
    }
    uint64_t target = next_member(client, client->read_cursor, true);
    if (target != VSR_NO_REPLICA) {
        client->read_cursor = target;
    }
    barrier->min_op = client->min_op;
    *replica = target;
    return VSR_OK;
}

void vsr_client_observe(struct vsr_client *client, uint64_t applied)
{
    if (client != NULL) {
        raise_min_op(client, applied);
    }
}

uint64_t vsr_client_min_op(const struct vsr_client *client)
{
    return client == NULL ? 0 : client->min_op;
}

/* -------------------------------------------------------------------------
 * Persistence
 *
 * Image layout, every integer little-endian, no padding:
 *
 *   header, 72 bytes
 *     0  u32 magic 0x43525356 ("VSRC")
 *     4  u32 VSR_CLIENT_STATE_VERSION
 *     8  u32 lane records
 *    12  u32 member records (zero when no membership is known)
 *    16  u64 image bytes, trailer included
 *    24  u64 routing epoch
 *    32  u64 view of the primary knowledge
 *    40  u64 primary, or VSR_NO_REPLICA
 *    48  u64 min_op
 *    56  u64 membership epoch
 *    64  u32 membership faults
 *    68  u32 flags: 1 membership known, 2 primary reported by a reply
 *   lane records, 48 bytes each, in increasing lane index
 *     0  u32 lane index
 *     4  u32 flags: 1 a request is pending
 *     8  u64 incarnation.hi, 16 u64 incarnation.lo
 *    24  u64 next_number (UINT64_MAX once the counter is exhausted)
 *    32  u64 pending_number (zero unless pending)
 *    40  u32 pending type (zero unless pending), 44 u32 attempts
 *   member records, 16 bytes each, in ID order
 *     0  u64 id, 8 u32 role, 12 u32 zero
 *   trailer: u32 vsr_io_crc32c (Castagnoli) of every preceding byte.
 *
 * Pending bodies are the caller's and are not included. A version other
 * than this one is rejected, as is any size, checksum or field mismatch.
 * ---------------------------------------------------------------------- */

#define IMAGE_MAGIC UINT32_C(0x43525356)
#define IMAGE_HEADER 72u
#define IMAGE_LANE 48u
#define IMAGE_MEMBER 16u
#define IMAGE_TRAILER 4u
#define IMAGE_KNOWN 1u
#define IMAGE_WITNESSED 2u
#define IMAGE_PENDING 1u

static void put32(unsigned char *p, uint32_t value)
{
    for (unsigned i = 0; i < 4; i++) {
        p[i] = (unsigned char)((value >> (8 * i)) & 0xffu);
    }
}

static void put64(unsigned char *p, uint64_t value)
{
    for (unsigned i = 0; i < 8; i++) {
        p[i] = (unsigned char)((value >> (8 * i)) & 0xffu);
    }
}

static uint32_t get32(const unsigned char *p)
{
    uint32_t value = 0;
    for (unsigned i = 0; i < 4; i++) {
        value |= (uint32_t)p[i] << (8 * i);
    }
    return value;
}

static uint64_t get64(const unsigned char *p)
{
    uint64_t value = 0;
    for (unsigned i = 0; i < 8; i++) {
        value |= (uint64_t)p[i] << (8 * i);
    }
    return value;
}

static bool image_size(uint32_t lanes, uint32_t members, size_t *size)
{
    size_t lane_bytes;
    size_t member_bytes;
    return vsr_size_mul(lanes, IMAGE_LANE, &lane_bytes) &&
           vsr_size_mul(members, IMAGE_MEMBER, &member_bytes) &&
           vsr_size_add(IMAGE_HEADER + IMAGE_TRAILER, lane_bytes, size) &&
           vsr_size_add(*size, member_bytes, size);
}

size_t vsr_client_export_size(const struct vsr_client *client)
{
    size_t size = 0;
    if (client == NULL ||
        !image_size(client->open, client->known ? client->membership.count : 0,
                    &size)) {
        return 0;
    }
    return size;
}

int vsr_client_export(const struct vsr_client *client, void *bytes, size_t size,
                      size_t *written)
{
    if (client == NULL || written == NULL) {
        return VSR_EINVAL;
    }
    size_t needed = vsr_client_export_size(client);
    *written = needed;
    if (needed == 0) {
        return VSR_ELIMIT;
    }
    if (size < needed) {
        return VSR_ELIMIT;
    }
    if (bytes == NULL || overlaps(bytes, size, client, client->arena_size)) {
        *written = 0;
        return VSR_EINVAL;
    }
    unsigned char *p = bytes;
    uint32_t members = client->known ? client->membership.count : 0;
    put32(p, IMAGE_MAGIC);
    put32(p + 4, VSR_CLIENT_STATE_VERSION);
    put32(p + 8, client->open);
    put32(p + 12, members);
    put64(p + 16, (uint64_t)needed);
    put64(p + 24, client->epoch);
    put64(p + 32, client->view);
    put64(p + 40, client->primary);
    put64(p + 48, client->min_op);
    put64(p + 56, client->known ? client->membership.epoch : 0);
    put32(p + 64, client->known ? client->membership.faults : 0);
    put32(p + 68, (client->known ? IMAGE_KNOWN : 0u) |
                      (client->witnessed ? IMAGE_WITNESSED : 0u));
    p += IMAGE_HEADER;
    for (uint32_t i = 0; i < client->options.lanes; i++) {
        const struct client_lane *lane = &client->lanes[i];
        if (lane->state == VSR_CLIENT_LANE_CLOSED) {
            continue;
        }
        bool pending = lane->state != VSR_CLIENT_LANE_IDLE;
        put32(p, i);
        put32(p + 4, pending ? IMAGE_PENDING : 0u);
        put64(p + 8, lane->incarnation.hi);
        put64(p + 16, lane->incarnation.lo);
        put64(p + 24, lane->next_number);
        put64(p + 32, lane->pending_number);
        put32(p + 40, pending ? lane->type : 0u);
        put32(p + 44, pending ? lane->attempts : 0u);
        p += IMAGE_LANE;
    }
    for (uint32_t i = 0; i < members; i++) {
        put64(p, client->membership.members[i].id);
        put32(p + 8, client->membership.members[i].role);
        put32(p + 12, 0);
        p += IMAGE_MEMBER;
    }
    put32(p, vsr_io_crc32c(0, bytes, needed - IMAGE_TRAILER));
    return VSR_OK;
}

struct image {
    const unsigned char *lanes;
    const unsigned char *members;
    uint32_t lane_count;
    uint32_t member_count;
    uint64_t epoch;
    uint64_t view;
    uint64_t primary;
    uint64_t min_op;
    uint64_t membership_epoch;
    uint32_t faults;
    uint32_t flags;
};

static int parse_header(const unsigned char *p, size_t size,
                        struct image *image)
{
    size_t expected;
    if (size < IMAGE_HEADER + IMAGE_TRAILER || get32(p) != IMAGE_MAGIC ||
        get32(p + 4) != VSR_CLIENT_STATE_VERSION) {
        return VSR_EINVAL;
    }
    image->lane_count = get32(p + 8);
    image->member_count = get32(p + 12);
    if (!image_size(image->lane_count, image->member_count, &expected) ||
        expected != size || get64(p + 16) != (uint64_t)size ||
        get32(p + size - IMAGE_TRAILER) !=
            vsr_io_crc32c(0, p, size - IMAGE_TRAILER)) {
        return VSR_EINVAL;
    }
    image->epoch = get64(p + 24);
    image->view = get64(p + 32);
    image->primary = get64(p + 40);
    image->min_op = get64(p + 48);
    image->membership_epoch = get64(p + 56);
    image->faults = get32(p + 64);
    image->flags = get32(p + 68);
    image->lanes = p + IMAGE_HEADER;
    image->members = image->lanes + (size_t)image->lane_count * IMAGE_LANE;
    return VSR_OK;
}

static int check_image_topology(const struct vsr_client *client,
                                const struct image *image)
{
    bool known = (image->flags & IMAGE_KNOWN) != 0;
    if ((image->flags & ~(IMAGE_KNOWN | IMAGE_WITNESSED)) != 0 ||
        image->epoch == UINT64_MAX || image->view == UINT64_MAX ||
        image->min_op == UINT64_MAX ||
        (!known && (image->member_count != 0 || image->membership_epoch != 0 ||
                    image->faults != 0)) ||
        (known &&
         (image->member_count == 0 || image->membership_epoch > image->epoch ||
          (uint64_t)image->faults * 2 + 1 > image->member_count))) {
        return VSR_EINVAL;
    }
    uint64_t previous = 0;
    uint32_t full = 0;
    for (uint32_t i = 0; i < image->member_count; i++) {
        const unsigned char *p = image->members + (size_t)i * IMAGE_MEMBER;
        uint64_t id = get64(p);
        uint32_t role = get32(p + 8);
        if (id <= previous || get32(p + 12) != 0 ||
            (role != VSR_MEMBER_FULL && role != VSR_MEMBER_WITNESS)) {
            return VSR_EINVAL;
        }
        previous = id;
        full += role == VSR_MEMBER_FULL ? 1u : 0u;
    }
    if (known && full <= image->faults) {
        return VSR_EINVAL;
    }
    return image->member_count > client->options.members ? VSR_ELIMIT : VSR_OK;
}

static int check_image_lanes(const struct vsr_client *client,
                             const struct image *image)
{
    int limit = VSR_OK;
    for (uint32_t i = 0; i < image->lane_count; i++) {
        const unsigned char *p = image->lanes + (size_t)i * IMAGE_LANE;
        uint32_t index = get32(p);
        uint32_t flags = get32(p + 4);
        struct vsr_id incarnation = {get64(p + 8), get64(p + 16)};
        uint64_t next = get64(p + 24);
        uint64_t pending = get64(p + 32);
        uint32_t type = get32(p + 40);
        uint32_t attempts = get32(p + 44);
        if ((i != 0 && index <= get32(p - IMAGE_LANE)) ||
            (flags & ~IMAGE_PENDING) != 0 || !vsr_id_present(incarnation) ||
            next == 0) {
            return VSR_EINVAL;
        }
        if (flags == IMAGE_PENDING
                ? pending == 0 || pending != next - 1 ||
                      (type != VSR_REQUEST_COMMAND &&
                       type != VSR_REQUEST_RECONFIGURE &&
                       type != VSR_REQUEST_CHECK_EPOCH)
                : pending != 0 || type != 0 || attempts != 0) {
            return VSR_EINVAL;
        }
        for (uint32_t j = 0; j < i; j++) {
            const unsigned char *q = image->lanes + (size_t)j * IMAGE_LANE;
            if (get64(q + 8) == incarnation.hi &&
                get64(q + 16) == incarnation.lo) {
                return VSR_EINVAL;
            }
        }
        if (index >= client->options.lanes) {
            limit = VSR_ELIMIT;
        }
    }
    return limit;
}

int vsr_client_import(struct vsr_client *client, const void *bytes, size_t size)
{
    if (client == NULL || bytes == NULL ||
        overlaps(bytes, size, client, client->arena_size)) {
        return VSR_EINVAL;
    }
    if (client->open != 0) {
        return VSR_EBUSY;
    }
    struct image image;
    int result = parse_header(bytes, size, &image);
    if (result != VSR_OK) {
        return result;
    }
    int topology = check_image_topology(client, &image);
    if (topology == VSR_EINVAL) {
        return VSR_EINVAL;
    }
    result = check_image_lanes(client, &image);
    if (result != VSR_OK) {
        return result;
    }
    if (topology != VSR_OK) {
        return topology;
    }
    for (uint32_t i = 0; i < image.lane_count; i++) {
        const unsigned char *p = image.lanes + (size_t)i * IMAGE_LANE;
        struct client_lane *lane = &client->lanes[get32(p)];
        memset(lane, 0, sizeof(*lane));
        lane->incarnation = (struct vsr_id){get64(p + 8), get64(p + 16)};
        lane->next_number = get64(p + 24);
        lane->pending_number = get64(p + 32);
        lane->type = get32(p + 40);
        lane->attempts = get32(p + 44);
        lane->deadline = VSR_NO_DEADLINE;
        lane->state = get32(p + 4) == IMAGE_PENDING ? VSR_CLIENT_LANE_DETACHED
                                                    : VSR_CLIENT_LANE_IDLE;
        client->open++;
    }
    /* Knowledge older than what the client already holds, such as a newer
     * seed, does not replace it. */
    if (image.epoch >= client->epoch) {
        if ((image.flags & IMAGE_KNOWN) != 0) {
            for (uint32_t i = 0; i < image.member_count; i++) {
                const unsigned char *p =
                    image.members + (size_t)i * IMAGE_MEMBER;
                client->members[i] =
                    (struct vsr_member){get64(p), get32(p + 8), 0};
            }
            client->membership =
                (struct vsr_membership){image.membership_epoch, client->members,
                                        image.member_count, image.faults};
            client->known = true;
        }
        client->epoch = image.epoch;
        client->view = image.view;
        client->primary = image.primary;
        client->witnessed = (image.flags & IMAGE_WITNESSED) != 0;
    }
    raise_min_op(client, image.min_op);
    return VSR_OK;
}
