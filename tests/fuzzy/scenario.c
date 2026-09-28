#include "config.h"

#include "fuzzy/scenario.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <inttypes.h>
#include <stdio.h>
#include <string.h>

enum {
    MAX_NODES = 9, /* mem_options reserves nine members per configuration. */
    CLIENTS = 8,
    READS = 256,
    EPOCHS = 16,
    COMMAND_CLIENT = 100,
    ADMIN_CLIENT = 200, /* RECONFIGURE requests. */
    CHECK_CLIENT = 201, /* CHECK_EPOCH requests. */
    HEAL_CLIENT = 999
};

struct read_record {
    uint64_t cookie;
    uint64_t min_op;
    uint64_t floor; /* Committed position at admission. */
    uint32_t node;
    uint32_t consistency;
    bool active;
};

/* Full members of an observed epoch, in ID order: primary = full[view % n]. */
struct epoch_record {
    uint64_t epoch;
    uint64_t full[MAX_NODES];
    uint32_t full_count;
    bool used;
};

struct group {
    uint64_t epoch;
    struct vsr_member members[MAX_NODES];
    uint32_t count;
    uint32_t faults;
};

struct simulation {
    struct mem_cluster *cluster;
    struct mem_node *nodes[MAX_NODES];
    const struct scenario_source *source;
    struct vsr_options base; /* Limits shared by every node, seed unset. */
    uint64_t seed;
    uint64_t now;
    uint64_t incarnation[MAX_NODES];
    uint64_t requests[CLIENTS];
    size_t replies[MAX_NODES];
    size_t reads_seen[MAX_NODES];
    uint64_t floor[MAX_NODES];     /* Commitment a restart must not lose. */
    uint64_t committed[MAX_NODES]; /* Last observation in this incarnation. */
    bool unrecovered[MAX_NODES];   /* Restarted, recovery not yet finished. */
    bool was_member[MAX_NODES];
    uint32_t role[MAX_NODES]; /* Genesis or join role. */
    struct group known;       /* Latest membership observed at any node. */
    struct group proposed;
    struct read_record reads[READS];
    struct epoch_record epochs[EPOCHS];
    uint64_t admin_number;
    uint64_t check_number;
    uint64_t check_target;
    uint64_t route;
    uint32_t count;
    uint32_t step;
    uint32_t profile;
    uint32_t durability;
    uint32_t partition;
    uint32_t faults_left; /* Remaining injected I/O faults. */
    int crashed;          /* Profiles without HARSH: the one crashed node. */
    bool proposing;
    bool checking;
    bool trace;
    bool healing;
};

static char header[256];

const char *scenario_header(void)
{
    return header;
}

static uint32_t choose(struct simulation *s, uint32_t limit)
{
    uint32_t value;
    CHECK(limit != 0);
    value = s->source->choose(s->source->context, limit);
    CHECK(value < limit);
    return value;
}

static bool flag(const struct simulation *s, uint32_t bit)
{
    return (s->profile & bit) != 0;
}

static void record(const struct simulation *s, const char *kind, uint64_t a,
                   uint64_t b)
{
    if (s->trace)
        fprintf(stderr, "%" PRIu64 " %u %s %" PRIu64 " %" PRIu64 "\n", s->seed,
                s->step, kind, a, b);
}

static struct vsr_status state(struct mem_node *node)
{
    struct vsr_status status;
    vsr_get_status(mem_node_core(node), &status);
    return status;
}

static bool alive(const struct simulation *s, uint32_t index)
{
    return mem_node_alive(s->nodes[index]);
}

static void time_event(struct mem_node *node, uint64_t now)
{
    /* Pending output may consume an entire work quantum before TIME admission. */
    for (unsigned attempt = 0; attempt < 10000; ++attempt) {
        struct mem_step result = mem_node_time(node, now);
        CHECK(result.result == VSR_OK || result.result == VSR_AGAIN);
        if (result.consumed == 1)
            return;
    }
    CHECK(false);
}

/* Membership bookkeeping. */

static const struct vsr_member *group_member(const struct group *group,
                                             uint64_t id)
{
    for (uint32_t i = 0; i < group->count; ++i)
        if (group->members[i].id == id)
            return &group->members[i];
    return NULL;
}

static bool removed(const struct simulation *s, uint32_t index)
{
    return s->was_member[index] && group_member(&s->known, index + 1) == NULL;
}

static bool learner(const struct simulation *s, uint32_t index)
{
    return !s->was_member[index];
}

static void adopt(struct simulation *s, const struct vsr_membership *membership)
{
    if (membership->epoch <= s->known.epoch && s->known.count != 0)
        return;
    CHECK(membership->count <= MAX_NODES);
    s->known.epoch = membership->epoch;
    s->known.count = membership->count;
    s->known.faults = membership->faults;
    for (uint32_t i = 0; i < membership->count; ++i) {
        s->known.members[i] = membership->members[i];
        CHECK(membership->members[i].id >= 1 &&
              membership->members[i].id <= s->count);
        s->was_member[membership->members[i].id - 1] = true;
    }
}

static void remember_epoch(struct simulation *s,
                           const struct vsr_membership *membership)
{
    struct epoch_record *slot = NULL;
    for (uint32_t i = 0; i < EPOCHS; ++i) {
        if (s->epochs[i].used && s->epochs[i].epoch == membership->epoch)
            return;
        if (slot == NULL || !s->epochs[i].used ||
            (slot->used && s->epochs[i].epoch < slot->epoch))
            slot = &s->epochs[i];
    }
    slot->used = true;
    slot->epoch = membership->epoch;
    slot->full_count = 0;
    for (uint32_t i = 0; i < membership->count; ++i)
        if (membership->members[i].role == VSR_MEMBER_FULL)
            slot->full[slot->full_count++] = membership->members[i].id;
}

static const struct epoch_record *epoch_lookup(const struct simulation *s,
                                               uint64_t epoch)
{
    for (uint32_t i = 0; i < EPOCHS; ++i)
        if (s->epochs[i].used && s->epochs[i].epoch == epoch)
            return &s->epochs[i];
    return NULL;
}

/* Learn memberships and recovery progress from every live node. */
static void observe(struct simulation *s)
{
    for (uint32_t i = 0; i < s->count; ++i) {
        struct vsr_status current;
        if (!alive(s, i))
            continue;
        current = state(s->nodes[i]);
        if (current.configuration != NULL) {
            remember_epoch(s, current.configuration->current);
            if (current.configuration->previous != NULL)
                remember_epoch(s, current.configuration->previous);
            adopt(s, current.configuration->current);
        }
        if (s->unrecovered[i]) {
            if (current.state == VSR_STATE_STARTING ||
                current.state == VSR_STATE_RECOVERING ||
                current.state == VSR_STATE_WARMING)
                continue;
            s->unrecovered[i] = false;
            /* A durable restart restores the persisted commitment; quorum
             * recovery restores at least what f + 1 other members knew
             * committed at the crash (see quorum_known). */
            if (group_member(&s->known, i + 1) != NULL &&
                current.state != VSR_STATE_RETIRED)
                CHECK(current.committed >= s->floor[i]);
            s->committed[i] = current.committed;
        }
        CHECK(current.committed >= s->committed[i]);
        s->committed[i] = current.committed;
    }
}

/* Reads. */

static struct read_record *read_find(struct simulation *s, uint64_t cookie)
{
    for (uint32_t i = 0; i < READS; ++i)
        if (s->reads[i].active && s->reads[i].cookie == cookie)
            return &s->reads[i];
    return NULL;
}

static void issue_read(struct simulation *s, uint32_t index,
                       uint32_t consistency, uint64_t min_op)
{
    struct mem_node *node = s->nodes[index];
    struct vsr_status current = state(node);
    const struct vsr_read_barrier read = {min_op, s->now + 30, consistency, 0};
    const struct vsr_event input = {VSR_EVENT_READ, 0, ++s->route, &read, 1};
    struct read_record *slot = &s->reads[0];
    struct mem_step result;
    for (uint32_t i = 0; i < READS; ++i) {
        if (!s->reads[i].active) {
            slot = &s->reads[i];
            break;
        }
        if (s->reads[i].cookie < slot->cookie)
            slot = &s->reads[i];
    }
    record(s, "read", index, s->route);
    result = mem_node_event(node, &input);
    CHECK(result.result == VSR_OK || result.result == VSR_AGAIN);
    if (result.consumed == 0)
        return;
    *slot = (struct read_record){s->route, min_op,      current.committed,
                                 index,    consistency, true};
}

static void check_fence(struct simulation *s, uint32_t index,
                        const struct vsr_read_fence *fence)
{
    struct read_record *read = read_find(s, fence->cookie);
    if (read == NULL)
        return;
    CHECK(read->node == index);
    CHECK(fence->applied >= read->min_op);
    if (read->consistency == VSR_READ_LINEARIZABLE) {
        const struct epoch_record *epoch = epoch_lookup(s, fence->epoch);
        struct vsr_status current = state(s->nodes[index]);
        CHECK(fence->applied >= read->floor);
        /* A linearizable fence is granted only by the primary of the view
         * that confirmed it, whatever happened to leadership afterwards. */
        if (current.epoch == fence->epoch && current.view == fence->view)
            CHECK(current.primary == index + 1);
        if (epoch != NULL) {
            CHECK(epoch->full_count != 0);
            CHECK(epoch->full[fence->view % epoch->full_count] == index + 1);
        }
    }
    read->active = false;
}

/* Replies of the administrative clients. */

static void admin_reply(struct simulation *s, const struct vsr_reply *reply)
{
    if (!s->proposing || reply->request.number != s->admin_number)
        return;
    record(s, "reconfigure_reply", reply->status, reply->primary);
    switch (reply->status) {
    case VSR_REPLY_OK: {
        const struct vsr_membership membership = {
            s->proposed.epoch, s->proposed.members, s->proposed.count,
            s->proposed.faults};
        record(s, "reconfigured", s->proposed.epoch, reply->op);
        adopt(s, &membership);
        s->proposing = false;
        break;
    }
    case VSR_REPLY_INVALID:
    case VSR_REPLY_LIMIT:
    case VSR_REPLY_STALE_REQUEST:
        record(s, "rejected", s->proposed.epoch, reply->status);
        s->proposing = false;
        break;
    case VSR_REPLY_NEW_EPOCH:
        if (reply->membership != NULL)
            adopt(s, reply->membership);
        break;
    default:
        break;
    }
}

static void check_reply(struct simulation *s, const struct vsr_reply *reply)
{
    if (!s->checking || reply->request.number != s->check_number)
        return;
    record(s, "check_epoch_reply", reply->status, reply->primary);
    switch (reply->status) {
    case VSR_REPLY_OK:
        CHECK(reply->flags == VSR_REPLY_EXECUTED);
        CHECK(reply->result.data.size == 0 && reply->result.code == 0);
        /* Success certifies handoff into the target: nobody alive can still
         * be steady in an earlier epoch as a current member. */
        for (uint32_t i = 0; i < s->count; ++i) {
            struct vsr_status current;
            if (!alive(s, i) || group_member(&s->known, i + 1) == NULL)
                continue;
            current = state(s->nodes[i]);
            if (current.state == VSR_STATE_NORMAL &&
                current.configuration != NULL &&
                current.configuration->phase == VSR_EPOCH_STEADY)
                CHECK(current.epoch >= s->check_target);
        }
        record(s, "certified", s->check_target, reply->op);
        s->checking = false;
        break;
    case VSR_REPLY_INVALID:
    case VSR_REPLY_LIMIT:
    case VSR_REPLY_STALE_REQUEST:
        record(s, "uncertified", s->check_target, reply->status);
        s->checking = false;
        break;
    case VSR_REPLY_NEW_EPOCH:
        if (reply->membership != NULL)
            adopt(s, reply->membership);
        break;
    default:
        break;
    }
}

/* Safety checks after every action. */

static void check(struct simulation *s)
{
    mem_cluster_check(s->cluster);
    for (uint32_t i = 0; i < s->count; ++i) {
        if (alive(s, i)) {
            const struct vsr_status current = state(s->nodes[i]);
            if (current.failure.code != VSR_FAILURE_NONE)
                fprintf(stderr,
                        "seed=%" PRIu64
                        " step=%u node=%u failure=%u effect=%u id=%" PRIu64
                        " status=%d view=%" PRIu64 " commit=%" PRIu64
                        " apply=%" PRIu64 "\n",
                        s->seed, s->step, i + 1, current.failure.code,
                        current.failure.operation_type,
                        current.failure.operation, current.failure.status,
                        current.view, current.committed, current.applied);
            CHECK(current.failure.code == VSR_FAILURE_NONE);
        }
        for (; s->replies[i] < mem_node_replies(s->nodes[i]); ++s->replies[i]) {
            uint64_t route;
            const struct vsr_reply *reply =
                mem_node_reply(s->nodes[i], s->replies[i], &route);
            if (reply->request.client.hi == ADMIN_CLIENT) {
                admin_reply(s, reply);
                continue;
            }
            if (reply->request.client.hi == CHECK_CLIENT) {
                check_reply(s, reply);
                continue;
            }
            if (reply->request.client.hi == 0 &&
                reply->request.client.lo == 0) {
                struct read_record *read = read_find(s, route);
                if (read != NULL)
                    read->active = false;
                continue;
            }
            if (reply->status != VSR_REPLY_OK ||
                reply->flags != VSR_REPLY_EXECUTED ||
                reply->request.client.hi != COMMAND_CLIENT ||
                reply->request.client.lo >= CLIENTS)
                continue;
            uint32_t client = (uint32_t)reply->request.client.lo;
            CHECK(reply->request.number <= s->requests[client]);
            if (reply->request.number == s->requests[client])
                ++s->requests[client];
        }
        for (; s->reads_seen[i] < mem_node_reads(s->nodes[i]);
             ++s->reads_seen[i])
            check_fence(s, i, mem_node_read(s->nodes[i], s->reads_seen[i]));
    }
    /* Two NORMAL members of the same epoch and view agree on the primary. */
    for (uint32_t i = 0; i < s->count; ++i) {
        struct vsr_status a;
        if (!alive(s, i))
            continue;
        a = state(s->nodes[i]);
        if (a.state != VSR_STATE_NORMAL)
            continue;
        for (uint32_t j = i + 1; j < s->count; ++j) {
            struct vsr_status b;
            if (!alive(s, j))
                continue;
            b = state(s->nodes[j]);
            if (b.state == VSR_STATE_NORMAL && b.epoch == a.epoch &&
                b.view == a.view)
                CHECK(a.primary == b.primary);
        }
    }
    observe(s);
}

/* Actions. */

static void submit(struct simulation *s, uint32_t index, uint32_t client)
{
    unsigned char bytes[32];
    for (unsigned i = 0; i < sizeof(bytes); ++i)
        bytes[i] = (unsigned char)((s->requests[client] + client + i) & 255u);
    uint32_t split = 1 + choose(s, 31);
    struct vsr_span spans[2] = {{bytes, split},
                                {bytes + split, sizeof(bytes) - split}};
    struct vsr_blob body = {spans, sizeof(bytes), 2, 0};
    struct vsr_request request = {
        {{COMMAND_CLIENT, client}, s->requests[client]},
        0,
        VSR_REQUEST_COMMAND,
        0,
        &body};
    struct vsr_event event = {VSR_EVENT_REQUEST, 0, ++s->route, &request, 1};
    record(s, "request", index, client);
    struct mem_step result = mem_node_event(s->nodes[index], &event);
    CHECK(result.result == VSR_OK || result.result == VSR_AGAIN);
}

static void restart(struct simulation *s, uint32_t index)
{
    ++s->incarnation[index];
    CHECK(mem_node_restart(s->nodes[index],
                           (struct vsr_id){index + 1, s->incarnation[index]}) ==
          VSR_OK);
    s->unrecovered[index] = true;
    s->committed[index] = 0;
    time_event(s->nodes[index], s->now);
    s->crashed = -1;
    record(s, "restart", index, s->incarnation[index]);
}

/* Commitment a replicated-mode restart of this member must not lose. Nothing
 * local survives such a restart: quorum recovery installs the highest-view
 * primary's history, whose commitment is the maximum a view-change or recovery
 * quorum knows (protocol.md "Preparation and view change", "Restart and
 * persistence"). A position only this member had learned committed (a primary
 * commits first; a backup learns from a COMMIT the others may not have
 * received) can lie outside every later quorum: the entries survive in the
 * selected history and are re-proposed and re-committed in the next view, but
 * the counter restarts below the pre-crash value. Every quorum intersects any
 * f + 1 members, so the floor is the highest position that at least f + 1
 * other members of the known group, alive and recovered, report committed.
 * mem_cluster_check separately keeps the committed entries of every
 * incarnation and compares them across nodes. */
static uint64_t quorum_known(struct simulation *s, uint32_t index)
{
    uint64_t values[MAX_NODES];
    uint32_t count = 0;
    uint32_t needed = s->known.faults + 1;
    for (uint32_t i = 0; i < s->count; ++i) {
        if (i == index || !alive(s, i) || s->unrecovered[i] ||
            group_member(&s->known, i + 1) == NULL)
            continue;
        values[count++] = state(s->nodes[i]).committed;
    }
    if (count < needed)
        return 0;
    /* Sort descending; the needed-th highest is known by needed members. */
    for (uint32_t i = 1; i < count; ++i)
        for (uint32_t j = i; j > 0 && values[j - 1] < values[j]; --j) {
            uint64_t swap = values[j - 1];
            values[j - 1] = values[j];
            values[j] = swap;
        }
    return values[needed - 1];
}

static void crash(struct simulation *s, uint32_t index)
{
    struct mem_node *node = s->nodes[index];
    uint64_t floor = 0;
    if (s->durability == VSR_DURABLE) {
        struct mem_store *store = mem_node_store(node);
        const struct vsr_recovered *durable =
            mem_store_recovered(store, mem_store_durable(store));
        if (durable != NULL)
            floor = durable->hard.committed;
    } else if (!s->unrecovered[index]) {
        floor = quorum_known(s, index);
    }
    if (floor > s->floor[index])
        s->floor[index] = floor;
    mem_node_crash(node);
    s->unrecovered[index] = true;
    record(s, "crash", index, 0);
}

/* At most f members of every configuration that may still need them can be
 * crashed or restarted-but-unrecovered at once. Successive restarts do not
 * reset that budget: only finished recovery does. */
static bool crash_allowed(struct simulation *s, uint32_t index)
{
    bool relevant[MAX_NODES] = {false};
    uint32_t faults = UINT32_MAX;
    uint32_t unavailable = 0;
    const struct group *groups[2] = {&s->known, &s->proposed};
    for (uint32_t g = 0; g < (s->proposing ? 2u : 1u); ++g) {
        for (uint32_t i = 0; i < groups[g]->count; ++i)
            relevant[groups[g]->members[i].id - 1] = true;
        if (groups[g]->faults < faults)
            faults = groups[g]->faults;
    }
    for (uint32_t i = 0; i < s->count; ++i) {
        struct vsr_status current;
        const struct vsr_membership *memberships[2];
        if (!alive(s, i))
            continue;
        current = state(s->nodes[i]);
        if (current.configuration == NULL)
            continue;
        memberships[0] = current.configuration->current;
        memberships[1] = current.configuration->phase == VSR_EPOCH_STEADY
                             ? NULL
                             : current.configuration->previous;
        for (uint32_t m = 0; m < 2; ++m) {
            if (memberships[m] == NULL)
                continue;
            for (uint32_t j = 0; j < memberships[m]->count; ++j) {
                CHECK(memberships[m]->members[j].id >= 1 &&
                      memberships[m]->members[j].id <= s->count);
                relevant[memberships[m]->members[j].id - 1] = true;
            }
            if (memberships[m]->faults < faults)
                faults = memberships[m]->faults;
        }
    }
    if (!relevant[index] || s->unrecovered[index])
        return true;
    for (uint32_t i = 0; i < s->count; ++i)
        if (relevant[i] && (!alive(s, i) || s->unrecovered[i]))
            ++unavailable;
    return unavailable < faults;
}

static bool restartable(const struct simulation *s, uint32_t index)
{
    /* A removed member that lost its replicated state has no group left to
     * recover from; the model treats it as stopped. */
    return s->durability == VSR_DURABLE || !removed(s, index);
}

static void harsh(struct simulation *s, uint32_t index)
{
    uint32_t down[MAX_NODES];
    uint32_t count = 0;
    for (uint32_t i = 0; i < s->count; ++i)
        if (!alive(s, i) && restartable(s, i))
            down[count++] = i;
    if (count != 0 && choose(s, 2) == 0) {
        restart(s, down[choose(s, count)]);
        return;
    }
    if (alive(s, index) && crash_allowed(s, index))
        crash(s, index);
}

static void read_before(struct simulation *s, uint32_t index)
{
    uint32_t consistency = choose(s, 2);
    uint32_t target = index;
    if (consistency == VSR_READ_LINEARIZABLE) {
        for (uint32_t i = 0; i < s->count; ++i) {
            if (!alive(s, i))
                continue;
            struct vsr_status current = state(s->nodes[i]);
            if (current.state == VSR_STATE_NORMAL && current.primary == i + 1) {
                target = i;
                break;
            }
        }
    }
    if (!alive(s, target))
        return;
    issue_read(s, target, consistency,
               choose(s, 2) == 0 ? 0 : state(s->nodes[target]).committed);
}

static bool retryable(uint32_t type)
{
    return type == VSR_OP_LOAD || type == VSR_OP_SNAPSHOT_CAPTURE ||
           type == VSR_OP_SNAPSHOT_FETCH || type == VSR_OP_SNAPSHOT_DROP ||
           type == VSR_OP_RECLAIM;
}

static void spawn_learner(struct simulation *s)
{
    const struct vsr_membership seed = {s->known.epoch, s->known.members,
                                        s->known.count, s->known.faults};
    struct vsr_options options = s->base;
    uint32_t index = s->count;
    uint32_t role;
    if (index >= MAX_NODES)
        return;
    role = choose(s, 2) == 0 ? VSR_MEMBER_FULL : VSR_MEMBER_WITNESS;
    options.replica = index + 1;
    options.incarnation = (struct vsr_id){index + 1, 1};
    options.seed = &seed;
    options.start_mode = VSR_START_JOIN;
    options.join_role = role;
    options.limits.client_cache_entries = 1 + choose(s, 4);
    options.limits.work_per_step = 1 + choose(s, 16);
    s->nodes[index] = mem_cluster_add(s->cluster, &options);
    s->incarnation[index] = 1;
    s->role[index] = role;
    s->count = index + 1;
    mem_node_output_capacity(s->nodes[index], 1 + choose(s, 4));
    time_event(s->nodes[index], s->now);
    record(s, "learner", index, role);
}

static bool warmed(struct simulation *s, uint32_t index)
{
    struct vsr_status current;
    if (!alive(s, index) || !learner(s, index))
        return false;
    current = state(s->nodes[index]);
    return current.state == VSR_STATE_WARMING &&
           current.configuration != NULL &&
           current.configuration->phase == VSR_EPOCH_STEADY;
}

static void group_insert(struct group *group, struct vsr_member member)
{
    uint32_t i = group->count;
    CHECK(i < MAX_NODES);
    while (i > 0 && group->members[i - 1].id > member.id) {
        group->members[i] = group->members[i - 1];
        --i;
    }
    group->members[i] = member;
    ++group->count;
}

static void group_remove(struct group *group, uint32_t index)
{
    for (uint32_t i = index + 1; i < group->count; ++i)
        group->members[i - 1] = group->members[i];
    --group->count;
}

static uint32_t group_full(const struct group *group)
{
    uint32_t full = 0;
    for (uint32_t i = 0; i < group->count; ++i)
        full += group->members[i].role == VSR_MEMBER_FULL ? 1 : 0;
    return full;
}

/* Keep n >= 2f + 1 and at least f + 1 full members; false if no full member. */
static bool group_valid(struct group *group)
{
    uint32_t full = group_full(group);
    if (group->count == 0 || full == 0)
        return false;
    if (group->faults > (group->count - 1) / 2)
        group->faults = (group->count - 1) / 2;
    if (group->faults > full - 1)
        group->faults = full - 1;
    return true;
}

/* A membership is only proposed if it tolerates the members of it that are
 * already unavailable. A member without recoverable state when the boundary
 * commits (crashed, or restarted under replicated durability) rejoins the new
 * epoch only through quorum recovery there, which needs q = n - f responses
 * from that epoch's other members, and handoff readiness needs q promises
 * (protocol.md "Authority and quorums", "Epoch handoff and witnesses"). With
 * more such members than the new f, neither quorum can ever form: the
 * configuration is born beyond its fault tolerance, an administrative error
 * rather than a liveness bug. crash_allowed bounds later crashes by the
 * proposed group as well, so this check closes the only gap. */
static bool group_tolerates_unavailable(const struct simulation *s,
                                        const struct group *group)
{
    uint32_t unavailable = 0;
    for (uint32_t i = 0; i < group->count; ++i) {
        uint32_t index = (uint32_t)group->members[i].id - 1;
        if (!alive(s, index) || s->unrecovered[index])
            ++unavailable;
    }
    return unavailable <= group->faults;
}

static void send_reconfigure(struct simulation *s, uint32_t index)
{
    const struct vsr_membership membership = {
        s->proposed.epoch, s->proposed.members, s->proposed.count,
        s->proposed.faults};
    const struct vsr_request request = {{{ADMIN_CLIENT, 1}, s->admin_number},
                                        s->known.epoch,
                                        VSR_REQUEST_RECONFIGURE,
                                        0,
                                        &membership};
    const struct vsr_event event = {VSR_EVENT_REQUEST, 0, ++s->route, &request,
                                    1};
    struct mem_step result;
    record(s, "reconfigure", index, s->proposed.epoch);
    result = mem_node_event(s->nodes[index], &event);
    CHECK(result.result == VSR_OK || result.result == VSR_AGAIN);
}

static void propose(struct simulation *s, uint32_t index)
{
    struct group group = s->known;
    uint32_t warm[MAX_NODES];
    uint32_t warm_count = 0;
    uint32_t kind = choose(s, 8);
    for (uint32_t i = 0; i < s->count; ++i)
        if (warmed(s, i))
            warm[warm_count++] = i;
    group.epoch = s->known.epoch + 1;
    if ((kind == 0 || kind == 1 || kind == 5) && warm_count == 0)
        kind = 4;
    switch (kind) {
    case 0:
    case 1: {
        /* Admit one warmed learner in its warm-up role. */
        uint32_t pick = warm[choose(s, warm_count)];
        if (group.count >= MAX_NODES)
            return;
        group_insert(&group, (struct vsr_member){pick + 1, s->role[pick], 0});
        if (choose(s, 2) == 0)
            group.faults = (group.count - 1) / 2;
        break;
    }
    case 2:
        if (group.count == 1)
            return;
        group_remove(&group, choose(s, group.count));
        break;
    case 3: {
        struct vsr_member *member = &group.members[choose(s, group.count)];
        member->role = member->role == VSR_MEMBER_FULL ? VSR_MEMBER_WITNESS
                                                       : VSR_MEMBER_FULL;
        break;
    }
    case 4:
        group.faults = choose(s, (group.count + 1) / 2);
        break;
    case 5:
        /* A fully disjoint successor group made of warmed learners. */
        group.count = 0;
        for (uint32_t i = 0; i < warm_count; ++i)
            group_insert(&group,
                         (struct vsr_member){warm[i] + 1, s->role[warm[i]], 0});
        group.faults = (group.count - 1) / 2;
        break;
    default:
        /* Same members, next epoch: a pure epoch bump. */
        break;
    }
    if (!group_valid(&group) || !group_tolerates_unavailable(s, &group))
        return;
    s->proposed = group;
    s->proposing = true;
    ++s->admin_number;
    send_reconfigure(s, index);
}

static void send_check_epoch(struct simulation *s, uint32_t index)
{
    const struct vsr_check_epoch check_epoch = {s->check_target};
    const struct vsr_request request = {{{CHECK_CLIENT, 1}, s->check_number},
                                        s->known.epoch,
                                        VSR_REQUEST_CHECK_EPOCH,
                                        0,
                                        &check_epoch};
    const struct vsr_event event = {VSR_EVENT_REQUEST, 0, ++s->route, &request,
                                    1};
    struct mem_step result;
    record(s, "check_epoch", index, s->check_target);
    result = mem_node_event(s->nodes[index], &event);
    CHECK(result.result == VSR_OK || result.result == VSR_AGAIN);
}

static void membership(struct simulation *s)
{
    uint32_t kind = choose(s, 8);
    uint32_t index = choose(s, s->count);
    if (kind < 2) {
        spawn_learner(s);
        return;
    }
    if (!alive(s, index))
        return;
    if (kind < 4) {
        /* A new fence, or the same one again through a redirect. */
        if (!s->checking) {
            CHECK(s->known.epoch < UINT32_MAX);
            s->check_target = choose(s, (uint32_t)s->known.epoch + 1);
            ++s->check_number;
            s->checking = true;
        }
        send_check_epoch(s, index);
        return;
    }
    if (s->proposing)
        send_reconfigure(s, index);
    else
        propose(s, index);
}

static void event(struct simulation *s)
{
    if (flag(s, SCENARIO_MEMBERSHIP) && choose(s, 8) == 0) {
        membership(s);
        return;
    }
    uint32_t action = choose(s, 24);
    uint32_t index = choose(s, s->count);
    struct mem_node *node = s->nodes[index];
    if (action == 23) {
        if (flag(s, SCENARIO_READS))
            read_before(s, index);
        if (flag(s, SCENARIO_HARSH)) {
            harsh(s, index);
            return;
        }
        if (s->crashed >= 0)
            restart(s, (uint32_t)s->crashed);
        else {
            /* In replicated mode an un-recovered restart still counts as a
             * failed member. Sequential crashes must not remove the recovery
             * quorum merely because all processes have been restarted. */
            for (uint32_t i = 0; i < s->count; ++i)
                if (!alive(s, i) ||
                    state(s->nodes[i]).state != VSR_STATE_NORMAL)
                    return;
            crash(s, index);
            s->crashed = (int)index;
        }
        return;
    }
    if (action >= 12 && action < 17) {
        size_t count = mem_cluster_messages(s->cluster);
        if (count == 0)
            return;
        CHECK(count <= UINT32_MAX);
        size_t message = choose(s, (uint32_t)count);
        if (action == 15) {
            record(s, "drop", message, 0);
            mem_cluster_drop(s->cluster, message);
        } else if (action == 16 && count < 256) {
            record(s, "duplicate", message, 0);
            mem_cluster_duplicate(s->cluster, message);
        } else {
            record(s, "deliver", message, 0);
            uint64_t to;
            const struct vsr_message *wire =
                mem_cluster_message(s->cluster, message, &to);
            CHECK(wire->from >= 1 && wire->from <= s->count && to >= 1 &&
                  to <= s->count);
            bool source_side =
                (s->partition & (UINT32_C(1) << (wire->from - 1))) != 0;
            bool target_side = (s->partition & (UINT32_C(1) << (to - 1))) != 0;
            if (source_side != target_side)
                mem_cluster_drop(s->cluster, message);
            else
                (void)mem_cluster_deliver(s->cluster, message);
        }
        return;
    }
    if (!mem_node_alive(node))
        return;
    if (action < 12) {
        size_t count = mem_node_effects(node);
        if (count != 0) {
            CHECK(count <= UINT32_MAX);
            size_t effect = choose(s, (uint32_t)count);
            const struct vsr_op *op = mem_node_effect(node, effect);
            int result = op->type == VSR_OP_SEND && choose(s, 32) == 0
                             ? VSR_IO_RETRY
                             : VSR_IO_OK;
            if (flag(s, SCENARIO_IO_FAULTS) && !s->healing &&
                s->faults_left != 0 && retryable(op->type) &&
                choose(s, 8) == 0) {
                /* Only LOAD absence would be unexpected; the others may
                 * retry or rediscover after both transient statuses. */
                result = op->type != VSR_OP_LOAD && choose(s, 2) == 0
                             ? VSR_IO_NOT_FOUND
                             : VSR_IO_RETRY;
                --s->faults_left;
                record(s, "fault", op->id, (uint64_t)result);
            }
            record(s, action < 3 ? "execute" : "complete", index, op->id);
            if (action < 3)
                (void)mem_node_execute(node, effect, result);
            else
                (void)mem_node_complete(node, effect, result);
        } else {
            record(s, "drain", index, 0);
            (void)mem_node_event(node, NULL);
        }
    } else if (action == 17 || action == 18) {
        submit(s, index, choose(s, CLIENTS));
    } else if (action == 19) {
        if (flag(s, SCENARIO_READS))
            read_before(s, index);
        s->now += choose(s, 8);
        record(s, "time", index, s->now);
        time_event(node, s->now);
    } else if (action == 20) {
        const struct vsr_event hint = {VSR_EVENT_CHECKPOINT, 0, 0, NULL, 0};
        record(s, "checkpoint", index, 0);
        (void)mem_node_event(node, &hint);
    } else if (action == 21) {
        issue_read(s, index, choose(s, 2), state(node).committed);
    } else {
        if (flag(s, SCENARIO_PARTITION)) {
            if (flag(s, SCENARIO_READS))
                read_before(s, index);
            s->partition = s->partition == 0
                               ? 1 + choose(s, (UINT32_C(1) << s->count) - 2)
                               : 0;
            record(s, "partition", s->partition, 0);
        }
        record(s, "drain", index, 0);
        (void)mem_node_event(node, NULL);
    }
}

/* Liveness after faults cease. */

/* A learner rediscovers the group through the members it knows. Once every
 * one of them has retired or stopped, nothing can redirect it to the current
 * epoch: RETIRED is terminal and answers no discovery. */
static bool learner_reachable(struct simulation *s, uint32_t index)
{
    struct vsr_status current = state(s->nodes[index]);
    if (current.configuration == NULL)
        return false;
    for (uint32_t i = 0; i < current.configuration->current->count; ++i) {
        uint64_t id = current.configuration->current->members[i].id;
        uint32_t peer = (uint32_t)(id - 1);
        if (id == 0 || id > s->count || !alive(s, peer))
            continue;
        if (state(s->nodes[peer]).state != VSR_STATE_RETIRED)
            return true;
    }
    return false;
}

static bool converged(struct simulation *s, uint64_t target)
{
    for (uint32_t i = 0; i < s->count; ++i) {
        const struct vsr_member *member = group_member(&s->known, i + 1);
        struct vsr_status current;
        if (!alive(s, i)) {
            if (removed(s, i))
                continue;
            return false;
        }
        current = state(s->nodes[i]);
        if (member != NULL) {
            if (current.state != VSR_STATE_NORMAL ||
                current.epoch != s->known.epoch ||
                current.configuration == NULL ||
                current.configuration->phase != VSR_EPOCH_STEADY ||
                current.committed < target)
                return false;
            if (member->role == VSR_MEMBER_FULL && current.applied < target)
                return false;
        } else if (removed(s, i)) {
            if (current.state != VSR_STATE_RETIRED)
                return false;
        } else if (current.state != VSR_STATE_WARMING) {
            return false;
        } else if (learner_reachable(s, i) &&
                   (current.epoch != s->known.epoch ||
                    current.configuration == NULL ||
                    current.configuration->phase != VSR_EPOCH_STEADY ||
                    current.committed < target)) {
            /* Warm-up is continuous: a learner that can still reach a live
             * member follows the group into the current epoch and keeps up
             * with what it commits. */
            return false;
        }
    }
    return true;
}

static void heal(struct simulation *s)
{
    uint64_t committed = 0;
    s->healing = true;
    s->partition = 0;
    for (uint32_t i = 0; i < s->count; ++i)
        if (!alive(s, i) && restartable(s, i))
            restart(s, i);
    /* A stable network, available members, and fair effect completions must
     * eventually admit and execute a new request on every full replica of the
     * current membership; removed members retire and reachable learners warm
     * up to that request in the current epoch. */
    for (unsigned round = 0; round < 1000; ++round) {
        s->now += 5;
        record(s, "heal", round, s->now);
        for (uint32_t i = 0; i < s->count; ++i)
            if (alive(s, i))
                time_event(s->nodes[i], s->now);
        CHECK(mem_cluster_run(s->cluster, 100000) < 100000);
        observe(s);
        for (uint32_t i = 0; i < s->count; ++i) {
            struct vsr_status current;
            if (!alive(s, i))
                continue;
            current = state(s->nodes[i]);
            if (committed == 0 && current.state == VSR_STATE_NORMAL &&
                current.primary == i + 1 && current.epoch == s->known.epoch &&
                group_member(&s->known, i + 1) != NULL) {
                const struct vsr_blob body = {0};
                const struct vsr_request request = {{{HEAL_CLIENT, 1}, 1},
                                                    s->known.epoch,
                                                    VSR_REQUEST_COMMAND,
                                                    0,
                                                    &body};
                const struct vsr_event input = {VSR_EVENT_REQUEST, 0,
                                                ++s->route, &request, 1};
                struct mem_step result = mem_node_event(s->nodes[i], &input);
                CHECK(result.result == VSR_OK || result.result == VSR_AGAIN);
            }
            for (size_t j = 0; j < mem_node_replies(s->nodes[i]); ++j) {
                const struct vsr_reply *reply =
                    mem_node_reply(s->nodes[i], j, NULL);
                if (reply->request.client.hi == HEAL_CLIENT &&
                    reply->status == VSR_REPLY_OK)
                    committed = reply->op;
            }
        }
        check(s);
        if (committed != 0 && converged(s, committed))
            return;
    }
    for (uint32_t i = 0; i < s->count; ++i) {
        struct vsr_status current;
        if (!alive(s, i)) {
            fprintf(stderr, "node %u stopped\n", i + 1);
            continue;
        }
        current = state(s->nodes[i]);
        fprintf(stderr,
                "node %u state=%u epoch=%" PRIu64 " view=%" PRIu64
                " commit=%" PRIu64 " apply=%" PRIu64 " role=%u phase=%u\n",
                i + 1, current.state, current.epoch, current.view,
                current.committed, current.applied, current.role,
                current.configuration == NULL ? 0
                                              : current.configuration->phase);
    }
    fprintf(stderr,
            "membership epoch=%" PRIu64 " count=%u faults=%u target=%" PRIu64
            "\n",
            s->known.epoch, s->known.count, s->known.faults, committed);
    for (uint32_t i = 0; i < s->known.count; ++i)
        fprintf(stderr, "member %" PRIu64 " role=%u\n", s->known.members[i].id,
                s->known.members[i].role);
    CHECK(false);
}

static void warm(struct simulation *s)
{
    for (uint32_t i = 0; i < 16; ++i) {
        uint32_t client = i % CLIENTS;
        uint64_t number_before = s->requests[client];
        for (unsigned attempt = 0;
             attempt < 1000 && s->requests[client] == number_before;
             ++attempt) {
            submit(s, 0, client);
            CHECK(mem_cluster_run(s->cluster, 100000) < 100000);
            check(s);
        }
        CHECK(s->requests[client] > number_before);
    }
    s->now = 10;
    for (uint32_t i = 0; i < s->count; ++i)
        time_event(s->nodes[i], s->now);
    CHECK(mem_cluster_run(s->cluster, 100000) < 100000);
    if ((s->seed & 1u) != 0) {
        for (uint32_t i = 0; i < s->count; ++i) {
            const struct vsr_event hint = {VSR_EVENT_CHECKPOINT, 0, 0, NULL, 0};
            (void)mem_node_event(s->nodes[i], &hint);
        }
        CHECK(mem_cluster_run(s->cluster, 100000) < 100000);
    }
    check(s);
}

void scenario_run(const struct scenario_options *options,
                  const struct scenario_source *source)
{
    struct simulation s = {.cluster = mem_cluster_create(),
                           .source = source,
                           .seed = options->seed,
                           .crashed = -1,
                           .profile = options->profile,
                           .trace = options->trace};
    CHECK(options->profile <= SCENARIO_PROFILE_MAX);
    s.count = 3 + choose(&s, 3);
    uint32_t faults = s.count == 5 ? 2 : 1;
    bool witness = choose(&s, 2) != 0;
    s.durability = choose(&s, 2);
    uint32_t operations = UINT32_C(1) << choose(&s, 5);
    uint32_t cache = UINT32_C(1) << choose(&s, 4);
    s.faults_left = options->steps / 4;
    s.known.epoch = 0;
    s.known.count = s.count;
    s.known.faults = faults;
    for (uint32_t i = 0; i < s.count; ++i) {
        s.known.members[i] = (struct vsr_member){
            i + 1,
            witness && i + 1 == s.count ? VSR_MEMBER_WITNESS : VSR_MEMBER_FULL,
            0};
        s.role[i] = s.known.members[i].role;
        s.was_member[i] = true;
    }
    const struct vsr_membership membership = {0, s.known.members, s.count,
                                              faults};
    remember_epoch(&s, &membership);
    snprintf(header, sizeof(header),
             "seed=%" PRIu64 " steps=%u nodes=%u f=%u witness=%u durability=%u "
             "operations=%u cache=%u profile=%u",
             options->seed, options->steps, s.count, faults, (unsigned)witness,
             s.durability, operations, cache, options->profile);
    if (!options->quiet)
        fprintf(stderr, "%s\n", header);
    s.base = mem_options(1, NULL);
    s.base.durability = s.durability;
    s.base.limits.operations = operations;
    s.base.limits.log_cache_entries = cache;
    s.base.limits.batch_entries = cache;
    if (flag(&s, SCENARIO_MINIMAL)) {
        s.base.limits.transfers = 1;
        s.base.limits.input_leases = 9;
        s.base.limits.command_bytes = 32;
        s.base.limits.result_bytes = 18;
        s.base.limits.manifest_bytes = 8;
        s.base.limits.message_bytes = 40;
        s.base.limits.pinned_payload_bytes = 154;
    }
    for (uint32_t i = 0; i < s.count; ++i) {
        struct vsr_options node_options = s.base;
        node_options.replica = i + 1;
        node_options.incarnation = (struct vsr_id){i + 1, 1};
        node_options.seed = &membership;
        node_options.limits.client_cache_entries = 1 + choose(&s, 4);
        node_options.limits.work_per_step = 1 + choose(&s, 16);
        s.nodes[i] = mem_cluster_add(s.cluster, &node_options);
        s.incarnation[i] = 1;
        mem_node_output_capacity(s.nodes[i], 1 + choose(&s, 4));
        time_event(s.nodes[i], 0);
    }
    for (uint32_t i = 0; i < CLIENTS; ++i)
        s.requests[i] = 1;
    CHECK(mem_cluster_run(s.cluster, 100000) < 100000);
    if (flag(&s, SCENARIO_WARM))
        warm(&s);
    for (s.step = 0; s.step < options->steps; ++s.step) {
        event(&s);
        check(&s);
    }
    heal(&s);
    mem_cluster_destroy(s.cluster);
}
