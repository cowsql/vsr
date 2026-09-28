#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <stdint.h>

/* Regression for `tests/fuzzy/cluster 1572 1 600 quiet 6`.
 *
 * A primary with two operation slots, one work unit per step and the minimum
 * payload budget for maximum-size commands served two pairs of lagging peers
 * whose next entries were no longer cached, while one slot stayed busy with
 * an operation whose completion the host kept offering behind pending output
 * and whose pins consumed most of the budget. The poll loaded the next entry
 * for one pair and moved its cursor to that peer, but a poll that ran while
 * the load was outstanding rotated past the loading peer, found the other
 * pair's entry cached, failed to acquire the SEND (both slots busy), returned
 * with the cursor past that peer, and the pressure eviction released the
 * cached entry because the outstanding load's reservation left less than the
 * progress reserve. When the load completed the rotation resumed at the
 * partner of the evicted pair, which issued a load for the evicted entry
 * before the loaded one was ever sent, and the eviction then released the
 * loaded entry in turn: load, evict, reload, forever, and the node never
 * idled.
 *
 * The protocol now remembers the peer a log load was issued for and resumes
 * the rotation at that peer when the load completes, and a peer whose SEND
 * is blocked only by an operation slot keeps the cursor, so every completed
 * load is followed by a send. The scenario rebuilds the shape: a five-member
 * group whose primary holds one APPLY outstanding (a busy slot with a pinned
 * command and a reserved result), peers 2 and 3 lagging at entry 4 and peers
 * 4 and 5 lagging at entry 11 after the primary prepared 12 alone (the only
 * entry left in its cache), and a host that completes each effect only in a
 * turn after the one that emitted it. The group must idle and converge once
 * the messages flow. */

enum { NODES = 5, PRIMARY = 1 };

struct policy {
    uint64_t dropped;    /* Messages to these replicas are discarded. */
    uint64_t held_apply; /* APPLY effects of these replicas stay outstanding. */
};

struct group {
    struct mem_cluster *cluster;
    struct mem_node *nodes[NODES];
    uint64_t now;
};

static uint64_t bit(uint64_t replica)
{
    return UINT64_C(1) << replica;
}

/* The seeded profile 2 budgets for maximum-size commands with the failing
 * seed's two operation slots, four-entry cache and one work unit per step.
 * The pinned APPLY entry, one cached entry and one log load's reservation
 * leave less than the 40-byte progress reserve of the 154-byte budget. */
static struct vsr_options options(uint64_t replica,
                                  const struct vsr_membership *group)
{
    struct vsr_options o = mem_options(replica, group);
    struct vsr_limits *l = &o.limits;
    l->operations = 2;
    l->transfers = 1;
    l->input_leases = l->transfers + 8u;
    l->log_cache_entries = 4;
    l->batch_entries = 1;
    l->client_cache_entries = 1;
    l->command_bytes = 32;
    l->result_bytes = 18;
    l->manifest_bytes = 8;
    l->message_bytes = 40;
    l->pinned_payload_bytes = l->message_bytes + 2 * l->command_bytes +
                              (l->transfers + 3) * l->manifest_bytes +
                              l->result_bytes;
    l->work_per_step = 1;
    /* Peers cut off from the primary must not elect: the scenario keeps one
     * primary and drives its send rotation. */
    o.view_timeout_ns = 1000000;
    o.durability = VSR_REPLICATED;
    {
        struct vsr_layout layout;
        struct vsr_options insufficient = o;
        insufficient.limits.pinned_payload_bytes--;
        CHECK(vsr_layout(&insufficient, &layout) == VSR_ELIMIT);
        CHECK(vsr_layout(&o, &layout) == VSR_OK);
    }
    return o;
}

static struct vsr_status status(struct mem_node *node)
{
    struct vsr_status result;
    vsr_get_status(mem_node_core(node), &result);
    CHECK(result.failure.code == VSR_FAILURE_NONE);
    return result;
}

static struct mem_node *node(struct group *g, uint64_t replica)
{
    return g->nodes[replica - 1];
}

/* One scheduler turn in the seeded host's order: every node polls once, one
 * effect per node completes, then one deliverable message is offered. Only
 * effects that were outstanding before the node's poll may complete, as the
 * seeded host tried the same effects it had listed before the turn. */
static bool turn(struct group *g, const struct policy *policy)
{
    bool progress = false;
    for (uint32_t i = 0; i < NODES; i++) {
        struct mem_node *n = g->nodes[i];
        const size_t count = mem_node_effects(n);
        const struct mem_step step = mem_node_event(n, NULL);
        CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
        progress |= step.emitted != 0 || (step.flags & VSR_UPDATE_MORE) != 0;
        for (size_t j = 0; j < count; j++) {
            if ((policy->held_apply & bit(i + 1)) != 0 &&
                mem_node_effect(n, j)->type == VSR_OP_APPLY)
                continue;
            if (mem_node_complete(n, j, VSR_IO_OK)) {
                progress = true;
                break;
            }
        }
    }
    for (size_t i = 0; i < mem_cluster_messages(g->cluster); i++) {
        uint64_t to;
        (void)mem_cluster_message(g->cluster, i, &to);
        if ((policy->dropped & bit(to)) != 0) {
            mem_cluster_drop(g->cluster, i);
            progress = true;
            break;
        }
        /* A refused message stays queued; the next one is tried. */
        if (mem_cluster_deliver(g->cluster, i)) {
            progress = true;
            break;
        }
    }
    mem_cluster_check(g->cluster);
    return progress;
}

/* Runs until nothing is runnable; a configuration that never idles is a
 * livelock. Held effects do not count as progress. */
static void settle(struct group *g, const struct policy *policy)
{
    for (unsigned t = 0; t < 100000; t++)
        if (!turn(g, policy))
            return;
    CHECK(false);
}

static void tick(struct group *g, const struct policy *policy)
{
    settle(g, policy);
    g->now += 5;
    for (uint32_t i = 0; i < NODES; i++) {
        if ((policy->dropped & bit(i + 1)) != 0)
            continue;
        for (unsigned attempt = 0;; attempt++) {
            CHECK(attempt < 1000);
            if (mem_node_time(g->nodes[i], g->now).consumed == 1)
                break;
        }
    }
    settle(g, policy);
}

/* A proposal may be refused while the primary's own cache pins are released;
 * it is retried across turns like any host input. */
static void submit(struct group *g, const struct policy *policy,
                   uint64_t number)
{
    unsigned char bytes[32];
    for (unsigned i = 0; i < sizeof(bytes); i++)
        bytes[i] = (unsigned char)(number + i);
    const struct vsr_span span = {bytes, sizeof(bytes)};
    const struct vsr_blob body = {&span, sizeof(bytes), 1, 0};
    const struct vsr_request request = {
        {{7, number}, 1}, 0, VSR_REQUEST_COMMAND, 0, &body};
    const struct vsr_event event = {VSR_EVENT_REQUEST, 0, number, &request, 1};
    for (unsigned attempt = 0;; attempt++) {
        CHECK(attempt < 1000);
        if (mem_node_event(node(g, PRIMARY), &event).consumed == 1)
            return;
        (void)turn(g, policy);
    }
}

static bool converged(struct group *g, const struct policy *policy,
                      uint64_t committed)
{
    for (uint32_t i = 0; i < NODES; i++) {
        const struct vsr_status current = status(g->nodes[i]);
        if ((policy->dropped & bit(i + 1)) != 0)
            continue;
        if (current.state != VSR_STATE_NORMAL ||
            current.committed < committed ||
            ((policy->held_apply & bit(i + 1)) == 0 &&
             current.applied < committed))
            return false;
    }
    return true;
}

static void converge(struct group *g, const struct policy *policy,
                     uint64_t committed)
{
    for (unsigned round = 0; round < 200 && !converged(g, policy, committed);
         round++)
        tick(g, policy);
    CHECK(converged(g, policy, committed));
}

static uint64_t stored_end(struct mem_node *n)
{
    const struct vsr_recovered *recovered = mem_store_recovered(
        mem_node_store(n), mem_store_readable(mem_node_store(n)));
    return recovered == NULL ? 1 : recovered->log_end;
}

static size_t outstanding(struct mem_node *n, uint32_t type)
{
    size_t count = 0;
    for (size_t j = 0; j < mem_node_effects(n); j++)
        if (mem_node_effect(n, j)->type == type)
            count++;
    return count;
}

int main(void)
{
    const struct policy open = {0, 0};
    const struct policy first_pair = {bit(2) | bit(3), 0};
    const struct policy first_pair_busy = {bit(2) | bit(3), bit(PRIMARY)};
    const struct policy alone = {bit(2) | bit(3) | bit(4) | bit(5),
                                 bit(PRIMARY)};
    const struct policy busy = {0, bit(PRIMARY)};
    struct group g = {mem_cluster_create(), {0}, 0};
    struct vsr_member members[NODES];
    for (uint32_t i = 0; i < NODES; i++)
        members[i] = (struct vsr_member){i + 1, VSR_MEMBER_FULL, 0};
    const struct vsr_membership group = {0, members, NODES, 2};
    for (uint32_t i = 0; i < NODES; i++) {
        struct vsr_options o = options(i + 1, &group);
        g.nodes[i] = mem_cluster_add(g.cluster, &o);
        mem_node_output_capacity(g.nodes[i], 1);
        for (unsigned attempt = 0;; attempt++) {
            CHECK(attempt < 1000);
            if (mem_node_time(g.nodes[i], 0).consumed == 1)
                break;
        }
    }
    settle(&g, &open);
    CHECK(status(node(&g, PRIMARY)).primary == PRIMARY);
    for (uint64_t i = 1; i <= 4; i++) {
        submit(&g, &open, i);
        converge(&g, &open, i);
    }

    /* Peers 2 and 3 stop hearing the primary and stay at entry 4 while the
     * other pair commits 5..7 with it. */
    for (uint64_t i = 5; i <= 7; i++) {
        submit(&g, &first_pair, i);
        converge(&g, &first_pair, i);
    }
    CHECK(status(node(&g, 2)).committed == 4);
    CHECK(status(node(&g, 3)).committed == 4);

    /* The primary's application stops answering: APPLY 8 stays outstanding
     * with its entry pinned and its result reserved, and 9..11 commit
     * behind it. */
    for (uint64_t i = 8; i <= 11; i++) {
        submit(&g, &first_pair_busy, i);
        converge(&g, &first_pair_busy, i);
    }
    CHECK(status(node(&g, PRIMARY)).committed == 11);
    CHECK(status(node(&g, PRIMARY)).applied == 7);
    CHECK(outstanding(node(&g, PRIMARY), VSR_OP_APPLY) == 1);

    /* Nobody hears the primary while it prepares 12 alone, so peers 4 and 5
     * stay at entry 11. Admitting the proposal at the minimum budget
     * releases every other cached entry, so the cache keeps exactly 12
     * beside the pinned entry 8. */
    submit(&g, &alone, 12);
    settle(&g, &alone);
    CHECK(stored_end(node(&g, PRIMARY)) == 13);
    CHECK(status(node(&g, PRIMARY)).committed == 11);
    CHECK(status(node(&g, 4)).committed == 11);
    CHECK(status(node(&g, 5)).committed == 11);

    /* The network heals with the application still busy: the retry timer
     * resets every peer's send cursor, peers 2 and 3 need entry 5 and peers
     * 4 and 5 need entry 12, and the primary must serve both pairs from its
     * four-entry cache. The old core loaded 5 for peer 3, failed to send the
     * cached 12 to peer 4 (both operation slots busy), evicted 12 under the
     * load's reservation, resumed at peer 5 and reloaded 12, then evicted 5
     * for peer 2's reload, forever, without sending any of them. */
    tick(&g, &busy);
    converge(&g, &busy, 12);
    CHECK(status(node(&g, PRIMARY)).applied == 7);

    /* The application answers; everything applies. */
    converge(&g, &open, 12);
    for (uint32_t i = 0; i < NODES; i++)
        CHECK(status(g.nodes[i]).applied == 12);
    CHECK(mem_node_replies(node(&g, PRIMARY)) == 12);
    mem_cluster_check(g.cluster);
    mem_cluster_destroy(g.cluster);
    return 0;
}
