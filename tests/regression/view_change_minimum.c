#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <stdint.h>

/* Regression for `tests/fuzzy/cluster 33 1 2000 quiet 3`.
 *
 * A primary's pending request keeps its command payload pinned while the
 * proposal waits for a quorum, and released it only on seeing the proposal's
 * cache placement stored. With a one-slot cache the next proposal, decided
 * ahead of the route poll as soon as the store completes, takes that
 * placement first, so a primary demoted with proposals in flight carried the
 * payloads into the view change. At the documented minimum payload budget
 * three pinned maximum-size commands (or as many smaller ones) left room for
 * neither a transfer chunk beside the input progress reserve nor a comparison
 * load beside a fetched command; nothing could free them, since a demoted
 * primary answers those requests only after the view change. The lagging
 * member's catch-up never completed, its election timer restarted the view
 * change, and the group elected forever.
 *
 * The route now releases its payload once the append carrying it is stable,
 * whether or not the placement is still cached. The scenario pins seven
 * payloads at the primary by completing its stores one at a time with the
 * next proposal already decided, keeps its cache slot unstored so its sends
 * cannot reload the evicted entries, isolates it while the other two members
 * elect a new view and commit three commands, then reconnects it: the view
 * change must complete and a new command must commit everywhere. */

enum { NODES = 3, OLD_PRIMARY = 1, NEW_PRIMARY = 2, WITNESS = 3 };
enum { SMALL = 14, LARGE = 32 };

struct policy {
    uint64_t dropped_to;   /* Messages to these replicas are discarded. */
    uint64_t dropped_from; /* Messages from these replicas are discarded. */
    uint64_t held_to;      /* Messages to these replicas wait in the queue. */
    uint64_t held_from;    /* Messages from these replicas wait in the queue. */
    uint64_t held_store; /* STORE effects of these replicas stay outstanding. */
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

/* The seeded profile 3 byte budgets for maximum-size commands with the
 * one-slot cache of the failing seed: the documented minimum, which
 * provisions one fetched and one comparison command beside the 40-byte input
 * progress reserve. Seven pinned 14-byte payloads take more of it than the
 * seed's three 32-byte ones; the lease budget stays out of the way. */
static struct vsr_options options(uint64_t replica,
                                  const struct vsr_membership *group)
{
    struct vsr_options o = mem_options(replica, group);
    struct vsr_limits *l = &o.limits;
    l->operations = 8;
    l->transfers = 1;
    l->input_leases = 16;
    l->log_cache_entries = 1;
    l->batch_entries = 1;
    l->client_cache_entries = 2;
    l->command_bytes = LARGE;
    l->result_bytes = 18;
    l->manifest_bytes = 8;
    l->message_bytes = 40;
    l->pinned_payload_bytes = l->message_bytes + 2 * l->command_bytes +
                              (l->transfers + 3) * l->manifest_bytes +
                              l->result_bytes;
    l->work_per_step = 1;
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

static bool deliverable(const struct policy *policy,
                        const struct vsr_message *message, uint64_t to)
{
    return (policy->held_to & bit(to)) == 0 &&
           (policy->held_from & bit(message->from)) == 0;
}

/* One scheduler turn in the seeded host's order: every node polls once, one
 * effect per node completes, then one deliverable message is offered. */
static bool turn(struct group *g, const struct policy *policy)
{
    bool progress = false;
    for (uint32_t i = 0; i < NODES; i++) {
        struct mem_node *n = g->nodes[i];
        const struct mem_step step = mem_node_event(n, NULL);
        CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
        progress |= step.emitted != 0 || (step.flags & VSR_UPDATE_MORE) != 0;
        for (size_t j = 0; j < mem_node_effects(n); j++) {
            if ((policy->held_store & bit(i + 1)) != 0 &&
                mem_node_effect(n, j)->type == VSR_OP_STORE)
                continue;
            if (mem_node_complete(n, j, VSR_IO_OK)) {
                progress = true;
                break;
            }
        }
    }
    for (size_t i = 0; i < mem_cluster_messages(g->cluster); i++) {
        uint64_t to;
        const struct vsr_message *message =
            mem_cluster_message(g->cluster, i, &to);
        if ((policy->dropped_to & bit(to)) != 0 ||
            (policy->dropped_from & bit(message->from)) != 0) {
            mem_cluster_drop(g->cluster, i);
            progress = true;
            break;
        }
        if (!deliverable(policy, message, to))
            continue;
        /* A refused message stays queued and does not block the ones behind
         * it, as the seeded host's random delivery order would not. */
        if (!mem_cluster_deliver(g->cluster, i))
            continue;
        progress = true;
        break;
    }
    mem_cluster_check(g->cluster);
    return progress;
}

/* Runs until nothing is runnable; a configuration that never idles is a
 * livelock. Held messages and held stores do not count as progress. */
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
                   uint64_t replica, uint64_t client, uint64_t number,
                   size_t size)
{
    unsigned char bytes[LARGE];
    CHECK(size <= sizeof(bytes));
    for (unsigned i = 0; i < size; i++)
        bytes[i] = (unsigned char)(client * 64 + number + i);
    const struct vsr_span span = {bytes, size};
    const struct vsr_blob body = {&span, size, 1, 0};
    const struct vsr_request request = {
        {{client, number}, 1}, 0, VSR_REQUEST_COMMAND, 0, &body};
    const struct vsr_event event = {VSR_EVENT_REQUEST, 0,
                                    client * 1000 + number, &request, 1};
    for (unsigned attempt = 0;; attempt++) {
        CHECK(attempt < 1000);
        if (mem_node_event(node(g, replica), &event).consumed == 1)
            return;
        (void)turn(g, policy);
    }
}

/* Every replica in the mask is NORMAL at or past the committed position, with
 * full members applied there. */
static bool converged(struct group *g, uint64_t mask, uint64_t committed)
{
    for (uint32_t i = 0; i < NODES; i++) {
        const struct vsr_status current = status(g->nodes[i]);
        if ((mask & bit(i + 1)) == 0)
            continue;
        if (current.state != VSR_STATE_NORMAL ||
            current.committed < committed ||
            (current.role == VSR_MEMBER_FULL && current.applied < committed))
            return false;
    }
    return true;
}

static void converge(struct group *g, const struct policy *policy,
                     uint64_t mask, uint64_t committed)
{
    for (unsigned round = 0; round < 200 && !converged(g, mask, committed);
         round++)
        tick(g, policy);
    CHECK(converged(g, mask, committed));
}

/* Discards every queued message from this replica. */
static void purge(struct group *g, uint64_t from)
{
    for (size_t i = 0; i < mem_cluster_messages(g->cluster);) {
        uint64_t to;
        const struct vsr_message *message =
            mem_cluster_message(g->cluster, i, &to);
        if (message->from == from)
            mem_cluster_drop(g->cluster, i);
        else
            i++;
    }
}

static uint64_t stored_end(struct mem_node *n)
{
    const struct vsr_recovered *recovered = mem_store_recovered(
        mem_node_store(n), mem_store_readable(mem_node_store(n)));
    return recovered == NULL ? 1 : recovered->log_end;
}

static size_t outstanding_stores(struct mem_node *n)
{
    size_t count = 0;
    for (size_t j = 0; j < mem_node_effects(n); j++)
        if (mem_node_effect(n, j)->type == VSR_OP_STORE)
            count++;
    return count;
}

/* Completes the oldest outstanding STORE of this replica. */
static void complete_store(struct mem_node *n)
{
    for (size_t j = 0; j < mem_node_effects(n); j++)
        if (mem_node_effect(n, j)->type == VSR_OP_STORE) {
            CHECK(mem_node_complete(n, j, VSR_IO_OK));
            return;
        }
    CHECK(false);
}

/* The NORMAL primary every replica in the mask agrees on, or zero. */
static uint64_t leader(struct group *g, uint64_t mask)
{
    uint64_t primary = 0;
    for (uint32_t i = 0; i < NODES; i++) {
        const struct vsr_status current = status(g->nodes[i]);
        if ((mask & bit(i + 1)) == 0)
            continue;
        if (current.state != VSR_STATE_NORMAL ||
            (primary != 0 && current.primary != primary))
            return 0;
        primary = current.primary;
    }
    return primary;
}

int main(void)
{
    const struct policy open = {0, 0, 0, 0, 0};
    const struct policy staged = {bit(NEW_PRIMARY), 0, bit(WITNESS),
                                  bit(WITNESS), bit(OLD_PRIMARY)};
    const struct policy isolated = {bit(OLD_PRIMARY), bit(OLD_PRIMARY), 0, 0,
                                    bit(OLD_PRIMARY)};
    const struct policy reconnected = {0, 0, 0, 0, bit(OLD_PRIMARY)};
    const uint64_t all = bit(OLD_PRIMARY) | bit(NEW_PRIMARY) | bit(WITNESS);
    const uint64_t others = bit(NEW_PRIMARY) | bit(WITNESS);
    struct group g = {mem_cluster_create(), {0}, 0};
    struct vsr_member members[] = {{OLD_PRIMARY, VSR_MEMBER_FULL, 0},
                                   {NEW_PRIMARY, VSR_MEMBER_FULL, 0},
                                   {WITNESS, VSR_MEMBER_WITNESS, 0}};
    const struct vsr_membership group = {0, members, NODES, 1};
    struct mem_node *primary;
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
    primary = node(&g, OLD_PRIMARY);
    settle(&g, &open);
    CHECK(status(primary).primary == OLD_PRIMARY);
    for (uint64_t i = 1; i <= 16; i++) {
        submit(&g, &open, OLD_PRIMARY, 9, i, LARGE);
        converge(&g, &open, all, i);
    }

    /* Eight small requests from eight clients reach the primary while its
     * stores stay outstanding, the other full member hears nothing, and the
     * witness is parked: 17 is appended and the rest wait, decided, for its
     * placement. All eight fit beside the progress reserve. */
    for (uint64_t i = 1; i <= 8; i++)
        submit(&g, &staged, OLD_PRIMARY, i, 1, SMALL);
    settle(&g, &staged);
    CHECK(outstanding_stores(primary) == 1);
    CHECK(stored_end(primary) == 17);

    /* Each completed store lets the next decided request take the slot
     * before the route poll can see the stored placement, so the payloads of
     * 17..23 stay pinned. 24 is placed with its store outstanding, which
     * keeps the slot unstored: the primary cannot reload an evicted entry to
     * send it while it is cut off. */
    for (uint64_t op = 17; op <= 23; op++) {
        complete_store(primary);
        settle(&g, &staged);
        CHECK(stored_end(primary) == op + 1);
        CHECK(outstanding_stores(primary) == 1);
    }
    CHECK(status(primary).committed == 16);

    /* The others elect a view the old primary never joins and commit three
     * commands of their own at 17..19. */
    purge(&g, OLD_PRIMARY);
    for (unsigned round = 0; round < 100 && leader(&g, others) != NEW_PRIMARY;
         round++)
        tick(&g, &isolated);
    CHECK(leader(&g, others) == NEW_PRIMARY);
    for (uint64_t i = 17; i <= 19; i++) {
        submit(&g, &isolated, NEW_PRIMARY, 9, i, LARGE);
        converge(&g, &isolated, others, i);
    }
    CHECK(status(primary).committed == 16);
    CHECK(status(primary).state == VSR_STATE_NORMAL);
    CHECK(status(primary).primary == OLD_PRIMARY);

    /* Reconnected, the old primary learns the later view from the next
     * heartbeat and begins its catch-up, which drains its last store first. */
    for (unsigned round = 0;
         round < 100 && status(primary).state == VSR_STATE_NORMAL; round++)
        tick(&g, &reconnected);
    CHECK(status(primary).state == VSR_STATE_VIEW_CHANGE);
    CHECK(outstanding_stores(primary) >= 1);

    /* Its pending payloads may not starve the transfer that replaces 17..24
     * with the selected history. The old core elected forever here. */
    converge(&g, &open, all, 19);
    for (unsigned round = 0; round < 100 && leader(&g, all) == 0; round++)
        tick(&g, &open);
    CHECK(leader(&g, all) != 0);
    submit(&g, &open, leader(&g, all), 9, 20, LARGE);
    converge(&g, &open, all, 20);
    CHECK(mem_node_applied(primary) == mem_node_applied(node(&g, NEW_PRIMARY)));
    CHECK(mem_node_checksum(primary) ==
          mem_node_checksum(node(&g, NEW_PRIMARY)));
    mem_cluster_check(g.cluster);
    mem_cluster_destroy(g.cluster);
    return 0;
}
