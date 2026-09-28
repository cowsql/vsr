#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <stdint.h>

/* Regression for `tests/fuzzy/cluster 137 1 2000 quiet 3`.
 *
 * At the documented minimum payload budget a witness held three command-size
 * leases its transfer could not release yet, and its commit-notification loop
 * had just loaded the next committed entry into an otherwise empty cache. A
 * transfer chunk that did not fit beside those holds was refused with
 * VSR_AGAIN, and the core released a readable cache pin at admission time to
 * make room for the retry. The only releasable pin was the loaded notification
 * head, which the very next poll reloaded before the host retried the input,
 * so the input evicted it again: the notification never advanced, the chunk
 * was never admitted, and the node never idled.
 *
 * Pressure relief now waits until the protocol poll is idle, when no poll can
 * consume the pin before the retry. The scenario rebuilds the shape with one
 * work unit per step: the witness appends proposals 17..19 whose STORE stays
 * outstanding (three pins nothing may release), learns that 16 committed so
 * the notification loop must load the evicted entry 13, and is then offered
 * PREPARE 20. The witness must finish the notification and idle with the
 * input blocked; once its stores complete the PREPARE is admitted and the
 * group converges. */

enum { NODES = 3, PRIMARY = 1, LAGGING = 2, WITNESS = 3 };

struct policy {
    uint64_t dropped;    /* Messages to these replicas are discarded. */
    uint64_t held_to;    /* Messages to these replicas wait in the queue. */
    uint64_t held_from;  /* Messages from these replicas wait in the queue. */
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

/* The seeded profile 2 budgets for maximum-size commands, with the eight-slot
 * cache of the failing seed, a single work unit per step, and enough effect
 * slots for a LOAD beside the outstanding stores. Three 32-byte holds plus
 * one 32-byte input exceed the 154-byte budget with its 40-byte progress
 * reserve; two holds fit. */
static struct vsr_options options(uint64_t replica,
                                  const struct vsr_membership *group)
{
    struct vsr_options o = mem_options(replica, group);
    struct vsr_limits *l = &o.limits;
    l->operations = 8;
    l->transfers = 1;
    l->input_leases = l->transfers + 8u;
    l->log_cache_entries = 8;
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
        if ((policy->dropped & bit(to)) != 0) {
            mem_cluster_drop(g->cluster, i);
            progress = true;
            break;
        }
        if (!deliverable(policy, message, to))
            continue;
        if (mem_cluster_deliver(g->cluster, i))
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
            (current.role == VSR_MEMBER_FULL && current.applied < committed))
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

static size_t queued(struct group *g, uint64_t from, uint64_t to, uint32_t type)
{
    size_t count = 0;
    for (size_t i = 0; i < mem_cluster_messages(g->cluster); i++) {
        uint64_t target;
        const struct vsr_message *message =
            mem_cluster_message(g->cluster, i, &target);
        if (message->from == from && target == to && message->type == type)
            count++;
    }
    return count;
}

/* Offers the first queued message of this kind and number (0 for any); false
 * when it was refused. */
static bool offer(struct group *g, uint64_t from, uint64_t to, uint32_t type,
                  uint64_t number)
{
    for (size_t i = 0; i < mem_cluster_messages(g->cluster); i++) {
        uint64_t target;
        const struct vsr_message *message =
            mem_cluster_message(g->cluster, i, &target);
        if (message->from == from && target == to && message->type == type &&
            (number == 0 || message->number == number))
            return mem_cluster_deliver(g->cluster, i);
    }
    CHECK(false);
    return false;
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

int main(void)
{
    const struct policy open = {0, 0, 0, 0};
    const struct policy staged = {bit(LAGGING), 0, bit(WITNESS), 0};
    const struct policy parked = {bit(LAGGING), bit(WITNESS), bit(WITNESS),
                                  bit(WITNESS)};
    const struct policy blocked = {bit(LAGGING), 0, 0, bit(WITNESS)};
    const struct policy lagging = {bit(LAGGING), 0, 0, 0};
    struct group g = {mem_cluster_create(), {0}, 0};
    struct vsr_member members[] = {{PRIMARY, VSR_MEMBER_FULL, 0},
                                   {LAGGING, VSR_MEMBER_FULL, 0},
                                   {WITNESS, VSR_MEMBER_WITNESS, 0}};
    const struct vsr_membership group = {0, members, NODES, 1};
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
    for (uint64_t i = 1; i <= 12; i++) {
        submit(&g, &open, i);
        converge(&g, &open, i);
    }

    /* Proposals 13..16 reach the witness and are stored while its
     * acknowledgements wait and the other full member hears nothing, so
     * nothing commits. Each admission releases the lowest cache slot first,
     * so entry 13 leaves the cache. */
    for (uint64_t i = 13; i <= 16; i++) {
        submit(&g, &staged, i);
        settle(&g, &staged);
        CHECK(stored_end(node(&g, WITNESS)) == i + 1);
    }
    CHECK(status(node(&g, PRIMARY)).committed == 12);
    CHECK(status(node(&g, WITNESS)).committed == 12);
    CHECK(queued(&g, WITNESS, PRIMARY, VSR_MSG_PREPARE_OK) != 0);

    /* Proposals 17..20 are prepared while everything to the witness is
     * parked, then the waiting acknowledgements commit 13..16 at the primary
     * and its COMMIT is parked too, so the inputs can be offered in order. */
    for (uint64_t i = 17; i <= 20; i++) {
        submit(&g, &parked, i);
        settle(&g, &parked);
    }
    CHECK(queued(&g, PRIMARY, WITNESS, VSR_MSG_PREPARE) == 4);
    while (queued(&g, WITNESS, PRIMARY, VSR_MSG_PREPARE_OK) != 0) {
        CHECK(offer(&g, WITNESS, PRIMARY, VSR_MSG_PREPARE_OK, 0));
        settle(&g, &parked);
    }
    CHECK(status(node(&g, PRIMARY)).committed == 16);

    /* The witness learns the commit and makes it stable, completing only the
     * effects that need to; the notification LOAD of entry 13 stays open. */
    CHECK(offer(&g, PRIMARY, WITNESS, VSR_MSG_COMMIT, 16));
    {
        struct mem_node *witness = node(&g, WITNESS);
        for (unsigned attempt = 0; status(witness).committed != 16; attempt++) {
            CHECK(attempt < 1000);
            (void)mem_node_event(witness, NULL);
            for (size_t j = 0; j < mem_node_effects(witness); j++)
                if (mem_node_effect(witness, j)->type != VSR_OP_LOAD &&
                    mem_node_complete(witness, j, VSR_IO_OK))
                    break;
        }
    }

    /* Proposals 17..19 are appended with their STORE outstanding: three
     * command-size pins that relief cannot release, like a transfer's held
     * chunks; each admission releases what is releasable, including the
     * loaded entry 13 whenever it is present. PREPARE 20 then never fits.
     * The old core released the loaded entry for it and reloaded it forever;
     * the witness must instead finish its announcements and idle with the
     * input blocked. */
    settle(&g, &blocked);
    CHECK(status(node(&g, WITNESS)).committed == 16);
    CHECK(stored_end(node(&g, WITNESS)) == 17);
    CHECK(outstanding_stores(node(&g, WITNESS)) != 0);
    CHECK(queued(&g, PRIMARY, WITNESS, VSR_MSG_PREPARE) == 1);

    /* Completing the stores frees the pins; the PREPARE is admitted. */
    settle(&g, &lagging);
    CHECK(queued(&g, PRIMARY, WITNESS, VSR_MSG_PREPARE) == 0);
    CHECK(stored_end(node(&g, WITNESS)) == 21);
    converge(&g, &lagging, 20);
    converge(&g, &open, 20);
    CHECK(status(node(&g, LAGGING)).applied >= 20);
    CHECK(mem_node_replies(node(&g, PRIMARY)) == 20);
    mem_cluster_check(g.cluster);
    mem_cluster_destroy(g.cluster);
    return 0;
}
