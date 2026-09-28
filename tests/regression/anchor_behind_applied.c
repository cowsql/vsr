#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <stdint.h>

/* Regression for `tests/fuzzy/cluster 203 1 2000 quiet 3`.
 *
 * The primary elected for a new view selected a peer's DO_VIEW_CHANGE offer
 * whose checkpoint was newer than its own published one but below its own
 * applied position, and adopted it: the RESTORE and SNAPSHOT_INSTALL rewound a
 * running application behind entries it had already applied, only for replay
 * to apply them again. In the window between the install's execution and its
 * completion the core still reported the old applied position, above the
 * application's, which the host oracle rejects.
 *
 * A full member whose application already reached the selected checkpoint now
 * keeps its state and fetches only the log past its position. The scenario:
 * node 2 checkpoints at op 4, node 3 at op 6, all three apply through op 7,
 * and nodes 1 and 3 commit op 8 without node 2. Node 1 then drops out, node 2
 * becomes primary of view 1 with node 3's longer offer, and must reach op 8
 * without a SNAPSHOT_INSTALL. */

enum { NODES = 3, OLD_PRIMARY = 1, NEW_PRIMARY = 2, DONOR = 3 };

struct policy {
    uint64_t isolated; /* Messages to or from these replicas are dropped. */
};

struct group {
    struct mem_cluster *cluster;
    struct mem_node *nodes[NODES];
    uint64_t now;
    size_t installs[NODES];
};

static uint64_t bit(uint64_t replica)
{
    return UINT64_C(1) << replica;
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

static bool turn(struct group *g, const struct policy *policy)
{
    bool progress = false;
    for (uint32_t i = 0; i < NODES; i++) {
        struct mem_node *n = g->nodes[i];
        const struct mem_step step = mem_node_event(n, NULL);
        CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
        progress |= step.emitted != 0 || (step.flags & VSR_UPDATE_MORE) != 0;
        for (size_t j = 0; j < mem_node_effects(n); j++) {
            const uint32_t type = mem_node_effect(n, j)->type;
            if (mem_node_complete(n, j, VSR_IO_OK)) {
                if (type == VSR_OP_SNAPSHOT_INSTALL)
                    g->installs[i]++;
                progress = true;
                break;
            }
        }
    }
    if (mem_cluster_messages(g->cluster) != 0) {
        uint64_t to;
        const struct vsr_message *message =
            mem_cluster_message(g->cluster, 0, &to);
        if ((policy->isolated & (bit(to) | bit(message->from))) != 0) {
            mem_cluster_drop(g->cluster, 0);
            progress = true;
        } else if (mem_cluster_deliver(g->cluster, 0)) {
            progress = true;
        }
    }
    mem_cluster_check(g->cluster);
    return progress;
}

static void settle(struct group *g, const struct policy *policy)
{
    for (unsigned t = 0; t < 100000; t++)
        if (!turn(g, policy))
            return;
    CHECK(false);
}

/* Isolated replicas receive no time either, so only the reachable members'
 * election timers run. */
static void tick(struct group *g, const struct policy *policy)
{
    settle(g, policy);
    g->now += 5;
    for (uint32_t i = 0; i < NODES; i++) {
        if ((policy->isolated & bit(i + 1)) != 0)
            continue;
        for (unsigned attempt = 0;; attempt++) {
            CHECK(attempt < 1000);
            if (mem_node_time(g->nodes[i], g->now).consumed == 1)
                break;
        }
    }
    settle(g, policy);
}

static void submit(struct group *g, const struct policy *policy,
                   uint64_t primary, uint64_t number)
{
    unsigned char bytes[8];
    for (unsigned i = 0; i < sizeof(bytes); i++)
        bytes[i] = (unsigned char)(number + i);
    const struct vsr_span span = {bytes, sizeof(bytes)};
    const struct vsr_blob body = {&span, sizeof(bytes), 1, 0};
    const struct vsr_request request = {
        {{7, number}, 1}, 0, VSR_REQUEST_COMMAND, 0, &body};
    const struct vsr_event event = {VSR_EVENT_REQUEST, 0, number, &request, 1};
    for (unsigned attempt = 0;; attempt++) {
        CHECK(attempt < 1000);
        if (mem_node_event(node(g, primary), &event).consumed == 1)
            return;
        (void)turn(g, policy);
    }
}

static void hint(struct mem_node *n)
{
    const struct vsr_event event = {VSR_EVENT_CHECKPOINT, 0, 0, NULL, 0};
    CHECK(mem_node_event(n, &event).consumed == 1);
}

static bool converged(struct group *g, const struct policy *policy,
                      uint64_t committed)
{
    for (uint32_t i = 0; i < NODES; i++) {
        const struct vsr_status current = status(g->nodes[i]);
        if ((policy->isolated & bit(i + 1)) != 0)
            continue;
        if (current.state != VSR_STATE_NORMAL ||
            current.committed < committed || current.applied < committed)
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

static void checkpoint_at(struct group *g, const struct policy *policy,
                          uint64_t replica, uint64_t op)
{
    hint(node(g, replica));
    for (unsigned round = 0;
         round < 200 && status(node(g, replica)).checkpoint_op != op; round++)
        tick(g, policy);
    CHECK(status(node(g, replica)).checkpoint_op == op);
}

int main(void)
{
    const struct policy open = {0};
    const struct policy without_new = {bit(NEW_PRIMARY)};
    const struct policy without_old = {bit(OLD_PRIMARY)};
    struct group g = {mem_cluster_create(), {0}, 0, {0}};
    struct vsr_member members[] = {{OLD_PRIMARY, VSR_MEMBER_FULL, 0},
                                   {NEW_PRIMARY, VSR_MEMBER_FULL, 0},
                                   {DONOR, VSR_MEMBER_FULL, 0}};
    const struct vsr_membership group = {0, members, NODES, 1};
    for (uint32_t i = 0; i < NODES; i++) {
        struct vsr_options o = mem_options(i + 1, &group);
        g.nodes[i] = mem_cluster_add(g.cluster, &o);
        for (unsigned attempt = 0;; attempt++) {
            CHECK(attempt < 1000);
            if (mem_node_time(g.nodes[i], 0).consumed == 1)
                break;
        }
    }
    settle(&g, &open);
    CHECK(status(node(&g, OLD_PRIMARY)).primary == OLD_PRIMARY);

    for (uint64_t i = 1; i <= 4; i++) {
        submit(&g, &open, OLD_PRIMARY, i);
        converge(&g, &open, i);
    }
    checkpoint_at(&g, &open, NEW_PRIMARY, 4);
    for (uint64_t i = 5; i <= 6; i++) {
        submit(&g, &open, OLD_PRIMARY, i);
        converge(&g, &open, i);
    }
    checkpoint_at(&g, &open, DONOR, 6);
    submit(&g, &open, OLD_PRIMARY, 7);
    converge(&g, &open, 7);
    CHECK(status(node(&g, NEW_PRIMARY)).checkpoint_op == 4);
    CHECK(status(node(&g, DONOR)).checkpoint_op == 6);

    /* Op 8 commits without the future primary, whose application stays at
     * op 7: above the donor's checkpoint, below the donor's log end. */
    submit(&g, &without_new, OLD_PRIMARY, 8);
    converge(&g, &without_new, 8);
    CHECK(status(node(&g, NEW_PRIMARY)).committed == 7);
    CHECK(status(node(&g, NEW_PRIMARY)).applied == 7);

    /* The old primary drops out. The new primary selects the donor's longer
     * offer and must not install the donor's checkpoint behind its own
     * application; it fetches op 8 and finishes the view. */
    const size_t installs = g.installs[NEW_PRIMARY - 1];
    for (unsigned round = 0; round < 400; round++) {
        const struct vsr_status current = status(node(&g, NEW_PRIMARY));
        if (current.state == VSR_STATE_NORMAL &&
            current.primary == NEW_PRIMARY && current.committed >= 8)
            break;
        tick(&g, &without_old);
    }
    CHECK(status(node(&g, NEW_PRIMARY)).primary == NEW_PRIMARY);
    CHECK(status(node(&g, NEW_PRIMARY)).state == VSR_STATE_NORMAL);
    converge(&g, &without_old, 8);
    CHECK(g.installs[NEW_PRIMARY - 1] == installs);
    CHECK(status(node(&g, NEW_PRIMARY)).checkpoint_op >= 4);

    converge(&g, &open, 8);
    submit(&g, &open, NEW_PRIMARY, 9);
    converge(&g, &open, 9);
    CHECK(mem_node_checksum(node(&g, NEW_PRIMARY)) ==
          mem_node_checksum(node(&g, DONOR)));
    CHECK(mem_node_checksum(node(&g, OLD_PRIMARY)) ==
          mem_node_checksum(node(&g, DONOR)));
    mem_cluster_check(g.cluster);
    mem_cluster_destroy(g.cluster);
    return 0;
}
