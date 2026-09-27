#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <stdint.h>

/* Regression for `tests/fuzzy/cluster 2 1 2000 quiet 3`.
 *
 * A replica loaded a client's completed record from its readable revision,
 * decided an EXECUTED reply from it, and only then emitted the reply, after a
 * state transfer had stored a RESTORE that replaced the indexed client base
 * with a donor checkpoint's. The readable revision no longer backed the reply:
 * its record for that client named a later request. Loads and decisions were
 * re-validated against the history generation, decided replies were not.
 *
 * An executed reply is now decided again after a restoration and is held
 * while one is outstanding. The scenario runs one work unit per step so the
 * decision, the RESTORE, and the emission are separate polls: node 1 answers
 * client 7's requests 1..3, its record load for request 3 is held, it drops
 * out, nodes 2 and 3 execute request 4 and checkpoint past it, and node 1
 * rejoins through a checkpoint fetch. Once the fetch completes, the held load
 * completes at the pre-restoration revision. Node 1 must answer request 3 as
 * stale from the restored base instead of replying EXECUTED from the old
 * revision, and the host oracle must see every reply backed by its store. */

enum { NODES = 3, OLD_PRIMARY = 1, NEW_PRIMARY = 2, THIRD = 3, CLIENT = 7 };

struct policy {
    uint64_t isolated; /* Messages to or from these replicas are dropped. */
    uint64_t hold;     /* Replicas whose record loads and fetches wait. */
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

static struct vsr_options options(uint64_t replica,
                                  const struct vsr_membership *group)
{
    struct vsr_options o = mem_options(replica, group);
    o.limits.work_per_step = 1;
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

static bool held(const struct vsr_op *op)
{
    if (op->type == VSR_OP_SNAPSHOT_FETCH)
        return true;
    if (op->type != VSR_OP_LOAD)
        return false;
    return ((const struct vsr_store_read *)op->data)->type == VSR_LOAD_CLIENT;
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
            if ((policy->hold & bit(i + 1)) != 0 && held(mem_node_effect(n, j)))
                continue;
            if (mem_node_complete(n, j, VSR_IO_OK)) {
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

/* Runs until nothing is runnable; held effects do not count as progress. */
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
        {{CLIENT, 1}, number}, 0, VSR_REQUEST_COMMAND, 0, &body};
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

static size_t outstanding(struct mem_node *n, uint32_t type)
{
    size_t count = 0;
    for (size_t j = 0; j < mem_node_effects(n); j++)
        if (mem_node_effect(n, j)->type == type)
            count++;
    return count;
}

/* Completes the first held effect of this kind without letting the node poll:
 * at one work unit per step the completion is admitted and nothing else. */
static void release(struct mem_node *n, uint32_t type)
{
    for (size_t j = 0; j < mem_node_effects(n); j++) {
        if (mem_node_effect(n, j)->type == type &&
            held(mem_node_effect(n, j))) {
            CHECK(mem_node_complete(n, j, VSR_IO_OK));
            return;
        }
    }
    CHECK(false);
}

int main(void)
{
    const struct policy open = {0, 0};
    const struct policy holding = {0, bit(OLD_PRIMARY)};
    const struct policy alone = {bit(OLD_PRIMARY), bit(OLD_PRIMARY)};
    struct group g = {mem_cluster_create(), {0}, 0};
    struct vsr_member members[] = {{OLD_PRIMARY, VSR_MEMBER_FULL, 0},
                                   {NEW_PRIMARY, VSR_MEMBER_FULL, 0},
                                   {THIRD, VSR_MEMBER_FULL, 0}};
    const struct vsr_membership group = {0, members, NODES, 1};
    for (uint32_t i = 0; i < NODES; i++) {
        struct vsr_options o = options(i + 1, &group);
        g.nodes[i] = mem_cluster_add(g.cluster, &o);
        for (unsigned attempt = 0;; attempt++) {
            CHECK(attempt < 1000);
            if (mem_node_time(g.nodes[i], 0).consumed == 1)
                break;
        }
    }
    settle(&g, &open);
    CHECK(status(node(&g, OLD_PRIMARY)).primary == OLD_PRIMARY);
    for (uint64_t i = 1; i <= 2; i++) {
        submit(&g, &open, OLD_PRIMARY, i);
        converge(&g, &open, i);
    }
    CHECK(mem_node_replies(node(&g, OLD_PRIMARY)) == 2);

    /* Request 3 commits and applies everywhere. Once node 1 has applied it,
     * the load of the completed record that decides its reply is held. */
    submit(&g, &open, OLD_PRIMARY, 3);
    for (unsigned t = 0;
         t < 100000 && status(node(&g, OLD_PRIMARY)).applied < 3; t++)
        (void)turn(&g, &open);
    CHECK(status(node(&g, OLD_PRIMARY)).applied == 3);
    CHECK(mem_node_replies(node(&g, OLD_PRIMARY)) == 2);
    converge(&g, &holding, 3);
    CHECK(mem_node_replies(node(&g, OLD_PRIMARY)) == 2);
    CHECK(outstanding(node(&g, OLD_PRIMARY), VSR_OP_LOAD) == 1);

    /* Without node 1, the others elect a new view, execute request 4, and
     * checkpoint past it, so the restored client base names request 4. */
    for (unsigned round = 0; round < 400; round++) {
        const struct vsr_status current = status(node(&g, NEW_PRIMARY));
        if (current.state == VSR_STATE_NORMAL && current.primary == NEW_PRIMARY)
            break;
        tick(&g, &alone);
    }
    CHECK(status(node(&g, NEW_PRIMARY)).primary == NEW_PRIMARY);
    submit(&g, &alone, NEW_PRIMARY, 4);
    converge(&g, &alone, 4);
    checkpoint_at(&g, &alone, NEW_PRIMARY, 4);
    checkpoint_at(&g, &alone, THIRD, 4);
    CHECK(status(node(&g, OLD_PRIMARY)).applied == 3);

    /* Node 1 rejoins and fetches the checkpoint; the fetch is held until it
     * is the only outstanding transfer effect. */
    for (unsigned round = 0; round < 400; round++) {
        if (outstanding(node(&g, OLD_PRIMARY), VSR_OP_SNAPSHOT_FETCH) != 0)
            break;
        tick(&g, &holding);
    }
    CHECK(outstanding(node(&g, OLD_PRIMARY), VSR_OP_SNAPSHOT_FETCH) == 1);
    CHECK(outstanding(node(&g, OLD_PRIMARY), VSR_OP_LOAD) == 1);
    CHECK(mem_node_replies(node(&g, OLD_PRIMARY)) == 2);

    /* The fetch completes, making the RESTORE due, and then the record load
     * completes at the revision before it: the decided EXECUTED reply must
     * not be emitted against the restored base. */
    release(node(&g, OLD_PRIMARY), VSR_OP_SNAPSHOT_FETCH);
    release(node(&g, OLD_PRIMARY), VSR_OP_LOAD);
    converge(&g, &open, 4);
    CHECK(mem_node_replies(node(&g, OLD_PRIMARY)) == 3);
    {
        const struct vsr_reply *reply =
            mem_node_reply(node(&g, OLD_PRIMARY), 2, NULL);
        CHECK(reply->request.number == 3);
        CHECK(reply->status == VSR_REPLY_STALE_REQUEST);
        CHECK((reply->flags & VSR_REPLY_EXECUTED) == 0);
    }
    submit(&g, &open, NEW_PRIMARY, 5);
    converge(&g, &open, 5);
    CHECK(mem_node_checksum(node(&g, OLD_PRIMARY)) ==
          mem_node_checksum(node(&g, NEW_PRIMARY)));
    mem_cluster_check(g.cluster);
    mem_cluster_destroy(g.cluster);
    return 0;
}
