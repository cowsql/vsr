#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <stdint.h>

/* Regression for `tests/fuzzy/cluster 1 1 600 quiet 125` (also 3, and
 * `3 1 600 quiet 120`).
 *
 * A JOIN learner restarted with RECOVER and its epoch-0 seed, the documented
 * restart during warm-up, took the quorum-recovery path whenever no complete
 * durable state existed: in durable mode before its first SYNC and in
 * replicated mode always. Not being a member of the seed, that recovery never
 * completed and the node stayed RECOVERING forever. Worse, it persisted a hard
 * state with role NONE, which the recovered-state validator rejects, so the
 * next restart could not boot at all.
 *
 * Quorum recovery is for members. A restarted learner resumes nonvoting
 * warm-up: from its persisted warming role when a store survives, and from
 * scratch, as the role RECOVER names, when none does. It never persists a
 * role it would reject. The learner must end WARMING and caught up after two
 * restarts in each situation, then be admitted by RECONFIGURE and execute
 * new commands as a member. */

enum { MEMBERS = 3, LEARNER = 3, NODES = 4, CLIENT = 210 };

static struct vsr_status status(struct mem_node *node)
{
    struct vsr_status result;
    vsr_get_status(mem_node_core(node), &result);
    CHECK(result.failure.code == VSR_FAILURE_NONE);
    return result;
}

static void drive(struct mem_cluster *cluster, struct mem_node **nodes)
{
    for (size_t turn = 0; turn < 100000; turn++) {
        bool progress = false;
        for (size_t i = 0; i < NODES; i++) {
            if (nodes[i] == NULL || !mem_node_alive(nodes[i]))
                continue;
            const struct mem_step step = mem_node_event(nodes[i], NULL);
            CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
            if (step.emitted != 0 || (step.flags & VSR_UPDATE_MORE) != 0)
                progress = true;
            for (size_t j = 0; j < mem_node_effects(nodes[i]); j++) {
                if (mem_node_complete(nodes[i], j, VSR_IO_OK)) {
                    progress = true;
                    break;
                }
            }
        }
        for (size_t i = 0; i < mem_cluster_messages(cluster); i++) {
            if (mem_cluster_deliver(cluster, i)) {
                progress = true;
                break;
            }
        }
        mem_cluster_check(cluster);
        if (!progress)
            return;
    }
    CHECK(false);
}

static void tick_node(struct mem_node *node, uint64_t now)
{
    unsigned attempt;
    for (attempt = 0; attempt < 10000; attempt++) {
        const struct mem_step step = mem_node_time(node, now);
        CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
        if (step.consumed == 1)
            break;
    }
    CHECK(attempt < 10000);
}

static void tick(struct mem_node **nodes, uint64_t now)
{
    for (size_t i = 0; i < NODES; i++)
        if (nodes[i] != NULL && mem_node_alive(nodes[i]))
            tick_node(nodes[i], now);
}

static struct mem_node *leader(struct mem_node **nodes)
{
    for (size_t i = 0; i < MEMBERS; i++)
        if (status(nodes[i]).state == VSR_STATE_NORMAL &&
            status(nodes[i]).primary == mem_node_id(nodes[i]))
            return nodes[i];
    return NULL;
}

/* Advance logical time in heartbeat steps until the learner reports the
 * expected state, epoch, and commitment; fail if it never does. */
static void settle_learner(struct mem_cluster *cluster, struct mem_node **nodes,
                           uint64_t *now, uint32_t state, uint64_t epoch,
                           uint64_t committed)
{
    for (unsigned round = 0; round < 512; round++) {
        drive(cluster, nodes);
        const struct vsr_status current = status(nodes[LEARNER]);
        if (current.state == state && current.epoch == epoch &&
            current.configuration != NULL &&
            current.configuration->phase == VSR_EPOCH_STEADY &&
            current.committed >= committed)
            return;
        *now += 10;
        tick(nodes, *now);
    }
    CHECK(false);
}

static void submit(struct mem_node *node, uint64_t epoch, uint64_t number,
                   uint32_t type, const void *body)
{
    const struct vsr_request request = {
        {{CLIENT, 1}, number}, epoch, type, 0, body};
    const struct vsr_event event = {VSR_EVENT_REQUEST, 0, number, &request, 1};
    const struct mem_step step = mem_node_event(node, &event);
    CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
    CHECK(step.consumed == 1);
}

static const struct vsr_reply *reply(struct mem_node *node, uint64_t route)
{
    for (size_t i = mem_node_replies(node); i > 0; i--) {
        uint64_t found;
        const struct vsr_reply *answer = mem_node_reply(node, i - 1, &found);
        if (found == route)
            return answer;
    }
    return NULL;
}

/* Submit one request at the current primary and drive until it is answered;
 * a NOT_PRIMARY answer during a view change is resubmitted at the new one. */
static uint64_t commit(struct mem_cluster *cluster, struct mem_node **nodes,
                       uint64_t *now, uint64_t epoch, uint64_t number,
                       uint32_t type, const void *body)
{
    for (unsigned round = 0; round < 512; round++) {
        struct mem_node *primary = leader(nodes);
        if (primary != NULL) {
            submit(primary, epoch, number, type, body);
            drive(cluster, nodes);
            const struct vsr_reply *answer = reply(primary, number);
            if (answer != NULL && answer->status == VSR_REPLY_OK)
                return answer->op;
        }
        *now += 10;
        tick(nodes, *now);
        drive(cluster, nodes);
    }
    CHECK(false);
    return 0;
}

static void build(struct mem_cluster *cluster, struct mem_node **nodes,
                  const struct vsr_membership *membership, uint32_t policy)
{
    for (uint32_t i = 0; i < MEMBERS; i++) {
        struct vsr_options options = mem_options((uint64_t)i + 1, membership);
        options.durability = policy;
        nodes[i] = mem_cluster_add(cluster, &options);
        tick_node(nodes[i], 0);
    }
    nodes[LEARNER] = NULL;
}

static void join(struct mem_cluster *cluster, struct mem_node **nodes,
                 const struct vsr_membership *membership, uint32_t policy,
                 uint64_t now)
{
    struct vsr_options options = mem_options(LEARNER + 1, membership);
    options.durability = policy;
    options.start_mode = VSR_START_JOIN;
    options.join_role = VSR_MEMBER_FULL;
    nodes[LEARNER] = mem_cluster_add(cluster, &options);
    tick_node(nodes[LEARNER], now);
}

/* Complete the learner's boot effects only until its first SYNC is issued,
 * then crash it with that SYNC outstanding: the durable store is still empty. */
static void crash_before_sync(struct mem_node *node)
{
    for (unsigned turn = 0; turn < 1000; turn++) {
        const struct mem_step step = mem_node_event(node, NULL);
        CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
        for (size_t j = 0; j < mem_node_effects(node); j++) {
            if (mem_node_effect(node, j)->type == VSR_OP_SYNC) {
                CHECK(mem_store_durable(mem_node_store(node)) == 0);
                mem_node_crash(node);
                return;
            }
            if (mem_node_complete(node, j, VSR_IO_OK))
                break;
        }
    }
    CHECK(false);
}

static void restart(struct mem_node **nodes, uint64_t incarnation, uint64_t now)
{
    CHECK(mem_node_restart(nodes[LEARNER],
                           (struct vsr_id){LEARNER + 1, incarnation}) ==
          VSR_OK);
    tick_node(nodes[LEARNER], now);
}

static void learner_restarts(uint32_t policy, bool before_sync)
{
    const struct vsr_member members[MEMBERS] = {{1, VSR_MEMBER_FULL, 0},
                                                {2, VSR_MEMBER_FULL, 0},
                                                {3, VSR_MEMBER_FULL, 0}};
    const struct vsr_membership membership = {0, members, MEMBERS, 1};
    const struct vsr_span span = {"restart", 7};
    const struct vsr_blob body = {&span, 7, 1, 0};
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[NODES];
    uint64_t now = 0;
    uint64_t committed = 0;
    build(cluster, nodes, &membership, policy);
    drive(cluster, nodes);
    for (uint64_t n = 1; n <= 3; n++)
        committed =
            commit(cluster, nodes, &now, 0, n, VSR_REQUEST_COMMAND, &body);
    CHECK(committed == 3);

    join(cluster, nodes, &membership, policy, now);
    if (before_sync) {
        crash_before_sync(nodes[LEARNER]);
    } else {
        settle_learner(cluster, nodes, &now, VSR_STATE_WARMING, 0, committed);
        CHECK(status(nodes[LEARNER]).applied == committed);
        mem_node_crash(nodes[LEARNER]);
    }
    if (policy == VSR_DURABLE && !before_sync)
        CHECK(mem_store_durable(mem_node_store(nodes[LEARNER])) != 0);
    else
        CHECK(mem_store_durable(mem_node_store(nodes[LEARNER])) == 0);

    /* Two restarts: the first resumes or restarts warm-up, and whatever it
     * persists must let the second boot and resume again. */
    for (uint64_t incarnation = 2; incarnation <= 3; incarnation++) {
        restart(nodes, incarnation, now);
        committed = commit(cluster, nodes, &now, 0, 10 + incarnation,
                           VSR_REQUEST_COMMAND, &body);
        settle_learner(cluster, nodes, &now, VSR_STATE_WARMING, 0, committed);
        CHECK(status(nodes[LEARNER]).role == VSR_MEMBER_FULL);
        CHECK(status(nodes[LEARNER]).applied == committed);
        if (incarnation == 2)
            mem_node_crash(nodes[LEARNER]);
    }

    /* The warmed learner is admitted and executes as a full member. */
    const struct vsr_member admitted[NODES] = {{1, VSR_MEMBER_FULL, 0},
                                               {2, VSR_MEMBER_FULL, 0},
                                               {3, VSR_MEMBER_FULL, 0},
                                               {4, VSR_MEMBER_FULL, 0}};
    const struct vsr_membership next = {1, admitted, NODES, 1};
    committed =
        commit(cluster, nodes, &now, 0, 20, VSR_REQUEST_RECONFIGURE, &next);
    settle_learner(cluster, nodes, &now, VSR_STATE_NORMAL, 1, committed);
    committed = commit(cluster, nodes, &now, 1, 21, VSR_REQUEST_COMMAND, &body);
    settle_learner(cluster, nodes, &now, VSR_STATE_NORMAL, 1, committed);
    for (unsigned round = 0;
         status(nodes[LEARNER]).applied < committed && round < 64; round++) {
        now += 10;
        tick(nodes, now);
        drive(cluster, nodes);
    }
    CHECK(status(nodes[LEARNER]).role == VSR_MEMBER_FULL);
    CHECK(status(nodes[LEARNER]).applied == committed);
    CHECK(mem_node_applied(nodes[LEARNER]) == committed);
    mem_cluster_destroy(cluster);
}

int main(int argc, char **argv)
{
    (void)argv;
    CHECK(argc == 1);
    learner_restarts(VSR_DURABLE, true);
    learner_restarts(VSR_DURABLE, false);
    learner_restarts(VSR_REPLICATED, false);
    return 0;
}
