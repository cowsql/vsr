#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <stdint.h>

/* Regression for the idle-learner observation of the seeded scheduler.
 *
 * A warmed learner transferred state once, at join, and then neither
 * received PREPARE/COMMIT nor rediscovered: its committed and applied
 * positions stayed where warm-up left them, and it learned later epochs only
 * once a reconfiguration named it. DESIGN.md makes warm-up exist to reduce
 * the transfer required at the boundary, so warm-up must be continuous.
 *
 * An idle learner, full or witness, must keep up with commands committed
 * after its join, follow a later epoch it was not named in and reach STEADY
 * there, keep following in that epoch, and finally be admitted with only the
 * remaining suffix left to transfer. It never votes on the way. A learner
 * whose seed already names that later epoch (the scheduler's seed 107 with
 * profile 120) discovers it as TRANSFERRING and must likewise establish
 * STEADY there once warmed, by collecting the members' promises. */

enum { MEMBERS = 3, LEARNER = 3, NODES = 4, CLIENT = 220 };

/* Set once the learner's admission is proposed; until then it may not vote. */
static bool admitted;

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
            if (nodes[i] == NULL)
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
            uint64_t to;
            const struct vsr_message *message =
                mem_cluster_message(cluster, i, &to);
            /* A learner never votes, acknowledges, or answers probes. */
            if (!admitted && message->from == LEARNER + 1)
                CHECK(message->type != VSR_MSG_PREPARE_OK &&
                      message->type != VSR_MSG_START_VIEW_CHANGE &&
                      message->type != VSR_MSG_DO_VIEW_CHANGE &&
                      message->type != VSR_MSG_RECOVERY_RESPONSE &&
                      message->type != VSR_MSG_READ_ACK);
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
        if (nodes[i] != NULL)
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

/* Tick until the learner reports the state, epoch, STEADY phase, commitment,
 * and (for a full learner) application expected, or fail. */
static void settle_learner(struct mem_cluster *cluster, struct mem_node **nodes,
                           uint64_t *now, uint32_t state, uint64_t epoch,
                           uint64_t committed, uint32_t role)
{
    for (unsigned round = 0; round < 256; round++) {
        drive(cluster, nodes);
        const struct vsr_status current = status(nodes[LEARNER]);
        if (current.state == state && current.epoch == epoch &&
            current.configuration != NULL &&
            current.configuration->phase == VSR_EPOCH_STEADY &&
            current.committed >= committed && current.role == role &&
            (role != VSR_MEMBER_FULL || current.applied >= committed))
            return;
        *now += 10;
        tick(nodes, *now);
    }
    CHECK(false);
}

static void join(struct mem_cluster *cluster, struct mem_node **nodes,
                 const struct vsr_membership *seed, uint32_t role, uint64_t now)
{
    struct vsr_options options = mem_options(LEARNER + 1, seed);
    options.start_mode = VSR_START_JOIN;
    options.join_role = role;
    nodes[LEARNER] = mem_cluster_add(cluster, &options);
    tick_node(nodes[LEARNER], now);
}

static void learner_follows(uint32_t role, bool late)
{
    const struct vsr_member members[MEMBERS] = {{1, VSR_MEMBER_FULL, 0},
                                                {2, VSR_MEMBER_FULL, 0},
                                                {3, VSR_MEMBER_FULL, 0}};
    const struct vsr_membership membership = {0, members, MEMBERS, 1};
    const struct vsr_span span = {"follow", 6};
    const struct vsr_blob body = {&span, 6, 1, 0};
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[NODES];
    uint64_t now = 0;
    uint64_t committed = 0;
    for (uint32_t i = 0; i < MEMBERS; i++) {
        struct vsr_options options = mem_options((uint64_t)i + 1, &membership);
        nodes[i] = mem_cluster_add(cluster, &options);
        tick_node(nodes[i], 0);
    }
    nodes[LEARNER] = NULL;
    drive(cluster, nodes);
    for (uint64_t n = 1; n <= 3; n++)
        committed =
            commit(cluster, nodes, &now, 0, n, VSR_REQUEST_COMMAND, &body);
    CHECK(committed == 3);

    if (!late) {
        join(cluster, nodes, &membership, role, now);
        settle_learner(cluster, nodes, &now, VSR_STATE_WARMING, 0, committed,
                       role);
    }

    /* Commands committed while the learner idles must reach it. */
    for (uint64_t n = 4; n <= 6; n++)
        committed =
            commit(cluster, nodes, &now, 0, n, VSR_REQUEST_COMMAND, &body);
    CHECK(committed == 6);
    if (!late)
        settle_learner(cluster, nodes, &now, VSR_STATE_WARMING, 0, committed,
                       role);

    /* A later epoch the learner is not named in must reach it too, and it
     * establishes STEADY there by collecting the members' promises; so must
     * a learner joining with that epoch as its seed. */
    const struct vsr_membership bumped = {1, members, MEMBERS, 1};
    committed =
        commit(cluster, nodes, &now, 0, 7, VSR_REQUEST_RECONFIGURE, &bumped);
    if (late)
        join(cluster, nodes, &bumped, role, now);
    settle_learner(cluster, nodes, &now, VSR_STATE_WARMING, 1, committed, role);
    committed = commit(cluster, nodes, &now, 1, 8, VSR_REQUEST_COMMAND, &body);
    settle_learner(cluster, nodes, &now, VSR_STATE_WARMING, 1, committed, role);

    /* Admission in the warm-up role, then a command executed as a member. */
    const struct vsr_member admitted_members[NODES] = {{1, VSR_MEMBER_FULL, 0},
                                                       {2, VSR_MEMBER_FULL, 0},
                                                       {3, VSR_MEMBER_FULL, 0},
                                                       {4, role, 0}};
    const struct vsr_membership next = {2, admitted_members, NODES, 1};
    admitted = true;
    committed =
        commit(cluster, nodes, &now, 1, 9, VSR_REQUEST_RECONFIGURE, &next);
    settle_learner(cluster, nodes, &now, VSR_STATE_NORMAL, 2, committed, role);
    committed = commit(cluster, nodes, &now, 2, 10, VSR_REQUEST_COMMAND, &body);
    settle_learner(cluster, nodes, &now, VSR_STATE_NORMAL, 2, committed, role);
    if (role == VSR_MEMBER_FULL)
        CHECK(mem_node_applied(nodes[LEARNER]) == committed);
    else
        CHECK(status(nodes[LEARNER]).applied == 0);
    mem_cluster_destroy(cluster);
}

int main(int argc, char **argv)
{
    (void)argv;
    CHECK(argc == 1);
    learner_follows(VSR_MEMBER_FULL, false);
    admitted = false;
    learner_follows(VSR_MEMBER_WITNESS, false);
    admitted = false;
    learner_follows(VSR_MEMBER_FULL, true);
    admitted = false;
    learner_follows(VSR_MEMBER_WITNESS, true);
    return 0;
}
