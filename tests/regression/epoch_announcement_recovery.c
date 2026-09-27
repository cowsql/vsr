#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <stdint.h>
#include <string.h>

/* Contract: a replicated-mode member that lost its store rejoins only through
 * quorum recovery, and recovery preserves the greatest known committed prefix
 * (docs/protocol.md "Restart and persistence": "Missing state requires quorum
 * recovery"; "A newly learned view or epoch invalidates incompatible responses
 * and restarts validation"; DESIGN.md "Replication and recovery": "Missing
 * durable storage also requires quorum recovery").
 *
 * Origin: tests/fuzzy/cluster 108 1 600 quiet 127. There a full member that
 * had committed 16 operations crashed with an empty store, restarted with its
 * seed epoch, obtained an epoch-0 recovery quorum and was installing the
 * primary's offer when the group committed a RECONFIGURE boundary and entered
 * epoch 1. The primary's START_EPOCH announcement reached the member while it
 * was still RECOVERING: src/epochs.c vsr_epochs_event took the pending
 * EPOCH_NEW_FENCE path, begin() abandoned the recovery round
 * (vsr_transition_epoch_entered) and vsr_transition_epoch started a single
 * donor transfer, so the member reported TRANSITIONING in epoch 1 with
 * committed 3, below the 16 it had acknowledged before the crash. The path is
 * the one review_epoch_recovery_vote reproduces through a NEW_EPOCH redirect;
 * this test reaches it through the announcement every member of the old or
 * new group receives until the handoff is STEADY, with NEW_EPOCH withheld.
 *
 * Scenario: full members 1 and 3 and witness 2, f = 1, replicated durability.
 * Member 3 commits three commands, then crashes and loses its store. Members
 * 1 and 2 commit a RECONFIGURE into epoch 1 (boundary op 4); STEADY needs
 * f + 1 full promises, so they keep announcing START_EPOCH to member 3.
 * Member 3 restarts RECOVERING in epoch 0; its NEW_EPOCH redirects are lost so
 * only START_EPOCH carries the epoch. It must leave RECOVERING only after a
 * recovery quorum in the learned epoch, hence with committed >= 3, and then
 * converge with the group. Expected to fail until the core restarts recovery
 * validation in the learned epoch instead of installing from one donor. */

enum { NODES = 3, PRIMARY = 0, WITNESS = 1, LOST = 2, COMMANDS = 3 };

static struct vsr_status status(struct mem_node *node)
{
    struct vsr_status result;
    vsr_get_status(mem_node_core(node), &result);
    CHECK(result.failure.code == VSR_FAILURE_NONE);
    return result;
}

static bool recovering(const struct vsr_status *current)
{
    return current->state == VSR_STATE_STARTING ||
           current->state == VSR_STATE_RECOVERING ||
           current->state == VSR_STATE_WARMING;
}

/* The lost member's first status outside recovery, observed after every
 * delivery like the simulator's oracle: the handoff and the donor transfer
 * both complete within one quiescent run. */
static struct {
    bool active;
    bool left;
    struct vsr_status first;
} watch;

static void observe(struct mem_node *node)
{
    if (!watch.active || watch.left || !mem_node_alive(node))
        return;
    const struct vsr_status current = status(node);
    if (recovering(&current))
        return;
    watch.left = true;
    watch.first = current;
}

/* A dead member receives nothing; the lost member's NEW_EPOCH redirects are
 * dropped so that the epoch reaches it only through START_EPOCH. */
static bool blocked(struct mem_node **nodes, const struct vsr_message *message,
                    uint64_t to)
{
    CHECK(to >= 1 && to <= NODES);
    if (!mem_node_alive(nodes[to - 1]))
        return true;
    return to == LOST + 1 && message->type == VSR_MSG_NEW_EPOCH;
}

static void drive(struct mem_cluster *cluster, struct mem_node **nodes)
{
    for (size_t turn = 0; turn < 100000; turn++) {
        bool progress = false;
        for (size_t i = 0; i < NODES; i++) {
            if (!mem_node_alive(nodes[i]))
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
            if (blocked(nodes, message, to)) {
                mem_cluster_drop(cluster, i);
                progress = true;
                break;
            }
            if (mem_cluster_deliver(cluster, i)) {
                progress = true;
                break;
            }
        }
        mem_cluster_check(cluster);
        observe(nodes[LOST]);
        if (!progress)
            return;
    }
    CHECK(false);
}

static void tick_node(struct mem_node *node, uint64_t now)
{
    /* Pending output may consume the work quantum before TIME admission. */
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
        if (mem_node_alive(nodes[i]))
            tick_node(nodes[i], now);
}

static void submit(struct mem_node *node, uint64_t number, uint64_t route)
{
    const struct vsr_span spans[] = {{"epoch", 5}, {" announced", 10}};
    const struct vsr_blob command = {spans, 15, 2, 0};
    const struct vsr_request request = {{{100, number}, 1},
                                        status(node).epoch,
                                        VSR_REQUEST_COMMAND,
                                        0,
                                        &command};
    const struct vsr_event event = {VSR_EVENT_REQUEST, 0, route, &request, 1};
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

static void build(struct mem_cluster *cluster, struct mem_node **nodes)
{
    const struct vsr_member members[NODES] = {{1, VSR_MEMBER_FULL, 0},
                                              {2, VSR_MEMBER_WITNESS, 0},
                                              {3, VSR_MEMBER_FULL, 0}};
    const struct vsr_membership membership = {0, members, NODES, 1};
    for (uint32_t i = 0; i < NODES; i++) {
        struct vsr_options options = mem_options((uint64_t)i + 1, &membership);
        options.durability = VSR_REPLICATED;
        nodes[i] = mem_cluster_add(cluster, &options);
        tick_node(nodes[i], 0);
    }
}

static bool entered(struct mem_node *node, uint64_t epoch, uint64_t committed)
{
    const struct vsr_status current = status(node);
    return current.state == VSR_STATE_NORMAL && current.epoch == epoch &&
           current.committed >= committed;
}

static bool steady(struct mem_node **nodes)
{
    for (size_t i = 0; i < NODES; i++) {
        const struct vsr_status current = status(nodes[i]);
        if (current.state != VSR_STATE_NORMAL || current.epoch != 1 ||
            current.configuration->phase != VSR_EPOCH_STEADY ||
            (i != WITNESS && current.applied < COMMANDS + 1))
            return false;
    }
    return true;
}

static void announcement_must_not_preempt_recovery(void)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[NODES];
    uint64_t now = 0;
    build(cluster, nodes);
    drive(cluster, nodes);
    for (uint64_t number = 1; number <= COMMANDS; number++) {
        submit(nodes[PRIMARY], number, number);
        drive(cluster, nodes);
        CHECK(reply(nodes[PRIMARY], number) != NULL);
        CHECK(reply(nodes[PRIMARY], number)->status == VSR_REPLY_OK);
    }
    CHECK(status(nodes[PRIMARY]).primary == PRIMARY + 1);
    const uint64_t floor = status(nodes[LOST]).committed;
    CHECK(floor == COMMANDS);

    /* Member 3 loses its store. The survivors commit the boundary at op 4 and
     * enter epoch 1; the handoff cannot become STEADY without a second full
     * member's promise, so they keep announcing START_EPOCH to member 3. */
    mem_node_crash(nodes[LOST]);
    const struct vsr_member members[NODES] = {{1, VSR_MEMBER_FULL, 0},
                                              {2, VSR_MEMBER_WITNESS, 0},
                                              {3, VSR_MEMBER_FULL, 0}};
    const struct vsr_membership next = {1, members, NODES, 1};
    const struct vsr_request request = {
        {{7, 1}, 1}, 0, VSR_REQUEST_RECONFIGURE, 0, &next};
    const struct vsr_event event = {VSR_EVENT_REQUEST, 0, 50, &request, 1};
    CHECK(mem_node_event(nodes[PRIMARY], &event).consumed == 1);
    for (unsigned round = 0; !entered(nodes[PRIMARY], 1, COMMANDS + 1) ||
                             !entered(nodes[WITNESS], 1, COMMANDS + 1);
         round++) {
        CHECK(round < 256);
        drive(cluster, nodes);
        now += 10;
        tick(nodes, now);
    }
    CHECK(status(nodes[PRIMARY]).configuration->phase != VSR_EPOCH_STEADY);

    /* Member 3 restarts with an empty store and enters RECOVERING in its seed
     * epoch. The first observation outside recovery must show at least the
     * commitment it had acknowledged before the crash: a recovery quorum in
     * any epoch includes a primary whose log holds that prefix. */
    CHECK(mem_node_restart(nodes[LOST], (struct vsr_id){LOST + 1, 2}) ==
          VSR_OK);
    tick_node(nodes[LOST], now);
    const struct vsr_status restarted = status(nodes[LOST]);
    CHECK(recovering(&restarted) && restarted.committed == 0);
    watch.active = true;
    for (unsigned round = 0; round < 256 && !watch.left; round++) {
        drive(cluster, nodes);
        now += 10;
        tick(nodes, now);
    }
    CHECK(watch.left);
    CHECK(watch.first.committed >= floor);

    /* The group then reaches STEADY in epoch 1 with the same history. */
    for (unsigned round = 0; !steady(nodes); round++) {
        CHECK(round < 256);
        drive(cluster, nodes);
        now += 10;
        tick(nodes, now);
    }
    for (uint64_t op = 1; op <= COMMANDS; op++) {
        const struct vsr_entry *entry = mem_node_history(nodes[LOST], op);
        CHECK(entry != NULL);
        CHECK(entry->request.client.lo == op && entry->request.number == 1);
    }
    mem_cluster_destroy(cluster);
}

int main(int argc, char **argv)
{
    (void)argv;
    CHECK(argc == 1);
    announcement_must_not_preempt_recovery();
    return 0;
}
