#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

/* Liveness of a lost-state member recovering into an epoch handoff
 * (docs/protocol.md "Epoch handoff and witnesses": "Learning a later epoch
 * never substitutes for quorum recovery"; "Once installed, a new member can
 * enter NORMAL with phase INSTALLED and contribute to new-epoch quorums";
 * "Authority and quorums": handoff readiness requires q new members and
 * f + 1 new full members with recoverable state).
 *
 * Group: full members 1 and 2, witness 3, f = 1, replicated durability, so
 * q = 2 and readiness needs both full members. One full member is cut off
 * while the others commit a RECONFIGURE into epoch 1 (same members), then
 * crashes and restarts with an empty store. The two survivors install and
 * enter NORMAL with phase INSTALLED, but cannot reach STEADY: the second
 * full promise can only come from the member that lost its state, and that
 * member can only install through quorum recovery in epoch 1, which needs
 * q = 2 responses from the survivors including the primary of the highest
 * view they report.
 *
 * Case (i): the lost member is 1, the designated primary of epoch 1 view 0
 * (lowest full ID). The survivors, NORMAL INSTALLED in view 0 with a silent
 * primary, must change to view 1 so that member 2, now primary, can supply
 * the authoritative offer; member 1 recovers into view 1, installs,
 * promises, and the group reaches STEADY.
 *
 * Case (ii): the lost member is 2. It recovers from the offer of primary 1
 * and witness 3's acknowledgment in view 0, installs, and promises.
 *
 * In both cases a new command must commit afterwards and apply on both full
 * members, and the recovered member's history must match the committed one
 * (the memory host compares every node's committed entries). */

enum { NODES = 3, WITNESS = 2 };

struct schedule {
    uint64_t cut; /* Drop every message from or to this member, or 0. */
};

static struct vsr_status status(struct mem_node *node)
{
    struct vsr_status result;
    vsr_get_status(mem_node_core(node), &result);
    CHECK(result.failure.code == VSR_FAILURE_NONE);
    return result;
}

static bool blocked(const struct schedule *schedule,
                    const struct vsr_message *message, uint64_t to)
{
    return schedule->cut != 0 &&
           (message->from == schedule->cut || to == schedule->cut);
}

static void drive(struct mem_cluster *cluster, struct mem_node **nodes,
                  const struct schedule *schedule)
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
            if (blocked(schedule, message, to)) {
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

/* Drive and advance time until the predicate holds, within a bounded number
 * of rounds. */
typedef bool (*predicate)(struct mem_node **nodes);

static void dump(struct mem_node **nodes)
{
    for (size_t i = 0; i < NODES; i++) {
        const struct vsr_status current = status(nodes[i]);
        fprintf(
            stderr,
            "node %" PRIu64 " state=%u epoch=%" PRIu64 " view=%" PRIu64
            " primary=%" PRIu64 " commit=%" PRIu64 " apply=%" PRIu64
            " phase=%u\n",
            mem_node_id(nodes[i]), current.state, current.epoch, current.view,
            current.primary, current.committed, current.applied,
            current.configuration == NULL ? 0 : current.configuration->phase);
    }
}

static void until(struct mem_cluster *cluster, struct mem_node **nodes,
                  const struct schedule *schedule, uint64_t *now,
                  predicate done, const char *what)
{
    for (unsigned round = 0; !done(nodes); round++) {
        if (round >= 512) {
            fprintf(stderr, "no progress towards %s\n", what);
            dump(nodes);
            CHECK(false);
        }
        drive(cluster, nodes, schedule);
        *now += 10;
        tick(nodes, *now);
    }
    drive(cluster, nodes, schedule);
}

static void submit(struct mem_node *node, uint64_t number, uint64_t route)
{
    const struct vsr_span spans[] = {{"handoff", 7}, {" recovery", 9}};
    const struct vsr_blob command = {spans, 16, 2, 0};
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

static void commit(struct mem_cluster *cluster, struct mem_node **nodes,
                   const struct schedule *schedule, struct mem_node *leader,
                   uint64_t first, uint64_t last)
{
    for (uint64_t number = first; number <= last; number++) {
        submit(leader, number, number);
        drive(cluster, nodes, schedule);
        const struct vsr_reply *result = reply(leader, number);
        CHECK(result != NULL);
        CHECK(result->status == VSR_REPLY_OK);
    }
}

static void build(struct mem_cluster *cluster, struct mem_node **nodes)
{
    const struct vsr_member members[NODES] = {{1, VSR_MEMBER_FULL, 0},
                                              {2, VSR_MEMBER_FULL, 0},
                                              {3, VSR_MEMBER_WITNESS, 0}};
    const struct vsr_membership membership = {0, members, NODES, 1};
    for (uint32_t i = 0; i < NODES; i++) {
        struct vsr_options options = mem_options((uint64_t)i + 1, &membership);
        options.durability = VSR_REPLICATED;
        nodes[i] = mem_cluster_add(cluster, &options);
        tick_node(nodes[i], 0);
    }
}

static void propose_epoch(struct mem_node *leader)
{
    const struct vsr_member members[NODES] = {{1, VSR_MEMBER_FULL, 0},
                                              {2, VSR_MEMBER_FULL, 0},
                                              {3, VSR_MEMBER_WITNESS, 0}};
    const struct vsr_membership next = {1, members, NODES, 1};
    const struct vsr_request request = {
        {{7, 1}, 1}, 0, VSR_REQUEST_RECONFIGURE, 0, &next};
    const struct vsr_event event = {VSR_EVENT_REQUEST, 0, 50, &request, 1};
    CHECK(mem_node_event(leader, &event).consumed == 1);
}

/* The NORMAL primary of the given epoch among the nodes, or NULL. */
static struct mem_node *leader_of(struct mem_node **nodes, uint64_t epoch)
{
    for (size_t i = 0; i < NODES; i++) {
        if (!mem_node_alive(nodes[i]))
            continue;
        const struct vsr_status current = status(nodes[i]);
        if (current.state == VSR_STATE_NORMAL && current.epoch == epoch &&
            current.primary == mem_node_id(nodes[i]))
            return nodes[i];
    }
    return NULL;
}

static bool node_2_leads_epoch_0(struct mem_node **nodes)
{
    const struct vsr_status current = status(nodes[1]);
    return current.state == VSR_STATE_NORMAL && current.epoch == 0 &&
           current.primary == 2;
}

/* The member the current case cuts off and then restarts without state. */
static uint64_t cut_member;

/* Every other member is NORMAL in epoch 1 with phase INSTALLED: installed,
 * voting, and still owed the cut member's promise. */
static bool survivors_installed(struct mem_node **nodes)
{
    for (size_t i = 0; i < NODES; i++) {
        if (mem_node_id(nodes[i]) == cut_member)
            continue;
        const struct vsr_status current = status(nodes[i]);
        if (current.state != VSR_STATE_NORMAL || current.epoch != 1 ||
            current.configuration->phase != VSR_EPOCH_INSTALLED)
            return false;
    }
    return true;
}

static bool all_steady(struct mem_node **nodes)
{
    for (size_t i = 0; i < NODES; i++) {
        const struct vsr_status current = status(nodes[i]);
        if (current.state != VSR_STATE_NORMAL || current.epoch != 1 ||
            current.configuration->phase != VSR_EPOCH_STEADY)
            return false;
    }
    return true;
}

static bool full_members_applied_4(struct mem_node **nodes)
{
    for (size_t i = 0; i < NODES; i++) {
        if (i == WITNESS)
            continue;
        if (status(nodes[i]).applied < 4)
            return false;
    }
    return true;
}

static void lost_member_recovers_into_handoff(uint64_t lost)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[NODES];
    struct schedule schedule = {0};
    uint64_t now = 0;
    struct mem_node *proposer;
    cut_member = lost;
    build(cluster, nodes);
    proposer = nodes[0];
    drive(cluster, nodes, &schedule);
    commit(cluster, nodes, &schedule, nodes[0], 1, 2);
    CHECK(status(nodes[(size_t)lost - 1]).committed == 2);

    /* The lost member stops hearing anyone. When it is the epoch-0 primary,
     * the survivors first elect member 2 in view 1 so the boundary can be
     * proposed and committed without it. */
    schedule.cut = lost;
    if (lost == 1) {
        until(cluster, nodes, &schedule, &now, node_2_leads_epoch_0,
              "member 2 leading epoch 0");
        proposer = nodes[1];
    }
    propose_epoch(proposer);
    until(cluster, nodes, &schedule, &now, survivors_installed,
          "survivors installed in epoch 1");
    CHECK(status(nodes[(size_t)lost - 1]).epoch == 0);
    for (size_t i = 0; i < NODES; i++)
        if (mem_node_id(nodes[i]) != lost)
            CHECK(status(nodes[i]).committed == 3);

    /* The member loses its store, restarts, and is reachable again. */
    mem_node_crash(nodes[(size_t)lost - 1]);
    CHECK(mem_node_restart(nodes[(size_t)lost - 1], (struct vsr_id){lost, 2}) ==
          VSR_OK);
    schedule.cut = 0;
    tick_node(nodes[(size_t)lost - 1], now);
    CHECK(status(nodes[(size_t)lost - 1]).committed == 0);
    CHECK(status(nodes[(size_t)lost - 1]).state != VSR_STATE_NORMAL);

    /* Quorum recovery in epoch 1 completes, the member installs and promises,
     * and the whole group reaches STEADY. */
    until(cluster, nodes, &schedule, &now, all_steady, "STEADY");
    const struct vsr_status recovered = status(nodes[(size_t)lost - 1]);
    CHECK(recovered.committed >= 3);
    if (lost == 1)
        CHECK(recovered.view >= 1);
    const struct vsr_entry *boundary =
        mem_node_history(nodes[(size_t)lost - 1], 3);
    CHECK(boundary != NULL);
    CHECK(boundary->type == VSR_REQUEST_RECONFIGURE);

    /* A new command commits in the new epoch and applies on both full
     * members. */
    struct mem_node *leader = leader_of(nodes, 1);
    CHECK(leader != NULL);
    submit(leader, 4, 4);
    until(cluster, nodes, &schedule, &now, full_members_applied_4,
          "command 4 applied");
    CHECK(reply(leader, 4) != NULL);
    CHECK(reply(leader, 4)->status == VSR_REPLY_OK);
    CHECK(reply(leader, 4)->op == 4);
    mem_cluster_destroy(cluster);
}

int main(int argc, char **argv)
{
    (void)argv;
    CHECK(argc == 1);
    lost_member_recovers_into_handoff(1);
    lost_member_recovers_into_handoff(2);
    return 0;
}
