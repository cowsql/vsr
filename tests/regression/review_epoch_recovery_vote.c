#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <stdint.h>
#include <string.h>

/* Review property: a member that lost its safety state rejoins as a voter
 * only through quorum recovery (docs/protocol.md "Restart and persistence",
 * DESIGN.md "Replication and recovery": "Missing durable storage also
 * requires quorum recovery"; "membership hints alone never establish
 * recovery or voting readiness").
 *
 * Scenario: three full members, f = 1, replicated durability. After a
 * reconfiguration into epoch 1 the primary (1) commits two commands with only
 * member 2's acknowledgments while member 3 is cut off. Member 2 then crashes
 * and restarts with an empty store, and member 1 becomes unreachable. In the
 * paper member 2 needs q = 2 recovery responses from the two other members,
 * including the primary of the highest view, so it can never recover while
 * member 1 is away and the two survivors can never form a quorum: the
 * acknowledged commands are safe.
 *
 * Here member 2's RECOVERY carries its seed epoch 0, member 3 answers with a
 * NEW_EPOCH redirect, and the epoch machinery (src/epochs.c begin ->
 * vsr_transition_epoch -> TARGET_EPOCH -> persist_phase) installs member 3's
 * lagging history from that single donor and enters NORMAL. Members 2 and 3
 * then elect a view whose log ends at the boundary, and a new command executes
 * at the op member 1 already acknowledged to its client: the cluster-wide
 * execution table in the memory host reports the divergence. */

enum { NODES = 3, PRIMARY = 0, BACKUP = 1, LAGGING = 2 };

struct schedule {
    bool cut_lagging; /* Drop traffic between members 1 and 3. */
    bool cut_primary; /* Drop every message from or to member 1. */
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
    if (schedule->cut_primary &&
        (message->from == PRIMARY + 1 || to == PRIMARY + 1))
        return true;
    return schedule->cut_lagging &&
           ((message->from == PRIMARY + 1 && to == LAGGING + 1) ||
            (message->from == LAGGING + 1 && to == PRIMARY + 1));
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

static void submit(struct mem_node *node, uint64_t number, uint64_t route)
{
    const struct vsr_span spans[] = {{"epoch", 5}, {" recovery", 9}};
    const struct vsr_blob command = {spans, 14, 2, 0};
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

static bool steady(struct mem_node **nodes)
{
    for (size_t i = 0; i < NODES; i++) {
        const struct vsr_status current = status(nodes[i]);
        if (current.state != VSR_STATE_NORMAL || current.epoch != 1 ||
            current.configuration->phase != VSR_EPOCH_STEADY)
            return false;
    }
    return true;
}

static void build(struct mem_cluster *cluster, struct mem_node **nodes)
{
    const struct vsr_member members[NODES] = {{1, VSR_MEMBER_FULL, 0},
                                              {2, VSR_MEMBER_FULL, 0},
                                              {3, VSR_MEMBER_FULL, 0}};
    const struct vsr_membership membership = {0, members, NODES, 1};
    for (uint32_t i = 0; i < NODES; i++) {
        struct vsr_options options = mem_options((uint64_t)i + 1, &membership);
        options.durability = VSR_REPLICATED;
        nodes[i] = mem_cluster_add(cluster, &options);
        tick_node(nodes[i], 0);
    }
}

static void reconfigure(struct mem_cluster *cluster, struct mem_node **nodes,
                        const struct schedule *schedule, uint64_t *now)
{
    const struct vsr_member members[NODES] = {{1, VSR_MEMBER_FULL, 0},
                                              {2, VSR_MEMBER_FULL, 0},
                                              {3, VSR_MEMBER_FULL, 0}};
    const struct vsr_membership next = {1, members, NODES, 1};
    const struct vsr_request request = {
        {{7, 1}, 1}, 0, VSR_REQUEST_RECONFIGURE, 0, &next};
    const struct vsr_event event = {VSR_EVENT_REQUEST, 0, 50, &request, 1};
    CHECK(mem_node_event(nodes[PRIMARY], &event).consumed == 1);
    for (unsigned round = 0; !steady(nodes); round++) {
        CHECK(round < 256);
        drive(cluster, nodes, schedule);
        *now += 10;
        tick(nodes, *now);
    }
    drive(cluster, nodes, schedule);
}

/* A NORMAL primary among the members that can still talk to each other. */
static struct mem_node *survivor_leader(struct mem_node **nodes)
{
    for (size_t i = BACKUP; i < NODES; i++) {
        const struct vsr_status current = status(nodes[i]);
        if (current.state == VSR_STATE_NORMAL &&
            current.primary == mem_node_id(nodes[i]))
            return nodes[i];
    }
    return NULL;
}

static void lost_member_rejoins_through_epoch_redirect(void)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[NODES];
    struct schedule schedule = {false, false};
    uint64_t now = 0;
    build(cluster, nodes);
    drive(cluster, nodes, &schedule);
    commit(cluster, nodes, &schedule, nodes[PRIMARY], 1, 2);
    reconfigure(cluster, nodes, &schedule, &now);
    CHECK(status(nodes[PRIMARY]).committed == 3);
    CHECK(status(nodes[PRIMARY]).primary == PRIMARY + 1);

    /* Member 3 stops hearing from the primary; commands 4 and 5 commit with
     * member 2's acknowledgment alone and are answered to the client. */
    schedule.cut_lagging = true;
    commit(cluster, nodes, &schedule, nodes[PRIMARY], 4, 5);
    CHECK(reply(nodes[PRIMARY], 4)->op == 4);
    CHECK(reply(nodes[PRIMARY], 5)->op == 5);
    CHECK(status(nodes[BACKUP]).applied == 5);
    CHECK(status(nodes[LAGGING]).committed == 3);

    /* Member 2 loses its store; member 1 becomes unreachable. At most one
     * member lost state, so every acknowledged command must survive. */
    mem_node_crash(nodes[BACKUP]);
    CHECK(mem_node_restart(nodes[BACKUP], (struct vsr_id){2, 2}) == VSR_OK);
    schedule.cut_primary = true;
    tick_node(nodes[BACKUP], now);
    CHECK(status(nodes[BACKUP]).committed == 0);

    /* Only members 2 and 3 exchange messages from here on. Whenever one of
     * them claims to be a normal primary, offer it a new command: it can only
     * execute at op 4 by replacing what member 1 acknowledged, and the host's
     * execution oracle reports that divergence during drive. */
    bool offered = false;
    for (unsigned round = 0; round < 64; round++) {
        drive(cluster, nodes, &schedule);
        struct mem_node *leader = survivor_leader(nodes);
        if (leader != NULL && !offered) {
            submit(leader, 6, 6);
            offered = true;
            drive(cluster, nodes, &schedule);
            const struct vsr_reply *answer = reply(leader, 6);
            if (answer != NULL && answer->status == VSR_REPLY_OK) {
                CHECK(answer->op > 5);
            } else {
                offered = false;
            }
        }
        now += 10;
        tick(nodes, now);
    }
    CHECK(status(nodes[BACKUP]).state != VSR_STATE_NORMAL);
    CHECK(survivor_leader(nodes) == NULL);

    /* Once member 1 is back, everyone converges on the acknowledged history. */
    schedule.cut_primary = false;
    schedule.cut_lagging = false;
    for (unsigned round = 0;; round++) {
        CHECK(round < 256);
        drive(cluster, nodes, &schedule);
        bool converged = true;
        for (size_t i = 0; i < NODES; i++) {
            const struct vsr_status current = status(nodes[i]);
            if (current.state != VSR_STATE_NORMAL || current.applied < 5 ||
                current.view != status(nodes[PRIMARY]).view)
                converged = false;
        }
        if (converged)
            break;
        now += 10;
        tick(nodes, now);
    }
    for (size_t i = 0; i < NODES; i++) {
        const struct vsr_entry *entry = mem_node_history(nodes[i], 4);
        CHECK(entry != NULL);
        CHECK(entry->request.client.lo == 4 && entry->request.number == 1);
    }
    mem_cluster_destroy(cluster);
}

int main(int argc, char **argv)
{
    (void)argv;
    CHECK(argc == 1);
    lost_member_rejoins_through_epoch_redirect();
    return 0;
}
