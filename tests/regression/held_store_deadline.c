#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>

/* Reproduction of a busy loop found by tests/integration/engine over real
 * engines: a replica RECOVERING from a lost store (LOAD RECOVERY answered
 * NOT_FOUND) kept reporting a deadline at or before the TIME it had just
 * consumed for as long as its hard-state STORE was outstanding, so a host
 * that wakes at the deadline polled it in a loop. The I/O layer's store
 * holds that STORE while it creates the log (decision 71), and a RECOVER
 * that finds no store creates one, so an engine spun for the whole
 * creation.
 *
 * transition.c's recovery round set its retry_at when the round began and
 * recovery_poll returned early until the hard state was stored (hard_safe),
 * so nothing moved retry_at, and vsr_transition_deadline reported it all
 * the same. The same held for a view change's round (the second case
 * below), for a discovery round, for the warm-up timer, for the election
 * timer during a learner's transfer, and for retries that could not be
 * issued for want of an operation slot: a deadline is now reported only
 * while the poll that owns it would act on its expiry, and one that waits
 * for a completion is withheld until that completion's step polls again.
 * The memory cluster checks this at every idle return of every node
 * (lib/memory_cluster.c, submit_graph), so the seeded campaigns cover the
 * class; the checks here are the explicit form of the same rule. */

static const struct vsr_member members[3] = {
    {1, VSR_MEMBER_FULL, 0}, {2, VSR_MEMBER_FULL, 0}, {3, VSR_MEMBER_FULL, 0}};
static const struct vsr_membership membership = {0, members, 3, 1};

static void check_deadline(struct mem_node *node, const struct mem_step *step,
                           uint64_t now)
{
    if ((step->flags & VSR_UPDATE_MORE) != 0)
        return;
    if (step->deadline != VSR_NO_DEADLINE && step->deadline <= now) {
        struct vsr_status status;

        vsr_get_status(mem_node_core(node), &status);
        fprintf(stderr,
                "t=%" PRIu64 ": node %" PRIu64 " idle with deadline %" PRIu64
                ", state %u, %zu effects\n",
                now, mem_node_id(node), step->deadline, status.state,
                mem_node_effects(node));
        CHECK(false);
    }
}

/* A replica RECOVERING from a lost store whose hard-state STORE is held. */
static void lost_store(void)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct vsr_options options = mem_options(1, &membership);
    struct mem_node *node;
    uint64_t now = 0;
    bool loaded = false;

    options.start_mode = VSR_START_RECOVER;
    node = mem_cluster_add(cluster, &options);
    for (unsigned round = 0; round < 32; ++round) {
        struct mem_step step = mem_node_time(node, now);

        CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
        CHECK(step.consumed == 1);
        if (loaded)
            check_deadline(node, &step, now);
        /* The store answers the RECOVERY load (nothing survived) and holds
         * everything else, as a store creating its log does. */
        for (size_t j = 0; j < mem_node_effects(node); j++) {
            const struct vsr_op *op = mem_node_effect(node, j);

            if (op->type == VSR_OP_LOAD) {
                CHECK(mem_node_complete(node, j, VSR_IO_NOT_FOUND));
                loaded = true;
                break;
            }
        }
        step = mem_node_event(node, NULL);
        CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
        if (loaded)
            check_deadline(node, &step, now);
        now += 1;
    }
    CHECK(loaded);
    mem_cluster_destroy(cluster);
}

/* Steps every live node, completes every effect except the STOREs of the
 * held node, and delivers messages except those to a crashed node. */
static void run_holding(struct mem_cluster *cluster, struct mem_node **nodes,
                        struct mem_node *held)
{
    for (size_t turn = 0; turn < 100000; turn++) {
        bool progress = false;

        for (size_t i = 0; i < 3; i++) {
            if (!mem_node_alive(nodes[i]))
                continue;
            const struct mem_step step = mem_node_event(nodes[i], NULL);
            CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
            if (step.emitted != 0 || (step.flags & VSR_UPDATE_MORE) != 0)
                progress = true;
            for (size_t j = 0; j < mem_node_effects(nodes[i]); j++) {
                const struct vsr_op *op = mem_node_effect(nodes[i], j);

                if (nodes[i] == held && op->type == VSR_OP_STORE)
                    continue;
                if (mem_node_complete(nodes[i], j, VSR_IO_OK)) {
                    progress = true;
                    break;
                }
            }
        }
        for (size_t i = 0; i < mem_cluster_messages(cluster); i++) {
            uint64_t to;

            (void)mem_cluster_message(cluster, i, &to);
            if (!mem_node_alive(nodes[to - 1])) {
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

static uint32_t state_of(struct mem_node *node)
{
    struct vsr_status status;

    vsr_get_status(mem_node_core(node), &status);
    CHECK(status.failure.code == VSR_FAILURE_NONE);
    return status.state;
}

/* A backup whose primary went silent starts a view change; the STORE of
 * its view fence is held, so its round cannot send yet. */
static void held_view_change(void)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[3];
    uint64_t now = 0;
    bool changing = false;

    for (uint32_t i = 0; i < 3; i++) {
        struct vsr_options options = mem_options((uint64_t)i + 1, &membership);

        nodes[i] = mem_cluster_add(cluster, &options);
        CHECK(mem_node_time(nodes[i], now).consumed == 1);
    }
    run_holding(cluster, nodes, NULL);
    for (uint32_t i = 0; i < 3; i++)
        CHECK(state_of(nodes[i]) == VSR_STATE_NORMAL);
    mem_node_crash(nodes[0]);
    for (now = 1; now <= 150; now++) {
        for (uint32_t i = 1; i < 3; i++) {
            struct mem_step step;

            for (unsigned attempt = 0;; attempt++) {
                CHECK(attempt < 10000);
                step = mem_node_time(nodes[i], now);
                CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
                if (step.consumed == 1)
                    break;
            }
            check_deadline(nodes[i], &step, now);
        }
        run_holding(cluster, nodes, nodes[1]);
        for (uint32_t i = 1; i < 3; i++) {
            const struct mem_step step = mem_node_event(nodes[i], NULL);

            CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
            check_deadline(nodes[i], &step, now);
        }
        if (state_of(nodes[1]) == VSR_STATE_VIEW_CHANGE)
            changing = true;
    }
    CHECK(changing);
    mem_cluster_destroy(cluster);
}

int main(int argc, char **argv)
{
    (void)argv;
    CHECK(argc == 1);
    lost_store();
    held_view_change();
    return 0;
}
