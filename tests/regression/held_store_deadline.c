#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>

/* Reproduction of a busy loop found by tests/integration/engine over real
 * engines (docs/handover/engine-phase-2.md): a replica RECOVERING from a
 * lost store (LOAD RECOVERY answered NOT_FOUND) keeps reporting a deadline
 * at or before the TIME it has just consumed for as long as its hard-state
 * STORE is outstanding, so a host that wakes at the deadline polls it in a
 * loop. The I/O layer's store holds that STORE while it creates the log
 * (decision 71), and a RECOVER that finds no store creates one, so an
 * engine spins for the whole creation.
 *
 * transition.c's recovery round sets its retry_at when the round begins
 * and recovery_poll returns early until the hard state is stored
 * (hard_safe), so nothing moves retry_at, and vsr_transition_deadline
 * reports it all the same. Expected to fail until the core is fixed
 * (XFAIL_TESTS). */

int main(int argc, char **argv)
{
    const struct vsr_member members[3] = {{1, VSR_MEMBER_FULL, 0},
                                          {2, VSR_MEMBER_FULL, 0},
                                          {3, VSR_MEMBER_FULL, 0}};
    const struct vsr_membership membership = {0, members, 3, 1};
    struct mem_cluster *cluster = mem_cluster_create();
    struct vsr_options options = mem_options(1, &membership);
    struct mem_node *node;
    uint64_t now = 0;
    bool loaded = false;

    (void)argv;
    CHECK(argc == 1);
    options.start_mode = VSR_START_RECOVER;
    node = mem_cluster_add(cluster, &options);
    for (unsigned round = 0; round < 32; ++round) {
        const struct mem_step step = mem_node_time(node, now);

        CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
        if (loaded && step.consumed == 1 && step.deadline <= now) {
            struct vsr_status status;

            vsr_get_status(mem_node_core(node), &status);
            fprintf(stderr,
                    "t=%" PRIu64 ": deadline %" PRIu64
                    " after TIME consumed, state %u, %zu effects\n",
                    now, step.deadline, status.state, mem_node_effects(node));
            CHECK(false);
        }
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
        (void)mem_node_event(node, NULL);
        now += 1;
    }
    mem_cluster_destroy(cluster);
    return 0;
}
