#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>

/* Reproduction of a liveness bug found by tests/integration/engine over
 * real engines (docs/handover/engine-phase-2.md): a primary stops applying,
 * and so replying, while one backup of three is unreachable and its SENDs
 * complete RETRY, as the I/O layer's link module answers every SEND to a
 * node it has no link to (vsr.h: "SEND failures retry protocol work").
 *
 * The primary applies an entry only after it has issued a COMMIT at least
 * that high to every peer (the boundary rule of protocol.c's poll: "issue
 * the final old-group commit first"). A peer behind by more than one batch
 * only ever gets a PREPARE of its next batch_entries entries, whose
 * committed field is capped at the batch's end; a failed SEND, and every
 * retry_ns the retransmission timer, rewinds the peer's `sent` to what it
 * acknowledged, so the next PREPARE carries the same first batch again and
 * the peer's commit_sent never reaches the commitment. The memory cluster
 * completes every SEND OK at once, which hides it. Expected to fail until
 * the core is fixed (XFAIL_TESTS). */

enum { MEMBERS = 3, DEAD = 3, CLIENT = 330 };

static struct vsr_status status(struct mem_node *node)
{
    struct vsr_status result;

    vsr_get_status(mem_node_core(node), &result);
    CHECK(result.failure.code == VSR_FAILURE_NONE);
    return result;
}

/* Completes every outstanding SEND to the dead member with RETRY, as the
 * engine does retry_ns after it failed (decision 129): called when time
 * advances, never within a drive, where the core would send again at
 * once. */
static void fail_dead_sends(struct mem_node **nodes)
{
    for (size_t i = 0; i < MEMBERS; i++) {
        uint64_t ids[64];
        size_t count = 0;

        if (!mem_node_alive(nodes[i]))
            continue;
        /* Only those outstanding now: each RETRY makes the core send
         * again at once, and those wait for the next time step. */
        for (size_t j = 0; j < mem_node_effects(nodes[i]) && count < 64; j++) {
            const struct vsr_op *op = mem_node_effect(nodes[i], j);

            if (op->type == VSR_OP_SEND && op->arg == DEAD)
                ids[count++] = op->id;
        }
        for (size_t k = 0; k < count; k++) {
            for (size_t j = 0; j < mem_node_effects(nodes[i]); j++) {
                if (mem_node_effect(nodes[i], j)->id == ids[k]) {
                    (void)mem_node_complete(nodes[i], j, VSR_IO_RETRY);
                    break;
                }
            }
        }
    }
}

/* Steps every live node, completes its effects (OK, but SENDs to the dead
 * member, which wait for fail_dead_sends), delivers messages except to the
 * dead one. */
static void drive(struct mem_cluster *cluster, struct mem_node **nodes)
{
    for (size_t turn = 0; turn < 100000; turn++) {
        bool progress = false;

        for (size_t i = 0; i < MEMBERS; i++) {
            if (!mem_node_alive(nodes[i]))
                continue;
            const struct mem_step step = mem_node_event(nodes[i], NULL);
            CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
            if (step.emitted != 0 || (step.flags & VSR_UPDATE_MORE) != 0)
                progress = true;
            for (size_t j = 0; j < mem_node_effects(nodes[i]); j++) {
                const struct vsr_op *op = mem_node_effect(nodes[i], j);

                if (op->type == VSR_OP_SEND && op->arg == DEAD)
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
            if (to == DEAD) {
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

static void tick(struct mem_node **nodes, uint64_t now)
{
    for (size_t i = 0; i < MEMBERS; i++) {
        if (!mem_node_alive(nodes[i]))
            continue;
        for (unsigned attempt = 0;; attempt++) {
            CHECK(attempt < 10000);
            const struct mem_step step = mem_node_time(nodes[i], now);
            CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
            if (step.consumed == 1)
                break;
        }
    }
}

static struct mem_node *leader(struct mem_node **nodes)
{
    for (size_t i = 0; i < MEMBERS; i++)
        if (mem_node_alive(nodes[i]) &&
            status(nodes[i]).state == VSR_STATE_NORMAL &&
            status(nodes[i]).primary == mem_node_id(nodes[i]))
            return nodes[i];
    return NULL;
}

static bool replied(struct mem_node *node, uint64_t route)
{
    for (size_t i = mem_node_replies(node); i > 0; i--) {
        uint64_t found;
        const struct vsr_reply *answer = mem_node_reply(node, i - 1, &found);

        if (found == route && answer->status == VSR_REPLY_OK)
            return true;
    }
    return false;
}

int main(int argc, char **argv)
{
    const struct vsr_member members[MEMBERS] = {{1, VSR_MEMBER_FULL, 0},
                                                {2, VSR_MEMBER_FULL, 0},
                                                {3, VSR_MEMBER_FULL, 0}};
    const struct vsr_membership membership = {0, members, MEMBERS, 1};
    const struct vsr_span span = {"dead", 4};
    const struct vsr_blob body = {&span, 4, 1, 0};
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[MEMBERS] = {NULL, NULL, NULL};
    uint64_t now = 0;

    (void)argv;
    CHECK(argc == 1);
    for (uint32_t i = 0; i < MEMBERS; i++) {
        struct vsr_options options = mem_options((uint64_t)i + 1, &membership);
        nodes[i] = mem_cluster_add(cluster, &options);
    }
    tick(nodes, 0);
    drive(cluster, nodes);
    /* The dead member is gone from the start: every request below runs
     * with it more than a batch (8 entries) behind. */
    mem_node_crash(nodes[DEAD - 1]);
    for (uint64_t number = 1; number <= 24; number++) {
        bool done = false;

        for (unsigned round = 0; round < 64 && !done; round++) {
            struct mem_node *primary = leader(nodes);

            if (primary != NULL && round % 16 == 0) {
                const struct vsr_request request = {
                    {{CLIENT, 1}, number}, 0, VSR_REQUEST_COMMAND, 0, &body};
                const struct vsr_event event = {VSR_EVENT_REQUEST, 0, number,
                                                &request, 1};
                const struct mem_step step = mem_node_event(primary, &event);
                CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
            }
            drive(cluster, nodes);
            if (primary != NULL && replied(primary, number)) {
                done = true;
                break;
            }
            now += 1;
            tick(nodes, now);
            fail_dead_sends(nodes);
            drive(cluster, nodes);
        }
        if (!done) {
            struct vsr_status st = status(leader(nodes));

            fprintf(stderr,
                    "request %" PRIu64 " not replied: committed %" PRIu64
                    " applied %" PRIu64 "\n",
                    number, st.committed, st.applied);
        }
        CHECK(done);
    }
    mem_cluster_destroy(cluster);
    return 0;
}
