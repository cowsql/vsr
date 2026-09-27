#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <stdint.h>
#include <string.h>

/* Regression for `tests/fuzzy/cluster 8 1 2000 quiet 5` (also 62 and 190).
 *
 * A witness was installing a new primary's selected history. The primary had
 * never checkpointed, so the transfer compared the witness's retained log from
 * its first entry, one LOAD at a time. Two other full members advertised
 * checkpoints covering the witness's committed prefix, and the checkpoint
 * module published that remote anchor and trimmed the prefix in the middle of
 * the transfer. The next comparison loaded an entry behind the new log_begin,
 * the store answered NOT_FOUND, and the witness latched VSR_FAILURE_STORAGE.
 *
 * The scenario holds the witness's LOADs while both advertisements arrive.
 * Anchor publication must wait for the transfer to finish; afterwards the
 * witness adopts the anchor, trims, and the cluster converges. */

enum { NODES = 4, PRIMARY = 0, DONOR = 1, OTHER = 2, WITNESS = 3 };

struct schedule {
    bool hide_coverage;   /* Drop CHECKPOINT advertisements to the witness. */
    bool isolate_primary; /* Drop all traffic to and from the old primary. */
    bool hold_loads;      /* Leave the witness's LOAD effects pending. */
};

static struct vsr_status status(struct mem_node *node)
{
    struct vsr_status result;
    vsr_get_status(mem_node_core(node), &result);
    CHECK(result.failure.code == VSR_FAILURE_NONE);
    return result;
}

static uint64_t readable_begin(struct mem_node *node)
{
    const struct vsr_recovered *recovered = mem_store_recovered(
        mem_node_store(node), mem_store_readable(mem_node_store(node)));
    return recovered == NULL ? 1 : recovered->log_begin;
}

static bool blocked(const struct schedule *schedule,
                    const struct vsr_message *message, uint64_t to)
{
    if (schedule->hide_coverage && to == WITNESS + 1 &&
        message->type == VSR_MSG_CHECKPOINT)
        return true;
    return schedule->isolate_primary &&
           (message->from == PRIMARY + 1 || to == PRIMARY + 1);
}

static size_t pending_loads(struct mem_node *node)
{
    size_t count = 0;
    for (size_t j = 0; j < mem_node_effects(node); j++)
        if (mem_node_effect(node, j)->type == VSR_OP_LOAD)
            count++;
    return count;
}

static void drive(struct mem_cluster *cluster, struct mem_node **nodes,
                  const struct schedule *schedule)
{
    for (size_t turn = 0; turn < 100000; turn++) {
        bool progress = false;
        for (size_t i = 0; i < NODES; i++) {
            const struct mem_step step = mem_node_event(nodes[i], NULL);
            CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
            if (step.emitted != 0 || (step.flags & VSR_UPDATE_MORE) != 0)
                progress = true;
            for (size_t j = 0; j < mem_node_effects(nodes[i]); j++) {
                if (schedule->hold_loads && i == WITNESS &&
                    mem_node_effect(nodes[i], j)->type == VSR_OP_LOAD)
                    continue;
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
        tick_node(nodes[i], now);
}

static bool converged(struct mem_node **nodes, uint64_t committed)
{
    for (size_t i = 0; i < NODES; i++) {
        const struct vsr_status current = status(nodes[i]);
        if (current.state != VSR_STATE_NORMAL ||
            current.view != status(nodes[DONOR]).view ||
            current.committed < committed)
            return false;
        if (current.role == VSR_MEMBER_FULL && current.applied < committed)
            return false;
    }
    return true;
}

/* Advance logical time in heartbeat steps until every member converged on
 * the committed position. */
static void settle(struct mem_cluster *cluster, struct mem_node **nodes,
                   const struct schedule *schedule, uint64_t *now,
                   uint64_t committed)
{
    for (unsigned round = 0; round < 256; round++) {
        drive(cluster, nodes, schedule);
        if (converged(nodes, committed))
            return;
        *now += 10;
        tick(nodes, *now);
    }
    CHECK(false);
}

static void hint(struct mem_node *node)
{
    const struct vsr_event event = {VSR_EVENT_CHECKPOINT, 0, 0, NULL, 0};
    CHECK(mem_node_event(node, &event).consumed == 1);
}

static void submit(struct mem_node *node, uint64_t op)
{
    const struct vsr_span spans[] = {{"transfer", 8}, {" anchor", 7}};
    const struct vsr_blob command = {spans, 15, 2, 0};
    const struct vsr_request request = {
        {{100, op}, 1}, 0, VSR_REQUEST_COMMAND, 0, &command};
    const struct vsr_event event = {VSR_EVENT_REQUEST, 0, op, &request, 1};
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
    CHECK(false);
    return NULL;
}

static void commit(struct mem_cluster *cluster, struct mem_node **nodes,
                   const struct schedule *schedule, struct mem_node *leader,
                   uint64_t first, uint64_t last)
{
    for (uint64_t op = first; op <= last; op++) {
        submit(leader, op);
        drive(cluster, nodes, schedule);
        CHECK(reply(leader, op)->status == VSR_REPLY_OK);
        CHECK(reply(leader, op)->op == op);
    }
}

static void build(struct mem_cluster *cluster, struct mem_node **nodes)
{
    const struct vsr_member members[NODES] = {{1, VSR_MEMBER_FULL, 0},
                                              {2, VSR_MEMBER_FULL, 0},
                                              {3, VSR_MEMBER_FULL, 0},
                                              {4, VSR_MEMBER_WITNESS, 0}};
    const struct vsr_membership membership = {0, members, NODES, 1};
    for (uint32_t i = 0; i < NODES; i++) {
        struct vsr_options options = mem_options((uint64_t)i + 1, &membership);
        options.durability = VSR_REPLICATED;
        /* A one-entry cache makes every comparison in the transfer a LOAD
         * that the scenario can hold. */
        options.limits.log_cache_entries = 1;
        options.limits.batch_entries = 1;
        nodes[i] = mem_cluster_add(cluster, &options);
        tick_node(nodes[i], 0);
    }
}

static void anchor_waits_for_transfer(void)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[NODES];
    struct schedule schedule = {false, false, false};
    uint64_t now = 0;
    build(cluster, nodes);
    drive(cluster, nodes, &schedule);
    commit(cluster, nodes, &schedule, nodes[PRIMARY], 1, 3);
    CHECK(status(nodes[WITNESS]).committed == 3);

    /* Two full members anchor at op 3; the future primary never does, so its
     * selected history still begins at op 1. Publication advertises at once,
     * so the witness must not see those advertisements yet. */
    schedule.hide_coverage = true;
    hint(nodes[PRIMARY]);
    hint(nodes[OTHER]);
    drive(cluster, nodes, &schedule);
    CHECK(status(nodes[PRIMARY]).checkpoint_op == 3);
    CHECK(status(nodes[OTHER]).checkpoint_op == 3);
    CHECK(status(nodes[DONOR]).checkpoint_op == 0);
    CHECK(readable_begin(nodes[DONOR]) == 1);
    CHECK(readable_begin(nodes[WITNESS]) == 1);

    /* Isolate the old primary so the remaining quorum elects the donor with
     * its own untrimmed log. The witness's transfer then compares from op 1,
     * and its first LOAD stays pending. */
    schedule.isolate_primary = true;
    schedule.hold_loads = true;
    for (unsigned round = 0;
         status(nodes[DONOR]).state != VSR_STATE_NORMAL ||
         status(nodes[DONOR]).view == 0 || pending_loads(nodes[WITNESS]) == 0;
         round++) {
        CHECK(round < 64);
        now += 10;
        tick(nodes, now);
        drive(cluster, nodes, &schedule);
    }
    CHECK(status(nodes[DONOR]).primary == mem_node_id(nodes[DONOR]));
    CHECK(status(nodes[WITNESS]).state == VSR_STATE_VIEW_CHANGE);
    CHECK(readable_begin(nodes[WITNESS]) == 1);

    /* Reconnect the old primary: it follows the new view, and both anchored
     * members advertise coverage of op 3 to the witness mid-transfer. Stay
     * inside the view timeout so the witness keeps this transfer. */
    schedule.isolate_primary = false;
    schedule.hide_coverage = false;
    for (unsigned round = 0; round < 4; round++) {
        now += 10;
        tick(nodes, now);
        drive(cluster, nodes, &schedule);
    }
    CHECK(status(nodes[PRIMARY]).state == VSR_STATE_NORMAL);
    CHECK(status(nodes[PRIMARY]).view == status(nodes[DONOR]).view);
    CHECK(status(nodes[WITNESS]).state == VSR_STATE_VIEW_CHANGE);
    CHECK(pending_loads(nodes[WITNESS]) != 0);

    /* Release the LOADs. The remaining comparisons must find their entries;
     * only then does the witness take the covered anchor and trim. */
    schedule.hold_loads = false;
    settle(cluster, nodes, &schedule, &now, 3);
    CHECK(status(nodes[WITNESS]).state == VSR_STATE_NORMAL);
    CHECK(status(nodes[WITNESS]).checkpoint_op == 3);
    CHECK(readable_begin(nodes[WITNESS]) == 4);

    commit(cluster, nodes, &schedule, nodes[DONOR], 4, 4);
    settle(cluster, nodes, &schedule, &now, 4);
    CHECK(status(nodes[WITNESS]).committed == 4);
    mem_cluster_destroy(cluster);
}

int main(int argc, char **argv)
{
    (void)argv;
    CHECK(argc == 1);
    anchor_waits_for_transfer();
    return 0;
}
