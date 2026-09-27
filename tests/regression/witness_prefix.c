#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <stdint.h>
#include <string.h>

/* Regression for `tests/fuzzy/cluster 273 1 2000 quiet 4` (also profile 5).
 *
 * A three-member group lost its second full member to a replicated-mode
 * restart while the witness, which had prepared op 1 but never learned its
 * commitment, was changing view. The surviving full member had anchored at
 * op 1 and trimmed. Its START_VIEW offer therefore began at op 2, so the
 * witness could not compare op 1 against the donor and adopted the anchor
 * instead. That RESTORE waited for f+1 full members to advertise coverage,
 * the recovering member could not advertise before recovering, and recovery
 * needed the witness normal to reach a quorum of responders. Election
 * timeouts advanced the view a hundred times without progress.
 *
 * A proposal is identified by its epoch, view, and op, so the witness can
 * check its own boundary entry against the anchor without the donor: a match
 * proves the retained prefix is the committed one and nothing needs an
 * anchor. The scenario requires the witness to keep op 1, rejoin, answer the
 * recovery, and commit again. */

enum { NODES = 3, PRIMARY = 0, BACKUP = 1, WITNESS = 2 };

struct schedule {
    bool hide_commit;    /* Drop COMMIT from the primary to the witness. */
    bool delay_recovery; /* Drop all traffic from the recovering member. */
};

static struct vsr_status status(struct mem_node *node)
{
    struct vsr_status result;
    vsr_get_status(mem_node_core(node), &result);
    CHECK(result.failure.code == VSR_FAILURE_NONE);
    return result;
}

static const struct vsr_recovered *readable(struct mem_node *node)
{
    const struct vsr_recovered *recovered = mem_store_recovered(
        mem_node_store(node), mem_store_readable(mem_node_store(node)));
    CHECK(recovered != NULL);
    return recovered;
}

/* RESTORE stores completed by the witness; a verified prefix needs none. */
static size_t witness_restores;

static size_t observe(struct mem_node *node, size_t index)
{
    const struct vsr_op *op = mem_node_effect(node, index);
    size_t count = 0;
    if (op->type != VSR_OP_STORE)
        return 0;
    const struct vsr_store *store = op->data;
    for (uint32_t i = 0; i < store->count; i++)
        if (store->changes[i].type == VSR_STORE_RESTORE_CHECKPOINT)
            count++;
    return count;
}

static bool blocked(const struct schedule *schedule,
                    const struct vsr_message *message, uint64_t to)
{
    if (schedule->hide_commit && message->from == PRIMARY + 1 &&
        to == WITNESS + 1 && message->type == VSR_MSG_COMMIT)
        return true;
    return schedule->delay_recovery && message->from == BACKUP + 1;
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
                const size_t restores = i == WITNESS ? observe(nodes[i], j) : 0;
                if (mem_node_complete(nodes[i], j, VSR_IO_OK)) {
                    witness_restores += restores;
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

static bool converged(struct mem_node **nodes, uint64_t committed)
{
    for (size_t i = 0; i < NODES; i++) {
        const struct vsr_status current = status(nodes[i]);
        if (current.state != VSR_STATE_NORMAL ||
            current.view != status(nodes[PRIMARY]).view ||
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
    const struct vsr_span spans[] = {{"witness", 7}, {" prefix", 7}};
    const struct vsr_blob command = {spans, 14, 2, 0};
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

static struct mem_node *leader(struct mem_node **nodes)
{
    for (size_t i = 0; i < NODES; i++)
        if (status(nodes[i]).primary == mem_node_id(nodes[i]))
            return nodes[i];
    CHECK(false);
    return NULL;
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
        /* The seed ran with a one-entry cache: the boundary entry check may
         * have to read the store rather than the cache. */
        options.limits.log_cache_entries = 1;
        options.limits.batch_entries = 1;
        nodes[i] = mem_cluster_add(cluster, &options);
        tick_node(nodes[i], 0);
    }
}

static void witness_keeps_verified_prefix(void)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[NODES];
    struct schedule schedule = {false, false};
    uint64_t now = 0;
    build(cluster, nodes);
    drive(cluster, nodes, &schedule);

    /* The witness prepares op 1 but never hears that it committed. */
    schedule.hide_commit = true;
    commit(cluster, nodes, &schedule, nodes[PRIMARY], 1, 1);
    CHECK(status(nodes[WITNESS]).committed == 0);
    CHECK(readable(nodes[WITNESS])->log_end == 2);

    /* The primary anchors at op 1 and trims it away. */
    hint(nodes[PRIMARY]);
    drive(cluster, nodes, &schedule);
    CHECK(status(nodes[PRIMARY]).checkpoint_op == 1);
    CHECK(readable(nodes[PRIMARY])->log_begin == 2);

    /* The backup loses its store. Its recovery traffic is delayed until the
     * witness, still without heartbeats, has started a view change. */
    mem_node_crash(nodes[BACKUP]);
    CHECK(mem_node_restart(nodes[BACKUP], (struct vsr_id){2, 2}) == VSR_OK);
    tick_node(nodes[BACKUP], now);
    CHECK(status(nodes[BACKUP]).committed == 0);
    schedule.delay_recovery = true;
    for (unsigned round = 0; status(nodes[WITNESS]).view == 0; round++) {
        CHECK(round < 64);
        now += 10;
        tick(nodes, now);
        drive(cluster, nodes, &schedule);
    }
    CHECK(status(nodes[WITNESS]).state == VSR_STATE_VIEW_CHANGE);
    schedule.hide_commit = false;
    schedule.delay_recovery = false;

    /* The surviving full member wins a later view with its trimmed history.
     * The witness must recognize op 1 as the anchor's own boundary entry,
     * keep it, rejoin, and let the backup recover through both of them. */
    settle(cluster, nodes, &schedule, &now, 1);
    CHECK(status(nodes[WITNESS]).state == VSR_STATE_NORMAL);
    CHECK(status(nodes[WITNESS]).committed == 1);
    CHECK(witness_restores == 0);
    CHECK(status(nodes[BACKUP]).state == VSR_STATE_NORMAL);
    CHECK(status(nodes[BACKUP]).applied == 1);
    CHECK(leader(nodes) == nodes[PRIMARY]);

    commit(cluster, nodes, &schedule, nodes[PRIMARY], 2, 2);
    settle(cluster, nodes, &schedule, &now, 2);
    CHECK(status(nodes[WITNESS]).committed == 2);
    CHECK(status(nodes[BACKUP]).applied == 2);
    CHECK(witness_restores == 0);
    mem_cluster_destroy(cluster);
}

int main(int argc, char **argv)
{
    (void)argv;
    CHECK(argc == 1);
    witness_keeps_verified_prefix();
    return 0;
}
