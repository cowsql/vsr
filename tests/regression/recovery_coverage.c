#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <stdint.h>
#include <string.h>

/* Regression for `tests/fuzzy/cluster 108 1 2000 quiet 1`.
 *
 * A witness restarted in replicated mode recovers from an empty store. The
 * highest-view primary's recovery response named a checkpoint, so the witness
 * adopted that remote anchor, and its RESTORE waited for f+1 full members to
 * advertise coverage of the anchor as if it were trimming retained entries.
 * The full members then changed view; every later CHECKPOINT advertisement
 * carried the new view, and the transition module dropped any message whose
 * view differed from the recovering member's old view before the checkpoint
 * module could count it. The witness stayed RECOVERING at the old view forever
 * while the rest of the cluster ran normally.
 *
 * A RESTORE replaces only a prefix the witness never vouched for, so it needs
 * no promises: the recovering witness must install the anchor and rejoin
 * without any advertisement, and remain a normal member across the next view
 * change. Advertisements are epoch-scoped retention promises and are counted
 * whatever view they carry; the scenario never delivers one to the witness. */

enum { NODES = 3, WITNESS = 2, PRIMARY = 0, BACKUP = 1 };

struct schedule {
    bool hide_coverage;  /* Drop CHECKPOINT advertisements to the witness. */
    bool isolate_backup; /* Drop traffic between the two full members. */
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
    return schedule->isolate_backup &&
           ((message->from == PRIMARY + 1 && to == BACKUP + 1) ||
            (message->from == BACKUP + 1 && to == PRIMARY + 1));
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
 * the committed position, or run a fixed number of rounds when none given. */
static void settle(struct mem_cluster *cluster, struct mem_node **nodes,
                   const struct schedule *schedule, uint64_t *now,
                   uint64_t committed, unsigned rounds)
{
    for (unsigned round = 0; round < (rounds == 0 ? 256u : rounds); round++) {
        drive(cluster, nodes, schedule);
        if (rounds == 0 && converged(nodes, committed))
            return;
        *now += 10;
        tick(nodes, *now);
    }
    if (rounds == 0)
        CHECK(false);
}

static void hint(struct mem_node *node)
{
    const struct vsr_event event = {VSR_EVENT_CHECKPOINT, 0, 0, NULL, 0};
    CHECK(mem_node_event(node, &event).consumed == 1);
}

static void submit(struct mem_node *node, uint64_t op)
{
    const struct vsr_span spans[] = {{"recovery", 8}, {" coverage", 9}};
    const struct vsr_blob command = {spans, 17, 2, 0};
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
        nodes[i] = mem_cluster_add(cluster, &options);
        tick_node(nodes[i], 0);
    }
}

static void recovering_witness_rejoins_without_advertisements(void)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[NODES];
    struct schedule schedule = {false, false};
    uint64_t now = 0;
    build(cluster, nodes);
    drive(cluster, nodes, &schedule);
    commit(cluster, nodes, &schedule, nodes[PRIMARY], 1, 3);
    CHECK(status(nodes[WITNESS]).committed == 3);

    /* Both full members publish an anchor at op 3 and trim behind it. The
     * witness never receives their advertisements: whatever it held before
     * the restart is lost with its store. */
    schedule.hide_coverage = true;
    hint(nodes[PRIMARY]);
    hint(nodes[BACKUP]);
    drive(cluster, nodes, &schedule);
    CHECK(status(nodes[PRIMARY]).checkpoint_op == 3);
    CHECK(status(nodes[BACKUP]).checkpoint_op == 3);
    CHECK(readable_begin(nodes[PRIMARY]) == 4);
    CHECK(readable_begin(nodes[WITNESS]) == 1);

    /* A replicated-mode restart loses the store: the witness recovers from a
     * quorum. The primary's offer names the anchor; installing it replaces
     * nothing the witness prepared, so no retention promise is required. */
    mem_node_crash(nodes[WITNESS]);
    CHECK(mem_node_restart(nodes[WITNESS], (struct vsr_id){3, 2}) == VSR_OK);
    tick_node(nodes[WITNESS], now);
    CHECK(status(nodes[WITNESS]).committed == 0);
    settle(cluster, nodes, &schedule, &now, 3, 0);
    CHECK(status(nodes[WITNESS]).state == VSR_STATE_NORMAL);
    CHECK(status(nodes[WITNESS]).role == VSR_MEMBER_WITNESS);
    CHECK(status(nodes[WITNESS]).checkpoint_op == 3);
    CHECK(readable_begin(nodes[WITNESS]) == 4);

    /* Cut the full members apart until the backup's election timeout moves
     * the group to a later view; the rejoined witness must follow it. */
    schedule.isolate_backup = true;
    for (unsigned round = 0; status(nodes[BACKUP]).view == 0; round++) {
        CHECK(round < 64);
        now += 10;
        tick(nodes, now);
        drive(cluster, nodes, &schedule);
    }
    schedule.isolate_backup = false;
    settle(cluster, nodes, &schedule, &now, 3, 0);
    CHECK(status(nodes[PRIMARY]).view >= 1);
    CHECK(status(nodes[WITNESS]).view == status(nodes[PRIMARY]).view);

    commit(cluster, nodes, &schedule, leader(nodes), 4, 4);
    settle(cluster, nodes, &schedule, &now, 4, 0);
    CHECK(reply(leader(nodes), 4)->status == VSR_REPLY_OK);
    CHECK(status(nodes[WITNESS]).committed == 4);
    mem_cluster_destroy(cluster);
}

int main(int argc, char **argv)
{
    (void)argv;
    CHECK(argc == 1);
    recovering_witness_rejoins_without_advertisements();
    return 0;
}
