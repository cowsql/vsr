#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <stdint.h>
#include <string.h>

/* Regression for `tests/fuzzy/cluster 1017 1 2000 quiet 0`.
 *
 * A primary published a checkpoint and stored its TRIM while one backup was
 * partitioned. In durable mode the TRIM is readable before it is durable, and
 * the core only moved log_begin once it was durable. The retry path resent
 * PREPAREs from the backup's acknowledged position through a LOAD naming the
 * readable revision, which no longer retained that prefix, so the expected
 * NOT_FOUND latched VSR_FAILURE_STORAGE. Once the trim was durable the primary
 * refused the load instead and never contacted the peer again, so the backup
 * could not learn that it had to fetch state.
 *
 * The scenario holds the trim's SYNC to keep that window open while the
 * partition heals: the primary must stay healthy, steer the backup to state
 * transfer with its commit number, and every member must converge. A witness
 * variant checks that the lagging member adopts the remote anchor only. */

enum { NODES = 3, LAGGING = 2, PRIMARY = 0 };

struct trace {
    size_t fetches;
    size_t installs;
};

struct schedule {
    uint64_t isolated;     /* Replica whose messages are dropped; 0 for none. */
    struct mem_node *held; /* Node whose TRIM stays readable but not durable. */
};

static struct vsr_status status(struct mem_node *node)
{
    struct vsr_status result;
    vsr_get_status(mem_node_core(node), &result);
    CHECK(result.failure.code == VSR_FAILURE_NONE);
    return result;
}

static uint64_t log_begin(struct mem_node *node, uint64_t sequence)
{
    const struct vsr_recovered *recovered =
        mem_store_recovered(mem_node_store(node), sequence);
    return recovered == NULL ? 1 : recovered->log_begin;
}

static uint64_t readable_begin(struct mem_node *node)
{
    return log_begin(node, mem_store_readable(mem_node_store(node)));
}

static uint64_t durable_begin(struct mem_node *node)
{
    return log_begin(node, mem_store_durable(mem_node_store(node)));
}

static void drive(struct mem_cluster *cluster, struct mem_node **nodes,
                  const struct schedule *schedule, struct trace *traces)
{
    for (size_t turn = 0; turn < 100000; turn++) {
        bool progress = false;
        for (size_t i = 0; i < NODES; i++) {
            const struct mem_step step = mem_node_event(nodes[i], NULL);
            CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
            if (step.emitted != 0 || (step.flags & VSR_UPDATE_MORE) != 0)
                progress = true;
            for (size_t j = 0; j < mem_node_effects(nodes[i]); j++) {
                const uint32_t type = mem_node_effect(nodes[i], j)->type;
                if (type == VSR_OP_SYNC && schedule->held == nodes[i] &&
                    readable_begin(nodes[i]) != durable_begin(nodes[i]))
                    continue;
                if (mem_node_complete(nodes[i], j, VSR_IO_OK)) {
                    if (type == VSR_OP_SNAPSHOT_FETCH)
                        traces[i].fetches++;
                    if (type == VSR_OP_SNAPSHOT_INSTALL)
                        traces[i].installs++;
                    progress = true;
                    break;
                }
            }
        }
        for (size_t i = 0; i < mem_cluster_messages(cluster); i++) {
            uint64_t to;
            const struct vsr_message *message =
                mem_cluster_message(cluster, i, &to);
            if (schedule->isolated != 0 &&
                (to == schedule->isolated ||
                 message->from == schedule->isolated)) {
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
    for (size_t i = 0; i < NODES; i++) {
        /* Pending output may consume the work quantum before TIME admission. */
        unsigned attempt;
        for (attempt = 0; attempt < 10000; attempt++) {
            const struct mem_step step = mem_node_time(nodes[i], now);
            CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
            if (step.consumed == 1)
                break;
        }
        CHECK(attempt < 10000);
    }
}

static bool converged(struct mem_node **nodes, uint64_t committed)
{
    const struct vsr_status leader = status(nodes[PRIMARY]);
    if (leader.state != VSR_STATE_NORMAL || leader.committed != committed)
        return false;
    for (size_t i = 0; i < NODES; i++) {
        const struct vsr_status current = status(nodes[i]);
        if (current.state != VSR_STATE_NORMAL || current.view != leader.view ||
            current.committed != committed)
            return false;
        if (current.role == VSR_MEMBER_FULL &&
            (current.applied != committed ||
             mem_node_checksum(nodes[i]) != mem_node_checksum(nodes[PRIMARY])))
            return false;
    }
    return true;
}

/* Advance logical time in heartbeat steps until every member converged on
 * the committed position, or run a fixed number of rounds when none given. */
static void settle(struct mem_cluster *cluster, struct mem_node **nodes,
                   const struct schedule *schedule, struct trace *traces,
                   uint64_t *now, uint64_t committed, unsigned rounds)
{
    for (unsigned round = 0; round < (rounds == 0 ? 64u : rounds); round++) {
        drive(cluster, nodes, schedule, traces);
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
    const struct vsr_span spans[] = {{"trimmed", 7}, {" resend", 7}};
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
                   const struct schedule *schedule, struct trace *traces,
                   uint64_t first, uint64_t last)
{
    for (uint64_t op = first; op <= last; op++) {
        submit(nodes[PRIMARY], op);
        drive(cluster, nodes, schedule, traces);
        CHECK(reply(nodes[PRIMARY], op)->status == VSR_REPLY_OK);
        CHECK(reply(nodes[PRIMARY], op)->op == op);
    }
}

static void build(struct mem_cluster *cluster, struct mem_node **nodes,
                  uint32_t lagging_role)
{
    const struct vsr_member members[NODES] = {
        {1, VSR_MEMBER_FULL, 0}, {2, VSR_MEMBER_FULL, 0}, {3, lagging_role, 0}};
    const struct vsr_membership membership = {0, members, NODES, 1};
    for (uint32_t i = 0; i < NODES; i++) {
        struct vsr_options options = mem_options((uint64_t)i + 1, &membership);
        options.durability = VSR_DURABLE;
        /* The seed ran with a one-entry cache: resending a trimmed prefix
         * therefore needs a LOAD rather than cached entries. */
        options.limits.log_cache_entries = 1;
        options.limits.batch_entries = 1;
        nodes[i] = mem_cluster_add(cluster, &options);
        CHECK(mem_node_time(nodes[i], 0).consumed == 1);
    }
}

static void lagging_full_replica_fetches_checkpoint(void)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[NODES];
    struct trace traces[NODES] = {{0}};
    struct schedule schedule = {0, NULL};
    uint64_t now = 0;
    build(cluster, nodes, VSR_MEMBER_FULL);
    drive(cluster, nodes, &schedule, traces);
    commit(cluster, nodes, &schedule, traces, 1, 3);
    CHECK(status(nodes[LAGGING]).applied == 3);

    schedule.isolated = mem_node_id(nodes[LAGGING]);
    commit(cluster, nodes, &schedule, traces, 4, 6);
    CHECK(status(nodes[LAGGING]).applied == 3);

    /* Publish and trim on the primary while its trim is readable only. */
    schedule.held = nodes[PRIMARY];
    hint(nodes[PRIMARY]);
    drive(cluster, nodes, &schedule, traces);
    CHECK(status(nodes[PRIMARY]).checkpoint_op == 6);
    CHECK(readable_begin(nodes[PRIMARY]) == 7);
    CHECK(durable_begin(nodes[PRIMARY]) == 1);

    /* Heal inside that window. Retries restart from the backup's acknowledged
     * position, below the retained log, and must not load or latch. */
    schedule.isolated = 0;
    settle(cluster, nodes, &schedule, traces, &now, 6, 4);
    CHECK(status(nodes[PRIMARY]).state == VSR_STATE_NORMAL);
    CHECK(status(nodes[PRIMARY]).primary == mem_node_id(nodes[PRIMARY]));
    CHECK(readable_begin(nodes[PRIMARY]) == 7);

    schedule.held = NULL;
    settle(cluster, nodes, &schedule, traces, &now, 6, 0);
    CHECK(durable_begin(nodes[PRIMARY]) == 7);
    CHECK(status(nodes[LAGGING]).checkpoint_op == 6);
    CHECK(readable_begin(nodes[LAGGING]) == 7);
    CHECK(traces[LAGGING].fetches >= 1 && traces[LAGGING].installs >= 1);
    CHECK(traces[PRIMARY].fetches == 0);

    commit(cluster, nodes, &schedule, traces, 7, 7);
    settle(cluster, nodes, &schedule, traces, &now, 7, 0);
    CHECK(reply(nodes[PRIMARY], 7)->status == VSR_REPLY_OK);
    mem_cluster_destroy(cluster);
}

static void lagging_witness_adopts_anchor_only(void)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[NODES];
    struct trace traces[NODES] = {{0}};
    struct schedule schedule = {0, NULL};
    uint64_t now = 0;
    build(cluster, nodes, VSR_MEMBER_WITNESS);
    drive(cluster, nodes, &schedule, traces);
    commit(cluster, nodes, &schedule, traces, 1, 3);
    CHECK(status(nodes[LAGGING]).committed == 3);

    schedule.isolated = mem_node_id(nodes[LAGGING]);
    commit(cluster, nodes, &schedule, traces, 4, 6);
    CHECK(status(nodes[LAGGING]).committed == 3);

    /* Both full members retain a checkpoint covering the trimmed prefix. */
    schedule.held = nodes[PRIMARY];
    hint(nodes[PRIMARY]);
    hint(nodes[1]);
    drive(cluster, nodes, &schedule, traces);
    CHECK(status(nodes[PRIMARY]).checkpoint_op == 6);
    CHECK(status(nodes[1]).checkpoint_op == 6);
    CHECK(readable_begin(nodes[PRIMARY]) == 7);
    CHECK(durable_begin(nodes[PRIMARY]) == 1);

    schedule.isolated = 0;
    settle(cluster, nodes, &schedule, traces, &now, 6, 4);
    CHECK(status(nodes[PRIMARY]).state == VSR_STATE_NORMAL);
    CHECK(status(nodes[PRIMARY]).primary == mem_node_id(nodes[PRIMARY]));

    schedule.held = NULL;
    settle(cluster, nodes, &schedule, traces, &now, 6, 0);
    CHECK(status(nodes[LAGGING]).role == VSR_MEMBER_WITNESS);
    CHECK(status(nodes[LAGGING]).checkpoint_op == 6);
    CHECK(readable_begin(nodes[LAGGING]) == 7);
    CHECK(traces[LAGGING].fetches == 0 && traces[LAGGING].installs == 0);

    commit(cluster, nodes, &schedule, traces, 7, 7);
    settle(cluster, nodes, &schedule, traces, &now, 7, 0);
    CHECK(reply(nodes[PRIMARY], 7)->status == VSR_REPLY_OK);
    mem_cluster_destroy(cluster);
}

static bool selected(int argc, char **argv, const char *name)
{
    return argc == 1 || strcmp(argv[1], name) == 0;
}

int main(int argc, char **argv)
{
    unsigned int count = 0;
    CHECK(argc <= 2);
    if (selected(argc, argv, "full")) {
        lagging_full_replica_fetches_checkpoint();
        count++;
    }
    if (selected(argc, argv, "witness")) {
        lagging_witness_adopts_anchor_only();
        count++;
    }
    CHECK(count != 0);
    return 0;
}
