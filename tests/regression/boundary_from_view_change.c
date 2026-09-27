#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <stdint.h>

/* Regression for `tests/fuzzy/cluster 4 1 470 quiet 32` (also seeds 22, 42,
 * 81, 95, 115, 124, 136, 160, 174, 188 of profile 32 at 600 steps).
 *
 * A RECONFIGURE boundary can be learned as committed from any state, not only
 * NORMAL: the quorum was collected in the old epoch, and the hard-state store
 * that makes the commitment stable may complete only after a
 * START_VIEW_CHANGE quorum moved the replica to VIEW_CHANGE. The old core
 * entered the new epoch with the old-epoch view-change round still active.
 * That round kept the transition module busy, so the replica never counted
 * as locally installed, never promised EPOCH_STARTED, and kept electing in
 * the new epoch forever while the other members waited in INSTALLED.
 *
 * Entering an epoch now abandons every old-epoch round. A replica that holds
 * the complete history through the boundary installs directly, exactly as it
 * would from NORMAL.
 *
 * Both scenarios below withhold one store completion at the replica under
 * test so the commit store is issued before the view change and completes
 * after it. The first is the old primary with a PREPARE_OK quorum. The second
 * is the view-change side: a witness backup learns the boundary from the
 * final old-group COMMIT while a START_VIEW_CHANGE is pending, and with a
 * one-unit work budget its DO_VIEW_CHANGE offer reports the committed
 * boundary before the notification moves it into the epoch. The new primary
 * then learns the committed boundary during log selection and must abandon
 * that selection instead of finishing an old-epoch view. */

enum { NODES = 3, PRIMARY = 0, BACKUP = 1, WITNESS = 2 };

struct schedule {
    bool open;     /* Deliver everything and complete every effect. */
    uint32_t held; /* Node whose effects stay outstanding, or 0. */
    uint32_t promises[NODES]; /* EPOCH_STARTED observed per sender. */
};

static struct vsr_status status(struct mem_node *node)
{
    struct vsr_status result;
    vsr_get_status(mem_node_core(node), &result);
    CHECK(result.failure.code == VSR_FAILURE_NONE);
    return result;
}

/* Only PREPARE from the old primary flows while the schedule is closed:
 * everything else stays queued until the scenario delivers it explicitly. */
static bool allowed(const struct schedule *schedule,
                    const struct vsr_message *message)
{
    return schedule->open ||
           (message->from == PRIMARY + 1 && message->type == VSR_MSG_PREPARE);
}

static void observe(struct schedule *schedule,
                    const struct vsr_message *message)
{
    if (message->type == VSR_MSG_EPOCH_STARTED && message->from >= 1 &&
        message->from <= NODES)
        schedule->promises[message->from - 1]++;
}

static void drain(struct mem_node *node)
{
    for (unsigned attempt = 0; attempt < 10000; attempt++) {
        const struct mem_step step = mem_node_event(node, NULL);
        CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
        if ((step.flags & VSR_UPDATE_MORE) == 0 ||
            (step.flags & VSR_UPDATE_OUTPUT_FULL) != 0)
            return;
    }
    CHECK(false);
}

static void drive(struct mem_cluster *cluster, struct mem_node **nodes,
                  struct schedule *schedule)
{
    for (size_t turn = 0; turn < 100000; turn++) {
        bool progress = false;
        for (size_t i = 0; i < NODES; i++) {
            if (schedule->held == i + 1)
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
            if (!allowed(schedule, message))
                continue;
            observe(schedule, message);
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

/* Delivers the first queued message of one type and number between two
 * replicas; stale acknowledgments of position zero stay queued. */
static void deliver(struct mem_cluster *cluster, uint64_t from, uint64_t to,
                    uint32_t type, uint64_t number)
{
    for (size_t i = 0; i < mem_cluster_messages(cluster); i++) {
        uint64_t target;
        const struct vsr_message *message =
            mem_cluster_message(cluster, i, &target);
        if (message->from == from && target == to && message->type == type &&
            message->number == number) {
            CHECK(mem_cluster_deliver(cluster, i));
            return;
        }
    }
    CHECK(false);
}

/* The index of the replica's single outstanding STORE; a SYNC for an
 * earlier sequence may be queued beside it. */
static size_t store_index(struct mem_node *node)
{
    size_t found = SIZE_MAX;
    for (size_t j = 0; j < mem_node_effects(node); j++) {
        if (mem_node_effect(node, j)->type != VSR_OP_STORE)
            continue;
        CHECK(found == SIZE_MAX);
        found = j;
    }
    CHECK(found != SIZE_MAX);
    return found;
}

/* Completes that STORE, and first any effect it waits on. */
static void complete_store(struct mem_node *node)
{
    for (unsigned attempt = 0; attempt < 16; attempt++) {
        if (mem_node_complete(node, store_index(node), VSR_IO_OK))
            return;
        for (size_t j = 0; j < mem_node_effects(node); j++)
            if (j != store_index(node) && mem_node_complete(node, j, VSR_IO_OK))
                break;
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

static bool converged(struct mem_node **nodes, uint64_t epoch,
                      uint64_t committed)
{
    for (size_t i = 0; i < NODES; i++) {
        const struct vsr_status current = status(nodes[i]);
        if (current.state != VSR_STATE_NORMAL || current.epoch != epoch ||
            current.configuration->phase != VSR_EPOCH_STEADY ||
            current.view != status(nodes[PRIMARY]).view ||
            current.committed < committed)
            return false;
        if (current.role == VSR_MEMBER_FULL && current.applied < committed)
            return false;
    }
    return true;
}

/* Advance logical time in heartbeat steps until every member is STEADY in
 * the epoch and NORMAL in one view with the committed position. */
static void settle(struct mem_cluster *cluster, struct mem_node **nodes,
                   struct schedule *schedule, uint64_t *now, uint64_t epoch,
                   uint64_t committed)
{
    for (unsigned round = 0; round < 256; round++) {
        drive(cluster, nodes, schedule);
        if (converged(nodes, epoch, committed))
            return;
        *now += 10;
        tick(nodes, *now);
    }
    CHECK(false);
}

static void submit(struct mem_node *node, const struct vsr_request *request,
                   uint64_t route)
{
    const struct vsr_event event = {VSR_EVENT_REQUEST, 0, route, request, 1};
    const struct mem_step step = mem_node_event(node, &event);
    CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
    CHECK(step.consumed == 1);
}

static const struct vsr_member members[NODES] = {{1, VSR_MEMBER_FULL, 0},
                                                 {2, VSR_MEMBER_FULL, 0},
                                                 {3, VSR_MEMBER_WITNESS, 0}};

/* Steps one replica alone, completing its effects, until it is idle or
 * reaches the epoch. */
static void advance(struct mem_node *node, uint64_t epoch)
{
    for (unsigned round = 0; round < 1000; round++) {
        const struct mem_step step = mem_node_event(node, NULL);
        CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
        bool completed = false;
        for (size_t j = 0; j < mem_node_effects(node); j++)
            if (mem_node_complete(node, j, VSR_IO_OK)) {
                completed = true;
                break;
            }
        if (status(node).epoch == epoch)
            return;
        if (!completed && (step.flags & VSR_UPDATE_MORE) == 0)
            return;
    }
    CHECK(false);
}

static void build(struct mem_cluster *cluster, struct mem_node **nodes,
                  uint32_t witness_work)
{
    const struct vsr_membership membership = {0, members, NODES, 1};
    for (uint32_t i = 0; i < NODES; i++) {
        struct vsr_options options = mem_options((uint64_t)i + 1, &membership);
        if (i == WITNESS)
            options.limits.work_per_step = witness_work;
        nodes[i] = mem_cluster_add(cluster, &options);
        tick_node(nodes[i], 0);
    }
}

/* Proposes the epoch-1 membership (same members) at the primary and lets
 * only the PREPARE reach the backups: the primary holds its own vote and
 * both PREPARE_OK replies wait in the queue. */
static void propose(struct mem_cluster *cluster, struct mem_node **nodes,
                    struct schedule *schedule)
{
    const struct vsr_membership next = {1, members, NODES, 1};
    const struct vsr_request request = {
        {{7, 1}, 1}, 0, VSR_REQUEST_RECONFIGURE, 0, &next};
    submit(nodes[PRIMARY], &request, 1);
    drive(cluster, nodes, schedule);
    for (size_t i = 0; i < NODES; i++) {
        CHECK(status(nodes[i]).committed == 0);
        CHECK(status(nodes[i]).epoch == 0);
    }
}

/* The backups time out without heartbeats and announce view 1; their
 * START_VIEW_CHANGE messages stay queued for explicit delivery. */
static void elect(struct mem_cluster *cluster, struct mem_node **nodes,
                  struct schedule *schedule, uint64_t *now)
{
    *now += 60;
    tick_node(nodes[BACKUP], *now);
    tick_node(nodes[WITNESS], *now);
    drive(cluster, nodes, schedule);
    CHECK(status(nodes[BACKUP]).state == VSR_STATE_VIEW_CHANGE);
    CHECK(status(nodes[BACKUP]).view == 1);
    CHECK(status(nodes[WITNESS]).state == VSR_STATE_VIEW_CHANGE);
    CHECK(status(nodes[WITNESS]).view == 1);
    CHECK(status(nodes[PRIMARY]).state == VSR_STATE_NORMAL);
    CHECK(status(nodes[PRIMARY]).view == 0);
}

static void finish(struct mem_cluster *cluster, struct mem_node **nodes,
                   struct schedule *schedule, uint64_t *now)
{
    schedule->open = true;
    schedule->held = 0;
    for (size_t i = 0; i < NODES; i++)
        mem_node_output_capacity(nodes[i], 16);
    settle(cluster, nodes, schedule, now, 1, 1);
    for (size_t i = 0; i < NODES; i++) {
        CHECK(status(nodes[i]).role == members[i].role);
        CHECK(schedule->promises[i] != 0);
    }
    CHECK(status(nodes[PRIMARY]).applied == 1);
    CHECK(status(nodes[BACKUP]).applied == 1);

    /* The new epoch serves ordinary commands from its view-zero primary. */
    const struct vsr_span span = {"boundary", 8};
    const struct vsr_blob command = {&span, 8, 1, 0};
    const struct vsr_request request = {
        {{7, 2}, 1}, 1, VSR_REQUEST_COMMAND, 0, &command};
    submit(nodes[PRIMARY], &request, 2);
    settle(cluster, nodes, schedule, now, 1, 2);
    CHECK(status(nodes[PRIMARY]).applied == 2);
    CHECK(status(nodes[BACKUP]).applied == 2);
    CHECK(status(nodes[WITNESS]).committed == 2);
}

static void primary_commits_from_view_change(void)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[NODES];
    struct schedule schedule = {true, 0, {0}};
    uint64_t now = 0;
    build(cluster, nodes, 128);
    drive(cluster, nodes, &schedule);
    CHECK(status(nodes[PRIMARY]).primary == PRIMARY + 1);

    schedule.open = false;
    propose(cluster, nodes, &schedule);
    elect(cluster, nodes, &schedule, &now);

    /* One output slot and a withheld completion: the PREPARE_OK quorum
     * issues the commit store, which stays outstanding while the view
     * change arrives. */
    mem_node_output_capacity(nodes[PRIMARY], 1);
    schedule.held = PRIMARY + 1;
    deliver(cluster, BACKUP + 1, PRIMARY + 1, VSR_MSG_PREPARE_OK, 1);
    drain(nodes[PRIMARY]);
    CHECK(store_index(nodes[PRIMARY]) == 0);
    CHECK(status(nodes[PRIMARY]).committed == 0);

    /* Both START_VIEW_CHANGE arrive before that store completes. */
    deliver(cluster, BACKUP + 1, PRIMARY + 1, VSR_MSG_START_VIEW_CHANGE, 0);
    deliver(cluster, WITNESS + 1, PRIMARY + 1, VSR_MSG_START_VIEW_CHANGE, 0);
    CHECK(status(nodes[PRIMARY]).state == VSR_STATE_VIEW_CHANGE);
    CHECK(status(nodes[PRIMARY]).view == 1);
    CHECK(status(nodes[PRIMARY]).epoch == 0);

    /* The completion commits the boundary from VIEW_CHANGE. The primary
     * holds the complete history through it and must enter the new epoch
     * on its way to INSTALLED, not as a transferring voter-less replica. */
    complete_store(nodes[PRIMARY]);
    schedule.held = 0;
    advance(nodes[PRIMARY], 1);
    CHECK(status(nodes[PRIMARY]).epoch == 1);
    CHECK(status(nodes[PRIMARY]).committed == 1);
    CHECK(status(nodes[PRIMARY]).view == 0);
    advance(nodes[PRIMARY], 2);
    CHECK(status(nodes[PRIMARY]).state == VSR_STATE_NORMAL);
    CHECK(status(nodes[PRIMARY]).configuration->phase == VSR_EPOCH_INSTALLED);
    CHECK(status(nodes[PRIMARY]).view == 0);

    finish(cluster, nodes, &schedule, &now);
    mem_cluster_destroy(cluster);
}

static void new_primary_selects_committed_boundary(void)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[NODES];
    struct schedule schedule = {true, 0, {0}};
    uint64_t now = 0;
    build(cluster, nodes, 1);
    drive(cluster, nodes, &schedule);
    CHECK(status(nodes[PRIMARY]).primary == PRIMARY + 1);

    schedule.open = false;
    propose(cluster, nodes, &schedule);

    /* The old primary commits from NORMAL with the witness's vote and sends
     * the final old-group COMMIT before announcing the epoch. */
    deliver(cluster, WITNESS + 1, PRIMARY + 1, VSR_MSG_PREPARE_OK, 1);
    drive(cluster, nodes, &schedule);
    CHECK(status(nodes[PRIMARY]).epoch == 1);
    CHECK(status(nodes[PRIMARY]).committed == 1);

    /* Only the full backup times out; it leads view 1. */
    now += 60;
    tick_node(nodes[BACKUP], now);
    drive(cluster, nodes, &schedule);
    CHECK(status(nodes[BACKUP]).state == VSR_STATE_VIEW_CHANGE);
    CHECK(status(nodes[BACKUP]).view == 1);
    CHECK(status(nodes[WITNESS]).state == VSR_STATE_NORMAL);
    CHECK(status(nodes[WITNESS]).view == 0);

    /* The witness issues its commit store in NORMAL; the START_VIEW_CHANGE
     * moves it to view 1 before that store completes. */
    schedule.held = WITNESS + 1;
    deliver(cluster, PRIMARY + 1, WITNESS + 1, VSR_MSG_COMMIT, 1);
    drain(nodes[WITNESS]);
    CHECK(store_index(nodes[WITNESS]) == 0);
    CHECK(status(nodes[WITNESS]).committed == 0);
    deliver(cluster, BACKUP + 1, WITNESS + 1, VSR_MSG_START_VIEW_CHANGE, 0);
    CHECK(status(nodes[WITNESS]).state == VSR_STATE_VIEW_CHANGE);
    CHECK(status(nodes[WITNESS]).view == 1);
    complete_store(nodes[WITNESS]);
    schedule.held = 0;

    /* One work unit per step: the view-change round offers the committed
     * boundary to the new primary before the boundary notification runs,
     * then the witness enters the epoch from VIEW_CHANGE and installs
     * immediately. */
    advance(nodes[WITNESS], 1);
    CHECK(status(nodes[WITNESS]).epoch == 1);
    CHECK(status(nodes[WITNESS]).committed == 1);
    CHECK(status(nodes[WITNESS]).view == 0);
    advance(nodes[WITNESS], 2);
    CHECK(status(nodes[WITNESS]).state == VSR_STATE_NORMAL);
    CHECK(status(nodes[WITNESS]).configuration->phase == VSR_EPOCH_INSTALLED);
    bool offered = false;
    for (size_t i = 0; i < mem_cluster_messages(cluster); i++) {
        uint64_t to;
        const struct vsr_message *message =
            mem_cluster_message(cluster, i, &to);
        if (message->from == WITNESS + 1 && to == BACKUP + 1 &&
            message->type == VSR_MSG_DO_VIEW_CHANGE && message->epoch == 0) {
            const struct vsr_log_state *state = message->body;
            CHECK(state->committed == 1 && state->log_end == 2);
            offered = true;
        }
    }
    CHECK(offered);

    /* The new primary collects the witness's vote and offer, selects the
     * log ending at the boundary, and learns its commitment while the
     * primary selection is active in VIEW_CHANGE. */
    deliver(cluster, WITNESS + 1, BACKUP + 1, VSR_MSG_START_VIEW_CHANGE, 0);
    advance(nodes[BACKUP], 1);
    CHECK(status(nodes[BACKUP]).state == VSR_STATE_VIEW_CHANGE);
    CHECK(status(nodes[BACKUP]).epoch == 0);
    deliver(cluster, WITNESS + 1, BACKUP + 1, VSR_MSG_DO_VIEW_CHANGE, 1);
    advance(nodes[BACKUP], 1);
    CHECK(status(nodes[BACKUP]).epoch == 1);
    CHECK(status(nodes[BACKUP]).committed == 1);
    CHECK(status(nodes[BACKUP]).view == 0);
    advance(nodes[BACKUP], 2);
    CHECK(status(nodes[BACKUP]).state == VSR_STATE_NORMAL);
    CHECK(status(nodes[BACKUP]).configuration->phase == VSR_EPOCH_INSTALLED);

    finish(cluster, nodes, &schedule, &now);
    mem_cluster_destroy(cluster);
}

int main(int argc, char **argv)
{
    (void)argv;
    CHECK(argc == 1);
    primary_commits_from_view_change();
    new_primary_selects_committed_boundary();
    return 0;
}
