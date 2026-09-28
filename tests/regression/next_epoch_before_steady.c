#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <stdint.h>

/* Availability property from the protocol review (docs/protocol.md "Epoch
 * handoff and witnesses": a replica establishes STEADY after collecting the
 * required EPOCH_STARTED promises "or learning a later committed epoch that
 * necessarily follows that handoff").
 *
 * Scenario: three full members reconfigure into epoch 1. Member 3 installs
 * the epoch and enters NORMAL with phase INSTALLED, but every EPOCH_STARTED
 * addressed to it is lost, so it keeps collecting promises while members 1
 * and 2 reach STEADY. The primary then admits the next RECONFIGURE, which it
 * may do only after its own STEADY, and member 3 accepts it through PREPARE
 * and learns its commitment through COMMIT alone (its START_EPOCH copies are
 * lost too). The old core latched VSR_FAILURE_INVARIANT at that commit
 * notification because its phase was not STEADY. The boundary committing in
 * epoch 1 proves that handoff complete, so member 3 must finish it locally,
 * enter epoch 2, and let the group reach STEADY there. */

enum { NODES = 3, PRIMARY = 0, BACKUP = 1, LATE = 2 };

struct schedule {
    bool drop_promises;  /* Lose EPOCH_STARTED addressed to member 3. */
    bool drop_announces; /* Lose START_EPOCH and NEW_EPOCH to member 3. */
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
    if (to != LATE + 1)
        return false;
    if (schedule->drop_promises && message->type == VSR_MSG_EPOCH_STARTED)
        return true;
    return schedule->drop_announces && (message->type == VSR_MSG_START_EPOCH ||
                                        message->type == VSR_MSG_NEW_EPOCH);
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

static void submit(struct mem_node *node, const struct vsr_request *request,
                   uint64_t route)
{
    const struct vsr_event event = {VSR_EVENT_REQUEST, 0, route, request, 1};
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

static bool steady(struct mem_node *node, uint64_t epoch)
{
    const struct vsr_status current = status(node);
    return current.state == VSR_STATE_NORMAL && current.epoch == epoch &&
           current.configuration->phase == VSR_EPOCH_STEADY;
}

static bool all_steady(struct mem_node **nodes, uint64_t epoch)
{
    for (size_t i = 0; i < NODES; i++)
        if (!steady(nodes[i], epoch))
            return false;
    return true;
}

static const struct vsr_member members[NODES] = {
    {1, VSR_MEMBER_FULL, 0}, {2, VSR_MEMBER_FULL, 0}, {3, VSR_MEMBER_FULL, 0}};

static void build(struct mem_cluster *cluster, struct mem_node **nodes)
{
    const struct vsr_membership membership = {0, members, NODES, 1};
    for (uint32_t i = 0; i < NODES; i++) {
        struct vsr_options options = mem_options((uint64_t)i + 1, &membership);
        nodes[i] = mem_cluster_add(cluster, &options);
        tick_node(nodes[i], 0);
    }
}

static void reconfigure(struct mem_node *primary, uint64_t epoch,
                        uint64_t route)
{
    const struct vsr_membership next = {epoch, members, NODES, 1};
    const struct vsr_request request = {
        {{7, epoch}, 1}, epoch - 1, VSR_REQUEST_RECONFIGURE, 0, &next};
    submit(primary, &request, route);
}

/* Advance logical time in heartbeat steps until the predicate holds. */
#define SETTLE(cluster, nodes, schedule, now, predicate)                       \
    do {                                                                       \
        for (unsigned round = 0;; round++) {                                   \
            CHECK(round < 256);                                                \
            drive(cluster, nodes, schedule);                                   \
            if (predicate)                                                     \
                break;                                                         \
            *(now) += 10;                                                      \
            tick(nodes, *(now));                                               \
        }                                                                      \
    } while (0)

static void late_backup_commits_next_boundary(void)
{
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[NODES];
    struct schedule schedule = {true, false};
    uint64_t now = 0;
    build(cluster, nodes);
    drive(cluster, nodes, &schedule);
    CHECK(status(nodes[PRIMARY]).primary == PRIMARY + 1);

    /* Epoch 1: members 1 and 2 collect their promises; member 3 installs
     * and votes but never sees an EPOCH_STARTED. */
    reconfigure(nodes[PRIMARY], 1, 1);
    SETTLE(cluster, nodes, &schedule, &now,
           steady(nodes[PRIMARY], 1) && steady(nodes[BACKUP], 1));
    CHECK(status(nodes[LATE]).state == VSR_STATE_NORMAL);
    CHECK(status(nodes[LATE]).epoch == 1);
    CHECK(status(nodes[LATE]).configuration->phase == VSR_EPOCH_INSTALLED);
    CHECK(status(nodes[LATE]).committed == 1);

    /* Epoch 2 is admitted by the STEADY primary. Member 3 learns its
     * boundary only through PREPARE and COMMIT: the announcements are lost.
     * It must enter the epoch instead of fencing itself. */
    schedule.drop_announces = true;
    reconfigure(nodes[PRIMARY], 2, 2);
    SETTLE(cluster, nodes, &schedule, &now,
           status(nodes[LATE]).epoch == 2 && status(nodes[PRIMARY]).epoch == 2);
    CHECK(reply(nodes[PRIMARY], 2) != NULL);
    CHECK(reply(nodes[PRIMARY], 2)->status == VSR_REPLY_OK);
    CHECK(status(nodes[LATE]).committed == 2);
    CHECK(status(nodes[LATE]).failure.code == VSR_FAILURE_NONE);

    /* With every message flowing again the group completes the handoff and
     * serves a command in the new epoch. */
    schedule.drop_promises = false;
    schedule.drop_announces = false;
    SETTLE(cluster, nodes, &schedule, &now, all_steady(nodes, 2));
    const struct vsr_span span = {"steady", 6};
    const struct vsr_blob command = {&span, 6, 1, 0};
    const struct vsr_request request = {
        {{7, 3}, 1}, 2, VSR_REQUEST_COMMAND, 0, &command};
    submit(nodes[PRIMARY], &request, 3);
    SETTLE(cluster, nodes, &schedule, &now,
           reply(nodes[PRIMARY], 3) != NULL &&
               status(nodes[LATE]).applied == 3);
    CHECK(reply(nodes[PRIMARY], 3)->status == VSR_REPLY_OK);
    for (size_t i = 0; i < NODES; i++) {
        CHECK(status(nodes[i]).applied == 3);
        CHECK(status(nodes[i]).view == status(nodes[PRIMARY]).view);
    }
    mem_cluster_destroy(cluster);
}

int main(int argc, char **argv)
{
    (void)argv;
    CHECK(argc == 1);
    late_backup_commits_next_boundary();
    return 0;
}
