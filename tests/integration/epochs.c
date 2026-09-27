#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

static void run(struct mem_cluster *cluster)
{
    CHECK(mem_cluster_run(cluster, 100000) < 100000);
    mem_cluster_check(cluster);
}

static void submit(struct mem_node *node, struct vsr_request request,
                   uint64_t route)
{
    struct vsr_event event = {VSR_EVENT_REQUEST, VSR_IO_OK, route, &request, 1};
    CHECK(mem_node_event(node, &event).consumed == 1);
}

static void same_members(uint32_t count, uint32_t policy)
{
    struct vsr_member members[] = {{1, VSR_MEMBER_FULL, 0},
                                   {2, VSR_MEMBER_FULL, 0},
                                   {3, VSR_MEMBER_FULL, 0}};
    struct vsr_membership group = {0, members, count, count == 1 ? 0 : 1};
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[3];
    for (uint32_t i = 0; i < count; ++i) {
        struct vsr_options options = mem_options(i + 1, &group);
        options.durability = policy;
        nodes[i] = mem_cluster_add(cluster, &options);
        CHECK(mem_node_time(nodes[i], 0).consumed == 1);
    }
    run(cluster);

    struct vsr_member next_members[] = {{1, VSR_MEMBER_FULL, 0},
                                        {2, VSR_MEMBER_FULL, 0},
                                        {3, VSR_MEMBER_WITNESS, 0}};
    struct vsr_membership next = {1, next_members, count, group.faults};
    submit(
        nodes[0],
        (struct vsr_request){{{7, 1}, 1}, 0, VSR_REQUEST_RECONFIGURE, 0, &next},
        1);
    run(cluster);
    for (uint32_t i = 0; i < count; ++i) {
        struct vsr_status status;
        vsr_get_status(mem_node_core(nodes[i]), &status);
        CHECK(status.failure.code == VSR_FAILURE_NONE);
        CHECK(status.epoch == 1 && status.view == 0);
        CHECK(status.configuration->phase == VSR_EPOCH_STEADY);
        CHECK(status.state == VSR_STATE_NORMAL);
        CHECK(status.role == next_members[i].role);
        CHECK(status.committed == 1);
    }

    struct vsr_check_epoch check = {1};
    struct vsr_request request = {
        {{7, 2}, 1}, 1, VSR_REQUEST_CHECK_EPOCH, 0, &check};
    submit(nodes[0], request, 2);
    run(cluster);
    const struct vsr_reply *reply = mem_node_reply(nodes[0], 1, NULL);
    CHECK(reply->status == VSR_REPLY_OK && reply->op == 2);
    CHECK(reply->flags == VSR_REPLY_EXECUTED);
    CHECK(reply->result.code == 0 && reply->result.data.size == 0);
    submit(nodes[0], request, 3);
    run(cluster);
    reply = mem_node_reply(nodes[0], 2, NULL);
    CHECK(reply->status == VSR_REPLY_OK && reply->op == 2);

    /* A later command cannot make the demoted member execute in its new
     * witness role, even though it retained the old full donor image. */
    const struct vsr_blob empty = {0};
    submit(nodes[0],
           (struct vsr_request){{{7, 3}, 1}, 1, VSR_REQUEST_COMMAND, 0, &empty},
           4);
    run(cluster);
    if (count == 3) {
        struct vsr_status status;
        vsr_get_status(mem_node_core(nodes[2]), &status);
        CHECK(status.committed == 3 && status.applied == 0);
    }
    mem_cluster_destroy(cluster);
}

static void disjoint(uint32_t policy)
{
    struct vsr_member old[] = {{1, VSR_MEMBER_FULL, 0},
                               {2, VSR_MEMBER_FULL, 0},
                               {3, VSR_MEMBER_FULL, 0}};
    struct vsr_membership group = {0, old, 3, 1};
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[6];
    for (uint32_t i = 0; i < 3; ++i) {
        struct vsr_options options = mem_options(i + 1, &group);
        options.durability = policy;
        options.limits.log_cache_entries = 4;
        options.limits.batch_entries = 2;
        options.limits.client_cache_entries = 1;
        nodes[i] = mem_cluster_add(cluster, &options);
        CHECK(mem_node_time(nodes[i], 0).consumed == 1);
    }
    run(cluster);
    struct vsr_span span = {"handoff", 7};
    struct vsr_blob body = {&span, 7, 1, 0};
    for (uint32_t i = 0; i < 10; ++i) {
        submit(nodes[0],
               (struct vsr_request){
                   {{50, i + 1}, 1}, 0, VSR_REQUEST_COMMAND, 0, &body},
               10 + i);
        run(cluster);
    }
    for (uint32_t i = 3; i < 6; ++i) {
        struct vsr_options options = mem_options(i + 1, &group);
        options.durability = policy;
        options.start_mode = VSR_START_JOIN;
        options.join_role = VSR_MEMBER_FULL;
        options.limits.log_cache_entries = 4;
        options.limits.batch_entries = 2;
        options.limits.client_cache_entries = 1;
        nodes[i] = mem_cluster_add(cluster, &options);
        CHECK(mem_node_time(nodes[i], 0).consumed == 1);
    }
    run(cluster);
    for (uint32_t i = 3; i < 6; ++i) {
        struct vsr_status status;
        vsr_get_status(mem_node_core(nodes[i]), &status);
        CHECK(status.state == VSR_STATE_WARMING && status.applied == 10);
    }
    struct vsr_member members[] = {{4, VSR_MEMBER_FULL, 0},
                                   {5, VSR_MEMBER_FULL, 0},
                                   {6, VSR_MEMBER_FULL, 0}};
    struct vsr_membership next = {1, members, 3, 1};
    submit(nodes[0],
           (struct vsr_request){
               {{60, 1}, 1}, 0, VSR_REQUEST_RECONFIGURE, 0, &next},
           30);
    run(cluster);
    for (uint32_t i = 0; i < 6; ++i) {
        struct vsr_status status;
        vsr_get_status(mem_node_core(nodes[i]), &status);
        CHECK(status.failure.code == VSR_FAILURE_NONE);
        CHECK(status.epoch == 1 &&
              status.configuration->phase == VSR_EPOCH_STEADY);
        CHECK(status.state == (i < 3 ? VSR_STATE_RETIRED : VSR_STATE_NORMAL));
        CHECK(status.applied == 11);
    }
    /* A different primary and disjoint client table base still suppress the
     * very first command after history has exceeded both working caches. */
    submit(nodes[3],
           (struct vsr_request){{{50, 1}, 1}, 1, VSR_REQUEST_COMMAND, 0, &body},
           31);
    run(cluster);
    const struct vsr_reply *reply = mem_node_reply(nodes[3], 0, NULL);
    CHECK(reply->status == VSR_REPLY_OK && reply->op == 1);
    submit(nodes[3],
           (struct vsr_request){{{60, 2}, 1}, 1, VSR_REQUEST_COMMAND, 0, &body},
           32);
    run(cluster);
    reply = mem_node_reply(nodes[3], 1, NULL);
    CHECK(reply->status == VSR_REPLY_OK && reply->op == 12);
    if (policy == VSR_DURABLE) {
        /* The persisted removal tombstone survives a donor restart. It must
         * not attempt a recovery quorum in a group it no longer belongs to. */
        mem_node_crash(nodes[0]);
        CHECK(mem_node_restart(nodes[0], (struct vsr_id){1, 2}) == VSR_OK);
        CHECK(mem_node_time(nodes[0], 0).consumed == 1);
        run(cluster);
        struct vsr_status retired;
        vsr_get_status(mem_node_core(nodes[0]), &retired);
        CHECK(retired.failure.code == VSR_FAILURE_NONE);
        CHECK(retired.state == VSR_STATE_RETIRED && retired.epoch == 1);
    }
    mem_cluster_destroy(cluster);
}

static void promotion(uint32_t policy)
{
    struct vsr_member old[] = {{1, VSR_MEMBER_FULL, 0},
                               {2, VSR_MEMBER_FULL, 0},
                               {3, VSR_MEMBER_WITNESS, 0}};
    struct vsr_membership group = {0, old, 3, 1};
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[3];
    for (uint32_t i = 0; i < 3; ++i) {
        struct vsr_options options = mem_options(i + 1, &group);
        options.durability = policy;
        nodes[i] = mem_cluster_add(cluster, &options);
        CHECK(mem_node_time(nodes[i], 0).consumed == 1);
    }
    run(cluster);
    struct vsr_blob empty = {0};
    for (uint32_t i = 0; i < 5; ++i) {
        submit(nodes[0],
               (struct vsr_request){
                   {{80, i + 1}, 1}, 0, VSR_REQUEST_COMMAND, 0, &empty},
               1 + i);
        run(cluster);
    }
    /* Both full replicas promise retained snapshots. The witness may retain
     * a remote anchor and trim, but promotion must fetch a full image. */
    for (uint32_t i = 0; i < 2; ++i) {
        struct vsr_event hint = {VSR_EVENT_CHECKPOINT, VSR_IO_OK, 0, NULL, 0};
        CHECK(mem_node_event(nodes[i], &hint).consumed == 1);
        run(cluster);
    }
    struct vsr_member members[] = {{1, VSR_MEMBER_WITNESS, 0},
                                   {2, VSR_MEMBER_FULL, 0},
                                   {3, VSR_MEMBER_FULL, 0}};
    struct vsr_membership next = {1, members, 3, 1};
    submit(nodes[0],
           (struct vsr_request){
               {{90, 1}, 1}, 0, VSR_REQUEST_RECONFIGURE, 0, &next},
           10);
    run(cluster);
    for (uint32_t i = 0; i < 3; ++i) {
        struct vsr_status status;
        vsr_get_status(mem_node_core(nodes[i]), &status);
        CHECK(status.failure.code == VSR_FAILURE_NONE);
        CHECK(status.state == VSR_STATE_NORMAL && status.primary == 2);
        CHECK(status.epoch == 1 &&
              status.configuration->phase == VSR_EPOCH_STEADY);
        CHECK(status.role == members[i].role);
        CHECK(status.applied == (i == 0 ? 0 : 6));
    }
    submit(
        nodes[1],
        (struct vsr_request){{{90, 2}, 1}, 1, VSR_REQUEST_COMMAND, 0, &empty},
        11);
    run(cluster);
    const struct vsr_reply *reply = mem_node_reply(nodes[1], 0, NULL);
    CHECK(reply->status == VSR_REPLY_OK && reply->op == 7);
    struct vsr_status promoted;
    vsr_get_status(mem_node_core(nodes[2]), &promoted);
    CHECK(promoted.applied == 7);
    mem_cluster_destroy(cluster);
}

static void stale_seed_discovery(uint32_t policy, bool learner, uint64_t epochs)
{
    struct vsr_member members[] = {{1, VSR_MEMBER_FULL, 0},
                                   {2, VSR_MEMBER_FULL, 0},
                                   {3, VSR_MEMBER_FULL, 0}};
    const struct vsr_membership original = {0, members, 3, 1};
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[4];
    for (uint32_t i = 0; i < 3; ++i) {
        struct vsr_options options = mem_options(i + 1, &original);
        options.durability = policy;
        nodes[i] = mem_cluster_add(cluster, &options);
        CHECK(mem_node_time(nodes[i], 0).consumed == 1);
    }
    run(cluster);
    if (!learner)
        mem_node_crash(nodes[2]);
    struct vsr_status status;
    for (uint64_t epoch = 1; epoch <= epochs; ++epoch) {
        const struct vsr_membership next = {epoch, members, 3, 1};
        submit(nodes[0],
               (struct vsr_request){{{120, 1}, epoch},
                                    epoch - 1,
                                    VSR_REQUEST_RECONFIGURE,
                                    0,
                                    &next},
               epoch);
        run(cluster);
        vsr_get_status(mem_node_core(nodes[0]), &status);
        CHECK(status.epoch == epoch &&
              status.configuration->phase == VSR_EPOCH_STEADY);
    }
    const struct vsr_blob body = {0};
    submit(nodes[0],
           (struct vsr_request){
               {{120, 2}, 1}, epochs, VSR_REQUEST_COMMAND, 0, &body},
           epochs + 1);
    run(cluster);
    /* Lose all historical handoff announcements. Discovery must still work
     * through a live overlapping seed after the handoff manager is idle. */
    while (mem_cluster_messages(cluster) != 0)
        mem_cluster_drop(cluster, 0);
    uint32_t index = learner ? 3 : 2;
    uint32_t count = learner ? 4 : 3;
    if (learner) {
        struct vsr_options options = mem_options(4, &original);
        options.durability = policy;
        options.start_mode = VSR_START_JOIN;
        options.join_role = VSR_MEMBER_FULL;
        nodes[index] = mem_cluster_add(cluster, &options);
    } else {
        CHECK(mem_node_restart(nodes[index], (struct vsr_id){3, 2}) == VSR_OK);
    }
    CHECK(mem_node_time(nodes[index], 0).consumed == 1);
    bool caught_up = false;
    for (uint64_t now = 0; now < 1000 && !caught_up; now += 5) {
        for (uint32_t i = 0; i < count; ++i)
            CHECK(mem_node_time(nodes[i], now).consumed == 1);
        run(cluster);
        vsr_get_status(mem_node_core(nodes[index]), &status);
        CHECK(status.failure.code == VSR_FAILURE_NONE);
        caught_up =
            status.epoch == epochs && status.applied >= epochs + 1 &&
            status.state == (learner ? VSR_STATE_WARMING : VSR_STATE_NORMAL);
    }
    CHECK(caught_up);
    mem_cluster_destroy(cluster);
}

/* A learner that discovered the cluster through a stale seed at a later epoch
 * warms without voting and is admitted by the next reconfiguration exactly
 * like one that joined at epoch 0. It is never retired. */
static void late_admission(uint32_t policy)
{
    struct vsr_member members[] = {{1, VSR_MEMBER_FULL, 0},
                                   {2, VSR_MEMBER_FULL, 0},
                                   {3, VSR_MEMBER_FULL, 0}};
    const struct vsr_membership original = {0, members, 3, 1};
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[4];
    for (uint32_t i = 0; i < 3; ++i) {
        struct vsr_options options = mem_options(i + 1, &original);
        options.durability = policy;
        nodes[i] = mem_cluster_add(cluster, &options);
        CHECK(mem_node_time(nodes[i], 0).consumed == 1);
    }
    run(cluster);
    const struct vsr_membership first = {1, members, 3, 1};
    submit(nodes[0],
           (struct vsr_request){
               {{130, 1}, 1}, 0, VSR_REQUEST_RECONFIGURE, 0, &first},
           1);
    run(cluster);
    const struct vsr_blob body = {0};
    submit(
        nodes[0],
        (struct vsr_request){{{130, 2}, 1}, 1, VSR_REQUEST_COMMAND, 0, &body},
        2);
    run(cluster);
    while (mem_cluster_messages(cluster) != 0)
        mem_cluster_drop(cluster, 0);
    struct vsr_options options = mem_options(4, &original);
    options.durability = policy;
    options.start_mode = VSR_START_JOIN;
    options.join_role = VSR_MEMBER_FULL;
    nodes[3] = mem_cluster_add(cluster, &options);
    struct vsr_status status;
    bool warm = false;
    uint64_t now = 0;
    for (; now < 1000 && !warm; now += 5) {
        for (uint32_t i = 0; i < 4; ++i)
            CHECK(mem_node_time(nodes[i], now).consumed == 1);
        run(cluster);
        vsr_get_status(mem_node_core(nodes[3]), &status);
        CHECK(status.failure.code == VSR_FAILURE_NONE);
        CHECK(status.state != VSR_STATE_RETIRED);
        warm = status.epoch == 1 && status.applied == 2 &&
               status.state == VSR_STATE_WARMING &&
               status.configuration->phase == VSR_EPOCH_STEADY;
    }
    CHECK(warm);
    /* A member's promise to a learner does not leave the group with any
     * pending handoff work: the next reconfiguration is admitted at once. */
    struct vsr_member admitted[] = {{1, VSR_MEMBER_FULL, 0},
                                    {2, VSR_MEMBER_FULL, 0},
                                    {3, VSR_MEMBER_FULL, 0},
                                    {4, VSR_MEMBER_FULL, 0}};
    const struct vsr_membership next = {2, admitted, 4, 1};
    submit(nodes[0],
           (struct vsr_request){
               {{130, 3}, 1}, 1, VSR_REQUEST_RECONFIGURE, 0, &next},
           3);
    run(cluster);
    const struct vsr_reply *reply = mem_node_reply(nodes[0], 2, NULL);
    CHECK(reply->status == VSR_REPLY_OK && reply->op == 3);
    for (uint32_t i = 0; i < 4; ++i) {
        vsr_get_status(mem_node_core(nodes[i]), &status);
        CHECK(status.failure.code == VSR_FAILURE_NONE);
        CHECK(status.epoch == 2 &&
              status.configuration->phase == VSR_EPOCH_STEADY);
        CHECK(status.state == VSR_STATE_NORMAL);
        CHECK(status.role == VSR_MEMBER_FULL);
        CHECK(status.applied == 3);
    }
    submit(
        nodes[0],
        (struct vsr_request){{{130, 4}, 1}, 2, VSR_REQUEST_COMMAND, 0, &body},
        4);
    run(cluster);
    reply = mem_node_reply(nodes[0], 3, NULL);
    CHECK(reply->status == VSR_REPLY_OK && reply->op == 4);
    vsr_get_status(mem_node_core(nodes[3]), &status);
    CHECK(status.applied == 4 && status.committed == 4);
    mem_cluster_destroy(cluster);
}

/* Only an authenticated peer known in the receiver's current membership or
 * seed can report a later committed epoch. Self and unknown senders cannot
 * fence a NORMAL member, however well formed their descriptor. */
static void epoch_report_authority(uint32_t policy)
{
    struct vsr_member members[] = {{1, VSR_MEMBER_FULL, 0},
                                   {2, VSR_MEMBER_FULL, 0},
                                   {3, VSR_MEMBER_FULL, 0}};
    const struct vsr_membership original = {0, members, 3, 1};
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *nodes[3];
    for (uint32_t i = 0; i < 3; ++i) {
        struct vsr_options options = mem_options(i + 1, &original);
        options.durability = policy;
        nodes[i] = mem_cluster_add(cluster, &options);
        CHECK(mem_node_time(nodes[i], 0).consumed == 1);
    }
    run(cluster);
    const struct vsr_membership first = {1, members, 3, 1};
    submit(nodes[0],
           (struct vsr_request){
               {{140, 1}, 1}, 0, VSR_REQUEST_RECONFIGURE, 0, &first},
           1);
    run(cluster);
    struct vsr_status status;
    vsr_get_status(mem_node_core(nodes[0]), &status);
    CHECK(status.state == VSR_STATE_NORMAL && status.epoch == 1);
    CHECK(status.committed == 1);
    const struct vsr_membership claimed = {2, members, 3, 1};
    const struct vsr_epoch report = {&claimed, &first, 2,
                                     VSR_EPOCH_TRANSFERRING, 0};
    const uint64_t senders[] = {99, 1, 2};
    for (uint32_t i = 0; i < 3; ++i) {
        const struct vsr_message message = {
            {1, 1}, 2, 0, senders[i], VSR_MSG_NEW_EPOCH, 0, 2, &report};
        const struct vsr_event event = {VSR_EVENT_MESSAGE, VSR_IO_OK, 0,
                                        &message, 1};
        CHECK(mem_node_event(nodes[0], &event).consumed == 1);
        run(cluster);
        vsr_get_status(mem_node_core(nodes[0]), &status);
        CHECK(status.failure.code == VSR_FAILURE_NONE);
        if (senders[i] != 2) {
            CHECK(status.state == VSR_STATE_NORMAL && status.epoch == 1);
            CHECK(status.configuration->phase == VSR_EPOCH_STEADY);
        }
    }
    /* A current member's report fences the old epoch without granting a vote
     * or catch-up: the receiver leaves NORMAL until the history arrives. */
    CHECK(status.state == VSR_STATE_RECOVERING ||
          status.state == VSR_STATE_TRANSITIONING);
    mem_cluster_destroy(cluster);
}

int main(void)
{
    same_members(1, VSR_DURABLE);
    same_members(1, VSR_REPLICATED);
    same_members(3, VSR_DURABLE);
    same_members(3, VSR_REPLICATED);
    disjoint(VSR_DURABLE);
    disjoint(VSR_REPLICATED);
    promotion(VSR_DURABLE);
    promotion(VSR_REPLICATED);
    for (uint32_t policy = 0; policy < 2; ++policy)
        for (uint64_t epochs = 1; epochs <= 3; epochs += 2) {
            stale_seed_discovery(policy, false, epochs);
            stale_seed_discovery(policy, true, epochs);
        }
    late_admission(VSR_DURABLE);
    late_admission(VSR_REPLICATED);
    epoch_report_authority(VSR_DURABLE);
    epoch_report_authority(VSR_REPLICATED);
    return 0;
}
