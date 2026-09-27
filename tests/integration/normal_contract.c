#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

/* Exercise the planner's progress reserves with one effect slot, one log cache
 * slot, one client slot, one transfer, and one output descriptor. */
static void minimum_capacity(void)
{
    for (unsigned policy = 0; policy < 2; policy++) {
        struct vsr_member members[] = {{1, VSR_MEMBER_FULL, 0},
                                       {2, VSR_MEMBER_FULL, 0},
                                       {3, VSR_MEMBER_WITNESS, 0}};
        struct vsr_membership group = {0, members, 3, 1};
        struct mem_cluster *c = mem_cluster_create();
        struct mem_node *nodes[3];
        for (unsigned i = 0; i < 3; i++) {
            struct vsr_options o = mem_options(i + 1, &group);
            o.durability = policy;
            o.limits.operations = 1;
            o.limits.transfers = 1;
            o.limits.input_leases = 9;
            o.limits.log_cache_entries = 1;
            o.limits.batch_entries = 1;
            o.limits.client_cache_entries = 1;
            o.limits.pinned_payload_bytes = 11520;
            nodes[i] = mem_cluster_add(c, &o);
            mem_node_output_capacity(nodes[i], 1);
            CHECK(mem_node_time(nodes[i], 0).consumed == 1);
        }
        CHECK(mem_cluster_run(c, 100000) < 100000);
        for (unsigned i = 0; i < 12; i++) {
            struct vsr_blob body = {0};
            struct vsr_request req = {
                {{4, i + 1}, 1}, 0, VSR_REQUEST_COMMAND, 0, &body};
            struct vsr_event e = {VSR_EVENT_REQUEST, 0, i + 1, &req, 1};
            struct mem_step s = mem_node_event(nodes[0], &e);
            CHECK(s.consumed == 1);
            CHECK(mem_cluster_run(c, 100000) < 100000);
            struct vsr_status st;
            vsr_get_status(mem_node_core(nodes[0]), &st);
            CHECK(st.failure.code == 0);
            CHECK(st.applied == i + 1);
            CHECK(mem_node_replies(nodes[0]) == i + 1);
        }
        mem_cluster_check(c);
        mem_cluster_destroy(c);
    }
}

/* Readability must wait for the first missing STORE completion even when all
 * later physical transactions have already become readable in the adapter. */
static void reversed_stores(void)
{
    struct vsr_member m = {1, VSR_MEMBER_FULL, 0};
    struct vsr_membership group = {0, &m, 1, 0};
    struct mem_cluster *c = mem_cluster_create();
    struct vsr_options o = mem_options(1, &group);
    struct mem_node *n = mem_cluster_add(c, &o);
    mem_node_time(n, 0);
    CHECK(mem_cluster_run(c, 100000) < 100000);
    for (unsigned i = 0; i < 8; i++) {
        struct vsr_blob b = {0};
        struct vsr_request r = {{{2, i + 1}, 1}, 0, VSR_REQUEST_COMMAND, 0, &b};
        struct vsr_event e = {VSR_EVENT_REQUEST, 0, i + 1, &r, 1};
        CHECK(mem_node_event(n, &e).consumed == 1);
    }
    for (unsigned tries = 0; tries < 100; tries++) {
        bool progress = false;
        for (size_t i = 0; i < mem_node_effects(n); i++) {
            const struct vsr_op *op = mem_node_effect(n, i);
            if (op->type == VSR_OP_LOAD) {
                CHECK(mem_node_execute(n, i, VSR_IO_OK));
                CHECK(mem_node_complete(n, i, VSR_IO_OK));
                progress = true;
                break;
            }
        }
        if (!progress)
            break;
    }
    size_t stores = 0;
    for (size_t i = 0; i < mem_node_effects(n); i++) {
        const struct vsr_op *op = mem_node_effect(n, i);
        if (op->type == VSR_OP_STORE) {
            stores++;
            CHECK(mem_node_execute(n, i, VSR_IO_OK));
        }
    }
    CHECK(stores >= 2);
    struct vsr_status before, after;
    vsr_get_status(mem_node_core(n), &before);
    for (size_t i = mem_node_effects(n); i > 0; i--) {
        const struct vsr_op *op = mem_node_effect(n, i - 1);
        if (op->type == VSR_OP_STORE) {
            uint64_t seq = ((const struct vsr_store *)op->data)->sequence;
            CHECK(mem_node_complete(n, i - 1, VSR_IO_OK));
            vsr_get_status(mem_node_core(n), &after);
            if (seq > before.stored_sequence + 1)
                CHECK(after.stored_sequence == before.stored_sequence);
        }
    }
    CHECK(mem_cluster_run(c, 100000) < 100000);
    CHECK(mem_node_replies(n) == 8);
    mem_cluster_check(c);
    mem_cluster_destroy(c);
}

/* The seeded profile 2 configuration: the documented minimum lease and
 * payload budgets for maximum-size commands, one log cache slot, and one entry
 * per batch, so every lagging peer and every replay competes for one entry. */
static struct vsr_options minimum_options(uint64_t replica,
                                          const struct vsr_membership *group,
                                          uint32_t durability)
{
    struct vsr_options o = mem_options(replica, group);
    struct vsr_limits *l = &o.limits;
    o.durability = durability;
    l->operations = 4;
    l->transfers = 1;
    l->input_leases = l->transfers + 8u;
    l->log_cache_entries = 1;
    l->batch_entries = 1;
    l->client_cache_entries = 1;
    l->command_bytes = 32;
    l->result_bytes = 18;
    l->manifest_bytes = 8;
    l->message_bytes = 40;
    l->pinned_payload_bytes = l->message_bytes + 2 * l->command_bytes +
                              (l->transfers + 3) * l->manifest_bytes +
                              l->result_bytes;
    {
        struct vsr_layout layout;
        struct vsr_options insufficient = o;
        insufficient.limits.input_leases--;
        CHECK(vsr_layout(&insufficient, &layout) == VSR_ELIMIT);
        insufficient = o;
        insufficient.limits.pinned_payload_bytes--;
        CHECK(vsr_layout(&insufficient, &layout) == VSR_ELIMIT);
        CHECK(vsr_layout(&o, &layout) == VSR_OK);
    }
    return o;
}

struct policy {
    uint64_t isolated; /* Every message to a replica in this mask is dropped. */
};

static uint64_t replica_bit(uint64_t replica)
{
    return UINT64_C(1) << replica;
}

struct group {
    struct mem_cluster *cluster;
    struct mem_node *nodes[3];
    uint32_t count;
    uint64_t now;
};

static struct vsr_status status(struct mem_node *node)
{
    struct vsr_status result;
    vsr_get_status(mem_node_core(node), &result);
    CHECK(result.failure.code == VSR_FAILURE_NONE);
    return result;
}

/* Runs until idle. A configuration that never idles is a livelock. */
static void settle(struct group *g, const struct policy *policy)
{
    for (unsigned turn = 0; turn < 100000; turn++) {
        bool progress = false;
        for (uint32_t i = 0; i < g->count; i++) {
            struct mem_node *node = g->nodes[i];
            struct mem_step step = mem_node_event(node, NULL);
            CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
            progress |=
                step.emitted != 0 || (step.flags & VSR_UPDATE_MORE) != 0;
            for (size_t j = 0; j < mem_node_effects(node); j++) {
                if (mem_node_complete(node, j, VSR_IO_OK)) {
                    progress = true;
                    break;
                }
            }
        }
        if (mem_cluster_messages(g->cluster) != 0) {
            uint64_t to;
            (void)mem_cluster_message(g->cluster, 0, &to);
            if ((policy->isolated & replica_bit(to)) != 0) {
                mem_cluster_drop(g->cluster, 0);
                progress = true;
            } else if (mem_cluster_deliver(g->cluster, 0)) {
                progress = true;
            }
        }
        mem_cluster_check(g->cluster);
        if (!progress)
            return;
    }
    CHECK(false);
}

/* Isolated replicas receive no time either, so their election timers cannot
 * force views on the reachable members. */
static void tick(struct group *g, const struct policy *policy)
{
    settle(g, policy);
    g->now += 5;
    for (uint32_t i = 0; i < g->count; i++) {
        if ((policy->isolated & replica_bit(i + 1)) != 0)
            continue;
        for (unsigned attempt = 0;; attempt++) {
            CHECK(attempt < 1000);
            if (mem_node_time(g->nodes[i], g->now).consumed == 1)
                break;
        }
    }
    settle(g, policy);
}

static void command(struct mem_node *node, uint64_t client, uint64_t route)
{
    unsigned char bytes[32];
    for (unsigned i = 0; i < sizeof(bytes); i++)
        bytes[i] = (unsigned char)(client + i);
    const struct vsr_span span = {bytes, sizeof(bytes)};
    const struct vsr_blob body = {&span, sizeof(bytes), 1, 0};
    const struct vsr_request request = {
        {{7, client}, 1}, 0, VSR_REQUEST_COMMAND, 0, &body};
    const struct vsr_event event = {VSR_EVENT_REQUEST, 0, route, &request, 1};
    CHECK(mem_node_event(node, &event).consumed == 1);
}

static void hint(struct mem_node *node)
{
    const struct vsr_event event = {VSR_EVENT_CHECKPOINT, 0, 0, NULL, 0};
    CHECK(mem_node_event(node, &event).consumed == 1);
}

static struct group boot(uint32_t durability, bool witness,
                         const struct policy *policy)
{
    struct group g = {mem_cluster_create(), {0}, 3, 0};
    struct vsr_member members[] = {
        {1, VSR_MEMBER_FULL, 0},
        {2, VSR_MEMBER_FULL, 0},
        {3, witness ? VSR_MEMBER_WITNESS : VSR_MEMBER_FULL, 0}};
    const struct vsr_membership group = {0, members, 3, 1};
    for (uint32_t i = 0; i < 3; i++) {
        struct vsr_options o = minimum_options(i + 1, &group, durability);
        g.nodes[i] = mem_cluster_add(g.cluster, &o);
        mem_node_output_capacity(g.nodes[i], 1);
        CHECK(mem_node_time(g.nodes[i], 0).consumed == 1);
    }
    settle(&g, policy);
    for (uint32_t i = 0; i < 3; i++)
        CHECK(status(g.nodes[i]).state == VSR_STATE_NORMAL);
    CHECK(status(g.nodes[0]).primary == 1);
    return g;
}

static bool converged(struct group *g, const struct policy *policy,
                      uint64_t committed)
{
    for (uint32_t i = 0; i < g->count; i++) {
        const struct vsr_status current = status(g->nodes[i]);
        if ((policy->isolated & replica_bit(i + 1)) != 0)
            continue;
        if (current.state != VSR_STATE_NORMAL ||
            current.committed < committed ||
            (current.role == VSR_MEMBER_FULL && current.applied < committed))
            return false;
    }
    return true;
}

static void converge(struct group *g, const struct policy *policy,
                     uint64_t committed)
{
    for (unsigned round = 0; round < 200 && !converged(g, policy, committed);
         round++)
        tick(g, policy);
    CHECK(converged(g, policy, committed));
}

/* Two peers lagging at different positions share the single cache slot with
 * two pinned proposals and a deferred checkpoint hint. Optional maintenance
 * may neither evict an entry that a peer resend or commit notification
 * reloads, nor may serving one peer evict the entry just loaded for another;
 * either turns fair retries into an endless load/evict cycle in which no
 * PREPARE is ever built, so the pinned proposals never commit. */
static void lagging_peers_minimum_cache(void)
{
    for (uint32_t durability = 0; durability < 2; durability++) {
        const struct policy open = {0};
        const struct policy second = {replica_bit(2)};
        const struct policy third = {replica_bit(3)};
        const struct policy both = {replica_bit(2) | replica_bit(3)};
        struct group g = boot(durability, false, &open);
        command(g.nodes[0], 1, 1);
        converge(&g, &open, 1);
        command(g.nodes[0], 2, 2);
        settle(&g, &third);
        CHECK(status(g.nodes[0]).applied == 2);
        CHECK(status(g.nodes[2]).committed == 1);
        /* The third replica fills its gap through catch-up before acking. */
        command(g.nodes[0], 3, 3);
        converge(&g, &second, 3);
        CHECK(status(g.nodes[1]).committed == 2);
        command(g.nodes[0], 4, 4);
        settle(&g, &both);
        command(g.nodes[0], 5, 5);
        settle(&g, &both);
        CHECK(status(g.nodes[0]).committed == 3);
        CHECK(status(g.nodes[0]).applied == 3);
        hint(g.nodes[0]);
        settle(&g, &both);
        CHECK(status(g.nodes[0]).checkpoint_op == 0);
        converge(&g, &open, 5);
        /* The deferred hint is honoured once the pinned requests resolve. */
        for (unsigned round = 0;
             round < 200 && status(g.nodes[0]).checkpoint_op == 0; round++)
            tick(&g, &open);
        CHECK(status(g.nodes[0]).checkpoint_op >= 3);
        CHECK(mem_node_replies(g.nodes[0]) == 5);
        mem_cluster_destroy(g.cluster);
    }
}

/* A primary that trimmed below an unreachable witness's position must still
 * announce commits and apply its own log; once reachable, the witness with an
 * empty log adopts the newer anchor without waiting for coverage of entries it
 * never retained. */
static void witness_rejoins_trimmed_primary(void)
{
    for (uint32_t durability = 0; durability < 2; durability++) {
        const struct policy open = {0};
        const struct policy isolated = {replica_bit(3)};
        struct group g = boot(durability, true, &isolated);
        for (uint64_t i = 1; i <= 3; i++) {
            command(g.nodes[0], i, i);
            converge(&g, &isolated, i);
        }
        CHECK(status(g.nodes[2]).committed == 0);
        hint(g.nodes[0]);
        for (unsigned round = 0;
             round < 200 && status(g.nodes[0]).checkpoint_op != 3; round++)
            tick(&g, &isolated);
        CHECK(status(g.nodes[0]).checkpoint_op == 3);
        CHECK(status(g.nodes[1]).checkpoint_op == 0);
        command(g.nodes[0], 4, 4);
        converge(&g, &isolated, 4);
        CHECK(mem_node_replies(g.nodes[0]) == 4);
        CHECK(status(g.nodes[2]).committed == 0);
        converge(&g, &open, 4);
        CHECK(status(g.nodes[2]).checkpoint_op == 3);
        command(g.nodes[0], 5, 5);
        converge(&g, &open, 5);
        mem_cluster_destroy(g.cluster);
    }
}

int main(void)
{
    minimum_capacity();
    reversed_stores();
    lagging_peers_minimum_cache();
    witness_rejoins_trimmed_primary();
    return 0;
}
