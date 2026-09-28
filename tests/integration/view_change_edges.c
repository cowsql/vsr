#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <stdint.h>
#include <string.h>

/* View change, recovery, and catch-up edge paths derived from
 * docs/protocol.md and docs/vsr-api.md. Every scenario is a deterministic
 * schedule over the in-memory host: message verdicts come from a per-scenario
 * filter, logical clocks are per node so time skew is explicit, and effects
 * complete in order unless a scenario holds a specific one. */

enum { MAX_NODES = 5 };
enum verdict { DELIVER, HOLD, DROP };

struct group;
typedef enum verdict (*filter_fn)(struct group *g, const struct vsr_message *m,
                                  uint64_t to);
typedef bool (*hold_fn)(struct group *g, struct mem_node *node,
                        const struct vsr_op *op);

struct group {
    struct mem_cluster *cluster;
    struct mem_node *nodes[MAX_NODES];
    struct vsr_member members[MAX_NODES];
    struct vsr_membership membership;
    uint32_t count;
    uint64_t clock[MAX_NODES];
    filter_fn filter;
    hold_fn hold;
    unsigned int phase;
    uint64_t drop_from;  /* Isolate a sender; 0 disables. */
    uint32_t count_type; /* Delivered messages of this type from count_from */
    uint64_t count_from; /* are tallied in counted; UINT32_MAX disables. */
    size_t counted;
};

static struct vsr_status status(struct mem_node *node)
{
    struct vsr_status result;
    vsr_get_status(mem_node_core(node), &result);
    return result;
}

static struct vsr_status healthy(struct mem_node *node)
{
    struct vsr_status result = status(node);
    CHECK(result.failure.code == VSR_FAILURE_NONE);
    return result;
}

static uint64_t stored_log_end(struct mem_node *node)
{
    struct mem_store *store = mem_node_store(node);
    const struct vsr_recovered *recovered =
        mem_store_recovered(store, mem_store_readable(store));
    CHECK(recovered != NULL);
    return recovered->log_end;
}

static size_t find_effect(struct mem_node *node, uint32_t type,
                          uint32_t load_type)
{
    for (size_t i = 0; i < mem_node_effects(node); i++) {
        const struct vsr_op *op = mem_node_effect(node, i);
        if (op->type != type)
            continue;
        if (type == VSR_OP_LOAD &&
            ((const struct vsr_store_read *)op->data)->type != load_type)
            continue;
        return i;
    }
    return SIZE_MAX;
}

static size_t find_message(const struct group *g, uint32_t type, uint64_t from,
                           uint64_t to)
{
    for (size_t i = 0; i < mem_cluster_messages(g->cluster); i++) {
        uint64_t destination;
        const struct vsr_message *m =
            mem_cluster_message(g->cluster, i, &destination);
        if (m->type == type && (from == 0 || m->from == from) &&
            (to == 0 || destination == to))
            return i;
    }
    return SIZE_MAX;
}

static void drive(struct group *g)
{
    for (uint32_t turn = 0; turn < 200000; turn++) {
        bool progress = false;
        for (uint32_t i = 0; i < g->count; i++) {
            struct mem_node *node = g->nodes[i];
            if (!mem_node_alive(node))
                continue;
            const struct mem_step step = mem_node_event(node, NULL);
            CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
            progress |=
                step.emitted != 0 || (step.flags & VSR_UPDATE_MORE) != 0;
            for (size_t j = 0; j < mem_node_effects(node); j++) {
                if (g->hold != NULL &&
                    g->hold(g, node, mem_node_effect(node, j)))
                    continue;
                if (mem_node_complete(node, j, VSR_IO_OK)) {
                    progress = true;
                    break;
                }
            }
        }
        for (size_t i = 0; i < mem_cluster_messages(g->cluster); i++) {
            uint64_t to;
            const struct vsr_message *m =
                mem_cluster_message(g->cluster, i, &to);
            const uint32_t type = m->type;
            const uint64_t from = m->from;
            enum verdict verdict =
                g->filter == NULL ? DELIVER : g->filter(g, m, to);
            if (verdict == HOLD)
                continue;
            struct mem_node *destination = mem_cluster_find(g->cluster, to);
            if (verdict == DROP || from == g->drop_from ||
                destination == NULL || !mem_node_alive(destination)) {
                mem_cluster_drop(g->cluster, i);
                progress = true;
                break;
            }
            if (mem_cluster_deliver(g->cluster, i)) {
                if (type == g->count_type && from == g->count_from)
                    g->counted++;
                progress = true;
                break;
            }
        }
        mem_cluster_check(g->cluster);
        if (!progress)
            return;
    }
    CHECK(false);
}

static void time_node(struct group *g, uint32_t index, uint64_t now)
{
    struct mem_node *node = g->nodes[index];
    CHECK(now >= g->clock[index] || !mem_node_alive(node));
    for (uint32_t attempt = 0; attempt < 1000; attempt++) {
        const struct mem_step step = mem_node_time(node, now);
        CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
        if (step.consumed == 1) {
            g->clock[index] = now;
            return;
        }
        drive(g);
    }
    CHECK(false);
}

/* Advance the clocks named by mask by delta each; per-node clocks make time
 * skew between replicas an explicit part of every schedule. */
static void advance(struct group *g, uint32_t mask, uint64_t delta)
{
    for (uint32_t i = 0; i < g->count; i++) {
        if ((mask & (1u << i)) != 0 && mem_node_alive(g->nodes[i]))
            time_node(g, i, g->clock[i] + delta);
    }
}

static void settle(struct group *g, uint32_t mask,
                   bool (*done)(const struct group *g))
{
    for (uint32_t round = 0; round < 400; round++) {
        drive(g);
        if (done(g))
            return;
        advance(g, mask, 5);
    }
    CHECK(false);
}

static bool all_normal(const struct group *g)
{
    for (uint32_t i = 0; i < g->count; i++) {
        if (!mem_node_alive(g->nodes[i]))
            continue;
        const struct vsr_status s = healthy(g->nodes[i]);
        if (s.state != VSR_STATE_NORMAL)
            return false;
    }
    return true;
}

static bool all_applied_one(const struct group *g)
{
    for (uint32_t i = 0; i < g->count; i++) {
        if (!mem_node_alive(g->nodes[i]))
            continue;
        const struct vsr_status s = healthy(g->nodes[i]);
        if (s.state != VSR_STATE_NORMAL || s.committed < 1 || s.applied < 1)
            return false;
    }
    return true;
}

static struct group create(uint32_t count, uint32_t durability, bool timed,
                           void (*tweak)(struct vsr_options *options))
{
    struct group g;
    memset(&g, 0, sizeof(g));
    g.cluster = mem_cluster_create();
    g.count = count;
    g.count_type = UINT32_MAX;
    for (uint32_t i = 0; i < count; i++)
        g.members[i] = (struct vsr_member){i + 1, VSR_MEMBER_FULL, 0};
    g.membership =
        (struct vsr_membership){0, g.members, count, (count - 1) / 2};
    for (uint32_t i = 0; i < count; i++) {
        struct vsr_options options = mem_options(i + 1, &g.membership);
        options.durability = durability;
        if (tweak != NULL)
            tweak(&options);
        g.nodes[i] = mem_cluster_add(g.cluster, &options);
    }
    if (timed) {
        for (uint32_t i = 0; i < count; i++)
            time_node(&g, i, 0);
    }
    return g;
}

static void boot(struct group *g)
{
    drive(g);
    for (uint32_t i = 0; i < g->count; i++) {
        if (mem_node_alive(g->nodes[i]))
            CHECK(healthy(g->nodes[i]).state == VSR_STATE_NORMAL);
    }
}

static void submit(struct mem_node *node, uint64_t client, uint64_t number,
                   uint64_t route)
{
    const struct vsr_span span = {"edge", 4};
    const struct vsr_blob body = {&span, 4, 1, 0};
    const struct vsr_request request = {
        {{client, 1}, number}, 0, VSR_REQUEST_COMMAND, 0, &body};
    const struct vsr_event event = {VSR_EVENT_REQUEST, VSR_IO_OK, route,
                                    &request, 1};
    CHECK(mem_node_event(node, &event).consumed == 1);
}

static void inject(struct mem_node *node, const struct vsr_message *message)
{
    const struct vsr_event event = {VSR_EVENT_MESSAGE, 0, 0, message, 1};
    const struct mem_step step = mem_node_event(node, &event);
    CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
    CHECK(step.consumed == 1);
}

static struct vsr_message control(uint64_t from, uint32_t type, uint64_t view,
                                  uint64_t number)
{
    return (struct vsr_message){{1, 1}, 0, view, from, type, 0, number, NULL};
}

static void restart(struct group *g, uint32_t index, uint64_t generation)
{
    struct mem_node *node = g->nodes[index];
    mem_node_crash(node);
    CHECK(mem_node_restart(node, (struct vsr_id){index + 1, generation}) ==
          VSR_OK);
    g->clock[index] = 0;
}

/* docs/protocol.md: "A recovering would-be primary cannot recover from
 * itself; peers must advance to another view." */
static void recovering_primary(void)
{
    struct group g = create(3, VSR_REPLICATED, true, NULL);
    boot(&g);
    CHECK(healthy(g.nodes[0]).primary == 1);
    restart(&g, 0, 2);
    time_node(&g, 0, 0);
    drive(&g);
    CHECK(healthy(g.nodes[0]).state == VSR_STATE_RECOVERING);
    /* Recovery retries against peers that still report view 0 cannot
     * complete: the only primary they know is the recovering replica. */
    for (unsigned int round = 0; round < 6; round++) {
        advance(&g, 1u << 0, 5);
        drive(&g);
        CHECK(healthy(g.nodes[0]).state == VSR_STATE_RECOVERING);
        CHECK(healthy(g.nodes[1]).view == 0 && healthy(g.nodes[2]).view == 0);
    }
    settle(&g, UINT32_MAX, all_normal);
    for (uint32_t i = 0; i < 3; i++) {
        const struct vsr_status s = healthy(g.nodes[i]);
        CHECK(s.view >= 1 && s.primary != 1);
    }
    submit(g.nodes[healthy(g.nodes[0]).primary - 1], 100, 1, 1);
    settle(&g, UINT32_MAX, all_applied_one);
    mem_cluster_destroy(g.cluster);
}

static enum verdict skew_filter(struct group *g, const struct vsr_message *m,
                                uint64_t to)
{
    (void)to;
    if (m->type == VSR_MSG_DO_VIEW_CHANGE && m->from == 3 && m->view == 1)
        return g->phase == 1 ? HOLD : DROP;
    return DELIVER;
}

/* docs/protocol.md: "Learning a higher view fences the old view before any
 * response endorsing the new one" and "A view change can be retried at a
 * higher view when its primary or required offers remain unavailable."
 * Only replica 3's clock advances, so replica 2 enters each view purely from
 * the START_VIEW_CHANGE it receives. */
static void skew_and_interrupt(void)
{
    struct group g = create(3, VSR_DURABLE, true, NULL);
    boot(&g);
    g.filter = skew_filter;
    g.phase = 1;
    mem_node_crash(g.nodes[0]);
    time_node(&g, 2, 60);
    drive(&g);
    CHECK(healthy(g.nodes[2]).state == VSR_STATE_VIEW_CHANGE);
    CHECK(healthy(g.nodes[2]).view == 1);
    CHECK(healthy(g.nodes[1]).state == VSR_STATE_VIEW_CHANGE);
    CHECK(healthy(g.nodes[1]).view == 1 && healthy(g.nodes[1]).primary == 2);
    CHECK(g.clock[1] == 0);
    /* The withheld offer keeps view 1 incomplete until replica 3's election
     * timer expires again, which retries at view 2 with itself as primary. */
    time_node(&g, 2, 120);
    drive(&g);
    CHECK(healthy(g.nodes[2]).view == 2 && healthy(g.nodes[2]).primary == 3);
    CHECK(healthy(g.nodes[1]).view == 2 && healthy(g.nodes[1]).primary == 3);
    g.phase = 2;
    settle(&g, UINT32_MAX, all_normal);
    CHECK(healthy(g.nodes[1]).view == 2 && healthy(g.nodes[2]).view == 2);
    submit(g.nodes[2], 100, 1, 1);
    settle(&g, UINT32_MAX, all_applied_one);
    mem_cluster_destroy(g.cluster);
}

/* docs/vsr-api.md: "The first TIME establishes the origin; protocol timers
 * and read deadlines do not fire beforehand." A replica that has never seen
 * TIME still enters the higher view it learns, but its election retry waits
 * for the origin plus view_timeout_ns. */
static void timers_wait_for_origin(void)
{
    struct group g = create(3, VSR_DURABLE, false, NULL);
    boot(&g);
    g.drop_from = 2;
    const struct vsr_message change =
        control(3, VSR_MSG_START_VIEW_CHANGE, 1, 0);
    inject(g.nodes[1], &change);
    drive(&g);
    CHECK(healthy(g.nodes[1]).state == VSR_STATE_VIEW_CHANGE);
    CHECK(healthy(g.nodes[1]).view == 1);
    for (unsigned int i = 0; i < 200; i++) {
        const struct mem_step step = mem_node_event(g.nodes[1], NULL);
        CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
        CHECK(step.deadline == VSR_NO_DEADLINE);
    }
    CHECK(healthy(g.nodes[1]).view == 1);
    time_node(&g, 1, 0);
    drive(&g);
    CHECK(healthy(g.nodes[1]).view == 1);
    time_node(&g, 1, 49);
    drive(&g);
    CHECK(healthy(g.nodes[1]).view == 1);
    time_node(&g, 1, 50);
    drive(&g);
    CHECK(healthy(g.nodes[1]).view == 2);
    CHECK(healthy(g.nodes[1]).state == VSR_STATE_VIEW_CHANGE);
    mem_cluster_destroy(g.cluster);
}

/* Phase 1 leaves op 1 prepared only on replicas 1 and 3 and uncommitted.
 * Phase 2 withholds the new primary's GET_LOG. Phase 3 delivers everything. */
static enum verdict primary_filter(struct group *g, const struct vsr_message *m,
                                   uint64_t to)
{
    if (g->phase == 1) {
        if ((m->type == VSR_MSG_PREPARE || m->type == VSR_MSG_COMMIT) &&
            to == 2)
            return DROP;
        if (m->type == VSR_MSG_PREPARE_OK && m->from == 3)
            return DROP;
    }
    if (g->phase == 2 && m->type == VSR_MSG_GET_LOG)
        return HOLD;
    return DELIVER;
}

/* docs/vsr-api.md: "Unknown/expired revisions or unavailable ranges produce
 * STATE_UNAVAILABLE ... The receiver revalidates that offer." and "Source
 * restart changes the revision incarnation." docs/protocol.md: "An expired
 * revision forces renewed protocol validation. A view change can be retried
 * at a higher view when its primary or required offers remain unavailable."
 * The offer a DO_VIEW_CHANGE sender advertises stays referenced by its own
 * round, so the revision becomes unavailable through the source's restart. */
static void unavailable_offer_primary(void)
{
    struct group g = create(3, VSR_DURABLE, true, NULL);
    boot(&g);
    g.filter = primary_filter;
    g.phase = 1;
    submit(g.nodes[0], 100, 1, 1);
    drive(&g);
    CHECK(stored_log_end(g.nodes[2]) == 2 && stored_log_end(g.nodes[1]) == 1);
    CHECK(healthy(g.nodes[0]).committed == 0);
    mem_node_crash(g.nodes[0]);
    g.phase = 2;
    time_node(&g, 1, 60);
    time_node(&g, 2, 60);
    drive(&g);
    CHECK(healthy(g.nodes[1]).state == VSR_STATE_VIEW_CHANGE);
    CHECK(healthy(g.nodes[1]).view == 1 && healthy(g.nodes[1]).primary == 2);
    CHECK(find_message(&g, VSR_MSG_GET_LOG, 2, 3) != SIZE_MAX);
    restart(&g, 2, 2);
    time_node(&g, 2, 60);
    g.phase = 3;
    drive(&g);
    settle(&g, UINT32_MAX, all_applied_one);
    for (uint32_t i = 1; i < 3; i++) {
        const struct vsr_status s = healthy(g.nodes[i]);
        CHECK(s.view == 2 && s.primary == 3 && s.committed == 1);
        CHECK(mem_node_history(g.nodes[i], 1)->request.client.hi == 100);
    }
    mem_cluster_destroy(g.cluster);
}

/* Phase 1 leaves op 1 prepared only on replicas 1 and 2 and uncommitted.
 * Phase 2 withholds the lagging backup's GET_LOG. Phase 3 delivers all. */
static enum verdict backup_filter(struct group *g, const struct vsr_message *m,
                                  uint64_t to)
{
    if (g->phase == 1) {
        if ((m->type == VSR_MSG_PREPARE || m->type == VSR_MSG_COMMIT) &&
            to == 3)
            return DROP;
        if (m->type == VSR_MSG_PREPARE_OK && m->from == 2)
            return DROP;
    }
    if (g->phase == 2 && m->type == VSR_MSG_GET_LOG)
        return HOLD;
    return DELIVER;
}

/* docs/vsr-api.md: "`transfer_timeout_ns` expires idle offers" and
 * "Unknown/expired revisions ... produce STATE_UNAVAILABLE ... The receiver
 * revalidates that offer." A backup whose START_VIEW fetch names an offer the
 * primary let expire rediscovers the current offer and still installs the
 * same history. */
static void expired_offer_backup(void)
{
    struct group g = create(3, VSR_DURABLE, true, NULL);
    boot(&g);
    g.filter = backup_filter;
    g.phase = 1;
    submit(g.nodes[0], 100, 1, 1);
    drive(&g);
    CHECK(stored_log_end(g.nodes[1]) == 2 && stored_log_end(g.nodes[2]) == 1);
    mem_node_crash(g.nodes[0]);
    g.phase = 2;
    time_node(&g, 1, 60);
    time_node(&g, 2, 60);
    drive(&g);
    CHECK(healthy(g.nodes[1]).state == VSR_STATE_NORMAL);
    CHECK(healthy(g.nodes[1]).view == 1 && healthy(g.nodes[1]).primary == 2);
    CHECK(healthy(g.nodes[2]).state == VSR_STATE_VIEW_CHANGE);
    CHECK(find_message(&g, VSR_MSG_GET_LOG, 3, 2) != SIZE_MAX);
    /* Idle for longer than transfer_timeout_ns on the primary alone. */
    time_node(&g, 1, 170);
    drive(&g);
    g.phase = 3;
    drive(&g);
    settle(&g, UINT32_MAX, all_applied_one);
    for (uint32_t i = 1; i < 3; i++) {
        const struct vsr_status s = healthy(g.nodes[i]);
        CHECK(s.view == 1 && s.primary == 2 && s.committed == 1);
        CHECK(mem_node_history(g.nodes[i], 1)->request.client.hi == 100);
    }
    mem_cluster_destroy(g.cluster);
}

static enum verdict tie_filter(struct group *g, const struct vsr_message *m,
                               uint64_t to)
{
    (void)to;
    if (g->phase == 1 &&
        (m->type == VSR_MSG_DO_VIEW_CHANGE || m->type == VSR_MSG_GET_LOG))
        return HOLD;
    return DELIVER;
}

/* docs/protocol.md: "Offers are ranked by (last_normal_view, log_end) ...
 * Ties describe the same history; choosing the lowest source ID makes fetch
 * scheduling deterministic." The primary of view 4 (replica 5) receives the
 * offer of replica 4 first, then replica 3's equal offer, and fetches the
 * selected log from replica 3. */
static void lowest_source_wins(void)
{
    struct group g = create(5, VSR_DURABLE, true, NULL);
    boot(&g);
    submit(g.nodes[0], 100, 1, 1);
    settle(&g, UINT32_MAX, all_applied_one);
    g.filter = tie_filter;
    g.phase = 1;
    for (uint32_t i = 0; i < 5; i++) {
        const struct vsr_message change =
            control(i == 0 ? 2 : 1, VSR_MSG_START_VIEW_CHANGE, 4, 0);
        inject(g.nodes[i], &change);
    }
    drive(&g);
    for (uint32_t i = 0; i < 5; i++) {
        CHECK(healthy(g.nodes[i]).view == 4);
        CHECK(healthy(g.nodes[i]).primary == 5);
        CHECK(healthy(g.nodes[i]).state == VSR_STATE_VIEW_CHANGE);
    }
    size_t index = find_message(&g, VSR_MSG_DO_VIEW_CHANGE, 4, 5);
    CHECK(index != SIZE_MAX && mem_cluster_deliver(g.cluster, index));
    drive(&g);
    CHECK(find_message(&g, VSR_MSG_GET_LOG, 5, 0) == SIZE_MAX);
    index = find_message(&g, VSR_MSG_DO_VIEW_CHANGE, 3, 5);
    CHECK(index != SIZE_MAX && mem_cluster_deliver(g.cluster, index));
    drive(&g);
    CHECK(find_message(&g, VSR_MSG_GET_LOG, 5, 3) != SIZE_MAX);
    for (uint64_t to = 1; to <= 5; to++) {
        if (to != 3)
            CHECK(find_message(&g, VSR_MSG_GET_LOG, 5, to) == SIZE_MAX);
    }
    g.phase = 2;
    settle(&g, UINT32_MAX, all_applied_one);
    for (uint32_t i = 0; i < 5; i++) {
        const struct vsr_status s = healthy(g.nodes[i]);
        CHECK(s.view == 4 && s.primary == 5);
    }
    mem_cluster_destroy(g.cluster);
}

/* include/vsr.h: "Counters never wrap." and VSR_FAILURE_EXHAUSTED: "Counter/
 * time domain exhausted; never wrap." A retry at a higher view from the last
 * usable view fences the replica instead of wrapping. */
static void view_domain_exhausted(void)
{
    struct group g = create(3, VSR_DURABLE, true, NULL);
    boot(&g);
    g.drop_from = 1;
    const struct vsr_message change =
        control(2, VSR_MSG_START_VIEW_CHANGE, UINT64_MAX - 1, 0);
    inject(g.nodes[0], &change);
    drive(&g);
    CHECK(healthy(g.nodes[0]).state == VSR_STATE_VIEW_CHANGE);
    CHECK(healthy(g.nodes[0]).view == UINT64_MAX - 1);
    time_node(&g, 0, 60);
    drive(&g);
    const struct vsr_status s = status(g.nodes[0]);
    CHECK(s.state == VSR_STATE_FAILED);
    CHECK(s.failure.code == VSR_FAILURE_EXHAUSTED);
    CHECK(s.failure.operation == 0);
    CHECK(healthy(g.nodes[1]).view == 0 && healthy(g.nodes[2]).view == 0);
    mem_cluster_destroy(g.cluster);
}

static void tiny_cache(struct vsr_options *options)
{
    options->limits.log_cache_entries = 1;
    options->limits.batch_entries = 1;
}

/* Phase 1: only replica 5 receives PREPARE and its acknowledgments vanish.
 * Phase 2: replica 5 is partitioned. Phase 3: everything is delivered. */
static enum verdict suffix_filter(struct group *g, const struct vsr_message *m,
                                  uint64_t to)
{
    if (g->phase == 1) {
        if (m->type == VSR_MSG_PREPARE && to != 5)
            return DROP;
        if (m->type == VSR_MSG_PREPARE_OK && m->from == 5)
            return DROP;
    }
    if (g->phase == 2 && (to == 5 || m->from == 5))
        return DROP;
    return DELIVER;
}

static bool hold_log_loads(struct group *g, struct mem_node *node,
                           const struct vsr_op *op)
{
    return mem_node_id(node) == g->count_from && op->type == VSR_OP_LOAD &&
           ((const struct vsr_store_read *)op->data)->type == VSR_LOAD_LOG;
}

enum load_variant { LOAD_COMPLETES, LOAD_RETRIES, LOAD_ABSENT };

/* Complete the held indexed LOAD on node according to the variant. RETRY must
 * be retried with a new operation; NOT_FOUND on retained history is fatal
 * and latches the operation. Returns false once the node is fenced. */
static bool complete_held_load(struct group *g, struct mem_node *node,
                               enum load_variant variant, uint64_t first,
                               uint64_t end)
{
    size_t index = find_effect(node, VSR_OP_LOAD, VSR_LOAD_LOG);
    CHECK(index != SIZE_MAX);
    const struct vsr_store_read *read = mem_node_effect(node, index)->data;
    CHECK(read->first == first && read->end == end);
    CHECK(read->sequence == healthy(node).stored_sequence);
    const uint64_t id = mem_node_effect(node, index)->id;
    if (variant == LOAD_RETRIES) {
        CHECK(mem_node_complete(node, index, VSR_IO_RETRY));
        CHECK(healthy(node).failure.code == VSR_FAILURE_NONE);
        drive(g);
        index = find_effect(node, VSR_OP_LOAD, VSR_LOAD_LOG);
        CHECK(index != SIZE_MAX);
        CHECK(mem_node_effect(node, index)->id != id);
        read = mem_node_effect(node, index)->data;
        CHECK(read->first == first && read->end == end);
    }
    if (variant == LOAD_ABSENT) {
        CHECK(mem_node_complete(node, index, VSR_IO_NOT_FOUND));
        const struct vsr_status s = status(node);
        CHECK(s.state == VSR_STATE_FAILED);
        CHECK(s.failure.code == VSR_FAILURE_STORAGE);
        CHECK(s.failure.operation == id);
        CHECK(s.failure.operation_type == VSR_OP_LOAD);
        CHECK(s.failure.status == VSR_IO_NOT_FOUND);
        return false;
    }
    CHECK(mem_node_complete(node, index, VSR_IO_OK));
    g->hold = NULL;
    return true;
}

/* include/vsr.h: "Only committed positions are permanent; an uncommitted
 * suffix can be replaced." docs/vsr-api.md: "TRUNCATE followed by APPEND
 * replaces an uncommitted suffix" and LOAD rules: "an unexplained hole in
 * retained local history ... is fatal", "Retry transient unavailability".
 * Replica 5 holds two uncommitted entries from the crashed primary; its
 * one-entry cache no longer holds op 1, so the comparison against the new
 * committed history goes through an indexed LOAD of its own store. */
static void uncommitted_suffix_replaced(enum load_variant variant)
{
    struct group g = create(5, VSR_DURABLE, true, tiny_cache);
    boot(&g);
    g.filter = suffix_filter;
    g.phase = 1;
    submit(g.nodes[0], 100, 1, 1);
    drive(&g);
    submit(g.nodes[0], 101, 1, 2);
    drive(&g);
    CHECK(stored_log_end(g.nodes[4]) == 3 && stored_log_end(g.nodes[0]) == 3);
    CHECK(stored_log_end(g.nodes[1]) == 1);
    CHECK(healthy(g.nodes[0]).committed == 0);
    mem_node_crash(g.nodes[0]);
    g.phase = 2;
    for (uint32_t i = 1; i < 4; i++)
        time_node(&g, i, 60);
    settle(&g, 0x0eu, all_normal);
    CHECK(healthy(g.nodes[1]).view == 1 && healthy(g.nodes[1]).primary == 2);
    submit(g.nodes[1], 102, 1, 3);
    drive(&g);
    for (uint32_t i = 1; i < 4; i++)
        CHECK(healthy(g.nodes[i]).applied == 1);
    CHECK(healthy(g.nodes[4]).state == VSR_STATE_NORMAL);
    CHECK(healthy(g.nodes[4]).view == 0);
    g.phase = 3;
    g.count_from = 5;
    g.hold = hold_log_loads;
    /* The primary's retry resends PREPARE to the healed replica. */
    time_node(&g, 1, g.clock[1] + 10);
    drive(&g);
    CHECK(healthy(g.nodes[4]).view == 1);
    if (!complete_held_load(&g, g.nodes[4], variant, 1, 2)) {
        mem_cluster_destroy(g.cluster);
        return;
    }
    settle(&g, UINT32_MAX, all_applied_one);
    CHECK(stored_log_end(g.nodes[4]) == 2);
    CHECK(mem_node_history(g.nodes[4], 1)->request.client.hi == 102);
    CHECK(mem_node_history(g.nodes[4], 2) == NULL);
    CHECK(healthy(g.nodes[4]).view == 1 && healthy(g.nodes[4]).primary == 2);
    mem_cluster_destroy(g.cluster);
}

/* docs/protocol.md: "The primary fetches the complete selected history before
 * entering NORMAL and sending START_VIEW". When its own offer is selected the
 * history comes from its own indexed store: LOAD RETRY is retried, absence of
 * retained history is fatal. */
static void selected_local_log(enum load_variant variant)
{
    struct group g = create(3, VSR_DURABLE, true, NULL);
    boot(&g);
    submit(g.nodes[0], 100, 1, 1);
    settle(&g, UINT32_MAX, all_applied_one);
    mem_node_crash(g.nodes[0]);
    g.count_from = 2;
    g.hold = hold_log_loads;
    time_node(&g, 1, 60);
    time_node(&g, 2, 60);
    drive(&g);
    CHECK(healthy(g.nodes[1]).state == VSR_STATE_VIEW_CHANGE);
    CHECK(healthy(g.nodes[1]).view == 1 && healthy(g.nodes[1]).primary == 2);
    if (!complete_held_load(&g, g.nodes[1], variant, 1, 2)) {
        mem_cluster_destroy(g.cluster);
        return;
    }
    settle(&g, UINT32_MAX, all_applied_one);
    CHECK(healthy(g.nodes[1]).view == 1 && healthy(g.nodes[2]).view == 1);
    submit(g.nodes[1], 100, 2, 2);
    drive(&g);
    CHECK(healthy(g.nodes[1]).applied == 2);
    mem_cluster_destroy(g.cluster);
}

/* docs/protocol.md: "Wrong-cluster, unauthorized-for-this-round, stale, and
 * unsolicited messages cannot advance protocol state" and "No message can
 * appoint another primary." Each message is consumed and changes nothing. */
static void ignored_messages(void)
{
    struct group g = create(3, VSR_DURABLE, true, NULL);
    boot(&g);
    struct vsr_message messages[5];
    messages[0] = control(2, VSR_MSG_START_VIEW_CHANGE, 1, 0);
    messages[0].cluster = (struct vsr_id){9, 9};
    messages[1] = control(99, VSR_MSG_START_VIEW_CHANGE, 1, 0);
    messages[2] = control(1, VSR_MSG_START_VIEW_CHANGE, 1, 0);
    messages[3] = control(3, VSR_MSG_COMMIT, 0, 5);
    messages[4] = control(2, VSR_MSG_PREPARE_OK, 7, 0);
    for (size_t i = 0; i < 5; i++) {
        inject(g.nodes[0], &messages[i]);
        drive(&g);
        const struct vsr_status s = healthy(g.nodes[0]);
        CHECK(s.state == VSR_STATE_NORMAL && s.view == 0);
        CHECK(s.primary == 1 && s.committed == 0);
        CHECK(healthy(g.nodes[1]).view == 0 && healthy(g.nodes[2]).view == 0);
    }
    submit(g.nodes[0], 100, 1, 1);
    drive(&g);
    CHECK(healthy(g.nodes[0]).applied == 1);
    mem_cluster_destroy(g.cluster);
}

static enum verdict start_view_filter(struct group *g,
                                      const struct vsr_message *m, uint64_t to)
{
    (void)to;
    return g->phase == 1 && m->type == VSR_MSG_START_VIEW ? HOLD : DELIVER;
}

/* docs/protocol.md: "well-formed inapplicable messages are consumed and
 * ignored or answered" and "A backup acknowledges only in NORMAL after
 * installing that view's selected history." A retransmitted START_VIEW for
 * the installed view is answered with a fresh PREPARE_OK, nothing else. */
static void duplicate_start_view(void)
{
    struct group g = create(3, VSR_DURABLE, true, NULL);
    boot(&g);
    g.filter = start_view_filter;
    g.phase = 1;
    g.count_type = VSR_MSG_PREPARE_OK;
    g.count_from = 3;
    mem_node_crash(g.nodes[0]);
    time_node(&g, 1, 60);
    time_node(&g, 2, 60);
    drive(&g);
    CHECK(healthy(g.nodes[1]).state == VSR_STATE_NORMAL);
    CHECK(healthy(g.nodes[1]).view == 1);
    size_t index = find_message(&g, VSR_MSG_START_VIEW, 2, 3);
    CHECK(index != SIZE_MAX);
    mem_cluster_duplicate(g.cluster, index);
    CHECK(mem_cluster_deliver(g.cluster, index));
    drive(&g);
    CHECK(healthy(g.nodes[2]).state == VSR_STATE_NORMAL);
    CHECK(healthy(g.nodes[2]).view == 1);
    const size_t before = g.counted;
    CHECK(before >= 1);
    index = find_message(&g, VSR_MSG_START_VIEW, 2, 3);
    CHECK(index != SIZE_MAX);
    CHECK(mem_cluster_deliver(g.cluster, index));
    drive(&g);
    CHECK(g.counted == before + 1);
    CHECK(healthy(g.nodes[2]).state == VSR_STATE_NORMAL);
    CHECK(healthy(g.nodes[2]).view == 1);
    g.phase = 2;
    submit(g.nodes[1], 100, 1, 1);
    settle(&g, UINT32_MAX, all_applied_one);
    mem_cluster_destroy(g.cluster);
}

static enum verdict response_filter(struct group *g,
                                    const struct vsr_message *m, uint64_t to)
{
    return g->phase == 1 && m->type == VSR_MSG_RECOVERY_RESPONSE && to == 3
               ? HOLD
               : DELIVER;
}

/* docs/protocol.md: "Recovery does not downgrade VIEW_CHANGE to NORMAL, undo
 * RETIRED, or turn TRANSFERRING into readiness" and "Missing state requires
 * quorum recovery". A durable store that persisted RECOVERING before the
 * quorum answered resumes quorum recovery after another restart. */
static void restart_while_recovering(void)
{
    struct group g = create(3, VSR_DURABLE, true, NULL);
    mem_node_crash(g.nodes[2]);
    boot(&g);
    submit(g.nodes[0], 100, 1, 1);
    drive(&g);
    CHECK(healthy(g.nodes[0]).applied == 1 && healthy(g.nodes[1]).applied == 1);
    g.filter = response_filter;
    g.phase = 1;
    restart(&g, 2, 2);
    time_node(&g, 2, 0);
    drive(&g);
    CHECK(healthy(g.nodes[2]).state == VSR_STATE_RECOVERING);
    struct mem_store *store = mem_node_store(g.nodes[2]);
    CHECK(mem_store_durable(store) >= 1);
    const struct vsr_recovered *recovered =
        mem_store_recovered(store, mem_store_durable(store));
    CHECK(recovered != NULL && recovered->hard.state == VSR_HARD_RECOVERING);
    restart(&g, 2, 3);
    time_node(&g, 2, 0);
    drive(&g);
    CHECK(healthy(g.nodes[2]).state == VSR_STATE_RECOVERING);
    CHECK(healthy(g.nodes[2]).applied == 0);
    g.phase = 2;
    settle(&g, UINT32_MAX, all_applied_one);
    CHECK(mem_node_history(g.nodes[2], 1)->request.client.hi == 100);
    mem_cluster_destroy(g.cluster);
}

static enum verdict learner_filter(struct group *g, const struct vsr_message *m,
                                   uint64_t to)
{
    if (m->from == 4) {
        CHECK(m->type == VSR_MSG_GET_STATE);
        if (to == 1)
            g->counted++;
    }
    return DELIVER;
}

static bool learner_warmed(const struct group *g)
{
    const struct vsr_status s = healthy(g->nodes[3]);
    return s.state == VSR_STATE_WARMING && s.applied == 1 && s.committed == 1;
}

/* docs/vsr-api.md: "a WARMING learner rediscovers the members it knows every
 * heartbeat and catches up". A learner whose seed points at a dead primary
 * must rotate its discovery to another full member instead of stalling, and
 * never sends anything but discovery while warming. */
static void learner_rotates_donor(void)
{
    struct group g = create(3, VSR_DURABLE, true, NULL);
    boot(&g);
    mem_node_crash(g.nodes[0]);
    advance(&g, UINT32_MAX, 60);
    settle(&g, UINT32_MAX, all_normal);
    CHECK(healthy(g.nodes[1]).view == 1 && healthy(g.nodes[1]).primary == 2);
    submit(g.nodes[1], 100, 1, 1);
    settle(&g, UINT32_MAX, all_applied_one);
    /* The group was returned by value: point its membership at this copy. */
    g.membership.members = g.members;
    struct vsr_options options = mem_options(4, &g.membership);
    options.start_mode = VSR_START_JOIN;
    options.join_role = VSR_MEMBER_FULL;
    g.nodes[3] = mem_cluster_add(g.cluster, &options);
    g.count = 4;
    g.filter = learner_filter;
    time_node(&g, 3, 0);
    drive(&g);
    CHECK(g.counted >= 1);
    CHECK(healthy(g.nodes[3]).state == VSR_STATE_WARMING);
    CHECK(healthy(g.nodes[3]).applied == 0);
    settle(&g, UINT32_MAX, learner_warmed);
    CHECK(mem_node_history(g.nodes[3], 1)->request.client.hi == 100);
    mem_cluster_destroy(g.cluster);
}

int main(int argc, char **argv)
{
    CHECK(argc <= 2);
    const char *only = argc == 2 ? argv[1] : NULL;
#define SELECTED(name) (only == NULL || strcmp(only, name) == 0)
    if (SELECTED("recovering-primary"))
        recovering_primary();
    if (SELECTED("skew"))
        skew_and_interrupt();
    if (SELECTED("origin"))
        timers_wait_for_origin();
    if (SELECTED("unavailable-primary"))
        unavailable_offer_primary();
    if (SELECTED("expired-backup"))
        expired_offer_backup();
    if (SELECTED("lowest-source"))
        lowest_source_wins();
    if (SELECTED("exhausted"))
        view_domain_exhausted();
    if (SELECTED("suffix")) {
        uncommitted_suffix_replaced(LOAD_COMPLETES);
        uncommitted_suffix_replaced(LOAD_RETRIES);
        uncommitted_suffix_replaced(LOAD_ABSENT);
    }
    if (SELECTED("local-log")) {
        selected_local_log(LOAD_COMPLETES);
        selected_local_log(LOAD_RETRIES);
        selected_local_log(LOAD_ABSENT);
    }
    if (SELECTED("ignored"))
        ignored_messages();
    if (SELECTED("duplicate-start-view"))
        duplicate_start_view();
    if (SELECTED("restart-recovering"))
        restart_while_recovering();
    if (SELECTED("learner"))
        learner_rotates_donor();
#undef SELECTED
    return 0;
}
