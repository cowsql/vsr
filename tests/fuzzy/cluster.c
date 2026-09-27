#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"
#include "lib/random.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { MAX_NODES = 5, CLIENTS = 8 };

struct simulation {
    struct mem_cluster *cluster;
    struct mem_node *nodes[MAX_NODES];
    struct test_random random;
    uint64_t seed;
    uint64_t now;
    uint64_t incarnation[MAX_NODES];
    uint64_t requests[CLIENTS];
    size_t replies[MAX_NODES];
    uint64_t route;
    uint32_t count;
    uint32_t step;
    uint32_t profile;
    uint32_t partition;
    int crashed;
    bool trace;
};

static uint32_t choose(struct simulation *s, uint32_t limit)
{
    return test_random_bounded(&s->random, limit);
}

static void record(const struct simulation *s, const char *kind, uint64_t a,
                   uint64_t b)
{
    if (s->trace)
        fprintf(stderr, "%" PRIu64 " %u %s %" PRIu64 " %" PRIu64 "\n", s->seed,
                s->step, kind, a, b);
}

static struct vsr_status state(struct mem_node *node)
{
    struct vsr_status status;
    vsr_get_status(mem_node_core(node), &status);
    return status;
}

static void time_event(struct mem_node *node, uint64_t now)
{
    /* Pending output may consume an entire work quantum before TIME admission. */
    for (unsigned attempt = 0; attempt < 10000; ++attempt) {
        struct mem_step result = mem_node_time(node, now);
        CHECK(result.result == VSR_OK || result.result == VSR_AGAIN);
        if (result.consumed == 1)
            return;
    }
    CHECK(false);
}

static void check(struct simulation *s)
{
    mem_cluster_check(s->cluster);
    for (uint32_t i = 0; i < s->count; ++i) {
        if (mem_node_alive(s->nodes[i])) {
            const struct vsr_status current = state(s->nodes[i]);
            if (current.failure.code != VSR_FAILURE_NONE)
                fprintf(stderr,
                        "seed=%" PRIu64
                        " step=%u node=%u failure=%u effect=%u id=%" PRIu64
                        " status=%d view=%" PRIu64 " commit=%" PRIu64
                        " apply=%" PRIu64 "\n",
                        s->seed, s->step, i + 1, current.failure.code,
                        current.failure.operation_type,
                        current.failure.operation, current.failure.status,
                        current.view, current.committed, current.applied);
            CHECK(current.failure.code == VSR_FAILURE_NONE);
        }
        for (; s->replies[i] < mem_node_replies(s->nodes[i]); ++s->replies[i]) {
            const struct vsr_reply *reply =
                mem_node_reply(s->nodes[i], s->replies[i], NULL);
            if (reply->status != VSR_REPLY_OK ||
                reply->flags != VSR_REPLY_EXECUTED ||
                reply->request.client.hi != 100 ||
                reply->request.client.lo >= CLIENTS)
                continue;
            uint32_t client = (uint32_t)reply->request.client.lo;
            CHECK(reply->request.number <= s->requests[client]);
            if (reply->request.number == s->requests[client])
                ++s->requests[client];
        }
    }
}

static void submit(struct simulation *s, uint32_t index, uint32_t client)
{
    unsigned char bytes[32];
    for (unsigned i = 0; i < sizeof(bytes); ++i)
        bytes[i] = (unsigned char)((s->requests[client] + client + i) & 255u);
    uint32_t split = 1 + choose(s, 31);
    struct vsr_span spans[2] = {{bytes, split},
                                {bytes + split, sizeof(bytes) - split}};
    struct vsr_blob body = {spans, sizeof(bytes), 2, 0};
    struct vsr_request request = {
        {{100, client}, s->requests[client]}, 0, VSR_REQUEST_COMMAND, 0, &body};
    struct vsr_event event = {VSR_EVENT_REQUEST, 0, ++s->route, &request, 1};
    record(s, "request", index, client);
    struct mem_step result = mem_node_event(s->nodes[index], &event);
    CHECK(result.result == VSR_OK || result.result == VSR_AGAIN);
}

static void restart(struct simulation *s, uint32_t index)
{
    ++s->incarnation[index];
    CHECK(mem_node_restart(s->nodes[index],
                           (struct vsr_id){index + 1, s->incarnation[index]}) ==
          VSR_OK);
    time_event(s->nodes[index], s->now);
    s->crashed = -1;
    record(s, "restart", index, s->incarnation[index]);
}

static void event(struct simulation *s)
{
    uint32_t action = choose(s, 24);
    uint32_t index = choose(s, s->count);
    struct mem_node *node = s->nodes[index];
    if (action == 23) {
        if (s->crashed >= 0)
            restart(s, (uint32_t)s->crashed);
        else {
            /* In replicated mode an un-recovered restart still counts as a
             * failed member. Sequential crashes must not remove the recovery
             * quorum merely because all processes have been restarted. */
            for (uint32_t i = 0; i < s->count; ++i)
                if (state(s->nodes[i]).state != VSR_STATE_NORMAL)
                    return;
            mem_node_crash(node);
            s->crashed = (int)index;
            record(s, "crash", index, 0);
        }
        return;
    }
    if (action >= 12 && action < 17) {
        size_t count = mem_cluster_messages(s->cluster);
        if (count == 0)
            return;
        CHECK(count <= UINT32_MAX);
        size_t message = choose(s, (uint32_t)count);
        if (action == 15) {
            record(s, "drop", message, 0);
            mem_cluster_drop(s->cluster, message);
        } else if (action == 16 && count < 256) {
            record(s, "duplicate", message, 0);
            mem_cluster_duplicate(s->cluster, message);
        } else {
            record(s, "deliver", message, 0);
            uint64_t to;
            const struct vsr_message *wire =
                mem_cluster_message(s->cluster, message, &to);
            CHECK(wire->from >= 1 && wire->from <= s->count && to >= 1 &&
                  to <= s->count);
            bool source_side =
                (s->partition & (UINT32_C(1) << (wire->from - 1))) != 0;
            bool target_side = (s->partition & (UINT32_C(1) << (to - 1))) != 0;
            if (source_side != target_side)
                mem_cluster_drop(s->cluster, message);
            else
                (void)mem_cluster_deliver(s->cluster, message);
        }
        return;
    }
    if (!mem_node_alive(node))
        return;
    if (action < 12) {
        size_t count = mem_node_effects(node);
        if (count != 0) {
            CHECK(count <= UINT32_MAX);
            size_t effect = choose(s, (uint32_t)count);
            const struct vsr_op *op = mem_node_effect(node, effect);
            int result = op->type == VSR_OP_SEND && choose(s, 32) == 0
                             ? VSR_IO_RETRY
                             : VSR_IO_OK;
            record(s, action < 3 ? "execute" : "complete", index, op->id);
            if (action < 3)
                (void)mem_node_execute(node, effect, result);
            else
                (void)mem_node_complete(node, effect, result);
        } else {
            record(s, "drain", index, 0);
            (void)mem_node_event(node, NULL);
        }
    } else if (action == 17 || action == 18) {
        submit(s, index, choose(s, CLIENTS));
    } else if (action == 19) {
        s->now += choose(s, 8);
        record(s, "time", index, s->now);
        time_event(node, s->now);
    } else if (action == 20) {
        const struct vsr_event hint = {VSR_EVENT_CHECKPOINT, 0, 0, NULL, 0};
        record(s, "checkpoint", index, 0);
        (void)mem_node_event(node, &hint);
    } else if (action == 21) {
        struct vsr_status current = state(node);
        const struct vsr_read_barrier read = {current.committed, s->now + 30,
                                              choose(s, 2), 0};
        const struct vsr_event input = {VSR_EVENT_READ, 0, ++s->route, &read,
                                        1};
        record(s, "read", index, s->route);
        struct mem_step result = mem_node_event(node, &input);
        CHECK(result.result == VSR_OK || result.result == VSR_AGAIN);
    } else {
        if ((s->profile & 4u) != 0) {
            s->partition = s->partition == 0
                               ? 1 + choose(s, (UINT32_C(1) << s->count) - 2)
                               : 0;
            record(s, "partition", s->partition, 0);
        }
        record(s, "drain", index, 0);
        (void)mem_node_event(node, NULL);
    }
}

static void heal(struct simulation *s)
{
    s->partition = 0;
    if (s->crashed >= 0)
        restart(s, (uint32_t)s->crashed);
    uint64_t committed = 0;
    /* A stable network, available members, and fair effect completions must
     * eventually admit and execute a new request on every full replica. */
    for (unsigned round = 0; round < 1000; ++round) {
        s->now += 5;
        record(s, "heal", round, s->now);
        for (uint32_t i = 0; i < s->count; ++i)
            time_event(s->nodes[i], s->now);
        CHECK(mem_cluster_run(s->cluster, 100000) < 100000);
        for (uint32_t i = 0; i < s->count; ++i) {
            struct vsr_status current = state(s->nodes[i]);
            if (committed == 0 && current.state == VSR_STATE_NORMAL &&
                current.primary == i + 1) {
                const struct vsr_blob body = {0};
                const struct vsr_request request = {
                    {{999, 1}, 1}, 0, VSR_REQUEST_COMMAND, 0, &body};
                const struct vsr_event input = {VSR_EVENT_REQUEST, 0,
                                                ++s->route, &request, 1};
                struct mem_step result = mem_node_event(s->nodes[i], &input);
                CHECK(result.result == VSR_OK || result.result == VSR_AGAIN);
            }
            for (size_t j = 0; j < mem_node_replies(s->nodes[i]); ++j) {
                const struct vsr_reply *reply =
                    mem_node_reply(s->nodes[i], j, NULL);
                if (reply->request.client.hi == 999 &&
                    reply->status == VSR_REPLY_OK)
                    committed = reply->op;
            }
        }
        check(s);
        if (committed != 0) {
            bool converged = true;
            for (uint32_t i = 0; i < s->count; ++i) {
                struct vsr_status current = state(s->nodes[i]);
                converged &= current.state == VSR_STATE_NORMAL &&
                             current.committed >= committed;
                if (current.role == VSR_MEMBER_FULL)
                    converged &= current.applied >= committed;
            }
            if (converged)
                return;
        }
    }
    for (uint32_t i = 0; i < s->count; ++i) {
        struct vsr_status current = state(s->nodes[i]);
        fprintf(stderr,
                "node %u state=%u view=%" PRIu64 " commit=%" PRIu64
                " apply=%" PRIu64 "\n",
                i + 1, current.state, current.view, current.committed,
                current.applied);
    }
    CHECK(false);
}

static void warm(struct simulation *s)
{
    for (uint32_t i = 0; i < 16; ++i) {
        uint32_t client = i % CLIENTS;
        uint64_t number_before = s->requests[client];
        for (unsigned attempt = 0;
             attempt < 1000 && s->requests[client] == number_before;
             ++attempt) {
            submit(s, 0, client);
            CHECK(mem_cluster_run(s->cluster, 100000) < 100000);
            check(s);
        }
        CHECK(s->requests[client] > number_before);
    }
    s->now = 10;
    for (uint32_t i = 0; i < s->count; ++i)
        time_event(s->nodes[i], s->now);
    CHECK(mem_cluster_run(s->cluster, 100000) < 100000);
    if ((s->seed & 1u) != 0) {
        for (uint32_t i = 0; i < s->count; ++i) {
            const struct vsr_event hint = {VSR_EVENT_CHECKPOINT, 0, 0, NULL, 0};
            (void)mem_node_event(s->nodes[i], &hint);
        }
        CHECK(mem_cluster_run(s->cluster, 100000) < 100000);
    }
    check(s);
}

static void run(uint64_t seed, uint32_t steps, bool trace, uint32_t profile)
{
    struct simulation s = {.cluster = mem_cluster_create(),
                           .seed = seed,
                           .crashed = -1,
                           .profile = profile,
                           .trace = trace};
    test_random_seed(&s.random, seed, 54);
    s.count = 3 + choose(&s, 3);
    uint32_t faults = s.count == 5 ? 2 : 1;
    bool witness = choose(&s, 2) != 0;
    uint32_t durability = choose(&s, 2);
    uint32_t operations = UINT32_C(1) << choose(&s, 5);
    uint32_t cache = UINT32_C(1) << choose(&s, 4);
    struct vsr_member members[MAX_NODES];
    const struct vsr_membership membership = {0, members, s.count, faults};
    for (uint32_t i = 0; i < s.count; ++i)
        members[i] = (struct vsr_member){
            i + 1,
            witness && i + 1 == s.count ? VSR_MEMBER_WITNESS : VSR_MEMBER_FULL,
            0};
    fprintf(stderr,
            "seed=%" PRIu64 " steps=%u nodes=%u f=%u witness=%u durability=%u "
            "operations=%u cache=%u profile=%u\n",
            seed, steps, s.count, faults, (unsigned)witness, durability,
            operations, cache, profile);
    for (uint32_t i = 0; i < s.count; ++i) {
        struct vsr_options options = mem_options(i + 1, &membership);
        options.durability = durability;
        options.limits.operations = operations;
        options.limits.log_cache_entries = cache;
        options.limits.batch_entries = cache;
        options.limits.client_cache_entries = 1 + choose(&s, 4);
        options.limits.work_per_step = 1 + choose(&s, 16);
        if ((profile & 2u) != 0) {
            options.limits.transfers = 1;
            options.limits.input_leases = 9;
            options.limits.command_bytes = 32;
            options.limits.result_bytes = 18;
            options.limits.manifest_bytes = 8;
            options.limits.message_bytes = 40;
            options.limits.pinned_payload_bytes = 154;
        }
        s.nodes[i] = mem_cluster_add(s.cluster, &options);
        s.incarnation[i] = 1;
        mem_node_output_capacity(s.nodes[i], 1 + choose(&s, 4));
        time_event(s.nodes[i], 0);
    }
    for (uint32_t i = 0; i < CLIENTS; ++i)
        s.requests[i] = 1;
    CHECK(mem_cluster_run(s.cluster, 100000) < 100000);
    if ((profile & 1u) != 0)
        warm(&s);
    for (s.step = 0; s.step < steps; ++s.step) {
        event(&s);
        check(&s);
    }
    heal(&s);
    mem_cluster_destroy(s.cluster);
}

static uint64_t number(const char *text)
{
    char *end;
    errno = 0;
    uintmax_t result = strtoumax(text, &end, 10);
    CHECK(errno == 0 && *text != '\0' && *text != '-' && *end == '\0' &&
          result <= UINT64_MAX);
    return (uint64_t)result;
}

int main(int argc, char **argv)
{
    CHECK(argc <= 6);
    uint64_t seed = argc > 1 ? number(argv[1]) : 1;
    uint64_t count = argc > 2 ? number(argv[2]) : 32;
    uint64_t steps = argc > 3 ? number(argv[3]) : 600;
    uint64_t profile = argc > 5 ? number(argv[5]) : 0;
    CHECK(profile <= 7);
    CHECK(steps <= UINT32_MAX && count != 0 && count - 1 <= UINT64_MAX - seed);
    for (uint64_t i = 0; i < count; ++i)
        run(seed + i, (uint32_t)steps,
            argc > 4 && strcmp(argv[4], "trace") == 0, (uint32_t)profile);
    return 0;
}
