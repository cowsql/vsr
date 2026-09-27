#ifndef VSR_EXAMPLE_COMMON_H
#define VSR_EXAMPLE_COMMON_H

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <inttypes.h>
#include <stdio.h>
#include <string.h>

/* This deterministic host drives the public event/effect interface. Its
 * implementation is shared with tests; production hosts supply their own I/O. */
struct example_cluster {
    struct mem_cluster *host;
    struct mem_node *nodes[6];
    uint32_t count;
};

static inline struct example_cluster example_create(uint32_t count,
                                                    uint32_t faults,
                                                    uint32_t policy,
                                                    const uint32_t *roles)
{
    struct example_cluster cluster = {.host = mem_cluster_create(),
                                      .count = count};
    struct vsr_member members[6];
    CHECK(count <= 6);
    for (uint32_t i = 0; i < count; ++i)
        members[i] = (struct vsr_member){
            i + 1, roles == NULL ? VSR_MEMBER_FULL : roles[i], 0};
    struct vsr_membership group = {0, members, count, faults};
    for (uint32_t i = 0; i < count; ++i) {
        struct vsr_options options = mem_options(i + 1, &group);
        options.durability = policy;
        cluster.nodes[i] = mem_cluster_add(cluster.host, &options);
        CHECK(mem_node_time(cluster.nodes[i], 0).consumed == 1);
    }
    CHECK(mem_cluster_run(cluster.host, 100000) < 100000);
    return cluster;
}

static inline void example_run(struct example_cluster *cluster)
{
    CHECK(mem_cluster_run(cluster->host, 100000) < 100000);
    mem_cluster_check(cluster->host);
}

static inline struct vsr_status example_status(struct mem_node *node)
{
    struct vsr_status status;
    vsr_get_status(mem_node_core(node), &status);
    CHECK(status.failure.code == VSR_FAILURE_NONE);
    return status;
}

static inline void example_request(struct mem_node *node,
                                   const struct vsr_request *request,
                                   uint64_t route)
{
    const struct vsr_event event = {VSR_EVENT_REQUEST, VSR_IO_OK, route,
                                    request, 1};
    CHECK(mem_node_event(node, &event).consumed == 1);
}

static inline void example_command(struct mem_node *node, uint64_t client,
                                   uint64_t number, uint64_t epoch,
                                   uint64_t route, const char *bytes)
{
    const struct vsr_span span = {bytes, strlen(bytes)};
    const struct vsr_blob body = {span.size == 0 ? NULL : &span, span.size,
                                  span.size == 0 ? 0u : 1u, 0};
    const struct vsr_request request = {
        {{100, client}, number}, epoch, VSR_REQUEST_COMMAND, 0, &body};
    example_request(node, &request, route);
}

static inline const struct vsr_reply *example_reply(struct mem_node *node,
                                                    uint64_t route)
{
    for (size_t i = mem_node_replies(node); i > 0; --i) {
        uint64_t found;
        const struct vsr_reply *reply = mem_node_reply(node, i - 1, &found);
        if (found == route)
            return reply;
    }
    CHECK(false);
    return NULL;
}

static inline void example_time(struct example_cluster *cluster, uint64_t now)
{
    for (uint32_t i = 0; i < cluster->count; ++i)
        if (mem_node_alive(cluster->nodes[i]))
            CHECK(mem_node_time(cluster->nodes[i], now).consumed == 1);
    example_run(cluster);
}

#endif
