#define _POSIX_C_SOURCE 200809L
#include "config.h"

#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* GNU/LLVM linker wrapping measures the public step calls made by the host.
 * Host graph copies, snapshot copies, history checks, and queue manipulation
 * occur outside this interval. The library itself needs no instrumentation. */
int __real_vsr_step(struct vsr *v, const struct vsr_event *event,
                    struct vsr_update *update);
int __wrap_vsr_step(struct vsr *v, const struct vsr_event *event,
                    struct vsr_update *update);

static uint64_t samples[262144];
static size_t sample_count;
static uint64_t core_ns;
static bool measuring;

static uint64_t now(void)
{
    struct timespec stamp;
    CHECK(clock_gettime(CLOCK_MONOTONIC, &stamp) == 0);
    CHECK(stamp.tv_sec >= 0 &&
          (uint64_t)stamp.tv_sec < UINT64_MAX / 1000000000);
    return (uint64_t)stamp.tv_sec * 1000000000 + (uint64_t)stamp.tv_nsec;
}

int __wrap_vsr_step(struct vsr *v, const struct vsr_event *event,
                    struct vsr_update *update)
{
    if (!measuring)
        return __real_vsr_step(v, event, update);
    uint64_t start = now();
    int result = __real_vsr_step(v, event, update);
    uint64_t elapsed = now() - start;
    core_ns += elapsed;
    CHECK(sample_count < sizeof(samples) / sizeof(samples[0]));
    samples[sample_count++] = elapsed;
    return result;
}

static int compare(const void *a, const void *b)
{
    uint64_t left = *(const uint64_t *)a, right = *(const uint64_t *)b;
    return left < right ? -1 : left > right ? 1 : 0;
}

static void workload(uint32_t replicas, uint32_t durability, uint32_t batch,
                     uint32_t spans, uint32_t bytes)
{
    enum { REQUESTS = 64 };
    struct vsr_member members[3];
    struct mem_node *nodes[3];
    struct vsr_membership group = {0, members, replicas, replicas == 1 ? 0 : 1};
    struct mem_cluster *cluster = mem_cluster_create();
    struct vsr_layout layout = {0};
    unsigned char payload[4096];
    struct vsr_span fragments[8];
    CHECK(bytes <= sizeof(payload) && spans <= 8 && bytes % spans == 0);
    memset(payload, 0x5a, bytes);
    for (uint32_t i = 0; i < spans; ++i)
        fragments[i] = (struct vsr_span){payload + (size_t)i * (bytes / spans),
                                         bytes / spans};
    const struct vsr_blob body = {fragments, bytes, spans, 0};
    for (uint32_t i = 0; i < replicas; ++i)
        members[i] = (struct vsr_member){i + 1, VSR_MEMBER_FULL, 0};
    for (uint32_t i = 0; i < replicas; ++i) {
        struct vsr_options options = mem_options(i + 1, &group);
        options.durability = durability;
        options.limits.members = replicas;
        options.limits.batch_entries = batch;
        options.limits.log_cache_entries = batch;
        options.limits.pending_requests = batch;
        options.limits.command_bytes = bytes;
        options.limits.message_bytes =
            (uint64_t)bytes * batch + options.limits.manifest_bytes;
        CHECK(vsr_layout(&options, &layout) == VSR_OK);
        nodes[i] = mem_cluster_add(cluster, &options);
        CHECK(mem_node_time(nodes[i], 0).consumed == 1);
    }
    CHECK(mem_cluster_run(cluster, 100000) < 100000);
    sample_count = 0;
    core_ns = 0;
    uint64_t start = now();
    measuring = true;
    for (uint32_t base = 0; base < REQUESTS; base += batch) {
        for (uint32_t i = 0; i < batch && base + i < REQUESTS; ++i) {
            uint64_t id = base + i + 1;
            const struct vsr_request request = {
                {{100, id}, 1}, 0, VSR_REQUEST_COMMAND, 0, &body};
            const struct vsr_event event = {VSR_EVENT_REQUEST, 0, id, &request,
                                            1};
            CHECK(mem_node_event(nodes[0], &event).consumed == 1);
        }
        CHECK(mem_cluster_run(cluster, 100000) < 100000);
    }
    for (uint32_t i = 0; i < replicas; ++i)
        CHECK(mem_node_time(nodes[i], 10).consumed == 1);
    CHECK(mem_cluster_run(cluster, 100000) < 100000);
    measuring = false;
    uint64_t total_ns = now() - start;
    CHECK(mem_node_replies(nodes[0]) == REQUESTS && sample_count != 0);
    qsort(samples, sample_count, sizeof(samples[0]), compare);
    printf("%u,%u,%u,%u,%u,%zu,%zu,%.1f,%" PRIu64 ",%" PRIu64 ",%" PRIu64 "\n",
           replicas, durability, batch, spans, bytes, layout.size, sample_count,
           (double)core_ns / (double)REQUESTS, samples[sample_count / 2],
           samples[(sample_count - 1) * 99 / 100], total_ns);
    mem_cluster_check(cluster);
    mem_cluster_destroy(cluster);
}

int main(void)
{
    printf(
        "replicas,durability,batch,spans,payload_bytes,arena_bytes,step_calls,"
        "core_ns_per_request,step_p50_ns,step_p99_ns,host_total_ns\n");
    for (uint32_t replicas = 1; replicas <= 3; replicas += 2)
        for (uint32_t policy = 0; policy < 2; ++policy) {
            workload(replicas, policy, 1, 1, 32);
            workload(replicas, policy, 8, 8, 32);
            workload(replicas, policy, 8, 8, 4096);
        }
    return 0;
}
