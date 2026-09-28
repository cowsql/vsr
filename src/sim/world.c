#include "config.h"

#define _GNU_SOURCE 1

#include "sim/sim.h"

#include <errno.h>
#include <limits.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>

/* ------------------------------------------------------------------------
 * Allocation and diagnostics
 * --------------------------------------------------------------------- */

void *vsr_sim_alloc(size_t size)
{
    void *memory = calloc(1, size == 0 ? 1 : size);

    if (memory == NULL) {
        fprintf(stderr, "vsr-sim: out of memory\n");
        abort();
    }
    return memory;
}

void *vsr_sim_grow(void *memory, size_t count, size_t size)
{
    void *grown;

    if (size != 0 && count > SIZE_MAX / size) {
        fprintf(stderr, "vsr-sim: allocation overflow\n");
        abort();
    }
    grown = realloc(memory, count * size == 0 ? 1 : count * size);
    if (grown == NULL) {
        fprintf(stderr, "vsr-sim: out of memory\n");
        abort();
    }
    return grown;
}

void *vsr_sim_grow_table(void *table, size_t count)
{
    return vsr_sim_grow(table, count, sizeof(void *));
}

_Noreturn void vsr_sim_fatal(const char *message, uint32_t node,
                             uint64_t user_data)
{
    fprintf(stderr, "vsr-sim: node %u: %s (user_data 0x%016llx)\n", node,
            message, (unsigned long long)user_data);
    abort();
}

uint64_t vsr_sim_add(uint64_t a, uint64_t b)
{
    return a > UINT64_MAX - b ? UINT64_MAX : a + b;
}

/* ------------------------------------------------------------------------
 * Generator: PCG XSH-RR 64/32 with explicit modular arithmetic, so integer
 * sanitizers stay enabled without exemptions (as tests/lib/random.c).
 * --------------------------------------------------------------------- */

static uint64_t add_mod64(uint64_t a, uint64_t b)
{
    uint64_t low = (uint64_t)(uint32_t)a + (uint32_t)b;
    uint32_t high = (uint32_t)((a >> 32) + (b >> 32) + (low >> 32));

    return ((uint64_t)high << 32) | (uint32_t)low;
}

static uint64_t pcg_advance(uint64_t state, uint64_t increment)
{
    const uint32_t multiplier_low = UINT32_C(0x4c957f2d);
    const uint32_t multiplier_high = UINT32_C(0x5851f42d);
    uint32_t low = (uint32_t)state;
    uint32_t high = (uint32_t)(state >> 32);
    uint64_t product = (uint64_t)low * multiplier_low;
    uint32_t upper = (uint32_t)((product >> 32) +
                                (uint32_t)((uint64_t)low * multiplier_high) +
                                (uint32_t)((uint64_t)high * multiplier_low));

    return add_mod64(((uint64_t)upper << 32) | (uint32_t)product, increment);
}

uint32_t vsr_sim_pcg_next(struct vsr_sim_pcg *pcg)
{
    uint64_t previous = pcg->state;
    uint32_t bits = (uint32_t)(((previous >> 18) ^ previous) >> 27);
    uint32_t rotation = (uint32_t)(previous >> 59);
    uint32_t left = (32u - rotation) & 31u;

    pcg->state = pcg_advance(previous, pcg->increment);
    return (bits >> rotation) | (uint32_t)((uint64_t)bits << left);
}

static void pcg_seed(struct vsr_sim_pcg *pcg, uint64_t seed, uint64_t stream)
{
    pcg->state = 0;
    pcg->increment = (stream & (UINT64_MAX >> 1)) * 2 + 1;
    (void)vsr_sim_pcg_next(pcg);
    pcg->state = add_mod64(pcg->state, seed);
    (void)vsr_sim_pcg_next(pcg);
}

static uint64_t pcg_next64(struct vsr_sim_pcg *pcg)
{
    uint64_t high = vsr_sim_pcg_next(pcg);

    return (high << 32) | vsr_sim_pcg_next(pcg);
}

uint64_t vsr_sim_pcg_below(struct vsr_sim_pcg *pcg, uint64_t bound)
{
    if (bound == 0) {
        return 0;
    }
    if (bound <= UINT32_MAX) {
        uint32_t small = (uint32_t)bound;
        uint32_t threshold = (uint32_t)((UINT64_C(1) << 32) % small);

        for (;;) {
            uint32_t value = vsr_sim_pcg_next(pcg);

            if (value >= threshold) {
                return value % small;
            }
        }
    }
    {
        uint64_t threshold = (UINT64_MAX - bound + 1) % bound;

        for (;;) {
            uint64_t value = pcg_next64(pcg);

            if (value >= threshold) {
                return value % bound;
            }
        }
    }
}

bool vsr_sim_chance(struct vsr_sim *sim, uint32_t ppm)
{
    if (ppm == 0) {
        return false;
    }
    if (ppm >= 1000000u) {
        return true;
    }
    return vsr_sim_pcg_below(&sim->random, 1000000u) < ppm;
}

uint64_t vsr_sim_range(struct vsr_sim *sim, uint64_t min, uint64_t max)
{
    if (min >= max) {
        return min;
    }
    if (max - min == UINT64_MAX) {
        return pcg_next64(&sim->random);
    }
    return min + vsr_sim_pcg_below(&sim->random, max - min + 1);
}

uint64_t vsr_sim_random(struct vsr_sim *sim)
{
    return pcg_next64(&sim->random);
}

uint64_t vsr_sim_random_below(struct vsr_sim *sim, uint64_t bound)
{
    return vsr_sim_pcg_below(&sim->random, bound);
}

/* ------------------------------------------------------------------------
 * Trace
 * --------------------------------------------------------------------- */

void vsr_sim_emit_event(struct vsr_sim *sim,
                        const struct vsr_sim_trace_event *event)
{
    if (sim->trace.event != NULL) {
        sim->trace.event(sim->trace.ctx, event);
    }
}

void vsr_sim_emit(struct vsr_sim *sim, uint32_t kind, uint32_t node,
                  uint32_t peer, uint64_t bytes)
{
    struct vsr_sim_trace_event event;

    if (sim->trace.event == NULL) {
        return;
    }
    memset(&event, 0, sizeof(event));
    event.kind = kind;
    event.node = node;
    event.peer = peer;
    event.bytes = bytes;
    event.now_ns = sim->now_ns;
    sim->trace.event(sim->trace.ctx, &event);
}

void vsr_sim_set_trace(struct vsr_sim *sim, const struct vsr_sim_trace *trace)
{
    if (trace == NULL) {
        memset(&sim->trace, 0, sizeof(sim->trace));
    } else {
        sim->trace = *trace;
    }
}

void vsr_sim_set_faults(struct vsr_sim *sim,
                        const struct vsr_sim_faults *faults)
{
    sim->faults = *faults;
}

/* ------------------------------------------------------------------------
 * Event queue: a binary heap ordered by (due, sequence).
 * --------------------------------------------------------------------- */

static bool event_before(const struct vsr_sim_event *a,
                         const struct vsr_sim_event *b)
{
    if (a->due_ns != b->due_ns) {
        return a->due_ns < b->due_ns;
    }
    return a->sequence < b->sequence;
}

void vsr_sim_schedule(struct vsr_sim *sim, uint64_t due_ns, uint32_t kind,
                      uint32_t node, uint32_t index, uint32_t peer,
                      uint32_t generation)
{
    struct vsr_sim_event event;
    uint32_t at;

    if (due_ns == VSR_SIM_NEVER) {
        return;
    }
    memset(&event, 0, sizeof(event));
    event.due_ns = due_ns < sim->now_ns ? sim->now_ns : due_ns;
    event.sequence = ++sim->sequence;
    event.kind = kind;
    event.node = node;
    event.index = index;
    event.peer = peer;
    event.generation = generation;
    if (kind == VSR_SIM_EVENT_OP) {
        event.incarnation = sim->nodes[node].incarnation;
    }
    if (sim->events_count == sim->events_capacity) {
        sim->events_capacity =
            sim->events_capacity == 0 ? 64 : sim->events_capacity * 2;
        sim->events = vsr_sim_grow(sim->events, sim->events_capacity,
                                   sizeof(*sim->events));
    }
    at = sim->events_count++;
    while (at > 0) {
        uint32_t parent = (at - 1) / 2;

        if (!event_before(&event, &sim->events[parent])) {
            break;
        }
        sim->events[at] = sim->events[parent];
        at = parent;
    }
    sim->events[at] = event;
}

static struct vsr_sim_event pop_event(struct vsr_sim *sim)
{
    struct vsr_sim_event top = sim->events[0];
    struct vsr_sim_event last = sim->events[--sim->events_count];
    uint32_t at = 0;

    for (;;) {
        uint32_t child = at * 2 + 1;

        if (child >= sim->events_count) {
            break;
        }
        if (child + 1 < sim->events_count &&
            event_before(&sim->events[child + 1], &sim->events[child])) {
            ++child;
        }
        if (!event_before(&sim->events[child], &last)) {
            break;
        }
        sim->events[at] = sim->events[child];
        at = child;
    }
    if (sim->events_count > 0) {
        sim->events[at] = last;
    }
    return top;
}

static bool dispatch(struct vsr_sim *sim, const struct vsr_sim_event *event)
{
    switch (event->kind) {
    case VSR_SIM_EVENT_OP: {
        struct vsr_sim_node *node = &sim->nodes[event->node];
        struct vsr_sim_op *op;

        if (!node->alive || node->incarnation != event->incarnation ||
            event->index >= node->ops_count) {
            return false;
        }
        op = node->ops[event->index];
        if (op == NULL || op->generation != event->generation ||
            (op->state != VSR_SIM_OP_WAITING &&
             op->state != VSR_SIM_OP_NOTIF)) {
            return false;
        }
        vsr_sim_exec_event(node, event->index);
        return true;
    }
    case VSR_SIM_EVENT_SEGMENT:
        return vsr_sim_net_segment(sim, event->index, event->peer,
                                   event->generation);
    case VSR_SIM_EVENT_STALL:
        return vsr_sim_net_stall(sim, event->index, event->generation);
    default:
        return false;
    }
}

/* Whether dispatching the event would do anything. A cancelled op, a
 * superseded timer (TIMEOUT_UPDATE bumps the generation), a crashed node,
 * a freed connection, or a segment that a partition stalled leave their
 * events in the heap; they are not pending work, so advance must not move
 * the clock to them (decision 59). */
static bool event_live(const struct vsr_sim *sim,
                       const struct vsr_sim_event *event)
{
    switch (event->kind) {
    case VSR_SIM_EVENT_OP: {
        const struct vsr_sim_node *node = &sim->nodes[event->node];
        const struct vsr_sim_op *op;

        if (!node->alive || node->incarnation != event->incarnation ||
            event->index >= node->ops_count) {
            return false;
        }
        op = node->ops[event->index];
        return op != NULL && op->generation == event->generation &&
               (op->state == VSR_SIM_OP_WAITING ||
                op->state == VSR_SIM_OP_NOTIF);
    }
    case VSR_SIM_EVENT_SEGMENT:
        return vsr_sim_net_segment_live(sim, event->index, event->peer,
                                        event->generation, event->due_ns);
    case VSR_SIM_EVENT_STALL:
        return vsr_sim_net_stall_live(sim, event->index, event->generation);
    default:
        return false;
    }
}

/* ------------------------------------------------------------------------
 * Clock and scheduling
 * --------------------------------------------------------------------- */

uint64_t vsr_sim_node_now(const struct vsr_sim_node *node)
{
    return vsr_sim_add(node->world->now_ns, node->clock_offset_ns);
}

uint64_t vsr_sim_now(const struct vsr_sim *sim)
{
    return sim->now_ns;
}

int vsr_sim_ready(const struct vsr_sim *sim, uint32_t node)
{
    if (node >= sim->nodes_count || !sim->nodes[node].alive) {
        return 0;
    }
    return vsr_sim_exec_ready(&sim->nodes[node]) ? 1 : 0;
}

uint32_t vsr_sim_inflight(const struct vsr_sim *sim, uint32_t node)
{
    if (node >= sim->nodes_count) {
        return 0;
    }
    return sim->nodes[node].inflight;
}

int vsr_sim_advance(struct vsr_sim *sim)
{
    uint64_t count = 0;
    uint64_t next = VSR_SIM_NEVER;

    while (sim->events_count > 0 && sim->events[0].due_ns <= sim->now_ns) {
        struct vsr_sim_event event = pop_event(sim);

        if (dispatch(sim, &event)) {
            ++count;
        }
    }
    if (count > 0) {
        return count > INT_MAX ? INT_MAX : (int)count;
    }
    /* Prune stale events off the top so that the earliest real one, if
     * any, decides where the clock goes. */
    while (sim->events_count > 0 && !event_live(sim, &sim->events[0])) {
        (void)pop_event(sim);
    }
    if (sim->events_count > 0) {
        next = sim->events[0].due_ns;
    }
    for (uint32_t i = 0; i < sim->nodes_count; ++i) {
        const struct vsr_sim_node *node = &sim->nodes[i];
        uint64_t at;

        if (!node->alive || vsr_sim_exec_ready(node)) {
            continue;
        }
        at = vsr_sim_exec_wake_at(node);
        if (at < next) {
            next = at;
        }
    }
    if (next == VSR_SIM_NEVER) {
        return -1;
    }
    if (next > sim->now_ns) {
        sim->now_ns = next;
        vsr_sim_emit(sim, VSR_SIM_TRACE_CLOCK, VSR_SIM_NO_NODE, VSR_SIM_NO_NODE,
                     next);
    }
    return 0;
}

/* ------------------------------------------------------------------------
 * Partitions
 * --------------------------------------------------------------------- */

bool vsr_sim_partitioned(const struct vsr_sim *sim, uint32_t a, uint32_t b)
{
    if (a == b || a >= sim->nodes_count || b >= sim->nodes_count) {
        return false;
    }
    return sim->partitions[(size_t)a * sim->nodes_count + b] != 0;
}

void vsr_sim_partition(struct vsr_sim *sim, uint32_t a, uint32_t b, int cut)
{
    bool was;

    if (a == b || a >= sim->nodes_count || b >= sim->nodes_count) {
        return;
    }
    was = vsr_sim_partitioned(sim, a, b);
    sim->partitions[(size_t)a * sim->nodes_count + b] = cut != 0;
    sim->partitions[(size_t)b * sim->nodes_count + a] = cut != 0;
    if (cut != 0 && !was) {
        vsr_sim_net_partition(sim, a, b);
    }
}

void vsr_sim_isolate(struct vsr_sim *sim, uint32_t node, int cut)
{
    for (uint32_t i = 0; i < sim->nodes_count; ++i) {
        if (i != node) {
            vsr_sim_partition(sim, node, i, cut);
        }
    }
}

void vsr_sim_reset(struct vsr_sim *sim, uint32_t a, uint32_t b)
{
    if (a >= sim->nodes_count || b >= sim->nodes_count) {
        return;
    }
    vsr_sim_net_reset_between(sim, a, b);
}

/* ------------------------------------------------------------------------
 * Addresses
 * --------------------------------------------------------------------- */

static struct vsr_io_address node_address(uint32_t node, uint16_t port)
{
    struct vsr_io_address address;
    struct sockaddr_in in;

    memset(&address, 0, sizeof(address));
    memset(&in, 0, sizeof(in));
    in.sin_family = AF_INET;
    in.sin_port = htons(port);
    /* 10.0.0.1 + node: one canonical address per node. */
    in.sin_addr.s_addr = htonl((uint32_t)(UINT32_C(0x0a000001) + node));
    memcpy(&address.sockaddr, &in, sizeof(in));
    address.length = (uint32_t)sizeof(in);
    return address;
}

struct vsr_io_address vsr_sim_address(const struct vsr_sim *sim, uint32_t node,
                                      uint16_t port)
{
    if (node >= sim->nodes_count) {
        struct vsr_io_address none;

        memset(&none, 0, sizeof(none));
        return none;
    }
    return node_address(node, port);
}

/* ------------------------------------------------------------------------
 * Nodes, crash and restart
 * --------------------------------------------------------------------- */

static struct vsr_sim_handle *new_handle(struct vsr_sim_node *node)
{
    struct vsr_sim_handle *handle = vsr_sim_alloc(sizeof(*handle));

    handle->node = node;
    handle->incarnation = node->incarnation;
    handle->next = node->handles;
    node->handles = handle;
    node->handle = handle;
    return handle;
}

static void boot_node(struct vsr_sim *sim, struct vsr_sim_node *node)
{
    node->alive = 1;
    node->clock_offset_ns =
        vsr_sim_range(sim, 0, sim->faults.clock.offset_max_ns);
    node->waited = 0;
    node->want = 0;
    node->wake_pending = 0;
    node->wait_started_ns = 0;
    node->min_wait_ns = 0;
    node->deadline_ns = VSR_NO_DEADLINE;
    node->next_port = VSR_SIM_EPHEMERAL_PORT;
    (void)new_handle(node);
}

static bool valid_options(const struct vsr_sim_options *options)
{
    const struct vsr_sim_faults *faults = &options->faults;
    uint32_t block = options->block_bytes;

    if (options->nodes == 0 || options->nodes > 65536u) {
        return false;
    }
    if (block != 0 && (block < 512u || (block & (block - 1)) != 0)) {
        return false;
    }
    if (options->file_slots > VSR_SIM_MAX_DIRECT ||
        options->buffer_regions > VSR_SIM_MAX_DIRECT) {
        return false;
    }
    if (faults->network.drop_ppm > 1000000u ||
        faults->network.corrupt_ppm > 1000000u ||
        faults->network.reset_ppm > 1000000u ||
        faults->network.split_ppm > 1000000u ||
        faults->disk.bitrot_ppm > 1000000u ||
        faults->disk.error_ppm > 1000000u ||
        faults->disk.enospc_ppm > 1000000u ||
        faults->disk.unsynced_keep_ppm > 1000000u) {
        return false;
    }
    if (faults->network.delay_min_ns > faults->network.delay_max_ns ||
        faults->disk.latency_min_ns > faults->disk.latency_max_ns ||
        faults->disk.fsync_min_ns > faults->disk.fsync_max_ns) {
        return false;
    }
    return true;
}

/* The world, its node table, the partition matrix and the disk roots are
 * the allocations create reports as -ENOMEM; everything the world allocates
 * later aborts with a diagnostic instead, as test infrastructure may. */
int vsr_sim_create(const struct vsr_sim_options *options, struct vsr_sim **out)
{
    struct vsr_sim *sim;

    if (out == NULL) {
        return -EINVAL;
    }
    *out = NULL;
    if (options == NULL || !valid_options(options)) {
        return -EINVAL;
    }
    sim = calloc(1, sizeof(*sim));
    if (sim == NULL) {
        return -ENOMEM;
    }
    sim->options = *options;
    if (sim->options.block_bytes == 0) {
        sim->options.block_bytes = 4096;
    }
    sim->faults = options->faults;
    pcg_seed(&sim->random, options->seed, UINT64_C(0x5653522d53494d));
    sim->nodes = calloc(options->nodes, sizeof(*sim->nodes));
    sim->partitions = calloc((size_t)options->nodes, (size_t)options->nodes);
    if (sim->nodes == NULL || sim->partitions == NULL) {
        vsr_sim_destroy(sim);
        return -ENOMEM;
    }
    sim->nodes_count = options->nodes;
    for (uint32_t i = 0; i < options->nodes; ++i) {
        struct vsr_sim_node *node = &sim->nodes[i];

        node->world = sim;
        node->index = i;
        node->address = node_address(i, 0);
        if (vsr_sim_disk_init(&node->disk, sim->options.block_bytes) != 0) {
            vsr_sim_destroy(sim);
            return -ENOMEM;
        }
    }
    for (uint32_t i = 0; i < options->nodes; ++i) {
        boot_node(sim, &sim->nodes[i]);
    }
    *out = sim;
    return 0;
}

void vsr_sim_destroy(struct vsr_sim *sim)
{
    if (sim == NULL) {
        return;
    }
    /* A create that failed early has no node table (and nodes_count 0). */
    for (uint32_t i = 0; sim->nodes != NULL && i < sim->nodes_count; ++i) {
        struct vsr_sim_node *node = &sim->nodes[i];
        struct vsr_sim_handle *handle = node->handles;

        vsr_sim_exec_free(node);
        vsr_sim_disk_free(&node->disk);
        while (handle != NULL) {
            struct vsr_sim_handle *next = handle->next;

            free(handle);
            handle = next;
        }
    }
    vsr_sim_net_free(sim);
    free(sim->events);
    free(sim->partitions);
    free(sim->nodes);
    free(sim);
}

struct vsr_io_executor vsr_sim_executor(struct vsr_sim *sim, uint32_t node)
{
    struct vsr_io_executor executor;

    executor.ops = NULL;
    executor.ctx = NULL;
    if (node < sim->nodes_count) {
        executor.ops = &vsr_sim_executor_ops;
        executor.ctx = sim->nodes[node].handle;
    }
    return executor;
}

void vsr_sim_crash(struct vsr_sim *sim, uint32_t index)
{
    struct vsr_sim_node *node;

    if (index >= sim->nodes_count || !sim->nodes[index].alive) {
        return;
    }
    node = &sim->nodes[index];
    vsr_sim_emit(sim, VSR_SIM_TRACE_CRASH, index, VSR_SIM_NO_NODE, 0);
    node->alive = 0;
    ++node->incarnation;
    vsr_sim_net_crash(node);
    vsr_sim_exec_crash(node);
    vsr_sim_disk_crash(sim, node);
}

struct vsr_io_executor vsr_sim_restart(struct vsr_sim *sim, uint32_t index)
{
    struct vsr_io_executor executor;

    executor.ops = NULL;
    executor.ctx = NULL;
    if (index >= sim->nodes_count || sim->nodes[index].alive) {
        return executor;
    }
    boot_node(sim, &sim->nodes[index]);
    vsr_sim_emit(sim, VSR_SIM_TRACE_RESTART, index, VSR_SIM_NO_NODE, 0);
    return vsr_sim_executor(sim, index);
}

int vsr_sim_alive(const struct vsr_sim *sim, uint32_t node)
{
    return node < sim->nodes_count && sim->nodes[node].alive ? 1 : 0;
}
