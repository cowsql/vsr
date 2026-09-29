#include "config.h"

#include "lib/memory_cluster.h"

#include "lib/check.h"

#include <inttypes.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct mem_lease {
    uint64_t id;
    struct mem_graph *graph;
};

struct mem_effect {
    struct mem_graph *graph;
    struct vsr_op op;
    struct mem_graph *result_graph;
    const void *result;
    int status;
    bool started;
    bool executed;
};

struct mem_delivery {
    struct mem_graph *graph;
    const struct vsr_message *message;
    uint64_t to;
};

struct mem_answer {
    struct mem_graph *graph;
    const struct vsr_reply *reply;
    uint64_t route;
};

struct mem_snapshot {
    struct mem_graph *graph;
    const struct vsr_checkpoint *checkpoint;
    struct mem_store *source;
    const struct vsr_entry **history;
    uint64_t checksum;
    void *image;
    size_t image_size;
};

struct mem_snapshot_hold {
    struct mem_snapshot *snapshot;
    bool durable;
    bool held;
};

/* An admitted READ event, remembered until its fence is issued. */
struct mem_admission {
    uint64_t cookie;
    uint64_t min_op;
    uint64_t committed; /* This node's committed position at admission. */
    uint32_t consistency;
    bool fenced;
};

/* Cluster-wide execution record of one client request number. */
struct mem_execution {
    struct vsr_request_id request;
    uint64_t op;
    struct vsr_value result;
};

struct mem_client_latest {
    struct vsr_id client;
    uint64_t number;
};

struct mem_node {
    struct mem_cluster *cluster;
    struct mem_graph *metadata;
    struct vsr_options options;
    struct vsr *core;
    void *arena;
    struct mem_store *store;
    struct mem_effect **effects;
    size_t effect_count;
    struct mem_lease *leases;
    size_t lease_count;
    uint64_t next_lease;
    uint64_t next_snapshot;
    struct vsr_op *output;
    uint32_t output_capacity;
    bool runnable;
    size_t effect_cursor;
    uint64_t applied;
    uint64_t checksum;
    void *application;
    struct mem_graph *history_graph;
    const struct vsr_entry **history;
    size_t history_count;
    const struct vsr_entry **committed_history;
    size_t committed_count;
    struct mem_answer *answers;
    size_t answer_count;
    struct vsr_read_fence *reads;
    size_t read_count;
    struct mem_snapshot_hold *snapshots;
    size_t snapshot_count;
    struct mem_admission *admissions;
    size_t admission_count;
    uint64_t completing; /* Operation whose completion this step consumed. */
    uint64_t issued;     /* Operations emitted by this incarnation. */
    uint64_t completed;  /* Completions it accepted. */
};

struct mem_cluster {
    struct mem_node **nodes;
    size_t node_count;
    struct mem_graph *message_vectors;
    struct mem_delivery *messages;
    size_t message_count;
    size_t message_capacity;
    struct mem_snapshot **snapshots;
    size_t snapshot_count;
    const struct mem_application *application;
    struct mem_graph *oracle;
    struct mem_execution *executions;
    size_t execution_count;
    struct mem_client_latest *clients;
    size_t client_count;
};

static void action_prerequisites(struct mem_node *node,
                                 const struct vsr_op *op);
static void observe_store(struct mem_node *node, uint64_t first);

/* Contract violations name the node, the operation, and the observed values
 * so a failing seed or test explains itself without a debugger. */
static _Noreturn void oracle_fail(const struct mem_node *node,
                                  const struct vsr_op *op, const char *file,
                                  int line, const char *condition,
                                  const char *format, ...)
    __attribute__((format(printf, 6, 7)));

static _Noreturn void oracle_fail(const struct mem_node *node,
                                  const struct vsr_op *op, const char *file,
                                  int line, const char *condition,
                                  const char *format, ...)
{
    va_list arguments;
    fprintf(stderr, "%s:%d: oracle failed: %s\n  node %" PRIu64, file, line,
            condition, node->options.replica);
    if (op != NULL)
        fprintf(stderr, " op %" PRIu64 " type %" PRIu32, op->id, op->type);
    fputs(": ", stderr);
    va_start(arguments, format);
    /* False positive: clang-tidy 18 reports it only for a file that the
     * compilation database lists twice (the extended fuzz cluster).
     * NOLINTNEXTLINE(clang-analyzer-valist.Uninitialized) */
    vfprintf(stderr, format, arguments);
    va_end(arguments);
    fputc('\n', stderr);
    abort();
}

#define ORACLE(node, op, condition, ...)                                       \
    do {                                                                       \
        if (!(condition))                                                      \
            oracle_fail(node, op, __FILE__, __LINE__, #condition,              \
                        __VA_ARGS__);                                          \
    } while (0)

static void check_graphs(const struct mem_node *node, uint64_t completed)
{
    for (size_t i = 0; i < node->effect_count; i++) {
        if (node->effects[i]->op.id != completed)
            CHECK(mem_graph_unchanged(node->effects[i]->graph));
    }
    for (size_t i = 0; i < node->lease_count; i++)
        CHECK(mem_graph_unchanged(node->leases[i].graph));
}

static void *resize(void *array, size_t count, size_t size)
{
    void *result;
    CHECK(size != 0 && count <= SIZE_MAX / size);
    result = realloc(array, count * size);
    CHECK(result != NULL || count == 0);
    return result;
}

static bool same_id(struct vsr_id a, struct vsr_id b)
{
    return a.hi == b.hi && a.lo == b.lo;
}

struct mem_cluster *mem_cluster_create(void)
{
    struct mem_cluster *cluster = calloc(1, sizeof(*cluster));
    CHECK(cluster != NULL);
    cluster->message_vectors = mem_graph_create();
    cluster->oracle = mem_graph_create();
    return cluster;
}

void mem_cluster_set_application(struct mem_cluster *cluster,
                                 const struct mem_application *application)
{
    CHECK(cluster->node_count == 0);
    cluster->application = application;
}

void *mem_node_application(const struct mem_node *node)
{
    return node->application;
}

struct vsr_options mem_options(uint64_t replica,
                               const struct vsr_membership *membership)
{
    struct vsr_options options = {0};
    options.cluster = (struct vsr_id){1, 1};
    options.incarnation = (struct vsr_id){replica, 1};
    options.replica = replica;
    options.seed = membership;
    options.limits = (struct vsr_limits){.members = 9,
                                         .operations = 64,
                                         .input_leases = 128,
                                         .pending_requests = 16,
                                         .pending_reads = 8,
                                         .transfers = 8,
                                         .log_cache_entries = 32,
                                         .client_cache_entries = 16,
                                         .batch_entries = 8,
                                         .spans_per_blob = 8,
                                         .work_per_step = 128,
                                         .command_bytes = 1024,
                                         .result_bytes = 1024,
                                         .manifest_bytes = 64,
                                         .message_bytes = 8192,
                                         .pinned_payload_bytes = 1048576};
    options.heartbeat_ns = 10;
    options.view_timeout_ns = 50;
    options.retry_ns = 5;
    options.transfer_timeout_ns = 100;
    options.start_mode = VSR_START_NEW;
    options.durability = VSR_DURABLE;
    return options;
}

static int node_init(struct mem_node *node)
{
    struct vsr_layout layout;
    int result = vsr_layout(&node->options, &layout);
    if (result != VSR_OK)
        return result;
    CHECK(layout.alignment != 0 && layout.size <= SIZE_MAX - layout.alignment);
    node->arena = aligned_alloc(layout.alignment,
                                (layout.size + layout.alignment - 1) /
                                    layout.alignment * layout.alignment);
    CHECK(node->arena != NULL);
    result = vsr_init(node->arena, layout.size, &node->options, &node->core);
    if (result != VSR_OK) {
        free(node->arena);
        node->arena = NULL;
    }
    node->runnable = result == VSR_OK;
    return result;
}

struct mem_node *mem_cluster_add(struct mem_cluster *cluster,
                                 const struct vsr_options *options)
{
    struct mem_node *node = calloc(1, sizeof(*node));
    CHECK(node != NULL);
    CHECK(mem_cluster_find(cluster, options->replica) == NULL);
    node->cluster = cluster;
    node->metadata = mem_graph_create();
    node->options = *options;
    node->options.seed =
        mem_clone(node->metadata, MEM_MEMBERSHIP, options->seed);
    node->store = mem_store_create();
    node->history_graph = mem_graph_create();
    node->next_lease = 1;
    node->next_snapshot = 1;
    if (cluster->application != NULL)
        node->application = cluster->application->create();
    mem_node_output_capacity(node, 16);
    CHECK(node_init(node) == VSR_OK);
    cluster->nodes = resize(cluster->nodes, cluster->node_count + 1,
                            sizeof(struct mem_node *));
    cluster->nodes[cluster->node_count++] = node;
    return node;
}

struct mem_node *mem_cluster_find(const struct mem_cluster *cluster,
                                  uint64_t replica)
{
    for (size_t i = 0; i < cluster->node_count; i++) {
        if (cluster->nodes[i]->options.replica == replica)
            return cluster->nodes[i];
    }
    return NULL;
}

struct vsr *mem_node_core(struct mem_node *node)
{
    return node->core;
}

struct mem_store *mem_node_store(struct mem_node *node)
{
    return node->store;
}

uint64_t mem_node_id(const struct mem_node *node)
{
    return node->options.replica;
}

bool mem_node_alive(const struct mem_node *node)
{
    return node->core != NULL;
}

void mem_node_output_capacity(struct mem_node *node, uint32_t capacity)
{
    CHECK(capacity != 0);
    node->output = resize(node->output, capacity, sizeof(*node->output));
    node->output_capacity = capacity;
}

static void lease_remove(struct mem_node *node, uint64_t id, bool destroy)
{
    size_t index = 0;
    while (index < node->lease_count && node->leases[index].id != id)
        index++;
    CHECK(index < node->lease_count);
    CHECK(mem_graph_unchanged(node->leases[index].graph));
    if (destroy)
        mem_graph_destroy(node->leases[index].graph);
    if (index + 1 < node->lease_count)
        memmove(&node->leases[index], &node->leases[index + 1],
                (node->lease_count - index - 1) * sizeof(*node->leases));
    node->lease_count--;
}

static void outputs_accept(struct mem_node *node,
                           const struct vsr_update *update)
{
    CHECK(update->count <= update->capacity);
    for (uint32_t i = 0; i < update->count; i++) {
        const struct vsr_op *op = &update->ops[i];
        struct mem_effect *effect;
        CHECK(op->flags == 0);
        if (op->type == VSR_OP_RELEASE) {
            size_t index = 0;
            CHECK(op->id == 0 && op->data == NULL);
            while (index < node->lease_count &&
                   node->leases[index].id != op->arg)
                index++;
            ORACLE(node, op, index < node->lease_count,
                   "RELEASE of lease %" PRIu64
                   " that was never accepted or was already released",
                   op->arg);
            lease_remove(node, op->arg, true);
            continue;
        }
        CHECK(op->id != 0);
        action_prerequisites(node, op);
        for (size_t j = 0; j < node->effect_count; j++)
            CHECK(node->effects[j]->op.id != op->id);
        node->issued++;
        effect = calloc(1, sizeof(*effect));
        CHECK(effect != NULL);
        effect->graph = mem_graph_create();
        mem_graph_watch_sources(effect->graph);
        effect->op = mem_clone_op(effect->graph, op);
        node->effects = resize(node->effects, node->effect_count + 1,
                               sizeof(struct mem_effect *));
        node->effects[node->effect_count++] = effect;
    }
}

/* STOPPED promises that nothing is outstanding: every emitted operation has
 * received exactly one completion and every accepted lease was released. */
static void check_stopped(struct mem_node *node)
{
    struct vsr_status status;
    if (node->core == NULL)
        return;
    vsr_get_status(node->core, &status);
    if (status.state != VSR_STATE_STOPPED)
        return;
    ORACLE(node, NULL, node->effect_count == 0,
           "STOPPED with %zu outstanding operations, first op %" PRIu64,
           node->effect_count,
           node->effect_count == 0 ? 0 : node->effects[0]->op.id);
    ORACLE(node, NULL, node->lease_count == 0,
           "STOPPED with %zu unreleased leases, first lease %" PRIu64,
           node->lease_count, node->lease_count == 0 ? 0 : node->leases[0].id);
    ORACLE(node, NULL, node->issued == node->completed,
           "STOPPED after %" PRIu64 " operations but %" PRIu64 " completions",
           node->issued, node->completed);
}

static struct mem_step submit_graph(struct mem_node *node,
                                    const struct vsr_event *input,
                                    struct mem_graph *graph)
{
    struct vsr_event event = {0};
    struct vsr_update update = {node->output,   node->output_capacity, 0, 0, 0,
                                VSR_NO_DEADLINE};
    struct mem_step result;
    CHECK(node->core != NULL);
    check_graphs(node, 0);
    if (graph != NULL)
        mem_graph_freeze(graph);
    if (input != NULL) {
        event = *input;
        event.lease = 0;
        if (event.data != NULL) {
            CHECK(graph != NULL && node->next_lease != UINT64_MAX);
            event.lease = node->next_lease++;
            node->leases = resize(node->leases, node->lease_count + 1,
                                  sizeof(*node->leases));
            node->leases[node->lease_count++] =
                (struct mem_lease){event.lease, graph};
        }
    }
    result.result =
        vsr_step(node->core, input == NULL ? NULL : &event, &update);
    result.consumed = update.consumed;
    result.emitted = update.count;
    result.flags = update.flags;
    result.deadline = update.deadline_ns;
    CHECK(update.consumed <= (input == NULL ? 0u : 1u));
    if (graph != NULL &&
        (input == NULL || event.data == NULL || update.consumed == 0)) {
        if (event.lease != 0)
            lease_remove(node, event.lease, false);
        mem_graph_destroy(graph);
    }
    /* A completed effect stays listed until this call returns, but it is no
     * longer outstanding for the operations emitted by the same step. */
    node->completing = input != NULL && event.type == VSR_EVENT_COMPLETE &&
                               update.consumed == 1
                           ? event.id
                           : 0;
    outputs_accept(node, &update);
    /* A successfully completed effect may already have had its operation
     * storage recycled during this call. Every other pin must be unchanged. */
    check_graphs(node, node->completing);
    node->completing = 0;
    node->runnable = (update.flags & VSR_UPDATE_MORE) != 0;
    {
        struct vsr_status status;
        vsr_get_status(node->core, &status);
        if (status.state == VSR_STATE_STOPPED) {
            for (size_t i = 0; i < node->snapshot_count; i++)
                node->snapshots[i].held = false;
        }
    }
    return result;
}

static struct mem_client_latest *client_latest(struct mem_cluster *cluster,
                                               struct vsr_id client)
{
    for (size_t i = 0; i < cluster->client_count; i++) {
        if (same_id(cluster->clients[i].client, client))
            return &cluster->clients[i];
    }
    return NULL;
}

/* The host knows every request number a client ever issued, on any node. */
static void client_issued(struct mem_cluster *cluster,
                          const struct vsr_request *request)
{
    struct mem_client_latest *latest =
        client_latest(cluster, request->id.client);
    if (latest == NULL) {
        cluster->clients = resize(cluster->clients, cluster->client_count + 1,
                                  sizeof(*cluster->clients));
        latest = &cluster->clients[cluster->client_count++];
        *latest = (struct mem_client_latest){request->id.client, 0};
    }
    if (request->id.number > latest->number)
        latest->number = request->id.number;
}

static void read_admitted(struct mem_node *node,
                          const struct vsr_read_barrier *barrier,
                          uint64_t cookie, uint64_t committed)
{
    node->admissions = resize(node->admissions, node->admission_count + 1,
                              sizeof(*node->admissions));
    node->admissions[node->admission_count++] = (struct mem_admission){
        cookie, barrier->min_op, committed, barrier->consistency, false};
}

struct mem_step mem_node_event(struct mem_node *node,
                               const struct vsr_event *input)
{
    struct vsr_event event;
    struct mem_graph *graph;
    struct mem_step step;
    if (input == NULL || input->data == NULL) {
        step = submit_graph(node, input, NULL);
        check_stopped(node);
        return step;
    }
    event = *input;
    graph = mem_graph_create();
    switch (event.type) {
    case VSR_EVENT_MESSAGE:
        event.data = mem_clone(graph, MEM_MESSAGE, input->data);
        break;
    case VSR_EVENT_REQUEST:
        event.data = mem_clone(graph, MEM_REQUEST, input->data);
        client_issued(node->cluster, event.data);
        break;
    case VSR_EVENT_CLIENT_QUERY:
        event.data = mem_clone(graph, MEM_ID, input->data);
        break;
    case VSR_EVENT_READ: {
        struct vsr_status status;
        event.data = mem_clone(graph, MEM_READ_BARRIER, input->data);
        /* The admitting step may already emit the fence, so remember the
         * barrier first with a lower bound on the committed position at
         * admission; the step itself can only advance it. */
        CHECK(node->core != NULL);
        vsr_get_status(node->core, &status);
        read_admitted(node, event.data, event.id, status.committed);
        break;
    }
    case VSR_EVENT_TIME:
    case VSR_EVENT_CHECKPOINT:
    case VSR_EVENT_COMPLETE:
    case VSR_EVENT_STOP:
    default:
        CHECK(false);
    }
    step = submit_graph(node, &event, graph);
    if (event.type == VSR_EVENT_READ && step.consumed == 0)
        node->admission_count--;
    check_stopped(node);
    return step;
}

struct mem_step mem_node_time(struct mem_node *node, uint64_t now)
{
    const struct vsr_event event = {VSR_EVENT_TIME, 0, now, NULL, 0};
    return mem_node_event(node, &event);
}

size_t mem_node_leases(const struct mem_node *node)
{
    return node->lease_count;
}

size_t mem_node_effects(const struct mem_node *node)
{
    return node->effect_count;
}

const struct vsr_op *mem_node_effect(const struct mem_node *node, size_t index)
{
    CHECK(index < node->effect_count);
    return &node->effects[index]->op;
}

static struct mem_delivery *message_append(struct mem_cluster *cluster,
                                           const struct vsr_message *message,
                                           uint64_t to)
{
    struct mem_delivery delivery;
    delivery.graph = mem_graph_create();
    delivery.message = mem_clone(delivery.graph, MEM_MESSAGE, message);
    delivery.to = to;
    const size_t count = cluster->message_count;
    struct mem_delivery *messages = cluster->messages;
    if (count == cluster->message_capacity) {
        CHECK(count <= SIZE_MAX / 2);
        const size_t capacity = count == 0 ? 16 : count * 2;
        CHECK(capacity <= SIZE_MAX / sizeof(*messages));
        /* Geometric arena growth keeps all queue vectors under one explicit
         * owner. Superseded vectors occupy less space than the active one;
         * individual message payloads still release immediately on dequeue. */
        messages = mem_graph_alloc(cluster->message_vectors, capacity,
                                   sizeof(*messages));
        if (count != 0)
            memcpy(messages, cluster->messages, count * sizeof(*messages));
        cluster->message_capacity = capacity;
    }
    messages[count] = delivery;
    cluster->message_count = count + 1;
    return messages;
}

static void history_record(struct mem_node *node, const struct vsr_entry *entry)
{
    CHECK(entry->op != 0 && entry->op < SIZE_MAX);
    if (entry->op <= node->history_count) {
        CHECK(mem_entry_equal(node->history[(size_t)entry->op - 1], entry));
        return;
    }
    CHECK(entry->op == node->history_count + 1);
    /* This independent history is never truncated, including on crash/replay. */
    node->history = resize(node->history, node->history_count + 1,
                           sizeof(const struct vsr_entry *));
    node->history[node->history_count++] =
        mem_clone(node->history_graph, MEM_ENTRY, entry);
}

static uint64_t checksum_byte(uint64_t checksum, unsigned char byte)
{
    return (((checksum & (UINT64_MAX >> 7)) << 7) | (checksum >> 57)) ^ byte;
}

static const struct mem_execution *
execution_find(const struct mem_cluster *cluster, struct vsr_request_id request)
{
    for (size_t i = 0; i < cluster->execution_count; i++) {
        const struct mem_execution *execution = &cluster->executions[i];
        if (same_id(execution->request.client, request.client) &&
            execution->request.number == request.number)
            return execution;
    }
    return NULL;
}

/* Every replica executes a request number at one op with one result. The
 * cluster-wide table outlives crashes, so replays are compared as well. */
static void execution_record(struct mem_node *node, const struct vsr_op *op,
                             const struct vsr_entry *entry,
                             const struct vsr_value *value)
{
    struct mem_cluster *cluster = node->cluster;
    const struct mem_execution *known = execution_find(cluster, entry->request);
    struct mem_execution *execution;
    if (known != NULL) {
        ORACLE(node, op, known->op == entry->op,
               "client %" PRIu64 ":%" PRIu64 " number %" PRIu64
               " executes at op %" PRIu64 " after op %" PRIu64,
               entry->request.client.hi, entry->request.client.lo,
               entry->request.number, entry->op, known->op);
        ORACLE(node, op,
               known->result.code == value->code &&
                   mem_blob_equal(&known->result.data, &value->data),
               "client %" PRIu64 ":%" PRIu64 " number %" PRIu64
               " at op %" PRIu64 " produced code %" PRId32 " size %" PRIu64
               " after code %" PRId32 " size %" PRIu64,
               entry->request.client.hi, entry->request.client.lo,
               entry->request.number, entry->op, value->code, value->data.size,
               known->result.code, known->result.data.size);
        return;
    }
    cluster->executions =
        resize(cluster->executions, cluster->execution_count + 1,
               sizeof(*cluster->executions));
    execution = &cluster->executions[cluster->execution_count++];
    execution->request = entry->request;
    execution->op = entry->op;
    execution->result = *value;
    execution->result.data = *(const struct vsr_blob *)mem_clone(
        cluster->oracle, MEM_BLOB, &value->data);
}

static int32_t application_apply(struct mem_node *node, const struct vsr_op *op,
                                 const struct vsr_entry *entry,
                                 struct mem_graph *graph,
                                 struct vsr_value *value)
{
    const struct mem_application *application = node->cluster->application;
    CHECK(entry->op == node->applied + 1);
    history_record(node, entry);
    node->applied = entry->op;
    value->data = (struct vsr_blob){NULL, 0, 0, 0};
    value->code = 0;
    if (application != NULL) {
        const size_t capacity = (size_t)node->options.limits.result_bytes;
        char *bytes = mem_graph_alloc(graph, capacity + 1, 1);
        size_t length = 0;
        value->code = application->apply(node->application, entry, bytes,
                                         capacity, &length);
        CHECK(length <= capacity);
        if (length != 0) {
            struct vsr_span *span = mem_graph_alloc(graph, 1, sizeof(*span));
            *span = (struct vsr_span){bytes, length};
            value->data = (struct vsr_blob){span, length, 1, 0};
        }
    } else if (entry->type == VSR_REQUEST_COMMAND) {
        const struct vsr_blob *command = entry->body;
        node->checksum = checksum_byte(node->checksum, 255);
        for (uint32_t i = 0; i < command->count; i++) {
            const unsigned char *bytes = command->spans[i].data;
            for (size_t j = 0; j < command->spans[i].size; j++)
                node->checksum = checksum_byte(node->checksum, bytes[j]);
        }
        value->code = (int32_t)(node->checksum & INT32_MAX);
    }
    if (entry->type != VSR_REQUEST_NOOP)
        execution_record(node, op, entry, value);
    return value->code;
}

static void application_reset(struct mem_node *node)
{
    node->applied = 0;
    node->checksum = 0;
    if (node->cluster->application != NULL)
        node->cluster->application->reset(node->application);
}

static struct mem_snapshot_hold *snapshot_hold(struct mem_node *node,
                                               struct vsr_id id)
{
    for (size_t i = 0; i < node->snapshot_count; i++) {
        if (same_id(node->snapshots[i].snapshot->checkpoint->id, id))
            return &node->snapshots[i];
    }
    return NULL;
}

static void snapshot_acquire(struct mem_node *node,
                             struct mem_snapshot *snapshot)
{
    struct mem_snapshot_hold *hold =
        snapshot_hold(node, snapshot->checkpoint->id);
    if (hold != NULL) {
        hold->held = true;
        return;
    }
    node->snapshots = resize(node->snapshots, node->snapshot_count + 1,
                             sizeof(*node->snapshots));
    node->snapshots[node->snapshot_count++] =
        (struct mem_snapshot_hold){snapshot, false, true};
}

static bool snapshot_available(struct mem_node *node,
                               struct mem_snapshot_hold *hold)
{
    if (hold == NULL)
        return false;
    if (hold->held)
        return true;
    for (uint64_t sequence = 1; sequence <= mem_store_readable(node->store);
         sequence++) {
        const struct vsr_recovered *recovered =
            mem_store_recovered(node->store, sequence);
        if (recovered != NULL && recovered->checkpoint != NULL &&
            same_id(recovered->checkpoint->id, hold->snapshot->checkpoint->id))
            return true;
    }
    return false;
}

static const struct vsr_entry *revision_entry(struct mem_node *node,
                                              uint64_t sequence, uint64_t op)
{
    const struct vsr_entry *entry = mem_store_entry(node->store, sequence, op);
    const struct vsr_recovered *recovered;
    if (entry != NULL)
        return entry;
    recovered = mem_store_recovered(node->store, sequence);
    if (recovered == NULL || recovered->checkpoint == NULL ||
        op > recovered->checkpoint->op)
        return NULL;
    for (size_t i = 0; i < node->cluster->snapshot_count; i++) {
        const struct mem_snapshot *snapshot = node->cluster->snapshots[i];
        if (same_id(snapshot->checkpoint->id, recovered->checkpoint->id))
            return snapshot->history[(size_t)op - 1];
    }
    return NULL;
}

static void observe_store(struct mem_node *node, uint64_t first)
{
    const uint64_t end = mem_store_readable(node->store);
    for (uint64_t sequence = first; sequence <= end; sequence++) {
        const struct vsr_recovered *recovered =
            mem_store_recovered(node->store, sequence);
        CHECK(recovered != NULL);
        for (uint64_t op = 1; op <= recovered->hard.committed; op++) {
            const struct vsr_entry *entry = revision_entry(node, sequence, op);
            CHECK(entry != NULL);
            if (op <= node->committed_count) {
                CHECK(mem_entry_equal(node->committed_history[(size_t)op - 1],
                                      entry));
            } else {
                CHECK(op == node->committed_count + 1);
                node->committed_history =
                    resize(node->committed_history, node->committed_count + 1,
                           sizeof(const struct vsr_entry *));
                node->committed_history[node->committed_count++] =
                    mem_clone(node->history_graph, MEM_ENTRY, entry);
            }
        }
    }
}

/* APPLY, CAPTURE, and INSTALL mutate or freeze the application, so at most
 * one is outstanding and none overlaps a read fence, whether issued or
 * executed. Read fences may overlap each other: nothing moves the boundary
 * they all describe while any of them is outstanding. */
static bool mutation_type(uint32_t type)
{
    return type == VSR_OP_APPLY || type == VSR_OP_SNAPSHOT_CAPTURE ||
           type == VSR_OP_SNAPSHOT_INSTALL;
}

static bool outstanding(const struct mem_node *node, const struct vsr_op *op,
                        const struct vsr_op *other)
{
    return other->id != op->id && other->id != node->completing;
}

static void check_fence(const struct mem_node *node, const struct vsr_op *op,
                        const char *when)
{
    const struct vsr_op *other = NULL;
    if (!mutation_type(op->type) && op->type != VSR_OP_READ_READY)
        return;
    for (size_t i = 0; i < node->effect_count && other == NULL; i++) {
        const struct vsr_op *candidate = &node->effects[i]->op;
        if (!outstanding(node, op, candidate))
            continue;
        if (mutation_type(candidate->type) ||
            (mutation_type(op->type) && candidate->type == VSR_OP_READ_READY))
            other = candidate;
    }
    ORACLE(node, op, other == NULL,
           "%s while op %" PRIu64 " type %" PRIu32 " is outstanding", when,
           other == NULL ? 0 : other->id, other == NULL ? 0 : other->type);
}

static void check_read_fence(struct mem_node *node, const struct vsr_op *op)
{
    const struct vsr_read_fence *fence = op->data;
    struct mem_admission *admission = NULL;
    for (size_t i = node->admission_count; i > 0 && admission == NULL; i--) {
        if (node->admissions[i - 1].cookie == fence->cookie)
            admission = &node->admissions[i - 1];
    }
    ORACLE(node, op, admission != NULL,
           "READ_READY for cookie %" PRIu64 " that was never admitted",
           fence->cookie);
    ORACLE(node, op, !admission->fenced,
           "second READ_READY for cookie %" PRIu64, fence->cookie);
    admission->fenced = true;
    ORACLE(node, op, fence->applied >= admission->min_op,
           "cookie %" PRIu64 " fence applied %" PRIu64 " below min_op %" PRIu64,
           fence->cookie, fence->applied, admission->min_op);
    if (admission->consistency == VSR_READ_LINEARIZABLE)
        ORACLE(node, op, fence->applied >= admission->committed,
               "cookie %" PRIu64 " linearizable fence applied %" PRIu64
               " below committed %" PRIu64 " at admission",
               fence->cookie, fence->applied, admission->committed);
    ORACLE(node, op, fence->applied == node->applied,
           "cookie %" PRIu64 " fence applied %" PRIu64
           " but the application is at %" PRIu64,
           fence->cookie, fence->applied, node->applied);
}

/* An advertised offer describes exactly one retained revision of the sender's
 * own store, including any attached entry chunk. */
static void check_offer(const struct mem_node *node, const struct vsr_op *op,
                        const struct vsr_message *message,
                        const struct vsr_log_state *state)
{
    const uint64_t sequence = state->revision.sequence;
    const struct vsr_recovered *stored =
        mem_store_recovered(node->store, sequence);
    ORACLE(node, op,
           same_id(state->revision.incarnation, node->options.incarnation),
           "offer names incarnation %" PRIu64 ":%" PRIu64
           " but this incarnation is %" PRIu64 ":%" PRIu64,
           state->revision.incarnation.hi, state->revision.incarnation.lo,
           node->options.incarnation.hi, node->options.incarnation.lo);
    ORACLE(node, op,
           sequence >= 1 && sequence <= mem_store_readable(node->store),
           "offer names revision %" PRIu64 " beyond readable frontier %" PRIu64,
           sequence, mem_store_readable(node->store));
    ORACLE(node, op, stored != NULL, "offer names reclaimed revision %" PRIu64,
           sequence);
    ORACLE(node, op,
           state->log_begin == stored->log_begin &&
               state->log_end == stored->log_end,
           "offer range [%" PRIu64 ",%" PRIu64 ") but revision %" PRIu64
           " retains [%" PRIu64 ",%" PRIu64 ")",
           state->log_begin, state->log_end, sequence, stored->log_begin,
           stored->log_end);
    ORACLE(node, op, state->committed == stored->hard.committed,
           "offer committed %" PRIu64 " but revision %" PRIu64
           " committed %" PRIu64,
           state->committed, sequence, stored->hard.committed);
    ORACLE(node, op,
           state->view == stored->hard.view &&
               state->last_normal_view == stored->hard.last_normal_view,
           "offer view %" PRIu64 "/%" PRIu64 " but revision %" PRIu64
           " stored %" PRIu64 "/%" PRIu64,
           state->view, state->last_normal_view, sequence, stored->hard.view,
           stored->hard.last_normal_view);
    ORACLE(node, op, message->number == state->log_end - 1,
           "message number %" PRIu64 " but offered log_end %" PRIu64,
           message->number, state->log_end);
    ORACLE(node, op,
           state->epoch != NULL && stored->hard.epoch != NULL &&
               state->epoch->current->epoch ==
                   stored->hard.epoch->current->epoch &&
               state->epoch->boundary == stored->hard.epoch->boundary,
           "offer epoch %" PRIu64 " boundary %" PRIu64 " but revision %" PRIu64
           " stored epoch %" PRIu64 " boundary %" PRIu64,
           state->epoch == NULL ? 0 : state->epoch->current->epoch,
           state->epoch == NULL ? 0 : state->epoch->boundary, sequence,
           stored->hard.epoch == NULL ? 0 : stored->hard.epoch->current->epoch,
           stored->hard.epoch == NULL ? 0 : stored->hard.epoch->boundary);
    ORACLE(node, op,
           (state->checkpoint == NULL) == (stored->checkpoint == NULL),
           "offer %s a checkpoint but revision %" PRIu64 " %s one",
           state->checkpoint == NULL ? "omits" : "names", sequence,
           stored->checkpoint == NULL ? "has none" : "retains");
    if (state->checkpoint != NULL) {
        const struct vsr_checkpoint *offered = state->checkpoint;
        const struct vsr_checkpoint *retained = stored->checkpoint;
        ORACLE(node, op,
               same_id(offered->id, retained->id) &&
                   offered->op == retained->op &&
                   offered->view == retained->view &&
                   mem_blob_equal(&offered->manifest, &retained->manifest),
               "offer checkpoint %" PRIu64 ":%" PRIu64 " op %" PRIu64
               " view %" PRIu64 " but revision %" PRIu64 " retains %" PRIu64
               ":%" PRIu64 " op %" PRIu64 " view %" PRIu64,
               offered->id.hi, offered->id.lo, offered->op, offered->view,
               sequence, retained->id.hi, retained->id.lo, retained->op,
               retained->view);
    }
    for (uint32_t i = 0; i < state->entries.count; i++) {
        const struct vsr_entry *entry = &state->entries.entries[i];
        const struct vsr_entry *retained =
            mem_store_entry(node->store, sequence, entry->op);
        ORACLE(node, op, entry->op == state->entries.entries[0].op + i,
               "chunk entry %" PRIu32 " has op %" PRIu64
               " after first op %" PRIu64,
               i, entry->op, state->entries.entries[0].op);
        ORACLE(node, op,
               entry->op >= state->log_begin && entry->op < state->log_end,
               "chunk entry op %" PRIu64 " outside offered range [%" PRIu64
               ",%" PRIu64 ")",
               entry->op, state->log_begin, state->log_end);
        ORACLE(node, op, retained != NULL && mem_entry_equal(retained, entry),
               "chunk entry op %" PRIu64 " differs from revision %" PRIu64,
               entry->op, sequence);
    }
}

static void check_chunk(const struct mem_node *node, const struct vsr_op *op,
                        const struct vsr_message *message)
{
    const struct vsr_state_chunk *chunk = message->body;
    const uint32_t count = chunk->state.entries.count;
    if (message->type == VSR_MSG_STATE_UNAVAILABLE || chunk->first == 0) {
        ORACLE(node, op, count == 0 && chunk->next == chunk->first,
               "%s carries %" PRIu32 " entries with first %" PRIu64
               " next %" PRIu64,
               message->type == VSR_MSG_STATE_UNAVAILABLE ? "STATE_UNAVAILABLE"
                                                          : "discovery",
               count, chunk->first, chunk->next);
        return;
    }
    ORACLE(
        node, op,
        chunk->next == chunk->first + count &&
            (count == 0 || chunk->state.entries.entries[0].op == chunk->first),
        "chunk first %" PRIu64 " next %" PRIu64 " but %" PRIu32
        " entries starting at op %" PRIu64,
        chunk->first, chunk->next, count,
        count == 0 ? 0 : chunk->state.entries.entries[0].op);
}

/* Audit at issuance, before the adapter can accidentally hide missing core
 * dependencies by completing a STORE/SYNC ahead of a SEND or APPLY. */
static void action_prerequisites(struct mem_node *node, const struct vsr_op *op)
{
    const uint64_t sequence = node->options.durability == VSR_DURABLE
                                  ? mem_store_durable(node->store)
                                  : mem_store_readable(node->store);
    const struct vsr_recovered *ready =
        mem_store_recovered(node->store, sequence);
    check_fence(node, op, "issued");
    if (op->type == VSR_OP_STORE) {
        const struct vsr_store *transaction = op->data;
        const struct vsr_recovered *readable =
            mem_store_recovered(node->store, mem_store_readable(node->store));
        uint32_t role =
            readable == NULL ? VSR_MEMBER_NONE : readable->hard.role;
        const int shape = mem_store_validate(node->store, transaction);
        ORACLE(node, op, shape == VSR_IO_OK,
               "STORE sequence %" PRIu64 " with %" PRIu32
               " changes is malformed against revision %" PRIu64
               " (status %d, readable %" PRIu64 ")",
               transaction->sequence, transaction->count,
               transaction->sequence - 1, shape,
               mem_store_readable(node->store));
        for (uint32_t i = 0; i < transaction->count; i++) {
            if (transaction->changes[i].type == VSR_STORE_HARD_STATE)
                role = ((const struct vsr_hard_state *)transaction->changes[i]
                            .data)
                           ->role;
        }
        for (uint32_t i = 0; i < transaction->count; i++) {
            const struct vsr_change *change = &transaction->changes[i];
            if (role == VSR_MEMBER_FULL &&
                (change->type == VSR_STORE_PUBLISH_CHECKPOINT ||
                 change->type == VSR_STORE_RESTORE_CHECKPOINT)) {
                const struct vsr_checkpoint *checkpoint = change->data;
                struct mem_snapshot_hold *hold =
                    snapshot_hold(node, checkpoint->id);
                CHECK(snapshot_available(node, hold));
                if (node->options.durability == VSR_DURABLE)
                    CHECK(hold->durable);
            }
        }
    } else if (op->type == VSR_OP_APPLY) {
        const struct vsr_apply *apply = op->data;
        CHECK(ready != NULL && ready->hard.committed >= apply->through);
        for (uint32_t i = 0; i < apply->batch.count; i++) {
            const struct vsr_entry *stored =
                revision_entry(node, sequence, apply->batch.entries[i].op);
            CHECK(stored != NULL &&
                  mem_entry_equal(stored, &apply->batch.entries[i]));
        }
    } else if (op->type == VSR_OP_LOAD) {
        const struct vsr_store_read *read = op->data;
        if (read->type != VSR_LOAD_RECOVERY) {
            ORACLE(node, op,
                   read->sequence >= 1 &&
                       read->sequence <= mem_store_readable(node->store),
                   "LOAD type %" PRIu32 " names revision %" PRIu64
                   " beyond readable frontier %" PRIu64,
                   read->type, read->sequence, mem_store_readable(node->store));
            ORACLE(node, op,
                   mem_store_recovered(node->store, read->sequence) != NULL,
                   "LOAD type %" PRIu32 " names reclaimed revision %" PRIu64,
                   read->type, read->sequence);
        }
    } else if (op->type == VSR_OP_RECLAIM) {
        ORACLE(node, op, op->arg <= mem_store_readable(node->store),
               "RECLAIM %" PRIu64 " above readable frontier %" PRIu64, op->arg,
               mem_store_readable(node->store));
        for (size_t i = 0; i < node->effect_count; i++) {
            const struct vsr_op *other = &node->effects[i]->op;
            uint64_t needed = UINT64_MAX;
            if (!outstanding(node, op, other))
                continue;
            if (other->type == VSR_OP_LOAD) {
                const struct vsr_store_read *read = other->data;
                if (read->type != VSR_LOAD_RECOVERY)
                    needed = read->sequence;
            } else if (other->type == VSR_OP_SNAPSHOT_CAPTURE) {
                const struct vsr_snapshot_task *task = other->data;
                if (task->sequence != 0)
                    needed = task->sequence;
            }
            ORACLE(node, op, op->arg <= needed,
                   "RECLAIM %" PRIu64 " above revision %" PRIu64
                   " named by outstanding op %" PRIu64 " type %" PRIu32,
                   op->arg, needed, other->id, other->type);
        }
    } else if (op->type == VSR_OP_READ_READY) {
        check_read_fence(node, op);
    } else if (op->type == VSR_OP_REPLY) {
        const struct vsr_reply *reply = op->data;
        if (reply->status == VSR_REPLY_OK &&
            (reply->flags & VSR_REPLY_EXECUTED) != 0) {
            const struct vsr_client_record *client =
                mem_store_client(node->store, mem_store_readable(node->store),
                                 reply->request.client);
            CHECK(ready != NULL && ready->hard.committed >= reply->op);
            CHECK(client != NULL &&
                  client->request.number == reply->request.number);
            CHECK(client->op == reply->op &&
                  client->result.code == reply->result.code);
            CHECK(mem_blob_equal(&client->result.data, &reply->result.data));
        }
        if ((reply->flags & VSR_REPLY_EXECUTED) != 0) {
            const struct mem_execution *known =
                execution_find(node->cluster, reply->request);
            ORACLE(node, op, known != NULL,
                   "EXECUTED reply for client %" PRIu64 ":%" PRIu64
                   " number %" PRIu64 " that no replica executed",
                   reply->request.client.hi, reply->request.client.lo,
                   reply->request.number);
            ORACLE(node, op,
                   known->op == reply->op &&
                       known->result.code == reply->result.code &&
                       mem_blob_equal(&known->result.data, &reply->result.data),
                   "EXECUTED reply for client %" PRIu64 ":%" PRIu64
                   " number %" PRIu64 " reports op %" PRIu64 " code %" PRId32
                   " size %" PRIu64 " but it executed at op %" PRIu64
                   " code %" PRId32 " size %" PRIu64,
                   reply->request.client.hi, reply->request.client.lo,
                   reply->request.number, reply->op, reply->result.code,
                   reply->result.data.size, known->op, known->result.code,
                   known->result.data.size);
        }
        if (reply->status == VSR_REPLY_STALE_REQUEST) {
            const struct mem_client_latest *latest =
                client_latest(node->cluster, reply->request.client);
            ORACLE(node, op,
                   latest != NULL && reply->request.number < latest->number,
                   "STALE_REQUEST for client %" PRIu64 ":%" PRIu64
                   " number %" PRIu64 " whose latest issued number is %" PRIu64,
                   reply->request.client.hi, reply->request.client.lo,
                   reply->request.number, latest == NULL ? 0 : latest->number);
        }
    } else if (op->type == VSR_OP_SEND) {
        const struct vsr_message *message = op->data;
        bool fence = false, normal = false, prefix = false;
        switch (message->type) {
        case VSR_MSG_PREPARE:
        case VSR_MSG_PREPARE_OK:
            fence = true;
            prefix = true;
            break;
        case VSR_MSG_START_VIEW_CHANGE:
        case VSR_MSG_DO_VIEW_CHANGE:
            fence = true;
            break;
        case VSR_MSG_START_VIEW:
        case VSR_MSG_RECOVERY_RESPONSE:
        case VSR_MSG_READ_ACK:
            fence = true;
            normal = true;
            prefix = true;
            break;
        case VSR_MSG_EPOCH_STARTED:
            fence = true;
            prefix = true;
            break;
        case VSR_MSG_COMMIT:
            CHECK(ready != NULL && ready->hard.committed >= message->number);
            break;
        case VSR_MSG_CHECKPOINT: {
            const struct vsr_checkpoint *checkpoint = message->body;
            CHECK(ready != NULL && ready->hard.committed >= checkpoint->op);
            if (ready->hard.role == VSR_MEMBER_FULL) {
                struct mem_snapshot_hold *hold =
                    snapshot_hold(node, checkpoint->id);
                CHECK(hold != NULL);
                if (node->options.durability == VSR_DURABLE)
                    CHECK(hold->durable);
            }
            break;
        }
        case VSR_MSG_START_EPOCH:
        case VSR_MSG_NEW_EPOCH:
            CHECK(ready != NULL && ready->hard.committed >= message->number);
            break;
        case VSR_MSG_RECOVERY:
        case VSR_MSG_GET_STATE:
        case VSR_MSG_NEW_STATE:
        case VSR_MSG_GET_LOG:
        case VSR_MSG_LOG:
        case VSR_MSG_STATE_UNAVAILABLE:
        case VSR_MSG_READ_PROBE:
            break;
        default:
            CHECK(false);
        }
        if (fence) {
            CHECK(ready != NULL && ready->hard.epoch != NULL);
            CHECK(ready->hard.epoch->current->epoch >= message->epoch);
            if (ready->hard.epoch->current->epoch == message->epoch)
                CHECK(ready->hard.view >= message->view);
        }
        if (normal)
            CHECK(ready != NULL && ready->hard.state == VSR_HARD_NORMAL);
        if (prefix)
            CHECK(ready != NULL && ready->log_end > message->number);
        if (message->type == VSR_MSG_PREPARE) {
            const struct vsr_prepare *prepare = message->body;
            for (uint32_t i = 0; i < prepare->batch.count; i++) {
                const struct vsr_entry *stored = revision_entry(
                    node, sequence, prepare->batch.entries[i].op);
                CHECK(stored != NULL &&
                      mem_entry_equal(stored, &prepare->batch.entries[i]));
            }
        }
        switch (message->type) {
        case VSR_MSG_DO_VIEW_CHANGE:
        case VSR_MSG_START_VIEW:
            check_offer(node, op, message, message->body);
            break;
        case VSR_MSG_RECOVERY_RESPONSE: {
            const struct vsr_recovery *recovery = message->body;
            if (recovery->state != NULL)
                check_offer(node, op, message, recovery->state);
            break;
        }
        case VSR_MSG_NEW_STATE:
        case VSR_MSG_LOG:
        case VSR_MSG_STATE_UNAVAILABLE:
            check_chunk(node, op, message);
            check_offer(
                node, op, message,
                &((const struct vsr_state_chunk *)message->body)->state);
            break;
        default:
            break;
        }
    }
}

static const struct vsr_checkpoint *
snapshot_capture(struct mem_node *node, const struct vsr_snapshot_task *task)
{
    struct mem_snapshot *snapshot = calloc(1, sizeof(*snapshot));
    struct vsr_checkpoint *checkpoint;
    CHECK(snapshot != NULL && task->checkpoint != NULL);
    CHECK(task->op == node->applied && node->next_snapshot != UINT64_MAX);
    snapshot->graph = mem_graph_create();
    checkpoint = mem_clone(snapshot->graph, MEM_CHECKPOINT, task->checkpoint);
    checkpoint->id =
        (struct vsr_id){node->options.replica, node->next_snapshot++};
    snapshot->checkpoint = checkpoint;
    snapshot->source = node->store;
    snapshot->checksum = node->checksum;
    if (node->cluster->application != NULL)
        snapshot->image = node->cluster->application->capture(
            node->application, &snapshot->image_size);
    snapshot->history = mem_graph_alloc(snapshot->graph, (size_t)task->op,
                                        sizeof(const struct vsr_entry *));
    for (uint64_t i = 0; i < task->op; i++)
        snapshot->history[(size_t)i] =
            mem_clone(snapshot->graph, MEM_ENTRY, node->history[(size_t)i]);
    mem_store_checkpoint_from_revision(node->store, checkpoint, task->sequence);
    struct mem_cluster *cluster = node->cluster;
    const size_t count = cluster->snapshot_count;
    struct mem_snapshot **snapshots =
        resize(cluster->snapshots, count + 1, sizeof(struct mem_snapshot *));
    snapshots[count] = snapshot;
    cluster->snapshots = snapshots;
    cluster->snapshot_count = count + 1;
    snapshot_acquire(node, snapshot);
    return checkpoint;
}

static int snapshot_execute(struct mem_node *node, struct mem_effect *effect)
{
    const struct vsr_snapshot_task *task = effect->op.data;
    struct mem_snapshot_hold *hold;
    if (effect->op.type == VSR_OP_SNAPSHOT_CAPTURE) {
        if (mem_store_readable(node->store) < task->sequence)
            return VSR_IO_RETRY;
        effect->result = mem_clone(effect->result_graph, MEM_CHECKPOINT,
                                   snapshot_capture(node, task));
        return VSR_IO_OK;
    }
    if (effect->op.type == VSR_OP_SNAPSHOT_INSTALL &&
        task->checkpoint == NULL) {
        CHECK(task->op == 0 && task->sequence == 0);
        application_reset(node);
        return VSR_IO_OK;
    }
    CHECK(task->checkpoint != NULL);
    hold = snapshot_hold(node, task->checkpoint->id);
    if (effect->op.type == VSR_OP_SNAPSHOT_FETCH) {
        struct mem_node *source = mem_cluster_find(node->cluster, task->peer);
        struct mem_snapshot_hold *remote =
            source == NULL ? NULL : snapshot_hold(source, task->checkpoint->id);
        if (!snapshot_available(node, hold)) {
            if (source == NULL || !mem_node_alive(source) ||
                !snapshot_available(source, remote))
                return VSR_IO_NOT_FOUND;
            CHECK(mem_store_checkpoint_copy(node->store, source->store,
                                            task->checkpoint));
            snapshot_acquire(node, remote->snapshot);
            hold = snapshot_hold(node, task->checkpoint->id);
        } else {
            hold->held = true;
        }
        CHECK(hold != NULL);
        effect->result = mem_clone(effect->result_graph, MEM_CHECKPOINT,
                                   hold->snapshot->checkpoint);
        return VSR_IO_OK;
    }
    if (effect->op.type == VSR_OP_SNAPSHOT_DROP) {
        /* Holds are keyed by snapshot ID: repeated FETCH shares one hold, so
         * exactly one DROP ends it and a second DROP names nothing held. */
        ORACLE(node, &effect->op, hold != NULL,
               "DROP of unknown snapshot %" PRIu64 ":%" PRIu64,
               task->checkpoint->id.hi, task->checkpoint->id.lo);
        ORACLE(node, &effect->op, hold->held,
               "DROP of snapshot %" PRIu64 ":%" PRIu64
               " that this incarnation does not hold",
               task->checkpoint->id.hi, task->checkpoint->id.lo);
        hold->held = false;
        return VSR_IO_OK;
    }
    if (!snapshot_available(node, hold))
        return VSR_IO_NOT_FOUND;
    switch (effect->op.type) {
    case VSR_OP_SNAPSHOT_SYNC:
        hold->durable = true;
        return VSR_IO_OK;
    case VSR_OP_SNAPSHOT_INSTALL:
        CHECK(task->op == hold->snapshot->checkpoint->op);
        for (uint64_t i = 0; i < task->op; i++)
            history_record(node, hold->snapshot->history[(size_t)i]);
        node->applied = task->op;
        node->checksum = hold->snapshot->checksum;
        if (node->cluster->application != NULL)
            node->cluster->application->install(node->application,
                                                hold->snapshot->image,
                                                hold->snapshot->image_size);
        return VSR_IO_OK;
    default:
        CHECK(false);
        return VSR_IO_FAILED;
    }
}

bool mem_node_execute(struct mem_node *node, size_t index, int status)
{
    struct mem_effect *effect;
    CHECK(index < node->effect_count);
    check_graphs(node, 0);
    effect = node->effects[index];
    if (effect->executed)
        return true;
    if (!effect->started) {
        effect->started = true;
        effect->status = status;
        effect->result_graph = mem_graph_create();
        if (status != VSR_IO_OK) {
            effect->executed = true;
            return true;
        }
        check_fence(node, &effect->op, "executed");
        if (effect->op.type == VSR_OP_STORE) {
            const uint64_t first = mem_store_readable(node->store) + 1;
            effect->status = mem_store_submit(node->store, effect->op.data);
            observe_store(node, first);
            if (effect->status != VSR_IO_OK) {
                effect->executed = true;
                return true;
            }
        }
    }
    switch (effect->op.type) {
    case VSR_OP_SEND: {
        struct mem_cluster *cluster = node->cluster;
        cluster->messages =
            message_append(cluster, effect->op.data, effect->op.arg);
        break;
    }
    case VSR_OP_REPLY: {
        struct mem_answer answer;
        answer.graph = mem_graph_create();
        answer.reply = mem_clone(answer.graph, MEM_REPLY, effect->op.data);
        answer.route = effect->op.arg;
        node->answers = resize(node->answers, node->answer_count + 1,
                               sizeof(*node->answers));
        node->answers[node->answer_count++] = answer;
        break;
    }
    case VSR_OP_LOAD: {
        struct vsr_loaded *loaded;
        const struct vsr_store_read *read = effect->op.data;
        ORACLE(node, &effect->op,
               read->type == VSR_LOAD_RECOVERY ||
                   read->sequence <= mem_store_readable(node->store),
               "LOAD type %" PRIu32 " names revision %" PRIu64
               " beyond readable frontier %" PRIu64,
               read->type, read->sequence, mem_store_readable(node->store));
        effect->status =
            mem_store_load(node->store, read, effect->result_graph, &loaded);
        effect->result = loaded;
        if (effect->status == VSR_IO_OK && read->type == VSR_LOAD_RECOVERY) {
            const struct vsr_recovered *recovered = loaded->items;
            if (recovered->checkpoint != NULL &&
                recovered->hard.role == VSR_MEMBER_FULL) {
                struct mem_snapshot_hold *hold =
                    snapshot_hold(node, recovered->checkpoint->id);
                CHECK(hold != NULL && hold->durable);
                hold->held = true;
            }
        }
        break;
    }
    case VSR_OP_STORE:
        if (((const struct vsr_store *)effect->op.data)->sequence >
            mem_store_readable(node->store))
            return false;
        break;
    case VSR_OP_SYNC:
        if (effect->op.arg > mem_store_readable(node->store))
            return false;
        effect->status = mem_store_sync(node->store, effect->op.arg);
        break;
    case VSR_OP_RECLAIM:
        mem_store_reclaim(node->store, effect->op.arg);
        break;
    case VSR_OP_APPLY: {
        const struct vsr_apply *apply = effect->op.data;
        struct vsr_applied *applied =
            mem_graph_alloc(effect->result_graph, 1, sizeof(*applied));
        struct vsr_value *results = mem_graph_alloc(
            effect->result_graph, apply->batch.count, sizeof(*results));
        CHECK(apply->batch.count != 0);
        for (uint32_t i = 0; i < apply->batch.count; i++) {
            results[i].reserved = 0;
            results[i].code =
                application_apply(node, &effect->op, &apply->batch.entries[i],
                                  effect->result_graph, &results[i]);
        }
        CHECK(node->applied == apply->through);
        applied->results = results;
        applied->count = apply->batch.count;
        effect->result = applied;
        break;
    }
    case VSR_OP_READ_READY: {
        const struct vsr_read_fence *fence = effect->op.data;
        ORACLE(node, &effect->op, fence->applied == node->applied,
               "cookie %" PRIu64 " fence applied %" PRIu64
               " but the application is at %" PRIu64 " when captured",
               fence->cookie, fence->applied, node->applied);
        node->reads =
            resize(node->reads, node->read_count + 1, sizeof(*node->reads));
        node->reads[node->read_count++] = *fence;
        break;
    }
    case VSR_OP_SNAPSHOT_CAPTURE:
    case VSR_OP_SNAPSHOT_FETCH:
    case VSR_OP_SNAPSHOT_SYNC:
    case VSR_OP_SNAPSHOT_INSTALL:
    case VSR_OP_SNAPSHOT_DROP:
        effect->status = snapshot_execute(node, effect);
        if (effect->status == VSR_IO_RETRY)
            return false;
        break;
    case VSR_OP_RELEASE:
    default:
        CHECK(false);
    }
    effect->executed = true;
    return true;
}

static void effect_destroy(struct mem_effect *effect)
{
    mem_graph_destroy(effect->graph);
    mem_graph_destroy(effect->result_graph);
    free(effect);
}

struct mem_step mem_node_notify(struct mem_node *node, size_t index, int status,
                                const void *data)
{
    struct mem_effect *effect;
    struct mem_graph *graph = NULL;
    struct vsr_event event;
    struct mem_step step;
    CHECK(index < node->effect_count);
    effect = node->effects[index];
    event =
        (struct vsr_event){VSR_EVENT_COMPLETE, status, effect->op.id, NULL, 0};
    if (data != NULL) {
        graph = mem_graph_create();
        switch (effect->op.type) {
        case VSR_OP_LOAD:
            event.data = mem_clone_loaded(
                graph, ((const struct vsr_store_read *)effect->op.data)->type,
                data);
            break;
        case VSR_OP_APPLY:
            event.data = mem_clone(graph, MEM_APPLIED, data);
            break;
        case VSR_OP_SNAPSHOT_CAPTURE:
        case VSR_OP_SNAPSHOT_FETCH:
            event.data = mem_clone(graph, MEM_CHECKPOINT, data);
            break;
        default:
            CHECK(false);
        }
    }
    step = submit_graph(node, &event, graph);
    if (step.consumed == 0)
        return step;
    CHECK(node->effects[index] == effect);
    effect_destroy(effect);
    if (index + 1 < node->effect_count)
        memmove(&node->effects[index], &node->effects[index + 1],
                (node->effect_count - index - 1) * sizeof(struct mem_effect *));
    node->effect_count--;
    node->completed++;
    check_stopped(node);
    return step;
}

bool mem_node_complete(struct mem_node *node, size_t index, int status)
{
    struct mem_step step;
    if (!mem_node_execute(node, index, status))
        return false;
    const struct mem_effect *effect = node->effects[index];
    step = mem_node_notify(node, index, effect->status, effect->result);
    CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
    return step.consumed != 0;
}

size_t mem_cluster_messages(const struct mem_cluster *cluster)
{
    return cluster->message_count;
}

const struct vsr_message *mem_cluster_message(const struct mem_cluster *cluster,
                                              size_t index, uint64_t *to)
{
    CHECK(index < cluster->message_count);
    if (to != NULL)
        *to = cluster->messages[index].to;
    return cluster->messages[index].message;
}

void mem_cluster_drop(struct mem_cluster *cluster, size_t index)
{
    CHECK(index < cluster->message_count);
    mem_graph_destroy(cluster->messages[index].graph);
    if (index + 1 < cluster->message_count)
        memmove(&cluster->messages[index], &cluster->messages[index + 1],
                (cluster->message_count - index - 1) *
                    sizeof(*cluster->messages));
    cluster->message_count--;
}

bool mem_cluster_deliver(struct mem_cluster *cluster, size_t index)
{
    struct mem_node *node;
    struct vsr_event event;
    struct mem_step step;
    CHECK(index < cluster->message_count);
    node = mem_cluster_find(cluster, cluster->messages[index].to);
    if (node == NULL || !mem_node_alive(node))
        return false;
    event = (struct vsr_event){VSR_EVENT_MESSAGE, 0, 0,
                               cluster->messages[index].message, 0};
    step = mem_node_event(node, &event);
    CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
    if (step.consumed == 0)
        return false;
    mem_cluster_drop(cluster, index);
    return true;
}

void mem_cluster_duplicate(struct mem_cluster *cluster, size_t index)
{
    CHECK(index < cluster->message_count);
    cluster->messages = message_append(
        cluster, cluster->messages[index].message, cluster->messages[index].to);
}

size_t mem_cluster_run(struct mem_cluster *cluster, size_t budget)
{
    size_t actions = 0;
    while (actions < budget) {
        bool progress = false;
        for (size_t i = 0; i < cluster->node_count && actions < budget; i++) {
            struct mem_node *node = cluster->nodes[i];
            CHECK(node->cluster == cluster);
            if (!mem_node_alive(node))
                continue;
            if (node->runnable) {
                const struct mem_step step = mem_node_event(node, NULL);
                CHECK(step.result == VSR_OK || step.result == VSR_AGAIN);
                actions++;
                progress = true;
            }
            /* MORE may mean a budget-limited poll encountered an input whose
             * dependency is an outstanding effect. Fair completion service
             * must share the turn with drains, even at work_per_step == 1.
             * A completion offered while output is pending may spend the work
             * quantum on that output and remain unconsumed; rotating the first
             * effect tried keeps every effect eventually served. */
            const size_t count = node->effect_count;
            bool completed = false;
            for (size_t k = 0; k < count && actions < budget; k++) {
                const size_t j = (node->effect_cursor + k) % count;
                if (mem_node_complete(node, j, VSR_IO_OK)) {
                    node->effect_cursor = j;
                    completed = true;
                    actions++;
                    progress = true;
                    break;
                }
            }
            if (!completed && count != 0)
                node->effect_cursor = (node->effect_cursor + 1) % count;
        }
        for (size_t i = 0; i < cluster->message_count && actions < budget;
             i++) {
            if (mem_cluster_deliver(cluster, i)) {
                actions++;
                progress = true;
                break;
            }
        }
        mem_cluster_check(cluster);
        if (!progress)
            break;
    }
    return actions;
}

void mem_node_crash(struct mem_node *node)
{
    check_graphs(node, 0);
    for (size_t i = 0; i < node->effect_count; i++)
        effect_destroy(node->effects[i]);
    node->effect_count = 0;
    node->effect_cursor = 0;
    for (size_t i = 0; i < node->lease_count; i++)
        mem_graph_destroy(node->leases[i].graph);
    node->lease_count = 0;
    free(node->arena);
    node->arena = NULL;
    node->core = NULL;
    node->runnable = false;
    node->admission_count = 0;
    node->issued = 0;
    node->completed = 0;
    application_reset(node);
    mem_store_crash(node->store);
    for (size_t i = node->snapshot_count; i > 0; i--) {
        const size_t index = i - 1;
        if (!node->snapshots[index].durable) {
            node->snapshots[index] = node->snapshots[node->snapshot_count - 1];
            node->snapshot_count--;
        } else {
            node->snapshots[index].held = false;
        }
    }
}

int mem_node_restart(struct mem_node *node, struct vsr_id incarnation)
{
    CHECK(node->core == NULL &&
          !same_id(node->options.incarnation, incarnation));
    node->options.incarnation = incarnation;
    /* A restart uses RECOVER even during warm-up; a learner keeps its join
     * role as the warm-up role to resume with should its store be lost. */
    node->options.start_mode = VSR_START_RECOVER;
    return node_init(node);
}

uint64_t mem_node_applied(const struct mem_node *node)
{
    return node->applied;
}

uint64_t mem_node_checksum(const struct mem_node *node)
{
    return node->checksum;
}

const struct vsr_entry *mem_node_history(const struct mem_node *node,
                                         uint64_t op)
{
    if (op == 0 || op > node->history_count)
        return NULL;
    return node->history[(size_t)op - 1];
}

size_t mem_node_replies(const struct mem_node *node)
{
    return node->answer_count;
}

const struct vsr_reply *mem_node_reply(const struct mem_node *node,
                                       size_t index, uint64_t *route)
{
    CHECK(index < node->answer_count);
    if (route != NULL)
        *route = node->answers[index].route;
    return node->answers[index].reply;
}

size_t mem_node_reads(const struct mem_node *node)
{
    return node->read_count;
}

const struct vsr_read_fence *mem_node_read(const struct mem_node *node,
                                           size_t index)
{
    CHECK(index < node->read_count);
    return &node->reads[index];
}

void mem_cluster_check(const struct mem_cluster *cluster)
{
    for (size_t i = 0; i < cluster->node_count; i++) {
        const struct mem_node *node = cluster->nodes[i];
        check_graphs(node, 0);
        if (mem_node_alive(node)) {
            struct vsr_status status;
            vsr_get_status(node->core, &status);
            CHECK(status.applied <= node->applied);
            CHECK(status.stored_sequence <= mem_store_readable(node->store));
            CHECK(status.durable_sequence <= mem_store_durable(node->store));
            CHECK(status.durable_sequence <= status.stored_sequence);
            CHECK(status.outstanding_ops == node->effect_count);
            CHECK(status.outstanding_leases == node->lease_count);
        }
        for (size_t j = i + 1; j < cluster->node_count; j++) {
            const struct mem_node *other = cluster->nodes[j];
            size_t common = node->history_count < other->history_count
                                ? node->history_count
                                : other->history_count;
            for (size_t k = 0; k < common; k++)
                CHECK(mem_entry_equal(node->history[k], other->history[k]));
            common = node->committed_count < other->committed_count
                         ? node->committed_count
                         : other->committed_count;
            for (size_t k = 0; k < common; k++)
                CHECK(mem_entry_equal(node->committed_history[k],
                                      other->committed_history[k]));
        }
        for (size_t k = 0; k < node->history_count && k < node->committed_count;
             k++)
            CHECK(
                mem_entry_equal(node->history[k], node->committed_history[k]));
    }
}

void mem_cluster_destroy(struct mem_cluster *cluster)
{
    if (cluster == NULL)
        return;
    while (cluster->message_count != 0)
        mem_cluster_drop(cluster, cluster->message_count - 1);
    mem_graph_destroy(cluster->message_vectors);
    for (size_t i = 0; i < cluster->node_count; i++) {
        struct mem_node *node = cluster->nodes[i];
        mem_node_crash(node);
        for (size_t j = 0; j < node->answer_count; j++)
            mem_graph_destroy(node->answers[j].graph);
        mem_graph_destroy(node->metadata);
        mem_graph_destroy(node->history_graph);
        mem_store_destroy(node->store);
        if (cluster->application != NULL)
            cluster->application->destroy(node->application);
        free(node->snapshots);
        free(node->admissions);
        free(node->reads);
        free(node->answers);
        free(node->history);
        free(node->committed_history);
        free(node->output);
        free(node->leases);
        free(node->effects);
        free(node);
    }
    for (size_t i = 0; i < cluster->snapshot_count; i++) {
        mem_graph_destroy(cluster->snapshots[i]->graph);
        free(cluster->snapshots[i]->image);
        free(cluster->snapshots[i]);
    }
    free(cluster->snapshots);
    free(cluster->executions);
    free(cluster->clients);
    mem_graph_destroy(cluster->oracle);
    free(cluster->nodes);
    free(cluster);
}
