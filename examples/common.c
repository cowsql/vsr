#include "config.h"

#include "common.h"

#include <stdarg.h>
#include <stdlib.h>

enum { EXAMPLE_CLIENT_SPACE = 100 }; /* High half of every client ID. */

static void fail(const char *why)
{
    fprintf(stderr, "example failed: %s\n", why);
    abort();
}

void say(const char *format, ...)
{
    va_list arguments;
    va_start(arguments, format);
    vprintf(format, arguments);
    va_end(arguments);
    putchar('\n');
    fflush(stdout);
}

const char *example_state_name(uint32_t state)
{
    static const char *const names[] = {
        "STARTING",      "WARMING", "RECOVERING", "NORMAL",   "VIEW_CHANGE",
        "TRANSITIONING", "RETIRED", "FAILED",     "STOPPING", "STOPPED"};
    return state < sizeof(names) / sizeof(names[0]) ? names[state] : "?";
}

const char *example_role_name(uint32_t role)
{
    return role == VSR_MEMBER_FULL      ? "full"
           : role == VSR_MEMBER_WITNESS ? "witness"
                                        : "none";
}

const char *example_reply_status_name(uint32_t status)
{
    static const char *const names[] = {
        "OK",    "NOT_PRIMARY",   "NEW_EPOCH",      "BUSY",         "INVALID",
        "LIMIT", "STALE_REQUEST", "CLIENT_UNKNOWN", "CLIENT_STATE", "TIMEOUT"};
    return status < sizeof(names) / sizeof(names[0]) ? names[status] : "?";
}

static const char *durability_name(uint32_t durability)
{
    return durability == VSR_DURABLE ? "durable" : "replicated";
}

static struct mem_node *add_node(struct example_cluster *cluster,
                                 struct vsr_options *options)
{
    const uint32_t index = cluster->count;
    CHECK(index < EXAMPLE_MAX_REPLICAS);
    options->durability = cluster->durability;
    cluster->nodes[index] = mem_cluster_add(cluster->host, options);
    cluster->generation[index] = options->incarnation.lo;
    cluster->count = index + 1;
    /* The first TIME event fixes the clock origin and starts boot work. */
    CHECK(mem_node_time(cluster->nodes[index], cluster->now).consumed == 1);
    return cluster->nodes[index];
}

struct example_cluster example_start(const char *title, uint32_t count,
                                     uint32_t faults, uint32_t durability,
                                     const uint32_t *roles)
{
    struct example_cluster cluster = {.host = mem_cluster_create(),
                                      .durability = durability,
                                      .genesis = count,
                                      .faults = faults,
                                      .next_route = 1};
    char text[EXAMPLE_TEXT_BYTES];
    size_t length = 0;
    CHECK(count <= EXAMPLE_MAX_REPLICAS);
    mem_cluster_set_application(cluster.host, &kv_application);
    for (uint32_t i = 0; i < count; ++i) {
        const uint32_t role = roles == NULL ? VSR_MEMBER_FULL : roles[i];
        cluster.members[i] = (struct vsr_member){i + 1, role, 0};
        length += (size_t)snprintf(
            text + length, sizeof(text) - length, "%sreplica %" PRIu32 " (%s)",
            i == 0 ? "" : ", ", i + 1, example_role_name(role));
    }
    say("== %s ==", title);
    say("group: %s; f = %" PRIu32 ", quorum = %" PRIu32 ", %s storage", text,
        faults, count - faults, durability_name(durability));
    for (uint32_t i = 0; i < count; ++i) {
        const struct vsr_membership seed = example_seed(&cluster);
        struct vsr_options options = mem_options(i + 1, &seed);
        add_node(&cluster, &options);
    }
    example_run(&cluster);
    struct vsr_status status = example_status(example_primary(&cluster));
    say("replica %" PRIu64 " is primary in view %" PRIu64, status.primary,
        status.view);
    return cluster;
}

struct vsr_membership example_seed(const struct example_cluster *cluster)
{
    /* The struct is returned by value, so the pointer is rebuilt from the
     * caller's copy instead of being stored inside the struct itself. */
    return (struct vsr_membership){0, cluster->members, cluster->genesis,
                                   cluster->faults};
}

struct mem_node *example_join(struct example_cluster *cluster, uint32_t role)
{
    const struct vsr_membership seed = example_seed(cluster);
    struct vsr_options options = mem_options(cluster->count + 1, &seed);
    struct mem_node *node;
    options.start_mode = VSR_START_JOIN;
    options.join_role = role;
    node = add_node(cluster, &options);
    say("replica %" PRIu64 " joins as a nonvoting %s replica",
        mem_node_id(node), example_role_name(role));
    return node;
}

struct mem_node *example_replica(struct example_cluster *cluster, uint64_t id)
{
    struct mem_node *node = mem_cluster_find(cluster->host, id);
    CHECK(node != NULL);
    return node;
}

struct mem_node *example_primary(struct example_cluster *cluster)
{
    for (uint32_t i = 0; i < cluster->count; ++i) {
        struct mem_node *node = cluster->nodes[i];
        if (!mem_node_alive(node))
            continue;
        struct vsr_status status = example_status(node);
        if (status.state == VSR_STATE_NORMAL &&
            status.primary == mem_node_id(node))
            return node;
    }
    fail("no replica is primary");
    return NULL;
}

struct vsr_status example_status(struct mem_node *node)
{
    struct vsr_status status;
    vsr_get_status(mem_node_core(node), &status);
    CHECK(status.failure.code == VSR_FAILURE_NONE);
    return status;
}

struct kv *example_store(struct mem_node *node)
{
    return mem_node_application(node);
}

void example_show_store(struct mem_node *node)
{
    char text[EXAMPLE_TEXT_BYTES];
    const struct kv *store = example_store(node);
    say("replica %" PRIu64 " store: %s (%" PRIu64 " commands executed here)",
        mem_node_id(node), kv_format(store, text, sizeof(text)),
        store->commands);
}

void example_run(struct example_cluster *cluster)
{
    CHECK(mem_cluster_run(cluster->host, EXAMPLE_ACTION_BUDGET) <
          EXAMPLE_ACTION_BUDGET);
    mem_cluster_check(cluster->host);
}

/* A hand-rolled scheduler equivalent to mem_cluster_run: drain every replica,
 * complete its effects, deliver queued messages, repeat until nothing moves.
 * It can keep one effect type pending and count messages by sender/type. */
static size_t pump(struct example_cluster *cluster, struct mem_node *hold_node,
                   uint32_t held_type, uint64_t count_from, uint32_t count_type)
{
    size_t counted = 0;
    for (uint32_t step = 0; step < EXAMPLE_ACTION_BUDGET; ++step) {
        bool progress = false;
        for (uint32_t i = 0; i < cluster->count; ++i) {
            struct mem_node *node = cluster->nodes[i];
            if (!mem_node_alive(node))
                continue;
            struct mem_step drained = mem_node_event(node, NULL);
            progress |=
                drained.emitted != 0 || (drained.flags & VSR_UPDATE_MORE) != 0;
            for (size_t j = 0; j < mem_node_effects(node); ++j) {
                if (node == hold_node &&
                    mem_node_effect(node, j)->type == held_type)
                    continue;
                if (mem_node_complete(node, j, VSR_IO_OK)) {
                    progress = true;
                    break;
                }
            }
        }
        for (size_t i = 0; i < mem_cluster_messages(cluster->host); ++i) {
            const struct vsr_message *message =
                mem_cluster_message(cluster->host, i, NULL);
            const bool counts =
                message->from == count_from && message->type == count_type;
            if (mem_cluster_deliver(cluster->host, i)) {
                counted += counts ? 1 : 0;
                progress = true;
                break;
            }
        }
        if (!progress) {
            mem_cluster_check(cluster->host);
            return counted;
        }
    }
    fail("the host did not become idle within its action budget");
    return counted;
}

void example_run_holding(struct example_cluster *cluster,
                         struct mem_node *hold_node, uint32_t held_type)
{
    pump(cluster, hold_node, held_type, 0, 0);
}

size_t example_run_counting(struct example_cluster *cluster, uint64_t from,
                            uint32_t message_type)
{
    return pump(cluster, NULL, 0, from, message_type);
}

void example_elapse(struct example_cluster *cluster, uint64_t duration)
{
    const struct vsr_membership seed = example_seed(cluster);
    const uint64_t step = mem_options(1, &seed).retry_ns;
    const uint64_t until = cluster->now + duration;
    while (cluster->now < until) {
        cluster->now =
            until - cluster->now < step ? until : cluster->now + step;
        for (uint32_t i = 0; i < cluster->count; ++i)
            if (mem_node_alive(cluster->nodes[i]))
                CHECK(mem_node_time(cluster->nodes[i], cluster->now).consumed ==
                      1);
        example_run(cluster);
    }
}

void example_crash(struct example_cluster *cluster, struct mem_node *node)
{
    (void)cluster;
    say("crash replica %" PRIu64, mem_node_id(node));
    mem_node_crash(node);
}

void example_restart(struct example_cluster *cluster, struct mem_node *node)
{
    const uint64_t id = mem_node_id(node);
    uint64_t *generation = &cluster->generation[id - 1];
    CHECK(cluster->nodes[id - 1] == node);
    *generation += 1;
    say("restart replica %" PRIu64 " as incarnation %" PRIu64
        " (%s storage kept; memory lost)",
        id, *generation, durability_name(cluster->durability));
    CHECK(mem_node_restart(node, (struct vsr_id){id, *generation}) == VSR_OK);
    CHECK(mem_node_time(node, cluster->now).consumed == 1);
    example_run(cluster);
}

void example_checkpoint(struct example_cluster *cluster, struct mem_node *node)
{
    const struct vsr_event hint = {VSR_EVENT_CHECKPOINT, VSR_IO_OK, 0, NULL, 0};
    CHECK(mem_node_event(node, &hint).consumed == 1);
    example_run(cluster);
    say("replica %" PRIu64
        " captured and published a checkpoint at op %" PRIu64,
        mem_node_id(node), example_status(node).checkpoint_op);
}

struct example_client example_client(const char *name, uint64_t id)
{
    return (struct example_client){name, {EXAMPLE_CLIENT_SPACE, id}, 0, 0};
}

/* Submits a request under the client's current number: a new request after
 * example_submit_request/example_submit bumped it, otherwise a retry. */
static uint64_t submit_as(struct example_cluster *cluster,
                          struct mem_node *node,
                          const struct example_client *client, uint32_t type,
                          const void *body)
{
    const uint64_t route = cluster->next_route++;
    const struct vsr_request request = {
        {client->id, client->number}, client->epoch, type, 0, body};
    /* The host clones the request graph, so the placeholder lease 1 and this
     * stack-local request are fine here; a production host keeps the bytes
     * immutable until the matching VSR_OP_RELEASE. */
    const struct vsr_event event = {VSR_EVENT_REQUEST, VSR_IO_OK, route,
                                    &request, 1};
    CHECK(mem_node_event(node, &event).consumed == 1);
    return route;
}

uint64_t example_submit_request(struct example_cluster *cluster,
                                struct mem_node *node,
                                struct example_client *client, uint32_t type,
                                const void *body)
{
    client->number += 1;
    return submit_as(cluster, node, client, type, body);
}

static uint64_t submit_command(struct example_cluster *cluster,
                               struct mem_node *node,
                               struct example_client *client,
                               const char *command)
{
    const struct vsr_span span = {command, strlen(command)};
    const struct vsr_blob body = {&span, span.size, 1, 0};
    return submit_as(cluster, node, client, VSR_REQUEST_COMMAND, &body);
}

uint64_t example_submit(struct example_cluster *cluster, struct mem_node *node,
                        struct example_client *client, const char *command)
{
    client->number += 1;
    return submit_command(cluster, node, client, command);
}

const struct vsr_reply *example_reply(struct mem_node *node, uint64_t route)
{
    for (size_t i = mem_node_replies(node); i > 0; --i) {
        uint64_t found;
        const struct vsr_reply *reply = mem_node_reply(node, i - 1, &found);
        if (found == route)
            return reply;
    }
    fail("no reply was produced for the request");
    return NULL;
}

const char *example_result(const struct vsr_reply *reply, char *text,
                           size_t size)
{
    size_t length = 0;
    for (uint32_t i = 0; i < reply->result.data.count; ++i) {
        const struct vsr_span *span = &reply->result.data.spans[i];
        const size_t room = size - 1 - length;
        const size_t take = span->size < room ? span->size : room;
        memcpy(text + length, span->data, take);
        length += take;
    }
    text[length] = '\0';
    return text;
}

static const struct vsr_reply *await(struct example_cluster *cluster,
                                     struct mem_node *node,
                                     const struct example_client *client,
                                     const char *command, uint64_t route,
                                     const char *how)
{
    char text[EXAMPLE_TEXT_BYTES];
    const struct vsr_reply *reply;
    example_run(cluster);
    reply = example_reply(node, route);
    if (reply->status == VSR_REPLY_OK)
        say("%s %s \"%s\" to replica %" PRIu64 " -> OK \"%s\" (op %" PRIu64 ")",
            client->name, how, command, mem_node_id(node),
            example_result(reply, text, sizeof(text)), reply->op);
    else if (reply->status == VSR_REPLY_NOT_PRIMARY)
        say("%s %s \"%s\" to replica %" PRIu64
            " -> NOT_PRIMARY (primary is replica %" PRIu64 ")",
            client->name, how, command, mem_node_id(node), reply->primary);
    else
        say("%s %s \"%s\" to replica %" PRIu64 " -> %s", client->name, how,
            command, mem_node_id(node),
            example_reply_status_name(reply->status));
    return reply;
}

const struct vsr_reply *example_call(struct example_cluster *cluster,
                                     struct mem_node *node,
                                     struct example_client *client,
                                     const char *command)
{
    const uint64_t route = example_submit(cluster, node, client, command);
    return await(cluster, node, client, command, route, "sends");
}

const struct vsr_reply *example_retry(struct example_cluster *cluster,
                                      struct mem_node *node,
                                      struct example_client *client,
                                      const char *command)
{
    const uint64_t route = submit_command(cluster, node, client, command);
    return await(cluster, node, client, command, route, "retries");
}

void example_finish(struct example_cluster *cluster)
{
    mem_cluster_check(cluster->host);
    mem_cluster_destroy(cluster->host);
    say("done");
}
