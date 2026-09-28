#ifndef VSR_EXAMPLE_COMMON_H
#define VSR_EXAMPLE_COMMON_H

/*
 * Shared driver for the example programs.
 *
 * The core performs no I/O: it consumes events (requests, peer messages, time,
 * completions) and returns effects (sends, replies, storage, APPLY, snapshots).
 * The deterministic in-memory host in tests/lib executes those effects. These
 * helpers wrap the host so each program reads as a story: start a group, send
 * commands as a named client, advance the logical clock, crash and restart
 * replicas, and print what happened. CHECK() states the expectations that
 * make each program a contract as well as a demonstration.
 */

#include "kv.h"
#include "lib/check.h"
#include "lib/memory_cluster.h"

#include <inttypes.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    EXAMPLE_MAX_REPLICAS = 6,
    EXAMPLE_TEXT_BYTES = 256,
    EXAMPLE_ACTION_BUDGET = 100000 /* Host actions per run before giving up. */
};

/* A client incarnation: requests are identified by client ID and number. */
struct example_client {
    const char *name;
    struct vsr_id id;
    uint64_t number; /* Last request number issued. */
    uint64_t epoch;  /* Routing epoch the client believes is current. */
};

struct example_cluster {
    struct mem_cluster *host;
    struct mem_node *nodes[EXAMPLE_MAX_REPLICAS];
    uint32_t count;
    uint32_t durability;
    struct vsr_member members[EXAMPLE_MAX_REPLICAS]; /* Genesis group. */
    uint32_t genesis;    /* Members in the genesis group. */
    uint32_t faults;     /* Tolerated faults of the genesis group. */
    uint64_t now;        /* Logical clock, nanoseconds. */
    uint64_t next_route; /* Reply route per submitted request. */
    uint64_t generation[EXAMPLE_MAX_REPLICAS]; /* Incarnations per replica. */
};

/* Narration: one line of plain text on stdout. */
__attribute__((format(printf, 1, 2))) void say(const char *format, ...);
const char *example_state_name(uint32_t state);
const char *example_role_name(uint32_t role);
const char *example_reply_status_name(uint32_t status);

/* Starts replicas 1..count with the key-value application; roles may be NULL
 * (all full). Runs until the group is in normal operation and narrates it. */
struct example_cluster example_start(const char *title, uint32_t count,
                                     uint32_t faults, uint32_t durability,
                                     const uint32_t *roles);
/* The genesis membership, pointing at the cluster's own member array. */
struct vsr_membership example_seed(const struct example_cluster *cluster);
/* Adds a nonvoting replica that joins an existing group with the given role. */
struct mem_node *example_join(struct example_cluster *cluster, uint32_t role);
struct mem_node *example_replica(struct example_cluster *cluster, uint64_t id);
/* The replica that currently leads (state NORMAL, primary == its own ID). */
struct mem_node *example_primary(struct example_cluster *cluster);
struct vsr_status example_status(struct mem_node *node);
struct kv *example_store(struct mem_node *node);
/* Prints "replica N store: a=1 b=2 (N commands executed)". */
void example_show_store(struct mem_node *node);

/* Executes effects and delivers messages until the host is idle. */
void example_run(struct example_cluster *cluster);
/* Same, but leaves effects of held_type on hold_node pending. */
void example_run_holding(struct example_cluster *cluster,
                         struct mem_node *hold_node, uint32_t held_type);
/* Same, counting messages of one type sent by one replica. */
size_t example_run_counting(struct example_cluster *cluster, uint64_t from,
                            uint32_t message_type);
/* Advances the logical clock in retry-sized steps, running after each. */
void example_elapse(struct example_cluster *cluster, uint64_t duration);
void example_crash(struct example_cluster *cluster, struct mem_node *node);
/* Restarts with a fresh incarnation; the store survives, the arena does not. */
void example_restart(struct example_cluster *cluster, struct mem_node *node);
/* Asks the replica to capture and publish a checkpoint now. */
void example_checkpoint(struct example_cluster *cluster, struct mem_node *node);

struct example_client example_client(const char *name, uint64_t id);
/* Submits a command as a new request; returns its reply route. */
uint64_t example_submit(struct example_cluster *cluster, struct mem_node *node,
                        struct example_client *client, const char *command);
/* Submits any request type with the client's next number. */
uint64_t example_submit_request(struct example_cluster *cluster,
                                struct mem_node *node,
                                struct example_client *client, uint32_t type,
                                const void *body);
/* The reply received for a route; the request must have completed. */
const struct vsr_reply *example_reply(struct mem_node *node, uint64_t route);
/* Copies a reply's result bytes into text. */
const char *example_result(const struct vsr_reply *reply, char *text,
                           size_t size);
/* Submits a command as a new request, runs the host, and narrates the reply,
 * e.g. client A sends "set counter 7" to replica 1 -> OK "7" (op 1). */
const struct vsr_reply *example_call(struct example_cluster *cluster,
                                     struct mem_node *node,
                                     struct example_client *client,
                                     const char *command);
/* Resends the client's last request unchanged, as a client retry would. */
const struct vsr_reply *example_retry(struct example_cluster *cluster,
                                      struct mem_node *node,
                                      struct example_client *client,
                                      const char *command);

/* Checks host invariants and frees everything. */
void example_finish(struct example_cluster *cluster);

#endif
