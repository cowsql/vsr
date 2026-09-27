#ifndef VSR_TEST_MEMORY_CLUSTER_H
#define VSR_TEST_MEMORY_CLUSTER_H

#include "lib/memory.h"

/* Fully in-memory deterministic host. Selection is explicit; no clocks,
 * threads, sockets, files, or hidden random choices. Indices are queue positions
 * and change after removal. Node pointers remain stable until destruction. */
struct mem_cluster;
struct mem_node;

struct mem_step {
    int result;
    uint32_t consumed;
    uint32_t emitted;
    uint32_t flags;
    uint64_t deadline;
};

struct mem_cluster *mem_cluster_create(void);
void mem_cluster_destroy(struct mem_cluster *cluster);
struct mem_node *mem_cluster_add(struct mem_cluster *cluster,
                                 const struct vsr_options *options);
struct vsr_options mem_options(uint64_t replica,
                               const struct vsr_membership *membership);
struct mem_node *mem_cluster_find(const struct mem_cluster *cluster,
                                  uint64_t replica);
struct vsr *mem_node_core(struct mem_node *node);
struct mem_store *mem_node_store(struct mem_node *node);
uint64_t mem_node_id(const struct mem_node *node);
bool mem_node_alive(const struct mem_node *node);
void mem_node_output_capacity(struct mem_node *node, uint32_t capacity);
/* Clones the complete input graph and chooses its lease. COMPLETE events are
 * submitted through mem_node_complete. A NULL input drains runnable work. */
struct mem_step mem_node_event(struct mem_node *node,
                               const struct vsr_event *event);
struct mem_step mem_node_time(struct mem_node *node, uint64_t now);
size_t mem_node_leases(const struct mem_node *node);
size_t mem_node_effects(const struct mem_node *node);
const struct vsr_op *mem_node_effect(const struct mem_node *node, size_t index);
/* Execution and notification are independent so writes/sends can finish before
 * their completion events arrive. False means an external dependency is still
 * pending (e.g. a preceding STORE or a future snapshot revision). */
bool mem_node_execute(struct mem_node *node, size_t index, int status);
bool mem_node_complete(struct mem_node *node, size_t index, int status);
/* Explicit completion injection for contract tests. Does not execute the
 * effect. A rejected event leaves the effect active; consumed completion ends
 * its graph lifetime. Data is cloned using the pending operation's schema. */
struct mem_step mem_node_notify(struct mem_node *node, size_t index, int status,
                                const void *data);
size_t mem_cluster_messages(const struct mem_cluster *cluster);
const struct vsr_message *mem_cluster_message(const struct mem_cluster *cluster,
                                              size_t index, uint64_t *to);
bool mem_cluster_deliver(struct mem_cluster *cluster, size_t index);
void mem_cluster_drop(struct mem_cluster *cluster, size_t index);
void mem_cluster_duplicate(struct mem_cluster *cluster, size_t index);
/* Executes/completes effects and delivers messages in FIFO order until idle or
 * the supplied action budget is exhausted. Does not advance logical time. */
size_t mem_cluster_run(struct mem_cluster *cluster, size_t budget);
void mem_node_crash(struct mem_node *node);
int mem_node_restart(struct mem_node *node, struct vsr_id incarnation);
uint64_t mem_node_applied(const struct mem_node *node);
uint64_t mem_node_checksum(const struct mem_node *node);
const struct vsr_entry *mem_node_history(const struct mem_node *node,
                                         uint64_t op);
size_t mem_node_replies(const struct mem_node *node);
const struct vsr_reply *mem_node_reply(const struct mem_node *node,
                                       size_t index, uint64_t *route);
size_t mem_node_reads(const struct mem_node *node);
const struct vsr_read_fence *mem_node_read(const struct mem_node *node,
                                           size_t index);
/* Checks agreement of all externally executed histories, and exposed core
 * progress against adapter frontiers. Historical execution survives crashes. */
void mem_cluster_check(const struct mem_cluster *cluster);

#endif /* VSR_TEST_MEMORY_CLUSTER_H */
