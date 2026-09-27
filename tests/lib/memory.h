#ifndef VSR_TEST_MEMORY_H
#define VSR_TEST_MEMORY_H

#include "vsr.h"

#include <stdbool.h>
#include <stddef.h>

/* Test-only ownership: every allocation in a graph dies together. All clone
 * functions copy the complete reachable tree, including scatter/gather bytes.
 * Inputs must be structurally valid; malformed API tests can use raw objects. */
struct mem_graph;
enum mem_kind {
    MEM_ID,
    MEM_BLOB,
    MEM_MEMBERSHIP,
    MEM_EPOCH,
    MEM_REQUEST,
    MEM_ENTRY,
    MEM_CLIENT,
    MEM_REPLY,
    MEM_CHECKPOINT,
    MEM_MESSAGE,
    MEM_HARD_STATE,
    MEM_IDENTITY,
    MEM_RECOVERED,
    MEM_STORE_READ,
    MEM_STORE,
    MEM_APPLY,
    MEM_APPLIED,
    MEM_READ_BARRIER,
    MEM_READ_FENCE,
    MEM_SNAPSHOT_TASK
};

struct mem_graph *mem_graph_create(void);
void mem_graph_destroy(struct mem_graph *graph);
void *mem_graph_alloc(struct mem_graph *graph, size_t count, size_t size);
/* Watch complete source graphs while cloning borrowed output payloads. The
 * output descriptor array itself is intentionally not watched. Freeze instead
 * watches this graph's owned allocations before submitting an immutable input.
 * Source/frozen memory must remain alive until the corresponding pin ends. */
void mem_graph_watch_sources(struct mem_graph *graph);
void mem_graph_freeze(struct mem_graph *graph);
bool mem_graph_unchanged(const struct mem_graph *graph);
void *mem_clone(struct mem_graph *graph, enum mem_kind kind,
                const void *object);
struct vsr_loaded *mem_clone_loaded(struct mem_graph *graph, uint32_t load_type,
                                    const struct vsr_loaded *loaded);
struct vsr_op mem_clone_op(struct mem_graph *graph, const struct vsr_op *op);
bool mem_blob_equal(const struct vsr_blob *a, const struct vsr_blob *b);
bool mem_entry_equal(const struct vsr_entry *a, const struct vsr_entry *b);

/* A deliberately simple independent storage oracle. Each transaction produces
 * a complete immutable revision. Out-of-order submissions become readable only
 * as a contiguous prefix. Crash discards every revision after the last sync.
 * Borrowed pointers survive until reclaim/crash/destroy. */
struct mem_store;
struct mem_store *mem_store_create(void);
void mem_store_destroy(struct mem_store *store);
int mem_store_submit(struct mem_store *store,
                     const struct vsr_store *transaction);
int mem_store_sync(struct mem_store *store, uint64_t sequence);
void mem_store_crash(struct mem_store *store);
void mem_store_reclaim(struct mem_store *store, uint64_t oldest);
uint64_t mem_store_readable(const struct mem_store *store);
uint64_t mem_store_durable(const struct mem_store *store);
const struct vsr_recovered *mem_store_recovered(const struct mem_store *store,
                                                uint64_t sequence);
const struct vsr_entry *mem_store_entry(const struct mem_store *store,
                                        uint64_t sequence, uint64_t op);
const struct vsr_client_record *mem_store_client(const struct mem_store *store,
                                                 uint64_t sequence,
                                                 struct vsr_id client);
/* Output lives in the supplied graph, independently of revision retention. */
int mem_store_load(const struct mem_store *store,
                   const struct vsr_store_read *read, struct mem_graph *graph,
                   struct vsr_loaded **loaded);
/* Register the client table contained in an immutable snapshot. Witness remote
 * anchors need no registration; RESTORE requires the snapshot locally. */
void mem_store_checkpoint(struct mem_store *store,
                          const struct vsr_checkpoint *checkpoint,
                          const struct vsr_client_record *clients,
                          uint32_t count);
void mem_store_checkpoint_from_revision(struct mem_store *store,
                                        const struct vsr_checkpoint *checkpoint,
                                        uint64_t sequence);
bool mem_store_checkpoint_copy(struct mem_store *destination,
                               const struct mem_store *source,
                               const struct vsr_checkpoint *checkpoint);

#endif /* VSR_TEST_MEMORY_H */
