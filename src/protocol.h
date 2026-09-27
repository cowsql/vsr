#ifndef VSR_PROTOCOL_H
#define VSR_PROTOCOL_H

#include "internal.h"
#include "objects.h"

/* Completion tags are partitioned so extension modules can own their effects. */
enum vsr_protocol_tag {
    VSR_TAG_BOOT = 1,
    VSR_TAG_STORE,
    VSR_TAG_SYNC,
    VSR_TAG_INSTALL,
    VSR_TAG_CLIENT,
    VSR_TAG_REQUEST,
    VSR_TAG_LOG,
    VSR_TAG_APPLY,
    VSR_TAG_SEND,
    VSR_TAG_REPLY,
    VSR_TAG_RECLAIM,
    VSR_TAG_EXTENSION = 256
};

enum vsr_route_state {
    VSR_ROUTE_FREE,
    VSR_ROUTE_CLIENT,
    VSR_ROUTE_CLIENT_LOADING,
    VSR_ROUTE_REQUEST,
    VSR_ROUTE_REQUEST_LOADING,
    VSR_ROUTE_DECIDE,
    VSR_ROUTE_WAIT,
    VSR_ROUTE_REPLY
};

struct vsr_peer {
    uint64_t id;
    uint64_t prepared;
    uint64_t sent;
    uint64_t commit_sent;
    uint64_t retry_at;
    bool sending;
    bool heartbeat;
};

struct vsr_log_slot {
    struct vsr_entry entry;
    uint64_t sequence; /* zero before APPEND submission */
    uint32_t lease;
    bool used;
};

struct vsr_client_slot {
    struct vsr_id id;
    uint64_t completed;
    uint64_t completed_op;
    uint64_t request;
    uint64_t request_op;
    uint64_t stamp;
};

struct vsr_route {
    struct vsr_request request;
    struct vsr_client_record completed;
    uint64_t route;
    uint64_t op;
    uint64_t read_sequence;
    uint64_t history_generation;
    uint64_t latest;
    uint64_t latest_op;
    uint64_t check_epoch;
    uint32_t lease;
    uint32_t result_lease;
    uint32_t state;
    uint32_t reply;
    bool query;
    bool executed;
};

struct vsr_transaction {
    uint64_t sequence;
    uint64_t append_end;
    uint64_t committed;
    uint64_t clients_through;
    uint64_t begin; /* retained log begin after TRIM/RESTORE; zero otherwise */
    bool completed;
};

struct vsr_protocol {
    struct vsr_epoch epoch;
    struct vsr_membership current;
    struct vsr_membership previous;
    struct vsr_member *current_members;
    struct vsr_member *previous_members;
    struct vsr_peer *peers;
    struct vsr_log_slot *log;
    struct vsr_client_slot *clients;
    struct vsr_route *routes;
    struct vsr_transaction *transactions;
    struct vsr_entry *batch;
    struct vsr_client_record *results;
    uint64_t next_sequence;
    uint64_t safe_sequence;
    uint64_t sync_requested;
    uint64_t sync_completed;
    /* log_begin describes the safe revision that offers name; readable_begin
     * describes stored_sequence, which every LOAD names. A TRIM or RESTORE is
     * readable before it is safe, so readable_begin >= log_begin always. */
    uint64_t log_begin;
    uint64_t readable_begin;
    uint64_t log_end;
    uint64_t written_end;
    uint64_t stable_end;
    uint64_t desired_commit;
    uint64_t stable_commit;
    uint64_t notified_commit;
    uint64_t proposed_boundary;
    uint64_t clients_stored;
    uint64_t clients_sequence;
    uint64_t applied_view;
    uint64_t nonce_counter;
    uint64_t reclaim_sequence;
    uint64_t reclaimed_sequence;
    uint64_t last_normal_view;
    uint64_t hard_sequence;
    uint64_t heartbeat_at;
    uint64_t election_at;
    uint64_t retry_at;
    uint64_t append_at;
    uint64_t load_first;
    uint64_t load_end;
    uint64_t cache_stamp;
    uint64_t history_generation;
    uint64_t results_through;
    uint64_t results_sequence;
    uint32_t self;
    uint32_t transaction_capacity;
    uint32_t results_count;
    uint32_t results_lease;
    uint32_t peer_cursor;
    uint32_t route_cursor;
    uint32_t boot;
    bool hard_dirty;
    bool identity_pending;
    bool sync_busy;
    bool log_loading;
    bool reclaim_busy;
    bool application_busy;
    bool results_pending;
    bool replay;
    bool checkpoint_requested;
    bool stopped;
    void *checkpoint;
    void *reads;
    void *extension; /* Root-owned election/recovery/transfer/read module. */
};

static inline struct vsr_protocol *vsr_protocol(struct vsr *v)
{
    return v->protocol;
}
static inline const struct vsr_protocol *vsr_protocol_const(const struct vsr *v)
{
    return v->protocol;
}

/* Helpers are bounded by configured capacities. Metadata is copied by emit;
 * leases name the external graphs backing payload spans in that metadata. */
bool vsr_protocol_emit(struct vsr *v, uint32_t type, uint64_t arg, uint64_t tag,
                       const void *data, const uint32_t *leases, uint32_t count,
                       uint64_t completion_bytes);
bool vsr_protocol_send(struct vsr *v, uint64_t peer, uint32_t type,
                       uint64_t number, const void *body, uint32_t lease);
void vsr_protocol_hard(struct vsr *v, struct vsr_hard_state *hard);
void vsr_protocol_configuration(struct vsr *v, const struct vsr_epoch *epoch);
/* Invalidate client indexes and in-flight route reads after history rebasing. */
void vsr_protocol_history_changed(struct vsr *v);
struct vsr_log_slot *vsr_protocol_log_find(struct vsr *v, uint64_t op);
bool vsr_protocol_log_put(struct vsr *v, const struct vsr_entry *entry,
                          uint32_t lease, uint64_t sequence);
void vsr_protocol_log_clear(struct vsr *v, uint64_t first);
void vsr_protocol_normal(struct vsr *v);
uint64_t vsr_protocol_available_bytes(const struct vsr *v);
bool vsr_protocol_load_log(struct vsr *v, uint64_t first, uint64_t end);

bool vsr_protocol_store(struct vsr *v, const struct vsr_change *changes,
                        uint32_t count, const uint32_t *leases,
                        uint32_t lease_count, uint64_t append_end,
                        uint64_t committed, uint64_t clients_through,
                        uint64_t *sequence);
bool vsr_protocol_ready(const struct vsr *v);
bool vsr_protocol_noop(struct vsr *v);
bool vsr_protocol_nonce(struct vsr *v, struct vsr_nonce *nonce);

#endif
