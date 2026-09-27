#ifndef VSR_H
#define VSR_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Viewstamped Replication core -- interface sketch, not an implementation.
 *
 * One owner thread per instance; no overlapping/reentrant calls, callbacks,
 * I/O, clock reads, randomness, or hidden allocation. All external work is
 * represented by operations and completed by events. See docs/vsr-api.md.
 *
 * These are naturally aligned, host-side logical views, NOT wire/disk structs.
 * The adapter owns framing, encoding, checksums, authentication, compression,
 * addresses, file/block layout, buffering, and physical I/O scheduling.
 * Reserved fields must be zero. Counts/ranges must fit configured limits.
 * All operation-number ranges are [first, end); operation numbers start at 1
 * and remain increasing across epochs. Epoch/view numbers start at 0.
 */
struct vsr;

#define VSR_NO_REPLICA UINT64_C(0)
#define VSR_NO_DEADLINE UINT64_MAX

struct vsr_id {
    uint64_t hi;
    uint64_t lo;
};

struct vsr_span {
    const void *data;
    size_t size;
};

/* No coalescing/copy required: spans can reference registered I/O buffers. */
struct vsr_blob {
    const struct vsr_span *spans;
    uint64_t size;                /* Sum of span sizes, checked on admission. */
    uint32_t count;
    uint32_t reserved;
};

enum vsr_result {
    VSR_OK = 0,
    VSR_AGAIN = 1,               /* Accepted a prefix; see update flags. */
    VSR_EINVAL = -1,
    VSR_ELIMIT = -2,
    VSR_EBUSY = -3
};

enum vsr_io_status {
    VSR_IO_OK = 0,
    VSR_IO_RETRY,
    VSR_IO_NOT_FOUND,
    VSR_IO_CORRUPT,
    VSR_IO_FAILED,
    VSR_IO_CANCELLED
};

enum vsr_durability {
    VSR_DURABLE,                 /* Persist safety state before relying on it. */
    VSR_REPLICATED               /* VR recovery quorum required after restart. */
};

enum vsr_member_role {
    VSR_MEMBER_NONE,             /* Status only; invalid in a membership list. */
    VSR_MEMBER_FULL,             /* Stores/applies state; eligible as primary. */
    VSR_MEMBER_WITNESS           /* Votes/stores log; never primary or executor. */
};

struct vsr_member {
    uint64_t id;                 /* Stable, nonzero; independent of array index. */
    uint32_t role;               /* enum vsr_member_role */
    uint32_t reserved;
};

/*
 * Members are sorted by ID, with no duplicates. count >= 2*faults + 1;
 * quorum = count - faults; at least faults + 1 members must be FULL.
 * A one-member, zero-fault group is permitted for development.
 * Primary selection rotates through FULL members in this canonical order.
 * Warming nodes are not voters and do not appear here until reconfiguration.
 */
struct vsr_membership {
    uint64_t epoch;
    const struct vsr_member *members;
    uint32_t count;
    uint32_t faults;
};

enum vsr_epoch_phase {
    VSR_EPOCH_STEADY,
    VSR_EPOCH_TRANSFERRING,
    VSR_EPOCH_INSTALLED
};

struct vsr_epoch {
    const struct vsr_membership *current;
    const struct vsr_membership *previous; /* NULL before any transition. */
    uint64_t boundary;           /* Reconfiguration op ending previous epoch. */
    uint32_t phase;
    uint32_t reserved;
};

enum vsr_state {
    VSR_STATE_STARTING,
    VSR_STATE_WARMING,
    VSR_STATE_RECOVERING,
    VSR_STATE_NORMAL,
    VSR_STATE_VIEW_CHANGE,
    VSR_STATE_TRANSITIONING,
    VSR_STATE_RETIRED,           /* Removal handoff completed; safe to stop. */
    VSR_STATE_FAILED,
    VSR_STATE_STOPPING,
    VSR_STATE_STOPPED
};

struct vsr_request_id {
    struct vsr_id client;        /* Stable, globally unique client incarnation. */
    uint64_t number;             /* Nonzero, increasing; retry with same bytes. */
};

enum vsr_request_type {
    VSR_REQUEST_COMMAND,        /* body: const struct vsr_blob * */
    VSR_REQUEST_RECONFIGURE,    /* body: const struct vsr_membership * */
    VSR_REQUEST_CHECK_EPOCH,     /* body: NULL; quorum-confirm requested epoch. */
    VSR_REQUEST_NOOP            /* Internal only; body: NULL, client ID zero. */
};

/*
 * One outstanding request per client incarnation, as in VR. Different clients
 * pipeline freely. Retransmissions are deduplicated and cached replies resent.
 * Client IDs must not be reused. Client-table eviction is a cache operation,
 * not permission to forget duplicate suppression in storage/checkpoints.
 * Encode nondeterministic inputs (time, random choices, etc.) in the command.
 */
struct vsr_request {
    struct vsr_request_id id;
    uint64_t epoch;
    uint32_t type;
    uint32_t reserved;
    const void *body;
};

/* A 64-byte log header on common 64-bit ABIs; large/cold bodies stay separate. */
struct vsr_entry {
    uint64_t op;
    uint64_t epoch;
    uint64_t view;
    struct vsr_request_id request;
    uint32_t type;               /* enum vsr_request_type */
    uint32_t reserved;
    const void *body;            /* Same type mapping as vsr_request. */
};

struct vsr_entries {
    const struct vsr_entry *entries;
    uint32_t count;
    uint32_t reserved;
};

struct vsr_value {
    struct vsr_blob data;
    int32_t code;               /* Deterministic application result, not I/O. */
    uint32_t reserved;
};

/* Only executed requests live here; pending dedup state comes from the log. */
struct vsr_client_record {
    struct vsr_request_id request;
    uint64_t op;
    struct vsr_value result;
};

enum vsr_reply_status {
    VSR_REPLY_OK,
    VSR_REPLY_NOT_PRIMARY,
    VSR_REPLY_NEW_EPOCH,
    VSR_REPLY_BUSY,
    VSR_REPLY_INVALID,
    VSR_REPLY_LIMIT,
    VSR_REPLY_STALE_REQUEST,
    VSR_REPLY_CLIENT_UNKNOWN,
    VSR_REPLY_CLIENT_STATE
};

enum vsr_reply_flags {
    VSR_REPLY_EXECUTED = 1u << 0 /* result is available for request.number. */
};

struct vsr_reply {
    struct vsr_request_id request;
    uint64_t op;
    uint64_t view;
    uint64_t primary;
    const struct vsr_membership *membership;
    struct vsr_value result;
    uint32_t status;
    uint32_t flags;
};

/* Unique across process restarts AND attempts within a process. */
struct vsr_nonce {
    struct vsr_id incarnation;   /* Supplied by caller at init, never reused. */
    uint64_t counter;
};

/* An immutable local storage revision; never confused with a log position. */
struct vsr_revision {
    struct vsr_id incarnation;
    uint64_t sequence;
};

/*
 * Names an immutable snapshot of application state AND the completed client
 * table through op, plus epoch/transition metadata. manifest is adapter-defined
 * metadata, not the snapshot contents. The adapter can use trees, extents,
 * content-addressed blocks, or other incremental/differential representations.
 */
struct vsr_checkpoint {
    struct vsr_id id;
    uint64_t op;
    uint64_t view;
    const struct vsr_epoch *epoch;
    struct vsr_blob manifest;
};

enum vsr_message_type {
    VSR_MSG_PREPARE,             /* number: last op; body: vsr_prepare */
    VSR_MSG_PREPARE_OK,          /* number: last prepared op; body: NULL */
    VSR_MSG_COMMIT,              /* number: commit op; body: NULL */
    VSR_MSG_START_VIEW_CHANGE,   /* number: 0; body: NULL */
    VSR_MSG_DO_VIEW_CHANGE,      /* number: last op; body: vsr_log_state */
    VSR_MSG_START_VIEW,          /* number: last op; body: vsr_log_state */
    VSR_MSG_RECOVERY,            /* number: 0; body: vsr_recovery */
    VSR_MSG_RECOVERY_RESPONSE,   /* number: last op or 0; body: vsr_recovery */
    VSR_MSG_GET_STATE,           /* number: 0; body: vsr_fetch */
    VSR_MSG_NEW_STATE,           /* number: last op; body: vsr_state_chunk */
    VSR_MSG_GET_LOG,             /* number: 0; body: vsr_fetch (chosen log) */
    VSR_MSG_LOG,                 /* number: last op; body: vsr_state_chunk */
    VSR_MSG_STATE_UNAVAILABLE,   /* number: last op; body: vsr_state_chunk */
    VSR_MSG_START_EPOCH,         /* number: boundary; body: vsr_epoch */
    VSR_MSG_EPOCH_STARTED,       /* number: boundary; body: NULL */
    VSR_MSG_NEW_EPOCH,           /* number: boundary; body: vsr_epoch */
    VSR_MSG_CHECKPOINT,          /* number: checkpoint op; body: vsr_checkpoint */
    VSR_MSG_READ_PROBE,          /* number: commit floor; body: vsr_nonce */
    VSR_MSG_READ_ACK             /* number: commit floor; body: vsr_nonce */
};

/* A 64-byte hot envelope on common 64-bit ABIs. flags currently must be zero. */
struct vsr_message {
    struct vsr_id cluster;
    uint64_t epoch;
    uint64_t view;
    uint64_t from;
    uint32_t type;
    uint32_t flags;
    uint64_t number;
    const void *body;
};

struct vsr_prepare {
    struct vsr_entries batch;
    uint64_t committed;
};

/*
 * A bounded suffix, not necessarily the entire log. last_normal_view is
 * distinct from message.view; it is needed for VR's view-change selection.
 * The receiver fetches missing ranges/checkpoints before using the chosen log.
 */
struct vsr_log_state {
    struct vsr_revision revision;
    uint64_t last_normal_view;
    uint64_t committed;
    uint64_t log_begin;
    const struct vsr_epoch *epoch;
    struct vsr_entries suffix;
    const struct vsr_checkpoint *checkpoint;
};

struct vsr_recovery {
    struct vsr_nonce nonce;
    const struct vsr_log_state *state; /* NULL in request/non-primary response. */
};

struct vsr_fetch {
    struct vsr_nonce nonce;
    struct vsr_revision revision; /* Zero for GET_STATE discovery. */
    uint64_t first;
    uint64_t end;
    uint64_t max_bytes;
    uint32_t max_entries;
    uint32_t reserved;
};

struct vsr_state_chunk {
    struct vsr_nonce nonce;
    struct vsr_log_state state;
};

/* Logical recovery metadata; no physical WAL/file format is implied. */
struct vsr_hard_state {
    uint64_t view;
    uint64_t last_normal_view;
    uint64_t committed;
    const struct vsr_epoch *epoch;
    uint32_t state;              /* Protocol state, including retirement. */
    uint32_t reserved;
};

struct vsr_recovered {
    struct vsr_id cluster;
    uint64_t replica;
    uint64_t sequence;           /* Last complete recovered store transaction. */
    uint64_t log_begin;
    uint64_t log_end;
    struct vsr_hard_state hard;
    const struct vsr_checkpoint *checkpoint;
    uint32_t durability;
    uint32_t reserved;
};

enum vsr_read_type {
    VSR_LOAD_RECOVERY,           /* One vsr_recovered; only at startup. */
    VSR_LOAD_LOG,                /* Contiguous vsr_entry array. */
    VSR_LOAD_CLIENT              /* Zero/one vsr_client_record. */
};

struct vsr_store_read {
    uint64_t sequence;           /* Read this exact logical revision. */
    uint64_t first;
    uint64_t end;
    struct vsr_id client;
    uint64_t max_bytes;
    uint32_t type;
    uint32_t max_count;
};

struct vsr_loaded {
    const void *items;           /* Type selected by the matching LOAD op. */
    uint64_t sequence;
    uint64_t next;               /* LOG continuation; end means complete. */
    uint32_t count;
    uint32_t reserved;
};

enum vsr_change_type {
    VSR_STORE_APPEND,            /* first/count/data: consecutive vsr_entry[]. */
    VSR_STORE_TRUNCATE,          /* Remove uncommitted suffix at first; no data. */
    VSR_STORE_CLIENTS,           /* count/data: completed vsr_client_record[]. */
    VSR_STORE_HARD_STATE,        /* count=1, data: vsr_hard_state. */
    VSR_STORE_PUBLISH_CHECKPOINT, /* count=1, data: vsr_checkpoint. */
    VSR_STORE_RESTORE_CHECKPOINT, /* count=1, data: vsr_checkpoint; reset base. */
    VSR_STORE_TRIM               /* Remove prefix < first; no data. */
};

struct vsr_change {
    uint32_t type;
    uint32_t count;
    uint64_t first;
    const void *data;
};

/*
 * A logical atomic transaction. Sequences increase by one, independently of
 * log op numbers. Apply changes in array order and transactions in sequence
 * order. Physical writes/completions may be batched, concurrent, or reordered.
 * Recovery must expose a complete prefix: no partial transaction or holes.
 * APPEND after TRUNCATE can replace a divergent suffix without rewriting a log.
 * Neither operation may remove/overwrite committed entries. PUBLISH changes
 * the recovery anchor, not the running application or current client table.
 * RESTORE resets the indexed log/client base; the same transaction includes
 * the appropriate hard state and any retained suffix. It is not application I/O.
 */
struct vsr_store {
    uint64_t sequence;
    const struct vsr_change *changes;
    uint32_t count;
    uint32_t reserved;
};

struct vsr_apply {
    struct vsr_entries batch;
    uint64_t through;
    uint32_t replay;             /* Rebuilding from checkpoint after restart. */
    uint32_t reserved;
};

struct vsr_applied {
    const struct vsr_value *results; /* One per entry, in the same order. */
    uint32_t count;
    uint32_t reserved;
};

enum vsr_read_consistency {
    VSR_READ_LINEARIZABLE,       /* Established view + fresh quorum confirmation. */
    VSR_READ_CAUSAL              /* May run on a backup; only min_op guaranteed. */
};

struct vsr_read_barrier {
    uint64_t min_op;
    uint32_t consistency;
    uint32_t reserved;
};

struct vsr_read_fence {
    uint64_t cookie;             /* READ event ID, not a replicated request ID. */
    uint64_t applied;
    uint64_t epoch;
    uint64_t view;
};

struct vsr_snapshot_task {
    uint64_t op;
    uint64_t sequence;           /* Matching indexed client-table revision. */
    uint64_t peer;               /* FETCH source; zero for local tasks. */
    const struct vsr_checkpoint *checkpoint;
    const struct vsr_checkpoint *basis; /* Optional differential-transfer base. */
};

enum vsr_event_type {
    VSR_EVENT_TIME,              /* id: monotonic nanoseconds; data: NULL. */
    VSR_EVENT_MESSAGE,           /* data: vsr_message; id: 0. */
    VSR_EVENT_REQUEST,           /* data: vsr_request; id: nonzero reply route. */
    VSR_EVENT_CLIENT_QUERY,      /* data: vsr_id; id: nonzero reply route. */
    VSR_EVENT_READ,              /* data: vsr_read_barrier; id: local cookie. */
    VSR_EVENT_CHECKPOINT,        /* Hint to take a checkpoint; id: 0, data: NULL. */
    VSR_EVENT_COMPLETE,          /* id: operation ID; status: vsr_io_status. */
    VSR_EVENT_STOP               /* Stop protocol work and drain; no payload. */
};

/*
 * 32-byte input descriptor on common 64-bit ABIs. status is zero except for
 * COMPLETE. A non-NULL data pointer requires a nonzero lease covering its ENTIRE
 * reachable object graph: typed records, arrays, spans, and span bytes.
 *
 * Accepting an event transfers a read-only pin, not ownership of allocation.
 * Nothing under the pin may change until VSR_OP_RELEASE returns that lease.
 * Lease IDs are caller-chosen and unique among outstanding pins. Separate
 * events sharing an allocation use distinct leases (caller-side refcounts).
 * The descriptor itself is only borrowed for the call. A rejected/unconsumed
 * event transfers nothing. NULL data requires lease=0. No implicit payload copy.
 */
struct vsr_event {
    uint32_t type;
    int32_t status;
    uint64_t id;
    const void *data;
    uint64_t lease;
};

enum vsr_op_type {
    VSR_OP_SEND,                 /* data: vsr_message; arg: destination member. */
    VSR_OP_REPLY,                /* data: vsr_reply; arg: local reply route. */
    VSR_OP_LOAD,                 /* data: vsr_store_read; arg: 0. */
    VSR_OP_STORE,                /* data: vsr_store; arg: 0. */
    VSR_OP_SYNC,                 /* data: NULL; arg: store sequence to flush. */
    VSR_OP_RECLAIM,              /* data: NULL; arg: oldest revision still needed. */
    VSR_OP_APPLY,                /* data: vsr_apply; arg: 0. */
    VSR_OP_READ_READY,           /* data: vsr_read_fence; arg: 0. */
    VSR_OP_SNAPSHOT_CAPTURE,     /* data: vsr_snapshot_task; arg: 0. */
    VSR_OP_SNAPSHOT_FETCH,       /* data: vsr_snapshot_task; arg: 0. */
    VSR_OP_SNAPSHOT_SYNC,        /* data: vsr_snapshot_task; arg: 0. */
    VSR_OP_SNAPSHOT_INSTALL,     /* data: vsr_snapshot_task; arg: 0. */
    VSR_OP_SNAPSHOT_DROP,        /* data: vsr_snapshot_task; arg: 0. */
    VSR_OP_RELEASE               /* data: NULL; arg: lease; id: 0; no completion. */
};

/*
 * 32-byte output descriptor on common 64-bit ABIs; flags currently zero.
 * Except RELEASE, each op has a nonzero ID never reused within the instance.
 * Its ENTIRE data graph stays immutable/pinned until COMPLETE is accepted.
 * Submit exactly one completion, including failure to submit or cancellation.
 * Finish accesses relying on this pin (including zero-copy-send notifications).
 * An adapter may first acquire its own references to externally owned payload
 * buffers; it must not retain borrowed core-arena records after completion.
 * IDs are opaque: use them directly as user_data or through an adapter table.
 *
 * SEND/REPLY: completion releases buffers, not a protocol acknowledgment.
 * LOAD: success data = vsr_loaded, with its own input lease.
 * STORE: success data = NULL; the complete transaction is now readable and
 *        no longer relies on this op's pin; NOT necessarily durable.
 * SYNC: success data = NULL; every transaction <= arg is crash-durable and
 *       recoverable as a prefix. May wait for earlier in-flight STOREs. Several
 *       SYNCs can share a flush; writes can be pipelined without per-entry fsync.
 * RECLAIM: success data = NULL; revisions < arg may be reclaimed once external
 *       readers drain. This never deletes entries reachable from a retained
 *       revision/checkpoint. STORE revisions remain readable until this hint;
 *       the core retains revisions needed by outstanding transfers/reads.
 * APPLY: success data = vsr_applied. Execute a consecutive committed batch in
 *        order; control entries are application no-ops with empty results.
 *        One APPLY at a time, concurrent with network/storage work. Completion
 *        is execution progress, not durable application progress. Replay MUST
 *        execute the suffix after the installed checkpoint, even if cached
 *        client results already exist; ordinary dedup must not skip rebuilding.
 * READ_READY: capture/read an application snapshot at fence.applied, then
 *        complete with NULL. The core holds APPLY/INSTALL until capture ends;
 *        expensive read evaluation may continue on that snapshot afterwards.
 *        Linearizable reads first require a current-view committed NOOP after
 *        the inherited log, then fresh quorum confirmation and applied progress.
 * CAPTURE: success data = vsr_checkpoint. Freeze application and indexed client
 *        state at (op, sequence); completion releases the application fence.
 *        Only capture is fenced: later copying/flushing can run concurrently.
 * FETCH: success data = vsr_checkpoint matching the requested logical snapshot,
 *        fully verified/readable locally. Bulk transfer bypasses the core;
 *        the adapter may stream pages/blocks or use differential transfer.
 * SNAPSHOT_SYNC: success means snapshot content/metadata are crash-durable.
 * INSTALL: success means the application is exactly at checkpoint.op.
 * DROP: releases the named local snapshot, after the adapter's own readers drain.
 *        The core emits DROP only when its recovery/transfer obligations permit.
 * All other successes and all failures have NULL data. Failure is reported as
 * an event, not a failed vsr_step call. No partial APPLY/STORE successes.
 */
struct vsr_op {
    uint32_t type;
    uint32_t flags;
    uint64_t id;
    const void *data;
    uint64_t arg;
};

enum vsr_update_flags {
    VSR_UPDATE_MORE = 1u << 0,       /* Runnable work: drain with no new events. */
    VSR_UPDATE_OUTPUT_FULL = 1u << 1, /* More output needs caller slots. */
    VSR_UPDATE_INPUT_BLOCKED = 1u << 2, /* Head event needs freed resources. */
    VSR_UPDATE_STATE_CHANGED = 1u << 3  /* Inspect vsr_get_status if interested. */
};

struct vsr_update {
    struct vsr_op *ops;           /* Caller-owned contiguous array. */
    uint32_t capacity;           /* Input; must be >= 1. */
    uint32_t count;              /* Output: populated operation descriptors. */
    uint32_t consumed;           /* Output: accepted prefix of input events. */
    uint32_t flags;
    uint64_t deadline_ns;        /* Replaces prior deadline; NO_DEADLINE cancels. */
};

enum vsr_start_mode {
    VSR_START_NEW,               /* Explicit new cluster, never lost-state reset. */
    VSR_START_RECOVER,           /* Load local state, then recover as required. */
    VSR_START_JOIN               /* Nonvoting warm-up using seed membership. */
};

/* Fixed arena capacities, not limits on total disk log/client-table size. */
struct vsr_limits {
    uint32_t members;            /* Per config; reserve old AND new membership. */
    uint32_t operations;         /* Outstanding effects, independent of batches. */
    uint32_t input_leases;
    uint32_t log_cache_entries;
    uint32_t client_cache_entries;
    uint32_t batch_entries;
    uint32_t spans_per_blob;
    uint32_t work_per_step;      /* Bounded internal transitions per call. */
    uint64_t command_bytes;
    uint64_t result_bytes;
    uint64_t message_bytes;
    uint64_t pinned_payload_bytes;
};

struct vsr_options {
    struct vsr_id cluster;
    struct vsr_id incarnation;   /* Fresh across starts, including storage loss. */
    uint64_t replica;
    const struct vsr_membership *seed;
    struct vsr_limits limits;
    uint64_t heartbeat_ns;
    uint64_t view_timeout_ns;    /* Greater than heartbeat_ns > 0. */
    uint64_t retry_ns;           /* Nonzero. */
    uint64_t batch_delay_ns;     /* 0: no intentional batching delay. */
    uint64_t checkpoint_interval; /* In operations; 0: explicit requests only. */
    uint32_t start_mode;
    uint32_t durability;
    uint32_t cache_line_bytes;   /* Power of two; 0 selects 64, not CPU detection. */
    uint32_t reserved;
};

struct vsr_layout {
    size_t size;
    size_t alignment;
};

struct vsr_status {
    uint64_t epoch;
    uint64_t view;
    uint64_t primary;
    uint64_t committed;
    uint64_t applied;
    uint64_t stored_sequence;
    uint64_t durable_sequence;
    uint64_t checkpoint_op;
    uint64_t transition_op;
    uint32_t state;
    uint32_t role;
    uint32_t outstanding_ops;
    uint32_t outstanding_leases;
    const struct vsr_epoch *configuration; /* Borrowed until next step. */
};

/*
 * Compute the arena requirement, including progress reserves, from the limits.
 * No allocation. Sizes are checked for overflow. Configurations exceeding
 * limits are rejected before admission; exhaustion never drops accepted work.
 */
int vsr_layout(const struct vsr_options *options, struct vsr_layout *layout);

/*
 * Initialize in caller-provided aligned memory; copies options/seed metadata.
 * No retained pointers into options. Memory must remain fixed until deinit.
 * There is no malloc/free on any path; payloads live in caller-managed pools.
 * A zero-event step starts boot operations; TIME establishes the clock origin.
 * All participants use the same durability policy, immutable across epochs.
 * NEW requires an empty store and a common epoch-0 seed configuration. RECOVER
 * loads metadata/log lazily; missing/untrusted state requires quorum recovery,
 * never bootstrap. JOIN cannot vote or execute as primary before admission.
 */
int vsr_init(void *memory, size_t size, const struct vsr_options *options,
             struct vsr **out);

/*
 * Single-event convenience; event=NULL drains ready work. No sleeping/waiting.
 * TIME IDs must be nondecreasing, < VSR_NO_DEADLINE, and share one monotonic
 * clock domain. Early/stale timer wakeups merely recheck current deadlines.
 */
int vsr_step(struct vsr *v, const struct vsr_event *event,
             struct vsr_update *update);

/*
 * Process a prefix in input order, coalescing prepares, stores, and replies.
 * count=0 permits events=NULL. Input/output arrays must not alias each other or
 * live pinned memory. An event is either wholly accepted or not consumed.
 * update is ALWAYS usable on return, even if a later event is invalid: earlier
 * accepted events and emitted operations are not rolled back. A negative result
 * identifies the event at consumed (or invalid call arguments); VSR_AGAIN means
 * budget/capacity stopped progress, not failure. Never resubmit the consumed
 * prefix. Service output before reusing its array; retained op bodies stay valid
 * until completion even after the array is overwritten.
 *
 * On MORE/OUTPUT_FULL, drain with count=0. On INPUT_BLOCKED without MORE, service
 * completions and deadlines before retrying the unconsumed input. Reorder the
 * caller's pending queue if a blocked proposal precedes needed completions.
 * Admission reserves completion/lease-release capacity; COMPLETE, STOP, and
 * zero-input draining must not depend on new proposal/cache admission capacity.
 * Old/duplicate protocol messages are consumed and ignored where appropriate;
 * unknown/duplicate operation completions are API errors. A request's acceptance
 * is not a promise of commitment: clients retry an uncertain outcome with the
 * same identity. Membership changes/redirects/rejections are ordinary replies.
 */
int vsr_step_many(struct vsr *v, const struct vsr_event *events, uint32_t count,
                  struct vsr_update *update);

/* Cold-path observation; status copies are not imposed on each event. */
void vsr_get_status(const struct vsr *v, struct vsr_status *status);

/*
 * After STOP, drain/cancel external operations and consume all RELEASE outputs.
 * Deinit succeeds only in STOPPED with no outstanding ops/leases; otherwise
 * EBUSY. It does not free caller memory. STOP is not a guarantee that pending
 * requests committed. RETIRED reports safe membership removal; merely learning
 * a new configuration is not permission to shut down an old state-transfer donor.
 */
int vsr_deinit(struct vsr *v);

#ifdef __cplusplus
}
#endif

#endif /* VSR_H */
