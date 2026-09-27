#ifndef VSR_H
#define VSR_H

#include <stddef.h>
#include <stdint.h>

/*
 * VSR: deterministic, asynchronous Viewstamped Replication for C11.
 *
 * One owner thread per instance; no overlapping/reentrant calls, callbacks,
 * I/O, clock reads, randomness, or hidden allocation. All external work is
 * represented by operations and completed by events. See DESIGN.md for protocol
 * invariants and docs/vsr-api.md for adapter contracts.
 *
 * These are naturally aligned, host-side logical views, NOT wire/disk structs.
 * The adapter owns framing, encoding, checksums, authentication, compression,
 * addresses, file/block layout, buffering, and physical I/O scheduling.
 * Integer enum fields accept only the listed values; unknown flags and reserved
 * fields must be zero. Unused fields are zero/NULL. Arrays use NULL iff empty;
 * all pointers must be valid and naturally aligned for their declared types.
 * Counts/ranges must fit configured limits. Input objects must not overlap the
 * arena. Object graphs must be acyclic. Object equality is fieldwise, never
 * memcmp over padding or pointers. See docs/protocol.md for message validation.
 * All operation-number ranges are [first, end); operation numbers start at 1
 * and do not reset across epochs. Epochs start at 0; views restart at 0 in each
 * epoch. Only committed positions are permanent; an uncommitted suffix can be
 * replaced. UINT64_MAX is reserved for exclusive range ends and disabled
 * deadlines, never a numeric counter (including request/nonce numbers) or time.
 * Opaque replica/client/lease/route IDs may use all their nonzero bits. Counters
 * never wrap. Zero op means no progress.
 */
struct vsr;

#define VSR_API_VERSION 1u /* Source API, independent of adapter formats. */
#define VSR_NO_REPLICA UINT64_C(0)
#define VSR_NO_DEADLINE UINT64_MAX
#define VSR_MAX_STORE_CHANGES 8u

/* Equality compares both words. The all-zero ID is reserved for absence. */
struct vsr_id {
    uint64_t hi;
    uint64_t lo;
};

struct vsr_span {
    const void *data;
    size_t size;
};

/*
 * Scatter/gather bytes; no flattening required. Every span has nonzero size and
 * non-NULL data. size is the overflow-checked sum; empty = { NULL, 0, 0, 0 }.
 * Span boundaries do not affect logical equality or encoded payload contents.
 */
struct vsr_blob {
    const struct vsr_span *spans;
    uint64_t size; /* Sum of span sizes, checked on admission. */
    uint32_t count;
    uint32_t reserved;
};

enum vsr_result {
    VSR_OK = 0,
    VSR_AGAIN = 1,   /* Budget/backpressure; inspect update flags. */
    VSR_EINVAL = -1, /* Invalid argument, shape, or completion. */
    VSR_ELIMIT = -2, /* Permanent size/capacity/overflow violation. */
    VSR_EBUSY = -3   /* Deinit before STOPPED. */
};

enum vsr_io_status {
    VSR_IO_OK = 0,
    VSR_IO_RETRY,     /* Transient unavailability, no success implied. */
    VSR_IO_NOT_FOUND, /* Absent store/range/snapshot; see LOAD rules. */
    VSR_IO_CORRUPT,   /* Integrity or logical consistency failure. */
    VSR_IO_FAILED,    /* Permanent or ambiguous failure. */
    VSR_IO_CANCELLED  /* Accesses ended; no success implied. */
};

enum vsr_durability {
    VSR_DURABLE,   /* Persist safety state before relying on it. */
    VSR_REPLICATED /* VR recovery quorum required after restart. */
};

enum vsr_member_role {
    VSR_MEMBER_NONE,   /* Status only; invalid in a membership list. */
    VSR_MEMBER_FULL,   /* Stores/applies state; eligible as primary. */
    VSR_MEMBER_WITNESS /* Votes/stores log; never primary or executor. */
};

struct vsr_member {
    uint64_t id;   /* Stable, nonzero; independent of array index. */
    uint32_t role; /* enum vsr_member_role */
    uint32_t reserved;
};

/*
 * Members are sorted by ID, with no duplicates. count >= 2*faults + 1;
 * quorum = count - faults; at least faults + 1 members must be FULL.
 * A one-member, zero-fault group is valid and has no failure tolerance.
 * Primary = FULL members[view % full_count], in increasing ID order.
 * Warming nodes are not voters and do not appear here until reconfiguration.
 */
struct vsr_membership {
    uint64_t epoch;
    const struct vsr_member *members;
    uint32_t count;
    uint32_t faults;
};

enum vsr_epoch_phase {
    VSR_EPOCH_STEADY,       /* Initial epoch or completed group handoff. */
    VSR_EPOCH_TRANSFERRING, /* Boundary committed; local catch-up pending. */
    VSR_EPOCH_INSTALLED     /* Local catch-up done; group handoff pending. */
};

/*
 * Epoch 0 is STEADY with previous=NULL and boundary=0. Later epochs retain the
 * immediate predecessor (current.epoch-1) and a nonzero boundary through STEADY.
 * Peer/snapshot phases describe their owner, not the receiver's readiness.
 */
struct vsr_epoch {
    const struct vsr_membership
        *current; /* Non-NULL, new group in a transition. */
    const struct vsr_membership
        *previous;     /* Immediate predecessor; NULL at 0. */
    uint64_t boundary; /* Reconfiguration ending previous; 0 at epoch 0. */
    uint32_t phase;    /* enum vsr_epoch_phase; local progress only. */
    uint32_t reserved;
};

enum vsr_state {
    VSR_STATE_STARTING,
    VSR_STATE_WARMING,
    VSR_STATE_RECOVERING,
    VSR_STATE_NORMAL,
    VSR_STATE_VIEW_CHANGE,
    VSR_STATE_TRANSITIONING,
    VSR_STATE_RETIRED, /* Removal handoff completed; safe to stop. */
    VSR_STATE_FAILED,
    VSR_STATE_STOPPING,
    VSR_STATE_STOPPED
};

struct vsr_request_id {
    struct vsr_id client; /* Stable, globally unique client incarnation. */
    uint64_t number;      /* Nonzero, increasing; retry with same bytes. */
};

enum vsr_request_type {
    VSR_REQUEST_COMMAND,     /* body: vsr_blob, including an empty command. */
    VSR_REQUEST_RECONFIGURE, /* body: membership, epoch = request.epoch+1. */
    VSR_REQUEST_CHECK_EPOCH, /* body: vsr_check_epoch; logged handoff fence. */
    VSR_REQUEST_NOOP         /* Internal only; body: NULL, entire ID zero. */
};

/*
 * Immutable target; request.epoch is only a routing precondition on retries.
 * Target <= routing epoch; zero is trivially ready. No successful execution or
 * completed client record precedes target handoff readiness, including retries.
 */
struct vsr_check_epoch {
    uint64_t epoch; /* Success certifies handoff into this epoch. */
};

/*
 * One outstanding request per client incarnation, as in VR. Different clients
 * pipeline freely. Retransmissions are deduplicated and cached replies resent.
 * IDs/type/body must agree on retry; epoch is a routing precondition and may
 * follow redirects. Clients durably retain sequence/pending command or use a
 * new incarnation after losing them. A local CLIENT_QUERY is advisory only.
 * A newer request cannot overtake a known pending request from that client.
 * Reusing an ID with different contents violates this contract; after trimming,
 * completed records do not retain bodies for indefinite conflict detection.
 * Client IDs must not be reused. Client-table eviction is a cache operation,
 * not permission to forget duplicate suppression in storage/checkpoints.
 * Encode nondeterministic inputs (time, random choices, etc.) in the command.
 */
struct vsr_request {
    struct vsr_request_id id;
    uint64_t epoch; /* Routing epoch; not part of request identity. */
    uint32_t type;
    uint32_t reserved;
    const void *body;
};

/* A 64-byte log header on common 64-bit ABIs; large/cold bodies stay separate. */
struct vsr_entry {
    uint64_t op;
    uint64_t epoch;
    uint64_t view; /* Original proposal view; not rewritten on fetch. */
    struct vsr_request_id request;
    uint32_t type; /* enum vsr_request_type */
    uint32_t reserved;
    const void *body; /* Same type mapping as vsr_request. */
};

struct vsr_entries {
    const struct vsr_entry *entries;
    uint32_t count;
    uint32_t reserved;
};

struct vsr_value {
    struct vsr_blob data;
    int32_t code; /* Deterministic application result, not I/O. */
    uint32_t reserved;
};

/*
 * Latest executed request per client; op > 0. Retain this record through cache
 * eviction, checkpointing, and reconfiguration. Older results may be discarded.
 * Pending requests are indexed separately by LOAD_REQUEST over retained log.
 */
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
    VSR_REPLY_CLIENT_STATE,
    VSR_REPLY_TIMEOUT /* Local read deadline expired before readiness. */
};

enum vsr_reply_flags {
    VSR_REPLY_EXECUTED = 1u << 0 /* result is available for request.number. */
};

struct vsr_reply {
    struct vsr_request_id
        request; /* Echo; CLIENT_QUERY instead reports latest ID. */
    uint64_t op; /* Known request position; 0 if absent/rejected. */
    uint64_t view;
    uint64_t primary;
    const struct vsr_membership
        *membership; /* Latest known group, else NULL. */
    struct vsr_value
        result; /* Empty unless EXECUTED; controls return code 0. */
    uint32_t status;
    uint32_t flags;
};

/* Unique across starts and attempts. counter > 0; incarnation is cluster-unique. */
struct vsr_nonce {
    struct vsr_id incarnation; /* Supplied by caller at init, never reused. */
    uint64_t counter;
};

/* An immutable local storage revision; never confused with a log position. */
struct vsr_revision {
    struct vsr_id incarnation;
    uint64_t sequence;
};

/*
 * Nonzero id names an immutable snapshot of application state AND the completed
 * client table through op, plus epoch/transition metadata. manifest contains
 * adapter-defined metadata, not snapshot contents. Snapshot IDs are cluster-unique
 * and never reused for different contents. The adapter can use trees, extents,
 * content-addressed blocks, or other incremental/differential representations.
 */
struct vsr_checkpoint {
    struct vsr_id id;
    uint64_t op;
    uint64_t view; /* Proposal view of entry at op; zero at genesis. */
    const struct vsr_epoch *epoch;
    struct vsr_blob manifest;
};

enum vsr_message_type {
    VSR_MSG_PREPARE,           /* number: last op; body: vsr_prepare */
    VSR_MSG_PREPARE_OK,        /* number: last prepared op; body: NULL */
    VSR_MSG_COMMIT,            /* number: commit op; body: NULL */
    VSR_MSG_START_VIEW_CHANGE, /* number: 0; body: NULL */
    VSR_MSG_DO_VIEW_CHANGE,    /* number: last op; body: vsr_log_state */
    VSR_MSG_START_VIEW,        /* number: last op; body: vsr_log_state */
    VSR_MSG_RECOVERY,          /* number: 0; body: vsr_recovery */
    VSR_MSG_RECOVERY_RESPONSE, /* number: last op or 0; body: vsr_recovery */
    VSR_MSG_GET_STATE, /* number: 0; body: vsr_fetch (discovery/range) */
    VSR_MSG_NEW_STATE, /* number: last op; body: vsr_state_chunk */
    VSR_MSG_GET_LOG,   /* number: 0; body: vsr_fetch (chosen log) */
    VSR_MSG_LOG,       /* number: last op; body: vsr_state_chunk */
    VSR_MSG_STATE_UNAVAILABLE, /* number: last op; body: vsr_state_chunk */
    VSR_MSG_START_EPOCH,       /* number: boundary; body: vsr_epoch */
    VSR_MSG_EPOCH_STARTED, /* number: boundary; body: NULL; retention promise */
    VSR_MSG_NEW_EPOCH,     /* number: boundary; body: vsr_epoch */
    VSR_MSG_CHECKPOINT,    /* number: retained op; body: vsr_checkpoint */
    VSR_MSG_READ_PROBE,    /* number: commit floor; body: vsr_nonce */
    VSR_MSG_READ_ACK       /* number: commit floor; body: vsr_nonce */
};

/* A 64-byte hot envelope on common 64-bit ABIs. flags must be zero. */
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
 * An immutable log offer. [log_begin, log_end) is the retained range; entries
 * is any bounded contiguous portion, possibly empty. committed < log_end and
 * log_begin <= committed+1. Missing prefixes require the offered checkpoint.
 * view/last_normal_view describe this revision, independently of message.view.
 * Selection uses (last_normal_view, log_end), never the size of the included
 * portion. Fetch and validate missing data before adopting a chosen log.
 * checkpoint, if present, covers [1, checkpoint.op], with checkpoint.op <=
 * committed and log_begin <= checkpoint.op+1 <= log_end. log_begin > 1
 * requires a checkpoint. At genesis log_begin=log_end=1, committed=0.
 * A witness's checkpoint is a remote anchor, not evidence of local ownership.
 */
struct vsr_log_state {
    struct vsr_revision revision;
    uint64_t view;
    uint64_t last_normal_view;
    uint64_t committed;
    uint64_t log_begin;
    uint64_t log_end;
    const struct vsr_epoch *epoch;
    struct vsr_entries entries;
    const struct vsr_checkpoint *checkpoint;
};

struct vsr_recovery {
    struct vsr_nonce nonce;
    const struct vsr_log_state
        *state; /* NULL in request/non-primary response. */
};

struct vsr_fetch {
    struct vsr_nonce nonce;
    struct vsr_revision revision; /* Zero for GET_STATE discovery. */
    uint64_t first;     /* Discovery: first=end=0; otherwise first>=1. */
    uint64_t end;       /* Exact requested range in the named revision. */
    uint64_t max_bytes; /* >= command_bytes+manifest_bytes; <= message_bytes. */
    uint32_t max_entries; /* Positive, <= batch_entries. */
    uint32_t reserved;
};

struct vsr_state_chunk {
    struct vsr_nonce nonce;
    struct vsr_log_state state;
    uint64_t first; /* Echo fetch.first; zero for discovery. */
    uint64_t next;  /* Next missing op; fetch.end if done; discovery=0. */
};

/* Only these protocol states may be persisted; runtime stop/failure is local. */
enum vsr_hard_state_type {
    VSR_HARD_NORMAL,
    VSR_HARD_VIEW_CHANGE,
    VSR_HARD_RECOVERING,
    VSR_HARD_TRANSITIONING,
    VSR_HARD_RETIRED
};

/* Logical recovery metadata; no physical WAL/file format is implied. */
struct vsr_hard_state {
    uint64_t view;
    uint64_t last_normal_view; /* <= view; equal in HARD_NORMAL. */
    uint64_t committed;
    const struct vsr_epoch *epoch;
    uint32_t state; /* enum vsr_hard_state_type */
    uint32_t role;  /* Local materialized/warming role; survives restart. */
};

/* Immutable store identity, written once in transaction 1 of an empty store. */
struct vsr_store_identity {
    struct vsr_id cluster;
    uint64_t replica;
    uint32_t durability;
    uint32_t reserved;
};

struct vsr_recovered {
    struct vsr_store_identity identity;
    uint64_t sequence; /* Last complete recovered store transaction. */
    uint64_t log_begin;
    uint64_t log_end;
    struct vsr_hard_state hard;
    const struct vsr_checkpoint *checkpoint;
};

enum vsr_load_type {
    VSR_LOAD_RECOVERY, /* One vsr_recovered; only at startup. */
    VSR_LOAD_LOG,      /* Contiguous vsr_entry array. */
    VSR_LOAD_CLIENT,   /* Zero/one latest completed vsr_client_record. */
    VSR_LOAD_REQUEST   /* Zero/one latest retained vsr_entry for client. */
};

struct vsr_store_read {
    uint64_t sequence;    /* Read this exact logical revision. */
    uint64_t first;       /* LOG only; other read types use zero. */
    uint64_t end;         /* LOG only. */
    struct vsr_id client; /* CLIENT/REQUEST only. */
    uint64_t max_bytes;   /* Sum of blob payload bytes returned. */
    uint32_t type;        /* enum vsr_load_type */
    uint32_t max_count;   /* LOG: <= batch_entries; otherwise 1. */
};

struct vsr_loaded {
    const void *items; /* Type selected by the matching LOAD op. */
    uint64_t sequence; /* Exact read revision; recovered one at startup. */
    uint64_t next;     /* LOG continuation; end means complete; else 0. */
    uint32_t count;
    uint32_t reserved;
};

enum vsr_change_type {
    VSR_STORE_APPEND,     /* first/count/data: consecutive vsr_entry[]. */
    VSR_STORE_TRUNCATE,   /* Remove uncommitted suffix at first; no data. */
    VSR_STORE_CLIENTS,    /* count/data: completed vsr_client_record[]. */
    VSR_STORE_HARD_STATE, /* count=1, data: vsr_hard_state. */
    VSR_STORE_PUBLISH_CHECKPOINT, /* count=1, data: vsr_checkpoint. */
    VSR_STORE_RESTORE_CHECKPOINT, /* count=1, data: vsr_checkpoint; reset base. */
    VSR_STORE_TRIM,               /* Remove prefix < first; no data. */
    VSR_STORE_IDENTITY /* count=1, data: vsr_store_identity; only once. */
};

struct vsr_change {
    uint32_t type;
    uint32_t count;
    uint64_t first;
    const void *data;
};

/*
 * A logical atomic transaction, with 1..VSR_MAX_STORE_CHANGES changes.
 * Sequences start at 1 and increase by one, independently of
 * log op numbers. Apply changes in array order and transactions in sequence
 * order. Physical writes/completions may be batched, concurrent, or reordered.
 * Recovery must expose a complete prefix: no partial transaction or holes.
 * APPEND after TRUNCATE can replace a divergent suffix without rewriting a log.
 * Neither operation may remove/overwrite committed entries. PUBLISH changes
 * the recovery anchor, not the running application or current client table.
 * APPEND/TRUNCATE/TRIM also update the per-client retained-request index.
 * CLIENTS updates latest completed results monotonically by request number;
 * replay of older completed requests must not move that index backwards.
 * RESTORE replaces the checkpoint/client base and removes entries <= its op;
 * entries above it remain by reference. The core first validates this retained
 * suffix against the selected history and truncates any divergent uncommitted
 * tail. The same transaction includes HARD_STATE; retained entries need not be
 * resubmitted. Missing suffixes are appended in bounded later transactions.
 * RESTORE is not application I/O and cannot discard a known committed suffix.
 */
struct vsr_store {
    uint64_t sequence;
    const struct vsr_change *changes;
    uint32_t count;
    uint32_t reserved;
};

struct vsr_apply {
    struct vsr_entries batch;
    uint64_t through; /* Last entry op; nonempty batch, consecutive. */
    uint32_t replay;  /* 0/1: rebuilding application state from a base. */
    uint32_t reserved;
};

struct vsr_applied {
    const struct vsr_value *results; /* One per entry, in the same order. */
    uint32_t count;
    uint32_t reserved;
};

enum vsr_read_consistency {
    VSR_READ_LINEARIZABLE, /* Established view + fresh quorum confirmation. */
    VSR_READ_CAUSAL        /* Full backup allowed; caller propagates min_op. */
};

struct vsr_read_barrier {
    uint64_t min_op;      /* Committed position in this cluster, or zero. */
    uint64_t deadline_ns; /* Absolute TIME domain; NO_DEADLINE disables. */
    uint32_t consistency;
    uint32_t reserved;
};

struct vsr_read_fence {
    uint64_t cookie; /* READ event ID, not a replicated request ID. */
    uint64_t applied;
    uint64_t epoch;
    uint64_t view;
};

struct vsr_snapshot_task {
    uint64_t op;       /* checkpoint.op, or 0 at genesis. */
    uint64_t sequence; /* CAPTURE: client revision; INSTALL: base; else 0. */
    uint64_t peer;     /* FETCH source; zero for local tasks. */
    const struct vsr_checkpoint
        *checkpoint; /* Template on CAPTURE; NULL at genesis. */
    const struct vsr_checkpoint
        *basis; /* FETCH only: optional differential base. */
};

enum vsr_event_type {
    VSR_EVENT_TIME,         /* id: monotonic nanoseconds; data: NULL. */
    VSR_EVENT_MESSAGE,      /* data: vsr_message; id: 0. */
    VSR_EVENT_REQUEST,      /* data: vsr_request; id: nonzero reply route. */
    VSR_EVENT_CLIENT_QUERY, /* data: nonzero vsr_id; id: local reply route. */
    VSR_EVENT_READ,         /* data: vsr_read_barrier; id: local cookie. */
    VSR_EVENT_CHECKPOINT,   /* Coalescible local hint; id: 0, data: NULL. */
    VSR_EVENT_COMPLETE,     /* id: operation ID; status: vsr_io_status. */
    VSR_EVENT_STOP          /* Stop protocol work and drain; id=0, data=NULL. */
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
    VSR_OP_SEND,       /* data: vsr_message; arg: destination member. */
    VSR_OP_REPLY,      /* data: vsr_reply; arg: local reply route. */
    VSR_OP_LOAD,       /* data: vsr_store_read; arg: 0. */
    VSR_OP_STORE,      /* data: vsr_store; arg: 0. */
    VSR_OP_SYNC,       /* data: NULL; arg: store sequence to flush. */
    VSR_OP_RECLAIM,    /* data: NULL; arg: oldest revision still needed. */
    VSR_OP_APPLY,      /* data: vsr_apply; arg: 0. */
    VSR_OP_READ_READY, /* data: vsr_read_fence; arg: 0. */
    VSR_OP_SNAPSHOT_CAPTURE, /* data: vsr_snapshot_task; arg: 0. */
    VSR_OP_SNAPSHOT_FETCH,   /* data: vsr_snapshot_task; arg: 0. */
    VSR_OP_SNAPSHOT_SYNC,    /* data: vsr_snapshot_task; arg: 0. */
    VSR_OP_SNAPSHOT_INSTALL, /* data: vsr_snapshot_task; arg: 0. */
    VSR_OP_SNAPSHOT_DROP,    /* data: vsr_snapshot_task; arg: 0. */
    VSR_OP_RELEASE           /* data: NULL; arg: lease; id: 0; no completion. */
};

/*
 * 32-byte output descriptor on common 64-bit ABIs; flags must be zero.
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
 *        Linearizable reads first require a current-view committed entry after
 *        the inherited log (an internal NOOP if necessary), then fresh quorum
 *        confirmation and applied progress.
 *        Fences are serialized with each other, CAPTURE, APPLY, and INSTALL.
 * CAPTURE: success data = vsr_checkpoint. Freeze application and indexed client
 *        state at (op, sequence); completion releases the application fence.
 *        task.checkpoint supplies op/view/epoch with zero id/manifest; return
 *        those same logical fields with a fresh id and local manifest.
 *        Only capture is fenced: later copying/flushing can run concurrently.
 * FETCH: success data = vsr_checkpoint matching the requested logical snapshot,
 *        fully verified/readable locally. Bulk transfer bypasses the core;
 *        the adapter may stream pages/blocks or use differential transfer.
 * SNAPSHOT_SYNC: success means snapshot content/metadata are crash-durable.
 * INSTALL: success means the application is exactly at checkpoint.op. A NULL
 *        checkpoint with op=sequence=0 resets to the agreed genesis state;
 *        this is also emitted when starting/recovering without a checkpoint.
 * DROP: releases the named local snapshot, after the adapter's own readers drain.
 *        The core emits DROP only when its recovery/transfer obligations permit.
 * On witnesses, PUBLISH may record a remote recovery anchor without a local
 * snapshot; no SYNC/INSTALL/DROP snapshot operation is issued for that anchor.
 * Such publication/trimming requires retained full-member checkpoint coverage.
 * RECOVERY on a materialized full replica also acquires a local hold on its
 * checkpoint. Local holds are keyed by snapshot ID, not operation ID. Repeated
 * FETCH of an already held ID does not add a hold; one DROP ends that hold.
 * Failed CAPTURE/FETCH must clean up private partial objects in the adapter.
 * All other successes and all failures have NULL data. Failure is reported as
 * an event, not a failed vsr_step call. No partial APPLY/STORE successes.
 * Every non-OK STORE/SYNC/APPLY/INSTALL/SNAPSHOT_SYNC completion fences the
 * replica, even RETRY/CANCELLED. SEND failures retry protocol work; REPLY/READ
 * delivery failures abandon that local route. LOAD corruption/unexpected
 * absence is fatal; FETCH unavailability restarts discovery. CAPTURE/cleanup
 * failures may retry. CORRUPT from LOAD/RECLAIM fences storage; from any
 * snapshot operation it latches VSR_FAILURE_SNAPSHOT (including INSTALL).
 * STOPPING consumes completions without starting retries.
 */
struct vsr_op {
    uint32_t type;
    uint32_t flags;
    uint64_t id;
    const void *data;
    uint64_t arg;
};

enum vsr_update_flags {
    VSR_UPDATE_MORE = 1u << 0, /* Runnable work: drain with no new events. */
    VSR_UPDATE_OUTPUT_FULL = 1u
                             << 1, /* More output needs slots; implies MORE. */
    VSR_UPDATE_INPUT_BLOCKED = 1u << 2, /* Head event needs freed resources. */
    VSR_UPDATE_STATE_CHANGED =
        1u << 3 /* State/role/epoch/phase/view/failure changed. */
};

struct vsr_update {
    struct vsr_op *ops; /* Caller-owned contiguous array. */
    uint32_t capacity;  /* Input; must be >= 1. */
    uint32_t count;     /* Output: populated operation descriptors. */
    uint32_t consumed;  /* Output: accepted prefix of input events. */
    uint32_t flags;
    uint64_t deadline_ns; /* Replaces prior deadline; NO_DEADLINE cancels. */
};

enum vsr_start_mode {
    VSR_START_NEW,     /* Explicit new cluster, never lost-state reset. */
    VSR_START_RECOVER, /* Load local state, then recover as required. */
    VSR_START_JOIN     /* Nonvoting warm-up using seed membership. */
};

/*
 * Positive fixed capacities, independent of total indexed history. Byte limits
 * count blob.size values (not host headers, span arrays, codecs, or backing
 * allocations), once per occurrence in an input graph. Across leases, shared
 * bytes count again; outgoing references to the same lease do not count again.
 * Completion reservations count against these budgets before work is emitted.
 * Admission also preserves capacity for dependent work needed to release input;
 * accepted requests must not pin the resources needed to store/apply them.
 * batch_entries <= log_cache_entries; message_bytes >= command_bytes +
 * manifest_bytes (checked for overflow), so a transfer can include both.
 * Configuration, batch, span, and per-object byte limits must be compatible
 * across all replicas; cache/queue capacities may differ. No size negotiation.
 */
struct vsr_limits {
    uint32_t members;    /* Per config; reserve old AND new membership. */
    uint32_t operations; /* Outstanding effects, independent of batches. */
    uint32_t input_leases;
    uint32_t pending_requests; /* Local request/query reply routes. */
    uint32_t pending_reads; /* Waiting read barriers and active read fences. */
    uint32_t transfers;     /* Concurrent incoming/outgoing log offers. */
    uint32_t log_cache_entries;
    uint32_t client_cache_entries;
    uint32_t batch_entries;
    uint32_t spans_per_blob;
    uint32_t work_per_step; /* Bounded internal transitions per call. */
    uint64_t command_bytes;
    uint64_t result_bytes;
    uint64_t manifest_bytes; /* Per checkpoint descriptor, not snapshot data. */
    uint64_t message_bytes;  /* Aggregate blob bytes per message/log load. */
    uint64_t
        pinned_payload_bytes; /* Includes reserved LOAD/APPLY/snapshot results. */
};

struct vsr_options {
    struct vsr_id cluster;
    struct vsr_id
        incarnation; /* Fresh across starts, including storage loss. */
    uint64_t replica;
    const struct vsr_membership
        *seed; /* Required: genesis group or discovery hint. */
    struct vsr_limits limits;
    uint64_t heartbeat_ns;
    uint64_t view_timeout_ns;     /* Greater than heartbeat_ns > 0. */
    uint64_t retry_ns;            /* Nonzero. */
    uint64_t transfer_timeout_ns; /* Nonzero; idle log-offer retention. */
    uint64_t batch_delay_ns;      /* 0: no intentional batching delay. */
    uint64_t
        checkpoint_interval; /* In operations; 0: explicit requests only. */
    uint32_t start_mode;     /* enum vsr_start_mode */
    uint32_t durability;     /* enum vsr_durability */
    uint32_t join_role;      /* JOIN: FULL/WITNESS; otherwise MEMBER_NONE. */
    uint32_t
        cache_line_bytes; /* Power of two; 0 selects 64, not CPU detection. */
    uint32_t reserved;
};

struct vsr_layout {
    size_t size;
    size_t alignment;
};

enum vsr_failure_code {
    VSR_FAILURE_NONE,
    VSR_FAILURE_IDENTITY, /* Cluster/replica/policy mismatch or NEW reused. */
    VSR_FAILURE_STORAGE,  /* Unsafe/unreadable logical storage. */
    VSR_FAILURE_APPLICATION, /* Failed apply/install or invalid app output. */
    VSR_FAILURE_SNAPSHOT,    /* Snapshot durability/integrity failure. */
    VSR_FAILURE_EXHAUSTED,   /* Counter/time domain exhausted; never wrap. */
    VSR_FAILURE_INVARIANT    /* Internally inconsistent protocol state. */
};

/* First fatal failure is latched through STOPPED; zero fields without an op. */
struct vsr_failure {
    uint64_t operation;
    uint32_t code;           /* enum vsr_failure_code */
    uint32_t operation_type; /* enum vsr_op_type, valid if operation != 0. */
    int32_t status;          /* enum vsr_io_status, or OK for core detection. */
    uint32_t reserved;
};

struct vsr_status {
    uint64_t epoch;
    uint64_t view;
    uint64_t primary;
    uint64_t committed;
    uint64_t applied; /* 0 for witnesses. */
    uint64_t
        stored_sequence; /* Contiguous successfully completed STORE prefix. */
    uint64_t durable_sequence; /* Completed SYNC prefix, <= stored_sequence. */
    uint64_t checkpoint_op; /* Published recovery anchor (remote on witness). */
    uint64_t transition_op;
    uint32_t state; /* enum vsr_state */
    uint32_t role;  /* Materialized role; membership governs voting. */
    uint32_t outstanding_ops;
    uint32_t outstanding_leases;
    const struct vsr_epoch
        *configuration; /* NULL until known; borrowed to next step. */
    struct vsr_failure failure;
};

/*
 * Compute the arena requirement, including progress reserves, from the limits.
 * No allocation. Sizes are checked for overflow. Configurations exceeding
 * limits are rejected before admission; exhaustion never drops accepted work.
 * Returns OK, EINVAL, or ELIMIT. On error a valid layout is set to {0, 0}.
 * NULL arguments return EINVAL; inaccessible non-NULL pointers are caller errors.
 */
int vsr_layout(const struct vsr_options *options, struct vsr_layout *layout);

/*
 * Initialize in caller-provided aligned memory; copies options/seed metadata.
 * No retained pointers into options. Memory must remain fixed until deinit.
 * There is no malloc/free on any path; payloads live in caller-managed pools.
 * A zero-event step starts boot operations; TIME establishes the clock origin.
 * All participants use the same durability policy, immutable across epochs.
 * NEW requires an empty store and a common epoch-0 seed configuration. RECOVER
 * loads metadata/log lazily; missing/stale state requires quorum recovery,
 * never bootstrap. JOIN cannot vote or execute as primary before admission.
 * NEW requires replica in seed; JOIN requires it absent. Only one live instance
 * may use a replica identity/store. NEW/JOIN require an empty store verified by
 * LOAD_RECOVERY; missing store returns NOT_FOUND, not an invented recovered row.
 * Cluster/incarnation/replica IDs are nonzero. Options/seed are borrowed only
 * during this call. Supply at least layout.size bytes at layout.alignment.
 * Returns OK, EINVAL, or ELIMIT; *out=NULL on error when out is valid. The arena
 * has no valid instance after failure. A live instance must not be reinitialized.
 * NULL arguments/misalignment return EINVAL; insufficient size returns ELIMIT.
 * memory must not overlap options, seed metadata, or out.
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
 * With valid v/update/output storage, update is usable on every return, even
 * if a later event is invalid: accepted events and emitted operations are not
 * rolled back. A negative result identifies the event at consumed (or invalid
 * call arguments); VSR_AGAIN means
 * budget/capacity stopped progress (possibly after consuming all input), not
 * failure. OK means all input consumed and no immediately runnable work remains;
 * asynchronous operations may still be outstanding. Never resubmit the consumed
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
 * TIME/STOP/COMPLETE take priority in a driver's queue. STOP is idempotent;
 * after STOP, only those events and zero-input drains are accepted. FAILED and
 * RETIRED also consume other well-formed events without admitting new work.
 * A valid completion is eventually admissible through the reserved capacity;
 * a full output array or exhausted work budget may still require drain calls.
 * Null v/update, inaccessible memory, and unsynchronized/reentrant calls are
 * programming errors. Other invalid call arguments return EINVAL with zero
 * output counts/flags and the current deadline. Invalid event: consumed points
 * at that event; process output, then correct/drop it before submitting the rest.
 */
int vsr_step_many(struct vsr *v, const struct vsr_event *events, uint32_t count,
                  struct vsr_update *update);

/*
 * Cold-path observation; no mutation/allocation. Both pointers must be valid.
 * Does not extend configuration pointer lifetime or grant protocol authority.
 * All instance calls, including observation, require the same exclusive owner.
 */
void vsr_get_status(const struct vsr *v, struct vsr_status *status);

/*
 * After STOP, drain/cancel external operations and consume all RELEASE outputs.
 * Deinit succeeds only in STOPPED with no outstanding ops/leases; otherwise
 * EBUSY. It does not free caller memory. STOP is not a guarantee that pending
 * requests committed. RETIRED reports safe membership removal; merely learning
 * a new configuration is not permission to shut down an old state-transfer donor.
 * At STOPPED core snapshot holds end; persistent store references and the
 * adapter's own readers still govern reclamation of stored snapshot objects.
 */
int vsr_deinit(struct vsr *v);

#endif /* VSR_H */
