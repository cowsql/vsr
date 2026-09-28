#ifndef VSR_CLIENT_H
#define VSR_CLIENT_H

#include "vsr.h"

#include <stddef.h>
#include <stdint.h>

/*
 * VSR-CLIENT: sans-IO bookkeeping for a client of a VSR group.
 *
 * A client of this protocol has real logic and no I/O in it: it owns client
 * incarnations that are never reused, numbers requests monotonically with
 * one outstanding per incarnation, retries an uncertain outcome with the
 * identical body, follows NOT_PRIMARY to the advertised primary and adopts
 * the membership that comes with NEW_EPOCH, backs off on BUSY, and carries
 * the operation positions it has seen into causal reads. This header is
 * that logic and nothing else. It performs no I/O, reads no clock, draws no
 * randomness, and allocates nothing: the caller supplies an arena, passes
 * time as a parameter, chooses incarnation IDs, moves bytes over whatever
 * protocol it owns, and feeds back the struct vsr_reply it received. The
 * same code drives a production client and a simulated one.
 *
 * Submission is always local to a node: the caller's server decodes its own
 * protocol and submits a struct vsr_request to its local replica. The
 * identity in that request must nevertheless be the REMOTE client's, which
 * is why this bookkeeping lives at the remote end: if node Y submits a
 * request and crashes before replying, the client's retry at the new
 * primary is deduplicated only when it carries the same incarnation and
 * number that Y submitted.
 *
 * Retention consequence: the cluster keeps the latest completed result of
 * every incarnation forever (see vsr.h, struct vsr_client_record). Each
 * incarnation therefore costs one permanent record. Open lanes once, when
 * the client process starts, and keep them for its lifetime; never open a
 * lane per request.
 *
 * A LANE is one incarnation with its own counter and at most one request in
 * flight. Requests on one lane complete in order; requests on different
 * lanes have no mutual ordering, which is the only way to pipeline.
 *
 * One owner calls every function on a client; no reentrancy. All times are
 * nanoseconds in one monotonic domain chosen by the caller, below
 * VSR_NO_DEADLINE. Reserved fields must be zero.
 */

#define VSR_CLIENT_API_VERSION 1u
#define VSR_CLIENT_STATE_VERSION 1u /* Export format, see vsr_client_export. */
#define VSR_CLIENT_NO_LANE UINT32_MAX

struct vsr_client;

/*
 * Positive fixed capacities. members bounds the copied membership; a reply
 * carrying a larger membership is still interpreted but its membership is
 * not adopted, and vsr_client_reply reports ELIMIT after filling the
 * outcome. seed, when non-NULL, is copied as the initial topology hint with
 * an unknown primary. Backoff after BUSY is deterministic: backoff_ns
 * doubled per consecutive BUSY on that lane, capped at backoff_max_ns; add
 * jitter outside if you want it. request_timeout_ns bounds one attempt.
 */
struct vsr_client_options {
    const struct vsr_membership *seed;
    uint32_t lanes;
    uint32_t members;
    uint64_t request_timeout_ns; /* Nonzero. */
    uint64_t backoff_ns;         /* Nonzero. */
    uint64_t backoff_max_ns;     /* >= backoff_ns. */
    uint32_t cache_line_bytes;   /* Power of two; 0 selects 64. */
    uint32_t reserved;
};

/* Arena requirement. No allocation. Returns OK, EINVAL, or ELIMIT. */
int vsr_client_layout(const struct vsr_client_options *options,
                      struct vsr_layout *layout);
/* Copies options and seed; memory stays fixed and owned by the caller until
 * the client is abandoned. There is nothing to deinitialize. Returns OK,
 * EINVAL, or ELIMIT; on error *out is NULL. */
int vsr_client_init(void *memory, size_t size,
                    const struct vsr_client_options *options,
                    struct vsr_client **out);

/* -------------------------------------------------------------------------
 * Lanes
 * ---------------------------------------------------------------------- */

enum vsr_client_lane_state {
    VSR_CLIENT_LANE_CLOSED,
    VSR_CLIENT_LANE_IDLE,     /* No request in flight. */
    VSR_CLIENT_LANE_PENDING,  /* An attempt is outstanding until deadline. */
    VSR_CLIENT_LANE_WAITING,  /* Backing off; retry after deadline. */
    VSR_CLIENT_LANE_DETACHED, /* Imported pending request without its body:
                                 needs vsr_client_resume or a query. */
    VSR_CLIENT_LANE_QUERYING  /* A client query for the pending number is
                                 outstanding. */
};

/*
 * Opens a lane for a NEW incarnation the caller guarantees unique across
 * every client that ever used the cluster; next_number is its first request
 * number, nonzero. Returns OK and sets *lane, or ELIMIT when every lane is
 * open. A lane may be closed only while IDLE; its incarnation is then
 * retired and must never be opened again.
 */
int vsr_client_open(struct vsr_client *client, struct vsr_id incarnation,
                    uint64_t next_number, uint32_t *lane);
int vsr_client_close(struct vsr_client *client, uint32_t lane);

struct vsr_client_lane_status {
    struct vsr_id incarnation;
    uint64_t next_number;    /* Number the next vsr_client_begin will use. */
    uint64_t pending_number; /* Number in flight, or zero. */
    uint64_t deadline_ns;    /* Attempt or backoff deadline; NO_DEADLINE. */
    uint64_t replica;        /* Target of the current attempt, or none. */
    uint32_t state;          /* enum vsr_client_lane_state */
    uint32_t attempts;       /* Attempts made for the pending request. */
    uint32_t busy_streak;    /* Consecutive BUSY replies, drives backoff. */
    uint32_t reserved;
};

void vsr_client_lane_status(const struct vsr_client *client, uint32_t lane,
                            struct vsr_client_lane_status *status);

/* -------------------------------------------------------------------------
 * Requests
 *
 * The body is caller-owned and immutable by reference until the request is
 * DONE or FAILED: a vsr_blob for COMMAND, a vsr_membership for RECONFIGURE,
 * a vsr_check_epoch for CHECK_EPOCH; NOOP is not a client request. The
 * attempt's request is what the caller must transmit: identity, routing
 * epoch, type and body pointer. replica is the advertised primary; when that
 * is unknown, the next known member in ID order after the lane's previous
 * target, whose NOT_PRIMARY reply will name the primary; VSR_NO_REPLICA only
 * when no membership is known, and then any node may be tried. Every
 * attempt of one request carries the same identity, type and body; only
 * request.epoch follows redirects, exactly as vsr.h requires.
 * ---------------------------------------------------------------------- */

struct vsr_client_attempt {
    struct vsr_request request;
    uint64_t replica;     /* Where to send it, or VSR_NO_REPLICA. */
    uint64_t deadline_ns; /* Report to vsr_client_time at or after this. */
    uint32_t lane;
    uint32_t attempt; /* 1 for the first attempt of a request. */
};

/* EBUSY while the lane is not IDLE; EINVAL for a bad lane, type or body. */
int vsr_client_begin(struct vsr_client *client, uint32_t lane, uint32_t type,
                     const void *body, uint64_t now_ns,
                     struct vsr_client_attempt *attempt);

enum vsr_client_action {
    VSR_CLIENT_IGNORE, /* Stale or duplicate reply; nothing changed. */
    VSR_CLIENT_DONE,   /* The reply passed in is the final answer; for a
                          COMMAND its result is in reply.result. */
    VSR_CLIENT_RETRY,  /* Send attempt.request to attempt.replica now. */
    VSR_CLIENT_WAIT,   /* Do nothing until deadline_ns, then call
                          vsr_client_time. */
    VSR_CLIENT_FAILED  /* Terminal: status names the reply status. The lane
                          is IDLE again; the request will never execute. */
};

struct vsr_client_outcome {
    uint32_t action; /* enum vsr_client_action */
    uint32_t lane;
    uint32_t status; /* FAILED: enum vsr_reply_status of the reply. */
    uint32_t reserved;
    uint64_t deadline_ns;              /* WAIT: when to call vsr_client_time. */
    struct vsr_client_attempt attempt; /* RETRY: the next attempt. */
};

/*
 * Interprets a reply for a lane. OK with EXECUTED completes the request and
 * raises the client's observed position to reply.op. NOT_PRIMARY records the
 * advertised primary and membership and yields RETRY there. NEW_EPOCH adopts
 * the returned membership and its epoch, then RETRY with the new routing
 * epoch. BUSY yields WAIT with the lane's backoff. INVALID, LIMIT and
 * STALE_REQUEST are FAILED; after STALE_REQUEST the lane's counter is behind
 * a number the cluster already saw, so the caller should close the lane and
 * open a fresh incarnation. A reply whose request number is not the pending
 * one, or for an IDLE lane, is IGNORE. CLIENT_STATE and CLIENT_UNKNOWN
 * belong to vsr_client_queried; TIMEOUT belongs to reads and is IGNORE here.
 * A NOT_PRIMARY naming no primary, or a NEW_EPOCH no newer than the attempt's
 * routing epoch (a lagging replica), yields WAIT for backoff_ns instead; a
 * NOT_PRIMARY naming the attempt's own target in its epoch is IGNORE, left to
 * the attempt's timeout. Only a PENDING lane follows redirects and BUSY; a
 * WAITING lane still accepts the final statuses. Returns OK, ELIMIT when a
 * membership could not be adopted (outcome still valid), or EINVAL.
 */
int vsr_client_reply(struct vsr_client *client, uint32_t lane,
                     const struct vsr_reply *reply, uint64_t now_ns,
                     struct vsr_client_outcome *outcome);

/*
 * Expires attempts and ends backoffs. Returns 1 and one outcome (RETRY) for
 * the earliest lane whose deadline is at or before now_ns, 0 when none is
 * due; call until it returns 0. An expired attempt retries at the current
 * primary if known, else at the next known member in ID order, so a dead
 * primary is eventually bypassed by a NOT_PRIMARY from a live backup; an
 * attempt that expires at the advertised primary makes the primary unknown
 * until a reply names one again. EINVAL for NULL arguments.
 */
int vsr_client_time(struct vsr_client *client, uint64_t now_ns,
                    struct vsr_client_outcome *outcome);
/* Earliest lane deadline, or VSR_NO_DEADLINE; arm a timer for it. */
uint64_t vsr_client_deadline(const struct vsr_client *client);

/*
 * Resolving an uncertain request without its body. After import, a lane
 * whose request was pending is DETACHED. vsr_client_resume reattaches the
 * identical body and yields a RETRY attempt. Alternatively vsr_client_query
 * gives the incarnation to submit as a CLIENT_QUERY (vsr.h) and moves the
 * lane to QUERYING; vsr_client_queried interprets the CLIENT_STATE or
 * CLIENT_UNKNOWN reply: DONE if it reports the pending number as EXECUTED
 * (result in reply.result), FAILED with STALE_REQUEST if a later number is
 * known, otherwise RETRY-less IGNORE with the lane back to DETACHED, since a
 * query is local knowledge only and cannot prove the request never ran. A
 * caller that cannot supply the body and gets IGNORE must treat the outcome
 * as unknown, close nothing, and open a fresh incarnation for new work.
 * resume and query return EINVAL unless the lane is DETACHED or QUERYING.
 */
int vsr_client_resume(struct vsr_client *client, uint32_t lane,
                      const void *body, uint64_t now_ns,
                      struct vsr_client_attempt *attempt);
int vsr_client_query(struct vsr_client *client, uint32_t lane,
                     struct vsr_id *incarnation, uint64_t *replica);
int vsr_client_queried(struct vsr_client *client, uint32_t lane,
                       const struct vsr_reply *reply, uint64_t now_ns,
                       struct vsr_client_outcome *outcome);

/* -------------------------------------------------------------------------
 * Topology and reads
 * ---------------------------------------------------------------------- */

struct vsr_client_status {
    uint64_t epoch;   /* Routing epoch used for new requests. */
    uint64_t primary; /* Advertised primary, or VSR_NO_REPLICA. */
    uint64_t min_op;  /* Greatest position observed; see vsr_client_min_op. */
    uint32_t lanes;   /* Open lanes. */
    uint32_t busy;    /* Lanes not IDLE. */
    const struct vsr_membership *membership; /* Copy in the arena, or NULL;
                                                borrowed until the next
                                                mutating call. */
};

void vsr_client_get_status(const struct vsr_client *client,
                           struct vsr_client_status *status);

/*
 * Out-of-band topology hint, for example from discovery or another client.
 * membership is copied when non-NULL and its epoch is not older than the
 * current one; primary VSR_NO_REPLICA leaves the primary unknown. A hint
 * never overrides knowledge learned from a newer reply. Returns OK, ELIMIT
 * when the membership exceeds capacity, or EINVAL.
 */
int vsr_client_learn(struct vsr_client *client,
                     const struct vsr_membership *membership, uint64_t primary);

/*
 * Reads are not requests: the caller carries them in its own protocol to a
 * node, which submits a VSR_EVENT_READ locally and answers on its own. This
 * bookkeeping supplies the barrier and the target. LINEARIZABLE targets the
 * primary (VSR_NO_REPLICA when unknown, in which case the caller must first
 * learn it, for example through any request's reply). CAUSAL targets full
 * members round-robin in ID order and sets min_op to the greatest position
 * this client has observed, which gives it read-your-writes and monotonic
 * reads but not linearizability across clients. deadline_ns is copied into
 * the barrier. After the node answers, feed the fence's applied position to
 * vsr_client_observe so later causal reads do not travel back in time.
 */
int vsr_client_read(struct vsr_client *client, uint32_t consistency,
                    uint64_t deadline_ns, struct vsr_read_barrier *barrier,
                    uint64_t *replica);
void vsr_client_observe(struct vsr_client *client, uint64_t applied);
/* Greatest of completed writes' op and observed applied positions. */
uint64_t vsr_client_min_op(const struct vsr_client *client);

/* -------------------------------------------------------------------------
 * Persistence
 *
 * Export writes an opaque, versioned image of the bookkeeping: lanes with
 * incarnations, counters and pending identities, the topology, and min_op.
 * Pending bodies are not included; they are the caller's. Persisting this
 * image after every vsr_client_begin, and importing it on restart, is what
 * allows resuming an incarnation and resolving a request that was in flight
 * across the restart. A caller that does not persist must open fresh
 * incarnations after a restart and accept that the outcome of any request
 * in flight at the crash is unknown: the cluster may or may not have
 * executed it, and a new incarnation cannot ask.
 * ---------------------------------------------------------------------- */

size_t vsr_client_export_size(const struct vsr_client *client);
/* Returns OK, or ELIMIT with *written = the required size. */
int vsr_client_export(const struct vsr_client *client, void *bytes, size_t size,
                      size_t *written);
/* Only into a client with no open lane, initialized with lane and member
 * capacities at least those of the image. Lanes with a pending request come
 * back DETACHED, at their exported lane indexes. The image's topology
 * replaces the client's unless the client already holds a newer epoch;
 * min_op is the greater of both. Returns OK, EINVAL for a malformed or newer
 * image, ELIMIT when capacities do not fit, EBUSY when lanes are open. */
int vsr_client_import(struct vsr_client *client, const void *bytes,
                      size_t size);

#endif /* VSR_CLIENT_H */
