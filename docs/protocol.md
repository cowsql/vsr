# Protocol contract

This document defines the protocol obligations of [the public API](../include/vsr.h).
The [adapter reference](vsr-api.md) defines how external work fulfills them.
The base protocol is Liskov and Cowling's
[Viewstamped Replication Revisited](https://pages.cs.wisc.edu/~remzi/Classes/739/Papers/vr-revisited.pdf),
sections 4–7: ordered preparation, view change, recovery, checkpoint transfer,
and epoch reconfiguration. Witness retention, durable storage transactions, and
unlogged read barriers are additional contracts specified here.

## Authority and quorums

Let `q = n - f`, with `n >= 2f + 1`. Each quorum contains distinct voters from
one epoch. The local member counts only after satisfying the same prerequisites
as other voters. Thus two quorums intersect in at least `n - 2f` members, and
any quorum intersects a set of `f + 1` members. Full members and witnesses have
equal votes; only full members may lead or execute application commands.

Preparation requires `q` votes including the primary. View change requires `q`
START_VIEW_CHANGE senders before DO_VIEW_CHANGE, then `q` DO_VIEW_CHANGE offers
at the designated primary, including its own. Recovery requires `q` responses
from other members, including the primary of the highest reported view. Read
confirmation requires `q` voters including the primary. Handoff readiness
requires both `q` new members and `f + 1` new full members with recoverable state.

The adapter authenticates cluster and sender identity before submitting a
MESSAGE. Authentication also covers authorized learners and discovery peers;
restricting transport to current voters alone would prevent joining and disjoint
reconfiguration. Authorization to exchange state does not confer a vote.
Administrative authorization of RECONFIGURE and access control for application
commands belong to the adapter.

The core checks message type, epoch, view, sender role, ranges, and outstanding
round before using a message. Wrong-cluster, unauthorized-for-this-round, stale,
and unsolicited messages cannot advance protocol state. Structurally invalid
events return an API error without consumption; well-formed inapplicable
messages are consumed and ignored or answered with current epoch information.
Duplicate senders count once. A conflicting peer message cannot overwrite a
committed entry; irreconcilable authoritative histories fence the replica with
`VSR_FAILURE_INVARIANT`.

The primary is the full member at `view % full_count` in sorted-ID order.
No message can appoint another primary. Learning a higher view fences the old
view before any response endorsing the new one. Learning a committed later
epoch fences the earlier epoch; it does not establish local catch-up.
A persisted RECOVERING/TRANSITIONING hard state may record that later descriptor
with phase TRANSFERRING even when its boundary exceeds local commitment. Such a
record grants no vote and survives restart as an unfinished installation. Only
this explicit nonvoting combination permits the local history to lag the known
epoch boundary; log offers always describe an actually available complete history. Epoch
discovery can skip obsolete configurations using authenticated committed state,
but membership hints alone never establish recovery or voting readiness.

## Preparation and view change

PREPARE batches are nonempty, consecutive, and end at `message.number`.
`committed <= message.number`. Their entries may retain earlier proposal views
when re-proposed after view change. Newly allocated entries use the current
epoch and view; request identity and contents never change during retransmission.
COMMIT carries no entries and also acts as the idle heartbeat. A received commit
beyond local history is a catch-up target; local commitment advances only once
the prefix is present. A primary never resends entries below its retained log:
a backup acknowledged below that log receives COMMIT and catches up through
state transfer. PREPARE_OK certifies a contiguous prefix in the envelope's
epoch and view, never mere receipt of a batch or transport completion.

Only the designated primary sends PREPARE, COMMIT, and START_VIEW. A backup
acknowledges only in NORMAL after installing that view's selected history.
START_VIEW_CHANGE and DO_VIEW_CHANGE fence old-view preparation. Offers are
ranked by `(last_normal_view, log_end)`; commitment is the maximum known committed
position, including the receiver's. Ties describe the same history; choosing the
lowest source ID makes fetch scheduling deterministic. A selected log shorter
than that committed prefix is inconsistent and cannot be installed.

The primary fetches the complete selected history before entering NORMAL and
sending START_VIEW; a backup does the same before acknowledging START_VIEW.
`last_normal_view` advances only at that point; it equals `view` in NORMAL and
never exceeds it. Both reset to zero on entering a new epoch. Missing chunks do not
authorize changing the selected history. An expired revision forces renewed
protocol validation. A view change can be retried at a higher view when its
primary or required offers remain unavailable.

After a view change, the new primary re-proposes any inherited uncommitted
suffix and collects current-view acknowledgments. An inherited RECONFIGURE at
the log end prevents further old-epoch proposals, including internal NOOPs.
If log selection discards an uncommitted reconfiguration, ordinary proposals
can resume. Once it commits, the old epoch accepts no further entries.

## Restart and persistence

A durable restart restores the complete acknowledged storage prefix, including
identity, view fences, last normal view, commitment, and epoch metadata. Recovery
does not downgrade VIEW_CHANGE to NORMAL, undo RETIRED, or turn TRANSFERRING into
readiness. Application reconstruction precedes normal service on a full member.
Missing state requires quorum recovery; malformed or corrupt local state fences
the instance. Rolling a store back behind acknowledged durability is outside
the storage contract.

Replicated-mode restarts that can participate enter RECOVERING, even when a
plausible local checkpoint exists; a retained removal tombstone remains RETIRED.
A recovering replica does not vote, lead, acknowledge reads, or
answer recovery requests. Recovery responses come only from NORMAL current
members and echo a fresh nonce. Non-primary responses have `state=NULL` and
`number=0`; the highest-view primary supplies the authoritative complete log
offer. Responses from different epochs are never combined. A newly learned
view or epoch invalidates incompatible responses and restarts validation.

The recovering member installs the selected checkpoint/log and restores client
and application state before participation. A recovering would-be primary
cannot recover from itself; peers must advance to another view. If `f=0`, an
absent or replicated-mode store cannot obtain `n` responses from `n-1` peers.
Durable recovery with intact storage remains possible. A new cluster requires
explicit NEW startup with a new cluster identity and common genesis state.

The following table gives the durable prerequisites for externally visible
actions. In replicated mode, the same logical state must be readable, while
restart safety comes from quorum recovery instead of a mandatory flush.

| Action | State that must precede it |
| --- | --- |
| PREPARE or PREPARE_OK | Identity, epoch/view fences, and the entire endorsed prepared prefix |
| START_VIEW_CHANGE or DO_VIEW_CHANGE | The new view fence and recovery metadata for the offered history |
| START_VIEW, READ_ACK, or a recovery response | The sender's eligible NORMAL state and any history the action endorses |
| APPLY or successful request reply | The committed log prefix and its commitment metadata |
| CHECKPOINT advertisement or EPOCH_STARTED | The advertised recoverable coverage, including local checkpoint content when used |
| CHECK_EPOCH success or donor retirement | Completed handoff recorded in local epoch metadata |
| Checkpoint publication followed by trim | Durable snapshot contents, then durable publication of the recovery anchor |

STORE completion establishes readability. SYNC establishes a durable transaction
prefix; out-of-order completions cannot create holes in either frontier.
Dependent actions wait for both. Group commit and pipelining can satisfy many
prerequisites with one synchronization. A completed result is stored before its
successful reply; deterministic replay can reconstruct it from the durable
checkpoint and committed log without requiring a separate result flush.

## Epoch handoff and witnesses

RECONFIGURE is an ordinary client request whose body names the next membership.
Its boundary is committed by the old group. START_EPOCH and NEW_EPOCH describe
that committed boundary; an uncommitted proposal cannot introduce an epoch.
The next epoch starts at view zero and the next log position after the boundary.
Its membership may be disjoint and may change `f` or member roles.

An authenticated peer already known in the receiver's current membership or
seed may report a later committed epoch, including a jump over compacted
intermediate epochs. This relies on the crash-only, non-Byzantine peer model;
self and unknown unsolicited senders cannot introduce such knowledge. Learning
metadata preserves the local committed floor and fences the receiver as
TRANSFERRING until it verifies and installs the required history. Locally
proposed RECONFIGURE requests still name exactly the next epoch. Surviving
peers answer stale-epoch traffic, including authorized learner discovery, with
NEW_EPOCH so loss of earlier handoff announcements does not strand a seed.

`vsr_epoch.phase` is local materialization/retention progress, independent of
NORMAL or VIEW_CHANGE. TRANSFERRING prohibits voting. Once installed, a new
member can enter NORMAL with phase INSTALLED and contribute to new-epoch
quorums. A full member first rebuilds application and completed-client state
through the boundary; a witness retains the protocol history and required remote
checkpoint coverage. EPOCH_STARTED promises that this coverage is recoverable
under the cluster's durability policy and will be retained until safe successor
coverage exists. It is sent to both groups and retransmitted in response to
START_EPOCH. A current member that has already promised also answers
START_EPOCH from an authenticated learner outside both groups with the same
promise, unicast to that sender; this retransmission adds no vote and no
retention obligation beyond what the member owes the two groups. It is never
sent merely because a replica knows the membership.

Each replica, including a learner, establishes STEADY after collecting the
required distinct EPOCH_STARTED promises or learning a later committed epoch
that necessarily follows that handoff. A local durable STEADY record survives
restart. A peer's phase alone does not establish the receiver's installation or
retirement. Readiness collection is independent of current view so view changes
cannot erase the obligation. Until STEADY, the next RECONFIGURE is rejected
with BUSY.
CHECK_EPOCH may commit but cannot execute successfully before its target is
ready; this also prevents a cached duplicate from bypassing the retention fence.

Removed members serve as donors while TRANSITIONING, then persist retirement
and become RETIRED. A learner that installs through the boundary is never
retired: at STEADY it resumes nonvoting warm-up in the learned epoch and awaits
logged admission. A full member demoted to witness retains its full donor
state through handoff; its materialized `status.role` can therefore remain FULL
while the new membership permits only witness voting. It cannot lead or serve
application reads in that epoch. A promoted witness cannot vote as a new full
member or become primary before reconstruction. No role change or restart
permits dropping coverage still promised to the old or new group.

Full members retain an application/client checkpoint and all subsequent needed
log entries. A witness can trim through position `k` only after `f + 1` full
members in the responsible configuration advertise retained checkpoints at or
beyond `k`. They need not advertise the same snapshot ID. CHECKPOINT identifies
the sender's own retained image; forwarding a remote anchor is not an ownership
advertisement. Coverage promises are monotonic: replacing an image must preserve
at least its recoverable prefix. Recovered witnesses reacquire advertisements
before extending trim coverage; they cannot treat their remote anchor as a set
of fresh promises.

During handoff, old promises remain binding until new coverage is established.
This rule permits a quorum containing mostly witnesses without losing the
application state needed by a future full primary. It can require an unavailable
full member to recover before donor retirement, even when a voting quorum is
already processing commands.

## Checkpoints and immutable offers

A checkpoint binds application state, the latest completed result per client,
and epoch metadata to a committed operation boundary. Its `view` is the original
proposal view of that boundary entry, not the view during capture. At genesis
the operation and view are zero. At a reconfiguration boundary its epoch metadata
names the new group; the boundary entry itself belongs to the preceding epoch.
The core preserves this boundary metadata across trimming.

A nonempty retained log is contiguous. In both recovered state and log offers,
`1 <= log_begin <= log_end`, `committed < log_end`, and
`log_begin <= committed + 1`. If `log_begin > 1`, a checkpoint is required with
`log_begin <= checkpoint.op + 1 <= log_end` and `checkpoint.op <= committed`.
An optional checkpoint at an untrimmed base obeys the same bounds. Entry chunks
lie entirely inside this range. Witness offers may name a remote anchor;
fetching the actual application snapshot requires a full donor.

One revision fixes its metadata, checkpoint identity, and complete logical log,
not just the attached entry chunk. GET_LOG and range GET_STATE address that
revision exactly; discovery alone uses revision zero. Nonces identify fetch
attempts, and responses match source, revision, epoch context, requested range,
and byte/count limits. A source changing view cannot silently substitute a
different revision. Source restart changes the revision incarnation.

Checkpoint replacement may make a witness's remote snapshot ID unavailable.
The receiver discovers another full donor and validates its checkpoint against
the selected committed history. A checkpoint beyond the selected offer's
committed position requires renewed protocol validation; it cannot be grafted
onto an unrelated suffix. Expiration never authorizes dropping a committed
entry. Active adapter snapshot readers hold their own references independently
of core offer expiration.

## Read barriers

A linearizable barrier first requires a NORMAL full primary with a committed
current-view entry following its inherited suffix. An ordinary command suffices;
an internal NOOP is proposed only when needed. After read admission, the
primary chooses a fresh nonce and a committed floor, and sends READ_PROBE.
Every acknowledgment matches that nonce, epoch, view, and floor exactly. A
backup responds only to its designated primary while NORMAL with a contiguous
prepared prefix covering the floor and its view fence established. Witnesses
can acknowledge; application execution on backups is unnecessary.

After a quorum confirms, the primary waits for application progress through
both the floor and `min_op`. A view or epoch change before READ_READY invalidates
the round. Reads admitted after its first probe cannot join it. The resulting
fence serializes application mutation until snapshot capture completes; a
subsequent view change does not invalidate a fence already granted. READ_READY
can therefore complete after leadership has changed, with the read ordered
before that change. No timeout or transport-send completion establishes a read
quorum, and no clock lease is assumed.

A causal barrier runs only on a NORMAL full member and waits for `min_op`.
The caller supplies committed positions from this cluster and carries the
returned applied position into later dependent reads. Logging a read as a
COMMAND remains available when the read must participate in duplicate suppression
or update application state.
