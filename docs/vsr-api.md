# Adapter reference

[include/vsr.h](../include/vsr.h) defines the public types and entry points.
[DESIGN.md](../DESIGN.md) defines the design principles, and the
[protocol contract](protocol.md) defines quorum and message rules. This reference
specifies how an adapter drives an instance and implements its external operations.

## Initialization

Zero reserved fields and initialize every option explicitly; there are no
implicit capacity or timeout defaults. `cache_line_bytes = 0` selects 64-byte
alignment. Other values must be powers of two; the returned arena alignment also
satisfies the alignment of every internal type. `vsr_layout` validates options,
checks size arithmetic, and includes progress reserves in its result. `vsr_init`
requires at least that size and alignment and copies all option/seed metadata.
Every `*_ns` option must be below `UINT64_MAX`; both calls return `VSR_EINVAL`
otherwise. `vsr_init` clears `*out` on every error except when `out` lies
inside the arena, which returns `VSR_EINVAL` without writing through it. From
`vsr_init` on, `vsr_status.configuration` is never NULL: it describes the seed
as a STEADY epoch with no predecessor and a zero boundary, the genesis
configuration under `NEW` and a discovery hint under `RECOVER` and `JOIN`,
until a recovered or learned descriptor replaces it.

Cluster, replica, and process-incarnation IDs must be nonzero. Incarnations must
be unique across all starts in the cluster, including starts after storage loss.
Only one live instance may use a replica ID or its logical store. Replica IDs
are stable across membership changes and independent of transport addresses.

| Start mode | Seed and store | Startup behavior |
| --- | --- | --- |
| `NEW` | Common epoch-0 membership containing this replica; empty store | Establish the agreed genesis state and initialize storage |
| `RECOVER` | A discovery seed; matching local store if available | Restore local state and perform quorum recovery when required; a replica absent from its group resumes warm-up |
| `JOIN` | Known membership excluding this replica; empty store | Warm as `join_role` without voting; await logged admission |

The first step starts asynchronous `LOAD_RECOVERY`. `NOT_FOUND` means no
initialized store. `NEW` and `JOIN` fail on an existing store. `RECOVER` fails on
identity/policy mismatch or corruption; an absent store requires quorum recovery,
never automatic bootstrap. Stale but internally valid state in replicated mode
is only a recovery hint. A restart uses `RECOVER`, including during warm-up.
The persisted hard-state role records whether application state is being
maintained; it does not grant a vote to a replica absent from the membership.
A restarted learner resumes nonvoting warm-up in that role; quorum recovery is
for members. When no store survives (replicated mode, or a durable crash before
the first SYNC), `join_role` names the role to warm up as from the seed and
`MEMBER_NONE` warms as a witness; a surviving store's persisted role takes
precedence. Warm-up is continuous: a WARMING learner rediscovers the members it
knows every heartbeat and catches up, or follows a later epoch, until a
reconfiguration admits it.
A learned later epoch can be persisted before its boundary is locally present:
only `HARD_RECOVERING` or `HARD_TRANSITIONING` with epoch phase `TRANSFERRING`
permits `epoch.boundary > committed`. This records authenticated configuration
knowledge and fences the previous epoch; it does not assert possession of that
history. Restart preserves this nonvoting state until installation completes.
NORMAL state, INSTALLED/STEADY phases, and every advertised log offer retain the
ordinary complete-history bounds. A later-epoch seed is only a membership hint;
JOIN first discovers the full committed epoch descriptor before initializing
its store, rather than inventing its predecessor or transition boundary.


Transaction 1 of a previously empty store contains `STORE_IDENTITY` and initial
hard state. Identity is immutable, independent of process incarnation, and
preserved through checkpoint restoration. Restoring a peer's checkpoint must
not replace the destination's local replica identity or transaction sequence.
A full replica receives `SNAPSHOT_INSTALL` before any `APPLY`; a NULL checkpoint
means reset to the cluster's agreed genesis application state.

All limits are positive. `message_bytes` must fit the checked sum of one maximum
command and one maximum checkpoint manifest; `batch_entries` must fit the log cache.
The planner rejects capacities that cannot support minimum protocol progress.
The current implementation requires `input_leases >= transfers + 8`. This covers
retained snapshot generations named by immutable offers, the selected source
history, snapshot adoption, one incoming history chunk, a comparison load, and
control/completion progress. Its minimum payload budget is
`message_bytes + 2 * command_bytes + (transfers + 3) * manifest_bytes + result_bytes`,
with checked arithmetic. These are conservative implementation reserves, not
protocol message-size limits; ordinary STORE/APPLY/transfer batches remain
bounded by the configured batch limit and available completion budget.
Admission additionally reserves space for each operation's maximum completion.
Limit exhaustion must defer new work before consuming those reserves.
Configuration, entry-count, span-count, command, result, manifest, and message
limits are deployment compatibility requirements; there is no peer negotiation.
Replicas in a deployment use identical values for those limits. Operational
queue/cache capacities, timing, and checkpoint intervals may differ. The planner
checks all completion-size arithmetic, including `batch_entries * result_bytes`.
APPLY batches may be shortened to fit current reservations. A legal maximum-size
command must remain processable with its maximum-size result and a checkpoint
manifest; otherwise `vsr_layout` returns `VSR_ELIMIT`.

`VSR_API_VERSION` identifies the source contract. It is not a wire, storage, or
application-schema version. Build adapters and the library against the same
header. The deployment owns codec/schema compatibility; it must not negotiate
away quorum, durability, identity, or retention requirements.

## Driving the core

`vsr_step(v, NULL, &update)` and `vsr_step_many(v, NULL, 0, &update)` drain ready
work. Before every call, provide `update.ops` and a positive `update.capacity`.
The other update fields are outputs. Input and output arrays must not overlap
each other, the arena, or pinned objects. Serialize all calls on an instance.

Each return, including an event error, reports:

| Output | Meaning |
| --- | --- |
| `consumed` | Exact accepted prefix; resubmit only the suffix |
| `count` | Valid operations in the caller's output array |
| `deadline_ns` | Absolute replacement timer deadline; `VSR_NO_DEADLINE` cancels it |
| `MORE` | Runnable work remains; drain without new input |
| `OUTPUT_FULL` | Additional output needs slots; implies `MORE` |
| `INPUT_BLOCKED` | The next event needs resources released by other work |
| `STATE_CHANGED` | State, role, epoch, transition phase, view, or latched failure changed |

`VSR_OK` means all input was consumed and no immediately runnable work remains;
external operations may still be outstanding. `VSR_AGAIN` indicates a work budget
or capacity boundary, even if all input was accepted. `VSR_EINVAL` identifies an
invalid event or argument; `VSR_ELIMIT` identifies a permanent size or capacity
violation. An event error leaves that event unconsumed and preserves earlier
accepted events and output. Neither a negative result nor `AGAIN` rolls back work.
`MORE` promises progress: a drain either advances the protocol or returns
without `MORE` once nothing runnable remains. It never spends its work undoing
its own earlier work, such as releasing a cache entry it loaded for a peer
before that peer is served, and retries are coalesced under timers rather than
spun; a host that drains on `MORE` and services completions fairly reaches an
idle return.

Dispatch all returned operations before reusing the array. Queue immediate
completions and feed them back after dispatch; do not call into the core
reentrantly. On `MORE`, drain before adding requests. If blocked without `MORE`,
service completions and time rather than repeatedly offering the same request.
Reorder an adapter queue when a blocked proposal precedes a needed completion.
Completions, STOP, and lease release retain progress capacity under saturation.

Supply `TIME` regularly, including under sustained traffic. Its ID is monotonic
nanoseconds in one local clock domain; values may repeat but never decrease.
Other events use the last accepted time. The first TIME establishes the origin;
protocol timers and read deadlines do not fire beforehand. A returned deadline
at or before the current time requires another current TIME event. Early timer
wakeups are harmless. `batch_delay_ns = 0` disables intentional delay; positive
values bound it from the first queued entry. Heartbeats and retries cannot wait
for application batching. Counter or time arithmetic must fail before wrapping.

Deterministic replay records options, event values and order, call boundaries,
and output capacity. Replace process-local pointers with stable recorder handles.
Operation IDs are unique within an instance, not across starts; the adapter must
drain old work before reusing storage or associate completions with an instance
generation.

## Ownership and resource accounting

An accepted event with non-NULL data transfers an immutable lease over every
reachable record, array, span descriptor, and payload byte. The event descriptor
itself is borrowed only for the call. The adapter keeps the graph alive until
`RELEASE(lease)`, even if a request has already completed or one send has finished.
Unconsumed events transfer nothing. Shared allocations use distinct lease IDs
and adapter-side reference counts. IDs may be reused only after release is
processed. Input graphs may not point into the core arena.

An emitted operation pins its entire data graph until its matching COMPLETE is
accepted. Overwriting the caller's output array does not invalidate that graph.
Finish every access relying on the pin, including deferred zero-copy send
notifications, before completing. Storage may acquire its own references to
adapter-owned payload buffers; it must consume or copy core-owned descriptors
before completion. A completion carrying data is itself a new leased input;
its metadata must not retain pointers into the operation's core-owned records.
For example, a CAPTURE result copies the template's epoch/member descriptors
into adapter-owned storage instead of returning their arena pointers.

Every operation except RELEASE receives exactly one completion, including failed
submission and cancellation. Unknown or duplicate operation IDs are API errors.
Cancellation means accesses have ended; it cannot undo an operation already
performed or certify successful persistence. A rejected completion still owns
its operation pin and transfers no new lease. Correct it or replace it with a
failure completion after external accesses end.

Limits count logical blob bytes, not encoded framing or host structure padding.
`command_bytes`, `result_bytes`, and `manifest_bytes` bound individual objects;
`message_bytes` bounds their aggregate in a message or log load. Typed array
counts separately bound metadata. `pinned_payload_bytes` counts payload bytes
once per occurrence in each admitted graph, including duplicate references;
sharing across leases counts again. Core-generated references to an existing
lease add no charge. Reserved completion capacity is unavailable to new input.
Adapters separately account for backing allocation sizes and external queues.
Admission also reserves the resources needed to make accepted input releasable:
an input queue cannot consume the final slots needed for its own LOAD, STORE,
APPLY, or result. Peer control traffic and bounded completions retain a path to
progress while proposal traffic is throttled. A permanently inadmissible graph
returns `ELIMIT`; temporary resource pressure returns `AGAIN` without consuming it.

## Requests, replies, and reads

A request uses a nonzero client incarnation and nonzero increasing number. At
most one request per incarnation is outstanding. Retry the same identity, type,
and body after an uncertain outcome; only its routing epoch may follow redirects.
Known duplicate results are resolved before rejecting a stale routing epoch.
Do not change a command merely because its first attempt timed out.

Reply routes and read cookies are nonzero, generation-qualified local handles.
Closed routes fail delivery and must not be reassigned to another request while
referenced. They are never stored in the replicated log. An inherited request
without a local route retains its result for a client retry. Duplicate attempts
may share one pending route; admission is not a promise that every retransmission
will receive a separate reply.

| Reply | Contract |
| --- | --- |
| `OK` | Request committed and completed; `EXECUTED` set; result and op valid |
| `NOT_PRIMARY` | Retry using the advertised primary, if known |
| `NEW_EPOCH` | Routing epoch differs; use returned membership and retry |
| `BUSY` | Admission or transition temporarily prevents handling |
| `INVALID` / `LIMIT` | Well-formed request violates a protocol rule or admission limit |
| `STALE_REQUEST` | A newer request is known; this request will not execute again |
| `CLIENT_STATE` | Latest locally known request, with result only if `EXECUTED` |
| `CLIENT_UNKNOWN` | No local completed or retained request; queried client, number/op zero |
| `TIMEOUT` | Read deadline expired before a read fence was emitted |

Ordinary replies echo the submitted request ID. Client queries instead report
the latest locally known ID for the queried client. Read rejections use an
all-zero request ID and the cookie as the reply route. Absent results are empty
blobs with code zero; control requests also have empty successful results.
An unknown primary is `VSR_NO_REPLICA`. Structural errors and oversized input
graphs can be rejected before admission with an API error instead of a reply.

`CLIENT_QUERY` is local observation and may reveal an uncommitted request that
later disappears. `CLIENT_UNKNOWN` does not prove global absence. A client that
keeps an incarnation must durably preserve its issued sequence and unresolved
command; a query alone cannot reconstruct a safe next number. A fresh incarnation
avoids number reuse but does not resolve an old command's uncertain outcome.
No client-record deletion API is provided: eviction removes a memory cache entry,
not persistent duplicate suppression.
Identity reuse with different contents violates the client contract. The core
can reject conflicts while the original entry is available; completed records
do not retain command bodies for indefinite comparison. A newer request cannot
overtake a known pending request from the same client: it receives `BUSY` until
that request resolves. Applications needing multiple outstanding commands use
independent client incarnations.

`CHECK_EPOCH` carries `struct vsr_check_epoch`, whose `epoch` is immutable across
retries. The target may equal or precede the routing epoch; a future target is
`INVALID`. Success certifies completion of handoff into the target, with epoch 0
trivially ready. The request is logged in the current epoch. Application progress
stops before a CHECK_EPOCH entry whose target handoff is unfinished; it produces
no completed client record or successful duplicate reply before readiness. Since
successive reconfigurations require the preceding handoff to complete, a target
older than the current epoch is already safe. RECONFIGURE similarly preserves
its target membership on retry; a fresh proposal must target routing epoch + 1.

`READ` carries an applied minimum and an absolute deadline, or
`VSR_NO_DEADLINE`. A zero deadline is already expired once time is established.
Linearizable reads require a normal, established primary and a fresh quorum
round begun after admission. Reads admitted after a probe starts cannot share
that probe. Causal reads require a normal full replica and only the applied
minimum. Waiting reads occupy `pending_reads`; view/epoch changes reject or
restart unfinished rounds without reusing old acknowledgments.

A successful barrier emits `READ_READY`, freezing application mutation at
`fence.applied >= min_op`. Capture a snapshot or finish the read, then complete
the operation. Evaluation and delivery may continue independently on that
snapshot. No APPLY, INSTALL, or CAPTURE overlaps the fence. Once READ_READY is
emitted, its deadline no longer cancels the pin; the adapter must complete it.
Failed read delivery abandons the local route. The core produces no read value.
Fences are serialized; each READY describes the exact captured boundary, not a
promise that the live application will remain there after completion. Causal
reads provide dependency ordering only when the caller propagates committed
positions from this cluster. They do not establish cross-cluster causality.

## Indexed storage

The store provides immutable logical revisions, not full physical copies of
each revision. Sequence 0 denotes the empty store. Transactions begin at 1 and
continue from the recovered sequence without gaps. Apply changes in array order
and transactions in sequence order, regardless of physical write scheduling.

| Change | Required fields and behavior |
| --- | --- |
| `IDENTITY` | `first=0, count=1`; immutable identity, only in transaction 1 |
| `APPEND` | `first>=1, count>0`; consecutive entries at the current log end |
| `TRUNCATE` | `first>committed, count=0, data=NULL`; remove suffix starting there |
| `CLIENTS` | `first=0, count>0`; update latest completed client records |
| `HARD_STATE` | `first=0, count=1`; replace protocol metadata without weakening fences |
| `PUBLISH_CHECKPOINT` | `first=0, count=1`; replace recovery anchor without resetting live state |
| `RESTORE_CHECKPOINT` | `first=0, count=1`; replace checkpoint/client base, retaining validated log entries above its op |
| `TRIM` | `first>=1, count=0, data=NULL`; remove covered log entries below first |

APPEND and CLIENTS arrays each contain at most `batch_entries` items. Every
transaction has at most `VSR_MAX_STORE_CHANGES` changes. TRUNCATE followed by
APPEND replaces an uncommitted suffix. RESTORE replaces the completed-client
table with the checkpoint's table and removes log entries through checkpoint.op.
Entries above it remain by reference; the retained-request index is rebuilt
logically from that suffix. The resulting range begins at checkpoint.op + 1 and
ends at the greater of that position and the previous log end. A preceding
TRUNCATE in the transaction removes any divergent uncommitted tail. The core
validates every retained suffix against the selected history before RESTORE.
The same transaction includes HARD_STATE, without reducing known commitment or
view/epoch fences. Missing suffix entries arrive through bounded later APPENDs;
no voting or normal service resumes before the selected history is installed.
RESTORE neither installs the application nor resets local identity or storage
sequence. On witnesses it installs only the remote anchor and protocol indexes.

Maintain an index from client ID to its latest retained log entry. APPEND,
TRUNCATE, RESTORE, and TRIM update that index atomically with the log. Completed
client records form a separate index. Replaying old results must not overwrite
a record for a later request. The core merges both indexes with in-flight state
to deduplicate without scanning an unbounded log on each cache miss. Witnesses
do not generate application-result records; client queries receive NOT_PRIMARY.

| Load | Success data in `vsr_loaded.items` |
| --- | --- |
| `RECOVERY` | Exactly one `vsr_recovered`; returned sequence equals recovered sequence |
| `LOG` | Consecutive `vsr_entry[]` beginning at `first`; `next=first+count` |
| `CLIENT` | Zero or one latest completed `vsr_client_record` for `client` |
| `REQUEST` | Zero or one latest retained `vsr_entry` for `client`, executed or pending |

RECOVERY ignores the requested sequence. Other loads read exactly that sequence.
For LOG, obey both byte and count bounds, return `next=end` when complete, and
never return empty nonterminal success. Empty ranges are valid. For other loads,
`next=0`; absent client/index records are successful count-zero results with
NULL items. Entirely uninitialized storage returns RECOVERY/NOT_FOUND. Expected
compacted log ranges may return NOT_FOUND; an unexplained hole in retained local
history or a missing live revision is fatal.
Because LOAD addresses an exact retained revision, compaction of a newer revision
cannot make its entries disappear. A requested range outside a known retained
range is expected absence; an issued valid read losing its data is not. The
core therefore never issues a LOAD below the range that the named revision
retains: a TRIM or RESTORE bounds later loads as soon as its STORE completes,
before it is durable, even though offers keep describing the safe revision.
Non-LOG loads use `first=end=0`; RECOVERY uses sequence 0 and a zero client ID.
LOG uses a zero client ID. All returned blobs obey both the operation's byte
budget and their per-object limits. The recovered log/checkpoint bounds are the
same as those for a log offer in the protocol contract.

STORE success means the complete transaction is readable and independent of its
operation pin, not necessarily durable. SYNC success certifies all transactions
through its argument as crash-durable. For example, STORE(41), STORE(42), and
SYNC(42) can share physical writes and one flush, but each receives a completion.
SYNC may be issued before earlier STORE completions; the adapter enforces that
dependency. The core accounts for out-of-order completions before advancing its
readable and durable prefixes.

Recovery discards torn transactions and their dependents and exposes a complete
prefix. Previously acknowledged durable transactions cannot be discarded.
RECLAIM permits collecting revisions below its argument after independent
readers drain, without deleting data reachable from retained revisions or
checkpoints. No later core load names a reclaimed revision.

## Log and checkpoint transfer

A log offer identifies the source incarnation and exact store sequence, view,
last normal view, committed position, retained range, epoch, and optional
checkpoint. `message.number` is the offered last op (`log_end - 1`), never the
end of the included entry chunk. A bounded portion may accompany the offer.
The incarnation qualifies the revision, including when a store is reopened with
the same sequence numbers. Revision zero is reserved for discovery requests.

GET_STATE with zero revision and `first=end=0` discovers a current offer. Its
response has `first=next=0`. Range GET_STATE or GET_LOG names an exact revision
and `[first,end)` with positive entry/byte limits; GET_LOG is used for the log
chosen by view change. A successful chunk echoes the nonce and first, contains
exactly `[first,next)`, and advances toward the requested end. Metadata remains
fixed for that revision even if the sender's current envelope view advances.
All responses are checked against the outstanding request and protocol context.
`max_bytes` bounds every blob in the response, including its checkpoint manifest,
and `max_entries` bounds its entry array. Every fetch reserves at least
`command_bytes + manifest_bytes` and at most `message_bytes`. A nonempty range
must return at least one entry on success; an empty range returns `next=first=end`. Discovery
has no entries. STATE_UNAVAILABLE also obeys the byte limit. Responding to a
fetch never changes the source revision's commitment or epoch metadata.

Unknown/expired revisions or unavailable ranges produce STATE_UNAVAILABLE with
the echoed nonce and first, `next=first`, and a current offer without chunk data.
The receiver revalidates that offer. `transfers` bounds retained offers;
`transfer_timeout_ns` expires idle offers. When capacity is needed for a newer
fenced revision, the oldest unreferenced offer may be evicted earlier; requests
for it receive STATE_UNAVAILABLE and must revalidate. Active loads/sends retain their pins
until completion even if the offer expires. A valid range request renews its
retention. Discovery offers may include a checkpoint instead of older log data.

CAPTURE supplies a checkpoint template containing the exact op, view, and epoch,
and a readable store sequence with the matching completed client table. Return
the same logical boundary with a fresh snapshot ID and local manifest. Capture
must freeze both application state and that indexed client revision; it must not
read a moving current table. The core issues it only when replay is complete and
client records through that boundary are stored. APPLY and INSTALL cannot overlap
capture. Completion releases the mutation fence; later copying/flushing is free
to proceed concurrently.

FETCH names a peer, target checkpoint, and optional locally retained basis.
The adapter handles bulk transport, validation, and differential materialization
outside `step`. Success returns the same snapshot identity and logical contents,
fully readable locally; its local manifest may differ. The source adapter pins
authorized objects for active readers. An unavailable object causes rediscovery.
A witness cannot serve a full snapshot; the core chooses a full source.

Epoch descriptors retain the current and immediate previous membership plus the
committed transition boundary. Their phase is the owner's local progress, not
evidence that the receiver has caught up. EPOCH_STARTED is sent to both old and
new groups only after the sender has recoverable state through the boundary;
full members have also rebuilt application/client state. A member that has
promised retransmits it unicast to an authenticated learner's START_EPOCH.
Handoff completion, for members and learners alike, combines distinct
current-member acknowledgments under DESIGN.md's quorum and full-member
retention rule; a learner's STEADY grants it no vote and obliges no one further.
A JOIN replica whose seed is stale learns the committed epoch from a seed
member's NEW_EPOCH, installs through the boundary, collects the same promises,
and returns to WARMING rather than RETIRED; it is admitted by a later
RECONFIGURE like a learner that joined at epoch 0. The epoch metadata remains
after handoff so a restart can recover the configuration and removal boundary.
A demoted full member keeps its donor image and materialized FULL role until
handoff permits release, even though voting and primary eligibility use the new
membership.

In durable mode the order is SNAPSHOT_SYNC, STORE publication/restoration, SYNC
of that store revision, then dependent trimming or participation. PUBLISH does
not rewind the running application or client table. RESTORE selects the indexed
base; INSTALL then establishes the application boundary before suffix replay.
A failed INSTALL cannot be reported as partial success. Successful CAPTURE/FETCH
objects remain available until DROP or STOPPED, including when a subsequent
transition makes their original purpose obsolete. References from retained store
revisions and independent adapter readers can keep objects alive beyond either.
A successful RECOVERY load on a materialized full replica acquires a local hold
on its recovered checkpoint. Local core holds are keyed by snapshot ID: repeated
successful FETCH of the same ID shares one hold and requires one eventual DROP, not a DROP for each FETCH.
The core does not FETCH an ID while its DROP is in flight. After a completed
DROP, a subsequent FETCH can establish a new hold. An adapter cleans up private
partial CAPTURE/FETCH objects after failure. Corruption reported by any snapshot
operation fences the instance with `VSR_FAILURE_SNAPSHOT`.

CHECKPOINT events and `checkpoint_interval` are coalescible scheduling hints,
not completion promises. They capture only eligible applied progress, and may
be deferred while replay, transfer, or another application fence is active, or
while retained inputs leave less than the capture baseline; deferral releases
only applied cache entries whose release restores that baseline and never
blocks a transition.
Witnesses ignore local capture hints. Observe `checkpoint_op` to track published
coverage; snapshot capture alone does not advance it.

For witnesses, a published checkpoint is a remote recovery anchor only; no local
application image or completed-client table is implied. Such anchors may be
published and trimmed against only with the retention guarantees in DESIGN.md.
They are rediscovered from full replicas when needed; witnesses do not issue
CAPTURE, SNAPSHOT_SYNC, or INSTALL for them. Coverage by `f + 1` full members
is required only for retained entries a RESTORE or TRIM discards; adopting an
anchor beyond an empty or shorter retained range needs none. Promotion must
first materialize a full checkpoint or replay the complete log. DROP releases only locally owned
snapshot objects after adapter readers drain, never another replica's anchor.

## Application operations and failures

APPLY contains one nonempty consecutive committed batch. Execute all entries in
order and return exactly one `vsr_value` per entry, each within `result_bytes`.
A `vsr_applied` whose `count` differs from the batch is a structurally valid but
inconsistent result: the completion is consumed and the replica fences with
`VSR_FAILURE_APPLICATION`. A count above `batch_entries` is instead rejected
unconsumed with `VSR_ELIMIT`.
Control entries are application no-ops with empty results. `through` is the last
entry's op. At most one APPLY is active; execution does not overlap read/capture
fences or INSTALL. Application result codes are deterministic data, including
application-level rejection; an unsuccessful completion indicates inability to
execute the batch and fences the replica.

Completion data is present only for successful LOAD (`vsr_loaded`), APPLY
(`vsr_applied`), and CAPTURE/FETCH (`vsr_checkpoint`). Every other completion has
NULL data and lease zero. Success of SEND/REPLY means buffer access ended, not
receipt by a peer or client. Successful RECLAIM/DROP ends the core's retention
obligation; independent adapter readers can delay physical reclamation.

| Operation or condition | Non-success handling |
| --- | --- |
| SEND | Retry/coalesce protocol work |
| REPLY / READ_READY | Abandon local delivery; client may retry |
| LOAD | Retry transient unavailability; expected absence follows load rules; corruption, unexpected absence, `FAILED`, and `CANCELLED` all fence storage |
| STORE / SYNC | Fence storage on every non-success, including cancellation/retry |
| APPLY / INSTALL | Fence application on every non-success; INSTALL corruption latches snapshot failure |
| CAPTURE | Retry ordinary failures; corruption fences snapshot state; no partial object is adopted |
| FETCH | Retry unavailability or discover another valid offer; corruption fences snapshot state |
| SNAPSHOT_SYNC | Fence snapshot durability on every non-success |
| RECLAIM / DROP | Retry cleanup; reported corruption fences the affected storage or snapshot state |
| Identity mismatch / exhausted counter / invariant violation | Fence immediately and latch the corresponding diagnostic |

`vsr_get_status` exposes the first fatal failure, its triggering operation and
completion status when applicable, and progress counters. Reaching FAILED never
turns an unsuccessful effect into protocol progress. Shape-invalid completions
are API errors and remain unconsumed; structurally valid but inconsistent
storage/application results fence the instance. Diagnostic information remains
available through STOPPED.

STOP is idempotent, disables deadlines, stops new protocol work, and abandons
unissued requests. Only TIME, COMPLETE, STOP, and drain calls are then accepted;
any other event returns `VSR_EINVAL` and stays unconsumed, in STOPPING and in
STOPPED alike. Finish or cancel every emitted operation and continue draining
RELEASE outputs; STOPPING starts no retries. FAILED and RETIRED also permit
draining and STOP but, until STOP, consume other well-formed inputs without new
protocol work. Recovery metadata retains its protocol state, not the transient
runtime failure/stop state.

STOPPED means no outstanding operations, pins, or queued releases remain.
`vsr_deinit` then succeeds without freeing the caller's arena; otherwise it
returns `VSR_EBUSY`. Shutdown does not promise a new durable checkpoint or resolve
uncertain client requests. At STOPPED the adapter owns any remaining cleanup;
published recovery data remains subject to store retention and durability.
A runtime restart still follows the recovery rules.
