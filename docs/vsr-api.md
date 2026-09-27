# VSR API sketch

Project-wide architectural principles are defined in [DESIGN.md](../DESIGN.md).

The public surface is in [include/vsr.h](../include/vsr.h). It describes a
replica engine and its contracts, not an implementation or a verified protocol.
The protocol reference is Liskov and Cowling's
[Viewstamped Replication Revisited](https://pages.cs.wisc.edu/~remzi/Classes/739/Papers/vr-revisited.pdf).
The logical storage interface, memory ownership, batching, and quorum read
barrier below are design choices for this library.

## Coverage

| Concern | Interface |
| --- | --- |
| Normal replication, heartbeats, retransmission | `PREPARE`, `PREPARE_OK`, `COMMIT`, `TIME`, batched log entries |
| View change | `START_VIEW_CHANGE`, `DO_VIEW_CHANGE`, `START_VIEW`; explicit last normal view and missing-log fetch |
| Crash recovery | Async recovery metadata/log loads, fresh recovery nonces, recovery responses, application replay |
| State transfer | Bounded log chunks, exact source revisions, checkpoint offers, out-of-core snapshot fetch/install |
| Reconfiguration | Logged reconfiguration and check-epoch requests; current/previous memberships, transition boundary, epoch messages, retirement |
| Joining and witnesses | Nonvoting warm-up; explicit full/witness roles; promotion requires application catch-up |
| Client semantics | Stable request identity, pending-request suppression, indexed completed-client table and cached replies, client recovery query |
| Application execution | Ordered committed batches, deterministic results, separate committed/applied/durable progress |
| Reads | Ordinary logged commands, fresh-quorum read fences, or explicitly weaker causal backup reads |
| Persistence | Incremental atomic logical transactions, suffix replacement, pipelined writes, group durability barriers |
| Checkpoints and compaction | Capture, persist, publish, fetch, install, trim, revision reclamation, snapshot deletion |
| Resource management | Fixed arena, bounded caches, input pins, bounded output arrays, partial batch consumption, stop/drain |

There is no client-side proxy, address discovery service, codec, storage engine,
application, or I/O executor in this header. These belong outside the replica
engine. Client and replica traffic can use different transports. Reads that
update access timestamps or otherwise change state must be logged commands.
There is no implicit time-based read lease or unsynchronized primary-local read.

## The driver boundary

`vsr_step(v, event, update)` remains the single-event entry point;
`vsr_step_many` amortizes dispatch and lets the engine form protocol and storage
batches. Supply a `TIME` event with the current monotonic time each loop turn,
including during sustained traffic. Other events use the last accepted time.
No protocol timeout runs until the first `TIME` establishes the clock origin.
Use the latest returned deadline to arm or replace one outer timer.

The driver owns the output array. Dispatch every returned operation before
reusing those slots; operation bodies remain valid until their completions are
accepted. Queue immediate completions while iterating the output, then feed
them back afterwards. `RELEASE` is a notification, not work needing completion.

Reply routes are process-local, generation-qualified adapter handles, not
replicated client identities. A stale/closed route fails delivery instead of
being reassigned to another client. An inherited log request with no local route
caches its result for the client's retry. `CLIENT_QUERY` replies use the largest
locally known request number; `VSR_REPLY_EXECUTED` distinguishes an available
cached result from a merely pending request. READ cookies also act as reply
routes for rejection, so they must be nonzero and follow the same rule.

Every return reports how many input events were accepted. Retain and retry only
the unconsumed suffix. Results already produced remain valid if a later event
is invalid. With valid output storage, invalid call arguments clear output
counts; null/invalid instance or output pointers are programming errors.

`MORE` or `OUTPUT_FULL` means call again without new input to drain work.
`INPUT_BLOCKED` without `MORE` means progress needs a completion, release of
resources, or a timer event; do not busy-spin on the same proposal. Prioritize
completions over blocked requests. Slow peers cannot cause unbounded buffering:
the engine must coalesce retry intent, bound outstanding sends, and load older
entries from storage when needed.

The arena planner must reserve enough space to consume each already-issued
operation's bounded completion, return every accepted input lease, and finish
shutdown even when new request admission is saturated. Work can be continued
over several drain calls, but accepted work must not be dropped. Incoming
messages may be deferred or retransmitted under pressure; progress capacity
must be reserved independently of new application requests. Invalid or
oversized input is distinguished from temporary pressure.

## Memory and cache layout

On the tested 64-bit ABI, events, operations, and updates are 32 bytes; message
envelopes and log-entry headers are 64 bytes; member records are 16 bytes.
The layout checks live in [tests/header.c](../tests/header.c). These sizes are
not a wire ABI, an architecture-independent cache-line claim, or a measured
performance result.

Use contiguous arrays and slab indices internally. Place frequently accessed
replica state separately from membership, recovery, transfer, and checkpoint
state. Align the arena and hot array bases using the requested cache-line size;
avoid padding every small descriptor to a full line. Separate per-instance
arenas when different threads own different replicas. There is no shared queue
or atomic reference counting requirement inside the single-owner core.

`vsr_layout`/`vsr_init` replace heap allocation. Limits bound metadata and active
working sets, not the total indexed log or client table. Large command, reply,
and snapshot bytes remain in adapter-managed pools. The core may copy small
headers into its arena; it does not secretly flatten or copy payload buffers.
Scatter/gather spans can reference receive buffers, mmap regions, or registered
I/O memory. The same immutable payload can back several peer sends and a store
transaction. A backend may still need a copy for its chosen codec or lifetime;
the interface does not force that copy.

An accepted event with a body transfers a lease over the entire reachable
object graph. Returning from `step`, accepting a proposal, or completing one
send does not release that input. Only the matching `RELEASE` does. Shared
allocations use separate leases and adapter-side reference counts. Avoid a
stack-allocated body unless its lifetime really extends to release. Large
receive buffers can remain pinned by a small surviving slice; choose pool and
lease granularity accordingly. The payload-byte limit counts logical referenced
bytes; the adapter must also account for actual backing allocation sizes.

Operation body pins and input leases have different lifetimes. The core can
release an operation after its completion while still retaining an input lease
for another send, a cached reply, or an unpersisted log entry. Conversely, it
can evict persisted payloads and later request them with `LOAD`, returning input
leases without keeping an entire history in RAM.

The adapter may acquire independent references to its own payload buffers
before completing a store operation, allowing zero-copy retention in an
in-memory store or cache. Completing the operation ends reliance on the core's
pin; it need not destroy an independently owned backing allocation. Core-arena
descriptors cannot be retained this way and must be consumed or copied first.

## Storage and application progress

The adapter supplies an indexed logical store: recovery metadata, log ranges,
and completed client records. It can implement that store using a WAL, immutable
segments, a page tree, an in-memory map, or another design. The core chooses
logical changes and their ordering; the adapter chooses their representation.
`VSR_REPLICATED` still needs this readable store, which may be entirely volatile.

For example, the engine may emit `STORE(41)`, `STORE(42)`, and `SYNC(42)`.
The adapter can serialize both transactions into one write and flush, or use
parallel writes. It reports each operation separately. A `STORE` completion
means the transaction is readable and no longer depends on its operation pin;
a `SYNC` completion certifies the whole durable prefix, not just the last write.
The engine tracks readable and durable prefixes despite out-of-order completions.
It may submit `SYNC` before earlier store completions, so the adapter implements
the barrier dependency. Durable protocol messages wait in the core until all
necessary durability and completion conditions hold.

Transactions atomically combine logical suffix truncation/replacement, view and
epoch fences, client results, and checkpoint references as needed. Their sequence
numbers are unrelated to consensus operation numbers. Recovery exposes a valid
complete transaction prefix. Torn transactions and later dependent records are
discarded by the adapter; acknowledged durable state must never be discarded.
Compaction cannot erase the fencing metadata needed to recover safely.

`LOAD_RECOVERY` ignores its input sequence and returns the recovered sequence;
other loads read the requested revision. `LOAD_LOG` returns consecutive entries
starting at `first`, within both limits, and advances `next`; it must not return
an empty nonterminal success. An empty range is valid. `LOAD_CLIENT` returns
zero or one completed record. Older log ranges already compacted can return
`NOT_FOUND`, allowing checkpoint transfer instead of treating that as an empty
log. Unexpected holes or corrupt safety state must not become successful reads.

Store revisions are immutable logical views, not mandatory physical copies.
`RECLAIM(min_sequence)` permits collecting older versions after external readers
drain, without deleting data still reachable from a retained version. The core
must retain revisions needed by outstanding loads and checkpoint capture. Exported
log offers have bounded retention; an expired offer receives `STATE_UNAVAILABLE`
with its echoed nonce and a current offer. The receiver revalidates the new offer
under its recovery/view-change rules instead of substituting a different log.

`APPLY` executes an ordered committed batch. Application result codes are data;
execution failure is an unsuccessful completion and halts the replica. Control
entries occupy log positions but are application no-ops; the core handles their
protocol effects and replies. Only full replicas execute commands. No reply is
released before its operation has committed and its result is available.

Application progress in RAM is not durable application progress. On restart,
install the checkpoint and execute every committed suffix entry in order,
including entries whose reply records survived. Never deduplicate away replay
needed to rebuild application state. The engine suppresses external replies
until the relevant application state has caught up. Arbitrary external side
effects still require application-level transactional/idempotency discipline.

## Checkpoints and transfer

Capture freezes application state and the indexed client-table revision at one
exact operation boundary. No apply/install overlaps that capture. After the
capture completes, execution resumes while the snapshot is copied or flushed.
The adapter includes the client table, epoch/transition metadata, and application
state in one recoverable snapshot. The capture task identifies the exact store
revision from which to obtain metadata; it must not use a moving current table.

In durable mode, snapshot contents become durable before a store transaction
publishes the recovery anchor, and that publication is itself made durable
before dependent log trimming. Publishing a locally captured checkpoint does
not rewind the running application or client table. Installing a received
checkpoint is different: the core atomically selects a matching logical store
base, preserves necessary view/epoch fences, then installs the application and
replays the selected suffix. It does not acknowledge catch-up prematurely.

Checkpoint transfer is a logical `FETCH` operation naming a peer, target, and
optional local basis. The adapter owns bulk transport, page/block selection,
validation, and local materialization. It must expose a readable immutable
snapshot on success, without passing every byte through `step`. The source
adapter serves only authorized snapshot objects and pins them during active
reads. If an advertised object has expired, fetch fails and the core negotiates
a current offer. Local manifests may differ between machines; snapshot identity,
operation boundary, epoch metadata, and logical contents must match.

Witnesses never serve application state they do not hold. Witness log trimming
must preserve reconstructability: enough full members must retain recoverable
checkpoints covering the trimmed prefix to survive the configured faults.
Checkpoint advertisements allow the core to track this. Revision reclamation,
logical log trimming, and snapshot deletion are distinct operations.

## Membership, recovery, and reads

Reconfiguration is a replicated request ending the old epoch. A proposed
configuration must use exactly the next epoch, fit capacity, and satisfy quorum
and role constraints. Only one transition proceeds at a time. The old group
commits the boundary, and the new group obtains the state through it before
participating. Removed replicas continue serving transfer until the required
new-group quorum reports readiness. A committed configuration alone is not
permission to retire its donors; a check-epoch request provides an administrative
completion fence. Uncommitted reconfiguration at a selected log's tail still
blocks later old-epoch requests after a view change.

JOIN warms a node without counting its votes. Changing a witness into a full
member requires materializing application state before it can execute or lead.
Member identity is stable across configurations and never interpreted as an
array index. Transport addresses and permission to request reconfiguration or
warm-up state transfer are adapter/deployment responsibilities.

Durability policy is cluster-wide and immutable in this sketch. Durable mode
fences protocol actions with recoverable storage. Replicated mode allows the
normal protocol to proceed without foreground disk durability; restart must
complete quorum recovery, using a fresh attempt nonce and the applicable
primary's state. Stale or missing local storage is only a catch-up hint. Both
modes block voting while state recovery is incomplete and handle epoch changes
during recovery. A previously removed node cannot use bootstrap to rejoin.

Client recovery queries return the latest locally known request number, including
pending log state, and a cached result when available. This is not a linearizable
read of arbitrary application data. A client preserving its incarnation must
recover its sequence using the client recovery protocol; alternatively it can
use a new globally unique incarnation. Duplicate suppression persists across
cache eviction, checkpoints, and membership changes. Missing older replies
must produce a stale-request response, never another execution.

Before its first unlogged read in each view, a primary must commit a current-view
no-op after the entire inherited log. Otherwise an old primary's acknowledged
write could survive in that log but remain unapplied at the new primary, allowing
a stale read even after a fresh quorum probe. This establishment barrier is
amortized across subsequent reads in the same view.

Linearizable read fences then use a fresh nonce, epoch, view, and post-request
quorum confirmation. Only an established primary can initiate them; acknowledgers
must still be normal in that epoch/view. View/epoch changes invalidate unfinished
rounds. Wait for the confirmed commit floor and requested minimum to be applied,
then fence application mutation until the caller captures the read snapshot.
This quorum-read extension needs correctness tests in the implementation; merely
being the apparent primary is insufficient. Causal reads omit the quorum and
promise only the requested applied prefix. A read can return a normal rejection
`REPLY` using its cookie as the route and a zero client ID, or `READ_READY` on
success; the outer layer executes and returns the actual read result.

## Failures and implementation validation

Send failures cause protocol retry. Read/transfer unavailability causes retry
or renewed state discovery. Failed or ambiguous store/sync, snapshot durability,
application install, or application execution must fence the replica into
`FAILED`; they cannot be treated as successful progress. Cleanup failures can be
retried without changing consensus. Failure and retirement still accept
completions, releases, time updates, and STOP so resources can drain. STOP stops
new protocol work, disables deadlines, and eventually returns all input leases.
The adapter finishes or cancels every emitted operation before deinitialization.
Identifiers/counters must never wrap and silently reuse an earlier identity.

The supplied checks verify header usability and descriptor layout only:

```sh
cc -std=c11 -Wall -Wextra -Werror -pedantic -Iinclude -fsyntax-only tests/header.c
c++ -std=c++11 -Wall -Wextra -Werror -pedantic -Iinclude -x c++ -fsyntax-only tests/header.c
```

A future implementation needs deterministic simulations of loss, duplication,
reordering, view changes during recovery/reconfiguration, disjoint membership
handoffs, witness promotion, stale read probes, and checkpoint installation.
Storage testing must crash at every transaction/flush/publish boundary, including
out-of-order completions. Saturated queues and small arenas must still drain
completions and release every pin exactly once. Benchmarks, not header layout
alone, must establish allocation behavior, copy volume, throughput, and latency.
