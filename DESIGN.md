# Design

VSR separates a deterministic replication state machine from its environment.
The core decides which actions are safe and necessary; the host executes those
actions and reports their outcomes. The public contract is
[include/vsr.h](include/vsr.h), with integration details in the
[adapter reference](docs/vsr-api.md).

The protocol foundation is Liskov and Cowling's
[Viewstamped Replication Revisited](https://pages.cs.wisc.edu/~remzi/Classes/739/Papers/vr-revisited.pdf).
Logical storage transactions, explicit ownership, bounded scheduling, and fresh
quorum read barriers are library contracts. The
[protocol contract](docs/protocol.md) specifies their interaction with recovery,
membership changes, and witness retention.

## Assumptions

Replicas fail by crashing. Messages can be delayed, lost, duplicated, or
reordered. Adapters authenticate peers, reject malformed encodings, preserve
message contents, and enforce administrative authorization. Byzantine replicas
and storage that falsely acknowledges durability are outside the model.

Commands and their results are deterministic from the same initial application
state. Time, randomness, and other nondeterministic choices enter as command
data. Timeouts affect progress, not safety. Progress requires an available
quorum, an eligible full replica, communication and execution delays eventually
within the configured timeouts, and continued service of time events and external
operations. At most `f` members may have lost unrecovered safety state; successive
restarts do not reset that failure budget.

A configuration has `n >= 2f + 1` members and quorum size `n - f`. At least
`f + 1` are full replicas; witnesses never lead or execute commands. Membership
is sorted by stable ID. View `v` selects full replica `v % full_count` in that
order. Views reset at each epoch; committed operation positions never reset.

## Deterministic execution

One owner serializes every call on an instance, including observation. The core
has no threads, atomics, callbacks, I/O, clock reads, random source, or allocator.
Configuration, event contents and order, step boundaries, and output capacities
fully determine logical outputs. Pointer addresses and physical encodings are
not part of replay equivalence. Instances share no mutable core state.

Work is bounded by configured limits and a per-call transition budget. A step
consumes an exact input prefix, emits bounded output, and reports whether to
drain, wait for resources, or wake at a deadline. Different batching boundaries
may change scheduling and grouping while preserving protocol guarantees.

## Replication and recovery

A replica acknowledges only a contiguous prepared prefix. A primary commits
only after the required quorum has prepared that prefix in the current epoch
and view. Full replicas execute committed entries in order; commitment,
application, readable storage, and durable storage are separate progress values.
Every quorum counts distinct members of one configuration. The local replica
counts only after satisfying the same state and persistence requirements as a
remote voter. Witnesses vote but never supply application results.

View change selects a log by its last normal view, then its last operation,
and preserves the greatest known committed prefix. Bounded log offers are
metadata about complete histories: missing ranges must be fetched before the
selected state can be used. An expired offer requires protocol revalidation,
not substitution of unrelated current data. Committed entries cannot be
truncated or overwritten.

Durability policy is fixed for the cluster:

| Policy | Before relying on local safety state | After restart |
| --- | --- | --- |
| `VSR_DURABLE` | Store and synchronize the necessary state | Recover a complete durable prefix, rebuild application state, and catch up |
| `VSR_REPLICATED` | Keep state in the readable logical store | Obtain fresh recovery responses from a quorum of other replicas, including the primary of the highest reported view |

A recovering replica contributes no vote to its own recovery. Missing durable
storage also requires quorum recovery; corruption fences the replica. Neither
case authorizes creation of a new cluster. Replicated mode cannot recover from
loss of all sufficiently recent copies. A zero-fault configuration has no spare
replica with which to complete quorum recovery after state loss.

## Storage and application state

The adapter maintains immutable logical revisions of an indexed store. Atomic
transactions append or replace suffixes, update client records and protocol
metadata, and publish checkpoint references. Transaction sequences are local
storage identities, independent of consensus positions. Recovery exposes a
complete transaction prefix, including the immutable cluster/replica identity.

Writes can be pipelined and physical completions reordered. A synchronization
operation certifies a durable transaction prefix. The core advances dependent
protocol work only after the relevant completions and durability barriers.
Revision retention, logical log trimming, and physical snapshot deletion have
separate lifetimes.

A full checkpoint binds application state, completed client records, and epoch
metadata to one operation boundary. Capture briefly fences application mutation;
copying, transfer, and persistence proceed asynchronously afterwards. Durable
contents precede durable publication, which precedes dependent trimming.
Installing a checkpoint selects a matching store base without weakening current
view or epoch fences, then restores the application and replays its suffix.
Restoration retains validated suffix entries by reference, so transaction size
does not grow with history. Missing entries arrive in bounded subsequent batches.

Replay executes every committed entry after the checkpoint, including requests
whose result records survived the crash. Duplicate suppression must not skip
application reconstruction. External side effects require application-level
transactions or idempotency; replication alone cannot atomically update an
unrelated external system.

## Membership and state availability

Reconfiguration is the final logged request of its epoch. Once proposed, it
blocks later old-epoch proposals until resolved, including after view change.
The old group commits the boundary; the new group obtains that history before
participating. Only one handoff is active. Nonvoting warm-up reduces the transfer
required at the boundary. A witness promoted to full must first reconstruct
application and client state.

A reconfiguration reply acknowledges the boundary, not permission to stop old
donors. `CHECK_EPOCH` is a logged request in the new epoch whose reply waits for
handoff readiness. Readiness requires a new-group quorum and at least `f + 1`
new full members with recoverable state through the boundary. Removed replicas
serve transfer until this condition holds, then become `RETIRED`. With witnesses,
this conservative retention rule can delay retirement while a full member is
unavailable, even when normal commands can commit.
`CHECK_EPOCH` has an immutable target independent of its routing epoch. Its
successful result cannot be executed, cached, or replied to before that handoff
is safe. Another reconfiguration is admitted only after the current handoff is
complete. Demoted full members retain their donor state until then; voting and
primary eligibility follow the new membership immediately on entering its epoch.

Full replicas retain a checkpoint plus its subsequent log. Witnesses can trim a
prefix only after at least `f + 1` full members in the responsible configuration
advertise retained checkpoints covering it. Advertisements are retention
promises: a checkpoint may be replaced by a later recoverable checkpoint, but
coverage cannot be withdrawn before safe handoff. Witnesses retain remote
checkpoint anchors as protocol metadata and never advertise ownership of
application snapshots. Recoverability must survive the configured failures
before any trim, deletion, demotion, or donor retirement.

## Client and read semantics

Each client incarnation has one outstanding request and monotonically increasing
request numbers. Retries preserve identity, type, and body. Completed records
and the retained-request index suppress duplicates across cache eviction,
restart, and reconfiguration. Only the latest completed result per client must
be retained. Older requests receive a stale response and never execute again.
Local client queries describe local knowledge; they cannot safely allocate a
replacement request number after a client loses its own sequence state.

An unlogged linearizable read requires a normal primary that has committed a
current-view entry after its inherited log. An existing command can establish
this fence; otherwise the core proposes a no-op. After admitting the read, it obtains
fresh quorum confirmation tied to a nonce, epoch, view, and commit floor. It
waits for that floor and the requested minimum to be applied, then fences
application mutation while the host captures a read snapshot. An unfinished
round is invalidated by a view or epoch change. There are no clock leases.

A causal read can use a normal full backup and guarantees only its requested
applied minimum within this cluster. Clients carry operation positions from
completed writes and read fences to preserve their dependencies. Reads that
mutate application state must be logged commands.

## Memory, scheduling, and failure

The caller supplies one fixed arena. Metadata uses contiguous arrays and slab
indices, with frequently accessed state separate from transfer and checkpoint
state. Align hot array bases to the configured cache-line size; do not pad every
small descriptor to a cache line. The core may copy small metadata but does not
flatten payloads or retain the entire history in memory.

Immutable input leases and operation pins make buffer lifetime explicit.
Persisted data can leave the caches and return through bounded indexed loads.
Large payloads stay in adapter-owned pools and may back several concurrent sends
and stores. Payload quotas count logical bytes; adapters also bound actual
allocations, registered buffers, transport queues, and disk usage.

Admission reserves resources for issued operations' bounded completions, lease
release, and shutdown. Slow peers coalesce retry intent instead of growing
unbounded queues. Storage and application batches share work without imposing a
flush per entry; intentional batching delay is bounded. No accepted work is
silently lost to resource exhaustion.

Ambiguous storage, execution, installation, or snapshot-durability failures
fence participation and latch a diagnostic. STOP disables protocol activity and
drains ownership; it does not promise commitment or persistence of pending
requests. No instance memory is reclaimed before outstanding accesses end.

Validation requires deterministic network fault simulation, crash injection at
transaction and checkpoint boundaries, and exhaustive ownership accounting under
small arenas and saturated queues. Safety cases include stale read probes,
recovery during view change, disjoint membership handoff, and witness promotion.
Allocation, copying, cache behavior, throughput, and latency require measurement;
public structure sizes alone are not performance evidence. The validation matrix
also covers maximum-size commands/results under completion backpressure,
checkpoint restore with a suffix larger than one transaction, duplicate
CHECK_EPOCH requests across redirects, and demotion during disjoint handoff.
