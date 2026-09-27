# VSR design principles

Build a small, complete, efficient C engine for Viewstamped Replication.
Target crash failures and networks that delay, lose, duplicate, or reorder messages.
These principles guide the project; the [header](include/vsr.h) and
[API notes](docs/vsr-api.md) express the current design, whose details may evolve.

## Founding requirements

The user established four requirements:

- Cover VSR's full lifecycle, including membership changes.
- Use one thread per instance and an asynchronous event-to-update step function,
  suitable for an io_uring-style driver without depending on that runtime.
- Keep actual network and disk I/O outside the core; allow efficient outer
  layers to choose their own mechanics and data formats.
- Design for performance and cache locality, avoiding significant unnecessary
  allocations and data copies.

## Derived principles

1. **Deterministic core, explicit environment.** Make execution reproducible
   from configuration, events, and call boundaries. Inject time,
   randomness, requests, and completions. Perform no I/O, blocking waits,
   callbacks, or hidden allocation. Keep transport, codecs, discovery,
   authentication, routing, and application execution in adapters.

2. **Separate every guarantee.** Admission, preparation, commitment, application,
   readable storage, and durability are distinct milestones. Local completion
   does not imply remote acknowledgment. Enforce dependencies explicitly;
   tolerate reordered completions. Distinguish application results, retriable
   failures, and failures requiring the replica to stop participating.

3. **Specify logical storage and recovery guarantees.** Express incremental,
   ordered, atomic changes and exact revision reads. Separate writes from
   durability barriers. Recover a complete transaction prefix. Make the
   cluster's durability policy explicit and fixed across epochs. Durable mode
   gates dependent actions on persistence; replicated mode requires quorum
   recovery after restart.
   Host structs impose no wire or disk layout.

4. **Expose useful concurrency.** Batch dispatch, replication, execution, and
   persistence; pipeline independent work and permit group flushes. Bound
   batching delay to protect latency. Keep bulk snapshot copying and transfer
   outside the engine; fence application mutation only when consistency needs it.

5. **Keep the working set compact.** Use caller-provisioned arenas, contiguous
   metadata, appropriate alignment, and separation of frequently accessed fields
   from large or infrequent data. Bound caches independently of stored history.
   Support scatter/gather and shared immutable payloads. Small metadata copies
   are acceptable; cache-line sizes and descriptor widths are target-dependent.

6. **Make lifetime explicit.** Accepted inputs carry immutable pins over their
   complete reachable data; release them explicitly. Operation data survives
   until its matching completion. Allow independent adapter references to
   payload buffers. Distinguish durable identities, process-local routes, and
   completion tokens; prevent stale references, accidental reuse, and counter wrap.

7. **Bound resources while preserving progress.** Limit per-call work, queues,
   caches, and pinned bytes. Report the exact accepted input prefix and apply
   backpressure before admission. Reserve capacity for completions, releases,
   and shutdown even under saturation. Never silently drop accepted work;
   drain or cancel external accesses before reclaiming memory.

8. **Preserve safety through every lifecycle transition.** Model view change,
   recovery, joining, witnesses, membership handoff, and retirement explicitly.
   Require appropriate quorums and catch-up before participation or execution.
   Retain old donors until handoff is safe. Checkpoints bind application,
   client, and protocol state to one boundary; publish safely before trimming.
   Reclamation must preserve recoverability. Lost state never implies bootstrap.

9. **Preserve application and client semantics.** Execute committed commands
   deterministically in order, with nondeterministic inputs already recorded.
   Stable request identities and retained client results support retries across
   failures and cache eviction. Deduplication must not suppress reconstruction
   after a checkpoint. Application-side transactions or idempotency govern
   external side effects.

10. **State read consistency explicitly.** Linearizable reads require an
    established view, fresh quorum confirmation, and an applied-state fence.
    Label weaker causal reads accordingly. Primary status alone grants no read
    authority; introduce no implicit clock-synchronization or lease assumptions.

11. **Keep complexity accountable.** Use the smallest mechanism satisfying these
    contracts. Validate safety and progress with deterministic fault simulation
    and crash-boundary tests; validate performance with allocation, copying,
    cache, throughput, and latency measurements. Declarations and layout checks
    establish neither protocol correctness nor speed.

Preserve safety and explicit semantics, keep execution and resources bounded,
then optimize measured performance with the least added complexity.
