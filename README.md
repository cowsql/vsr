# VSR

VSR is a C interface for Viewstamped Replication: one ordered history of
commands, replicated across a group that can recover from crashes and change
membership.

The core is deterministic, single-threaded, and asynchronous. An application
supplies events and receives operations to execute. The core performs no I/O,
reads no clocks, invokes no callbacks, and allocates no memory. Transport,
storage, application execution, and scheduling belong to the host.

## Execution model

```text
requests · peer messages · time · completions
                     │
                     ▼
                 vsr_step_many
                     │
                     ▼
sends · replies · storage · application · snapshots · buffer releases
                     │
                     └──────── host executes and reports completions
```

One owner drives each instance. Network, storage, and snapshot work can run
concurrently outside it; completions return to the owner as events. Batches
amortize dispatch and persistence. Scatter/gather payloads and explicit buffer
leases allow shared immutable data without requiring copies. A caller-provided
arena bounds core memory independently of stored log and client history.

The API covers replication, view changes, recovery, membership handoff,
nonvoting warm-up, witnesses, duplicate suppression, checkpoints, and state
transfer. Reads can be logged commands, linearizable quorum barriers, or local
reads constrained by an applied-operation minimum.

## Integration

Include [include/vsr.h](include/vsr.h). The header supports C11 and C++11;
its structures are logical host objects, not wire or disk formats.

1. Define replica identities, membership, durability policy, and resource limits.
2. Call `vsr_layout`, provide the requested aligned arena, and call `vsr_init`.
3. Feed requests, messages, monotonic time, and operation completions through
   `vsr_step` or `vsr_step_many`.
4. Dispatch every returned operation. Retry only unconsumed input; drain ready
   work when instructed and arm the returned deadline.
5. Submit `STOP`, finish or cancel issued operations, process every buffer
   release, and call `vsr_deinit` after the instance reaches `STOPPED`.

Payloads remain immutable until their explicit release. Every operation except
`RELEASE` requires one completion, including failed submissions and cancellations.
A send completion releases buffers; a peer acknowledgment advances replication.

A group with `n` members and failure threshold `f` requires `n >= 2f + 1` and uses
quorums of `n - f`. At least `f + 1` members execute application commands;
witnesses store protocol state and log entries. `VSR_DURABLE` persists safety
state before dependent protocol actions. `VSR_REPLICATED` relies on surviving
replicas and requires quorum recovery after every restart.

Reconfiguration ends an epoch at a committed log entry and transfers that history
to the next group. A successful `CHECK_EPOCH` for the target epoch certifies that
the old donors can retire. Client retries retain their original request identity
and body across redirects. Full replicas preserve duplicate suppression in
indexed storage and checkpoints, independently of the in-memory cache.

## Documentation and checks

- [Design](DESIGN.md): assumptions, invariants, and performance constraints.
- [Public header](include/vsr.h): types, lifetimes, limits, and function contracts.
- [Protocol contract](docs/protocol.md): quorums, message validation, recovery,
  reads, and membership transitions.
- [Adapter reference](docs/vsr-api.md): driving, storage, snapshots, and failures.

Run the interface checks with:

```sh
make check
```

These checks verify C11/C++11 compatibility, function signatures, and descriptor
layouts. The design specifies the separate protocol and performance validation
requirements.
