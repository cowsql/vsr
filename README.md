# VSR

VSR is a C11 library for Viewstamped Replication: one ordered history of
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

Include [include/vsr.h](include/vsr.h). The header supports C11;
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

Build from Git and run the tests with Clang, ASan and UBSan:

```sh
./bootstrap
mkdir -p build/asan
cd build/asan
../../configure --enable-werror
make -j"$(nproc)" check
```

The build produces `src/libvsr.a` and five runnable [in-memory examples](examples/README.md)
in `examples/`. `make check` runs unit tests, public protocol/adapter scenarios,
deterministic seeded fault simulations, and the examples. The simulator checks
agreement, storage prerequisites, buffer immutability, and eventual progress
after faults stop. Seeds and optional action traces make failures replayable.

For an uninstrumented installation, configure a separate build with
`CFLAGS='-O2 -g' ../../configure --disable-sanitize --prefix=/your/prefix`, then
run `make check` and `make install`. The installation includes `vsr.h`,
`libvsr.a`, and a `vsr.pc` file for `pkg-config`. Sanitizers default to enabled
for development; use the uninstrumented build when linking ordinary consumers.

Arena size depends on the explicit capacity limits, independently of stored
history and payload backing allocations. Batching, output capacity, durability,
cache sizes, scatter/gather limits, and checkpoint policy remain caller choices.
`make benchmark` measures arena use and core step timings separately from the
instrumented host; see [benchmark notes](tests/benchmark/README.md).

See [development](docs/development.md) for dependencies, compiler options,
formatting, static analysis, Valgrind, coverage, fuzzing, and distribution checks.
The [test layout](tests/README.md) has unit, integration, fuzzing and fault
simulation, regression, and benchmark layers.
