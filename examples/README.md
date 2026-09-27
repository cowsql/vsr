# Runnable protocol examples

These programs use the real library and a deterministic in-memory host. They
construct public `vsr_event` objects, drive the resulting effects, and assert the
observable protocol state. They perform no socket or disk I/O.

From a source checkout:

```sh
./bootstrap
mkdir -p build/examples
cd build/examples
../../configure
make examples/replication examples/failover examples/checkpoint examples/epochs examples/reads
./examples/replication
./examples/failover
./examples/checkpoint
./examples/epochs
./examples/reads
```

| Program | Behavior demonstrated |
| --- | --- |
| [replication.c](replication.c) | Three replicas commit a command; retrying the same client ID and request number returns the same result. Runs both durability policies and prints their separate readable/durable revision frontiers. |
| [failover.c](failover.c) | The primary crashes, logical time triggers a view change, the next primary commits another command, and the original replica restarts with a fresh incarnation and catches up. |
| [checkpoint.c](checkpoint.c) | A full replica captures and publishes a checkpoint. A new nonvoting learner fetches that image and rebuilds through its boundary. |
| [epochs.c](epochs.c) | Three learners replace a completely disjoint old group. Each group has two full replicas and one witness. The old donors retire after the new group's handoff promises; a logged `CHECK_EPOCH` confirms readiness. |
| [reads.c](reads.c) | A fresh linearizable barrier yields `READ_READY`. Holding that effect keeps application state fixed while another write commits. Completing it permits application progress; a backup then serves a causal barrier at the caller's minimum operation. |

The commands are opaque bytes interpreted by the sample deterministic application
in the host. Text such as `set counter 7` is illustrative; this application
records commands and returns a deterministic checksum instead of parsing a
key-value language.

## The shared host

[common.h](common.h) contains small example helpers. The host implementation is
shared with the tests:

- [tests/lib/memory_cluster.c](../tests/lib/memory_cluster.c) drives event and
  completion queues, delivers peer messages, supplies logical time, simulates
  crashes, and implements the application/snapshot effects.
- [tests/lib/memory.c](../tests/lib/memory.c) implements immutable logical store
  revisions, client/request indexes, and object graph cloning.
- [tests/lib/memory_cluster.h](../tests/lib/memory_cluster.h) exposes the host
  controls used by these programs.

This is a reference/testing host. It deliberately clones input/output graphs and
retains execution history for agreement checks. Those oracle and copying costs
are not required by the core, and these programs are not performance benchmarks.
The core itself continues to use the caller-provided fixed arena and borrowed
payload leases.

`mem_node_event` copies its input graph and substitutes a fresh lease ID. That is
why a stack-local request and the placeholder lease `1` are valid in these
programs. A production adapter instead supplies a unique lease covering an
immutable graph and preserves it until the corresponding `VSR_OP_RELEASE`.

A production driver's loop follows the same public contract:

1. Submit events with `vsr_step_many`, retaining the unconsumed suffix.
2. Dispatch every returned effect. `SEND` delivers a peer message; `LOAD`,
   `STORE`, `SYNC`, and snapshot effects use the indexed persistence adapter;
   `APPLY` calls the deterministic application.
3. Complete each effect exactly once, after all accesses to its borrowed data
   finish. Transport completion does not count as a replication acknowledgment.
4. Consume `RELEASE` outputs without issuing completions for them. Drain when
   `MORE` is set, and supply monotonic `TIME` events at the returned deadline.
5. On shutdown, submit `STOP`, finish/cancel outstanding effects, drain releases,
   and deinitialize only after `STOPPED`.

`STORE` completion makes a transaction readable. In durable mode, `SYNC` must
also establish the necessary durable prefix before dependent votes, execution,
or replies. Replicated mode uses readable storage for normal operation and a
fresh recovery quorum after restart. The host implements both paths and checks
their prerequisites when effects are issued.

The examples keep assertions enabled even in release builds. An assertion
failure identifies a violated demonstration expectation or adapter invariant;
production integrations should handle application delivery and I/O failures
according to [the adapter contract](../docs/vsr-api.md).
