# Runnable protocol examples

These programs use the real library and a deterministic in-memory host. Each
one is a narrated walkthrough of one feature: it prints what is happening in
plain lines and asserts the expected protocol state with `CHECK`, so the same
program is both a demonstration and a test. They perform no socket or disk I/O.

From a source checkout:

```sh
./bootstrap
mkdir -p build/examples
cd build/examples
../../configure
make examples/replication examples/failover examples/checkpoint \
     examples/epochs examples/reads examples/witness
./examples/replication
```

Read a program top to bottom: the header comment states the scenario and what
to look for in the output, and the body follows the same steps. Sample output
from `replication`:

```text
== replication (durable storage) ==
group: replica 1 (full), replica 2 (full), replica 3 (full); f = 1, quorum = 2, durable storage
replica 1 is primary in view 0
client A sends "set counter 7" to replica 1 -> OK "7" (op 1)
all 3 replicas now hold counter=7
client A retries "set counter 7" to replica 1 -> OK "7" (op 1)
the retry was answered from the client table: op 1 again, nothing re-executed
client A sends "incr counter" to replica 1 -> OK "8" (op 2)
```

| Program | Behavior demonstrated |
| --- | --- |
| [replication.c](replication.c) | Three replicas commit commands in order. A client retry with the same client ID and request number gets the cached reply without re-execution. Runs under both durability policies and prints their readable/durable storage frontiers. |
| [failover.c](failover.c) | The primary crashes; once logical time passes the view timeout the backups elect replica 2, which commits the next command. The crashed replica restarts with a fresh incarnation, recovers, and rebuilds its store by replaying the log. |
| [checkpoint.c](checkpoint.c) | The primary captures and publishes a checkpoint whose image is the serialized store. A new nonvoting learner fetches and installs that image, and the primary itself reinstalls it after a crash, both without executing a command. |
| [epochs.c](epochs.c) | Three learners replace a completely disjoint old group (two full replicas and a witness each). The old donors retire after the new group's handoff promises; a logged `CHECK_EPOCH` certifies readiness, and the state written in epoch 0 is readable in epoch 1. |
| [reads.c](reads.c) | A linearizable read barrier yields `READ_READY`. While the host holds that effect, a newer command commits but is not applied, so the state being read cannot move. Completing it releases `APPLY`; a backup then serves a causal read at the caller's minimum op. |
| [witness.c](witness.c) | Two full replicas and one witness. The witness votes (its `PREPARE_OK` messages are counted while a full replica is down) but never receives `APPLY` and holds no application state. |

## The sample application

The core treats commands and results as opaque bytes. The examples plug a tiny
deterministic key-value store, [kv.c](kv.c), into the host through the
`mem_application` callbacks in
[tests/lib/memory_cluster.h](../tests/lib/memory_cluster.h). Commands are short
text and so are results:

| Command | Result |
| --- | --- |
| `set counter 7` | `7` |
| `get counter` | `7`, or `(nil)` when absent |
| `del counter` | `1` if a key was removed, else `0` |
| `incr counter` | the incremented integer; an absent key counts as `0` |

`SNAPSHOT_CAPTURE` serializes the map as `key=value` lines and that text is the
checkpoint image; `SNAPSHOT_INSTALL` parses it back. The store also counts the
commands it executed itself, which is how the programs tell state rebuilt by
replay (`failover`) from state installed from an image (`checkpoint`). Without
a registered application the host falls back to a checksum of every command,
which is what the test suite uses.

## The shared driver

[common.h](common.h) and [common.c](common.c) turn host calls into the steps
the programs narrate: `example_start` builds a group and reports its primary,
`example_call` submits a command as a named client and prints the reply,
`example_elapse` advances the logical clock, and `example_crash`,
`example_restart`, `example_join`, and `example_checkpoint` do what their names
say. The host itself is shared with the tests:

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
why a stack-local request and the placeholder lease `1` are valid in
`common.c`. A production adapter instead supplies a unique lease covering an
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
