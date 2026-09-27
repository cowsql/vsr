# Test layers

All tests, drivers, fixtures, corpora, and benchmarks belong under `tests/`.
The split takes inspiration from the sibling Raft project. Fuzzing and
seeded fault simulation share one directory.

| Directory | Purpose | Current state |
| --- | --- | --- |
| `unit/` | Small deterministic tests of private modules | Arithmetic, equality, RNG, runtime, graph validation, reads, host storage |
| `integration/` | Public API and host adapter contracts | Replication, resource minima, reads, checkpoints, epoch handoff |
| `fuzzy/` | Coverage-guided fuzzing and seeded cluster/fault simulation | Arithmetic, graph, and cluster-scenario libFuzzer harnesses; seeded cluster scheduler |
| `regression/` | Minimal reproducers for fixed bugs and crashes | Trimmed-prefix resend to a lagging peer (`trim_resend`); add with each bug fix |
| `benchmark/` | Repeatable latency, throughput, allocation and copy measurements | Reserved; implementation required |
| `lib/` | Shared test-only assertions and fixtures | In-memory immutable storage, cluster host, seeded RNG, always-active checks |

`make check` builds the contract archives and runs executable tests using
Automake's parallel test harness. Failures appear in `test-suite.log` and
per-test `.log` files in the build tree. `make check-unit`,
`make check-integration`, and `make check-regression` select the existing
layers. Empty layers deliberately have no passing placeholder tests or
misleading coverage claims.

ASan and UBSan are on by default, including in the library under test. Keep the
shared `AM_CFLAGS` and `AM_LDFLAGS` when adding a target so every executable
layer retains instrumentation. Any sanitizer violation must fail the test.

Register new test programs explicitly in the top-level `Makefile.am` using
`check_PROGRAMS`, `_SOURCES`, `_LDADD`, and `TESTS`. Add private sources to
`src_libvsr_a_SOURCES`; link tests against `src/libvsr.a`. Do not include `.c`
implementation files in tests.

Use `lib/check.h` instead of `assert()` for test expectations so release builds
still test behavior. Tests must work from a separate build directory and not
write into the source tree. Register fixtures with `EXTRA_DIST`, resolve input
fixtures relative to the source tree, and keep generated data in the build tree.
Every randomized failure must print its seed and produce a replayable trace.

See [development workflows](../docs/development.md) for tools and commands.

`make fuzz` uses libFuzzer with ASan/UBSan and defaults to 10,000 executions.
Use `FUZZ_RUNS=0 FUZZ_ARGS='-max_total_time=60'` for a time budget. Corpora and
crashes live in the build tree under `tests/fuzzy/corpus/` and
`tests/fuzzy/artifacts/`, and survive `make clean`. Replay with
`./tests/fuzzy/checked PATH_TO_ARTIFACT`; keep useful minimized inputs in source
control and list them in `EXTRA_DIST`.

## Seeded cluster scheduler

The scenario engine lives in `fuzzy/scenario.c`. Every nondeterministic decision
is one bounded choice taken from a pluggable source: `fuzzy/cluster` draws them
from the seeded PCG generator, `fuzzy/cluster_fuzz` from a libFuzzer byte stream.
The scheduler accepts `SEED COUNT STEPS [trace|quiet] [PROFILE] [SEEDS]`
(defaults: `1 32 600 quiet 0`). For example, from a build directory:

```sh
./tests/fuzzy/cluster 1 1000 2000
./tests/fuzzy/cluster 9 1 600 trace > seed9.log 2>&1
./tests/fuzzy/cluster 1 1000 2000 quiet 7
./tests/fuzzy/cluster 1 1 600 quiet 127 200
```

Each run prints its seed and configuration before executing. The optional trace
prints the exact actions with stable node indices, queue indices, effect IDs,
and logical times; replay uses no process addresses or wall-clock inputs.
The scheduler separates effect execution from completion, reorders messages,
drops and duplicates traffic, changes logical time, crashes and restarts a
member, issues checkpoint hints and read barriers, retries segmented commands,
and varies work/output/effect/cache limits. The independent host checks safety
after each action; the scheduler additionally checks that two NORMAL members in
the same epoch and view never report different primaries, that a member's
committed position never decreases within an incarnation or across a restart
(against its durable hard state, or its last replicated commitment), and that
every `READ_READY` fence satisfies its barrier (`applied >= min_op`; for a
linearizable read also `applied >=` the commitment observed at admission) and
was granted by the primary of the fence's epoch and view. Once faults stop,
fair scheduling must commit a new request and catch up all full members. A
restart counts as unavailable until recovery finishes; another crash cannot
silently destroy the quorum required by the replicated durability policy.
Additional out-of-quorum safety scenarios belong in targeted tests without an
impossible liveness requirement.

Profile flags combine by addition: `1` begins with 16 committed commands and
optional checkpoints; `2` uses the exact minimum lease and payload budgets with
maximum-size commands; `4` adds changing network partitions. Profile `7` enables
all three. Profile `0` keeps the original cold-start event schedules stable for
regression replay, and each higher flag consumes choices only when it is set, so
profiles `0`..`7` replay the same schedules for existing seeds. The seed header
records the profile as part of the scenario.

| Flag | Behavior |
| --- | --- |
| `8` | Harsh crashes: a member may crash in any state (view change, recovering, transitioning, installing) and several may be down at once, within the failure budget below. |
| `16` | I/O faults: transient `RETRY` on `LOAD`, and `RETRY`/`NOT_FOUND` on `SNAPSHOT_CAPTURE`, `SNAPSHOT_FETCH`, `SNAPSHOT_DROP`, and `RECLAIM`, at most one in eight of the chosen effects and at most `STEPS/4` in total, never on the fencing operations (`STORE`, `SYNC`, `APPLY`, `SNAPSHOT_INSTALL`, `SNAPSHOT_SYNC`) and never once faults have ceased. |
| `32` | Membership: nonvoting learners join (full or witness) with the latest known membership as their seed; random `RECONFIGURE` requests admit a warmed learner, remove a member, swap full and witness roles, change `f`, bump the epoch, or hand off to a fully disjoint group of learners, always keeping `n >= 2f + 1` with at least `f + 1` full members; `CHECK_EPOCH` requests target `0..current epoch` and are resubmitted with the same identity through redirects; a pending `RECONFIGURE` is retried with the same identity and body until it resolves. |
| `64` | Reads under change: linearizable and causal read barriers issued right before crashes, partitions, and time advances (linearizable ones at the node that believes it is primary), checked as described above. |

Profile `127` enables everything; `tests/fuzzy/cluster_extended` is the same
program with defaults `1 16 600 quiet 93` (every flag except `2` and `32`,
whose known core failures are listed in `fuzzy/KNOWN_FAILURES.md`) and runs
under `make check` next to the profile `0` run.

Failure budget. At most `f` members of every configuration that may still need
them can be unavailable at once, where unavailable means crashed or restarted
without finishing recovery (`STARTING`/`RECOVERING`). The configurations are the
latest known membership, a pending proposal, and the current and (until
`STEADY`) previous membership of every live node; the budget is the smallest
`f` among them. Successive restarts do not reset the budget: only a finished
recovery does. In replicated mode a removed member that lost its state has no
group left to recover from, so it is treated as stopped and never restarted.
With flag `32`, liveness after faults cease is judged against the current
membership: every current member must be `NORMAL` and `STEADY` in that epoch
with the new request committed and, for full members, applied; removed members
must be `RETIRED` or stopped; learners must be `WARMING`.

Campaign mode. A trailing `SEEDS` argument (`quiet` required) runs seeds
`SEED..SEED+SEEDS-1` in one process, ignores `COUNT`, prints only the header
and replay command of each failing seed, and ends with a pass/fail summary. A
failing check aborts only that seed; its cluster is abandoned and the process
exits nonzero without the leak checker. The single-seed forms are unchanged.

`tests/fuzzy/cluster_fuzz` (a `--enable-fuzzing` build, run by `make fuzz` with
`-max_len=512`) drives the same engine from libFuzzer input: the first byte is
the seed label, the second the profile, and every later byte or byte pair
selects the next action or parameter; an exhausted input keeps choosing zero.
Replay an artifact with `./tests/fuzzy/cluster_fuzz PATH_TO_ARTIFACT`, with
`VSR_CLUSTER_TRACE=1` in the environment for the action trace.
Known failures of the current core found by campaigns are listed with their
mechanism and replay command in `fuzzy/KNOWN_FAILURES.md`.
