# Test layers

All tests, drivers, fixtures, corpora, and benchmarks belong under `tests/`.
The split takes inspiration from the sibling Raft project. Fuzzing and
seeded fault simulation share one directory.

| Directory | Purpose | Current state |
| --- | --- | --- |
| `unit/` | Small deterministic tests of private modules | Checked arithmetic (`checked`), logical equality (`objects`), PCG (`random`), in-memory store (`memory`, `memory_watch`), graph validation (`validate`), runtime ownership and scheduling against a protocol double (`runtime`), read rounds (`reads`), descriptor copies (`descriptors`), arena planner (`layout`) |
| `integration/` | Public API and host adapter contracts | Replication, normal-path minima and lagging peers (`normal_contract`), reads, checkpoints, epoch handoff (`epochs`), stop/failure/crash lifecycle (`lifecycle`), transfer and recovery transitions (`transitions`), effect prerequisites (`audit`, `transfer_audit`), failure handling (`failures`), view change edges (`view_change_edges`), log offers (`offers`), capture scheduling (`capture`), client contract (`clients`), the independent driver-facing API conformance suite (`api_contract`), and the compile-only header check (`libheader.a`) |
| `fuzzy/` | Coverage-guided fuzzing and seeded cluster/fault simulation | libFuzzer harnesses for checked arithmetic (`checked`), graph validation (`validation`), the cluster scenario (`cluster_fuzz`), the codec's decoders (`frame`) and its indexed entry decoder (`entry`); seeded cluster scheduler (`cluster`, `cluster_extended`); `KNOWN_FAILURES.md` for open campaign findings |
| `regression/` | Minimal reproducers for fixed bugs and crashes | Seventeen reproducers, each with its origin in `regression/README.md`: trimmed-prefix resend (`trim_resend`); witness anchor adoption during recovery and transfer (`recovery_coverage`, `transfer_anchor`, `witness_prefix`); boundaries learned in view change or before STEADY (`boundary_from_view_change`, `next_epoch_before_steady`); lost-state members rejoining only through quorum recovery (`review_epoch_recovery_vote`, `epoch_announcement_recovery`, `recovery_into_handoff`); learner restart and continuous warm-up (`learner_restart`, `learner_follows`); input-pressure relief and a view change at minimum budgets (`pressure_reload`, `view_change_minimum`); a lagging peer's loaded entry sent before another peer's load can evict it (`lagging_reload`); checkpoint adoption behind the applied position and replies across a restoration (`anchor_behind_applied`, `reply_restore`); `vsr_init` output clearing (`api_init_out`); add with each bug fix |
| `benchmark/` | Repeatable latency, throughput, allocation and copy measurements | `core`: arena bytes and `vsr_step` timings over a small configuration matrix, run by `make benchmark` outside `TESTS`; copying and cache behavior are not measured |
| `lib/` | Shared test-only assertions and fixtures | In-memory immutable storage validating each transaction's shape at issuance; cluster host with always-active oracles for fence exclusion, read-fence bounds, a cluster-wide client execution/reply table, offer-versus-store equality, LOAD/RECLAIM/DROP retention rules, lease release, and STOPPED accounting; seeded RNG |

`make check` builds the contract archives and runs executable tests using
Automake's parallel test harness. Failures appear in `test-suite.log` and
per-test `.log` files in the build tree. `make check-unit`,
`make check-integration`, `make check-regression`, and `make check-fuzzy`
select the existing layers. Empty layers deliberately have no passing placeholder tests or
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
`tests/fuzzy/artifacts/`, and survive `make clean`. Replay with the harness that
produced the artifact, e.g. `./tests/fuzzy/validation PATH_TO_ARTIFACT`; keep
useful minimized inputs in source control and list them in `EXTRA_DIST`.
The `frame` and `entry` corpora are seeded by `tests/unit/codec`, which writes
encodings it builds when `VSR_CODEC_CORPUS` and `VSR_CODEC_ENTRY_CORPUS` name
directories.

## Seeded cluster scheduler

The scenario engine lives in `fuzzy/scenario.c`. Every nondeterministic decision
is one bounded choice taken from a pluggable source: `fuzzy/cluster` draws them
from the seeded PCG generator, `fuzzy/cluster_fuzz` from a libFuzzer byte stream.
The scheduler accepts `SEED COUNT STEPS [trace|quiet] [PROFILE] [SEEDS]`
(defaults: seed 1, 32 seeds, 600 steps, header without trace, profile 0). For
example, from a build directory:

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
silently destroy the quorum required by the replicated durability policy, and
a proposed membership always tolerates the members of it already unavailable,
since a lost-state member rejoins the new epoch only through its quorum
recovery.
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
which the campaigns listed in `docs/implementation.md` exercise instead, to
bound `make check` time) and runs under `make check` next to the profile `0`
run.

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
must be `RETIRED` or stopped; a learner that can still reach a live,
unretired member of the membership it knows must be `WARMING` and `STEADY` in
the current epoch with the new request committed, and any other learner need
only be `WARMING`.

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
Failures found by campaigns that no regression test fixes yet are listed with
their mechanism and replay command in `fuzzy/KNOWN_FAILURES.md`; the list is
empty on a tree where the campaigns in `docs/implementation.md` pass.
