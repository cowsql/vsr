# Test layers

All tests, drivers, fixtures, corpora, and benchmarks belong under `tests/`.
The split takes inspiration from the sibling Raft project. Fuzzing and
seeded fault simulation share one directory.

| Directory | Purpose | Current state |
| --- | --- | --- |
| `unit/` | Small deterministic tests of private modules | Arithmetic, equality, RNG, runtime, graph validation, reads, host storage |
| `integration/` | Public API and host adapter contracts | Replication, resource minima, reads, checkpoints, epoch handoff |
| `fuzzy/` | Coverage-guided fuzzing and seeded cluster/fault simulation | Arithmetic and graph libFuzzer harnesses; seeded cluster scheduler |
| `regression/` | Minimal reproducers for fixed bugs and crashes | Trimmed-prefix resend to a lagging peer (`trim_resend`); witness anchor adoption during recovery and transfer (`recovery_coverage`, `transfer_anchor`, `witness_prefix`); input-pressure relief around a pin the next poll reloads (`pressure_reload`); checkpoint adoption behind the applied position and executed replies across a restoration (`anchor_behind_applied`, `reply_restore`); add with each bug fix |
| `benchmark/` | Repeatable latency, throughput, allocation and copy measurements | Reserved; implementation required |
| `lib/` | Shared test-only assertions and fixtures | In-memory immutable storage validating each transaction's shape at issuance; cluster host with always-active oracles for fence exclusion, read-fence bounds, a cluster-wide client execution/reply table, offer-versus-store equality, LOAD/RECLAIM/DROP retention rules, lease release, and STOPPED accounting; seeded RNG |

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

The seeded scheduler accepts `SEED COUNT STEPS [trace|quiet] [PROFILE]`
(defaults: `1 32 600 quiet 0`).
For example, from a build directory:

```sh
./tests/fuzzy/cluster 1 1000 2000
./tests/fuzzy/cluster 9 1 600 trace > seed9.log 2>&1
./tests/fuzzy/cluster 1 1000 2000 quiet 7
```

Each run prints its seed and configuration before executing. The optional trace
prints the exact actions with stable node indices, queue indices, effect IDs,
and logical times; replay uses no process addresses or wall-clock inputs.
The scheduler separates effect execution from completion, reorders messages,
drops and duplicates traffic, changes logical time, crashes and restarts a
member, issues checkpoint hints and read barriers, retries segmented commands,
and varies work/output/effect/cache limits. The independent host checks safety
after each action. Once faults stop, fair scheduling must commit a new request
and catch up all full members. A restart counts as unavailable until recovery
finishes; another crash cannot silently destroy the quorum required by the
replicated durability policy. Additional out-of-quorum safety scenarios belong
in targeted tests without an impossible liveness requirement.

Profile flags combine by addition: `1` begins with 16 committed commands and
optional checkpoints; `2` uses the exact minimum lease and payload budgets with
maximum-size commands; `4` adds changing network partitions. Profile `7` enables
all three. Profile `0` keeps the original cold-start event schedules stable for
regression replay. The seed header records the profile as part of the scenario.
