# Test layers

All tests, drivers, fixtures, corpora, and benchmarks belong under `tests/`.
The split takes inspiration from the sibling Raft project. Fuzzing and
seeded fault simulation share one directory.

| Directory | Purpose | Current state |
| --- | --- | --- |
| `unit/` | Small deterministic tests of private modules | Checked size arithmetic, including overflow boundaries |
| `integration/` | Public API and host adapter contracts | Existing C11 compile-only signature/layout checks |
| `fuzz/` | Coverage-guided fuzzing and seeded cluster/fault simulation | Arithmetic libFuzzer harness; simulation awaits the protocol |
| `regression/` | Minimal reproducers for fixed bugs and crashes | Reserved; add with each bug fix |
| `benchmark/` | Repeatable latency, throughput, allocation and copy measurements | Reserved; implementation required |
| `lib/` | Shared test-only assertions and future fixtures | Always-active `CHECK` macro |

`make check` builds the contract archives and runs executable tests using
Automake's parallel test harness. Failures appear in `test-suite.log` and
per-test `.log` files in the build tree. `make check-unit` and
`make check-integration` select the existing layers. Empty layers deliberately
have no passing placeholder tests or misleading coverage claims.

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
crashes live in the build tree under `tests/fuzz/corpus/` and
`tests/fuzz/artifacts/`, and survive `make clean`. Replay with
`./tests/fuzz/checked PATH_TO_ARTIFACT`; keep useful minimized inputs in source
control and list them in `EXTRA_DIST`.

Future fault simulations in `fuzz/` should drive logical time, network delivery
and a fake durable store from a recorded seed. Check safety after every event;
check eventual progress after restoring delivery and scheduling. Cover message
loss/reordering, partitions, crashes, checkpoint boundaries and backpressure.
