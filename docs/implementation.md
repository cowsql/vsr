# Implementation work

The acceptance target is the complete public contract in `include/vsr.h`,
`DESIGN.md`, `docs/protocol.md`, and `docs/vsr-api.md`. Public API refinements
are allowed when justified; keep those sources consistent with the implementation.
The refinements made so far are inventoried in [changes.md](changes.md).

## Engineering priorities

- Preserve protocol safety, explicit buffer ownership, and bounded progress.
- Keep the C11 core deterministic, single-owner, and free of allocation and I/O.
- Reference immutable payloads; copy small metadata where it simplifies ownership.
- Use contiguous fixed-capacity pools and separate frequently accessed metadata
  from transfer, snapshot, and diagnostic state. Align array bases, not every
  small descriptor.
- Keep batching, capacity, durability, and timing choices explicit. Measure the
  cost of supported choices instead of assuming one setting suits every host.
- Tests use independent histories and ownership/durability checks, not merely
  a second spelling of the implementation's decisions.

## Work sequence

1. Structural validation, checked arena planning, operation/lease ownership,
   bounded event/effect scheduling, failure diagnostics, and shutdown.
2. Deterministic in-memory host: immutable indexed storage revisions, crash
   durability, snapshots, application execution, and queued transport effects.
3. Normal replication, storage prerequisites, batched application, and persistent
   client duplicate suppression.
4. View change, immutable history transfer, catch-up, and restart recovery.
5. Checkpoint capture/installation/publication, replay, retention, and reclamation.
6. Epoch handoff, nonvoting warm-up, witnesses, and role changes.
7. Linearizable and causal read barriers; cross-feature lifecycle integration.
8. Reproducible fault campaigns, focused regressions, runnable examples,
   performance measurements, documentation, and complete build checks.

The simulator and validation suites grow with each increment. These are internal
milestones, not reductions of the final acceptance target. All eight are
complete; the sequence remains the order in which a change is re-validated.

## Validation obligations

Safety is checked after each simulated event. Eventual progress is checked after
faults cease and fair delivery/execution resumes, within the documented failure
model. Randomized runs record the seed, options, event ordering, call boundaries,
and output capacities; object handles replace process-local addresses.

The fault matrix includes message loss/duplication/reordering, partitions,
concurrent view changes and recovery, crashes at storage and snapshot boundaries,
reordered effect completions, stale read rounds, disjoint epoch handoff, witness
promotion/demotion, cache eviction, maximum payloads, small capacities, numeric
limits, malformed input, and shutdown under saturation. Regression tests retain
minimal reproductions of discovered failures.

Existing compiler/sanitizer/static-analysis/distribution checks remain part of
validation. Benchmarks report configuration and distinguish core work from
the deliberately instrumented in-memory host.

## Current status

The implementation is complete against the contract. Every increment of the
work sequence has a passing test layer, the campaigns below pass, and
`tests/fuzzy/KNOWN_FAILURES.md`, which holds failures found by campaigns that
no regression test fixes yet, is empty.

`make check` runs 47 programs, under Clang ASan/UBSan by default:

| Layer | Programs |
| --- | --- |
| Unit (`tests/unit`) | Ten: checked arithmetic, logical equality, PCG, the in-memory store, graph validation, runtime ownership and scheduling against a protocol double, read rounds, descriptor copies, the arena planner |
| Integration (`tests/integration`) | Fifteen: replication, normal-path minima, reads, checkpoints, epochs, lifecycle, transitions, audit, transfer audit, failures, view change edges, offers, capture, clients, and the independent API conformance suite `api_contract`; plus the compile-only header check |
| Regression (`tests/regression`) | Fourteen minimal reproducers of fixed bugs, each with its origin in `tests/regression/README.md` |
| Fault simulation (`tests/fuzzy`) | `cluster` (32 seeds, profile 0) and `cluster_extended` (16 seeds, profile 93) |
| Examples (`examples`) | Six narrated key-value walkthroughs |

CI adds GCC with `-fanalyzer`, the Clang integer sanitizer, lint (format,
ShellCheck, clang-tidy, Cppcheck), Clang static analysis, Valgrind, a
10,000-run libFuzzer smoke test of the three harnesses, coverage, and
`distcheck`.

### Campaigns

The seeded scheduler runs many seeds in one process when given a trailing
`SEEDS` argument: `./tests/fuzzy/cluster SEED 1 STEPS quiet PROFILE SEEDS`
from a build directory, with profiles and flags as in `tests/README.md`. It
prints the header and replay command of every failing seed and a pass/fail
summary, and exits nonzero on any failure. Replay one seed with `trace` in
place of `quiet`. Coverage-guided runs use `make fuzz` in an
`--enable-fuzzing` build; `VSR_CLUSTER_TRACE=1 ./tests/fuzzy/cluster_fuzz
ARTIFACT` replays an artifact with its action trace.

Campaigns run on this tree, all passing:

| Command | Seeds |
| --- | --- |
| `cluster 1 1 600 quiet 127 200` | 200 |
| `cluster 1 1 2000 quiet 127 32` | 32 |
| `cluster 1 1 600 quiet 34 100` | 100 |
| `cluster 1 1 2000 quiet 2 32` | 32 |
| `cluster 1 1 600 quiet P 32` for `P` in 2, 3, 32, 127 | 32 each |
| `cluster 1000 1 600 quiet P 1000` for `P` in 2, 3, 6, 7 | 1000 each |
| `cluster 1 1 2000 quiet P 150` for `P` in 6, 7 | 150 each |

Every replay command formerly listed in `tests/fuzzy/KNOWN_FAILURES.md` also
passes. Flags `2` and `32` are exercised by these campaigns rather than by
`cluster_extended`, whose default union stays at `93` to bound `make check`
time. Rerun the table after any change under `src/`; a failing seed becomes a
regression test, not an entry that stays open.

### Remaining optional work

- Performance. `make benchmark` reports arena bytes and `vsr_step` timings for
  a small configuration matrix in an uninstrumented build. CI does not run
  it, copying and cache behavior are not measured, and no optimization pass
  has been made. Public structure sizes are checked by
  `tests/integration/header.c` but are not performance evidence.
- Campaign breadth: longer and wider campaigns than the table, and a
  small-command/large-manifest size configuration (see the coverage table).
- A threaded host adapter. The core needs none; the TSan build exists for one.

## Coverage of the validation matrix

The matrix combines the last paragraph of `DESIGN.md` with the fault matrix
above. "Scheduler" is `tests/fuzzy/scenario.c` with its profile flags; the
host oracles of `tests/lib/memory_cluster.c` (graph immutability, lease
release, STOPPED accounting, fence exclusion, read-fence bounds, offer
equality, effect prerequisites, the cluster-wide execution table) run on every
action of every scheduler run and integration test.

| Obligation | Coverage | Where |
| --- | --- | --- |
| Message loss, duplication, reordering | Covered | Scheduler, every profile (`drop`, `duplicate`, `deliver` actions) |
| Partitions | Covered | Scheduler flag `4` |
| Concurrent view changes and recovery; recovery during view change | Covered | Scheduler flag `8` (crashes in VIEW_CHANGE, RECOVERING, TRANSITIONING, up to `f` down); `view_change_edges` (`recovering_primary`, `restart_while_recovering`, `skew_and_interrupt`); `transitions`; regressions `review_epoch_recovery_vote`, `epoch_announcement_recovery` |
| Crash injection at transaction and checkpoint boundaries | Covered | `lifecycle` `append_crashes` (crash before the STORE executes, after it is readable but unacknowledged, after it is durable but unacknowledged) and `snapshot_crashes` (four stages around CAPTURE and SNAPSHOT_SYNC); scheduler crashes at any action, with the host discarding non-durable revisions and snapshots (`mem_store_crash`) |
| Reordered effect completions | Covered | Scheduler separates `execute` from `complete` and picks effects at random; `normal_contract` `reversed_stores` |
| Stale read rounds and read probes | Covered | Scheduler flag `64` with the `check_read_fence` oracle; `reads` (`quorum_and_application_fence`, `backup_causal_and_deadline`) |
| I/O faults | Covered | Scheduler flag `16`; `lifecycle` `failure_matrix` (every operation with every status); `failures`; `checkpoints` `capture_retry_and_stop` |
| Disjoint epoch handoff | Covered | `epochs` `disjoint`; `examples/epochs`; scheduler flag `32` (disjoint successor of warmed learners) |
| Witness promotion and demotion | Covered | `epochs` `promotion` and `same_members` (the demoted member keeps its donor image and executes nothing as a witness); scheduler flag `32` role swap |
| Demotion during disjoint handoff | Partially | Demotion and disjoint handoff are separate proposals in `epochs` and in the scheduler (one change kind per `RECONFIGURE`); no scenario demotes a surviving member in the proposal that replaces the rest of the group |
| Duplicate CHECK_EPOCH requests across redirects | Covered | Scheduler flag `32` (`CHECK_EPOCH` resubmitted with the same identity through redirects, `certified`/`uncertified` oracle); `epochs` `same_members`; `clients` `routing` |
| Cache eviction | Covered | `client_cache_entries = 1` in `replication`, `normal_contract`, `epochs`, `clients`, `capture`, `checkpoints`, `failures`; scheduler varies client and log cache sizes; `offers` `eviction`; regressions `pressure_reload`, `lagging_reload` |
| Maximum-size commands and manifests under completion backpressure | Covered | `clients` `payload_backpressure` (maximum commands with APPLY held); `audit` `exact_apply_reservation`; `capture` `manifest_at_limits`; scheduler flag `2` |
| Maximum-size results | Partially | The planner reserves `batch_entries * result_bytes` (`api_contract`, `unit/layout`), but the host applications return short results: no test executes a result of exactly `result_bytes` |
| Small capacities | Covered | `normal_contract` `minimum_capacity`, `lagging_peers_minimum_cache`; scheduler flag `2` (exact minimum lease and payload budgets), one to sixteen operations, `work_per_step` 1..16, output capacity 1..4 |
| Numeric limits | Covered | `unit/validate`, `unit/layout`, `api_contract` (`UINT64_MAX` limits and timeouts); `view_change_edges` `view_domain_exhausted`; `failures` and `unit/runtime` (`VSR_FAILURE_EXHAUSTED`) |
| Malformed input | Covered | `unit/validate`; `fuzzy/validation` under `make fuzz`; `api_contract`; `lifecycle` `completion_validation` |
| Shutdown under saturation | Covered | `lifecycle` `stop_under_saturation`; `api_contract` `test_backpressure_stop`; `offers` `stop_while_serving` |
| Ownership accounting under small arenas and saturated queues | Covered | Host oracles on every step (`outstanding_ops`/`outstanding_leases`, lease release, STOPPED accounting); `api_contract` `test_ownership`, `test_backpressure_release`; `offers` `saturation`; scheduler flag `2` |
| Checkpoint restore with a suffix larger than one transaction | Covered | `checkpoints` (retained suffix exceeding cache and transaction size, both policies, restart in durable mode); `transitions` `interrupted_transfer_preserves_suffix` |
| Storage prerequisites and buffer immutability | Covered | Host audit at issuance (`action_prerequisites`) and `check_graphs` on every action |
| Measurement of allocation, throughput, and latency | Partially | `make benchmark` reports arena bytes, step counts, core nanoseconds per request, and p50/p99 step latency; not run by CI |
| Measurement of copying and cache behavior | Not covered | No copy counting or hardware-counter measurement; `tests/benchmark/README.md` points at an external profiler |
| Large-command/small-manifest and small-command/large-manifest campaigns | Not covered | The scheduler uses two fixed size configurations (1024/1024/64 bytes by default, 32/18/8 at flag `2`); no campaign uses a manifest larger than its command |
