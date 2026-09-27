# Implementation work

The acceptance target is the complete public contract in `include/vsr.h`,
`DESIGN.md`, `docs/protocol.md`, and `docs/vsr-api.md`. Public API refinements
are allowed when justified; keep those sources consistent with the implementation.

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
milestones, not reductions of the final acceptance target.

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

- Initial repository and contract review completed; implementation authorized.
- Runtime, graph validation, checked arena planning, ownership, deterministic
  random generation, and the in-memory host have passing unit tests. Normal
  replication and reads have passed isolated protocol checks.
- Seven unit executables pass the configured Clang ASan/UBSan build. The first
  six also passed the optimized GCC analyzer build. The typed graph validator
  completed 100,000 libFuzzer inputs with seed 1 without a sanitizer finding.
- Normal replication, reads, checkpoints, transition/recovery, and epoch handoff
  are being linked and tested together. Isolated tests do not establish that
  cross-module safety or liveness is correct.
- Public API replication, read-fence, and checkpoint tests and a deterministic
  fault scheduler are present; combined execution and regression repair are the
  current work. Examples and performance measurements remain outstanding.

## Resource progress review

Permanent snapshot references and selected history offers share the input lease
budget with requests, incoming chunks, and loaded comparisons. A layout must
reserve enough capacity for these dependencies even when operation concurrency,
cache capacity, or output capacity is one. Optional snapshot generations must
not occupy the reserves required to complete an accepted transfer. The runtime
planner and minimum-capacity integration tests have been reconciled against the
simultaneous hold categories at the documented minimum; the seeded profile 2
stalls were scheduling cycles rather than reservation leaks. Optional
maintenance now releases only applied, announced cache entries and only when
that restores the capture baseline; a capture hint whose baseline is
unavailable is deferred instead of holding transitions busy; a lagging peer is
served before the cache slot loaded for it can be reused; entries are not
loaded for application while a transition forbids it; a primary announces
commits to peers whose next entry lies below its trimmed log; and a witness
needs coverage only for retained entries it actually discards. Increasing test
defaults is not a substitute for checking the accepted minimum. Large
command/small manifest and small command/large manifest configurations need
separate fault campaigns.
