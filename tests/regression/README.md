# Regression tests

Reserved for focused reproductions of actual fixed bugs. Name tests after the
behavior, reference the issue or invariant, and retain minimized fault traces
or fuzz inputs beside the reproducer. Register executable regressions in
`check_PROGRAMS`/`TESTS` so they run under `make check` and sanitizers.

| Test | Origin | Behavior |
| --- | --- | --- |
| `trim_resend` | `tests/fuzzy/cluster 1017 1 2000 quiet 0` | A primary whose readable revision holds a not-yet-durable TRIM must not load or resend the compacted prefix to a lagging peer; the peer is steered to state transfer (checkpoint FETCH on a full replica, anchor only on a witness) and the cluster converges |
| `recovery_coverage` | `tests/fuzzy/cluster 108 1 2000 quiet 1` | A witness recovering from a lost store must install the primary's remote anchor without waiting for f+1 retention advertisements (which the transition module also dropped whenever they carried a later view), rejoin, and follow a later view change |
| `transfer_anchor` | `tests/fuzzy/cluster 8 1 2000 quiet 5` (also 62, 190) | While a witness compares its retained log against a new primary's untrimmed history, coverage advertisements must not publish a remote anchor and trim under the transfer's cursor; the pending comparison LOADs still find their entries, and the anchor is adopted afterwards |
| `witness_prefix` | `tests/fuzzy/cluster 273 1 2000 quiet 4` (also profile 5) | A witness holding the very entry a donor's anchor closes with keeps its prefix instead of restoring the anchor, so a lost full member can recover through it rather than deadlocking on promises the recovering member cannot yet make |
| `review_epoch_recovery_vote` | protocol safety review (open bug, `XFAIL_TESTS`) | A full member that lost its store must not rejoin as a voter without the recovery quorum. Its RECOVERY carries the seed epoch, a survivor answers NEW_EPOCH, and the epoch handoff installs one donor's lagging history and enters NORMAL; the two survivors then elect a view whose log lacks commands the unreachable primary already acknowledged, and a new command executes at an acknowledged op. Expected to fail until the core runs quorum recovery in the learned epoch |
