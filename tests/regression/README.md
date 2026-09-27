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
| `boundary_from_view_change` | `tests/fuzzy/cluster 4 1 470 quiet 32` (also seeds 22, 42, 81, 95, 115, 124, 136, 160, 174, 188 at 600 steps) | A RECONFIGURE boundary learned as committed from VIEW_CHANGE (the old primary's deferred commit store, or a new primary's log selection fed by a witness offer) must abandon the old-epoch round and install directly, promise EPOCH_STARTED, and let the group reach STEADY instead of electing forever in the new epoch |
| `review_epoch_recovery_vote` | protocol safety review (open bug, `XFAIL_TESTS`) | A full member that lost its store must not rejoin as a voter without the recovery quorum. Its RECOVERY carries the seed epoch, a survivor answers NEW_EPOCH, and the epoch handoff installs one donor's lagging history and enters NORMAL; the two survivors then elect a view whose log lacks commands the unreachable primary already acknowledged, and a new command executes at an acknowledged op. Expected to fail until the core runs quorum recovery in the learned epoch |
| `learner_restart` | `tests/fuzzy/cluster 1 1 600 quiet 125` (also 3, and `3 1 600 quiet 120`) | A JOIN learner restarted with RECOVER resumes nonvoting warm-up instead of entering quorum recovery as a non-member, whether its durable store survived, was lost before the first SYNC, or is replicated; it never persists a role its validator rejects, survives two restarts, and is then admitted and executes commands |
| `learner_follows` | idle-learner observation of the seeded scheduler | Warm-up is continuous: an idle full or witness learner keeps up with later commits, follows a later epoch it is not named in to STEADY, and is admitted with only the remaining suffix to transfer, never voting on the way |
