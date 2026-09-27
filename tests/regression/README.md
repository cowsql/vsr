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
| `learner_restart` | `tests/fuzzy/cluster 1 1 600 quiet 125` (also 3, and `3 1 600 quiet 120`) | A JOIN learner restarted with RECOVER resumes nonvoting warm-up instead of entering quorum recovery as a non-member, whether its durable store survived, was lost before the first SYNC, or is replicated; it never persists a role its validator rejects, survives two restarts, and is then admitted and executes commands |
| `learner_follows` | idle-learner observation of the seeded scheduler | Warm-up is continuous: an idle full or witness learner keeps up with later commits, follows a later epoch it is not named in to STEADY, and is admitted with only the remaining suffix to transfer, never voting on the way |
