# Regression tests

Reserved for focused reproductions of actual fixed bugs. Name tests after the
behavior, reference the issue or invariant, and retain minimized fault traces
or fuzz inputs beside the reproducer. Register executable regressions in
`check_PROGRAMS`/`TESTS` so they run under `make check` and sanitizers.

| Test | Origin | Behavior |
| --- | --- | --- |
| `trim_resend` | `tests/fuzzy/cluster 1017 1 2000 quiet 0` | A primary whose readable revision holds a not-yet-durable TRIM must not load or resend the compacted prefix to a lagging peer; the peer is steered to state transfer (checkpoint FETCH on a full replica, anchor only on a witness) and the cluster converges |
