# Regression tests

Reserved for focused reproductions of actual fixed bugs. Name tests after the
behavior, reference the issue or invariant, and retain minimized fault traces
or fuzz inputs beside the reproducer. Register executable regressions in
`check_PROGRAMS`/`TESTS` so they run under `make check` and sanitizers.
