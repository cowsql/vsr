# Known failures found by the seeded scheduler

This file lists failures of the current core that the seeded scheduler
(`tests/fuzzy/cluster`, alone or in campaign mode) has found and that no
regression test fixes yet. Each entry names the mechanism, the observed
effect, and one replay command (run from a build directory; add `trace` in
place of `quiet` for the action trace). Remove an entry once its regression
test passes. Keep the file, with an empty list, when nothing is open, so that
`tests/README.md` and `docs/implementation.md` can point here unconditionally.

Entries are defects inside the documented failure model: every member is
available and fairly scheduled once faults cease, yet a safety oracle fails or
the group never converges. Impossible liveness demands (more than `f` members
lost, a zero-fault group losing state) belong in targeted tests, not here.

## Open failures

None. Every command previously listed here (`23 1 600 quiet 64`,
`54 1 600 quiet 8`, `77 1 600 quiet 8`, and profile `2` seeds 6, 11, 13, 25,
and 32 at 600 steps) passes on this tree, as do the campaigns recorded in
`docs/implementation.md`.
