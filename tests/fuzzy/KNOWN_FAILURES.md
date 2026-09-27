# Known failures found by the seeded scheduler

Each entry names the mechanism, the observed effect, and one replay command
(run from a build directory; add `trace` in place of `quiet` for the action
trace). Failures below are liveness stalls inside the documented failure model:
every member is available and fairly scheduled once faults cease, yet the group
never converges. Remove an entry once its regression test passes.

## C. Recovering witness blocked on full-member coverage of the donor anchor

Mechanism: a witness whose store was lost (crash before its first SYNC in
durable mode, or any crash in replicated mode) recovers from a quorum whose
primary offers `log_begin > 1` with a checkpoint at `log_begin - 1`. The
witness selects that offer and must RESTORE the anchor, but `store_poll`
refuses the RESTORE until `f + 1` full members have advertised a checkpoint at
or beyond the anchor to this witness. Only the members that actually captured
a checkpoint ever advertise one (`checkpoint_interval` is zero and hints are
explicit), so the witness stays RECOVERING with an active target forever while
every other member is NORMAL.

```sh
./tests/fuzzy/cluster 23 1 600 quiet 64
./tests/fuzzy/cluster 54 1 600 quiet 8
```

## D. Primary never accepts its own LOAD_LOG completion and spins on MORE

Mechanism: after two members recovered in turn, the primary (durable,
`log_cache_entries = batch_entries = 1`, NORMAL, committed 6, readable
revision 18) has one outstanding `LOAD_LOG` for revision 18, range `[1, 8)`.
The host offers the successful completion on every pass and the core never
consumes it (`consumed = 0`), yet every zero-input drain returns
`VSR_UPDATE_MORE` without emitting an operation, and a peer's discovery
GET_STATE is likewise left unconsumed on every delivery. Nothing can advance
within one logical instant, so the host's action budget is exhausted.

```sh
./tests/fuzzy/cluster 77 1 600 quiet 8
```

## Pre-existing failures of profile 2 (minimum budgets)

Profile 2 and every union containing it (3, 5, 6, 7, 127) already failed in
the seeded scheduler before the extended profiles existed, for example seeds
6, 11, 13, 25, 32 at 600 steps (`mem_cluster_run` budget exhausted in the
liveness phase). They are not attributed to the extended flags.
