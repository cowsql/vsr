# Known failures found by the seeded scheduler

Each entry names the mechanism, the observed effect, and one replay command
(run from a build directory; add `trace` in place of `quiet` for the action
trace). Failures below are liveness stalls inside the documented failure model:
every member is available and fairly scheduled once faults cease, yet the group
never converges. Remove an entry once its regression test passes.

## A. Boundary committed from VIEW_CHANGE leaves the old primary TRANSFERRING

Mechanism: the primary holds a PREPARE_OK quorum for a RECONFIGURE entry but
its commit work is deferred (output slots or work budget exhausted); a
START_VIEW_CHANGE quorum then moves it to VIEW_CHANGE, and the next drain
commits the boundary from that state. The node enters the new epoch as
TRANSITIONING with phase TRANSFERRING although it already holds the complete
history through the boundary (committed == applied == boundary), issues no
fetch, never promises EPOCH_STARTED, and later flaps through views forever
(state VIEW_CHANGE, view climbing) while the other members, or a learner that
needs `f + 1` full-member promises, wait in INSTALLED. Deterministic
reproduction: three members, output capacity 1 at the primary, deliver
PREPARE_OK then both START_VIEW_CHANGE without an intervening drain, then
drain.

```sh
./tests/fuzzy/cluster 4 1 470 quiet 32
./tests/fuzzy/cluster 4 1 600 quiet 32
```

Profile 32 (seeds 1..200, 600 steps): 4, 22, 42, 81, 95, 115, 124, 136, 160,
174, 188 (42 and 95 show the learner side: witness learners stay
TRANSITIONING/INSTALLED waiting for the stuck member's promise).

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

## F. Learner restart enters quorum recovery as a non-member

Mechanism: a JOIN learner restarted with `RECOVER` and its epoch-0 seed
(the documented restart during warm-up) takes the `begin_recovery` path when
no complete durable state exists (durable mode before the first SYNC, and
replicated mode before warm-up completed) and, in durable mode, even after it
had warmed. It is not a member of the seed, so recovery never completes: the
node stays RECOVERING with role NONE forever. Deterministic reproduction:
three full members, one JOIN learner, crash and restart the learner.

```sh
./tests/fuzzy/cluster 1 1 600 quiet 125
./tests/fuzzy/cluster 3 1 600 quiet 125
```

This mechanism dominates the profiles that combine `8` and `32`.

## G. A restarted learner persists a hard state its own validator rejects

Mechanism: the non-member recovery of mechanism F stores a hard state with
`state = HARD_RECOVERING` and `role = VSR_MEMBER_NONE`. On the learner's next
restart the successful `LOAD_RECOVERY` completion carries that row, and
`vsr_validate_recovered` rejects `role == NONE` with `VSR_EINVAL`, so the
completion is never consumed and the instance cannot boot at all (the host's
`mem_node_complete` check fails). Observed as
`memory_cluster.c: check failed: step.result == VSR_OK || step.result == VSR_AGAIN`
for a learner restarted twice.

```sh
./tests/fuzzy/cluster 3 1 600 quiet 120
```

## Observation: an idle learner does not follow later commitments

A warmed learner transfers state once, at join, and then neither receives
PREPARE/COMMIT nor re-discovers: its committed and applied positions stay
where warm-up left them, and it does not learn later epochs until a
reconfiguration names it. The scheduler therefore only requires learners to be
WARMING (in any epoch) after faults cease. Whether continuous warm-up is
required is a contract question; `docs/vsr-api.md` says a learner "resumes
nonvoting warm-up" only after installing a later epoch.

## Pre-existing failures of profile 2 (minimum budgets)

Profile 2 and every union containing it (3, 5, 6, 7, 127) already failed in
the seeded scheduler before the extended profiles existed, for example seeds
6, 11, 13, 25, 32 at 600 steps (`mem_cluster_run` budget exhausted in the
liveness phase). They are not attributed to the extended flags.
