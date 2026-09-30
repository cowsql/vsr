# Core fixes: dead_backup_apply and held_store_deadline

Branch `wt/core-fixes` on top of `6e6a636` (the I/O layer's engine phase 2
branch). Commits, in order: `Core: apply committed entries without waiting on
sends to every peer`; `Core: report a deadline only while its poll would act
on it`; `Model links in the memory cluster and check every idle deadline`;
`Turn the two engine-found reproductions into regressions of the fixes`; `Do
not count an idle, silent drain as progress in mem_cluster_run`; `Make the
dead-backup reconfiguration case withhold announcements`; `Core: recognise
the boundary by its proposal fence before loading it`; and this handover.
Nothing under `src/io/`, `tests/lib/io_world.*` or the io documents was
touched. `include/vsr.h` is unchanged.

## Bug 1: dead_backup_apply (liveness)

Mechanism. `vsr_protocol_poll` advances `notified_commit` one entry at a
time; `apply_poll` applies nothing beyond it. The notification advanced
only once every peer's `commit_sent` reached `stable_commit`, and
`commit_sent` moves only when a COMMIT, or a PREPARE whose `committed`
field covers the position, is issued to the peer. A peer more than a batch
behind only ever receives the PREPARE of its next `batch_entries` entries,
whose `committed` is capped at the batch's end, and both a failed SEND
(`vsr_protocol_complete`, `VSR_TAG_SEND`) and the retransmission timer
(`vsr_protocol_event`, TIME) rewind `peer->sent` to `peer->prepared`, so
the same first batch went out again for ever and the peer's `commit_sent`
never reached the commitment. The I/O layer's link module answers every
SEND to a node it has no link to with RETRY (deferred `retry_ns`, decision
129), so with one backup of three unreachable, request `batch_entries + 1`
committed and was never applied. The memory cluster completed every SEND
OK and queued the message, so every batch went out in one drain and the
COMMIT followed; that hid the bug from every core campaign. The same rule
also deadlocked a reconfiguration that removes a dead member, and stalled
a new primary after a view change (its cursors start at zero and the log
is resent from its beginning).

The gate's rationale. The comment on the gate said it: "A boundary
announcement changes the membership immediately. Issue the final old-group
commit first, while its envelope is still valid." A committed RECONFIGURE
ends its epoch; `vsr_epochs_committed` enters the next one the moment the
entry is notified, `vsr_protocol_configuration` replaces the peer table, and
the primary never again sends in the old epoch's envelope. A peer that has
not been sent the boundary's commitment by then learns it from START_EPOCH
(`send_announcements`, retried every `retry_ns` until STEADY) and takes the
handoff's fence and single-donor transfer (`vsr_transition_epoch`, with
`rebuild`), copying a history it already holds. So the rule buys the old
group the cheap path: COMMIT, then `vsr_epochs_committed` locally, then
INSTALLED without a transfer. It is not a safety property: an issued SEND
is not a delivery, and the announcement path is correct on its own (and is
what the regressions `boundary_from_view_change` and
`next_epoch_before_steady` exercise, both of which expect the late witness
or backup to receive that final COMMIT). Applied to every entry it was
merely over-broad; applied to every peer it was a deadlock.

The fix (`boundary_commit_pending`). Only the RECONFIGURE that ends the
current epoch waits, and only for the peers up to date with it: a peer
whose `max(sent, prepared) >= boundary`, that is, one whose PREPARE of the
boundary went out and was not rewound since, or that acknowledged it. That
set always contains the quorum that acknowledged the boundary (their
`prepared >= boundary`), so at least a quorum of the old group, the primary
included, is sent the final COMMIT, and it contains every live peer that
was up to date, which is what the two regressions above rely on. Each such
COMMIT is a single send: `network_poll` takes the COMMIT branch for a peer
with nothing left to PREPARE and issues it as soon as the peer's send in
flight completes, so the wait is bounded by completions of sends already
issued, never by a peer. A peer that is behind would first need its
remaining batches acknowledged, and an unreachable one never is (its SENDs
fail and its cursor rewinds to its last acknowledgment, which drops it out
of the set); both learn the boundary from the announcement. Every other
entry is notified, and applied, as soon as it is committed and present.
`docs/protocol.md` "Epoch handoff and witnesses" states the rule. A first
attempt gated on a plain quorum count broke `boundary_from_view_change` and
`next_epoch_before_steady` (the witness or the late backup was not among
the first `q - 1` peers the cursor reached); the set above is the precise
property.

The gate recognises the boundary by the proposal fence `proposed_boundary`
(the RECONFIGURE of the current epoch, set at proposal or transfer and
surviving eviction), not by loading the entry. A first version loaded it,
and `cluster 135 1 600 quiet 44` (partitions, harsh crashes, membership, a
one-entry cache) looped in `heal`: the boundary and the entry a lagging
peer needed shared the cache slot, so the notification's load and
`network_poll`'s load evicted each other for the whole action budget. The
old rule never loaded the boundary until every COMMIT was out, which hid
the conflict. With the fence, the COMMITs go out first and the entry is
loaded once. A fence that is not set leaves the notification ungated,
which is safe because the announcement path never depended on the gate.

## Bug 2: held_store_deadline (host spin)

Mechanism. `begin_recovery` sets the round's `retry_at = now` and marks the
hard state dirty; `recovery_poll` returns before touching the timer until
`hard_safe` (the hard-state STORE completed and, under DURABLE, synced);
`vsr_transition_deadline` reported `t->retry_at` for any round. A host that
arms its timer from each return woke at once, fed TIME, polled, and got the
same deadline back, for as long as the STORE was outstanding: the I/O
layer's store holds it while it creates the log (decision 71), so an
engine spun for the whole creation a RECOVER without a store performs.

The class. Any deadline reported while what its poll waits for is a
completion rather than time. Found by reading every `*_deadline` function
against the poll that owns each timer, then by the new harness oracle:

| Timer | Owner | Why it stayed expired | Fix |
| --- | --- | --- | --- |
| `t->retry_at`, ROUND_RECOVERY / ROUND_VIEW | `recovery_poll`, `view_poll` | both return before the timer until `hard_safe` | reported only while `round_timer_live`: `hard_safe` and no transfer active |
| `t->retry_at`, discovery rounds (CATCHUP, WARM, EPOCH) | `discovery_poll` | gated on `hard_safe` (unless discovering from an uninitialized store); a request the poll could not send left the timer expired | reported only while a request is out (`discovery_waiting`) and the gate holds; at expiry the poll clears `discovery_waiting` and retries the send at every poll |
| `t->retry_at`, any round with `target.active` | none | the round's poll does not run during the transfer it selected, so the timer went stale for the whole transfer | not reported while a transfer is active |
| `t->election_at` | `vsr_transition_poll`, `view_poll` | acted on ungated only for ROUND_CATCHUP and a view change's transfer; a view round's is gated on `hard_safe`; a learner's warm-up transfer outlives `view_timeout_ns` with nothing acting on it | reported only under `election_timer_live`, the same conditions |
| `t->warm_at` | `warm_poll` | gated on WARMING, no round, no transfer, no boot restore, `hard_safe` | `warm_idle` shared by the poll and the deadline |
| `target.retry_at` (fetch) | `fetch_phase` | at expiry the refetch may fail for want of an operation slot, leaving `waiting` and the timer expired | expiry clears `waiting`; the fetch is issued at every poll |
| `servers[i].retry_at` | `server_poll` | a served load's retry may fail for want of budget | cleared when due |
| `p->append_at` | `store_poll` | a due batched append may lack a transaction slot or operation | cleared when due |
| `checkpoint->advertise_at` | `advertisement_poll` | the round in progress is driven by polls, and its timer was stale; worse, a capture hint deferred under input pressure returned from `vsr_checkpoint_poll` before ever reaching `advertisement_poll` (every fourth seed of the profiles with flag 2 once the oracle existed) | cleared at the round's start, not reported while `advertise`, and the deferred capture falls through to the advertisement |
| `e->retry_at` (epochs) | `vsr_epochs_event` TIME | re-armed at every TIME only while the stage is not IDLE, so a stale value at IDLE would be reported for ever | not reported at IDLE; cleared on entering a STEADY epoch |

Timers already correct: the protocol's `heartbeat_at` and the peers'
`retry_at` (re-armed by every TIME), the reads' round timer (re-armed at
expiry before sending) and barrier deadlines (rejected slots are not
reported), the checkpoint's `retry_at` (cleared when due), offers'
`expires` (released at the poll or unreported while referenced). The
protocol's `election_at` is not reported at all (a backup checks it at its
heartbeat wakeups); that pre-existing imprecision is not a spin and was
left alone.

The rule, now in `docs/vsr-api.md` "Driving the core": a return without
`MORE` never reports a deadline at or before the last accepted time; every
expired timer has been acted on, and a timer whose action also waits for a
completion is withheld until that completion's step polls again.
`include/vsr.h` is unchanged; `docs/changes.md` lists the refinement under
"API and driver semantics".

## Harness and scheduler additions

`tests/lib/memory_cluster.[ch]`:

- Link model: `mem_cluster_model_links(cluster, true)` makes a SEND to a
  node that is crashed, unknown, or whose link the driver cut
  (`mem_cluster_set_link(cluster, from, to, up)`) execute to RETRY without
  carrying a message, as the I/O layer's link module does. Because the core
  sends again at once on RETRY, the completion is held until the sender's
  clock has advanced `retry_ns` past the failure (`mem_effect.held/due`),
  as the engine's deferred completions do (decision 129):
  `mem_node_complete` returns false for it until then, so an eager driver
  (`mem_cluster_run`, the tests' `drive` loops) does not spin.
  `mem_node_notify` bypasses the hold, as before, for contract tests.
- Per-node clock: `mem_node.now/time_set`, the last TIME the incarnation
  consumed (reset on crash).
- Idle-deadline oracle, in `submit_graph`: at every return without `MORE`
  of a node that has consumed a TIME, the reported deadline is
  `VSR_NO_DEADLINE` or greater than that TIME. It runs on every step of
  every integration test and scheduler run, which is how the checkpoint
  case above was found.

`tests/fuzzy/scenario.[ch]`: flag `128` (`SCENARIO_LINKS`) turns the link
model on and, at every partition change (and in `heal`, and for a spawned
learner), cuts or restores the links across the partition
(`apply_links`). Profile `255` is now everything; `cluster_extended`'s
default union is `221` (`Makefile.am`). Flags `0..127` keep their
schedules: the new flag consumes no choices. Documented in
`tests/README.md`.

## Reproductions

- `tests/regression/dead_backup_apply.c`: three members, one crashed from
  the start, the link model on; 24 requests must each be replied to; then a
  RECONFIGURE (epoch bump), with START_EPOCH and NEW_EPOCH to the primary
  withheld, must commit, reply, and reach STEADY with the live backup's
  promise alone. Failed on the unfixed core at request 9 (`committed 9
  applied 8`); the scheduler with flag 128 failed on seeds 1, 2, 3 of
  profiles 128, 136 and 255 alike (the harness oracle or the liveness check
  at heal). The withheld announcements matter: with them flowing, even the
  old every-peer rule completed the reconfiguration, because the live
  backup received the final COMMIT, entered the epoch, and its START_EPOCH
  pulled the primary in after it (mutant M2 below survived until then). The
  old rule's deadlock was therefore for ordinary entries; at the boundary it
  cost a round trip through a backup and depended on one being up to date.
- `tests/regression/held_store_deadline.c`: the lost-store case (RECOVER
  with the RECOVERY load answered NOT_FOUND and every other effect held)
  and a backup whose view fence STORE is held while its primary is silent;
  both check at every TIME and every idle drain that the deadline is after
  the time. The first case failed on the unfixed core at t=1; the second
  is killed by mutants M6 and M7 below.

Both are out of `XFAIL_TESTS`, which is empty.

## Campaigns

All from `build/asan` on the final tree. The documented table
(`docs/implementation.md`), including the rows added for the link model:

| Command | Seeds | Result |
| --- | --- | --- |
| `cluster 1 1 600 quiet 127 200` | 200 | pass |
| `cluster 1 1 2000 quiet 127 32` | 32 | pass |
| `cluster 1 1 600 quiet 34 100` | 100 | pass |
| `cluster 1 1 2000 quiet 2 32` | 32 | pass |
| `cluster 1 1 600 quiet P 32` for `P` in 2, 3, 32, 127 | 32 each | pass |
| `cluster 1000 1 600 quiet P 1000` for `P` in 2, 3, 6, 7 | 1000 each | pass |
| `cluster 1 1 2000 quiet P 150` for `P` in 6, 7 | 150 each | pass |
| `cluster 1 1 600 quiet P 200` for `P` in 128, 255 | 200 each | pass |
| `cluster 1 1 600 quiet P 100` for `P` in 129, 130, 131, 132, 136, 144, 160, 192, 221 | 100 each | pass |
| `cluster 1000 1 600 quiet P 300` for `P` in 134, 135, 136 | 300 each | pass |
| `cluster 1 1 2000 quiet P 32` for `P` in 221, 255 | 32 each | pass |
| `cluster 1 1 600 quiet P 300` for `P` in 44, 172 | 300 each | pass |
| `cluster 1 1 600 quiet P 200` for `P` in 36, 164 | 200 each | pass |

33 campaigns, 8,024 seeds, of which 2,764 under the link model; no failure.
Three earlier runs of the queue found what the final tree fixes: every
fourth seed of the profiles with flag 2 failed the new deadline oracle on
the checkpoint advertisement timer (fixed in the deadline commit); seed 120
of profile 255 stopped in `heal` on the harness's own livelock (fixed in
the `mem_cluster_run` commit; see its message); and, from the mutation
hunt's wider profiles, seed 135 of profile 44 stopped in `heal` on the
cache placement conflict at a boundary (fixed in the proposal-fence
commit), which is why the last two rows, partitions with membership
changes, were added to the documented list. On the tree before the core
fixes, with only the harness changes, nearly every seed of every profile
with flag 128 failed (the apply gate).

## Mutation

Each mutant was applied to the source, rebuilt in `build/gcc`, and run
against the regression and integration programs and then a mini campaign
(24 seeds of profiles 2, 3, 6, 7, 128, 255, 221, 127, 34); the first
failure is the killer.

| Mutant | Change | Result |
| --- | --- | --- |
| M1 | gate: peers exactly at the boundary not waited for (`reached >= op` to `>`) | killed by `next_epoch_before_steady` |
| M2 | gate: every peer waited for (the old rule, boundary only) | killed by `dead_backup_apply` (the announcement-withheld reconfiguration; it survived the case with announcements flowing, see above) |
| M3 | gate: the primary condition dropped (backups gate too) | survived: equivalent. A backup's peer cursors are never advanced (`vsr_protocol_normal` zeroes them, only the primary moves them), so `reached >= op` is false for every peer and the gate is false on a backup; the condition documents intent |
| M4 | apply gate: `first > notified_commit` dropped from `apply_poll` | survived: equivalent. The batch loop breaks at `n > notified_commit` on its first entry, so nothing is applied; only the same log load the notification issues can be issued early |
| M5 | deadline: recovery round timer reported while the hard state is stored | killed by `held_store_deadline` |
| M6 | deadline: view round timer reported while the fence is stored | killed by `held_store_deadline` |
| M7 | deadline: election timer of a view round reported while the fence is stored | killed by `held_store_deadline` |
| M8 | deadline: round timer reported during the transfer the round selected | killed by `integration/view_change_edges` |
| M9 | checkpoint: the deferred capture ends the poll before the advertisement (old code) | killed by `cluster 1 1 600 quiet 2 24` (seeds 9, 11) |
| M10 | checkpoint deadline: an advertisement round in progress reported | killed by `cluster 1 1 600 quiet 2 24` (seed 17) |
| M11 | store: a due batched append keeps its deadline (old code) | killed by `integration/clients` (its batching-delay preset) |
| M12 | epochs deadline: an idle handoff module reports its timer (old code) | survived: no test reaches an IDLE stage with a stale `retry_at`; the clearing on entering a STEADY epoch is belt and braces for the recovery path into a STEADY history |
| M13 | fetch: an unanswered fetch keeps waiting at expiry (old code) | killed by `cluster 1 1 600 quiet 2 24` (seed 7) |
| M14 | discovery: an unanswered discovery keeps waiting at expiry (old code) | killed by `cluster 1 1 600 quiet 3 24` (seed 24) |
| M15 | warm-up: the warm timer reported without its gate (old code) | survived: `learner_follows`, `learner_restart`, `integration/epochs`, and 1,700 learner seeds (100 each of profiles 32, 160, 255, 125; 500 each of 160 and 32; 100 each of 160 and 255 at 2,000 steps; 300 of 44). Reachable only if a learner's fence STORE, or its wait for the group's promises after `learner_installs`, outlives a warm-up timer armed just before; `warm_poll` acts on the timer at the first poll after it expires while the learner is idle, so the window is a few actions wide and the seeded runs did not produce it. The one failure the hunt produced, seed 135 of profile 44, failed on the fixed core too (the cache placement conflict above) and is not a kill |

Eleven of fifteen killed; the four survivors are analysed in the table.
The killers of M9, M10, M13 and M14 are the harness's idle-deadline oracle
in seeded runs: those timers stay expired only when a retry cannot be
issued for want of budget, which the exact-minimum profiles produce. M2
was killed only once the reconfiguration case withheld announcements from
the primary, which is why that variant exists.

## Checks

- clang 21, ASan+UBSan (`build/asan`, `../../configure --enable-werror`):
  `make check` green, 75 of 75 programs, on the final tree.
- gcc 16 (`build/gcc`, `CC=gcc ../../configure --enable-werror
  --disable-sanitize`): `tests/lib/io_world.c` does not compile under gcc
  16's `-Werror` (`-Wduplicated-branches` at line 2397 and
  `-Wformat-truncation` at line 2456), which is the sibling branch's file,
  unmodified here and failing the same way on the base commit `6e6a636`.
  Built with `make -k`, every other program links; all 71 of them pass when
  run directly (automake's `check` refuses to run while four programs,
  `integration/engine`, `streams`, `snapshots`, `uring_faults`, cannot be
  built). The core and its tests are gcc-clean.
- `make format-check` clean.
- `make compile-commands && make lint` (`build/lint`): `format-check` and
  `shellcheck` pass; `tidy` stops on `tests/integration/engine.c`,
  `snapshots.c`, `streams.c`, `uring_faults.c` and `tests/lib/io_world.c`
  (findings in `io_world.h`, `streams.c`, `uring_faults.c`), the sibling
  branch's files, whose fixes are on main (`78dd426`) and are not repeated
  here by the coordinator's instruction, so `cppcheck` is not reached by
  that target. Run directly with the Makefile's invocations against the
  same `compile_commands.json`, clang-tidy and cppcheck are both clean on
  every file this branch changed: `src/protocol.c`, `src/transition.c`,
  `src/epochs.c`, `src/checkpoint.c`, `tests/lib/memory_cluster.c`,
  `tests/fuzzy/scenario.c`, `tests/regression/dead_backup_apply.c`,
  `tests/regression/held_store_deadline.c`.

## Contract change

None to `include/vsr.h`. Two documents were refined to state what the core
now guarantees: `docs/protocol.md` (the boundary's final COMMIT goes to the
peers up to date with it, and no other commitment waits on a send) and
`docs/vsr-api.md` (a return without `MORE` never reports a deadline at or
before the last accepted time). Both are listed in `docs/changes.md`.
