# Contract changes since the baseline

Every refinement of the public contract made between commit `0ec4aa9`, the
review baseline before the completion effort, and this tree, grouped by area.
`include/vsr.h`, `docs/protocol.md`, and `docs/vsr-api.md` changed;
`DESIGN.md` did not. Each entry names the change, the reason, the section it
lives in, and the test that covers it. Tests are named by program
(`tests/regression/NAME`, `tests/integration/NAME`) and, where useful, by the
scenario function inside it; "scheduler flag" refers to the profile flags of
`tests/fuzzy/cluster` described in `tests/README.md`.

## Epochs and learners

- Learners collect handoff promises and never retire. A current member that
  has already promised answers `START_EPOCH` from an authenticated learner
  outside both groups with a unicast `EPOCH_STARTED`, adding no vote and no
  retention obligation; a learner establishes STEADY by collecting the same
  promises, and one that installs through a boundary returns to WARMING
  rather than RETIRED. A JOIN replica with a stale seed learns the committed
  epoch from a seed member's `NEW_EPOCH`. Why: a learner behind a handoff
  either retired as if removed or waited at INSTALLED for promises nobody
  sent it. Where: `docs/protocol.md` "Epoch handoff and witnesses";
  `docs/vsr-api.md` "Log and checkpoint transfer"; `include/vsr.h`
  `VSR_MSG_EPOCH_STARTED`. Tests: `integration/epochs`
  (`stale_seed_discovery`, `late_admission`), `regression/learner_follows`.
- A restarted learner resumes warm-up; quorum recovery is for members. A
  replica in neither group of its configuration resumes nonvoting warm-up in
  its persisted role or, when no store survives, in `join_role` from its seed
  (`MEMBER_NONE` warms as a witness). It never recovers from a group it does
  not belong to and never persists a role its own validation rejects. Why: a
  JOIN learner restarted with `RECOVER` entered quorum recovery against a
  group it was not a member of and never finished. Where: `docs/protocol.md`
  "Restart and persistence"; `docs/vsr-api.md` "Initialization" (`RECOVER`
  row and the paragraph after the table); `include/vsr.h` `join_role`.
  Tests: `regression/learner_restart`; scheduler flag `32`.
- Warm-up is continuous. An idle learner periodically rediscovers the members
  it knows, catches up to their latest commitment, follows a later committed
  epoch and establishes STEADY there, and never votes, acknowledges
  preparation, or answers recovery or read probes. Why: a warmed learner
  stopped at the position where warm-up left it, so admission transferred
  everything warm-up was meant to save. Where: `docs/protocol.md` "Epoch
  handoff and witnesses"; `docs/vsr-api.md` "Initialization". Tests:
  `regression/learner_follows`, `regression/learner_restart`.
- Boundary commitment can be learned in any state. Commitment of a
  `RECONFIGURE` boundary may arrive in VIEW_CHANGE or during log selection,
  because its quorum was collected in the old epoch; entering the epoch
  abandons every old-epoch round, a replica that already holds the history
  installs directly, and it is TRANSFERRING only while history is missing.
  Why: a boundary learned from `VIEW_CHANGE` left the member electing
  forever in the new epoch. Where: `docs/protocol.md` "Epoch handoff and
  witnesses". Test: `regression/boundary_from_view_change`.
- The next epoch's boundary is STEADY evidence. A later boundary committing
  in the current epoch proves the current handoff completed, because its
  primary admitted it only after its own STEADY; a member or learner that
  commits it before collecting promises finishes the handoff locally, and a
  removed donor retires through it. Why: a backup INSTALLED but still
  collecting promises was fenced with `VSR_FAILURE_INVARIANT` when it
  committed the next `RECONFIGURE`. Where: `docs/protocol.md` "Epoch handoff
  and witnesses". Test: `regression/next_epoch_before_steady`.
- Installation and warm-up may fetch from any NORMAL full member of either
  group and reselect after a bounded number of unanswered chunk fetches; a
  recovery keeps its highest-view primary as its sole source. Why: a removed
  donor retires once the new group is ready and a crashed one may never
  return. Where: `docs/protocol.md` "Epoch handoff and witnesses". Tests:
  `integration/view_change_edges` (`learner_rotates_donor`),
  `regression/review_epoch_recovery_vote`.

## Replication

- Application waits on no send. A primary notifies, and applies, a committed
  entry as soon as it is committed and present; only the RECONFIGURE that
  ends an epoch waits, for its COMMIT to be issued in the old envelope to
  every peer up to date with the boundary (its PREPARE went out and was not
  rewound since; the acknowledging quorum among them), one send each, so
  the wait is bounded by sends in flight and never by a peer. Peers behind
  or unreachable learn the boundary from the handoff announcement and
  transfer the history they hold. Why: the poll
  advanced its commit notification, which gates application, only once a
  COMMIT at least that high had been issued to every peer; a peer more than
  a batch behind only ever received PREPAREs whose `committed` was capped at
  the batch's end, and a failed SEND or the retry timer rewound its cursor to
  its last acknowledgment, so with one backup of three unreachable (its
  SENDs completing `RETRY`, as the I/O layer answers a SEND to a node it has
  no link to) request `batch_entries + 1` committed and was never applied.
  Where: `docs/protocol.md` "Epoch handoff and witnesses". Tests:
  `regression/dead_backup_apply`; scheduler flag `128` (the memory cluster's
  link model, `mem_cluster_model_links`).

## Recovery

- Learning a later epoch never substitutes for quorum recovery. A replica
  whose state does not qualify it to vote, because it is recovering from
  missing or replicated-mode state or restarted from a RECOVERING record,
  restarts quorum recovery in the learned epoch against that epoch's
  membership and quorum, and again for each further epoch learned meanwhile.
  The single-donor transfer is reserved for a replica whose own state is
  authoritative through its committed prefix and that cast no vote in the
  learned epoch: an old-epoch member learning the boundary, a durable restart
  of an unfinished installation, or a learner. A removed member that lost its
  state needs only the history through the boundary in order to retire. With
  `f = 0` in the learned epoch the recovery quorum cannot form and one NORMAL
  member's history is authoritative. Why: the handoff installed one donor's
  lagging history and let a lost-state member rejoin as a voter, so a later
  view elected a log missing commands the unreachable primary had
  acknowledged. Where: `docs/protocol.md` "Epoch handoff and witnesses".
  Tests: `regression/review_epoch_recovery_vote`,
  `regression/epoch_announcement_recovery`.
- A recovering replica offers no discovery. Its view and last normal view
  are unvalidated pre-crash metadata, so it does not answer `GET_STATE`
  discovery, although it still serves range fetches of a revision it
  published before recovering. Why: a discovery offer would let a peer
  install unvalidated metadata as a NORMAL history. Where:
  `docs/protocol.md` "Restart and persistence". Tests:
  `regression/review_epoch_recovery_vote`,
  `regression/epoch_announcement_recovery`; scheduler flag `8`.
- A reconfiguration must tolerate the members already without state. A
  member of the new group that lost its state rejoins only through quorum
  recovery there, with `q` responses from the other members, and readiness
  needs `q` promises, so a membership naming more such members than its `f`
  can never recover them or reach STEADY. This is a precondition on the
  adapter that authorizes RECONFIGURE, not a core rule: the core cannot tell
  a slow member from one without state. A tolerated lost-state member, even
  the designated primary of the new epoch, recovers from the installed
  members, which are NORMAL and change view around a silent primary. Why:
  the seeded scheduler lowered `f` from 2 to 1 while two members were
  crashed (`tests/fuzzy/cluster 1823 1 600 quiet 120` and `1536 1 600 quiet
  127`), and both lost-state members waited forever for a fourth recovery
  response from three NORMAL members while the handoff waited for their
  promises. Where: `docs/protocol.md` "Epoch handoff and witnesses";
  `docs/vsr-api.md` "Requests, replies, and reads". Tests:
  `regression/recovery_into_handoff`; the scheduler proposes only
  memberships that tolerate their unavailable members.

## Checkpoints and trim

- A primary never resends entries below its retained log. A backup
  acknowledged below that log receives `COMMIT` and catches up through state
  transfer: a checkpoint `FETCH` on a full replica, the anchor only on a
  witness. Why: the retry path loaded a compacted prefix from a revision
  whose `TRIM` was readable but not yet durable, and the unexplained hole
  fenced storage. Where: `docs/protocol.md` "Preparation and view change".
  Tests: `regression/trim_resend`, `integration/transfer_audit`
  (`offer_after_trim_completion`).
- Witnesses adopt remote anchors without fresh promises. Installing a remote
  anchor during recovery or state transfer needs no `f + 1` advertisements: a
  witness holding the anchor's own boundary entry keeps its retained prefix,
  and otherwise it replaces only a prefix it never prepared. Coverage is
  required only for retained entries a `RESTORE` or `TRIM` discards; a
  witness in a transfer defers adopting an advertised anchor until the
  transfer completes. Why: a recovering witness waited forever for
  advertisements that only capturing members send, an anchor adopted
  mid-transfer trimmed under the comparison cursor, and a witness that
  restored over its own prefix deadlocked a lost full member's recovery.
  Where: `docs/protocol.md` "Checkpoints and immutable offers";
  `docs/vsr-api.md` "Log and checkpoint transfer" (witness paragraph).
  Tests: `regression/recovery_coverage`, `regression/transfer_anchor`,
  `regression/witness_prefix`, `integration/normal_contract`
  (`witness_rejoins_trimmed_primary`).

## Storage loads

- No `LOAD` below the retained range. The core never issues a `LOAD` below
  the range the named revision retains: a `TRIM` or `RESTORE` bounds later
  loads as soon as its `STORE` completes, before it is durable, even though
  offers keep describing the safe revision. Why: the adapter may report
  `NOT_FOUND` for a compacted range, and the trimmed-prefix resend above made
  the core treat that as an unexplained hole. Where: `docs/vsr-api.md`
  "Indexed storage". Tests: `regression/trim_resend`,
  `integration/transfer_audit`.

- Range fetches are batched. A `GET_LOG` or range `GET_STATE` asks for as
  many entries as `batch_entries` and the requester's free payload budget
  allow (`fetch_budget`), and a comparison load covers the whole overlap in
  as many bounded loads as the budget takes; a local history load is sized
  the same way. Why: every transfer fetched one entry per round trip, so a
  backup missing fifty committed entries needed fifty `GET_LOG` exchanges.
  Where: `docs/vsr-api.md` "Log and checkpoint transfer". Tests:
  `integration/transitions` (`batched_log_transfer`, which counts the
  fetches: fifty before, at most eight after).

## Resource minima

- Capture hints defer under input pressure. A `CHECKPOINT` hint may be
  deferred while retained inputs leave less than the capture baseline;
  deferral releases only applied cache entries whose release restores that
  baseline and never blocks a transition. Why: at the exact minimum lease and
  payload budgets a hint whose baseline was unavailable held transitions
  busy, a scheduling cycle that first looked like a reservation leak. Where:
  `docs/vsr-api.md` "Log and checkpoint transfer" (`CHECKPOINT` paragraph).
  Tests: `integration/normal_contract` (`lagging_peers_minimum_cache`),
  `integration/capture` (`capacity_deferral`); scheduler flag `2`.
- The documented minima are unchanged: `input_leases >= transfers + 8` and
  the payload budget `message_bytes + 2 * command_bytes + (transfers + 3) *
  manifest_bytes + result_bytes` (`docs/vsr-api.md` "Initialization"). They
  were re-validated against the simultaneous hold categories by
  `integration/normal_contract` (`minimum_capacity`) and scheduler flag `2`;
  the stalls found there were fixed without raising a minimum
  (`regression/pressure_reload`, `regression/anchor_behind_applied`,
  `regression/reply_restore`).
- A drain never spends its progress undoing itself. A log entry loaded for a
  lagging peer is sent to that peer before another peer's load can evict it:
  the completion of the load resumes the send rotation at the peer it was
  issued for, and a peer whose SEND lacks only an operation slot keeps the
  rotation until a slot frees. Why: at the minimum payload budget with two
  operation slots, one busy, a poll running while the load was outstanding
  rotated past its peer, failed another pair's SEND, evicted that pair's
  cached entry under the load's reservation, and reloaded it before the
  loaded entry was sent, forever (`tests/fuzzy/cluster 1572 1 600 quiet 6`).
  Where: `docs/vsr-api.md` "Driving the core". Tests:
  `regression/lagging_reload`; scheduler profiles `2`, `3`, `6`, `7`.

## Reads

- No read-barrier rule changed. The learner rule above adds that a learner
  never answers read probes (`docs/protocol.md` "Epoch handoff and
  witnesses"). Read fences are checked by scheduler flag `64` with the host's
  read-fence oracle and by `integration/reads`.

## API and driver semantics

These clarifications were made in the final documentation pass after the API
conformance suite (`integration/api_contract`) found the wording ambiguous;
each states what `src/core.c` and `src/validate.c` already did.

- After `STOP`, an event other than `TIME`, `COMPLETE`, or `STOP` returns
  `VSR_EINVAL` and is not consumed, in STOPPING and STOPPED alike; FAILED and
  RETIRED instead consume other well-formed events without doing work until
  STOP. Where: `include/vsr.h` `vsr_step_many`; `docs/vsr-api.md`
  "Application operations and failures". Tests: `api_contract`
  (`test_stop_lifecycle`, `test_failure_latch`), `integration/lifecycle`
  (`stop_matrix`, `failure_matrix`).
- `vsr_status.configuration` is never NULL on an initialized instance: from
  `vsr_init` it describes the seed as a STEADY epoch with no predecessor and
  a zero boundary until a recovered or learned descriptor replaces it. Where:
  `include/vsr.h` `struct vsr_status`; `docs/vsr-api.md` "Initialization".
  Test: `api_contract` (`test_status_and_boot`).
- A `LOAD` completed with `FAILED` or `CANCELLED` fences storage exactly like
  corruption or unexpected absence; only `RETRY` and an expected `NOT_FOUND`
  do not. Where: `include/vsr.h` `struct vsr_op`; `docs/vsr-api.md`
  "Application operations and failures" (table). Tests:
  `integration/lifecycle` (`failure_matrix`), `integration/failures`.
- A `vsr_applied` whose `count` differs from the batch is an inconsistent
  application result, not a shape error: the completion is consumed and the
  replica fences with `VSR_FAILURE_APPLICATION`; a count above
  `batch_entries` is rejected unconsumed with `VSR_ELIMIT`. Where:
  `include/vsr.h` `struct vsr_op` (`APPLY`); `docs/vsr-api.md` "Application
  operations and failures". Test: `api_contract` (`test_inconsistent_apply`).
- Every `*_ns` option equal to `UINT64_MAX` is rejected with `VSR_EINVAL` by
  `vsr_layout` and `vsr_init`. Where: `include/vsr.h` `struct vsr_options`;
  `docs/vsr-api.md` "Initialization". Tests: `api_contract`
  (`test_layout_validation`), `unit/validate`.
- `vsr_init` clears `*out` on every error except when `out` lies inside the
  arena, which returns `VSR_EINVAL` without writing through `out`. Where:
  `include/vsr.h` `vsr_init`; `docs/vsr-api.md` "Initialization". Tests:
  `regression/api_init_out`, `api_contract` (`test_init_validation`).
- A deadline is a promise that time alone will wake the core into work: a
  return without `MORE` never reports a deadline at or before the last
  accepted time. Every expired timer has been acted on, and a timer whose
  action also waits for a completion is withheld until that completion's
  step polls again. Why: a replica RECOVERING from a lost store reported its
  recovery round's `retry_at`, set when the round began, while its hard-state
  STORE was outstanding, so a host waking at the deadline polled in a loop
  for the whole log creation; the same held for a view change's round and
  election timer, for discovery rounds, for the warm-up timer, for the
  election timer during a learner's transfer, for retries (a fetch, a
  served load, a batched append, a checkpoint advertisement) that stayed
  expired for want of an operation slot or budget, and for the
  advertisement timer while a capture hint deferred under input pressure
  ended the checkpoint poll before it (every fourth seed of the profiles
  with flag `2`). `include/vsr.h` is unchanged: this states what a deadline
  meant. Where: `docs/vsr-api.md`
  "Driving the core". Tests: `regression/held_store_deadline`; the memory
  cluster checks it at every idle return of every node, so every integration
  test and scheduler run does.

## Fixed without a contract change

Defects whose fix needed no new contract text, listed for completeness:
`regression/pressure_reload` (input-pressure relief waits for an idle protocol
poll so a pin the next poll consumes is not evicted and reloaded),
`regression/anchor_behind_applied` (a selected offer's checkpoint below the
applied position is not installed behind applied entries),
`regression/reply_restore` (a reply decided before a `RESTORE` replaced the
client base is decided again from the restored revision), and
`regression/compare_load_order`, found by `integration/uring_faults`
(`test_write_errors`: a transition's comparison waits for its own comparison
LOAD instead of advancing through entries a concurrent replay LOAD cached,
which let the chunk be replaced and the stale load be compared at the new
chunk's offset, fencing a healthy replica with `VSR_FAILURE_INVARIANT` when
LOADs completed out of order).
