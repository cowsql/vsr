# Engine part 2, phase 2: the engine end to end

Worktree build/wt/engine2, branch `wt/engine2`, from main e7a7e63 (engine
phase 1 merged). Not pushed. This phase proves the engine with the
integration tests of the testing plan (docs/io-implementation.md section
9: `engine`, `streams`, `snapshots`, `uring_faults`) over one shared
harness, `tests/lib/io_world`, and fixes what they found: four I/O-layer
bugs (placeholder decisions G1 to G4 in docs/io-design.md section 10;
the coordinator renumbers them), one core bug fixed in a separately
labelled commit, and two core bugs left open as XFAIL reproductions.

Commits (first parent):

| Commit | What |
| --- | --- |
| 1bd1c69 | `tests/lib/io_world` and `tests/integration/engine` |
| 74956cb | Failing test: a full node send queue loops within one poll (`test_queue_full`) |
| 5c256d7 | G1: an evicted SEND's RETRY goes through the deferred ring |
| a325323 | `tests/integration/streams`; two cases fail on an idle source |
| 7c10b7a | G2: timers armed at completion time count from the next poll |
| e7aa0c5 | `tests/integration/snapshots` (and a harness overrun fixed) |
| e6a8b74 | XFAIL `tests/regression/dead_backup_apply` (core) |
| 3933bb1 | XFAIL `tests/regression/held_store_deadline` (core) |
| 9846556 | `tests/integration/uring_faults`; its groups fail on AF_UNIX links |
| 2110e52 | Failing conformance check: no zero-copy send on AF_UNIX |
| 7c964c2 | G3: a link sends plain once its socket refuses zero-copy |
| fb875dd | Write-error cases; the ring's replicated group fails in the core |
| 62a2ece | Core: `compare_chunk` waits for its comparison load |
| be0d654 | Failing test: a burst overflows a paused stream link (`test_burst`) |
| 6e6a636 | G4: a stream link holds a run per pool slab |
| cc35a0a | The harness checks its snprintf results (gcc 16's format-truncation) |
| 5fe0787 | `tests/regression/compare_load_order`: the core fix above over the simulation |
| 98db249 | Docs: section 9 rows, section 11, tests/README.md |
| 78dd426 | clang-tidy and Cppcheck findings in the harness and tests |
| (last) | This file |

## The harness: tests/lib/io_world

One header (`tests/lib/io_world.h`, the full API is commented there) and
one source, linked by every new integration test (`IO_WORLD` in
Makefile.am). A world holds up to five nodes; each node is one engine
(`struct vsr_io`) over its own executor:

- Backends: `iow_open_sim(nodes, seed, faults)` (one `vsr_sim` world,
  virtual time, replay by seed) or `iow_open_uring(nodes, seed)` (one
  ring per node in this thread, abstract AF_UNIX addresses named after
  the pid, stores in `iow.<pid>.XXXXXX` under the build directory, real
  time; returns false without a ring, and the test exits 77).
- Executor stack, engine down: `pure_executor` (armed around
  `vsr_io_complete`, `poll`, `submit`, `prepare`, `close` and the node
  calls; any executor call there aborts), the hook (targeted rules on
  submitted records: `IOW_FAIL` rewrites the result of every completion
  of a matched record, `IOW_FAIL_CHAINED` makes a matched FILES_UPDATE
  fail in the kernel so its LINK chain is cancelled as a real failure's
  is, `IOW_HOLD` keeps completions until `iow_release_held` or, newest
  first, `iow_release_held_reversed` (`iow_hold` holds again after a
  release); a failed submission; counts), the
  optional `faulty_executor` (`iow_node_faulty` before the node opens),
  and the base (the sim handle or the ring). The executor's tables are
  registered on both backends as a process would.
- Memory: every engine, payload pool, replica and tail lives in one of
  two banks per node, mapped anonymously on first use and kept for the
  process (io_uring pins only anonymous memory; a large static array can
  start in `.data`'s file-backed last page, which the ring refused). A
  restart uses the other bank, so a ring still tearing down never shares
  pages with its successor.
- The application (`struct iow_app`, one per replica): a digest state
  machine. Every COMMAND folds into a 64-bit FNV digest whose value after
  the command is its result. Every APPLY and INSTALL is checked against
  one history per cluster (the digest at every op, the first time any
  replica reaches it), every EXECUTED reply against the first reply for
  its (cluster, client, number): exactly once, across view changes,
  crashes, restores and retries. CAPTURE puts (digest, op) in the
  manifest; FETCH completes with the task's checkpoint at once or, with
  `fetch_by_stream`, after pulling the application image from the source
  node's application over a caller stream (a request of kind APPSNAP);
  INSTALL takes the digest back; DROP forgets the image. Leases are kept
  until their RELEASE op and counted at detach. `hold_apply`,
  `capture_status`, `fetch_status` change its behaviour; `sync_ns` and
  `failed_ns` time its SYNC and first failure.
- Streams: a request that starts with `IOW_STREAM_MAGIC` names a seeded
  pattern (length, piece per WRITE, BUFFERS or FILE writes from the
  node's caller file at `IOW_CALLER_SLOT`, a CLOSE status, a stop point,
  a refusal, `hold` to never close); the source writes it through the
  rails (AGAIN waits for WRITTEN), the requester checks every byte and
  offset. `hold_serve` and `hold_data` hold the rails back.
- The loop: `iow_iterate(n)` is one iteration of vsr-io.h's loop (reap,
  poll/step/submit until quiet, prepare, submit_and_wait: `want` 1 in
  the simulation, a non-blocking submit on a ring). Rail descriptors are
  copied out of the poll's array before any other engine call, since the
  next call may reuse their ring entries (decision 132). `iow_round()`
  runs every ready node, and in the simulation keeps running nodes that
  became ready during their own iteration before it advances the clock
  (vsr_sim_advance moves the clock past a ready node otherwise; a node
  ready only because the deadline it asked for passed, after doing
  nothing, is a spinning loop in real time, so the clock moves for it and
  `spins` counts it, aborting at 1000 in a row). On rings an idle round
  sleeps on the ring descriptors for at most a millisecond.
  `iow_run_until(pred, ctx, ns)` and `iow_run_for(ns)` arm a caller
  TIMEOUT record so the simulated clock stops at the end of the stretch
  (it used to jump to the next pending event, far beyond). Livelock
  checks abort an iteration whose engine stays runnable for 10000 poll
  rounds, and a stretch that takes 20 million rounds.
- Crash and restart: `iow_crash` crashes the executor (`vsr_sim_crash`,
  or the ring's deinit) and abandons the engine and its cores, never
  calling them again (not `vsr_io_deinit`, which is EBUSY until closed);
  applications keep their snapshot images (their disk) unless the test
  bumps the store's generation (a lost disk). `iow_restart` opens a new
  executor and engine with the world's node table and authorizations;
  on a ring it first waits until the abstract name binds again.
- Groups and helpers: `iow_group_open`, `iow_group_commit` (clients 1..4
  round robin, retried at the new primary on a non-OK reply),
  `iow_group_close`, `iow_mesh`, `iow_authorize` (kept and re-applied at
  restarts), `iow_reconfigure`, `iow_submit` (armed submit for tests that
  read the status), `iow_dump_node` (engine, links, peers, replicas,
  snapshot registry, core ops).

## The tests

tests/integration/engine (over the simulation; `IOW_TEST`, `IOW_SEED`):

- `test_attach_modes`: NEW; RECOVER (the committed prefix replayed from
  genesis, a retry of a client's last request answered from the recovered
  table, nothing executed); NEW over the used log (IDENTITY); RECOVER over
  an empty directory (NOT_FOUND, RECOVERING for good alone).
- `test_group_lifecycle`: a backup crashes and RECOVERs; the primary
  crashes, a view change, it RECOVERs as a backup; a checkpoint; a backup
  loses its disk and recovers through the group (NOT_FOUND, a fetched
  anchor); a JOIN learner warms from the anchor and a RECONFIGURE admits
  it; four replicas commit.
- `test_admission`: max_clients 4: a backup admits a fifth client while
  its table has room and the redirect ends it; once the tables are full
  the fifth is ELIMIT at the primary and at a backup; known clients and
  their retries go on.
- `test_close_ordering`: requests queued with a STOP dropped with their
  leases returned; a held APPLY keeps STOPPED and detach off; the STATUS
  STOPPED sitting in the ring keeps detach EBUSY; a replicated replica
  STOPPED with a record write held; `vsr_io_close` EBUSY with a replica
  attached, deinit EBUSY before closed; a caller stream cut by the source
  engine's close (CANCELLED there, RETRY at the requester).
- `test_minimum`: vsr-io.h's minimum slab count (and ELIMIT one below)
  with `caller_slabs` 0, file slots at the documented minimum; commits,
  two checkpoints, a JOIN learner's fetch.
- `test_two_groups`: two groups on the same three engines through a crash.
- `test_learner_down`, `test_queue_full` (G1), `test_write_errors`.

tests/integration/streams: `test_transfers`, `test_backpressure`,
`test_burst` (G4), `test_early_ends`, `test_idle_source` (G2),
`test_loss`, `test_faults`, `test_beside_group` (each described in its
comment and in section 9).

tests/integration/snapshots: `test_fetch_restore`, `test_sync_drop`,
`test_corrupt_source` (123), `test_prepare_wake` (122).

tests/integration/uring_faults (real rings, exit 77 without one):
`test_group_faults`, `test_write_errors`, `test_registrations`,
`test_creation_error` (125), `test_stop_held`.

Also: `executor_conformance` checks the AF_UNIX zero-copy refusal on
every column; `tests/unit/link` checks the deferred eviction;
`tests/unit/deadline` gains `test_rebase`; the regression
`compare_load_order` holds the core fix (item 5 below) over the
simulation.

## Bugs found and fixed

1. G1, a hot loop (`test_queue_full`; first seen as a seeded run of
   `test_group_lifecycle` never finishing one poll). A full node queue
   evicted its oldest unstarted SEND with RETRY at once, inside the
   routing of the newer SEND; the core re-sends on a failed SEND (the
   epochs extension restarts its whole START_EPOCH broadcast), which
   evicted again. Evictions now complete through the deferred ring,
   `retry_ns` later, like refused SENDs.
2. G2, idle engines killed new links (`test_idle_source`, and the last
   case of `test_early_ends`). Timers armed while completions are
   processed counted from the previous poll's time; an engine asleep
   without a deadline armed an accepted link's handshake timer already
   past, and the next poll closed the link ETIMEDOUT. Deadlines armed in
   `vsr_io_complete` are marked and moved by the next poll or prepare.
3. G3, AF_UNIX links never worked on a ring (`uring_faults`' groups, the
   `unix_socket` conformance scenario). Linux has no zero-copy send on
   AF_UNIX and the engine's pool sends are zero-copy; the simulation
   accepted them. The link falls back to plain sends at the first
   `-EOPNOTSUPP`; the simulation now refuses like the kernel.
4. G4, bulk streams ended RETRY (`test_burst`; 24 of 40 seeds of the
   streams suite). The whole reap batch is carved in `vsr_io_complete`
   before the caller can complete a DATA op, so a window of two blocked
   the third chunk and the rest of the batch overflowed the paused
   link's eight held runs (`-ENOBUFS`). Stream links now hold a run per
   pool slab (the bound of what the kernel can deliver before the
   CANCEL lands); peer links keep eight.
5. Core (fixed in its own commit, for the coordinator to review): a
   replicated group on a ring fenced a backup INVARIANT when it restarted
   with RECOVER (`uring_faults` `test_write_errors`, with or without
   faults). `transition.c`'s `compare_chunk` advanced through entries a
   concurrent replay LOAD had cached while its own comparison LOAD was
   outstanding;
   the chunk was replaced and the stale load compared at the new offset.
   The comparison now waits for its load (one line moved up). The
   documented core campaigns pass with it (numbers below). A trace of
   the ring run showed the order: the replay's LOAD of the entries was
   issued before the chunk arrived and completed after the comparison's
   was issued. `tests/regression/compare_load_order` reproduces it over
   the simulation: a backup crashes with 20 entries, the group commits 6
   more, the backup RECOVERs with its cold LOAD reads held and completed
   in batches, oldest first (reversed completions, tried first, give
   the harmless order). Without the fix it fences the backup INVARIANT
   at every batch period (1 to 20 ms) and seed tried; the ring run
   (`uring_faults` `test_write_errors`) fails in 5 of 7 runs.
6. Harness bugs on the way (no library change): the simulated clock
   jumping past ready nodes and past the end of `run_for`; a stream
   request copied into a buffer 36 bytes short.

Open core bugs, XFAIL (tests/regression/README.md has both):
`dead_backup_apply` (a primary stops applying once an unreachable
backup, whose SENDs the engine completes RETRY, is more than
`batch_entries` behind: the apply gate needs a COMMIT that high issued
to every peer, and a failed SEND rewinds the peer's `sent`) and
`held_store_deadline` (a RECOVERING replica reports a deadline already
past while its hard-state STORE is held: the recovery round's
`retry_at`; excluding it from `vsr_transition_deadline` while
`!hard_safe` makes the reproduction pass, checked and not committed).
The engine tests keep a member away for fewer commits than a batch.

## Checks and flakiness

On the final tree (Linux 7.2.6, 8 cores, RLIMIT_MEMLOCK 8 MiB):

- `make -j6 check`, clang 21 with ASan+UBSan (`--enable-werror`): 76
  tests, 74 PASS, 2 XFAIL (`dead_backup_apply`, `held_store_deadline`),
  no FAIL, XPASS or SKIP. gcc 16 (`--enable-werror --disable-sanitize`):
  the same. gcc's format-truncation analysis needed cc35a0a.
- `make format-check` and `make compile-commands && make lint` (format,
  ShellCheck, clang-tidy, Cppcheck) clean; the phase's tests had never
  been linted, 78dd426 clears what they found.
- Repeated runs under ASan, 0 failures: `engine`, `streams` and
  `snapshots` 30 times each at their own seeds and at `IOW_SEED` 1 to
  200 (every case of the suite on a different seed each time);
  `uring_faults` alone, 30 times at the default seed and at `IOW_SEED` 1
  to 50 (the fault wrapper's seed); `compare_load_order` at `IOW_SEED` 1
  to 21, each at batch periods 1 to 8 ms. After 78dd426 (a field
  reorder), 20 more seeds of every suite and of `uring_faults` passed
  too, and the two XFAIL reproductions failed in 10 of 10 runs each (the
  memory cluster, deterministic).
- `compare_load_order` with 62a2ece reverted: fenced INVARIANT in every
  run tried (seeds 1 to 3 at periods 1 to 8, 10, 12, 16, 20 ms; seeds 2
  to 21 at 1, 4 and 8 ms); `uring_faults` `test_write_errors` failed 5
  of 7 runs.
- The core campaigns of 62a2ece, re-run on the final tree (gcc build),
  all passed: `tests/fuzzy/cluster 1 1 600 quiet 127 200` (200/200), `1 1
  2000 quiet 127 32` (32/32), `1 1 600 quiet 34 100` (100/100), `1 1
  2000 quiet 2 32` (32/32), `1 1 600 quiet P 32` for P in 2 3 32 127
  (32/32 each), `1000 1 600 quiet P 1000` for P in 2 3 6 7 (1000/1000
  each), `1 1 2000 quiet P 150` for P in 6 7 (150/150 each): 4792 seeds.
- A ring test that aborts leaves its `iow.<pid>.XXXXXX` store directory
  in the build directory (a passing one removes it).

## What the iocluster fuzzer needs from the harness

- Deterministic replay: the simulation backend replays from the world
  seed as long as the fuzzer draws its choices from `vsr_sim_random` in a
  fixed order; the engine draws its entropy once at init (134).
  `IOW_TRACE=1` prints the simulation's trace and every iteration.
- Nodes and roles: `iow_node_open`, `iow_attach`/`iow_attach_with` (any
  start mode, a store generation for a lost disk), `iow_mesh` and
  `iow_authorize` for learners, `iow_reconfigure` for membership.
- Crashes exactly as the plan wants: `iow_crash` after `vsr_sim_crash`
  (buffer ownership verified with the memory valid), the engine
  abandoned; `vsr_sim_set_faults` for torn writes (`unsynced_keep_ppm`),
  bit rot, network faults; `iow_restart` and RECOVER.
- Oracles: the digest history per cluster and the exactly-once reply
  table are checked as ops arrive. Missing for the fuzzer: a
  linearizability checker over client invocations (the harness records
  replies, not invocations), a durability oracle (record SYNC
  completions per replica and compare after torn-write crashes), and
  convergence after faults stop (`iow_group_applied` is the building
  block).
- The application: replace the digest with examples/kv.c's state
  machine behind the same forwarded ops (the harness's
  `complete_apply`/`app_capture`/`app_install` are the seams);
  vsr-client.h clients would submit through `iow_make_request`.
- INPUT_BLOCKED coverage (M19): the minimum budgets are reachable through
  `iow.limits` before attach (flag 2); `iow_submit` returns ELIMIT and
  AGAIN to a fuzzer that answers LIMIT itself.
- Faults not in the simulation: the hook (fail, fail in the chain, hold,
  release newest first) and `faulty_executor` over the simulation or a
  ring.
- Budget the known core bugs: until `dead_backup_apply` is fixed, a
  schedule that keeps a member unreachable for more than `batch_entries`
  commits stalls the group; the harness's spin counter aborts at 1000
  idle past-deadline iterations in a row (raise it or fix
  `held_store_deadline` first).

## What the networked examples need

- AF_UNIX works on rings since G3; TCP loopback needs no change.
- A ring caller registers the executor's file and buffer tables itself
  (`register_files`, `register_buffers`) before `vsr_io_init`, maps its
  regions anonymously (the payload pool and tails are registered), and
  after a crash-like teardown waits for the listen address to bind again
  before a new engine's first prepare (a failed bind is fatal).
- Memory: registered memory counts against RLIMIT_MEMLOCK per user; the
  uring_faults configuration (24-25 slabs of 16 KiB, 32 KiB pinned
  payload) registers well under 1 MiB per engine.

## Open items

In docs/io-implementation.md section 11: the two core XFAILs; a REQUEST
the core refuses at the step gets no REPLY; G2 moves armed timers, not
the times completions record; the decision-96 linger residual, now
described by what the tests hit (the linger ends while a requester still
drains the socket buffer: the source cannot see the requester read, so a
slow consumer of the socket buffer's worth gets RETRY; a progress frame
from the requester would fix it and is a wire change), and what is left
of decision 99's after G4 (interleaved deliveries splitting a slab; a
paused link keeping the slabs it holds from the other links). Also: the
simulation takes up to 256 KiB per send, so a source's END reaches the
kernel before an early loss and keeps its status (decision 97): tests of
a source's CANCELLED or RETRY must keep the source open (`hold`).
