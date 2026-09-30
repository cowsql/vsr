# Engine part 2, phase 1: the loop, the replica life cycle, the routing

Worktree build/wt/engine, branch `wt/engine`, from main fb747c3 with
main bf5a9d4 (the snapshot review) merged in. Not pushed. Every function
vsr-io.h declared and nothing defined is now implemented: the node API,
attach, detach, the replica accessors, `vsr_io_submit`, `poll`,
`prepare`, `complete` and `run`. Placeholder decisions 127 to 135 are in
docs/io-design.md section 10 (after main's 117 to 123; the coordinator
renumbers). docs/io-implementation.md sections 7.1 to 7.8, 8, 9, 10 and
11 describe the code as it is.

Commits (first parent):

| Commit | What |
| --- | --- |
| 0953251 | Pool shares (127 first form): `internal_taken`, the slab's `internal` flag, `vsr_io_pool_handoff`, `vsr_io_pool_floor` |
| 041fe43 | The engine proper: `src/io/replica.c`, `src/io/loop.c`, engine.c's node API, close and closing |
| 7717524 | tests/unit/engine.c, the first seventeen tests |
| d53ff02 | Tests for the bounds, timers and close that mutants showed unguarded |
| 927d851 | `VSR_IO_SQE_FILES_UPDATE` and `VSR_IO_SQE_PROVIDE` in the executor contract, uring.c, the sim, the fault wrapper, conformance |
| 279ba7d | Purity (133 to 135): the generator, link takeover by record, slot clears by record, PROVIDE from prepare, tests/lib/pure_executor |
| 9d9523a | 127's final form (reserve `links + streams * window + 4 * replicas + 1`, a ring slab per link), the snapshot test at `caller_slabs = 0` |
| 75f6b94 | Decisions 128 to 132, the plan's sections brought in line, vsr-io.h comments |
| cbc95a6 | Merge of main bf5a9d4 |
| 55dffc6 | A check that prepare surfaces a CAPTURE deadline armed at now (decision 122) |
| 49f2c38 | This file |
| 77397bc | Lint findings (clang-tidy, cppcheck, gcc 16's format truncation) |
| 6aa5d2f | The store review's engine items: detach asserts the store's queues are empty, `test_replicated_flush` |

## Source layout

The engine is three translation units, split so that tests/unit/stream.c,
which defines strong doubles of the snapshot hooks, links without
snapshot.o:

- `src/io/engine.c`: the kernel. Layout and init/deinit, close and
  `check_closed`, the forwarded ring (`vsr_io_forward`), leases, MESSAGE
  delivery, engine file slots and their clears (135), the generator (134),
  the reserve (127), caller slabs, the node API (thin wrappers over
  link.c). No reference to the store or the snapshot module.
- `src/io/replica.c`: `vsr_io_replica_layout`/`size`, attach, detach,
  `replica_status`/`core`/`find`.
- `src/io/loop.c`: complete, the routing, the step loop, poll, submit,
  prepare, run.

## Data structures (src/io/engine.h)

Per replica (`struct vsr_io_replica`, in the caller's metadata region,
laid out by `replica_plan` in replica.c: core arena first, then the
member and path copies, the rings, the scratch arrays, the leases, the
decode regions, the store, the snapshot module):

- `completions` [operations]: internal COMPLETEs (store, SEND, snapshot,
  LOAD results with engine leases). One per outstanding core op, so it
  never fills (asserted).
- `deferred` [operations]: completions the routing decided at once, each
  due `retry_ns` after routing (129). Due in order; the head is earliest.
- `priority` [operations + 1]: the caller's COMPLETE and STOP (128).
- `events` [limits.events]: the caller's REQUEST, CLIENT_QUERY, READ,
  CHECKPOINT.
- `messages` [regions]: MESSAGEs from links under engine leases.
- `step_events` [operations + (operations + 1) + events + regions + 1]:
  the scratch the step loop builds, every queue plus TIME.
- `step_ops` [step_capacity]; `leases` [regions] (states FREE, QUEUED
  while the event waits, LEASED once a step consumed it).
- `stopping` (a STOP was submitted), `status_pending`, `core_deadline`,
  four deadline handles (core, flush, sync, capture), `tail_region`.

Per engine (`struct vsr_io`, in the engine metadata region): the
forwarded ring [limits.ops] of `struct vsr_io_forwarded` (the op plus a
union of the rail descriptors, STATUS's `vsr_status` included, so op.data
points into the entry), `now` (monotonic), `random_state[4]`
(xoshiro256**), `clears` [file_slots] (slots waiting for their
FILES_UPDATE of -1), `provide_buffers` [slabs] (the PROVIDE record's
buffers, read at submission), `releases_rejected` and `events_rejected`
(internal counters), and `vsr_io_run`'s scratch (`run_sqes`, `run_cqes`
[batch], `run_ops` [ops], `run_events` [ops + events]).

user_data of every engine record: owner(8) | kind(8) | index(24) |
generation(24) (slots.h). Kinds added: `FILES` (a clear or a takeover's
FILES_UPDATE, index the engine slot) and `PROVIDE` (no slot:
`VSR_IO_USER_DATA(owner, PROVIDE << 48)`, checked before resolving).

Engine lease id: `VSR_IO_LEASE_ENGINE | replica << 40 | region << 16 |
generation`.

## Loop order

`vsr_io_poll(now)`: clamp `now` (never back), clear the wake, dispatch
due deadlines (LINK/DIAL to link.c, STREAM to stream.c; FLUSH, SYNC,
CAPTURE and CORE need nothing, the polls below handle them), links_poll,
streams_poll, then per attached replica: store_poll and snapshots_poll;
move due deferred completions into `completions`; if OPENING or RUNNING,
the step loop; store_poll and snapshots_poll again (an op just routed
starts at once; new completions mean MORE); STATUS if pending. Finally
copy the ring out and set MORE/OUTPUT_FULL.

The step loop (replica_step, at most 256 steps): drain the store's
completions; room = ops - forwarded_count (0: overflow, MORE); build
internal completions, priority, MESSAGEs (unless skipped), other events
(unless skipped), TIME (until consumed once); `vsr_step_many` with
capacity min(step_capacity, room); accept the consumed prefix (queued
leases become LEASED); route every op; on `r < 0` drop the refused event
(`drop_invalid`: caller lease returned by RELEASE, engine lease freed,
counted); on INPUT_BLOCKED at a MESSAGE or other-event, skip that queue
for the rest of the poll (at a completion: stop, retry next poll); stop
when a step made no progress and reported no MORE.

`vsr_io_prepare(now)`: provision (`vsr_io_pool_provide` into
`provide_buffers`, one place set aside), links, streams, per replica
store then snapshots, engine file clears, the PROVIDE record last;
deadline = earliest engine deadline, or the earliest deferred due, or now
when the batch filled. Capacity at least 5.

`vsr_io_run`: reap, dispatch by owner tag; loop {poll, step hook,
run_submit} until nothing moves; prepare plus the prepare hook; deadline
now if events wait; return on `stats.failure`, closed with no replica and
nothing to submit (OK), or an executor error; submit_and_wait.

## Routing tables

Completions (`complete_one`), after the PROVIDE check and slot
resolution:

| Slot kind | To |
| --- | --- |
| LISTEN, CONNECT, RECV, SEND, SHUTDOWN | `vsr_io_links_complete` (the link's FILES_UPDATE takeover arrives here too, kind CONNECT) |
| WRITE, FLUSH, SUPER, LOAD, FILE | `vsr_io_store_complete` of the owner replica; consumed and ignored when the replica is gone |
| CLIENTS | `vsr_io_snapshots_complete` of the owner; same |
| STREAM | `vsr_io_streams_complete` |
| FILES | `vsr_io_engine_files_complete` (slot back on the free list) |

Core ops (`vsr_io_engine_route`: every LOAD, then every STORE, then SYNC
and RECLAIM, then the rest in emission order):

| Op | Route |
| --- | --- |
| SEND | `vsr_io_links_send`; RETRY: deferred RETRY |
| LOAD RECOVERY | `vsr_io_store_open`; the store already open: deferred FAILED |
| LOAD, STORE, SYNC, RECLAIM | store; EINVAL (malformed): deferred FAILED |
| SNAPSHOT_CAPTURE, FETCH, SYNC, DROP | snapshots; a status returned: deferred with it |
| RELEASE of an engine lease | LEASED: store unpin, region and slab freed; else `releases_rejected` |
| RELEASE of a caller lease, APPLY, READ_READY, SNAPSHOT_INSTALL | forwarded |
| REPLY | forwarded; status not OK: `vsr_io_store_replied` |

Submit (CORE events; rails go to stream.c and link.c by op id):
EINVAL for a replica not attached or STOPPED, an engine lease, data and
lease not both set or both clear, TIME, MESSAGE, and after STOP any
REQUEST/CLIENT_QUERY/READ/CHECKPOINT; COMPLETE of a snapshot-forwarded op
goes to `vsr_io_snapshots_forwarded_done` and its lease comes back as a
RELEASE op at once (AGAIN without ring room); other COMPLETE and STOP to
`priority`; REQUEST: AGAIN if full, then ELIMIT if not admitted, then
queued.

## Invariants

- Purity (133): complete, poll, submit, prepare, and everything they
  reach, call no executor op and no system call. Executor calls only in
  init (registrations, the generator's seed), deinit, attach
  (`update_buffer` of the tail), detach (its unregistration), wake and
  the loop. `tests/lib/pure_executor` enforces it; every engine test runs
  with it armed around the primitives, the routing seam, the module polls,
  close and the node calls, abort on violation.
- An update never emits more ops than the forwarded ring has room for,
  so routing never drops a forwarded op (`LOOP_ASSERT`s on the forward).
- `completions` never fills (one per outstanding op); `priority` holds
  every COMPLETE a caller within the core's contract can send, plus STOP.
- A lease is released once: QUEUED leases go at detach or on a drop,
  LEASED ones on the core's RELEASE; the rest is counted and ignored.
- `io->now` never decreases; deadlines are armed from it.
- `closed` requires every slot free, every link FREE, no stream active
  and no clear pending (the closing engine waits for the FILES_UPDATEs).
- Detach requires STOPPED, no forwarded op naming the replica, no packed
  bytes unwritten, no deferred completion, and both modules closed.
- Pool (127): provision leaves FREE the reserve's unheld part plus the
  caller's untaken share; a caller acquire fails at its share or when
  FREE is down to the reserve's unheld part; internal slabs handed to a
  lease stop counting.

## Tests

tests/unit/engine.c: 23 tests over one simulated world (3 nodes, up to 2
replicas per engine, real cores and stores), listed in
docs/io-implementation.md section 9. Each test's header comment says what
it pins. Run one with `VSR_ENGINE_TEST=name`; `VSR_ENGINE_TRACE=1` prints
the simulation's trace and every iteration. The harness pieces a phase-2
test can copy: the executor wrapper (fail `update_buffer`, fail a submit,
count provisions, log CQEs for replays), `struct app` (a caller that
answers every forwarded op and keeps its leases until RELEASE), `iterate`,
`run_until`, `run_for`, `group_open`.

tests/unit/engine_tables.c: layout and reserve arithmetic, share tests,
minimum-slab boundaries, batch 4 ELIMIT and 5 OK, the generator seeded
once.

Module tests touched: link (adopt establishes after the takeover's
completion; a failed install closes twice), stream and snapshot (their
fake executors run FILES_UPDATE; `engine_step` dispatches the FILES kind
and calls `vsr_io_engine_prepare_files`; snapshot runs `caller_slabs =
0`), store (SLABS 32), pool (the model tracks internal holds).

Conformance: `scenario_files_update` and `scenario_provide` pass on every
column (sim, uring, uring-sqpoll, uring-napi, faulty-sim, faulty-uring).

Mutation (34 mutants of loop.c, engine.c, replica.c, link.c; runner kept
out of tree): 31 killed. Survivors: M19 (the MESSAGE skip after an
INPUT_BLOCKED MESSAGE: unit budgets never block a MESSAGE with
completions pending), M21 (MORE after the post-step polls queue
completions: latency only, the next poll takes them), M24 (close without
the stream shutdown: equivalent under decision 101, the link shutdown
ends the streams CANCELLED). All four purity mutants (the generator, a
provide call, an `update_file` clear, a clock read in the takeover) are
killed by the guard.

Not tested in phase 1: anything over a real ring or under the fault
wrapper with the engine on top; crash and recovery (only a clean detach
and RECOVER); JOIN beyond its refusal; more than one replica per node in
a group; the LIMIT answer path through `vsr_io_run`; `wait_min_*`.

## What phase 2 must test

1. Integration `engine` over the sim (section 9's row): NEW, RECOVER and
   JOIN; three replicas committing, checkpointing, fetching and
   restarting; max_clients admission at the primary; close and detach
   ordering with work in flight; `caller_slabs = 0` and the minimum
   slabs.
2. `streams` and `snapshots` integration: fetch across nodes with
   engines on both sides, decision 122's wake from the module's prepare
   end to end (the unit check arms the deadline by hand), 123's FAILED.
3. `uring_faults` over the fault wrapper: FILES_UPDATE may fail (`-EIO`
   is eligible; a failed takeover closes the link and clears its slot, a
   failed clear still frees the slot at its completion, the stale file
   staying registered until the next direct open into the slot replaces
   it, as the kernel and the sim both do), PROVIDE passes through. The plan's
   "crash simulated by `vsr_io_deinit` without flush" cannot be written as
   is: deinit is EBUSY until closed and every replica detached, and
   detach waits for the store. Crash by abandoning the engine's memory
   after `vsr_io_uring_deinit` (or `vsr_sim_crash`), never calling the
   engine again; `vsr_deinit` the cores, which is pure.
4. The iocluster fuzzer needs: deterministic replay by seed (the engine
   draws entropy once, at init, from the executor, so a sim seed fixes
   it); engines abandoned at a crash as above, with `vsr_sim_crash` first
   (it verifies buffer ownership and needs the memory valid); the
   INPUT_BLOCKED paths (flag 2's minimum budgets should reach M19's);
   STATUS copies per op, so a harness may hold several; `vsr_io_run` or
   its own loop (its own if it answers LIMIT).
5. Purity under phase 2: wrap each engine's executor with
   `tests/lib/pure_executor` (arm around the four primitives) in the new
   tests too; it can wrap a ring.

## The store review's items for the engine

- `vsr_io_store_close` drops queued STOREs, SYNCs and LOADs without a
  completion. Detach runs only at STOPPED, which core.c's `maybe_stopped`
  grants only with every operation slot free and no lease: every store op
  was completed and fed before the STATUS that reported STOPPED. Detach
  asserts the store's queues are empty (replica.c). No cancel path is
  needed for exactly-once; what phase 2 should test is that a STOP never
  waits for ever on a held STORE (the holds vsr-io.h lists all end
  without the core: writes complete, leases are released by the stopping
  core, a clients file is read), e.g. STOP with write-behind full and a
  LOAD lease pinning the ring.
- Replicated mode: the core issues no SYNC; `vsr_io_store_poll` arms the
  replica's FLUSH deadline at `flush_interval_ns`, prepare's deadline
  covers it (`vsr_io_deadlines_earliest`), and the next poll's store poll
  issues the flush. `test_replicated_flush` pins it (the store's
  `flushed` reaches what it packed with no further event). The status's
  `durable` stays 0 there (it counts SYNCs; section 11).
- Decision 125 (on main with the store review): a creation write error
  fences under CONTINUE, a SYNC in memory-only mode completes FAILED. The
  engine routes both like any completion; detach does not wait for
  unwritten bytes of a fenced store (`store_unwritten`). Untested at the
  engine level: phase 2's fault-wrapper tests should fail a creation
  write and check STOP and detach still finish.
- The core serializes CAPTURE and PUBLISH and the replicated-mode DROP of
  the previous anchor; the engine keeps emission order across updates and
  within an update routes LOADs, then STOREs, then SYNC and RECLAIM, then
  the rest (decision 51), so a CAPTURE is routed after its update's
  STOREs and never ahead of an earlier update's op.

## Open items

In docs/io-implementation.md section 11: EXTERNAL inbound links cannot
carry streams (the HANDSHAKE completion has no purpose field); a REPLY
not OK ends admission also for BUSY; REQUESTs queued before a STOP get no
REPLY; a refused SEND costs `retry_ns`; `vsr_io_run` drops ELIMIT; a
failed executor submission leaves slots taken and the engine unable to
close; M19's path. Also: a keyed handshake needs secret nonces from a
loop-refilled entropy pool, not the generator (134); the snapshot module
still calls `vsr_io_engine_slot_free` on slots that never held a file,
which stays correct under 135; a clear that fails leaves its file
registered (not open to the engine, but not closed) until the slot is
reused.

Module edits outside the engine: link.c/link.h (takeover by FILES_UPDATE,
`VSR_IO_STAGE_INSTALL`, the HANDSHAKE stage cleared before the install),
snapshot.c (`slot_drop` only: `vsr_io_engine_slot_clear`), pool.c/pool.h
(127), slots.h (two kinds), uring.c, src/sim (the two records), the fault
wrapper, include/vsr-io.h (two opcodes, the sizing rule, comments).
