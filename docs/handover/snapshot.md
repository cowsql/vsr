# Snapshot module handoff (for the engine agent and the reviewer)

Branch `wt/snapshot`: 2c0c11d implemented `src/io/snapshot.c` (the first
author); this session reviewed it against the engine's contracts, fixed
nine gaps (below), wrote `tests/unit/snapshot.c` (eleven tests over two
engines with real stores, links and streams) and ran two mutation
campaigns. Decisions 105..109 in docs/io-design.md section 10 are
placeholders (the coordinator renumbers); docs/io-implementation.md
"Snapshots", 5.4, 7.1, 7.4, 7.5, section 10 and section 11 are updated.
`store.c`/`store.h` are untouched. `main` (e3385c0, the stream review,
decisions 100-103) is merged in; the module's library-stream paths agree
with them (it completes DATA ops by the id given, closes each accepted
served stream once with a valid status, and its served FILE write names
an engine slot, which 103 allows for library streams).

Verification (Debian, kernel 7.2, clang 21, gcc 16): `make check` 67/67
under clang ASan+UBSan (`--enable-werror`) and gcc (`--enable-werror
--disable-sanitize`); `make format-check` clean; clang-tidy 21 and cppcheck
clean on src/io/snapshot.c and tests/unit/snapshot.c. `make lint` still
fails on files outside this work: clang-tidy 21's new
`clang-analyzer-security.ArrayBound` on store.c's container_of, cppcheck
on uring.c (style), tests/unit/stream.c and executor_conformance.c.

## Data structures (src/io/snapshot.h)

- `struct vsr_io_snapshots` (in `vsr_io_replica.snapshots`): `entries`
  (the registry, `transfers + 4`), `capture` (the entry whose CAPTURE is
  outstanding), `writer`, `reader`, `fileops[VSR_IO_SNAPSHOT_FILEOPS]`
  (the records in flight, with the path bytes they borrow), `chunks` (the
  fetch's chunk ring, `stream_window`), `serves[streams]` (by stream
  index), the directory (`dir_state`, `dir_slot`, `dir_fileop`),
  `pending_base`, `retry`, `closed`, `error` (last errno), the CAPTURE
  template (`task`, `template`, `template_region`) and the fetch's 56-byte
  wire `request` (pinned until its stream ends).
- `struct vsr_io_snapshot` (an entry): `id`, `state` (FREE, WRITING,
  WRITTEN, SYNCING, DURABLE, FETCHING, DROPPING), `readers` (served
  streams), `sequence` (the capture's, or the RESTORE's that loaded a
  fetched file; 0 until known), `bytes`, the op in progress (`op`,
  `op_type`, `task`, `forwarded`, `forward_due`, `caller_status`,
  `library_status`), `lease` + `result` (105), `file_slot` (kept) and
  `tmp_slot` (transient), `job` (NONE, WRITE, DISCARD, LOAD, FETCH, SYNC,
  DROP, RELEASE) with its `step` (the record to issue next: OPEN, STAT,
  READ, WRITE, FSYNC, CLOSE, FSYNC_DIR, RENAME, UNLINK) and `fileop`,
  `job_next` (a SYNC queued behind a RELEASE's CLOSE), `unlink_due`,
  `on_disk`, `sync_failed`.
- Writer (one per replica): staging `slab`, `staged`, `next`/`count` over
  the table snapshot (`table[max_clients]`), `cold_slab` + `cold_valid`,
  `cold_bytes`, `cold_offset`, `file_offset`, `trailer_staged`, `aborted`.
- Reader (one per replica, base loads and fetch verification): `slab`
  with `filled`/`consumed`, `file_offset`, `file_size` (UINT64_MAX for a
  fetch), `stage` (HEADER, RECORDS, TRAILER, DONE, FAILED), `purpose`,
  `expected`/`seen`, `sequence` (the base's), `status`, `stream`, `reads`
  (a load's read, or a fetch's .tmp open, in flight), `stat` (statx).
- Serve (by stream index): `state` (FREE, OPENING, OPEN, CLOSING),
  `snapshot`, `slot`, `fileop`, `ended`.
- Slot `sub` of a record: `SUB_ENTRY | index`, `SUB_SERVE | stream` or
  `SUB_DIR`; the cookie is the step.

## The op flows

- CAPTURE: take lease, slab, slot, entry (RETRY otherwise), id from
  executor entropy, template copy, `vsr_io_store_snapshot_clients`
  (capture floor). Poll: forward; `writer_stage` stages header, records
  (hot from the ring, cold from the log or the base file) and trailer,
  setting the entry's step (READ, WRITE); prepare issues it; complete
  advances. `writer_finish`: OK keeps the slot (latest capture) and calls
  `capture_end(id)`; a failure calls `capture_end(zero)`. `capture_settle`
  completes when both halves are over (a failed half first discards the
  file: CLOSE, UNLINK).
- SYNC: forwarded at once; FSYNC (kept slot) or OPEN, FSYNC, CLOSE; then
  FSYNC of the directory; `sync_settle`.
- FETCH: held id: forward at once. Else open a library stream, OPEN the
  .tmp; chunks queue (at most the window) and are fed to the reader;
  prepare writes the head chunk; its completion completes that DATA op;
  `stream_end` records the status; with no record out and no chunk left,
  `fetch_end_decide`: CLOSE then RENAME (OK END, reader DONE) or UNLINK;
  `fetch_finish` forwards (OK) or completes the core op.
- DROP: forward; on the caller's OK the poll starts the DROP job (CLOSE,
  UNLINK) once no reader and no job remain; with readers the core hears OK
  at once. An unknown id gets a transient entry (107).
- Base loads: the poll calls `vsr_io_snapshots_load_base` whenever
  `vsr_io_store_base_wanted`; OPEN, STAT, READs through the reader
  (`base_record` per record); `load_finish` keeps the slot, sets
  `pending_base`, calls `base_end` (OK) and `base_resume`, then
  `base_track`.
- `base_track` (every poll and after a load): `store.base_slot` = the
  kept slot of the entry named by `client_base_id` (-1 when it has none
  and no cold read is in flight); marks RELEASE on every other kept slot
  but the latest capture's and the pending base's.

## The hooks (decision 98), as implemented

- `vsr_io_snapshots_serve(io, stream, request)`: refusals are
  NOT_FOUND (version, kind, cluster, replica, unknown id, incomplete file,
  CAPTURE outstanding, being discarded or dropped) or FAILED (a stream
  index beyond the serves, a serve still busy on that index); OK sets
  `streams[stream].replica` and `readers++`. The open is issued from the
  module's prepare; its completion feeds `vsr_io_streams_write(FILE, the
  engine slot, 0, bytes)` and `vsr_io_streams_close(OK)` (FAILED when the
  write is refused, NOT_FOUND/FAILED for a failed open). One close per
  accepted stream, statuses within enum vsr_io_status (decision 102); the
  served FILE write names an engine slot, which decision 103 allows for
  library streams only.
- `vsr_io_snapshots_stream_data(io, replica, stream, op, offset, bytes,
  slab)`: the module keeps the op id and completes it with
  `vsr_io_streams_data_done` when that chunk's write completed (decision
  100's sequence ids are used as given); a chunk it cannot take (no fetch,
  wrong offset, full ring, failed reader) fails the reader and is
  completed at once. It never retains the slab itself.
- `vsr_io_snapshots_stream_end(io, replica, stream, status)`: the
  requester's (the reader's stream) records the status; a source's marks
  the serve ended and closes its slot (at once, after the open in flight,
  or not at all when the open was never issued).
- `tests/unit/stream.c` defines strong doubles of the three hooks; that
  binary must keep not pulling `snapshot.o` in.

## Engine part 2: what to call, in which order

1. Attach: `vsr_io_snapshots_size(core limits, io limits, max_clients)`
   and `vsr_io_snapshots_init` in the replica's metadata; bind the
   replica's CAPTURE deadline handle (`deadline_capture`, kind CAPTURE,
   index = replica); the lease regions must hold
   `vsr_io_snapshots_region_bytes` (the load region does).
2. `vsr_io_complete`: slot kind `VSR_IO_SLOT_CLIENTS` ->
   `vsr_io_snapshots_complete(io, slot.owner, slot, cqe)` (it consumes the
   slot). FILE stays the store's.
3. `vsr_io_poll`: a popped CAPTURE deadline needs no call. Per replica in
   OPENING or RUNNING (recovery needs OPENING: decision 90's load starts
   from this poll): `vsr_io_store_poll` then
   `vsr_io_snapshots_poll(io, replica, now)` (forwards, base loads, the
   writer's staging, DROP unlinks, `base_track`). Forwarded ops use
   `vsr_io_forward(io, replica, CORE)` with the core's op id; a full ring
   leaves `forward_due` for the next poll.
4. Routing (7.3): SNAPSHOT_CAPTURE, FETCH, SYNC, DROP ->
   `vsr_io_snapshots_capture/fetch/sync/drop(io, replica, op.id, task)`;
   a status returned is a COMPLETE to queue at once (no data, no lease);
   EINVAL is a malformed op. INSTALL is forwarded verbatim. CAPTURE
   snapshots the client table when called: within one update the engine
   routes STOREs before the other ops (decision 51); the core's capture
   fence keeps a later CLIENTS change out of that update, and the module
   answers FAILED if `store.clients_sequence` passed the task's sequence.
5. `vsr_io_submit`, CORE COMPLETE: if `vsr_io_snapshots_owns(&replica->
   snapshots, event.id)`, call `vsr_io_snapshots_forwarded_done(io,
   replica, event.id, event.status, event.data)` (the module copies a
   CAPTURE/FETCH checkpoint into its lease region during the call), then
   release the caller's lease; else queue the event for the core.
6. The module's COMPLETEs arrive through `vsr_io_engine_complete_core`
   in the replica's internal completion ring: an OK CAPTURE/FETCH carries
   `completion.lease` (an engine lease index; the event gets its id) and
   the checkpoint in that region; the core releases it with a RELEASE op
   of `VSR_IO_LEASE_ENGINE`, which the routing resolves and frees.
7. `vsr_io_prepare`: after `vsr_io_store_prepare`,
   `vsr_io_snapshots_prepare(io, replica, sqes, capacity, &count)`.
8. Streams: stream.c calls the three hooks itself (library streams have
   `owner == LIBRARY`); nothing to route.
9. Detach: `vsr_io_snapshots_close(io, replica)` is EBUSY while a record
   is in flight, the writer or reader is busy or a serve is open; the
   stopped core has no op outstanding, so `vsr_io_streams_shutdown` (a
   fetch or serve in progress ends CANCELLED/RETRY) and the completions of
   the records in flight are what the engine waits for. OK drops every
   kept slot (update_file -1, engine slot freed), clears
   `store.base_slot` and releases abandoned leases.

## Invariants (checked after every step by tests/unit/snapshot.c)

- `check_snapshots`: a FREE entry holds nothing; WRITING only while the
  writer holds it or its library half is over awaiting discard; FETCHING
  only as the reader's entry; `forwarded` is 0 or the op; records in the
  file-operation table equal the fileops the entries, serves and
  directory name; chunks only for a fetch; `pending_base` names a live
  entry; the writer's and reader's slabs are referenced.
- `check_file_slots`: the engine file slots the module holds (kept,
  transient, serve, directory) are distinct, allocated (not on the
  engine's free list, which has no duplicate), never the log's, and every
  handle the model has open other than the log's is one of them;
  `store.base_slot` is one of them when set.
- `check_records`: every CLIENTS record in the slot table is one of the
  module's file operations, at most `VSR_IO_SNAPSHOT_FILEOPS`.
- `check_leases`: reserved leases are distinct, taken, and belong to an
  outstanding CAPTURE or FETCH.
- `expect_idle` after each scenario: no job, op, lease, transient slot,
  reader, slab, file operation or serve left; `leases_free` full.

## Harness (tests/unit/snapshot.c)

The two-engine world of tests/unit/stream.c (copied) plus a directory
model per engine (`struct dfile`: name, durable name, refs, data and a
flushed shadow; `fslots` behind registered slots; `dir_crash` keeps what
was synced) and faults: `fail_open/write/read/fsync/fsync_dir/rename/
unlink/log_read`, `short_write`, `short_log_read`, holds (pool writes,
log writes, opens, reads, reads at EOF, renames, fsyncs) released by
`world_release_held`. Each engine has replica 0 set up by hand with a
real store (`replica_setup`), node = index + 1, replica id = index + 1;
`two_engines` authorizes each for the other. Helpers: `fresh_store`,
`fetch_setup` (a captured 8-client table at a), `capture`/`capture_begin`/
`capture_stepped`, `fetch`/`fetch_start`/`fetch_fails`, `joint_run`,
`craft_file`/`craft_seal`/`file_install` (crafted clients files),
`restore_fetched`, `expect_checkpoint` (the lease-carried result),
`expect_core`/`expect_cores`, `step_checked`. Engines get
`caller_slabs = 8` (see the open item on the pool reserve).

## Gaps found and fixed in this session

1. OK CAPTURE/FETCH completions carried a module-wide copy and no lease
   (vsr.h violation; the core's retained checkpoint was overwritten) -> 105.
2. A fetch chunk write's completion completed every queued chunk: holes in
   a file renamed into place -> 109.
3. A serve whose open found no file operation was never retried; an end
   before the open left it OPENING; a failed open after the end closed the
   index's next stream -> 108.
4. A wait found in prepare armed no deadline -> 109 (retry_later).
5. A file loaded for a held RESTORE/PUBLISH the store could not pack at
   once was closed by base_track -> 106 (pending base).
6. SYNC (and DROP) during a RELEASE of the kept slot answered RETRY,
   which fences the replica for SYNC -> 107.
7. DROP of an id the registry lacks was FAILED (retried forever) -> 107.
8. A file was servable while its CAPTURE was outstanding, and its discard
   could free the entry under a reader -> 108.
9. A FETCH of a held file whose kept slot was being released never
   completed when the caller answered before the CLOSE did.
Also: reads longer than asked are I/O errors; close releases abandoned
leases; gcc 16's -Wnull-dereference in the test.

## Mutation testing (tests/unit/snapshot, clang ASan)

| # | Mutation | Result |
| --- | --- | --- |
| M1 | record length not bounded by `result_bytes` | killed (a sealed 128-byte result at a fetch) |
| M2 | record parsed before its bytes arrived | killed (records straddling chunks) |
| M3 | record not bounded by the bytes before the trailer | equivalent: such a record fails its CRC or leaves no trailer, CORRUPT either way |
| M4 | record CRC not verified | killed |
| M5 | close and rename at the last chunk's write, before the END | killed (EOF read held) |
| M6 | a non-OK END ignored | killed |
| M7 | op fields not cleared at completion | killed |
| M8 | forwarded id not cleared at the caller's completion | killed |
| M9 | `base_slot` not set | killed |
| M10 | `base_slot` not cleared at the DROP close | equivalent: base_track clears it in the same step |
| M10b | `base_slot` never cleared (both sites) | killed (DROP of the base) |
| M11 | every queued chunk completed at one write | killed (files differ) |
| M12 | serve open retried only without a slot | killed |
| M13 | stale handle closed after the end | killed (caller stream on the index) |
| M14 | lease kept on a failed op | killed |
| M15 | retry deadline not armed | killed |
| M16 | bytes after the trailer accepted | killed |
| M17 | chunk offset not checked | killed |
| M18 | transient slot freed twice | killed (engine free list) |
| M19 | pending base not kept open | killed (write-behind hold) |
| M20 | header count not bounded by `max_clients` | killed (nine records) |
| M21 | trailer count not checked | killed |
| M22 | SYNC not waiting for the directory fsync | killed |
| M23 | DROP unlinking under a reader | killed |
| M24 | serving a file whose CAPTURE is outstanding | killed |
| M25 | FETCH settle waiting for any job | killed |

The runner applies each mutation to a copy, rebuilds (a failed build is
reported, not counted), runs the test with a timeout and restores.

## Open items

In docs/io-implementation.md section 11: the pool reserve does not count
the module's staging slabs or a new stream link's send slab (a fetch can
starve its own handshake on an idle engine); a queued base CLIENT load
reads the new base file after a base change; a failed fetch keeps
receiving until END; a failed directory open is final; a failed caller
half after a complete capture leaves `last_capture` on the unlinked id;
one cold read per record. Still open from before: decision 92 (strays
need a directory listing), `tests/integration/snapshots` over the
simulation, and an independent review of snapshot.c.
