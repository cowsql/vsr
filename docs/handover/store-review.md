# Store module review (src/io/store.c, store.h, tests/unit/store.c)

Worktree build/wt/store-review, branch `wt/store-review`, commits on top
of main fb747c3 (not merged, not pushed). Every fix has its breaking test
first (section "Review: ..." at the end of tests/unit/store.c), each
observed failing before its fix; the other review tests pin down corners
found sound. snapshot.c, engine.c and link.c untouched; store.h changed
only in one comment (`freeing_flush`). Placeholder decisions V1, V2, V3
in docs/io-design.md section 10 (the coordinator renumbers).

Checks on the final tree: clang 21 (ASan+UBSan, `--enable-werror`)
`make check` 68/68; gcc 16 (`--enable-werror --disable-sanitize`) 68/68;
`make format-check` clean; `make compile-commands && make lint`
(clang-tidy 21, cppcheck) clean. The unit suite runs in 0.2 s, a walk
seed in about 15 ms, which is what made the mutation and walk campaigns
below cheap.

## CONFIRMED (3)

1. **A write, flush or superblock error during the log's creation under
   CONTINUE left the store CREATING forever** (area 3, exactly-once).
   `write_error` took the CONTINUE branch (stop writing, serve from
   memory) whatever the state; in CREATING nothing else ever completes
   the header write or the flush after it, so the RECOVERY load of NEW
   or JOIN never completed, and under RECOVER (NOT_FOUND already
   reported) the STOREs held for the header hung. Fix 30b2e26: a
   creation error fences under every `on_write_error` (there is no log
   to serve from memory); the load or the held STOREs complete FAILED.
   Decision V2. Test `test_review_create_continue` (a failed header
   write under NEW, a failed creation flush under RECOVER with a held
   STORE, a failed superblock write in DSYNC mode).

2. **A SYNC queued before, or submitted after, a write error under
   CONTINUE never completed** (area 3, exactly-once). In memory-only
   mode no flush is ever issued again and `written` stops, so
   `syncs_settle` never satisfied it; vsr.h requires exactly one
   completion per op. Fix 30b2e26: `syncs_fail` completes the queued
   SYNCs FAILED at the error and `vsr_io_store_sync` fails a later one a
   completed flush does not already satisfy (a satisfied one still
   completes OK). Decision V2. Test `test_review_sync_continue` (both
   sync modes). Replicated replicas never SYNC, so this reaches only a
   misconfigured durable one, which then fences instead of hanging.

3. **The idle superblock write of decision 50 was never flushed in
   FDATASYNC mode** (area 1, recovery's floor). The write that exists to
   put the acknowledged durable sequence on media sat in the page cache
   until the next SYNC's flush, which comes only with the next record:
   the window decision 50 closes stayed open exactly while the log was
   idle, and a crash losing unflushed blocks lost the floor. Fix
   30b2e26: `superblock_done` asks for the store's own flush (the
   `freeing_flush` request of decision 111) whenever a completed
   superblock write raised `superblock_floor` in FDATASYNC mode; an
   O_DSYNC write needs none. Decision V1. Test `test_review_idle_flush`
   (the flush follows, a crash losing unflushed blocks keeps the floor,
   the recovery reads it; no flush in DSYNC mode); `test_idle_superblock`
   now completes that flush.

## The bit-rot open item (phase 4): settled, not a contract violation

The question: media corruption of records on media that no floor covers
(decision 50's residual) brings back an older row than the one freeing
relied on (112); missing ops are CORRUPT (114), but a CLIENTS record lost
that way leaves the client's older record in the table, so could a
completed request run again?

No, given the core's recovery (src/protocol.c, src/checkpoint.c,
src/validate.c):

- `vsr_validate_recovered` runs `log_bounds` on the row: a log that does
  not reach the anchor's op (`anchor->op + 1 < log_begin`) is
  inconsistent and fences (VSR_FAILURE_STORAGE). With 114, an older row
  that is served holds every op from the anchor's op on.
- The anchor's clients file holds every record of a request executed at
  or before the anchor's op (the capture is fenced with APPLY). A CLIENTS
  record the older row lacks therefore completed a request whose entry
  lies above the anchor, in the log the row holds.
- The core installs the anchor (`clients_stored = task->op`, so it trusts
  the store's table only up to the anchor's op) and replays the suffix
  through APPLY, whose every completion sets `results_pending` and stores
  the results again as a CLIENTS record, before the replica serves
  clients. The lost record is regenerated; the table is consistent with
  the row by the time it matters.

What the older row lost is acknowledged transactions above the floor on
media: exactly decision 50's residual, which freeing does not widen (the
freed slots held nothing a consistent older row needs). Bounding freeing
by the durable floor on media would free nothing in replicated mode, and
the header's client base predates its segment's records, so a check of
the client base against the start segment cannot see records freed under
a later base. Recorded as decision V3; the idle floor flush of V1 narrows
50's window in FDATASYNC mode. The section-11 item is rewritten.

## The independent checker (tests/unit/store.c, `checked_*`)

Audited against docs sections 5 and 6.4 before reading the store's
scanner. Verdict: a genuine reading of the format, byte offsets verified
against wire.h (superblock crc at 96, header last_sequence at 40 and
floor at 48, log bounds at 64 and 72, record flushed at 24, count 32, run
36, payload_crc 40, header_crc 44), and its rules match the docs: the
newer valid copy, geometry, the file's slots against the superblock's,
every valid header's floor, the start slot holding the start segment,
the chain (next sequence, run at or above, decision 116's flushed rule
with `record_run`, a PAD only to its block's end), the dead tail inside a
block swept and the chain resumed at the boundary (110), a verdict at a
boundary ending the chain, the rest of the slot and every unvisited slot
swept at every aligned position (88), the successor rule (greatest number
among headers naming the sequence with a run at or above), stale and
abandoned slots freed, F against the sequence, identity and hard state
for a log with records, 114's op check, NOT_FOUND for no record, and the
resume offset. Findings:

- Docs 5.1 did not say that the bytes after the superblock's crc (its
  reserved field and the rest of the block) are zero, which both the
  checker and the codec (`vsr_io_codec_get_superblock`) require; 5.2 said
  it for headers. Docs fixed (30b2e26). Nothing the checker accepts is
  forbidden by the docs, and nothing it rejects is allowed.
- Not modelled, so a mismatch there would not be caught: RESTORE (its
  begin is in the payload; the walk never RESTOREs), change descriptors
  and payloads (a valid record whose content contradicts the log is
  CORRUPT in the store; the checker applies APPEND/TRUNCATE/TRIM by
  their descriptors alone), and the superblock identity against the
  replayed one (decision 90; the walk never varies it). Each is covered
  by unit tests instead (recover_modes, bad_descriptor, recover_floor).
- The PAD and record rules agree with the codec's: a record needs count
  in 1..8, length a multiple of 8 within 48 + 24 * count .. the record
  limit, both CRCs and the generation; a PAD is skipped only when its
  uncovered length runs exactly to the block's end.
- The walk's crash model: tears keep a prefix of a write's blocks (never
  a lost middle block in DSYNC mode; FDATASYNC loses any dirty block), a
  write issued after a torn one persists whole (reordered persistence),
  a crash disarms the tear so a recovery's own writes are never torn
  (now `test_review_recovery_crash`), and a walk ends after a flip
  crash, so the model is not checked below the media sequence. Freed
  slots forget their never-rewrite bits at FREEING, so a premature
  reuse would be caught by a recovery verdict, not by the model.

## NOT A BUG, examined and rejected

- Recovery of a log whose last record ends at its segment's last byte:
  the log resumes at the segment's end with an empty extent there, the
  next record seals and opens the next segment; both recoveries agree
  with the checker (`test_review_full_segment`).
- A segment header whose read fails twice: the start slot's is CORRUPT;
  another's makes the slot free, the chain ends before it, and the floor
  its records carry (the slot is swept as a free one) makes it CORRUPT
  too; without an acknowledged floor the older row is served, which is
  decision 50's residual again (`test_review_header_unreadable`).
- Crashes during a recovery: its superblock write torn or lost before
  FLUSH_AGAIN leaves the older superblock and the run is counted from it
  again, the records read being on media by the flush before the write;
  a machine crash before that first flush loses the unacknowledged
  records only (`test_review_recovery_crash`).
- The dead-tail resume of 110 with a stale record straddling the resume
  boundary: both the store and the checker count its floor and resume at
  the boundary inside it, where no header can be.
- Reordered persistence between two writes in flight (W1 torn at a
  block, W2 whole): the chain ends at W1's last valid record; W2's records
  are swept for floors and, being above the in-order `written`, were
  never acknowledged.
- Growth crashing between FALLOCATE and its superblock (a spare slot, its
  header invalid); the FALLOCATE's persistence relies on fdatasync and
  O_DSYNC committing the allocation, which ext4 and XFS do.
- Superblock alternation and revisions: the recovery's write goes to the
  copy not chosen; a torn write leaves the older valid copy, which names
  a start segment whose slot is still FREEING or FLUSHING (111).
- Freed slots' numbers are never duplicated on media: a header that
  never reached media may reuse its number, one that did is seen and
  raises `next_segment`.
- `recovery_judge`'s `retried`/`retry_offset` pair re-reads each position
  once; a re-read that passes leaves the pair stale but a later position
  differs; the chain resume resets it.
- `ring_wrap`, `pack_end`, `pack_needs`, `ring_reserve`'s floor and the
  extent FIFO: consistent with decision 69; the newest extent is never
  dropped, a write's range is never trimmed (written_floor bounds the
  floor), an extent of a freed slot is dead for `hot` and skipped by
  `issue_extent`.
- `write_plan`: a header is its own write, records wait for its
  completion (a sealed HEADER segment stays SEALED at completion), the
  newest extent is padded when planned, `written` follows the in-order
  prefix and freezes after an error.
- The flush pipeline: WANTED (sync delay) -> DUE at `flush_target`, the
  own flush without a target, one flush serves both, the cookie is
  `written` at issue; a SYNC arriving while a flush is out waits for the
  next (`syncs_settle` re-arms DUE); `freeing_flush` INFLIGHT overwritten
  by a later completion's WANTED is conservative.
- Freeing: the floor terms (first live op from `retained_begin`, kept
  versions, `reclaimed`, `client_base_floor + 1`, `capture_floor`),
  `segment_quiescent` and `segment_loading`, FREEING -> FLUSHING -> FREE
  with a dirty superblock deferring the conversion, `reclaim_apply` and
  `base_mark` (a lowering base lowers the term at once, a raising one
  waits; two changes in flight stay conservative), memory-only applying
  every term.
- The client table: open addressing with backward-shift deletion (the
  probe-path rule), sweeps re-examining a bucket after a move, prune
  rules, `base_record`'s merge and `base_apply`'s lowering, 113's
  `record_live` and the base read only up to the base, a queued base
  read resolved again at issue; a record freed under the base always has
  a `base_offset` (the base term frees only what the base covers).
- Loads: resolved at acceptance, RETRY rules (51, 79), hot batches
  re-checked for hotness at each drain (a ring reuse makes them cold),
  the single cold read, slab accounting under a fence (`cold_slab`), a
  missing `base_slot` is FAILED, `build_client` checks the record's
  client id, the byte bound never cuts the first entry.
- Exactly-once: STORE (packed, aborted with its status, failed at a
  fence, or failed by `base_resume`), SYNC (settled, failed at a fence,
  V2), LOAD (a resolve failure, `load_finish`, a read error, a fence, no
  base slot), RECLAIM (at once), the RECOVERY load (NOT_FOUND at the
  probe under RECOVER or at creation's end, OK, CORRUPT/FAILED through
  `recovery_fail`/`store_fail`, once because `load_op` is zeroed).
- The superblock's `run`: records of run N exist only once the superblock
  naming N is on media (recovery's FLUSH_AGAIN, O_DSYNC at creation and
  in DSYNC mode).

## OPEN, not fixed

- `vsr_io_store_close` drops queued STOREs, SYNCs and LOADs without a
  completion; the engine's detach (part 2) must complete them CANCELLED
  before closing, as vsr.h requires one completion per op.
- A PUBLISH of the latest capture applies the offsets the capture
  recorded; a capture started meanwhile resets and refills them with the
  new file's. The core captures one checkpoint at a time and publishes
  it before starting the next, so the overlap does not arise; a wrong
  offset would be CORRUPT at the CLIENT load, not another client's
  record. Noted in docs section 11; a staged offset set would remove the
  assumption (a store.h change).
- In REPLICATED mode the core lets the previous anchor's clients file be
  dropped once the PUBLISH is stored, not on media; a machine crash
  within the flush interval then recovers the previous anchor whose file
  is gone: CORRUPT rather than an older row (core-side; docs section 11).
- A hot LOG batch pins the ring from its lowest record to its highest;
  after a TRUNCATE and re-append the records of one batch can lie far
  apart, so the pin can exceed decision 69's two-record slack and a
  STORE is held until the lease is released. The core's lease releases
  never depend on a STORE, so the hold ends; a tighter rule would read
  such batches cold.
- A superblock read that fails twice is CORRUPT (a read error, not
  damage); FAILED would say more, both fence.
- The recovery's re-read of a range whose first read was short judges a
  persistent short read as a bad range at that position (the docs say
  so); only a file shorter than STATX reported can cause it.
- From the phases: `flush_target` waits for held STOREs' SYNCs, the
  creation of a log leaves no directory fsync, decision 57's listing,
  the harness's 16-entry captures.

## Mutation testing (60 mutants, store.c restored after each)

Driver: the review's scratch script applied one textual mutant, rebuilt
tests/unit/store (ASan), ran the unit suite (0.2 s), and for a mutant
the suite passed ran seeds 1..300 of the walk (15 ms each). The first
pass (before the review tests of fd81dea and its predecessors) left 13
survivors and 14 mutants -Werror refused (`if (false)` is unreachable
code; an unused function or variable), which were rewritten as semantic
changes; every survivor got a test, except four argued equivalent below.
Final tally: 56 killed by the unit suite, 4 equivalent, none left to the
walk alone (the walk killed several in the first pass before the tests
existed, e.g. the lost `chain_resume`).

| Mutant | Killed by |
| --- | --- |
| M01 judge: no chain resume past a dead tail (110) | test_recover_torn |
| M02 chunk: 116 clause dropped | test_recover_splice |
| M03 chunk: record_run never follows the record | test_recover_prefixes |
| M04 scan_end: 114 loop empty | test_review_unreplayed_op |
| M05 superblock_done: FDATASYNC frees at the write completion (111) | test_freeing_flush |
| M06 flush_done: a flush wanted (not issued since) frees | test_freeing_flush |
| M07 prepare_flush: WANTED never becomes INFLIGHT | test_idle_superblock |
| M08 prepare_flush: an own flush clears a due request | survived: equivalent (below) |
| M09 prepare_flush: due ignores flush_target | test_sync_fdatasync |
| M10 reclaim_apply: RECLAIM applies past the media sequence (112) | test_empty_index |
| M11 reclaim_apply: a pending base applies at once (112) | test_reclaim_media |
| M12 base_mark: a lowering base does not lower the floor at once | test_review_base_lowering |
| M13 media_sequence: memory-only mode waits for the media | test_review_memory_only_free |
| M14 store_pack: a waiting floor term fails the STORE instead of holding | test_reclaim_media |
| M15 pack_needs: the wrap pad not counted in the seal decision | test_wrap_seal |
| M16 ring_wrap: a head exactly at the ring end continues the extent | test_wrap_exact |
| M17 record_live: off by one at the reused slot's first sequence (113) | survived: equivalent (below) |
| M18 resolve_client: the base file serves records above the base (113) | test_client_newer |
| M19 snapshot_clients: a live record captured as the base file's | test_client_freed |
| M20 prepare_load: a queued base read not resolved again | test_client_base_moved |
| M21 write_done: creation READY before its flush (115) | test_create |
| M22 write_error: CONTINUE keeps FREEING/FLUSHING slots | test_review_memory_only_free |
| M23 chunk: the run rule dropped (34) | test_recover_torn |
| M24 chunk: any higher sequence continues the chain | test_review_scan_gap |
| M25 successor: a header of a lower run qualifies (48) | test_review_successor_rules |
| M26 successor: the smallest number wins | test_review_successor_rules |
| M27 scan_end: a scan one below the floor is not CORRUPT (50) | test_recover_floor |
| M28 scan_end: an abandoned successor is kept | test_recover_stale |
| M29 scan_end: durable not the recovered sequence (89) | test_recover_prefixes |
| M30 header: a header floor does not raise F | test_review_header_floor |
| M31 chunk: swept records do not raise F (88) | test_recover_torn |
| M32 superblocks: the older revision wins | test_recover_prefixes |
| M33 superblocks: fewer slots than named is not CORRUPT | test_review_short_file |
| M34 free_floor: the base term is client_base + 2 | test_floor |
| M35 free_floor: kept versions do not hold their records (78) | test_floor |
| M36 segment_loading: only base loads keep a slot (78) | test_free_segments |
| M37 segment_quiescent: only an extent starting above the floor blocks | survived: equivalent (below) |
| M38 write_plan: records written before their header completed | test_segments |
| M39 syncs_settle: a SYNC completes one sequence early | test_sync_fdatasync |
| M40 apply_clients: an equal number ignored without the op check | test_clients |
| M41 apply_truncate: the retained entry not restored to previous | test_index_ops |
| M42 trims_reclaim: the ring below retained_begin not cleared | test_floor |
| M43 pack_end: the seal header ignores the wrap (69) | test_review_seal_wrap_pin |
| M44 pin_floor: pins ignored | test_pins |
| M45 superblock_done: no flush after a raised floor (V1) | test_idle_superblock, test_review_idle_flush |
| M46 write_error: creation errors follow CONTINUE (V2) | test_review_create_continue |
| M47 sync: SYNCs in memory-only mode wait forever (V2) | test_review_sync_continue |
| M48 chunk: the chain resumes one position past the boundary | survived: equivalent (below) |
| M49 judge: a bad range is never judged (re-read forever) | test_recover_torn |
| M50 record_changes: a descriptor may overlap the last one (77) | test_review_descriptor_overlap |
| M51 scan_end: stale headers keep their slots | test_recover_stale |
| M52 write_done: written advances after an error | test_review_sync_continue |
| M53 segment_open: headers carry no floor | test_review_header_floor |
| M54 store_pack: records carry no floor (50) | test_sync_fdatasync |
| M55 superblock_done: FREEING slots free under a dirty superblock | test_review_freeing_dirty |
| M56 resolve_request: no RETRY behind a reindex (79) | test_index_ops |
| M57 resolve_client: no RETRY for a newer record (51) | test_clients |
| M58 base_apply: only a covered record lowers the base (80) | test_capture_base |
| M59 recovery_install: the resume offset is the slot start | test_recover_modes |
| M60 chunk: PAD length unchecked | test_recover_floor |

Equivalent survivors:

- M08: an own flush that is not due clears the SYNC's DUE request; at
  that flush's completion `syncs_settle` re-arms DUE for the SYNCs left
  (the same flush_target), so the SYNC completes at the next flush as
  before. The only difference is a sync delay skipped once.
- M17: `record_live` also counts a record at the reused slot's
  `first_sequence - 1`. Such a record was freed under the base (the base
  term is the only one that frees a completed record), so `resolve_client`
  takes the base-file branch before the cold log branch, and
  `snapshot_clients` only lowers the capture floor by one record
  (conservative).
- M37: `segment_quiescent` counting only extents that start above the
  written floor. Decision 112 bounds the freeing floor by the sequence on
  media plus one, `written` follows the writes' issue order, and a
  slot's writes (its seal's pad included) are issued before the next
  slot's, so a slot whose last record is below the floor has every write
  complete; in memory-only mode nothing is ever unwritten. The check
  stays as a belt.
- M48: resuming the chain only past the boundary. The resume resets
  `position` to the boundary, so the record there is swept (its floor
  counted) and then chained; the sweep of it is idempotent.

## Campaigns (final tree)

- Seeded walks (`./tests/unit/store SEED`, ASan build): seeds 1..7000,
  3525 DSYNC and 3475 FDATASYNC, 0 failures. In all 1,658,889 steps,
  1,309,195 STOREs (70,500 held), 154,848 SYNCs, 425,923 RECLAIMs,
  1,400,190 LOADs (960,015 cold), 176,227 captures, 103,917 crashes
  (51,903 torn writes, 58,736 blocks lost, 9,885 flips): 101,795
  recoveries agreeing with the checker and the model, 1,503 CORRUPT
  verdicts (all under flips, all the checker's too), 0 NOT_FOUND. Every
  crash's recovery reached the media sequence when nothing was flipped.
- Recovery fuzzer (build/fuzz, `--enable-fuzzing`, clang 21, one worker,
  `-max_len=4096`, corpus seeded with the 12 default walks): 21,771 runs
  in 1,501 s (14/s), 2,010 edges (13,946 features), corpus 790 units,
  no crash, no artifact.
