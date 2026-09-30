# Snapshot module review (src/io/snapshot.c, snapshot.h, tests/unit/snapshot.c)

Worktree build/wt/snapshot-review, branch `wt/snapshot-review`, commits on
top of main fb747c3 (not pushed, not merged). Every fix has its breaking
test first, observed failing before the fix (section "Review" at the end
of tests/unit/snapshot.c, `test_review_*`). store.c, stream.c, link.c,
engine.c and pool.c untouched; the clients-file format change touches
wire.h, codec.c/.h, tests/unit/codec.c and tests/fuzzy/frame.c.
Placeholder decisions 117..123 in docs/io-design.md section 10 (the
coordinator renumbers); docs/io-implementation.md "Snapshots", 5.4, 7.4,
section 10 (two rows) and section 11 updated.

Commits: 1166607 (117), 9cf3e6a (118), e23f2e3 (119), 72abe58 (120),
0f14160 (121), 090da53 (122), dbc726c and ec6bd46 (tests closing mutation
survivors, slot accounting), 2d4710f and 46f77b6 (lint in the new tests),
f23abc0 (decisions and docs), cad7411 (wake_now moved away from
`slot_drop`), this file, then 766053d (123, the coordinator's decision on
the first open item) and its docs. No `vsr_io_engine_install` or other executor
call was added or moved: `slot_drop` and its surroundings are as on main
(the engine agent converts that call into a submission record); the fixes
issue only records (the directory's OPENAT for 119) and arm deadlines.

Checks (kernel 7.2, clang 21 ASan+UBSan and gcc 16 `--disable-sanitize`,
both `--enable-werror`): `make check` 68/68 on both (the first gcc run was
terminated from outside mid-compile and rerun); `make format-check` clean;
`make compile-commands && make lint` clean after the two lint commits
(clang-tidy 21, cppcheck, shellcheck); after cad7411 (a move of one
function) tests/unit/snapshot passed again under both compilers and
clang-tidy, cppcheck and clang-format are clean on snapshot.c.
tests/fuzzy/frame.c is not built outside `--enable-fuzzing`: built by hand
against the ASan library and run 30 s on the corpus tests/unit/codec seeds
(5.2M runs, no failure), clang-tidy clean. The experiment behind the
pool-reserve item below was a scratch change to the harness, not committed.
After 123: tests/unit/snapshot passes under both compilers; format-check,
clang-tidy and cppcheck are clean on snapshot.c and its test.

## CONFIRMED (7, all fixed)

1. **A CAPTURE or SYNC answered while base_track released its file's slot
   never completed** (exactly-once). `base_track` marked a RELEASE (CLOSE
   of the kept slot) on every file that is neither the base, the latest
   capture nor the pending base, also while a core op was outstanding on
   the entry. `capture_settle` and `sync_settle` wait for the entry's job
   to end and `release_done` settles nothing, so a caller that answered
   while the CLOSE was out left the op uncompleted forever. Reachable with
   no help from the core: a CAPTURE whose write fails (ENOSPC) or whose
   cold record is CORRUPT after the file's creation is never the latest
   capture, so the next poll releases it; the caller's answer in that one
   iteration left the application fence up for good and `s->capture` set
   (no later CAPTURE). The same for a SYNC whose file stopped being the
   latest capture (a capture completing during a SYNC, which today's core
   stages never produce). Fix 1166607: no RELEASE while `entry->op != 0`.
   Tests `test_review_capture_release`, `test_review_sync_release`. 117.

2. **A repeated FETCH failing at its caller unlinked the core's held file**
   (lease/hold rules). A FETCH of an id the module holds transfers nothing;
   `fetch_settle` still unlinked the file when its caller failed and no
   RESTORE had adopted it (`sequence == 0`). docs/vsr-api.md keeps
   successful CAPTURE/FETCH objects until DROP and has a repeated FETCH
   share the hold, so the core kept a hold on a file that was gone (a
   later RESTORE `CORRUPT`). Today's core fetches only ids it does not
   hold locally (checkpoint.c), so this is a contract violation it does
   not reach yet. The author's test asserted the unlink under a comment
   saying the opposite. Fix 9cf3e6a: `transferred` (the former
   `reserved`) marks a file this op's stream wrote; only that one is
   discarded. Test `test_review_refetch_kept`. 118.

3. **A failed directory open was final: every SNAPSHOT_SYNC fenced the
   replica** (transient condition). The directory is opened once, at the
   first poll; EMFILE/ENFILE/ENOMEM there failed every later SYNC, and any
   non-OK SNAPSHOT_SYNC fences (vsr.h). Was an open item. Fix e23f2e3: a
   SYNC reaching the directory fsync with the directory FAILED has the
   poll re-arm the open once for it (`dir_retried`); only that attempt's
   failure fails it, so nothing loops. Test `test_review_dir_retry`
   (harness fault `fail_dir_opens`). 119.

4. **Records were not bound to their file or place** (format). Each record
   CRC covers its own bytes; a record written over another of the same
   length (a neighbour's duplicate, another client's record left by a lost
   or misdirected write) or two swapped records passed every check, count
   and trailer included. A RESTORE then deleted the client whose record
   was replaced (a completed request could run again) and a fetch renamed
   the file. Fix 72abe58, clients file format 2: a 16-byte trailer
   `{magic, count, crc, reserved}` whose crc is the CRC32C of the header,
   every record without its own CRC, and the trailer's magic and count,
   extended by the writer as it stages and by the sequential reader as it
   parses; record CRCs stay for the single-record readers (store CLIENT
   loads, the writer's file-only records). The first version digested the
   raw file and the new test still passed tampered files: a CRC run over
   bytes followed by their own CRC ends in a constant (the residue), so
   that digest depended only on the record lengths. Test
   `test_review_record_integrity` (duplicate, resealed foreign record,
   swap; each through a load and a fetch). 120.

5. **A witness promoted to FULL by a RESTORE whose file had gone came up
   with an empty client table** (base handling). `load_done` read ENOENT
   as an empty base whenever the store's current hard state was WITNESS.
   The store wants a file only for a held transaction whose role is FULL
   (`base_hold` reads the transaction's role) or a FULL replica's recovery,
   so the branch never served a witness and fired only on promotion. Fix
   0f14160: a missing base file is CORRUPT. Test
   `test_review_witness_promotion`. 121.

6. **Outcomes decided in the module's prepare slept until an unrelated
   wake** (liveness). `vsr_io_snapshots_prepare` runs after the core's poll
   and the links' prepare; a serve refused for want of an engine file slot
   queued its END in the link, and a SYNC failing there with its caller
   already answered queued its core completion, with nothing in
   `vsr_io_prepare`'s deadline to bring the loop back: the END waited for
   the requester's inactivity timer (the fetch ended RETRY by timeout).
   Found by the mutation test of that refusal (an OK close there would be
   a truncated file, CORRUPT at the requester). Fix 090da53: such paths
   arm the replica's CAPTURE deadline at `io->now` (`wake_now`). Tests
   `test_review_serve_no_slot`, the prepare-time failure in
   `test_review_dir_retry`. 122.

7. **A remote file's corruption latched the fetching replica** (status;
   reported open, decided by the coordinator). A FETCH whose received
   bytes failed verification completed CORRUPT, and core.c's
   `completion_failure` latches `VSR_FAILURE_SNAPSHOT` for CORRUPT from any
   snapshot op: one damaged clients file at a source failed every replica
   that fetched it, though their state was intact. FAILED goes through
   checkpoint.c's `finish()` and transition.c's `restart_selection()`:
   discovery restarts and may pick another source. Fix 766053d:
   `fetch_finish` completes such a fetch (and a source's CORRUPT END)
   FAILED; CORRUPT stays for the replica's own files. Test
   `test_review_fetch_corrupt_failed` (corrupt chunk, bad trailer, a swap
   only the file's crc catches; the .tmp unlinked each time);
   test_fetch_failures and the fetches of test_review_record_integrity now
   expect FAILED. 123.

## OPEN, not fixed

8. **The pool reserve** (docs section 11, recommendation below).
9. **A damaged source keeps failing its fetchers.** A source serves its
   own file raw (the FILE write is not read through the reader), so every
   fetch from a source whose file is damaged ends FAILED (123) until
   discovery picks another source; the source learns of it only at its own
   next load. Verifying on serve is left open (section 11).
10. **A base change during a capture** would point the writer's cold read
   of a file-only record at the new base file (`store.base_slot` as of the
   read, offsets of the old base): CORRUPT, latched. The core's checkpoint
   stages never overlap a capture with a PUBLISH or RESTORE; keeping the
   capture's base entry open and reading through its own slot removes the
   dependency.
11. Minor: a serve refused because the index's previous serve is still
   closing answers FAILED (RETRY would be accurate; the requester
   rediscovers either way); a read reporting more bytes than asked but
   within the slab's room is parsed (CORRUPT at the trailer) rather than
   reported as the I/O error it is; `load_finish` applies `base_end` to a
   store that failed during the load (harmless: the store is fenced).
   Section 11 lists the first two.

## The pool reserve: assessment and recommendation for engine part 2

The reserve (`replicas + 1` FREE slabs, decision 54's floor, set in
`vsr_io_init`) is smaller than what internal holders keep for good. Every
established link holds its send slab for its whole life (`link_send_slab`;
released only at close), so an idle engine with two peer links and two
replicas has one FREE slab once the ring took the rest. The snapshot
module's staging slabs (writer, writer cold, reader: up to three per
replica) and the store's cold slab compete for it with a new stream link's
send slab. Measured in the unit harness with `caller_slabs = 0` (two
engines, two established peer links, 29 of 32 slabs in the ring): the
FETCH took the last free slab for its reader, its stream link could not
queue its HELLO, and the fetch ended RETRY at the handshake timeout, and
again on every retry.

Recommendation (engine.c only; pool.c unchanged): reserve `links + 4 *
replicas + 1` slabs (a send slab per link; the store's cold or recovery
slab and the module's three per replica; one for a reassembly) and raise
the minimum-slabs rule by the difference, `links + 3 * replicas`, so the
ring keeps its present share. A cheaper alternative on the link side is to
release a link's send slab when its send ring drains, which would leave
`4 * replicas + streams + 1`. The module already answers RETRY at once
when a slab is missing at an op's start; the unit harness keeps
`caller_slabs = 8` until the engine changes.

## NOT A BUG, examined and rejected

- Record lengths are bounded before any CRC: `record_at` checks `length <=
  result_bytes`, then `40 + padded + 4` against the bytes before the
  trailer when the size is known, then waits for the bytes, then the CRC,
  then decodes exactly those bytes; the module never calls
  `get_clients_record` (the store does, bounded by its cursor). Header:
  magic, format, CRC, snapshot id, cluster, `count <= max_clients`.
  Trailer: magic, count, crc, reserved. Truncation: loads compare with the
  STATX size (a 0-byte read is CORRUPT), fetches need the reader DONE and
  an OK END; extension: any byte after the trailer is CORRUPT.
- Fetch rename: only after an OK END with the reader DONE, every chunk
  written at its offset one write at a time (each DATA op completed at its
  own write, all at once once the fetch failed), the file closed; a stale
  `.tmp` is truncated (`O_TRUNC`, now tested). No fsync before the rename:
  SNAPSHOT_SYNC makes the file and the name durable, which the core issues
  before the RESTORE in DURABLE mode.
- Library stream ends: `stream_finish` calls the end hook only with every
  DATA op completed (`units_used == 0`), so a chunk's slab is never freed
  under its write; a stream ending before the `.tmp` open was issued frees
  the never-installed slot (now tested).
- Leases (105): one per CAPTURE/FETCH, reserved at take; an OK completion
  hands it over with the whole checkpoint graph in its region (epoch,
  memberships and members, manifest as one span; the region's 64-byte
  slack covers the alignment padding); every other outcome and `close`
  release it. Checked per step by `check_leases`.
- Slots, slabs, file operations: each taken and released once on every
  path read (writer, reader, loads, fetch, sync, drop, discard, release,
  serve, directory); `expect_idle` now also accounts for every allocated
  engine file slot at rest (module, log, sockets).
- SYNC/DROP statuses (107) with the current core: SYNC and CAPTURE never
  overlap (one checkpoint stage at a time), SYNC follows the op that made
  the file, DROP of a held id waits (RETRY) for a job, and FAILED for a
  file being written or fetched only under a core that breaks its own
  schedule.
- A `vsr_io_store_base_record` duplicate in one load merges silently;
  the trailer crc (120) now rejects such a file first.
- A queued base CLIENT load across a base change (store phase 4):
  `base_track` runs in every poll, which the loop runs before every
  prepare; a pack inside `vsr_io_complete` (a held STORE drained at a write
  completion) is followed by a poll before the store's next issue. Engine
  part 2 must keep poll before prepare after a complete.

## Mutation testing (snapshot.c restored after each; 55 new mutants)

The runner applies each mutant to the file, rebuilds tests/unit/snapshot
(clang ASan+UBSan), runs it with a timeout, and restores the file.

| # | Mutation | Result |
| --- | --- | --- |
| R1 | base_track releases a slot under a core op (117 reverted) | test_review_capture_release, test_review_sync_release |
| R2 | fetch_settle discards without `transferred` (118 reverted) | test_review_refetch_kept |
| R3 | fetch_finish does not mark the file `transferred` | test_fetch_failures (the failed caller's private file stays) |
| R4 | op_complete keeps `transferred` | test_fetch_failures, test_review_refetch_kept |
| R5 | the poll does not re-arm a failed directory open (119) | test_review_dir_retry |
| R6 | prepare fails a SYNC at a failed directory before its retry | equivalent: FSYNC_DIR is reached only at a completion, so the poll re-arms before prepare sees it, except after a prepare_dir failure in the same prepare, whose retry fails the same way |
| R7 | `dir_retried` not reset by a new SYNC | test_review_dir_retry |
| R8 | the file digest over the record CRCs too (the residue) | test_capture (file_parse), test_review_record_integrity |
| R9 | trailer count not compared | test_load_failures (count edited, crc resealed) |
| R10 | a missing base file is an empty base (121 reverted, any role) | test_load_failures, test_review_witness_promotion |
| R11 | a load overwrites a known entry's `sequence` | equivalent: `sequence` is only ever compared with 0, and both leave a loaded entry nonzero |
| R12 | serve accepts an entry whose file is not on disk | test_review_serve_refusals |
| R13 | serve accepts an entry being discarded | test_review_serve_refusals |
| R14 | DROP of an id the registry lacks refused FAILED | test_drop |
| R15 | an unlink's ENOENT is a failure | test_drop |
| R17 | a RELEASE while a cold read is in flight | test_review_release_cold_read |
| R18 | CAPTURE's `clients_sequence` check removed | test_capture |
| R19 | a base load of a file being dropped | test_review_load_dropping |
| R20 | a serve without a file slot closes OK | test_review_serve_no_slot |
| R21 | a held FETCH without the `on_disk` check | equivalent under the core: an entry with no op, no file and a state other than WRITING, FETCHING or DROPPING exists only while a load creates it, which for an id the registry lacks is recovery's anchor load, when the core issues no op (every other path that clears `on_disk` frees the entry) |
| R22 | record CRC over the unpadded result | test_review_odd_results |
| R23 | the handed-over lease also released | test_capture (expect_checkpoint) |
| R24 | DROP under a reader not completed at once | test_serve |
| R25 | a failed capture's file not discarded | trap in entry_free (test_capture_failures) |
| R26 | the kept slot's fdatasync failure ignored | test_sync |
| R27 | a 0-byte read of a base file is data | test_review_short_file (read loop) |
| R28 | a read longer than the slab's room accepted | test_review_long_read |
| R29 | a short cold log read accepted | test_review_short_cold_read |
| R31 | a rename failure taken as success | test_fetch_failures |
| R35 | a FETCH taken while the reader is busy | test_fetch_failures |
| R37 | a source end frees the serve while its open is out | check_snapshots (test_serve) |
| R38 | fetch chunk writes without `reads == 0` | equivalent: an open in flight holds `fileop`, an unissued one leaves the step at OPEN |
| R40 | a caller failure does not abort the writer | test_capture_failures |
| R41 | wake_now does nothing (122 reverted) | test_review_dir_retry, test_review_serve_no_slot |
| R43 | no wake after the serve refusal | test_review_serve_no_slot |
| R44 | no wake after a SYNC failed in prepare | test_review_dir_retry |
| R46 | END OK short of the trailer not CORRUPT | test_fetch_failures |
| R49 | a completed capture's slot not kept | check_file_slots (test_capture) |
| R50 | a load never calls base_end | test_base_loads |
| R53 | a SYNC never cancels an unissued RELEASE | test_sync |
| R55 | CAPTURE forwarded with the core's template | test_capture (capture_begin) |
| R60 | the header's cluster not checked | test_load_failures |
| R62 | no CLOSE after the served write | test_fetch |
| R63 | a never-issued serve open's slot not freed | expect_idle slot accounting (test_serve) |
| R64 | the fetch's `reads` not set at start | test_review_fetch_open_waits |
| R65 | the .tmp opened without O_TRUNC | test_review_stale_tmp |
| R70 | `capture` not cleared at an OK completion | test_capture |
| R71 | the pending base never cleared | test_base_loads |
| R73 | a failed fetch completes RETRY | test_fetch_failures |
| R76 | RENAMEAT to the .tmp name | test_fetch |
| R78 | fetch_settle does not wait for a DISCARD | equivalent: a DISCARD starts only once the caller answered, and no settle runs again before it ends |
| T1 | codec: the trailer's crc not compared (codec.c) | test_load_failures, test_review_record_integrity |
| R79 | a failed fetch completes CORRUPT again (123 reverted) | test_review_fetch_corrupt_failed (observed failing before the fix) |
| A4 | record CRC not verified (the author's M4, rechecked under 120) | test_fetch_failures (the CRC byte flipped: the file digest leaves record CRCs out) |
| A16 | bytes after the trailer accepted (M16 rechecked) | test_load_failures |
| A20 | header count not bounded (M20 rechecked) | test_fetch_failures |

53 new mutants (R*, T1): 48 killed, 5 equivalent (R6, R11, R21, R38, R78);
the author's M4, M16 and M20 rechecked under the new trailer, killed. The
runner and the lists are in the session scratchpad, not the tree; each
entry is (file, exact snippet, replacement).
