# Store phase 4 handoff (from the phase-4 agent, resuming the paused WIP e4e2720; commits on wt/store4 from bc9c35e to the handoff commit; main merged at 4200617)

Phases 1-3: store-phase-1/2/3.md. Phase 4 decisions, placeholders in docs/io-design.md section 10 (the coordinator renumbers): S14 the chain resumes after a block's dead tail; S15 a freed slot waits for its superblock on media (FLUSHING, the store's own flush); S16 the RECLAIM and base floor terms as of the sequence on media; S17 a client record freed under the base, or newer than it; S18 a recovered log with an unreplayed op is CORRUPT; S19 the creation flushes in FDATASYNC mode; S20 a run's first record follows a recovered one.

## Commits
- bc9c35e walk replays alike under clang and gcc (argument evaluation order), seeds on stderr, VSR_STORE_TEST filter.
- 82407b6 S15/S16 finished: `flush_own` (issued without waiting for `flush_target`: a SYNC of the STORE held for the slot deadlocked), CONTINUE releases FREEING/FLUSHING and applies the terms (`media_sequence`), a lowering base binds at once; tests rewritten verdict-first.
- a13102f S17 `record_live`: a CLIENT load or capture of a record whose slot was freed under the base reads the base file.
- d5026c5 format; 22ad8b4 `store->replica` back-pointer set by init (clang-tidy 21 ArrayBound on container_of).
- 883e818 walk SYNC underflow, VSR_WALK_TRACE (steps, I/O, image dump at each crash).
- c230c57 S19 creation flush (FDATASYNC: 150 of the first 3000 seeds were false CORRUPTs at the first crash).
- 8b44752 a head exactly at the ring's end begins a new extent (a write ran past the ring into the superblock copies: flushed records lost).
- 7840c68 S17 second half: `base_offset` is used only for a record at or below the base (phase-2 bug: stale file record served).
- 6e76898 S18 + the checker's log model; fa7afff flips persist in `disk_flushed`, the walk ends after a flip crash, rows below the media sequence under flips are decision 50's residual, without flips every recovery must reach the media sequence.
- 689c090 S20 (run splice) + `txn_salt` in the walk; 911af2c tidy widening; 4ec68a0 docs.
- 6855e87, 2927f6f walk fixes from the fuzzer's two crashes (outstanding ops bounded by `operations`; housekeeping before PUBLISH steps), test_walk_bounds.
- 4200617 merge of main (stream review, uring 7.2, snapshot module): tests/unit/snapshot passes against this store.
- 53e90a0 a queued base CLIENT load is resolved again at issue (the snapshot module's open item; test_client_base_moved); 14ec2a1 lint.

## What changed (src/io/store.c, store.h, wire.h comment)
- Recovery: `recovery_judge` sets `chain_resume` for a mid-block verdict (S14); `record_run` (was `reserved`) and the S20 test in `recovery_chunk`; the S18 loop in `recovery_scan_end`.
- Freeing: `media_sequence`, `reclaim_apply` (flush_done, DSYNC write_done, RECLAIM, write_error), `reclaim_pending`, `base_mark`; `superblock_done` FREEING -> FLUSHING (FDATASYNC), `flush_done` FLUSHING -> FREE when `freeing_flush` was INFLIGHT; `prepare_flush` issues `flush_own` or a due SYNC flush.
- Creation: `creation_done` after the header write (DSYNC) or the flush after it (FDATASYNC); CREATING's prepare also calls `prepare_flush`.
- `ring_wrap` begins a new extent at the exact ring end; `pack_needs` (WIP) counts the wrap pad in the seal decision.
- Loads/captures: `record_live`, `resolve_client` (base file only up to `client_base`), `snapshot_clients` reports freed records as file-only; `prepare_load` resolves a queued base read again before issuing it.

## Invariants (in addition to phases 1-3)
- No extent's ring range crosses a multiple of `ring_size`; every segment write is contiguous in the ring (the disk model checks it).
- A slot is FREE only once a superblock naming a start other than it is on media; FLUSHING needs a flush issued after that superblock's completion.
- Freed segments hold only records no revision >= the media sequence needs; a recovery without media corruption returns >= that sequence and replays every op of its log.
- `base_offset` names the current record only while `current.sequence <= client_base`; `client_base_floor <= client_base`.
- Along the recovered chain, a record whose run exceeds its predecessor's carries `flushed >= predecessor's sequence`.

## Harness additions (tests/unit/store.c)
- Independent checker `checked_*` (superblock choice, headers, chain with S14/S20, sweeps, floor, successor rule, freed slots, log model for S18) and `checked_dump`.
- Random walk `walk_*` against a model built from the txn table; `./tests/unit/store SEED [BYTES]`; env VSR_WALK_TRACE, VSR_STORE_TEST, VSR_RECOVERY_CORPUS; `txn_salt` per run; flips reach `disk_flushed`; the disk model checks ring-contiguous writes and traces lost blocks.
- New tests: test_freeing_flush (4 variants incl. the false CORRUPT and the SYNC deadlock), test_reclaim_media (both modes), test_client_freed, test_client_newer, test_client_base_moved, test_wrap_seal, test_wrap_exact, test_recover_splice, test_walk_bounds; helpers freeing_setup, complete_writes, run_except, media_setup, crash_recover. `harness_start` zeroes `disk.flushes` after the creation's flush.
- tests/fuzzy/recovery.c includes tests/unit/store.c and runs `walk_run` on the input; `make fuzz` seeds corpus/recovery.

## Campaigns
- Seeded walks (`./tests/unit/store SEED`, ASan): the first 3000-seed pass on the WIP failed 331, all triaged into the fixes above (store: creation flush, ring end, stale base_offset, freed base record, run splice, unreplayed ops under flips; walk: argument order, SYNC underflow, non-persistent flips, stale records after flips). On the final tree 6000 seeds (1..6000; 3033 DSYNC, 2967 FDATASYNC) pass.
- Recovery fuzzer (build/fuzz, `--enable-fuzzing`, one worker, `-max_len=4096`, corpus seeded with the 12 default walks and seeds 13..112): two crashes in the first minutes, both walk bugs (outstanding ops past `operations`; no RECLAIM before PUBLISH steps), fixed with test_walk_bounds; then 35 minutes on the final tree: 45,369 execs (21/s), 2025 edges (14,278 features), corpus 745 units, no crash.
- `make check` 68/68 under clang 21 (ASan+UBSan, --enable-werror) and gcc 16 (--enable-werror --disable-sanitize) after the merge of main; format-check, shellcheck, clang-tidy 21 and cppcheck clean (tests/fuzzy/recovery.c checked by hand with the fuzz build's flags).

## Open items
- Decision 50's residual reaches the client table: corruption of records on media no floor covers returns an older row; missing ops are CORRUPT (S18) but a lost CLIENTS record silently leaves the older one (docs section 11). Bounding freeing by the durable floor on media closes it in DURABLE mode only.
- A crash during creation (before NOT_FOUND) leaves a log recovery calls CORRUPT; no directory fsync after the create.
- A foreign base that does not cover a record freed under the previous base (phase 2's file-only open item) now also covers records S17 serves from the file.
- Phase 1-3 items remain: flush_target waits for held STOREs' SYNCs (the own flush no longer does), CONTINUE's `written`, decision 57's listing, snapshot_clients beyond 16 entries in the harness.

## Mutation candidates (for the mutation-testing agent)
1. `recovery_judge`: `at % block != 0` for `chain_resume` (S14).
2. `recovery_chunk`: the S20 clause (`header.run > r->record_run && header.flushed < r->sequence`), `r->record_run = header.run`.
3. `recovery_scan_end`: the S18 loop bounds.
4. `superblock_done`: FDATASYNC -> FLUSHING; `flush_done`: INFLIGHT vs WANTED; `prepare_flush`: WANTED -> INFLIGHT, `due` vs `flush_own`, clearing `flush_pending` only when due.
5. `reclaim_apply`: `min(reclaim, media)`, the pending test `<= media`; `base_mark`: the lowering; `media_sequence`: memory-only.
6. `store_pack`: `reclaim_pending` hold and `written > flushed`.
7. `ring_wrap`: the exact-end extent (`newest->ring_offset < head`); `pack_needs`.
8. `record_live`: `sequence >= first_sequence`; `resolve_client`: `version->sequence <= client_base`; `snapshot_clients`: the freed branch and the floor; `prepare_load`: the re-resolution of a queued base read.
9. `write_done`/`flush_done`: `creation_done` in FDATASYNC only after the flush.
10. `write_error` under CONTINUE: the FREEING/FLUSHING release.
