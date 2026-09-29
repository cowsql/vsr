# Stream module review (src/io/stream.c, stream.h, tests/unit/stream.c)

Worktree build/wt/stream-review, branch `wt/stream-review`, on top of
1ba88c1 (main merged). The review started with a paused reviewer's WIP
(7482545: decision B2, unbuilt); this pass verified it and finished the
review. Every fix has its breaking test first, observed failing before
the fix (section "Review" at the end of tests/unit/stream.c). link.c and
store.c untouched; include/vsr-io.h changed in one comment (section 10
row).

Commits: 7482545 (B2, previous reviewer), bdc3735, ea18dd2, 79fe89e,
977b57f, 7c62e27 (fixes, each with its test), 97921e6, eb42291, 6c7edcf
(tests closing mutation survivors, formatting), 0e3b55e (decisions and
docs), a clang-tidy fix to a test, then this file.

Checks (kernel 7.2, clang 21 ASan+UBSan and gcc 16 --disable-sanitize,
both --enable-werror): `make check` 65/66 on both, the one failure being
executor_conformance's uring `connect_refused`, which main fixed after
this branch's merge (see open items); tests/unit/stream also with seeds
1..80, 7, 12345, 48879. clang-format 21 clean on every touched file
(`make format-check` stops only at tests/integration/clients.c, fixed on
main by ed82e1a); clang-tidy 21 (`-p build/asan`) and cppcheck 2.21 (the
Makefile's flags, filtered to the files) clean on src/io/stream.c and
tests/unit/stream.c; shellcheck clean.

## CONFIRMED (6, all fixed)

0. **B2, from the paused reviewer: a DATA op id named the chunk's ring
   slot**, so an id completed twice after the ring wrapped freed a later
   chunk's live unit (a second slab release under the caller). The id
   now carries the chunk's 16-bit sequence and the unit is found by its
   distance from the head's sequence (`chunks - units_used`). Verified:
   test_window's B2 block fails at `k.held_ops[1] != first` with main's
   stream.c and passes with the fix; the math holds across the 32-bit
   wrap of `chunks` (2^32 is a multiple of 2^16) and for windows up to
   65535.

1. **No stream ended CANCELLED in vsr_io_close's documented order**
   (area: loss status). Section 7.7 shuts the links down before the
   streams; `vsr_io_links_shutdown` closes every link, `link_lost` ended
   each stream RETRY, and the stream shutdown then kept that status for
   every stream already ending. vsr-io.h promises CANCELLED when the
   engine closes; a requester's caller would retry against a closing
   engine. Fix bdc3735: `link_lost` reports CANCELLED while
   `io->links.closing`; either order now gives the same statuses, the
   peer still RETRY. Test `test_review_shutdown_order` (both engines
   closing, and the source's alone). Decision B3.

2. **A DATA completion did not re-arm the requester's clock** (area:
   deadlines). Decision 97 re-arms at every completion and caller call;
   `vsr_io_streams_data_done` did not, and the frame its completion let
   through is delivered in the links' poll, after the poll drained the
   deadlines: a caller freeing its window just before the deadline lost
   the stream RETRY when its next poll came after it, and one completing
   newer DATA ops while holding the oldest never re-armed at all. Fix
   ea18dd2: re-arm while REQUESTED (untimed after END as before). Test
   `test_review_data_done_rearms`.

3. **STREAM_CLOSE racing a read failure was EINVAL; any int32 went out as
   the END status** (area: close order). Close was OK on an ending stream
   only with `end_due` clear ("lost or cancelled"), but a file read that
   fails after the caller's last write sets `end_due` too, and the caller
   cannot know before the END op; EINVAL is what vsr.h reserves for a
   broken call. And close did not check its status: the requester's
   decoder refuses a status outside enum vsr_io_status, so a caller's bad
   value became a protocol error at the peer (FAILED, frames_rejected,
   -EPROTO) while the source reported the bad value. Fix 79fe89e: a
   `closed` flag; close is OK once per accepted stream until its END op
   (a no-op after an engine-decided end), EINVAL after the caller's own
   close or refusal, after the END op, or for a bad status. Test
   `test_review_close`. Decision B4 (the WIP had documented the old
   behaviour in stream.h; that comment is replaced).

4. **A failed read over-reported the END's byte count** (area: unit ring).
   `vsr_io_streams_complete` released and trimmed the failed unit before
   `source_fail`'s abort computed the cut, so `offset` fell back to the
   next unit (a read in flight behind it) or not at all: the END frame
   and the source's END op said 1000 bytes where 0 were sent (a failed
   head chunk), 2000 where 1000 were (a failed last chunk). The
   requester's status was FAILED either way, which is why test_file's
   read error passed. Fix 977b57f: the unit stays for the abort, which
   cuts at it and releases it. Test `test_review_read_in_flight` (also
   covers reads in flight at a shutdown and behind a failed read, dropped
   at their completion). Implements decision 97 as written.

5. **A caller's FILE write could name one of the engine's slots**
   (hardening). The engine would read its own descriptors (a link's
   socket, the store's log, a clients file) and stream the bytes to the
   peer. Fix 7c62e27: EINVAL for a caller stream's range on
   `[file_slot_base, file_slot_base + file_slots)`; library streams (the
   snapshot module's served file) are exempt. Test
   `test_review_engine_slot`. Decision B5.

Docs (0e3b55e): decisions B3-B5 in docs/io-design.md section 10;
docs/io-implementation.md Streams (write/close refusals, early ends,
timeouts), 7.7, section 10 rows (the B2 row loses its close sentence,
now B4's), two section 11 items; stream.h and vsr-io.h comments.

## Test gaps closed (mutants that survived the suite)

- The forwarded ring full: a request waiting for SERVE room, a chunk for
  DATA room, WRITTEN and END retried at the next poll. The harness always
  drained the ring, so dropping any of them, or forwarding into a full
  ring (a trap), went unnoticed: `test_review_full_ring` (ring of one).
- The source's linger ending at its timer while the requester, blocked
  on its window, has not closed: `test_review_data_done_rearms`.
- A chunk re-arming the requester's clock while the caller holds its
  DATA ops (masked by fix 2): `test_review_chunk_rearms`, which also
  checks the requester waits untimed after END.
- A read completing after an abort treated as data:
  `test_review_read_in_flight`.
- `stream_resolve` ignoring the generation, a zero-length chunk becoming
  a DATA op: `test_review_stale_ids` (reuses both engines' slots and
  replays the old SERVE id, handle and DATA id).
- A request accepted after the stream shutdown, and the dial clock
  (test_timeout carried the comment of a dial test that did not exist):
  `test_review_closing_and_dial` (a test-owned listener that never
  accepts).

## Checked and found fine

- Which side reports what (97, 99): requester lost while DIALING or
  REQUESTED RETRY (CANCELLED if its own engine closes, B3); after the END
  frame it keeps the END's status (a mismatched count is FAILED), also
  at a later loss, timeout or shutdown; a refusing side's protocol error
  FAILED, the other side RETRY; source lost while SERVING: END op RETRY
  only once the caller accepts, nothing after a refusal; source after
  CLOSE keeps its status iff the END frame reached the kernel
  (`sent_offset`), which in io_uring's CQE order precedes the peer's EOF;
  a read failure FAILED on both sides; timeout RETRY on both; a paused
  requester reads a reset only when it receives again, bounded by its
  own clock; shutdown CANCELLED on the closing side, an ending stream
  keeps its status.
- Exactly once: END op (`ended`, emission retried on a full ring); a
  WRITTEN per queued write in order before END (the `writes_count == 0`
  gate and one FIFO ring; abort marks writes chunked); DATA units live
  until the caller completes them, repeats and stale ids EINVAL (B2 and
  the generation); slab references released once (`unit_release`
  asserts, request slab at `served`); `active` pairs take/free and every
  non-FREE state reaches `stream_finish` (DIALING/REQUESTED by loss,
  timeout or END; SERVING by `served`; OPEN by close, loss, timeout;
  lingering by EOF, timer or shutdown).
- The link's synchronous callbacks: `link_lost` re-entry is guarded by
  `link_gone`, which every close path sets; `requester_send_request`'s
  permanent failure cannot run inside `sent` (send_frame checks ELIMIT
  before EBUSY, so the first attempt at `link_up` already fails); frames,
  `link_up` and `sent` reach only ESTABLISHED links, so a stream slot
  reused while an old link of the same index is CLOSING never sees its
  events; `stream_link` rejects a freed or re-bound entry.
- `link_quiet` and SENT units after a close that dropped the build: the
  notified offset stays below them until the entry frees (conservative,
  never early).
- Snapshot hooks (98): the weak stub's synchronous `data_done` inside
  `stream_data`; `stream_end` runs before `stream_free` (a hook opening a
  new stream cannot take the slot); a library source is told only when
  accepted; the WIP snapshot module (wt/snapshot) ignores write/close
  results and closes after a failed open, which B4 keeps OK after a loss.
- Deadlines and teardown (74, 87): streams' deadlines are dispatched
  before the polls; a stream's END op never waits for the link's
  CONNECT cancel (its request was never queued); a lost stream's clock
  is disarmed and nothing re-arms it.

## Mutation testing (stream.c restored after each; 45 mutants)

| Mutant | Result |
| --- | --- |
| Loss keeps no status for an END in the kernel | test_loss |
| No linger: the source closes after its END op | test_window |
| data_done by slot, no position bound nor sequence check | test_window (B2) |
| data_done by `sequence % window` keeping the position bound | equivalent (the unit's slot is its sequence mod window) |
| A late OK SERVE on a lost stream owes no END op | test_loss |
| END op before the END frame is notified | test_review_close |
| Abort leaves the writes unchunked | test_backpressure |
| link_lost always RETRY (fix 1 reverted) | test_review_shutdown_order |
| data_done does not re-arm (fix 2 reverted) | test_review_data_done_rearms |
| close never marks `closed` | test_review_close |
| close keeps OK as the END status | test_review_close |
| END byte count mismatch ignored | test_protocol |
| Abort keeps `offset` | test_file |
| Failed read released before the cut (fix 4 reverted) | test_review_read_in_flight |
| Chunk does not re-arm the requester | test_review_chunk_rearms (survived before) |
| Aborted read completion taken as data | test_review_read_in_flight (survived before) |
| Linger untimed | test_review_data_done_rearms (survived before) |
| Requester keeps its link after END | test_window |
| A read of 0 is not a failure | test_file |
| Shutdown RETRY | test_backpressure |
| Requester chunk / source request ignore ring room | test_review_full_ring (survived before) |
| WRITTEN / END op dropped on a full ring | test_review_full_ring (survived before) |
| Requester window bound off by one | trap in unit_alloc |
| Library source told of a refused end | test_library |
| `served` releases no request slab | test_basic |
| `stream_resolve` ignores the generation | test_review_stale_ids (survived before) |
| Generation not bumped at take | test_basic |
| SENT released one frame late | test_errors |
| Request accepted while closing | test_review_closing_and_dial (survived before) |
| Zero-length chunk becomes a DATA op | test_review_stale_ids (survived before) |
| Refusal sends no END frame | test_loss |
| Library chunk skips the hook | test_library |
| Library source gets WRITTEN | test_library |
| Short read taken as complete | test_file |
| OK SERVE does not open the stream | test_errors |
| Chunk at a wrong offset accepted | test_protocol |
| END frame before the writes are chunked | test_window |
| Serve refusal with status OK | test_loss |
| Deadline ignores `link_gone` | equivalent (nothing re-arms a lost stream's clock) |
| Requester clock left armed after END (alone, or with the above) | equivalent (a deadline on an ending stream keeps its status) |
| A frame on a link bound to another stream accepted | equivalent (only ESTABLISHED links deliver, always their stream's) |
| open does not arm the dial clock | equivalent (the link's handshake timer ends the dial at the same time) |

## OPEN, not fixed

- A STREAM_WRITE or STREAM_CLOSE racing a STREAM_END still unread in the
  forwarded ring is EINVAL once the stream was freed (a lost source is
  freed right after its END op). vsr-io.h now says a caller may get
  EINVAL there; removing the race needs the ring to report dequeues.
  Section 11.
- A source whose linger ends at its timer (requester slower than
  `handshake_timeout_ns` per window) closes first; a multishot receive
  posting data and the EOF in one batch while a frame waits for a unit
  closes the requester's link with the END still held (RETRY there, OK at
  the source). Section 11.
- `stream_resolve` ignores bits 16..31 and the kind bits of a handle, so
  a SERVE op id works as its stream's handle; harmless (index and
  generation still name one stream), could be tightened.
- Not streams: tests/integration/executor_conformance fails on this
  branch on kernel 7.2 (uring columns, `connect_refused`, both
  compilers); main fixed it after this branch's merge (4962691), and
  353b6a0 clears clang-tidy 21 / cppcheck 2.21 findings in other test
  files. Not merged here (the reviewer does not merge).
- From the author's handoff, still open: the request of a requester whose
  slabs cannot hold 8 + 4096 + 24 bytes fails FAILED only at the link;
  `vsr_io_streams_open`'s duplicate-cookie scan is O(streams).
