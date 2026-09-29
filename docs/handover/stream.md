# Stream module handoff (for the snapshot and engine agents)

Branch `wt/stream`, on top of 8fe324d (link phase 3): 48a1445 Implement the
stream module, 9a90ae3 Test it over the link module, 46c3c5d Log decisions
93 to 98, d1cbff9 Adapt the link's stream test (the one link.c-test edit;
drop or re-apply it if the reviewer's work conflicts). clang (ASan+UBSan)
and gcc `make check` 66/66; format-check, clang-tidy and cppcheck clean on
stream.c, stream.h, tests/unit/stream.c and tests/unit/link.c; mutations
caught: the window bound, END before every DATA completed, WRITTEN at the
send result instead of the NOTIF, a short read taken as the whole chunk. `src/io/stream.h`,
`src/io/stream.c` implement both roles of a bulk transfer over a
STREAM-purpose link; `tests/unit/stream.c` drives them over the real link
module. Decisions 93..98 in docs/io-design.md section 10 (placeholders, the
coordinator renumbers); docs/io-implementation.md "Streams", 7.1, 7.2, 7.4,
7.6, section 10 and section 11 updated; `include/vsr-io.h`'s bulk-stream
comment carries the lease rule and AGAIN for STREAM_WRITE.

## Data structures (stream.h)

- `struct vsr_io_streams`: `streams[limits.streams]`, `units[streams *
  stream_window]`, `writes[streams * stream_window]`, `count`, `window`,
  `chunk_bytes`, `active` (streams not FREE; `vsr_io_stats.streams` reads
  it), `closing`.
- `struct vsr_io_stream`: `state` (FREE, DIALING, REQUESTED, SERVING, OPEN,
  ENDING), `owner` (CALLER, LIBRARY), `direction` (OUTBOUND = requester,
  INBOUND = source), `link` (kept after the loss, to watch the entry's
  teardown), `generation` (in handles and op ids; bumped at every take,
  30 bits), `deadline` (handle bound by the engine), `cookie` (requester:
  the caller's; source: the handle), `node`, `offset` (bytes delivered /
  assigned to chunks), `replica` (library streams; snapshot.h sets it),
  `request` + `request_lease` + `request_slab`, `request_pending`,
  `serve_op`, `accepted`, `link_gone`, `end_due`, `end_sent`, `ended`,
  `end_offset`, `send_end` (link offset after the last frame queued),
  `status`, the unit ring (`units_head`, `units_used` = ring span, holes
  included), the write ring (`writes_head`, `writes_count`), `aborted`.
- `struct vsr_io_stream_unit`: one chunk. States READING (FILE read to
  issue when `slot == NONE`, else in flight; `filled` bytes so far, a short
  read resumes), READY (chunk bytes ready to send), SENT (frame queued;
  freed once `notified_offset >= send_end`), DATA (requester: DATA op at
  the caller/library, one slab reference). `slab` NONE for a BUFFERS
  chunk, whose spans are `span`/`span_offset` into its write's blob;
  `write` is the ring index of its write.
- `struct vsr_io_stream_queued_write`: a STREAM_WRITE (kind, slot, write
  id, lease, offset/length, `chunked` bytes, spans + chunking cursor,
  `units` not yet freed). Done when `chunked == length && units == 0`, then
  WRITTEN in write order.
- Handles: source handle = `generation << 32 | index` (`vsr_io_streams_handle`).
  Op ids: `VSR_IO_STREAM_OP_SERVE | gen << 32 | index` and
  `VSR_IO_STREAM_OP_DATA | gen << 32 | unit << 16 | index`;
  `vsr_io_streams_op_kind(id)` tells them from HANDSHAKE ids (top bits
  clear). `vsr_io_streams_size` is ELIMIT beyond 65535 streams/window.

## Invariants (tests/unit/stream.c `check_streams` checks them every step)

- `units_used <= window`, `writes_count <= window`, `active` = streams not
  FREE; the ring's head is never a FREE unit (trimmed after every
  releasing loop: `unit_release` never trims by itself).
- Requester units are DATA with a slab reference; source units are never
  DATA; a unit with a slab holds a pool reference; a source stream's
  `request_slab` holds one until `served`.
- Every close of the stream's link is issued by `drop_link` (sets
  `link_gone` first; the synchronous re-entrant `link_lost` returns at
  once) and never from inside `vsr_io_streams_sent`.
- Loss effects (`stream_loss_effects`): status RETRY (CANCELLED at
  shutdown) unless already ENDING; a source ENDING with `end_due` keeps its
  status only if the END frame reached the kernel (`sent_offset >=
  end_offset`); the source stops chunking (`source_abort`).
- `link_quiet`: the stream's frames are no longer read once the link
  entry is gone (FREE or reused: `link->stream != index`) or
  `notified_offset >= send_end`. `stream_finish` (poll only) emits END /
  the library end, then frees; a source lingers until `link_gone` (96).
- Source send order: units are queued in ring order; a READING unit blocks
  the later ones; END goes after every unit is SENT and every write chunked.

## What the engine must call, in which order

1. `vsr_io_complete`: `vsr_io_slots_resolve`; slot kind `VSR_IO_SLOT_STREAM`
   -> `vsr_io_streams_complete(io, slot, cqe)` (it consumes the slot).
2. `vsr_io_poll`: set `io->now` (the module arms deadlines from it); drain
   deadlines, kind `VSR_IO_DEADLINE_STREAM` -> `vsr_io_streams_deadline(io,
   index, now)`; then `vsr_io_links_poll`; then `vsr_io_streams_poll(io,
   now)` (chunking, sends, WRITTEN/END ops into the forwarded ring with
   `vsr_io_forward(io, NULL, kind)`, link closes, stream release). A full
   forwarded ring sets `forwarded_overflow` (poll reports MORE) and the
   emission is retried at the next poll; a frame that finds no unit, no
   stream or no ring room is left in the link (`vsr_io_streams_frame`
   false).
3. `vsr_io_submit`: STREAM_OPEN -> `vsr_io_streams_open(io, event.id
   (cookie), data, event.lease, VSR_IO_STREAM_CALLER, &index)` (ELIMIT /
   EINVAL stop the batch); STREAM_WRITE -> `vsr_io_streams_write(io, data,
   event.lease)` (AGAIN stops the batch: the queue holds stream_window
   writes; EINVAL for a bad handle / not OPEN / malformed buffers);
   STREAM_CLOSE -> `vsr_io_streams_close(io, event.id, event.status)`;
   rail COMPLETE -> by `vsr_io_streams_op_kind(event.id)`: SERVE ->
   `vsr_io_streams_served(io, id, status)`, DATA ->
   `vsr_io_streams_data_done(io, id)`, else `vsr_io_links_handshake_done`;
   EINVAL from these means a stale or repeated id. No RELEASE op is emitted
   for STREAM_OPEN/WRITE leases (94): STREAM_END and STREAM_WRITTEN say the
   bytes are free; the caller's lease ids are only stored.
4. `vsr_io_prepare`: after `vsr_io_links_prepare`, `vsr_io_streams_prepare(io,
   sqes, capacity, &count)` continues the same array/count (FILE chunk
   reads: READ | FIXED_FILE | FIXED_BUFFER, fd = the caller's slot, one
   completion, slot kind STREAM, owner = stream, sub = unit).
5. `vsr_io_close`: `vsr_io_streams_shutdown(io)` (every stream ends
   CANCELLED; refuses new opens/requests); the engine is closed once
   `io->streams.active == 0` besides the link and slot conditions (the
   caller must still complete its DATA and SERVE ops; a read in flight
   completes first).
6. Forwarded ops: SERVE `{handle, node, request span into the slab}` with
   `op.id` = SERVE id; DATA `{cookie, offset, span into the slab}` with the
   DATA id; END `{cookie or handle, bytes, status}` and WRITTEN `{handle,
   write}` with id 0. `op.data` points at the entry's rail (the ring entry
   is reused only after the poll that dequeues it).

## The snapshot hooks (98; weak stubs at the end of stream.c, delete them)

- Source: `int vsr_io_snapshots_serve(io, stream, const
  vsr_io_wire_library_request *)` is called at the request frame (before
  the stream forwards anything) with the decoded request. Return OK to
  serve: set `io->streams.streams[stream].replica`, then (asynchronously)
  open the file into a slot and submit one FILE write with
  `vsr_io_streams_write(io, &{stream = vsr_io_streams_handle(&io->streams,
  stream), write = anything, kind = FILE, slot, offset, length}, 0)` and
  `vsr_io_streams_close(io, handle, VSR_IO_OK)`. Any other return (RETRY,
  NOT_FOUND, FAILED) refuses: END with that status, then the link closes.
  A served library stream gets no WRITTEN; its end (any status) comes
  through `vsr_io_snapshots_stream_end(io, replica, stream, status)`: close
  the slot and decrement `readers` there. The write's file range is read
  `stream_chunk_bytes` at a time into pool slabs (internal priority); a
  short read resumes; a read of 0 or an error ends the stream with FAILED.
- Requester: `vsr_io_streams_open(io, cookie, &open{node, request = the
  56-byte wire request in the module's memory, pinned until stream_end},
  0, VSR_IO_STREAM_LIBRARY, &index)`, then set
  `io->streams.streams[index].replica`. Chunks arrive through
  `vsr_io_snapshots_stream_data(io, replica, stream, op, offset, bytes,
  slab)` in order; the bytes stay pinned until
  `vsr_io_streams_data_done(io, op)` (at most stream_window outstanding;
  the link holds further frames meanwhile). The end comes through
  `vsr_io_snapshots_stream_end(io, replica, stream, status)` once every
  chunk was completed; the stream is freed right after. OK means every
  byte of the source's file arrived; RETRY a loss/timeout/refusal; FAILED
  a source read error or a protocol error; NOT_FOUND etc. the source's
  serve status.
- `tests/unit/stream.c` defines strong versions of the three hooks as test
  doubles (they override the weak stubs); once snapshot.c exists, that
  test binary must not pull snapshot.o in (it references nothing else of
  it), or the doubles clash.

## Harness (tests/unit/stream.c)

The two-engine world of tests/unit/link.c, copied (not shared), plus:
caller files behind registered slots `FILE_FD_BASE..` (`engine.files[]`,
READ records against them, `world.short_read`, `random_reads`,
`fail_read`); `engine_step` sets `io->now`, dispatches completions by slot
kind, deadlines by kind, runs both polls and both prepares;
`world_run_checked`/`settle` check the stream invariants every step;
`sink` (the requester's caller: takes DATA/END ops, completes DATA at once
or holds them) and `feed` (the source's caller: SERVE, WRITTEN, END,
`feed_write_buffers` with span slicing, `feed_write_file`); `pump` steps
and drains until nothing moves (a real loop completes DATA ops every
iteration; running the world to quiescence without the caller acting
piles frames up in the link, see the open issue). `forwarded_take`
rotates ring entries, so tests read `f->rail.X`, not `op.data`.
`tests/unit/stream SEED` seeds the random walk (40 rounds of transfers
with random writes, spans, receive slicing, held DATA ops and NOTIFs,
clock jumps and resets; prints a summary). Seeds 1..80 pass.

## tests/unit/link.c (one separate commit, the last one)

`test_stream` used engine b as a frame sink that the stub tolerated (a
chunk at a source, a second request); the real module refuses those.
The sink is now a test-owned listening peer (`peer_listen`, `peer_accept`,
`peer_handshake`: the acceptor answers with a HELLO alone), every link-level
assertion kept, byte counts read from the peer. Drop or re-apply that
commit if the reviewer's link.c work conflicts.

## Open issues

- Stream backpressure vs. the link's held-run bound (docs section 11): a
  requester whose caller falls behind by more than VSR_IO_LINK_HELD (8)
  receive runs (about 8 slabs) has its link closed -ENOBUFS by the link
  module, before the pool starves (the layout minimum already exceeds 8
  slabs); the stream ends RETRY at the requester while the source may
  report OK. Fix belongs to the link module: pause the RECV of a link
  whose stream frame is blocked (cancel, re-arm at poll), or a bigger
  bound for stream links.
- A source's lingering after END (96) holds its link entry and stream for
  up to handshake_timeout_ns when the requester never closes; the timer
  bounds it.
- The request frame of a requester whose slab_bytes cannot hold 8 + 4096
  + 24 bytes fails with FAILED (ELIMIT from the link); the engine could
  refuse such an open earlier.
- `vsr_io_streams_open` scans the table for a duplicate caller cookie
  (O(streams)); streams are few.
