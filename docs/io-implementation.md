# I/O layer implementation

The acceptance target is the contract in `include/vsr-io.h`,
`include/vsr-sim.h` and `include/vsr-client.h`, designed in
[io-design.md](io-design.md) and refined by its decision log (decisions 32
to 51 record the choices this document applies). This document is the
internal architecture that the implementation follows: the source layout,
every module with its private header, the byte formats, the indexes, the
engine's entry points, the executor contract that both executors satisfy,
and the tests. The private headers under `src/io`, `src/sim` and
`src/client` are the interface specification: they declare every function
with its semantics and are compiled by the header check, so this document
does not repeat declarations; it explains what they must do together.

Conventions carry over from [implementation.md](implementation.md): C11,
one owner thread, no allocation and no I/O outside the two modules that
touch the kernel, fixed-capacity arrays sized by layout functions with
checked arithmetic (`src/checked.h`), hot descriptors separate from cold
metadata, `config.h` first in every implementation file, and tests in
`tests/` with `tests/lib/check.h`. Internal symbols are prefixed `vsr_io_`
with a module infix (`vsr_io_store_`, `vsr_io_links_`), `vsr_sim_` in the
simulation and `vsr_client_` in the client.

## 1. Source layout and dependency order

```
 src/io/            the engine and its planners, the io_uring executor
   cursor.h          cursor.c     boundary-aware byte reader
   crc32c.h          crc32c.c     CRC32C (exists)
   wire.h                         byte layouts (network, store, clients file)
   codec.h           codec.c      host structs <-> wire; region sizing
   pool.h            pool.c       slabs, refcounts, provided-ring bookkeeping
   slots.h           slots.c      user_data table
   deadline.h        deadline.c   deadline set (heap)
   link.h            link.c       nodes, authorizations, links, handshake,
                                  send coalescing, receive framing
   stream.h          stream.c     bulk streams, both roles
   store.h           store.c      log format, indexes, tail ring, writes,
                                  recovery
   snapshot.h        snapshot.c   clients files and the joint snapshot ops
   engine.h          engine.c     struct vsr_io, replicas, entry points
   uring.h           uring.c      vsr_io_uring_* over the io_uring syscalls
   uapi/io_uring.h                the kernel's UAPI header, vendored
 src/sim/           the simulated world
   sim.h             world.c net.c disk.c exec.c
 src/client/        the client bookkeeping
                     client.c     structures private to the file
```

Dependency order, lowest first; a module includes only headers below it
except that every planner takes `struct vsr_io *` (declared in `vsr-io.h`)
to reach the engine-wide tables through `engine.h`, exactly as the core's
modules take `struct vsr *`:

| Level | Modules | Depends on |
| --- | --- | --- |
| 0 | `crc32c`, `cursor`, `wire` | `vsr.h`, `vsr-io.h` |
| 1 | `codec` | level 0 |
| 2 | `pool`, `slots`, `deadline` | `vsr-io.h` |
| 3 | `link`, `stream`, `store` | levels 0 to 2 |
| 4 | `snapshot` | `store`, `stream` |
| 5 | `engine` | everything above; the only module that calls the executor |
| 6 | `uring` | `vsr-io.h`; `uapi/io_uring.h`; the kernel |
| 6 | `sim` | `vsr-io.h`, `vsr-sim.h`; libc |
| 6 | `client` | `vsr.h`, `vsr-client.h`, `crc32c` |

Two modules touch the kernel: `uring.c` (through the `io_uring_setup`,
`io_uring_enter` and `io_uring_register` syscalls, `getrandom`,
`clock_gettime`, `eventfd`) and the simulation (`malloc` only). Every other
module is a pure planner over records: it consumes ops, events and
completion records and produces submission records, events and
completions. All of it is one archive, `src/libvsr.a` (decision 32);
`Makefile.am` lists the sources in this order.

## 2. Memory model

Every region is sized by a layout function with checked arithmetic and
supplied by the caller (`docs/io-design.md`, section 4). The engine has two
regions, a replica has two more:

```
 vsr_io_layout(options)                     vsr_io_replica_layout(io, options)
 ┌──────────── metadata (page) ───────────┐ ┌──────────── metadata ───────────┐
 │ struct vsr_io                          │ │ struct vsr_io_replica          │
 │ listen address copies                  │ │ core arena (vsr_layout)        │
 │ pool bookkeeping + provided-ring memory│ │ seed copy                      │
 │ slot table                             │ │ event queue [events]           │
 │ deadline set                           │ │ message queue [regions]        │
 │ node, authorization, link tables       │ │ step scratch: events, ops      │
 │ per-node send queues [nodes*link_queue]│ │ lease table [regions]          │
 │ per-link vectors [links*SEND_VECTORS]  │ │ decode regions [regions]       │
 │ stream table + units                   │ │ store bookkeeping (indexes,    │
 │ forwarded-op ring [ops]                │ │   segments, extents, queues)   │
 │ replica table [replicas]               │ │ snapshot registry, capture     │
 └────────────────────────────────────────┘ │   table [max_clients], regions │
 ┌──────────── payload (page) ────────────┐ └────────────────────────────────┘
 │ slabs[slabs] of slab_bytes: registered │ ┌──────────── tail (block) ───────┐
 │ as ONE executor buffer region          │ │ ring [cache_bytes] + 2 super-  │
 └────────────────────────────────────────┘ │ blocks: registered as one      │
                                            │ executor buffer region         │
                                            └────────────────────────────────┘
```

Derived capacities, all from `vsr_io_limits`, `vsr_io_store_options` and
the replica's `vsr_limits`, checked at `vsr_io_layout` and `vsr_io_attach`
with `VSR_ELIMIT`:

| Capacity | Value | Where |
| --- | --- | --- |
| Decode regions per replica | `input_leases + events` | `engine.c`, decision 42 |
| Decode region bytes | `max(vsr_io_codec_message_region, vsr_io_codec_load_region)` | `codec.c` |
| Frame limit | `vsr_io_codec_frame_limit(limits)`; `slab_bytes >=` this and `>= stream_chunk_bytes + 40` | `codec.c` |
| Record limit | `vsr_io_codec_record_limit(limits)`; `<= slab_bytes - 2 * block_bytes` and `<= segment_bytes - header_bytes` | `store.c`, decision 37 |
| Segment header bytes | `round_up(vsr_io_codec_segment_limit(limits), block_bytes)` | `store.c` |
| Tail region | `cache_bytes + 2 * block_bytes`; `cache_bytes >= write_behind_bytes + pinned_payload_bytes + 2 * record limit` | `store.c`, decision 36 |
| Slots | `vsr_io_slots_size`: `listeners + 7 * links + streams * (stream_window + 2) + replicas * (inflight_writes + 8) + 8`; per link a receive, a shutdown, a connect and `VSR_IO_LINK_SENDS` (4) sends awaiting NOTIF | `slots.c` |
| Deadlines | `links + nodes + 4 * replicas + streams` | `engine.c` |
| Pool reserve | `replicas + 1` slabs never provided to the kernel | `pool.c` |
| Minimum slabs | `links + streams * (stream_window + 1) + 2 * replicas + 4 + caller_slabs` | `vsr-io.h`, decision 54 |
| Send queue | `link_queue` entries per node | `link.c`, decision 38 |
| Versions table | `max_entries` | `store.c` |
| Client table | next power of two `>= 2 * max_clients` buckets | `store.c` |
| Snapshot registry | `transfers + 4` per replica, the core's own hold count | `snapshot.c` |
| Store op queues | `operations` each for held STOREs, SYNCs, LOADs; `3 * operations` completions | `store.c` |

Error codes: every internal function returns `VSR_OK`, `VSR_EINVAL` for a
malformed input the caller must fix, `VSR_ELIMIT` for a capacity that can
never be met, or a status of `enum vsr_io_status` where the result is an
op completion. Negative errno values appear only in completion records and
in the executor's return values.

## 3. Modules

### Cursor (`src/io/cursor.h`)

Purpose: one bounded reader for every decoder, over one or more pieces, so
that the day a frame body arrives in two slabs (decision 24) the decoders
change nothing. Pure, no state beyond the struct.

- Invariants: `position <= length`; every accessor either succeeds and
  advances or fails and leaves the cursor unchanged; `span` reports
  `contiguous` false rather than copying.
- Emits nothing; consumed by `codec`, `link`, `store`, `snapshot`.
- Tests: `tests/unit/cursor`: reads across piece boundaries at every split
  of a 1..64-byte sequence, alignment, span contiguity, CRC equal to the
  one-shot CRC, failure leaves position unchanged.

### CRC32C (`src/io/crc32c.h`)

Exists; `tests/unit/crc32c` covers vectors, chaining and the two paths.

### Codec (`src/io/codec.h`, `src/io/wire.h`)

Purpose: the only place that knows byte layouts. Everything in section 4
is implemented here. Encoding is vectored for the network (headers written
through a `vsr_io_writer` over the send slab, payload referenced, spans
below `VSR_IO_INLINE_BYTES` copied) and a plain copy for the store;
decoding is in place into a `vsr_io_bump` region.

- Memory: none of its own; the region sizes it computes are the replica's
  decode regions.
- Invariants: `vsr_io_codec_message_digest` and `vsr_io_encoder_emit`
  produce the same bytes as `vsr_io_codec_put_record` would for the same
  objects (entries encode identically on the wire and in records); decoding
  an encoding yields a logically equal graph (`src/objects.h` equality);
  every decoder rejects any input that a bounded encoder could not have
  produced (`EINVAL`) and any that exceeds the limits (`ELIMIT`).
- Tests: `tests/unit/codec`: round trip of every message type at minimum
  and maximum limits, every store change, superblock, segment header,
  clients file records; digest equals the encoded body's length and CRC;
  emit with vector capacity 1, writer capacity 24 and budget 1 byte
  reproduces the same byte stream; truncated and bit-flipped inputs are
  rejected; region sizes suffice for the largest graphs (an oracle counts
  bump allocations). `tests/fuzzy/frame` (libFuzzer): bytes -> decode must
  never crash, and a decoded graph re-encodes to bytes that decode equal.

### Pool (`src/io/pool.h`)

Purpose: slab ownership. A slab is FREE, KERNEL or HELD (see the header);
holders are counted in one `refs`. The provided ring is the executor's;
the pool records what it provided and what completions returned.

- Receive bookkeeping: the executor never reports offsets, so
  `vsr_io_pool_recv_begin` derives the offset from the slab's `consumed`
  count, which starts at zero when the slab is provided and is exact because
  the kernel consumes each provided buffer from its start, in order, until
  `BUFFER_MORE` is clear. A slab is FREE again only when the kernel left it
  (`recv_end`) and `refs` is zero.
- Provision: `vsr_io_pool_provide` hands FREE slabs to the ring during
  prepare while `free_count > reserve + caller_slabs - caller_taken`
  (decision 54: the part of the caller's share it does not hold stays
  FREE); `starved` remembers `-ENOBUFS`, across a ring loss too, so
  receives are re-armed once the ring holds a buffer again.
- Caller slabs: `vsr_io_pool_acquire(pool, true)` fails once
  `caller_taken == caller_slabs` or `free_count <= reserve`; internal
  acquires may take every FREE slab. The caller's release goes through
  `vsr_io_pool_caller_release`, which rejects an id the caller does not
  hold (EINVAL for `vsr_io_slab_release`).
- Invariants: `free_count + kernel_count + held == slabs`; a KERNEL slab is
  never handed to `acquire`; a slab in the ring is never written by the
  engine; `caller_taken` counts the slabs flagged `caller`, all HELD.
- Tests: `tests/unit/pool`: state transitions including incremental
  consumption with many frames per slab, reserve and caller-share
  enforcement, starvation flag, ring loss, the largest ring of 32768 ids,
  and a randomized holder model (every retain has a release, caller holds
  released through `caller_release`) checking the invariant after each
  step.

### Slots (`src/io/slots.h`)

Purpose: `user_data` allocation and completion dispatch. A completion is
resolved to a slot by index; the generation rejects stale completions of a
recycled slot (a cancelled multishot's late CQE, for example) which are
dropped and counted.

- Tests: `tests/unit/slots`: allocate to exhaustion, multishot `more`
  keeping the slot, generation mismatch after free, owner-tag mismatch,
  the `expected` count of zero-copy sends.

### Deadlines (`src/io/deadline.h`)

Purpose: the one timer mechanism of the engine (decision 40). Handles are
dense per kind and bound once at init; arm and pop are heap operations.

- Tests: `tests/unit/deadline`: earliest after random arms and disarms
  equals a linear scan; pop order; re-arming a popped periodic entry.

### Links (`src/io/link.h`)

Purpose: everything between a node table entry and a decoded frame.

Link life cycle:

```
 dial:    FREE ─SOCKET/CONNECT─▶ CONNECTING ─▶ HELLO (preamble+HELLO sent)
 accept:  FREE ◀─multishot ACCEPT CQE─ HELLO (waiting for preamble+HELLO)
 adopt:   FREE ─vsr_io_adopt─▶ ESTABLISHED (flags 0) | HELLO (HANDSHAKE)
 EXTERNAL mode: CONNECTING/accept ─▶ EXTERNAL (HANDSHAKE op) ─▶ ESTABLISHED
 HELLO ─both HELLOs seen, mode and version match─▶ ESTABLISHED
 any ─error, timeout, revoke, close─▶ CLOSING ─recv terminated, NOTIFs in─▶ FREE
```

Handshake (TRUSTED): the dialer sends the 8-byte preamble then a HELLO
frame; the acceptor consumes the preamble, reads HELLO, checks `handshake`
and the frame version, records `node` and `purpose`, sends its own HELLO,
and is established; the dialer is established on receiving the acceptor's
HELLO whose `node` must equal the dialed node. Sends are queued during the
handshake and flow once established. A HELLO not received within
`handshake_timeout_ns` closes the link. A peer link from a node that is not
in the node table is closed (a node must be known to be authorized; an
unknown node cannot be authorized for any envelope).

Carrier election (decision 41): whenever a peer link to node N becomes
established or leaves ESTABLISHED, `nodes[N].carrier` is recomputed as the
established peer link whose dialer is the lower node identity (`OUTBOUND`
if `own < N`, else `INBOUND`), oldest `established_ns` first; if none has
that direction, the oldest established link of any direction. Sends to N
go to the carrier only. A non-carrier link with nothing received or sent
for `idle_timeout_ns` is closed; the carrier is never idle-closed while the
node has queued sends, and is idle-closed otherwise like any link, since a
node with no traffic needs no link (the next SEND redials).

Dialing: a node with an address and no established or pending link is
dialed when a SEND is queued for it or a replica authorizes it, immediately
the first time and then at `next_dial_ns` = last attempt +
`connect_backoff_ns << min(attempts, 4)`. A caller-dialed node (NULL
address) gets a `LINK_WANTED` op on the same schedule. Success resets
`attempts`.

Send path per link (decision 38), executed in `vsr_io_links_prepare`:

1. If a send is in flight, or every `sends[]` entry awaits a NOTIF, or the
   node's queue is empty and the encoder idle, do nothing.
2. Otherwise build one send: for the queued messages in order, run
   `vsr_io_encoder_emit` into the link's `vecs` and the free part of the
   send slab ring, stopping at `send_coalesce_bytes`, `VSR_IO_SEND_VECTORS`
   vectors or the ring's free space. Record each message's `end` stream
   offset as it is fully emitted.
3. Classify: `fixed` when every vector lies in the pool
   (`vsr_io_pool_contains`); else `zero_copy` when bytes `>=
   zero_copy_bytes`. Allocate a SEND slot expecting two completions for a
   zero-copy send and one otherwise; emit the record.
4. On the result CQE: a negative result closes the link (queued and
   in-flight messages complete `RETRY`); a short result re-issues the
   remainder from the vectors, which stay valid because every referenced
   byte is pinned by the SEND ops covered; a full result advances
   `sent_offset` and, for a plain send, `notified_offset`.
5. On the NOTIF (or the plain send's completion), advance
   `notified_offset` to the send's `end`; every queued message whose `end
   <= notified_offset` completes `OK` to its replica (a `COMPLETE` event
   queued for the next poll); the send-slab ring floor advances past the
   send's header bytes.

Receive path per link: one multishot RECV with `BUFFER_SELECT` on the
pool's group, re-armed after `-ENOBUFS` once slabs are provided and after
any termination while the link is open. Each RECV CQE with `BUFFER` gives
`(slab, offset, bytes)` through `vsr_io_pool_recv_begin`; the link then
carves frames:

1. While a preamble is expected (acceptor side), match the 8 bytes
   (possibly across CQEs); a mismatch closes the link.
2. If `partial_length` is zero, read a frame header at the current bytes;
   if fewer than 24 bytes are available, keep them as the partial (same
   slab, `partial_offset`). Validate the header (`vsr_io_codec_get_frame`
   with the frame limit); reject -> close.
3. If the whole frame (header + length) is within the current slab's
   delivered bytes, decode it in place; else, if it continues in the same
   slab on a later CQE, wait; else (the next CQE names another slab) copy
   the partial bytes into a fresh reassembly slab
   (`vsr_io_pool_acquire`, waiting if none is free) and append every later
   CQE's bytes into it until the frame is complete, then decode from the
   reassembly slab. Since a frame fits one slab and the reassembly slab is
   fresh, this always fits.
4. Decoding: check the body CRC over the cursor; HELLO goes to the
   handshake, MESSAGE to `vsr_io_engine_deliver` (which allocates a region,
   decodes, checks that `message.from` is authorized for `link.node` in
   `message.cluster`, and queues the event; a rejection counts
   `frames_rejected` and drops the frame; a missing region leaves the bytes
   in place and retries next poll), stream frames to `vsr_io_streams_frame`.
5. The slab reference taken at `recv_begin` is released once every byte of
   that CQE has been decoded (each MESSAGE lease took its own reference) or
   discarded.

Invariants: a link never has two sends in flight; `notified_offset <=
sent_offset <= stream_offset`; every queued message completes exactly
once; a slab is referenced by a link only while it holds bytes not yet
leased or discarded.

Tests: `tests/unit/link`: a fake engine feeding CQEs and checking records:
dial with backoff to 16x, accept, both handshake modes, refusal on mode and
version mismatch, simultaneous dial with carrier election on both ends and
idle closure of the loser, send classification for the three flag cases,
coalescing bounds, a message spanning two sends, short sends, NOTIF
ordering and RETRY on close, receive splits at every byte boundary of a
two-frame stream across one and two slabs, revoke and address change.

### Streams (`src/io/stream.h`)

Purpose: the two sides of a bulk transfer over a STREAM-purpose link.

Requester: `STREAM_OPEN` takes a stream and a link (`ELIMIT` when none),
dials, sends preamble, HELLO and the request frame, then receives chunks;
each chunk becomes a `STREAM_DATA` op (a window unit holding the slab
reference) forwarded in order; the unit is released by the caller's
completion. When `stream_window` units are outstanding the link's received
bytes wait in their slabs (backpressure through the pool). `STREAM_END`
arrives as a frame or is synthesized on link loss with `RETRY`; the op is
emitted once every DATA op was completed and the cookie is then reusable.

Source: an accepted STREAM link waits for the request frame; a request
beginning with `VSR_IO_LIBRARY_MAGIC` goes to `vsr_io_snapshots_serve`,
any other becomes a `STREAM_SERVE` op with the request bytes pinned in
their slab. Refusal (any completion status but OK) sends END with `RETRY`
and closes. Accepted: `STREAM_WRITE` events are queued in order. A
BUFFERS write is sent as one or more chunk frames (each at most
`stream_chunk_bytes`, vectors from the caller's lease, classified by the
send rule) and `STREAM_WRITTEN` is emitted when the link's
`notified_offset` passes the last chunk. A FILE write is chunked by
reading `stream_chunk_bytes` at a time from `slot`/`offset` into a pool
slab with a `READ | FIXED_FILE | FIXED_BUFFER`, then sent like a BUFFERS
chunk; at most `stream_window` reads or sends are in flight; the slab is
released at NOTIF. `STREAM_CLOSE` sends END with the status after the
last chunk and closes the link.

Timeouts: a stream with no progress for `handshake_timeout_ns` (the only
per-stream duration in the options; a dedicated option is deferred) ends
with `RETRY`.

Tests: `tests/unit/stream` with the fake engine (request, chunks, END,
window exhaustion, FILE chunking with short reads, link loss both sides,
refusal, library request routing) and `tests/integration/streams` over the
simulation (two engines, BUFFERS and FILE transfers of 0, 1, exact-window
and many chunks with split and corrupt faults, a stream never delays a
heartbeat on the peer link).

### Store (`src/io/store.h`)

Purpose: the indexed log. Section 5 gives the format, section 6 the
indexes, write pipeline and recovery. The module consumes the core's
LOAD/STORE/SYNC/RECLAIM in emission order and produces completions in
`completions` for the engine to feed back.

Tests: `tests/unit/store` (fake engine: every change type through the
indexes, held STOREs by pinned floor and write-behind, write planning at
block, segment and ring boundaries, superblock rewrites on segment change
and growth, freeing floor with each floor dominating in turn, SYNC in both
modes with out-of-order write completion, LOAD hot and cold for every
type at current and older revisions, versions after TRUNCATE, RECLAIM,
recovery over synthesized files including every torn-tail case of section
6.4, CORRUPT below the durable floor, the idle superblock write after
a flush with no later record, LOADs routed before the STOREs of one
update, and RETRY for a CLIENT load naming an older sequence); `tests/fuzzy/recovery` (libFuzzer:
bytes as a log file -> `vsr_io_store_open` must never crash, and the
recovered prefix must satisfy the CRC, sequence and run invariants when
re-scanned by an independent checker in the test).

### Snapshots (`src/io/snapshot.h`)

Purpose: the clients file and the joint ops (decision 43). The header
states the four op sequences; the details:

- CAPTURE: `vsr_io_snapshots_capture` requires
  `store.clients_sequence <= task->sequence` (else `FAILED`, an invariant
  since ops are processed in emission order), takes a registry entry and a
  staging slab (`RETRY` when none), draws 16 random bytes for the id
  (redrawn while zero), copies the task and its checkpoint into the
  template with the id, snapshots the table (`vsr_io_store_snapshot_clients`,
  which also sets the capture floor), and forwards the op with
  `data = &template task`. The writer then produces the file: header; for
  each snapshot entry, the record bytes come from the ring (hot: cursor
  over the pinned range) or from a cold read of the record into
  `cold_slab`; each record is encoded into the staging slab and the slab is
  written when full or at the end with `WRITE | FIXED_FILE | FIXED_BUFFER`
  (the clients file is opened without O_DIRECT, so chunks need no
  alignment); trailer; close. `vsr_io_store_capture_offset` records each
  entry's offset; `capture_end` sets `last_capture`. Completion to the
  core: `OK` with the caller's checkpoint (copied into `result_region`)
  when both halves are OK; otherwise the file is unlinked and the caller's
  status, or the library's failure, is reported. Any I/O error of the
  writer fails the capture with `FAILED`; the core schedules captures on
  its own, so the engine does not retry.
- SNAPSHOT_SYNC: `FSYNC | DATASYNC` of the file, then `FSYNC` of the
  directory (opened once per replica into a slot at attach), then forward;
  completion when both halves succeed; any failure is reported as-is (the
  core fences on it).
- FETCH: `task->peer` is resolved to a node through the authorization
  table (no node: `RETRY`); a library stream is opened with a
  `vsr_io_wire_library_request` for `(cluster, task->peer, task->checkpoint->id)`;
  chunks are written to `clients-<id>.tmp` at their offsets and parsed by
  the reader as they arrive (header, records, trailer); END with OK and a
  complete, verified file renames it to `clients-<id>` (RENAMEAT), creates
  the registry entry with `sequence = 0` (set at RESTORE), and forwards
  FETCH; any other outcome unlinks the temporary file and completes the
  core op with the stream's status (`RETRY` on loss). `basis` is passed
  through; the library half does not use it.
- DROP: forwards; on the caller's completion, unlinks the file once
  `readers == 0` (a served library stream still reading it delays the
  unlink, not the completion) and frees the registry entry.
- Base loads: `vsr_io_snapshots_load_base(id, sequence)` opens
  `clients-<id>`, reads it through the reader in slab-sized chunks, calls
  `vsr_io_store_base_begin`, `base_record` per record and `base_end`, then
  `vsr_io_store_base_resume(status)`. A missing file on a replica whose
  hard-state role is `FULL` is `CORRUPT`; on a `WITNESS` it is expected and
  yields an empty table.
- Serving: `vsr_io_snapshots_serve` finds the replica by cluster and
  replica id, requires the registry entry in WRITTEN or later state (else
  END `NOT_FOUND`), opens the file into a slot, increments `readers`, and
  feeds one FILE write of the whole file to the stream module followed by
  CLOSE OK; the slot is closed and `readers` decremented at END.

Tests: `tests/unit/snapshot` (fake engine and a fake store: file bytes
produced for a table of 0, 1 and `max_clients` entries with hot and cold
records, chunk boundaries at every record boundary, header/record/trailer
verification, each joint op's completion ordering with the caller half
completing first and last, failure on each half, DROP with a reader);
`tests/integration/snapshots` over the simulation (capture on one engine,
fetch by another, restore, recovery from the file after a crash, a corrupt
file detected).

### Engine (`src/io/engine.h`)

Section 7.

### Executor: io_uring (`src/io/uring.h`)

Section 8 is the contract; `uring.c` realizes it on Linux >= 6.18 (design
section 2) through the raw `io_uring_setup`, `io_uring_enter` and
`io_uring_register` syscalls over the vendored UAPI header
`src/io/uapi/io_uring.h` (decision 52); there is no liburing and no other
library. Specifics: the executor state and its tables live in the caller's
page-aligned region; the rings and the SQE array are the kernel's pages,
mapped from the ring descriptor by init (one mapping for both rings,
`IORING_FEAT_SINGLE_MMAP`, one for the SQEs) and unmapped by deinit before
the descriptor is closed, not caller memory through `NO_MMAP`, because the
kernel finishes a closed ring asynchronously and still writes its ring
words then (it commits the CQ tail and clears the SQ flags while it
cancels), which would land in memory the caller had already been given
back; the head, tail and flag words are addressed through the offsets
setup returns and accessed with C11 atomics, acquire loads for the words
the kernel writes (SQ head, CQ tail, SQ flags) and release stores for the
words this side writes (SQ tail, CQ head, the provided-buffer ring tail).
`IORING_SETUP_SINGLE_ISSUER | DEFER_TASKRUN | COOP_TASKRUN | TASKRUN_FLAG |
SUBMIT_ALL | NO_SQARRAY | CQSIZE` (`SQPOLL` in place of the three
task-work flags when `sqpoll_idle_ms > 0`); the ring descriptor is
registered (`IORING_REGISTER_RING_FDS`) and every later enter and register
call goes through the registered index. `cq_entries` from the options,
checked at init to be at least `2 * sq_entries` so the CQ never overflows
under the engine's slot count (every slot expects at most two
completions). Init checks once that the feature bits it relies on
(`SINGLE_MMAP`, `NODROP`, `EXT_ARG`, `MIN_TIMEOUT`, `REG_REG_RING`,
`RSRC_TAGS`, `CQE_SKIP`, `LINKED_FILE`) are set and that `IORING_REGISTER_PROBE` reports
every opcode the translation table emits, and fails with `-ENOSYS`
otherwise (decision 53); nothing is probed after that and there is no
fallback. `submit_and_wait` translates the records into the SQ (a full SQ
is drained by an enter, under `SQPOLL` by `SQ_WAIT`), publishes the tail
and enters once with `GETEVENTS` and no minimum so completions already due
are posted, then waits with `GETEVENTS | EXT_ARG` and a
`struct io_uring_getevents_arg` on the stack carrying `min_wait_usec`
(from `min_wait_ns`, `IORING_FEAT_MIN_TIMEOUT` semantics) and the relative
timeout derived from the absolute deadline; registered wait regions are
not needed. `reap` enters with `GETEVENTS` and no minimum when the SQ
flags carry `IORING_SQ_TASKRUN` or `IORING_SQ_CQ_OVERFLOW`, so deferred
task work and overflowed completions are posted without blocking, then
copies the CQEs between head and tail and advances the head once; the
eventfd wake is a multishot `POLL_ADD` whose completions are consumed in
`reap`. Registration: `REGISTER_FILES2` sparse and `FILES_UPDATE`,
`REGISTER_BUFFERS2` sparse and `BUFFERS_UPDATE` with tags,
`REGISTER_PBUF_RING`/`UNREGISTER_PBUF_RING` with `IOU_PBUF_RING_INC` for
incremental rings and `PBUF_STATUS` for the kernel's head, `REGISTER_NAPI`
when requested; `provide` writes `struct io_uring_buf` entries then
publishes the ring tail with a release store. A vectored zero-copy send
from a registered region is `SENDMSG_ZC` with `IORING_RECVSEND_FIXED_BUF`
and a `struct msghdr` naming the record's vectors, kept in a per-SQE-slot
table like the timespec of a timeout (decision 53); the other sends are
`SEND` and `SEND_ZC` with `IORING_SEND_VECTORIZED` where vectored. A
`BUFFER_SELECT` receive carries `IORING_RECVSEND_POLL_FIRST`, so the
kernel selects the buffer at a delivery and an empty ring is `-ENOBUFS`
then, never at arming; `GETSOCKOPT` at a level other than `SOL_SOCKET` is
refused at translation with `-EOPNOTSUPP` (decision 65); a zero-copy
record rejected at translation completes once, with `MORE` clear and no
`NOTIF` (decision 62); `update_buffer` of a region a pending record uses
returns 0, the kernel keeping the old registration alive until those
records complete. The errno values of the registration calls the
contract fixes (`-EBUSY`, `-EEXIST`, `-ENOENT`, `-EINVAL`) are the
executor's own checks before any kernel call.
`vsr_io_uring_layout` sums `sizeof(struct vsr_io_uring)` and the tables.
`vsr_io_uring_deinit` cancels every request still in flight
(`IORING_REGISTER_SYNC_CANCEL` with `ANY | ALL`, in bounded rounds) and
discards the completions before unmapping and closing the ring, so that
as little as possible completes into the caller's buffers during the
kernel's asynchronous teardown; a request the cancel cannot reach (a
zero-copy send awaiting its NOTIF, an operation in progress in a kernel
worker) still completes then, so the caller sees such records complete
before deinit.

Tests: `tests/integration/executor_conformance` (section 8) run over this
executor; `tests/unit/uring_translate` for the record-to-SQE table with a
fake SQE buffer (every opcode and flag combination, rejection of
out-of-region fixed buffers); `tests/integration/uring_smoke` for the ring
against the kernel.

### Simulation (`src/sim/sim.h`)

Purpose: the second executor and the world it runs in, implementing
section 8 exactly. Structure: `world.c` (create, destroy, generator,
clock, `advance`, `ready`, faults, trace, crash and restart), `net.c`
(listeners, connections, segments with delay, drop, corruption, split,
reset, partitions; sockets are `AF_INET` on the node's canonical address or
`AF_UNIX` in a per-node namespace), `disk.c` (files as block arrays with
`written_at`/`synced_at`, `O_DIRECT` alignment checks, capacity and
`ENOSPC`, bit rot on read, latency, the crash model, inspection), `exec.c`
(the ops table; each record is executed to completion or parked WAITING on
a socket, a timer or a latency deadline; completions are queued with a
global sequence so ties are deterministic).

Determinism rules: one generator; every random decision drawn in a fixed
order per event; no iteration over hash tables; `advance` delivers all
events due at `now` in `(due, sequence)` order before moving the clock. A
node's executor handle is `{ops, ctx = node}`, and the node's
`incarnation` is checked on every call so a handle from before a crash
aborts.

Ownership enforcement (design section 9): the sim checksums the bytes of
every buffer a submitted record references (recv buffers excepted) at
submission and again at completion and aborts on a change; a provided
buffer is checksummed when provided and until it is returned; a slab that
the engine released to the ring while still referenced shows up as a
change. This is `VSR_SIM_CHECK` behaviour, always on; the cost is a
memory pass per record, acceptable in tests.

Tests: `tests/unit/sim_world` (clock, generator reproducibility across two
worlds with one seed, `advance` ordering, partitions, crash model keeping
synced blocks and dropping unsynced ones with `unsynced_keep_ppm` 0 and
1000000, directory operations, inspection functions);
`tests/integration/executor_conformance` over the simulation.

### Client (`src/client/client.c`)

Purpose: `vsr-client.h`. State machine per lane:

```
 IDLE ─begin─▶ PENDING ─reply OK/EXECUTED─▶ IDLE
              PENDING ─NOT_PRIMARY, NEW_EPOCH─▶ PENDING (new attempt)
              PENDING ─BUSY─▶ WAITING ─time─▶ PENDING
              PENDING ─INVALID, LIMIT, STALE─▶ IDLE (FAILED)
              PENDING ─time (deadline)─▶ PENDING (next target)
 import:      DETACHED ─resume─▶ PENDING;  DETACHED ─query─▶ QUERYING ─queried─▶ IDLE | DETACHED
```

Target choice: `primary` when known; else members in id order starting at
`rotation`, advancing per attempt; CAUSAL reads round-robin over FULL
members from `causal_cursor`. `min_op` is the maximum of completed
replies' `op` and observed applied positions. Export writes
the documented little-endian image (see the comment in `client.c`) with a
trailing CRC32C; import validates and marks pending lanes
DETACHED.

Tests: `tests/unit/client`: every transition above, backoff doubling and
cap, membership adoption and the `ELIMIT` case, redirect keeps identity and
body and changes only the epoch, export/import round trip and rejection of
each corruption, read targets. The client is also exercised end to end by
`tests/fuzzy/iocluster` (section 9).

## 4. Wire format

All layouts are in `src/io/wire.h` with their sizes pinned by static
assertions; little-endian, natural alignment, every structure a multiple
of 8 bytes, blobs padded to 8. A connection starts with the dialer's
8-byte preamble `VSR_IO_WIRE_MAGIC` ("VSRIO\0\0\0"), then frames.

### 4.1 Frame

| Offset | Size | Field | Rule |
| --- | --- | --- | --- |
| 0 | 4 | magic | `VSR_IO_FRAME_MAGIC` "FRM1" |
| 4 | 2 | version | `VSR_IO_WIRE_VERSION` |
| 6 | 2 | kind | HELLO 1, MESSAGE 2, STREAM_REQUEST 3, STREAM_CHUNK 4, STREAM_END 5 |
| 8 | 4 | length | body bytes, multiple of 8, `<= slab_bytes - 24` |
| 12 | 4 | body_crc | CRC32C of the body (0 when empty) |
| 16 | 4 | header_crc | CRC32C of bytes 0..15 |
| 20 | 4 | reserved | zero |

The link closes on any violation. The body CRC is checked before decoding.

### 4.2 HELLO (32 bytes)

`handshake` (mode), `purpose` (PEER 1, STREAM 2), `node`, `nonce` (from
the executor's entropy; reserved for KEYED), `reserved`. A HELLO whose
mode or frame version differs from the receiver's closes the link.

### 4.3 MESSAGE

Envelope `vsr_io_wire_message` (56 bytes): cluster hi/lo, epoch, view,
from, type, flags (must be 0), number. Then the body by type:

| Type | Body |
| --- | --- |
| PREPARE | `prepare{committed}` ENTRIES |
| PREPARE_OK, COMMIT, START_VIEW_CHANGE, EPOCH_STARTED | none |
| DO_VIEW_CHANGE, START_VIEW | LOG_STATE |
| RECOVERY, RECOVERY_RESPONSE | `recovery{nonce, flags}` [LOG_STATE if flags&1] |
| GET_STATE, GET_LOG | `fetch{nonce, revision, first, end, max_bytes, max_entries}` |
| NEW_STATE, LOG, STATE_UNAVAILABLE | `state_chunk{nonce, first, next}` LOG_STATE |
| START_EPOCH, NEW_EPOCH | EPOCH |
| CHECKPOINT | CHECKPOINT |
| READ_PROBE, READ_ACK | `nonce{incarnation hi/lo, counter}` |

Nested objects:

| Object | Layout |
| --- | --- |
| BLOB | `{u64 size}` then `size` bytes, pad to 8; decodes to one span (count 0 when empty) |
| MEMBER | `{u64 id, u32 role, u32 reserved}` |
| MEMBERSHIP | `{u64 epoch, u32 count, u32 faults}` then `count` MEMBERs (`count <= members`) |
| EPOCH | `{u64 boundary, u32 phase, u32 flags}` MEMBERSHIP current [MEMBERSHIP previous if flags&1] |
| CHECKPOINT | `{id hi, id lo, op, view}` EPOCH BLOB manifest (`<= manifest_bytes`) |
| ENTRY | `{op, epoch, view, client hi, client lo, number, u32 type, u32 body_length}` then the body: COMMAND BLOB (`<= command_bytes`), RECONFIGURE MEMBERSHIP, CHECK_EPOCH `{u64 epoch}`, NOOP nothing; `body_length` is the encoded body size so an entry can be skipped |
| ENTRIES | `{u32 count, u32 reserved}` then `count` ENTRYs (`count <= batch_entries`) |
| LOG_STATE | `{revision{hi, lo, sequence}, view, last_normal_view, committed, log_begin, log_end, u32 flags, u32 reserved}` EPOCH ENTRIES [CHECKPOINT if flags&1] |

Decoding builds the host graph in the decode region: `vsr_message`, the
body struct, `vsr_epoch` with two `vsr_membership` and their member
arrays, `vsr_checkpoint`, `vsr_entry[count]` with one `vsr_blob` and one
`vsr_span` per COMMAND body pointing into the slab, and so on. The region
bound is `vsr_io_codec_message_region`: `sizeof(vsr_message)` plus the
larger of the PREPARE graph and the LOG_STATE graph, each entry costing
`sizeof(vsr_entry) + max(sizeof(vsr_blob) + sizeof(vsr_span),
sizeof(vsr_membership) + members * sizeof(vsr_member),
sizeof(vsr_check_epoch))`, aligned to 8. The aggregate blob bytes are
checked against `message_bytes` while decoding.

### 4.4 Stream frames

| Kind | Body |
| --- | --- |
| STREAM_REQUEST | `{u32 length, u32 reserved}` then `length` bytes (`<= VSR_IO_STREAM_REQUEST_BYTES`), pad to 8 |
| STREAM_CHUNK | `{u64 offset, u32 length, u32 reserved}` then `length` payload bytes (`<= stream_chunk_bytes`), pad to 8; the frame's body_crc covers them |
| STREAM_END | `{u64 bytes, i32 status, u32 reserved}` |

A chunk whose `offset` is not the stream's current offset ends the stream
with `FAILED`. The library request (56 bytes) is `{magic
VSR_IO_LIBRARY_MAGIC, u32 version 1, u32 kind CLIENTS=1, cluster hi/lo,
replica, snapshot hi/lo}`.

## 5. Store format

```
 file `log` in the replica's directory
 ┌──────────────┬──────────────┬─────────────────┬─────────────────┬─────┐
 │ superblock A │ superblock B │ slot 0          │ slot 1          │ ... │
 │ block 0      │ block 1      │ segment_bytes   │ segment_bytes   │     │
 └──────────────┴──────────────┴─────────────────┴─────────────────┴─────┘
 slot:    [header: header_blocks blocks][data: records and pads ...]
 write:   |record|record|record|PAD..|   whole blocks, from a block boundary
 record:  |hdr 48|change[count] 24 each|payload ... pad to 8|
```

### 5.1 Superblock (`vsr_io_wire_superblock`, 104 bytes in one block)

magic "SSB1", format, generation (random 64-bit from the executor at
creation), revision (write counter; the valid copy with the greater one
wins), cluster hi/lo, replica, durability, block_bytes, segment_bytes,
header_blocks, slots (allocated), start_segment and start_slot (oldest
live segment), run (opens of the store), durable_floor (acknowledged
durable sequence when written), crc (over everything before it). Written:
at creation (revision 1, both copies), at every open (run + 1, before the
first record write), when the start segment changes, when slots grow. A
copy is written to the block the previous write did not use.

### 5.2 Segment header (`vsr_io_wire_segment`, 80 bytes fixed part)

magic "SEG1", format, generation, segment (number from 1, monotonic, never
reused), length (meaningful bytes, CRC included), flags (STATE 1, ANCHOR
2), run, last_sequence (last record before this segment; 0 in a fresh
store), durable_floor, client_base, log_begin, log_end; then, when STATE
is set: `vsr_io_wire_identity` (32), `vsr_io_wire_hard_state` (32) and its
EPOCH, then CHECKPOINT when ANCHOR is set; then `u32 crc` over everything
before it. The header occupies `header_blocks = ceil(segment limit /
block_bytes)` blocks; bytes after the CRC are zero. The header is packed
into the ring and written as its own write; no record of the segment is
written before the header's write completed.

### 5.3 Record (`vsr_io_wire_record`, 48 bytes)

magic "REC1", length (total, multiple of 8), sequence, generation, flushed
(the durable sequence acknowledged to the core when the record was packed,
decision 50), count (descriptors), run, payload_crc (bytes 48..length),
header_crc (bytes 0..43). Then `count` descriptors `{u32 type, u32 count, u64 first, u32
offset, u32 length}` (offset from the record start), then payloads in
descriptor order:

| Change | Payload |
| --- | --- |
| IDENTITY | `vsr_io_wire_identity` |
| APPEND | `count` ENTRYs (as on the wire, no ENTRIES header) |
| TRUNCATE, TRIM | none |
| CLIENTS | `count` × (`vsr_io_wire_client_record{client hi/lo, number, op, i32 code, u32 length}` then `length` result bytes, pad to 8) |
| HARD_STATE | `vsr_io_wire_hard_state` EPOCH |
| PUBLISH_CHECKPOINT, RESTORE_CHECKPOINT | CHECKPOINT |

`vsr_io_codec_record_limit` is the largest record: 48 + 8 × 24 + (APPEND:
`batch_entries × (56 + max(15, 16 + 16 × members)) + message_bytes` padded, since every entry carries a blob header and a RECONFIGURE body holds a membership) + (CLIENTS: `batch_entries ×
(40 + pad(result_bytes))`) + (HARD_STATE: 32 + 16 + 2 × (16 + members ×
16)) + 2 × (PUBLISH/RESTORE: 32 + 16 + 2 × (16 + members × 16) + 8 +
pad(manifest_bytes)) + 32, with checked arithmetic. A transaction larger
than this is impossible for a core within its limits; a STORE that
nevertheless is fails with `FAILED`.

PAD: `{magic "PAD1", u32 length}` runs to the block end; fewer than 8
bytes left in a block are zero and skipped implicitly.

### 5.4 Clients file (`clients-<32 hex>`)

Header (64 bytes): magic "CLT1", format, cluster hi/lo, snapshot hi/lo, op,
sequence (the writer's), count, crc (over the preceding bytes). Records:
`vsr_io_wire_client_record`, result bytes padded to 8, then `u32 crc` of
the record and bytes. Trailer (8 bytes): magic, count. A reader verifies
every CRC and both counts; anything else is `CORRUPT`. Files are written
once, sequentially, and never modified; the fetch path writes
`clients-<id>.tmp` and renames.

## 6. Store internals

### 6.1 Tail ring and extents

The ring is `cache_bytes` (a multiple of `block_bytes`), addressed by
unwrapped offsets (`head` grows forever; the byte is at `head % size`).
Packing writes at `head`: a segment header (aligned to a block, `header_bytes`
long) or a record. Before packing a record of `n` bytes:

1. If the current segment's remaining data bytes are fewer than `n`: seal
   the segment (the current write, if any, is closed and padded), allocate
   the next segment (free slot, else grow by one `FALLOCATE` when `slots <
   max_segments`, else the STORE is held until a slot frees; a store that
   cannot free because every floor is high fails with `FAILED`), align
   `head` to a block, pack its header (new extent), plan its write, and
   mark it HEADER; records for it are packed but their write is planned
   only after the header's write completes.
2. If the ring's remaining bytes before the wrap are fewer than `n`: close
   the current write (pad), move `head` to the next ring start (a new
   extent begins; the file offset continues at the next block boundary,
   and the skipped ring bytes are not mirrored).
3. If `head + n` would pass `pin_floor + size`, where `pin_floor` is the
   minimum of every active ring pin's `begin` and the `ring_begin` of the
   oldest write not yet complete (or `head` when none): hold the STORE
   (queue it; nothing after it is processed until it proceeds, since ops
   are applied in order). The attach rule guarantees the hold ends:
   pinned LOAD bytes are bounded by `pinned_payload_bytes` plus two record
   alignments, unwritten bytes by `write_behind_bytes`.
4. Likewise hold while `head - issued_complete > write_behind_bytes`.
5. Copy the record (`vsr_io_codec_put_record`), advance `head`, extend the
   current extent, index it, queue the completion.

Extents map ring ranges to file ranges; the table is a FIFO whose oldest
entries are dropped as the ring reuses their bytes; `retained_floor` is
the oldest mirrored unwrapped byte. `vsr_io_store_hot(offset, length)`
finds the extent containing the file range and returns a piece into the
ring when every byte is still mirrored (`>= retained_floor`).

### 6.2 Write pipeline

`vsr_io_store_prepare` plans at most one record write per call per
replica: the range `[issued, head)` rounded up to a block, padded with a
PAD in the ring (the pad is mirrored, so the file and the ring agree),
split into one record per contiguous (ring, file) range (a wrap or a
segment boundary splits it; each part is `WRITE | FIXED_FILE |
FIXED_BUFFER` on the tail region), provided a write entry is free
(`inflight_writes`). `issued` advances to the padded end; the next write
starts there, so no block is rewritten. Superblock writes use the two
superblock blocks after the ring with their own slot kind and are never
concurrent with each other. Completion: a write's `state` becomes complete;
`written` advances over the contiguous prefix of complete writes in issue
order to the `last_sequence` of the last one; a negative result sets
`error` and applies `on_write_error` (FENCE: state FAILED, every queued op
completes `FAILED`; CONTINUE, replicated only: the store stops writing and
serves from memory).

Flush: `flush_pending` is set by a SYNC (after `sync_delay_ns` from the
first SYNC of a batch), by the flush interval in replicated mode when
`written > flushed`, and by SNAPSHOT_SYNC for its own file. In `DSYNC`
mode writes carry `O_DSYNC` (the file is opened with it) and a SYNC
completes when `written >= sequence`; in `FDATASYNC` mode a `FSYNC |
DATASYNC` is issued once `written >= flush_target` and its completion sets
`flushed = written at issue`, completing every SYNC with `sequence <=
flushed`. `durable` is the largest sequence acknowledged to the core.

Idle floor (decision 50): when `durable` passes `superblock_floor` and no
record is packed within `flush_interval_ns` (100 ms when zero), a
superblock write carrying `durable_floor = durable` is planned; a record
packed meanwhile carries the floor in its `flushed` field instead and the
write is skipped.

### 6.3 Indexes

Op ring `ops[op % max_entries]` holds the current version of every op in
`[retained_begin, log_end)`; `retained_begin = min(log_begin, log_begin as
of the RECLAIM revision)` from the `trims` history, so an unreclaimed
revision can still name its entries. Each change type:

| Change | Index effect |
| --- | --- |
| APPEND | for each entry: ring slot set (a nonempty slot with a different op means the ring is full: `FAILED`); `previous = clients[c].retained`, `clients[c].retained = op`, creating the entry if absent (counts against `max_clients`; over: `FAILED`) |
| TRUNCATE | for op from `log_end - 1` down to `first`: move the version to `versions` with `truncated = sequence` (table full: `FAILED`), clear the slot, and if `clients[c].retained == op` set it to `previous` when `previous >= log_begin`, else 0; `log_end = first` |
| CLIENTS | for each record: entry `current` replaced when `number` is greater; equal number with different op or result is `CORRUPT`; lower is ignored; `clients_sequence = sequence` |
| HARD_STATE | copied into `hard` and `state_region` |
| PUBLISH | `anchor` replaced; if `id == last_capture` then `base_set(id, last_capture_sequence)`; else if `id` differs from the current base and the role is FULL, the transaction waits (`base_wanted`) for `vsr_io_snapshots_load_base` |
| RESTORE | entries `<= op` dropped from the ring as by TRIM to `op + 1`; `anchor` replaced; the table is rebuilt: the transaction waits for the base load of `id` at this sequence (WITNESS: emptied at once), then every client's `retained` is recomputed by walking `[log_begin, log_end)` |
| TRIM | for op from `log_begin` to `first - 1`: if `clients[c].retained == op` clear it, and delete the client entry when it has no record and is not in flight; `log_begin = first`; a `trims` entry `(sequence, first)` is recorded (history full: `retained_begin` simply does not advance until it drains, which is safe) |
| IDENTITY | `identity` set; only at sequence 1 |
| RECLAIM(oldest) | `reclaim = oldest`; `retained_begin` = `log_begin` as of `oldest - 1` from `trims`; versions with `truncated <= oldest` deleted; then `vsr_io_store_free_segments` |

LOAD at sequence `s` (the core always names its `stored_sequence`; the
engine routes every LOAD of an update before its STOREs, so normally
`readable == s`, decision 51):

- RECOVERY: only from `open`; answered by recovery.
- LOG `[first, end)`: for `op = first` while `op < end`, count and bytes
  allow: the version is `ops[op]` when `ops[op].op == op` and `sequence <=
  s`, else the entry of `versions` with `appended <= s < truncated`, else
  stop. Consecutive versions in one record are read together. Hot
  (`vsr_io_store_hot`): the region gets `vsr_entry`s with spans into the
  ring, one pin covering the records. Cold: one `READ | FIXED_FILE |
  FIXED_BUFFER` of the block-aligned range covering the records (bounded
  by the slab; the batch is cut where it would exceed the slab or leave
  the segment) into a slab; on completion the records' CRCs are checked
  (`CORRUPT` on failure), entries decoded with spans into the slab. `next
  = first + count`, `next = end` when the range is complete; an empty
  result for a nonempty range is `NOT_FOUND`.
- CLIENT: the entry's `current` when `current.sequence <= s`, else
  `RETRY` (decision 51: the core resets the route and reloads at its newer
  stored sequence, without fencing); no entry or `number == 0` is count 0. The record is read hot or cold
  from the log when `sequence > client_base`, else from the base file at
  `base_offset` (a cold read through the clients-file slot).
- REQUEST: `retained` when the version's `sequence <= s`, else walk
  `previous` links; the entry is decoded from its record like LOG.

`vsr_io_store_admit` looks the incarnation up; unknown and `clients_count
== max_clients` returns false; unknown otherwise inserts an entry with
`inflight` set. `vsr_io_store_replied` clears `inflight` and deletes an
entry with nothing else set. The engine calls `replied` when it forwards a
REPLY whose status is not OK and the entry has no record and no retained
entry; an OK reply follows a CLIENTS record, which cleared `inflight` at
indexing (APPEND clears it too).

### 6.4 Recovery

`vsr_io_store_open` runs these steps, each an executor record whose
completion advances `recovery.stage`:

1. `OPENAT` the directory (kept in a slot for fsync) and `log`
   (`O_RDWR | O_DIRECT` when `direct_io`, `O_DSYNC` in DSYNC mode, DIRECT
   into an engine slot). `ENOENT`: NEW/JOIN create (`OPENAT` with
   `O_CREAT | O_EXCL`, `FALLOCATE` of `2 * block_bytes + segments *
   segment_bytes`, superblocks with `generation` from `random`, `run = 1`,
   `start_segment = 1`, `slots = segments`, then the first segment's
   header with STATE clear; the RECOVERY load completes `NOT_FOUND` once
   the header write completed, which is what the core expects for an empty
   store); RECOVER completes `NOT_FOUND` at once. An existing file under
   NEW/JOIN is recovered like RECOVER; the core rejects the recovered row
   itself.
2. `STATX` for the size; `READ` both superblocks; keep the valid one with
   the greater revision; none valid is `CORRUPT`. Geometry must match the
   options (`block_bytes`, `segment_bytes`, `header_blocks`) and identity
   the replica's, else `CORRUPT` (identity mismatch is left to the core:
   the row is returned and the core fences with `IDENTITY`).
3. For every slot, `READ` its header blocks; validate magic, format,
   generation and CRC; a slot with a valid header gets its `number`,
   `last_sequence` and `run` into the segment table; anything else is a
   free slot. The start segment must be present in `start_slot`, else
   `CORRUPT`.
4. Load the start segment's state into `identity`, `hard`, `anchor`,
   `log_begin`, `log_end`, `client_base`; `sequence = last_sequence`;
   `F = max(superblock.durable_floor, header.durable_floor)`, raised by
   every later segment header's floor and every valid record's `flushed`
   as the scan proceeds (decision 50).
5. Scan the segment's data in slab-sized `READ`s: for each record header
   (`vsr_io_codec_get_record`): PAD skips; END or a bad header, wrong
   generation, `sequence != last + 1` or `run < last run` ends the scan;
   a header valid but payload CRC bad ends it too. Before a range is
   judged bad it is read once more (`retried`): a transient bit flip or
   read error passes on the re-read and only a second failure ends the
   scan. A record that ends the scan with `sequence <= F` is `CORRUPT`;
   above `F` it is the torn tail. A record that
   straddles the chunk end is re-read from its start (a record fits a
   slab). Each valid record is applied to the indexes exactly as a STORE
   is (section 6.3), through the same code.
   CLIENTS records with `sequence <= client_base` still update
   `current`; the base file load in step 7 then re-points entries at the
   file, which is consistent because every record after the base is in
   the scan.
6. At the segment's end (a record does not fit, or data exhausted), pick
   the successor (decision 48): among headers with `last_sequence ==
   sequence` and `run >= last run`, the greatest `number`; none ends the
   scan. Headers with `last_sequence < sequence` are stale: their slots are
   freed. Repeat from step 5.
7. If `hard.role == FULL` and `anchor.id` is nonzero, load
   `clients-<anchor id>` as the base at `client_base` (a missing file is
   `CORRUPT`); WITNESS skips it. Then walk `[log_begin, log_end)` to set
   every client's `retained`.
8. Rewrite the superblock with `run + 1` and the current start segment
   (the oldest slot whose `last_sequence >=` the freeing floor, computed
   with the reclaim floor at 0), wait for its completion, set `head` to
   the block after the last valid record (the ring starts empty; the
   current segment is the one holding that record, OPEN), and complete the
   RECOVERY load with `vsr_recovered{identity, sequence, log_begin,
   log_end, hard, anchor or NULL}` in a load region.

Torn-tail cases the tests synthesize: a lost last block; a lost middle
block of a multi-block write with the later block persisted; a persisted
block from a torn write behind a block rewritten by a later run (rejected
by the run rule); a stale header of a segment whose records were torn; a
bad record at or below the durable floor (`CORRUPT`), including a floor
that only a later valid record's `flushed` carries; a bad record above the
floor followed by a valid later record (still the torn tail); a block that
reads bad once and clean on the re-read (passes); both superblocks
valid with different revisions; one superblock corrupt; the start slot
holding another segment (`CORRUPT`).

### 6.5 Freeing

`vsr_io_store_free_floor` = min(`ops[retained_begin].sequence` (or
`readable + 1` when the log is empty), `reclaim`, `client_base + 1`,
`capture_floor`). A SEALED slot whose `last_sequence < floor` is freed;
when the start slot is freed the new start is the live slot with the
smallest number and a superblock write is planned; freed slots are not
reused until that write completes (a superblock always names a present
segment). Growth also rewrites the superblock after the `FALLOCATE`
completes.

## 7. Engine internals

### 7.1 vsr_io_poll

```
 vsr_io_poll(io, now, ops, capacity, count, flags)
   io->now = now; wake_pending = 0
   while deadlines_pop(now, kind, index): dispatch (LINK/DIAL -> links,
       FLUSH/SYNC -> store, STREAM -> streams, CAPTURE -> snapshots,
       CORE -> nothing: TIME below handles it)
   links_poll(now); streams_poll(now)
   for each replica in OPENING or RUNNING:
       do {
           n = 0
           events[n++] = every queued internal COMPLETE, in queue order
                         (store completions, SEND completions, snapshot
                         completions, LOAD results with their leases)
           events[n++] = every caller COMPLETE and STOP, in order
           events[n++] = every MESSAGE in the messages ring, in order
           events[n++] = every other caller event, in submission order
           events[n++] = TIME(now)
           r = vsr_step_many(core, events, n, &update{step_ops})
           consume the accepted prefix: internal completions are dropped,
             caller events dequeued, MESSAGE leases go from queued to
             leased; unconsumed caller events and messages stay (TIME is
             always rebuilt); EINVAL on a caller event: the event is
             dequeued and dropped with its lease released as a RELEASE op
             would (the caller's contract was broken; counted in stats)
           route every update.ops[i] (7.3)
           if update.flags & STATE_CHANGED: status_pending = 1
           deadlines_arm(deadline_core, update.deadline_ns)
       } while ((update.flags & MORE) || new internal completions were
                queued by routing) && forwarded ring not full
       store_poll(replica, now); snapshots_poll(replica, now)
       if status_pending: forward STATUS (copy vsr_get_status into
           replica->status); if state is STOPPED: replica state STOPPED
   copy the forwarded ring into ops[0..capacity); *count; *flags = MORE
       when the ring is not empty or a replica loop stopped on a full
       ring, OUTPUT_FULL when ops filled and the ring is not empty
```

Event order per step, the one order used everywhere: internal
completions; the caller's COMPLETE and STOP events; MESSAGEs; the caller's
other events (REQUEST, CLIENT_QUERY, READ, CHECKPOINT) in submission order;
TIME last. Completions and STOP come first because the core reserves
capacity for them and they release resources (vsr.h: they take priority in
a driver's queue). TIME is last so that a message received in this batch,
which may reset a timer (a heartbeat from the primary, for example), is
seen before the time that would otherwise expire it. The caller's COMPLETE
and STOP are taken out of the caller ring in order when the array is
built; the remaining caller events keep their relative order. The core
coalesces across the whole array in one `vsr_step_many`.

`work_per_step` bounds a step; the loop drains MORE, so a poll performs
bounded work only because the forwarded ring (`limits.ops`) and the
completion queues are bounded: when the ring fills the loop stops and the
poll reports MORE, and the caller polls again after stepping.

### 7.2 vsr_io_submit

Events are taken in order; the first refusal stops with `*consumed` and
the reason:

| Kind / type | Action |
| --- | --- |
| CORE TIME, MESSAGE | `EINVAL` |
| CORE REQUEST | `vsr_io_store_admit(client)`; false: stop with `ELIMIT`; else queue (ring full: stop with `AGAIN`) |
| CORE COMPLETE for an op the snapshot module forwarded | `vsr_io_snapshots_forwarded_done`; the event is consumed, the lease (if any) released immediately after the data is copied |
| CORE COMPLETE, STOP, CLIENT_QUERY, READ, CHECKPOINT | queue |
| COMPLETE (rail) | HANDSHAKE -> `links_handshake_done`; STREAM_SERVE -> `streams_served`; STREAM_DATA -> `streams_data_done`; unknown id: `EINVAL` |
| STREAM_OPEN / WRITE / CLOSE | `streams_open` / `write` / `close`; `ELIMIT` from open stops |

A replica in STOPPED refuses every CORE event with `EINVAL`.

### 7.3 Op routing

For each op the core emits, by `op.type`; within one update every LOAD is
routed first, then every STORE, then SYNC and RECLAIM, then the other ops
in emission order (decision 51):

| Op | Route |
| --- | --- |
| SEND | `vsr_io_links_send(replica, id, message, arg)`; `RETRY` returned -> COMPLETE(RETRY) queued |
| LOAD, STORE, SYNC, RECLAIM | `vsr_io_store_*`; completions arrive through `next_completion` |
| SNAPSHOT_CAPTURE, FETCH, SYNC, DROP | `vsr_io_snapshots_*`; a status returned -> COMPLETE queued |
| RELEASE with `VSR_IO_LEASE_ENGINE` | `vsr_io_lease_resolve`; release the region and slab/pin |
| REPLY | forwarded (CORE kind); if status != OK and the client has no record and no retained entry: `vsr_io_store_replied` |
| APPLY, READ_READY, SNAPSHOT_INSTALL, RELEASE of caller leases | forwarded verbatim |

Forwarded CORE ops keep the core's `vsr_op` verbatim; the pin rules of
`vsr.h` hold because the engine never copies a forwarded op's data.

### 7.4 vsr_io_prepare

In order, each stopping when `capacity` or a table is exhausted (leftovers
stay queued and the engine sets `*deadline_ns = now_ns` so the loop
returns immediately and prepares again):

1. Pool provision: `provide()` executor call for the slabs
   `vsr_io_pool_provide` returns (not a record).
2. `vsr_io_links_prepare`: listener setup on the first call (SOCKET, BIND,
   LISTEN, then multishot ACCEPT with DIRECT, one chain per listen address,
   LINKed), connects, HELLO sends, coalesced sends, receive re-arms,
   shutdowns and closes.
3. `vsr_io_streams_prepare`: file chunk reads.
4. Per replica: `vsr_io_store_prepare` (open/create steps, header writes,
   one record write, superblock write, flush, one cold read), then
   `vsr_io_snapshots_prepare` (clients file reads and writes, fsyncs,
   renames, unlinks).
5. `*deadline_ns = vsr_io_deadlines_earliest()`.

### 7.5 vsr_io_complete

For each record: `vsr_io_slots_resolve` (foreign owner tag or stale
generation: dropped, `stats.frames_rejected` untouched, a counter in the
slot table); then by `slot.kind`: LISTEN, CONNECT, RECV, SEND, SHUTDOWN ->
`vsr_io_links_complete`; WRITE, FLUSH, SUPER, LOAD, FILE (owner replica)
-> `vsr_io_store_complete`; CLIENTS, FILE (owner snapshot) ->
`vsr_io_snapshots_complete`; STREAM -> `vsr_io_streams_complete`. Nothing
steps a core here; every effect is queued for the next poll.

### 7.6 Leases

`vsr_io_lease_alloc` takes a free lease entry (and thus a region), records
the slab or pin, bumps the generation, and returns the index; the id is
built by `vsr_io_lease_id`. A MESSAGE event's lease is allocated at
delivery with one slab reference; a LOAD completion's lease at completion
with a slab reference (cold) or a ring pin (hot). RELEASE frees the region,
drops the reference or pin, and marks the entry free; a stale id is
`EINVAL` in the routing, reported through `stats` and ignored.

### 7.7 Replica attach, start, stop, detach

`vsr_io_attach`: validate (`vsr_io_store_check`, frame limit against
`slab_bytes`, regions against the layout), `vsr_init` the core in the
metadata region, initialize the store, snapshots, queues and leases, bind
deadline handles, open the directory (a `FILE` record on the first
prepare), state OPENING. The first poll steps the core with only TIME; its
RECOVERY LOAD routes to `vsr_io_store_open(start_mode, op)`. The load
completes with the recovered row or `NOT_FOUND` (empty store, as NEW and
JOIN expect) or `CORRUPT`; the core proceeds, and the replica is RUNNING
from the first `STATUS`. Peers are dialed when the first SEND names them.

Stop: the caller submits STOP; the core drains; `STATUS` with STOPPED
marks the replica STOPPED; queued sends for it complete `CANCELLED` at
that point (the core has already consumed their completions or will
consume them as CANCELLED). `vsr_io_detach`: `EBUSY` unless STOPPED and
`vsr_io_store_close` and `vsr_io_snapshots_close` both return OK (no write,
flush or file operation in flight); then `update_file(slot, -1)` for the
log, directory and any clients file, `update_buffer(index, NULL)` for the
tail, and the replica entry is freed. The core is left in the caller's
region for `vsr_deinit`.

`vsr_io_close`: state closing; `vsr_io_links_shutdown` (CANCEL of every
multishot accept and receive, SHUTDOWN and CLOSE of every link),
`vsr_io_streams_shutdown` (END `CANCELLED` to every stream); `closed` when
every slot is free and every link FREE; `vsr_io_deinit` then unregisters
the pool region and the buffer ring.

### 7.8 STATUS and stats

STATUS is forwarded once per poll in which `STATE_CHANGED` was reported,
and once when the core reaches STOPPED, with `op.data` pointing at
`replica->status`, valid until the next poll. `vsr_io_get_stats` reads
counters the modules maintain directly in `io->stats`.

## 8. Executor contract

Both executors implement exactly this; `tests/integration/executor_conformance`
runs one test body over both, byte-identical, from a table of
`vsr_io_executor` factories. Every result is the operation's result or a
negative errno. Completions of independent records may be reaped in any
order. `user_data` is returned unchanged. Unless stated, a record produces
exactly one completion.

Common flags:

| Flag | Meaning |
| --- | --- |
| `LINK` | The next record in the same `submit_and_wait` batch runs after this one completes successfully; success for READ, READV, WRITE, WRITEV, SEND and a non-multishot RECV means a non-negative result equal to the requested length, for every other opcode a non-negative result. On failure every remaining record of the chain completes with `-ECANCELED`. A chain never spans batches |
| `FIXED_FILE` | `fd` is a slot of the registered file table; an empty slot completes `-EBADF` |
| `FIXED_BUFFER` | Memory is inside registered region `buffer_index`: `addr`..`addr + length` for READ/WRITE/RECV/SEND, every vector for READV/WRITEV/vectored SEND. Outside: `-EFAULT`; unregistered index: `-EFAULT` |
| `BUFFER_SELECT` | RECV takes buffers from ring `buffer_group` |
| `SKIP_SUCCESS` | No completion is produced when the result is non-negative; a failure still completes. Not allowed on multishot or zero-copy records (`-EINVAL`) |
| `DIRECT` | The resulting descriptor goes to slot `fd2`, or a free slot when `fd2 == VSR_IO_SLOT_ALLOC`; the result is the slot index; no free slot: `-ENFILE` |

Opcodes:

| Opcode | Semantics |
| --- | --- |
| NOP | result 0 |
| READ, WRITE | `length` bytes at `offset` of `fd` from/to `addr`; result is the byte count, possibly short; `O_DIRECT` files require `offset` and `length` aligned to the logical block (512 in the ring, `options.block_bytes` in the sim) else `-EINVAL`, and `addr` to the device's DMA alignment, which the sim does not check (decision 65); past-end reads are short or 0 |
| READV, WRITEV | `addr` is `struct vsr_io_vec[length]`; same results |
| FSYNC | `DATASYNC` selects fdatasync; result 0; the sim marks every block written before the record's submission as synced |
| FALLOCATE | zero-filled preallocation of `[offset, offset + length)`; `op_flags` mode 0 only; `-ENOSPC` when the sim's capacity is exceeded |
| OPENAT | `fd` is a directory descriptor (`AT_FDCWD` / `VSR_SIM_ROOT` for the node root), `addr` a NUL-terminated path, `op_flags` `O_*` (`O_CLOEXEC` is dropped under `DIRECT`, decision 68), `length` the mode; result the descriptor or slot; `-ENOENT`, `-EEXIST` (with `O_EXCL`) as libc |
| CLOSE | `fd`, or the slot with `FIXED_FILE` which then becomes empty; result 0. Never completes the records pending on the descriptor: they keep the underlying socket or file alive until they complete or are cancelled (the engine cancels, decision 49), and a socket's FIN goes out only then; a receive parked on a closed descriptor still delivers what the peer sends (decision 58) |
| RENAMEAT | `addr` -> `addr2`, both relative to `fd`; atomic replace; result 0 |
| UNLINKAT | removes the name; open descriptors keep the data; result 0 or `-ENOENT` |
| MKDIRAT | result 0 or `-EEXIST` |
| STATX | fills `addr2` (`struct statx`); the sim fills `stx_size`, `stx_blksize` and `stx_mode` only; result 0 |
| SOCKET | `length` domain, `op_flags` type, `offset` protocol; `AF_INET`, `AF_INET6` (ring only), `AF_UNIX`; result the descriptor or slot |
| CONNECT | to `addr` of `length` bytes; result 0, or `-ECONNREFUSED` (no listener), `-EHOSTUNREACH` (partitioned, after `connect_timeout_ns`), `-EAFNOSUPPORT` |
| BIND | result 0 or `-EADDRINUSE` |
| LISTEN | `length` backlog; result 0 |
| ACCEPT | with `MULTISHOT`: a completion per accepted connection with `MORE` set, result the descriptor or slot (with `DIRECT`); terminates with `MORE` clear on a negative result: `-ECANCELED` (cancelled), `-ENFILE` (no free slot with `DIRECT`: that connection was accepted and is closed again, so its peer sees a reset; later connections stay queued, decision 65). Closing the listener's descriptor does not terminate it: the listener lives on until the record is cancelled (decision 58). A full completion queue at a delivery also ends it, with that connection's result and `MORE` clear (ring only, decision 68). Without `MULTISHOT`: one accept |
| RECV | Without `BUFFER_SELECT`: into `addr`/`length`; result bytes, 0 at end of stream, `-ECONNRESET` on reset; `PEEK` returns bytes without consuming them. With `BUFFER_SELECT`: buffers are taken from ring `buffer_group` in the order they were provided; `BUFFER` set and `buffer_id` names the buffer; bytes land at the buffer's base plus the bytes previously delivered from it (INCREMENTAL rings) or at its base (others, which consume the whole buffer per completion); `BUFFER_MORE` set means the buffer remains at the head of the ring with its remaining space and the next receive continues in it; clear means the buffer left the ring. With `MULTISHOT`: a completion per delivery with `MORE` set; terminates (`MORE` clear) with `-ENOBUFS` when the ring is empty or has been unregistered at a delivery (never at arming: the ring issues these receives poll-first and the sim parks the record until bytes arrive, decisions 58 and 65), with 0 at end of stream, with a negative result on error or cancel, or (ring only) with that delivery's positive result when the completion queue is full at a delivery (decision 68); after termination the record must be resubmitted. A buffer never appears in two receives at once |
| SEND | `addr`/`length`, or with `VECTORED` `addr` is `vsr_io_vec[length]`; result bytes sent, possibly short (the caller resubmits the rest); `-EPIPE`/`-ECONNRESET` on a closed peer. With `ZERO_COPY`: two completions with the same `user_data`: first the result with `MORE` set, then a completion with `NOTIF` set and result 0 once the kernel no longer reads the buffers; both always arrive, even after a failed send. The bytes must stay unchanged until the NOTIF. Two sends on one socket may complete in either order; ordering requires waiting for the first result. A zero-copy record the executor rejects at translation (`SKIP_SUCCESS` set, an out-of-region `FIXED_BUFFER`, an unsupported opcode) completes exactly once with the error and `MORE` clear, so a first completion without `MORE` means no `NOTIF` follows; the result completion always precedes the `NOTIF` (decision 62) |
| SHUTDOWN | `length` `SHUT_*`; result 0; the peer's receive returns 0 after the queued bytes |
| SETSOCKOPT, GETSOCKOPT | `op_flags = level << 16 \| name`; result 0 (get: the value length); GETSOCKOPT serves level `SOL_SOCKET` only and any other level completes `-EOPNOTSUPP` on both executors, since the kernel's socket command serves no other level; the sim accepts `TCP_NODELAY` and `SO_KEEPALIVE` only (decision 65) |
| TIMEOUT | fires at `offset` ns (absolute in the executor clock with `ABSOLUTE`, else relative to submission) with `-ETIME`; `-ECANCELED` when cancelled; the sim adds the clock jitter fault |
| TIMEOUT_UPDATE | `addr2` points at the target `user_data` (u64), `offset` the new expiry; result 0 or `-ENOENT` |
| CANCEL | targets `offset` as `user_data`, or every record on `fd` with `BY_FD` (the records holding the object `fd` names now, as io_uring compares files; `-EBADF` when `fd` is closed); `ALL` cancels every match, else one match, which one is unspecified (decision 65); result the count with `ALL`, 0 when one was cancelled, `-ENOENT` when none, `-EALREADY` when the target is already completing. Cancelled records complete with `-ECANCELED` (multishot ones terminate) |

Registration (all synchronous, all executor-wide):

| Call | Semantics |
| --- | --- |
| `register_files(slots)` | Sparse table of `slots` empty entries; once per executor, a second call is `-EBUSY` |
| `update_file(slot, fd)` | Installs `fd` (the executor owns it; `CLOSE` with `FIXED_FILE` or `update_file(slot, -1)` closes it, though records pending on it keep the object as under CLOSE); `-EBADF` for a bad `fd`, `-EINVAL` for a slot outside the table or before registration (decisions 58 and 65) |
| `register_buffers(regions)` | Sparse table; once, a second call is `-EBUSY` |
| `update_buffer(index, region)` | Installs or clears (`NULL`) a region; `-EINVAL` for an index outside the table or before registration; replacing a region a pending record uses is a caller error: the sim reports `-EBUSY`, the ring returns 0 and the kernel keeps the old registration alive until those records complete (decision 65) |
| `buffer_ring(group, entries, flags, memory)` | Registers ring `group` with `entries` (power of two) over `memory` (page-aligned, `16 * entries` bytes, the caller's until the ring is unregistered by passing `entries == 0`); `INCREMENTAL` selects incremental consumption; a group already registered: `-EEXIST`; unregistering an unknown group: `-ENOENT`. Unregistering with receives pending on the group is allowed: the buffers are the caller's again at once and each pending receive terminates with `-ENOBUFS` at its next delivery (decisions 58 and 65) |
| `provide(group, buffers, count)` | Appends buffers to the ring tail; a full ring: `-ENOSPC`; an unknown group: `-ENOENT`; ids are the caller's, 16-bit |
| `submit_and_wait(sqes, count, want, min_wait_ns, deadline_ns)` | Submits all `count` records (a full submission queue is drained internally), then waits per the header; result 0 on return by completions, deadline or wake (a timeout is a normal return, never `-ETIME`), or a fatal negative errno from the ring; a record whose `user_data` is `UINT64_MAX` (reserved) is refused with `-EINVAL` before anything is submitted, as is `want` above the CQ size (decision 68) |
| `reap(cqes, capacity)` | Moves up to `capacity` completions; never blocks |
| `now()` | Monotonic nanoseconds; the sim's node clock |
| `random(bytes, size)` | Fills bytes |
| `wake()` | Thread-safe; a blocked or the next `submit_and_wait` returns; idempotent |

The conformance suite: for each executor, a scenario per row above
(including every termination case of multishot records, incremental
consumption with buffers smaller than one delivery, `-ENOBUFS` and
re-arm, both zero-copy completions with and without `FIXED_BUFFER`, short
sends into a full socket buffer, LINK success and failure, CANCEL of each
kind, DIRECT slot allocation and exhaustion, `O_DIRECT` alignment
rejection, `FIXED_BUFFER` rejection, rename atomicity, timeouts absolute
and relative and their update and cancel, and wake from another thread
for the ring, from the same thread for the sim). The ring version uses
loopback sockets and a temporary directory; the sim version the same code
with node 0 talking to node 1.

## 9. Testing plan

| Test | Layer | Target in `Makefile.am` | Checks |
| --- | --- | --- | --- |
| `cursor`, `codec`, `pool`, `slots`, `deadline`, `link`, `stream`, `store`, `snapshot`, `sim_world`, `client`, `uring_translate` | unit | `UNIT_TESTS`, each `tests_unit_NAME_SOURCES = tests/unit/NAME.c`, `_LDADD = $(LIBVSR)` | Section 3 |
| `frame`, `recovery` | fuzzy (libFuzzer) | `if FUZZING` programs and the `fuzz` target, with corpora under `tests/fuzzy/corpus/frame` and `corpus/recovery` | Decoder and recovery never crash; recovered prefixes satisfy the invariants |
| `executor_conformance` | integration | `INTEGRATION_TESTS`; runs over the sim and, when `/dev/null` is writable and a ring can be created, over io_uring (skipped with exit 77 otherwise), then over both again through the fault-injecting wrapper | Section 8 |
| `engine` | integration | `INTEGRATION_TESTS` | Over the sim: attach NEW, RECOVER, JOIN; empty-store checks; a three-replica group commits, replies, checkpoints, fetches, restarts; STATUS emission; close and detach sequencing; max_clients admission at the primary |
| `streams`, `snapshots` | integration | `INTEGRATION_TESTS` | Section 3 |
| `iocluster`, `iocluster_extended` | fuzzy (seeded) | `FUZZY_TESTS`; `SEED COUNT STEPS [trace\|quiet] [PROFILE] [SEEDS]` as `tests/fuzzy/cluster`; the extended program sets a wider default profile | Below |
| `uring_faults` | integration | `INTEGRATION_TESTS` (skips without a ring) | A three-replica group over real rings under the fault-injecting wrapper executor: transient `-EIO` on writes, short sends, delayed NOTIFs, cancelled receives; recovery after a crash simulated by `vsr_io_deinit` without flush |

`tests/fuzzy/iocluster` composes real engines over one simulated world:
N nodes (3 to 5 replicas, plus 0 to 2 learners with flag 32), each an
engine with one replica running the examples' key-value application
(`examples/kv.c`, driven through the forwarded ops instead of
`tests/lib/memory_cluster`), and M client nodes built on `vsr-client.h`
submitting through the node's local API with simulated latency. Fault
profiles reuse the flag vocabulary of `tests/README.md` where the meaning
carries over (1 pre-committed history, 2 minimum budgets, 4 partitions, 8
harsh crashes with torn writes at block granularity, 16 disk faults, 32
membership, 64 reads) and add 128 (network faults: drop, corrupt, reset,
split, delay) and 256 (client crashes with and without persisted state).
Oracles, checked after every harness action: exactly-once per client lane
(every EXECUTED reply's `op` is unique per lane and the KV state shows
each command once); linearizability of the client-observed history
(a checker over the recorded invocations and responses of all lanes,
with the KV model); every replica's KV state identical at equal applied
positions; after a crash with torn writes, every transaction acknowledged
durable to the core before the crash is recovered (the harness records
each SYNC completion the engine delivered); after faults stop, every
member converges (as the core scheduler defines liveness). Each run
prints its seed and configuration; `trace` prints every harness action,
delivery and completion with node indices and virtual times.

The fault-injecting wrapper executor (`tests/lib/faulty_executor.c`)
wraps any `vsr_io_executor`: it forwards records to the inner executor
and, from a seeded generator, rewrites results (`-EIO`, short counts),
delays completions by holding them for a number of reaps, and cancels
receives; it never alters bytes, so it is safe over real files and
sockets.

`make check` grows by the programs above; `make check-unit`,
`check-integration` and `check-fuzzy` select layers as before; `make fuzz`
adds the two harnesses; CI's ring-dependent tests skip on runners without
io_uring.

## 10. Contract changes

Every public-header edit made for this plan, mirrored in the decision log
of `docs/io-design.md`:

| Header | Change | Decision |
| --- | --- | --- |
| `vsr-io.h` | `VSR_IO_SQE_PIPE` and `VSR_IO_SQE_SPLICE` removed; stream FILE writes are read into slabs; `file_slots` sizing counts one slot per stream instead of two pipe ends | 39 |
| `vsr-io.h` | Sizing rules rewritten: `slab_bytes` also covers the largest record plus two blocks; minimum `slabs`; `link_queue` is per node | 37, 38, 42 |
| `vsr-io.h` | `VSR_IO_LIBRARY_MAGIC` reserved prefix of stream requests; joint snapshot op semantics (engine-generated id in the forwarded CAPTURE template, FETCH after the clients half, SYNC and DROP both halves) | 43 |
| `vsr-io.h` | Exact send flag rule in the payload pool section | 38 |
| `vsr-io.h` | Engine arms no timer; `vsr_io_prepare`'s deadline must reach `submit_and_wait` | 40 |
| `vsr-io.h` | Several links per node; carrier rule | 41 |
| `vsr-io.h` | Listeners set up asynchronously; bind/listen failure is fatal through `vsr_io_stats.failure`; `vsr_io_init` no longer blocks on the ring | 45 |
| `vsr-io.h` | `VSR_IO_CQE_MORE` documented on the first completion of a zero-copy send; executor semantics referenced to this document; uring options comment corrected (no `buffer_ring_memory` field) | 45 |
| `vsr-io.h` | Store section: never-rewrite rule, held-STORE conditions (pinned floor, write-behind, RESTORE/PUBLISH base load), index overflow fails with FAILED; `cache_bytes` rule | 33, 36, 43 |
| `vsr-io.h` | `vsr_io_detach` is EBUSY while write-behind writes or flushes are in flight | 49 |
| `vsr-io.h` | Store section: the durable prefix below which a bad record is CORRUPT is the greatest durable sequence any persisted record, segment header or superblock carries; idle superblock write | 50 |
| `vsr-io.h` | `vsr_io_limits.caller_slabs`, the caller's share of the pool; the minimum-slabs rule adds it; `vsr_io_slab_acquire` is ELIMIT once the caller holds the share or the free slabs are down to the reserve | 54 |
| `Makefile.am`, `vsr.pc.in`, `configure.ac` | One `libvsr.a` with liburing; three headers installed (done by the build skeleton) | 32 |
| `vsr-io.h` | Platform note: Linux >= 6.18 through the io_uring syscalls, no liburing; `vsr_io_uring_init` probes once and fails with `-ENOSYS` on an older kernel | 52, 53 |
| `Makefile.am`, `vsr.pc.in`, `configure.ac` | liburing dropped: no pkg-config check, no `Requires.private`; the kernel's UAPI header vendored under `src/io/uapi` | 52 |

`vsr-sim.h` and `vsr-client.h` are unchanged.

## 11. Open questions

- The per-stream inactivity timeout reuses `handshake_timeout_ns`; a
  dedicated option may be wanted once real transfers are measured.
- `versions` is sized like the op ring, which is conservative; a smaller
  bound tied to the core's uncommitted suffix needs a core-side statement
  of that bound.
- Whether NEW and JOIN should refuse a directory that holds stray
  `clients-*` files, or ignore them.
