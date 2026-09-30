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
   uapi/io_uring.h                the kernel's UAPI header, vendored: Linux
   uapi/io_uring/zcrx.h           7.2.6's headers_install output (from
                                  linux-libc-dev 7.2.6-1), one include
                                  redirected; uapi/README.md
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
| Tail region | `cache_bytes + 2 * block_bytes`; `cache_bytes >= write_behind_bytes + pinned_payload_bytes + 2 * record limit + 2 * header bytes + block_bytes` | `store.c`, decisions 36 and 69 |
| Single write | `write_behind_bytes + record limit + block_bytes <= 1 GiB`; `segment_bytes + 2 * block_bytes <= UINT32_MAX` | `store.c`, decision 70 |
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
  `tests/fuzzy/entry` (libFuzzer): `get_entry_at` over split payloads fails
  exactly when the skipped entries leave the payload, and an entry it
  accepts is within the limits and its region budget, re-encodes to the
  bytes it was read from, and agrees with `get_entries`.

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
 dial:    FREE ─SOCKET, then CONNECT on the raw fd─▶ CONNECTING ─▶ HELLO
 accept:  FREE ◀─multishot ACCEPT CQE (raw fd)─ HELLO (preamble+HELLO due)
 adopt:   FREE ─vsr_io_adopt─▶ ESTABLISHED (flags 0) | HELLO (HANDSHAKE)
 EXTERNAL mode: CONNECTING/accept ─preamble on the raw fd─▶ EXTERNAL
                (HANDSHAKE op) ─caller OK, fd installed─▶ ESTABLISHED
 HELLO ─both HELLOs seen, mode and version match─▶ ESTABLISHED
 any ─error, timeout, revoke, close─▶ CLOSING ─recv terminated, NOTIFs in─▶ FREE
```

Descriptors (decision 72): a link's socket is a raw descriptor
(`raw_fd`) until the engine takes it over into an engine file slot (`fd`)
through `vsr_io_engine_install`, at the CONNECT or ACCEPT completion in
TRUSTED mode, at the caller's OK HANDSHAKE completion in EXTERNAL mode,
and at adopt; from then on every record is FIXED_FILE on the slot. The
listening socket is DIRECT into an explicit engine slot; the accept itself
is a plain multishot ACCEPT delivering raw descriptors. Listener state
(`vsr_io_listener`: SETUP, ACTIVE, REARM, CANCEL, CLOSING) lives in a
fixed table of `VSR_IO_LISTENERS_MAX` entries; an accepted descriptor that
finds no free link entry is parked in `orphans` and closed by the next
prepare.

Handshake (TRUSTED): the dialer sends the 8-byte preamble then a HELLO
frame; the acceptor consumes the preamble, reads HELLO, checks `handshake`
and the frame version, records `node` and `purpose`, sends its own HELLO,
and is established; the dialer is established on receiving the acceptor's
HELLO whose `node` and `purpose` must be the dialed ones. Sends are queued
during the handshake and flow once established. A HELLO not received
within `handshake_timeout_ns` closes the link. A peer link from a node
that is not in the node table is closed (a node must be known to be
authorized; an unknown node cannot be authorized for any envelope), as is
one claiming the engine's own node id (the engine never dials itself, so
such a link could only inject its own replicas' frames; decision 86), a
second HELLO, a HELLO on an established link, or any other frame before
the handshake is done.

Handshake (EXTERNAL): the dialer sends the preamble on the raw descriptor
and, once that send's result is in, the HANDSHAKE op names the raw
descriptor with OUTBOUND and the expected node; the acceptor reads exactly
the 8 preamble bytes with a plain RECV on the raw descriptor (re-issued
for a short read), verifies them, and the op names the descriptor with
INBOUND and no node. The engine touches the socket no further until
`vsr_io_links_handshake_done`: status OK with the expected node (outbound)
or a known node other than the engine's own (inbound) installs the
descriptor and establishes; any other outcome closes it with a plain
CLOSE. A link closed while the caller holds its descriptor (revoke,
shutdown, timeout) stays CLOSING until the completion returns it; one
closed while its preamble RECV or its CONNECT is still in flight has that
record cancelled by the teardown (decision 87).

Carrier election (decision 41): whenever a peer link to node N becomes
established or leaves ESTABLISHED, `nodes[N].carrier` is recomputed as the
established peer link whose dialer is the lower node identity (`OUTBOUND`
if `own < N`, else `INBOUND`), oldest `established_ns` first; if none has
that direction, the oldest established link of any direction. Sends to N
go to the carrier only. A non-carrier link with nothing received or sent
for `idle_timeout_ns` is closed; the carrier is never idle-closed while the
node has queued sends, and is idle-closed otherwise like any link, since a
node with no traffic needs no link (the next SEND redials).

Dialing (decision 73): a node with an address and no established or
pending link is dialed when a SEND is queued for it or a replica
authorizes it (`wanted`), immediately the first time and then at
`next_dial_ns` = failure time + `connect_backoff_ns << min(attempts - 1,
4)`, where `attempts` counts consecutive dials that failed at any step
before ESTABLISHED (connect refused, handshake refused, timeout, the
caller's refusal). A caller-dialed node (NULL address) gets a
`LINK_WANTED` op on the same schedule, carrying the attempt number. An
established peer link resets `attempts` and clears `wanted`; an idle
close clears `wanted` too, so the next SEND redials at once; the engine
never dials its own node id. Due dials are found by poll through
`dials_due`; a backoff in the future is a DIAL deadline. The node's
`last_error` is the last failure of a link identified as its; an inbound
link that failed before identifying itself is nobody's.

Send path per link (decisions 38 and 82), executed in
`vsr_io_links_prepare`. The bytes a link sends form one stream counted by
`stream_offset`; the BUILD is the send under construction, `vec_count`
vectors in `vecs` covering the `build_bytes` bytes below `stream_offset`,
to which control bytes, encoded messages and stream frames all append.
Header bytes live in the send slab used as a ring addressed by unwrapped
64-bit counters (`header_tail` the write position, `header_sent` the
start of the build's header bytes, `header_head` the floor: the oldest
live send's `header_begin`); the writer area is the contiguous space at
the tail, which skips to the slab's start when the space before the end
is below a frame header plus one inline copy and the front holds more.

1. If a send is in flight, or every `sends[]` entry awaits a NOTIF, do
   nothing. Otherwise the node's carrier extends the build with queued
   messages in order: `vsr_io_encoder_emit` into the free vectors and the
   writer area, stopping at `send_coalesce_bytes`, `VSR_IO_SEND_VECTORS`
   vectors or the ring's room; a message continues in the next build. A
   message's `end` stream offset is assigned when its encoding starts
   (its frame length is known from the digest), so the messages handed to
   the carrier's stream are a prefix of the queue. A new vector contiguous
   in memory with the previous one (a frame's padding and the next
   frame's header) merges with it.
2. Emit the build if it is not empty: classify (`fixed` when every vector
   lies in the pool, `vsr_io_pool_contains`; else `zero_copy` when bytes
   `>= zero_copy_bytes`), allocate a SEND slot expecting two completions
   for a zero-copy send and one otherwise, fill the entry (`begin`, `end`,
   `header_begin`, `header_end`) and mark the link in flight.
3. On the result CQE: a negative result closes the link (the queued
   messages complete `RETRY` through the node's review, below); a short
   result trims the entry to the bytes sent and leaves the unsent tail of
   the vectors in place as the next build, its header bytes reserved from
   the entry's own `header_begin` until it goes out, so stream offsets
   never rewind and every message `end` stays valid; a full result empties
   the build. `sent_offset` advances either way.
4. On the NOTIF (or the plain send's completion) the entry frees; the
   floor and `notified_offset` are recomputed as the minimum over the live
   entries (`header_begin` and `begin`), falling back to `header_sent` and
   `sent_offset`, so NOTIFs may arrive in any order. Then the node's
   queue completes every message nobody reads any more: the carrier's
   with `OK` once `end <= notified_offset` (a `COMPLETE` event queued
   through `vsr_io_engine_complete_core`), a retiring one (below) with
   `RETRY` once its link's notified offset passed it or that link has no
   live send; completions may thus leave the queue out of order, and the
   carrier's encoder index follows the message it is on. A stream link
   calls `vsr_io_streams_sent` (also at a zero-copy result, when the
   vectors are free again).

Queue rules (decision 83): `vsr_io_links_send` digests the message
(`vsr_io_codec_message_digest` with the replica's limits), wants a link to
the node, and returns `RETRY` for the engine to complete at once when the
destination is unknown, unauthorized or the own node, the message cannot
be digested, the replica is unattached or the engine closing, or the
queue is full of messages on the wire; a full queue with a message not on
the wire retries the oldest such and takes the newcomer. The node's queue
is reviewed after every election and dial failure: a carrier change
RETIRES the started prefix (`retiring` names the old link: the bytes on a
lost or demoted link are not delivered by the next one, and a zero-copy
send of that link may still read them, so each message completes `RETRY`
only at the NOTIF that releases it, or at once when the link has no live
send) and closes a demoted link that had a frame half sent or a build
pending (`-ECANCELED`) rather than leave its stream torn; a node with no
carrier and no peer dial pending (`node_peer_pending`, which counts peer
links only) retries its waiting messages at once, which is how a failed
dial, a lost carrier and an unanswered `LINK_WANTED` complete them.
`vsr_io_node_clear` and `vsr_io_links_shutdown` complete everything at
once (`RETRY`, `CANCELLED`), retiring messages included: the node or the
engine is going away, and a NOTIF still pending on a closing link reads
the bytes for microseconds at most. `messages_sent` and
`messages_retried` count every outcome, the returned `RETRY` included.

TCP_NODELAY (decision 85): with `options.nodelay` every taken-over socket
gets one SETSOCKOPT record on its connect slot (idle by then, tagged
`VSR_IO_STAGE_NODELAY`) whose result is ignored, so a socket without the
option is simply left as it is; a dialed AF_UNIX peer skips it.

Receive path per link: one multishot RECV with `BUFFER_SELECT` on the
pool's group, re-armed after `-ENOBUFS` once slabs are provided and after
any termination while the link is open, except while a stream link is
paused (step 4). Each RECV CQE with `BUFFER` gives
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
   slab on a later CQE, wait; else (the next CQE names another slab, or
   the partial's frame is still to be delivered) the delivery joins the
   link's HELD runs (`held[VSR_IO_LINK_HELD]`, arrival order, one pool
   reference each, a delivery contiguous with the last run merging into
   it). Once the partial run is consumed the first held run becomes the
   partial; while the partial's frame is incomplete and a run is held,
   the frame is reassembled (decision 76): the partial bytes are copied
   into a reassembly slab acquired for the frame (`vsr_io_pool_acquire`
   with internal priority, counted in `links.reassembled`) and the
   frame's missing bytes, header first so that its length is known and
   never past the frame's end, are copied behind them from the held run,
   so the bytes that follow the frame stay in place for zero-copy
   carving. A frame fits one slab (`frame_limit` is `slab_bytes`) and the
   run starts at the slab's offset 0, so the copy always fits; the slab
   is released once the frame is consumed. No free slab: the runs stay
   held, `retry` is set and every `vsr_io_links_poll` carves again; a
   link needing more than `VSR_IO_LINK_HELD` runs is closed with
   `-ENOBUFS`.
4. Decoding: check the body CRC over the cursor; HELLO goes to the
   handshake, stream frames to `vsr_io_streams_frame`. A MESSAGE on an
   established peer link (decisions 67 and 75): the envelope's `cluster` and
   `from` are read, the replica of the cluster resolved
   (`vsr_io_engine_replica`) and `vsr_io_links_lookup(cluster, from)` must
   equal the link's node; then `vsr_io_engine_deliver` allocates a
   region, decodes and queues the event. A body shorter than its
   envelope, a cluster without a replica, an unauthorized `from` or a
   body the decoder rejects is dropped and counted in `frames_rejected`,
   the link staying up; a MESSAGE before the handshake or on a stream
   link closes the link with `-EPROTO`. A replica without a free region
   leaves the bytes in place with `retry` set; the next poll delivers
   them. Every accepted frame updates the node's `last_received_ns`. A
   stream frame the stream module cannot take yet (`false`: the
   requester's window, or the forwarded ring, is full) stays in place
   with `retry` set the same way, and the link PAUSES (decision 99,
   `recv_paused`): the next prepare cancels its multishot RECV (a CANCEL
   on the shutdown slot, one completion whose result is ignored, since
   `-ENOENT` or `-EALREADY` mean the receive terminated on its own;
   `recv_cancelled` tells its `-ECANCELED` termination from a loss), no
   receive is re-armed while the link is paused or that CANCEL is out,
   and the pause lifts when a poll's retry carves every byte the link
   holds (the stream module freed a window unit through
   `vsr_io_streams_data_done`, or the ring drained), at which point the
   next prepare re-arms the receive. The socket buffer then fills and the
   TCP window throttles the source; what the kernel delivered before the
   cancel took effect joins the held runs, whose bound (step 3) must
   absorb that burst (section 11). A paused link learns of a reset or an
   EOF only when it receives again; peer links never pause.
5. The slab reference taken at `recv_begin` is released once every byte of
   its run has been decoded (each MESSAGE lease took its own reference)
   or discarded; closing a link releases the partial, the held runs and
   the reassembly slab at once.

Invariants: a link never has two sends in flight; `notified_offset <=
sent_offset <= stream_offset` and `header_head <= header_sent <=
header_tail <= header_head + slab_bytes`; the build is empty while a send
is in flight, so `vecs` is the in-flight send's; the messages with `end`
assigned are a prefix of the node's queue, the retiring ones first, then
the carrier's;
every queued message completes exactly once; a slab is referenced by a
link only while it holds bytes not yet leased or discarded, exactly once
per run (the partial's reference is the reassembly slab's own once the
run lives there), plus its send slab; `retries_due` counts the links with
`retry` set.

Tests: `tests/unit/link`: a fake engine feeding CQEs and checking records:
dial with backoff to 16x, accept, both handshake modes, refusal on mode and
version mismatch, simultaneous dial with carrier election on both ends and
idle closure of the loser, send classification for the four flag cases
(slab-only, inline copy, outside payload below and at `zero_copy_bytes`,
payload in a taken slab), coalescing bounds (the budget, a message cut at
the budget and continued, the vector bound with 16 KiB slabs, the ring
filling, wrapping and draining under held NOTIFs), short sends at every
byte boundary of a two-frame send, NOTIFs held and released out of order,
a queue full of messages on the wire and one with a message that yields,
link loss by reset, by a failed send and by end of stream with messages
queued, awaiting NOTIF and in flight (every op once, references back to
the send slabs), a carrier change by an adopted preferred link with whole
frames and with a frame half sent, a zero-copy send refused at
translation, stream links (dial, refusals, request, chunk and end frames
under the flag rule, EBUSY on a send in flight, on every entry busy, on
the vectors and on the budget, ELIMIT, the idle exemption of a bound
link, an orderly close, a failed dial that schedules nothing, a stream
frame on a peer link), receive splits at every byte boundary of a
two-frame stream across one, two and three slabs (the header's too), many
frames in one slab, a frame ending exactly at a slab's end, slab-sized
frames, delivery refused for want of a region and retried at poll, the
reassembly slab unavailable then available, a bad CRC and an oversized
length in reassembled frames, an unauthorized sender and an unknown
cluster dropped, closing with runs held (references back to zero), a
seeded random walk of MESSAGE sequences under random slicing, slab cuts,
pool drought and lease holding (every authorized message exactly once, in
order, byte-identical, references balanced), an end-to-end seeded random
walk of two engines exchanging messages both ways under short sends, held
NOTIFs, receive slicing and resets (exactly-once in-order delivery per
link epoch, every op once, references balanced), revoke and address
change.

### Streams (`src/io/stream.h`)

Purpose: the two sides of a bulk transfer over a STREAM-purpose link.

State: `limits.streams` entries, each with a ring of `stream_window`
UNITS in stream order (a chunk: a DATA op at the requester, a chunk read
or send at the source) and a ring of `stream_window` queued WRITES (the
source's `STREAM_WRITE` events). Handles and ids (decision 93): the
source's handle is `generation << 32 | index`, never reused; SERVE and
DATA op ids carry their kind in the top two bits (`VSR_IO_STREAM_OP_*`),
the generation, the stream and, for DATA, the chunk's 16-bit sequence in
the stream (decision 100: the unit is found by the sequence's distance
from the head unit's, never by a ring slot, which a later chunk reuses),
so the engine routes a rail COMPLETE by id alone (HANDSHAKE ids have
those bits clear) and a stale or repeated completion is `EINVAL` (a DATA
id recurs only 65536 chunks later).

Requester: `STREAM_OPEN` takes a stream and a link (`ELIMIT` when none;
`EINVAL` for a malformed request, a caller request with the library
prefix, a cookie already in use by a caller stream, an unknown,
caller-dialed or own node, a closing engine), dials
(`vsr_io_links_open_stream`); at `link_up` it queues the request frame
(retried at `sent` and at poll after `EBUSY`; too large for the frame
limit: FAILED) and receives chunks: each becomes a `STREAM_DATA` op (a
unit holding one slab reference, `vsr_io_pool_retain`) forwarded in
order, released by the caller's completion (`vsr_io_streams_data_done`);
a zero-length chunk is consumed without an op. When `stream_window` units
are outstanding, or the forwarded ring is full, the frame is left in the
link (`vsr_io_streams_frame` returns false) and retried at every poll,
and the link pauses its receive meanwhile (decision 99), so a requester
whose caller is behind throttles the source through the socket buffer and
the TCP window rather than through the link's held runs. A
unit completed out of order frees its slot only once the older ones are
in (the ring's head moves over freed units). `STREAM_END` arrives as a
frame (a byte count other than the bytes delivered: FAILED), or is
synthesized on link loss with `RETRY`; the requester closes its link at
the END frame; the END op is emitted at poll once every DATA op was
completed and the link's sends were notified, and the cookie is then
reusable. The lease of a `STREAM_OPEN` ends with its `STREAM_END`: the
engine emits no RELEASE op for its own event kinds (decision 94).

Source: an accepted STREAM link waits for the request frame; the stream
is created then (no free stream, or no room in the forwarded ring: the
frame is kept and retried at poll; the unbound link is idle-closable
meanwhile), the link bound (`links.links[link].stream`) and the request
slab retained. A request beginning with `VSR_IO_LIBRARY_MAGIC` goes to
`vsr_io_snapshots_serve` (a malformed one closes the link `-EPROTO`); OK
opens the stream with owner LIBRARY (the module then feeds one FILE
write and CLOSE through the write and close calls, naming
`vsr_io_streams_handle`), any other status sends END with that status
and closes. Any other request becomes a `STREAM_SERVE` op with the
request bytes pinned in their slab until `vsr_io_streams_served`.
Refusal (any completion status but OK) sends END with `RETRY` and
closes; no END op follows a refused stream. Accepted: `STREAM_WRITE`
events queue in order (`AGAIN` when `stream_window` are queued: resubmit
after a WRITTEN; `EINVAL` for a handle that is not an OPEN source
stream, malformed buffers, or a caller stream's FILE range on one of the
engine's file slots, which would stream the engine's own descriptors to
the peer: decision 103). The queue is chunked in order into units: a
BUFFERS write into slices of at most `stream_chunk_bytes` and
`VSR_IO_STREAM_CHUNK_VECTORS` (64) spans, ready at once; a FILE write
into `stream_chunk_bytes` reads (`READ | FIXED_FILE | FIXED_BUFFER` from
the caller's slot into a pool slab acquired with internal priority; a
short read resumes the chunk; a result of 0 or an error ends the stream
with FAILED after the chunks the link already took). Units are sent in
stream order (a unit still reading blocks the later ones; `EBUSY` from
`vsr_io_links_send_frame` is retried at `sent` and at poll), each as one
chunk frame whose CRC the module computes over header and payload; a
SENT unit is released once the link's `notified_offset` passes its frame
(a FILE unit's slab then). `STREAM_WRITTEN` is emitted, in write order,
once a write is fully chunked and its last unit released; it ends the
write's lease (decision 94) and is emitted for every queued write, also
when the stream ends early. `STREAM_CLOSE` queues END with the status
after the last chunk of the last write; it is OK once per accepted
stream: while OPEN, and after the stream ended under the caller (loss,
timeout, shutdown, a file read failure) until its END op is emitted,
when it changes nothing; `EINVAL` after the caller's own close or
refusal, once the END op went out, and for a status outside `enum
vsr_io_status`, which the requester's decoder would refuse as a
malformed END (decision 102). The source's END op (accepted streams only)
is emitted once the END frame is notified (or the link is gone); the
stream then LINGERS on its link until the requester's close reaches it,
or the inactivity timer closes it (decision 96): a source closing first
would cut the frames a requester blocked on its window still holds in
its link, which the link module discards at EOF. Library-owned streams
get no WRITTEN and hear their end through `vsr_io_snapshots_stream_end`.

Early ends (decision 97): link loss ends a stream with `RETRY`, unless
it is a source whose END frame already reached the kernel
(`sent_offset`), which keeps its status; a frame the stream's state
refuses (a chunk or end at the source, a second request, a request at
the requester, a chunk at the wrong offset, a malformed body) counts in
`frames_rejected` and closes the link `-EPROTO`, FAILED on the refusing
side; shutdown ends every stream `CANCELLED`, also when the link
shutdown closed its link first (`link_lost` while `links.closing`:
decision 101), and a stream already ending keeps its status
(`vsr_io_close` is done once `streams.active` is zero: the caller still
completes its DATA and SERVE ops). At the source an early end stops
chunking (`aborted`): unsent units are released (a read in flight at its
completion), `offset` falls back to the first byte the link never took
(for a failed read, the failed chunk's first byte: the unit stays in the
ring until the abort cut there), the queued writes are marked chunked so
their WRITTEN follows, SENT units wait for their NOTIF (the link entry's
teardown, watched through `stream->link`, or `notified_offset`). The
module never closes a link from inside `vsr_io_streams_sent` (a send
completion): closes happen in poll, at frames and at caller calls.

Timeouts: a stream with no progress for `handshake_timeout_ns` (the only
per-stream duration in the options; a dedicated option is deferred) ends
with `RETRY`: the deadline (`VSR_IO_DEADLINE_STREAM`, dispatched to
`vsr_io_streams_deadline`) is re-armed at every frame, send progress,
completion and caller call, a DATA completion included (the poll after
it drains deadlines before the link retries the frame the window held);
the requester's covers the dial, the source's the SERVE at the caller
and the drain and linger after CLOSE; a requester waiting for its
caller's DATA completions after END is untimed.

What the link module gives the stream module (decision 84):
`vsr_io_links_open_stream` dials a STREAM-purpose link at once, whatever
the node's backoff (`EINVAL` for an unknown, caller-dialed or own node,
`ELIMIT` with no free link entry), and `vsr_io_streams_link_up` /
`link_lost` follow; a stream dial's failure schedules nothing for the
node. An inbound STREAM link is bound to its stream by setting
`links.links[link].stream` at the request frame; a bound link is never
idle-closed (the module closes it with `vsr_io_links_close`), an unbound
one is. `vsr_io_streams_frame` receives every stream frame with its slab
(to retain if the bytes are kept) and returns false to leave the frame in
place, retried at every poll like a MESSAGE without a region.
`vsr_io_links_send_frame` queues one raw frame: the codec-put body header,
payload vectors referenced in place (pinned until `notified_offset`
passes the returned `end`), `body_crc` over header then payload (the link
pads to 8 with zeros from its slab and extends the CRC); `EBUSY` while a
send is in flight, every entry awaits its NOTIF, or the budget, the
vectors or the ring are full, after which `vsr_io_streams_sent` (called
at every send result and NOTIF of the link) is the cue to retry.
`vsr_io_links_close` calls `link_lost` synchronously, so the module sets
`link_gone` before closing and ignores the re-entrant call.

Tests: `tests/unit/stream` over the real link module in the two-engine
world of `tests/unit/link` (copied harness, plus caller files behind
registered slots for FILE reads with short, random and failing results,
and test doubles of the snapshot hooks): refusals and limits, request,
chunks, WRITTEN, END and cookie reuse, the window on both sides, FILE
transfers of 0, 1, exact-window and many chunks with short and random
reads, EOF and read errors, mixed FILE and BUFFERS writes, link loss on
either side at every stage, refusal, protocol errors, library streams
served and requested, timeouts and shutdown, backpressure (a requester
whose caller drips one completion at a time over many chunks with a
socket buffer of four slabs, alternating and in one burst with a window
of one, completes byte-identical with the source parked; the harness's
socket buffer parks a send when full, as the kernel does, and fails it at
a shutdown or reset), and a seeded random walk (`tests/unit/stream SEED`,
prints its seed) of transfers with random writes, span slicing, receive
slicing, held DATA ops and NOTIFs, clock jumps and resets, checking every
byte arrives exactly once in order, a WRITTEN for every queued write,
every slab reference, slot and link released after each transfer, that no
link is ever closed `-ENOBUFS`, and that a transfer ends RETRY only after
a refusal, a reset or a clock jump, never for slowness alone. `tests/integration/streams` over the
simulation (two engines, BUFFERS and FILE transfers with split and
corrupt faults, a stream never delays a heartbeat on the peer link) is
still to come.

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
  since ops are processed in emission order), takes an engine lease (for
  the completion, 105), a staging slab, an engine file slot and a registry
  entry (`RETRY` when any is missing, or while another CAPTURE runs), draws
  16 random bytes for the id (redrawn while zero or held), copies the task
  and its checkpoint into the template with the id, snapshots the table
  (`vsr_io_store_snapshot_clients`, which also sets the capture floor),
  and forwards the op with `data = &template task` at its next poll. The
  writer then produces the file (`O_RDWR | O_CREAT | O_EXCL`, no
  O_DIRECT, so chunks need no alignment): header; for each snapshot entry
  the record bytes come from the ring when hot (`vsr_io_store_hot`), else
  from a cold read into `cold_slab`, block-aligned from the log for a log
  record (the CLIENTS record's CRCs checked, the client record located
  through its change descriptor) or `slab_bytes` from the base file
  through `store.base_slot` for a file-only record (bounded and CRC-checked
  like any file record); each record plus its CRC is staged in the slab,
  written (`WRITE | FIXED_FILE | FIXED_BUFFER`) when full and at the end;
  trailer, whose crc the writer extends over the header and each record
  (without its CRC) as it stages them (W4).
  `vsr_io_store_capture_offset` records each entry's offset;
  `capture_end` sets `last_capture` (a zero id when the library half
  fails: the floor only). The file stays open (the latest capture's kept
  slot, 106). Completion to the core: `OK` with the caller's checkpoint
  under the lease when both halves are OK; otherwise the file is closed and
  unlinked first, then the caller's status (or the library's: `FAILED`
  for I/O errors, `CORRUPT` for record bytes that fail their checks) is
  reported. The core schedules captures on its own; the engine does not
  retry.
- SNAPSHOT_SYNC: forwarded at once; `FSYNC | DATASYNC` of the file
  through its kept slot or a transient read-only open (closed again),
  then `FSYNC` of the directory (opened into a slot at the module's first
  poll); completion when both halves are over, with the caller's status
  or `FAILED`; any failure is reported as-is (the core fences on it). A
  SYNC of an entry whose kept slot is being released cancels or follows
  that RELEASE (107). A SYNC that reaches the directory fsync while the
  directory's open has failed has the poll retry that open once for it,
  and fails only if that attempt fails (W3).
- FETCH: an id already held is forwarded at once. Otherwise `task->peer`
  is resolved to a node through the authorization table (no node:
  `RETRY`), an engine lease, the reader's slab, a file slot and an entry
  are taken (`RETRY` when missing, or while the reader serves another
  fetch or a base load), and a library stream is opened with a
  `vsr_io_wire_library_request` for
  `(cluster, task->peer, task->checkpoint->id)`. `clients-<id>.tmp` is
  opened (`O_WRONLY | O_CREAT | O_TRUNC`); chunks arrive through
  `vsr_io_snapshots_stream_data` in order, are copied into the reader and
  verified as they arrive, and are written at their offsets one write at a
  time, each DATA op completed when its write completes (109). At
  `stream_end`: an OK END with the reader past a matching trailer closes
  the file, renames it to `clients-<id>` (RENAMEAT), keeps the entry with
  `sequence = 0` (set by the RESTORE's load) and forwards FETCH; any other
  outcome closes and unlinks the temporary file and completes the core op
  with the stream's status (`RETRY` on loss, `FAILED` for a source read
  error or a local I/O error, `CORRUPT` for bytes that fail verification,
  `NOT_FOUND` from the source) without involving the caller. A caller
  failure on a file this FETCH's stream wrote, not yet adopted by a
  RESTORE, unlinks it (vsr.h's private partial object); a FETCH of an id
  already held transferred nothing and keeps the file whatever its caller
  answers (the core's hold of the earlier FETCH, W2). `basis` is passed
  through; the library half does not use it.
- DROP: forwards; on the caller's OK, the kept slot is closed and the
  file unlinked once `readers == 0`, then the entry freed; a served library
  stream still reading it delays the unlink, not the completion. An id the
  registry does not hold is forwarded all the same and its file unlinked
  if present (107). The caller's failure keeps the file.
- Base loads: the module's poll starts one whenever
  `vsr_io_store_base_wanted` reports a file (a held RESTORE, a PUBLISH of
  a file that is neither the latest capture nor the base, or recovery's
  anchor, decision 90); `vsr_io_snapshots_load_base(id, sequence)` opens
  `clients-<id>`, STATXes its size, reads it in slab-sized reads through
  the reader, calls `vsr_io_store_base_begin`, `base_record` per record
  (ELIMIT: `FAILED`, EINVAL: `CORRUPT`) and `base_end`, then
  `vsr_io_store_base_resume(status)`. A missing file is `CORRUPT`: the
  store wants one only for a FULL role (the held transaction's, or a FULL
  replica's recovery), so a witness's RESTORE loads nothing and a witness
  promoted to FULL needs its file like any full replica (W5). The file
  stays open; `store.base_slot` follows the store's client base (106). A
  kept slot is released (a RELEASE job: CLOSE) only while no core op is in
  progress on its entry (W1): the settles wait for the entry's job to end,
  and a RELEASE settles nothing.
- Serving: `vsr_io_snapshots_serve` finds the replica by cluster and
  replica id, requires the registry entry complete, not discarded or
  dropped and with no CAPTURE outstanding (else END `NOT_FOUND`),
  increments `readers`, then opens the file into a slot from prepare and
  feeds one FILE write of the whole file to the stream module followed by
  CLOSE OK (FAILED when the write is refused; the open's failure ends the
  stream `NOT_FOUND` for `ENOENT`, else `FAILED`); the slot is closed and
  `readers` decremented at `stream_end` (108).

At most `VSR_IO_SNAPSHOT_FILEOPS` (4) records of a replica's module are in
flight (slot kind CLIENTS, owner the replica, `sub` naming the entry, the
serve or the directory, the cookie the step). A wait for a slab, a slot, a
lease or a file operation arms the replica's CAPTURE deadline at
`io->now + 1 ms`; its pop needs no dispatch, the poll that follows retries.
An outcome decided in the module's prepare (a serve refused for want of a
slot, a SYNC failed there, a name that does not fit) arms it at `io->now`,
since the core's poll and the links' prepare already ran (W6).
`vsr_io_snapshots_close` (detach) is EBUSY while a record is in flight,
the writer or reader is busy or a served stream is open; otherwise it
drops every kept slot, clears `store.base_slot` and releases the leases of
ops the stopped core abandoned.

Tests: `tests/unit/snapshot` (two engines over fake executors with a
modelled network and a directory model per engine: named files behind
registered slots with a flushed shadow and durable names, faults and holds
per record kind; each engine has a real store, real links and streams,
and the test plays engine part 2): capture from the ring, cold log reads
and the base file, files of 0, 2 and `max_clients` records, every
library-half and caller-half failure of a capture, SYNC with and without a
kept slot and racing a RELEASE, DROP (unknown ids, under a reader, of the
base), fetches of several chunks, every CORRUPT/FAILED/NOT_FOUND/RETRY
cause on both sides of a library stream, the rename only after a verified
END, the serve's file-operation wait and its end in every state, base
loads for a RESTORE and a PUBLISH (also one that cannot pack at once), 14
load failures, recovery of the anchor (decision 90) including a missing,
corrupt or unreadable file, close and engine shutdown with work in flight.
After every step the harness checks the module's invariants: engine file
slots held exactly once and covering every open handle, CLIENTS records
equal to the file operations and at most four, reserved leases distinct.
The review's tests (`test_review_*`) cover an op answered while a RELEASE
of its file is out (W1), a repeated FETCH failing at its caller (W2), a
failed directory open (W3) and tampered files, each through a load and a
fetch (W4).
`tests/integration/snapshots` over the simulation (capture on one engine,
fetch by another, restore, recovery from the file after a crash, a corrupt
file detected) is still to be written.

### Engine (`src/io/engine.h`)

Section 7.

### Executor: io_uring (`src/io/uring.h`)

Section 8 is the contract; `uring.c` realizes it on Linux >= 6.18 (design
section 2) through the raw `io_uring_setup`, `io_uring_enter` and
`io_uring_register` syscalls over the vendored UAPI header
`src/io/uapi/io_uring.h`, Linux 7.2.6's (decision 52; `src/io/uapi/README.md`
says where it comes from and how to refresh it, and `uring.c` asserts the
layout of every structure it hands the kernel); there is no liburing and no
other library. Specifics: the executor state and its tables live in the
caller's page-aligned region; the rings and the SQE array are the kernel's pages,
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
otherwise (decision 53); a setup refused with `-EINVAL` is classified by
the feature bits of a plain one-entry ring, `-ENOSYS` when they lack a
required one (a kernel before 6.6 refuses `NO_SQARRAY` so), else
`-EINVAL` (decision 104); nothing is probed after that and there is no
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
when requested (`IO_URING_NAPI_REGISTER_OP` with dynamic tracking, the ring
learning the NAPI ids of the sockets it polls); `provide` writes `struct io_uring_buf` entries then
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
executor, as the default ring and as the `uring-sqpoll` (SQ thread idling
after 10 ms) and `uring-napi` (20 us busy poll) columns;
`tests/unit/uring_translate` for the record-to-SQE table with a fake SQE
buffer (every opcode and flag combination, rejection of out-of-region
fixed buffers); `tests/integration/uring_refusals` for init's refusals on
emulated older kernels (section 9); `tests/integration/uring_smoke` for
the ring against the kernel, the whole suite over the default, SQPOLL,
NAPI and SQPOLL+NAPI rings (a variant refused with `EPERM` is reported
and skipped). Loopback
sockets carry no NAPI id, so the NAPI runs cover the registration (read
back from the kernel) and every path with busy polling enabled, not the
polling of a device queue.

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
bytes left in a block are zero and skipped implicitly. The gap between the
descriptors and the first payload, the gaps between payloads and the
record's tail padding are CRC-covered but not verified zero when a record
is read back: they are never interpreted, so a nonzero gap in a CRC-valid
record is harmless.

### 5.4 Clients file (`clients-<32 hex>`)

Header (64 bytes): magic "CLT1", format (2), cluster hi/lo, snapshot hi/lo,
op, sequence (the writer's), count, crc (over the preceding bytes). Records:
`vsr_io_wire_client_record`, result bytes padded to 8, then `u32 crc` of
the record and bytes. Trailer (16 bytes): magic, count, crc, reserved (0);
the trailer's crc is the CRC32C of the header, of every record without its
own CRC, in file order, and of the trailer's magic and count (W4), so a
record is bound to its file and its place (a record CRC alone accepts a
record written over another of the same length, or two swapped). The
record CRCs are left out because a CRC run over bytes followed by their
own CRC ends in a constant, whatever the bytes. A reader verifies every
CRC and both counts; anything else is `CORRUPT`. The snapshot
module's reader bounds a record's `length` by `result_bytes` and, when the
file's size is known (loads, not fetches), the record's whole extent by the
bytes left before the trailer; it waits for the record's bytes, checks the
CRC over them and only then decodes the fixed part
(`vsr_io_codec_skip_client_record` over exactly those bytes), so no length
reads past what it holds. A header naming another snapshot or cluster or
more than `max_clients` records, a record longer than a slab, a trailer
count that disagrees, or any byte after the trailer is `CORRUPT`. Files are
written once, sequentially, and never modified; the fetch path writes
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
   and the skipped ring bytes are not mirrored). A `head` exactly at the
   ring's end needs no gap, but the bytes go to the ring's start: a new
   extent begins there too, so no extent, and no write, runs past the
   ring's end (where the superblock copies follow it).
3. If the end of the packing (the seal's pad, the header after a possible
   wrap, the record after a possible wrap, and its trailing pad, simulated
   before anything is touched) would pass `pin_floor + size`, where
   `pin_floor` is the minimum of every active ring pin's `begin` and the
   `ring_begin` of the oldest write not yet complete (or `issued` when
   none): hold the STORE (queue it; nothing after it is processed until it
   proceeds, since ops are applied in order). The attach rule of decision
   69 guarantees the hold ends: pinned LOAD bytes are bounded by
   `pinned_payload_bytes` plus two record alignments, unwritten bytes by
   `write_behind_bytes`, and the header and the pads by the rest of the
   slack. The steps of 1 and 2 are taken only once 3 and 4 pass.
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
replica: the first extent with bytes in `[issued, head)`, cut at the
extent's end (a wrap or a segment boundary starts a new extent; each
write is `WRITE | FIXED_FILE | FIXED_BUFFER` on the tail region), the
newest extent padded to a block with a PAD in the ring first (the pad is
mirrored, so the file and the ring agree), provided a write entry is free
(`inflight_writes`). A segment header is a write of its own and the
records after it are not planned until it completed. `issued` advances to
the write's end (and over a wrap's gap); the next write starts there, so
no block is rewritten. A write is never longer than `write_behind_bytes +
record limit + block_bytes`, which the check keeps at or below 1 GiB
(decision 70). Superblock writes use the two superblock blocks after the
ring with their own slot kind and are never concurrent with each other.
Completion: a write's `state` becomes complete; `written` advances over
the contiguous prefix of complete writes in issue order to the
`last_sequence` of the last one; a negative or short result sets `error`
and applies `on_write_error` (FENCE: state FAILED, every queued op
completes `FAILED`; CONTINUE, replicated only: the store stops writing,
growing and rewriting the superblock and serves from memory, taking slots
of the table without preallocating them).

Flush: `flush_pending` is set by a SYNC (after `sync_delay_ns` from the
first SYNC of a batch), by the flush interval in replicated mode when
`written > flushed`, and by SNAPSHOT_SYNC for its own file. In `DSYNC`
mode writes carry `O_DSYNC` (the file is opened with it) and a SYNC
completes when `written >= sequence`; in `FDATASYNC` mode a `FSYNC |
DATASYNC` is issued once `written >= flush_target` and its completion sets
`flushed = written at issue`, completing every SYNC with `sequence <=
flushed`. The store also flushes on its own behalf (`flush_own`, section
6.5): such a flush is issued as soon as none is out, without waiting for
`flush_target`, and leaves a SYNC's request pending when it does not
satisfy it. `durable` is the largest sequence acknowledged to the core.

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
| RECLAIM(oldest) | `reclaim = oldest`; `retained_begin` = `log_begin` as of `oldest` from `trims` (the newest event with `sequence <= oldest`; a full history merges the newest event into the new one, which reports the older begin in between and so retains more); the ring below it is cleared; versions with `truncated <= oldest` deleted; then `vsr_io_store_free_segments` |

`index_apply` works from a record's bytes (the ring copy of a STORE, or
a recovery slab): the header, then every change descriptor checked
against the record's length (`offset >= 48 + 24 * count`, `offset +
length <= length`), then each change from its payload cursor. An empty
log (`log_begin == log_end == 0`, never appended) takes its bounds from
the first APPEND, TRIM or RESTORE (the core's genesis is 1). Bytes the
codec rejects or a change contradicting the log (an APPEND not at the log
end, a TRUNCATE below `log_begin`, an IDENTITY after sequence 1) are
`CORRUPT`; an index at capacity is `FAILED`; either fails the STORE and
fences. HARD_STATE and the anchor are decoded into `state_region` (the
hard state's epoch first, then the checkpoint with its epoch and a copy of
the manifest bytes, which the ring would overwrite); IDENTITY sets
`superblock_dirty` so the superblock carries the identity. The client
table is an open-addressing table with backward-shift deletion (no
tombstones); `reindexed` records the sequence of the latest TRUNCATE,
TRIM or RESTORE and `restored` that of the latest RESTORE.

LOAD at sequence `s` (the core always names its `stored_sequence`; the
engine routes every LOAD of an update before its STOREs, so normally
`readable == s`, decision 51). `s > readable`, `s < reclaim`, a bad type or
count, or `first > end` are EINVAL (a caller bug). The records a load
needs are resolved against the indexes when it is accepted and kept in
`load_refs`; the result, an empty one included, is a `vsr_loaded` graph
in an engine lease (`vsr_io_lease_alloc` through the replica embedding
the store) whose pin or slab the completion's `lease` names. A load that
finds no free lease waits in the load queue; the queue is FIFO and a cold
load at its head waits for the single read in flight (decision 37):

- RECOVERY: only from `open`; answered by recovery.
- LOG `[first, end)`: for `op = first` while `op < end` and the count
  allows: the version is `ops[op]` when `ops[op].op == op` and `sequence
  <= s`, else the entry of `versions` with `appended <= s < truncated`,
  else stop. The byte bound is applied when the entries are decoded (the
  body sizes are in the records): the result stops before the entry that
  would exceed it, never before the first. Hot (every record in the
  ring): the region gets `vsr_entry`s with spans into the ring, one pin
  covering the records. Cold: one `READ | FIXED_FILE | FIXED_BUFFER` of
  the block-aligned range covering the records (bounded by the slab; the
  batch is cut where it would exceed the slab or leave the segment) into
  a slab; on completion (a short read that leaves a record incomplete is
  `CORRUPT`, a negative result `FAILED`, neither fences) the records'
  headers, sequences and CRCs are checked (`CORRUPT` on failure), entries
  decoded with spans into the slab, and the lease takes the slab's
  reference. `next = first + count`, which is `end` when the range is
  complete; an empty result for a nonempty range is `NOT_FOUND`.
- CLIENT: the entry's `current` when `current.sequence <= s` and no
  RESTORE rebuilt the table after `s`, else `RETRY` (decision 51: the core
  resets the route and reloads at its newer stored sequence, without
  fencing); no entry or `number == 0` is count 0. The record is read from
  the ring when still there, else from the base file when `base_offset`
  is set (a cold read through `base_slot`, the registered slot of the
  current base file the snapshot module keeps open: a slab from the
  record's block, short at the file's end, whose record CRC the clients
  codec checks), else cold from the log. The log copy counts only while
  the record's slot still holds the segment it was packed into
  (`record_live`: a live slot whose `first_sequence` is at or below the
  record's): the base term of the floor frees a completed record once
  the base file holds it, and a reused slot's ring extents cover the old
  file range with other bytes (decision 113). A capture
  (`vsr_io_store_snapshot_clients`) likewise reports such a record as
  one only the base file has (`sequence` 0, the base offset), which the
  snapshot module copies from the file, and leaves it out of
  `capture_floor`. A queued base read is resolved again when it is
  issued: a PUBLISH or RESTORE packed while it waited may have moved the
  record in the file `base_slot` names by then (or, with a newer record
  or a RESTORE after `s`, it completes `RETRY` as at acceptance).
- REQUEST: `retained` when the version's `sequence <= s`, else walk
  `previous` links over the APPENDs after `s`; `RETRY` when a TRUNCATE,
  TRIM or RESTORE was applied after `s` (`reindexed > s`); the entry is
  decoded from its record like LOG.

The store fencing completes every queued load `FAILED`; a read in flight
keeps its slab (`cold_slab`) until its completion, and `vsr_io_store_close`
is EBUSY until then.

`vsr_io_store_admit` looks the incarnation up; unknown and `clients_count
== max_clients` returns false; unknown otherwise inserts an entry with
`inflight` set. `vsr_io_store_replied` clears `inflight` and deletes an
entry with nothing else set. The engine calls `replied` when it forwards a
REPLY whose status is not OK and the entry has no record and no retained
entry; an OK reply follows a CLIENTS record, which cleared `inflight` at
indexing (APPEND clears it too).

### 6.4 Recovery

`vsr_io_store_open` runs these steps, each an executor record whose
completion advances `recovery.stage` (`prepare_recovery` issues one
record at a time and moves through the stages that need none):

1. `OPENAT` `log` (`O_RDWR | O_DIRECT` when `direct_io`, `O_DSYNC` in
   DSYNC mode, DIRECT into an engine slot). `ENOENT`: NEW/JOIN create
   (`OPENAT` with `O_CREAT | O_EXCL`, `FALLOCATE` of `2 * block_bytes +
   segments * segment_bytes`, superblocks with `generation` from
   `random`, `run = 1`, `start_segment = 1`, `slots = segments`, then the
   first segment's header with STATE clear, then, in FDATASYNC mode, a
   `FSYNC | DATASYNC`, so no crash leaves a log without them (decision
   115); the RECOVERY load completes `NOT_FOUND` once the header write, or
   that flush, completed, which is what the core expects for an empty
   store); RECOVER completes `NOT_FOUND` at once and
   creates the empty log the same way, holding the STOREs of a warm-up
   until the header write completed (decision 71). An existing file under
   NEW/JOIN is recovered like RECOVER; the core rejects the recovered row
   itself, and an empty log (no record) is `NOT_FOUND` under every mode,
   the store READY over it.
2. `STATX` for the size (into the ring, scratch until the scan is over);
   `READ` both superblocks into their tail blocks; keep the valid one with
   the greater revision; none valid is `CORRUPT`. Geometry must match the
   options (`block_bytes`, `segment_bytes`, `header_blocks`), else
   `CORRUPT`. `slots` is the file's, `(size - 2 * block_bytes) /
   segment_bytes`: a growth that crashed between its `FALLOCATE` and its
   superblock leaves one more slot than the superblock names, whose
   header is not valid and which is therefore free; fewer slots than the
   superblock names is `CORRUPT`, more than `max_segments` is `FAILED`.
   `run` becomes the superblock's plus one; the next superblock goes to
   the copy not chosen.
3. For every slot, `READ` its header blocks into a pool slab (decoded
   into the ring as scratch); validate magic, format, generation and CRC;
   a slot with a valid header is a candidate segment: its `number`, the
   header's `last_sequence` (the sequence before it) and `run` go into the
   segment table; anything else is a free slot. Every valid header's
   `durable_floor` raises F. The start segment must be present in
   `start_slot`, else `CORRUPT`. `next_segment` is the greatest number
   seen plus one.
4. Load the start segment's state into `identity`, `hard`, `anchor`,
   `log_begin`, `log_end`, `client_base` (the anchor's id is the base
   id); `sequence = last_sequence`, `run` the header's;
   `F = max(superblock.durable_floor, header.durable_floor)`, raised by
   every valid header's floor and every CRC-valid record's `flushed` as
   the scan proceeds (decision 50).
5. Scan the segment's data in slab-sized `READ`s (a chunk is the slab
   rounded down to blocks, cut at the segment's end): for each record
   header (`vsr_io_codec_get_record`): a PAD skips when its length, which
   no CRC covers, runs exactly to its block's end; END (zero fill, a
   foreign magic, or a RECORD magic with fewer than 48 bytes left) or a
   bad header, wrong generation, `sequence != last + 1`, `run < last
   run`, or a run above the last record's (the start header's for the
   first) with `flushed` below the last sequence (decision 116: a run
   begins at a recovery that made its prefix durable, so its first record
   follows a recovered one) ends the chain, as does a header valid but
   payload CRC bad, when it lies at a block boundary. Inside a block such a verdict ends only
   the block: the bytes after the last valid record of a block are dead
   once writing resumed at the block after it (step 6), so they are
   swept for floors and the chain resumes at that boundary with a record
   of the next sequence and a run at or above the last one, when one is
   there (decision 110: neither a bad PAD, whose length no CRC covers,
   nor the head of a record torn at the following block hides the
   records written after a recovery). A
   record that straddles the chunk's end (its header short, or its length
   past the chunk) is read again from its block, provided that makes
   progress and the segment has the bytes; a read that fails or comes
   back short is a bad range. Before a range is judged bad it is read once
   more (`retried`, `retry_offset`): a transient bit flip or read error
   passes on the re-read and only a second failure at the same position
   ends the chain there. Each valid record is applied to the indexes
   exactly as a STORE is (section 6.3), through the same code: valid bytes
   whose content contradicts the log are `CORRUPT` wherever they lie.
   CLIENTS records with `sequence <= client_base` still update `current`;
   the base file load in step 7 then re-points entries at the file, which
   is consistent because every record after the base is in the scan. A
   replayed PUBLISH of an id other than the current base makes it the base
   at the PUBLISH's sequence; the file's merge lowers that below any
   record the file does not cover, as at runtime.
6. Where the chain ends in a slot (a bad range judged twice, or data
   exhausted) the rest of that slot is swept for floors only: PAD and
   record headers are tried at every aligned position and every CRC-valid
   record of the generation, whatever its sequence or run, raises F by its
   `flushed`, so a record behind a torn one still carries its floor. Then
   the successor (decision 48): among candidates not yet visited whose
   header names `sequence` with `run >= last run`, the greatest `number`;
   the current run becomes the header's when greater. Repeat from step 5.
   When none, every slot the chain never visited is swept the same way,
   which also counts records in stale segments and freed slots (a floor a
   persisted record carries was acknowledged whenever it was written).
   Then: `sequence < F` is `CORRUPT`; a log with records but no identity
   or hard state, or one whose superblock carries an identity other than
   the replayed one, is `CORRUPT` too, and so is a log with an op of
   `[log_begin, log_end)` the scan did not replay (decision 114: freeing
   keeps every revision from the sequence on media on, so only media
   corruption of records no floor covers returns such an older row). Stale headers (never visited) and
   abandoned successors (visited after the last record, so still naming
   it) free their slots; the rest of the chain is SEALED but for the
   segment holding the last valid record, which is OPEN with `used` at the
   block after it. `readable = written = flushed = durable = sequence`,
   the ring starts empty (`head = issued = 0`) with one extent at
   `file_head = resume`, so the next write starts there: no block below
   the recovered prefix is ever rewritten (the tail's persisted blocks
   above it are).
7. If `hard.role == FULL` and `anchor.id` is nonzero, the store wants the
   anchor's file as the base at `client_base` (`base_wanted`, kind
   RECOVERY): the snapshot module loads `clients-<anchor id>` through
   `base_begin`, `base_record` and `base_end`, which applies the merge at
   once, then `base_resume(status)`; OK rebuilds every client's `retained`
   over `[log_begin, log_end)`, any other status (a missing file is
   `CORRUPT`) ends the recovery with it. WITNESS skips it.
8. In FDATASYNC mode, `FSYNC | DATASYNC` (the records read may still be
   in the page cache after a process crash: the recovered sequence is
   reported durable to the core, so it is made durable first); rewrite the
   superblock with `run + 1`, the file's `slots`, the current start
   segment (unchanged: with the RECLAIM floor at 0 nothing frees) and
   `durable_floor = sequence`, wait for its completion; in FDATASYNC mode
   flush again, so the new run is on media before a record carries it
   (the run rule of decision 34 needs that); then READY, and the RECOVERY
   load completes with `vsr_recovered{identity, sequence, log_begin,
   log_end, hard, anchor or NULL}` built in a load region, the hard
   state's epoch and the anchor with its manifest bytes copied there (the
   core retains the checkpoint under the lease while later records
   replace the store's copies). A read in flight makes `close` EBUSY; a
   fence during recovery releases the slab at that read's completion.

Torn-tail cases the tests synthesize (`tests/unit/store`): a lost last
block; a lost middle block of a multi-block write with the later block
persisted; a persisted block from a torn write behind a block rewritten
by a later run (rejected by the run rule); a stale header of a segment
whose records were torn (an abandoned successor); a new segment's header
lost while its records persisted, under an old header naming the same
sequence (found) or another (the tail); a bad record at or below the
durable floor (`CORRUPT`), including a floor that only a later valid
record's `flushed` carries, and one the idle superblock write persisted; a
bad record above the floor followed by a valid later record (still the
torn tail); a PAD whose length misses its block's end (a dead tail: the
chain resumes at the next block) and one at a block boundary (the end);
the record after a torn straddling one, written at the next block and
found by the next recovery; an older run's record spliced from a torn
rewrite's lost first block and its kept second one, with the later run's
next record behind it (116); a block that
reads bad once and clean on the re-read (passes), a read error and a
short read; a record header straddling a chunk's end; both superblocks
valid with different revisions; one superblock corrupt; both corrupt; a
growth whose superblock write was lost; the start slot holding another
segment (`CORRUPT`); recovery after every prefix of a scripted log with
segments, wraps, growth and freeing, compared with the pre-crash indexes,
then more STOREs and a second recovery.

### 6.5 Freeing

`vsr_io_store_free_floor` = min(the sequence of the first live op from
`retained_begin` (or `readable + 1` when the log is empty; sequences never
decrease along the ring), the `appended` of every kept version,
`reclaimed`, `client_base_floor + 1`, `capture_floor`). A crash brings
back any revision from the sequence on media (`flushed`, or `written` in
DSYNC mode) on, and the recovered row must serve it whole, so the RECLAIM
revision applies as far as that sequence (`reclaimed`; `trims_reclaim`
and `versions_reclaim` follow it, LOADs are still refused below
`reclaim`), the rest as flushes or O_DSYNC writes advance it
(`reclaim_apply`), and the base term is the client base as of that
sequence (`client_base_floor`, with `client_base_pending` the revision of
a change above it; a change that lowers the base lowers the term at once,
which is right on both sides of it; decision 112). In memory-only mode
nothing is written any more, so a slot freed in the table is never
rewritten on disk and every term applies at once (`media_sequence`); the
write error that enters that mode applies the waiting terms. A SEALED
slot whose `last_sequence < floor`, whose bytes are all written (nothing
of it planned or in flight) and from which no queued load still reads is
freed: its extents die (`segment = NONE`, so `vsr_io_store_hot` no
longer answers for its file range) and the new start is the live slot
with the smallest number, with a superblock write planned. A freed slot
is `FREEING` until a superblock write issued after the free completed (a
superblock always names a present segment; a slot freed after the write
was issued waits for the next one), then, in FDATASYNC mode, `FLUSHING`
until a flush issued after that completion completed, since the write
may still be in the page cache (decision 111): the store asks for that
flush itself (`freeing_flush`, `flush_request` setting `flush_own`), and
a flush in flight at the completion does not count. An O_DSYNC write is
on media at its completion, and in memory-only mode the slot is free at
once (a write error under CONTINUE frees the `FREEING` and `FLUSHING`
slots too). A STORE that needs a slot frees first, then grows, then
waits while a slot is `FREEING` or `FLUSHING` or while a RECLAIM or a
base change waits for the media (asking for the flush when `written >
flushed`; the writes' completions retry it otherwise), and fails with
`FAILED` when nothing can free (section 6.1). The store's own flush does
not wait for `flush_target`: a SYNC may name the STORE held for the slot,
whose write the SYNC's flush would wait for. RECLAIM, a capture's end,
`base_set`, a superblock write and a flush retry held STOREs. Growth also
rewrites the superblock after the `FALLOCATE` completes.

Base files (decision 43): `store_pack` holds a RESTORE, or a PUBLISH of a
snapshot that is neither the latest capture nor the current base, before
packing it when the replica's role after the transaction is FULL:
`base_wanted` reports the id and the transaction's sequence,
`base_begin` clears every entry's `next_offset`, `base_record` merges a
file record by request number (greater: `current` replaced, `sequence
0`, the file offset; equal: the ops must agree, else EINVAL; lower: the
log's record stays and the entry is not covered; a full table is ELIMIT),
`base_end` marks the load done and `base_resume(OK)` packs the
transaction, whose PUBLISH or RESTORE then applies the base: a RESTORE
deletes the entries the file lacks (a WITNESS empties the table without a
file) and rebuilds every `retained` from `[log_begin, log_end)`;
`client_base` becomes the transaction's sequence lowered below the record
of any entry the file does not cover, so that `client_base + 1` never
frees a record only the log holds; `client_base_id` the file's. A PUBLISH
of the latest capture applies the capture's offsets the same way without
a load (`base_set`). `base_resume` with any other status fails the held
STORE with it and fences. Recovery loads the anchor's file after its scan
through the same `base_begin/record/end`, which applies it at once when
no transaction waits.

## 7. Engine internals

### 7.1 vsr_io_poll

```
 vsr_io_poll(io, now, ops, capacity, count, flags)
   io->now = now; wake_pending = 0
   while deadlines_pop(now, kind, index): dispatch (LINK/DIAL ->
       vsr_io_links_deadline, FLUSH/SYNC -> store, STREAM ->
       vsr_io_streams_deadline,
       CAPTURE -> nothing: the module's poll below retries its waits,
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
| COMPLETE (rail) | by `vsr_io_streams_op_kind(id)`: SERVE -> `streams_served`; DATA -> `streams_data_done`; else `links_handshake_done`; a stale id is `EINVAL` (decision 93) |
| STREAM_OPEN / WRITE / CLOSE | `streams_open` / `write` / `close`; `ELIMIT` from open and `AGAIN` from write (the write queue is full) stop; no RELEASE op follows these events' leases: STREAM_END and STREAM_WRITTEN release them (decision 94) |

A replica in STOPPED refuses every CORE event with `EINVAL`.

### 7.3 Op routing

For each op the core emits, by `op.type`; within one update every LOAD is
routed first, then every STORE, then SYNC and RECLAIM, then the other ops
in emission order (decision 51):

| Op | Route |
| --- | --- |
| SEND | `vsr_io_links_send(replica, id, message, arg)`; `RETRY` returned -> COMPLETE(RETRY) queued; OK -> queued, completed later through the replica's completion ring (decision 83) |
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
2. `vsr_io_links_prepare`: listener setup on the first call (SOCKET
   DIRECT into an engine slot, BIND, LISTEN, then a plain multishot
   ACCEPT, one chain per listen address, LINKed with SKIP_SUCCESS on all
   but the ACCEPT; decision 72), orphan closes, then per link: the dial's
   SOCKET or CONNECT, the EXTERNAL preamble receive, the control bytes
   (preamble, HELLO) and coalesced sends, receive arming and re-arming
   (not while a stream link is paused: then the CANCEL of its receive on
   the shutdown slot, decision 99), the teardown (SHUTDOWN, CANCEL of the
   receive, CLOSE of the slot, or a
   plain CLOSE of a raw descriptor, preceded by a CANCEL of a CONNECT or
   preamble RECV still in flight on it).
3. `vsr_io_streams_prepare`: file chunk reads (slot kind STREAM, one
   completion each; the same `count` continues the array).
4. Per replica: `vsr_io_store_prepare` (open/create steps, header writes,
   one record write, superblock write, flush, one cold read), then
   `vsr_io_snapshots_prepare` (the directory open, clients file opens,
   reads, writes, STATX, fsyncs, closes, renames, unlinks, at most
   `VSR_IO_SNAPSHOT_FILEOPS` in flight; the same `count` continues; an
   outcome it decides arms the replica's CAPTURE deadline at `now`, W6).
5. `*deadline_ns = vsr_io_deadlines_earliest()`.

### 7.5 vsr_io_complete

For each record: `vsr_io_slots_resolve` (foreign owner tag or stale
generation: dropped, `stats.frames_rejected` untouched, a counter in the
slot table); then by `slot.kind`: LISTEN, CONNECT, RECV, SEND, SHUTDOWN ->
`vsr_io_links_complete`; WRITE, FLUSH, SUPER, LOAD, FILE (owner replica)
-> `vsr_io_store_complete`; CLIENTS -> `vsr_io_snapshots_complete(io,
slot.owner, slot, cqe)` (every record of the snapshot module has that
kind, owner = replica index); STREAM -> `vsr_io_streams_complete`. The
module consumes the slot (`vsr_io_slots_consumed` with the record's MORE,
or `vsr_io_slots_free` for a zero-copy send refused before the kernel took
it), since it knows what each completion means (decision 74). Nothing
steps a core here; every effect is queued for the next poll, and a
time-based effect uses `io->now`, the last poll time.

### 7.6 Leases

`vsr_io_lease_alloc` takes a free lease entry (and thus a region), records
the slab or pin, bumps the generation, and returns the index; the id is
built by `vsr_io_lease_id`. A MESSAGE event's lease is allocated at
delivery with one slab reference; a LOAD completion's lease at completion
with a slab reference (cold) or a ring pin (hot). RELEASE frees the region,
drops the reference or pin, and marks the entry free; a stale id is
`EINVAL` in the routing, reported through `stats` and ignored.

The caller's leases on engine-level events (STREAM_OPEN's request,
STREAM_WRITE's buffers) get no RELEASE op: the STREAM_END of the stream
and the write's STREAM_WRITTEN say the bytes are no longer read
(decision 94).

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
`vsr_io_streams_shutdown` (END `CANCELLED` to every stream; a stream
whose link the link shutdown closed first is CANCELLED already, decision
101, so the order is free); `closed` when every slot is free and every
link FREE; `vsr_io_deinit` then unregisters the pool region and the
buffer ring.

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
| CONNECT | to `addr` of `length` bytes; result 0, or `-ECONNREFUSED` (no listener), `-EHOSTUNREACH` (partitioned, after `connect_timeout_ns`); a foreign family as the kernel orders it: an `AF_INET` socket answers `-EINVAL` below a `sockaddr_in`'s length, then `-EAFNOSUPPORT`, an `AF_UNIX` socket `-EINVAL` (BIND likewise) |
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
| `uring_refusals` | integration | `INTEGRATION_TESTS` (skips without a ring or seccomp) | `vsr_io_uring_init` on emulated kernels, each in a forked child under a seccomp filter: no io_uring (`ENOSYS`) and io_uring disabled (`EPERM`) pass through; Linux 6.1 (setup refuses `NO_SQARRAY`, old features), 6.11 (no `MIN_TIMEOUT`), 6.14 (no `READV_FIXED`) and a kernel without networking are `-ENOSYS`, answered by a supervisor thread through `SECCOMP_RET_USER_NOTIF`; no descriptor stays open; a nonexistent SQPOLL CPU stays `-EINVAL` (decision 104) |
| `executor_conformance` | integration | `INTEGRATION_TESTS`; runs over the sim and, when `/dev/null` is writable and a ring can be created, over io_uring (skipped with exit 77 otherwise) as the default, SQPOLL and NAPI rings, then over the sim and the default ring again through the fault-injecting wrapper | Section 8 |
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
sockets. It keeps every completion order a caller relies on (per
`user_data`, per provided buffer, per LINK chain), shortens file results
only in whole units of the request's alignment so the rest of an
`O_DIRECT` transfer stays aligned, and with every rate zero is
transparent but for the one `user_data` it reserves:
`executor_conformance` runs every scenario through it at rate zero over
both executors, and one more scenario at nonzero rates. Its header states
what callers may assume of the faults it injects.

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
| `vsr-io.h` | `cache_bytes` rule: two headers and a block of slack besides the two records | 69 |
| `store.h` | Extents carry their segment, a header flag and their last sequence; the store keeps `file_head`, `superblock_dirty`, `growth` and the log path; the check rule adds the executor-length bounds | 69, 70 |
| `vsr-io.h` | At most 8 listen addresses (`vsr_io_layout` is ELIMIT beyond); `vsr_io_authorize` requires a node already set (EINVAL), revoking closes links only once nothing names the node, `vsr_io_node_clear` removes the node's authorizations; the HANDSHAKE op is emitted after the preamble was exchanged on the raw descriptor | 72, 73 |
| `link.h` (internal) | Receive side of `vsr_io_link`: `held[VSR_IO_LINK_HELD]` runs (`vsr_io_run`) behind the partial, `retry`; `vsr_io_links.retries_due` and `reassembled`; `vsr_io_links_poll` also retries held bytes | 75, 76 |
| `store.h` | `VSR_IO_SEGMENT_FREEING`; `vsr_io_load_ref` and `load_refs`, the pending load's resolved records and read state, `cold_slab`; `reindexed`, `restored`, the `base_*` fields and `base_slot`; the client entry's `next_offset`; a LOAD completion's lease carries every OK result; `release` only unpins | 77, 78, 79, 80 |
| `link.h`, `stream.h` (internal) | Send side of `vsr_io_link`: unwrapped 64-bit ring counters (`header_*`, `vsr_io_send.header_*`), the build (`vec_count`, `build_bytes`), `nodelay_set`, `VSR_IO_STAGE_NODELAY`; `vsr_io_links_send` contract (RETRY only when the engine completes at once); `vsr_io_links_open_stream` and `vsr_io_links_send_frame` contracts; `vsr_io_streams_frame` returns whether the frame was consumed; `vsr_io_streams_sent` at every send result and NOTIF | 82, 83, 84, 85 |
| `store.h` | `vsr_io_segment.run` (the header's run, for the successor rule); `vsr_io_recovery` reshaped for the scan (the pool slab and executor slot of the read in flight, the superblock copy, the chunk's offset and the position judged, the sweep mode, the resume offset); the base-load kinds include the recovery's; a LOAD slot's `sub` tells a recovery read from a cold load's | 88, 89, 90 |
| `codec.h` | `vsr_io_codec_load_region` also holds the recovered row's manifest bytes, copied into the region | 91 |
| `vsr-io.h` | Bulk streams: the engine emits no RELEASE for a STREAM_OPEN or STREAM_WRITE lease (STREAM_END and STREAM_WRITTEN end them; a WRITTEN follows every queued write, also on an early end); `vsr_io_submit` refuses a STREAM_WRITE with `AGAIN` while `stream_window` writes are queued | 94, 95 |
| `stream.h` (internal) | Units are chunks (`vsr_io_stream_unit` states READING, READY, SENT, DATA), the write queue (`vsr_io_stream_queued_write`, `writes` rings), handles and op ids (`VSR_IO_STREAM_OP_*`, `vsr_io_streams_op_kind`, `vsr_io_streams_handle`), `generation`, `ended`, `aborted`, `link_gone`, `end_*`, `send_end`, `closing`; `vsr_io_streams_deadline`; `vsr_io_streams_size` is ELIMIT beyond 65535 streams or window | 93, 95, 96, 97 |
| `snapshot.h` (internal) | `vsr_io_snapshots_serve` returns OK to serve or the END status; `vsr_io_snapshots_stream_data` takes the DATA op id; `stream_end` is told for both sides of a library stream; weak stubs in stream.c until snapshot.c | 98 |
| `link.h` (internal) | `vsr_io_link.recv_paused` and `recv_cancelled`; the shutdown slot also carries a paused receive's CANCEL (the teardown waits for it); a stream link whose frame waits is not closed for its held runs but paused, and its receive's `-ECANCELED` is not a loss | 99 |
| `stream.h` (internal) | `vsr_io_stream_unit.sequence` (was `reserved`) and `vsr_io_stream.chunks`: a DATA op id's unit field is the chunk's sequence, not its slot | 100 |
| `stream.h` (internal) | `vsr_io_streams_link_lost` ends a stream CANCELLED while the link module shuts down; `vsr_io_stream.closed`: `vsr_io_streams_close` is OK once per accepted stream until its END op (also after an end under the caller, a read failure included), EINVAL for a status outside enum vsr_io_status; `vsr_io_streams_write` is EINVAL for a caller stream's FILE range on an engine file slot; `vsr_io_streams_data_done` re-arms the requester's clock | 101, 102, 103, 97 |
| `vsr-io.h` | Bulk streams: CANCELLED on the closing engine whichever shutdown runs first; STREAM_CLOSE once, OK until STREAM_END also after an end under the caller, its status an enum vsr_io_status; STREAM_WRITE EINVAL once the stream ended; a FILE range on one of the engine's slots EINVAL | 101, 102, 103 |
| `vsr-io.h` | `vsr_io_uring_init` names its refusals: `-ENOSYS` (no io_uring, or a kernel older than the baseline, including one whose setup refuses a flag with `EINVAL`), `-EPERM`, `-EINVAL` | 104 |
| `snapshot.h` (internal) | The module's structures as implemented: `vsr_io_snapshots_size`/`init` take the engine limits (`stream_window`, `streams`) and `max_clients`; `vsr_io_snapshots_region_bytes`; a registry entry carries the file size, the job and its step, the record in flight, the kept and transient slots, `on_disk`, `sync_failed`, `job_next`, the reserved `lease` and the copied `result`; the writer, the reader, the chunk ring, the serves, the file-operation table, the directory slot and `pending_base`; the ops are `RETRY` without an engine lease; an OK CAPTURE or FETCH completion carries the lease; a DROP of an id the registry lacks is taken; a file with a CAPTURE outstanding is not served; the weak hook stubs left stream.c | 105, 106, 107, 108, 109 |
| `store.h` | `vsr_io_recovery.chain_resume` (the boundary at which the chain resumes past a block's dead tail); `VSR_IO_SEGMENT_FLUSHING`, `freeing_flush` and `flush_own` (a freed slot waits for the flush after its superblock write, which the store issues without waiting for a SYNC's target); `reclaimed`, `client_base_floor` and `client_base_pending` (the floor terms bounded by the sequence on media); `snapshot_clients` reports a record freed under the base as the base file's (sequence 0, the base offset); `vsr_io_store.replica`, the embedding replica, set by init; `vsr_io_recovery.record_run` (was `reserved`), the run of the last replayed record | 110, 111, 112, 113, 116 |
| `wire.h`, `codec.h` (internal) | Clients file format 2: `vsr_io_wire_clients_trailer` is 16 bytes (`crc`, `reserved`); `vsr_io_codec_put_clients_trailer` and `get_clients_trailer` take the running CRC32C of the header and the records (each without its CRC), which the trailer's crc extends over its magic and count | W4 |
| `snapshot.h` (internal) | `vsr_io_snapshot.transferred` (was `reserved`: a FETCH's own stream wrote the file) and `dir_retried` (a SYNC retried the directory's open); the writer's and reader's `digest`; `base_track` releases no slot of an entry with an op in progress; a missing base file is `CORRUPT` on any role; outcomes decided in `vsr_io_snapshots_prepare` wake the loop at once | W1, W2, W3, W4, W5, W6 |

`vsr-sim.h` and `vsr-client.h` are unchanged.

## 11. Open questions

- The per-stream inactivity timeout reuses `handshake_timeout_ns`; a
  dedicated option may be wanted once real transfers are measured.
- `versions` is sized like the op ring, which is conservative; a smaller
  bound tied to the core's uncommitted suffix needs a core-side statement
  of that bound.
- Whether NEW and JOIN should refuse a directory that holds stray
  `clients-*` files, or ignore them; the executor has no directory
  listing, so the store refuses only through the log (decision 92).
- A RECLAIM above the sequence on media takes effect only as flushes
  advance it (decision 112); a core that reclaims far ahead of its SYNCs
  in FDATASYNC mode makes the store flush on its own when it runs out of
  slots, which is correct but not the cheapest cadence.
- Decision 50's residual reaches freed slots: media corruption of
  records on media that no durable floor covers makes recovery return an
  older row than the one freeing relied on (112). Missing ops make it
  `CORRUPT` (114), but a CLIENTS record lost that way leaves the client's
  older record (from the base file) in the table unnoticed, so a request
  it completed could run again. Bounding freeing by the durable floor on
  media would close it in DURABLE mode (not in REPLICATED mode, which
  acknowledges nothing durable); a cheap recovery check of the client
  base against the start segment is another candidate.
- A crash during the creation itself (before the RECOVERY load's
  `NOT_FOUND`) can leave a log without a valid superblock, which the next
  open reports `CORRUPT`; the directory is not fsynced after the create
  either (the engine or the snapshot module should).
- A paused stream link (decision 99) still holds what the kernel
  delivered between the frame that blocked and the CANCEL taking effect,
  within the `VSR_IO_LINK_HELD` (8) runs of decision 76: with page slabs a
  burst above about 32 KiB reaped at once (a socket buffer holding more,
  a multishot receive delivering up to its per-wake limit) closes the
  link `-ENOBUFS` as before, and the transfer ends RETRY. Larger slabs
  raise the bound in bytes; giving stream links a held capacity of the
  pool's worth of runs (memory per link) would make the pool the only
  bound. The unit harness bounds the burst with its socket buffer
  (`world.inbox_limit`); a burst past the bound is not modelled as a
  passing test.
- A source throttled by a paused requester makes no send progress and is
  bounded by the inactivity timer like a dead one; a requester slower
  than `handshake_timeout_ns` per window ends RETRY at the source.
- A STREAM_WRITE or STREAM_CLOSE racing the stream's STREAM_END still
  unread in the forwarded ring is EINVAL once the stream was freed (a
  lost source is freed right after its END op; decision 102 makes CLOSE
  OK only until then), so a caller must take EINVAL for a stream whose
  STREAM_END it has not consumed yet as benign; keeping the stream until
  its END op is dequeued would need the ring to report dequeues.
- A source whose linger ends at its inactivity timer closes first
  (decision 96's bound): a paused requester then reads the rest and the
  EOF when it receives again, and a multishot receive that posts data
  and the EOF in one completion batch while a frame waits for a window
  unit closes the link `-EPIPE` with the END frame still held, ending
  the transfer RETRY at the requester while the source reported OK. Only
  a requester slower than `handshake_timeout_ns` per window gets there.
- Where Linux 7.2.6 and the simulation answer differently within the
  contract (the conformance and smoke logs print these, recorded and not
  checked), the engine depends on neither answer today, so the
  simulation is left as it is:
  - A CANCEL of a disk record: the simulation never cancels disk work
    (`-EALREADY`, and the record completes); the kernel cancels a write it
    queued for a worker and had not started (a 1 MiB buffered write on
    btrfs: the CANCEL 0, the write `-ECANCELED`). The engine cancels only
    socket records (receives, accepts, connects, by `user_data`); code
    that ever cancels a disk record must take `-ECANCELED` as well, which
    the simulation would not exercise.
  - CANCEL `BY_FD` without `ALL`: the simulation cancels the earliest
    match, the kernel cancelled the later of two receives (its hash
    order); the contract leaves it unspecified and the engine never
    cancels by descriptor.
  - `update_buffer` of a region a pending record uses: `-EBUSY` in the
    simulation, 0 on the ring, the kernel keeping the old registration
    until the record completes (decision 65); the engine registers its
    pool region once, at init.
  - Direct I/O alignment is the filesystem's: btrfs and tmpfs serve a
    misaligned `O_DIRECT` read buffered, the simulation refuses it with
    `-EINVAL` like ext4 and XFS; the engine aligns everything, and the
    `odirect_*` conformance rows skip on such a filesystem (they need a
    build tree on ext4 or XFS to run over the ring).
  - Short-send lengths depend on socket buffers (6144 of 8 MiB with a
    4096-byte `SO_SNDBUF` on the ring, 262144 in the simulation); only
    shortness is contract.
- The pool's reserve (`replicas + 1` free slabs, decision 54's floor) is
  smaller than what internal holders keep for good: every established link
  holds its send slab for its lifetime (`link_send_slab`, released only at
  close), so with two peer links an idle engine of two replicas has one
  free slab once the ring holds the rest, and none with three. The
  snapshot module's staging slabs (the writer's, its cold slab, the
  reader's: up to three per replica) and the store's cold slab compete for
  what is left with a new stream link's send slab. Measured in the unit
  harness with `caller_slabs = 0` (two engines, two established peer
  links): the FETCH took the last free slab for its reader, its stream
  link could not send its HELLO, and the fetch ended `RETRY` at the
  handshake timeout, then again on every retry, while the ring held 29
  idle slabs. Recommendation for engine part 2 (`vsr_io_init` and the
  minimum-slabs rule in engine.c, pool.c unchanged): reserve `links + 4 *
  replicas + 1` slabs (a send slab per link, the store's cold or recovery
  slab and the module's three per replica, one for a reassembly) and raise
  the minimum-slabs rule by the same difference, `links + 3 * replicas`,
  so the ring keeps its share; alternatively release a link's send slab
  when its send ring drains (link.c), which leaves `4 * replicas + streams
  + 1`. The unit harness keeps `caller_slabs = 8` meanwhile; the module
  answers `RETRY` at once when a slab is missing at an op's start.
- A fetch whose verification or local write failed keeps receiving the
  stream until its END (the chunks are completed and dropped at once); the
  requester has no early abort of a library stream. The files are small.
- A capture whose caller half fails after the file was complete has
  already moved the store's `last_capture` to its id, whose file is then
  unlinked; harmless, since the core never publishes an id whose CAPTURE
  failed, and the next capture moves it again.
- The capture writer reads one cold record at a time (one READ per record
  not in the ring); batching adjacent records of one segment would save
  reads for large tables.
- A FETCH whose bytes fail verification completes `CORRUPT`, and the core
  latches `VSR_FAILURE_SNAPSHOT` on any `CORRUPT` snapshot completion
  (core.c `completion_failure`), so a clients file corrupted at one source
  fails every replica that fetches it, though their own state is intact;
  docs/vsr-api.md's FETCH row ("discover another valid offer; corruption
  fences snapshot state") reads either way. Reporting a remote file's
  corruption as `FAILED` (a new discovery) would confine it to the source,
  which finds it at its own next load. The source serves its file without
  reading it through the reader.
- The capture writer reads a file-only record through `store.base_slot`
  as it is when the read is issued, at the offset the table snapshot
  took from the base of that moment; a PUBLISH or RESTORE packed during a
  capture would point the read at another file (`CORRUPT`, latched). The
  core's checkpoint stages never overlap a capture with a base change;
  keeping the capture's base entry open and reading through its own slot
  would remove the dependency.
- A serve refused because the stream index's previous serve is still
  closing answers `FAILED` (the requester's FETCH fails and rediscovers);
  `RETRY` would say what it is. A read reporting more bytes than asked but
  within the slab's room is parsed as data (bytes after the trailer:
  `CORRUPT`), not reported as the I/O error it is.
