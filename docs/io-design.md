# I/O layer design

This document records the design of the Linux io_uring host for the VSR core
and the reasoning behind every decision, so that a reader can reconstruct the
design discussion from it. The contract is [include/vsr-io.h](../include/vsr-io.h);
the deterministic simulation is [include/vsr-sim.h](../include/vsr-sim.h) and
the client bookkeeping is [include/vsr-client.h](../include/vsr-client.h). The
core they serve is described in [DESIGN.md](../DESIGN.md), the
[protocol contract](protocol.md), and the [adapter reference](vsr-api.md).

## 1. Purpose and scope

The core in `vsr.h` performs no I/O: it consumes events and emits operations.
This layer executes those operations on Linux with io_uring: network transport
between replicas, an indexed log store on disk, time, and the plumbing that
lets an application execute its own operations. It is a generic host for the
core, not an application. The application that motivated it, a replicated
SQLite database, is deliberately out of scope: the generic design comes first
and is tweaked afterwards against that application's real needs.

The target is a high-performance storage engine. `VSR_REPLICATED` matters more
than `VSR_DURABLE`, and where throughput and latency conflict the choice is a
tunable with a documented default rather than a fixed policy.

The principles of the core carry over unchanged, one level down:

| Principle | In this layer |
| --- | --- |
| Single-threaded, asynchronous, event-driven | One engine per thread; no locks on any hot path; threads share nothing |
| Sans-IO planners wherever possible | Codec, handshake, link state machine, store layout, timers and streams are deterministic planners that emit work records and consume completion records |
| Cache-friendly layouts | Contiguous arrays and slab indexes, fixed capacities, completions dispatched by table index rather than lookup |
| Zero-copy where possible | Received bytes feed the core, the log and forwarded sends without a user-space copy; zero-copy sends from registered buffers |
| Caller-provided memory, no malloc | Every region is sized by a layout function and supplied by the caller; pools are registered with the kernel |
| No callbacks | The application boundary is the core's own ops-and-events vocabulary |
| No hidden clock or randomness | Time and entropy come from the executor, so a simulation supplies both |

## 2. Platform baseline

The baseline is Linux 6.18 (decision 53). The executor drives io_uring through the `io_uring_setup`, `io_uring_enter`
and `io_uring_register` syscalls over a vendored copy of the kernel's UAPI
header, `src/io/uapi/io_uring.h` (decision 52): the library depends on
libc alone and the build needs no io_uring library or header on the
machine. The vendored header tracks Linux 7.2, so every opcode and flag the
executor uses is declared regardless of the system's `linux-libc-dev`.

The original development machine ran kernel 7.2.6; a probe program against
that kernel confirmed:

- All 65 opcodes in the header are reported supported, up to and including
  `RECV_ZC`, `EPOLL_WAIT`, `READV_FIXED`, `WRITEV_FIXED`, `PIPE`, `NOP128` and
  `URING_CMD128`.
- Every feature bit up to `NO_IOWAIT` is set.
- Ring setups verified by creating them: `SINGLE_ISSUER|DEFER_TASKRUN`,
  `SQPOLL`, `IOPOLL` and `IOPOLL|HYBRID_IOPOLL`, `SQE128|CQE32`,
  `REGISTERED_FD_ONLY|NO_MMAP`, `SUBMIT_ALL|R_DISABLED`,
  `COOP_TASKRUN|TASKRUN_FLAG`.
- A provided-buffer ring with `IOU_PBUF_RING_INC` (incremental consumption).
- User-provided ring memory through `NO_MMAP`. The executor does not use
  it: the kernel finishes a closed ring asynchronously and still writes
  the ring words then, so ring memory inside the caller's region would be
  written after deinit had returned it; the rings are the kernel's pages,
  mapped from the ring descriptor.

The executor's smoke test (`tests/integration/uring_smoke`) then passed
completely on a 6.18 kernel, whose one difference that matters is the
vectored zero-copy send from a registered buffer: `SEND_ZC` accepts
`IORING_SEND_VECTORIZED` together with `IORING_RECVSEND_FIXED_BUF` only from
7.x, so the executor issues that send as `SENDMSG_ZC` with a fixed buffer
and an iovec, supported since 6.15, on every kernel.

Decision: no feature probing beyond one check at initialization, and no
fallbacks. `vsr_io_uring_init` reads the feature bits the ring reports and
the opcode table (`IORING_REGISTER_PROBE`) once and refuses an older kernel
with `-ENOSYS`. The design relies directly on: multishot accept and recv,
provided-buffer rings with incremental consumption, zero-copy send from
registered buffers (`SEND_ZC` with `IORING_RECVSEND_FIXED_BUF`, and
`SENDMSG_ZC` for the vectored form), vectored fixed-buffer disk reads and
writes, the extended wait argument with minimum-timeout batching
(`IORING_FEAT_MIN_TIMEOUT`), bind and listen as ring operations, direct
descriptors and peek receives. Registered wait regions are not needed: the
wait argument is a stack struct per call. Zero-copy receive (`RECV_ZC`)
requires NIC support for header and data split and is deferred. Splice and
pipes are not used (decision 39).

Machine facts that informed defaults: 8 logical CPUs, an NVMe device
reporting 512-byte logical and physical blocks, and 15 GiB of RAM. The store
block size therefore defaults to 4096 independently of the device.

## 3. Architecture

### The executor seam

The seam between deterministic code and the kernel is the executor, a
virtual io_uring: a submission record (`struct vsr_io_sqe`, 64 bytes)
describes one kernel operation, a completion record (`struct vsr_io_cqe`, 16
bytes) reports its result, and `struct vsr_io_executor_ops` is the interface
for submitting a batch, waiting, reaping a batch, reading the clock, drawing
entropy, and registering files, buffer regions and buffer rings. The
operation set is the subset the library needs, and where a flag is named
after an io_uring flag its semantics are io_uring's. Two implementations
exist: `vsr_io_uring_*` over a real ring, and the simulation in `vsr-sim.h`.
Library code is byte-identical over both.

The seam is an interface at run time, not a compile-time switch. The
interface is batch-shaped, so the cost is one indirect call per submit and one
per reap, with every per-record path direct inside the executor. An indirect
call costs a few nanoseconds when predicted and tens under retpoline, against
a syscall and submission-queue preparation that cost far more per batch, so it
lands well under a percent. The two conditions that keep it that way are no
per-operation callback and no per-operation copy beyond the record itself. If
a profile ever disagrees, selecting a backend statically or devirtualizing
with LTO is a mechanical change that touches no contract.

```
              ┌────────────────────────────────────────────────────────┐
              │ application core: sans-IO state machine               │
              │ (no I/O, no clock, no allocation; arenas from caller)  │
              └──────────▲───────────────────┬───────────────────┬─────┘
   forwarded ops:        │                   │ caller events:    │ the caller's
   APPLY, READ_READY,    │                   │ COMPLETE, REQUEST │ own records
   SNAPSHOT_*, REPLY,    │                   │ READ, CHECKPOINT  │ (its owner
   RELEASE(caller lease) │                   │ STOP, STREAM_*    │  tag)
┌────────────────────────┴───────────────────▼─────────────────┐ │
│ engine: struct vsr_io (one per thread)                       │ │
│                                                              │ │
│  replica[g] ──┐    links          store        deadlines pool│ │
│   vsr core    │    codec + link   log layout    set     slabs│ │
│   (vsr.h)     │    state machine  + indexes                  │ │
│               │                                              │ │
│  executes: SEND, network REPLY, LOAD, STORE, SYNC, RECLAIM,  │ │
│            RELEASE(engine leases); forwards the rest upward  │ │
└──────────────┬───────────────────────────────▲───────────────┘ │
   submission  │ batch                         │ completion       │
   records     ▼                               │ records          ▼
┌──────────────────────────────────────────────┴─────────────────────────┐
│ executor: vsr_io_executor_ops (submit_and_wait / reap / now / random)  │
├────────────────────────────────────┬───────────────────────────────────┤
│ vsr_io_uring_*                     │ vsr_sim_*                         │
│ record -> SQE, CQE -> record       │ virtual disks, network, clock,    │
│ real clock, real ring              │ faults; one world, N handles      │
└────────────────────────────────────┴───────────────────────────────────┘
```

### A loop-less library

The library owns no loop. Its primitive entry points are `vsr_io_complete`,
`vsr_io_poll`, `vsr_io_submit` and `vsr_io_prepare`, and `vsr_io_run` is a
ready-made loop built from them for callers that have none. One iteration:

```
 loop {
   ── 1. reap ───────────────────────────────────────────────────────────
   now = ex->now(ex)
   n = ex->reap(ex, cqes, cap)
   for each cqe:
       VSR_IO_OWNER(cqe.user_data) == engine ? vsr_io_complete(io, &cqe, 1)
                                             : app_complete(app, &cqe)
       // vsr_io_complete only translates and queues: a recv becomes a
       // MESSAGE event, a write or NOTIF becomes a COMPLETE event.
       // No core step happens here.

   ── 2. run until quiescent ────────────────────────────────────────────
   do {
       vsr_io_poll(io, now, ops, cap, &k, &flags)
           // steps every core with its queued events plus TIME(now),
           // coalescing via vsr_step_many; ops the engine owns go to its
           // internal link and store queues; forwarded ops are returned
       m = app_step(app, ops, k, events)      // pure; may emit events
       vsr_io_submit(io, events, m, &consumed) // queued for the next poll
   } while (k > 0 || m > 0 || (flags & VSR_IO_POLL_MORE))

   ── 3. prepare ────────────────────────────────────────────────────────
   vsr_io_prepare(io, now, batch, cap, &count, &deadline)
           // flushes internal queues into records: sends coalesced per
           // link, STOREs packed into one write (+ one flush if a SYNC
           // is pending), recv/accept re-armed, recycled slabs provided;
           // returns the earliest engine deadline (no engine timer records)
   deadline = min(deadline, app_prepare(app, batch))   // caller's records

   ── 4. block once ─────────────────────────────────────────────────────
   ex->submit_and_wait(ex, batch, count, want, wait_min_ns, deadline)
 }
```

Why poll steps while prepare only flushes: the iteration is the batching
unit. Everything packed, coalesced or armed during one iteration goes to the
kernel in one submission, and the `want` and `wait_min_ns` arguments of step 4
are the throughput-versus-latency knob on the completion side: a longer wait
lets more STOREs share one write and more sends share one `sendmsg`. Feeding
completions directly into a core step would spread that work across the
iteration and put a full blocking wait between an APPLY completion and the
step that consumes it. The clock is read once per iteration from the executor
and passed to both the caller and the engine, so simulated time reaches the
core through exactly the path real time does.

### Three loop forms

| Form | Who owns the loop | Mechanism |
| --- | --- | --- |
| Ready-made | The library | `vsr_io_run` with `struct vsr_io_hooks`: complete, step, prepare. These are the only callbacks in the header and exist only for this convenience |
| Caller-owned | The caller | The four primitives over the library's executor; the caller pushes its own records into the same batch and dispatches completions by owner tag. This is the shared-ring case, by construction, with no extra mechanism |
| Pre-existing ring | The caller | The caller implements `vsr_io_executor_ops` over its ring; the engine never sees the difference |

An earlier draft treated "loop ownership" as the design question, with a
separate shared-ring embedding mechanism to be added later. Making the
executor the seam removed the question: every form is one of the three above.

### Engines and threads

One engine per thread, with its own executor and ring. A group lives on
exactly one engine. Scaling out is N engines on N cores that share nothing;
cross-engine traffic is an executor wake plus an application-level queue,
never a lock on the hot path; `MSG_RING` is a possible later executor
operation for the same purpose. Each engine holds its own links to each peer
node, so two engines on one machine hold two connections to the same peer.

### Multi-group scope

The same set of nodes may run several independent groups, each with its own
log. The API keeps that shape because it costs one object split now and would
be painful to retrofit: one `struct vsr_io_replica` per group on an engine,
links keyed by node and shared across groups with the envelope's cluster ID as
the demultiplexer, one store directory per group so groups can move between
disks, and a node identity distinct from per-cluster replica IDs. The
application maps a cluster and replica pair to a node with `vsr_io_authorize`,
which is trivial when replicas are numbered the same way in every group. The
expected count is a handful, probably one, so nothing is tuned for thousands:
no cross-group batching beyond the per-link send coalescing, no shared store.

## 4. Ownership model

The rules of `vsr.h` apply unchanged: an accepted event transfers a lease
over its whole graph until `RELEASE`, and an emitted op pins its graph until
its completion. The engine adds three things.

- Slabs. The payload pool is a caller-supplied, page-aligned region divided
  into fixed-size slabs, registered with the executor and handed to the kernel
  through a provided-buffer ring. A slab is held by core leases, in-flight
  records and, through the store's own tail buffers, nothing else: the store
  copies records into its tail buffers, so receive slabs are held only by
  leases and sends. A slab returns to the ring when its references drop to
  zero.
- Leases. Caller lease IDs must have `VSR_IO_LEASE_ENGINE` clear; the engine's
  own leases have it set and never reach the caller. `RELEASE` ops therefore
  route to whoever owns the buffer.
- user_data. The top byte is the owner tag, `VSR_IO_OWNER`; the engine uses
  `vsr_io_options.owner` and the caller any other value, so one executor
  serves both in one loop. Below the tag the engine packs a kind, a slot index
  into a fixed per-engine table, and a generation that rejects stale
  completions.

```
 user_data, 64 bits
 ┌───────┬───────┬──────────────┬──────────────┐
 │ owner │ kind  │ index        │ generation   │
 │ 8     │ 8     │ 24           │ 24           │
 └───────┴───────┴──────────────┴──────────────┘
 owner = engine's tag or the caller's; the caller owns the layout below its tag
 engine kinds: accept, recv, send, notif, write, fsync, timer, cancel, ...
 index = slot in a fixed per-engine table; generation rejects stale CQEs

 process
 ┌───────── thread 0 ─────────────┐   ┌───────── thread 1 ─────────────┐
 │ engine 0 on ring 0             │   │ engine 1 on ring 1             │
 │ groups {A, C, E}               │   │ groups {B, D}                  │
 │ own links to nodes 2 and 3     │   │ own links to nodes 2 and 3     │
 └──────────────▲─────────┬───────┘   └───────▲─────────┬──────────────┘
                │         └── executor wake ──┘         │
                └────────── + application queue ────────┘
 a group lives on exactly one engine; engines share nothing
```

### One PREPARE through the mechanics

```
 peer TCP ──▶ RECV multishot CQE {buffer_id -> slab S, length}
                │
                ▼  frame decoded in place, no copy
   link decoder ──▶ MESSAGE event {headers built over S,
                                   lease L = ref on S}        S.refs = 1
                │
                ▼  (step 2, inside vsr_io_poll)
   vsr core ──▶ STORE {APPEND: entry bodies are spans into S}  pin S
            ──▶ SEND  {PREPARE_OK}
            ──▶ APPLY {entries: spans into S}                   pin S
                │
   ┌────────────┼──────────────────────────────────────────────────┐
   │ STORE ──▶ store: record header + bodies copied into the tail  │
   │           buffer (block-aligned, registered); indexed;        │
   │           COMPLETE(STORE, OK) queued at once (readable)       │
   │           (prepare, step 3) ──▶ WRITE FIXED_BUFFER of the      │
   │           packed range; CQE only advances written/durable     │
   │ SEND  ──▶ link: coalesced with other sends to that peer        │
   │           ──▶ SEND ZERO_COPY|VECTORED {frame hdr, body spans}  │
   │           CQE(result) then CQE(NOTIF) ──▶ COMPLETE(SEND, OK)   │
   │ APPLY ──▶ returned by vsr_io_poll ──▶ app_step reads spans in S│
   │           ──▶ vsr_io_submit(COMPLETE(APPLY, results))          │
   └────────────────────────────────────────────────────────────────┘
                │
   later: core emits RELEASE(L) ──▶ S.refs--
          S returns to the provided-buffer ring when refs reach 0
```

### Receive path

Every link runs a multishot recv with buffer selection from the engine's
provided-buffer group. A frame that lies within one slab is decoded in place
and is zero copy all the way to the core, the log and any forwarded send. A
frame that straddles slabs is reassembled once into a fresh slab; large slabs
plus incremental consumption make that rare. When the pool is exhausted the
engine stops providing buffers and receiving pauses, which is the correct
backpressure toward peers: TCP flow control does the rest.

A frame therefore fits one slab, and `slab_bytes` less framing is the
cluster's largest message and, through the core's `message_bytes`, its
largest command. The first application replicates page diffs or B-tree
changes rather than whole pages, so a cap of a few megabytes is a policy
rather than an obstacle, and a pool of a few slabs of that size with
incremental consumption costs little memory. Letting a body span slabs,
delivered as one span per slab through the core's scatter-gather blobs, would
remove the coupling and the reassembly copy at the price of slab chains,
multi-slab leases and an exhaustion rule for half-received frames. It is
deferred; the decoder reads through a boundary-aware cursor from the start so
that adding it later changes no contract.

### Send path

Each node has a bounded queue of pending core SEND ops
(`vsr_io_limits.link_queue`); several links to a node may exist (decision
41) and the carrier link drains the queue. At prepare time the messages
queued for one peer are gathered into one vectored send of at most
`send_coalesce_bytes`, with the frame headers written into a pool slab the
link owns (its send slab) and the bodies referenced from where they live; a
message that does not fit continues in the next send, since the connection
is a byte stream. One kernel send is in flight per link, because two sends
on one socket may complete out of order. The flag rule is exact (decision
38): a send whose vectors all lie inside the pool region, which covers
frame headers, received bytes and caller bytes in taken slabs, goes
`SEND_ZERO_COPY | VECTORED` with `FIXED_BUFFER` on the pool's single
registered region; a send with any vector outside the pool (tail ring, core
arena, caller memory) goes zero-copy without `FIXED_BUFFER`, which pins the
pages per send, at or above `zero_copy_bytes`, and plain `SEND` (kernel
copy) below. The second completion of a zero-copy send, the `NOTIF`, is
exactly the moment the core's SEND completion rule asks for: buffers no
longer read. An application whose request bodies are hot, such as a
database submitting its own pages, places them in pool slabs it takes
through `vsr_io_slab_acquire`, so the primary's hottest send is a
fixed-buffer one. A full queue completes the oldest SEND with `RETRY`; the
core already coalesces retries under its retry timer, so this is bounded
and never silent. Changing a node's address or revoking its authorization
closes its links and retries their queued sends the same way.

## 5. Network

### Links, nodes and authorization

A node is a transport identity: one process that may host replicas of
several groups. `vsr_io_node_set` records where a node listens and
`vsr_io_authorize` records that node N may act as replica R of cluster C. One
table serves both purposes: it is used to dial peers and to accept inbound
envelopes, and an envelope whose `from` field is not authorized for its
link's node is rejected. Learners and discovery peers are entries the
application adds before they connect, which is exactly the administrative
authorization the protocol contract asks the adapter for. Links are keyed by
node and shared by every replica on the engine.

Addresses are raw socket addresses as the kernel takes them, `AF_INET`,
`AF_INET6` or `AF_UNIX` including abstract names, rather than a library
structure: the executor is a virtual io_uring and its connect and bind
records carry what the real ones carry, and the first application listens on
abstract Unix sockets.

Two arrangements the first application needs are covered without a second
listener mechanism. A caller that owns the listener for its own clients can
serve peers on the same port: every peer or stream connection begins with
`VSR_IO_WIRE_MAGIC`, so the caller peeks the first eight bytes with a `PEEK`
receive and, for a peer, adopts the socket with `VSR_IO_ADOPT_HANDSHAKE`,
which runs the configured handshake on the untouched stream. A caller that
must dial peers itself, through a proxy or its own authenticated channel,
records the node with a NULL address: the engine emits `LINK_WANTED` on its
backoff schedule instead of connecting, and the caller dials and adopts with
`HANDSHAKE | OUTBOUND`. `vsr_io_node_status` reports the link state toward a
node, when it was established, when a frame was last accepted from it and
the next dial deadline, which is what role management and cluster status
answers need; polling peers through the application protocol is unnecessary.

### Handshake modes and threat model

The core requires that a MESSAGE claiming to come from replica R of cluster
C really did, and that the sender is authorized for its role; otherwise
anything that can reach the port can vote. The model is crash-only and
non-Byzantine, so this is about keeping outsiders out. Three levels were
considered:

1. Trusted network. The handshake asserts a node identity; CRC32C protects
   against corruption only. Anyone who can connect can impersonate. Fine for
   loopback, tests, a physically private fabric, or a WireGuard or IPsec
   underlay, where the network layer authenticates and encrypts below the
   socket and the zero-copy path is untouched.
2. Shared cluster secret. A challenge-response handshake proves possession of
   a key, then the connection is trusted. Optionally every frame carries a MAC
   under a per-connection derived key with a sequence number, which stops
   injection into an established stream and replaces the CRC. Cost is one
   pass over the bytes, the same as checksumming, with no confidentiality.
   Design sketch, one and a half round trips, with key IDs for rotation:

   ```
    dialer                                   acceptor
      HELLO_a {version, node, key id, nonce_a} ──▶
      ◀── HELLO_b {version, node, nonce_b, tag_b = MAC(k, "b" | HELLO_a | HELLO_b)}
      AUTH_a {tag_a = MAC(k, "a" | HELLO_a | HELLO_b)} ──▶
      established; K_conn = MAC(k, nonce_a | nonce_b); frames may carry
      MAC(K_conn, seq | frame)
   ```

   A keyed BLAKE2b with a constant-time compare would be about two hundred
   in-tree lines and no dependency.
3. Mutual TLS. The handshake runs outside the library, kTLS is enabled on the
   socket, and the descriptor is handed to the engine. Multishot recv with
   buffer rings works unchanged. Zero-copy send loses its benefit because the
   TLS layer copies into records anyway, so encryption costs one in-kernel
   copy on transmit, which is inherent without NIC TLS offload.

Decision: identity and authorization are the application's concern, with a
cheap built-in and enough flexibility for anything else. The handshake is a
sans-IO state machine per link with the authenticator as a pluggable step.
`VSR_IO_HANDSHAKE_TRUSTED` ships now; `VSR_IO_HANDSHAKE_EXTERNAL` emits a
`VSR_IO_OP_HANDSHAKE` op carrying the raw descriptor, the application runs
whatever it likes without blocking the loop and completes the op with the
node identity, which is how TLS or any custom scheme plugs in; `vsr_io_adopt`
takes a connection the application dialed and authenticated entirely by
itself or, with its `HANDSHAKE` flag, a raw connection on which the engine
runs the configured mode; `VSR_IO_HANDSHAKE_KEYED` is reserved for the
built-in secret and not implemented. The library takes no crypto dependency. Randomness for nonces
comes from the executor, next to the clock, so the handshake replays under
simulation. The mode is a cluster-wide policy and a peer proposing another is
refused.

### Wire format

Frames are fixed little-endian structures with natural alignment so headers
decode into the host structs cheaply, versioned by `VSR_IO_WIRE_VERSION`, with
CRC32C over header and body. Fixed-size headers decode into the core's
logical structs; payload bytes such as command bodies, results and manifests
are never copied and become spans into the receive slab. Control messages and
bulk streams use separate connections so a large transfer never head-of-line
blocks heartbeats.

### Bulk streams

A stream is a byte flow from a source node to a requester over its own
connection with the same handshake as peer links. Both ends are driven
through ops and events:

```
 requester (fulfilling FETCH)                       source
 app ── STREAM_OPEN {cookie, node, request} ─▶ engine
        engine dials, handshakes, sends request ───────▶ engine
                                                          └─▶ STREAM_SERVE {stream, node, request} ─▶ app
                                                   engine ◀── STREAM_WRITE {stream, buffers | file} ── app
        engine ◀── chunks, CRC32C each ◀──────────────── engine sends: zero-copy from buffers,
 app ◀── STREAM_DATA {cookie, offset, slab bytes}                 file ranges read into slabs
 app ── COMPLETE(STREAM_DATA) releases the slab ─▶ engine  app ◀── STREAM_WRITTEN {stream, write}
        ...                                        engine ◀── STREAM_CLOSE {stream, status} ── app
 app ◀── STREAM_END {cookie, bytes, status}
 app ── COMPLETE(SNAPSHOT_FETCH, checkpoint) ─▶ engine ─▶ core
```

- The request bytes are the caller's, bounded by
  `VSR_IO_STREAM_REQUEST_BYTES`, which is where differential transfer lives:
  send the basis ID and let the source decide what to ship.
- Flow control is credit: at most `stream_window` DATA ops outstanding at the
  requester and WRITE events at the source; receiving pauses when the caller
  is behind, and TCP does the rest.
- Every chunk carries a CRC32C checked by the engine.
- Data arrives in pool slabs straight from the receive buffer ring, so the
  application can write a slab to its own file with a fixed-buffer write and
  the buffered path is network to disk with no user-space copy.
- A file-range write is served by reading the range into pool slabs,
  `stream_chunk_bytes` at a time with fixed-buffer reads, and sending each
  slab as a chunk exactly like a buffer write; there is no splice and no
  pipe (decision 39). Buffer writes go zero-copy from the caller's lease.
- The engine's own stream requests (the clients half of a snapshot fetch,
  decision 43) begin with `VSR_IO_LIBRARY_MAGIC` and are served by the
  source engine itself; caller requests must not use that prefix.
- Connection loss ends the stream on both sides with `RETRY`; the application
  fails its FETCH with `RETRY` and the core rediscovers a source, as its
  contract already says.

## 6. Store

One preallocated file per group, laid out like a device so a block device can
replace it later without a format change, plus one small file per retained
checkpoint.

```
 ┌────────────┬────────────┬───────────┬───────────┬─────┬───────────┐
 │ superblock │ superblock │ segment 0 │ segment 1 │ ... │ segment N │
 │     A      │     B      │  64 MiB   │           │     │           │
 └────────────┴────────────┴───────────┴───────────┴─────┴───────────┘
 segment: [header block(s)][record][record]...[pad to block][record]...
 record:  header {seq, generation, run, length, crc(header), crc(payload)}
          + change descriptors + payload (entries, client records,
          hard state, checkpoint descriptors)
```

### Records and recovery

- Every transaction is one record, in sequence order, in a ring of segment
  slots. Records pack back to back inside a physical write; a write starts
  at a block boundary, covers whole blocks and pads its last block with a
  PAD marker, and the next write starts at the next block boundary, so a
  written block is never rewritten (decision 33). The file grows by
  fallocate up to `max_segments`; a block device is the same layout at a
  fixed size.
- Every segment begins with a header holding the store's complete logical
  state as of its first record: identity, log bounds, hard state with both
  memberships, the anchor checkpoint with its manifest, the client base and
  the last sequence before the segment (decision 34). The superblocks are a
  double-buffered pointer to the segment to start scanning from plus
  identity, geometry, the run counter and a durable floor; they are
  rewritten only when the start segment changes, slots grow, or, when no
  record follows a flush within the flush interval, to persist a newer
  durable floor; never per transaction. Every record also carries the
  durable sequence acknowledged when it was packed (decision 50). Hard-state changes travel inside transaction records.
- Torn-tail rule: records must be contiguous by sequence, CRC-clean and
  non-decreasing in their run counter. The first record that is short,
  fails its CRC, does not carry the expected next sequence, or carries a
  run below its predecessor's ends the log. A range is read once more
  before it is judged bad, since reads can fail transiently; a bad record
  at or below the durable floor, the greatest acknowledged durable
  sequence carried by any superblock, segment header or valid record, is
  CORRUPT and fences the replica (decision 50). A stale record from a recycled
  slot can never match, because sequences never repeat; a persisted block
  of a torn write that survives behind a block rewritten by a later run is
  rejected by the run rule. A bad record at or below the acknowledged
  durable prefix, whose lower bound the superblock and the segment headers
  persist, is `CORRUPT` and fences the replica; a bad record above it is
  the expected torn tail.
- Recovery loads the newer valid superblock, reads every slot's header to
  map segment numbers to slots, loads the state of the start segment and
  replays the records after it in sequence order across segments in number
  order, rebuilding every index. At NVMe speeds a few gigabytes take
  seconds, so index records that would shorten the scan are a later
  optimization and not part of the format now. Fewer format elements, fewer
  bugs.
- The client table of a checkpoint lives in `clients-<snapshot id>`, written
  at CAPTURE and unlinked at DROP; the log only references it. Two reasons: a
  live checkpoint record inside the ring would pin its segment slot against
  reuse, or force relocation on trim; and the application already keeps
  per-checkpoint files, so files are the natural unit now. A block device
  later needs a metadata area for these; not now. Recovery rebuilds the
  client index from the anchor's file plus the CLIENTS records after it.

### Write path

- A STORE completes as soon as its record is packed into the tail buffers and
  indexed. That is exactly the contract's meaning of readable-not-durable, and
  in replicated mode it takes the disk off the PREPARE_OK critical path
  entirely. Only SYNC waits for the write and a flush.
- One path: records are packed by copy into block-aligned registered tail
  buffers, and the file is opened with O_DIRECT by default. The copy costs
  memory bandwidth on entry bodies, far below NVMe write bandwidth, and it
  buys aligned batched writes with no page cache. An earlier draft kept a
  second, gather-from-slabs path for buffered mode; it was dropped because
  O_DIRECT needs block-aligned iovecs and TCP lands bytes at arbitrary slab
  offsets, so the copy is needed anyway for the default and one path means
  fewer bugs.
- The tail buffers are one block-aligned ring that mirrors the file byte
  for byte and doubles as the read cache (decision 36): the last
  `cache_bytes` of the log stay in memory in their on-disk form, writes are
  issued straight from the ring with fixed buffers, hot LOAD results point
  into it, and receive slabs are held only by core leases and in-flight
  sends. A LOAD lease pins its ring range and unwritten bytes are pinned
  until their write completes; a STORE whose record would overwrite pinned
  bytes is held, and the attach-time rule `cache_bytes >=
  write_behind_bytes + pinned_payload_bytes + 2 * max_record_bytes` makes
  every hold end.
- Group commit per loop iteration: one write covers everything packed since
  the last one, then one flush if a SYNC is pending, either an O_DSYNC write
  or a plain write followed by fdatasync (`sync_mode`). `sync_delay_ns` holds
  the flush to batch more SYNCs at the cost of latency.
- Backpressure: `write_behind_bytes` bounds unwritten data; when reached,
  STORE completions wait. Together with the pinned-floor hold and the
  clients-file read a RESTORE needs (decision 43), these are the only times
  storage latency reaches the core. `inflight_writes` record writes may be
  in flight; the acknowledged prefix stays contiguous because SYNC
  acknowledges only after every earlier write completed and the flush
  returned.
- A failed write fences by default (`VSR_IO_WRITE_ERROR_FENCE`). In
  replicated mode `VSR_IO_WRITE_ERROR_CONTINUE` keeps serving from memory,
  since the local log there is only a recovery hint.

### Replicated mode

Verified in the core: `sync_poll` in `src/protocol.c` emits SYNC only under
`VSR_DURABLE`, and both the safe floor and `reclaim_poll` use the stored
sequence otherwise. In `VSR_REPLICATED` the core therefore never issues SYNC,
persistence is entirely the store's policy, and the readable store is what
matters. The same log becomes write-behind, flushed every
`flush_interval_ns`, and its only roles are faster recovery and catch-up after
a restart and a basis for differential snapshot fetch. The memory budget for
readable entries becomes the real constraint, and entries beyond it are read
back from disk.

### Indexes

All indexes are fixed-capacity arrays from the replica's metadata region.

- Op to record location is a ring indexed by op number, since the retained
  range is dense; it holds every op an unreclaimed revision can still name,
  which RECLAIM advances. Versions removed by TRUNCATE go to a versions
  table with their validity range until RECLAIM passes it, which is how LOAD
  can name an older retained revision.
- One open-addressing table over the 128-bit client IDs with capacity
  `max_clients` holds, per incarnation, its latest completed record (one
  version: the engine routes the LOADs of a core update before its STOREs,
  and a load that still names an older sequence than the client's record
  completes with RETRY, which makes the core reload), its latest retained
  entry, its offset in the current base file and an in-flight flag
  (decisions 35, 44 and 51). The contract
  retains every client's latest record forever and provides no deletion, so
  the client set is unbounded by design while the arena is not.
  `max_clients` is therefore a deployment-wide limit, identical on every
  replica, enforced where new incarnations enter: `vsr_io_submit` leaves a
  REQUEST from an incarnation the table does not know unconsumed with
  `ELIMIT` when the table, counting incarnations in flight, is full, and the
  application answers `LIMIT`. Only the primary admits requests and every
  replica holds the same set, so no backup ever overflows; an overflow
  reached any other way still fences, as an invariant rather than a policy.
  A logged request that retires an incarnation on a clean client disconnect
  would bound growth further and is a core follow-up.
- A segment slot is freed only when every record in it is below all of: the
  record holding the oldest retained entry, the RECLAIM revision, the client
  base sequence plus one, and any capture still reading result bytes from
  it (decision 35). The client index is recovered from `clients-<anchor>`
  plus the CLIENTS records after the client base sequence.
- A cold LOAD reads the covering record range into one pool slab with a
  fixed-buffer read (a record fits a slab by construction, decision 37),
  checks CRCs, and builds the loaded graph in a load region with one span
  per body; the lease is that slab reference plus the region, or a ring pin
  plus the region when the records are still in the tail ring.

### Tunables

| Option | Default | Trade |
| --- | --- | --- |
| `block_bytes` | 4096 | Device alignment; independent of the 512 the NVMe reports |
| `segment_bytes`, `segments`, `max_segments` | 64 MiB, sized at creation | Space versus reclamation granularity |
| `direct_io` | on | Page cache off; the tail cache is ours |
| `sync_mode` | O_DSYNC write | Versus fdatasync; device dependent |
| `sync_delay_ns` | 0 | Batch more SYNCs per flush at the cost of latency |
| `flush_interval_ns` | 100 ms, replicated mode only | How far the write-behind log may lag |
| `write_behind_bytes` | 64 MiB | Backpressure point |
| `cache_bytes` | 256 MiB | Tail ring; at least `write_behind_bytes + pinned_payload_bytes + 2 * max_record_bytes` |
| `max_entries`, `max_clients` | required | Index capacities; the arena is sized from them |
| `inflight_writes` | 2 | Concurrent record writes; the acknowledged prefix stays contiguous |
| `on_write_error` | fence | Or continue memory-only in replicated mode |

A store-only ring with IOPOLL for polled NVMe access is possible later, since
an engine may hold more than one executor, but is not in this design.

## 7. Application boundary

### No vtables, no callbacks

A vtable would have been a struct of function pointers the application fills
in, one per operation: apply, read, capture, install, and so on. It was
dropped in favor of the same shape the core uses, for three reasons. It keeps
the application core pure, which is what makes it testable in the virtual
cluster without I/O, time or allocation. It lets the application, the core and
the engine share one outer loop of reap, consume and queue, submit. And it
gives the application a vocabulary it already knows: the engine is a filter
over the core's contract, not a second contract. The only callbacks in the
header are the three optional hooks of `vsr_io_run`.

### Wrappers

`struct vsr_io_op` is a `struct vsr_op` plus its replica and a kind;
`struct vsr_io_event` is a `struct vsr_event` plus the same. Kind
`VSR_IO_OP_CORE` carries a core op verbatim with the pin and completion rules
of `vsr.h`, completed by a `VSR_IO_EVENT_CORE` event of type
`VSR_EVENT_COMPLETE` with the same id. Forwarded core ops are APPLY,
READ_READY, every SNAPSHOT_* op, REPLY with the caller's route in `arg`, and
RELEASE of caller leases. The engine consumes SEND, LOAD, STORE, SYNC, RECLAIM
and RELEASE of its own leases. The other kinds are the rails: `HANDSHAKE`,
`STREAM_SERVE` and `STREAM_DATA` require a completion; `STREAM_END`,
`STREAM_WRITTEN` and `STATUS` are informational. `STATUS` carries the replica's
`vsr_status` whenever the core reports `STATE_CHANGED` and once at STOPPED, so
the application never polls for state.

### Joint snapshot ops

A checkpoint has two halves. The library owns the envelope and the client
table; the application owns the image. The manifest bytes that peers see in
CHECKPOINT messages and log offers are entirely the application's, since the
library finds its own half by snapshot ID.

| Core op | Library does | Application does |
| --- | --- | --- |
| CAPTURE | Generates the snapshot ID from executor entropy, snapshots the client table at the task's sequence before any later STORE, writes `clients-<id>` streaming through one staging slab, forwards the op with the ID in its template | Freezes its state, returns its manifest with that ID; the core's completion follows when both halves are done and releases the fence |
| SNAPSHOT_SYNC | Makes `clients-<id>` and the directory entry durable | Makes its files durable; the library completes to the core when both halves are |
| FETCH | Pulls `clients-<id>` from the peer over its own stream into a temporary file, verifies it, renames it, then forwards | Pulls its image with a bulk stream or any channel it prefers, then completes |
| INSTALL | Nothing; the store's RESTORE already reset the client base | Loads the image so the state is exactly at checkpoint.op |
| DROP | Forwards, then unlinks `clients-<id>` once no served stream reads it | Deletes its files once its own readers drain |

### Reads are local

In the core a read is a local event with a cookie, and READ_READY carries no
value. The application submits a READ event with the barrier and its own
cookie, receives READ_READY with the fence, takes its snapshot or finishes the
read, completes the op, and answers the caller on its own connection. Query
payloads never touch the library; the application keeps them keyed by its
cookie. An earlier draft had the library carry a query payload and a
read-result event; both disappeared once the application owned its client
protocol.

```
 client ── read {query} ──▶ application frontend (its own protocol)
                              │ READ event, cookie = its route
                              ▼
                            vsr core ──▶ READ_READY {fence}
                              │
                              ▼ (vsr_io_poll)
                            application evaluates at fence.applied,
                            completes READ_READY, answers on its socket
```

### Local submission and no client-facing surface

REQUEST, CLIENT_QUERY, READ, CHECKPOINT and STOP events go in under
application-owned leases; REPLY, READ_READY and RELEASE of those leases come
back out. Administrative requests such as RECONFIGURE and CHECK_EPOCH use the
same path from whatever admin surface the application builds. Because the
application owns its client protocol and sockets, the library never sees a
client. This removed from earlier drafts: a client listener, a client wire
format, per-principal flags, and a generic client node type for simulation.
Library authentication now covers exactly two things, peer links and bulk
streams. In the virtual cluster a simulated client submits to a node's local
API directly, with the harness adding latency if a scenario wants it.

### Helpers

Slab allocation from the registered pool is provided: `vsr_io_slab_acquire`
hands the caller a slab for its own request bodies and file I/O, which the
engine then sends and writes with fixed-buffer operations, and
`vsr_io_slab_release` returns it once nothing of the caller's covers it. The
first application's request bodies are its own database pages, so this is
the difference between registered and unregistered zero-copy on the
primary's hottest send. CRC32C exists internally (`src/io/crc32c.h`) and the
atomic-publish sequence (write a temporary file, fsync, rename, fsync the
directory) is what the fetch of a clients file does; neither is public
until a need appears.

## 8. Client

### What the core requires of a client

- Own an incarnation ID that is never reused, and number requests
  monotonically with one outstanding per incarnation.
- Retry an uncertain outcome with the identical type and body, changing only
  the routing epoch.
- Interpret replies: follow NOT_PRIMARY to the advertised primary, adopt the
  membership returned with NEW_EPOCH, back off on BUSY, fail on INVALID or
  LIMIT, and treat STALE_REQUEST as a sequencing error.
- Track membership from replies, send writes and linearizable reads to the
  primary and causal reads to any full replica, and carry the applied
  position from earlier operations into causal reads.
- Optionally persist sequence state and the unresolved command so a restart
  resumes the same incarnation; otherwise start a fresh incarnation and
  accept that the last outcome stays unknown.

### Topology-aware clients, no forwarding

The application's remote client speaks the application's protocol to an
application node. The client is topology-aware: it knows the primary and the
replicas, sends writes and linearizable reads to the primary and causal reads
to any full replica, and rediscovers on a stale topology, which is one extra
round trip per change and negligible on average since topology changes are
rare. A write arriving at a backup gets an error naming the primary. Internal
forwarding was considered, implemented as the node embedding the client
bookkeeping and relaying, and rejected in favor of the topology-aware client
because steady state then has no extra hop.

| Read | Who serves it | Freshness mechanism |
| --- | --- | --- |
| Linearizable | Primary only | Fresh quorum round per admission, like Raft's ReadIndex, no leases or clock assumptions |
| Causal | Any NORMAL full replica | Waits until applied covers the min_op the client supplies |

A backup cannot serve a linearizable read in this core and there is no
follower-forwarded read index. Reads admitted at the primary before a probe
starts share one quorum round. A causal read gives one client read-your-writes
and monotonic reads, not linearizability across clients: a consistent snapshot
of a committed prefix that may be stale. Every replica applies the same
entries in the same order, so the state at applied position N is logically
identical on every node, and READ_READY guarantees N does not move while the
snapshot is taken; an application built on snapshot isolation, such as one
over SQLite in WAL mode where taking the snapshot means opening a read
transaction, serves such reads from replicas naturally, with long queries off
the loop thread.

### Identity ownership

Submission is always local: a node submits to its own replica. But the
incarnation and request number must be owned by the remote client, not the
node. Client X sends a write to primary Y, Y submits it and crashes before
replying; X times out, finds the new primary Z, and retries. Z deduplicates
only if the retry carries the same incarnation and number that Y submitted,
which Z can know only if X chose them. Had Y chosen them, the retry would be a
new request and the write would execute twice.

The contract retains every client's latest completed record through
checkpoints and reconfiguration forever, provides no deletion, and forbids ID
reuse, so the client index and every checkpoint grow with the number of
client incarnations ever seen. Two designs were weighed:

| Design | Exactly-once | Growth |
| --- | --- | --- |
| Remote-owned identities | The library's, across node failures | One record per client process start |
| Node-owned lanes plus application idempotency keys | The library's for node-level retries only; application-level keys with application-controlled retention across node failures | Bounded by nodes times lanes |

Decision: remote-owned identities, because the application's clients are
long-lived services like database connections, so growth is small. The
bounded-lane alternative stays the application's concern if it ever needs it.

### Lanes

The protocol allows one outstanding request per incarnation and answers BUSY
to a second, so one identity is strictly sequential at one commit round trip
at a time. A lane is one long-lived incarnation with its own counter; a client
holding several lanes keeps several requests in flight, with ordering within
a lane and none across lanes. Because of the growth rule lanes live as long
as the client process, never one per request. One lane per connection is the
initial plan, which matches a database connection running one statement at a
time; the bookkeeping supports several.

### vsr-client.h

Not a network client. A sans-IO bookkeeping core with no sockets, framing,
clock or allocation: lanes with their incarnations and counters, retention of
the pending request for retry, reply interpretation into a next action (retry
here, go to the advertised primary, adopt this membership and retry, back
off, fail, or done), time-driven timeouts, a topology cache, min_op tracking
for causal reads, and export and import of its state so the application can
persist it. The application embeds it in its client and carries the fields
in its own protocol. In the virtual cluster it is one more node type, which
is what lets histories be checked as clients observed them and clients be
crashed with and without durable state to verify the exactly-once contract
end to end.

## 9. Simulation and testing

`vsr-sim.h` provides a world (virtual clock, network, disks, entropy, fault
injection, all deterministic from one seed) and one executor handle per node
over it, so the library and the application run byte-identical code over
`vsr_io_executor_ops` in production and in tests. The application core stays
pure: it pulls ops from the engine, emits its own I/O as executor records,
receives time as events and takes arenas instead of allocating, so an entire
application node can be tested end to end with no I/O, no time and no
allocation inside its core.

- The virtual cluster is single-threaded and deterministic. In simulation
  `submit_and_wait` never blocks; the harness runs each node's iteration in
  turn and advances the virtual clock to the earliest deadline when every
  node is idle. Every run replays from its seed.
- Nodes crash and restart with their virtual disks retained, so recovery,
  torn tails and quorum recovery in replicated mode are exercised for real.
- Fault model: for disks, torn writes at block granularity on crash, bit rot,
  latency, transient errors and ENOSPC; for the network, partitions, delay,
  silent drops that stall a connection until it resets, explicit resets,
  byte corruption, and segments split across receive completions at random
  boundaries so that frame reassembly across completions and slabs is
  exercised. Duplication and reordering within a connection are not
  modeled because sockets are ordered byte streams; reordering between
  connections falls out of their independent delays. The clock is per node,
  monotonic, with a per-node offset and late-only timer jitter.
- The application's own files are separate from the store and live on the
  same virtual disks, so application snapshots and their durability are
  covered.
- The simulation enforces ownership rules mechanically: pinned buffers must
  not change, released slabs must not be touched, and provided buffers are
  owned by the kernel until a completion returns them. This is the class of
  bug hardest to catch on real hardware.
- Relation to existing tests: the in-memory host in `tests/lib` and the
  seeded scheduler in `tests/fuzzy` remain the core-level layer. The new
  layer composes the real engine, store and links over the simulated executor
  and extends the existing profile-flag vocabulary of `tests/README.md`;
  the executor contract is checked by one conformance suite that runs
  byte-identical over the simulation and over io_uring, and real-I/O tests
  run under a seeded fault-injecting wrapper executor (decisions 45 and
  46). The plan is in docs/io-implementation.md.
- The simulation is a public library, not test-only code, because
  applications build their own end-to-end tests on it. It may allocate
  internally; the application core it hosts does not.

## 10. Decision log

In the order the decisions were taken.

| # | Decision | Rationale | Rejected |
| --- | --- | --- | --- |
| 1 | The executor is the seam; the library is loop-less | Every loop form falls out of four primitives; sharing one executor is the shared-ring case by construction | Loop ownership as the design question, with a separate shared-ring mechanism later |
| 2 | Multi-group kept in the API shape, not tuned | One object split now, painful to retrofit; expected count is a handful, probably one | Single-group API; designing for thousands of groups |
| 3 | No vtables or callbacks at the application boundary | Same op/event shape as the core keeps the application pure and shares one loop | A struct of application callbacks |
| 4 | Authentication is the application's, with cheap built-ins | Threat models differ per deployment; no crypto dependency in the library | Built-in TLS; mandatory keyed handshake |
| 5 | Kernel 7.2 and liburing 2.15 baseline, no fallbacks | Probe confirmed every feature; probing and fallbacks are complexity with no user | 6.1 LTS baseline with probed extras |
| 6 | Files only, device-style layout | Simple now; a block device later is a different open call | One file per segment; block devices in v1 |
| 7 | Sans-IO application mode as the organizing principle | End-to-end deterministic testing of the whole application node, as the core already enjoys | Testing the application only over real I/O |
| 8 | Runtime indirection for the executor | Batch-shaped interface costs well under a percent; static selection is a mechanical change later | Compile-time backend switch |
| 9 | Separate `vsr-sim.h` | The world has its own large surface for faults, clocks and inspection | Everything in one header |
| 10 | Single-thread deterministic virtual cluster | Replay by seed, as the existing simulator does | Multi-threaded simulation |
| 11 | The application owns its own disk files | Simplest; keeps the store format free of application state | A reserved partition for application state inside the store file |
| 12 | No client-facing surface in the library | The application owns its client protocol; the library never sees a client | Client listener, client wire format, principals, generic client node type |
| 13 | File-range stream sends with splice | Zero copy from the page cache for snapshot files | Read into pool plus send only |
| 14 | Helpers deferred | No demonstrated need yet | Slab allocation, CRC32C, atomic publish now |
| 15 | STORE completes when packed and indexed; only SYNC waits | Readable-not-durable is the contract; disk leaves the PREPARE_OK critical path in replicated mode | STORE waits for the write |
| 16 | Single packed write path, O_DIRECT default | O_DIRECT needs aligned iovecs anyway; one path, fewer bugs; tail buffers double as cache | A second gather-from-slabs path for buffered mode |
| 17 | Per-checkpoint client-table files | A live record inside the ring would pin its segment slot; files are the natural unit now | A metadata area inside the log file |
| 18 | Recovery by full scan, no index records | Seconds at NVMe speed; fewer format elements | Periodic index records in the format |
| 19 | Remote-owned client identities | Exactly-once across node failure requires the client to own the incarnation; clients are long-lived so growth is small | Node-owned lanes with application idempotency keys |
| 20 | One lane initially, multi-lane supported with per-lane ordering | Matches a connection running one statement at a time; pipelining needs lanes | Per-request incarnations, which the retention rule forbids in practice |
| 21 | `vsr-client.h` is bookkeeping only | The application's protocol carries the fields; submission is always local to a node | A networked VSR client library |
| 22 | Topology-aware clients, no internal forwarding | No extra hop in steady state; rediscovery is a small, rare hit | Forwarding requests from backups to the primary |
| 23 | Deferred: store IOPOLL ring, RECV_ZC, keyed handshake, index records, block devices, TLS helpers | Each is an addition behind an existing seam; none is needed for the first version | Including them now |
| 24 | A frame fits one slab; `slab_bytes` caps message and command size; the decoder reads through a boundary-aware cursor | The first application replicates diffs, so a cap of a few megabytes is policy; multi-slab bodies cost slab chains, multi-slab leases and an exhaustion rule; the cursor keeps that a later, contract-free change | Multi-slab bodies in the first version |
| 25 | Executor addresses are raw socket addresses, Unix sockets included | The executor mirrors io_uring; the first application listens on abstract Unix sockets | A library address structure limited to INET |
| 26 | Caller-owned listeners and dialers: a wire magic, `PEEK`, adopt with `HANDSHAKE`, and `LINK_WANTED` for address-less nodes | One port for peers and clients, and dialing through the application's own channel, are both existing deployments | A second listener mechanism; passing consumed prefix bytes into adopt |
| 27 | `max_clients` is a deployment-wide limit enforced at `vsr_io_submit` with `ELIMIT`; the fence stays as an invariant | Fencing a replica when a table fills is a time bomb under connection churn; admission at the primary keeps every replica within the limit | Fence as the only handling; admission control inside the core |
| 28 | Per-node link status query | Role management and cluster status answers need link state, not polling through the application protocol | Aggregate statistics only |
| 29 | Sends from registered memory use fixed buffers, other caller memory is zero-copy without registration; pool slabs are lendable to the caller | The primary's hottest send is its own request bodies, which live in application memory | Leaving caller-memory sends unspecified; deferring the slab helper |
| 30 | Cross-engine wakeup is the executor's wake plus an application queue; socket options are two operations | No engine needs `MSG_RING` yet; get and set have different result semantics | `MSG_RING` in the first executor; one ambiguous SOCKOPT |
| 31 | The simulation splits segments across receive completions | Real stacks do; reassembly across completions and slabs is the path most likely to hide bugs | Whole-segment delivery only |
| 32 | One `src/libvsr.a` holds the core, the I/O engine, the io_uring executor, the simulation and the client; sources under `src/io`, `src/sim`, `src/client` | One archive and one pkg-config file are simpler to ship and test; the core's zero-dependency property is a property of its sources, checked by which objects a core-only program pulls in | A separate `libvsr-io.a` |
| 33 | A written block is never rewritten: every write starts at a block boundary, covers whole blocks, pads its last block with a PAD marker; the next write starts at the next boundary | A torn rewrite can never damage data already relied on; the scanner's rules stay local to one block | Filling partial blocks in place |
| 34 | Format: superblocks A/B with identity, geometry, start segment, run and durable floor; segment headers with the full logical state as of the segment's first record; records with sequence, generation, run and two CRCs; a run counter incremented at every open | Recovery loads one header and replays; the run rule rejects a persisted block of a torn write behind a rewritten one; the durable floor makes CORRUPT detectable | Hard state in the superblock; index records; a single CRC |
| 35 | Freeing floor = min(record of the oldest retained entry, RECLAIM revision, client base + 1, capture read floor); client index recovered from `clients-<anchor>` plus CLIENTS records after the base; one client table holding completed, retained and in-flight state | One floor rule covers every reader; one table keeps `max_clients` accounting exact | Two client tables; freeing by trim point and reclaim floor only |
| 36 | The tail ring mirrors the file, is write staging and read cache, and has a pinned floor; `cache_bytes >= write_behind_bytes + pinned_payload_bytes + 2 * max_record_bytes` | Writes go from the ring with fixed buffers, hot loads point into it, and the rule guarantees a held STORE always proceeds | A separate read cache; unbounded holds |
| 37 | `max_record_bytes` is computed from the core limits; a record fits one slab (plus two blocks of alignment) and one segment; cold LOADs read into one slab, one at a time per replica; the wire decoder and the record reader share the boundary-aware cursor | Sizes derive from limits the deployment already fixes; one cold read at a time keeps the pool bound simple | Multi-slab reads; per-replica read parallelism |
| 38 | Send flag rule: all vectors in the pool -> `SEND_ZERO_COPY \| VECTORED \| FIXED_BUFFER`; any vector outside -> zero-copy without `FIXED_BUFFER` at or above `zero_copy_bytes`, plain send below; send queues are per node; one kernel send in flight per link; frame headers in a per-link send slab | The pool is one registered region, so the check is a range test; two sends on one socket may complete out of order | Per-link queues; pipelined sends |
| 39 | No splice and no pipes: a FILE stream write is read into pool slabs and sent like buffers; `PIPE` and `SPLICE` removed from the executor; `file_slots` no longer counts pipe ends. Supersedes 13 | One send path for both write kinds; two fewer executor operations to specify and simulate; the copy is a page-cache read the NIC would do anyway | Splice from the page cache |
| 40 | The engine arms no `TIMEOUT` record; every engine deadline is an entry of a deadline set reported by `vsr_io_prepare` | One mechanism, no kernel timers to cancel or update, and the loop already blocks with a deadline | Timer records per deadline |
| 41 | Several established links to one node are legal; the carrier is the link dialed by the lower node identity, oldest first; idle timeout closes the rest | Simultaneous dial is normal; a rule both ends compute needs no protocol | Refusing the second connection; a tie-break message |
| 42 | A MESSAGE lease is one slab reference plus one decode region from a per-replica pool of `input_leases + events` regions sized from the core limits; LOAD leases are a slab reference or ring pin plus a load region; slabs are refcounted by leases, in-flight vectors, stream DATA ops, cold reads and caller holds | Regions bound decoding memory without allocation; one count per slab covers every holder | Decoding into caller-visible slabs; per-message allocation |
| 43 | Joint snapshot ops: the engine generates the snapshot id and the forwarded CAPTURE carries it; it writes, syncs, fetches (over a library stream whose request starts with `VSR_IO_LIBRARY_MAGIC`) and unlinks `clients-<id>`; the core's completion follows both halves; a RESTORE, or a PUBLISH of an id other than the latest capture, waits for the file to be read; the clients file is versioned with a CRC per record and a count | The library owns the client table, so it owns the file and the id; waiting on the rare RESTORE avoids a staging table | Caller-generated ids; staging tables for fetched files |
| 44 | `max_clients` admission tracks in-flight incarnations as a flag in the client table entry, cleared when the incarnation is indexed or replied to | No second structure; the count is the table's | A separate in-flight set |
| 45 | Executor semantics are specified once in docs/io-implementation.md and checked by `tests/integration/executor_conformance` over both executors; the first completion of a zero-copy send carries `MORE`; `FIXED_BUFFER` addresses are validated against the region; listeners are set up asynchronously through the executor and a failure is fatal through `vsr_io_stats.failure` | Two implementations of one contract need the contract written down; init that blocks on the ring would steal the caller's completions | Probing semantics from the kernel; synchronous bind in init |
| 46 | Test plan: unit tests per module, libFuzzer harnesses for the frame decoder and store recovery, engine contracts over the simulation, the conformance suite, and `tests/fuzzy/iocluster` with fault profiles and a seeded fault-injecting wrapper executor for real I/O | Mirrors the core's layers; every randomized failure replays from its seed | Real-I/O-only integration tests |
| 47 | Superseded by 51. CLIENT and REQUEST loads answer from the current record and the one it replaced; a load older than both fails as an invariant | The core issues them at its stored sequence, which trails the readable one by at most one poll, and replaces a client's record at most once per poll | Full history per client |
| 48 | Recovery successor rule: after replaying a segment, continue in the segment with the greatest number among those whose header names the replayed sequence with a run at or above the current one; a header naming a lower sequence is stale and its slot is free; the durable floor is the maximum persisted floor | Stale headers of torn segments are distinguishable without rewriting them | Zeroing stale headers at recovery |
| 49 | `vsr_io_detach` is EBUSY while write-behind writes or flushes are in flight; `vsr_io_close` cancels listeners and receives, closes links and streams, and reports `closed` when every slot is free | Nothing is torn down under an in-flight kernel operation | Synchronous teardown |
| 50 | Exact durable floor: every record carries `flushed`, the durable sequence acknowledged to the core when it was packed; recovery's floor is the maximum over the superblock, the segment headers and every valid record; a scan that ends at or below it is CORRUPT; a bad range is re-read once before it is judged; when no record follows a flush within `flush_interval_ns` (100 ms when zero) an idle superblock write persists the new floor | In DURABLE mode a replica that silently drops an acknowledged transaction can vote twice after a restart, and a floor only as fresh as the last superblock would misread corrupted durable records as a torn tail. Flushed blocks survive a crash, so the residual case is media corruption of the last flushed records before a later record or the idle write persists their floor | A superblock write per SYNC batch; the floor of superblocks and headers only |
| 51 | One completed-record version per client: within one core update the engine routes every LOAD before any STORE, so a CLIENT or REQUEST load is answered with `readable` equal to the sequence it names; a load that still names a sequence older than the client's current record (a STORE completion the core has not consumed yet) completes with RETRY | The core names its `stored_sequence` in these loads and on RETRY resets the route and reloads without fencing (`load_route` and `route_load_complete` in src/protocol.c) | Decision 47's second version per client; FAILED as an invariant |
| 52 | liburing is dropped: the executor drives io_uring through the raw `io_uring_setup`, `io_uring_enter` and `io_uring_register` syscalls over a vendored copy of the kernel's UAPI header (`src/io/uapi/io_uring.h`, GPL-2.0 WITH Linux-syscall-note, as liburing itself vendors it); `libvsr.a` depends on libc alone; the record-to-SQE table and its unit test stay. Supersedes the liburing half of 5 and 32 | liburing >= 2.15 ships in no distribution, so every consumer built it from source, and the system UAPI header is what actually limited the build; the library used only the ring plumbing (setup, memory layout, enter, register), whose protocol is UAPI; the simulation already mirrors the raw semantics; inline helpers hid NULL SQEs and the enter flags of each call and tripped compiler diagnostics across versions | Keeping liburing, optionally or as a build-time backend |
| 53 | The kernel baseline is Linux 6.18, tested on 6.18 and 7.2; init probes the opcode table once (`IORING_REGISTER_PROBE`) and the required feature bits and refuses an older kernel with `-ENOSYS`; a vectored zero-copy send from a registered region is issued as `SENDMSG_ZC` with `FIXED_BUF` and an iovec, supported since 6.15, on every kernel, since `SEND_ZC` accepts the vectorized fixed-buffer combination only from 7.x; there is still no runtime fallback. Amends 5 | The substitution is one opcode in the translation table with identical completions; a version-gated dual path would be the fallback the design excludes | Dropping `FIXED_BUFFER` on those sends (loses registered-buffer zero copy); a probe-selected dual path |
| 54 | `vsr_io_limits.caller_slabs` is the caller's share of the pool: the pool never provides a slab while the FREE count would drop below `reserve + caller_slabs - taken`, where `taken` counts slabs the caller currently holds through `vsr_io_slab_acquire`, and acquire returns ELIMIT once `taken == caller_slabs` or the FREE count is at the reserve; the minimum-slabs rule adds `caller_slabs`; the default 0 means no caller share | Provision handed every FREE slab above the reserve to the kernel, which returns one only after filling it, so a caller could never obtain a slab under load | Taking slabs back from the ring (no such operation short of unregistering it); a second registered region for callers (breaks the single-region fixed-buffer rule) |
| 55 | `vsr_client_open` of an incarnation already open returns EINVAL; `vsr_client_begin` returns ELIMIT when the lane's request counter is exhausted; both documented in `vsr-client.h`; client.c uses the shared CRC32C of `src/io/crc32c` | The behaviours existed and were undocumented; one CRC implementation | A second CRC; silent wrap of the counter |
| 56 | The per-stream inactivity timeout is `handshake_timeout_ns`: a stream that makes no progress for that long ends with RETRY on both sides; the versions table stays sized like the op ring | Closes the first two open questions of docs/io-implementation.md section 11 without a new knob before real transfers are measured | A dedicated option now; a smaller versions bound that needs a core-side statement |
| 57 | NEW and JOIN refuse a directory that already holds a `log` or any `clients-*` file, failing the attach the way RECOVER reports NOT_FOUND; RECOVER unlinks every `clients-*` file the recovered log does not reference | Stray files mean an operator mistake or a half-created store, so refusing is the safe default; a CAPTURE whose record never committed leaves such a file, so recovery removes it | Ignoring stray files |
| 58 | Executor contract clarifications: CLOSE never completes the records pending on the descriptor, which keep the underlying object alive until they complete or are cancelled (the engine cancels, decision 49); the ACCEPT row no longer terminates on a closed listener; unregistering a buffer ring with pending receives is allowed and those receives terminate with `-ENOBUFS` at their next delivery; `update_file` and `update_buffer` before registration return `-EINVAL`; the simulation's parked socket records hold a reference as its disk records do | These are io_uring's semantics (a pending request holds its file), and the engine never relies on close cancelling anything | Close-cancels in the simulation; `-EBUSY` on unregister |
| 59 | `vsr-sim.h`: every `int` function returns 0 or a negative errno (`-EINVAL` bad node or argument, `-ENOENT` missing path, `-ENOTDIR`, `-ENOMEM` from create); `vsr_sim_restart` of a live node returns a handle with NULL ops; `vsr_sim_advance` counts cancelled and superseded events as not pending, so it returns -1 once nothing real is due; `LISTEN` records but does not enforce its backlog | `VSR_EINVAL` is -1 and collides with `-EPERM`; a cancelled timer must not move the clock; the backlog has no observable effect the engine depends on | Mixed error vocabularies; backlog enforcement |
| 60 | Deadline handle bases are assigned in `enum vsr_io_deadline_kind` order, so entries due at one instant are dispatched by kind, then by index; an owner re-arms strictly after `now` | Ties must break by a rule the engine and a replay both predict; a re-arm at or before `now` would pop again in the same drain and livelock `vsr_io_poll` | Heap position as the tie-break |
| 61 | Pool rules the review settled: losing the ring keeps the starvation flag, so a receive that ended `-ENOBUFS` is re-armed once a new ring holds a buffer; `vsr_io_pool_caller_release` returns the caller's share at once while other holders keep the slab out of the ring; the engine calls `ring_lost` only after translating every reaped completion; `provide` cannot fail for a ring sized at least `slabs`, so a provide error is fatal through `vsr_io_stats.failure` (decision 45) | Each was an unstated case the model test needed an answer for; the fatal rule avoids a rollback path with no user | Clearing starvation at ring loss; rolling back a failed provide |
| 62 | Executor contract: a zero-copy send completes twice only once the executor accepted it; a record rejected at translation (`SKIP_SUCCESS` on a zero-copy or multishot record, an out-of-region `FIXED_BUFFER`, an unsupported opcode) completes exactly once with the error and `MORE` clear, so a first completion without `MORE` means no `NOTIF` follows and the slot is freed; the result completion always precedes the `NOTIF`; in a `LINK` chain `SKIP_SUCCESS` records count zero completions and after a failure the `-ECANCELED` completions arrive for slots already freed | io_uring posts one CQE for a request refused before its notification exists and has ordered the `NOTIF` after the result since 6.1; a counting slot table works only under these rules | Synthesizing a `NOTIF` for rejected records; counting one completion per zero-copy send |
| 63 | Client: `NOT_PRIMARY` is judged against what the client already knows: a reply from an older view changes nothing, a reply naming no primary is IGNORE when the client's primary is the attempt's own target, otherwise the attempt moves to the next target; a `NEW_EPOCH` carrying the attempt's own routing epoch is a late duplicate and IGNORE | The core never sends `NEW_EPOCH` within the request's own epoch, and a duplicated redirect must not forget a good primary | Treating every `NOT_PRIMARY` as a WAIT |
| 64 | Client: `FAILED` with `STALE_REQUEST` means the command's outcome is unknown, it may have executed, so the caller must not resubmit it as a new request; the lane refuses `vsr_client_begin` with EINVAL until closed and a fresh incarnation is opened; the mark is in-memory only and not part of the export image, so a caller closes such a lane before exporting | A caller trusting "never executed" would run a command twice; a lane whose counter is behind the cluster's cannot safely continue | Leaving the lane IDLE and usable |
| 65 | Executor contract clarifications from the conformance suite: `BUFFER_SELECT` receives are issued poll-first on the ring so `-ENOBUFS` happens at a delivery, never at arming; `update_buffer` of a region a pending record uses is a caller error that the simulation reports as `-EBUSY` while the ring returns 0 and the kernel keeps the old registration alive until those records complete; `GETSOCKOPT` serves level `SOL_SOCKET` only and other levels complete `-EOPNOTSUPP` on both executors (`SETSOCKOPT` serves every level); `ACCEPT` with `DIRECT` and no free slot accepts and closes that connection (`-ENFILE` ends the multishot, the peer sees a reset) while later connections stay queued; `O_DIRECT` requires `offset` and `length` aligned to the logical block, else `-EINVAL`, and the address only to the device's DMA alignment, which the simulation does not check; `CANCEL` without `ALL` cancels one matching record, which one is unspecified; `user_data` `UINT64_MAX` is reserved and `submit_and_wait` returns `-EINVAL` for a record carrying it; a second `register_files` or `register_buffers` is `-EBUSY`, `buffer_ring` for a registered group `-EEXIST`, unregistering or providing to an unknown group `-ENOENT`, an index outside a table `-EINVAL`; `-EALREADY` from `CANCEL` is timing-dependent on the ring | Each is a case where the kernel's behaviour was fixed by the hardware or the kernel and the simulation had to follow, or where the contract left a value unspecified that two implementations then chose differently | Counting pending region users in the ring executor; queuing an unslotted direct accept |
| 66 | The ring's shared memory (submission and completion rings, the SQE array) is the kernel's, mapped from the ring descriptor and unmapped before it is closed; the caller's executor region holds only executor state and tables; `vsr_io_uring_deinit` first drains the ring with `IORING_REGISTER_SYNC_CANCEL` and discards the completions; the ring is set up with `TASKRUN_FLAG` so reap can run deferred task work exactly when `IORING_SQ_TASKRUN` says so; init requires the feature bits `SINGLE_MMAP`, `NODROP`, `EXT_ARG`, `REG_REG_RING`, `MIN_TIMEOUT`, `RSRC_TAGS`, `CQE_SKIP` and `LINKED_FILE` besides the opcode probe. Amends the `NO_MMAP` choice of decision 45's era | The kernel finishes a closed ring asynchronously and still writes the ring words afterwards, so ring memory inside the caller's region was written after deinit had returned it (a nondeterministic crash under gcc; hidden by ASan's quarantine under clang); the drain bounds what can still complete during teardown | `NO_MMAP` over caller memory; leaving the region pinned until an unknowable time |
| 67 | Engine kernel rules: a replica may configure at most 8 `inflight_writes` (the slot table reserves that many per replica before any attach); the engine and the executor assume 4096-byte pages, and a kernel with larger pages fails `vsr_io_init` at the buffer-ring registration with `-EINVAL`; the link module checks the envelope's `from` authorization against the link's node before handing a body to `vsr_io_engine_deliver`, which verifies only that the decoded cluster is the replica's; a replica carries its own ring of internal completions so section 7.1's order (internal completions, caller events, messages, time) needs no lookup; the DIAL deadline handle of a node is `limits.links + node index` (decision 60's bases); `vsr_io_init` registers only the pool region and the buffer ring, since the file and buffer tables are executor-wide | Each was a gap the kernel's implementation had to close; the caps turn unbounded sizing into checked arithmetic | Growing the slot table per attach; a page-size probe |
| 68 | Executor contract notes from the executor review: a `DIRECT` `OPENAT` strips `O_CLOEXEC` (the kernel refuses it on a slot, as it does `SOCK_CLOEXEC` on a direct accept); on the ring a multishot receive or accept can also terminate with a positive result and `MORE` clear when the completion queue is full at a delivery, so `cq_entries` must exceed the completions pending records can post (the engine's 2 * sq_entries rule) and a caller re-arms on any `MORE`-clear completion; `submit_and_wait` refuses `want` above the CQ size with `-EINVAL` | The kernel does not overflow multishot completions, it ends the request; refusing an unsatisfiable `want` beats waiting until the deadline | Silently clamping `want`; queuing multishot CQEs in the overflow list |
| L1 | Link descriptors are raw until the engine takes them over, then engine file slots: a dialed link issues SOCKET, then CONNECT on the raw descriptor; a listener runs a plain multishot ACCEPT (its own socket is DIRECT into an explicit engine slot); `vsr_io_adopt` brings a raw descriptor; the takeover installs the descriptor with `update_file` (the executor owns it from then on) at the CONNECT or ACCEPT completion in TRUSTED mode, at the caller's OK HANDSHAKE completion in EXTERNAL mode, and at adopt; every later record is FIXED_FILE on the slot and a CLOSE frees it. In EXTERNAL mode the dialer sends the preamble and the acceptor reads exactly its 8 bytes on the raw descriptor before the HANDSHAKE op is emitted, so the caller's protocol starts after the magic on an otherwise untouched stream. Listener state lives in a fixed table of `VSR_IO_LISTENERS_MAX` (8) entries; `vsr_io_layout` refuses more listen addresses with ELIMIT. Amends section 7.4's "multishot ACCEPT with DIRECT" | Both executors allocate a `SLOT_ALLOC` descriptor from the whole file table, so a DIRECT accept lands in the caller's slots; the EXTERNAL caller needs a raw descriptor to run TLS on and adopt already delivers one; one install path serves dial, accept and adopt; `listen_count` is not a limit, so the listener table cannot be sized by the layout | DIRECT accepts with `SLOT_ALLOC`; a registered allocation range in the executor contract; per-listener memory in the layout |
| L2 | Authorization and dial rules: `vsr_io_authorize` names a node already in the node table (EINVAL otherwise), an identical re-authorization still wants a link, and revoking closes the node's links only once no (cluster, replica) names it; `vsr_io_node_clear` removes the node's authorizations; an inbound HELLO or an EXTERNAL completion naming a node not in the table is refused; the engine never dials nor emits LINK_WANTED for its own node id. A node's want is set by an authorization or a SEND while no link is established and cleared when a peer link is established or idle-closed; a dialed link that fails at any step before ESTABLISHED (connect, handshake, timeout, refusal) backs off like a refused connect, while an inbound link's failure is attributed to no node and schedules nothing | An unknown node can never match a link, so authorizing it would only hide a configuration error; a node authorized for two clusters must keep its link when one is revoked; a handshake refused in a tight loop would otherwise redial without pause | Dangling authorizations for unknown nodes; closing every link on any revoke; backoff for connect failures only |
| L3 | Engine kernel additions for the link module: `vsr_io_engine_install` wraps the executor's `update_file` for the takeover of L1; `vsr_io_links_deadline(kind, index, now)` is what the poll's deadline drain dispatches LINK and DIAL entries to, before `vsr_io_links_poll`; the link module consumes and frees its own slots (`vsr_io_slots_consumed`, or `vsr_io_slots_free` for a zero-copy send refused before the kernel took it) after `vsr_io_complete` resolved them; completion-time decisions use `io->now`, the last poll time, since completions carry no clock | The planner never calls the executor itself, so an install needs an engine helper; the deadline set is drained by the engine and each kind needs a dispatch target; a completion's MORE and NOTIF semantics are the module's to interpret | Installing from the engine's prepare (a round trip per takeover); a `now` argument on every completion |

## 11. Open items and implementation order

Open items, each behind an existing seam:

- `VSR_IO_HANDSHAKE_KEYED`: the shared-secret handshake and optional per-frame
  MAC of section 5.
- Block devices: the same layout at a fixed size plus a metadata area for the
  per-checkpoint client tables.
- Index records to shorten recovery of very large retained logs.
- A store-only executor with IOPOLL for polled NVMe access.
- Zero-copy receive on NICs that support it.
- A public atomic-publish helper, if an application asks for one.
- The bounded-lane client alternative, if an application with short-lived
  clients ever needs it.
- Multi-slab frame bodies, delivered as one span per slab, once a
  transaction cap is no longer acceptable.
- `MSG_RING` as an executor operation for cross-engine completions.
- Core follow-ups agreed for the first application: a planned primary
  handoff event, so a primary about to stop starts the view change itself
  instead of waiting out the view timeout; and a logged request that retires
  a client incarnation on a clean disconnect.
- Application-specific tuning for the replicated SQLite database, taken up
  after the generic layer exists.

Implementation order, each milestone with its own deterministic tests:

1. Codec and framing: wire structs, CRC32C, in-place decoding through a
   boundary-aware cursor, fuzzed.
2. Link state machine: dial, accept, TRUSTED and EXTERNAL handshakes, backoff,
   send coalescing and queue backpressure, receive reassembly.
3. Store layout and recovery: records, superblocks, indexes, torn-tail
   handling, per-checkpoint client tables, crash injection at block
   granularity.
4. The deadline set.
5. The io_uring executor, `vsr_io_uring_*`, and the engine over it.
6. The simulated executor and world, `vsr-sim.h`.
7. The virtual cluster harness composing engines over the simulation, with
   the fault profiles extended from the existing scheduler.
8. A networked example over the key-value application of `examples/`, with a
   client built on `vsr-client.h`.
9. Benchmarks: throughput and latency across the tunables, memory and copies.
10. The application-specific pass.

The module boundaries, private headers, formats and tests of these
milestones are specified in [io-implementation.md](io-implementation.md).
Everything ships in the one `src/libvsr.a` with the one `vsr.pc`; the core
keeps its zero-dependency property as a property of its sources (decision
32).
