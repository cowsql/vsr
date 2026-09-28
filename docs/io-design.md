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

The development machine runs kernel 7.2.6 with liburing 2.15, and the UAPI
header on the machine comes from the matching `linux-libc-dev` 7.2.6, so the
header and the running kernel agree. A probe program against the running
kernel confirmed:

- All 65 opcodes in the header are reported supported, up to and including
  `RECV_ZC`, `EPOLL_WAIT`, `READV_FIXED`, `WRITEV_FIXED`, `PIPE`, `NOP128` and
  `URING_CMD128`.
- Every feature bit up to `NO_IOWAIT` is set.
- Ring setups verified by creating them: `SINGLE_ISSUER|DEFER_TASKRUN`,
  `SQPOLL`, `IOPOLL` and `IOPOLL|HYBRID_IOPOLL`, `SQE128|CQE32`,
  `REGISTERED_FD_ONLY|NO_MMAP`, `SUBMIT_ALL|R_DISABLED`,
  `COOP_TASKRUN|TASKRUN_FLAG`.
- A provided-buffer ring with `IOU_PBUF_RING_INC` (incremental consumption).
- User-provided ring memory through `NO_MMAP` and `io_uring_queue_init_mem`.

Decision: the baseline is kernel 7.2 and liburing 2.15, with no feature
probing and no fallbacks. Initialization checks once and refuses to run on an
older kernel with a clear error. The design relies directly on: multishot
accept and recv, provided-buffer rings with incremental consumption,
vectorized zero-copy send from registered buffers (`SEND_ZC` with
`IORING_SEND_VECTORIZED` and `IORING_RECVSEND_FIXED_BUF`), vectored
fixed-buffer disk reads and writes, registered wait arguments with
minimum-timeout batching, bind and listen as ring operations, direct
descriptors, `MSG_RING` for cross-engine wakeups, and splice with a pipe for
file-range sends. Zero-copy receive (`RECV_ZC`) requires NIC support for
header and data split and is deferred.

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
│  replica[g] ──┐    links          store         timers pools │ │
│   vsr core    │    codec + link   log layout    heap   slabs │ │
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
           // is pending), recv/accept re-armed, recycled slabs provided,
           // timers armed; returns the earliest engine deadline
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
cross-engine traffic is a `MSG_RING` wakeup plus an application-level queue,
never a lock on the hot path. Each engine holds its own links to each peer
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
                │         └── MSG_RING wakeup ┘         │
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

### Send path

Each link has a bounded queue of pending core SEND ops
(`vsr_io_limits.link_queue`). At prepare time the messages queued for one
peer are gathered into one vectored send of at most `send_coalesce_bytes`,
with the frame headers in engine memory and the bodies referenced from their
slabs. Sends at or above `zero_copy_bytes` use `SEND_ZERO_COPY`, whose second
completion, the `NOTIF`, is exactly the moment the core's SEND completion rule
asks for: buffers no longer read. Smaller sends copy in the kernel and
complete on their single completion. A full queue completes the oldest SEND
with `RETRY`; the core already coalesces retries under its retry timer, so
this is bounded and never silent. Changing a node's address or revoking its
authorization closes its links and retries their queued sends the same way.

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
itself; `VSR_IO_HANDSHAKE_KEYED` is reserved for the built-in secret and not
implemented. The library takes no crypto dependency. Randomness for nonces
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
 app ◀── STREAM_DATA {cookie, offset, slab bytes}                 splice for file ranges
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
- File-range writes are sent with splice through a pipe pair the engine owns
  per stream, which is zero copy from the page cache; buffer writes go
  zero-copy from the caller's lease.
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
 segment: [record][record][record]...[pad to block]   append-only ring
 record:  header {seq, generation, length, crc(header), crc(payload)}
          + change descriptors + payload (entries, client records,
          hard state, checkpoint descriptors)
```

### Records and recovery

- Every transaction is one record, in sequence order, in a ring of segment
  slots. Records pack back to back; only each physical write is padded to
  `block_bytes`. The file grows by fallocate up to `max_segments`; a block
  device is the same layout at a fixed size.
- The superblocks are a double-buffered metadata checkpoint, written lazily
  on segment roll or every so many transactions and never per transaction.
  They hold identity, format, geometry, the log range, the recovery anchor and
  hard state as of some sequence, and the segment to start scanning from.
  Hard-state changes travel inside transaction records, so nothing forces a
  superblock write per transaction.
- Torn-tail rule: records must be contiguous by sequence and CRC-clean. The
  first record that is short, fails its CRC, or does not carry the expected
  next sequence ends the log. A stale record from a recycled slot can never
  match, because sequences never repeat. A bad record below the acknowledged
  durable prefix is `CORRUPT` and fences the replica; a bad record above it is
  the expected torn tail.
- Recovery loads the newer valid superblock and scans live segments
  sequentially from its start segment, rebuilding every index. At NVMe speeds
  a few gigabytes take seconds, so index records that would shorten the scan
  are a later optimization and not part of the format now. Fewer format
  elements, fewer bugs.
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
- The tail buffers are also the read cache: the last `cache_bytes` of the log
  stay in memory in their on-disk form, hot LOADs never touch the disk, and
  receive slabs are held only by core leases and in-flight sends.
- Group commit per loop iteration: one write covers everything packed since
  the last one, then one flush if a SYNC is pending, either an O_DSYNC write
  or a plain write followed by fdatasync (`sync_mode`). `sync_delay_ns` holds
  the flush to batch more SYNCs at the cost of latency.
- Backpressure: `write_behind_bytes` bounds unwritten data; when reached,
  STORE completions wait, which is the only time storage latency reaches the
  core. `inflight_writes` record writes may be in flight; the acknowledged
  prefix stays contiguous because SYNC acknowledges only after every earlier
  write completed and the flush returned.
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
  range is dense. Versions created by TRUNCATE followed by APPEND go to a
  small overflow table until RECLAIM passes their revision, which is how LOAD
  can name an older retained revision.
- Client to latest completed record and client to latest retained entry are
  two open-addressing tables over the 128-bit IDs with capacity
  `max_clients`. Exceeding it fences: the contract retains every client's
  latest record forever and provides no deletion, so the client set is
  unbounded by design while the arena is not.
- A segment slot is freed only when every record in it is below both the trim
  point and the reclaim floor.
- LOAD reads the covering byte range into pool slabs with a fixed-buffer
  read, checks CRCs, and builds the loaded graph in a per-load arena region
  with one span per body; the lease is that slab reference plus the region.

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
| `cache_bytes` | 256 MiB | Tail kept in memory |
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
| CAPTURE | Generates the snapshot ID, records the client table at the named store revision | Freezes its state, returns its manifest; completion releases the fence, copying continues after |
| SNAPSHOT_SYNC | Makes its client-table file durable | Makes its files durable; the library completes to the core when both halves are |
| FETCH | Pulls its client-table half from the peer over its own stream first, then forwards | Pulls its image with a bulk stream or any channel it prefers, then completes |
| INSTALL | Nothing; the store's RESTORE already reset the client base | Loads the image so the state is exactly at checkpoint.op |
| DROP | Deletes its client-table file once no reader needs it | Deletes its files once its own readers drain |

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

Slab allocation from the registered pool for the application's own file I/O,
a CRC32C routine, and an atomic-publish sequence (write a temporary file,
fsync, rename, fsync the directory) were considered and deferred until a need
appears.

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
  silent drops that stall a connection until it resets, explicit resets and
  byte corruption. Duplication and reordering within a connection are not
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
  and extends the existing profile-flag vocabulary of `tests/README.md`.
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

## 11. Open items and implementation order

Open items, each behind an existing seam:

- `VSR_IO_HANDSHAKE_KEYED`: the shared-secret handshake and optional per-frame
  MAC of section 5.
- Block devices: the same layout at a fixed size plus a metadata area for the
  per-checkpoint client tables.
- Index records to shorten recovery of very large retained logs.
- A store-only executor with IOPOLL for polled NVMe access.
- Zero-copy receive on NICs that support it.
- Helpers: slab allocation for application I/O, CRC32C, atomic publish.
- The bounded-lane client alternative, if an application with short-lived
  clients ever needs it.
- Application-specific tuning for the replicated SQLite database, taken up
  after the generic layer exists.

Implementation order, each milestone with its own deterministic tests:

1. Codec and framing: wire structs, CRC32C, in-place decoding, fuzzed.
2. Link state machine: dial, accept, TRUSTED and EXTERNAL handshakes, backoff,
   send coalescing and queue backpressure, receive reassembly.
3. Store layout and recovery: records, superblocks, indexes, torn-tail
   handling, per-checkpoint client tables, crash injection at block
   granularity.
4. Timers and the deadline heap.
5. The io_uring executor, `vsr_io_uring_*`, and the engine over it.
6. The simulated executor and world, `vsr-sim.h`.
7. The virtual cluster harness composing engines over the simulation, with
   the fault profiles extended from the existing scheduler.
8. A networked example over the key-value application of `examples/`, with a
   client built on `vsr-client.h`.
9. Benchmarks: throughput and latency across the tunables, memory and copies.
10. The application-specific pass.

The library ships as a separate static library with its own pkg-config file
so the core keeps its zero-dependency property.
