#ifndef VSR_IO_H
#define VSR_IO_H

#include "vsr.h"

#include <stddef.h>
#include <stdint.h>
#include <sys/socket.h>

/*
 * VSR-IO: Linux io_uring host for the VSR core.
 *
 * vsr.h separates a deterministic core from its environment. This header
 * applies the same split one level down. Every subsystem here (wire codec,
 * link handshake, log store, timers, bulk streams) is a deterministic planner
 * that emits work records and consumes completion records. The only thing that
 * touches the kernel is an EXECUTOR: an interface shaped like io_uring with two
 * implementations, a real ring (vsr_io_uring_*) and a deterministic simulation
 * (vsr-sim.h). Library code is byte-identical over both.
 *
 * The library is loop-less and has no callbacks. The caller owns one loop per
 * thread, shares its executor with the library, and exchanges ops and events
 * with it using the core's own vocabulary: an ENGINE (struct vsr_io) hosts one
 * or more REPLICAS (struct vsr_io_replica), each binding one core instance to
 * one store. Ops the engine can execute itself (SEND, LOAD, STORE, SYNC,
 * RECLAIM, network buffer RELEASE) never reach the caller; ops that belong to
 * the application (APPLY, READ_READY, snapshot work, REPLY, RELEASE of
 * application buffers) are forwarded verbatim and completed through events.
 *
 * Ownership rules of vsr.h apply unchanged: leases pin input graphs, ops pin
 * output graphs, and every op except RELEASE and the informational kinds
 * listed below receives exactly one completion. No thread other than the owner
 * touches an engine, except through vsr_io_wake. There is no malloc on any
 * path: memory regions come from the caller, sized by the layout functions,
 * and are registered with the executor where the kernel benefits.
 *
 * Platform: Linux >= 6.18, driven through the io_uring syscalls directly
 * (no liburing; the library depends on libc alone). Required features are
 * used without fallbacks; vsr_io_uring_init probes the kernel once and fails
 * with -ENOSYS on an older one. See docs/io-design.md for the design and
 * the rationale behind it.
 */

#define VSR_IO_API_VERSION 1u  /* Source API of this header. */
#define VSR_IO_WIRE_VERSION 1u /* Frame and handshake format between nodes. */
#define VSR_IO_STORE_FORMAT 1u /* On-disk log format. */
#define VSR_IO_NO_NODE UINT64_C(0)
/* "VSRIO" little-endian: the first eight bytes a dialing engine sends on
 * every peer or stream connection, so a caller-owned listener can tell peers
 * from its own clients (see vsr_io_adopt). */
#define VSR_IO_WIRE_MAGIC UINT64_C(0x0000004F49525356)
/* "VSRLIB" little-endian: the first eight bytes of a bulk stream request
 * the engine makes for itself (the clients half of a snapshot fetch);
 * caller requests must not begin with it. */
#define VSR_IO_LIBRARY_MAGIC UINT64_C(0x000042494C525356)

/* -------------------------------------------------------------------------
 * Executor: a virtual io_uring
 *
 * A submission record (SQE) describes one kernel operation; a completion
 * record (CQE) reports its result. Records are plain C11 structs so that a
 * simulation can implement the same semantics without kernel headers. The
 * operation set is the subset the library needs; semantics follow io_uring
 * exactly where a flag is named after an io_uring flag, and the "Executor
 * contract" section of docs/io-implementation.md states them once for both
 * implementations; tests/integration/executor_conformance checks them.
 * user_data is opaque to the executor and is returned unchanged in the
 * completion.
 *
 * Ownership of user_data: the top byte is the OWNER tag. The engine uses the
 * tag from vsr_io_options.owner (default 0); the caller uses any other tag for
 * its own records and dispatches completions by tag, so one executor serves
 * the library and the application in the same loop.
 * ---------------------------------------------------------------------- */

#define VSR_IO_OWNER_SHIFT 56
#define VSR_IO_OWNER(user_data) ((uint8_t)((user_data) >> VSR_IO_OWNER_SHIFT))
#define VSR_IO_USER_DATA(owner, value)                                         \
    (((uint64_t)(uint8_t)(owner) << VSR_IO_OWNER_SHIFT) |                      \
     ((value) & ((UINT64_C(1) << VSR_IO_OWNER_SHIFT) - 1)))

#define VSR_IO_SLOT_ALLOC (-1) /* DIRECT: let the executor pick a free slot. */

enum vsr_io_sqe_opcode {
    VSR_IO_SQE_NOP,
    VSR_IO_SQE_READ,  /* addr/length at offset; FIXED_BUFFER: buffer_index */
    VSR_IO_SQE_WRITE, /* same fields as READ */
    VSR_IO_SQE_READV, /* addr: vsr_io_vec[length]; FIXED_BUFFER: all in index */
    VSR_IO_SQE_WRITEV,    /* same fields as READV */
    VSR_IO_SQE_FSYNC,     /* op_flags: VSR_IO_FSYNC_DATASYNC */
    VSR_IO_SQE_FALLOCATE, /* offset/length; op_flags: Linux fallocate mode */
    VSR_IO_SQE_OPENAT,    /* fd: dir; addr: path; op_flags: O_* flags; length:
                            mode; DIRECT: fd2 = slot or SLOT_ALLOC */
    VSR_IO_SQE_CLOSE,     /* fd, or slot with FIXED_FILE */
    VSR_IO_SQE_RENAMEAT,  /* fd: dir; addr: old path; addr2: new path */
    VSR_IO_SQE_UNLINKAT,  /* fd: dir; addr: path */
    VSR_IO_SQE_MKDIRAT,   /* fd: dir; addr: path; length: mode */
    VSR_IO_SQE_STATX,     /* fd: dir; addr: path; addr2: struct statx; op_flags:
                            AT_* flags; length: STATX_* mask */
    VSR_IO_SQE_SOCKET,    /* length: domain; op_flags: type; offset: protocol;
                            DIRECT: fd2 slot */
    VSR_IO_SQE_CONNECT,   /* addr: struct sockaddr; length: its size */
    VSR_IO_SQE_BIND,      /* addr: struct sockaddr; length: its size */
    VSR_IO_SQE_LISTEN,    /* length: backlog */
    VSR_IO_SQE_ACCEPT,    /* op_flags: MULTISHOT; DIRECT allocates slots; result
                            is the descriptor or slot */
    VSR_IO_SQE_RECV,      /* op_flags: MULTISHOT, PEEK; BUFFER_SELECT from
                            buffer_group, else addr/length */
    VSR_IO_SQE_SEND,      /* op_flags: ZERO_COPY, VECTORED (addr: vecs,
                            length: count); FIXED_BUFFER: buffer_index */
    VSR_IO_SQE_SHUTDOWN,  /* length: SHUT_* how */
    VSR_IO_SQE_SETSOCKOPT, /* op_flags: level << 16 | name; addr/length: value */
    VSR_IO_SQE_GETSOCKOPT,     /* same fields; result is the value's length */
    VSR_IO_SQE_TIMEOUT,        /* offset: ns, ABSOLUTE in the executor clock or
                           relative; completes with -ETIME when it fires;
                           for callers: the engine arms none for itself */
    VSR_IO_SQE_TIMEOUT_UPDATE, /* addr2: target user_data as uint64; offset */
    VSR_IO_SQE_CANCEL,         /* offset: target user_data; op_flags: BY_FD (all
                            operations on fd), ALL (every match) */
    VSR_IO_SQE_FILES_UPDATE,   /* offset: first registered slot; addr:
                                  int32_t descriptors[length], -1 clears a
                                  slot; each slot takes its own reference
                                  and the descriptors stay open; result:
                                  the count updated */
    VSR_IO_SQE_PROVIDE         /* buffer_group; addr: vsr_io_buffer[length]:
                                  provided by the executor itself before the
                                  batch's other records; no completion on
                                  success */
};

enum vsr_io_sqe_flags {
    VSR_IO_SQE_LINK = 1u << 0,          /* Next record runs after this one
                                           succeeds; failure fails the chain. */
    VSR_IO_SQE_FIXED_FILE = 1u << 1,    /* fd is a registered slot. */
    VSR_IO_SQE_FIXED_BUFFER = 1u << 2,  /* Memory lies in registered region
                                           buffer_index. */
    VSR_IO_SQE_BUFFER_SELECT = 1u << 3, /* RECV takes a provided buffer. */
    VSR_IO_SQE_SKIP_SUCCESS = 1u << 4,  /* No completion unless it fails. */
    VSR_IO_SQE_DIRECT = 1u << 5         /* Result descriptor goes to a slot. */
};

enum vsr_io_sqe_op_flags {
    VSR_IO_FSYNC_DATASYNC = 1u << 0,
    VSR_IO_ACCEPT_MULTISHOT = 1u << 0,
    VSR_IO_RECV_MULTISHOT = 1u << 0,
    VSR_IO_RECV_PEEK = 1u << 1,      /* Return bytes without consuming them. */
    VSR_IO_SEND_ZERO_COPY = 1u << 0, /* Two completions: result, then NOTIF. */
    VSR_IO_SEND_VECTORED = 1u << 1,
    VSR_IO_TIMEOUT_ABSOLUTE = 1u << 0,
    VSR_IO_CANCEL_BY_FD = 1u << 0,
    VSR_IO_CANCEL_ALL = 1u << 1
};

struct vsr_io_vec {
    void *base;
    size_t length;
};

/* 64 bytes on common 64-bit ABIs. Unused fields are zero. */
struct vsr_io_sqe {
    uint8_t opcode; /* enum vsr_io_sqe_opcode */
    uint8_t flags;  /* enum vsr_io_sqe_flags */
    uint16_t buffer_group;
    int32_t fd;
    uint64_t user_data;
    uint64_t offset;
    const void *addr;
    const void *addr2;
    uint32_t length;
    uint32_t op_flags; /* enum vsr_io_sqe_op_flags or Linux flags per opcode */
    uint16_t buffer_index;
    uint16_t reserved16;
    int32_t fd2;
    uint64_t reserved;
};

enum vsr_io_cqe_flags {
    VSR_IO_CQE_MORE = 1u << 0,        /* Multishot: more completions follow;
                                         also on the first of the two
                                         completions of a zero-copy send. */
    VSR_IO_CQE_BUFFER = 1u << 1,      /* buffer_id names the provided buffer. */
    VSR_IO_CQE_BUFFER_MORE = 1u << 2, /* Incremental consumption: the kernel
                                         keeps the rest of that buffer. */
    VSR_IO_CQE_NOTIF = 1u << 3        /* Zero-copy send: buffers released. */
};

/* 16 bytes. result is the operation result or a negative errno. */
struct vsr_io_cqe {
    uint64_t user_data;
    int32_t result;
    uint16_t flags; /* enum vsr_io_cqe_flags */
    uint16_t buffer_id;
};

struct vsr_io_region {
    void *base;
    size_t size;
};

struct vsr_io_buffer {
    void *base;
    uint32_t length;
    uint16_t id;
    uint16_t reserved;
};

enum vsr_io_buffer_ring_flags {
    VSR_IO_BUFFER_RING_INCREMENTAL = 1u << 0 /* Partial consumption allowed. */
};

/*
 * Two record kinds are registration work rather than kernel operations, so
 * that a planner emits it as data like any other record (docs/io-design.md
 * decision E7: the engine's primitives never call the executor).
 * FILES_UPDATE installs descriptors into registered slots from `offset`
 * (io_uring's FILES_UPDATE): each slot takes its own reference, the
 * descriptors stay open and the caller's, -1 empties a slot; the result is
 * the number of slots updated before the first failure, or -EBADF for a
 * bad first descriptor, -EINVAL for slots beyond the table (or before
 * registration), -EFAULT for a NULL addr; FIXED_FILE, FIXED_BUFFER,
 * BUFFER_SELECT and DIRECT are -EINVAL. PROVIDE appends the `length`
 * buffers at addr to ring `buffer_group` as provide() does, before any
 * other record of the same submit_and_wait batch reaches the kernel, so a
 * receive armed in the batch sees them; it produces no completion when it
 * succeeds and one with provide()'s errno when it fails; it takes no flags
 * and never follows a LINK record (-EINVAL). The buffers array is read
 * during the call.
 *
 * All functions except wake are called only by the owner thread. Errors are
 * negative errno values. Registered files, buffer regions, and buffer rings
 * are executor-wide resources shared by every user of the executor; the
 * engine allocates its slots, region indexes, and groups from the ranges the
 * options below reserve for it, and never touches others.
 *
 * submit_and_wait submits every record (the executor handles a full submission
 * queue internally), then waits until `want` completions are available or
 * `deadline_ns` (executor clock; VSR_NO_DEADLINE never) passes. A positive
 * min_wait_ns batches: once at least one completion exists, waiting continues
 * only until min_wait_ns has elapsed since the call. want=0 with deadline_ns
 * <= now never blocks. reap moves up to capacity completions to the caller.
 * now is the monotonic clock in nanoseconds, always below VSR_NO_DEADLINE.
 * random fills bytes from the executor's entropy source: getrandom in
 * production, the seeded generator in simulation.
 * wake makes a blocked or future submit_and_wait return promptly; it is the
 * only thread-safe entry point.
 */
struct vsr_io_executor_ops {
    uint64_t (*now)(void *ctx);
    void (*random)(void *ctx, void *bytes, size_t size);
    int (*submit_and_wait)(void *ctx, const struct vsr_io_sqe *sqes,
                           uint32_t count, uint32_t want, uint64_t min_wait_ns,
                           uint64_t deadline_ns);
    uint32_t (*reap)(void *ctx, struct vsr_io_cqe *cqes, uint32_t capacity);
    int (*register_files)(void *ctx, uint32_t slots);
    int (*update_file)(void *ctx, uint32_t slot, int fd); /* fd < 0 clears */
    int (*register_buffers)(void *ctx, uint32_t regions); /* sparse table */
    int (*update_buffer)(void *ctx, uint32_t index,
                         const struct vsr_io_region *region); /* NULL clears */
    int (*buffer_ring)(void *ctx, uint16_t group, uint32_t entries,
                       uint32_t flags, const struct vsr_io_region *memory);
    int (*provide)(void *ctx, uint16_t group,
                   const struct vsr_io_buffer *buffers, uint32_t count);
    void (*wake)(void *ctx);
};

struct vsr_io_executor {
    const struct vsr_io_executor_ops *ops;
    void *ctx;
};

/*
 * Production executor over one io_uring instance. The ring is created with
 * SINGLE_ISSUER and DEFER_TASKRUN, a registered ring descriptor, and the CQ
 * size below; its rings are the kernel's pages, mapped by init and unmapped
 * by deinit. wake uses an eventfd polled by the ring. sqpoll_idle_ms > 0
 * selects SQPOLL; napi_busy_poll_us > 0 registers
 * NAPI busy polling. IOPOLL rings cannot serve sockets and are not created
 * here. file_slots and buffer_regions size the sparse registered tables;
 * the memory of a provided-buffer ring is the caller's, handed to
 * buffer_ring (the engine keeps it in its metadata region).
 */
struct vsr_io_uring_options {
    uint32_t sq_entries;
    uint32_t cq_entries;
    uint32_t file_slots;
    uint32_t buffer_regions;
    uint32_t sqpoll_idle_ms;
    uint32_t sqpoll_cpu; /* UINT32_MAX: unpinned */
    uint32_t napi_busy_poll_us;
    uint32_t reserved;
};

struct vsr_io_need {
    size_t size;
    size_t alignment;
};

/* Executor state, one page-aligned region. Returns OK/EINVAL/ELIMIT. */
int vsr_io_uring_layout(const struct vsr_io_uring_options *options,
                        struct vsr_io_need *need);
/* memory must satisfy the layout; returns OK or a negative errno: -ENOSYS
 * for a kernel without io_uring or older than the baseline, -EPERM when
 * io_uring is disabled for the caller, -EINVAL for options the kernel
 * refuses. */
int vsr_io_uring_init(void *memory, size_t size,
                      const struct vsr_io_uring_options *options,
                      struct vsr_io_executor *out);
/* Cancels every record still in flight and discards its completion, then
 * closes the ring: every registered resource is released and the memory
 * is the caller's. A record the kernel cannot cancel (a zero-copy send
 * awaiting its NOTIF, an operation in progress in a kernel worker) still
 * completes during the kernel's asynchronous teardown, into the record's
 * buffers: see such records complete before calling this. */
void vsr_io_uring_deinit(struct vsr_io_executor *executor);
/* The ring descriptor, readable when completions are pending, for callers
 * that embed the loop in epoll or another ring's EPOLL_WAIT. */
int vsr_io_uring_fd(const struct vsr_io_executor *executor);

/* -------------------------------------------------------------------------
 * Addresses and nodes
 *
 * A NODE is a transport identity: one process that may host replicas of
 * several groups. Replica IDs are per cluster (vsr_member.id); the caller
 * tells the engine which node may act as which replica of which cluster.
 * That table is used both to dial peers and to authorize inbound envelopes,
 * so it is also the administrative authorization list the core contract
 * requires for members, learners, and discovery peers.
 * ---------------------------------------------------------------------- */

/* A socket address as the kernel takes it: AF_INET, AF_INET6, or AF_UNIX,
 * pathname or abstract. length is the number of bytes of sockaddr in use. */
struct vsr_io_address {
    uint32_t length;
    uint32_t reserved;
    struct sockaddr_storage sockaddr;
};

/*
 * Link authentication. The handshake is a sans-IO state machine per link.
 * TRUSTED: peers assert their node ID; integrity by CRC32C only. For private
 *   fabrics or an authenticating underlay such as WireGuard or IPsec.
 * EXTERNAL: after TCP setup the engine emits a HANDSHAKE op with the raw
 *   descriptor; the caller runs any protocol it likes (TLS with kTLS, for
 *   example) without blocking the loop and completes with the node identity.
 * KEYED: reserved for a built-in challenge-response over a keyed hash; not
 *   implemented, EINVAL.
 * The mode is a cluster-wide policy; a peer proposing another is refused.
 */
enum vsr_io_handshake_mode {
    VSR_IO_HANDSHAKE_TRUSTED,
    VSR_IO_HANDSHAKE_EXTERNAL,
    VSR_IO_HANDSHAKE_KEYED
};

/* -------------------------------------------------------------------------
 * Engine
 * ---------------------------------------------------------------------- */

struct vsr_io;
struct vsr_io_replica;
struct vsr_io_op;
struct vsr_io_event;

/* Fixed capacities, positive except caller_slabs. Memory is sized from them,
 * never grown. */
struct vsr_io_limits {
    uint32_t replicas;       /* Groups attachable to this engine. */
    uint32_t nodes;          /* Node table entries. */
    uint32_t authorizations; /* (cluster, replica) -> node entries. */
    uint32_t links;          /* Connections of every kind, both directions. */
    uint32_t link_queue;     /* Pending SEND ops per node before RETRY. */
    uint32_t streams;        /* Concurrent bulk streams, both roles. */
    uint32_t stream_window;  /* Outstanding DATA or WRITE per stream. */
    uint32_t events;         /* Queued caller events per replica. */
    uint32_t ops;            /* Forwarded ops queued for vsr_io_poll. */
    uint32_t batch;          /* Records per vsr_io_prepare. */
    uint32_t slabs;          /* Payload pool: slab count... */
    uint32_t slab_bytes;     /* ...and size, a multiple of the page size. */
    uint32_t caller_slabs;   /* Slabs kept for vsr_io_slab_acquire; 0: none. */
    uint32_t file_slots; /* Registered file slots reserved for the engine. */
    uint32_t buffer_regions; /* Registered regions reserved for the engine. */
};

/*
 * Sizing rules, checked by vsr_io_layout and vsr_io_attach (ELIMIT):
 * slab_bytes must hold the largest frame of every attached replica (its
 * message_bytes plus every header the frame carries, computed from the
 * core limits), one stream chunk plus framing, and the largest store record
 * plus two blocks of alignment, so a frame fits one slab, a straddling
 * frame is copied into a fresh one, and a cold LOAD reads its records into
 * one slab. slabs must be at least links + streams * (stream_window + 1) +
 * 4 * replicas + 5 + caller_slabs. The pool has three shares: the engine's
 * reserve of links + streams * stream_window + 4 * replicas + 1 slabs (a
 * send slab per link for its frame headers, the chunk reads of every
 * stream window, per replica a cold-load or recovery read slab and three
 * snapshot staging slabs, one reassembly slab), the caller's share, and
 * the ring's, at least streams + 4 slabs; whatever part of the first two
 * is not held stays out of the ring, since the kernel returns a provided
 * slab only once it has filled it. More slabs than the minimum buy
 * receive throughput.
 * file_slots covers the listeners, every link, one per stream (the file a
 * served stream reads), and per replica the log plus one transient
 * clients file. buffer_regions covers one region for the payload pool plus
 * one per attached replica for its tail buffers.
 */

struct vsr_io_options {
    struct vsr_io_executor executor;
    uint64_t node;                       /* Own node identity, nonzero. */
    const struct vsr_io_address *listen; /* Accept peers and streams here;
                                             copied by vsr_io_init; set up
                                             through the executor on the first
                                             prepare, a bind or listen failure
                                             is fatal: vsr_io_stats.failure. */
    uint32_t listen_count;               /* Zero: dial only; at most 8. */
    uint32_t handshake;                  /* enum vsr_io_handshake_mode */
    struct vsr_io_limits limits;
    uint32_t file_slot_base;     /* First executor slot/region/group index */
    uint32_t buffer_region_base; /* the engine may use; the caller keeps */
    uint16_t buffer_group;       /* the rest for itself. */
    uint8_t owner;               /* user_data owner tag of the engine. */
    uint8_t nodelay;             /* TCP_NODELAY on every link (default 1). */
    uint64_t connect_backoff_ns; /* Initial redial delay; doubles to 16x. */
    uint64_t handshake_timeout_ns;
    uint64_t idle_timeout_ns;     /* Close a link idle this long; 0 never. */
    uint64_t wait_min_ns;         /* vsr_io_run: completion batching. */
    uint32_t wait_min_complete;   /* vsr_io_run: completions to batch for. */
    uint32_t send_coalesce_bytes; /* Gather up to this many bytes per send. */
    uint32_t zero_copy_bytes;     /* Sends at or above use SEND_ZERO_COPY. */
    uint32_t stream_chunk_bytes;  /* Bulk stream frame payload. */
    uint32_t cache_line_bytes;    /* Power of two; 0 selects 64. */
    uint32_t reserved;
};

/* Regions the caller supplies: metadata (engine arena) and the payload pool,
 * which is page-aligned and registered with the executor. */
struct vsr_io_layout {
    struct vsr_io_need metadata;
    struct vsr_io_need payload;
};

int vsr_io_layout(const struct vsr_io_options *options,
                  struct vsr_io_layout *layout);
/*
 * Copies options; registers the payload pool region at buffer_region_base
 * and the provided-buffer ring at buffer_group with the executor (the file
 * and buffer tables themselves are executor-wide and registered by whoever
 * created the executor); queues the listener setup for the first prepare. Regions must satisfy the layout and stay fixed until
 * deinit. Returns OK, EINVAL, ELIMIT, or a negative errno from the
 * executor's registrations.
 */
int vsr_io_init(const struct vsr_io_options *options,
                const struct vsr_io_region *metadata,
                const struct vsr_io_region *payload, struct vsr_io **out);
/* Stops accepting, closes every link and stream after their pins end, and
 * releases executor registrations. Requires no attached replica. Asynchronous:
 * keep driving the loop until vsr_io_stats.closed, then deinit. */
int vsr_io_close(struct vsr_io *io);
int vsr_io_deinit(struct vsr_io *io); /* EBUSY until closed. */
/* Thread-safe. Wakes the owner's loop; the next poll sees nothing new. */
void vsr_io_wake(struct vsr_io *io);

/*
 * Node table and authorization. A node must be set before it is authorized
 * (EINVAL otherwise); clearing it removes its authorizations. Changing a
 * node's address closes its links, and revoking an authorization closes
 * the node's links once no (cluster, replica) names it any more; a SEND
 * queued on them completes with RETRY. authorize with node = VSR_IO_NO_NODE
 * revokes. A NULL address records a CALLER-DIALED node: the engine never
 * connects to it and instead emits a LINK_WANTED op, on its redial backoff
 * schedule, whenever it needs a link and has none; the caller connects
 * however it likes and adopts the socket.
 */
int vsr_io_node_set(struct vsr_io *io, uint64_t node,
                    const struct vsr_io_address *address);
int vsr_io_node_clear(struct vsr_io *io, uint64_t node);
int vsr_io_authorize(struct vsr_io *io, struct vsr_id cluster, uint64_t replica,
                     uint64_t node);

/*
 * Adopt a connected socket as a link; the engine owns the descriptor from
 * now on. With flags zero the caller has already run the cluster's handshake
 * or its own authentication, and node is the authenticated peer. HANDSHAKE
 * instead runs the configured handshake mode on the socket, as an inbound
 * link (node = VSR_IO_NO_NODE, identity from the handshake) or, with
 * OUTBOUND, as a dialed link to the expected node: this is how a
 * caller-owned listener hands over the peer connections it demultiplexes
 * and how a caller-dialed node gets its link. Every peer or stream
 * connection starts with VSR_IO_WIRE_MAGIC, so a caller-owned listener can
 * tell a peer from one of its own clients with a PEEK receive and adopt the
 * socket with its stream untouched. Returns OK, ELIMIT when no link slot is
 * free, or EINVAL.
 */
enum vsr_io_adopt_flags {
    VSR_IO_ADOPT_HANDSHAKE = 1u << 0,
    VSR_IO_ADOPT_OUTBOUND = 1u << 1 /* Only with HANDSHAKE. */
};

int vsr_io_adopt(struct vsr_io *io, int fd, uint64_t node, uint32_t flags);

/* Link state toward one node, for role and health decisions. Several
 * established links to one node are legal (both ends dialed at once): the
 * link dialed by the lower node identity, oldest first, carries the sends,
 * so both ends agree and the others go idle and are closed by the idle
 * timeout. */
enum vsr_io_node_state {
    VSR_IO_NODE_UNLINKED, /* No link and none in progress. */
    VSR_IO_NODE_PENDING,  /* Dialing, waiting for the caller, or handshaking. */
    VSR_IO_NODE_LINKED    /* At least one established link. */
};

struct vsr_io_node_status {
    uint32_t state;            /* enum vsr_io_node_state */
    uint32_t links;            /* Established links of every kind. */
    uint64_t linked_since_ns;  /* Executor clock; 0 while unlinked. */
    uint64_t last_received_ns; /* Last frame accepted from the node; 0 never. */
    uint64_t next_dial_ns;     /* Backoff deadline while UNLINKED, else
                                  VSR_NO_DEADLINE. */
    int32_t last_error;        /* Negative errno of the last failure, or 0. */
    uint32_t reserved;
};

/* EINVAL for an unknown node. */
int vsr_io_node_status(const struct vsr_io *io, uint64_t node,
                       struct vsr_io_node_status *status);

/* -------------------------------------------------------------------------
 * Loop entry points
 *
 * One iteration, in this order (see docs/io-design.md for the diagram):
 *   1. reap:     ex->reap(); for each cqe: owner tag == engine ?
 *                vsr_io_complete() : the caller's own handling
 *   2. run:      do { vsr_io_poll(); caller steps; vsr_io_submit(); }
 *                while (ops returned or events submitted)
 *   3. prepare:  vsr_io_prepare() into the batch; caller appends its own
 *                records; deadline = min(engine, caller)
 *   4. block:    ex->submit_and_wait(batch, want, min_wait, deadline)
 * vsr_io_complete only translates and queues. vsr_io_poll steps every core
 * with its queued events plus one TIME(now) event, coalescing through
 * vsr_step_many, executes what the engine owns into internal link and store
 * queues, and returns the forwarded ops. vsr_io_prepare flushes those queues
 * into records: sends coalesced per link, STOREs packed into one write and,
 * if a SYNC is pending, one flush; receives and accepts re-armed; recycled
 * slabs provided. The engine arms no timer of its own: every engine
 * deadline (core deadlines, dial backoff, handshake and idle timeouts,
 * flush interval, sync delay, stream inactivity) is reported as the
 * *deadline_ns of vsr_io_prepare, which the loop passes to submit_and_wait.
 * The iteration is therefore the batching boundary, and wait_min_ns/want
 * in step 4 trade latency for batching.
 * now_ns is read once per iteration from the executor and shared by caller
 * and engine so simulation time reaches the core through the same path.
 * ---------------------------------------------------------------------- */

enum vsr_io_poll_flags {
    VSR_IO_POLL_MORE = 1u << 0, /* Runnable work remains; poll again before
                                   prepare, with or without new events. */
    VSR_IO_POLL_OUTPUT_FULL = 1u << 1 /* ops array filled; implies MORE. */
};

int vsr_io_complete(struct vsr_io *io, const struct vsr_io_cqe *cqes,
                    uint32_t count);
int vsr_io_poll(struct vsr_io *io, uint64_t now_ns, struct vsr_io_op *ops,
                uint32_t capacity, uint32_t *count, uint32_t *flags);
/* Accepts a prefix of events in order; unconsumed events are resubmitted
 * after the next poll. Reasons for stopping mirror vsr_step_many, plus one of
 * the engine's own: a REQUEST from a client incarnation the store does not
 * know, while its client table (counting incarnations in flight) is full, is
 * left unconsumed with ELIMIT, and the caller answers LIMIT itself. With the
 * same max_clients on every replica, backups therefore never exceed theirs. */
int vsr_io_submit(struct vsr_io *io, const struct vsr_io_event *events,
                  uint32_t count, uint32_t *consumed);
/* Fills at most capacity records; *deadline_ns is the earliest engine
 * deadline or VSR_NO_DEADLINE, and must reach submit_and_wait. Records left
 * over stay queued. */
int vsr_io_prepare(struct vsr_io *io, uint64_t now_ns, struct vsr_io_sqe *sqes,
                   uint32_t capacity, uint32_t *count, uint64_t *deadline_ns);

/*
 * Ready-made loop for callers without one. It is the iteration above with
 * three hooks; these are the only callbacks in this header and are optional
 * for a caller that owns no I/O. complete receives the caller's completions;
 * step receives forwarded ops and returns events to submit; prepare appends
 * the caller's records and returns its deadline. vsr_io_run returns when
 * every replica is detached and the engine is closed, or on executor error.
 */
struct vsr_io_hooks {
    void *ctx;
    void (*complete)(void *ctx, const struct vsr_io_cqe *cqe);
    uint32_t (*step)(void *ctx, const struct vsr_io_op *ops, uint32_t count,
                     struct vsr_io_event *events, uint32_t capacity);
    uint64_t (*prepare)(void *ctx, struct vsr_io_sqe *sqes, uint32_t capacity,
                        uint32_t *count);
};

int vsr_io_run(struct vsr_io *io, const struct vsr_io_hooks *hooks);

/* -------------------------------------------------------------------------
 * Application boundary: forwarded ops and caller events
 *
 * Every op is a struct vsr_op inside a wrapper naming its replica and kind.
 * Kind CORE carries a core op verbatim, with the same pin and completion
 * rules as vsr.h: complete it with a CORE event of type VSR_EVENT_COMPLETE
 * and the same id. Forwarded core ops are APPLY, READ_READY, all SNAPSHOT_*
 * ops, REPLY (arg is the caller's route, opaque to the engine), and RELEASE
 * of caller leases. The other kinds are engine-level rails; their op.data
 * points at the kind's descriptor, op.id is nonzero when a completion is
 * required and zero for informational kinds, and op.arg is per kind.
 *
 * Leases: caller lease IDs must have VSR_IO_LEASE_ENGINE clear; the engine's
 * own leases have it set and never reach the caller. Reply routes and read
 * cookies are the caller's; the engine passes them through unchanged.
 *
 * Snapshot ops are joint (docs/io-design.md, "Joint snapshot ops"). The
 * engine generates the snapshot id: the SNAPSHOT_CAPTURE it forwards
 * carries a task whose checkpoint template already has the id, and the
 * caller returns that id with its manifest; the engine writes the
 * checkpoint's client table to clients-<id> meanwhile and completes to
 * the core only when both halves are done. SNAPSHOT_FETCH is forwarded
 * only after the engine pulled its clients file from the peer over its
 * own stream (a lost stream completes the core op with RETRY without
 * involving the caller); SNAPSHOT_SYNC and SNAPSHOT_DROP are forwarded and
 * completed to the core when the file is durable, or unlinked, and the
 * caller completed. SNAPSHOT_INSTALL is forwarded unchanged.
 * ---------------------------------------------------------------------- */

#define VSR_IO_LEASE_ENGINE (UINT64_C(1) << 63)

enum vsr_io_op_kind {
    VSR_IO_OP_CORE,         /* op: vsr_op verbatim; replica set. */
    VSR_IO_OP_HANDSHAKE,    /* data: vsr_io_handshake; completion required. */
    VSR_IO_OP_STREAM_SERVE, /* data: vsr_io_stream_serve; completion needed. */
    VSR_IO_OP_STREAM_DATA,  /* data: vsr_io_stream_data; completion releases
                               the slab bytes. */
    VSR_IO_OP_STREAM_END,   /* data: vsr_io_stream_end; informational, on
                               both sides of a stream. */
    VSR_IO_OP_STREAM_WRITTEN, /* data: vsr_io_stream_written; informational:
                                 that write's buffers or file range are no
                                 longer read. */
    VSR_IO_OP_STATUS,         /* data: vsr_status of replica, borrowed until
                               the next poll; informational. Emitted when
                               STATE_CHANGED, and once at STOPPED. */
    VSR_IO_OP_LINK_WANTED     /* data: vsr_io_link_wanted; informational: a
                               caller-dialed node needs a link. */
};

/* 48 bytes on common 64-bit ABIs. */
struct vsr_io_op {
    struct vsr_io_replica *replica; /* NULL for engine-level kinds. */
    uint32_t kind;                  /* enum vsr_io_op_kind */
    uint32_t reserved;
    struct vsr_op op;
};

enum vsr_io_direction { VSR_IO_INBOUND, VSR_IO_OUTBOUND };

/*
 * EXTERNAL handshake. The descriptor is connected, the VSR_IO_WIRE_MAGIC
 * preamble has been sent (OUTBOUND) or consumed (INBOUND), and the
 * descriptor is owned by the caller until completion. Complete with status
 * OK and data = vsr_io_handshake_done to install the link, or any other
 * status to close it. The engine does not read or write the socket
 * meanwhile. OUTBOUND names the expected node; an INBOUND identity must be
 * a node in the table.
 */
struct vsr_io_handshake {
    int32_t fd;
    uint32_t direction; /* enum vsr_io_direction */
    uint64_t node;      /* OUTBOUND: expected; INBOUND: VSR_IO_NO_NODE. */
    struct vsr_io_address peer;
};

struct vsr_io_handshake_done {
    uint64_t node; /* Authenticated identity; must match for OUTBOUND. */
};

/*
 * A caller-dialed node (vsr_io_node_set with a NULL address) has no link.
 * Connect and vsr_io_adopt with HANDSHAKE | OUTBOUND; the op repeats on the
 * redial backoff schedule until a link exists.
 */
struct vsr_io_link_wanted {
    uint64_t node;
    uint32_t attempt; /* Consecutive emissions without a link, from 1. */
    uint32_t reserved;
};

/*
 * Bulk streams: a byte flow from a source node to a requester over its own
 * connection, so a large transfer never delays protocol messages. Request
 * bytes are the caller's, which is where differential transfer lives. Every
 * chunk is CRC32C-checked by the engine. Flow control is credit: at most
 * stream_window DATA ops outstanding at the requester and WRITE events
 * outstanding at the source; receiving pauses when the caller is behind.
 *
 * Requester: submit STREAM_OPEN {id = own cookie, node, request bytes under
 * a caller lease}; receive STREAM_DATA ops in order and complete each after
 * consuming or copying its bytes; receive STREAM_END exactly once with the
 * status and total bytes, after which the cookie may be reused. A caller
 * fulfilling SNAPSHOT_FETCH completes the core op only after STREAM_END.
 *
 * Source: receive STREAM_SERVE {stream, node, request bytes}; complete it
 * with OK to accept or any other status to refuse; then submit STREAM_WRITE
 * events, each either caller buffers under a lease or a file range on a
 * registered slot, in order (vsr_io_submit refuses one with AGAIN while
 * stream_window writes are queued: resubmit after a STREAM_WRITTEN);
 * STREAM_WRITTEN reports when a write's bytes are no longer read, for every
 * queued write in order, also when the stream ends early; submit
 * STREAM_CLOSE {stream, status} last, status an enum vsr_io_status (else
 * EINVAL). CLOSE is taken once, also after the stream ended under the caller
 * (connection loss, a read failure, the engine closing) until its STREAM_END
 * is emitted, while WRITE is EINVAL once the stream ended; a CLOSE or WRITE
 * racing a STREAM_END not yet polled may be EINVAL. A file range is on a
 * slot the caller owns: one of the engine's file_slots is EINVAL. The engine
 * sends buffers zero-copy; a file range is read into pool slabs,
 * stream_chunk_bytes at a time with fixed-buffer reads, and sent chunk by
 * chunk exactly like buffers, so no pipe or splice is involved; a read
 * error, or a file ending inside the range, ends the stream with FAILED.
 * Connection loss ends the stream on both sides with VSR_IO_RETRY, as does
 * no progress for handshake_timeout_ns. Request bytes beginning with
 * VSR_IO_LIBRARY_MAGIC are the engine's own and are never offered to the
 * caller as STREAM_SERVE, nor accepted from a caller's STREAM_OPEN.
 *
 * Leases: the engine emits no RELEASE op for the lease of a STREAM_OPEN or
 * a STREAM_WRITE. The request bytes are no longer read once the stream's
 * STREAM_END arrived; a write's buffers once its STREAM_WRITTEN did.
 */
struct vsr_io_stream_serve {
    uint64_t stream;
    uint64_t node;
    struct vsr_span request;
};

struct vsr_io_stream_data {
    uint64_t stream;       /* The requester's cookie. */
    uint64_t offset;       /* Byte position of this chunk in the stream. */
    struct vsr_span bytes; /* Pool slab bytes; pinned until completion. */
};

struct vsr_io_stream_end {
    uint64_t stream; /* Requester: its cookie; source: the served handle. */
    uint64_t bytes;  /* Bytes delivered or sent before the end. */
    int32_t status;  /* enum vsr_io_status */
    uint32_t reserved;
};

struct vsr_io_stream_written {
    uint64_t stream;
    uint64_t write; /* The caller's write id from vsr_io_stream_write. */
};

enum vsr_io_event_kind {
    VSR_IO_EVENT_CORE,         /* event: vsr_event verbatim; replica set.
                                  Accepted types: REQUEST, CLIENT_QUERY, READ,
                                  CHECKPOINT, COMPLETE, STOP. TIME and MESSAGE
                                  are the engine's and return EINVAL. */
    VSR_IO_EVENT_COMPLETE,     /* Completion of a HANDSHAKE, STREAM_SERVE or
                                  STREAM_DATA op: event.id = op id and
                                  event.status; data is a vsr_io_handshake_done
                                  for a successful HANDSHAKE, copied during the
                                  call, else NULL; lease is zero. */
    VSR_IO_EVENT_STREAM_OPEN,  /* event.id: cookie; data: vsr_io_stream_open;
                                  lease covers the request bytes. */
    VSR_IO_EVENT_STREAM_WRITE, /* data: vsr_io_stream_write; lease covers
                                  caller buffers, zero for a file range. */
    VSR_IO_EVENT_STREAM_CLOSE  /* event.id: stream; event.status. */
};

/* 48 bytes on common 64-bit ABIs. */
struct vsr_io_event {
    struct vsr_io_replica *replica; /* NULL for engine-level kinds. */
    uint32_t kind;                  /* enum vsr_io_event_kind */
    uint32_t reserved;
    struct vsr_event event;
};

struct vsr_io_stream_open {
    uint64_t node;
    struct vsr_span request; /* At most VSR_IO_STREAM_REQUEST_BYTES. */
};

enum vsr_io_write_kind { VSR_IO_WRITE_BUFFERS, VSR_IO_WRITE_FILE };

struct vsr_io_stream_write {
    uint64_t stream;
    uint64_t write;  /* Caller's write id, reported by STREAM_WRITTEN. */
    uint32_t kind;   /* enum vsr_io_write_kind */
    uint32_t slot;   /* FILE: registered file slot owned by the caller. */
    uint64_t offset; /* FILE: byte offset. */
    uint64_t length; /* FILE: byte count. */
    struct vsr_blob buffers; /* BUFFERS: gathered in order. */
};

#define VSR_IO_STREAM_REQUEST_BYTES 4096u

/* Engine-level completions and STREAM_END use enum vsr_io_status of vsr.h
 * with the same meanings: OK, RETRY for a lost connection or refused
 * stream, FAILED for a permanent error, CANCELLED when the engine closes. */

/* -------------------------------------------------------------------------
 * Payload pool for the caller
 *
 * The caller may take slabs of the registered payload pool for its own
 * bytes: request bodies it will submit, file I/O it issues itself. A taken
 * slab is the caller's until released, and the engine does not provide it
 * to the kernel meanwhile. The send rule is exact: a coalesced vectored
 * send whose vectors all lie inside the payload pool (frame headers always
 * do; bodies do when they are received bytes or caller bytes in taken
 * slabs) goes SEND_ZERO_COPY | VECTORED with FIXED_BUFFER on the pool's
 * single registered region; a send with any vector outside the pool (tail
 * buffers, the core arena, caller memory) goes SEND_ZERO_COPY | VECTORED
 * without FIXED_BUFFER, which pins the pages per send, when its bytes reach
 * zero_copy_bytes, and plain VECTORED SEND (kernel copy) below. Release a
 * slab only when no lease, op, or record of the caller still covers it.
 * The caller holds at most limits.caller_slabs slabs at once, and the
 * engine never hands the part of that share the caller does not hold to
 * the kernel, which returns a provided slab only once it has filled it.
 * acquire returns OK, ELIMIT once the caller holds caller_slabs slabs (or,
 * transiently, while the engine's own users hold more than its reserve and
 * use the untaken share), or EINVAL (a closing engine included).
 * ---------------------------------------------------------------------- */

struct vsr_io_slab {
    void *base;
    uint32_t length; /* slab_bytes */
    uint16_t id;     /* Provided-buffer id, for release. */
    uint16_t region; /* Registered region index, for FIXED_BUFFER records. */
};

int vsr_io_slab_acquire(struct vsr_io *io, struct vsr_io_slab *slab);
int vsr_io_slab_release(struct vsr_io *io, uint16_t id);

/* -------------------------------------------------------------------------
 * Replicas and the store
 *
 * A replica binds one core instance to one directory holding its store: the
 * file `log`, a device-style preallocated file of superblocks and segment
 * slots, and one `clients-<snapshot id>` file per retained checkpoint holding
 * that checkpoint's completed-client table. The engine executes LOAD, STORE,
 * SYNC and RECLAIM against it. Its contract is the storage section of
 * docs/vsr-api.md; what follows are the physical policies.
 *
 * STORE completes as soon as its record is packed into the tail buffers and
 * indexed: readable, not durable. Only SYNC waits for the write and a flush.
 * Records are packed by copy into the block-aligned registered tail ring,
 * which mirrors the file and is also the read cache; the file is opened
 * with O_DIRECT unless direct_io is zero. A written block is never
 * rewritten: every write starts at a block boundary, pads its last block,
 * and the next write starts at the next boundary. One write per loop
 * iteration covers everything packed since the last one. A STORE
 * completion is held only while its record would overwrite ring bytes
 * still referenced by a LOAD lease or not yet written, or while unwritten
 * bytes exceed write_behind_bytes; a RESTORE, or a PUBLISH of a snapshot
 * other than the latest capture, also waits for its clients file to be
 * read. Those are the only times storage latency reaches the core. In
 * replicated mode the core never issues SYNC, so the log is write-behind:
 * flushed every flush_interval_ns, and its only role is recovery and
 * catch-up. Recovery scans live segments from the newer superblock; a
 * record that is short, fails its CRC, or breaks the sequence ends the log;
 * a bad record below the acknowledged durable prefix is CORRUPT, the
 * prefix being the greatest durable sequence any persisted record or
 * superblock carries, so only the transactions acknowledged by the last
 * flush before a crash, until a later record or the idle superblock write
 * (within flush_interval_ns, 100 ms when zero) persists that sequence, can
 * read as a torn tail instead. NEW and
 * JOIN create the store; RECOVER opens it and reports NOT_FOUND when the
 * directory has no log. An index overflow (more than max_entries ops
 * retained by unreclaimed revisions, or more than max_clients incarnations
 * reached other than through vsr_io_submit) fails the STORE with FAILED,
 * which fences the replica.
 * ---------------------------------------------------------------------- */

enum vsr_io_sync_mode {
    VSR_IO_SYNC_DSYNC,    /* Write with O_DSYNC; no separate flush. */
    VSR_IO_SYNC_FDATASYNC /* Plain write, then fdatasync. */
};

enum vsr_io_write_error {
    VSR_IO_WRITE_ERROR_FENCE,   /* Any failed write fences the replica. */
    VSR_IO_WRITE_ERROR_CONTINUE /* Replicated mode only: keep serving from
                                   memory; recovery will need a quorum. */
};

struct vsr_io_store_options {
    uint32_t block_bytes;        /* Power of two >= 512; default 4096. */
    uint32_t segments;           /* Slots allocated at creation. */
    uint32_t max_segments;       /* Growth bound; equal to segments: fixed. */
    uint32_t max_entries;        /* Retained log entries the index can hold. */
    uint32_t max_clients;        /* Distinct client incarnations the table
                                    holds: a deployment-wide limit, identical
                                    on every replica and enforced at
                                    vsr_io_submit; an overflow reached any
                                    other way fences, since the contract never
                                    forgets one. */
    uint32_t inflight_writes;    /* Concurrent record writes; default 2. */
    uint64_t segment_bytes;      /* Multiple of block_bytes; default 64 MiB. */
    uint64_t sync_delay_ns;      /* Hold a flush to batch SYNCs; default 0. */
    uint64_t flush_interval_ns;  /* Replicated mode write-behind cadence. */
    uint64_t write_behind_bytes; /* Unwritten bytes before STORE waits. */
    uint64_t cache_bytes;        /* Tail ring, a multiple of block_bytes and at
                               least write_behind_bytes + the core's
                               pinned_payload_bytes + twice the largest
                               record + twice the segment header +
                               block_bytes, so a held STORE always
                               proceeds. */
    uint8_t direct_io;           /* Default 1. */
    uint8_t sync_mode;           /* enum vsr_io_sync_mode */
    uint8_t on_write_error;      /* enum vsr_io_write_error */
    uint8_t reserved[5];
};

struct vsr_io_store_status {
    uint64_t readable; /* Last packed transaction. */
    uint64_t written;  /* Last transaction whose write completed. */
    uint64_t durable;  /* Last flushed transaction. */
    uint64_t log_begin;
    uint64_t log_end;
    uint64_t unwritten_bytes;
    uint64_t used_bytes;
    uint64_t capacity_bytes;
    uint32_t live_segments;
    uint32_t entries;
    uint32_t clients;
    int32_t error; /* Last errno from the store's I/O, or zero. */
};

struct vsr_io_replica_options {
    struct vsr_options core; /* Borrowed during attach; copied. */
    struct vsr_io_store_options store;
    const char *path; /* Store directory; must exist. Opened relative
                                to AT_FDCWD; a simulation maps that to the
                                node's virtual root. */
    uint32_t reserved;
};

/* metadata: replica bookkeeping, the core arena and the store indexes.
 * tail: block-aligned store tail buffers, registered with the executor. */
struct vsr_io_replica_layout {
    struct vsr_io_need metadata;
    struct vsr_io_need tail;
};

int vsr_io_replica_layout(const struct vsr_io *io,
                          const struct vsr_io_replica_options *options,
                          struct vsr_io_replica_layout *layout);
/*
 * Initializes the core in the metadata region, opens or creates the store
 * according to core.start_mode, and starts driving the instance on the next
 * poll. Returns OK, EINVAL, ELIMIT, EBUSY when limits.replicas is reached, or
 * a negative errno. The cluster's authorizations should be in place first;
 * peers that are not yet authorized are dialed once they are.
 */
int vsr_io_attach(struct vsr_io *io,
                  const struct vsr_io_replica_options *options,
                  const struct vsr_io_region *metadata,
                  const struct vsr_io_region *tail,
                  struct vsr_io_replica **out);
/* Only after the STATUS op reporting STOPPED and once the store's own
 * write-behind writes and flushes have completed: closes the store files
 * and releases the tail registration. EBUSY otherwise; keep driving the
 * loop. The regions are the caller's again. */
int vsr_io_detach(struct vsr_io_replica *replica);
void vsr_io_replica_status(const struct vsr_io_replica *replica,
                           struct vsr_status *core,
                           struct vsr_io_store_status *store);
struct vsr *vsr_io_replica_core(struct vsr_io_replica *replica);
struct vsr_io_replica *
vsr_io_replica_find(struct vsr_io *io, struct vsr_id cluster, uint64_t replica);

/* -------------------------------------------------------------------------
 * Engine statistics and health
 * ---------------------------------------------------------------------- */

struct vsr_io_stats {
    uint32_t replicas;
    uint32_t links;         /* Established, any kind. */
    uint32_t links_pending; /* Dialing or handshaking. */
    uint32_t streams;
    uint32_t slabs_free;
    uint32_t closed; /* 1 once vsr_io_close has finished. */
    int32_t failure; /* Negative errno of a fatal executor error. */
    uint32_t reserved;
    uint64_t bytes_sent;
    uint64_t bytes_received;
    uint64_t sends; /* Kernel send operations issued. */
    uint64_t sends_zero_copy;
    uint64_t messages_sent;    /* Core SEND ops completed OK. */
    uint64_t messages_retried; /* Core SEND ops completed RETRY. */
    uint64_t frames_rejected;  /* Bad CRC, unauthorized, or malformed. */
    uint64_t writes;           /* Store record writes. */
    uint64_t flushes;
};

void vsr_io_get_stats(const struct vsr_io *io, struct vsr_io_stats *stats);

#endif /* VSR_IO_H */
