#ifndef VSR_IO_LINK_H
#define VSR_IO_LINK_H

#include "io/codec.h"
#include "vsr-io.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/*
 * Links: nodes, authorizations, connections, the handshake, send
 * coalescing and receive framing (docs/io-implementation.md, "Links"). A
 * sans-IO planner: it consumes executor completions and core SEND ops, and
 * emits executor records through prepare and MESSAGE events into replica
 * queues. It never calls the executor.
 *
 * A NODE is a transport identity with an optional address and a dial
 * backoff. An AUTHORIZATION maps (cluster, replica) to a node. A LINK is
 * one connection of either purpose (peer or stream) in either direction.
 * Several established peer links to one node are legal, since two nodes may
 * dial each other at once; the CARRIER of a node's sends is, among its
 * established peer links, the one dialed by the node with the lower node
 * identity, oldest established first. Both ends compute the same carrier,
 * so the other links fall idle and idle_timeout_ns closes them.
 *
 * Sends are queued per NODE (vsr_io_limits.link_queue entries): a SEND op
 * waits while the node is pending and completes with RETRY when the node
 * becomes unlinked, is unknown or unauthorized, or when the queue is full
 * (the oldest is retried). One kernel send is in flight per link at a time,
 * because two sends on one socket may complete out of order; a send covers
 * as many queued messages as send_coalesce_bytes, VSR_IO_SEND_VECTORS and
 * the send slab allow, and a message may continue in the next send. Header
 * bytes of a send live in the link's SEND SLAB, a pool slab used as a ring
 * whose floor advances as zero-copy NOTIFs arrive; VSR_IO_LINK_SENDS sends
 * may await their NOTIF.
 *
 * The send flags rule (docs/io-design.md decision 38): every vector inside
 * the pool region -> SEND | ZERO_COPY | VECTORED | FIXED_BUFFER on the pool
 * region; any vector outside it and total bytes >= zero_copy_bytes ->
 * ZERO_COPY | VECTORED without FIXED_BUFFER; otherwise plain VECTORED SEND.
 *
 * Receiving: every link runs one multishot RECV with BUFFER_SELECT from the
 * pool's group. Delivered bytes are carved into frames in place; a frame
 * that straddles two slabs is copied into a fresh REASSEMBLY slab, and a
 * frame is decoded only when complete. Bytes not yet consumed keep their
 * slab referenced; when the pool runs dry the kernel stops delivering,
 * which is the intended backpressure.
 *
 * Descriptors (decision 72): a link's socket is a RAW descriptor until the
 * engine takes it over, and an engine FILE SLOT from then on. A dialed link
 * issues SOCKET, then CONNECT on the raw descriptor; a listener runs a plain
 * multishot ACCEPT; vsr_io_adopt brings a raw descriptor. The takeover
 * installs the descriptor into the slot through vsr_io_engine_install: at
 * the CONNECT or ACCEPT completion in TRUSTED mode, at the caller's OK
 * HANDSHAKE completion in EXTERNAL mode (the preamble is exchanged on the
 * raw descriptor before the op is emitted, and the caller owns it until the
 * completion), and at adopt. Every later record is FIXED_FILE on the slot,
 * which a CLOSE frees; a closed raw descriptor is a plain CLOSE.
 */

#define VSR_IO_SEND_VECTORS 128u /* Well under IOV_MAX; a message continues */
#define VSR_IO_LINK_SENDS 4u     /* Sends awaiting NOTIF per link. */
#define VSR_IO_BACKOFF_MAX_SHIFT 4u /* connect_backoff_ns doubles to 16x. */
#define VSR_IO_LISTENERS_MAX 8u     /* Listen addresses per engine. */
#define VSR_IO_LISTEN_BACKLOG 128u
#define VSR_IO_LINK_ORPHANS 16u  /* Accepted sockets awaiting a CLOSE. */
#define VSR_IO_PREAMBLE_BYTES 8u /* VSR_IO_WIRE_MAGIC on the wire. */

enum vsr_io_link_state {
    VSR_IO_LINK_FREE,
    VSR_IO_LINK_CONNECTING, /* SOCKET/CONNECT in flight (dialed). */
    VSR_IO_LINK_EXTERNAL,   /* HANDSHAKE op outstanding at the caller. */
    VSR_IO_LINK_HELLO,      /* TRUSTED: preamble/HELLO exchange pending. */
    VSR_IO_LINK_ESTABLISHED,
    VSR_IO_LINK_CLOSING /* SHUTDOWN/CANCEL issued; waiting for the recv to
                           terminate and every NOTIF to arrive. */
};

struct vsr_io_node {
    uint64_t id; /* 0: free entry. */
    struct vsr_io_address address;
    bool has_address;     /* Else caller-dialed: LINK_WANTED. */
    bool dialing;         /* A CONNECTING link exists, or LINK_WANTED was
                             emitted and its backoff has not expired. */
    bool wanted;          /* A link is needed: set by authorize and SEND,
                             cleared when a peer link is established or
                             idle-closed (the next SEND redials). */
    bool due;             /* The dial backoff expired; poll acts on it. */
    uint32_t attempts;    /* Consecutive failed dials or LINK_WANTED. */
    uint32_t carrier;     /* Link index or INDEX_NONE. */
    uint32_t established; /* Established links of every purpose. */
    uint32_t pending;     /* Links in CONNECTING, EXTERNAL or HELLO. */
    uint64_t next_dial_ns;
    uint64_t linked_since_ns;
    uint64_t last_received_ns;
    int32_t last_error;
    uint32_t queue_head; /* Send queue ring over the table's queue area. */
    uint32_t queue_count;
    uint32_t reserved;
};

struct vsr_io_authorization {
    struct vsr_id cluster; /* Zero: free entry. */
    uint64_t replica;
    uint64_t node;
};

/* One queued core SEND op. */
struct vsr_io_queued_send {
    uint64_t op;      /* Core op id. */
    uint32_t replica; /* Replica index. */
    uint32_t reserved;
    const struct vsr_message *message;
    uint32_t length; /* Body length from the digest. */
    uint32_t crc;
    uint64_t end; /* Stream offset after this message, once assigned. */
};

struct vsr_io_send {
    uint32_t slot;         /* Slot table index; INDEX_NONE when unused. */
    uint32_t state;        /* 0 free, 1 in flight, 2 sent awaiting NOTIF. */
    uint32_t header_begin; /* Send-slab ring range of its header bytes. */
    uint32_t header_end;
    uint64_t begin; /* Stream offsets covered. */
    uint64_t end;
    bool zero_copy;
    bool fixed;
};

/* Progress of a link within its state: the dial's two records, the
 * handshake's halves and the teardown. */
enum vsr_io_link_stage {
    VSR_IO_STAGE_NONE,
    VSR_IO_STAGE_SOCKET,        /* CONNECTING: SOCKET to issue (raw_fd < 0). */
    VSR_IO_STAGE_CONNECT,       /* CONNECTING: CONNECT to issue or in flight. */
    VSR_IO_STAGE_PREAMBLE_SEND, /* EXTERNAL dialer: preamble send pending. */
    VSR_IO_STAGE_PREAMBLE_RECV, /* EXTERNAL acceptor: 8-byte RECV pending. */
    VSR_IO_STAGE_HANDSHAKE      /* EXTERNAL: op emitted, or to emit when
                                   handshake_op is 0. */
};

struct vsr_io_link {
    uint32_t state;      /* enum vsr_io_link_state */
    uint32_t purpose;    /* enum vsr_io_link_purpose */
    uint32_t direction;  /* enum vsr_io_direction */
    uint32_t node_index; /* INDEX_NONE until identified. */
    uint64_t node;       /* Peer node id; NO_NODE until identified. */
    int32_t fd;          /* Registered file slot (FIXED_FILE), or -1. */
    int32_t raw_fd;      /* Raw descriptor before the takeover, or -1. */
    uint32_t stream;     /* Stream index for STREAM purpose. */
    uint32_t stage;      /* enum vsr_io_link_stage */
    uint64_t established_ns;
    uint64_t last_active_ns; /* Last byte received or send completed. */
    uint64_t nonce;          /* Own HELLO nonce. */
    uint64_t handshake_op;   /* EXTERNAL: op id of the HANDSHAKE, or 0. */
    struct vsr_io_address peer;
    uint32_t connect_slot; /* SOCKET/CONNECT slot, or the EXTERNAL preamble
                              send or receive. */
    bool hello_sent;       /* Own HELLO queued into the send slab. */
    bool hello_seen;       /* Peer's HELLO accepted. */
    bool torn_down;        /* CLOSING: the teardown records were emitted. */
    bool recv_starved;     /* RECV ended -ENOBUFS; re-arm once provided. */
    int32_t error;         /* Reason for closing, or 0. */
    unsigned char preamble[VSR_IO_PREAMBLE_BYTES]; /* EXTERNAL acceptor's
                                                       8-byte receive. */
    /* Receive side. */
    uint32_t recv_slot;
    uint32_t partial_slab; /* Slab holding an incomplete frame, or NONE. */
    uint32_t partial_offset;
    uint32_t partial_length;
    uint32_t reassembly_slab; /* Fresh slab a straddling frame is copied to. */
    uint32_t preamble_seen;   /* Bytes of the 8-byte preamble matched. */
    /* Send side. */
    uint32_t send_slab;   /* Pool slab holding header bytes; INDEX_NONE. */
    uint32_t header_head; /* Ring over the send slab. */
    uint32_t header_tail;
    uint32_t header_sent;     /* First header byte not yet covered by a send. */
    uint64_t stream_offset;   /* Bytes handed to sends. */
    uint64_t sent_offset;     /* Bytes whose send completed. */
    uint64_t notified_offset; /* Bytes released by NOTIF (or by a plain
                                 send's completion). */
    struct vsr_io_send sends[VSR_IO_LINK_SENDS];
    struct vsr_io_encoder encoder; /* Position in the message being sent. */
    uint32_t encoding;             /* Queue index being encoded, or NONE. */
    uint32_t inflight;             /* 1 while a send is in flight. */
    uint32_t deadline;             /* Deadline handle. */
    uint32_t shutdown_slot;
    struct vsr_io_vec *vecs; /* [VSR_IO_SEND_VECTORS] for the in-flight send */
};

enum vsr_io_listener_state {
    VSR_IO_LISTENER_FREE,
    VSR_IO_LISTENER_SETUP,  /* SOCKET/BIND/LISTEN/ACCEPT chain to emit. */
    VSR_IO_LISTENER_ACTIVE, /* Multishot ACCEPT armed. */
    VSR_IO_LISTENER_REARM,  /* ACCEPT terminated; to issue again. */
    VSR_IO_LISTENER_CANCEL, /* Shutdown: CANCEL and CLOSE to emit. */
    VSR_IO_LISTENER_CLOSING /* Waiting for the ACCEPT to terminate. */
};

/* One listen address (vsr_io_options.listen[i]). The listening socket is
 * DIRECT into an engine file slot; the chain shares one LISTEN slot whose
 * only counted completions are the ACCEPT's (SKIP_SUCCESS on the rest), so
 * a failed BIND or LISTEN completes it once with the error. */
struct vsr_io_listener {
    uint32_t state; /* enum vsr_io_listener_state */
    uint32_t file_slot;
    uint32_t slot;   /* Slot table index of the ACCEPT, or NONE. */
    uint32_t cancel; /* Slot of the CANCEL/CLOSE pair, or NONE. */
};

struct vsr_io_links {
    struct vsr_io_node *nodes;                   /* [limits.nodes] */
    struct vsr_io_authorization *authorizations; /* [limits.authorizations] */
    struct vsr_io_link *links;                   /* [limits.links] */
    struct vsr_io_queued_send *queue; /* [nodes * link_queue] rings. */
    struct vsr_io_vec *vecs;          /* [links * SEND_VECTORS] */
    uint32_t nodes_count;
    uint32_t authorizations_count;
    uint32_t links_count;
    uint32_t link_queue;
    uint32_t established; /* Stats. */
    uint32_t pending;
    uint32_t listeners;      /* Listener records still to set up or active. */
    uint32_t closing;        /* 1 after vsr_io_links_shutdown. */
    uint64_t frame_limit;    /* Largest frame accepted: slab_bytes. */
    uint64_t handshake_ops;  /* HANDSHAKE op ids issued, from 1. */
    uint32_t dials_due;      /* Nodes with `due` set. */
    uint32_t handshakes_due; /* EXTERNAL links whose op is still to emit. */
    uint32_t listen_started; /* 1 once the first prepare set listeners up. */
    uint32_t orphans_count;  /* Accepted descriptors with no free link,
                                closed by the next prepare. */
    int32_t orphans[VSR_IO_LINK_ORPHANS];
    struct vsr_io_listener listener_table[VSR_IO_LISTENERS_MAX];
};

/* Bytes and alignment of the tables for the limits. */
int vsr_io_links_size(const struct vsr_io_limits *limits, size_t *bytes,
                      size_t *alignment);
void vsr_io_links_init(struct vsr_io_links *links, void *memory, size_t size,
                       const struct vsr_io_limits *limits,
                       uint64_t frame_limit);

/* Node table and authorization, backing the public calls. Changing an
 * address or revoking closes the affected links and retries their queued
 * sends. */
int vsr_io_links_node_set(struct vsr_io *io, uint64_t node,
                          const struct vsr_io_address *address);
int vsr_io_links_node_clear(struct vsr_io *io, uint64_t node);
int vsr_io_links_authorize(struct vsr_io *io, struct vsr_id cluster,
                           uint64_t replica, uint64_t node);
/* Node of (cluster, replica), or NO_NODE. */
uint64_t vsr_io_links_lookup(const struct vsr_io_links *links,
                             struct vsr_id cluster, uint64_t replica);
uint32_t vsr_io_links_node_index(const struct vsr_io_links *links,
                                 uint64_t node);
int vsr_io_links_node_status(const struct vsr_io *io, uint64_t node,
                             struct vsr_io_node_status *status);
int vsr_io_links_adopt(struct vsr_io *io, int fd, uint64_t node,
                       uint32_t flags);

/*
 * Core SEND op from a replica: digests the message, resolves the member to
 * a node through the authorizations and queues it. Returns OK when queued,
 * or VSR_IO_RETRY when the op must be completed with RETRY at once (unknown
 * or unauthorized destination, queue full after retrying the oldest).
 */
int vsr_io_links_send(struct vsr_io *io, uint32_t replica, uint64_t op,
                      const struct vsr_message *message, uint64_t member);
/* Streams open their own links: dial `node` with STREAM purpose; the link
 * index is returned and the stream module is told when it is established
 * or fails. */
int vsr_io_links_open_stream(struct vsr_io *io, uint64_t node, uint32_t stream,
                             uint32_t *link);
/* Queues raw frames on a stream link (request, chunk, end): bytes in the
 * send slab and payload vectors. Returns EBUSY when the link cannot take
 * more, the stream module then waits for a send completion. */
int vsr_io_links_send_frame(struct vsr_io *io, uint32_t link, uint16_t kind,
                            const unsigned char *header, size_t header_bytes,
                            const struct vsr_io_vec *payload, uint32_t count,
                            uint32_t body_crc, uint64_t *end);
void vsr_io_links_close(struct vsr_io *io, uint32_t link, int32_t error);

/* Poll-time work: due dials and LINK_WANTED, HANDSHAKE ops the forwarded
 * ring could not take earlier, carrier election after state changes. Emits
 * LINK_WANTED and HANDSHAKE ops through the engine's forwarded-op queue. */
void vsr_io_links_poll(struct vsr_io *io, uint64_t now);
/* A popped LINK deadline (handshake or idle timeout of link `index`) or
 * DIAL deadline (backoff of node `index`), dispatched by the engine's poll
 * before vsr_io_links_poll. */
void vsr_io_links_deadline(struct vsr_io *io, uint16_t kind, uint32_t index,
                           uint64_t now);
/* Prepare-time work: listener setup, connects, HELLO frames, sends,
 * re-armed receives, shutdowns and closes. Stops when the record array or
 * the slot table is full and resumes next time. */
void vsr_io_links_prepare(struct vsr_io *io, struct vsr_io_sqe *sqes,
                          uint32_t capacity, uint32_t *count);
/* Completion dispatch by slot kind (LISTEN, CONNECT, RECV, SEND, SHUTDOWN). */
void vsr_io_links_complete(struct vsr_io *io, uint32_t slot,
                           const struct vsr_io_cqe *cqe);
/* Caller's HANDSHAKE completion (EXTERNAL mode). */
int vsr_io_links_handshake_done(struct vsr_io *io, uint64_t op, int32_t status,
                                const struct vsr_io_handshake_done *done);
/* vsr_io_close: stop listening, close every link; done when links_count
 * active is zero and every slot is free. */
void vsr_io_links_shutdown(struct vsr_io *io);

#endif /* VSR_IO_LINK_H */
