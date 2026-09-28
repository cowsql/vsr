#ifndef VSR_SIM_INTERNAL_H
#define VSR_SIM_INTERNAL_H

#include "vsr-sim.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

/*
 * Simulated world internals (docs/io-implementation.md, "Simulation").
 * Four source files share this header: world.c (creation, clock, generator,
 * event queue, scheduling, faults, trace, crash and restart), net.c
 * (sockets, listeners, connections, segments, partitions), disk.c (virtual
 * files and directories with the block persistence model, inspection) and
 * exec.c (the per-node executor: record semantics, completions, chains,
 * cancellation, timers, registered files, buffer regions and rings, and the
 * ownership checks). The world may malloc; it never calls back into engine
 * code except the trace hook.
 *
 * Determinism: every random draw comes from one PCG stream seeded from
 * options.seed and is taken in a fixed order per event; timed work is an
 * entry of one event queue ordered by (due time, sequence number); every
 * container is an array scanned in index order, never a hash table keyed
 * by addresses; a run replays exactly from the seed and the harness's
 * sequence of calls.
 *
 * Tables of objects, connections, listeners, directory entries, inodes and
 * operations hold pointers to separately allocated elements, so an element
 * address stays valid while the table grows; a freed element's index is
 * reused lowest first.
 */

#define VSR_SIM_NONE UINT32_MAX
#define VSR_SIM_NEVER UINT64_MAX
#define VSR_SIM_MAX_DIRECT 65536u
#define VSR_SIM_MAX_RING 32768u
#define VSR_SIM_MAX_VECS 1024u
#define VSR_SIM_PATH_BYTES 4096u
/* Bytes one direction of a connection holds (in flight plus unread) before
 * a SEND waits for space; a SEND takes what fits, so results are short. */
#define VSR_SIM_SOCKET_BUFFER (UINT64_C(256) * 1024)
#define VSR_SIM_FIRST_FD 3
#define VSR_SIM_EPHEMERAL_PORT 40000u

/* ------------------------------------------------------------------------
 * Generator, trace and the event queue (world.c)
 * --------------------------------------------------------------------- */

struct vsr_sim_pcg {
    uint64_t state;
    uint64_t increment;
};

enum vsr_sim_event_kind {
    VSR_SIM_EVENT_OP,      /* node, index = op, generation = op's. */
    VSR_SIM_EVENT_SEGMENT, /* index = connection, peer = receiving end. */
    VSR_SIM_EVENT_STALL    /* index = connection: stalled, now reset. */
};

struct vsr_sim_event {
    uint64_t due_ns;
    uint64_t sequence;
    uint32_t kind; /* enum vsr_sim_event_kind */
    uint32_t node;
    uint32_t index;
    uint32_t peer;
    uint32_t generation;
    uint32_t incarnation;
};

/* ------------------------------------------------------------------------
 * Operations and the executor (exec.c)
 * --------------------------------------------------------------------- */

enum vsr_sim_op_state {
    VSR_SIM_OP_FREE,
    VSR_SIM_OP_PENDING, /* Submitted; not yet acted upon. */
    VSR_SIM_OP_WAITING, /* Parked on a socket or an event (timer, disk
                           latency, connect). */
    VSR_SIM_OP_LINKED,  /* Waits for the previous record of its chain. */
    VSR_SIM_OP_NOTIF    /* Zero-copy send: result posted, NOTIF due. */
};

/* What a due event means for a WAITING op. */
enum vsr_sim_action {
    VSR_SIM_ACTION_NONE,
    VSR_SIM_ACTION_SOCKET, /* Parked on op->object; no event. */
    VSR_SIM_ACTION_DISK,   /* Disk latency elapsed: apply and complete. */
    VSR_SIM_ACTION_TIMEOUT,
    VSR_SIM_ACTION_CONNECT, /* Connection attempt reaches the peer. */
    VSR_SIM_ACTION_UNREACHABLE,
    VSR_SIM_ACTION_NOTIF
};

/* Canonical socket name: AF_INET port, or AF_UNIX name bytes (a leading
 * NUL marks an abstract name). */
struct vsr_sim_key {
    uint32_t family;
    uint32_t port;
    uint32_t length;
    uint32_t reserved;
    unsigned char path[108];
};

/* A block copied at FSYNC submission; applied as durable at completion. */
struct vsr_sim_capture {
    uint64_t block;
    uint64_t version;
    unsigned char *bytes;
};

/* One submitted record and its state. Multishot records stay until they
 * terminate; zero-copy sends stay until their NOTIF is queued. Everything a
 * record points at other than data buffers (vectors, paths, addresses,
 * option values, the TIMEOUT_UPDATE target) is copied at submission, as the
 * kernel does at prep. */
struct vsr_sim_op {
    struct vsr_io_sqe sqe;
    uint64_t sequence; /* Submission order, for deterministic ties. */
    uint64_t due_ns;   /* Event time while an event is armed. */
    uint64_t started_ns;
    uint64_t requested; /* LINK success length; UINT64_MAX: result >= 0. */
    uint64_t checksum;  /* Of the data the record reads (ownership). */
    uint64_t target;    /* TIMEOUT_UPDATE target user_data. */
    uint32_t state;     /* enum vsr_sim_op_state */
    uint32_t action;    /* enum vsr_sim_action */
    uint32_t generation;
    uint32_t link_next; /* Chain successor or NONE. */
    uint32_t object;    /* Object the op waits on or holds, or NONE. */
    uint32_t holds;     /* The op holds a reference on object. */
    uint32_t region;    /* FIXED_BUFFER region in use, or NONE. */
    uint32_t multishot;
    uint32_t zero_copy;
    uint32_t checked; /* checksum is meaningful. */
    int32_t error;    /* Submission-time failure (bad pointer, length). */
    uint32_t peer;    /* CONNECT: target node. */
    struct vsr_sim_key key;
    struct vsr_io_vec *vecs;
    unsigned char *raw; /* CONNECT, BIND: the socket address. */
    uint32_t raw_length;
    uint32_t vec_count;
    uint32_t option_length;
    uint32_t reserved;
    unsigned char option[16];
    char *path;
    char *path2;
    struct vsr_sim_capture *captures;
    uint64_t capture_count;
};

struct vsr_sim_provided {
    struct vsr_io_buffer buffer;
    uint32_t consumed; /* Bytes already delivered (INCREMENTAL). */
    uint32_t reserved;
    uint64_t checksum; /* Of the bytes the kernel still owns. */
};

struct vsr_sim_ring {
    uint32_t used;
    uint32_t group;
    uint32_t entries;
    uint32_t flags;
    uint32_t head; /* Slot of the next buffer taken. */
    uint32_t count;
    struct vsr_sim_provided *slots; /* [entries], circular. */
};

/* An open file description: what descriptors and registered slots name. */
enum vsr_sim_object_kind {
    VSR_SIM_OBJECT_FREE,
    VSR_SIM_OBJECT_FILE,
    VSR_SIM_OBJECT_DIR,
    VSR_SIM_OBJECT_SOCKET
};

enum vsr_sim_socket_state {
    VSR_SIM_SOCKET_NEW,
    VSR_SIM_SOCKET_BOUND,
    VSR_SIM_SOCKET_LISTENING,
    VSR_SIM_SOCKET_CONNECTING,
    VSR_SIM_SOCKET_CONNECTED
};

struct vsr_sim_object {
    uint32_t kind; /* enum vsr_sim_object_kind */
    uint32_t refs; /* Descriptors, slots and ops holding it. */
    uint32_t index;
    uint32_t flags; /* O_* of a file or directory. */
    uint32_t inode; /* FILE: inode; DIR: directory entry. */
    uint32_t domain;
    uint32_t state; /* enum vsr_sim_socket_state */
    uint32_t bound;
    uint32_t listener;
    uint32_t connection;
    uint32_t end;
    uint32_t nodelay;
    uint32_t keepalive;
    uint32_t shut_rd;
    uint32_t shut_wr;
    uint32_t reserved;
    struct vsr_sim_key key; /* Bound name. */
};

/* ------------------------------------------------------------------------
 * Network (net.c)
 * --------------------------------------------------------------------- */

/* Bytes in flight on one direction of a connection, in order. */
struct vsr_sim_segment {
    struct vsr_sim_segment *next;
    unsigned char *bytes;
    uint32_t length;
    uint32_t offset; /* Bytes already consumed by receives. */
    uint64_t due_ns; /* VSR_SIM_NEVER: dropped, blocks the direction. */
    uint32_t arrived;
    uint32_t fin;      /* End of stream marker (no bytes). */
    uint32_t boundary; /* A receive stops after this segment (split). */
    uint32_t corrupted;
};

struct vsr_sim_end {
    uint32_t node;
    uint32_t object; /* Socket object, or NONE when closed or pending. */
    uint32_t closed; /* The socket went away. */
    uint32_t reset;
    uint32_t fin_sent; /* This end sent its FIN. */
    uint32_t reserved;
    struct vsr_sim_segment *head; /* Segments arriving at this end. */
    struct vsr_sim_segment *tail;
    uint64_t queued; /* Bytes in the queue, arrived or not. */
    uint64_t last_due_ns;
};

struct vsr_sim_connection {
    uint32_t used;
    uint32_t generation;
    uint32_t stalled;
    uint32_t listener; /* Listener whose accept queue holds it, or NONE. */
    struct vsr_sim_end ends[2]; /* 0 dialed, 1 accepted. */
};

struct vsr_sim_listener {
    uint32_t used;
    uint32_t node;
    uint32_t object;
    uint32_t backlog;
    struct vsr_sim_key key;
    uint32_t *pending; /* Connections not yet accepted, FIFO. */
    uint32_t pending_count;
    uint32_t pending_capacity;
};

/* ------------------------------------------------------------------------
 * Disk (disk.c)
 * --------------------------------------------------------------------- */

struct vsr_sim_block {
    unsigned char *bytes;   /* NULL: never allocated (reads as zero). */
    unsigned char *durable; /* Content a crash keeps; NULL: zeros. */
    uint64_t written_at;    /* Version of the last write. */
    uint64_t synced_at;     /* Version the durable copy holds. */
    uint32_t dirty;         /* Written since the last covering sync. */
    uint32_t reserved;
};

struct vsr_sim_inode {
    uint32_t used;
    uint32_t links; /* Names. */
    uint32_t opens; /* Open descriptions. */
    uint32_t mode;
    uint64_t size;
    uint64_t truncated_at; /* Version of the last truncation. */
    struct vsr_sim_block *blocks;
    uint64_t block_count;
};

struct vsr_sim_entry {
    uint32_t used;
    uint32_t parent;
    uint32_t directory;
    uint32_t inode; /* Files. */
    uint32_t mode;  /* Directories. */
    uint32_t opens; /* Open directory descriptions. */
    uint32_t removed;
    uint32_t reserved;
    char *name;
};

struct vsr_sim_disk {
    struct vsr_sim_entry **entries; /* [0] is the root. */
    uint32_t entries_count;
    uint32_t inodes_count;
    struct vsr_sim_inode **inodes;
    uint64_t block_bytes;
    uint64_t used_bytes;
    uint64_t version; /* Write counter. */
};

/* ------------------------------------------------------------------------
 * Nodes and the world (world.c)
 * --------------------------------------------------------------------- */

/* One executor handle, the ctx of a vsr_io_executor. Handles live until
 * vsr_sim_destroy; one whose incarnation is not the node's aborts. */
struct vsr_sim_handle {
    struct vsr_sim_node *node;
    uint32_t incarnation;
    uint32_t reserved;
    struct vsr_sim_handle *next;
};

/* Per-node state; executor state is recreated on restart, the disk kept. */
struct vsr_sim_node {
    struct vsr_sim *world;
    uint32_t index;
    uint32_t alive;
    uint32_t incarnation; /* Bumped at crash; stale handles abort. */
    uint32_t next_port;
    uint64_t clock_offset_ns;
    struct vsr_sim_handle *handle;
    struct vsr_sim_handle *handles;
    int32_t *fds; /* Descriptor -> object, -1 free. */
    uint32_t fds_count;
    uint32_t objects_count;
    struct vsr_sim_object **objects;
    struct vsr_sim_op **ops;
    uint32_t ops_count;
    uint32_t inflight;
    struct vsr_io_cqe *cqes; /* Circular queue of completions. */
    uint32_t cqes_head;
    uint32_t cqes_count;
    uint32_t cqes_capacity;
    uint32_t slots_count;
    int32_t *slots; /* Registered file slot -> object, -1 empty. */
    struct vsr_io_region *regions;
    uint32_t regions_count;
    uint32_t rings_count;
    struct vsr_sim_ring *rings;
    /* Last recorded wait. */
    uint32_t waited;
    uint32_t want;
    uint32_t wake_pending;
    uint32_t reserved;
    uint64_t wait_started_ns; /* Node clock. */
    uint64_t min_wait_ns;
    uint64_t deadline_ns;
    struct vsr_sim_disk disk;
    struct vsr_io_address address; /* Canonical AF_INET address. */
};

struct vsr_sim {
    struct vsr_sim_options options;
    struct vsr_sim_faults faults;
    struct vsr_sim_trace trace;
    struct vsr_sim_pcg random;
    uint64_t now_ns;
    uint64_t sequence; /* Global counter for deterministic ties. */
    struct vsr_sim_node *nodes;
    uint32_t nodes_count;
    uint32_t connections_count;
    struct vsr_sim_connection **connections;
    struct vsr_sim_listener **listeners;
    uint32_t listeners_count;
    uint32_t events_count;
    struct vsr_sim_event *events; /* Binary heap on (due, sequence). */
    uint32_t events_capacity;
    uint32_t reserved;
    unsigned char *partitions; /* [nodes * nodes] cut flags. */
};

/* world.c: allocation that aborts on exhaustion (test infrastructure). */
void *vsr_sim_alloc(size_t size);
void *vsr_sim_grow(void *memory, size_t count, size_t size);
/* Grows a table of pointers (the element type is a pointer, whatever it
 * points at) to count entries. */
void *vsr_sim_grow_table(void *table, size_t count);
/* Both spellings: C11's for the compilers and the GNU attribute for
 * cppcheck, which ignores _Noreturn and evaluates #if against the compile
 * command alone, so a compiler guard around the attribute would hide it.
 * Every compiler this tree builds with accepts both. */
_Noreturn void vsr_sim_fatal(const char *message, uint32_t node,
                             uint64_t user_data) __attribute__((noreturn));

/* The record fields are const void * as the kernel's are, though a READ,
 * RECV, GETSOCKOPT or STATX writes through them: the caller made the
 * memory writable by naming it. Copying the pointer value casts nothing. */
static inline void *vsr_sim_mutable(const void *pointer)
{
    void *writable;

    memcpy(&writable, &pointer, sizeof(writable));
    return writable;
}

/* Generator (PCG-XSH-RR 64/32), the only source of randomness. */
uint32_t vsr_sim_pcg_next(struct vsr_sim_pcg *pcg);
uint64_t vsr_sim_pcg_below(struct vsr_sim_pcg *pcg, uint64_t bound);
/* Draws a probability-per-million event; 0 and 1000000 draw nothing. */
bool vsr_sim_chance(struct vsr_sim *sim, uint32_t ppm);
/* Uniform in [min, max]; draws nothing when min >= max. */
uint64_t vsr_sim_range(struct vsr_sim *sim, uint64_t min, uint64_t max);
void vsr_sim_emit(struct vsr_sim *sim, uint32_t kind, uint32_t node,
                  uint32_t peer, uint64_t bytes);
void vsr_sim_emit_event(struct vsr_sim *sim,
                        const struct vsr_sim_trace_event *event);
void vsr_sim_schedule(struct vsr_sim *sim, uint64_t due_ns, uint32_t kind,
                      uint32_t node, uint32_t index, uint32_t peer,
                      uint32_t generation);
uint64_t vsr_sim_node_now(const struct vsr_sim_node *node);
bool vsr_sim_partitioned(const struct vsr_sim *sim, uint32_t a, uint32_t b);
uint64_t vsr_sim_add(uint64_t a, uint64_t b); /* Saturating. */

/* exec.c: the ops table and the per-record semantics. */
extern const struct vsr_io_executor_ops vsr_sim_executor_ops;
struct vsr_sim_op *vsr_sim_op(const struct vsr_sim_node *node, uint32_t op);
/* The op's terminal completion: verifies ownership, posts the CQE unless
 * SKIP_SUCCESS hides it, continues or cancels its chain, and frees the op
 * (a zero-copy send instead waits for its NOTIF at op->due_ns). */
void vsr_sim_exec_complete(struct vsr_sim_node *node, uint32_t op,
                           int32_t result, uint16_t flags, uint16_t buffer_id);
/* A multishot completion with MORE; the op stays. */
void vsr_sim_exec_post(struct vsr_sim_node *node, uint32_t op, int32_t result,
                       uint16_t flags, uint16_t buffer_id);
/* Parks the op until due_ns, when the action runs. */
void vsr_sim_exec_arm(struct vsr_sim_node *node, uint32_t op, uint64_t due_ns,
                      uint32_t action);
/* A due event of a WAITING or NOTIF op (world.c checked its generation). */
void vsr_sim_exec_event(struct vsr_sim_node *node, uint32_t op);
/* The op takes a reference on the object it is parked on or works on; the
 * reference drops at the op's terminal completion, so a CLOSE of the
 * descriptor never ends the op and the object outlives the descriptor
 * (decision 58), as a pending io_uring request holds its file. */
void vsr_sim_exec_hold(struct vsr_sim_node *node, uint32_t op, uint32_t object);
/* The object a record's fd names, honouring FIXED_FILE; -EBADF. */
int vsr_sim_exec_resolve(struct vsr_sim_node *node, int32_t fd, bool fixed,
                         uint32_t *object);
/* Installs a new object as a descriptor, or a slot with DIRECT (-ENFILE
 * when no slot is free or no table exists, -EINVAL for a slot past the
 * table); the result is the descriptor or slot. check only reports whether
 * install would succeed. */
int vsr_sim_exec_install(struct vsr_sim_node *node,
                         const struct vsr_io_sqe *sqe, uint32_t object,
                         bool check);
/* FIXED_BUFFER: the memory lies inside region sqe->buffer_index. */
int vsr_sim_exec_region(const struct vsr_sim_node *node,
                        const struct vsr_io_sqe *sqe, const void *base,
                        size_t length);
uint32_t vsr_sim_object_new(struct vsr_sim_node *node, uint32_t kind);
struct vsr_sim_object *vsr_sim_object(const struct vsr_sim_node *node,
                                      uint32_t object);
void vsr_sim_object_release(struct vsr_sim_node *node, uint32_t object);
/* First WAITING op parked on object with opcode, in submission order. */
uint32_t vsr_sim_exec_waiter(const struct vsr_sim_node *node, uint32_t object,
                             uint8_t opcode);
struct vsr_sim_ring *vsr_sim_exec_ring(struct vsr_sim_node *node,
                                       uint32_t group);
/* Aborts when the head buffer's unconsumed bytes changed since the kernel
 * took ownership of them. */
void vsr_sim_ring_verify(const struct vsr_sim_node *node,
                         const struct vsr_sim_ring *ring);
/* Hands the head buffer's unconsumed space to a receive and takes it back
 * after `used` bytes were written; returns BUFFER_MORE when it stays. */
uint16_t vsr_sim_ring_consume(struct vsr_sim_node *node,
                              struct vsr_sim_ring *ring, uint32_t used);
uint64_t vsr_sim_checksum(const void *bytes, size_t size, uint64_t seed);
/* Verifies every buffer the executor still references (in-flight sources
 * and provided buffers), then drops every op, completion, object, table
 * and ring of the node. */
void vsr_sim_exec_crash(struct vsr_sim_node *node);
void vsr_sim_exec_free(struct vsr_sim_node *node);
/* Whether the node's recorded wait is satisfied now. */
bool vsr_sim_exec_ready(const struct vsr_sim_node *node);
/* Earliest world time at which the node's recorded wait is satisfied
 * without further events, or NEVER. */
uint64_t vsr_sim_exec_wake_at(const struct vsr_sim_node *node);

/* net.c */
void vsr_sim_net_start(struct vsr_sim_node *node, uint32_t op);
void vsr_sim_net_action(struct vsr_sim_node *node, uint32_t op);
void vsr_sim_net_cancel(struct vsr_sim_node *node, uint32_t op);
/* Completes whatever the socket's state lets its parked ops do. */
void vsr_sim_net_serve(struct vsr_sim_node *node, uint32_t object);
/* The last reference to a socket object went away. */
void vsr_sim_net_close(struct vsr_sim_node *node, uint32_t object);
bool vsr_sim_net_segment(struct vsr_sim *sim, uint32_t connection, uint32_t end,
                         uint32_t generation);
bool vsr_sim_net_stall(struct vsr_sim *sim, uint32_t connection,
                       uint32_t generation);
/* Whether a queued SEGMENT or STALL event still has something to do, so
 * that advance can tell a pending event from a superseded one. */
bool vsr_sim_net_segment_live(const struct vsr_sim *sim, uint32_t connection,
                              uint32_t end, uint32_t generation,
                              uint64_t due_ns);
bool vsr_sim_net_stall_live(const struct vsr_sim *sim, uint32_t connection,
                            uint32_t generation);
void vsr_sim_net_partition(struct vsr_sim *sim, uint32_t a, uint32_t b);
void vsr_sim_net_reset_between(struct vsr_sim *sim, uint32_t a, uint32_t b);
/* Resets every connection of a crashed node, removes its listeners. */
void vsr_sim_net_crash(struct vsr_sim_node *node);
void vsr_sim_net_free(struct vsr_sim *sim);

/* disk.c */
/* Creates the root directory; -ENOMEM leaves an empty disk that
 * vsr_sim_disk_free accepts. */
int vsr_sim_disk_init(struct vsr_sim_disk *disk, uint64_t block_bytes);
void vsr_sim_disk_free(struct vsr_sim_disk *disk);
void vsr_sim_disk_start(struct vsr_sim_node *node, uint32_t op);
void vsr_sim_disk_finish(struct vsr_sim_node *node, uint32_t op);
/* The last reference to a file or directory object went away. */
void vsr_sim_disk_close(struct vsr_sim_node *node, uint32_t object);
/* Applies the crash model: unsynced blocks survive with unsynced_keep_ppm;
 * names without open descriptions lose nothing. */
void vsr_sim_disk_crash(struct vsr_sim *sim, struct vsr_sim_node *node);

#endif /* VSR_SIM_INTERNAL_H */
