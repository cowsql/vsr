#ifndef VSR_SIM_INTERNAL_H
#define VSR_SIM_INTERNAL_H

#include "vsr-sim.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/*
 * Simulated world internals (docs/io-implementation.md, "Simulation").
 * Four source files share this header: world.c (creation, clock, generator,
 * scheduling, faults, trace), net.c (listeners, connections, segments,
 * partitions), disk.c (virtual files and directories with the block
 * persistence model, inspection) and exec.c (the per-node executor: record
 * semantics, completions, registered files, buffer regions and rings).
 * The world may malloc; it never calls back into engine code except the
 * trace hook.
 *
 * Determinism: every random draw comes from one PCG stream seeded from
 * options.seed; event delivery is ordered by (due time, sequence number);
 * containers are arrays indexed by node and by slot, never hash tables
 * keyed by addresses; a run replays exactly from the seed and the
 * harness's sequence of calls.
 */

#define VSR_SIM_MAX_DIRECT 65536u

enum vsr_sim_op_state {
    VSR_SIM_OP_PENDING,   /* Submitted; not yet acted upon. */
    VSR_SIM_OP_WAITING,   /* Blocked: recv without data, accept, timeout,
                             connect in flight, disk latency. */
    VSR_SIM_OP_LINKED,    /* Waits for the previous record of its chain. */
    VSR_SIM_OP_COMPLETED, /* CQE queued; slot freed after reap. */
    VSR_SIM_OP_FREE
};

/* One submitted record and its state. Multishot records stay until they
 * terminate; zero-copy sends stay until their NOTIF is queued. */
struct vsr_sim_op {
    struct vsr_io_sqe sqe;
    uint64_t sequence;  /* Submission order, for deterministic ties. */
    uint64_t due_ns;    /* Disk latency or timer expiry; 0 none. */
    uint32_t state;     /* enum vsr_sim_op_state */
    uint32_t link_next; /* Chain successor index or NONE. */
    uint32_t link_failed;
    uint32_t socket; /* Connection or listener index the op waits on. */
    uint32_t sent;   /* Bytes already transferred (partial sends). */
    uint32_t notif_pending;
};

struct vsr_sim_cqe_node {
    struct vsr_io_cqe cqe;
    uint64_t sequence;
};

/* Registered resources of one node executor. */
struct vsr_sim_files {
    int32_t *fds; /* [file_slots]; -1 empty. Slots map to descriptors of
                     the node's descriptor table. */
    uint32_t count;
};

struct vsr_sim_buffers {
    struct vsr_io_region *regions; /* [buffer_regions]; NULL base = empty */
    uint32_t count;
};

struct vsr_sim_buffer_ring {
    uint16_t group;
    uint16_t registered;
    uint32_t entries;
    uint32_t flags;
    uint32_t head;                 /* Next buffer the kernel side takes. */
    uint32_t tail;                 /* Next buffer the user side provides. */
    struct vsr_io_buffer *buffers; /* [entries] ring */
    uint32_t *consumed;            /* [entries] bytes used (INCREMENTAL). */
};

/* Descriptor table entry of a node: a socket, listener, file or eventfd. */
enum vsr_sim_fd_kind {
    VSR_SIM_FD_FREE,
    VSR_SIM_FD_SOCKET, /* Unconnected, connecting, connected or closed. */
    VSR_SIM_FD_LISTENER,
    VSR_SIM_FD_FILE,
    VSR_SIM_FD_DIR
};

struct vsr_sim_fd {
    uint32_t kind;   /* enum vsr_sim_fd_kind */
    uint32_t index;  /* Into the world's sockets, listeners or the node's
                       files. */
    uint64_t offset; /* File position (unused: every I/O is positional). */
    uint32_t flags;  /* O_* of an open file. */
    uint32_t refs;
};

/* A byte segment in flight on a connection, in order after its delay. */
struct vsr_sim_segment {
    unsigned char *bytes;
    uint32_t length;
    uint32_t delivered; /* Bytes already given to receives (split). */
    uint64_t due_ns;
    uint64_t sequence;
    uint32_t corrupted;
    uint32_t next; /* Queue link. */
};

struct vsr_sim_socket_end {
    uint32_t node;
    int32_t fd;          /* Node descriptor, or -1 once closed. */
    uint32_t queue_head; /* Segments arriving at this end. */
    uint32_t queue_count;
    uint32_t stalled; /* A drop happened; reset at stall_reset_ns. */
    uint64_t stall_until_ns;
    uint32_t eof;   /* Peer closed: recv returns 0 after the queue. */
    uint32_t reset; /* recv/send return -ECONNRESET. */
};

struct vsr_sim_connection {
    uint32_t state; /* 0 free, 1 connecting, 2 open, 3 half-closed... */
    uint32_t reserved;
    struct vsr_sim_socket_end ends[2];
    uint64_t sequence;
};

struct vsr_sim_listener {
    uint32_t node;
    int32_t fd;
    struct vsr_io_address address; /* Canonical (family, port or path). */
    uint32_t backlog;
    uint32_t pending_head; /* Connections accepted by the stack, waiting
                              for an ACCEPT record. */
    uint32_t pending_count;
};

/* Virtual file: a sparse array of blocks with the persistence model. */
struct vsr_sim_block {
    unsigned char *bytes; /* NULL: never written (reads as zero). */
    uint64_t written_at;  /* Last write sequence. */
    uint64_t synced_at;   /* Last completed fsync covering it; 0 none. */
    uint32_t dirty;       /* Written since the last completed fsync. */
    uint32_t reserved;
};

struct vsr_sim_file {
    char *name; /* Relative to the node's root; NULL free. */
    struct vsr_sim_block *blocks;
    uint64_t block_count;
    uint64_t size;
    uint32_t refs;     /* Open descriptors. */
    uint32_t unlinked; /* Name removed; data lives until refs drop. */
    uint64_t syncs_pending;
};

struct vsr_sim_disk {
    struct vsr_sim_file *files;
    uint32_t count;
    uint32_t capacity;
    uint64_t used_bytes;
    uint64_t block_bytes;
    uint32_t dir_synced; /* Directory entries durable (renames, unlinks). */
    uint32_t reserved;
};

/* Per-node executor state; recreated on restart with the disk retained. */
struct vsr_sim_node {
    struct vsr_sim *world;
    uint32_t index;
    uint32_t alive;
    uint32_t incarnation; /* Restarts; embedded in the handle to abort on a
                             stale one. */
    uint32_t reserved;
    uint64_t clock_offset_ns;
    struct vsr_sim_fd *fds;
    uint32_t fds_count;
    struct vsr_sim_op *ops;
    uint32_t ops_count;
    uint32_t ops_capacity;
    struct vsr_sim_cqe_node *cqes; /* Completed, in queue order. */
    uint32_t cqes_head;
    uint32_t cqes_count;
    uint32_t cqes_capacity;
    struct vsr_sim_files files;
    struct vsr_sim_buffers buffers;
    struct vsr_sim_buffer_ring *rings;
    uint32_t rings_count;
    /* Last recorded wait. */
    uint32_t want;
    uint32_t woken;
    uint64_t wait_started_ns;
    uint64_t min_wait_ns;
    uint64_t deadline_ns;
    uint32_t waited; /* 1 after the first submit_and_wait. */
    uint32_t reserved2;
    struct vsr_sim_disk disk;
    struct vsr_io_address address; /* Canonical AF_INET address. */
};

struct vsr_sim_pcg {
    uint64_t state;
    uint64_t increment;
};

struct vsr_sim {
    struct vsr_sim_options options;
    struct vsr_sim_faults faults;
    struct vsr_sim_trace trace;
    struct vsr_sim_pcg random;
    uint64_t now_ns;
    uint64_t sequence; /* Global event counter for deterministic ties. */
    struct vsr_sim_node *nodes;
    uint32_t nodes_count;
    uint32_t reserved;
    struct vsr_sim_connection *connections;
    uint32_t connections_count;
    uint32_t connections_capacity;
    struct vsr_sim_listener *listeners;
    uint32_t listeners_count;
    uint32_t listeners_capacity;
    struct vsr_sim_segment *segments;
    uint32_t segments_count;
    uint32_t segments_capacity;
    uint32_t segments_free;
    uint32_t reserved2;
    unsigned char *partitions; /* [nodes * nodes] cut flags. */
};

/* Generator (PCG-XSH-RR 64/32), the only source of randomness. */
uint32_t vsr_sim_pcg_next(struct vsr_sim_pcg *pcg);
uint64_t vsr_sim_pcg_below(struct vsr_sim_pcg *pcg, uint64_t bound);
/* Draws a probability-per-million event. */
bool vsr_sim_chance(struct vsr_sim *sim, uint32_t ppm);
uint64_t vsr_sim_range(struct vsr_sim *sim, uint64_t min, uint64_t max);
void vsr_sim_emit(struct vsr_sim *sim, const struct vsr_sim_trace_event *event);

/* exec.c: the ops table and the per-record semantics. The ctx of a handle
 * is the node; a stale incarnation aborts. */
extern const struct vsr_io_executor_ops vsr_sim_executor_ops;
void vsr_sim_exec_submit(struct vsr_sim_node *node,
                         const struct vsr_io_sqe *sqe);
/* Runs every op that can make progress now (no time passes). */
void vsr_sim_exec_run(struct vsr_sim_node *node);
void vsr_sim_exec_complete(struct vsr_sim_node *node, uint32_t op,
                           int32_t result, uint16_t flags, uint16_t buffer_id);
void vsr_sim_exec_crash(struct vsr_sim_node *node);
/* Earliest due time among the node's timers and latency-bound ops. */
uint64_t vsr_sim_exec_next_due(const struct vsr_sim_node *node);

/* net.c */
int vsr_sim_net_connect(struct vsr_sim *sim, struct vsr_sim_node *node,
                        int32_t fd, const struct vsr_io_address *address,
                        uint32_t op);
int vsr_sim_net_bind(struct vsr_sim *sim, struct vsr_sim_node *node, int32_t fd,
                     const struct vsr_io_address *address);
int vsr_sim_net_listen(struct vsr_sim *sim, struct vsr_sim_node *node,
                       int32_t fd, uint32_t backlog);
int vsr_sim_net_send(struct vsr_sim *sim, struct vsr_sim_node *node,
                     uint32_t connection, uint32_t end,
                     const struct vsr_io_vec *vecs, uint32_t count,
                     uint32_t *sent);
/* Delivers due segments into waiting receives; returns completions made. */
uint32_t vsr_sim_net_deliver(struct vsr_sim *sim);
void vsr_sim_net_close(struct vsr_sim *sim, uint32_t connection, uint32_t end,
                       bool reset);
uint64_t vsr_sim_net_next_due(const struct vsr_sim *sim);

/* disk.c */
struct vsr_sim_file *vsr_sim_disk_open(struct vsr_sim_disk *disk,
                                       const char *name, uint32_t flags,
                                       int *error);
int vsr_sim_disk_read(struct vsr_sim *sim, struct vsr_sim_disk *disk,
                      struct vsr_sim_file *file, uint64_t offset, void *bytes,
                      size_t size, size_t *read);
int vsr_sim_disk_write(struct vsr_sim *sim, struct vsr_sim_disk *disk,
                       struct vsr_sim_file *file, uint64_t offset,
                       const struct vsr_io_vec *vecs, uint32_t count,
                       size_t *written);
int vsr_sim_disk_fsync(struct vsr_sim_disk *disk, struct vsr_sim_file *file);
int vsr_sim_disk_fallocate(struct vsr_sim_disk *disk, struct vsr_sim_file *file,
                           uint64_t offset, uint64_t length);
int vsr_sim_disk_rename(struct vsr_sim_disk *disk, const char *from,
                        const char *to);
int vsr_sim_disk_unlink(struct vsr_sim_disk *disk, const char *name);
/* Applies the crash model: unsynced blocks survive with unsynced_keep_ppm. */
void vsr_sim_disk_crash(struct vsr_sim *sim, struct vsr_sim_disk *disk);

#endif /* VSR_SIM_INTERNAL_H */
