#ifndef VSR_SIM_H
#define VSR_SIM_H

#include "vsr-io.h"

#include <stddef.h>
#include <stdint.h>

/*
 * VSR-SIM: a deterministic simulated world for vsr-io executors.
 *
 * One world holds a virtual clock, a virtual network, one virtual disk per
 * node, a seeded generator, and a fault model. Every node obtains a
 * struct vsr_io_executor whose operations have exactly the semantics that
 * vsr-io.h documents for the production executor: multishot accept and recv,
 * provided-buffer rings with incremental consumption, peek receives,
 * zero-copy sends with a second NOTIF completion, linked records,
 * cancellation, absolute and relative timeouts, registered files and buffer
 * regions, and wake. Engine and application code therefore run
 * byte-identical over the simulation and over io_uring; the only difference
 * is who advances time.
 *
 * Nothing here blocks. submit_and_wait records the node's want, min_wait_ns
 * and deadline_ns and returns; the harness runs each node's loop iteration
 * in turn whenever vsr_sim_ready says its wait is satisfied, and calls
 * vsr_sim_advance in between. The whole world is single-threaded. Given the
 * same seed, the same options, and the same sequence of harness calls, every
 * completion, delivery, fault, and clock value repeats exactly, so a failure
 * is replayable from the seed plus the harness's own action trace.
 *
 * This header is test infrastructure. Unlike the core and the engine, the
 * world allocates internally with malloc and may invoke a trace callback;
 * the application core and the engine under test still allocate nothing and
 * receive no callbacks. The world composes no cluster: which nodes exist,
 * what runs on them, and which invariants hold are the harness's, because
 * the application core is application-specific.
 */

#define VSR_SIM_API_VERSION 1u
#define VSR_SIM_NO_NODE UINT32_MAX
/* Directory descriptor meaning "the node's virtual root", the same value as
 * Linux AT_FDCWD, so store and application paths work unchanged. */
#define VSR_SIM_ROOT (-100)

struct vsr_sim;

/*
 * Network faults. Sockets are ordered reliable byte streams, as TCP is, so a
 * fault is visible only as delay, a stall, a reset, or corrupted bytes.
 * Probabilities are per segment, where a segment is one SEND record's bytes.
 * Segments of one connection are delivered in order after their delay;
 * delays of different connections are independent, which reorders traffic
 * between peers freely. drop loses a segment silently: the connection stalls
 * and is reset once stall_reset_ns pass, as a real stack gives up. corrupt
 * flips one or more bytes in flight, which the engine's CRCs must catch.
 * A partition (vsr_sim_partition) drops every segment and connection attempt
 * between two nodes, and connections already open across it stall then
 * reset. A node with no route to a peer sees CONNECT complete with
 * -EHOSTUNREACH after connect_timeout_ns, or -ECONNREFUSED at once when the
 * peer is up but nothing listens. split delivers a segment across two or
 * more receive completions at boundaries drawn at random, as a real stack
 * may, so that frame reassembly across completions and provided buffers is
 * exercised; it is not a fault, since the bytes and their order are intact.
 */
struct vsr_sim_network_faults {
    uint32_t drop_ppm;
    uint32_t corrupt_ppm;
    uint32_t reset_ppm;
    uint32_t split_ppm;
    uint64_t delay_min_ns;
    uint64_t delay_max_ns;
    uint64_t stall_reset_ns;
    uint64_t connect_timeout_ns;
};

/*
 * Disk faults. Reads and writes complete after a latency drawn from the
 * range; fsync after its own range. bitrot_ppm is per block read: the block
 * is returned with flipped bytes and no error, so a checksum must catch it.
 * error_ppm is per operation: a transient -EIO with no effect on the file.
 * capacity_bytes bounds the node's disk (0: unbounded); a write or
 * fallocate past it fails with -ENOSPC, and enospc_ppm injects the same
 * failure at random. On crash, every written block that no completed fsync
 * covers persists independently with probability unsynced_keep_ppm
 * (1000000 keeps all, 0 keeps none); blocks that fsync covered always
 * persist; nothing else changes. Block granularity is options.block_bytes.
 */
struct vsr_sim_disk_faults {
    uint64_t latency_min_ns;
    uint64_t latency_max_ns;
    uint64_t fsync_min_ns;
    uint64_t fsync_max_ns;
    uint64_t capacity_bytes;
    uint32_t bitrot_ppm;
    uint32_t error_ppm;
    uint32_t enospc_ppm;
    uint32_t unsynced_keep_ppm;
};

/*
 * Clock faults. Each node's executor clock is the world clock plus a fixed
 * per-node offset drawn from [0, offset_max_ns] at creation and at every
 * restart; nodes share no clock assumptions, so any offset is legal. Timers
 * fire late by a jitter drawn from [0, jitter_max_ns], never early.
 */
struct vsr_sim_clock_faults {
    uint64_t offset_max_ns;
    uint64_t jitter_max_ns;
};

struct vsr_sim_faults {
    struct vsr_sim_network_faults network;
    struct vsr_sim_disk_faults disk;
    struct vsr_sim_clock_faults clock;
};

struct vsr_sim_options {
    uint64_t seed;
    uint32_t nodes;          /* Node indices are 0..nodes-1. */
    uint32_t block_bytes;    /* Torn-write and bit-rot granularity; 0 = 4096. */
    uint32_t file_slots;     /* Registered file table size per node executor. */
    uint32_t buffer_regions; /* Registered buffer table size per node. */
    struct vsr_sim_faults faults;
};

/*
 * Trace hook. Called synchronously from inside world calls with a borrowed
 * event; the callback must not call back into the world. Test-only.
 */
enum vsr_sim_trace_kind {
    VSR_SIM_TRACE_SUBMIT,   /* node, user_data, opcode */
    VSR_SIM_TRACE_COMPLETE, /* node, user_data, result */
    VSR_SIM_TRACE_DELIVER,  /* node (receiver), peer, bytes */
    VSR_SIM_TRACE_DROP,     /* node (sender), peer, bytes */
    VSR_SIM_TRACE_CORRUPT,  /* node (receiver), peer, bytes */
    VSR_SIM_TRACE_RESET,    /* node, peer */
    VSR_SIM_TRACE_BITROT,   /* node, offset in bytes */
    VSR_SIM_TRACE_TORN,     /* node, offset: a block discarded at crash */
    VSR_SIM_TRACE_CRASH,    /* node */
    VSR_SIM_TRACE_RESTART,  /* node */
    VSR_SIM_TRACE_CLOCK     /* now advanced; bytes = new world clock */
};

struct vsr_sim_trace_event {
    uint32_t kind; /* enum vsr_sim_trace_kind */
    uint32_t node;
    uint32_t peer;
    uint32_t opcode;
    uint64_t user_data;
    int64_t result;
    uint64_t bytes;
    uint64_t now_ns;
};

struct vsr_sim_trace {
    void *ctx;
    void (*event)(void *ctx, const struct vsr_sim_trace_event *event);
};

/* Allocates the world. Returns OK, EINVAL, or a negative errno. */
int vsr_sim_create(const struct vsr_sim_options *options, struct vsr_sim **out);
/* Frees everything, including every executor handle ever returned. */
void vsr_sim_destroy(struct vsr_sim *sim);
/* NULL disables tracing. */
void vsr_sim_set_trace(struct vsr_sim *sim, const struct vsr_sim_trace *trace);
/* Replaces the fault model from the next event on; not retroactive. */
void vsr_sim_set_faults(struct vsr_sim *sim,
                        const struct vsr_sim_faults *faults);

/*
 * The node's current executor handle. Its now is the node's skewed clock;
 * its random draws from the world generator. Valid until the node crashes.
 * Calling any operation through a handle invalidated by a crash aborts the
 * process: that is a harness bug, never a scenario.
 */
struct vsr_io_executor vsr_sim_executor(struct vsr_sim *sim, uint32_t node);

/*
 * Scheduling. vsr_sim_ready reports whether the node's last recorded wait is
 * satisfied: at least `want` completions are reapable, or min_wait_ns has
 * elapsed with one or more, or its deadline passed, or it was woken, or it
 * never waited. vsr_sim_advance delivers every completion, segment, and
 * timer due at the current world clock and returns their count; when nothing
 * is due anywhere it moves the clock to the earliest pending event or node
 * deadline and returns 0; when there is no pending event and no deadline it
 * returns -1, meaning only a harness action or a wake can make progress.
 * The harness pattern is: run every ready node's iteration, then advance.
 * A node that is ready is run before the clock moves, so a node never
 * misses its own deadline by more than the clock jitter it configured.
 */
int vsr_sim_ready(const struct vsr_sim *sim, uint32_t node);
int vsr_sim_advance(struct vsr_sim *sim);
uint64_t vsr_sim_now(const struct vsr_sim *sim); /* World clock, ns. */
/* Number of operations submitted by node and not yet reapable. */
uint32_t vsr_sim_inflight(const struct vsr_sim *sim, uint32_t node);

/*
 * Addresses are raw socket addresses as in vsr-io.h. Every node owns one
 * canonical AF_INET address; binding to it, or to the unspecified address,
 * on a port registers the node as the listener for (address, port), and
 * vsr_sim_address is what a peer dials. AF_UNIX names, pathname or abstract,
 * live in a namespace private to each node, as on a real host: a connection
 * to one reaches only a listener on the same node. Other families complete
 * with -EAFNOSUPPORT.
 */
struct vsr_io_address vsr_sim_address(const struct vsr_sim *sim, uint32_t node,
                                      uint16_t port);

/* Symmetric partition control; cut != 0 severs, 0 heals. vsr_sim_isolate is
 * a partition between node and every other node. vsr_sim_reset resets every
 * open connection between the two nodes once, at the current clock. */
void vsr_sim_partition(struct vsr_sim *sim, uint32_t a, uint32_t b, int cut);
void vsr_sim_isolate(struct vsr_sim *sim, uint32_t node, int cut);
void vsr_sim_reset(struct vsr_sim *sim, uint32_t a, uint32_t b);

/*
 * Crash and restart. Crash ends every in-flight operation of the node as if
 * cancelled, delivers nothing more to it, closes its sockets so peers see
 * resets, drops its registered files, buffers, and rings, applies the
 * unsynced-write model to its disk, and invalidates its executor handle.
 * The disk contents that survive are the node's until vsr_sim_destroy.
 * Restart draws a fresh clock offset and returns a fresh handle over that
 * disk. Restarting a node that has not crashed is EINVAL; crashing a crashed
 * node is a no-op.
 */
void vsr_sim_crash(struct vsr_sim *sim, uint32_t node);
struct vsr_io_executor vsr_sim_restart(struct vsr_sim *sim, uint32_t node);
int vsr_sim_alive(const struct vsr_sim *sim, uint32_t node);

/*
 * Disk inspection for oracles. Paths are the node's, relative to its root,
 * as the engine and application name them. Reads return the bytes that a
 * read through the executor would return now, including unsynced writes,
 * without faults. corrupt flips bytes in place and does not wait for a
 * crash. Directory listing is by index; a name buffer of at least
 * VSR_SIM_NAME_BYTES receives the entry name, NUL-terminated.
 */
#define VSR_SIM_NAME_BYTES 256u

int vsr_sim_file_size(const struct vsr_sim *sim, uint32_t node,
                      const char *path, uint64_t *size);
int vsr_sim_file_read(const struct vsr_sim *sim, uint32_t node,
                      const char *path, uint64_t offset, void *bytes,
                      size_t size, size_t *read);
int vsr_sim_file_corrupt(struct vsr_sim *sim, uint32_t node, const char *path,
                         uint64_t offset, size_t size);
/* The synced prefix as the crash model would keep it: bytes covered by a
 * completed fsync; unsynced regions read back as they would after a crash
 * that keeps nothing. */
int vsr_sim_file_read_durable(const struct vsr_sim *sim, uint32_t node,
                              const char *path, uint64_t offset, void *bytes,
                              size_t size, size_t *read);
int vsr_sim_dir_count(const struct vsr_sim *sim, uint32_t node,
                      const char *path, uint32_t *count);
int vsr_sim_dir_entry(const struct vsr_sim *sim, uint32_t node,
                      const char *path, uint32_t index, char *name,
                      size_t name_size, uint64_t *size);

/*
 * Deterministic choices for the harness, drawn from the same generator as
 * the faults, so a scenario replays from its seed alone as long as the
 * harness consumes choices in a fixed order. random_below(0) returns 0.
 */
uint64_t vsr_sim_random(struct vsr_sim *sim);
uint64_t vsr_sim_random_below(struct vsr_sim *sim, uint64_t bound);

#endif /* VSR_SIM_H */
