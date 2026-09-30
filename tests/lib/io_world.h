#ifndef VSR_TEST_IO_WORLD_H
#define VSR_TEST_IO_WORLD_H

#include "lib/faulty_executor.h"
#include "lib/pure_executor.h"
#include "vsr-io.h"
#include "vsr-sim.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/*
 * A world of engines for the integration tests (tests/integration/engine,
 * streams, snapshots, uring_faults) and, later, the iocluster fuzzer: up to
 * IOW_NODES nodes, each one process-like engine (struct vsr_io) over its own
 * executor, running real cores (vsr_init) with real stores, driven through
 * vsr-io.h's public loop exactly as a caller would.
 *
 * Backends. IOW_SIM: one simulated world (vsr-sim.h); every node's executor
 * is its handle, time is virtual, runs replay from the seed. IOW_URING: one
 * io_uring per node in this thread, abstract AF_UNIX addresses, a temporary
 * directory for the stores, real time; each iteration submits without
 * blocking and an idle round sleeps at most a millisecond.
 *
 * Executor stack of a node, from the engine down:
 *   pure_executor (armed around vsr_io_complete, poll, submit, prepare and
 *                  the node calls: any executor call there aborts, 133)
 *   hook          (the harness's targeted faults: fail or hold the records
 *                  a rule matches, fail a submission, count provisions)
 *   faulty_executor (optional, seeded rates, tests/lib/faulty_executor.h)
 *   base          (the sim handle or the ring)
 *
 * The caller of each replica (struct iow_app) is an application with a
 * deterministic state: every COMMAND folds into a 64-bit digest whose value
 * after the command is its result, so replies, applies and installs can be
 * checked against one history per cluster (the digest at every op, the
 * first time any replica reaches it). It answers every forwarded op: APPLY,
 * READ_READY, REPLY, the snapshot ops (CAPTURE puts the digest in the
 * manifest; INSTALL takes it back; FETCH completes with the task's
 * checkpoint, at once or after pulling the source application's image over
 * a caller stream), and keeps its leases until their RELEASE op.
 *
 * Crashes: iow_crash abandons the node's engine memory after vsr_sim_crash
 * (or the ring's deinit), never calling the engine again, as a process
 * crash would; iow_restart gives the node a new executor and engine (on the
 * other memory bank, so a ring still tearing down never shares pages with
 * the new one) with the world's node table and authorizations, and the
 * test re-attaches its replicas with RECOVER.
 *
 * Streams: a request that starts with IOW_STREAM_MAGIC names a pattern the
 * source writes (seeded bytes, BUFFERS or FILE writes of `piece` bytes,
 * an early CLOSE status, a refused SERVE); the requester verifies every
 * byte against the pattern. The application's snapshot fetch is one such
 * stream (kind APPSNAP).
 *
 * Every function CHECKs its preconditions and aborts on a harness misuse.
 * IOW_TRACE=1 in the environment prints the simulation's trace and every
 * iteration.
 */

#define IOW_NODES 5u
#define IOW_REPLICAS 2u  /* Per node. */
#define IOW_MEMBERS 5u   /* Per membership. */
#define IOW_LEASES 192u  /* Caller leases per replica. */
#define IOW_PENDING 256u /* Events waiting for a submit, per node. */
#define IOW_SNAPSHOTS 16u
#define IOW_STREAMS 8u /* Harness streams per node, each role. */
#define IOW_STREAM_BYTES (256u * 1024u)
#define IOW_STREAM_WRITES 64u
#define IOW_FILE_BYTES (64u * 1024u)
#define IOW_HISTORY 8192u /* Ops tracked per cluster. */
#define IOW_CLUSTERS 8u
#define IOW_RULES 8u
#define IOW_MARKS 64u
#define IOW_HELD 256u
#define IOW_OWNER 0x5Au
#define IOW_APP_OWNER 0x33u
#define IOW_FILE_SLOT_BASE 8u
#define IOW_REGION_BASE 2u
#define IOW_GROUP 1u
#define IOW_CALLER_SLOT 4u /* The caller's own file, below the engine's. */
#define IOW_MS UINT64_C(1000000)
#define IOW_STREAM_MAGIC UINT32_C(0x53574F49) /* "IOWS" */
#define IOW_FILE_SEED UINT64_C(0xF11E)
#define IOW_APPSNAP_BYTES 20000u

enum iow_backend { IOW_SIM, IOW_URING };

/* A caller lease and the graph it covers, kept until the RELEASE op. */
struct iow_lease {
    uint64_t id; /* 0: free. */
    struct vsr_request request;
    struct vsr_blob blob;
    struct vsr_span span;
    unsigned char bytes[64];
    struct vsr_applied applied;
    struct vsr_value values[16];
    struct vsr_span result_spans[16];
    unsigned char results[16][8];
    struct vsr_checkpoint checkpoint;
    struct vsr_membership membership;
    struct vsr_member members[IOW_MEMBERS];
};

struct iow_snapshot {
    bool used;
    struct vsr_id id;
    uint64_t op;
    uint64_t digest;
};

/* The application's fetch of a snapshot image, over a caller stream. */
struct iow_fetch {
    bool active;
    uint64_t op;                             /* The core's FETCH op. */
    const struct vsr_checkpoint *checkpoint; /* The task's, pinned. */
    uint64_t cookie;
    uint64_t digest; /* From the manifest: the image's seed. */
};

struct iow_node;

struct iow_app {
    struct iow_node *node;
    struct vsr_io_replica *replica;
    uint32_t index;
    bool attached;
    struct vsr_membership seed;
    struct vsr_member members[IOW_MEMBERS];
    struct vsr_io_replica_options options;
    char path[256];
    struct iow_lease leases[IOW_LEASES];
    uint64_t next_route;
    /* Observations. */
    uint32_t statuses;
    struct vsr_status status;
    uint32_t stopped_statuses; /* STATUS ops reporting STOPPED. */
    uint64_t replies;
    uint64_t replies_ok;
    uint64_t replies_executed;
    uint32_t last_reply_status;
    uint32_t reply_statuses[16]; /* By enum vsr_reply_status. */
    uint64_t applies;
    uint64_t applied_entries;
    uint64_t releases;
    uint64_t captures;
    uint64_t fetches;
    uint64_t fetches_ok;
    uint64_t installs;
    uint64_t syncs;
    uint64_t drops;
    uint64_t read_ready;
    /* The state machine. */
    uint64_t digest;
    uint64_t applied_op;
    uint64_t commands; /* Executed by this incarnation. */
    struct iow_snapshot snapshots[IOW_SNAPSHOTS];
    struct iow_fetch fetch;
    /* Behaviour. */
    bool hold_apply;     /* Keep APPLY completions back. */
    uint64_t held_apply; /* Its op id, 0 none. */
    const struct vsr_apply *held_data;
    bool fetch_by_stream;   /* Pull the image before completing FETCH. */
    int32_t capture_status; /* Answer CAPTURE with this (OK: normal). */
    int32_t fetch_status;   /* Answer FETCH with this (OK: normal). */
    bool check_history;     /* Compare digests with the cluster's. */
};

/* A requester's stream, by cookie. */
struct iow_rstream {
    bool used;
    uint64_t cookie;
    unsigned char request[64]; /* Pinned until END (the lease covers it). */
    uint32_t request_bytes;
    struct vsr_io_stream_open open;
    uint64_t seed;
    uint64_t received;
    uint32_t data_ops;
    bool mismatch; /* A byte differed from the pattern, or an offset. */
    bool ended;
    int32_t status;
    uint64_t end_bytes;
    struct iow_app *app; /* An application fetch, else NULL. */
};

/* A source's stream, by handle. */
struct iow_sstream {
    bool used;
    uint64_t handle;
    uint32_t kind;
    uint64_t seed;
    uint64_t length;
    uint64_t stop; /* Bytes written before the CLOSE. */
    uint32_t piece;
    uint32_t write_kind;
    int32_t close_status;
    bool hold; /* Never CLOSE. */
    bool accepted;
    bool closed; /* CLOSE submitted. */
    uint64_t queued;
    uint32_t writes;  /* WRITE events accepted. */
    uint32_t written; /* WRITTEN ops seen. */
    bool ended;
    int32_t status;
    uint64_t end_bytes;
    uint64_t serve_op;     /* A SERVE held back, 0 none. */
    unsigned char *buffer; /* [IOW_STREAM_BYTES] the pattern bytes. */
    struct vsr_io_stream_write descriptors[IOW_STREAM_WRITES];
    struct vsr_span spans[IOW_STREAM_WRITES];
};

enum iow_stream_kind { IOW_STREAM_PATTERN, IOW_STREAM_APPSNAP };

/* A harness stream request, as its bytes. */
struct iow_stream_request {
    uint32_t magic;
    uint32_t kind;       /* enum iow_stream_kind */
    uint64_t seed;       /* PATTERN: the bytes' seed. */
    uint64_t length;     /* PATTERN: bytes to send. */
    uint64_t stop;       /* PATTERN: CLOSE after this many (<= length). */
    uint32_t piece;      /* PATTERN: bytes per WRITE. */
    uint32_t write_kind; /* enum vsr_io_write_kind */
    int32_t close_status;
    int32_t refuse; /* Nonzero: complete the SERVE with it. */
    uint32_t hold;  /* Nonzero: never CLOSE (the stream stays open). */
    uint32_t reserved;
    struct vsr_id snapshot; /* APPSNAP. */
    struct vsr_id cluster;  /* APPSNAP. */
    uint64_t replica;       /* APPSNAP. */
};

/* Targeted faults (the hook): a rule matches submitted records; its action
 * applies to the completions of the records it marked. */
enum iow_action {
    IOW_FAIL,         /* Every completion of the record carries `result`. */
    IOW_FAIL_CHAINED, /* The record is made to fail in the kernel (a
                         FILES_UPDATE beyond the table), so its LINK chain
                         is cancelled as a real failure's is; its own
                         completion then carries `result`. */
    IOW_HOLD          /* Completions kept until iow_release_held. */
};

typedef bool (*iow_match)(void *ctx, const struct vsr_io_sqe *sqe);

struct iow_rule {
    iow_match match;
    void *ctx;
    uint32_t action;
    int32_t result;
    uint32_t count; /* Records left to mark; 0: the rule is spent. */
    uint32_t hits;
};

struct iow_mark {
    uint64_t user_data;
    uint32_t action;
    int32_t result;
};

struct iow_hook {
    struct vsr_io_executor inner;
    struct iow_rule rules[IOW_RULES];
    struct iow_mark marks[IOW_MARKS];
    uint32_t marks_count;
    struct vsr_io_cqe held[IOW_HELD];
    uint32_t held_count;
    bool holding; /* Keep matching completions back. */
    int fail_submit;
    uint32_t submits;
    uint32_t provides; /* PROVIDE records submitted. */
    struct vsr_io_sqe batch[256];
};

struct iow_node {
    uint32_t index; /* Node id index + 1. */
    bool open;      /* The loop runs it. */
    bool crashed;
    uint32_t bank;        /* Memory bank of the current engine. */
    uint32_t incarnation; /* Engines this node has had. */
    struct vsr_io *io;
    struct vsr_io_executor base;
    struct faulty_executor *faulty; /* NULL: none. */
    struct faulty_executor_options faulty_options;
    bool use_faulty;
    struct iow_hook hook;
    struct pure_executor pure;
    struct vsr_io_executor ex; /* The engine's. */
    void *ring_memory;
    struct vsr_io_options options;
    struct vsr_io_address address;
    struct iow_app apps[IOW_REPLICAS];
    struct vsr_io_event pending[IOW_PENDING];
    uint32_t pending_count;
    uint32_t hold_ops; /* Poll with capacity 0 while set. */
    uint64_t deadline; /* The last prepare's, for the ring's idle wait. */
    bool busy;         /* The last iteration did something. */
    uint32_t spins;    /* Idle iterations in a row with a past deadline. */
    /* Rails. */
    struct iow_rstream rstreams[IOW_STREAMS];
    struct iow_sstream sstreams[IOW_STREAMS];
    uint64_t stream_serves;
    uint64_t stream_ends;
    uint64_t stream_data;
    uint64_t stream_written;
    uint64_t handshakes;
    uint64_t links_wanted;
    bool hold_serve; /* Leave STREAM_SERVE unanswered. */
    bool hold_data;  /* Keep STREAM_DATA completions back. */
    uint64_t held_data[IOW_PENDING];
    uint32_t held_data_count;
    uint64_t foreign; /* Completions of the caller's own records. */
    int32_t foreign_result;
    uint64_t foreign_user_data;
    bool foreign_seen;
    bool file_ready; /* The caller's pattern file exists at its slot. */
};

struct iow_authorization {
    struct vsr_id cluster;
    uint64_t replica;
    uint64_t node;
};

struct iow_history {
    bool used;
    struct vsr_id cluster;
    uint64_t digest[IOW_HISTORY];
    unsigned char known[IOW_HISTORY];
};

struct iow_reply_record {
    struct vsr_id cluster;
    struct vsr_id client;
    uint64_t number;
    uint64_t op;
    uint64_t result;
};

struct iow_world {
    uint32_t backend; /* enum iow_backend */
    uint64_t seed;
    uint32_t nodes;
    struct vsr_sim *sim;
    struct vsr_sim_options sim_options;
    char directory[256]; /* IOW_URING: the stores' root. */
    struct iow_node node[IOW_NODES];
    uint64_t incarnation;
    /* Defaults for the replicas attached next. */
    uint32_t durability;
    struct vsr_limits limits;
    struct vsr_io_store_options store;
    uint64_t heartbeat_ns;
    uint64_t view_timeout_ns;
    uint64_t retry_ns;
    uint64_t transfer_timeout_ns;
    uint64_t checkpoint_interval;
    /* Defaults for the engines opened next. */
    struct vsr_io_limits io_limits;
    uint32_t block_bytes;
    uint64_t handshake_timeout_ns;
    uint32_t stream_chunk_bytes;
    /* The mesh, re-applied at every restart. */
    struct iow_authorization authorizations[32];
    uint32_t authorizations_count;
    /* Oracles. */
    struct iow_history history[IOW_CLUSTERS];
    struct iow_reply_record replies[1024];
    uint32_t replies_count;
    uint64_t rounds;
    bool trace;
};

extern struct iow_world iow;

/* The world. iow_open_sim takes the network, disk and clock faults of the
 * simulation (NULL: small latencies, no faults); iow_open_uring returns
 * false when no ring can be made (the caller skips). */
void iow_open_sim(uint32_t nodes, uint64_t seed,
                  const struct vsr_sim_faults *faults);
bool iow_open_uring(uint32_t nodes, uint64_t seed);
void iow_close(void);

/* Engines. iow_engine_options fills the defaults (world fields above);
 * iow_node_open_with takes the caller's (the executor is set by it). */
struct vsr_io_options iow_engine_options(struct iow_node *n);
struct iow_node *iow_node_open(uint32_t index);
struct iow_node *iow_node_open_with(uint32_t index,
                                    const struct vsr_io_options *options);
/* The faulty executor under the hook, from the node's next executor on
 * (call before iow_node_open or iow_restart). */
void iow_node_faulty(uint32_t index, const struct faulty_executor_options *o);
/* Every node knows every other's address (nodes [0, count)) and authorizes
 * (cluster, replica r) as node r for r in [1, count]; kept in the world
 * and re-applied at restarts. */
void iow_mesh(uint32_t count, struct vsr_id cluster);
void iow_authorize(struct vsr_id cluster, uint64_t replica, uint64_t node);
/* STOP, detach and close are the test's; iow_close_node closes an engine
 * with no replica attached and deinits it. */
void iow_close_io(struct iow_node *n);
void iow_close_node(struct iow_node *n);
bool iow_node_closed(void *node);

/* Crash: the executor goes (vsr_sim_crash, or the ring's deinit) and the
 * engine's memory is abandoned; restart: a new executor and engine. */
void iow_crash(struct iow_node *n);
void iow_restart(struct iow_node *n);

/* Replicas. The directory is `c<cluster>-r<replica>` (plus `-<generation>`
 * for a generation above zero, a disk that lost its contents), made when
 * the mode is not RECOVER. */
void iow_replica_options(struct iow_app *app, struct vsr_id cluster,
                         uint64_t replica, uint32_t members, uint32_t mode,
                         uint32_t generation);
struct iow_app *iow_attach(struct iow_node *n, uint32_t r,
                           struct vsr_id cluster, uint64_t replica,
                           uint32_t members, uint32_t mode);
struct iow_app *iow_attach_with(struct iow_node *n, uint32_t r);
void iow_stop(struct iow_app *app);
/* Completes the APPLY the application held (hold_apply). */
void iow_complete_held_apply(struct iow_app *app);
void iow_detach(struct iow_app *app);
bool iow_app_normal(void *app);
bool iow_app_stopped(void *app);
struct vsr_id iow_cluster(uint64_t n);
struct vsr_id iow_client(uint64_t n);

/* Events. */
void iow_queue(struct iow_node *n, const struct vsr_io_event *event);
void iow_core_event(struct iow_app *app, uint32_t type, int32_t status,
                    uint64_t id, const void *data, uint64_t lease);
/* A COMMAND of client `client`, number `number`; returns its route. */
uint64_t iow_request(struct iow_app *app, uint64_t client, uint64_t number);
void iow_make_request(struct iow_app *app, uint64_t client, uint64_t number,
                      struct vsr_io_event *out);
/* A RECONFIGURE to members 1..count (all FULL, f = (count - 1) / 2) in
 * `epoch`. */
uint64_t iow_reconfigure(struct iow_app *app, uint64_t client, uint64_t number,
                         uint64_t epoch, uint32_t count);
uint32_t iow_leases_out(const struct iow_app *app);
/* The lease of an event the engine did not consume is the caller's again. */
void iow_lease_forget(struct iow_app *app, uint64_t id);
/* vsr_io_submit with the purity guard armed, for tests that look at the
 * status themselves. */
int iow_submit(struct iow_node *n, const struct vsr_io_event *events,
               uint32_t count, uint32_t *consumed);
/* A directory for a store, through the node's executor (or mkdir on a
 * ring). */
void iow_make_directory(struct iow_node *n, const char *path);

/* The loop. iow_iterate is one iteration of vsr-io.h's loop for a node;
 * iow_round runs every ready node once and advances the simulation (or
 * sleeps briefly on rings); iow_run_until returns true once `until` holds,
 * false after `ns` of world time. */
typedef bool (*iow_predicate)(void *ctx);
void iow_iterate(struct iow_node *n);
bool iow_round(void);
bool iow_run_until(iow_predicate until, void *ctx, uint64_t ns);
void iow_run_for(uint64_t ns);
uint64_t iow_now(void);

/* Groups. */
struct iow_group {
    uint32_t count;
    struct iow_app *apps[IOW_NODES];
};

/* Every attached member is NORMAL in one view and epoch (a crashed one,
 * not attached, is skipped). */
bool iow_group_normal(void *group);
struct iow_app *iow_group_primary(const struct iow_group *g);
/* Nodes [0, count) open, meshed, one replica each (NEW), NORMAL. */
struct iow_group iow_group_open(uint32_t count, struct vsr_id cluster);
/* STOP everywhere, detach, close every engine of the group. */
void iow_group_close(struct iow_group *g);
/* Submits `count` requests at the primary (clients 1..clients round
 * robin, numbers continuing each client's) and waits for their replies. */
void iow_group_commit(struct iow_group *g, uint32_t count);
/* The last request number iow_group_commit used for a client. */
uint64_t iow_last_number(uint64_t client);
/* Every attached replica of the group has applied at least `op`. */
bool iow_group_applied(const struct iow_group *g, uint64_t op);

/* Streams. iow_stream_open queues a STREAM_OPEN from n to node `peer` of
 * a PATTERN request (the returned requester record fills as it goes). */
struct iow_rstream *iow_stream_open(struct iow_node *n, uint64_t peer,
                                    uint64_t cookie,
                                    const struct iow_stream_request *request);
struct iow_rstream *iow_rstream_find(struct iow_node *n, uint64_t cookie);
struct iow_sstream *iow_sstream_find(struct iow_node *n, uint64_t handle);
struct iow_stream_request iow_pattern(uint64_t seed, uint64_t length,
                                      uint32_t piece);
uint8_t iow_pattern_byte(uint64_t seed, uint64_t offset);
void iow_release_data(struct iow_node *n);
/* The caller's pattern file (IOW_FILE_SEED) at IOW_CALLER_SLOT, for FILE
 * writes; made through the node's executor. */
void iow_caller_file(struct iow_node *n);

/* Targeted faults. */
void iow_rule(struct iow_node *n, iow_match match, void *ctx, uint32_t action,
              int32_t result, uint32_t count);
void iow_rules_clear(struct iow_node *n);
void iow_release_held(struct iow_node *n);
uint32_t iow_rule_hits(const struct iow_node *n);
/* Matchers over the engine's user_data (slot kind in bits 48..55). */
uint8_t iow_slot_kind(uint64_t user_data);
bool iow_match_opcode(void *opcode, const struct vsr_io_sqe *sqe);

/* Oracles: the digest history of every cluster and the exactly-once reply
 * table are checked as ops arrive; these check the rest. */
void iow_check_idle_replica(const struct iow_app *app);
void iow_dump_node(const struct iow_node *n);

#endif /* VSR_TEST_IO_WORLD_H */
