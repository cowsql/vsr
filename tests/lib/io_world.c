#define _GNU_SOURCE
#include "config.h"

#include "lib/io_world.h"

#include "internal.h"
#include "io/codec.h"
#include "io/engine.h"
#include "lib/check.h"

#include <errno.h>
#include <fcntl.h>
#include <ftw.h>
#include <inttypes.h>
#include <poll.h>
#include <stdalign.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

/*
 * The engine world of tests/lib/io_world.h. Memory: every engine, payload
 * pool, replica and tail lives in static banks, two per node, so an
 * abandoned engine's memory (a crash) is never reused by its successor
 * while a ring may still be tearing down, and never freed at all.
 */

#define ENGINE_BYTES (2u << 20)
#define PAYLOAD_BYTES (1u << 20)
#define REPLICA_BYTES (6u << 20)
#define TAIL_BYTES (1u << 20)
#define BANKS 2u
#define BATCH 64u
#define OPS_MAX 64u
#define CQES 256u
#define PORT 7000u

struct iow_world iow;

/* Memory banks, mapped anonymously on first use and kept for the process:
 * io_uring pins only anonymous memory for its registered buffers (a large
 * static array can begin inside .data's last, file-backed page), and a
 * crashed ring's teardown may still hold its pages for a while. */
enum bank_kind {
    BANK_ENGINE,
    BANK_PAYLOAD,
    BANK_REPLICA,
    BANK_TAIL,
    BANKS_KINDS
};

static const size_t bank_bytes[BANKS_KINDS] = {ENGINE_BYTES, PAYLOAD_BYTES,
                                               REPLICA_BYTES, TAIL_BYTES};
static unsigned char *banks[BANKS_KINDS][IOW_NODES][BANKS][IOW_REPLICAS];

static unsigned char *bank(uint32_t kind, uint32_t node, uint32_t index,
                           uint32_t replica)
{
    unsigned char **slot = &banks[kind][node][index][replica];

    if (*slot == NULL) {
        void *p = mmap(NULL, bank_bytes[kind], PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);

        CHECK(p != MAP_FAILED);
        *slot = p;
    }
    return *slot;
}
static unsigned char stream_memory[IOW_NODES][IOW_STREAMS][IOW_STREAM_BYTES];
static unsigned char file_bytes[IOW_FILE_BYTES];
static struct faulty_executor faulty_memory[IOW_NODES];
static uint64_t cookie_counter = 1;
static uint64_t lease_counter = 1;
static uint64_t client_numbers[64];

static void app_op(struct iow_node *n, const struct vsr_io_op *op);

/* -------------------------------------------------------------------------
 * Helpers
 * ---------------------------------------------------------------------- */

static uint64_t mix(uint64_t x)
{
    x += UINT64_C(0x9E3779B97F4A7C15);
    x = (x ^ (x >> 30)) * UINT64_C(0xBF58476D1CE4E5B9);
    x = (x ^ (x >> 27)) * UINT64_C(0x94D049BB133111EB);
    return x ^ (x >> 31);
}

uint8_t iow_pattern_byte(uint64_t seed, uint64_t offset)
{
    return (uint8_t)(mix(seed ^ mix(offset >> 3)) >> ((offset & 7u) * 8u));
}

static uint64_t fnv(uint64_t h, const void *bytes, size_t size)
{
    const unsigned char *p = bytes;

    for (size_t i = 0; i < size; ++i) {
        h ^= p[i];
        h *= UINT64_C(0x100000001B3);
    }
    return h;
}

static void put64(unsigned char *p, uint64_t v)
{
    for (uint32_t i = 0; i < 8; ++i) {
        p[i] = (unsigned char)(v >> (8 * i));
    }
}

static uint64_t get64(const unsigned char *p)
{
    uint64_t v = 0;

    for (uint32_t i = 0; i < 8; ++i) {
        v |= (uint64_t)p[i] << (8 * i);
    }
    return v;
}

static uint64_t monotonic(void)
{
    struct timespec ts;

    CHECK(clock_gettime(CLOCK_MONOTONIC, &ts) == 0);
    return (uint64_t)ts.tv_sec * UINT64_C(1000000000) + (uint64_t)ts.tv_nsec;
}

uint64_t iow_now(void)
{
    return iow.backend == IOW_SIM ? vsr_sim_now(iow.sim) : monotonic();
}

struct vsr_id iow_cluster(uint64_t n)
{
    struct vsr_id id = {0xC1u, n};

    return id;
}

struct vsr_id iow_client(uint64_t n)
{
    struct vsr_id id = {0xC11E47u, n};

    return id;
}

static bool id_equal(struct vsr_id a, struct vsr_id b)
{
    return a.hi == b.hi && a.lo == b.lo;
}

uint8_t iow_slot_kind(uint64_t user_data)
{
    return (uint8_t)(user_data >> 48);
}

bool iow_match_opcode(void *opcode, const struct vsr_io_sqe *sqe)
{
    return VSR_IO_OWNER(sqe->user_data) == IOW_OWNER &&
           sqe->opcode == (uint8_t)(uintptr_t)opcode;
}

/* A call into the engine with the purity guard armed (decision 133). */
#define PURE(node, call)                                                       \
    (pure_executor_arm(&(node)->pure), pure_end((node), (call)))

static int pure_end(struct iow_node *n, int result)
{
    pure_executor_disarm(&n->pure);
    return result;
}

/* -------------------------------------------------------------------------
 * The hook: targeted faults
 * ---------------------------------------------------------------------- */

static struct iow_mark *mark_find(struct iow_hook *h, uint64_t user_data)
{
    for (uint32_t i = 0; i < h->marks_count; ++i) {
        if (h->marks[i].user_data == user_data) {
            return &h->marks[i];
        }
    }
    return NULL;
}

static void mark_drop(struct iow_hook *h, struct iow_mark *mark)
{
    uint32_t at = (uint32_t)(mark - h->marks);

    memmove(&h->marks[at], &h->marks[at + 1],
            (size_t)(h->marks_count - at - 1) * sizeof(h->marks[0]));
    h->marks_count--;
}

static uint64_t hook_now(void *ctx)
{
    struct iow_hook *h = ctx;

    return h->inner.ops->now(h->inner.ctx);
}

static void hook_random(void *ctx, void *bytes, size_t size)
{
    struct iow_hook *h = ctx;

    h->inner.ops->random(h->inner.ctx, bytes, size);
}

static int hook_submit(void *ctx, const struct vsr_io_sqe *sqes, uint32_t count,
                       uint32_t want, uint64_t min_wait_ns,
                       uint64_t deadline_ns)
{
    struct iow_hook *h = ctx;
    const struct vsr_io_sqe *out = sqes;

    h->submits++;
    if (h->fail_submit != 0) {
        int rc = h->fail_submit;

        h->fail_submit = 0;
        return rc;
    }
    for (uint32_t i = 0; i < count; ++i) {
        if (sqes[i].opcode == VSR_IO_SQE_PROVIDE) {
            h->provides++;
        }
        for (uint32_t r = 0; r < IOW_RULES; ++r) {
            struct iow_rule *rule = &h->rules[r];

            if (rule->count == 0 || !rule->match(rule->ctx, &sqes[i])) {
                continue;
            }
            rule->count--;
            rule->hits++;
            CHECK(h->marks_count < IOW_MARKS);
            h->marks[h->marks_count].user_data = sqes[i].user_data;
            h->marks[h->marks_count].action = rule->action;
            h->marks[h->marks_count].result = rule->result;
            h->marks_count++;
            if (rule->action == IOW_FAIL_CHAINED) {
                CHECK(sqes[i].opcode == VSR_IO_SQE_FILES_UPDATE);
                CHECK(count <= sizeof(h->batch) / sizeof(h->batch[0]));
                if (out == sqes) {
                    memcpy(h->batch, sqes, (size_t)count * sizeof(*sqes));
                    out = h->batch;
                }
                /* Beyond any table: the kernel fails it (-EINVAL) and
                 * cancels the rest of its chain. */
                h->batch[i].offset = UINT64_C(0xFFFFFF);
            }
            break;
        }
    }
    return h->inner.ops->submit_and_wait(h->inner.ctx, out, count, want,
                                         min_wait_ns, deadline_ns);
}

static uint32_t hook_reap(void *ctx, struct vsr_io_cqe *cqes, uint32_t capacity)
{
    struct iow_hook *h = ctx;
    uint32_t n = 0;
    uint32_t got;
    uint32_t kept = 0;

    if ((!h->holding || h->reverse) && h->held_count > 0) {
        uint32_t take = h->held_count < capacity ? h->held_count : capacity;

        if (h->reverse) {
            /* Newest first. */
            for (uint32_t i = 0; i < take; ++i) {
                cqes[i] = h->held[h->held_count - 1 - i];
            }
        } else {
            memcpy(cqes, h->held, (size_t)take * sizeof(*cqes));
            memmove(h->held, h->held + take,
                    (size_t)(h->held_count - take) * sizeof(h->held[0]));
        }
        h->held_count -= take;
        h->reverse = false;
        n = take;
    }
    got = h->inner.ops->reap(h->inner.ctx, cqes + n, capacity - n);
    for (uint32_t i = n; i < n + got; ++i) {
        struct vsr_io_cqe cqe = cqes[i];
        struct iow_mark *mark = mark_find(h, cqe.user_data);
        bool last = (cqe.flags & VSR_IO_CQE_MORE) == 0;

        if (mark != NULL) {
            switch (mark->action) {
            case IOW_FAIL:
                if (cqe.result >= 0 && (cqe.flags & VSR_IO_CQE_NOTIF) == 0) {
                    cqe.result = mark->result;
                }
                if (last) {
                    mark_drop(h, mark);
                }
                break;
            case IOW_FAIL_CHAINED:
                /* Its own completion; the chain's cancelled records share
                 * the user_data and pass. */
                cqe.result = mark->result;
                mark_drop(h, mark);
                break;
            case IOW_HOLD:
            default:
                if (h->holding || h->held_count > 0) {
                    CHECK(h->held_count < IOW_HELD);
                    h->held[h->held_count++] = cqe;
                    if (last) {
                        mark_drop(h, mark);
                    }
                    continue;
                }
                if (last) {
                    mark_drop(h, mark);
                }
                break;
            }
        }
        cqes[n + kept] = cqe;
        kept++;
    }
    return n + kept;
}

static int hook_register_files(void *ctx, uint32_t slots)
{
    struct iow_hook *h = ctx;

    return h->inner.ops->register_files(h->inner.ctx, slots);
}

static int hook_update_file(void *ctx, uint32_t slot, int fd)
{
    struct iow_hook *h = ctx;

    return h->inner.ops->update_file(h->inner.ctx, slot, fd);
}

static int hook_register_buffers(void *ctx, uint32_t regions)
{
    struct iow_hook *h = ctx;

    return h->inner.ops->register_buffers(h->inner.ctx, regions);
}

static int hook_update_buffer(void *ctx, uint32_t index,
                              const struct vsr_io_region *region)
{
    struct iow_hook *h = ctx;
    int rc = h->inner.ops->update_buffer(h->inner.ctx, index, region);

    if (rc != 0) {
        fprintf(stderr, "update_buffer %u %p %zu: %d\n", index,
                region != NULL ? region->base : NULL,
                region != NULL ? region->size : 0, rc);
    }
    return rc;
}

static int hook_buffer_ring(void *ctx, uint16_t group, uint32_t entries,
                            uint32_t flags, const struct vsr_io_region *memory)
{
    struct iow_hook *h = ctx;
    int rc =
        h->inner.ops->buffer_ring(h->inner.ctx, group, entries, flags, memory);

    if (rc != 0) {
        fprintf(stderr, "buffer_ring %u %u %u: %d\n", group, entries, flags,
                rc);
    }
    return rc;
}

static int hook_provide(void *ctx, uint16_t group,
                        const struct vsr_io_buffer *buffers, uint32_t count)
{
    struct iow_hook *h = ctx;

    return h->inner.ops->provide(h->inner.ctx, group, buffers, count);
}

static void hook_wake(void *ctx)
{
    struct iow_hook *h = ctx;

    h->inner.ops->wake(h->inner.ctx);
}

static const struct vsr_io_executor_ops hook_ops = {hook_now,
                                                    hook_random,
                                                    hook_submit,
                                                    hook_reap,
                                                    hook_register_files,
                                                    hook_update_file,
                                                    hook_register_buffers,
                                                    hook_update_buffer,
                                                    hook_buffer_ring,
                                                    hook_provide,
                                                    hook_wake};

void iow_rule(struct iow_node *n, iow_match match, void *ctx, uint32_t action,
              int32_t result, uint32_t count)
{
    for (uint32_t r = 0; r < IOW_RULES; ++r) {
        struct iow_rule *rule = &n->hook.rules[r];

        if (rule->count == 0) {
            rule->match = match;
            rule->ctx = ctx;
            rule->action = action;
            rule->result = result;
            rule->count = count;
            rule->hits = 0;
            if (action == IOW_HOLD) {
                n->hook.holding = true;
            }
            return;
        }
    }
    CHECK(false);
}

void iow_rules_clear(struct iow_node *n)
{
    memset(n->hook.rules, 0, sizeof(n->hook.rules));
}

uint32_t iow_rule_hits(const struct iow_node *n)
{
    uint32_t hits = 0;

    for (uint32_t r = 0; r < IOW_RULES; ++r) {
        hits += n->hook.rules[r].hits;
    }
    return hits;
}

void iow_release_held(struct iow_node *n)
{
    n->hook.holding = false;
}

void iow_release_held_reversed(struct iow_node *n)
{
    n->hook.reverse = true;
}

void iow_hold(struct iow_node *n)
{
    n->hook.holding = true;
}

/* -------------------------------------------------------------------------
 * Executors
 * ---------------------------------------------------------------------- */

static uint32_t table_slots(void)
{
    return IOW_FILE_SLOT_BASE + iow.io_limits.file_slots + 8u;
}

static uint32_t table_regions(void)
{
    return IOW_REGION_BASE + 1u + IOW_REPLICAS;
}

static struct vsr_io_address unix_address(uint32_t index)
{
    struct vsr_io_address address;
    struct sockaddr_un *un = (struct sockaddr_un *)(void *)&address.sockaddr;
    int length;

    memset(&address, 0, sizeof(address));
    un->sun_family = AF_UNIX;
    /* Abstract: a leading NUL, private to this process's run. */
    length = snprintf(un->sun_path + 1, sizeof(un->sun_path) - 1,
                      "vsr-iow-%ld-%u", (long)getpid(), index);
    CHECK(length > 0);
    address.length = (uint32_t)(offsetof(struct sockaddr_un, sun_path) + 1u +
                                (size_t)length);
    return address;
}

static int ring_open(struct iow_node *n)
{
    struct vsr_io_uring_options o;
    struct vsr_io_need need;
    size_t size;
    int rc;

    memset(&o, 0, sizeof(o));
    o.sq_entries = 128;
    o.cq_entries = 1024;
    o.file_slots = table_slots();
    o.buffer_regions = table_regions();
    o.sqpoll_cpu = UINT32_MAX;
    CHECK(vsr_io_uring_layout(&o, &need) == VSR_OK);
    size = (need.size + need.alignment - 1) & ~(need.alignment - 1);
    n->ring_memory = aligned_alloc(need.alignment, size);
    CHECK(n->ring_memory != NULL);
    rc = vsr_io_uring_init(n->ring_memory, need.size, &o, &n->base);
    if (rc != 0) {
        free(n->ring_memory);
        n->ring_memory = NULL;
    }
    return rc;
}

static void ring_close(struct iow_node *n)
{
    if (n->ring_memory != NULL) {
        vsr_io_uring_deinit(&n->base);
        free(n->ring_memory);
        n->ring_memory = NULL;
    }
}

/* The node's executor stack; the tables are registered as a process would
 * before the engine's init. */
static void node_executor(struct iow_node *n)
{
    struct vsr_io_executor below;

    if (iow.backend == IOW_SIM) {
        n->base = vsr_sim_executor(iow.sim, n->index);
        CHECK(n->base.ops != NULL);
    } else {
        CHECK(ring_open(n) == 0);
    }
    below = n->base;
    if (n->use_faulty) {
        n->faulty = &faulty_memory[n->index];
        faulty_executor_init(n->faulty, &n->base, &n->faulty_options);
        below = faulty_executor_handle(n->faulty);
    } else {
        n->faulty = NULL;
    }
    memset(&n->hook, 0, sizeof(n->hook));
    n->hook.inner = below;
    n->ex.ops = &hook_ops;
    n->ex.ctx = &n->hook;
    n->ex = pure_executor_init(&n->pure, n->ex, true);
    CHECK(n->ex.ops->register_files(n->ex.ctx, table_slots()) == 0);
    CHECK(n->ex.ops->register_buffers(n->ex.ctx, table_regions()) == 0);
}

void iow_node_faulty(uint32_t index, const struct faulty_executor_options *o)
{
    CHECK(index < iow.nodes);
    iow.node[index].use_faulty = o != NULL;
    if (o != NULL) {
        iow.node[index].faulty_options = *o;
    }
}

/* -------------------------------------------------------------------------
 * The world
 * ---------------------------------------------------------------------- */

static void trace_event(void *ctx, const struct vsr_sim_trace_event *event)
{
    (void)ctx;
    fprintf(stderr,
            "trace t=%" PRIu64 " kind %u node %u peer %u op %u ud %" PRIx64
            " result %" PRId64 " bytes %" PRIu64 "\n",
            event->now_ns, event->kind, event->node, event->peer, event->opcode,
            event->user_data, event->result, event->bytes);
}

/* members, operations, input_leases, pending_requests, pending_reads,
 * transfers, log_cache_entries, client_cache_entries, batch_entries,
 * spans_per_blob, work_per_step, command_bytes, result_bytes,
 * manifest_bytes, message_bytes, pinned_payload_bytes. members counts the
 * old and the new membership of a reconfiguration. */
static const struct vsr_limits default_limits = {
    IOW_MEMBERS, 32, 16, 8, 4, 2, 32, 16, 8, 4, 64, 256, 64, 64, 4096, 65536};

static uint64_t round_up(uint64_t value, uint64_t multiple)
{
    return (value + multiple - 1) / multiple * multiple;
}

/* Store options for the world's limits and block size; write_behind 0
 * takes the default of eight blocks. */
static void store_sized(uint64_t write_behind)
{
    struct vsr_io_store_options *o = &iow.store;
    uint64_t max_record = 0;
    size_t segment_limit = 0;
    uint64_t header_bytes;
    uint32_t block = iow.block_bytes;

    CHECK(vsr_io_codec_record_limit(&iow.limits, &max_record) == VSR_OK);
    CHECK(vsr_io_codec_segment_limit(&iow.limits, &segment_limit) == VSR_OK);
    header_bytes = round_up(segment_limit, block);
    memset(o, 0, sizeof(*o));
    o->block_bytes = block;
    o->segments = 2;
    o->max_segments = 8;
    o->max_entries = 256;
    o->max_clients = 8;
    o->inflight_writes = 2;
    o->segment_bytes = round_up(header_bytes + 8 * max_record, block);
    o->write_behind_bytes =
        write_behind != 0 ? write_behind : (uint64_t)8 * block;
    o->cache_bytes =
        round_up(o->write_behind_bytes + iow.limits.pinned_payload_bytes +
                     2 * max_record + 2 * header_bytes + block,
                 block);
    o->direct_io = 1;
    o->sync_mode = VSR_IO_SYNC_FDATASYNC;
    o->on_write_error = VSR_IO_WRITE_ERROR_FENCE;
}

static void default_store(void)
{
    store_sized(0);
}

/* Recomputes the store's derived sizes after a test changed the limits,
 * the block size or write_behind_bytes, keeping the test's policies. */
static void store_resize(void)
{
    struct vsr_io_store_options keep = iow.store;

    store_sized(keep.write_behind_bytes);
    iow.store.max_clients = keep.max_clients;
    iow.store.sync_mode = keep.sync_mode;
    iow.store.on_write_error = keep.on_write_error;
    iow.store.direct_io = keep.direct_io;
    iow.store.flush_interval_ns = keep.flush_interval_ns;
    iow.store.sync_delay_ns = keep.sync_delay_ns;
}

static void world_defaults(uint32_t nodes, uint64_t seed)
{
    struct vsr_io_limits *l = &iow.io_limits;

    memset(&iow, 0, sizeof(iow));
    iow.seed = seed;
    iow.nodes = nodes;
    iow.incarnation = 1;
    iow.durability = VSR_DURABLE;
    iow.limits = default_limits;
    iow.block_bytes = 512;
    iow.heartbeat_ns = 10 * IOW_MS;
    iow.view_timeout_ns = 50 * IOW_MS;
    iow.retry_ns = 5 * IOW_MS;
    iow.transfer_timeout_ns = 100 * IOW_MS;
    iow.handshake_timeout_ns = 200 * IOW_MS;
    iow.stream_chunk_bytes = 1024;
    iow.trace = getenv("IOW_TRACE") != NULL;
    l->replicas = IOW_REPLICAS;
    l->nodes = IOW_NODES + 1;
    l->authorizations = 16;
    l->links = 8;
    l->link_queue = 8;
    l->streams = 4;
    l->stream_window = 2;
    l->events = 16;
    l->ops = 32;
    l->batch = BATCH;
    l->slabs = 64;
    l->slab_bytes = 16384;
    l->caller_slabs = 2;
    l->file_slots = 32;
    l->buffer_regions = 1 + IOW_REPLICAS;
    default_store();
    for (uint32_t i = 0; i < nodes; ++i) {
        iow.node[i].index = i;
    }
    for (uint32_t i = 0; i < 64; ++i) {
        client_numbers[i] = 0;
    }
    for (uint32_t i = 0; i < IOW_FILE_BYTES; ++i) {
        file_bytes[i] = iow_pattern_byte(IOW_FILE_SEED, i);
    }
}

void iow_open_sim(uint32_t nodes, uint64_t seed,
                  const struct vsr_sim_faults *faults)
{
    struct vsr_sim_options *options = &iow.sim_options;

    CHECK(nodes <= IOW_NODES);
    world_defaults(nodes, seed);
    iow.backend = IOW_SIM;
    memset(options, 0, sizeof(*options));
    options->seed = seed;
    options->nodes = nodes;
    options->block_bytes = iow.block_bytes;
    options->file_slots = table_slots();
    options->buffer_regions = table_regions();
    if (faults != NULL) {
        options->faults = *faults;
    } else {
        options->faults.disk.latency_min_ns = 1000;
        options->faults.disk.latency_max_ns = 20000;
        options->faults.disk.fsync_min_ns = 1000;
        options->faults.disk.fsync_max_ns = 50000;
        options->faults.disk.unsynced_keep_ppm = 1000000;
        options->faults.network.delay_min_ns = 1000;
        options->faults.network.delay_max_ns = 50000;
        options->faults.network.stall_reset_ns = 100 * IOW_MS;
        options->faults.network.connect_timeout_ns = 100 * IOW_MS;
    }
    CHECK(vsr_sim_create(options, &iow.sim) == 0);
    if (iow.trace) {
        static const struct vsr_sim_trace trace = {NULL, trace_event};

        vsr_sim_set_trace(iow.sim, &trace);
    }
    for (uint32_t i = 0; i < nodes; ++i) {
        iow.node[i].address = vsr_sim_address(iow.sim, i, (uint16_t)PORT);
    }
}

bool iow_open_uring(uint32_t nodes, uint64_t seed)
{
    struct iow_node probe;
    int rc;

    CHECK(nodes <= IOW_NODES);
    world_defaults(nodes, seed);
    iow.backend = IOW_URING;
    /* Real files: whole 4 KiB blocks for O_DIRECT on any device. */
    iow.block_bytes = 4096;
    default_store();
    memset(&probe, 0, sizeof(probe));
    rc = ring_open(&probe);
    if (rc != 0) {
        fprintf(stderr, "no ring: %s\n", strerror(-rc));
        return false;
    }
    ring_close(&probe);
    (void)snprintf(iow.directory, sizeof(iow.directory), "iow.%ld.XXXXXX",
                   (long)getpid());
    CHECK(mkdtemp(iow.directory) != NULL);
    for (uint32_t i = 0; i < nodes; ++i) {
        iow.node[i].address = unix_address(i);
    }
    return true;
}

static int remove_entry(const char *path, const struct stat *st, int flag,
                        struct FTW *ftw)
{
    (void)st;
    (void)flag;
    (void)ftw;
    return remove(path);
}

void iow_close(void)
{
    for (uint32_t i = 0; i < iow.nodes; ++i) {
        struct iow_node *n = &iow.node[i];

        if (iow.backend == IOW_URING) {
            ring_close(n);
        }
    }
    if (iow.backend == IOW_SIM) {
        vsr_sim_destroy(iow.sim);
    } else if (iow.directory[0] != '\0') {
        CHECK(nftw(iow.directory, remove_entry, 16, FTW_DEPTH | FTW_PHYS) == 0);
    }
    memset(&iow, 0, sizeof(iow));
}

/* -------------------------------------------------------------------------
 * Engines
 * ---------------------------------------------------------------------- */

struct vsr_io_options iow_engine_options(struct iow_node *n)
{
    struct vsr_io_options options;

    memset(&options, 0, sizeof(options));
    options.executor = n->ex;
    options.node = n->index + 1;
    options.listen = &n->address;
    options.listen_count = 1;
    options.handshake = VSR_IO_HANDSHAKE_TRUSTED;
    options.limits = iow.io_limits;
    options.file_slot_base = IOW_FILE_SLOT_BASE;
    options.buffer_region_base = IOW_REGION_BASE;
    options.buffer_group = IOW_GROUP;
    options.owner = IOW_OWNER;
    options.nodelay = 1;
    options.connect_backoff_ns = IOW_MS;
    options.handshake_timeout_ns = iow.handshake_timeout_ns;
    options.idle_timeout_ns = 0;
    options.send_coalesce_bytes = 65536;
    options.zero_copy_bytes = 4096;
    options.stream_chunk_bytes = iow.stream_chunk_bytes;
    return options;
}

static void node_reset(struct iow_node *n)
{
    n->pending_count = 0;
    n->hold_ops = 0;
    memset(n->rstreams, 0, sizeof(n->rstreams));
    for (uint32_t i = 0; i < IOW_STREAMS; ++i) {
        memset(&n->sstreams[i], 0, sizeof(n->sstreams[i]));
        n->sstreams[i].buffer = stream_memory[n->index][i];
    }
    n->stream_serves = n->stream_ends = n->stream_data = 0;
    n->stream_written = 0;
    n->handshakes = n->links_wanted = 0;
    n->hold_serve = n->hold_data = false;
    n->held_data_count = 0;
    n->busy = true;
    n->deadline = 0;
    n->file_ready = false;
    for (uint32_t r = 0; r < IOW_REPLICAS; ++r) {
        struct iow_app *app = &n->apps[r];

        app->node = n;
        app->index = r;
        app->attached = false;
        app->replica = NULL;
        memset(app->leases, 0, sizeof(app->leases));
        memset(&app->fetch, 0, sizeof(app->fetch));
        app->hold_apply = false;
        app->held_apply = 0;
        if (app->next_route == 0) {
            app->next_route = 1;
        }
    }
}

static void node_tables(struct iow_node *n)
{
    for (uint32_t j = 0; j < iow.nodes; ++j) {
        if (j != n->index) {
            CHECK(PURE(n, vsr_io_node_set(n->io, j + 1,
                                          &iow.node[j].address)) == VSR_OK);
        }
    }
    for (uint32_t a = 0; a < iow.authorizations_count; ++a) {
        const struct iow_authorization *auth = &iow.authorizations[a];

        if (auth->node != n->index + 1) {
            CHECK(PURE(n, vsr_io_authorize(n->io, auth->cluster, auth->replica,
                                           auth->node)) == VSR_OK);
        }
    }
}

static void engine_init(struct iow_node *n, const struct vsr_io_options *custom)
{
    struct vsr_io_layout layout;
    struct vsr_io_region metadata;
    struct vsr_io_region payload;
    struct vsr_io_options options;

    node_executor(n);
    options = custom != NULL ? *custom : iow_engine_options(n);
    options.executor = n->ex;
    options.listen = &n->address;
    n->options = options;
    n->options.listen = &n->address;
    CHECK(vsr_io_layout(&options, &layout) == VSR_OK);
    CHECK(layout.metadata.size <= ENGINE_BYTES);
    CHECK(layout.payload.size <= PAYLOAD_BYTES);
    metadata.base = bank(BANK_ENGINE, n->index, n->bank, 0);
    metadata.size = layout.metadata.size;
    payload.base = bank(BANK_PAYLOAD, n->index, n->bank, 0);
    payload.size = layout.payload.size;
    {
        int rc = vsr_io_init(&options, &metadata, &payload, &n->io);

        if (rc != VSR_OK) {
            fprintf(stderr, "node %u: vsr_io_init %d (%s)\n", n->index, rc,
                    rc < 0 && rc > -4096 ? strerror(-rc) : "");
        }
        CHECK(rc == VSR_OK);
    }
    n->open = true;
    n->crashed = false;
    n->incarnation++;
    node_reset(n);
}

struct iow_node *iow_node_open_with(uint32_t index,
                                    const struct vsr_io_options *options)
{
    struct iow_node *n = &iow.node[index];

    CHECK(index < iow.nodes && !n->open);
    engine_init(n, options);
    node_tables(n);
    return n;
}

struct iow_node *iow_node_open(uint32_t index)
{
    return iow_node_open_with(index, NULL);
}

void iow_authorize(struct vsr_id cluster, uint64_t replica, uint64_t node)
{
    CHECK(iow.authorizations_count < 32);
    iow.authorizations[iow.authorizations_count].cluster = cluster;
    iow.authorizations[iow.authorizations_count].replica = replica;
    iow.authorizations[iow.authorizations_count].node = node;
    iow.authorizations_count++;
    for (uint32_t i = 0; i < iow.nodes; ++i) {
        struct iow_node *n = &iow.node[i];

        if (n->open && node != i + 1) {
            CHECK(PURE(n, vsr_io_authorize(n->io, cluster, replica, node)) ==
                  VSR_OK);
        }
    }
}

void iow_mesh(uint32_t count, struct vsr_id cluster)
{
    for (uint32_t j = 0; j < count; ++j) {
        iow_authorize(cluster, j + 1, j + 1);
    }
}

bool iow_node_closed(void *node)
{
    const struct iow_node *n = node;
    struct vsr_io_stats stats;

    vsr_io_get_stats(n->io, &stats);
    return stats.closed != 0;
}

void iow_close_io(struct iow_node *n)
{
    CHECK(PURE(n, vsr_io_close(n->io)) == VSR_OK);
    vsr_io_wake(n->io);
}

void iow_close_node(struct iow_node *n)
{
    iow_close_io(n);
    if (!iow_run_until(iow_node_closed, n, 5000 * IOW_MS)) {
        iow_dump_node(n);
        CHECK(false);
    }
    CHECK(vsr_io_deinit(n->io) == VSR_OK);
    n->open = false;
    if (iow.backend == IOW_URING) {
        ring_close(n);
    }
}

/* -------------------------------------------------------------------------
 * Crash and restart
 * ---------------------------------------------------------------------- */

void iow_crash(struct iow_node *n)
{
    CHECK(n->open);
    if (iow.backend == IOW_SIM) {
        /* The sim verifies what the executor still references: the
         * engine's memory is still valid here, and abandoned after. */
        vsr_sim_crash(iow.sim, n->index);
    } else {
        ring_close(n);
    }
    n->open = false;
    n->crashed = true;
    n->io = NULL;
    for (uint32_t r = 0; r < IOW_REPLICAS; ++r) {
        n->apps[r].attached = false;
        n->apps[r].replica = NULL;
    }
    node_reset(n);
}

/* On a ring the crashed listener closes asynchronously; its abstract name
 * is free once a bind succeeds. */
static void wait_address(const struct iow_node *n)
{
    for (uint32_t i = 0; i < 5000; ++i) {
        int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
        int rc;

        CHECK(fd >= 0);
        rc = bind(fd, (const struct sockaddr *)&n->address.sockaddr,
                  (socklen_t)n->address.length);
        CHECK(close(fd) == 0);
        if (rc == 0) {
            return;
        }
        CHECK(errno == EADDRINUSE);
        CHECK(usleep(1000) == 0);
    }
    CHECK(false);
}

void iow_restart(struct iow_node *n)
{
    CHECK(n->crashed && !n->open);
    if (iow.backend == IOW_SIM) {
        struct vsr_io_executor fresh = vsr_sim_restart(iow.sim, n->index);

        CHECK(fresh.ops != NULL);
    } else {
        wait_address(n);
    }
    n->bank = (n->bank + 1) % BANKS;
    engine_init(n, &n->options);
    node_tables(n);
}

/* -------------------------------------------------------------------------
 * Leases and events
 * ---------------------------------------------------------------------- */

static struct iow_lease *lease_take(struct iow_app *app)
{
    for (uint32_t i = 0; i < IOW_LEASES; ++i) {
        struct iow_lease *slot = &app->leases[i];

        if (slot->id == 0) {
            memset(slot, 0, sizeof(*slot));
            slot->id = lease_counter++;
            return slot;
        }
    }
    CHECK(false);
    return NULL;
}

uint32_t iow_leases_out(const struct iow_app *app)
{
    uint32_t count = 0;

    for (uint32_t i = 0; i < IOW_LEASES; ++i) {
        count += app->leases[i].id != 0 ? 1u : 0u;
    }
    return count;
}

static void lease_release(struct iow_app *app, uint64_t id)
{
    for (uint32_t i = 0; i < IOW_LEASES; ++i) {
        if (app->leases[i].id == id) {
            app->leases[i].id = 0;
            app->releases++;
            return;
        }
    }
    fprintf(stderr,
            "RELEASE of a lease the caller does not hold: %" PRIu64 "\n", id);
    CHECK(false);
}

void iow_lease_forget(struct iow_app *app, uint64_t id)
{
    for (uint32_t i = 0; i < IOW_LEASES; ++i) {
        if (app->leases[i].id == id) {
            app->leases[i].id = 0;
            return;
        }
    }
    CHECK(false);
}

int iow_submit(struct iow_node *n, const struct vsr_io_event *events,
               uint32_t count, uint32_t *consumed)
{
    return PURE(n, vsr_io_submit(n->io, events, count, consumed));
}

void iow_queue(struct iow_node *n, const struct vsr_io_event *event)
{
    CHECK(n->open);
    CHECK(n->pending_count < IOW_PENDING);
    n->pending[n->pending_count++] = *event;
    vsr_io_wake(n->io);
}

void iow_core_event(struct iow_app *app, uint32_t type, int32_t status,
                    uint64_t id, const void *data, uint64_t lease)
{
    struct vsr_io_event event;

    memset(&event, 0, sizeof(event));
    event.replica = app->replica;
    event.kind = VSR_IO_EVENT_CORE;
    event.event.type = type;
    event.event.status = status;
    event.event.id = id;
    event.event.data = data;
    event.event.lease = lease;
    iow_queue(app->node, &event);
}

static void complete_op(struct iow_app *app, uint64_t id, int32_t status)
{
    iow_core_event(app, VSR_EVENT_COMPLETE, status, id, NULL, 0);
}

void iow_make_request(struct iow_app *app, uint64_t client, uint64_t number,
                      struct vsr_io_event *out)
{
    struct iow_lease *slot = lease_take(app);
    uint64_t route = app->next_route++;

    put64(slot->bytes, number);
    slot->span.data = slot->bytes;
    slot->span.size = 8;
    slot->blob.spans = &slot->span;
    slot->blob.size = 8;
    slot->blob.count = 1;
    slot->request.id.client = iow_client(client);
    slot->request.id.number = number;
    slot->request.epoch = app->status.epoch;
    slot->request.type = VSR_REQUEST_COMMAND;
    slot->request.body = &slot->blob;
    memset(out, 0, sizeof(*out));
    out->replica = app->replica;
    out->kind = VSR_IO_EVENT_CORE;
    out->event.type = VSR_EVENT_REQUEST;
    out->event.id = route;
    out->event.data = &slot->request;
    out->event.lease = slot->id;
}

uint64_t iow_request(struct iow_app *app, uint64_t client, uint64_t number)
{
    struct vsr_io_event event;

    iow_make_request(app, client, number, &event);
    iow_queue(app->node, &event);
    return event.event.id;
}

uint64_t iow_reconfigure(struct iow_app *app, uint64_t client, uint64_t number,
                         uint64_t epoch, uint32_t count)
{
    struct iow_lease *slot = lease_take(app);
    struct vsr_io_event event;

    CHECK(count <= IOW_MEMBERS);
    for (uint32_t i = 0; i < count; ++i) {
        slot->members[i].id = i + 1;
        slot->members[i].role = VSR_MEMBER_FULL;
        slot->members[i].reserved = 0;
    }
    slot->membership.epoch = epoch;
    slot->membership.members = slot->members;
    slot->membership.count = count;
    slot->membership.faults = (count - 1) / 2;
    slot->request.id.client = iow_client(client);
    slot->request.id.number = number;
    slot->request.epoch = epoch - 1;
    slot->request.type = VSR_REQUEST_RECONFIGURE;
    slot->request.body = &slot->membership;
    memset(&event, 0, sizeof(event));
    event.replica = app->replica;
    event.kind = VSR_IO_EVENT_CORE;
    event.event.type = VSR_EVENT_REQUEST;
    event.event.id = app->next_route++;
    event.event.data = &slot->request;
    event.event.lease = slot->id;
    iow_queue(app->node, &event);
    return event.event.id;
}

/* -------------------------------------------------------------------------
 * The application
 * ---------------------------------------------------------------------- */

static struct iow_app *app_of(struct iow_node *n,
                              const struct vsr_io_replica *replica)
{
    for (uint32_t r = 0; r < IOW_REPLICAS; ++r) {
        if (n->apps[r].attached && n->apps[r].replica == replica) {
            return &n->apps[r];
        }
    }
    return NULL;
}

static struct iow_history *history_of(const struct iow_app *app)
{
    struct vsr_id cluster = app->options.core.cluster;

    for (uint32_t i = 0; i < IOW_CLUSTERS; ++i) {
        if (iow.history[i].used && id_equal(iow.history[i].cluster, cluster)) {
            return &iow.history[i];
        }
    }
    for (uint32_t i = 0; i < IOW_CLUSTERS; ++i) {
        if (!iow.history[i].used) {
            iow.history[i].used = true;
            iow.history[i].cluster = cluster;
            return &iow.history[i];
        }
    }
    CHECK(false);
    return NULL;
}

/* The digest after `op` is the same on every replica that reaches it. */
static void history_check(struct iow_app *app, uint64_t op, uint64_t digest)
{
    struct iow_history *h = history_of(app);

    if (!app->check_history || op >= IOW_HISTORY) {
        return;
    }
    if (h->known[op]) {
        if (h->digest[op] != digest) {
            fprintf(stderr,
                    "node %u replica %" PRIu64 ": digest at op %" PRIu64
                    " is %" PRIx64 ", the history says %" PRIx64 "\n",
                    app->node->index, app->options.core.replica, op, digest,
                    h->digest[op]);
            CHECK(false);
        }
        return;
    }
    h->known[op] = 1;
    h->digest[op] = digest;
}

static uint64_t fold_entry(uint64_t digest, const struct vsr_entry *entry)
{
    const struct vsr_blob *body = entry->body;
    unsigned char head[32];

    put64(head, entry->op);
    put64(head + 8, entry->request.client.hi);
    put64(head + 16, entry->request.client.lo);
    put64(head + 24, entry->request.number);
    digest = fnv(digest == 0 ? UINT64_C(0xCBF29CE484222325) : digest, head,
                 sizeof(head));
    if (body != NULL) {
        for (uint32_t i = 0; i < body->count; ++i) {
            digest = fnv(digest, body->spans[i].data, body->spans[i].size);
        }
    }
    return digest;
}

static void complete_apply(struct iow_app *app, uint64_t id,
                           const struct vsr_apply *apply)
{
    struct iow_lease *slot = lease_take(app);

    CHECK(apply->batch.count <= 16);
    for (uint32_t i = 0; i < apply->batch.count; ++i) {
        const struct vsr_entry *entry = &apply->batch.entries[i];
        struct vsr_value *value = &slot->values[i];

        memset(value, 0, sizeof(*value));
        if (entry->op != app->applied_op + 1) {
            fprintf(stderr,
                    "node %u replica %" PRIu64 ": APPLY of op %" PRIu64
                    " after op %" PRIu64 "\n",
                    app->node->index, app->options.core.replica, entry->op,
                    app->applied_op);
            CHECK(false);
        }
        app->applied_op = entry->op;
        if (entry->type == VSR_REQUEST_COMMAND) {
            app->digest = fold_entry(app->digest, entry);
            app->commands++;
            put64(slot->results[i], app->digest);
            slot->result_spans[i].data = slot->results[i];
            slot->result_spans[i].size = 8;
            value->data.spans = &slot->result_spans[i];
            value->data.size = 8;
            value->data.count = 1;
        }
        history_check(app, entry->op, app->digest);
    }
    slot->applied.results = apply->batch.count > 0 ? slot->values : NULL;
    slot->applied.count = apply->batch.count;
    app->applies++;
    app->applied_entries += apply->batch.count;
    iow_core_event(app, VSR_EVENT_COMPLETE, VSR_IO_OK, id, &slot->applied,
                   slot->id);
}

/* The reply of an executed request carries the digest after its op, the
 * same on every replica and at every retry (exactly once). */
static void reply_check(const struct iow_app *app,
                        const struct vsr_reply *reply)
{
    const struct vsr_blob *blob = &reply->result.data;
    unsigned char bytes[8];
    uint64_t result;
    struct iow_reply_record *record = NULL;
    size_t filled = 0;

    if ((reply->flags & VSR_REPLY_EXECUTED) == 0) {
        return;
    }
    CHECK(blob->size == 8 || blob->size == 0);
    memset(bytes, 0, sizeof(bytes));
    if (blob->size == 0) {
        filled = 8; /* A control entry's empty result. */
    }
    for (uint32_t i = 0; i < blob->count && filled < 8; ++i) {
        size_t take = blob->spans[i].size;

        CHECK(filled + take <= 8);
        memcpy(bytes + filled, blob->spans[i].data, take);
        filled += take;
    }
    CHECK(filled == 8);
    result = get64(bytes);
    if (blob->size == 8 && app->check_history && reply->op < IOW_HISTORY &&
        history_of(app)->known[reply->op]) {
        CHECK(history_of(app)->digest[reply->op] == result);
    }
    for (uint32_t i = 0; i < iow.replies_count; ++i) {
        if (id_equal(iow.replies[i].cluster, app->options.core.cluster) &&
            id_equal(iow.replies[i].client, reply->request.client) &&
            iow.replies[i].number == reply->request.number) {
            record = &iow.replies[i];
            break;
        }
    }
    if (record != NULL) {
        if (record->op != reply->op || record->result != result) {
            fprintf(stderr,
                    "request %" PRIu64 "/%" PRIu64 " replied op %" PRIu64
                    " result %" PRIx64 ", earlier op %" PRIu64
                    " result %" PRIx64 "\n",
                    reply->request.client.lo, reply->request.number, reply->op,
                    result, record->op, record->result);
            CHECK(false);
        }
        return;
    }
    CHECK(iow.replies_count < sizeof(iow.replies) / sizeof(iow.replies[0]));
    record = &iow.replies[iow.replies_count++];
    record->cluster = app->options.core.cluster;
    record->client = reply->request.client;
    record->number = reply->request.number;
    record->op = reply->op;
    record->result = result;
}

static struct iow_snapshot *snapshot_find(struct iow_app *app, struct vsr_id id)
{
    for (uint32_t i = 0; i < IOW_SNAPSHOTS; ++i) {
        if (app->snapshots[i].used && id_equal(app->snapshots[i].id, id)) {
            return &app->snapshots[i];
        }
    }
    return NULL;
}

static void snapshot_keep(struct iow_app *app, struct vsr_id id, uint64_t op,
                          uint64_t digest)
{
    struct iow_snapshot *s = snapshot_find(app, id);

    for (uint32_t i = 0; s == NULL && i < IOW_SNAPSHOTS; ++i) {
        if (!app->snapshots[i].used) {
            s = &app->snapshots[i];
        }
    }
    CHECK(s != NULL);
    s->used = true;
    s->id = id;
    s->op = op;
    s->digest = digest;
}

/* The digest and op a checkpoint's manifest carries. */
static void manifest_read(const struct vsr_checkpoint *checkpoint,
                          uint64_t *digest, uint64_t *op)
{
    unsigned char bytes[16];
    size_t filled = 0;

    CHECK(checkpoint->manifest.size == 16);
    for (uint32_t i = 0; i < checkpoint->manifest.count; ++i) {
        size_t take = checkpoint->manifest.spans[i].size;

        CHECK(filled + take <= sizeof(bytes));
        memcpy(bytes + filled, checkpoint->manifest.spans[i].data, take);
        filled += take;
    }
    CHECK(filled == 16);
    *digest = get64(bytes);
    *op = get64(bytes + 8);
}

/* A checkpoint returned to the core under a fresh lease: the task's fields,
 * with the manifest the caller's (CAPTURE) or the task's own (FETCH). */
static void complete_checkpoint(struct iow_app *app, uint64_t id,
                                const struct vsr_checkpoint *checkpoint,
                                bool manifest, uint64_t digest)
{
    struct iow_lease *slot = lease_take(app);

    slot->checkpoint = *checkpoint;
    if (manifest) {
        put64(slot->bytes, digest);
        put64(slot->bytes + 8, checkpoint->op);
        slot->span.data = slot->bytes;
        slot->span.size = 16;
        slot->checkpoint.manifest.spans = &slot->span;
        slot->checkpoint.manifest.size = 16;
        slot->checkpoint.manifest.count = 1;
    }
    iow_core_event(app, VSR_EVENT_COMPLETE, VSR_IO_OK, id, &slot->checkpoint,
                   slot->id);
}

static void app_capture(struct iow_app *app, const struct vsr_op *op)
{
    const struct vsr_snapshot_task *task = op->data;

    CHECK(task != NULL && task->checkpoint != NULL);
    CHECK(task->checkpoint->id.hi != 0 || task->checkpoint->id.lo != 0);
    app->captures++;
    if (app->capture_status != VSR_IO_OK) {
        complete_op(app, op->id, app->capture_status);
        return;
    }
    /* The core fences APPLY around CAPTURE: the application is at op. */
    CHECK(app->applied_op == task->checkpoint->op);
    snapshot_keep(app, task->checkpoint->id, task->checkpoint->op, app->digest);
    complete_checkpoint(app, op->id, task->checkpoint, true, app->digest);
}

static struct iow_node *node_of_replica(struct vsr_id cluster, uint64_t replica)
{
    for (uint32_t a = 0; a < iow.authorizations_count; ++a) {
        const struct iow_authorization *auth = &iow.authorizations[a];

        if (id_equal(auth->cluster, cluster) && auth->replica == replica &&
            auth->node >= 1 && auth->node <= iow.nodes) {
            return &iow.node[auth->node - 1];
        }
    }
    return NULL;
}

static void app_fetch(struct iow_app *app, const struct vsr_op *op)
{
    const struct vsr_snapshot_task *task = op->data;
    struct iow_node *source;
    struct iow_stream_request request;
    uint64_t digest = 0;
    uint64_t at = 0;

    CHECK(task != NULL && task->checkpoint != NULL);
    app->fetches++;
    if (app->fetch_status != VSR_IO_OK) {
        complete_op(app, op->id, app->fetch_status);
        return;
    }
    manifest_read(task->checkpoint, &digest, &at);
    CHECK(at == task->checkpoint->op);
    if (!app->fetch_by_stream) {
        app->fetches_ok++;
        complete_checkpoint(app, op->id, task->checkpoint, false, 0);
        return;
    }
    source = node_of_replica(app->options.core.cluster, task->peer);
    CHECK(!app->fetch.active);
    if (source == NULL || source == app->node) {
        complete_op(app, op->id, VSR_IO_RETRY);
        return;
    }
    memset(&request, 0, sizeof(request));
    request.magic = IOW_STREAM_MAGIC;
    request.kind = IOW_STREAM_APPSNAP;
    request.snapshot = task->checkpoint->id;
    request.cluster = app->options.core.cluster;
    request.replica = task->peer;
    app->fetch.active = true;
    app->fetch.op = op->id;
    app->fetch.checkpoint = task->checkpoint;
    app->fetch.digest = digest;
    app->fetch.cookie = (UINT64_C(1) << 63) | cookie_counter++;
    iow_stream_open(app->node, source->index + 1, app->fetch.cookie, &request)
        ->app = app;
}

/* The application's image arrived (or not): FETCH completes OK only with
 * every byte of the image the manifest names. */
static void app_fetch_end(struct iow_app *app, const struct iow_rstream *s)
{
    uint64_t op = app->fetch.op;

    CHECK(app->fetch.active);
    app->fetch.active = false;
    if (s->status == VSR_IO_OK && !s->mismatch &&
        s->received == IOW_APPSNAP_BYTES) {
        app->fetches_ok++;
        complete_checkpoint(app, op, app->fetch.checkpoint, false, 0);
        return;
    }
    complete_op(app, op, VSR_IO_RETRY);
}

static void app_install(struct iow_app *app, const struct vsr_op *op)
{
    const struct vsr_snapshot_task *task = op->data;
    uint64_t digest = 0;
    uint64_t at = 0;

    CHECK(task != NULL);
    app->installs++;
    if (task->checkpoint == NULL) {
        CHECK(task->op == 0);
        app->digest = 0;
        app->applied_op = 0;
    } else {
        manifest_read(task->checkpoint, &digest, &at);
        CHECK(at == task->checkpoint->op);
        history_check(app, at, digest);
        app->digest = digest;
        app->applied_op = at;
        snapshot_keep(app, task->checkpoint->id, at, digest);
    }
    complete_op(app, op->id, VSR_IO_OK);
}

static void app_drop(struct iow_app *app, const struct vsr_op *op)
{
    const struct vsr_snapshot_task *task = op->data;
    struct iow_snapshot *s;

    app->drops++;
    if (task != NULL && task->checkpoint != NULL) {
        s = snapshot_find(app, task->checkpoint->id);
        if (s != NULL) {
            s->used = false;
        }
    }
    complete_op(app, op->id, VSR_IO_OK);
}

static void app_core_op(struct iow_app *app, const struct vsr_op *op)
{
    const struct vsr_reply *reply;

    switch (op->type) {
    case VSR_OP_APPLY:
        if (app->hold_apply) {
            CHECK(app->held_apply == 0);
            app->held_apply = op->id;
            app->held_data = op->data;
            return;
        }
        complete_apply(app, op->id, op->data);
        return;
    case VSR_OP_READ_READY:
        app->read_ready++;
        complete_op(app, op->id, VSR_IO_OK);
        return;
    case VSR_OP_REPLY:
        reply = op->data;
        app->replies++;
        app->last_reply_status = reply->status;
        if (reply->status < 16) {
            app->reply_statuses[reply->status]++;
        }
        if (reply->status == VSR_REPLY_OK) {
            app->replies_ok++;
        }
        if ((reply->flags & VSR_REPLY_EXECUTED) != 0) {
            app->replies_executed++;
        }
        reply_check(app, reply);
        complete_op(app, op->id, VSR_IO_OK);
        return;
    case VSR_OP_SNAPSHOT_CAPTURE:
        app_capture(app, op);
        return;
    case VSR_OP_SNAPSHOT_FETCH:
        app_fetch(app, op);
        return;
    case VSR_OP_SNAPSHOT_SYNC:
        app->syncs++;
        app->sync_ns = iow_now();
        complete_op(app, op->id, VSR_IO_OK);
        return;
    case VSR_OP_SNAPSHOT_INSTALL:
        app_install(app, op);
        return;
    case VSR_OP_SNAPSHOT_DROP:
        app_drop(app, op);
        return;
    case VSR_OP_RELEASE:
        lease_release(app, op->arg);
        return;
    default:
        fprintf(stderr, "unexpected forwarded op type %u\n", op->type);
        CHECK(false);
    }
}

/* -------------------------------------------------------------------------
 * Streams
 * ---------------------------------------------------------------------- */

struct iow_stream_request iow_pattern(uint64_t seed, uint64_t length,
                                      uint32_t piece)
{
    struct iow_stream_request r;

    memset(&r, 0, sizeof(r));
    r.magic = IOW_STREAM_MAGIC;
    r.kind = IOW_STREAM_PATTERN;
    r.seed = seed;
    r.length = length;
    r.stop = length;
    r.piece = piece;
    r.write_kind = VSR_IO_WRITE_BUFFERS;
    r.close_status = VSR_IO_OK;
    return r;
}

struct iow_rstream *iow_rstream_find(struct iow_node *n, uint64_t cookie)
{
    for (uint32_t i = 0; i < IOW_STREAMS; ++i) {
        if (n->rstreams[i].used && n->rstreams[i].cookie == cookie) {
            return &n->rstreams[i];
        }
    }
    return NULL;
}

struct iow_sstream *iow_sstream_find(struct iow_node *n, uint64_t handle)
{
    for (uint32_t i = 0; i < IOW_STREAMS; ++i) {
        if (n->sstreams[i].used && n->sstreams[i].handle == handle) {
            return &n->sstreams[i];
        }
    }
    return NULL;
}

struct iow_rstream *iow_stream_open(struct iow_node *n, uint64_t peer,
                                    uint64_t cookie,
                                    const struct iow_stream_request *request)
{
    struct iow_rstream *s = NULL;
    struct vsr_io_event event;

    for (uint32_t i = 0; i < IOW_STREAMS; ++i) {
        if (!n->rstreams[i].used || n->rstreams[i].ended) {
            if (!n->rstreams[i].used || n->rstreams[i].app == NULL) {
                s = &n->rstreams[i];
                break;
            }
        }
    }
    CHECK(s != NULL);
    memset(s, 0, sizeof(*s));
    s->used = true;
    s->cookie = cookie;
    s->seed = request->kind == IOW_STREAM_APPSNAP ? 0 : request->seed;
    s->status = -1;
    s->request = *request;
    s->open.node = peer;
    s->open.request.data = &s->request;
    s->open.request.size = sizeof(s->request);
    memset(&event, 0, sizeof(event));
    event.kind = VSR_IO_EVENT_STREAM_OPEN;
    event.event.id = cookie;
    event.event.data = &s->open;
    event.event.lease = lease_counter++;
    iow_queue(n, &event);
    return s;
}

/* A rail COMPLETE, submitted at once (a rail completion is never AGAIN). */
static void rail_complete(struct iow_node *n, uint64_t id, int32_t status)
{
    struct vsr_io_event event;
    uint32_t consumed = 0;

    memset(&event, 0, sizeof(event));
    event.kind = VSR_IO_EVENT_COMPLETE;
    event.event.type = VSR_EVENT_COMPLETE;
    event.event.id = id;
    event.event.status = status;
    CHECK(PURE(n, vsr_io_submit(n->io, &event, 1, &consumed)) == VSR_OK);
    CHECK(consumed == 1);
}

void iow_release_data(struct iow_node *n)
{
    n->hold_data = false;
    for (uint32_t i = 0; i < n->held_data_count; ++i) {
        rail_complete(n, n->held_data[i], VSR_IO_OK);
    }
    n->held_data_count = 0;
}

static struct iow_sstream *sstream_take(struct iow_node *n)
{
    for (uint32_t i = 0; i < IOW_STREAMS; ++i) {
        if (!n->sstreams[i].used) {
            struct iow_sstream *s = &n->sstreams[i];
            unsigned char *buffer = s->buffer;

            memset(s, 0, sizeof(*s));
            s->buffer = buffer;
            s->used = true;
            return s;
        }
    }
    CHECK(false);
    return NULL;
}

/* The source of a harness stream: decides from the request's bytes. */
static void rail_serve(struct iow_node *n, uint64_t op,
                       const struct vsr_io_stream_serve *serve)
{
    struct iow_stream_request r;
    struct iow_sstream *s;

    n->stream_serves++;
    CHECK(serve->request.size == sizeof(r));
    memcpy(&r, serve->request.data, sizeof(r));
    CHECK(r.magic == IOW_STREAM_MAGIC);
    s = sstream_take(n);
    s->handle = serve->stream;
    s->status = -1;
    if (r.kind == IOW_STREAM_APPSNAP) {
        struct iow_app *app = NULL;
        struct iow_snapshot *snap = NULL;

        for (uint32_t i = 0; i < IOW_REPLICAS; ++i) {
            if (n->apps[i].attached &&
                id_equal(n->apps[i].options.core.cluster, r.cluster) &&
                n->apps[i].options.core.replica == r.replica) {
                app = &n->apps[i];
            }
        }
        if (app != NULL) {
            snap = snapshot_find(app, r.snapshot);
        }
        if (snap == NULL) {
            r.refuse = VSR_IO_NOT_FOUND;
        } else {
            r.seed = snap->digest;
            r.length = IOW_APPSNAP_BYTES;
            r.stop = r.length;
            r.piece = 4096;
            r.write_kind = VSR_IO_WRITE_BUFFERS;
            r.close_status = VSR_IO_OK;
        }
    }
    s->kind = r.kind;
    s->seed = r.seed;
    s->length = r.length;
    s->stop = r.stop < r.length ? r.stop : r.length;
    s->piece = r.piece > 0 ? r.piece : 1024;
    s->write_kind = r.write_kind;
    s->close_status = r.close_status;
    s->hold = r.hold != 0;
    if (r.write_kind == VSR_IO_WRITE_BUFFERS) {
        CHECK(s->length <= IOW_STREAM_BYTES);
        for (uint64_t i = 0; i < s->length; ++i) {
            s->buffer[i] = iow_pattern_byte(s->seed, i);
        }
    } else {
        CHECK(s->seed == IOW_FILE_SEED && n->file_ready);
    }
    if (r.refuse != 0) {
        /* A refused stream is freed by the engine without an END op. */
        s->used = false;
        rail_complete(n, op, r.refuse);
        return;
    }
    if (n->hold_serve) {
        s->serve_op = op;
        return;
    }
    s->accepted = true;
    rail_complete(n, op, VSR_IO_OK);
}

/* Sources write what they can: AGAIN (the window) waits for WRITTEN. */
static bool pump_sources(struct iow_node *n)
{
    bool progress = false;

    for (uint32_t i = 0; i < IOW_STREAMS; ++i) {
        struct iow_sstream *s = &n->sstreams[i];

        while (s->used && s->accepted && !s->closed && !s->ended) {
            struct vsr_io_event event;
            uint32_t consumed = 0;
            int rc;

            memset(&event, 0, sizeof(event));
            if (s->queued < s->stop) {
                uint32_t at = s->writes % IOW_STREAM_WRITES;
                struct vsr_io_stream_write *w = &s->descriptors[at];
                uint64_t take = s->stop - s->queued;

                CHECK(s->writes - s->written < IOW_STREAM_WRITES);
                if (take > s->piece) {
                    take = s->piece;
                }
                memset(w, 0, sizeof(*w));
                w->stream = s->handle;
                w->write = s->writes + 1u;
                w->kind = s->write_kind;
                if (s->write_kind == VSR_IO_WRITE_FILE) {
                    w->slot = IOW_CALLER_SLOT;
                    w->offset = s->queued;
                    w->length = take;
                } else {
                    s->spans[at].data = s->buffer + s->queued;
                    s->spans[at].size = (size_t)take;
                    w->buffers.spans = &s->spans[at];
                    w->buffers.size = take;
                    w->buffers.count = 1;
                    event.event.lease = lease_counter++;
                }
                event.kind = VSR_IO_EVENT_STREAM_WRITE;
                event.event.data = w;
                rc = PURE(n, vsr_io_submit(n->io, &event, 1, &consumed));
                if (rc == VSR_AGAIN) {
                    break;
                }
                if (rc != VSR_OK) {
                    /* Ended under the caller before its END op was read. */
                    s->closed = true;
                    break;
                }
                s->queued += take;
                s->writes++;
                progress = true;
                continue;
            }
            if (s->hold) {
                break;
            }
            event.kind = VSR_IO_EVENT_STREAM_CLOSE;
            event.event.id = s->handle;
            event.event.status = s->close_status;
            rc = PURE(n, vsr_io_submit(n->io, &event, 1, &consumed));
            s->closed = true;
            progress = true;
            (void)rc; /* EINVAL: the stream ended first (decision 102). */
        }
    }
    return progress;
}

static void rail_data(struct iow_node *n, uint64_t op,
                      const struct vsr_io_stream_data *data)
{
    struct iow_rstream *s = iow_rstream_find(n, data->stream);
    const unsigned char *bytes = data->bytes.data;

    n->stream_data++;
    CHECK(s != NULL && !s->ended);
    s->data_ops++;
    if (data->offset != s->received) {
        s->mismatch = true;
    }
    if (s->app != NULL && s->seed == 0) {
        s->seed = s->app->fetch.digest;
    }
    for (size_t i = 0; i < data->bytes.size; ++i) {
        if (bytes[i] != iow_pattern_byte(s->seed, data->offset + i)) {
            s->mismatch = true;
            break;
        }
    }
    s->received += data->bytes.size;
    if (n->hold_data) {
        CHECK(n->held_data_count < IOW_PENDING);
        n->held_data[n->held_data_count++] = op;
        return;
    }
    rail_complete(n, op, VSR_IO_OK);
}

static void rail_end(struct iow_node *n, const struct vsr_io_stream_end *end)
{
    struct iow_rstream *r = iow_rstream_find(n, end->stream);
    struct iow_sstream *s = iow_sstream_find(n, end->stream);

    n->stream_ends++;
    if (r != NULL && !r->ended) {
        r->ended = true;
        r->status = end->status;
        r->end_bytes = end->bytes;
        if (r->app != NULL) {
            struct iow_app *app = r->app;

            r->app = NULL;
            if (app->attached && app->fetch.active &&
                app->fetch.cookie == r->cookie) {
                app_fetch_end(app, r);
            }
        }
        return;
    }
    CHECK(s != NULL && !s->ended);
    s->ended = true;
    s->status = end->status;
    s->end_bytes = end->bytes;
}

/* A forwarded op and its rail descriptor, copied before any other engine
 * call can reuse the ring entry the descriptor lives in (decision 132). */
struct op_copy {
    struct vsr_io_op op;
    union {
        struct vsr_io_stream_serve serve;
        struct vsr_io_stream_data data;
        struct vsr_io_stream_end end;
        struct vsr_io_stream_written written;
        struct vsr_io_link_wanted wanted;
        struct vsr_status status;
    } rail;
};

static void copy_op(struct op_copy *out, const struct vsr_io_op *op)
{
    out->op = *op;
    switch (op->kind) {
    case VSR_IO_OP_STREAM_SERVE:
        out->rail.serve = *(const struct vsr_io_stream_serve *)op->op.data;
        out->op.op.data = &out->rail.serve;
        break;
    case VSR_IO_OP_STREAM_DATA:
        out->rail.data = *(const struct vsr_io_stream_data *)op->op.data;
        out->op.op.data = &out->rail.data;
        break;
    case VSR_IO_OP_STREAM_END:
        out->rail.end = *(const struct vsr_io_stream_end *)op->op.data;
        out->op.op.data = &out->rail.end;
        break;
    case VSR_IO_OP_STREAM_WRITTEN:
        out->rail.written = *(const struct vsr_io_stream_written *)op->op.data;
        out->op.op.data = &out->rail.written;
        break;
    case VSR_IO_OP_LINK_WANTED:
        out->rail.wanted = *(const struct vsr_io_link_wanted *)op->op.data;
        out->op.op.data = &out->rail.wanted;
        break;
    case VSR_IO_OP_STATUS:
        out->rail.status = *(const struct vsr_status *)op->op.data;
        out->op.op.data = &out->rail.status;
        break;
    default:
        break;
    }
}

static void app_op(struct iow_node *n, const struct vsr_io_op *op)
{
    struct iow_app *app;

    switch (op->kind) {
    case VSR_IO_OP_CORE:
        app = app_of(n, op->replica);
        CHECK(app != NULL);
        app_core_op(app, &op->op);
        return;
    case VSR_IO_OP_STATUS:
        app = app_of(n, op->replica);
        CHECK(app != NULL);
        app->status = *(const struct vsr_status *)op->op.data;
        app->statuses++;
        if (app->status.state == VSR_STATE_STOPPED) {
            app->stopped_statuses++;
        }
        if (app->status.failure.code != VSR_FAILURE_NONE &&
            app->failed_ns == 0) {
            app->failed_ns = iow_now();
        }
        return;
    case VSR_IO_OP_STREAM_SERVE:
        rail_serve(n, op->op.id, op->op.data);
        return;
    case VSR_IO_OP_STREAM_DATA:
        rail_data(n, op->op.id, op->op.data);
        return;
    case VSR_IO_OP_STREAM_END:
        rail_end(n, op->op.data);
        return;
    case VSR_IO_OP_STREAM_WRITTEN: {
        const struct vsr_io_stream_written *w = op->op.data;
        struct iow_sstream *s = iow_sstream_find(n, w->stream);

        n->stream_written++;
        CHECK(s != NULL);
        CHECK(w->write == (uint64_t)s->written + 1u);
        s->written++;
        return;
    }
    case VSR_IO_OP_LINK_WANTED:
        n->links_wanted++;
        return;
    case VSR_IO_OP_HANDSHAKE:
    default:
        fprintf(stderr, "unexpected rail op kind %u\n", op->kind);
        CHECK(false);
    }
}

/* -------------------------------------------------------------------------
 * The loop
 * ---------------------------------------------------------------------- */

/* Submits the pending events in order; AGAIN keeps the rest for the next
 * poll, anything else is a harness failure. */
static void submit_pending(struct iow_node *n)
{
    uint32_t consumed = 0;
    int rc;

    if (n->pending_count == 0) {
        return;
    }
    rc = PURE(n, vsr_io_submit(n->io, n->pending, n->pending_count, &consumed));
    if (rc != VSR_OK && rc != VSR_AGAIN) {
        fprintf(stderr,
                "node %u: submit refused event %u (kind %u type %u): %d\n",
                n->index, consumed, n->pending[consumed].kind,
                n->pending[consumed].event.type, rc);
        CHECK(false);
    }
    memmove(n->pending, n->pending + consumed,
            (size_t)(n->pending_count - consumed) * sizeof(n->pending[0]));
    n->pending_count -= consumed;
}

static void foreign_complete(struct iow_node *n, const struct vsr_io_cqe *cqe)
{
    n->foreign++;
    if (cqe->user_data == n->foreign_user_data) {
        n->foreign_seen = true;
        n->foreign_result = cqe->result;
    }
}

void iow_iterate(struct iow_node *n)
{
    struct vsr_io_cqe cqes[CQES];
    struct vsr_io_sqe sqes[BATCH];
    struct vsr_io_op ops[OPS_MAX];
    static struct op_copy copies[OPS_MAX];
    uint32_t capacity =
        n->options.limits.ops < OPS_MAX ? n->options.limits.ops : OPS_MAX;
    uint64_t now = n->ex.ops->now(n->ex.ctx);
    uint32_t reaped = n->ex.ops->reap(n->ex.ctx, cqes, CQES);
    uint32_t count = 0;
    uint64_t deadline = 0;
    uint32_t rounds = 0;
    bool busy = reaped > 0;

    if (iow.trace) {
        fprintf(stderr, "iterate node %u at %" PRIu64 " reaped %u\n", n->index,
                now, reaped);
    }
    for (uint32_t i = 0; i < reaped; ++i) {
        if (VSR_IO_OWNER(cqes[i].user_data) == IOW_OWNER) {
            CHECK(PURE(n, vsr_io_complete(n->io, &cqes[i], 1)) == VSR_OK);
        } else {
            foreign_complete(n, &cqes[i]);
        }
    }
    for (;;) {
        uint32_t k = 0;
        uint32_t flags = 0;
        uint32_t before;
        bool pumped;

        CHECK(PURE(n,
                   vsr_io_poll(n->io, now, ops, n->hold_ops != 0 ? 0 : capacity,
                               &k, &flags)) == VSR_OK);
        for (uint32_t i = 0; i < k; ++i) {
            copy_op(&copies[i], &ops[i]);
        }
        for (uint32_t i = 0; i < k; ++i) {
            app_op(n, &copies[i].op);
        }
        before = n->pending_count;
        submit_pending(n);
        pumped = pump_sources(n);
        if (k > 0 || pumped) {
            busy = true;
        }
        if (k == 0 && n->pending_count == before && !pumped &&
            ((flags & VSR_IO_POLL_MORE) == 0 || n->hold_ops != 0)) {
            break;
        }
        /* The engine must not stay runnable forever in one iteration. */
        if (++rounds > 9990) {
            fprintf(stderr,
                    "node %u round %u: ops %u flags %u pending %u -> %u "
                    "pumped %d\n",
                    n->index, rounds, k, flags, before, n->pending_count,
                    pumped);
            for (uint32_t i = 0; i < k; ++i) {
                fprintf(stderr, "  op kind %u type %u id %" PRIx64 "\n",
                        copies[i].op.kind, copies[i].op.op.type,
                        copies[i].op.op.id);
            }
            if (rounds == 9999) {
                iow_dump_node(n);
            }
        }
        CHECK(rounds < 10000);
    }
    CHECK(PURE(n, vsr_io_prepare(n->io, now, sqes, BATCH, &count, &deadline)) ==
          VSR_OK);
    if (n->pending_count > 0) {
        deadline = now;
    }
    if (count > 0) {
        busy = true;
    }
    if (iow.trace) {
        fprintf(stderr, "  node %u prepared %u deadline %" PRIu64 "\n",
                n->index, count, deadline);
    }
    /* An iteration that did nothing and asks to run again at once (its
     * deadline already past) is a busy loop in the code under test when it
     * repeats: a real loop would spin at full CPU. */
    if (!busy && n->pending_count == 0 && deadline <= now) {
        if (++n->spins >= 1000) {
            fprintf(stderr,
                    "node %u spins: nothing to do, deadline %" PRIu64
                    " at %" PRIu64 "\n",
                    n->index, deadline, now);
            iow_dump_node(n);
            CHECK(false);
        }
    } else {
        n->spins = 0;
    }
    n->deadline = deadline;
    n->busy = busy;
    if (iow.backend == IOW_SIM) {
        CHECK(n->ex.ops->submit_and_wait(n->ex.ctx, sqes, count, 1, 0,
                                         deadline) == 0);
    } else {
        int rc = n->ex.ops->submit_and_wait(n->ex.ctx, sqes, count, 0, 0, 0);

        if (rc != 0) {
            fprintf(stderr, "node %u: submit_and_wait %d\n", n->index, rc);
            CHECK(false);
        }
    }
}

static bool node_ready(const struct iow_node *n)
{
    if (!n->open) {
        return false;
    }
    if ((!n->hook.holding || n->hook.reverse) && n->hook.held_count > 0) {
        return true;
    }
    if (n->faulty != NULL && n->faulty->held_count > 0) {
        return true;
    }
    return vsr_sim_ready(iow.sim, n->index) != 0;
}

/* Rings: nothing moved in a round, so wait for a completion on any ring
 * or the earliest deadline, at most a millisecond. */
static void uring_idle(void)
{
    struct pollfd fds[IOW_NODES];
    uint32_t count = 0;
    uint64_t now = monotonic();
    uint64_t wait = IOW_MS;

    for (uint32_t i = 0; i < iow.nodes; ++i) {
        const struct iow_node *n = &iow.node[i];

        if (!n->open) {
            continue;
        }
        if (n->busy || (n->faulty != NULL && n->faulty->held_count > 0) ||
            ((!n->hook.holding || n->hook.reverse) && n->hook.held_count > 0)) {
            return;
        }
        if (n->deadline <= now) {
            return;
        }
        if (n->deadline - now < wait) {
            wait = n->deadline - now;
        }
        fds[count].fd = vsr_io_uring_fd(&n->base);
        fds[count].events = POLLIN;
        fds[count].revents = 0;
        count++;
    }
    {
        struct timespec ts;

        ts.tv_sec = 0;
        ts.tv_nsec = (long)wait;
        (void)ppoll(fds, count, &ts, NULL);
    }
}

bool iow_round(void)
{
    bool ran = false;

    iow.rounds++;
    for (uint32_t i = 0; i < iow.nodes; ++i) {
        struct iow_node *n = &iow.node[i];

        if (iow.backend == IOW_SIM ? node_ready(n) : n->open) {
            iow_iterate(n);
            ran = true;
        }
    }
    if (iow.backend == IOW_SIM) {
        /* vsr_sim_advance moves the clock whenever no event is due, even
         * past a node that became ready during its own iteration (a record
         * completing at submission): run those first. A node ready only
         * because the deadline it asked for has passed, after an iteration
         * that did nothing, is a loop that would spin while real time
         * passes: the clock moves (the spin counter watches it). */
        for (uint32_t i = 0; i < iow.nodes; ++i) {
            const struct iow_node *n = &iow.node[i];

            if (node_ready(n) && (n->busy || n->spins == 0)) {
                return true;
            }
        }
        return vsr_sim_advance(iow.sim) >= 0 || ran;
    }
    uring_idle();
    return true;
}

/* The simulation moves its clock to the next pending event, which may lie
 * far beyond the stretch a test asked for: a caller TIMEOUT record on the
 * first open node stops the clock at the stretch's end. */
static void sim_stop_at(uint64_t ns)
{
    static uint64_t counter = 1;

    for (uint32_t i = 0; i < iow.nodes; ++i) {
        struct iow_node *n = &iow.node[i];
        struct vsr_io_sqe sqe;

        if (!n->open) {
            continue;
        }
        memset(&sqe, 0, sizeof(sqe));
        sqe.opcode = VSR_IO_SQE_TIMEOUT;
        sqe.fd = -1;
        sqe.offset = ns;
        sqe.user_data =
            VSR_IO_USER_DATA(IOW_APP_OWNER, (UINT64_C(0x71) << 40) | counter++);
        CHECK(n->ex.ops->submit_and_wait(n->ex.ctx, &sqe, 1, 0, 0, 0) == 0);
        return;
    }
}

bool iow_run_until(iow_predicate until, void *ctx, uint64_t ns)
{
    uint64_t end = iow_now() + ns;

    if (iow.backend == IOW_SIM) {
        sim_stop_at(ns);
    }
    for (uint64_t round = 0; round < UINT64_C(20000000); ++round) {
        bool alive;

        if (until(ctx)) {
            return true;
        }
        alive = iow_round();
        if (until(ctx)) {
            return true;
        }
        if (iow_now() >= end || !alive) {
            return until(ctx);
        }
    }
    CHECK(false);
    return false;
}

void iow_run_for(uint64_t ns)
{
    uint64_t end = iow_now() + ns;

    if (iow.backend == IOW_SIM) {
        sim_stop_at(ns);
    }
    for (uint64_t round = 0; round < UINT64_C(20000000); ++round) {
        if (iow_now() >= end || !iow_round()) {
            return;
        }
    }
    CHECK(false);
}

/* -------------------------------------------------------------------------
 * The caller's own records: directories and the pattern file
 * ---------------------------------------------------------------------- */

/* One record of the caller's through the node's executor, completions of
 * the engine's reaped meanwhile going to the engine (it only queues). */
static int32_t caller_record(struct iow_node *n, struct vsr_io_sqe *sqe)
{
    static uint64_t counter = 1;

    sqe->user_data = VSR_IO_USER_DATA(IOW_APP_OWNER, counter++);
    n->foreign_user_data = sqe->user_data;
    n->foreign_seen = false;
    CHECK(n->ex.ops->submit_and_wait(n->ex.ctx, sqe, 1, 0, 0, 0) == 0);
    for (uint32_t i = 0; i < 1000000 && !n->foreign_seen; ++i) {
        struct vsr_io_cqe cqes[CQES];
        uint32_t reaped = n->ex.ops->reap(n->ex.ctx, cqes, CQES);

        for (uint32_t j = 0; j < reaped; ++j) {
            if (VSR_IO_OWNER(cqes[j].user_data) == IOW_OWNER) {
                CHECK(PURE(n, vsr_io_complete(n->io, &cqes[j], 1)) == VSR_OK);
            } else {
                foreign_complete(n, &cqes[j]);
            }
        }
        if (n->foreign_seen) {
            break;
        }
        if (iow.backend == IOW_SIM) {
            (void)vsr_sim_advance(iow.sim);
        } else {
            CHECK(n->ex.ops->submit_and_wait(n->ex.ctx, NULL, 0, 1, 0,
                                             monotonic() + IOW_MS) == 0);
        }
    }
    CHECK(n->foreign_seen);
    return n->foreign_result;
}

void iow_make_directory(struct iow_node *n, const char *path)
{
    if (iow.backend == IOW_URING) {
        CHECK(mkdir(path, 0755) == 0 || errno == EEXIST);
        return;
    }
    {
        struct vsr_io_sqe sqe;
        int32_t rc;

        memset(&sqe, 0, sizeof(sqe));
        sqe.opcode = VSR_IO_SQE_MKDIRAT;
        sqe.fd = VSR_SIM_ROOT;
        sqe.addr = path;
        sqe.length = 0755;
        rc = caller_record(n, &sqe);
        CHECK(rc == 0 || rc == -EEXIST);
    }
}

void iow_caller_file(struct iow_node *n)
{
    struct vsr_io_sqe sqe;
    char path[300];
    int written;

    if (n->file_ready) {
        return;
    }
    if (iow.backend == IOW_URING) {
        written = snprintf(path, sizeof(path), "%s/n%u-file", iow.directory,
                           n->index);
    } else {
        written = snprintf(path, sizeof(path), "caller-file");
    }
    CHECK(written > 0 && (size_t)written < sizeof(path));
    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode = VSR_IO_SQE_OPENAT;
    sqe.flags = VSR_IO_SQE_DIRECT;
    sqe.fd = AT_FDCWD; /* VSR_SIM_ROOT in a simulation: the same value. */
    sqe.fd2 = (int32_t)IOW_CALLER_SLOT;
    sqe.addr = path;
    sqe.op_flags = O_RDWR | O_CREAT;
    sqe.length = 0644;
    CHECK(caller_record(n, &sqe) == (int32_t)IOW_CALLER_SLOT);
    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode = VSR_IO_SQE_WRITE;
    sqe.flags = VSR_IO_SQE_FIXED_FILE;
    sqe.fd = (int32_t)IOW_CALLER_SLOT;
    sqe.addr = file_bytes;
    sqe.length = IOW_FILE_BYTES;
    CHECK(caller_record(n, &sqe) == (int32_t)IOW_FILE_BYTES);
    n->file_ready = true;
}

/* -------------------------------------------------------------------------
 * Replicas
 * ---------------------------------------------------------------------- */

void iow_replica_options(struct iow_app *app, struct vsr_id cluster,
                         uint64_t replica, uint32_t members, uint32_t mode,
                         uint32_t generation)
{
    struct vsr_options *core = &app->options.core;
    char suffix[16] = "";
    int written;

    CHECK(members <= IOW_MEMBERS);
    memset(&app->options, 0, sizeof(app->options));
    for (uint32_t i = 0; i < members; ++i) {
        app->members[i].id = i + 1;
        app->members[i].role = VSR_MEMBER_FULL;
        app->members[i].reserved = 0;
    }
    app->seed.epoch = 0;
    app->seed.members = app->members;
    app->seed.count = members;
    app->seed.faults = (members - 1) / 2;
    core->cluster = cluster;
    core->incarnation.hi = 0x1Cu;
    core->incarnation.lo = iow.incarnation++;
    core->replica = replica;
    core->seed = &app->seed;
    core->limits = iow.limits;
    core->heartbeat_ns = iow.heartbeat_ns;
    core->view_timeout_ns = iow.view_timeout_ns;
    core->retry_ns = iow.retry_ns;
    core->transfer_timeout_ns = iow.transfer_timeout_ns;
    core->checkpoint_interval = iow.checkpoint_interval;
    core->start_mode = mode;
    core->durability = iow.durability;
    core->join_role = mode == VSR_START_JOIN ? VSR_MEMBER_FULL : 0;
    store_resize();
    app->options.store = iow.store;
    if (generation > 0) {
        written = snprintf(suffix, sizeof(suffix), "-%u", generation);
        CHECK(written > 0 && (size_t)written < sizeof(suffix));
    }
    if (iow.backend == IOW_URING) {
        written = snprintf(app->path, sizeof(app->path),
                           "%s/n%u-c%" PRIu64 "-r%" PRIu64 "%s", iow.directory,
                           app->node->index, cluster.lo, replica, suffix);
    } else {
        written = snprintf(app->path, sizeof(app->path),
                           "c%" PRIu64 "-r%" PRIu64 "%s", cluster.lo, replica,
                           suffix);
    }
    CHECK(written > 0 && (size_t)written < sizeof(app->path));
    app->options.path = app->path;
}

struct iow_app *iow_attach_with(struct iow_node *n, uint32_t r)
{
    struct iow_app *app = &n->apps[r];
    struct vsr_io_replica_layout layout;
    struct vsr_io_region metadata;
    struct vsr_io_region tail;
    int rc;

    CHECK(n->open && !app->attached);
    memset(app->leases, 0, sizeof(app->leases));
    app->statuses = app->stopped_statuses = 0;
    memset(&app->status, 0, sizeof(app->status));
    app->replies = app->replies_ok = app->replies_executed = 0;
    memset(app->reply_statuses, 0, sizeof(app->reply_statuses));
    app->applies = app->applied_entries = app->releases = 0;
    app->captures = app->fetches = app->fetches_ok = app->installs = 0;
    app->syncs = app->drops = app->read_ready = 0;
    app->sync_ns = app->failed_ns = 0;
    app->hold_apply = false;
    app->held_apply = 0;
    app->digest = 0;
    app->applied_op = 0;
    app->commands = 0;
    app->check_history = true;
    memset(&app->fetch, 0, sizeof(app->fetch));
    if (app->options.core.start_mode != VSR_START_RECOVER) {
        iow_make_directory(n, app->path);
    }
    CHECK(vsr_io_replica_layout(n->io, &app->options, &layout) == VSR_OK);
    CHECK(layout.metadata.size <= REPLICA_BYTES);
    CHECK(layout.metadata.alignment <= 4096);
    CHECK(layout.tail.size <= TAIL_BYTES && layout.tail.alignment <= 4096);
    metadata.base = bank(BANK_REPLICA, n->index, n->bank, r);
    metadata.size = REPLICA_BYTES;
    tail.base = bank(BANK_TAIL, n->index, n->bank, r);
    tail.size = layout.tail.size;
    rc = vsr_io_attach(n->io, &app->options, &metadata, &tail, &app->replica);
    if (rc != VSR_OK) {
        fprintf(stderr, "attach: %d\n", rc);
        CHECK(false);
    }
    CHECK(app->replica != NULL);
    app->attached = true;
    /* Attached from outside the loop: the caller's loop runs again. */
    vsr_io_wake(n->io);
    return app;
}

struct iow_app *iow_attach(struct iow_node *n, uint32_t r,
                           struct vsr_id cluster, uint64_t replica,
                           uint32_t members, uint32_t mode)
{
    struct iow_app *app = &n->apps[r];

    app->node = n;
    app->index = r;
    if (app->next_route == 0) {
        app->next_route = 1;
    }
    iow_replica_options(app, cluster, replica, members, mode, 0);
    return iow_attach_with(n, r);
}

bool iow_app_normal(void *ctx)
{
    const struct iow_app *app = ctx;

    return app->attached && app->statuses > 0 &&
           app->status.state == VSR_STATE_NORMAL;
}

bool iow_app_stopped(void *ctx)
{
    const struct iow_app *app = ctx;
    struct vsr_status core;

    if (!app->attached) {
        return false;
    }
    vsr_io_replica_status(app->replica, &core, NULL);
    return app->stopped_statuses > 0 && core.state == VSR_STATE_STOPPED;
}

void iow_stop(struct iow_app *app)
{
    iow_core_event(app, VSR_EVENT_STOP, 0, 0, NULL, 0);
    if (!iow_run_until(iow_app_stopped, app, 20000 * IOW_MS)) {
        iow_dump_node(app->node);
        CHECK(false);
    }
}

void iow_complete_held_apply(struct iow_app *app)
{
    uint64_t id = app->held_apply;

    CHECK(id != 0 && app->held_data != NULL);
    app->held_apply = 0;
    complete_apply(app, id, app->held_data);
    app->held_data = NULL;
}

static bool detached(void *ctx)
{
    struct iow_app *app = ctx;

    if (vsr_io_detach(app->replica) != VSR_OK) {
        return false;
    }
    /* The detach queued slot clears for the next prepare. */
    vsr_io_wake(app->node->io);
    return true;
}

void iow_detach(struct iow_app *app)
{
    if (!iow_run_until(detached, app, 20000 * IOW_MS)) {
        iow_dump_node(app->node);
        CHECK(false);
    }
    app->attached = false;
    CHECK(iow_leases_out(app) == 0);
    CHECK(vsr_deinit(vsr_io_replica_core(app->replica)) == VSR_OK);
}

/* -------------------------------------------------------------------------
 * Groups
 * ---------------------------------------------------------------------- */

bool iow_group_normal(void *ctx)
{
    const struct iow_group *g = ctx;
    uint64_t view = 0;
    uint64_t epoch = 0;
    bool first = true;

    for (uint32_t i = 0; i < g->count; ++i) {
        const struct iow_app *app = g->apps[i];

        if (app == NULL || !app->attached) {
            continue;
        }
        if (app->statuses == 0 || app->status.state != VSR_STATE_NORMAL ||
            app->status.primary == 0) {
            return false;
        }
        if (!first &&
            (app->status.view != view || app->status.epoch != epoch)) {
            return false;
        }
        view = app->status.view;
        epoch = app->status.epoch;
        first = false;
    }
    return true;
}

struct iow_app *iow_group_primary(const struct iow_group *g)
{
    for (uint32_t i = 0; i < g->count; ++i) {
        const struct iow_app *app = g->apps[i];

        if (app != NULL && app->attached &&
            app->status.primary == app->options.core.replica &&
            app->status.state == VSR_STATE_NORMAL) {
            return g->apps[i];
        }
    }
    return NULL;
}

struct iow_group iow_group_open(uint32_t count, struct vsr_id cluster)
{
    struct iow_group g;

    memset(&g, 0, sizeof(g));
    for (uint32_t i = 0; i < count; ++i) {
        iow_node_open(i);
    }
    iow_mesh(count, cluster);
    g.count = count;
    for (uint32_t i = 0; i < count; ++i) {
        g.apps[i] =
            iow_attach(&iow.node[i], 0, cluster, i + 1, count, VSR_START_NEW);
    }
    CHECK(iow_run_until(iow_group_normal, &g, 20000 * IOW_MS));
    return g;
}

void iow_group_close(struct iow_group *g)
{
    for (uint32_t i = 0; i < g->count; ++i) {
        if (g->apps[i] != NULL && g->apps[i]->attached) {
            iow_core_event(g->apps[i], VSR_EVENT_STOP, 0, 0, NULL, 0);
        }
    }
    for (uint32_t i = 0; i < g->count; ++i) {
        if (g->apps[i] != NULL && g->apps[i]->attached) {
            if (!iow_run_until(iow_app_stopped, g->apps[i], 20000 * IOW_MS)) {
                iow_dump_node(g->apps[i]->node);
                CHECK(false);
            }
        }
    }
    for (uint32_t i = 0; i < g->count; ++i) {
        if (g->apps[i] != NULL && g->apps[i]->attached) {
            iow_detach(g->apps[i]);
        }
    }
    for (uint32_t i = 0; i < iow.nodes; ++i) {
        if (iow.node[i].open) {
            iow_close_io(&iow.node[i]);
        }
    }
    for (uint32_t i = 0; i < iow.nodes; ++i) {
        struct iow_node *n = &iow.node[i];

        if (!n->open) {
            continue;
        }
        if (!iow_run_until(iow_node_closed, n, 20000 * IOW_MS)) {
            iow_dump_node(n);
            CHECK(false);
        }
        CHECK(vsr_io_deinit(n->io) == VSR_OK);
        n->open = false;
        if (iow.backend == IOW_URING) {
            ring_close(n);
        }
    }
}

struct reply_wait {
    struct iow_app *app;
    uint64_t replies;
};

static bool replies_reached(void *ctx)
{
    const struct reply_wait *want = ctx;

    return !want->app->attached || want->app->replies >= want->replies;
}

void iow_group_commit(struct iow_group *g, uint32_t count)
{
    for (uint32_t i = 0; i < count; ++i) {
        uint64_t client = 1 + i % 4;
        uint64_t number = ++client_numbers[client];
        bool done = false;

        for (uint32_t attempt = 0; attempt < 100 && !done; ++attempt) {
            struct iow_app *primary;
            struct reply_wait want;

            if (!iow_run_until(iow_group_normal, g, 20000 * IOW_MS)) {
                for (uint32_t k = 0; k < iow.nodes; ++k) {
                    if (iow.node[k].open) {
                        iow_dump_node(&iow.node[k]);
                    }
                }
                CHECK(false);
            }
            primary = iow_group_primary(g);
            if (primary == NULL) {
                iow_run_for(10 * IOW_MS);
                continue;
            }
            want.app = primary;
            want.replies = primary->replies + 1;
            (void)iow_request(primary, client, number);
            if (!iow_run_until(replies_reached, &want, 5000 * IOW_MS)) {
                continue;
            }
            done =
                primary->attached && primary->last_reply_status == VSR_REPLY_OK;
        }
        if (!done) {
            fprintf(stderr,
                    "no OK reply for client %" PRIu64 " number %" PRIu64 "\n",
                    client, number);
            for (uint32_t k = 0; k < iow.nodes; ++k) {
                if (iow.node[k].open) {
                    iow_dump_node(&iow.node[k]);
                }
            }
            CHECK(false);
        }
    }
}

uint64_t iow_last_number(uint64_t client)
{
    CHECK(client < 64);
    return client_numbers[client];
}

bool iow_group_applied(const struct iow_group *g, uint64_t op)
{
    for (uint32_t i = 0; i < g->count; ++i) {
        const struct iow_app *app = g->apps[i];

        if (app != NULL && app->attached && app->applied_op < op) {
            return false;
        }
    }
    return true;
}

/* -------------------------------------------------------------------------
 * Checks and dumps
 * ---------------------------------------------------------------------- */

void iow_check_idle_replica(const struct iow_app *app)
{
    struct vsr_status core;

    vsr_io_replica_status(app->replica, &core, NULL);
    CHECK(core.outstanding_ops == 0);
    CHECK(core.outstanding_leases == 0);
}

void iow_dump_node(const struct iow_node *n)
{
    struct vsr_io_stats stats;

    if (n->io == NULL) {
        fprintf(stderr, "node %u: no engine\n", n->index);
        return;
    }
    vsr_io_get_stats(n->io, &stats);
    fprintf(stderr,
            "node %u: open %d replicas %u links %u pending %u streams %u "
            "slabs_free %u closed %u failure %d pending events %u\n",
            n->index, n->open, stats.replicas, stats.links, stats.links_pending,
            stats.streams, stats.slabs_free, stats.closed, stats.failure,
            n->pending_count);
    fprintf(stderr, "  hook: rule hits %u held %u marks %u\n", iow_rule_hits(n),
            n->hook.held_count, n->hook.marks_count);
    for (uint32_t j = 0; j < iow.nodes; ++j) {
        struct vsr_io_node_status st;

        if (j != n->index && vsr_io_node_status(n->io, j + 1, &st) == VSR_OK) {
            fprintf(
                stderr,
                "  peer node %u: state %u links %u error %d next dial %" PRIu64
                "\n",
                j + 1, st.state, st.links, st.last_error, st.next_dial_ns);
        }
    }
    fprintf(stderr,
            "  engine: file slots next %u free %u of %u; slots free %u/%u; "
            "forwarded %u\n",
            n->io->file_slot_next, n->io->file_slots_free_count,
            n->io->options.limits.file_slots, n->io->slots.free_count,
            n->io->slots.count, n->io->forwarded_count);
    {
        const struct vsr_io *io = n->io;

        fprintf(stderr, "  engine: clears %u; listener slots",
                io->clears_count);
        for (uint32_t i = 0; i < io->options.listen_count; ++i) {
            fprintf(stderr, " state %u", io->links.listener_table[i].state);
        }
        fprintf(stderr, "\n");
        for (uint32_t i = 0; i < io->links.links_count; ++i) {
            const struct vsr_io_link *link = &io->links.links[i];

            if (link->state != VSR_IO_LINK_FREE) {
                fprintf(stderr,
                        "  link %u: state %u stage %u node %" PRIu64
                        " fd %d raw %d install %u\n",
                        i, link->state, link->stage, link->node, link->fd,
                        link->raw_fd, link->install_slot);
            }
        }
        for (uint32_t r = 0; r < io->options.limits.replicas; ++r) {
            const struct vsr_io_replica *rep = &io->replicas[r];

            if (rep->state == VSR_IO_REPLICA_FREE) {
                continue;
            }
            fprintf(stderr, "  replica %u: log slot %u dir slot %u serves", r,
                    rep->store.file_slot, rep->snapshots.dir_slot);
            for (uint32_t i = 0; i < io->options.limits.streams; ++i) {
                fprintf(stderr, " %u/%u", rep->snapshots.serves[i].state,
                        rep->snapshots.serves[i].slot);
            }
            fprintf(stderr, "\n");
        }
    }
    for (uint32_t r = 0; r < IOW_REPLICAS; ++r) {
        const struct iow_app *app = &n->apps[r];
        struct vsr_status core;
        struct vsr_io_store_status store;

        if (!app->attached) {
            continue;
        }
        vsr_io_replica_status(app->replica, &core, &store);
        {
            const struct vsr_io_replica *rep = app->replica;
            const struct vsr_io_snapshots *s = &rep->snapshots;

            fprintf(
                stderr,
                "  engine replica %u: state %u completions %u deferred %u "
                "priority %u events %u messages %u leases free %u/%u "
                "status statuses %u stopped %u replies %" PRIu64
                " last %u app status primary %" PRIu64 " view %" PRIu64 "\n",
                rep->index, rep->state, rep->completions_count,
                rep->deferred_count, rep->priority_count, rep->events_count,
                rep->messages_count, rep->leases_free, rep->regions_count,
                app->statuses, app->stopped_statuses, app->replies,
                app->last_reply_status, app->status.primary, app->status.view);
            for (uint32_t e = 0; e < s->count; ++e) {
                const struct vsr_io_snapshot *entry = &s->entries[e];

                if (entry->state == 0) {
                    continue;
                }
                fprintf(stderr,
                        "    snapshot %u: state %u op %" PRIu64
                        " type %u fwd %" PRIu64 " caller %d library %d slot %d "
                        "job %u step %u readers %u\n",
                        e, entry->state, entry->op, entry->op_type,
                        entry->forwarded, entry->caller_status,
                        entry->library_status, entry->file_slot, entry->job,
                        entry->step, entry->readers);
            }
        }
        {
            const struct vsr *v = app->replica->core;

            for (uint32_t i = 0; i < v->options.limits.operations; ++i) {
                const struct vsr_operation *op = &v->operations[i];

                if (op->state != 0) {
                    fprintf(stderr,
                            "    core op %u: state %u type %u id %" PRIu64
                            " arg %" PRIu64 " tag %" PRIu64 "\n",
                            i, op->state, op->output.type, op->output.id,
                            op->output.arg, op->tag);
                }
            }
        }
        fprintf(stderr,
                "  replica %" PRIu64 ": state %u view %" PRIu64
                " primary %" PRIu64 " committed %" PRIu64 " applied %" PRIu64
                " checkpoint %" PRIu64
                " ops %u leases %u failure %u/%d op %" PRIu64
                " type %u store readable %" PRIu64 " written %" PRIu64
                " durable %" PRIu64 " error %d\n",
                app->options.core.replica, core.state, core.view, core.primary,
                core.committed, core.applied, core.checkpoint_op,
                core.outstanding_ops, core.outstanding_leases,
                core.failure.code, core.failure.status, core.failure.operation,
                core.failure.operation_type, store.readable, store.written,
                store.durable, store.error);
    }
}
