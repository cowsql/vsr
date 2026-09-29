#include "config.h"

#define _GNU_SOURCE 1

#include "lib/check.h"
#include "lib/faulty_executor.h"
#include "lib/random.h"
#include "vsr-sim.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>

/*
 * The fault-injecting wrapper executor (tests/lib/faulty_executor.c) over a
 * two-node simulated world: transparency with every rate zero (the CQE
 * sequence equals the bare simulation's), each fault alone at 1,000,000 ppm
 * on every eligible completion with its counter, and a seeded random walk
 * with mixed rates checking exactly-once delivery, zero-copy result-then-
 * NOTIF order, an intact byte stream and a trace that repeats per seed.
 * Then the review cases, one test each, every one naming the property it
 * holds the wrapper to. `faulty_executor [SEED [TEST]]` runs one test.
 */

#define BLOCK 4096u
#define PORT 7u
#define TRACE_MAX 16384u
#define PARKED_MAX 64u
#define SPIN_MAX 1000000u
#define PPM UINT32_C(1000000)
#define MESSAGE "hello world"
#define MESSAGE_BYTES 11u

static uint64_t seed = 1;

struct trace_entry {
    uint32_t node;
    uint32_t reserved;
    struct vsr_io_cqe cqe;
};

struct trace {
    uint32_t count;
    struct trace_entry entries[TRACE_MAX];
};

struct world {
    struct vsr_sim *sim;
    struct vsr_io_executor ex[2];
    struct faulty_executor *faulty[2]; /* NULL: bare simulation. */
    struct trace *trace;
    struct vsr_io_cqe parked[2][PARKED_MAX];
    uint32_t parked_count[2];
};

static struct faulty_executor faulty_storage[2];
static struct trace traces[2];

/* ------------------------------------------------------------------------
 * World and helpers
 * --------------------------------------------------------------------- */

static void open_world(struct world *w, struct trace *trace,
                       const struct faulty_executor_options *options)
{
    struct vsr_sim_options sim_options;

    memset(w, 0, sizeof(*w));
    memset(&sim_options, 0, sizeof(sim_options));
    sim_options.seed = seed;
    sim_options.nodes = 2;
    sim_options.block_bytes = BLOCK;
    sim_options.file_slots = 8;
    sim_options.buffer_regions = 4;
    CHECK(vsr_sim_create(&sim_options, &w->sim) == 0);
    for (uint32_t node = 0; node < 2; ++node) {
        struct vsr_io_executor bare = vsr_sim_executor(w->sim, node);

        CHECK(bare.ops != NULL);
        w->ex[node] = bare;
        if (options != NULL) {
            struct faulty_executor_options own = *options;

            own.seed = options->seed + node;
            w->faulty[node] = &faulty_storage[node];
            faulty_executor_init(w->faulty[node], &bare, &own);
            w->ex[node] = faulty_executor_handle(w->faulty[node]);
        }
    }
    trace->count = 0;
    w->trace = trace;
}

static void close_world(struct world *w)
{
    vsr_sim_destroy(w->sim);
    w->sim = NULL;
}

static struct faulty_executor_stats stats_of(const struct world *w)
{
    struct faulty_executor_stats sum;

    memset(&sum, 0, sizeof(sum));
    for (uint32_t node = 0; node < 2; ++node) {
        struct faulty_executor_stats one;

        if (w->faulty[node] == NULL) {
            continue;
        }
        faulty_executor_stats(w->faulty[node], &one);
        sum.eio += one.eio;
        sum.shortened += one.shortened;
        sum.delayed += one.delayed;
        sum.cancelled += one.cancelled;
    }
    return sum;
}

static struct vsr_io_sqe make_sqe(uint8_t opcode, int32_t fd,
                                  uint64_t user_data)
{
    struct vsr_io_sqe sqe;

    memset(&sqe, 0, sizeof(sqe));
    sqe.opcode = opcode;
    sqe.fd = fd;
    sqe.user_data = user_data;
    return sqe;
}

static void submit(struct world *w, uint32_t node,
                   const struct vsr_io_sqe *sqes, uint32_t count)
{
    struct vsr_io_executor ex = w->ex[node];

    CHECK(ex.ops->submit_and_wait(ex.ctx, sqes, count, 0, 0, 0) == 0);
}

static uint32_t reap(struct world *w, uint32_t node, struct vsr_io_cqe *cqes,
                     uint32_t capacity)
{
    struct vsr_io_executor ex = w->ex[node];
    uint32_t count = ex.ops->reap(ex.ctx, cqes, capacity);

    CHECK(count <= capacity);
    for (uint32_t i = 0; i < count; ++i) {
        struct trace_entry *entry;

        CHECK(w->trace->count < TRACE_MAX);
        entry = &w->trace->entries[w->trace->count++];
        CHECK(cqes[i].user_data != FAULTY_EXECUTOR_USER_DATA);
        memset(entry, 0, sizeof(*entry));
        entry->node = node;
        entry->cqe = cqes[i];
    }
    return count;
}

/* The node's next completion for user_data; others are parked for later
 * waits. Advances the world while nothing is reapable. */
static struct vsr_io_cqe wait_for(struct world *w, uint32_t node,
                                  uint64_t user_data)
{
    for (uint32_t i = 0; i < w->parked_count[node]; ++i) {
        if (w->parked[node][i].user_data == user_data) {
            struct vsr_io_cqe found = w->parked[node][i];

            memmove(&w->parked[node][i], &w->parked[node][i + 1],
                    sizeof(found) * (w->parked_count[node] - i - 1));
            --w->parked_count[node];
            return found;
        }
    }
    for (uint32_t spin = 0; spin < SPIN_MAX; ++spin) {
        struct vsr_io_cqe cqes[8];
        uint32_t count = reap(w, node, cqes, 8);
        struct vsr_io_cqe found;
        bool matched = false;

        memset(&found, 0, sizeof(found));
        for (uint32_t i = 0; i < count; ++i) {
            if (!matched && cqes[i].user_data == user_data) {
                found = cqes[i];
                matched = true;
            } else {
                CHECK(w->parked_count[node] < PARKED_MAX);
                w->parked[node][w->parked_count[node]++] = cqes[i];
            }
        }
        if (matched) {
            return found;
        }
        if (count == 0) {
            (void)vsr_sim_advance(w->sim);
        }
    }
    fprintf(stderr, "node %" PRIu32 ": no completion for %" PRIx64 "\n", node,
            user_data);
    abort();
}

static struct vsr_io_cqe run(struct world *w, uint32_t node,
                             const struct vsr_io_sqe *sqe)
{
    submit(w, node, sqe, 1);
    return wait_for(w, node, sqe->user_data);
}

/* ------------------------------------------------------------------------
 * The scripted scenario
 * --------------------------------------------------------------------- */

struct outcome {
    int32_t write;
    int32_t read;
    int32_t fsync;
    int32_t send;
    int32_t recv;
    int32_t zc_send;
    uint16_t zc_flags;
    uint16_t notif_flags;
};

static int32_t make_socket(struct world *w, uint32_t node, uint64_t user_data)
{
    struct vsr_io_sqe sqe = make_sqe(VSR_IO_SQE_SOCKET, -1, user_data);
    struct vsr_io_cqe cqe;

    sqe.length = AF_INET;
    sqe.op_flags = SOCK_STREAM;
    cqe = run(w, node, &sqe);
    CHECK(cqe.result >= 0);
    return cqe.result;
}

/* Node 0: a file written, read and synced. Node 1 listens; node 0 connects,
 * sends to a receive node 1 posted first, then sends zero-copy. */
static struct outcome scenario(struct world *w)
{
    static const char message[] = MESSAGE;
    static char read_buffer[MESSAGE_BYTES];
    static char recv_buffer[MESSAGE_BYTES];
    struct vsr_io_address address = vsr_sim_address(w->sim, 1, PORT);
    struct outcome outcome;
    struct vsr_io_sqe sqe;
    struct vsr_io_cqe cqe;
    int32_t fd;
    int32_t listener;
    int32_t client;
    int32_t server;

    memset(&outcome, 0, sizeof(outcome));
    sqe = make_sqe(VSR_IO_SQE_OPENAT, VSR_SIM_ROOT, 0x10);
    sqe.addr = "data";
    sqe.op_flags = O_CREAT | O_RDWR;
    sqe.length = 0600;
    fd = run(w, 0, &sqe).result;
    CHECK(fd >= 0);
    sqe = make_sqe(VSR_IO_SQE_WRITE, fd, 0x11);
    sqe.addr = message;
    sqe.length = MESSAGE_BYTES;
    outcome.write = run(w, 0, &sqe).result;
    sqe = make_sqe(VSR_IO_SQE_READ, fd, 0x12);
    sqe.addr = read_buffer;
    sqe.length = MESSAGE_BYTES;
    outcome.read = run(w, 0, &sqe).result;
    sqe = make_sqe(VSR_IO_SQE_FSYNC, fd, 0x13);
    outcome.fsync = run(w, 0, &sqe).result;
    sqe = make_sqe(VSR_IO_SQE_CLOSE, fd, 0x14);
    CHECK(run(w, 0, &sqe).result == 0);

    listener = make_socket(w, 1, 0x20);
    sqe = make_sqe(VSR_IO_SQE_BIND, listener, 0x21);
    sqe.addr = &address.sockaddr;
    sqe.length = address.length;
    CHECK(run(w, 1, &sqe).result == 0);
    sqe = make_sqe(VSR_IO_SQE_LISTEN, listener, 0x22);
    sqe.length = 4;
    CHECK(run(w, 1, &sqe).result == 0);
    sqe = make_sqe(VSR_IO_SQE_ACCEPT, listener, 0x23);
    submit(w, 1, &sqe, 1);
    client = make_socket(w, 0, 0x24);
    sqe = make_sqe(VSR_IO_SQE_CONNECT, client, 0x25);
    sqe.addr = &address.sockaddr;
    sqe.length = address.length;
    CHECK(run(w, 0, &sqe).result == 0);
    server = wait_for(w, 1, 0x23).result;
    CHECK(server >= 0);

    sqe = make_sqe(VSR_IO_SQE_RECV, server, 0x26);
    sqe.addr = recv_buffer;
    sqe.length = MESSAGE_BYTES;
    submit(w, 1, &sqe, 1);
    sqe = make_sqe(VSR_IO_SQE_SEND, client, 0x27);
    sqe.addr = message;
    sqe.length = MESSAGE_BYTES;
    outcome.send = run(w, 0, &sqe).result;
    outcome.recv = wait_for(w, 1, 0x26).result;
    if (outcome.recv > 0) {
        CHECK(memcmp(recv_buffer, message, (size_t)outcome.recv) == 0);
    }

    sqe = make_sqe(VSR_IO_SQE_SEND, client, 0x28);
    sqe.addr = message;
    sqe.length = MESSAGE_BYTES;
    sqe.op_flags = VSR_IO_SEND_ZERO_COPY;
    cqe = run(w, 0, &sqe);
    outcome.zc_send = cqe.result;
    outcome.zc_flags = cqe.flags;
    CHECK((cqe.flags & VSR_IO_CQE_MORE) != 0);
    cqe = wait_for(w, 0, 0x28);
    CHECK(cqe.result == 0);
    outcome.notif_flags = cqe.flags;

    sqe = make_sqe(VSR_IO_SQE_CLOSE, client, 0x29);
    CHECK(run(w, 0, &sqe).result == 0);
    sqe = make_sqe(VSR_IO_SQE_CLOSE, server, 0x2A);
    CHECK(run(w, 1, &sqe).result == 0);
    sqe = make_sqe(VSR_IO_SQE_CLOSE, listener, 0x2B);
    CHECK(run(w, 1, &sqe).result == 0);
    CHECK(w->parked_count[0] == 0 && w->parked_count[1] == 0);
    return outcome;
}

static bool same_entry(const struct trace_entry *a, const struct trace_entry *b)
{
    return a->node == b->node && a->cqe.user_data == b->cqe.user_data &&
           a->cqe.result == b->cqe.result && a->cqe.flags == b->cqe.flags &&
           a->cqe.buffer_id == b->cqe.buffer_id;
}

static bool same_trace(const struct trace *a, const struct trace *b)
{
    if (a->count != b->count) {
        return false;
    }
    for (uint32_t i = 0; i < a->count; ++i) {
        if (!same_entry(&a->entries[i], &b->entries[i])) {
            return false;
        }
    }
    return true;
}

/* Stable sort by (node, user_data): keeps each user_data's own order. */
static void sort_trace(struct trace *trace)
{
    for (uint32_t i = 1; i < trace->count; ++i) {
        struct trace_entry entry = trace->entries[i];
        uint32_t at = i;

        while (at > 0) {
            const struct trace_entry *before = &trace->entries[at - 1];

            if (before->node < entry.node ||
                (before->node == entry.node &&
                 before->cqe.user_data <= entry.cqe.user_data)) {
                break;
            }
            trace->entries[at] = *before;
            --at;
        }
        trace->entries[at] = entry;
    }
}

static struct outcome
run_scenario(struct trace *trace, const struct faulty_executor_options *options,
             struct faulty_executor_stats *stats)
{
    struct world w;
    struct outcome outcome;

    open_world(&w, trace, options);
    outcome = scenario(&w);
    *stats = stats_of(&w);
    close_world(&w);
    return outcome;
}

static void test_transparent(void)
{
    struct faulty_executor_options options;
    struct faulty_executor_stats stats;
    struct outcome bare;
    struct outcome wrapped;

    memset(&options, 0, sizeof(options));
    options.seed = seed;
    options.delay_reaps_max = 4;
    bare = run_scenario(&traces[0], NULL, &stats);
    CHECK(bare.write == (int32_t)MESSAGE_BYTES);
    CHECK(bare.read == (int32_t)MESSAGE_BYTES);
    CHECK(bare.fsync == 0);
    CHECK(bare.send == (int32_t)MESSAGE_BYTES);
    CHECK(bare.recv == (int32_t)MESSAGE_BYTES);
    CHECK(bare.zc_send == (int32_t)MESSAGE_BYTES);
    CHECK(bare.zc_flags == VSR_IO_CQE_MORE);
    CHECK(bare.notif_flags == VSR_IO_CQE_NOTIF);
    wrapped = run_scenario(&traces[1], &options, &stats);
    CHECK(memcmp(&bare, &wrapped, sizeof(bare)) == 0);
    CHECK(same_trace(&traces[0], &traces[1]));
    CHECK(stats.eio == 0 && stats.shortened == 0 && stats.delayed == 0 &&
          stats.cancelled == 0);
}

static void test_each_fault(void)
{
    struct faulty_executor_options options;
    struct faulty_executor_stats stats;
    struct outcome bare;
    struct outcome faulty;
    uint32_t notifs = 0;

    bare = run_scenario(&traces[0], NULL, &stats);
    for (uint32_t i = 0; i < traces[0].count; ++i) {
        if ((traces[0].entries[i].cqe.flags & VSR_IO_CQE_NOTIF) != 0) {
            ++notifs;
        }
    }
    CHECK(notifs == 1);

    /* -EIO on the write, the read and the fsync; nothing else changes. */
    memset(&options, 0, sizeof(options));
    options.seed = seed;
    options.eio_ppm = PPM;
    faulty = run_scenario(&traces[1], &options, &stats);
    CHECK(faulty.write == -EIO && faulty.read == -EIO && faulty.fsync == -EIO);
    CHECK(faulty.send == bare.send && faulty.recv == bare.recv);
    CHECK(faulty.zc_send == bare.zc_send && faulty.zc_flags == bare.zc_flags);
    CHECK(stats.eio == 3 && stats.shortened == 0 && stats.delayed == 0 &&
          stats.cancelled == 0);

    /* Short write, read, send and receive; the zero-copy send is whole. */
    memset(&options, 0, sizeof(options));
    options.seed = seed;
    options.short_ppm = PPM;
    faulty = run_scenario(&traces[1], &options, &stats);
    CHECK(faulty.write >= 1 && faulty.write < bare.write);
    CHECK(faulty.read >= 1 && faulty.read < bare.read);
    CHECK(faulty.send >= 1 && faulty.send < bare.send);
    CHECK(faulty.recv >= 1 && faulty.recv < bare.recv);
    CHECK(faulty.recv <= faulty.send);
    CHECK(faulty.fsync == 0);
    CHECK(faulty.zc_send == bare.zc_send && faulty.zc_flags == bare.zc_flags);
    CHECK(faulty.notif_flags == VSR_IO_CQE_NOTIF);
    CHECK(stats.shortened == 4 && stats.eio == 0 && stats.delayed == 0 &&
          stats.cancelled == 0);

    /* Every completion held; each user_data's completions arrive intact
     * and in order, the NOTIF held behind its result only when both were
     * held together. */
    memset(&options, 0, sizeof(options));
    options.seed = seed;
    options.delay_ppm = PPM;
    options.delay_reaps_max = 3;
    faulty = run_scenario(&traces[1], &options, &stats);
    CHECK(memcmp(&bare, &faulty, sizeof(bare)) == 0);
    CHECK(traces[1].count == traces[0].count);
    CHECK(stats.delayed >= traces[0].count - notifs &&
          stats.delayed <= traces[0].count);
    CHECK(stats.eio == 0 && stats.shortened == 0 && stats.cancelled == 0);
    sort_trace(&traces[0]);
    sort_trace(&traces[1]);
    CHECK(same_trace(&traces[0], &traces[1]));

    /* The receive posted before any byte is cancelled. */
    memset(&options, 0, sizeof(options));
    options.seed = seed;
    options.cancel_recv_ppm = PPM;
    faulty = run_scenario(&traces[1], &options, &stats);
    CHECK(faulty.recv == -ECANCELED);
    CHECK(faulty.write == bare.write && faulty.read == bare.read);
    CHECK(faulty.send == bare.send && faulty.zc_send == bare.zc_send);
    CHECK(stats.cancelled == 1 && stats.eio == 0 && stats.shortened == 0 &&
          stats.delayed == 0);
}

/* ------------------------------------------------------------------------
 * Seeded random walk with mixed rates
 * --------------------------------------------------------------------- */

#define WALK_RECORDS 2000u
#define RECORDS_MAX 4096u
#define WALK_OWNER 1u

enum walk_state { WALK_FREE, WALK_PENDING, WALK_NOTIF, WALK_DONE };

struct walk_record {
    uint8_t opcode;
    uint8_t zero_copy;
    uint8_t state;
    uint8_t node;
};

struct walk {
    struct world w;
    struct test_random random;
    struct walk_record records[RECORDS_MAX];
    uint32_t count;
    uint32_t outstanding;
    uint32_t sends_open; /* Sends whose result is not delivered. */
    uint32_t recvs_open;
    uint64_t sent;
    uint64_t received;
    uint64_t eio_seen;
    uint64_t cancelled_seen;
    int32_t fd;
    int32_t client;
    int32_t server;
};

static const unsigned char walk_source[64] = {
    1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16,
    17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32,
    33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48,
    49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64};
static unsigned char walk_read[64];
static unsigned char walk_recv[256];
static struct walk walks[2];

static uint32_t below(struct walk *walk, uint32_t bound)
{
    return test_random_bounded(&walk->random, bound);
}

/* A fresh tracked record for node; the caller fills the rest. */
static struct vsr_io_sqe walk_add(struct walk *walk, uint32_t node,
                                  uint8_t opcode, int32_t fd)
{
    struct walk_record *record = &walk->records[walk->count];
    uint64_t user_data = VSR_IO_USER_DATA(WALK_OWNER, walk->count);

    CHECK(walk->count < RECORDS_MAX);
    record->opcode = opcode;
    record->zero_copy = 0;
    record->state = WALK_PENDING;
    record->node = (uint8_t)node;
    ++walk->count;
    ++walk->outstanding;
    if (opcode == VSR_IO_SQE_SEND) {
        ++walk->sends_open;
    } else if (opcode == VSR_IO_SQE_RECV) {
        ++walk->recvs_open;
    }
    return make_sqe(opcode, fd, user_data);
}

static struct vsr_io_sqe walk_recv_record(struct walk *walk)
{
    struct vsr_io_sqe sqe = walk_add(walk, 1, VSR_IO_SQE_RECV, walk->server);

    sqe.addr = walk_recv;
    sqe.length = sizeof(walk_recv);
    return sqe;
}

static void walk_complete(struct walk *walk, uint32_t node,
                          const struct vsr_io_cqe *cqe)
{
    uint64_t index = cqe->user_data & ((UINT64_C(1) << VSR_IO_OWNER_SHIFT) - 1);
    struct walk_record *record;

    CHECK(VSR_IO_OWNER(cqe->user_data) == WALK_OWNER);
    CHECK(index < walk->count);
    record = &walk->records[index];
    CHECK(record->node == node);
    if ((cqe->flags & VSR_IO_CQE_NOTIF) != 0) {
        /* Only after its result, exactly once. */
        CHECK(record->zero_copy && record->state == WALK_NOTIF);
        CHECK(cqe->result == 0 && cqe->flags == VSR_IO_CQE_NOTIF);
        record->state = WALK_DONE;
        --walk->outstanding;
        return;
    }
    CHECK(record->state == WALK_PENDING);
    CHECK((cqe->flags & (VSR_IO_CQE_BUFFER | VSR_IO_CQE_BUFFER_MORE)) == 0);
    if (record->zero_copy && (cqe->flags & VSR_IO_CQE_MORE) != 0) {
        record->state = WALK_NOTIF;
    } else {
        CHECK(cqe->flags == 0);
        record->state = WALK_DONE;
        --walk->outstanding;
    }
    switch (record->opcode) {
    case VSR_IO_SQE_SEND:
        CHECK(cqe->result > 0);
        walk->sent += (uint32_t)cqe->result;
        --walk->sends_open;
        break;
    case VSR_IO_SQE_RECV:
        CHECK(cqe->result >= 0 || cqe->result == -ECANCELED);
        if (cqe->result > 0) {
            walk->received += (uint32_t)cqe->result;
        } else if (cqe->result == -ECANCELED) {
            ++walk->cancelled_seen;
        }
        --walk->recvs_open;
        break;
    case VSR_IO_SQE_READ:
    case VSR_IO_SQE_WRITE:
    case VSR_IO_SQE_FSYNC:
        CHECK(cqe->result >= 0 || cqe->result == -EIO);
        if (cqe->result == -EIO) {
            ++walk->eio_seen;
        }
        break;
    default:
        CHECK(cqe->result == 0);
        break;
    }
}

static void walk_pump(struct walk *walk)
{
    for (uint32_t node = 0; node < 2; ++node) {
        struct vsr_io_cqe cqes[16];
        uint32_t count = reap(&walk->w, node, cqes, 1u + below(walk, 16));

        for (uint32_t i = 0; i < count; ++i) {
            walk_complete(walk, node, &cqes[i]);
        }
    }
    (void)vsr_sim_advance(walk->w.sim);
}

static void walk_step(struct walk *walk, struct vsr_io_sqe batches[2][8],
                      uint32_t counts[2])
{
    uint32_t kind = below(walk, 100);
    uint32_t length = 1u + below(walk, 64);
    uint64_t offset = (uint64_t)below(walk, 16) * 64u;
    struct vsr_io_sqe sqe;
    uint32_t node = 0;

    if (kind < 12) {
        node = below(walk, 2);
        sqe = walk_add(walk, node, VSR_IO_SQE_NOP, -1);
    } else if (kind < 28) {
        sqe = walk_add(walk, 0, VSR_IO_SQE_WRITE, walk->fd);
        sqe.addr = walk_source;
        sqe.length = length;
        sqe.offset = offset;
    } else if (kind < 44) {
        sqe = walk_add(walk, 0, VSR_IO_SQE_READ, walk->fd);
        sqe.addr = walk_read;
        sqe.length = length;
        sqe.offset = offset;
    } else if (kind < 50) {
        sqe = walk_add(walk, 0, VSR_IO_SQE_FSYNC, walk->fd);
    } else if (kind < 66) {
        sqe = walk_add(walk, 0, VSR_IO_SQE_SEND, walk->client);
        sqe.addr = walk_source;
        sqe.length = length;
    } else if (kind < 76) {
        sqe = walk_add(walk, 0, VSR_IO_SQE_SEND, walk->client);
        sqe.addr = walk_source;
        sqe.length = length;
        sqe.op_flags = VSR_IO_SEND_ZERO_COPY;
        walk->records[walk->count - 1].zero_copy = 1;
    } else {
        node = 1;
        sqe = walk_recv_record(walk);
    }
    batches[node][counts[node]++] = sqe;
}

static void walk_run(struct walk *walk, struct trace *trace,
                     struct faulty_executor_stats *stats)
{
    struct faulty_executor_options options;
    struct world *w = &walk->w;
    struct vsr_io_address address;
    struct vsr_io_sqe sqe;
    int32_t listener;
    uint32_t submitted = 0;
    bool shut = false;

    memset(walk, 0, sizeof(*walk));
    test_random_seed(&walk->random, seed, 0x3A1C);
    memset(&options, 0, sizeof(options));
    options.seed = seed;
    open_world(w, trace, &options);

    /* Setup with faults off. */
    sqe = make_sqe(VSR_IO_SQE_OPENAT, VSR_SIM_ROOT, 0x10);
    sqe.addr = "walk";
    sqe.op_flags = O_CREAT | O_RDWR;
    sqe.length = 0600;
    walk->fd = run(w, 0, &sqe).result;
    CHECK(walk->fd >= 0);
    address = vsr_sim_address(w->sim, 1, PORT);
    listener = make_socket(w, 1, 0x20);
    sqe = make_sqe(VSR_IO_SQE_BIND, listener, 0x21);
    sqe.addr = &address.sockaddr;
    sqe.length = address.length;
    CHECK(run(w, 1, &sqe).result == 0);
    sqe = make_sqe(VSR_IO_SQE_LISTEN, listener, 0x22);
    sqe.length = 4;
    CHECK(run(w, 1, &sqe).result == 0);
    sqe = make_sqe(VSR_IO_SQE_ACCEPT, listener, 0x23);
    submit(w, 1, &sqe, 1);
    walk->client = make_socket(w, 0, 0x24);
    sqe = make_sqe(VSR_IO_SQE_CONNECT, walk->client, 0x25);
    sqe.addr = &address.sockaddr;
    sqe.length = address.length;
    CHECK(run(w, 0, &sqe).result == 0);
    walk->server = wait_for(w, 1, 0x23).result;
    CHECK(walk->server >= 0);
    CHECK(w->parked_count[0] == 0 && w->parked_count[1] == 0);
    trace->count = 0;

    options.eio_ppm = 50000;
    options.short_ppm = 100000;
    options.delay_ppm = 200000;
    options.delay_reaps_max = 4;
    options.cancel_recv_ppm = 50000;
    faulty_executor_set_options(w->faulty[0], &options);
    faulty_executor_set_options(w->faulty[1], &options);

    while (submitted < WALK_RECORDS) {
        struct vsr_io_sqe batches[2][8];
        uint32_t counts[2] = {0, 0};
        uint32_t size = 1u + below(walk, 8);

        if (size > WALK_RECORDS - submitted) {
            size = WALK_RECORDS - submitted;
        }
        for (uint32_t i = 0; i < size; ++i) {
            walk_step(walk, batches, counts);
        }
        submitted += size;
        for (uint32_t node = 0; node < 2; ++node) {
            if (counts[node] > 0) {
                submit(w, node, batches[node], counts[node]);
            }
        }
        walk_pump(walk);
    }

    /* Drain: keep a receive posted until every sent byte is received,
     * then shut the sender down so parked receives end at end of stream. */
    for (uint32_t spin = 0;
         walk->outstanding > 0 || walk->received < walk->sent; ++spin) {
        CHECK(spin < SPIN_MAX);
        if (walk->recvs_open == 0 && walk->received < walk->sent) {
            sqe = walk_recv_record(walk);
            submit(w, 1, &sqe, 1);
        }
        if (!shut && walk->sends_open == 0) {
            sqe = walk_add(walk, 0, VSR_IO_SQE_SHUTDOWN, walk->client);
            sqe.length = SHUT_WR;
            submit(w, 0, &sqe, 1);
            shut = true;
        }
        walk_pump(walk);
    }
    CHECK(walk->received == walk->sent);
    for (uint32_t i = 0; i < walk->count; ++i) {
        CHECK(walk->records[i].state == WALK_DONE);
    }
    *stats = stats_of(w);
    CHECK(stats->eio == walk->eio_seen);
    CHECK(stats->cancelled == walk->cancelled_seen);
    CHECK(stats->eio > 0 && stats->shortened > 0 && stats->delayed > 0);
    close_world(w);
}

static void test_random_walk(void)
{
    struct faulty_executor_stats first;
    struct faulty_executor_stats second;

    walk_run(&walks[0], &traces[0], &first);
    walk_run(&walks[1], &traces[1], &second);
    CHECK(same_trace(&traces[0], &traces[1]));
    CHECK(memcmp(&first, &second, sizeof(first)) == 0);
    CHECK(walks[0].count == walks[1].count);
    CHECK(walks[0].sent == walks[1].sent);
    printf("walk: %" PRIu32 " records, %" PRIu32 " completions, %" PRIu64
           " bytes; eio %" PRIu64 ", shortened %" PRIu64 ", delayed %" PRIu64
           ", cancelled %" PRIu64 "\n",
           walks[0].count, traces[0].count, walks[0].sent, first.eio,
           first.shortened, first.delayed, first.cancelled);
}

static void test_reserved(void)
{
    struct faulty_executor_options options;
    struct vsr_io_sqe sqe =
        make_sqe(VSR_IO_SQE_NOP, -1, FAULTY_EXECUTOR_USER_DATA);
    struct world w;

    memset(&options, 0, sizeof(options));
    options.seed = seed;
    open_world(&w, &traces[0], &options);
    CHECK(w.ex[0].ops->submit_and_wait(w.ex[0].ctx, &sqe, 1, 0, 0, 0) ==
          -EINVAL);
    CHECK(vsr_sim_inflight(w.sim, 0) == 0);
    close_world(&w);
}

/* ------------------------------------------------------------------------
 * Review cases: ordering the caller relies on, plausible counts, waits,
 * chains, capacities
 * --------------------------------------------------------------------- */

#define NOPS 1600u

static uint8_t nop_seen[NOPS];

static struct faulty_executor_options rates(void)
{
    struct faulty_executor_options options;

    memset(&options, 0, sizeof(options));
    options.seed = seed;
    return options;
}

static void set_rates(struct world *w, uint32_t node,
                      const struct faulty_executor_options *options)
{
    faulty_executor_set_options(w->faulty[node], options);
}

/* Node 1 listens on PORT. */
static int32_t listen_on(struct world *w, uint64_t user_data)
{
    struct vsr_io_address address = vsr_sim_address(w->sim, 1, PORT);
    int32_t listener = make_socket(w, 1, user_data);
    struct vsr_io_sqe sqe = make_sqe(VSR_IO_SQE_BIND, listener, user_data + 1);

    sqe.addr = &address.sockaddr;
    sqe.length = address.length;
    CHECK(run(w, 1, &sqe).result == 0);
    sqe = make_sqe(VSR_IO_SQE_LISTEN, listener, user_data + 2);
    sqe.length = 4;
    CHECK(run(w, 1, &sqe).result == 0);
    return listener;
}

/* Node 0's client connected to node 1's accepted server end. */
static void connect_pair(struct world *w, int32_t listener, uint64_t user_data,
                         int32_t *client, int32_t *server)
{
    struct vsr_io_address address = vsr_sim_address(w->sim, 1, PORT);
    struct vsr_io_sqe sqe = make_sqe(VSR_IO_SQE_ACCEPT, listener, user_data);

    submit(w, 1, &sqe, 1);
    *client = make_socket(w, 0, user_data + 1);
    sqe = make_sqe(VSR_IO_SQE_CONNECT, *client, user_data + 2);
    sqe.addr = &address.sockaddr;
    sqe.length = address.length;
    CHECK(run(w, 0, &sqe).result == 0);
    *server = wait_for(w, 1, user_data).result;
    CHECK(*server >= 0);
}

/* Node 0 sends all of bytes on client (its wrapper must be quiet). */
static void send_exact(struct world *w, int32_t client, const void *bytes,
                       uint32_t size, uint64_t user_data)
{
    struct vsr_io_sqe sqe = make_sqe(VSR_IO_SQE_SEND, client, user_data);

    sqe.addr = bytes;
    sqe.length = size;
    CHECK(run(w, 0, &sqe).result == (int32_t)size);
}

/* Records of a LINK chain get no fault, and a batch whose last record is
 * linked gets no appended CANCEL, which would join that chain. */
static void test_chains(void)
{
    static char read_buffer[MESSAGE_BYTES];
    static char recv_buffer[MESSAGE_BYTES];
    struct faulty_executor_options quiet = rates();
    struct faulty_executor_options all = rates();
    struct faulty_executor_stats stats;
    struct vsr_io_sqe batch[2];
    struct vsr_io_cqe cqe;
    struct world w;
    int32_t listener;
    int32_t client;
    int32_t server;
    int32_t fd;
    uint32_t got;

    all.eio_ppm = PPM;
    all.short_ppm = PPM;
    all.delay_ppm = PPM;
    all.delay_reaps_max = 2;
    all.cancel_recv_ppm = PPM;
    open_world(&w, &traces[0], &quiet);
    batch[0] = make_sqe(VSR_IO_SQE_OPENAT, VSR_SIM_ROOT, 0x10);
    batch[0].addr = "chain";
    batch[0].op_flags = O_CREAT | O_RDWR;
    batch[0].length = 0600;
    fd = run(&w, 0, &batch[0]).result;
    CHECK(fd >= 0);
    listener = listen_on(&w, 0x20);
    connect_pair(&w, listener, 0x30, &client, &server);
    set_rates(&w, 0, &all);
    set_rates(&w, 1, &all);

    /* A WRITE linked to a READ: both whole, neither -EIO nor delayed. */
    batch[0] = make_sqe(VSR_IO_SQE_WRITE, fd, 0x90);
    batch[0].flags = VSR_IO_SQE_LINK;
    batch[0].addr = MESSAGE;
    batch[0].length = MESSAGE_BYTES;
    batch[1] = make_sqe(VSR_IO_SQE_READ, fd, 0x91);
    batch[1].addr = read_buffer;
    batch[1].length = MESSAGE_BYTES;
    submit(&w, 0, batch, 2);
    CHECK(wait_for(&w, 0, 0x90).result == (int32_t)MESSAGE_BYTES);
    CHECK(wait_for(&w, 0, 0x91).result == (int32_t)MESSAGE_BYTES);
    CHECK(memcmp(read_buffer, MESSAGE, MESSAGE_BYTES) == 0);
    faulty_executor_stats(w.faulty[0], &stats);
    CHECK(stats.eio == 0 && stats.shortened == 0 && stats.delayed == 0);

    /* A RECV ending a chain is neither shortened nor cancelled. */
    set_rates(&w, 0, &quiet);
    batch[0] = make_sqe(VSR_IO_SQE_NOP, -1, 0x92);
    batch[0].flags = VSR_IO_SQE_LINK;
    batch[1] = make_sqe(VSR_IO_SQE_RECV, server, 0x93);
    batch[1].addr = recv_buffer;
    batch[1].length = MESSAGE_BYTES;
    submit(&w, 1, batch, 2);
    send_exact(&w, client, MESSAGE, MESSAGE_BYTES, 0x94);
    CHECK(wait_for(&w, 1, 0x92).result == 0);
    CHECK(wait_for(&w, 1, 0x93).result == (int32_t)MESSAGE_BYTES);
    CHECK(memcmp(recv_buffer, MESSAGE, MESSAGE_BYTES) == 0);

    /* A RECV before a linked tail: shortened (it is not in the chain) but
     * never cancelled. */
    batch[0] = make_sqe(VSR_IO_SQE_RECV, server, 0x95);
    batch[0].addr = recv_buffer;
    batch[0].length = MESSAGE_BYTES;
    batch[1] = make_sqe(VSR_IO_SQE_NOP, -1, 0x96);
    batch[1].flags = VSR_IO_SQE_LINK;
    submit(&w, 1, batch, 2);
    send_exact(&w, client, MESSAGE, MESSAGE_BYTES, 0x97);
    CHECK(wait_for(&w, 1, 0x96).result == 0);
    cqe = wait_for(&w, 1, 0x95);
    CHECK(cqe.result > 0 && cqe.result < (int32_t)MESSAGE_BYTES);
    got = (uint32_t)cqe.result;
    CHECK(memcmp(recv_buffer, MESSAGE, got) == 0);
    faulty_executor_stats(w.faulty[1], &stats);
    CHECK(stats.cancelled == 0 && stats.shortened == 1);
    set_rates(&w, 1, &quiet);
    while (got < MESSAGE_BYTES) {
        batch[0] = make_sqe(VSR_IO_SQE_RECV, server, 0x98 + got);
        batch[0].addr = recv_buffer;
        batch[0].length = MESSAGE_BYTES - got;
        cqe = run(&w, 1, &batch[0]);
        CHECK(cqe.result > 0);
        CHECK(memcmp(recv_buffer, &MESSAGE[got], (size_t)cqe.result) == 0);
        got += (uint32_t)cqe.result;
    }
    close_world(&w);
}

/* More completions than the wrapper holds, each delayed: the rest wait in
 * the inner executor, and every one arrives exactly once. */
static void test_capacity(void)
{
    struct faulty_executor_options options = rates();
    struct faulty_executor_stats stats;
    struct vsr_io_sqe batch[200];
    struct world w;
    uint32_t delivered = 0;
    uint32_t most = 0;

    options.delay_ppm = PPM;
    options.delay_reaps_max = 3;
    open_world(&w, &traces[0], &options);
    memset(nop_seen, 0, sizeof(nop_seen));
    for (uint32_t first = 0; first < NOPS; first += 200) {
        for (uint32_t i = 0; i < 200; ++i) {
            batch[i] = make_sqe(VSR_IO_SQE_NOP, -1, 0x1000 + first + i);
        }
        submit(&w, 0, batch, 200);
    }
    for (uint32_t spin = 0; delivered < NOPS; ++spin) {
        struct vsr_io_cqe cqes[16];
        struct vsr_io_executor ex = w.ex[0];
        uint32_t count = ex.ops->reap(ex.ctx, cqes, 16);

        CHECK(spin < SPIN_MAX);
        CHECK(w.faulty[0]->held_count <= FAULTY_EXECUTOR_HELD);
        most = w.faulty[0]->held_count > most ? w.faulty[0]->held_count : most;
        for (uint32_t i = 0; i < count; ++i) {
            uint64_t index = cqes[i].user_data - 0x1000;

            CHECK(index < NOPS && nop_seen[index] == 0);
            CHECK(cqes[i].result == 0 && cqes[i].flags == 0);
            nop_seen[index] = 1;
        }
        delivered += count;
    }
    CHECK(most == FAULTY_EXECUTOR_HELD);
    faulty_executor_stats(w.faulty[0], &stats);
    CHECK(stats.delayed == NOPS);
    close_world(&w);
}

/* The user_data map: the newest record of a user_data decides, and a
 * record pushed out of the map by later ones only ever gets delayed. */
static void test_map(void)
{
    static char read_buffer[MESSAGE_BYTES];
    struct faulty_executor_options options = rates();
    struct vsr_io_sqe batch[128];
    struct vsr_io_sqe sqe;
    struct world w;
    int32_t fd;

    options.eio_ppm = PPM;
    open_world(&w, &traces[0], &options);
    sqe = make_sqe(VSR_IO_SQE_OPENAT, VSR_SIM_ROOT, 0x10);
    sqe.addr = "map";
    sqe.op_flags = O_CREAT | O_RDWR;
    sqe.length = 0600;
    fd = run(&w, 0, &sqe).result;
    CHECK(fd >= 0);

    /* A successful SKIP_SUCCESS write never completes, so its entry stays;
     * a NOP reusing its user_data must not be taken for the write. */
    sqe = make_sqe(VSR_IO_SQE_WRITE, fd, 0x2000);
    sqe.flags = VSR_IO_SQE_SKIP_SUCCESS;
    sqe.addr = MESSAGE;
    sqe.length = MESSAGE_BYTES;
    submit(&w, 0, &sqe, 1);
    for (uint32_t spin = 0; vsr_sim_inflight(w.sim, 0) > 0; ++spin) {
        CHECK(spin < SPIN_MAX);
        CHECK(vsr_sim_advance(w.sim) >= 0);
    }
    sqe = make_sqe(VSR_IO_SQE_NOP, -1, 0x2000);
    CHECK(run(&w, 0, &sqe).result == 0);

    /* A READ outlives more records than the map holds. */
    sqe = make_sqe(VSR_IO_SQE_READ, fd, 0x2001);
    sqe.addr = read_buffer;
    sqe.length = MESSAGE_BYTES;
    submit(&w, 0, &sqe, 1);
    for (uint32_t first = 0; first < FAULTY_EXECUTOR_RECORDS; first += 128) {
        struct vsr_io_cqe cqes[128];
        uint32_t count = 0;

        for (uint32_t i = 0; i < 128; ++i) {
            batch[i] = make_sqe(VSR_IO_SQE_NOP, -1, 0x10000 + first + i);
        }
        submit(&w, 0, batch, 128);
        while (count < 128) {
            uint32_t got = reap(&w, 0, cqes, 128 - count);

            CHECK(got > 0);
            for (uint32_t i = 0; i < got; ++i) {
                CHECK(cqes[i].user_data != 0x2001);
            }
            count += got;
        }
    }
    CHECK(vsr_sim_inflight(w.sim, 0) == 1);
    CHECK(wait_for(&w, 0, 0x2001).result == (int32_t)MESSAGE_BYTES);
    CHECK(memcmp(read_buffer, MESSAGE, MESSAGE_BYTES) == 0);
    close_world(&w);
}

/* A CANCEL of a completion the wrapper holds finds nothing, as one of a
 * completion still in the ring's CQ does; the completion then arrives. */
static void test_cancel_held(void)
{
    struct faulty_executor_options options = rates();
    struct vsr_io_sqe sqe;
    struct vsr_io_cqe cqe;
    struct world w;

    options.delay_ppm = PPM;
    options.delay_reaps_max = 1000;
    open_world(&w, &traces[0], &options);
    sqe = make_sqe(VSR_IO_SQE_NOP, -1, 0xA0);
    submit(&w, 0, &sqe, 1);
    CHECK(reap(&w, 0, &cqe, 1) == 0);
    sqe = make_sqe(VSR_IO_SQE_CANCEL, -1, 0xA1);
    sqe.offset = 0xA0;
    CHECK(run(&w, 0, &sqe).result == -ENOENT);
    CHECK(wait_for(&w, 0, 0xA0).result == 0);
    CHECK(run(&w, 0, &sqe).result == -ENOENT);
    CHECK(w.parked_count[0] == 0);
    /* Torn down with a completion held: nothing refers to it, and init
     * over a new inner executor starts empty. */
    sqe = make_sqe(VSR_IO_SQE_NOP, -1, 0xA2);
    submit(&w, 0, &sqe, 1);
    CHECK(reap(&w, 0, &cqe, 1) == 0);
    CHECK(w.faulty[0]->held_count == 1);
    close_world(&w);
    open_world(&w, &traces[0], &options);
    CHECK(w.faulty[0]->held_count == 0);
    CHECK(reap(&w, 0, &cqe, 1) == 0);
    close_world(&w);
}

/* A completion is held for 1..delay_reaps_max reaps (0 counting as 1). */
static void test_delay_bounds(void)
{
    static const uint32_t limits[] = {0, 1, 3};
    struct faulty_executor_options options = rates();
    struct world w;
    uint64_t user_data = 0xB000;

    options.delay_ppm = PPM;
    open_world(&w, &traces[0], &options);
    for (uint32_t l = 0; l < sizeof(limits) / sizeof(limits[0]); ++l) {
        uint32_t most = limits[l] > 0 ? limits[l] : 1u;
        bool seen[3] = {false, false, false};

        options.delay_reaps_max = limits[l];
        set_rates(&w, 0, &options);
        for (uint32_t i = 0; i < 200; ++i) {
            struct vsr_io_sqe sqe = make_sqe(VSR_IO_SQE_NOP, -1, user_data++);
            struct vsr_io_cqe cqe;
            uint32_t calls = 1;

            submit(&w, 0, &sqe, 1);
            while (reap(&w, 0, &cqe, 1) == 0) {
                ++calls;
                CHECK(calls <= most + 1);
            }
            /* The first reap pulled and held it. */
            CHECK(calls >= 2);
            seen[calls - 2] = true;
        }
        for (uint32_t r = 0; r < most; ++r) {
            CHECK(seen[r]);
        }
    }
    close_world(&w);
}

/* Only a batch of at most FAULTY_EXECUTOR_BATCH records gets
 * submission-side faults, and an appended CANCEL needs room in it. */
static void test_batches(void)
{
    static const uint32_t sizes[] = {FAULTY_EXECUTOR_BATCH - 1,
                                     FAULTY_EXECUTOR_BATCH,
                                     FAULTY_EXECUTOR_BATCH + 1};
    static const uint32_t cancels[] = {1, 0, 0};
    static struct vsr_io_sqe batch[FAULTY_EXECUTOR_BATCH + 1];
    static char buffer[MESSAGE_BYTES];
    struct faulty_executor_options all = rates();
    struct world w;
    int32_t listener;
    int32_t client;
    int32_t server;
    uint64_t user_data = 0x5000;

    open_world(&w, &traces[0], &all);
    listener = listen_on(&w, 0x20);
    connect_pair(&w, listener, 0x30, &client, &server);
    all.short_ppm = PPM;
    all.cancel_recv_ppm = PPM;
    set_rates(&w, 1, &all);
    for (uint32_t k = 0; k < sizeof(sizes) / sizeof(sizes[0]); ++k) {
        struct faulty_executor_stats before;
        struct faulty_executor_stats after;
        uint32_t cancelled = 0;
        uint32_t got;

        faulty_executor_stats(w.faulty[1], &before);
        for (uint32_t i = 0; i < sizes[k]; ++i) {
            batch[i] = make_sqe(VSR_IO_SQE_RECV, server, user_data++);
            batch[i].addr = buffer;
            batch[i].length = MESSAGE_BYTES;
        }
        submit(&w, 1, batch, sizes[k]);
        do {
            struct vsr_io_cqe cqes[8];

            got = reap(&w, 1, cqes, 8);
            for (uint32_t i = 0; i < got; ++i) {
                CHECK(cqes[i].result == -ECANCELED);
                ++cancelled;
            }
        } while (got > 0);
        faulty_executor_stats(w.faulty[1], &after);
        CHECK(cancelled == cancels[k]);
        CHECK(after.cancelled - before.cancelled == cancels[k]);
        CHECK(after.shortened - before.shortened ==
              (sizes[k] <= FAULTY_EXECUTOR_BATCH ? sizes[k] : 0));
    }
    close_world(&w);
}

/* Rates above 1,000,000 ppm saturate: the same trace as at 1,000,000. */
static void test_saturated(void)
{
    struct faulty_executor_options options = rates();
    struct faulty_executor_stats first;
    struct faulty_executor_stats second;

    options.eio_ppm = PPM;
    options.short_ppm = PPM;
    options.delay_ppm = PPM;
    options.delay_reaps_max = 3;
    (void)run_scenario(&traces[0], &options, &first);
    options.eio_ppm = UINT32_MAX;
    options.short_ppm = UINT32_MAX;
    options.delay_ppm = UINT32_MAX;
    (void)run_scenario(&traces[1], &options, &second);
    CHECK(same_trace(&traces[0], &traces[1]));
    CHECK(memcmp(&first, &second, sizeof(first)) == 0);
    CHECK(first.eio > 0 && first.delayed > 0);
}

struct unit {
    const char *name;
    void (*run)(void);
};

static const struct unit units[] = {
    {"reserved", test_reserved},
    {"transparent", test_transparent},
    {"each_fault", test_each_fault},
    {"random_walk", test_random_walk},
    {"chains", test_chains},
    {"capacity", test_capacity},
    {"map", test_map},
    {"cancel_held", test_cancel_held},
    {"delay_bounds", test_delay_bounds},
    {"batches", test_batches},
    {"saturated", test_saturated},
};

/* Usage: faulty_executor [SEED [TEST]]. */
int main(int argc, char **argv)
{
    const char *only = argc > 2 ? argv[2] : NULL;

    if (argc > 1) {
        seed = strtoull(argv[1], NULL, 0);
    }
    printf("faulty_executor seed %" PRIu64 "\n", seed);
    for (size_t i = 0; i < sizeof(units) / sizeof(units[0]); ++i) {
        if (only == NULL || strcmp(only, units[i].name) == 0) {
            units[i].run();
        }
    }
    return 0;
}
