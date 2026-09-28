#include "config.h"

#define _GNU_SOURCE 1

#include "lib/check.h"
#include "vsr-sim.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

/*
 * The simulated world (docs/io-implementation.md, "Simulation"): the clock
 * and its faults, advance ordering and its -1 return, reproducibility of a
 * fault-heavy scenario across two worlds with one seed, partitions, the
 * crash model at block granularity, directory operations, inspection, the
 * decision-58 lifetime rules (CLOSE never completes pending records, ring
 * unregistration under a pending receive), and the two aborts the world
 * promises: an ownership violation and a stale executor handle, each run
 * in a forked child that must die of SIGABRT.
 */

#define MS UINT64_C(1000000)
#define BLOCK UINT64_C(4096)
#define PORT 7u

static uint64_t seed = 1;

/* ------------------------------------------------------------------------
 * Helpers over the executor
 * --------------------------------------------------------------------- */

static struct vsr_sim *make_world(uint64_t world_seed, uint32_t nodes,
                                  const struct vsr_sim_faults *faults)
{
    struct vsr_sim_options options;
    struct vsr_sim *sim = NULL;

    memset(&options, 0, sizeof(options));
    options.seed = world_seed;
    options.nodes = nodes;
    options.block_bytes = BLOCK;
    options.file_slots = 8;
    options.buffer_regions = 4;
    if (faults != NULL) {
        options.faults = *faults;
    }
    CHECK(vsr_sim_create(&options, &sim) == 0);
    CHECK(sim != NULL);
    return sim;
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

static void submit(struct vsr_io_executor ex, const struct vsr_io_sqe *sqes,
                   uint32_t count)
{
    CHECK(ex.ops->submit_and_wait(ex.ctx, sqes, count, 0, 0, 0) == 0);
}

static uint32_t reap(struct vsr_io_executor ex, struct vsr_io_cqe *cqes,
                     uint32_t capacity)
{
    return ex.ops->reap(ex.ctx, cqes, capacity);
}

/* The node's next completion, advancing the world until one exists. */
static struct vsr_io_cqe wait_one(struct vsr_sim *sim,
                                  struct vsr_io_executor ex)
{
    struct vsr_io_cqe cqe;

    for (;;) {
        if (reap(ex, &cqe, 1) == 1) {
            return cqe;
        }
        CHECK(vsr_sim_advance(sim) >= 0);
    }
}

static struct vsr_io_cqe run(struct vsr_sim *sim, struct vsr_io_executor ex,
                             const struct vsr_io_sqe *sqe)
{
    submit(ex, sqe, 1);
    return wait_one(sim, ex);
}

/* Advances until nothing is pending anywhere; returns the count of
 * advances that delivered something. */
static uint32_t settle(struct vsr_sim *sim)
{
    uint32_t delivered = 0;

    for (;;) {
        int result = vsr_sim_advance(sim);

        if (result < 0) {
            return delivered;
        }
        if (result > 0) {
            ++delivered;
        }
    }
}

static int open_socket(struct vsr_sim *sim, struct vsr_io_executor ex)
{
    struct vsr_io_sqe sqe = make_sqe(VSR_IO_SQE_SOCKET, -1, 0x50);
    struct vsr_io_cqe cqe;

    sqe.length = AF_INET;
    sqe.op_flags = SOCK_STREAM;
    cqe = run(sim, ex, &sqe);
    CHECK(cqe.result >= 0);
    return cqe.result;
}

static int listen_on(struct vsr_sim *sim, struct vsr_io_executor ex,
                     uint32_t node, uint16_t port)
{
    int fd = open_socket(sim, ex);
    struct vsr_io_address address = vsr_sim_address(sim, node, port);
    struct vsr_io_sqe sqe = make_sqe(VSR_IO_SQE_BIND, fd, 0x51);

    sqe.addr = &address.sockaddr;
    sqe.length = address.length;
    CHECK(run(sim, ex, &sqe).result == 0);
    sqe = make_sqe(VSR_IO_SQE_LISTEN, fd, 0x52);
    sqe.length = 16;
    CHECK(run(sim, ex, &sqe).result == 0);
    return fd;
}

/* Connects a fresh socket of `ex` to node/port and returns the descriptor
 * with the CONNECT result in *result. */
static int connect_to(struct vsr_sim *sim, struct vsr_io_executor ex,
                      uint32_t node, uint16_t port, int32_t *result)
{
    int fd = open_socket(sim, ex);
    struct vsr_io_address address = vsr_sim_address(sim, node, port);
    struct vsr_io_sqe sqe = make_sqe(VSR_IO_SQE_CONNECT, fd, 0x53);

    sqe.addr = &address.sockaddr;
    sqe.length = address.length;
    *result = run(sim, ex, &sqe).result;
    return fd;
}

/* One connection from node `client` to the listener on node `server`. */
static void connect_pair(struct vsr_sim *sim, struct vsr_io_executor client,
                         struct vsr_io_executor server, int listener,
                         uint32_t server_node, int *client_fd, int *server_fd)
{
    struct vsr_io_sqe accept = make_sqe(VSR_IO_SQE_ACCEPT, listener, 0xAC);
    struct vsr_io_cqe cqe;
    int32_t result = -1;

    submit(server, &accept, 1);
    *client_fd = connect_to(sim, client, server_node, PORT, &result);
    CHECK(result == 0);
    cqe = wait_one(sim, server);
    CHECK(cqe.user_data == 0xAC && cqe.result >= 0);
    *server_fd = cqe.result;
}

static int32_t send_bytes(struct vsr_sim *sim, struct vsr_io_executor ex,
                          int fd, const void *bytes, uint32_t length)
{
    struct vsr_io_sqe sqe = make_sqe(VSR_IO_SQE_SEND, fd, 0x54);

    sqe.addr = bytes;
    sqe.length = length;
    return run(sim, ex, &sqe).result;
}

static int32_t recv_bytes(struct vsr_sim *sim, struct vsr_io_executor ex,
                          int fd, void *bytes, uint32_t length)
{
    struct vsr_io_sqe sqe = make_sqe(VSR_IO_SQE_RECV, fd, 0x55);

    sqe.addr = bytes;
    sqe.length = length;
    return run(sim, ex, &sqe).result;
}

static int32_t close_fd(struct vsr_sim *sim, struct vsr_io_executor ex, int fd)
{
    struct vsr_io_sqe sqe = make_sqe(VSR_IO_SQE_CLOSE, fd, 0x56);

    return run(sim, ex, &sqe).result;
}

static int32_t open_at(struct vsr_sim *sim, struct vsr_io_executor ex, int dir,
                       const char *path, uint32_t flags)
{
    struct vsr_io_sqe sqe = make_sqe(VSR_IO_SQE_OPENAT, dir, 0x57);

    sqe.addr = path;
    sqe.op_flags = flags;
    sqe.length = 0644;
    return run(sim, ex, &sqe).result;
}

static int32_t write_at(struct vsr_sim *sim, struct vsr_io_executor ex, int fd,
                        uint64_t offset, const void *bytes, uint32_t length)
{
    struct vsr_io_sqe sqe = make_sqe(VSR_IO_SQE_WRITE, fd, 0x58);

    sqe.offset = offset;
    sqe.addr = bytes;
    sqe.length = length;
    return run(sim, ex, &sqe).result;
}

static int32_t read_at(struct vsr_sim *sim, struct vsr_io_executor ex, int fd,
                       uint64_t offset, void *bytes, uint32_t length)
{
    struct vsr_io_sqe sqe = make_sqe(VSR_IO_SQE_READ, fd, 0x59);

    sqe.offset = offset;
    sqe.addr = bytes;
    sqe.length = length;
    return run(sim, ex, &sqe).result;
}

static int32_t fsync_fd(struct vsr_sim *sim, struct vsr_io_executor ex, int fd)
{
    struct vsr_io_sqe sqe = make_sqe(VSR_IO_SQE_FSYNC, fd, 0x5A);

    return run(sim, ex, &sqe).result;
}

static int32_t mkdir_at(struct vsr_sim *sim, struct vsr_io_executor ex,
                        const char *path)
{
    struct vsr_io_sqe sqe = make_sqe(VSR_IO_SQE_MKDIRAT, VSR_SIM_ROOT, 0x5B);

    sqe.addr = path;
    sqe.length = 0755;
    return run(sim, ex, &sqe).result;
}

static int32_t rename_at(struct vsr_sim *sim, struct vsr_io_executor ex,
                         const char *from, const char *to)
{
    struct vsr_io_sqe sqe = make_sqe(VSR_IO_SQE_RENAMEAT, VSR_SIM_ROOT, 0x5C);

    sqe.addr = from;
    sqe.addr2 = to;
    return run(sim, ex, &sqe).result;
}

static int32_t unlink_at(struct vsr_sim *sim, struct vsr_io_executor ex,
                         const char *path, uint32_t flags)
{
    struct vsr_io_sqe sqe = make_sqe(VSR_IO_SQE_UNLINKAT, VSR_SIM_ROOT, 0x5D);

    sqe.addr = path;
    sqe.op_flags = flags;
    return run(sim, ex, &sqe).result;
}

static int32_t statx_at(struct vsr_sim *sim, struct vsr_io_executor ex, int dir,
                        const char *path, uint32_t flags, struct statx *out)
{
    struct vsr_io_sqe sqe = make_sqe(VSR_IO_SQE_STATX, dir, 0x5E);

    sqe.addr = path;
    sqe.addr2 = out;
    sqe.op_flags = flags;
    sqe.length = STATX_BASIC_STATS;
    return run(sim, ex, &sqe).result;
}

static int32_t timeout_ns(struct vsr_sim *sim, struct vsr_io_executor ex,
                          uint64_t ns)
{
    struct vsr_io_sqe sqe = make_sqe(VSR_IO_SQE_TIMEOUT, -1, 0x5F);

    sqe.offset = ns;
    return run(sim, ex, &sqe).result;
}

static void fill(unsigned char *bytes, size_t size, unsigned char value)
{
    memset(bytes, value, size);
}

static bool all_bytes(const unsigned char *bytes, size_t size,
                      unsigned char value)
{
    for (size_t i = 0; i < size; ++i) {
        if (bytes[i] != value) {
            return false;
        }
    }
    return true;
}

/* ------------------------------------------------------------------------
 * Error vocabulary (decision 59)
 * --------------------------------------------------------------------- */

static void test_errors(void)
{
    struct vsr_sim_options options;
    struct vsr_sim *sim = NULL;
    uint64_t size = 0;
    uint32_t count = 0;
    char name[VSR_SIM_NAME_BYTES];
    struct vsr_io_executor ex;

    memset(&options, 0, sizeof(options));
    options.nodes = 1;
    CHECK(vsr_sim_create(&options, NULL) == -EINVAL);
    CHECK(vsr_sim_create(NULL, &sim) == -EINVAL && sim == NULL);
    options.nodes = 0;
    CHECK(vsr_sim_create(&options, &sim) == -EINVAL && sim == NULL);
    options.nodes = 1;
    options.block_bytes = 1000;
    CHECK(vsr_sim_create(&options, &sim) == -EINVAL);
    options.block_bytes = 0;
    options.faults.network.drop_ppm = 1000001;
    CHECK(vsr_sim_create(&options, &sim) == -EINVAL);
    options.faults.network.drop_ppm = 0;
    options.faults.disk.latency_min_ns = 2;
    options.faults.disk.latency_max_ns = 1;
    CHECK(vsr_sim_create(&options, &sim) == -EINVAL);
    options.faults.disk.latency_min_ns = 0;
    CHECK(vsr_sim_create(&options, &sim) == 0 && sim != NULL);

    /* Node bounds and the restart of a live node. */
    CHECK(vsr_sim_executor(sim, 1).ops == NULL);
    CHECK(vsr_sim_restart(sim, 0).ops == NULL);
    CHECK(vsr_sim_restart(sim, 1).ops == NULL);
    CHECK(vsr_sim_alive(sim, 0) == 1 && vsr_sim_alive(sim, 1) == 0);
    CHECK(vsr_sim_ready(sim, 1) == 0 && vsr_sim_inflight(sim, 1) == 0);
    CHECK(vsr_sim_address(sim, 1, PORT).length == 0);
    vsr_sim_crash(sim, 1); /* No-op. */
    vsr_sim_partition(sim, 0, 1, 1);
    vsr_sim_reset(sim, 0, 1);

    /* Inspection: bad node or argument, missing path, wrong kind. */
    CHECK(vsr_sim_file_size(sim, 1, "f", &size) == -EINVAL);
    CHECK(vsr_sim_file_size(sim, 0, NULL, &size) == -EINVAL);
    CHECK(vsr_sim_file_size(sim, 0, "f", NULL) == -EINVAL);
    CHECK(vsr_sim_file_size(sim, 0, "f", &size) == -ENOENT);
    CHECK(vsr_sim_file_size(sim, 0, "", &size) == -ENOENT);
    CHECK(vsr_sim_file_size(sim, 0, ".", &size) == -EISDIR);
    CHECK(vsr_sim_file_read(sim, 0, "f", 0, name, 1, NULL) == -EINVAL);
    CHECK(vsr_sim_file_corrupt(sim, 0, "f", 0, 1) == -ENOENT);
    CHECK(vsr_sim_dir_count(sim, 0, ".", NULL) == -EINVAL);
    CHECK(vsr_sim_dir_count(sim, 0, ".", &count) == 0 && count == 0);
    CHECK(vsr_sim_dir_count(sim, 0, "d", &count) == -ENOENT);
    CHECK(vsr_sim_dir_entry(sim, 0, ".", 0, NULL, 1, NULL) == -EINVAL);
    CHECK(vsr_sim_dir_entry(sim, 0, ".", 0, name, sizeof(name), NULL) ==
          -ENOENT);
    ex = vsr_sim_executor(sim, 0);
    CHECK(open_at(sim, ex, VSR_SIM_ROOT, "file", O_CREAT | O_RDWR) >= 0);
    CHECK(vsr_sim_dir_count(sim, 0, "file", &count) == -ENOTDIR);
    CHECK(vsr_sim_dir_entry(sim, 0, "file", 0, name, sizeof(name), NULL) ==
          -ENOTDIR);
    CHECK(vsr_sim_dir_entry(sim, 0, ".", 0, name, 2, NULL) == -EINVAL);
    CHECK(vsr_sim_dir_entry(sim, 0, ".", 0, name, 5, &size) == 0);
    CHECK(strcmp(name, "file") == 0 && size == 0);
    CHECK(vsr_sim_file_size(sim, 0, "file/x", &size) == -ENOTDIR);
    CHECK(vsr_sim_random_below(sim, 0) == 0);
    for (uint32_t i = 0; i < 64; ++i) {
        CHECK(vsr_sim_random_below(sim, 10) < 10);
    }
    vsr_sim_destroy(sim);
    vsr_sim_destroy(NULL);
}

/* ------------------------------------------------------------------------
 * Clock
 * --------------------------------------------------------------------- */

static void test_clock(void)
{
    struct vsr_sim_faults faults;
    struct vsr_sim *sim;
    struct vsr_io_executor ex[3];
    uint64_t offsets[3];
    uint64_t before;
    struct vsr_io_sqe sqe;
    struct vsr_io_cqe cqe;

    memset(&faults, 0, sizeof(faults));
    faults.clock.offset_max_ns = 1000 * MS;
    faults.clock.jitter_max_ns = 5 * MS;
    sim = make_world(seed, 3, &faults);
    CHECK(vsr_sim_now(sim) == 0);
    for (uint32_t i = 0; i < 3; ++i) {
        ex[i] = vsr_sim_executor(sim, i);
        CHECK(ex[i].ops != NULL);
        offsets[i] = ex[i].ops->now(ex[i].ctx);
        CHECK(offsets[i] <= 1000 * MS);
        CHECK(vsr_sim_ready(sim, i) == 1); /* Never waited. */
    }

    /* A relative timer fires late by at most the jitter, in node time. */
    before = ex[0].ops->now(ex[0].ctx);
    sqe = make_sqe(VSR_IO_SQE_TIMEOUT, -1, 1);
    sqe.offset = 10 * MS;
    CHECK(ex[0].ops->submit_and_wait(ex[0].ctx, &sqe, 1, 1, 0,
                                     VSR_NO_DEADLINE) == 0);
    CHECK(vsr_sim_ready(sim, 0) == 0);
    CHECK(vsr_sim_inflight(sim, 0) == 1);
    CHECK(vsr_sim_advance(sim) == 0); /* Moves the clock. */
    CHECK(vsr_sim_now(sim) >= 10 * MS && vsr_sim_now(sim) <= 15 * MS);
    CHECK(vsr_sim_advance(sim) == 1); /* Delivers the timer. */
    CHECK(vsr_sim_ready(sim, 0) == 1);
    CHECK(vsr_sim_inflight(sim, 0) == 0);
    CHECK(reap(ex[0], &cqe, 1) == 1);
    CHECK(cqe.user_data == 1 && cqe.result == -ETIME && cqe.flags == 0);
    CHECK(ex[0].ops->now(ex[0].ctx) >= before + 10 * MS);
    /* Every node's clock moved by the same amount. */
    for (uint32_t i = 0; i < 3; ++i) {
        CHECK(ex[i].ops->now(ex[i].ctx) == offsets[i] + vsr_sim_now(sim));
    }

    /* An absolute timer in the node's clock. */
    before = ex[1].ops->now(ex[1].ctx);
    sqe = make_sqe(VSR_IO_SQE_TIMEOUT, -1, 2);
    sqe.offset = before + 20 * MS;
    sqe.op_flags = VSR_IO_TIMEOUT_ABSOLUTE;
    cqe = run(sim, ex[1], &sqe);
    CHECK(cqe.result == -ETIME);
    CHECK(ex[1].ops->now(ex[1].ctx) >= before + 20 * MS);
    CHECK(ex[1].ops->now(ex[1].ctx) <= before + 25 * MS);
    /* Already due: fires at the next advance. */
    sqe.offset = 1;
    CHECK(run(sim, ex[1], &sqe).result == -ETIME);

    /* Waits: a deadline moves the clock; min_wait batches; wake. */
    before = vsr_sim_now(sim);
    CHECK(ex[2].ops->submit_and_wait(ex[2].ctx, NULL, 0, 1, 0,
                                     ex[2].ops->now(ex[2].ctx) + 7 * MS) == 0);
    CHECK(vsr_sim_ready(sim, 2) == 0);
    CHECK(vsr_sim_advance(sim) == 0);
    CHECK(vsr_sim_now(sim) == before + 7 * MS);
    CHECK(vsr_sim_ready(sim, 2) == 1);
    sqe = make_sqe(VSR_IO_SQE_NOP, -1, 3);
    before = vsr_sim_now(sim);
    CHECK(ex[2].ops->submit_and_wait(ex[2].ctx, &sqe, 1, 2, 3 * MS,
                                     VSR_NO_DEADLINE) == 0);
    CHECK(vsr_sim_ready(sim, 2) == 0); /* One of two, min_wait not over. */
    CHECK(vsr_sim_advance(sim) == 0);
    CHECK(vsr_sim_now(sim) == before + 3 * MS);
    CHECK(vsr_sim_ready(sim, 2) == 1);
    CHECK(reap(ex[2], &cqe, 1) == 1 && cqe.user_data == 3 && cqe.result == 0);
    CHECK(ex[2].ops->submit_and_wait(ex[2].ctx, NULL, 0, 1, 0,
                                     VSR_NO_DEADLINE) == 0);
    CHECK(vsr_sim_ready(sim, 2) == 0);
    CHECK(vsr_sim_advance(sim) == -1);
    ex[2].ops->wake(ex[2].ctx);
    CHECK(vsr_sim_ready(sim, 2) == 1);
    CHECK(reap(ex[2], &cqe, 1) == 0); /* Consumes the wake. */
    CHECK(vsr_sim_ready(sim, 2) == 0);

    /* random fills from the generator. */
    {
        unsigned char bytes[13];
        unsigned char zeros[13];

        memset(bytes, 0, sizeof(bytes));
        memset(zeros, 0, sizeof(zeros));
        ex[0].ops->random(ex[0].ctx, bytes, sizeof(bytes));
        CHECK(memcmp(bytes, zeros, sizeof(bytes)) != 0);
    }
    vsr_sim_destroy(sim);
}

/* ------------------------------------------------------------------------
 * Advance: (due, sequence) order, counts, and the -1 return
 * --------------------------------------------------------------------- */

static void test_advance(void)
{
    struct vsr_sim *sim = make_world(seed, 2, NULL);
    struct vsr_io_executor ex0 = vsr_sim_executor(sim, 0);
    struct vsr_io_executor ex1 = vsr_sim_executor(sim, 1);
    struct vsr_io_sqe sqes[4];
    struct vsr_io_cqe cqes[4];
    uint64_t target;

    /* Four timers: 30, 10, 10, 20 ms on node 0; 15 ms on node 1. */
    sqes[0] = make_sqe(VSR_IO_SQE_TIMEOUT, -1, 1);
    sqes[0].offset = 30 * MS;
    sqes[1] = make_sqe(VSR_IO_SQE_TIMEOUT, -1, 2);
    sqes[1].offset = 10 * MS;
    sqes[2] = make_sqe(VSR_IO_SQE_TIMEOUT, -1, 3);
    sqes[2].offset = 10 * MS;
    sqes[3] = make_sqe(VSR_IO_SQE_TIMEOUT, -1, 4);
    sqes[3].offset = 20 * MS;
    submit(ex0, sqes, 4);
    sqes[0] = make_sqe(VSR_IO_SQE_TIMEOUT, -1, 5);
    sqes[0].offset = 15 * MS;
    submit(ex1, sqes, 1);
    CHECK(vsr_sim_inflight(sim, 0) == 4 && vsr_sim_inflight(sim, 1) == 1);

    CHECK(vsr_sim_advance(sim) == 0 && vsr_sim_now(sim) == 10 * MS);
    CHECK(vsr_sim_advance(sim) == 2); /* Both 10 ms timers, in order. */
    CHECK(reap(ex0, cqes, 4) == 2);
    CHECK(cqes[0].user_data == 2 && cqes[1].user_data == 3);
    CHECK(cqes[0].result == -ETIME && cqes[1].result == -ETIME);
    CHECK(vsr_sim_advance(sim) == 0 && vsr_sim_now(sim) == 15 * MS);
    CHECK(vsr_sim_advance(sim) == 1);
    CHECK(reap(ex1, cqes, 4) == 1 && cqes[0].user_data == 5);
    CHECK(reap(ex0, cqes, 4) == 0);
    CHECK(vsr_sim_advance(sim) == 0 && vsr_sim_now(sim) == 20 * MS);
    CHECK(vsr_sim_advance(sim) == 1);
    CHECK(reap(ex0, cqes, 4) == 1 && cqes[0].user_data == 4);
    CHECK(vsr_sim_advance(sim) == 0 && vsr_sim_now(sim) == 30 * MS);
    CHECK(vsr_sim_advance(sim) == 1);
    CHECK(reap(ex0, cqes, 4) == 1 && cqes[0].user_data == 1);
    CHECK(vsr_sim_advance(sim) == -1 && vsr_sim_now(sim) == 30 * MS);

    /* A cancelled one-hour timer is not pending: advance returns -1 at
     * once and the clock does not move. */
    sqes[0] = make_sqe(VSR_IO_SQE_TIMEOUT, -1, 6);
    sqes[0].offset = 3600 * (1000 * MS);
    submit(ex0, sqes, 1);
    sqes[0] = make_sqe(VSR_IO_SQE_CANCEL, -1, 7);
    sqes[0].offset = 6;
    submit(ex0, sqes, 1);
    CHECK(reap(ex0, cqes, 4) == 2);
    CHECK(cqes[0].user_data == 6 && cqes[0].result == -ECANCELED);
    CHECK(cqes[1].user_data == 7 && cqes[1].result == 0);
    CHECK(vsr_sim_advance(sim) == -1 && vsr_sim_now(sim) == 30 * MS);
    /* Cancelling again: nothing to cancel. */
    submit(ex0, sqes, 1);
    CHECK(reap(ex0, cqes, 4) == 1 && cqes[0].result == -ENOENT);

    /* A timer superseded by TIMEOUT_UPDATE fires at the new time only. */
    sqes[0] = make_sqe(VSR_IO_SQE_TIMEOUT, -1, 8);
    sqes[0].offset = 3600 * (1000 * MS);
    submit(ex0, sqes, 1);
    target = 8;
    sqes[0] = make_sqe(VSR_IO_SQE_TIMEOUT_UPDATE, -1, 9);
    sqes[0].addr2 = &target;
    sqes[0].offset = 5 * MS;
    submit(ex0, sqes, 1);
    CHECK(reap(ex0, cqes, 4) == 1 && cqes[0].user_data == 9 &&
          cqes[0].result == 0);
    CHECK(vsr_sim_advance(sim) == 0 && vsr_sim_now(sim) == 35 * MS);
    CHECK(vsr_sim_advance(sim) == 1);
    CHECK(reap(ex0, cqes, 4) == 1 && cqes[0].user_data == 8 &&
          cqes[0].result == -ETIME);
    CHECK(vsr_sim_advance(sim) == -1 && vsr_sim_now(sim) == 35 * MS);
    target = 42;
    submit(ex0, sqes, 1);
    CHECK(reap(ex0, cqes, 4) == 1 && cqes[0].result == -ENOENT);

    /* A crashed node's timer is not pending either. */
    sqes[0] = make_sqe(VSR_IO_SQE_TIMEOUT, -1, 10);
    sqes[0].offset = 3600 * (1000 * MS);
    submit(ex1, sqes, 1);
    vsr_sim_crash(sim, 1);
    CHECK(vsr_sim_advance(sim) == -1 && vsr_sim_now(sim) == 35 * MS);
    CHECK(vsr_sim_inflight(sim, 1) == 0);
    ex1 = vsr_sim_restart(sim, 1);
    CHECK(ex1.ops != NULL);
    CHECK(timeout_ns(sim, ex1, 1 * MS) == -ETIME);
    CHECK(vsr_sim_now(sim) == 36 * MS);
    vsr_sim_destroy(sim);
}

/* ------------------------------------------------------------------------
 * Partitions
 * --------------------------------------------------------------------- */

static void test_partitions(void)
{
    struct vsr_sim_faults faults;
    struct vsr_sim *sim;
    struct vsr_io_executor ex0;
    struct vsr_io_executor ex1;
    int listener;
    int client;
    int server;
    int32_t result;
    unsigned char bytes[64];
    struct vsr_io_sqe sqe;
    struct vsr_io_cqe cqe;
    uint64_t before;

    memset(&faults, 0, sizeof(faults));
    faults.network.delay_min_ns = 1 * MS;
    faults.network.delay_max_ns = 2 * MS;
    faults.network.stall_reset_ns = 50 * MS;
    faults.network.connect_timeout_ns = 100 * MS;
    sim = make_world(seed, 3, &faults);
    ex0 = vsr_sim_executor(sim, 0);
    ex1 = vsr_sim_executor(sim, 1);
    listener = listen_on(sim, ex1, 1, PORT);
    connect_pair(sim, ex0, ex1, listener, 1, &client, &server);

    /* Bytes flow with the configured delay. */
    before = vsr_sim_now(sim);
    CHECK(send_bytes(sim, ex0, client, "hello", 5) == 5);
    CHECK(recv_bytes(sim, ex1, server, bytes, sizeof(bytes)) == 5);
    CHECK(memcmp(bytes, "hello", 5) == 0);
    CHECK(vsr_sim_now(sim) >= before + 1 * MS);
    CHECK(vsr_sim_now(sim) <= before + 2 * MS);

    /* A partition stalls the connection, then resets it. */
    vsr_sim_partition(sim, 0, 1, 1);
    CHECK(send_bytes(sim, ex0, client, "lost", 4) == 4);
    before = vsr_sim_now(sim);
    sqe = make_sqe(VSR_IO_SQE_RECV, server, 0x60);
    sqe.addr = bytes;
    sqe.length = sizeof(bytes);
    cqe = run(sim, ex1, &sqe);
    CHECK(cqe.result == -ECONNRESET);
    CHECK(vsr_sim_now(sim) == before + 50 * MS);
    CHECK(recv_bytes(sim, ex0, client, bytes, sizeof(bytes)) == -ECONNRESET);
    CHECK(send_bytes(sim, ex0, client, "x", 1) == -ECONNRESET);
    CHECK(close_fd(sim, ex0, client) == 0);
    CHECK(close_fd(sim, ex1, server) == 0);

    /* Across the partition a connect is unreachable after the timeout. */
    before = vsr_sim_now(sim);
    client = connect_to(sim, ex0, 1, PORT, &result);
    CHECK(result == -EHOSTUNREACH);
    CHECK(vsr_sim_now(sim) == before + 100 * MS);
    CHECK(close_fd(sim, ex0, client) == 0);
    /* A host that does not exist is unreachable as well. */
    before = vsr_sim_now(sim);
    client = connect_to(sim, ex0, 2, PORT, &result);
    CHECK(result == -ECONNREFUSED); /* Node 2 is up, nothing listens. */
    CHECK(vsr_sim_now(sim) == before);
    CHECK(close_fd(sim, ex0, client) == 0);
    vsr_sim_crash(sim, 2);
    client = connect_to(sim, ex0, 2, PORT, &result);
    CHECK(result == -EHOSTUNREACH);
    CHECK(vsr_sim_now(sim) == before + 100 * MS);
    CHECK(close_fd(sim, ex0, client) == 0);

    /* Healed: refused where nothing listens, accepted where it does. */
    vsr_sim_partition(sim, 0, 1, 0);
    client = connect_to(sim, ex0, 1, PORT + 1, &result);
    CHECK(result == -ECONNREFUSED);
    CHECK(close_fd(sim, ex0, client) == 0);
    connect_pair(sim, ex0, ex1, listener, 1, &client, &server);
    CHECK(send_bytes(sim, ex0, client, "again", 5) == 5);
    CHECK(recv_bytes(sim, ex1, server, bytes, sizeof(bytes)) == 5);

    /* An explicit reset ends the connection at once, both ways. */
    vsr_sim_reset(sim, 1, 0);
    CHECK(recv_bytes(sim, ex1, server, bytes, sizeof(bytes)) == -ECONNRESET);
    CHECK(recv_bytes(sim, ex0, client, bytes, sizeof(bytes)) == -ECONNRESET);
    CHECK(close_fd(sim, ex0, client) == 0);
    CHECK(close_fd(sim, ex1, server) == 0);

    /* Isolation is a partition from everyone; a dropped segment stalls
     * the connection the same way a partition does. */
    connect_pair(sim, ex0, ex1, listener, 1, &client, &server);
    vsr_sim_isolate(sim, 1, 1);
    before = vsr_sim_now(sim);
    CHECK(send_bytes(sim, ex0, client, "cut", 3) == 3);
    CHECK(recv_bytes(sim, ex0, client, bytes, sizeof(bytes)) == -ECONNRESET);
    CHECK(vsr_sim_now(sim) == before + 50 * MS);
    vsr_sim_isolate(sim, 1, 0);
    CHECK(close_fd(sim, ex0, client) == 0);
    CHECK(close_fd(sim, ex1, server) == 0);
    faults.network.drop_ppm = 1000000;
    vsr_sim_set_faults(sim, &faults);
    connect_pair(sim, ex0, ex1, listener, 1, &client, &server);
    before = vsr_sim_now(sim);
    CHECK(send_bytes(sim, ex0, client, "drop", 4) == 4);
    CHECK(recv_bytes(sim, ex1, server, bytes, sizeof(bytes)) == -ECONNRESET);
    CHECK(vsr_sim_now(sim) == before + 50 * MS);

    /* Graceful close: the peer reads what was sent, then end of stream. */
    faults.network.drop_ppm = 0;
    vsr_sim_set_faults(sim, &faults);
    CHECK(close_fd(sim, ex0, client) == 0);
    CHECK(close_fd(sim, ex1, server) == 0);
    connect_pair(sim, ex0, ex1, listener, 1, &client, &server);
    CHECK(send_bytes(sim, ex0, client, "bye", 3) == 3);
    CHECK(close_fd(sim, ex0, client) == 0);
    CHECK(recv_bytes(sim, ex1, server, bytes, sizeof(bytes)) == 3);
    CHECK(recv_bytes(sim, ex1, server, bytes, sizeof(bytes)) == 0);
    /* Data reaching the closed end answers with a reset. */
    CHECK(send_bytes(sim, ex1, server, "late", 4) == 4);
    CHECK(settle(sim) == 1);
    CHECK(send_bytes(sim, ex1, server, "late", 4) == -ECONNRESET);
    CHECK(close_fd(sim, ex1, server) == 0);
    CHECK(close_fd(sim, ex1, listener) == 0);
    (void)settle(sim);
    vsr_sim_destroy(sim);
}

/* ------------------------------------------------------------------------
 * Decision 58: CLOSE never completes pending records; rings
 * --------------------------------------------------------------------- */

static _Alignas(4096) unsigned char ring_memory[4096];

static void test_close_holds(void)
{
    struct vsr_sim_faults faults;
    struct vsr_sim *sim;
    struct vsr_io_executor ex0;
    struct vsr_io_executor ex1;
    int listener;
    int client;
    int server;
    int32_t result;
    unsigned char bytes[64];
    struct vsr_io_sqe sqe;
    struct vsr_io_cqe cqe;
    struct vsr_io_cqe cqes[2];

    memset(&faults, 0, sizeof(faults));
    faults.network.delay_min_ns = 1 * MS;
    faults.network.delay_max_ns = 1 * MS;
    faults.network.connect_timeout_ns = 100 * MS;
    sim = make_world(seed, 2, &faults);
    ex0 = vsr_sim_executor(sim, 0);
    ex1 = vsr_sim_executor(sim, 1);
    listener = listen_on(sim, ex1, 1, PORT);
    connect_pair(sim, ex0, ex1, listener, 1, &client, &server);

    /* A parked RECV survives the CLOSE of its descriptor and still
     * receives; the socket dies (FIN) once the record completes. */
    sqe = make_sqe(VSR_IO_SQE_RECV, server, 0x70);
    sqe.addr = bytes;
    sqe.length = sizeof(bytes);
    submit(ex1, &sqe, 1);
    CHECK(close_fd(sim, ex1, server) == 0);
    CHECK(settle(sim) == 0);
    CHECK(reap(ex1, &cqe, 1) == 0 && vsr_sim_inflight(sim, 1) == 1);
    CHECK(send_bytes(sim, ex0, client, "held", 4) == 4);
    cqe = wait_one(sim, ex1);
    CHECK(cqe.user_data == 0x70 && cqe.result == 4);
    CHECK(memcmp(bytes, "held", 4) == 0);
    CHECK(recv_bytes(sim, ex0, client, bytes, sizeof(bytes)) == 0);
    CHECK(close_fd(sim, ex0, client) == 0);

    /* Cancelling by a closed descriptor is -EBADF; by user_data works and
     * the cancellation, not the close, ends the record. The cancelled
     * record's completion precedes the CANCEL's own. */
    connect_pair(sim, ex0, ex1, listener, 1, &client, &server);
    sqe = make_sqe(VSR_IO_SQE_RECV, server, 0x71);
    sqe.addr = bytes;
    sqe.length = sizeof(bytes);
    submit(ex1, &sqe, 1);
    sqe = make_sqe(VSR_IO_SQE_CANCEL, server, 0x72);
    sqe.op_flags = VSR_IO_CANCEL_BY_FD | VSR_IO_CANCEL_ALL;
    submit(ex1, &sqe, 1);
    CHECK(reap(ex1, cqes, 2) == 2);
    CHECK(cqes[0].user_data == 0x71 && cqes[0].result == -ECANCELED);
    CHECK(cqes[1].user_data == 0x72 && cqes[1].result == 1);
    sqe = make_sqe(VSR_IO_SQE_RECV, server, 0x73);
    sqe.addr = bytes;
    sqe.length = sizeof(bytes);
    submit(ex1, &sqe, 1);
    CHECK(close_fd(sim, ex1, server) == 0);
    sqe = make_sqe(VSR_IO_SQE_CANCEL, server, 0x74);
    sqe.op_flags = VSR_IO_CANCEL_BY_FD;
    CHECK(run(sim, ex1, &sqe).result == -EBADF);
    sqe = make_sqe(VSR_IO_SQE_CANCEL, -1, 0x75);
    sqe.offset = 0x73;
    submit(ex1, &sqe, 1);
    CHECK(reap(ex1, cqes, 2) == 2);
    CHECK(cqes[0].user_data == 0x73 && cqes[0].result == -ECANCELED);
    CHECK(cqes[1].user_data == 0x75 && cqes[1].result == 0);
    CHECK(recv_bytes(sim, ex0, client, bytes, sizeof(bytes)) == 0);
    CHECK(close_fd(sim, ex0, client) == 0);

    /* A multishot ACCEPT keeps its listener alive past the CLOSE; a
     * connect still succeeds; cancelling it frees the name. */
    sqe = make_sqe(VSR_IO_SQE_ACCEPT, listener, 0x76);
    sqe.op_flags = VSR_IO_ACCEPT_MULTISHOT;
    submit(ex1, &sqe, 1);
    CHECK(close_fd(sim, ex1, listener) == 0);
    client = connect_to(sim, ex0, 1, PORT, &result);
    CHECK(result == 0);
    cqe = wait_one(sim, ex1);
    CHECK(cqe.user_data == 0x76 && cqe.result >= 0 &&
          (cqe.flags & VSR_IO_CQE_MORE) != 0);
    server = cqe.result;
    sqe = make_sqe(VSR_IO_SQE_CANCEL, -1, 0x77);
    sqe.offset = 0x76;
    submit(ex1, &sqe, 1);
    CHECK(reap(ex1, cqes, 2) == 2);
    CHECK(cqes[0].user_data == 0x76 && cqes[0].result == -ECANCELED &&
          cqes[0].flags == 0);
    CHECK(cqes[1].user_data == 0x77 && cqes[1].result == 0);
    CHECK(close_fd(sim, ex1, server) == 0);
    CHECK(close_fd(sim, ex0, client) == 0);
    client = connect_to(sim, ex0, 1, PORT, &result);
    CHECK(result == -ECONNREFUSED);
    CHECK(close_fd(sim, ex0, client) == 0);
    /* The name is free again. */
    listener = listen_on(sim, ex1, 1, PORT);

    /* Registration before the tables exist is -EINVAL. */
    {
        struct vsr_io_region region = {.base = bytes, .size = sizeof(bytes)};

        CHECK(ex1.ops->update_file(ex1.ctx, 0, listener) == -EINVAL);
        CHECK(ex1.ops->update_buffer(ex1.ctx, 0, &region) == -EINVAL);
        CHECK(ex1.ops->register_files(ex1.ctx, 4) == 0);
        CHECK(ex1.ops->register_buffers(ex1.ctx, 2) == 0);
        CHECK(ex1.ops->update_file(ex1.ctx, 4, listener) == -EINVAL);
        CHECK(ex1.ops->update_buffer(ex1.ctx, 2, &region) == -EINVAL);
        CHECK(ex1.ops->update_file(ex1.ctx, 0, 99) == -EBADF);
        CHECK(ex1.ops->update_buffer(ex1.ctx, 0, &region) == 0);
        CHECK(ex1.ops->update_buffer(ex1.ctx, 0, NULL) == 0);
    }

    /* Provided buffers: an empty ring parks the receive and fails it at
     * the delivery; unregistering under a pending receive is allowed and
     * ends it with -ENOBUFS at its next delivery. */
    {
        struct vsr_io_region memory = {.base = ring_memory,
                                       .size = sizeof(ring_memory)};
        static unsigned char provided[2][8];
        struct vsr_io_buffer buffers[2] = {
            {.base = provided[0], .length = 8, .id = 10},
            {.base = provided[1], .length = 8, .id = 11}};

        CHECK(ex1.ops->buffer_ring(ex1.ctx, 1, 4, 0, &memory) == 0);
        CHECK(ex1.ops->buffer_ring(ex1.ctx, 1, 4, 0, &memory) == -EEXIST);
        CHECK(ex1.ops->buffer_ring(ex1.ctx, 2, 0, 0, NULL) == -ENOENT);
        connect_pair(sim, ex0, ex1, listener, 1, &client, &server);
        sqe = make_sqe(VSR_IO_SQE_RECV, server, 0x78);
        sqe.flags = VSR_IO_SQE_BUFFER_SELECT;
        sqe.buffer_group = 1;
        sqe.op_flags = VSR_IO_RECV_MULTISHOT;
        submit(ex1, &sqe, 1);
        CHECK(settle(sim) == 0);
        CHECK(reap(ex1, &cqe, 1) == 0); /* Parked, not -ENOBUFS. */
        CHECK(send_bytes(sim, ex0, client, "0123456789abcdefghij", 20) == 20);
        cqe = wait_one(sim, ex1);
        CHECK(cqe.user_data == 0x78 && cqe.result == -ENOBUFS &&
              cqe.flags == 0);
        /* The bytes are still queued; two buffers take 16 of them. */
        CHECK(ex1.ops->provide(ex1.ctx, 1, buffers, 2) == 0);
        submit(ex1, &sqe, 1);
        cqe = wait_one(sim, ex1);
        CHECK(cqe.user_data == 0x78 && cqe.result == 8);
        CHECK(cqe.flags == (VSR_IO_CQE_MORE | VSR_IO_CQE_BUFFER));
        CHECK(cqe.buffer_id == 10 && memcmp(provided[0], "01234567", 8) == 0);
        cqe = wait_one(sim, ex1);
        CHECK(cqe.result == 8 && cqe.buffer_id == 11);
        CHECK(memcmp(provided[1], "89abcdef", 8) == 0);
        cqe = wait_one(sim, ex1);
        CHECK(cqe.result == -ENOBUFS && cqe.flags == 0);
        /* Pending again with a buffer; unregister; the next delivery
         * terminates the receive and the bytes stay queued. */
        CHECK(ex1.ops->provide(ex1.ctx, 1, buffers, 1) == 0);
        submit(ex1, &sqe, 1);
        cqe = wait_one(sim, ex1);
        CHECK(cqe.result == 4 && cqe.buffer_id == 10);
        CHECK(memcmp(provided[0], "ghij", 4) == 0);
        CHECK(ex1.ops->provide(ex1.ctx, 1, buffers, 1) == 0);
        CHECK(ex1.ops->buffer_ring(ex1.ctx, 1, 0, 0, NULL) == 0);
        CHECK(settle(sim) == 0);
        CHECK(reap(ex1, &cqe, 1) == 0 && vsr_sim_inflight(sim, 1) == 1);
        CHECK(send_bytes(sim, ex0, client, "more", 4) == 4);
        cqe = wait_one(sim, ex1);
        CHECK(cqe.user_data == 0x78 && cqe.result == -ENOBUFS &&
              cqe.flags == 0);
        CHECK(recv_bytes(sim, ex1, server, bytes, sizeof(bytes)) == 4);
        CHECK(memcmp(bytes, "more", 4) == 0);

        /* Incremental consumption: one 16-byte buffer over three
         * deliveries; BUFFER_MORE while it stays at the head. */
        CHECK(ex1.ops->buffer_ring(
                  ex1.ctx, 1, 4, VSR_IO_BUFFER_RING_INCREMENTAL, &memory) == 0);
        buffers[0].length = 16;
        buffers[0].base = bytes;
        CHECK(ex1.ops->provide(ex1.ctx, 1, buffers, 1) == 0);
        submit(ex1, &sqe, 1);
        CHECK(send_bytes(sim, ex0, client, "abcde", 5) == 5);
        cqe = wait_one(sim, ex1);
        CHECK(cqe.result == 5 && cqe.buffer_id == 10);
        CHECK(cqe.flags ==
              (VSR_IO_CQE_MORE | VSR_IO_CQE_BUFFER | VSR_IO_CQE_BUFFER_MORE));
        CHECK(send_bytes(sim, ex0, client, "fghij", 5) == 5);
        cqe = wait_one(sim, ex1);
        CHECK(cqe.result == 5 && (cqe.flags & VSR_IO_CQE_BUFFER_MORE) != 0);
        CHECK(memcmp(bytes, "abcdefghij", 10) == 0);
        CHECK(send_bytes(sim, ex0, client, "klmnopqrstuv", 12) == 12);
        cqe = wait_one(sim, ex1);
        CHECK(cqe.result == 6 &&
              cqe.flags == (VSR_IO_CQE_MORE | VSR_IO_CQE_BUFFER));
        CHECK(memcmp(bytes, "abcdefghijklmnop", 16) == 0);
        cqe = wait_one(sim, ex1);
        CHECK(cqe.result == -ENOBUFS);
        CHECK(recv_bytes(sim, ex1, server, bytes, sizeof(bytes)) == 6);
        CHECK(memcmp(bytes, "qrstuv", 6) == 0);
        CHECK(ex1.ops->buffer_ring(ex1.ctx, 1, 0, 0, NULL) == 0);
    }

    /* Zero-copy send: result with MORE, then NOTIF once delivered. */
    {
        static unsigned char payload[100];

        fill(payload, sizeof(payload), 0x5a);
        sqe = make_sqe(VSR_IO_SQE_SEND, client, 0x79);
        sqe.addr = payload;
        sqe.length = sizeof(payload);
        sqe.op_flags = VSR_IO_SEND_ZERO_COPY;
        submit(ex0, &sqe, 1);
        cqe = wait_one(sim, ex0);
        CHECK(cqe.user_data == 0x79 && cqe.result == 100 &&
              cqe.flags == VSR_IO_CQE_MORE);
        CHECK(vsr_sim_inflight(sim, 0) == 1);
        cqe = wait_one(sim, ex0);
        CHECK(cqe.user_data == 0x79 && cqe.result == 0 &&
              cqe.flags == VSR_IO_CQE_NOTIF);
        CHECK(vsr_sim_inflight(sim, 0) == 0);
        CHECK(recv_bytes(sim, ex1, server, bytes, sizeof(bytes)) == 64);
        CHECK(recv_bytes(sim, ex1, server, bytes, sizeof(bytes)) == 36);
    }

    /* LINK: a failing head cancels its chain; a succeeding one runs it. */
    {
        struct vsr_io_sqe chain[3];
        struct vsr_io_cqe results[3];

        chain[0] = make_sqe(VSR_IO_SQE_NOP, -1, 0x7A);
        chain[0].flags = VSR_IO_SQE_LINK;
        chain[1] = make_sqe(VSR_IO_SQE_CLOSE, 999, 0x7B);
        chain[1].flags = VSR_IO_SQE_LINK;
        chain[2] = make_sqe(VSR_IO_SQE_NOP, -1, 0x7C);
        submit(ex0, chain, 3);
        CHECK(reap(ex0, results, 3) == 3);
        CHECK(results[0].user_data == 0x7A && results[0].result == 0);
        CHECK(results[1].user_data == 0x7B && results[1].result == -EBADF);
        CHECK(results[2].user_data == 0x7C && results[2].result == -ECANCELED);
        chain[1] = make_sqe(VSR_IO_SQE_NOP, -1, 0x7B);
        chain[1].flags = VSR_IO_SQE_LINK | VSR_IO_SQE_SKIP_SUCCESS;
        submit(ex0, chain, 3);
        CHECK(reap(ex0, results, 3) == 2);
        CHECK(results[0].user_data == 0x7A && results[1].user_data == 0x7C);
    }
    CHECK(close_fd(sim, ex0, client) == 0);
    CHECK(close_fd(sim, ex1, server) == 0);
    CHECK(close_fd(sim, ex1, listener) == 0);
    (void)settle(sim);
    vsr_sim_destroy(sim);
}

/* ------------------------------------------------------------------------
 * Crash model
 * --------------------------------------------------------------------- */

struct torn_count {
    uint32_t torn;
    uint32_t crashes;
};

static void count_torn(void *ctx, const struct vsr_sim_trace_event *event)
{
    struct torn_count *count = ctx;

    if (event->kind == VSR_SIM_TRACE_TORN) {
        ++count->torn;
    } else if (event->kind == VSR_SIM_TRACE_CRASH) {
        ++count->crashes;
    }
}

/* Four blocks A B C D: A and B synced, C and D not; then B rewritten
 * unsynced. Returns what survives with the given keep rate. */
static void crash_scenario(uint32_t keep_ppm, bool expect_keep)
{
    struct vsr_sim_faults faults;
    struct vsr_sim *sim;
    struct vsr_io_executor ex;
    struct vsr_sim_trace trace;
    struct torn_count count;
    static unsigned char blocks[4][BLOCK];
    static unsigned char again[BLOCK];
    unsigned char got[BLOCK];
    uint64_t size = 0;
    size_t read = 0;
    int fd;

    memset(&faults, 0, sizeof(faults));
    faults.disk.unsynced_keep_ppm = keep_ppm;
    faults.disk.latency_min_ns = 1 * MS;
    faults.disk.latency_max_ns = 2 * MS;
    faults.disk.fsync_min_ns = 3 * MS;
    faults.disk.fsync_max_ns = 4 * MS;
    memset(&count, 0, sizeof(count));
    trace.ctx = &count;
    trace.event = count_torn;
    sim = make_world(seed, 1, &faults);
    vsr_sim_set_trace(sim, &trace);
    ex = vsr_sim_executor(sim, 0);
    for (uint32_t b = 0; b < 4; ++b) {
        fill(blocks[b], BLOCK, (unsigned char)(0xA1 + 0x11 * b));
    }
    fill(again, BLOCK, 0xEE);
    fd = open_at(sim, ex, VSR_SIM_ROOT, "log", O_CREAT | O_RDWR);
    CHECK(fd >= 0);
    CHECK(write_at(sim, ex, fd, 0, blocks[0], BLOCK) == (int32_t)BLOCK);
    CHECK(write_at(sim, ex, fd, BLOCK, blocks[1], BLOCK) == (int32_t)BLOCK);
    CHECK(fsync_fd(sim, ex, fd) == 0);
    CHECK(write_at(sim, ex, fd, 2 * BLOCK, blocks[2], BLOCK) == (int32_t)BLOCK);
    CHECK(write_at(sim, ex, fd, 3 * BLOCK, blocks[3], BLOCK) == (int32_t)BLOCK);
    CHECK(write_at(sim, ex, fd, BLOCK, again, BLOCK) == (int32_t)BLOCK);

    /* Before the crash: everything reads as written; the durable view
     * shows the synced prefix only. */
    CHECK(vsr_sim_file_size(sim, 0, "log", &size) == 0 && size == 4 * BLOCK);
    CHECK(vsr_sim_file_read(sim, 0, "log", BLOCK, got, BLOCK, &read) == 0);
    CHECK(read == BLOCK && all_bytes(got, BLOCK, 0xEE));
    CHECK(vsr_sim_file_read(sim, 0, "log", 3 * BLOCK, got, BLOCK, &read) == 0);
    CHECK(read == BLOCK && memcmp(got, blocks[3], BLOCK) == 0);
    CHECK(vsr_sim_file_read_durable(sim, 0, "log", 0, got, BLOCK, &read) == 0);
    CHECK(read == BLOCK && memcmp(got, blocks[0], BLOCK) == 0);
    CHECK(vsr_sim_file_read_durable(sim, 0, "log", BLOCK, got, BLOCK, &read) ==
          0);
    CHECK(read == BLOCK && memcmp(got, blocks[1], BLOCK) == 0);
    CHECK(vsr_sim_file_read_durable(sim, 0, "log", 2 * BLOCK, got, BLOCK,
                                    &read) == 0);
    CHECK(read == BLOCK && all_bytes(got, BLOCK, 0));

    vsr_sim_crash(sim, 0);
    CHECK(count.crashes == 1);
    CHECK(vsr_sim_alive(sim, 0) == 0);
    CHECK(vsr_sim_file_size(sim, 0, "log", &size) == 0 && size == 4 * BLOCK);
    CHECK(vsr_sim_file_read(sim, 0, "log", 0, got, BLOCK, &read) == 0);
    CHECK(read == BLOCK && memcmp(got, blocks[0], BLOCK) == 0);
    CHECK(vsr_sim_file_read(sim, 0, "log", BLOCK, got, BLOCK, &read) == 0);
    if (expect_keep) {
        CHECK(count.torn == 0);
        CHECK(all_bytes(got, BLOCK, 0xEE));
    } else {
        CHECK(count.torn == 3);
        CHECK(memcmp(got, blocks[1], BLOCK) == 0); /* Back to synced. */
    }
    for (uint32_t b = 2; b < 4; ++b) {
        CHECK(vsr_sim_file_read(sim, 0, "log", b * BLOCK, got, BLOCK, &read) ==
              0);
        CHECK(read == BLOCK);
        CHECK(expect_keep ? memcmp(got, blocks[b], BLOCK) == 0
                          : all_bytes(got, BLOCK, 0));
    }
    /* What survived is durable now, and the executor reads it back. */
    ex = vsr_sim_restart(sim, 0);
    CHECK(ex.ops != NULL && vsr_sim_alive(sim, 0) == 1);
    fd = open_at(sim, ex, VSR_SIM_ROOT, "log", O_RDWR);
    CHECK(fd >= 0);
    CHECK(read_at(sim, ex, fd, 2 * BLOCK, got, BLOCK) == (int32_t)BLOCK);
    CHECK(expect_keep ? memcmp(got, blocks[2], BLOCK) == 0
                      : all_bytes(got, BLOCK, 0));
    CHECK(read_at(sim, ex, fd, 4 * BLOCK, got, BLOCK) == 0);
    CHECK(vsr_sim_file_read_durable(sim, 0, "log", 3 * BLOCK, got, BLOCK,
                                    &read) == 0);
    CHECK(expect_keep ? memcmp(got, blocks[3], BLOCK) == 0
                      : all_bytes(got, BLOCK, 0));
    vsr_sim_destroy(sim);
}

static void test_crash_model(void)
{
    struct vsr_sim_faults faults;
    struct vsr_sim *sim;
    struct vsr_io_executor ex;
    struct vsr_sim_trace trace;
    struct torn_count count;
    static unsigned char blocks[8][BLOCK];
    unsigned char got[BLOCK];
    struct vsr_io_sqe sqes[2];
    struct vsr_io_cqe cqes[2];
    size_t read = 0;
    uint64_t size = 0;
    uint32_t zero = 0;
    int fd;

    crash_scenario(0, false);
    crash_scenario(1000000, true);

    /* Torn writes at block granularity: one eight-block write, half the
     * blocks kept on average, each one whole or gone. */
    memset(&faults, 0, sizeof(faults));
    faults.disk.unsynced_keep_ppm = 500000;
    memset(&count, 0, sizeof(count));
    trace.ctx = &count;
    trace.event = count_torn;
    sim = make_world(seed, 1, &faults);
    vsr_sim_set_trace(sim, &trace);
    ex = vsr_sim_executor(sim, 0);
    for (uint32_t b = 0; b < 8; ++b) {
        for (size_t i = 0; i < BLOCK; ++i) {
            blocks[b][i] = (unsigned char)((size_t)b * 37 + i);
        }
    }
    fd = open_at(sim, ex, VSR_SIM_ROOT, "torn", O_CREAT | O_RDWR);
    CHECK(fd >= 0);
    CHECK(write_at(sim, ex, fd, 0, blocks[0], 8 * BLOCK) ==
          (int32_t)(8 * BLOCK));
    vsr_sim_crash(sim, 0);
    for (uint32_t b = 0; b < 8; ++b) {
        CHECK(vsr_sim_file_read(sim, 0, "torn", b * BLOCK, got, BLOCK, &read) ==
              0);
        CHECK(read == BLOCK);
        if (all_bytes(got, BLOCK, 0)) {
            ++zero;
        } else {
            CHECK(memcmp(got, blocks[b], BLOCK) == 0);
        }
    }
    CHECK(count.torn == zero);
    printf("sim_world torn: %" PRIu32 " of 8 blocks discarded\n", zero);
    vsr_sim_destroy(sim);

    /* FSYNC covers the blocks written before its submission: a write
     * submitted in the same batch after it is not covered. O_DSYNC
     * writes are durable at completion. */
    faults.disk.unsynced_keep_ppm = 0;
    faults.disk.latency_min_ns = 1 * MS;
    faults.disk.latency_max_ns = 1 * MS;
    faults.disk.fsync_min_ns = 5 * MS;
    faults.disk.fsync_max_ns = 5 * MS;
    sim = make_world(seed, 1, &faults);
    ex = vsr_sim_executor(sim, 0);
    fd = open_at(sim, ex, VSR_SIM_ROOT, "order", O_CREAT | O_RDWR);
    CHECK(write_at(sim, ex, fd, 0, blocks[0], BLOCK) == (int32_t)BLOCK);
    sqes[0] = make_sqe(VSR_IO_SQE_FSYNC, fd, 0x80);
    sqes[1] = make_sqe(VSR_IO_SQE_WRITE, fd, 0x81);
    sqes[1].offset = BLOCK;
    sqes[1].addr = blocks[1];
    sqes[1].length = BLOCK;
    submit(ex, sqes, 2);
    CHECK(settle(sim) > 0);
    CHECK(reap(ex, cqes, 2) == 2);
    CHECK(cqes[0].user_data == 0x81 && cqes[0].result == (int32_t)BLOCK);
    CHECK(cqes[1].user_data == 0x80 && cqes[1].result == 0);
    {
        int sync_fd =
            open_at(sim, ex, VSR_SIM_ROOT, "dsync", O_CREAT | O_RDWR | O_DSYNC);

        CHECK(sync_fd >= 0);
        CHECK(write_at(sim, ex, sync_fd, 0, blocks[2], BLOCK) ==
              (int32_t)BLOCK);
    }
    vsr_sim_crash(sim, 0);
    CHECK(vsr_sim_file_read(sim, 0, "order", 0, got, BLOCK, &read) == 0);
    CHECK(memcmp(got, blocks[0], BLOCK) == 0);
    CHECK(vsr_sim_file_read(sim, 0, "order", BLOCK, got, BLOCK, &read) == 0);
    CHECK(all_bytes(got, BLOCK, 0));
    CHECK(vsr_sim_file_read(sim, 0, "dsync", 0, got, BLOCK, &read) == 0);
    CHECK(memcmp(got, blocks[2], BLOCK) == 0);
    vsr_sim_destroy(sim);

    /* A crash keeps names that were unlinked while open only until the
     * crash; a WRITE whose latency has not elapsed is lost entirely. */
    faults.disk.unsynced_keep_ppm = 1000000;
    faults.disk.latency_min_ns = 10 * MS;
    faults.disk.latency_max_ns = 10 * MS;
    sim = make_world(seed, 1, &faults);
    ex = vsr_sim_executor(sim, 0);
    fd = open_at(sim, ex, VSR_SIM_ROOT, "gone", O_CREAT | O_RDWR);
    CHECK(unlink_at(sim, ex, "gone", 0) == 0);
    CHECK(write_at(sim, ex, fd, 0, blocks[0], BLOCK) == (int32_t)BLOCK);
    fd = open_at(sim, ex, VSR_SIM_ROOT, "late", O_CREAT | O_RDWR);
    sqes[0] = make_sqe(VSR_IO_SQE_WRITE, fd, 0x82);
    sqes[0].addr = blocks[0];
    sqes[0].length = BLOCK;
    submit(ex, sqes, 1);
    CHECK(vsr_sim_inflight(sim, 0) == 1);
    vsr_sim_crash(sim, 0);
    CHECK(vsr_sim_inflight(sim, 0) == 0);
    CHECK(vsr_sim_file_size(sim, 0, "gone", &size) == -ENOENT);
    CHECK(vsr_sim_dir_count(sim, 0, ".", &zero) == 0 && zero == 1);
    CHECK(vsr_sim_file_size(sim, 0, "late", &size) == 0 && size == 0);
    vsr_sim_destroy(sim);
}

/* ------------------------------------------------------------------------
 * Directories and inspection
 * --------------------------------------------------------------------- */

static void test_directories(void)
{
    struct vsr_sim *sim = make_world(seed, 1, NULL);
    struct vsr_io_executor ex = vsr_sim_executor(sim, 0);
    struct statx info;
    char name[VSR_SIM_NAME_BYTES];
    unsigned char got[16];
    uint64_t size = 0;
    uint32_t count = 0;
    size_t read = 0;
    int first;
    int second;
    int dir;

    CHECK(mkdir_at(sim, ex, "d") == 0);
    CHECK(mkdir_at(sim, ex, "d") == -EEXIST);
    CHECK(mkdir_at(sim, ex, "missing/d") == -ENOENT);
    CHECK(open_at(sim, ex, VSR_SIM_ROOT, "d/f", O_RDWR) == -ENOENT);
    first = open_at(sim, ex, VSR_SIM_ROOT, "d/f", O_CREAT | O_EXCL | O_RDWR);
    CHECK(first >= 0);
    CHECK(open_at(sim, ex, VSR_SIM_ROOT, "d/f", O_CREAT | O_EXCL | O_RDWR) ==
          -EEXIST);
    CHECK(open_at(sim, ex, VSR_SIM_ROOT, "d/f", O_RDONLY | O_DIRECTORY) ==
          -ENOTDIR);
    CHECK(open_at(sim, ex, VSR_SIM_ROOT, "d", O_RDWR) == -EISDIR);
    CHECK(write_at(sim, ex, first, 0, "first", 5) == 5);

    /* Rename: the old name is gone, the new one has the data. */
    CHECK(rename_at(sim, ex, "d/f", "d/g") == 0);
    CHECK(vsr_sim_file_size(sim, 0, "d/f", &size) == -ENOENT);
    CHECK(vsr_sim_file_size(sim, 0, "d/g", &size) == 0 && size == 5);
    CHECK(rename_at(sim, ex, "d/f", "d/h") == -ENOENT);

    /* Atomic replace: the replaced file's open descriptor keeps its
     * data; the name now reads the other file. */
    second = open_at(sim, ex, VSR_SIM_ROOT, "d/h", O_CREAT | O_EXCL | O_RDWR);
    CHECK(second >= 0);
    CHECK(write_at(sim, ex, second, 0, "second!", 7) == 7);
    CHECK(rename_at(sim, ex, "d/h", "d/g") == 0);
    CHECK(vsr_sim_dir_count(sim, 0, "d", &count) == 0 && count == 1);
    CHECK(vsr_sim_file_read(sim, 0, "d/g", 0, got, sizeof(got), &read) == 0);
    CHECK(read == 7 && memcmp(got, "second!", 7) == 0);
    CHECK(read_at(sim, ex, first, 0, got, sizeof(got)) == 5);
    CHECK(memcmp(got, "first", 5) == 0);
    CHECK(write_at(sim, ex, first, 5, "+", 1) == 1);
    CHECK(read_at(sim, ex, first, 0, got, sizeof(got)) == 6);
    CHECK(close_fd(sim, ex, first) == 0);

    /* Unlink: the name goes, the open descriptor keeps the data. */
    CHECK(unlink_at(sim, ex, "d/g", 0) == 0);
    CHECK(unlink_at(sim, ex, "d/g", 0) == -ENOENT);
    CHECK(vsr_sim_dir_count(sim, 0, "d", &count) == 0 && count == 0);
    CHECK(read_at(sim, ex, second, 0, got, sizeof(got)) == 7);
    CHECK(unlink_at(sim, ex, "d", 0) == -EISDIR);
    CHECK(unlink_at(sim, ex, "d", AT_REMOVEDIR) == 0);
    CHECK(vsr_sim_dir_count(sim, 0, "d", &count) == -ENOENT);
    CHECK(close_fd(sim, ex, second) == 0);

    /* A directory descriptor as the base of relative paths; statx. */
    CHECK(mkdir_at(sim, ex, "e") == 0);
    dir = open_at(sim, ex, VSR_SIM_ROOT, "e", O_RDONLY | O_DIRECTORY);
    CHECK(dir >= 0);
    first = open_at(sim, ex, dir, "x", O_CREAT | O_RDWR);
    CHECK(first >= 0);
    CHECK(write_at(sim, ex, first, 0, "0123456789", 10) == 10);
    CHECK(vsr_sim_file_size(sim, 0, "e/x", &size) == 0 && size == 10);
    CHECK(statx_at(sim, ex, dir, "x", 0, &info) == 0);
    CHECK(S_ISREG(info.stx_mode) && info.stx_size == 10);
    CHECK((info.stx_mode & 07777) == 0644 && info.stx_blksize == BLOCK);
    CHECK(statx_at(sim, ex, VSR_SIM_ROOT, "e", 0, &info) == 0);
    CHECK(S_ISDIR(info.stx_mode) && (info.stx_mode & 07777) == 0755);
    CHECK(statx_at(sim, ex, first, "", AT_EMPTY_PATH, &info) == 0);
    CHECK(S_ISREG(info.stx_mode) && info.stx_size == 10);
    CHECK(statx_at(sim, ex, dir, "y", 0, &info) == -ENOENT);
    CHECK(unlink_at(sim, ex, "e", AT_REMOVEDIR) == -ENOTEMPTY);
    CHECK(rename_at(sim, ex, "e/x", "e") == -EISDIR);
    CHECK(rename_at(sim, ex, "e", "e/x") == -EINVAL); /* Into itself. */
    CHECK(mkdir_at(sim, ex, "e/sub") == 0);
    CHECK(rename_at(sim, ex, "e/sub", "e/x") == -ENOTDIR);
    CHECK(rename_at(sim, ex, "e/sub", "e/sub") == 0);
    CHECK(unlink_at(sim, ex, "e/sub", AT_REMOVEDIR) == 0);
    CHECK(close_fd(sim, ex, first) == 0);
    CHECK(close_fd(sim, ex, dir) == 0);

    /* Listing is sorted by name and reports sizes. */
    CHECK(open_at(sim, ex, VSR_SIM_ROOT, "e/c", O_CREAT | O_RDWR) >= 0);
    CHECK(open_at(sim, ex, VSR_SIM_ROOT, "e/a", O_CREAT | O_RDWR) >= 0);
    CHECK(vsr_sim_dir_count(sim, 0, "e", &count) == 0 && count == 3);
    CHECK(vsr_sim_dir_entry(sim, 0, "e", 0, name, sizeof(name), &size) == 0);
    CHECK(strcmp(name, "a") == 0 && size == 0);
    CHECK(vsr_sim_dir_entry(sim, 0, "e", 1, name, sizeof(name), &size) == 0);
    CHECK(strcmp(name, "c") == 0);
    CHECK(vsr_sim_dir_entry(sim, 0, "e", 2, name, sizeof(name), &size) == 0);
    CHECK(strcmp(name, "x") == 0 && size == 10);
    CHECK(vsr_sim_dir_entry(sim, 0, "e", 3, name, sizeof(name), &size) ==
          -ENOENT);
    CHECK(vsr_sim_dir_count(sim, 0, "/", &count) == 0 && count == 1);
    CHECK(vsr_sim_dir_entry(sim, 0, "/", 0, name, sizeof(name), &size) == 0);
    CHECK(strcmp(name, "e") == 0);

    /* Reads: short at the end, empty past it, the same through both. */
    CHECK(vsr_sim_file_read(sim, 0, "e/x", 8, got, sizeof(got), &read) == 0);
    CHECK(read == 2 && memcmp(got, "89", 2) == 0);
    CHECK(vsr_sim_file_read(sim, 0, "e/x", 10, got, sizeof(got), &read) == 0);
    CHECK(read == 0);
    CHECK(vsr_sim_file_read(sim, 0, "e/x", 0, got, 0, &read) == 0 && read == 0);

    /* Corruption flips bytes in place; a dirty block's corruption is not
     * durable, a synced block's is. */
    CHECK(vsr_sim_file_corrupt(sim, 0, "e/x", 2, 3) == 0);
    CHECK(vsr_sim_file_read(sim, 0, "e/x", 0, got, sizeof(got), &read) == 0);
    CHECK(read == 10 && got[1] == '1' && got[2] == (unsigned char)~'2' &&
          got[4] == (unsigned char)~'4' && got[5] == '5');
    CHECK(vsr_sim_file_read_durable(sim, 0, "e/x", 0, got, sizeof(got),
                                    &read) == 0);
    CHECK(read == 10 && all_bytes(got, 10, 0));
    first = open_at(sim, ex, VSR_SIM_ROOT, "e/x", O_RDWR);
    CHECK(read_at(sim, ex, first, 0, got, sizeof(got)) == 10);
    CHECK(got[2] == (unsigned char)~'2' && got[3] == (unsigned char)~'3');
    CHECK(fsync_fd(sim, ex, first) == 0);
    CHECK(vsr_sim_file_corrupt(sim, 0, "e/x", 0, 1) == 0);
    CHECK(vsr_sim_file_read_durable(sim, 0, "e/x", 0, got, sizeof(got),
                                    &read) == 0);
    CHECK(read == 10 && got[0] == (unsigned char)~'0' && got[1] == '1');
    CHECK(vsr_sim_file_corrupt(sim, 0, "e/x", 100, 1) == 0);
    CHECK(close_fd(sim, ex, first) == 0);
    vsr_sim_destroy(sim);
}

/* ------------------------------------------------------------------------
 * Reproducibility: a fault-heavy scenario hashed through the trace
 * --------------------------------------------------------------------- */

#define SCENARIO_STEPS 1500u
#define SCENARIO_MESSAGES 12u
#define STAGE_SLOTS 64u

#define SCENARIO_BATCH 64u

enum scenario_tag {
    TAG_ACCEPT = 1,
    TAG_RECV,
    TAG_SEND,
    TAG_TIMER,
    TAG_WRITE,
    TAG_FSYNC,
    TAG_SOCKET,
    TAG_CONNECT,
    TAG_CLOSE,
    TAG_OTHER
};

struct trace_hash {
    uint64_t hash;
    uint64_t count;
    uint64_t kinds[VSR_SIM_TRACE_CLOCK + 1];
};

static void hash_event(void *ctx, const struct vsr_sim_trace_event *event)
{
    struct trace_hash *hash = ctx;
    const uint64_t words[] = {event->kind,      event->node,
                              event->peer,      event->opcode,
                              event->user_data, (uint64_t)event->result,
                              event->bytes,     event->now_ns};

    for (size_t i = 0; i < sizeof(words) / sizeof(words[0]); ++i) {
        for (unsigned shift = 0; shift < 64; shift += 8) {
            hash->hash ^= (words[i] >> shift) & 0xffu;
            hash->hash *= UINT64_C(1099511628211);
        }
    }
    ++hash->count;
    if (event->kind <= VSR_SIM_TRACE_CLOCK) {
        ++hash->kinds[event->kind];
    }
}

/* The server: accepts one connection at a time, receives through an
 * incremental ring, appends what arrives to a file, fsyncs every fourth
 * write, and re-arms after every termination. */
struct scenario_server {
    struct vsr_sim *sim;
    struct vsr_io_executor ex;
    int listener;
    int conn;
    int file;
    uint64_t offset;
    uint32_t writes;
    uint32_t staged; /* WRITEs in flight. */
    unsigned char buffers[4][256];
    uint32_t delivered[4]; /* Bytes delivered from each buffer so far. */
    unsigned char stage[STAGE_SLOTS][256];
    bool stage_used[STAGE_SLOTS];
    struct vsr_io_sqe batch[SCENARIO_BATCH];
    uint32_t batch_count;
};

/* The client: connects, sends messages of random sizes (every other one
 * zero-copy), closes, waits, and connects again; on any failure it closes
 * and retries after a timer. Every step is a record whose completion
 * drives the next, so the harness loop is the only place that reaps. */
struct scenario_client {
    struct vsr_sim *sim;
    struct vsr_io_executor ex;
    struct vsr_io_address address;
    int socket;
    uint32_t sent;
    uint32_t outstanding; /* SEND records not yet fully completed. */
    unsigned char message[4][512];
    struct vsr_io_sqe batch[SCENARIO_BATCH];
    uint32_t batch_count;
};

static _Alignas(4096) unsigned char scenario_ring[4096];

static struct vsr_io_sqe *batch_next(struct vsr_io_sqe *batch, uint32_t *count,
                                     uint8_t opcode, int32_t fd,
                                     uint64_t user_data)
{
    struct vsr_io_sqe *sqe;

    CHECK(*count < SCENARIO_BATCH);
    sqe = &batch[(*count)++];
    *sqe = make_sqe(opcode, fd, user_data);
    return sqe;
}

static void server_setup(struct scenario_server *server)
{
    struct vsr_sim *sim = server->sim;
    struct vsr_io_region memory = {.base = scenario_ring,
                                   .size = sizeof(scenario_ring)};
    struct vsr_io_buffer buffers[4];
    struct vsr_io_sqe *sqe;
    uint64_t size = 0;

    server->conn = -1;
    server->staged = 0;
    server->writes = 0;
    server->batch_count = 0;
    memset(server->stage_used, 0, sizeof(server->stage_used));
    memset(server->delivered, 0, sizeof(server->delivered));
    server->file =
        open_at(sim, server->ex, VSR_SIM_ROOT, "log", O_CREAT | O_RDWR);
    CHECK(server->file >= 0);
    CHECK(vsr_sim_file_size(sim, 1, "log", &size) == 0);
    server->offset = size;
    server->listener = listen_on(sim, server->ex, 1, PORT);
    CHECK(server->ex.ops->buffer_ring(server->ex.ctx, 1, 4,
                                      VSR_IO_BUFFER_RING_INCREMENTAL,
                                      &memory) == 0);
    for (uint32_t i = 0; i < 4; ++i) {
        buffers[i].base = server->buffers[i];
        buffers[i].length = sizeof(server->buffers[i]);
        buffers[i].id = (uint16_t)i;
        buffers[i].reserved = 0;
    }
    CHECK(server->ex.ops->provide(server->ex.ctx, 1, buffers, 4) == 0);
    sqe = batch_next(server->batch, &server->batch_count, VSR_IO_SQE_ACCEPT,
                     server->listener, VSR_IO_USER_DATA(TAG_ACCEPT, 0));
    sqe->op_flags = VSR_IO_ACCEPT_MULTISHOT;
}

static void server_arm_recv(struct scenario_server *server)
{
    struct vsr_io_sqe *sqe =
        batch_next(server->batch, &server->batch_count, VSR_IO_SQE_RECV,
                   server->conn, VSR_IO_USER_DATA(TAG_RECV, 0));

    sqe->flags = VSR_IO_SQE_BUFFER_SELECT;
    sqe->buffer_group = 1;
    sqe->op_flags = VSR_IO_RECV_MULTISHOT;
}

static void server_complete(struct scenario_server *server,
                            const struct vsr_io_cqe *cqe)
{
    struct vsr_io_sqe *sqe;
    uint64_t value = cqe->user_data & ((UINT64_C(1) << 56) - 1);

    switch (VSR_IO_OWNER(cqe->user_data)) {
    case TAG_ACCEPT:
        if (cqe->result >= 0) {
            if (server->conn < 0) {
                server->conn = cqe->result;
                server_arm_recv(server);
            } else {
                (void)batch_next(server->batch, &server->batch_count,
                                 VSR_IO_SQE_CLOSE, cqe->result,
                                 VSR_IO_USER_DATA(TAG_CLOSE, 0));
            }
        }
        if ((cqe->flags & VSR_IO_CQE_MORE) == 0) {
            sqe = batch_next(server->batch, &server->batch_count,
                             VSR_IO_SQE_ACCEPT, server->listener,
                             VSR_IO_USER_DATA(TAG_ACCEPT, 0));
            sqe->op_flags = VSR_IO_ACCEPT_MULTISHOT;
        }
        break;
    case TAG_RECV:
        if (cqe->result > 0) {
            uint32_t slot = STAGE_SLOTS;
            uint32_t id = cqe->buffer_id;
            const unsigned char *source;

            /* The bytes landed after what the buffer delivered before:
             * BUFFER_MORE keeps it at the head with its prefix consumed. */
            CHECK((cqe->flags & VSR_IO_CQE_BUFFER) != 0 && id < 4);
            CHECK(server->delivered[id] + (uint32_t)cqe->result <= 256);
            source = server->buffers[id] + server->delivered[id];
            server->delivered[id] += (uint32_t)cqe->result;
            for (uint32_t i = 0; i < STAGE_SLOTS; ++i) {
                if (!server->stage_used[i]) {
                    slot = i;
                    break;
                }
            }
            if (slot < STAGE_SLOTS) {
                uint32_t length = (uint32_t)cqe->result;

                server->stage_used[slot] = true;
                memcpy(server->stage[slot], source, length);
                sqe = batch_next(server->batch, &server->batch_count,
                                 VSR_IO_SQE_WRITE, server->file,
                                 VSR_IO_USER_DATA(TAG_WRITE, slot));
                sqe->addr = server->stage[slot];
                sqe->length = length;
                sqe->offset = server->offset;
                server->offset += length;
                ++server->staged;
                if (++server->writes % 4 == 0) {
                    (void)batch_next(server->batch, &server->batch_count,
                                     VSR_IO_SQE_FSYNC, server->file,
                                     VSR_IO_USER_DATA(TAG_FSYNC, 0));
                }
            }
            if ((cqe->flags & VSR_IO_CQE_BUFFER_MORE) == 0) {
                struct vsr_io_buffer buffer = {.base = server->buffers[id],
                                               .length =
                                                   sizeof(server->buffers[0]),
                                               .id = (uint16_t)id};

                /* The buffer left the ring: ours again, and back it goes. */
                server->delivered[id] = 0;
                CHECK(server->ex.ops->provide(server->ex.ctx, 1, &buffer, 1) ==
                      0);
            }
        }
        if ((cqe->flags & VSR_IO_CQE_MORE) == 0) {
            if (cqe->result == -ENOBUFS) {
                server_arm_recv(server);
            } else {
                (void)batch_next(server->batch, &server->batch_count,
                                 VSR_IO_SQE_CLOSE, server->conn,
                                 VSR_IO_USER_DATA(TAG_CLOSE, 0));
                server->conn = -1;
            }
        }
        break;
    case TAG_WRITE:
        CHECK(value < STAGE_SLOTS && server->stage_used[value]);
        server->stage_used[value] = false;
        --server->staged;
        break;
    default:
        break;
    }
}

/* Step one: a socket; its completion submits the CONNECT. */
static void client_connect(struct scenario_client *client)
{
    struct vsr_io_sqe *sqe =
        batch_next(client->batch, &client->batch_count, VSR_IO_SQE_SOCKET, -1,
                   VSR_IO_USER_DATA(TAG_SOCKET, 0));

    sqe->length = AF_INET;
    sqe->op_flags = SOCK_STREAM;
    client->sent = 0;
}

static void client_send(struct scenario_client *client)
{
    struct vsr_io_sqe *sqe =
        batch_next(client->batch, &client->batch_count, VSR_IO_SQE_SEND,
                   client->socket, VSR_IO_USER_DATA(TAG_SEND, client->sent));
    uint32_t length = 1 + (uint32_t)vsr_sim_random_below(client->sim, 400);
    unsigned char *message = client->message[client->sent % 4];

    fill(message, sizeof(client->message[0]),
         (unsigned char)(0x10 + client->sent));
    sqe->addr = message;
    sqe->length = length;
    if (client->sent % 2 == 1) {
        sqe->op_flags = VSR_IO_SEND_ZERO_COPY;
    }
    ++client->sent;
    ++client->outstanding;
}

static void client_retry(struct scenario_client *client, uint64_t delay_ns)
{
    struct vsr_io_sqe *sqe;

    if (client->socket >= 0) {
        (void)batch_next(client->batch, &client->batch_count, VSR_IO_SQE_CLOSE,
                         client->socket, VSR_IO_USER_DATA(TAG_CLOSE, 0));
        client->socket = -1;
    }
    sqe = batch_next(client->batch, &client->batch_count, VSR_IO_SQE_TIMEOUT,
                     -1, VSR_IO_USER_DATA(TAG_TIMER, 0));
    sqe->offset = delay_ns;
}

static void client_complete(struct scenario_client *client,
                            const struct vsr_io_cqe *cqe)
{
    struct vsr_io_sqe *sqe;

    switch (VSR_IO_OWNER(cqe->user_data)) {
    case TAG_SOCKET:
        CHECK(cqe->result >= 0 && client->socket < 0);
        client->socket = cqe->result;
        /* The address is copied at submission. */
        sqe =
            batch_next(client->batch, &client->batch_count, VSR_IO_SQE_CONNECT,
                       client->socket, VSR_IO_USER_DATA(TAG_CONNECT, 0));
        sqe->addr = &client->address.sockaddr;
        sqe->length = client->address.length;
        break;
    case TAG_CONNECT:
        if (cqe->result == 0) {
            client_send(client);
        } else {
            client_retry(client, 30 * MS);
        }
        break;
    case TAG_SEND:
        /* A zero-copy send is done at its NOTIF, any other at its result;
         * a failed send drops the connection but its NOTIF still comes. */
        if ((cqe->flags & VSR_IO_CQE_NOTIF) != 0 ||
            (cqe->flags & VSR_IO_CQE_MORE) == 0) {
            CHECK(client->outstanding > 0);
            --client->outstanding;
        }
        if ((cqe->flags & VSR_IO_CQE_NOTIF) == 0 && cqe->result < 0) {
            client_retry(client, 20 * MS);
        }
        if (client->outstanding > 0 || client->socket < 0) {
            return;
        }
        if (client->sent < SCENARIO_MESSAGES) {
            client_send(client);
        } else {
            client_retry(client, 40 * MS);
        }
        break;
    case TAG_TIMER:
        client_connect(client);
        break;
    default:
        break;
    }
}

static uint64_t run_scenario(uint64_t scenario_seed, struct trace_hash *hash)
{
    struct vsr_sim_faults faults;
    struct vsr_sim *sim;
    struct vsr_sim_trace trace;
    struct scenario_server *server = calloc(1, sizeof(*server));
    struct scenario_client *client = calloc(1, sizeof(*client));
    struct vsr_io_cqe cqes[16];

    CHECK(server != NULL && client != NULL);
    memset(&faults, 0, sizeof(faults));
    faults.network.drop_ppm = 150000;
    faults.network.corrupt_ppm = 200000;
    faults.network.reset_ppm = 50000;
    faults.network.split_ppm = 500000;
    faults.network.delay_min_ns = 1 * MS;
    faults.network.delay_max_ns = 8 * MS;
    faults.network.stall_reset_ns = 40 * MS;
    faults.network.connect_timeout_ns = 60 * MS;
    faults.disk.latency_min_ns = 1 * MS;
    faults.disk.latency_max_ns = 4 * MS;
    faults.disk.fsync_min_ns = 2 * MS;
    faults.disk.fsync_max_ns = 6 * MS;
    faults.disk.bitrot_ppm = 300000;
    faults.disk.error_ppm = 100000;
    faults.disk.enospc_ppm = 50000;
    faults.disk.unsynced_keep_ppm = 500000;
    faults.clock.offset_max_ns = 100 * MS;
    faults.clock.jitter_max_ns = 2 * MS;
    memset(hash, 0, sizeof(*hash));
    trace.ctx = hash;
    trace.event = hash_event;
    sim = make_world(scenario_seed, 2, &faults);
    vsr_sim_set_trace(sim, &trace);
    server->sim = sim;
    server->ex = vsr_sim_executor(sim, 1);
    client->sim = sim;
    client->ex = vsr_sim_executor(sim, 0);
    client->address = vsr_sim_address(sim, 1, PORT);
    client->socket = -1;
    server_setup(server);
    client_connect(client);

    for (uint32_t step = 0; step < SCENARIO_STEPS; ++step) {
        if (step == 300) {
            vsr_sim_crash(sim, 1);
        } else if (step == 340) {
            server->ex = vsr_sim_restart(sim, 1);
            CHECK(server->ex.ops != NULL);
            server_setup(server);
            /* Bit rot: a read of the recovered file through the executor
             * (its result, bytes or -EIO, is part of the trace). */
            (void)read_at(sim, server->ex, server->file, 0, server->stage[0],
                          sizeof(server->stage[0]));
        } else if (step == 600) {
            vsr_sim_partition(sim, 0, 1, 1);
        } else if (step == 700) {
            vsr_sim_partition(sim, 0, 1, 0);
        } else if (step == 900) {
            vsr_sim_reset(sim, 0, 1);
        } else if (step == 1000) {
            vsr_sim_crash(sim, 0);
        } else if (step == 1020) {
            client->ex = vsr_sim_restart(sim, 0);
            CHECK(client->ex.ops != NULL);
            client->socket = -1;
            client->outstanding = 0;
            client->batch_count = 0;
            client_connect(client);
        }
        if (vsr_sim_alive(sim, 1) && vsr_sim_ready(sim, 1)) {
            uint32_t count = reap(server->ex, cqes, 16);

            for (uint32_t i = 0; i < count; ++i) {
                server_complete(server, &cqes[i]);
            }
            CHECK(server->ex.ops->submit_and_wait(server->ex.ctx, server->batch,
                                                  server->batch_count, 1, 0,
                                                  VSR_NO_DEADLINE) == 0);
            server->batch_count = 0;
        }
        if (vsr_sim_alive(sim, 0) && vsr_sim_ready(sim, 0)) {
            uint32_t count = reap(client->ex, cqes, 16);

            for (uint32_t i = 0; i < count; ++i) {
                client_complete(client, &cqes[i]);
            }
            CHECK(client->ex.ops->submit_and_wait(client->ex.ctx, client->batch,
                                                  client->batch_count, 1, 0,
                                                  VSR_NO_DEADLINE) == 0);
            client->batch_count = 0;
        }
        (void)vsr_sim_advance(sim);
    }
    {
        uint64_t now = vsr_sim_now(sim);

        vsr_sim_destroy(sim);
        free(server);
        free(client);
        return now;
    }
}

static void test_reproducibility(void)
{
    struct trace_hash first;
    struct trace_hash second;
    struct trace_hash other;
    uint64_t end_first = run_scenario(seed, &first);
    uint64_t end_second = run_scenario(seed, &second);

    CHECK(first.count == second.count && first.hash == second.hash);
    CHECK(end_first == end_second);
    CHECK(first.kinds[VSR_SIM_TRACE_DELIVER] > 0);
    CHECK(first.kinds[VSR_SIM_TRACE_DROP] > 0);
    CHECK(first.kinds[VSR_SIM_TRACE_COMPLETE] > 0);
    CHECK(first.kinds[VSR_SIM_TRACE_CRASH] == 2);
    CHECK(first.kinds[VSR_SIM_TRACE_RESTART] == 2);
    CHECK(first.kinds[VSR_SIM_TRACE_CLOCK] > 0);
    printf("sim_world scenario: %" PRIu64 " events, %" PRIu64
           " deliveries, %" PRIu64 " drops, %" PRIu64 " corruptions, %" PRIu64
           " resets, %" PRIu64 " torn, %" PRIu64 " bitrot, ends at %" PRIu64
           " ns\n",
           first.count, first.kinds[VSR_SIM_TRACE_DELIVER],
           first.kinds[VSR_SIM_TRACE_DROP], first.kinds[VSR_SIM_TRACE_CORRUPT],
           first.kinds[VSR_SIM_TRACE_RESET], first.kinds[VSR_SIM_TRACE_TORN],
           first.kinds[VSR_SIM_TRACE_BITROT], end_first);
    (void)run_scenario(seed + 1, &other);
    CHECK(other.hash != first.hash);
}

/* ------------------------------------------------------------------------
 * Aborts: ownership violations and stale handles, in a forked child
 * --------------------------------------------------------------------- */

static void expect_abort(void (*body)(void))
{
    pid_t pid = fork();
    int status = 0;

    CHECK(pid >= 0);
    if (pid == 0) {
        int null = open("/dev/null", O_WRONLY);

        if (null >= 0) {
            (void)dup2(null, STDERR_FILENO);
            (void)close(null);
        }
        body();
        _exit(0); /* Returning is the failure the parent detects. */
    }
    CHECK(waitpid(pid, &status, 0) == pid);
    CHECK(WIFSIGNALED(status) && WTERMSIG(status) == SIGABRT);
}

static struct vsr_sim_faults slow_faults(void)
{
    struct vsr_sim_faults faults;

    memset(&faults, 0, sizeof(faults));
    faults.disk.latency_min_ns = 5 * MS;
    faults.disk.latency_max_ns = 5 * MS;
    faults.network.delay_min_ns = 5 * MS;
    faults.network.delay_max_ns = 5 * MS;
    return faults;
}

static unsigned char abort_buffer[BLOCK];

/* A WRITE's bytes change before its completion. */
static void body_write_changed(void)
{
    struct vsr_sim_faults faults = slow_faults();
    struct vsr_sim *sim = make_world(seed, 1, &faults);
    struct vsr_io_executor ex = vsr_sim_executor(sim, 0);
    int fd = open_at(sim, ex, VSR_SIM_ROOT, "f", O_CREAT | O_RDWR);
    struct vsr_io_sqe sqe = make_sqe(VSR_IO_SQE_WRITE, fd, 0x90);

    sqe.addr = abort_buffer;
    sqe.length = sizeof(abort_buffer);
    submit(ex, &sqe, 1);
    abort_buffer[7] ^= 1;
    (void)wait_one(sim, ex);
}

/* The same, cut short by a crash: the crash verifies too. */
static void body_write_changed_crash(void)
{
    struct vsr_sim_faults faults = slow_faults();
    struct vsr_sim *sim = make_world(seed, 1, &faults);
    struct vsr_io_executor ex = vsr_sim_executor(sim, 0);
    int fd = open_at(sim, ex, VSR_SIM_ROOT, "f", O_CREAT | O_RDWR);
    struct vsr_io_sqe sqe = make_sqe(VSR_IO_SQE_WRITE, fd, 0x91);

    sqe.addr = abort_buffer;
    sqe.length = sizeof(abort_buffer);
    submit(ex, &sqe, 1);
    abort_buffer[7] ^= 1;
    vsr_sim_crash(sim, 0);
}

static void connected_pair(struct vsr_sim **sim_out, struct vsr_io_executor *c,
                           struct vsr_io_executor *s, int *client_fd,
                           int *server_fd)
{
    struct vsr_sim_faults faults = slow_faults();
    struct vsr_sim *sim = make_world(seed, 2, &faults);
    int listener;

    *c = vsr_sim_executor(sim, 0);
    *s = vsr_sim_executor(sim, 1);
    listener = listen_on(sim, *s, 1, PORT);
    connect_pair(sim, *c, *s, listener, 1, client_fd, server_fd);
    *sim_out = sim;
}

/* A zero-copy send's bytes change before the NOTIF. */
static void body_zero_copy_changed(void)
{
    struct vsr_sim *sim;
    struct vsr_io_executor c;
    struct vsr_io_executor s;
    int client;
    int server;
    struct vsr_io_sqe sqe;
    struct vsr_io_cqe cqe;

    connected_pair(&sim, &c, &s, &client, &server);
    sqe = make_sqe(VSR_IO_SQE_SEND, client, 0x92);
    sqe.addr = abort_buffer;
    sqe.length = 64;
    sqe.op_flags = VSR_IO_SEND_ZERO_COPY;
    submit(c, &sqe, 1);
    cqe = wait_one(sim, c);
    CHECK(cqe.result == 64 && cqe.flags == VSR_IO_CQE_MORE);
    abort_buffer[1] ^= 1;
    (void)wait_one(sim, c);
}

/* A provided buffer changes while the kernel owns it: caught at
 * unregistration. */
static void body_provided_changed(void)
{
    struct vsr_sim_faults faults = slow_faults();
    struct vsr_sim *sim = make_world(seed, 1, &faults);
    struct vsr_io_executor ex = vsr_sim_executor(sim, 0);
    struct vsr_io_region memory = {.base = ring_memory,
                                   .size = sizeof(ring_memory)};
    struct vsr_io_buffer buffer = {
        .base = abort_buffer, .length = sizeof(abort_buffer), .id = 1};

    CHECK(ex.ops->buffer_ring(ex.ctx, 3, 8, 0, &memory) == 0);
    CHECK(ex.ops->provide(ex.ctx, 3, &buffer, 1) == 0);
    abort_buffer[100] ^= 1;
    (void)ex.ops->buffer_ring(ex.ctx, 3, 0, 0, NULL);
}

/* The same, caught at a crash. */
static void body_provided_changed_crash(void)
{
    struct vsr_sim_faults faults = slow_faults();
    struct vsr_sim *sim = make_world(seed, 1, &faults);
    struct vsr_io_executor ex = vsr_sim_executor(sim, 0);
    struct vsr_io_region memory = {.base = ring_memory,
                                   .size = sizeof(ring_memory)};
    struct vsr_io_buffer buffer = {
        .base = abort_buffer, .length = sizeof(abort_buffer), .id = 1};

    CHECK(ex.ops->buffer_ring(ex.ctx, 3, 8, 0, &memory) == 0);
    CHECK(ex.ops->provide(ex.ctx, 3, &buffer, 1) == 0);
    abort_buffer[100] ^= 1;
    vsr_sim_crash(sim, 0);
}

/* A buffer is provided while a send still reads it. */
static void body_provide_in_flight(void)
{
    struct vsr_sim *sim;
    struct vsr_io_executor c;
    struct vsr_io_executor s;
    int client;
    int server;
    struct vsr_io_sqe sqe;
    struct vsr_io_cqe cqe;
    struct vsr_io_region memory = {.base = ring_memory,
                                   .size = sizeof(ring_memory)};
    struct vsr_io_buffer buffer = {
        .base = abort_buffer + 32, .length = 64, .id = 1};

    connected_pair(&sim, &c, &s, &client, &server);
    sqe = make_sqe(VSR_IO_SQE_SEND, client, 0x93);
    sqe.addr = abort_buffer;
    sqe.length = 64;
    sqe.op_flags = VSR_IO_SEND_ZERO_COPY;
    submit(c, &sqe, 1);
    cqe = wait_one(sim, c);
    CHECK(cqe.result == 64 && cqe.flags == VSR_IO_CQE_MORE);
    CHECK(c.ops->buffer_ring(c.ctx, 3, 8, 0, &memory) == 0);
    (void)c.ops->provide(c.ctx, 3, &buffer, 1);
}

/* A buffer is provided twice. */
static void body_provide_twice(void)
{
    struct vsr_sim *sim = make_world(seed, 1, NULL);
    struct vsr_io_executor ex = vsr_sim_executor(sim, 0);
    struct vsr_io_region memory = {.base = ring_memory,
                                   .size = sizeof(ring_memory)};
    struct vsr_io_buffer buffer = {.base = abort_buffer, .length = 64, .id = 1};

    CHECK(ex.ops->buffer_ring(ex.ctx, 3, 8, 0, &memory) == 0);
    CHECK(ex.ops->provide(ex.ctx, 3, &buffer, 1) == 0);
    (void)ex.ops->provide(ex.ctx, 3, &buffer, 1);
}

/* A handle from before a crash. */
static void body_stale_handle(void)
{
    struct vsr_sim *sim = make_world(seed, 1, NULL);
    struct vsr_io_executor ex = vsr_sim_executor(sim, 0);

    vsr_sim_crash(sim, 0);
    (void)ex.ops->now(ex.ctx);
}

/* The same handle after a restart handed out a fresh one. */
static void body_stale_handle_restarted(void)
{
    struct vsr_sim *sim = make_world(seed, 1, NULL);
    struct vsr_io_executor ex = vsr_sim_executor(sim, 0);
    struct vsr_io_sqe sqe = make_sqe(VSR_IO_SQE_NOP, -1, 0x94);

    vsr_sim_crash(sim, 0);
    CHECK(vsr_sim_restart(sim, 0).ops != NULL);
    (void)ex.ops->submit_and_wait(ex.ctx, &sqe, 1, 0, 0, 0);
}

static void test_aborts(void)
{
    /* The controls: the same bodies without the offending change run to
     * completion in the other tests; here every body must abort. */
    fill(abort_buffer, sizeof(abort_buffer), 0x33);
    expect_abort(body_write_changed);
    expect_abort(body_write_changed_crash);
    expect_abort(body_zero_copy_changed);
    expect_abort(body_provided_changed);
    expect_abort(body_provided_changed_crash);
    expect_abort(body_provide_in_flight);
    expect_abort(body_provide_twice);
    expect_abort(body_stale_handle);
    expect_abort(body_stale_handle_restarted);
}

/* ------------------------------------------------------------------------
 * Decisions 62 and 65: single completion of a rejected zero-copy send,
 * reserved user_data, GETSOCKOPT levels, ACCEPT with DIRECT and no slot,
 * O_DIRECT alignment, registration errno values
 * --------------------------------------------------------------------- */

static _Alignas(4096) unsigned char direct_buffer[3 * BLOCK];

static void test_contract_details(void)
{
    struct vsr_sim_faults faults;
    struct vsr_sim *sim;
    struct vsr_io_executor ex0;
    struct vsr_io_executor ex1;
    int listener;
    int client;
    int server;
    int fd;
    int32_t result;
    int option;
    unsigned char bytes[64];
    struct vsr_io_sqe sqes[2];
    struct vsr_io_cqe cqes[2];
    struct vsr_io_cqe cqe;

    memset(&faults, 0, sizeof(faults));
    faults.network.delay_min_ns = 1 * MS;
    faults.network.delay_max_ns = 1 * MS;
    sim = make_world(seed, 2, &faults);
    ex0 = vsr_sim_executor(sim, 0);
    ex1 = vsr_sim_executor(sim, 1);
    listener = listen_on(sim, ex1, 1, PORT);
    connect_pair(sim, ex0, ex1, listener, 1, &client, &server);

    /* A zero-copy send rejected at validation completes exactly once,
     * MORE clear and without a NOTIF (decision 62). */
    sqes[0] = make_sqe(VSR_IO_SQE_SEND, client, 0xA0);
    sqes[0].flags = VSR_IO_SQE_SKIP_SUCCESS;
    sqes[0].addr = bytes;
    sqes[0].length = 8;
    sqes[0].op_flags = VSR_IO_SEND_ZERO_COPY;
    submit(ex0, sqes, 1);
    CHECK(reap(ex0, cqes, 2) == 1);
    CHECK(cqes[0].user_data == 0xA0 && cqes[0].result == -EINVAL &&
          cqes[0].flags == 0);
    CHECK(vsr_sim_inflight(sim, 0) == 0);
    (void)settle(sim);
    CHECK(reap(ex0, cqes, 2) == 0);
    sqes[0].flags = 0;
    sqes[0].op_flags = VSR_IO_SEND_ZERO_COPY | 0x80; /* Unknown flag. */
    submit(ex0, sqes, 1);
    CHECK(reap(ex0, cqes, 2) == 1 && cqes[0].result == -EINVAL &&
          cqes[0].flags == 0);
    CHECK(vsr_sim_inflight(sim, 0) == 0);
    /* A zero-copy send that fails later still posts both, result first. */
    sqes[0].op_flags = VSR_IO_SEND_ZERO_COPY;
    sqes[0].fd = listener; /* Node 0 has no such socket: -EBADF at start. */
    submit(ex1, sqes, 1);
    (void)settle(sim);
    CHECK(reap(ex1, cqes, 2) == 2);
    CHECK(cqes[0].result == -ENOTCONN && cqes[0].flags == VSR_IO_CQE_MORE);
    CHECK(cqes[1].result == 0 && cqes[1].flags == VSR_IO_CQE_NOTIF);

    /* user_data UINT64_MAX is reserved: the batch is refused whole. */
    sqes[0] = make_sqe(VSR_IO_SQE_NOP, -1, 0xA1);
    sqes[1] = make_sqe(VSR_IO_SQE_NOP, -1, UINT64_MAX);
    CHECK(ex0.ops->submit_and_wait(ex0.ctx, sqes, 2, 0, 0, 0) == -EINVAL);
    CHECK(reap(ex0, cqes, 2) == 0 && vsr_sim_inflight(sim, 0) == 0);

    /* GETSOCKOPT serves SOL_SOCKET only; SETSOCKOPT takes TCP_NODELAY. */
    option = 1;
    sqes[0] = make_sqe(VSR_IO_SQE_SETSOCKOPT, client, 0xA2);
    sqes[0].op_flags = (IPPROTO_TCP << 16) | TCP_NODELAY;
    sqes[0].addr = &option;
    sqes[0].length = sizeof(option);
    CHECK(run(sim, ex0, &sqes[0]).result == 0);
    sqes[0].op_flags = (SOL_SOCKET << 16) | SO_KEEPALIVE;
    CHECK(run(sim, ex0, &sqes[0]).result == 0);
    option = 0;
    sqes[0].opcode = VSR_IO_SQE_GETSOCKOPT;
    CHECK(run(sim, ex0, &sqes[0]).result == (int32_t)sizeof(option));
    CHECK(option == 1);
    sqes[0].op_flags = (IPPROTO_TCP << 16) | TCP_NODELAY;
    CHECK(run(sim, ex0, &sqes[0]).result == -EOPNOTSUPP);
    sqes[0].op_flags = (SOL_SOCKET << 16) | SO_REUSEADDR;
    CHECK(run(sim, ex0, &sqes[0]).result == -ENOPROTOOPT);
    sqes[0].opcode = VSR_IO_SQE_SETSOCKOPT;
    CHECK(run(sim, ex0, &sqes[0]).result == -ENOPROTOOPT);
    CHECK(close_fd(sim, ex0, client) == 0);
    CHECK(close_fd(sim, ex1, server) == 0);

    /* Registration errno values. */
    CHECK(ex1.ops->register_files(ex1.ctx, 1) == 0);
    CHECK(ex1.ops->register_files(ex1.ctx, 1) == -EBUSY);
    CHECK(ex1.ops->register_buffers(ex1.ctx, 1) == 0);
    CHECK(ex1.ops->register_buffers(ex1.ctx, 1) == -EBUSY);
    CHECK(ex1.ops->provide(ex1.ctx, 9, NULL, 0) == -ENOENT);
    CHECK(ex1.ops->buffer_ring(ex1.ctx, 9, 0, 0, NULL) == -ENOENT);

    /* ACCEPT with DIRECT and no free slot: the connection is accepted and
     * closed (the peer sees a reset), -ENFILE ends the multishot, and a
     * later connection stays queued until a slot is free. Node 0 has no
     * table at all, which is no free slot either. */
    sqes[0] = make_sqe(VSR_IO_SQE_OPENAT, VSR_SIM_ROOT, 0xA3);
    sqes[0].flags = VSR_IO_SQE_DIRECT;
    sqes[0].fd2 = 0;
    sqes[0].addr = "slot";
    sqes[0].op_flags = O_CREAT | O_RDWR;
    CHECK(run(sim, ex1, &sqes[0]).result == 0); /* Slot 0 is taken. */
    sqes[0].fd2 = VSR_IO_SLOT_ALLOC;
    CHECK(run(sim, ex1, &sqes[0]).result == -ENFILE);
    CHECK(run(sim, ex0, &sqes[0]).result == -ENFILE);
    sqes[0] = make_sqe(VSR_IO_SQE_ACCEPT, listener, 0xA4);
    sqes[0].flags = VSR_IO_SQE_DIRECT;
    sqes[0].fd2 = VSR_IO_SLOT_ALLOC;
    sqes[0].op_flags = VSR_IO_ACCEPT_MULTISHOT;
    submit(ex1, sqes, 1);
    client = connect_to(sim, ex0, 1, PORT, &result);
    CHECK(result == 0);
    cqe = wait_one(sim, ex1);
    CHECK(cqe.user_data == 0xA4 && cqe.result == -ENFILE && cqe.flags == 0);
    CHECK(vsr_sim_inflight(sim, 1) == 0);
    CHECK(recv_bytes(sim, ex0, client, bytes, sizeof(bytes)) == -ECONNRESET);
    CHECK(close_fd(sim, ex0, client) == 0);
    client = connect_to(sim, ex0, 1, PORT, &result);
    CHECK(result == 0); /* Queued: nobody accepts yet. */
    sqes[1] = make_sqe(VSR_IO_SQE_CLOSE, 0, 0xA5);
    sqes[1].flags = VSR_IO_SQE_FIXED_FILE;
    CHECK(run(sim, ex1, &sqes[1]).result == 0); /* Slot 0 is free. */
    submit(ex1, sqes, 1);
    cqe = wait_one(sim, ex1);
    CHECK(cqe.user_data == 0xA4 && cqe.result == 0 &&
          cqe.flags == VSR_IO_CQE_MORE);
    CHECK(send_bytes(sim, ex0, client, "slot", 4) == 4);
    sqes[1] = make_sqe(VSR_IO_SQE_RECV, 0, 0xA6);
    sqes[1].flags = VSR_IO_SQE_FIXED_FILE;
    sqes[1].addr = bytes;
    sqes[1].length = sizeof(bytes);
    CHECK(run(sim, ex1, &sqes[1]).result == 4 && memcmp(bytes, "slot", 4) == 0);
    sqes[1] = make_sqe(VSR_IO_SQE_CANCEL, -1, 0xA7);
    sqes[1].offset = 0xA4;
    submit(ex1, &sqes[1], 1);
    CHECK(reap(ex1, cqes, 2) == 2 && cqes[0].result == -ECANCELED);
    sqes[1] = make_sqe(VSR_IO_SQE_CLOSE, 0, 0xA8);
    sqes[1].flags = VSR_IO_SQE_FIXED_FILE;
    CHECK(run(sim, ex1, &sqes[1]).result == 0);
    CHECK(recv_bytes(sim, ex0, client, bytes, sizeof(bytes)) == 0);
    CHECK(close_fd(sim, ex0, client) == 0);
    CHECK(close_fd(sim, ex1, listener) == 0);

    /* O_DIRECT: offset and length must be block multiples; a misaligned
     * address is served. */
    fd = open_at(sim, ex0, VSR_SIM_ROOT, "direct", O_CREAT | O_RDWR | O_DIRECT);
    CHECK(fd >= 0);
    fill(direct_buffer, sizeof(direct_buffer), 0x77);
    CHECK(write_at(sim, ex0, fd, 0, direct_buffer + 1, (uint32_t)BLOCK) ==
          (int32_t)BLOCK);
    CHECK(write_at(sim, ex0, fd, BLOCK, direct_buffer, (uint32_t)BLOCK - 1) ==
          -EINVAL);
    CHECK(write_at(sim, ex0, fd, 1, direct_buffer, (uint32_t)BLOCK) == -EINVAL);
    CHECK(read_at(sim, ex0, fd, 0, direct_buffer + 3, (uint32_t)BLOCK) ==
          (int32_t)BLOCK);
    CHECK(read_at(sim, ex0, fd, 0, direct_buffer, 100) == -EINVAL);
    CHECK(read_at(sim, ex0, fd, 8, direct_buffer, (uint32_t)BLOCK) == -EINVAL);
    CHECK(close_fd(sim, ex0, fd) == 0);
    (void)settle(sim);
    vsr_sim_destroy(sim);
}

int main(int argc, char **argv)
{
    if (argc > 1) {
        seed = strtoull(argv[1], NULL, 0);
    }
    printf("sim_world seed %" PRIu64 "\n", seed);
    test_errors();
    test_clock();
    test_advance();
    test_partitions();
    test_close_holds();
    test_crash_model();
    test_directories();
    test_contract_details();
    test_reproducibility();
    test_aborts();
    return 0;
}
