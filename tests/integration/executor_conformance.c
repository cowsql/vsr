/* Executor conformance suite: one test body over a table of executor
 * factories, run byte-identical over the deterministic simulation
 * (vsr-sim.h: node 0 is the executor under test, node 1 the peer) and over
 * the real io_uring executor (loopback sockets, a temporary directory). It
 * checks section 8 "Executor contract" of docs/io-implementation.md, as
 * amended by decisions 45, 58 and 59 of docs/io-design.md, row by row.
 *
 * The factories differ only in how to wait (advance the world, or block on
 * the ring with a deadline), who the peer is (node 1's executor, or plain
 * non-blocking loopback sockets in this process), the directory descriptor
 * (VSR_SIM_ROOT, or an opened mkdtemp directory), the wake source (the same
 * thread, or another thread) and a few environment facts (addresses, disk
 * capacity, socket buffer sizes). Everything else is one code path over
 * executor records. The ring runs as three columns: the default ring,
 * uring-sqpoll (an SQ thread) and uring-napi (NAPI busy polling).
 *
 * Two more factories run the same bodies through the fault-injecting
 * wrapper (tests/lib/faulty_executor.c) at rate zero over each executor:
 * with every rate zero the wrapper must be transparent.
 *
 * Every scenario runs in a forked child with a fresh fixture, so an abort
 * (a CHECK, a sanitizer report, the simulation's ownership checks) fails
 * that scenario alone. The driver prints `factory: scenario: PASS|FAIL|
 * SKIP(reason)` per pair and exits 1 when anything failed, 77 when no ring
 * could be created and the simulation passed, else 0. Setting
 * VSR_CONFORMANCE_ONLY to a scenario name runs that scenario alone. */
#define _GNU_SOURCE
#include "config.h"

#include "lib/check.h"
#include "lib/faulty_executor.h"
#include "vsr-io.h"
#include "vsr-sim.h"

#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define MS UINT64_C(1000000)
#define SECOND (1000 * MS)
#define STASH 1024u
#define PEERS 8u
#define TAKE_MS 5000u
#define SIM_STEPS 4000000u
#define ALIGNED 4096u /* Aligned direct I/O unit for every block size. */
#define SIM_CAPACITY (UINT64_C(16) << 20)
#define BIG (8u << 20)

/* user_data carries an owner tag so the whole width is checked. */
#define UD(value) VSR_IO_USER_DATA(0x5a, (uint64_t)(value))

struct fixture;

/* The factory: everything that differs between the executors. */
struct factory {
    const char *name;
    bool temp_directory; /* The driver makes and removes fixture.dir_path. */
    bool ring;           /* Skipped entirely when no ring can be created. */
    /* The ring's variant: an SQPOLL thread idling after this many ms, NAPI
     * busy polling for this many us (0: the default task-work ring). */
    uint32_t sqpoll_idle_ms;
    uint32_t napi_busy_poll_us;
    /* 0, or a negative errno; -ENOSYS/-EPERM mean no such executor here. */
    int (*create)(struct fixture *f);
    void (*destroy)(struct fixture *f);
    /* Records a wait through submit_and_wait and lets the world run until
     * it is satisfied; 0 when the wait returned. */
    int (*wait)(struct fixture *f, uint32_t want, uint64_t min_wait_ns,
                uint64_t deadline_ns);
    /* Arranges a wake that reaches the next (or a blocked) wait. */
    void (*wake_elsewhere)(struct fixture *f);
    /* 1 when the file's filesystem enforces direct I/O alignment. */
    int (*direct_alignment)(struct fixture *f, int32_t fd);
    /* Cuts (1) or heals (0) the route to the peer; -ENOTSUP if none. */
    int (*partition)(struct fixture *f, int cut);
    /* The address the executor binds a listener to, what a peer dials for
     * that listener, and an address nothing listens on. */
    void (*bind_address)(struct fixture *f, struct vsr_io_address *out);
    void (*dial_address)(struct fixture *f, int32_t listener,
                         struct vsr_io_address *out);
    void (*unused_address)(struct fixture *f, struct vsr_io_address *out);
    /* Bounds the executor socket's send buffer so a large send is short. */
    void (*shrink_send_buffer)(struct fixture *f, int32_t fd);
    /* The peer: handles are small non-negative integers; results are 0 or
     * counts, or a negative errno. */
    int (*peer_listen)(struct fixture *f, bool small_buffers,
                       struct vsr_io_address *out);
    int (*peer_accept)(struct fixture *f, int listener);
    int (*peer_connect)(struct fixture *f, const struct vsr_io_address *to);
    int (*peer_send)(struct fixture *f, int peer, const void *bytes,
                     size_t size);
    int (*peer_recv)(struct fixture *f, int peer, void *bytes, size_t size);
    int (*peer_shutdown)(struct fixture *f, int peer);
    int (*peer_close)(struct fixture *f, int peer, bool reset);
};

struct fixture {
    const struct factory *factory;
    struct vsr_io_executor ex;
    int32_t dir;            /* Directory descriptor for the path records. */
    uint64_t disk_capacity; /* 0: unbounded. */
    struct vsr_io_cqe stash[STASH];
    uint32_t stashed;
    uint64_t sequence;
    int peers[PEERS]; /* -1: free. */
    /* Simulation. */
    struct vsr_sim *sim;
    struct vsr_io_executor peer_ex;
    uint16_t next_port;
    uint16_t bound_port;
    uint64_t peer_sequence;
    /* The wrapper at rate zero (NULL: none) and the executor it wraps,
     * which the factory's own functions use. */
    struct faulty_executor *faulty;
    struct vsr_io_executor inner;
    bool faults; /* A scenario raised the wrapper's rates. */
    /* Ring. */
    void *memory;
    char dir_path[64];
    pthread_t waker;
    bool waker_live;
};

static int reason_fd = -1;

static void report(const char *format, ...)
    __attribute__((format(printf, 1, 2)));
static _Noreturn void skip(const char *format, ...)
    __attribute__((format(printf, 1, 2)));

static void report(const char *format, ...)
{
    char text[256];
    va_list args;
    size_t length;

    va_start(args, format);
    (void)vsnprintf(text, sizeof(text), format, args);
    va_end(args);
    length = strlen(text);
    if (reason_fd >= 0) {
        (void)!write(reason_fd, text, length);
    }
}

static _Noreturn void skip(const char *format, ...)
{
    char text[256];
    va_list args;

    va_start(args, format);
    (void)vsnprintf(text, sizeof(text), format, args);
    va_end(args);
    report("%s", text);
    exit(77);
}

/* ------------------------------------------------------------------------
 * Shared helpers over executor records
 * --------------------------------------------------------------------- */

static uint64_t now(const struct fixture *f)
{
    return f->ex.ops->now(f->ex.ctx);
}

static uint64_t next_ud(struct fixture *f)
{
    return UD(0x100000 + ++f->sequence);
}

static struct vsr_io_sqe rec(uint8_t opcode, uint64_t user_data)
{
    struct vsr_io_sqe r;

    memset(&r, 0, sizeof(r));
    r.opcode = opcode;
    r.user_data = user_data;
    return r;
}

/* Submits without waiting. */
static void submit(struct fixture *f, const struct vsr_io_sqe *records,
                   uint32_t count)
{
    CHECK(f->ex.ops->submit_and_wait(f->ex.ctx, records, count, 0, 0, 0) == 0);
}

static void submit1(struct fixture *f, struct vsr_io_sqe record)
{
    submit(f, &record, 1);
}

static void reap_into_stash(struct fixture *f)
{
    f->stashed +=
        f->ex.ops->reap(f->ex.ctx, f->stash + f->stashed, STASH - f->stashed);
}

/* One wait for a completion, bounded by the deadline, then a reap. */
static void pump(struct fixture *f, uint64_t deadline)
{
    CHECK(f->factory->wait(f, 1, 0, deadline) == 0);
    reap_into_stash(f);
}

static bool find(struct fixture *f, uint64_t user_data, struct vsr_io_cqe *out)
{
    for (uint32_t i = 0; i < f->stashed; ++i) {
        if (f->stash[i].user_data == user_data) {
            *out = f->stash[i];
            memmove(&f->stash[i], &f->stash[i + 1],
                    (f->stashed - i - 1) * sizeof(f->stash[0]));
            f->stashed--;
            return true;
        }
    }
    return false;
}

/* The next completion of user_data, within TAKE_MS. */
static struct vsr_io_cqe take(struct fixture *f, uint64_t user_data)
{
    uint64_t deadline = now(f) + TAKE_MS * MS;
    struct vsr_io_cqe cqe;

    while (!find(f, user_data, &cqe)) {
        if (now(f) >= deadline) {
            fprintf(stderr, "no completion for user_data 0x%llx\n",
                    (unsigned long long)user_data);
            CHECK(false);
        }
        pump(f, deadline);
    }
    return cqe;
}

/* The oldest completion not yet taken, in reap order. */
static struct vsr_io_cqe take_next(struct fixture *f)
{
    uint64_t deadline = now(f) + TAKE_MS * MS;
    struct vsr_io_cqe cqe;

    while (f->stashed == 0) {
        CHECK(now(f) < deadline);
        pump(f, deadline);
    }
    cqe = f->stash[0];
    memmove(&f->stash[0], &f->stash[1], (f->stashed - 1) * sizeof(f->stash[0]));
    f->stashed--;
    return cqe;
}

/* True when user_data completes within `ms`. */
static bool arrives(struct fixture *f, uint64_t user_data, uint64_t ms,
                    struct vsr_io_cqe *out)
{
    uint64_t deadline = now(f) + ms * MS;

    while (!find(f, user_data, out)) {
        if (now(f) >= deadline) {
            return false;
        }
        pump(f, deadline);
    }
    return true;
}

static int32_t run(struct fixture *f, struct vsr_io_sqe record)
{
    submit1(f, record);
    return take(f, record.user_data).result;
}

/* Lets `ms` of executor time pass, with everything due in it delivered. */
static void settle(struct fixture *f, uint64_t ms)
{
    struct vsr_io_sqe r = rec(VSR_IO_SQE_TIMEOUT, next_ud(f));

    r.offset = ms * MS;
    CHECK(run(f, r) == -ETIME);
}

static void *page_alloc(size_t size)
{
    void *p = aligned_alloc(4096, (size + 4095) & ~(size_t)4095);

    CHECK(p != NULL);
    memset(p, 0, size);
    return p;
}

/* vsr_io_vec.base is not const-qualified; a write or send only reads it. */
static void *readable(const char *text)
{
    /* NOLINTNEXTLINE(performance-no-int-to-ptr): only drops const */
    return (void *)(uintptr_t)text;
}

static int peer_alloc(struct fixture *f, int value)
{
    for (uint32_t i = 0; i < PEERS; ++i) {
        if (f->peers[i] < 0) {
            f->peers[i] = value;
            return (int)i;
        }
    }
    CHECK(false);
    return -1;
}

static struct vsr_io_sqe io_record(uint8_t opcode, int32_t fd, const void *addr,
                                   uint32_t length, uint64_t offset,
                                   uint64_t user_data)
{
    struct vsr_io_sqe r = rec(opcode, user_data);

    r.fd = fd;
    r.addr = addr;
    r.length = length;
    r.offset = offset;
    return r;
}

static int32_t io(struct fixture *f, uint8_t opcode, int32_t fd,
                  const void *addr, uint32_t length, uint64_t offset,
                  uint64_t user_data)
{
    return run(f, io_record(opcode, fd, addr, length, offset, user_data));
}

static int32_t fixed_io(struct fixture *f, uint8_t opcode, int32_t fd,
                        const void *addr, uint32_t length, uint64_t offset,
                        uint16_t index, uint64_t user_data)
{
    struct vsr_io_sqe r =
        io_record(opcode, fd, addr, length, offset, user_data);

    r.flags = VSR_IO_SQE_FIXED_BUFFER;
    r.buffer_index = index;
    return run(f, r);
}

static int32_t open_at(struct fixture *f, int32_t dir, const char *name,
                       uint32_t flags, uint64_t user_data)
{
    struct vsr_io_sqe r = rec(VSR_IO_SQE_OPENAT, user_data);

    r.fd = dir;
    r.addr = name;
    r.op_flags = flags;
    r.length = 0644;
    return run(f, r);
}

static int32_t close_fd(struct fixture *f, int32_t fd, bool fixed)
{
    struct vsr_io_sqe r = rec(VSR_IO_SQE_CLOSE, next_ud(f));

    r.fd = fd;
    r.flags = fixed ? VSR_IO_SQE_FIXED_FILE : 0;
    return run(f, r);
}

static int32_t make_socket(struct fixture *f, uint32_t domain)
{
    struct vsr_io_sqe r = rec(VSR_IO_SQE_SOCKET, next_ud(f));

    r.length = domain;
    r.op_flags = SOCK_STREAM | SOCK_CLOEXEC;
    return run(f, r);
}

static int32_t connect_to(struct fixture *f, int32_t fd, bool fixed,
                          const struct vsr_io_address *address,
                          uint64_t user_data)
{
    struct vsr_io_sqe r = rec(VSR_IO_SQE_CONNECT, user_data);

    r.fd = fd;
    r.flags = fixed ? VSR_IO_SQE_FIXED_FILE : 0;
    r.addr = &address->sockaddr;
    r.length = address->length;
    return run(f, r);
}

/* A listening socket of the executor; `dial` is what the peer connects to. */
static int32_t open_listener(struct fixture *f, struct vsr_io_address *dial)
{
    struct vsr_io_address address;
    struct vsr_io_sqe r;
    int32_t fd = make_socket(f, AF_INET);

    CHECK(fd >= 0);
    f->factory->bind_address(f, &address);
    r = rec(VSR_IO_SQE_BIND, next_ud(f));
    r.fd = fd;
    r.addr = &address.sockaddr;
    r.length = address.length;
    CHECK(run(f, r) == 0);
    r = rec(VSR_IO_SQE_LISTEN, next_ud(f));
    r.fd = fd;
    r.length = 16;
    CHECK(run(f, r) == 0);
    f->factory->dial_address(f, fd, dial);
    return fd;
}

struct link {
    int32_t listener;
    int32_t server; /* The executor's end. */
    int peer;       /* The peer's end. */
};

/* The executor accepts one connection from the peer (a single accept). */
static void open_link(struct fixture *f, struct link *l)
{
    struct vsr_io_address dial;
    struct vsr_io_sqe r;
    struct vsr_io_cqe cqe;

    l->listener = open_listener(f, &dial);
    r = rec(VSR_IO_SQE_ACCEPT, next_ud(f));
    r.fd = l->listener;
    submit1(f, r);
    l->peer = f->factory->peer_connect(f, &dial);
    CHECK(l->peer >= 0);
    cqe = take(f, r.user_data);
    CHECK(cqe.result >= 0 && cqe.flags == 0);
    l->server = cqe.result;
}

static void close_link(struct fixture *f, struct link *l)
{
    CHECK(close_fd(f, l->server, false) == 0);
    CHECK(close_fd(f, l->listener, false) == 0);
    CHECK(f->factory->peer_close(f, l->peer, false) == 0);
}

/* The executor connects to a listening peer; returns the executor's end. */
static int32_t dial_peer(struct fixture *f, bool small_buffers, int *peer)
{
    struct vsr_io_address address;
    int listener = f->factory->peer_listen(f, small_buffers, &address);
    int32_t fd = make_socket(f, AF_INET);

    CHECK(listener >= 0 && fd >= 0);
    if (small_buffers) {
        f->factory->shrink_send_buffer(f, fd);
    }
    CHECK(connect_to(f, fd, false, &address, next_ud(f)) == 0);
    *peer = f->factory->peer_accept(f, listener);
    CHECK(*peer >= 0);
    CHECK(f->factory->peer_close(f, listener, false) == 0);
    return fd;
}

static struct vsr_io_sqe send_record(int32_t fd, bool fixed_file,
                                     const void *bytes, uint32_t length,
                                     uint64_t user_data)
{
    struct vsr_io_sqe r = rec(VSR_IO_SQE_SEND, user_data);

    r.fd = fd;
    r.flags = fixed_file ? VSR_IO_SQE_FIXED_FILE : 0;
    r.addr = bytes;
    r.length = length;
    return r;
}

static struct vsr_io_sqe recv_record(int32_t fd, bool fixed_file, void *bytes,
                                     uint32_t length, uint64_t user_data)
{
    struct vsr_io_sqe r = rec(VSR_IO_SQE_RECV, user_data);

    r.fd = fd;
    r.flags = fixed_file ? VSR_IO_SQE_FIXED_FILE : 0;
    r.addr = bytes;
    r.length = length;
    return r;
}

/* Receives exactly `length` bytes on a plain descriptor. */
static void receive_exact(struct fixture *f, int32_t fd, void *bytes,
                          uint32_t length)
{
    uint32_t got = 0;

    while (got < length) {
        int32_t n = run(f, recv_record(fd, false, (unsigned char *)bytes + got,
                                       length - got, next_ud(f)));

        CHECK(n > 0);
        got += (uint32_t)n;
    }
}

static void peer_receive_exact(struct fixture *f, int peer, void *bytes,
                               size_t length)
{
    size_t got = 0;

    while (got < length) {
        int n = f->factory->peer_recv(f, peer, (unsigned char *)bytes + got,
                                      length - got);

        CHECK(n > 0);
        got += (size_t)n;
    }
}

static int32_t cancel_user_data(struct fixture *f, uint64_t target)
{
    struct vsr_io_sqe r = rec(VSR_IO_SQE_CANCEL, next_ud(f));

    r.offset = target;
    return run(f, r);
}

static void fill_pattern(unsigned char *bytes, size_t size)
{
    for (size_t i = 0; i < size; ++i) {
        bytes[i] = (unsigned char)('A' + (i * 7 + i / 26) % 26);
    }
}

/* ------------------------------------------------------------------------
 * Factory: simulation
 * --------------------------------------------------------------------- */

static int sim_create(struct fixture *f)
{
    struct vsr_sim_options options;

    memset(&options, 0, sizeof(options));
    options.seed = 1;
    options.nodes = 2;
    options.block_bytes = 512;
    options.file_slots = 16;
    options.buffer_regions = 4;
    options.faults.disk.capacity_bytes = SIM_CAPACITY;
    CHECK(vsr_sim_create(&options, &f->sim) == 0);
    f->ex = vsr_sim_executor(f->sim, 0);
    f->peer_ex = vsr_sim_executor(f->sim, 1);
    CHECK(f->ex.ops != NULL && f->peer_ex.ops != NULL);
    f->dir = VSR_SIM_ROOT;
    f->disk_capacity = SIM_CAPACITY;
    f->next_port = 7000;
    return 0;
}

static void sim_destroy(struct fixture *f)
{
    vsr_sim_destroy(f->sim);
    f->sim = NULL;
}

static int sim_wait(struct fixture *f, uint32_t want, uint64_t min_wait_ns,
                    uint64_t deadline_ns)
{
    CHECK(f->ex.ops->submit_and_wait(f->ex.ctx, NULL, 0, want, min_wait_ns,
                                     deadline_ns) == 0);
    for (uint32_t step = 0; step < SIM_STEPS; ++step) {
        if (vsr_sim_ready(f->sim, 0)) {
            return 0;
        }
        if (vsr_sim_advance(f->sim) < 0) {
            return -ETIMEDOUT; /* Only a harness action could progress. */
        }
    }
    return -ETIMEDOUT;
}

static void sim_wake_elsewhere(struct fixture *f)
{
    f->ex.ops->wake(f->ex.ctx);
}

static int sim_direct_alignment(struct fixture *f, int32_t fd)
{
    (void)f;
    (void)fd;
    return 1;
}

static int sim_partition(struct fixture *f, int cut)
{
    vsr_sim_partition(f->sim, 0, 1, cut);
    return 0;
}

static void sim_bind_address(struct fixture *f, struct vsr_io_address *out)
{
    f->bound_port = f->next_port++;
    *out = vsr_sim_address(f->sim, 0, f->bound_port);
}

static void sim_dial_address(struct fixture *f, int32_t listener,
                             struct vsr_io_address *out)
{
    (void)listener;
    *out = vsr_sim_address(f->sim, 0, f->bound_port);
}

static void sim_unused_address(struct fixture *f, struct vsr_io_address *out)
{
    *out = vsr_sim_address(f->sim, 1, 9);
}

static void sim_shrink_send_buffer(struct fixture *f, int32_t fd)
{
    (void)f;
    (void)fd; /* The simulated socket buffer is bounded already. */
}

/* Runs one record on the peer node to completion. */
static int32_t sim_peer_run(struct fixture *f, struct vsr_io_sqe record)
{
    struct vsr_io_cqe cqes[16];

    record.user_data = VSR_IO_USER_DATA(0x99, ++f->peer_sequence);
    CHECK(f->peer_ex.ops->submit_and_wait(f->peer_ex.ctx, &record, 1, 1, 0,
                                          VSR_NO_DEADLINE) == 0);
    for (uint32_t step = 0; step < SIM_STEPS; ++step) {
        uint32_t n = f->peer_ex.ops->reap(f->peer_ex.ctx, cqes, 16);

        /* One record at a time: any completion is this one's. */
        CHECK(n <= 1);
        if (n == 1) {
            CHECK(cqes[0].user_data == record.user_data);
            return cqes[0].result;
        }
        if (vsr_sim_advance(f->sim) < 0) {
            return -ETIMEDOUT;
        }
    }
    return -ETIMEDOUT;
}

static int32_t sim_peer_socket(struct fixture *f)
{
    struct vsr_io_sqe r = rec(VSR_IO_SQE_SOCKET, 0);

    r.length = AF_INET;
    r.op_flags = SOCK_STREAM;
    return sim_peer_run(f, r);
}

static int sim_peer_listen(struct fixture *f, bool small_buffers,
                           struct vsr_io_address *out)
{
    struct vsr_io_sqe r;
    int32_t fd = sim_peer_socket(f);
    int32_t rc;

    (void)small_buffers;
    if (fd < 0) {
        return fd;
    }
    *out = vsr_sim_address(f->sim, 1, f->next_port++);
    r = rec(VSR_IO_SQE_BIND, 0);
    r.fd = fd;
    r.addr = &out->sockaddr;
    r.length = out->length;
    rc = sim_peer_run(f, r);
    if (rc == 0) {
        r = rec(VSR_IO_SQE_LISTEN, 0);
        r.fd = fd;
        r.length = 16;
        rc = sim_peer_run(f, r);
    }
    return rc == 0 ? peer_alloc(f, fd) : rc;
}

static int sim_peer_accept(struct fixture *f, int listener)
{
    struct vsr_io_sqe r = rec(VSR_IO_SQE_ACCEPT, 0);
    int32_t fd;

    r.fd = f->peers[listener];
    fd = sim_peer_run(f, r);
    return fd < 0 ? fd : peer_alloc(f, fd);
}

static int sim_peer_connect(struct fixture *f, const struct vsr_io_address *to)
{
    struct vsr_io_sqe r;
    int32_t fd = sim_peer_socket(f);
    int32_t rc;

    if (fd < 0) {
        return fd;
    }
    r = rec(VSR_IO_SQE_CONNECT, 0);
    r.fd = fd;
    r.addr = &to->sockaddr;
    r.length = to->length;
    rc = sim_peer_run(f, r);
    return rc == 0 ? peer_alloc(f, fd) : rc;
}

static int sim_peer_send(struct fixture *f, int peer, const void *bytes,
                         size_t size)
{
    size_t sent = 0;

    while (sent < size) {
        struct vsr_io_sqe r = rec(VSR_IO_SQE_SEND, 0);
        int32_t n;

        r.fd = f->peers[peer];
        r.addr = (const unsigned char *)bytes + sent;
        r.length = (uint32_t)(size - sent);
        n = sim_peer_run(f, r);
        if (n < 0) {
            return n;
        }
        sent += (size_t)n;
    }
    return 0;
}

static int sim_peer_recv(struct fixture *f, int peer, void *bytes, size_t size)
{
    struct vsr_io_sqe r = rec(VSR_IO_SQE_RECV, 0);

    r.fd = f->peers[peer];
    r.addr = bytes;
    r.length = (uint32_t)size;
    return sim_peer_run(f, r);
}

static int sim_peer_shutdown(struct fixture *f, int peer)
{
    struct vsr_io_sqe r = rec(VSR_IO_SQE_SHUTDOWN, 0);

    r.fd = f->peers[peer];
    r.length = SHUT_WR;
    return sim_peer_run(f, r);
}

static int sim_peer_close(struct fixture *f, int peer, bool reset)
{
    struct vsr_io_sqe r = rec(VSR_IO_SQE_CLOSE, 0);
    int32_t rc;

    if (reset) {
        vsr_sim_reset(f->sim, 0, 1);
    }
    r.fd = f->peers[peer];
    rc = sim_peer_run(f, r);
    f->peers[peer] = -1;
    return rc;
}

static const struct factory sim_factory = {
    .name = "sim",
    .create = sim_create,
    .destroy = sim_destroy,
    .wait = sim_wait,
    .wake_elsewhere = sim_wake_elsewhere,
    .direct_alignment = sim_direct_alignment,
    .partition = sim_partition,
    .bind_address = sim_bind_address,
    .dial_address = sim_dial_address,
    .unused_address = sim_unused_address,
    .shrink_send_buffer = sim_shrink_send_buffer,
    .peer_listen = sim_peer_listen,
    .peer_accept = sim_peer_accept,
    .peer_connect = sim_peer_connect,
    .peer_send = sim_peer_send,
    .peer_recv = sim_peer_recv,
    .peer_shutdown = sim_peer_shutdown,
    .peer_close = sim_peer_close,
};

/* ------------------------------------------------------------------------
 * Factory: io_uring
 * --------------------------------------------------------------------- */

static int ring_open(struct fixture *f)
{
    struct vsr_io_uring_options o;
    struct vsr_io_need need;
    size_t size;
    int rc;

    memset(&o, 0, sizeof(o));
    o.sq_entries = 32;
    o.cq_entries = 256;
    o.file_slots = 16;
    o.buffer_regions = 4;
    o.sqpoll_idle_ms = f->factory->sqpoll_idle_ms;
    o.sqpoll_cpu = UINT32_MAX;
    o.napi_busy_poll_us = f->factory->napi_busy_poll_us;
    CHECK(vsr_io_uring_layout(&o, &need) == VSR_OK);
    size = (need.size + need.alignment - 1) & ~(need.alignment - 1);
    f->memory = aligned_alloc(need.alignment, size);
    CHECK(f->memory != NULL);
    rc = vsr_io_uring_init(f->memory, need.size, &o, &f->ex);
    if (rc != 0) {
        free(f->memory);
        f->memory = NULL;
    }
    return rc;
}

static void ring_close(struct fixture *f)
{
    if (f->faulty != NULL) {
        f->ex = f->inner;
    }
    vsr_io_uring_deinit(&f->ex);
    free(f->memory);
    f->memory = NULL;
}

static int ring_create(struct fixture *f)
{
    int rc = ring_open(f);

    if (rc != 0) {
        return rc;
    }
    f->dir = open(f->dir_path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    CHECK(f->dir >= 0);
    f->disk_capacity = 0;
    return 0;
}

static void ring_destroy(struct fixture *f)
{
    if (f->waker_live) {
        CHECK(pthread_join(f->waker, NULL) == 0);
        f->waker_live = false;
    }
    for (uint32_t i = 0; i < PEERS; ++i) {
        if (f->peers[i] >= 0) {
            (void)close(f->peers[i]);
            f->peers[i] = -1;
        }
    }
    ring_close(f);
    CHECK(close(f->dir) == 0);
}

static int ring_wait(struct fixture *f, uint32_t want, uint64_t min_wait_ns,
                     uint64_t deadline_ns)
{
    return f->ex.ops->submit_and_wait(f->ex.ctx, NULL, 0, want, min_wait_ns,
                                      deadline_ns);
}

/* Runs the ring's deferred work without waiting, so that the executor's
 * socket operations progress while the peer works in this thread. */
static void ring_drain(struct fixture *f)
{
    CHECK(f->ex.ops->submit_and_wait(f->ex.ctx, NULL, 0, 0, 0, 0) == 0);
    reap_into_stash(f);
}

static void ring_yield(struct fixture *f, int fd, short events)
{
    struct pollfd pfd;

    pfd.fd = fd;
    pfd.events = events;
    pfd.revents = 0;
    (void)poll(&pfd, 1, 10);
    ring_drain(f);
}

static void *ring_waker(void *arg)
{
    struct fixture *f = arg;
    struct timespec ts;

    ts.tv_sec = 0;
    ts.tv_nsec = 50 * 1000000L;
    (void)nanosleep(&ts, NULL);
    f->ex.ops->wake(f->ex.ctx);
    return NULL;
}

static void ring_wake_elsewhere(struct fixture *f)
{
    if (f->waker_live) {
        CHECK(pthread_join(f->waker, NULL) == 0);
    }
    CHECK(pthread_create(&f->waker, NULL, ring_waker, f) == 0);
    f->waker_live = true;
}

/* Probes the filesystem: a direct read at a misaligned offset must be
 * refused (btrfs and tmpfs serve it buffered instead). */
static int ring_direct_alignment(struct fixture *f, int32_t fd)
{
    unsigned char *page = page_alloc(8192);
    ssize_t n;

    (void)f;
    n = pread(fd, page, 512, 100);
    free(page);
    return n < 0 && errno == EINVAL ? 1 : 0;
}

static int ring_partition(struct fixture *f, int cut)
{
    (void)f;
    (void)cut;
    return -ENOTSUP;
}

static void loopback_address(struct vsr_io_address *out)
{
    struct sockaddr_in in;

    memset(out, 0, sizeof(*out));
    memset(&in, 0, sizeof(in));
    in.sin_family = AF_INET;
    in.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    memcpy(&out->sockaddr, &in, sizeof(in));
    out->length = (uint32_t)sizeof(in);
}

static void socket_name(int fd, struct vsr_io_address *out)
{
    socklen_t length;

    memset(out, 0, sizeof(*out));
    length = sizeof(out->sockaddr);
    CHECK(getsockname(fd, (struct sockaddr *)&out->sockaddr, &length) == 0);
    out->length = (uint32_t)length;
}

static void ring_bind_address(struct fixture *f, struct vsr_io_address *out)
{
    (void)f;
    loopback_address(out);
}

static void ring_dial_address(struct fixture *f, int32_t listener,
                              struct vsr_io_address *out)
{
    (void)f;
    socket_name(listener, out);
}

static void ring_unused_address(struct fixture *f, struct vsr_io_address *out)
{
    struct vsr_io_address any;
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);

    (void)f;
    CHECK(fd >= 0);
    loopback_address(&any);
    CHECK(bind(fd, (struct sockaddr *)&any.sockaddr, any.length) == 0);
    socket_name(fd, out);
    CHECK(close(fd) == 0);
}

static void ring_shrink_send_buffer(struct fixture *f, int32_t fd)
{
    int size = 4096;

    (void)f;
    CHECK(setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &size, sizeof(size)) == 0);
}

static int ring_peer_listen(struct fixture *f, bool small_buffers,
                            struct vsr_io_address *out)
{
    struct vsr_io_address any;
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    int size = 4096;

    CHECK(fd >= 0);
    if (small_buffers) {
        CHECK(setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &size, sizeof(size)) == 0);
    }
    loopback_address(&any);
    CHECK(bind(fd, (struct sockaddr *)&any.sockaddr, any.length) == 0);
    CHECK(listen(fd, 16) == 0);
    socket_name(fd, out);
    return peer_alloc(f, fd);
}

static int ring_peer_accept(struct fixture *f, int listener)
{
    uint64_t until = now(f) + TAKE_MS * MS;

    while (now(f) < until) {
        int fd = accept4(f->peers[listener], NULL, NULL,
                         SOCK_NONBLOCK | SOCK_CLOEXEC);

        if (fd >= 0) {
            return peer_alloc(f, fd);
        }
        if (errno != EAGAIN) {
            return -errno;
        }
        ring_yield(f, f->peers[listener], POLLIN);
    }
    return -ETIMEDOUT;
}

static int ring_peer_connect(struct fixture *f, const struct vsr_io_address *to)
{
    struct vsr_io_address address = *to;
    uint64_t until = now(f) + TAKE_MS * MS;
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    int error = 0;
    socklen_t length = sizeof(error);

    CHECK(fd >= 0);
    if (connect(fd, (struct sockaddr *)&address.sockaddr, address.length) !=
        0) {
        if (errno != EINPROGRESS) {
            error = errno;
            (void)close(fd);
            return -error;
        }
        for (;;) {
            struct pollfd pfd;

            pfd.fd = fd;
            pfd.events = POLLOUT;
            pfd.revents = 0;
            if (poll(&pfd, 1, 10) == 1) {
                break;
            }
            ring_drain(f);
            if (now(f) >= until) {
                (void)close(fd);
                return -ETIMEDOUT;
            }
        }
        CHECK(getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &length) == 0);
        if (error != 0) {
            (void)close(fd);
            return -error;
        }
    }
    return peer_alloc(f, fd);
}

static int ring_peer_send(struct fixture *f, int peer, const void *bytes,
                          size_t size)
{
    uint64_t until = now(f) + TAKE_MS * MS;
    size_t sent = 0;

    while (sent < size) {
        ssize_t n = send(f->peers[peer], (const unsigned char *)bytes + sent,
                         size - sent, MSG_NOSIGNAL | MSG_DONTWAIT);

        if (n > 0) {
            sent += (size_t)n;
            continue;
        }
        if (n < 0 && errno != EAGAIN) {
            return -errno;
        }
        if (now(f) >= until) {
            return -ETIMEDOUT;
        }
        ring_yield(f, f->peers[peer], POLLOUT);
    }
    return 0;
}

static int ring_peer_recv(struct fixture *f, int peer, void *bytes, size_t size)
{
    uint64_t until = now(f) + TAKE_MS * MS;

    for (;;) {
        ssize_t n = recv(f->peers[peer], bytes, size, MSG_DONTWAIT);

        if (n >= 0) {
            return (int)n;
        }
        if (errno != EAGAIN) {
            return -errno;
        }
        if (now(f) >= until) {
            return -ETIMEDOUT;
        }
        ring_yield(f, f->peers[peer], POLLIN);
    }
}

static int ring_peer_shutdown(struct fixture *f, int peer)
{
    return shutdown(f->peers[peer], SHUT_WR) == 0 ? 0 : -errno;
}

static int ring_peer_close(struct fixture *f, int peer, bool reset)
{
    int fd = f->peers[peer];
    int rc;

    if (reset) {
        struct linger l;

        l.l_onoff = 1;
        l.l_linger = 0;
        CHECK(setsockopt(fd, SOL_SOCKET, SO_LINGER, &l, sizeof(l)) == 0);
    }
    rc = close(fd) == 0 ? 0 : -errno;
    f->peers[peer] = -1;
    return rc;
}

static const struct factory ring_factory = {
    .name = "uring",
    .temp_directory = true,
    .ring = true,
    .create = ring_create,
    .destroy = ring_destroy,
    .wait = ring_wait,
    .wake_elsewhere = ring_wake_elsewhere,
    .direct_alignment = ring_direct_alignment,
    .partition = ring_partition,
    .bind_address = ring_bind_address,
    .dial_address = ring_dial_address,
    .unused_address = ring_unused_address,
    .shrink_send_buffer = ring_shrink_send_buffer,
    .peer_listen = ring_peer_listen,
    .peer_accept = ring_peer_accept,
    .peer_connect = ring_peer_connect,
    .peer_send = ring_peer_send,
    .peer_recv = ring_peer_recv,
    .peer_shutdown = ring_peer_shutdown,
    .peer_close = ring_peer_close,
};

/* The same ring with a kernel thread polling the SQ (idling after 10 ms,
 * so both the awake thread and the NEED_WAKEUP path are taken) in place of
 * the task-work flags, and with NAPI busy polling registered. Loopback
 * sockets carry no NAPI id, so the second exercises the registration and
 * the wait path with NAPI enabled, not a busy poll of a device queue. */
static const struct factory ring_sqpoll_factory = {
    .name = "uring-sqpoll",
    .temp_directory = true,
    .ring = true,
    .sqpoll_idle_ms = 10,
    .create = ring_create,
    .destroy = ring_destroy,
    .wait = ring_wait,
    .wake_elsewhere = ring_wake_elsewhere,
    .direct_alignment = ring_direct_alignment,
    .partition = ring_partition,
    .bind_address = ring_bind_address,
    .dial_address = ring_dial_address,
    .unused_address = ring_unused_address,
    .shrink_send_buffer = ring_shrink_send_buffer,
    .peer_listen = ring_peer_listen,
    .peer_accept = ring_peer_accept,
    .peer_connect = ring_peer_connect,
    .peer_send = ring_peer_send,
    .peer_recv = ring_peer_recv,
    .peer_shutdown = ring_peer_shutdown,
    .peer_close = ring_peer_close,
};

static const struct factory ring_napi_factory = {
    .name = "uring-napi",
    .temp_directory = true,
    .ring = true,
    .napi_busy_poll_us = 20,
    .create = ring_create,
    .destroy = ring_destroy,
    .wait = ring_wait,
    .wake_elsewhere = ring_wake_elsewhere,
    .direct_alignment = ring_direct_alignment,
    .partition = ring_partition,
    .bind_address = ring_bind_address,
    .dial_address = ring_dial_address,
    .unused_address = ring_unused_address,
    .shrink_send_buffer = ring_shrink_send_buffer,
    .peer_listen = ring_peer_listen,
    .peer_accept = ring_peer_accept,
    .peer_connect = ring_peer_connect,
    .peer_send = ring_peer_send,
    .peer_recv = ring_peer_recv,
    .peer_shutdown = ring_peer_shutdown,
    .peer_close = ring_peer_close,
};

/* ------------------------------------------------------------------------
 * Scenarios: basics and common flags
 * --------------------------------------------------------------------- */

/* NOP: result 0; user_data returned unchanged over its whole width;
 * independent records of one batch all complete. */
static void scenario_nop(struct fixture *f)
{
    static const uint64_t values[] = {UD(1), 0, UINT64_MAX - 1,
                                      VSR_IO_USER_DATA(0xff, UINT64_MAX - 1)};
    struct vsr_io_sqe batch[8];

    for (size_t i = 0; i < sizeof(values) / sizeof(values[0]); ++i) {
        struct vsr_io_sqe r = rec(VSR_IO_SQE_NOP, values[i]);
        struct vsr_io_cqe cqe;

        if (f->faulty != NULL && values[i] == FAULTY_EXECUTOR_USER_DATA) {
            /* The wrapper's one reserved value, for its own CANCELs. */
            CHECK(f->ex.ops->submit_and_wait(f->ex.ctx, &r, 1, 0, 0, 0) ==
                  -EINVAL);
            continue;
        }
        submit1(f, r);
        cqe = take(f, values[i]);
        CHECK(cqe.user_data == values[i]);
        CHECK(cqe.result == 0 && cqe.flags == 0 && cqe.buffer_id == 0);
    }
    for (uint32_t i = 0; i < 8; ++i) {
        batch[i] = rec(VSR_IO_SQE_NOP, UD(100 + i));
    }
    submit(f, batch, 8);
    for (uint32_t i = 0; i < 8; ++i) {
        CHECK(take(f, UD(100 + i)).result == 0);
    }
}

/* SKIP_SUCCESS: no completion on success, one on failure; -EINVAL on
 * multishot and zero-copy records. */
static void scenario_skip_success(struct fixture *f)
{
    struct link l;
    struct vsr_io_sqe r;
    struct vsr_io_cqe cqe;
    unsigned char *ring_memory = page_alloc(4096);
    struct vsr_io_region memory;
    unsigned char bytes[16] = {0};

    r = rec(VSR_IO_SQE_NOP, UD(1));
    r.flags = VSR_IO_SQE_SKIP_SUCCESS;
    submit1(f, r);
    CHECK(!arrives(f, UD(1), 30, &cqe));
    r = io_record(VSR_IO_SQE_READ, -1, bytes, 8, 0, UD(2));
    r.flags = VSR_IO_SQE_SKIP_SUCCESS;
    CHECK(run(f, r) == -EBADF);

    open_link(f, &l);
    memory.base = ring_memory;
    memory.size = 4096;
    CHECK(f->ex.ops->buffer_ring(f->ex.ctx, 7, 8, 0, &memory) == 0);
    r = rec(VSR_IO_SQE_ACCEPT, UD(3));
    r.fd = l.listener;
    r.flags = VSR_IO_SQE_SKIP_SUCCESS;
    r.op_flags = VSR_IO_ACCEPT_MULTISHOT;
    CHECK(run(f, r) == -EINVAL);
    r = rec(VSR_IO_SQE_RECV, UD(4));
    r.fd = l.server;
    r.flags = VSR_IO_SQE_SKIP_SUCCESS | VSR_IO_SQE_BUFFER_SELECT;
    r.buffer_group = 7;
    r.op_flags = VSR_IO_RECV_MULTISHOT;
    CHECK(run(f, r) == -EINVAL);
    r = send_record(l.server, false, "x", 1, UD(5));
    r.flags = VSR_IO_SQE_SKIP_SUCCESS;
    r.op_flags = VSR_IO_SEND_ZERO_COPY;
    CHECK(run(f, r) == -EINVAL);
    CHECK(!arrives(f, UD(5), 30, &cqe)); /* No NOTIF either. */
    CHECK(f->ex.ops->buffer_ring(f->ex.ctx, 7, 0, 0, NULL) == 0);
    close_link(f, &l);
    free(ring_memory);
}

/* LINK: a chain runs in order after each success; a failure or a short
 * transfer cancels the rest; a chain never spans batches. */
static void scenario_link_chains(struct fixture *f)
{
    struct vsr_io_sqe chain[3];
    struct vsr_io_cqe cqe;
    struct link l;
    char buffer[64] = {0};
    int32_t fd = open_at(f, f->dir, "chain", O_CREAT | O_RDWR, UD(1));

    CHECK(fd >= 0);
    chain[0] = io_record(VSR_IO_SQE_WRITE, fd, "0123456789", 10, 0, UD(2));
    chain[0].flags = VSR_IO_SQE_LINK;
    chain[1] = rec(VSR_IO_SQE_FSYNC, UD(3));
    chain[1].flags = VSR_IO_SQE_LINK;
    chain[1].fd = fd;
    chain[2] = io_record(VSR_IO_SQE_READ, fd, buffer, 10, 0, UD(4));
    submit(f, chain, 3);
    cqe = take_next(f);
    CHECK(cqe.user_data == UD(2) && cqe.result == 10);
    cqe = take_next(f);
    CHECK(cqe.user_data == UD(3) && cqe.result == 0);
    cqe = take_next(f);
    CHECK(cqe.user_data == UD(4) && cqe.result == 10);
    CHECK(memcmp(buffer, "0123456789", 10) == 0);

    /* A short read (the file holds 10) fails its chain. */
    chain[0] = io_record(VSR_IO_SQE_READ, fd, buffer, 20, 0, UD(5));
    chain[0].flags = VSR_IO_SQE_LINK;
    chain[1] = rec(VSR_IO_SQE_NOP, UD(6));
    chain[1].flags = VSR_IO_SQE_LINK;
    chain[2] = rec(VSR_IO_SQE_NOP, UD(7));
    submit(f, chain, 3);
    CHECK(take(f, UD(5)).result == 10);
    CHECK(take(f, UD(6)).result == -ECANCELED);
    CHECK(take(f, UD(7)).result == -ECANCELED);

    /* A record the executor rejects fails its chain the same way. */
    chain[0] = io_record(VSR_IO_SQE_WRITE, fd, buffer, 4, 0, UD(8));
    chain[0].flags = VSR_IO_SQE_LINK | VSR_IO_SQE_FIXED_BUFFER;
    chain[1] = rec(VSR_IO_SQE_NOP, UD(9));
    submit(f, chain, 2);
    CHECK(take(f, UD(8)).result == -EFAULT);
    CHECK(take(f, UD(9)).result == -ECANCELED);

    /* A trailing LINK has no successor; the next batch is independent. */
    chain[0] = rec(VSR_IO_SQE_NOP, UD(10));
    chain[0].flags = VSR_IO_SQE_LINK;
    submit(f, chain, 1);
    CHECK(take(f, UD(10)).result == 0);
    submit1(f, rec(VSR_IO_SQE_NOP, UD(11)));
    CHECK(take(f, UD(11)).result == 0);
    CHECK(close_fd(f, fd, false) == 0);

    /* SEND: success means the whole length. RECV: a non-multishot receive
     * links only when its whole length arrives; the stream ending after 3
     * of 4 bytes gives 3 and cancels the successor. */
    open_link(f, &l);
    chain[0] = send_record(l.server, false, "link", 4, UD(12));
    chain[0].flags = VSR_IO_SQE_LINK;
    chain[1] = rec(VSR_IO_SQE_NOP, UD(13));
    submit(f, chain, 2);
    CHECK(take(f, UD(12)).result == 4 && take(f, UD(13)).result == 0);
    peer_receive_exact(f, l.peer, buffer, 4);
    CHECK(memcmp(buffer, "link", 4) == 0);
    CHECK(f->factory->peer_send(f, l.peer, "bye", 3) == 0);
    CHECK(f->factory->peer_shutdown(f, l.peer) == 0);
    chain[0] = recv_record(l.server, false, buffer, 4, UD(14));
    chain[0].flags = VSR_IO_SQE_LINK;
    chain[1] = rec(VSR_IO_SQE_NOP, UD(15));
    submit(f, chain, 2);
    CHECK(take(f, UD(14)).result == 3 && memcmp(buffer, "bye", 3) == 0);
    CHECK(take(f, UD(15)).result == -ECANCELED);
    close_link(f, &l);
}

/* submit_and_wait: want, min_wait_ns and deadline_ns; a deadline is a
 * normal return; a full submission queue is drained; reap never blocks. */
static void scenario_submit_and_wait(struct fixture *f)
{
    struct vsr_io_sqe records[100];
    uint64_t t0;
    uint64_t deadline;
    bool seen[100];

    /* want 0 with a past deadline never blocks; a deadline returns 0. */
    t0 = now(f);
    CHECK(f->ex.ops->submit_and_wait(f->ex.ctx, NULL, 0, 0, 0, 0) == 0);
    CHECK(f->ex.ops->submit_and_wait(f->ex.ctx, NULL, 0, 0, 0, t0) == 0);
    CHECK(f->ex.ops->reap(f->ex.ctx, f->stash, STASH) == 0);
    deadline = now(f) + 30 * MS;
    CHECK(f->factory->wait(f, 1, 0, deadline) == 0);
    CHECK(now(f) >= deadline);
    CHECK(f->ex.ops->reap(f->ex.ctx, f->stash, STASH) == 0);

    /* want counts completions. */
    records[0] = rec(VSR_IO_SQE_NOP, UD(1));
    records[1] = rec(VSR_IO_SQE_NOP, UD(2));
    CHECK(f->ex.ops->submit_and_wait(f->ex.ctx, records, 2, 0, 0, 0) == 0);
    CHECK(f->factory->wait(f, 2, 0, now(f) + 2 * SECOND) == 0);
    CHECK(f->ex.ops->reap(f->ex.ctx, f->stash, STASH) == 2);
    CHECK(f->stash[0].result == 0 && f->stash[1].result == 0);

    /* min_wait_ns ends the wait once one completion exists and the window
     * has elapsed, with and without a deadline. */
    records[0] = rec(VSR_IO_SQE_NOP, UD(3));
    submit(f, records, 1);
    t0 = now(f);
    CHECK(f->factory->wait(f, 4, 20 * MS, t0 + 2 * SECOND) == 0);
    CHECK(now(f) >= t0 + 20 * MS && now(f) < t0 + SECOND);
    CHECK(f->ex.ops->reap(f->ex.ctx, f->stash, 1) == 1);
    CHECK(f->stash[0].user_data == UD(3));
    records[0] = rec(VSR_IO_SQE_NOP, UD(4));
    submit(f, records, 1);
    t0 = now(f);
    CHECK(f->factory->wait(f, 4, 20 * MS, VSR_NO_DEADLINE) == 0);
    CHECK(now(f) >= t0 + 20 * MS && now(f) < t0 + SECOND);
    CHECK(f->ex.ops->reap(f->ex.ctx, f->stash, STASH) == 1);
    CHECK(f->stash[0].user_data == UD(4));

    /* More records than the submission queue holds, in one call. */
    for (uint32_t i = 0; i < 100; ++i) {
        records[i] = rec(VSR_IO_SQE_NOP, UD(1000 + i));
        seen[i] = false;
    }
    submit(f, records, 100);
    for (uint32_t i = 0; i < 100; ++i) {
        struct vsr_io_cqe cqe = take_next(f);

        CHECK(cqe.user_data >= UD(1000) && cqe.user_data < UD(1100));
        CHECK(!seen[cqe.user_data - UD(1000)] && cqe.result == 0);
        seen[cqe.user_data - UD(1000)] = true;
    }
    CHECK(f->stashed == 0);
    CHECK(f->ex.ops->reap(f->ex.ctx, f->stash, STASH) == 0);
}

/* now is monotonic and below VSR_NO_DEADLINE; random fills bytes. */
static void scenario_clock_and_random(struct fixture *f)
{
    unsigned char a[64];
    unsigned char b[64];
    unsigned char c[7];
    uint64_t previous = now(f);
    bool nonzero = false;

    for (uint32_t i = 0; i < 1000; ++i) {
        uint64_t t = now(f);

        CHECK(t >= previous && t < VSR_NO_DEADLINE);
        previous = t;
    }
    memset(a, 0, sizeof(a));
    memset(b, 0, sizeof(b));
    f->ex.ops->random(f->ex.ctx, a, sizeof(a));
    f->ex.ops->random(f->ex.ctx, b, sizeof(b));
    for (size_t i = 0; i < sizeof(a); ++i) {
        nonzero = nonzero || a[i] != 0;
    }
    CHECK(nonzero && memcmp(a, b, sizeof(a)) != 0);
    memset(c, 0, sizeof(c));
    f->ex.ops->random(f->ex.ctx, c, sizeof(c));
    f->ex.ops->random(f->ex.ctx, c, 0);
}

/* wake: from another thread (ring) or the same thread (sim); a wake before
 * the wait returns it at once; wakes are idempotent and consumed. */
static void scenario_wake(struct fixture *f)
{
    static const uint32_t wants[] = {1, 8, 8, 1};
    static const uint64_t windows[] = {0, 0, 100 * MS, 0};
    uint64_t t0;
    uint64_t deadline;

    for (size_t i = 0; i < 4; ++i) {
        f->factory->wake_elsewhere(f);
        t0 = now(f);
        CHECK(f->factory->wait(f, wants[i], windows[i], VSR_NO_DEADLINE) == 0);
        CHECK(now(f) - t0 < 3 * SECOND);
        /* Wake completions never reach reap. */
        CHECK(f->ex.ops->reap(f->ex.ctx, f->stash, STASH) == 0);
    }
    f->ex.ops->wake(f->ex.ctx);
    f->ex.ops->wake(f->ex.ctx);
    t0 = now(f);
    CHECK(f->factory->wait(f, 1, 0, VSR_NO_DEADLINE) == 0);
    CHECK(now(f) - t0 < SECOND);
    CHECK(f->ex.ops->reap(f->ex.ctx, f->stash, STASH) == 0);
    /* Consumed: the following wait runs until its deadline. */
    deadline = now(f) + 30 * MS;
    CHECK(f->factory->wait(f, 1, 0, deadline) == 0);
    CHECK(now(f) >= deadline);
    CHECK(f->ex.ops->reap(f->ex.ctx, f->stash, STASH) == 0);
}

/* Registration order: update_* before registration is -EINVAL (decision
 * 58); each table registers once; bad slots and indexes are refused. */
static void scenario_registration(struct fixture *f)
{
    struct vsr_io_region region;
    unsigned char *page = page_alloc(4096);
    int rc;

    region.base = page;
    region.size = 4096;
    CHECK(f->ex.ops->update_file(f->ex.ctx, 0, -1) == -EINVAL);
    CHECK(f->ex.ops->update_buffer(f->ex.ctx, 0, NULL) == -EINVAL);
    CHECK(f->ex.ops->update_buffer(f->ex.ctx, 0, &region) == -EINVAL);
    CHECK(f->ex.ops->register_files(f->ex.ctx, 4) == 0);
    CHECK(f->ex.ops->register_files(f->ex.ctx, 4) < 0);
    CHECK(f->ex.ops->register_buffers(f->ex.ctx, 2) == 0);
    CHECK(f->ex.ops->register_buffers(f->ex.ctx, 2) < 0);
    rc = f->ex.ops->update_file(f->ex.ctx, 4, -1);
    CHECK(rc == -EBADF || rc == -EINVAL);
    CHECK(f->ex.ops->update_file(f->ex.ctx, 3, -1) == 0);
    CHECK(f->ex.ops->update_buffer(f->ex.ctx, 2, &region) < 0);
    CHECK(f->ex.ops->update_buffer(f->ex.ctx, 1, &region) == 0);
    CHECK(f->ex.ops->update_buffer(f->ex.ctx, 1, NULL) == 0);
    free(page);
}

/* ------------------------------------------------------------------------
 * Scenarios: files
 * --------------------------------------------------------------------- */

/* READ, WRITE, READV, WRITEV, FSYNC, FALLOCATE, STATX, CLOSE. */
static void scenario_file_io(struct fixture *f)
{
    struct vsr_io_sqe r;
    struct vsr_io_vec vecs[2];
    struct statx stx;
    char buffer[64];
    unsigned char zeros[64];
    int32_t fd = open_at(f, f->dir, "data", O_CREAT | O_RDWR, UD(1));

    CHECK(fd >= 0);
    CHECK(io(f, VSR_IO_SQE_WRITE, fd, "hello world", 11, 0, UD(2)) == 11);
    memset(buffer, 0, sizeof(buffer));
    CHECK(io(f, VSR_IO_SQE_READ, fd, buffer, sizeof(buffer), 0, UD(3)) == 11);
    CHECK(memcmp(buffer, "hello world", 11) == 0);
    /* Past-end reads are short or 0. */
    memset(buffer, 0, sizeof(buffer));
    CHECK(io(f, VSR_IO_SQE_READ, fd, buffer, 8, 8, UD(4)) == 3);
    CHECK(memcmp(buffer, "rld", 3) == 0);
    CHECK(io(f, VSR_IO_SQE_READ, fd, buffer, 8, 100, UD(5)) == 0);

    vecs[0].base = readable("abc");
    vecs[0].length = 3;
    vecs[1].base = readable("defgh");
    vecs[1].length = 5;
    CHECK(io(f, VSR_IO_SQE_WRITEV, fd, vecs, 2, 20, UD(6)) == 8);
    memset(buffer, 0, sizeof(buffer));
    vecs[0].base = buffer;
    vecs[0].length = 4;
    vecs[1].base = buffer + 10;
    vecs[1].length = 4;
    CHECK(io(f, VSR_IO_SQE_READV, fd, vecs, 2, 20, UD(7)) == 8);
    CHECK(memcmp(buffer, "abcd", 4) == 0 &&
          memcmp(buffer + 10, "efgh", 4) == 0);
    /* The file holds 28 bytes: a vectored read at 26 is short. */
    CHECK(io(f, VSR_IO_SQE_READV, fd, vecs, 2, 26, UD(8)) == 2);
    CHECK(memcmp(buffer, "gh", 2) == 0);

    r = rec(VSR_IO_SQE_FSYNC, UD(9));
    r.fd = fd;
    r.op_flags = VSR_IO_FSYNC_DATASYNC;
    CHECK(run(f, r) == 0);
    r.user_data = UD(10);
    r.op_flags = 0;
    CHECK(run(f, r) == 0);

    r = rec(VSR_IO_SQE_FALLOCATE, UD(11));
    r.fd = fd;
    r.offset = 0;
    r.length = 8192;
    CHECK(run(f, r) == 0);
    memset(&stx, 0, sizeof(stx));
    r = rec(VSR_IO_SQE_STATX, UD(12));
    r.fd = f->dir;
    r.addr = "data";
    r.addr2 = &stx;
    r.length = STATX_SIZE | STATX_MODE;
    CHECK(run(f, r) == 0);
    CHECK(stx.stx_size == 8192 && S_ISREG(stx.stx_mode));
    /* Preallocation reads as zeros. */
    memset(buffer, 'x', sizeof(buffer));
    memset(zeros, 0, sizeof(zeros));
    CHECK(io(f, VSR_IO_SQE_READ, fd, buffer, 64, 4096, UD(13)) == 64);
    CHECK(memcmp(buffer, zeros, 64) == 0);

    CHECK(close_fd(f, fd, false) == 0);
    CHECK(io(f, VSR_IO_SQE_READ, fd, buffer, 8, 0, UD(14)) == -EBADF);
}

/* Opens an O_DIRECT file holding one aligned block, or skips. */
static bool all_bytes(const unsigned char *bytes, size_t size, unsigned char c)
{
    for (size_t i = 0; i < size; ++i) {
        if (bytes[i] != c) {
            return false;
        }
    }
    return true;
}

static int32_t open_direct(struct fixture *f, unsigned char **page)
{
    int32_t fd =
        open_at(f, f->dir, "odirect", O_CREAT | O_RDWR | O_DIRECT, UD(1));

    if (fd == -EINVAL) {
        skip("O_DIRECT unsupported by the directory's filesystem");
    }
    CHECK(fd >= 0);
    *page = page_alloc((size_t)3 * ALIGNED);
    memset(*page, 'd', ALIGNED);
    CHECK(io(f, VSR_IO_SQE_WRITE, fd, *page, ALIGNED, 0, UD(2)) == ALIGNED);
    CHECK(io(f, VSR_IO_SQE_READ, fd, *page + ALIGNED, ALIGNED, 0, UD(3)) ==
          ALIGNED);
    CHECK(memcmp(*page, *page + ALIGNED, ALIGNED) == 0);
    if (!f->factory->direct_alignment(f, fd)) {
        skip("the filesystem serves misaligned direct I/O");
    }
    return fd;
}

/* O_DIRECT: offset and length must be block-aligned, else -EINVAL. */
static void scenario_odirect_alignment(struct fixture *f)
{
    struct vsr_io_vec vecs[2];
    unsigned char *page;
    int32_t fd = open_direct(f, &page);

    CHECK(io(f, VSR_IO_SQE_READ, fd, page, 512, 100, UD(5)) == -EINVAL);
    CHECK(io(f, VSR_IO_SQE_READ, fd, page, 100, 0, UD(6)) == -EINVAL);
    CHECK(io(f, VSR_IO_SQE_WRITE, fd, page, 100, 0, UD(7)) == -EINVAL);
    vecs[0].base = page + ALIGNED;
    vecs[0].length = 100;
    CHECK(io(f, VSR_IO_SQE_READV, fd, vecs, 1, 0, UD(8)) == -EINVAL);
    vecs[0].length = ALIGNED;
    vecs[1].base = page + (size_t)2 * ALIGNED;
    vecs[1].length = ALIGNED;
    CHECK(io(f, VSR_IO_SQE_READV, fd, vecs, 2, 100, UD(9)) == -EINVAL);
    CHECK(io(f, VSR_IO_SQE_READV, fd, vecs, 2, 0, UD(10)) == ALIGNED);
    CHECK(close_fd(f, fd, false) == 0);
    free(page);
}

/* O_DIRECT: only offset and length need block alignment; a misaligned
 * address is served (decision 65). */
static void scenario_odirect_address(struct fixture *f)
{
    struct vsr_io_vec vecs[1];
    unsigned char *page;
    int32_t fd = open_direct(f, &page);

    /* Only offset and length need block alignment; a misaligned address is
     * served, bounced by the kernel where the device needs it (decision
     * 65). The file holds ALIGNED bytes of 'd' from open_direct. */
    memset(page + 1, 0, 512);
    CHECK(io(f, VSR_IO_SQE_READ, fd, page + 1, 512, 0, UD(4)) == 512);
    CHECK(all_bytes(page + 1, 512, 'd'));
    CHECK(io(f, VSR_IO_SQE_WRITE, fd, page + 1, 512, 0, UD(5)) == 512);
    vecs[0].base = page + ALIGNED + 3;
    vecs[0].length = 512;
    memset(page + ALIGNED + 3, 0, 512);
    CHECK(io(f, VSR_IO_SQE_READV, fd, vecs, 1, 0, UD(6)) == 512);
    CHECK(all_bytes(page + ALIGNED + 3, 512, 'd'));
    CHECK(close_fd(f, fd, false) == 0);
    free(page);
}

/* OPENAT, MKDIRAT, RENAMEAT, UNLINKAT, STATX over a directory. */
static void scenario_directory_ops(struct fixture *f)
{
    struct vsr_io_sqe r;
    struct statx stx;
    char buffer[32];
    int32_t fd;
    int32_t kept;
    int32_t sub;

    r = rec(VSR_IO_SQE_MKDIRAT, UD(1));
    r.fd = f->dir;
    r.addr = "sub";
    r.length = 0755;
    CHECK(run(f, r) == 0);
    r.user_data = UD(2);
    CHECK(run(f, r) == -EEXIST);
    CHECK(open_at(f, f->dir, "missing", O_RDONLY, UD(3)) == -ENOENT);
    fd = open_at(f, f->dir, "a", O_CREAT | O_RDWR, UD(4));
    CHECK(fd >= 0 && io(f, VSR_IO_SQE_WRITE, fd, "AAAA", 4, 0, UD(5)) == 4);
    CHECK(close_fd(f, fd, false) == 0);
    CHECK(open_at(f, f->dir, "a", O_CREAT | O_EXCL | O_RDWR, UD(6)) == -EEXIST);
    kept = open_at(f, f->dir, "b", O_CREAT | O_RDWR, UD(7));
    CHECK(kept >= 0);
    CHECK(io(f, VSR_IO_SQE_WRITE, kept, "BBBBBB", 6, 0, UD(8)) == 6);

    /* Atomic replace: the old descriptor keeps the replaced bytes, the
     * name resolves to the new file, the old name is gone. */
    r = rec(VSR_IO_SQE_RENAMEAT, UD(9));
    r.fd = f->dir;
    r.addr = "a";
    r.addr2 = "b";
    CHECK(run(f, r) == 0);
    memset(buffer, 0, sizeof(buffer));
    CHECK(io(f, VSR_IO_SQE_READ, kept, buffer, 16, 0, UD(10)) == 6);
    CHECK(memcmp(buffer, "BBBBBB", 6) == 0);
    CHECK(close_fd(f, kept, false) == 0);
    fd = open_at(f, f->dir, "b", O_RDONLY, UD(11));
    memset(buffer, 0, sizeof(buffer));
    CHECK(fd >= 0 && io(f, VSR_IO_SQE_READ, fd, buffer, 16, 0, UD(12)) == 4);
    CHECK(memcmp(buffer, "AAAA", 4) == 0);
    CHECK(open_at(f, f->dir, "a", O_RDONLY, UD(13)) == -ENOENT);
    r.user_data = UD(14);
    r.addr = "gone";
    r.addr2 = "c";
    CHECK(run(f, r) == -ENOENT);

    /* Unlink: the open descriptor keeps the data. */
    r = rec(VSR_IO_SQE_UNLINKAT, UD(15));
    r.fd = f->dir;
    r.addr = "b";
    CHECK(run(f, r) == 0);
    memset(buffer, 0, sizeof(buffer));
    CHECK(io(f, VSR_IO_SQE_READ, fd, buffer, 16, 0, UD(16)) == 4);
    CHECK(memcmp(buffer, "AAAA", 4) == 0);
    CHECK(close_fd(f, fd, false) == 0);
    r.user_data = UD(17);
    CHECK(run(f, r) == -ENOENT);
    memset(&stx, 0, sizeof(stx));
    r = rec(VSR_IO_SQE_STATX, UD(18));
    r.fd = f->dir;
    r.addr = "b";
    r.addr2 = &stx;
    r.length = STATX_SIZE;
    CHECK(run(f, r) == -ENOENT);

    /* Paths relative to another directory descriptor. */
    sub = open_at(f, f->dir, "sub", O_RDONLY | O_DIRECTORY, UD(19));
    CHECK(sub >= 0);
    fd = open_at(f, sub, "inner", O_CREAT | O_RDWR, UD(20));
    CHECK(fd >= 0 && io(f, VSR_IO_SQE_WRITE, fd, "in", 2, 0, UD(21)) == 2);
    CHECK(close_fd(f, fd, false) == 0);
    memset(&stx, 0, sizeof(stx));
    r = rec(VSR_IO_SQE_STATX, UD(22));
    r.fd = f->dir;
    r.addr = "sub/inner";
    r.addr2 = &stx;
    r.length = STATX_SIZE | STATX_MODE;
    CHECK(run(f, r) == 0);
    CHECK(stx.stx_size == 2 && S_ISREG(stx.stx_mode));
    r = rec(VSR_IO_SQE_UNLINKAT, UD(23));
    r.fd = sub;
    r.addr = "inner";
    CHECK(run(f, r) == 0);
    CHECK(close_fd(f, sub, false) == 0);
}

/* FALLOCATE past the disk's capacity: -ENOSPC (a bounded disk only). */
static void scenario_fallocate_enospc(struct fixture *f)
{
    struct vsr_io_sqe r;
    int32_t fd;

    if (f->disk_capacity == 0) {
        skip("unbounded disk");
    }
    fd = open_at(f, f->dir, "huge", O_CREAT | O_RDWR, UD(1));
    CHECK(fd >= 0);
    r = rec(VSR_IO_SQE_FALLOCATE, UD(2));
    r.fd = fd;
    r.offset = 0;
    r.length = 4096;
    CHECK(run(f, r) == 0);
    r.user_data = UD(3);
    r.offset = f->disk_capacity;
    r.length = (uint32_t)(f->disk_capacity < UINT32_MAX ? f->disk_capacity
                                                        : UINT32_MAX);
    CHECK(run(f, r) == -ENOSPC);
    CHECK(close_fd(f, fd, false) == 0);
}

/* FIXED_BUFFER: inside a registered region, outside it, an unregistered
 * index, a cleared region; update_buffer of a region in use is -EBUSY. */
static void scenario_fixed_buffers(struct fixture *f)
{
    struct vsr_io_sqe r;
    struct vsr_io_region region;
    struct vsr_io_vec vecs[2];
    struct link l;
    unsigned char *page = page_alloc(8192);
    char buffer[64];
    int rc;
    int32_t fd = open_at(f, f->dir, "fixed", O_CREAT | O_RDWR, UD(1));

    CHECK(fd >= 0);
    CHECK(f->ex.ops->register_buffers(f->ex.ctx, 2) == 0);
    region.base = page;
    region.size = 8192;
    CHECK(f->ex.ops->update_buffer(f->ex.ctx, 0, &region) == 0);
    memcpy(page, "fixed-bytes", 11);
    CHECK(fixed_io(f, VSR_IO_SQE_WRITE, fd, page, 11, 100, 0, UD(2)) == 11);
    CHECK(fixed_io(f, VSR_IO_SQE_READ, fd, page + 4096, 11, 100, 0, UD(3)) ==
          11);
    CHECK(memcmp(page + 4096, "fixed-bytes", 11) == 0);
    vecs[0].base = page + 100;
    vecs[0].length = 5;
    vecs[1].base = page + 200;
    vecs[1].length = 6;
    r = io_record(VSR_IO_SQE_READV, fd, vecs, 2, 100, UD(4));
    r.flags = VSR_IO_SQE_FIXED_BUFFER;
    CHECK(run(f, r) == 11);
    CHECK(memcmp(page + 100, "fixed", 5) == 0 &&
          memcmp(page + 200, "-bytes", 6) == 0);
    r.opcode = VSR_IO_SQE_WRITEV;
    r.user_data = UD(5);
    r.offset = 300;
    CHECK(run(f, r) == 11);
    memset(buffer, 0, sizeof(buffer));
    CHECK(io(f, VSR_IO_SQE_READ, fd, buffer, 11, 300, UD(6)) == 11);
    CHECK(memcmp(buffer, "fixed-bytes", 11) == 0);

    /* Outside the region, straddling its end, an empty index, an index
     * beyond the table, one vector outside. */
    CHECK(fixed_io(f, VSR_IO_SQE_READ, fd, buffer, 8, 0, 0, UD(7)) == -EFAULT);
    CHECK(fixed_io(f, VSR_IO_SQE_READ, fd, page + 8190, 4, 0, 0, UD(8)) ==
          -EFAULT);
    CHECK(fixed_io(f, VSR_IO_SQE_READ, fd, page, 8, 0, 1, UD(9)) == -EFAULT);
    CHECK(fixed_io(f, VSR_IO_SQE_READ, fd, page, 8, 0, 3, UD(10)) == -EFAULT);
    vecs[1].base = buffer;
    r.opcode = VSR_IO_SQE_READV;
    r.user_data = UD(11);
    CHECK(run(f, r) == -EFAULT);
    CHECK(fixed_io(f, VSR_IO_SQE_WRITE, fd, buffer, 8, 0, 0, UD(12)) ==
          -EFAULT);
    /* Cleared, then installed again. */
    CHECK(f->ex.ops->update_buffer(f->ex.ctx, 0, NULL) == 0);
    CHECK(fixed_io(f, VSR_IO_SQE_READ, fd, page, 8, 0, 0, UD(13)) == -EFAULT);
    CHECK(f->ex.ops->update_buffer(f->ex.ctx, 0, &region) == 0);
    CHECK(fixed_io(f, VSR_IO_SQE_READ, fd, page, 8, 0, 0, UD(14)) == 8);
    CHECK(close_fd(f, fd, false) == 0);

    /* Replacing a region a pending record uses is a caller error: the sim
     * reports -EBUSY, the ring cannot tell and returns 0 while the kernel
     * keeps the old registration alive (decision 65). */
    open_link(f, &l);
    r = recv_record(l.server, false, page, 64, UD(15));
    r.flags = VSR_IO_SQE_FIXED_BUFFER;
    r.buffer_index = 0;
    submit1(f, r);
    rc = f->ex.ops->update_buffer(f->ex.ctx, 0, NULL);
    CHECK(rc == -EBUSY || rc == 0);
    region.base = page + 4096;
    region.size = 4096;
    rc = f->ex.ops->update_buffer(f->ex.ctx, 0, &region);
    CHECK(rc == -EBUSY || rc == 0);
    CHECK(cancel_user_data(f, UD(15)) == 0);
    CHECK(take(f, UD(15)).result == -ECANCELED);
    CHECK(f->ex.ops->update_buffer(f->ex.ctx, 0, NULL) == 0);
    close_link(f, &l);
    free(page);
}

/* FIXED_FILE and DIRECT: slot allocation, named slots, CLOSE of a slot,
 * -EBADF on an empty slot, -ENFILE on exhaustion, update_file. */
static void scenario_fixed_files(struct fixture *f)
{
    struct vsr_io_sqe r;
    char buffer[64];
    int32_t fd;
    int32_t slot;
    int32_t slots[8];
    uint32_t opened = 0;
    int rc;

    CHECK(f->ex.ops->register_files(f->ex.ctx, 8) == 0);
    r = rec(VSR_IO_SQE_OPENAT, UD(1));
    r.flags = VSR_IO_SQE_DIRECT;
    r.fd = f->dir;
    r.addr = "direct";
    r.op_flags = O_CREAT | O_RDWR;
    r.length = 0644;
    r.fd2 = VSR_IO_SLOT_ALLOC;
    slot = run(f, r);
    CHECK(slot >= 0 && slot < 8);
    r.user_data = UD(2);
    r.fd2 = 5;
    r.op_flags = O_RDWR;
    CHECK(slot != 5 && run(f, r) == 5);
    r = io_record(VSR_IO_SQE_WRITE, slot, "direct!", 7, 0, UD(3));
    r.flags = VSR_IO_SQE_FIXED_FILE;
    CHECK(run(f, r) == 7);
    memset(buffer, 0, sizeof(buffer));
    r = io_record(VSR_IO_SQE_READ, 5, buffer, 16, 0, UD(4));
    r.flags = VSR_IO_SQE_FIXED_FILE;
    CHECK(run(f, r) == 7 && memcmp(buffer, "direct!", 7) == 0);
    CHECK(close_fd(f, 5, true) == 0);
    r.user_data = UD(5);
    CHECK(run(f, r) == -EBADF);
    CHECK(close_fd(f, 5, true) == -EBADF);
    /* Every free slot fills, then -ENFILE. */
    slots[opened++] = slot;
    for (;;) {
        r = rec(VSR_IO_SQE_OPENAT, UD(10 + opened));
        r.flags = VSR_IO_SQE_DIRECT;
        r.fd = f->dir;
        r.addr = "direct";
        r.op_flags = O_RDONLY;
        r.fd2 = VSR_IO_SLOT_ALLOC;
        slot = run(f, r);
        if (slot < 0) {
            break;
        }
        CHECK(opened < 8);
        slots[opened++] = slot;
    }
    CHECK(slot == -ENFILE && opened == 8);
    for (uint32_t i = 0; i < opened; ++i) {
        CHECK(close_fd(f, slots[i], true) == 0);
    }

    /* update_file installs a descriptor the executor then owns. */
    fd = open_at(f, f->dir, "direct", O_RDONLY, UD(20));
    CHECK(fd >= 0);
    rc = f->ex.ops->update_file(f->ex.ctx, 8, fd);
    CHECK(rc == -EBADF || rc == -EINVAL);
    CHECK(f->ex.ops->update_file(f->ex.ctx, 2, fd) == 0);
    memset(buffer, 0, sizeof(buffer));
    r = io_record(VSR_IO_SQE_READ, 2, buffer, 16, 0, UD(21));
    r.flags = VSR_IO_SQE_FIXED_FILE;
    CHECK(run(f, r) == 7 && memcmp(buffer, "direct!", 7) == 0);
    CHECK(f->ex.ops->update_file(f->ex.ctx, 2, -1) == 0);
    r.user_data = UD(22);
    CHECK(run(f, r) == -EBADF);
}

/* FILES_UPDATE (decision E7): a record installs descriptors into slots,
 * each slot taking its own reference while the descriptor stays open and
 * the caller's; -1 empties a slot; the count updated before a bad
 * descriptor; -EBADF, -EINVAL (beyond the table, a fixed file), -EFAULT. */
static void scenario_files_update(struct fixture *f)
{
    struct vsr_io_sqe r;
    char buffer[16];
    int32_t fds[2];
    int32_t fd;
    int32_t other;

    CHECK(f->ex.ops->register_files(f->ex.ctx, 8) == 0);
    fd = open_at(f, f->dir, "updated", O_CREAT | O_RDWR, UD(1));
    CHECK(fd >= 0);
    CHECK(io(f, VSR_IO_SQE_WRITE, fd, "updated", 7, 0, UD(2)) == 7);
    fds[0] = fd;
    r = rec(VSR_IO_SQE_FILES_UPDATE, UD(3));
    r.offset = 3;
    r.addr = fds;
    r.length = 1;
    CHECK(run(f, r) == 1);
    memset(buffer, 0, sizeof(buffer));
    r = io_record(VSR_IO_SQE_READ, 3, buffer, 16, 0, UD(4));
    r.flags = VSR_IO_SQE_FIXED_FILE;
    CHECK(run(f, r) == 7 && memcmp(buffer, "updated", 7) == 0);
    /* The descriptor is still open; closing it leaves the slot's file. */
    memset(buffer, 0, sizeof(buffer));
    CHECK(io(f, VSR_IO_SQE_READ, fd, buffer, 16, 0, UD(5)) == 7);
    CHECK(close_fd(f, fd, false) == 0);
    r.user_data = UD(6);
    CHECK(run(f, r) == 7);
    /* Two slots with a bad descriptor after a good one: the count before
     * it, the second slot untouched. */
    other = open_at(f, f->dir, "updated", O_RDONLY, UD(7));
    CHECK(other >= 0);
    fds[0] = other;
    fds[1] = 1000000;
    r = rec(VSR_IO_SQE_FILES_UPDATE, UD(8));
    r.offset = 4;
    r.addr = fds;
    r.length = 2;
    CHECK(run(f, r) == 1);
    r = io_record(VSR_IO_SQE_READ, 5, buffer, 16, 0, UD(9));
    r.flags = VSR_IO_SQE_FIXED_FILE;
    CHECK(run(f, r) == -EBADF);
    r = io_record(VSR_IO_SQE_READ, 4, buffer, 16, 0, UD(10));
    r.flags = VSR_IO_SQE_FIXED_FILE;
    CHECK(run(f, r) == 7);
    CHECK(close_fd(f, other, false) == 0);
    /* -1 empties both. */
    fds[0] = -1;
    fds[1] = -1;
    r = rec(VSR_IO_SQE_FILES_UPDATE, UD(11));
    r.offset = 3;
    r.addr = fds;
    r.length = 2;
    CHECK(run(f, r) == 2);
    r = io_record(VSR_IO_SQE_READ, 3, buffer, 16, 0, UD(12));
    r.flags = VSR_IO_SQE_FIXED_FILE;
    CHECK(run(f, r) == -EBADF);
    /* Refusals. */
    fds[0] = 1000000;
    r = rec(VSR_IO_SQE_FILES_UPDATE, UD(13));
    r.offset = 3;
    r.addr = fds;
    r.length = 1;
    CHECK(run(f, r) == -EBADF);
    fds[0] = -1;
    r.user_data = UD(14);
    r.offset = 7;
    r.length = 2;
    CHECK(run(f, r) == -EINVAL);
    r.user_data = UD(15);
    r.offset = 8;
    r.length = 1;
    CHECK(run(f, r) == -EINVAL);
    r.user_data = UD(16);
    r.offset = 0;
    r.flags = VSR_IO_SQE_FIXED_FILE;
    CHECK(run(f, r) == -EINVAL);
    r.user_data = UD(17);
    r.flags = 0;
    r.addr = NULL;
    CHECK(run(f, r) == -EFAULT);
}

/* ------------------------------------------------------------------------
 * Scenarios: sockets
 * --------------------------------------------------------------------- */

/* SOCKET, BIND (-EADDRINUSE), LISTEN, SETSOCKOPT/GETSOCKOPT. */
static void scenario_socket_setup(struct fixture *f)
{
    struct vsr_io_address dial;
    struct vsr_io_sqe r;
    struct vsr_io_cqe cqe;
    int value;
    int32_t listener;
    int32_t other;
    int32_t server;
    int peer;

    r = rec(VSR_IO_SQE_SOCKET, UD(1));
    r.length = 200; /* No such family. */
    r.op_flags = SOCK_STREAM;
    CHECK(run(f, r) == -EAFNOSUPPORT);
    r = rec(VSR_IO_SQE_SOCKET, UD(2));
    r.length = AF_INET;
    r.op_flags = SOCK_STREAM;
    r.offset = IPPROTO_TCP;
    other = run(f, r);
    CHECK(other >= 0);
    listener = open_listener(f, &dial);
    r = rec(VSR_IO_SQE_BIND, UD(3));
    r.fd = other;
    r.addr = &dial.sockaddr;
    r.length = dial.length;
    CHECK(run(f, r) == -EADDRINUSE);
    CHECK(close_fd(f, other, false) == 0);

    r = rec(VSR_IO_SQE_ACCEPT, UD(4));
    r.fd = listener;
    submit1(f, r);
    peer = f->factory->peer_connect(f, &dial);
    CHECK(peer >= 0);
    cqe = take(f, UD(4));
    CHECK(cqe.result >= 0 && cqe.flags == 0);
    server = cqe.result;
    /* GETSOCKOPT serves SOL_SOCKET only; other levels complete
     * -EOPNOTSUPP on both executors, while SETSOCKOPT serves every level
     * (decision 65). */
    for (uint32_t option = 0; option < 2; ++option) {
        uint32_t level = option == 0 ? SOL_SOCKET : (uint32_t)IPPROTO_TCP;
        uint32_t name = option == 0 ? SO_KEEPALIVE : (uint32_t)TCP_NODELAY;
        int32_t got = option == 0 ? (int32_t)sizeof(value) : -EOPNOTSUPP;

        value = 7;
        r = rec(VSR_IO_SQE_GETSOCKOPT, UD(10 + option));
        r.fd = server;
        r.op_flags = level << 16 | name;
        r.addr = &value;
        r.length = sizeof(value);
        CHECK(run(f, r) == got && value == (option == 0 ? 0 : 7));
        value = 1;
        r = rec(VSR_IO_SQE_SETSOCKOPT, UD(20 + option));
        r.fd = server;
        r.op_flags = level << 16 | name;
        r.addr = &value;
        r.length = sizeof(value);
        CHECK(run(f, r) == 0);
        value = 7;
        r = rec(VSR_IO_SQE_GETSOCKOPT, UD(30 + option));
        r.fd = server;
        r.op_flags = level << 16 | name;
        r.addr = &value;
        r.length = sizeof(value);
        CHECK(run(f, r) == got && value == (option == 0 ? 1 : 7));
    }
    CHECK(close_fd(f, server, false) == 0);
    CHECK(close_fd(f, listener, false) == 0);
    CHECK(f->factory->peer_close(f, peer, false) == 0);
}

/* SOCKET with DIRECT into a named slot and an allocated one; CONNECT,
 * SEND and CLOSE through FIXED_FILE. */
static void scenario_socket_direct(struct fixture *f)
{
    struct vsr_io_address address;
    struct vsr_io_sqe r;
    char buffer[16];
    int listener;
    int peer;
    int32_t slot;

    CHECK(f->ex.ops->register_files(f->ex.ctx, 4) == 0);
    r = rec(VSR_IO_SQE_SOCKET, UD(1));
    r.flags = VSR_IO_SQE_DIRECT;
    r.fd2 = 3;
    r.length = AF_INET;
    r.op_flags = SOCK_STREAM;
    CHECK(run(f, r) == 3);
    r.user_data = UD(2);
    r.fd2 = VSR_IO_SLOT_ALLOC;
    slot = run(f, r);
    CHECK(slot >= 0 && slot < 4 && slot != 3);
    CHECK(close_fd(f, slot, true) == 0);

    listener = f->factory->peer_listen(f, false, &address);
    CHECK(listener >= 0);
    CHECK(connect_to(f, 3, true, &address, UD(3)) == 0);
    peer = f->factory->peer_accept(f, listener);
    CHECK(peer >= 0);
    CHECK(run(f, send_record(3, true, "direct", 6, UD(4))) == 6);
    peer_receive_exact(f, peer, buffer, 6);
    CHECK(memcmp(buffer, "direct", 6) == 0);
    CHECK(close_fd(f, 3, true) == 0);
    /* The slot's socket is closed: the peer sees the end of the stream. */
    CHECK(f->factory->peer_recv(f, peer, buffer, sizeof(buffer)) == 0);
    CHECK(f->factory->peer_close(f, peer, false) == 0);
    CHECK(f->factory->peer_close(f, listener, false) == 0);
}

/* CONNECT: -ECONNREFUSED without a listener; for a foreign family, an
 * AF_INET socket checks the length first (-EINVAL below a sockaddr_in, then
 * -EAFNOSUPPORT) and an AF_UNIX socket answers -EINVAL. */
static void scenario_connect_refused(struct fixture *f)
{
    struct vsr_io_address address;
    struct sockaddr_un un;
    struct sockaddr_in in;
    struct vsr_io_sqe r;
    int32_t fd = make_socket(f, AF_INET);
    int32_t local;

    CHECK(fd >= 0);
    f->factory->unused_address(f, &address);
    CHECK(connect_to(f, fd, false, &address, UD(1)) == -ECONNREFUSED);
    memset(&un, 0, sizeof(un));
    un.sun_family = AF_UNIX;
    memcpy(un.sun_path + 1, "nowhere", 7);
    r = rec(VSR_IO_SQE_CONNECT, UD(2));
    r.fd = fd;
    r.addr = &un;
    r.length = (uint32_t)(offsetof(struct sockaddr_un, sun_path) + 8);
    CHECK(run(f, r) == -EINVAL);
    r = rec(VSR_IO_SQE_CONNECT, UD(3));
    r.fd = fd;
    r.addr = &un;
    r.length = (uint32_t)sizeof(un);
    CHECK(run(f, r) == -EAFNOSUPPORT);
    CHECK(close_fd(f, fd, false) == 0);
    local = make_socket(f, AF_UNIX);
    CHECK(local >= 0);
    memset(&in, 0, sizeof(in));
    in.sin_family = AF_INET;
    in.sin_port = htons(1);
    in.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    r = rec(VSR_IO_SQE_CONNECT, UD(4));
    r.fd = local;
    r.addr = &in;
    r.length = (uint32_t)sizeof(in);
    CHECK(run(f, r) == -EINVAL);
    CHECK(close_fd(f, local, false) == 0);
}

/* CONNECT across a partition: -EHOSTUNREACH after connect_timeout_ns. */
static void scenario_connect_unreachable(struct fixture *f)
{
    struct vsr_io_address address;
    int listener;
    int32_t fd;

    if (f->factory->partition(f, 1) == -ENOTSUP) {
        skip("no partition control");
    }
    listener = f->factory->peer_listen(f, false, &address);
    CHECK(listener >= 0);
    fd = make_socket(f, AF_INET);
    CHECK(fd >= 0);
    CHECK(connect_to(f, fd, false, &address, UD(1)) == -EHOSTUNREACH);
    CHECK(f->factory->partition(f, 0) == 0);
    CHECK(connect_to(f, fd, false, &address, UD(2)) == 0);
    CHECK(f->factory->peer_accept(f, listener) >= 0);
    CHECK(close_fd(f, fd, false) == 0);
}

/* ACCEPT with MULTISHOT and DIRECT: MORE per connection, slots allocated,
 * a receive and a cancel by slot, termination by CANCEL; a plain accept. */
static void scenario_accept_multishot(struct fixture *f)
{
    struct vsr_io_address dial;
    struct vsr_io_sqe r;
    struct vsr_io_cqe cqe;
    char buffer[16];
    int32_t listener = open_listener(f, &dial);
    int32_t first;
    int32_t second;
    int peers[3];

    CHECK(f->ex.ops->register_files(f->ex.ctx, 4) == 0);
    r = rec(VSR_IO_SQE_ACCEPT, UD(1));
    r.fd = listener;
    r.flags = VSR_IO_SQE_DIRECT;
    r.fd2 = VSR_IO_SLOT_ALLOC;
    r.op_flags = VSR_IO_ACCEPT_MULTISHOT;
    submit1(f, r);
    peers[0] = f->factory->peer_connect(f, &dial);
    CHECK(peers[0] >= 0);
    cqe = take(f, UD(1));
    first = cqe.result;
    CHECK(first >= 0 && first < 4 && (cqe.flags & VSR_IO_CQE_MORE));
    peers[1] = f->factory->peer_connect(f, &dial);
    CHECK(peers[1] >= 0);
    cqe = take(f, UD(1));
    second = cqe.result;
    CHECK(second >= 0 && second < 4 && second != first &&
          (cqe.flags & VSR_IO_CQE_MORE));

    CHECK(f->factory->peer_send(f, peers[0], "one", 3) == 0);
    memset(buffer, 0, sizeof(buffer));
    CHECK(run(f, recv_record(first, true, buffer, sizeof(buffer), UD(2))) == 3);
    CHECK(memcmp(buffer, "one", 3) == 0);
    /* CANCEL BY_FD names a slot with FIXED_FILE. */
    submit1(f, recv_record(second, true, buffer, sizeof(buffer), UD(3)));
    r = rec(VSR_IO_SQE_CANCEL, UD(4));
    r.flags = VSR_IO_SQE_FIXED_FILE;
    r.fd = second;
    r.op_flags = VSR_IO_CANCEL_BY_FD;
    CHECK(run(f, r) == 0);
    CHECK(take(f, UD(3)).result == -ECANCELED);

    CHECK(cancel_user_data(f, UD(1)) == 0);
    cqe = take(f, UD(1));
    CHECK(cqe.result == -ECANCELED && !(cqe.flags & VSR_IO_CQE_MORE));
    CHECK(!arrives(f, UD(1), 30, &cqe));
    CHECK(close_fd(f, first, true) == 0);
    CHECK(close_fd(f, second, true) == 0);
    CHECK(f->factory->peer_recv(f, peers[0], buffer, sizeof(buffer)) == 0);

    /* Without MULTISHOT: one accept, no MORE, nothing more. */
    r = rec(VSR_IO_SQE_ACCEPT, UD(5));
    r.fd = listener;
    submit1(f, r);
    peers[2] = f->factory->peer_connect(f, &dial);
    CHECK(peers[2] >= 0);
    cqe = take(f, UD(5));
    CHECK(cqe.result >= 0 && cqe.flags == 0);
    CHECK(close_fd(f, cqe.result, false) == 0);
    CHECK(!arrives(f, UD(5), 30, &cqe));
    for (uint32_t i = 0; i < 3; ++i) {
        CHECK(f->factory->peer_close(f, peers[i], false) == 0);
    }
    CHECK(close_fd(f, listener, false) == 0);
}

static void accept_until_enfile(struct fixture *f, int32_t listener,
                                const struct vsr_io_address *dial, int *peers)
{
    struct vsr_io_sqe r;
    struct vsr_io_cqe cqe;

    CHECK(f->ex.ops->register_files(f->ex.ctx, 2) == 0);
    r = rec(VSR_IO_SQE_ACCEPT, UD(1));
    r.fd = listener;
    r.flags = VSR_IO_SQE_DIRECT;
    r.fd2 = VSR_IO_SLOT_ALLOC;
    r.op_flags = VSR_IO_ACCEPT_MULTISHOT;
    submit1(f, r);
    for (uint32_t i = 0; i < 2; ++i) {
        peers[i] = f->factory->peer_connect(f, dial);
        CHECK(peers[i] >= 0);
        cqe = take(f, UD(1));
        CHECK(cqe.result >= 0 && cqe.result < 2 &&
              (cqe.flags & VSR_IO_CQE_MORE));
    }
    peers[2] = f->factory->peer_connect(f, dial);
    CHECK(peers[2] >= 0);
    cqe = take(f, UD(1));
    CHECK(cqe.result == -ENFILE && !(cqe.flags & VSR_IO_CQE_MORE));
    CHECK(!arrives(f, UD(1), 30, &cqe));
}

/* A multishot DIRECT accept with no free slot terminates with -ENFILE. */
static void scenario_accept_enfile(struct fixture *f)
{
    struct vsr_io_address dial;
    int32_t listener = open_listener(f, &dial);
    int peers[3];

    accept_until_enfile(f, listener, &dial, peers);
    for (uint32_t i = 0; i < 3; ++i) {
        CHECK(f->factory->peer_close(f, peers[i], false) == 0);
    }
    CHECK(close_fd(f, listener, false) == 0);
}

/* After -ENFILE the pending connection stays queued: once a slot is free
 * a new accept takes it, and the connection carries bytes. */
static void scenario_accept_enfile_queued(struct fixture *f)
{
    struct vsr_io_address dial;
    struct vsr_io_sqe r;
    struct vsr_io_cqe cqe;
    char buffer[16];
    int32_t listener = open_listener(f, &dial);
    int peers[3];
    int fresh;
    int n;

    accept_until_enfile(f, listener, &dial, peers);
    /* The connection that found no slot was accepted and closed, so its
     * peer sees the end of the stream or a reset, and nothing stays queued
     * for the next accept (decision 65). */
    n = f->factory->peer_recv(f, peers[2], buffer, sizeof(buffer));
    CHECK(n == 0 || n == -ECONNRESET);
    CHECK(close_fd(f, 0, true) == 0);
    r = rec(VSR_IO_SQE_ACCEPT, UD(2));
    r.fd = listener;
    r.flags = VSR_IO_SQE_DIRECT;
    r.fd2 = VSR_IO_SLOT_ALLOC;
    submit1(f, r);
    CHECK(!arrives(f, UD(2), 100, &cqe));
    fresh = f->factory->peer_connect(f, &dial);
    CHECK(fresh >= 0);
    CHECK(arrives(f, UD(2), 500, &cqe));
    CHECK(cqe.result == 0 && cqe.flags == 0);
    CHECK(f->factory->peer_send(f, fresh, "queued", 6) == 0);
    memset(buffer, 0, sizeof(buffer));
    CHECK(run(f, recv_record(0, true, buffer, sizeof(buffer), UD(3))) == 6);
    CHECK(memcmp(buffer, "queued", 6) == 0);
    for (uint32_t i = 0; i < 3; ++i) {
        CHECK(f->factory->peer_close(f, peers[i], false) == 0);
    }
    CHECK(f->factory->peer_close(f, fresh, false) == 0);
    CHECK(close_fd(f, listener, false) == 0);
}

/* RECV without BUFFER_SELECT: bytes, PEEK, partial reads, 0 at the end of
 * the stream, -ECONNRESET on reset. */
static void scenario_recv_plain(struct fixture *f)
{
    struct link l;
    struct vsr_io_sqe r;
    char buffer[64];

    open_link(f, &l);
    CHECK(f->factory->peer_send(f, l.peer, "hello", 5) == 0);
    memset(buffer, 0, sizeof(buffer));
    CHECK(run(f, recv_record(l.server, false, buffer, 64, UD(1))) == 5);
    CHECK(memcmp(buffer, "hello", 5) == 0);
    CHECK(f->factory->peer_send(f, l.peer, "xyz", 3) == 0);
    memset(buffer, 0, sizeof(buffer));
    r = recv_record(l.server, false, buffer, 64, UD(2));
    r.op_flags = VSR_IO_RECV_PEEK;
    CHECK(run(f, r) == 3 && memcmp(buffer, "xyz", 3) == 0);
    memset(buffer, 0, sizeof(buffer));
    CHECK(run(f, recv_record(l.server, false, buffer, 2, UD(3))) == 2);
    CHECK(memcmp(buffer, "xy", 2) == 0);
    CHECK(run(f, recv_record(l.server, false, buffer, 64, UD(4))) == 1);
    CHECK(buffer[0] == 'z');
    CHECK(f->factory->peer_send(f, l.peer, "end", 3) == 0);
    CHECK(f->factory->peer_shutdown(f, l.peer) == 0);
    receive_exact(f, l.server, buffer, 3);
    CHECK(memcmp(buffer, "end", 3) == 0);
    CHECK(run(f, recv_record(l.server, false, buffer, 64, UD(5))) == 0);
    CHECK(run(f, recv_record(l.server, false, buffer, 64, UD(6))) == 0);
    close_link(f, &l);

    open_link(f, &l);
    submit1(f, recv_record(l.server, false, buffer, 64, UD(7)));
    CHECK(f->factory->peer_close(f, l.peer, true) == 0);
    CHECK(take(f, UD(7)).result == -ECONNRESET);
    CHECK(close_fd(f, l.server, false) == 0);
    CHECK(close_fd(f, l.listener, false) == 0);
}

/* The provided-buffer model: buffers leave the ring head first; a delivery
 * lands at the head buffer's base plus what it already delivered. */
struct provided {
    unsigned char *base;
    uint32_t length;
    uint32_t consumed;
    uint16_t id;
};

struct buffer_ring {
    struct provided buffers[16];
    struct vsr_io_buffer records[16];
    uint32_t head;
    bool incremental;
};

static void ring_model(struct buffer_ring *ring, unsigned char *pool,
                       uint32_t length, uint16_t first_id, bool incremental)
{
    memset(ring, 0, sizeof(*ring));
    ring->incremental = incremental;
    for (uint32_t i = 0; i < 16; ++i) {
        ring->buffers[i].base = pool + (size_t)length * i;
        ring->buffers[i].length = length;
        ring->buffers[i].id = (uint16_t)(first_id + i);
        ring->records[i].base = ring->buffers[i].base;
        ring->records[i].length = length;
        ring->records[i].id = ring->buffers[i].id;
    }
}

/* Takes multishot deliveries of `user_data` until `total` bytes equal to
 * `expected` arrived, checking every completion against the model. */
static void collect(struct fixture *f, uint64_t user_data,
                    struct buffer_ring *ring, const unsigned char *expected,
                    uint32_t total)
{
    uint32_t got = 0;

    while (got < total) {
        struct vsr_io_cqe cqe = take(f, user_data);
        struct provided *head = &ring->buffers[ring->head];
        uint32_t space = head->length - head->consumed;

        CHECK(cqe.result > 0);
        CHECK((cqe.flags & VSR_IO_CQE_MORE) && (cqe.flags & VSR_IO_CQE_BUFFER));
        CHECK(cqe.buffer_id == head->id);
        CHECK((uint32_t)cqe.result <= space);
        CHECK((uint32_t)cqe.result <= total - got);
        CHECK(memcmp(head->base + head->consumed, expected + got,
                     (size_t)cqe.result) == 0);
        got += (uint32_t)cqe.result;
        if (ring->incremental) {
            head->consumed += (uint32_t)cqe.result;
            if (head->consumed < head->length) {
                CHECK(cqe.flags & VSR_IO_CQE_BUFFER_MORE);
                continue;
            }
        }
        CHECK(!(cqe.flags & VSR_IO_CQE_BUFFER_MORE));
        ring->head++;
    }
    CHECK(got == total);
}

static struct vsr_io_sqe select_record(int32_t fd, uint16_t group,
                                       bool multishot, uint64_t user_data)
{
    struct vsr_io_sqe r = rec(VSR_IO_SQE_RECV, user_data);

    r.fd = fd;
    r.flags = VSR_IO_SQE_BUFFER_SELECT;
    r.buffer_group = group;
    r.op_flags = multishot ? VSR_IO_RECV_MULTISHOT : 0;
    return r;
}

/* PROVIDE (decision E7): the executor provides the buffers itself before
 * any other record of the batch reaches the kernel, so a receive armed in
 * the same batch takes them; it completes only when it fails (-ENOENT, a
 * full ring's -ENOSPC, -EINVAL for a flag or a LINK before it). */
static void scenario_provide(struct fixture *f)
{
    struct link l;
    struct buffer_ring ring;
    struct vsr_io_region memory;
    struct vsr_io_sqe batch[3];
    struct vsr_io_cqe cqe;
    unsigned char *ring_memory = page_alloc(4096);
    unsigned char *pool = page_alloc(4096);

    open_link(f, &l);
    memory.base = ring_memory;
    memory.size = 4096;
    CHECK(f->ex.ops->buffer_ring(f->ex.ctx, 7, 4, 0, &memory) == 0);
    ring_model(&ring, pool, 64, 20, false);
    batch[0] = rec(VSR_IO_SQE_PROVIDE, UD(1));
    batch[0].buffer_group = 7;
    batch[0].addr = ring.records;
    batch[0].length = 2;
    batch[1] = select_record(l.server, 7, true, UD(2));
    submit(f, batch, 2);
    CHECK(f->factory->peer_send(f, l.peer, "abc", 3) == 0);
    collect(f, UD(2), &ring, (const unsigned char *)"abc", 3);
    CHECK(f->factory->peer_send(f, l.peer, "defg", 4) == 0);
    collect(f, UD(2), &ring, (const unsigned char *)"defg", 4);
    CHECK(!arrives(f, UD(1), 30, &cqe)); /* No completion on success. */
    /* Refusals complete, with provide()'s errno or -EINVAL. */
    batch[0].user_data = UD(3);
    batch[0].buffer_group = 9;
    submit(f, batch, 1);
    CHECK(take(f, UD(3)).result == -ENOENT);
    batch[0].user_data = UD(4);
    batch[0].buffer_group = 7;
    batch[0].addr = &ring.records[2];
    batch[0].length = 5; /* Four entries in the ring. */
    submit(f, batch, 1);
    CHECK(take(f, UD(4)).result == -ENOSPC);
    batch[0].user_data = UD(5);
    batch[0].length = 1;
    batch[0].flags = VSR_IO_SQE_SKIP_SUCCESS;
    submit(f, batch, 1);
    CHECK(take(f, UD(5)).result == -EINVAL);
    batch[0] = rec(VSR_IO_SQE_NOP, UD(6));
    batch[0].flags = VSR_IO_SQE_LINK;
    batch[1] = rec(VSR_IO_SQE_PROVIDE, UD(7));
    batch[1].buffer_group = 7;
    batch[1].addr = &ring.records[2];
    batch[1].length = 1;
    submit(f, batch, 2);
    CHECK(take(f, UD(7)).result == -EINVAL);
    cqe = take(f, UD(6));
    (void)cqe; /* Its chain ended with the refused record: either way. */
    CHECK(cancel_user_data(f, UD(2)) == 0);
    CHECK(take(f, UD(2)).result == -ECANCELED);
    CHECK(f->ex.ops->buffer_ring(f->ex.ctx, 7, 0, 0, NULL) == 0);
    close_link(f, &l);
    free(ring_memory);
    free(pool);
}

/* RECV with BUFFER_SELECT on an INCREMENTAL ring whose buffers are smaller
 * than one delivery: BUFFER_MORE, buffers leaving the ring, -ENOBUFS
 * termination, re-arm after provide, -ENOSPC on a full ring. */
static void scenario_recv_select_incremental(struct fixture *f)
{
    struct link l;
    struct buffer_ring ring;
    struct vsr_io_region memory;
    struct vsr_io_cqe cqe;
    unsigned char *ring_memory = page_alloc(4096);
    unsigned char *pool = page_alloc(4096);
    unsigned char pattern[256];

    fill_pattern(pattern, sizeof(pattern));
    open_link(f, &l);
    memory.base = ring_memory;
    memory.size = 4096;
    CHECK(f->ex.ops->buffer_ring(f->ex.ctx, 7, 6, 0, &memory) < 0);
    CHECK(f->ex.ops->buffer_ring(f->ex.ctx, 7, 8,
                                 VSR_IO_BUFFER_RING_INCREMENTAL, &memory) == 0);
    CHECK(f->ex.ops->buffer_ring(f->ex.ctx, 7, 8,
                                 VSR_IO_BUFFER_RING_INCREMENTAL, &memory) < 0);
    ring_model(&ring, pool, 64, 10, true);
    CHECK(f->ex.ops->provide(f->ex.ctx, 9, ring.records, 1) < 0);
    CHECK(f->ex.ops->provide(f->ex.ctx, 7, ring.records, 2) == 0);
    submit1(f, select_record(l.server, 7, true, UD(1)));
    CHECK(f->factory->peer_send(f, l.peer, "abc", 3) == 0);
    collect(f, UD(1), &ring, (const unsigned char *)"abc", 3);
    CHECK(f->factory->peer_send(f, l.peer, "defg", 4) == 0);
    collect(f, UD(1), &ring, (const unsigned char *)"defg", 4);
    CHECK(memcmp(pool, "abcdefg", 7) == 0);
    CHECK(ring.head == 0 && ring.buffers[0].consumed == 7);
    /* 60 bytes: the 57 left in buffer 10, which leaves the ring, then 3 in
     * buffer 11. */
    CHECK(f->factory->peer_send(f, l.peer, pattern, 60) == 0);
    collect(f, UD(1), &ring, pattern, 60);
    CHECK(ring.head == 1 && ring.buffers[1].consumed == 3);
    /* 100 bytes: 61 fill buffer 11, then the ring is empty at the next
     * delivery: -ENOBUFS ends the multishot receive. */
    CHECK(f->factory->peer_send(f, l.peer, pattern + 60, 100) == 0);
    collect(f, UD(1), &ring, pattern + 60, 61);
    CHECK(ring.head == 2);
    cqe = take(f, UD(1));
    CHECK(cqe.result == -ENOBUFS && !(cqe.flags & VSR_IO_CQE_MORE));
    CHECK(!arrives(f, UD(1), 30, &cqe));
    /* Re-arm after providing: the 39 bytes waiting arrive. */
    CHECK(f->ex.ops->provide(f->ex.ctx, 7, &ring.records[2], 1) == 0);
    submit1(f, select_record(l.server, 7, true, UD(2)));
    collect(f, UD(2), &ring, pattern + 121, 39);
    CHECK(ring.head == 2 && ring.buffers[2].consumed == 39);
    /* The ring holds 8 entries and buffer 12 is still in it. */
    CHECK(f->ex.ops->provide(f->ex.ctx, 7, &ring.records[3], 8) == -ENOSPC);
    CHECK(f->ex.ops->provide(f->ex.ctx, 7, &ring.records[3], 5) == 0);
    CHECK(f->ex.ops->provide(f->ex.ctx, 7, &ring.records[8], 3) == -ENOSPC);
    CHECK(f->ex.ops->provide(f->ex.ctx, 7, &ring.records[8], 2) == 0);
    CHECK(f->ex.ops->provide(f->ex.ctx, 7, &ring.records[10], 1) == -ENOSPC);
    /* Cancel terminates the multishot receive. */
    CHECK(cancel_user_data(f, UD(2)) == 0);
    cqe = take(f, UD(2));
    CHECK(cqe.result == -ECANCELED && !(cqe.flags & VSR_IO_CQE_MORE));
    CHECK(f->ex.ops->buffer_ring(f->ex.ctx, 7, 0, 0, NULL) == 0);
    CHECK(f->ex.ops->buffer_ring(f->ex.ctx, 7, 0, 0, NULL) < 0);
    close_link(f, &l);
    free(ring_memory);
    free(pool);
}

/* A ring without INCREMENTAL consumes a whole buffer per completion; a
 * single-shot BUFFER_SELECT receive takes one buffer. */
static void scenario_recv_select_whole(struct fixture *f)
{
    struct link l;
    struct buffer_ring ring;
    struct vsr_io_region memory;
    struct vsr_io_cqe cqe;
    unsigned char *ring_memory = page_alloc(4096);
    unsigned char *pool = page_alloc(4096);
    unsigned char pattern[256];

    fill_pattern(pattern, sizeof(pattern));
    open_link(f, &l);
    memory.base = ring_memory;
    memory.size = 4096;
    CHECK(f->ex.ops->buffer_ring(f->ex.ctx, 8, 4, 0, &memory) == 0);
    ring_model(&ring, pool, 64, 20, false);
    CHECK(f->ex.ops->provide(f->ex.ctx, 8, ring.records, 2) == 0);
    submit1(f, select_record(l.server, 8, true, UD(1)));
    CHECK(f->factory->peer_send(f, l.peer, pattern, 100) == 0);
    collect(f, UD(1), &ring, pattern, 100);
    CHECK(ring.head == 2);
    CHECK(f->factory->peer_send(f, l.peer, "z", 1) == 0);
    cqe = take(f, UD(1));
    CHECK(cqe.result == -ENOBUFS && !(cqe.flags & VSR_IO_CQE_MORE));
    CHECK(f->ex.ops->provide(f->ex.ctx, 8, &ring.records[2], 1) == 0);
    cqe =
        (submit1(f, select_record(l.server, 8, false, UD(2))), take(f, UD(2)));
    CHECK(cqe.result == 1 && cqe.buffer_id == 22 &&
          cqe.flags == VSR_IO_CQE_BUFFER);
    CHECK(pool[128] == 'z');
    CHECK(!arrives(f, UD(2), 30, &cqe));
    CHECK(f->ex.ops->buffer_ring(f->ex.ctx, 8, 0, 0, NULL) == 0);
    close_link(f, &l);
    free(ring_memory);
    free(pool);
}

/* Arming a BUFFER_SELECT receive on an empty ring is allowed: it pends,
 * and terminates with -ENOBUFS at its first delivery. */
static void scenario_recv_select_empty_arm(struct fixture *f)
{
    struct link l;
    struct buffer_ring ring;
    struct vsr_io_region memory;
    struct vsr_io_cqe cqe;
    unsigned char *ring_memory = page_alloc(4096);
    unsigned char *pool = page_alloc(4096);

    open_link(f, &l);
    memory.base = ring_memory;
    memory.size = 4096;
    CHECK(f->ex.ops->buffer_ring(f->ex.ctx, 7, 8,
                                 VSR_IO_BUFFER_RING_INCREMENTAL, &memory) == 0);
    ring_model(&ring, pool, 64, 10, true);
    submit1(f, select_record(l.server, 7, true, UD(1)));
    CHECK(!arrives(f, UD(1), 50, &cqe));
    CHECK(f->factory->peer_send(f, l.peer, "x", 1) == 0);
    cqe = take(f, UD(1));
    CHECK(cqe.result == -ENOBUFS && !(cqe.flags & VSR_IO_CQE_MORE));
    CHECK(f->ex.ops->provide(f->ex.ctx, 7, ring.records, 1) == 0);
    submit1(f, select_record(l.server, 7, true, UD(2)));
    collect(f, UD(2), &ring, (const unsigned char *)"x", 1);
    CHECK(cancel_user_data(f, UD(2)) == 0);
    CHECK(take(f, UD(2)).result == -ECANCELED);
    CHECK(f->ex.ops->buffer_ring(f->ex.ctx, 7, 0, 0, NULL) == 0);
    close_link(f, &l);
    free(ring_memory);
    free(pool);
}

/* Multishot receive terminations: 0 at the end of the stream, -ECANCELED
 * on cancel, -ECONNRESET on reset, each with MORE clear and final. */
static void scenario_recv_multishot_terminations(struct fixture *f)
{
    struct link l;
    struct buffer_ring ring;
    struct vsr_io_region memory;
    struct vsr_io_cqe cqe;
    unsigned char *ring_memory = page_alloc(4096);
    unsigned char *pool = page_alloc(4096);

    memory.base = ring_memory;
    memory.size = 4096;
    CHECK(f->ex.ops->buffer_ring(f->ex.ctx, 7, 8,
                                 VSR_IO_BUFFER_RING_INCREMENTAL, &memory) == 0);
    ring_model(&ring, pool, 64, 10, true);
    CHECK(f->ex.ops->provide(f->ex.ctx, 7, ring.records, 4) == 0);

    open_link(f, &l);
    submit1(f, select_record(l.server, 7, true, UD(1)));
    CHECK(f->factory->peer_send(f, l.peer, "x", 1) == 0);
    collect(f, UD(1), &ring, (const unsigned char *)"x", 1);
    CHECK(f->factory->peer_shutdown(f, l.peer) == 0);
    cqe = take(f, UD(1));
    CHECK(cqe.result == 0 && !(cqe.flags & VSR_IO_CQE_MORE));
    CHECK(!arrives(f, UD(1), 30, &cqe));
    close_link(f, &l);

    open_link(f, &l);
    submit1(f, select_record(l.server, 7, true, UD(2)));
    CHECK(cancel_user_data(f, UD(2)) == 0);
    cqe = take(f, UD(2));
    CHECK(cqe.result == -ECANCELED && !(cqe.flags & VSR_IO_CQE_MORE));
    CHECK(!arrives(f, UD(2), 30, &cqe));
    close_link(f, &l);

    open_link(f, &l);
    submit1(f, select_record(l.server, 7, true, UD(3)));
    CHECK(f->factory->peer_close(f, l.peer, true) == 0);
    cqe = take(f, UD(3));
    CHECK(cqe.result == -ECONNRESET && !(cqe.flags & VSR_IO_CQE_MORE));
    CHECK(!arrives(f, UD(3), 30, &cqe));
    CHECK(close_fd(f, l.server, false) == 0);
    CHECK(close_fd(f, l.listener, false) == 0);
    CHECK(f->ex.ops->buffer_ring(f->ex.ctx, 7, 0, 0, NULL) == 0);
    free(ring_memory);
    free(pool);
}

/* Decision 58: a ring may be unregistered with receives pending; those
 * terminate with -ENOBUFS at their next delivery. */
static void scenario_buffer_ring_unregister_pending(struct fixture *f)
{
    struct link l;
    struct buffer_ring ring;
    struct vsr_io_region memory;
    struct vsr_io_cqe cqe;
    unsigned char *ring_memory = page_alloc(4096);
    unsigned char *pool = page_alloc(4096);

    open_link(f, &l);
    memory.base = ring_memory;
    memory.size = 4096;
    CHECK(f->ex.ops->buffer_ring(f->ex.ctx, 7, 8,
                                 VSR_IO_BUFFER_RING_INCREMENTAL, &memory) == 0);
    ring_model(&ring, pool, 64, 10, true);
    CHECK(f->ex.ops->provide(f->ex.ctx, 7, ring.records, 1) == 0);
    submit1(f, select_record(l.server, 7, true, UD(1)));
    CHECK(f->ex.ops->buffer_ring(f->ex.ctx, 7, 0, 0, NULL) == 0);
    CHECK(!arrives(f, UD(1), 30, &cqe));
    CHECK(f->factory->peer_send(f, l.peer, "x", 1) == 0);
    cqe = take(f, UD(1));
    CHECK(cqe.result == -ENOBUFS && !(cqe.flags & VSR_IO_CQE_MORE));
    /* The same group registers again over the same memory. */
    CHECK(f->ex.ops->buffer_ring(f->ex.ctx, 7, 8,
                                 VSR_IO_BUFFER_RING_INCREMENTAL, &memory) == 0);
    CHECK(f->ex.ops->provide(f->ex.ctx, 7, &ring.records[1], 1) == 0);
    ring.head = 1;
    submit1(f, select_record(l.server, 7, true, UD(2)));
    collect(f, UD(2), &ring, (const unsigned char *)"x", 1);
    CHECK(cancel_user_data(f, UD(2)) == 0);
    CHECK(take(f, UD(2)).result == -ECANCELED);
    CHECK(f->ex.ops->buffer_ring(f->ex.ctx, 7, 0, 0, NULL) == 0);
    close_link(f, &l);
    free(ring_memory);
    free(pool);
}

/* SEND: plain, VECTORED, short into a full socket buffer, -EPIPE or
 * -ECONNRESET once the peer has closed. */
static void scenario_send_plain(struct fixture *f)
{
    static unsigned char big[BIG];
    struct link l;
    struct vsr_io_sqe r;
    struct vsr_io_vec vecs[2];
    unsigned char *received;
    char buffer[16];
    int32_t fd;
    int32_t n;
    int peer;

    open_link(f, &l);
    CHECK(run(f, send_record(l.server, false, "hello", 5, UD(1))) == 5);
    peer_receive_exact(f, l.peer, buffer, 5);
    CHECK(memcmp(buffer, "hello", 5) == 0);
    vecs[0].base = readable("vec");
    vecs[0].length = 3;
    vecs[1].base = readable("tored");
    vecs[1].length = 5;
    r = send_record(l.server, false, vecs, 2, UD(2));
    r.op_flags = VSR_IO_SEND_VECTORED;
    CHECK(run(f, r) == 8);
    peer_receive_exact(f, l.peer, buffer, 8);
    CHECK(memcmp(buffer, "vectored", 8) == 0);
    close_link(f, &l);

    /* A peer that does not read and small buffers: the send is short and
     * the bytes sent are the prefix. */
    fill_pattern(big, sizeof(big));
    fd = dial_peer(f, true, &peer);
    n = run(f, send_record(fd, false, big, sizeof(big), UD(3)));
    printf("  short send: %d of %u bytes\n", n, (unsigned)sizeof(big));
    CHECK(n > 0 && (uint32_t)n < sizeof(big));
    received = malloc((size_t)n);
    CHECK(received != NULL);
    peer_receive_exact(f, peer, received, (size_t)n);
    CHECK(memcmp(received, big, (size_t)n) == 0);
    free(received);
    /* The peer closes: sends fail with -EPIPE or -ECONNRESET. */
    CHECK(f->factory->peer_close(f, peer, false) == 0);
    n = 0;
    for (uint32_t attempt = 0; attempt < 10 && n >= 0; ++attempt) {
        settle(f, 10);
        n = run(f, send_record(fd, false, big, 1024, UD(10 + attempt)));
    }
    CHECK(n == -EPIPE || n == -ECONNRESET);
    CHECK(close_fd(f, fd, false) == 0);
}

struct zero_copy {
    int32_t result;
    uint16_t result_flags;
    int32_t notif;
    uint16_t notif_flags;
};

/* A zero-copy send: the result with MORE, then the NOTIF, both always. */
static struct zero_copy zc_send(struct fixture *f, struct link *l,
                                struct vsr_io_sqe r, void *received,
                                size_t expected)
{
    struct zero_copy out;
    struct vsr_io_cqe cqe;

    r.op_flags |= VSR_IO_SEND_ZERO_COPY;
    submit1(f, r);
    cqe = take(f, r.user_data);
    out.result = cqe.result;
    out.result_flags = cqe.flags;
    CHECK(cqe.flags & VSR_IO_CQE_MORE);
    if (cqe.result > 0) {
        CHECK((size_t)cqe.result == expected);
        peer_receive_exact(f, l->peer, received, expected);
    }
    cqe = take(f, r.user_data);
    out.notif = cqe.result;
    out.notif_flags = cqe.flags;
    CHECK(cqe.result == 0 && (cqe.flags & VSR_IO_CQE_NOTIF) &&
          !(cqe.flags & VSR_IO_CQE_MORE));
    CHECK(!arrives(f, r.user_data, 30, &cqe));
    return out;
}

/* SEND with ZERO_COPY: plain, FIXED_BUFFER, vectored fixed and unfixed,
 * -EFAULT outside the region (still with a NOTIF), and after a failed
 * send; a plain send's FIXED_BUFFER rejection is one completion. */
static void scenario_send_zero_copy(struct fixture *f)
{
    struct link l;
    struct link other;
    struct vsr_io_sqe r;
    struct vsr_io_cqe cqe;
    struct vsr_io_region region;
    struct vsr_io_vec vecs[2];
    struct zero_copy z;
    unsigned char *zc = page_alloc(4096);
    char buffer[64];

    open_link(f, &l);
    CHECK(f->ex.ops->register_buffers(f->ex.ctx, 2) == 0);
    region.base = zc;
    region.size = 4096;
    CHECK(f->ex.ops->update_buffer(f->ex.ctx, 1, &region) == 0);

    z = zc_send(f, &l, send_record(l.server, false, "zc-data", 7, UD(1)),
                buffer, 7);
    CHECK(z.result == 7 && memcmp(buffer, "zc-data", 7) == 0);
    /* NOLINTNEXTLINE(bugprone-not-null-terminated-result): raw bytes */
    memcpy(zc, "fixed-zero-copy", 15);
    r = send_record(l.server, false, zc, 15, UD(2));
    r.flags |= VSR_IO_SQE_FIXED_BUFFER;
    r.buffer_index = 1;
    z = zc_send(f, &l, r, buffer, 15);
    CHECK(z.result == 15 && memcmp(buffer, "fixed-zero-copy", 15) == 0);
    vecs[0].base = zc + 6;
    vecs[0].length = 5;
    vecs[1].base = zc;
    vecs[1].length = 5;
    r = send_record(l.server, false, vecs, 2, UD(4));
    r.op_flags = VSR_IO_SEND_VECTORED;
    z = zc_send(f, &l, r, buffer, 10);
    CHECK(z.result == 10 && memcmp(buffer, "zero-fixed", 10) == 0);
    /* Outside the region: -EFAULT, and still the NOTIF. */
    r = send_record(l.server, false, buffer, 4, UD(5));
    r.flags |= VSR_IO_SQE_FIXED_BUFFER;
    r.buffer_index = 1;
    z = zc_send(f, &l, r, buffer, 0);
    CHECK(z.result == -EFAULT);
    /* A plain send's FIXED_BUFFER rejection: one completion. */
    r = send_record(l.server, false, buffer, 4, UD(6));
    r.flags |= VSR_IO_SQE_FIXED_BUFFER;
    r.buffer_index = 1;
    submit1(f, r);
    cqe = take(f, UD(6));
    CHECK(cqe.result == -EFAULT && cqe.flags == 0);
    CHECK(!arrives(f, UD(6), 30, &cqe));
    /* NOLINTNEXTLINE(bugprone-not-null-terminated-result): raw bytes */
    memcpy(zc, "plain-fixed", 11);
    r = send_record(l.server, false, zc, 11, UD(7));
    r.flags |= VSR_IO_SQE_FIXED_BUFFER;
    r.buffer_index = 1;
    CHECK(run(f, r) == 11);
    peer_receive_exact(f, l.peer, buffer, 11);
    CHECK(memcmp(buffer, "plain-fixed", 11) == 0);
    /* A failed zero-copy send after SHUTDOWN: result, then NOTIF. */
    open_link(f, &other);
    r = rec(VSR_IO_SQE_SHUTDOWN, UD(8));
    r.fd = other.server;
    r.length = SHUT_WR;
    CHECK(run(f, r) == 0);
    z = zc_send(f, &other, send_record(other.server, false, "late", 4, UD(9)),
                buffer, 0);
    CHECK(z.result == -EPIPE || z.result == -ECONNRESET);
    close_link(f, &other);
    /* Vectored from the registered region (decision 53). */
    /* NOLINTNEXTLINE(bugprone-not-null-terminated-result): raw bytes */
    memcpy(zc, "fixed-zero-copy", 15);
    r = send_record(l.server, false, vecs, 2, UD(3));
    r.flags |= VSR_IO_SQE_FIXED_BUFFER;
    r.buffer_index = 1;
    r.op_flags = VSR_IO_SEND_VECTORED;
    z = zc_send(f, &l, r, buffer, 10);
    CHECK(z.result == 10 && memcmp(buffer, "zero-fixed", 10) == 0);
    close_link(f, &l);
    free(zc);
}

/* SHUTDOWN: the peer's receive returns 0 after the queued bytes; a send
 * after shutting down the write side fails. */
static void scenario_shutdown(struct fixture *f)
{
    struct link l;
    struct vsr_io_sqe r;
    char buffer[16];
    int32_t n;

    open_link(f, &l);
    CHECK(run(f, send_record(l.server, false, "bye", 3, UD(1))) == 3);
    r = rec(VSR_IO_SQE_SHUTDOWN, UD(2));
    r.fd = l.server;
    r.length = SHUT_WR;
    CHECK(run(f, r) == 0);
    peer_receive_exact(f, l.peer, buffer, 3);
    CHECK(memcmp(buffer, "bye", 3) == 0);
    CHECK(f->factory->peer_recv(f, l.peer, buffer, sizeof(buffer)) == 0);
    n = run(f, send_record(l.server, false, "more", 4, UD(3)));
    CHECK(n == -EPIPE || n == -ECONNRESET);
    /* The read side still works. */
    CHECK(f->factory->peer_send(f, l.peer, "back", 4) == 0);
    receive_exact(f, l.server, buffer, 4);
    CHECK(memcmp(buffer, "back", 4) == 0);
    close_link(f, &l);
}

/* ------------------------------------------------------------------------
 * Scenarios: timers, cancellation, lifetimes
 * --------------------------------------------------------------------- */

/* TIMEOUT relative and ABSOLUTE, TIMEOUT_UPDATE, CANCEL of a timeout. */
static void scenario_timeouts(struct fixture *f)
{
    struct vsr_io_sqe r;
    struct vsr_io_sqe update;
    uint64_t target;
    uint64_t other;
    uint64_t missing = UD(999);
    uint64_t t0;
    uint64_t deadline;

    t0 = now(f);
    r = rec(VSR_IO_SQE_TIMEOUT, UD(1));
    r.offset = 20 * MS;
    CHECK(run(f, r) == -ETIME);
    CHECK(now(f) >= t0 + 20 * MS && now(f) < t0 + 2 * SECOND);

    deadline = now(f) + 30 * MS;
    r = rec(VSR_IO_SQE_TIMEOUT, UD(2));
    r.op_flags = VSR_IO_TIMEOUT_ABSOLUTE;
    r.offset = deadline;
    CHECK(run(f, r) == -ETIME);
    CHECK(now(f) >= deadline && now(f) < deadline + 2 * SECOND);
    t0 = now(f);
    r.user_data = UD(3);
    r.offset = t0 - 1; /* In the past: fires at once. */
    CHECK(run(f, r) == -ETIME);
    CHECK(now(f) < t0 + SECOND);

    /* Update to a relative and to an absolute expiry. */
    r = rec(VSR_IO_SQE_TIMEOUT, UD(4));
    r.offset = 10 * SECOND;
    submit1(f, r);
    target = UD(4);
    update = rec(VSR_IO_SQE_TIMEOUT_UPDATE, UD(5));
    update.addr2 = &target;
    update.offset = 20 * MS;
    t0 = now(f);
    CHECK(run(f, update) == 0);
    CHECK(take(f, UD(4)).result == -ETIME);
    CHECK(now(f) >= t0 + 20 * MS && now(f) < t0 + 2 * SECOND);
    r.user_data = UD(6);
    submit1(f, r);
    other = UD(6);
    update.user_data = UD(7);
    update.addr2 = &other;
    update.op_flags = VSR_IO_TIMEOUT_ABSOLUTE;
    deadline = now(f) + 20 * MS;
    update.offset = deadline;
    CHECK(run(f, update) == 0);
    CHECK(take(f, UD(6)).result == -ETIME);
    CHECK(now(f) >= deadline && now(f) < deadline + 2 * SECOND);
    update.user_data = UD(8);
    update.addr2 = &missing;
    CHECK(run(f, update) == -ENOENT);

    /* Cancel: -ECANCELED, and the clock does not run to the expiry. */
    r.user_data = UD(9);
    submit1(f, r);
    t0 = now(f);
    CHECK(cancel_user_data(f, UD(9)) == 0);
    CHECK(take(f, UD(9)).result == -ECANCELED);
    settle(f, 10);
    CHECK(now(f) < t0 + 5 * SECOND);
}

/* CANCEL by user_data, of nothing (-ENOENT), BY_FD (the first), BY_FD
 * with ALL (the count). */
static void scenario_cancel(struct fixture *f)
{
    struct link l;
    struct vsr_io_sqe r;
    struct vsr_io_cqe cqe;
    char buffer[16];
    uint64_t other;

    open_link(f, &l);
    submit1(f, recv_record(l.server, false, buffer, sizeof(buffer), UD(1)));
    CHECK(cancel_user_data(f, UD(1)) == 0);
    CHECK(take(f, UD(1)).result == -ECANCELED);
    CHECK(cancel_user_data(f, UD(1)) == -ENOENT);
    r = rec(VSR_IO_SQE_CANCEL, UD(2));
    r.fd = l.server;
    r.op_flags = VSR_IO_CANCEL_BY_FD;
    CHECK(run(f, r) == -ENOENT);

    submit1(f, recv_record(l.server, false, buffer, sizeof(buffer), UD(3)));
    submit1(f, recv_record(l.server, false, buffer, sizeof(buffer), UD(4)));
    r.user_data = UD(5);
    CHECK(run(f, r) == 0);
    /* Without ALL exactly one match is cancelled; the contract's "the
     * first" does not say in which order matches are searched. */
    cqe = take_next(f);
    CHECK(cqe.result == -ECANCELED &&
          (cqe.user_data == UD(3) || cqe.user_data == UD(4)));
    printf("  CANCEL BY_FD without ALL cancelled the %s of two records\n",
           cqe.user_data == UD(3) ? "earlier" : "later");
    other = cqe.user_data == UD(3) ? UD(4) : UD(3);
    CHECK(!arrives(f, other, 30, &cqe));
    r.user_data = UD(6);
    r.op_flags = VSR_IO_CANCEL_BY_FD | VSR_IO_CANCEL_ALL;
    CHECK(run(f, r) == 1);
    CHECK(take(f, other).result == -ECANCELED);
    for (uint32_t i = 0; i < 3; ++i) {
        submit1(f, recv_record(l.server, false, buffer, sizeof(buffer),
                               UD(10 + i)));
    }
    r.user_data = UD(7);
    CHECK(run(f, r) == 3);
    for (uint32_t i = 0; i < 3; ++i) {
        CHECK(take(f, UD(10 + i)).result == -ECANCELED);
    }
    /* A cancelled record is gone: nothing more completes for it, and the
     * socket still works. */
    CHECK(f->factory->peer_send(f, l.peer, "ok", 2) == 0);
    receive_exact(f, l.server, buffer, 2);
    CHECK(memcmp(buffer, "ok", 2) == 0);
    close_link(f, &l);
}

/* CANCEL of a record already completing: -EALREADY where reachable. A
 * write submitted in the same batch is completing in the simulation; a
 * real ring may have finished it (-ENOENT) or cancelled it (0). The
 * outcome must be consistent with the write's own completion. */
static void scenario_cancel_already(struct fixture *f)
{
    static unsigned char bytes[1u << 20];
    struct vsr_io_sqe batch[2];
    int32_t fd = open_at(f, f->dir, "busy", O_CREAT | O_RDWR, UD(1));
    int32_t cancelled;
    int32_t written;

    CHECK(fd >= 0);
    fill_pattern(bytes, sizeof(bytes));
    batch[0] = io_record(VSR_IO_SQE_WRITE, fd, bytes, sizeof(bytes), 0, UD(2));
    batch[1] = rec(VSR_IO_SQE_CANCEL, UD(3));
    batch[1].offset = UD(2);
    submit(f, batch, 2);
    cancelled = take(f, UD(3)).result;
    written = take(f, UD(2)).result;
    printf("  cancel of a completing write: %d; the write: %d\n", cancelled,
           written);
    CHECK(cancelled == -EALREADY || cancelled == -ENOENT || cancelled == 0);
    if (cancelled == 0) {
        CHECK(written == -ECANCELED);
    } else {
        CHECK(written == (int32_t)sizeof(bytes));
    }
    CHECK(close_fd(f, fd, false) == 0);
}

/* Decision 58: CLOSE never completes the records pending on the
 * descriptor; they keep the object alive until they complete. */
static void scenario_close_pending(struct fixture *f)
{
    struct link l;
    struct vsr_io_address dial;
    struct vsr_io_sqe r;
    struct vsr_io_cqe cqe;
    char buffer[16];
    int32_t listener;
    int peer;

    open_link(f, &l);
    submit1(f, recv_record(l.server, false, buffer, sizeof(buffer), UD(1)));
    CHECK(close_fd(f, l.server, false) == 0);
    CHECK(!arrives(f, UD(1), 30, &cqe));
    CHECK(f->factory->peer_send(f, l.peer, "late", 4) == 0);
    cqe = take(f, UD(1));
    CHECK(cqe.result == 4 && memcmp(buffer, "late", 4) == 0);
    /* The receive was the last reference: the peer sees the stream end. */
    CHECK(f->factory->peer_recv(f, l.peer, buffer, sizeof(buffer)) == 0);
    CHECK(f->factory->peer_close(f, l.peer, false) == 0);
    CHECK(close_fd(f, l.listener, false) == 0);

    /* A multishot accept outlives its listener's descriptor. */
    listener = open_listener(f, &dial);
    r = rec(VSR_IO_SQE_ACCEPT, UD(2));
    r.fd = listener;
    r.op_flags = VSR_IO_ACCEPT_MULTISHOT;
    submit1(f, r);
    CHECK(close_fd(f, listener, false) == 0);
    CHECK(!arrives(f, UD(2), 30, &cqe));
    peer = f->factory->peer_connect(f, &dial);
    CHECK(peer >= 0);
    cqe = take(f, UD(2));
    CHECK(cqe.result >= 0 && (cqe.flags & VSR_IO_CQE_MORE));
    CHECK(close_fd(f, cqe.result, false) == 0);
    CHECK(cancel_user_data(f, UD(2)) == 0);
    cqe = take(f, UD(2));
    CHECK(cqe.result == -ECANCELED && !(cqe.flags & VSR_IO_CQE_MORE));
    CHECK(f->factory->peer_close(f, peer, false) == 0);
}

/* AF_UNIX: a listener and a client of the executor in one namespace. */
static void scenario_unix_socket(struct fixture *f)
{
    struct sockaddr_un un;
    struct vsr_io_sqe r;
    struct vsr_io_cqe cqe;
    char buffer[16];
    uint32_t length;
    int32_t listener = make_socket(f, AF_UNIX);
    int32_t client = make_socket(f, AF_UNIX);
    int32_t server;

    CHECK(listener >= 0 && client >= 0);
    memset(&un, 0, sizeof(un));
    un.sun_family = AF_UNIX;
    snprintf(un.sun_path + 1, sizeof(un.sun_path) - 1, "vsr-conformance-%ld",
             (long)getpid());
    length = (uint32_t)(offsetof(struct sockaddr_un, sun_path) + 1 +
                        strlen(un.sun_path + 1));
    r = rec(VSR_IO_SQE_BIND, UD(1));
    r.fd = listener;
    r.addr = &un;
    r.length = length;
    CHECK(run(f, r) == 0);
    r = rec(VSR_IO_SQE_LISTEN, UD(2));
    r.fd = listener;
    r.length = 4;
    CHECK(run(f, r) == 0);
    r = rec(VSR_IO_SQE_ACCEPT, UD(3));
    r.fd = listener;
    submit1(f, r);
    r = rec(VSR_IO_SQE_CONNECT, UD(4));
    r.fd = client;
    r.addr = &un;
    r.length = length;
    CHECK(run(f, r) == 0);
    cqe = take(f, UD(3));
    CHECK(cqe.result >= 0 && cqe.flags == 0);
    server = cqe.result;
    CHECK(run(f, send_record(client, false, "unix", 4, UD(5))) == 4);
    receive_exact(f, server, buffer, 4);
    CHECK(memcmp(buffer, "unix", 4) == 0);
    CHECK(close_fd(f, client, false) == 0);
    CHECK(run(f, recv_record(server, false, buffer, 16, UD(6))) == 0);
    CHECK(close_fd(f, server, false) == 0);
    CHECK(close_fd(f, listener, false) == 0);
}

/* ------------------------------------------------------------------------
 * The wrapper at nonzero rates: every result is one the contract allows
 * --------------------------------------------------------------------- */

#define FAULTY_BYTES 3000u
#define FAULTY_LINK_BYTES 1500u
#define FAULTY_STEPS 100000u

static uint32_t at_most(uint32_t value, uint32_t bound)
{
    return value < bound ? value : bound;
}

/* A file written and read back in pieces: each result whole, short or
 * -EIO; the rest of a short count, or the whole piece after -EIO, is
 * issued again, rewriting the same bytes. */
static void faulty_file(struct fixture *f, const unsigned char *pattern)
{
    static unsigned char back[FAULTY_BYTES];
    int32_t fd = open_at(f, f->dir, "faulty", O_CREAT | O_RDWR, next_ud(f));
    uint32_t at = 0;

    CHECK(fd >= 0);
    for (uint32_t step = 0; at < FAULTY_BYTES; ++step) {
        uint32_t length = at_most(1u + (step * 37u) % 300u, FAULTY_BYTES - at);
        int32_t n =
            io(f, VSR_IO_SQE_WRITE, fd, pattern + at, length, at, next_ud(f));

        CHECK(step < FAULTY_STEPS);
        CHECK(n == -EIO || (n > 0 && (uint32_t)n <= length));
        at += n > 0 ? (uint32_t)n : 0;
    }
    for (uint32_t step = 0;; ++step) {
        struct vsr_io_sqe r = rec(VSR_IO_SQE_FSYNC, next_ud(f));
        int32_t n;

        CHECK(step < FAULTY_STEPS);
        r.fd = fd;
        n = run(f, r);
        CHECK(n == 0 || n == -EIO);
        if (n == 0) {
            break;
        }
    }
    at = 0;
    for (uint32_t step = 0; at < FAULTY_BYTES; ++step) {
        uint32_t length = at_most(1u + (step * 41u) % 300u, FAULTY_BYTES - at);
        int32_t n =
            io(f, VSR_IO_SQE_READ, fd, back + at, length, at, next_ud(f));

        CHECK(step < FAULTY_STEPS);
        CHECK(n == -EIO || (n > 0 && (uint32_t)n <= length));
        if (n > 0) {
            CHECK(memcmp(back + at, pattern + at, (size_t)n) == 0);
            at += (uint32_t)n;
        }
    }
    CHECK(close_fd(f, fd, false) == 0);
}

/* Plain receives posted before their bytes (whole, short, or cancelled
 * with the bytes left queued), then plain and zero-copy sends (whole or
 * short; a zero-copy result with MORE, then its NOTIF). */
static void faulty_stream(struct fixture *f, const unsigned char *pattern)
{
    static unsigned char got[FAULTY_BYTES];
    struct link l;
    uint32_t sent = 0;
    uint32_t received = 0;

    open_link(f, &l);
    for (uint32_t step = 0; received < FAULTY_BYTES; ++step) {
        uint32_t length =
            at_most(1u + (step * 53u) % 200u, FAULTY_BYTES - received);
        struct vsr_io_sqe r =
            recv_record(l.server, false, got + received, length, next_ud(f));
        struct vsr_io_cqe cqe;

        CHECK(step < FAULTY_STEPS);
        submit1(f, r);
        if (sent == received) {
            uint32_t chunk =
                at_most(1u + (step * 29u) % 250u, FAULTY_BYTES - sent);

            CHECK(f->factory->peer_send(f, l.peer, pattern + sent, chunk) == 0);
            sent += chunk;
        }
        cqe = take(f, r.user_data);
        CHECK(cqe.flags == 0);
        CHECK(cqe.result == -ECANCELED ||
              (cqe.result > 0 && (uint32_t)cqe.result <= length));
        if (cqe.result > 0) {
            CHECK(memcmp(got + received, pattern + received,
                         (size_t)cqe.result) == 0);
            received += (uint32_t)cqe.result;
        }
    }
    sent = 0;
    for (uint32_t step = 0; sent < FAULTY_BYTES; ++step) {
        uint32_t length =
            at_most(1u + (step * 41u) % 400u, FAULTY_BYTES - sent);
        bool zero_copy = step % 3 == 2;
        struct vsr_io_sqe r =
            send_record(l.server, false, pattern + sent, length, next_ud(f));
        struct vsr_io_cqe cqe;
        int32_t n;

        CHECK(step < FAULTY_STEPS);
        r.op_flags = zero_copy ? VSR_IO_SEND_ZERO_COPY : 0;
        submit1(f, r);
        cqe = take(f, r.user_data);
        n = cqe.result;
        CHECK(n > 0 && (uint32_t)n <= length);
        CHECK(cqe.flags == (zero_copy ? VSR_IO_CQE_MORE : 0));
        if (zero_copy) {
            cqe = take(f, r.user_data);
            CHECK(cqe.result == 0 && cqe.flags == VSR_IO_CQE_NOTIF);
        }
        peer_receive_exact(f, l.peer, got + sent, (size_t)n);
        CHECK(memcmp(got + sent, pattern + sent, (size_t)n) == 0);
        sent += (uint32_t)n;
    }
    close_link(f, &l);
}

/* Two links' multishot receives on one INCREMENTAL ring: where each
 * delivery's bytes lie follows from the order of the completions across
 * both, which delays must keep; a receive ended by an injected CANCEL (or
 * a full CQ) is re-armed and continues in the head buffer. */
static void faulty_shared_ring(struct fixture *f,
                               const unsigned char *patterns[2])
{
    struct link links[2];
    struct buffer_ring ring;
    struct vsr_io_region memory;
    unsigned char *ring_memory = page_alloc(4096);
    unsigned char *pool = page_alloc((size_t)16 * 256);
    uint64_t armed[2];
    uint32_t sent[2] = {0, 0};
    uint32_t received[2] = {0, 0};
    uint32_t chunks = 0;

    open_link(f, &links[0]);
    open_link(f, &links[1]);
    memory.base = ring_memory;
    memory.size = 4096;
    CHECK(f->ex.ops->buffer_ring(f->ex.ctx, 11, 16,
                                 VSR_IO_BUFFER_RING_INCREMENTAL, &memory) == 0);
    ring_model(&ring, pool, 256, 30, true);
    CHECK(f->ex.ops->provide(f->ex.ctx, 11, ring.records, 16) == 0);
    for (uint32_t i = 0; i < 2; ++i) {
        armed[i] = next_ud(f);
        submit1(f, select_record(links[i].server, 11, true, armed[i]));
    }
    for (uint32_t step = 0; received[0] + received[1] < 2 * FAULTY_LINK_BYTES;
         ++step) {
        struct vsr_io_cqe cqe;
        uint32_t which;

        CHECK(step < FAULTY_STEPS);
        for (uint32_t i = 0; i < 2; ++i) {
            if (sent[i] == received[i] && sent[i] < FAULTY_LINK_BYTES) {
                uint32_t chunk = at_most(1u + (chunks++ * 31u) % 120u,
                                         FAULTY_LINK_BYTES - sent[i]);

                CHECK(f->factory->peer_send(f, links[i].peer,
                                            patterns[i] + sent[i], chunk) == 0);
                sent[i] += chunk;
            }
        }
        cqe = take_next(f);
        CHECK(cqe.user_data == armed[0] || cqe.user_data == armed[1]);
        which = cqe.user_data == armed[0] ? 0 : 1;
        if (cqe.result > 0) {
            struct provided *head = &ring.buffers[ring.head];

            CHECK(ring.head < 16);
            CHECK((cqe.flags & VSR_IO_CQE_BUFFER) && cqe.buffer_id == head->id);
            CHECK((uint32_t)cqe.result <= head->length - head->consumed);
            CHECK((uint32_t)cqe.result <= sent[which] - received[which]);
            CHECK(memcmp(head->base + head->consumed,
                         patterns[which] + received[which],
                         (size_t)cqe.result) == 0);
            received[which] += (uint32_t)cqe.result;
            head->consumed += (uint32_t)cqe.result;
            if (head->consumed < head->length) {
                CHECK(cqe.flags & VSR_IO_CQE_BUFFER_MORE);
            } else {
                CHECK(!(cqe.flags & VSR_IO_CQE_BUFFER_MORE));
                ring.head++;
            }
        } else {
            CHECK(cqe.result == -ECANCELED && cqe.flags == 0);
        }
        if (!(cqe.flags & VSR_IO_CQE_MORE)) {
            armed[which] = next_ud(f);
            submit1(f,
                    select_record(links[which].server, 11, true, armed[which]));
        }
    }
    for (uint32_t i = 0; i < 2; ++i) {
        struct vsr_io_cqe cqe;

        CHECK(cancel_user_data(f, armed[i]) == 0);
        cqe = take(f, armed[i]);
        CHECK(cqe.result == -ECANCELED && cqe.flags == 0);
        close_link(f, &links[i]);
    }
    CHECK(f->ex.ops->buffer_ring(f->ex.ctx, 11, 0, 0, NULL) == 0);
    free(ring_memory);
    free(pool);
}

static void scenario_faulty_rates(struct fixture *f)
{
    static unsigned char pattern[FAULTY_BYTES];
    static unsigned char lower[FAULTY_LINK_BYTES];
    const unsigned char *patterns[2] = {pattern, lower};
    struct faulty_executor_options options;
    struct faulty_executor_stats stats;

    if (f->faulty == NULL) {
        skip("no wrapper");
    }
    fill_pattern(pattern, sizeof(pattern));
    for (uint32_t i = 0; i < FAULTY_LINK_BYTES; ++i) {
        lower[i] = (unsigned char)(pattern[i] | 0x20); /* Lower case. */
    }
    memset(&options, 0, sizeof(options));
    options.eio_ppm = 250000;
    options.short_ppm = 300000;
    options.delay_ppm = 300000;
    options.delay_reaps_max = 4;
    options.cancel_recv_ppm = 300000;
    f->faults = true;
    faulty_executor_set_options(f->faulty, &options);
    faulty_file(f, pattern);
    faulty_stream(f, pattern);
    faulty_shared_ring(f, patterns);
    faulty_executor_stats(f->faulty, &stats);
    printf("  injected: eio %llu, shortened %llu, delayed %llu, "
           "cancelled %llu\n",
           (unsigned long long)stats.eio, (unsigned long long)stats.shortened,
           (unsigned long long)stats.delayed,
           (unsigned long long)stats.cancelled);
    CHECK(stats.eio > 0 && stats.shortened > 0 && stats.delayed > 0 &&
          stats.cancelled > 0);
}

/* ------------------------------------------------------------------------
 * Factories: the wrapper at rate zero over each executor
 * --------------------------------------------------------------------- */

static void faulty_wrap(struct fixture *f)
{
    struct faulty_executor_options options;

    memset(&options, 0, sizeof(options));
    options.seed = 1;
    options.delay_reaps_max = 4;
    f->faulty = calloc(1, sizeof(*f->faulty));
    CHECK(f->faulty != NULL);
    f->inner = f->ex;
    faulty_executor_init(f->faulty, &f->inner, &options);
    f->ex = faulty_executor_handle(f->faulty);
}

/* At rate zero nothing was injected. */
static void faulty_check_quiet(const struct fixture *f)
{
    struct faulty_executor_stats stats;

    faulty_executor_stats(f->faulty, &stats);
    CHECK(f->faults || (stats.eio == 0 && stats.shortened == 0 &&
                        stats.delayed == 0 && stats.cancelled == 0));
}

static void faulty_unwrap(struct fixture *f)
{
    faulty_check_quiet(f);
    f->ex = f->inner;
    free(f->faulty);
    f->faulty = NULL;
}

static int faulty_sim_create(struct fixture *f)
{
    CHECK(sim_create(f) == 0);
    faulty_wrap(f);
    return 0;
}

static void faulty_sim_destroy(struct fixture *f)
{
    faulty_unwrap(f);
    sim_destroy(f);
}

static int faulty_ring_create(struct fixture *f)
{
    int rc = ring_create(f);

    if (rc == 0) {
        faulty_wrap(f);
    }
    return rc;
}

static void faulty_ring_destroy(struct fixture *f)
{
    struct faulty_executor *faulty = f->faulty;

    faulty_check_quiet(f);
    ring_destroy(f); /* ring_close unwraps to deinit the ring. */
    f->faulty = NULL;
    free(faulty);
}

static const struct factory faulty_sim_factory = {
    .name = "faulty-sim",
    .create = faulty_sim_create,
    .destroy = faulty_sim_destroy,
    .wait = sim_wait,
    .wake_elsewhere = sim_wake_elsewhere,
    .direct_alignment = sim_direct_alignment,
    .partition = sim_partition,
    .bind_address = sim_bind_address,
    .dial_address = sim_dial_address,
    .unused_address = sim_unused_address,
    .shrink_send_buffer = sim_shrink_send_buffer,
    .peer_listen = sim_peer_listen,
    .peer_accept = sim_peer_accept,
    .peer_connect = sim_peer_connect,
    .peer_send = sim_peer_send,
    .peer_recv = sim_peer_recv,
    .peer_shutdown = sim_peer_shutdown,
    .peer_close = sim_peer_close,
};

static const struct factory faulty_ring_factory = {
    .name = "faulty-uring",
    .temp_directory = true,
    .ring = true,
    .create = faulty_ring_create,
    .destroy = faulty_ring_destroy,
    .wait = ring_wait,
    .wake_elsewhere = ring_wake_elsewhere,
    .direct_alignment = ring_direct_alignment,
    .partition = ring_partition,
    .bind_address = ring_bind_address,
    .dial_address = ring_dial_address,
    .unused_address = ring_unused_address,
    .shrink_send_buffer = ring_shrink_send_buffer,
    .peer_listen = ring_peer_listen,
    .peer_accept = ring_peer_accept,
    .peer_connect = ring_peer_connect,
    .peer_send = ring_peer_send,
    .peer_recv = ring_peer_recv,
    .peer_shutdown = ring_peer_shutdown,
    .peer_close = ring_peer_close,
};

/* ------------------------------------------------------------------------
 * Driver
 * --------------------------------------------------------------------- */

struct scenario {
    const char *name;
    void (*run)(struct fixture *f);
};

static const struct scenario scenarios[] = {
    {"nop", scenario_nop},
    {"skip_success", scenario_skip_success},
    {"link_chains", scenario_link_chains},
    {"submit_and_wait", scenario_submit_and_wait},
    {"clock_and_random", scenario_clock_and_random},
    {"wake", scenario_wake},
    {"registration", scenario_registration},
    {"file_io", scenario_file_io},
    {"odirect_alignment", scenario_odirect_alignment},
    {"odirect_address", scenario_odirect_address},
    {"directory_ops", scenario_directory_ops},
    {"fallocate_enospc", scenario_fallocate_enospc},
    {"fixed_buffers", scenario_fixed_buffers},
    {"fixed_files", scenario_fixed_files},
    {"files_update", scenario_files_update},
    {"socket_setup", scenario_socket_setup},
    {"socket_direct", scenario_socket_direct},
    {"connect_refused", scenario_connect_refused},
    {"connect_unreachable", scenario_connect_unreachable},
    {"accept_multishot", scenario_accept_multishot},
    {"accept_enfile", scenario_accept_enfile},
    {"accept_enfile_queued", scenario_accept_enfile_queued},
    {"recv_plain", scenario_recv_plain},
    {"recv_select_incremental", scenario_recv_select_incremental},
    {"recv_select_whole", scenario_recv_select_whole},
    {"recv_select_empty_arm", scenario_recv_select_empty_arm},
    {"recv_multishot_terminations", scenario_recv_multishot_terminations},
    {"buffer_ring_unregister_pending", scenario_buffer_ring_unregister_pending},
    {"provide", scenario_provide},
    {"send_plain", scenario_send_plain},
    {"send_zero_copy", scenario_send_zero_copy},
    {"shutdown", scenario_shutdown},
    {"timeouts", scenario_timeouts},
    {"cancel", scenario_cancel},
    {"cancel_already", scenario_cancel_already},
    {"close_pending", scenario_close_pending},
    {"unix_socket", scenario_unix_socket},
    {"faulty_rates", scenario_faulty_rates},
};

static const struct factory *const factories[] = {
    &sim_factory,       &ring_factory,       &ring_sqpoll_factory,
    &ring_napi_factory, &faulty_sim_factory, &faulty_ring_factory};

enum outcome { PASS, FAIL, SKIP };

/* Removes the temporary directory of a scenario, one level deep. */
static void remove_directory(const char *path)
{
    int dir = open(path, O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    DIR *directory;
    struct dirent *entry;

    CHECK(dir >= 0);
    directory = fdopendir(dir);
    CHECK(directory != NULL);
    while ((entry = readdir(directory)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 ||
            strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        if (unlinkat(dir, entry->d_name, 0) != 0) {
            CHECK(unlinkat(dir, entry->d_name, AT_REMOVEDIR) == 0);
        }
        rewinddir(directory);
    }
    CHECK(closedir(directory) == 0);
    CHECK(rmdir(path) == 0);
}

/* Runs one scenario in a child with a fresh fixture; `reason` receives
 * what the child reported. */
static enum outcome run_scenario(const struct factory *factory,
                                 const struct scenario *scenario, char *reason,
                                 size_t reason_size)
{
    struct fixture *f = calloc(1, sizeof(*f));
    int pipefd[2];
    pid_t pid;
    int status;
    size_t length = 0;

    CHECK(f != NULL);
    f->factory = factory;
    for (uint32_t i = 0; i < PEERS; ++i) {
        f->peers[i] = -1;
    }
    if (factory->temp_directory) {
        snprintf(f->dir_path, sizeof(f->dir_path),
                 "executor_conformance.XXXXXX");
        CHECK(mkdtemp(f->dir_path) != NULL);
    }
    CHECK(pipe2(pipefd, O_CLOEXEC) == 0);
    fflush(stdout);
    fflush(stderr);
    pid = fork();
    CHECK(pid >= 0);
    if (pid == 0) {
        int rc;

        (void)close(pipefd[0]);
        reason_fd = pipefd[1];
        (void)signal(SIGPIPE, SIG_IGN);
        (void)alarm(120);
        rc = factory->create(f);
        if (rc != 0) {
            skip("no executor: %s", strerror(-rc));
        }
        scenario->run(f);
        factory->destroy(f);
        free(f);
        exit(0);
    }
    (void)close(pipefd[1]);
    reason[0] = '\0';
    while (length + 1 < reason_size) {
        ssize_t n = read(pipefd[0], reason + length, reason_size - 1 - length);

        if (n <= 0) {
            break;
        }
        length += (size_t)n;
    }
    reason[length] = '\0';
    (void)close(pipefd[0]);
    CHECK(waitpid(pid, &status, 0) == pid);
    if (factory->temp_directory) {
        remove_directory(f->dir_path);
    }
    free(f);
    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
        return PASS;
    }
    if (WIFEXITED(status) && WEXITSTATUS(status) == 77) {
        return SKIP;
    }
    if (WIFEXITED(status)) {
        snprintf(reason, reason_size, "exit %d", WEXITSTATUS(status));
    } else if (WIFSIGNALED(status)) {
        snprintf(reason, reason_size, "%s",
                 WTERMSIG(status) == SIGALRM   ? "timed out"
                 : WTERMSIG(status) == SIGABRT ? "aborted"
                                               : strsignal(WTERMSIG(status)));
    }
    return FAIL;
}

int main(void)
{
    const size_t count = sizeof(scenarios) / sizeof(scenarios[0]);
    const char *only = getenv("VSR_CONFORMANCE_ONLY");
    bool failed = false;
    bool ring_skipped = false;
    char reason[256];

    setvbuf(stdout, NULL, _IONBF, 0);
    (void)signal(SIGPIPE, SIG_IGN);
    printf("executor conformance: %zu scenarios\n", count);
    for (size_t i = 0; i < sizeof(factories) / sizeof(factories[0]); ++i) {
        const struct factory *factory = factories[i];
        unsigned passed = 0;
        unsigned failures = 0;
        unsigned skipped = 0;

        if (factory->ring) {
            struct fixture *probe = calloc(1, sizeof(*probe));
            int rc;

            CHECK(probe != NULL);
            probe->factory = factory;
            rc = ring_open(probe);
            if (rc == 0) {
                ring_close(probe);
            }
            free(probe);
            if (rc == -ENOSYS || rc == -EPERM) {
                /* A variant the machine refuses (SQPOLL to an unprivileged
                 * user, say) is reported; only a missing ring skips. */
                printf("%s: skipped entirely: no ring (%s)\n", factory->name,
                       strerror(-rc));
                ring_skipped =
                    ring_skipped || (factory->sqpoll_idle_ms == 0 &&
                                     factory->napi_busy_poll_us == 0);
                continue;
            }
        }
        for (size_t s = 0; s < count; ++s) {
            enum outcome outcome;

            if (only != NULL && strcmp(only, scenarios[s].name) != 0) {
                continue;
            }
            outcome =
                run_scenario(factory, &scenarios[s], reason, sizeof(reason));
            if (outcome == PASS) {
                passed++;
                printf("%s: %s: PASS\n", factory->name, scenarios[s].name);
            } else if (outcome == SKIP) {
                skipped++;
                printf("%s: %s: SKIP(%s)\n", factory->name, scenarios[s].name,
                       reason);
            } else {
                failures++;
                failed = true;
                printf("%s: %s: FAIL(%s)\n", factory->name, scenarios[s].name,
                       reason);
            }
        }
        printf("%s: %u passed, %u failed, %u skipped\n", factory->name, passed,
               failures, skipped);
    }
    if (failed) {
        return 1;
    }
    return ring_skipped ? 77 : 0;
}
