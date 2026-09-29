#include "config.h"

#include "io/codec.h"
#include "io/crc32c.h"
#include "io/engine.h"
#include "io/link.h"
#include "io/stream.h"
#include "lib/check.h"
#include "lib/random.h"
#include "vsr-io.h"

#include <errno.h>
#include <inttypes.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdalign.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>

/*
 * The stream planner over the link planner, both real, in the in-memory
 * world of tests/unit/link.c (copied: two engines with fake executors over
 * modelled sockets; the test plays engine part 2 and the kernel). Added
 * here: READ records against per-engine caller files (short, random and
 * failing reads), the stream module's completions, deadlines, poll and
 * prepare in engine_step, and test doubles of the snapshot module's three
 * stream hooks, which override the weak stubs of stream.c.
 *
 * Harness API:
 *   world_reset(seed), engine_open(index, node, handshake)
 *   engine_step(e)            complete + deadlines + polls + prepares + run
 *   settle()                  step every engine until nothing moves
 *   world_advance(ns)         move the clock (all engines share it)
 *   forwarded_take(e, kind)   the next forwarded op of that kind, or NULL
 *                             (rotated to the head: op.data still points
 *                             at the original slot, so read `rail`)
 *   world.slice_max           random receive slicing (0: whole runs)
 *   world.hold_notifs         NOTIFs are queued until released
 *   world.short_read / random_reads / fail_read   file read faults
 *   world.streams / window / ops / chunk_bytes    limits of the next open
 *   link_reset(e, link)       the connection of a link is reset
 */

#define PAGE 4096u
#define NONE UINT32_MAX
#define SLABS 32u
#define ENGINES 2u
#define SOCKETS 24u
#define FD_BASE 300
#define INBOX_BYTES 65536u
#define RING_ENTRIES 128u
#define CQ_CAP 256u
#define SQ_CAP 32u
#define LOG_CAP 256u
#define FILE_SLOTS 16u
#define FILE_SLOT_BASE 40u
#define FILE_FD_BASE 100u /* Caller file slots: not the engine's. */
#define FILES 4u
#define REGION_BASE 5u
#define GROUP 3u
#define OWNER 0x5Au
#define TEST_OWNER NONE
#define LINKS 4u
#define NODES 4u
#define STREAMS 2u
#define WINDOW 2u
#define OPS 8u
#define CHUNK                                                                  \
    UINT64_C(1000) /* stream_chunk_bytes: a page holds three and a tail. */
#define BACKOFF_NS UINT64_C(1000000)
#define HANDSHAKE_NS UINT64_C(50000000)
#define IDLE_NS UINT64_C(200000000)
#define HELLO_BYTES (24u + 32u)

/* -------------------------------------------------------------------------
 * The world: sockets and the clock
 * ---------------------------------------------------------------------- */

struct sock {
    bool used;
    uint32_t owner; /* Engine index, or TEST_OWNER. */
    int raw_fd;     /* Raw descriptor, or -1. */
    int slot;       /* Registered slot, or -1. */
    bool listening;
    struct vsr_io_address address;
    uint32_t peer; /* Other end, or NONE. */
    bool peer_closed;
    bool reset;
    bool shutdown;
    bool nodelay; /* TCP_NODELAY set through SETSOCKOPT. */
    unsigned char inbox[INBOX_BYTES];
    uint32_t inbox_len;
    /* Pending records. */
    bool recv_armed;
    uint64_t recv_ud;
    bool recv_multishot;
    bool recv_select;
    unsigned char *recv_addr;
    uint32_t recv_len;
    bool send_parked; /* A SEND waiting for room in the peer's inbox: the
                         socket buffer is full, the TCP window closed. */
    uint64_t send_ud;
    const struct vsr_io_vec *send_vecs; /* Pinned while the send is out. */
    uint32_t send_count;
    bool send_zero_copy;
    bool accept_armed;
    uint64_t accept_ud;
    uint32_t backlog[8];
    uint32_t backlog_count;
};

struct ring_entry {
    uint16_t id;
    unsigned char *base;
    uint32_t length;
    uint32_t consumed;
};

struct record_log {
    uint8_t opcode;
    uint8_t flags;
    uint32_t op_flags;
    uint32_t length;
    uint16_t buffer_index;
};

/* A caller file behind a registered slot, read by FILE writes. */
struct file {
    const unsigned char *bytes;
    size_t size;
};

struct engine {
    struct vsr_io *io;
    uint32_t index;
    bool open;
    uint64_t node;
    struct ring_entry ring[RING_ENTRIES];
    uint32_t ring_head;
    uint32_t ring_count;
    struct vsr_io_cqe cq[CQ_CAP];
    uint32_t cq_count;
    struct record_log log[LOG_CAP];
    uint32_t log_count; /* Records since the last log_clear. */
    uint32_t records;   /* Records executed in the last step. */
    uint32_t deliveries;
    uint32_t updates; /* update_file calls. */
    int fail_update;  /* Result of the next update_file, 0 = OK. */
    struct vsr_io_address listen;
    struct file files[FILES]; /* By slot - FILE_FD_BASE. */
    uint32_t reads;           /* READ records executed. */
};

struct world {
    struct sock socks[SOCKETS];
    struct engine engines[ENGINES];
    uint64_t now;
    int next_fd;
    struct test_random random;
    uint32_t slice_max;
    bool hold_notifs;
    bool hold_cancels; /* A CANCEL's own result is held like a NOTIF. */
    uint32_t short_send;
    uint32_t inbox_limit; /* Socket buffer bytes: a full one parks sends. */
    uint32_t reject_send; /* Zero-copy sends to refuse with -EINVAL. */
    uint32_t link_queue;  /* limits.link_queue of the next engine_open. */
    uint32_t slab_bytes;  /* limits.slab_bytes of the next engine_open. */
    uint32_t streams;     /* limits.streams of the next engine_open. */
    uint32_t window;      /* limits.stream_window of the next engine_open. */
    uint32_t ops;         /* limits.ops of the next engine_open. */
    uint32_t chunk_bytes; /* stream_chunk_bytes of the next engine_open. */
    uint32_t short_read;  /* Reads return at most this many bytes; 0: all. */
    bool random_reads;    /* Reads return a random positive count. */
    uint32_t fail_read;   /* Reads to fail with -EIO. */
    struct vsr_io_cqe held[CQ_CAP]; /* NOTIFs held back, with the engine. */
    uint32_t held_engine[CQ_CAP];
    uint32_t held_count;
};

static struct world world;
static _Alignas(4096) unsigned char metadata[ENGINES][1u << 20];
#define SLAB_BYTES_MAX (4u * PAGE) /* world.slab_bytes at most. */
static _Alignas(4096) unsigned char payload[ENGINES][SLABS * SLAB_BYTES_MAX];

/* A record's receive buffer is written through its const addr, as the
 * simulation does (vsr_sim_mutable). */
static void *mutable_of(const void *pointer)
{
    void *writable;

    memcpy(&writable, &pointer, sizeof(writable));
    return writable;
}

static uint32_t sock_alloc(uint32_t owner)
{
    for (uint32_t i = 0; i < SOCKETS; ++i) {
        struct sock *sock = &world.socks[i];

        if (!sock->used) {
            memset(sock, 0, sizeof(*sock));
            sock->used = true;
            sock->owner = owner;
            sock->raw_fd = -1;
            sock->slot = -1;
            sock->peer = NONE;
            return i;
        }
    }
    CHECK(false); /* Socket table full. */
    return NONE;
}

static int fd_alloc(void)
{
    return world.next_fd++;
}

static uint32_t sock_by_fd(uint32_t owner, int fd)
{
    for (uint32_t i = 0; i < SOCKETS; ++i) {
        const struct sock *sock = &world.socks[i];

        if (sock->used && sock->owner == owner && sock->raw_fd == fd) {
            return i;
        }
    }
    return NONE;
}

static uint32_t sock_by_slot(uint32_t owner, int slot)
{
    for (uint32_t i = 0; i < SOCKETS; ++i) {
        const struct sock *sock = &world.socks[i];

        if (sock->used && sock->owner == owner && sock->slot == slot) {
            return i;
        }
    }
    return NONE;
}

/* A socket dies when no descriptor and no record holds it; the peer then
 * reads EOF after the bytes already queued. */
static void sock_drop(uint32_t index)
{
    struct sock *sock = &world.socks[index];

    if (sock->raw_fd >= 0 || sock->slot >= 0 || sock->recv_armed ||
        sock->accept_armed || sock->send_parked) {
        return; /* A parked send keeps the socket until it completes. */
    }
    if (sock->peer != NONE) {
        struct sock *peer = &world.socks[sock->peer];

        peer->peer_closed = true;
        peer->peer = NONE;
    }
    sock->used = false;
}

static void cq_push(struct engine *e, const struct vsr_io_cqe *cqe)
{
    CHECK(e->cq_count < CQ_CAP);
    e->cq[e->cq_count++] = *cqe;
}

static void complete(struct engine *e, uint64_t user_data, int32_t result,
                     uint16_t flags, uint16_t buffer_id)
{
    struct vsr_io_cqe cqe;

    cqe.user_data = user_data;
    cqe.result = result;
    cqe.flags = flags;
    cqe.buffer_id = buffer_id;
    cq_push(e, &cqe);
}

/* Bytes queued for a socket's owner to receive. */
static void inbox_append(uint32_t index, const unsigned char *bytes,
                         size_t length)
{
    struct sock *sock = &world.socks[index];

    CHECK(sock->inbox_len + length <= INBOX_BYTES);
    memcpy(sock->inbox + sock->inbox_len, bytes, length);
    sock->inbox_len += (uint32_t)length;
}

/* Takes back the last `length` bytes appended to a socket's inbox. */
static void inbox_consume_tail(uint32_t index, uint32_t length)
{
    CHECK(index != NONE && world.socks[index].inbox_len >= length);
    world.socks[index].inbox_len -= length;
}

static void inbox_consume(struct sock *sock, uint32_t length)
{
    memmove(sock->inbox, sock->inbox + length, sock->inbox_len - length);
    sock->inbox_len -= length;
}

/* -------------------------------------------------------------------------
 * Fake executor
 * ---------------------------------------------------------------------- */

static uint64_t fake_now(void *ctx)
{
    (void)ctx;
    return world.now;
}

static void fake_random(void *ctx, void *bytes, size_t size)
{
    unsigned char *out = bytes;

    (void)ctx;
    for (size_t i = 0; i < size; ++i) {
        out[i] = (unsigned char)test_random_next(&world.random);
    }
}

static int fake_submit_and_wait(void *ctx, const struct vsr_io_sqe *sqes,
                                uint32_t count, uint32_t want,
                                uint64_t min_wait_ns, uint64_t deadline_ns)
{
    (void)ctx;
    (void)sqes;
    (void)count;
    (void)want;
    (void)min_wait_ns;
    (void)deadline_ns;
    return 0;
}

static uint32_t fake_reap(void *ctx, struct vsr_io_cqe *cqes, uint32_t capacity)
{
    (void)ctx;
    (void)cqes;
    (void)capacity;
    return 0;
}

static int fake_register_files(void *ctx, uint32_t slots)
{
    (void)ctx;
    (void)slots;
    return 0;
}

static int fake_update_file(void *ctx, uint32_t slot, int fd)
{
    struct engine *e = ctx;
    uint32_t index;

    e->updates++;
    if (e->fail_update != 0) {
        int result = e->fail_update;

        e->fail_update = 0;
        return result;
    }
    CHECK(slot >= FILE_SLOT_BASE && slot < FILE_SLOT_BASE + FILE_SLOTS);
    index = sock_by_slot(e->index, (int)slot);
    if (index != NONE) {
        world.socks[index].slot = -1;
        sock_drop(index);
    }
    if (fd < 0) {
        return 0;
    }
    index = sock_by_fd(e->index, fd);
    if (index == NONE) {
        return -EBADF;
    }
    /* The executor takes the descriptor over. */
    world.socks[index].raw_fd = -1;
    world.socks[index].slot = (int)slot;
    return 0;
}

static int fake_register_buffers(void *ctx, uint32_t regions)
{
    (void)ctx;
    (void)regions;
    return 0;
}

static int fake_update_buffer(void *ctx, uint32_t index,
                              const struct vsr_io_region *region)
{
    (void)ctx;
    (void)index;
    (void)region;
    return 0;
}

static int fake_buffer_ring(void *ctx, uint16_t group, uint32_t entries,
                            uint32_t flags, const struct vsr_io_region *memory)
{
    (void)ctx;
    (void)group;
    (void)entries;
    (void)flags;
    (void)memory;
    return 0;
}

static int fake_provide(void *ctx, uint16_t group,
                        const struct vsr_io_buffer *buffers, uint32_t count)
{
    struct engine *e = ctx;

    CHECK(group == GROUP);
    for (uint32_t i = 0; i < count; ++i) {
        struct ring_entry *entry;

        CHECK(e->ring_count < RING_ENTRIES);
        entry = &e->ring[(e->ring_head + e->ring_count) % RING_ENTRIES];
        entry->id = buffers[i].id;
        entry->base = buffers[i].base;
        entry->length = buffers[i].length;
        entry->consumed = 0;
        e->ring_count++;
    }
    return 0;
}

static void fake_wake(void *ctx)
{
    (void)ctx;
}

static const struct vsr_io_executor_ops fake_ops = {
    .now = fake_now,
    .random = fake_random,
    .submit_and_wait = fake_submit_and_wait,
    .reap = fake_reap,
    .register_files = fake_register_files,
    .update_file = fake_update_file,
    .register_buffers = fake_register_buffers,
    .update_buffer = fake_update_buffer,
    .buffer_ring = fake_buffer_ring,
    .provide = fake_provide,
    .wake = fake_wake,
};

/* -------------------------------------------------------------------------
 * Record execution against the model
 * ---------------------------------------------------------------------- */

static void log_record(struct engine *e, const struct vsr_io_sqe *sqe)
{
    struct record_log *entry;

    if (e->log_count == LOG_CAP) {
        memmove(e->log, e->log + 1, (LOG_CAP - 1) * sizeof(e->log[0]));
        e->log_count--;
    }
    entry = &e->log[e->log_count++];
    entry->opcode = sqe->opcode;
    entry->flags = sqe->flags;
    entry->op_flags = sqe->op_flags;
    entry->length = sqe->length;
    entry->buffer_index = sqe->buffer_index;
}

/* The socket a record names: a slot with FIXED_FILE, else a raw fd. */
static uint32_t record_sock(const struct engine *e,
                            const struct vsr_io_sqe *sqe)
{
    if ((sqe->flags & VSR_IO_SQE_FIXED_FILE) != 0) {
        return sock_by_slot(e->index, sqe->fd);
    }
    return sock_by_fd(e->index, sqe->fd);
}

static uint32_t find_listener(const struct vsr_io_address *address)
{
    for (uint32_t i = 0; i < SOCKETS; ++i) {
        const struct sock *sock = &world.socks[i];

        if (sock->used && sock->listening &&
            sock->address.length == address->length &&
            memcmp(&sock->address.sockaddr, &address->sockaddr,
                   address->length) == 0) {
            return i;
        }
    }
    return NONE;
}

/* Hands queued connections of a listener to its armed ACCEPT. */
static void listener_deliver(struct engine *e, uint32_t index)
{
    struct sock *listener = &world.socks[index];

    while (listener->accept_armed && listener->backlog_count > 0) {
        uint32_t child = listener->backlog[0];

        memmove(listener->backlog, listener->backlog + 1,
                (listener->backlog_count - 1) * sizeof(listener->backlog[0]));
        listener->backlog_count--;
        world.socks[child].raw_fd = fd_alloc();
        complete(e, listener->accept_ud, world.socks[child].raw_fd,
                 VSR_IO_CQE_MORE, 0);
    }
}

/* Connects a fresh socket to a listener: the child is queued at the
 * listener; NONE when no listener is bound there. */
static uint32_t world_connect(uint32_t client, const struct vsr_io_address *to)
{
    uint32_t index = find_listener(to);
    uint32_t child;

    if (index == NONE) {
        return NONE;
    }
    child = sock_alloc(world.socks[index].owner);
    world.socks[child].peer = client;
    world.socks[client].peer = child;
    CHECK(world.socks[index].backlog_count < 8);
    world.socks[index].backlog[world.socks[index].backlog_count++] = child;
    if (world.socks[index].owner != TEST_OWNER) {
        listener_deliver(&world.engines[world.socks[index].owner], index);
    }
    return child;
}

static void cancel_record(struct engine *e, uint64_t user_data, int *result)
{
    for (uint32_t i = 0; i < SOCKETS; ++i) {
        struct sock *sock = &world.socks[i];

        if (!sock->used || sock->owner != e->index) {
            continue;
        }
        if (sock->recv_armed && sock->recv_ud == user_data) {
            sock->recv_armed = false;
            complete(e, user_data, -ECANCELED, 0, 0);
            sock_drop(i);
            *result = 0;
            return;
        }
        if (sock->accept_armed && sock->accept_ud == user_data) {
            sock->accept_armed = false;
            complete(e, user_data, -ECANCELED, 0, 0);
            sock_drop(i);
            *result = 0;
            return;
        }
    }
    *result = -ENOENT;
}

/* Transmits a SEND into the peer's inbox, which is the socket buffer:
 * what fits, short when world.short_send says so. A full inbox PARKS the
 * send until the peer receives, as the kernel holds a send while the TCP
 * window is closed; a parked send is retried at every step of its engine
 * (sends_resume) and fails once the socket is shut down, reset or
 * unpaired. Completes the record, with the zero-copy NOTIF rules. */
static void sock_send(struct engine *e, uint32_t index, uint64_t user_data,
                      const struct vsr_io_vec *vecs, uint32_t count,
                      bool zero_copy)
{
    struct sock *sock = &world.socks[index];
    uint32_t total = 0;
    uint32_t sent = 0;
    int result;

    for (uint32_t i = 0; i < count; ++i) {
        total += (uint32_t)vecs[i].length;
    }
    if (sock->reset) {
        result = -ECONNRESET;
    } else if (sock->peer == NONE || sock->shutdown) {
        result = -EPIPE;
    } else {
        uint32_t limit = world.short_send > 0 && world.short_send < total
                             ? world.short_send
                             : total;
        uint32_t room = world.inbox_limit - world.socks[sock->peer].inbox_len;

        if (limit > room) {
            limit = room;
        }
        for (uint32_t i = 0; i < count && sent < limit; ++i) {
            uint32_t take = (uint32_t)vecs[i].length;

            if (take > limit - sent) {
                take = limit - sent;
            }
            inbox_append(sock->peer, vecs[i].base, take);
            sent += take;
        }
        result = (int)sent;
    }
    if (zero_copy && world.reject_send > 0) {
        /* Refused at translation: once, without MORE, no NOTIF
         * (decision 62); nothing reached the socket. */
        world.reject_send--;
        if (sent > 0) {
            inbox_consume_tail(sock->peer, sent);
        }
        complete(e, user_data, -EINVAL, 0, 0);
        return;
    }
    if (result == 0 && total > 0) {
        sock->send_parked = true;
        sock->send_ud = user_data;
        sock->send_vecs = vecs;
        sock->send_count = count;
        sock->send_zero_copy = zero_copy;
        return;
    }
    if (!zero_copy) {
        complete(e, user_data, result, 0, 0);
        return;
    }
    complete(e, user_data, result, VSR_IO_CQE_MORE, 0);
    if (world.hold_notifs) {
        CHECK(world.held_count < CQ_CAP);
        world.held[world.held_count].user_data = user_data;
        world.held[world.held_count].result = 0;
        world.held[world.held_count].flags = VSR_IO_CQE_NOTIF;
        world.held[world.held_count].buffer_id = 0;
        world.held_engine[world.held_count] = e->index;
        world.held_count++;
    } else {
        complete(e, user_data, 0, VSR_IO_CQE_NOTIF, 0);
    }
}

/* Retries the parked sends of an engine's sockets: the peer may have
 * received (room in its inbox), or the socket may have gone. */
static void sends_resume(struct engine *e)
{
    for (uint32_t i = 0; i < SOCKETS; ++i) {
        struct sock *sock = &world.socks[i];

        if (sock->used && sock->owner == e->index && sock->send_parked) {
            sock->send_parked = false;
            sock_send(e, i, sock->send_ud, sock->send_vecs, sock->send_count,
                      sock->send_zero_copy);
            if (!sock->send_parked) {
                sock_drop(i);
            }
        }
    }
}

static void execute(struct engine *e, const struct vsr_io_sqe *sqe,
                    bool *chain_failed)
{
    uint32_t index;
    struct sock *sock;
    int result = 0;
    bool linked = (sqe->flags & VSR_IO_SQE_LINK) != 0;
    bool skip = (sqe->flags & VSR_IO_SQE_SKIP_SUCCESS) != 0;

    log_record(e, sqe);
    e->records++;
    if (*chain_failed) {
        complete(e, sqe->user_data, -ECANCELED, 0, 0);
        if (!linked) {
            *chain_failed = false;
        }
        return;
    }
    switch (sqe->opcode) {
    case VSR_IO_SQE_READ: {
        const struct file *file;
        uint32_t take;

        CHECK(sqe->flags == (VSR_IO_SQE_FIXED_FILE | VSR_IO_SQE_FIXED_BUFFER));
        CHECK(sqe->buffer_index == REGION_BASE);
        CHECK(vsr_io_pool_contains(&e->io->pool, sqe->addr, sqe->length));
        CHECK(sqe->fd >= (int32_t)FILE_FD_BASE &&
              sqe->fd < (int32_t)(FILE_FD_BASE + FILES));
        file = &e->files[(uint32_t)sqe->fd - FILE_FD_BASE];
        e->reads++;
        if (world.fail_read > 0) {
            world.fail_read--;
            result = -EIO;
            break;
        }
        take = sqe->offset >= file->size
                   ? 0
                   : (uint32_t)(file->size - sqe->offset < sqe->length
                                    ? file->size - sqe->offset
                                    : sqe->length);
        if (world.short_read > 0 && take > world.short_read) {
            take = world.short_read;
        }
        if (world.random_reads && take > 1) {
            take = 1 + test_random_bounded(&world.random, take);
        }
        memcpy(mutable_of(sqe->addr), file->bytes + sqe->offset, take);
        result = (int)take;
        break;
    }
    case VSR_IO_SQE_SOCKET:
        CHECK(sqe->op_flags == SOCK_STREAM);
        index = sock_alloc(e->index);
        if ((sqe->flags & VSR_IO_SQE_DIRECT) != 0) {
            CHECK(sqe->fd2 >= (int32_t)FILE_SLOT_BASE);
            world.socks[index].slot = sqe->fd2;
            result = sqe->fd2;
        } else {
            world.socks[index].raw_fd = fd_alloc();
            result = world.socks[index].raw_fd;
        }
        break;
    case VSR_IO_SQE_BIND:
        index = record_sock(e, sqe);
        CHECK(index != NONE);
        sock = &world.socks[index];
        sock->address.length = sqe->length;
        memcpy(&sock->address.sockaddr, sqe->addr, sqe->length);
        if (find_listener(&sock->address) != NONE) {
            result = -EADDRINUSE;
        }
        break;
    case VSR_IO_SQE_LISTEN:
        index = record_sock(e, sqe);
        CHECK(index != NONE);
        world.socks[index].listening = true;
        break;
    case VSR_IO_SQE_ACCEPT:
        index = record_sock(e, sqe);
        CHECK(index != NONE && world.socks[index].listening);
        CHECK(sqe->op_flags == VSR_IO_ACCEPT_MULTISHOT);
        CHECK((sqe->flags & VSR_IO_SQE_DIRECT) == 0);
        sock = &world.socks[index];
        sock->accept_armed = true;
        sock->accept_ud = sqe->user_data;
        listener_deliver(e, index);
        return;
    case VSR_IO_SQE_CONNECT: {
        struct vsr_io_address to;

        index = record_sock(e, sqe);
        CHECK(index != NONE);
        memset(&to, 0, sizeof(to));
        to.length = sqe->length;
        memcpy(&to.sockaddr, sqe->addr, sqe->length);
        if (world_connect(index, &to) == NONE) {
            result = -ECONNREFUSED;
        }
        break;
    }
    case VSR_IO_SQE_RECV:
        index = record_sock(e, sqe);
        CHECK(index != NONE);
        sock = &world.socks[index];
        CHECK(!sock->recv_armed);
        sock->recv_armed = true;
        sock->recv_ud = sqe->user_data;
        sock->recv_multishot = (sqe->op_flags & VSR_IO_RECV_MULTISHOT) != 0;
        sock->recv_select = (sqe->flags & VSR_IO_SQE_BUFFER_SELECT) != 0;
        if (sock->recv_select) {
            CHECK(sqe->buffer_group == GROUP && sock->recv_multishot);
        } else {
            CHECK(!sock->recv_multishot);
        }
        sock->recv_addr = mutable_of(sqe->addr);
        sock->recv_len = sqe->length;
        return;
    case VSR_IO_SQE_SEND: {
        const struct vsr_io_vec *vecs = sqe->addr;

        CHECK((sqe->op_flags & VSR_IO_SEND_VECTORED) != 0);
        CHECK(!linked && !skip);
        index = record_sock(e, sqe);
        CHECK(index != NONE);
        sock = &world.socks[index];
        CHECK(!sock->send_parked); /* One send in flight per socket. */
        for (uint32_t i = 0; i < sqe->length; ++i) {
            if ((sqe->flags & VSR_IO_SQE_FIXED_BUFFER) != 0) {
                CHECK(sqe->buffer_index == REGION_BASE);
                CHECK(vsr_io_pool_contains(&e->io->pool, vecs[i].base,
                                           vecs[i].length));
            }
        }
        sock_send(e, index, sqe->user_data, vecs, sqe->length,
                  (sqe->op_flags & VSR_IO_SEND_ZERO_COPY) != 0);
        return;
    }
    case VSR_IO_SQE_SETSOCKOPT:
        index = record_sock(e, sqe);
        CHECK(index != NONE);
        CHECK((sqe->flags & VSR_IO_SQE_FIXED_FILE) != 0);
        CHECK(sqe->op_flags == (((uint32_t)IPPROTO_TCP << 16) | TCP_NODELAY));
        CHECK(sqe->length == sizeof(int) && sqe->addr != NULL);
        CHECK(*(const int *)sqe->addr == 1);
        world.socks[index].nodelay = true;
        break;
    case VSR_IO_SQE_SHUTDOWN:
        index = record_sock(e, sqe);
        CHECK(index != NONE);
        sock = &world.socks[index];
        CHECK(sqe->length == SHUT_RDWR);
        sock->shutdown = true;
        if (sock->peer != NONE) {
            world.socks[sock->peer].peer_closed = true;
        }
        if (sock->send_parked) {
            /* Shutting down wakes a send blocked on the window: -EPIPE. */
            sock->send_parked = false;
            sock_send(e, index, sock->send_ud, sock->send_vecs,
                      sock->send_count, sock->send_zero_copy);
        }
        break;
    case VSR_IO_SQE_CLOSE:
        index = record_sock(e, sqe);
        if (index == NONE) {
            result = -EBADF;
            break;
        }
        if ((sqe->flags & VSR_IO_SQE_FIXED_FILE) != 0) {
            world.socks[index].slot = -1;
        } else {
            world.socks[index].raw_fd = -1;
        }
        sock_drop(index);
        break;
    case VSR_IO_SQE_CANCEL:
        CHECK(sqe->op_flags == 0);
        cancel_record(e, sqe->offset, &result);
        if (world.hold_cancels) {
            CHECK(world.held_count < CQ_CAP);
            world.held[world.held_count].user_data = sqe->user_data;
            world.held[world.held_count].result = result;
            world.held[world.held_count].flags = 0;
            world.held[world.held_count].buffer_id = 0;
            world.held_engine[world.held_count] = e->index;
            world.held_count++;
            return;
        }
        break;
    default:
        CHECK(false); /* Unexpected opcode. */
        return;
    }
    if (result < 0 && linked) {
        *chain_failed = true;
    }
    if (result >= 0 && skip) {
        return;
    }
    complete(e, sqe->user_data, result, 0, 0);
}

/* Delivers queued bytes, EOF and resets to armed receives, sliced at
 * random when slice_max is set. */
static void deliver(struct engine *e)
{
    for (uint32_t i = 0; i < SOCKETS; ++i) {
        struct sock *sock = &world.socks[i];

        while (sock->used && sock->owner == e->index && sock->recv_armed) {
            uint32_t take;

            if (e->cq_count + 4 > CQ_CAP) {
                return; /* The completion queue is full; next step. */
            }
            if (sock->reset) {
                sock->recv_armed = false;
                complete(e, sock->recv_ud, -ECONNRESET, 0, 0);
                sock_drop(i);
                break;
            }
            if (sock->inbox_len == 0) {
                if (sock->peer_closed) {
                    sock->recv_armed = false;
                    complete(e, sock->recv_ud, 0, 0, 0);
                    sock_drop(i);
                }
                break;
            }
            take = sock->inbox_len;
            if (world.slice_max > 0) {
                uint32_t slice =
                    1 + test_random_bounded(&world.random, world.slice_max);

                if (slice < take) {
                    take = slice;
                }
            }
            if (sock->recv_select) {
                struct ring_entry *entry;
                uint16_t flags = VSR_IO_CQE_BUFFER;

                if (e->ring_count == 0) {
                    sock->recv_armed = false;
                    complete(e, sock->recv_ud, -ENOBUFS, 0, 0);
                    sock_drop(i);
                    break;
                }
                entry = &e->ring[e->ring_head];
                if (take > entry->length - entry->consumed) {
                    take = entry->length - entry->consumed;
                }
                memcpy(entry->base + entry->consumed, sock->inbox, take);
                entry->consumed += take;
                if (entry->consumed < entry->length) {
                    flags |= VSR_IO_CQE_BUFFER_MORE;
                } else {
                    e->ring_head = (e->ring_head + 1) % RING_ENTRIES;
                    e->ring_count--;
                }
                if (sock->recv_multishot) {
                    flags |= VSR_IO_CQE_MORE;
                } else {
                    sock->recv_armed = false;
                }
                complete(e, sock->recv_ud, (int32_t)take, flags, entry->id);
            } else {
                if (take > sock->recv_len) {
                    take = sock->recv_len;
                }
                memcpy(sock->recv_addr, sock->inbox, take);
                sock->recv_armed = false;
                complete(e, sock->recv_ud, (int32_t)take, 0, 0);
            }
            inbox_consume(sock, take);
            e->deliveries++;
        }
    }
}

/* -------------------------------------------------------------------------
 * Engines
 * ---------------------------------------------------------------------- */

static void address_of(struct vsr_io_address *address, char tag)
{
    struct sockaddr_un *un = (struct sockaddr_un *)(void *)&address->sockaddr;

    memset(address, 0, sizeof(*address));
    un->sun_family = AF_UNIX;
    un->sun_path[0] = 0;
    un->sun_path[1] = tag;
    address->length = (uint32_t)(offsetof(struct sockaddr_un, sun_path) + 2);
}

static struct vsr_io_options engine_options(struct engine *e, uint64_t node,
                                            uint32_t handshake)
{
    struct vsr_io_options options;
    struct vsr_io_limits *limits = &options.limits;

    memset(&options, 0, sizeof(options));
    options.executor.ops = &fake_ops;
    options.executor.ctx = e;
    options.node = node;
    options.listen = &e->listen;
    options.listen_count = 1;
    options.handshake = handshake;
    limits->replicas = 2;
    limits->nodes = NODES;
    limits->authorizations = 8;
    limits->links = LINKS;
    limits->link_queue = world.link_queue;
    limits->streams = world.streams;
    limits->stream_window = world.window;
    limits->events = 8;
    limits->ops = world.ops;
    limits->batch = SQ_CAP;
    limits->slabs = SLABS;
    limits->slab_bytes = world.slab_bytes;
    limits->caller_slabs = 0;
    limits->file_slots = FILE_SLOTS;
    limits->buffer_regions = 3;
    options.file_slot_base = FILE_SLOT_BASE;
    options.buffer_region_base = REGION_BASE;
    options.buffer_group = GROUP;
    options.owner = OWNER;
    options.nodelay = 1;
    options.connect_backoff_ns = BACKOFF_NS;
    options.handshake_timeout_ns = HANDSHAKE_NS;
    options.idle_timeout_ns = IDLE_NS;
    options.send_coalesce_bytes = 65536;
    options.zero_copy_bytes = 4096;
    options.stream_chunk_bytes = world.chunk_bytes;
    return options;
}

static struct engine *engine_open_at(uint32_t index, uint64_t node,
                                     uint32_t handshake, char tag)
{
    struct engine *e = &world.engines[index];
    struct vsr_io_options options;
    struct vsr_io_layout layout;
    struct vsr_io_region region;
    struct vsr_io_region pool;

    memset(e, 0, sizeof(*e));
    e->index = index;
    e->node = node;
    address_of(&e->listen, tag);
    options = engine_options(e, node, handshake);
    CHECK(vsr_io_layout(&options, &layout) == VSR_OK);
    CHECK(layout.metadata.size <= sizeof(metadata[index]));
    CHECK(layout.payload.size <= sizeof(payload[index]));
    memset(metadata[index], 0xEE, sizeof(metadata[index]));
    region.base = metadata[index];
    region.size = layout.metadata.size;
    pool.base = payload[index];
    pool.size = layout.payload.size;
    CHECK(vsr_io_init(&options, &region, &pool, &e->io) == VSR_OK);
    e->open = true;
    return e;
}

/* Engine `index` listens at the address tagged 'A' + index. */
static struct engine *engine_open(uint32_t index, uint64_t node,
                                  uint32_t handshake)
{
    return engine_open_at(index, node, handshake, (char)('A' + index));
}

/* The engine's process ends: every socket it holds closes, so the peers
 * read EOF after the bytes already queued; the engine is not deinit'ed
 * (its memory is simply reused by the next open). */
static void engine_forget(struct engine *e)
{
    for (uint32_t i = 0; i < SOCKETS; ++i) {
        struct sock *sock = &world.socks[i];

        if (sock->used && sock->owner == e->index) {
            sock->raw_fd = -1;
            sock->slot = -1;
            sock->recv_armed = false;
            sock->accept_armed = false;
            sock->listening = false;
            sock_drop(i);
        }
    }
    e->open = false;
    e->io = NULL;
}

static void world_reset(uint64_t seed)
{
    memset(&world, 0, sizeof(world));
    world.link_queue = 4;
    world.slab_bytes = PAGE;
    world.streams = STREAMS;
    world.window = WINDOW;
    world.ops = OPS;
    world.chunk_bytes = CHUNK;
    world.inbox_limit = INBOX_BYTES;
    world.now = 1000000000;
    world.next_fd = FD_BASE;
    test_random_seed(&world.random, seed, 1);
}

static void world_advance(uint64_t ns)
{
    world.now += ns;
}

/* Reads the next forwarded op of a kind off the engine's ring, in order,
 * as vsr_io_poll would hand it to the caller; NULL when none. */
static const struct vsr_io_forwarded *forwarded_take(struct engine *e,
                                                     uint32_t kind)
{
    struct vsr_io *io = e->io;
    uint32_t ops = io->options.limits.ops;

    for (uint32_t i = 0; i < io->forwarded_count; ++i) {
        uint32_t at = (io->forwarded_head + i) % ops;
        struct vsr_io_forwarded *entry = &io->forwarded[at];

        if (entry->op.kind != kind) {
            continue;
        }
        /* Dequeue by rotating the entry to the head, then advancing. */
        if (i != 0) {
            struct vsr_io_forwarded copy = *entry;

            for (uint32_t j = i; j > 0; --j) {
                uint32_t from = (io->forwarded_head + j - 1) % ops;
                uint32_t to = (io->forwarded_head + j) % ops;

                io->forwarded[to] = io->forwarded[from];
            }
            io->forwarded[io->forwarded_head] = copy;
        }
        entry = &io->forwarded[io->forwarded_head];
        io->forwarded_head = (io->forwarded_head + 1) % ops;
        io->forwarded_count--;
        io->forwarded_overflow = 0;
        return entry;
    }
    return NULL;
}

static uint32_t forwarded_count(const struct engine *e, uint32_t kind)
{
    const struct vsr_io *io = e->io;
    uint32_t count = 0;

    for (uint32_t i = 0; i < io->forwarded_count; ++i) {
        uint32_t at = (io->forwarded_head + i) % io->options.limits.ops;

        if (io->forwarded[at].op.kind == kind) {
            count++;
        }
    }
    return count;
}

/* One loop iteration, as engine part 2 will run it for the link and
 * stream modules: completions by slot kind, deadlines by kind, the polls,
 * provision, the prepares, execution and delivery. */
static void engine_step(struct engine *e)
{
    struct vsr_io *io = e->io;
    struct vsr_io_cqe cq[CQ_CAP];
    uint32_t cq_count = e->cq_count;
    struct vsr_io_sqe sqes[SQ_CAP];
    struct vsr_io_buffer buffers[SLABS];
    uint32_t count = 0;
    uint16_t kind;
    uint32_t index;
    bool chain_failed = false;

    e->records = 0;
    e->deliveries = 0;
    io->now = world.now;
    memcpy(cq, e->cq, cq_count * sizeof(cq[0]));
    e->cq_count = 0;
    for (uint32_t i = 0; i < cq_count; ++i) {
        uint32_t slot;

        if (vsr_io_slots_resolve(&io->slots, cq[i].user_data, &slot) == NULL) {
            continue;
        }
        if (io->slots.slots[slot].kind == VSR_IO_SLOT_STREAM) {
            vsr_io_streams_complete(io, slot, &cq[i]);
        } else {
            vsr_io_links_complete(io, slot, &cq[i]);
        }
    }
    while (vsr_io_deadlines_pop(&io->deadlines, world.now, &kind, &index)) {
        if (kind == VSR_IO_DEADLINE_STREAM) {
            vsr_io_streams_deadline(io, index, world.now);
        } else {
            CHECK(kind == VSR_IO_DEADLINE_LINK || kind == VSR_IO_DEADLINE_DIAL);
            vsr_io_links_deadline(io, kind, index, world.now);
        }
    }
    vsr_io_links_poll(io, world.now);
    vsr_io_streams_poll(io, world.now);
    count = vsr_io_pool_provide(&io->pool, buffers, SLABS);
    if (count > 0) {
        CHECK(fake_provide(e, GROUP, buffers, count) == 0);
    }
    count = 0;
    vsr_io_links_prepare(io, sqes, SQ_CAP, &count);
    vsr_io_streams_prepare(io, sqes, SQ_CAP, &count);
    sends_resume(e);
    for (uint32_t i = 0; i < count; ++i) {
        execute(e, &sqes[i], &chain_failed);
    }
    deliver(e);
}

/* Releases the i-th held NOTIF (arrival order); the later ones move up. */
static void world_release_notif(uint32_t i)
{
    CHECK(i < world.held_count);
    cq_push(&world.engines[world.held_engine[i]], &world.held[i]);
    for (uint32_t j = i; j + 1 < world.held_count; ++j) {
        world.held[j] = world.held[j + 1];
        world.held_engine[j] = world.held_engine[j + 1];
    }
    world.held_count--;
}

static void world_release_notifs(void)
{
    while (world.held_count > 0) {
        world_release_notif(0);
    }
}

/* -------------------------------------------------------------------------
 * Helpers over the planner's state
 * ---------------------------------------------------------------------- */

static uint32_t links_in_state(const struct engine *e, uint32_t state)
{
    uint32_t count = 0;

    for (uint32_t i = 0; i < LINKS; ++i) {
        if (e->io->links.links[i].state == state) {
            count++;
        }
    }
    return count;
}

static const struct vsr_io_link *link_to(const struct engine *e, uint64_t node,
                                         uint32_t direction, uint32_t state)
{
    for (uint32_t i = 0; i < LINKS; ++i) {
        const struct vsr_io_link *link = &e->io->links.links[i];

        if (link->node == node && link->direction == direction &&
            link->state == state) {
            return link;
        }
    }
    return NULL;
}

/* Two engines, each knowing the other; no peer link is authorized, so the
 * only links are the streams'. */
static void two_engines(uint32_t handshake)
{
    struct engine *a = engine_open(0, 1, handshake);
    struct engine *b = engine_open(1, 2, handshake);

    CHECK(vsr_io_links_node_set(a->io, 2, &b->listen) == VSR_OK);
    CHECK(vsr_io_links_node_set(b->io, 1, &a->listen) == VSR_OK);
    /* Own entries without an address: a node may list itself. */
    CHECK(vsr_io_links_node_set(a->io, 1, NULL) == VSR_OK);
    CHECK(vsr_io_links_node_set(b->io, 2, NULL) == VSR_OK);
}

static uint32_t pool_refs(const struct engine *e)
{
    uint32_t refs = 0;

    for (uint32_t i = 0; i < SLABS; ++i) {
        refs += e->io->pool.entries[i].refs;
    }
    return refs;
}

/* The connection of an established link is reset by the network: both
 * ends read -ECONNRESET and sends fail. */
static void link_reset(struct engine *e, const struct vsr_io_link *link)
{
    uint32_t index = sock_by_slot(e->index, link->fd);
    struct sock *sock;

    CHECK(index != NONE);
    sock = &world.socks[index];
    if (sock->peer != NONE) {
        world.socks[sock->peer].reset = true;
        world.socks[sock->peer].peer = NONE;
    }
    sock->reset = true;
    sock->peer = NONE;
}

/* The stream table's invariants, checked after every step: the window
 * bound, the unit states of each role, the slab references every unit
 * and request holds, the write queue bound. */
static void check_streams(const struct engine *e)
{
    const struct vsr_io_streams *streams = &e->io->streams;
    uint32_t active = 0;

    for (uint32_t i = 0; i < streams->count; ++i) {
        const struct vsr_io_stream *s = &streams->streams[i];
        uint32_t busy = 0;

        if (s->state == VSR_IO_STREAM_FREE) {
            CHECK(s->units_used == 0 && s->writes_count == 0);
            continue;
        }
        active++;
        CHECK(s->units_used <= streams->window);
        CHECK(s->writes_count <= streams->window);
        if (s->request_slab != NONE) {
            CHECK(e->io->pool.entries[s->request_slab].refs > 0);
        }
        for (uint32_t n = 0; n < s->units_used; ++n) {
            const struct vsr_io_stream_unit *u =
                &s->units[(s->units_head + n) % streams->window];

            if (u->state == VSR_IO_UNIT_FREE) {
                CHECK(n > 0); /* The head is never a hole. */
                continue;
            }
            busy++;
            if (s->direction == VSR_IO_OUTBOUND) {
                CHECK(u->state == VSR_IO_UNIT_DATA && u->slab != NONE);
            } else {
                CHECK(u->state != VSR_IO_UNIT_DATA);
                CHECK(u->slab != NONE ||
                      s->writes[u->write].kind == VSR_IO_WRITE_BUFFERS);
            }
            if (u->slab != NONE) {
                CHECK(e->io->pool.entries[u->slab].refs > 0);
            }
        }
        (void)busy;
    }
    CHECK(active == streams->active);
    for (uint32_t i = 0; i < e->io->links.nodes_count; ++i) {
        /* A link is never closed for its held runs: a stream link whose
         * frame waits pauses its receive instead (decision 99). */
        CHECK(e->io->links.nodes[i].last_error != -ENOBUFS);
    }
}

/* Steps every open engine until a full round moves nothing, checking the
 * stream invariants on the way. */
static bool world_run_checked(uint32_t rounds)
{
    for (uint32_t round = 0; round < rounds; ++round) {
        bool moved = false;

        for (uint32_t i = 0; i < ENGINES; ++i) {
            struct engine *e = &world.engines[i];

            if (!e->open) {
                continue;
            }
            engine_step(e);
            check_streams(e);
            if (e->records > 0 || e->deliveries > 0 || e->cq_count > 0) {
                moved = true;
            }
        }
        for (uint32_t i = 0; i < ENGINES; ++i) {
            if (world.engines[i].open && world.engines[i].cq_count > 0) {
                moved = true;
            }
        }
        if (!moved) {
            return true;
        }
    }
    return false;
}

static void settle(void)
{
    CHECK(world_run_checked(400)); /* The world did not settle. */
}

/* -------------------------------------------------------------------------
 * Test doubles of the snapshot module's stream hooks
 * ---------------------------------------------------------------------- */

struct snapshot_end {
    uint32_t stream;
    uint32_t replica;
    int32_t status;
};

struct snapshot_double {
    int serve_result; /* What vsr_io_snapshots_serve returns. */
    uint32_t serves;  /* Calls to it. */
    uint32_t served_stream;
    struct vsr_io_wire_library_request request; /* The last one. */
    uint32_t data_stream;  /* The requester-side library stream expected... */
    uint32_t data_replica; /* ...and its replica. */
    uint32_t data_calls;
    bool hold_data;    /* Chunks are completed by the test, not at once. */
    uint64_t held[64]; /* Their op ids, in order. */
    uint32_t held_count;
    uint64_t received; /* Bytes received in order. */
    unsigned char bytes[1u << 16];
    uint32_t ends;
    struct snapshot_end end[4]; /* In order of arrival. */
};

static struct snapshot_double snap;

int vsr_io_snapshots_serve(struct vsr_io *io, uint32_t stream,
                           const struct vsr_io_wire_library_request *request)
{
    snap.serves++;
    snap.served_stream = stream;
    snap.request = *request;
    if (snap.serve_result == VSR_OK) {
        io->streams.streams[stream].replica = 7;
    }
    return snap.serve_result;
}

void vsr_io_snapshots_stream_data(struct vsr_io *io, uint32_t replica,
                                  uint32_t stream, uint64_t op, uint64_t offset,
                                  const struct vsr_span *bytes, uint32_t slab)
{
    CHECK(replica == snap.data_replica && stream == snap.data_stream);
    CHECK(offset == snap.received && bytes->size > 0);
    CHECK(vsr_io_pool_contains(&io->pool, bytes->data, bytes->size));
    CHECK(vsr_io_pool_locate(&io->pool, bytes->data) == slab);
    CHECK(snap.received + bytes->size <= sizeof(snap.bytes));
    memcpy(snap.bytes + snap.received, bytes->data, bytes->size);
    snap.received += bytes->size;
    snap.data_calls++;
    if (snap.hold_data) {
        CHECK(snap.held_count < 64);
        snap.held[snap.held_count++] = op;
    } else {
        CHECK(vsr_io_streams_data_done(io, op) == VSR_OK);
    }
}

void vsr_io_snapshots_stream_end(struct vsr_io *io, uint32_t replica,
                                 uint32_t stream, int32_t status)
{
    (void)io;
    CHECK(snap.ends < 4);
    snap.end[snap.ends].stream = stream;
    snap.end[snap.ends].replica = replica;
    snap.end[snap.ends].status = status;
    snap.ends++;
}

/* The status the end hook reported for a stream, or -1 when none. */
static int32_t snap_end_status(uint32_t stream, uint32_t replica)
{
    for (uint32_t i = 0; i < snap.ends; ++i) {
        if (snap.end[i].stream == stream && snap.end[i].replica == replica) {
            return snap.end[i].status;
        }
    }
    return -1;
}

static void snap_release(struct vsr_io *io, uint32_t count)
{
    for (uint32_t i = 0; i < count; ++i) {
        CHECK(snap.held_count > 0);
        CHECK(vsr_io_streams_data_done(io, snap.held[0]) == VSR_OK);
        memmove(snap.held, snap.held + 1,
                (snap.held_count - 1) * sizeof(snap.held[0]));
        snap.held_count--;
    }
}

/* -------------------------------------------------------------------------
 * The two callers: a SINK behind the requester, a FEED behind the source
 * ---------------------------------------------------------------------- */

#define DATA_HELD 64u
#define BYTES_MAX (1u << 16)

struct sink {
    struct engine *e;
    uint64_t cookie;
    unsigned char bytes[BYTES_MAX]; /* Received in order. */
    uint64_t received;
    uint32_t data_ops;
    bool hold; /* DATA ops are completed by the test. */
    struct vsr_io_stream_data held[DATA_HELD];
    uint64_t held_ops[DATA_HELD];
    uint32_t held_count;
    bool ended;
    struct vsr_io_stream_end end;
};

static void sink_init(struct sink *k, struct engine *e, uint64_t cookie)
{
    memset(k, 0, sizeof(*k));
    k->e = e;
    k->cookie = cookie;
}

/* Completes the oldest held DATA op after checking its bytes are still
 * the ones delivered (the slab is pinned until now). */
static void sink_complete_one(struct sink *k)
{
    const struct vsr_io_stream_data *data;

    CHECK(k->held_count > 0);
    data = &k->held[0];
    CHECK(memcmp(k->bytes + data->offset, data->bytes.data, data->bytes.size) ==
          0);
    CHECK(vsr_io_streams_data_done(k->e->io, k->held_ops[0]) == VSR_OK);
    memmove(k->held, k->held + 1, (k->held_count - 1) * sizeof(k->held[0]));
    memmove(k->held_ops, k->held_ops + 1,
            (k->held_count - 1) * sizeof(k->held_ops[0]));
    k->held_count--;
}

static void sink_complete(struct sink *k, uint32_t count)
{
    for (uint32_t i = 0; i < count; ++i) {
        sink_complete_one(k);
    }
}

/* Takes every DATA and END op of the requester's caller off the ring. */
static void sink_drain(struct sink *k)
{
    const struct vsr_io_forwarded *f;

    while ((f = forwarded_take(k->e, VSR_IO_OP_STREAM_DATA)) != NULL) {
        const struct vsr_io_stream_data *data = &f->rail.data;

        CHECK(f->op.replica == NULL && f->op.op.id != 0);
        CHECK(vsr_io_streams_op_kind(f->op.op.id) == VSR_IO_STREAM_OP_DATA);
        CHECK(data->stream == k->cookie);
        CHECK(data->offset == k->received);
        CHECK(data->bytes.size > 0);
        CHECK(vsr_io_pool_contains(&k->e->io->pool, data->bytes.data,
                                   data->bytes.size));
        CHECK(!k->ended);
        CHECK(k->received + data->bytes.size <= BYTES_MAX);
        memcpy(k->bytes + k->received, data->bytes.data, data->bytes.size);
        k->received += data->bytes.size;
        k->data_ops++;
        CHECK(k->held_count < DATA_HELD);
        k->held[k->held_count] = *data;
        k->held_ops[k->held_count] = f->op.op.id;
        k->held_count++;
        if (!k->hold) {
            sink_complete_one(k);
        }
    }
    while ((f = forwarded_take(k->e, VSR_IO_OP_STREAM_END)) != NULL) {
        const struct vsr_io_stream_end *end = &f->rail.end;

        CHECK(end->stream == k->cookie); /* The tests keep roles apart. */
        CHECK(f->op.op.id == 0);
        CHECK(!k->ended);
        CHECK(k->held_count == 0); /* END only after every DATA completed. */
        k->ended = true;
        k->end = *end;
    }
}

struct feed {
    struct engine *e;
    uint64_t handle;
    uint64_t node;
    unsigned char request[VSR_IO_STREAM_REQUEST_BYTES];
    size_t request_size;
    bool served; /* SERVE taken. */
    uint64_t written[DATA_HELD];
    uint32_t written_count;
    uint64_t serve_op;
    bool ended;
    struct vsr_io_stream_end end;
};

static void feed_init(struct feed *d, struct engine *e)
{
    memset(d, 0, sizeof(*d));
    d->e = e;
}

/* Takes the SERVE op; the test completes it. */
static bool feed_take_serve(struct feed *d)
{
    const struct vsr_io_forwarded *f =
        forwarded_take(d->e, VSR_IO_OP_STREAM_SERVE);
    const struct vsr_io_stream_serve *serve;

    if (f == NULL) {
        return false;
    }
    serve = &f->rail.serve;
    CHECK(f->op.replica == NULL);
    CHECK(vsr_io_streams_op_kind(f->op.op.id) == VSR_IO_STREAM_OP_SERVE);
    CHECK(!d->served);
    d->served = true;
    d->serve_op = f->op.op.id;
    d->handle = serve->stream;
    d->node = serve->node;
    d->request_size = serve->request.size;
    CHECK(d->request_size <= sizeof(d->request));
    if (d->request_size > 0) {
        CHECK(vsr_io_pool_contains(&d->e->io->pool, serve->request.data,
                                   serve->request.size));
        memcpy(d->request, serve->request.data, d->request_size);
    }
    return true;
}

static void feed_drain(struct feed *d)
{
    const struct vsr_io_forwarded *f;

    while ((f = forwarded_take(d->e, VSR_IO_OP_STREAM_WRITTEN)) != NULL) {
        const struct vsr_io_stream_written *written = &f->rail.written;

        CHECK(f->op.op.id == 0);
        CHECK(written->stream == d->handle);
        CHECK(!d->ended);
        CHECK(d->written_count < DATA_HELD);
        d->written[d->written_count++] = written->write;
    }
    while ((f = forwarded_take(d->e, VSR_IO_OP_STREAM_END)) != NULL) {
        const struct vsr_io_stream_end *end = &f->rail.end;

        CHECK(f->op.op.id == 0);
        CHECK(end->stream == d->handle);
        CHECK(!d->ended);
        d->ended = true;
        d->end = *end;
    }
}

/* Span arrays for BUFFERS writes, one per write id slot. */
#define SPAN_SETS 16u
#define SPANS_MAX 4097u
static struct vsr_span span_sets[SPAN_SETS][SPANS_MAX];

/* A BUFFERS write of `size` bytes from `bytes`, sliced into spans of at
 * most `span_bytes` (0: one span). */
static int feed_write_buffers(struct feed *d, uint64_t id,
                              const unsigned char *bytes, size_t size,
                              size_t span_bytes)
{
    struct vsr_io_stream_write write;
    struct vsr_span *spans = span_sets[id % SPAN_SETS];
    uint32_t count = 0;

    for (size_t at = 0; at < size; count++) {
        size_t take =
            span_bytes == 0 || span_bytes > size - at ? size - at : span_bytes;

        CHECK(count < SPANS_MAX);
        spans[count].data = bytes + at;
        spans[count].size = take;
        at += take;
    }
    memset(&write, 0, sizeof(write));
    write.stream = d->handle;
    write.write = id;
    write.kind = VSR_IO_WRITE_BUFFERS;
    write.buffers.spans = spans;
    write.buffers.count = count;
    write.buffers.size = size;
    return vsr_io_streams_write(d->e->io, &write, id + 1000);
}

static int feed_write_file(struct feed *d, uint64_t id, uint32_t slot,
                           uint64_t offset, uint64_t length)
{
    struct vsr_io_stream_write write;

    memset(&write, 0, sizeof(write));
    write.stream = d->handle;
    write.write = id;
    write.kind = VSR_IO_WRITE_FILE;
    write.slot = slot;
    write.offset = offset;
    write.length = length;
    return vsr_io_streams_write(d->e->io, &write, 0);
}

/* Runs the world and both callers until nothing moves any more: the sink
 * completes DATA ops only when drained, which frees the window for the
 * frames the link holds. */
static void pump(struct sink *k, struct feed *d)
{
    uint32_t quiet = 0;

    for (uint32_t round = 0; round < 4000 && quiet < 4; ++round) {
        uint32_t data_ops = k->data_ops;
        uint32_t written = d->written_count;
        bool ended = k->ended && d->ended;
        bool settled = world_run_checked(1);

        sink_drain(k);
        feed_drain(d);
        quiet = settled && k->data_ops == data_ops &&
                        d->written_count == written &&
                        (k->ended && d->ended) == ended
                    ? quiet + 1
                    : 0;
    }
    CHECK(quiet == 4); /* The transfer did not settle. */
}

/* Both engines up and linked as nodes 1 and 2, with a request stream from
 * a's caller opened and served at b: the sink and the feed are ready. */
static void open_stream(struct sink *k, struct feed *d, uint64_t cookie,
                        const void *request, size_t request_size,
                        uint32_t *index)
{
    struct vsr_io_stream_open open;
    struct engine *a = &world.engines[0];
    struct engine *b = &world.engines[1];

    sink_init(k, a, cookie);
    feed_init(d, b);
    memset(&open, 0, sizeof(open));
    open.node = 2;
    open.request.data = request;
    open.request.size = request_size;
    CHECK(vsr_io_streams_open(a->io, cookie, &open, cookie + 500,
                              VSR_IO_STREAM_CALLER, index) == VSR_OK);
    CHECK(*index != NONE);
}

static const unsigned char *pattern(uint32_t seed)
{
    static unsigned char bytes[BYTES_MAX];
    struct test_random random;

    test_random_seed(&random, seed, 2);
    for (uint32_t i = 0; i < BYTES_MAX; ++i) {
        bytes[i] = (unsigned char)test_random_next(&random);
    }
    return bytes;
}

static const struct vsr_io_stream *stream_at(const struct engine *e,
                                             uint32_t index)
{
    return &e->io->streams.streams[index];
}

/* -------------------------------------------------------------------------
 * Request, chunks, WRITTEN, END, cookie reuse
 * ---------------------------------------------------------------------- */

static void test_basic(void)
{
    static unsigned char hello[] = "hello world!";
    const unsigned char *bytes = pattern(1);
    struct engine *a;
    struct engine *b;
    struct sink k;
    struct feed d;
    uint32_t index = NONE;
    const struct vsr_io_stream *s;
    const struct vsr_io_stream *t;
    const struct vsr_io_link *in;
    uint64_t handle;

    world_reset(11);
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    settle();
    open_stream(&k, &d, 77, hello, sizeof(hello) - 1, &index);
    s = stream_at(a, index);
    CHECK(s->state == VSR_IO_STREAM_DIALING &&
          s->owner == VSR_IO_STREAM_CALLER);
    CHECK(s->request_lease == 577 && a->io->streams.active == 1);
    /* A second stream with the same cookie is refused while this one
     * lives. */
    {
        struct vsr_io_stream_open open;
        uint32_t other = 0;

        memset(&open, 0, sizeof(open));
        open.node = 2;
        CHECK(vsr_io_streams_open(a->io, 77, &open, 0, VSR_IO_STREAM_CALLER,
                                  &other) == VSR_EINVAL);
        CHECK(other == NONE);
    }
    settle();
    /* Dialed, handshaken, the request delivered: SERVE at b with the
     * bytes pinned in a slab, the inbound link bound to the stream. */
    CHECK(s->state == VSR_IO_STREAM_REQUESTED && s->request_pending == 0);
    CHECK(feed_take_serve(&d));
    CHECK(d.node == 1 && d.request_size == 12);
    CHECK(memcmp(d.request, hello, 12) == 0);
    t = &b->io->streams.streams[(uint32_t)(d.handle & 0xFFFF)];
    CHECK(t->state == VSR_IO_STREAM_SERVING && t->request_slab != NONE);
    CHECK(d.handle == vsr_io_streams_handle(&b->io->streams,
                                            (uint32_t)(d.handle & 0xFFFF)));
    in = link_to(b, 1, VSR_IO_INBOUND, VSR_IO_LINK_ESTABLISHED);
    CHECK(in != NULL && in->stream == (uint32_t)(d.handle & 0xFFFF));
    CHECK(in->purpose == VSR_IO_PURPOSE_STREAM);
    CHECK(pool_refs(b) >= 1);
    /* Writes are refused before the SERVE completes, and a stale or
     * foreign completion is EINVAL. */
    CHECK(feed_write_buffers(&d, 1, bytes, 100, 0) == VSR_EINVAL);
    CHECK(vsr_io_streams_served(b->io, d.serve_op ^ 1, VSR_IO_OK) ==
          VSR_EINVAL);
    CHECK(vsr_io_streams_data_done(b->io, d.serve_op) == VSR_EINVAL);
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_EINVAL);
    CHECK(t->state == VSR_IO_STREAM_OPEN && t->request_slab == NONE);
    settle();
    CHECK(pool_refs(b) == links_in_state(b, VSR_IO_LINK_ESTABLISHED));
    /* A BUFFERS write of two and a half chunks in 7-byte spans: a chunk
     * takes at most VSR_IO_STREAM_CHUNK_VECTORS spans (448 bytes here),
     * so six chunk frames, DATA ops in order, WRITTEN once the sends
     * completed. */
    CHECK(feed_write_buffers(&d, 1, bytes, 2 * CHUNK + CHUNK / 2, 7) == VSR_OK);
    pump(&k, &d);
    CHECK(k.data_ops == 6 && k.received == 2 * CHUNK + CHUNK / 2);
    CHECK(memcmp(k.bytes, bytes, k.received) == 0);
    CHECK(d.written_count == 1 && d.written[0] == 1);
    CHECK(!k.ended && !d.ended);
    /* A zero-length write and a one-byte one, in order. */
    CHECK(feed_write_buffers(&d, 2, bytes, 0, 0) == VSR_OK);
    CHECK(feed_write_buffers(&d, 3, bytes + k.received, 1, 0) == VSR_OK);
    pump(&k, &d);
    CHECK(k.data_ops == 7 && k.received == 2 * CHUNK + CHUNK / 2 + 1);
    CHECK(d.written_count == 3 && d.written[1] == 2 && d.written[2] == 3);
    /* CLOSE: END after the last chunk, on both sides; the handle and the
     * cookie are then free. */
    handle = d.handle;
    CHECK(vsr_io_streams_close(b->io, handle, VSR_IO_OK) == VSR_OK);
    CHECK(feed_write_buffers(&d, 4, bytes, 10, 0) == VSR_EINVAL);
    pump(&k, &d);
    CHECK(k.ended && k.end.status == VSR_IO_OK);
    CHECK(k.end.bytes == k.received && k.end.stream == 77);
    CHECK(d.ended && d.end.status == VSR_IO_OK && d.end.bytes == k.received);
    CHECK(d.end.stream == handle);
    CHECK(vsr_io_streams_close(b->io, handle, VSR_IO_OK) == VSR_EINVAL);
    CHECK(s->state == VSR_IO_STREAM_FREE && t->state == VSR_IO_STREAM_FREE);
    CHECK(a->io->streams.active == 0 && b->io->streams.active == 0);
    CHECK(links_in_state(a, VSR_IO_LINK_FREE) == LINKS);
    CHECK(links_in_state(b, VSR_IO_LINK_FREE) == LINKS);
    CHECK(pool_refs(a) == 0 && pool_refs(b) == 0);
    CHECK(a->io->stats.frames_rejected == 0 &&
          b->io->stats.frames_rejected == 0);
    /* The cookie is reusable: a second, empty stream with a new handle. */
    open_stream(&k, &d, 77, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d) && d.request_size == 0 && d.handle != handle);
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_OK);
    pump(&k, &d);
    CHECK(k.ended && k.end.status == VSR_IO_OK && k.end.bytes == 0);
    CHECK(k.data_ops == 0 && d.ended && d.end.bytes == 0);
    CHECK(a->io->streams.active == 0 && b->io->streams.active == 0);
    CHECK(pool_refs(a) == 0 && pool_refs(b) == 0);
    CHECK(links_in_state(a, VSR_IO_LINK_FREE) == LINKS);
    engine_forget(a);
    engine_forget(b);
}

/* -------------------------------------------------------------------------
 * Refusals of the caller calls, table limits
 * ---------------------------------------------------------------------- */

static void test_errors(void)
{
    static const unsigned char big[VSR_IO_STREAM_REQUEST_BYTES + 1];
    struct engine *a;
    struct engine *b;
    struct vsr_io_stream_open open;
    struct vsr_io_stream_write write;
    struct vsr_io_limits limits;
    struct sink k;
    struct feed d;
    uint32_t index = 0;
    size_t bytes;
    size_t alignment;
    uint64_t magic = VSR_IO_LIBRARY_MAGIC;

    /* Sizing: the op id fields bound the tables. */
    memset(&limits, 0, sizeof(limits));
    limits.streams = 0x10000;
    limits.stream_window = 2;
    CHECK(vsr_io_streams_size(&limits, &bytes, &alignment) == VSR_ELIMIT);
    limits.streams = 2;
    limits.stream_window = 0x10000;
    CHECK(vsr_io_streams_size(&limits, &bytes, &alignment) == VSR_ELIMIT);
    limits.stream_window = 0xFFFF;
    CHECK(vsr_io_streams_size(&limits, &bytes, &alignment) == VSR_OK);
    world_reset(12);
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    CHECK(vsr_io_links_node_set(a->io, 3, NULL) == VSR_OK);
    settle();
    memset(&open, 0, sizeof(open));
    open.node = 2;
    CHECK(vsr_io_streams_open(a->io, 1, NULL, 0, VSR_IO_STREAM_CALLER,
                              &index) == VSR_EINVAL);
    CHECK(index == NONE);
    CHECK(vsr_io_streams_open(a->io, 1, &open, 0, VSR_IO_STREAM_CALLER, NULL) ==
          VSR_EINVAL);
    CHECK(vsr_io_streams_open(a->io, 1, &open, 0, 3, &index) == VSR_EINVAL);
    open.request.data = big;
    open.request.size = sizeof(big);
    CHECK(vsr_io_streams_open(a->io, 1, &open, 0, VSR_IO_STREAM_CALLER,
                              &index) == VSR_EINVAL);
    open.request.data = NULL;
    open.request.size = 1;
    CHECK(vsr_io_streams_open(a->io, 1, &open, 0, VSR_IO_STREAM_CALLER,
                              &index) == VSR_EINVAL);
    /* The library prefix is the engine's. */
    open.request.data = &magic;
    open.request.size = sizeof(magic);
    CHECK(vsr_io_streams_open(a->io, 1, &open, 0, VSR_IO_STREAM_CALLER,
                              &index) == VSR_EINVAL);
    open.request.size = 0;
    /* Unknown, own and caller-dialed nodes. */
    open.node = 9;
    CHECK(vsr_io_streams_open(a->io, 1, &open, 0, VSR_IO_STREAM_CALLER,
                              &index) == VSR_EINVAL);
    open.node = 1;
    CHECK(vsr_io_streams_open(a->io, 1, &open, 0, VSR_IO_STREAM_CALLER,
                              &index) == VSR_EINVAL);
    open.node = 3;
    CHECK(vsr_io_streams_open(a->io, 1, &open, 0, VSR_IO_STREAM_CALLER,
                              &index) == VSR_EINVAL);
    CHECK(a->io->streams.active == 0);
    /* ELIMIT once the stream table is full (STREAMS = 2). */
    open.node = 2;
    CHECK(vsr_io_streams_open(a->io, 1, &open, 0, VSR_IO_STREAM_CALLER,
                              &index) == VSR_OK);
    CHECK(vsr_io_streams_open(a->io, 2, &open, 0, VSR_IO_STREAM_CALLER,
                              &index) == VSR_OK);
    CHECK(vsr_io_streams_open(a->io, 3, &open, 0, VSR_IO_STREAM_CALLER,
                              &index) == VSR_ELIMIT);
    CHECK(a->io->streams.active == 2);
    settle();
    CHECK(forwarded_count(b, VSR_IO_OP_STREAM_SERVE) == 2);
    /* Write and close refusals at the source. */
    feed_init(&d, b);
    CHECK(feed_take_serve(&d));
    CHECK(vsr_io_streams_write(b->io, NULL, 0) == VSR_EINVAL);
    CHECK(vsr_io_streams_close(b->io, d.handle ^ 0x100000000, VSR_IO_OK) ==
          VSR_EINVAL);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_EINVAL);
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    memset(&write, 0, sizeof(write));
    write.stream = d.handle;
    write.kind = 5;
    CHECK(vsr_io_streams_write(b->io, &write, 0) == VSR_EINVAL);
    write.kind = VSR_IO_WRITE_BUFFERS;
    write.buffers.count = 1;
    CHECK(vsr_io_streams_write(b->io, &write, 0) == VSR_EINVAL);
    write.buffers.spans = span_sets[0];
    span_sets[0][0].data = big;
    span_sets[0][0].size = 10;
    write.buffers.size = 11;
    CHECK(vsr_io_streams_write(b->io, &write, 0) == VSR_EINVAL);
    write.stream = d.handle ^ 1;
    write.buffers.size = 10;
    CHECK(vsr_io_streams_write(b->io, &write, 0) == VSR_EINVAL);
    /* The requester's handle is not a source handle. */
    CHECK(vsr_io_streams_write(a->io, &write, 0) == VSR_EINVAL);
    CHECK(vsr_io_streams_served(b->io, 5, VSR_IO_OK) == VSR_EINVAL);
    CHECK(vsr_io_streams_data_done(a->io, VSR_IO_STREAM_OP_DATA | 5) ==
          VSR_EINVAL);
    CHECK(vsr_io_streams_data_done(a->io, 5) == VSR_EINVAL);
    /* AGAIN once stream_window writes wait for their WRITTEN: FILE
     * chunks go zero-copy from their slabs, so held NOTIFs hold them
     * (a small BUFFERS chunk is a kernel copy, released at once). */
    b->files[0].bytes = big;
    b->files[0].size = sizeof(big);
    world.hold_notifs = true;
    CHECK(feed_write_file(&d, 1, FILE_FD_BASE, 0, 10) == VSR_OK);
    CHECK(feed_write_file(&d, 2, FILE_FD_BASE, 0, 10) == VSR_OK);
    CHECK(feed_write_file(&d, 3, FILE_FD_BASE, 0, 10) == VSR_AGAIN);
    settle();
    feed_drain(&d);
    CHECK(d.written_count == 0 && world.held_count > 0);
    world.hold_notifs = false;
    world_release_notifs();
    settle();
    feed_drain(&d);
    CHECK(d.written_count == 2);
    CHECK(feed_write_file(&d, 3, FILE_FD_BASE, 0, 10) == VSR_OK);
    sink_init(&k, a, d.request_size == 0 ? 1 : 2);
    engine_forget(a);
    engine_forget(b);
    /* ELIMIT once no link entry is free (LINKS = 4, 5 streams). */
    world_reset(12);
    world.streams = 5;
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    settle();
    for (uint32_t i = 0; i < LINKS; ++i) {
        CHECK(vsr_io_streams_open(a->io, i, &open, 0, VSR_IO_STREAM_CALLER,
                                  &index) == VSR_OK);
    }
    CHECK(vsr_io_streams_open(a->io, 9, &open, 0, VSR_IO_STREAM_CALLER,
                              &index) == VSR_ELIMIT);
    CHECK(a->io->streams.active == LINKS);
    engine_forget(a);
    engine_forget(&world.engines[1]);
}

/* -------------------------------------------------------------------------
 * The window: DATA ops outstanding at the requester, units at the source
 * ---------------------------------------------------------------------- */

static void test_window(void)
{
    const unsigned char *bytes = pattern(2);
    struct engine *a;
    struct engine *b;
    struct sink k;
    struct feed d;
    uint32_t index = NONE;
    const struct vsr_io_stream *s;
    const struct vsr_io_stream *t;
    const struct vsr_io_link *in;
    uint64_t first;

    world_reset(13);
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    settle();
    open_stream(&k, &d, 1, NULL, 0, &index);
    s = stream_at(a, index);
    settle();
    CHECK(feed_take_serve(&d));
    t = stream_at(b, (uint32_t)(d.handle & 0xFFFF));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    /* Ten chunks with the requester's caller holding its DATA ops: at
     * most WINDOW outstanding, the next frame waits in the link. */
    k.hold = true;
    CHECK(feed_write_buffers(&d, 1, bytes, 10 * CHUNK, 0) == VSR_OK);
    settle();
    sink_drain(&k);
    CHECK(k.data_ops == WINDOW && k.held_count == WINDOW);
    CHECK(s->units_used == WINDOW);
    in = link_to(a, 2, VSR_IO_OUTBOUND, VSR_IO_LINK_ESTABLISHED);
    CHECK(in != NULL && in->retry && in->partial_length > 0);
    /* One completion admits one more chunk; out of order completions
     * free the window only once the older ones are in. */
    sink_complete(&k, 1);
    settle();
    sink_drain(&k);
    CHECK(k.data_ops == WINDOW + 1 && k.held_count == WINDOW);
    while (k.data_ops < 10) {
        sink_complete(&k, 1);
        settle();
        sink_drain(&k);
    }
    CHECK(k.received == 10 * CHUNK && memcmp(k.bytes, bytes, k.received) == 0);
    feed_drain(&d);
    CHECK(d.written_count == 1);
    /* END waits for the last DATA completion. */
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_OK);
    pump(&k, &d);
    CHECK(!k.ended && d.ended && d.end.status == VSR_IO_OK);
    CHECK(s->state == VSR_IO_STREAM_ENDING && s->units_used == WINDOW);
    CHECK(t->state == VSR_IO_STREAM_FREE);
    sink_complete(&k, 1);
    settle();
    sink_drain(&k);
    CHECK(!k.ended);
    sink_complete(&k, 1);
    settle();
    sink_drain(&k);
    CHECK(k.ended && k.end.status == VSR_IO_OK && k.end.bytes == 10 * CHUNK);
    CHECK(a->io->streams.active == 0 && pool_refs(a) == 0);
    /* The source's window: with NOTIFs held, at most WINDOW chunk sends
     * are in flight (FILE chunks, zero-copy from their slabs), the rest
     * of the write waits. */
    b->files[0].bytes = bytes;
    b->files[0].size = BYTES_MAX;
    open_stream(&k, &d, 2, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    t = stream_at(b, (uint32_t)(d.handle & 0xFFFF));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    world.hold_notifs = true;
    CHECK(feed_write_file(&d, 1, FILE_FD_BASE, 0, 10 * CHUNK) == VSR_OK);
    settle();
    sink_drain(&k);
    CHECK(k.data_ops == WINDOW && t->units_used == WINDOW);
    CHECK(t->writes_count == 1 && t->writes[t->writes_head].units == WINDOW);
    world_release_notif(0);
    settle();
    sink_drain(&k);
    /* One NOTIF may cover both chunks when their frames coalesced. */
    CHECK(k.data_ops > WINDOW && k.data_ops <= 2 * WINDOW);
    CHECK(t->units_used == WINDOW);
    world.hold_notifs = false;
    world_release_notifs();
    pump(&k, &d);
    CHECK(k.data_ops == 10 && d.written_count == 1);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_OK);
    pump(&k, &d);
    CHECK(k.ended && d.ended && k.end.bytes == 10 * CHUNK);
    CHECK(pool_refs(a) == 0 && pool_refs(b) == 0);
    /* DATA op ids name the chunk, not its ring slot: the chunk that
     * reuses a completed op's slot gets an id of its own, so a repeated
     * completion of the old id is EINVAL and never frees the live unit
     * (decision B2). */
    open_stream(&k, &d, 3, NULL, 0, &index);
    s = stream_at(a, index);
    settle();
    CHECK(feed_take_serve(&d));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    k.hold = true;
    CHECK(feed_write_buffers(&d, 1, bytes, 6 * CHUNK, 0) == VSR_OK);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_OK);
    settle();
    sink_drain(&k);
    CHECK(k.held_count == WINDOW);
    first = k.held_ops[0];
    sink_complete(&k, 1);
    settle();
    sink_drain(&k);
    CHECK(k.held_count == WINDOW); /* The next chunk took the slot... */
    CHECK(k.held_ops[1] != first); /* ...under an id of its own. */
    CHECK(vsr_io_streams_data_done(a->io, first) == VSR_EINVAL);
    CHECK(s->units_used == WINDOW && k.held_count == WINDOW);
    for (uint32_t n = 0; n < WINDOW; ++n) {
        CHECK(s->units[(s->units_head + n) % WINDOW].state == VSR_IO_UNIT_DATA);
    }
    while (!k.ended) {
        CHECK(k.held_count > 0);
        sink_complete(&k, 1);
        settle();
        sink_drain(&k);
    }
    CHECK(k.end.status == VSR_IO_OK && k.received == 6 * CHUNK);
    CHECK(memcmp(k.bytes, bytes, k.received) == 0);
    pump(&k, &d);
    CHECK(d.ended && d.end.status == VSR_IO_OK && d.written_count == 1);
    CHECK(pool_refs(a) == 0 && pool_refs(b) == 0);
    CHECK(a->io->streams.active == 0 && b->io->streams.active == 0);
    engine_forget(a);
    engine_forget(b);
}

/* -------------------------------------------------------------------------
 * FILE writes: chunking, short reads, EOF and read errors, mixed writes
 * ---------------------------------------------------------------------- */

/* One transfer of a file range as a single FILE write, closed OK: the
 * requester receives exactly the range. Returns the READ records issued
 * by the source. */
static uint32_t file_transfer(uint64_t cookie, const unsigned char *file,
                              size_t file_size, uint64_t offset,
                              uint64_t length)
{
    struct engine *a = &world.engines[0];
    struct engine *b = &world.engines[1];
    struct sink k;
    struct feed d;
    uint32_t index = NONE;

    b->files[0].bytes = file;
    b->files[0].size = file_size;
    b->reads = 0;
    open_stream(&k, &d, cookie, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    CHECK(feed_write_file(&d, 1, FILE_FD_BASE, offset, length) == VSR_OK);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_OK);
    pump(&k, &d);
    if (!(k.ended && k.end.status == VSR_IO_OK && k.end.bytes == length)) {
        const struct vsr_io_stream *s = stream_at(a, index);

        fprintf(stderr,
                "file_transfer: length %" PRIu64
                " ended %d status %d bytes %" PRIu64 " received %" PRIu64
                " ops %u reads %u; a state %u units %u"
                " gone %u; b active %u d.ended %d\n",
                length, k.ended, k.end.status, k.end.bytes, k.received,
                k.data_ops, b->reads, s->state, s->units_used, s->link_gone,
                b->io->streams.active, d.ended);
    }
    CHECK(k.ended && k.end.status == VSR_IO_OK && k.end.bytes == length);
    CHECK(k.received == length);
    CHECK(memcmp(k.bytes, file + offset, (size_t)length) == 0);
    CHECK(d.ended && d.end.status == VSR_IO_OK && d.end.bytes == length);
    CHECK(d.written_count == 1 && d.written[0] == 1);
    CHECK(a->io->streams.active == 0 && b->io->streams.active == 0);
    CHECK(pool_refs(a) == 0 && pool_refs(b) == 0);
    CHECK(links_in_state(a, VSR_IO_LINK_FREE) == LINKS);
    CHECK(links_in_state(b, VSR_IO_LINK_FREE) == LINKS);
    return b->reads;
}

/* A requester whose caller falls behind by more than the link's held-run
 * bound: the link pauses its receive while the stream's window is full,
 * the source's sends back up in the socket (the TCP window closes) and
 * every chunk arrives once the caller catches up; no link is closed with
 * -ENOBUFS and the source's status matches the requester's (decision 99). */
static void test_backpressure(void)
{
    const unsigned char *bytes = pattern(5);
    const uint64_t length = 60 * CHUNK; /* Fifteen slabs of chunk frames. */
    struct engine *a;
    struct engine *b;
    struct sink k;
    struct feed d;
    uint32_t index = NONE;
    const struct vsr_io_stream *s;
    const struct vsr_io_stream *t;
    const struct vsr_io_link *in;
    uint32_t pauses = 0;
    uint32_t slots_free;

    world_reset(17);
    world.inbox_limit = 4 * PAGE; /* A socket buffer of four slabs. */
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    settle();
    /* Steady drip: the engines alternate, the caller completes one DATA
     * op at a time, the source is closed right after its write. */
    open_stream(&k, &d, 1, NULL, 0, &index);
    s = stream_at(a, index);
    settle();
    CHECK(feed_take_serve(&d));
    t = stream_at(b, (uint32_t)(d.handle & 0xFFFF));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    k.hold = true;
    CHECK(feed_write_buffers(&d, 1, bytes, length, 0) == VSR_OK);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_OK);
    settle();
    sink_drain(&k);
    feed_drain(&d);
    CHECK(k.data_ops == WINDOW && k.held_count == WINDOW);
    CHECK(s->units_used == WINDOW);
    /* The window is full: the link holds the next frame, its receive is
     * off (no RECV slot) and the source's window is full too: its send
     * is parked in the socket, so the source's stream cannot end. */
    in = link_to(a, 2, VSR_IO_OUTBOUND, VSR_IO_LINK_ESTABLISHED);
    CHECK(in != NULL && in->retry && in->partial_length > 0);
    CHECK(in->recv_slot == NONE);
    CHECK(t->units_used == WINDOW && !d.ended);
    while (!k.ended) {
        CHECK(k.held_count > 0); /* Else nothing would ever move. */
        sink_complete(&k, 1);
        settle();
        sink_drain(&k);
        feed_drain(&d);
        in = link_to(a, 2, VSR_IO_OUTBOUND, VSR_IO_LINK_ESTABLISHED);
        if (in != NULL && in->recv_slot == NONE) {
            pauses++;
        }
    }
    CHECK(pauses > 0);
    CHECK(k.end.status == VSR_IO_OK && k.end.bytes == length);
    CHECK(k.received == length && memcmp(k.bytes, bytes, length) == 0);
    CHECK(d.ended && d.end.status == VSR_IO_OK && d.end.bytes == length);
    CHECK(d.written_count == 1 && d.written[0] == 1);
    settle();
    CHECK(a->io->streams.active == 0 && b->io->streams.active == 0);
    CHECK(pool_refs(a) == 0 && pool_refs(b) == 0);
    CHECK(links_in_state(a, VSR_IO_LINK_FREE) == LINKS);
    CHECK(links_in_state(b, VSR_IO_LINK_FREE) == LINKS);
    /* Burst, with a window of one: the source runs ahead (FILE chunks,
     * zero-copy) while the requester's engine does not step, filling the
     * socket buffer; what the socket holds is delivered in one burst,
     * then the caller catches up through the same drip. */
    world_reset(18);
    world.window = 1;
    world.inbox_limit = 4 * PAGE;
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    settle();
    b->files[0].bytes = bytes;
    b->files[0].size = BYTES_MAX;
    open_stream(&k, &d, 2, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    k.hold = true;
    CHECK(feed_write_file(&d, 1, FILE_FD_BASE, 0, length) == VSR_OK);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_OK);
    for (uint32_t i = 0; i < 200; ++i) {
        engine_step(b);
        check_streams(b);
        feed_drain(&d);
    }
    CHECK(!d.ended); /* Its send is parked: the socket buffer is full. */
    while (!k.ended) {
        settle();
        sink_drain(&k);
        feed_drain(&d);
        if (!k.ended) {
            CHECK(k.held_count > 0);
            sink_complete(&k, 1);
        }
    }
    CHECK(k.end.status == VSR_IO_OK && k.end.bytes == length);
    CHECK(k.received == length && memcmp(k.bytes, bytes, length) == 0);
    CHECK(d.ended && d.end.status == VSR_IO_OK && d.written_count == 1);
    settle();
    CHECK(a->io->streams.active == 0 && b->io->streams.active == 0);
    CHECK(pool_refs(a) == 0 && pool_refs(b) == 0);
    CHECK(links_in_state(a, VSR_IO_LINK_FREE) == LINKS);
    CHECK(links_in_state(b, VSR_IO_LINK_FREE) == LINKS);
    /* The pause's CANCEL still out (its result held) when the link
     * closes: the teardown waits for it before taking the shutdown slot,
     * then the link frees with every slot returned. */
    world_reset(19);
    world.inbox_limit = 4 * PAGE;
    world.hold_cancels = true;
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    settle();
    slots_free = a->io->slots.free_count; /* The listener keeps one. */
    open_stream(&k, &d, 3, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    k.hold = true;
    CHECK(feed_write_buffers(&d, 1, bytes, length, 0) == VSR_OK);
    settle();
    sink_drain(&k);
    CHECK(k.data_ops == WINDOW);
    in = link_to(a, 2, VSR_IO_OUTBOUND, VSR_IO_LINK_ESTABLISHED);
    CHECK(in != NULL && in->recv_slot == NONE && in->shutdown_slot != NONE);
    CHECK(world.held_count == 1); /* The CANCEL's own result. */
    vsr_io_streams_shutdown(a->io);
    sink_complete(&k, k.held_count);
    settle();
    sink_drain(&k);
    CHECK(k.ended && k.end.status == VSR_IO_CANCELLED);
    CHECK(links_in_state(a, VSR_IO_LINK_CLOSING) == 1);
    CHECK(in->state == VSR_IO_LINK_CLOSING && !in->torn_down);
    CHECK(in->shutdown_slot != NONE && world.held_count == 1);
    world.hold_cancels = false;
    world_release_notifs();
    settle();
    feed_drain(&d);
    CHECK(links_in_state(a, VSR_IO_LINK_FREE) == LINKS);
    CHECK(a->io->slots.free_count == slots_free);
    CHECK(a->io->streams.active == 0 && pool_refs(a) == 0);
    vsr_io_streams_shutdown(b->io);
    pump(&k, &d);
    CHECK(d.ended && b->io->streams.active == 0 && pool_refs(b) == 0);
}

static void test_file(void)
{
    const unsigned char *file = pattern(3);
    struct engine *a;
    struct engine *b;
    struct sink k;
    struct feed d;
    uint32_t index = NONE;
    uint64_t cookie = 1;
    static const uint64_t sizes[] = {0, 1, WINDOW * CHUNK, 20 * CHUNK + 123};

    world_reset(14);
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    settle();
    /* 0, 1, exact-window and many chunks: one read per chunk. */
    for (size_t i = 0; i < sizeof(sizes) / sizeof(sizes[0]); ++i) {
        uint64_t size = sizes[i];

        CHECK(file_transfer(cookie++, file, BYTES_MAX, 0, size) ==
              (size + CHUNK - 1) / CHUNK);
    }
    /* A range inside the file. */
    CHECK(file_transfer(cookie++, file, BYTES_MAX, 500, 2500) == 3);
    /* Short reads resume the chunk: 300 bytes at a time takes four reads
     * per full chunk. */
    world.short_read = 300;
    CHECK(file_transfer(cookie++, file, BYTES_MAX, 0, 3 * CHUNK + 100) ==
          3 * 4 + 1);
    world.short_read = 0;
    world.random_reads = true;
    CHECK(file_transfer(cookie++, file, BYTES_MAX, 7, 5 * CHUNK + 1) >= 6);
    world.random_reads = false;
    /* The file ends inside the range: END with FAILED after the chunks
     * that were read, the link closed by the source. */
    b->files[0].bytes = file;
    b->files[0].size = 2 * CHUNK + 10;
    open_stream(&k, &d, cookie++, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    CHECK(feed_write_file(&d, 1, FILE_FD_BASE, 0, 4 * CHUNK) == VSR_OK);
    CHECK(feed_write_file(&d, 2, FILE_FD_BASE, 0, CHUNK) == VSR_OK);
    pump(&k, &d);
    CHECK(k.ended && k.end.status == VSR_IO_FAILED);
    CHECK(k.received == 2 * CHUNK && k.end.bytes == k.received);
    CHECK(memcmp(k.bytes, file, k.received) == 0);
    CHECK(d.ended && d.end.status == VSR_IO_FAILED && d.end.bytes == 2 * CHUNK);
    CHECK(d.written_count == 2 && d.written[0] == 1 && d.written[1] == 2);
    CHECK(a->io->streams.active == 0 && b->io->streams.active == 0);
    CHECK(pool_refs(a) == 0 && pool_refs(b) == 0);
    /* A read error. */
    b->files[0].size = BYTES_MAX;
    world.fail_read = 1;
    open_stream(&k, &d, cookie++, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    CHECK(feed_write_file(&d, 1, FILE_FD_BASE, 0, 3 * CHUNK) == VSR_OK);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_OK);
    pump(&k, &d);
    CHECK(k.ended && k.end.status == VSR_IO_FAILED && k.received == 0);
    CHECK(d.ended && d.end.status == VSR_IO_FAILED && d.written_count == 1);
    CHECK(world.fail_read == 0 && pool_refs(b) == 0);
    /* FILE and BUFFERS writes interleave in order, chunks pipelined. */
    open_stream(&k, &d, cookie++, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    CHECK(feed_write_file(&d, 1, FILE_FD_BASE, 0, 3 * CHUNK) == VSR_OK);
    CHECK(feed_write_buffers(&d, 2, file + 3 * CHUNK, CHUNK + CHUNK / 2, 13) ==
          VSR_OK);
    settle();
    CHECK(feed_write_file(&d, 3, FILE_FD_BASE, 4 * CHUNK + CHUNK / 2,
                          2 * CHUNK) == VSR_OK);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_OK);
    pump(&k, &d);
    CHECK(k.ended && k.end.status == VSR_IO_OK);
    CHECK(k.received == 6 * CHUNK + CHUNK / 2);
    CHECK(memcmp(k.bytes, file, k.received) == 0);
    CHECK(d.written_count == 3 && d.written[0] == 1 && d.written[2] == 3);
    CHECK(pool_refs(a) == 0 && pool_refs(b) == 0);
    engine_forget(a);
    engine_forget(b);
}

/* -------------------------------------------------------------------------
 * Link loss on either side, refusal, protocol errors
 * ---------------------------------------------------------------------- */

static void test_loss(void)
{
    const unsigned char *bytes = pattern(4);
    struct engine *a;
    struct engine *b;
    struct sink k;
    struct feed d;
    uint32_t index = NONE;
    const struct vsr_io_stream *s;
    const struct vsr_io_stream *t;
    const struct vsr_io_link *link;

    world_reset(15);
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    settle();
    /* Reset mid-transfer with DATA ops held: the source reports WRITTEN
     * for the write (its bytes are no longer read) and END RETRY; the
     * requester's END RETRY follows its last completion. */
    open_stream(&k, &d, 1, NULL, 0, &index);
    s = stream_at(a, index);
    settle();
    CHECK(feed_take_serve(&d));
    t = stream_at(b, (uint32_t)(d.handle & 0xFFFF));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    k.hold = true;
    CHECK(feed_write_buffers(&d, 1, bytes, 10 * CHUNK, 0) == VSR_OK);
    settle();
    sink_drain(&k);
    CHECK(k.data_ops == WINDOW);
    link = link_to(a, 2, VSR_IO_OUTBOUND, VSR_IO_LINK_ESTABLISHED);
    CHECK(link != NULL);
    link_reset(a, link);
    pump(&k, &d);
    /* The requester, paused on its full window (decision 99), has no receive to
     * read the reset with: its stream stays REQUESTED with the frames the
     * link holds until the caller frees the window. The source ends. */
    CHECK(!k.ended && s->state == VSR_IO_STREAM_REQUESTED && !s->link_gone);
    CHECK(link->state == VSR_IO_LINK_ESTABLISHED && link->recv_slot == NONE);
    CHECK(d.written_count == 1 && d.ended && d.end.status == VSR_IO_RETRY);
    CHECK(d.end.bytes >= k.received && d.end.bytes <= 10 * CHUNK);
    CHECK(t->state == VSR_IO_STREAM_FREE && pool_refs(b) == 0);
    /* The lost stream still refuses writes; close is EINVAL now that its
     * END op went out. */
    CHECK(feed_write_buffers(&d, 2, bytes, 10, 0) == VSR_EINVAL);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_EINVAL);
    /* Each completion admits a held frame; once the link holds nothing
     * the receive resumes, reads the reset, and END RETRY follows the
     * last completion: the chunks the kernel had delivered, no more. */
    sink_complete(&k, WINDOW);
    settle();
    sink_drain(&k);
    CHECK(!k.ended && k.held_count > 0); /* The link held more chunks. */
    while (!k.ended) {
        CHECK(k.held_count > 0);
        sink_complete(&k, k.held_count);
        settle();
        sink_drain(&k);
    }
    CHECK(k.end.status == VSR_IO_RETRY);
    CHECK(k.end.bytes == k.received && k.received > WINDOW * CHUNK);
    CHECK(k.received < 10 * CHUNK); /* The reset discarded the rest. */
    CHECK(memcmp(k.bytes, bytes, k.received) == 0);
    CHECK(a->io->streams.active == 0 && pool_refs(a) == 0);
    CHECK(links_in_state(a, VSR_IO_LINK_FREE) == LINKS);
    CHECK(links_in_state(b, VSR_IO_LINK_FREE) == LINKS);
    /* Loss while the SERVE is at the caller: the source's END op (RETRY)
     * follows the caller's OK; a refusal then reports nothing. */
    open_stream(&k, &d, 2, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    t = stream_at(b, (uint32_t)(d.handle & 0xFFFF));
    link = link_to(b, 1, VSR_IO_INBOUND, VSR_IO_LINK_ESTABLISHED);
    CHECK(link != NULL);
    link_reset(b, link);
    settle();
    sink_drain(&k);
    CHECK(k.ended && k.end.status == VSR_IO_RETRY && k.end.bytes == 0);
    CHECK(t->state == VSR_IO_STREAM_ENDING && t->request_slab != NONE);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_EINVAL);
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    CHECK(t->request_slab == NONE);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_OK);
    settle();
    feed_drain(&d);
    CHECK(d.ended && d.end.status == VSR_IO_RETRY && d.end.bytes == 0);
    CHECK(t->state == VSR_IO_STREAM_FREE && pool_refs(b) == 0);
    open_stream(&k, &d, 3, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    t = stream_at(b, (uint32_t)(d.handle & 0xFFFF));
    link_reset(b, link_to(b, 1, VSR_IO_INBOUND, VSR_IO_LINK_ESTABLISHED));
    settle();
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_FAILED) == VSR_OK);
    settle();
    feed_drain(&d);
    sink_drain(&k);
    CHECK(!d.ended && t->state == VSR_IO_STREAM_FREE);
    CHECK(k.ended && k.end.status == VSR_IO_RETRY);
    /* A dial that fails: END RETRY at once, nothing at the source. */
    engine_forget(b);
    open_stream(&k, &d, 4, NULL, 0, &index);
    settle();
    sink_drain(&k);
    CHECK(k.ended && k.end.status == VSR_IO_RETRY && k.end.bytes == 0);
    CHECK(a->io->streams.active == 0);
    CHECK(links_in_state(a, VSR_IO_LINK_FREE) == LINKS);
    engine_forget(a);
    /* CLOSE, then loss before the END frame's NOTIF: the source's END was
     * accepted by the kernel, so it reports the close status. */
    world_reset(16);
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    settle();
    b->files[0].bytes = bytes;
    b->files[0].size = BYTES_MAX;
    open_stream(&k, &d, 5, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    t = stream_at(b, (uint32_t)(d.handle & 0xFFFF));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    world.hold_notifs = true;
    CHECK(feed_write_file(&d, 1, FILE_FD_BASE, 0, CHUNK) == VSR_OK);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_OK);
    settle();
    CHECK(t->end_sent && t->state == VSR_IO_STREAM_ENDING);
    /* The requester closes its link at the END frame, so the source's is
     * already lost; a reset does the same when it is not. */
    link = link_to(b, 1, VSR_IO_INBOUND, VSR_IO_LINK_ESTABLISHED);
    if (link != NULL) {
        link_reset(b, link);
    }
    settle();
    feed_drain(&d);
    CHECK(!d.ended && t->link_gone);
    world.hold_notifs = false;
    world_release_notifs();
    pump(&k, &d);
    CHECK(d.ended && d.end.status == VSR_IO_OK && d.written_count == 1);
    CHECK(pool_refs(b) == 0 && links_in_state(b, VSR_IO_LINK_FREE) == LINKS);
    CHECK(k.ended); /* OK or RETRY, as the reset raced the END frame. */
    CHECK(a->io->streams.active == 0 && pool_refs(a) == 0);
    /* Refusal: END RETRY at the requester, the link closed, no END op at
     * the source. */
    open_stream(&k, &d, 6, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    t = stream_at(b, (uint32_t)(d.handle & 0xFFFF));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_FAILED) == VSR_OK);
    pump(&k, &d);
    CHECK(k.ended && k.end.status == VSR_IO_RETRY && k.end.bytes == 0);
    CHECK(!d.ended && t->state == VSR_IO_STREAM_FREE);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_EINVAL);
    CHECK(links_in_state(a, VSR_IO_LINK_FREE) == LINKS);
    CHECK(links_in_state(b, VSR_IO_LINK_FREE) == LINKS);
    CHECK(pool_refs(a) == 0 && pool_refs(b) == 0);
    engine_forget(a);
    engine_forget(b);
}

/* Sends a raw stream frame on a link, as a misbehaving peer would. */
static void send_raw(struct engine *e, const struct vsr_io_link *link,
                     uint16_t kind, const unsigned char *header,
                     size_t header_bytes)
{
    uint64_t end = 0;
    uint32_t crc = vsr_io_crc32c(0, header, header_bytes);

    CHECK(vsr_io_links_send_frame(e->io, (uint32_t)(link - e->io->links.links),
                                  kind, header, header_bytes, NULL, 0, crc,
                                  &end) == VSR_OK);
}

static void test_protocol(void)
{
    const unsigned char *bytes = pattern(5);
    struct engine *a;
    struct engine *b;
    struct sink k;
    struct feed d;
    uint32_t index = NONE;
    unsigned char header[16];
    uint64_t rejected;

    world_reset(17);
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    settle();
    /* A chunk at the wrong offset: FAILED at the requester, the link
     * closed with -EPROTO; the source sees a loss. */
    open_stream(&k, &d, 1, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    CHECK(feed_write_buffers(&d, 1, bytes, CHUNK, 0) == VSR_OK);
    settle();
    sink_drain(&k);
    CHECK(k.data_ops == 1);
    rejected = a->io->stats.frames_rejected;
    vsr_io_codec_put_stream_chunk(header, 5, 0);
    send_raw(b, link_to(b, 1, VSR_IO_INBOUND, VSR_IO_LINK_ESTABLISHED),
             VSR_IO_FRAME_STREAM_CHUNK, header, 16);
    pump(&k, &d);
    CHECK(k.ended && k.end.status == VSR_IO_FAILED && k.end.bytes == CHUNK);
    CHECK(a->io->stats.frames_rejected == rejected + 1);
    CHECK(d.ended && d.end.status == VSR_IO_RETRY && d.written_count == 1);
    CHECK(links_in_state(a, VSR_IO_LINK_FREE) == LINKS);
    /* END announcing more bytes than delivered: FAILED. */
    open_stream(&k, &d, 2, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    vsr_io_codec_put_stream_end(header, 999, VSR_IO_OK);
    send_raw(b, link_to(b, 1, VSR_IO_INBOUND, VSR_IO_LINK_ESTABLISHED),
             VSR_IO_FRAME_STREAM_END, header, 16);
    pump(&k, &d);
    CHECK(k.ended && k.end.status == VSR_IO_FAILED && k.end.bytes == 0);
    CHECK(d.ended && d.end.status == VSR_IO_RETRY);
    /* A chunk from the requester, and a second request: the source
     * closes with -EPROTO and reports FAILED; the requester a loss. */
    open_stream(&k, &d, 3, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    rejected = b->io->stats.frames_rejected;
    vsr_io_codec_put_stream_chunk(header, 0, 0);
    send_raw(a, link_to(a, 2, VSR_IO_OUTBOUND, VSR_IO_LINK_ESTABLISHED),
             VSR_IO_FRAME_STREAM_CHUNK, header, 16);
    pump(&k, &d);
    CHECK(b->io->stats.frames_rejected == rejected + 1);
    CHECK(d.ended && d.end.status == VSR_IO_FAILED);
    CHECK(k.ended && k.end.status == VSR_IO_RETRY);
    open_stream(&k, &d, 4, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    vsr_io_codec_put_stream_request(header, 0);
    send_raw(a, link_to(a, 2, VSR_IO_OUTBOUND, VSR_IO_LINK_ESTABLISHED),
             VSR_IO_FRAME_STREAM_REQUEST, header, 8);
    settle();
    sink_drain(&k);
    CHECK(b->io->stats.frames_rejected == rejected + 2);
    CHECK(k.ended && k.end.status == VSR_IO_RETRY);
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    settle();
    feed_drain(&d);
    CHECK(d.ended && d.end.status == VSR_IO_FAILED);
    /* A request at the requester. */
    open_stream(&k, &d, 5, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    send_raw(b, link_to(b, 1, VSR_IO_INBOUND, VSR_IO_LINK_ESTABLISHED),
             VSR_IO_FRAME_STREAM_REQUEST, header, 8);
    pump(&k, &d);
    CHECK(k.ended && k.end.status == VSR_IO_FAILED);
    CHECK(d.ended && d.end.status == VSR_IO_RETRY);
    CHECK(a->io->streams.active == 0 && b->io->streams.active == 0);
    CHECK(pool_refs(a) == 0 && pool_refs(b) == 0);
    CHECK(links_in_state(a, VSR_IO_LINK_FREE) == LINKS);
    CHECK(links_in_state(b, VSR_IO_LINK_FREE) == LINKS);
    engine_forget(a);
    engine_forget(b);
}

/* -------------------------------------------------------------------------
 * Library streams: served by the engine, requested by the engine
 * ---------------------------------------------------------------------- */

static void test_library(void)
{
    const unsigned char *file = pattern(6);
    struct engine *a;
    struct engine *b;
    struct sink k;
    struct feed d;
    struct vsr_io_wire_library_request request;
    struct vsr_io_stream_open open;
    unsigned char wire[56];
    uint32_t index = NONE;
    const struct vsr_io_stream *s;
    const struct vsr_io_stream *t;

    world_reset(18);
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    settle();
    memset(&request, 0, sizeof(request));
    request.magic = VSR_IO_LIBRARY_MAGIC;
    request.version = VSR_IO_LIBRARY_REQUEST_VERSION;
    request.kind = VSR_IO_LIBRARY_CLIENTS;
    request.cluster_hi = 11;
    request.cluster_lo = 12;
    request.replica = 2;
    request.snapshot_hi = 13;
    request.snapshot_lo = 14;
    vsr_io_codec_put_library_request(wire, &request);
    b->files[0].bytes = file;
    b->files[0].size = BYTES_MAX;
    memset(&open, 0, sizeof(open));
    open.node = 2;
    open.request.data = wire;
    open.request.size = sizeof(wire);
    /* A library request from a caller is refused at open. */
    CHECK(vsr_io_streams_open(a->io, 1, &open, 0, VSR_IO_STREAM_CALLER,
                              &index) == VSR_EINVAL);
    /* The engine's own request, served by the engine: the request goes
     * to the snapshot hook at the source, never to a caller; the chunks
     * and both ends reach the hooks at the requester and the source. The
     * test then feeds the file as the snapshot module would. */
    memset(&snap, 0, sizeof(snap));
    snap.serve_result = VSR_OK;
    sink_init(&k, a, 1);
    feed_init(&d, b);
    CHECK(vsr_io_streams_open(a->io, 1, &open, 0, VSR_IO_STREAM_LIBRARY,
                              &index) == VSR_OK);
    s = stream_at(a, index);
    a->io->streams.streams[index].replica = 3;
    snap.data_stream = index;
    snap.data_replica = 3;
    settle();
    CHECK(snap.serves == 1 && forwarded_count(b, VSR_IO_OP_STREAM_SERVE) == 0);
    CHECK(memcmp(&snap.request, &request, sizeof(request)) == 0);
    t = stream_at(b, snap.served_stream);
    CHECK(t->state == VSR_IO_STREAM_OPEN && t->owner == VSR_IO_STREAM_LIBRARY);
    CHECK(t->replica == 7 && t->request_slab == NONE && pool_refs(b) == 1);
    d.handle = vsr_io_streams_handle(&b->io->streams, snap.served_stream);
    CHECK(feed_write_file(&d, 1, FILE_FD_BASE, 0, 3 * CHUNK + 7) == VSR_OK);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_OK);
    pump(&k, &d);
    CHECK(k.data_ops == 0 && !k.ended); /* No ops for the library. */
    CHECK(!d.ended && d.written_count == 0);
    CHECK(snap.data_calls == 4 && snap.received == 3 * CHUNK + 7);
    CHECK(memcmp(snap.bytes, file, snap.received) == 0);
    CHECK(snap.ends == 2);
    CHECK(snap_end_status(index, 3) == VSR_IO_OK);
    CHECK(snap_end_status(snap.served_stream, 7) == VSR_IO_OK);
    CHECK(s->state == VSR_IO_STREAM_FREE && t->state == VSR_IO_STREAM_FREE);
    CHECK(pool_refs(a) == 0 && pool_refs(b) == 0);
    /* Refused by the snapshot module: the end carries its status. */
    memset(&snap, 0, sizeof(snap));
    snap.serve_result = VSR_IO_NOT_FOUND;
    CHECK(vsr_io_streams_open(a->io, 1, &open, 0, VSR_IO_STREAM_LIBRARY,
                              &index) == VSR_OK);
    a->io->streams.streams[index].replica = 3;
    snap.data_stream = index;
    snap.data_replica = 3;
    settle();
    CHECK(snap.serves == 1 && snap.ends == 1 && snap.data_calls == 0);
    CHECK(snap.end[0].stream == index && snap.end[0].replica == 3);
    CHECK(snap.end[0].status == VSR_IO_NOT_FOUND);
    CHECK(a->io->streams.active == 0 && b->io->streams.active == 0);
    CHECK(pool_refs(a) == 0 && pool_refs(b) == 0);
    /* A malformed library request closes the link. */
    memset(&snap, 0, sizeof(snap));
    wire[8] ^= 1; /* version */
    CHECK(vsr_io_streams_open(a->io, 1, &open, 0, VSR_IO_STREAM_LIBRARY,
                              &index) == VSR_OK);
    a->io->streams.streams[index].replica = 3;
    snap.data_stream = index;
    snap.data_replica = 3;
    settle();
    CHECK(snap.serves == 0 && b->io->stats.frames_rejected == 1);
    CHECK(snap.ends == 1 && snap.end[0].status == VSR_IO_RETRY);
    CHECK(a->io->streams.active == 0 && b->io->streams.active == 0);
    wire[8] ^= 1;
    /* Requested by the engine from a caller's source: chunks and the end
     * reach the hooks, no op is forwarded at the requester; held chunks
     * delay the end. */
    memset(&snap, 0, sizeof(snap));
    snap.hold_data = true;
    feed_init(&d, b);
    open.request.data = file;
    open.request.size = 40; /* Not the library prefix: a caller serves. */
    CHECK(vsr_io_streams_open(a->io, 9, &open, 0, VSR_IO_STREAM_LIBRARY,
                              &index) == VSR_OK);
    a->io->streams.streams[index].replica = 3;
    snap.data_stream = index;
    snap.data_replica = 3;
    settle();
    CHECK(feed_take_serve(&d) && d.request_size == 40);
    CHECK(memcmp(d.request, file, 40) == 0);
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    CHECK(feed_write_buffers(&d, 1, file, 3 * CHUNK, 0) == VSR_OK);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_OK);
    settle();
    feed_drain(&d);
    CHECK(snap.data_calls == WINDOW && snap.held_count == WINDOW);
    CHECK(forwarded_count(a, VSR_IO_OP_STREAM_DATA) == 0);
    snap_release(a->io, WINDOW);
    settle();
    feed_drain(&d);
    CHECK(snap.data_calls == 3 && snap.received == 3 * CHUNK && snap.ends == 0);
    CHECK(d.ended && d.end.status == VSR_IO_OK && d.written_count == 1);
    snap_release(a->io, 1);
    settle();
    CHECK(snap.ends == 1 && snap.end[0].status == VSR_IO_OK);
    CHECK(snap.end[0].stream == index && snap.end[0].replica == 3);
    CHECK(memcmp(snap.bytes, file, snap.received) == 0);
    CHECK(forwarded_count(a, VSR_IO_OP_STREAM_END) == 0);
    CHECK(a->io->streams.active == 0 && pool_refs(a) == 0);
    CHECK(links_in_state(a, VSR_IO_LINK_FREE) == LINKS);
    CHECK(links_in_state(b, VSR_IO_LINK_FREE) == LINKS);
    memset(&snap, 0, sizeof(snap));
    engine_forget(a);
    engine_forget(b);
}

/* -------------------------------------------------------------------------
 * Timeouts and shutdown
 * ---------------------------------------------------------------------- */

static void test_timeout(void)
{
    const unsigned char *bytes = pattern(7);
    struct engine *a;
    struct engine *b;
    struct sink k;
    struct feed d;
    uint32_t index = NONE;
    const struct vsr_io_stream *t;

    world_reset(19);
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    settle();
    /* A SERVE nobody answers: both sides end with RETRY once the
     * inactivity timeout passes; the late OK gets the END op. */
    open_stream(&k, &d, 1, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    t = stream_at(b, (uint32_t)(d.handle & 0xFFFF));
    world_advance(HANDSHAKE_NS / 2);
    settle();
    sink_drain(&k);
    CHECK(!k.ended);
    world_advance(HANDSHAKE_NS / 2 + 1);
    pump(&k, &d);
    CHECK(k.ended && k.end.status == VSR_IO_RETRY);
    CHECK(!d.ended && t->state == VSR_IO_STREAM_ENDING && t->link_gone);
    CHECK(links_in_state(b, VSR_IO_LINK_FREE) == LINKS);
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    settle();
    feed_drain(&d);
    CHECK(d.ended && d.end.status == VSR_IO_RETRY);
    CHECK(a->io->streams.active == 0 && b->io->streams.active == 0);
    /* Progress keeps a stream alive past the timeout. */
    open_stream(&k, &d, 2, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    for (uint32_t i = 0; i < 6; ++i) {
        world_advance(HANDSHAKE_NS / 2);
        settle();
        CHECK(feed_write_buffers(&d, i, bytes + i * CHUNK, CHUNK, 0) == VSR_OK);
        settle();
        sink_drain(&k);
        CHECK(k.data_ops == i + 1 && !k.ended);
    }
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_OK);
    pump(&k, &d);
    CHECK(k.ended && k.end.status == VSR_IO_OK && k.received == 6 * CHUNK);
    CHECK(d.ended && d.written_count == 6);
    /* The requester's clock covers the dial: a listener that never
     * answers (its ACCEPT is never armed) times out through the link. */
    /* A source whose chunk NOTIFs never come: the timeout closes the
     * link, the END op (RETRY) waits for the NOTIFs, the write's WRITTEN
     * comes with it. */
    b->files[0].bytes = bytes;
    b->files[0].size = BYTES_MAX;
    open_stream(&k, &d, 3, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    t = stream_at(b, (uint32_t)(d.handle & 0xFFFF));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    world.hold_notifs = true;
    CHECK(feed_write_file(&d, 1, FILE_FD_BASE, 0, CHUNK) == VSR_OK);
    settle();
    sink_drain(&k);
    CHECK(k.data_ops == 1 && !t->link_gone && t->units_used == 1);
    world_advance(HANDSHAKE_NS + 1);
    settle();
    feed_drain(&d);
    sink_drain(&k);
    CHECK(t->link_gone && !d.ended && t->units_used == 1);
    CHECK(k.ended && k.end.status == VSR_IO_RETRY && k.received == CHUNK);
    world.hold_notifs = false;
    world_release_notifs();
    pump(&k, &d);
    CHECK(d.ended && d.end.status == VSR_IO_RETRY && d.written_count == 1);
    CHECK(d.end.bytes == CHUNK);
    CHECK(a->io->streams.active == 0 && b->io->streams.active == 0);
    CHECK(pool_refs(a) == 0 && pool_refs(b) == 0);
    CHECK(links_in_state(a, VSR_IO_LINK_FREE) == LINKS);
    CHECK(links_in_state(b, VSR_IO_LINK_FREE) == LINKS);
    engine_forget(a);
    engine_forget(b);
}

static void test_shutdown(void)
{
    const unsigned char *bytes = pattern(8);
    struct engine *a;
    struct engine *b;
    struct sink k;
    struct feed d;
    struct vsr_io_stream_open open;
    uint32_t index = NONE;

    world_reset(20);
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    settle();
    open_stream(&k, &d, 1, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    k.hold = true;
    CHECK(feed_write_buffers(&d, 1, bytes, 5 * CHUNK, 0) == VSR_OK);
    settle();
    sink_drain(&k);
    CHECK(k.data_ops == WINDOW);
    vsr_io_streams_shutdown(a->io);
    vsr_io_streams_shutdown(b->io);
    memset(&open, 0, sizeof(open));
    open.node = 2;
    CHECK(vsr_io_streams_open(a->io, 2, &open, 0, VSR_IO_STREAM_CALLER,
                              &index) == VSR_EINVAL);
    pump(&k, &d);
    CHECK(!k.ended && d.ended && d.end.status == VSR_IO_CANCELLED);
    CHECK(d.written_count == 1 && b->io->streams.active == 0);
    sink_complete(&k, WINDOW);
    settle();
    sink_drain(&k);
    CHECK(k.ended && k.end.status == VSR_IO_CANCELLED);
    CHECK(k.end.bytes == WINDOW * CHUNK);
    CHECK(a->io->streams.active == 0 && pool_refs(a) == 0 && pool_refs(b) == 0);
    CHECK(links_in_state(a, VSR_IO_LINK_FREE) == LINKS);
    CHECK(links_in_state(b, VSR_IO_LINK_FREE) == LINKS);
    engine_forget(a);
    engine_forget(b);
}

/* -------------------------------------------------------------------------
 * Random walk: transfers of random writes under slicing, holds and loss
 * ---------------------------------------------------------------------- */

#define WALK_ROUNDS 40u
#define WALK_WRITES 5u

struct walk_write {
    uint32_t kind;
    uint64_t offset; /* FILE: in the file. */
    uint64_t length;
    size_t span_bytes;
};

static void test_random(uint64_t seed)
{
    const unsigned char *file = pattern(9);
    struct engine *a;
    struct engine *b;
    struct test_random random;
    static unsigned char expected[BYTES_MAX];
    uint32_t completed = 0;
    uint32_t partial = 0;
    uint32_t refused = 0;

    printf("stream walk seed %" PRIu64 "\n", seed);
    world_reset(seed);
    test_random_seed(&random, seed, 3);
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    settle();
    b->files[0].bytes = file;
    b->files[0].size = BYTES_MAX;
    for (uint32_t round = 0; round < WALK_ROUNDS; ++round) {
        struct sink k;
        struct feed d;
        struct walk_write writes[WALK_WRITES];
        uint32_t count = 1 + test_random_bounded(&random, WALK_WRITES);
        uint32_t fed = 0;
        uint64_t total = 0;
        uint32_t index = NONE;
        bool refuse = test_random_bounded(&random, 8) == 0;
        bool lose = !refuse && test_random_bounded(&random, 3) == 0;
        uint32_t lose_at = test_random_bounded(&random, 60);
        bool closed = false;
        bool stopped = false;
        bool jumped = false; /* The clock advanced: timers may have fired. */
        uint32_t steps = 0;
        uint32_t loser = test_random_bounded(&random, 2);

        world.slice_max = test_random_bounded(&random, 2) == 0
                              ? 0
                              : 1 + test_random_bounded(&random, 3000);
        world.short_read = test_random_bounded(&random, 3) == 0
                               ? 1 + test_random_bounded(&random, CHUNK)
                               : 0;
        world.random_reads = test_random_bounded(&random, 3) == 0;
        world.hold_notifs = test_random_bounded(&random, 4) == 0;
        for (uint32_t i = 0; i < count; ++i) {
            struct walk_write *w = &writes[i];

            w->kind = test_random_bounded(&random, 2) == 0
                          ? VSR_IO_WRITE_FILE
                          : VSR_IO_WRITE_BUFFERS;
            w->length = test_random_bounded(&random, 4) == 0
                            ? test_random_bounded(&random, 3)
                            : test_random_bounded(&random, 4 * CHUNK + 1);
            w->offset = test_random_bounded(&random, 2000);
            w->span_bytes = test_random_bounded(&random, 2) == 0
                                ? 0
                                : 1 + test_random_bounded(&random, CHUNK);
            CHECK(total + w->length <= BYTES_MAX);
            memcpy(expected + total, file + w->offset, (size_t)w->length);
            total += w->length;
        }
        open_stream(&k, &d, round + 1, file, round % 100, &index);
        k.hold = test_random_bounded(&random, 2) == 0;
        settle();
        CHECK(feed_take_serve(&d));
        CHECK(d.request_size == round % 100);
        CHECK(vsr_io_streams_served(b->io, d.serve_op,
                                    refuse ? VSR_IO_FAILED : VSR_IO_OK) ==
              VSR_OK);
        while (!(k.ended && (d.ended || refuse)) && steps < 4000) {
            uint32_t action = test_random_bounded(&random, 8);

            steps++;
            if (!refuse && fed < count && !stopped && action < 3) {
                const struct walk_write *w = &writes[fed];
                int r =
                    w->kind == VSR_IO_WRITE_FILE
                        ? feed_write_file(&d, fed, FILE_FD_BASE, w->offset,
                                          w->length)
                        : feed_write_buffers(&d, fed, file + w->offset,
                                             (size_t)w->length, w->span_bytes);

                if (r == VSR_OK) {
                    fed++;
                } else {
                    CHECK(r == VSR_AGAIN || r == VSR_EINVAL);
                    CHECK(r == VSR_AGAIN || d.ended ||
                          stream_at(b, (uint32_t)(d.handle & 0xFFFF))->state ==
                              VSR_IO_STREAM_ENDING);
                    if (r == VSR_EINVAL) {
                        stopped = true; /* Lost: nothing more to feed. */
                    }
                }
            } else if (!refuse && (fed == count || stopped) && !closed &&
                       action < 4) {
                int r = vsr_io_streams_close(b->io, d.handle, VSR_IO_OK);

                CHECK(r == VSR_OK || r == VSR_EINVAL);
                closed = true;
            } else if (action == 4 && k.held_count > 0) {
                sink_complete(&k,
                              1 + test_random_bounded(&random, k.held_count));
            } else if (action == 5 && world.held_count > 0) {
                world_release_notif(
                    test_random_bounded(&random, world.held_count));
            } else if (action == 6 && world.hold_notifs &&
                       test_random_bounded(&random, 4) == 0) {
                world.hold_notifs = false;
                world_release_notifs();
            } else if (lose && steps == lose_at) {
                struct engine *e = loser == 0 ? a : b;
                const struct vsr_io_link *link =
                    loser == 0 ? link_to(a, 2, VSR_IO_OUTBOUND,
                                         VSR_IO_LINK_ESTABLISHED)
                               : link_to(b, 1, VSR_IO_INBOUND,
                                         VSR_IO_LINK_ESTABLISHED);

                if (link != NULL) {
                    link_reset(e, link);
                }
            } else if (action == 7) {
                world_advance(1 + test_random_bounded(
                                      &random, (uint32_t)HANDSHAKE_NS / 8));
                jumped = true;
            }
            (void)world_run_checked(1 + test_random_bounded(&random, 3));
            sink_drain(&k);
            feed_drain(&d);
            if (k.ended && k.held_count > 0) {
                CHECK(false); /* END before every DATA completed. */
            }
            if (steps == 3000) {
                /* Wind down: complete everything, release everything. */
                k.hold = false;
                world.hold_notifs = false;
                world_release_notifs();
                sink_complete(&k, k.held_count);
            }
        }
        if (!(k.ended && (d.ended || refuse))) {
            const struct vsr_io_stream *sa = stream_at(a, index);
            const struct vsr_io_stream *sb =
                stream_at(b, (uint32_t)(d.handle & 0xFFFF));

            fprintf(stderr,
                    "round %u stuck: k.ended %d d.ended %d fed %u/%u closed %d"
                    " stopped %d lose %d\n a: state %u units %u gone %u"
                    " ended %u link %u\n b: state %u units %u writes %u"
                    " gone %u end_due %u end_sent %u ended %u serve %" PRIu64
                    " link %u aborted %u held notifs %u\n",
                    round, k.ended, d.ended, fed, count, closed, stopped, lose,
                    sa->state, sa->units_used, sa->link_gone, sa->ended,
                    sa->link, sb->state, sb->units_used, sb->writes_count,
                    sb->link_gone, sb->end_due, sb->end_sent, sb->ended,
                    sb->serve_op, sb->link, sb->aborted, world.held_count);
            for (uint32_t n = 0; n < sb->units_used; ++n) {
                const struct vsr_io_stream_unit *u =
                    &sb->units[(sb->units_head + n) % WINDOW];

                fprintf(stderr,
                        " b unit %u state %u slab %u slot %u length"
                        " %u filled %u write %u send_end %" PRIu64 "\n",
                        n, u->state, u->slab, u->slot, u->length, u->filled,
                        u->write, u->send_end);
            }
            if (sb->link != NONE) {
                const struct vsr_io_link *l = &b->io->links.links[sb->link];

                fprintf(stderr,
                        " b link state %u stream %u inflight %u"
                        " notified %" PRIu64 " sent %" PRIu64 " offset %" PRIu64
                        " send_end %" PRIu64 "\n",
                        l->state, l->stream, l->inflight, l->notified_offset,
                        l->sent_offset, l->stream_offset, sb->send_end);
            }
        }
        CHECK(k.ended && (d.ended || refuse));
        /* Every byte arrived exactly once, in order: a prefix of the
         * expected stream, the whole of it when the end is OK. */
        CHECK(k.received <= total);
        CHECK(memcmp(k.bytes, expected, (size_t)k.received) == 0);
        CHECK(k.end.bytes == k.received);
        if (k.end.status == VSR_IO_OK) {
            CHECK(k.received == total && d.end.status == VSR_IO_OK);
            CHECK(d.end.bytes == total);
            completed++;
        } else {
            /* A refusal, a reset, or a timer (a requester stalled past
             * the inactivity timeout, or a source whose linger ended
             * first: OK there); never slowness alone, since the link's
             * receive pauses instead of running out of held runs. */
            CHECK(k.end.status == VSR_IO_RETRY);
            CHECK(refuse || lose || d.end.status != VSR_IO_OK || jumped);
            (void)(refuse ? refused++ : partial++);
        }
        if (!refuse) {
            /* Every queued write's lease was released by a WRITTEN. */
            CHECK(d.written_count == fed);
            for (uint32_t i = 0; i < fed; ++i) {
                CHECK(d.written[i] == i);
            }
            CHECK(d.end.bytes >= k.received);
        }
        world.hold_notifs = false;
        world_release_notifs();
        settle();
        sink_drain(&k);
        feed_drain(&d);
        CHECK(a->io->streams.active == 0 && b->io->streams.active == 0);
        CHECK(pool_refs(a) == 0 && pool_refs(b) == 0);
        CHECK(links_in_state(a, VSR_IO_LINK_FREE) == LINKS);
        CHECK(links_in_state(b, VSR_IO_LINK_FREE) == LINKS);
        CHECK(a->io->slots.free_count == a->io->slots.count - 1);
        CHECK(b->io->slots.free_count == b->io->slots.count - 1);
    }
    printf("stream walk: %u complete, %u partial, %u refused\n", completed,
           partial, refused);
    CHECK(completed >= WALK_ROUNDS / 4);
    engine_forget(a);
    engine_forget(b);
}

/* -------------------------------------------------------------------------
 * Review: shutdown order, the requester's clock, close after a failure
 * ---------------------------------------------------------------------- */

/* vsr_io_close shuts the links down before the streams (implementation
 * section 7.7): a stream whose link the link module's shutdown closes still
 * ends CANCELLED on the closing engine (vsr-io.h: CANCELLED when the engine
 * closes), not RETRY as for a loss; the peer, which sees a loss, reports
 * RETRY. */
static void test_review_shutdown_order(void)
{
    const unsigned char *bytes = pattern(21);
    struct engine *a;
    struct engine *b;
    struct sink k;
    struct feed d;
    uint32_t index = NONE;

    world_reset(21);
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    settle();
    /* Both engines close mid-transfer, DATA ops held at the requester. */
    open_stream(&k, &d, 1, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    k.hold = true;
    CHECK(feed_write_buffers(&d, 1, bytes, 5 * CHUNK, 0) == VSR_OK);
    settle();
    sink_drain(&k);
    CHECK(k.data_ops == WINDOW);
    vsr_io_links_shutdown(a->io);
    vsr_io_streams_shutdown(a->io);
    vsr_io_links_shutdown(b->io);
    vsr_io_streams_shutdown(b->io);
    pump(&k, &d);
    CHECK(d.ended && d.end.status == VSR_IO_CANCELLED && d.written_count == 1);
    sink_complete(&k, k.held_count);
    settle();
    sink_drain(&k);
    CHECK(k.ended && k.end.status == VSR_IO_CANCELLED);
    CHECK(a->io->streams.active == 0 && b->io->streams.active == 0);
    CHECK(pool_refs(a) == 0 && pool_refs(b) == 0);
    engine_forget(a);
    engine_forget(b);
    /* Only the source's engine closes: CANCELLED there, RETRY at the
     * requester, which sees the connection go. */
    world_reset(22);
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    settle();
    open_stream(&k, &d, 2, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    CHECK(feed_write_buffers(&d, 1, bytes, CHUNK, 0) == VSR_OK);
    pump(&k, &d);
    CHECK(k.data_ops == 1 && !k.ended && !d.ended);
    vsr_io_links_shutdown(b->io);
    vsr_io_streams_shutdown(b->io);
    pump(&k, &d);
    CHECK(d.ended && d.end.status == VSR_IO_CANCELLED && d.written_count == 1);
    CHECK(k.ended && k.end.status == VSR_IO_RETRY && k.end.bytes == CHUNK);
    CHECK(a->io->streams.active == 0 && b->io->streams.active == 0);
    CHECK(pool_refs(a) == 0 && pool_refs(b) == 0);
    engine_forget(a);
    engine_forget(b);
}

/* A DATA completion is a caller call, which re-arms the inactivity timer
 * (decision 97): a caller that frees its window just before the deadline
 * keeps the stream, even when the poll after its completion comes after
 * the deadline (the poll drains deadlines before the links retry the
 * held frame, whose delivery would re-arm it). */
static void test_review_data_done_rearms(void)
{
    const unsigned char *bytes = pattern(23);
    struct engine *a;
    struct engine *b;
    struct sink k;
    struct feed d;
    uint32_t index = NONE;
    const struct vsr_io_stream *s;

    world_reset(23);
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    settle();
    open_stream(&k, &d, 1, NULL, 0, &index);
    s = stream_at(a, index);
    settle();
    CHECK(feed_take_serve(&d));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    k.hold = true;
    CHECK(feed_write_buffers(&d, 1, bytes, 4 * CHUNK, 0) == VSR_OK);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_OK);
    settle();
    sink_drain(&k);
    feed_drain(&d);
    /* The source sent everything and lingers; the requester's window is
     * full, its link holds the rest. */
    CHECK(k.held_count == WINDOW && s->state == VSR_IO_STREAM_REQUESTED);
    CHECK(d.ended && d.end.status == VSR_IO_OK);
    world_advance(HANDSHAKE_NS - 1);
    settle();
    sink_drain(&k);
    CHECK(!k.ended && s->state == VSR_IO_STREAM_REQUESTED);
    sink_complete(&k, 1);
    world_advance(2);
    settle();
    sink_drain(&k);
    /* The freed unit took the next chunk; the END frame is still held, so
     * the requester has not closed: the source's linger ended at its own
     * timer (decision 96 bounds it), its END op long out. */
    CHECK(!k.ended && k.held_count == WINDOW && k.received == 3 * CHUNK);
    CHECK(s->state == VSR_IO_STREAM_REQUESTED);
    CHECK(b->io->streams.active == 0);
    k.hold = false;
    sink_complete(&k, k.held_count);
    pump(&k, &d);
    CHECK(k.ended && k.end.status == VSR_IO_OK && k.received == 4 * CHUNK);
    CHECK(memcmp(k.bytes, bytes, k.received) == 0);
    CHECK(a->io->streams.active == 0 && b->io->streams.active == 0);
    CHECK(pool_refs(a) == 0 && pool_refs(b) == 0);
    engine_forget(a);
    engine_forget(b);
}

/* STREAM_CLOSE racing an end the engine decided under the caller (a file
 * read failed after the caller's last write) is OK like after a loss: the
 * caller cannot know until the END op, which follows with the failure's
 * status. A second close, and a close with a status that is not an enum
 * vsr_io_status (which the peer's decoder would refuse as a malformed END,
 * failing the transfer as a protocol error), are EINVAL. */
static void test_review_close(void)
{
    const unsigned char *bytes = pattern(24);
    struct engine *a;
    struct engine *b;
    struct sink k;
    struct feed d;
    uint32_t index = NONE;
    const struct vsr_io_stream *t;

    world_reset(24);
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    settle();
    b->files[0].bytes = bytes;
    b->files[0].size = BYTES_MAX;
    open_stream(&k, &d, 1, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    t = stream_at(b, (uint32_t)(d.handle & 0xFFFF));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    CHECK(vsr_io_streams_close(b->io, d.handle, -1) == VSR_EINVAL);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_CANCELLED + 1) ==
          VSR_EINVAL);
    CHECK(t->state == VSR_IO_STREAM_OPEN);
    world.fail_read = 1;
    CHECK(feed_write_file(&d, 1, FILE_FD_BASE, 0, CHUNK) == VSR_OK);
    for (uint32_t i = 0; i < 8 && t->state != VSR_IO_STREAM_ENDING; ++i) {
        engine_step(b);
    }
    CHECK(t->state == VSR_IO_STREAM_ENDING && t->end_due && !t->ended);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_OK);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_EINVAL);
    pump(&k, &d);
    CHECK(d.ended && d.end.status == VSR_IO_FAILED && d.written_count == 1);
    CHECK(k.ended && k.end.status == VSR_IO_FAILED && k.received == 0);
    CHECK(a->io->stats.frames_rejected == 0);
    /* A close decided by the caller stays the only one: EINVAL after it,
     * and after a refusal. */
    open_stream(&k, &d, 2, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_RETRY) == VSR_OK);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_EINVAL);
    pump(&k, &d);
    CHECK(k.ended && k.end.status == VSR_IO_RETRY);
    CHECK(d.ended && d.end.status == VSR_IO_RETRY);
    open_stream(&k, &d, 3, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_FAILED) == VSR_OK);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_EINVAL);
    pump(&k, &d);
    CHECK(k.ended && k.end.status == VSR_IO_RETRY && !d.ended);
    CHECK(a->io->streams.active == 0 && b->io->streams.active == 0);
    CHECK(pool_refs(a) == 0 && pool_refs(b) == 0);
    engine_forget(a);
    engine_forget(b);
}

/* A chunk the requester takes re-arms its clock even while the caller
 * holds every DATA op (decision 97: every frame). */
static void test_review_chunk_rearms(void)
{
    const unsigned char *bytes = pattern(26);
    struct engine *a;
    struct engine *b;
    struct sink k;
    struct feed d;
    uint32_t index = NONE;
    const struct vsr_io_stream *s;

    world_reset(26);
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    settle();
    open_stream(&k, &d, 1, NULL, 0, &index);
    s = stream_at(a, index);
    settle();
    CHECK(feed_take_serve(&d));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    k.hold = true;
    world_advance(HANDSHAKE_NS / 2);
    CHECK(feed_write_buffers(&d, 1, bytes, CHUNK, 0) == VSR_OK);
    settle();
    sink_drain(&k);
    CHECK(k.held_count == 1);
    /* Past the request's deadline, within the first chunk's. */
    world_advance(HANDSHAKE_NS / 2 + HANDSHAKE_NS / 4);
    CHECK(feed_write_buffers(&d, 2, bytes + CHUNK, CHUNK, 0) == VSR_OK);
    settle();
    sink_drain(&k);
    CHECK(!k.ended && k.held_count == 2 && s->state == VSR_IO_STREAM_REQUESTED);
    /* Past the first chunk's deadline, within the second's. */
    world_advance(HANDSHAKE_NS / 2);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_OK);
    settle();
    sink_drain(&k);
    CHECK(!k.ended && s->state == VSR_IO_STREAM_ENDING);
    /* After the END frame the requester waits for its caller untimed. */
    world_advance(2 * HANDSHAKE_NS);
    settle();
    sink_drain(&k);
    CHECK(!k.ended && s->state == VSR_IO_STREAM_ENDING && k.held_count == 2);
    k.hold = false;
    sink_complete(&k, k.held_count);
    pump(&k, &d);
    CHECK(k.ended && k.end.status == VSR_IO_OK && k.received == 2 * CHUNK);
    CHECK(d.ended && d.end.status == VSR_IO_OK && d.written_count == 2);
    CHECK(a->io->streams.active == 0 && b->io->streams.active == 0);
    CHECK(pool_refs(a) == 0 && pool_refs(b) == 0);
    engine_forget(a);
    engine_forget(b);
}

/* A FILE read still in flight when the source aborts (a shutdown, a read
 * failure of an earlier chunk) is dropped at its completion: its bytes are
 * never sent, the stream ends, its slab and slot are released. */
static void test_review_read_in_flight(void)
{
    const unsigned char *bytes = pattern(27);
    struct engine *a;
    struct engine *b;
    struct sink k;
    struct feed d;
    uint32_t index = NONE;
    const struct vsr_io_stream *t;

    world_reset(27);
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    settle();
    b->files[0].bytes = bytes;
    b->files[0].size = BYTES_MAX;
    /* Shutdown between the READs' issue and their completions. */
    open_stream(&k, &d, 1, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    t = stream_at(b, (uint32_t)(d.handle & 0xFFFF));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    CHECK(feed_write_file(&d, 1, FILE_FD_BASE, 0, 3 * CHUNK) == VSR_OK);
    engine_step(b);
    CHECK(t->units_used == WINDOW && b->cq_count == WINDOW);
    for (uint32_t n = 0; n < WINDOW; ++n) {
        CHECK(t->units[n].state == VSR_IO_UNIT_READING &&
              t->units[n].slot != NONE);
    }
    vsr_io_streams_shutdown(b->io);
    CHECK(t->units_used == WINDOW); /* The reads complete first. */
    pump(&k, &d);
    CHECK(d.ended && d.end.status == VSR_IO_CANCELLED && d.end.bytes == 0);
    CHECK(d.written_count == 1 && b->io->streams.active == 0);
    CHECK(k.ended && k.end.status == VSR_IO_RETRY && k.received == 0);
    CHECK(pool_refs(a) == 0 && pool_refs(b) == 0);
    CHECK(b->io->slots.free_count == b->io->slots.count - 1);
    engine_forget(a);
    engine_forget(b);
    /* The first read fails while the second is in flight: the second's
     * bytes are dropped, END(FAILED) announces none, and the requester
     * sees no chunk (one at offset CHUNK would be a protocol error). */
    world_reset(28);
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    settle();
    b->files[0].bytes = bytes;
    b->files[0].size = BYTES_MAX;
    open_stream(&k, &d, 1, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    world.fail_read = 1;
    CHECK(feed_write_file(&d, 1, FILE_FD_BASE, 0, 3 * CHUNK) == VSR_OK);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_OK);
    pump(&k, &d);
    CHECK(world.fail_read == 0);
    CHECK(k.ended && k.end.status == VSR_IO_FAILED && k.received == 0);
    CHECK(a->io->stats.frames_rejected == 0);
    CHECK(d.ended && d.end.status == VSR_IO_FAILED && d.end.bytes == 0);
    CHECK(d.written_count == 1);
    /* A failed read after sent chunks, with no chunk behind it: the END
     * counts the sent chunks only. */
    open_stream(&k, &d, 2, NULL, 0, &index);
    settle();
    CHECK(feed_take_serve(&d));
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    CHECK(feed_write_file(&d, 1, FILE_FD_BASE, 0, CHUNK) == VSR_OK);
    pump(&k, &d);
    CHECK(k.received == CHUNK && d.written_count == 1);
    world.fail_read = 1;
    CHECK(feed_write_file(&d, 2, FILE_FD_BASE, CHUNK, CHUNK) == VSR_OK);
    pump(&k, &d);
    CHECK(world.fail_read == 0);
    CHECK(k.ended && k.end.status == VSR_IO_FAILED && k.received == CHUNK);
    CHECK(d.ended && d.end.status == VSR_IO_FAILED && d.end.bytes == CHUNK);
    CHECK(d.written_count == 2 && a->io->stats.frames_rejected == 0);
    CHECK(a->io->streams.active == 0 && b->io->streams.active == 0);
    CHECK(pool_refs(a) == 0 && pool_refs(b) == 0);
    engine_forget(a);
    engine_forget(b);
}

/* A forwarded ring of one entry: every op the module emits finds it full
 * at times. A request waits in its link for SERVE room, a chunk for DATA
 * room (the link pauses), and WRITTEN and END ops are retried at the next
 * poll; none is lost or duplicated. */
static void test_review_full_ring(void)
{
    const unsigned char *bytes = pattern(29);
    struct engine *a;
    struct engine *b;
    struct sink k;
    struct sink k2;
    struct feed d;
    struct feed d2;
    struct vsr_io_stream_open open;
    uint32_t index = NONE;
    uint32_t second = NONE;
    const struct vsr_io_stream *s;
    const struct vsr_io_stream *t;

    world_reset(29);
    world.ops = 1;
    world.window = 4; /* Four writes queue. */
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    settle();
    /* Two requests: the second SERVE waits for the first to be taken. */
    open_stream(&k, &d, 1, NULL, 0, &index);
    s = stream_at(a, index);
    sink_init(&k2, a, 2);
    feed_init(&d2, b);
    memset(&open, 0, sizeof(open));
    open.node = 2;
    CHECK(vsr_io_streams_open(a->io, 2, &open, 0, VSR_IO_STREAM_CALLER,
                              &second) == VSR_OK);
    settle();
    CHECK(forwarded_count(b, VSR_IO_OP_STREAM_SERVE) == 1);
    CHECK(b->io->streams.active == 1 && b->io->forwarded_overflow);
    CHECK(feed_take_serve(&d));
    settle();
    CHECK(b->io->streams.active == 2);
    CHECK(feed_take_serve(&d2));
    /* The first request's stream is served, the other refused. */
    if (stream_at(b, (uint32_t)(d.handle & 0xFFFF))->node != 1 ||
        d.handle == d2.handle) {
        CHECK(false);
    }
    t = stream_at(b, (uint32_t)(d.handle & 0xFFFF));
    CHECK(vsr_io_streams_served(b->io, d2.serve_op, VSR_IO_FAILED) == VSR_OK);
    settle();
    /* Which requester the refusal ended depends on dial order: drain the
     * END with the sink of its cookie and serve the other one. */
    if (forwarded_count(a, VSR_IO_OP_STREAM_END) == 1 &&
        a->io->forwarded[a->io->forwarded_head].rail.end.stream == 1) {
        struct sink swap = k;

        k = k2;
        k2 = swap;
        s = stream_at(a, second);
    }
    sink_drain(&k2);
    CHECK(k2.ended && k2.end.status == VSR_IO_RETRY);
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    /* Four one-chunk writes: DATA ops and WRITTEN ops one at a time. */
    for (uint64_t w = 0; w < 4; ++w) {
        CHECK(feed_write_buffers(&d, w, bytes + w * CHUNK, CHUNK, 0) ==
              VSR_OK);
    }
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_OK);
    settle();
    CHECK(forwarded_count(a, VSR_IO_OP_STREAM_DATA) == 1);
    CHECK(s->units_used == 1 && a->io->forwarded_overflow); /* Ring, not
                                                               window. */
    CHECK(forwarded_count(b, VSR_IO_OP_STREAM_WRITTEN) == 1);
    CHECK(t->writes_count == 3 && b->io->forwarded_overflow);
    /* Drain one op of each side per step. */
    for (uint32_t round = 0; round < 64 && !(k.ended && d.ended); ++round) {
        const struct vsr_io_forwarded *f;

        f = forwarded_take(a, VSR_IO_OP_STREAM_DATA);
        if (f != NULL) {
            CHECK(f->rail.data.offset == k.received);
            CHECK(memcmp(f->rail.data.bytes.data, bytes + k.received,
                         f->rail.data.bytes.size) == 0);
            k.received += f->rail.data.bytes.size;
            k.data_ops++;
            CHECK(vsr_io_streams_data_done(a->io, f->op.op.id) == VSR_OK);
        } else if ((f = forwarded_take(a, VSR_IO_OP_STREAM_END)) != NULL) {
            CHECK(!k.ended && f->rail.end.stream == k.cookie);
            k.ended = true;
            k.end = f->rail.end;
        }
        f = forwarded_take(b, VSR_IO_OP_STREAM_WRITTEN);
        if (f != NULL) {
            CHECK(!d.ended && f->rail.written.write == d.written_count);
            d.written[d.written_count++] = f->rail.written.write;
        } else if ((f = forwarded_take(b, VSR_IO_OP_STREAM_END)) != NULL) {
            CHECK(!d.ended && d.written_count == 4);
            d.ended = true;
            d.end = f->rail.end;
        }
        (void)world_run_checked(1);
    }
    CHECK(k.ended && k.end.status == VSR_IO_OK && k.received == 4 * CHUNK);
    CHECK(k.data_ops == 4);
    CHECK(d.ended && d.end.status == VSR_IO_OK && d.written_count == 4);
    settle();
    CHECK(a->io->forwarded_count == 0 && b->io->forwarded_count == 0);
    CHECK(a->io->streams.active == 0 && b->io->streams.active == 0);
    CHECK(pool_refs(a) == 0 && pool_refs(b) == 0);
    engine_forget(a);
    engine_forget(b);
}

/* Handles and op ids of a finished stream never name the stream that
 * reuses its slot (decision 93: the generation); a zero-length chunk is
 * consumed without a DATA op (decision 95). */
static void test_review_stale_ids(void)
{
    const unsigned char *bytes = pattern(30);
    struct engine *a;
    struct engine *b;
    struct sink k;
    struct feed d;
    struct vsr_io_stream_write write;
    unsigned char header[16];
    uint32_t index = NONE;
    uint32_t first = NONE;
    uint64_t old_handle;
    uint64_t old_serve;
    uint64_t old_data;

    world_reset(30);
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    settle();
    open_stream(&k, &d, 1, NULL, 0, &first);
    settle();
    CHECK(feed_take_serve(&d));
    old_serve = d.serve_op;
    old_handle = d.handle;
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    k.hold = true;
    CHECK(feed_write_buffers(&d, 1, bytes, CHUNK, 0) == VSR_OK);
    settle();
    sink_drain(&k);
    CHECK(k.held_count == 1);
    old_data = k.held_ops[0];
    /* A zero-length chunk at the stream's offset: consumed, no op. */
    vsr_io_codec_put_stream_chunk(header, CHUNK, 0);
    send_raw(b, link_to(b, 1, VSR_IO_INBOUND, VSR_IO_LINK_ESTABLISHED),
             VSR_IO_FRAME_STREAM_CHUNK, header, 16);
    settle();
    sink_drain(&k);
    CHECK(k.data_ops == 1 && a->io->stats.frames_rejected == 0);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_OK);
    k.hold = false;
    sink_complete(&k, 1);
    pump(&k, &d);
    CHECK(k.ended && k.end.status == VSR_IO_OK && k.end.bytes == CHUNK);
    CHECK(d.ended && d.end.status == VSR_IO_OK);
    /* The next stream takes the same slots on both engines. */
    open_stream(&k, &d, 2, NULL, 0, &index);
    settle();
    CHECK(index == first && feed_take_serve(&d));
    CHECK((d.handle & 0xFFFF) == (old_handle & 0xFFFF) && d.handle != old_handle);
    CHECK(vsr_io_streams_served(b->io, old_serve, VSR_IO_OK) == VSR_EINVAL);
    CHECK(vsr_io_streams_served(b->io, d.serve_op, VSR_IO_OK) == VSR_OK);
    CHECK(vsr_io_streams_close(b->io, old_handle, VSR_IO_OK) == VSR_EINVAL);
    memset(&write, 0, sizeof(write));
    write.stream = old_handle;
    write.kind = VSR_IO_WRITE_BUFFERS;
    CHECK(vsr_io_streams_write(b->io, &write, 0) == VSR_EINVAL);
    k.hold = true;
    CHECK(feed_write_buffers(&d, 1, bytes, CHUNK, 0) == VSR_OK);
    settle();
    sink_drain(&k);
    CHECK(k.held_count == 1 && k.held_ops[0] != old_data);
    CHECK(vsr_io_streams_data_done(a->io, old_data) == VSR_EINVAL);
    CHECK(stream_at(a, index)->units_used == 1);
    CHECK(vsr_io_streams_close(b->io, d.handle, VSR_IO_OK) == VSR_OK);
    k.hold = false;
    sink_complete(&k, 1);
    pump(&k, &d);
    CHECK(k.ended && k.end.status == VSR_IO_OK && k.end.bytes == CHUNK);
    CHECK(a->io->streams.active == 0 && b->io->streams.active == 0);
    CHECK(pool_refs(a) == 0 && pool_refs(b) == 0);
    engine_forget(a);
    engine_forget(b);
}

/* A request reaching an engine whose streams shut down is refused at the
 * link: no SERVE op, no stream; the requester sees a loss. And the
 * requester's clock covers the dial: a peer that accepts the connection
 * but never answers the HELLO ends the stream RETRY after the timeout. */
static void test_review_closing_and_dial(void)
{
    struct engine *a;
    struct engine *b;
    struct sink k;
    struct feed d;
    struct vsr_io_address silent;
    uint32_t listener;
    uint32_t index = NONE;

    world_reset(31);
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    settle();
    open_stream(&k, &d, 1, NULL, 0, &index);
    vsr_io_streams_shutdown(b->io);
    settle();
    sink_drain(&k);
    CHECK(forwarded_count(b, VSR_IO_OP_STREAM_SERVE) == 0);
    CHECK(b->io->streams.active == 0);
    CHECK(k.ended && k.end.status == VSR_IO_RETRY && k.end.bytes == 0);
    CHECK(a->io->streams.active == 0);
    engine_forget(b);
    listener = sock_alloc(TEST_OWNER);
    address_of(&silent, 'S');
    world.socks[listener].listening = true;
    world.socks[listener].address = silent;
    CHECK(vsr_io_links_node_set(a->io, 3, &silent) == VSR_OK);
    sink_init(&k, a, 2);
    {
        struct vsr_io_stream_open open;

        memset(&open, 0, sizeof(open));
        open.node = 3;
        CHECK(vsr_io_streams_open(a->io, 2, &open, 0, VSR_IO_STREAM_CALLER,
                                  &index) == VSR_OK);
    }
    settle();
    CHECK(world.socks[listener].backlog_count == 1); /* Connected. */
    world_advance(HANDSHAKE_NS - 1);
    settle();
    sink_drain(&k);
    CHECK(!k.ended && stream_at(a, index)->state == VSR_IO_STREAM_DIALING);
    world_advance(2);
    settle();
    sink_drain(&k);
    CHECK(k.ended && k.end.status == VSR_IO_RETRY && k.end.bytes == 0);
    CHECK(a->io->streams.active == 0 && pool_refs(a) == 0);
    engine_forget(a);
}

int main(int argc, char **argv)
{
    uint64_t seed = argc > 1 ? strtoull(argv[1], NULL, 10) : 4242;

    test_errors();
    test_basic();
    test_window();
    test_backpressure();
    test_file();
    test_loss();
    test_protocol();
    test_library();
    test_timeout();
    test_shutdown();
    test_review_shutdown_order();
    test_review_data_done_rearms();
    test_review_close();
    test_review_chunk_rearms();
    test_review_read_in_flight();
    test_review_full_ring();
    test_review_stale_ids();
    test_review_closing_and_dial();
    test_random(seed);
    test_random(seed + 1);
    printf("stream: ok\n");
    return 0;
}
