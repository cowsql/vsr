#include "config.h"

#include "io/codec.h"
#include "io/crc32c.h"
#include "io/engine.h"
#include "io/link.h"
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
 * The link planner over an in-memory world: two engines (nodes 1 and 2),
 * each with a fake executor, connected through modelled sockets. The test
 * plays the part of engine part 2 (complete, deadlines, poll, prepare) and
 * of the kernel (records are executed against the model, completions are
 * queued back), so every byte the planners exchange passes through the
 * model and can be sliced, delayed, corrupted or cut at will. Test-owned
 * raw peers connect to an engine's listener to speak arbitrary bytes.
 *
 * Harness API (reused by the later phases):
 *   world_reset(seed), engine_open(index, node, handshake), engine_close
 *   engine_step(e)            complete + deadlines + poll + prepare + run
 *   world_settle()            step every engine until nothing moves
 *   world_advance(ns)         move the clock (all engines share it)
 *   peer_connect(e)           a test-owned raw connection to e's listener
 *   peer_write / peer_read / peer_close / peer_reset
 *   forwarded_take(e, kind)   the next forwarded op of that kind, or NULL
 *   world.slice_max           random receive slicing (0: whole runs)
 *   world.hold_notifs         NOTIFs are queued until released
 *   world.short_send          sends complete with at most this many bytes
 *   world.reject_send         the next zero-copy sends fail at translation
 *   engine_connect(e, to)     an engine-owned raw socket to a listener
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
#define REGION_BASE 5u
#define GROUP 3u
#define OWNER 0x5Au
#define TEST_OWNER NONE
#define LINKS 4u
#define NODES 4u
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
};

struct world {
    struct sock socks[SOCKETS];
    struct engine engines[ENGINES];
    uint64_t now;
    int next_fd;
    struct test_random random;
    uint32_t slice_max;
    bool hold_notifs;
    uint32_t short_send;
    uint32_t reject_send; /* Zero-copy sends to refuse with -EINVAL. */
    uint32_t link_queue;  /* limits.link_queue of the next engine_open. */
    uint32_t slab_bytes;  /* limits.slab_bytes of the next engine_open. */
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
        sock->accept_armed) {
        return;
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

static void log_clear(struct engine *e)
{
    e->log_count = 0;
}

static uint32_t log_count(const struct engine *e, uint8_t opcode)
{
    uint32_t count = 0;

    for (uint32_t i = 0; i < e->log_count; ++i) {
        if (e->log[i].opcode == opcode) {
            count++;
        }
    }
    return count;
}

static const struct record_log *log_last(const struct engine *e, uint8_t opcode)
{
    for (uint32_t i = e->log_count; i > 0; --i) {
        if (e->log[i - 1].opcode == opcode) {
            return &e->log[i - 1];
        }
    }
    return NULL;
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
        bool zero_copy = (sqe->op_flags & VSR_IO_SEND_ZERO_COPY) != 0;
        uint32_t total = 0;

        CHECK((sqe->op_flags & VSR_IO_SEND_VECTORED) != 0);
        index = record_sock(e, sqe);
        CHECK(index != NONE);
        sock = &world.socks[index];
        for (uint32_t i = 0; i < sqe->length; ++i) {
            total += (uint32_t)vecs[i].length;
            if ((sqe->flags & VSR_IO_SQE_FIXED_BUFFER) != 0) {
                CHECK(sqe->buffer_index == REGION_BASE);
                CHECK(vsr_io_pool_contains(&e->io->pool, vecs[i].base,
                                           vecs[i].length));
            }
        }
        if (sock->peer == NONE || sock->shutdown) {
            result = -EPIPE;
        } else {
            uint32_t limit = world.short_send > 0 && world.short_send < total
                                 ? world.short_send
                                 : total;
            uint32_t sent = 0;

            for (uint32_t i = 0; i < sqe->length && sent < limit; ++i) {
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
            if (result > 0) {
                inbox_consume_tail(sock->peer, (uint32_t)result);
            }
            complete(e, sqe->user_data, -EINVAL, 0, 0);
            return;
        }
        if (zero_copy) {
            complete(e, sqe->user_data, result, VSR_IO_CQE_MORE, 0);
            if (world.hold_notifs) {
                CHECK(world.held_count < CQ_CAP);
                world.held[world.held_count].user_data = sqe->user_data;
                world.held[world.held_count].result = 0;
                world.held[world.held_count].flags = VSR_IO_CQE_NOTIF;
                world.held[world.held_count].buffer_id = 0;
                world.held_engine[world.held_count] = e->index;
                world.held_count++;
            } else {
                complete(e, sqe->user_data, 0, VSR_IO_CQE_NOTIF, 0);
            }
            return;
        }
        break;
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
    limits->streams = 2;
    limits->stream_window = 2;
    limits->events = 8;
    limits->ops = 6;
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
    options.stream_chunk_bytes = PAGE - 40;
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

/* One loop iteration, as engine part 2 will run it for the link module. */
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
    memcpy(cq, e->cq, cq_count * sizeof(cq[0]));
    e->cq_count = 0;
    for (uint32_t i = 0; i < cq_count; ++i) {
        uint32_t slot;

        if (vsr_io_slots_resolve(&io->slots, cq[i].user_data, &slot) != NULL) {
            vsr_io_links_complete(io, slot, &cq[i]);
        }
    }
    while (vsr_io_deadlines_pop(&io->deadlines, world.now, &kind, &index)) {
        CHECK(kind == VSR_IO_DEADLINE_LINK || kind == VSR_IO_DEADLINE_DIAL);
        vsr_io_links_deadline(io, kind, index, world.now);
    }
    vsr_io_links_poll(io, world.now);
    count = vsr_io_pool_provide(&io->pool, buffers, SLABS);
    if (count > 0) {
        CHECK(fake_provide(e, GROUP, buffers, count) == 0);
    }
    vsr_io_links_prepare(io, sqes, SQ_CAP, &count);
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

/* Steps every open engine until a full round moves nothing, or `rounds`
 * rounds went by; true when the world settled. */
static bool world_run(uint32_t rounds)
{
    for (uint32_t round = 0; round < rounds; ++round) {
        bool moved = false;

        for (uint32_t i = 0; i < ENGINES; ++i) {
            struct engine *e = &world.engines[i];

            if (!e->open) {
                continue;
            }
            engine_step(e);
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

static void world_settle_rounds(uint32_t rounds)
{
    CHECK(world_run(rounds)); /* The world did not settle. */
}

static void world_settle(void)
{
    world_settle_rounds(200);
}

/* -------------------------------------------------------------------------
 * Test-owned raw peers
 * ---------------------------------------------------------------------- */

static uint32_t peer_connect(struct engine *e)
{
    uint32_t client = sock_alloc(TEST_OWNER);
    uint32_t child;

    world.socks[client].raw_fd = fd_alloc();
    child = world_connect(client, &e->listen);
    CHECK(child != NONE);
    return client;
}

/* A raw socket owned by engine e, connected to the listener at `to`, for
 * vsr_io_adopt; returns its descriptor. */
static int engine_connect(struct engine *e, const struct vsr_io_address *to)
{
    uint32_t client = sock_alloc(e->index);

    world.socks[client].raw_fd = fd_alloc();
    CHECK(world_connect(client, to) != NONE);
    return world.socks[client].raw_fd;
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

static void peer_write(uint32_t peer, const void *bytes, size_t length)
{
    struct sock *sock = &world.socks[peer];

    CHECK(sock->used && sock->peer != NONE);
    inbox_append(sock->peer, bytes, length);
}

static size_t peer_read(uint32_t peer, void *out, size_t capacity)
{
    struct sock *sock = &world.socks[peer];
    size_t take = sock->inbox_len < capacity ? sock->inbox_len : capacity;

    memcpy(out, sock->inbox, take);
    inbox_consume(sock, (uint32_t)take);
    return take;
}

static bool peer_eof(uint32_t peer)
{
    return world.socks[peer].peer_closed && world.socks[peer].inbox_len == 0;
}

static void peer_close(uint32_t peer)
{
    world.socks[peer].raw_fd = -1;
    sock_drop(peer);
}

static void peer_reset(uint32_t peer)
{
    struct sock *sock = &world.socks[peer];

    if (sock->peer != NONE) {
        world.socks[sock->peer].reset = true;
        world.socks[sock->peer].peer = NONE;
    }
    sock->peer = NONE;
    sock->raw_fd = -1;
    sock_drop(peer);
}

/* Frame bytes of a HELLO. */
static size_t put_hello(unsigned char *out, uint32_t handshake,
                        uint32_t purpose, uint64_t node, uint64_t nonce)
{
    unsigned char *body = out + VSR_IO_FRAME_HEADER_BYTES;

    vsr_io_codec_put_hello(body, handshake, purpose, node, nonce);
    vsr_io_codec_put_frame(
        out, VSR_IO_FRAME_HELLO, sizeof(struct vsr_io_wire_hello),
        vsr_io_crc32c(0, body, sizeof(struct vsr_io_wire_hello)));
    return HELLO_BYTES;
}

static size_t put_preamble(unsigned char *out)
{
    vsr_io_put_u64(out, VSR_IO_WIRE_MAGIC);
    return VSR_IO_PREAMBLE_BYTES;
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

static uint32_t node_state(const struct engine *e, uint64_t node)
{
    struct vsr_io_node_status status;

    CHECK(vsr_io_links_node_status(e->io, node, &status) == VSR_OK);
    return status.state;
}

/* No link and no slot in use beyond the listener's own ACCEPT. */
static void check_quiet(const struct engine *e)
{
    const struct vsr_io *io = e->io;
    uint32_t busy = 0;

    for (uint32_t i = 0; i < VSR_IO_LISTENERS_MAX; ++i) {
        busy += io->links.listener_table[i].slot != NONE ? 1 : 0;
    }
    CHECK(io->slots.free_count == io->slots.count - busy);
    CHECK(links_in_state(e, VSR_IO_LINK_FREE) == LINKS);
    CHECK(io->links.established == 0 && io->links.pending == 0);
}

/* Two engines, each knowing and authorizing the other for cluster 1. */
static const struct vsr_id cluster = {1, 1};

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

/* -------------------------------------------------------------------------
 * Node and authorization tables
 * ---------------------------------------------------------------------- */

static void test_tables(void)
{
    struct engine *a;
    struct vsr_io_node_status status;
    struct vsr_io_address address;
    struct vsr_io_address bad;
    struct vsr_id other = {2, 2};
    struct vsr_id zero = {0, 0};

    world_reset(1);
    a = engine_open(0, 1, VSR_IO_HANDSHAKE_TRUSTED);
    address_of(&address, 'X');
    /* Validation. */
    CHECK(vsr_io_links_node_set(a->io, VSR_IO_NO_NODE, &address) == VSR_EINVAL);
    bad = address;
    bad.length = 0;
    CHECK(vsr_io_links_node_set(a->io, 5, &bad) == VSR_EINVAL);
    bad = address;
    bad.length = sizeof(bad.sockaddr) + 1;
    CHECK(vsr_io_links_node_set(a->io, 5, &bad) == VSR_EINVAL);
    bad = address;
    bad.reserved = 1;
    CHECK(vsr_io_links_node_set(a->io, 5, &bad) == VSR_EINVAL);
    CHECK(vsr_io_links_node_index(&a->io->links, 5) == NONE);
    CHECK(vsr_io_links_node_status(a->io, 5, &status) == VSR_EINVAL);
    CHECK(vsr_io_links_node_status(a->io, 5, NULL) == VSR_EINVAL);
    /* Set, status, idempotent re-set, table full. */
    CHECK(vsr_io_links_node_set(a->io, 5, &address) == VSR_OK);
    CHECK(vsr_io_links_node_index(&a->io->links, 5) == 0);
    CHECK(vsr_io_links_node_status(a->io, 5, &status) == VSR_OK);
    CHECK(status.state == VSR_IO_NODE_UNLINKED && status.links == 0);
    CHECK(status.next_dial_ns == VSR_NO_DEADLINE && status.last_error == 0);
    CHECK(status.linked_since_ns == 0 && status.last_received_ns == 0);
    CHECK(vsr_io_links_node_set(a->io, 5, &address) == VSR_OK);
    CHECK(a->io->links.nodes[0].has_address);
    CHECK(vsr_io_links_node_set(a->io, 6, NULL) == VSR_OK);
    CHECK(!a->io->links.nodes[1].has_address);
    CHECK(vsr_io_links_node_set(a->io, 7, &address) == VSR_OK);
    CHECK(vsr_io_links_node_set(a->io, 8, &address) == VSR_OK);
    CHECK(vsr_io_links_node_set(a->io, 9, &address) == VSR_ELIMIT);
    CHECK(vsr_io_links_node_clear(a->io, 9) == VSR_EINVAL);
    CHECK(vsr_io_links_node_clear(a->io, 8) == VSR_OK);
    CHECK(vsr_io_links_node_index(&a->io->links, 8) == NONE);
    CHECK(vsr_io_links_node_set(a->io, 9, &address) == VSR_OK);
    /* Authorizations: a zero cluster or an unknown node is refused; a
     * known node is recorded, replaced, revoked; revoking is idempotent;
     * the table fills. */
    CHECK(vsr_io_links_authorize(a->io, zero, 1, 5) == VSR_EINVAL);
    CHECK(vsr_io_links_authorize(a->io, cluster, 1, 42) == VSR_EINVAL);
    CHECK(vsr_io_links_lookup(&a->io->links, cluster, 1) == VSR_IO_NO_NODE);
    CHECK(vsr_io_links_authorize(a->io, cluster, 1, 5) == VSR_OK);
    CHECK(vsr_io_links_lookup(&a->io->links, cluster, 1) == 5);
    CHECK(vsr_io_links_lookup(&a->io->links, cluster, 2) == VSR_IO_NO_NODE);
    CHECK(vsr_io_links_lookup(&a->io->links, other, 1) == VSR_IO_NO_NODE);
    CHECK(vsr_io_links_authorize(a->io, cluster, 1, 5) == VSR_OK);
    CHECK(vsr_io_links_authorize(a->io, cluster, 1, 6) == VSR_OK);
    CHECK(vsr_io_links_lookup(&a->io->links, cluster, 1) == 6);
    CHECK(vsr_io_links_authorize(a->io, other, 1, 6) == VSR_OK);
    CHECK(vsr_io_links_authorize(a->io, cluster, 1, VSR_IO_NO_NODE) == VSR_OK);
    CHECK(vsr_io_links_lookup(&a->io->links, cluster, 1) == VSR_IO_NO_NODE);
    CHECK(vsr_io_links_lookup(&a->io->links, other, 1) == 6);
    CHECK(vsr_io_links_authorize(a->io, cluster, 1, VSR_IO_NO_NODE) == VSR_OK);
    for (uint64_t replica = 2; replica < 9; ++replica) {
        CHECK(vsr_io_links_authorize(a->io, cluster, replica, 7) == VSR_OK);
    }
    CHECK(vsr_io_links_authorize(a->io, cluster, 9, 7) == VSR_ELIMIT);
    CHECK(vsr_io_links_lookup(&a->io->links, cluster, 8) == 7);
    /* Clearing a node removes its authorizations (decision 73). */
    CHECK(vsr_io_links_node_clear(a->io, 7) == VSR_OK);
    CHECK(vsr_io_links_lookup(&a->io->links, cluster, 8) == VSR_IO_NO_NODE);
    CHECK(vsr_io_links_lookup(&a->io->links, other, 1) == 6);
    CHECK(vsr_io_links_authorize(a->io, cluster, 9, 5) == VSR_OK);
    /* Authorizing a node with an address wants a dial; nothing is dialed
     * before a poll, and the listener of a dial-only world refuses. */
    CHECK(a->io->links.nodes[0].wanted && a->io->links.nodes[0].due);
    CHECK(a->io->links.nodes[1].wanted && a->io->links.nodes[1].due);
    CHECK(a->io->links.dials_due == 2);
    engine_forget(a);
}

/* -------------------------------------------------------------------------
 * Dialing, backoff and LINK_WANTED
 * ---------------------------------------------------------------------- */

static void test_backoff(void)
{
    struct engine *a;
    struct vsr_io_node_status status;
    struct vsr_io_address nowhere;
    uint64_t expected[] = {1, 2, 4, 8, 16, 16, 16};
    uint64_t attempt_at;

    world_reset(2);
    a = engine_open(0, 1, VSR_IO_HANDSHAKE_TRUSTED);
    world_settle();            /* Listener setup, before the dial records. */
    address_of(&nowhere, 'B'); /* Engine 1's address, not listening yet. */
    CHECK(vsr_io_links_node_set(a->io, 2, &nowhere) == VSR_OK);
    CHECK(vsr_io_links_authorize(a->io, cluster, 2, 2) == VSR_OK);
    /* Each attempt is a SOCKET then a CONNECT refused; the delays double
     * from the initial one to 16x and stay there. */
    for (uint32_t i = 0; i < 7; ++i) {
        attempt_at = world.now;
        log_clear(a);
        world_settle();
        CHECK(log_count(a, VSR_IO_SQE_SOCKET) == 1);
        CHECK(log_count(a, VSR_IO_SQE_CONNECT) == 1);
        CHECK(log_count(a, VSR_IO_SQE_CLOSE) == 1);
        CHECK(vsr_io_links_node_status(a->io, 2, &status) == VSR_OK);
        CHECK(status.state == VSR_IO_NODE_UNLINKED);
        CHECK(status.last_error == -ECONNREFUSED);
        CHECK(status.next_dial_ns == attempt_at + expected[i] * BACKOFF_NS);
        CHECK(a->io->links.nodes[0].attempts == i + 1);
        CHECK(vsr_io_deadlines_earliest(&a->io->deadlines) ==
              status.next_dial_ns);
        check_quiet(a);
        /* Just before the deadline nothing happens. */
        world.now = status.next_dial_ns - 1;
        log_clear(a);
        world_settle();
        CHECK(log_count(a, VSR_IO_SQE_SOCKET) == 0);
        world.now = status.next_dial_ns;
    }
    /* The listener appears (node 2 listens at B): the next attempt
     * succeeds and resets the schedule; a later loss redials at once. */
    {
        struct engine *b = engine_open(1, 2, VSR_IO_HANDSHAKE_TRUSTED);

        CHECK(vsr_io_links_node_set(b->io, 1, &a->listen) == VSR_OK);
        world_settle();
        world.now = status.next_dial_ns;
        world_settle();
        CHECK(node_state(a, 2) == VSR_IO_NODE_LINKED);
        CHECK(a->io->links.nodes[0].attempts == 0);
        CHECK(vsr_io_links_node_status(a->io, 2, &status) == VSR_OK);
        CHECK(status.last_error == 0 && status.next_dial_ns == VSR_NO_DEADLINE);
        engine_forget(b);
        world_settle();
        CHECK(node_state(a, 2) == VSR_IO_NODE_UNLINKED);
        CHECK(vsr_io_links_node_status(a->io, 2, &status) == VSR_OK);
        CHECK(status.next_dial_ns == VSR_NO_DEADLINE);
        /* A SEND wants the link again: the dial is immediate, fails at
         * once (no listener) and the schedule restarts at 1x. */
        a->io->replicas[0].state = VSR_IO_REPLICA_RUNNING;
        a->io->replicas[0].options.cluster = cluster;
        CHECK(vsr_io_links_send(a->io, 0, 1, NULL, 2) == VSR_IO_RETRY);
        CHECK(!a->io->links.nodes[0].wanted);
        {
            struct vsr_message message;

            memset(&message, 0, sizeof(message));
            CHECK(vsr_io_links_send(a->io, 0, 1, &message, 2) == VSR_IO_RETRY);
        }
        CHECK(a->io->links.nodes[0].wanted);
        attempt_at = world.now;
        log_clear(a);
        world_settle();
        CHECK(log_count(a, VSR_IO_SQE_CONNECT) == 1);
        CHECK(vsr_io_links_node_status(a->io, 2, &status) == VSR_OK);
        CHECK(status.next_dial_ns == attempt_at + BACKOFF_NS);
        a->io->replicas[0].state = VSR_IO_REPLICA_FREE;
    }
    engine_forget(a);
}

/* A caller-dialed node: LINK_WANTED on the backoff schedule with the
 * attempt count, until a link is adopted. */
static void test_link_wanted(void)
{
    struct engine *a;
    const struct vsr_io_forwarded *op;
    struct vsr_io_node_status status;
    uint32_t peer;
    unsigned char bytes[128];
    size_t n;

    world_reset(3);
    a = engine_open(0, 1, VSR_IO_HANDSHAKE_TRUSTED);
    CHECK(vsr_io_links_node_set(a->io, 2, NULL) == VSR_OK);
    CHECK(vsr_io_links_authorize(a->io, cluster, 2, 2) == VSR_OK);
    for (uint32_t i = 0; i < 6; ++i) {
        uint64_t delay = (uint64_t)(i < 4 ? 1u << i : 16u) * BACKOFF_NS;
        uint64_t emitted_at = world.now;

        world_settle();
        op = forwarded_take(a, VSR_IO_OP_LINK_WANTED);
        CHECK(op != NULL);
        CHECK(op->op.replica == NULL && op->op.op.id == 0);
        CHECK(op->op.op.data == &op->rail.wanted);
        CHECK(op->rail.wanted.node == 2 && op->rail.wanted.attempt == i + 1);
        CHECK(forwarded_count(a, VSR_IO_OP_LINK_WANTED) == 0);
        CHECK(vsr_io_links_node_status(a->io, 2, &status) == VSR_OK);
        CHECK(status.state == VSR_IO_NODE_PENDING);
        CHECK(status.next_dial_ns == VSR_NO_DEADLINE);
        CHECK(a->io->links.nodes[0].next_dial_ns == emitted_at + delay);
        world.now = emitted_at + delay - 1;
        world_settle();
        CHECK(forwarded_take(a, VSR_IO_OP_LINK_WANTED) == NULL);
        world.now = emitted_at + delay;
    }
    /* The caller connects and adopts with HANDSHAKE | OUTBOUND: the engine
     * sends the preamble and HELLO; an answer naming another node, or a
     * stream purpose, is refused and counts as a failed attempt, and the
     * op repeats on the schedule. */
    world_settle();
    CHECK(forwarded_take(a, VSR_IO_OP_LINK_WANTED) != NULL);
    for (uint32_t wrong = 0; wrong < 2; ++wrong) {
        uint64_t rejected = a->io->stats.frames_rejected;
        uint32_t attempts = a->io->links.nodes[0].attempts;

        peer = sock_alloc(TEST_OWNER);
        {
            uint32_t engine_side = sock_alloc(0);

            world.socks[peer].raw_fd = fd_alloc();
            world.socks[engine_side].raw_fd = fd_alloc();
            world.socks[peer].peer = engine_side;
            world.socks[engine_side].peer = peer;
            CHECK(vsr_io_links_adopt(a->io, world.socks[engine_side].raw_fd, 2,
                                     VSR_IO_ADOPT_HANDSHAKE |
                                         VSR_IO_ADOPT_OUTBOUND) == VSR_OK);
        }
        world_settle();
        n = peer_read(peer, bytes, sizeof(bytes));
        CHECK(n == VSR_IO_PREAMBLE_BYTES + HELLO_BYTES);
        n = put_hello(bytes, VSR_IO_HANDSHAKE_TRUSTED,
                      wrong == 0 ? VSR_IO_PURPOSE_PEER : VSR_IO_PURPOSE_STREAM,
                      wrong == 0 ? 3 : 2, 77);
        peer_write(peer, bytes, n);
        world_settle();
        CHECK(a->io->stats.frames_rejected == rejected + 1);
        CHECK(peer_eof(peer));
        check_quiet(a);
        CHECK(node_state(a, 2) == VSR_IO_NODE_UNLINKED);
        CHECK(a->io->links.nodes[0].attempts == attempts + 1);
        CHECK(a->io->links.nodes[0].last_error == -EPROTO);
        peer_close(peer);
        world.now = a->io->links.nodes[0].next_dial_ns;
        world_settle();
        CHECK(forwarded_take(a, VSR_IO_OP_LINK_WANTED) != NULL);
    }
    /* The right answer links the node and no LINK_WANTED follows. */
    peer = sock_alloc(TEST_OWNER);
    {
        uint32_t engine_side = sock_alloc(0);

        world.socks[peer].raw_fd = fd_alloc();
        world.socks[engine_side].raw_fd = fd_alloc();
        world.socks[peer].peer = engine_side;
        world.socks[engine_side].peer = peer;
        CHECK(vsr_io_links_adopt(a->io, world.socks[engine_side].raw_fd, 2,
                                 VSR_IO_ADOPT_HANDSHAKE |
                                     VSR_IO_ADOPT_OUTBOUND) == VSR_OK);
    }
    CHECK(node_state(a, 2) == VSR_IO_NODE_PENDING);
    world_settle();
    n = peer_read(peer, bytes, sizeof(bytes));
    CHECK(n == VSR_IO_PREAMBLE_BYTES + HELLO_BYTES);
    CHECK(vsr_io_get_u64(bytes) == VSR_IO_WIRE_MAGIC);
    n = put_hello(bytes, VSR_IO_HANDSHAKE_TRUSTED, VSR_IO_PURPOSE_PEER, 2, 77);
    peer_write(peer, bytes, n);
    world_settle();
    CHECK(node_state(a, 2) == VSR_IO_NODE_LINKED);
    CHECK(a->io->links.nodes[0].attempts == 0);
    CHECK(!a->io->links.nodes[0].wanted);
    world_advance(100 * BACKOFF_NS);
    world_settle();
    CHECK(forwarded_take(a, VSR_IO_OP_LINK_WANTED) == NULL);
    /* Adopt argument checks. */
    CHECK(vsr_io_links_adopt(a->io, -1, 2, 0) == VSR_EINVAL);
    CHECK(vsr_io_links_adopt(a->io, 900, 2, 4) == VSR_EINVAL);
    CHECK(vsr_io_links_adopt(a->io, 900, 2, VSR_IO_ADOPT_OUTBOUND) ==
          VSR_EINVAL);
    CHECK(vsr_io_links_adopt(a->io, 900, 42, 0) == VSR_EINVAL);
    CHECK(vsr_io_links_adopt(a->io, 900, 2, VSR_IO_ADOPT_HANDSHAKE) ==
          VSR_EINVAL);
    engine_forget(a);
}

/* -------------------------------------------------------------------------
 * The TRUSTED handshake between two engines
 * ---------------------------------------------------------------------- */

static void check_linked(struct engine *a, struct engine *b)
{
    const struct vsr_io_link *out =
        link_to(a, 2, VSR_IO_OUTBOUND, VSR_IO_LINK_ESTABLISHED);
    const struct vsr_io_link *in =
        link_to(b, 1, VSR_IO_INBOUND, VSR_IO_LINK_ESTABLISHED);
    struct vsr_io_node_status status;

    CHECK(out != NULL && in != NULL);
    CHECK(out->purpose == VSR_IO_PURPOSE_PEER && in->purpose == out->purpose);
    CHECK(out->fd >= (int32_t)FILE_SLOT_BASE && out->raw_fd == -1);
    CHECK(in->fd >= (int32_t)FILE_SLOT_BASE && in->raw_fd == -1);
    CHECK(out->recv_slot != NONE && in->recv_slot != NONE);
    CHECK(out->hello_sent && out->hello_seen && in->hello_sent &&
          in->hello_seen);
    CHECK(a->io->links.established == 1 && a->io->links.pending == 0);
    CHECK(b->io->links.established == 1 && b->io->links.pending == 0);
    CHECK(a->io->links.nodes[0].carrier ==
          (uint32_t)(out - a->io->links.links));
    CHECK(b->io->links.nodes[0].carrier == (uint32_t)(in - b->io->links.links));
    CHECK(vsr_io_links_node_status(a->io, 2, &status) == VSR_OK);
    CHECK(status.state == VSR_IO_NODE_LINKED && status.links == 1);
    CHECK(status.linked_since_ns == out->established_ns);
    CHECK(status.last_received_ns != 0);
    CHECK(status.next_dial_ns == VSR_NO_DEADLINE);
    CHECK(vsr_io_links_node_status(b->io, 1, &status) == VSR_OK);
    CHECK(status.state == VSR_IO_NODE_LINKED && status.links == 1);
    /* Every send went zero-copy from the pool region and, unless the
     * world holds them back, every NOTIF is in: no send entry is live. */
    CHECK(out->sent_offset == out->stream_offset);
    CHECK(out->sent_offset == VSR_IO_PREAMBLE_BYTES + HELLO_BYTES);
    CHECK(in->sent_offset == HELLO_BYTES);
    if (world.hold_notifs) {
        return;
    }
    for (uint32_t i = 0; i < VSR_IO_LINK_SENDS; ++i) {
        CHECK(out->sends[i].slot == NONE && in->sends[i].slot == NONE);
    }
    CHECK(out->notified_offset == out->sent_offset);
    CHECK(in->notified_offset == in->sent_offset);
}

/* Phase 3 helpers the earlier tests borrow: a replica shell, and a SEND
 * of a fresh sample message (op id = its number). */
static struct vsr_io_replica *open_replica(struct engine *e, uint32_t index,
                                           struct vsr_id id);
static int send_fresh(struct engine *e, uint32_t index, uint64_t number,
                      uint32_t size, uint64_t member);

static void test_handshake(uint32_t slice_max)
{
    struct engine *a;
    struct engine *b;
    const struct record_log *send;
    struct vsr_io_stats stats;

    world_reset(4 + slice_max);
    world.slice_max = slice_max;
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    /* Listeners come up on the first prepare: SOCKET/BIND/LISTEN/ACCEPT
     * chained, the first three skipping their success. */
    world_settle();
    CHECK(log_count(a, VSR_IO_SQE_SOCKET) == 1);
    CHECK(log_count(a, VSR_IO_SQE_ACCEPT) == 1);
    CHECK(a->log[0].opcode == VSR_IO_SQE_SOCKET);
    CHECK(a->log[0].flags ==
          (VSR_IO_SQE_LINK | VSR_IO_SQE_SKIP_SUCCESS | VSR_IO_SQE_DIRECT));
    CHECK(a->log[1].opcode == VSR_IO_SQE_BIND);
    CHECK(a->log[2].opcode == VSR_IO_SQE_LISTEN);
    CHECK(a->log[3].opcode == VSR_IO_SQE_ACCEPT);
    CHECK(a->log[3].flags == VSR_IO_SQE_FIXED_FILE);
    CHECK(a->io->links.listener_table[0].state == VSR_IO_LISTENER_ACTIVE);
    CHECK(a->io->links.listeners == 1);
    CHECK(node_state(a, 2) == VSR_IO_NODE_UNLINKED);
    /* Authorizing dials: A connects to B, the dialer's preamble and HELLO
     * go out in one zero-copy fixed-buffer send, B answers, both link. */
    log_clear(a);
    log_clear(b);
    CHECK(vsr_io_links_authorize(a->io, cluster, 2, 2) == VSR_OK);
    CHECK(vsr_io_links_authorize(a->io, cluster, 1, 1) == VSR_OK);
    world_settle();
    check_linked(a, b);
    /* B authorizes A once linked: no dial, the node is linked already;
     * neither engine ever wants a link to its own node. */
    CHECK(vsr_io_links_authorize(b->io, cluster, 1, 1) == VSR_OK);
    CHECK(vsr_io_links_authorize(b->io, cluster, 2, 2) == VSR_OK);
    world_settle();
    CHECK(forwarded_count(a, VSR_IO_OP_LINK_WANTED) == 0);
    CHECK(forwarded_count(b, VSR_IO_OP_LINK_WANTED) == 0);
    check_linked(a, b);
    CHECK(log_count(a, VSR_IO_SQE_SOCKET) == 1);
    CHECK(log_count(a, VSR_IO_SQE_CONNECT) == 1);
    CHECK(log_count(a, VSR_IO_SQE_SEND) == 1);
    send = log_last(a, VSR_IO_SQE_SEND);
    CHECK(send->op_flags == (VSR_IO_SEND_ZERO_COPY | VSR_IO_SEND_VECTORED));
    CHECK(send->flags == (VSR_IO_SQE_FIXED_FILE | VSR_IO_SQE_FIXED_BUFFER));
    CHECK(send->buffer_index == REGION_BASE && send->length == 1);
    CHECK(log_count(b, VSR_IO_SQE_SEND) == 1);
    CHECK(log_count(b, VSR_IO_SQE_SOCKET) == 0);
    CHECK(a->updates == 1 && b->updates == 1);
    vsr_io_get_stats(a->io, &stats);
    CHECK(stats.links == 1 && stats.links_pending == 0);
    CHECK(stats.sends == 1 && stats.sends_zero_copy == 1);
    CHECK(stats.bytes_sent == VSR_IO_PREAMBLE_BYTES + HELLO_BYTES);
    CHECK(stats.bytes_received == HELLO_BYTES);
    CHECK(stats.frames_rejected == 0 && stats.failure == 0);
    vsr_io_get_stats(b->io, &stats);
    CHECK(stats.bytes_received == VSR_IO_PREAMBLE_BYTES + HELLO_BYTES);
    /* B did not dial: it holds no want for node 1 while linked, and the
     * pending counters are back to zero on both sides. */
    CHECK(!b->io->links.nodes[0].wanted && !a->io->links.nodes[0].wanted);
    CHECK(a->io->links.nodes[0].pending == 0 &&
          b->io->links.nodes[0].pending == 0);
    /* Slab accounting: each link holds its send slab; receive slabs are
     * back in the ring or held by nothing else. */
    CHECK(a->io->pool.free_count + a->io->pool.kernel_count == SLABS - 1);
    CHECK(b->io->pool.free_count + b->io->pool.kernel_count == SLABS - 1);
    /* Nothing was retained under a partial frame. */
    CHECK(
        link_to(a, 2, VSR_IO_OUTBOUND, VSR_IO_LINK_ESTABLISHED)->partial_slab ==
        NONE);
    /* Peer closes: A's receive reads EOF (the multishot ended, so no
     * CANCEL is needed), the link tears down with SHUTDOWN and CLOSE and
     * the entry is FREE. No redial follows (nothing queued). */
    engine_forget(b);
    log_clear(a);
    world_settle();
    CHECK(log_count(a, VSR_IO_SQE_SHUTDOWN) == 1);
    CHECK(log_count(a, VSR_IO_SQE_CANCEL) == 0);
    CHECK(log_count(a, VSR_IO_SQE_CLOSE) == 1);
    CHECK(log_count(a, VSR_IO_SQE_SOCKET) == 0);
    check_quiet(a);
    CHECK(node_state(a, 2) == VSR_IO_NODE_UNLINKED);
    CHECK(a->io->links.nodes[0].last_error == -EPIPE);
    CHECK(a->io->file_slots_free_count == 1);
    CHECK(a->io->pool.free_count + a->io->pool.kernel_count == SLABS);
    engine_forget(a);
}

/* Both dial at once: two established links per side; the carrier on both
 * ends is the link dialed by node 1; the idle timeout closes the other and
 * keeps the carrier; the carrier goes idle in turn once nothing flows. */
static void test_election(void)
{
    struct engine *a;
    struct engine *b;
    const struct vsr_io_link *a_out;
    const struct vsr_io_link *a_in;
    const struct vsr_io_link *b_out;
    const struct vsr_io_link *b_in;
    struct vsr_io_node_status status;
    uint64_t linked_at;

    world_reset(5);
    world.slice_max = 7;
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    world_settle();
    CHECK(vsr_io_links_authorize(a->io, cluster, 2, 2) == VSR_OK);
    CHECK(vsr_io_links_authorize(b->io, cluster, 1, 1) == VSR_OK);
    linked_at = world.now;
    world_settle();
    a_out = link_to(a, 2, VSR_IO_OUTBOUND, VSR_IO_LINK_ESTABLISHED);
    a_in = link_to(a, 2, VSR_IO_INBOUND, VSR_IO_LINK_ESTABLISHED);
    b_out = link_to(b, 1, VSR_IO_OUTBOUND, VSR_IO_LINK_ESTABLISHED);
    b_in = link_to(b, 1, VSR_IO_INBOUND, VSR_IO_LINK_ESTABLISHED);
    CHECK(a_out != NULL && a_in != NULL && b_out != NULL && b_in != NULL);
    CHECK(a->io->links.established == 2 && b->io->links.established == 2);
    CHECK(a->io->links.nodes[0].established == 2);
    CHECK(vsr_io_links_node_status(a->io, 2, &status) == VSR_OK);
    CHECK(status.state == VSR_IO_NODE_LINKED && status.links == 2);
    CHECK(status.linked_since_ns == linked_at);
    /* Node 1 < node 2: the link A dialed carries at both ends. */
    CHECK(a->io->links.nodes[0].carrier ==
          (uint32_t)(a_out - a->io->links.links));
    CHECK(b->io->links.nodes[0].carrier ==
          (uint32_t)(b_in - b->io->links.links));
    /* Sends queued for the node keep the carrier: one message each way,
     * its NOTIF held so it never completes; the other link expires after
     * the idle timeout and is closed at both ends, the carrier stays. */
    open_replica(a, 0, cluster);
    open_replica(b, 0, cluster);
    world.hold_notifs = true;
    CHECK(send_fresh(a, 0, 1, 16, 2) == VSR_OK);
    CHECK(send_fresh(b, 0, 1, 16, 1) == VSR_OK);
    world_settle();
    CHECK(a->io->links.nodes[0].queue_count == 1);
    CHECK(b->io->links.nodes[0].queue_count == 1);
    world_advance(IDLE_NS / 2);
    world_settle();
    CHECK(a->io->links.established == 2);
    world_advance(IDLE_NS / 2);
    world_settle();
    CHECK(a->io->links.established == 1 && b->io->links.established == 1);
    CHECK(a_out->state == VSR_IO_LINK_ESTABLISHED);
    CHECK(b_in->state == VSR_IO_LINK_ESTABLISHED);
    CHECK(a_in->state == VSR_IO_LINK_FREE && b_out->state == VSR_IO_LINK_FREE);
    CHECK(a->io->links.nodes[0].carrier ==
          (uint32_t)(a_out - a->io->links.links));
    CHECK(b->io->links.nodes[0].carrier ==
          (uint32_t)(b_in - b->io->links.links));
    CHECK(node_state(a, 2) == VSR_IO_NODE_LINKED);
    /* With nothing queued the carrier is idle-closed like any link, and
     * no redial follows until a SEND wants one. */
    world_advance(IDLE_NS);
    world_settle();
    CHECK(a->io->links.established == 1 && b->io->links.established == 1);
    world.hold_notifs = false;
    world_release_notifs();
    world_settle();
    CHECK(a->io->links.nodes[0].queue_count == 0);
    CHECK(b->io->links.nodes[0].queue_count == 0);
    world_advance(IDLE_NS);
    world_settle();
    check_quiet(a);
    check_quiet(b);
    CHECK(node_state(a, 2) == VSR_IO_NODE_UNLINKED);
    CHECK(a->io->links.nodes[0].last_error == 0);
    /* A SEND toward node 2 redials at once (attempts were reset). */
    {
        struct vsr_message message;

        memset(&message, 0, sizeof(message));
        a->io->replicas[0].state = VSR_IO_REPLICA_RUNNING;
        a->io->replicas[0].options.cluster = cluster;
        CHECK(vsr_io_links_send(a->io, 0, 1, &message, 2) == VSR_IO_RETRY);
        CHECK(vsr_io_links_send(a->io, 0, 1, &message, 9) == VSR_IO_RETRY);
        CHECK(a->io->links.nodes[0].wanted);
        world_settle();
        CHECK(link_to(a, 2, VSR_IO_OUTBOUND, VSR_IO_LINK_ESTABLISHED) != NULL);
        CHECK(a->io->links.nodes[0].carrier != NONE);
        a->io->replicas[0].state = VSR_IO_REPLICA_FREE;
    }
    engine_forget(a);
    engine_forget(b);
}

/* -------------------------------------------------------------------------
 * Refusals: bad preamble, bad frames, bad HELLOs, timeouts
 * ---------------------------------------------------------------------- */

/* One engine listening; a raw peer speaks bytes; the link must close with
 * the frame counted as rejected, the socket closed (EOF at the peer). */
static void refuse(struct engine *a, const unsigned char *bytes, size_t length,
                   uint64_t rejected_before, int32_t error)
{
    uint32_t peer = peer_connect(a);
    unsigned char out[256];

    world_settle();
    CHECK(links_in_state(a, VSR_IO_LINK_HELLO) == 1);
    CHECK(a->io->links.pending == 1);
    peer_write(peer, bytes, length);
    world_settle();
    CHECK(a->io->stats.frames_rejected == rejected_before + 1);
    CHECK(peer_eof(peer));
    CHECK(peer_read(peer, out, sizeof(out)) == 0);
    check_quiet(a);
    CHECK(a->io->links.links[0].error == 0);
    (void)error;
    peer_close(peer);
}

static void test_refusals(void)
{
    struct engine *a;
    unsigned char bytes[256];
    size_t n;
    uint64_t rejected = 0;
    uint32_t peer;

    world_reset(6);
    world.slice_max = 5;
    a = engine_open(0, 1, VSR_IO_HANDSHAKE_TRUSTED);
    CHECK(vsr_io_links_node_set(a->io, 2, NULL) == VSR_OK);
    world_settle();
    /* Bad preamble byte, at each position. */
    for (uint32_t i = 0; i < VSR_IO_PREAMBLE_BYTES; ++i) {
        n = put_preamble(bytes);
        bytes[i] ^= 0x40;
        refuse(a, bytes, n, rejected++, -EPROTO);
    }
    /* Frame header faults: magic, version, kind, length not a multiple
     * of 8, length above the limit, header CRC, reserved. */
    n = put_preamble(bytes);
    n += put_hello(bytes + n, VSR_IO_HANDSHAKE_TRUSTED, VSR_IO_PURPOSE_PEER, 2,
                   1);
    bytes[8] ^= 1;
    refuse(a, bytes, n, rejected++, -EBADMSG);
    n = put_preamble(bytes);
    n += put_hello(bytes + n, VSR_IO_HANDSHAKE_TRUSTED, VSR_IO_PURPOSE_PEER, 2,
                   1);
    vsr_io_put_u32(bytes + 8 + 4, 2u | (VSR_IO_FRAME_HELLO << 16));
    vsr_io_put_u32(bytes + 8 + 16, vsr_io_crc32c(0, bytes + 8, 16));
    refuse(a, bytes, n, rejected++, -EBADMSG);
    n = put_preamble(bytes);
    n += put_hello(bytes + n, VSR_IO_HANDSHAKE_TRUSTED, VSR_IO_PURPOSE_PEER, 2,
                   1);
    vsr_io_put_u32(bytes + 8 + 4, VSR_IO_WIRE_VERSION | (9u << 16));
    vsr_io_put_u32(bytes + 8 + 16, vsr_io_crc32c(0, bytes + 8, 16));
    refuse(a, bytes, n, rejected++, -EBADMSG);
    n = put_preamble(bytes);
    vsr_io_codec_put_frame(bytes + n, VSR_IO_FRAME_HELLO, 36, 0);
    n += VSR_IO_FRAME_HEADER_BYTES;
    refuse(a, bytes, n, rejected++, -EBADMSG);
    n = put_preamble(bytes);
    vsr_io_codec_put_frame(bytes + n, VSR_IO_FRAME_HELLO, PAGE - 24 + 8, 0);
    n += VSR_IO_FRAME_HEADER_BYTES;
    refuse(a, bytes, n, rejected++, -EBADMSG);
    /* The largest legal length is accepted by the header check and then
     * waits for its body: the handshake timeout closes it. */
    n = put_preamble(bytes);
    vsr_io_codec_put_frame(bytes + n, VSR_IO_FRAME_HELLO, PAGE - 24, 0);
    n += VSR_IO_FRAME_HEADER_BYTES;
    peer = peer_connect(a);
    world_settle();
    peer_write(peer, bytes, n);
    world_settle();
    CHECK(links_in_state(a, VSR_IO_LINK_HELLO) == 1);
    CHECK(a->io->stats.frames_rejected == rejected);
    CHECK(a->io->links.links[0].partial_length == VSR_IO_FRAME_HEADER_BYTES);
    world_advance(HANDSHAKE_NS - 1);
    world_settle();
    CHECK(links_in_state(a, VSR_IO_LINK_HELLO) == 1);
    world_advance(1);
    world_settle();
    CHECK(peer_eof(peer));
    check_quiet(a);
    CHECK(a->io->pool.free_count + a->io->pool.kernel_count == SLABS);
    peer_close(peer);
    n = put_preamble(bytes);
    n += put_hello(bytes + n, VSR_IO_HANDSHAKE_TRUSTED, VSR_IO_PURPOSE_PEER, 2,
                   1);
    bytes[8 + 17] ^= 1;
    refuse(a, bytes, n, rejected++, -EBADMSG);
    n = put_preamble(bytes);
    n += put_hello(bytes + n, VSR_IO_HANDSHAKE_TRUSTED, VSR_IO_PURPOSE_PEER, 2,
                   1);
    bytes[8 + 20] = 1;
    refuse(a, bytes, n, rejected++, -EBADMSG);
    /* Body CRC. */
    n = put_preamble(bytes);
    n += put_hello(bytes + n, VSR_IO_HANDSHAKE_TRUSTED, VSR_IO_PURPOSE_PEER, 2,
                   1);
    bytes[8 + 24 + 8] ^= 1;
    refuse(a, bytes, n, rejected++, -EBADMSG);
    /* HELLO faults: mode, purpose, unknown node, node zero, reserved, a
     * MESSAGE frame before the handshake. */
    n = put_preamble(bytes);
    n += put_hello(bytes + n, VSR_IO_HANDSHAKE_EXTERNAL, VSR_IO_PURPOSE_PEER, 2,
                   1);
    refuse(a, bytes, n, rejected++, -EPROTO);
    n = put_preamble(bytes);
    n += put_hello(bytes + n, VSR_IO_HANDSHAKE_TRUSTED, 3, 2, 1);
    refuse(a, bytes, n, rejected++, -EBADMSG);
    n = put_preamble(bytes);
    n += put_hello(bytes + n, VSR_IO_HANDSHAKE_TRUSTED, VSR_IO_PURPOSE_PEER, 42,
                   1);
    refuse(a, bytes, n, rejected++, -EPROTO);
    n = put_preamble(bytes);
    n += put_hello(bytes + n, VSR_IO_HANDSHAKE_TRUSTED, VSR_IO_PURPOSE_PEER, 0,
                   1);
    refuse(a, bytes, n, rejected++, -EPROTO);
    n = put_preamble(bytes);
    n += put_hello(bytes + n, VSR_IO_HANDSHAKE_TRUSTED, VSR_IO_PURPOSE_PEER, 2,
                   1);
    bytes[8 + 24 + 24] = 1;
    vsr_io_put_u32(bytes + 8 + 12, vsr_io_crc32c(0, bytes + 8 + 24, 32));
    vsr_io_put_u32(bytes + 8 + 16, vsr_io_crc32c(0, bytes + 8, 16));
    refuse(a, bytes, n, rejected++, -EBADMSG);
    n = put_preamble(bytes);
    vsr_io_codec_put_frame(bytes + n, VSR_IO_FRAME_MESSAGE, 0, 0);
    n += VSR_IO_FRAME_HEADER_BYTES;
    refuse(a, bytes, n, rejected++, -EPROTO);
    /* A second HELLO after the first is a protocol error on an
     * established link; a stream frame on a peer link too. */
    n = put_preamble(bytes);
    n += put_hello(bytes + n, VSR_IO_HANDSHAKE_TRUSTED, VSR_IO_PURPOSE_PEER, 2,
                   1);
    n += put_hello(bytes + n, VSR_IO_HANDSHAKE_TRUSTED, VSR_IO_PURPOSE_PEER, 2,
                   1);
    refuse(a, bytes, n, rejected++, -EPROTO);
    n = put_preamble(bytes);
    n += put_hello(bytes + n, VSR_IO_HANDSHAKE_TRUSTED, VSR_IO_PURPOSE_PEER, 2,
                   1);
    vsr_io_codec_put_frame(bytes + n, VSR_IO_FRAME_STREAM_END, 0, 0);
    n += VSR_IO_FRAME_HEADER_BYTES;
    refuse(a, bytes, n, rejected++, -EPROTO);
    /* A silent peer: the handshake timeout. */
    peer = peer_connect(a);
    world_settle();
    world_advance(HANDSHAKE_NS);
    world_settle();
    CHECK(peer_eof(peer));
    check_quiet(a);
    CHECK(a->io->stats.frames_rejected == rejected);
    peer_close(peer);
    /* A peer that resets mid-handshake: an inbound link that never said
     * who it was is nobody's failure. */
    {
        int32_t before = a->io->links.nodes[0].last_error;

        peer = peer_connect(a);
        world_settle();
        put_preamble(bytes);
        peer_write(peer, bytes, 3);
        world_settle();
        CHECK(a->io->links.links[0].preamble_seen == 3);
        peer_reset(peer);
        world_settle();
        check_quiet(a);
        CHECK(a->io->links.nodes[0].last_error == before);
    }
    /* A good HELLO after all that: linked, the HELLO answered, a stream
     * frame then (phase 3) is dropped, and EOF from the peer closes. */
    peer = peer_connect(a);
    world_settle();
    n = put_preamble(bytes);
    n += put_hello(bytes + n, VSR_IO_HANDSHAKE_TRUSTED, VSR_IO_PURPOSE_PEER, 2,
                   5);
    peer_write(peer, bytes, n);
    world_settle();
    CHECK(node_state(a, 2) == VSR_IO_NODE_LINKED);
    CHECK(links_in_state(a, VSR_IO_LINK_ESTABLISHED) == 1);
    n = peer_read(peer, bytes, sizeof(bytes));
    CHECK(n == HELLO_BYTES);
    {
        struct vsr_io_cursor cursor;
        struct vsr_io_wire_frame frame;
        struct vsr_io_wire_hello hello;

        vsr_io_cursor_init_one(&cursor, bytes, n);
        CHECK(vsr_io_codec_get_frame(&cursor, PAGE, &frame) == VSR_OK);
        CHECK(frame.kind == VSR_IO_FRAME_HELLO && frame.length == 32);
        CHECK(vsr_io_codec_get_hello(&cursor, &hello) == VSR_OK);
        CHECK(hello.handshake == VSR_IO_HANDSHAKE_TRUSTED);
        CHECK(hello.purpose == VSR_IO_PURPOSE_PEER && hello.node == 1);
        CHECK(hello.nonce == a->io->links.links[0].nonce);
    }
    CHECK(a->io->stats.frames_rejected == rejected);
    peer_close(peer);
    world_settle();
    check_quiet(a);
    engine_forget(a);
}

/* -------------------------------------------------------------------------
 * EXTERNAL mode
 * ---------------------------------------------------------------------- */

static void test_external(void)
{
    struct engine *a;
    struct engine *b;
    const struct vsr_io_forwarded *op;
    struct vsr_io_handshake handshake;
    struct vsr_io_handshake_done done;
    uint32_t peer;
    unsigned char bytes[64];
    uint64_t op_a;
    uint64_t op_b;

    world_reset(7);
    world.slice_max = 3;
    two_engines(VSR_IO_HANDSHAKE_EXTERNAL);
    a = &world.engines[0];
    b = &world.engines[1];
    world_settle();
    CHECK(vsr_io_links_authorize(a->io, cluster, 2, 2) == VSR_OK);
    log_clear(a);
    log_clear(b);
    world_settle();
    /* A: SOCKET, CONNECT, the preamble sent on the raw descriptor (zero
     * copy from the pool, not FIXED_FILE), then a HANDSHAKE op with the
     * raw descriptor, OUTBOUND, expecting node 2; no receive is armed. */
    CHECK(log_count(a, VSR_IO_SQE_SEND) == 1);
    CHECK((log_last(a, VSR_IO_SQE_SEND)->flags & VSR_IO_SQE_FIXED_FILE) == 0);
    CHECK(log_last(a, VSR_IO_SQE_SEND)->op_flags ==
          (VSR_IO_SEND_ZERO_COPY | VSR_IO_SEND_VECTORED));
    CHECK(log_count(a, VSR_IO_SQE_RECV) == 0);
    CHECK(a->updates == 0);
    op = forwarded_take(a, VSR_IO_OP_HANDSHAKE);
    CHECK(op != NULL && op->op.replica == NULL);
    handshake = *(const struct vsr_io_handshake *)op->op.op.data;
    op_a = op->op.op.id;
    CHECK(op_a != 0);
    CHECK(handshake.direction == VSR_IO_OUTBOUND && handshake.node == 2);
    CHECK(handshake.fd == world.socks[sock_by_fd(0, handshake.fd)].raw_fd);
    CHECK(handshake.peer.length == b->listen.length);
    CHECK(links_in_state(a, VSR_IO_LINK_EXTERNAL) == 1);
    CHECK(node_state(a, 2) == VSR_IO_NODE_PENDING);
    /* B: the preamble read exactly (a plain 8-byte RECV, re-issued for
     * the sliced bytes), then HANDSHAKE INBOUND with no node. */
    CHECK(log_count(b, VSR_IO_SQE_RECV) >= 1);
    CHECK(log_last(b, VSR_IO_SQE_RECV)->op_flags == 0);
    CHECK(log_last(b, VSR_IO_SQE_RECV)->length <= VSR_IO_PREAMBLE_BYTES);
    CHECK(log_count(b, VSR_IO_SQE_SEND) == 0);
    op = forwarded_take(b, VSR_IO_OP_HANDSHAKE);
    CHECK(op != NULL);
    handshake = *(const struct vsr_io_handshake *)op->op.op.data;
    op_b = op->op.op.id;
    CHECK(handshake.direction == VSR_IO_INBOUND && handshake.node == 0);
    CHECK(links_in_state(b, VSR_IO_LINK_EXTERNAL) == 1);
    /* The caller's protocol runs on the raw descriptors meanwhile: the
     * engines armed nothing on them, so bytes pass untouched. */
    {
        uint32_t sa = sock_by_fd(b->index, handshake.fd);

        CHECK(sa != NONE && !world.socks[sa].recv_armed);
        CHECK(world.socks[sa].peer != NONE &&
              !world.socks[world.socks[sa].peer].recv_armed);
    }
    /* Completions: a wrong node for OUTBOUND closes; an unknown id is
     * EINVAL; a refusal closes with the raw descriptor CLOSEd. */
    done.node = 3;
    CHECK(vsr_io_links_handshake_done(a->io, 999, VSR_IO_OK, &done) ==
          VSR_EINVAL);
    CHECK(vsr_io_links_handshake_done(a->io, 0, VSR_IO_OK, &done) ==
          VSR_EINVAL);
    log_clear(a);
    CHECK(vsr_io_links_handshake_done(a->io, op_a, VSR_IO_OK, &done) == VSR_OK);
    CHECK(vsr_io_links_handshake_done(a->io, op_a, VSR_IO_OK, &done) ==
          VSR_EINVAL);
    world_settle();
    CHECK(log_count(a, VSR_IO_SQE_CLOSE) == 1);
    CHECK((log_last(a, VSR_IO_SQE_CLOSE)->flags & VSR_IO_SQE_FIXED_FILE) == 0);
    check_quiet(a);
    CHECK(a->io->links.nodes[0].last_error == -EACCES);
    CHECK(a->io->links.nodes[0].attempts == 1);
    /* B's caller refuses: closed the same way, no update_file. */
    log_clear(b);
    CHECK(vsr_io_links_handshake_done(b->io, op_b, VSR_IO_FAILED, NULL) ==
          VSR_OK);
    world_settle();
    CHECK(log_count(b, VSR_IO_SQE_CLOSE) == 1);
    CHECK(b->updates == 0);
    check_quiet(b);
    /* Second round after the backoff: both callers accept. A's caller
     * names node 2, B's names node 1; the descriptors are installed and
     * the links established with receives armed on the slots. */
    world_advance(BACKOFF_NS);
    world_settle();
    op = forwarded_take(a, VSR_IO_OP_HANDSHAKE);
    CHECK(op != NULL);
    op_a = op->op.op.id;
    op = forwarded_take(b, VSR_IO_OP_HANDSHAKE);
    CHECK(op != NULL);
    op_b = op->op.op.id;
    CHECK(op_a == 2 && op_b == 2);
    done.node = 2;
    log_clear(a);
    log_clear(b);
    CHECK(vsr_io_links_handshake_done(a->io, op_a, VSR_IO_OK, &done) == VSR_OK);
    done.node = 1;
    CHECK(vsr_io_links_handshake_done(b->io, op_b, VSR_IO_OK, &done) == VSR_OK);
    world_settle();
    CHECK(a->updates == 1 && b->updates == 1);
    CHECK(log_count(a, VSR_IO_SQE_RECV) == 1);
    CHECK(log_last(a, VSR_IO_SQE_RECV)->flags ==
          (VSR_IO_SQE_FIXED_FILE | VSR_IO_SQE_BUFFER_SELECT));
    CHECK(log_count(a, VSR_IO_SQE_SEND) == 0 &&
          log_count(b, VSR_IO_SQE_SEND) == 0);
    CHECK(link_to(a, 2, VSR_IO_OUTBOUND, VSR_IO_LINK_ESTABLISHED) != NULL);
    CHECK(link_to(b, 1, VSR_IO_INBOUND, VSR_IO_LINK_ESTABLISHED) != NULL);
    CHECK(node_state(a, 2) == VSR_IO_NODE_LINKED);
    CHECK(node_state(b, 1) == VSR_IO_NODE_LINKED);
    CHECK(a->io->links.nodes[0].attempts == 0);
    /* An unknown node from B's caller on a fresh inbound link closes it
     * (a raw peer this time), and a bad preamble never reaches the
     * caller. */
    peer = peer_connect(b);
    world_settle();
    CHECK(forwarded_take(b, VSR_IO_OP_HANDSHAKE) == NULL);
    put_preamble(bytes);
    peer_write(peer, bytes, VSR_IO_PREAMBLE_BYTES);
    world_settle();
    op = forwarded_take(b, VSR_IO_OP_HANDSHAKE);
    CHECK(op != NULL);
    done.node = 42;
    CHECK(vsr_io_links_handshake_done(b->io, op->op.op.id, VSR_IO_OK, &done) ==
          VSR_OK);
    world_settle();
    CHECK(peer_eof(peer));
    CHECK(links_in_state(b, VSR_IO_LINK_ESTABLISHED) == 1);
    peer_close(peer);
    peer = peer_connect(b);
    world_settle();
    memset(bytes, 'x', VSR_IO_PREAMBLE_BYTES);
    peer_write(peer, bytes, VSR_IO_PREAMBLE_BYTES);
    world_settle();
    CHECK(forwarded_take(b, VSR_IO_OP_HANDSHAKE) == NULL);
    CHECK(peer_eof(peer));
    CHECK(b->io->stats.frames_rejected == 1);
    peer_close(peer);
    /* A revoke while the caller holds a descriptor: the link is CLOSING
     * but the CLOSE waits for the completion, then goes out. */
    peer = peer_connect(b);
    world_settle();
    put_preamble(bytes);
    peer_write(peer, bytes, VSR_IO_PREAMBLE_BYTES);
    world_settle();
    op = forwarded_take(b, VSR_IO_OP_HANDSHAKE);
    CHECK(op != NULL);
    vsr_io_links_shutdown(b->io);
    world_settle();
    CHECK(links_in_state(b, VSR_IO_LINK_CLOSING) == 1);
    CHECK(!peer_eof(peer));
    done.node = 1;
    CHECK(vsr_io_links_handshake_done(b->io, op->op.op.id, VSR_IO_OK, &done) ==
          VSR_OK);
    world_settle();
    CHECK(peer_eof(peer));
    check_quiet(b);
    CHECK(b->io->links.listeners == 0);
    CHECK(vsr_io_links_adopt(b->io, 900, 1, 0) == VSR_EINVAL);
    peer_close(peer);
    engine_forget(a);
    engine_forget(b);
}

/* -------------------------------------------------------------------------
 * Adopt, revoke, address change, shutdown, slot pressure
 * ---------------------------------------------------------------------- */

static void test_adopt_and_close(void)
{
    struct engine *a;
    uint32_t peer;
    uint32_t engine_side;
    unsigned char bytes[128];
    size_t n;
    struct vsr_io_address moved;

    world_reset(8);
    a = engine_open(0, 1, VSR_IO_HANDSHAKE_TRUSTED);
    CHECK(vsr_io_links_node_set(a->io, 2, NULL) == VSR_OK);
    CHECK(vsr_io_links_authorize(a->io, cluster, 2, 2) == VSR_OK);
    world_settle();
    CHECK(forwarded_take(a, VSR_IO_OP_LINK_WANTED) != NULL);
    /* flags 0: established at once, installed, receive armed. */
    peer = sock_alloc(TEST_OWNER);
    engine_side = sock_alloc(0);
    world.socks[peer].raw_fd = fd_alloc();
    world.socks[engine_side].raw_fd = fd_alloc();
    world.socks[peer].peer = engine_side;
    world.socks[engine_side].peer = peer;
    log_clear(a);
    CHECK(vsr_io_links_adopt(a->io, world.socks[engine_side].raw_fd, 2, 0) ==
          VSR_OK);
    CHECK(node_state(a, 2) == VSR_IO_NODE_LINKED);
    CHECK(a->updates == 1);
    world_settle();
    CHECK(log_count(a, VSR_IO_SQE_SEND) == 0);
    CHECK(log_count(a, VSR_IO_SQE_RECV) == 1);
    CHECK(peer_read(peer, bytes, sizeof(bytes)) == 0);
    /* Revoking the only authorization closes the link and ends the want;
     * the peer reads EOF. */
    CHECK(vsr_io_links_authorize(a->io, cluster, 2, VSR_IO_NO_NODE) == VSR_OK);
    CHECK(links_in_state(a, VSR_IO_LINK_CLOSING) == 1);
    world_settle();
    CHECK(peer_eof(peer));
    check_quiet(a);
    CHECK(!a->io->links.nodes[0].wanted);
    world_advance(100 * BACKOFF_NS);
    world_settle();
    CHECK(forwarded_take(a, VSR_IO_OP_LINK_WANTED) == NULL);
    peer_close(peer);
    /* Inbound adopt with HANDSHAKE: the engine expects the preamble and
     * HELLO, then answers. A second authorization keeps the link across
     * the revoke of the first. */
    CHECK(vsr_io_links_authorize(a->io, cluster, 2, 2) == VSR_OK);
    world_settle();
    (void)forwarded_take(a, VSR_IO_OP_LINK_WANTED);
    peer = sock_alloc(TEST_OWNER);
    engine_side = sock_alloc(0);
    world.socks[peer].raw_fd = fd_alloc();
    world.socks[engine_side].raw_fd = fd_alloc();
    world.socks[peer].peer = engine_side;
    world.socks[engine_side].peer = peer;
    CHECK(vsr_io_links_adopt(a->io, world.socks[engine_side].raw_fd,
                             VSR_IO_NO_NODE, VSR_IO_ADOPT_HANDSHAKE) == VSR_OK);
    world_settle();
    CHECK(links_in_state(a, VSR_IO_LINK_HELLO) == 1);
    n = put_preamble(bytes);
    n += put_hello(bytes + n, VSR_IO_HANDSHAKE_TRUSTED, VSR_IO_PURPOSE_PEER, 2,
                   9);
    peer_write(peer, bytes, n);
    world_settle();
    CHECK(node_state(a, 2) == VSR_IO_NODE_LINKED);
    CHECK(peer_read(peer, bytes, sizeof(bytes)) == HELLO_BYTES);
    {
        struct vsr_id other = {3, 3};

        CHECK(vsr_io_links_authorize(a->io, other, 5, 2) == VSR_OK);
        CHECK(vsr_io_links_authorize(a->io, cluster, 2, VSR_IO_NO_NODE) ==
              VSR_OK);
        world_settle();
        CHECK(node_state(a, 2) == VSR_IO_NODE_LINKED);
        /* Moving the node to an address closes its links and dials. */
        address_of(&moved, 'Q');
        log_clear(a);
        CHECK(vsr_io_links_node_set(a->io, 2, &moved) == VSR_OK);
        world_settle();
        CHECK(peer_eof(peer));
        CHECK(log_count(a, VSR_IO_SQE_CONNECT) == 1);
        CHECK(node_state(a, 2) == VSR_IO_NODE_UNLINKED);
        CHECK(a->io->links.nodes[0].last_error == -ECONNREFUSED);
        CHECK(vsr_io_links_authorize(a->io, other, 5, VSR_IO_NO_NODE) ==
              VSR_OK);
    }
    peer_close(peer);
    world_settle();
    check_quiet(a);
    /* Clearing the node while a link is up closes it too. */
    CHECK(vsr_io_links_node_set(a->io, 2, NULL) == VSR_OK);
    CHECK(vsr_io_links_authorize(a->io, cluster, 2, 2) == VSR_OK);
    world_settle();
    (void)forwarded_take(a, VSR_IO_OP_LINK_WANTED);
    peer = sock_alloc(TEST_OWNER);
    engine_side = sock_alloc(0);
    world.socks[peer].raw_fd = fd_alloc();
    world.socks[engine_side].raw_fd = fd_alloc();
    world.socks[peer].peer = engine_side;
    world.socks[engine_side].peer = peer;
    CHECK(vsr_io_links_adopt(a->io, world.socks[engine_side].raw_fd, 2, 0) ==
          VSR_OK);
    world_settle();
    CHECK(vsr_io_links_node_clear(a->io, 2) == VSR_OK);
    world_settle();
    CHECK(peer_eof(peer));
    check_quiet(a);
    CHECK(vsr_io_links_lookup(&a->io->links, cluster, 2) == VSR_IO_NO_NODE);
    peer_close(peer);
    /* An install failure closes the adopted descriptor. */
    CHECK(vsr_io_links_node_set(a->io, 2, NULL) == VSR_OK);
    peer = sock_alloc(TEST_OWNER);
    engine_side = sock_alloc(0);
    world.socks[peer].raw_fd = fd_alloc();
    world.socks[engine_side].raw_fd = fd_alloc();
    world.socks[peer].peer = engine_side;
    world.socks[engine_side].peer = peer;
    a->fail_update = -EBADF;
    log_clear(a);
    CHECK(vsr_io_links_adopt(a->io, world.socks[engine_side].raw_fd, 2, 0) ==
          VSR_OK);
    world_settle();
    CHECK(log_count(a, VSR_IO_SQE_CLOSE) == 1);
    CHECK(peer_eof(peer));
    check_quiet(a);
    CHECK(a->io->links.nodes[0].last_error == -EBADF);
    peer_close(peer);
    /* Link table exhaustion: adopt is ELIMIT; an accepted connection with
     * no free link is closed again. */
    {
        uint32_t peers[LINKS + 1];

        for (uint32_t i = 0; i < LINKS; ++i) {
            peers[i] = peer_connect(a);
        }
        world_settle();
        CHECK(links_in_state(a, VSR_IO_LINK_HELLO) == LINKS);
        CHECK(vsr_io_links_adopt(a->io, 900, 2, 0) == VSR_ELIMIT);
        peers[LINKS] = peer_connect(a);
        world_settle();
        CHECK(peer_eof(peers[LINKS]));
        CHECK(a->io->links.orphans_count == 0);
        for (uint32_t i = 0; i < LINKS; ++i) {
            CHECK(!peer_eof(peers[i]));
        }
        /* Shutdown: the listener is cancelled and closed, every link is
         * torn down, every peer reads EOF, every slot is free. */
        log_clear(a);
        vsr_io_links_shutdown(a->io);
        world_settle();
        CHECK(log_count(a, VSR_IO_SQE_CANCEL) == LINKS + 1);
        CHECK(log_count(a, VSR_IO_SQE_CLOSE) == LINKS + 1);
        for (uint32_t i = 0; i <= LINKS; ++i) {
            CHECK(peer_eof(peers[i]));
            peer_close(peers[i]);
        }
        check_quiet(a);
        CHECK(a->io->links.listeners == 0);
        CHECK(a->io->links.listener_table[0].state == VSR_IO_LISTENER_FREE);
        CHECK(a->io->file_slots_free_count ==
              a->io->file_slot_next - FILE_SLOT_BASE);
        /* A connection after shutdown finds no listener. */
        CHECK(find_listener(&a->listen) == NONE);
    }
    engine_forget(a);
}

/* Listener failures are fatal (decision 45); a terminated accept is
 * re-armed; -ENOBUFS pauses a receive until the pool provides again. */
static void test_listener_and_pressure(void)
{
    struct engine *a;
    struct engine *b;
    uint32_t peer;
    struct sock *listener;
    uint32_t index;
    unsigned char bytes[128];
    size_t n;

    world_reset(9);
    /* B binds A's address first: A's BIND fails in the chain. */
    b = engine_open_at(1, 2, VSR_IO_HANDSHAKE_TRUSTED, 'A');
    world_settle();
    a = engine_open(0, 1, VSR_IO_HANDSHAKE_TRUSTED);
    world_settle();
    CHECK(a->io->stats.failure == -EADDRINUSE);
    CHECK(a->io->links.listener_table[0].state == VSR_IO_LISTENER_FREE);
    CHECK(a->io->links.listeners == 0);
    CHECK(a->io->slots.free_count == a->io->slots.count);
    engine_forget(a);
    engine_forget(b);
    /* An accept that terminates (a full completion queue on the ring, or
     * -EMFILE) is re-armed on the same listening socket. */
    world_reset(10);
    a = engine_open(0, 1, VSR_IO_HANDSHAKE_TRUSTED);
    CHECK(vsr_io_links_node_set(a->io, 2, NULL) == VSR_OK);
    world_settle();
    index = find_listener(&a->listen);
    CHECK(index != NONE);
    listener = &world.socks[index];
    listener->accept_armed = false;
    complete(a, listener->accept_ud, -EMFILE, 0, 0);
    log_clear(a);
    world_settle();
    CHECK(log_count(a, VSR_IO_SQE_ACCEPT) == 1);
    CHECK(log_count(a, VSR_IO_SQE_SOCKET) == 0);
    CHECK(listener->accept_armed);
    CHECK(a->io->links.listener_table[0].state == VSR_IO_LISTENER_ACTIVE);
    /* A positive result without MORE: accepted and re-armed. */
    peer = peer_connect(a);
    world_settle();
    CHECK(links_in_state(a, VSR_IO_LINK_HELLO) == 1);
    peer_close(peer);
    world_settle();
    check_quiet(a);
    /* Pool starvation: with the ring empty, a delivery ends the receive
     * with -ENOBUFS; once the pool provides again the receive is
     * re-armed and the bytes arrive. */
    peer = peer_connect(a);
    world_settle();
    a->ring_count = 0;
    a->ring_head = 0;
    while (a->io->pool.kernel_count > 0) {
        /* Return the kernel's slabs as if fully consumed by others. */
        for (uint32_t i = 0; i < SLABS; ++i) {
            if (a->io->pool.entries[i].state == VSR_IO_SLAB_KERNEL) {
                vsr_io_pool_recv_end(&a->io->pool, (uint16_t)i);
                break;
            }
        }
    }
    a->io->pool.ring_registered = false; /* Nothing provided meanwhile. */
    n = put_preamble(bytes);
    n += put_hello(bytes + n, VSR_IO_HANDSHAKE_TRUSTED, VSR_IO_PURPOSE_PEER, 2,
                   1);
    peer_write(peer, bytes, n);
    log_clear(a);
    world_settle();
    CHECK(a->io->links.links[0].recv_starved);
    CHECK(a->io->links.links[0].recv_slot == NONE);
    CHECK(a->io->links.links[0].state == VSR_IO_LINK_HELLO);
    CHECK(log_count(a, VSR_IO_SQE_RECV) == 0);
    CHECK(a->io->pool.starved);
    a->io->pool.ring_registered = true;
    world_settle();
    CHECK(log_count(a, VSR_IO_SQE_RECV) == 1);
    CHECK(node_state(a, 2) == VSR_IO_NODE_LINKED);
    peer_close(peer);
    world_settle();
    check_quiet(a);
    engine_forget(a);
}

/* Prepare stops at the batch capacity and the slot table and resumes;
 * a held NOTIF keeps the link CLOSING until it arrives; a short send is
 * continued. */
static void test_capacity(void)
{
    struct engine *a;
    struct engine *b;
    struct vsr_io_sqe sqes[SQ_CAP];
    uint32_t count;
    uint32_t peer;
    unsigned char bytes[128];
    size_t n;

    world_reset(11);
    world.short_send = 20;
    world.hold_notifs = true;
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    /* Too small a batch for the listener chain: nothing goes out. */
    vsr_io_links_prepare(a->io, sqes, 3, &count);
    CHECK(count == 0);
    CHECK(a->io->links.listener_table[0].state == VSR_IO_LISTENER_SETUP);
    world_settle();
    CHECK(a->io->links.listener_table[0].state == VSR_IO_LISTENER_ACTIVE);
    /* Short sends: the preamble and HELLO go out in 20-byte sends, each
     * continued from where the previous stopped; every NOTIF is held. */
    CHECK(vsr_io_links_authorize(a->io, cluster, 2, 2) == VSR_OK);
    log_clear(a);
    log_clear(b);
    world_settle();
    check_linked(a, b);
    CHECK(log_count(a, VSR_IO_SQE_SEND) == 4);
    CHECK(log_count(b, VSR_IO_SQE_SEND) == 3);
    CHECK(world.held_count == 7);
    {
        const struct vsr_io_link *out =
            link_to(a, 2, VSR_IO_OUTBOUND, VSR_IO_LINK_ESTABLISHED);
        uint32_t live = 0;

        for (uint32_t i = 0; i < VSR_IO_LINK_SENDS; ++i) {
            live += out->sends[i].slot != NONE ? 1 : 0;
        }
        /* Four sends awaited their NOTIF, at most VSR_IO_LINK_SENDS. */
        CHECK(live == 4 && out->inflight == 0);
        CHECK(out->notified_offset == 0 && out->sent_offset == 64);
    }
    /* Close with NOTIFs outstanding: CLOSING until they arrive. */
    engine_forget(b);
    world_settle();
    CHECK(links_in_state(a, VSR_IO_LINK_CLOSING) == 1);
    world_release_notifs();
    world_settle();
    check_quiet(a);
    CHECK(a->io->pool.free_count + a->io->pool.kernel_count == SLABS);
    /* Slot table pressure: with no free slot, a dial waits and resumes
     * once a slot frees. */
    world.short_send = 0;
    world.hold_notifs = false;
    b = engine_open(1, 2, VSR_IO_HANDSHAKE_TRUSTED);
    CHECK(vsr_io_links_node_set(b->io, 1, &a->listen) == VSR_OK);
    world_settle();
    {
        uint32_t taken[256];
        uint32_t taken_count = 0;

        while (a->io->slots.free_count > 0) {
            taken[taken_count++] =
                vsr_io_slots_alloc(&a->io->slots, VSR_IO_SLOT_FILE, 1, 0, 0, 0);
        }
        CHECK(vsr_io_links_authorize(a->io, cluster, 2, 2) == VSR_OK);
        log_clear(a);
        world_settle();
        CHECK(log_count(a, VSR_IO_SQE_SOCKET) == 0);
        CHECK(links_in_state(a, VSR_IO_LINK_CONNECTING) == 1);
        for (uint32_t i = 0; i < taken_count; ++i) {
            vsr_io_slots_free(&a->io->slots, taken[i]);
        }
        world_settle();
        CHECK(link_to(a, 2, VSR_IO_OUTBOUND, VSR_IO_LINK_ESTABLISHED) != NULL);
    }
    /* Bytes arriving for a closing link are dropped, not carved, and the
     * slab they landed in goes back to the pool. */
    peer = peer_connect(a);
    world_settle();
    n = put_preamble(bytes);
    peer_write(peer, bytes, n);
    world_settle();
    vsr_io_links_shutdown(a->io);
    n = put_hello(bytes, VSR_IO_HANDSHAKE_TRUSTED, VSR_IO_PURPOSE_PEER, 2, 1);
    peer_write(peer, bytes, n);
    world_settle();
    check_quiet(a);
    CHECK(peer_eof(peer));
    CHECK(a->io->pool.free_count + a->io->pool.kernel_count == SLABS);
    peer_close(peer);
    engine_forget(a);
    engine_forget(b);
}

/* -------------------------------------------------------------------------
 * Receive framing and MESSAGE delivery
 * ---------------------------------------------------------------------- */

#define REGIONS 4u
#define REGION_BYTES 8192u
#define OPERATIONS 128u
#define SCRATCH_BYTES ((size_t)2 * PAGE)
#define FRAME_FIXED                                                            \
    160u /* Header 24, envelope 56, PREPARE 8, ENTRIES 8,
                            ENTRY 56, BLOB 8. */
#define PAYLOAD_MAX (PAGE - FRAME_FIXED) /* The frame then fills a slab. */
#define WALK_ROUNDS 160u
#define WALK_BATCH_BYTES ((size_t)3 * PAGE)
#define WALK_PENDING 64u

static struct vsr_io_lease leases[ENGINES][REGIONS];
static struct vsr_io_queued_event messages[ENGINES][REGIONS];
static struct vsr_io_queued_event completions[ENGINES][OPERATIONS];
static _Alignas(16) unsigned char regions[ENGINES][REGIONS][REGION_BYTES];

/* A replica shell for delivery, the way tests/unit/engine_tables.c sets
 * one up: the tables are the test's. The limits admit a PREPARE whose
 * frame fills a slab. */
static struct vsr_io_replica *open_replica(struct engine *e, uint32_t index,
                                           struct vsr_id id)
{
    struct vsr_io_replica *replica = &e->io->replicas[index];
    size_t region = 0;

    CHECK(replica->io == e->io && replica->index == index);
    CHECK(replica->state == VSR_IO_REPLICA_FREE);
    replica->state = VSR_IO_REPLICA_RUNNING;
    replica->options.cluster = id;
    replica->options.limits.operations = OPERATIONS;
    replica->options.limits.members = 3;
    replica->options.limits.batch_entries = 4;
    replica->options.limits.spans_per_blob = 1;
    replica->options.limits.command_bytes = PAGE;
    replica->options.limits.result_bytes = 64;
    replica->options.limits.manifest_bytes = 64;
    replica->options.limits.message_bytes = PAGE;
    CHECK(vsr_io_codec_message_region(&replica->options.limits, &region) ==
          VSR_OK);
    CHECK(region <= REGION_BYTES);
    replica->regions_count = REGIONS;
    memset(leases[e->index], 0, sizeof(leases[e->index]));
    for (uint32_t i = 0; i < REGIONS; ++i) {
        leases[e->index][i].slab = NONE;
        leases[e->index][i].pin = NONE;
        leases[e->index][i].region.base = regions[e->index][i];
        leases[e->index][i].region.size = REGION_BYTES;
    }
    replica->leases = leases[e->index];
    replica->leases_free = REGIONS;
    memset(messages[e->index], 0, sizeof(messages[e->index]));
    replica->messages = messages[e->index];
    replica->messages_head = 0;
    replica->messages_count = 0;
    memset(completions[e->index], 0, sizeof(completions[e->index]));
    replica->completions = completions[e->index];
    replica->completions_head = 0;
    replica->completions_count = 0;
    e->io->replicas_count++;
    return replica;
}

/* The test's message: a PREPARE carrying one COMMAND entry of `size`
 * payload bytes derived from `number`, or an envelope-only PREPARE_OK
 * when size is NONE. */
struct sample {
    struct vsr_message message;
    struct vsr_prepare prepare;
    struct vsr_entry entry;
    struct vsr_blob blob;
    struct vsr_span span;
    unsigned char payload[PAGE];
};

static unsigned char payload_byte(uint64_t number, uint32_t i)
{
    return (unsigned char)(number * 31 + (uint64_t)i * 7 + 1);
}

static void sample_init(struct sample *s, struct vsr_id id, uint64_t from,
                        uint64_t number, uint32_t size)
{
    memset(s, 0, sizeof(*s));
    s->message.cluster = id;
    s->message.epoch = 1;
    s->message.view = 2;
    s->message.from = from;
    s->message.number = number;
    if (size == NONE) {
        s->message.type = VSR_MSG_PREPARE_OK;
        return;
    }
    CHECK(size <= PAYLOAD_MAX);
    s->message.type = VSR_MSG_PREPARE;
    s->message.body = &s->prepare;
    s->prepare.committed = number;
    s->prepare.batch.entries = &s->entry;
    s->prepare.batch.count = 1;
    s->entry.op = number;
    s->entry.epoch = 1;
    s->entry.view = 2;
    s->entry.request.client.hi = 7;
    s->entry.request.client.lo = number;
    s->entry.request.number = number;
    s->entry.type = VSR_REQUEST_COMMAND;
    s->entry.body = &s->blob;
    for (uint32_t i = 0; i < size; ++i) {
        s->payload[i] = payload_byte(number, i);
    }
    s->span.data = s->payload;
    s->span.size = size;
    s->blob.spans = size > 0 ? &s->span : NULL;
    s->blob.size = size;
    s->blob.count = size > 0 ? 1 : 0;
}

/* Frame bytes of a sample of `size`. */
static size_t frame_bytes(uint32_t size)
{
    if (size == NONE) {
        return VSR_IO_FRAME_HEADER_BYTES + sizeof(struct vsr_io_wire_message);
    }
    return FRAME_FIXED + ((size + 7u) & ~7u);
}

/* Encodes a MESSAGE frame, header and body, into `out` with the codec's
 * encoder; returns the frame's bytes. */
static size_t encode_message(unsigned char *out,
                             const struct vsr_message *message,
                             const struct vsr_limits *limits)
{
    static unsigned char headers[1024];
    struct vsr_io_writer writer = {headers, sizeof(headers), 0};
    struct vsr_io_encoder encoder;
    struct vsr_io_vec vecs[16];
    uint32_t length = 0;
    uint32_t crc = 0;
    uint32_t count = 0;
    bool done = false;
    size_t used = 0;

    CHECK(vsr_io_codec_message_digest(message, limits, &length, &crc) ==
          VSR_OK);
    vsr_io_encoder_begin(&encoder, message, length, crc);
    CHECK(vsr_io_encoder_emit(&encoder, &writer, vecs, 16, &count,
                              SCRATCH_BYTES, &done) == VSR_OK);
    CHECK(done && count > 0);
    for (uint32_t i = 0; i < count; ++i) {
        CHECK(used + vecs[i].length <= SCRATCH_BYTES);
        memcpy(out + used, vecs[i].base, vecs[i].length);
        used += vecs[i].length;
    }
    CHECK(used == VSR_IO_FRAME_HEADER_BYTES + length);
    return used;
}

/* A sample's frame into `out`. */
static size_t put_sample(unsigned char *out, const struct vsr_limits *limits,
                         struct vsr_id id, uint64_t from, uint64_t number,
                         uint32_t size)
{
    struct sample s;
    size_t n;

    sample_init(&s, id, from, number, size);
    n = encode_message(out, &s.message, limits);
    CHECK(n == frame_bytes(size));
    return n;
}

/* The next delivered MESSAGE of the replica, checked against the sample
 * it must be; returns its lease. */
static uint32_t take_message(struct vsr_io_replica *replica, uint64_t from,
                             uint64_t number, uint32_t size)
{
    struct vsr_io_queued_event *queued;
    const struct vsr_message *m;
    uint32_t lease;

    CHECK(replica->messages_count > 0);
    queued = &replica->messages[replica->messages_head];
    replica->messages_head =
        (replica->messages_head + 1) % replica->regions_count;
    replica->messages_count--;
    CHECK(queued->event.type == VSR_EVENT_MESSAGE);
    CHECK(queued->kind == VSR_IO_EVENT_CORE);
    lease = queued->lease;
    CHECK(lease < REGIONS && replica->leases[lease].state == 1);
    CHECK(queued->event.lease == vsr_io_lease_id(replica, lease));
    m = queued->event.data;
    CHECK(m->cluster.hi == replica->options.cluster.hi);
    CHECK(m->cluster.lo == replica->options.cluster.lo);
    CHECK(m->from == from && m->number == number);
    CHECK(m->epoch == 1 && m->view == 2);
    if (size == NONE) {
        CHECK(m->type == VSR_MSG_PREPARE_OK && m->body == NULL);
        return lease;
    }
    {
        const struct vsr_prepare *p = m->body;
        const struct vsr_blob *blob;

        CHECK(m->type == VSR_MSG_PREPARE && p != NULL);
        CHECK(p->committed == number && p->batch.count == 1);
        CHECK(p->batch.entries[0].op == number);
        CHECK(p->batch.entries[0].type == VSR_REQUEST_COMMAND);
        CHECK(p->batch.entries[0].request.client.lo == number);
        blob = p->batch.entries[0].body;
        CHECK(blob != NULL && blob->size == size);
        if (size == 0) {
            CHECK(blob->count == 0);
        } else {
            const unsigned char *data;

            CHECK(blob->count == 1 && blob->spans[0].size == size);
            data = blob->spans[0].data;
            /* Zero copy: the span points into the pool, at the lease's
             * slab. */
            CHECK(vsr_io_pool_contains(&replica->io->pool, data, size));
            CHECK(vsr_io_pool_locate(&replica->io->pool, data) ==
                  replica->leases[lease].slab);
            for (uint32_t i = 0; i < size; ++i) {
                CHECK(data[i] == payload_byte(number, i));
            }
        }
    }
    return lease;
}

/* Bytes the peer wrote that the engine has not received yet. */
static uint32_t peer_unread(uint32_t peer)
{
    const struct sock *sock = &world.socks[peer];

    CHECK(sock->used && sock->peer != NONE);
    return world.socks[sock->peer].inbox_len;
}

static uint32_t pool_refs(const struct engine *e)
{
    uint32_t refs = 0;

    for (uint32_t i = 0; i < SLABS; ++i) {
        refs += e->io->pool.entries[i].refs;
    }
    return refs;
}

/* The kernel retires the ring's n-th buffer after `bytes` more bytes, so
 * a delivery continues in the following one. */
static void ring_cut(struct engine *e, uint32_t n, uint32_t bytes)
{
    struct ring_entry *entry = &e->ring[(e->ring_head + n) % RING_ENTRIES];

    CHECK(n < e->ring_count && bytes > 0);
    CHECK(entry->consumed + bytes <= entry->length);
    entry->length = entry->consumed + bytes;
}

static uint32_t ring_room(const struct engine *e, uint32_t n)
{
    const struct ring_entry *entry =
        &e->ring[(e->ring_head + n) % RING_ENTRIES];

    CHECK(n < e->ring_count);
    return entry->length - entry->consumed;
}

/* A test-owned peer presenting itself as `node` links to e in TRUSTED
 * mode; e's HELLO is read off the peer. */
static uint32_t peer_link(struct engine *e, uint64_t node)
{
    unsigned char bytes[VSR_IO_PREAMBLE_BYTES + HELLO_BYTES];
    unsigned char hello[HELLO_BYTES];
    uint32_t peer = peer_connect(e);
    size_t n;

    n = put_preamble(bytes);
    n += put_hello(bytes + n, VSR_IO_HANDSHAKE_TRUSTED, VSR_IO_PURPOSE_PEER,
                   node, 5);
    peer_write(peer, bytes, n);
    world_settle();
    CHECK(link_to(e, node, VSR_IO_INBOUND, VSR_IO_LINK_ESTABLISHED) != NULL);
    CHECK(peer_read(peer, hello, sizeof(hello)) == HELLO_BYTES);
    return peer;
}

/* Engine 0 as node 2 with one replica of `cluster`, linked from a
 * test-owned peer that is node 1, authorized as replica 1. */
struct receiver {
    struct engine *e;
    struct vsr_io_replica *replica;
    const struct vsr_limits *limits;
    const struct vsr_io_link *link;
    uint32_t peer;
};

/* Read through a call so that a static checker cannot fold a comparison
 * across the world's steps. */
static uint64_t reassembled_count(const struct receiver *r)
{
    return r->e->io->links.reassembled;
}

static void receiver_relink(struct receiver *r)
{
    r->peer = peer_link(r->e, 1);
    r->link = link_to(r->e, 1, VSR_IO_INBOUND, VSR_IO_LINK_ESTABLISHED);
    CHECK(r->link != NULL);
}

static void receiver_open(struct receiver *r, uint64_t seed)
{
    world_reset(seed);
    r->e = engine_open(0, 2, VSR_IO_HANDSHAKE_TRUSTED);
    CHECK(vsr_io_links_node_set(r->e->io, 1, NULL) == VSR_OK);
    world_settle();
    receiver_relink(r);
    r->replica = open_replica(r->e, 0, cluster);
    r->limits = &r->replica->options.limits;
    CHECK(vsr_io_links_authorize(r->e->io, cluster, 1, 1) == VSR_OK);
    world_settle();
    CHECK(pool_refs(r->e) == 1); /* The link's send slab. */
}

/* Nothing held on the link and the pool's references are the link's send
 * slab plus `outstanding` message leases and `taken` slabs of the test. */
static void check_drained(const struct receiver *r, uint32_t outstanding,
                          uint32_t taken)
{
    const struct vsr_io_link *link = r->link;

    CHECK(link->state == VSR_IO_LINK_ESTABLISHED);
    CHECK(link->partial_length == 0 && link->partial_slab == NONE);
    CHECK(link->held_count == 0 && link->reassembly_slab == NONE);
    CHECK(!link->retry && r->e->io->links.retries_due == 0);
    CHECK(pool_refs(r->e) == 1 + outstanding + taken);
}

/* One frame written, delivered and released; `reassembled` says whether
 * it had to be copied. */
static void deliver_one(struct receiver *r, const unsigned char *frame,
                        size_t n, uint64_t number, uint32_t size,
                        bool reassembled)
{
    uint64_t before = r->e->io->links.reassembled;
    uint32_t lease;

    peer_write(r->peer, frame, n);
    world_settle();
    CHECK(r->e->io->links.reassembled == before + (reassembled ? 1 : 0));
    CHECK(r->replica->messages_count == 1);
    lease = take_message(r->replica, 1, number, size);
    CHECK(pool_refs(r->e) == 2);
    vsr_io_lease_release(r->replica, lease);
    check_drained(r, 0, 0);
    CHECK(r->e->io->links.nodes[0].last_received_ns == world.now);
}

/* Frames split at every byte boundary across two and three slabs,
 * including the header's, with and without random slicing within a
 * slab. */
static void test_receive_splits(void)
{
    struct receiver r;
    unsigned char frame[SCRATCH_BYTES];
    uint64_t number = 0;
    size_t na = frame_bytes(100);
    size_t nb = frame_bytes(NONE);
    size_t n;

    receiver_open(&r, 21);
    /* One frame across two slabs, every boundary. */
    for (uint32_t slice = 0; slice < 2; ++slice) {
        world.slice_max = slice == 0 ? 0 : 7;
        for (uint32_t p = 1; p < na; ++p) {
            n = put_sample(frame, r.limits, cluster, 1, ++number, 100);
            ring_cut(r.e, 0, p);
            deliver_one(&r, frame, n, number, 100, true);
        }
    }
    world.slice_max = 0;
    /* A two-frame stream split at every byte: the second frame is
     * decoded in place in the second slab (a split exactly between the
     * frames copies nothing). */
    for (uint32_t p = 1; p < na + nb; ++p) {
        uint64_t before = r.e->io->links.reassembled;
        uint32_t lease;

        n = put_sample(frame, r.limits, cluster, 1, number + 1, 100);
        n += put_sample(frame + n, r.limits, cluster, 1, number + 2, NONE);
        ring_cut(r.e, 0, p);
        peer_write(r.peer, frame, n);
        world_settle();
        CHECK(r.e->io->links.reassembled == before + (p == na ? 0 : 1));
        CHECK(r.replica->messages_count == 2);
        lease = take_message(r.replica, 1, number + 1, 100);
        vsr_io_lease_release(r.replica, lease);
        lease = take_message(r.replica, 1, number + 2, NONE);
        vsr_io_lease_release(r.replica, lease);
        check_drained(&r, 0, 0);
        number += 2;
    }
    /* Three slabs: the second holds q bytes of the frame. */
    for (uint32_t p = 1; p < na; ++p) {
        static const uint32_t cuts[] = {1, 7, 8, 9, 16, 23, 24, 25, 56, 100};

        for (uint32_t c = 0; c < sizeof(cuts) / sizeof(cuts[0]); ++c) {
            uint32_t q = cuts[c];

            if (p + q >= na) {
                continue;
            }
            n = put_sample(frame, r.limits, cluster, 1, ++number, 100);
            ring_cut(r.e, 0, p);
            ring_cut(r.e, 1, q);
            deliver_one(&r, frame, n, number, 100, true);
        }
    }
    /* The header alone across three slabs, one byte in the middle. */
    for (uint32_t p = 1; p + 1 < VSR_IO_FRAME_HEADER_BYTES; ++p) {
        n = put_sample(frame, r.limits, cluster, 1, ++number, 8);
        ring_cut(r.e, 0, p);
        ring_cut(r.e, 1, 1);
        deliver_one(&r, frame, n, number, 8, true);
    }
    peer_close(r.peer);
    world_settle();
    check_quiet(r.e);
    CHECK(pool_refs(r.e) == 0);
    engine_forget(r.e);
}

/* Many frames in one slab, a frame ending exactly at a slab's end, and
 * frames that fill a whole slab. */
static void test_receive_fill(void)
{
    struct receiver r;
    static unsigned char frame[INBOX_BYTES];
    uint64_t number = 0;
    uint64_t before;
    uint32_t lease;
    uint32_t room;
    size_t n = 0;

    receiver_open(&r, 22);
    /* 40 envelope-only frames in one write, all within the slab: they are
     * delivered in order, REGIONS at a time, the rest waiting in place. */
    for (uint32_t i = 0; i < 40; ++i) {
        n += put_sample(frame + n, r.limits, cluster, 1, ++number, NONE);
    }
    CHECK(n <= ring_room(r.e, 0));
    before = r.e->io->links.reassembled;
    peer_write(r.peer, frame, n);
    for (uint64_t taken = 0; taken < 40;) {
        world_settle();
        CHECK(r.replica->messages_count ==
              (40 - taken < REGIONS ? 40 - taken : REGIONS));
        CHECK(r.link->retry == (40 - taken > REGIONS));
        CHECK(pool_refs(r.e) ==
              1 + r.replica->messages_count + (r.link->retry ? 1 : 0));
        while (r.replica->messages_count > 0) {
            lease = take_message(r.replica, 1, ++taken, NONE);
            vsr_io_lease_release(r.replica, lease);
        }
    }
    world_settle();
    CHECK(reassembled_count(&r) == before);
    check_drained(&r, 0, 0);
    /* A frame ending exactly at the slab's end needs no copy; the frame
     * after it starts the next slab. */
    room = ring_room(r.e, 0);
    CHECK(room > FRAME_FIXED + 8 && room % 8 == 0);
    n = put_sample(frame, r.limits, cluster, 1, number + 1, room - FRAME_FIXED);
    CHECK(n == room);
    n += put_sample(frame + n, r.limits, cluster, 1, number + 2, NONE);
    peer_write(r.peer, frame, n);
    world_settle();
    CHECK(reassembled_count(&r) == before);
    CHECK(r.replica->messages_count == 2);
    lease = take_message(r.replica, 1, number + 1, room - FRAME_FIXED);
    vsr_io_lease_release(r.replica, lease);
    lease = take_message(r.replica, 1, number + 2, NONE);
    vsr_io_lease_release(r.replica, lease);
    check_drained(&r, 0, 0);
    number += 2;
    /* Two slab-sized frames back to back: each straddles and is copied
     * whole; the payload arrives byte-identical. */
    CHECK(frame_bytes(PAYLOAD_MAX) == PAGE);
    n = put_sample(frame, r.limits, cluster, 1, number + 1, PAYLOAD_MAX);
    n += put_sample(frame + n, r.limits, cluster, 1, number + 2, PAYLOAD_MAX);
    CHECK(ring_room(r.e, 0) < PAGE);
    peer_write(r.peer, frame, n);
    world_settle();
    CHECK(r.e->io->links.reassembled == before + 2);
    CHECK(r.replica->messages_count == 2);
    lease = take_message(r.replica, 1, number + 1, PAYLOAD_MAX);
    vsr_io_lease_release(r.replica, lease);
    lease = take_message(r.replica, 1, number + 2, PAYLOAD_MAX);
    vsr_io_lease_release(r.replica, lease);
    check_drained(&r, 0, 0);
    number += 2;
    /* A slab-sized frame that starts a fresh slab fills it exactly and is
     * decoded in place. */
    room = ring_room(r.e, 0);
    n = put_sample(frame, r.limits, cluster, 1, number + 1, room - FRAME_FIXED);
    n += put_sample(frame + n, r.limits, cluster, 1, number + 2, PAYLOAD_MAX);
    peer_write(r.peer, frame, n);
    world_settle();
    CHECK(r.e->io->links.reassembled == before + 2);
    CHECK(r.replica->messages_count == 2);
    lease = take_message(r.replica, 1, number + 1, room - FRAME_FIXED);
    vsr_io_lease_release(r.replica, lease);
    lease = take_message(r.replica, 1, number + 2, PAYLOAD_MAX);
    vsr_io_lease_release(r.replica, lease);
    check_drained(&r, 0, 0);
    peer_close(r.peer);
    world_settle();
    check_quiet(r.e);
    CHECK(pool_refs(r.e) == 0);
    engine_forget(r.e);
}

/* Every free slab taken by the test, until released. */
static uint32_t pool_dry(struct engine *e, uint32_t *taken)
{
    uint32_t count = 0;

    for (;;) {
        uint32_t slab = vsr_io_pool_acquire(&e->io->pool, false);

        if (slab == NONE) {
            break;
        }
        CHECK(count < SLABS);
        taken[count++] = slab;
    }
    CHECK(e->io->pool.free_count == 0);
    return count;
}

static void pool_refill(struct engine *e, uint32_t *taken, uint32_t count)
{
    for (uint32_t i = 0; i < count; ++i) {
        vsr_io_pool_release(&e->io->pool, taken[i]);
    }
}

/* Delivery refused and retried at poll, the reassembly slab unavailable
 * then available, refusals of every kind, and closing with bytes held. */
static void test_receive_pressure(void)
{
    struct receiver r;
    unsigned char frame[SCRATCH_BYTES];
    uint32_t taken[SLABS];
    uint32_t taken_count;
    uint32_t held[REGIONS];
    uint64_t number = 0;
    uint64_t rejected = 0;
    uint64_t before;
    uint32_t lease;
    size_t n;

    receiver_open(&r, 23);
    /* The replica's regions all leased: the next frame stays in place,
     * a poll after a lease release delivers it. */
    for (uint32_t i = 0; i < REGIONS; ++i) {
        n = put_sample(frame, r.limits, cluster, 1, ++number, 16);
        peer_write(r.peer, frame, n);
        world_settle();
        held[i] = take_message(r.replica, 1, number, 16);
    }
    CHECK(r.replica->leases_free == 0);
    n = put_sample(frame, r.limits, cluster, 1, ++number, 16);
    peer_write(r.peer, frame, n);
    world_settle();
    CHECK(r.replica->messages_count == 0);
    CHECK(r.link->retry && r.e->io->links.retries_due == 1);
    CHECK(r.link->partial_length == n);
    CHECK(pool_refs(r.e) == 1 + REGIONS + 1);
    engine_step(r.e);
    CHECK(r.link->retry && r.replica->messages_count == 0);
    vsr_io_lease_release(r.replica, held[0]);
    engine_step(r.e);
    CHECK(!r.link->retry && r.e->io->links.retries_due == 0);
    CHECK(r.replica->messages_count == 1);
    lease = take_message(r.replica, 1, number, 16);
    vsr_io_lease_release(r.replica, lease);
    for (uint32_t i = 1; i < REGIONS; ++i) {
        vsr_io_lease_release(r.replica, held[i]);
    }
    check_drained(&r, 0, 0);
    /* The same with the frame straddling: reassembled, then blocked. */
    for (uint32_t i = 0; i < REGIONS; ++i) {
        n = put_sample(frame, r.limits, cluster, 1, ++number, 16);
        peer_write(r.peer, frame, n);
        world_settle();
        held[i] = take_message(r.replica, 1, number, 16);
    }
    n = put_sample(frame, r.limits, cluster, 1, ++number, 16);
    ring_cut(r.e, 0, 40);
    peer_write(r.peer, frame, n);
    world_settle();
    CHECK(r.replica->messages_count == 0 && r.link->retry);
    CHECK(r.link->reassembly_slab == r.link->partial_slab);
    CHECK(r.link->partial_length == n && r.link->held_count == 0);
    CHECK(pool_refs(r.e) == 1 + REGIONS + 1);
    for (uint32_t i = 0; i < REGIONS; ++i) {
        vsr_io_lease_release(r.replica, held[i]);
    }
    engine_step(r.e);
    lease = take_message(r.replica, 1, number, 16);
    CHECK(r.replica->leases[lease].slab == r.link->reassembly_slab ||
          r.link->reassembly_slab == NONE);
    vsr_io_lease_release(r.replica, lease);
    check_drained(&r, 0, 0);
    /* The pool dry: a straddling frame waits with both runs held; a slab
     * freed later lets poll reassemble and deliver it. */
    before = r.e->io->links.reassembled;
    taken_count = pool_dry(r.e, taken);
    CHECK(taken_count > 0);
    n = put_sample(frame, r.limits, cluster, 1, ++number, 200);
    ring_cut(r.e, 0, 100);
    peer_write(r.peer, frame, n);
    world_settle();
    CHECK(r.replica->messages_count == 0 && r.link->retry);
    CHECK(r.link->partial_length == 100 && r.link->held_count == 1);
    CHECK(r.link->held[0].length == n - 100);
    CHECK(r.link->reassembly_slab == NONE);
    CHECK(reassembled_count(&r) == before);
    CHECK(pool_refs(r.e) == 1 + taken_count + 2);
    engine_step(r.e);
    CHECK(r.link->retry && r.link->held_count == 1);
    vsr_io_pool_release(&r.e->io->pool, taken[--taken_count]);
    engine_step(r.e);
    CHECK(r.e->io->links.reassembled == before + 1);
    CHECK(r.replica->messages_count == 1 && !r.link->retry);
    lease = take_message(r.replica, 1, number, 200);
    vsr_io_lease_release(r.replica, lease);
    check_drained(&r, 0, taken_count);
    pool_refill(r.e, taken, taken_count);
    world_settle();
    check_drained(&r, 0, 0);
    /* Refusals that drop the frame and keep the link: an unauthorized
     * sender, a cluster without a replica, a body shorter than its
     * envelope. */
    n = put_sample(frame, r.limits, cluster, 3, ++number, 16);
    peer_write(r.peer, frame, n);
    world_settle();
    CHECK(r.e->io->stats.frames_rejected == ++rejected);
    CHECK(r.replica->messages_count == 0);
    check_drained(&r, 0, 0);
    {
        struct vsr_id other = {9, 9};

        n = put_sample(frame, r.limits, other, 1, ++number, 16);
        ring_cut(r.e, 0, 30);
        peer_write(r.peer, frame, n);
        world_settle();
        CHECK(r.e->io->stats.frames_rejected == ++rejected);
        CHECK(r.replica->messages_count == 0);
        check_drained(&r, 0, 0);
    }
    memset(frame + VSR_IO_FRAME_HEADER_BYTES, 0, 8);
    vsr_io_codec_put_frame(
        frame, VSR_IO_FRAME_MESSAGE, 8,
        vsr_io_crc32c(0, frame + VSR_IO_FRAME_HEADER_BYTES, 8));
    peer_write(r.peer, frame, VSR_IO_FRAME_HEADER_BYTES + 8);
    world_settle();
    CHECK(r.e->io->stats.frames_rejected == ++rejected);
    check_drained(&r, 0, 0);
    /* A frame the replica's decoder rejects (a truncated body under a
     * valid CRC) is dropped by the engine and counted. */
    n = put_sample(frame, r.limits, cluster, 1, ++number, 16);
    vsr_io_codec_put_frame(
        frame, VSR_IO_FRAME_MESSAGE, (uint32_t)(n - 24 - 8),
        vsr_io_crc32c(0, frame + VSR_IO_FRAME_HEADER_BYTES, n - 24 - 8));
    peer_write(r.peer, frame, n - 8);
    world_settle();
    CHECK(r.e->io->stats.frames_rejected == ++rejected);
    check_drained(&r, 0, 0);
    /* A delivered frame after the refusals: the link is unharmed. */
    n = put_sample(frame, r.limits, cluster, 1, ++number, 16);
    deliver_one(&r, frame, n, number, 16, false);
    /* A bad body CRC in a reassembled frame closes the link (-EBADMSG),
     * with every reference released. */
    n = put_sample(frame, r.limits, cluster, 1, ++number, 100);
    frame[n - 1] ^= 0x40;
    ring_cut(r.e, 0, 50);
    peer_write(r.peer, frame, n);
    world_settle();
    CHECK(r.e->io->stats.frames_rejected == ++rejected);
    CHECK(r.e->io->links.nodes[0].last_error == -EBADMSG);
    CHECK(peer_eof(r.peer));
    peer_close(r.peer);
    world_settle();
    check_quiet(r.e);
    CHECK(pool_refs(r.e) == 0);
    /* An oversized length in a header reassembled across two slabs. */
    receiver_relink(&r);
    vsr_io_codec_put_frame(frame, VSR_IO_FRAME_MESSAGE, PAGE - 24 + 8, 0);
    ring_cut(r.e, 0, 10);
    peer_write(r.peer, frame, VSR_IO_FRAME_HEADER_BYTES);
    world_settle();
    CHECK(r.e->io->stats.frames_rejected == ++rejected);
    CHECK(r.e->io->links.nodes[0].last_error == -EBADMSG);
    peer_close(r.peer);
    world_settle();
    check_quiet(r.e);
    CHECK(pool_refs(r.e) == 0);
    /* A frame the peer never finishes, held across slabs with the pool
     * dry: closing the link returns every reference. */
    receiver_relink(&r);
    taken_count = pool_dry(r.e, taken);
    n = put_sample(frame, r.limits, cluster, 1, ++number, 300);
    ring_cut(r.e, 0, 60);
    ring_cut(r.e, 1, 60);
    peer_write(r.peer, frame, n - 8);
    world_settle();
    CHECK(r.link->partial_length == 60 && r.link->held_count == 2);
    CHECK(pool_refs(r.e) == 1 + taken_count + 3);
    vsr_io_links_close(r.e->io, (uint32_t)(r.link - r.e->io->links.links), 0);
    CHECK(r.e->io->links.retries_due == 0);
    CHECK(pool_refs(r.e) == 1 + taken_count);
    world_settle();
    pool_refill(r.e, taken, taken_count);
    peer_close(r.peer);
    world_settle();
    check_quiet(r.e);
    CHECK(pool_refs(r.e) == 0);
    /* A whole frame blocked on a region, and bytes behind it, at close. */
    receiver_relink(&r);
    for (uint32_t i = 0; i < REGIONS; ++i) {
        n = put_sample(frame, r.limits, cluster, 1, ++number, 16);
        peer_write(r.peer, frame, n);
        world_settle();
        held[i] = take_message(r.replica, 1, number, 16);
    }
    n = put_sample(frame, r.limits, cluster, 1, ++number, 16);
    n += put_sample(frame + n, r.limits, cluster, 1, ++number, 16);
    ring_cut(r.e, 0, (uint32_t)n - 8);
    peer_write(r.peer, frame, n);
    world_settle();
    CHECK(r.link->retry && r.link->held_count == 1);
    CHECK(pool_refs(r.e) == 1 + REGIONS + 2);
    vsr_io_links_close(r.e->io, (uint32_t)(r.link - r.e->io->links.links),
                       -ECONNRESET);
    CHECK(pool_refs(r.e) == 1 + REGIONS);
    CHECK(r.e->io->links.retries_due == 0);
    for (uint32_t i = 0; i < REGIONS; ++i) {
        vsr_io_lease_release(r.replica, held[i]);
    }
    peer_close(r.peer);
    world_settle();
    check_quiet(r.e);
    CHECK(pool_refs(r.e) == 0);
    /* Closing the second link of the node leaves the messages already
     * delivered untouched. */
    CHECK(r.replica->messages_count == 0);
    engine_forget(r.e);
}

/* Random MESSAGE sequences under random slicing, slab cuts, pool drought
 * and lease holding: every authorized message arrives exactly once, in
 * order, byte-identical, nothing else does, and the references balance. */
static void test_receive_random(uint64_t seed)
{
    struct receiver r;
    static unsigned char batch[WALK_BATCH_BYTES + PAGE];
    uint32_t pending[WALK_PENDING] = {0}; /* Frame sizes sent, in order. */
    uint32_t pending_head = 0;
    uint32_t pending_count = 0;
    uint32_t taken[SLABS];
    uint32_t taken_count = 0;
    uint32_t held[REGIONS];
    uint32_t held_count = 0;
    uint64_t sent = 0;
    uint64_t received = 0;
    uint64_t rejected = 0;
    uint64_t delivered_bytes = 0;

    printf("link: random walk seed %" PRIu64 "\n", seed);
    receiver_open(&r, seed);
    for (uint32_t round = 0; round < WALK_ROUNDS; ++round) {
        struct test_random *random = &world.random;
        uint32_t frames = 1 + test_random_bounded(random, 6);
        size_t used = 0;

        for (uint32_t f = 0; f < frames; ++f) {
            uint32_t roll = test_random_bounded(random, 12);
            uint32_t size;

            switch (test_random_bounded(random, 5)) {
            case 0:
                size = NONE;
                break;
            case 1:
                size = PAYLOAD_MAX - 8 * test_random_bounded(random, 4);
                break;
            case 2:
                size = test_random_bounded(random, 64);
                break;
            default:
                size = test_random_bounded(random, PAYLOAD_MAX + 1);
                break;
            }
            if (used + frame_bytes(size) > WALK_BATCH_BYTES ||
                pending_count == WALK_PENDING) {
                break;
            }
            if (roll == 0) {
                used +=
                    put_sample(batch + used, r.limits, cluster, 3, 77, size);
                rejected++;
            } else if (roll == 1) {
                struct vsr_id other = {9, 9};

                used += put_sample(batch + used, r.limits, other, 1, 78, size);
                rejected++;
            } else {
                used += put_sample(batch + used, r.limits, cluster, 1, ++sent,
                                   size);
                pending[(pending_head + pending_count++) % WALK_PENDING] = size;
            }
        }
        if (used == 0) {
            continue;
        }
        world.slice_max = test_random_bounded(random, 3) == 0
                              ? 0
                              : 1 + test_random_bounded(random, 500);
        if (test_random_bounded(random, 2) == 0) {
            uint32_t room = ring_room(r.e, 0);

            ring_cut(r.e, 0, 1 + test_random_bounded(random, room));
        }
        if (test_random_bounded(random, 3) == 0 && r.e->ring_count > 1) {
            ring_cut(r.e, 1,
                     1 + test_random_bounded(random, ring_room(r.e, 1)));
        }
        if (taken_count == 0 && test_random_bounded(random, 4) == 0) {
            taken_count = pool_dry(r.e, taken);
        }
        peer_write(r.peer, batch, used);
        /* Until the batch is through: delivered messages checked in order,
         * some leases kept; when stuck on a region or a slab, one is given
         * back. */
        for (uint32_t attempt = 0;; ++attempt) {
            CHECK(attempt < 96);
            world_settle();
            while (r.replica->messages_count > 0) {
                uint32_t size = pending[pending_head];
                uint32_t lease;

                CHECK(pending_count > 0);
                pending_head = (pending_head + 1) % WALK_PENDING;
                pending_count--;
                lease = take_message(r.replica, 1, ++received, size);
                delivered_bytes += frame_bytes(size);
                if (held_count < REGIONS - 1 &&
                    test_random_bounded(random, 3) == 0) {
                    held[held_count++] = lease;
                } else {
                    vsr_io_lease_release(r.replica, lease);
                }
            }
            CHECK(r.link->state == VSR_IO_LINK_ESTABLISHED);
            if (pending_count == 0 && peer_unread(r.peer) == 0 &&
                r.link->partial_length == 0 && r.link->held_count == 0 &&
                r.e->io->stats.frames_rejected == rejected) {
                break;
            }
            if (held_count > 0 &&
                (taken_count == 0 || test_random_bounded(random, 2) == 0)) {
                vsr_io_lease_release(r.replica, held[--held_count]);
            } else if (taken_count > 0) {
                vsr_io_pool_release(&r.e->io->pool, taken[--taken_count]);
            }
        }
        if (test_random_bounded(random, 3) == 0) {
            while (held_count > 0) {
                vsr_io_lease_release(r.replica, held[--held_count]);
            }
            pool_refill(r.e, taken, taken_count);
            taken_count = 0;
            world_settle();
            check_drained(&r, 0, 0);
        }
    }
    while (held_count > 0) {
        vsr_io_lease_release(r.replica, held[--held_count]);
    }
    pool_refill(r.e, taken, taken_count);
    world.slice_max = 0;
    world_settle();
    CHECK(received == sent && pending_count == 0);
    CHECK(r.e->io->stats.frames_rejected == rejected);
    CHECK(r.e->io->stats.bytes_received >= delivered_bytes);
    check_drained(&r, 0, 0);
    printf("link: random walk %" PRIu64 " messages, %" PRIu64
           " reassembled, %" PRIu64 " rejected\n",
           received, r.e->io->links.reassembled, rejected);
    peer_close(r.peer);
    world_settle();
    check_quiet(r.e);
    CHECK(pool_refs(r.e) == 0);
    engine_forget(r.e);
}

/* -------------------------------------------------------------------------
 * The send path: core SENDs over a two-engine world
 * ---------------------------------------------------------------------- */

#define SAMPLES 128u /* Outstanding SEND ops per engine, as a ring. */

static struct sample samples[ENGINES][SAMPLES];
static uint32_t samples_next[ENGINES];

/* A fresh sample of the engine's ring, from the engine's own node id (the
 * tests authorize replica n as node n), pinned until its op completes. */
static struct sample *sample_fresh(struct engine *e, uint64_t number,
                                   uint32_t size)
{
    struct sample *s = &samples[e->index][samples_next[e->index] % SAMPLES];

    samples_next[e->index]++;
    sample_init(s, cluster, e->node, number, size);
    return s;
}

/* A SEND op of the sample (op id = its number) to `member`. */
static int send_sample(struct engine *e, uint32_t index, const struct sample *s,
                       uint64_t member)
{
    return vsr_io_links_send(e->io, index, s->message.number, &s->message,
                             member);
}

static int send_fresh(struct engine *e, uint32_t index, uint64_t number,
                      uint32_t size, uint64_t member)
{
    return send_sample(e, index, sample_fresh(e, number, size), member);
}

/* Pops the next internal completion of the replica, false when none. */
static bool take_completion(struct vsr_io_replica *replica, uint64_t *op,
                            int32_t *status)
{
    const struct vsr_io_queued_event *queued;

    if (replica->completions_count == 0) {
        return false;
    }
    queued = &replica->completions[replica->completions_head];
    CHECK(queued->event.type == VSR_EVENT_COMPLETE);
    CHECK(queued->kind == VSR_IO_EVENT_CORE && queued->lease == NONE);
    CHECK(queued->event.data == NULL && queued->event.lease == 0);
    *op = queued->event.id;
    *status = queued->event.status;
    replica->completions_head =
        (replica->completions_head + 1) % replica->options.limits.operations;
    replica->completions_count--;
    return true;
}

/* The next completion must be `op` with `status`. */
static void expect_completion(struct vsr_io_replica *replica, uint64_t op,
                              int32_t status)
{
    uint64_t got_op = 0;
    int32_t got_status = 0;

    CHECK(take_completion(replica, &got_op, &got_status));
    CHECK(got_op == op && got_status == status);
}

/* Two linked engines, each with replica 0 of `cluster`, authorized as
 * replica 1 (node 1, engine a) and replica 2 (node 2, engine b). */
struct pair {
    struct engine *a;
    struct engine *b;
    struct vsr_io_replica *ra;
    struct vsr_io_replica *rb;
};

static void pair_open(struct pair *p, uint64_t seed, uint32_t link_queue,
                      uint32_t slab_bytes)
{
    world_reset(seed);
    world.link_queue = link_queue;
    world.slab_bytes = slab_bytes;
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    p->a = &world.engines[0];
    p->b = &world.engines[1];
    p->ra = open_replica(p->a, 0, cluster);
    p->rb = open_replica(p->b, 0, cluster);
    /* a dials; b, linked by then, authorizes without dialing. */
    CHECK(vsr_io_links_authorize(p->a->io, cluster, 2, 2) == VSR_OK);
    world_settle();
    CHECK(vsr_io_links_authorize(p->a->io, cluster, 1, 1) == VSR_OK);
    CHECK(vsr_io_links_authorize(p->b->io, cluster, 1, 1) == VSR_OK);
    CHECK(vsr_io_links_authorize(p->b->io, cluster, 2, 2) == VSR_OK);
    world_settle();
    check_linked(p->a, p->b);
    memset(samples_next, 0, sizeof(samples_next));
}

/* The carrier link of node `node` at engine e. */
static const struct vsr_io_link *carrier_of(const struct engine *e,
                                            uint64_t node)
{
    uint32_t index = vsr_io_links_node_index(&e->io->links, node);

    CHECK(index != NONE);
    CHECK(e->io->links.nodes[index].carrier != NONE);
    return &e->io->links.links[e->io->links.nodes[index].carrier];
}

/* Settles the world while taking the replica's messages: numbers
 * first..first + count - 1 in order, all of `size`; leases released. The
 * receiver has REGIONS leases, so a long batch is delivered in waves. */
static void drain_messages(struct vsr_io_replica *replica, uint64_t from,
                           uint64_t first, uint32_t count, uint32_t size)
{
    uint32_t taken = 0;

    for (uint32_t attempt = 0; taken < count; ++attempt) {
        CHECK(attempt < 64);
        world_settle();
        while (replica->messages_count > 0) {
            uint32_t lease = take_message(replica, from, first + taken, size);

            vsr_io_lease_release(replica, lease);
            taken++;
        }
    }
    world_settle();
    CHECK(replica->messages_count == 0);
}

/* One message of each shape, classified by the flag rule (decision 38)
 * and delivered byte-identical; the SEND op completes OK at the NOTIF. */
static void test_send_flags(void)
{
    struct pair p;
    struct sample *s;
    const struct record_log *record;
    uint32_t slab;
    uint32_t lease;

    pair_open(&p, 21, 4, PAGE);
    CHECK(vsr_io_links_send(p.a->io, 0, 1, NULL, 2) == VSR_IO_RETRY);
    CHECK(vsr_io_links_send(p.a->io, 5, 1, &sample_fresh(p.a, 1, 8)->message,
                            2) == VSR_IO_RETRY);
    CHECK(send_fresh(p.a, 0, 1, 8, 9) == VSR_IO_RETRY); /* Unauthorized. */
    CHECK(send_fresh(p.a, 0, 1, 8, 1) == VSR_IO_RETRY); /* Own node. */
    CHECK(p.a->io->stats.messages_retried == 4);
    /* Envelope only: every byte comes from the send slab. */
    log_clear(p.a);
    CHECK(send_fresh(p.a, 0, 1, NONE, 2) == VSR_OK);
    CHECK(p.a->io->links.nodes[0].queue_count == 1);
    world_settle();
    CHECK(log_count(p.a, VSR_IO_SQE_SEND) == 1);
    record = log_last(p.a, VSR_IO_SQE_SEND);
    CHECK(record->op_flags == (VSR_IO_SEND_ZERO_COPY | VSR_IO_SEND_VECTORED));
    CHECK((record->flags & VSR_IO_SQE_FIXED_BUFFER) != 0);
    CHECK(record->buffer_index == REGION_BASE && record->length == 1);
    CHECK(p.rb->messages_count == 1);
    lease = take_message(p.rb, 1, 1, NONE);
    vsr_io_lease_release(p.rb, lease);
    expect_completion(p.ra, 1, VSR_IO_OK);
    CHECK(p.a->io->stats.messages_sent == 1);
    CHECK(p.a->io->links.nodes[0].queue_count == 0);
    /* A payload below VSR_IO_INLINE_BYTES is copied into the slab. */
    log_clear(p.a);
    CHECK(send_fresh(p.a, 0, 2, 100, 2) == VSR_OK);
    world_settle();
    record = log_last(p.a, VSR_IO_SQE_SEND);
    CHECK(record->op_flags == (VSR_IO_SEND_ZERO_COPY | VSR_IO_SEND_VECTORED));
    CHECK((record->flags & VSR_IO_SQE_FIXED_BUFFER) != 0 &&
          record->length == 1);
    lease = take_message(p.rb, 1, 2, 100);
    vsr_io_lease_release(p.rb, lease);
    expect_completion(p.ra, 2, VSR_IO_OK);
    /* A referenced payload outside the pool below zero_copy_bytes: a
     * plain vectored send (header run, payload, padding). */
    log_clear(p.a);
    CHECK(send_fresh(p.a, 0, 3, 300, 2) == VSR_OK);
    world_settle();
    record = log_last(p.a, VSR_IO_SQE_SEND);
    CHECK(record->op_flags == VSR_IO_SEND_VECTORED);
    CHECK((record->flags & VSR_IO_SQE_FIXED_BUFFER) == 0 &&
          record->length == 3);
    lease = take_message(p.rb, 1, 3, 300);
    vsr_io_lease_release(p.rb, lease);
    expect_completion(p.ra, 3, VSR_IO_OK);
    /* At zero_copy_bytes with a vector outside the pool: zero-copy
     * without FIXED_BUFFER. */
    log_clear(p.a);
    CHECK(send_fresh(p.a, 0, 4, PAYLOAD_MAX, 2) == VSR_OK);
    world_settle();
    record = log_last(p.a, VSR_IO_SQE_SEND);
    CHECK(record->op_flags == (VSR_IO_SEND_ZERO_COPY | VSR_IO_SEND_VECTORED));
    CHECK((record->flags & VSR_IO_SQE_FIXED_BUFFER) == 0 &&
          record->length == 2);
    lease = take_message(p.rb, 1, 4, PAYLOAD_MAX);
    vsr_io_lease_release(p.rb, lease);
    expect_completion(p.ra, 4, VSR_IO_OK);
    /* The same payload in a taken pool slab: fixed. */
    slab = vsr_io_pool_acquire(&p.a->io->pool, false);
    CHECK(slab != NONE);
    s = sample_fresh(p.a, 5, PAYLOAD_MAX);
    memcpy(vsr_io_pool_slab(&p.a->io->pool, slab), s->payload, PAYLOAD_MAX);
    s->span.data = vsr_io_pool_slab(&p.a->io->pool, slab);
    log_clear(p.a);
    CHECK(send_sample(p.a, 0, s, 2) == VSR_OK);
    world_settle();
    record = log_last(p.a, VSR_IO_SQE_SEND);
    CHECK(record->op_flags == (VSR_IO_SEND_ZERO_COPY | VSR_IO_SEND_VECTORED));
    CHECK((record->flags & VSR_IO_SQE_FIXED_BUFFER) != 0 &&
          record->length == 2);
    lease = take_message(p.rb, 1, 5, PAYLOAD_MAX);
    vsr_io_lease_release(p.rb, lease);
    expect_completion(p.ra, 5, VSR_IO_OK);
    vsr_io_pool_release(&p.a->io->pool, slab);
    CHECK(p.a->io->stats.messages_sent == 5);
    CHECK(p.a->io->stats.bytes_sent == VSR_IO_PREAMBLE_BYTES + HELLO_BYTES +
                                           frame_bytes(NONE) +
                                           frame_bytes(100) + frame_bytes(300) +
                                           2 * frame_bytes(PAYLOAD_MAX));
    /* Both ways: b's replica sends to node 1. */
    CHECK(send_fresh(p.b, 0, 1, 40, 1) == VSR_OK);
    world_settle();
    lease = take_message(p.ra, 2, 1, 40);
    vsr_io_lease_release(p.ra, lease);
    expect_completion(p.rb, 1, VSR_IO_OK);
    CHECK(pool_refs(p.a) == 1 && pool_refs(p.b) == 1);
    /* The node's TCP_NODELAY: set on the accepted socket (the dialer's
     * peer is known to be AF_UNIX and skips it). */
    {
        const struct vsr_io_link *in =
            link_to(p.b, 1, VSR_IO_INBOUND, VSR_IO_LINK_ESTABLISHED);
        uint32_t sock = sock_by_slot(p.b->index, in->fd);

        CHECK(sock != NONE && world.socks[sock].nodelay);
        CHECK(in->nodelay_set && in->connect_slot == NONE);
    }
    engine_forget(p.a);
    engine_forget(p.b);
}

/* Coalescing: queued messages share one send up to send_coalesce_bytes, a
 * message continues in the next send, the vector bound splits a send,
 * and the send-slab ring fills, wraps and drains under held NOTIFs. */
static void test_send_coalesce(void)
{
    struct pair p;
    uint64_t before;

    /* The vector bound, with 16 KiB send slabs so that the ring is not
     * what cuts the send: 70 messages of two vectors each (a frame's pad
     * merges with the next header run) need two sends, the first with
     * VSR_IO_SEND_VECTORS vectors exactly and a message cut in two. */
    pair_open(&p, 25, 80, SLAB_BYTES_MAX);
    log_clear(p.a);
    for (uint64_t i = 0; i < 70; ++i) {
        CHECK(send_fresh(p.a, 0, 10 + i, 300, 2) == VSR_OK);
    }
    engine_step(p.a);
    CHECK(log_count(p.a, VSR_IO_SQE_SEND) == 1);
    CHECK(log_last(p.a, VSR_IO_SQE_SEND)->length == VSR_IO_SEND_VECTORS);
    CHECK(carrier_of(p.a, 2)->encoding != NONE);
    drain_messages(p.rb, 1, 10, 70, 300);
    CHECK(log_count(p.a, VSR_IO_SQE_SEND) == 2);
    for (uint64_t i = 0; i < 70; ++i) {
        expect_completion(p.ra, 10 + i, VSR_IO_OK);
    }
    CHECK(pool_refs(p.a) == 1 && pool_refs(p.b) == 1);
    engine_forget(p.a);
    engine_forget(p.b);
    pair_open(&p, 22, 48, PAGE);
    /* Three messages queued before a step: one send, three frames. */
    log_clear(p.a);
    CHECK(send_fresh(p.a, 0, 1, NONE, 2) == VSR_OK);
    CHECK(send_fresh(p.a, 0, 2, 100, 2) == VSR_OK);
    CHECK(send_fresh(p.a, 0, 3, 8, 2) == VSR_OK);
    CHECK(p.a->io->links.nodes[0].queue_count == 3);
    world_settle();
    CHECK(log_count(p.a, VSR_IO_SQE_SEND) == 1);
    CHECK(log_last(p.a, VSR_IO_SQE_SEND)->length == 1); /* One ring run. */
    CHECK(p.rb->messages_count == 3);
    vsr_io_lease_release(p.rb, take_message(p.rb, 1, 1, NONE));
    vsr_io_lease_release(p.rb, take_message(p.rb, 1, 2, 100));
    vsr_io_lease_release(p.rb, take_message(p.rb, 1, 3, 8));
    expect_completion(p.ra, 1, VSR_IO_OK);
    expect_completion(p.ra, 2, VSR_IO_OK);
    expect_completion(p.ra, 3, VSR_IO_OK);
    /* A 200-byte budget over three 80-byte frames: the third is cut at
     * the budget and continues in a second send. */
    p.a->io->options.send_coalesce_bytes = 200;
    log_clear(p.a);
    before = p.a->io->stats.bytes_sent;
    CHECK(send_fresh(p.a, 0, 4, NONE, 2) == VSR_OK);
    CHECK(send_fresh(p.a, 0, 5, NONE, 2) == VSR_OK);
    CHECK(send_fresh(p.a, 0, 6, NONE, 2) == VSR_OK);
    world_settle();
    CHECK(log_count(p.a, VSR_IO_SQE_SEND) == 2);
    CHECK(p.a->io->stats.bytes_sent == before + 240);
    CHECK(carrier_of(p.a, 2)->encoding == NONE);
    drain_messages(p.rb, 1, 4, 3, NONE);
    expect_completion(p.ra, 4, VSR_IO_OK);
    expect_completion(p.ra, 5, VSR_IO_OK);
    expect_completion(p.ra, 6, VSR_IO_OK);
    /* A message larger than the budget spans several sends. */
    log_clear(p.a);
    CHECK(send_fresh(p.a, 0, 7, PAYLOAD_MAX, 2) == VSR_OK);
    world_settle();
    CHECK(log_count(p.a, VSR_IO_SQE_SEND) ==
          (frame_bytes(PAYLOAD_MAX) + 199) / 200);
    drain_messages(p.rb, 1, 7, 1, PAYLOAD_MAX);
    expect_completion(p.ra, 7, VSR_IO_OK);
    p.a->io->options.send_coalesce_bytes = 65536;
    /* The ring: with NOTIFs held, 100-byte inline payloads (264 slab bytes
     * per frame) fill the 4096-byte send slab; the send that hits the end
     * carries a cut frame, nothing goes out until a NOTIF frees the
     * floor, and the write position wraps to the slab's start. */
    world.hold_notifs = true;
    log_clear(p.a);
    for (uint64_t i = 0; i < 30; ++i) {
        CHECK(send_fresh(p.a, 0, 100 + i, 100, 2) == VSR_OK);
        engine_step(p.a);
        engine_step(p.a);
        if (world.held_count == VSR_IO_LINK_SENDS) {
            world_release_notif(0);
        }
    }
    {
        const struct vsr_io_link *out = carrier_of(p.a, 2);

        CHECK(out->header_tail > PAGE); /* Wrapped at least once. */
        CHECK(out->header_tail - out->header_head <= PAGE);
        CHECK(out->notified_offset <= out->sent_offset);
        CHECK(out->sent_offset <= out->stream_offset);
    }
    world.hold_notifs = false;
    world_release_notifs();
    drain_messages(p.rb, 1, 100, 30, 100);
    for (uint64_t i = 0; i < 30; ++i) {
        expect_completion(p.ra, 100 + i, VSR_IO_OK);
    }
    CHECK(p.a->io->links.nodes[0].queue_count == 0);
    {
        const struct vsr_io_link *out = carrier_of(p.a, 2);

        CHECK(out->notified_offset == out->stream_offset);
        CHECK(out->header_head == out->header_tail && out->vec_count == 0);
        for (uint32_t i = 0; i < VSR_IO_LINK_SENDS; ++i) {
            CHECK(out->sends[i].slot == NONE);
        }
    }
    CHECK(pool_refs(p.a) == 1 && pool_refs(p.b) == 1);
    engine_forget(p.a);
    engine_forget(p.b);
}

/* Short sends at every byte boundary of a two-frame send: each send
 * resumes from the short count, the frames arrive byte-identical and
 * every op completes once. */
static void test_send_short(void)
{
    struct pair p;
    const uint32_t total = (uint32_t)(frame_bytes(NONE) + frame_bytes(300));
    uint64_t number = 1;

    pair_open(&p, 23, 4, PAGE);
    for (uint32_t cut = 1; cut <= total + 1; ++cut) {
        uint64_t before = p.a->io->stats.bytes_sent;
        uint64_t sends = p.a->io->stats.sends;

        world.short_send = cut;
        CHECK(send_fresh(p.a, 0, number, NONE, 2) == VSR_OK);
        CHECK(send_fresh(p.a, 0, number + 1, 300, 2) == VSR_OK);
        world_settle_rounds(4 * total);
        CHECK(p.a->io->stats.sends == sends + (total + cut - 1) / cut);
        CHECK(p.a->io->stats.bytes_sent == before + total);
        CHECK(p.rb->messages_count == 2);
        vsr_io_lease_release(p.rb, take_message(p.rb, 1, number, NONE));
        vsr_io_lease_release(p.rb, take_message(p.rb, 1, number + 1, 300));
        expect_completion(p.ra, number, VSR_IO_OK);
        expect_completion(p.ra, number + 1, VSR_IO_OK);
        CHECK(p.ra->completions_count == 0);
        number += 2;
    }
    world.short_send = 0;
    CHECK(pool_refs(p.a) == 1 && pool_refs(p.b) == 1);
    {
        const struct vsr_io_link *out = carrier_of(p.a, 2);

        CHECK(out->notified_offset == out->stream_offset &&
              out->vec_count == 0);
    }
    engine_forget(p.a);
    engine_forget(p.b);
}

/* Every send entry awaiting its NOTIF: the queue waits, nothing completes
 * until the NOTIFs come, in any order; a full queue refuses a newcomer
 * when everything queued is on the wire, else the oldest message not on
 * the wire yields to it. */
static void test_send_pressure(void)
{
    struct pair p;
    const struct vsr_io_link *out;

    pair_open(&p, 24, 4, PAGE);
    out = carrier_of(p.a, 2);
    world.hold_notifs = true;
    /* Four messages, one send each, every entry awaiting its NOTIF: the
     * queue is full of messages on the wire and a fifth is refused. */
    for (uint64_t i = 1; i <= 4; ++i) {
        CHECK(send_fresh(p.a, 0, i, 16, 2) == VSR_OK);
        world_settle();
    }
    CHECK(world.held_count == VSR_IO_LINK_SENDS);
    CHECK(out->inflight == 0 && out->vec_count == 0);
    CHECK(p.a->io->links.nodes[0].queue_count == 4);
    CHECK(p.ra->completions_count == 0 && p.rb->messages_count == 4);
    CHECK(out->notified_offset == VSR_IO_PREAMBLE_BYTES + HELLO_BYTES);
    CHECK(send_fresh(p.a, 0, 5, 16, 2) == VSR_IO_RETRY);
    CHECK(p.a->io->stats.messages_retried == 1);
    /* The second NOTIF alone moves nothing: the first send is still read. */
    world_release_notif(1);
    world_settle();
    CHECK(p.ra->completions_count == 0);
    CHECK(out->notified_offset == VSR_IO_PREAMBLE_BYTES + HELLO_BYTES);
    CHECK(p.a->io->links.nodes[0].queue_count == 4);
    /* The first: messages 1 and 2 complete and two entries free. */
    world_release_notif(0);
    world_settle();
    expect_completion(p.ra, 1, VSR_IO_OK);
    expect_completion(p.ra, 2, VSR_IO_OK);
    CHECK(p.ra->completions_count == 0);
    CHECK(p.a->io->links.nodes[0].queue_count == 2);
    CHECK(world.held_count == 2);
    /* Two more share one send; their delivery waits for a region at the
     * receiver, whose four are taken; the queue is full again. */
    CHECK(send_fresh(p.a, 0, 5, 16, 2) == VSR_OK);
    CHECK(send_fresh(p.a, 0, 6, 16, 2) == VSR_OK);
    world_settle();
    CHECK(world.held_count == 3);
    CHECK(p.a->io->links.nodes[0].queue_count == 4);
    CHECK(p.rb->messages_count == 4 && p.rb->leases_free == 0);
    CHECK(send_fresh(p.a, 0, 7, 16, 2) == VSR_IO_RETRY);
    drain_messages(p.rb, 1, 1, 6, 16);
    world.hold_notifs = false;
    world_release_notifs();
    world_settle();
    for (uint64_t i = 3; i <= 6; ++i) {
        expect_completion(p.ra, i, VSR_IO_OK);
    }
    CHECK(p.ra->completions_count == 0);
    CHECK(p.a->io->links.nodes[0].queue_count == 0);
    CHECK(pool_refs(p.a) == 1 && pool_refs(p.b) == 1);
    engine_forget(p.a);
    engine_forget(p.b);
    /* A queue of eight: four on the wire, four waiting for an entry; the
     * ninth makes the oldest waiting one yield. */
    pair_open(&p, 34, 8, PAGE);
    world.hold_notifs = true;
    for (uint64_t i = 1; i <= 8; ++i) {
        CHECK(send_fresh(p.a, 0, i, 16, 2) == VSR_OK);
        world_settle();
    }
    CHECK(world.held_count == VSR_IO_LINK_SENDS);
    CHECK(p.a->io->links.nodes[0].queue_count == 8);
    CHECK(send_fresh(p.a, 0, 9, 16, 2) == VSR_OK);
    expect_completion(p.ra, 5, VSR_IO_RETRY);
    CHECK(p.ra->completions_count == 0);
    CHECK(p.a->io->links.nodes[0].queue_count == 8);
    drain_messages(p.rb, 1, 1, 4, 16);
    world.hold_notifs = false;
    world_release_notifs();
    world_settle();
    for (uint64_t i = 1; i <= 4; ++i) {
        expect_completion(p.ra, i, VSR_IO_OK);
    }
    drain_messages(p.rb, 1, 6, 4, 16);
    for (uint64_t i = 6; i <= 9; ++i) {
        expect_completion(p.ra, i, VSR_IO_OK);
    }
    CHECK(p.ra->completions_count == 0);
    CHECK(p.a->io->links.nodes[0].queue_count == 0);
    CHECK(p.a->io->stats.messages_retried == 1);
    CHECK(pool_refs(p.a) == 1 && pool_refs(p.b) == 1);
    engine_forget(p.a);
    engine_forget(p.b);
}

/* Link loss with messages queued, awaiting NOTIF and in flight: every op
 * completes exactly once with RETRY, the link frees once its NOTIFs are
 * in and the pool's references return to the send slabs. */
static void test_send_loss(void)
{
    struct pair p;
    const struct vsr_io_link *out;
    uint64_t op = 0;
    int32_t status = 0;
    bool done[16];

    pair_open(&p, 26, 8, PAGE);
    out = carrier_of(p.a, 2);
    world.hold_notifs = true;
    /* Four sends awaiting their NOTIF (messages 1-4), 5 and 6 behind. */
    for (uint64_t i = 1; i <= 6; ++i) {
        CHECK(send_fresh(p.a, 0, i, 16, 2) == VSR_OK);
        world_settle();
    }
    CHECK(world.held_count == VSR_IO_LINK_SENDS);
    CHECK(p.a->io->links.nodes[0].queue_count == 6);
    /* The network resets the connection: the receive fails, the link
     * closes, the waiting messages complete RETRY at once and those on
     * the wire when their NOTIFs release them; none OK. */
    link_reset(p.a, out);
    world_settle();
    CHECK(out->state == VSR_IO_LINK_CLOSING); /* NOTIFs still out. */
    CHECK(p.a->io->links.nodes[0].queue_count == 4);
    expect_completion(p.ra, 6, VSR_IO_RETRY);
    expect_completion(p.ra, 5, VSR_IO_RETRY);
    CHECK(p.ra->completions_count == 0);
    CHECK(node_state(p.a, 2) == VSR_IO_NODE_UNLINKED); /* No SEND: no dial. */
    world.hold_notifs = false;
    world_release_notif(2);
    world_settle();
    CHECK(p.ra->completions_count == 0); /* Send 0 still reads them. */
    world_release_notifs();
    world_settle();
    CHECK(out->state == VSR_IO_LINK_FREE);
    CHECK(p.a->io->links.nodes[0].queue_count == 0);
    memset(done, 0, sizeof(done));
    while (take_completion(p.ra, &op, &status)) {
        CHECK(op >= 1 && op <= 4 && !done[op] && status == VSR_IO_RETRY);
        done[op] = true;
    }
    for (uint64_t i = 1; i <= 4; ++i) {
        CHECK(done[i]);
    }
    CHECK(p.a->io->stats.messages_retried == 6);
    CHECK(p.a->io->stats.messages_sent == 0);
    check_quiet(p.a);
    CHECK(pool_refs(p.a) == 0);
    /* b got the four that were on the wire, on a link since reset. */
    check_quiet(p.b);
    for (uint64_t i = 1; i <= 4; ++i) {
        vsr_io_lease_release(p.rb, take_message(p.rb, 1, i, 16));
    }
    CHECK(p.rb->messages_count == 0 && pool_refs(p.b) == 0);
    /* A SEND redials at once; the send then fails on a socket reset
     * meanwhile (-EPIPE, with the NOTIF a failed zero-copy send still
     * gets): the op completes RETRY once and the link frees. */
    CHECK(send_fresh(p.a, 0, 7, 16, 2) == VSR_OK);
    world_settle();
    out = carrier_of(p.a, 2);
    drain_messages(p.rb, 1, 7, 1, 16);
    expect_completion(p.ra, 7, VSR_IO_OK);
    link_reset(p.a, out);
    CHECK(send_fresh(p.a, 0, 8, 16, 2) == VSR_OK);
    engine_step(p.a); /* The send goes out and fails. */
    world_settle();
    expect_completion(p.ra, 8, VSR_IO_RETRY);
    CHECK(p.ra->completions_count == 0);
    CHECK(p.a->io->links.nodes[0].last_error == -EPIPE ||
          p.a->io->links.nodes[0].last_error == -ECONNRESET);
    check_quiet(p.a);
    CHECK(pool_refs(p.a) == 0);
    world_settle();
    check_quiet(p.b);
    /* The peer's process ends while a message is queued behind a send:
     * end of stream closes the link and the message completes RETRY. */
    CHECK(send_fresh(p.a, 0, 9, 16, 2) == VSR_OK);
    world_settle();
    drain_messages(p.rb, 1, 9, 1, 16);
    expect_completion(p.ra, 9, VSR_IO_OK);
    world.hold_notifs = true;
    CHECK(send_fresh(p.a, 0, 10, 16, 2) == VSR_OK);
    world_settle();
    CHECK(send_fresh(p.a, 0, 11, 16, 2) == VSR_OK);
    engine_forget(p.b);
    world_settle();
    /* 11 went out too, into a failing send: both await their NOTIFs. */
    CHECK(p.ra->completions_count == 0);
    CHECK(p.a->io->links.nodes[0].queue_count == 2);
    world.hold_notifs = false;
    world_release_notifs();
    world_settle();
    memset(done, 0, sizeof(done));
    while (take_completion(p.ra, &op, &status)) {
        CHECK(op >= 10 && op <= 11 && !done[op] && status == VSR_IO_RETRY);
        done[op] = true;
    }
    CHECK(done[10] && done[11]);
    check_quiet(p.a);
    CHECK(pool_refs(p.a) == 0);
    CHECK(p.a->io->stats.messages_retried == 9);
    CHECK(p.a->io->stats.messages_sent == 2);
    engine_forget(p.a);
}

/* A carrier change with a queue: the messages the old carrier had put on
 * its stream complete RETRY, the rest go out on the new carrier; a
 * demoted link with a frame half sent is closed. */
static void test_send_switch(void)
{
    struct engine *a;
    struct engine *b;
    struct vsr_io_replica *ra;
    struct vsr_io_replica *rb;
    const struct vsr_io_link *in;
    const struct vsr_io_link *out;
    int fd;

    world_reset(27);
    world.link_queue = 8;
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    ra = open_replica(a, 0, cluster);
    rb = open_replica(b, 0, cluster);
    /* b dials: at a the inbound link carries for want of a better one. */
    CHECK(vsr_io_links_authorize(b->io, cluster, 1, 1) == VSR_OK);
    world_settle();
    CHECK(vsr_io_links_authorize(a->io, cluster, 1, 1) == VSR_OK);
    CHECK(vsr_io_links_authorize(a->io, cluster, 2, 2) == VSR_OK);
    CHECK(vsr_io_links_authorize(b->io, cluster, 2, 2) == VSR_OK);
    world_settle();
    in = link_to(a, 2, VSR_IO_INBOUND, VSR_IO_LINK_ESTABLISHED);
    CHECK(in != NULL && carrier_of(a, 2) == in);
    CHECK(links_in_state(a, VSR_IO_LINK_ESTABLISHED) == 1);
    /* Four messages on the wire with NOTIFs held, two queued behind. */
    world.hold_notifs = true;
    for (uint64_t i = 1; i <= 6; ++i) {
        CHECK(send_fresh(a, 0, i, 16, 2) == VSR_OK);
        world_settle();
    }
    CHECK(world.held_count == VSR_IO_LINK_SENDS);
    CHECK(a->io->links.nodes[0].queue_count == 6);
    /* a adopts an outbound socket to b with the TRUSTED handshake: the
     * preferred direction wins the election at both ends. */
    fd = engine_connect(a, &b->listen);
    CHECK(vsr_io_links_adopt(a->io, fd, 2,
                             VSR_IO_ADOPT_HANDSHAKE | VSR_IO_ADOPT_OUTBOUND) ==
          VSR_OK);
    world_settle();
    out = link_to(a, 2, VSR_IO_OUTBOUND, VSR_IO_LINK_ESTABLISHED);
    CHECK(out != NULL && carrier_of(a, 2) == out);
    CHECK(in->state == VSR_IO_LINK_ESTABLISHED); /* Whole frames: stays. */
    CHECK(carrier_of(b, 1) ==
          link_to(b, 1, VSR_IO_INBOUND, VSR_IO_LINK_ESTABLISHED));
    /* 1-4 retire until the old link's NOTIFs; 5 and 6 went out on the new
     * carrier and await theirs; the old link's fourth NOTIF alone frees
     * nothing, its first releases 1 and 2. */
    CHECK(ra->completions_count == 0);
    CHECK(a->io->links.nodes[0].queue_count == 6);
    CHECK(world.held_count >= VSR_IO_LINK_SENDS + 1); /* Plus HELLOs. */
    world_release_notif(3);
    world_settle();
    CHECK(ra->completions_count == 0);
    world_release_notif(0);
    world_settle();
    expect_completion(ra, 1, VSR_IO_RETRY); /* 2's send is still live. */
    CHECK(ra->completions_count == 0);
    world.hold_notifs = false;
    world_release_notifs();
    world_settle();
    for (uint64_t i = 2; i <= 4; ++i) {
        expect_completion(ra, i, VSR_IO_RETRY);
    }
    expect_completion(ra, 5, VSR_IO_OK);
    expect_completion(ra, 6, VSR_IO_OK);
    CHECK(a->io->stats.messages_retried == 4 &&
          a->io->stats.messages_sent == 2);
    drain_messages(rb, 1, 1, 6, 16);
    /* Both links idle away: the demoted one, and the carrier of a node
     * with nothing queued. */
    world_advance(IDLE_NS);
    world_settle();
    CHECK(links_in_state(a, VSR_IO_LINK_ESTABLISHED) == 0);
    CHECK(in->state == VSR_IO_LINK_FREE && out->state == VSR_IO_LINK_FREE);
    CHECK(node_state(a, 2) == VSR_IO_NODE_UNLINKED);
    CHECK(pool_refs(a) == 0 && pool_refs(b) == 0);
    engine_forget(a);
    engine_forget(b);
    /* Again, with a message half sent when the carrier is demoted: a
     * 100-byte budget cuts a 464-byte frame into sends, four go out
     * (NOTIFs held) and the fifth waits for an entry. */
    world_reset(37);
    world.link_queue = 8;
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    ra = open_replica(a, 0, cluster);
    rb = open_replica(b, 0, cluster);
    CHECK(vsr_io_links_authorize(b->io, cluster, 1, 1) == VSR_OK);
    world_settle();
    CHECK(vsr_io_links_authorize(a->io, cluster, 2, 2) == VSR_OK);
    CHECK(vsr_io_links_authorize(b->io, cluster, 2, 2) == VSR_OK);
    world_settle();
    in = link_to(a, 2, VSR_IO_INBOUND, VSR_IO_LINK_ESTABLISHED);
    CHECK(in != NULL && carrier_of(a, 2) == in);
    a->io->options.send_coalesce_bytes = 100;
    world.hold_notifs = true;
    {
        /* The payload in a pool slab: every partial send is zero-copy,
         * so every entry ends up awaiting a NOTIF. */
        struct sample *s = sample_fresh(a, 1, 300);
        uint32_t slab = vsr_io_pool_acquire(&a->io->pool, false);

        CHECK(slab != NONE);
        memcpy(vsr_io_pool_slab(&a->io->pool, slab), s->payload, 300);
        s->span.data = vsr_io_pool_slab(&a->io->pool, slab);
        CHECK(send_sample(a, 0, s, 2) == VSR_OK);
        vsr_io_pool_release(&a->io->pool, slab); /* The send's now. */
    }
    CHECK(send_fresh(a, 0, 2, 16, 2) == VSR_OK);
    world_settle();
    CHECK(world.held_count == VSR_IO_LINK_SENDS && in->encoding != NONE);
    CHECK(in->sent_offset == HELLO_BYTES + 400); /* No preamble inbound. */
    fd = engine_connect(a, &b->listen);
    CHECK(vsr_io_links_adopt(a->io, fd, 2,
                             VSR_IO_ADOPT_HANDSHAKE | VSR_IO_ADOPT_OUTBOUND) ==
          VSR_OK);
    world_settle();
    out = link_to(a, 2, VSR_IO_OUTBOUND, VSR_IO_LINK_ESTABLISHED);
    CHECK(out != NULL && carrier_of(a, 2) == out);
    CHECK(in->state == VSR_IO_LINK_CLOSING && in->error == -ECANCELED);
    CHECK(ra->completions_count == 0 && a->io->links.nodes[0].queue_count == 2);
    world.hold_notifs = false;
    world_release_notifs();
    world_settle();
    CHECK(in->state == VSR_IO_LINK_FREE);
    expect_completion(ra, 1, VSR_IO_RETRY);
    expect_completion(ra, 2, VSR_IO_OK);
    drain_messages(rb, 1, 2, 1, 16);
    /* b dropped the cut frame with the link it came on. */
    CHECK(b->io->stats.frames_rejected == 0);
    CHECK(links_in_state(b, VSR_IO_LINK_ESTABLISHED) == 1);
    CHECK(pool_refs(a) == 1 && pool_refs(b) == 1);
    engine_forget(a);
    engine_forget(b);
}

/* A zero-copy send refused at translation completes once without MORE:
 * the link closes with the error, its slot frees, the op completes RETRY
 * and the next SEND relinks. */
static void test_send_reject(void)
{
    struct pair p;

    pair_open(&p, 28, 4, PAGE);
    world.reject_send = 1;
    CHECK(send_fresh(p.a, 0, 1, 16, 2) == VSR_OK);
    world_settle();
    expect_completion(p.ra, 1, VSR_IO_RETRY);
    CHECK(p.ra->completions_count == 0);
    CHECK(p.a->io->stats.messages_retried == 1);
    CHECK(p.a->io->links.nodes[0].last_error == -EINVAL);
    CHECK(world.reject_send == 0);
    check_quiet(p.a);
    check_quiet(p.b);
    CHECK(pool_refs(p.a) == 0 && pool_refs(p.b) == 0);
    CHECK(send_fresh(p.a, 0, 2, 16, 2) == VSR_OK);
    world_settle();
    drain_messages(p.rb, 1, 2, 1, 16);
    expect_completion(p.ra, 2, VSR_IO_OK);
    CHECK(pool_refs(p.a) == 1 && pool_refs(p.b) == 1);
    engine_forget(p.a);
    engine_forget(p.b);
}

/* Stream links: dialed for a stream, raw frames queued under the flag
 * rule, EBUSY when the send side is full, the idle exemption of a bound
 * link, an orderly close, a failed dial and refusals. */
static void test_stream(void)
{
    static unsigned char bytes[PAGE];
    static unsigned char hello[12] = "hello world!";
    struct engine *a;
    struct engine *b;
    const struct vsr_io_link *link;
    const struct vsr_io_link *in;
    const struct record_log *record;
    struct vsr_io_vec vecs[VSR_IO_SEND_VECTORS];
    unsigned char header[16];
    uint64_t end = 0;
    uint64_t before;
    uint32_t index = 0;
    uint32_t slab;
    uint32_t crc;
    uint32_t peer;

    world_reset(29);
    two_engines(VSR_IO_HANDSHAKE_TRUSTED);
    a = &world.engines[0];
    b = &world.engines[1];
    world_settle();
    for (uint32_t i = 0; i < PAGE; ++i) {
        bytes[i] = (unsigned char)(i * 7 + 3);
    }
    /* Refusals: an unknown, a caller-dialed and the own node. */
    CHECK(vsr_io_links_open_stream(a->io, 7, 0, &index) == VSR_EINVAL);
    CHECK(index == NONE);
    CHECK(vsr_io_links_node_set(a->io, 3, NULL) == VSR_OK);
    CHECK(vsr_io_links_open_stream(a->io, 3, 0, &index) == VSR_EINVAL);
    CHECK(vsr_io_links_open_stream(a->io, 1, 0, &index) == VSR_EINVAL);
    CHECK(vsr_io_links_open_stream(a->io, 2, 0, NULL) == VSR_EINVAL);
    /* The dial: a STREAM link at both ends, no carrier, the node LINKED,
     * and a peer link still dialed when wanted. */
    CHECK(vsr_io_links_open_stream(a->io, 2, 0, &index) == VSR_OK);
    CHECK(index != NONE);
    link = &a->io->links.links[index];
    CHECK(link->state == VSR_IO_LINK_CONNECTING);
    CHECK(link->purpose == VSR_IO_PURPOSE_STREAM && link->stream == 0);
    world_settle();
    CHECK(link->state == VSR_IO_LINK_ESTABLISHED);
    CHECK(a->io->links.nodes[0].carrier == NONE);
    in = link_to(b, 1, VSR_IO_INBOUND, VSR_IO_LINK_ESTABLISHED);
    CHECK(in != NULL && in->purpose == VSR_IO_PURPOSE_STREAM);
    CHECK(in->stream == NONE && b->io->links.nodes[0].carrier == NONE);
    CHECK(node_state(a, 2) == VSR_IO_NODE_LINKED);
    CHECK(node_state(b, 1) == VSR_IO_NODE_LINKED);
    CHECK(vsr_io_links_authorize(a->io, cluster, 2, 2) == VSR_OK);
    world_settle();
    CHECK(carrier_of(a, 2)->purpose == VSR_IO_PURPOSE_PEER);
    CHECK(links_in_state(a, VSR_IO_LINK_ESTABLISHED) == 2);
    /* A request with a 12-byte payload from test memory: header run,
     * payload and pad, a plain vectored send; the stream module (a stub
     * here) consumes it at b, nothing rejected. */
    vsr_io_codec_put_stream_request(header, 12);
    vecs[0].base = hello;
    vecs[0].length = 12;
    crc = vsr_io_crc32c(vsr_io_crc32c(0, header, 8), hello, 12);
    log_clear(a);
    before = b->io->stats.bytes_received;
    CHECK(vsr_io_links_send_frame(a->io, index, VSR_IO_FRAME_STREAM_REQUEST,
                                  header, 8, vecs, 1, crc, &end) == VSR_OK);
    CHECK(end == link->stream_offset);
    CHECK(end == VSR_IO_PREAMBLE_BYTES + HELLO_BYTES + 24 + 8 + 16);
    world_settle();
    record = log_last(a, VSR_IO_SQE_SEND);
    CHECK(record->op_flags == VSR_IO_SEND_VECTORED && record->length == 3);
    CHECK(b->io->stats.bytes_received == before + 48);
    CHECK(b->io->stats.frames_rejected == 0 && in->partial_length == 0);
    CHECK(link->notified_offset == end);
    CHECK(b->io->links.nodes[0].last_received_ns == world.now);
    /* A chunk from a pool slab: fixed, header run and payload. */
    slab = vsr_io_pool_acquire(&a->io->pool, false);
    CHECK(slab != NONE);
    memcpy(vsr_io_pool_slab(&a->io->pool, slab), bytes, 1000);
    vsr_io_codec_put_stream_chunk(header, 0, 1000);
    vecs[0].base = vsr_io_pool_slab(&a->io->pool, slab);
    vecs[0].length = 1000;
    crc = vsr_io_crc32c(vsr_io_crc32c(0, header, 16), bytes, 1000);
    log_clear(a);
    before = b->io->stats.bytes_received;
    CHECK(vsr_io_links_send_frame(a->io, index, VSR_IO_FRAME_STREAM_CHUNK,
                                  header, 16, vecs, 1, crc, &end) == VSR_OK);
    world_settle();
    record = log_last(a, VSR_IO_SQE_SEND);
    CHECK(record->op_flags == (VSR_IO_SEND_ZERO_COPY | VSR_IO_SEND_VECTORED));
    CHECK((record->flags & VSR_IO_SQE_FIXED_BUFFER) != 0 &&
          record->length == 2);
    CHECK(b->io->stats.bytes_received == before + 24 + 16 + 1000);
    CHECK(b->io->stats.frames_rejected == 0 && in->partial_length == 0);
    CHECK(link->notified_offset == end);
    vsr_io_pool_release(&a->io->pool, slab);
    /* END: the header alone, one ring run. */
    vsr_io_codec_put_stream_end(header, 1012, VSR_IO_OK);
    crc = vsr_io_crc32c(0, header, 16);
    log_clear(a);
    CHECK(vsr_io_links_send_frame(a->io, index, VSR_IO_FRAME_STREAM_END, header,
                                  16, NULL, 0, crc, &end) == VSR_OK);
    world_settle();
    record = log_last(a, VSR_IO_SQE_SEND);
    CHECK((record->flags & VSR_IO_SQE_FIXED_BUFFER) != 0 &&
          record->length == 1);
    CHECK(b->io->stats.frames_rejected == 0 && in->partial_length == 0);
    /* EBUSY while a send is in flight; queued again once its result is
     * in, and coalesced with what follows. */
    vsr_io_codec_put_stream_request(header, 0);
    crc = vsr_io_crc32c(0, header, 8);
    CHECK(vsr_io_links_send_frame(a->io, index, VSR_IO_FRAME_STREAM_REQUEST,
                                  header, 8, NULL, 0, crc, &end) == VSR_OK);
    engine_step(a);
    CHECK(link->inflight == 1);
    CHECK(vsr_io_links_send_frame(a->io, index, VSR_IO_FRAME_STREAM_REQUEST,
                                  header, 8, NULL, 0, crc, &end) == VSR_EBUSY);
    engine_step(a);
    CHECK(link->inflight == 0);
    log_clear(a);
    CHECK(vsr_io_links_send_frame(a->io, index, VSR_IO_FRAME_STREAM_REQUEST,
                                  header, 8, NULL, 0, crc, &end) == VSR_OK);
    CHECK(vsr_io_links_send_frame(a->io, index, VSR_IO_FRAME_STREAM_REQUEST,
                                  header, 8, NULL, 0, crc, &end) == VSR_OK);
    world_settle();
    CHECK(log_count(a, VSR_IO_SQE_SEND) == 1);
    CHECK(log_last(a, VSR_IO_SQE_SEND)->length == 1); /* Contiguous. */
    CHECK(link->notified_offset == end && link->vec_count == 0);
    /* Every entry awaiting its NOTIF. */
    world.hold_notifs = true;
    for (uint32_t i = 0; i < VSR_IO_LINK_SENDS; ++i) {
        CHECK(vsr_io_links_send_frame(a->io, index, VSR_IO_FRAME_STREAM_REQUEST,
                                      header, 8, NULL, 0, crc, &end) == VSR_OK);
        engine_step(a);
        engine_step(a);
    }
    CHECK(world.held_count == VSR_IO_LINK_SENDS && link->inflight == 0);
    CHECK(vsr_io_links_send_frame(a->io, index, VSR_IO_FRAME_STREAM_REQUEST,
                                  header, 8, NULL, 0, crc, &end) == VSR_EBUSY);
    world_release_notif(0);
    world_settle();
    CHECK(vsr_io_links_send_frame(a->io, index, VSR_IO_FRAME_STREAM_REQUEST,
                                  header, 8, NULL, 0, crc, &end) == VSR_OK);
    world.hold_notifs = false;
    world_release_notifs();
    world_settle();
    CHECK(link->notified_offset == end && end == link->stream_offset);
    /* The vectors: 100 one-byte payload runs (every other byte, so that
     * none merges with its neighbour) take 102 (header, runs, pad); 30
     * more would overflow the array until that send is out. */
    for (uint32_t i = 0; i < 100; ++i) {
        vecs[i].base = bytes + (size_t)2 * i;
        vecs[i].length = 1;
    }
    vsr_io_codec_put_stream_chunk(header, 0, 100);
    crc = vsr_io_crc32c(0, header, 16);
    for (uint32_t i = 0; i < 100; ++i) {
        crc = vsr_io_crc32c(crc, vecs[i].base, 1);
    }
    CHECK(vsr_io_links_send_frame(a->io, index, VSR_IO_FRAME_STREAM_CHUNK,
                                  header, 16, vecs, 100, crc, &end) == VSR_OK);
    CHECK(link->vec_count == 102);
    vsr_io_codec_put_stream_chunk(header, 100, 30);
    crc = vsr_io_crc32c(0, header, 16);
    for (uint32_t i = 0; i < 30; ++i) {
        crc = vsr_io_crc32c(crc, vecs[i].base, 1);
    }
    CHECK(vsr_io_links_send_frame(a->io, index, VSR_IO_FRAME_STREAM_CHUNK,
                                  header, 16, vecs, 30, crc,
                                  &end) == VSR_EBUSY);
    world_settle();
    log_clear(a);
    CHECK(vsr_io_links_send_frame(a->io, index, VSR_IO_FRAME_STREAM_CHUNK,
                                  header, 16, vecs, 30, crc, &end) == VSR_OK);
    world_settle();
    CHECK(log_last(a, VSR_IO_SQE_SEND)->length == 32);
    CHECK(b->io->stats.frames_rejected == 0 && in->partial_length == 0);
    /* The coalesce budget: the third 48-byte frame waits for the send. */
    a->io->options.send_coalesce_bytes = 100;
    vsr_io_codec_put_stream_request(header, 12);
    vecs[0].base = hello;
    vecs[0].length = 12;
    crc = vsr_io_crc32c(vsr_io_crc32c(0, header, 8), hello, 12);
    CHECK(vsr_io_links_send_frame(a->io, index, VSR_IO_FRAME_STREAM_REQUEST,
                                  header, 8, vecs, 1, crc, &end) == VSR_OK);
    CHECK(vsr_io_links_send_frame(a->io, index, VSR_IO_FRAME_STREAM_REQUEST,
                                  header, 8, vecs, 1, crc, &end) == VSR_OK);
    CHECK(vsr_io_links_send_frame(a->io, index, VSR_IO_FRAME_STREAM_REQUEST,
                                  header, 8, vecs, 1, crc, &end) == VSR_EBUSY);
    world_settle();
    CHECK(vsr_io_links_send_frame(a->io, index, VSR_IO_FRAME_STREAM_REQUEST,
                                  header, 8, vecs, 1, crc, &end) == VSR_OK);
    world_settle();
    a->io->options.send_coalesce_bytes = 65536;
    CHECK(b->io->stats.frames_rejected == 0 && in->partial_length == 0);
    /* ELIMIT beyond the frame limit; EINVAL for a peer link, a bad kind,
     * a bad index. */
    vsr_io_codec_put_stream_chunk(header, 0, PAGE);
    vecs[0].base = bytes;
    vecs[0].length = PAGE;
    CHECK(vsr_io_links_send_frame(a->io, index, VSR_IO_FRAME_STREAM_CHUNK,
                                  header, 16, vecs, 1, 0, &end) == VSR_ELIMIT);
    CHECK(vsr_io_links_send_frame(
              a->io, (uint32_t)(carrier_of(a, 2) - a->io->links.links),
              VSR_IO_FRAME_STREAM_END, header, 16, NULL, 0, 0,
              &end) == VSR_EINVAL);
    CHECK(vsr_io_links_send_frame(a->io, index, VSR_IO_FRAME_MESSAGE, header,
                                  16, NULL, 0, 0, &end) == VSR_EINVAL);
    CHECK(vsr_io_links_send_frame(a->io, LINKS, VSR_IO_FRAME_STREAM_END, header,
                                  16, NULL, 0, 0, &end) == VSR_EINVAL);
    CHECK(vsr_io_links_send_frame(a->io, index, VSR_IO_FRAME_STREAM_END, header,
                                  16, NULL, 0, 0, NULL) == VSR_EINVAL);
    /* Idle: links bound to a stream (b's as the stream module binds it at
     * the request) live through the idle timeout; the idle peer link
     * goes. */
    b->io->links.links[in - b->io->links.links].stream = 1;
    world_advance(IDLE_NS * 3);
    world_settle();
    CHECK(link->state == VSR_IO_LINK_ESTABLISHED);
    CHECK(in->state == VSR_IO_LINK_ESTABLISHED);
    CHECK(links_in_state(a, VSR_IO_LINK_ESTABLISHED) == 1);
    /* The stream module closes its link: orderly at both ends. */
    vsr_io_links_close(a->io, index, 0);
    world_settle();
    CHECK(link->state == VSR_IO_LINK_FREE && in->state == VSR_IO_LINK_FREE);
    check_quiet(a);
    check_quiet(b);
    CHECK(pool_refs(a) == 0 && pool_refs(b) == 0);
    /* A stream dial that fails (nobody listens) schedules nothing for
     * the node; the stream module hears link_lost (a stub here). */
    engine_forget(b);
    CHECK(vsr_io_links_open_stream(a->io, 2, 0, &index) == VSR_OK);
    world_settle();
    CHECK(a->io->links.links[index].state == VSR_IO_LINK_FREE);
    CHECK(a->io->links.nodes[0].attempts == 0);
    CHECK(a->io->links.nodes[0].next_dial_ns == VSR_NO_DEADLINE);
    CHECK(a->io->links.nodes[0].last_error == -ECONNREFUSED);
    /* ELIMIT once every link entry is taken. */
    for (uint32_t i = 0; i < LINKS; ++i) {
        CHECK(vsr_io_links_open_stream(a->io, 2, i, &index) == VSR_OK);
    }
    CHECK(vsr_io_links_open_stream(a->io, 2, 0, &index) == VSR_ELIMIT);
    world_settle();
    check_quiet(a);
    /* A stream frame on a peer link closes it. */
    peer = peer_link(a, 2);
    before = a->io->stats.frames_rejected;
    vsr_io_codec_put_stream_end(header, 0, VSR_IO_OK);
    {
        unsigned char frame[24 + 16];

        vsr_io_codec_put_frame(frame, VSR_IO_FRAME_STREAM_END, 16,
                               vsr_io_crc32c(0, header, 16));
        memcpy(frame + 24, header, 16);
        peer_write(peer, frame, sizeof(frame));
    }
    world_settle();
    CHECK(a->io->stats.frames_rejected == before + 1);
    CHECK(peer_eof(peer));
    check_quiet(a);
    peer_close(peer);
    engine_forget(a);
}

/* -------------------------------------------------------------------------
 * End-to-end random walk: two engines exchanging messages both ways
 * ---------------------------------------------------------------------- */

#define SEND_WALK_ROUNDS 300u
#define SEND_WALK_MAX 2048u /* Numbers per direction. */
#define SEND_WALK_OUT 100u  /* Outstanding ops per direction (< SAMPLES). */
#define SEND_WALK_PENDING (-1)
#define SEND_WALK_REFUSED (-2) /* vsr_io_links_send returned RETRY. */

/* One direction of the walk. Every message sent gets a fresh number; its
 * op's status is recorded once, and the numbers still to be delivered
 * form a queue in send order. */
struct walker {
    struct engine *sender;
    struct vsr_io_replica *sender_replica;
    struct vsr_io_replica *receiver;
    uint64_t from;   /* The sender's node id and replica id. */
    uint64_t member; /* The receiver's. */
    uint64_t next;   /* Next number to send. */
    uint64_t floor;  /* Numbers below it were sent before the last link
                        epoch change and may have been lost. */
    int8_t status[SEND_WALK_MAX];
    uint32_t size[SEND_WALK_MAX];
    uint32_t queue[SEND_WALK_MAX]; /* Numbers awaiting delivery. */
    uint32_t queue_head;
    uint32_t queue_count;
    uint32_t outstanding; /* Ops with a pending status. */
    uint32_t held[REGIONS];
    uint32_t held_count;
    uint64_t oks;
    uint64_t retries;
    uint64_t refused;
    uint64_t delivered;
    uint64_t skipped;
};

static void walker_init(struct walker *w, struct engine *sender,
                        struct vsr_io_replica *sender_replica,
                        struct vsr_io_replica *receiver, uint64_t member)
{
    memset(w, 0, sizeof(*w));
    w->sender = sender;
    w->sender_replica = sender_replica;
    w->receiver = receiver;
    w->from = sender->node;
    w->member = member;
    w->next = 1;
    w->floor = 1;
}

/* The number of the replica's next queued MESSAGE. */
static uint64_t peek_number(const struct vsr_io_replica *replica)
{
    const struct vsr_io_queued_event *queued =
        &replica->messages[replica->messages_head];
    const struct vsr_message *m = queued->event.data;

    CHECK(replica->messages_count > 0 && m != NULL);
    return m->number;
}

static void walker_send(struct walker *w, uint32_t size)
{
    uint64_t n = w->next++;
    int rc;

    CHECK(n < SEND_WALK_MAX);
    rc = send_fresh(w->sender, 0, n, size, w->member);
    CHECK(rc == VSR_OK || rc == VSR_IO_RETRY);
    w->size[n] = size;
    w->queue[(w->queue_head + w->queue_count++) % SEND_WALK_MAX] = (uint32_t)n;
    if (rc == VSR_OK) {
        w->status[n] = SEND_WALK_PENDING;
        w->outstanding++;
    } else {
        w->status[n] = SEND_WALK_REFUSED;
        w->refused++;
    }
}

/* Every completion of the sender's replica: once per op, OK or RETRY. */
static void walker_completions(struct walker *w)
{
    uint64_t op = 0;
    int32_t status = 0;

    while (take_completion(w->sender_replica, &op, &status)) {
        CHECK(op >= 1 && op < w->next && w->status[op] == SEND_WALK_PENDING);
        CHECK(status == VSR_IO_OK || status == VSR_IO_RETRY);
        w->status[op] = (int8_t)status;
        w->outstanding--;
        if (status == VSR_IO_OK) {
            w->oks++;
        } else {
            w->retries++;
        }
    }
}

/* Every delivered message: in send order, each number at most once,
 * byte-identical; a number is skipped only when its op did not complete
 * OK or it was sent before the last link epoch change. */
static void walker_deliveries(struct walker *w, struct test_random *random)
{
    while (w->receiver->messages_count > 0) {
        uint64_t n = peek_number(w->receiver);
        uint32_t lease;

        for (;;) {
            uint32_t head;

            CHECK(w->queue_count > 0);
            head = w->queue[w->queue_head % SEND_WALK_MAX];
            if (head == n) {
                break;
            }
            CHECK(head < n);
            CHECK(w->status[head] != VSR_IO_OK || head < w->floor);
            w->queue_head++;
            w->queue_count--;
            w->skipped++;
        }
        w->queue_head++;
        w->queue_count--;
        lease = take_message(w->receiver, w->from, n, w->size[n]);
        w->delivered++;
        if (w->held_count < REGIONS - 1 &&
            test_random_bounded(random, 3) == 0) {
            w->held[w->held_count++] = lease;
        } else {
            vsr_io_lease_release(w->receiver, lease);
        }
    }
}

static void walker_release(struct walker *w, bool all,
                           struct test_random *random)
{
    while (w->held_count > 0 && (all || test_random_bounded(random, 2) == 0)) {
        vsr_io_lease_release(w->receiver, w->held[--w->held_count]);
    }
}

/* The identity of a's carrier to node 2, or 0 when none: a change means a
 * new link epoch on both sides (the same connection carries both ways). */
static uint64_t carrier_epoch(const struct engine *e)
{
    const struct vsr_io_node *node = &e->io->links.nodes[0];

    if (node->carrier == NONE) {
        return 0;
    }
    return (e->io->links.links[node->carrier].established_ns << 8) +
           node->carrier + 1;
}

static uint32_t walk_size(struct test_random *random)
{
    switch (test_random_bounded(random, 5)) {
    case 0:
        return NONE;
    case 1:
        return PAYLOAD_MAX - 8 * test_random_bounded(random, 4);
    case 2:
        return test_random_bounded(random, 64);
    default:
        return test_random_bounded(random, PAYLOAD_MAX + 1);
    }
}

/* No link of the engine holds received bytes it has not carved. */
static bool links_drained(const struct engine *e)
{
    for (uint32_t i = 0; i < LINKS; ++i) {
        const struct vsr_io_link *link = &e->io->links.links[i];

        if (link->partial_length != 0 || link->held_count != 0 || link->retry) {
            return false;
        }
    }
    return true;
}

/* Slabs the links of an engine hold: one send slab per link that has
 * one. */
static uint32_t send_slabs(const struct engine *e)
{
    uint32_t count = 0;

    for (uint32_t i = 0; i < LINKS; ++i) {
        count += e->io->links.links[i].send_slab != NONE ? 1 : 0;
    }
    return count;
}

static void test_send_random(uint64_t seed)
{
    struct pair p;
    static struct walker ab;
    static struct walker ba;
    struct walker *walkers[2] = {&ab, &ba};
    struct test_random *random;
    uint64_t epoch;
    uint64_t resets = 0;
    uint64_t epochs = 0;

    printf("link: send walk seed %" PRIu64 "\n", seed);
    pair_open(&p, seed, 16, PAGE);
    random = &world.random;
    walker_init(&ab, p.a, p.ra, p.rb, 2);
    walker_init(&ba, p.b, p.rb, p.ra, 1);
    epoch = carrier_epoch(p.a);
    for (uint32_t round = 0; round < SEND_WALK_ROUNDS; ++round) {
        for (uint32_t d = 0; d < 2; ++d) {
            struct walker *w = walkers[d];
            uint32_t k = test_random_bounded(random, 5);
            /* Mostly within the queue's room, sometimes past it. */
            uint32_t room = 16 - w->sender->io->links.nodes[0].queue_count;

            if (k > room + 1) {
                k = room + 1;
            }
            for (uint32_t i = 0; i < k && w->outstanding < SEND_WALK_OUT; ++i) {
                walker_send(w, walk_size(random));
            }
        }
        if (test_random_bounded(random, 3) == 0) {
            world.short_send = test_random_bounded(random, 2) == 0
                                   ? 0
                                   : 1 + test_random_bounded(random, 600);
        }
        if (test_random_bounded(random, 3) == 0) {
            world.slice_max = test_random_bounded(random, 2) == 0
                                  ? 0
                                  : 1 + test_random_bounded(random, 500);
        }
        if (test_random_bounded(random, 4) == 0) {
            world.hold_notifs = !world.hold_notifs;
        }
        if (world.held_count > 0 && test_random_bounded(random, 2) == 0) {
            world_release_notif(test_random_bounded(random, world.held_count));
        }
        if (test_random_bounded(random, 40) == 0 &&
            p.a->io->links.nodes[0].carrier != NONE) {
            const struct vsr_io_link *carrier = carrier_of(p.a, 2);

            if (carrier->fd >= 0) {
                link_reset(p.a, carrier);
                resets++;
            }
        }
        if (test_random_bounded(random, 30) == 0) {
            world_advance(IDLE_NS);
        } else if (test_random_bounded(random, 10) == 0) {
            world_advance(4 * BACKOFF_NS);
        }
        /* A bounded run: under 1-byte sends a frame takes thousands of
         * steps, and the next round finds the world mid-way. */
        (void)world_run(1 + test_random_bounded(random, 40));
        if (carrier_epoch(p.a) != epoch) {
            epoch = carrier_epoch(p.a);
            epochs++;
            ab.floor = ab.next;
            ba.floor = ba.next;
        }
        for (uint32_t d = 0; d < 2; ++d) {
            walker_completions(walkers[d]);
            walker_deliveries(walkers[d], random);
            walker_release(walkers[d], false, random);
        }
    }
    /* Wind down: every op completes, every message in flight arrives or
     * is lost with its link, every lease goes back. */
    world.short_send = 0;
    world.slice_max = 0;
    world.hold_notifs = false;
    world_release_notifs();
    for (uint32_t attempt = 0;; ++attempt) {
        CHECK(attempt < 64);
        world_settle_rounds(5000);
        if (carrier_epoch(p.a) != epoch) {
            epoch = carrier_epoch(p.a);
            epochs++;
            ab.floor = ab.next;
            ba.floor = ba.next;
        }
        for (uint32_t d = 0; d < 2; ++d) {
            walker_completions(walkers[d]);
            walker_deliveries(walkers[d], random);
            walker_release(walkers[d], true, random);
        }
        if (ab.outstanding == 0 && ba.outstanding == 0 &&
            p.ra->messages_count == 0 && p.rb->messages_count == 0 &&
            world.held_count == 0 && links_drained(p.a) && links_drained(p.b)) {
            break;
        }
        world_advance(16 * BACKOFF_NS);
    }
    for (uint32_t d = 0; d < 2; ++d) {
        const struct walker *w = walkers[d];

        for (uint64_t n = 1; n < w->next; ++n) {
            CHECK(w->status[n] != SEND_WALK_PENDING);
        }
        CHECK(w->sender->io->links.nodes[0].queue_count == 0);
        CHECK(w->held_count == 0);
    }
    CHECK(ab.oks + ab.retries == p.a->io->stats.messages_sent +
                                     p.a->io->stats.messages_retried -
                                     ab.refused);
    CHECK(p.a->io->stats.messages_sent == ab.oks);
    CHECK(p.b->io->stats.messages_sent == ba.oks);
    CHECK(pool_refs(p.a) == send_slabs(p.a));
    CHECK(pool_refs(p.b) == send_slabs(p.b));
    printf("link: send walk a->b %" PRIu64 " ok %" PRIu64 " retry %" PRIu64
           " refused %" PRIu64 " delivered %" PRIu64 " skipped, b->a %" PRIu64
           " ok %" PRIu64 " retry %" PRIu64 " refused %" PRIu64
           " delivered %" PRIu64 " skipped, %" PRIu64 " resets, %" PRIu64
           " epochs\n",
           ab.oks, ab.retries, ab.refused, ab.delivered, ab.skipped, ba.oks,
           ba.retries, ba.refused, ba.delivered, ba.skipped, resets, epochs);
    engine_forget(p.a);
    engine_forget(p.b);
}

int main(int argc, char **argv)
{
    uint64_t seed = 0x5EED2u;

    if (argc > 1) {
        seed = strtoull(argv[1], NULL, 0);
    }
    test_tables();
    test_backoff();
    test_link_wanted();
    test_handshake(0);
    test_handshake(1);
    test_handshake(5);
    test_handshake(64);
    test_election();
    test_refusals();
    test_external();
    test_adopt_and_close();
    test_listener_and_pressure();
    test_capacity();
    test_receive_splits();
    test_receive_fill();
    test_receive_pressure();
    test_receive_random(seed);
    test_send_flags();
    test_send_coalesce();
    test_send_short();
    test_send_pressure();
    test_send_loss();
    test_send_switch();
    test_send_reject();
    test_stream();
    test_send_random(seed);
    printf("link: ok\n");
    return 0;
}
