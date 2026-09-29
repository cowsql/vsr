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
    struct vsr_io_cqe held[CQ_CAP]; /* NOTIFs held back, with the engine. */
    uint32_t held_engine[CQ_CAP];
    uint32_t held_count;
};

static struct world world;
static _Alignas(4096) unsigned char metadata[ENGINES][1u << 20];
static _Alignas(4096) unsigned char payload[ENGINES][SLABS * PAGE];

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
    limits->link_queue = 4;
    limits->streams = 2;
    limits->stream_window = 2;
    limits->events = 8;
    limits->ops = 6;
    limits->batch = SQ_CAP;
    limits->slabs = SLABS;
    limits->slab_bytes = PAGE;
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

static void world_release_notifs(void)
{
    for (uint32_t i = 0; i < world.held_count; ++i) {
        cq_push(&world.engines[world.held_engine[i]], &world.held[i]);
    }
    world.held_count = 0;
}

/* Steps every open engine until a full round moves nothing. */
static void world_settle(void)
{
    for (uint32_t round = 0; round < 200; ++round) {
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
            return;
        }
    }
    CHECK(false); /* The world did not settle. */
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
    /* Sends queued for the node keep the carrier (phase 3 fills the
     * queue; here the count is set by hand); the other link expires after
     * the idle timeout and is closed at both ends, the carrier stays. */
    a->io->links.nodes[0].queue_count = 1;
    b->io->links.nodes[0].queue_count = 1;
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
    a->io->links.nodes[0].queue_count = 0;
    b->io->links.nodes[0].queue_count = 0;
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

int main(int argc, char **argv)
{
    (void)argc;
    (void)argv;
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
    printf("link: ok\n");
    return 0;
}
