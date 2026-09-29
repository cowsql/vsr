/* The GNU declarations (O_DIRECT, AT_FDCWD) precede every system header. */
#define _GNU_SOURCE
#include "config.h"

#include "io/codec.h"
#include "io/crc32c.h"
#include "io/engine.h"
#include "io/link.h"
#include "io/snapshot.h"
#include "io/store.h"
#include "io/stream.h"
#include "lib/check.h"
#include "lib/random.h"
#include "vsr-io.h"

#include <errno.h>
#include <fcntl.h>
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
#include <sys/stat.h>
#include <sys/un.h>

/*
 * Snapshot module unit test: the clients-file writer and reader, the joint
 * ops and the library streams, over a real store, real links and real
 * streams. The world of tests/unit/stream.c (copied: two engines with
 * fake executors over modelled sockets) gains a DIRECTORY MODEL per
 * engine: named files behind registered slots that honour OPENAT, CLOSE,
 * READ, WRITE, FSYNC, FALLOCATE, STATX, RENAMEAT and UNLINKAT, with a
 * flushed shadow per file and durable names so a crash keeps only what
 * was synced. Each engine has one replica with a real store over that
 * directory (set up by hand as tests/unit/store.c does) and the snapshot
 * module beside it; the test plays engine part 2: it routes completions by
 * slot kind, runs the polls and prepares, hands the core's snapshot ops to
 * the module, completes the forwarded halves as the caller and reads the
 * core completions the module queues.
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
#define FILE_SLOTS 24u
#define FILE_SLOT_BASE 40u
#define REGION_BASE 5u
#define TAIL_REGION (REGION_BASE + 1u)
#define GROUP 3u
#define OWNER 0x5Au
#define TEST_OWNER NONE
#define LINKS 4u
#define NODES 4u
#define STREAMS 2u
#define WINDOW 2u
#define OPS 8u
#define CHUNK UINT64_C(300) /* stream_chunk_bytes: a file spans chunks. */
#define BACKOFF_NS UINT64_C(1000000)
#define HANDSHAKE_NS UINT64_C(50000000)
#define IDLE_NS UINT64_C(200000000)
#define REPLICA 0u
#define REGIONS 8u
#define LEASE_BYTES 16384u
#define BLOCK UINT64_C(512)
#define DIRECTORY "store-dir"
#define DFILES 8u
#define DFILE_BYTES (1u << 18)
#define DNAME_BYTES 96u
#define HELD_MAX 16u
#define TXN_MAX 64u
/* Deadline handles of replica 0: LINK (4) + DIAL (4) + CORE (2) = 10. */
#define DEADLINE_FLUSH 10u
#define DEADLINE_SYNC 12u
#define DEADLINE_CAPTURE 14u
#define OPERATIONS 16u

/* members, operations, input_leases, pending_requests, pending_reads,
 * transfers, log_cache_entries, client_cache_entries, batch_entries,
 * spans_per_blob, work_per_step, command_bytes, result_bytes,
 * manifest_bytes, message_bytes, pinned_payload_bytes. */
static const struct vsr_limits limits = {
    3, OPERATIONS, 4, 4, 4, 2, 16, 8, 4, 2, 16, 256, 64, 64, 1024, 1024};

/* -------------------------------------------------------------------------
 * The world: sockets, files and the clock
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
    bool nodelay;
    unsigned char inbox[INBOX_BYTES];
    uint32_t inbox_len;
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
    int32_t fd;
    uint64_t offset;
};

/* A file of the directory model. */
struct dfile {
    bool used;
    char name[DNAME_BYTES];         /* Empty once unlinked. */
    char durable_name[DNAME_BYTES]; /* As of the last directory fsync. */
    uint32_t refs;                  /* Open slots. */
    uint32_t flags;                 /* Flags of the last open. */
    uint64_t size;
    uint64_t flushed_size;
    uint32_t writes;
    uint32_t reads;
    uint32_t fsyncs;
    unsigned char *data;    /* [DFILE_BYTES] */
    unsigned char *flushed; /* [DFILE_BYTES] */
};

struct fslot {
    int file; /* dfile index, -1 free, -2 the directory. */
    uint32_t flags;
};

struct held {
    struct vsr_io_sqe sqe;
    bool used;
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
    uint32_t log_count;
    uint32_t records;
    uint32_t deliveries;
    uint32_t updates;
    int fail_update;
    struct vsr_io_address listen;
    /* The directory. */
    struct dfile files[DFILES];
    struct fslot fslots[FILE_SLOTS];
    uint32_t dir_fsyncs;
    uint32_t dir_opens;
    /* Faults on the next matching record. */
    int fail_open;
    int fail_write; /* Pool-buffer writes only (the module's). */
    int fail_read;  /* Pool-buffer reads of clients files. */
    int fail_fsync;
    int fail_fsync_dir;
    int fail_rename;
    int fail_unlink;
    int fail_log_read;     /* Pool-buffer reads of the log. */
    bool short_write;      /* The next pool-buffer write stops halfway. */
    bool short_log_read;   /* The next pool-buffer log read returns 8. */
    bool hold_pool_writes; /* Pool-buffer writes are held. */
    bool hold_rename;
    bool hold_fsync;
    bool hold_open;  /* Opens of clients files are held. */
    bool hold_reads; /* Pool-buffer reads of clients files are held. */
    struct held held[HELD_MAX];
    /* The replica's regions. */
    struct vsr_io_replica *replica;
    struct vsr_io_store *store;
    struct vsr_io_snapshots *snapshots;
    struct vsr_io_store_options options;
    struct vsr_io_lease leases[REGIONS];
    struct vsr_io_queued_event completions[OPERATIONS];
    uint64_t next_op;
    uint64_t header_bytes;
    uint64_t max_record;
    size_t tail_bytes;
};

struct world {
    struct sock socks[SOCKETS];
    struct engine engines[ENGINES];
    uint64_t now;
    int next_fd;
    struct test_random random;
    uint32_t slice_max;
};

static struct world world;
static _Alignas(4096) unsigned char metadata[ENGINES][1u << 20];
static _Alignas(4096) unsigned char payload[ENGINES][SLABS * PAGE];
static _Alignas(4096) unsigned char store_metadata[ENGINES][1u << 20];
static _Alignas(4096) unsigned char tail_memory[ENGINES][1u << 20];
static _Alignas(16) unsigned char lease_memory[ENGINES][REGIONS * LEASE_BYTES];
static _Alignas(16) unsigned char snapshot_memory[ENGINES][1u << 17];
static unsigned char file_data[ENGINES][DFILES][DFILE_BYTES];
static unsigned char file_flushed[ENGINES][DFILES][DFILE_BYTES];

/* A record's buffer is written through its const addr, as the simulation
 * does (vsr_sim_mutable). */
static void *mutable_of(const void *pointer)
{
    void *writable;

    memcpy(&writable, &pointer, sizeof(writable));
    return writable;
}

static uint64_t round_up(uint64_t value, uint64_t multiple)
{
    return (value + multiple - 1) / multiple * multiple;
}

/* -------------------------------------------------------------------------
 * Sockets (tests/unit/stream.c)
 * ---------------------------------------------------------------------- */

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
    CHECK(false);
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
 * The directory model
 * ---------------------------------------------------------------------- */

static int dfile_find(const struct engine *e, const char *name)
{
    for (uint32_t i = 0; i < DFILES; ++i) {
        if (e->files[i].used && strcmp(e->files[i].name, name) == 0) {
            return (int)i;
        }
    }
    return -1;
}

static int dfile_create(struct engine *e, const char *name)
{
    for (uint32_t i = 0; i < DFILES; ++i) {
        struct dfile *f = &e->files[i];

        if (!f->used) {
            memset(f, 0, sizeof(*f));
            f->used = true;
            CHECK(strlen(name) < DNAME_BYTES);
            strcpy(f->name, name);
            f->data = file_data[e->index][i];
            f->flushed = file_flushed[e->index][i];
            memset(f->data, 0, DFILE_BYTES);
            memset(f->flushed, 0, DFILE_BYTES);
            return (int)i;
        }
    }
    CHECK(false);
    return -1;
}

/* A file with no name and no open slot is gone. */
static void dfile_reap(struct dfile *f)
{
    if (f->used && f->refs == 0 && f->name[0] == 0) {
        f->used = false;
    }
}

static struct fslot *fslot_of(struct engine *e, int32_t slot)
{
    CHECK(slot >= (int32_t)FILE_SLOT_BASE &&
          slot < (int32_t)(FILE_SLOT_BASE + FILE_SLOTS));
    return &e->fslots[(uint32_t)slot - FILE_SLOT_BASE];
}

/* The file an open slot names. */
static struct dfile *dfile_of_slot(struct engine *e, int32_t slot)
{
    struct fslot *s = fslot_of(e, slot);

    CHECK(s->file >= 0);
    return &e->files[s->file];
}

/* The file named `name` (a clients file or the log), or NULL. */
static const struct dfile *dfile_named(const struct engine *e, const char *name)
{
    int i = dfile_find(e, name);

    return i < 0 ? NULL : &e->files[i];
}

/* The store's log file, which must exist. */
static const struct dfile *log_file(const struct engine *e)
{
    const struct dfile *f = dfile_named(e, DIRECTORY "/log");

    CHECK(f != NULL);
    return f;
}

static bool in_pool(const struct engine *e, const void *addr, size_t length)
{
    return vsr_io_pool_contains(&e->io->pool, addr, length);
}

static bool in_tail(const struct engine *e, const void *addr, size_t length)
{
    uintptr_t p = (uintptr_t)addr;
    uintptr_t base = (uintptr_t)tail_memory[e->index];

    return p >= base && length <= e->tail_bytes &&
           p - base <= e->tail_bytes - length;
}

/* The directory reaches the state of its last fsync and every file the
 * state of its last fsync: what a crash keeps. */
static void dir_crash(struct engine *e)
{
    for (uint32_t i = 0; i < DFILES; ++i) {
        struct dfile *f = &e->files[i];

        if (!f->used) {
            continue;
        }
        f->refs = 0;
        if (f->durable_name[0] == 0) {
            f->used = false;
            continue;
        }
        strcpy(f->name, f->durable_name);
        memcpy(f->data, f->flushed, DFILE_BYTES);
        f->size = f->flushed_size;
    }
    for (uint32_t i = 0; i < FILE_SLOTS; ++i) {
        e->fslots[i].file = -1;
    }
}

/* Applies a file record; returns false when the record is held. */
static bool dir_apply(struct engine *e, const struct vsr_io_sqe *sqe,
                      int *result)
{
    struct dfile *f;
    struct fslot *s;

    switch (sqe->opcode) {
    case VSR_IO_SQE_OPENAT: {
        uint32_t flags = sqe->op_flags;
        int file;

        CHECK(sqe->fd == AT_FDCWD);
        CHECK(sqe->flags == VSR_IO_SQE_DIRECT);
        CHECK(sqe->fd2 >= (int32_t)FILE_SLOT_BASE);
        s = fslot_of(e, sqe->fd2);
        CHECK(s->file == -1);
        if (e->fail_open != 0) {
            *result = e->fail_open;
            e->fail_open = 0;
            return true;
        }
        if ((flags & O_DIRECTORY) != 0) {
            CHECK(strcmp(sqe->addr, DIRECTORY) == 0);
            s->file = -2;
            s->flags = flags;
            e->dir_opens++;
            *result = sqe->fd2;
            return true;
        }
        CHECK(strncmp(sqe->addr, DIRECTORY "/", sizeof(DIRECTORY)) == 0);
        if (e->hold_open && strstr(sqe->addr, "/clients-") != NULL) {
            return false;
        }
        file = dfile_find(e, sqe->addr);
        if ((flags & O_CREAT) != 0) {
            CHECK(sqe->length == 0644);
            if (file >= 0 && (flags & O_EXCL) != 0) {
                *result = -EEXIST;
                return true;
            }
            if (file < 0) {
                file = dfile_create(e, sqe->addr);
            } else if ((flags & O_TRUNC) != 0) {
                e->files[file].size = 0;
            }
        } else if (file < 0) {
            *result = -ENOENT;
            return true;
        }
        f = &e->files[file];
        f->refs++;
        f->flags = flags;
        s->file = file;
        s->flags = flags;
        *result = sqe->fd2;
        return true;
    }
    case VSR_IO_SQE_CLOSE:
        CHECK((sqe->flags & VSR_IO_SQE_FIXED_FILE) != 0);
        s = fslot_of(e, sqe->fd);
        CHECK(s->file != -1);
        if (s->file >= 0) {
            f = &e->files[s->file];
            CHECK(f->refs > 0);
            f->refs--;
            dfile_reap(f);
        }
        s->file = -1;
        *result = 0;
        return true;
    case VSR_IO_SQE_FALLOCATE:
        CHECK(sqe->flags == VSR_IO_SQE_FIXED_FILE);
        f = dfile_of_slot(e, sqe->fd);
        CHECK(sqe->offset + sqe->length <= DFILE_BYTES);
        if (sqe->offset + sqe->length > f->size) {
            f->size = sqe->offset + sqe->length;
        }
        *result = 0;
        return true;
    case VSR_IO_SQE_WRITE: {
        bool pool = in_pool(e, sqe->addr, sqe->length);
        uint64_t end = sqe->offset + sqe->length;

        CHECK(sqe->flags == (VSR_IO_SQE_FIXED_FILE | VSR_IO_SQE_FIXED_BUFFER));
        CHECK(pool || in_tail(e, sqe->addr, sqe->length));
        CHECK(sqe->buffer_index ==
              (pool ? e->io->pool.region_index : TAIL_REGION));
        f = dfile_of_slot(e, sqe->fd);
        s = fslot_of(e, sqe->fd);
        if ((s->flags & O_DIRECT) != 0) {
            CHECK(sqe->offset % BLOCK == 0 && sqe->length % BLOCK == 0);
        }
        CHECK(end <= DFILE_BYTES);
        if (pool && e->hold_pool_writes) {
            return false;
        }
        if (pool && e->fail_write != 0) {
            *result = e->fail_write;
            e->fail_write = 0;
            return true;
        }
        if (pool && e->short_write && sqe->length > 1) {
            e->short_write = false;
            memcpy(f->data + sqe->offset, sqe->addr, sqe->length / 2);
            if (sqe->offset + sqe->length / 2 > f->size) {
                f->size = sqe->offset + sqe->length / 2;
            }
            *result = (int32_t)(sqe->length / 2);
            return true;
        }
        memcpy(f->data + sqe->offset, sqe->addr, sqe->length);
        if (end > f->size) {
            f->size = end;
        }
        if ((s->flags & O_DSYNC) != 0) {
            memcpy(f->flushed + sqe->offset, sqe->addr, sqe->length);
            if (end > f->flushed_size) {
                f->flushed_size = end;
            }
        }
        f->writes++;
        *result = (int32_t)sqe->length;
        return true;
    }
    case VSR_IO_SQE_READ: {
        uint64_t end = sqe->offset + sqe->length;
        bool pool = in_pool(e, sqe->addr, sqe->length);

        CHECK((sqe->flags & VSR_IO_SQE_FIXED_FILE) != 0);
        CHECK(pool || in_tail(e, sqe->addr, sqe->length));
        f = dfile_of_slot(e, sqe->fd);
        s = fslot_of(e, sqe->fd);
        if ((s->flags & O_DIRECT) != 0) {
            CHECK(sqe->offset % BLOCK == 0 && sqe->length % BLOCK == 0);
        }
        if (pool && strcmp(f->name, DIRECTORY "/log") != 0 && e->hold_reads) {
            return false;
        }
        f->reads++;
        if (pool && strcmp(f->name, DIRECTORY "/log") != 0 &&
            e->fail_read != 0) {
            *result = e->fail_read;
            e->fail_read = 0;
            return true;
        }
        if (pool && strcmp(f->name, DIRECTORY "/log") == 0 &&
            e->fail_log_read != 0) {
            *result = e->fail_log_read;
            e->fail_log_read = 0;
            return true;
        }
        if (pool && strcmp(f->name, DIRECTORY "/log") == 0 &&
            e->short_log_read && sqe->offset < f->size) {
            e->short_log_read = false;
            memcpy(mutable_of(sqe->addr), f->data + sqe->offset, 8);
            *result = 8;
            return true;
        }
        if (sqe->offset >= f->size) {
            *result = 0;
            return true;
        }
        if (end > f->size) {
            end = f->size;
        }
        memcpy(mutable_of(sqe->addr), f->data + sqe->offset, end - sqe->offset);
        *result = (int32_t)(end - sqe->offset);
        return true;
    }
    case VSR_IO_SQE_FSYNC:
        CHECK(sqe->flags == VSR_IO_SQE_FIXED_FILE);
        s = fslot_of(e, sqe->fd);
        CHECK(s->file != -1);
        if (s->file == -2) {
            CHECK(sqe->op_flags == 0);
            if (e->fail_fsync_dir != 0) {
                *result = e->fail_fsync_dir;
                e->fail_fsync_dir = 0;
                return true;
            }
            for (uint32_t i = 0; i < DFILES; ++i) {
                if (e->files[i].used) {
                    strcpy(e->files[i].durable_name, e->files[i].name);
                }
            }
            e->dir_fsyncs++;
            *result = 0;
            return true;
        }
        CHECK(sqe->op_flags == VSR_IO_FSYNC_DATASYNC);
        f = &e->files[s->file];
        if (e->hold_fsync) {
            return false;
        }
        if (e->fail_fsync != 0) {
            *result = e->fail_fsync;
            e->fail_fsync = 0;
            return true;
        }
        memcpy(f->flushed, f->data, DFILE_BYTES);
        f->flushed_size = f->size;
        f->fsyncs++;
        *result = 0;
        return true;
    case VSR_IO_SQE_STATX: {
        struct statx *out;
        int file;

        CHECK(sqe->fd == AT_FDCWD && sqe->flags == 0);
        CHECK((sqe->length & STATX_SIZE) != 0 && sqe->addr2 != NULL);
        out = (struct statx *)mutable_of(sqe->addr2);
        file = dfile_find(e, sqe->addr);
        if (file < 0) {
            *result = -ENOENT;
            return true;
        }
        memset(out, 0, sizeof(*out));
        out->stx_mask = STATX_SIZE;
        out->stx_size = e->files[file].size;
        *result = 0;
        return true;
    }
    case VSR_IO_SQE_RENAMEAT: {
        int from;
        int to;

        CHECK(sqe->fd == AT_FDCWD && sqe->flags == 0);
        if (e->hold_rename) {
            return false;
        }
        if (e->fail_rename != 0) {
            *result = e->fail_rename;
            e->fail_rename = 0;
            return true;
        }
        from = dfile_find(e, sqe->addr);
        if (from < 0) {
            *result = -ENOENT;
            return true;
        }
        to = dfile_find(e, sqe->addr2);
        if (to >= 0) {
            e->files[to].name[0] = 0;
            dfile_reap(&e->files[to]);
        }
        CHECK(strlen(sqe->addr2) < DNAME_BYTES);
        strcpy(e->files[from].name, sqe->addr2);
        *result = 0;
        return true;
    }
    case VSR_IO_SQE_UNLINKAT: {
        int file;

        CHECK(sqe->fd == AT_FDCWD && sqe->flags == 0);
        if (e->fail_unlink != 0) {
            *result = e->fail_unlink;
            e->fail_unlink = 0;
            return true;
        }
        file = dfile_find(e, sqe->addr);
        if (file < 0) {
            *result = -ENOENT;
            return true;
        }
        e->files[file].name[0] = 0;
        dfile_reap(&e->files[file]);
        *result = 0;
        return true;
    }
    default:
        CHECK(false);
        return true;
    }
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
        /* A file slot cleared by the snapshot module or the store. */
        struct fslot *s = &e->fslots[slot - FILE_SLOT_BASE];

        if (s->file >= 0) {
            CHECK(e->files[s->file].refs > 0);
            e->files[s->file].refs--;
            dfile_reap(&e->files[s->file]);
        }
        s->file = -1;
        return 0;
    }
    index = sock_by_fd(e->index, fd);
    if (index == NONE) {
        return -EBADF;
    }
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
    entry->fd = sqe->fd;
    entry->offset = sqe->offset;
}

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

/* Keeps a record for later (world_release_held). */
static void hold_record(struct engine *e, const struct vsr_io_sqe *sqe)
{
    for (uint32_t i = 0; i < HELD_MAX; ++i) {
        if (!e->held[i].used) {
            e->held[i].used = true;
            e->held[i].sqe = *sqe;
            return;
        }
    }
    CHECK(false);
}

static uint32_t held_count(const struct engine *e)
{
    uint32_t n = 0;

    for (uint32_t i = 0; i < HELD_MAX; ++i) {
        n += e->held[i].used ? 1 : 0;
    }
    return n;
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
    case VSR_IO_SQE_OPENAT:
    case VSR_IO_SQE_FALLOCATE:
    case VSR_IO_SQE_WRITE:
    case VSR_IO_SQE_READ:
    case VSR_IO_SQE_FSYNC:
    case VSR_IO_SQE_STATX:
    case VSR_IO_SQE_RENAMEAT:
    case VSR_IO_SQE_UNLINKAT:
        if (!dir_apply(e, sqe, &result)) {
            hold_record(e, sqe);
            e->records--;
            return;
        }
        break;
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
        sock->recv_addr = mutable_of(sqe->addr);
        sock->recv_len = sqe->length;
        return;
    case VSR_IO_SQE_SEND: {
        const struct vsr_io_vec *vecs = sqe->addr;
        bool zero_copy = (sqe->op_flags & VSR_IO_SEND_ZERO_COPY) != 0;

        CHECK((sqe->op_flags & VSR_IO_SEND_VECTORED) != 0);
        index = record_sock(e, sqe);
        CHECK(index != NONE);
        sock = &world.socks[index];
        if (sock->peer == NONE || sock->shutdown) {
            result = -EPIPE;
        } else {
            uint32_t sent = 0;

            for (uint32_t i = 0; i < sqe->length; ++i) {
                inbox_append(sock->peer, vecs[i].base, vecs[i].length);
                sent += (uint32_t)vecs[i].length;
            }
            result = (int)sent;
        }
        if (zero_copy) {
            complete(e, sqe->user_data, result, VSR_IO_CQE_MORE, 0);
            complete(e, sqe->user_data, 0, VSR_IO_CQE_NOTIF, 0);
            return;
        }
        break;
    }
    case VSR_IO_SQE_SETSOCKOPT:
        index = record_sock(e, sqe);
        CHECK(index != NONE);
        world.socks[index].nodelay = true;
        break;
    case VSR_IO_SQE_SHUTDOWN:
        index = record_sock(e, sqe);
        CHECK(index != NONE);
        sock = &world.socks[index];
        sock->shutdown = true;
        if (sock->peer != NONE) {
            world.socks[sock->peer].peer_closed = true;
        }
        break;
    case VSR_IO_SQE_CLOSE:
        index = record_sock(e, sqe);
        if (index == NONE) {
            /* A file slot. */
            if (!dir_apply(e, sqe, &result)) {
                hold_record(e, sqe);
                e->records--;
                return;
            }
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
        cancel_record(e, sqe->offset, &result);
        break;
    default:
        CHECK(false);
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

/* Executes the records held so far (the faults that held them cleared
 * by the test first). */
static void world_release_held(struct engine *e)
{
    bool chain_failed = false;

    for (uint32_t i = 0; i < HELD_MAX; ++i) {
        if (e->held[i].used) {
            struct vsr_io_sqe sqe = e->held[i].sqe;

            e->held[i].used = false;
            execute(e, &sqe, &chain_failed);
        }
    }
}

static void deliver(struct engine *e)
{
    for (uint32_t i = 0; i < SOCKETS; ++i) {
        struct sock *sock = &world.socks[i];

        while (sock->used && sock->owner == e->index && sock->recv_armed) {
            uint32_t take;

            if (e->cq_count + 4 > CQ_CAP) {
                return;
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
 * Engines and their replica
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

static struct vsr_io_options engine_options(struct engine *e, uint64_t node)
{
    struct vsr_io_options options;
    struct vsr_io_limits *l = &options.limits;

    memset(&options, 0, sizeof(options));
    options.executor.ops = &fake_ops;
    options.executor.ctx = e;
    options.node = node;
    options.listen = &e->listen;
    options.listen_count = 1;
    options.handshake = VSR_IO_HANDSHAKE_TRUSTED;
    l->replicas = 2;
    l->nodes = NODES;
    l->authorizations = 8;
    l->links = LINKS;
    l->link_queue = 4;
    l->streams = STREAMS;
    l->stream_window = WINDOW;
    l->events = 8;
    l->ops = OPS;
    l->batch = SQ_CAP;
    l->slabs = SLABS;
    l->slab_bytes = PAGE;
    /* The caller's untaken share stays FREE (decision 54); the internal
     * acquires of the links (send slabs) and of this module (staging) can
     * take it. Without it the ring holds every slab above the reserve of
     * replicas + 1 while idle peer links return none, and a module slab
     * can starve a new stream link's send slab (docs section 11). */
    l->caller_slabs = 8;
    l->file_slots = FILE_SLOTS;
    l->buffer_regions = 3;
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
    options.stream_chunk_bytes = (uint32_t)CHUNK;
    return options;
}

static struct vsr_io_store_options store_options(void)
{
    struct vsr_io_store_options o;
    uint64_t header_bytes;
    uint64_t max_record;
    size_t segment_limit = 0;

    CHECK(vsr_io_codec_record_limit(&limits, &max_record) == VSR_OK);
    CHECK(vsr_io_codec_segment_limit(&limits, &segment_limit) == VSR_OK);
    header_bytes = round_up(segment_limit, BLOCK);
    memset(&o, 0, sizeof(o));
    o.block_bytes = BLOCK;
    o.segments = 2;
    o.max_segments = 4;
    o.max_entries = 4096;
    o.max_clients = 8;
    o.inflight_writes = 2;
    o.segment_bytes = round_up(header_bytes + 4 * max_record, BLOCK);
    o.write_behind_bytes = 4 * BLOCK;
    o.cache_bytes =
        round_up(o.write_behind_bytes + limits.pinned_payload_bytes +
                     2 * max_record + 2 * header_bytes + BLOCK,
                 BLOCK);
    o.direct_io = 1;
    o.sync_mode = VSR_IO_SYNC_FDATASYNC;
    o.on_write_error = VSR_IO_WRITE_ERROR_FENCE;
    return o;
}

/* Sets replica 0 of the engine up by hand: the core options the modules
 * read, the lease table, the completion ring, the store and the snapshot
 * module. The directory model is left as it is (a restart keeps it). */
static void replica_setup(struct engine *e, uint64_t replica_id)
{
    struct vsr_io_replica *rep = &e->io->replicas[REPLICA];
    size_t bytes = 0;
    size_t alignment = 0;
    size_t tail_alignment = 0;
    uint64_t max_record;
    size_t segment_limit = 0;

    e->replica = rep;
    rep->state = VSR_IO_REPLICA_RUNNING;
    rep->options.limits = limits;
    rep->options.durability = VSR_DURABLE;
    rep->options.cluster.hi = 0x77;
    rep->options.cluster.lo = 0x99;
    rep->options.replica = replica_id;
    rep->deadline_flush = DEADLINE_FLUSH;
    rep->deadline_sync = DEADLINE_SYNC;
    rep->deadline_capture = DEADLINE_CAPTURE;
    vsr_io_deadlines_bind(&e->io->deadlines, DEADLINE_CAPTURE,
                          VSR_IO_DEADLINE_CAPTURE, REPLICA);
    CHECK(vsr_io_codec_load_region(&limits, &bytes) == VSR_OK &&
          bytes <= LEASE_BYTES);
    memset(e->leases, 0, sizeof(e->leases));
    for (uint32_t i = 0; i < REGIONS; ++i) {
        e->leases[i].slab = NONE;
        e->leases[i].pin = NONE;
        vsr_io_bump_init(&e->leases[i].region,
                         lease_memory[e->index] + (size_t)i * LEASE_BYTES,
                         LEASE_BYTES);
    }
    rep->leases = e->leases;
    rep->regions_count = REGIONS;
    rep->leases_free = REGIONS;
    memset(e->completions, 0, sizeof(e->completions));
    rep->completions = e->completions;
    rep->completions_head = 0;
    rep->completions_count = 0;
    e->store = &rep->store;
    e->snapshots = &rep->snapshots;
    e->options = store_options();
    CHECK(vsr_io_codec_record_limit(&limits, &max_record) == VSR_OK);
    CHECK(vsr_io_codec_segment_limit(&limits, &segment_limit) == VSR_OK);
    e->header_bytes = round_up(segment_limit, BLOCK);
    e->max_record = max_record;
    CHECK(vsr_io_store_check(&e->options, &limits, PAGE) == VSR_OK);
    CHECK(vsr_io_store_size(&e->options, &limits, REGIONS, &bytes, &alignment,
                            &e->tail_bytes, &tail_alignment) == VSR_OK);
    CHECK(bytes <= sizeof(store_metadata[e->index]) &&
          e->tail_bytes <= sizeof(tail_memory[e->index]));
    memset(store_metadata[e->index], 0xEE, sizeof(store_metadata[e->index]));
    vsr_io_store_init(e->store, store_metadata[e->index], bytes,
                      tail_memory[e->index], e->tail_bytes, &e->options,
                      &limits, REGIONS, TAIL_REGION, DIRECTORY, AT_FDCWD);
    CHECK(vsr_io_snapshots_size(&limits, &e->io->options.limits,
                                e->options.max_clients, &bytes,
                                &alignment) == VSR_OK);
    CHECK(bytes <= sizeof(snapshot_memory[e->index]) && alignment <= 16);
    vsr_io_snapshots_init(e->snapshots, snapshot_memory[e->index], bytes,
                          &limits, &e->io->options.limits,
                          e->options.max_clients);
    e->next_op = 1;
}

/* Opens engine `index` (node = index + 1, listening at 'A' + index) with
 * its replica; the directory model is reset unless `keep`. */
static struct engine *engine_open_keep(uint32_t index, bool keep)
{
    struct engine *e = &world.engines[index];
    struct vsr_io_options options;
    struct vsr_io_layout layout;
    struct vsr_io_region region;
    struct vsr_io_region pool;
    struct dfile files[DFILES];
    uint64_t next_op = e->next_op;

    memcpy(files, e->files, sizeof(files));
    memset(e, 0, sizeof(*e));
    e->index = index;
    e->node = index + 1;
    if (keep) {
        memcpy(e->files, files, sizeof(files));
    }
    for (uint32_t i = 0; i < FILE_SLOTS; ++i) {
        e->fslots[i].file = -1;
    }
    address_of(&e->listen, (char)('A' + index));
    options = engine_options(e, e->node);
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
    replica_setup(e, index + 1);
    if (keep) {
        e->next_op = next_op;
    }
    return e;
}

static struct engine *engine_open(uint32_t index)
{
    return engine_open_keep(index, false);
}

/* The engine's process dies: sockets close, the directory keeps what was
 * synced; the engine is not deinit'ed (its memory is reused by the next
 * open). */
static void engine_crash(struct engine *e)
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
    dir_crash(e);
    memset(e->held, 0, sizeof(e->held));
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

/* One loop iteration as engine part 2 runs it: completions by slot kind,
 * deadlines by kind, the polls, provision, the prepares, execution. */
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
        const struct vsr_io_slot *record;

        if (vsr_io_slots_resolve(&io->slots, cq[i].user_data, &slot) == NULL) {
            continue;
        }
        record = &io->slots.slots[slot];
        switch (record->kind) {
        case VSR_IO_SLOT_STREAM:
            vsr_io_streams_complete(io, slot, &cq[i]);
            break;
        case VSR_IO_SLOT_CLIENTS:
            vsr_io_snapshots_complete(io, record->owner, slot, &cq[i]);
            break;
        case VSR_IO_SLOT_WRITE:
        case VSR_IO_SLOT_FLUSH:
        case VSR_IO_SLOT_SUPER:
        case VSR_IO_SLOT_LOAD:
        case VSR_IO_SLOT_FILE:
            vsr_io_store_complete(io, record->owner, slot, &cq[i]);
            break;
        default:
            vsr_io_links_complete(io, slot, &cq[i]);
            break;
        }
    }
    while (vsr_io_deadlines_pop(&io->deadlines, world.now, &kind, &index)) {
        if (kind == VSR_IO_DEADLINE_STREAM) {
            vsr_io_streams_deadline(io, index, world.now);
        } else if (kind == VSR_IO_DEADLINE_LINK ||
                   kind == VSR_IO_DEADLINE_DIAL) {
            vsr_io_links_deadline(io, kind, index, world.now);
        }
    }
    vsr_io_links_poll(io, world.now);
    vsr_io_streams_poll(io, world.now);
    vsr_io_store_poll(io, REPLICA, world.now);
    vsr_io_snapshots_poll(io, REPLICA, world.now);
    count = vsr_io_pool_provide(&io->pool, buffers, SLABS);
    if (count > 0) {
        CHECK(fake_provide(e, GROUP, buffers, count) == 0);
    }
    count = 0;
    vsr_io_links_prepare(io, sqes, SQ_CAP, &count);
    vsr_io_streams_prepare(io, sqes, SQ_CAP, &count);
    vsr_io_store_prepare(io, REPLICA, sqes, SQ_CAP, &count);
    vsr_io_snapshots_prepare(io, REPLICA, sqes, SQ_CAP, &count);
    for (uint32_t i = 0; i < count; ++i) {
        execute(e, &sqes[i], &chain_failed);
    }
    deliver(e);
}

/* The stream table's invariants (tests/unit/stream.c). */
static void check_streams(const struct engine *e)
{
    const struct vsr_io_streams *streams = &e->io->streams;
    uint32_t active = 0;

    for (uint32_t i = 0; i < streams->count; ++i) {
        const struct vsr_io_stream *s = &streams->streams[i];

        if (s->state == VSR_IO_STREAM_FREE) {
            CHECK(s->units_used == 0 && s->writes_count == 0);
            continue;
        }
        active++;
        CHECK(s->units_used <= streams->window);
        CHECK(s->writes_count <= streams->window);
    }
    CHECK(active == streams->active);
}

/* The snapshot module's invariants, checked after every step. */
static void check_snapshots(const struct engine *e)
{
    const struct vsr_io_snapshots *s = e->snapshots;
    uint32_t fileops = 0;
    uint32_t used = 0;

    for (uint32_t i = 0; i < VSR_IO_SNAPSHOT_FILEOPS; ++i) {
        fileops += s->fileops[i].used ? 1 : 0;
    }
    for (uint32_t i = 0; i < s->count; ++i) {
        const struct vsr_io_snapshot *entry = &s->entries[i];

        if (entry->state == VSR_IO_SNAPSHOT_FREE) {
            CHECK(entry->file_slot < 0 && entry->tmp_slot == NONE &&
                  entry->fileop == NONE && entry->op == 0);
            continue;
        }
        CHECK(entry->id.hi != 0 || entry->id.lo != 0);
        if (entry->fileop != NONE) {
            used++;
            CHECK(s->fileops[entry->fileop].used);
        }
        if (entry->state == VSR_IO_SNAPSHOT_WRITING) {
            /* The writer holds it until the library half is over; a failed
             * file then waits for the caller and its discard. */
            CHECK(s->capture == i &&
                  (s->writer.snapshot == i || entry->library_status != -1));
        }
        if (entry->state == VSR_IO_SNAPSHOT_FETCHING) {
            CHECK(s->reader.snapshot == i);
        }
        /* An op forwarded is one in progress. */
        CHECK(entry->forwarded == 0 || entry->forwarded == entry->op);
    }
    for (uint32_t i = 0; i < s->serves_count; ++i) {
        if (s->serves[i].fileop != NONE) {
            used++;
        }
    }
    if (s->dir_fileop != NONE) {
        used++;
    }
    CHECK(used == fileops);
    CHECK(s->chunks_count <= s->chunks_capacity);
    CHECK(s->chunks_count == 0 ||
          (s->reader.snapshot != NONE &&
           s->entries[s->reader.snapshot].job == VSR_IO_SNAPSHOT_JOB_FETCH));
    CHECK(s->pending_base == NONE ||
          s->entries[s->pending_base].state != VSR_IO_SNAPSHOT_FREE);
    if (s->writer.snapshot != NONE) {
        CHECK(s->writer.slab != NONE &&
              e->io->pool.entries[s->writer.slab].refs > 0);
        CHECK(s->writer.staged <= PAGE && s->writer.next <= s->writer.count);
    }
    if (s->reader.snapshot != NONE) {
        CHECK(s->reader.slab != NONE &&
              e->io->pool.entries[s->reader.slab].refs > 0);
        CHECK(s->reader.consumed <= s->reader.filled &&
              s->reader.filled <= PAGE);
    }
}

/* The engine file slots the module holds (entries' kept and transient
 * slots, serves, the directory) are distinct and allocated (not on the
 * engine's free list, which has no duplicate), and every slot the model
 * has open besides the log's is one of them: nothing leaks, nothing is
 * freed twice or used after its free. */
static void check_file_slots(const struct engine *e)
{
    const struct vsr_io_snapshots *s = e->snapshots;
    const struct vsr_io *io = e->io;
    uint32_t held[64];
    uint32_t n = 0;

    for (uint32_t i = 0; i < s->count; ++i) {
        const struct vsr_io_snapshot *entry = &s->entries[i];

        if (entry->file_slot >= 0) {
            held[n++] = (uint32_t)entry->file_slot;
        }
        if (entry->tmp_slot != NONE) {
            held[n++] = entry->tmp_slot;
        }
    }
    for (uint32_t i = 0; i < s->serves_count; ++i) {
        if (s->serves[i].slot != NONE) {
            held[n++] = s->serves[i].slot;
        }
    }
    if (s->dir_slot != NONE) {
        held[n++] = s->dir_slot;
    }
    CHECK(n <= FILE_SLOTS);
    for (uint32_t i = 0; i < io->file_slots_free_count; ++i) {
        for (uint32_t j = i + 1; j < io->file_slots_free_count; ++j) {
            CHECK(io->file_slots_free[i] != io->file_slots_free[j]);
        }
    }
    for (uint32_t i = 0; i < n; ++i) {
        CHECK(held[i] >= FILE_SLOT_BASE && held[i] < io->file_slot_next);
        CHECK((int32_t)held[i] != e->store->log_slot);
        for (uint32_t j = i + 1; j < n; ++j) {
            CHECK(held[i] != held[j]);
        }
        for (uint32_t j = 0; j < io->file_slots_free_count; ++j) {
            CHECK(io->file_slots_free[j] != held[i]);
        }
    }
    for (uint32_t i = 0; i < FILE_SLOTS; ++i) {
        uint32_t slot = FILE_SLOT_BASE + i;
        bool found = false;

        if (e->fslots[i].file == -1 || (int32_t)slot == e->store->log_slot ||
            (e->fslots[i].file >= 0 &&
             strcmp(e->files[e->fslots[i].file].name, DIRECTORY "/log") == 0)) {
            continue; /* The store's. */
        }
        for (uint32_t j = 0; j < n; ++j) {
            found = found || held[j] == slot;
        }
        if (!found) {
            fprintf(stderr,
                    "file slot %u (file %d %s) open in the model, not held "
                    "(log slot %d)\n",
                    slot, e->fslots[i].file,
                    e->fslots[i].file >= 0 ? e->files[e->fslots[i].file].name
                                           : "dir",
                    e->store->log_slot);
        }
        CHECK(found);
    }
    /* The store reads the base through a slot the module holds. */
    if (e->store->base_slot >= 0) {
        bool found = false;

        for (uint32_t j = 0; j < n; ++j) {
            found = found || (int32_t)held[j] == e->store->base_slot;
        }
        CHECK(found);
    }
}

/* Every CLIENTS record in the slot table is one of the module's file
 * operations, and at most VSR_IO_SNAPSHOT_FILEOPS are in flight. */
static void check_records(const struct engine *e)
{
    const struct vsr_io_slots *slots = &e->io->slots;
    uint32_t records = 0;
    uint32_t fileops = 0;

    for (uint32_t i = 0; i < slots->count; ++i) {
        if (slots->slots[i].kind == VSR_IO_SLOT_CLIENTS) {
            CHECK(slots->slots[i].owner == REPLICA);
            records++;
        }
    }
    for (uint32_t i = 0; i < VSR_IO_SNAPSHOT_FILEOPS; ++i) {
        if (e->snapshots->fileops[i].used) {
            fileops++;
            CHECK(e->snapshots->fileops[i].slot < slots->count &&
                  slots->slots[e->snapshots->fileops[i].slot].kind ==
                      VSR_IO_SLOT_CLIENTS);
        }
    }
    CHECK(records == fileops && fileops <= VSR_IO_SNAPSHOT_FILEOPS);
}

/* Leases the module reserved are distinct and taken; a copied result
 * lives in its lease. */
static void check_leases(const struct engine *e)
{
    const struct vsr_io_snapshots *s = e->snapshots;

    for (uint32_t i = 0; i < s->count; ++i) {
        const struct vsr_io_snapshot *entry = &s->entries[i];

        if (entry->lease == NONE) {
            CHECK(entry->result == NULL);
            continue;
        }
        CHECK(entry->lease < REGIONS && entry->state != VSR_IO_SNAPSHOT_FREE);
        CHECK(e->replica->leases[entry->lease].state != 0);
        CHECK(entry->op != 0 && (entry->op_type == VSR_OP_SNAPSHOT_CAPTURE ||
                                 entry->op_type == VSR_OP_SNAPSHOT_FETCH));
        for (uint32_t j = i + 1; j < s->count; ++j) {
            CHECK(s->entries[j].lease != entry->lease);
        }
    }
}

/* Steps every open engine until a full round moves nothing. */
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
            check_snapshots(e);
            check_file_slots(e);
            check_records(e);
            check_leases(e);
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
    CHECK(world_run_checked(400));
}

/* Settles, then moves the clock past the retry deadline and settles
 * again, for steps that wait for a resource. */
static void settle_retry(void)
{
    settle();
    world_advance(2000000);
    settle();
}

static uint32_t pool_refs(const struct engine *e)
{
    uint32_t refs = 0;

    for (uint32_t i = 0; i < SLABS; ++i) {
        refs += e->io->pool.entries[i].refs;
    }
    return refs;
}

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

/* The connection of an established link is reset by the network. */
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

/* Two engines, each knowing the other; replica 2 (engine b) is authorized
 * at a for the cluster, so a can fetch from it, and vice versa. */
static void two_engines(void)
{
    struct engine *a = engine_open(0);
    struct engine *b = engine_open(1);
    struct vsr_id cluster = {0x77, 0x99};

    CHECK(vsr_io_links_node_set(a->io, 2, &b->listen) == VSR_OK);
    CHECK(vsr_io_links_node_set(b->io, 1, &a->listen) == VSR_OK);
    CHECK(vsr_io_links_node_set(a->io, 1, NULL) == VSR_OK);
    CHECK(vsr_io_links_node_set(b->io, 2, NULL) == VSR_OK);
    CHECK(vsr_io_links_authorize(a->io, cluster, 2, 2) == VSR_OK);
    CHECK(vsr_io_links_authorize(b->io, cluster, 1, 1) == VSR_OK);
}

/* -------------------------------------------------------------------------
 * The store: start, transactions, completions
 * ---------------------------------------------------------------------- */

static uint64_t next_op(struct engine *e)
{
    return e->next_op++;
}

static bool store_completion(struct engine *e, struct vsr_io_completion *out)
{
    return vsr_io_store_next_completion(e->store, out);
}

static void expect_store(struct engine *e, uint64_t op, int32_t status)
{
    struct vsr_io_completion completion;

    CHECK(store_completion(e, &completion));
    if (completion.op != op || completion.status != status) {
        fprintf(stderr,
                "store completion op %" PRIu64 " status %d, expected %" PRIu64
                " status %d (state %u error %d readable %" PRIu64
                " clients %u)\n",
                completion.op, completion.status, op, status, e->store->state,
                e->store->error, e->store->readable, e->store->clients_count);
    }
    CHECK(completion.op == op && completion.status == status);
}

static void expect_no_store(struct engine *e)
{
    struct vsr_io_completion completion;

    CHECK(!store_completion(e, &completion));
}

/* Opens the store with `mode` and drives it to READY; the RECOVERY load's
 * completion is left in the queue (NOT_FOUND for a new store). */
static uint64_t store_start(struct engine *e, uint32_t mode)
{
    struct vsr_store_read read;
    uint64_t op = next_op(e);

    memset(&read, 0, sizeof(read));
    read.type = VSR_LOAD_RECOVERY;
    vsr_io_store_open(e->store, mode, op, &read);
    settle();
    CHECK(e->store->state == VSR_IO_STORE_READY);
    return op;
}

struct txn {
    struct vsr_store store;
    struct vsr_change changes[3];
    struct vsr_entry entries[4];
    struct vsr_blob blobs[4];
    struct vsr_span spans[4];
    unsigned char bodies[4][256];
    struct vsr_client_record records[4];
    struct vsr_span result_spans[4];
    unsigned char results[4][64];
    struct vsr_hard_state hard;
    struct vsr_epoch epoch;
    struct vsr_membership membership;
    struct vsr_member members[3];
    struct vsr_checkpoint checkpoint;
    struct vsr_span manifest;
    unsigned char manifest_bytes[16];
    struct vsr_store_identity identity;
    size_t bytes;
};

static struct txn txns[ENGINES][TXN_MAX];

static uint64_t log_end_before(const struct engine *e, uint64_t sequence)
{
    uint64_t end = 1;

    for (uint64_t q = 1; q < sequence; ++q) {
        const struct txn *t = &txns[e->index][q];

        for (uint32_t i = 0; i < t->store.count; ++i) {
            const struct vsr_change *change = &t->store.changes[i];

            if (change->type == VSR_STORE_APPEND) {
                end = change->first + change->count;
            } else if (change->type == VSR_STORE_TRUNCATE) {
                end = change->first;
            }
        }
    }
    return end;
}

static struct txn *txn_begin(const struct engine *e, uint64_t sequence)
{
    struct txn *t;

    CHECK(sequence >= 1 && sequence < TXN_MAX);
    t = &txns[e->index][sequence];
    memset(t, 0, sizeof(*t));
    t->store.sequence = sequence;
    t->store.changes = t->changes;
    return t;
}

static void txn_add(struct txn *t, uint32_t type, uint64_t first,
                    uint32_t count, const void *data)
{
    struct vsr_change *change;

    CHECK(t->store.count < 3);
    change = &t->changes[t->store.count++];
    change->type = type;
    change->count = count;
    change->first = first;
    change->data = data;
}

static const struct txn *txn_finish(struct txn *t)
{
    CHECK(vsr_io_codec_record_bytes(&t->store, &limits, &t->bytes) == VSR_OK);
    return t;
}

static void txn_entries(struct txn *t, uint64_t first, uint32_t count,
                        size_t body)
{
    uint64_t sequence = t->store.sequence;

    CHECK(count >= 1 && count <= 4 && body <= 256);
    for (uint32_t i = 0; i < count; ++i) {
        struct vsr_entry *entry = &t->entries[i];

        for (size_t b = 0; b < body; ++b) {
            t->bodies[i][b] =
                (unsigned char)(sequence * 31u + (uint64_t)i * 7u + b);
        }
        t->spans[i].data = t->bodies[i];
        t->spans[i].size = body;
        t->blobs[i].spans = body > 0 ? &t->spans[i] : NULL;
        t->blobs[i].size = body;
        t->blobs[i].count = body > 0 ? 1 : 0;
        entry->op = first + i;
        entry->epoch = 0;
        entry->view = 1;
        entry->request.client.hi = 0x1000 + (i % 2);
        entry->request.client.lo = 1;
        entry->request.number = sequence;
        entry->type = VSR_REQUEST_COMMAND;
        entry->body = &t->blobs[i];
    }
    txn_add(t, VSR_STORE_APPEND, first, count, t->entries);
}

/* An APPEND at the log end of `sequence`. */
static const struct txn *txn_append(const struct engine *e, uint64_t sequence,
                                    uint32_t count, size_t body)
{
    struct txn *t = txn_begin(e, sequence);

    txn_entries(t, log_end_before(e, sequence), count, body);
    return txn_finish(t);
}

/* CLIENTS records: client ids[i] completed request numbers[i] at ops[i]
 * with a result of `result` bytes. */
static const struct txn *txn_clients(const struct engine *e, uint64_t sequence,
                                     uint32_t count, const struct vsr_id *ids,
                                     const uint64_t *numbers,
                                     const uint64_t *ops, size_t result)
{
    struct txn *t = txn_begin(e, sequence);

    CHECK(count >= 1 && count <= 4 && result <= 64);
    for (uint32_t i = 0; i < count; ++i) {
        struct vsr_client_record *record = &t->records[i];

        for (size_t b = 0; b < result; ++b) {
            t->results[i][b] =
                (unsigned char)(numbers[i] * 13u + b + ids[i].hi);
        }
        t->result_spans[i].data = t->results[i];
        t->result_spans[i].size = result;
        record->request.client = ids[i];
        record->request.number = numbers[i];
        record->op = ops[i];
        record->result.code = (int32_t)(i + 1);
        record->result.data.spans = result > 0 ? &t->result_spans[i] : NULL;
        record->result.data.size = result;
        record->result.data.count = result > 0 ? 1 : 0;
    }
    txn_add(t, VSR_STORE_CLIENTS, 0, count, t->records);
    return txn_finish(t);
}

static void txn_epoch(struct txn *t)
{
    t->members[0].id = 1;
    t->members[0].role = VSR_MEMBER_FULL;
    t->membership.epoch = 0;
    t->membership.members = t->members;
    t->membership.count = 1;
    t->membership.faults = 0;
    t->epoch.current = &t->membership;
    t->epoch.previous = NULL;
    t->epoch.boundary = 0;
    t->epoch.phase = VSR_EPOCH_STEADY;
}

static void txn_hard_state(struct txn *t, uint32_t role)
{
    txn_epoch(t);
    t->hard.view = 1;
    t->hard.last_normal_view = 1;
    t->hard.committed = 0;
    t->hard.epoch = &t->epoch;
    t->hard.state = VSR_HARD_NORMAL;
    t->hard.role = role;
    txn_add(t, VSR_STORE_HARD_STATE, 0, 1, &t->hard);
}

/* Transaction 1 of a new store: IDENTITY then HARD_STATE with `role`. */
static const struct txn *txn_identity(const struct engine *e, uint64_t sequence,
                                      uint32_t role)
{
    struct txn *t = txn_begin(e, sequence);

    t->identity.cluster.hi = 0x77;
    t->identity.cluster.lo = 0x99;
    t->identity.replica = e->index + 1;
    t->identity.durability = VSR_DURABLE;
    txn_add(t, VSR_STORE_IDENTITY, 0, 1, &t->identity);
    txn_hard_state(t, role);
    return txn_finish(t);
}

static void txn_checkpoint(struct txn *t, struct vsr_id id, uint64_t op)
{
    txn_epoch(t);
    for (size_t b = 0; b < sizeof(t->manifest_bytes); ++b) {
        t->manifest_bytes[b] = (unsigned char)(id.lo + b);
    }
    t->manifest.data = t->manifest_bytes;
    t->manifest.size = sizeof(t->manifest_bytes);
    t->checkpoint.id = id;
    t->checkpoint.op = op;
    t->checkpoint.view = 1;
    t->checkpoint.epoch = &t->epoch;
    t->checkpoint.manifest.spans = &t->manifest;
    t->checkpoint.manifest.size = sizeof(t->manifest_bytes);
    t->checkpoint.manifest.count = 1;
}

static const struct txn *txn_publish(const struct engine *e, uint64_t sequence,
                                     struct vsr_id id, uint64_t op)
{
    struct txn *t = txn_begin(e, sequence);

    txn_checkpoint(t, id, op);
    txn_add(t, VSR_STORE_PUBLISH_CHECKPOINT, 0, 1, &t->checkpoint);
    return txn_finish(t);
}

static const struct txn *txn_restore(const struct engine *e, uint64_t sequence,
                                     struct vsr_id id, uint64_t op,
                                     uint32_t role)
{
    struct txn *t = txn_begin(e, sequence);

    txn_checkpoint(t, id, op);
    txn_add(t, VSR_STORE_RESTORE_CHECKPOINT, 0, 1, &t->checkpoint);
    txn_hard_state(t, role);
    return txn_finish(t);
}

static uint64_t submit(struct engine *e, const struct txn *t)
{
    uint64_t op = next_op(e);

    CHECK(vsr_io_store_store(e->store, op, &t->store) == VSR_OK);
    return op;
}

static uint64_t submit_sync(struct engine *e, uint64_t sequence)
{
    uint64_t op = next_op(e);

    CHECK(vsr_io_store_sync(e->store, op, sequence) == VSR_OK);
    return op;
}

/* Submits a STORE, runs the world and expects its completion. */
static void store_run(struct engine *e, const struct txn *t)
{
    uint64_t op = submit(e, t);

    settle();
    expect_store(e, op, VSR_IO_OK);
}

static const struct vsr_io_client *client_of(const struct engine *e,
                                             struct vsr_id id)
{
    for (uint32_t i = 0; i < e->store->clients_capacity; ++i) {
        const struct vsr_io_client *entry = &e->store->clients[i];

        if (entry->id.hi == id.hi && entry->id.lo == id.lo) {
            return entry;
        }
    }
    return NULL;
}

static uint64_t load_client(struct engine *e, uint64_t sequence,
                            struct vsr_id client)
{
    struct vsr_store_read read;
    uint64_t op = next_op(e);

    memset(&read, 0, sizeof(read));
    read.type = VSR_LOAD_CLIENT;
    read.sequence = sequence;
    read.client = client;
    read.max_count = 1;
    read.max_bytes = limits.message_bytes;
    CHECK(vsr_io_store_load(e->store, op, &read) == VSR_OK);
    return op;
}

/* The next store completion must be `op` with `status`; an OK LOAD
 * carries its graph in a lease, returned. */
static const struct vsr_loaded *expect_loaded(struct engine *e, uint64_t op,
                                              int32_t status, uint32_t *lease)
{
    struct vsr_io_completion completion;

    CHECK(store_completion(e, &completion));
    if (completion.op != op || completion.status != status) {
        fprintf(stderr,
                "completion op %" PRIu64 " status %d, expected %" PRIu64
                " status %d\n",
                completion.op, completion.status, op, status);
    }
    CHECK(completion.op == op && completion.status == status);
    if (status != VSR_IO_OK) {
        CHECK(completion.lease == NONE && completion.data == NULL);
        return NULL;
    }
    CHECK(completion.lease < REGIONS && completion.data != NULL);
    *lease = completion.lease;
    return completion.data;
}

static void release_lease(struct engine *e, uint32_t lease)
{
    vsr_io_store_release(e->store, lease);
    vsr_io_lease_release(e->replica, lease);
}

/* A new store with identity (1) and two clients A and B completed at 2
 * with 8-byte results, then one append (3). */
static const struct vsr_id client_a = {0xA, 1};
static const struct vsr_id client_b = {0xB, 1};

static void store_populate(struct engine *e)
{
    struct vsr_id ids[2] = {client_a, client_b};
    uint64_t numbers[2] = {1, 1};
    uint64_t ops[2] = {1, 2};
    uint64_t op = store_start(e, VSR_START_NEW);

    expect_store(e, op, VSR_IO_NOT_FOUND);
    store_run(e, txn_identity(e, 1, VSR_MEMBER_FULL));
    store_run(e, txn_clients(e, 2, 2, ids, numbers, ops, 8));
    store_run(e, txn_append(e, 3, 1, 16));
}

/* -------------------------------------------------------------------------
 * The snapshot module as the engine drives it
 * ---------------------------------------------------------------------- */

/* A CAPTURE task with a template: op, view 1, a steady epoch, no id. */
struct task_holder {
    struct vsr_snapshot_task task;
    struct vsr_checkpoint checkpoint;
    struct vsr_epoch epoch;
    struct vsr_membership membership;
    struct vsr_member members[1];
    struct vsr_span manifest;
    unsigned char manifest_bytes[24];
};

static void task_init(struct task_holder *h, struct vsr_id id, uint64_t op,
                      uint64_t sequence, uint64_t peer)
{
    memset(h, 0, sizeof(*h));
    h->members[0].id = 1;
    h->members[0].role = VSR_MEMBER_FULL;
    h->membership.epoch = 0;
    h->membership.members = h->members;
    h->membership.count = 1;
    h->epoch.current = &h->membership;
    h->epoch.phase = VSR_EPOCH_STEADY;
    h->checkpoint.id = id;
    h->checkpoint.op = op;
    h->checkpoint.view = 1;
    h->checkpoint.epoch = &h->epoch;
    h->task.op = op;
    h->task.sequence = sequence;
    h->task.peer = peer;
    h->task.checkpoint = &h->checkpoint;
}

/* The caller's checkpoint result: the id with a manifest. */
static void task_result(struct task_holder *h, struct vsr_id id)
{
    for (size_t b = 0; b < sizeof(h->manifest_bytes); ++b) {
        h->manifest_bytes[b] = (unsigned char)(id.lo * 3 + b);
    }
    h->manifest.data = h->manifest_bytes;
    h->manifest.size = sizeof(h->manifest_bytes);
    h->checkpoint.id = id;
    h->checkpoint.manifest.spans = &h->manifest;
    h->checkpoint.manifest.size = sizeof(h->manifest_bytes);
    h->checkpoint.manifest.count = 1;
}

/* The next core completion the module queued, or false. */
static bool core_take(struct engine *e, struct vsr_event *out)
{
    struct vsr_io_replica *rep = e->replica;

    if (rep->completions_count == 0) {
        return false;
    }
    *out = rep->completions[rep->completions_head].event;
    rep->completions_head = (rep->completions_head + 1) % OPERATIONS;
    rep->completions_count--;
    return true;
}

/* The next core completion is `op` with `status`, carrying no data and
 * no lease (every completion but an OK CAPTURE or FETCH). */
static void expect_core(struct engine *e, uint64_t op, int32_t status)
{
    struct vsr_event event;

    CHECK(core_take(e, &event));
    if (event.id != op || event.status != status) {
        fprintf(stderr,
                "core completion op %" PRIu64 " status %d, expected %" PRIu64
                " status %d\n",
                event.id, event.status, op, status);
    }
    CHECK(event.type == VSR_EVENT_COMPLETE && event.id == op &&
          event.status == status);
    CHECK(event.data == NULL && event.lease == 0);
}

static void expect_no_core(struct engine *e)
{
    struct vsr_event event;

    CHECK(!core_take(e, &event));
}

/* The next `count` core completions are ops[] in any order, each once,
 * with `status`, no data and no lease. */
static void expect_cores(struct engine *e, const uint64_t *ops, uint32_t count,
                         int32_t status)
{
    bool seen[8] = {false};

    CHECK(count <= 8);
    for (uint32_t n = 0; n < count; ++n) {
        struct vsr_event event;
        bool found = false;

        CHECK(core_take(e, &event));
        CHECK(event.type == VSR_EVENT_COMPLETE && event.status == status &&
              event.data == NULL && event.lease == 0);
        for (uint32_t i = 0; i < count; ++i) {
            if (!seen[i] && ops[i] == event.id) {
                seen[i] = true;
                found = true;
                break;
            }
        }
        CHECK(found);
    }
}

/* The next forwarded CORE op, which must be of `type` for op `id`, or
 * NULL when none is queued. */
static const struct vsr_op *forwarded_core(struct engine *e, uint32_t type,
                                           uint64_t id)
{
    const struct vsr_io_forwarded *f;

    f = forwarded_take(e, VSR_IO_OP_CORE);
    if (f == NULL) {
        return NULL;
    }
    CHECK(f->op.replica == e->replica && f->op.op.type == type &&
          f->op.op.id == id);
    return &f->op.op;
}

static const struct vsr_io_snapshot *entry_of(const struct engine *e,
                                              struct vsr_id id)
{
    for (uint32_t i = 0; i < e->snapshots->count; ++i) {
        const struct vsr_io_snapshot *entry = &e->snapshots->entries[i];

        if (entry->state != VSR_IO_SNAPSHOT_FREE && entry->id.hi == id.hi &&
            entry->id.lo == id.lo) {
            return entry;
        }
    }
    return NULL;
}

static uint32_t entries_used(const struct engine *e)
{
    uint32_t n = 0;

    for (uint32_t i = 0; i < e->snapshots->count; ++i) {
        n += e->snapshots->entries[i].state != VSR_IO_SNAPSHOT_FREE ? 1 : 0;
    }
    return n;
}

static void clients_path(char *out, struct vsr_id id, bool tmp)
{
    char name[VSR_IO_CLIENTS_NAME_BYTES];

    vsr_io_codec_clients_name(id, name);
    sprintf(out, DIRECTORY "/%s%s", name, tmp ? ".tmp" : "");
}

static const struct dfile *clients_file(const struct engine *e,
                                        struct vsr_id id, bool tmp)
{
    char path[128];

    clients_path(path, id, tmp);
    return dfile_named(e, path);
}

/* Releases an engine lease a core completion carried, as the core's
 * RELEASE op would. */
static void core_release(struct engine *e, uint64_t lease)
{
    struct vsr_io_replica *replica = NULL;
    uint32_t index = NONE;

    CHECK(vsr_io_lease_resolve(e->io, lease, &replica, &index) == VSR_OK);
    CHECK(replica == e->replica);
    vsr_io_lease_release(replica, index);
}

/* The next core completion is `op` with `status`; an OK CAPTURE or FETCH
 * carries a checkpoint of `id` under an engine lease, checked against the
 * caller's result in `h` and released. */
static void expect_checkpoint(struct engine *e, uint64_t op, struct vsr_id id,
                              const struct task_holder *h)
{
    struct vsr_event event;
    const struct vsr_checkpoint *result;
    struct vsr_io_replica *replica = NULL;
    uint32_t index = NONE;

    CHECK(core_take(e, &event));
    if (event.id != op || event.status != VSR_IO_OK) {
        fprintf(stderr,
                "core completion op %" PRIu64 " status %d, expected %" PRIu64
                " OK\n",
                event.id, event.status, op);
    }
    CHECK(event.type == VSR_EVENT_COMPLETE && event.id == op &&
          event.status == VSR_IO_OK);
    result = event.data;
    CHECK(result != NULL && event.lease != 0);
    CHECK(vsr_io_lease_resolve(e->io, event.lease, &replica, &index) == VSR_OK);
    /* The whole graph lives in the lease's region (vsr.h). */
    {
        const unsigned char *base = replica->leases[index].region.base;
        size_t used = replica->leases[index].region.used;
        const unsigned char *at = (const unsigned char *)result;

        CHECK(at >= base && at + sizeof(*result) <= base + used);
        CHECK(result->epoch == NULL ||
              ((const unsigned char *)result->epoch >= base &&
               (const unsigned char *)result->epoch < base + used));
        CHECK(result->manifest.count == 0 ||
              ((const unsigned char *)result->manifest.spans[0].data >= base &&
               (const unsigned char *)result->manifest.spans[0].data <
                   base + used));
    }
    CHECK(result->id.hi == id.hi && result->id.lo == id.lo);
    CHECK(result->op == h->checkpoint.op && result->view == h->checkpoint.view);
    CHECK(result->epoch != NULL && result->epoch->current != NULL &&
          result->epoch->current->count == 1 &&
          result->epoch->current->members[0].id == 1);
    CHECK(result->manifest.size == sizeof(h->manifest_bytes) &&
          result->manifest.count == 1 &&
          memcmp(result->manifest.spans[0].data, h->manifest_bytes,
                 sizeof(h->manifest_bytes)) == 0);
    core_release(e, event.lease);
}

/* Runs a CAPTURE at the store's readable sequence to the point where the
 * op is forwarded; returns the generated id. */
static struct vsr_id capture_begin(struct engine *e, struct task_holder *h,
                                   uint64_t *op)
{
    struct vsr_id zero = {0, 0};
    const struct vsr_op *forwarded;
    const struct vsr_snapshot_task *task;

    task_init(h, zero, 5, e->store->readable, 0);
    *op = next_op(e);
    CHECK(vsr_io_snapshots_capture(e->io, REPLICA, *op, &h->task) == VSR_OK);
    settle();
    forwarded = forwarded_core(e, VSR_OP_SNAPSHOT_CAPTURE, *op);
    CHECK(forwarded != NULL);
    task = forwarded->data;
    CHECK(task->op == 5 && task->sequence == h->task.sequence);
    CHECK(task->checkpoint != NULL &&
          (task->checkpoint->id.hi != 0 || task->checkpoint->id.lo != 0));
    CHECK(task->checkpoint->op == 5 && task->checkpoint->epoch != NULL &&
          task->checkpoint->epoch->current->count == 1 &&
          task->checkpoint->manifest.size == 0);
    return task->checkpoint->id;
}

/* A whole CAPTURE: forwarded, the caller completes OK, the core hears OK
 * with the caller's checkpoint. */
static struct vsr_id capture(struct engine *e)
{
    struct task_holder h;
    uint64_t op;
    struct vsr_id id = capture_begin(e, &h, &op);

    task_result(&h, id);
    CHECK(vsr_io_snapshots_forwarded_done(e->io, REPLICA, op, VSR_IO_OK,
                                          &h.checkpoint) == VSR_OK);
    settle_retry();
    expect_checkpoint(e, op, id, &h);
    CHECK(entry_of(e, id) != NULL &&
          entry_of(e, id)->state == VSR_IO_SNAPSHOT_WRITTEN);
    CHECK(e->store->last_capture.hi == id.hi);
    return id;
}

/* Parses a clients file image: count records, each checked against the
 * store's client table (id, number, op). */
struct parsed_record {
    struct vsr_io_wire_client_record wire;
    const unsigned char *result;
    uint64_t offset;
};

static uint32_t file_parse(const unsigned char *bytes, uint64_t size,
                           struct vsr_id id, struct parsed_record *out,
                           uint32_t capacity)
{
    struct vsr_io_cursor cursor;
    struct vsr_io_wire_clients_header header;
    uint32_t count = 0;
    uint32_t trailer = 0;

    vsr_io_cursor_init_one(&cursor, bytes, (size_t)size);
    CHECK(vsr_io_codec_get_clients_header(&cursor, &header) == VSR_OK);
    CHECK(header.snapshot_hi == id.hi && header.snapshot_lo == id.lo);
    CHECK(header.cluster_hi == 0x77 && header.cluster_lo == 0x99);
    CHECK(header.count <= capacity);
    for (uint32_t i = 0; i < header.count; ++i) {
        const unsigned char *at = bytes + cursor.position;
        uint32_t length;
        uint64_t padded;

        out[i].offset = cursor.position;
        CHECK(vsr_io_codec_skip_client_record(&cursor, &out[i].wire) == VSR_OK);
        length = out[i].wire.length;
        padded = length + (8 - length % 8) % 8;
        CHECK(vsr_io_crc32c(0, at, 40 + (size_t)padded) ==
              ((uint32_t)at[40 + padded] | (uint32_t)at[41 + padded] << 8 |
               (uint32_t)at[42 + padded] << 16 |
               (uint32_t)at[43 + padded] << 24));
        out[i].result = at + 40;
        CHECK(vsr_io_cursor_skip(&cursor, 4));
        count++;
    }
    CHECK(vsr_io_codec_get_clients_trailer(&cursor, &trailer) == VSR_OK);
    CHECK(trailer == header.count && cursor.position == size);
    return count;
}

/* A store with `count` clients (0x40 + i), each completed with a result of
 * `result` bytes; transactions from `sequence` on, four clients each.
 * Returns the next sequence. */
static uint64_t store_clients(struct engine *e, uint64_t sequence,
                              uint32_t count, size_t result)
{
    for (uint32_t done = 0; done < count;) {
        struct vsr_id ids[4];
        uint64_t numbers[4];
        uint64_t ops[4];
        uint32_t n = count - done < 4 ? count - done : 4;

        for (uint32_t i = 0; i < n; ++i) {
            ids[i].hi = 0x40 + done + i;
            ids[i].lo = 1;
            numbers[i] = 3;
            ops[i] = 20 + done + i;
        }
        store_run(e, txn_clients(e, sequence, n, ids, numbers, ops, result));
        sequence++;
        done += n;
    }
    return sequence;
}

/* A FETCH of `id` from replica `peer`: the task names the checkpoint the
 * core wants (the id, op 5, view 1). Returns the module's answer. */
static int fetch_start(struct engine *e, struct task_holder *h,
                       struct vsr_id id, uint64_t peer, uint64_t *op)
{
    task_init(h, id, 5, 0, peer);
    *op = next_op(e);
    return vsr_io_snapshots_fetch(e->io, REPLICA, *op, &h->task);
}

/* A whole FETCH of `id` by e from replica `peer`, expected to succeed. */
static void fetch(struct engine *e, struct vsr_id id, uint64_t peer)
{
    struct task_holder h;
    uint64_t op;
    const struct vsr_op *forwarded;

    CHECK(fetch_start(e, &h, id, peer, &op) == VSR_OK);
    settle();
    forwarded = forwarded_core(e, VSR_OP_SNAPSHOT_FETCH, op);
    CHECK(forwarded != NULL && forwarded->data == &h.task);
    CHECK(clients_file(e, id, false) != NULL &&
          clients_file(e, id, true) == NULL);
    task_result(&h, id);
    CHECK(vsr_io_snapshots_forwarded_done(e->io, REPLICA, op, VSR_IO_OK,
                                          &h.checkpoint) == VSR_OK);
    settle();
    expect_checkpoint(e, op, id, &h);
    expect_no_core(e);
}

/* The bytes of two files are equal. */
static bool files_equal(const struct dfile *x, const struct dfile *y)
{
    return x != NULL && y != NULL && x->size == y->size &&
           memcmp(x->data, y->data, x->size) == 0;
}

/* A SYNC or DROP of `id` taken by the module (task in `h`, pinned until
 * the completion); returns the module's answer. */
static int joint_start(struct engine *e, uint32_t type, struct task_holder *h,
                       struct vsr_id id, uint64_t *op)
{
    task_init(h, id, 5, 0, 0);
    *op = next_op(e);
    if (type == VSR_OP_SNAPSHOT_SYNC) {
        return vsr_io_snapshots_sync(e->io, REPLICA, *op, &h->task);
    }
    return vsr_io_snapshots_drop(e->io, REPLICA, *op, &h->task);
}

/* The caller's half of a forwarded op. */
static void caller_done(struct engine *e, uint64_t op, int32_t status,
                        const void *data)
{
    CHECK(vsr_io_snapshots_forwarded_done(e->io, REPLICA, op, status, data) ==
          VSR_OK);
}

/* A whole SYNC or DROP: forwarded, the caller completes with `caller`, the
 * core hears `expected`. */
static void joint_run(struct engine *e, uint32_t type, struct vsr_id id,
                      int32_t caller, int32_t expected)
{
    struct task_holder h;
    uint64_t op;

    CHECK(joint_start(e, type, &h, id, &op) == VSR_OK);
    settle();
    CHECK(forwarded_core(e, type, op) != NULL);
    caller_done(e, op, caller, NULL);
    settle();
    expect_core(e, op, expected);
    expect_no_core(e);
}

/* Appends until no client record is hot in the ring any more. */
static uint64_t evict_clients(struct engine *e, uint64_t sequence)
{
    for (;;) {
        bool hot = false;

        for (uint32_t i = 0; i < e->store->clients_capacity; ++i) {
            const struct vsr_io_client *c = &e->store->clients[i];
            struct vsr_io_piece piece;

            if ((c->id.hi != 0 || c->id.lo != 0) && c->current.number != 0 &&
                c->current.sequence != 0 &&
                vsr_io_store_hot(e->store, c->current.offset, c->current.length,
                                 &piece)) {
                hot = true;
            }
        }
        if (!hot) {
            return sequence;
        }
        store_run(e, txn_append(e, sequence, 4, 200));
        sequence++;
        CHECK(sequence < TXN_MAX);
    }
}

/* A clients file built by the test: header, `count` records of clients
 * 0x60 + i (number 4, op 30 + i, `result` bytes each), trailer. */
struct craft {
    unsigned char bytes[4096];
    uint64_t size;
    uint64_t record_at[9]; /* Record offsets; [count] = the trailer's. */
};

static void craft_file(struct craft *c, struct vsr_id id, uint32_t count,
                       size_t result)
{
    struct vsr_io_wire_clients_header header;
    unsigned char data[256];

    CHECK(count <= 8 && result <= sizeof(data));
    memset(c, 0, sizeof(*c));
    memset(&header, 0, sizeof(header));
    header.cluster_hi = 0x77;
    header.cluster_lo = 0x99;
    header.snapshot_hi = id.hi;
    header.snapshot_lo = id.lo;
    header.op = 5;
    header.sequence = 1;
    header.count = count;
    vsr_io_codec_put_clients_header(&header, c->bytes);
    c->size = 64;
    for (uint32_t i = 0; i < count; ++i) {
        struct vsr_client_record record;
        struct vsr_span span;

        for (size_t b = 0; b < result; ++b) {
            data[b] = (unsigned char)(i * 17u + b);
        }
        span.data = data;
        span.size = result;
        memset(&record, 0, sizeof(record));
        record.request.client.hi = 0x60 + i;
        record.request.client.lo = 1;
        record.request.number = 4;
        record.op = 30 + i;
        record.result.code = (int32_t)i;
        record.result.data.spans = result > 0 ? &span : NULL;
        record.result.data.size = result;
        record.result.data.count = result > 0 ? 1 : 0;
        c->record_at[i] = c->size;
        CHECK(c->size + vsr_io_codec_clients_record_bytes(result) + 8 <=
              sizeof(c->bytes));
        vsr_io_codec_put_clients_record(&record, c->bytes + c->size);
        c->size += vsr_io_codec_clients_record_bytes(result);
    }
    c->record_at[count] = c->size;
    vsr_io_codec_put_clients_trailer(count, c->bytes + c->size);
    c->size += 8;
}

static void put_le32(unsigned char *at, uint32_t value)
{
    for (uint32_t i = 0; i < 4; ++i) {
        at[i] = (unsigned char)(value >> (8 * i));
    }
}

/* Recomputes the CRC of record `i` of a crafted file after an edit. */
static void craft_seal(struct craft *c, uint32_t i)
{
    uint64_t at = c->record_at[i];
    uint32_t length =
        (uint32_t)c->bytes[at + 36] | (uint32_t)c->bytes[at + 37] << 8 |
        (uint32_t)c->bytes[at + 38] << 16 | (uint32_t)c->bytes[at + 39] << 24;
    uint64_t padded = length + (8 - length % 8) % 8;

    CHECK(at + 40 + padded + 4 <= sizeof(c->bytes));
    put_le32(c->bytes + at + 40 + padded,
             vsr_io_crc32c(0, c->bytes + at, (size_t)(40 + padded)));
}

/* Writes `bytes` as clients-<id> (tmp: .tmp) into the engine's directory,
 * durable (synced, its name too). */
static struct dfile *file_install(struct engine *e, struct vsr_id id, bool tmp,
                                  const unsigned char *bytes, uint64_t size)
{
    char path[128];
    int i;
    struct dfile *f;

    clients_path(path, id, tmp);
    i = dfile_find(e, path);
    if (i < 0) {
        i = dfile_create(e, path);
    }
    f = &e->files[i];
    CHECK(size <= DFILE_BYTES);
    memset(f->data, 0, DFILE_BYTES);
    memcpy(f->data, bytes, (size_t)size);
    f->size = size;
    memcpy(f->flushed, f->data, DFILE_BYTES);
    f->flushed_size = size;
    strcpy(f->durable_name, f->name);
    return f;
}

/* A writable view of a file of the model. */
static struct dfile *dfile_mutable(struct engine *e, const struct dfile *f)
{
    CHECK(f != NULL && f >= e->files && f < e->files + DFILES);
    return &e->files[f - e->files];
}

/* The replica's retry deadline is armed. */
static bool capture_deadline_armed(const struct engine *e)
{
    return e->io->deadlines.entries[DEADLINE_CAPTURE].when != VSR_NO_DEADLINE;
}

/* A fresh engine 0 with a store holding an identity and `clients` clients
 * with 8-byte results; returns the next sequence. */
static uint64_t fresh_store(struct engine **out, uint64_t seed,
                            uint32_t clients)
{
    struct engine *a;
    uint64_t sequence = 2;

    world_reset(seed);
    a = engine_open(0);
    expect_store(a, store_start(a, VSR_START_NEW), VSR_IO_NOT_FOUND);
    store_run(a, txn_identity(a, 1, VSR_MEMBER_FULL));
    if (clients > 0) {
        sequence = store_clients(a, sequence, clients, 8);
    }
    *out = a;
    return sequence;
}

/* The module holds nothing but kept slots: no job, writer, reader, lease,
 * slab or file operation. */
static void expect_idle(const struct engine *e)
{
    const struct vsr_io_snapshots *s = e->snapshots;

    CHECK(s->writer.snapshot == NONE && s->reader.snapshot == NONE);
    CHECK(s->writer.slab == NONE && s->writer.cold_slab == NONE &&
          s->reader.slab == NONE);
    CHECK(s->chunks_count == 0 && s->capture == NONE);
    for (uint32_t i = 0; i < VSR_IO_SNAPSHOT_FILEOPS; ++i) {
        CHECK(!s->fileops[i].used);
    }
    for (uint32_t i = 0; i < s->count; ++i) {
        const struct vsr_io_snapshot *entry = &s->entries[i];

        CHECK(entry->job == VSR_IO_SNAPSHOT_JOB_NONE && entry->op == 0 &&
              entry->lease == NONE && entry->tmp_slot == NONE &&
              entry->readers == 0);
    }
    for (uint32_t i = 0; i < s->serves_count; ++i) {
        CHECK(s->serves[i].state == 0 && s->serves[i].slot == NONE);
    }
    CHECK(e->replica->leases_free == REGIONS);
}

/* Client i of store_clients. */
static struct vsr_id client_n(uint32_t i)
{
    struct vsr_id id = {0x40 + i, 1};

    return id;
}

/* One step of one engine with the invariants checked. */
static void step_checked(struct engine *e)
{
    engine_step(e);
    check_streams(e);
    check_snapshots(e);
    check_file_slots(e);
    check_records(e);
    check_leases(e);
}

/* A CAPTURE stepped by hand until its file is complete (the writer done,
 * nothing settled beyond); the forwarded op is taken and the id
 * returned. */
static struct vsr_id capture_stepped(struct engine *e, struct task_holder *h,
                                     uint64_t *op)
{
    struct vsr_id zero = {0, 0};
    const struct vsr_op *forwarded;
    uint32_t index;
    uint32_t steps = 0;

    task_init(h, zero, 5, e->store->readable, 0);
    *op = next_op(e);
    CHECK(vsr_io_snapshots_capture(e->io, REPLICA, *op, &h->task) == VSR_OK);
    index = e->snapshots->capture;
    while (e->snapshots->entries[index].state != VSR_IO_SNAPSHOT_WRITTEN) {
        step_checked(e);
        CHECK(++steps < 20);
    }
    forwarded = forwarded_core(e, VSR_OP_SNAPSHOT_CAPTURE, *op);
    CHECK(forwarded != NULL);
    return ((const struct vsr_snapshot_task *)forwarded->data)->checkpoint->id;
}

/* Two engines; a's store holds `clients` clients with 64-byte results
 * and a capture of them (returned); b's store is new with an identity. */
static struct vsr_id fetch_setup(uint64_t seed, uint32_t clients)
{
    struct engine *a;
    struct engine *b;

    world_reset(seed);
    two_engines();
    a = &world.engines[0];
    b = &world.engines[1];
    expect_store(a, store_start(a, VSR_START_NEW), VSR_IO_NOT_FOUND);
    store_run(a, txn_identity(a, 1, VSR_MEMBER_FULL));
    if (clients > 0) {
        store_clients(a, 2, clients, 64);
    }
    expect_store(b, store_start(b, VSR_START_NEW), VSR_IO_NOT_FOUND);
    store_run(b, txn_identity(b, 1, VSR_MEMBER_FULL));
    return capture(a);
}

/* A writable view of a registry entry (a test reaching into the module). */
static struct vsr_io_snapshot *entry_mutable(const struct engine *e,
                                             struct vsr_id id)
{
    const struct vsr_io_snapshot *entry = entry_of(e, id);

    CHECK(entry != NULL);
    return mutable_of(entry);
}

/* A FETCH of `id` from replica 1 that fails with `status` before the
 * caller sees it: no file, no temporary file, no entry, nothing held on
 * either side. */
static void fetch_fails(struct engine *b, struct vsr_id id, int32_t status)
{
    struct task_holder h;
    uint64_t op;

    CHECK(fetch_start(b, &h, id, 1, &op) == VSR_OK);
    settle();
    CHECK(forwarded_count(b, VSR_IO_OP_CORE) == 0);
    expect_core(b, op, status);
    expect_no_core(b);
    CHECK(clients_file(b, id, false) == NULL &&
          clients_file(b, id, true) == NULL && entry_of(b, id) == NULL);
    expect_idle(b);
    if (world.engines[0].snapshots->capture == NONE) {
        expect_idle(&world.engines[0]);
    }
    for (uint32_t i = 0; i < STREAMS; ++i) {
        CHECK(world.engines[0].snapshots->serves[i].state == 0);
    }
    CHECK(b->io->streams.active == 0 &&
          world.engines[0].io->streams.active == 0);
}

/* The link of b's fetch stream. */
static const struct vsr_io_link *fetch_link(const struct engine *b)
{
    uint32_t stream = b->snapshots->reader.stream;
    uint32_t link;

    CHECK(stream != NONE);
    link = b->io->streams.streams[stream].link;
    CHECK(link < LINKS);
    return &b->io->links.links[link];
}

/* -------------------------------------------------------------------------
 * Tests
 * ---------------------------------------------------------------------- */

/* Sizes, an empty table, one and max_clients records, hot and cold
 * records, the file's bytes and the store's offsets. */
static void test_capture(void)
{
    struct engine *a;
    struct vsr_id id;
    const struct dfile *file;
    struct parsed_record records[8];
    struct task_holder h;
    uint64_t op;
    size_t bytes = 0;
    size_t alignment = 0;
    struct vsr_limits big = limits;
    struct vsr_io_limits io_limits;
    uint32_t n;

    /* Sizing: overflow is ELIMIT, a zero window EINVAL. */
    world_reset(1);
    a = engine_open(0);
    io_limits = a->io->options.limits;
    CHECK(vsr_io_snapshots_size(&limits, &io_limits, 8, &bytes, &alignment) ==
          VSR_OK);
    CHECK(bytes > 8 * sizeof(struct vsr_io_client_snapshot) &&
          bytes >
              VSR_IO_SNAPSHOT_FILEOPS * sizeof(struct vsr_io_snapshot_fileop));
    CHECK(alignment == 16);
    big.transfers = UINT32_MAX - 1;
    CHECK(vsr_io_snapshots_size(&big, &io_limits, 8, &bytes, &alignment) ==
          VSR_ELIMIT);
    big = limits;
    big.manifest_bytes = UINT64_MAX;
    CHECK(vsr_io_snapshots_size(&big, &io_limits, 8, &bytes, &alignment) ==
          VSR_ELIMIT);
    io_limits.stream_window = 0;
    CHECK(vsr_io_snapshots_size(&limits, &io_limits, 8, &bytes, &alignment) ==
          VSR_EINVAL);
    CHECK(a->snapshots->count == limits.transfers + VSR_IO_SNAPSHOT_EXTRA);
    /* An empty table: header and trailer only. */
    expect_store(a, store_start(a, VSR_START_NEW), VSR_IO_NOT_FOUND);
    store_run(a, txn_identity(a, 1, VSR_MEMBER_FULL));
    CHECK(a->dir_opens == 1); /* The directory, once. */
    id = capture(a);
    file = clients_file(a, id, false);
    CHECK(file != NULL && file->size == 72 && file->refs == 1);
    CHECK(file_parse(file->data, file->size, id, records, 8) == 0);
    CHECK(clients_file(a, id, true) == NULL);
    CHECK(entry_of(a, id)->file_slot >= 0 && entry_of(a, id)->bytes == 72);
    CHECK(entry_of(a, id)->sequence == 1);
    CHECK(a->snapshots->writer.snapshot == NONE && pool_refs(a) == 0);
    CHECK(a->store->capture_floor == UINT64_MAX);
    /* Two hot records: the ring's bytes, offsets recorded per client. */
    {
        struct vsr_id ids[2] = {client_a, client_b};
        uint64_t numbers[2] = {1, 1};
        uint64_t ops[2] = {1, 2};
        struct vsr_id second;
        const struct vsr_io_snapshot *entry;

        store_run(a, txn_clients(a, 2, 2, ids, numbers, ops, 8));
        store_run(a, txn_append(a, 3, 1, 16));
        second = capture(a);
        file = clients_file(a, second, false);
        CHECK(file != NULL);
        n = file_parse(file->data, file->size, second, records, 8);
        CHECK(n == 2 && file->size == 64 + 2 * (40 + 8 + 4) + 8);
        for (uint32_t i = 0; i < n; ++i) {
            struct vsr_id c = {records[i].wire.client_hi,
                               records[i].wire.client_lo};
            const struct txn *t = &txns[0][2];
            uint32_t r = c.hi == 0xA ? 0 : 1;

            CHECK(records[i].wire.number == 1 &&
                  records[i].wire.op == (c.hi == 0xA ? 1 : 2));
            CHECK(records[i].wire.length == 8 &&
                  memcmp(records[i].result, t->results[r], 8) == 0);
            CHECK(client_of(a, c)->capture_offset == records[i].offset);
        }
        entry = entry_of(a, second);
        CHECK(entry->sequence == 3 && entry->bytes == file->size);
        /* The first capture is no longer the latest: its slot closed. */
        settle();
        CHECK(entry_of(a, id)->file_slot < 0 &&
              clients_file(a, id, false)->refs == 0);
        CHECK(a->store->last_capture.hi == second.hi);
        /* A PUBLISH of the latest capture makes it the base at once and
         * the store reads the file through the module's slot. */
        store_run(a, txn_publish(a, 4, second, 3));
        CHECK(a->store->client_base_id.hi == second.hi);
        settle();
        CHECK(a->store->base_slot == entry->file_slot);
        {
            uint64_t sequence = 5;
            struct vsr_io_piece piece;
            const struct vsr_loaded *loaded;
            const struct vsr_client_record *record;
            uint32_t lease = NONE;
            uint32_t reads;

            while (vsr_io_store_hot(
                a->store, client_of(a, client_b)->current.offset,
                client_of(a, client_b)->current.length, &piece)) {
                store_run(a, txn_append(a, sequence, 4, 200));
                sequence++;
                CHECK(sequence < 60);
            }
            reads = file->reads;
            op = load_client(a, sequence - 1, client_b);
            settle();
            loaded = expect_loaded(a, op, VSR_IO_OK, &lease);
            record = loaded->items;
            CHECK(file->reads == reads + 1);
            CHECK(record->request.client.hi == 0xB &&
                  record->request.number == 1 && record->op == 2 &&
                  record->result.data.size == 8);
            release_lease(a, lease);
            /* A capture now: b's record is cold in the log, a's too; the
             * writer reads them with block-aligned reads of the log. */
            reads = log_file(a)->reads;
            {
                struct vsr_id third = capture(a);

                file = clients_file(a, third, false);
                CHECK(file != NULL);
                n = file_parse(file->data, file->size, third, records, 8);
                CHECK(n == 2);
                CHECK(log_file(a)->reads >= reads + 1);
                for (uint32_t i = 0; i < n; ++i) {
                    const struct txn *t = &txns[0][2];
                    uint32_t r = records[i].wire.client_hi == 0xA ? 0 : 1;

                    CHECK(memcmp(records[i].result, t->results[r], 8) == 0);
                }
                /* The base (second) stays open; third is the latest. */
                settle();
                CHECK(entry_of(a, second)->file_slot >= 0 &&
                      entry_of(a, third)->file_slot >= 0);
                CHECK(a->store->base_slot == entry_of(a, second)->file_slot);
            }
        }
    }
    /* max_clients records: one per entry, several chunks of the staging
     * slab are not needed (the file is small) but the count is full. */
    {
        struct vsr_id ids[4];
        uint64_t numbers[4] = {2, 2, 2, 2};
        uint64_t ops[4] = {10, 11, 12, 13};
        struct vsr_id full;
        uint64_t sequence = a->store->readable + 1;

        for (uint32_t i = 0; i < 4; ++i) {
            ids[i].hi = 0x20 + i;
            ids[i].lo = 1;
        }
        store_run(a, txn_clients(a, sequence, 4, ids, numbers, ops, 64));
        sequence++;
        /* The appends' clients have entries already: complete them. */
        ids[0].hi = 0x1000;
        ids[1].hi = 0x1001;
        store_run(a, txn_clients(a, sequence, 2, ids, numbers, ops, 0));
        sequence++;
        CHECK(a->store->clients_count == 8);
        full = capture(a);
        file = clients_file(a, full, false);
        CHECK(file != NULL);
        n = file_parse(file->data, file->size, full, records, 8);
        CHECK(n == 8);
        CHECK(entries_used(a) == 4);
    }
    /* A task that contradicts the store: FAILED at once; a second CAPTURE
     * while one runs: RETRY. */
    {
        struct vsr_id zero = {0, 0};
        uint64_t second_op;

        task_init(&h, zero, 5, a->store->clients_sequence - 1, 0);
        CHECK(vsr_io_snapshots_capture(a->io, REPLICA, next_op(a), &h.task) ==
              VSR_IO_FAILED);
        h.task.checkpoint = NULL;
        h.task.sequence = a->store->readable;
        CHECK(vsr_io_snapshots_capture(a->io, REPLICA, next_op(a), &h.task) ==
              VSR_IO_FAILED);
        a->hold_pool_writes = true;
        id = capture_begin(a, &h, &op);
        task_init(&h, zero, 5, a->store->readable, 0);
        second_op = next_op(a);
        CHECK(vsr_io_snapshots_capture(a->io, REPLICA, second_op, &h.task) ==
              VSR_IO_RETRY);
        a->hold_pool_writes = false;
        world_release_held(a);
        task_result(&h, id);
        CHECK(vsr_io_snapshots_forwarded_done(a->io, REPLICA, op, VSR_IO_OK,
                                              &h.checkpoint) == VSR_OK);
        settle();
        expect_checkpoint(a, op, id, &h);
        CHECK(vsr_io_snapshots_forwarded_done(a->io, REPLICA, op, VSR_IO_OK,
                                              &h.checkpoint) == VSR_EINVAL);
    }
    CHECK(pool_refs(a) == 0);
    CHECK(vsr_io_snapshots_close(a->io, REPLICA) == VSR_OK);
    CHECK(a->store->base_slot == -1);
    for (uint32_t i = 0; i < DFILES; ++i) {
        /* Every clients file closed; the log is the store's. */
        CHECK(a->files[i].refs == 0 ||
              strcmp(a->files[i].name, DIRECTORY "/log") == 0);
    }
    CHECK(vsr_io_snapshots_close(a->io, REPLICA) == VSR_OK);
    engine_crash(a);
}

/* A library stream carries a file of several chunks, in order: the
 * requester writes each chunk to clients-<id>.tmp at its offset, verifies
 * the records as they arrive, renames the file at the verified END and
 * only then forwards the FETCH; the source closes its slot at the end. */
static void test_fetch(void)
{
    struct engine *a;
    struct engine *b;
    struct vsr_id id;
    const struct dfile *source;
    const struct dfile *copy;
    uint32_t refs_a;
    uint32_t refs_b;

    world_reset(2);
    two_engines();
    a = &world.engines[0];
    b = &world.engines[1];
    expect_store(a, store_start(a, VSR_START_NEW), VSR_IO_NOT_FOUND);
    store_run(a, txn_identity(a, 1, VSR_MEMBER_FULL));
    store_clients(a, 2, 8, 64);
    refs_a = pool_refs(a); /* The peer links' send slabs. */
    refs_b = pool_refs(b);
    id = capture(a);
    source = clients_file(a, id, false);
    CHECK(source != NULL && source->size == 64 + 8 * (40 + 64 + 4) + 8);
    CHECK(source->size > 3 * CHUNK);
    fetch(b, id, 1);
    copy = clients_file(b, id, false);
    CHECK(files_equal(source, copy));
    CHECK(copy->refs == 0 && clients_file(b, id, true) == NULL);
    CHECK(entry_of(b, id) != NULL &&
          entry_of(b, id)->state == VSR_IO_SNAPSHOT_WRITTEN &&
          entry_of(b, id)->sequence == 0 &&
          entry_of(b, id)->bytes == source->size);
    settle();
    CHECK(entry_of(a, id)->readers == 0 && source->refs == 1);
    CHECK(pool_refs(a) == refs_a && pool_refs(b) == refs_b);
    CHECK(a->io->streams.active == 0 && b->io->streams.active == 0);
    CHECK(b->replica->leases_free == REGIONS &&
          a->replica->leases_free == REGIONS);
}

/* A CAPTURE whose caller half fails, or whose library half fails at each
 * step: the file never outlives the op, the core hears the caller's
 * status (else FAILED, CORRUPT for bad record bytes) once both halves are
 * over, and the capture floor, lease, slabs and slots are all released. */
static void test_capture_failures(void)
{
    struct engine *a;
    struct task_holder h;
    struct vsr_id id;
    struct vsr_id before;
    uint64_t op;
    uint64_t sequence;

    /* The caller fails while the header write is held: the writer stops,
     * the file is closed and unlinked, then the core hears RETRY. */
    sequence = fresh_store(&a, 3, 2);
    (void)sequence;
    before = a->store->last_capture;
    a->hold_pool_writes = true;
    id = capture_begin(a, &h, &op);
    CHECK(held_count(a) == 1 && clients_file(a, id, false) != NULL);
    CHECK(a->store->capture_floor != UINT64_MAX);
    caller_done(a, op, VSR_IO_RETRY, NULL);
    settle();
    expect_no_core(a); /* The write is still out. */
    a->hold_pool_writes = false;
    world_release_held(a);
    settle();
    expect_core(a, op, VSR_IO_RETRY);
    CHECK(clients_file(a, id, false) == NULL && entry_of(a, id) == NULL);
    CHECK(a->store->capture_floor == UINT64_MAX);
    CHECK(a->store->last_capture.hi == before.hi &&
          a->store->last_capture.lo == before.lo);
    expect_idle(a);
    CHECK(pool_refs(a) == 0);
    /* The caller fails after the file is complete: unlinked, then its
     * status reaches the core. */
    id = capture_begin(a, &h, &op);
    CHECK(clients_file(a, id, false) != NULL &&
          entry_of(a, id)->state == VSR_IO_SNAPSHOT_WRITTEN);
    caller_done(a, op, VSR_IO_FAILED, NULL);
    settle();
    expect_core(a, op, VSR_IO_FAILED);
    CHECK(clients_file(a, id, false) == NULL && entry_of(a, id) == NULL);
    expect_idle(a);
    /* The caller answers OK with another id, or with no checkpoint:
     * FAILED, and the file goes. */
    id = capture_begin(a, &h, &op);
    {
        struct vsr_id other = {id.hi ^ 1, id.lo};

        task_result(&h, other);
    }
    caller_done(a, op, VSR_IO_OK, &h.checkpoint);
    settle();
    expect_core(a, op, VSR_IO_FAILED);
    CHECK(clients_file(a, id, false) == NULL);
    id = capture_begin(a, &h, &op);
    caller_done(a, op, VSR_IO_OK, NULL);
    settle();
    expect_core(a, op, VSR_IO_FAILED);
    CHECK(clients_file(a, id, false) == NULL);
    expect_idle(a);
    /* Library failures: the open, a write, a short write. The caller
     * completes OK; the core hears FAILED; nothing stays on disk, and the
     * store's latest capture is not moved (a file completed but discarded
     * above did move it: harmless, since the core never publishes an id
     * whose CAPTURE failed). */
    before = a->store->last_capture;
    for (uint32_t fault = 0; fault < 3; ++fault) {
        if (fault == 0) {
            a->fail_open = -EIO;
        } else if (fault == 1) {
            a->fail_write = -ENOSPC;
        } else {
            a->short_write = true;
        }
        id = capture_begin(a, &h, &op);
        task_result(&h, id);
        caller_done(a, op, VSR_IO_OK, &h.checkpoint);
        settle();
        expect_core(a, op, VSR_IO_FAILED);
        CHECK(clients_file(a, id, false) == NULL && entry_of(a, id) == NULL);
        CHECK(a->store->capture_floor == UINT64_MAX);
        expect_idle(a);
        CHECK(pool_refs(a) == 0);
    }
    CHECK(a->snapshots->error != 0);
    CHECK(a->store->last_capture.hi == before.hi &&
          a->store->last_capture.lo == before.lo);
    engine_crash(a);

    /* Cold records: a read error is FAILED, a short read or bytes that
     * fail their CRC are CORRUPT. */
    for (uint32_t fault = 0; fault < 3; ++fault) {
        sequence = fresh_store(&a, 4 + fault, 2);
        sequence = evict_clients(a, sequence);
        (void)sequence;
        if (fault == 0) {
            a->fail_log_read = -EIO;
        } else if (fault == 1) {
            a->short_log_read = true;
        } else {
            const struct vsr_io_client *c = client_of(a, client_n(0));
            struct dfile *log = dfile_mutable(a, log_file(a));

            log->data[c->current.offset + 60] ^= 0x10;
        }
        id = capture_begin(a, &h, &op);
        task_result(&h, id);
        caller_done(a, op, VSR_IO_OK, &h.checkpoint);
        settle();
        expect_core(a, op, fault == 0 ? VSR_IO_FAILED : VSR_IO_CORRUPT);
        CHECK(clients_file(a, id, false) == NULL && entry_of(a, id) == NULL);
        CHECK(a->store->capture_floor == UINT64_MAX);
        expect_idle(a);
        CHECK(pool_refs(a) == 0);
        engine_crash(a);
    }

    /* RETRY at once when a lease, a slab, a file slot or a registry entry
     * is missing, or while another CAPTURE runs; nothing is taken. */
    sequence = fresh_store(&a, 8, 2);
    (void)sequence;
    {
        struct vsr_id zero = {0, 0};
        uint32_t taken[SLABS];
        uint32_t count = 0;
        uint32_t leases[REGIONS];
        uint32_t slots[FILE_SLOTS];
        uint32_t lease;
        uint32_t slot;

        task_init(&h, zero, 5, a->store->readable, 0);
        while ((lease = vsr_io_lease_alloc(a->replica, NONE, NONE)) != NONE) {
            leases[count++] = lease;
        }
        CHECK(vsr_io_snapshots_capture(a->io, REPLICA, next_op(a), &h.task) ==
              VSR_IO_RETRY);
        while (count > 0) {
            vsr_io_lease_release(a->replica, leases[--count]);
        }
        while ((slot = vsr_io_pool_acquire(&a->io->pool, false)) != NONE) {
            taken[count++] = slot;
        }
        CHECK(vsr_io_snapshots_capture(a->io, REPLICA, next_op(a), &h.task) ==
              VSR_IO_RETRY);
        while (count > 0) {
            vsr_io_pool_release(&a->io->pool, taken[--count]);
        }
        while ((slot = vsr_io_engine_slot_alloc(a->io)) != NONE) {
            slots[count++] = slot;
        }
        CHECK(vsr_io_snapshots_capture(a->io, REPLICA, next_op(a), &h.task) ==
              VSR_IO_RETRY);
        while (count > 0) {
            vsr_io_engine_slot_free(a->io, slots[--count]);
        }
        CHECK(a->replica->leases_free == REGIONS);
        expect_idle(a);
        /* The registry full: count entries taken by captures. */
        for (uint32_t i = 0; i < a->snapshots->count; ++i) {
            (void)capture(a);
        }
        CHECK(entries_used(a) == a->snapshots->count);
        CHECK(vsr_io_snapshots_capture(a->io, REPLICA, next_op(a), &h.task) ==
              VSR_IO_RETRY);
        expect_no_core(a);
        /* A slab missing when a cold record needs one: the step waits
         * with the retry deadline armed, then proceeds. */
        joint_run(a, VSR_OP_SNAPSHOT_DROP, a->store->last_capture, VSR_IO_OK,
                  VSR_IO_OK);
    }
    engine_crash(a);
    sequence = fresh_store(&a, 9, 2);
    sequence = evict_clients(a, sequence);
    (void)sequence;
    {
        struct vsr_id zero = {0, 0};
        uint32_t taken[SLABS];
        uint32_t count = 0;
        uint32_t slab;

        task_init(&h, zero, 5, a->store->readable, 0);
        op = next_op(a);
        CHECK(vsr_io_snapshots_capture(a->io, REPLICA, op, &h.task) == VSR_OK);
        while ((slab = vsr_io_pool_acquire(&a->io->pool, false)) != NONE) {
            taken[count++] = slab;
        }
        for (uint32_t steps = 0; !a->snapshots->retry; ++steps) {
            step_checked(a); /* The open first, then the staging. */
            CHECK(steps < 10);
        }
        CHECK(capture_deadline_armed(a));
        CHECK(a->snapshots->writer.cold_slab == NONE);
        while (count > 0) {
            vsr_io_pool_release(&a->io->pool, taken[--count]);
        }
        settle_retry();
        id = entry_of(a, a->snapshots->entries[a->snapshots->capture].id)->id;
        CHECK(forwarded_core(a, VSR_OP_SNAPSHOT_CAPTURE, op) != NULL);
        task_result(&h, id);
        caller_done(a, op, VSR_IO_OK, &h.checkpoint);
        settle();
        expect_checkpoint(a, op, id, &h);
        {
            struct parsed_record records[8];
            const struct dfile *file = clients_file(a, id, false);

            CHECK(file != NULL &&
                  file_parse(file->data, file->size, id, records, 8) == 2);
        }
    }
    expect_idle(a);
    engine_crash(a);
}

/* SYNC: the file's fdatasync then the directory's fsync, forwarded to the
 * caller at once; the core hears OK only when both halves are durable, and
 * the directory model keeps the file across a crash. A file without a
 * kept slot is opened, synced and closed. Failures of either half are
 * reported, never retried. */
static void test_sync(void)
{
    struct engine *a;
    struct task_holder h;
    struct vsr_id x;
    struct vsr_id y;
    uint64_t op;
    const struct dfile *file;
    uint32_t dir_fsyncs;
    uint64_t sequence = fresh_store(&a, 10, 2);

    (void)sequence;
    x = capture(a);
    file = clients_file(a, x, false);
    CHECK(file != NULL && file->fsyncs == 0 && file->durable_name[0] == 0);
    dir_fsyncs = a->dir_fsyncs;
    /* The latest capture keeps its slot: fdatasync on it, no open. */
    {
        uint32_t opens_before = a->dir_opens;

        joint_run(a, VSR_OP_SNAPSHOT_SYNC, x, VSR_IO_OK, VSR_IO_OK);
        CHECK(a->dir_opens == opens_before);
    }
    CHECK(file->fsyncs == 1 && a->dir_fsyncs == dir_fsyncs + 1);
    CHECK(strcmp(file->durable_name, file->name) == 0 &&
          file->flushed_size == file->size);
    CHECK(entry_of(a, x)->state == VSR_IO_SNAPSHOT_DURABLE);
    CHECK(entry_of(a, x)->file_slot >= 0);
    expect_idle(a);
    /* A second capture: x's slot is released; its SYNC opens the file
     * read-only, syncs it and closes it again. */
    y = capture(a);
    settle();
    CHECK(entry_of(a, x)->file_slot < 0 && file->refs == 0);
    joint_run(a, VSR_OP_SNAPSHOT_SYNC, x, VSR_IO_OK, VSR_IO_OK);
    CHECK(file->fsyncs == 2 && file->refs == 0 &&
          entry_of(a, x)->file_slot < 0);
    /* The half that fails decides: the caller's status, the fdatasync's
     * (kept slot and transient one), the directory's. */
    joint_run(a, VSR_OP_SNAPSHOT_SYNC, y, VSR_IO_FAILED, VSR_IO_FAILED);
    CHECK(entry_of(a, y)->state == VSR_IO_SNAPSHOT_WRITTEN);
    a->fail_fsync = -EIO;
    joint_run(a, VSR_OP_SNAPSHOT_SYNC, y, VSR_IO_OK, VSR_IO_FAILED);
    a->fail_fsync = -EIO;
    joint_run(a, VSR_OP_SNAPSHOT_SYNC, x, VSR_IO_OK, VSR_IO_FAILED);
    CHECK(file->refs == 0 && entry_of(a, x)->state == VSR_IO_SNAPSHOT_WRITTEN);
    a->fail_fsync_dir = -EIO;
    joint_run(a, VSR_OP_SNAPSHOT_SYNC, y, VSR_IO_OK, VSR_IO_FAILED);
    a->fail_open = -EMFILE;
    joint_run(a, VSR_OP_SNAPSHOT_SYNC, x, VSR_IO_OK, VSR_IO_FAILED);
    joint_run(a, VSR_OP_SNAPSHOT_SYNC, y, VSR_IO_OK, VSR_IO_OK);
    CHECK(entry_of(a, y)->state == VSR_IO_SNAPSHOT_DURABLE);
    expect_idle(a);
    /* Refusals: an unknown id is FAILED; a second op on an entry RETRY. */
    {
        struct vsr_id unknown = {0x1234, 5};
        struct task_holder h2;
        uint64_t op2;

        CHECK(joint_start(a, VSR_OP_SNAPSHOT_SYNC, &h, unknown, &op) ==
              VSR_IO_FAILED);
        a->hold_fsync = true;
        CHECK(joint_start(a, VSR_OP_SNAPSHOT_SYNC, &h, x, &op) == VSR_OK);
        CHECK(joint_start(a, VSR_OP_SNAPSHOT_SYNC, &h2, x, &op2) ==
              VSR_IO_RETRY);
        CHECK(joint_start(a, VSR_OP_SNAPSHOT_DROP, &h2, x, &op2) ==
              VSR_IO_RETRY);
        settle();
        CHECK(forwarded_core(a, VSR_OP_SNAPSHOT_SYNC, op) != NULL);
        caller_done(a, op, VSR_IO_OK, NULL);
        settle();
        expect_no_core(a); /* The fdatasync is held. */
        a->hold_fsync = false;
        world_release_held(a);
        settle();
        expect_core(a, op, VSR_IO_OK);
    }
    /* A SYNC arriving while base_track releases the previous latest
     * capture's kept slot: before the CLOSE is issued it cancels the
     * release and syncs through the kept slot; with the CLOSE in flight it
     * follows it (a RETRY would fence the replica). */
    for (uint32_t variant = 0; variant < 2; ++variant) {
        struct vsr_id prev = a->store->last_capture;
        const struct vsr_io_snapshot *entry = entry_of(a, prev);
        struct task_holder hc;
        uint64_t capture_op;
        struct vsr_id next;
        uint32_t steps = 0;

        CHECK(entry != NULL && entry->file_slot >= 0 &&
              entry->job == VSR_IO_SNAPSHOT_JOB_NONE);
        next = capture_stepped(a, &hc, &capture_op);
        CHECK(a->store->last_capture.hi == next.hi &&
              a->store->last_capture.lo == next.lo);
        CHECK(entry->job == VSR_IO_SNAPSHOT_JOB_NONE && entry->file_slot >= 0);
        if (variant == 0) {
            vsr_io_snapshots_poll(a->io, REPLICA, world.now);
            CHECK(entry->job == VSR_IO_SNAPSHOT_JOB_RELEASE &&
                  entry->fileop == NONE);
            CHECK(joint_start(a, VSR_OP_SNAPSHOT_SYNC, &h, prev, &op) ==
                  VSR_OK);
            CHECK(entry->job == VSR_IO_SNAPSHOT_JOB_SYNC &&
                  entry->file_slot >= 0);
        } else {
            while (entry->fileop == NONE) {
                step_checked(a);
                CHECK(++steps < 10);
            }
            CHECK(entry->job == VSR_IO_SNAPSHOT_JOB_RELEASE);
            CHECK(joint_start(a, VSR_OP_SNAPSHOT_SYNC, &h, prev, &op) ==
                  VSR_OK);
            CHECK(entry->job == VSR_IO_SNAPSHOT_JOB_RELEASE &&
                  entry->job_next == VSR_IO_SNAPSHOT_JOB_SYNC);
        }
        settle();
        CHECK(forwarded_core(a, VSR_OP_SNAPSHOT_SYNC, op) != NULL);
        caller_done(a, op, VSR_IO_OK, NULL);
        settle();
        expect_core(a, op, VSR_IO_OK);
        CHECK(entry->state == VSR_IO_SNAPSHOT_DURABLE && entry->file_slot < 0);
        CHECK(clients_file(a, prev, false)->refs == 0);
        task_result(&hc, next);
        caller_done(a, capture_op, VSR_IO_OK, &hc.checkpoint);
        settle();
        expect_checkpoint(a, capture_op, next, &hc);
    }
    expect_idle(a);
    /* A crash keeps exactly the files whose SYNC completed. */
    engine_crash(a);
    CHECK(clients_file(a, x, false) != NULL &&
          clients_file(a, y, false) != NULL);
}

/* DROP: forwarded first; on the caller's OK the kept slot is closed and
 * the file unlinked, then the core hears the unlink's outcome. A served
 * stream still reading the file delays the unlink, not the completion. A
 * DROP of an id the registry lacks unlinks a stray file of that name. */
static void test_drop(void)
{
    struct engine *a;
    struct task_holder h;
    struct vsr_id x;
    struct vsr_id y;
    uint64_t op;
    uint64_t sequence = fresh_store(&a, 11, 2);

    (void)sequence;
    x = capture(a);
    y = capture(a);
    settle();
    /* The caller's failure keeps the file. */
    joint_run(a, VSR_OP_SNAPSHOT_DROP, x, VSR_IO_FAILED, VSR_IO_FAILED);
    CHECK(clients_file(a, x, false) != NULL && entry_of(a, x) != NULL);
    /* OK: closed (y is the latest, its slot is kept) and unlinked. */
    CHECK(entry_of(a, y)->file_slot >= 0);
    joint_run(a, VSR_OP_SNAPSHOT_DROP, y, VSR_IO_OK, VSR_IO_OK);
    CHECK(clients_file(a, y, false) == NULL && entry_of(a, y) == NULL);
    /* An unlink failure is reported; the entry is gone (the file is a
     * stray now). */
    a->fail_unlink = -EIO;
    joint_run(a, VSR_OP_SNAPSHOT_DROP, x, VSR_IO_OK, VSR_IO_FAILED);
    CHECK(entry_of(a, x) == NULL && clients_file(a, x, false) != NULL);
    /* Unknown ids: the stray file is unlinked; no file is fine too. */
    joint_run(a, VSR_OP_SNAPSHOT_DROP, x, VSR_IO_OK, VSR_IO_OK);
    CHECK(clients_file(a, x, false) == NULL && entry_of(a, x) == NULL);
    joint_run(a, VSR_OP_SNAPSHOT_DROP, x, VSR_IO_OK, VSR_IO_OK);
    joint_run(a, VSR_OP_SNAPSHOT_DROP, x, VSR_IO_RETRY, VSR_IO_RETRY);
    CHECK(entry_of(a, x) == NULL);
    /* Refusals: a zero id, a file being written, a second DROP. */
    {
        struct vsr_id zero = {0, 0};
        struct task_holder h2;
        uint64_t op2;
        uint64_t capture_op;
        struct vsr_id w;

        CHECK(joint_start(a, VSR_OP_SNAPSHOT_DROP, &h, zero, &op) ==
              VSR_IO_FAILED);
        a->hold_pool_writes = true;
        w = capture_begin(a, &h2, &capture_op);
        CHECK(joint_start(a, VSR_OP_SNAPSHOT_DROP, &h, w, &op) ==
              VSR_IO_FAILED);
        a->hold_pool_writes = false;
        world_release_held(a);
        settle();
        task_result(&h2, w);
        caller_done(a, capture_op, VSR_IO_OK, &h2.checkpoint);
        settle();
        expect_checkpoint(a, capture_op, w, &h2);
        CHECK(joint_start(a, VSR_OP_SNAPSHOT_DROP, &h, w, &op) == VSR_OK);
        CHECK(joint_start(a, VSR_OP_SNAPSHOT_DROP, &h2, w, &op2) ==
              VSR_IO_RETRY);
        settle();
        CHECK(forwarded_core(a, VSR_OP_SNAPSHOT_DROP, op) != NULL);
        caller_done(a, op, VSR_IO_OK, NULL);
        settle();
        expect_core(a, op, VSR_IO_OK);
        CHECK(joint_start(a, VSR_OP_SNAPSHOT_DROP, &h2, w, &op2) == VSR_OK);
        settle();
        CHECK(forwarded_core(a, VSR_OP_SNAPSHOT_DROP, op2) != NULL);
        caller_done(a, op2, VSR_IO_OK, NULL);
        settle();
        expect_core(a, op2, VSR_IO_OK);
    }
    expect_idle(a);
    engine_crash(a);
}

/* The requester side's failures. A file the source serves wrongly (a bad
 * record CRC, a record longer than result_bytes, a header naming another
 * snapshot, a file cut before its trailer) is CORRUPT; a source that ends
 * short or fails to read is FAILED; a lost link RETRY; the requester's own
 * open, write, short write and rename errors FAILED. In every case the
 * temporary file is unlinked and never renamed, and the core hears the
 * status without the caller. */
static void test_fetch_failures(void)
{
    struct engine *a = &world.engines[0];
    struct engine *b = &world.engines[1];
    struct vsr_id x;
    struct dfile *source;
    struct craft craft;

    for (uint32_t fault = 0; fault < 7; ++fault) {
        x = fetch_setup(20 + fault, 8);
        source = dfile_mutable(a, clients_file(a, x, false));
        switch (fault) {
        case 0: /* A result byte: the record CRC fails. */
            source->data[64 + 2 * 108 + 50] ^= 0x01;
            break;
        case 1: /* The CRC itself. */
            source->data[64 + 108 - 1] ^= 0x80;
            break;
        case 2: /* A record announcing more than result_bytes, sealed. */
            craft_file(&craft, x, 3, 128);
            (void)file_install(a, x, false, craft.bytes, craft.size);
            entry_mutable(a, x)->bytes = craft.size;
            break;
        case 3: /* Another snapshot's header. */
        {
            struct vsr_id other = {x.hi, x.lo ^ 1};

            craft_file(&craft, other, 3, 8);
            (void)file_install(a, x, false, craft.bytes, craft.size);
            entry_mutable(a, x)->bytes = craft.size;
            break;
        }
        case 4: /* Cut before the trailer, consistently: END OK. */
            source->size -= 8;
            entry_mutable(a, x)->bytes = source->size;
            break;
        case 5: /* A record length beyond every byte that follows. */
            craft_file(&craft, x, 3, 8);
            put_le32(craft.bytes + craft.record_at[2] + 36, 60);
            craft_seal(&craft, 2);
            (void)file_install(a, x, false, craft.bytes, craft.size);
            entry_mutable(a, x)->bytes = craft.size;
            break;
        default: /* A count above max_clients. */
            craft_file(&craft, x, 3, 8);
            {
                struct vsr_io_wire_clients_header header;
                struct vsr_io_cursor cursor;

                vsr_io_cursor_init_one(&cursor, craft.bytes, 64);
                CHECK(vsr_io_codec_get_clients_header(&cursor, &header) ==
                      VSR_OK);
                header.count = 9;
                vsr_io_codec_put_clients_header(&header, craft.bytes);
            }
            (void)file_install(a, x, false, craft.bytes, craft.size);
            entry_mutable(a, x)->bytes = craft.size;
            break;
        }
        fetch_fails(b, x, VSR_IO_CORRUPT);
    }
    /* The source ends short (its file is smaller than the bytes it
     * serves), or its read fails: END FAILED. The first case delivers a
     * complete, valid file before the END: still no rename. */
    for (uint32_t fault = 0; fault < 3; ++fault) {
        x = fetch_setup(30 + fault, 8);
        source = dfile_mutable(a, clients_file(a, x, false));
        if (fault == 0) {
            entry_mutable(a, x)->bytes = source->size + 100;
        } else if (fault == 1) {
            source->size -= 200;
        } else {
            a->fail_read = -EIO;
        }
        fetch_fails(b, x, VSR_IO_FAILED);
    }
    /* The requester's own errors. */
    for (uint32_t fault = 0; fault < 4; ++fault) {
        x = fetch_setup(40 + fault, 8);
        if (fault == 0) {
            b->fail_open = -EACCES;
        } else if (fault == 1) {
            b->fail_write = -ENOSPC;
        } else if (fault == 2) {
            b->short_write = true;
        } else {
            b->fail_rename = -EXDEV;
        }
        fetch_fails(b, x, VSR_IO_FAILED);
        CHECK(b->snapshots->error != 0);
    }
    /* The source has no such file: NOT_FOUND. */
    x = fetch_setup(50, 2);
    {
        struct vsr_id unknown = {x.hi ^ 0xFF, x.lo};

        fetch_fails(b, unknown, VSR_IO_NOT_FOUND);
    }
    /* The link is lost mid-transfer: RETRY on both sides. */
    x = fetch_setup(51, 8);
    {
        struct task_holder h;
        uint64_t op;
        uint32_t rounds = 0;
        const struct vsr_io_clients_reader *r = &b->snapshots->reader;

        CHECK(fetch_start(b, &h, x, 1, &op) == VSR_OK);
        while (r->file_offset + r->filled == 0) {
            CHECK(!world_run_checked(1) || rounds < 100);
            CHECK(++rounds < 200);
        }
        CHECK(r->stage != 3 /* DONE */);
        link_reset(b, fetch_link(b));
        settle();
        expect_core(b, op, VSR_IO_RETRY);
        CHECK(clients_file(b, x, false) == NULL &&
              clients_file(b, x, true) == NULL && entry_of(b, x) == NULL);
        expect_idle(b);
        expect_idle(a);
        CHECK(entry_of(a, x)->readers == 0);
    }
    /* A chunk at another offset than the bytes received (the stream module
     * never delivers one; the hook checks all the same): FAILED. */
    x = fetch_setup(52, 8);
    {
        struct task_holder h;
        uint64_t op;
        uint32_t rounds = 0;
        const struct vsr_io_clients_reader *r = &b->snapshots->reader;
        struct vsr_span bytes = {craft.bytes, 16};

        b->hold_pool_writes = true;
        CHECK(fetch_start(b, &h, x, 1, &op) == VSR_OK);
        while (r->file_offset + r->filled == 0) {
            (void)world_run_checked(1);
            CHECK(++rounds < 200);
        }
        vsr_io_snapshots_stream_data(b->io, REPLICA, r->stream, 0,
                                     r->file_offset + r->filled + 1, &bytes,
                                     NONE);
        CHECK(r->stage == 4 /* FAILED */);
        b->hold_pool_writes = false;
        world_release_held(b);
        settle();
        expect_core(b, op, VSR_IO_FAILED);
        CHECK(clients_file(b, x, false) == NULL &&
              clients_file(b, x, true) == NULL);
        expect_idle(b);
    }
    /* The file is renamed only after the verified END: with the rename
     * held, the stream is over and only the temporary file exists. */
    x = fetch_setup(53, 8);
    {
        struct task_holder h;
        uint64_t op;

        b->hold_rename = true;
        CHECK(fetch_start(b, &h, x, 1, &op) == VSR_OK);
        settle();
        CHECK(b->io->streams.active == 0 && held_count(b) == 1);
        CHECK(clients_file(b, x, false) == NULL &&
              clients_file(b, x, true) != NULL);
        CHECK(files_equal(clients_file(a, x, false), clients_file(b, x, true)));
        CHECK(forwarded_count(b, VSR_IO_OP_CORE) == 0);
        b->hold_rename = false;
        world_release_held(b);
        settle();
        CHECK(clients_file(b, x, false) != NULL &&
              clients_file(b, x, true) == NULL);
        CHECK(forwarded_core(b, VSR_OP_SNAPSHOT_FETCH, op) != NULL);
        /* The caller fails: the private file is removed (vsr.h). */
        caller_done(b, op, VSR_IO_RETRY, NULL);
        settle();
        expect_core(b, op, VSR_IO_RETRY);
        CHECK(clients_file(b, x, false) == NULL && entry_of(b, x) == NULL);
        expect_idle(b);
    }
    /* Refusals at once: no node authorized for the peer, a fetch already
     * running, no lease; a FETCH of a held id transfers nothing. */
    x = fetch_setup(54, 8);
    {
        struct task_holder h;
        struct task_holder h2;
        uint64_t op;
        uint64_t op2;
        struct vsr_id other = {x.hi, x.lo + 1};
        uint32_t leases[REGIONS];
        uint32_t count = 0;
        uint32_t lease;
        uint32_t links = links_in_state(b, VSR_IO_LINK_ESTABLISHED);

        CHECK(fetch_start(b, &h, x, 9, &op) == VSR_IO_RETRY);
        b->hold_open = true;
        CHECK(fetch_start(b, &h, x, 1, &op) == VSR_OK);
        CHECK(fetch_start(b, &h2, other, 1, &op2) == VSR_IO_RETRY);
        CHECK(fetch_start(b, &h2, x, 1, &op2) == VSR_IO_RETRY);
        b->hold_open = false;
        world_release_held(b);
        settle();
        CHECK(forwarded_core(b, VSR_OP_SNAPSHOT_FETCH, op) != NULL);
        task_result(&h, x);
        caller_done(b, op, VSR_IO_OK, &h.checkpoint);
        settle();
        expect_checkpoint(b, op, x, &h);
        while ((lease = vsr_io_lease_alloc(b->replica, NONE, NONE)) != NONE) {
            leases[count++] = lease;
        }
        CHECK(fetch_start(b, &h, x, 1, &op) == VSR_IO_RETRY);
        while (count > 0) {
            vsr_io_lease_release(b->replica, leases[--count]);
        }
        CHECK(fetch_start(b, &h, x, 1, &op) == VSR_OK);
        CHECK(b->io->streams.active == 0);
        settle();
        CHECK(forwarded_core(b, VSR_OP_SNAPSHOT_FETCH, op) != NULL);
        task_result(&h, x);
        caller_done(b, op, VSR_IO_OK, &h.checkpoint);
        settle();
        expect_checkpoint(b, op, x, &h);
        CHECK(links_in_state(b, VSR_IO_LINK_ESTABLISHED) == links);
        /* The held file is kept when that caller fails. */
        CHECK(fetch_start(b, &h, x, 1, &op) == VSR_OK);
        settle();
        CHECK(forwarded_core(b, VSR_OP_SNAPSHOT_FETCH, op) != NULL);
        caller_done(b, op, VSR_IO_FAILED, NULL);
        settle();
        expect_core(b, op, VSR_IO_FAILED);
        CHECK(clients_file(b, x, false) == NULL); /* Not adopted: private. */
        expect_idle(b);
    }
}

/* The source side: refusals, the serve's open, its end in every state,
 * the file-operation bound, DROP under a reader. */
static void test_serve(void)
{
    struct engine *a = &world.engines[0];
    struct engine *b = &world.engines[1];
    struct vsr_id x;

    /* Refusals of the request itself (the hook, called directly). */
    x = fetch_setup(60, 2);
    {
        struct vsr_io_wire_library_request request;
        struct vsr_io_wire_library_request bad;

        memset(&request, 0, sizeof(request));
        request.magic = VSR_IO_LIBRARY_MAGIC;
        request.version = VSR_IO_LIBRARY_REQUEST_VERSION;
        request.kind = VSR_IO_LIBRARY_CLIENTS;
        request.cluster_hi = 0x77;
        request.cluster_lo = 0x99;
        request.replica = 1;
        request.snapshot_hi = x.hi;
        request.snapshot_lo = x.lo;
        CHECK(vsr_io_snapshots_serve(a->io, STREAMS, &request) ==
              VSR_IO_FAILED);
        bad = request;
        bad.version = 2;
        CHECK(vsr_io_snapshots_serve(a->io, 1, &bad) == VSR_IO_NOT_FOUND);
        bad = request;
        bad.kind = 2;
        CHECK(vsr_io_snapshots_serve(a->io, 1, &bad) == VSR_IO_NOT_FOUND);
        bad = request;
        bad.cluster_lo = 0x98;
        CHECK(vsr_io_snapshots_serve(a->io, 1, &bad) == VSR_IO_NOT_FOUND);
        bad = request;
        bad.replica = 2;
        CHECK(vsr_io_snapshots_serve(a->io, 1, &bad) == VSR_IO_NOT_FOUND);
        bad = request;
        bad.snapshot_lo ^= 1;
        CHECK(vsr_io_snapshots_serve(a->io, 1, &bad) == VSR_IO_NOT_FOUND);
        CHECK(vsr_io_snapshots_serve(a->io, 1, NULL) == VSR_IO_FAILED);
        expect_idle(a);
    }
    /* A file whose CAPTURE is outstanding is not served; once the core has
     * it, it is. */
    {
        struct task_holder h;
        uint64_t op;
        struct vsr_id y = capture_begin(a, &h, &op);

        fetch_fails(b, y, VSR_IO_NOT_FOUND);
        task_result(&h, y);
        caller_done(a, op, VSR_IO_OK, &h.checkpoint);
        settle();
        expect_checkpoint(a, op, y, &h);
        fetch(b, y, 1);
    }
    /* The serve's open fails: FAILED, or NOT_FOUND for a missing file. */
    x = fetch_setup(61, 2);
    a->fail_open = -EMFILE;
    fetch_fails(b, x, VSR_IO_FAILED);
    dfile_mutable(a, clients_file(a, x, false))->name[0] = 0;
    fetch_fails(b, x, VSR_IO_NOT_FOUND);
    CHECK(entry_of(a, x)->readers == 0);

    /* Every file operation busy at the source: the serve's open waits
     * (the retry deadline armed) instead of hanging, then proceeds. */
    x = fetch_setup(62, 8);
    {
        struct task_holder hs[3];
        uint64_t ops[3];
        struct vsr_id ids[3];
        struct task_holder hc;
        uint64_t capture_op;
        struct vsr_id w;
        struct task_holder hf;
        uint64_t fetch_op;

        ids[0] = x;
        ids[1] = capture(a);
        ids[2] = capture(a);
        a->hold_fsync = true;
        for (uint32_t i = 0; i < 3; ++i) {
            CHECK(joint_start(a, VSR_OP_SNAPSHOT_SYNC, &hs[i], ids[i],
                              &ops[i]) == VSR_OK);
        }
        settle();
        for (uint32_t i = 0; i < 3; ++i) {
            CHECK(forwarded_core(a, VSR_OP_SNAPSHOT_SYNC, ops[i]) != NULL);
        }
        a->hold_pool_writes = true;
        w = capture_begin(a, &hc, &capture_op);
        CHECK(held_count(a) == 4);
        CHECK(fetch_start(b, &hf, x, 1, &fetch_op) == VSR_OK);
        settle();
        CHECK(a->snapshots->serves[0].state != 0 &&
              a->snapshots->serves[0].fileop == NONE);
        CHECK(capture_deadline_armed(a));
        a->hold_fsync = false;
        a->hold_pool_writes = false;
        world_release_held(a);
        settle_retry();
        CHECK(forwarded_core(b, VSR_OP_SNAPSHOT_FETCH, fetch_op) != NULL);
        task_result(&hf, x);
        caller_done(b, fetch_op, VSR_IO_OK, &hf.checkpoint);
        settle();
        expect_checkpoint(b, fetch_op, x, &hf);
        CHECK(
            files_equal(clients_file(a, x, false), clients_file(b, x, false)));
        for (uint32_t i = 0; i < 3; ++i) {
            caller_done(a, ops[i], VSR_IO_OK, NULL);
        }
        task_result(&hc, w);
        caller_done(a, capture_op, VSR_IO_OK, &hc.checkpoint);
        settle();
        expect_cores(a, ops, 3, VSR_IO_OK);
        expect_checkpoint(a, capture_op, w, &hc);
        expect_idle(a);
        expect_idle(b);
    }

    /* The stream ends while the serve's open is out: the slot is closed
     * once it completes; with the open never issued, freed at once. */
    x = fetch_setup(63, 8);
    {
        struct task_holder h;
        uint64_t op;
        uint32_t rounds = 0;

        a->hold_open = true;
        CHECK(fetch_start(b, &h, x, 1, &op) == VSR_OK);
        while (held_count(a) == 0) {
            (void)world_run_checked(1);
            CHECK(++rounds < 200);
        }
        CHECK(a->snapshots->serves[0].fileop != NONE);
        link_reset(b, fetch_link(b));
        settle();
        expect_core(b, op, VSR_IO_RETRY);
        CHECK(a->snapshots->serves[0].ended == 1 &&
              entry_of(a, x)->readers == 1);
        a->hold_open = false;
        world_release_held(a);
        settle();
        expect_idle(a);
        expect_idle(b);
    }
    x = fetch_setup(64, 8);
    {
        struct task_holder hs[3];
        uint64_t ops[3];
        struct vsr_id ids[3];
        struct task_holder hc;
        uint64_t capture_op;
        struct vsr_id w;
        struct task_holder hf;
        uint64_t fetch_op;

        ids[0] = x;
        ids[1] = capture(a);
        ids[2] = capture(a);
        a->hold_fsync = true;
        for (uint32_t i = 0; i < 3; ++i) {
            CHECK(joint_start(a, VSR_OP_SNAPSHOT_SYNC, &hs[i], ids[i],
                              &ops[i]) == VSR_OK);
        }
        settle();
        for (uint32_t i = 0; i < 3; ++i) {
            CHECK(forwarded_core(a, VSR_OP_SNAPSHOT_SYNC, ops[i]) != NULL);
            caller_done(a, ops[i], VSR_IO_OK, NULL);
        }
        a->hold_pool_writes = true;
        w = capture_begin(a, &hc, &capture_op);
        CHECK(fetch_start(b, &hf, x, 1, &fetch_op) == VSR_OK);
        settle();
        CHECK(a->snapshots->serves[0].state != 0 &&
              a->snapshots->serves[0].fileop == NONE);
        link_reset(b, fetch_link(b));
        settle();
        expect_core(b, fetch_op, VSR_IO_RETRY);
        CHECK(a->snapshots->serves[0].state == 0 &&
              entry_of(a, x)->readers == 0);
        a->hold_fsync = false;
        a->hold_pool_writes = false;
        world_release_held(a);
        settle();
        task_result(&hc, w);
        caller_done(a, capture_op, VSR_IO_OK, &hc.checkpoint);
        settle();
        expect_cores(a, ops, 3, VSR_IO_OK);
        expect_checkpoint(a, capture_op, w, &hc);
        expect_idle(a);
        expect_idle(b);
    }
    /* An open that fails after the stream ended closes nothing: the
     * stream's index may carry a caller's stream already. */
    x = fetch_setup(65, 8);
    {
        struct task_holder h;
        uint64_t op;
        uint32_t rounds = 0;
        struct vsr_io_stream_open open;
        unsigned char request[16] = {1, 2, 3};
        uint32_t index = NONE;
        const struct vsr_io_forwarded *serve;

        a->hold_open = true;
        CHECK(fetch_start(b, &h, x, 1, &op) == VSR_OK);
        while (held_count(a) == 0) {
            (void)world_run_checked(1);
            CHECK(++rounds < 200);
        }
        link_reset(b, fetch_link(b));
        settle();
        expect_core(b, op, VSR_IO_RETRY);
        CHECK(a->io->streams.streams[0].state == VSR_IO_STREAM_FREE);
        memset(&open, 0, sizeof(open));
        open.node = 1;
        open.request.data = request;
        open.request.size = sizeof(request);
        CHECK(vsr_io_streams_open(b->io, 77, &open, 0, VSR_IO_STREAM_CALLER,
                                  &index) == VSR_OK);
        settle();
        serve = forwarded_take(a, VSR_IO_OP_STREAM_SERVE);
        CHECK(serve != NULL && (uint32_t)serve->rail.serve.stream == 0);
        a->hold_open = false;
        a->fail_open = -EIO;
        world_release_held(a);
        settle();
        CHECK(a->io->streams.streams[0].state == VSR_IO_STREAM_SERVING);
        CHECK(vsr_io_streams_served(a->io, serve->op.op.id, VSR_IO_NOT_FOUND) ==
              VSR_OK);
        settle();
        CHECK(a->io->streams.active == 0 && b->io->streams.active == 0);
        expect_idle(a);
    }

    /* DROP while a served stream reads the file: the core hears OK at
     * once, the unlink waits for the stream's end. */
    x = fetch_setup(66, 8);
    {
        struct task_holder h;
        uint64_t op;
        struct task_holder hd;
        uint64_t drop_op;
        uint32_t rounds = 0;

        a->hold_reads = true;
        CHECK(fetch_start(b, &h, x, 1, &op) == VSR_OK);
        while (held_count(a) == 0) {
            (void)world_run_checked(1);
            CHECK(++rounds < 200);
        }
        CHECK(entry_of(a, x)->readers == 1);
        CHECK(joint_start(a, VSR_OP_SNAPSHOT_DROP, &hd, x, &drop_op) == VSR_OK);
        settle();
        CHECK(forwarded_core(a, VSR_OP_SNAPSHOT_DROP, drop_op) != NULL);
        caller_done(a, drop_op, VSR_IO_OK, NULL);
        settle();
        expect_core(a, drop_op, VSR_IO_OK);
        CHECK(clients_file(a, x, false) != NULL &&
              entry_of(a, x)->state == VSR_IO_SNAPSHOT_DROPPING);
        a->hold_reads = false;
        world_release_held(a);
        settle();
        CHECK(clients_file(a, x, false) == NULL && entry_of(a, x) == NULL);
        CHECK(forwarded_core(b, VSR_OP_SNAPSHOT_FETCH, op) != NULL);
        task_result(&h, x);
        caller_done(b, op, VSR_IO_OK, &h.checkpoint);
        settle();
        expect_checkpoint(b, op, x, &h);
        CHECK(clients_file(b, x, false) != NULL);
        expect_idle(a);
        expect_idle(b);
    }
}

int main(void)
{
    /* TEMP: helpers of the tests still to come. */
    (void)link_to;
    (void)expect_no_store;
    (void)txn_restore;
    (void)submit_sync;
    (void)store_populate;
    (void)expect_no_core;
    test_capture();
    test_fetch();
    test_capture_failures();
    test_sync();
    test_drop();
    test_fetch_failures();
    test_serve();
    return 0;
}
