/* The GNU declarations (O_DIRECT, AT_FDCWD) precede every system header. */
#define _GNU_SOURCE
#include "config.h"

#include "io/codec.h"
#include "io/engine.h"
#include "io/store.h"
#include "lib/check.h"
#include "vsr-io.h"

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdalign.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * Store unit test, phase 1: layout checks, creation of an empty store,
 * packing into the tail ring, extents and the wrap, the write pipeline
 * (write-behind and pinned holds, SYNC after every earlier write and the
 * flush, the never-rewrite rule), segment sealing and growth, the
 * superblock writes, status and close.
 *
 * The store is driven directly: a struct vsr_io built by vsr_io_init over
 * a fake executor supplies the pool, the slot table and the deadline set;
 * replica 0 is set up by hand (engine part 2's attach does the same). The
 * harness collects the SQEs vsr_io_store_prepare emits and applies them,
 * in any order the test chooses, to an in-memory DISK MODEL: a byte array
 * that honours the file operations, checks O_DIRECT alignment and the
 * registered regions, asserts that a segment block is never written twice
 * (decision 33), and can tear a write at a block boundary and lose it,
 * for the recovery tests of later phases.
 */

#define PAGE 4096u
#define NONE UINT32_MAX
#define SLABS 24u
#define OWNER 0x5Au
#define FILE_SLOT_BASE 100u
#define REGION_BASE 5u
#define TAIL_REGION (REGION_BASE + 1u)
#define GROUP 3u
#define REPLICA 0u
#define REGIONS 8u
#define BLOCK UINT64_C(512)
#define DISK_BYTES (1u << 20)
#define DISK_BLOCKS (DISK_BYTES / 512u)
#define PENDING_MAX 64u
#define TXN_MAX 1024u
#define DIRECTORY "store-dir"
/* Deadline handles of replica 0: LINK (4) + DIAL (4) + CORE (2) = 10. */
#define DEADLINE_FLUSH 10u
#define DEADLINE_SYNC 12u

static _Alignas(4096) unsigned char metadata[1u << 20];
static _Alignas(4096) unsigned char payload[SLABS * PAGE];
static _Alignas(4096) unsigned char store_metadata[1u << 20];
static _Alignas(4096) unsigned char tail_memory[1u << 20];
static unsigned char disk_image[DISK_BYTES];

/* members, operations, input_leases, pending_requests, pending_reads,
 * transfers, log_cache_entries, client_cache_entries, batch_entries,
 * spans_per_blob, work_per_step, command_bytes, result_bytes,
 * manifest_bytes, message_bytes, pinned_payload_bytes. */
static const struct vsr_limits limits = {3, 16, 4,  4,   4,  2,  16,   8,
                                         4, 2,  16, 256, 64, 64, 1024, 1024};

/* -------------------------------------------------------------------------
 * Disk model
 * ---------------------------------------------------------------------- */

struct disk {
    uint64_t size;   /* Allocated bytes. */
    bool exists;     /* The log file exists. */
    int32_t slot;    /* Registered slot holding it, or -1. */
    uint32_t flags;  /* Open flags. */
    uint32_t writes; /* Applied WRITEs of every kind. */
    uint32_t flushes;
    uint32_t fallocates;
    uint8_t once[DISK_BLOCKS];  /* A segment block written already. */
    uint8_t dirty[DISK_BLOCKS]; /* Written since the last flush. */
    /* Faults: the next WRITE persists only `tear_blocks` blocks and its
     * completion is lost (a crash); `fail_write` makes it complete with
     * that errno instead. */
    uint32_t tear_armed;
    uint32_t tear_blocks;
    int32_t fail_write;
    int32_t fail_flush;
};

static struct disk disk;

static void disk_reset(void)
{
    memset(&disk, 0, sizeof(disk));
    memset(disk_image, 0, sizeof(disk_image));
    disk.slot = -1;
}

/* -------------------------------------------------------------------------
 * Fake executor: the store never calls it; the engine registers the pool.
 * ---------------------------------------------------------------------- */

struct fake {
    uint64_t now;
    uint32_t randoms;
};

static struct fake fake;

static uint64_t fake_now(void *ctx)
{
    return ((struct fake *)ctx)->now;
}

static void fake_random(void *ctx, void *bytes, size_t size)
{
    struct fake *f = ctx;
    unsigned char *out = bytes;

    f->randoms++;
    for (size_t i = 0; i < size; ++i) {
        out[i] = (unsigned char)(i * 13u + (size_t)f->randoms * 7u + 1u);
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
    (void)ctx;
    (void)slot;
    (void)fd;
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
    (void)ctx;
    (void)group;
    (void)buffers;
    (void)count;
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
 * Harness: the engine tables, one replica, the pending SQEs
 * ---------------------------------------------------------------------- */

struct config {
    uint64_t segment_bytes;
    uint64_t cache_bytes;
    uint64_t write_behind_bytes;
    uint64_t sync_delay_ns;
    uint64_t flush_interval_ns;
    uint32_t segments;
    uint32_t max_segments;
    uint32_t inflight_writes;
    uint32_t max_entries;
    uint32_t max_clients;
    uint8_t sync_mode;
    uint8_t on_write_error;
    uint8_t direct_io;
    uint32_t durability;
};

struct pending {
    struct vsr_io_sqe sqe;
    uint32_t state; /* 0 free, 1 pending, 2 done. */
};

struct harness {
    struct vsr_io *io;
    struct vsr_io_replica *replica;
    struct vsr_io_store *store;
    struct vsr_io_store_options options;
    struct pending pending[PENDING_MAX];
    uint32_t pending_count;
    uint64_t now;
    uint64_t next_op;
    uint64_t header_bytes;
    uint64_t max_record;
    size_t tail_bytes;
};

static struct harness h;

static void txns_reset(void);

static uint64_t round_up(uint64_t value, uint64_t multiple)
{
    return (value + multiple - 1) / multiple * multiple;
}

static struct vsr_io_options engine_options(void)
{
    struct vsr_io_options options;
    struct vsr_io_limits *l = &options.limits;

    memset(&options, 0, sizeof(options));
    options.executor.ops = &fake_ops;
    options.executor.ctx = &fake;
    options.node = 7;
    options.handshake = VSR_IO_HANDSHAKE_TRUSTED;
    l->replicas = 2;
    l->nodes = 4;
    l->authorizations = 8;
    l->links = 4;
    l->link_queue = 4;
    l->streams = 2;
    l->stream_window = 2;
    l->events = 8;
    l->ops = 6;
    l->batch = 16;
    l->slabs = SLABS;
    l->slab_bytes = PAGE;
    l->caller_slabs = 0;
    l->file_slots = 12;
    l->buffer_regions = 3;
    options.file_slot_base = FILE_SLOT_BASE;
    options.buffer_region_base = REGION_BASE;
    options.buffer_group = GROUP;
    options.owner = OWNER;
    options.nodelay = 1;
    options.connect_backoff_ns = 1000000;
    options.handshake_timeout_ns = 1000000000;
    options.send_coalesce_bytes = 65536;
    options.zero_copy_bytes = 4096;
    options.stream_chunk_bytes = PAGE - 40;
    return options;
}

static struct vsr_io_store_options store_options(const struct config *c)
{
    struct vsr_io_store_options o;

    memset(&o, 0, sizeof(o));
    o.block_bytes = BLOCK;
    o.segments = c->segments;
    o.max_segments = c->max_segments;
    o.max_entries = c->max_entries;
    o.max_clients = c->max_clients;
    o.inflight_writes = c->inflight_writes;
    o.segment_bytes = c->segment_bytes;
    o.sync_delay_ns = c->sync_delay_ns;
    o.flush_interval_ns = c->flush_interval_ns;
    o.write_behind_bytes = c->write_behind_bytes;
    o.cache_bytes = c->cache_bytes;
    o.direct_io = c->direct_io;
    o.sync_mode = c->sync_mode;
    o.on_write_error = c->on_write_error;
    return o;
}

/* The derived sizes every configuration is built around. */
static void derive(uint64_t *header_bytes, uint64_t *max_record)
{
    size_t segment_limit = 0;

    CHECK(vsr_io_codec_record_limit(&limits, max_record) == VSR_OK);
    CHECK(vsr_io_codec_segment_limit(&limits, &segment_limit) == VSR_OK);
    *header_bytes = round_up(segment_limit, BLOCK);
}

/* A configuration whose segments hold a few records and whose ring the
 * check rule just admits: the tests then shrink or grow one knob. */
static struct config base_config(void)
{
    struct config c;
    uint64_t header_bytes;
    uint64_t max_record;

    derive(&header_bytes, &max_record);
    memset(&c, 0, sizeof(c));
    c.segment_bytes = round_up(header_bytes + 4 * max_record, BLOCK);
    c.write_behind_bytes = 4 * BLOCK;
    c.cache_bytes =
        round_up(c.write_behind_bytes + limits.pinned_payload_bytes +
                     2 * max_record + 2 * header_bytes + BLOCK,
                 BLOCK);
    c.segments = 2;
    c.max_segments = 4;
    c.inflight_writes = 2;
    c.max_entries = 4096; /* The write tests never reclaim. */
    c.max_clients = 8;
    c.sync_mode = VSR_IO_SYNC_FDATASYNC;
    c.on_write_error = VSR_IO_WRITE_ERROR_FENCE;
    c.direct_io = 1;
    c.durability = VSR_DURABLE;
    return c;
}

static void harness_open(const struct config *c)
{
    struct vsr_io_options options = engine_options();
    struct vsr_io_layout layout;
    struct vsr_io_region region;
    struct vsr_io_region pool;
    size_t bytes = 0;
    size_t alignment = 0;
    size_t tail_alignment = 0;

    memset(&h, 0, sizeof(h));
    memset(&fake, 0, sizeof(fake));
    txns_reset();
    disk_reset();
    h.now = 1000;
    h.next_op = 1;
    derive(&h.header_bytes, &h.max_record);
    CHECK(vsr_io_layout(&options, &layout) == VSR_OK);
    CHECK(layout.metadata.size <= sizeof(metadata));
    CHECK(layout.payload.size <= sizeof(payload));
    region.base = metadata;
    region.size = layout.metadata.size;
    pool.base = payload;
    pool.size = layout.payload.size;
    CHECK(vsr_io_init(&options, &region, &pool, &h.io) == VSR_OK);
    h.replica = &h.io->replicas[REPLICA];
    h.replica->state = VSR_IO_REPLICA_RUNNING;
    h.replica->options.limits = limits;
    h.replica->options.durability = c->durability;
    h.replica->deadline_flush = DEADLINE_FLUSH;
    h.replica->deadline_sync = DEADLINE_SYNC;
    h.store = &h.replica->store;
    h.options = store_options(c);
    CHECK(vsr_io_store_check(&h.options, &limits, PAGE) == VSR_OK);
    CHECK(vsr_io_store_size(&h.options, &limits, REGIONS, &bytes, &alignment,
                            &h.tail_bytes, &tail_alignment) == VSR_OK);
    CHECK(bytes <= sizeof(store_metadata) &&
          h.tail_bytes <= sizeof(tail_memory));
    CHECK(alignment <= PAGE && tail_alignment == PAGE);
    CHECK(h.tail_bytes == c->cache_bytes + 2 * BLOCK);
    memset(store_metadata, 0xEE, sizeof(store_metadata));
    memset(tail_memory, 0xEE, sizeof(tail_memory));
    vsr_io_store_init(h.store, store_metadata, bytes, tail_memory, h.tail_bytes,
                      &h.options, &limits, REGIONS, TAIL_REGION, DIRECTORY,
                      AT_FDCWD);
    CHECK(h.store->state == VSR_IO_STORE_CLOSED);
    CHECK(h.store->header_bytes == h.header_bytes);
    CHECK(h.store->max_record_bytes == h.max_record);
}

static void harness_close(void)
{
    CHECK(vsr_io_close(h.io) == VSR_OK);
    CHECK(vsr_io_deinit(h.io) == VSR_OK);
}

static uint64_t next_op(void)
{
    return h.next_op++;
}

/* Collects the SQEs of one prepare into the pending table. */
static uint32_t harness_prepare(void)
{
    struct vsr_io_sqe sqes[PENDING_MAX];
    uint32_t count = 0;

    vsr_io_store_prepare(h.io, REPLICA, sqes, PENDING_MAX, &count);
    for (uint32_t i = 0; i < count; ++i) {
        uint32_t at = NONE;

        for (uint32_t j = 0; j < PENDING_MAX; ++j) {
            if (h.pending[j].state == 0) {
                at = j;
                break;
            }
        }
        CHECK(at != NONE);
        h.pending[at].sqe = sqes[i];
        h.pending[at].state = 1;
        h.pending_count++;
    }
    return count;
}

static void harness_poll(void)
{
    vsr_io_store_poll(h.io, REPLICA, h.now);
}

static bool in_tail(const void *addr, size_t length)
{
    const unsigned char *at = addr;

    return at >= tail_memory && length <= h.tail_bytes &&
           (size_t)(at - tail_memory) <= h.tail_bytes - length;
}

static bool in_pool(const void *addr, size_t length)
{
    const unsigned char *at = addr;

    return at >= payload && length <= sizeof(payload) &&
           (size_t)(at - payload) <= sizeof(payload) - length;
}

/* The writable view of a registered address a READ targets. */
static unsigned char *writable(const void *addr, size_t length)
{
    const unsigned char *at = addr;

    if (in_tail(addr, length)) {
        return tail_memory + (at - tail_memory);
    }
    CHECK(in_pool(addr, length));
    return payload + (at - payload);
}

/* Applies one SQE to the disk and returns its result, or false when the
 * operation is lost (a torn write). */
static bool disk_apply(const struct vsr_io_sqe *sqe, int32_t *result)
{
    uint64_t end;

    switch (sqe->opcode) {
    case VSR_IO_SQE_OPENAT: {
        bool create = (sqe->op_flags & O_CREAT) != 0;

        CHECK(sqe->fd == AT_FDCWD);
        CHECK(sqe->flags == VSR_IO_SQE_DIRECT);
        CHECK(strcmp(sqe->addr, DIRECTORY "/log") == 0);
        CHECK((sqe->op_flags & O_ACCMODE) == O_RDWR);
        CHECK(((sqe->op_flags & O_DIRECT) != 0) == (h.options.direct_io != 0));
        CHECK(((sqe->op_flags & O_DSYNC) != 0) ==
              (h.options.sync_mode == VSR_IO_SYNC_DSYNC));
        if (create) {
            CHECK((sqe->op_flags & O_EXCL) != 0);
            CHECK(sqe->length == 0644);
            if (disk.exists) {
                *result = -EEXIST;
                return true;
            }
            disk.exists = true;
            disk.size = 0;
        } else if (!disk.exists) {
            *result = -ENOENT;
            return true;
        }
        CHECK(sqe->fd2 >= (int32_t)FILE_SLOT_BASE);
        disk.slot = sqe->fd2;
        disk.flags = sqe->op_flags;
        *result = sqe->fd2;
        return true;
    }
    case VSR_IO_SQE_FALLOCATE:
        CHECK(sqe->flags == VSR_IO_SQE_FIXED_FILE && sqe->fd == disk.slot);
        CHECK(sqe->op_flags == 0);
        end = sqe->offset + sqe->length;
        CHECK(end <= DISK_BYTES);
        CHECK(sqe->offset == disk.size); /* Sequential preallocation. */
        if (end > disk.size) {
            disk.size = end;
        }
        disk.fallocates++;
        *result = 0;
        return true;
    case VSR_IO_SQE_WRITE: {
        uint64_t first;
        uint64_t blocks;
        uint64_t keep;

        CHECK(sqe->flags == (VSR_IO_SQE_FIXED_FILE | VSR_IO_SQE_FIXED_BUFFER));
        CHECK(sqe->fd == disk.slot);
        CHECK(sqe->buffer_index == TAIL_REGION);
        CHECK(in_tail(sqe->addr, sqe->length));
        CHECK(sqe->length > 0);
        if ((disk.flags & O_DIRECT) != 0) {
            CHECK(sqe->offset % BLOCK == 0 && sqe->length % BLOCK == 0);
        }
        end = sqe->offset + sqe->length;
        CHECK(end <= disk.size);
        if (disk.fail_write != 0) {
            *result = disk.fail_write;
            disk.fail_write = 0;
            return true;
        }
        first = sqe->offset / BLOCK;
        blocks = (sqe->length + BLOCK - 1) / BLOCK;
        keep = disk.tear_armed != 0 ? disk.tear_blocks : blocks;
        for (uint64_t b = 0; b < blocks && b < keep; ++b) {
            uint64_t at = first + b;
            uint64_t from = at * BLOCK;
            uint64_t to = from + BLOCK < end ? from + BLOCK : end;

            if (at >= 2) {
                CHECK(disk.once[at] == 0); /* Never rewritten. */
                disk.once[at] = 1;
            }
            memcpy(disk_image + from,
                   (const unsigned char *)sqe->addr + (from - sqe->offset),
                   to - from);
            disk.dirty[at] = (disk.flags & O_DSYNC) == 0 ? 1 : 0;
        }
        disk.writes++;
        if (disk.tear_armed != 0) {
            disk.tear_armed = 0;
            return false;
        }
        *result = (int32_t)sqe->length;
        return true;
    }
    case VSR_IO_SQE_FSYNC:
        CHECK(sqe->flags == VSR_IO_SQE_FIXED_FILE && sqe->fd == disk.slot);
        CHECK(sqe->op_flags == VSR_IO_FSYNC_DATASYNC);
        disk.flushes++;
        if (disk.fail_flush != 0) {
            *result = disk.fail_flush;
            disk.fail_flush = 0;
            return true;
        }
        memset(disk.dirty, 0, sizeof(disk.dirty));
        *result = 0;
        return true;
    case VSR_IO_SQE_READ:
        CHECK((sqe->flags & VSR_IO_SQE_FIXED_FILE) != 0 &&
              sqe->fd == disk.slot);
        CHECK(sqe->offset <= disk.size);
        end = sqe->offset + sqe->length;
        if (end > disk.size) {
            end = disk.size;
        }
        memcpy(writable(sqe->addr, sqe->length), disk_image + sqe->offset,
               end - sqe->offset);
        *result = (int32_t)(end - sqe->offset);
        return true;
    default:
        CHECK(false);
        return false;
    }
}

/* Completes pending SQE `at`: applies it and feeds the CQE back. */
static void harness_complete(uint32_t at)
{
    struct pending *p = &h.pending[at];
    struct vsr_io_cqe cqe;
    struct vsr_io_slot *slot;
    uint32_t index = NONE;
    int32_t result = 0;

    CHECK(p->state == 1);
    p->state = 0;
    h.pending_count--;
    if (!disk_apply(&p->sqe, &result)) {
        return; /* Lost: the completion never arrives. */
    }
    memset(&cqe, 0, sizeof(cqe));
    cqe.user_data = p->sqe.user_data;
    cqe.result = result;
    slot = vsr_io_slots_resolve(&h.io->slots, cqe.user_data, &index);
    CHECK(slot != NULL && slot->owner == REPLICA);
    vsr_io_store_complete(h.io, REPLICA, index, &cqe);
}

/* The oldest pending SQE, or NONE. */
static uint32_t pending_first(void)
{
    for (uint32_t i = 0; i < PENDING_MAX; ++i) {
        if (h.pending[i].state == 1) {
            return i;
        }
    }
    return NONE;
}

static uint32_t pending_of(uint8_t opcode)
{
    for (uint32_t i = 0; i < PENDING_MAX; ++i) {
        if (h.pending[i].state == 1 && h.pending[i].sqe.opcode == opcode) {
            return i;
        }
    }
    return NONE;
}

/* The newest pending SQE of the opcode. */
static uint32_t pending_last_of(uint8_t opcode)
{
    uint32_t found = NONE;

    for (uint32_t i = 0; i < PENDING_MAX; ++i) {
        if (h.pending[i].state == 1 && h.pending[i].sqe.opcode == opcode) {
            found = i;
        }
    }
    return found;
}

/* Runs prepare and completes everything, in order, until quiescent. */
static void harness_run(void)
{
    for (unsigned rounds = 0; rounds < 1000; ++rounds) {
        uint32_t at;

        harness_poll();
        harness_prepare();
        at = pending_first();
        if (at == NONE) {
            return;
        }
        while (at != NONE) {
            harness_complete(at);
            at = pending_first();
        }
    }
    CHECK(false); /* Never quiescent. */
}

static bool next_completion(struct vsr_io_completion *out)
{
    return vsr_io_store_next_completion(h.store, out);
}

static void expect_completion(uint64_t op, int32_t status)
{
    struct vsr_io_completion completion;

    CHECK(next_completion(&completion));
    if (completion.op != op) {
        fprintf(stderr,
                "completion op %" PRIu64 " status %d, expected %" PRIu64 "\n",
                completion.op, completion.status, op);
    }
    CHECK(completion.op == op);
    if (completion.status != status) {
        fprintf(stderr, "completion op %" PRIu64 " status %d, expected %d\n",
                completion.op, completion.status, status);
    }
    CHECK(completion.status == status);
    CHECK(completion.lease == NONE);
    CHECK(completion.data == NULL);
}

static void expect_no_completion(void)
{
    struct vsr_io_completion completion;

    CHECK(!next_completion(&completion));
}

/* Opens the store with `mode` and drives creation to READY. */
static uint64_t harness_start(uint32_t mode)
{
    struct vsr_store_read read;
    uint64_t op = next_op();

    memset(&read, 0, sizeof(read));
    read.type = VSR_LOAD_RECOVERY;
    vsr_io_store_open(h.store, mode, op, &read);
    CHECK(h.store->state == VSR_IO_STORE_OPENING);
    harness_run();
    CHECK(h.store->state == VSR_IO_STORE_READY);
    expect_completion(op, VSR_IO_NOT_FOUND);
    expect_no_completion();
    return op;
}

/* -------------------------------------------------------------------------
 * Transactions
 * ---------------------------------------------------------------------- */

struct txn {
    struct vsr_store store;
    struct vsr_change change;
    struct vsr_entry entries[4];
    struct vsr_blob blobs[4];
    struct vsr_span spans[4];
    unsigned char bodies[4][256];
    size_t bytes; /* Encoded record bytes. */
};

static struct txn txns[TXN_MAX];

static void txns_reset(void)
{
    memset(txns, 0, sizeof(txns));
}

/* The log end after the transactions below `sequence`: APPENDs extend
 * it, a TRUNCATE cuts it; the first op is 1 (the core's genesis). */
static uint64_t log_end_before(uint64_t sequence)
{
    uint64_t end = 1;

    for (uint64_t q = 1; q < sequence; ++q) {
        const struct txn *t = &txns[q];

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

/* An APPEND of `count` COMMAND entries of `body` bytes each at the log
 * end of `sequence`, pinned in the static table until the test ends. */
static const struct txn *txn_append(uint64_t sequence, uint32_t count,
                                    size_t body)
{
    struct txn *t;
    uint64_t first;

    CHECK(sequence < TXN_MAX && count >= 1 && count <= 4 && body <= 256);
    first = log_end_before(sequence);
    t = &txns[sequence];
    memset(t, 0, sizeof(*t));
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
    t->change.type = VSR_STORE_APPEND;
    t->change.count = count;
    t->change.first = first;
    t->change.data = t->entries;
    t->store.sequence = sequence;
    t->store.changes = &t->change;
    t->store.count = 1;
    CHECK(vsr_io_codec_record_bytes(&t->store, &limits, &t->bytes) == VSR_OK);
    return t;
}

/* A TRIM: the smallest record (header and one descriptor). */
static const struct txn *txn_trim(uint64_t sequence)
{
    struct txn *t;

    CHECK(sequence < TXN_MAX);
    t = &txns[sequence];
    memset(t, 0, sizeof(*t));
    t->change.type = VSR_STORE_TRIM;
    t->change.count = 0;
    t->change.first = 1;
    t->change.data = NULL;
    t->store.sequence = sequence;
    t->store.changes = &t->change;
    t->store.count = 1;
    CHECK(vsr_io_codec_record_bytes(&t->store, &limits, &t->bytes) == VSR_OK);
    return t;
}

/* Submits a STORE and returns its op id. */
static uint64_t submit(const struct txn *t)
{
    uint64_t op = next_op();

    CHECK(vsr_io_store_store(h.store, op, &t->store) == VSR_OK);
    return op;
}

static uint64_t submit_sync(uint64_t sequence)
{
    uint64_t op = next_op();

    CHECK(vsr_io_store_sync(h.store, op, sequence) == VSR_OK);
    return op;
}

/* -------------------------------------------------------------------------
 * Inspection of the image and the ring
 * ---------------------------------------------------------------------- */

static const struct vsr_io_extent *newest_extent(void)
{
    CHECK(h.store->extents_count > 0);
    return &h.store
                ->extents[(h.store->extents_head + h.store->extents_count - 1) %
                          h.store->extents_capacity];
}

static uint64_t slot_offset(uint32_t slot)
{
    return 2 * BLOCK + (uint64_t)slot * h.options.segment_bytes;
}

static void read_superblock(uint32_t copy, struct vsr_io_wire_superblock *out)
{
    CHECK(vsr_io_codec_get_superblock(disk_image + copy * BLOCK, BLOCK, out) ==
          VSR_OK);
}

static void read_header(uint32_t slot, struct vsr_io_wire_segment *fixed)
{
    static unsigned char region_memory[8192];
    struct vsr_io_bump region;
    struct vsr_store_identity identity;
    struct vsr_hard_state hard;
    struct vsr_checkpoint *checkpoint = NULL;

    vsr_io_bump_init(&region, region_memory, sizeof(region_memory));
    CHECK(vsr_io_codec_get_segment(
              disk_image + slot_offset(slot), (size_t)h.header_bytes, &limits,
              &region, fixed, &identity, &hard, &checkpoint) == VSR_OK);
}

/* Scans records from `offset` on the image, checking each against the
 * store's stamps, and returns the last sequence seen; PADs and zero fill
 * skip to the next block. */
static uint64_t scan_records(uint64_t offset, uint64_t end,
                             uint64_t expect_first)
{
    uint64_t expected = expect_first;
    uint64_t last = expect_first - 1;

    while (offset < end) {
        struct vsr_io_cursor cursor;
        struct vsr_io_wire_record header;
        uint32_t kind = 0;

        vsr_io_cursor_init_one(&cursor, disk_image + offset, end - offset);
        CHECK(vsr_io_codec_get_record(&cursor, h.max_record, &header, &kind) ==
              VSR_OK);
        if (kind == VSR_IO_SCAN_END) {
            offset = round_up(offset + 1, BLOCK);
            continue;
        }
        if (kind == VSR_IO_SCAN_PAD) {
            CHECK((offset + header.length) % BLOCK == 0);
            offset += header.length;
            continue;
        }
        CHECK(vsr_io_codec_check_record(&cursor, &header));
        CHECK(header.sequence == expected);
        CHECK(header.generation == h.store->generation);
        CHECK(header.run == h.store->run);
        CHECK(header.length == txns[expected].bytes);
        last = expected;
        expected++;
        offset += header.length;
    }
    return last;
}

/* The ring bytes of the newest record, decoded. */
static void check_ring_record(uint64_t sequence, uint64_t flushed)
{
    const struct txn *t = &txns[sequence];
    struct vsr_io_cursor cursor;
    struct vsr_io_wire_record header;
    uint64_t at = (h.store->head - t->bytes) % h.store->ring_size;
    uint32_t kind = 0;

    vsr_io_cursor_init_one(&cursor, h.store->ring + at, t->bytes);
    CHECK(vsr_io_codec_get_record(&cursor, h.max_record, &header, &kind) ==
          VSR_OK);
    CHECK(kind == VSR_IO_SCAN_RECORD);
    CHECK(vsr_io_codec_check_record(&cursor, &header));
    CHECK(header.sequence == sequence);
    CHECK(header.flushed == flushed);
    CHECK(header.length == t->bytes);
}

/* -------------------------------------------------------------------------
 * Tests
 * ---------------------------------------------------------------------- */

static int check_of(const struct vsr_io_store_options *o)
{
    return vsr_io_store_check(o, &limits, PAGE);
}

static void test_check(void)
{
    struct config c = base_config();
    struct vsr_io_store_options base = store_options(&c);
    struct vsr_io_store_options o;
    struct vsr_limits huge;
    size_t bytes = 0;
    size_t alignment = 0;
    size_t tail_bytes = 0;
    size_t tail_alignment = 0;
    uint64_t header_bytes;
    uint64_t max_record;

    derive(&header_bytes, &max_record);
    CHECK(check_of(&base) == VSR_OK);
    CHECK(vsr_io_store_check(NULL, &limits, PAGE) == VSR_EINVAL);
    CHECK(vsr_io_store_check(&base, NULL, PAGE) == VSR_EINVAL);
    /* Geometry. */
    o = base;
    o.block_bytes = 256;
    CHECK(check_of(&o) == VSR_EINVAL);
    o.block_bytes = 768;
    CHECK(check_of(&o) == VSR_EINVAL);
    o = base;
    o.segment_bytes += 8;
    CHECK(check_of(&o) == VSR_EINVAL);
    o.segment_bytes = 0;
    CHECK(check_of(&o) == VSR_EINVAL);
    o = base;
    o.cache_bytes += 8;
    CHECK(check_of(&o) == VSR_EINVAL);
    o = base;
    o.direct_io = 2;
    CHECK(check_of(&o) == VSR_EINVAL);
    o = base;
    o.sync_mode = 2;
    CHECK(check_of(&o) == VSR_EINVAL);
    o = base;
    o.on_write_error = 2;
    CHECK(check_of(&o) == VSR_EINVAL);
    o = base;
    o.reserved[3] = 1;
    CHECK(check_of(&o) == VSR_EINVAL);
    o = base;
    o.sync_delay_ns = VSR_NO_DEADLINE;
    CHECK(check_of(&o) == VSR_EINVAL);
    o = base;
    o.flush_interval_ns = VSR_NO_DEADLINE;
    CHECK(check_of(&o) == VSR_EINVAL);
    o = base;
    o.inflight_writes = 0;
    CHECK(check_of(&o) == VSR_EINVAL);
    o = base;
    o.max_clients = 0;
    CHECK(check_of(&o) == VSR_EINVAL);
    /* Capacities. */
    o = base;
    o.segments = 1;
    CHECK(check_of(&o) == VSR_ELIMIT);
    o = base;
    o.max_segments = o.segments - 1;
    CHECK(check_of(&o) == VSR_ELIMIT);
    o = base;
    o.max_entries = limits.batch_entries - 1;
    CHECK(check_of(&o) == VSR_ELIMIT);
    o.max_entries = limits.batch_entries;
    CHECK(check_of(&o) == VSR_OK);
    /* A record fits a slab less two blocks. */
    CHECK(vsr_io_store_check(&base, &limits,
                             (uint32_t)(max_record + 2 * BLOCK)) == VSR_OK);
    CHECK(vsr_io_store_check(&base, &limits,
                             (uint32_t)(max_record + 2 * BLOCK - 1)) ==
          VSR_ELIMIT);
    /* A record fits a segment after its header. */
    o = base;
    o.segment_bytes = round_up(header_bytes + max_record, BLOCK);
    CHECK(check_of(&o) == VSR_OK);
    o.segment_bytes = round_up(header_bytes + max_record, BLOCK) - BLOCK;
    CHECK(check_of(&o) == VSR_ELIMIT);
    /* The ring rule of decision 69, exactly. */
    o = base;
    o.cache_bytes =
        round_up(o.write_behind_bytes + limits.pinned_payload_bytes +
                     2 * max_record + 2 * header_bytes + BLOCK,
                 BLOCK);
    CHECK(check_of(&o) == VSR_OK);
    o.cache_bytes -= BLOCK;
    CHECK(check_of(&o) == VSR_ELIMIT);
    o = base;
    o.write_behind_bytes += BLOCK;
    CHECK(check_of(&o) == VSR_ELIMIT);
    /* Executor lengths (decision 70). */
    o = base;
    o.segment_bytes = round_up(UINT32_MAX - 2 * BLOCK + 1, BLOCK);
    o.cache_bytes = UINT64_C(1) << 40;
    CHECK(check_of(&o) == VSR_ELIMIT);
    o = base;
    o.write_behind_bytes = UINT64_C(1) << 30;
    o.cache_bytes = UINT64_C(1) << 40;
    CHECK(check_of(&o) == VSR_ELIMIT);
    /* Sizes. */
    CHECK(vsr_io_store_size(&base, &limits, REGIONS, &bytes, &alignment,
                            &tail_bytes, &tail_alignment) == VSR_OK);
    CHECK(bytes > 0 && alignment >= 8 && tail_bytes == base.cache_bytes + 1024);
    CHECK(tail_alignment == PAGE);
    CHECK(vsr_io_store_size(NULL, &limits, REGIONS, &bytes, &alignment,
                            &tail_bytes, &tail_alignment) == VSR_EINVAL);
    o = base;
    o.segment_bytes = 8;
    CHECK(vsr_io_store_size(&o, &limits, REGIONS, &bytes, &alignment,
                            &tail_bytes, &tail_alignment) == VSR_EINVAL);
    huge = limits;
    huge.manifest_bytes = UINT64_MAX; /* The state copies overflow. */
    CHECK(vsr_io_store_size(&base, &huge, REGIONS, &bytes, &alignment,
                            &tail_bytes, &tail_alignment) == VSR_ELIMIT);
}

/* Creation: the probe, the create, one FALLOCATE per slot, both
 * superblocks, the first header; NOT_FOUND once the header is on disk. */
static void test_create(void)
{
    struct config c = base_config();
    struct vsr_store_read read;
    struct vsr_io_wire_superblock a;
    struct vsr_io_wire_superblock b;
    struct vsr_io_wire_segment header;
    struct vsr_io_store_status status;
    uint64_t op;
    uint32_t at;

    harness_open(&c);
    memset(&read, 0, sizeof(read));
    read.type = VSR_LOAD_RECOVERY;
    op = next_op();
    vsr_io_store_open(h.store, VSR_START_NEW, op, &read);
    /* The probe. */
    CHECK(harness_prepare() == 1);
    at = pending_of(VSR_IO_SQE_OPENAT);
    CHECK(at != NONE && (h.pending[at].sqe.op_flags & O_CREAT) == 0);
    CHECK(h.store->file_slot == FILE_SLOT_BASE);
    CHECK(harness_prepare() == 0); /* One file operation at a time. */
    harness_complete(at);
    CHECK(h.store->state == VSR_IO_STORE_CREATING);
    expect_no_completion();
    /* The create. */
    CHECK(harness_prepare() == 1);
    at = pending_of(VSR_IO_SQE_OPENAT);
    CHECK(at != NONE && (h.pending[at].sqe.op_flags & O_CREAT) != 0);
    harness_complete(at);
    CHECK(h.store->log_slot == (int32_t)FILE_SLOT_BASE && disk.exists);
    /* One FALLOCATE per slot, sequentially. */
    for (uint32_t i = 0; i < c.segments; ++i) {
        CHECK(harness_prepare() == 1);
        at = pending_of(VSR_IO_SQE_FALLOCATE);
        CHECK(at != NONE);
        CHECK(h.pending[at].sqe.length ==
              c.segment_bytes + (i == 0 ? 2 * BLOCK : 0));
        harness_complete(at);
    }
    CHECK(disk.size == 2 * BLOCK + c.segments * c.segment_bytes);
    CHECK(h.store->file_size == disk.size);
    /* Both superblocks in one write. */
    CHECK(harness_prepare() == 1);
    at = pending_of(VSR_IO_SQE_WRITE);
    CHECK(at != NONE && h.pending[at].sqe.offset == 0 &&
          h.pending[at].sqe.length == 2 * BLOCK);
    CHECK(harness_prepare() == 0);
    harness_complete(at);
    read_superblock(0, &a);
    read_superblock(1, &b);
    CHECK(memcmp(&a, &b, sizeof(a)) == 0);
    CHECK(a.generation != 0 && a.generation == h.store->generation);
    CHECK(a.revision == 1 && a.run == 1 && a.slots == c.segments);
    CHECK(a.start_segment == 1 && a.start_slot == 0 && a.durable_floor == 0);
    CHECK(a.block_bytes == BLOCK && a.segment_bytes == c.segment_bytes);
    CHECK(a.header_blocks == h.header_bytes / BLOCK);
    CHECK(a.cluster_hi == 0 && a.cluster_lo == 0 && a.replica == 0);
    CHECK(h.store->slots == c.segments);
    /* The first segment's header, from the ring. */
    expect_no_completion();
    CHECK(harness_prepare() == 1);
    at = pending_of(VSR_IO_SQE_WRITE);
    CHECK(at != NONE && h.pending[at].sqe.offset == 2 * BLOCK &&
          h.pending[at].sqe.length == h.header_bytes);
    CHECK(h.pending[at].sqe.addr == h.store->ring);
    CHECK(h.store->current == 0 &&
          h.store->segments[0].phase == VSR_IO_SEGMENT_HEADER);
    CHECK(h.store->segments[0].number == 1 && h.store->next_segment == 2);
    CHECK(h.store->head == h.header_bytes && h.store->issued == h.header_bytes);
    CHECK(h.store->file_head == 2 * BLOCK + h.header_bytes);
    CHECK(h.store->extents_count == 1 && h.store->extents[0].header == 1);
    CHECK(h.store->state == VSR_IO_STORE_CREATING);
    expect_no_completion();
    harness_complete(at);
    CHECK(h.store->state == VSR_IO_STORE_READY);
    CHECK(h.store->segments[0].phase == VSR_IO_SEGMENT_OPEN);
    CHECK(h.store->segments[0].header_write == NONE);
    expect_completion(op, VSR_IO_NOT_FOUND);
    expect_no_completion();
    read_header(0, &header);
    CHECK(header.segment == 1 && header.run == 1 && header.flags == 0);
    CHECK(header.generation == a.generation && header.last_sequence == 0);
    CHECK(header.durable_floor == 0 && header.log_begin == 0);
    CHECK(harness_prepare() == 0);
    vsr_io_store_status(h.store, &status);
    CHECK(status.readable == 0 && status.written == 0 && status.durable == 0);
    CHECK(status.unwritten_bytes == 0 && status.live_segments == 1);
    CHECK(status.capacity_bytes == c.segments * c.segment_bytes);
    CHECK(status.used_bytes == h.header_bytes && status.error == 0);
    CHECK(vsr_io_store_close(h.store) == VSR_OK);
    CHECK(h.store->state == VSR_IO_STORE_CLOSED && h.store->log_slot == -1);
    harness_close();
}

/* RECOVER of a missing log: NOT_FOUND at once, the log created behind it,
 * and a STORE issued meanwhile held until the first header is on disk. */
static void test_recover_missing(void)
{
    struct config c = base_config();
    struct vsr_store_read read;
    uint64_t op;
    uint64_t store_op;
    uint32_t at;

    harness_open(&c);
    memset(&read, 0, sizeof(read));
    read.type = VSR_LOAD_RECOVERY;
    op = next_op();
    vsr_io_store_open(h.store, VSR_START_RECOVER, op, &read);
    CHECK(harness_prepare() == 1);
    harness_complete(pending_of(VSR_IO_SQE_OPENAT));
    expect_completion(op, VSR_IO_NOT_FOUND);
    CHECK(h.store->state == VSR_IO_STORE_CREATING);
    store_op = submit(txn_append(1, 1, 16));
    expect_no_completion();
    /* Everything up to the header write. */
    for (;;) {
        CHECK(harness_prepare() == 1);
        at = pending_first();
        if (h.pending[at].sqe.opcode == VSR_IO_SQE_WRITE &&
            h.pending[at].sqe.offset == 2 * BLOCK) {
            break;
        }
        harness_complete(at);
        expect_no_completion();
    }
    CHECK(h.store->stores_count == 1);
    harness_complete(at);
    CHECK(h.store->state == VSR_IO_STORE_READY);
    expect_completion(store_op, VSR_IO_OK);
    expect_no_completion();
    CHECK(h.store->readable == 1 && h.store->stores_count == 0);
    harness_close();
}

/* An existing log routes to recovery, which phase 3 delivers; until then
 * the open fails, and NEW over stray state is refused either way. */
static void test_open_existing(void)
{
    struct config c = base_config();
    struct vsr_store_read read;
    uint64_t op;

    harness_open(&c);
    disk.exists = true;
    disk.size = 2 * BLOCK;
    memset(&read, 0, sizeof(read));
    read.type = VSR_LOAD_RECOVERY;
    op = next_op();
    vsr_io_store_open(h.store, VSR_START_NEW, op, &read);
    CHECK(harness_prepare() == 1);
    harness_complete(pending_of(VSR_IO_SQE_OPENAT));
    CHECK(h.store->state == VSR_IO_STORE_FAILED);
    CHECK(h.store->log_slot == (int32_t)FILE_SLOT_BASE);
    expect_completion(op, VSR_IO_FAILED);
    expect_no_completion();
    /* Every later op fails at once. */
    CHECK(vsr_io_store_store(h.store, 77, &txn_append(1, 1, 8)->store) ==
          VSR_OK);
    expect_completion(77, VSR_IO_FAILED);
    CHECK(vsr_io_store_sync(h.store, 78, 1) == VSR_OK);
    expect_completion(78, VSR_IO_FAILED);
    CHECK(harness_prepare() == 0);
    harness_close();
}

/* A STORE completes when packed; the write covers its block, padded; the
 * disk mirrors the ring; the next record goes to the next block. */
static void test_store_packs(void)
{
    struct config c = base_config();
    struct vsr_io_store_status status;
    const struct txn *t1;
    const struct txn *t2;
    uint64_t op;
    uint64_t data;
    uint32_t at;

    harness_open(&c);
    harness_start(VSR_START_NEW);
    data = 2 * BLOCK + h.header_bytes;
    t1 = txn_append(1, 2, 40);
    op = submit(t1);
    expect_completion(op, VSR_IO_OK);
    CHECK(h.store->readable == 1 && h.store->written == 0);
    check_ring_record(1, 0);
    CHECK(h.store->head == h.header_bytes + t1->bytes);
    CHECK(h.store->file_head == data + t1->bytes);
    vsr_io_store_status(h.store, &status);
    CHECK(status.unwritten_bytes == t1->bytes);
    /* Malformed STOREs are the caller's bug. */
    CHECK(vsr_io_store_store(h.store, 0, &t1->store) == VSR_EINVAL);
    CHECK(vsr_io_store_store(h.store, 99, NULL) == VSR_EINVAL);
    CHECK(vsr_io_store_store(h.store, 99, &t1->store) == VSR_EINVAL);
    CHECK(vsr_io_store_store(h.store, 99, &txn_append(3, 1, 8)->store) ==
          VSR_EINVAL);
    expect_no_completion();
    /* The write: one padded block, straight from the ring. */
    CHECK(harness_prepare() == 1);
    at = pending_of(VSR_IO_SQE_WRITE);
    CHECK(at != NONE);
    CHECK(h.pending[at].sqe.offset == data);
    CHECK(h.pending[at].sqe.length == round_up(t1->bytes, BLOCK));
    CHECK(h.pending[at].sqe.addr == h.store->ring + h.header_bytes);
    CHECK(h.store->issued == round_up(h.header_bytes + t1->bytes, BLOCK));
    CHECK(h.store->head == h.store->issued); /* Padded. */
    CHECK(h.store->writes_count == 1);
    CHECK(harness_prepare() == 0); /* One record write per prepare. */
    /* A record packed meanwhile goes after the pad. */
    t2 = txn_append(2, 1, 100);
    op = submit(t2);
    expect_completion(op, VSR_IO_OK);
    CHECK(h.store->head == h.store->issued + t2->bytes);
    CHECK(h.store->written == 0);
    harness_complete(at);
    CHECK(h.store->written == 1 && h.store->writes_count == 0);
    CHECK(memcmp(disk_image + data, h.store->ring + h.header_bytes,
                 round_up(t1->bytes, BLOCK)) == 0);
    CHECK(scan_records(data, data + round_up(t1->bytes, BLOCK), 1) == 1);
    CHECK(harness_prepare() == 1);
    at = pending_of(VSR_IO_SQE_WRITE);
    CHECK(h.pending[at].sqe.offset == data + round_up(t1->bytes, BLOCK));
    harness_complete(at);
    CHECK(h.store->written == 2);
    CHECK(scan_records(data, h.store->file_head, 1) == 2);
    CHECK(disk.writes == 4); /* Superblocks, header, two records. */
    vsr_io_store_status(h.store, &status);
    CHECK(status.readable == 2 && status.written == 2 && status.durable == 0);
    CHECK(status.unwritten_bytes == 0);
    harness_close();
}

/* Backpressure: STOREs are held once the unwritten bytes exceed
 * write_behind_bytes and proceed as writes complete, in order. */
static void test_write_behind(void)
{
    struct config c = base_config();
    uint64_t ops[16] = {0};
    uint64_t sequence = 1;
    uint32_t at;

    c.write_behind_bytes = 2 * BLOCK;
    harness_open(&c);
    harness_start(VSR_START_NEW);
    /* Pack without writing until a STORE is held. */
    while (h.store->stores_count == 0) {
        const struct txn *t = txn_append(sequence, 4, 100);

        CHECK(t->bytes > BLOCK && t->bytes < 2 * BLOCK);
        ops[sequence] = submit(t);
        if (h.store->stores_count == 0) {
            expect_completion(ops[sequence], VSR_IO_OK);
        }
        sequence++;
        CHECK(sequence < 8);
    }
    expect_no_completion();
    CHECK(h.store->head - h.header_bytes > c.write_behind_bytes);
    /* More STOREs queue behind it, in order. */
    ops[sequence] = submit(txn_trim(sequence));
    sequence++;
    CHECK(h.store->stores_count == 2);
    expect_no_completion();
    /* SYNC of a held sequence is accepted; beyond the newest is a bug. */
    CHECK(vsr_io_store_sync(h.store, 500, sequence) == VSR_EINVAL);
    CHECK(vsr_io_store_sync(h.store, 501, 0) == VSR_EINVAL);
    /* The write of everything packed lets the held STOREs proceed once it
     * completes and poll runs; not before. */
    CHECK(harness_prepare() == 1);
    at = pending_of(VSR_IO_SQE_WRITE);
    harness_poll();
    expect_no_completion();
    CHECK(h.store->stores_count == 2);
    harness_complete(at);
    CHECK(h.store->stores_count == 0);
    expect_completion(ops[sequence - 2], VSR_IO_OK);
    expect_completion(ops[sequence - 1], VSR_IO_OK);
    expect_no_completion();
    CHECK(h.store->readable == sequence - 1);
    harness_run();
    CHECK(h.store->written == sequence - 1);
    CHECK(scan_records(2 * BLOCK + h.header_bytes, h.store->file_head, 1) ==
          sequence - 1);
    harness_close();
}

/* SYNC in FDATASYNC mode: the flush waits for every earlier write, writes
 * completing out of order do not advance `written`, and the SYNC
 * completes only after the flush. */
static void test_sync_fdatasync(void)
{
    struct config c = base_config();
    uint64_t sync_op;
    uint64_t early;
    uint32_t first;
    uint32_t second;
    uint32_t flush;

    c.write_behind_bytes = 8 * BLOCK;
    c.cache_bytes += 4 * BLOCK;
    harness_open(&c);
    harness_start(VSR_START_NEW);
    expect_completion(submit(txn_append(1, 1, 50)), VSR_IO_OK);
    CHECK(harness_prepare() == 1);
    first = pending_of(VSR_IO_SQE_WRITE);
    expect_completion(submit(txn_append(2, 1, 60)), VSR_IO_OK);
    CHECK(harness_prepare() == 1);
    second = pending_last_of(VSR_IO_SQE_WRITE);
    CHECK(second != first && h.store->writes_count == 2);
    CHECK(harness_prepare() == 0); /* inflight_writes reached. */
    sync_op = submit_sync(2);
    expect_no_completion();
    CHECK(h.store->flush_target == 2);
    harness_poll();
    CHECK(h.store->flush_pending == 1);
    CHECK(harness_prepare() == 0); /* Nothing written yet. */
    /* The second write completes first: nothing advances. */
    harness_complete(second);
    CHECK(h.store->written == 0 && h.store->writes_count == 2);
    CHECK(harness_prepare() == 0);
    expect_no_completion();
    harness_complete(first);
    CHECK(h.store->written == 2 && h.store->writes_count == 0);
    expect_no_completion();
    /* Now the flush, then the SYNC. */
    CHECK(harness_prepare() == 1);
    flush = pending_of(VSR_IO_SQE_FSYNC);
    CHECK(flush != NONE && h.store->flush_slot != NONE);
    CHECK(harness_prepare() == 0);
    /* A SYNC arriving while the flush is out waits for the next one. */
    early = submit_sync(1);
    expect_no_completion();
    harness_complete(flush);
    CHECK(h.store->flushed == 2 && h.store->durable == 2);
    expect_completion(sync_op, VSR_IO_OK);
    expect_completion(early, VSR_IO_OK);
    expect_no_completion();
    CHECK(disk.flushes == 1);
    harness_poll();
    CHECK(harness_prepare() == 0); /* Nothing to flush again. */
    /* A SYNC of a durable sequence completes at once. */
    expect_completion(submit_sync(2), VSR_IO_OK);
    expect_completion(submit_sync(1), VSR_IO_OK);
    /* The next record carries the acknowledged floor. */
    expect_completion(submit(txn_append(3, 1, 8)), VSR_IO_OK);
    check_ring_record(3, 2);
    harness_run();
    CHECK(disk.flushes == 1 && h.store->written == 3);
    harness_close();
}

/* DSYNC mode: no flush record; SYNC completes when written reaches it. */
static void test_sync_dsync(void)
{
    struct config c = base_config();
    uint64_t sync_op;
    uint32_t at;

    c.sync_mode = VSR_IO_SYNC_DSYNC;
    harness_open(&c);
    harness_start(VSR_START_NEW);
    CHECK((disk.flags & O_DSYNC) != 0);
    expect_completion(submit(txn_append(1, 1, 50)), VSR_IO_OK);
    sync_op = submit_sync(1);
    expect_no_completion();
    harness_poll();
    CHECK(harness_prepare() == 1);
    at = pending_of(VSR_IO_SQE_WRITE);
    CHECK(at != NONE);
    harness_complete(at);
    expect_completion(sync_op, VSR_IO_OK);
    CHECK(h.store->durable == 1 && h.store->written == 1);
    harness_poll();
    CHECK(harness_prepare() == 0);
    CHECK(disk.flushes == 0);
    harness_close();
}

/* sync_delay_ns holds the flush on the SYNC deadline handle. */
static void test_sync_delay(void)
{
    struct config c = base_config();
    uint64_t sync_op;

    c.sync_delay_ns = 5000;
    harness_open(&c);
    harness_start(VSR_START_NEW);
    expect_completion(submit(txn_append(1, 1, 50)), VSR_IO_OK);
    harness_run();
    CHECK(h.store->written == 1);
    sync_op = submit_sync(1);
    CHECK(h.store->flush_pending == 2);
    harness_poll();
    CHECK(h.store->flush_pending == 0);
    CHECK(h.store->flush_deadline == h.now + 5000);
    CHECK(vsr_io_store_deadline(h.store) == h.now + 5000);
    CHECK(vsr_io_deadlines_earliest(&h.io->deadlines) == h.now + 5000);
    CHECK(harness_prepare() == 0);
    h.now += 4999;
    harness_poll();
    CHECK(harness_prepare() == 0);
    /* A second SYNC joins the batch. */
    CHECK(vsr_io_store_sync(h.store, 900, 1) == VSR_OK);
    CHECK(h.store->flush_deadline == h.now + 1);
    h.now += 1;
    harness_poll();
    CHECK(h.store->flush_deadline == VSR_NO_DEADLINE);
    CHECK(vsr_io_deadlines_earliest(&h.io->deadlines) == VSR_NO_DEADLINE);
    CHECK(harness_prepare() == 1);
    harness_complete(pending_of(VSR_IO_SQE_FSYNC));
    expect_completion(sync_op, VSR_IO_OK);
    expect_completion(900, VSR_IO_OK);
    CHECK(disk.flushes == 1);
    harness_close();
}

/* Segments: a record that does not fit seals the segment (its last block
 * padded), the next slot's header is packed at a block boundary and
 * written on its own, and the records after it wait for that write. */
static void test_segments(void)
{
    struct config c = base_config();
    struct vsr_io_wire_segment header;
    uint64_t sequence = 1;
    uint64_t sealed_at;
    uint32_t at;
    uint32_t record_write;

    c.segments = 3;
    c.write_behind_bytes = c.segment_bytes;
    c.cache_bytes += c.segment_bytes;
    harness_open(&c);
    harness_start(VSR_START_NEW);
    /* Fill slot 0 up to its seal. */
    while (h.store->current == 0) {
        expect_completion(submit(txn_append(sequence, 4, 128)), VSR_IO_OK);
        sequence++;
        CHECK(sequence < 64);
    }
    sealed_at = sequence - 2;
    CHECK(h.store->current == 1 && h.store->readable == sequence - 1);
    CHECK(h.store->segments[0].phase == VSR_IO_SEGMENT_SEALED);
    CHECK(h.store->segments[0].last_sequence == sealed_at);
    CHECK(h.store->segments[0].used <= c.segment_bytes);
    CHECK(h.store->segments[0].used % BLOCK == 0); /* Padded. */
    CHECK(h.store->segments[1].phase == VSR_IO_SEGMENT_HEADER);
    CHECK(h.store->segments[1].number == 2);
    CHECK(h.store->segments[1].first_sequence == sequence - 1);
    CHECK(h.store->extents_count == 2);
    CHECK(h.store->extents[1].header == 1 && h.store->extents[1].segment == 1);
    CHECK(h.store->extents[1].file_offset == slot_offset(1));
    CHECK(h.store->extents[1].ring_offset % BLOCK == 0);
    /* Writes in order: the sealed segment's records, then the header,
     * then the new segment's records only after the header completed. */
    harness_run();
    CHECK(h.store->written == sequence - 1);
    read_header(1, &header);
    CHECK(header.segment == 2 && header.last_sequence == sealed_at);
    CHECK(header.run == 1 && header.flags == 0);
    CHECK(scan_records(slot_offset(0) + h.header_bytes, slot_offset(1), 1) ==
          sealed_at);
    CHECK(scan_records(slot_offset(1) + h.header_bytes, h.store->file_head,
                       sealed_at + 1) == sequence - 1);
    CHECK(h.store->segments[1].phase == VSR_IO_SEGMENT_OPEN);
    /* The ordering rule, observed step by step on the next seal. */
    while (h.store->current == 1) {
        expect_completion(submit(txn_append(sequence, 4, 128)), VSR_IO_OK);
        sequence++;
        CHECK(sequence < 128);
    }
    CHECK(h.store->current == 2);
    /* The sealed segment's records first (in two writes when the ring
     * wrapped inside it), then the header. */
    CHECK(harness_prepare() == 1);
    at = pending_of(VSR_IO_SQE_WRITE);
    while (h.pending[at].sqe.offset < slot_offset(2)) {
        harness_complete(at);
        CHECK(harness_prepare() == 1);
        at = pending_of(VSR_IO_SQE_WRITE);
    }
    CHECK(h.pending[at].sqe.offset == slot_offset(2));
    CHECK(h.pending[at].sqe.length == h.header_bytes);
    CHECK(h.store->segments[2].header_write != NONE);
    CHECK(harness_prepare() == 0); /* Records wait for the header. */
    expect_completion(submit(txn_append(sequence, 1, 8)), VSR_IO_OK);
    sequence++;
    CHECK(harness_prepare() == 0);
    harness_complete(at);
    CHECK(h.store->segments[2].phase == VSR_IO_SEGMENT_OPEN);
    CHECK(harness_prepare() == 1);
    record_write = pending_of(VSR_IO_SQE_WRITE);
    CHECK(h.pending[record_write].sqe.offset ==
          slot_offset(2) + h.header_bytes);
    harness_complete(record_write);
    harness_run();
    CHECK(h.store->written == sequence - 1);
    harness_close();
}

/* Growth: the third segment needs a FALLOCATE and a superblock naming the
 * new slot before its header; the STORE waits; a fourth cannot be had. */
static void test_growth(void)
{
    struct config c = base_config();
    struct vsr_io_wire_superblock superblock;
    uint64_t sequence = 1;
    uint64_t held = 0;
    uint32_t at;

    c.segments = 2;
    c.max_segments = 3;
    c.write_behind_bytes = c.segment_bytes;
    c.cache_bytes += c.segment_bytes;
    harness_open(&c);
    harness_start(VSR_START_NEW);
    while (h.store->stores_count == 0) {
        uint64_t op = submit(txn_append(sequence, 4, 128));

        if (h.store->stores_count == 0) {
            expect_completion(op, VSR_IO_OK);
            harness_run();
        } else {
            held = op;
        }
        sequence++;
        CHECK(sequence < 128);
    }
    CHECK(h.store->growth != 0 && h.store->current == 1);
    CHECK(h.store->segments[1].phase == VSR_IO_SEGMENT_OPEN);
    expect_no_completion();
    CHECK(harness_prepare() == 1);
    at = pending_of(VSR_IO_SQE_FALLOCATE);
    CHECK(at != NONE && h.pending[at].sqe.offset == slot_offset(2));
    CHECK(h.pending[at].sqe.length == c.segment_bytes);
    CHECK(harness_prepare() == 0);
    harness_poll();
    expect_no_completion();
    harness_complete(at);
    CHECK(h.store->slots == 2); /* Named by the superblock first. */
    CHECK(harness_prepare() == 1);
    at = pending_of(VSR_IO_SQE_WRITE);
    CHECK(at != NONE && h.pending[at].sqe.offset == 0 &&
          h.pending[at].sqe.length == BLOCK);
    harness_poll();
    expect_no_completion();
    harness_complete(at);
    CHECK(h.store->slots == 3);
    read_superblock(0, &superblock);
    CHECK(superblock.revision == 2 && superblock.slots == 3);
    CHECK(superblock.start_segment == 1 && superblock.start_slot == 0);
    read_superblock(1, &superblock);
    CHECK(superblock.revision == 1);
    harness_poll();
    expect_completion(held, VSR_IO_OK);
    CHECK(h.store->current == 2 && h.store->segments[2].number == 3);
    harness_run();
    CHECK(disk.size == 2 * BLOCK + 3 * c.segment_bytes);
    CHECK(h.store->written == sequence - 1);
    /* No fourth slot, and no RECLAIM ever: every floor keeps every slot,
     * so the STORE that needs one fails with FAILED and the store fences
     * (section 6.1). */
    while (h.store->state == VSR_IO_STORE_READY) {
        uint64_t op = submit(txn_append(sequence, 4, 128));

        if (h.store->state == VSR_IO_STORE_READY) {
            expect_completion(op, VSR_IO_OK);
            harness_run();
        } else {
            expect_completion(op, VSR_IO_FAILED);
        }
        sequence++;
        CHECK(sequence < 128);
    }
    CHECK(h.store->growth == 0 && h.store->stores_count == 0);
    expect_no_completion();
    harness_close();
}

/* The wrap: records never straddle the ring end, the skipped bytes are
 * not mirrored, the file stays contiguous, and hot() answers only for
 * bytes still in the ring. */
static void test_wrap(void)
{
    struct config c = base_config();
    struct vsr_io_piece piece;
    uint64_t sequence = 1;
    uint64_t first_offset;
    uint64_t first_bytes;
    uint64_t header_bytes;
    uint64_t max_record;
    uint32_t wraps = 0;

    derive(&header_bytes, &max_record);
    c.segment_bytes = round_up(header_bytes + 64 * max_record, BLOCK);
    harness_open(&c);
    harness_start(VSR_START_NEW);
    first_offset = h.store->file_head;
    first_bytes = txn_append(1, 3, 100)->bytes;
    expect_completion(submit(&txns[1]), VSR_IO_OK);
    CHECK(
        vsr_io_store_hot(h.store, first_offset, (uint32_t)first_bytes, &piece));
    CHECK(piece.base == h.store->ring + h.header_bytes &&
          piece.length == first_bytes);
    CHECK(!vsr_io_store_hot(h.store, first_offset + 8, (uint32_t)first_bytes,
                            &piece));
    sequence = 2;
    while (wraps < 3) {
        uint64_t before = h.store->head;
        uint64_t file_before = h.store->file_head;
        const struct txn *t = txn_append(sequence, 1 + (uint32_t)(sequence % 4),
                                         (size_t)(sequence * 37 % 200));

        expect_completion(submit(t), VSR_IO_OK);
        if (h.store->head / c.cache_bytes != before / c.cache_bytes) {
            /* A wrap: the record starts at the ring start, the file
             * continued at the next block. */
            wraps++;
            CHECK((h.store->head - t->bytes) % c.cache_bytes == 0);
            CHECK(h.store->file_head - t->bytes ==
                  round_up(file_before, BLOCK));
            CHECK(h.store->file_head - t->bytes ==
                  newest_extent()->file_offset);
        } else {
            CHECK(h.store->file_head == file_before + t->bytes);
        }
        sequence++;
        harness_run();
        CHECK(h.store->written == sequence - 1);
        CHECK(h.store->extents_count <= h.store->extents_capacity);
    }
    CHECK(h.store->head > 3 * c.cache_bytes && h.store->current == 0);
    CHECK(!vsr_io_store_hot(h.store, first_offset, (uint32_t)first_bytes,
                            &piece));
    CHECK(h.store->retained_floor == h.store->head - c.cache_bytes);
    CHECK(vsr_io_store_hot(h.store,
                           h.store->file_head - txns[sequence - 1].bytes,
                           (uint32_t)txns[sequence - 1].bytes, &piece));
    CHECK(scan_records(first_offset, h.store->file_head, 1) == sequence - 1);
    harness_close();
}

/* A LOAD lease pins ring bytes: the STORE that would overwrite them is
 * held until the release; unwritten bytes hold the same way. */
static void test_pins(void)
{
    struct config c = base_config();
    struct vsr_io_piece piece;
    uint64_t sequence = 1;
    uint64_t pinned_offset;
    uint64_t pinned_bytes;
    uint64_t header_bytes;
    uint64_t max_record;
    uint64_t held = 0;
    uint32_t pin;

    derive(&header_bytes, &max_record);
    c.segment_bytes = round_up(header_bytes + 64 * max_record, BLOCK);
    harness_open(&c);
    harness_start(VSR_START_NEW);
    /* Pin the first record of the second block. */
    expect_completion(submit(txn_append(1, 4, 200)), VSR_IO_OK);
    harness_run();
    pinned_offset = h.store->file_head;
    pinned_bytes = txn_append(2, 2, 100)->bytes;
    expect_completion(submit(&txns[2]), VSR_IO_OK);
    CHECK(vsr_io_store_hot(h.store, pinned_offset, (uint32_t)pinned_bytes,
                           &piece));
    pin = vsr_io_store_pin(h.store, pinned_offset, (uint32_t)pinned_bytes, 3);
    CHECK(pin == 3 && h.store->pins[3].begin == h.store->head - pinned_bytes);
    CHECK(vsr_io_store_pin(h.store, 0, 8, 2) == NONE); /* Not in the ring. */
    CHECK(vsr_io_store_pin(h.store, pinned_offset, 8, REGIONS) == NONE);
    sequence = 3;
    while (h.store->stores_count == 0) {
        uint64_t op = submit(txn_append(sequence, 4, 200));

        if (h.store->stores_count == 0) {
            expect_completion(op, VSR_IO_OK);
            harness_run();
        } else {
            held = op;
        }
        sequence++;
        CHECK(sequence < 256);
    }
    /* Everything is written, so only the pin holds. */
    CHECK(h.store->writes_count == 0 && h.store->issued == h.store->head);
    CHECK(h.store->head + txns[sequence - 1].bytes >
          h.store->pins[3].begin + c.cache_bytes);
    CHECK(vsr_io_store_hot(h.store, pinned_offset, (uint32_t)pinned_bytes,
                           &piece));
    harness_run();
    expect_no_completion();
    vsr_io_store_release(h.store, 3);
    CHECK(h.store->pins[3].begin == UINT64_MAX);
    harness_poll();
    expect_completion(held, VSR_IO_OK);
    CHECK(!vsr_io_store_hot(h.store, pinned_offset, (uint32_t)pinned_bytes,
                            &piece));
    harness_run();
    CHECK(scan_records(2 * BLOCK + h.header_bytes, h.store->file_head, 1) ==
          sequence - 1);
    harness_close();
}

/* The idle superblock write (decision 50): after a SYNC with no record
 * packed within the flush interval, the floor goes to the superblock; a
 * record packed meanwhile skips it. */
static void test_idle_superblock(void)
{
    struct config c = base_config();
    struct vsr_io_wire_superblock superblock;
    uint64_t sync_op;
    uint32_t at;

    c.flush_interval_ns = 0; /* 100 ms. */
    harness_open(&c);
    harness_start(VSR_START_NEW);
    expect_completion(submit(txn_append(1, 1, 50)), VSR_IO_OK);
    expect_completion(submit(txn_append(2, 1, 50)), VSR_IO_OK);
    sync_op = submit_sync(2);
    harness_run();
    expect_completion(sync_op, VSR_IO_OK);
    CHECK(h.store->durable == 2 && h.store->superblock_floor == 0);
    CHECK(h.store->idle_deadline == h.now + UINT64_C(100000000));
    CHECK(vsr_io_deadlines_earliest(&h.io->deadlines) ==
          h.store->idle_deadline);
    h.now += UINT64_C(99999999);
    harness_poll();
    CHECK(harness_prepare() == 0);
    /* A record packed meanwhile: the write is skipped. */
    expect_completion(submit(txn_append(3, 1, 50)), VSR_IO_OK);
    check_ring_record(3, 2);
    h.now += 1;
    harness_poll();
    CHECK(h.store->idle_deadline == VSR_NO_DEADLINE);
    CHECK(h.store->superblock_dirty == 0);
    harness_run();
    CHECK(h.store->superblock_floor == 0 && h.store->superblock_revision == 1);
    /* The next SYNC re-arms it; quiet this time. */
    sync_op = submit_sync(3);
    harness_run();
    expect_completion(sync_op, VSR_IO_OK);
    CHECK(h.store->durable == 3);
    CHECK(h.store->idle_deadline == h.now + UINT64_C(100000000));
    h.now += UINT64_C(100000000);
    harness_poll();
    CHECK(h.store->superblock_dirty == 1);
    CHECK(harness_prepare() == 1);
    at = pending_of(VSR_IO_SQE_WRITE);
    CHECK(at != NONE && h.pending[at].sqe.offset == 0);
    harness_complete(at);
    CHECK(h.store->superblock_floor == 3 && h.store->superblock_revision == 2);
    read_superblock(0, &superblock);
    CHECK(superblock.durable_floor == 3 && superblock.revision == 2);
    harness_poll();
    CHECK(h.store->idle_deadline == VSR_NO_DEADLINE);
    CHECK(harness_prepare() == 0);
    /* Alternating copies: the next rewrite goes to copy B. */
    sync_op = submit_sync(3);
    expect_completion(sync_op, VSR_IO_OK);
    expect_completion(submit(txn_append(4, 1, 50)), VSR_IO_OK);
    sync_op = submit_sync(4);
    harness_run();
    expect_completion(sync_op, VSR_IO_OK);
    h.now += UINT64_C(100000000);
    harness_run();
    read_superblock(1, &superblock);
    CHECK(superblock.durable_floor == 4 && superblock.revision == 3);
    harness_close();
}

/* Replicated mode: no SYNC ever; the log is flushed on the interval. */
static void test_replicated_flush(void)
{
    struct config c = base_config();

    c.durability = VSR_REPLICATED;
    c.flush_interval_ns = 7000;
    harness_open(&c);
    harness_start(VSR_START_NEW);
    expect_completion(submit(txn_append(1, 1, 50)), VSR_IO_OK);
    harness_run();
    CHECK(h.store->written == 1 && h.store->flushed == 0);
    CHECK(h.store->flush_deadline == h.now + 7000);
    CHECK(vsr_io_deadlines_earliest(&h.io->deadlines) == h.now + 7000);
    h.now += 7000;
    harness_run();
    CHECK(disk.flushes == 1 && h.store->flushed == 1);
    CHECK(h.store->durable == 0); /* Nothing acknowledged. */
    CHECK(h.store->flush_deadline == VSR_NO_DEADLINE);
    CHECK(h.store->idle_deadline == VSR_NO_DEADLINE);
    h.now += 7000;
    harness_run();
    CHECK(disk.flushes == 1); /* Nothing new. */
    harness_close();
}

/* A failed write fences: queued ops and later ones complete FAILED; with
 * CONTINUE the store keeps serving from memory and stops writing. */
static void test_write_error(void)
{
    struct config c = base_config();
    struct vsr_io_store_status status;
    uint64_t sync_op;
    uint64_t held = 0;
    uint64_t sequence = 1;
    uint32_t at;

    c.write_behind_bytes = 2 * BLOCK;
    harness_open(&c);
    harness_start(VSR_START_NEW);
    while (h.store->stores_count == 0) {
        uint64_t op = submit(txn_append(sequence, 4, 200));

        if (h.store->stores_count == 0) {
            expect_completion(op, VSR_IO_OK);
        } else {
            held = op;
        }
        sequence++;
    }
    sync_op = submit_sync(1);
    CHECK(harness_prepare() == 1);
    at = pending_of(VSR_IO_SQE_WRITE);
    disk.fail_write = -EIO;
    harness_complete(at);
    CHECK(h.store->state == VSR_IO_STORE_FAILED && h.store->error == -EIO);
    expect_completion(held, VSR_IO_FAILED);
    expect_completion(sync_op, VSR_IO_FAILED);
    expect_no_completion();
    CHECK(vsr_io_store_store(h.store, 400,
                             &txn_append(sequence, 1, 8)->store) == VSR_OK);
    expect_completion(400, VSR_IO_FAILED);
    harness_poll();
    CHECK(harness_prepare() == 0);
    vsr_io_store_status(h.store, &status);
    CHECK(status.error == -EIO);
    CHECK(vsr_io_store_close(h.store) == VSR_OK);
    harness_close();

    /* CONTINUE in replicated mode. */
    c = base_config();
    c.durability = VSR_REPLICATED;
    c.on_write_error = VSR_IO_WRITE_ERROR_CONTINUE;
    c.write_behind_bytes = 2 * BLOCK;
    harness_open(&c);
    harness_start(VSR_START_NEW);
    expect_completion(submit(txn_append(1, 1, 50)), VSR_IO_OK);
    CHECK(harness_prepare() == 1);
    disk.fail_write = -EIO;
    harness_complete(pending_of(VSR_IO_SQE_WRITE));
    CHECK(h.store->state == VSR_IO_STORE_READY && h.store->error == -EIO);
    for (sequence = 2; sequence < 30; ++sequence) {
        expect_completion(submit(txn_append(sequence, 4, 200)), VSR_IO_OK);
        harness_poll();
        CHECK(harness_prepare() == 0); /* Writing stopped, growth too. */
    }
    CHECK(h.store->slots > c.segments); /* Slots of the table only. */
    CHECK(disk.size == 2 * BLOCK + c.segments * c.segment_bytes);
    vsr_io_store_status(h.store, &status);
    CHECK(status.readable == 29 && status.unwritten_bytes == 0);
    CHECK(status.error == -EIO);
    harness_close();
}

/* A short write is an error too. */
static void test_short_write(void)
{
    struct config c = base_config();
    struct vsr_io_cqe cqe;
    struct vsr_io_slot *slot;
    uint32_t index = NONE;
    uint32_t at;

    harness_open(&c);
    harness_start(VSR_START_NEW);
    expect_completion(submit(txn_append(1, 1, 50)), VSR_IO_OK);
    CHECK(harness_prepare() == 1);
    at = pending_of(VSR_IO_SQE_WRITE);
    h.pending[at].state = 0;
    h.pending_count--;
    memset(&cqe, 0, sizeof(cqe));
    cqe.user_data = h.pending[at].sqe.user_data;
    cqe.result = (int32_t)h.pending[at].sqe.length - 8;
    slot = vsr_io_slots_resolve(&h.io->slots, cqe.user_data, &index);
    CHECK(slot != NULL);
    vsr_io_store_complete(h.io, REPLICA, index, &cqe);
    CHECK(h.store->state == VSR_IO_STORE_FAILED && h.store->error == -EIO);
    harness_close();
}

/* close is EBUSY while a write, a flush or a file operation is out. */
static void test_close(void)
{
    struct config c = base_config();
    uint32_t at;

    harness_open(&c);
    CHECK(vsr_io_store_close(h.store) == VSR_OK); /* Never opened. */
    harness_open(&c);
    harness_start(VSR_START_NEW);
    expect_completion(submit(txn_append(1, 1, 50)), VSR_IO_OK);
    CHECK(harness_prepare() == 1);
    CHECK(vsr_io_store_close(h.store) == VSR_EBUSY);
    at = pending_of(VSR_IO_SQE_WRITE);
    submit_sync(1);
    harness_complete(at);
    CHECK(vsr_io_store_close(h.store) == VSR_OK); /* The flush is not out. */
    harness_open(&c);
    harness_start(VSR_START_NEW);
    expect_completion(submit(txn_append(1, 1, 50)), VSR_IO_OK);
    submit_sync(1);
    harness_poll();
    CHECK(harness_prepare() == 1);
    harness_complete(pending_of(VSR_IO_SQE_WRITE));
    CHECK(harness_prepare() == 1);
    CHECK(pending_of(VSR_IO_SQE_FSYNC) != NONE);
    CHECK(vsr_io_store_close(h.store) == VSR_EBUSY);
    harness_complete(pending_of(VSR_IO_SQE_FSYNC));
    CHECK(vsr_io_store_close(h.store) == VSR_OK);
    harness_close();
}

/* The phase-2 and phase-3 entry points refuse until they exist. */
/* The phase-2 entry points on an empty store: a malformed LOAD is EINVAL,
 * RECLAIM completes, admission works, nothing waits for a base. */
static void test_empty_index(void)
{
    struct config c = base_config();
    struct vsr_store_read read;
    struct vsr_id id = {1, 2};

    harness_open(&c);
    harness_start(VSR_START_NEW);
    memset(&read, 0, sizeof(read));
    read.type = VSR_LOAD_LOG;
    CHECK(vsr_io_store_load(h.store, 50, &read) == VSR_EINVAL);
    CHECK(vsr_io_store_reclaim(h.store, 51, 1) == VSR_OK);
    expect_completion(51, VSR_IO_OK);
    CHECK(vsr_io_store_admit(h.store, id));
    CHECK(h.store->clients_count == 1);
    vsr_io_store_replied(h.store, id);
    CHECK(h.store->clients_count == 0);
    CHECK(!vsr_io_store_base_wanted(h.store, &id, NULL));
    CHECK(vsr_io_store_free_floor(h.store) == 1);
    expect_no_completion();
    harness_close();
}

int main(void)
{
    uint64_t header_bytes;
    uint64_t max_record;

    derive(&header_bytes, &max_record);
    printf("store: header %" PRIu64 " bytes, record limit %" PRIu64 "\n",
           header_bytes, max_record);
    test_check();
    test_create();
    test_recover_missing();
    test_open_existing();
    test_store_packs();
    test_write_behind();
    test_sync_fdatasync();
    test_sync_dsync();
    test_sync_delay();
    test_segments();
    test_growth();
    test_wrap();
    test_pins();
    test_idle_superblock();
    test_replicated_flush();
    test_write_error();
    test_short_write();
    test_close();
    test_empty_index();
    printf("store: ok\n");
    return 0;
}
