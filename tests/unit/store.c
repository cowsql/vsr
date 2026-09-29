/* The GNU declarations (O_DIRECT, AT_FDCWD) precede every system header. */
#define _GNU_SOURCE
#include "config.h"

#include "io/codec.h"
#include "io/crc32c.h"
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
#include <sys/stat.h>

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
#define BASE_SLOT                                                              \
    109 /* The base file's slot, as the snapshot module holds it. */
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
/* Replica 0's lease table, as engine part 2's attach sets it up. */
#define LEASE_BYTES 16384u
static struct vsr_io_lease leases[REGIONS];
static _Alignas(16) unsigned char lease_memory[REGIONS * LEASE_BYTES];
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
    uint32_t reads;
    uint32_t stats; /* STATX calls. */
    uint64_t base_size; /* The base file image (base_image). */
    int32_t base_slot;
    uint8_t once[DISK_BLOCKS];  /* A segment block written already. */
    uint8_t dirty[DISK_BLOCKS]; /* Written since the last flush. */
    /* Faults: the next WRITE persists only `tear_blocks` blocks and its
     * completion is lost (a crash); `fail_write` makes it complete with
     * that errno instead. */
    uint32_t tear_armed;
    uint32_t tear_blocks;
    int32_t fail_write;
    int32_t fail_flush;
    int32_t fail_read;   /* The next READ completes with this errno. */
    uint32_t short_read; /* The next READ returns only this many bytes. */
};

static struct disk disk;
static unsigned char base_image[65536];

static void disk_reset(void)
{
    memset(&disk, 0, sizeof(disk));
    memset(disk_image, 0, sizeof(disk_image));
    disk.slot = -1;
    disk.base_slot = -1;
}

/* A freed slot may be rewritten: the model forgets its blocks when the
 * store has freed the slot, which the tests do only for such a slot. */
static void disk_forget_slot(uint32_t slot, uint64_t segment_bytes)
{
    uint64_t first = (2 * BLOCK + slot * segment_bytes) / BLOCK;

    memset(disk.once + first, 0, segment_bytes / BLOCK);
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
    CHECK(vsr_io_codec_load_region(&limits, &bytes) == VSR_OK &&
          bytes <= LEASE_BYTES);
    memset(leases, 0, sizeof(leases));
    for (uint32_t i = 0; i < REGIONS; ++i) {
        leases[i].slab = NONE;
        leases[i].pin = NONE;
        vsr_io_bump_init(&leases[i].region,
                         lease_memory + (size_t)i * LEASE_BYTES, LEASE_BYTES);
    }
    h.replica->leases = leases;
    h.replica->regions_count = REGIONS;
    h.replica->leases_free = REGIONS;
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
    case VSR_IO_SQE_STATX: {
        struct statx *out = (struct statx *)(uintptr_t)sqe->addr2;

        CHECK(sqe->fd == AT_FDCWD && sqe->flags == 0);
        CHECK(strcmp(sqe->addr, DIRECTORY "/log") == 0);
        CHECK((sqe->length & STATX_SIZE) != 0 && out != NULL);
        if (!disk.exists) {
            *result = -ENOENT;
            return true;
        }
        memset(out, 0, sizeof(*out));
        out->stx_mask = STATX_SIZE;
        out->stx_size = disk.size;
        disk.stats++;
        *result = 0;
        return true;
    }
    case VSR_IO_SQE_READ: {
        const unsigned char *image = disk_image;
        uint64_t size = disk.size;

        CHECK((sqe->flags & VSR_IO_SQE_FIXED_FILE) != 0);
        if ((sqe->flags & VSR_IO_SQE_FIXED_BUFFER) != 0 &&
            in_pool(sqe->addr, sqe->length)) {
            CHECK(sqe->buffer_index == h.io->pool.region_index);
        }
        if (sqe->fd == disk.base_slot) {
            image = base_image;
            size = disk.base_size;
        } else {
            CHECK(sqe->fd == disk.slot);
            if ((disk.flags & O_DIRECT) != 0) {
                CHECK(sqe->offset % BLOCK == 0 && sqe->length % BLOCK == 0);
            }
        }
        CHECK(sqe->offset <= size);
        disk.reads++;
        if (disk.fail_read != 0) {
            *result = disk.fail_read;
            disk.fail_read = 0;
            return true;
        }
        end = sqe->offset + sqe->length;
        if (end > size) {
            end = size;
        }
        if (disk.short_read != 0) {
            end = sqe->offset + disk.short_read;
            disk.short_read = 0;
        }
        memcpy(writable(sqe->addr, sqe->length), image + sqe->offset,
               end - sqe->offset);
        *result = (int32_t)(end - sqe->offset);
        return true;
    }
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

/* Transactions are built in the static table by sequence and pinned
 * there until the test ends: begin, add changes, finish. */
static struct txn *txn_begin(uint64_t sequence)
{
    struct txn *t;

    CHECK(sequence >= 1 && sequence < TXN_MAX);
    t = &txns[sequence];
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

/* `count` COMMAND entries of `body` bytes each from op `first`, for
 * `client` (zero: alternating default clients) with request numbers
 * from `number`. */
static void txn_entries(struct txn *t, uint64_t first, uint32_t count,
                        size_t body, struct vsr_id client, uint64_t number)
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
        if (client.hi == 0 && client.lo == 0) {
            entry->request.client.hi = 0x1000 + (i % 2);
            entry->request.client.lo = 1;
            entry->request.number = sequence;
        } else {
            entry->request.client = client;
            entry->request.number = number + i;
        }
        entry->type = VSR_REQUEST_COMMAND;
        entry->body = &t->blobs[i];
    }
    txn_add(t, VSR_STORE_APPEND, first, count, t->entries);
}

/* An APPEND at the log end of `sequence`. */
static const struct txn *txn_append(uint64_t sequence, uint32_t count,
                                    size_t body)
{
    struct txn *t = txn_begin(sequence);
    struct vsr_id none = {0, 0};

    txn_entries(t, log_end_before(sequence), count, body, none, 0);
    return txn_finish(t);
}

/* An APPEND of entries of one client, numbered from `number`. */
static const struct txn *txn_append_client(uint64_t sequence, uint32_t count,
                                           size_t body, struct vsr_id client,
                                           uint64_t number)
{
    struct txn *t = txn_begin(sequence);

    txn_entries(t, log_end_before(sequence), count, body, client, number);
    return txn_finish(t);
}

/* A TRUNCATE at `first`, followed by an APPEND of `count` entries there
 * when count is nonzero. */
static const struct txn *txn_truncate(uint64_t sequence, uint64_t first,
                                      uint32_t count, size_t body)
{
    struct txn *t = txn_begin(sequence);
    struct vsr_id none = {0, 0};

    txn_add(t, VSR_STORE_TRUNCATE, first, 0, NULL);
    if (count > 0) {
        txn_entries(t, first, count, body, none, 0);
    }
    return txn_finish(t);
}

/* A TRIM to `first`: the smallest record (header and one descriptor). */
static const struct txn *txn_trim(uint64_t sequence, uint64_t first)
{
    struct txn *t = txn_begin(sequence);

    txn_add(t, VSR_STORE_TRIM, first, 0, NULL);
    return txn_finish(t);
}

/* CLIENTS records: for each i, client ids[i] completed request
 * numbers[i] at ops[i] with a result of `result` bytes. */
static const struct txn *txn_clients(uint64_t sequence, uint32_t count,
                                     const struct vsr_id *ids,
                                     const uint64_t *numbers,
                                     const uint64_t *ops, size_t result)
{
    struct txn *t = txn_begin(sequence);

    CHECK(count >= 1 && count <= 4 && result <= 64);
    for (uint32_t i = 0; i < count; ++i) {
        struct vsr_client_record *record = &t->records[i];

        for (size_t b = 0; b < result; ++b) {
            t->results[i][b] = (unsigned char)(numbers[i] * 13u + b);
        }
        t->result_spans[i].data = t->results[i];
        t->result_spans[i].size = result;
        record->request.client = ids[i];
        record->request.number = numbers[i];
        record->op = ops[i];
        record->result.code = 0;
        record->result.data.spans = result > 0 ? &t->result_spans[i] : NULL;
        record->result.data.size = result;
        record->result.data.count = result > 0 ? 1 : 0;
    }
    txn_add(t, VSR_STORE_CLIENTS, 0, count, t->records);
    return txn_finish(t);
}

/* One steady epoch of one FULL member. */
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
static const struct txn *txn_identity(uint64_t sequence, uint32_t role)
{
    struct txn *t = txn_begin(sequence);

    t->identity.cluster.hi = 0x77;
    t->identity.cluster.lo = 0x99;
    t->identity.replica = 3;
    t->identity.durability = VSR_DURABLE;
    txn_add(t, VSR_STORE_IDENTITY, 0, 1, &t->identity);
    txn_hard_state(t, role);
    return txn_finish(t);
}

static const struct txn *txn_hard(uint64_t sequence, uint32_t role)
{
    struct txn *t = txn_begin(sequence);

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

static const struct txn *txn_publish(uint64_t sequence, struct vsr_id id,
                                     uint64_t op)
{
    struct txn *t = txn_begin(sequence);

    txn_checkpoint(t, id, op);
    txn_add(t, VSR_STORE_PUBLISH_CHECKPOINT, 0, 1, &t->checkpoint);
    return txn_finish(t);
}

/* A RESTORE with the HARD_STATE the contract requires beside it. */
static const struct txn *txn_restore(uint64_t sequence, struct vsr_id id,
                                     uint64_t op, uint32_t role)
{
    struct txn *t = txn_begin(sequence);

    txn_checkpoint(t, id, op);
    txn_add(t, VSR_STORE_RESTORE_CHECKPOINT, 0, 1, &t->checkpoint);
    txn_hard_state(t, role);
    return txn_finish(t);
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

static uint64_t submit_reclaim(uint64_t oldest)
{
    uint64_t op = next_op();

    CHECK(vsr_io_store_reclaim(h.store, op, oldest) == VSR_OK);
    return op;
}

/* -------------------------------------------------------------------------
 * Loads
 * ---------------------------------------------------------------------- */

static struct vsr_store_read read_of(uint32_t type, uint64_t sequence,
                                     struct vsr_id client)
{
    struct vsr_store_read read;

    memset(&read, 0, sizeof(read));
    read.type = type;
    read.sequence = sequence;
    read.client = client;
    read.max_count = 1;
    read.max_bytes = limits.message_bytes;
    return read;
}

static uint64_t load_log(uint64_t sequence, uint64_t first, uint64_t end,
                         uint32_t max_count, uint64_t max_bytes)
{
    struct vsr_id none = {0, 0};
    struct vsr_store_read read = read_of(VSR_LOAD_LOG, sequence, none);
    uint64_t op = next_op();

    read.first = first;
    read.end = end;
    read.max_count = max_count;
    read.max_bytes = max_bytes;
    CHECK(vsr_io_store_load(h.store, op, &read) == VSR_OK);
    return op;
}

static uint64_t load_client(uint64_t sequence, struct vsr_id client)
{
    struct vsr_store_read read = read_of(VSR_LOAD_CLIENT, sequence, client);
    uint64_t op = next_op();

    CHECK(vsr_io_store_load(h.store, op, &read) == VSR_OK);
    return op;
}

static uint64_t load_request(uint64_t sequence, struct vsr_id client)
{
    struct vsr_store_read read = read_of(VSR_LOAD_REQUEST, sequence, client);
    uint64_t op = next_op();

    CHECK(vsr_io_store_load(h.store, op, &read) == VSR_OK);
    return op;
}

/* The next completion must be `op` with `status`; an OK LOAD carries
 * its graph in a lease, returned; any other has neither. */
static const struct vsr_loaded *expect_loaded(uint64_t op, int32_t status,
                                              uint32_t *lease)
{
    struct vsr_io_completion completion;

    CHECK(next_completion(&completion));
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
    CHECK(h.replica->leases[completion.lease].state != 0);
    *lease = completion.lease;
    return completion.data;
}

/* RELEASE of a LOAD lease, as the engine routes it. */
static void release_lease(uint32_t lease)
{
    vsr_io_store_release(h.store, lease);
    vsr_io_lease_release(h.replica, lease);
}

/* The entry must be entry `i` of the APPEND of `t`, body included. */
static void check_entry(const struct vsr_entry *entry, const struct txn *t,
                        uint32_t i)
{
    const struct vsr_entry *expected = &t->entries[i];
    const struct vsr_blob *body = entry->body;

    CHECK(entry->op == expected->op && entry->type == VSR_REQUEST_COMMAND);
    CHECK(entry->request.client.hi == expected->request.client.hi &&
          entry->request.client.lo == expected->request.client.lo &&
          entry->request.number == expected->request.number);
    CHECK(body != NULL && body->size == t->blobs[i].size);
    if (body->size > 0) {
        CHECK(body->count == 1 && memcmp(body->spans[0].data, t->bodies[i],
                                         (size_t)body->size) == 0);
    }
}

/* The base file image: client records written by the test in place of
 * the snapshot module, read by the store through the base slot. */
static void base_file_write(const struct vsr_client_record *records,
                            uint32_t count, uint64_t *offsets)
{
    size_t at = 64; /* Where a header would be. */

    memset(base_image, 0, sizeof(base_image));
    for (uint32_t i = 0; i < count; ++i) {
        size_t bytes =
            vsr_io_codec_clients_record_bytes(records[i].result.data.size);

        CHECK(at + bytes <= sizeof(base_image));
        vsr_io_codec_put_clients_record(&records[i], base_image + at);
        offsets[i] = at;
        at += bytes;
    }
    disk.base_size = at;
    disk.base_slot = BASE_SLOT;
    h.store->base_slot = BASE_SLOT;
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

/* An existing log routes to recovery under every start mode (NEW over
 * stray state is refused through the recovered row, decision 57); a file
 * without a valid superblock is CORRUPT, which fences. */
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
    CHECK(h.store->state == VSR_IO_STORE_RECOVERING);
    CHECK(h.store->log_slot == (int32_t)FILE_SLOT_BASE);
    CHECK(harness_prepare() == 1);
    harness_complete(pending_of(VSR_IO_SQE_STATX));
    CHECK(harness_prepare() == 1);
    harness_complete(pending_of(VSR_IO_SQE_READ));
    CHECK(h.store->state == VSR_IO_STORE_FAILED);
    expect_completion(op, VSR_IO_CORRUPT);
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
    ops[sequence] = submit(txn_trim(sequence, 1));
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

/* -------------------------------------------------------------------------
 * Phase 2: indexes, loads, reclaim, freeing, clients, captures
 * ---------------------------------------------------------------------- */

static const struct vsr_io_op_ref *op_ref(uint64_t op)
{
    return &h.store->ops[op % h.options.max_entries];
}

static const struct vsr_io_client *client_of(struct vsr_id id)
{
    for (uint32_t i = 0; i < h.store->clients_capacity; ++i) {
        const struct vsr_io_client *entry = &h.store->clients[i];

        if (entry->id.hi == id.hi && entry->id.lo == id.lo) {
            return entry;
        }
    }
    return NULL;
}

/* Appends records (written before the next) until the record of
 * sequence 1 left the ring. */
static uint64_t evict_first(uint64_t sequence)
{
    struct vsr_io_piece piece;
    uint64_t first = 2 * BLOCK + h.header_bytes;

    while (vsr_io_store_hot(h.store, first, (uint32_t)txns[1].bytes, &piece)) {
        expect_completion(submit(txn_append(sequence, 4, 200)), VSR_IO_OK);
        harness_run();
        sequence++;
        CHECK(sequence < 256);
    }
    return sequence;
}

/* The op ring after APPENDs by two clients, LOG and REQUEST loads at the
 * current and an older sequence, TRUNCATE moving versions and fixing the
 * retained entries, RETRY for a REQUEST behind a TRUNCATE, RECLAIM
 * dropping the versions. */
static void test_index_ops(void)
{
    struct config c = base_config();
    struct vsr_id a = {0xA, 1};
    struct vsr_id b = {0xB, 1};
    const struct vsr_loaded *loaded;
    const struct vsr_entry *entries;
    struct vsr_store_read read;
    uint32_t lease = NONE;
    uint64_t op;

    harness_open(&c);
    harness_start(VSR_START_NEW);
    expect_completion(submit(txn_append_client(1, 2, 20, a, 1)), VSR_IO_OK);
    expect_completion(submit(txn_append_client(2, 1, 20, b, 10)), VSR_IO_OK);
    expect_completion(submit(txn_append_client(3, 2, 20, a, 3)), VSR_IO_OK);
    CHECK(h.store->log_begin == 1 && h.store->log_end == 6);
    CHECK(h.store->retained_begin == 1 && h.store->clients_count == 2);
    CHECK(op_ref(5)->op == 5 && op_ref(5)->sequence == 3 &&
          op_ref(5)->index == 1 && op_ref(5)->previous == 4);
    CHECK(op_ref(4)->previous == 2 && op_ref(2)->previous == 1 &&
          op_ref(1)->previous == 0 && op_ref(3)->previous == 0);
    CHECK(op_ref(3)->client.hi == 0xB && op_ref(3)->change == 0);
    CHECK(client_of(a)->retained == 5 && client_of(b)->retained == 3);
    /* REQUEST at the current sequence, and at the one before (decision
     * 51: the previous link). */
    op = load_request(3, a);
    loaded = expect_loaded(op, VSR_IO_OK, &lease);
    CHECK(loaded->count == 1 && loaded->sequence == 3 && loaded->next == 0);
    check_entry(loaded->items, &txns[3], 1);
    CHECK(h.store->pins[lease].begin != UINT64_MAX);
    release_lease(lease);
    CHECK(h.store->pins[lease].begin == UINT64_MAX);
    op = load_request(2, a);
    loaded = expect_loaded(op, VSR_IO_OK, &lease);
    check_entry(loaded->items, &txns[1], 1);
    release_lease(lease);
    op = load_request(3, b);
    loaded = expect_loaded(op, VSR_IO_OK, &lease);
    check_entry(loaded->items, &txns[2], 0);
    release_lease(lease);
    /* LOG: hot across records, the count and byte bounds, the ends. */
    op = load_log(3, 1, 6, 4, limits.message_bytes);
    loaded = expect_loaded(op, VSR_IO_OK, &lease);
    CHECK(loaded->count == 4 && loaded->next == 5);
    entries = loaded->items;
    check_entry(&entries[0], &txns[1], 0);
    check_entry(&entries[1], &txns[1], 1);
    check_entry(&entries[2], &txns[2], 0);
    check_entry(&entries[3], &txns[3], 0);
    release_lease(lease);
    op = load_log(3, 5, 6, 4, limits.message_bytes);
    loaded = expect_loaded(op, VSR_IO_OK, &lease);
    CHECK(loaded->count == 1 && loaded->next == 6);
    check_entry(loaded->items, &txns[3], 1);
    release_lease(lease);
    op = load_log(3, 1, 6, 4, 30); /* One entry of 20 fits, two do not. */
    loaded = expect_loaded(op, VSR_IO_OK, &lease);
    CHECK(loaded->count == 1 && loaded->next == 2);
    release_lease(lease);
    op = load_log(3, 6, 6, 4, limits.message_bytes); /* Empty range. */
    loaded = expect_loaded(op, VSR_IO_OK, &lease);
    CHECK(loaded->count == 0 && loaded->next == 6 && loaded->items == NULL);
    release_lease(lease);
    op = load_log(3, 6, 9, 4, limits.message_bytes);
    expect_loaded(op, VSR_IO_NOT_FOUND, &lease);
    op = load_log(2, 4, 6, 4, limits.message_bytes); /* Not yet at 2. */
    expect_loaded(op, VSR_IO_NOT_FOUND, &lease);
    op = load_log(2, 1, 6, 4, limits.message_bytes);
    loaded = expect_loaded(op, VSR_IO_OK, &lease);
    CHECK(loaded->count == 3 && loaded->next == 4);
    release_lease(lease);
    /* Malformed loads are the caller's bug. */
    read = read_of(VSR_LOAD_LOG, 4, a);
    CHECK(vsr_io_store_load(h.store, 70, &read) == VSR_EINVAL); /* > readable */
    read = read_of(VSR_LOAD_RECOVERY, 3, a);
    CHECK(vsr_io_store_load(h.store, 70, &read) == VSR_EINVAL);
    read = read_of(VSR_LOAD_LOG, 3, a);
    read.first = 5;
    read.end = 4;
    CHECK(vsr_io_store_load(h.store, 70, &read) == VSR_EINVAL);
    read.end = 6;
    read.max_count = limits.batch_entries + 1;
    CHECK(vsr_io_store_load(h.store, 70, &read) == VSR_EINVAL);
    CHECK(vsr_io_store_load(h.store, 0, &read) == VSR_EINVAL);
    CHECK(vsr_io_store_load(h.store, 70, NULL) == VSR_EINVAL);
    expect_no_completion();
    /* TRUNCATE at 4 with a new op 4: the versions of 4 and 5 move to the
     * table, a's retained entry falls back to 2. */
    expect_completion(submit(txn_truncate(4, 4, 1, 20)), VSR_IO_OK);
    CHECK(h.store->log_end == 5 && h.store->versions_count == 2);
    CHECK(op_ref(4)->sequence == 4 && op_ref(5)->op == 0);
    CHECK(client_of(a)->retained == 2 && h.store->reindexed == 4);
    for (uint32_t i = 0; i < 2; ++i) {
        const struct vsr_io_version *v = &h.store->versions[i];

        CHECK((v->op == 4 || v->op == 5) && v->appended == 3 &&
              v->truncated == 4 && v->client.hi == 0xA);
    }
    op = load_log(3, 4, 6, 4, limits.message_bytes); /* The old versions. */
    loaded = expect_loaded(op, VSR_IO_OK, &lease);
    CHECK(loaded->count == 2 && loaded->next == 6);
    entries = loaded->items;
    check_entry(&entries[0], &txns[3], 0);
    check_entry(&entries[1], &txns[3], 1);
    release_lease(lease);
    op = load_log(4, 4, 6, 4, limits.message_bytes);
    loaded = expect_loaded(op, VSR_IO_OK, &lease);
    CHECK(loaded->count == 1 && loaded->next == 5);
    check_entry(loaded->items, &txns[4], 0);
    release_lease(lease);
    op = load_request(3, a);
    expect_loaded(op, VSR_IO_RETRY, &lease);
    op = load_request(4, a);
    loaded = expect_loaded(op, VSR_IO_OK, &lease);
    check_entry(loaded->items, &txns[1], 1);
    release_lease(lease);
    /* RECLAIM past the truncation drops the versions; revision 3 can no
     * longer be named. */
    expect_completion(submit_reclaim(4), VSR_IO_OK);
    CHECK(h.store->versions_count == 0 && h.store->reclaim == 4);
    read = read_of(VSR_LOAD_LOG, 3, a);
    read.end = 6;
    CHECK(vsr_io_store_load(h.store, 71, &read) == VSR_EINVAL);
    CHECK(vsr_io_store_reclaim(h.store, 72, 6) == VSR_EINVAL);
    expect_completion(submit_reclaim(2), VSR_IO_OK); /* Never backwards. */
    CHECK(h.store->reclaim == 4);
    expect_no_completion();
    harness_close();
}

/* The op ring and the client table at capacity fail the STORE with
 * FAILED and fence (an invariant, decision 44). */
static void test_index_full(void)
{
    struct config c = base_config();
    struct vsr_id a = {0xA, 1};
    struct vsr_id b = {0xB, 1};
    struct vsr_id d = {0xD, 1};
    uint64_t op;

    c.max_entries = 8;
    harness_open(&c);
    harness_start(VSR_START_NEW);
    expect_completion(submit(txn_append(1, 4, 8)), VSR_IO_OK);
    expect_completion(submit(txn_append(2, 4, 8)), VSR_IO_OK);
    op = submit(txn_append(3, 1, 8));
    expect_completion(op, VSR_IO_FAILED);
    CHECK(h.store->state == VSR_IO_STORE_FAILED);
    op = submit(txn_append(4, 1, 8)); /* Fenced: everything fails. */
    expect_completion(op, VSR_IO_FAILED);
    harness_close();

    c = base_config();
    c.max_clients = 2;
    harness_open(&c);
    harness_start(VSR_START_NEW);
    expect_completion(submit(txn_append_client(1, 1, 8, a, 1)), VSR_IO_OK);
    CHECK(vsr_io_store_admit(h.store, b)); /* In flight counts. */
    CHECK(!vsr_io_store_admit(h.store, d));
    CHECK(vsr_io_store_admit(h.store, a)); /* Known. */
    expect_completion(submit(txn_append_client(2, 1, 8, b, 1)), VSR_IO_OK);
    CHECK(client_of(b)->inflight == 0 && h.store->clients_count == 2);
    op = submit(txn_append_client(3, 1, 8, d, 1));
    expect_completion(op, VSR_IO_FAILED);
    CHECK(h.store->state == VSR_IO_STORE_FAILED);
    harness_close();
}

/* Cold LOADs: one read at a time into a pool slab the lease then holds,
 * the batch cut to a slab, a short read, a read error, a fence with a
 * read in flight. */
static void test_load_cold(void)
{
    struct config c = base_config();
    const struct vsr_loaded *loaded;
    const struct vsr_entry *entries;
    const struct vsr_blob *body;
    uint64_t header_bytes;
    uint64_t max_record;
    uint64_t sequence;
    uint64_t data;
    uint64_t op;
    uint64_t op2;
    uint32_t lease = NONE;
    uint32_t free_slabs;
    uint32_t at;

    derive(&header_bytes, &max_record);
    c.segment_bytes = round_up(header_bytes + 64 * max_record, BLOCK);
    harness_open(&c);
    harness_start(VSR_START_NEW);
    data = 2 * BLOCK + h.header_bytes;
    expect_completion(submit(txn_append(1, 4, 200)), VSR_IO_OK);
    harness_run();
    sequence = evict_first(2);
    free_slabs = h.io->pool.free_count;
    /* The read: block aligned, fixed file and buffer, into the pool. */
    op = load_log(sequence - 1, 1, 5, 4, limits.message_bytes);
    expect_no_completion();
    CHECK(h.store->loads_count == 1);
    CHECK(harness_prepare() == 1);
    at = pending_of(VSR_IO_SQE_READ);
    CHECK(at != NONE && h.pending[at].sqe.fd == disk.slot);
    CHECK(h.pending[at].sqe.flags ==
          (VSR_IO_SQE_FIXED_FILE | VSR_IO_SQE_FIXED_BUFFER));
    CHECK(h.pending[at].sqe.offset == data / BLOCK * BLOCK);
    CHECK(h.pending[at].sqe.length ==
          round_up(data + txns[1].bytes, BLOCK) - data / BLOCK * BLOCK);
    CHECK(in_pool(h.pending[at].sqe.addr, h.pending[at].sqe.length));
    CHECK(h.io->pool.free_count == free_slabs - 1);
    CHECK(h.store->cold_active == 1);
    harness_complete(at);
    loaded = expect_loaded(op, VSR_IO_OK, &lease);
    CHECK(loaded->count == 4 && loaded->next == 5 &&
          loaded->sequence == sequence - 1);
    entries = loaded->items;
    for (uint32_t i = 0; i < 4; ++i) {
        check_entry(&entries[i], &txns[1], i);
    }
    body = entries[0].body;
    CHECK(in_pool(body->spans[0].data, (size_t)body->size));
    CHECK(h.replica->leases[lease].slab != NONE);
    CHECK(h.store->pins[lease].begin == UINT64_MAX); /* No ring pin. */
    CHECK(h.io->pool.free_count == free_slabs - 1);
    release_lease(lease);
    CHECK(h.io->pool.free_count == free_slabs);
    /* Two cold loads: the second reads only after the first completed;
     * a hot one behind them waits its turn. */
    op = load_log(sequence - 1, 1, 2, 1, limits.message_bytes);
    op2 = load_log(sequence - 1, 3, 4, 1, limits.message_bytes);
    CHECK(harness_prepare() == 1);
    CHECK(harness_prepare() == 0);
    harness_complete(pending_of(VSR_IO_SQE_READ));
    loaded = expect_loaded(op, VSR_IO_OK, &lease);
    CHECK(loaded->count == 1);
    check_entry(loaded->items, &txns[1], 0);
    release_lease(lease);
    expect_no_completion();
    CHECK(harness_prepare() == 1);
    harness_complete(pending_of(VSR_IO_SQE_READ));
    loaded = expect_loaded(op2, VSR_IO_OK, &lease);
    check_entry(loaded->items, &txns[1], 2);
    release_lease(lease);
    /* A short read leaves the record incomplete: CORRUPT, slab back. */
    op = load_log(sequence - 1, 1, 2, 1, limits.message_bytes);
    CHECK(harness_prepare() == 1);
    disk.short_read = 8;
    harness_complete(pending_of(VSR_IO_SQE_READ));
    expect_loaded(op, VSR_IO_CORRUPT, &lease);
    CHECK(h.io->pool.free_count == free_slabs);
    CHECK(h.store->state == VSR_IO_STORE_READY);
    op = load_log(sequence - 1, 1, 2, 1, limits.message_bytes);
    CHECK(harness_prepare() == 1);
    disk.fail_read = -EIO;
    harness_complete(pending_of(VSR_IO_SQE_READ));
    expect_loaded(op, VSR_IO_FAILED, &lease);
    CHECK(h.io->pool.free_count == free_slabs);
    CHECK(h.store->state == VSR_IO_STORE_READY);
    /* A fence with the read in flight: the load fails at once and the
     * slab returns when the read completes. */
    op = load_log(sequence - 1, 1, 2, 1, limits.message_bytes);
    expect_completion(submit(txn_append(sequence, 1, 8)), VSR_IO_OK);
    CHECK(harness_prepare() == 2);
    disk.fail_write = -EIO;
    harness_complete(pending_of(VSR_IO_SQE_WRITE));
    CHECK(h.store->state == VSR_IO_STORE_FAILED);
    expect_completion(op, VSR_IO_FAILED);
    CHECK(h.io->pool.free_count == free_slabs - 1);
    harness_complete(pending_of(VSR_IO_SQE_READ));
    CHECK(h.io->pool.free_count == free_slabs && h.store->cold_active == 0);
    harness_close();
}

/* CLIENTS records and CLIENT loads: the latest completed record, RETRY
 * for a load behind it (decision 51), older results ignored, admission
 * and replies, a contradicting record CORRUPT. */
static void test_clients(void)
{
    struct config c = base_config();
    struct vsr_id ids[4] = {{0xA, 1}, {0xB, 1}, {0xC, 1}, {0xD, 1}};
    uint64_t numbers[4] = {1, 1, 0, 0};
    uint64_t ops[4] = {1, 2, 0, 0};
    const struct vsr_loaded *loaded;
    const struct vsr_client_record *record;
    uint32_t lease = NONE;
    uint64_t op;

    c.max_clients = 3;
    harness_open(&c);
    harness_start(VSR_START_NEW);
    expect_completion(submit(txn_clients(1, 2, ids, numbers, ops, 8)),
                      VSR_IO_OK);
    CHECK(h.store->clients_count == 2 && h.store->clients_sequence == 1);
    CHECK(client_of(ids[0])->current.number == 1 &&
          client_of(ids[0])->current.sequence == 1 &&
          client_of(ids[1])->current.index == 1);
    op = load_client(1, ids[0]);
    loaded = expect_loaded(op, VSR_IO_OK, &lease);
    CHECK(loaded->count == 1 && loaded->next == 0);
    record = loaded->items;
    CHECK(record->request.number == 1 && record->op == 1 &&
          record->result.data.size == 8 &&
          memcmp(record->result.data.spans[0].data, txns[1].results[0], 8) ==
              0);
    release_lease(lease);
    op = load_client(1, ids[3]); /* Unknown: a count-zero success. */
    loaded = expect_loaded(op, VSR_IO_OK, &lease);
    CHECK(loaded->count == 0 && loaded->items == NULL);
    release_lease(lease);
    numbers[0] = 2;
    ops[0] = 3;
    expect_completion(submit(txn_clients(2, 1, ids, numbers, ops, 8)),
                      VSR_IO_OK);
    op = load_client(1, ids[0]);
    expect_loaded(op, VSR_IO_RETRY, &lease);
    op = load_client(2, ids[0]);
    loaded = expect_loaded(op, VSR_IO_OK, &lease);
    record = loaded->items;
    CHECK(record->request.number == 2 && record->op == 3);
    release_lease(lease);
    numbers[0] = 1;
    ops[0] = 1;
    expect_completion(submit(txn_clients(3, 1, ids, numbers, ops, 8)),
                      VSR_IO_OK); /* Replayed: ignored. */
    CHECK(client_of(ids[0])->current.number == 2);
    /* Admission: known or room within max_clients; a reply to an
     * incarnation with nothing else deletes it; an APPEND indexes it. */
    CHECK(vsr_io_store_admit(h.store, ids[2]));
    CHECK(client_of(ids[2])->inflight == 1 && h.store->clients_count == 3);
    CHECK(!vsr_io_store_admit(h.store, ids[3]));
    vsr_io_store_replied(h.store, ids[2]);
    CHECK(client_of(ids[2]) == NULL && h.store->clients_count == 2);
    vsr_io_store_replied(h.store, ids[0]); /* Has a record: stays. */
    CHECK(client_of(ids[0]) != NULL);
    CHECK(vsr_io_store_admit(h.store, ids[3]));
    expect_completion(submit(txn_append_client(4, 1, 8, ids[3], 5)), VSR_IO_OK);
    CHECK(client_of(ids[3])->inflight == 0 && client_of(ids[3])->retained == 1);
    /* Same number, another op: CORRUPT, fenced. */
    numbers[0] = 2;
    ops[0] = 99;
    op = submit(txn_clients(5, 1, ids, numbers, ops, 8));
    expect_completion(op, VSR_IO_CORRUPT);
    CHECK(h.store->state == VSR_IO_STORE_FAILED);
    op = load_client(5, ids[0]);
    expect_loaded(op, VSR_IO_FAILED, &lease);
    harness_close();
}

/* The freeing floor with each term binding in turn: the oldest retained
 * record, the RECLAIM revision, the client base, a capture, a kept
 * version. */
static void test_floor(void)
{
    struct config c = base_config();
    struct vsr_id a = {0xA, 1};
    struct vsr_id b = {0xB, 1};
    struct vsr_id x = {0x51, 1};
    struct vsr_id y = {0x52, 1};
    struct vsr_id none = {0, 0};
    struct vsr_io_client_snapshot out[8];
    uint64_t numbers[1] = {1};
    uint64_t ops[1] = {1};

    harness_open(&c);
    harness_start(VSR_START_NEW);
    CHECK(vsr_io_store_free_floor(h.store) == 0); /* No RECLAIM yet. */
    expect_completion(submit(txn_append_client(1, 2, 16, a, 1)), VSR_IO_OK);
    expect_completion(submit(txn_append_client(2, 1, 16, b, 1)), VSR_IO_OK);
    expect_completion(submit(txn_append_client(3, 1, 16, a, 3)), VSR_IO_OK);
    expect_completion(submit_reclaim(2), VSR_IO_OK);
    CHECK(vsr_io_store_free_floor(h.store) == 1); /* Base 0 + 1. */
    vsr_io_store_base_set(h.store, x, 3); /* No records: nothing lower. */
    CHECK(h.store->client_base == 3);
    CHECK(vsr_io_store_free_floor(h.store) == 1); /* Op 1's record. */
    expect_completion(submit(txn_trim(4, 3)), VSR_IO_OK);
    CHECK(h.store->log_begin == 3 && h.store->retained_begin == 1);
    CHECK(client_of(a)->retained == 4 && client_of(b)->retained == 3);
    CHECK(vsr_io_store_free_floor(h.store) == 1);
    expect_completion(submit_reclaim(4), VSR_IO_OK);
    CHECK(h.store->retained_begin == 3 && op_ref(1)->op == 0);
    CHECK(vsr_io_store_free_floor(h.store) == 2); /* Op 3's record. */
    expect_completion(submit(txn_trim(5, 5)), VSR_IO_OK);
    CHECK(client_of(a) == NULL && client_of(b) == NULL);
    expect_completion(submit_reclaim(6), VSR_IO_OK);
    CHECK(h.store->log_begin == 5 && h.store->log_end == 5);
    CHECK(vsr_io_store_free_floor(h.store) == 4); /* Client base 3. */
    vsr_io_store_base_set(h.store, x, 5);
    CHECK(vsr_io_store_free_floor(h.store) == 6); /* Everything. */
    expect_completion(submit(txn_append_client(6, 1, 16, a, 9)), VSR_IO_OK);
    expect_completion(submit(txn_trim(7, 6)), VSR_IO_OK);
    CHECK(client_of(a) == NULL); /* Its retained entry was trimmed. */
    expect_completion(submit_reclaim(7), VSR_IO_OK);
    vsr_io_store_base_set(h.store, x, 7);
    CHECK(vsr_io_store_free_floor(h.store) == 7); /* RECLAIM. */
    /* A capture: its floor is the oldest record it copies. */
    expect_completion(submit(txn_clients(8, 1, &a, numbers, ops, 8)),
                      VSR_IO_OK);
    expect_completion(submit_reclaim(9), VSR_IO_OK);
    CHECK(vsr_io_store_snapshot_clients(h.store, out, 8) == 1);
    CHECK(out[0].id.hi == 0xA && out[0].record.sequence == 8);
    CHECK(h.store->capture_floor == 8);
    CHECK(vsr_io_store_free_floor(h.store) == 8); /* Base 7 + 1 too. */
    vsr_io_store_capture_offset(h.store, a, 100);
    vsr_io_store_capture_end(h.store, y, 8);
    CHECK(h.store->capture_floor == UINT64_MAX);
    vsr_io_store_base_set(h.store, y, 9);
    CHECK(h.store->client_base == 9 && client_of(a)->base_offset == 100);
    CHECK(vsr_io_store_free_floor(h.store) == 9);
    CHECK(vsr_io_store_snapshot_clients(h.store, out, 8) == 1);
    CHECK(vsr_io_store_free_floor(h.store) == 8); /* The capture. */
    vsr_io_store_capture_end(h.store, none, 0);   /* Abandoned. */
    CHECK(vsr_io_store_free_floor(h.store) == 9);
    vsr_io_store_base_set(h.store, x, 9); /* A file not covering a. */
    CHECK(h.store->client_base == 7);
    CHECK(vsr_io_store_free_floor(h.store) == 8);
    /* A kept version binds below everything else. */
    expect_completion(submit(txn_append(9, 1, 16)), VSR_IO_OK);
    expect_completion(submit(txn_append(10, 1, 16)), VSR_IO_OK);
    expect_completion(submit(txn_truncate(11, 6, 0, 0)), VSR_IO_OK);
    CHECK(h.store->versions_count == 2 && h.store->log_end == 6);
    vsr_io_store_capture_offset(h.store, a, 100);
    vsr_io_store_base_set(h.store, y, 11);
    CHECK(h.store->client_base == 11);
    expect_completion(submit_reclaim(10), VSR_IO_OK);
    CHECK(h.store->versions_count == 2);
    CHECK(vsr_io_store_free_floor(h.store) == 9); /* Op 6's version. */
    expect_completion(submit_reclaim(11), VSR_IO_OK);
    CHECK(h.store->versions_count == 0);
    CHECK(vsr_io_store_free_floor(h.store) == 11);
    expect_no_completion();
    harness_close();
}

/* Freeing: a sealed slot below the floor is freed at RECLAIM, its
 * extents die, the superblock names the new start before the slot is
 * reused, a STORE needing the slot waits for that write, and the reuse
 * rewrites the slot's blocks (the model forgets a freed slot). */
static void test_free_segments(void)
{
    struct config c = base_config();
    struct vsr_io_wire_superblock superblock;
    struct vsr_id x = {0x51, 1};
    struct vsr_io_piece piece;
    uint64_t sequence = 1;
    uint64_t sealed_at;
    uint64_t held;
    uint32_t at;

    c.segments = 2;
    c.max_segments = 2;
    c.write_behind_bytes = c.segment_bytes;
    c.cache_bytes += c.segment_bytes;
    harness_open(&c);
    harness_start(VSR_START_NEW);
    while (h.store->current == 0) {
        expect_completion(submit(txn_append(sequence, 4, 128)), VSR_IO_OK);
        sequence++;
    }
    harness_run();
    sealed_at = h.store->segments[0].last_sequence;
    CHECK(vsr_io_store_hot(h.store, slot_offset(0) + h.header_bytes,
                           (uint32_t)txns[1].bytes, &piece));
    /* Nothing frees while the floors keep slot 0. */
    vsr_io_store_free_segments(h.store);
    CHECK(h.store->segments[0].number == 1);
    expect_completion(submit(txn_trim(sequence, h.store->log_end)), VSR_IO_OK);
    sequence++;
    expect_completion(submit_reclaim(sequence), VSR_IO_OK);
    CHECK(h.store->segments[0].number == 1); /* Client base 0. */
    vsr_io_store_base_set(h.store, x, sequence - 1);
    CHECK(vsr_io_store_free_floor(h.store) == sequence);
    CHECK(sealed_at < sequence);
    vsr_io_store_free_segments(h.store);
    CHECK(h.store->segments[0].number == 0 &&
          h.store->segments[0].phase == VSR_IO_SEGMENT_FREEING);
    CHECK(h.store->start_segment == 2 && h.store->start_slot == 1);
    CHECK(h.store->superblock_dirty == 1);
    CHECK(!vsr_io_store_hot(h.store, slot_offset(0) + h.header_bytes,
                            (uint32_t)txns[1].bytes, &piece));
    disk_forget_slot(0, c.segment_bytes);
    /* Fill slot 1: the STORE that needs a slot waits for the superblock
     * write, then reuses slot 0 as segment 3. */
    held = 0;
    while (h.store->stores_count == 0) {
        uint64_t op = submit(txn_append(sequence, 4, 128));

        if (h.store->stores_count == 0) {
            expect_completion(op, VSR_IO_OK);
        } else {
            held = op;
        }
        sequence++;
        CHECK(sequence < 128);
    }
    CHECK(h.store->state == VSR_IO_STORE_READY && held != 0);
    at = NONE;
    while (at == NONE) {
        CHECK(harness_prepare() >= 1);
        for (uint32_t i = 0; i < PENDING_MAX; ++i) {
            if (h.pending[i].state == 1 &&
                h.pending[i].sqe.opcode == VSR_IO_SQE_WRITE &&
                h.pending[i].sqe.offset < 2 * BLOCK) {
                at = i;
            }
        }
        if (at == NONE) {
            harness_complete(pending_of(VSR_IO_SQE_WRITE));
        }
    }
    harness_poll();
    expect_no_completion();
    harness_complete(at); /* The slot frees and is reused at once. */
    expect_completion(held, VSR_IO_OK);
    CHECK(h.store->current == 0 && h.store->segments[0].number == 3);
    CHECK(h.store->segments[0].phase == VSR_IO_SEGMENT_HEADER);
    harness_run();
    read_superblock(h.store->superblock_next == 0 ? 1 : 0, &superblock);
    CHECK(superblock.start_segment == 2 && superblock.start_slot == 1);
    CHECK(scan_records(slot_offset(0) + h.header_bytes, h.store->file_head,
                       sequence - 1) == sequence - 1);
    expect_no_completion();
    harness_close();
}

/* A capture's snapshot and offsets, PUBLISH of the latest capture making
 * it the base, a CLIENT load from the base file, then the held
 * transactions: a PUBLISH of a foreign snapshot merging its file, a
 * RESTORE replacing the table, a base load that fails. */
static void test_capture_base(void)
{
    struct config c = base_config();
    struct vsr_id ids[2] = {{0xA, 1}, {0xB, 1}};
    struct vsr_id x = {0x51, 1};
    struct vsr_id y = {0x52, 1};
    struct vsr_id z = {0x53, 1};
    struct vsr_id w = {0x54, 1};
    struct vsr_id v = {0x55, 1};
    struct vsr_id defaults[2] = {{0x1000, 1}, {0x1001, 1}};
    uint64_t numbers[2] = {1, 1};
    uint64_t ops[2] = {1, 2};
    struct vsr_io_client_snapshot out[8];
    struct vsr_client_record records[2];
    struct vsr_io_wire_client_record wire;
    uint64_t offsets[2] = {0, 0};
    uint64_t header_bytes;
    uint64_t max_record;
    uint64_t sequence;
    uint64_t op;
    uint64_t wanted_sequence = 0;
    const struct vsr_loaded *loaded;
    const struct vsr_client_record *record;
    struct vsr_id wanted;
    struct vsr_io_piece piece;
    uint32_t lease = NONE;
    uint32_t at;

    derive(&header_bytes, &max_record);
    c.segment_bytes = round_up(header_bytes + 64 * max_record, BLOCK);
    harness_open(&c);
    harness_start(VSR_START_NEW);
    expect_completion(submit(txn_identity(1, VSR_MEMBER_FULL)), VSR_IO_OK);
    expect_completion(submit(txn_clients(2, 2, ids, numbers, ops, 8)),
                      VSR_IO_OK);
    expect_completion(submit(txn_append_client(3, 1, 16, ids[0], 2)),
                      VSR_IO_OK);
    CHECK(vsr_io_store_snapshot_clients(h.store, out, 8) == 2);
    CHECK(vsr_io_store_snapshot_clients(h.store, out, 1) == 2);
    CHECK(h.store->capture_floor == 2);
    for (uint32_t i = 0; i < 2; ++i) {
        records[i] = txns[2].records[out[i].id.hi == 0xA ? 0 : 1];
        CHECK(out[i].record.sequence == 2 &&
              out[i].record.offset == client_of(out[i].id)->current.offset);
    }
    base_file_write(records, 2, offsets);
    for (uint32_t i = 0; i < 2; ++i) {
        vsr_io_store_capture_offset(h.store, out[i].id, offsets[i]);
    }
    vsr_io_store_capture_end(h.store, x, 3);
    expect_completion(submit(txn_publish(4, x, 1)), VSR_IO_OK); /* No wait. */
    CHECK(h.store->client_base == 3 && h.store->client_base_id.hi == 0x51);
    CHECK(client_of(out[0].id)->base_offset == offsets[0]);
    CHECK(h.store->anchor.id.hi == 0x51 && h.store->anchor.op == 1);
    CHECK(h.store->anchor.manifest.size == 16 &&
          memcmp(h.store->anchor.manifest.spans[0].data, txns[4].manifest_bytes,
                 16) == 0);
    /* The record is still hot: read from the ring. */
    op = load_client(4, ids[0]);
    loaded = expect_loaded(op, VSR_IO_OK, &lease);
    CHECK(loaded->count == 1 && h.store->pins[lease].begin != UINT64_MAX);
    release_lease(lease);
    /* Once evicted: from the base file through its slot. */
    sequence = 5;
    while (vsr_io_store_hot(h.store, client_of(ids[0])->current.offset,
                            client_of(ids[0])->current.length, &piece)) {
        expect_completion(submit(txn_append(sequence, 4, 200)), VSR_IO_OK);
        harness_run();
        sequence++;
        CHECK(sequence < 256);
    }
    op = load_client(sequence - 1, ids[1]);
    CHECK(harness_prepare() == 1);
    at = pending_of(VSR_IO_SQE_READ);
    CHECK(at != NONE && h.pending[at].sqe.fd == BASE_SLOT);
    CHECK(h.pending[at].sqe.offset <= offsets[1] &&
          h.pending[at].sqe.offset + h.pending[at].sqe.length > offsets[1]);
    harness_complete(at);
    loaded = expect_loaded(op, VSR_IO_OK, &lease);
    record = loaded->items;
    CHECK(record->request.client.hi == 0xB && record->request.number == 1 &&
          record->op == 2 && record->result.data.size == 8);
    CHECK(in_pool(record->result.data.spans[0].data, 8));
    release_lease(lease);
    /* A foreign PUBLISH waits for its file: b's newer record comes from
     * it, a keeps the log's newer one and lowers the base below it. */
    numbers[0] = 3;
    ops[0] = 7;
    expect_completion(submit(txn_clients(sequence, 1, ids, numbers, ops, 8)),
                      VSR_IO_OK);
    sequence++;
    op = submit(txn_publish(sequence, y, 3));
    expect_no_completion();
    CHECK(h.store->stores_count == 1);
    CHECK(vsr_io_store_base_wanted(h.store, &wanted, &wanted_sequence));
    CHECK(wanted.hi == 0x52 && wanted_sequence == sequence);
    vsr_io_store_base_begin(h.store);
    CHECK(!vsr_io_store_base_wanted(h.store, &wanted, NULL));
    memset(&wire, 0, sizeof(wire));
    wire.client_hi = 0xB;
    wire.client_lo = 1;
    wire.number = 4;
    wire.op = 9;
    CHECK(vsr_io_store_base_record(h.store, &wire, 300) == VSR_OK);
    wire.client_hi = 0xA;
    wire.number = 1; /* Older than a's record: not covered. */
    wire.op = 1;
    CHECK(vsr_io_store_base_record(h.store, &wire, 400) == VSR_OK);
    wire.number = 0;
    CHECK(vsr_io_store_base_record(h.store, &wire, 500) == VSR_EINVAL);
    vsr_io_store_base_end(h.store, y, sequence);
    expect_no_completion();
    vsr_io_store_base_resume(h.store, VSR_IO_OK);
    expect_completion(op, VSR_IO_OK);
    CHECK(h.store->client_base_id.hi == 0x52 &&
          h.store->client_base == sequence - 2);
    CHECK(client_of(ids[1])->current.number == 4 &&
          client_of(ids[1])->current.sequence == 0 &&
          client_of(ids[1])->base_offset == 300);
    CHECK(client_of(ids[0])->current.number == 3 &&
          client_of(ids[0])->base_offset == UINT64_MAX);
    sequence++;
    /* A RESTORE replaces the table with the file's and rebuilds the
     * retained entries from the log above the checkpoint. */
    op = submit(txn_restore(sequence, z, 2, VSR_MEMBER_FULL));
    CHECK(vsr_io_store_base_wanted(h.store, &wanted, NULL) &&
          wanted.hi == 0x53);
    vsr_io_store_base_begin(h.store);
    wire.client_hi = 0xB;
    wire.number = 5;
    wire.op = 11;
    CHECK(vsr_io_store_base_record(h.store, &wire, 600) == VSR_OK);
    vsr_io_store_base_end(h.store, z, sequence);
    vsr_io_store_base_resume(h.store, VSR_IO_OK);
    expect_completion(op, VSR_IO_OK);
    CHECK(h.store->log_begin == 3 && h.store->restored == sequence);
    CHECK(client_of(ids[1])->current.number == 5 &&
          client_of(ids[1])->base_offset == 600);
    CHECK(client_of(ids[0]) == NULL); /* Its only entry, op 1, is gone. */
    CHECK(client_of(defaults[0])->retained != 0 &&
          client_of(defaults[0])->current.number == 0);
    CHECK(h.store->clients_count == 3);
    CHECK(h.store->client_base == sequence && h.store->anchor.op == 2);
    sequence++;
    /* A witness needs no file: the table keeps only retained entries. */
    op = submit(txn_restore(sequence, w, 2, VSR_MEMBER_WITNESS));
    expect_completion(op, VSR_IO_OK);
    CHECK(client_of(ids[1]) == NULL && h.store->clients_count == 2);
    CHECK(client_of(defaults[1])->retained == h.store->log_end - 1);
    sequence++;
    expect_completion(submit(txn_hard(sequence, VSR_MEMBER_FULL)), VSR_IO_OK);
    sequence++;
    /* A base load that fails fails the transaction and fences. */
    op = submit(txn_publish(sequence, v, 3));
    CHECK(vsr_io_store_base_wanted(h.store, &wanted, NULL));
    vsr_io_store_base_resume(h.store, VSR_IO_CORRUPT);
    expect_completion(op, VSR_IO_CORRUPT);
    CHECK(h.store->state == VSR_IO_STORE_FAILED);
    harness_close();
}

/* The logical state reaches later segment headers and the superblock:
 * identity, hard state and the anchor. */
static void test_state_headers(void)
{
    struct config c = base_config();
    struct vsr_io_wire_superblock superblock;
    struct vsr_io_wire_segment fixed;
    struct vsr_store_identity identity;
    struct vsr_hard_state hard;
    struct vsr_checkpoint *checkpoint = NULL;
    struct vsr_io_bump region;
    struct vsr_id x = {0x51, 1};
    uint64_t sequence = 3;
    uint32_t at;

    c.write_behind_bytes = c.segment_bytes;
    c.cache_bytes += c.segment_bytes;
    harness_open(&c);
    harness_start(VSR_START_NEW);
    expect_completion(submit(txn_identity(1, VSR_MEMBER_WITNESS)), VSR_IO_OK);
    CHECK(h.store->identity_set && h.store->hard.role == VSR_MEMBER_WITNESS);
    CHECK(h.store->superblock_dirty == 1);
    expect_completion(submit(txn_publish(2, x, 7)), VSR_IO_OK);
    CHECK(harness_prepare() >= 1);
    at = pending_of(VSR_IO_SQE_WRITE);
    while (h.pending[at].sqe.offset >= 2 * BLOCK) {
        harness_complete(at);
        harness_prepare();
        at = pending_of(VSR_IO_SQE_WRITE);
        CHECK(at != NONE);
    }
    harness_complete(at);
    read_superblock(h.pending[at].sqe.offset == 0 ? 0 : 1, &superblock);
    CHECK(superblock.cluster_hi == 0x77 && superblock.cluster_lo == 0x99 &&
          superblock.replica == 3);
    while (h.store->current == 0) {
        expect_completion(submit(txn_append(sequence, 4, 128)), VSR_IO_OK);
        sequence++;
        CHECK(sequence < 64);
    }
    harness_run();
    vsr_io_bump_init(&region, lease_memory, LEASE_BYTES);
    CHECK(vsr_io_codec_get_segment(
              disk_image + slot_offset(1), (size_t)h.header_bytes, &limits,
              &region, &fixed, &identity, &hard, &checkpoint) == VSR_OK);
    CHECK(identity.cluster.hi == 0x77 && identity.replica == 3);
    CHECK(hard.role == VSR_MEMBER_WITNESS && hard.epoch != NULL &&
          hard.epoch->current->count == 1);
    CHECK(checkpoint != NULL && checkpoint->id.hi == 0x51 &&
          checkpoint->op == 7 && checkpoint->manifest.size == 16);
    harness_close();
}

/* A change descriptor reaching past its record (the codec bounds neither
 * offset + length nor the descriptor area) is CORRUPT when the record is
 * read back. */
static void test_bad_descriptor(void)
{
    struct config c = base_config();
    uint64_t header_bytes;
    uint64_t max_record;
    uint64_t data;
    uint64_t sequence;
    uint64_t op;
    uint32_t lease = NONE;
    unsigned char *record;
    uint32_t length;

    derive(&header_bytes, &max_record);
    c.segment_bytes = round_up(header_bytes + 64 * max_record, BLOCK);
    harness_open(&c);
    harness_start(VSR_START_NEW);
    data = 2 * BLOCK + h.header_bytes;
    expect_completion(submit(txn_append(1, 1, 64)), VSR_IO_OK);
    harness_run();
    record = disk_image + data;
    length = vsr_io_get_u32(record + 4);
    CHECK(length == txns[1].bytes);
    /* Descriptor 0's length becomes the whole record: offset + length
     * overshoots. Both CRCs are made good again. */
    vsr_io_put_u32(record + 48 + 20, length);
    vsr_io_put_u32(record + 40, vsr_io_crc32c(0, record + 48, length - 48));
    vsr_io_put_u32(record + 44, vsr_io_crc32c(0, record, 44));
    sequence = evict_first(2);
    op = load_log(sequence - 1, 1, 2, 1, limits.message_bytes);
    CHECK(harness_prepare() == 1);
    harness_complete(pending_of(VSR_IO_SQE_READ));
    expect_loaded(op, VSR_IO_CORRUPT, &lease);
    CHECK(h.store->state == VSR_IO_STORE_READY);
    expect_no_completion();
    harness_close();
}

/* Names the test that fails. */
#define RUN(test)                                                              \
    do {                                                                       \
        fprintf(stderr, "store: %s\n", #test);                                 \
        test();                                                                \
    } while (0)

int main(void)
{
    uint64_t header_bytes;
    uint64_t max_record;

    derive(&header_bytes, &max_record);
    printf("store: header %" PRIu64 " bytes, record limit %" PRIu64 "\n",
           header_bytes, max_record);
    RUN(test_check);
    RUN(test_create);
    RUN(test_recover_missing);
    RUN(test_open_existing);
    RUN(test_store_packs);
    RUN(test_write_behind);
    RUN(test_sync_fdatasync);
    RUN(test_sync_dsync);
    RUN(test_sync_delay);
    RUN(test_segments);
    RUN(test_growth);
    RUN(test_wrap);
    RUN(test_pins);
    RUN(test_idle_superblock);
    RUN(test_replicated_flush);
    RUN(test_write_error);
    RUN(test_short_write);
    RUN(test_close);
    RUN(test_empty_index);
    RUN(test_index_ops);
    RUN(test_index_full);
    RUN(test_load_cold);
    RUN(test_clients);
    RUN(test_floor);
    RUN(test_free_segments);
    RUN(test_capture_base);
    RUN(test_state_headers);
    RUN(test_bad_descriptor);
    printf("store: ok\n");
    return 0;
}
