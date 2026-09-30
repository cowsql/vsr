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
#define SLABS 32u
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
    uint32_t stats;     /* STATX calls. */
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
    uint32_t flip_armed; /* The next READ of the log covering flip_offset
                            returns that byte inverted (a transient error
                            the re-read of decision 50 gets past). */
    uint64_t flip_offset;
};

static struct disk disk;
static bool disk_trace; /* The walk's trace: every WRITE and FSYNC. */
static unsigned char base_image[65536];
/* The image as of the last flush: what a crash is sure to keep. */
static unsigned char disk_flushed[DISK_BYTES];

static void disk_reset(void)
{
    memset(&disk, 0, sizeof(disk));
    memset(disk_image, 0, sizeof(disk_image));
    memset(disk_flushed, 0, sizeof(disk_flushed));
    disk.slot = -1;
    disk.base_slot = -1;
}

/* A crash: the instance is abandoned (the next harness_open_keep starts
 * a fresh one over the same image), every block persists, or only the
 * flushed ones when `lose_dirty` (an unflushed block reverts to its
 * content as of the last flush; O_DSYNC writes count as flushed). */
static void disk_crash(bool lose_dirty)
{
    if (lose_dirty) {
        for (uint64_t at = 0; at < DISK_BLOCKS; ++at) {
            if (disk.dirty[at] != 0) {
                memcpy(disk_image + at * BLOCK, disk_flushed + at * BLOCK,
                       BLOCK);
            }
        }
    }
    memset(disk.dirty, 0, sizeof(disk.dirty));
    disk.slot = -1;
    disk.flags = 0;
    disk.base_slot = -1;
    disk.writes = 0;
    disk.flushes = 0;
    disk.fallocates = 0;
    disk.reads = 0;
    disk.stats = 0;
    disk.tear_armed = 0;
    disk.fail_write = 0;
    disk.fail_flush = 0;
    disk.fail_read = 0;
    disk.short_read = 0;
    disk.flip_armed = 0;
}

/* One block written since the last flush did not persist. */
static void disk_lose_block(uint64_t at)
{
    if (disk_trace) {
        fprintf(stderr, "disk: lose block %" PRIu64 "\n", at);
    }
    CHECK(disk.dirty[at] != 0);
    memcpy(disk_image + at * BLOCK, disk_flushed + at * BLOCK, BLOCK);
    disk.dirty[at] = 0;
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
static const struct vsr_io_op_ref *op_ref(uint64_t op);
static uint64_t slot_offset(uint32_t slot);

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

/* A fresh instance over the engine tables; `keep` leaves the disk image
 * and the transaction table as they are (a restart after a crash). */
static void harness_open_keep(const struct config *c, bool keep)
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
    if (!keep) {
        txns_reset();
        disk_reset();
    }
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

static void harness_open(const struct config *c)
{
    harness_open_keep(c, false);
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

/* The model's rule for slot reuse: a slot's never-rewrite bits are
 * forgotten once the store freed it (FREEING observed before the
 * superblock write that lets it be reused), never for a live slot. */
static void disk_track_freeing(void)
{
    for (uint32_t slot = 0; slot < h.store->slots; ++slot) {
        if (h.store->segments[slot].phase == VSR_IO_SEGMENT_FREEING ||
            h.store->segments[slot].phase == VSR_IO_SEGMENT_FLUSHING) {
            disk_forget_slot(slot, h.options.segment_bytes);
        }
    }
}

/* Collects the SQEs of one prepare into the pending table. */
static uint32_t harness_prepare(void)
{
    struct vsr_io_sqe sqes[PENDING_MAX];
    uint32_t count = 0;

    disk_track_freeing();
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
        if (sqe->offset >= 2 * BLOCK) {
            /* A segment's bytes come from the ring, contiguous in it (the
             * superblocks follow the ring in the tail region). */
            const unsigned char *at = sqe->addr;

            CHECK(at >= h.store->ring &&
                  (uint64_t)(at - h.store->ring) + sqe->length <=
                      h.store->ring_size);
        }
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
        if (disk_trace) {
            fprintf(stderr, "disk: write %" PRIu64 "+%u%s\n", sqe->offset,
                    sqe->length, disk.tear_armed != 0 ? " (torn)" : "");
        }
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
            if ((disk.flags & O_DSYNC) != 0) {
                memcpy(disk_flushed + from, disk_image + from, to - from);
            }
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
        if (disk_trace) {
            fprintf(stderr, "disk: flush\n");
        }
        if (disk.fail_flush != 0) {
            *result = disk.fail_flush;
            disk.fail_flush = 0;
            return true;
        }
        memset(disk.dirty, 0, sizeof(disk.dirty));
        memcpy(disk_flushed, disk_image, sizeof(disk_flushed));
        *result = 0;
        return true;
    case VSR_IO_SQE_STATX: {
        struct statx *out;

        CHECK(sqe->fd == AT_FDCWD && sqe->flags == 0);
        CHECK(strcmp(sqe->addr, DIRECTORY "/log") == 0);
        CHECK((sqe->length & STATX_SIZE) != 0 && sqe->addr2 != NULL);
        out = (struct statx *)(void *)writable(sqe->addr2, sizeof(*out));
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
        if (disk.flip_armed != 0 && image == disk_image &&
            disk.flip_offset >= sqe->offset && disk.flip_offset < end) {
            writable(sqe->addr, sqe->length)[disk.flip_offset - sqe->offset] ^=
                0xFF;
            disk.flip_armed = 0;
        }
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
    disk_track_freeing();
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
    disk.flushes = 0; /* The tests count the flushes after the creation's. */
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
    size_t bytes;         /* Encoded record bytes. */
    uint64_t file_offset; /* Where it was packed, when submit saw it. */
};

static struct txn txns[TXN_MAX];

static void feeds_reset(void);

/* Mixed into the bytes of every transaction built after it is set: the
 * random walk sets one value per run, so that a run's transaction at a
 * sequence differs from the one an older run packed there and no torn
 * rewrite splices the two into one CRC-valid record the model cannot
 * know (a splice with the next record is decision 116's). 0 otherwise. */
static uint32_t txn_salt;

static void txns_reset(void)
{
    memset(txns, 0, sizeof(txns));
    txn_salt = 0;
    feeds_reset();
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
                (unsigned char)(sequence * 31u + (uint64_t)i * 7u + b +
                                (uint64_t)txn_salt * 101u);
        }
        t->spans[i].data = t->bodies[i];
        t->spans[i].size = body;
        t->blobs[i].spans = body > 0 ? &t->spans[i] : NULL;
        t->blobs[i].size = body;
        t->blobs[i].count = body > 0 ? 1 : 0;
        entry->op = first + i;
        entry->epoch = 0;
        entry->view = 1 + txn_salt;
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
            t->results[i][b] = (unsigned char)(numbers[i] * 13u + b +
                                               (uint64_t)txn_salt * 101u);
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
    if (h.store->readable == t->store.sequence) {
        /* Packed at once: its file offset for the tests that damage it. */
        txns[t->store.sequence].file_offset = h.store->file_head - t->bytes;
    }
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
 * Recovery harness: the base file the snapshot module would load, the
 * restart, index snapshots compared after recovery, and the script that
 * builds a log image (phase 4's drivers start from these).
 * ---------------------------------------------------------------------- */

/* The clients files as the snapshot module would feed them at a base
 * load: per snapshot id, the records of its capture (or a file the test
 * composed) with their offsets; a wanted id without a file is CORRUPT,
 * as a missing file is. `feed` records the last load. */
#define FEEDS 256u

struct base_feed {
    struct vsr_id id; /* Zero: free. */
    struct vsr_client_record records[16];
    uint64_t offsets[16];
    uint32_t count;
};

static struct base_feed feeds[FEEDS];
static struct {
    bool fed; /* A load was wanted and fed since the last recover. */
    int32_t status;
    struct vsr_id id;
    uint64_t sequence;
} feed;

static void feeds_reset(void)
{
    memset(feeds, 0, sizeof(feeds));
    memset(&feed, 0, sizeof(feed));
}

static struct base_feed *feed_of(struct vsr_id id, bool create)
{
    for (uint32_t i = 0; i < FEEDS; ++i) {
        if (feeds[i].id.hi == id.hi && feeds[i].id.lo == id.lo) {
            return &feeds[i];
        }
    }
    if (!create) {
        return NULL;
    }
    for (uint32_t i = 0; i < FEEDS; ++i) {
        if (feeds[i].id.hi == 0 && feeds[i].id.lo == 0) {
            feeds[i].id = id;
            return &feeds[i];
        }
    }
    CHECK(false);
    return NULL;
}

/* Acts as the snapshot module for the base load the store wants. */
static void feed_base(void)
{
    const struct base_feed *file;

    CHECK(vsr_io_store_base_wanted(h.store, &feed.id, &feed.sequence));
    feed.fed = true;
    file = feed_of(feed.id, false);
    feed.status = file == NULL ? VSR_IO_CORRUPT : VSR_IO_OK;
    if (feed.status != VSR_IO_OK) {
        vsr_io_store_base_resume(h.store, feed.status);
        return;
    }
    vsr_io_store_base_begin(h.store);
    for (uint32_t i = 0; i < file->count; ++i) {
        const struct vsr_client_record *record = &file->records[i];
        struct vsr_io_wire_client_record wire;

        memset(&wire, 0, sizeof(wire));
        wire.client_hi = record->request.client.hi;
        wire.client_lo = record->request.client.lo;
        wire.number = record->request.number;
        wire.op = record->op;
        wire.length = (uint32_t)record->result.data.size;
        CHECK(vsr_io_store_base_record(h.store, &wire, file->offsets[i]) ==
              VSR_OK);
    }
    vsr_io_store_base_end(h.store, feed.id, feed.sequence);
    vsr_io_store_base_resume(h.store, VSR_IO_OK);
}

/* A capture of the table at the current sequence under snapshot `id`, as
 * the snapshot module does it: every completed record goes to the base
 * file and to the feed, each entry learns its offset. */
static void harness_capture(struct vsr_id id)
{
    struct vsr_io_client_snapshot out[16];
    struct base_feed *file = feed_of(id, true);
    uint32_t count = vsr_io_store_snapshot_clients(h.store, out, 16);
    uint32_t n = 0;

    CHECK(count <= 16);
    for (uint32_t i = 0; i < count; ++i) {
        const struct vsr_io_client_version *record = &out[i].record;
        const struct base_feed *previous;

        if (record->number == 0) {
            continue;
        }
        if (record->sequence != 0) {
            file->records[n] = txns[record->sequence].records[record->index];
        } else {
            bool found = false;

            /* Only the current base file has it. */
            previous = feed_of(h.store->client_base_id, false);
            CHECK(previous != NULL);
            for (uint32_t j = 0; j < previous->count; ++j) {
                if (previous->records[j].request.client.hi == out[i].id.hi &&
                    previous->records[j].request.client.lo == out[i].id.lo) {
                    file->records[n] = previous->records[j];
                    found = true;
                }
            }
            CHECK(found);
        }
        CHECK(file->records[n].request.number == record->number);
        n++;
    }
    file->count = n;
    base_file_write(file->records, n, file->offsets);
    for (uint32_t i = 0; i < n; ++i) {
        vsr_io_store_capture_offset(h.store, file->records[i].request.client,
                                    file->offsets[i]);
    }
    vsr_io_store_capture_end(h.store, id, h.store->readable);
}

/* The disk model after a recovery: the blocks the store may write again
 * lose their never-rewrite bit (every free slot; the current slot from
 * where writing resumes), every other block of a live slot keeps it. */
static void disk_after_recovery(void)
{
    memset(disk.once, 0, sizeof(disk.once));
    for (uint32_t slot = 0; slot < h.store->slots; ++slot) {
        const struct vsr_io_segment *segment = &h.store->segments[slot];
        uint64_t first = slot_offset(slot) / BLOCK;
        uint64_t count = h.options.segment_bytes / BLOCK;

        if (segment->number == 0) {
            continue;
        }
        if (slot == h.store->current) {
            CHECK(h.store->file_head >= slot_offset(slot) + h.header_bytes);
            count = (h.store->file_head - slot_offset(slot)) / BLOCK;
        }
        memset(disk.once + first, 1, count);
    }
}

/* Opens the log under `mode` and drives the recovery to its completion,
 * standing in for the snapshot module when the anchor's file is wanted.
 * Returns the RECOVERY load's status, with the row and its lease when
 * OK. */
static int32_t harness_recover(uint32_t mode, const struct vsr_loaded **loaded,
                               uint32_t *lease)
{
    struct vsr_store_read read;
    struct vsr_io_completion completion;
    uint64_t op = next_op();

    memset(&read, 0, sizeof(read));
    read.type = VSR_LOAD_RECOVERY;
    feed.fed = false;
    vsr_io_store_open(h.store, mode, op, &read);
    CHECK(h.store->state == VSR_IO_STORE_OPENING);
    harness_run();
    if (vsr_io_store_base_wanted(h.store, NULL, NULL)) {
        feed_base();
        harness_run();
    }
    CHECK(next_completion(&completion));
    CHECK(completion.op == op);
    *loaded = NULL;
    *lease = NONE;
    if (completion.status == VSR_IO_OK) {
        CHECK(completion.lease < REGIONS && completion.data != NULL);
        *loaded = completion.data;
        *lease = completion.lease;
    } else {
        CHECK(completion.lease == NONE && completion.data == NULL);
    }
    expect_no_completion();
    if (h.store->state == VSR_IO_STORE_READY) {
        disk_after_recovery();
    } else {
        CHECK(h.store->state == VSR_IO_STORE_FAILED);
    }
    return completion.status;
}

/* An index snapshot, taken after every scripted sequence and compared
 * with the state recovered at that sequence. */
#define SNAP_OPS 256u
#define SNAP_CLIENTS 16u
#define SNAPSHOTS 512u

struct snap_client {
    struct vsr_id id;
    uint64_t number;
    uint64_t op;
    uint64_t retained;
};

struct snapshot {
    uint64_t log_begin;
    uint64_t log_end;
    uint64_t clients_sequence;
    uint64_t anchor_op;
    uint64_t hard_view;
    uint64_t hard_committed;
    struct vsr_id anchor_id;
    struct vsr_id client_base_id;
    struct vsr_store_identity identity;
    struct snap_client clients[SNAP_CLIENTS];
    struct vsr_io_op_ref ops[SNAP_OPS]; /* [log_begin, log_end) */
    uint32_t hard_role;
    uint32_t manifest_size;
    uint32_t ops_count;
    uint32_t clients_count;
    bool taken;
    bool identity_set;
    unsigned char manifest[64];
};

static struct snapshot snapshots[SNAPSHOTS];

static const struct vsr_io_op_ref *op_ref(uint64_t op);
static const struct vsr_io_client *client_of(struct vsr_id id);

static void snapshot_take(void)
{
    struct snapshot *s;

    CHECK(h.store->readable < SNAPSHOTS);
    s = &snapshots[h.store->readable];
    memset(s, 0, sizeof(*s));
    s->taken = true;
    s->identity_set = h.store->identity_set;
    s->identity = h.store->identity;
    s->log_begin = h.store->log_begin;
    s->log_end = h.store->log_end;
    s->clients_sequence = h.store->clients_sequence;
    s->anchor_id = h.store->anchor.id;
    s->anchor_op = h.store->anchor.op;
    s->client_base_id = h.store->client_base_id;
    s->hard_view = h.store->hard.view;
    s->hard_committed = h.store->hard.committed;
    s->hard_role = h.store->hard.role;
    s->manifest_size = (uint32_t)h.store->anchor.manifest.size;
    CHECK(s->manifest_size <= sizeof(s->manifest));
    for (uint32_t i = 0; i < h.store->anchor.manifest.count; ++i) {
        memcpy(s->manifest, h.store->anchor.manifest.spans[i].data,
               h.store->anchor.manifest.spans[i].size);
    }
    CHECK(s->log_end - s->log_begin <= SNAP_OPS);
    for (uint64_t op = s->log_begin; op < s->log_end; ++op) {
        const struct vsr_io_op_ref *ref = op_ref(op);

        if (ref->op == op) {
            s->ops[s->ops_count] = *ref;
        } else {
            memset(&s->ops[s->ops_count], 0, sizeof(s->ops[0]));
        }
        s->ops_count++;
    }
    for (uint32_t i = 0; i < h.store->clients_capacity; ++i) {
        const struct vsr_io_client *entry = &h.store->clients[i];
        struct snap_client *c;

        if (entry->id.hi == 0 && entry->id.lo == 0) {
            continue;
        }
        CHECK(s->clients_count < SNAP_CLIENTS);
        c = &s->clients[s->clients_count++];
        c->id = entry->id;
        c->number = entry->current.number;
        c->op = entry->current.op;
        c->retained = entry->retained;
    }
}

/* The recovered indexes must agree with the snapshot of `sequence` on
 * the live log, every client's record and retained entry, and the
 * logical state; the client base must honour its invariant. Versions are
 * not compared: every one in the snapshot was truncated at or before
 * `sequence`, and a recovered store serves no older revision, so a
 * RECLAIM between the snapshot and the crash may have dropped them. */
static void snapshot_check(uint64_t sequence)
{
    const struct snapshot *s = &snapshots[sequence];

    CHECK(s->taken && h.store->readable == sequence);
    CHECK(h.store->identity_set == s->identity_set);
    CHECK(memcmp(&h.store->identity, &s->identity, sizeof(s->identity)) == 0);
    CHECK(h.store->log_begin == s->log_begin);
    CHECK(h.store->log_end == s->log_end);
    CHECK(h.store->retained_begin <= s->log_begin);
    CHECK(h.store->clients_sequence <= s->clients_sequence);
    CHECK(h.store->anchor.id.hi == s->anchor_id.hi &&
          h.store->anchor.id.lo == s->anchor_id.lo);
    CHECK(h.store->anchor.op == s->anchor_op);
    CHECK(h.store->anchor.manifest.size == s->manifest_size);
    if (s->manifest_size > 0) {
        CHECK(h.store->anchor.manifest.count == 1 &&
              memcmp(h.store->anchor.manifest.spans[0].data, s->manifest,
                     s->manifest_size) == 0);
    }
    CHECK(h.store->client_base_id.hi == s->client_base_id.hi &&
          h.store->client_base_id.lo == s->client_base_id.lo);
    CHECK(h.store->hard.view == s->hard_view &&
          h.store->hard.committed == s->hard_committed &&
          h.store->hard.role == s->hard_role);
    CHECK((h.store->hard.epoch != NULL) == s->identity_set);
    for (uint32_t i = 0; i < s->ops_count; ++i) {
        const struct vsr_io_op_ref *expected = &s->ops[i];
        const struct vsr_io_op_ref *ref = op_ref(s->log_begin + i);

        if (expected->op == 0) {
            CHECK(ref->op != s->log_begin + i);
            continue;
        }
        if (ref->op != expected->op || ref->sequence != expected->sequence) {
            fprintf(stderr,
                    "snapshot %" PRIu64 ": op %" PRIu64 " sequence %" PRIu64
                    " recovered as op %" PRIu64 " sequence %" PRIu64 "\n",
                    sequence, expected->op, expected->sequence, ref->op,
                    ref->sequence);
        }
        CHECK(ref->op == expected->op && ref->sequence == expected->sequence);
        CHECK(ref->offset == expected->offset &&
              ref->length == expected->length);
        CHECK(ref->change == expected->change && ref->index == expected->index);
        CHECK(ref->client.hi == expected->client.hi &&
              ref->client.lo == expected->client.lo);
    }
    for (uint32_t i = 0; i < s->clients_count; ++i) {
        const struct snap_client *expected = &s->clients[i];
        const struct vsr_io_client *entry = client_of(expected->id);

        if (expected->number == 0 && expected->retained == 0) {
            continue; /* An in-flight entry: the crash forgot it. */
        }
        CHECK(entry != NULL);
        if (entry->current.number != expected->number ||
            entry->current.op != expected->op) {
            fprintf(stderr,
                    "snapshot %" PRIu64 ": client %" PRIx64 " number %" PRIu64
                    " op %" PRIu64 " recovered as number %" PRIu64
                    " op %" PRIu64 " (sequence %" PRIu64
                    ", base offset %" PRIu64 ")\n",
                    sequence, expected->id.hi, expected->number, expected->op,
                    entry->current.number, entry->current.op,
                    entry->current.sequence, entry->base_offset);
        }
        CHECK(entry->current.number == expected->number &&
              entry->current.op == expected->op);
        CHECK(entry->retained == expected->retained);
    }
    for (uint32_t i = 0; i < h.store->clients_capacity; ++i) {
        const struct vsr_io_client *entry = &h.store->clients[i];
        bool found = false;

        if (entry->id.hi == 0 && entry->id.lo == 0) {
            continue;
        }
        for (uint32_t j = 0; j < s->clients_count; ++j) {
            if (s->clients[j].id.hi == entry->id.hi &&
                s->clients[j].id.lo == entry->id.lo) {
                found = true;
            }
        }
        CHECK(found && entry->inflight == 0);
        /* The base invariant: a record at or below the base is in the
         * file. */
        if (entry->current.number != 0 &&
            entry->current.sequence <= h.store->client_base) {
            CHECK(entry->base_offset != UINT64_MAX);
        }
    }
}

/* The script: a deterministic transaction per sequence covering every
 * change type at a fixed cadence (a TRUNCATE at 0, a TRIM at 3, CLIENTS
 * at 5, HARD_STATE at 7, a client's APPEND at 8, plain APPENDs
 * otherwise), with an own capture at 1 before the PUBLISH at 2 of every
 * ten sequences from 12. Phase 4's drivers may run it to any length to
 * build a log image. */
#define SCRIPT_LENGTH 44u

static const struct vsr_id script_a = {0xA, 1};
static const struct vsr_id script_b = {0xB, 1};

static struct vsr_id script_snapshot(uint64_t publish_sequence)
{
    struct vsr_id id = {0x50 + publish_sequence / 10, 1};

    return id;
}

/* The log begin after the transactions below `sequence`. */
static uint64_t log_begin_before(uint64_t sequence)
{
    uint64_t begin = 1;

    for (uint64_t q = 1; q < sequence; ++q) {
        const struct txn *t = &txns[q];

        for (uint32_t i = 0; i < t->store.count; ++i) {
            const struct vsr_change *change = &t->store.changes[i];

            if (change->type == VSR_STORE_TRIM) {
                begin = change->first;
            } else if (change->type == VSR_STORE_RESTORE_CHECKPOINT) {
                begin = t->checkpoint.op + 1;
            }
        }
    }
    return begin;
}

static const struct txn *script_txn(uint64_t sequence)
{
    uint64_t begin = log_begin_before(sequence);
    uint64_t end = log_end_before(sequence);
    struct vsr_id ids[2];
    uint64_t numbers[2] = {sequence, sequence};
    uint64_t ops[2] = {end > 1 ? end - 1 : 1, end > 1 ? end - 1 : 1};

    ids[0] = (sequence / 10) % 2 == 0 ? script_a : script_b;
    ids[1] = script_b;
    if (sequence == 1) {
        return txn_identity(1, VSR_MEMBER_FULL);
    }
    if (sequence == 2) {
        numbers[0] = 1;
        numbers[1] = 1;
        ids[0] = script_a;
        return txn_clients(2, 2, ids, numbers, ops, 8);
    }
    if (sequence % 10 == 2 && sequence >= 12) {
        return txn_publish(sequence, script_snapshot(sequence), end - 1);
    }
    switch (sequence % 10) {
    case 0:
        if (end - 1 > begin) {
            return txn_truncate(sequence, end - 1, 1, 24);
        }
        break;
    case 3:
        if (end > begin + 1) {
            return txn_trim(sequence, end - 1);
        }
        break;
    case 5:
        return txn_clients(sequence, 1, ids, numbers, ops, 16);
    case 7:
        return txn_hard(sequence, VSR_MEMBER_FULL);
    case 8:
        return txn_append_client(sequence, 1 + (uint32_t)(sequence % 3), 32,
                                 ids[0], sequence * 4);
    default:
        break;
    }
    return txn_append(sequence, 1 + (uint32_t)(sequence % 4),
                      (size_t)(sequence * 37 % 200));
}

/* Runs the script from `from` to `to`: each transaction submitted and
 * driven to completion, writes included (a base load fed when wanted); a
 * SYNC every fourth sequence; RECLAIM of the sequence after each (so
 * slots free); the capture before a PUBLISH; an index snapshot after
 * each. */
static void script_run(uint64_t from, uint64_t to)
{
    for (uint64_t sequence = from; sequence <= to; ++sequence) {
        uint64_t op;

        if (sequence % 10 == 1 && sequence >= 11) {
            harness_capture(script_snapshot(sequence + 1));
        }
        op = submit(script_txn(sequence));
        harness_run();
        if (vsr_io_store_base_wanted(h.store, NULL, NULL)) {
            /* A PUBLISH after a restart: the capture is not remembered,
             * so the file is loaded as the snapshot module would. */
            feed_base();
            harness_run();
        }
        expect_completion(op, VSR_IO_OK);
        CHECK(h.store->readable == sequence);
        if (sequence % 4 == 0) {
            op = submit_sync(sequence);
            harness_run();
            expect_completion(op, VSR_IO_OK);
        }
        op = submit_reclaim(sequence);
        expect_completion(op, VSR_IO_OK);
        harness_run();
        expect_no_completion();
        snapshot_take();
    }
}

/* The configuration the script runs in: slots of a few records (each
 * written on its own, so padded to a block), three of them at most (one
 * by growth), a ring that wraps every few records, a write-behind hold
 * of two blocks. */
static struct config script_config(uint8_t sync_mode)
{
    struct config c = base_config();
    uint64_t header_bytes;
    uint64_t max_record;

    derive(&header_bytes, &max_record);
    c.segment_bytes = round_up(header_bytes + 2 * max_record, BLOCK);
    c.segments = 2;
    c.max_segments = 3;
    c.write_behind_bytes = 2 * BLOCK;
    c.cache_bytes =
        round_up(c.write_behind_bytes + limits.pinned_payload_bytes +
                     2 * max_record + 2 * header_bytes + BLOCK,
                 BLOCK);
    c.sync_mode = sync_mode;
    return c;
}

/* The snapshot id of the latest PUBLISH at or below `sequence`, or zero. */
static struct vsr_id script_anchor(uint64_t sequence)
{
    struct vsr_id none = {0, 0};

    if (sequence < 12) {
        return none;
    }
    return script_snapshot((sequence - 12) / 10 * 10 + 12);
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
    CHECK(h.store->segments[0].phase == VSR_IO_SEGMENT_OPEN);
    CHECK(h.store->segments[0].header_write == NONE);
    /* FDATASYNC: the superblocks and the header are flushed before the
     * store is READY; a crash losing them would leave a CORRUPT log. */
    CHECK(h.store->state == VSR_IO_STORE_CREATING);
    expect_no_completion();
    CHECK(harness_prepare() == 1);
    at = pending_of(VSR_IO_SQE_FSYNC);
    CHECK(at != NONE && disk.flushes == 0);
    harness_complete(at);
    CHECK(h.store->state == VSR_IO_STORE_READY && disk.flushes == 1);
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
    /* The flush of the creation (FDATASYNC), then READY. */
    CHECK(h.store->state == VSR_IO_STORE_CREATING);
    CHECK(harness_prepare() == 1);
    harness_complete(pending_of(VSR_IO_SQE_FSYNC));
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
    CHECK(vsr_io_store_free_floor(h.store) == 0); /* Nothing on media. */
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
    /* RECLAIM past the truncation: revision 3 can no longer be named;
     * the versions go once the media reaches the reclaimed revision
     * (decision 112), here with the SYNC of 4. */
    expect_completion(submit_reclaim(4), VSR_IO_OK);
    CHECK(h.store->versions_count == 2 && h.store->reclaim == 4);
    CHECK(h.store->reclaimed == h.store->flushed);
    read = read_of(VSR_LOAD_LOG, 3, a);
    read.end = 6;
    CHECK(vsr_io_store_load(h.store, 71, &read) == VSR_EINVAL);
    op = submit_sync(4);
    harness_run();
    expect_completion(op, VSR_IO_OK);
    CHECK(h.store->versions_count == 0 && h.store->reclaimed == 4);
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

    c.sync_mode = VSR_IO_SYNC_DSYNC; /* Every record on media once written:
                                         the terms apply as they are set
                                         (decision 112). */
    harness_open(&c);
    harness_start(VSR_START_NEW);
    CHECK(vsr_io_store_free_floor(h.store) == 0); /* No RECLAIM yet. */
    expect_completion(submit(txn_append_client(1, 2, 16, a, 1)), VSR_IO_OK);
    expect_completion(submit(txn_append_client(2, 1, 16, b, 1)), VSR_IO_OK);
    harness_run();
    expect_completion(submit(txn_append_client(3, 1, 16, a, 3)), VSR_IO_OK);
    expect_completion(submit_reclaim(2), VSR_IO_OK);
    harness_run();
    CHECK(vsr_io_store_free_floor(h.store) == 1); /* Base 0 + 1. */
    vsr_io_store_base_set(h.store, x, 3); /* No records: nothing lower. */
    CHECK(h.store->client_base == 3);
    CHECK(vsr_io_store_free_floor(h.store) == 1); /* Op 1's record. */
    expect_completion(submit(txn_trim(4, 3)), VSR_IO_OK);
    harness_run();
    CHECK(h.store->log_begin == 3 && h.store->retained_begin == 1);
    CHECK(client_of(a)->retained == 4 && client_of(b)->retained == 3);
    CHECK(vsr_io_store_free_floor(h.store) == 1);
    expect_completion(submit_reclaim(4), VSR_IO_OK);
    CHECK(h.store->retained_begin == 3 && op_ref(1)->op == 0);
    CHECK(vsr_io_store_free_floor(h.store) == 2); /* Op 3's record. */
    expect_completion(submit(txn_trim(5, 5)), VSR_IO_OK);
    harness_run();
    CHECK(client_of(a) == NULL && client_of(b) == NULL);
    expect_completion(submit_reclaim(6), VSR_IO_OK);
    CHECK(h.store->log_begin == 5 && h.store->log_end == 5);
    CHECK(vsr_io_store_free_floor(h.store) == 4); /* Client base 3. */
    vsr_io_store_base_set(h.store, x, 5);
    CHECK(vsr_io_store_free_floor(h.store) == 5); /* RECLAIM 6 as far as
                                                     the media, 5 (112). */
    expect_completion(submit(txn_append_client(6, 1, 16, a, 9)), VSR_IO_OK);
    expect_completion(submit(txn_trim(7, 6)), VSR_IO_OK);
    harness_run();
    CHECK(client_of(a) == NULL); /* Its retained entry was trimmed. */
    expect_completion(submit_reclaim(7), VSR_IO_OK);
    vsr_io_store_base_set(h.store, x, 7);
    CHECK(vsr_io_store_free_floor(h.store) == 7); /* RECLAIM. */
    /* A capture: its floor is the oldest record it copies. */
    expect_completion(submit(txn_clients(8, 1, &a, numbers, ops, 8)),
                      VSR_IO_OK);
    harness_run();
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
    CHECK(vsr_io_store_free_floor(h.store) == 8); /* RECLAIM 9 as far as 8. */
    CHECK(vsr_io_store_snapshot_clients(h.store, out, 8) == 1);
    CHECK(vsr_io_store_free_floor(h.store) == 8); /* The capture. */
    vsr_io_store_capture_end(h.store, none, 0);   /* Abandoned. */
    CHECK(vsr_io_store_free_floor(h.store) == 8);
    vsr_io_store_base_set(h.store, x, 9); /* A file not covering a. */
    CHECK(h.store->client_base == 7);
    CHECK(vsr_io_store_free_floor(h.store) == 8);
    /* A kept version binds below everything else. */
    expect_completion(submit(txn_append(9, 1, 16)), VSR_IO_OK);
    expect_completion(submit(txn_append(10, 1, 16)), VSR_IO_OK);
    harness_run();
    expect_completion(submit(txn_truncate(11, 6, 0, 0)), VSR_IO_OK);
    harness_run();
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
    c.sync_mode = VSR_IO_SYNC_DSYNC; /* Written is on media (112). */
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
    harness_run();
    sequence++;
    expect_completion(submit_reclaim(sequence), VSR_IO_OK);
    CHECK(h.store->segments[0].number == 1); /* Client base 0. */
    vsr_io_store_base_set(h.store, x, sequence - 1);
    /* The RECLAIM of readable + 1 applies as far as the media. */
    CHECK(vsr_io_store_free_floor(h.store) == sequence - 1);
    CHECK(sealed_at < sequence - 1);
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

/* -------------------------------------------------------------------------
 * Recovery
 * ---------------------------------------------------------------------- */

/* A configuration without holds: a submitted record is packed at once
 * (its offset known), a write covers everything packed since the last. */
static struct config plain_config(void)
{
    struct config c = base_config();

    c.write_behind_bytes = c.segment_bytes;
    c.cache_bytes += c.segment_bytes;
    return c;
}

/* Submits a transaction and drives it to completion (writes, growth or
 * a wanted base load included), then snapshots the indexes. */
static void store_run(const struct txn *t)
{
    uint64_t op = submit(t);

    harness_run();
    if (vsr_io_store_base_wanted(h.store, NULL, NULL)) {
        feed_base();
        harness_run();
    }
    expect_completion(op, VSR_IO_OK);
    CHECK(h.store->readable == t->store.sequence);
    snapshot_take();
}

/* Transaction 1 then plain appends up to `count`, each written before the
 * next, a SYNC at `sync_at` (0: none), a snapshot after each. */
static void plain_run(uint64_t count, uint64_t sync_at)
{
    for (uint64_t sequence = 1; sequence <= count; ++sequence) {
        const struct txn *t = sequence == 1 ? txn_identity(1, VSR_MEMBER_FULL)
                                            : txn_append(sequence, 2, 100);

        store_run(t);
        if (sequence == sync_at) {
            uint64_t op = submit_sync(sequence);

            harness_run();
            expect_completion(op, VSR_IO_OK);
        }
    }
}

/* Recovers after a crash that kept every block and expects `sequence`
 * with the indexes of its snapshot; the row's lease is released. */
static void expect_recovered(const struct config *c, uint64_t sequence)
{
    const struct vsr_loaded *loaded = NULL;
    uint32_t lease = NONE;
    int32_t status;

    disk_crash(false);
    harness_open_keep(c, true);
    status = harness_recover(VSR_START_RECOVER, &loaded, &lease);
    if (status != VSR_IO_OK || loaded->sequence != sequence) {
        fprintf(stderr,
                "recovered: status %d sequence %" PRIu64 ", expected %" PRIu64
                "\n",
                status, status == VSR_IO_OK ? loaded->sequence : 0, sequence);
    }
    CHECK(status == VSR_IO_OK);
    CHECK(loaded->sequence == sequence && loaded->count == 1);
    CHECK(((const struct vsr_recovered *)loaded->items)->sequence == sequence);
    release_lease(lease);
    snapshot_check(sequence);
}

static void expect_corrupt(const struct config *c)
{
    const struct vsr_loaded *loaded = NULL;
    uint32_t lease = NONE;

    disk_crash(false);
    harness_open_keep(c, true);
    CHECK(harness_recover(VSR_START_RECOVER, &loaded, &lease) ==
          VSR_IO_CORRUPT);
    CHECK(h.store->state == VSR_IO_STORE_FAILED);
}

/* Both CRCs of the record at `offset` made good again after an edit. */
static void image_fix_record(uint64_t offset)
{
    unsigned char *record = disk_image + offset;
    uint32_t length = vsr_io_get_u32(record + 4);

    vsr_io_put_u32(record + 40, vsr_io_crc32c(0, record + 48, length - 48));
    vsr_io_put_u32(record + 44, vsr_io_crc32c(0, record, 44));
}

/* Every prefix of the script: the crash after sequence n (every block
 * kept) recovers exactly n with the indexes, the state and the base of
 * its snapshot, in a superblock with run + 1 and the floor n; the script
 * then continues over the recovered log (the never-rewrite model checks
 * every write) and a second crash finds all of it. Segments seal, the
 * ring wraps, the third slot grows and slots free along the way. */
static void recover_prefixes(uint8_t sync_mode)
{
    struct config c = script_config(sync_mode);
    bool reused = false;

    for (uint64_t n = 1; n <= SCRIPT_LENGTH; ++n) {
        const struct vsr_loaded *loaded = NULL;
        const struct vsr_recovered *row;
        struct vsr_io_wire_superblock superblock;
        struct vsr_id anchor = script_anchor(n);
        uint32_t lease = NONE;
        uint32_t run;
        uint64_t revision;

        harness_open(&c);
        harness_start(VSR_START_NEW);
        script_run(1, n);
        run = h.store->run;
        revision = h.store->superblock_revision;
        reused = reused || h.store->next_segment > c.max_segments + 1;
        disk_crash(false);
        harness_open_keep(&c, true);
        CHECK(harness_recover(VSR_START_RECOVER, &loaded, &lease) == VSR_IO_OK);
        row = loaded->items;
        CHECK(loaded->count == 1 && loaded->sequence == n);
        CHECK(row->sequence == n);
        CHECK(row->identity.cluster.hi == 0x77 && row->identity.replica == 3);
        CHECK(row->log_begin == snapshots[n].log_begin &&
              row->log_end == snapshots[n].log_end);
        CHECK(row->hard.role == VSR_MEMBER_FULL && row->hard.view == 1);
        CHECK(row->hard.epoch != NULL && row->hard.epoch->current != NULL &&
              row->hard.epoch->current->count == 1 &&
              row->hard.epoch->current->members[0].id == 1);
        if (anchor.hi == 0) {
            CHECK(row->checkpoint == NULL && !feed.fed);
        } else {
            uint64_t published = (n - 12) / 10 * 10 + 12;

            CHECK(row->checkpoint != NULL &&
                  row->checkpoint->id.hi == anchor.hi);
            CHECK(row->checkpoint->manifest.size == 16 &&
                  memcmp(row->checkpoint->manifest.spans[0].data,
                         txns[published].manifest_bytes, 16) == 0);
            CHECK(feed.fed && feed.id.hi == anchor.hi);
            CHECK(h.store->client_base_id.hi == anchor.hi);
        }
        release_lease(lease);
        snapshot_check(n);
        CHECK(h.store->run == run + 1 && h.store->durable == n);
        CHECK(h.store->written == n && h.store->flushed == n);
        CHECK(h.store->superblock_floor == n && h.store->reclaim == 0);
        CHECK(h.store->superblock_revision == revision + 1);
        read_superblock(h.store->superblock_next == 0 ? 1 : 0, &superblock);
        CHECK(superblock.run == run + 1 && superblock.durable_floor == n);
        CHECK(superblock.slots == h.store->slots &&
              superblock.revision == revision + 1);
        CHECK(superblock.cluster_hi == 0x77 && superblock.replica == 3);
        CHECK(disk.flushes == (sync_mode == VSR_IO_SYNC_FDATASYNC ? 2u : 0u));
        CHECK(h.store->head == 0 && h.store->file_head % BLOCK == 0);
        CHECK(h.store->segments[h.store->current].phase == VSR_IO_SEGMENT_OPEN);
        /* Writing resumes; the second crash finds the continuation. */
        script_run(n + 1, n + 3);
        expect_recovered(&c, n + 3);
        harness_close();
    }
    CHECK(reused);
}

static void test_recover_prefixes(void)
{
    recover_prefixes(VSR_IO_SYNC_FDATASYNC);
}

static void test_recover_prefixes_dsync(void)
{
    recover_prefixes(VSR_IO_SYNC_DSYNC);
}

/* The torn tail: the last write, of three records over several blocks,
 * persists only its first `tear` blocks; the records complete within
 * them are recovered, the rest is the tail, and writing resumes at the
 * block after the last valid record. Then a lost middle block with the
 * later block persisted, and the run rule: a persisted block of a torn
 * write behind a block rewritten by a later run is rejected. */
static void test_recover_torn(void)
{
    struct config c = plain_config();
    uint32_t at;
    uint64_t expected = 5;

    for (uint32_t tear = 0;; ++tear) {
        uint64_t write_offset;
        uint64_t blocks;
        uint64_t resume;

        expected = 5;

        harness_open(&c);
        harness_start(VSR_START_NEW);
        plain_run(5, 5);
        for (uint64_t sequence = 6; sequence <= 8; ++sequence) {
            expect_completion(submit(txn_append(sequence, 3, 100)), VSR_IO_OK);
            snapshot_take();
        }
        CHECK(harness_prepare() == 1);
        at = pending_of(VSR_IO_SQE_WRITE);
        write_offset = h.pending[at].sqe.offset;
        blocks = (h.pending[at].sqe.length + BLOCK - 1) / BLOCK;
        CHECK(write_offset == txns[6].file_offset && blocks >= 3);
        for (uint64_t sequence = 6; sequence <= 8; ++sequence) {
            if (txns[sequence].file_offset + txns[sequence].bytes <=
                write_offset + tear * BLOCK) {
                expected = sequence;
            }
        }
        disk.tear_armed = 1;
        disk.tear_blocks = tear;
        harness_complete(at); /* Lost. */
        expect_recovered(&c, expected);
        /* Records 6 to 8 carry the floor 5; none of 1 to 5 does. */
        CHECK(h.store->recovery.durable_floor == (expected > 5 ? 5u : 0u));
        resume = h.store->file_head;
        CHECK(
            resume ==
            round_up(txns[expected].file_offset + txns[expected].bytes, BLOCK));
        /* The next record goes to that block; the model would trap a
         * rewrite below it. The bytes after the last valid record in its
         * block (a torn record's head, when one straddles the tear) are
         * dead: the next recovery skips them to find that record
         * (decision 110). */
        expect_completion(submit(txn_append(expected + 1, 1, 8)), VSR_IO_OK);
        harness_run();
        CHECK(txns[expected + 1].file_offset == resume);
        snapshot_take();
        expect_recovered(&c, expected + 1);
        harness_close();
        if (tear == blocks) {
            CHECK(expected == 8);
            break;
        }
        CHECK(expected < 8);
    }
    /* A lost middle block: the record straddling it ends the log, the
     * later block's records are the tail and their flushed floors count
     * (the sweep) but stay below the recovered sequence. */
    harness_open(&c);
    harness_start(VSR_START_NEW);
    plain_run(5, 5);
    for (uint64_t sequence = 6; sequence <= 8; ++sequence) {
        expect_completion(submit(txn_append(sequence, 3, 100)), VSR_IO_OK);
        snapshot_take();
    }
    harness_run();
    CHECK(h.store->written == 8);
    at = (uint32_t)((txns[7].file_offset + 8) / BLOCK);
    disk_lose_block(at);
    CHECK(txns[8].file_offset >= (at + 1) * BLOCK); /* 8 survives whole. */
    for (uint64_t sequence = 5; sequence <= 8; ++sequence) {
        if (txns[sequence].file_offset + txns[sequence].bytes <= at * BLOCK) {
            expected = sequence;
        }
    }
    CHECK(expected < 8);
    expect_recovered(&c, expected);
    CHECK(h.store->recovery.durable_floor == 5); /* Record 8, swept. */
    harness_close();
    /* The run rule: record 2 (run 1) fills block b exactly, record 3
     * (run 1) starts at b + 1; block b is lost, 2' (run 2) rewrites it
     * within the block; the next recovery must not take the old 3. */
    harness_open(&c);
    harness_start(VSR_START_NEW);
    plain_run(1, 1);
    expect_completion(submit(txn_truncate(2, 1, 2, 144)), VSR_IO_OK);
    CHECK(txns[2].bytes == BLOCK && txns[2].file_offset % BLOCK == 0);
    snapshot_take();
    expect_completion(submit(txn_append(3, 1, 8)), VSR_IO_OK);
    snapshot_take();
    CHECK(txns[3].file_offset == txns[2].file_offset + BLOCK);
    harness_run();
    disk_lose_block(txns[2].file_offset / BLOCK);
    expect_recovered(&c, 1);
    CHECK(h.store->file_head == txns[2].file_offset);
    expect_completion(submit(txn_append(2, 1, 8)), VSR_IO_OK);
    CHECK(txns[2].bytes < BLOCK);
    harness_run();
    snapshot_take();
    CHECK(h.store->run == 2);
    expect_recovered(&c, 2);
    CHECK(h.store->run == 3 && h.store->log_end == 2);
    harness_close();
}

/* The durable floor (decision 50): a bad record above F is the torn
 * tail, at or below F it is CORRUPT, F being carried by a later valid
 * record's flushed alone (the superblock still says 0); a PAD whose
 * length does not reach the block's end is a bad range too. */
static void test_recover_floor(void)
{
    struct config c = plain_config();
    struct vsr_io_wire_superblock superblock;
    uint64_t pad;

    /* Record 9 damaged: above F = 6 (records 7 to 10 carry it). */
    harness_open(&c);
    harness_start(VSR_START_NEW);
    plain_run(10, 6);
    read_superblock(0, &superblock);
    CHECK(superblock.durable_floor == 0);
    disk_image[txns[9].file_offset + 60] ^= 0xFF;
    expect_recovered(&c, 8);
    CHECK(h.store->recovery.durable_floor == 6);
    harness_close();
    /* Record 7 damaged: the first above F. */
    harness_open(&c);
    harness_start(VSR_START_NEW);
    plain_run(10, 6);
    disk_image[txns[7].file_offset + 60] ^= 0xFF;
    expect_recovered(&c, 6);
    harness_close();
    /* Record 5 damaged: at or below F, which only record 7's flushed
     * carries, found by the sweep behind the bad one. */
    harness_open(&c);
    harness_start(VSR_START_NEW);
    plain_run(10, 6);
    disk_image[txns[5].file_offset + 60] ^= 0xFF;
    expect_corrupt(&c);
    CHECK(h.store->recovery.durable_floor == 6);
    CHECK(h.store->recovery.sequence == 4);
    harness_close();
    /* The floor from the superblock alone (an idle write persisted it):
     * record 6 damaged is CORRUPT even with no record after it. */
    harness_open(&c);
    harness_start(VSR_START_NEW);
    plain_run(6, 6);
    h.now += 200000000;
    harness_run(); /* The idle superblock write. */
    read_superblock(h.store->superblock_next == 0 ? 1 : 0, &superblock);
    CHECK(superblock.durable_floor == 6);
    disk_image[txns[6].file_offset + 60] ^= 0xFF;
    expect_corrupt(&c);
    harness_close();
    /* A bad PAD (its length is not CRC-covered) after record 8, or after
     * record 4 below F, is the dead tail of its block: the chain resumes
     * with record 9, or 5, at the next block (decision 110); the
     * recovered log is whole. */
    harness_open(&c);
    harness_start(VSR_START_NEW);
    plain_run(10, 6);
    pad = txns[8].file_offset + txns[8].bytes;
    CHECK(pad % BLOCK != 0 &&
          vsr_io_get_u32(disk_image + pad) == VSR_IO_PAD_MAGIC);
    vsr_io_put_u32(disk_image + pad + 4, BLOCK); /* Past the block. */
    expect_recovered(&c, 10);
    CHECK(h.store->recovery.durable_floor == 6);
    harness_close();
    harness_open(&c);
    harness_start(VSR_START_NEW);
    plain_run(10, 6);
    pad = txns[4].file_offset + txns[4].bytes;
    CHECK(pad % BLOCK != 0);
    vsr_io_put_u32(disk_image + pad + 4, 8); /* Short of the block. */
    expect_recovered(&c, 10);
    harness_close();
    /* A bad PAD at a block boundary (the PAD of a write whose last
     * record filled its block) ends the chain there. */
    harness_open(&c);
    harness_start(VSR_START_NEW);
    plain_run(10, 6);
    vsr_io_put_u32(disk_image + txns[9].file_offset, VSR_IO_PAD_MAGIC);
    vsr_io_put_u32(disk_image + txns[9].file_offset + 4, 8);
    CHECK(txns[9].file_offset % BLOCK == 0);
    expect_recovered(&c, 8);
    harness_close();
    /* CRC-valid bytes whose content contradicts the log (an APPEND not
     * at the log end) are CORRUPT wherever they lie. */
    harness_open(&c);
    harness_start(VSR_START_NEW);
    plain_run(10, 6);
    vsr_io_put_u64(disk_image + txns[9].file_offset + 48 + 8, 1000);
    image_fix_record(txns[9].file_offset);
    expect_corrupt(&c);
    harness_close();
}

/* Stale headers (decision 48): a successor whose records were all lost
 * is abandoned and its slot freed, writing resumes in the segment
 * holding the last record; the new segment then taking that slot loses
 * its header write while its records persist, and the old header, which
 * names the same sequence, still leads to them; a reused slot whose new
 * header is lost under an old header naming another sequence makes its
 * records the tail. */
static void test_recover_stale(void)
{
    struct config c = script_config(VSR_IO_SYNC_FDATASYNC);
    struct vsr_id x = {0x51, 1};
    uint64_t sequence = 2;
    uint64_t sealed_at;
    uint64_t before;

    c.write_behind_bytes = c.segment_bytes;
    c.cache_bytes += c.segment_bytes;
    harness_open(&c);
    harness_start(VSR_START_NEW);
    plain_run(1, 1);
    while (h.store->current == 0) {
        store_run(txn_append(sequence, 2, 100));
        sequence++;
    }
    sealed_at = h.store->segments[0].last_sequence;
    CHECK(sealed_at == sequence - 2 && h.store->segments[1].number == 2);
    /* Segment 2's records lost, its header kept: abandoned. */
    for (uint64_t at = (slot_offset(1) + h.header_bytes) / BLOCK;
         at < h.store->file_head / BLOCK; ++at) {
        disk_lose_block(at);
    }
    expect_recovered(&c, sealed_at);
    CHECK(h.store->current == 0 && h.store->segments[1].number == 0);
    CHECK(h.store->segments[0].phase == VSR_IO_SEGMENT_OPEN);
    CHECK(h.store->next_segment == 3);
    /* A record that does not fit seals segment 1 again: segment 3 takes
     * slot 1 over the abandoned header, whose write is then lost while
     * the record persists: header 2 names sealed_at too, so the record
     * is found under it. */
    store_run(txn_append(sealed_at + 1, 4, 250));
    CHECK(h.store->current == 1 && h.store->segments[1].number == 3);
    CHECK(disk.dirty[slot_offset(1) / BLOCK] != 0);
    disk_lose_block(slot_offset(1) / BLOCK);
    CHECK(vsr_io_get_u64(disk_image + slot_offset(1) + 16) == 2);
    expect_recovered(&c, sealed_at + 1);
    CHECK(h.store->segments[1].number == 2 && h.store->current == 1);
    CHECK(h.store->next_segment == 3 && h.store->segments[1].run == 1);
    /* Fill slot 1, grow into slot 2 (segment 3 again: 3 was never
     * durable), recover once more. */
    sequence = sealed_at + 2;
    while (h.store->current == 1) {
        store_run(txn_append(sequence, 2, 100));
        sequence++;
    }
    CHECK(h.store->current == 2 && h.store->segments[2].number == 3);
    store_run(txn_append(sequence, 1, 8));
    expect_recovered(&c, sequence);
    CHECK(h.store->segments[2].number == 3 && h.store->current == 2);
    sequence++;
    /* Free slot 0's segment and reuse it as segment 4: its header write
     * is lost but its records persist; the old header names sequence 0,
     * so they are the tail and the slot is free again. */
    store_run(txn_trim(sequence, h.store->log_end));
    sequence++;
    harness_capture(x); /* A base above the records frees their slots. */
    store_run(txn_publish(sequence, x, h.store->log_end - 1));
    expect_completion(submit_reclaim(sequence), VSR_IO_OK);
    harness_run();
    sequence++;
    while (h.store->current == 2) {
        store_run(txn_append(sequence, 2, 100));
        sequence++;
    }
    before = h.store->segments[2].last_sequence;
    CHECK(h.store->current == 0 && h.store->segments[0].number == 4);
    CHECK(before == sequence - 2 && disk.dirty[slot_offset(0) / BLOCK] != 0);
    disk_lose_block(slot_offset(0) / BLOCK);
    CHECK(vsr_io_get_u64(disk_image + slot_offset(0) + 16) == 1);
    expect_recovered(&c, before);
    CHECK(h.store->current == 2 && h.store->segments[0].number == 0);
    CHECK(h.store->next_segment == 4);
    harness_close();
}

/* The superblocks: the newer valid copy wins, a damaged copy leaves the
 * other, both damaged is CORRUPT, and a growth whose superblock write was
 * lost is recovered from the file's size; the geometry must match. */
static void test_recover_superblocks(void)
{
    struct config c = plain_config();
    struct vsr_io_wire_superblock superblock;
    uint64_t sequence = 2;
    uint64_t held;

    c.segments = 2;
    c.max_segments = 3;
    harness_open(&c);
    harness_start(VSR_START_NEW);
    plain_run(1, 1);
    while (h.store->slots < 3) {
        store_run(txn_append(sequence, 4, 128));
        sequence++;
    }
    /* Creation wrote revision 1 to both, the identity 2 to copy 0, the
     * growth 3 to copy 1. */
    read_superblock(0, &superblock);
    CHECK(superblock.revision == 2 && superblock.slots == 2);
    read_superblock(1, &superblock);
    CHECK(superblock.revision == 3 && superblock.slots == 3);
    /* The newer copy damaged: the older, naming two slots, with a file
     * of three; the recovery rewrites the damaged copy. */
    memset(disk_image + BLOCK, 0, BLOCK);
    expect_recovered(&c, sequence - 1);
    CHECK(h.store->recovery.copy == 0 && h.store->slots == 3);
    read_superblock(1, &superblock);
    CHECK(superblock.revision == 3 && superblock.slots == 3);
    /* Both valid: copy 1 wins; the recovery writes copy 0. */
    expect_recovered(&c, sequence - 1);
    CHECK(h.store->recovery.copy == 1);
    read_superblock(0, &superblock);
    CHECK(superblock.revision == 4 && superblock.slots == 3);
    /* Both damaged. */
    memset(disk_image, 0, 2 * BLOCK);
    expect_corrupt(&c);
    harness_close();
    /* A crash between the growth's FALLOCATE and its superblock. */
    harness_open(&c);
    harness_start(VSR_START_NEW);
    plain_run(1, 1);
    sequence = 2;
    while (h.store->growth == 0) {
        uint64_t op = submit(txn_append(sequence, 4, 128));

        if (h.store->growth == 0) {
            expect_completion(op, VSR_IO_OK);
            harness_run();
            snapshot_take();
        } else {
            held = op;
        }
        sequence++;
    }
    (void)held;
    CHECK(harness_prepare() == 1);
    harness_complete(pending_of(VSR_IO_SQE_FALLOCATE));
    CHECK(harness_prepare() == 1);
    {
        uint32_t at = pending_of(VSR_IO_SQE_WRITE);

        CHECK(h.pending[at].sqe.offset ==
              BLOCK); /* Copy 1: the identity took 0. */
        harness_complete(at);
    }
    disk_lose_block(1);
    expect_recovered(&c, sequence - 2);
    CHECK(h.store->slots == 3 && h.store->file_size == disk.size);
    read_superblock(1, &superblock);
    CHECK(superblock.slots == 3 && superblock.revision == 3);
    /* The third slot is there: no FALLOCATE for the next segment. */
    store_run(txn_append(sequence - 1, 4, 128));
    CHECK(h.store->current == 2 && disk.fallocates == 0);
    /* The geometry must match the options. */
    c.segment_bytes += BLOCK;
    c.cache_bytes += BLOCK;
    expect_corrupt(&c);
    harness_close();
}

/* Restarts and drives the recovery until the first chunk read of the
 * start segment is pending; returns its pending index. */
static uint32_t recover_until_chunk(const struct config *c, uint64_t *op)
{
    struct vsr_store_read read;

    disk_crash(false);
    harness_open_keep(c, true);
    memset(&read, 0, sizeof(read));
    read.type = VSR_LOAD_RECOVERY;
    *op = next_op();
    vsr_io_store_open(h.store, VSR_START_RECOVER, *op, &read);
    for (;;) {
        uint32_t at;

        CHECK(harness_prepare() == 1);
        at = pending_first();
        if (h.pending[at].sqe.opcode == VSR_IO_SQE_READ &&
            h.pending[at].sqe.offset == slot_offset(0) + h.header_bytes) {
            return at;
        }
        harness_complete(at);
    }
}

/* The one re-read of a bad range (decision 50): a read error, a flipped
 * byte and a short read on the first attempt all pass on the second, on
 * the superblocks as on a chunk; a record whose header straddles a
 * chunk's end is read again from its block. */
static void test_recover_reread(void)
{
    struct config c = plain_config();
    const struct vsr_loaded *loaded = NULL;
    uint32_t lease = NONE;
    uint64_t op;
    uint64_t reads;
    uint64_t end;
    uint64_t body;
    uint64_t sequence;
    uint32_t at;

    harness_open(&c);
    harness_start(VSR_START_NEW);
    plain_run(8, 8);
    /* The superblocks: a read error, then a short read. */
    disk_crash(false);
    harness_open_keep(&c, true);
    disk.fail_read = -EIO;
    CHECK(harness_recover(VSR_START_RECOVER, &loaded, &lease) == VSR_IO_OK);
    CHECK(loaded->sequence == 8);
    release_lease(lease);
    reads = disk.reads;
    disk_crash(false);
    harness_open_keep(&c, true);
    disk.short_read = BLOCK;
    CHECK(harness_recover(VSR_START_RECOVER, &loaded, &lease) == VSR_IO_OK);
    CHECK(loaded->sequence == 8 && disk.reads == reads);
    release_lease(lease);
    /* A chunk: a read error, then a short read. */
    at = recover_until_chunk(&c, &op);
    CHECK(vsr_io_store_close(h.store) == VSR_EBUSY);
    disk.fail_read = -EIO;
    harness_complete(at);
    CHECK(h.store->state == VSR_IO_STORE_RECOVERING);
    harness_run();
    expect_loaded(op, VSR_IO_OK, &lease);
    release_lease(lease);
    CHECK(h.store->readable == 8 && disk.reads == reads);
    disk_after_recovery();
    at = recover_until_chunk(&c, &op);
    disk.short_read = BLOCK;
    harness_complete(at);
    harness_run();
    expect_loaded(op, VSR_IO_OK, &lease);
    release_lease(lease);
    CHECK(h.store->readable == 8);
    disk_after_recovery();
    /* A flipped byte on the first read of record 5. */
    disk_crash(false);
    harness_open_keep(&c, true);
    disk.flip_armed = 1;
    disk.flip_offset = txns[5].file_offset + 60;
    CHECK(harness_recover(VSR_START_RECOVER, &loaded, &lease) == VSR_IO_OK);
    CHECK(loaded->sequence == 8 && disk.flip_armed == 0);
    release_lease(lease);
    snapshot_check(8);
    harness_close();
    /* A record ending 40 bytes before the chunk's end (the chunk is the
     * slab, 4096 bytes from the data start): the next header is short in
     * the chunk and read again from its block. */
    harness_open(&c);
    harness_start(VSR_START_NEW);
    plain_run(1, 1);
    end = slot_offset(0) + h.header_bytes + PAGE - 40;
    sequence = 2;
    for (;;) {
        uint64_t gap = end - h.store->file_head;

        /* A one-entry APPEND is 136 bytes plus its body. */
        if (gap >= 136 && gap <= 136 + 256) {
            break; /* One record of a body in range ends there. */
        }
        if (gap < 136) {
            end += PAGE; /* The next chunk's end. */
            continue;
        }
        expect_completion(submit(txn_append(sequence, 1, 200)), VSR_IO_OK);
        snapshot_take();
        sequence++;
    }
    body = end - h.store->file_head - 136;
    expect_completion(submit(txn_append(sequence, 1, (size_t)body)), VSR_IO_OK);
    snapshot_take();
    sequence++;
    CHECK(h.store->file_head == end);
    expect_completion(submit(txn_append(sequence, 2, 100)), VSR_IO_OK);
    snapshot_take();
    harness_run(); /* One write: the records lie back to back. */
    expect_recovered(&c, sequence);
    harness_close();
}

/* Start modes and the base: NEW and JOIN over an empty log proceed with
 * it, over records they get the row; a witness loads no file; a missing
 * file is CORRUPT; a foreign RESTORE's file is wanted again at recovery;
 * an identity the superblock contradicts is CORRUPT. */
static void test_recover_modes(void)
{
    struct config c = script_config(VSR_IO_SYNC_FDATASYNC);
    struct vsr_io_wire_superblock superblock;
    const struct vsr_loaded *loaded = NULL;
    struct vsr_id x = {0x51, 1};
    struct vsr_id z = {0x53, 1};
    struct base_feed *file;
    uint32_t lease = NONE;

    /* An empty log under NEW, JOIN and RECOVER: NOT_FOUND, then usable. */
    harness_open(&c);
    harness_start(VSR_START_NEW);
    for (uint32_t mode = VSR_START_NEW; mode <= VSR_START_RECOVER; ++mode) {
        disk_crash(false);
        harness_open_keep(&c, true);
        CHECK(harness_recover(mode, &loaded, &lease) == VSR_IO_NOT_FOUND);
        CHECK(h.store->state == VSR_IO_STORE_READY && h.store->run == 2 + mode);
        CHECK(h.store->current == 0 && h.store->readable == 0);
    }
    store_run(txn_identity(1, VSR_MEMBER_FULL));
    expect_recovered(&c, 1);
    /* Records under NEW: the row (the core refuses it, decision 57). */
    disk_crash(false);
    harness_open_keep(&c, true);
    CHECK(harness_recover(VSR_START_NEW, &loaded, &lease) == VSR_IO_OK);
    CHECK(loaded->sequence == 1);
    release_lease(lease);
    harness_close();
    /* A witness with an anchor loads no file. */
    harness_open(&c);
    harness_start(VSR_START_NEW);
    store_run(txn_identity(1, VSR_MEMBER_WITNESS));
    store_run(txn_publish(2, x, 1));
    store_run(txn_append(3, 2, 50));
    CHECK(h.store->client_base == 2 && h.store->client_base_id.hi == 0x51);
    expect_recovered(&c, 3);
    CHECK(!feed.fed && h.store->hard.role == VSR_MEMBER_WITNESS);
    CHECK(h.store->anchor.id.hi == 0x51 && h.store->client_base == 2);
    harness_close();
    /* A FULL replica whose anchor's file is missing. */
    harness_open(&c);
    harness_start(VSR_START_NEW);
    script_run(1, 14);
    feeds_reset();
    expect_corrupt(&c);
    CHECK(feed.fed && feed.status == VSR_IO_CORRUPT);
    harness_close();
    /* A foreign RESTORE: its file is wanted at the STORE and again at
     * recovery, at the RESTORE's sequence. */
    harness_open(&c);
    harness_start(VSR_START_NEW);
    script_run(1, 6);
    file = feed_of(z, true);
    file->count = 1;
    file->records[0] = txns[5].records[0];
    file->records[0].request.number = 100;
    file->records[0].op = 3;
    file->offsets[0] = 64;
    store_run(txn_restore(7, z, 2, VSR_MEMBER_FULL));
    CHECK(feed.fed && h.store->log_begin == 3 && h.store->client_base == 7);
    CHECK(client_of(txns[5].records[0].request.client)->current.number == 100);
    store_run(txn_append(8, 2, 60));
    expect_recovered(&c, 8);
    CHECK(feed.fed && feed.id.hi == 0x53 && feed.sequence == 7);
    CHECK(h.store->restored == 7 && h.store->log_begin == 3);
    harness_close();
    /* The superblock carries another replica's identity. */
    harness_open(&c);
    harness_start(VSR_START_NEW);
    script_run(1, 4);
    read_superblock(0, &superblock);
    CHECK(superblock.replica == 3);
    superblock.replica = 4;
    vsr_io_codec_put_superblock(&superblock, disk_image, BLOCK);
    superblock.revision++;
    vsr_io_codec_put_superblock(&superblock, disk_image + BLOCK, BLOCK);
    expect_corrupt(&c);
    harness_close();
}

/* An APPEND at `sequence` of exactly `bytes` record bytes, found over
 * the entry count and body size. */
static const struct txn *append_of(uint64_t sequence, uint64_t bytes)
{
    for (uint32_t count = 1; count <= 4; ++count) {
        for (size_t body = 0; body <= 256; body += 8) {
            const struct txn *t = txn_append(sequence, count, body);

            if (t->bytes == bytes) {
                return t;
            }
        }
    }
    CHECK(false);
    return NULL;
}

/* The ring's wrap pads the file too: a record that fits the segment by
 * its own bytes but not with the padding of the block the wrap closes
 * seals the segment instead of overrunning it (the seal rule counts the
 * padding). The ring is a block shorter than the segment; writes bring
 * the head to a block boundary a couple of blocks before the ring's end,
 * three unwritten records bring it 200 bytes before it, and a 600-byte
 * record then needs 200 bytes of padding plus itself: 400 over the
 * segment's end. */
static void test_wrap_seal(void)
{
    struct config c = base_config();
    uint64_t ring = c.cache_bytes;
    uint64_t sequence = 1;
    uint64_t target;

    c.segment_bytes = ring + BLOCK;
    harness_open(&c);
    harness_start(VSR_START_NEW);
    CHECK(h.store->ring_size == ring && h.store->head == h.header_bytes);
    expect_completion(submit(txn_identity(sequence++, VSR_MEMBER_FULL)),
                      VSR_IO_OK);
    harness_run();
    /* Each small record written alone pads to its block. */
    while (h.store->head < ring - 2 * BLOCK) {
        expect_completion(submit(txn_append(sequence++, 1, 8)), VSR_IO_OK);
        harness_run();
        CHECK(h.store->head % BLOCK == 0);
    }
    CHECK(h.store->head == ring - 2 * BLOCK && h.store->current == 0);
    CHECK(h.store->segments[0].used == h.store->head);
    target = ring - 200;
    while (h.store->head < target) {
        uint64_t left = target - h.store->head;
        uint64_t bytes = left > UINT64_C(384) ? 272 : left;

        expect_completion(submit(append_of(sequence++, bytes)), VSR_IO_OK);
    }
    CHECK(h.store->head == target && h.store->head % BLOCK == 312);
    CHECK(h.store->segments[0].used + 600 <= c.segment_bytes);
    expect_completion(submit(append_of(sequence++, 600)), VSR_IO_OK);
    CHECK(h.store->current == 1); /* Sealed: 200 of padding and 600. */
    CHECK(h.store->segments[0].used <= c.segment_bytes);
    CHECK(h.store->segments[0].phase == VSR_IO_SEGMENT_SEALED);
    CHECK(h.store->file_head <= slot_offset(1) + c.segment_bytes);
    harness_run();
    CHECK(h.store->written == sequence - 1);
    harness_close();
}

/* A record ending exactly at the ring's end needs no wrap gap, but the
 * next one goes to the ring's start: it begins a new extent, so the bytes
 * on either side of the end are two writes, each contiguous in the ring
 * (a single write from before the end read the superblock copies that
 * follow the ring into the file, or ran past the tail region). Found by a
 * random walk: a flushed record's block held a superblock image. */
static void test_wrap_exact(void)
{
    struct config c = base_config();
    uint64_t ring = c.cache_bytes;
    uint64_t sequence = 1;

    c.segment_bytes = ring + 2 * BLOCK;
    harness_open(&c);
    harness_start(VSR_START_NEW);
    store_run(txn_identity(sequence++, VSR_MEMBER_FULL));
    /* Each small record written alone pads to its block. */
    while (h.store->head < ring - 2 * BLOCK) {
        store_run(txn_append(sequence++, 1, 8));
        CHECK(h.store->head % BLOCK == 0);
    }
    CHECK(h.store->head == ring - 2 * BLOCK && h.store->current == 0);
    /* Unwritten records up to the ring's end exactly, then one more. */
    while (h.store->head < ring) {
        uint64_t left = ring - h.store->head;

        expect_completion(
            submit(append_of(sequence++, left > 384 ? 272 : left)), VSR_IO_OK);
    }
    CHECK(h.store->head == ring && h.store->issued == ring - 2 * BLOCK);
    expect_completion(submit(txn_append(sequence++, 1, 8)), VSR_IO_OK);
    CHECK(h.store->head > ring && h.store->current == 0);
    harness_run(); /* The disk model checks each write's ring range. */
    CHECK(h.store->written == sequence - 1);
    snapshot_take();
    expect_recovered(&c, sequence - 1);
    harness_close();
}

/* A record of a later run behind an older run's record it does not
 * follow (decision 116). Run 1 packs A at k + 1, two blocks from a block
 * boundary, and a crash tears its write after the first block, which
 * recovery's flush persists; run 2 packs A' at k + 1, of A's length and
 * bytes past the first block (only its entries' clients differ), then B'
 * at k + 2 in the second block, and a crash keeps that block but loses
 * the first: A's first block and A''s tail read as A, CRC-valid, and B'
 * (run 2, flushed k) followed it. The chain now ends at A: run 2's
 * recovery made k durable and recovered nothing after it, so a run-2
 * record behind k + 1 is not its successor. */
static void test_recover_splice(void)
{
    struct config c = plain_config();
    const struct vsr_loaded *loaded = NULL;
    const struct vsr_entry *entry;
    struct vsr_id x = {0x77, 1};
    const struct txn *t;
    uint32_t lease = NONE;
    uint64_t offset;
    uint64_t k = 4;
    uint64_t op;
    uint32_t at;

    harness_open(&c);
    harness_start(VSR_START_NEW);
    plain_run(k, k);
    t = txn_append(k + 1, 2, 256); /* Clients 0x1000 and 0x1001. */
    CHECK(t->bytes > BLOCK && t->bytes <= 2 * BLOCK);
    disk.tear_armed = 1;
    disk.tear_blocks = 1;
    expect_completion(submit(t), VSR_IO_OK);
    offset = txns[k + 1].file_offset;
    CHECK(offset % BLOCK == 0);
    harness_run(); /* The write is torn: its completion never comes. */
    expect_recovered(&c, k);
    CHECK(h.store->file_head == offset && h.store->run == 2);
    /* Run 2: A' and B' in one write over A's blocks. */
    t = txn_append_client(k + 1, 2, 256, x, 1);
    CHECK(t->bytes == txns[k + 1].bytes);
    expect_completion(submit(t), VSR_IO_OK);
    expect_completion(submit(txn_append(k + 2, 1, 8)), VSR_IO_OK);
    CHECK(harness_prepare() == 1);
    at = pending_of(VSR_IO_SQE_WRITE);
    CHECK(at != NONE && h.pending[at].sqe.offset == offset &&
          h.pending[at].sqe.length == 2 * BLOCK);
    harness_complete(at);
    disk_lose_block(offset / BLOCK);
    disk_crash(false);
    harness_open_keep(&c, true);
    CHECK(harness_recover(VSR_START_RECOVER, &loaded, &lease) == VSR_IO_OK);
    CHECK(loaded->sequence == k + 1); /* A, not A' and B'. */
    release_lease(lease);
    op = load_log(k + 1, h.store->log_end - 2, h.store->log_end, 2,
                  limits.message_bytes);
    harness_run();
    loaded = expect_loaded(op, VSR_IO_OK, &lease);
    CHECK(loaded->count == 2);
    entry = loaded->items;
    CHECK(entry[0].request.client.hi == 0x1000 &&
          entry[1].request.client.hi == 0x1001);
    release_lease(lease);
    harness_close();
}

/* Records of four entries until the current slot changes: the sequence
 * that opened the new slot. */
static uint64_t fill_slot(uint64_t sequence)
{
    uint32_t current = h.store->current;

    while (h.store->current == current) {
        store_run(txn_append(sequence++, 4, 250));
    }
    return sequence - 1;
}

/* Slot 0 freed with the superblock write naming slot 1 as the start
 * dirty, slot 1 full: the transaction returned (at the next sequence)
 * needs slot 0. Slot 0 holds sequence 1 and the records up to the one
 * that opened slot 1; a TRIM, a capture and its PUBLISH, a SYNC and a
 * RECLAIM put every floor term above it; the records that filled slot 1
 * after the SYNC are written, not flushed. */
static const struct txn *freeing_setup(const struct config *c)
{
    struct vsr_id x = {0x51, 1};
    const struct txn *t;
    uint64_t published;
    uint64_t sequence;
    uint64_t op;

    harness_open(c);
    harness_start(VSR_START_NEW);
    store_run(txn_identity(1, VSR_MEMBER_FULL));
    sequence = fill_slot(2); /* Opened slot 1. */
    CHECK(h.store->current == 1 && h.store->segments[0].number == 1);
    store_run(txn_trim(++sequence, h.store->log_end - 1));
    harness_capture(x);
    store_run(txn_publish(++sequence, x, h.store->log_end - 1));
    op = submit_sync(sequence);
    harness_run();
    expect_completion(op, VSR_IO_OK);
    published = sequence;
    for (;;) {
        t = txn_append(++sequence, 4, 250);
        CHECK(h.store->head % BLOCK == 0); /* No wrap padding to count. */
        if (h.store->segments[1].used + t->bytes > c->segment_bytes) {
            break;
        }
        store_run(t);
    }
    CHECK(h.store->written > h.store->flushed); /* Unflushed records. */
    expect_completion(submit_reclaim(published), VSR_IO_OK);
    CHECK(h.store->segments[0].phase == VSR_IO_SEGMENT_FREEING);
    CHECK(h.store->start_slot == 1 && h.store->superblock_dirty == 1);
    return t;
}

/* Completes every pending WRITE, prepares, and again until none is. */
static void complete_writes(void)
{
    harness_prepare();
    for (uint32_t at = pending_of(VSR_IO_SQE_WRITE); at != NONE;
         at = pending_of(VSR_IO_SQE_WRITE)) {
        harness_complete(at);
        harness_prepare();
    }
}

/* A freed slot is reused only once the superblock naming the new start
 * segment is on media (decision 111): in FDATASYNC mode the write's
 * completion makes the slot FLUSHING, the store asks for a flush of its
 * own, and the completion of a flush issued after that makes it FREE; a
 * STORE needing the slot waits meanwhile. Variants: a crash that loses
 * the superblock's unflushed block (the older copy names segment 1 in
 * slot 0, which must still be there: reusing the slot at the write's
 * completion made this recovery a false CORRUPT); the flush freeing the
 * slot for the held STORE; a SYNC naming the held STORE's sequence (the
 * store's own flush does not wait for it: that would never end); a
 * flush issued before the write completed, which does not count. */
static void test_freeing_flush(void)
{
    struct config c = script_config(VSR_IO_SYNC_FDATASYNC);
    struct vsr_io_completion completion;
    const struct txn *t;
    uint64_t superblock_at;
    uint64_t store_op;
    uint64_t op;
    uint32_t at;

    c.max_segments = 2;
    /* The crash. */
    t = freeing_setup(&c);
    CHECK(harness_prepare() == 1);
    at = pending_of(VSR_IO_SQE_WRITE);
    CHECK(at != NONE && h.pending[at].sqe.offset < 2 * BLOCK);
    superblock_at = h.pending[at].sqe.offset;
    harness_complete(at);
    (void)submit(t);
    complete_writes();
    snapshot_take();
    disk_lose_block(superblock_at / BLOCK);
    expect_recovered(&c, t->store.sequence - 1); /* Held, never packed. */
    CHECK(h.store->start_slot == 0 && h.store->segments[0].number == 1);
    harness_close();
    /* The flush frees the slot and the held STORE takes it. */
    t = freeing_setup(&c);
    CHECK(harness_prepare() == 1);
    harness_complete(pending_of(VSR_IO_SQE_WRITE));
    CHECK(h.store->segments[0].phase == VSR_IO_SEGMENT_FLUSHING);
    store_op = submit(t);
    CHECK(h.store->stores_count == 1);
    CHECK(harness_prepare() == 1);
    at = pending_of(VSR_IO_SQE_FSYNC);
    CHECK(at != NONE);
    CHECK(h.store->segments[0].phase == VSR_IO_SEGMENT_FLUSHING);
    harness_complete(at);
    CHECK(h.store->stores_count == 0 && h.store->current == 0);
    CHECK(h.store->segments[0].number > 2 &&
          h.store->segments[0].phase == VSR_IO_SEGMENT_HEADER);
    expect_completion(store_op, VSR_IO_OK);
    harness_run();
    expect_no_completion();
    harness_close();
    /* A SYNC of the held sequence: the flush the SYNCs wait for needs
     * that STORE written, the STORE needs the slot. */
    t = freeing_setup(&c);
    CHECK(harness_prepare() == 1);
    harness_complete(pending_of(VSR_IO_SQE_WRITE));
    store_op = submit(t);
    op = submit_sync(t->store.sequence);
    harness_run();
    expect_completion(store_op, VSR_IO_OK);
    expect_completion(op, VSR_IO_OK);
    CHECK(h.store->flushed == t->store.sequence);
    harness_close();
    /* A flush in flight when the write completes was issued before it:
     * the slot waits for the next one. */
    t = freeing_setup(&c);
    op = submit_sync(t->store.sequence - 1);
    harness_poll(); /* No sync delay: the flush is due. */
    CHECK(harness_prepare() == 2);
    harness_complete(pending_of(VSR_IO_SQE_WRITE));
    CHECK(h.store->segments[0].phase == VSR_IO_SEGMENT_FLUSHING);
    store_op = submit(t);
    harness_complete(pending_of(VSR_IO_SQE_FSYNC));
    expect_completion(op, VSR_IO_OK);
    CHECK(h.store->segments[0].phase == VSR_IO_SEGMENT_FLUSHING);
    CHECK(h.store->stores_count == 1);
    CHECK(harness_prepare() == 1);
    harness_complete(pending_of(VSR_IO_SQE_FSYNC));
    CHECK(h.store->stores_count == 0 && h.store->current == 0);
    expect_completion(store_op, VSR_IO_OK);
    harness_run();
    while (next_completion(&completion)) {
        CHECK(false);
    }
    harness_close();
}

/* Completes pending SQEs, preparing more, until only the WRITE starting
 * at file offset `lost` is left: the write a crash then loses. */
static void run_except(uint64_t lost)
{
    for (unsigned rounds = 0; rounds < 1000; ++rounds) {
        uint32_t found = NONE;

        harness_poll();
        harness_prepare();
        for (uint32_t i = 0; i < PENDING_MAX && found == NONE; ++i) {
            if (h.pending[i].state == 1 &&
                (h.pending[i].sqe.opcode != VSR_IO_SQE_WRITE ||
                 h.pending[i].sqe.offset != lost)) {
                found = i;
            }
        }
        if (found == NONE) {
            return;
        }
        harness_complete(found);
    }
    CHECK(false);
}

/* Slot 0 holds sequence 1, a CLIENTS record of client 0xC at 2 and the
 * records up to the one that opened slot 1, whose sequence is returned;
 * two slots at most. */
static uint64_t media_setup(const struct config *c)
{
    struct vsr_id client = {0xC, 1};
    uint64_t number = 1;
    uint64_t op = 1;
    uint64_t sequence;

    harness_open(c);
    harness_start(VSR_START_NEW);
    store_run(txn_identity(1, VSR_MEMBER_FULL));
    store_run(txn_clients(2, 1, &client, &number, &op, 16));
    sequence = fill_slot(3);
    CHECK(h.store->current == 1 && h.store->segments[0].number == 1);
    return sequence;
}

/* A crash that loses every unflushed block, then the recovery of
 * `sequence`, whose log and clients read back as before (the snapshot
 * check). */
static void crash_recover(const struct config *c, uint64_t sequence)
{
    disk_crash(true);
    expect_recovered(c, sequence);
    CHECK(h.store->reclaimed == 0 && h.store->client_base_pending == 0);
}

/* The freeing floor counts the RECLAIM revision and the client base as
 * of the sequence on media (decision 112): a crash can bring back any
 * revision from there on, whose row must be served whole. Each part
 * puts a change above the media with its record's write still out, lets
 * everything else complete (the superblock naming a new start segment
 * and the flush after it, when a slot was freed) and crashes, losing
 * that write: the recovered row needs slot 0, which freeing by the
 * RECLAIM or the base as they stood in memory gave up. Then the terms
 * follow a SYNC, and a STORE finding nothing to free while a term waits
 * holds and gets the flush it needs rather than failing. */
static void test_reclaim_media(void)
{
    struct vsr_id x = {0x51, 1};

    for (uint32_t mode = 0; mode < 2; ++mode) {
        struct config c = script_config(mode == 0 ? VSR_IO_SYNC_FDATASYNC
                                                  : VSR_IO_SYNC_DSYNC);
        const struct txn *t;
        uint64_t reclaimed;
        uint64_t retained;
        uint64_t sequence;
        uint64_t pending;
        uint64_t floor;
        uint64_t op;
        uint32_t phase;

        c.max_segments = 2;
        /* The RECLAIM term: the base covers client 0xC's record, the
         * TRIM and its RECLAIM are above the media. */
        sequence = media_setup(&c);
        harness_capture(x);
        store_run(txn_publish(++sequence, x, h.store->log_end - 1));
        op = submit_sync(sequence);
        harness_run();
        expect_completion(op, VSR_IO_OK);
        t = txn_trim(++sequence, h.store->log_end - 1);
        expect_completion(submit(t), VSR_IO_OK); /* Packed, not written. */
        expect_completion(submit_reclaim(sequence), VSR_IO_OK);
        reclaimed = h.store->reclaimed;
        retained = h.store->retained_begin;
        floor = vsr_io_store_free_floor(h.store);
        phase = h.store->segments[0].phase;
        run_except(t->file_offset);
        crash_recover(&c, sequence - 1);
        harness_close();
        /* What kept slot 0 (checked after the recovery, the verdict). */
        CHECK(reclaimed == sequence - 1 && retained == 1);
        CHECK(floor == 3 && phase == VSR_IO_SEGMENT_SEALED); /* Op 1. */
        /* The base term: the TRIM and its RECLAIM are on media, the
         * PUBLISH of a base covering client 0xC's record is not. */
        sequence = media_setup(&c);
        store_run(txn_trim(++sequence, h.store->log_end - 1));
        op = submit_sync(sequence);
        harness_run();
        expect_completion(op, VSR_IO_OK);
        expect_completion(submit_reclaim(sequence), VSR_IO_OK);
        CHECK(vsr_io_store_free_floor(h.store) == 1); /* Base 0 + 1. */
        harness_capture(x);
        t = txn_publish(++sequence, x, h.store->log_end - 1);
        expect_completion(submit(t), VSR_IO_OK);
        expect_completion(submit_reclaim(sequence), VSR_IO_OK);
        CHECK(h.store->client_base == sequence - 1);
        pending = h.store->client_base_pending;
        floor = h.store->client_base_floor;
        phase = h.store->segments[0].phase;
        run_except(t->file_offset);
        crash_recover(&c, sequence - 1);
        harness_close();
        CHECK(pending == sequence && floor == 0);
        CHECK(phase == VSR_IO_SEGMENT_SEALED);
        /* The SYNC of the PUBLISH lets both terms through. */
        sequence = media_setup(&c);
        store_run(txn_trim(++sequence, h.store->log_end - 1));
        harness_capture(x);
        store_run(txn_publish(++sequence, x, h.store->log_end - 1));
        expect_completion(submit_reclaim(sequence), VSR_IO_OK);
        CHECK(h.store->segments[0].number == 1 ||
              mode == 1); /* O_DSYNC: on media once written. */
        op = submit_sync(sequence);
        harness_run();
        expect_completion(op, VSR_IO_OK);
        CHECK(h.store->reclaimed == sequence);
        CHECK(h.store->client_base_floor == sequence - 1 &&
              h.store->client_base_pending == 0);
        CHECK(h.store->segments[0].number == 0 &&
              h.store->segments[0].phase == VSR_IO_SEGMENT_FREE);
        harness_close();
        if (mode == 1) {
            continue;
        }
        /* A STORE that needs a slot while only a term waiting for the
         * media keeps slot 0: held, and the store flushes on its own. */
        sequence = media_setup(&c);
        store_run(txn_trim(++sequence, h.store->log_end - 1));
        expect_completion(submit_reclaim(sequence), VSR_IO_OK);
        harness_capture(x);
        store_run(txn_publish(++sequence, x, h.store->log_end - 1));
        expect_completion(submit_reclaim(sequence), VSR_IO_OK);
        CHECK(h.store->segments[0].number == 1);
        CHECK(disk.flushes == 0 && h.store->syncs_count == 0);
        while (h.store->current == 1) {
            op = submit(txn_append(++sequence, 4, 250));
            harness_run();
            expect_completion(op, VSR_IO_OK);
        }
        CHECK(h.store->current == 0 && disk.flushes >= 2);
        CHECK(h.store->client_base_floor == h.store->client_base);
        harness_close();
    }
}

/* A client's completed record whose slot was freed under the base (the
 * base term of the floor lets it go) and reused: the ring's extents of
 * the new segment cover the old record's file range with other bytes, so
 * a CLIENT load reads the base file, and a capture copies the record from
 * it (sequence 0, the base offset). Found by a random walk: the load came
 * back CORRUPT from the new segment's bytes. */
static void test_client_freed(void)
{
    struct config c = script_config(VSR_IO_SYNC_FDATASYNC);
    struct vsr_io_client_snapshot out[4];
    struct vsr_id client = {0xC, 1};
    struct vsr_id x = {0x51, 1};
    struct vsr_id none = {0, 0};
    struct vsr_io_piece piece;
    const struct vsr_client_record *record;
    const struct vsr_loaded *loaded;
    uint32_t lease = NONE;
    uint64_t base_offset;
    uint64_t sequence;
    uint64_t op;

    c.max_segments = 2;
    sequence = media_setup(&c); /* Client 0xC's record at 2, in slot 0. */
    store_run(txn_trim(++sequence, h.store->log_end - 1));
    harness_capture(x);
    store_run(txn_publish(++sequence, x, h.store->log_end - 1));
    base_offset = client_of(client)->base_offset;
    CHECK(base_offset != UINT64_MAX && h.store->client_base == sequence - 1);
    op = submit_sync(sequence);
    harness_run();
    expect_completion(op, VSR_IO_OK);
    expect_completion(submit_reclaim(sequence), VSR_IO_OK);
    harness_run();
    CHECK(h.store->segments[0].number == 0 &&
          h.store->segments[0].phase == VSR_IO_SEGMENT_FREE);
    /* Slot 0 reused past the old record's bytes, still in the ring. */
    while (h.store->current != 0 ||
           h.store->file_head < txns[2].file_offset + txns[2].bytes) {
        store_run(txn_append(++sequence, 1, 200));
    }
    CHECK(vsr_io_store_hot(h.store, txns[2].file_offset,
                           (uint32_t)txns[2].bytes, &piece));
    op = load_client(h.store->readable, client);
    harness_run();
    loaded = expect_loaded(op, VSR_IO_OK, &lease);
    CHECK(loaded->count == 1);
    record = loaded->items;
    CHECK(record->request.number == 1 && record->op == 1);
    CHECK(record->result.data.size == 16 && record->result.data.count == 1 &&
          memcmp(record->result.data.spans[0].data, txns[2].results[0], 16) ==
              0);
    release_lease(lease);
    CHECK(vsr_io_store_snapshot_clients(h.store, out, 4) == 1);
    CHECK(out[0].record.sequence == 0 && out[0].record.number == 1 &&
          out[0].record.offset == base_offset);
    CHECK(h.store->capture_floor == UINT64_MAX);
    vsr_io_store_capture_end(h.store, none, 0);
    harness_close();
}

/* A client's record newer than the base, evicted from the ring: the
 * load reads it from the log, not the base file's older record, which
 * base_offset still names (it is valid only for a record at or below the
 * base). Found by a random walk: the load returned the older record. */
static void test_client_newer(void)
{
    struct config c = base_config();
    struct vsr_id client = {0xC, 1};
    struct vsr_id x = {0x51, 1};
    const struct vsr_client_record *record;
    const struct vsr_loaded *loaded;
    struct vsr_io_piece piece;
    uint32_t lease = NONE;
    uint64_t number = 1;
    uint64_t sequence = 5;
    uint64_t op = 1;

    harness_open(&c);
    harness_start(VSR_START_NEW);
    store_run(txn_identity(1, VSR_MEMBER_FULL));
    store_run(txn_append(2, 1, 16));
    store_run(txn_clients(3, 1, &client, &number, &op, 16));
    harness_capture(x);
    store_run(txn_publish(4, x, 1));
    CHECK(h.store->client_base == 3 &&
          client_of(client)->base_offset != UINT64_MAX);
    number = 2;
    store_run(txn_clients(5, 1, &client, &number, &op, 24));
    while (vsr_io_store_hot(h.store, txns[5].file_offset,
                            (uint32_t)txns[5].bytes, &piece)) {
        store_run(txn_append(++sequence, 4, 200));
    }
    op = load_client(h.store->readable, client);
    harness_run();
    loaded = expect_loaded(op, VSR_IO_OK, &lease);
    CHECK(loaded->count == 1);
    record = loaded->items;
    CHECK(record->request.number == 2 && record->result.data.size == 24);
    CHECK(memcmp(record->result.data.spans[0].data, txns[5].results[0], 24) ==
          0);
    release_lease(lease);
    harness_close();
}

/* A CLIENT load of a record the base file holds, queued behind a cold
 * read, and a PUBLISH of a new capture packed meanwhile: the new file has
 * the record at another offset and base_slot names it when the load is
 * issued, so the load is resolved again then (it read the new file at
 * the old offset and completed CORRUPT; the snapshot module's open item). */
static void test_client_base_moved(void)
{
    struct config c = base_config();
    struct vsr_id ids[3] = {{0xA, 1}, {0xB, 1}, {0xC, 1}};
    struct vsr_id others[2];
    struct vsr_id x1 = {0x61, 1};
    struct vsr_id x2 = {0x62, 1};
    uint64_t numbers[3] = {1, 1, 1};
    uint64_t ops[3] = {1, 1, 1};
    const struct vsr_client_record *record;
    const struct base_feed *file;
    const struct vsr_loaded *loaded;
    struct vsr_io_piece piece;
    struct vsr_id target;
    uint32_t lease = NONE;
    uint64_t sequence = 5;
    uint64_t old_offset;
    uint64_t log_op;
    uint64_t op;
    uint32_t index = NONE;

    harness_open(&c);
    harness_start(VSR_START_NEW);
    store_run(txn_identity(1, VSR_MEMBER_FULL));
    store_run(txn_append(2, 1, 16));
    store_run(txn_clients(3, 3, ids, numbers, ops, 16));
    harness_capture(x1);
    store_run(txn_publish(4, x1, 1));
    /* The file's last record is the target; the others grow before it. */
    file = feed_of(x1, false);
    CHECK(file != NULL && file->count == 3);
    target = file->records[2].request.client;
    old_offset = file->offsets[2];
    for (uint32_t i = 0, n = 0; i < 3; ++i) {
        if (ids[i].hi == target.hi) {
            index = i;
        } else {
            others[n++] = ids[i];
        }
    }
    CHECK(index != NONE);
    numbers[0] = 2;
    numbers[1] = 2;
    store_run(txn_clients(sequence, 2, others, numbers, ops, 64));
    while (vsr_io_store_hot(h.store, txns[3].file_offset,
                            (uint32_t)txns[3].bytes, &piece)) {
        store_run(txn_append(++sequence, 4, 200));
    }
    /* A cold LOG read in flight; the CLIENT load queues behind it. */
    log_op = load_log(h.store->readable, 1, 2, 1, limits.message_bytes);
    CHECK(harness_prepare() == 1 && pending_of(VSR_IO_SQE_READ) != NONE);
    op = load_client(h.store->readable, target);
    CHECK(h.store->loads_count == 2);
    /* The new capture and its PUBLISH move the record in the base file. */
    harness_capture(x2);
    file = feed_of(x2, false);
    CHECK(file->count == 3 && file->offsets[2] != old_offset);
    expect_completion(submit(txn_publish(++sequence, x2, 1)), VSR_IO_OK);
    harness_run();
    (void)expect_loaded(log_op, VSR_IO_OK, &lease);
    release_lease(lease);
    loaded = expect_loaded(op, VSR_IO_OK, &lease);
    CHECK(loaded->count == 1);
    record = loaded->items;
    CHECK(record->request.number == 1 && record->result.data.size == 16 &&
          memcmp(record->result.data.spans[0].data, txns[3].results[index],
                 16) == 0);
    release_lease(lease);
    harness_close();
}

/* -------------------------------------------------------------------------
 * An independent reading of the image
 *
 * What the format promises recovery finds, computed from the raw bytes by
 * the rules of docs/io-implementation.md sections 5 and 6.4 (decisions 48,
 * 50 and 88) and nothing of the store's scanner: the random walk and the
 * recovery fuzzer compare the store's verdict with it.
 * ---------------------------------------------------------------------- */

#define CHECKED_SLOTS 8u
#define CHECKED_OPS 4096u /* Ops the checker tracks: the walk's. */

struct checked_header {
    bool valid;
    bool visited;
    uint32_t run;
    uint32_t flags;
    uint64_t number;
    uint64_t last_sequence;
    uint64_t floor;
};

struct checked {
    int32_t status; /* OK, NOT_FOUND or CORRUPT. */
    uint64_t sequence;
    uint64_t floor;
    uint64_t resume;         /* File offset where writing resumes. */
    uint32_t run;            /* Of the chain so far. */
    uint32_t record_run;     /* Of its last record (the start header's). */
    uint32_t superblock_run; /* The superblock's. */
    uint32_t last_slot;      /* Slot of the last valid record, else the
                                start's. */
    uint32_t start_slot;
    uint32_t slots;
    uint64_t start_segment;
    uint64_t number[CHECKED_SLOTS]; /* Live segment by slot; 0 when free. */
    uint64_t log_begin; /* The log: the start header's, then as the chain's */
    uint64_t log_end;   /* APPEND, TRUNCATE and TRIM changes move it. */
    uint8_t appended[CHECKED_OPS]; /* Op appended by a replayed record. */
    bool identity;
    bool hard;
};

/* Bytes after a superblock's or a header's CRC (a superblock's reserved
 * field included) are zero, or the block is not valid. */
static bool checked_zero(const unsigned char *bytes, uint64_t size)
{
    for (uint64_t i = 0; i < size; ++i) {
        if (bytes[i] != 0) {
            return false;
        }
    }
    return true;
}

/* The valid superblock copy with the greater revision, or NULL. */
static const unsigned char *checked_superblock(void)
{
    const unsigned char *best = NULL;
    uint64_t revision = 0;

    for (uint32_t copy = 0; copy < 2; ++copy) {
        const unsigned char *b = disk_image + copy * BLOCK;

        if (vsr_io_get_u32(b) != VSR_IO_SUPERBLOCK_MAGIC ||
            vsr_io_get_u32(b + 4) != VSR_IO_STORE_FORMAT ||
            vsr_io_get_u32(b + 96) != vsr_io_crc32c(0, b, 96) ||
            !checked_zero(b + 100, BLOCK - 100)) {
            continue;
        }
        if (best == NULL || vsr_io_get_u64(b + 16) > revision) {
            best = b;
            revision = vsr_io_get_u64(b + 16);
        }
    }
    return best;
}

/* The header of `slot` when valid: magic, format, generation, a length
 * within the header blocks and the CRC before it (section 5.2). */
static void checked_header(uint32_t slot, uint64_t generation,
                           struct checked_header *out)
{
    const unsigned char *b = disk_image + slot_offset(slot);
    uint32_t length = vsr_io_get_u32(b + 24);

    memset(out, 0, sizeof(*out));
    if (vsr_io_get_u32(b) != VSR_IO_SEGMENT_MAGIC ||
        vsr_io_get_u32(b + 4) != VSR_IO_STORE_FORMAT ||
        vsr_io_get_u64(b + 8) != generation || length < 84 ||
        length > h.header_bytes ||
        vsr_io_get_u32(b + length - 4) != vsr_io_crc32c(0, b, length - 4) ||
        !checked_zero(b + length, h.header_bytes - length)) {
        return;
    }
    out->valid = true;
    out->number = vsr_io_get_u64(b + 16);
    out->flags = vsr_io_get_u32(b + 28);
    out->run = vsr_io_get_u32(b + 32);
    out->last_sequence = vsr_io_get_u64(b + 40);
    out->floor = vsr_io_get_u64(b + 48);
}

/* The length of a CRC-valid record of `generation` at `at` with `left`
 * bytes to the segment's end (section 5.3), or 0. */
static uint32_t checked_record(const unsigned char *at, uint64_t left,
                               uint64_t generation)
{
    uint32_t length;
    uint32_t count;

    if (left < 48 || vsr_io_get_u32(at) != VSR_IO_RECORD_MAGIC) {
        return 0;
    }
    length = vsr_io_get_u32(at + 4);
    if (length < 48 || length % 8 != 0 || length > left ||
        length > h.max_record ||
        vsr_io_get_u32(at + 44) != vsr_io_crc32c(0, at, 44) ||
        vsr_io_get_u64(at + 16) != generation) {
        return 0;
    }
    count = vsr_io_get_u32(at + 32);
    if (count == 0 || count > VSR_MAX_STORE_CHANGES ||
        48 + 24 * (uint64_t)count > length ||
        vsr_io_get_u32(at + 40) != vsr_io_crc32c(0, at + 48, length - 48)) {
        return 0;
    }
    return length;
}

/* Floors from the records starting in [from, limit) of a slot's data
 * that ends at `end`: every aligned position is tried, a PAD running to
 * its block's end is skipped (decision 88). */
static void checked_sweep(const unsigned char *data, uint64_t from,
                          uint64_t limit, uint64_t end, uint64_t generation,
                          uint64_t *floor)
{
    uint64_t at = from;

    while (at + 8 <= limit) {
        uint32_t length;
        uint64_t flushed;

        if (vsr_io_get_u32(data + at) == VSR_IO_PAD_MAGIC &&
            vsr_io_get_u32(data + at + 4) == BLOCK - at % BLOCK) {
            at += BLOCK - at % BLOCK;
            continue;
        }
        length = checked_record(data + at, end - at, generation);
        if (length == 0) {
            at += 8;
            continue;
        }
        flushed = vsr_io_get_u64(data + at + 24);
        if (flushed > *floor) {
            *floor = flushed;
        }
        at += length;
    }
}

/* A replayed change as the log sees it (section 6.3): an APPEND at the
 * log's end, a TRUNCATE dropping the ops from `first`, a TRIM moving the
 * begin (an empty log starts at the first op either names); IDENTITY and
 * HARD_STATE are the state a log with records needs. The walk never
 * RESTOREs, whose begin is in its payload. */
static void checked_change(struct checked *c, uint32_t type, uint32_t count,
                           uint64_t first)
{
    if ((type == VSR_STORE_APPEND || type == VSR_STORE_TRIM) &&
        c->log_begin == 0 && c->log_end == 0) {
        c->log_begin = first;
        c->log_end = first;
    }
    switch (type) {
    case VSR_STORE_APPEND:
        for (uint64_t op = first; op < first + count; ++op) {
            CHECK(op < CHECKED_OPS);
            c->appended[op] = 1;
        }
        c->log_end = first + count;
        break;
    case VSR_STORE_TRUNCATE:
        for (uint64_t op = first; op < c->log_end && op < CHECKED_OPS; ++op) {
            c->appended[op] = 0;
        }
        if (first < c->log_end) {
            c->log_end = first;
        }
        break;
    case VSR_STORE_TRIM:
        first = first < c->log_end ? first : c->log_end;
        if (first > c->log_begin) {
            c->log_begin = first;
        }
        break;
    case VSR_STORE_IDENTITY:
        c->identity = true;
        break;
    case VSR_STORE_HARD_STATE:
        c->hard = true;
        break;
    default:
        break;
    }
}

/* The chain through the data of `slot` (step 5 of 6.4): a PAD to its
 * block's end skips, a valid record continues when it is the next
 * sequence with a run not below the last one; anything else inside a
 * block is the dead tail of the last valid record's block, swept for
 * floors and skipped (decision 110), at a block boundary it ends the
 * chain; the rest of the slot is swept for floors. */
static void checked_scan(struct checked *c, uint32_t slot, uint64_t generation)
{
    const unsigned char *data = disk_image + slot_offset(slot);
    uint64_t end = h.options.segment_bytes;
    uint64_t at = h.header_bytes;

    while (at < end) {
        uint64_t block_left = BLOCK - at % BLOCK;
        uint64_t sequence;
        uint64_t flushed;
        uint32_t length;
        uint32_t count;
        uint32_t run;
        bool next;

        if (vsr_io_get_u32(data + at) == VSR_IO_PAD_MAGIC &&
            vsr_io_get_u32(data + at + 4) == block_left) {
            at += block_left;
            continue;
        }
        length = checked_record(data + at, end - at, generation);
        sequence = length != 0 ? vsr_io_get_u64(data + at + 8) : 0;
        run = length != 0 ? vsr_io_get_u32(data + at + 36) : 0;
        flushed = length != 0 ? vsr_io_get_u64(data + at + 24) : 0;
        /* The next sequence, a run not below the chain's, and a later
         * run's record carrying its predecessor as flushed (116). */
        next = length != 0 && sequence == c->sequence + 1 && run >= c->run &&
               (run <= c->record_run || flushed >= c->sequence);
        if (!next) {
            if (block_left == BLOCK) {
                break;
            }
            checked_sweep(data, at, at + block_left, end, generation,
                          &c->floor);
            at += block_left;
            continue;
        }
        count = vsr_io_get_u32(data + at + 32);
        if (flushed > c->floor) {
            c->floor = flushed;
        }
        c->sequence = sequence;
        c->run = run;
        c->record_run = run;
        c->last_slot = slot;
        c->resume = slot_offset(slot) + round_up(at + length, BLOCK);
        for (uint32_t i = 0; i < count; ++i) {
            const unsigned char *change = data + at + 48 + 24 * (uint64_t)i;

            checked_change(c, vsr_io_get_u32(change),
                           vsr_io_get_u32(change + 4),
                           vsr_io_get_u64(change + 8));
        }
        at += length;
    }
    checked_sweep(data, at, end, end, generation, &c->floor);
}

/* The image as the checker reads it, on stderr (the walk's trace): the
 * superblocks, each slot's header and every CRC-valid record or PAD at an
 * aligned position of its data. */
static void checked_dump(void)
{
    const unsigned char *sb = checked_superblock();
    uint64_t generation = sb != NULL ? vsr_io_get_u64(sb + 8) : 0;
    uint64_t slots = disk.size > 2 * BLOCK
                         ? (disk.size - 2 * BLOCK) / h.options.segment_bytes
                         : 0;

    for (uint32_t copy = 0; copy < 2; ++copy) {
        const unsigned char *b = disk_image + copy * BLOCK;

        fprintf(stderr,
                "image: superblock %u: revision %" PRIu64 " start %" PRIu64
                "/%u run %u floor %" PRIu64 "%s\n",
                copy, vsr_io_get_u64(b + 16), vsr_io_get_u64(b + 72),
                vsr_io_get_u32(b + 80), vsr_io_get_u32(b + 84),
                vsr_io_get_u64(b + 88), b == sb ? " (chosen)" : "");
    }
    for (uint32_t slot = 0; slot < slots && slot < CHECKED_SLOTS; ++slot) {
        const unsigned char *data = disk_image + slot_offset(slot);
        struct checked_header header;

        checked_header(slot, generation, &header);
        fprintf(stderr,
                "image: slot %u: header %s segment %" PRIu64 " last %" PRIu64
                " run %u floor %" PRIu64 "\n",
                slot, header.valid ? "valid" : "invalid", header.number,
                header.last_sequence, header.run, header.floor);
        for (uint64_t at = h.header_bytes; at + 8 <= h.options.segment_bytes;
             at += 8) {
            uint32_t length = checked_record(
                data + at, h.options.segment_bytes - at, generation);

            if (length != 0) {
                fprintf(stderr,
                        "image:   %" PRIu64 ": record %" PRIu64 " run %u "
                        "flushed %" PRIu64 " length %u\n",
                        at, vsr_io_get_u64(data + at + 8),
                        vsr_io_get_u32(data + at + 36),
                        vsr_io_get_u64(data + at + 24), length);
            } else if (vsr_io_get_u32(data + at) == VSR_IO_PAD_MAGIC) {
                fprintf(stderr, "image:   %" PRIu64 ": pad %u\n", at,
                        vsr_io_get_u32(data + at + 4));
            }
        }
    }
}

/* The verdict on the image as it is now, under the harness
 * configuration. */
static void checked_image(struct checked *c)
{
    struct checked_header headers[CHECKED_SLOTS];
    const unsigned char *sb = checked_superblock();
    uint64_t generation;
    uint32_t slot;

    memset(c, 0, sizeof(*c));
    memset(headers, 0, sizeof(headers));
    c->status = VSR_IO_CORRUPT;
    if (sb == NULL) {
        return;
    }
    generation = vsr_io_get_u64(sb + 8);
    if (vsr_io_get_u32(sb + 52) != BLOCK ||
        vsr_io_get_u64(sb + 56) != h.options.segment_bytes ||
        vsr_io_get_u32(sb + 64) != h.header_bytes / BLOCK) {
        return;
    }
    CHECK(disk.size >= 2 * BLOCK);
    c->slots = (uint32_t)((disk.size - 2 * BLOCK) / h.options.segment_bytes);
    if (c->slots < vsr_io_get_u32(sb + 68)) {
        return;
    }
    CHECK(c->slots <= CHECKED_SLOTS && c->slots <= h.options.max_segments);
    c->superblock_run = vsr_io_get_u32(sb + 84);
    c->floor = vsr_io_get_u64(sb + 88);
    c->start_segment = vsr_io_get_u64(sb + 72);
    c->start_slot = vsr_io_get_u32(sb + 80);
    for (slot = 0; slot < c->slots; ++slot) {
        checked_header(slot, generation, &headers[slot]);
        if (headers[slot].valid && headers[slot].floor > c->floor) {
            c->floor = headers[slot].floor;
        }
    }
    slot = c->start_slot;
    if (slot >= c->slots || !headers[slot].valid ||
        headers[slot].number != c->start_segment) {
        return;
    }
    headers[slot].visited = true;
    c->sequence = headers[slot].last_sequence;
    c->run = headers[slot].run;
    c->record_run = headers[slot].run;
    c->identity = (headers[slot].flags & VSR_IO_SEGMENT_STATE) != 0;
    c->hard = c->identity;
    c->log_begin = vsr_io_get_u64(disk_image + slot_offset(slot) + 64);
    c->log_end = vsr_io_get_u64(disk_image + slot_offset(slot) + 72);
    c->last_slot = slot;
    c->resume = slot_offset(slot) + h.header_bytes;
    for (;;) {
        uint32_t best = NONE;

        checked_scan(c, slot, generation);
        for (uint32_t s = 0; s < c->slots; ++s) {
            const struct checked_header *header = &headers[s];

            if (header->valid && !header->visited &&
                header->last_sequence == c->sequence && header->run >= c->run &&
                (best == NONE || header->number > headers[best].number)) {
                best = s;
            }
        }
        if (best == NONE) {
            break;
        }
        slot = best;
        headers[slot].visited = true;
        if (headers[slot].run > c->run) {
            c->run = headers[slot].run;
        }
    }
    for (uint32_t s = 0; s < c->slots; ++s) {
        if (!headers[s].visited) {
            checked_sweep(disk_image + slot_offset(s), h.header_bytes,
                          h.options.segment_bytes, h.options.segment_bytes,
                          generation, &c->floor);
        }
    }
    if (c->sequence < c->floor) {
        return;
    }
    if (c->sequence > 0 && !(c->identity && c->hard)) {
        return;
    }
    /* Every op of the recovered log was replayed (decision 114). */
    for (uint64_t op = c->log_begin; op < c->log_end; ++op) {
        if (op >= CHECKED_OPS || c->appended[op] == 0) {
            return;
        }
    }
    for (uint32_t s = 0; s < c->slots; ++s) {
        /* Visited and not an abandoned successor. */
        if (headers[s].visited &&
            (s == c->last_slot || headers[s].last_sequence < c->sequence)) {
            c->number[s] = headers[s].number;
        }
    }
    c->status = c->sequence == 0 ? VSR_IO_NOT_FOUND : VSR_IO_OK;
}

/* -------------------------------------------------------------------------
 * Random walk
 *
 * A byte stream drives STOREs of every kind, SYNCs, RECLAIMs, LOADs (hot
 * and cold), admissions and replies, idle time, and crashes with a torn
 * write, lost unflushed blocks or flipped bytes, each followed by a
 * recovery, against a model of the log and the clients' records built
 * from the transaction table: what was acknowledged durable is recovered
 * (unless bytes were flipped), the recovered verdict is the checker's,
 * the recovered indexes are the pre-crash snapshot's and the segment
 * table the checker's, everything the model holds reads back identically
 * after every recovery and along the way, and the disk model's
 * never-rewrite bits hold across crashes. The unit test drives it from a
 * seeded generator, the recovery fuzzer from its input.
 * ---------------------------------------------------------------------- */

#define WALK_CLIENTS 3u
#define WALK_OPS 4096u
#define WALK_MAX_LOG 200u
#define WALK_BYTES 4096u
#define WALK_EXPECTED 64u

enum walk_kind { WALK_STORE, WALK_SYNC, WALK_RECLAIM };

struct walk_entry {
    uint64_t sequence; /* Transaction that appended the op; 0 none. */
    uint32_t index;
};

struct walk_client {
    uint64_t sequence; /* Latest CLIENTS record: transaction, index. */
    uint64_t number;
    uint64_t op;
    uint32_t index;
};

struct walk_stats {
    uint64_t steps;
    uint64_t stores;
    uint64_t held;
    uint64_t syncs;
    uint64_t reclaims;
    uint64_t loads;
    uint64_t cold;
    uint64_t captures;
    uint64_t crashes;
    uint64_t torn;
    uint64_t lost;
    uint64_t flips;
    uint64_t recovered;
    uint64_t not_found;
    uint64_t corrupt;
};

struct walk {
    struct config c;
    const uint8_t *bytes;
    size_t size;
    size_t at;
    uint64_t sequence;    /* Last submitted (or recovered) transaction. */
    uint64_t durable_ack; /* Greatest sequence acknowledged durable. */
    uint32_t captures;
    bool trace;      /* VSR_WALK_TRACE set: every step on stderr. */
    bool corrupting; /* Bytes flipped at the last crash. */
    bool failed;     /* The store is fenced, or bytes were flipped: the
                        walk is over. */
    bool exhausted;  /* Out of snapshot ids: the walk is over. */
    uint64_t log_begin;
    uint64_t log_end;
    struct walk_entry entries[WALK_OPS];
    struct walk_client clients[WALK_CLIENTS];
    uint64_t next_number[WALK_CLIENTS];
    struct {
        uint64_t op;
        uint64_t sequence;
        uint32_t kind;
    } expected[WALK_EXPECTED];
    uint32_t expected_count;
    struct walk_stats stats;
};

static struct walk walk;
static const struct vsr_id walk_ids[WALK_CLIENTS] = {
    {0x21, 1}, {0x22, 1}, {0x23, 1}};

static uint8_t walk_byte(void)
{
    if (walk.at >= walk.size) {
        walk.at = walk.size + 1;
        return 0;
    }
    return walk.bytes[walk.at++];
}

/* The configuration from the first byte: sync mode, records per segment,
 * slots, write-behind, writes in flight. */
static struct config walk_config(uint8_t bits)
{
    struct config c = base_config();
    uint64_t header_bytes;
    uint64_t max_record;
    uint64_t records = 4 + ((bits >> 1) & 3);

    derive(&header_bytes, &max_record);
    c.segment_bytes = round_up(header_bytes + records * max_record, BLOCK);
    c.segments = 2;
    c.max_segments = 3 + ((bits >> 3) & 1);
    c.write_behind_bytes = (bits & 0x20) != 0 ? 4 * BLOCK : 2 * BLOCK;
    c.cache_bytes =
        round_up(c.write_behind_bytes + limits.pinned_payload_bytes +
                     2 * max_record + 2 * header_bytes + BLOCK,
                 BLOCK);
    c.inflight_writes = (bits & 0x40) != 0 ? 3 : 2;
    c.sync_mode = (bits & 1) != 0 ? VSR_IO_SYNC_DSYNC : VSR_IO_SYNC_FDATASYNC;
    return c;
}

static uint32_t walk_client_index(struct vsr_id id)
{
    for (uint32_t i = 0; i < WALK_CLIENTS; ++i) {
        if (walk_ids[i].hi == id.hi && walk_ids[i].lo == id.lo) {
            return i;
        }
    }
    return NONE;
}

/* The model's step over transaction `sequence`. */
static void walk_apply(uint64_t sequence)
{
    const struct txn *t = &txns[sequence];

    for (uint32_t i = 0; i < t->store.count; ++i) {
        const struct vsr_change *change = &t->store.changes[i];

        switch (change->type) {
        case VSR_STORE_APPEND:
            for (uint32_t k = 0; k < change->count; ++k) {
                const struct vsr_entry *entry = &t->entries[k];
                uint32_t ci = walk_client_index(entry->request.client);

                CHECK(entry->op < WALK_OPS);
                walk.entries[entry->op].sequence = sequence;
                walk.entries[entry->op].index = k;
                if (ci != NONE &&
                    entry->request.number >= walk.next_number[ci]) {
                    walk.next_number[ci] = entry->request.number + 1;
                }
            }
            walk.log_end = change->first + change->count;
            break;
        case VSR_STORE_TRUNCATE:
            if (change->first < walk.log_end) {
                walk.log_end = change->first;
            }
            break;
        case VSR_STORE_TRIM:
            if (change->first > walk.log_begin) {
                walk.log_begin =
                    change->first < walk.log_end ? change->first : walk.log_end;
            }
            break;
        case VSR_STORE_CLIENTS:
            for (uint32_t k = 0; k < change->count; ++k) {
                const struct vsr_client_record *record = &t->records[k];
                uint32_t ci = walk_client_index(record->request.client);

                CHECK(ci != NONE);
                if (record->request.number > walk.clients[ci].number) {
                    walk.clients[ci].sequence = sequence;
                    walk.clients[ci].number = record->request.number;
                    walk.clients[ci].op = record->op;
                    walk.clients[ci].index = k;
                }
                if (record->request.number >= walk.next_number[ci]) {
                    walk.next_number[ci] = record->request.number + 1;
                }
            }
            break;
        default:
            break;
        }
    }
    walk.sequence = sequence;
}

/* The model as of `sequence`, from the transactions 1..sequence. */
static void walk_replay(uint64_t sequence)
{
    walk.log_begin = 1;
    walk.log_end = 1;
    memset(walk.entries, 0, sizeof(walk.entries));
    memset(walk.clients, 0, sizeof(walk.clients));
    for (uint32_t i = 0; i < WALK_CLIENTS; ++i) {
        walk.next_number[i] = 1;
    }
    walk.sequence = 0;
    for (uint64_t q = 1; q <= sequence; ++q) {
        walk_apply(q);
    }
}

static void walk_expect(uint64_t op, uint32_t kind, uint64_t sequence)
{
    CHECK(walk.expected_count < WALK_EXPECTED);
    walk.expected[walk.expected_count].op = op;
    walk.expected[walk.expected_count].kind = kind;
    walk.expected[walk.expected_count].sequence = sequence;
    walk.expected_count++;
}

/* Every queued completion must be an expected op, OK; a SYNC's sequence
 * is acknowledged durable. */
static void walk_drain(void)
{
    struct vsr_io_completion completion;

    while (next_completion(&completion)) {
        uint32_t at = NONE;

        for (uint32_t i = 0; i < walk.expected_count; ++i) {
            if (walk.expected[i].op == completion.op) {
                at = i;
            }
        }
        CHECK(at != NONE);
        if (completion.status != VSR_IO_OK) {
            fprintf(stderr, "walk: op %" PRIu64 " kind %" PRIu32 " status %d\n",
                    completion.op, walk.expected[at].kind, completion.status);
        }
        CHECK(completion.status == VSR_IO_OK);
        CHECK(completion.lease == NONE && completion.data == NULL);
        if (walk.expected[at].kind == WALK_SYNC &&
            walk.expected[at].sequence > walk.durable_ack) {
            walk.durable_ack = walk.expected[at].sequence;
        }
        walk.expected[at] = walk.expected[walk.expected_count - 1];
        walk.expected_count--;
    }
}

/* Drives everything submitted to completion, a wanted base fed. */
static void walk_settle(void)
{
    harness_run();
    if (vsr_io_store_base_wanted(h.store, NULL, NULL)) {
        feed_base();
        harness_run();
    }
    walk_drain();
}

/* Room for one more op: the core never has more than `operations` ops
 * outstanding (the store sizes its queues and completion ring by it), so
 * the walk takes the completions, or waits for them, first. */
static void walk_room(void)
{
    if (walk.expected_count + 1 >= limits.operations) {
        walk_drain();
    }
    if (walk.expected_count + 1 >= limits.operations) {
        walk_settle();
    }
}

/* Submits a transaction: packed at once it is snapshotted now, held it
 * is driven until it is. */
static void walk_submit(const struct txn *t)
{
    uint64_t op;

    walk_room();
    op = submit(t);
    walk_expect(op, WALK_STORE, t->store.sequence);
    walk_apply(t->store.sequence);
    walk.stats.stores++;
    if (h.store->readable != t->store.sequence) {
        walk.stats.held++;
        walk_settle();
        CHECK(h.store->readable == t->store.sequence);
    }
    snapshot_take();
}

static uint64_t walk_checkpoint_op(void)
{
    return walk.log_end > 1 ? walk.log_end - 1 : 1;
}

/* The transaction of a STORE step: its kind from the action, its shape
 * from `arg`; a log grown long is trimmed instead, a truncation with
 * many versions kept becomes an append. */
static const struct txn *walk_txn(uint8_t action, uint8_t arg)
{
    uint64_t q = walk.sequence + 1;
    uint64_t begin = walk.log_begin;
    uint64_t end = walk.log_end;
    struct vsr_id ids[2];
    uint64_t numbers[2];
    uint64_t ops[2];
    uint32_t ci = arg % WALK_CLIENTS;
    uint32_t count = 1 + ((arg >> 2) & 3);
    size_t body = (size_t)(arg >> 2) * 4;
    struct txn *t;

    if (q == 1) {
        return txn_identity(1, VSR_MEMBER_FULL);
    }
    if (end - begin > WALK_MAX_LOG) {
        return txn_trim(q, end - 1);
    }
    switch (action) {
    case 4:
        t = txn_begin(q);
        txn_entries(t, end, 1 + ((arg >> 2) % 3), body, walk_ids[ci],
                    walk.next_number[ci]);
        return txn_finish(t);
    case 5:
        if (end - 1 <= begin || h.store->versions_count > 100) {
            break;
        }
        t = txn_begin(q);
        txn_add(t, VSR_STORE_TRUNCATE,
                end - 1 - (arg % 3) < begin + 1 ? begin + 1
                                                : end - 1 - (arg % 3),
                0, NULL);
        if ((arg & 0x80) != 0) {
            struct vsr_id none = {0, 0};

            txn_entries(t, t->changes[0].first, count, body, none, 0);
        }
        return txn_finish(t);
    case 6:
        if (end <= begin + 1) {
            break;
        }
        return txn_trim(q, begin + 1 + (arg % 4) < end ? begin + 1 + (arg % 4)
                                                       : end - 1);
    case 7:
        ids[0] = walk_ids[ci];
        ids[1] = walk_ids[(ci + 1) % WALK_CLIENTS];
        numbers[0] = walk.next_number[ci];
        numbers[1] = walk.next_number[(ci + 1) % WALK_CLIENTS];
        ops[0] = walk_checkpoint_op();
        ops[1] = ops[0];
        return txn_clients(q, 1 + ((arg >> 2) & 1), ids, numbers, ops,
                           (size_t)(arg >> 3) % 65);
    case 8:
        return txn_hard(q, VSR_MEMBER_FULL);
    default:
        break;
    }
    return txn_append(q, count, body);
}

/* A capture of the table under a fresh id, then its PUBLISH. */
static void walk_publish(void)
{
    struct vsr_id id = {0x60 + walk.captures, 1};

    if (walk.sequence == 0) {
        walk_submit(walk_txn(0, 8));
        return;
    }
    if (walk.captures + 1 >= FEEDS) {
        walk.exhausted = true;
        return;
    }
    walk_settle();
    harness_capture(id);
    walk.captures++;
    walk.stats.captures++;
    walk_submit(txn_publish(walk.sequence + 1, id, walk_checkpoint_op()));
    walk_settle();
}

static void walk_reclaim(uint64_t oldest)
{
    walk_room();
    walk_expect(submit_reclaim(oldest), WALK_RECLAIM, oldest);
    walk.stats.reclaims++;
}

/* A SYNC of `sequence`. */
static void walk_sync(uint64_t sequence)
{
    walk_room();
    walk_expect(submit_sync(sequence), WALK_SYNC, sequence);
    walk.stats.syncs++;
}

/* What the core does so that slots free: once no slot is free, a fresh
 * record at the log's end, a TRIM to it, a capture and its PUBLISH, a
 * RECLAIM after each; then everything but the current slot is below
 * the floor. */
static void walk_housekeep(void)
{
    if (walk.sequence == 0 || walk.exhausted) {
        return;
    }
    for (uint32_t slot = 0; slot < h.store->slots; ++slot) {
        uint32_t phase = h.store->segments[slot].phase;

        if (phase == VSR_IO_SEGMENT_FREE || phase == VSR_IO_SEGMENT_FREEING) {
            return;
        }
    }
    walk_settle();
    walk_submit(txn_append(walk.sequence + 1, 1, 8));
    walk_reclaim(h.store->readable);
    walk_settle();
    walk_submit(txn_trim(walk.sequence + 1, walk.log_end - 1));
    walk_reclaim(h.store->readable);
    walk_settle();
    walk_publish();
    walk_reclaim(h.store->readable);
    walk_settle();
}

/* The base file of the current base, as the snapshot module keeps it
 * open: rewritten from its feed after a recovery. */
static void feed_install(struct vsr_id id)
{
    struct base_feed *file = feed_of(id, false);

    CHECK(file != NULL);
    base_file_write(file->records, file->count, file->offsets);
}

/* [first, end) of the log reads back as the model's entries. */
static void walk_check_log(uint64_t first, uint64_t end)
{
    const struct vsr_loaded *loaded;
    const struct vsr_entry *entries;
    uint32_t reads = disk.reads;
    uint32_t lease = NONE;

    while (first < end) {
        uint64_t op =
            load_log(h.store->readable, first, end, 4, limits.message_bytes);

        harness_run();
        loaded = expect_loaded(op, VSR_IO_OK, &lease);
        /* A cold batch is cut to a slab: at least one entry comes. */
        CHECK(loaded->count >= 1 && loaded->count <= end - first);
        CHECK(loaded->next == first + loaded->count);
        CHECK(loaded->sequence == h.store->readable);
        entries = loaded->items;
        for (uint32_t i = 0; i < loaded->count; ++i) {
            const struct walk_entry *e = &walk.entries[first + i];

            CHECK(e->sequence != 0);
            check_entry(&entries[i], &txns[e->sequence], e->index);
        }
        release_lease(lease);
        walk.stats.loads++;
        if (disk.reads > reads) {
            walk.stats.cold++;
        }
        first = loaded->next;
    }
}

/* The client's latest completed record reads back as the model's. */
static void walk_check_client(uint32_t ci)
{
    const struct walk_client *m = &walk.clients[ci];
    const struct vsr_loaded *loaded;
    uint32_t reads = disk.reads;
    uint32_t lease = NONE;
    uint64_t op = load_client(h.store->readable, walk_ids[ci]);

    harness_run();
    loaded = expect_loaded(op, VSR_IO_OK, &lease);
    if (m->number == 0) {
        CHECK(loaded->count == 0);
    } else {
        const struct txn *t = &txns[m->sequence];
        const struct vsr_client_record *record = loaded->items;
        uint64_t size = t->records[m->index].result.data.size;

        CHECK(loaded->count == 1);
        if (record->request.number != m->number || record->op != m->op) {
            fprintf(stderr,
                    "walk: client %u loaded number %" PRIu64 " op %" PRIu64
                    ", the model's %" PRIu64 " op %" PRIu64
                    " (sequence %" PRIu64 ")\n",
                    ci, record->request.number, record->op, m->number, m->op,
                    m->sequence);
        }
        CHECK(record->request.number == m->number && record->op == m->op);
        CHECK(record->result.data.size == size);
        if (size > 0) {
            CHECK(record->result.data.count == 1 &&
                  memcmp(record->result.data.spans[0].data,
                         t->results[m->index], (size_t)size) == 0);
        }
    }
    release_lease(lease);
    walk.stats.loads++;
    if (disk.reads > reads) {
        walk.stats.cold++;
    }
}

/* The client's retained entry is its latest in the model's log. */
static void walk_check_request(uint32_t ci)
{
    const struct vsr_loaded *loaded;
    uint32_t reads = disk.reads;
    uint32_t lease = NONE;
    uint64_t found = 0;
    uint64_t op;

    for (uint64_t at = walk.log_end; at > walk.log_begin && found == 0; --at) {
        const struct walk_entry *e = &walk.entries[at - 1];
        const struct vsr_entry *entry = &txns[e->sequence].entries[e->index];

        if (e->sequence != 0 &&
            walk_client_index(entry->request.client) == ci) {
            found = at - 1;
        }
    }
    op = load_request(h.store->readable, walk_ids[ci]);
    harness_run();
    loaded = expect_loaded(op, VSR_IO_OK, &lease);
    if (found == 0) {
        CHECK(loaded->count == 0);
    } else {
        const struct vsr_entry *entry = loaded->items;
        const struct walk_entry *e = &walk.entries[found];

        CHECK(loaded->count == 1 && entry->op == found);
        check_entry(entry, &txns[e->sequence], e->index);
    }
    release_lease(lease);
    walk.stats.loads++;
    if (disk.reads > reads) {
        walk.stats.cold++;
    }
}

/* Everything the model holds reads back. */
static void walk_audit(void)
{
    for (uint64_t first = walk.log_begin; first < walk.log_end; first += 4) {
        walk_check_log(first,
                       first + 4 < walk.log_end ? first + 4 : walk.log_end);
    }
    for (uint32_t ci = 0; ci < WALK_CLIENTS; ++ci) {
        walk_check_client(ci);
        walk_check_request(ci);
    }
}

/* The segment table after a recovery is the checker's. */
static void walk_check_segments(const struct checked *c)
{
    CHECK(h.store->slots == c->slots);
    for (uint32_t slot = 0; slot < c->slots; ++slot) {
        const struct vsr_io_segment *segment = &h.store->segments[slot];

        CHECK(segment->number == c->number[slot]);
        if (segment->number == 0) {
            CHECK(segment->phase == VSR_IO_SEGMENT_FREE);
        } else if (slot == c->last_slot) {
            CHECK(segment->phase == VSR_IO_SEGMENT_OPEN);
        } else {
            CHECK(segment->phase == VSR_IO_SEGMENT_SEALED);
        }
    }
    CHECK(h.store->current == c->last_slot);
    CHECK(h.store->file_head == c->resume);
    CHECK(h.store->run == c->superblock_run + 1);
    CHECK(h.store->start_slot == c->start_slot);
    CHECK(h.store->start_segment == c->start_segment);
}

/* A crash: perhaps with a STORE and a SYNC just submitted and the next
 * write torn, perhaps losing unflushed blocks, perhaps flipping bytes;
 * then the recovery, checked. */
static void walk_crash(uint8_t arg)
{
    const struct vsr_loaded *loaded = NULL;
    struct checked checked;
    uint32_t lease = NONE;
    uint64_t packed;
    uint64_t media;
    int32_t status;

    walk.corrupting = false;
    walk_housekeep();
    if ((arg & 1) != 0 && !walk.exhausted) {
        /* Read in order: the order of a call's arguments is the
         * compiler's, and a walk must replay under both. */
        uint8_t action = walk_byte();
        uint8_t shape = walk_byte();
        const struct txn *t = walk_txn(action % 9, shape);
        uint64_t op;

        walk_room();
        op = submit(t);

        walk_expect(op, WALK_STORE, t->store.sequence);
        walk_apply(t->store.sequence);
        walk.stats.stores++;
    }
    if ((arg & 2) != 0 && h.store->readable > 0) {
        walk_sync(h.store->readable);
    }
    if ((arg & 4) != 0) {
        disk.tear_armed = 1;
        disk.tear_blocks = (arg >> 3) & 3;
        walk.stats.torn++;
    }
    harness_run(); /* Until the lost completion stalls it. */
    disk.tear_armed = 0;
    packed = h.store->readable;
    /* The sequence on media, which no crash loses: freeing counted on
     * every revision from it on (decision 112). */
    media = walk.c.sync_mode == VSR_IO_SYNC_DSYNC ? h.store->written
                                                  : h.store->flushed;
    if (packed > 0) {
        snapshot_take();
    }
    if ((arg & 0x40) != 0 && walk.c.sync_mode == VSR_IO_SYNC_FDATASYNC) {
        for (uint64_t at = 0; at < disk.size / BLOCK; ++at) {
            if (disk.dirty[at] != 0 && (walk_byte() & 1) != 0) {
                disk_lose_block(at);
                walk.stats.lost++;
            }
        }
    }
    if ((arg & 0xF0) == 0xF0) {
        uint32_t flips = 1 + walk_byte() % 2;

        for (uint32_t i = 0; i < flips; ++i) {
            uint64_t offset = walk_byte();
            unsigned char bit;

            offset = offset << 8 | walk_byte();
            offset = (offset << 8 | walk_byte()) % disk.size;
            bit = (unsigned char)(1u << (walk_byte() & 7));
            /* On media: a later crash reverting the block to its flushed
             * content keeps the flip. */
            disk_image[offset] ^= bit;
            disk_flushed[offset] ^= bit;
            walk.stats.flips++;
        }
        walk.corrupting = true;
    }
    disk_crash(false);
    walk.stats.crashes++;
    txn_salt = (uint32_t)walk.stats.crashes; /* The next run's bytes. */
    walk.expected_count = 0;
    harness_open_keep(&walk.c, true);
    checked_image(&checked); /* Before the recovery's superblock write. */
    if (walk.trace) {
        checked_dump();
    }
    status = harness_recover(VSR_START_RECOVER, &loaded, &lease);
    if (status != checked.status ||
        (status == VSR_IO_OK && loaded->sequence != checked.sequence)) {
        fprintf(stderr,
                "walk: recovery %d/%" PRIu64 ", the checker %d/%" PRIu64
                " (floor %" PRIu64 ")\n",
                status, status == VSR_IO_OK ? loaded->sequence : 0,
                checked.status, checked.sequence, checked.floor);
        CHECK(false);
    }
    switch (status) {
    case VSR_IO_OK:
        CHECK(loaded->count == 1 && loaded->sequence <= packed);
        CHECK(((const struct vsr_recovered *)loaded->items)->sequence ==
              loaded->sequence);
        CHECK(walk.corrupting || loaded->sequence >= walk.durable_ack);
        CHECK(walk.corrupting || loaded->sequence >= media);
        if (loaded->sequence < media) {
            /* Flipped bytes cut records on media no floor covers, which
             * recovery takes for a torn tail (decision 50's residual):
             * slots freed for the revisions from `media` on may hold what
             * this older row needs, so it is not checked against the
             * model. The walk ends below. */
            release_lease(lease);
            walk.stats.recovered++;
            break;
        }
        walk_replay(loaded->sequence);
        walk.durable_ack = loaded->sequence;
        release_lease(lease);
        snapshot_check(walk.sequence);
        walk_check_segments(&checked);
        if (feed.fed) {
            feed_install(feed.id);
        }
        walk_audit();
        walk_reclaim(h.store->readable); /* The core's floor. */
        walk_settle();
        walk.stats.recovered++;
        break;
    case VSR_IO_NOT_FOUND:
        CHECK(walk.corrupting || walk.durable_ack == 0);
        walk_replay(0);
        walk.durable_ack = 0;
        walk_check_segments(&checked);
        walk.stats.not_found++;
        break;
    default:
        CHECK(status == VSR_IO_CORRUPT && walk.corrupting);
        CHECK(h.store->state == VSR_IO_STORE_FAILED);
        walk.failed = true;
        walk.stats.corrupt++;
        break;
    }
    /* After flipped bytes the walk ends once their recovery is checked:
     * the records a flip cut off stay on media, CRC-valid behind the
     * damage, and a later crash that loses the block a new run rewrote
     * over the damage can bring them back (an unacknowledged suffix of an
     * older run), which the model of the log cannot follow. */
    if (walk.corrupting) {
        walk.failed = true;
    }
}

static void walk_step(uint8_t action, uint8_t arg)
{
    uint32_t ci = arg % WALK_CLIENTS;

    if (walk.trace) {
        fprintf(stderr,
                "walk: step %" PRIu64 " action %u arg %u: readable %" PRIu64
                " written %" PRIu64 " flushed %" PRIu64 " durable ack %" PRIu64
                " reclaim %" PRIu64 "/%" PRIu64 " base %" PRIu64 "/%" PRIu64
                " pending %" PRIu64 "\n",
                walk.stats.steps, action, arg, h.store->readable,
                h.store->written, h.store->flushed, walk.durable_ack,
                h.store->reclaim, h.store->reclaimed, h.store->client_base,
                h.store->client_base_floor, h.store->client_base_pending);
    }
    h.now += (uint64_t)(action >> 4) * 10000000; /* Idle writes fire. */
    switch (action & 15) {
    case 9:
        walk_housekeep(); /* A PUBLISH is a record too. */
        if (!walk.exhausted) {
            walk_publish();
        }
        break;
    case 10:
        if (h.store->readable > 0) {
            /* A little behind the packed sequence, never below 1. */
            uint64_t sequence =
                h.store->readable > arg % 3 ? h.store->readable - arg % 3 : 1;

            walk_sync(sequence);
            if ((arg & 4) != 0) {
                walk_settle();
            }
        }
        break;
    case 11:
        walk_reclaim(h.store->readable > arg % 6 ? h.store->readable - arg % 6
                                                 : 0);
        break;
    case 12:
        walk_settle();
        if (walk.log_end > walk.log_begin) {
            uint64_t span = walk.log_end - walk.log_begin;
            uint64_t first = walk.log_begin + (uint64_t)arg * 7u % span;
            uint64_t end = first + 1 + ((arg >> 3) & 3);

            walk_check_log(first, end < walk.log_end ? end : walk.log_end);
        }
        walk_check_client(ci);
        walk_check_request(ci);
        break;
    case 13:
        CHECK(vsr_io_store_admit(h.store, walk_ids[ci]));
        if ((arg & 4) != 0) {
            vsr_io_store_replied(h.store, walk_ids[ci]);
        }
        break;
    case 14:
        walk_crash(arg);
        break;
    case 15:
        walk_settle();
        break;
    default:
        walk_housekeep();
        if (!walk.exhausted) {
            walk_submit(walk_txn(action & 15, arg));
        }
        break;
    }
}

/* One walk over `bytes`: the configuration, then two bytes per step. */
static void walk_run(const uint8_t *bytes, size_t size)
{
    memset(&walk, 0, sizeof(walk));
    walk.trace = getenv("VSR_WALK_TRACE") != NULL;
    disk_trace = walk.trace;
    walk.bytes = bytes;
    walk.size = size;
    walk.c = walk_config(walk_byte());
    walk_replay(0);
    harness_open(&walk.c);
    harness_start(VSR_START_NEW);
    while (walk.at < walk.size && !walk.failed && !walk.exhausted &&
           walk.sequence + 8 < SNAPSHOTS && walk.log_end + 8 < WALK_OPS) {
        uint8_t action = walk_byte();
        uint8_t arg = walk_byte();

        walk.stats.steps++;
        walk_step(action, arg);
    }
    if (!walk.failed) {
        walk_settle();
        walk_audit();
    }
    harness_close();
}

/* splitmix64 over the seed. */
static void walk_bytes(uint64_t seed, uint8_t *bytes, size_t size)
{
    uint64_t state = seed;

    for (size_t i = 0; i < size; i += 8) {
        uint64_t z;

        state += UINT64_C(0x9E3779B97F4A7C15);
        z = state;
        z = (z ^ (z >> 30)) * UINT64_C(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)) * UINT64_C(0x94D049BB133111EB);
        z ^= z >> 31;
        for (size_t b = 0; b < 8 && i + b < size; ++b) {
            bytes[i + b] = (uint8_t)(z >> (8 * b));
        }
    }
}

/* A seeded walk, its seed and statistics printed; the bytes go to the
 * fuzz corpus when VSR_RECOVERY_CORPUS names a directory. */
static void walk_seeded(uint64_t seed, size_t size)
{
    static uint8_t bytes[WALK_BYTES];
    const char *corpus = getenv("VSR_RECOVERY_CORPUS");
    const struct walk_stats *s = &walk.stats;

    CHECK(size <= sizeof(bytes));
    walk_bytes(seed, bytes, size);
    if (corpus != NULL) {
        char path[512];
        FILE *file;

        CHECK(snprintf(path, sizeof(path), "%s/walk-%016" PRIx64, corpus,
                       seed) < (int)sizeof(path));
        file = fopen(path, "wb");
        CHECK(file != NULL && fwrite(bytes, 1, size, file) == size);
        CHECK(fclose(file) == 0);
    }
    /* On stderr, unbuffered: a failing walk names its seed. */
    fprintf(stderr, "store: walk seed %" PRIu64 " (%s)\n", seed,
            size > 0 && (bytes[0] & 1) != 0 ? "dsync" : "fdatasync");
    walk_run(bytes, size);
    fprintf(stderr,
            "store: walk seed %" PRIu64 ": %" PRIu64 " steps, %" PRIu64
            " stores (%" PRIu64 " held), %" PRIu64 " syncs, %" PRIu64
            " reclaims, %" PRIu64 " loads (%" PRIu64 " cold), %" PRIu64
            " captures, %" PRIu64 " crashes (%" PRIu64 " torn, %" PRIu64
            " blocks lost, %" PRIu64 " flips): %" PRIu64 " recovered, %" PRIu64
            " not found, %" PRIu64 " corrupt\n",
            seed, s->steps, s->stores, s->held, s->syncs, s->reclaims, s->loads,
            s->cold, s->captures, s->crashes, s->torn, s->lost, s->flips,
            s->recovered, s->not_found, s->corrupt);
}

static void test_walk(void)
{
    static const uint64_t seeds[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
    struct walk_stats total;

    memset(&total, 0, sizeof(total));
    for (size_t i = 0; i < sizeof(seeds) / sizeof(seeds[0]); ++i) {
        walk_seeded(seeds[i], WALK_BYTES);
        total.cold += walk.stats.cold;
        total.held += walk.stats.held;
        total.recovered += walk.stats.recovered;
        total.torn += walk.stats.torn;
        total.lost += walk.stats.lost;
        total.captures += walk.stats.captures;
    }
    CHECK(total.cold > 0 && total.held > 0 && total.recovered > 0);
    CHECK(total.torn > 0 && total.lost > 0 && total.captures > 0);
}

/* Walks the recovery fuzzer broke: more SYNCs than the core's
 * `operations` bound before they complete, and more RECLAIMs than the
 * store's completion ring holds (the walk waits as the core would: the
 * fuzzer's first crash was the store refusing the seventeenth SYNC). */
static void test_walk_bounds(void)
{
    uint8_t bytes[1 + 2 * (3 + 40 + 70)];
    size_t n = 0;

    bytes[n++] = 0; /* FDATASYNC, the smallest configuration. */
    for (uint32_t i = 0; i < 3; ++i) {
        bytes[n++] = 2; /* Transaction 1 (IDENTITY), then APPENDs. */
        bytes[n++] = 0;
    }
    for (uint32_t i = 0; i < 40; ++i) {
        bytes[n++] = 10; /* A SYNC of the packed sequence, not waited. */
        bytes[n++] = 0;
    }
    for (uint32_t i = 0; i < 70; ++i) {
        bytes[n++] = 11; /* A RECLAIM of the packed sequence. */
        bytes[n++] = 0;
    }
    CHECK(n == sizeof(bytes));
    walk_run(bytes, n);
    CHECK(!walk.failed && walk.stats.syncs == 40 && walk.stats.reclaims >= 70);
    /* PUBLISH steps alone: each is a record, so the walk reclaims before
     * them as before any STORE (the fuzzer's second crash: the slots
     * filled under a RECLAIM that never moved and a STORE failed). */
    n = 1;
    for (uint32_t i = 0; i < 60; ++i) {
        bytes[n++] = 9;
        bytes[n++] = 0;
    }
    walk_run(bytes, n);
    CHECK(!walk.failed && walk.stats.captures >= 60);
}

/* Names the test that fails; VSR_STORE_TEST in the environment runs
 * only the test of that name. */
#define RUN(test)                                                              \
    do {                                                                       \
        const char *only = getenv("VSR_STORE_TEST");                           \
                                                                               \
        if (only == NULL || strcmp(only, #test) == 0) {                        \
            fprintf(stderr, "store: %s\n", #test);                             \
            test();                                                            \
        }                                                                      \
    } while (0)

/* The recovery fuzzer includes this file and names the entry point
 * itself; libFuzzer owns main there. */
#ifndef VSR_STORE_TESTS_MAIN
#define VSR_STORE_TESTS_MAIN main
#endif

/* With a seed on the command line, one walk of that seed (and an
 * optional byte count) instead of the tests. */
int VSR_STORE_TESTS_MAIN(int argc, char **argv)
{
    uint64_t header_bytes;
    uint64_t max_record;

    derive(&header_bytes, &max_record);
    printf("store: header %" PRIu64 " bytes, record limit %" PRIu64 "\n",
           header_bytes, max_record);
    if (argc > 1) {
        uint64_t seed = strtoull(argv[1], NULL, 0);
        size_t size = argc > 2 ? (size_t)strtoul(argv[2], NULL, 0) : WALK_BYTES;

        walk_seeded(seed, size);
        return 0;
    }
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
    RUN(test_recover_prefixes);
    RUN(test_recover_prefixes_dsync);
    RUN(test_recover_torn);
    RUN(test_recover_floor);
    RUN(test_recover_stale);
    RUN(test_recover_superblocks);
    RUN(test_recover_reread);
    RUN(test_recover_modes);
    RUN(test_recover_splice);
    RUN(test_wrap_seal);
    RUN(test_wrap_exact);
    RUN(test_freeing_flush);
    RUN(test_reclaim_media);
    RUN(test_client_freed);
    RUN(test_client_newer);
    RUN(test_client_base_moved);
    RUN(test_walk);
    RUN(test_walk_bounds);
    printf("store: ok\n");
    return 0;
}
