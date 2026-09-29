/* The GNU declarations (O_DIRECT, AT_FDCWD) precede every system header. */
#define _GNU_SOURCE
#include "config.h"

#include "io/store.h"

#include "checked.h"
#include "io/engine.h"

#include <errno.h>
#include <fcntl.h>
#include <stdalign.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <sys/stat.h>

/*
 * Store: the tail ring, the write pipeline and the file life cycle
 * (docs/io-implementation.md, section 6). This file is delivered in
 * phases; the parts of later phases are marked "Phase N" and left as stubs
 * that keep every earlier behaviour honest:
 *   1. layout and checks, NEW/JOIN creation, packing into the ring,
 *      extents and the wrap, the write pipeline, SYNC and flushes, the
 *      superblock writes (creation, growth, idle floor), status and close;
 *   2. the indexes (op ring, versions, client table), hot and cold LOADs,
 *      RECLAIM and freeing, admission, capture and base support;
 *   3. recovery of an existing log (decisions 48 and 50): the
 *      "Recovery" section.
 *
 * Bookkeeping region, in offset order (vsr_io_store_size): the segment
 * table [max_segments], the extent FIFO, the write ring [inflight_writes],
 * the op ring and the versions table [max_entries], the client table, the
 * held STORE, SYNC and LOAD queues [operations], the ring pins [regions],
 * the completion ring [3 * operations], the logical state copies and the
 * log path. The tail region is the ring [cache_bytes] followed by the two
 * superblock blocks.
 *
 * Ring invariants (section 6.1): head is the unwrapped offset of the next
 * packed byte and file_head its file offset; every extent maps a ring
 * range to a file range with block boundaries coinciding, so a write of
 * [begin, end) inside one extent goes from ring + begin % size to the
 * file at extent.file_offset + (begin - extent.ring_offset); issued <=
 * head, and every byte below issued belongs to a planned write or to a
 * ring gap skipped at a wrap; writes complete out of order but `written`
 * and written_ring advance over the contiguous prefix in issue order.
 */

#define NONE VSR_IO_INDEX_NONE
#define NO_SEQUENCE UINT64_C(0)
/* The log path buffer: PATH_MAX on every supported platform. */
#define STORE_PATH_BYTES 4096u
#define STORE_LOG_NAME "log"
#define STORE_FILE_MODE 0644u
#define STORE_SUPERBLOCKS 2u
/* flush_interval_ns when the option is zero (decision 50). */
#define STORE_FLUSH_INTERVAL_NS UINT64_C(100000000)
/* The largest single write the pipeline plans: below the kernel's per-call
 * cap, so a write is never short by design (decision 70). */
#define STORE_WRITE_MAX_BYTES (UINT64_C(1) << 30)
#define STORE_BLOCK_MIN 512u

/* Invariant checks in debug builds; a violation traps (see pool.c). */
#ifdef NDEBUG
#define STORE_ASSERT(condition) ((void)sizeof(condition))
#else
#define STORE_ASSERT(condition) ((condition) ? (void)0 : __builtin_trap())
#endif

enum write_state { WRITE_FREE, WRITE_PENDING, WRITE_INFLIGHT, WRITE_COMPLETE };

/* FILE slot sub-operations, in `file_op` while one is in flight. */
enum file_op {
    FILE_NONE,
    FILE_PROBE,    /* OPENAT of an existing log. */
    FILE_CREATE,   /* OPENAT with O_CREAT | O_EXCL. */
    FILE_ALLOCATE, /* Creation FALLOCATE, one slot at a time. */
    FILE_GROW,     /* Growth FALLOCATE of one more slot. */
    FILE_STAT      /* Recovery STATX of the log for its size. */
};

/* Recovery (section 6.4), in order; prepare_recovery advances through the
 * stages that need no I/O and issues one record for the others. */
enum recovery_stage {
    RECOVERY_NONE,
    RECOVERY_STAT,        /* STATX of the log: its size. */
    RECOVERY_SUPERBLOCKS, /* READ of both superblock blocks. */
    RECOVERY_HEADERS,     /* READ of the header of slot `slot`. */
    RECOVERY_SCAN,        /* READ of the chunk at `offset`, then replay. */
    RECOVERY_BASE,        /* The anchor's clients file (snapshot module). */
    RECOVERY_FLUSH,       /* FDATASYNC: the records read are on media. */
    RECOVERY_SUPERBLOCK,  /* The superblock with run + 1 and the floor. */
    RECOVERY_FLUSH_AGAIN, /* FDATASYNC: the new run is on media. */
    RECOVERY_FINISH       /* READY; the RECOVERY load completes. */
};

/* Scan modes: the chain is replayed until a range is judged bad twice;
 * the rest of that slot, and once the chain is over every slot it did
 * not visit, is swept for floors only: every CRC-valid record of the
 * generation, wherever it lies (a torn tail, a stale segment, a freed
 * slot), carries an acknowledged durable sequence (decision 50). */
enum recovery_mode {
    SCAN_CHAIN, /* Replaying; a judged range switches to SWEEP. */
    SCAN_SWEEP, /* Floors only, to the slot's end, then the successor. */
    SCAN_REST   /* Floors only over the unvisited slots, then the end. */
};

/* The `sub` of a LOAD slot taken by recovery rather than by a cold LOAD. */
#define RECOVERY_SUB 1u

enum growth_stage {
    GROWTH_NONE,
    GROWTH_ALLOCATING, /* FALLOCATE in flight. */
    GROWTH_SUPERBLOCK  /* The superblock naming the new slot in flight. */
};

enum flush_state {
    FLUSH_NONE,
    FLUSH_DUE,   /* Issue once written >= flush_target. */
    FLUSH_WANTED /* A SYNC asked; poll applies sync_delay_ns. */
};

enum freeing_flush {
    FREEING_FLUSH_NONE,
    FREEING_FLUSH_WANTED,  /* FLUSHING slots wait for a flush issued
                              after the superblock write completed. */
    FREEING_FLUSH_INFLIGHT /* One such flush is out. */
};

enum load_state {
    LOAD_QUEUED,  /* Records resolved; hot ones wait for a lease. */
    LOAD_READING, /* The cold read is in flight. */
    LOAD_READ     /* Read complete; waiting for a lease. */
};

/* The base file load a held RESTORE or PUBLISH waits for (decision 43). */
enum base_state {
    BASE_NONE,
    BASE_WANTED,  /* base_wanted reports it; the snapshot module starts. */
    BASE_LOADING, /* base_begin was called. */
    BASE_LOADED   /* base_end was called; base_resume packs or fails. */
};

enum base_kind {
    BASE_PUBLISH,
    BASE_RESTORE,
    BASE_RECOVERY /* The anchor's file after the scan (section 6.4). */
};

/* -------------------------------------------------------------------------
 * Layout
 * ---------------------------------------------------------------------- */

struct store_plan {
    uint64_t max_record_bytes;
    uint64_t header_bytes;
    uint32_t header_blocks;
    uint32_t extents_capacity;
    uint32_t clients_capacity;
    size_t state_bytes;
    size_t segments;
    size_t extents;
    size_t writes;
    size_t ops;
    size_t versions;
    size_t clients;
    size_t stores;
    size_t syncs;
    size_t loads;
    size_t load_refs;
    size_t pins;
    size_t completions;
    size_t state;
    size_t path;
    size_t total;
    size_t alignment;
    size_t tail_bytes;
    size_t tail_alignment;
};

static bool power_of_two(uint64_t value)
{
    return value != 0 && (value & (value - 1)) == 0;
}

static uint64_t round_up(uint64_t value, uint64_t multiple)
{
    return (value + multiple - 1) / multiple * multiple;
}

static bool place(size_t *offset, size_t bytes, size_t alignment, size_t *out)
{
    size_t aligned;

    if (!vsr_size_add(*offset, alignment - 1, &aligned)) {
        return false;
    }
    aligned &= ~(alignment - 1);
    *out = aligned;
    return vsr_size_add(aligned, bytes, offset);
}

static bool add64(uint64_t a, uint64_t b, uint64_t *out)
{
    if (a > UINT64_MAX - b) {
        return false;
    }
    *out = a + b;
    return true;
}

static bool to_size(uint64_t value, size_t *out)
{
    if ((uint64_t)(size_t)value != value) {
        return false;
    }
    *out = (size_t)value;
    return true;
}

/* The values every layout derives from the options and the limits: the
 * record limit and the header size. EINVAL for malformed geometry, ELIMIT
 * when the codec limits overflow. */
static int store_derive(const struct vsr_io_store_options *options,
                        const struct vsr_limits *limits,
                        struct store_plan *plan)
{
    size_t segment_limit;
    uint64_t header_bytes;

    memset(plan, 0, sizeof(*plan));
    if (!power_of_two(options->block_bytes) ||
        options->block_bytes < STORE_BLOCK_MIN || options->segment_bytes == 0 ||
        options->segment_bytes % options->block_bytes != 0 ||
        options->cache_bytes == 0 ||
        options->cache_bytes % options->block_bytes != 0) {
        return VSR_EINVAL;
    }
    if (vsr_io_codec_record_limit(limits, &plan->max_record_bytes) != VSR_OK ||
        vsr_io_codec_segment_limit(limits, &segment_limit) != VSR_OK) {
        return VSR_ELIMIT;
    }
    if (!add64(segment_limit, options->block_bytes - 1u, &header_bytes)) {
        return VSR_ELIMIT;
    }
    header_bytes -= header_bytes % options->block_bytes;
    if (header_bytes / options->block_bytes > UINT32_MAX) {
        return VSR_ELIMIT;
    }
    plan->header_bytes = header_bytes;
    plan->header_blocks = (uint32_t)(header_bytes / options->block_bytes);
    return VSR_OK;
}

/* Bytes of the logical state copies: the hard state's epoch with two
 * memberships, and the anchor with its own epoch and manifest span. */
/* Bytes of a decoded epoch with two memberships of `members` each. */
static bool epoch_bytes(const struct vsr_limits *limits, size_t *bytes)
{
    size_t members;

    return vsr_size_mul(limits->members, sizeof(struct vsr_member), &members) &&
           vsr_size_mul(members, 2, &members) &&
           vsr_size_add(members, sizeof(struct vsr_epoch), bytes) &&
           vsr_size_add(*bytes, 2 * sizeof(struct vsr_membership), bytes);
}

/* The logical state copies: the hard state's epoch, then the anchor with
 * its epoch, the decoder's manifest span, a second span and the manifest
 * bytes copied out of the record. */
static bool state_bytes(const struct vsr_limits *limits, size_t *bytes)
{
    size_t epoch;
    size_t manifest;
    size_t total;

    if (!epoch_bytes(limits, &epoch) || !vsr_size_mul(epoch, 2, &total) ||
        !vsr_size_add(total, sizeof(struct vsr_checkpoint), &total) ||
        !vsr_size_add(total, 2 * sizeof(struct vsr_span), &total) ||
        !to_size(limits->manifest_bytes, &manifest) ||
        !vsr_size_add(total, manifest, &total)) {
        return false;
    }
    *bytes = total;
    return true;
}

static int store_plan(const struct vsr_io_store_options *options,
                      const struct vsr_limits *limits, uint32_t regions,
                      struct store_plan *plan)
{
    size_t offset = 0;
    size_t bytes;
    size_t queue;
    size_t completions;
    uint64_t extents;
    uint64_t clients;
    uint64_t tail;
    int rc;

    rc = store_derive(options, limits, plan);
    if (rc != VSR_OK) {
        return rc;
    }
    /* Extents: one per segment start in the ring (a segment is sealed only
     * once fewer than a record's bytes remain) plus the wrap and the
     * partial first one. */
    if (options->segment_bytes <= plan->max_record_bytes) {
        return VSR_ELIMIT;
    }
    extents = options->cache_bytes /
                  (options->segment_bytes - plan->max_record_bytes) +
              4;
    clients = 1;
    while (clients < 2 * (uint64_t)options->max_clients) {
        clients *= 2;
    }
    if (extents > UINT32_MAX || clients > UINT32_MAX ||
        !state_bytes(limits, &plan->state_bytes) ||
        !add64(options->cache_bytes,
               STORE_SUPERBLOCKS * (uint64_t)options->block_bytes, &tail) ||
        !to_size(tail, &plan->tail_bytes)) {
        return VSR_ELIMIT;
    }
    plan->extents_capacity = (uint32_t)extents;
    plan->clients_capacity = (uint32_t)clients;
    plan->tail_alignment =
        options->block_bytes > 4096u ? options->block_bytes : 4096u;
    plan->alignment = alignof(struct vsr_io_segment);
    if (!vsr_size_mul(options->max_segments, sizeof(struct vsr_io_segment),
                      &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_segment),
               &plan->segments) ||
        !vsr_size_mul(plan->extents_capacity, sizeof(struct vsr_io_extent),
                      &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_extent), &plan->extents) ||
        !vsr_size_mul(options->inflight_writes, sizeof(struct vsr_io_write),
                      &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_write), &plan->writes) ||
        !vsr_size_mul(options->max_entries, sizeof(struct vsr_io_op_ref),
                      &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_op_ref), &plan->ops) ||
        !vsr_size_mul(options->max_entries, sizeof(struct vsr_io_version),
                      &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_version),
               &plan->versions) ||
        !vsr_size_mul(plan->clients_capacity, sizeof(struct vsr_io_client),
                      &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_client), &plan->clients) ||
        !vsr_size_mul(limits->operations, sizeof(struct vsr_io_pending_store),
                      &queue) ||
        !place(&offset, queue, alignof(struct vsr_io_pending_store),
               &plan->stores) ||
        !vsr_size_mul(limits->operations, sizeof(struct vsr_io_pending_sync),
                      &queue) ||
        !place(&offset, queue, alignof(struct vsr_io_pending_sync),
               &plan->syncs) ||
        !vsr_size_mul(limits->operations, sizeof(struct vsr_io_pending_load),
                      &queue) ||
        !place(&offset, queue, alignof(struct vsr_io_pending_load),
               &plan->loads) ||
        !vsr_size_mul(limits->operations, limits->batch_entries, &queue) ||
        !vsr_size_mul(queue, sizeof(struct vsr_io_load_ref), &queue) ||
        !place(&offset, queue, alignof(struct vsr_io_load_ref),
               &plan->load_refs) ||
        !vsr_size_mul(regions, sizeof(struct vsr_io_ring_pin), &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_ring_pin), &plan->pins) ||
        !vsr_size_mul(limits->operations, 3, &completions) ||
        !vsr_size_mul(completions, sizeof(struct vsr_io_completion), &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_completion),
               &plan->completions) ||
        !place(&offset, plan->state_bytes, alignof(struct vsr_checkpoint),
               &plan->state) ||
        !place(&offset, STORE_PATH_BYTES, 1, &plan->path)) {
        return VSR_ELIMIT;
    }
    plan->total = offset;
    return VSR_OK;
}

int vsr_io_store_check(const struct vsr_io_store_options *options,
                       const struct vsr_limits *limits, uint32_t slab_bytes)
{
    struct store_plan plan;
    uint64_t minimum;
    uint64_t largest_write;
    int rc;

    if (options == NULL || limits == NULL) {
        return VSR_EINVAL;
    }
    if (options->segments == 0 || options->max_segments == 0 ||
        options->max_entries == 0 || options->max_clients == 0 ||
        options->inflight_writes == 0 || options->direct_io > 1 ||
        options->sync_mode > VSR_IO_SYNC_FDATASYNC ||
        options->on_write_error > VSR_IO_WRITE_ERROR_CONTINUE ||
        options->sync_delay_ns >= VSR_NO_DEADLINE ||
        options->flush_interval_ns >= VSR_NO_DEADLINE) {
        return VSR_EINVAL;
    }
    for (size_t i = 0; i < sizeof(options->reserved); ++i) {
        if (options->reserved[i] != 0) {
            return VSR_EINVAL;
        }
    }
    rc = store_derive(options, limits, &plan);
    if (rc != VSR_OK) {
        return rc;
    }
    if (options->segments < 2 || options->max_segments < options->segments ||
        options->max_entries < limits->batch_entries) {
        return VSR_ELIMIT;
    }
    /* A record fits one slab with two blocks of alignment and one
     * segment after its header (decision 37). */
    if ((uint64_t)slab_bytes < 2 * (uint64_t)options->block_bytes ||
        plan.max_record_bytes >
            (uint64_t)slab_bytes - 2 * (uint64_t)options->block_bytes ||
        options->segment_bytes < plan.header_bytes ||
        plan.max_record_bytes > options->segment_bytes - plan.header_bytes) {
        return VSR_ELIMIT;
    }
    /* Executor lengths are 32-bit: the creation FALLOCATE covers the
     * superblocks and one slot; the largest record write covers the
     * write-behind allowance, one record and its padding (decision 70). */
    if (!add64(options->segment_bytes, 2 * (uint64_t)options->block_bytes,
               &minimum) ||
        minimum > UINT32_MAX ||
        !add64(options->write_behind_bytes, plan.max_record_bytes,
               &largest_write) ||
        !add64(largest_write, options->block_bytes, &largest_write) ||
        largest_write > STORE_WRITE_MAX_BYTES) {
        return VSR_ELIMIT;
    }
    /* The ring holds the write-behind allowance, the pinned payload, two
     * records and the seal's header with its alignment (decision 69). */
    if (!add64(options->write_behind_bytes, limits->pinned_payload_bytes,
               &minimum) ||
        !add64(minimum, 2 * plan.max_record_bytes, &minimum) ||
        !add64(minimum, 2 * plan.header_bytes, &minimum) ||
        !add64(minimum, options->block_bytes, &minimum) ||
        options->cache_bytes < minimum) {
        return VSR_ELIMIT;
    }
    return VSR_OK;
}

int vsr_io_store_size(const struct vsr_io_store_options *options,
                      const struct vsr_limits *limits, uint32_t regions,
                      size_t *metadata_bytes, size_t *metadata_alignment,
                      size_t *tail_bytes, size_t *tail_alignment)
{
    struct store_plan plan;
    int rc;

    if (options == NULL || limits == NULL || metadata_bytes == NULL ||
        metadata_alignment == NULL || tail_bytes == NULL ||
        tail_alignment == NULL) {
        return VSR_EINVAL;
    }
    rc = store_plan(options, limits, regions, &plan);
    if (rc != VSR_OK) {
        return rc;
    }
    *metadata_bytes = plan.total;
    *metadata_alignment = plan.alignment;
    *tail_bytes = plan.tail_bytes;
    *tail_alignment = plan.tail_alignment;
    return VSR_OK;
}

/* Builds "<path>/log" into the path buffer; NULL when it does not fit. */
static char *log_path(char *buffer, const char *path)
{
    size_t length = path != NULL ? strlen(path) : 0;
    size_t at = 0;

    if (length + 1 + sizeof(STORE_LOG_NAME) > STORE_PATH_BYTES) {
        return NULL;
    }
    if (length > 0) {
        memcpy(buffer, path, length + 1);
        at = length;
        if (buffer[at - 1] != '/') {
            buffer[at++] = '/';
        }
    }
    memcpy(buffer + at, STORE_LOG_NAME, sizeof(STORE_LOG_NAME));
    return buffer;
}

void vsr_io_store_init(struct vsr_io_store *store, void *metadata,
                       size_t metadata_size, void *tail, size_t tail_size,
                       const struct vsr_io_store_options *options,
                       const struct vsr_limits *limits, uint32_t regions,
                       uint32_t region_index, const char *path, int dir_fd)
{
    struct store_plan plan;
    unsigned char *base = metadata;
    int rc = store_plan(options, limits, regions, &plan);

    STORE_ASSERT(rc == VSR_OK && plan.total <= metadata_size &&
                 plan.tail_bytes <= tail_size);
    (void)rc;
    (void)metadata_size;
    (void)tail_size;
    memset(store, 0, sizeof(*store));
    memset(base, 0, plan.total);
    store->options = *options;
    store->limits = *limits;
    store->state = VSR_IO_STORE_CLOSED;
    store->log_slot = -1;
    store->dir_fd = dir_fd;
    store->path = path;
    store->header_blocks = plan.header_blocks;
    store->header_bytes = plan.header_bytes;
    store->max_record_bytes = plan.max_record_bytes;
    store->data_bytes = options->segment_bytes - plan.header_bytes;
    store->state_region = base + plan.state;
    store->capture_floor = UINT64_MAX;
    store->segments = (struct vsr_io_segment *)(void *)(base + plan.segments);
    store->current = NONE;
    store->next_segment = 1;
    store->ring = tail;
    store->ring_size = options->cache_bytes;
    store->superblocks = store->ring + options->cache_bytes;
    store->region_index = region_index;
    store->extents = (struct vsr_io_extent *)(void *)(base + plan.extents);
    store->extents_capacity = plan.extents_capacity;
    store->writes = (struct vsr_io_write *)(void *)(base + plan.writes);
    for (uint32_t i = 0; i < options->inflight_writes; ++i) {
        store->writes[i].slot = NONE;
        store->writes[i].state = WRITE_FREE;
    }
    store->flush_slot = NONE;
    store->flush_deadline = VSR_NO_DEADLINE;
    store->idle_deadline = VSR_NO_DEADLINE;
    store->ops = (struct vsr_io_op_ref *)(void *)(base + plan.ops);
    store->versions = (struct vsr_io_version *)(void *)(base + plan.versions);
    store->clients_capacity = plan.clients_capacity;
    store->clients = (struct vsr_io_client *)(void *)(base + plan.clients);
    store->stores = (struct vsr_io_pending_store *)(void *)(base + plan.stores);
    store->syncs = (struct vsr_io_pending_sync *)(void *)(base + plan.syncs);
    store->loads = (struct vsr_io_pending_load *)(void *)(base + plan.loads);
    store->load_refs =
        (struct vsr_io_load_ref *)(void *)(base + plan.load_refs);
    store->base_slot = -1;
    store->pins = (struct vsr_io_ring_pin *)(void *)(base + plan.pins);
    store->pins_count = regions;
    for (uint32_t i = 0; i < regions; ++i) {
        store->pins[i].begin = UINT64_MAX;
        store->pins[i].end = UINT64_MAX;
    }
    store->completions =
        (struct vsr_io_completion *)(void *)(base + plan.completions);
    store->file_slot = NONE;
    store->file_op = FILE_NONE;
    store->recovery.slab = NONE;
    store->recovery.io_slot = NONE;
    store->log_path = log_path((char *)(base + plan.path), path);
    for (uint32_t i = 0; i < options->max_segments; ++i) {
        store->segments[i].header_write = NONE;
    }
}

/* -------------------------------------------------------------------------
 * Completions and queues
 * ---------------------------------------------------------------------- */

static uint32_t ring_slot(uint32_t head, uint32_t count, uint32_t capacity)
{
    return (uint32_t)(((uint64_t)head + count) % capacity);
}

static uint32_t completions_capacity(const struct vsr_io_store *store)
{
    return 3 * store->limits.operations;
}

/* Queues a completion for the engine; the ring holds three per outstanding
 * op, so it never fills. */
static void complete(struct vsr_io_store *store, uint64_t op, int32_t status,
                     uint32_t lease, const void *data)
{
    uint32_t capacity = completions_capacity(store);
    struct vsr_io_completion *out;

    STORE_ASSERT(store->completions_count < capacity);
    out = &store->completions[ring_slot(store->completions_head,
                                        store->completions_count, capacity)];
    store->completions_count++;
    out->op = op;
    out->status = status;
    out->lease = lease;
    out->data = data;
}

bool vsr_io_store_next_completion(struct vsr_io_store *store,
                                  struct vsr_io_completion *out)
{
    if (store == NULL || out == NULL || store->completions_count == 0) {
        return false;
    }
    *out = store->completions[store->completions_head];
    store->completions_head =
        ring_slot(store->completions_head, 1, completions_capacity(store));
    store->completions_count--;
    return true;
}

/* The replica embedding the store: LOAD results need its lease table and
 * its engine's pool, and the store is only ever a member of a replica
 * (engine attach, and the unit test's replica 0). */
static struct vsr_io_replica *store_replica(struct vsr_io_store *store)
{
    return (struct vsr_io_replica *)(void *)((unsigned char *)store -
                                             offsetof(struct vsr_io_replica,
                                                      store));
}

static struct vsr_io_pending_store *stores_at(struct vsr_io_store *store,
                                              uint32_t i)
{
    return &store->stores[ring_slot(store->stores_head, i,
                                    store->limits.operations)];
}

static struct vsr_io_pending_sync *syncs_at(struct vsr_io_store *store,
                                            uint32_t i)
{
    return &store->syncs[ring_slot(store->syncs_head, i,
                                   store->limits.operations)];
}

static void stores_pop(struct vsr_io_store *store)
{
    store->stores_head =
        ring_slot(store->stores_head, 1, store->limits.operations);
    store->stores_count--;
}

static void syncs_pop(struct vsr_io_store *store)
{
    store->syncs_head =
        ring_slot(store->syncs_head, 1, store->limits.operations);
    store->syncs_count--;
}

static struct vsr_io_pending_load *loads_at(struct vsr_io_store *store,
                                            uint32_t i)
{
    return &store->loads[ring_slot(store->loads_head, i,
                                   store->limits.operations)];
}

/* The record references of a queued load: batch_entries per queue slot. */
static struct vsr_io_load_ref *load_refs(struct vsr_io_store *store,
                                         const struct vsr_io_pending_load *load)
{
    size_t slot = (size_t)(load - store->loads);

    return &store->load_refs[slot * store->limits.batch_entries];
}

/* Drops the head load, releasing the slab of a cold read that is not
 * handed to a lease. */
static void loads_pop(struct vsr_io_store *store, bool release_slab)
{
    struct vsr_io_pending_load *load = loads_at(store, 0);

    if (release_slab && load->slab != NONE) {
        vsr_io_pool_release(&store_replica(store)->io->pool, load->slab);
    }
    load->slab = NONE;
    store->loads_head =
        ring_slot(store->loads_head, 1, store->limits.operations);
    store->loads_count--;
}

/* The sequence the next STORE must carry: after the newest held one. */
static uint64_t next_sequence(struct vsr_io_store *store)
{
    if (store->stores_count > 0) {
        return stores_at(store, store->stores_count - 1)->sequence + 1;
    }
    return store->readable + 1;
}

/* Fences the store: every queued op completes with FAILED and the
 * RECOVERY load, when still open, with `status`. */
static void store_fail(struct vsr_io_store *store, int32_t status)
{
    if (store->state == VSR_IO_STORE_FAILED) {
        return;
    }
    if (store->recovery.load_op != 0) {
        complete(store, store->recovery.load_op, status, NONE, NULL);
        store->recovery.load_op = 0;
    }
    store->state = VSR_IO_STORE_FAILED;
    while (store->stores_count > 0) {
        complete(store, stores_at(store, 0)->op, VSR_IO_FAILED, NONE, NULL);
        stores_pop(store);
    }
    while (store->syncs_count > 0) {
        complete(store, syncs_at(store, 0)->op, VSR_IO_FAILED, NONE, NULL);
        syncs_pop(store);
    }
    /* Queued LOADs too; a read in flight keeps its slab (cold_slab) until
     * its completion, which load_done then releases. */
    while (store->loads_count > 0) {
        struct vsr_io_pending_load *load = loads_at(store, 0);

        complete(store, load->op, VSR_IO_FAILED, NONE, NULL);
        loads_pop(store, load->state != LOAD_READING);
    }
    store->base_state = BASE_NONE;
    store->flush_pending = FLUSH_NONE;
    store->flush_deadline = VSR_NO_DEADLINE;
    store->idle_deadline = VSR_NO_DEADLINE;
    store->superblock_dirty = 0;
}

/* -------------------------------------------------------------------------
 * Ring and extents
 * ---------------------------------------------------------------------- */

static uint64_t block_bytes(const struct vsr_io_store *store)
{
    return store->options.block_bytes;
}

static uint64_t slot_offset(const struct vsr_io_store *store, uint32_t slot)
{
    return STORE_SUPERBLOCKS * block_bytes(store) +
           (uint64_t)slot * store->options.segment_bytes;
}

static struct vsr_io_extent *extent_at(const struct vsr_io_store *store,
                                       uint32_t i)
{
    return &store->extents[ring_slot(store->extents_head, i,
                                     store->extents_capacity)];
}

static struct vsr_io_extent *extent_newest(const struct vsr_io_store *store)
{
    if (store->extents_count == 0) {
        return NULL;
    }
    return extent_at(store, store->extents_count - 1);
}

static void extent_begin(struct vsr_io_store *store, uint32_t segment,
                         uint32_t header)
{
    struct vsr_io_extent *extent;

    STORE_ASSERT(store->extents_count < store->extents_capacity);
    extent = extent_at(store, store->extents_count);
    store->extents_count++;
    extent->file_offset = store->file_head;
    extent->ring_offset = store->head;
    extent->length = 0;
    extent->last_sequence = NO_SEQUENCE;
    extent->segment = segment;
    extent->header = header;
}

/* After a write error under CONTINUE the store serves from memory: no
 * write, flush, growth or superblock is issued and nothing waits for the
 * disk. */
static bool memory_only(const struct vsr_io_store *store)
{
    return store->error != 0 &&
           store->options.on_write_error == VSR_IO_WRITE_ERROR_CONTINUE;
}

/* The ring offset below which every write completed, in issue order. */
static uint64_t written_floor(const struct vsr_io_store *store)
{
    if (memory_only(store)) {
        return store->head;
    }
    if (store->writes_count > 0) {
        return store->writes[store->writes_head].ring_begin;
    }
    return store->issued;
}

/* The lowest ring offset a LOAD lease or an unfinished write still needs. */
static uint64_t pin_floor(const struct vsr_io_store *store)
{
    uint64_t floor = written_floor(store);

    for (uint32_t i = 0; i < store->pins_count; ++i) {
        if (store->pins[i].begin < floor) {
            floor = store->pins[i].begin;
        }
    }
    return floor;
}

/* Makes room for n bytes at head: the extents whose ring bytes are about
 * to be overwritten are dropped or trimmed. The hold rules keep pinned and
 * unwritten bytes above the new floor. */
static void ring_reserve(struct vsr_io_store *store, uint64_t n)
{
    uint64_t end = store->head + n;
    uint64_t floor = end > store->ring_size ? end - store->ring_size : 0;

    STORE_ASSERT(n <= store->ring_size && pin_floor(store) >= floor);
    /* The newest extent is the one being filled: it stays, trimmed. */
    while (store->extents_count > 1 &&
           extent_at(store, 0)->ring_offset + extent_at(store, 0)->length <=
               floor) {
        store->extents_head =
            ring_slot(store->extents_head, 1, store->extents_capacity);
        store->extents_count--;
    }
    if (store->extents_count > 0 && extent_at(store, 0)->ring_offset < floor) {
        struct vsr_io_extent *extent = extent_at(store, 0);
        uint64_t delta = floor - extent->ring_offset;

        STORE_ASSERT(delta <= extent->length);
        extent->ring_offset += delta;
        extent->file_offset += delta;
        extent->length -= delta;
    }
    if (floor > store->retained_floor) {
        store->retained_floor = floor;
    }
}

/* Appends n bytes at head to the newest extent (which must exist) and
 * returns their ring address; the caller fills them. */
static unsigned char *ring_append(struct vsr_io_store *store, uint64_t n)
{
    struct vsr_io_extent *extent = extent_newest(store);
    unsigned char *at;

    STORE_ASSERT(extent != NULL &&
                 store->head % store->ring_size + n <= store->ring_size);
    ring_reserve(store, n);
    at = store->ring + store->head % store->ring_size;
    extent->length += n;
    store->head += n;
    store->file_head += n;
    if (store->current != NONE) {
        store->segments[store->current].used += n;
    }
    return at;
}

/* Pads the packed bytes to the block boundary with a PAD marker, so the
 * next write starts at the next block and no block is rewritten. */
static void ring_pad(struct vsr_io_store *store)
{
    uint64_t remainder = store->head % block_bytes(store);
    uint64_t pad;

    if (remainder == 0) {
        return;
    }
    pad = block_bytes(store) - remainder;
    STORE_ASSERT(pad >= sizeof(struct vsr_io_wire_pad) &&
                 pad % VSR_IO_WIRE_ALIGN == 0);
    vsr_io_codec_put_pad(ring_append(store, pad), (uint32_t)pad);
}

/* Moves head to the next ring start when n bytes do not fit before the
 * wrap; the current write is closed first and the skipped bytes are not
 * mirrored (the file continues at the padded position). */
static void ring_wrap(struct vsr_io_store *store, uint64_t n)
{
    uint64_t next;

    if (store->head % store->ring_size + n <= store->ring_size) {
        return;
    }
    ring_pad(store);
    next = round_up(store->head, store->ring_size);
    if (store->issued == store->head) {
        store->issued = next; /* Nothing left to issue before the gap. */
    }
    store->head = next;
    if (store->current != NONE) {
        extent_begin(store, store->current, 0);
    }
}

/* Simulates the packing of n record bytes (with a seal first when `seal`)
 * and returns the unwrapped end of the block they are padded to, for the
 * hold rules: nothing packed or padded before the next hold check reaches
 * past it. */
static uint64_t pack_end(const struct vsr_io_store *store, bool seal,
                         uint64_t n)
{
    uint64_t size = store->ring_size;
    uint64_t pos = store->head;

    if (seal) {
        pos = round_up(pos, block_bytes(store));
        if (pos % size + store->header_bytes > size) {
            pos = round_up(pos, size);
        }
        pos += store->header_bytes;
    }
    if (pos % size + n > size) {
        pos = round_up(pos, size);
    }
    return round_up(pos + n, block_bytes(store));
}

/* The unwrapped ring offset of the file range [offset, offset + length)
 * when one live extent mirrors it. An extent of a freed slot (segment
 * NONE) is dead: the file bytes it names may be rewritten. */
static bool extent_locate(const struct vsr_io_store *store, uint64_t offset,
                          uint64_t length, uint64_t *ring)
{
    for (uint32_t i = 0; i < store->extents_count; ++i) {
        const struct vsr_io_extent *extent = extent_at(store, i);

        if (extent->segment != NONE && offset >= extent->file_offset &&
            offset - extent->file_offset <= extent->length &&
            length <= extent->length - (offset - extent->file_offset)) {
            *ring = extent->ring_offset + (offset - extent->file_offset);
            return true;
        }
    }
    return false;
}

bool vsr_io_store_hot(const struct vsr_io_store *store, uint64_t offset,
                      uint32_t length, struct vsr_io_piece *piece)
{
    uint64_t ring;

    if (store == NULL || piece == NULL ||
        !extent_locate(store, offset, length, &ring)) {
        return false;
    }
    piece->base = store->ring + ring % store->ring_size;
    piece->length = length;
    return true;
}

/* Pins the unwrapped ring range [begin, end) under lease. */
static void pin_range(struct vsr_io_store *store, uint32_t lease,
                      uint64_t begin, uint64_t end)
{
    struct vsr_io_ring_pin *pin = &store->pins[lease];

    STORE_ASSERT(pin->begin == UINT64_MAX && begin <= end);
    pin->begin = begin;
    pin->end = end;
}

uint32_t vsr_io_store_pin(struct vsr_io_store *store, uint64_t offset,
                          uint32_t length, uint32_t lease)
{
    uint64_t ring;

    if (store == NULL || lease >= store->pins_count ||
        !extent_locate(store, offset, length, &ring)) {
        return NONE;
    }
    pin_range(store, lease, ring, ring + length);
    return lease;
}

void vsr_io_store_unpin(struct vsr_io_store *store, uint32_t pin)
{
    if (store == NULL || pin >= store->pins_count) {
        return;
    }
    store->pins[pin].begin = UINT64_MAX;
    store->pins[pin].end = UINT64_MAX;
}

void vsr_io_store_release(struct vsr_io_store *store, uint32_t lease)
{
    /* A cold LOAD's slab reference belongs to the lease: the engine drops
     * it with the region. */
    vsr_io_store_unpin(store, lease);
}

/* -------------------------------------------------------------------------
 * Segments
 * ---------------------------------------------------------------------- */

static uint32_t segment_free_slot(const struct vsr_io_store *store)
{
    for (uint32_t i = 0; i < store->slots; ++i) {
        if (store->segments[i].number == 0 &&
            store->segments[i].phase == VSR_IO_SEGMENT_FREE) {
            return i;
        }
    }
    return NONE;
}

static void segment_seal(struct vsr_io_store *store)
{
    struct vsr_io_segment *segment;

    if (store->current == NONE) {
        return;
    }
    segment = &store->segments[store->current];
    ring_pad(store);
    segment->phase = VSR_IO_SEGMENT_SEALED;
    segment->last_sequence = store->readable;
    store->current = NONE;
}

/* Opens `slot` as the next segment: its header, carrying the logical
 * state as of now, is packed at the next block boundary of the ring
 * (after a wrap when it does not fit) as a new extent, and the slot is
 * HEADER until the write of that header completes. */
static void segment_open(struct vsr_io_store *store, uint32_t slot)
{
    struct vsr_io_segment *segment = &store->segments[slot];
    struct vsr_io_wire_segment fixed;
    struct vsr_io_segment_state state;
    unsigned char *at;
    size_t written = 0;
    bool logical;
    int rc;

    STORE_ASSERT(store->current == NONE && segment->number == 0);
    ring_pad(store);
    ring_wrap(store, store->header_bytes);
    memset(segment, 0, sizeof(*segment));
    segment->number = store->next_segment++;
    segment->first_sequence = store->readable + 1;
    segment->last_sequence = store->readable;
    segment->phase = VSR_IO_SEGMENT_HEADER;
    segment->header_write = NONE;
    store->current = slot;
    store->file_head = slot_offset(store, slot);
    extent_begin(store, slot, 1);
    memset(&fixed, 0, sizeof(fixed));
    fixed.generation = store->generation;
    fixed.segment = segment->number;
    fixed.run = store->run;
    /* The logical state once transaction 1 set it: the identity and the
     * hard state come together (the hard state's epoch is what the header
     * encodes), the anchor once one was published. */
    logical = store->identity_set && store->hard.epoch != NULL;
    state.identity = logical ? &store->identity : NULL;
    state.hard = logical ? &store->hard : NULL;
    state.checkpoint =
        logical && (store->anchor.id.hi != 0 || store->anchor.id.lo != 0)
            ? &store->anchor
            : NULL;
    state.log_begin = store->log_begin;
    state.log_end = store->log_end;
    state.client_base = store->client_base;
    state.last_sequence = store->readable;
    state.durable_floor = store->durable;
    at = ring_append(store, store->header_bytes);
    rc = vsr_io_codec_put_segment(&fixed, &state, at,
                                  (size_t)store->header_bytes, &written);
    STORE_ASSERT(rc == VSR_OK);
    (void)rc;
    (void)written;
    if (store->start_segment == 0) {
        store->start_segment = segment->number;
        store->start_slot = slot;
    }
}

/* -------------------------------------------------------------------------
 * Superblock
 * ---------------------------------------------------------------------- */

static void superblock_fill(const struct vsr_io_store *store,
                            struct vsr_io_wire_superblock *out,
                            uint64_t revision, uint32_t slots, uint64_t floor)
{
    memset(out, 0, sizeof(*out));
    out->generation = store->generation;
    out->revision = revision;
    out->cluster_hi = store->identity.cluster.hi;
    out->cluster_lo = store->identity.cluster.lo;
    out->replica = store->identity.replica;
    out->durability = store->identity.durability;
    out->block_bytes = store->options.block_bytes;
    out->segment_bytes = store->options.segment_bytes;
    out->header_blocks = store->header_blocks;
    out->slots = slots;
    out->start_segment = store->start_segment;
    out->start_slot = store->start_slot;
    out->run = store->run;
    out->durable_floor = floor;
}

/* Slots the file holds, from what was allocated in it. */
static uint32_t file_slots(const struct vsr_io_store *store)
{
    uint64_t fixed = STORE_SUPERBLOCKS * block_bytes(store);

    if (store->file_size < fixed) {
        return 0;
    }
    return (uint32_t)((store->file_size - fixed) /
                      store->options.segment_bytes);
}

/* -------------------------------------------------------------------------
 * Client table
 *
 * Open addressing with linear probing over clients_capacity buckets (a
 * power of two of at least 2 * max_clients); a deletion shifts the probe
 * chain back, so no tombstone ever accumulates. Every live entry counts
 * against max_clients, in-flight ones included (decision 44).
 * ---------------------------------------------------------------------- */

static bool id_zero(struct vsr_id id)
{
    return id.hi == 0 && id.lo == 0;
}

static bool id_equal(struct vsr_id a, struct vsr_id b)
{
    return a.hi == b.hi && a.lo == b.lo;
}

static uint32_t client_home(const struct vsr_io_store *store, struct vsr_id id)
{
    uint64_t h = id.hi * UINT64_C(0x9E3779B97F4A7C15) + id.lo;

    h ^= h >> 29;
    h *= UINT64_C(0xBF58476D1CE4E5B9);
    h ^= h >> 32;
    return (uint32_t)(h & (store->clients_capacity - 1));
}

static struct vsr_io_client *client_find(const struct vsr_io_store *store,
                                         struct vsr_id id)
{
    uint32_t mask = store->clients_capacity - 1;
    uint32_t at = client_home(store, id);

    if (id_zero(id)) {
        return NULL;
    }
    for (uint32_t n = 0; n < store->clients_capacity; ++n) {
        struct vsr_io_client *entry = &store->clients[(at + n) & mask];

        if (id_zero(entry->id)) {
            return NULL;
        }
        if (id_equal(entry->id, id)) {
            return entry;
        }
    }
    return NULL;
}

/* Inserts an empty entry for an id the table does not hold; NULL once
 * max_clients entries are live. */
static struct vsr_io_client *client_insert(struct vsr_io_store *store,
                                           struct vsr_id id)
{
    uint32_t mask = store->clients_capacity - 1;
    uint32_t at = client_home(store, id);
    struct vsr_io_client *entry;

    if (store->clients_count >= store->options.max_clients) {
        return NULL;
    }
    while (!id_zero(store->clients[at].id)) {
        at = (at + 1) & mask;
    }
    entry = &store->clients[at];
    memset(entry, 0, sizeof(*entry));
    entry->id = id;
    entry->base_offset = UINT64_MAX;
    entry->capture_offset = UINT64_MAX;
    entry->next_offset = UINT64_MAX;
    store->clients_count++;
    return entry;
}

/* The entry of id, created when absent; NULL when the table is full. */
static struct vsr_io_client *client_get(struct vsr_io_store *store,
                                        struct vsr_id id)
{
    struct vsr_io_client *entry = client_find(store, id);

    return entry != NULL ? entry : client_insert(store, id);
}

static void client_delete(struct vsr_io_store *store,
                          struct vsr_io_client *entry)
{
    uint32_t mask = store->clients_capacity - 1;
    uint32_t hole = (uint32_t)(entry - store->clients);
    uint32_t at = hole;

    for (;;) {
        struct vsr_io_client *candidate;
        uint32_t home;

        at = (at + 1) & mask;
        candidate = &store->clients[at];
        if (id_zero(candidate->id)) {
            break;
        }
        /* Movable when the hole lies on its probe path from home. */
        home = client_home(store, candidate->id);
        if (((at - home) & mask) >= ((at - hole) & mask)) {
            store->clients[hole] = *candidate;
            hole = at;
        }
    }
    memset(&store->clients[hole], 0, sizeof(store->clients[hole]));
    store->clients_count--;
}

/* Deletes an entry holding nothing: no record, no retained entry, not in
 * flight. */
static void client_prune(struct vsr_io_store *store,
                         struct vsr_io_client *entry)
{
    if (entry->current.number == 0 && entry->retained == 0 &&
        entry->inflight == 0) {
        client_delete(store, entry);
    }
}

/* Deletes every entry `keep` rejects. A deletion may move a later entry
 * into the bucket examined, so the bucket is examined again. */
static void clients_sweep(struct vsr_io_store *store,
                          bool (*keep)(const struct vsr_io_client *))
{
    for (uint32_t i = 0; i < store->clients_capacity;) {
        struct vsr_io_client *entry = &store->clients[i];

        if (!id_zero(entry->id) && !keep(entry)) {
            client_delete(store, entry);
        } else {
            i++;
        }
    }
}

static bool keep_nonempty(const struct vsr_io_client *entry)
{
    return entry->current.number != 0 || entry->retained != 0 ||
           entry->inflight != 0;
}

static bool keep_in_base(const struct vsr_io_client *entry)
{
    return entry->next_offset != UINT64_MAX;
}

static bool keep_none(const struct vsr_io_client *entry)
{
    (void)entry;
    return false;
}

/* -------------------------------------------------------------------------
 * Op ring, versions and the trim history (section 6.3)
 * ---------------------------------------------------------------------- */

static struct vsr_io_op_ref *op_slot(const struct vsr_io_store *store,
                                     uint64_t op)
{
    return &store->ops[op % store->options.max_entries];
}

/* The current version of op, or NULL. */
static struct vsr_io_op_ref *op_current(const struct vsr_io_store *store,
                                        uint64_t op)
{
    struct vsr_io_op_ref *ref = op_slot(store, op);

    return op != 0 && ref->op == op ? ref : NULL;
}

/* Moves a version truncated at `truncated` to the versions table. */
static bool version_add(struct vsr_io_store *store,
                        const struct vsr_io_op_ref *ref, uint64_t truncated)
{
    struct vsr_io_version *version;

    if (store->versions_count == store->options.max_entries) {
        return false;
    }
    version = &store->versions[store->versions_count++];
    memset(version, 0, sizeof(*version));
    version->op = ref->op;
    version->appended = ref->sequence;
    version->truncated = truncated;
    version->offset = ref->offset;
    version->length = ref->length;
    version->change = ref->change;
    version->index = ref->index;
    version->client = ref->client;
    return true;
}

/* The version of op valid at `sequence`, if a truncated one is. */
static const struct vsr_io_version *version_at(const struct vsr_io_store *store,
                                               uint64_t op, uint64_t sequence)
{
    for (uint32_t i = 0; i < store->versions_count; ++i) {
        const struct vsr_io_version *version = &store->versions[i];

        if (version->op == op && version->appended <= sequence &&
            sequence < version->truncated) {
            return version;
        }
    }
    return NULL;
}

static void versions_reclaim(struct vsr_io_store *store, uint64_t oldest)
{
    for (uint32_t i = 0; i < store->versions_count;) {
        if (store->versions[i].truncated <= oldest) {
            store->versions[i] = store->versions[--store->versions_count];
        } else {
            i++;
        }
    }
}

/* Records that the transaction at `sequence` moved log_begin to `begin`.
 * A full history merges the newest event into the new one: the revisions
 * between the two then report the older begin, which retains more. */
static void trims_record(struct vsr_io_store *store, uint64_t sequence,
                         uint64_t begin)
{
    struct vsr_io_trim_event *event;

    if (store->trims_count == VSR_IO_TRIM_HISTORY) {
        event = &store->trims[ring_slot(
            store->trims_head, store->trims_count - 1, VSR_IO_TRIM_HISTORY)];
    } else {
        event = &store->trims[ring_slot(store->trims_head, store->trims_count,
                                        VSR_IO_TRIM_HISTORY)];
        store->trims_count++;
    }
    event->sequence = sequence;
    event->log_begin = begin;
}

/* Advances retained_begin to log_begin as of revision `oldest`, the
 * oldest one a LOAD may still name, and clears the ring below it. */
static void trims_reclaim(struct vsr_io_store *store, uint64_t oldest)
{
    uint64_t begin = store->retained_begin;

    while (store->trims_count > 0 &&
           store->trims[store->trims_head].sequence <= oldest) {
        begin = store->trims[store->trims_head].log_begin;
        store->trims_head =
            ring_slot(store->trims_head, 1, VSR_IO_TRIM_HISTORY);
        store->trims_count--;
    }
    for (uint64_t op = store->retained_begin; op < begin; ++op) {
        struct vsr_io_op_ref *ref = op_current(store, op);

        if (ref != NULL) {
            memset(ref, 0, sizeof(*ref));
        }
    }
    if (begin > store->retained_begin) {
        store->retained_begin = begin;
    }
}

/* The empty log takes its bounds from the first change that names an op
 * (the core starts at 1; a recovered log has them from the header). */
static void log_start(struct vsr_io_store *store, uint64_t op)
{
    if (store->log_begin == 0 && store->log_end == 0) {
        store->log_begin = op;
        store->log_end = op;
        store->retained_begin = op;
    }
}

/* Recomputes every client's retained entry and the previous links from
 * the log, deleting the entries left with nothing. */
static int32_t retained_rebuild(struct vsr_io_store *store)
{
    for (uint32_t i = 0; i < store->clients_capacity; ++i) {
        store->clients[i].retained = 0;
    }
    for (uint64_t op = store->log_begin; op < store->log_end; ++op) {
        struct vsr_io_op_ref *ref = op_current(store, op);
        struct vsr_io_client *entry;

        if (ref == NULL) {
            continue;
        }
        ref->previous = 0;
        if (id_zero(ref->client)) {
            continue;
        }
        entry = client_get(store, ref->client);
        if (entry == NULL) {
            return VSR_IO_FAILED;
        }
        ref->previous = entry->retained;
        entry->retained = op;
    }
    clients_sweep(store, keep_nonempty);
    return VSR_IO_OK;
}

/* -------------------------------------------------------------------------
 * Logical state and the client base
 * ---------------------------------------------------------------------- */

/* Bump regions over the logical state copies: the hard state's epoch in
 * the first part, the anchor with its epoch and manifest in the rest. */
static void state_regions(struct vsr_io_store *store, struct vsr_io_bump *hard,
                          struct vsr_io_bump *anchor)
{
    size_t epoch = 0;
    size_t total = 0;
    bool sized = epoch_bytes(&store->limits, &epoch) &&
                 state_bytes(&store->limits, &total);

    STORE_ASSERT(sized);
    (void)sized;
    vsr_io_bump_init(hard, store->state_region, epoch);
    vsr_io_bump_init(anchor, store->state_region + epoch, total - epoch);
}

/* Decodes a checkpoint into the anchor's state copy, its manifest bytes
 * copied out of the record, which the ring will overwrite. */
static int32_t decode_anchor(struct vsr_io_store *store,
                             struct vsr_io_cursor *payload,
                             struct vsr_checkpoint *out)
{
    struct vsr_io_bump hard;
    struct vsr_io_bump region;
    struct vsr_checkpoint *checkpoint = NULL;

    state_regions(store, &hard, &region);
    if (vsr_io_codec_get_checkpoint(payload, &store->limits, &region,
                                    &checkpoint) != VSR_OK) {
        return VSR_IO_CORRUPT;
    }
    if (checkpoint->manifest.size > 0) {
        size_t size = (size_t)checkpoint->manifest.size;
        unsigned char *copy = vsr_io_bump_alloc(&region, size, 1);
        struct vsr_span *span =
            vsr_io_bump_alloc(&region, sizeof(*span), alignof(struct vsr_span));

        if (copy == NULL || span == NULL) {
            return VSR_IO_FAILED; /* Sized by state_bytes: impossible. */
        }
        memcpy(copy, checkpoint->manifest.spans[0].data, size);
        span->data = copy;
        span->size = size;
        checkpoint->manifest.spans = span;
        checkpoint->manifest.count = 1;
    }
    *out = *checkpoint;
    return VSR_IO_OK;
}

enum base_source {
    BASE_FROM_CAPTURE, /* The latest capture's offsets. */
    BASE_FROM_FILE,    /* A loaded file's offsets, merged. */
    BASE_FROM_RESTORE  /* A loaded file, replacing the table. */
};

/* Makes `id` the client base at `sequence`: the entries the file covers
 * point at it, and client_base is `sequence` lowered below the record of
 * any entry it does not cover, so that the floor rule client_base + 1
 * never frees a record only the log holds. */
static void base_mark(struct vsr_io_store *store, uint64_t at);

/* The base file `id` at `sequence`, applied by the transaction (or the
 * snapshot module's call) at revision `at`. */
static void base_apply(struct vsr_io_store *store, struct vsr_id id,
                       uint64_t sequence, uint64_t at, enum base_source source)
{
    uint64_t base = sequence;

    if (source == BASE_FROM_RESTORE) {
        clients_sweep(store, keep_in_base);
    }
    for (uint32_t i = 0; i < store->clients_capacity; ++i) {
        struct vsr_io_client *entry = &store->clients[i];

        if (id_zero(entry->id)) {
            continue;
        }
        entry->base_offset = source == BASE_FROM_CAPTURE ? entry->capture_offset
                                                         : entry->next_offset;
        if (entry->current.number != 0 && entry->base_offset == UINT64_MAX &&
            entry->current.sequence <= base) {
            base =
                entry->current.sequence > 0 ? entry->current.sequence - 1 : 0;
        }
    }
    store->client_base = base;
    store->client_base_id = id;
    base_mark(store, at);
}

/* -------------------------------------------------------------------------
 * Index application (section 6.3)
 * ---------------------------------------------------------------------- */

static int32_t apply_append(struct vsr_io_store *store, uint64_t sequence,
                            uint64_t offset, uint32_t length, uint32_t index,
                            const struct vsr_io_wire_change *change,
                            struct vsr_io_cursor *payload)
{
    if (change->count == 0 || change->first == 0) {
        return VSR_IO_CORRUPT;
    }
    log_start(store, change->first);
    if (change->first != store->log_end) {
        return VSR_IO_CORRUPT; /* Not at the log end. */
    }
    for (uint32_t i = 0; i < change->count; ++i) {
        struct vsr_io_op_ref *ref;
        struct vsr_id client;
        uint64_t op;
        uint64_t epoch;
        uint64_t view;
        uint64_t number;
        uint32_t type;
        uint32_t body;

        if (!vsr_io_cursor_u64(payload, &op) ||
            !vsr_io_cursor_u64(payload, &epoch) ||
            !vsr_io_cursor_u64(payload, &view) ||
            !vsr_io_cursor_u64(payload, &client.hi) ||
            !vsr_io_cursor_u64(payload, &client.lo) ||
            !vsr_io_cursor_u64(payload, &number) ||
            !vsr_io_cursor_u32(payload, &type) ||
            !vsr_io_cursor_u32(payload, &body) ||
            body % VSR_IO_WIRE_ALIGN != 0 ||
            !vsr_io_cursor_skip(payload, body) || op != change->first + i ||
            op != store->log_end) {
            return VSR_IO_CORRUPT;
        }
        ref = op_slot(store, op);
        if (ref->op != 0 && ref->op != op) {
            return VSR_IO_FAILED; /* An unreclaimed revision names it. */
        }
        memset(ref, 0, sizeof(*ref));
        ref->op = op;
        ref->sequence = sequence;
        ref->offset = offset;
        ref->length = length;
        ref->change = index;
        ref->index = i;
        ref->client = client;
        if (!id_zero(client)) {
            struct vsr_io_client *entry = client_get(store, client);

            if (entry == NULL) {
                return VSR_IO_FAILED;
            }
            ref->previous = entry->retained;
            entry->retained = op;
            entry->inflight = 0;
        }
        store->log_end = op + 1;
    }
    return VSR_IO_OK;
}

static int32_t apply_truncate(struct vsr_io_store *store, uint64_t sequence,
                              uint64_t first)
{
    if (first == 0 || first < store->log_begin) {
        return VSR_IO_CORRUPT;
    }
    for (uint64_t op = store->log_end; op > first; --op) {
        struct vsr_io_op_ref *ref = op_current(store, op - 1);

        if (ref == NULL) {
            continue;
        }
        if (!version_add(store, ref, sequence)) {
            return VSR_IO_FAILED;
        }
        if (!id_zero(ref->client)) {
            struct vsr_io_client *entry = client_find(store, ref->client);

            if (entry != NULL && entry->retained == op - 1) {
                entry->retained =
                    ref->previous >= store->log_begin &&
                            op_current(store, ref->previous) != NULL
                        ? ref->previous
                        : 0;
                client_prune(store, entry);
            }
        }
        memset(ref, 0, sizeof(*ref));
    }
    if (first < store->log_end) {
        store->log_end = first;
    }
    store->reindexed = sequence;
    return VSR_IO_OK;
}

static int32_t apply_clients(struct vsr_io_store *store, uint64_t sequence,
                             uint64_t offset, uint32_t length, uint32_t index,
                             const struct vsr_io_wire_change *change,
                             struct vsr_io_cursor *payload)
{
    if (change->count == 0) {
        return VSR_IO_CORRUPT;
    }
    for (uint32_t i = 0; i < change->count; ++i) {
        struct vsr_io_wire_client_record wire;
        struct vsr_io_client *entry;
        struct vsr_id client;

        if (vsr_io_codec_skip_client_record(payload, &wire) != VSR_OK) {
            return VSR_IO_CORRUPT;
        }
        client.hi = wire.client_hi;
        client.lo = wire.client_lo;
        if (id_zero(client) || wire.number == 0) {
            return VSR_IO_CORRUPT;
        }
        entry = client_get(store, client);
        if (entry == NULL) {
            return VSR_IO_FAILED;
        }
        if (wire.number < entry->current.number) {
            continue; /* A replayed older result never moves it back. */
        }
        if (wire.number == entry->current.number) {
            if (wire.op != entry->current.op) {
                return VSR_IO_CORRUPT;
            }
            continue;
        }
        memset(&entry->current, 0, sizeof(entry->current));
        entry->current.number = wire.number;
        entry->current.op = wire.op;
        entry->current.sequence = sequence;
        entry->current.offset = offset;
        entry->current.length = length;
        entry->current.change = index;
        entry->current.index = i;
        entry->inflight = 0;
    }
    store->clients_sequence = sequence;
    return VSR_IO_OK;
}

static int32_t apply_hard_state(struct vsr_io_store *store,
                                struct vsr_io_cursor *payload)
{
    struct vsr_io_bump region;
    struct vsr_io_bump anchor;
    struct vsr_hard_state hard;

    state_regions(store, &region, &anchor);
    if (vsr_io_codec_get_hard_state(payload, &store->limits, &region, &hard) !=
        VSR_OK) {
        return VSR_IO_CORRUPT;
    }
    store->hard = hard;
    return VSR_IO_OK;
}

static int32_t apply_publish(struct vsr_io_store *store, uint64_t sequence,
                             struct vsr_io_cursor *payload)
{
    struct vsr_checkpoint checkpoint;
    int32_t status = decode_anchor(store, payload, &checkpoint);

    if (status != VSR_IO_OK) {
        return status;
    }
    store->anchor = checkpoint;
    if (id_equal(checkpoint.id, store->last_capture)) {
        base_apply(store, checkpoint.id, store->last_capture_sequence,
                   sequence, BASE_FROM_CAPTURE);
    } else if (store->base_state == BASE_LOADED &&
               id_equal(store->base_id, checkpoint.id)) {
        base_apply(store, checkpoint.id, sequence, sequence, BASE_FROM_FILE);
        store->base_state = BASE_NONE;
    } else if (!id_equal(checkpoint.id, store->client_base_id)) {
        /* A witness, or recovery replaying: the published snapshot is
         * the base from this sequence (recovery loads its file once the
         * scan is done and lowers the base below the records the file
         * does not cover; a witness keeps no table). */
        store->client_base = sequence;
        store->client_base_id = checkpoint.id;
        base_mark(store, sequence);
    }
    return VSR_IO_OK;
}

static int32_t apply_restore(struct vsr_io_store *store, uint64_t sequence,
                             struct vsr_io_cursor *payload)
{
    struct vsr_checkpoint checkpoint;
    int32_t status = decode_anchor(store, payload, &checkpoint);
    uint64_t begin;

    if (status != VSR_IO_OK) {
        return status;
    }
    if (checkpoint.op == UINT64_MAX) {
        return VSR_IO_CORRUPT;
    }
    store->anchor = checkpoint;
    /* Entries through the checkpoint's op leave the log as by a TRIM;
     * the ring keeps them for older revisions until RECLAIM. */
    begin = checkpoint.op + 1;
    log_start(store, begin);
    if (begin > store->log_begin) {
        store->log_begin = begin;
        if (store->log_end < begin) {
            store->log_end = begin;
        }
        trims_record(store, sequence, begin);
    }
    if (store->base_state == BASE_LOADED &&
        id_equal(store->base_id, checkpoint.id)) {
        base_apply(store, checkpoint.id, sequence, sequence, BASE_FROM_RESTORE);
        store->base_state = BASE_NONE;
    } else {
        /* A witness installs only the anchor and the protocol indexes;
         * recovery replaying loads the anchor's file after the scan. */
        clients_sweep(store, keep_none);
        store->client_base = sequence;
        store->client_base_id = checkpoint.id;
        base_mark(store, sequence);
    }
    status = retained_rebuild(store);
    store->reindexed = sequence;
    store->restored = sequence;
    return status;
}

static int32_t apply_trim(struct vsr_io_store *store, uint64_t sequence,
                          uint64_t first)
{
    if (first == 0) {
        return VSR_IO_CORRUPT;
    }
    log_start(store, first);
    if (first > store->log_end) {
        first = store->log_end;
    }
    if (first <= store->log_begin) {
        return VSR_IO_OK;
    }
    for (uint64_t op = store->log_begin; op < first; ++op) {
        const struct vsr_io_op_ref *ref = op_current(store, op);
        struct vsr_io_client *entry;

        if (ref == NULL || id_zero(ref->client)) {
            continue;
        }
        entry = client_find(store, ref->client);
        if (entry != NULL && entry->retained == op) {
            entry->retained = 0;
            client_prune(store, entry);
        }
    }
    store->log_begin = first;
    trims_record(store, sequence, first);
    store->reindexed = sequence;
    return VSR_IO_OK;
}

static int32_t apply_identity(struct vsr_io_store *store, uint64_t sequence,
                              struct vsr_io_cursor *payload)
{
    struct vsr_store_identity identity;

    if (sequence != 1 || store->identity_set ||
        vsr_io_codec_get_identity(payload, &identity) != VSR_OK) {
        return VSR_IO_CORRUPT;
    }
    store->identity = identity;
    store->identity_set = true;
    store->superblock_dirty = 1; /* The superblock carries the identity. */
    return VSR_IO_OK;
}

static int32_t apply_change(struct vsr_io_store *store, uint64_t sequence,
                            uint64_t offset, uint32_t length, uint32_t index,
                            const struct vsr_io_wire_change *change,
                            struct vsr_io_cursor *payload)
{
    switch (change->type) {
    case VSR_STORE_APPEND:
        return apply_append(store, sequence, offset, length, index, change,
                            payload);
    case VSR_STORE_TRUNCATE:
        return apply_truncate(store, sequence, change->first);
    case VSR_STORE_CLIENTS:
        return apply_clients(store, sequence, offset, length, index, change,
                             payload);
    case VSR_STORE_HARD_STATE:
        return apply_hard_state(store, payload);
    case VSR_STORE_PUBLISH_CHECKPOINT:
        return apply_publish(store, sequence, payload);
    case VSR_STORE_RESTORE_CHECKPOINT:
        return apply_restore(store, sequence, payload);
    case VSR_STORE_TRIM:
        return apply_trim(store, sequence, change->first);
    case VSR_STORE_IDENTITY:
        return apply_identity(store, sequence, payload);
    default:
        return VSR_IO_CORRUPT;
    }
}

/* Reads the record header at `bytes` and its change descriptors, each
 * checked against the record's length (the codec bounds neither offset +
 * length nor the descriptors' own bytes). */
static int32_t record_changes(const unsigned char *bytes, uint32_t length,
                              struct vsr_io_wire_record *header,
                              struct vsr_io_wire_change *changes)
{
    struct vsr_io_cursor cursor;
    uint32_t kind = 0;
    uint64_t descriptors;

    vsr_io_cursor_init_one(&cursor, bytes, length);
    if (vsr_io_codec_get_record(&cursor, length, header, &kind) != VSR_OK ||
        kind != VSR_IO_SCAN_RECORD || header->length != length) {
        return VSR_IO_CORRUPT;
    }
    descriptors = sizeof(*header) +
                  (uint64_t)header->count * sizeof(struct vsr_io_wire_change);
    for (uint32_t i = 0; i < header->count; ++i) {
        if (vsr_io_codec_get_change(&cursor, &changes[i]) != VSR_OK ||
            changes[i].offset < descriptors || changes[i].offset > length ||
            changes[i].length > length - changes[i].offset) {
            return VSR_IO_CORRUPT;
        }
    }
    return VSR_IO_OK;
}

/* Applies one packed record to the indexes (section 6.3) from its bytes:
 * the ring copy of a STORE, or a recovery slab. False with the status to
 * fail with: CORRUPT for bytes the codec rejects or a change that
 * contradicts the log, FAILED for an index at capacity. */
static bool index_apply(struct vsr_io_store *store, uint64_t offset,
                        uint32_t length, const unsigned char *bytes,
                        int32_t *status)
{
    struct vsr_io_wire_record header;
    struct vsr_io_wire_change changes[VSR_MAX_STORE_CHANGES];

    *status = record_changes(bytes, length, &header, changes);
    if (*status != VSR_IO_OK) {
        return false;
    }
    for (uint32_t i = 0; i < header.count; ++i) {
        struct vsr_io_cursor payload;

        vsr_io_cursor_init_one(&payload, bytes + changes[i].offset,
                               changes[i].length);
        *status = apply_change(store, header.sequence, offset, length, i,
                               &changes[i], &payload);
        if (*status != VSR_IO_OK) {
            return false;
        }
    }
    return true;
}

/* A RESTORE, or a PUBLISH of a snapshot other than the latest capture and
 * the current base, waits for its clients file before it is packed
 * (decision 43) when the replica's role after the transaction is FULL.
 * True while `pending` must wait; base_wanted reports the file. */
static bool base_hold(struct vsr_io_store *store,
                      const struct vsr_io_pending_store *pending)
{
    const struct vsr_store *transaction = pending->transaction;
    const struct vsr_checkpoint *wanted = NULL;
    uint32_t role = store->hard.role;
    uint32_t kind = BASE_PUBLISH;

    if (store->base_state == BASE_LOADED) {
        return false;
    }
    if (store->base_state != BASE_NONE) {
        return true;
    }
    for (uint32_t i = 0; i < transaction->count; ++i) {
        const struct vsr_change *change = &transaction->changes[i];

        if (change->type == VSR_STORE_HARD_STATE && change->data != NULL) {
            role = ((const struct vsr_hard_state *)change->data)->role;
        }
    }
    for (uint32_t i = 0; i < transaction->count; ++i) {
        const struct vsr_change *change = &transaction->changes[i];
        const struct vsr_checkpoint *checkpoint = change->data;

        if (checkpoint == NULL) {
            continue;
        }
        if (change->type == VSR_STORE_RESTORE_CHECKPOINT) {
            wanted = checkpoint;
            kind = BASE_RESTORE;
        } else if (change->type == VSR_STORE_PUBLISH_CHECKPOINT &&
                   !id_equal(checkpoint->id, store->last_capture) &&
                   !id_equal(checkpoint->id, store->client_base_id)) {
            wanted = checkpoint;
            kind = BASE_PUBLISH;
        }
    }
    if (wanted == NULL || role != VSR_MEMBER_FULL) {
        return false;
    }
    store->base_state = BASE_WANTED;
    store->base_kind = kind;
    store->base_id = wanted->id;
    store->base_sequence = pending->sequence;
    store->base_op = pending->op;
    return true;
}

/* -------------------------------------------------------------------------
 * Packing
 * ---------------------------------------------------------------------- */

/* True while a freed slot waits for the superblock write that lets it be
 * reused. */
static void flush_request(struct vsr_io_store *store);
static bool reclaim_pending(const struct vsr_io_store *store);

static bool segment_freeing(const struct vsr_io_store *store)
{
    for (uint32_t i = 0; i < store->slots; ++i) {
        if (store->segments[i].phase == VSR_IO_SEGMENT_FREEING ||
            store->segments[i].phase == VSR_IO_SEGMENT_FLUSHING) {
            return true;
        }
    }
    return false;
}

/* Fails the head STORE (which `pending` is) with `status` and fences the
 * store: a non-OK STORE completion fences the replica (vsr.h). */
static void store_abort(struct vsr_io_store *store,
                        const struct vsr_io_pending_store *pending,
                        int32_t status)
{
    uint64_t op = pending->op;

    STORE_ASSERT(pending == stores_at(store, 0));
    stores_pop(store);
    complete(store, op, status, NONE, NULL);
    store_fail(store, status);
}

/* Tries to pack the head STORE: the base file it may wait for, the
 * segment fit, the slot supply (freeing, then growth), the pinned-floor
 * and write-behind holds (section 6.1 steps 1 to 4), then the copy and
 * the indexes (step 5). False when it must stay held, or when it was
 * failed and the store fenced. */
/* The bytes n record bytes take in the current segment: n, plus the
 * padding that closes the ring's last block when they do not fit before
 * the wrap (ring_wrap pads to the block, in the file too). */
static uint64_t pack_needs(const struct vsr_io_store *store, uint64_t n)
{
    uint64_t block = block_bytes(store);

    if (store->head % store->ring_size + n > store->ring_size &&
        store->head % block != 0) {
        return n + block - store->head % block;
    }
    return n;
}

static bool store_pack(struct vsr_io_store *store,
                       struct vsr_io_pending_store *pending)
{
    uint64_t n = pending->bytes;
    bool seal = store->current == NONE ||
                store->segments[store->current].used + pack_needs(store, n) >
                    store->options.segment_bytes;
    uint32_t slot = NONE;
    struct vsr_io_extent *newest;
    unsigned char *at;
    uint64_t offset;
    size_t written = 0;
    int32_t status;
    int rc;

    if (base_hold(store, pending)) {
        return false;
    }
    if (seal) {
        slot = segment_free_slot(store);
        if (slot == NONE) {
            vsr_io_store_free_segments(store);
            slot = segment_free_slot(store);
        }
        if (slot == NONE && memory_only(store) &&
            store->slots < store->options.max_segments) {
            slot = store->slots++; /* A slot of the table, not the file. */
        }
        if (slot == NONE) {
            if (store->growth == GROWTH_NONE &&
                store->slots < store->options.max_segments) {
                store->growth = GROWTH_ALLOCATING;
            } else if (store->growth == GROWTH_NONE &&
                       !segment_freeing(store)) {
                if (reclaim_pending(store)) {
                    /* A floor term waits for the media (decision S16):
                     * the flush that advances it may free a slot. */
                    flush_request(store);
                } else {
                    /* Every slot is kept by a floor: nothing can free. */
                    store_abort(store, pending, VSR_IO_FAILED);
                }
            }
            return false;
        }
    }
    if (pack_end(store, seal, n) > pin_floor(store) + store->ring_size) {
        return false; /* Pinned or unwritten bytes would be overwritten. */
    }
    if (store->head - written_floor(store) >
        store->options.write_behind_bytes) {
        return false; /* Backpressure. */
    }
    if (seal) {
        segment_seal(store);
        segment_open(store, slot);
    }
    ring_wrap(store, n);
    offset = store->file_head;
    at = ring_append(store, n);
    rc = vsr_io_codec_put_record(pending->transaction, store->generation,
                                 store->run, store->durable, at, (size_t)n,
                                 &written);
    STORE_ASSERT(rc == VSR_OK && written == n);
    (void)rc;
    newest = extent_newest(store); /* ring_append needs it: never NULL. */
    STORE_ASSERT(newest != NULL);
    if (newest != NULL) {
        newest->last_sequence = pending->sequence;
    }
    store->readable = pending->sequence;
    store->packed_since_durable = 1;
    if (!index_apply(store, offset, (uint32_t)n, at, &status)) {
        store_abort(store, pending, status);
        return false;
    }
    complete(store, pending->op, VSR_IO_OK, NONE, NULL);
    return true;
}

static void stores_drain(struct vsr_io_store *store)
{
    while (store->state == VSR_IO_STORE_READY && store->stores_count > 0 &&
           store_pack(store, stores_at(store, 0))) {
        stores_pop(store);
    }
}

int vsr_io_store_store(struct vsr_io_store *store, uint64_t op,
                       const struct vsr_store *transaction)
{
    struct vsr_io_pending_store *pending;
    size_t bytes = 0;
    int rc;

    if (store == NULL || op == 0 || transaction == NULL) {
        return VSR_EINVAL;
    }
    if (store->state == VSR_IO_STORE_FAILED) {
        complete(store, op, VSR_IO_FAILED, NONE, NULL);
        return VSR_OK;
    }
    if ((store->state != VSR_IO_STORE_READY &&
         store->state != VSR_IO_STORE_CREATING) ||
        transaction->sequence != next_sequence(store) ||
        store->stores_count == store->limits.operations) {
        return VSR_EINVAL;
    }
    rc = vsr_io_codec_record_bytes(transaction, &store->limits, &bytes);
    if (rc == VSR_EINVAL) {
        return VSR_EINVAL;
    }
    if (rc != VSR_OK || bytes > store->max_record_bytes) {
        /* Impossible for a core within its limits (section 5.3). */
        complete(store, op, VSR_IO_FAILED, NONE, NULL);
        store_fail(store, VSR_IO_FAILED);
        return VSR_OK;
    }
    pending = stores_at(store, store->stores_count);
    store->stores_count++;
    pending->op = op;
    pending->transaction = transaction;
    pending->sequence = transaction->sequence;
    pending->bytes = bytes;
    stores_drain(store);
    return VSR_OK;
}

/* -------------------------------------------------------------------------
 * SYNC and flushes
 * ---------------------------------------------------------------------- */

/* The sequence every transaction at or below which is durable now. */
static uint64_t durable_now(const struct vsr_io_store *store)
{
    return store->options.sync_mode == VSR_IO_SYNC_DSYNC ? store->written
                                                         : store->flushed;
}

/* The revisions a crash can bring back are those from the sequence on
 * media on (the recovered sequence is at least it, decision 89): the
 * RECLAIM revision applies as far as that sequence and the rest as it
 * advances, and the floor's client base term is the base as of it
 * (decision S16). */
static void reclaim_apply(struct vsr_io_store *store)
{
    uint64_t media = durable_now(store);
    uint64_t target = store->reclaim < media ? store->reclaim : media;

    if (store->client_base_pending != 0 &&
        store->client_base_pending <= media) {
        store->client_base_floor = store->client_base;
        store->client_base_pending = 0;
    }
    if (target > store->reclaimed) {
        store->reclaimed = target;
        trims_reclaim(store, target);
        versions_reclaim(store, target);
    }
    vsr_io_store_free_segments(store);
}

/* True while a RECLAIM, or a client base change, waits for the media
 * sequence to reach it: a flush may free a slot then. */
static bool reclaim_pending(const struct vsr_io_store *store)
{
    uint64_t target =
        store->reclaim < store->readable ? store->reclaim : store->readable;

    return store->reclaimed < target || store->client_base_pending != 0;
}

/* The client base changed at revision `at`: the floor's base term
 * follows once that revision is on media. */
static void base_mark(struct vsr_io_store *store, uint64_t at)
{
    if (at > durable_now(store)) {
        store->client_base_pending = at;
    } else {
        store->client_base_floor = store->client_base;
        store->client_base_pending = 0;
    }
}

/* Completes the SYNCs satisfied so far, in emission order. */
static void syncs_settle(struct vsr_io_store *store)
{
    uint64_t durable = durable_now(store);

    while (store->syncs_count > 0 && syncs_at(store, 0)->sequence <= durable) {
        struct vsr_io_pending_sync *sync = syncs_at(store, 0);

        if (sync->sequence > store->durable) {
            store->durable = sync->sequence;
            store->packed_since_durable = 0;
        }
        complete(store, sync->op, VSR_IO_OK, NONE, NULL);
        syncs_pop(store);
    }
    if (store->options.sync_mode == VSR_IO_SYNC_FDATASYNC &&
        store->syncs_count > 0 && store->flush_pending == FLUSH_NONE &&
        store->flush_slot == NONE && store->flush_deadline == VSR_NO_DEADLINE) {
        /* A batch that waited through a flush needs the next one. */
        store->flush_pending = FLUSH_DUE;
    }
}

int vsr_io_store_sync(struct vsr_io_store *store, uint64_t op,
                      uint64_t sequence)
{
    struct vsr_io_pending_sync *sync;

    if (store == NULL || op == 0) {
        return VSR_EINVAL;
    }
    if (store->state == VSR_IO_STORE_FAILED) {
        complete(store, op, VSR_IO_FAILED, NONE, NULL);
        return VSR_OK;
    }
    if ((store->state != VSR_IO_STORE_READY &&
         store->state != VSR_IO_STORE_CREATING) ||
        sequence == 0 || sequence >= next_sequence(store) ||
        store->syncs_count == store->limits.operations) {
        return VSR_EINVAL;
    }
    sync = syncs_at(store, store->syncs_count);
    store->syncs_count++;
    sync->op = op;
    sync->sequence = sequence;
    if (sequence > store->flush_target) {
        store->flush_target = sequence;
    }
    if (store->options.sync_mode == VSR_IO_SYNC_FDATASYNC &&
        sequence > store->flushed && store->flush_pending == FLUSH_NONE &&
        store->flush_slot == NONE && store->flush_deadline == VSR_NO_DEADLINE) {
        store->flush_pending = FLUSH_WANTED; /* Else the flush out decides. */
    }
    syncs_settle(store);
    return VSR_OK;
}

static uint64_t flush_interval(const struct vsr_io_store *store)
{
    return store->options.flush_interval_ns != 0
               ? store->options.flush_interval_ns
               : STORE_FLUSH_INTERVAL_NS;
}

/* -------------------------------------------------------------------------
 * Writes: planning and completion
 * ---------------------------------------------------------------------- */

static struct vsr_io_write *write_at(const struct vsr_io_store *store,
                                     uint32_t i)
{
    return &store->writes[ring_slot(store->writes_head, i,
                                    store->options.inflight_writes)];
}

/* The first extent with bytes at or after `issued`, which moves over the
 * gap a wrap left so that unissued bytes are counted exactly; NULL when
 * everything packed was issued. */
static struct vsr_io_extent *issue_extent(struct vsr_io_store *store)
{
    for (uint32_t i = 0; i < store->extents_count; ++i) {
        struct vsr_io_extent *extent = extent_at(store, i);

        if (extent->segment != NONE &&
            extent->ring_offset + extent->length > store->issued) {
            if (store->issued < extent->ring_offset) {
                store->issued = extent->ring_offset;
            }
            return extent;
        }
    }
    return NULL;
}

/* Plans the next write of [issued, head): the first extent with bytes to
 * issue, cut at the extent's end; a segment header is a write of its own,
 * and the records after it wait for the header's completion. The newest
 * extent is padded to a block first. Returns the entry or NULL. */
static struct vsr_io_write *write_plan(struct vsr_io_store *store)
{
    struct vsr_io_extent *extent;
    struct vsr_io_write *write;
    uint64_t begin;
    uint64_t end;
    uint32_t index;

    if (store->error != 0 ||
        store->writes_count == store->options.inflight_writes) {
        return NULL;
    }
    extent = issue_extent(store);
    if (extent == NULL) {
        return NULL;
    }
    begin = store->issued;
    index = ring_slot(store->writes_head, store->writes_count,
                      store->options.inflight_writes);
    write = &store->writes[index];
    if (begin == extent->ring_offset && extent->header != 0) {
        end = begin + store->header_bytes;
    } else {
        if (store->segments[extent->segment].phase == VSR_IO_SEGMENT_HEADER) {
            return NULL; /* Records wait for the header's write. */
        }
        if (extent == extent_newest(store)) {
            ring_pad(store);
        }
        end = extent->ring_offset + extent->length;
        if (end == begin) {
            return NULL; /* Nothing packed since the last write. */
        }
    }
    memset(write, 0, sizeof(*write));
    write->slot = NONE;
    if (begin == extent->ring_offset && extent->header != 0) {
        write->header = 1;
        write->last_sequence = NO_SEQUENCE;
        store->segments[extent->segment].header_write = index;
    } else {
        write->last_sequence = extent->last_sequence;
    }
    STORE_ASSERT(begin % block_bytes(store) == 0 &&
                 end % block_bytes(store) == 0 && end > begin &&
                 end - begin <= STORE_WRITE_MAX_BYTES);
    write->state = WRITE_PENDING;
    write->ring_begin = begin;
    write->ring_end = end;
    write->file_offset = extent->file_offset + (begin - extent->ring_offset);
    write->segment = extent->segment;
    store->issued = end;
    store->writes_count++;
    return write;
}

static void write_error(struct vsr_io_store *store, int32_t error)
{
    store->error = error;
    if (store->options.on_write_error == VSR_IO_WRITE_ERROR_FENCE) {
        store_fail(store, VSR_IO_FAILED);
    }
    /* CONTINUE: the store stops writing and serves from memory; no SYNC
     * is ever issued in replicated mode, so nothing else waits. */
}

/* A record or header write completed: the segment opens when its header
 * is on disk, then `written` follows the contiguous completed prefix. */
static void write_done(struct vsr_io_store *store, uint32_t index,
                       int32_t result)
{
    struct vsr_io_write *write = &store->writes[index];
    struct vsr_io_segment *segment = &store->segments[write->segment];

    STORE_ASSERT(write->state == WRITE_INFLIGHT);
    write->state = WRITE_COMPLETE;
    write->slot = NONE;
    if (result < 0 || (uint64_t)result != write->ring_end - write->ring_begin) {
        write_error(store, result < 0 ? result : -EIO);
    } else if (write->header != 0) {
        segment->header_write = NONE;
        if (segment->phase == VSR_IO_SEGMENT_HEADER) {
            segment->phase = VSR_IO_SEGMENT_OPEN;
        }
        if (store->state == VSR_IO_STORE_CREATING) {
            /* The empty store exists: what NEW and JOIN expect. */
            store->state = VSR_IO_STORE_READY;
            if (store->recovery.load_op != 0) {
                complete(store, store->recovery.load_op, VSR_IO_NOT_FOUND, NONE,
                         NULL);
                store->recovery.load_op = 0;
            }
        }
    }
    /* The completed prefix, in issue order; after an error nothing is
     * known to be on disk beyond what already was. */
    while (store->writes_count > 0 &&
           write_at(store, 0)->state == WRITE_COMPLETE) {
        struct vsr_io_write *oldest = write_at(store, 0);

        if (store->error == 0 && oldest->last_sequence > store->written) {
            store->written = oldest->last_sequence;
        }
        oldest->state = WRITE_FREE;
        store->writes_head =
            ring_slot(store->writes_head, 1, store->options.inflight_writes);
        store->writes_count--;
    }
    if (store->writes_count == 0) {
        (void)issue_extent(store); /* Nothing unwritten before a gap. */
    }
    if (store->options.sync_mode == VSR_IO_SYNC_DSYNC) {
        reclaim_apply(store); /* written is on media. */
    }
    syncs_settle(store);
}

/* A flush the store wants for itself: FLUSHING slots wait for one issued
 * after their superblock write completed (decision S15). */
static void flush_request(struct vsr_io_store *store)
{
    if (store->options.sync_mode == VSR_IO_SYNC_FDATASYNC &&
        store->error == 0 && store->flush_pending == FLUSH_NONE &&
        store->flush_slot == NONE) {
        store->flush_pending = FLUSH_DUE;
        store->flush_deadline = VSR_NO_DEADLINE;
    }
}

static void flush_done(struct vsr_io_store *store, uint64_t covered,
                       int32_t result)
{
    store->flush_slot = NONE;
    if (result < 0) {
        write_error(store, result);
        return;
    }
    if (covered > store->flushed) {
        store->flushed = covered;
    }
    if (store->freeing_flush == FREEING_FLUSH_INFLIGHT) {
        /* The superblock naming the new start segment is on media: the
         * slots it no longer names may be reused (decision S15). */
        for (uint32_t i = 0; i < store->slots; ++i) {
            if (store->segments[i].phase == VSR_IO_SEGMENT_FLUSHING) {
                store->segments[i].phase = VSR_IO_SEGMENT_FREE;
            }
        }
        store->freeing_flush = FREEING_FLUSH_NONE;
    } else if (store->freeing_flush == FREEING_FLUSH_WANTED) {
        flush_request(store); /* This one was issued before the write. */
    }
    reclaim_apply(store);
    syncs_settle(store);
}

static void superblock_done(struct vsr_io_store *store, uint64_t floor,
                            int32_t result)
{
    store->superblock_pending = 0;
    if (result < 0) {
        write_error(store, result);
        return;
    }
    if (floor > store->superblock_floor) {
        store->superblock_floor = floor;
    }
    if (store->growth == GROWTH_SUPERBLOCK) {
        store->slots = file_slots(store);
        store->growth = GROWTH_NONE;
    }
    /* The superblock naming a present start segment is written: the
     * slots freed before this write may be reused once it is on media,
     * which O_DSYNC made it, and which in FDATASYNC mode a flush issued
     * from now on makes it (section 6.5, decision S15). A slot freed
     * after it was issued set superblock_dirty again and waits for the
     * next write. */
    if (store->superblock_dirty != 0) {
        return;
    }
    for (uint32_t i = 0; i < store->slots; ++i) {
        if (store->segments[i].phase == VSR_IO_SEGMENT_FREEING) {
            if (store->options.sync_mode == VSR_IO_SYNC_FDATASYNC) {
                store->segments[i].phase = VSR_IO_SEGMENT_FLUSHING;
                store->freeing_flush = FREEING_FLUSH_WANTED;
            } else {
                store->segments[i].phase = VSR_IO_SEGMENT_FREE;
            }
        }
    }
    if (store->freeing_flush == FREEING_FLUSH_WANTED) {
        flush_request(store);
    }
}

static void recovery_fail(struct vsr_io_store *store, int32_t status);

static void file_done(struct vsr_io_store *store, uint32_t op, uint64_t cookie,
                      int32_t result)
{
    store->file_op = FILE_NONE;
    switch (op) {
    case FILE_PROBE:
        if (result >= 0) {
            /* An existing log is recovered under every start mode: NEW
             * and JOIN get the recovered row, which the core rejects
             * itself, or NOT_FOUND for an empty log (decision 71).
             * TODO(snapshot module, decision 92): the executor has no
             * directory listing, so stray clients-* files neither make
             * NEW and JOIN refuse the directory nor are unlinked by
             * RECOVER (decision 57); once a listing exists, the snapshot
             * module should do both at attach, unlinking every
             * clients-<id> the recovered anchor does not name. */
            store->log_slot = (int32_t)store->file_slot;
            store->state = VSR_IO_STORE_RECOVERING;
            store->recovery.stage = RECOVERY_STAT;
            return;
        }
        if (result != -ENOENT) {
            store->error = result;
            store_fail(store, VSR_IO_FAILED);
            return;
        }
        store->state = VSR_IO_STORE_CREATING;
        if (store->start_mode == VSR_START_RECOVER) {
            /* Nothing survives: the core hears so at once (section 6.4)
             * and the empty log is created for the warm-up that may
             * follow (decision 71); its STOREs are held until then. */
            complete(store, store->recovery.load_op, VSR_IO_NOT_FOUND, NONE,
                     NULL);
            store->recovery.load_op = 0;
        }
        return;
    case FILE_CREATE:
        if (result < 0) {
            store->error = result;
            store_fail(store, VSR_IO_FAILED);
            return;
        }
        store->log_slot = (int32_t)store->file_slot;
        return;
    case FILE_ALLOCATE:
        if (result < 0) {
            store->error = result;
            store_fail(store, VSR_IO_FAILED);
            return;
        }
        store->file_size += cookie; /* The chunk allocated. */
        return;
    case FILE_STAT:
        if (result < 0) {
            store->error = result;
            recovery_fail(store, VSR_IO_FAILED);
            return;
        }
        store->file_size =
            ((const struct statx *)(void *)store->ring)->stx_size;
        store->recovery.retried = 0;
        store->recovery.stage = RECOVERY_SUPERBLOCKS;
        return;
    case FILE_GROW:
        if (result < 0) {
            store->error = result;
            store_fail(store, VSR_IO_FAILED);
            return;
        }
        store->file_size += store->options.segment_bytes;
        if (memory_only(store)) {
            store->slots = file_slots(store); /* No superblock follows. */
            store->growth = GROWTH_NONE;
            return;
        }
        store->growth = GROWTH_SUPERBLOCK;
        store->superblock_dirty = 1;
        return;
    default:
        STORE_ASSERT(false);
        return;
    }
}

/* -------------------------------------------------------------------------
 * Executor records
 * ---------------------------------------------------------------------- */

/* Fills `sqe` with a FIXED_FILE operation on the log. */
static void log_sqe(const struct vsr_io_store *store, struct vsr_io_sqe *sqe,
                    uint8_t opcode, uint64_t user_data)
{
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode = opcode;
    sqe->flags = VSR_IO_SQE_FIXED_FILE;
    sqe->fd = store->log_slot;
    sqe->user_data = user_data;
}

static uint32_t take_slot(struct vsr_io *io, uint8_t kind, uint32_t replica,
                          uint32_t sub, uint64_t cookie)
{
    return vsr_io_slots_alloc(&io->slots, kind, 1, replica, sub, cookie);
}

/* -------------------------------------------------------------------------
 * Loads
 *
 * A LOAD is resolved against the indexes when it is accepted (decision
 * 51): the records it needs go to load_refs. When every record is hot and
 * a lease is free the result is built at once, under a ring pin;
 * otherwise the load queues, a hot one for a lease, a cold one for the
 * single read in flight per replica (decision 37), which brings the
 * block-aligned range covering its records into a pool slab whose
 * reference the lease then holds. Every result, an empty one included,
 * is a vsr_loaded graph in the lease's region.
 * ---------------------------------------------------------------------- */

/* The slot a log offset lies in, or NONE for the superblocks. */
static uint32_t segment_of(const struct vsr_io_store *store, uint64_t offset)
{
    uint64_t fixed = STORE_SUPERBLOCKS * block_bytes(store);

    if (offset < fixed) {
        return NONE;
    }
    return (uint32_t)((offset - fixed) / store->options.segment_bytes);
}

static void ref_set(struct vsr_io_load_ref *ref, uint64_t offset,
                    uint64_t sequence, uint32_t length, uint32_t change,
                    uint32_t index)
{
    memset(ref, 0, sizeof(*ref));
    ref->offset = offset;
    ref->sequence = sequence;
    ref->length = length;
    ref->change = change;
    ref->index = index;
}

/* LOG: the versions of [first, end) at the read's sequence, up to
 * max_count of them; the byte bound is applied when they are decoded. */
static int32_t resolve_log(const struct vsr_io_store *store,
                           struct vsr_io_pending_load *load,
                           struct vsr_io_load_ref *refs)
{
    const struct vsr_store_read *read = &load->read;
    uint64_t op = read->first;

    while (op < read->end && load->count < read->max_count) {
        const struct vsr_io_op_ref *ref = op_current(store, op);

        if (ref != NULL && ref->sequence <= read->sequence) {
            ref_set(&refs[load->count], ref->offset, ref->sequence, ref->length,
                    ref->change, ref->index);
        } else {
            const struct vsr_io_version *version =
                version_at(store, op, read->sequence);

            if (version == NULL) {
                break;
            }
            ref_set(&refs[load->count], version->offset, version->appended,
                    version->length, version->change, version->index);
        }
        load->count++;
        op++;
    }
    if (load->count == 0 && read->first < read->end) {
        return VSR_IO_NOT_FOUND;
    }
    return VSR_IO_OK;
}

/* CLIENT: the entry's completed record when the read's sequence covers
 * it (decision 51); from the ring when still there, else from the base
 * file when it holds the record, else from the log. */
static int32_t resolve_client(const struct vsr_io_store *store,
                              struct vsr_io_pending_load *load,
                              struct vsr_io_load_ref *refs)
{
    const struct vsr_io_client *entry = client_find(store, load->read.client);
    const struct vsr_io_client_version *version;
    uint64_t ring;

    if (entry == NULL || entry->current.number == 0) {
        return VSR_IO_OK;
    }
    if (entry->current.sequence > load->read.sequence ||
        store->restored > load->read.sequence) {
        return VSR_IO_RETRY;
    }
    version = &entry->current;
    if (version->sequence != 0 &&
        extent_locate(store, version->offset, version->length, &ring)) {
        ref_set(&refs[0], version->offset, version->sequence, version->length,
                version->change, version->index);
    } else if (entry->base_offset != UINT64_MAX) {
        ref_set(&refs[0], entry->base_offset, 0, 0, 0, 0);
        load->base = 1;
    } else if (version->sequence != 0) {
        ref_set(&refs[0], version->offset, version->sequence, version->length,
                version->change, version->index);
    } else {
        return VSR_IO_CORRUPT; /* Only a base file held it, and none does. */
    }
    load->count = 1;
    return VSR_IO_OK;
}

/* REQUEST: the entry's retained entry at the read's sequence, walking
 * the previous links over the APPENDs after it. */
static int32_t resolve_request(const struct vsr_io_store *store,
                               struct vsr_io_pending_load *load,
                               struct vsr_io_load_ref *refs)
{
    const struct vsr_io_client *entry = client_find(store, load->read.client);
    const struct vsr_io_op_ref *ref;

    if (entry == NULL || entry->retained == 0) {
        return VSR_IO_OK;
    }
    if (store->reindexed > load->read.sequence) {
        return VSR_IO_RETRY;
    }
    ref = op_current(store, entry->retained);
    while (ref != NULL && ref->sequence > load->read.sequence) {
        ref = ref->previous >= store->retained_begin
                  ? op_current(store, ref->previous)
                  : NULL;
    }
    if (ref == NULL) {
        return VSR_IO_OK;
    }
    ref_set(&refs[0], ref->offset, ref->sequence, ref->length, ref->change,
            ref->index);
    load->count = 1;
    return VSR_IO_OK;
}

/* True when every record of the load is in the ring; the unwrapped range
 * covering them is what the lease pins. */
static bool load_hot(const struct vsr_io_store *store,
                     const struct vsr_io_pending_load *load,
                     const struct vsr_io_load_ref *refs, uint64_t *begin,
                     uint64_t *end)
{
    if (load->base != 0) {
        return false;
    }
    *begin = 0;
    *end = 0;
    for (uint32_t i = 0; i < load->count; ++i) {
        uint64_t ring;

        if (!extent_locate(store, refs[i].offset, refs[i].length, &ring)) {
            return false;
        }
        if (i == 0 || ring < *begin) {
            *begin = ring;
        }
        if (ring + refs[i].length > *end) {
            *end = ring + refs[i].length;
        }
    }
    return true;
}

/* Where a result's record bytes come from: a slab holding the file range
 * [offset, offset + length), or the ring when base is NULL. */
struct load_source {
    const unsigned char *base;
    uint64_t offset;
    uint64_t length;
};

/* The bytes of [offset, offset + length) of the file, or NULL when the
 * source does not hold them all (a read short of the record). */
static const unsigned char *load_bytes(const struct vsr_io_store *store,
                                       const struct load_source *source,
                                       uint64_t offset, uint64_t length)
{
    struct vsr_io_piece piece;

    if (source->base != NULL) {
        if (offset < source->offset ||
            offset - source->offset > source->length ||
            length > source->length - (offset - source->offset)) {
            return NULL;
        }
        return source->base + (offset - source->offset);
    }
    if (length > UINT32_MAX ||
        !vsr_io_store_hot(store, offset, (uint32_t)length, &piece)) {
        return NULL;
    }
    return piece.base;
}

/* Positions `payload` at change `change` of the record at `bytes`, whose
 * header must carry `sequence`; the payload CRC is checked when `check`
 * (bytes read back from the disk). */
static int32_t record_payload(const unsigned char *bytes, uint32_t length,
                              uint64_t sequence, bool check, uint32_t change,
                              struct vsr_io_cursor *payload)
{
    struct vsr_io_cursor cursor;
    struct vsr_io_wire_record header;
    struct vsr_io_wire_change changes[VSR_MAX_STORE_CHANGES];
    int32_t status = record_changes(bytes, length, &header, changes);

    if (status != VSR_IO_OK) {
        return status;
    }
    if (header.sequence != sequence || change >= header.count) {
        return VSR_IO_CORRUPT;
    }
    vsr_io_cursor_init_one(&cursor, bytes, length);
    if (check && (!vsr_io_cursor_skip(&cursor, sizeof(header)) ||
                  !vsr_io_codec_check_record(&cursor, &header))) {
        return VSR_IO_CORRUPT;
    }
    vsr_io_cursor_init_one(payload, bytes + changes[change].offset,
                           changes[change].length);
    return VSR_IO_OK;
}

/* Payload bytes of a loaded entry, for the read's byte bound. */
static uint64_t entry_bytes(const struct vsr_entry *entry)
{
    if (entry->type == VSR_REQUEST_COMMAND && entry->body != NULL) {
        return ((const struct vsr_blob *)entry->body)->size;
    }
    return 0;
}

/* Decodes the resolved entries of a LOG or REQUEST load into an array
 * from the region, stopping before the entry that would exceed the byte
 * bound (never before the first). */
static int32_t build_entries(const struct vsr_io_store *store,
                             const struct vsr_io_pending_load *load,
                             const struct vsr_io_load_ref *refs,
                             const struct load_source *source,
                             struct vsr_io_bump *region,
                             struct vsr_loaded *loaded)
{
    struct vsr_entry *entries = vsr_io_bump_alloc(
        region, load->count * sizeof(*entries), alignof(struct vsr_entry));
    uint64_t bytes = 0;
    uint32_t n = 0;

    if (entries == NULL) {
        return VSR_IO_FAILED;
    }
    for (; n < load->count; ++n) {
        const struct vsr_io_load_ref *ref = &refs[n];
        const unsigned char *at =
            load_bytes(store, source, ref->offset, ref->length);
        struct vsr_io_cursor payload;
        int32_t status;
        int rc;

        if (at == NULL) {
            return VSR_IO_CORRUPT;
        }
        status = record_payload(at, ref->length, ref->sequence,
                                source->base != NULL, ref->change, &payload);
        if (status != VSR_IO_OK) {
            return status;
        }
        rc = vsr_io_codec_get_entry_at(&payload, ref->index, &store->limits,
                                       region, &entries[n]);
        if (rc != VSR_OK) {
            return rc == VSR_ELIMIT ? VSR_IO_FAILED : VSR_IO_CORRUPT;
        }
        if (n > 0 && bytes + entry_bytes(&entries[n]) > load->read.max_bytes) {
            break;
        }
        bytes += entry_bytes(&entries[n]);
    }
    loaded->items = entries;
    loaded->count = n;
    return VSR_IO_OK;
}

/* Decodes the completed client record of a CLIENT load: from the log
 * record's CLIENTS change, or from the base file. */
static int32_t build_client(const struct vsr_io_store *store,
                            const struct vsr_io_pending_load *load,
                            const struct vsr_io_load_ref *ref,
                            const struct load_source *source,
                            struct vsr_io_bump *region,
                            struct vsr_loaded *loaded)
{
    struct vsr_client_record *record = vsr_io_bump_alloc(
        region, sizeof(*record), alignof(struct vsr_client_record));
    struct vsr_io_cursor payload;
    int rc;

    if (record == NULL) {
        return VSR_IO_FAILED;
    }
    if (load->base != 0) {
        /* The file record's length is in its header: the read brought
         * whatever follows the offset, up to a slab. */
        if (source->base == NULL || ref->offset < source->offset ||
            ref->offset - source->offset >= source->length) {
            return VSR_IO_CORRUPT;
        }
        vsr_io_cursor_init_one(&payload,
                               source->base + (ref->offset - source->offset),
                               source->length - (ref->offset - source->offset));
        rc = vsr_io_codec_get_clients_record(&payload, &store->limits, region,
                                             record);
    } else {
        const unsigned char *at =
            load_bytes(store, source, ref->offset, ref->length);
        int32_t status;

        if (at == NULL) {
            return VSR_IO_CORRUPT;
        }
        status = record_payload(at, ref->length, ref->sequence,
                                source->base != NULL, ref->change, &payload);
        if (status != VSR_IO_OK) {
            return status;
        }
        for (uint32_t i = 0; i < ref->index; ++i) {
            struct vsr_io_wire_client_record skipped;

            if (vsr_io_codec_skip_client_record(&payload, &skipped) != VSR_OK) {
                return VSR_IO_CORRUPT;
            }
        }
        rc = vsr_io_codec_get_client_record(&payload, &store->limits, region,
                                            record);
    }
    if (rc != VSR_OK) {
        return rc == VSR_ELIMIT ? VSR_IO_FAILED : VSR_IO_CORRUPT;
    }
    if (!id_equal(record->request.client, load->read.client)) {
        return VSR_IO_CORRUPT;
    }
    loaded->items = record;
    loaded->count = 1;
    return VSR_IO_OK;
}

/* Builds the result of the head load from `source` in a fresh lease and
 * completes it. The lease takes `slab` (a cold read) or, for a hot
 * result, the pin over [begin, end). */
static void load_finish(struct vsr_io_store *store,
                        struct vsr_io_pending_load *load,
                        const struct vsr_io_load_ref *refs,
                        const struct load_source *source, uint32_t slab,
                        uint64_t begin, uint64_t end)
{
    struct vsr_io_replica *replica = store_replica(store);
    uint32_t lease = vsr_io_lease_alloc(replica, slab, NONE);
    struct vsr_io_bump *region;
    struct vsr_loaded *loaded;
    int32_t status = VSR_IO_OK;

    STORE_ASSERT(lease != NONE);
    region = &replica->leases[lease].region;
    loaded =
        vsr_io_bump_alloc(region, sizeof(*loaded), alignof(struct vsr_loaded));
    if (loaded == NULL) {
        status = VSR_IO_FAILED;
    } else {
        memset(loaded, 0, sizeof(*loaded));
        loaded->sequence = load->read.sequence;
        if (load->count == 0) {
            loaded->next = load->read.type == VSR_LOAD_LOG ? load->read.end : 0;
        } else if (load->read.type == VSR_LOAD_CLIENT) {
            status =
                build_client(store, load, &refs[0], source, region, loaded);
        } else {
            status = build_entries(store, load, refs, source, region, loaded);
            if (load->read.type == VSR_LOAD_LOG) {
                loaded->next = load->read.first + loaded->count;
            }
        }
    }
    if (status != VSR_IO_OK) {
        vsr_io_lease_release(replica, lease); /* Drops the slab too. */
        complete(store, load->op, status, NONE, NULL);
        return;
    }
    if (slab == NONE && load->count > 0) {
        pin_range(store, lease, begin, end);
        replica->leases[lease].pin = lease;
    }
    complete(store, load->op, VSR_IO_OK, lease, loaded);
}

/* Answers queued loads from the head while leases are free: a read one,
 * a hot one, an empty one; stops at a cold one, which prepare reads. */
static void loads_drain(struct vsr_io_store *store)
{
    struct vsr_io_replica *replica = store_replica(store);

    while (store->loads_count > 0 && store->state != VSR_IO_STORE_FAILED) {
        struct vsr_io_pending_load *load = loads_at(store, 0);
        struct vsr_io_load_ref *refs = load_refs(store, load);
        struct load_source source = {NULL, 0, 0};
        uint64_t begin = 0;
        uint64_t end = 0;

        if (load->state == LOAD_READING || replica->leases_free == 0) {
            return;
        }
        if (load->state == LOAD_READ) {
            source.base = vsr_io_pool_slab(&replica->io->pool, load->slab);
            source.offset = load->offset;
            source.length = load->got;
            load_finish(store, load, refs, &source, load->slab, 0, 0);
            loads_pop(store, false); /* The lease has the slab. */
            continue;
        }
        if (load->count > 0 && !load_hot(store, load, refs, &begin, &end)) {
            return;
        }
        load_finish(store, load, refs, &source, NONE, begin, end);
        loads_pop(store, false);
    }
}

/* The block-aligned file range covering the records one read brings in
 * one slab: the batch is cut where the range would exceed the slab or
 * leave the segment. A base file record's length is not known: the read
 * takes a slab from its offset and is short at the file's end. */
static void load_range(const struct vsr_io_store *store,
                       struct vsr_io_pending_load *load,
                       const struct vsr_io_load_ref *refs, uint64_t slab_bytes)
{
    uint64_t block = block_bytes(store);
    uint64_t begin = refs[0].offset / block * block;
    uint64_t end = begin;
    uint32_t segment = segment_of(store, refs[0].offset);
    uint32_t n = 0;

    if (load->base != 0) {
        load->offset = begin;
        load->length = slab_bytes;
        return;
    }
    for (; n < load->count; ++n) {
        uint64_t record_end = round_up(refs[n].offset + refs[n].length, block);

        if (n > 0 && (segment_of(store, refs[n].offset) != segment ||
                      record_end - begin > slab_bytes)) {
            break;
        }
        if (record_end > end) {
            end = record_end;
        }
    }
    STORE_ASSERT(n > 0 && end - begin <= slab_bytes);
    load->count = n;
    load->offset = begin;
    load->length = end - begin;
}

/* Issues the cold read of the head load into a slab. */
static bool prepare_load(struct vsr_io *io, uint32_t replica,
                         struct vsr_io_store *store, struct vsr_io_sqe *sqe)
{
    struct vsr_io_pending_load *load;
    struct vsr_io_load_ref *refs;
    uint64_t begin;
    uint64_t end;
    uint32_t slab;
    uint32_t slot;
    int32_t fd;

    if (store->loads_count == 0 || store->cold_active != 0) {
        return false;
    }
    load = loads_at(store, 0);
    refs = load_refs(store, load);
    if (load->state != LOAD_QUEUED || load->count == 0 ||
        load_hot(store, load, refs, &begin, &end)) {
        return false; /* Answered from poll. */
    }
    fd = load->base != 0 ? store->base_slot : store->log_slot;
    if (fd < 0) {
        complete(store, load->op, VSR_IO_FAILED, NONE, NULL);
        loads_pop(store, true);
        return false;
    }
    slab = vsr_io_pool_acquire(&io->pool, false);
    if (slab == NONE) {
        return false;
    }
    slot = take_slot(io, VSR_IO_SLOT_LOAD, replica, 0, 0);
    if (slot == NONE) {
        vsr_io_pool_release(&io->pool, slab);
        return false;
    }
    load_range(store, load, refs, io->pool.slab_bytes);
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode = VSR_IO_SQE_READ;
    sqe->flags = VSR_IO_SQE_FIXED_FILE | VSR_IO_SQE_FIXED_BUFFER;
    sqe->fd = fd;
    sqe->user_data = vsr_io_slots_user_data(&io->slots, slot);
    sqe->buffer_index = (uint16_t)io->pool.region_index;
    sqe->addr = vsr_io_pool_slab(&io->pool, slab);
    sqe->length = (uint32_t)load->length;
    sqe->offset = load->offset;
    load->state = LOAD_READING;
    load->slab = slab;
    load->slot = slot;
    store->cold_active = 1;
    store->cold_slab = slab;
    return true;
}

/* The cold read completed: the head load decodes once a lease is free.
 * When the store was fenced meanwhile the load is gone and the slab is
 * only now safe to release. */
static void load_done(struct vsr_io_store *store, int32_t result)
{
    struct vsr_io_pending_load *load;

    store->cold_active = 0;
    if (store->loads_count == 0 || loads_at(store, 0)->state != LOAD_READING) {
        vsr_io_pool_release(&store_replica(store)->io->pool, store->cold_slab);
        return;
    }
    load = loads_at(store, 0);
    load->slot = NONE;
    if (result < 0) {
        complete(store, load->op, VSR_IO_FAILED, NONE, NULL);
        loads_pop(store, true);
        return;
    }
    load->got = (uint64_t)result;
    load->state = LOAD_READ;
}

int vsr_io_store_load(struct vsr_io_store *store, uint64_t op,
                      const struct vsr_store_read *read)
{
    struct vsr_io_pending_load *load;
    int32_t status;

    if (store == NULL || op == 0 || read == NULL) {
        return VSR_EINVAL;
    }
    if (store->state == VSR_IO_STORE_FAILED) {
        complete(store, op, VSR_IO_FAILED, NONE, NULL);
        return VSR_OK;
    }
    if ((store->state != VSR_IO_STORE_READY &&
         store->state != VSR_IO_STORE_CREATING) ||
        read->type == VSR_LOAD_RECOVERY || read->type > VSR_LOAD_REQUEST ||
        read->sequence > store->readable || read->sequence < store->reclaim ||
        read->max_count == 0 ||
        store->loads_count == store->limits.operations) {
        return VSR_EINVAL;
    }
    if (read->type == VSR_LOAD_LOG &&
        (read->first > read->end ||
         read->max_count > store->limits.batch_entries)) {
        return VSR_EINVAL;
    }
    load = loads_at(store, store->loads_count);
    store->loads_count++;
    memset(load, 0, sizeof(*load));
    load->op = op;
    load->read = *read;
    load->state = LOAD_QUEUED;
    load->slab = NONE;
    load->slot = NONE;
    switch (read->type) {
    case VSR_LOAD_LOG:
        status = resolve_log(store, load, load_refs(store, load));
        break;
    case VSR_LOAD_CLIENT:
        status = resolve_client(store, load, load_refs(store, load));
        break;
    default:
        status = resolve_request(store, load, load_refs(store, load));
        break;
    }
    if (status != VSR_IO_OK) {
        store->loads_count--; /* The newest: dropped from the tail. */
        complete(store, op, status, NONE, NULL);
        return VSR_OK;
    }
    loads_drain(store);
    return VSR_OK;
}

/* -------------------------------------------------------------------------
 * Recovery (section 6.4)
 *
 * An existing log is recovered one executor record at a time: STATX for
 * the size, the two superblock blocks, every slot's header, then the
 * records of the start segment and its successors (decision 48) in
 * slab-sized chunks replayed through index_apply, the anchor's clients
 * file through the snapshot module (base_wanted), a flush, the superblock
 * naming run + 1 and the floor, a flush of it, and the RECOVERY load's
 * completion. The scan judges every range twice before ending at it: a
 * bad or short range is read once more, and a scan that ends below the
 * durable floor F is CORRUPT (decision 50). Writing resumes at the block
 * after the last valid record, in the segment holding it: no block below
 * the recovered prefix is ever rewritten.
 * ---------------------------------------------------------------------- */

static bool prepare_superblock(struct vsr_io *io, uint32_t replica,
                               struct vsr_io_store *store,
                               struct vsr_io_sqe *sqe);

/* Deep copies of the logical state into a region: the recovered row must
 * stay valid under its lease while later records replace the store's
 * copies, and the start header's state moves from scratch into the state
 * copies. NULL when the region is exhausted (impossible when sized). */
static const struct vsr_membership *
copy_membership(struct vsr_io_bump *region, const struct vsr_membership *in)
{
    struct vsr_membership *out;
    struct vsr_member *members = NULL;

    if (in == NULL) {
        return NULL;
    }
    out =
        vsr_io_bump_alloc(region, sizeof(*out), alignof(struct vsr_membership));
    if (out == NULL) {
        return NULL;
    }
    *out = *in;
    if (in->count > 0) {
        members =
            vsr_io_bump_alloc(region, (size_t)in->count * sizeof(*members),
                              alignof(struct vsr_member));
        if (members == NULL) {
            return NULL;
        }
        memcpy(members, in->members, (size_t)in->count * sizeof(*members));
    }
    out->members = members;
    return out;
}

static bool copy_epoch(struct vsr_io_bump *region, const struct vsr_epoch *in,
                       const struct vsr_epoch **out)
{
    struct vsr_epoch *epoch;

    *out = NULL;
    if (in == NULL) {
        return true;
    }
    epoch =
        vsr_io_bump_alloc(region, sizeof(*epoch), alignof(struct vsr_epoch));
    if (epoch == NULL) {
        return false;
    }
    *epoch = *in;
    epoch->current = copy_membership(region, in->current);
    epoch->previous = copy_membership(region, in->previous);
    if ((in->current != NULL && epoch->current == NULL) ||
        (in->previous != NULL && epoch->previous == NULL)) {
        return false;
    }
    *out = epoch;
    return true;
}

static bool copy_hard(struct vsr_io_bump *region,
                      const struct vsr_hard_state *in,
                      struct vsr_hard_state *out)
{
    *out = *in;
    return copy_epoch(region, in->epoch, &out->epoch);
}

/* The manifest bytes are copied too: they come from a record or a header
 * in a slab or the ring, which do not last. */
static bool copy_checkpoint(struct vsr_io_bump *region,
                            const struct vsr_checkpoint *in,
                            struct vsr_checkpoint *out)
{
    *out = *in;
    if (!copy_epoch(region, in->epoch, &out->epoch)) {
        return false;
    }
    out->manifest.spans = NULL;
    out->manifest.count = 0;
    if (in->manifest.size > 0) {
        size_t size = (size_t)in->manifest.size;
        unsigned char *bytes = vsr_io_bump_alloc(region, size, 1);
        struct vsr_span *span =
            vsr_io_bump_alloc(region, sizeof(*span), alignof(struct vsr_span));
        size_t at = 0;

        if (bytes == NULL || span == NULL) {
            return false;
        }
        for (uint32_t i = 0; i < in->manifest.count; ++i) {
            memcpy(bytes + at, in->manifest.spans[i].data,
                   in->manifest.spans[i].size);
            at += in->manifest.spans[i].size;
        }
        span->data = bytes;
        span->size = size;
        out->manifest.spans = span;
        out->manifest.count = 1;
    }
    return true;
}

/* Scratch region for header decoding: the ring, unused until the scan is
 * over. */
static void recovery_scratch(struct vsr_io_store *store,
                             struct vsr_io_bump *region)
{
    vsr_io_bump_init(region, store->ring, (size_t)store->ring_size);
}

static void recovery_release_slab(struct vsr_io_store *store)
{
    if (store->recovery.slab != NONE) {
        vsr_io_pool_release(&store_replica(store)->io->pool,
                            store->recovery.slab);
        store->recovery.slab = NONE;
    }
}

/* Ends the recovery with `status` on the RECOVERY load and fences. A read
 * in flight keeps the slab until its completion. */
static void recovery_fail(struct vsr_io_store *store, int32_t status)
{
    if (store->recovery.io_slot == NONE) {
        recovery_release_slab(store);
    }
    store->recovery.stage = RECOVERY_NONE;
    store_fail(store, status);
}

/* Step 2: the newer valid superblock; geometry and the file size. */
static void recovery_superblocks(struct vsr_io_store *store, int32_t result)
{
    struct vsr_io_recovery *r = &store->recovery;
    struct vsr_io_wire_superblock superblock;
    uint32_t chosen = NONE;
    uint32_t slots;

    memset(&superblock, 0, sizeof(superblock));
    if (result < 0 ||
        (uint64_t)result != STORE_SUPERBLOCKS * block_bytes(store)) {
        if (r->retried == 0) {
            r->retried = 1; /* Read once more (decision 50). */
            return;
        }
        recovery_fail(store, VSR_IO_CORRUPT);
        return;
    }
    for (uint32_t copy = 0; copy < STORE_SUPERBLOCKS; ++copy) {
        struct vsr_io_wire_superblock candidate;

        if (vsr_io_codec_get_superblock(
                store->superblocks + copy * block_bytes(store),
                store->options.block_bytes, &candidate) != VSR_OK) {
            continue;
        }
        if (chosen == NONE || candidate.revision > superblock.revision) {
            superblock = candidate;
            chosen = copy;
        }
    }
    slots = file_slots(store);
    if (chosen == NONE ||
        superblock.block_bytes != store->options.block_bytes ||
        superblock.segment_bytes != store->options.segment_bytes ||
        superblock.header_blocks != store->header_blocks ||
        superblock.start_segment == 0 || superblock.generation == 0 ||
        superblock.slots == 0 || slots < superblock.slots ||
        superblock.start_slot >= slots) {
        recovery_fail(store, VSR_IO_CORRUPT);
        return;
    }
    if (slots > store->options.max_segments) {
        store->error = -ENOSPC; /* More slots than the table holds. */
        recovery_fail(store, VSR_IO_FAILED);
        return;
    }
    r->copy = chosen;
    r->durable_floor = superblock.durable_floor;
    store->generation = superblock.generation;
    store->superblock_revision = superblock.revision;
    store->superblock_next = (chosen + 1) % STORE_SUPERBLOCKS;
    store->superblock_floor = superblock.durable_floor;
    store->start_segment = superblock.start_segment;
    store->start_slot = superblock.start_slot;
    store->slots = slots; /* The file's, which a crashed growth may have
                             made larger than the superblock's. */
    store->run = superblock.run + 1;
    r->slot = 0;
    r->retried = 0;
    r->stage = RECOVERY_HEADERS;
}

/* Step 4: the start segment's state into the store. */
static int32_t recovery_install(struct vsr_io_store *store,
                                const struct vsr_io_wire_segment *fixed,
                                const struct vsr_store_identity *identity,
                                const struct vsr_hard_state *hard,
                                const struct vsr_checkpoint *checkpoint)
{
    struct vsr_io_recovery *r = &store->recovery;
    struct vsr_io_bump hard_region;
    struct vsr_io_bump anchor_region;
    bool state = (fixed->flags & VSR_IO_SEGMENT_STATE) != 0;

    state_regions(store, &hard_region, &anchor_region);
    memset(&store->hard, 0, sizeof(store->hard));
    memset(&store->anchor, 0, sizeof(store->anchor));
    store->identity_set = state;
    if (state) {
        store->identity = *identity;
        if (!copy_hard(&hard_region, hard, &store->hard)) {
            return VSR_IO_FAILED;
        }
    }
    if (checkpoint != NULL &&
        !copy_checkpoint(&anchor_region, checkpoint, &store->anchor)) {
        return VSR_IO_FAILED;
    }
    store->log_begin = fixed->log_begin;
    store->log_end = fixed->log_end;
    store->retained_begin = fixed->log_begin;
    store->client_base = fixed->client_base;
    store->client_base_id = store->anchor.id;
    r->sequence = fixed->last_sequence;
    r->run = fixed->run;
    r->last_slot = store->start_slot;
    r->resume = slot_offset(store, store->start_slot) + store->header_bytes;
    return VSR_IO_OK;
}

/* Step 3: one slot's header; a valid one is a candidate segment, the
 * start slot must hold the start segment. */
static void recovery_header(struct vsr_io_store *store, int32_t result)
{
    struct vsr_io_recovery *r = &store->recovery;
    struct vsr_io_segment *segment = &store->segments[r->slot];
    struct vsr_io_wire_segment fixed;
    struct vsr_store_identity identity;
    struct vsr_hard_state hard;
    struct vsr_checkpoint *checkpoint = NULL;
    struct vsr_io_bump scratch;
    bool valid;

    if ((result < 0 || (uint64_t)result != store->header_bytes) &&
        r->retried == 0) {
        r->retried = 1; /* Read once more (decision 50). */
        return;
    }
    recovery_scratch(store, &scratch);
    memset(segment, 0, sizeof(*segment));
    segment->header_write = NONE;
    valid = result >= 0 && (uint64_t)result == store->header_bytes &&
            vsr_io_codec_get_segment(
                vsr_io_pool_slab(&store_replica(store)->io->pool, r->slab),
                (size_t)store->header_bytes, &store->limits, &scratch, &fixed,
                &identity, &hard, &checkpoint) == VSR_OK &&
            fixed.generation == store->generation && fixed.segment != 0;
    if (valid) {
        segment->number = fixed.segment;
        segment->first_sequence = fixed.last_sequence + 1;
        segment->last_sequence = fixed.last_sequence;
        segment->used = store->header_bytes;
        segment->run = fixed.run;
        segment->phase = VSR_IO_SEGMENT_FREE; /* A candidate until visited. */
        if (fixed.durable_floor > r->durable_floor) {
            r->durable_floor = fixed.durable_floor;
        }
        if (fixed.segment >= store->next_segment) {
            store->next_segment = fixed.segment + 1;
        }
    }
    if (r->slot == store->start_slot) {
        int32_t status;

        if (!valid || fixed.segment != store->start_segment) {
            recovery_fail(store, VSR_IO_CORRUPT);
            return;
        }
        status = recovery_install(store, &fixed, &identity, &hard, checkpoint);
        if (status != VSR_IO_OK) {
            recovery_fail(store, status);
            return;
        }
    }
    r->slot++;
    r->retried = 0;
    if (r->slot == store->slots) {
        /* Step 5 starts in the start segment. */
        r->slot = store->start_slot;
        store->segments[r->slot].phase = VSR_IO_SEGMENT_SEALED;
        r->mode = SCAN_CHAIN;
        r->chain_resume = 0;
        r->offset = slot_offset(store, r->slot) + store->header_bytes;
        r->position = r->offset;
        r->stage = RECOVERY_SCAN;
    }
}

/* Step 8's state: the log resumes after the valid prefix. */
static void recovery_scan_end(struct vsr_io_store *store)
{
    struct vsr_io_recovery *r = &store->recovery;
    struct vsr_io_wire_superblock superblock;
    int rc;

    recovery_release_slab(store);
    if (r->sequence < r->durable_floor) {
        recovery_fail(store, VSR_IO_CORRUPT); /* Decision 50. */
        return;
    }
    rc = vsr_io_codec_get_superblock(store->superblocks +
                                         r->copy * block_bytes(store),
                                     store->options.block_bytes, &superblock);
    STORE_ASSERT(rc == VSR_OK);
    (void)rc;
    if (r->sequence > 0) {
        /* A log with records has its identity and hard state from
         * transaction 1; the superblock, once it carries them, agrees. */
        if (!store->identity_set || store->hard.epoch == NULL ||
            ((superblock.cluster_hi != 0 || superblock.cluster_lo != 0 ||
              superblock.replica != 0) &&
             (superblock.cluster_hi != store->identity.cluster.hi ||
              superblock.cluster_lo != store->identity.cluster.lo ||
              superblock.replica != store->identity.replica ||
              superblock.durability != store->identity.durability))) {
            recovery_fail(store, VSR_IO_CORRUPT);
            return;
        }
    }
    /* Stale headers (never visited) and abandoned successors (visited
     * after the last record, so still naming it) free their slots
     * (decision 48); the rest of the chain is SEALED but for the segment
     * holding the last record, which is OPEN at the block after it. */
    for (uint32_t slot = 0; slot < store->slots; ++slot) {
        struct vsr_io_segment *segment = &store->segments[slot];

        if (segment->number != 0 && slot != r->last_slot &&
            (segment->phase == VSR_IO_SEGMENT_FREE ||
             segment->last_sequence >= r->sequence)) {
            memset(segment, 0, sizeof(*segment));
            segment->header_write = NONE;
        }
    }
    store->segments[r->last_slot].phase = VSR_IO_SEGMENT_OPEN;
    store->segments[r->last_slot].last_sequence = r->sequence;
    store->segments[r->last_slot].used =
        r->resume - slot_offset(store, r->last_slot);
    store->current = r->last_slot;
    store->readable = r->sequence;
    store->written = r->sequence;
    store->flushed = r->sequence;
    store->durable = r->sequence;
    store->reclaim = 0;
    store->reclaimed = 0;
    store->client_base_floor = store->client_base;
    store->client_base_pending = 0;
    store->packed_since_durable = 0;
    store->head = 0;
    store->issued = 0;
    store->retained_floor = 0;
    store->file_head = r->resume;
    extent_begin(store, r->last_slot, 0);
    if (r->sequence > 0 && store->hard.role == VSR_MEMBER_FULL &&
        !id_zero(store->anchor.id)) {
        /* Step 7: the snapshot module loads the anchor's file. */
        store->base_state = BASE_WANTED;
        store->base_kind = BASE_RECOVERY;
        store->base_op = 0;
        store->base_id = store->anchor.id;
        store->base_sequence = store->client_base;
        r->stage = RECOVERY_BASE;
        return;
    }
    r->stage = RECOVERY_FLUSH;
}

static void recovery_scan_end(struct vsr_io_store *store);

/* The sweep of the slots the chain never visited (decision 50), or the
 * end of the scan once every slot was seen. */
static void recovery_rest(struct vsr_io_store *store)
{
    struct vsr_io_recovery *r = &store->recovery;
    uint32_t slot = r->mode == SCAN_REST ? r->slot + 1 : 0;

    while (slot < store->slots &&
           store->segments[slot].phase == VSR_IO_SEGMENT_SEALED) {
        slot++; /* Visited: its records were counted or swept. */
    }
    if (slot == store->slots) {
        recovery_scan_end(store);
        return;
    }
    r->mode = SCAN_REST;
    r->chain_resume = 0;
    r->slot = slot;
    r->offset = slot_offset(store, slot) + store->header_bytes;
    r->position = r->offset;
    r->retried = 0;
}

/* Step 6: the scan of the current segment ended at `sequence`; the
 * successor is the candidate naming it with the greatest number and a
 * run at or above the current one, else the chain is over. */
static void recovery_successor(struct vsr_io_store *store)
{
    struct vsr_io_recovery *r = &store->recovery;
    uint32_t best = NONE;

    for (uint32_t slot = 0; slot < store->slots; ++slot) {
        const struct vsr_io_segment *segment = &store->segments[slot];

        if (segment->number != 0 && segment->phase == VSR_IO_SEGMENT_FREE &&
            segment->last_sequence == r->sequence && segment->run >= r->run &&
            (best == NONE || segment->number > store->segments[best].number)) {
            best = slot;
        }
    }
    if (best == NONE) {
        recovery_rest(store);
        return;
    }
    store->segments[best].phase = VSR_IO_SEGMENT_SEALED;
    if (store->segments[best].run > r->run) {
        r->run = store->segments[best].run;
    }
    r->mode = SCAN_CHAIN;
    r->chain_resume = 0;
    r->slot = best;
    r->offset = slot_offset(store, best) + store->header_bytes;
    r->position = r->offset;
    r->retried = 0;
}

/* A range at `at` that reads bad, short or foreign while replaying: read
 * once more before it is judged (true: a re-read is due); the second
 * verdict ends the chain in this slot, which is swept from `at` on. A
 * verdict inside a block ends only that block: the bytes after the last
 * valid record of a block are dead once writing resumed at the block
 * after it (decision 89), so the chain resumes there once the dead tail
 * is swept (decision S14). */
static bool recovery_judge(struct vsr_io_store *store, uint64_t at)
{
    struct vsr_io_recovery *r = &store->recovery;
    uint64_t block = block_bytes(store);

    if (r->mode != SCAN_CHAIN) {
        return false;
    }
    if (r->retried == 0 || r->retry_offset != at) {
        r->retried = 1;
        r->retry_offset = at;
        r->offset = at / block * block;
        r->position = at;
        return true;
    }
    r->mode = SCAN_SWEEP;
    r->chain_resume = at % block != 0 ? round_up(at, block) : 0;
    return false;
}

/* The slot's data is exhausted. */
static void recovery_exhausted(struct vsr_io_store *store)
{
    if (store->recovery.mode == SCAN_REST) {
        recovery_rest(store);
    } else {
        recovery_successor(store);
    }
}

/* Step 5: judges and replays the records of the chunk in the slab, or
 * sweeps it for floors. */
static void recovery_chunk(struct vsr_io_store *store, int32_t result)
{
    struct vsr_io_recovery *r = &store->recovery;
    const unsigned char *base =
        vsr_io_pool_slab(&store_replica(store)->io->pool, r->slab);
    uint64_t block = block_bytes(store);
    uint64_t data_end =
        slot_offset(store, r->slot) + store->options.segment_bytes;
    uint64_t chunk;

    if (result < 0) {
        if (recovery_judge(store, r->position)) {
            return;
        }
        /* Sweeping: a range that cannot be read carries no floor. */
        r->position = round_up(r->position + 1, block);
        r->offset = r->position;
        if (r->position >= data_end) {
            recovery_exhausted(store);
        }
        return;
    }
    chunk = (uint64_t)result;
    for (;;) {
        struct vsr_io_cursor cursor;
        struct vsr_io_wire_record header;
        uint64_t at;
        uint64_t p;
        uint64_t remaining;
        bool more;
        bool valid = false;
        uint32_t kind = 0;
        int32_t status;

        if (r->mode == SCAN_SWEEP && r->chain_resume != 0 &&
            r->position >= r->chain_resume) {
            /* The dead tail is swept (a stale record straddling its end
             * counted; its remainder is no header): the chain resumes
             * at the block after it (decision S14). */
            r->position = r->chain_resume;
            r->chain_resume = 0;
            r->mode = SCAN_CHAIN;
            r->retried = 0;
        }
        at = r->position;
        p = at - r->offset;
        remaining = p < chunk ? chunk - p : 0;
        /* A record straddling the chunk's end is read again from its
         * block, when that makes progress and the segment has the
         * bytes (SCAN_END covers a short header too). */
        more = r->offset + chunk < data_end && at / block * block > r->offset;
        if (at >= data_end) {
            recovery_exhausted(store);
            return;
        }
        if (remaining == 0 || remaining < sizeof(header)) {
            if (more) {
                r->offset = at / block * block;
                r->position = at;
                return;
            }
        }
        if (remaining == 0) {
            if (recovery_judge(store, at)) {
                return;
            }
            r->position = round_up(at + 1, block); /* Sweep: next block. */
            continue;
        }
        vsr_io_cursor_init_one(&cursor, base + p, (size_t)remaining);
        if (vsr_io_codec_get_record(&cursor, store->max_record_bytes, &header,
                                    &kind) == VSR_OK) {
            if (kind == VSR_IO_SCAN_PAD) {
                /* A PAD's length is not CRC-covered: it runs exactly to
                 * the block's end. */
                valid = header.length == block - at % block &&
                        header.length <= remaining;
            } else if (kind == VSR_IO_SCAN_RECORD) {
                if (header.length > remaining) {
                    if (more) {
                        r->offset = at / block * block;
                        r->position = at;
                        return;
                    }
                } else {
                    valid = vsr_io_codec_check_record(&cursor, &header) &&
                            header.generation == store->generation;
                }
            }
        }
        if (valid && kind == VSR_IO_SCAN_RECORD &&
            header.flushed > r->durable_floor) {
            r->durable_floor = header.flushed; /* Decision 50. */
        }
        if (valid && kind == VSR_IO_SCAN_RECORD && r->mode == SCAN_CHAIN &&
            (header.sequence != r->sequence + 1 || header.run < r->run)) {
            valid = false; /* Not the next record: stale or torn. */
        }
        if (!valid) {
            if (recovery_judge(store, at)) {
                return;
            }
            /* Sweeping: PAD and record headers are tried at every
             * aligned position, so a record behind a bad range counts. */
            r->position = at + VSR_IO_WIRE_ALIGN;
            continue;
        }
        if (kind == VSR_IO_SCAN_PAD) {
            r->position = at + header.length;
            continue;
        }
        if (r->mode == SCAN_CHAIN) {
            if (!index_apply(store, at, header.length, base + p, &status)) {
                recovery_fail(store, status); /* Valid bytes, wrong content. */
                return;
            }
            r->sequence = header.sequence;
            r->run = header.run;
            r->last_slot = r->slot;
            r->resume = round_up(at + header.length, block);
            store->segments[r->slot].last_sequence = header.sequence;
            store->segments[r->slot].used =
                r->resume - slot_offset(store, r->slot);
        }
        r->position = at + header.length;
    }
}

/* A recovery read completed. The store fenced meanwhile only releases the
 * slab. */
static void recovery_read_done(struct vsr_io_store *store, int32_t result)
{
    struct vsr_io_recovery *r = &store->recovery;

    r->io_slot = NONE;
    if (store->state != VSR_IO_STORE_RECOVERING) {
        recovery_release_slab(store);
        return;
    }
    switch (r->stage) {
    case RECOVERY_SUPERBLOCKS:
        recovery_superblocks(store, result);
        break;
    case RECOVERY_HEADERS:
        recovery_header(store, result);
        break;
    case RECOVERY_SCAN:
        recovery_chunk(store, result);
        break;
    default:
        STORE_ASSERT(false);
        break;
    }
}

/* Step 7 ended: the snapshot module loaded the anchor's file, which
 * base_end applied; the retained index is rebuilt over the table. */
static void recovery_base_done(struct vsr_io_store *store, int32_t status)
{
    if (status != VSR_IO_OK) {
        recovery_fail(store, status);
        return;
    }
    status = retained_rebuild(store);
    if (status != VSR_IO_OK) {
        recovery_fail(store, status);
        return;
    }
    store->recovery.stage = RECOVERY_FLUSH;
}

/* Step 8: READY, and the RECOVERY load completes with the row (in a
 * lease region) or NOT_FOUND for a log without records (decision 71). */
static void recovery_finish(struct vsr_io_store *store)
{
    struct vsr_io_recovery *r = &store->recovery;
    struct vsr_io_replica *replica = store_replica(store);
    struct vsr_io_bump *region;
    struct vsr_loaded *loaded = NULL;
    struct vsr_recovered *row = NULL;
    struct vsr_checkpoint *checkpoint = NULL;
    uint32_t lease;
    bool ok;

    r->stage = RECOVERY_NONE;
    store->state = VSR_IO_STORE_READY;
    if (r->load_op == 0) {
        return;
    }
    if (r->sequence == 0) {
        complete(store, r->load_op, VSR_IO_NOT_FOUND, NONE, NULL);
        r->load_op = 0;
        return;
    }
    lease = vsr_io_lease_alloc(replica, NONE, NONE);
    if (lease == NONE) {
        store_fail(store, VSR_IO_FAILED); /* Every lease is free at open. */
        return;
    }
    region = &replica->leases[lease].region;
    loaded =
        vsr_io_bump_alloc(region, sizeof(*loaded), alignof(struct vsr_loaded));
    row =
        vsr_io_bump_alloc(region, sizeof(*row), alignof(struct vsr_recovered));
    ok = loaded != NULL && row != NULL;
    if (ok) {
        memset(loaded, 0, sizeof(*loaded));
        memset(row, 0, sizeof(*row));
        row->identity = store->identity;
        row->sequence = r->sequence;
        row->log_begin = store->log_begin;
        row->log_end = store->log_end;
        ok = copy_hard(region, &store->hard, &row->hard);
    }
    if (ok && !id_zero(store->anchor.id)) {
        checkpoint = vsr_io_bump_alloc(region, sizeof(*checkpoint),
                                       alignof(struct vsr_checkpoint));
        ok = checkpoint != NULL &&
             copy_checkpoint(region, &store->anchor, checkpoint);
        row->checkpoint = checkpoint;
    }
    if (!ok) {
        vsr_io_lease_release(replica, lease);
        store_fail(store, VSR_IO_FAILED); /* Sized by load_region. */
        return;
    }
    loaded->items = row;
    loaded->sequence = r->sequence;
    loaded->count = 1;
    complete(store, r->load_op, VSR_IO_OK, lease, loaded);
    r->load_op = 0;
}

/* Issues one recovery READ of the log. */
static bool recovery_read(struct vsr_io *io, uint32_t replica,
                          struct vsr_io_store *store, struct vsr_io_sqe *sqe,
                          uint64_t offset, uint64_t length, void *addr,
                          uint32_t buffer_index)
{
    uint32_t slot = take_slot(io, VSR_IO_SLOT_LOAD, replica, RECOVERY_SUB, 0);

    if (slot == NONE) {
        return false;
    }
    log_sqe(store, sqe, VSR_IO_SQE_READ,
            vsr_io_slots_user_data(&io->slots, slot));
    sqe->flags |= VSR_IO_SQE_FIXED_BUFFER;
    sqe->buffer_index = (uint16_t)buffer_index;
    sqe->addr = addr;
    sqe->length = (uint32_t)length;
    sqe->offset = offset;
    store->recovery.io_slot = slot;
    return true;
}

/* Issues a recovery read into the slab, taken when needed. */
static bool recovery_read_slab(struct vsr_io *io, uint32_t replica,
                               struct vsr_io_store *store,
                               struct vsr_io_sqe *sqe, uint64_t offset,
                               uint64_t length)
{
    if (store->recovery.slab == NONE) {
        store->recovery.slab = vsr_io_pool_acquire(&io->pool, false);
        if (store->recovery.slab == NONE) {
            return false; /* A slab frees eventually (the reserve). */
        }
    }
    return recovery_read(io, replica, store, sqe, offset, length,
                         vsr_io_pool_slab(&io->pool, store->recovery.slab),
                         io->pool.region_index);
}

/* The flush of a recovery step in FDATASYNC mode; DSYNC needs none. */
static bool recovery_flush(struct vsr_io *io, uint32_t replica,
                           struct vsr_io_store *store, struct vsr_io_sqe *sqe)
{
    uint32_t slot = take_slot(io, VSR_IO_SLOT_FLUSH, replica, 0, 0);

    if (slot == NONE) {
        return false;
    }
    log_sqe(store, sqe, VSR_IO_SQE_FSYNC,
            vsr_io_slots_user_data(&io->slots, slot));
    sqe->op_flags = VSR_IO_FSYNC_DATASYNC;
    store->flush_slot = slot;
    io->stats.flushes++;
    return true;
}

/* One recovery step: advances through the stages needing no I/O and
 * issues at most one record; false while a completion is awaited. */
static bool prepare_recovery(struct vsr_io *io, uint32_t replica,
                             struct vsr_io_store *store, struct vsr_io_sqe *sqe)
{
    struct vsr_io_recovery *r = &store->recovery;
    uint64_t chunk;

    for (;;) {
        if (r->io_slot != NONE || store->file_op != FILE_NONE ||
            store->flush_slot != NONE || store->superblock_pending != 0) {
            return false;
        }
        if (store->error != 0) {
            recovery_fail(store, VSR_IO_FAILED); /* A flush or write failed. */
            return false;
        }
        switch (r->stage) {
        case RECOVERY_STAT: {
            uint32_t slot =
                take_slot(io, VSR_IO_SLOT_FILE, replica, FILE_STAT, 0);

            if (slot == NONE) {
                return false;
            }
            memset(sqe, 0, sizeof(*sqe));
            sqe->opcode = VSR_IO_SQE_STATX;
            sqe->fd = store->dir_fd;
            sqe->addr = store->log_path;
            sqe->addr2 = store->ring; /* Scratch for the struct statx. */
            sqe->length = STATX_SIZE;
            sqe->user_data = vsr_io_slots_user_data(&io->slots, slot);
            store->file_op = FILE_STAT;
            return true;
        }
        case RECOVERY_SUPERBLOCKS:
            return recovery_read(io, replica, store, sqe, 0,
                                 STORE_SUPERBLOCKS * block_bytes(store),
                                 store->superblocks, store->region_index);
        case RECOVERY_HEADERS:
            return recovery_read_slab(io, replica, store, sqe,
                                      slot_offset(store, r->slot),
                                      store->header_bytes);
        case RECOVERY_SCAN:
            chunk =
                io->pool.slab_bytes / block_bytes(store) * block_bytes(store);
            if (chunk > slot_offset(store, r->slot) +
                            store->options.segment_bytes - r->offset) {
                chunk = slot_offset(store, r->slot) +
                        store->options.segment_bytes - r->offset;
            }
            return recovery_read_slab(io, replica, store, sqe, r->offset,
                                      chunk);
        case RECOVERY_BASE:
            return false; /* base_resume advances. */
        case RECOVERY_FLUSH:
        case RECOVERY_FLUSH_AGAIN:
            if (store->options.sync_mode != VSR_IO_SYNC_FDATASYNC) {
                r->stage = r->stage == RECOVERY_FLUSH ? RECOVERY_SUPERBLOCK
                                                      : RECOVERY_FINISH;
                continue;
            }
            if (!recovery_flush(io, replica, store, sqe)) {
                return false;
            }
            r->stage = r->stage == RECOVERY_FLUSH ? RECOVERY_SUPERBLOCK
                                                  : RECOVERY_FINISH;
            return true;
        case RECOVERY_SUPERBLOCK:
            store->superblock_dirty = 1;
            if (!prepare_superblock(io, replica, store, sqe)) {
                return false;
            }
            r->stage = RECOVERY_FLUSH_AGAIN;
            return true;
        case RECOVERY_FINISH:
            recovery_finish(store);
            return false;
        default:
            return false;
        }
    }
}

/* -------------------------------------------------------------------------
 * Open, poll, prepare, complete
 * ---------------------------------------------------------------------- */

void vsr_io_store_open(struct vsr_io_store *store, uint32_t start_mode,
                       uint64_t load_op, const struct vsr_store_read *read)
{
    (void)read;
    if (store == NULL || store->state != VSR_IO_STORE_CLOSED) {
        return;
    }
    store->start_mode = start_mode;
    store->recovery.load_op = load_op;
    store->state = VSR_IO_STORE_OPENING;
    store->run = 1;
}

/* One creation step, derived from the state (section 6.4 step 1): the
 * FALLOCATEs one slot at a time, both superblocks, then the first
 * segment's header through the write planner. */
static bool prepare_create(struct vsr_io *io, uint32_t replica,
                           struct vsr_io_store *store, struct vsr_io_sqe *sqe)
{
    uint64_t target =
        STORE_SUPERBLOCKS * block_bytes(store) +
        (uint64_t)store->options.segments * store->options.segment_bytes;
    uint32_t slot;

    if (store->file_op != FILE_NONE || store->superblock_pending != 0) {
        return false;
    }
    if (store->log_slot < 0) {
        slot = take_slot(io, VSR_IO_SLOT_FILE, replica, FILE_CREATE, 0);
        if (slot == NONE) {
            return false;
        }
        memset(sqe, 0, sizeof(*sqe));
        sqe->opcode = VSR_IO_SQE_OPENAT;
        sqe->flags = VSR_IO_SQE_DIRECT;
        sqe->fd = store->dir_fd;
        sqe->fd2 = (int32_t)store->file_slot;
        sqe->addr = store->log_path;
        sqe->length = STORE_FILE_MODE;
        sqe->op_flags =
            (uint32_t)(O_RDWR | O_CREAT | O_EXCL) |
            (store->options.direct_io != 0 ? (uint32_t)O_DIRECT : 0u) |
            (store->options.sync_mode == VSR_IO_SYNC_DSYNC ? (uint32_t)O_DSYNC
                                                           : 0u);
        sqe->user_data = vsr_io_slots_user_data(&io->slots, slot);
        store->file_op = FILE_CREATE;
        return true;
    }
    if (store->file_size < target) {
        uint64_t chunk = store->options.segment_bytes;

        if (store->file_size == 0) {
            chunk += STORE_SUPERBLOCKS * block_bytes(store);
        }
        slot = take_slot(io, VSR_IO_SLOT_FILE, replica, FILE_ALLOCATE, chunk);
        if (slot == NONE) {
            return false;
        }
        log_sqe(store, sqe, VSR_IO_SQE_FALLOCATE,
                vsr_io_slots_user_data(&io->slots, slot));
        sqe->offset = store->file_size;
        sqe->length = (uint32_t)chunk;
        store->file_op = FILE_ALLOCATE;
        return true;
    }
    if (store->superblock_revision == 0) {
        struct vsr_io_wire_superblock superblock;

        while (store->generation == 0) {
            vsr_io_engine_random(io, &store->generation,
                                 sizeof(store->generation));
        }
        slot = take_slot(io, VSR_IO_SLOT_SUPER, replica, 0, 0);
        if (slot == NONE) {
            return false;
        }
        store->start_segment = 1;
        store->start_slot = 0;
        store->slots = file_slots(store);
        store->superblock_revision = 1;
        superblock_fill(store, &superblock, 1, store->slots, 0);
        for (uint32_t copy = 0; copy < STORE_SUPERBLOCKS; ++copy) {
            vsr_io_codec_put_superblock(
                &superblock, store->superblocks + copy * block_bytes(store),
                store->options.block_bytes);
        }
        log_sqe(store, sqe, VSR_IO_SQE_WRITE,
                vsr_io_slots_user_data(&io->slots, slot));
        sqe->flags |= VSR_IO_SQE_FIXED_BUFFER;
        sqe->buffer_index = (uint16_t)store->region_index;
        sqe->addr = store->superblocks;
        sqe->length = (uint32_t)(STORE_SUPERBLOCKS * block_bytes(store));
        sqe->offset = 0;
        store->superblock_pending = 1;
        store->superblock_next = 0;
        store->superblock_slot = slot;
        return true;
    }
    if (store->current == NONE) {
        segment_open(store, 0);
    }
    return false; /* The header write follows through the planner. */
}

static bool prepare_open(struct vsr_io *io, uint32_t replica,
                         struct vsr_io_store *store, struct vsr_io_sqe *sqe)
{
    uint32_t slot;

    if (store->file_op != FILE_NONE) {
        return false;
    }
    if (store->log_path == NULL) {
        store->error = -ENAMETOOLONG;
        store_fail(store, VSR_IO_FAILED);
        return false;
    }
    if (store->file_slot == NONE) {
        store->file_slot = vsr_io_engine_slot_alloc(io);
        if (store->file_slot == NONE) {
            store->error = -ENFILE;
            store_fail(store, VSR_IO_FAILED);
            return false;
        }
    }
    slot = take_slot(io, VSR_IO_SLOT_FILE, replica, FILE_PROBE, 0);
    if (slot == NONE) {
        return false;
    }
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode = VSR_IO_SQE_OPENAT;
    sqe->flags = VSR_IO_SQE_DIRECT;
    sqe->fd = store->dir_fd;
    sqe->fd2 = (int32_t)store->file_slot;
    sqe->addr = store->log_path;
    sqe->op_flags =
        (uint32_t)O_RDWR |
        (store->options.direct_io != 0 ? (uint32_t)O_DIRECT : 0u) |
        (store->options.sync_mode == VSR_IO_SYNC_DSYNC ? (uint32_t)O_DSYNC
                                                       : 0u);
    sqe->user_data = vsr_io_slots_user_data(&io->slots, slot);
    store->file_op = FILE_PROBE;
    return true;
}

/* The growth FALLOCATE, then the superblock naming the new slot. */
static bool prepare_growth(struct vsr_io *io, uint32_t replica,
                           struct vsr_io_store *store, struct vsr_io_sqe *sqe)
{
    uint32_t slot;

    if (store->growth != GROWTH_ALLOCATING || store->file_op != FILE_NONE ||
        memory_only(store)) {
        return false;
    }
    slot = take_slot(io, VSR_IO_SLOT_FILE, replica, FILE_GROW, 0);
    if (slot == NONE) {
        return false;
    }
    log_sqe(store, sqe, VSR_IO_SQE_FALLOCATE,
            vsr_io_slots_user_data(&io->slots, slot));
    sqe->offset = store->file_size;
    sqe->length = (uint32_t)store->options.segment_bytes;
    store->file_op = FILE_GROW;
    return true;
}

/* Rewrites the superblock into the copy the previous write did not use. */
static bool prepare_superblock(struct vsr_io *io, uint32_t replica,
                               struct vsr_io_store *store,
                               struct vsr_io_sqe *sqe)
{
    struct vsr_io_wire_superblock superblock;
    uint64_t floor = store->durable;
    uint32_t copy;
    uint32_t slot;

    if (store->superblock_dirty == 0 || store->superblock_pending != 0 ||
        memory_only(store)) {
        return false;
    }
    slot = take_slot(io, VSR_IO_SLOT_SUPER, replica, 0, floor);
    if (slot == NONE) {
        return false;
    }
    copy = store->superblock_next;
    store->superblock_next = (copy + 1) % STORE_SUPERBLOCKS;
    store->superblock_revision++;
    superblock_fill(store, &superblock, store->superblock_revision,
                    file_slots(store), floor);
    vsr_io_codec_put_superblock(&superblock,
                                store->superblocks + copy * block_bytes(store),
                                store->options.block_bytes);
    log_sqe(store, sqe, VSR_IO_SQE_WRITE,
            vsr_io_slots_user_data(&io->slots, slot));
    sqe->flags |= VSR_IO_SQE_FIXED_BUFFER;
    sqe->buffer_index = (uint16_t)store->region_index;
    sqe->addr = store->superblocks + copy * block_bytes(store);
    sqe->length = store->options.block_bytes;
    sqe->offset = copy * block_bytes(store);
    store->superblock_dirty = 0;
    store->superblock_pending = 1;
    store->superblock_slot = slot;
    return true;
}

static bool prepare_write(struct vsr_io *io, uint32_t replica,
                          struct vsr_io_store *store, struct vsr_io_sqe *sqe)
{
    struct vsr_io_write *write;
    uint32_t slot;

    if (store->writes_count == store->options.inflight_writes ||
        store->error != 0) {
        return false;
    }
    slot = take_slot(io, VSR_IO_SLOT_WRITE, replica, 0, 0);
    if (slot == NONE) {
        return false;
    }
    write = write_plan(store);
    if (write == NULL) {
        vsr_io_slots_free(&io->slots, slot);
        return false;
    }
    io->slots.slots[slot].sub =
        (uint32_t)(write - store->writes); /* Its index. */
    write->slot = slot;
    write->state = WRITE_INFLIGHT;
    log_sqe(store, sqe, VSR_IO_SQE_WRITE,
            vsr_io_slots_user_data(&io->slots, slot));
    sqe->flags |= VSR_IO_SQE_FIXED_BUFFER;
    sqe->buffer_index = (uint16_t)store->region_index;
    sqe->addr = store->ring + write->ring_begin % store->ring_size;
    sqe->length = (uint32_t)(write->ring_end - write->ring_begin);
    sqe->offset = write->file_offset;
    io->stats.writes++;
    return true;
}

static bool prepare_flush(struct vsr_io *io, uint32_t replica,
                          struct vsr_io_store *store, struct vsr_io_sqe *sqe)
{
    uint32_t slot;

    if (store->options.sync_mode != VSR_IO_SYNC_FDATASYNC ||
        store->flush_pending != FLUSH_DUE || store->flush_slot != NONE ||
        store->error != 0 || store->written < store->flush_target) {
        return false;
    }
    slot = take_slot(io, VSR_IO_SLOT_FLUSH, replica, 0, store->written);
    if (slot == NONE) {
        return false;
    }
    log_sqe(store, sqe, VSR_IO_SQE_FSYNC,
            vsr_io_slots_user_data(&io->slots, slot));
    sqe->op_flags = VSR_IO_FSYNC_DATASYNC;
    store->flush_slot = slot;
    store->flush_pending = FLUSH_NONE;
    if (store->freeing_flush == FREEING_FLUSH_WANTED) {
        store->freeing_flush = FREEING_FLUSH_INFLIGHT;
    }
    io->stats.flushes++;
    return true;
}

void vsr_io_store_prepare(struct vsr_io *io, uint32_t replica,
                          struct vsr_io_sqe *sqes, uint32_t capacity,
                          uint32_t *count)
{
    struct vsr_io_store *store = &io->replicas[replica].store;
    bool wrote = false;

    while (*count < capacity) {
        struct vsr_io_sqe *sqe = &sqes[*count];
        bool issued = false;

        switch ((enum vsr_io_store_state)store->state) {
        case VSR_IO_STORE_OPENING:
            issued = prepare_open(io, replica, store, sqe);
            break;
        case VSR_IO_STORE_CREATING:
            issued = prepare_create(io, replica, store, sqe);
            if (!issued && store->current != NONE && !wrote) {
                issued = wrote = prepare_write(io, replica, store, sqe);
            }
            break;
        case VSR_IO_STORE_READY:
            issued = prepare_growth(io, replica, store, sqe) ||
                     prepare_superblock(io, replica, store, sqe);
            if (!issued && !wrote) {
                issued = wrote = prepare_write(io, replica, store, sqe);
            }
            if (!issued) {
                issued = prepare_flush(io, replica, store, sqe) ||
                         prepare_load(io, replica, store, sqe);
            }
            break;
        case VSR_IO_STORE_RECOVERING:
            issued = prepare_recovery(io, replica, store, sqe);
            break;
        case VSR_IO_STORE_CLOSED:
        case VSR_IO_STORE_FAILED:
        case VSR_IO_STORE_CLOSING:
        default:
            break;
        }
        if (!issued) {
            break;
        }
        (*count)++;
    }
}

void vsr_io_store_complete(struct vsr_io *io, uint32_t replica, uint32_t slot,
                           const struct vsr_io_cqe *cqe)
{
    struct vsr_io_store *store = &io->replicas[replica].store;
    const struct vsr_io_slot *entry = &io->slots.slots[slot];
    uint8_t kind = entry->kind;
    uint32_t sub = entry->sub;
    uint64_t cookie = entry->cookie;

    vsr_io_slots_consumed(&io->slots, slot, false);
    switch (kind) {
    case VSR_IO_SLOT_WRITE:
        write_done(store, sub, cqe->result);
        break;
    case VSR_IO_SLOT_FLUSH:
        flush_done(store, cookie, cqe->result);
        break;
    case VSR_IO_SLOT_SUPER:
        superblock_done(store, cookie, cqe->result);
        break;
    case VSR_IO_SLOT_FILE:
        file_done(store, sub, cookie, cqe->result);
        break;
    case VSR_IO_SLOT_LOAD:
        if (sub == RECOVERY_SUB) {
            recovery_read_done(store, cqe->result);
        } else {
            load_done(store, cqe->result);
        }
        break;
    default:
        STORE_ASSERT(false);
        break;
    }
    if (store->state == VSR_IO_STORE_READY) {
        stores_drain(store);
        loads_drain(store);
    }
}

void vsr_io_store_poll(struct vsr_io *io, uint32_t replica, uint64_t now)
{
    struct vsr_io_replica *owner = &io->replicas[replica];
    struct vsr_io_store *store = &owner->store;
    bool replicated = owner->options.durability == VSR_REPLICATED;

    if (store->state == VSR_IO_STORE_READY) {
        /* Sync delay: the first SYNC of a batch starts it. */
        if (store->flush_pending == FLUSH_WANTED) {
            if (store->options.sync_delay_ns == 0) {
                store->flush_pending = FLUSH_DUE;
            } else {
                store->flush_pending = FLUSH_NONE;
                store->flush_deadline = now + store->options.sync_delay_ns;
            }
        }
        /* Write-behind cadence: a flush interval after the first
         * unflushed write. */
        if (replicated && store->options.sync_mode == VSR_IO_SYNC_FDATASYNC &&
            store->written > store->flushed &&
            store->flush_pending == FLUSH_NONE && store->flush_slot == NONE &&
            store->flush_deadline == VSR_NO_DEADLINE && store->error == 0) {
            store->flush_deadline = now + flush_interval(store);
        }
        if (store->flush_deadline <= now) {
            store->flush_deadline = VSR_NO_DEADLINE;
            store->flush_pending = FLUSH_DUE;
            if (store->written > store->flush_target) {
                store->flush_target = store->written;
            }
        }
        /* Idle floor (decision 50). */
        if (store->durable > store->superblock_floor &&
            store->packed_since_durable == 0 &&
            store->idle_deadline == VSR_NO_DEADLINE &&
            store->superblock_dirty == 0 && store->superblock_pending == 0) {
            store->idle_deadline = now + flush_interval(store);
        }
        if (store->idle_deadline <= now) {
            store->idle_deadline = VSR_NO_DEADLINE;
            if (store->packed_since_durable == 0 &&
                store->durable > store->superblock_floor) {
                store->superblock_dirty = 1;
            }
        }
        stores_drain(store);
        syncs_settle(store);
    }
    loads_drain(store);
    vsr_io_deadlines_arm(&io->deadlines, owner->deadline_sync,
                         replicated ? VSR_NO_DEADLINE : store->flush_deadline);
    vsr_io_deadlines_arm(&io->deadlines, owner->deadline_flush,
                         replicated ? store->flush_deadline
                                    : store->idle_deadline);
}

uint64_t vsr_io_store_deadline(const struct vsr_io_store *store)
{
    if (store == NULL) {
        return VSR_NO_DEADLINE;
    }
    return store->flush_deadline < store->idle_deadline ? store->flush_deadline
                                                        : store->idle_deadline;
}

/* -------------------------------------------------------------------------
 * Status and close
 * ---------------------------------------------------------------------- */

int vsr_io_store_close(struct vsr_io_store *store)
{
    if (store == NULL) {
        return VSR_EINVAL;
    }
    if (store->writes_count > 0 || store->flush_slot != NONE ||
        store->superblock_pending != 0 || store->file_op != FILE_NONE ||
        store->cold_active != 0 || store->recovery.io_slot != NONE) {
        return VSR_EBUSY;
    }
    recovery_release_slab(store);
    store->log_slot = -1;
    store->file_slot = NONE;
    store->state = VSR_IO_STORE_CLOSED;
    return VSR_OK;
}

void vsr_io_store_status(const struct vsr_io_store *store,
                         struct vsr_io_store_status *status)
{
    if (store == NULL || status == NULL) {
        return;
    }
    memset(status, 0, sizeof(*status));
    status->readable = store->readable;
    status->written = store->written;
    status->durable = store->durable;
    status->log_begin = store->log_begin;
    status->log_end = store->log_end;
    status->unwritten_bytes = store->head - written_floor(store);
    for (uint32_t i = 0; i < store->slots; ++i) {
        if (store->segments[i].number != 0) {
            status->live_segments++;
            status->used_bytes += store->segments[i].used;
        }
    }
    status->capacity_bytes =
        (uint64_t)store->slots * store->options.segment_bytes;
    status->entries = (uint32_t)(store->log_end - store->log_begin);
    status->clients = store->clients_count;
    status->error = store->error;
}

/* -------------------------------------------------------------------------
 * RECLAIM and freeing (section 6.5)
 * ---------------------------------------------------------------------- */

int vsr_io_store_reclaim(struct vsr_io_store *store, uint64_t op,
                         uint64_t oldest)
{
    if (store == NULL || op == 0) {
        return VSR_EINVAL;
    }
    if (store->state == VSR_IO_STORE_FAILED) {
        complete(store, op, VSR_IO_FAILED, NONE, NULL);
        return VSR_OK;
    }
    if (store->state != VSR_IO_STORE_READY || oldest > store->readable + 1) {
        return VSR_EINVAL;
    }
    if (oldest > store->reclaim) {
        store->reclaim = oldest;
        reclaim_apply(store);
    }
    complete(store, op, VSR_IO_OK, NONE, NULL);
    stores_drain(store); /* A STORE held for a slot may have one now. */
    return VSR_OK;
}

uint64_t vsr_io_store_free_floor(const struct vsr_io_store *store)
{
    uint64_t floor;

    if (store == NULL) {
        return 0;
    }
    /* The record of the oldest retained entry: the first live op from
     * retained_begin (sequences never decrease along the ring), or every
     * record when the log is empty; then every truncated version kept. */
    floor = store->readable + 1;
    for (uint64_t op = store->retained_begin; op < store->log_end; ++op) {
        const struct vsr_io_op_ref *ref = op_current(store, op);

        if (ref != NULL) {
            if (ref->sequence < floor) {
                floor = ref->sequence;
            }
            break;
        }
    }
    for (uint32_t i = 0; i < store->versions_count; ++i) {
        if (store->versions[i].appended < floor) {
            floor = store->versions[i].appended;
        }
    }
    if (store->reclaimed < floor) {
        floor = store->reclaimed;
    }
    if (store->client_base_floor + 1 < floor) {
        floor = store->client_base_floor + 1;
    }
    if (store->capture_floor < floor) {
        floor = store->capture_floor;
    }
    return floor;
}

/* True when every byte packed into the slot was written: nothing of it
 * is planned, in flight or waiting for a header. */
static bool segment_quiescent(const struct vsr_io_store *store, uint32_t slot)
{
    uint64_t floor = written_floor(store);

    for (uint32_t i = 0; i < store->extents_count; ++i) {
        const struct vsr_io_extent *extent = extent_at(store, i);

        if (extent->segment == slot &&
            extent->ring_offset + extent->length > floor) {
            return false;
        }
    }
    return true;
}

/* True when a queued load still has a record to read from the slot. */
static bool segment_loading(const struct vsr_io_store *store, uint32_t slot)
{
    for (uint32_t i = 0; i < store->loads_count; ++i) {
        const struct vsr_io_pending_load *load = &store->loads[ring_slot(
            store->loads_head, i, store->limits.operations)];
        const struct vsr_io_load_ref *refs =
            &store->load_refs[(size_t)(load - store->loads) *
                              store->limits.batch_entries];

        if (load->base != 0) {
            continue;
        }
        for (uint32_t k = 0; k < load->count; ++k) {
            if (segment_of(store, refs[k].offset) == slot) {
                return true;
            }
        }
    }
    return false;
}

void vsr_io_store_free_segments(struct vsr_io_store *store)
{
    uint64_t floor;
    uint32_t start = NONE;
    bool freed = false;

    if (store == NULL || store->state != VSR_IO_STORE_READY) {
        return;
    }
    floor = vsr_io_store_free_floor(store);
    for (uint32_t slot = 0; slot < store->slots; ++slot) {
        struct vsr_io_segment *segment = &store->segments[slot];

        if (segment->number == 0 || segment->phase != VSR_IO_SEGMENT_SEALED ||
            segment->last_sequence >= floor ||
            !segment_quiescent(store, slot) || segment_loading(store, slot)) {
            continue;
        }
        /* Its extents die: the file bytes they name may be rewritten. */
        for (uint32_t i = 0; i < store->extents_count; ++i) {
            struct vsr_io_extent *extent = extent_at(store, i);

            if (extent->segment == slot) {
                extent->segment = NONE;
            }
        }
        memset(segment, 0, sizeof(*segment));
        segment->header_write = NONE;
        segment->phase =
            memory_only(store) ? VSR_IO_SEGMENT_FREE : VSR_IO_SEGMENT_FREEING;
        freed = true;
    }
    if (!freed) {
        return;
    }
    /* The start segment: the live one with the smallest number, named by
     * a superblock write before any freed slot is reused. */
    for (uint32_t slot = 0; slot < store->slots; ++slot) {
        if (store->segments[slot].number != 0 &&
            (start == NONE ||
             store->segments[slot].number < store->segments[start].number)) {
            start = slot;
        }
    }
    if (start != NONE) {
        store->start_segment = store->segments[start].number;
        store->start_slot = start;
    }
    store->superblock_dirty = 1;
}

/* -------------------------------------------------------------------------
 * Admission, captures and base files
 * ---------------------------------------------------------------------- */

bool vsr_io_store_admit(struct vsr_io_store *store, struct vsr_id client)
{
    struct vsr_io_client *entry;

    if (store == NULL || id_zero(client)) {
        return false;
    }
    if (client_find(store, client) != NULL) {
        return true;
    }
    entry = client_insert(store, client);
    if (entry == NULL) {
        return false;
    }
    entry->inflight = 1;
    return true;
}

void vsr_io_store_replied(struct vsr_io_store *store, struct vsr_id client)
{
    struct vsr_io_client *entry;

    if (store == NULL) {
        return;
    }
    entry = client_find(store, client);
    if (entry != NULL) {
        entry->inflight = 0;
        client_prune(store, entry);
    }
}

uint32_t vsr_io_store_snapshot_clients(struct vsr_io_store *store,
                                       struct vsr_io_client_snapshot *out,
                                       uint32_t capacity)
{
    uint64_t floor = UINT64_MAX;
    uint32_t n = 0;

    if (store == NULL || out == NULL) {
        return 0;
    }
    for (uint32_t i = 0; i < store->clients_capacity; ++i) {
        struct vsr_io_client *entry = &store->clients[i];

        if (id_zero(entry->id)) {
            continue;
        }
        entry->capture_offset = UINT64_MAX;
        if (entry->current.number == 0) {
            continue;
        }
        if (n < capacity) {
            out[n].id = entry->id;
            out[n].record = entry->current;
        }
        if (entry->current.sequence != 0 && entry->current.sequence < floor) {
            floor = entry->current.sequence;
        }
        n++;
    }
    store->capture_floor = floor;
    return n;
}

void vsr_io_store_capture_offset(struct vsr_io_store *store,
                                 struct vsr_id client, uint64_t offset)
{
    struct vsr_io_client *entry;

    if (store == NULL) {
        return;
    }
    entry = client_find(store, client);
    if (entry != NULL) {
        entry->capture_offset = offset;
    }
}

void vsr_io_store_capture_end(struct vsr_io_store *store, struct vsr_id id,
                              uint64_t sequence)
{
    if (store == NULL) {
        return;
    }
    if (!id_zero(id)) {
        store->last_capture = id;
        store->last_capture_sequence = sequence;
    }
    store->capture_floor = UINT64_MAX;
    stores_drain(store); /* A STORE held for a slot may free one now. */
}

void vsr_io_store_base_set(struct vsr_io_store *store, struct vsr_id id,
                           uint64_t sequence)
{
    if (store == NULL) {
        return;
    }
    base_apply(store, id, sequence, store->readable,
               id_equal(id, store->last_capture) ? BASE_FROM_CAPTURE
                                                 : BASE_FROM_FILE);
    stores_drain(store);
}

void vsr_io_store_base_begin(struct vsr_io_store *store)
{
    if (store == NULL) {
        return;
    }
    for (uint32_t i = 0; i < store->clients_capacity; ++i) {
        store->clients[i].next_offset = UINT64_MAX;
    }
    if (store->base_state == BASE_WANTED) {
        store->base_state = BASE_LOADING;
    }
}

int vsr_io_store_base_record(struct vsr_io_store *store,
                             const struct vsr_io_wire_client_record *record,
                             uint64_t file_offset)
{
    struct vsr_io_client *entry;
    struct vsr_id client;

    if (store == NULL || record == NULL) {
        return VSR_EINVAL;
    }
    client.hi = record->client_hi;
    client.lo = record->client_lo;
    if (id_zero(client) || record->number == 0 || file_offset == UINT64_MAX) {
        return VSR_EINVAL;
    }
    entry = client_get(store, client);
    if (entry == NULL) {
        return VSR_ELIMIT;
    }
    if (record->number < entry->current.number) {
        return VSR_OK; /* The log holds a later record: not covered. */
    }
    if (record->number == entry->current.number) {
        if (record->op != entry->current.op) {
            return VSR_EINVAL;
        }
    } else {
        memset(&entry->current, 0, sizeof(entry->current));
        entry->current.number = record->number;
        entry->current.op = record->op;
        entry->current.offset = file_offset;
        entry->inflight = 0;
    }
    entry->next_offset = file_offset;
    return VSR_OK;
}

void vsr_io_store_base_end(struct vsr_io_store *store, struct vsr_id id,
                           uint64_t sequence)
{
    if (store == NULL) {
        return;
    }
    if (store->base_state == BASE_LOADING &&
        store->base_kind != BASE_RECOVERY) {
        store->base_state = BASE_LOADED; /* Applied when packed. */
        return;
    }
    /* Nothing waits (recovery, or a call outside a held transaction):
     * the file is the base now. */
    if (store->base_state == BASE_LOADING) {
        store->base_state = BASE_LOADED;
    }
    base_apply(store, id, sequence, store->readable, BASE_FROM_FILE);
}

bool vsr_io_store_base_wanted(const struct vsr_io_store *store,
                              struct vsr_id *id, uint64_t *sequence)
{
    if (store == NULL || store->base_state != BASE_WANTED) {
        return false;
    }
    if (id != NULL) {
        *id = store->base_id;
    }
    if (sequence != NULL) {
        *sequence = store->base_sequence;
    }
    return true;
}

void vsr_io_store_base_resume(struct vsr_io_store *store, int32_t status)
{
    uint64_t op;

    if (store == NULL || store->base_state == BASE_NONE) {
        return;
    }
    if (store->base_kind == BASE_RECOVERY) {
        /* Step 7 of recovery: a load that never began covers nothing. */
        if (status == VSR_IO_OK && store->base_state == BASE_WANTED) {
            vsr_io_store_base_begin(store);
            vsr_io_store_base_end(store, store->base_id, store->base_sequence);
        }
        store->base_state = BASE_NONE;
        recovery_base_done(store, status);
        return;
    }
    if (status == VSR_IO_OK) {
        if (store->base_state == BASE_WANTED) {
            vsr_io_store_base_begin(store); /* Nothing loaded: empty. */
        }
        store->base_state = BASE_LOADED;
        stores_drain(store);
        return;
    }
    /* The held transaction fails with the status, which fences the
     * replica (vsr.h); the store follows. */
    op = store->base_op;
    store->base_state = BASE_NONE;
    if (store->stores_count > 0 && stores_at(store, 0)->op == op) {
        stores_pop(store);
        complete(store, op, status, NONE, NULL);
    }
    store_fail(store, VSR_IO_FAILED);
}
