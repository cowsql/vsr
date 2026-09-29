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
 *   3. recovery of an existing log (decisions 48 and 50).
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
 * cap, so a write is never short by design (decision S2). */
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
    FILE_GROW      /* Growth FALLOCATE of one more slot. */
};

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
static bool state_bytes(const struct vsr_limits *limits, size_t *bytes)
{
    size_t epoch;
    size_t members;
    size_t manifest;
    size_t total;

    if (!vsr_size_mul(limits->members, sizeof(struct vsr_member), &members) ||
        !vsr_size_mul(members, 2, &members) ||
        !vsr_size_add(members, sizeof(struct vsr_epoch), &epoch) ||
        !vsr_size_add(epoch, 2 * sizeof(struct vsr_membership), &epoch) ||
        !vsr_size_mul(epoch, 2, &total) ||
        !vsr_size_add(total, sizeof(struct vsr_checkpoint), &total) ||
        !vsr_size_add(total, sizeof(struct vsr_span), &total) ||
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
     * write-behind allowance, one record and its padding (decision S2). */
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
     * records and the seal's header with its alignment (decision S1). */
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
    /* Phase 2: cold LOADs in flight complete FAILED here too. */
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

bool vsr_io_store_hot(const struct vsr_io_store *store, uint64_t offset,
                      uint32_t length, struct vsr_io_piece *piece)
{
    if (store == NULL || piece == NULL) {
        return false;
    }
    for (uint32_t i = 0; i < store->extents_count; ++i) {
        const struct vsr_io_extent *extent = extent_at(store, i);

        if (offset >= extent->file_offset &&
            offset - extent->file_offset <= extent->length &&
            length <= extent->length - (offset - extent->file_offset)) {
            uint64_t ring =
                extent->ring_offset + (offset - extent->file_offset);

            piece->base = store->ring + ring % store->ring_size;
            piece->length = length;
            return true;
        }
    }
    return false;
}

uint32_t vsr_io_store_pin(struct vsr_io_store *store, uint64_t offset,
                          uint32_t length, uint32_t lease)
{
    if (store == NULL || lease >= store->pins_count) {
        return NONE;
    }
    for (uint32_t i = 0; i < store->extents_count; ++i) {
        const struct vsr_io_extent *extent = extent_at(store, i);

        if (offset >= extent->file_offset &&
            offset - extent->file_offset <= extent->length &&
            length <= extent->length - (offset - extent->file_offset)) {
            struct vsr_io_ring_pin *pin = &store->pins[lease];

            STORE_ASSERT(pin->begin == UINT64_MAX);
            pin->begin = extent->ring_offset + (offset - extent->file_offset);
            pin->end = pin->begin + length;
            return lease;
        }
    }
    return NONE;
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
    /* Phase 2 also drops the slab reference of a cold LOAD here; the
     * engine frees the region. */
    vsr_io_store_unpin(store, lease);
}

/* -------------------------------------------------------------------------
 * Segments
 * ---------------------------------------------------------------------- */

static uint32_t segment_free_slot(const struct vsr_io_store *store)
{
    for (uint32_t i = 0; i < store->slots; ++i) {
        if (store->segments[i].number == 0) {
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
    state.identity = store->identity_set ? &store->identity : NULL;
    state.hard = store->identity_set ? &store->hard : NULL;
    state.checkpoint = store->identity_set && (store->anchor.id.hi != 0 ||
                                               store->anchor.id.lo != 0)
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
 * Packing
 * ---------------------------------------------------------------------- */

/* Phase 2: applies a packed transaction to the indexes (section 6.3) and
 * returns false with the status to fail with when an index overflows or
 * the transaction contradicts the store. Phase 1 indexes nothing. */
static bool index_apply(struct vsr_io_store *store,
                        const struct vsr_store *transaction, uint64_t offset,
                        uint32_t length, int32_t *status)
{
    (void)store;
    (void)transaction;
    (void)offset;
    (void)length;
    *status = VSR_IO_OK;
    return true;
}

/* Tries to pack one held STORE: the segment fit, the slot supply, the
 * pinned-floor and write-behind holds (section 6.1 steps 1 to 4), then the
 * copy (step 5). False when it must stay held. */
static bool store_pack(struct vsr_io_store *store,
                       struct vsr_io_pending_store *pending)
{
    uint64_t n = pending->bytes;
    bool seal =
        store->current == NONE ||
        store->segments[store->current].used + n > store->options.segment_bytes;
    uint32_t slot = NONE;
    unsigned char *at;
    uint64_t offset;
    size_t written = 0;
    int32_t status;
    int rc;

    if (seal) {
        slot = segment_free_slot(store);
        if (slot == NONE && memory_only(store) &&
            store->slots < store->options.max_segments) {
            slot = store->slots++; /* A slot of the table, not the file. */
        }
        if (slot == NONE) {
            /* Phase 2: vsr_io_store_free_segments may free one; a store
             * whose floors keep every slot fails with FAILED. */
            if (store->growth == GROWTH_NONE &&
                store->slots < store->options.max_segments) {
                store->growth = GROWTH_ALLOCATING;
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
    extent_newest(store)->last_sequence = pending->sequence;
    store->readable = pending->sequence;
    store->packed_since_durable = 1;
    if (!index_apply(store, pending->transaction, offset, (uint32_t)n,
                     &status)) {
        complete(store, pending->op, status, NONE, NULL);
        store_fail(store, status);
        return true;
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

        if (extent->ring_offset + extent->length > store->issued) {
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
    syncs_settle(store);
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
}

static void file_done(struct vsr_io_store *store, uint32_t op, uint64_t cookie,
                      int32_t result)
{
    store->file_op = FILE_NONE;
    switch (op) {
    case FILE_PROBE:
        if (result >= 0) {
            /* Phase 3: recover the existing log (RECOVER), or report it
             * to NEW and JOIN as decision 57 requires. Until then the
             * open fails; the slot holds the file for the engine to
             * close at detach. */
            store->log_slot = (int32_t)store->file_slot;
            store->state = VSR_IO_STORE_RECOVERING;
            store_fail(store, VSR_IO_FAILED);
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
             * follow (decision S3); its STOREs are held until then. */
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
                issued = prepare_flush(io, replica, store, sqe);
            }
            /* Phase 2: one cold LOAD read. */
            break;
        case VSR_IO_STORE_RECOVERING:
            /* Phase 3: superblock and header reads, the scan. */
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
        /* Phase 2: cold LOAD read completion. */
        break;
    default:
        STORE_ASSERT(false);
        break;
    }
    if (store->state == VSR_IO_STORE_READY) {
        stores_drain(store);
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
        store->cold_active != 0) {
        return VSR_EBUSY;
    }
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
 * Phase 2: indexes, LOADs, RECLAIM, freeing, admission, capture, base
 * files. Every entry point below is a stub that refuses or does nothing;
 * the tree builds and the phase-1 behaviour above does not depend on it.
 * ---------------------------------------------------------------------- */

int vsr_io_store_load(struct vsr_io_store *store, uint64_t op,
                      const struct vsr_store_read *read)
{
    (void)store;
    (void)op;
    (void)read;
    return VSR_EINVAL; /* Phase 2. */
}

int vsr_io_store_reclaim(struct vsr_io_store *store, uint64_t op,
                         uint64_t oldest)
{
    if (store == NULL || op == 0) {
        return VSR_EINVAL;
    }
    (void)oldest;
    return VSR_EINVAL; /* Phase 2. */
}

bool vsr_io_store_admit(struct vsr_io_store *store, struct vsr_id client)
{
    (void)store;
    (void)client;
    return false; /* Phase 2. */
}

void vsr_io_store_replied(struct vsr_io_store *store, struct vsr_id client)
{
    (void)store;
    (void)client; /* Phase 2. */
}

uint32_t vsr_io_store_snapshot_clients(struct vsr_io_store *store,
                                       struct vsr_io_client_snapshot *out,
                                       uint32_t capacity)
{
    (void)store;
    (void)out;
    (void)capacity;
    return 0; /* Phase 2. */
}

void vsr_io_store_capture_offset(struct vsr_io_store *store,
                                 struct vsr_id client, uint64_t offset)
{
    (void)store;
    (void)client;
    (void)offset; /* Phase 2. */
}

void vsr_io_store_capture_end(struct vsr_io_store *store, struct vsr_id id,
                              uint64_t sequence)
{
    (void)store;
    (void)id;
    (void)sequence; /* Phase 2. */
}

void vsr_io_store_base_set(struct vsr_io_store *store, struct vsr_id id,
                           uint64_t sequence)
{
    (void)store;
    (void)id;
    (void)sequence; /* Phase 2. */
}

void vsr_io_store_base_begin(struct vsr_io_store *store)
{
    (void)store; /* Phase 2. */
}

int vsr_io_store_base_record(struct vsr_io_store *store,
                             const struct vsr_io_wire_client_record *record,
                             uint64_t file_offset)
{
    (void)store;
    (void)record;
    (void)file_offset;
    return VSR_EINVAL; /* Phase 2. */
}

void vsr_io_store_base_end(struct vsr_io_store *store, struct vsr_id id,
                           uint64_t sequence)
{
    (void)store;
    (void)id;
    (void)sequence; /* Phase 2. */
}

bool vsr_io_store_base_wanted(const struct vsr_io_store *store,
                              struct vsr_id *id, uint64_t *sequence)
{
    (void)store;
    (void)id;
    (void)sequence;
    return false; /* Phase 2. */
}

void vsr_io_store_base_resume(struct vsr_io_store *store, int32_t status)
{
    (void)store;
    (void)status; /* Phase 2. */
}

uint64_t vsr_io_store_free_floor(const struct vsr_io_store *store)
{
    /* Phase 2: min(record of the oldest retained entry, reclaim, client
     * base + 1, capture floor). Nothing is freed until then. */
    (void)store;
    return 0;
}

void vsr_io_store_free_segments(struct vsr_io_store *store)
{
    (void)store; /* Phase 2. */
}
