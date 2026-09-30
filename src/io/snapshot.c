/* The GNU declarations (O_DIRECTORY, struct statx) precede every system
 * header. */
#define _GNU_SOURCE
#include "config.h"

#include "io/snapshot.h"

#include "checked.h"
#include "io/codec.h"
#include "io/crc32c.h"
#include "io/engine.h"
#include "io/link.h"
#include "io/stream.h"

#include <errno.h>
#include <fcntl.h>
#include <stdalign.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <sys/stat.h>

/*
 * Snapshot module (docs/io-implementation.md, "Snapshots"): the clients
 * file and the joint snapshot ops. Everything here is a planner driven by
 * the engine's poll, prepare and complete; the executor records it emits
 * all use slot kind CLIENTS.
 *
 * Every asynchronous activity is a JOB on a registry entry (or on a served
 * stream, or the directory) that runs one executor record at a time: the
 * entry's `step` names the record to issue next (or IDLE while the job
 * waits for the writer to stage bytes, for chunks, or for the caller),
 * prepare builds it and complete advances the job. The capture writer and
 * the file reader (base loads and fetch verification) are one each per
 * replica and serialize CAPTUREs and FETCHes/loads accordingly.
 */

#define NONE VSR_IO_INDEX_NONE
#define SNAPSHOT_FILE_MODE 0644u
#define SNAPSHOT_RETRY_NS UINT64_C(1000000)
#define SNAPSHOT_TMP_SUFFIX ".tmp"
#define SNAPSHOT_HEADER_BYTES 64u
#define SNAPSHOT_TRAILER_BYTES 8u
#define SNAPSHOT_RECORD_HEADER 40u
#define SNAPSHOT_RECORD_LENGTH_AT 36u

/* Invariant checks in debug builds; a violation traps (see pool.c). */
#ifdef NDEBUG
#define SNAPSHOT_ASSERT(condition) ((void)sizeof(condition))
#else
#define SNAPSHOT_ASSERT(condition) ((condition) ? (void)0 : __builtin_trap())
#endif

/* Slot `sub` values: the actor a record belongs to. */
#define SUB_KIND_SHIFT 24u
#define SUB_ENTRY (UINT32_C(1) << SUB_KIND_SHIFT)
#define SUB_SERVE (UINT32_C(2) << SUB_KIND_SHIFT)
#define SUB_DIR (UINT32_C(3) << SUB_KIND_SHIFT)
#define SUB_INDEX_MASK (SUB_ENTRY - 1u)

/* Steps: the record a job issues next; the slot cookie carries it. */
enum step {
    STEP_IDLE,
    STEP_OPEN,
    STEP_STAT,
    STEP_READ,
    STEP_WRITE,
    STEP_FSYNC,
    STEP_CLOSE,
    STEP_FSYNC_DIR,
    STEP_RENAME,
    STEP_UNLINK
};

enum reader_stage {
    STAGE_HEADER,
    STAGE_RECORDS,
    STAGE_TRAILER,
    STAGE_DONE,
    STAGE_FAILED
};

enum reader_purpose { PURPOSE_LOAD, PURPOSE_FETCH };

enum dir_state { DIR_CLOSED, DIR_OPENING, DIR_OPEN, DIR_FAILED };

enum serve_state { SERVE_FREE, SERVE_OPENING, SERVE_OPEN, SERVE_CLOSING };

/* A record parse that needs more bytes (private, positive). */
#define RECORD_MORE 1

static bool id_zero(struct vsr_id id)
{
    return id.hi == 0 && id.lo == 0;
}

static bool id_equal(struct vsr_id a, struct vsr_id b)
{
    return a.hi == b.hi && a.lo == b.lo;
}

static uint64_t padding_of(uint64_t size)
{
    return (8u - size % 8u) % 8u;
}

static uint64_t round_up(uint64_t value, uint64_t multiple)
{
    return (value + multiple - 1) / multiple * multiple;
}

static uint32_t get_u32(const unsigned char *at)
{
    return (uint32_t)at[0] | ((uint32_t)at[1] << 8) | ((uint32_t)at[2] << 16) |
           ((uint32_t)at[3] << 24);
}

static void put_u32(unsigned char *at, uint32_t value)
{
    at[0] = (unsigned char)(value & 0xFFu);
    at[1] = (unsigned char)((value >> 8) & 0xFFu);
    at[2] = (unsigned char)((value >> 16) & 0xFFu);
    at[3] = (unsigned char)((value >> 24) & 0xFFu);
}

static struct vsr_io_replica *replica_of(struct vsr_io *io, uint32_t replica)
{
    return &io->replicas[replica];
}

static struct vsr_io_snapshots *snapshots_of(struct vsr_io *io,
                                             uint32_t replica)
{
    return &io->replicas[replica].snapshots;
}

/* -------------------------------------------------------------------------
 * Layout
 * ---------------------------------------------------------------------- */

struct plan {
    size_t entries;
    size_t table;
    size_t fileops;
    size_t chunks;
    size_t serves;
    size_t regions;
    size_t region_bytes;
    size_t total;
    uint32_t count;
};

static bool place(size_t *offset, size_t bytes, size_t alignment, size_t *at)
{
    size_t aligned;

    if (!vsr_size_add(*offset, alignment - 1, &aligned)) {
        return false;
    }
    aligned = aligned / alignment * alignment;
    *at = aligned;
    return vsr_size_add(aligned, bytes, offset);
}

/* Bytes of one checkpoint copy: the checkpoint itself (a lease region
 * holds it with its graph), the epoch with two memberships, the manifest
 * bytes and their span, plus alignment slack. */
static bool region_bytes(const struct vsr_limits *limits, size_t *bytes)
{
    size_t members;
    size_t total;

    if (!vsr_size_mul(limits->members, sizeof(struct vsr_member), &members) ||
        !vsr_size_mul(members, 2, &members) ||
        !vsr_size_add(members, sizeof(struct vsr_epoch), &total) ||
        !vsr_size_add(total, sizeof(struct vsr_checkpoint), &total) ||
        !vsr_size_add(total, 2 * sizeof(struct vsr_membership), &total) ||
        !vsr_size_add(total, sizeof(struct vsr_span), &total) ||
        limits->manifest_bytes > SIZE_MAX / 2 ||
        !vsr_size_add(total, (size_t)limits->manifest_bytes, &total) ||
        !vsr_size_add(total, 64, &total)) {
        return false;
    }
    *bytes = total;
    return true;
}

static int plan_of(const struct vsr_limits *limits,
                   const struct vsr_io_limits *io_limits, uint32_t max_clients,
                   struct plan *plan)
{
    size_t offset = 0;
    size_t bytes;
    size_t count;

    memset(plan, 0, sizeof(*plan));
    if (!vsr_size_add(limits->transfers, VSR_IO_SNAPSHOT_EXTRA, &count) ||
        count > UINT32_MAX / 2 ||
        !vsr_size_mul(count, sizeof(struct vsr_io_snapshot), &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_snapshot),
               &plan->entries) ||
        !vsr_size_mul(max_clients, sizeof(struct vsr_io_client_snapshot),
                      &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_client_snapshot),
               &plan->table) ||
        !place(&offset,
               VSR_IO_SNAPSHOT_FILEOPS * sizeof(struct vsr_io_snapshot_fileop),
               alignof(struct vsr_io_snapshot_fileop), &plan->fileops) ||
        !vsr_size_mul(io_limits->stream_window,
                      sizeof(struct vsr_io_snapshot_chunk), &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_snapshot_chunk),
               &plan->chunks) ||
        !vsr_size_mul(io_limits->streams, sizeof(struct vsr_io_snapshot_serve),
                      &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_snapshot_serve),
               &plan->serves) ||
        !region_bytes(limits, &plan->region_bytes) ||
        !place(&offset, plan->region_bytes, 16, &plan->regions)) {
        return VSR_ELIMIT;
    }
    plan->count = (uint32_t)count;
    plan->total = offset;
    return VSR_OK;
}

int vsr_io_snapshots_region_bytes(const struct vsr_limits *limits,
                                  size_t *bytes)
{
    if (limits == NULL || bytes == NULL) {
        return VSR_EINVAL;
    }
    return region_bytes(limits, bytes) ? VSR_OK : VSR_ELIMIT;
}

int vsr_io_snapshots_size(const struct vsr_limits *limits,
                          const struct vsr_io_limits *io_limits,
                          uint32_t max_clients, size_t *bytes,
                          size_t *alignment)
{
    struct plan plan;
    int rc;

    if (limits == NULL || io_limits == NULL || bytes == NULL ||
        alignment == NULL || io_limits->stream_window == 0 ||
        io_limits->streams == 0) {
        return VSR_EINVAL;
    }
    rc = plan_of(limits, io_limits, max_clients, &plan);
    if (rc != VSR_OK) {
        return rc;
    }
    *bytes = plan.total;
    *alignment = 16;
    return VSR_OK;
}

void vsr_io_snapshots_init(struct vsr_io_snapshots *snapshots, void *memory,
                           size_t size, const struct vsr_limits *limits,
                           const struct vsr_io_limits *io_limits,
                           uint32_t max_clients)
{
    struct plan plan;
    unsigned char *base = memory;
    int rc = plan_of(limits, io_limits, max_clients, &plan);

    SNAPSHOT_ASSERT(rc == VSR_OK && plan.total <= size);
    (void)rc;
    (void)size;
    memset(snapshots, 0, sizeof(*snapshots));
    memset(base, 0, plan.total);
    snapshots->entries =
        (struct vsr_io_snapshot *)(void *)(base + plan.entries);
    snapshots->count = plan.count;
    for (uint32_t i = 0; i < plan.count; ++i) {
        snapshots->entries[i].file_slot = -1;
        snapshots->entries[i].fileop = NONE;
        snapshots->entries[i].tmp_slot = NONE;
        snapshots->entries[i].job_next = NONE;
        snapshots->entries[i].lease = NONE;
    }
    snapshots->capture = NONE;
    snapshots->pending_base = NONE;
    snapshots->writer.snapshot = NONE;
    snapshots->writer.slab = NONE;
    snapshots->writer.cold_slab = NONE;
    snapshots->writer.table =
        (struct vsr_io_client_snapshot *)(void *)(base + plan.table);
    snapshots->reader.snapshot = NONE;
    snapshots->reader.slab = NONE;
    snapshots->reader.stream = NONE;
    snapshots->fileops =
        (struct vsr_io_snapshot_fileop *)(void *)(base + plan.fileops);
    snapshots->chunks =
        (struct vsr_io_snapshot_chunk *)(void *)(base + plan.chunks);
    snapshots->chunks_capacity = io_limits->stream_window;
    snapshots->serves =
        (struct vsr_io_snapshot_serve *)(void *)(base + plan.serves);
    snapshots->serves_count = io_limits->streams;
    for (uint32_t i = 0; i < io_limits->streams; ++i) {
        snapshots->serves[i].slot = NONE;
        snapshots->serves[i].fileop = NONE;
    }
    snapshots->dir_slot = NONE;
    snapshots->dir_fileop = NONE;
    snapshots->max_clients = max_clients;
    snapshots->region_bytes = plan.region_bytes;
    snapshots->template_region = base + plan.regions;
}

/* -------------------------------------------------------------------------
 * Registry, paths, completions
 * ---------------------------------------------------------------------- */

static struct vsr_io_snapshot *entry_find(struct vsr_io_snapshots *s,
                                          struct vsr_id id)
{
    if (id_zero(id)) {
        return NULL;
    }
    for (uint32_t i = 0; i < s->count; ++i) {
        if (s->entries[i].state != VSR_IO_SNAPSHOT_FREE &&
            id_equal(s->entries[i].id, id)) {
            return &s->entries[i];
        }
    }
    return NULL;
}

static uint32_t entry_index(const struct vsr_io_snapshots *s,
                            const struct vsr_io_snapshot *entry)
{
    return (uint32_t)(entry - s->entries);
}

static struct vsr_io_snapshot *entry_take(struct vsr_io_snapshots *s,
                                          struct vsr_id id, uint32_t state)
{
    for (uint32_t i = 0; i < s->count; ++i) {
        struct vsr_io_snapshot *entry = &s->entries[i];

        if (entry->state == VSR_IO_SNAPSHOT_FREE) {
            memset(entry, 0, sizeof(*entry));
            entry->id = id;
            entry->state = state;
            entry->caller_status = -1;
            entry->library_status = -1;
            entry->file_slot = -1;
            entry->fileop = NONE;
            entry->tmp_slot = NONE;
            entry->job_next = NONE;
            entry->lease = NONE;
            return entry;
        }
    }
    return NULL;
}

static void entry_free(struct vsr_io_snapshots *s,
                       struct vsr_io_snapshot *entry)
{
    SNAPSHOT_ASSERT(entry->fileop == NONE && entry->file_slot < 0 &&
                    entry->tmp_slot == NONE && entry->lease == NONE &&
                    entry->readers == 0);
    if (s->capture == entry_index(s, entry)) {
        s->capture = NONE;
    }
    if (s->pending_base == entry_index(s, entry)) {
        s->pending_base = NONE;
    }
    memset(entry, 0, sizeof(*entry));
    entry->file_slot = -1;
    entry->fileop = NONE;
    entry->tmp_slot = NONE;
    entry->job_next = NONE;
    entry->lease = NONE;
}

/* Builds "<dir>/clients-<id>[.tmp]" (or "<dir>" alone, "." when empty)
 * into buffer; false when it does not fit. */
static bool path_of(char *buffer, const char *dir, struct vsr_id id, bool file,
                    bool tmp)
{
    size_t length = dir != NULL ? strlen(dir) : 0;
    size_t at = 0;
    char name[VSR_IO_CLIENTS_NAME_BYTES];

    if (length + 1 + sizeof(name) + sizeof(SNAPSHOT_TMP_SUFFIX) >
        VSR_IO_SNAPSHOT_PATH_BYTES) {
        return false;
    }
    if (length > 0) {
        memcpy(buffer, dir, length);
        at = length;
    }
    if (!file) {
        if (at == 0) {
            buffer[at++] = '.';
        }
        buffer[at] = 0;
        return true;
    }
    if (at > 0 && buffer[at - 1] != '/') {
        buffer[at++] = '/';
    }
    vsr_io_codec_clients_name(id, name);
    memcpy(buffer + at, name, sizeof(name));
    at += sizeof(name) - 1;
    if (tmp) {
        memcpy(buffer + at, SNAPSHOT_TMP_SUFFIX, sizeof(SNAPSHOT_TMP_SUFFIX));
    }
    return true;
}

/* Completes the core op in progress on `entry` with `status`, exactly
 * once: an OK CAPTURE or FETCH hands the caller's checkpoint over under
 * the entry's lease, any other outcome releases the lease; the op fields
 * are cleared. */
static void op_complete(struct vsr_io_replica *rep,
                        struct vsr_io_snapshot *entry, int32_t status)
{
    struct vsr_io_completion completion;

    SNAPSHOT_ASSERT(entry->op != 0 && entry->forwarded == 0);
    completion.op = entry->op;
    completion.status = status;
    completion.lease = NONE;
    completion.data = NULL;
    if (status == VSR_IO_OK && entry->result != NULL &&
        (entry->op_type == VSR_OP_SNAPSHOT_CAPTURE ||
         entry->op_type == VSR_OP_SNAPSHOT_FETCH)) {
        completion.lease = entry->lease;
        completion.data = entry->result;
        entry->lease = NONE;
    }
    if (entry->lease != NONE) {
        vsr_io_lease_release(rep, entry->lease);
        entry->lease = NONE;
    }
    entry->result = NULL;
    entry->op = 0;
    entry->op_type = 0;
    entry->task = NULL;
    vsr_io_engine_complete_core(rep, &completion);
}

/* A step waits for a resource (a slab, a slot, a file operation): the
 * replica's CAPTURE deadline makes the loop poll and prepare again. */
static void retry_later(struct vsr_io *io, struct vsr_io_replica *rep)
{
    rep->snapshots.retry = 1;
    vsr_io_deadlines_arm(&io->deadlines, rep->deadline_capture,
                         io->now + SNAPSHOT_RETRY_NS);
}

/* Frees an engine file slot the module holds, clearing its descriptor. */
static void slot_drop(struct vsr_io *io, uint32_t slot)
{
    (void)vsr_io_engine_install(io, slot, -1);
    vsr_io_engine_slot_free(io, slot);
}

/* -------------------------------------------------------------------------
 * Checkpoint copies (the CAPTURE template and the callers' results)
 * ---------------------------------------------------------------------- */

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

/* Deep copy of a checkpoint's graph into `region`: the epoch, the
 * manifest bytes as one span; `out` itself is the caller's. False when it
 * does not fit (a manifest beyond the limits). */
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
        size_t bytes_size;
        unsigned char *bytes;
        struct vsr_span *span;
        size_t at = 0;

        if (in->manifest.size > SIZE_MAX / 2 || in->manifest.spans == NULL) {
            return false;
        }
        bytes_size = (size_t)in->manifest.size;
        bytes = vsr_io_bump_alloc(region, bytes_size, 1);
        span =
            vsr_io_bump_alloc(region, sizeof(*span), alignof(struct vsr_span));
        if (bytes == NULL || span == NULL) {
            return false;
        }
        for (uint32_t i = 0; i < in->manifest.count; ++i) {
            if (in->manifest.spans[i].size > bytes_size - at ||
                (in->manifest.spans[i].size > 0 &&
                 in->manifest.spans[i].data == NULL)) {
                return false;
            }
            memcpy(bytes + at, in->manifest.spans[i].data,
                   in->manifest.spans[i].size);
            at += in->manifest.spans[i].size;
        }
        if (at != bytes_size) {
            return false;
        }
        span->data = bytes;
        span->size = bytes_size;
        out->manifest.spans = span;
        out->manifest.count = 1;
    }
    return true;
}

/* The caller's checkpoint of a CAPTURE or FETCH, deep-copied into the
 * entry's lease region (the checkpoint itself first): false when it does
 * not fit. */
static bool result_copy(struct vsr_io_replica *rep,
                        struct vsr_io_snapshot *entry,
                        const struct vsr_checkpoint *in)
{
    struct vsr_io_bump *region = &rep->leases[entry->lease].region;
    struct vsr_checkpoint *out;

    region->used = 0;
    out =
        vsr_io_bump_alloc(region, sizeof(*out), alignof(struct vsr_checkpoint));
    if (out == NULL || !copy_checkpoint(region, in, out)) {
        region->used = 0;
        return false;
    }
    entry->result = out;
    return true;
}

/* -------------------------------------------------------------------------
 * Clients-file records
 *
 * A record is a 40-byte header, result bytes padded to 8 and a CRC32C of
 * both. The reader bounds the announced length by the limits (and by the
 * bytes left before the trailer when the file size is known) before it
 * checksums anything (docs section 5.4), then verifies the CRC itself.
 * ---------------------------------------------------------------------- */

/* Parses the record at `bytes` (avail bytes of it): OK with the decoded
 * header and its total size, RECORD_MORE when the bytes end inside it,
 * CORRUPT for a bad shape or CRC. `limit` bounds the record's extent
 * (UINT64_MAX when unknown). */
static int32_t record_at(const unsigned char *bytes, uint64_t avail,
                         const struct vsr_limits *limits, uint64_t limit,
                         struct vsr_io_wire_client_record *out, uint64_t *total)
{
    struct vsr_io_cursor cursor;
    uint32_t length;
    uint64_t padded;
    uint32_t crc;

    if (avail < SNAPSHOT_RECORD_HEADER) {
        return RECORD_MORE;
    }
    length = get_u32(bytes + SNAPSHOT_RECORD_LENGTH_AT);
    if (length > limits->result_bytes) {
        return VSR_IO_CORRUPT;
    }
    padded = (uint64_t)length + padding_of(length);
    *total = SNAPSHOT_RECORD_HEADER + padded + sizeof(uint32_t);
    if (*total > limit) {
        return VSR_IO_CORRUPT;
    }
    if (avail < *total) {
        return RECORD_MORE;
    }
    crc = vsr_io_crc32c(0, bytes, (size_t)(SNAPSHOT_RECORD_HEADER + padded));
    if (get_u32(bytes + SNAPSHOT_RECORD_HEADER + padded) != crc) {
        return VSR_IO_CORRUPT;
    }
    vsr_io_cursor_init_one(&cursor, bytes, (size_t)*total);
    if (vsr_io_codec_skip_client_record(&cursor, out) != VSR_OK) {
        return VSR_IO_CORRUPT;
    }
    return VSR_IO_OK;
}

/* Locates the client record of table entry `e` inside a log record's
 * bytes (a CLIENTS transaction, CRC-checked): the raw header and padded
 * result, which the file record repeats before its CRC. */
static int32_t log_client_bytes(const struct vsr_io_store *store,
                                const unsigned char *bytes, uint32_t length,
                                const struct vsr_io_client_snapshot *e,
                                const unsigned char **raw, uint32_t *raw_bytes)
{
    struct vsr_io_cursor cursor;
    struct vsr_io_cursor payload;
    struct vsr_io_cursor copy;
    struct vsr_io_wire_record header;
    struct vsr_io_wire_change change;
    struct vsr_io_wire_client_record wire;
    uint32_t kind = 0;
    uint64_t descriptors;
    size_t at;

    vsr_io_cursor_init_one(&cursor, bytes, length);
    if (vsr_io_codec_get_record(&cursor, length, &header, &kind) != VSR_OK ||
        kind != VSR_IO_SCAN_RECORD || header.length != length ||
        header.sequence != e->record.sequence ||
        e->record.change >= header.count) {
        return VSR_IO_CORRUPT;
    }
    copy = cursor;
    if (!vsr_io_codec_check_record(&copy, &header)) {
        return VSR_IO_CORRUPT;
    }
    descriptors = sizeof(header) +
                  (uint64_t)header.count * sizeof(struct vsr_io_wire_change);
    for (uint32_t i = 0; i <= e->record.change; ++i) {
        if (vsr_io_codec_get_change(&cursor, &change) != VSR_OK ||
            change.offset < descriptors || change.offset > length ||
            change.length > length - change.offset) {
            return VSR_IO_CORRUPT;
        }
    }
    if (change.type != VSR_STORE_CLIENTS || e->record.index >= change.count) {
        return VSR_IO_CORRUPT;
    }
    vsr_io_cursor_init_one(&payload, bytes + change.offset, change.length);
    for (uint32_t i = 0; i < e->record.index; ++i) {
        if (vsr_io_codec_skip_client_record(&payload, &wire) != VSR_OK) {
            return VSR_IO_CORRUPT;
        }
    }
    at = payload.position;
    if (vsr_io_codec_skip_client_record(&payload, &wire) != VSR_OK ||
        wire.client_hi != e->id.hi || wire.client_lo != e->id.lo ||
        wire.number != e->record.number ||
        wire.length > store->limits.result_bytes) {
        return VSR_IO_CORRUPT;
    }
    *raw = bytes + change.offset + at;
    *raw_bytes = (uint32_t)(payload.position - at);
    return VSR_IO_OK;
}

/* -------------------------------------------------------------------------
 * The writer (CAPTURE)
 * ---------------------------------------------------------------------- */

static void writer_release(struct vsr_io *io, struct vsr_io_snapshots *s)
{
    struct vsr_io_clients_writer *w = &s->writer;

    if (w->slab != NONE) {
        vsr_io_pool_release(&io->pool, w->slab);
        w->slab = NONE;
    }
    if (w->cold_slab != NONE) {
        vsr_io_pool_release(&io->pool, w->cold_slab);
        w->cold_slab = NONE;
    }
    w->snapshot = NONE;
}

static void capture_settle(struct vsr_io *io, uint32_t replica,
                           struct vsr_io_snapshot *entry);

/* The library half is over, with `status`; the writer goes idle and the
 * capture floor is released. A failure leaves the file to the discard
 * job that capture_settle starts. */
static void writer_finish(struct vsr_io *io, uint32_t replica, int32_t status)
{
    struct vsr_io_replica *rep = replica_of(io, replica);
    struct vsr_io_snapshots *s = &rep->snapshots;
    struct vsr_io_clients_writer *w = &s->writer;
    struct vsr_io_snapshot *entry = &s->entries[w->snapshot];
    struct vsr_id zero = {0, 0};

    if (status == VSR_IO_OK) {
        entry->state = VSR_IO_SNAPSHOT_WRITTEN;
        entry->bytes = w->file_offset;
        entry->file_slot = (int32_t)entry->tmp_slot;
        entry->tmp_slot = NONE;
        vsr_io_store_capture_end(&rep->store, entry->id, entry->sequence);
    } else {
        vsr_io_store_capture_end(&rep->store, zero, 0);
        if (entry->tmp_slot != NONE) {
            /* Created: the discard job closes it; else never installed. */
            if (entry->on_disk) {
                entry->file_slot = (int32_t)entry->tmp_slot;
            } else {
                vsr_io_engine_slot_free(io, entry->tmp_slot);
            }
            entry->tmp_slot = NONE;
        }
    }
    entry->library_status = status;
    entry->job = VSR_IO_SNAPSHOT_JOB_NONE;
    entry->step = STEP_IDLE;
    writer_release(io, s);
    capture_settle(io, replica, entry);
}

/* Stages entry `next` from the bytes located: the raw record and its CRC.
 * False when the staging slab must be written first. */
static bool writer_put(struct vsr_io *io, struct vsr_io_store *store,
                       struct vsr_io_clients_writer *w,
                       const unsigned char *raw, uint32_t raw_bytes)
{
    unsigned char *slab = vsr_io_pool_slab(&io->pool, w->slab);
    uint32_t total = raw_bytes + (uint32_t)sizeof(uint32_t);

    if ((uint64_t)w->staged + total > io->pool.slab_bytes) {
        return false;
    }
    memcpy(slab + w->staged, raw, raw_bytes);
    put_u32(slab + w->staged + raw_bytes, vsr_io_crc32c(0, raw, raw_bytes));
    vsr_io_store_capture_offset(store, w->table[w->next].id,
                                w->file_offset + w->staged);
    w->staged += total;
    w->next++;
    w->cold_valid = 0;
    return true;
}

/* Locates the bytes of entry `next`: from the cold slab when it holds
 * them, from the ring when hot; else the cold read to issue is set up
 * (STEP_READ) and false returned with status OK. A status other than OK
 * fails the capture. */
static bool writer_locate(struct vsr_io *io, struct vsr_io_replica *rep,
                          struct vsr_io_snapshot *entry,
                          const unsigned char **raw, uint32_t *raw_bytes,
                          int32_t *status)
{
    struct vsr_io_snapshots *s = &rep->snapshots;
    struct vsr_io_clients_writer *w = &s->writer;
    const struct vsr_io_client_snapshot *e = &w->table[w->next];
    const unsigned char *bytes = NULL;
    uint64_t avail = 0;
    struct vsr_io_piece piece;

    *status = VSR_IO_OK;
    if (w->cold_valid) {
        bytes = vsr_io_pool_slab(&io->pool, w->cold_slab) +
                (e->record.offset - w->cold_offset);
        avail = w->cold_bytes - (e->record.offset - w->cold_offset);
    } else if (e->record.sequence != 0 &&
               vsr_io_store_hot(&rep->store, e->record.offset, e->record.length,
                                &piece)) {
        bytes = piece.base;
        avail = piece.length;
    } else {
        if (w->cold_slab == NONE) {
            w->cold_slab = vsr_io_pool_acquire(&io->pool, false);
            if (w->cold_slab == NONE) {
                retry_later(io, rep);
                return false;
            }
        }
        entry->step = STEP_READ;
        return false;
    }
    if (e->record.sequence != 0) {
        *status = log_client_bytes(&rep->store, bytes, e->record.length, e, raw,
                                   raw_bytes);
    } else {
        struct vsr_io_wire_client_record wire;
        uint64_t total = 0;
        int32_t r = record_at(bytes, avail, &rep->store.limits, UINT64_MAX,
                              &wire, &total);

        if (r != VSR_IO_OK || wire.client_hi != e->id.hi ||
            wire.client_lo != e->id.lo || wire.number != e->record.number) {
            *status = VSR_IO_CORRUPT;
        } else {
            *raw = bytes;
            *raw_bytes = (uint32_t)(total - sizeof(uint32_t));
        }
    }
    return *status == VSR_IO_OK;
}

/* Poll-time staging: header, records, trailer; sets the entry's next
 * step (READ, WRITE) when I/O is needed, finishes when nothing remains. */
static void writer_stage(struct vsr_io *io, uint32_t replica)
{
    struct vsr_io_replica *rep = replica_of(io, replica);
    struct vsr_io_snapshots *s = &rep->snapshots;
    struct vsr_io_clients_writer *w = &s->writer;
    struct vsr_io_snapshot *entry = &s->entries[w->snapshot];
    unsigned char *slab = vsr_io_pool_slab(&io->pool, w->slab);

    if (entry->job != VSR_IO_SNAPSHOT_JOB_WRITE || entry->step != STEP_IDLE ||
        entry->fileop != NONE || entry->tmp_slot == NONE) {
        return;
    }
    if (w->aborted) {
        writer_finish(io, replica, VSR_IO_CANCELLED);
        return;
    }
    if (w->staged == 0 && w->file_offset == 0) {
        struct vsr_io_wire_clients_header header;

        memset(&header, 0, sizeof(header));
        header.cluster_hi = rep->options.cluster.hi;
        header.cluster_lo = rep->options.cluster.lo;
        header.snapshot_hi = entry->id.hi;
        header.snapshot_lo = entry->id.lo;
        header.op = w->header_op;
        header.sequence = w->header_sequence;
        header.count = w->count;
        vsr_io_codec_put_clients_header(&header, slab);
        w->staged = SNAPSHOT_HEADER_BYTES;
    }
    while (w->next < w->count) {
        const unsigned char *raw = NULL;
        uint32_t raw_bytes = 0;
        int32_t status;

        if (!writer_locate(io, rep, entry, &raw, &raw_bytes, &status)) {
            if (status != VSR_IO_OK) {
                writer_finish(io, replica, status);
            }
            return;
        }
        if (!writer_put(io, &rep->store, w, raw, raw_bytes)) {
            entry->step = STEP_WRITE;
            return;
        }
    }
    if (!w->trailer_staged) {
        if ((uint64_t)w->staged + SNAPSHOT_TRAILER_BYTES >
            io->pool.slab_bytes) {
            entry->step = STEP_WRITE;
            return;
        }
        vsr_io_codec_put_clients_trailer(w->count, slab + w->staged);
        w->staged += SNAPSHOT_TRAILER_BYTES;
        w->trailer_staged = 1;
    }
    if (w->staged > 0) {
        entry->step = STEP_WRITE;
        return;
    }
    writer_finish(io, replica, VSR_IO_OK);
}

/* Completion of a writer record. */
static void writer_done(struct vsr_io *io, uint32_t replica, uint32_t step,
                        int32_t result)
{
    struct vsr_io_replica *rep = replica_of(io, replica);
    struct vsr_io_snapshots *s = &rep->snapshots;
    struct vsr_io_clients_writer *w = &s->writer;
    struct vsr_io_snapshot *entry = &s->entries[w->snapshot];
    const struct vsr_io_client_snapshot *e;

    entry->step = STEP_IDLE;
    switch (step) {
    case STEP_OPEN:
        if (result < 0) {
            s->error = result;
            writer_finish(io, replica, VSR_IO_FAILED);
            return;
        }
        entry->on_disk = 1; /* The slot holds the file from here. */
        return;
    case STEP_WRITE:
        if (result < 0 || (uint32_t)result != w->staged) {
            s->error = result < 0 ? result : -EIO;
            writer_finish(io, replica, VSR_IO_FAILED);
            return;
        }
        w->file_offset += w->staged;
        w->staged = 0;
        return;
    case STEP_READ:
        e = &w->table[w->next];
        if (result < 0 || (uint32_t)result > io->pool.slab_bytes) {
            s->error = result < 0 ? result : -EIO;
            writer_finish(io, replica, VSR_IO_FAILED);
            return;
        }
        if (e->record.sequence != 0
                ? (uint64_t)result <
                      (e->record.offset - w->cold_offset) + e->record.length
                : (uint64_t)result < SNAPSHOT_RECORD_HEADER) {
            writer_finish(io, replica, VSR_IO_CORRUPT);
            return;
        }
        w->cold_bytes = (uint32_t)result;
        w->cold_valid = 1;
        return;
    default:
        SNAPSHOT_ASSERT(false);
        return;
    }
}

/* -------------------------------------------------------------------------
 * The reader (base loads, fetch verification)
 * ---------------------------------------------------------------------- */

static void reader_release(struct vsr_io *io, struct vsr_io_snapshots *s)
{
    struct vsr_io_clients_reader *r = &s->reader;

    if (r->slab != NONE) {
        vsr_io_pool_release(&io->pool, r->slab);
        r->slab = NONE;
    }
    r->snapshot = NONE;
    r->stream = NONE;
}

static void reader_fail(struct vsr_io_clients_reader *r, int32_t status)
{
    r->stage = STAGE_FAILED;
    r->status = status;
}

/* Parses what the slab holds; stops when more bytes are needed, at the
 * end, or on a failure (r->status). */
static void reader_parse(struct vsr_io *io, struct vsr_io_replica *rep,
                         struct vsr_io_clients_reader *r)
{
    struct vsr_io_snapshots *s = &rep->snapshots;
    const struct vsr_io_snapshot *entry = &s->entries[r->snapshot];
    unsigned char *slab = vsr_io_pool_slab(&io->pool, r->slab);

    for (;;) {
        const unsigned char *at = slab + r->consumed;
        uint64_t avail = r->filled - r->consumed;
        uint64_t position = r->file_offset + r->consumed;

        if (r->stage == STAGE_HEADER) {
            struct vsr_io_cursor cursor;
            struct vsr_io_wire_clients_header header;

            if (avail < SNAPSHOT_HEADER_BYTES) {
                return;
            }
            vsr_io_cursor_init_one(&cursor, at, SNAPSHOT_HEADER_BYTES);
            if (vsr_io_codec_get_clients_header(&cursor, &header) != VSR_OK ||
                header.snapshot_hi != entry->id.hi ||
                header.snapshot_lo != entry->id.lo ||
                header.cluster_hi != rep->options.cluster.hi ||
                header.cluster_lo != rep->options.cluster.lo ||
                header.count > s->max_clients) {
                reader_fail(r, VSR_IO_CORRUPT);
                return;
            }
            r->expected = header.count;
            r->consumed += SNAPSHOT_HEADER_BYTES;
            r->stage = STAGE_RECORDS;
        } else if (r->stage == STAGE_RECORDS) {
            struct vsr_io_wire_client_record wire;
            uint64_t total = 0;
            uint64_t limit = UINT64_MAX;
            int32_t rc;

            if (r->seen == r->expected) {
                r->stage = STAGE_TRAILER;
                continue;
            }
            if (r->file_size != UINT64_MAX) {
                if (r->file_size < position + SNAPSHOT_TRAILER_BYTES) {
                    reader_fail(r, VSR_IO_CORRUPT);
                    return;
                }
                limit = r->file_size - SNAPSHOT_TRAILER_BYTES - position;
            }
            rc = record_at(at, avail, &rep->store.limits, limit, &wire, &total);
            if (rc == RECORD_MORE) {
                return;
            }
            if (rc != VSR_IO_OK) {
                reader_fail(r, rc);
                return;
            }
            if (r->purpose == PURPOSE_LOAD) {
                int b = vsr_io_store_base_record(&rep->store, &wire, position);

                if (b != VSR_OK) {
                    reader_fail(r, b == VSR_ELIMIT ? VSR_IO_FAILED
                                                   : VSR_IO_CORRUPT);
                    return;
                }
            }
            r->seen++;
            r->consumed += (uint32_t)total;
        } else if (r->stage == STAGE_TRAILER) {
            struct vsr_io_cursor cursor;
            uint32_t count = 0;

            if (avail < SNAPSHOT_TRAILER_BYTES) {
                return;
            }
            vsr_io_cursor_init_one(&cursor, at, SNAPSHOT_TRAILER_BYTES);
            if (vsr_io_codec_get_clients_trailer(&cursor, &count) != VSR_OK ||
                count != r->expected) {
                reader_fail(r, VSR_IO_CORRUPT);
                return;
            }
            r->consumed += SNAPSHOT_TRAILER_BYTES;
            r->stage = STAGE_DONE;
        } else {
            /* DONE: nothing may follow the trailer; FAILED: stopped. */
            if (r->stage == STAGE_DONE && avail > 0) {
                reader_fail(r, VSR_IO_CORRUPT);
            }
            return;
        }
    }
}

/* Moves the unparsed tail to the slab's start. */
static void reader_compact(struct vsr_io *io, struct vsr_io_clients_reader *r)
{
    unsigned char *slab = vsr_io_pool_slab(&io->pool, r->slab);

    if (r->consumed == 0) {
        return;
    }
    memmove(slab, slab + r->consumed, r->filled - r->consumed);
    r->file_offset += r->consumed;
    r->filled -= r->consumed;
    r->consumed = 0;
}

/* Feeds `length` bytes that follow the bytes fed so far (FETCH chunks). */
static void reader_feed(struct vsr_io *io, struct vsr_io_replica *rep,
                        struct vsr_io_clients_reader *r,
                        const unsigned char *data, uint32_t length)
{
    unsigned char *slab = vsr_io_pool_slab(&io->pool, r->slab);

    while (length > 0 && r->stage != STAGE_FAILED) {
        uint32_t room = io->pool.slab_bytes - r->filled;
        uint32_t take;

        if (room == 0) {
            reader_compact(io, r);
            room = io->pool.slab_bytes - r->filled;
            if (room == 0) {
                reader_fail(r, VSR_IO_CORRUPT); /* A record beyond a slab. */
                return;
            }
        }
        take = length < room ? length : room;
        memcpy(slab + r->filled, data, take);
        r->filled += take;
        data += take;
        length -= take;
        reader_parse(io, rep, r);
        reader_compact(io, r);
    }
}

static void reader_start(struct vsr_io_clients_reader *r, uint32_t snapshot,
                         uint32_t slab, uint32_t purpose, uint64_t sequence)
{
    r->snapshot = snapshot;
    r->slab = slab;
    r->filled = 0;
    r->consumed = 0;
    r->stage = STAGE_HEADER;
    r->purpose = purpose;
    r->file_offset = 0;
    r->file_size = UINT64_MAX;
    r->expected = 0;
    r->seen = 0;
    r->sequence = sequence;
    r->status = VSR_IO_OK;
    r->stream = NONE;
    r->reads = 0;
}

/* -------------------------------------------------------------------------
 * Base loads
 * ---------------------------------------------------------------------- */

static void base_track(struct vsr_io *io, uint32_t replica);

/* The load is over: the store resumes with `status`. On OK the file's
 * slot stays open with the entry: base_track makes it the base slot once
 * the store's client base names the id (at once for a recovery load,
 * whose merge applies in base_end; when the held RESTORE or PUBLISH packs
 * otherwise, the entry being the pending base until then). An entry this
 * load created for an id the registry did not hold is freed unless the
 * file was found. */
static void load_finish(struct vsr_io *io, uint32_t replica, int32_t status)
{
    struct vsr_io_replica *rep = replica_of(io, replica);
    struct vsr_io_snapshots *s = &rep->snapshots;
    struct vsr_io_clients_reader *r = &s->reader;
    struct vsr_io_snapshot *entry = &s->entries[r->snapshot];
    struct vsr_id id = entry->id;
    uint64_t sequence = r->sequence;

    entry->job = VSR_IO_SNAPSHOT_JOB_NONE;
    entry->step = STEP_IDLE;
    if (status == VSR_IO_OK && entry->tmp_slot != NONE) {
        if (entry->file_slot >= 0) {
            if (rep->store.base_slot == entry->file_slot) {
                rep->store.base_slot = (int32_t)entry->tmp_slot;
            }
            slot_drop(io, (uint32_t)entry->file_slot);
        }
        entry->file_slot = (int32_t)entry->tmp_slot;
        entry->tmp_slot = NONE;
        entry->on_disk = 1;
        entry->bytes = r->file_size;
        if (entry->sequence == 0) {
            entry->sequence = sequence;
        }
        s->pending_base = entry_index(s, entry);
    } else {
        if (entry->tmp_slot != NONE) {
            slot_drop(io, entry->tmp_slot);
            entry->tmp_slot = NONE;
        }
        if (!entry->on_disk && entry->op == 0) {
            entry_free(s, entry);
        }
    }
    reader_release(io, s);
    if (status == VSR_IO_OK) {
        vsr_io_store_base_end(&rep->store, id, sequence);
    }
    vsr_io_store_base_resume(&rep->store, status);
    base_track(io, replica);
}

int vsr_io_snapshots_load_base(struct vsr_io *io, uint32_t replica,
                               struct vsr_id id, uint64_t sequence)
{
    struct vsr_io_replica *rep;
    struct vsr_io_snapshots *s;
    struct vsr_io_snapshot *entry;
    uint32_t slab;
    uint32_t slot;

    if (io == NULL || replica >= io->options.limits.replicas || id_zero(id)) {
        return VSR_EINVAL;
    }
    rep = replica_of(io, replica);
    s = &rep->snapshots;
    if (s->closed || s->reader.snapshot != NONE) {
        return VSR_IO_RETRY;
    }
    entry = entry_find(s, id);
    if (entry != NULL && entry->job != VSR_IO_SNAPSHOT_JOB_NONE) {
        return VSR_IO_RETRY;
    }
    if (entry != NULL && (entry->state == VSR_IO_SNAPSHOT_WRITING ||
                          entry->state == VSR_IO_SNAPSHOT_FETCHING ||
                          entry->state == VSR_IO_SNAPSHOT_DROPPING)) {
        return VSR_IO_RETRY;
    }
    slab = vsr_io_pool_acquire(&io->pool, false);
    if (slab == NONE) {
        retry_later(io, rep);
        return VSR_IO_RETRY;
    }
    slot = vsr_io_engine_slot_alloc(io);
    if (slot == NONE) {
        vsr_io_pool_release(&io->pool, slab);
        retry_later(io, rep);
        return VSR_IO_RETRY;
    }
    if (entry == NULL) {
        entry = entry_take(s, id, VSR_IO_SNAPSHOT_DURABLE);
        if (entry == NULL) {
            vsr_io_pool_release(&io->pool, slab);
            vsr_io_engine_slot_free(io, slot);
            retry_later(io, rep);
            return VSR_IO_RETRY;
        }
        entry->library_status = VSR_IO_OK;
    }
    entry->job = VSR_IO_SNAPSHOT_JOB_LOAD;
    entry->step = STEP_OPEN;
    entry->tmp_slot = slot;
    reader_start(&s->reader, entry_index(s, entry), slab, PURPOSE_LOAD,
                 sequence);
    vsr_io_store_base_begin(&rep->store);
    return VSR_OK;
}

/* Completion of a load record. */
static void load_done(struct vsr_io *io, uint32_t replica, uint32_t step,
                      int32_t result)
{
    struct vsr_io_replica *rep = replica_of(io, replica);
    struct vsr_io_snapshots *s = &rep->snapshots;
    struct vsr_io_clients_reader *r = &s->reader;
    struct vsr_io_snapshot *entry = &s->entries[r->snapshot];

    entry->step = STEP_IDLE;
    switch (step) {
    case STEP_OPEN:
        if (result < 0) {
            int32_t status = VSR_IO_FAILED;

            if (result == -ENOENT) {
                /* A witness keeps no table: an empty base. */
                status = rep->store.hard.role == VSR_MEMBER_WITNESS
                             ? VSR_IO_OK
                             : VSR_IO_CORRUPT;
            } else {
                s->error = result;
            }
            vsr_io_engine_slot_free(io, entry->tmp_slot);
            entry->tmp_slot = NONE;
            r->file_size = 0;
            load_finish(io, replica, status);
            return;
        }
        entry->step = STEP_STAT;
        return;
    case STEP_STAT:
        if (result < 0) {
            s->error = result;
            entry->step = STEP_CLOSE;
            reader_fail(r, VSR_IO_FAILED);
            return;
        }
        r->file_size = ((const struct statx *)(const void *)r->stat)->stx_size;
        entry->step = STEP_READ;
        return;
    case STEP_READ:
        r->reads = 0;
        if (result < 0 || (uint32_t)result > io->pool.slab_bytes - r->filled) {
            s->error = result < 0 ? result : -EIO;
            reader_fail(r, VSR_IO_FAILED);
        } else if (result == 0) {
            reader_fail(r, VSR_IO_CORRUPT); /* Shorter than its size. */
        } else {
            r->filled += (uint32_t)result;
            reader_parse(io, rep, r);
            reader_compact(io, r);
        }
        if (r->stage == STAGE_FAILED) {
            entry->step = STEP_CLOSE;
            return;
        }
        if (r->file_offset + r->filled >= r->file_size) {
            if (r->stage != STAGE_DONE) {
                reader_fail(r, VSR_IO_CORRUPT);
                entry->step = STEP_CLOSE;
                return;
            }
            load_finish(io, replica, VSR_IO_OK);
            return;
        }
        entry->step = STEP_READ;
        return;
    case STEP_CLOSE:
        slot_drop(io, entry->tmp_slot);
        entry->tmp_slot = NONE;
        load_finish(io, replica, r->status);
        return;
    default:
        SNAPSHOT_ASSERT(false);
        return;
    }
}

/* -------------------------------------------------------------------------
 * Fetch (the requester side of a library stream)
 * ---------------------------------------------------------------------- */

static void fetch_settle(struct vsr_io *io, uint32_t replica,
                         struct vsr_io_snapshot *entry);

/* Completes the head chunk: its bytes are no longer needed. */
static void chunk_pop(struct vsr_io *io, struct vsr_io_snapshots *s)
{
    struct vsr_io_snapshot_chunk *chunk = &s->chunks[s->chunks_head];

    SNAPSHOT_ASSERT(s->chunks_count > 0 && !s->chunk_writing);
    (void)vsr_io_streams_data_done(io, chunk->op);
    s->chunks_head = (s->chunks_head + 1) % s->chunks_capacity;
    s->chunks_count--;
}

/* Completes every held chunk at once (the file is no longer written);
 * the head's write, when one is out, completes first. */
static void chunks_flush(struct vsr_io *io, struct vsr_io_snapshots *s)
{
    while (s->chunks_count > 0 && !s->chunk_writing) {
        chunk_pop(io, s);
    }
}

/* The library half of a FETCH is over with `status` (OK: the file is in
 * place). */
static void fetch_finish(struct vsr_io *io, uint32_t replica, int32_t status)
{
    struct vsr_io_replica *rep = replica_of(io, replica);
    struct vsr_io_snapshots *s = &rep->snapshots;
    struct vsr_io_snapshot *entry = &s->entries[s->reader.snapshot];

    SNAPSHOT_ASSERT(s->chunks_count == 0 && !s->chunk_writing);
    entry->job = VSR_IO_SNAPSHOT_JOB_NONE;
    entry->step = STEP_IDLE;
    entry->library_status = status;
    if (entry->tmp_slot != NONE) {
        slot_drop(io, entry->tmp_slot);
        entry->tmp_slot = NONE;
    }
    reader_release(io, s);
    if (status == VSR_IO_OK) {
        entry->state = VSR_IO_SNAPSHOT_WRITTEN;
        entry->on_disk = 1;
        entry->forward_due = 1;
        return;
    }
    /* The caller never saw the op: the core hears the library's status. */
    op_complete(rep, entry, status);
    entry_free(s, entry);
}

/* The stream ended and no record is in flight: decide the outcome. */
static void fetch_end_decide(struct vsr_io *io, uint32_t replica)
{
    struct vsr_io_replica *rep = replica_of(io, replica);
    struct vsr_io_snapshots *s = &rep->snapshots;
    struct vsr_io_clients_reader *r = &s->reader;
    struct vsr_io_snapshot *entry = &s->entries[r->snapshot];
    bool ok = r->status == VSR_IO_OK && r->stage == STAGE_DONE &&
              r->reads == 0; /* reads: the file open succeeded. */

    if (r->status == VSR_IO_OK && r->stage != STAGE_DONE) {
        r->status = VSR_IO_CORRUPT; /* Ended OK short of the trailer. */
    }
    if (ok) {
        entry->bytes = r->file_offset + r->filled;
        entry->step = STEP_CLOSE;
        return;
    }
    if (entry->tmp_slot != NONE && r->reads == 0) {
        entry->step = STEP_CLOSE; /* Then unlink. */
        return;
    }
    fetch_finish(io, replica, r->status);
}

int vsr_io_snapshots_fetch(struct vsr_io *io, uint32_t replica, uint64_t op,
                           const struct vsr_snapshot_task *task)
{
    struct vsr_io_replica *rep;
    struct vsr_io_snapshots *s;
    struct vsr_io_snapshot *entry;
    struct vsr_io_wire_library_request request;
    struct vsr_io_stream_open open;
    uint64_t node;
    uint32_t slab;
    uint32_t slot;
    uint32_t lease;
    uint32_t index = NONE;
    struct vsr_id id;

    if (io == NULL || replica >= io->options.limits.replicas || op == 0 ||
        task == NULL) {
        return VSR_EINVAL;
    }
    if (task->checkpoint == NULL || id_zero(task->checkpoint->id)) {
        return VSR_IO_FAILED;
    }
    rep = replica_of(io, replica);
    s = &rep->snapshots;
    id = task->checkpoint->id;
    if (s->closed) {
        return VSR_IO_RETRY;
    }
    entry = entry_find(s, id);
    if (entry != NULL) {
        /* Already held locally: nothing to transfer. */
        if (entry->op != 0 || entry->state == VSR_IO_SNAPSHOT_WRITING ||
            entry->state == VSR_IO_SNAPSHOT_FETCHING ||
            entry->state == VSR_IO_SNAPSHOT_DROPPING || !entry->on_disk) {
            return VSR_IO_RETRY;
        }
        lease = vsr_io_lease_alloc(rep, NONE, NONE);
        if (lease == NONE) {
            return VSR_IO_RETRY;
        }
        entry->lease = lease;
        entry->op = op;
        entry->op_type = VSR_OP_SNAPSHOT_FETCH;
        entry->task = task;
        entry->caller_status = -1;
        entry->library_status = VSR_IO_OK;
        entry->forward_due = 1;
        return VSR_OK;
    }
    if (s->reader.snapshot != NONE) {
        return VSR_IO_RETRY;
    }
    node = vsr_io_links_lookup(&io->links, rep->options.cluster, task->peer);
    if (node == VSR_IO_NO_NODE) {
        return VSR_IO_RETRY;
    }
    lease = vsr_io_lease_alloc(rep, NONE, NONE);
    if (lease == NONE) {
        return VSR_IO_RETRY;
    }
    slab = vsr_io_pool_acquire(&io->pool, false);
    if (slab == NONE) {
        vsr_io_lease_release(rep, lease);
        return VSR_IO_RETRY;
    }
    slot = vsr_io_engine_slot_alloc(io);
    if (slot == NONE) {
        vsr_io_pool_release(&io->pool, slab);
        vsr_io_lease_release(rep, lease);
        return VSR_IO_RETRY;
    }
    entry = entry_take(s, id, VSR_IO_SNAPSHOT_FETCHING);
    if (entry == NULL) {
        vsr_io_pool_release(&io->pool, slab);
        vsr_io_engine_slot_free(io, slot);
        vsr_io_lease_release(rep, lease);
        return VSR_IO_RETRY;
    }
    memset(&request, 0, sizeof(request));
    request.magic = VSR_IO_LIBRARY_MAGIC;
    request.version = VSR_IO_LIBRARY_REQUEST_VERSION;
    request.kind = VSR_IO_LIBRARY_CLIENTS;
    request.cluster_hi = rep->options.cluster.hi;
    request.cluster_lo = rep->options.cluster.lo;
    request.replica = task->peer;
    request.snapshot_hi = id.hi;
    request.snapshot_lo = id.lo;
    vsr_io_codec_put_library_request(s->request, &request);
    memset(&open, 0, sizeof(open));
    open.node = node;
    open.request.data = s->request;
    open.request.size = sizeof(s->request);
    if (vsr_io_streams_open(io, op, &open, 0, VSR_IO_STREAM_LIBRARY, &index) !=
        VSR_OK) {
        entry_free(s, entry);
        vsr_io_pool_release(&io->pool, slab);
        vsr_io_engine_slot_free(io, slot);
        vsr_io_lease_release(rep, lease);
        return VSR_IO_RETRY;
    }
    io->streams.streams[index].replica = replica;
    entry->lease = lease;
    entry->op = op;
    entry->op_type = VSR_OP_SNAPSHOT_FETCH;
    entry->task = task;
    entry->job = VSR_IO_SNAPSHOT_JOB_FETCH;
    entry->step = STEP_OPEN;
    entry->tmp_slot = slot;
    reader_start(&s->reader, entry_index(s, entry), slab, PURPOSE_FETCH, 0);
    s->reader.stream = index;
    s->reader.reads = 1; /* Cleared once the file is open. */
    s->chunks_head = 0;
    s->chunks_count = 0;
    s->chunk_writing = 0;
    return VSR_OK;
}

void vsr_io_snapshots_stream_data(struct vsr_io *io, uint32_t replica,
                                  uint32_t stream, uint64_t op, uint64_t offset,
                                  const struct vsr_span *bytes, uint32_t slab)
{
    struct vsr_io_replica *rep;
    struct vsr_io_snapshots *s;
    struct vsr_io_clients_reader *r;
    struct vsr_io_snapshot_chunk *chunk;

    if (io == NULL || replica >= io->options.limits.replicas) {
        return;
    }
    rep = replica_of(io, replica);
    s = &rep->snapshots;
    r = &s->reader;
    if (r->snapshot == NONE || r->purpose != PURPOSE_FETCH ||
        r->stream != stream || bytes == NULL || r->stage == STAGE_FAILED ||
        s->chunks_count == s->chunks_capacity ||
        offset != r->file_offset + r->filled || bytes->size > UINT32_MAX) {
        if (r->snapshot != NONE && r->stream == stream &&
            r->stage != STAGE_FAILED) {
            reader_fail(r, VSR_IO_FAILED); /* Out of order: protocol. */
        }
        (void)vsr_io_streams_data_done(io, op);
        return;
    }
    chunk = &s->chunks[(s->chunks_head + s->chunks_count) % s->chunks_capacity];
    chunk->op = op;
    chunk->offset = offset;
    chunk->data = bytes->data;
    chunk->length = (uint32_t)bytes->size;
    chunk->slab = slab;
    s->chunks_count++;
    reader_feed(io, rep, r, bytes->data, chunk->length);
    if (r->stage == STAGE_FAILED) {
        chunks_flush(io, s);
    }
}

/* Completion of a fetch record. */
static void fetch_done(struct vsr_io *io, uint32_t replica, uint32_t step,
                       int32_t result)
{
    struct vsr_io_replica *rep = replica_of(io, replica);
    struct vsr_io_snapshots *s = &rep->snapshots;
    struct vsr_io_clients_reader *r = &s->reader;
    struct vsr_io_snapshot *entry = &s->entries[r->snapshot];
    struct vsr_io_snapshot_chunk *chunk;

    entry->step = STEP_IDLE;
    switch (step) {
    case STEP_OPEN:
        if (result < 0) {
            s->error = result;
            vsr_io_engine_slot_free(io, entry->tmp_slot);
            entry->tmp_slot = NONE;
            reader_fail(r, VSR_IO_FAILED);
            chunks_flush(io, s);
        } else {
            entry->on_disk = 1; /* The temporary file. */
            r->reads = 0;
        }
        if (r->stream == NONE) {
            fetch_end_decide(io, replica);
        }
        return;
    case STEP_WRITE:
        chunk = &s->chunks[s->chunks_head];
        s->chunk_writing = 0;
        if (result < 0 || (uint32_t)result != chunk->length) {
            s->error = result < 0 ? result : -EIO;
            reader_fail(r, VSR_IO_FAILED);
        }
        /* The head chunk is on file; the others wait for their writes
         * unless the file is no longer written. */
        chunk_pop(io, s);
        if (r->stage == STAGE_FAILED) {
            chunks_flush(io, s);
        }
        if (r->stream == NONE && s->chunks_count == 0) {
            fetch_end_decide(io, replica);
        }
        return;
    case STEP_CLOSE:
        slot_drop(io, entry->tmp_slot);
        entry->tmp_slot = NONE;
        if (r->status == VSR_IO_OK && r->stage == STAGE_DONE) {
            entry->step = STEP_RENAME;
        } else {
            entry->step = STEP_UNLINK;
        }
        return;
    case STEP_RENAME:
        if (result < 0) {
            s->error = result;
            r->status = VSR_IO_FAILED;
            entry->step = STEP_UNLINK;
            return;
        }
        fetch_finish(io, replica, VSR_IO_OK);
        return;
    case STEP_UNLINK:
        entry->on_disk = 0;
        fetch_finish(io, replica, r->status);
        return;
    default:
        SNAPSHOT_ASSERT(false);
        return;
    }
}

/* -------------------------------------------------------------------------
 * Serving (the source side of a library stream)
 * ---------------------------------------------------------------------- */

int vsr_io_snapshots_serve(struct vsr_io *io, uint32_t stream,
                           const struct vsr_io_wire_library_request *request)
{
    struct vsr_io_replica *rep;
    struct vsr_io_snapshots *s;
    struct vsr_io_snapshot *entry;
    struct vsr_io_snapshot_serve *serve;
    struct vsr_id cluster;
    struct vsr_id id;

    if (io == NULL || request == NULL || stream >= io->streams.count) {
        return VSR_IO_FAILED;
    }
    if (request->version != VSR_IO_LIBRARY_REQUEST_VERSION ||
        request->kind != VSR_IO_LIBRARY_CLIENTS) {
        return VSR_IO_NOT_FOUND;
    }
    cluster.hi = request->cluster_hi;
    cluster.lo = request->cluster_lo;
    rep = vsr_io_engine_replica(io, cluster);
    if (rep == NULL || rep->options.replica != request->replica) {
        return VSR_IO_NOT_FOUND;
    }
    s = &rep->snapshots;
    id.hi = request->snapshot_hi;
    id.lo = request->snapshot_lo;
    entry = entry_find(s, id);
    /* A file is served once complete and published to the core: not while
     * its CAPTURE is outstanding (a failed caller half discards it). */
    if (s->closed || entry == NULL ||
        (entry->state != VSR_IO_SNAPSHOT_WRITTEN &&
         entry->state != VSR_IO_SNAPSHOT_SYNCING &&
         entry->state != VSR_IO_SNAPSHOT_DURABLE) ||
        entry->job == VSR_IO_SNAPSHOT_JOB_DISCARD || !entry->on_disk ||
        (entry->op != 0 && entry->op_type == VSR_OP_SNAPSHOT_CAPTURE)) {
        return VSR_IO_NOT_FOUND;
    }
    if (stream >= s->serves_count) {
        return VSR_IO_FAILED;
    }
    serve = &s->serves[stream];
    if (serve->state != SERVE_FREE) {
        return VSR_IO_FAILED;
    }
    memset(serve, 0, sizeof(*serve));
    serve->state = SERVE_OPENING;
    serve->snapshot = entry_index(s, entry);
    serve->slot = NONE;
    serve->fileop = NONE;
    entry->readers++;
    io->streams.streams[stream].replica = rep->index;
    return VSR_OK;
}

static void serve_free(struct vsr_io_snapshots *s,
                       struct vsr_io_snapshot_serve *serve)
{
    struct vsr_io_snapshot *entry = &s->entries[serve->snapshot];

    SNAPSHOT_ASSERT(entry->readers > 0);
    entry->readers--;
    memset(serve, 0, sizeof(*serve));
    serve->slot = NONE;
    serve->fileop = NONE;
}

/* Completion of a serve record (the open or the close). */
static void serve_done(struct vsr_io *io, uint32_t replica, uint32_t stream,
                       uint32_t step, int32_t result)
{
    struct vsr_io_snapshots *s = snapshots_of(io, replica);
    struct vsr_io_snapshot_serve *serve = &s->serves[stream];
    const struct vsr_io_snapshot *entry = &s->entries[serve->snapshot];
    uint64_t handle = vsr_io_streams_handle(&io->streams, stream);

    if (step == STEP_CLOSE) {
        vsr_io_engine_slot_free(io, serve->slot);
        serve->slot = NONE;
        serve_free(s, serve);
        return;
    }
    SNAPSHOT_ASSERT(step == STEP_OPEN);
    if (result < 0) {
        vsr_io_engine_slot_free(io, serve->slot);
        serve->slot = NONE;
        serve->state = SERVE_OPEN;
        if (!serve->ended) {
            /* The handle is the stream's only while it has not ended: an
             * ended stream's index may serve another stream already. */
            (void)vsr_io_streams_close(io, handle,
                                       result == -ENOENT ? VSR_IO_NOT_FOUND
                                                         : VSR_IO_FAILED);
        }
    } else if (serve->ended) {
        serve->state = SERVE_CLOSING;
    } else {
        struct vsr_io_stream_write write;

        memset(&write, 0, sizeof(write));
        write.stream = handle;
        write.write = 1;
        write.kind = VSR_IO_WRITE_FILE;
        write.slot = serve->slot;
        write.offset = 0;
        write.length = entry->bytes;
        serve->state = SERVE_OPEN;
        if (vsr_io_streams_write(io, &write, 0) == VSR_OK) {
            (void)vsr_io_streams_close(io, handle, VSR_IO_OK);
        } else {
            (void)vsr_io_streams_close(io, handle, VSR_IO_FAILED);
        }
    }
    if (serve->ended && serve->state != SERVE_CLOSING) {
        /* The end came while the open was out and nothing is open. */
        serve_free(s, serve);
    }
}

void vsr_io_snapshots_stream_end(struct vsr_io *io, uint32_t replica,
                                 uint32_t stream, int32_t status)
{
    struct vsr_io_replica *rep;
    struct vsr_io_snapshots *s;
    struct vsr_io_clients_reader *r;

    if (io == NULL || replica >= io->options.limits.replicas) {
        return;
    }
    rep = replica_of(io, replica);
    s = &rep->snapshots;
    r = &s->reader;
    if (r->snapshot != NONE && r->purpose == PURPOSE_FETCH &&
        r->stream == stream) {
        /* The requester: every chunk was completed already. */
        r->stream = NONE;
        if (status != VSR_IO_OK && r->status == VSR_IO_OK) {
            r->status = status;
            r->stage = STAGE_FAILED;
        }
        if (s->entries[r->snapshot].fileop == NONE && s->chunks_count == 0) {
            fetch_end_decide(io, replica);
        }
        return;
    }
    if (stream < s->serves_count && s->serves[stream].state != SERVE_FREE) {
        struct vsr_io_snapshot_serve *serve = &s->serves[stream];

        serve->ended = 1;
        if (serve->state == SERVE_OPENING && serve->fileop != NONE) {
            return; /* The open completes first. */
        }
        if (serve->state == SERVE_OPENING && serve->slot != NONE) {
            /* The open was never issued: the slot holds nothing. */
            vsr_io_engine_slot_free(io, serve->slot);
            serve->slot = NONE;
        }
        if (serve->slot != NONE) {
            serve->state = SERVE_CLOSING;
            return;
        }
        serve_free(s, serve);
    }
}

/* -------------------------------------------------------------------------
 * Joint ops
 * ---------------------------------------------------------------------- */

/* Starts the job that removes a failed file (a capture or fetch whose
 * other half failed): close the kept slot, unlink. */
static void discard_start(struct vsr_io_snapshot *entry)
{
    SNAPSHOT_ASSERT(entry->readers == 0 &&
                    entry->job == VSR_IO_SNAPSHOT_JOB_NONE);
    entry->job = VSR_IO_SNAPSHOT_JOB_DISCARD;
    entry->step = entry->file_slot >= 0 ? STEP_CLOSE : STEP_UNLINK;
}

static void capture_settle(struct vsr_io *io, uint32_t replica,
                           struct vsr_io_snapshot *entry)
{
    struct vsr_io_replica *rep = replica_of(io, replica);
    struct vsr_io_snapshots *s = &rep->snapshots;
    int32_t status;

    if (entry->caller_status == -1 || entry->library_status == -1 ||
        entry->job != VSR_IO_SNAPSHOT_JOB_NONE) {
        return;
    }
    if (entry->caller_status == VSR_IO_OK &&
        entry->library_status == VSR_IO_OK) {
        op_complete(rep, entry, VSR_IO_OK);
        s->capture = NONE;
        return;
    }
    if (entry->on_disk) {
        discard_start(entry);
        return;
    }
    status = entry->caller_status != VSR_IO_OK ? entry->caller_status
                                               : entry->library_status;
    op_complete(rep, entry, status);
    entry_free(s, entry);
}

static void fetch_settle(struct vsr_io *io, uint32_t replica,
                         struct vsr_io_snapshot *entry)
{
    struct vsr_io_replica *rep = replica_of(io, replica);
    struct vsr_io_snapshots *s = &rep->snapshots;

    /* Only the op's own jobs are waited for: a FETCH of a held file may
     * find housekeeping (the RELEASE of its kept slot) under way, which
     * calls no settle when it ends. */
    if (entry->caller_status == -1 || entry->library_status == -1 ||
        entry->job == VSR_IO_SNAPSHOT_JOB_FETCH ||
        entry->job == VSR_IO_SNAPSHOT_JOB_DISCARD) {
        return;
    }
    if (entry->caller_status == VSR_IO_OK) {
        op_complete(rep, entry, VSR_IO_OK);
        return;
    }
    /* A fetched file the caller failed on is a private partial object
     * (vsr.h), unless the store adopted it meanwhile. */
    if (entry->on_disk && entry->sequence == 0) {
        if (entry->readers == 0 && entry->job == VSR_IO_SNAPSHOT_JOB_NONE) {
            discard_start(entry);
            return;
        }
        entry->state = VSR_IO_SNAPSHOT_DROPPING;
        entry->unlink_due = 1;
    }
    op_complete(rep, entry, entry->caller_status);
    if (!entry->on_disk) {
        entry_free(s, entry);
    }
}

static void sync_settle(struct vsr_io *io, uint32_t replica,
                        struct vsr_io_snapshot *entry)
{
    struct vsr_io_replica *rep = replica_of(io, replica);
    int32_t status;

    if (entry->caller_status == -1 || entry->library_status == -1 ||
        entry->job != VSR_IO_SNAPSHOT_JOB_NONE) {
        return;
    }
    status = entry->caller_status != VSR_IO_OK ? entry->caller_status
                                               : entry->library_status;
    entry->state =
        status == VSR_IO_OK ? VSR_IO_SNAPSHOT_DURABLE : VSR_IO_SNAPSHOT_WRITTEN;
    op_complete(rep, entry, status);
}

static void drop_settle(struct vsr_io *io, uint32_t replica,
                        struct vsr_io_snapshot *entry)
{
    struct vsr_io_replica *rep = replica_of(io, replica);

    if (entry->caller_status == -1) {
        return;
    }
    if (entry->caller_status != VSR_IO_OK) {
        op_complete(rep, entry, entry->caller_status);
        if (!entry->on_disk && entry->file_slot < 0 &&
            entry->job == VSR_IO_SNAPSHOT_JOB_NONE) {
            entry_free(&rep->snapshots, entry); /* An unknown id's entry. */
        }
        return;
    }
    entry->state = VSR_IO_SNAPSHOT_DROPPING;
    entry->unlink_due = 1;
    if (entry->readers > 0) {
        /* A served stream still reads it: the unlink waits, the core
         * does not. */
        op_complete(rep, entry, VSR_IO_OK);
    }
}

int vsr_io_snapshots_capture(struct vsr_io *io, uint32_t replica, uint64_t op,
                             const struct vsr_snapshot_task *task)
{
    struct vsr_io_replica *rep;
    struct vsr_io_snapshots *s;
    struct vsr_io_snapshot *entry;
    struct vsr_io_clients_writer *w;
    struct vsr_io_bump template;
    struct vsr_id id;
    uint32_t slab;
    uint32_t slot;
    uint32_t lease;
    uint32_t count;

    if (io == NULL || replica >= io->options.limits.replicas || op == 0 ||
        task == NULL) {
        return VSR_EINVAL;
    }
    rep = replica_of(io, replica);
    s = &rep->snapshots;
    w = &s->writer;
    if (task->checkpoint == NULL ||
        rep->store.clients_sequence > task->sequence) {
        return VSR_IO_FAILED;
    }
    if (s->closed || w->snapshot != NONE || s->capture != NONE) {
        return VSR_IO_RETRY;
    }
    lease = vsr_io_lease_alloc(rep, NONE, NONE);
    if (lease == NONE) {
        return VSR_IO_RETRY;
    }
    slab = vsr_io_pool_acquire(&io->pool, false);
    if (slab == NONE) {
        vsr_io_lease_release(rep, lease);
        return VSR_IO_RETRY;
    }
    slot = vsr_io_engine_slot_alloc(io);
    if (slot == NONE) {
        vsr_io_pool_release(&io->pool, slab);
        vsr_io_lease_release(rep, lease);
        return VSR_IO_RETRY;
    }
    do {
        vsr_io_engine_random(io, &id, sizeof(id));
    } while (id_zero(id) || entry_find(s, id) != NULL);
    entry = entry_take(s, id, VSR_IO_SNAPSHOT_WRITING);
    if (entry == NULL) {
        vsr_io_pool_release(&io->pool, slab);
        vsr_io_engine_slot_free(io, slot);
        vsr_io_lease_release(rep, lease);
        return VSR_IO_RETRY;
    }
    s->task = *task;
    vsr_io_bump_init(&template, s->template_region, s->region_bytes);
    if (!copy_checkpoint(&template, task->checkpoint, &s->template)) {
        entry_free(s, entry);
        vsr_io_pool_release(&io->pool, slab);
        vsr_io_engine_slot_free(io, slot);
        vsr_io_lease_release(rep, lease);
        return VSR_IO_FAILED;
    }
    s->template.id = id;
    s->task.checkpoint = &s->template;
    count =
        vsr_io_store_snapshot_clients(&rep->store, w->table, s->max_clients);
    SNAPSHOT_ASSERT(count <= s->max_clients);
    entry->lease = lease;
    entry->op = op;
    entry->op_type = VSR_OP_SNAPSHOT_CAPTURE;
    entry->task = task;
    entry->sequence = task->sequence;
    entry->forward_due = 1;
    entry->job = VSR_IO_SNAPSHOT_JOB_WRITE;
    entry->step = STEP_OPEN;
    entry->tmp_slot = slot;
    w->snapshot = entry_index(s, entry);
    w->slab = slab;
    w->staged = 0;
    w->next = 0;
    w->count = count;
    w->cold_slab = NONE;
    w->cold_valid = 0;
    w->cold_bytes = 0;
    w->cold_offset = 0;
    w->file_offset = 0;
    w->header_op = task->op;
    w->header_sequence = task->sequence;
    w->trailer_staged = 0;
    w->aborted = 0;
    s->capture = w->snapshot;
    return VSR_OK;
}

int vsr_io_snapshots_sync(struct vsr_io *io, uint32_t replica, uint64_t op,
                          const struct vsr_snapshot_task *task)
{
    struct vsr_io_replica *rep;
    struct vsr_io_snapshots *s;
    struct vsr_io_snapshot *entry;

    if (io == NULL || replica >= io->options.limits.replicas || op == 0 ||
        task == NULL) {
        return VSR_EINVAL;
    }
    if (task->checkpoint == NULL) {
        return VSR_IO_FAILED;
    }
    rep = replica_of(io, replica);
    s = &rep->snapshots;
    entry = entry_find(s, task->checkpoint->id);
    if (s->closed) {
        return VSR_IO_RETRY;
    }
    if (entry == NULL || entry->state == VSR_IO_SNAPSHOT_WRITING ||
        entry->state == VSR_IO_SNAPSHOT_FETCHING ||
        entry->state == VSR_IO_SNAPSHOT_DROPPING || !entry->on_disk) {
        return VSR_IO_FAILED;
    }
    /* A RELEASE of the kept slot is housekeeping: a SYNC cancels it while
     * its CLOSE is still to issue, or follows it (a RETRY would fence the
     * replica). */
    if (entry->op != 0 || (entry->job != VSR_IO_SNAPSHOT_JOB_NONE &&
                           entry->job != VSR_IO_SNAPSHOT_JOB_RELEASE)) {
        return VSR_IO_RETRY;
    }
    entry->op = op;
    entry->op_type = VSR_OP_SNAPSHOT_SYNC;
    entry->task = task;
    entry->caller_status = -1;
    entry->library_status = -1;
    entry->forward_due = 1;
    entry->state = VSR_IO_SNAPSHOT_SYNCING;
    if (entry->job == VSR_IO_SNAPSHOT_JOB_RELEASE && entry->fileop != NONE) {
        entry->job_next = VSR_IO_SNAPSHOT_JOB_SYNC;
        return VSR_OK;
    }
    entry->job = VSR_IO_SNAPSHOT_JOB_SYNC;
    entry->step = entry->file_slot >= 0 ? STEP_FSYNC : STEP_OPEN;
    return VSR_OK;
}

int vsr_io_snapshots_drop(struct vsr_io *io, uint32_t replica, uint64_t op,
                          const struct vsr_snapshot_task *task)
{
    struct vsr_io_replica *rep;
    struct vsr_io_snapshots *s;
    struct vsr_io_snapshot *entry;

    if (io == NULL || replica >= io->options.limits.replicas || op == 0 ||
        task == NULL) {
        return VSR_EINVAL;
    }
    if (task->checkpoint == NULL) {
        return VSR_IO_FAILED;
    }
    rep = replica_of(io, replica);
    s = &rep->snapshots;
    if (id_zero(task->checkpoint->id)) {
        return VSR_IO_FAILED;
    }
    entry = entry_find(s, task->checkpoint->id);
    if (s->closed) {
        return VSR_IO_RETRY;
    }
    if (entry == NULL) {
        /* An id the registry does not hold (a file left by an earlier
         * run, or none): forwarded all the same, then clients-<id> is
         * unlinked if it exists. */
        entry = entry_take(s, task->checkpoint->id, VSR_IO_SNAPSHOT_DURABLE);
        if (entry == NULL) {
            return VSR_IO_RETRY;
        }
    } else if (entry->state == VSR_IO_SNAPSHOT_WRITING ||
               entry->state == VSR_IO_SNAPSHOT_FETCHING ||
               entry->state == VSR_IO_SNAPSHOT_DROPPING) {
        return VSR_IO_FAILED;
    } else if (entry->op != 0 || (entry->job != VSR_IO_SNAPSHOT_JOB_NONE &&
                                  entry->job != VSR_IO_SNAPSHOT_JOB_RELEASE)) {
        return VSR_IO_RETRY;
    }
    /* With a RELEASE pending, the unlink job starts once it ends. */
    entry->op = op;
    entry->op_type = VSR_OP_SNAPSHOT_DROP;
    entry->task = task;
    entry->caller_status = -1;
    entry->library_status = VSR_IO_OK;
    entry->forward_due = 1;
    return VSR_OK;
}

bool vsr_io_snapshots_owns(const struct vsr_io_snapshots *snapshots,
                           uint64_t forwarded_op)
{
    if (snapshots == NULL || forwarded_op == 0) {
        return false;
    }
    for (uint32_t i = 0; i < snapshots->count; ++i) {
        if (snapshots->entries[i].state != VSR_IO_SNAPSHOT_FREE &&
            snapshots->entries[i].forwarded == forwarded_op) {
            return true;
        }
    }
    return false;
}

int vsr_io_snapshots_forwarded_done(struct vsr_io *io, uint32_t replica,
                                    uint64_t op, int32_t status,
                                    const void *data)
{
    struct vsr_io_replica *rep;
    struct vsr_io_snapshots *s;
    struct vsr_io_snapshot *entry = NULL;
    int32_t caller = status;

    if (io == NULL || replica >= io->options.limits.replicas || op == 0) {
        return VSR_EINVAL;
    }
    rep = replica_of(io, replica);
    s = &rep->snapshots;
    for (uint32_t i = 0; i < s->count; ++i) {
        if (s->entries[i].state != VSR_IO_SNAPSHOT_FREE &&
            s->entries[i].forwarded == op) {
            entry = &s->entries[i];
            break;
        }
    }
    if (entry == NULL) {
        return VSR_EINVAL;
    }
    entry->forwarded = 0;
    if ((entry->op_type == VSR_OP_SNAPSHOT_CAPTURE ||
         entry->op_type == VSR_OP_SNAPSHOT_FETCH) &&
        caller == VSR_IO_OK) {
        const struct vsr_checkpoint *checkpoint = data;

        /* The core adopts the checkpoint for this id only (vsr.h). */
        if (checkpoint == NULL || !id_equal(checkpoint->id, entry->id) ||
            !result_copy(rep, entry, checkpoint)) {
            caller = VSR_IO_FAILED;
        }
    }
    entry->caller_status = caller;
    switch (entry->op_type) {
    case VSR_OP_SNAPSHOT_CAPTURE:
        if (caller != VSR_IO_OK &&
            s->writer.snapshot == entry_index(s, entry)) {
            s->writer.aborted = 1;
        }
        capture_settle(io, replica, entry);
        return VSR_OK;
    case VSR_OP_SNAPSHOT_FETCH:
        fetch_settle(io, replica, entry);
        return VSR_OK;
    case VSR_OP_SNAPSHOT_SYNC:
        sync_settle(io, replica, entry);
        return VSR_OK;
    case VSR_OP_SNAPSHOT_DROP:
        drop_settle(io, replica, entry);
        return VSR_OK;
    default:
        return VSR_EINVAL;
    }
}

/* -------------------------------------------------------------------------
 * Job completions: sync, drop, discard, release
 * ---------------------------------------------------------------------- */

static void sync_fail(struct vsr_io *io, uint32_t replica,
                      struct vsr_io_snapshot *entry)
{
    entry->library_status = VSR_IO_FAILED;
    entry->job = VSR_IO_SNAPSHOT_JOB_NONE;
    sync_settle(io, replica, entry);
}

static void sync_done(struct vsr_io *io, uint32_t replica,
                      struct vsr_io_snapshot *entry, uint32_t step,
                      int32_t result)
{
    struct vsr_io_snapshots *s = snapshots_of(io, replica);

    entry->step = STEP_IDLE;
    if (result < 0 && step != STEP_CLOSE) {
        s->error = result;
    }
    switch (step) {
    case STEP_OPEN:
        if (result < 0) {
            vsr_io_engine_slot_free(io, entry->tmp_slot);
            entry->tmp_slot = NONE;
            sync_fail(io, replica, entry);
            return;
        }
        entry->step = STEP_FSYNC;
        return;
    case STEP_FSYNC:
        if (result < 0) {
            entry->sync_failed = 1; /* Reported after the close. */
        }
        if (entry->tmp_slot != NONE) {
            entry->step = STEP_CLOSE;
        } else if (result < 0) {
            sync_fail(io, replica, entry);
        } else {
            entry->step = STEP_FSYNC_DIR;
        }
        return;
    case STEP_CLOSE:
        slot_drop(io, entry->tmp_slot);
        entry->tmp_slot = NONE;
        if (entry->sync_failed != 0) {
            entry->sync_failed = 0;
            sync_fail(io, replica, entry);
            return;
        }
        entry->step = STEP_FSYNC_DIR;
        return;
    case STEP_FSYNC_DIR:
        if (result < 0) {
            sync_fail(io, replica, entry);
            return;
        }
        entry->library_status = VSR_IO_OK;
        entry->job = VSR_IO_SNAPSHOT_JOB_NONE;
        sync_settle(io, replica, entry);
        return;
    default:
        SNAPSHOT_ASSERT(false);
        return;
    }
}

/* DROP and DISCARD: the kept slot closed, then the unlink. */
static void unlink_job_done(struct vsr_io *io, uint32_t replica,
                            struct vsr_io_snapshot *entry, uint32_t step,
                            int32_t result)
{
    struct vsr_io_replica *rep = replica_of(io, replica);
    struct vsr_io_snapshots *s = &rep->snapshots;
    uint32_t job = entry->job;
    int32_t status;

    entry->step = STEP_IDLE;
    if (step == STEP_CLOSE) {
        slot_drop(io, (uint32_t)entry->file_slot);
        entry->file_slot = -1;
        if (rep->store.base_slot >= 0 &&
            id_equal(rep->store.client_base_id, entry->id)) {
            rep->store.base_slot = -1;
        }
        entry->step = STEP_UNLINK;
        return;
    }
    SNAPSHOT_ASSERT(step == STEP_UNLINK);
    status = result >= 0 || result == -ENOENT ? VSR_IO_OK : VSR_IO_FAILED;
    if (status != VSR_IO_OK) {
        s->error = result;
    }
    entry->on_disk = 0;
    entry->job = VSR_IO_SNAPSHOT_JOB_NONE;
    if (job == VSR_IO_SNAPSHOT_JOB_DISCARD) {
        if (entry->op_type == VSR_OP_SNAPSHOT_CAPTURE) {
            capture_settle(io, replica, entry);
        } else {
            fetch_settle(io, replica, entry);
        }
        return;
    }
    if (entry->op != 0) {
        op_complete(rep, entry, status);
    }
    entry_free(s, entry);
}

/* The kept slot of a file that is neither the base nor the latest capture
 * is closed; a SYNC that arrived meanwhile starts now. */
static void release_done(struct vsr_io *io, uint32_t replica,
                         struct vsr_io_snapshot *entry)
{
    struct vsr_io_replica *rep = replica_of(io, replica);

    slot_drop(io, (uint32_t)entry->file_slot);
    if (rep->store.base_slot == entry->file_slot) {
        rep->store.base_slot = -1;
    }
    entry->file_slot = -1;
    entry->step = STEP_IDLE;
    entry->job = VSR_IO_SNAPSHOT_JOB_NONE;
    if (entry->job_next == VSR_IO_SNAPSHOT_JOB_SYNC) {
        entry->job_next = NONE;
        entry->job = VSR_IO_SNAPSHOT_JOB_SYNC;
        entry->step = STEP_OPEN;
    }
}

/* -------------------------------------------------------------------------
 * Poll
 * ---------------------------------------------------------------------- */

/* Keeps store.base_slot on the base's open file (the module owns that
 * field: the store only reads through it) and closes the slots of files
 * that are neither the base, nor the latest capture, nor a file loaded for
 * a held RESTORE or PUBLISH the store has yet to pack. A file with a core
 * op in progress keeps its slot until the op is over: the settles of
 * CAPTURE and SYNC wait for their entry's job to end, and a RELEASE's
 * completion settles nothing. */
static void base_track(struct vsr_io *io, uint32_t replica)
{
    struct vsr_io_replica *rep = replica_of(io, replica);
    struct vsr_io_store *store = &rep->store;
    struct vsr_io_snapshots *s = &rep->snapshots;
    struct vsr_io_snapshot *base = entry_find(s, store->client_base_id);

    if (base != NULL && base->file_slot >= 0 &&
        base->job != VSR_IO_SNAPSHOT_JOB_DROP &&
        base->job != VSR_IO_SNAPSHOT_JOB_DISCARD) {
        store->base_slot = base->file_slot;
    } else if (store->cold_active == 0) {
        store->base_slot = -1;
    }
    if (s->pending_base != NONE &&
        (&s->entries[s->pending_base] == base ||
         s->entries[s->pending_base].file_slot < 0)) {
        s->pending_base = NONE; /* Packed (or its file closed). */
    }
    for (uint32_t i = 0; i < s->count; ++i) {
        struct vsr_io_snapshot *entry = &s->entries[i];

        if (entry->state == VSR_IO_SNAPSHOT_FREE || entry->file_slot < 0 ||
            entry->job != VSR_IO_SNAPSHOT_JOB_NONE || entry->op != 0 ||
            entry == base || i == s->pending_base ||
            id_equal(entry->id, store->last_capture) ||
            store->cold_active != 0) {
            continue;
        }
        entry->job = VSR_IO_SNAPSHOT_JOB_RELEASE;
        entry->step = STEP_CLOSE;
    }
}

static void forward_due(struct vsr_io *io, uint32_t replica)
{
    struct vsr_io_replica *rep = replica_of(io, replica);
    struct vsr_io_snapshots *s = &rep->snapshots;

    for (uint32_t i = 0; i < s->count; ++i) {
        struct vsr_io_snapshot *entry = &s->entries[i];
        struct vsr_io_forwarded *f;

        if (entry->state == VSR_IO_SNAPSHOT_FREE || !entry->forward_due) {
            continue;
        }
        f = vsr_io_forward(io, rep, VSR_IO_OP_CORE);
        if (f == NULL) {
            return; /* Ring full: poll reports MORE. */
        }
        f->op.op.type = entry->op_type;
        f->op.op.flags = 0;
        f->op.op.id = entry->op;
        f->op.op.data = entry->op_type == VSR_OP_SNAPSHOT_CAPTURE
                            ? (const void *)&s->task
                            : (const void *)entry->task;
        f->op.op.arg = 0;
        entry->forwarded = entry->op;
        entry->forward_due = 0;
    }
}

void vsr_io_snapshots_poll(struct vsr_io *io, uint32_t replica, uint64_t now)
{
    (void)now; /* Waits arm the deadline from io->now (retry_later). */
    struct vsr_io_replica *rep;
    struct vsr_io_snapshots *s;
    struct vsr_id wanted;
    uint64_t sequence = 0;

    if (io == NULL || replica >= io->options.limits.replicas) {
        return;
    }
    rep = replica_of(io, replica);
    s = &rep->snapshots;
    if (s->closed) {
        return;
    }
    s->retry = 0;
    if (s->dir_state == DIR_CLOSED) {
        s->dir_state = DIR_OPENING;
    }
    base_track(io, replica);
    if (s->reader.snapshot == NONE &&
        vsr_io_store_base_wanted(&rep->store, &wanted, &sequence)) {
        (void)vsr_io_snapshots_load_base(io, replica, wanted, sequence);
    }
    if (s->writer.snapshot != NONE) {
        writer_stage(io, replica);
    }
    for (uint32_t i = 0; i < s->count; ++i) {
        struct vsr_io_snapshot *entry = &s->entries[i];

        if (entry->state == VSR_IO_SNAPSHOT_DROPPING && entry->unlink_due &&
            entry->readers == 0 && entry->job == VSR_IO_SNAPSHOT_JOB_NONE) {
            entry->unlink_due = 0;
            entry->job = VSR_IO_SNAPSHOT_JOB_DROP;
            entry->step = entry->file_slot >= 0 ? STEP_CLOSE : STEP_UNLINK;
        }
    }
    forward_due(io, replica);
}

/* -------------------------------------------------------------------------
 * Prepare
 * ---------------------------------------------------------------------- */

static uint32_t fileop_take(struct vsr_io_snapshots *s)
{
    for (uint32_t i = 0; i < VSR_IO_SNAPSHOT_FILEOPS; ++i) {
        if (!s->fileops[i].used) {
            s->fileops[i].used = 1;
            s->fileops[i].slot = NONE;
            return i;
        }
    }
    return NONE;
}

static void fileop_free(struct vsr_io_snapshots *s, uint32_t index)
{
    s->fileops[index].used = 0;
    s->fileops[index].slot = NONE;
}

static void sqe_fixed(struct vsr_io_sqe *sqe, uint8_t opcode, uint32_t slot,
                      uint64_t user_data)
{
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode = opcode;
    sqe->flags = VSR_IO_SQE_FIXED_FILE;
    sqe->fd = (int32_t)slot;
    sqe->user_data = user_data;
}

static void sqe_path(struct vsr_io_sqe *sqe, uint8_t opcode, int dir_fd,
                     const char *path, uint64_t user_data)
{
    memset(sqe, 0, sizeof(*sqe));
    sqe->opcode = opcode;
    sqe->fd = dir_fd;
    sqe->addr = path;
    sqe->user_data = user_data;
}

static void sqe_transfer(struct vsr_io *io, struct vsr_io_sqe *sqe,
                         uint8_t opcode, uint32_t slot, const void *addr,
                         uint32_t length, uint64_t offset, uint64_t user_data)
{
    sqe_fixed(sqe, opcode, slot, user_data);
    sqe->flags = VSR_IO_SQE_FIXED_FILE | VSR_IO_SQE_FIXED_BUFFER;
    sqe->buffer_index = (uint16_t)io->pool.region_index;
    sqe->addr = addr;
    sqe->length = length;
    sqe->offset = offset;
}

static void entry_done(struct vsr_io *io, uint32_t replica,
                       struct vsr_io_snapshot *entry, uint32_t step,
                       int32_t result);

/* Builds the record of entry `index`'s next step into sqe; false when
 * nothing can be issued now (a resource is missing, or the step waits). */
static bool prepare_entry(struct vsr_io *io, uint32_t replica, uint32_t index,
                          struct vsr_io_sqe *sqe)
{
    struct vsr_io_replica *rep = replica_of(io, replica);
    struct vsr_io_store *store = &rep->store;
    struct vsr_io_snapshots *s = &rep->snapshots;
    struct vsr_io_snapshot *entry = &s->entries[index];
    struct vsr_io_clients_writer *w = &s->writer;
    struct vsr_io_clients_reader *r = &s->reader;
    struct vsr_io_snapshot_fileop *fileop;
    uint32_t op;
    uint32_t slot;
    uint64_t user_data;
    uint32_t step = entry->step;
    bool tmp = false;

    if (step == STEP_IDLE && entry->job == VSR_IO_SNAPSHOT_JOB_FETCH &&
        s->chunks_count > 0 && !s->chunk_writing && entry->tmp_slot != NONE &&
        r->reads == 0 && r->stage != STAGE_FAILED) {
        step = STEP_WRITE;
    }
    if (step == STEP_IDLE || entry->fileop != NONE) {
        return false;
    }
    if (step == STEP_FSYNC_DIR) {
        if (s->dir_state == DIR_OPENING) {
            return false;
        }
        if (s->dir_state != DIR_OPEN) {
            sync_done(io, replica, entry, STEP_FSYNC_DIR, -EIO);
            return false;
        }
    }
    if (step == STEP_OPEN && entry->tmp_slot == NONE) {
        entry->tmp_slot = vsr_io_engine_slot_alloc(io);
        if (entry->tmp_slot == NONE) {
            retry_later(io, rep);
            return false;
        }
    }
    op = fileop_take(s);
    if (op == NONE) {
        retry_later(io, rep);
        return false;
    }
    slot = vsr_io_slots_alloc(&io->slots, VSR_IO_SLOT_CLIENTS, 1, replica,
                              SUB_ENTRY | index, step);
    if (slot == NONE) {
        fileop_free(s, op);
        retry_later(io, rep);
        return false;
    }
    fileop = &s->fileops[op];
    fileop->slot = slot;
    user_data = vsr_io_slots_user_data(&io->slots, slot);
    tmp = entry->job == VSR_IO_SNAPSHOT_JOB_FETCH;
    if ((step == STEP_OPEN || step == STEP_STAT || step == STEP_RENAME ||
         step == STEP_UNLINK) &&
        !path_of(fileop->path, store->path, entry->id, true, tmp)) {
        vsr_io_slots_free(&io->slots, slot);
        fileop_free(s, op);
        entry->step = STEP_IDLE;
        entry_done(io, replica, entry, step, -ENAMETOOLONG);
        return false;
    }
    switch (step) {
    case STEP_OPEN: {
        uint32_t flags;

        if (entry->job == VSR_IO_SNAPSHOT_JOB_WRITE) {
            flags = (uint32_t)(O_RDWR | O_CREAT | O_EXCL);
        } else if (entry->job == VSR_IO_SNAPSHOT_JOB_FETCH) {
            flags = (uint32_t)(O_WRONLY | O_CREAT | O_TRUNC);
        } else {
            flags = (uint32_t)O_RDONLY;
        }
        sqe_path(sqe, VSR_IO_SQE_OPENAT, store->dir_fd, fileop->path,
                 user_data);
        sqe->flags = VSR_IO_SQE_DIRECT;
        sqe->fd2 = (int32_t)entry->tmp_slot;
        sqe->op_flags = flags;
        sqe->length = SNAPSHOT_FILE_MODE;
        break;
    }
    case STEP_STAT:
        sqe_path(sqe, VSR_IO_SQE_STATX, store->dir_fd, fileop->path, user_data);
        sqe->addr2 = r->stat;
        sqe->length = STATX_SIZE;
        break;
    case STEP_READ:
        if (entry->job == VSR_IO_SNAPSHOT_JOB_LOAD) {
            uint64_t at = r->file_offset + r->filled;
            uint64_t want = io->pool.slab_bytes - r->filled;

            if (r->file_size - at < want) {
                want = r->file_size - at;
            }
            sqe_transfer(io, sqe, VSR_IO_SQE_READ, entry->tmp_slot,
                         vsr_io_pool_slab(&io->pool, r->slab) + r->filled,
                         (uint32_t)want, at, user_data);
            r->reads = 1;
        } else {
            const struct vsr_io_client_snapshot *e = &w->table[w->next];
            uint64_t begin = e->record.offset;
            uint64_t length = io->pool.slab_bytes;
            int32_t fd = store->base_slot;

            if (e->record.sequence != 0) {
                uint64_t block = store->options.block_bytes;

                begin = e->record.offset / block * block;
                length = round_up(e->record.offset + e->record.length, block) -
                         begin;
                fd = store->log_slot;
            }
            if (fd < 0) {
                vsr_io_slots_free(&io->slots, slot);
                fileop_free(s, op);
                entry->step = STEP_IDLE;
                writer_finish(io, replica, VSR_IO_FAILED);
                return false;
            }
            sqe_transfer(io, sqe, VSR_IO_SQE_READ, (uint32_t)fd,
                         vsr_io_pool_slab(&io->pool, w->cold_slab),
                         (uint32_t)length, begin, user_data);
            w->cold_offset = begin;
        }
        break;
    case STEP_WRITE:
        if (entry->job == VSR_IO_SNAPSHOT_JOB_WRITE) {
            sqe_transfer(io, sqe, VSR_IO_SQE_WRITE, entry->tmp_slot,
                         vsr_io_pool_slab(&io->pool, w->slab), w->staged,
                         w->file_offset, user_data);
        } else {
            const struct vsr_io_snapshot_chunk *chunk =
                &s->chunks[s->chunks_head];

            sqe_transfer(io, sqe, VSR_IO_SQE_WRITE, entry->tmp_slot,
                         chunk->data, chunk->length, chunk->offset, user_data);
            s->chunk_writing = 1;
        }
        break;
    case STEP_FSYNC:
        sqe_fixed(sqe, VSR_IO_SQE_FSYNC,
                  entry->tmp_slot != NONE ? entry->tmp_slot
                                          : (uint32_t)entry->file_slot,
                  user_data);
        sqe->op_flags = VSR_IO_FSYNC_DATASYNC;
        break;
    case STEP_FSYNC_DIR:
        sqe_fixed(sqe, VSR_IO_SQE_FSYNC, s->dir_slot, user_data);
        break;
    case STEP_CLOSE:
        sqe_fixed(sqe, VSR_IO_SQE_CLOSE,
                  entry->tmp_slot != NONE ? entry->tmp_slot
                                          : (uint32_t)entry->file_slot,
                  user_data);
        break;
    case STEP_RENAME:
        if (!path_of(fileop->path2, store->path, entry->id, true, false)) {
            SNAPSHOT_ASSERT(false); /* The .tmp path fit. */
        }
        sqe_path(sqe, VSR_IO_SQE_RENAMEAT, store->dir_fd, fileop->path,
                 user_data);
        sqe->addr2 = fileop->path2;
        break;
    case STEP_UNLINK:
        sqe_path(sqe, VSR_IO_SQE_UNLINKAT, store->dir_fd, fileop->path,
                 user_data);
        break;
    default:
        SNAPSHOT_ASSERT(false);
        return false;
    }
    entry->fileop = op;
    entry->step = step;
    return true;
}

static bool prepare_serve(struct vsr_io *io, uint32_t replica, uint32_t stream,
                          struct vsr_io_sqe *sqe)
{
    struct vsr_io_replica *rep = replica_of(io, replica);
    struct vsr_io_snapshots *s = &rep->snapshots;
    struct vsr_io_snapshot_serve *serve = &s->serves[stream];
    const struct vsr_io_snapshot *entry = &s->entries[serve->snapshot];
    struct vsr_io_snapshot_fileop *fileop;
    uint32_t op;
    uint32_t slot;
    uint64_t user_data;
    uint32_t step;

    if (serve->fileop != NONE) {
        return false;
    }
    /* OPENING: the open is still to issue (a slot may be allocated from an
     * earlier attempt that found no file operation free). */
    if (serve->state == SERVE_OPENING) {
        step = STEP_OPEN;
    } else if (serve->state == SERVE_CLOSING && serve->slot != NONE) {
        step = STEP_CLOSE;
    } else {
        return false;
    }
    if (step == STEP_OPEN && serve->slot == NONE) {
        serve->slot = vsr_io_engine_slot_alloc(io);
        if (serve->slot == NONE) {
            /* No file slot: refuse; the end frees the serve. */
            serve->state = SERVE_OPEN;
            (void)vsr_io_streams_close(
                io, vsr_io_streams_handle(&io->streams, stream), VSR_IO_RETRY);
            return false;
        }
    }
    op = fileop_take(s);
    if (op == NONE) {
        retry_later(io, rep);
        return false;
    }
    slot = vsr_io_slots_alloc(&io->slots, VSR_IO_SLOT_CLIENTS, 1, replica,
                              SUB_SERVE | stream, step);
    if (slot == NONE) {
        fileop_free(s, op);
        retry_later(io, rep);
        return false;
    }
    fileop = &s->fileops[op];
    fileop->slot = slot;
    user_data = vsr_io_slots_user_data(&io->slots, slot);
    if (step == STEP_OPEN) {
        if (!path_of(fileop->path, rep->store.path, entry->id, true, false)) {
            vsr_io_slots_free(&io->slots, slot);
            fileop_free(s, op);
            serve_done(io, replica, stream, STEP_OPEN, -ENAMETOOLONG);
            return false;
        }
        sqe_path(sqe, VSR_IO_SQE_OPENAT, rep->store.dir_fd, fileop->path,
                 user_data);
        sqe->flags = VSR_IO_SQE_DIRECT;
        sqe->fd2 = (int32_t)serve->slot;
        sqe->op_flags = (uint32_t)O_RDONLY;
    } else {
        sqe_fixed(sqe, VSR_IO_SQE_CLOSE, serve->slot, user_data);
    }
    serve->fileop = op;
    return true;
}

static bool prepare_dir(struct vsr_io *io, uint32_t replica,
                        struct vsr_io_sqe *sqe)
{
    struct vsr_io_replica *rep = replica_of(io, replica);
    struct vsr_io_snapshots *s = &rep->snapshots;
    struct vsr_io_snapshot_fileop *fileop;
    struct vsr_id zero = {0, 0};
    uint32_t op;
    uint32_t slot;

    if (s->dir_state != DIR_OPENING || s->dir_fileop != NONE) {
        return false;
    }
    if (s->dir_slot == NONE) {
        s->dir_slot = vsr_io_engine_slot_alloc(io);
        if (s->dir_slot == NONE) {
            retry_later(io, rep);
            return false;
        }
    }
    op = fileop_take(s);
    if (op == NONE) {
        retry_later(io, rep);
        return false;
    }
    slot = vsr_io_slots_alloc(&io->slots, VSR_IO_SLOT_CLIENTS, 1, replica,
                              SUB_DIR, STEP_OPEN);
    if (slot == NONE) {
        fileop_free(s, op);
        retry_later(io, rep);
        return false;
    }
    fileop = &s->fileops[op];
    fileop->slot = slot;
    if (!path_of(fileop->path, rep->store.path, zero, false, false)) {
        vsr_io_slots_free(&io->slots, slot);
        fileop_free(s, op);
        vsr_io_engine_slot_free(io, s->dir_slot);
        s->dir_slot = NONE;
        s->dir_state = DIR_FAILED;
        s->error = -ENAMETOOLONG;
        return false;
    }
    sqe_path(sqe, VSR_IO_SQE_OPENAT, rep->store.dir_fd, fileop->path,
             vsr_io_slots_user_data(&io->slots, slot));
    sqe->flags = VSR_IO_SQE_DIRECT;
    sqe->fd2 = (int32_t)s->dir_slot;
    sqe->op_flags = (uint32_t)(O_RDONLY | O_DIRECTORY);
    s->dir_fileop = op;
    return true;
}

void vsr_io_snapshots_prepare(struct vsr_io *io, uint32_t replica,
                              struct vsr_io_sqe *sqes, uint32_t capacity,
                              uint32_t *count)
{
    struct vsr_io_snapshots *s;

    if (io == NULL || replica >= io->options.limits.replicas || sqes == NULL ||
        count == NULL) {
        return;
    }
    s = snapshots_of(io, replica);
    if (s->closed) {
        return;
    }
    if (*count < capacity && prepare_dir(io, replica, &sqes[*count])) {
        (*count)++;
    }
    for (uint32_t i = 0; i < s->count && *count < capacity; ++i) {
        if (s->entries[i].state != VSR_IO_SNAPSHOT_FREE &&
            prepare_entry(io, replica, i, &sqes[*count])) {
            (*count)++;
        }
    }
    for (uint32_t i = 0; i < s->serves_count && *count < capacity; ++i) {
        if (s->serves[i].state != SERVE_FREE &&
            prepare_serve(io, replica, i, &sqes[*count])) {
            (*count)++;
        }
    }
}

/* -------------------------------------------------------------------------
 * Complete
 * ---------------------------------------------------------------------- */

/* Dispatches a completed step of an entry's job (a negative result for a
 * record that could not be issued, such as a path that does not fit). */
static void entry_done(struct vsr_io *io, uint32_t replica,
                       struct vsr_io_snapshot *entry, uint32_t step,
                       int32_t result)
{
    switch (entry->job) {
    case VSR_IO_SNAPSHOT_JOB_WRITE:
        writer_done(io, replica, step, result);
        return;
    case VSR_IO_SNAPSHOT_JOB_LOAD:
        load_done(io, replica, step, result);
        return;
    case VSR_IO_SNAPSHOT_JOB_FETCH:
        fetch_done(io, replica, step, result);
        return;
    case VSR_IO_SNAPSHOT_JOB_SYNC:
        sync_done(io, replica, entry, step, result);
        return;
    case VSR_IO_SNAPSHOT_JOB_DROP:
    case VSR_IO_SNAPSHOT_JOB_DISCARD:
        unlink_job_done(io, replica, entry, step, result);
        return;
    case VSR_IO_SNAPSHOT_JOB_RELEASE:
        release_done(io, replica, entry);
        return;
    case VSR_IO_SNAPSHOT_JOB_NONE:
    default:
        SNAPSHOT_ASSERT(false);
        return;
    }
}

void vsr_io_snapshots_complete(struct vsr_io *io, uint32_t replica,
                               uint32_t slot, const struct vsr_io_cqe *cqe)
{
    struct vsr_io_replica *rep;
    struct vsr_io_snapshots *s;
    struct vsr_io_slot *record;
    uint32_t sub;
    uint32_t step;
    uint32_t index;
    int32_t result;

    if (io == NULL || replica >= io->options.limits.replicas) {
        return;
    }
    rep = replica_of(io, replica);
    s = &rep->snapshots;
    if (slot >= io->slots.count || cqe == NULL) {
        return;
    }
    record = &io->slots.slots[slot];
    if (record->kind != VSR_IO_SLOT_CLIENTS) {
        return;
    }
    sub = record->sub;
    step = (uint32_t)record->cookie;
    result = cqe->result;
    vsr_io_slots_consumed(&io->slots, slot, false);
    index = sub & SUB_INDEX_MASK;
    if ((sub & ~SUB_INDEX_MASK) == SUB_ENTRY) {
        struct vsr_io_snapshot *entry;

        if (index >= s->count) {
            return;
        }
        entry = &s->entries[index];
        if (entry->fileop != NONE) {
            fileop_free(s, entry->fileop);
            entry->fileop = NONE;
        }
        entry_done(io, replica, entry, step, result);
    } else if ((sub & ~SUB_INDEX_MASK) == SUB_SERVE) {
        if (index >= s->serves_count) {
            return;
        }
        if (s->serves[index].fileop != NONE) {
            fileop_free(s, s->serves[index].fileop);
            s->serves[index].fileop = NONE;
        }
        serve_done(io, replica, index, step, result);
    } else if ((sub & ~SUB_INDEX_MASK) == SUB_DIR) {
        if (s->dir_fileop != NONE) {
            fileop_free(s, s->dir_fileop);
            s->dir_fileop = NONE;
        }
        if (result < 0) {
            s->error = result;
            vsr_io_engine_slot_free(io, s->dir_slot);
            s->dir_slot = NONE;
            s->dir_state = DIR_FAILED;
        } else {
            s->dir_state = DIR_OPEN;
        }
    }
}

/* -------------------------------------------------------------------------
 * Close
 * ---------------------------------------------------------------------- */

int vsr_io_snapshots_close(struct vsr_io *io, uint32_t replica)
{
    struct vsr_io_replica *rep;
    struct vsr_io_snapshots *s;

    if (io == NULL || replica >= io->options.limits.replicas) {
        return VSR_EINVAL;
    }
    rep = replica_of(io, replica);
    s = &rep->snapshots;
    if (s->closed) {
        return VSR_OK;
    }
    for (uint32_t i = 0; i < VSR_IO_SNAPSHOT_FILEOPS; ++i) {
        if (s->fileops[i].used) {
            return VSR_EBUSY;
        }
    }
    for (uint32_t i = 0; i < s->serves_count; ++i) {
        if (s->serves[i].state != SERVE_FREE) {
            return VSR_EBUSY;
        }
    }
    if (s->writer.snapshot != NONE || s->reader.snapshot != NONE) {
        return VSR_EBUSY;
    }
    for (uint32_t i = 0; i < s->count; ++i) {
        struct vsr_io_snapshot *entry = &s->entries[i];

        if (entry->file_slot >= 0) {
            slot_drop(io, (uint32_t)entry->file_slot);
            entry->file_slot = -1;
        }
        if (entry->tmp_slot != NONE) {
            slot_drop(io, entry->tmp_slot);
            entry->tmp_slot = NONE;
        }
        if (entry->lease != NONE) {
            /* An op the stopped core no longer waits for. */
            vsr_io_lease_release(rep, entry->lease);
            entry->lease = NONE;
            entry->result = NULL;
        }
    }
    if (s->dir_slot != NONE) {
        slot_drop(io, s->dir_slot);
        s->dir_slot = NONE;
    }
    s->dir_state = DIR_CLOSED;
    rep->store.base_slot = -1;
    s->closed = 1;
    return VSR_OK;
}
