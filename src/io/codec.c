#include "config.h"

#include "io/codec.h"

#include "checked.h"
#include "io/crc32c.h"
#include "io/cursor.h"
#include "io/wire.h"
#include "vsr.h"

#include <stdalign.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

/*
 * Every encoder is a WALK over a host graph that hands byte runs to a SINK:
 * header runs built on the stack through the little-endian helpers,
 * payload spans referenced from where they live, and zero padding. Three
 * sinks share every walk: one measures (length and, for the digest, the
 * CRC), one copies into a buffer (store records, segment headers), and
 * one produces vectors (the network encoder, resumable through the byte
 * offset it reached). The walks validate the graph shape (EINVAL) and,
 * when limits are supplied, the limits (ELIMIT), with the same rules the
 * decoders apply, so that an encoding decodes and a decoded graph
 * re-encodes to the same bytes.
 *
 * Every decoder reads through the cursor and builds host structs from a
 * bump region; padding bytes must be zero, presence flags may carry no
 * unknown bits, reserved fields must be zero, and an object must consume
 * exactly the bytes its enclosing length names.
 */

#define TRY(expression)                                                        \
    do {                                                                       \
        int try_result = (expression);                                         \
        if (try_result != VSR_OK) {                                            \
            return try_result;                                                 \
        }                                                                      \
    } while (0)

static const unsigned char zeros[VSR_IO_WIRE_ALIGN];

/* -------------------------------------------------------------------------
 * Checked 64-bit arithmetic and byte helpers
 * ---------------------------------------------------------------------- */

static bool add64(uint64_t a, uint64_t b, uint64_t *out)
{
    if (a > UINT64_MAX - b) {
        return false;
    }
    *out = a + b;
    return true;
}

static bool mul64(uint64_t a, uint64_t b, uint64_t *out)
{
    if (b != 0 && a > UINT64_MAX / b) {
        return false;
    }
    *out = a * b;
    return true;
}

/* size rounded up to the wire alignment. */
static bool pad64(uint64_t size, uint64_t *out)
{
    uint64_t padded;

    if (!add64(size, VSR_IO_WIRE_ALIGN - 1, &padded)) {
        return false;
    }
    *out = padded & ~(uint64_t)(VSR_IO_WIRE_ALIGN - 1);
    return true;
}

static uint64_t max64(uint64_t a, uint64_t b)
{
    return a > b ? a : b;
}

/* Padding bytes after size bytes of payload. */
static size_t padding_of(uint64_t size)
{
    return (size_t)((VSR_IO_WIRE_ALIGN - size % VSR_IO_WIRE_ALIGN) %
                    VSR_IO_WIRE_ALIGN);
}

static bool to_size(uint64_t value, size_t *out)
{
    if (value > SIZE_MAX) {
        return false;
    }
    *out = (size_t)value;
    return true;
}

/* Sequential little-endian writer over a buffer the caller sized. */
struct put {
    unsigned char *at;
};

static void put_u32(struct put *put, uint32_t value)
{
    vsr_io_put_u32(put->at, value);
    put->at += 4;
}

static void put_u64(struct put *put, uint64_t value)
{
    vsr_io_put_u64(put->at, value);
    put->at += 8;
}

/* -------------------------------------------------------------------------
 * Regions and limits
 * ---------------------------------------------------------------------- */

void vsr_io_bump_init(struct vsr_io_bump *bump, void *base, size_t size)
{
    bump->base = base;
    bump->size = size;
    bump->used = 0;
}

void *vsr_io_bump_alloc(struct vsr_io_bump *bump, size_t size, size_t alignment)
{
    uintptr_t address = (uintptr_t)bump->base + bump->used;
    size_t padding =
        (alignment - (address & (alignment - 1))) & (alignment - 1);
    size_t free = bump->size - bump->used;
    void *out;

    if (padding > free || size > free - padding) {
        return NULL;
    }
    out = bump->base + bump->used + padding;
    bump->used += padding + size;
    return out;
}

/* Wire bytes of a membership of count members. */
static bool membership_wire(uint64_t count, uint64_t *out)
{
    uint64_t members;

    return mul64(count, sizeof(struct vsr_io_wire_member), &members) &&
           add64(sizeof(struct vsr_io_wire_membership), members, out);
}

/* Largest EPOCH: two memberships of `members`. */
static bool epoch_wire_max(const struct vsr_limits *limits, uint64_t *out)
{
    uint64_t membership;
    uint64_t both;

    return membership_wire(limits->members, &membership) &&
           mul64(membership, 2, &both) &&
           add64(sizeof(struct vsr_io_wire_epoch), both, out);
}

/* Largest ENTRY not counting its blob bytes, which the aggregate
 * message_bytes covers: the header, then a blob header with its padding, a
 * membership of `members`, or a check-epoch body. */
static bool entry_wire_max(const struct vsr_limits *limits, uint64_t *out)
{
    uint64_t membership;
    uint64_t body;

    if (!membership_wire(limits->members, &membership)) {
        return false;
    }
    body = max64(sizeof(struct vsr_io_wire_blob) + VSR_IO_WIRE_ALIGN - 1,
                 membership);
    body = max64(body, sizeof(struct vsr_io_wire_check_epoch));
    return add64(sizeof(struct vsr_io_wire_entry), body, out);
}

/* batch_entries entries, without an ENTRIES header or blob bytes. */
static bool entry_array_wire_max(const struct vsr_limits *limits, uint64_t *out)
{
    uint64_t entry;

    return entry_wire_max(limits, &entry) &&
           mul64(entry, limits->batch_entries, out);
}

/* Largest CHECKPOINT with its manifest bytes included. */
static bool checkpoint_wire_max(const struct vsr_limits *limits, uint64_t *out)
{
    uint64_t epoch;
    uint64_t manifest;
    uint64_t total =
        sizeof(struct vsr_io_wire_checkpoint) + sizeof(struct vsr_io_wire_blob);

    return epoch_wire_max(limits, &epoch) &&
           pad64(limits->manifest_bytes, &manifest) &&
           add64(total, epoch, &total) && add64(total, manifest, out);
}

/* Host bytes of an epoch with two memberships of `members`. */
static bool epoch_graph_max(const struct vsr_limits *limits, uint64_t *out)
{
    uint64_t members;
    uint64_t membership;
    uint64_t both;

    return mul64(limits->members, sizeof(struct vsr_member), &members) &&
           add64(sizeof(struct vsr_membership), members, &membership) &&
           mul64(membership, 2, &both) &&
           add64(sizeof(struct vsr_epoch), both, out);
}

/* Host bytes of batch_entries entries, each with the largest body. */
static bool entry_graph_max(const struct vsr_limits *limits, uint64_t *out)
{
    uint64_t members;
    uint64_t membership;
    uint64_t body;
    uint64_t entry;

    if (!mul64(limits->members, sizeof(struct vsr_member), &members) ||
        !add64(sizeof(struct vsr_membership), members, &membership)) {
        return false;
    }
    body = max64(sizeof(struct vsr_blob) + sizeof(struct vsr_span), membership);
    body = max64(body, sizeof(struct vsr_check_epoch));
    return add64(sizeof(struct vsr_entry), body, &entry) &&
           mul64(entry, limits->batch_entries, out);
}

/* Host bytes of a checkpoint: the struct, its epoch and one manifest span. */
static bool checkpoint_graph_max(const struct vsr_limits *limits, uint64_t *out)
{
    uint64_t epoch;

    return epoch_graph_max(limits, &epoch) &&
           add64(sizeof(struct vsr_checkpoint) + sizeof(struct vsr_span), epoch,
                 out);
}

int vsr_io_codec_message_region(const struct vsr_limits *limits, size_t *bytes)
{
    uint64_t epoch;
    uint64_t entries;
    uint64_t checkpoint;
    uint64_t prepare;
    uint64_t chunk;
    uint64_t total;

    if (limits == NULL || bytes == NULL) {
        return VSR_EINVAL;
    }
    if (!epoch_graph_max(limits, &epoch) ||
        !entry_graph_max(limits, &entries) ||
        !checkpoint_graph_max(limits, &checkpoint) ||
        !add64(sizeof(struct vsr_prepare), entries, &prepare)) {
        return VSR_ELIMIT;
    }
    /* The largest body holding a log state is the state chunk: the chunk,
     * the epoch, the entries and the checkpoint with its own epoch. */
    chunk = sizeof(struct vsr_state_chunk);
    if (!add64(chunk, epoch, &chunk) || !add64(chunk, entries, &chunk) ||
        !add64(chunk, checkpoint, &chunk)) {
        return VSR_ELIMIT;
    }
    /* One alignment of slack lets the region start at any address. */
    total = VSR_IO_WIRE_ALIGN + sizeof(struct vsr_message);
    if (!add64(total, max64(prepare, chunk), &total) ||
        !to_size(total, bytes)) {
        return VSR_ELIMIT;
    }
    return VSR_OK;
}

int vsr_io_codec_load_region(const struct vsr_limits *limits, size_t *bytes)
{
    uint64_t epoch;
    uint64_t entries;
    uint64_t checkpoint;
    uint64_t recovered;
    uint64_t total;

    if (limits == NULL || bytes == NULL) {
        return VSR_EINVAL;
    }
    if (!epoch_graph_max(limits, &epoch) ||
        !entry_graph_max(limits, &entries) ||
        !checkpoint_graph_max(limits, &checkpoint)) {
        return VSR_ELIMIT;
    }
    /* A recovered row: the hard state's epoch and the anchor with its
     * manifest bytes, copied since the core retains the row's checkpoint
     * under the lease while the store's own copy moves on. */
    recovered = sizeof(struct vsr_recovered);
    if (!add64(recovered, epoch, &recovered) ||
        !add64(recovered, checkpoint, &recovered) ||
        !add64(recovered, limits->manifest_bytes, &recovered)) {
        return VSR_ELIMIT;
    }
    total = VSR_IO_WIRE_ALIGN + sizeof(struct vsr_loaded);
    if (!add64(
            total,
            max64(max64(recovered, entries),
                  sizeof(struct vsr_client_record) + sizeof(struct vsr_span)),
            &total) ||
        !to_size(total, bytes)) {
        return VSR_ELIMIT;
    }
    return VSR_OK;
}

int vsr_io_codec_frame_limit(const struct vsr_limits *limits, uint64_t *bytes)
{
    uint64_t epoch;
    uint64_t entries;
    uint64_t prepare;
    uint64_t chunk;
    uint64_t total;

    if (limits == NULL || bytes == NULL) {
        return VSR_EINVAL;
    }
    if (!epoch_wire_max(limits, &epoch) ||
        !entry_array_wire_max(limits, &entries)) {
        return VSR_ELIMIT;
    }
    /* PREPARE: its header, the ENTRIES header, the entries, blob bytes. */
    prepare =
        sizeof(struct vsr_io_wire_prepare) + sizeof(struct vsr_io_wire_entries);
    if (!add64(prepare, entries, &prepare) ||
        !add64(prepare, limits->message_bytes, &prepare)) {
        return VSR_ELIMIT;
    }
    /* State chunk: its header, the log state, an epoch, the entries, a
     * checkpoint with its epoch and manifest header and padding, and the
     * aggregate blob bytes of commands and manifest. */
    chunk = sizeof(struct vsr_io_wire_state_chunk) +
            sizeof(struct vsr_io_wire_log_state) +
            sizeof(struct vsr_io_wire_entries) +
            sizeof(struct vsr_io_wire_checkpoint) +
            sizeof(struct vsr_io_wire_blob) + (VSR_IO_WIRE_ALIGN - 1);
    if (!add64(chunk, epoch, &chunk) || !add64(chunk, epoch, &chunk) ||
        !add64(chunk, entries, &chunk) ||
        !add64(chunk, limits->message_bytes, &chunk)) {
        return VSR_ELIMIT;
    }
    total = VSR_IO_FRAME_HEADER_BYTES + sizeof(struct vsr_io_wire_message);
    if (!add64(total, max64(prepare, chunk), &total) || total > UINT32_MAX) {
        return VSR_ELIMIT;
    }
    *bytes = total;
    return VSR_OK;
}

int vsr_io_codec_record_limit(const struct vsr_limits *limits, uint64_t *bytes)
{
    uint64_t epoch;
    uint64_t entries;
    uint64_t checkpoint;
    uint64_t result;
    uint64_t clients;
    uint64_t total;

    if (limits == NULL || bytes == NULL) {
        return VSR_EINVAL;
    }
    if (!epoch_wire_max(limits, &epoch) ||
        !entry_array_wire_max(limits, &entries) ||
        !checkpoint_wire_max(limits, &checkpoint) ||
        !pad64(limits->result_bytes, &result) ||
        !add64(sizeof(struct vsr_io_wire_client_record), result, &result) ||
        !mul64(result, limits->batch_entries, &clients)) {
        return VSR_ELIMIT;
    }
    total = sizeof(struct vsr_io_wire_record) +
            VSR_MAX_STORE_CHANGES * sizeof(struct vsr_io_wire_change);
    /* APPEND: the entries and their aggregate blob bytes. */
    if (!add64(total, entries, &total) ||
        !add64(total, limits->message_bytes, &total) ||
        /* CLIENTS. */
        !add64(total, clients, &total) ||
        /* HARD_STATE. */
        !add64(total, sizeof(struct vsr_io_wire_hard_state), &total) ||
        !add64(total, epoch, &total) ||
        /* PUBLISH_CHECKPOINT and RESTORE_CHECKPOINT. */
        !add64(total, checkpoint, &total) ||
        !add64(total, checkpoint, &total) ||
        /* IDENTITY; TRUNCATE and TRIM carry nothing. */
        !add64(total, sizeof(struct vsr_io_wire_identity), &total) ||
        !pad64(total, &total) || total > UINT32_MAX) {
        return VSR_ELIMIT;
    }
    *bytes = total;
    return VSR_OK;
}

int vsr_io_codec_segment_limit(const struct vsr_limits *limits, size_t *bytes)
{
    uint64_t epoch;
    uint64_t checkpoint;
    uint64_t total;

    if (limits == NULL || bytes == NULL) {
        return VSR_EINVAL;
    }
    if (!epoch_wire_max(limits, &epoch) ||
        !checkpoint_wire_max(limits, &checkpoint)) {
        return VSR_ELIMIT;
    }
    total = sizeof(struct vsr_io_wire_segment) +
            sizeof(struct vsr_io_wire_identity) +
            sizeof(struct vsr_io_wire_hard_state) + sizeof(uint32_t);
    if (!add64(total, epoch, &total) || !add64(total, checkpoint, &total) ||
        !to_size(total, bytes)) {
        return VSR_ELIMIT;
    }
    return VSR_OK;
}

/* -------------------------------------------------------------------------
 * Frames
 * ---------------------------------------------------------------------- */

void vsr_io_codec_put_frame(unsigned char *out, uint16_t kind, uint32_t length,
                            uint32_t crc)
{
    struct put put = {out};

    put_u32(&put, VSR_IO_FRAME_MAGIC);
    put_u32(&put, (uint32_t)VSR_IO_WIRE_VERSION | ((uint32_t)kind << 16));
    put_u32(&put, length);
    put_u32(&put, crc);
    put_u32(&put, vsr_io_crc32c(
                      0, out, offsetof(struct vsr_io_wire_frame, header_crc)));
    put_u32(&put, 0);
}

int vsr_io_codec_get_frame(struct vsr_io_cursor *cursor, uint32_t limit,
                           struct vsr_io_wire_frame *out)
{
    struct vsr_io_cursor copy = *cursor;
    struct vsr_io_wire_frame frame;
    uint32_t crc = 0;

    if (!vsr_io_cursor_crc(
            cursor, offsetof(struct vsr_io_wire_frame, header_crc), &crc) ||
        !vsr_io_cursor_u32(&copy, &frame.magic) ||
        !vsr_io_cursor_u16(&copy, &frame.version) ||
        !vsr_io_cursor_u16(&copy, &frame.kind) ||
        !vsr_io_cursor_u32(&copy, &frame.length) ||
        !vsr_io_cursor_u32(&copy, &frame.body_crc) ||
        !vsr_io_cursor_u32(&copy, &frame.header_crc) ||
        !vsr_io_cursor_u32(&copy, &frame.reserved)) {
        return VSR_EINVAL;
    }
    if (frame.magic != VSR_IO_FRAME_MAGIC ||
        frame.version != VSR_IO_WIRE_VERSION ||
        frame.kind < VSR_IO_FRAME_HELLO ||
        frame.kind > VSR_IO_FRAME_STREAM_END ||
        frame.length % VSR_IO_WIRE_ALIGN != 0 || frame.length > limit ||
        frame.header_crc != crc || frame.reserved != 0) {
        return VSR_EINVAL;
    }
    *out = frame;
    *cursor = copy;
    return VSR_OK;
}

void vsr_io_codec_put_hello(unsigned char *out, uint32_t handshake,
                            uint32_t purpose, uint64_t node, uint64_t nonce)
{
    struct put put = {out};

    put_u32(&put, handshake);
    put_u32(&put, purpose);
    put_u64(&put, node);
    put_u64(&put, nonce);
    put_u64(&put, 0);
}

int vsr_io_codec_get_hello(struct vsr_io_cursor *cursor,
                           struct vsr_io_wire_hello *out)
{
    struct vsr_io_cursor copy = *cursor;
    struct vsr_io_wire_hello hello;

    if (!vsr_io_cursor_u32(&copy, &hello.handshake) ||
        !vsr_io_cursor_u32(&copy, &hello.purpose) ||
        !vsr_io_cursor_u64(&copy, &hello.node) ||
        !vsr_io_cursor_u64(&copy, &hello.nonce) ||
        !vsr_io_cursor_u64(&copy, &hello.reserved)) {
        return VSR_EINVAL;
    }
    if (hello.handshake > VSR_IO_HANDSHAKE_KEYED ||
        (hello.purpose != VSR_IO_PURPOSE_PEER &&
         hello.purpose != VSR_IO_PURPOSE_STREAM) ||
        hello.reserved != 0 || vsr_io_cursor_remaining(&copy) != 0) {
        return VSR_EINVAL;
    }
    *out = hello;
    *cursor = copy;
    return VSR_OK;
}

/* Claims size bytes as one contiguous span and checks its padding. */
static int get_span(struct vsr_io_cursor *cursor, uint64_t size,
                    struct vsr_span *span)
{
    unsigned char padding[VSR_IO_WIRE_ALIGN];
    size_t pad = padding_of(size);
    bool contiguous = false;
    const void *data;

    if (size > vsr_io_cursor_remaining(cursor)) {
        return VSR_EINVAL;
    }
    data = vsr_io_cursor_span(cursor, (size_t)size, &contiguous);
    if (data == NULL || !contiguous) {
        return VSR_EINVAL;
    }
    if (!vsr_io_cursor_read(cursor, padding, pad)) {
        return VSR_EINVAL;
    }
    for (size_t i = 0; i < pad; ++i) {
        if (padding[i] != 0) {
            return VSR_EINVAL;
        }
    }
    span->data = size == 0 ? NULL : data;
    span->size = (size_t)size;
    return VSR_OK;
}

void vsr_io_codec_put_stream_request(unsigned char *out, uint32_t length)
{
    struct put put = {out};

    put_u32(&put, length);
    put_u32(&put, 0);
}

int vsr_io_codec_get_stream_request(struct vsr_io_cursor *cursor,
                                    struct vsr_span *request)
{
    struct vsr_io_cursor copy = *cursor;
    struct vsr_span span;
    uint32_t length;
    uint32_t reserved;

    if (!vsr_io_cursor_u32(&copy, &length) ||
        !vsr_io_cursor_u32(&copy, &reserved) || reserved != 0 ||
        length > VSR_IO_STREAM_REQUEST_BYTES) {
        return VSR_EINVAL;
    }
    TRY(get_span(&copy, length, &span));
    if (vsr_io_cursor_remaining(&copy) != 0) {
        return VSR_EINVAL;
    }
    *request = span;
    *cursor = copy;
    return VSR_OK;
}

void vsr_io_codec_put_stream_chunk(unsigned char *out, uint64_t offset,
                                   uint32_t length)
{
    struct put put = {out};

    put_u64(&put, offset);
    put_u32(&put, length);
    put_u32(&put, 0);
}

int vsr_io_codec_get_stream_chunk(struct vsr_io_cursor *cursor,
                                  uint64_t *offset, struct vsr_span *payload)
{
    struct vsr_io_cursor copy = *cursor;
    struct vsr_span span;
    uint64_t at;
    uint32_t length;
    uint32_t reserved;

    if (!vsr_io_cursor_u64(&copy, &at) || !vsr_io_cursor_u32(&copy, &length) ||
        !vsr_io_cursor_u32(&copy, &reserved) || reserved != 0) {
        return VSR_EINVAL;
    }
    TRY(get_span(&copy, length, &span));
    if (vsr_io_cursor_remaining(&copy) != 0) {
        return VSR_EINVAL;
    }
    *offset = at;
    *payload = span;
    *cursor = copy;
    return VSR_OK;
}

void vsr_io_codec_put_stream_end(unsigned char *out, uint64_t bytes,
                                 int32_t status)
{
    struct put put = {out};

    put_u64(&put, bytes);
    put_u32(&put, (uint32_t)status);
    put_u32(&put, 0);
}

int vsr_io_codec_get_stream_end(struct vsr_io_cursor *cursor,
                                struct vsr_io_wire_stream_end *out)
{
    struct vsr_io_cursor copy = *cursor;
    struct vsr_io_wire_stream_end end;

    if (!vsr_io_cursor_u64(&copy, &end.bytes) ||
        !vsr_io_cursor_i32(&copy, &end.status) ||
        !vsr_io_cursor_u32(&copy, &end.reserved)) {
        return VSR_EINVAL;
    }
    if (end.status < VSR_IO_OK || end.status > VSR_IO_CANCELLED ||
        end.reserved != 0 || vsr_io_cursor_remaining(&copy) != 0) {
        return VSR_EINVAL;
    }
    *out = end;
    *cursor = copy;
    return VSR_OK;
}

void vsr_io_codec_put_library_request(
    unsigned char *out, const struct vsr_io_wire_library_request *request)
{
    struct put put = {out};

    put_u64(&put, VSR_IO_LIBRARY_MAGIC);
    put_u32(&put, VSR_IO_LIBRARY_REQUEST_VERSION);
    put_u32(&put, request->kind);
    put_u64(&put, request->cluster_hi);
    put_u64(&put, request->cluster_lo);
    put_u64(&put, request->replica);
    put_u64(&put, request->snapshot_hi);
    put_u64(&put, request->snapshot_lo);
}

int vsr_io_codec_get_library_request(const struct vsr_span *request,
                                     struct vsr_io_wire_library_request *out)
{
    struct vsr_io_cursor cursor;
    struct vsr_io_wire_library_request in;

    if (request->size != sizeof(in) || request->data == NULL) {
        return VSR_EINVAL;
    }
    vsr_io_cursor_init_one(&cursor, request->data, request->size);
    if (!vsr_io_cursor_u64(&cursor, &in.magic) ||
        !vsr_io_cursor_u32(&cursor, &in.version) ||
        !vsr_io_cursor_u32(&cursor, &in.kind) ||
        !vsr_io_cursor_u64(&cursor, &in.cluster_hi) ||
        !vsr_io_cursor_u64(&cursor, &in.cluster_lo) ||
        !vsr_io_cursor_u64(&cursor, &in.replica) ||
        !vsr_io_cursor_u64(&cursor, &in.snapshot_hi) ||
        !vsr_io_cursor_u64(&cursor, &in.snapshot_lo)) {
        return VSR_EINVAL;
    }
    if (in.magic != VSR_IO_LIBRARY_MAGIC ||
        in.version != VSR_IO_LIBRARY_REQUEST_VERSION ||
        in.kind != VSR_IO_LIBRARY_CLIENTS) {
        return VSR_EINVAL;
    }
    *out = in;
    return VSR_OK;
}

/* -------------------------------------------------------------------------
 * Encoding walks
 * ---------------------------------------------------------------------- */

struct sink {
    /* false stops the walk: the sink took no more of this run. */
    bool (*put)(struct sink *sink, const void *data, size_t size, bool payload);
};

struct walker {
    struct sink *sink;
    const struct vsr_limits *limits; /* NULL: shape checks only. */
    uint64_t payload;                /* Blob bytes walked so far. */
    uint64_t aggregate;              /* Bound on payload. */
};

static int emit(struct walker *walker, const void *data, size_t size,
                bool payload)
{
    if (size == 0) {
        return VSR_OK;
    }
    return walker->sink->put(walker->sink, data, size, payload) ? VSR_OK
                                                                : VSR_AGAIN;
}

static int emit_padding(struct walker *walker, uint64_t size)
{
    return emit(walker, zeros, padding_of(size), false);
}

/* Validates a blob's shape and, against limit and the aggregate, its size;
 * accounts the size. */
static int check_blob(struct walker *walker, const struct vsr_blob *blob,
                      uint64_t limit)
{
    uint64_t total = 0;

    if (blob->reserved != 0 || (blob->count == 0) != (blob->size == 0) ||
        (blob->count == 0) != (blob->spans == NULL)) {
        return VSR_EINVAL;
    }
    for (uint32_t i = 0; i < blob->count; ++i) {
        const struct vsr_span *span = &blob->spans[i];

        if (span->size == 0 || span->data == NULL ||
            !add64(total, span->size, &total)) {
            return VSR_EINVAL;
        }
    }
    if (total != blob->size) {
        return VSR_EINVAL;
    }
    if ((walker->limits != NULL && blob->size > limit) ||
        blob->size > walker->aggregate - walker->payload) {
        return VSR_ELIMIT;
    }
    walker->payload += blob->size;
    return VSR_OK;
}

/* The bytes of a checked blob, padded. */
static int walk_bytes(struct walker *walker, const struct vsr_blob *blob)
{
    for (uint32_t i = 0; i < blob->count; ++i) {
        TRY(emit(walker, blob->spans[i].data, blob->spans[i].size, true));
    }
    return emit_padding(walker, blob->size);
}

static int walk_blob(struct walker *walker, const struct vsr_blob *blob,
                     uint64_t limit)
{
    unsigned char header[sizeof(struct vsr_io_wire_blob)];

    TRY(check_blob(walker, blob, limit));
    vsr_io_put_u64(header, blob->size);
    TRY(emit(walker, header, sizeof(header), false));
    return walk_bytes(walker, blob);
}

static int walk_membership(struct walker *walker,
                           const struct vsr_membership *membership)
{
    unsigned char header[sizeof(struct vsr_io_wire_membership)];
    struct put put = {header};

    if (membership == NULL ||
        (membership->count == 0) != (membership->members == NULL)) {
        return VSR_EINVAL;
    }
    if (walker->limits != NULL && membership->count > walker->limits->members) {
        return VSR_ELIMIT;
    }
    put_u64(&put, membership->epoch);
    put_u32(&put, membership->count);
    put_u32(&put, membership->faults);
    TRY(emit(walker, header, sizeof(header), false));
    for (uint32_t i = 0; i < membership->count; ++i) {
        const struct vsr_member *member = &membership->members[i];
        unsigned char bytes[sizeof(struct vsr_io_wire_member)];

        if (member->reserved != 0) {
            return VSR_EINVAL;
        }
        put.at = bytes;
        put_u64(&put, member->id);
        put_u32(&put, member->role);
        put_u32(&put, 0);
        TRY(emit(walker, bytes, sizeof(bytes), false));
    }
    return VSR_OK;
}

static int walk_epoch(struct walker *walker, const struct vsr_epoch *epoch)
{
    unsigned char header[sizeof(struct vsr_io_wire_epoch)];
    struct put put = {header};

    if (epoch == NULL || epoch->reserved != 0 || epoch->current == NULL) {
        return VSR_EINVAL;
    }
    put_u64(&put, epoch->boundary);
    put_u32(&put, epoch->phase);
    put_u32(&put, epoch->previous != NULL ? VSR_IO_WIRE_EPOCH_PREVIOUS : 0);
    TRY(emit(walker, header, sizeof(header), false));
    TRY(walk_membership(walker, epoch->current));
    if (epoch->previous != NULL) {
        TRY(walk_membership(walker, epoch->previous));
    }
    return VSR_OK;
}

static int walk_checkpoint(struct walker *walker,
                           const struct vsr_checkpoint *checkpoint)
{
    unsigned char header[sizeof(struct vsr_io_wire_checkpoint)];
    struct put put = {header};

    if (checkpoint == NULL) {
        return VSR_EINVAL;
    }
    put_u64(&put, checkpoint->id.hi);
    put_u64(&put, checkpoint->id.lo);
    put_u64(&put, checkpoint->op);
    put_u64(&put, checkpoint->view);
    TRY(emit(walker, header, sizeof(header), false));
    TRY(walk_epoch(walker, checkpoint->epoch));
    return walk_blob(walker, &checkpoint->manifest,
                     walker->limits != NULL ? walker->limits->manifest_bytes
                                            : 0);
}

/* Encoded bytes of an entry's body, by its type; the shape of the body
 * pointer is checked here, its contents by the walk. */
static int entry_body_bytes(const struct vsr_entry *entry, uint64_t *out)
{
    switch (entry->type) {
    case VSR_REQUEST_COMMAND: {
        const struct vsr_blob *blob = entry->body;
        uint64_t padded;

        if (blob == NULL) {
            return VSR_EINVAL;
        }
        if (!pad64(blob->size, &padded) ||
            !add64(sizeof(struct vsr_io_wire_blob), padded, out)) {
            return VSR_ELIMIT;
        }
        return VSR_OK;
    }
    case VSR_REQUEST_RECONFIGURE: {
        const struct vsr_membership *membership = entry->body;

        if (membership == NULL) {
            return VSR_EINVAL;
        }
        return membership_wire(membership->count, out) ? VSR_OK : VSR_ELIMIT;
    }
    case VSR_REQUEST_CHECK_EPOCH:
        if (entry->body == NULL) {
            return VSR_EINVAL;
        }
        *out = sizeof(struct vsr_io_wire_check_epoch);
        return VSR_OK;
    case VSR_REQUEST_NOOP:
        if (entry->body != NULL) {
            return VSR_EINVAL;
        }
        *out = 0;
        return VSR_OK;
    default:
        return VSR_EINVAL;
    }
}

static int walk_entry(struct walker *walker, const struct vsr_entry *entry)
{
    unsigned char header[sizeof(struct vsr_io_wire_entry)];
    struct put put = {header};
    uint64_t body;

    if (entry->reserved != 0) {
        return VSR_EINVAL;
    }
    TRY(entry_body_bytes(entry, &body));
    if (body > UINT32_MAX) {
        return VSR_ELIMIT;
    }
    put_u64(&put, entry->op);
    put_u64(&put, entry->epoch);
    put_u64(&put, entry->view);
    put_u64(&put, entry->request.client.hi);
    put_u64(&put, entry->request.client.lo);
    put_u64(&put, entry->request.number);
    put_u32(&put, entry->type);
    put_u32(&put, (uint32_t)body);
    TRY(emit(walker, header, sizeof(header), false));
    switch (entry->type) {
    case VSR_REQUEST_COMMAND:
        return walk_blob(walker, entry->body,
                         walker->limits != NULL ? walker->limits->command_bytes
                                                : 0);
    case VSR_REQUEST_RECONFIGURE:
        return walk_membership(walker, entry->body);
    case VSR_REQUEST_CHECK_EPOCH: {
        const struct vsr_check_epoch *check = entry->body;
        unsigned char bytes[sizeof(struct vsr_io_wire_check_epoch)];

        vsr_io_put_u64(bytes, check->epoch);
        return emit(walker, bytes, sizeof(bytes), false);
    }
    default:
        return VSR_OK;
    }
}

/* count entries back to back, as an APPEND payload. */
static int walk_entry_array(struct walker *walker,
                            const struct vsr_entry *entries, uint32_t count)
{
    for (uint32_t i = 0; i < count; ++i) {
        TRY(walk_entry(walker, &entries[i]));
    }
    return VSR_OK;
}

static int walk_entries(struct walker *walker,
                        const struct vsr_entries *entries)
{
    unsigned char header[sizeof(struct vsr_io_wire_entries)];
    struct put put = {header};

    if (entries->reserved != 0 ||
        (entries->count == 0) != (entries->entries == NULL)) {
        return VSR_EINVAL;
    }
    if (walker->limits != NULL &&
        entries->count > walker->limits->batch_entries) {
        return VSR_ELIMIT;
    }
    put_u32(&put, entries->count);
    put_u32(&put, 0);
    TRY(emit(walker, header, sizeof(header), false));
    return walk_entry_array(walker, entries->entries, entries->count);
}

static int walk_log_state(struct walker *walker,
                          const struct vsr_log_state *state)
{
    unsigned char header[sizeof(struct vsr_io_wire_log_state)];
    struct put put = {header};

    if (state == NULL) {
        return VSR_EINVAL;
    }
    put_u64(&put, state->revision.incarnation.hi);
    put_u64(&put, state->revision.incarnation.lo);
    put_u64(&put, state->revision.sequence);
    put_u64(&put, state->view);
    put_u64(&put, state->last_normal_view);
    put_u64(&put, state->committed);
    put_u64(&put, state->log_begin);
    put_u64(&put, state->log_end);
    put_u32(&put, state->checkpoint != NULL ? VSR_IO_WIRE_LOG_CHECKPOINT : 0);
    put_u32(&put, 0);
    TRY(emit(walker, header, sizeof(header), false));
    TRY(walk_epoch(walker, state->epoch));
    TRY(walk_entries(walker, &state->entries));
    if (state->checkpoint != NULL) {
        TRY(walk_checkpoint(walker, state->checkpoint));
    }
    return VSR_OK;
}

static void put_nonce(struct put *put, const struct vsr_nonce *nonce)
{
    put_u64(put, nonce->incarnation.hi);
    put_u64(put, nonce->incarnation.lo);
    put_u64(put, nonce->counter);
}

static int walk_message(struct walker *walker,
                        const struct vsr_message *message)
{
    unsigned char header[sizeof(struct vsr_io_wire_fetch)];
    struct put put = {header};

    if (message == NULL || message->flags != 0) {
        return VSR_EINVAL;
    }
    put_u64(&put, message->cluster.hi);
    put_u64(&put, message->cluster.lo);
    put_u64(&put, message->epoch);
    put_u64(&put, message->view);
    put_u64(&put, message->from);
    put_u32(&put, message->type);
    put_u32(&put, message->flags);
    put_u64(&put, message->number);
    TRY(emit(walker, header, sizeof(struct vsr_io_wire_message), false));
    put.at = header;
    switch (message->type) {
    case VSR_MSG_PREPARE: {
        const struct vsr_prepare *prepare = message->body;

        if (prepare == NULL) {
            return VSR_EINVAL;
        }
        put_u64(&put, prepare->committed);
        TRY(emit(walker, header, sizeof(struct vsr_io_wire_prepare), false));
        return walk_entries(walker, &prepare->batch);
    }
    case VSR_MSG_PREPARE_OK:
    case VSR_MSG_COMMIT:
    case VSR_MSG_START_VIEW_CHANGE:
    case VSR_MSG_EPOCH_STARTED:
        return message->body == NULL ? VSR_OK : VSR_EINVAL;
    case VSR_MSG_DO_VIEW_CHANGE:
    case VSR_MSG_START_VIEW:
        return walk_log_state(walker, message->body);
    case VSR_MSG_RECOVERY:
    case VSR_MSG_RECOVERY_RESPONSE: {
        const struct vsr_recovery *recovery = message->body;

        if (recovery == NULL) {
            return VSR_EINVAL;
        }
        put_nonce(&put, &recovery->nonce);
        put_u32(&put, recovery->state != NULL ? VSR_IO_WIRE_RECOVERY_STATE : 0);
        put_u32(&put, 0);
        TRY(emit(walker, header, sizeof(struct vsr_io_wire_recovery), false));
        if (recovery->state != NULL) {
            return walk_log_state(walker, recovery->state);
        }
        return VSR_OK;
    }
    case VSR_MSG_GET_STATE:
    case VSR_MSG_GET_LOG: {
        const struct vsr_fetch *fetch = message->body;

        if (fetch == NULL || fetch->reserved != 0) {
            return VSR_EINVAL;
        }
        put_nonce(&put, &fetch->nonce);
        put_u64(&put, fetch->revision.incarnation.hi);
        put_u64(&put, fetch->revision.incarnation.lo);
        put_u64(&put, fetch->revision.sequence);
        put_u64(&put, fetch->first);
        put_u64(&put, fetch->end);
        put_u64(&put, fetch->max_bytes);
        put_u32(&put, fetch->max_entries);
        put_u32(&put, 0);
        return emit(walker, header, sizeof(struct vsr_io_wire_fetch), false);
    }
    case VSR_MSG_NEW_STATE:
    case VSR_MSG_LOG:
    case VSR_MSG_STATE_UNAVAILABLE: {
        const struct vsr_state_chunk *chunk = message->body;

        if (chunk == NULL) {
            return VSR_EINVAL;
        }
        put_nonce(&put, &chunk->nonce);
        put_u64(&put, chunk->first);
        put_u64(&put, chunk->next);
        TRY(emit(walker, header, sizeof(struct vsr_io_wire_state_chunk),
                 false));
        return walk_log_state(walker, &chunk->state);
    }
    case VSR_MSG_START_EPOCH:
    case VSR_MSG_NEW_EPOCH:
        return walk_epoch(walker, message->body);
    case VSR_MSG_CHECKPOINT:
        return walk_checkpoint(walker, message->body);
    case VSR_MSG_READ_PROBE:
    case VSR_MSG_READ_ACK: {
        const struct vsr_nonce *nonce = message->body;

        if (nonce == NULL) {
            return VSR_EINVAL;
        }
        put_nonce(&put, nonce);
        return emit(walker, header, sizeof(struct vsr_io_wire_nonce), false);
    }
    default:
        return VSR_EINVAL;
    }
}

static int walk_hard_state(struct walker *walker,
                           const struct vsr_hard_state *hard)
{
    unsigned char header[sizeof(struct vsr_io_wire_hard_state)];
    struct put put = {header};

    if (hard == NULL) {
        return VSR_EINVAL;
    }
    put_u64(&put, hard->view);
    put_u64(&put, hard->last_normal_view);
    put_u64(&put, hard->committed);
    put_u32(&put, hard->state);
    put_u32(&put, hard->role);
    TRY(emit(walker, header, sizeof(header), false));
    return walk_epoch(walker, hard->epoch);
}

static int walk_identity(struct walker *walker,
                         const struct vsr_store_identity *identity)
{
    unsigned char header[sizeof(struct vsr_io_wire_identity)];
    struct put put = {header};

    if (identity == NULL || identity->reserved != 0) {
        return VSR_EINVAL;
    }
    put_u64(&put, identity->cluster.hi);
    put_u64(&put, identity->cluster.lo);
    put_u64(&put, identity->replica);
    put_u32(&put, identity->durability);
    put_u32(&put, 0);
    return emit(walker, header, sizeof(header), false);
}

/* A client record: its header, then the result bytes without a blob
 * header, padded. */
static int walk_client_record(struct walker *walker,
                              const struct vsr_client_record *record)
{
    unsigned char header[sizeof(struct vsr_io_wire_client_record)];
    struct put put = {header};

    if (record->result.reserved != 0) {
        return VSR_EINVAL;
    }
    TRY(check_blob(walker, &record->result.data,
                   walker->limits != NULL ? walker->limits->result_bytes : 0));
    if (record->result.data.size > UINT32_MAX) {
        return VSR_ELIMIT;
    }
    put_u64(&put, record->request.client.hi);
    put_u64(&put, record->request.client.lo);
    put_u64(&put, record->request.number);
    put_u64(&put, record->op);
    put_u32(&put, (uint32_t)record->result.code);
    put_u32(&put, (uint32_t)record->result.data.size);
    TRY(emit(walker, header, sizeof(header), false));
    return walk_bytes(walker, &record->result.data);
}

/* -------------------------------------------------------------------------
 * Sinks
 * ---------------------------------------------------------------------- */

/* Counts bytes and, when digesting, checksums them. */
struct measure {
    struct sink sink;
    uint64_t length;
    uint32_t crc;
    bool digest;
    bool overflow;
};

static bool measure_put(struct sink *sink, const void *data, size_t size,
                        bool payload)
{
    struct measure *measure = (struct measure *)sink;

    (void)payload;
    if (size > UINT64_MAX - measure->length) {
        measure->overflow = true;
        return false;
    }
    measure->length += size;
    if (measure->digest) {
        measure->crc = vsr_io_crc32c(measure->crc, data, size);
    }
    return true;
}

static void measure_init(struct measure *measure, bool digest)
{
    measure->sink.put = measure_put;
    measure->length = 0;
    measure->crc = 0;
    measure->digest = digest;
    measure->overflow = false;
}

/* Copies into a bounded buffer. */
struct copy {
    struct sink sink;
    unsigned char *out;
    size_t capacity;
    size_t used;
    bool overflow;
};

static bool copy_put(struct sink *sink, const void *data, size_t size,
                     bool payload)
{
    struct copy *copy = (struct copy *)sink;

    (void)payload;
    if (size > copy->capacity - copy->used) {
        copy->overflow = true;
        return false;
    }
    memcpy(copy->out + copy->used, data, size);
    copy->used += size;
    return true;
}

static void copy_init(struct copy *copy, unsigned char *out, size_t capacity)
{
    copy->sink.put = copy_put;
    copy->out = out;
    copy->capacity = capacity;
    copy->used = 0;
    copy->overflow = false;
}

/*
 * Produces vectors: header runs and small payloads are copied into the
 * writer, larger payloads referenced in place. `skip` bytes of the walk
 * were emitted by earlier calls; `position` counts the bytes walked, so a
 * stop in the middle of a run resumes exactly there next time.
 */
struct vector {
    struct sink sink;
    struct vsr_io_writer *writer;
    struct vsr_io_vec *vecs;
    uint32_t capacity;
    uint32_t count;
    uint64_t budget;
    uint64_t skip;
    uint64_t position;
};

/* Appends [base, base + size) as a vector, extending the last one when it
 * ends exactly there; false when every vector is used. */
static bool vector_append(struct vector *vector, const unsigned char *base,
                          size_t size)
{
    if (vector->count > 0) {
        struct vsr_io_vec *last = &vector->vecs[vector->count - 1];

        if ((const unsigned char *)last->base + last->length == base) {
            last->length += size;
            return true;
        }
    }
    if (vector->count == vector->capacity) {
        return false;
    }
    /* vsr_io_vec is the executor's descriptor and is not const-qualified;
     * a send never writes through it. The uintptr_t round trip only drops
     * the qualifier (-Wcast-qual), so there is no provenance to lose. */
    /* NOLINTNEXTLINE(performance-no-int-to-ptr) */
    vector->vecs[vector->count].base = (void *)(uintptr_t)base;
    vector->vecs[vector->count].length = size;
    vector->count++;
    return true;
}

static bool vector_put(struct sink *sink, const void *data, size_t size,
                       bool payload)
{
    struct vector *vector = (struct vector *)sink;
    const unsigned char *bytes = data;
    bool reference = payload && size >= VSR_IO_INLINE_BYTES;

    if (size <= vector->skip - vector->position) {
        vector->position += size;
        return true;
    }
    if (vector->skip > vector->position) {
        size_t done = (size_t)(vector->skip - vector->position);

        bytes += done;
        size -= done;
        vector->position = vector->skip;
    }
    while (size > 0) {
        size_t take = size;

        if (vector->budget == 0) {
            return false;
        }
        if (take > vector->budget) {
            take = (size_t)vector->budget;
        }
        if (reference) {
            if (!vector_append(vector, bytes, take)) {
                return false;
            }
        } else {
            struct vsr_io_writer *writer = vector->writer;
            size_t room = writer->capacity - writer->used;
            unsigned char *out = writer->base + writer->used;

            if (room == 0) {
                return false;
            }
            if (take > room) {
                take = room;
            }
            if (!vector_append(vector, out, take)) {
                return false;
            }
            memcpy(out, bytes, take);
            writer->used += take;
        }
        bytes += take;
        size -= take;
        vector->budget -= take;
        vector->position += take;
        vector->skip = vector->position;
    }
    return true;
}

static void vector_init(struct vector *vector, struct vsr_io_writer *writer,
                        struct vsr_io_vec *vecs, uint32_t capacity,
                        uint64_t budget, uint64_t skip)
{
    vector->sink.put = vector_put;
    vector->writer = writer;
    vector->vecs = vecs;
    vector->capacity = capacity;
    vector->count = 0;
    vector->budget = budget;
    vector->skip = skip;
    vector->position = 0;
}

/* -------------------------------------------------------------------------
 * Messages
 * ---------------------------------------------------------------------- */

int vsr_io_codec_message_digest(const struct vsr_message *message,
                                const struct vsr_limits *limits,
                                uint32_t *length, uint32_t *crc)
{
    struct measure measure;
    struct walker walker;
    uint64_t frame_limit;
    int rc;

    if (message == NULL || limits == NULL || length == NULL || crc == NULL) {
        return VSR_EINVAL;
    }
    TRY(vsr_io_codec_frame_limit(limits, &frame_limit));
    measure_init(&measure, true);
    walker.sink = &measure.sink;
    walker.limits = limits;
    walker.payload = 0;
    walker.aggregate = limits->message_bytes;
    rc = walk_message(&walker, message);
    if (rc == VSR_AGAIN) {
        return VSR_ELIMIT;
    }
    TRY(rc);
    if (measure.length > frame_limit - VSR_IO_FRAME_HEADER_BYTES) {
        return VSR_ELIMIT;
    }
    *length = (uint32_t)measure.length;
    *crc = measure.crc;
    return VSR_OK;
}

void vsr_io_encoder_begin(struct vsr_io_encoder *encoder,
                          const struct vsr_message *message, uint32_t length,
                          uint32_t crc)
{
    encoder->message = message;
    encoder->length = length;
    encoder->crc = crc;
    encoder->stage = 0;
    encoder->entry = 0;
    encoder->span = 0;
    encoder->reserved = 0;
    encoder->offset = 0;
}

int vsr_io_encoder_emit(struct vsr_io_encoder *encoder,
                        struct vsr_io_writer *writer, struct vsr_io_vec *vecs,
                        uint32_t capacity, uint32_t *count, uint64_t budget,
                        bool *done)
{
    unsigned char header[VSR_IO_FRAME_HEADER_BYTES];
    struct vector vector;
    struct walker walker;
    int rc;

    *count = 0;
    *done = false;
    if (encoder->message == NULL || writer == NULL ||
        (vecs == NULL && capacity != 0) || writer->used > writer->capacity) {
        return VSR_EINVAL;
    }
    vector_init(&vector, writer, vecs, capacity, budget, encoder->offset);
    walker.sink = &vector.sink;
    walker.limits = NULL;
    walker.payload = 0;
    walker.aggregate = UINT64_MAX;
    vsr_io_codec_put_frame(header, VSR_IO_FRAME_MESSAGE, encoder->length,
                           encoder->crc);
    rc = emit(&walker, header, sizeof(header), false);
    if (rc == VSR_OK) {
        rc = walk_message(&walker, encoder->message);
    }
    encoder->offset = vector.position;
    *count = vector.count;
    if (rc == VSR_AGAIN) {
        return VSR_OK;
    }
    TRY(rc);
    /* The graph must not have changed since its digest. */
    if (vector.position !=
        (uint64_t)encoder->length + VSR_IO_FRAME_HEADER_BYTES) {
        return VSR_EINVAL;
    }
    *done = true;
    return VSR_OK;
}

/* -------------------------------------------------------------------------
 * Decoding
 * ---------------------------------------------------------------------- */

struct decoder {
    struct vsr_io_cursor *cursor;
    const struct vsr_limits *limits;
    struct vsr_io_bump *region;
    uint64_t payload;   /* Blob bytes decoded so far. */
    uint64_t aggregate; /* Bound on payload. */
};

static void decoder_init(struct decoder *decoder, struct vsr_io_cursor *cursor,
                         const struct vsr_limits *limits,
                         struct vsr_io_bump *region, uint64_t aggregate)
{
    decoder->cursor = cursor;
    decoder->limits = limits;
    decoder->region = region;
    decoder->payload = 0;
    decoder->aggregate = aggregate;
}

static int read_u32(struct decoder *decoder, uint32_t *value)
{
    return vsr_io_cursor_u32(decoder->cursor, value) ? VSR_OK : VSR_EINVAL;
}

static int read_u64(struct decoder *decoder, uint64_t *value)
{
    return vsr_io_cursor_u64(decoder->cursor, value) ? VSR_OK : VSR_EINVAL;
}

static int read_i32(struct decoder *decoder, int32_t *value)
{
    return vsr_io_cursor_i32(decoder->cursor, value) ? VSR_OK : VSR_EINVAL;
}

/* A reserved 32-bit field, which must be zero. */
static int read_reserved(struct decoder *decoder)
{
    uint32_t reserved;

    TRY(read_u32(decoder, &reserved));
    return reserved == 0 ? VSR_OK : VSR_EINVAL;
}

#define NEW(decoder, type)                                                     \
    ((type *)vsr_io_bump_alloc((decoder)->region, sizeof(type), alignof(type)))

static void *new_array(struct decoder *decoder, uint32_t count, size_t size,
                       size_t alignment)
{
    size_t bytes;

    if (!vsr_size_mul(count, size, &bytes)) {
        return NULL;
    }
    return vsr_io_bump_alloc(decoder->region, bytes, alignment);
}

/* size bytes of payload into one span from the region, padded; the size
 * is checked against its object limit and the aggregate. */
static int decode_bytes(struct decoder *decoder, uint64_t size, uint64_t limit,
                        struct vsr_blob *blob)
{
    struct vsr_span *span;

    if (size > limit || size > decoder->aggregate - decoder->payload) {
        return VSR_ELIMIT;
    }
    decoder->payload += size;
    if (size == 0) {
        struct vsr_span empty;

        blob->spans = NULL;
        blob->size = 0;
        blob->count = 0;
        blob->reserved = 0;
        return get_span(decoder->cursor, 0, &empty);
    }
    span = NEW(decoder, struct vsr_span);
    if (span == NULL) {
        return VSR_ELIMIT;
    }
    TRY(get_span(decoder->cursor, size, span));
    blob->spans = span;
    blob->size = size;
    blob->count = 1;
    blob->reserved = 0;
    return VSR_OK;
}

static int decode_blob(struct decoder *decoder, uint64_t limit,
                       struct vsr_blob *blob)
{
    uint64_t size;

    TRY(read_u64(decoder, &size));
    return decode_bytes(decoder, size, limit, blob);
}

static int decode_membership(struct decoder *decoder,
                             struct vsr_membership **out)
{
    struct vsr_membership *membership;
    struct vsr_member *members = NULL;
    uint64_t epoch;
    uint32_t count;
    uint32_t faults;

    TRY(read_u64(decoder, &epoch));
    TRY(read_u32(decoder, &count));
    TRY(read_u32(decoder, &faults));
    if (count > decoder->limits->members) {
        return VSR_ELIMIT;
    }
    membership = NEW(decoder, struct vsr_membership);
    if (membership == NULL) {
        return VSR_ELIMIT;
    }
    if (count > 0) {
        members = new_array(decoder, count, sizeof(*members),
                            alignof(struct vsr_member));
        if (members == NULL) {
            return VSR_ELIMIT;
        }
        for (uint32_t i = 0; i < count; ++i) {
            TRY(read_u64(decoder, &members[i].id));
            TRY(read_u32(decoder, &members[i].role));
            TRY(read_reserved(decoder));
            members[i].reserved = 0;
        }
    }
    membership->epoch = epoch;
    membership->members = members;
    membership->count = count;
    membership->faults = faults;
    *out = membership;
    return VSR_OK;
}

static int decode_epoch(struct decoder *decoder, struct vsr_epoch **out)
{
    struct vsr_epoch *epoch;
    struct vsr_membership *current;
    struct vsr_membership *previous = NULL;
    uint64_t boundary;
    uint32_t phase;
    uint32_t flags;

    TRY(read_u64(decoder, &boundary));
    TRY(read_u32(decoder, &phase));
    TRY(read_u32(decoder, &flags));
    if ((flags & ~VSR_IO_WIRE_EPOCH_PREVIOUS) != 0) {
        return VSR_EINVAL;
    }
    epoch = NEW(decoder, struct vsr_epoch);
    if (epoch == NULL) {
        return VSR_ELIMIT;
    }
    TRY(decode_membership(decoder, &current));
    if ((flags & VSR_IO_WIRE_EPOCH_PREVIOUS) != 0) {
        TRY(decode_membership(decoder, &previous));
    }
    epoch->current = current;
    epoch->previous = previous;
    epoch->boundary = boundary;
    epoch->phase = phase;
    epoch->reserved = 0;
    *out = epoch;
    return VSR_OK;
}

static int decode_checkpoint(struct decoder *decoder,
                             struct vsr_checkpoint **out)
{
    struct vsr_checkpoint *checkpoint;
    struct vsr_epoch *epoch;

    checkpoint = NEW(decoder, struct vsr_checkpoint);
    if (checkpoint == NULL) {
        return VSR_ELIMIT;
    }
    TRY(read_u64(decoder, &checkpoint->id.hi));
    TRY(read_u64(decoder, &checkpoint->id.lo));
    TRY(read_u64(decoder, &checkpoint->op));
    TRY(read_u64(decoder, &checkpoint->view));
    TRY(decode_epoch(decoder, &epoch));
    checkpoint->epoch = epoch;
    TRY(decode_blob(decoder, decoder->limits->manifest_bytes,
                    &checkpoint->manifest));
    *out = checkpoint;
    return VSR_OK;
}

static int decode_entry(struct decoder *decoder, struct vsr_entry *entry)
{
    uint32_t body_length;
    size_t start;

    TRY(read_u64(decoder, &entry->op));
    TRY(read_u64(decoder, &entry->epoch));
    TRY(read_u64(decoder, &entry->view));
    TRY(read_u64(decoder, &entry->request.client.hi));
    TRY(read_u64(decoder, &entry->request.client.lo));
    TRY(read_u64(decoder, &entry->request.number));
    TRY(read_u32(decoder, &entry->type));
    TRY(read_u32(decoder, &body_length));
    entry->reserved = 0;
    entry->body = NULL;
    if (body_length % VSR_IO_WIRE_ALIGN != 0) {
        return VSR_EINVAL;
    }
    start = decoder->cursor->position;
    switch (entry->type) {
    case VSR_REQUEST_COMMAND: {
        struct vsr_blob *blob = NEW(decoder, struct vsr_blob);

        if (blob == NULL) {
            return VSR_ELIMIT;
        }
        TRY(decode_blob(decoder, decoder->limits->command_bytes, blob));
        entry->body = blob;
        break;
    }
    case VSR_REQUEST_RECONFIGURE: {
        struct vsr_membership *membership;

        TRY(decode_membership(decoder, &membership));
        entry->body = membership;
        break;
    }
    case VSR_REQUEST_CHECK_EPOCH: {
        struct vsr_check_epoch *check = NEW(decoder, struct vsr_check_epoch);

        if (check == NULL) {
            return VSR_ELIMIT;
        }
        TRY(read_u64(decoder, &check->epoch));
        entry->body = check;
        break;
    }
    case VSR_REQUEST_NOOP:
        break;
    default:
        return VSR_EINVAL;
    }
    return decoder->cursor->position - start == body_length ? VSR_OK
                                                            : VSR_EINVAL;
}

static int decode_entry_array(struct decoder *decoder, uint32_t count,
                              struct vsr_entry **out)
{
    struct vsr_entry *entries;

    if (count > decoder->limits->batch_entries) {
        return VSR_ELIMIT;
    }
    if (count == 0) {
        *out = NULL;
        return VSR_OK;
    }
    entries =
        new_array(decoder, count, sizeof(*entries), alignof(struct vsr_entry));
    if (entries == NULL) {
        return VSR_ELIMIT;
    }
    for (uint32_t i = 0; i < count; ++i) {
        TRY(decode_entry(decoder, &entries[i]));
    }
    *out = entries;
    return VSR_OK;
}

static int decode_entries(struct decoder *decoder, struct vsr_entries *entries)
{
    struct vsr_entry *array;
    uint32_t count;

    TRY(read_u32(decoder, &count));
    TRY(read_reserved(decoder));
    TRY(decode_entry_array(decoder, count, &array));
    entries->entries = array;
    entries->count = count;
    entries->reserved = 0;
    return VSR_OK;
}

static int decode_log_state(struct decoder *decoder,
                            struct vsr_log_state *state)
{
    struct vsr_epoch *epoch;
    struct vsr_checkpoint *checkpoint = NULL;
    uint32_t flags;

    TRY(read_u64(decoder, &state->revision.incarnation.hi));
    TRY(read_u64(decoder, &state->revision.incarnation.lo));
    TRY(read_u64(decoder, &state->revision.sequence));
    TRY(read_u64(decoder, &state->view));
    TRY(read_u64(decoder, &state->last_normal_view));
    TRY(read_u64(decoder, &state->committed));
    TRY(read_u64(decoder, &state->log_begin));
    TRY(read_u64(decoder, &state->log_end));
    TRY(read_u32(decoder, &flags));
    TRY(read_reserved(decoder));
    if ((flags & ~VSR_IO_WIRE_LOG_CHECKPOINT) != 0) {
        return VSR_EINVAL;
    }
    TRY(decode_epoch(decoder, &epoch));
    state->epoch = epoch;
    TRY(decode_entries(decoder, &state->entries));
    if ((flags & VSR_IO_WIRE_LOG_CHECKPOINT) != 0) {
        TRY(decode_checkpoint(decoder, &checkpoint));
    }
    state->checkpoint = checkpoint;
    return VSR_OK;
}

static int decode_nonce(struct decoder *decoder, struct vsr_nonce *nonce)
{
    TRY(read_u64(decoder, &nonce->incarnation.hi));
    TRY(read_u64(decoder, &nonce->incarnation.lo));
    return read_u64(decoder, &nonce->counter);
}

int vsr_io_codec_decode_message(struct vsr_io_cursor *cursor,
                                const struct vsr_limits *limits,
                                struct vsr_io_bump *region,
                                struct vsr_message **out)
{
    struct decoder decoder;
    struct vsr_message *message;

    if (cursor == NULL || limits == NULL || region == NULL || out == NULL) {
        return VSR_EINVAL;
    }
    decoder_init(&decoder, cursor, limits, region, limits->message_bytes);
    message = NEW(&decoder, struct vsr_message);
    if (message == NULL) {
        return VSR_ELIMIT;
    }
    TRY(read_u64(&decoder, &message->cluster.hi));
    TRY(read_u64(&decoder, &message->cluster.lo));
    TRY(read_u64(&decoder, &message->epoch));
    TRY(read_u64(&decoder, &message->view));
    TRY(read_u64(&decoder, &message->from));
    TRY(read_u32(&decoder, &message->type));
    TRY(read_u32(&decoder, &message->flags));
    TRY(read_u64(&decoder, &message->number));
    message->body = NULL;
    if (message->flags != 0) {
        return VSR_EINVAL;
    }
    switch (message->type) {
    case VSR_MSG_PREPARE: {
        struct vsr_prepare *prepare = NEW(&decoder, struct vsr_prepare);

        if (prepare == NULL) {
            return VSR_ELIMIT;
        }
        TRY(read_u64(&decoder, &prepare->committed));
        TRY(decode_entries(&decoder, &prepare->batch));
        message->body = prepare;
        break;
    }
    case VSR_MSG_PREPARE_OK:
    case VSR_MSG_COMMIT:
    case VSR_MSG_START_VIEW_CHANGE:
    case VSR_MSG_EPOCH_STARTED:
        break;
    case VSR_MSG_DO_VIEW_CHANGE:
    case VSR_MSG_START_VIEW: {
        struct vsr_log_state *state = NEW(&decoder, struct vsr_log_state);

        if (state == NULL) {
            return VSR_ELIMIT;
        }
        TRY(decode_log_state(&decoder, state));
        message->body = state;
        break;
    }
    case VSR_MSG_RECOVERY:
    case VSR_MSG_RECOVERY_RESPONSE: {
        struct vsr_recovery *recovery = NEW(&decoder, struct vsr_recovery);
        struct vsr_log_state *state = NULL;
        uint32_t flags;

        if (recovery == NULL) {
            return VSR_ELIMIT;
        }
        TRY(decode_nonce(&decoder, &recovery->nonce));
        TRY(read_u32(&decoder, &flags));
        TRY(read_reserved(&decoder));
        if ((flags & ~VSR_IO_WIRE_RECOVERY_STATE) != 0) {
            return VSR_EINVAL;
        }
        if ((flags & VSR_IO_WIRE_RECOVERY_STATE) != 0) {
            state = NEW(&decoder, struct vsr_log_state);
            if (state == NULL) {
                return VSR_ELIMIT;
            }
            TRY(decode_log_state(&decoder, state));
        }
        recovery->state = state;
        message->body = recovery;
        break;
    }
    case VSR_MSG_GET_STATE:
    case VSR_MSG_GET_LOG: {
        struct vsr_fetch *fetch = NEW(&decoder, struct vsr_fetch);

        if (fetch == NULL) {
            return VSR_ELIMIT;
        }
        TRY(decode_nonce(&decoder, &fetch->nonce));
        TRY(read_u64(&decoder, &fetch->revision.incarnation.hi));
        TRY(read_u64(&decoder, &fetch->revision.incarnation.lo));
        TRY(read_u64(&decoder, &fetch->revision.sequence));
        TRY(read_u64(&decoder, &fetch->first));
        TRY(read_u64(&decoder, &fetch->end));
        TRY(read_u64(&decoder, &fetch->max_bytes));
        TRY(read_u32(&decoder, &fetch->max_entries));
        TRY(read_reserved(&decoder));
        fetch->reserved = 0;
        message->body = fetch;
        break;
    }
    case VSR_MSG_NEW_STATE:
    case VSR_MSG_LOG:
    case VSR_MSG_STATE_UNAVAILABLE: {
        struct vsr_state_chunk *chunk = NEW(&decoder, struct vsr_state_chunk);

        if (chunk == NULL) {
            return VSR_ELIMIT;
        }
        TRY(decode_nonce(&decoder, &chunk->nonce));
        TRY(read_u64(&decoder, &chunk->first));
        TRY(read_u64(&decoder, &chunk->next));
        TRY(decode_log_state(&decoder, &chunk->state));
        message->body = chunk;
        break;
    }
    case VSR_MSG_START_EPOCH:
    case VSR_MSG_NEW_EPOCH: {
        struct vsr_epoch *epoch;

        TRY(decode_epoch(&decoder, &epoch));
        message->body = epoch;
        break;
    }
    case VSR_MSG_CHECKPOINT: {
        struct vsr_checkpoint *checkpoint;

        TRY(decode_checkpoint(&decoder, &checkpoint));
        message->body = checkpoint;
        break;
    }
    case VSR_MSG_READ_PROBE:
    case VSR_MSG_READ_ACK: {
        struct vsr_nonce *nonce = NEW(&decoder, struct vsr_nonce);

        if (nonce == NULL) {
            return VSR_ELIMIT;
        }
        TRY(decode_nonce(&decoder, nonce));
        message->body = nonce;
        break;
    }
    default:
        return VSR_EINVAL;
    }
    if (vsr_io_cursor_remaining(cursor) != 0) {
        return VSR_EINVAL;
    }
    *out = message;
    return VSR_OK;
}

/* -------------------------------------------------------------------------
 * Store records
 * ---------------------------------------------------------------------- */

static int check_transaction(const struct vsr_store *transaction)
{
    if (transaction == NULL || transaction->changes == NULL ||
        transaction->count == 0 || transaction->count > VSR_MAX_STORE_CHANGES ||
        transaction->reserved != 0) {
        return VSR_EINVAL;
    }
    return VSR_OK;
}

/* The shape a change's descriptor must have for its type: single-object
 * changes carry exactly one object, TRUNCATE and TRIM none, APPEND and
 * CLIENTS a nonempty array. */
static bool change_shape(uint32_t type, uint32_t count, bool present)
{
    switch (type) {
    case VSR_STORE_APPEND:
    case VSR_STORE_CLIENTS:
        return count != 0 && present;
    case VSR_STORE_TRUNCATE:
    case VSR_STORE_TRIM:
        return count == 0 && !present;
    case VSR_STORE_HARD_STATE:
    case VSR_STORE_PUBLISH_CHECKPOINT:
    case VSR_STORE_RESTORE_CHECKPOINT:
    case VSR_STORE_IDENTITY:
        return count == 1 && present;
    default:
        return false;
    }
}

static int walk_change(struct walker *walker, const struct vsr_change *change)
{
    if (!change_shape(change->type, change->count, change->data != NULL)) {
        return VSR_EINVAL;
    }
    walker->payload = 0;
    walker->aggregate = UINT64_MAX;
    switch (change->type) {
    case VSR_STORE_APPEND:
        if (walker->limits != NULL) {
            if (change->count > walker->limits->batch_entries) {
                return VSR_ELIMIT;
            }
            walker->aggregate = walker->limits->message_bytes;
        }
        return walk_entry_array(walker, change->data, change->count);
    case VSR_STORE_CLIENTS: {
        const struct vsr_client_record *records = change->data;

        if (walker->limits != NULL &&
            change->count > walker->limits->batch_entries) {
            return VSR_ELIMIT;
        }
        for (uint32_t i = 0; i < change->count; ++i) {
            TRY(walk_client_record(walker, &records[i]));
        }
        return VSR_OK;
    }
    case VSR_STORE_HARD_STATE:
        return walk_hard_state(walker, change->data);
    case VSR_STORE_PUBLISH_CHECKPOINT:
    case VSR_STORE_RESTORE_CHECKPOINT:
        return walk_checkpoint(walker, change->data);
    case VSR_STORE_IDENTITY:
        return walk_identity(walker, change->data);
    default:
        return VSR_OK;
    }
}

int vsr_io_codec_record_bytes(const struct vsr_store *transaction,
                              const struct vsr_limits *limits, size_t *bytes)
{
    struct measure measure;
    struct walker walker;
    uint64_t limit;
    uint64_t total;

    if (limits == NULL || bytes == NULL) {
        return VSR_EINVAL;
    }
    TRY(check_transaction(transaction));
    TRY(vsr_io_codec_record_limit(limits, &limit));
    measure_init(&measure, false);
    walker.sink = &measure.sink;
    walker.limits = limits;
    walker.payload = 0;
    walker.aggregate = UINT64_MAX;
    for (uint32_t i = 0; i < transaction->count; ++i) {
        int rc = walk_change(&walker, &transaction->changes[i]);

        if (rc == VSR_AGAIN) {
            return VSR_ELIMIT;
        }
        TRY(rc);
    }
    total = sizeof(struct vsr_io_wire_record) +
            (uint64_t)transaction->count * sizeof(struct vsr_io_wire_change);
    if (!add64(total, measure.length, &total) || !pad64(total, &total) ||
        total > limit) {
        return VSR_ELIMIT;
    }
    *bytes = (size_t)total;
    return VSR_OK;
}

int vsr_io_codec_put_record(const struct vsr_store *transaction,
                            uint64_t generation, uint32_t run, uint64_t flushed,
                            unsigned char *out, size_t capacity,
                            size_t *written)
{
    struct copy copy;
    struct walker walker;
    struct put put = {out};
    uint32_t offsets[VSR_MAX_STORE_CHANGES];
    uint32_t lengths[VSR_MAX_STORE_CHANGES];
    size_t fixed;
    size_t total;
    size_t padded;

    if (out == NULL || written == NULL) {
        return VSR_EINVAL;
    }
    TRY(check_transaction(transaction));
    fixed = sizeof(struct vsr_io_wire_record) +
            transaction->count * sizeof(struct vsr_io_wire_change);
    if (capacity < fixed) {
        return VSR_ELIMIT;
    }
    copy_init(&copy, out + fixed, capacity - fixed);
    walker.sink = &copy.sink;
    walker.limits = NULL;
    walker.payload = 0;
    walker.aggregate = UINT64_MAX;
    for (uint32_t i = 0; i < transaction->count; ++i) {
        size_t before = copy.used;
        size_t length;
        int rc = walk_change(&walker, &transaction->changes[i]);

        if (rc == VSR_AGAIN) {
            return VSR_ELIMIT;
        }
        TRY(rc);
        /* False positives: walk_change advanced copy.used through the sink
         * (walker.sink points into copy), which cppcheck does not follow. */
        /* cppcheck-suppress duplicateExpression */
        length = copy.used - before;
        /* cppcheck-suppress unsignedLessThanZero */
        if (fixed + before > UINT32_MAX || length > UINT32_MAX) {
            return VSR_ELIMIT;
        }
        offsets[i] = (uint32_t)(fixed + before);
        lengths[i] = (uint32_t)length;
    }
    total = fixed + copy.used;
    padded = total + padding_of(total);
    if (padded > capacity || padded > UINT32_MAX) {
        return VSR_ELIMIT;
    }
    memset(out + total, 0, padded - total);
    put.at = out + sizeof(struct vsr_io_wire_record);
    for (uint32_t i = 0; i < transaction->count; ++i) {
        const struct vsr_change *change = &transaction->changes[i];

        put_u32(&put, change->type);
        put_u32(&put, change->count);
        put_u64(&put, change->first);
        put_u32(&put, offsets[i]);
        put_u32(&put, lengths[i]);
    }
    put.at = out;
    put_u32(&put, VSR_IO_RECORD_MAGIC);
    put_u32(&put, (uint32_t)padded);
    put_u64(&put, transaction->sequence);
    put_u64(&put, generation);
    put_u64(&put, flushed);
    put_u32(&put, transaction->count);
    put_u32(&put, run);
    put_u32(&put, vsr_io_crc32c(0, out + sizeof(struct vsr_io_wire_record),
                                padded - sizeof(struct vsr_io_wire_record)));
    put_u32(&put, vsr_io_crc32c(
                      0, out, offsetof(struct vsr_io_wire_record, header_crc)));
    *written = padded;
    return VSR_OK;
}

void vsr_io_codec_put_pad(unsigned char *out, uint32_t length)
{
    struct put put = {out};

    put_u32(&put, VSR_IO_PAD_MAGIC);
    put_u32(&put, length);
}

/*
 * The scan protocol: get_record leaves the cursor after the header it
 * accepted (the 48-byte record header, or the 8-byte PAD header, whose
 * length then says how far to skip; nothing on END), check_record takes
 * the cursor there and checksums the rest of the record, and get_change
 * reads the descriptors that follow. A header with no descriptors is not
 * a transaction (vsr.h: 1..VSR_MAX_STORE_CHANGES changes) and is EINVAL.
 */
int vsr_io_codec_get_record(struct vsr_io_cursor *cursor, uint64_t limit,
                            struct vsr_io_wire_record *out, uint32_t *kind)
{
    struct vsr_io_cursor copy = *cursor;
    struct vsr_io_wire_record record;
    uint32_t crc = 0;

    *kind = VSR_IO_SCAN_END;
    if (!vsr_io_cursor_u32(&copy, &record.magic)) {
        return VSR_OK;
    }
    if (record.magic == VSR_IO_PAD_MAGIC) {
        if (!vsr_io_cursor_u32(&copy, &record.length)) {
            return VSR_OK;
        }
        if (record.length < sizeof(struct vsr_io_wire_pad) ||
            record.length % VSR_IO_WIRE_ALIGN != 0) {
            return VSR_EINVAL;
        }
        memset(out, 0, sizeof(*out));
        out->magic = record.magic;
        out->length = record.length;
        *kind = VSR_IO_SCAN_PAD;
        *cursor = copy;
        return VSR_OK;
    }
    if (record.magic != VSR_IO_RECORD_MAGIC ||
        vsr_io_cursor_remaining(cursor) < sizeof(record)) {
        return VSR_OK;
    }
    if (!vsr_io_cursor_crc(
            cursor, offsetof(struct vsr_io_wire_record, header_crc), &crc) ||
        !vsr_io_cursor_u32(&copy, &record.length) ||
        !vsr_io_cursor_u64(&copy, &record.sequence) ||
        !vsr_io_cursor_u64(&copy, &record.generation) ||
        !vsr_io_cursor_u64(&copy, &record.flushed) ||
        !vsr_io_cursor_u32(&copy, &record.count) ||
        !vsr_io_cursor_u32(&copy, &record.run) ||
        !vsr_io_cursor_u32(&copy, &record.payload_crc) ||
        !vsr_io_cursor_u32(&copy, &record.header_crc)) {
        return VSR_OK;
    }
    if (record.header_crc != crc || record.count == 0 ||
        record.count > VSR_MAX_STORE_CHANGES ||
        record.length % VSR_IO_WIRE_ALIGN != 0 ||
        record.length <
            sizeof(record) + record.count * sizeof(struct vsr_io_wire_change) ||
        record.length > limit) {
        return VSR_EINVAL;
    }
    *out = record;
    *kind = VSR_IO_SCAN_RECORD;
    *cursor = copy;
    return VSR_OK;
}

bool vsr_io_codec_check_record(const struct vsr_io_cursor *cursor,
                               const struct vsr_io_wire_record *header)
{
    uint32_t crc = 0;

    if (header->length < sizeof(*header) ||
        !vsr_io_cursor_crc(cursor, header->length - sizeof(*header), &crc)) {
        return false;
    }
    return crc == header->payload_crc;
}

int vsr_io_codec_get_change(struct vsr_io_cursor *cursor,
                            struct vsr_io_wire_change *out)
{
    struct vsr_io_cursor copy = *cursor;
    struct vsr_io_wire_change change;

    if (!vsr_io_cursor_u32(&copy, &change.type) ||
        !vsr_io_cursor_u32(&copy, &change.count) ||
        !vsr_io_cursor_u64(&copy, &change.first) ||
        !vsr_io_cursor_u32(&copy, &change.offset) ||
        !vsr_io_cursor_u32(&copy, &change.length)) {
        return VSR_EINVAL;
    }
    /* A payload is present exactly when the descriptor has bytes: the
     * changes with data encode to at least one header. */
    if (!change_shape(change.type, change.count, change.length != 0) ||
        change.offset < sizeof(struct vsr_io_wire_record) ||
        change.offset % VSR_IO_WIRE_ALIGN != 0 ||
        change.length % VSR_IO_WIRE_ALIGN != 0 ||
        change.length > UINT32_MAX - change.offset) {
        return VSR_EINVAL;
    }
    *out = change;
    *cursor = copy;
    return VSR_OK;
}

int vsr_io_codec_get_entries(struct vsr_io_cursor *cursor, uint32_t count,
                             const struct vsr_limits *limits,
                             struct vsr_io_bump *region,
                             struct vsr_entry **entries)
{
    struct decoder decoder;

    if (cursor == NULL || limits == NULL || region == NULL || entries == NULL) {
        return VSR_EINVAL;
    }
    decoder_init(&decoder, cursor, limits, region, limits->message_bytes);
    return decode_entry_array(&decoder, count, entries);
}

int vsr_io_codec_get_entry_at(struct vsr_io_cursor *cursor, uint32_t skip,
                              const struct vsr_limits *limits,
                              struct vsr_io_bump *region,
                              struct vsr_entry *entry)
{
    struct decoder decoder;

    if (cursor == NULL || limits == NULL || region == NULL || entry == NULL) {
        return VSR_EINVAL;
    }
    decoder_init(&decoder, cursor, limits, region, limits->message_bytes);
    for (uint32_t i = 0; i < skip; ++i) {
        uint32_t body_length;

        if (!vsr_io_cursor_skip(
                cursor, offsetof(struct vsr_io_wire_entry, body_length))) {
            return VSR_EINVAL;
        }
        TRY(read_u32(&decoder, &body_length));
        if (body_length % VSR_IO_WIRE_ALIGN != 0 ||
            !vsr_io_cursor_skip(cursor, body_length)) {
            return VSR_EINVAL;
        }
    }
    return decode_entry(&decoder, entry);
}

static int decode_client_record(struct decoder *decoder,
                                struct vsr_client_record *record)
{
    uint32_t length;

    TRY(read_u64(decoder, &record->request.client.hi));
    TRY(read_u64(decoder, &record->request.client.lo));
    TRY(read_u64(decoder, &record->request.number));
    TRY(read_u64(decoder, &record->op));
    TRY(read_i32(decoder, &record->result.code));
    TRY(read_u32(decoder, &length));
    record->result.reserved = 0;
    return decode_bytes(decoder, length, decoder->limits->result_bytes,
                        &record->result.data);
}

int vsr_io_codec_get_client_record(struct vsr_io_cursor *cursor,
                                   const struct vsr_limits *limits,
                                   struct vsr_io_bump *region,
                                   struct vsr_client_record *record)
{
    struct decoder decoder;

    if (cursor == NULL || limits == NULL || region == NULL || record == NULL) {
        return VSR_EINVAL;
    }
    decoder_init(&decoder, cursor, limits, region, UINT64_MAX);
    return decode_client_record(&decoder, record);
}

int vsr_io_codec_skip_client_record(struct vsr_io_cursor *cursor,
                                    struct vsr_io_wire_client_record *out)
{
    struct vsr_io_cursor copy = *cursor;
    struct vsr_io_wire_client_record record;
    uint64_t padded;

    if (!vsr_io_cursor_u64(&copy, &record.client_hi) ||
        !vsr_io_cursor_u64(&copy, &record.client_lo) ||
        !vsr_io_cursor_u64(&copy, &record.number) ||
        !vsr_io_cursor_u64(&copy, &record.op) ||
        !vsr_io_cursor_i32(&copy, &record.code) ||
        !vsr_io_cursor_u32(&copy, &record.length) ||
        !pad64(record.length, &padded) ||
        !vsr_io_cursor_skip(&copy, (size_t)padded)) {
        return VSR_EINVAL;
    }
    *out = record;
    *cursor = copy;
    return VSR_OK;
}

int vsr_io_codec_get_hard_state(struct vsr_io_cursor *cursor,
                                const struct vsr_limits *limits,
                                struct vsr_io_bump *region,
                                struct vsr_hard_state *hard)
{
    struct decoder decoder;
    struct vsr_epoch *epoch;

    if (cursor == NULL || limits == NULL || region == NULL || hard == NULL) {
        return VSR_EINVAL;
    }
    decoder_init(&decoder, cursor, limits, region, UINT64_MAX);
    TRY(read_u64(&decoder, &hard->view));
    TRY(read_u64(&decoder, &hard->last_normal_view));
    TRY(read_u64(&decoder, &hard->committed));
    TRY(read_u32(&decoder, &hard->state));
    TRY(read_u32(&decoder, &hard->role));
    TRY(decode_epoch(&decoder, &epoch));
    hard->epoch = epoch;
    return VSR_OK;
}

int vsr_io_codec_get_checkpoint(struct vsr_io_cursor *cursor,
                                const struct vsr_limits *limits,
                                struct vsr_io_bump *region,
                                struct vsr_checkpoint **checkpoint)
{
    struct decoder decoder;

    if (cursor == NULL || limits == NULL || region == NULL ||
        checkpoint == NULL) {
        return VSR_EINVAL;
    }
    decoder_init(&decoder, cursor, limits, region, UINT64_MAX);
    return decode_checkpoint(&decoder, checkpoint);
}

int vsr_io_codec_get_identity(struct vsr_io_cursor *cursor,
                              struct vsr_store_identity *identity)
{
    struct vsr_io_cursor copy = *cursor;
    struct vsr_store_identity in;
    uint32_t reserved;

    if (!vsr_io_cursor_u64(&copy, &in.cluster.hi) ||
        !vsr_io_cursor_u64(&copy, &in.cluster.lo) ||
        !vsr_io_cursor_u64(&copy, &in.replica) ||
        !vsr_io_cursor_u32(&copy, &in.durability) ||
        !vsr_io_cursor_u32(&copy, &reserved) || reserved != 0) {
        return VSR_EINVAL;
    }
    in.reserved = 0;
    *identity = in;
    *cursor = copy;
    return VSR_OK;
}

/* -------------------------------------------------------------------------
 * Superblocks and segment headers
 * ---------------------------------------------------------------------- */

/* True when every byte of [begin, end) is zero. */
static bool all_zero(const unsigned char *bytes, size_t size)
{
    for (size_t i = 0; i < size; ++i) {
        if (bytes[i] != 0) {
            return false;
        }
    }
    return true;
}

/* block_bytes is a block of the store, at least the superblock's 104
 * bytes (the store requires 512 or more). */
void vsr_io_codec_put_superblock(const struct vsr_io_wire_superblock *in,
                                 unsigned char *block, uint32_t block_bytes)
{
    struct put put = {block};

    memset(block, 0, block_bytes);
    put_u32(&put, VSR_IO_SUPERBLOCK_MAGIC);
    put_u32(&put, VSR_IO_STORE_FORMAT);
    put_u64(&put, in->generation);
    put_u64(&put, in->revision);
    put_u64(&put, in->cluster_hi);
    put_u64(&put, in->cluster_lo);
    put_u64(&put, in->replica);
    put_u32(&put, in->durability);
    put_u32(&put, in->block_bytes);
    put_u64(&put, in->segment_bytes);
    put_u32(&put, in->header_blocks);
    put_u32(&put, in->slots);
    put_u64(&put, in->start_segment);
    put_u32(&put, in->start_slot);
    put_u32(&put, in->run);
    put_u64(&put, in->durable_floor);
    put_u32(&put, vsr_io_crc32c(0, block,
                                offsetof(struct vsr_io_wire_superblock, crc)));
    put_u32(&put, 0);
}

int vsr_io_codec_get_superblock(const unsigned char *block,
                                uint32_t block_bytes,
                                struct vsr_io_wire_superblock *out)
{
    struct vsr_io_cursor cursor;
    struct vsr_io_wire_superblock in;

    if (block == NULL || block_bytes < sizeof(in)) {
        return VSR_EINVAL;
    }
    vsr_io_cursor_init_one(&cursor, block, block_bytes);
    if (!vsr_io_cursor_u32(&cursor, &in.magic) ||
        !vsr_io_cursor_u32(&cursor, &in.format) ||
        !vsr_io_cursor_u64(&cursor, &in.generation) ||
        !vsr_io_cursor_u64(&cursor, &in.revision) ||
        !vsr_io_cursor_u64(&cursor, &in.cluster_hi) ||
        !vsr_io_cursor_u64(&cursor, &in.cluster_lo) ||
        !vsr_io_cursor_u64(&cursor, &in.replica) ||
        !vsr_io_cursor_u32(&cursor, &in.durability) ||
        !vsr_io_cursor_u32(&cursor, &in.block_bytes) ||
        !vsr_io_cursor_u64(&cursor, &in.segment_bytes) ||
        !vsr_io_cursor_u32(&cursor, &in.header_blocks) ||
        !vsr_io_cursor_u32(&cursor, &in.slots) ||
        !vsr_io_cursor_u64(&cursor, &in.start_segment) ||
        !vsr_io_cursor_u32(&cursor, &in.start_slot) ||
        !vsr_io_cursor_u32(&cursor, &in.run) ||
        !vsr_io_cursor_u64(&cursor, &in.durable_floor) ||
        !vsr_io_cursor_u32(&cursor, &in.crc) ||
        !vsr_io_cursor_u32(&cursor, &in.reserved)) {
        return VSR_EINVAL;
    }
    if (in.magic != VSR_IO_SUPERBLOCK_MAGIC ||
        in.format != VSR_IO_STORE_FORMAT ||
        in.crc != vsr_io_crc32c(0, block,
                                offsetof(struct vsr_io_wire_superblock, crc)) ||
        in.reserved != 0 ||
        !all_zero(block + sizeof(in), block_bytes - sizeof(in))) {
        return VSR_EINVAL;
    }
    *out = in;
    return VSR_OK;
}

/* Walks the state part of a segment header: identity, hard state with its
 * epoch, the anchor when present. */
static int walk_segment_state(struct walker *walker,
                              const struct vsr_io_segment_state *state)
{
    if (state->identity == NULL) {
        return VSR_OK;
    }
    TRY(walk_identity(walker, state->identity));
    TRY(walk_hard_state(walker, state->hard));
    if (state->checkpoint != NULL) {
        TRY(walk_checkpoint(walker, state->checkpoint));
    }
    return VSR_OK;
}

/*
 * `fixed` supplies what the store stamps (generation, segment, run); the
 * bounds, the client base, the last sequence and the durable floor come
 * from `state`, and magic, format, length, flags and the CRC are derived.
 * *written is the meaningful length, which the fixed part carries; the
 * rest of header_bytes is zeroed.
 */
int vsr_io_codec_put_segment(const struct vsr_io_wire_segment *fixed,
                             const struct vsr_io_segment_state *state,
                             unsigned char *out, size_t header_bytes,
                             size_t *written)
{
    struct measure measure;
    struct copy copy;
    struct walker walker;
    struct put put = {out};
    uint32_t flags = 0;
    size_t length;
    int rc;

    if (fixed == NULL || state == NULL || out == NULL || written == NULL ||
        (state->identity == NULL) != (state->hard == NULL) ||
        (state->identity == NULL && state->checkpoint != NULL)) {
        return VSR_EINVAL;
    }
    if (state->identity != NULL) {
        flags |= VSR_IO_SEGMENT_STATE;
        if (state->checkpoint != NULL) {
            flags |= VSR_IO_SEGMENT_ANCHOR;
        }
    }
    /* Measure first: the fixed part carries the length. */
    measure_init(&measure, false);
    walker.sink = &measure.sink;
    walker.limits = NULL;
    walker.payload = 0;
    walker.aggregate = UINT64_MAX;
    rc = walk_segment_state(&walker, state);
    if (rc == VSR_AGAIN) {
        return VSR_ELIMIT;
    }
    TRY(rc);
    if (measure.length > header_bytes ||
        header_bytes - measure.length <
            sizeof(struct vsr_io_wire_segment) + sizeof(uint32_t)) {
        return VSR_ELIMIT;
    }
    length = sizeof(struct vsr_io_wire_segment) + (size_t)measure.length +
             sizeof(uint32_t);
    if (length > UINT32_MAX) {
        return VSR_ELIMIT;
    }
    put_u32(&put, VSR_IO_SEGMENT_MAGIC);
    put_u32(&put, VSR_IO_STORE_FORMAT);
    put_u64(&put, fixed->generation);
    put_u64(&put, fixed->segment);
    put_u32(&put, (uint32_t)length);
    put_u32(&put, flags);
    put_u32(&put, fixed->run);
    put_u32(&put, 0);
    put_u64(&put, state->last_sequence);
    put_u64(&put, state->durable_floor);
    put_u64(&put, state->client_base);
    put_u64(&put, state->log_begin);
    put_u64(&put, state->log_end);
    copy_init(&copy, put.at, (size_t)measure.length);
    walker.sink = &copy.sink;
    walker.payload = 0;
    rc = walk_segment_state(&walker, state);
    if (rc == VSR_AGAIN) {
        return VSR_ELIMIT;
    }
    TRY(rc);
    put.at += copy.used;
    put_u32(&put, vsr_io_crc32c(0, out, length - sizeof(uint32_t)));
    memset(out + length, 0, header_bytes - length);
    *written = length;
    return VSR_OK;
}

int vsr_io_codec_get_segment(const unsigned char *header, size_t header_bytes,
                             const struct vsr_limits *limits,
                             struct vsr_io_bump *region,
                             struct vsr_io_wire_segment *fixed,
                             struct vsr_store_identity *identity,
                             struct vsr_hard_state *hard,
                             struct vsr_checkpoint **checkpoint)
{
    struct vsr_io_cursor cursor;
    struct vsr_io_wire_segment in;
    struct vsr_checkpoint *anchor = NULL;

    if (header == NULL || limits == NULL || region == NULL || fixed == NULL ||
        identity == NULL || hard == NULL || checkpoint == NULL) {
        return VSR_EINVAL;
    }
    vsr_io_cursor_init_one(&cursor, header, header_bytes);
    if (!vsr_io_cursor_u32(&cursor, &in.magic) ||
        !vsr_io_cursor_u32(&cursor, &in.format) ||
        !vsr_io_cursor_u64(&cursor, &in.generation) ||
        !vsr_io_cursor_u64(&cursor, &in.segment) ||
        !vsr_io_cursor_u32(&cursor, &in.length) ||
        !vsr_io_cursor_u32(&cursor, &in.flags) ||
        !vsr_io_cursor_u32(&cursor, &in.run) ||
        !vsr_io_cursor_u32(&cursor, &in.reserved) ||
        !vsr_io_cursor_u64(&cursor, &in.last_sequence) ||
        !vsr_io_cursor_u64(&cursor, &in.durable_floor) ||
        !vsr_io_cursor_u64(&cursor, &in.client_base) ||
        !vsr_io_cursor_u64(&cursor, &in.log_begin) ||
        !vsr_io_cursor_u64(&cursor, &in.log_end)) {
        return VSR_EINVAL;
    }
    if (in.magic != VSR_IO_SEGMENT_MAGIC || in.format != VSR_IO_STORE_FORMAT ||
        (in.flags & ~(VSR_IO_SEGMENT_STATE | VSR_IO_SEGMENT_ANCHOR)) != 0 ||
        ((in.flags & VSR_IO_SEGMENT_ANCHOR) != 0 &&
         (in.flags & VSR_IO_SEGMENT_STATE) == 0) ||
        in.reserved != 0 || in.length > header_bytes ||
        in.length < sizeof(in) + sizeof(uint32_t) ||
        vsr_io_get_u32(header + in.length - sizeof(uint32_t)) !=
            vsr_io_crc32c(0, header, in.length - sizeof(uint32_t)) ||
        !all_zero(header + in.length, header_bytes - in.length)) {
        return VSR_EINVAL;
    }
    /* The state lies between the fixed part and the CRC. */
    vsr_io_cursor_init_one(&cursor, header + sizeof(in),
                           in.length - sizeof(in) - sizeof(uint32_t));
    memset(identity, 0, sizeof(*identity));
    memset(hard, 0, sizeof(*hard));
    if ((in.flags & VSR_IO_SEGMENT_STATE) != 0) {
        TRY(vsr_io_codec_get_identity(&cursor, identity));
        TRY(vsr_io_codec_get_hard_state(&cursor, limits, region, hard));
        if ((in.flags & VSR_IO_SEGMENT_ANCHOR) != 0) {
            TRY(vsr_io_codec_get_checkpoint(&cursor, limits, region, &anchor));
        }
    }
    if (vsr_io_cursor_remaining(&cursor) != 0) {
        return VSR_EINVAL;
    }
    *fixed = in;
    *checkpoint = anchor;
    return VSR_OK;
}

/* -------------------------------------------------------------------------
 * Clients file
 * ---------------------------------------------------------------------- */

static void put_hex(char *out, uint64_t value)
{
    static const char digits[] = "0123456789abcdef";

    for (unsigned i = 0; i < 16; ++i) {
        out[i] = digits[(value >> (60 - 4 * i)) & 0xf];
    }
}

void vsr_io_codec_clients_name(struct vsr_id snapshot, char *name)
{
    memcpy(name, "clients-", 8);
    put_hex(name + 8, snapshot.hi);
    put_hex(name + 24, snapshot.lo);
    name[40] = '\0';
}

void vsr_io_codec_put_clients_header(
    const struct vsr_io_wire_clients_header *in, unsigned char *out)
{
    struct put put = {out};

    put_u32(&put, VSR_IO_CLIENTS_MAGIC);
    put_u32(&put, VSR_IO_CLIENTS_FORMAT);
    put_u64(&put, in->cluster_hi);
    put_u64(&put, in->cluster_lo);
    put_u64(&put, in->snapshot_hi);
    put_u64(&put, in->snapshot_lo);
    put_u64(&put, in->op);
    put_u64(&put, in->sequence);
    put_u32(&put, in->count);
    put_u32(&put,
            vsr_io_crc32c(0, out,
                          offsetof(struct vsr_io_wire_clients_header, crc)));
}

int vsr_io_codec_get_clients_header(struct vsr_io_cursor *cursor,
                                    struct vsr_io_wire_clients_header *out)
{
    struct vsr_io_cursor copy = *cursor;
    struct vsr_io_wire_clients_header in;
    uint32_t crc = 0;

    if (!vsr_io_cursor_crc(
            cursor, offsetof(struct vsr_io_wire_clients_header, crc), &crc) ||
        !vsr_io_cursor_u32(&copy, &in.magic) ||
        !vsr_io_cursor_u32(&copy, &in.format) ||
        !vsr_io_cursor_u64(&copy, &in.cluster_hi) ||
        !vsr_io_cursor_u64(&copy, &in.cluster_lo) ||
        !vsr_io_cursor_u64(&copy, &in.snapshot_hi) ||
        !vsr_io_cursor_u64(&copy, &in.snapshot_lo) ||
        !vsr_io_cursor_u64(&copy, &in.op) ||
        !vsr_io_cursor_u64(&copy, &in.sequence) ||
        !vsr_io_cursor_u32(&copy, &in.count) ||
        !vsr_io_cursor_u32(&copy, &in.crc)) {
        return VSR_EINVAL;
    }
    if (in.magic != VSR_IO_CLIENTS_MAGIC ||
        in.format != VSR_IO_CLIENTS_FORMAT || in.crc != crc) {
        return VSR_EINVAL;
    }
    *out = in;
    *cursor = copy;
    return VSR_OK;
}

size_t vsr_io_codec_clients_record_bytes(uint64_t result_bytes)
{
    return sizeof(struct vsr_io_wire_client_record) + (size_t)result_bytes +
           padding_of(result_bytes) + sizeof(uint32_t);
}

void vsr_io_codec_put_clients_record(const struct vsr_client_record *record,
                                     unsigned char *out)
{
    const struct vsr_blob *result = &record->result.data;
    struct put put = {out};
    size_t used;

    put_u64(&put, record->request.client.hi);
    put_u64(&put, record->request.client.lo);
    put_u64(&put, record->request.number);
    put_u64(&put, record->op);
    put_u32(&put, (uint32_t)record->result.code);
    put_u32(&put, (uint32_t)result->size);
    for (uint32_t i = 0; i < result->count; ++i) {
        memcpy(put.at, result->spans[i].data, result->spans[i].size);
        put.at += result->spans[i].size;
    }
    memset(put.at, 0, padding_of(result->size));
    put.at += padding_of(result->size);
    used = (size_t)(put.at - out);
    put_u32(&put, vsr_io_crc32c(0, out, used));
}

int vsr_io_codec_get_clients_record(struct vsr_io_cursor *cursor,
                                    const struct vsr_limits *limits,
                                    struct vsr_io_bump *region,
                                    struct vsr_client_record *record)
{
    struct vsr_io_cursor copy;
    struct vsr_io_wire_client_record header;
    uint32_t crc = 0;
    uint32_t stored;
    uint64_t padded;

    if (cursor == NULL || limits == NULL || region == NULL || record == NULL) {
        return VSR_EINVAL;
    }
    copy = *cursor;
    /* The CRC covers the header and the padded bytes: find their extent
     * from the header, checksum them from the start, then decode. */
    if (!vsr_io_cursor_skip(
            &copy, offsetof(struct vsr_io_wire_client_record, length)) ||
        !vsr_io_cursor_u32(&copy, &header.length) ||
        !pad64(header.length, &padded) ||
        !vsr_io_cursor_crc(cursor, sizeof(header) + (size_t)padded, &crc)) {
        return VSR_EINVAL;
    }
    TRY(vsr_io_codec_get_client_record(cursor, limits, region, record));
    if (!vsr_io_cursor_u32(cursor, &stored) || stored != crc) {
        return VSR_EINVAL;
    }
    return VSR_OK;
}

void vsr_io_codec_put_clients_trailer(uint32_t count, uint32_t before,
                                      unsigned char *out)
{
    struct put put = {out};

    put_u32(&put, VSR_IO_CLIENTS_MAGIC);
    put_u32(&put, count);
    put_u32(&put,
            vsr_io_crc32c(before, out,
                          offsetof(struct vsr_io_wire_clients_trailer, crc)));
    put_u32(&put, 0);
}

int vsr_io_codec_get_clients_trailer(struct vsr_io_cursor *cursor,
                                     uint32_t before, uint32_t *count)
{
    struct vsr_io_cursor copy = *cursor;
    uint32_t crc = before;
    uint32_t magic;
    uint32_t in;
    uint32_t stored;
    uint32_t reserved;

    if (!vsr_io_cursor_crc(
            cursor, offsetof(struct vsr_io_wire_clients_trailer, crc), &crc) ||
        !vsr_io_cursor_u32(&copy, &magic) || !vsr_io_cursor_u32(&copy, &in) ||
        !vsr_io_cursor_u32(&copy, &stored) ||
        !vsr_io_cursor_u32(&copy, &reserved) || magic != VSR_IO_CLIENTS_MAGIC ||
        stored != crc || reserved != 0) {
        return VSR_EINVAL;
    }
    *count = in;
    *cursor = copy;
    return VSR_OK;
}
