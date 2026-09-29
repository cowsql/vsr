#include "config.h"

#include "io/stream.h"

#include "checked.h"
#include "io/codec.h"
#include "io/crc32c.h"
#include "io/deadline.h"
#include "io/engine.h"
#include "io/link.h"
#include "io/pool.h"
#include "io/slots.h"
#include "io/snapshot.h"
#include "io/wire.h"

#include <errno.h>
#include <stdalign.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

/*
 * Table layout of struct vsr_io_streams, in one region: the stream table,
 * the window units (stream_window per stream, one ring each) and the write
 * queues (stream_window per stream, one ring each).
 *
 * Both roles are driven by three routines: `stream_drive` makes every
 * possible step that needs no link close (chunking, sends, the request or
 * END frame, unit release, WRITTEN ops), called from every entry point;
 * `stream_finish`, from poll only, emits the END op, closes the link and
 * frees the stream once nothing of it is outstanding; `stream_lose` ends a
 * stream early (link loss, timeout, protocol error, shutdown). The link
 * module calls back synchronously (link_lost from vsr_io_links_close, sent
 * from a send completion), so a close is never issued from inside `sent`
 * and `link_gone` is set before the close that triggers `link_lost`.
 */

#define STREAM_NONE VSR_IO_INDEX_NONE
#define STREAM_INDEX_BITS 16u
#define STREAM_INDEX_MAX UINT32_C(0xFFFF)
#define STREAM_GENERATION_MAX UINT32_C(0x3FFFFFFF)
#define STREAM_CHUNK_HEADER 16u
#define STREAM_REQUEST_HEADER 8u
#define STREAM_END_HEADER 16u

/* Invariant checks in debug builds; a violation traps (see pool.c). */
#ifdef NDEBUG
#define STREAMS_ASSERT(condition) ((void)sizeof(condition))
#else
#define STREAMS_ASSERT(condition) ((condition) ? (void)0 : __builtin_trap())
#endif

struct streams_plan {
    size_t streams;
    size_t units;
    size_t writes;
    size_t total;
    size_t alignment;
};

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

static size_t max_size(size_t a, size_t b)
{
    return a > b ? a : b;
}

static bool streams_plan(const struct vsr_io_limits *limits,
                         struct streams_plan *plan)
{
    size_t offset = 0;
    size_t bytes;
    size_t count;

    memset(plan, 0, sizeof(*plan));
    plan->alignment =
        max_size(alignof(struct vsr_io_stream),
                 max_size(alignof(struct vsr_io_stream_unit),
                          alignof(struct vsr_io_stream_queued_write)));
    if (limits->streams > STREAM_INDEX_MAX ||
        limits->stream_window > STREAM_INDEX_MAX) {
        return false;
    }
    if (!vsr_size_mul(limits->streams, sizeof(struct vsr_io_stream), &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_stream), &plan->streams) ||
        !vsr_size_mul(limits->streams, limits->stream_window, &count) ||
        !vsr_size_mul(count, sizeof(struct vsr_io_stream_unit), &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_stream_unit),
               &plan->units) ||
        !vsr_size_mul(count, sizeof(struct vsr_io_stream_queued_write),
                      &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_stream_queued_write),
               &plan->writes)) {
        return false;
    }
    plan->total = offset;
    return true;
}

int vsr_io_streams_size(const struct vsr_io_limits *limits, size_t *bytes,
                        size_t *alignment)
{
    struct streams_plan plan;

    if (limits == NULL || bytes == NULL || alignment == NULL) {
        return VSR_EINVAL;
    }
    if (!streams_plan(limits, &plan)) {
        return VSR_ELIMIT;
    }
    *bytes = plan.total;
    *alignment = plan.alignment;
    return VSR_OK;
}

static void unit_reset(struct vsr_io_stream_unit *unit)
{
    memset(unit, 0, sizeof(*unit));
    unit->slab = STREAM_NONE;
    unit->slot = STREAM_NONE;
}

/* Clears a stream's transfer state, keeping what the table owns: the
 * deadline handle, the unit and write arrays, the generation. */
static void stream_clear(struct vsr_io_stream *stream, uint32_t window)
{
    uint32_t deadline = stream->deadline;
    uint32_t generation = stream->generation;
    struct vsr_io_stream_unit *units = stream->units;
    struct vsr_io_stream_queued_write *writes = stream->writes;

    memset(stream, 0, sizeof(*stream));
    stream->state = VSR_IO_STREAM_FREE;
    stream->owner = VSR_IO_STREAM_CALLER;
    stream->link = STREAM_NONE;
    stream->request_slab = STREAM_NONE;
    stream->deadline = deadline;
    stream->generation = generation;
    stream->units = units;
    stream->writes = writes;
    for (uint32_t i = 0; i < window; ++i) {
        unit_reset(&units[i]);
    }
    memset(writes, 0, (size_t)window * sizeof(*writes));
}

void vsr_io_streams_init(struct vsr_io_streams *streams, void *memory,
                         size_t size, const struct vsr_io_limits *limits,
                         uint32_t chunk_bytes)
{
    struct streams_plan plan;
    unsigned char *base = memory;
    bool sized = streams_plan(limits, &plan);

    STREAMS_ASSERT(sized && plan.total <= size);
    (void)sized;
    (void)size;
    memset(streams, 0, sizeof(*streams));
    memset(base, 0, plan.total);
    streams->streams = (struct vsr_io_stream *)(void *)(base + plan.streams);
    streams->units = (struct vsr_io_stream_unit *)(void *)(base + plan.units);
    streams->writes =
        (struct vsr_io_stream_queued_write *)(void *)(base + plan.writes);
    streams->count = limits->streams;
    streams->window = limits->stream_window;
    streams->chunk_bytes = chunk_bytes;
    streams->active = 0;
    streams->closing = 0;
    for (uint32_t i = 0; i < streams->count; ++i) {
        struct vsr_io_stream *stream = &streams->streams[i];

        stream->units = streams->units + (size_t)i * streams->window;
        stream->writes = streams->writes + (size_t)i * streams->window;
        stream->deadline = STREAM_NONE; /* Bound by the engine after init. */
        stream->generation = 0;
        stream_clear(stream, streams->window);
    }
}

/* -------------------------------------------------------------------------
 * Handles, op ids and lookups
 * ---------------------------------------------------------------------- */

static uint32_t stream_index(const struct vsr_io *io,
                             const struct vsr_io_stream *stream)
{
    return (uint32_t)(stream - io->streams.streams);
}

static uint64_t handle_of(const struct vsr_io_stream *stream, uint32_t index)
{
    return ((uint64_t)stream->generation << 32) | index;
}

uint64_t vsr_io_streams_handle(const struct vsr_io_streams *streams,
                               uint32_t index)
{
    if (index >= streams->count) {
        return 0;
    }
    return handle_of(&streams->streams[index], index);
}

static uint64_t op_id(uint64_t kind, const struct vsr_io_stream *stream,
                      uint32_t index, uint32_t unit)
{
    return kind | ((uint64_t)stream->generation << 32) |
           ((uint64_t)unit << STREAM_INDEX_BITS) | index;
}

/* The active stream a handle or op id names, or NULL. */
static struct vsr_io_stream *stream_resolve(struct vsr_io *io, uint64_t id)
{
    uint32_t index = (uint32_t)(id & STREAM_INDEX_MAX);
    uint32_t generation = (uint32_t)((id >> 32) & STREAM_GENERATION_MAX);
    struct vsr_io_stream *stream;

    if (index >= io->streams.count) {
        return NULL;
    }
    stream = &io->streams.streams[index];
    if (stream->state == VSR_IO_STREAM_FREE ||
        stream->generation != generation) {
        return NULL;
    }
    return stream;
}

static struct vsr_io_stream *stream_active(struct vsr_io *io, uint32_t index)
{
    if (index >= io->streams.count ||
        io->streams.streams[index].state == VSR_IO_STREAM_FREE) {
        return NULL;
    }
    return &io->streams.streams[index];
}

static void *unconst(const void *pointer)
{
    void *writable;

    memcpy(&writable, &pointer, sizeof(writable));
    return writable;
}

static bool forward_room(const struct vsr_io *io)
{
    return io->forwarded_count < io->options.limits.ops;
}

/* -------------------------------------------------------------------------
 * The inactivity clock
 * ---------------------------------------------------------------------- */

static void stream_touch(struct vsr_io *io, struct vsr_io_stream *stream)
{
    uint64_t timeout = io->options.handshake_timeout_ns;
    uint64_t when = VSR_NO_DEADLINE;

    if (stream->deadline == STREAM_NONE) {
        return;
    }
    if (timeout != 0 && timeout < VSR_NO_DEADLINE - io->now) {
        when = io->now + timeout;
    }
    vsr_io_deadlines_arm(&io->deadlines, stream->deadline, when);
}

static void stream_disarm(struct vsr_io *io, struct vsr_io_stream *stream)
{
    if (stream->deadline != STREAM_NONE) {
        vsr_io_deadlines_arm(&io->deadlines, stream->deadline, VSR_NO_DEADLINE);
    }
}

/* -------------------------------------------------------------------------
 * Units
 * ---------------------------------------------------------------------- */

static struct vsr_io_stream_unit *unit_at(struct vsr_io_stream *stream,
                                          uint32_t window, uint32_t n)
{
    return &stream->units[(stream->units_head + n) % window];
}

static struct vsr_io_stream_unit *
unit_alloc(struct vsr_io *io, struct vsr_io_stream *stream, uint32_t *index)
{
    uint32_t window = io->streams.window;
    struct vsr_io_stream_unit *unit;

    if (stream->units_used == window) {
        return NULL;
    }
    *index = (stream->units_head + stream->units_used) % window;
    unit = &stream->units[*index];
    STREAMS_ASSERT(unit->state == VSR_IO_UNIT_FREE);
    unit_reset(unit);
    stream->units_used++;
    return unit;
}

/* Advances the head over freed units. */
static void units_trim(struct vsr_io *io, struct vsr_io_stream *stream)
{
    uint32_t window = io->streams.window;

    while (stream->units_used > 0 &&
           stream->units[stream->units_head].state == VSR_IO_UNIT_FREE) {
        stream->units_head = (stream->units_head + 1) % window;
        stream->units_used--;
    }
}

/* Frees a unit: its slab reference and its write's count. The ring's head
 * is not moved here (a loop over the ring may be releasing several); the
 * caller trims once it is done. */
static void unit_release(struct vsr_io *io, struct vsr_io_stream *stream,
                         struct vsr_io_stream_unit *unit)
{
    STREAMS_ASSERT(unit->state != VSR_IO_UNIT_FREE &&
                   unit->slot == STREAM_NONE);
    if (unit->slab != STREAM_NONE) {
        vsr_io_pool_release(&io->pool, unit->slab);
    }
    if (stream->direction == VSR_IO_INBOUND) {
        STREAMS_ASSERT(stream->writes[unit->write].units > 0);
        stream->writes[unit->write].units--;
    }
    unit_reset(unit);
}

/* -------------------------------------------------------------------------
 * The link
 * ---------------------------------------------------------------------- */

/* The stream's link entry while it is still the stream's: NULL once it was
 * freed or taken by another link. */
static const struct vsr_io_link *stream_link(const struct vsr_io *io,
                                             const struct vsr_io_stream *stream)
{
    const struct vsr_io_link *link;

    if (stream->link == STREAM_NONE) {
        return NULL;
    }
    link = &io->links.links[stream->link];
    if (link->state == VSR_IO_LINK_FREE ||
        link->stream != stream_index(io, stream)) {
        return NULL;
    }
    return link;
}

/* The kernel reads nothing of the stream's any more: every queued frame
 * was notified, or the link entry is gone (freed only after its NOTIFs). */
static bool link_quiet(const struct vsr_io *io,
                       const struct vsr_io_stream *stream)
{
    const struct vsr_io_link *link = stream_link(io, stream);

    return link == NULL || link->notified_offset >= stream->send_end;
}

/* Bytes the kernel accepted so far; everything once the entry is gone. */
static uint64_t link_notified(const struct vsr_io *io,
                              const struct vsr_io_stream *stream)
{
    const struct vsr_io_link *link = stream_link(io, stream);

    return link == NULL ? UINT64_MAX : link->notified_offset;
}

static uint64_t link_sent(const struct vsr_io *io,
                          const struct vsr_io_stream *stream)
{
    const struct vsr_io_link *link = stream_link(io, stream);

    return link == NULL ? UINT64_MAX : link->sent_offset;
}

/* Closes the stream's link once; link_lost re-enters and returns at once
 * because link_gone is already set. */
static void drop_link(struct vsr_io *io, struct vsr_io_stream *stream,
                      int32_t error)
{
    if (stream->link_gone) {
        return;
    }
    stream->link_gone = 1;
    if (stream->link != STREAM_NONE) {
        vsr_io_links_close(io, stream->link, error);
    }
}

/* -------------------------------------------------------------------------
 * Stream release and the END op
 * ---------------------------------------------------------------------- */

static void stream_free(struct vsr_io *io, struct vsr_io_stream *stream)
{
    STREAMS_ASSERT(stream->state != VSR_IO_STREAM_FREE &&
                   stream->units_used == 0 && stream->writes_count == 0 &&
                   stream->request_slab == STREAM_NONE);
    stream_disarm(io, stream);
    stream_clear(stream, io->streams.window);
    io->streams.active--;
}

/* Emits the END op (or the library's end); false when the forwarded ring
 * is full and the emission must be retried. */
static bool stream_emit_end(struct vsr_io *io, struct vsr_io_stream *stream)
{
    uint32_t index = stream_index(io, stream);
    struct vsr_io_forwarded *entry;

    if (stream->owner == VSR_IO_STREAM_LIBRARY) {
        vsr_io_snapshots_stream_end(io, (uint32_t)stream->replica, index,
                                    stream->status);
        return true;
    }
    entry = vsr_io_forward(io, NULL, VSR_IO_OP_STREAM_END);
    if (entry == NULL) {
        return false;
    }
    entry->op.op.id = 0;
    entry->op.op.data = &entry->rail.end;
    entry->rail.end.stream = stream->direction == VSR_IO_OUTBOUND
                                 ? stream->cookie
                                 : handle_of(stream, index);
    entry->rail.end.bytes = stream->offset;
    entry->rail.end.status = stream->status;
    return true;
}

/* -------------------------------------------------------------------------
 * Early ends: loss, timeout, protocol errors, shutdown
 * ---------------------------------------------------------------------- */

/* Stops the source's chunking: units not yet queued to the link are
 * released (a read in flight completes first), the writes are marked
 * chunked so their WRITTEN follows, and `offset` falls back to the first
 * byte the link never took. */
static void source_abort(struct vsr_io *io, struct vsr_io_stream *stream)
{
    uint32_t window = io->streams.window;
    bool cut = false;

    stream->aborted = 1;
    for (uint32_t n = 0; n < stream->units_used; ++n) {
        struct vsr_io_stream_unit *unit = unit_at(stream, window, n);

        if (unit->state == VSR_IO_UNIT_FREE ||
            unit->state == VSR_IO_UNIT_SENT) {
            continue;
        }
        if (!cut) {
            stream->offset = unit->offset;
            cut = true;
        }
        if (unit->slot == STREAM_NONE) {
            unit_release(io, stream, unit);
        }
    }
    units_trim(io, stream);
    for (uint32_t n = 0; n < stream->writes_count; ++n) {
        struct vsr_io_stream_queued_write *write =
            &stream->writes[(stream->writes_head + n) % window];

        write->chunked = write->length;
    }
}

/* The link is gone (lost, or closed by the stream): decide the status and
 * stop what needs the link. A stream already ending keeps its status,
 * unless it is a source whose END frame never reached the kernel. */
static void stream_loss_effects(struct vsr_io *io, struct vsr_io_stream *stream,
                                int32_t status)
{
    stream_disarm(io, stream);
    if (stream->state == VSR_IO_STREAM_ENDING) {
        if (stream->direction == VSR_IO_INBOUND && stream->end_due &&
            !(stream->end_sent &&
              link_sent(io, stream) >= stream->end_offset)) {
            stream->status = status;
        }
    } else {
        stream->status = status;
        stream->state = VSR_IO_STREAM_ENDING;
    }
    stream->request_pending = 0;
    if (stream->direction == VSR_IO_INBOUND) {
        source_abort(io, stream);
    }
}

static void stream_lose(struct vsr_io *io, struct vsr_io_stream *stream,
                        int32_t error, int32_t status)
{
    drop_link(io, stream, error);
    stream_loss_effects(io, stream, status);
}

/* A source-side failure (a file read error, a file ending inside the
 * range): the requester is told with an END frame after the chunks the
 * link already took, then the link closes as after a CLOSE. */
static void source_fail(struct vsr_io *io, struct vsr_io_stream *stream,
                        int32_t status)
{
    if (stream->link_gone) {
        return; /* The loss decided the status already. */
    }
    stream->state = VSR_IO_STREAM_ENDING;
    stream->end_due = 1;
    stream->status = status;
    source_abort(io, stream);
    stream_touch(io, stream);
}

/* A frame the stream's state refuses: the link closes with -EPROTO. */
static void stream_protocol_error(struct vsr_io *io,
                                  struct vsr_io_stream *stream)
{
    io->stats.frames_rejected++;
    stream_lose(io, stream, -EPROTO, VSR_IO_FAILED);
}

/* -------------------------------------------------------------------------
 * Frames out
 * ---------------------------------------------------------------------- */

/* Queues the requester's request frame; EBUSY keeps it pending. */
static void requester_send_request(struct vsr_io *io,
                                   struct vsr_io_stream *stream)
{
    unsigned char header[STREAM_REQUEST_HEADER];
    struct vsr_io_vec payload;
    uint64_t end = 0;
    uint32_t crc;
    int r;

    vsr_io_codec_put_stream_request(header, (uint32_t)stream->request.size);
    payload.base = unconst(stream->request.data);
    payload.length = stream->request.size;
    crc = vsr_io_crc32c(0, header, sizeof(header));
    if (payload.length > 0) {
        crc = vsr_io_crc32c(crc, payload.base, payload.length);
    }
    r = vsr_io_links_send_frame(io, stream->link, VSR_IO_FRAME_STREAM_REQUEST,
                                header, sizeof(header), &payload,
                                payload.length > 0 ? 1 : 0, crc, &end);
    if (r == VSR_OK) {
        stream->request_pending = 0;
        stream->send_end = end;
        stream_touch(io, stream);
    } else if (r == VSR_EBUSY) {
        stream->request_pending = 1;
    } else {
        /* Too large for the link's frame limit: a permanent error. */
        stream_lose(io, stream, -EMSGSIZE, VSR_IO_FAILED);
    }
}

/* The vectors of a BUFFERS chunk, from the write's spans. */
static uint32_t chunk_vectors(const struct vsr_io_stream_queued_write *write,
                              const struct vsr_io_stream_unit *unit,
                              struct vsr_io_vec *vecs, uint32_t *crc)
{
    uint32_t count = 0;
    uint32_t span = unit->span;
    uint64_t span_offset = unit->span_offset;
    uint64_t remaining = unit->length;

    while (remaining > 0) {
        const struct vsr_span *piece = &write->spans[span];
        uint64_t available = piece->size - span_offset;
        uint64_t take = available < remaining ? available : remaining;

        STREAMS_ASSERT(span < write->count &&
                       count < VSR_IO_STREAM_CHUNK_VECTORS);
        if (take > 0) {
            vecs[count].base =
                (unsigned char *)unconst(piece->data) + span_offset;
            vecs[count].length = (size_t)take;
            *crc = vsr_io_crc32c(*crc, vecs[count].base, (size_t)take);
            count++;
            remaining -= take;
        }
        span++;
        span_offset = 0;
    }
    return count;
}

/* Queues one READY chunk; false when the link is busy. */
static bool source_send_unit(struct vsr_io *io, struct vsr_io_stream *stream,
                             struct vsr_io_stream_unit *unit)
{
    unsigned char header[STREAM_CHUNK_HEADER];
    struct vsr_io_vec vecs[VSR_IO_STREAM_CHUNK_VECTORS];
    uint32_t count;
    uint64_t end = 0;
    uint32_t crc;
    int r;

    vsr_io_codec_put_stream_chunk(header, unit->offset, unit->length);
    crc = vsr_io_crc32c(0, header, sizeof(header));
    if (unit->slab != STREAM_NONE) {
        vecs[0].base = vsr_io_pool_slab(&io->pool, unit->slab);
        vecs[0].length = unit->length;
        crc = vsr_io_crc32c(crc, vecs[0].base, unit->length);
        count = 1;
    } else {
        count = chunk_vectors(&stream->writes[unit->write], unit, vecs, &crc);
    }
    r = vsr_io_links_send_frame(io, stream->link, VSR_IO_FRAME_STREAM_CHUNK,
                                header, sizeof(header), vecs, count, crc, &end);
    if (r == VSR_EBUSY) {
        return false;
    }
    if (r != VSR_OK) {
        /* The link is not established any more; its loss follows. */
        return false;
    }
    unit->state = VSR_IO_UNIT_SENT;
    unit->send_end = end;
    stream->send_end = end;
    stream_touch(io, stream);
    return true;
}

static bool source_send_end_frame(struct vsr_io *io,
                                  struct vsr_io_stream *stream)
{
    unsigned char header[STREAM_END_HEADER];
    uint64_t end = 0;
    uint32_t crc;
    int r;

    vsr_io_codec_put_stream_end(header, stream->offset, stream->status);
    crc = vsr_io_crc32c(0, header, sizeof(header));
    r = vsr_io_links_send_frame(io, stream->link, VSR_IO_FRAME_STREAM_END,
                                header, sizeof(header), NULL, 0, crc, &end);
    if (r != VSR_OK) {
        return false;
    }
    stream->end_sent = 1;
    stream->end_offset = end;
    stream->send_end = end;
    stream_touch(io, stream);
    return true;
}

/* -------------------------------------------------------------------------
 * The source's drive: chunking, sends, END, unit release, WRITTEN
 * ---------------------------------------------------------------------- */

/* Frees the SENT units whose frames the kernel no longer reads. */
static void source_release_notified(struct vsr_io *io,
                                    struct vsr_io_stream *stream)
{
    uint32_t window = io->streams.window;
    uint64_t notified = link_notified(io, stream);

    for (uint32_t n = 0; n < stream->units_used; ++n) {
        struct vsr_io_stream_unit *unit = unit_at(stream, window, n);

        if (unit->state == VSR_IO_UNIT_SENT && unit->send_end <= notified) {
            unit_release(io, stream, unit);
        }
    }
    units_trim(io, stream);
}

/* Slices the next chunk of a BUFFERS write off its spans. */
static void chunk_buffers(struct vsr_io_stream_queued_write *write,
                          struct vsr_io_stream_unit *unit, uint32_t chunk_bytes)
{
    uint64_t length = 0;
    uint32_t vectors = 0;

    unit->span = write->span;
    unit->span_offset = write->span_offset;
    while (write->span < write->count && length < chunk_bytes &&
           vectors < VSR_IO_STREAM_CHUNK_VECTORS) {
        const struct vsr_span *piece = &write->spans[write->span];
        uint64_t available = piece->size - write->span_offset;
        uint64_t take = chunk_bytes - length;

        if (available == 0) {
            write->span++;
            write->span_offset = 0;
            continue;
        }
        if (take > available) {
            take = available;
        }
        length += take;
        vectors++;
        write->span_offset += take;
        if (write->span_offset == piece->size) {
            write->span++;
            write->span_offset = 0;
        }
    }
    STREAMS_ASSERT(length > 0 && length <= chunk_bytes);
    unit->length = (uint32_t)length;
}

/* Turns the queued writes into units, in order, while the window and the
 * pool allow. */
static void source_chunk(struct vsr_io *io, struct vsr_io_stream *stream)
{
    uint32_t window = io->streams.window;
    uint32_t chunk_bytes = io->streams.chunk_bytes;

    if (stream->aborted) {
        return;
    }
    for (uint32_t n = 0; n < stream->writes_count; ++n) {
        uint32_t at = (stream->writes_head + n) % window;
        struct vsr_io_stream_queued_write *write = &stream->writes[at];

        while (write->chunked < write->length) {
            struct vsr_io_stream_unit *unit;
            uint32_t index;
            uint32_t slab = STREAM_NONE;

            if (stream->units_used == window) {
                return;
            }
            if (write->kind == VSR_IO_WRITE_FILE) {
                slab = vsr_io_pool_acquire(&io->pool, false);
                if (slab == STREAM_NONE) {
                    return; /* Retried at the next poll. */
                }
            }
            unit = unit_alloc(io, stream, &index);
            STREAMS_ASSERT(unit != NULL);
            unit->write = at;
            unit->offset = stream->offset;
            if (write->kind == VSR_IO_WRITE_FILE) {
                uint64_t remaining = write->length - write->chunked;

                unit->state = VSR_IO_UNIT_READING;
                unit->slab = slab;
                unit->length =
                    remaining < chunk_bytes ? (uint32_t)remaining : chunk_bytes;
                unit->file_offset = write->offset + write->chunked;
            } else {
                unit->state = VSR_IO_UNIT_READY;
                chunk_buffers(write, unit, chunk_bytes);
            }
            stream->offset += unit->length;
            write->chunked += unit->length;
            write->units++;
        }
    }
}

/* Queues READY units in stream order; a unit still reading blocks the
 * later ones, a busy link stops the pass. */
static void source_send(struct vsr_io *io, struct vsr_io_stream *stream)
{
    uint32_t window = io->streams.window;

    if (stream->link_gone) {
        return;
    }
    for (uint32_t n = 0; n < stream->units_used; ++n) {
        struct vsr_io_stream_unit *unit = unit_at(stream, window, n);

        if (unit->state == VSR_IO_UNIT_FREE ||
            unit->state == VSR_IO_UNIT_SENT) {
            continue;
        }
        if (unit->state != VSR_IO_UNIT_READY ||
            !source_send_unit(io, stream, unit)) {
            return;
        }
    }
}

/* The END frame goes after the last chunk of the last write. */
static void source_send_end(struct vsr_io *io, struct vsr_io_stream *stream)
{
    uint32_t window = io->streams.window;

    if (!stream->end_due || stream->end_sent || stream->link_gone) {
        return;
    }
    for (uint32_t n = 0; n < stream->units_used; ++n) {
        uint32_t state = unit_at(stream, window, n)->state;

        if (state != VSR_IO_UNIT_FREE && state != VSR_IO_UNIT_SENT) {
            return;
        }
    }
    for (uint32_t n = 0; n < stream->writes_count; ++n) {
        const struct vsr_io_stream_queued_write *write =
            &stream->writes[(stream->writes_head + n) % window];

        if (write->chunked < write->length) {
            return;
        }
    }
    (void)source_send_end_frame(io, stream);
}

/* WRITTEN for every finished write at the head of the queue, in order. */
static void source_written(struct vsr_io *io, struct vsr_io_stream *stream)
{
    uint32_t window = io->streams.window;

    while (stream->writes_count > 0) {
        struct vsr_io_stream_queued_write *write =
            &stream->writes[stream->writes_head];

        if (write->chunked < write->length || write->units > 0) {
            return;
        }
        if (stream->owner == VSR_IO_STREAM_CALLER) {
            struct vsr_io_forwarded *entry =
                vsr_io_forward(io, NULL, VSR_IO_OP_STREAM_WRITTEN);

            if (entry == NULL) {
                return;
            }
            entry->op.op.id = 0;
            entry->op.op.data = &entry->rail.written;
            entry->rail.written.stream =
                handle_of(stream, stream_index(io, stream));
            entry->rail.written.write = write->write;
        }
        memset(write, 0, sizeof(*write));
        stream->writes_head = (stream->writes_head + 1) % window;
        stream->writes_count--;
    }
}

static void stream_drive(struct vsr_io *io, struct vsr_io_stream *stream)
{
    if (stream->direction == VSR_IO_OUTBOUND) {
        if (stream->state == VSR_IO_STREAM_REQUESTED &&
            stream->request_pending) {
            requester_send_request(io, stream);
        }
        return;
    }
    source_release_notified(io, stream);
    source_chunk(io, stream);
    source_send(io, stream);
    source_send_end(io, stream);
    source_written(io, stream);
}

/* Poll only: a stream with nothing outstanding emits its END op and is
 * released. The requester closed its link at the END frame; the source
 * lingers on its link until the requester's close reaches it (a source
 * closing first would cut the frames a slow requester still holds in its
 * link) or the inactivity timer closes it. */
static void stream_finish(struct vsr_io *io, struct vsr_io_stream *stream)
{
    if (stream->state != VSR_IO_STREAM_ENDING || stream->units_used != 0 ||
        stream->writes_count != 0 || stream->serve_op != 0) {
        return;
    }
    if (stream->direction == VSR_IO_INBOUND && !stream->end_sent &&
        !stream->link_gone) {
        return; /* The END frame is still to queue (EBUSY). */
    }
    if (!link_quiet(io, stream)) {
        return;
    }
    if (!stream->ended) {
        if ((stream->direction == VSR_IO_OUTBOUND || stream->accepted) &&
            !stream_emit_end(io, stream)) {
            return;
        }
        stream->ended = 1;
    }
    if (stream->direction == VSR_IO_INBOUND && !stream->link_gone) {
        return; /* Lingering for the requester's close. */
    }
    drop_link(io, stream, 0);
    stream_free(io, stream);
}

/* -------------------------------------------------------------------------
 * Caller events
 * ---------------------------------------------------------------------- */

static bool request_is_library(const struct vsr_span *request)
{
    uint64_t magic;

    if (request->size < sizeof(magic)) {
        return false;
    }
    memcpy(&magic, request->data, sizeof(magic));
    return magic == VSR_IO_LIBRARY_MAGIC;
}

static struct vsr_io_stream *stream_take(struct vsr_io *io, uint32_t index)
{
    struct vsr_io_stream *stream = &io->streams.streams[index];

    STREAMS_ASSERT(stream->state == VSR_IO_STREAM_FREE);
    stream->generation = stream->generation >= STREAM_GENERATION_MAX
                             ? 1
                             : stream->generation + 1;
    io->streams.active++;
    return stream;
}

static uint32_t stream_find_free(const struct vsr_io *io)
{
    for (uint32_t i = 0; i < io->streams.count; ++i) {
        if (io->streams.streams[i].state == VSR_IO_STREAM_FREE) {
            return i;
        }
    }
    return STREAM_NONE;
}

int vsr_io_streams_open(struct vsr_io *io, uint64_t cookie,
                        const struct vsr_io_stream_open *open, uint64_t lease,
                        uint32_t owner, uint32_t *index)
{
    struct vsr_io_stream *stream;
    uint32_t at;
    uint32_t link = STREAM_NONE;
    int r;

    if (index == NULL) {
        return VSR_EINVAL;
    }
    *index = STREAM_NONE;
    if (open == NULL || io->streams.closing ||
        (owner != VSR_IO_STREAM_CALLER && owner != VSR_IO_STREAM_LIBRARY) ||
        open->request.size > VSR_IO_STREAM_REQUEST_BYTES ||
        (open->request.size > 0 && open->request.data == NULL)) {
        return VSR_EINVAL;
    }
    if (owner == VSR_IO_STREAM_CALLER) {
        if (request_is_library(&open->request)) {
            return VSR_EINVAL;
        }
        for (uint32_t i = 0; i < io->streams.count; ++i) {
            const struct vsr_io_stream *other = &io->streams.streams[i];

            if (other->state != VSR_IO_STREAM_FREE &&
                other->owner == VSR_IO_STREAM_CALLER &&
                other->direction == VSR_IO_OUTBOUND &&
                other->cookie == cookie) {
                return VSR_EINVAL;
            }
        }
    }
    at = stream_find_free(io);
    if (at == STREAM_NONE) {
        return VSR_ELIMIT;
    }
    r = vsr_io_links_open_stream(io, open->node, at, &link);
    if (r != VSR_OK) {
        return r;
    }
    stream = stream_take(io, at);
    stream->state = VSR_IO_STREAM_DIALING;
    stream->owner = owner;
    stream->direction = VSR_IO_OUTBOUND;
    stream->link = link;
    stream->cookie = cookie;
    stream->node = open->node;
    stream->request = open->request;
    stream->request_lease = lease;
    stream->status = VSR_IO_RETRY;
    stream_touch(io, stream);
    *index = at;
    return VSR_OK;
}

int vsr_io_streams_write(struct vsr_io *io,
                         const struct vsr_io_stream_write *write,
                         uint64_t lease)
{
    struct vsr_io_stream *stream;
    struct vsr_io_stream_queued_write *queued;
    uint32_t window = io->streams.window;

    if (write == NULL) {
        return VSR_EINVAL;
    }
    stream = stream_resolve(io, write->stream);
    if (stream == NULL || stream->direction != VSR_IO_INBOUND ||
        stream->state != VSR_IO_STREAM_OPEN) {
        return VSR_EINVAL;
    }
    if (write->kind == VSR_IO_WRITE_BUFFERS) {
        uint64_t total = 0;

        if (write->buffers.count > 0 && write->buffers.spans == NULL) {
            return VSR_EINVAL;
        }
        for (uint32_t i = 0; i < write->buffers.count; ++i) {
            const struct vsr_span *span = &write->buffers.spans[i];

            if (span->size > 0 && span->data == NULL) {
                return VSR_EINVAL;
            }
            if (span->size > UINT64_MAX - total) {
                return VSR_EINVAL;
            }
            total += span->size;
        }
        if (total != write->buffers.size) {
            return VSR_EINVAL;
        }
    } else if (write->kind != VSR_IO_WRITE_FILE ||
               (stream->owner == VSR_IO_STREAM_CALLER &&
                write->slot >= io->options.file_slot_base &&
                write->slot - io->options.file_slot_base <
                    io->options.limits.file_slots)) {
        /* A caller's range reads a slot of the caller's: an engine slot
         * (a socket, the store's log) would stream the engine's own bytes
         * to the peer (decision B5). The library reads engine slots. */
        return VSR_EINVAL;
    }
    if (stream->writes_count == window) {
        return VSR_AGAIN;
    }
    queued =
        &stream->writes[(stream->writes_head + stream->writes_count) % window];
    memset(queued, 0, sizeof(*queued));
    queued->kind = write->kind;
    queued->write = write->write;
    queued->lease = lease;
    if (write->kind == VSR_IO_WRITE_FILE) {
        queued->slot = write->slot;
        queued->offset = write->offset;
        queued->length = write->length;
    } else {
        queued->spans = write->buffers.spans;
        queued->count = write->buffers.count;
        queued->length = write->buffers.size;
    }
    stream->writes_count++;
    stream_touch(io, stream);
    stream_drive(io, stream);
    return VSR_OK;
}

int vsr_io_streams_close(struct vsr_io *io, uint64_t handle, int32_t status)
{
    struct vsr_io_stream *stream = stream_resolve(io, handle);

    /* The status goes out in the END frame, whose decoder refuses anything
     * but an enum vsr_io_status (a protocol error at the requester). */
    if (stream == NULL || stream->direction != VSR_IO_INBOUND ||
        status < VSR_IO_OK || status > VSR_IO_CANCELLED) {
        return VSR_EINVAL;
    }
    if (stream->state == VSR_IO_STREAM_ENDING) {
        /* Ended under the caller (lost, cancelled, a file read failed),
         * which it cannot know before the END op: that op is on its way
         * and carries the engine's status. A refused stream or a second
         * close was the caller's own doing (decision B4). */
        if (!stream->accepted || stream->closed || stream->ended) {
            return VSR_EINVAL;
        }
        stream->closed = 1;
        return VSR_OK;
    }
    if (stream->state != VSR_IO_STREAM_OPEN) {
        return VSR_EINVAL;
    }
    stream->state = VSR_IO_STREAM_ENDING;
    stream->closed = 1;
    stream->end_due = 1;
    stream->status = status;
    stream_touch(io, stream);
    stream_drive(io, stream);
    return VSR_OK;
}

int vsr_io_streams_served(struct vsr_io *io, uint64_t op, int32_t status)
{
    struct vsr_io_stream *stream;

    if (vsr_io_streams_op_kind(op) != VSR_IO_STREAM_OP_SERVE) {
        return VSR_EINVAL;
    }
    stream = stream_resolve(io, op);
    if (stream == NULL || stream->serve_op != op) {
        return VSR_EINVAL;
    }
    stream->serve_op = 0;
    if (stream->request_slab != STREAM_NONE) {
        vsr_io_pool_release(&io->pool, stream->request_slab);
        stream->request_slab = STREAM_NONE;
    }
    stream->request.data = NULL;
    stream->request.size = 0;
    if (stream->state == VSR_IO_STREAM_SERVING) {
        if (status == VSR_IO_OK) {
            stream->state = VSR_IO_STREAM_OPEN;
            stream->accepted = 1;
        } else {
            stream->state = VSR_IO_STREAM_ENDING;
            stream->end_due = 1;
            stream->status = VSR_IO_RETRY;
        }
        stream_touch(io, stream);
    } else if (status == VSR_IO_OK) {
        /* Lost meanwhile: the caller learns it from the END op. */
        stream->accepted = 1;
    }
    stream_drive(io, stream);
    return VSR_OK;
}

int vsr_io_streams_data_done(struct vsr_io *io, uint64_t op)
{
    struct vsr_io_stream *stream;
    struct vsr_io_stream_unit *unit;
    uint32_t sequence =
        (uint32_t)((op >> STREAM_INDEX_BITS) & STREAM_INDEX_MAX);
    uint32_t position;

    if (vsr_io_streams_op_kind(op) != VSR_IO_STREAM_OP_DATA) {
        return VSR_EINVAL;
    }
    stream = stream_resolve(io, op);
    if (stream == NULL || stream->direction != VSR_IO_OUTBOUND) {
        return VSR_EINVAL;
    }
    /* Units sit behind the head in sequence order (allocation is
     * sequential, the head moves over freed ones only), so the chunk's
     * distance from the head unit's sequence is its ring position. */
    position =
        (sequence - (stream->chunks - stream->units_used)) & STREAM_INDEX_MAX;
    if (position >= stream->units_used) {
        return VSR_EINVAL;
    }
    unit = unit_at(stream, io->streams.window, position);
    if (unit->state != VSR_IO_UNIT_DATA ||
        (unit->sequence & STREAM_INDEX_MAX) != sequence) {
        return VSR_EINVAL;
    }
    unit_release(io, stream, unit);
    units_trim(io, stream);
    /* A caller call re-arms the clock (decision 97): the poll after this
     * completion drains deadlines before the link retries the frame the
     * window held. Once ENDING the requester waits untimed. */
    if (stream->state == VSR_IO_STREAM_REQUESTED) {
        stream_touch(io, stream);
    }
    return VSR_OK;
}

/* -------------------------------------------------------------------------
 * Frames in
 * ---------------------------------------------------------------------- */

/* A request on an unbound inbound link: the stream is created here. */
static bool source_request(struct vsr_io *io, uint32_t link_index,
                           struct vsr_io_link *link,
                           const struct vsr_io_cursor *body, uint32_t slab)
{
    struct vsr_io_cursor cursor = *body;
    struct vsr_span request;
    struct vsr_io_wire_library_request library;
    struct vsr_io_stream *stream;
    uint32_t at;
    bool is_library;

    if (vsr_io_codec_get_stream_request(&cursor, &request) != VSR_OK) {
        io->stats.frames_rejected++;
        vsr_io_links_close(io, link_index, -EPROTO);
        return true;
    }
    is_library = request_is_library(&request);
    if (is_library &&
        vsr_io_codec_get_library_request(&request, &library) != VSR_OK) {
        io->stats.frames_rejected++;
        vsr_io_links_close(io, link_index, -EPROTO);
        return true;
    }
    if (io->streams.closing) {
        vsr_io_links_close(io, link_index, -ECANCELED);
        return true;
    }
    at = stream_find_free(io);
    if (at == STREAM_NONE) {
        return false; /* Retried at poll, until a stream frees up. */
    }
    if (!is_library && !forward_room(io)) {
        io->forwarded_overflow = 1;
        return false;
    }
    stream = stream_take(io, at);
    stream->state = VSR_IO_STREAM_SERVING;
    stream->owner = is_library ? VSR_IO_STREAM_LIBRARY : VSR_IO_STREAM_CALLER;
    stream->direction = VSR_IO_INBOUND;
    stream->link = link_index;
    stream->cookie = handle_of(stream, at);
    stream->node = link->node;
    stream->status = VSR_IO_RETRY;
    link->stream = at;
    stream_touch(io, stream);
    if (is_library) {
        int r = vsr_io_snapshots_serve(io, at, &library);

        /* cppcheck-suppress knownConditionTrueFalse ; the weak stub */
        if (r == VSR_OK) {
            stream->state = VSR_IO_STREAM_OPEN;
            stream->accepted = 1;
        } else {
            stream->state = VSR_IO_STREAM_ENDING;
            stream->end_due = 1;
            stream->status = r;
        }
        stream_drive(io, stream);
        return true;
    }
    {
        struct vsr_io_forwarded *entry =
            vsr_io_forward(io, NULL, VSR_IO_OP_STREAM_SERVE);

        STREAMS_ASSERT(entry != NULL);
        vsr_io_pool_retain(&io->pool, slab);
        stream->request = request;
        stream->request_slab = slab;
        stream->serve_op = op_id(VSR_IO_STREAM_OP_SERVE, stream, at, 0);
        entry->op.op.id = stream->serve_op;
        entry->op.op.data = &entry->rail.serve;
        entry->rail.serve.stream = stream->cookie;
        entry->rail.serve.node = stream->node;
        entry->rail.serve.request = request;
    }
    return true;
}

static bool requester_chunk(struct vsr_io *io, struct vsr_io_stream *stream,
                            const struct vsr_io_cursor *body, uint32_t slab)
{
    struct vsr_io_cursor cursor = *body;
    struct vsr_span payload;
    struct vsr_io_stream_unit *unit;
    uint64_t offset;
    uint32_t index;
    uint32_t at = stream_index(io, stream);
    uint64_t op;

    if (vsr_io_codec_get_stream_chunk(&cursor, &offset, &payload) != VSR_OK ||
        offset != stream->offset) {
        stream_protocol_error(io, stream);
        return true;
    }
    if (payload.size == 0) {
        stream_touch(io, stream);
        return true;
    }
    if (stream->units_used == io->streams.window) {
        return false;
    }
    if (stream->owner == VSR_IO_STREAM_CALLER && !forward_room(io)) {
        io->forwarded_overflow = 1;
        return false;
    }
    unit = unit_alloc(io, stream, &index);
    STREAMS_ASSERT(unit != NULL);
    vsr_io_pool_retain(&io->pool, slab);
    unit->state = VSR_IO_UNIT_DATA;
    unit->slab = slab;
    unit->length = (uint32_t)payload.size;
    unit->offset = offset;
    unit->sequence = stream->chunks++;
    stream->offset += payload.size;
    stream_touch(io, stream);
    /* The id names the chunk, not its ring slot, so the id of a completed
     * op does not come back with the chunk that reuses the slot; the ring
     * holds fewer than 65536 units, so 16 bits tell every live one. */
    op = op_id(VSR_IO_STREAM_OP_DATA, stream, at,
               unit->sequence & STREAM_INDEX_MAX);
    if (stream->owner == VSR_IO_STREAM_LIBRARY) {
        vsr_io_snapshots_stream_data(io, (uint32_t)stream->replica, at, op,
                                     offset, &payload, slab);
    } else {
        struct vsr_io_forwarded *entry =
            vsr_io_forward(io, NULL, VSR_IO_OP_STREAM_DATA);

        STREAMS_ASSERT(entry != NULL);
        entry->op.op.id = op;
        entry->op.op.data = &entry->rail.data;
        entry->rail.data.stream = stream->cookie;
        entry->rail.data.offset = offset;
        entry->rail.data.bytes = payload;
    }
    return true;
}

static void requester_end(struct vsr_io *io, struct vsr_io_stream *stream,
                          const struct vsr_io_cursor *body)
{
    struct vsr_io_cursor cursor = *body;
    struct vsr_io_wire_stream_end end;

    if (vsr_io_codec_get_stream_end(&cursor, &end) != VSR_OK) {
        stream_protocol_error(io, stream);
        return;
    }
    stream->status = end.bytes == stream->offset ? end.status : VSR_IO_FAILED;
    stream->end_sent = 1;
    stream->state = VSR_IO_STREAM_ENDING;
    stream_disarm(io, stream);
    drop_link(io, stream, 0);
}

bool vsr_io_streams_frame(struct vsr_io *io, uint32_t link_index, uint16_t kind,
                          const struct vsr_io_cursor *body, uint32_t slab)
{
    struct vsr_io_link *link;
    struct vsr_io_stream *stream;

    if (link_index >= io->links.links_count || body == NULL) {
        return true;
    }
    link = &io->links.links[link_index];
    if (link->stream == STREAM_NONE) {
        if (link->direction != VSR_IO_INBOUND ||
            kind != VSR_IO_FRAME_STREAM_REQUEST || slab == STREAM_NONE) {
            io->stats.frames_rejected++;
            vsr_io_links_close(io, link_index, -EPROTO);
            return true;
        }
        return source_request(io, link_index, link, body, slab);
    }
    stream = stream_active(io, link->stream);
    if (stream == NULL || stream->link != link_index) {
        io->stats.frames_rejected++;
        vsr_io_links_close(io, link_index, -EPROTO);
        return true;
    }
    if (stream->direction != VSR_IO_OUTBOUND ||
        stream->state != VSR_IO_STREAM_REQUESTED || slab == STREAM_NONE) {
        stream_protocol_error(io, stream);
        return true;
    }
    switch (kind) {
    case VSR_IO_FRAME_STREAM_CHUNK:
        return requester_chunk(io, stream, body, slab);
    case VSR_IO_FRAME_STREAM_END:
        requester_end(io, stream, body);
        return true;
    default:
        stream_protocol_error(io, stream);
        return true;
    }
}

/* -------------------------------------------------------------------------
 * Link lifecycle and send progress
 * ---------------------------------------------------------------------- */

void vsr_io_streams_link_up(struct vsr_io *io, uint32_t index)
{
    struct vsr_io_stream *stream = stream_active(io, index);

    if (stream == NULL || stream->state != VSR_IO_STREAM_DIALING) {
        return;
    }
    stream->state = VSR_IO_STREAM_REQUESTED;
    stream->request_pending = 1;
    stream_touch(io, stream);
    stream_drive(io, stream);
}

void vsr_io_streams_link_lost(struct vsr_io *io, uint32_t index, int32_t error)
{
    struct vsr_io_stream *stream = stream_active(io, index);

    (void)error;
    if (stream == NULL || stream->link_gone) {
        return;
    }
    stream->link_gone = 1;
    /* vsr_io_close shuts the links down before the streams (section 7.7):
     * a link closed by that shutdown ends its stream CANCELLED, as the
     * stream shutdown would, not RETRY as for a loss (decision B3). */
    stream_loss_effects(io, stream,
                        io->links.closing ? VSR_IO_CANCELLED : VSR_IO_RETRY);
}

void vsr_io_streams_sent(struct vsr_io *io, uint32_t index,
                         uint64_t notified_offset)
{
    struct vsr_io_stream *stream = stream_active(io, index);

    (void)notified_offset;
    if (stream == NULL || stream->link_gone) {
        return;
    }
    stream_touch(io, stream);
    stream_drive(io, stream);
}

/* -------------------------------------------------------------------------
 * Deadlines, poll, prepare, complete, shutdown
 * ---------------------------------------------------------------------- */

void vsr_io_streams_deadline(struct vsr_io *io, uint32_t index, uint64_t now)
{
    struct vsr_io_stream *stream = stream_active(io, index);

    (void)now;
    if (stream == NULL || stream->link_gone) {
        return;
    }
    stream_lose(io, stream, -ETIMEDOUT, VSR_IO_RETRY);
}

void vsr_io_streams_poll(struct vsr_io *io, uint64_t now)
{
    (void)now;
    for (uint32_t i = 0; i < io->streams.count; ++i) {
        struct vsr_io_stream *stream = &io->streams.streams[i];

        if (stream->state == VSR_IO_STREAM_FREE) {
            continue;
        }
        stream_drive(io, stream);
        stream_finish(io, stream);
    }
}

void vsr_io_streams_prepare(struct vsr_io *io, struct vsr_io_sqe *sqes,
                            uint32_t capacity, uint32_t *count)
{
    uint32_t window = io->streams.window;

    for (uint32_t i = 0; i < io->streams.count; ++i) {
        struct vsr_io_stream *stream = &io->streams.streams[i];

        if (stream->state == VSR_IO_STREAM_FREE ||
            stream->direction != VSR_IO_INBOUND) {
            continue;
        }
        for (uint32_t n = 0; n < stream->units_used; ++n) {
            uint32_t at = (stream->units_head + n) % window;
            struct vsr_io_stream_unit *unit = &stream->units[at];
            const struct vsr_io_stream_queued_write *write =
                &stream->writes[unit->write];
            struct vsr_io_sqe *sqe;
            uint32_t slot;

            if (unit->state != VSR_IO_UNIT_READING ||
                unit->slot != STREAM_NONE) {
                continue;
            }
            if (*count == capacity) {
                return;
            }
            slot = vsr_io_slots_alloc(&io->slots, VSR_IO_SLOT_STREAM, 1, i, at,
                                      unit->file_offset);
            if (slot == STREAM_NONE) {
                return;
            }
            unit->slot = slot;
            sqe = &sqes[(*count)++];
            memset(sqe, 0, sizeof(*sqe));
            sqe->opcode = VSR_IO_SQE_READ;
            sqe->flags = VSR_IO_SQE_FIXED_FILE | VSR_IO_SQE_FIXED_BUFFER;
            sqe->fd = (int32_t)write->slot;
            sqe->user_data = vsr_io_slots_user_data(&io->slots, slot);
            sqe->buffer_index = (uint16_t)io->pool.region_index;
            sqe->addr = vsr_io_pool_slab(&io->pool, unit->slab) + unit->filled;
            sqe->length = unit->length - unit->filled;
            sqe->offset = unit->file_offset + unit->filled;
        }
    }
}

void vsr_io_streams_complete(struct vsr_io *io, uint32_t slot,
                             const struct vsr_io_cqe *cqe)
{
    const struct vsr_io_slot *entry = &io->slots.slots[slot];
    uint32_t index = entry->owner;
    uint32_t at = entry->sub;
    struct vsr_io_stream *stream;
    struct vsr_io_stream_unit *unit;

    STREAMS_ASSERT(entry->kind == VSR_IO_SLOT_STREAM);
    vsr_io_slots_consumed(&io->slots, slot, false);
    stream = stream_active(io, index);
    if (stream == NULL || at >= io->streams.window) {
        return;
    }
    unit = &stream->units[at];
    if (unit->state != VSR_IO_UNIT_READING || unit->slot != slot) {
        return;
    }
    unit->slot = STREAM_NONE;
    if (stream->aborted) {
        unit_release(io, stream, unit);
        units_trim(io, stream);
        stream_drive(io, stream);
        return;
    }
    if (cqe->result <= 0 ||
        (uint32_t)cqe->result > unit->length - unit->filled) {
        /* A read error, or the file ends inside the write's range. The
         * unit stays in the ring for the abort: the END's byte count stops
         * at the first chunk the link never took, which may be this one
         * (decision 97); the abort releases it (its slot is NONE). */
        source_fail(io, stream, VSR_IO_FAILED);
        STREAMS_ASSERT(unit->state == VSR_IO_UNIT_FREE);
        stream_drive(io, stream);
        return;
    }
    unit->filled += (uint32_t)cqe->result;
    if (unit->filled == unit->length) {
        unit->state = VSR_IO_UNIT_READY;
    }
    stream_touch(io, stream);
    stream_drive(io, stream);
}

void vsr_io_streams_shutdown(struct vsr_io *io)
{
    io->streams.closing = 1;
    for (uint32_t i = 0; i < io->streams.count; ++i) {
        struct vsr_io_stream *stream = &io->streams.streams[i];

        if (stream->state == VSR_IO_STREAM_FREE) {
            continue;
        }
        stream_lose(io, stream, -ECANCELED, VSR_IO_CANCELLED);
    }
}

/* -------------------------------------------------------------------------
 * Weak stubs of the snapshot module's hooks
 *
 * STUB: snapshot.c is a later module. These weak definitions let the tree
 * link and stand in for it: a library request is refused with RETRY, a
 * library chunk is completed at once, an end is ignored. The snapshot
 * module's strong definitions replace them; delete these then.
 * ---------------------------------------------------------------------- */

__attribute__((weak)) int
vsr_io_snapshots_serve(struct vsr_io *io, uint32_t stream,
                       const struct vsr_io_wire_library_request *request)
{
    (void)io;
    (void)stream;
    (void)request;
    return VSR_IO_RETRY;
}

__attribute__((weak)) void
vsr_io_snapshots_stream_data(struct vsr_io *io, uint32_t replica,
                             uint32_t stream, uint64_t op, uint64_t offset,
                             const struct vsr_span *bytes, uint32_t slab)
{
    (void)replica;
    (void)stream;
    (void)offset;
    (void)bytes;
    (void)slab;
    (void)vsr_io_streams_data_done(io, op);
}

__attribute__((weak)) void vsr_io_snapshots_stream_end(struct vsr_io *io,
                                                       uint32_t replica,
                                                       uint32_t stream,
                                                       int32_t status)
{
    (void)io;
    (void)replica;
    (void)stream;
    (void)status;
}
