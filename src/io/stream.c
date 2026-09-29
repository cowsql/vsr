#include "config.h"

#include "io/stream.h"

#include "checked.h"
#include "io/pool.h"

#include <stdalign.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

/*
 * Table layout of struct vsr_io_streams, in one region: the stream table
 * and the window units (stream_window per stream, one ring each).
 */

#define STREAM_NONE VSR_IO_INDEX_NONE

/* Invariant checks in debug builds; a violation traps (see pool.c). */
#ifdef NDEBUG
#define STREAMS_ASSERT(condition) ((void)sizeof(condition))
#else
#define STREAMS_ASSERT(condition) ((condition) ? (void)0 : __builtin_trap())
#endif

struct streams_plan {
    size_t streams;
    size_t units;
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

static bool streams_plan(const struct vsr_io_limits *limits,
                         struct streams_plan *plan)
{
    size_t offset = 0;
    size_t bytes;
    size_t count;

    memset(plan, 0, sizeof(*plan));
    plan->alignment =
        alignof(struct vsr_io_stream) > alignof(struct vsr_io_stream_unit)
            ? alignof(struct vsr_io_stream)
            : alignof(struct vsr_io_stream_unit);
    if (!vsr_size_mul(limits->streams, sizeof(struct vsr_io_stream), &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_stream), &plan->streams) ||
        !vsr_size_mul(limits->streams, limits->stream_window, &count) ||
        !vsr_size_mul(count, sizeof(struct vsr_io_stream_unit), &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_stream_unit),
               &plan->units)) {
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

static void stream_reset(struct vsr_io_stream *stream,
                         struct vsr_io_stream_unit *units, uint32_t window)
{
    memset(stream, 0, sizeof(*stream));
    stream->state = VSR_IO_STREAM_FREE;
    stream->owner = VSR_IO_STREAM_CALLER;
    stream->link = STREAM_NONE;
    stream->request_slab = STREAM_NONE;
    stream->deadline = STREAM_NONE; /* Bound by the engine after init. */
    stream->file_slot = STREAM_NONE;
    stream->units = units;
    for (uint32_t i = 0; i < window; ++i) {
        unit_reset(&units[i]);
    }
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
    streams->count = limits->streams;
    streams->window = limits->stream_window;
    streams->chunk_bytes = chunk_bytes;
    streams->active = 0;
    for (uint32_t i = 0; i < streams->count; ++i) {
        stream_reset(&streams->streams[i],
                     streams->units + (size_t)i * streams->window,
                     streams->window);
    }
}

/*
 * Link-facing entry points, stubs until the stream module lands: the link
 * module tells a STREAM-purpose link's stream about its life cycle and
 * hands it inbound stream frames. A frame reaching a stub is dropped; a
 * link event is ignored.
 */

void vsr_io_streams_frame(struct vsr_io *io, uint32_t link, uint16_t kind,
                          const struct vsr_io_cursor *body, uint32_t slab)
{
    (void)io;
    (void)link;
    (void)kind;
    (void)body;
    (void)slab;
}

void vsr_io_streams_link_up(struct vsr_io *io, uint32_t stream)
{
    (void)io;
    (void)stream;
}

void vsr_io_streams_link_lost(struct vsr_io *io, uint32_t stream, int32_t error)
{
    (void)io;
    (void)stream;
    (void)error;
}

void vsr_io_streams_sent(struct vsr_io *io, uint32_t stream,
                         uint64_t notified_offset)
{
    (void)io;
    (void)stream;
    (void)notified_offset;
}
