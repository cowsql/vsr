#include "config.h"

#include "io/codec.h"
#include "io/engine.h"
#include "lib/check.h"
#include "vsr-io.h"

#include <errno.h>
#include <inttypes.h>
#include <stdalign.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>

#define PAGE 4096u
#define NONE UINT32_MAX
#define SLABS 32u
/* Record writes the engine reserves slots for per replica (engine.c). */
#define INFLIGHT_WRITES_MAX 8u
#define OWNER 0x5Au
#define FILE_SLOT_BASE 100u
#define REGION_BASE 5u
#define GROUP 3u

static _Alignas(4096) unsigned char metadata[1u << 20];
static _Alignas(4096) unsigned char payload[SLABS * PAGE];

/* -------------------------------------------------------------------------
 * Fake executor: records registrations, returns scripted results.
 * ---------------------------------------------------------------------- */

enum fake_kind {
    FAKE_UPDATE_BUFFER,
    FAKE_BUFFER_RING,
    FAKE_PROVIDE,
    FAKE_REGISTER_FILES,
    FAKE_UPDATE_FILE,
    FAKE_REGISTER_BUFFERS
};

struct fake_call {
    enum fake_kind kind;
    uint32_t index; /* Region index, group or slot. */
    uint32_t entries;
    uint32_t flags;
    const void *base; /* Region or ring memory; NULL when cleared. */
    size_t size;
};

struct fake {
    struct fake_call calls[32];
    uint32_t count;
    int fail_update_buffer; /* Result of the next update_buffer, 0 = OK. */
    int fail_buffer_ring;
    uint64_t now;
    uint32_t wakes;
    uint32_t randoms;
    uint32_t submits;
};

static struct fake_call *fake_record(struct fake *fake, enum fake_kind kind)
{
    struct fake_call *call;

    CHECK(fake->count < sizeof(fake->calls) / sizeof(fake->calls[0]));
    call = &fake->calls[fake->count++];
    memset(call, 0, sizeof(*call));
    call->kind = kind;
    return call;
}

static uint64_t fake_now(void *ctx)
{
    struct fake *fake = ctx;

    return fake->now;
}

static void fake_random(void *ctx, void *bytes, size_t size)
{
    struct fake *fake = ctx;
    unsigned char *out = bytes;

    fake->randoms++;
    for (size_t i = 0; i < size; ++i) {
        out[i] = (unsigned char)(i * 7u + fake->randoms);
    }
}

static int fake_submit_and_wait(void *ctx, const struct vsr_io_sqe *sqes,
                                uint32_t count, uint32_t want,
                                uint64_t min_wait_ns, uint64_t deadline_ns)
{
    struct fake *fake = ctx;

    (void)sqes;
    (void)count;
    (void)want;
    (void)min_wait_ns;
    (void)deadline_ns;
    fake->submits++;
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
    fake_record(ctx, FAKE_REGISTER_FILES)->entries = slots;
    return 0;
}

static int fake_update_file(void *ctx, uint32_t slot, int fd)
{
    struct fake_call *call = fake_record(ctx, FAKE_UPDATE_FILE);

    call->index = slot;
    call->entries = (uint32_t)fd;
    return 0;
}

static int fake_register_buffers(void *ctx, uint32_t regions)
{
    fake_record(ctx, FAKE_REGISTER_BUFFERS)->entries = regions;
    return 0;
}

static int fake_update_buffer(void *ctx, uint32_t index,
                              const struct vsr_io_region *region)
{
    struct fake *fake = ctx;
    struct fake_call *call = fake_record(fake, FAKE_UPDATE_BUFFER);
    int result = fake->fail_update_buffer;

    call->index = index;
    if (region != NULL) {
        call->base = region->base;
        call->size = region->size;
    }
    fake->fail_update_buffer = 0;
    return result;
}

static int fake_buffer_ring(void *ctx, uint16_t group, uint32_t entries,
                            uint32_t flags, const struct vsr_io_region *memory)
{
    struct fake *fake = ctx;
    struct fake_call *call = fake_record(fake, FAKE_BUFFER_RING);
    int result = fake->fail_buffer_ring;

    call->index = group;
    call->entries = entries;
    call->flags = flags;
    if (memory != NULL) {
        call->base = memory->base;
        call->size = memory->size;
    }
    fake->fail_buffer_ring = 0;
    return result;
}

static int fake_provide(void *ctx, uint16_t group,
                        const struct vsr_io_buffer *buffers, uint32_t count)
{
    struct fake_call *call = fake_record(ctx, FAKE_PROVIDE);

    (void)buffers;
    call->index = group;
    call->entries = count;
    return 0;
}

static void fake_wake(void *ctx)
{
    struct fake *fake = ctx;

    fake->wakes++;
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

static struct fake fake;

static void fake_reset(void)
{
    memset(&fake, 0, sizeof(fake));
    fake.now = 1000;
}

/* -------------------------------------------------------------------------
 * Options and the independently computed layout
 * ---------------------------------------------------------------------- */

static struct vsr_io_address listen_addresses[2];

static struct vsr_io_options base_options(void)
{
    struct vsr_io_options options;
    struct vsr_io_limits *limits = &options.limits;

    memset(&options, 0, sizeof(options));
    options.executor.ops = &fake_ops;
    options.executor.ctx = &fake;
    options.node = 7;
    options.listen = NULL;
    options.listen_count = 0;
    options.handshake = VSR_IO_HANDSHAKE_TRUSTED;
    limits->replicas = 2;
    limits->nodes = 4;
    limits->authorizations = 8;
    limits->links = 4;
    limits->link_queue = 4;
    limits->streams = 2;
    limits->stream_window = 2;
    limits->events = 8;
    limits->ops = 6;
    limits->batch = 16;
    limits->slabs = SLABS;
    limits->slab_bytes = PAGE;
    limits->caller_slabs = 2;
    /* listeners (2 at most) + links + streams + 2 * replicas = 12. */
    limits->file_slots = 12;
    limits->buffer_regions = 3;
    options.file_slot_base = FILE_SLOT_BASE;
    options.buffer_region_base = REGION_BASE;
    options.buffer_group = GROUP;
    options.owner = OWNER;
    options.nodelay = 1;
    options.connect_backoff_ns = 1000000;
    options.handshake_timeout_ns = 1000000000;
    options.idle_timeout_ns = 0;
    options.wait_min_ns = 0;
    options.wait_min_complete = 0;
    options.send_coalesce_bytes = 65536;
    options.zero_copy_bytes = 4096;
    options.stream_chunk_bytes = PAGE - 40;
    options.cache_line_bytes = 0;
    return options;
}

static void with_listeners(struct vsr_io_options *options)
{
    for (uint32_t i = 0; i < 2; ++i) {
        struct sockaddr_un *un =
            (struct sockaddr_un *)(void *)&listen_addresses[i].sockaddr;

        memset(&listen_addresses[i], 0, sizeof(listen_addresses[i]));
        un->sun_family = AF_UNIX;
        un->sun_path[0] = 0;
        un->sun_path[1] = (char)('a' + i);
        listen_addresses[i].length =
            (uint32_t)(offsetof(struct sockaddr_un, sun_path) + 2);
    }
    options->listen = listen_addresses;
    options->listen_count = 2;
}

static size_t align_up(size_t offset, size_t alignment)
{
    return (offset + alignment - 1) & ~(alignment - 1);
}

static size_t place(size_t *offset, size_t bytes, size_t alignment)
{
    size_t at = align_up(*offset, alignment);

    *offset = at + bytes;
    return at;
}

/* The metadata layout re-derived from the module size functions, in the
 * order engine.c documents: pool (ring memory first, page aligned), struct
 * vsr_io, listen copies, slots, deadlines, links, streams, forwarded ring,
 * replica table, file-slot free list. */
static size_t expected_metadata(const struct vsr_io_options *o)
{
    const struct vsr_io_limits *l = &o->limits;
    size_t offset = 0;
    size_t bytes = 0;
    size_t alignment = 0;
    uint32_t count = 0;
    uint32_t deadlines;

    CHECK(vsr_io_pool_size(l, PAGE, &bytes, &alignment) == VSR_OK);
    CHECK(alignment == PAGE);
    CHECK(place(&offset, bytes, PAGE) == 0);
    place(&offset, sizeof(struct vsr_io), alignof(struct vsr_io));
    place(&offset, (size_t)o->listen_count * sizeof(struct vsr_io_address),
          alignof(struct vsr_io_address));
    CHECK(vsr_io_slots_size(l, o->listen_count, INFLIGHT_WRITES_MAX, &bytes,
                            &count) == VSR_OK);
    place(&offset, bytes, alignof(struct vsr_io_slot));
    deadlines = l->links + l->nodes + 4 * l->replicas + l->streams;
    CHECK(vsr_io_deadlines_size(deadlines, &bytes) == VSR_OK);
    place(&offset, bytes, alignof(struct vsr_io_deadline_entry));
    CHECK(vsr_io_links_size(l, &bytes, &alignment) == VSR_OK);
    place(&offset, bytes, alignment);
    CHECK(vsr_io_streams_size(l, &bytes, &alignment) == VSR_OK);
    place(&offset, bytes, alignment);
    place(&offset, (size_t)l->ops * sizeof(struct vsr_io_forwarded),
          alignof(struct vsr_io_forwarded));
    place(&offset, (size_t)l->replicas * sizeof(struct vsr_io_replica),
          alignof(struct vsr_io_replica));
    place(&offset, (size_t)l->file_slots * sizeof(uint32_t), alignof(uint32_t));
    return offset;
}

static void check_layout(const struct vsr_io_options *o)
{
    struct vsr_io_layout layout;
    size_t bytes = 0;
    size_t alignment = 0;
    size_t expected = expected_metadata(o);

    memset(&layout, 0xEE, sizeof(layout));
    CHECK(vsr_io_layout(o, &layout) == VSR_OK);
    CHECK(layout.metadata.size == expected);
    CHECK(layout.metadata.alignment == PAGE);
    CHECK(layout.payload.size ==
          (size_t)o->limits.slabs * o->limits.slab_bytes);
    CHECK(layout.payload.alignment == PAGE);
    CHECK(vsr_io_engine_size(o, &bytes, &alignment) == VSR_OK);
    CHECK(bytes == expected && alignment == PAGE);
    CHECK(expected <= sizeof(metadata));
}

static int layout_of(const struct vsr_io_options *o)
{
    struct vsr_io_layout layout;
    int rc = vsr_io_layout(o, &layout);

    if (rc != VSR_OK) {
        CHECK(layout.metadata.size == 0 && layout.payload.size == 0);
    }
    return rc;
}

static void test_layout(void)
{
    struct vsr_io_options base = base_options();
    struct vsr_io_options o;
    struct vsr_io_layout layout;
    size_t small;
    size_t large;
    size_t bytes = 0;
    size_t alignment = 0;

    check_layout(&base);
    o = base;
    with_listeners(&o);
    check_layout(&o);
    CHECK(vsr_io_layout(&base, &layout) == VSR_OK);
    small = layout.metadata.size;
    CHECK(vsr_io_layout(&o, &layout) == VSR_OK);
    large = layout.metadata.size;
    CHECK(large >= small + 2 * sizeof(struct vsr_io_address));
    /* Every table grows the region. */
    o = base;
    o.limits.links = 8;
    o.limits.slabs = SLABS + 4;
    o.limits.file_slots = 16;
    check_layout(&o);
    o = base;
    o.limits.ops = 60;
    check_layout(&o);
    o = base;
    o.limits.replicas = 3;
    o.limits.buffer_regions = 4;
    o.limits.file_slots = 14;
    check_layout(&o);
    o = base;
    o.limits.caller_slabs = 0;
    check_layout(&o);

    /* NULL arguments. */
    CHECK(vsr_io_layout(NULL, &layout) == VSR_EINVAL);
    CHECK(layout.metadata.size == 0);
    CHECK(vsr_io_layout(&base, NULL) == VSR_EINVAL);
    CHECK(vsr_io_engine_size(NULL, &bytes, &alignment) == VSR_EINVAL);
    CHECK(vsr_io_engine_size(&base, NULL, &alignment) == VSR_EINVAL);
    CHECK(vsr_io_engine_size(&base, &bytes, NULL) == VSR_EINVAL);

    /* EINVAL: malformed options. */
    o = base;
    o.node = VSR_IO_NO_NODE;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o = base;
    o.reserved = 1;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o = base;
    o.listen_count = 1;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o = base;
    with_listeners(&o);
    listen_addresses[1].length = 0;
    CHECK(layout_of(&o) == VSR_EINVAL);
    with_listeners(&o);
    listen_addresses[0].length = sizeof(struct sockaddr_storage) + 1;
    CHECK(layout_of(&o) == VSR_EINVAL);
    with_listeners(&o);
    listen_addresses[0].reserved = 1;
    CHECK(layout_of(&o) == VSR_EINVAL);
    with_listeners(&o);
    o = base;
    o.handshake = VSR_IO_HANDSHAKE_KEYED;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o.handshake = 99;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o.handshake = VSR_IO_HANDSHAKE_EXTERNAL;
    CHECK(layout_of(&o) == VSR_OK);
    o = base;
    o.handshake_timeout_ns = 0;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o.handshake_timeout_ns = VSR_NO_DEADLINE;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o = base;
    o.connect_backoff_ns = VSR_NO_DEADLINE;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o = base;
    o.idle_timeout_ns = VSR_NO_DEADLINE;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o = base;
    o.wait_min_ns = VSR_NO_DEADLINE;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o = base;
    o.send_coalesce_bytes = 0;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o = base;
    o.stream_chunk_bytes = 0;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o = base;
    o.cache_line_bytes = 96;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o.cache_line_bytes = 128;
    CHECK(layout_of(&o) == VSR_OK);
    /* Every limit but caller_slabs is positive. */
    o = base;
    o.limits.replicas = 0;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o = base;
    o.limits.nodes = 0;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o = base;
    o.limits.authorizations = 0;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o = base;
    o.limits.links = 0;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o = base;
    o.limits.link_queue = 0;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o = base;
    o.limits.streams = 0;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o = base;
    o.limits.stream_window = 0;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o = base;
    o.limits.events = 0;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o = base;
    o.limits.ops = 0;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o = base;
    o.limits.batch = 0;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o = base;
    o.limits.slabs = 0;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o = base;
    o.limits.slab_bytes = 0;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o = base;
    o.limits.file_slots = 0;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o = base;
    o.limits.buffer_regions = 0;
    CHECK(layout_of(&o) == VSR_EINVAL);
    /* slab_bytes is a multiple of the page. */
    o = base;
    o.limits.slab_bytes = PAGE + 8;
    CHECK(layout_of(&o) == VSR_EINVAL);
    o.limits.slab_bytes = 2 * PAGE;
    CHECK(layout_of(&o) == VSR_OK);

    /* ELIMIT: minimum slabs = links + streams * (window + 1) + 2 *
     * replicas + 4 + caller_slabs = 4 + 6 + 4 + 4 + caller. */
    o = base;
    o.limits.slabs = 20;
    CHECK(layout_of(&o) == VSR_OK);
    o.limits.slabs = 19;
    CHECK(layout_of(&o) == VSR_ELIMIT);
    o = base;
    o.limits.caller_slabs = SLABS - 18;
    CHECK(layout_of(&o) == VSR_OK);
    o.limits.caller_slabs = SLABS - 17;
    CHECK(layout_of(&o) == VSR_ELIMIT);
    o = base;
    o.limits.slabs = 32769;
    CHECK(layout_of(&o) == VSR_ELIMIT);
    /* A stream chunk and its 40 framing bytes fit a slab. */
    o = base;
    o.stream_chunk_bytes = PAGE - 39;
    CHECK(layout_of(&o) == VSR_ELIMIT);
    /* The listener chain fits one batch. */
    o = base;
    o.limits.batch = 3;
    CHECK(layout_of(&o) == VSR_ELIMIT);
    o.limits.batch = 4;
    CHECK(layout_of(&o) == VSR_OK);
    /* File slots: listeners + links + streams + 2 * replicas. */
    o = base;
    o.limits.file_slots = 10;
    CHECK(layout_of(&o) == VSR_OK);
    o.limits.file_slots = 9;
    CHECK(layout_of(&o) == VSR_ELIMIT);
    o.limits.file_slots = 11;
    with_listeners(&o);
    CHECK(layout_of(&o) == VSR_ELIMIT);
    o.limits.file_slots = 12;
    CHECK(layout_of(&o) == VSR_OK);
    o = base;
    o.file_slot_base = (uint32_t)INT32_MAX - 10;
    CHECK(layout_of(&o) == VSR_ELIMIT);
    o.file_slot_base = (uint32_t)INT32_MAX - 11;
    CHECK(layout_of(&o) == VSR_OK);
    /* Buffer regions: the pool plus one per replica, 16-bit indexes. */
    o = base;
    o.limits.buffer_regions = 2;
    CHECK(layout_of(&o) == VSR_ELIMIT);
    o = base;
    o.buffer_region_base = 65534;
    CHECK(layout_of(&o) == VSR_ELIMIT);
    o.buffer_region_base = 65533;
    CHECK(layout_of(&o) == VSR_OK);
    /* Lease ids hold 23 bits of replica index. */
    o = base;
    o.limits.replicas = (1u << 23) + 1;
    CHECK(layout_of(&o) == VSR_ELIMIT);
    /* Overflow of the derived capacities is ELIMIT, never a bogus size. */
    o = base;
    o.limits.nodes = UINT32_MAX;
    o.limits.link_queue = UINT32_MAX;
    CHECK(layout_of(&o) == VSR_ELIMIT);
    o = base;
    o.limits.links = UINT32_MAX;
    CHECK(layout_of(&o) == VSR_ELIMIT);
    o = base;
    o.limits.streams = UINT32_MAX;
    o.limits.stream_window = UINT32_MAX;
    CHECK(layout_of(&o) == VSR_ELIMIT);
    o = base;
    o.limits.ops = UINT32_MAX;
    CHECK(layout_of(&o) == VSR_ELIMIT || layout_of(&o) == VSR_OK);
    /* The table size functions overflow on their own. */
    o = base;
    o.limits.nodes = UINT32_MAX;
    o.limits.link_queue = UINT32_MAX;
    CHECK(vsr_io_links_size(&o.limits, &bytes, &alignment) == VSR_ELIMIT);
    o = base;
    o.limits.streams = UINT32_MAX;
    o.limits.stream_window = UINT32_MAX;
    CHECK(vsr_io_streams_size(&o.limits, &bytes, &alignment) == VSR_ELIMIT);
    CHECK(vsr_io_links_size(NULL, &bytes, &alignment) == VSR_EINVAL);
    CHECK(vsr_io_streams_size(&o.limits, NULL, &alignment) == VSR_EINVAL);
}

/* -------------------------------------------------------------------------
 * Init, registration, close, deinit
 * ---------------------------------------------------------------------- */

static struct vsr_io *open_engine(const struct vsr_io_options *options)
{
    struct vsr_io_layout layout;
    struct vsr_io_region region;
    struct vsr_io_region pool;
    struct vsr_io *io = NULL;

    CHECK(vsr_io_layout(options, &layout) == VSR_OK);
    CHECK(layout.metadata.size <= sizeof(metadata));
    CHECK(layout.payload.size <= sizeof(payload));
    memset(metadata, 0xEE, sizeof(metadata));
    region.base = metadata;
    region.size = layout.metadata.size;
    pool.base = payload;
    pool.size = layout.payload.size;
    fake_reset();
    CHECK(vsr_io_init(options, &region, &pool, &io) == VSR_OK);
    CHECK(io != NULL);
    return io;
}

static void close_engine(struct vsr_io *io)
{
    CHECK(vsr_io_close(io) == VSR_OK);
    CHECK(vsr_io_deinit(io) == VSR_OK);
}

static void check_registration(const struct vsr_io *io)
{
    const struct fake_call *region = &fake.calls[0];
    const struct fake_call *ring = &fake.calls[1];
    size_t entries = 1;

    while (entries < io->options.limits.slabs) {
        entries *= 2;
    }
    CHECK(fake.count == 2);
    CHECK(region->kind == FAKE_UPDATE_BUFFER);
    CHECK(region->index == REGION_BASE);
    CHECK(region->base == payload);
    CHECK(region->size ==
          (size_t)io->options.limits.slabs * io->options.limits.slab_bytes);
    CHECK(ring->kind == FAKE_BUFFER_RING);
    CHECK(ring->index == GROUP);
    CHECK(ring->entries == entries);
    CHECK(ring->flags == VSR_IO_BUFFER_RING_INCREMENTAL);
    CHECK(ring->base == io->pool.ring_memory);
    CHECK(ring->size == entries * 16);
    CHECK(((uintptr_t)ring->base & (PAGE - 1)) == 0);
    CHECK((const unsigned char *)ring->base >= metadata &&
          (const unsigned char *)ring->base + ring->size <=
              metadata + sizeof(metadata));
    CHECK(io->pool.ring_registered);
    CHECK(io->pool.ring_entries == entries);
}

static void test_init(void)
{
    struct vsr_io_options options = base_options();
    struct vsr_io *io;
    struct vsr_io_stats stats;
    struct vsr_io_layout layout;
    struct vsr_io_region region;
    struct vsr_io_region pool;
    const struct vsr_io_limits *l = &options.limits;
    uint32_t base;
    unsigned char random[16];

    with_listeners(&options);
    io = open_engine(&options);
    check_registration(io);
    CHECK((unsigned char *)io >= metadata &&
          (unsigned char *)io < metadata + sizeof(metadata));
    /* Options are copied, the listen addresses into the region. */
    CHECK(io->options.node == 7);
    CHECK(io->options.listen == io->listen_copy);
    CHECK(io->options.listen != listen_addresses);
    CHECK(io->options.listen_count == 2);
    CHECK(memcmp(io->listen_copy, listen_addresses, sizeof(listen_addresses)) ==
          0);
    CHECK((unsigned char *)io->listen_copy >= metadata &&
          (unsigned char *)io->listen_copy < metadata + sizeof(metadata));
    CHECK(io->options.limits.slabs == SLABS);
    CHECK(io->ex.ops == &fake_ops && io->ex.ctx == &fake);
    CHECK(io->state == 0);
    CHECK(io->page_bytes == PAGE);
    /* Every table is initialized for the limits. */
    CHECK(io->pool.base == payload && io->pool.slabs == SLABS);
    CHECK(io->pool.reserve == l->replicas + 1);
    CHECK(io->pool.caller_slabs == 2);
    CHECK(io->pool.region_index == REGION_BASE && io->pool.group == GROUP);
    CHECK(io->pool.free_count == SLABS);
    CHECK(io->slots.owner == OWNER);
    CHECK(io->slots.count ==
          8 + 2 + 7 * 4 + 2 * (2 + 2) + 2 * (INFLIGHT_WRITES_MAX + 8));
    CHECK(io->slots.free_count == io->slots.count);
    CHECK(io->deadlines.capacity ==
          l->links + l->nodes + 4 * l->replicas + l->streams);
    CHECK(io->deadlines.count == 0);
    CHECK(io->links.nodes_count == l->nodes);
    CHECK(io->links.authorizations_count == l->authorizations);
    CHECK(io->links.links_count == l->links);
    CHECK(io->links.link_queue == l->link_queue);
    CHECK(io->links.frame_limit == l->slab_bytes);
    CHECK(io->links.established == 0 && io->links.pending == 0);
    for (uint32_t i = 0; i < l->links; ++i) {
        const struct vsr_io_link *link = &io->links.links[i];

        CHECK(link->state == VSR_IO_LINK_FREE);
        CHECK(link->node == VSR_IO_NO_NODE && link->node_index == NONE);
        CHECK(link->fd == -1);
        CHECK(link->send_slab == NONE && link->recv_slot == NONE);
        CHECK(link->vecs == io->links.vecs + (size_t)i * VSR_IO_SEND_VECTORS);
        for (uint32_t s = 0; s < VSR_IO_LINK_SENDS; ++s) {
            CHECK(link->sends[s].slot == NONE);
        }
    }
    for (uint32_t i = 0; i < l->nodes; ++i) {
        CHECK(io->links.nodes[i].id == 0);
        CHECK(io->links.nodes[i].carrier == NONE);
        CHECK(io->links.nodes[i].next_dial_ns == VSR_NO_DEADLINE);
    }
    CHECK(io->streams.count == l->streams);
    CHECK(io->streams.window == l->stream_window);
    CHECK(io->streams.chunk_bytes == options.stream_chunk_bytes);
    CHECK(io->streams.active == 0);
    for (uint32_t i = 0; i < l->streams; ++i) {
        const struct vsr_io_stream *stream = &io->streams.streams[i];

        CHECK(stream->state == VSR_IO_STREAM_FREE);
        CHECK(stream->link == NONE && stream->request_slab == NONE);
        CHECK(stream->units ==
              io->streams.units + (size_t)i * l->stream_window);
        for (uint32_t u = 0; u < l->stream_window; ++u) {
            CHECK(stream->units[u].state == 0);
            CHECK(stream->units[u].slab == NONE);
        }
    }
    /* Deadline handles are dense per kind, kinds in enum order (decision
     * 60): LINK, DIAL, CORE, FLUSH, SYNC, STREAM, CAPTURE. */
    base = 0;
    for (uint32_t i = 0; i < l->links; ++i) {
        CHECK(io->links.links[i].deadline == base + i);
        CHECK(io->deadlines.entries[base + i].kind == VSR_IO_DEADLINE_LINK);
        CHECK(io->deadlines.entries[base + i].index == i);
    }
    base += l->links;
    for (uint32_t i = 0; i < l->nodes; ++i) {
        CHECK(io->deadlines.entries[base + i].kind == VSR_IO_DEADLINE_DIAL);
        CHECK(io->deadlines.entries[base + i].index == i);
    }
    base += l->nodes;
    for (uint32_t i = 0; i < l->replicas; ++i) {
        CHECK(io->deadlines.entries[base + i].kind == VSR_IO_DEADLINE_CORE);
        CHECK(io->deadlines.entries[base + l->replicas + i].kind ==
              VSR_IO_DEADLINE_FLUSH);
        CHECK(io->deadlines.entries[base + 2 * l->replicas + i].kind ==
              VSR_IO_DEADLINE_SYNC);
        CHECK(io->deadlines.entries[base + 2 * l->replicas + i].index == i);
    }
    base += 3 * l->replicas;
    for (uint32_t i = 0; i < l->streams; ++i) {
        CHECK(io->streams.streams[i].deadline == base + i);
        CHECK(io->deadlines.entries[base + i].kind == VSR_IO_DEADLINE_STREAM);
        CHECK(io->deadlines.entries[base + i].index == i);
    }
    base += l->streams;
    for (uint32_t i = 0; i < l->replicas; ++i) {
        CHECK(io->deadlines.entries[base + i].kind == VSR_IO_DEADLINE_CAPTURE);
        CHECK(io->deadlines.entries[base + i].index == i);
    }
    CHECK(base + l->replicas == io->deadlines.capacity);
    /* Replicas, file slots, the forwarded ring and the stats start empty. */
    CHECK(io->replicas_count == 0);
    for (uint32_t i = 0; i < l->replicas; ++i) {
        CHECK(io->replicas[i].state == VSR_IO_REPLICA_FREE);
        CHECK(io->replicas[i].index == i && io->replicas[i].io == io);
    }
    CHECK(io->file_slot_next == FILE_SLOT_BASE);
    CHECK(io->file_slots_free_count == 0);
    CHECK(io->forwarded_count == 0 && io->forwarded_overflow == 0);
    CHECK(io->wake_pending == 0);
    CHECK(io->uring == NULL);
    memset(&stats, 0xEE, sizeof(stats));
    vsr_io_get_stats(io, &stats);
    CHECK(stats.replicas == 0 && stats.links == 0 && stats.links_pending == 0);
    CHECK(stats.streams == 0 && stats.slabs_free == SLABS);
    CHECK(stats.closed == 0 && stats.failure == 0);
    CHECK(stats.bytes_sent == 0 && stats.frames_rejected == 0);
    vsr_io_get_stats(NULL, &stats);
    vsr_io_get_stats(io, NULL);
    /* Wake sets the flag and reaches the executor. */
    vsr_io_wake(io);
    CHECK(io->wake_pending == 1);
    CHECK(fake.wakes == 1);
    vsr_io_wake(NULL);
    CHECK(fake.wakes == 1);
    /* Random bytes come from the executor. */
    memset(random, 0, sizeof(random));
    vsr_io_engine_random(io, random, sizeof(random));
    CHECK(fake.randoms == 1);
    CHECK(random[0] == 1 && random[1] == 8);
    /* Close and deinit: EBUSY before close, EBUSY with a replica, closed
     * at once with nothing in flight, deinit unregisters in reverse. */
    CHECK(vsr_io_deinit(io) == VSR_EBUSY);
    CHECK(vsr_io_close(NULL) == VSR_EINVAL);
    CHECK(vsr_io_deinit(NULL) == VSR_EINVAL);
    io->replicas_count = 1;
    CHECK(vsr_io_close(io) == VSR_EBUSY);
    io->replicas_count = 0;
    CHECK(vsr_io_close(io) == VSR_OK);
    CHECK(io->state == 2);
    vsr_io_get_stats(io, &stats);
    CHECK(stats.closed == 1);
    CHECK(vsr_io_close(io) == VSR_OK);
    CHECK(fake.count == 2);
    CHECK(vsr_io_deinit(io) == VSR_OK);
    CHECK(fake.count == 4);
    CHECK(fake.calls[2].kind == FAKE_BUFFER_RING);
    CHECK(fake.calls[2].index == GROUP && fake.calls[2].entries == 0);
    CHECK(fake.calls[3].kind == FAKE_UPDATE_BUFFER);
    CHECK(fake.calls[3].index == REGION_BASE && fake.calls[3].base == NULL);
    CHECK(!io->pool.ring_registered && io->pool.kernel_count == 0);

    /* Closing waits for slots and links: a busy slot or link keeps the
     * engine closing until check_closed runs again (part 2 drives it). */
    options = base_options();
    io = open_engine(&options);
    CHECK(vsr_io_slots_alloc(&io->slots, VSR_IO_SLOT_RECV, 1, 0, 0, 0) == 0);
    CHECK(vsr_io_close(io) == VSR_OK);
    CHECK(io->state == 1);
    CHECK(vsr_io_deinit(io) == VSR_EBUSY);
    vsr_io_slots_free(&io->slots, 0);
    io->links.links[1].state = VSR_IO_LINK_CLOSING;
    CHECK(vsr_io_close(io) == VSR_OK);
    CHECK(io->state == 1);
    io->links.links[1].state = VSR_IO_LINK_FREE;
    CHECK(vsr_io_close(io) == VSR_OK);
    CHECK(io->state == 2);
    CHECK(vsr_io_deinit(io) == VSR_OK);

    /* Scripted registration failures: the errno comes back and what
     * succeeded is undone. */
    options = base_options();
    CHECK(vsr_io_layout(&options, &layout) == VSR_OK);
    region.base = metadata;
    region.size = layout.metadata.size;
    pool.base = payload;
    pool.size = layout.payload.size;
    fake_reset();
    fake.fail_buffer_ring = -ENOMEM;
    io = (struct vsr_io *)(void *)metadata;
    CHECK(vsr_io_init(&options, &region, &pool, &io) == -ENOMEM);
    CHECK(io == NULL);
    CHECK(fake.count == 3);
    CHECK(fake.calls[0].kind == FAKE_UPDATE_BUFFER);
    CHECK(fake.calls[0].base == payload);
    CHECK(fake.calls[1].kind == FAKE_BUFFER_RING);
    CHECK(fake.calls[2].kind == FAKE_UPDATE_BUFFER);
    CHECK(fake.calls[2].index == REGION_BASE && fake.calls[2].base == NULL);
    fake_reset();
    fake.fail_update_buffer = -EBUSY;
    CHECK(vsr_io_init(&options, &region, &pool, &io) == -EBUSY);
    CHECK(io == NULL);
    CHECK(fake.count == 1);
    /* Region and argument checks. */
    fake_reset();
    CHECK(vsr_io_init(&options, &region, &pool, NULL) == VSR_EINVAL);
    CHECK(vsr_io_init(NULL, &region, &pool, &io) == VSR_EINVAL);
    CHECK(vsr_io_init(&options, NULL, &pool, &io) == VSR_EINVAL);
    CHECK(vsr_io_init(&options, &region, NULL, &io) == VSR_EINVAL);
    region.base = metadata + 8;
    CHECK(vsr_io_init(&options, &region, &pool, &io) == VSR_EINVAL);
    region.base = metadata;
    region.size = layout.metadata.size - 1;
    CHECK(vsr_io_init(&options, &region, &pool, &io) == VSR_ELIMIT);
    region.size = layout.metadata.size;
    pool.base = payload + 16;
    CHECK(vsr_io_init(&options, &region, &pool, &io) == VSR_EINVAL);
    pool.base = payload;
    pool.size = layout.payload.size - 1;
    CHECK(vsr_io_init(&options, &region, &pool, &io) == VSR_ELIMIT);
    pool.size = layout.payload.size;
    region.base = NULL;
    CHECK(vsr_io_init(&options, &region, &pool, &io) == VSR_EINVAL);
    region.base = metadata;
    options.executor.ops = NULL;
    CHECK(vsr_io_init(&options, &region, &pool, &io) == VSR_EINVAL);
    {
        struct vsr_io_executor_ops partial = fake_ops;

        partial.provide = NULL;
        options.executor.ops = &partial;
        CHECK(vsr_io_init(&options, &region, &pool, &io) == VSR_EINVAL);
    }
    options.executor.ops = &fake_ops;
    options.limits.batch = 1;
    CHECK(vsr_io_init(&options, &region, &pool, &io) == VSR_ELIMIT);
    options.limits.batch = 0;
    CHECK(vsr_io_init(&options, &region, &pool, &io) == VSR_EINVAL);
    CHECK(fake.count == 0);
    CHECK(io == NULL);
}

/* -------------------------------------------------------------------------
 * Caller slabs
 * ---------------------------------------------------------------------- */

static void test_slabs(void)
{
    struct vsr_io_options options = base_options();
    struct vsr_io *io = open_engine(&options);
    struct vsr_io_slab first;
    struct vsr_io_slab second;
    struct vsr_io_slab third;
    uint32_t taken;

    memset(&first, 0xEE, sizeof(first));
    CHECK(vsr_io_slab_acquire(NULL, &first) == VSR_EINVAL);
    CHECK(vsr_io_slab_acquire(io, NULL) == VSR_EINVAL);
    CHECK(vsr_io_slab_acquire(io, &first) == VSR_OK);
    CHECK(vsr_io_slab_acquire(io, &second) == VSR_OK);
    CHECK(first.id != second.id);
    CHECK(first.id < SLABS && second.id < SLABS);
    CHECK(first.base == payload + (size_t)first.id * PAGE);
    CHECK(second.base == payload + (size_t)second.id * PAGE);
    CHECK(first.length == PAGE && second.length == PAGE);
    CHECK(first.region == REGION_BASE && second.region == REGION_BASE);
    CHECK(io->pool.caller_taken == 2);
    CHECK(io->pool.entries[first.id].caller == 1);
    /* The share is two: the third is ELIMIT until one is released. */
    CHECK(vsr_io_slab_acquire(io, &third) == VSR_ELIMIT);
    CHECK(vsr_io_slab_release(io, 200) == VSR_EINVAL);
    taken = first.id == 0 ? 1 : 0;
    while (taken == first.id || taken == second.id) {
        taken++;
    }
    CHECK(vsr_io_slab_release(io, (uint16_t)taken) == VSR_EINVAL);
    CHECK(vsr_io_slab_release(NULL, first.id) == VSR_EINVAL);
    CHECK(vsr_io_slab_release(io, first.id) == VSR_OK);
    CHECK(io->pool.entries[first.id].state == VSR_IO_SLAB_FREE);
    CHECK(vsr_io_slab_release(io, first.id) == VSR_EINVAL);
    CHECK(vsr_io_slab_acquire(io, &third) == VSR_OK);
    CHECK(io->pool.caller_taken == 2);
    CHECK(vsr_io_slab_release(io, second.id) == VSR_OK);
    CHECK(vsr_io_slab_release(io, third.id) == VSR_OK);
    CHECK(io->pool.caller_taken == 0 && io->pool.free_count == SLABS);
    /* The free slabs never drop to the reserve for the caller. */
    for (uint32_t i = 0; i < SLABS - io->pool.reserve; ++i) {
        CHECK(vsr_io_pool_acquire(&io->pool, false) != NONE);
    }
    CHECK(io->pool.free_count == io->pool.reserve);
    CHECK(vsr_io_slab_acquire(io, &first) == VSR_ELIMIT);
    for (uint32_t i = 0; i < SLABS - io->pool.reserve; ++i) {
        vsr_io_pool_release(&io->pool, i);
    }
    CHECK(vsr_io_slab_acquire(io, &first) == VSR_OK);
    CHECK(vsr_io_slab_release(io, first.id) == VSR_OK);
    /* No acquire once closing. */
    CHECK(vsr_io_close(io) == VSR_OK);
    CHECK(vsr_io_slab_acquire(io, &first) == VSR_EINVAL);
    CHECK(vsr_io_deinit(io) == VSR_OK);
    /* No share: ELIMIT at once. */
    options.limits.caller_slabs = 0;
    io = open_engine(&options);
    CHECK(vsr_io_slab_acquire(io, &first) == VSR_ELIMIT);
    close_engine(io);
}

/* -------------------------------------------------------------------------
 * Forwarded-op ring
 * ---------------------------------------------------------------------- */

/* What vsr_io_poll does with the ring: takes count entries from the head
 * and clears the overflow mark. */
static void dequeue(struct vsr_io *io, uint32_t count)
{
    CHECK(count <= io->forwarded_count);
    io->forwarded_head = (io->forwarded_head + count) % io->options.limits.ops;
    io->forwarded_count -= count;
    io->forwarded_overflow = 0;
}

static void test_forward(void)
{
    struct vsr_io_options options = base_options();
    struct vsr_io *io = open_engine(&options);
    struct vsr_io_replica *replica = &io->replicas[1];
    struct vsr_io_forwarded *entry;
    uint32_t ops = options.limits.ops;

    CHECK(ops == 6);
    for (uint32_t i = 0; i < ops; ++i) {
        entry = vsr_io_forward(io, i % 2 ? replica : NULL, i);
        CHECK(entry == &io->forwarded[i]);
        CHECK(entry->op.replica == (i % 2 ? replica : NULL));
        CHECK(entry->op.kind == i);
        CHECK(entry->op.op.id == 0 && entry->op.op.data == NULL);
        entry->op.op.id = 100 + i;
        CHECK(io->forwarded_count == i + 1);
        CHECK(io->forwarded_overflow == 0);
    }
    CHECK(vsr_io_forward(io, replica, 7) == NULL);
    CHECK(io->forwarded_overflow == 1);
    CHECK(io->forwarded_count == ops);
    dequeue(io, 4);
    CHECK(io->forwarded_head == 4 && io->forwarded_count == 2);
    /* Wrap-around: the next entries reuse the array's start. */
    for (uint32_t i = 0; i < 3; ++i) {
        entry = vsr_io_forward(io, NULL, 10 + i);
        CHECK(entry == &io->forwarded[i]);
        CHECK(entry->op.kind == 10 + i);
        CHECK(entry->op.op.id == 0);
    }
    CHECK(io->forwarded_count == 5);
    /* FIFO order from the head: kinds 4, 5, 10, 11, 12. */
    for (uint32_t i = 0; i < io->forwarded_count; ++i) {
        uint32_t at = (io->forwarded_head + i) % ops;

        CHECK(io->forwarded[at].op.kind == (i < 2 ? 4 + i : 8 + i));
    }
    CHECK(vsr_io_forward(io, NULL, 13) == &io->forwarded[3]);
    CHECK(vsr_io_forward(io, NULL, 14) == NULL);
    CHECK(io->forwarded_overflow == 1);
    dequeue(io, 6);
    CHECK(io->forwarded_count == 0 && io->forwarded_head == 4);
    CHECK(vsr_io_forward(io, NULL, 15) == &io->forwarded[4]);
    close_engine(io);
}

/* -------------------------------------------------------------------------
 * A replica shell for the lease, completion and delivery helpers, set up
 * the way vsr_io_attach (part 2) will: the tables are the test's.
 * ---------------------------------------------------------------------- */

#define REGIONS 3u
#define OPERATIONS 4u
#define REGION_BYTES 1024u

static struct vsr_io_lease leases[REGIONS];
static struct vsr_io_queued_event messages[REGIONS];
static struct vsr_io_queued_event completions[OPERATIONS];
static _Alignas(16) unsigned char regions[REGIONS][REGION_BYTES];

static struct vsr_io_replica *open_replica(struct vsr_io *io, uint32_t index)
{
    struct vsr_io_replica *replica = &io->replicas[index];

    CHECK(replica->io == io && replica->index == index);
    replica->state = VSR_IO_REPLICA_RUNNING;
    replica->options.cluster.hi = 0x1000 + index;
    replica->options.cluster.lo = 0x2000 + index;
    replica->options.limits.operations = OPERATIONS;
    replica->options.limits.members = 3;
    replica->options.limits.batch_entries = 4;
    replica->options.limits.spans_per_blob = 1;
    replica->options.limits.command_bytes = 64;
    replica->options.limits.result_bytes = 64;
    replica->options.limits.manifest_bytes = 64;
    replica->options.limits.message_bytes = 256;
    replica->regions_count = REGIONS;
    memset(leases, 0, sizeof(leases));
    for (uint32_t i = 0; i < REGIONS; ++i) {
        leases[i].slab = NONE;
        leases[i].pin = NONE;
        leases[i].region.base = regions[i];
        leases[i].region.size = REGION_BYTES;
        leases[i].region.used = 0;
    }
    replica->leases = leases;
    replica->leases_free = REGIONS;
    memset(messages, 0, sizeof(messages));
    replica->messages = messages;
    replica->messages_head = 0;
    replica->messages_count = 0;
    memset(completions, 0, sizeof(completions));
    replica->completions = completions;
    replica->completions_head = 0;
    replica->completions_count = 0;
    io->replicas_count++;
    return replica;
}

static void close_replica(struct vsr_io_replica *replica)
{
    replica->state = VSR_IO_REPLICA_FREE;
    replica->io->replicas_count--;
}

static uint64_t lease_id_for(uint32_t replica, uint32_t region,
                             uint64_t generation)
{
    return VSR_IO_LEASE_ENGINE | (uint64_t)replica << 40 |
           (uint64_t)region << 16 | generation;
}

static void test_leases(void)
{
    struct vsr_io_options options = base_options();
    struct vsr_io *io = open_engine(&options);
    struct vsr_io_replica *replica = open_replica(io, 1);
    struct vsr_io_replica *found = NULL;
    uint32_t index = NONE;
    uint32_t a;
    uint32_t b;
    uint64_t id0;
    uint64_t id1;
    uint64_t id2;

    /* Slab a is retained for its lease (a MESSAGE keeps the receive
     * reference too); slab b is handed over (a cold LOAD's own slab). */
    a = vsr_io_pool_acquire(&io->pool, false);
    b = vsr_io_pool_acquire(&io->pool, false);
    CHECK(a != NONE && b != NONE && a != b);
    vsr_io_pool_retain(&io->pool, a);
    CHECK(io->pool.entries[a].refs == 2 && io->pool.entries[b].refs == 1);
    leases[0].region.used = 100;
    CHECK(vsr_io_lease_alloc(replica, a, NONE) == 0);
    CHECK(leases[0].state == 1 && leases[0].slab == a && leases[0].pin == NONE);
    CHECK(leases[0].generation == 1 && leases[0].region.used == 0);
    CHECK(replica->leases_free == 2);
    CHECK(vsr_io_lease_alloc(replica, b, NONE) == 1);
    CHECK(vsr_io_lease_alloc(replica, NONE, 7) == 2);
    CHECK(leases[2].slab == NONE && leases[2].pin == 7);
    CHECK(replica->leases_free == 0);
    CHECK(vsr_io_lease_alloc(replica, NONE, NONE) == NONE);
    /* Id encoding: ENGINE | replica << 40 | region << 16 | generation. */
    id0 = vsr_io_lease_id(replica, 0);
    id1 = vsr_io_lease_id(replica, 1);
    id2 = vsr_io_lease_id(replica, 2);
    CHECK(id0 == lease_id_for(1, 0, 1));
    CHECK(id1 == lease_id_for(1, 1, 1));
    CHECK(id2 == lease_id_for(1, 2, 1));
    CHECK((id0 & VSR_IO_LEASE_ENGINE) != 0);
    /* Resolution. */
    CHECK(vsr_io_lease_resolve(io, id0, &found, &index) == VSR_OK);
    CHECK(found == replica && index == 0);
    CHECK(vsr_io_lease_resolve(io, id2, &found, &index) == VSR_OK);
    CHECK(found == replica && index == 2);
    CHECK(vsr_io_lease_resolve(io, id0 & ~VSR_IO_LEASE_ENGINE, &found,
                               &index) == VSR_EINVAL);
    CHECK(vsr_io_lease_resolve(io, lease_id_for(0, 0, 1), &found, &index) ==
          VSR_EINVAL); /* Replica 0 is FREE. */
    CHECK(vsr_io_lease_resolve(io, lease_id_for(2, 0, 1), &found, &index) ==
          VSR_EINVAL); /* Beyond limits.replicas. */
    CHECK(vsr_io_lease_resolve(io, lease_id_for(1, 3, 1), &found, &index) ==
          VSR_EINVAL); /* Beyond regions_count. */
    CHECK(vsr_io_lease_resolve(io, lease_id_for(1, 0, 2), &found, &index) ==
          VSR_EINVAL); /* Wrong generation. */
    CHECK(vsr_io_lease_resolve(io, lease_id_for(1, 0, 0), &found, &index) ==
          VSR_EINVAL);
    /* Release drops the slab reference and frees the region; a stale id
     * no longer resolves, before and after the entry is reused. */
    leases[0].region.used = 50;
    vsr_io_lease_release(replica, 0);
    CHECK(leases[0].state == 0 && leases[0].slab == NONE);
    CHECK(leases[0].region.used == 0);
    CHECK(io->pool.entries[a].refs == 1);
    CHECK(replica->leases_free == 1);
    CHECK(vsr_io_lease_resolve(io, id0, &found, &index) == VSR_EINVAL);
    CHECK(vsr_io_lease_alloc(replica, a, NONE) == 0);
    CHECK(leases[0].generation == 2);
    CHECK(vsr_io_lease_id(replica, 0) == lease_id_for(1, 0, 2));
    CHECK(vsr_io_lease_resolve(io, id0, &found, &index) == VSR_EINVAL);
    CHECK(vsr_io_lease_resolve(io, vsr_io_lease_id(replica, 0), &found,
                               &index) == VSR_OK);
    CHECK(found == replica && index == 0);
    vsr_io_lease_release(replica, 0);
    CHECK(io->pool.entries[a].state == VSR_IO_SLAB_FREE);
    vsr_io_lease_release(replica, 1);
    CHECK(io->pool.entries[b].state == VSR_IO_SLAB_FREE);
    CHECK(io->pool.free_count == SLABS);
    vsr_io_lease_release(replica, 2);
    CHECK(replica->leases_free == REGIONS);
    CHECK(vsr_io_lease_resolve(io, id1, &found, &index) == VSR_EINVAL);
    CHECK(vsr_io_lease_resolve(io, id2, &found, &index) == VSR_EINVAL);
    /* The lookup by cluster id skips FREE entries. */
    CHECK(vsr_io_engine_replica(io, replica->options.cluster) == replica);
    CHECK(vsr_io_engine_replica(io, io->replicas[0].options.cluster) == NULL);
    io->replicas[0].options.cluster.hi = 0x1000;
    io->replicas[0].options.cluster.lo = 0x2000;
    CHECK(vsr_io_engine_replica(io, io->replicas[0].options.cluster) == NULL);
    io->replicas[0].state = VSR_IO_REPLICA_OPENING;
    CHECK(vsr_io_engine_replica(io, io->replicas[0].options.cluster) ==
          &io->replicas[0]);
    io->replicas[0].state = VSR_IO_REPLICA_FREE;
    close_replica(replica);
    close_engine(io);
}

/* -------------------------------------------------------------------------
 * Engine file slots
 * ---------------------------------------------------------------------- */

static void test_slots(void)
{
    struct vsr_io_options options = base_options();
    struct vsr_io *io = open_engine(&options);
    uint32_t count = options.limits.file_slots;

    for (uint32_t i = 0; i < count; ++i) {
        CHECK(vsr_io_engine_slot_alloc(io) == FILE_SLOT_BASE + i);
    }
    CHECK(vsr_io_engine_slot_alloc(io) == NONE);
    CHECK(io->file_slot_next == FILE_SLOT_BASE + count);
    /* Freed slots are reused most recently freed first, before the
     * high-water mark advances. */
    vsr_io_engine_slot_free(io, FILE_SLOT_BASE + 5);
    vsr_io_engine_slot_free(io, FILE_SLOT_BASE + 3);
    CHECK(io->file_slots_free_count == 2);
    CHECK(vsr_io_engine_slot_alloc(io) == FILE_SLOT_BASE + 3);
    CHECK(vsr_io_engine_slot_alloc(io) == FILE_SLOT_BASE + 5);
    CHECK(vsr_io_engine_slot_alloc(io) == NONE);
    for (uint32_t i = 0; i < count; ++i) {
        vsr_io_engine_slot_free(io, FILE_SLOT_BASE + i);
    }
    CHECK(io->file_slots_free_count == count);
    for (uint32_t i = 0; i < count; ++i) {
        CHECK(vsr_io_engine_slot_alloc(io) == FILE_SLOT_BASE + count - 1 - i);
    }
    CHECK(vsr_io_engine_slot_alloc(io) == NONE);
    close_engine(io);
    /* The high-water mark comes before any reuse on a fresh engine. */
    io = open_engine(&options);
    CHECK(vsr_io_engine_slot_alloc(io) == FILE_SLOT_BASE);
    CHECK(vsr_io_engine_slot_alloc(io) == FILE_SLOT_BASE + 1);
    vsr_io_engine_slot_free(io, FILE_SLOT_BASE);
    CHECK(vsr_io_engine_slot_alloc(io) == FILE_SLOT_BASE);
    CHECK(vsr_io_engine_slot_alloc(io) == FILE_SLOT_BASE + 2);
    close_engine(io);
}

/* -------------------------------------------------------------------------
 * Internal completions
 * ---------------------------------------------------------------------- */

static void test_complete_core(void)
{
    struct vsr_io_options options = base_options();
    struct vsr_io *io = open_engine(&options);
    struct vsr_io_replica *replica = open_replica(io, 0);
    struct vsr_io_completion completion;
    const struct vsr_io_queued_event *queued;
    static const int data;
    uint32_t lease;

    lease = vsr_io_lease_alloc(replica, NONE, 3);
    CHECK(lease == 0);
    memset(&completion, 0, sizeof(completion));
    completion.op = 11;
    completion.status = VSR_IO_OK;
    completion.lease = NONE;
    vsr_io_engine_complete_core(replica, &completion);
    completion.op = 12;
    completion.status = VSR_IO_RETRY;
    completion.lease = lease;
    completion.data = &data;
    vsr_io_engine_complete_core(replica, &completion);
    completion.op = 13;
    completion.status = VSR_IO_FAILED;
    completion.lease = NONE;
    completion.data = NULL;
    vsr_io_engine_complete_core(replica, &completion);
    CHECK(replica->completions_count == 3 && replica->completions_head == 0);
    queued = &completions[0];
    CHECK(queued->event.type == VSR_EVENT_COMPLETE);
    CHECK(queued->event.id == 11 && queued->event.status == VSR_IO_OK);
    CHECK(queued->event.data == NULL && queued->event.lease == 0);
    CHECK(queued->lease == NONE && queued->kind == VSR_IO_EVENT_CORE);
    queued = &completions[1];
    CHECK(queued->event.id == 12 && queued->event.status == VSR_IO_RETRY);
    CHECK(queued->event.data == &data);
    CHECK(queued->event.lease == vsr_io_lease_id(replica, lease));
    CHECK(queued->lease == lease && queued->kind == VSR_IO_EVENT_CORE);
    queued = &completions[2];
    CHECK(queued->event.id == 13 && queued->event.status == VSR_IO_FAILED);
    /* The poll consumes from the head; later completions wrap. */
    replica->completions_head = 2;
    replica->completions_count = 1;
    completion.op = 14;
    vsr_io_engine_complete_core(replica, &completion);
    completion.op = 15;
    vsr_io_engine_complete_core(replica, &completion);
    completion.op = 16;
    vsr_io_engine_complete_core(replica, &completion);
    CHECK(replica->completions_count == OPERATIONS);
    for (uint32_t i = 0; i < OPERATIONS; ++i) {
        uint32_t at = (replica->completions_head + i) % OPERATIONS;

        CHECK(completions[at].event.id == 13 + i);
    }
    vsr_io_lease_release(replica, lease);
    close_replica(replica);
    close_engine(io);
}

/* -------------------------------------------------------------------------
 * MESSAGE delivery
 * ---------------------------------------------------------------------- */

#ifndef ENGINE_TABLES_CODEC
/* SKIP: src/io/codec.c is not in the library yet (branch wt/codec). This
 * stand-in rejects every body, so the rejection and no-region paths run
 * and the round trip below is skipped. Remove it and define
 * ENGINE_TABLES_CODEC once the codec lands. */
int vsr_io_codec_decode_message(struct vsr_io_cursor *cursor,
                                const struct vsr_limits *limits,
                                struct vsr_io_bump *region,
                                struct vsr_message **out)
{
    (void)cursor;
    (void)limits;
    (void)region;
    *out = NULL;
    return VSR_EINVAL;
}
#endif

#ifdef ENGINE_TABLES_CODEC
/* Encodes a body-less message (PREPARE_OK) into a slab as one frame:
 * header, then the envelope. Returns the body length. */
static size_t encode_frame(struct vsr_io *io, uint32_t slab,
                           const struct vsr_message *message,
                           const struct vsr_limits *limits)
{
    unsigned char *bytes = vsr_io_pool_slab(&io->pool, slab);
    struct vsr_io_writer writer;
    struct vsr_io_encoder encoder;
    struct vsr_io_vec vecs[8];
    uint32_t length = 0;
    uint32_t crc = 0;
    uint32_t count = 0;
    bool done = false;
    size_t used = 0;

    CHECK(vsr_io_codec_message_digest(message, limits, &length, &crc) ==
          VSR_OK);
    writer.base = bytes + 2048;
    writer.capacity = 1024;
    writer.used = 0;
    vsr_io_encoder_begin(&encoder, message, length, crc);
    CHECK(vsr_io_encoder_emit(&encoder, &writer, vecs, 8, &count, 4096,
                              &done) == VSR_OK);
    CHECK(done && count > 0);
    for (uint32_t i = 0; i < count; ++i) {
        memcpy(bytes + used, vecs[i].base, vecs[i].length);
        used += vecs[i].length;
    }
    CHECK(used == VSR_IO_FRAME_HEADER_BYTES + length);
    return length;
}
#endif

static void test_deliver(void)
{
    struct vsr_io_options options = base_options();
    struct vsr_io *io = open_engine(&options);
    struct vsr_io_replica *replica = open_replica(io, 0);
    struct vsr_io_cursor cursor;
    uint32_t slab = vsr_io_pool_acquire(&io->pool, false);
    unsigned char *bytes = vsr_io_pool_slab(&io->pool, slab);

    CHECK(slab != NONE);
    memset(bytes, 0, PAGE);
    vsr_io_cursor_init_one(&cursor, bytes, 56);
    /* No region free: nothing happens and the link retries. */
    replica->leases_free = 0;
    CHECK(!vsr_io_engine_deliver(io, replica, &cursor, slab));
    CHECK(io->pool.entries[slab].refs == 1);
    CHECK(io->stats.frames_rejected == 0 && replica->messages_count == 0);
    replica->leases_free = REGIONS;
    /* A body the codec rejects is dropped and counted; the region and the
     * slab reference it took are released. */
    memset(bytes, 0xFF, 56);
    CHECK(vsr_io_engine_deliver(io, replica, &cursor, slab));
    CHECK(io->stats.frames_rejected == 1);
    CHECK(replica->leases_free == REGIONS && replica->messages_count == 0);
    CHECK(io->pool.entries[slab].refs == 1);
    CHECK(leases[0].state == 0 && leases[0].region.used == 0);
#ifdef ENGINE_TABLES_CODEC
    {
        struct vsr_message message;
        const struct vsr_io_queued_event *queued;
        const struct vsr_message *decoded;
        struct vsr_io_replica *found = NULL;
        uint32_t index = NONE;
        size_t length;

        memset(&message, 0, sizeof(message));
        message.cluster = replica->options.cluster;
        message.epoch = 3;
        message.view = 4;
        message.from = 2;
        message.type = VSR_MSG_PREPARE_OK;
        message.number = 99;
        message.body = NULL;
        length = encode_frame(io, slab, &message, &replica->options.limits);
        /* A valid body round-trips into the message ring under a lease
         * holding the slab. */
        vsr_io_cursor_init_one(&cursor, bytes + VSR_IO_FRAME_HEADER_BYTES,
                               length);
        CHECK(vsr_io_engine_deliver(io, replica, &cursor, slab));
        CHECK(io->stats.frames_rejected == 1);
        CHECK(replica->messages_count == 1 &&
              replica->leases_free == REGIONS - 1);
        queued = &messages[0];
        CHECK(queued->event.type == VSR_EVENT_MESSAGE);
        CHECK(queued->event.id == 0 && queued->event.status == 0);
        CHECK(queued->kind == VSR_IO_EVENT_CORE && queued->lease == 0);
        CHECK(queued->event.lease == vsr_io_lease_id(replica, 0));
        CHECK(vsr_io_lease_resolve(io, queued->event.lease, &found, &index) ==
              VSR_OK);
        CHECK(found == replica && index == 0);
        CHECK(leases[0].state == 1 && leases[0].slab == slab);
        CHECK(io->pool.entries[slab].refs == 2);
        decoded = queued->event.data;
        CHECK((const unsigned char *)decoded >= regions[0] &&
              (const unsigned char *)decoded < regions[0] + REGION_BYTES);
        CHECK(decoded->cluster.hi == message.cluster.hi);
        CHECK(decoded->cluster.lo == message.cluster.lo);
        CHECK(decoded->epoch == 3 && decoded->view == 4);
        CHECK(decoded->from == 2 && decoded->type == VSR_MSG_PREPARE_OK);
        CHECK(decoded->number == 99 && decoded->body == NULL);
        /* Another cluster's message is rejected. */
        message.cluster.lo ^= 1;
        length = encode_frame(io, slab, &message, &replica->options.limits);
        vsr_io_cursor_init_one(&cursor, bytes + VSR_IO_FRAME_HEADER_BYTES,
                               length);
        CHECK(vsr_io_engine_deliver(io, replica, &cursor, slab));
        CHECK(io->stats.frames_rejected == 2);
        CHECK(replica->messages_count == 1 &&
              replica->leases_free == REGIONS - 1);
        CHECK(io->pool.entries[slab].refs == 2);
        /* A truncated body is malformed. */
        message.cluster.lo ^= 1;
        length = encode_frame(io, slab, &message, &replica->options.limits);
        vsr_io_cursor_init_one(&cursor, bytes + VSR_IO_FRAME_HEADER_BYTES,
                               length - 8);
        CHECK(vsr_io_engine_deliver(io, replica, &cursor, slab));
        CHECK(io->stats.frames_rejected == 3);
        CHECK(replica->leases_free == REGIONS - 1);
        /* Regions run out after REGIONS deliveries. */
        vsr_io_cursor_init_one(&cursor, bytes + VSR_IO_FRAME_HEADER_BYTES,
                               length);
        CHECK(vsr_io_engine_deliver(io, replica, &cursor, slab));
        CHECK(vsr_io_engine_deliver(io, replica, &cursor, slab));
        CHECK(replica->leases_free == 0 && replica->messages_count == REGIONS);
        CHECK(!vsr_io_engine_deliver(io, replica, &cursor, slab));
        CHECK(io->pool.entries[slab].refs == 1 + REGIONS);
        for (uint32_t i = 0; i < REGIONS; ++i) {
            vsr_io_lease_release(replica, messages[i].lease);
        }
        replica->messages_count = 0;
        CHECK(io->pool.entries[slab].refs == 1);
    }
#else
    printf("SKIP: deliver round trip needs src/io/codec.c (branch "
           "wt/codec)\n");
#endif
    vsr_io_pool_release(&io->pool, slab);
    CHECK(io->pool.free_count == SLABS);
    close_replica(replica);
    close_engine(io);
}

int main(void)
{
    test_layout();
    test_init();
    test_slabs();
    test_forward();
    test_leases();
    test_slots();
    test_complete_core();
    test_deliver();
    printf("engine_tables: ok\n");
    return 0;
}
