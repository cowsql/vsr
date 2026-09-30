#include "config.h"

#include "io/pool.h"

#include "checked.h"

#include <stdalign.h>
#include <string.h>

/* Provided-buffer ids are 16 bits and the ring a power of two of at most
 * 32768 entries (the io_uring limit), which bounds the slab count. */
#define POOL_MAX_SLABS 32768u
#define POOL_RING_ENTRY_BYTES 16u

/* Invariant checks in debug builds. The library links nothing beyond the
 * mem* functions, so a violation traps instead of calling assert. */
#ifdef NDEBUG
#define POOL_ASSERT(condition) ((void)sizeof(condition))
#else
#define POOL_ASSERT(condition) ((condition) ? (void)0 : __builtin_trap())
#endif

static uint32_t ring_entries(uint32_t slabs)
{
    uint32_t entries = 1;

    while (entries < slabs) {
        entries <<= 1;
    }
    return entries;
}

/* The ring memory comes first, at the page-aligned start of the region, and
 * the entries array follows it; 16 bytes per ring entry keep the array
 * aligned. */
static bool pool_bytes(uint32_t slabs, size_t *ring, size_t *total)
{
    size_t entries;

    return vsr_size_mul(ring_entries(slabs), POOL_RING_ENTRY_BYTES, ring) &&
           vsr_size_mul(slabs, sizeof(struct vsr_io_slab_entry), &entries) &&
           vsr_size_add(*ring, entries, total);
}

int vsr_io_pool_size(const struct vsr_io_limits *limits, size_t page_bytes,
                     size_t *bytes, size_t *alignment)
{
    size_t ring;
    size_t payload;
    size_t total;

    if (limits == NULL || bytes == NULL || alignment == NULL ||
        page_bytes == 0 || (page_bytes & (page_bytes - 1)) != 0 ||
        limits->slabs == 0 || limits->slab_bytes == 0 ||
        limits->slab_bytes % page_bytes != 0) {
        return VSR_EINVAL;
    }
    if (limits->slabs > POOL_MAX_SLABS ||
        !vsr_size_mul(limits->slabs, limits->slab_bytes, &payload) ||
        !pool_bytes(limits->slabs, &ring, &total)) {
        return VSR_ELIMIT;
    }
    *bytes = total;
    *alignment = page_bytes > alignof(struct vsr_io_slab_entry)
                     ? page_bytes
                     : alignof(struct vsr_io_slab_entry);
    return VSR_OK;
}

static struct vsr_io_slab_entry *entry(struct vsr_io_pool *pool, uint32_t id)
{
    POOL_ASSERT(id < pool->slabs);
    return &pool->entries[id];
}

/* The part of the reserve internal users do not hold. */
static uint32_t reserve_left(const struct vsr_io_pool *pool)
{
    return pool->reserve > pool->internal_taken
               ? pool->reserve - pool->internal_taken
               : 0;
}

/* Provision keeps this many slabs FREE: the part of the reserve internal
 * users do not hold plus the part of the caller's share the caller does not
 * hold (decisions 54 and 127). reserve < slabs and caller_slabs <= slabs <=
 * 32768, so the sum cannot wrap. */
static uint32_t provision_floor(const struct vsr_io_pool *pool)
{
    return reserve_left(pool) + (pool->caller_slabs - pool->caller_taken);
}

uint32_t vsr_io_pool_floor(const struct vsr_io_pool *pool)
{
    return provision_floor(pool);
}

static void update_pending(struct vsr_io_pool *pool)
{
    uint32_t floor = provision_floor(pool);

    pool->pending = pool->free_count > floor ? pool->free_count - floor : 0;
}

static void free_push(struct vsr_io_pool *pool, uint32_t id)
{
    struct vsr_io_slab_entry *slab = entry(pool, id);

    POOL_ASSERT(slab->refs == 0);
    /* The caller's reference goes through caller_release, which clears the
     * flag and returns the share first. */
    POOL_ASSERT(slab->caller == 0);
    POOL_ASSERT(pool->free_count + pool->kernel_count < pool->slabs);
    if (slab->internal != 0) {
        POOL_ASSERT(pool->internal_taken > 0);
        slab->internal = 0;
        pool->internal_taken--;
    }
    slab->state = VSR_IO_SLAB_FREE;
    slab->consumed = 0;
    slab->next = pool->free_head;
    pool->free_head = id;
    pool->free_count++;
    update_pending(pool);
}

static uint32_t free_pop(struct vsr_io_pool *pool)
{
    uint32_t id = pool->free_head;
    struct vsr_io_slab_entry *slab = entry(pool, id);

    POOL_ASSERT(pool->free_count > 0);
    POOL_ASSERT(slab->state == VSR_IO_SLAB_FREE && slab->refs == 0);
    pool->free_head = slab->next;
    pool->free_count--;
    slab->next = VSR_IO_INDEX_NONE;
    update_pending(pool);
    return id;
}

void vsr_io_pool_init(struct vsr_io_pool *pool, void *base, size_t size,
                      const struct vsr_io_limits *limits, uint32_t reserve,
                      uint32_t region_index, uint16_t group, void *memory,
                      size_t memory_size)
{
    size_t ring = 0;
    size_t total = 0;
    bool sized;
    void *entries;

    /* The slab range first: ring_entries never ends beyond 2^31 slabs. */
    POOL_ASSERT(limits->slabs > 0 && limits->slabs <= POOL_MAX_SLABS);
    POOL_ASSERT(limits->slab_bytes > 0);
    POOL_ASSERT(reserve < limits->slabs);
    POOL_ASSERT(limits->caller_slabs <= limits->slabs);
    POOL_ASSERT((size_t)limits->slabs * limits->slab_bytes <= size);
    sized = pool_bytes(limits->slabs, &ring, &total);
    POOL_ASSERT(sized && total <= memory_size);
    (void)sized;
    (void)size;
    entries = (unsigned char *)memory + ring;
    memset(memory, 0, total);
    memset(pool, 0, sizeof(*pool));
    pool->base = base;
    pool->size = (size_t)limits->slabs * limits->slab_bytes;
    pool->slab_bytes = limits->slab_bytes;
    pool->slabs = limits->slabs;
    pool->free_head = VSR_IO_INDEX_NONE;
    pool->reserve = reserve;
    pool->caller_slabs = limits->caller_slabs;
    pool->caller_taken = 0;
    pool->region_index = region_index;
    pool->group = group;
    pool->ring_entries = (uint16_t)ring_entries(limits->slabs);
    pool->ring_registered = false;
    pool->starved = false;
    pool->entries = entries;
    pool->ring_memory = memory;
    /* Pushed in reverse so that acquire and provide hand out slab 0 first. */
    for (uint32_t i = 0; i < pool->slabs; ++i) {
        free_push(pool, pool->slabs - 1 - i);
    }
}

uint32_t vsr_io_pool_locate(const struct vsr_io_pool *pool, const void *ptr)
{
    const unsigned char *p = ptr;

    if (!vsr_io_pool_contains(pool, ptr, 1)) {
        return VSR_IO_INDEX_NONE;
    }
    return (uint32_t)((size_t)(p - pool->base) / pool->slab_bytes);
}

uint32_t vsr_io_pool_acquire(struct vsr_io_pool *pool, bool caller)
{
    uint32_t id;
    struct vsr_io_slab_entry *slab;

    if (pool->free_count == 0 ||
        (caller && (pool->caller_taken == pool->caller_slabs ||
                    pool->free_count <= reserve_left(pool)))) {
        return VSR_IO_INDEX_NONE;
    }
    id = free_pop(pool);
    slab = entry(pool, id);
    slab->state = VSR_IO_SLAB_HELD;
    slab->refs = 1;
    slab->caller = caller ? 1 : 0;
    slab->internal = caller ? 0 : 1;
    if (caller) {
        pool->caller_taken++;
    } else {
        pool->internal_taken++;
    }
    update_pending(pool);
    return id;
}

void vsr_io_pool_handoff(struct vsr_io_pool *pool, uint32_t id)
{
    struct vsr_io_slab_entry *slab = entry(pool, id);

    if (slab->internal == 0) {
        return;
    }
    POOL_ASSERT(slab->state == VSR_IO_SLAB_HELD);
    POOL_ASSERT(pool->internal_taken > 0);
    slab->internal = 0;
    pool->internal_taken--;
    update_pending(pool);
}

void vsr_io_pool_retain(struct vsr_io_pool *pool, uint32_t id)
{
    struct vsr_io_slab_entry *slab = entry(pool, id);

    POOL_ASSERT(slab->state != VSR_IO_SLAB_FREE);
    POOL_ASSERT(slab->refs < UINT32_MAX);
    slab->refs++;
}

void vsr_io_pool_release(struct vsr_io_pool *pool, uint32_t id)
{
    struct vsr_io_slab_entry *slab = entry(pool, id);

    POOL_ASSERT(slab->state != VSR_IO_SLAB_FREE);
    POOL_ASSERT(slab->refs > 0);
    slab->refs--;
    /* A KERNEL slab stays in the ring; recv_end frees it later. */
    if (slab->refs == 0 && slab->state == VSR_IO_SLAB_HELD) {
        free_push(pool, id);
    }
}

bool vsr_io_pool_caller_release(struct vsr_io_pool *pool, uint32_t id)
{
    struct vsr_io_slab_entry *slab;

    if (id >= pool->slabs || pool->entries[id].caller == 0) {
        return false;
    }
    slab = entry(pool, id);
    POOL_ASSERT(slab->state == VSR_IO_SLAB_HELD);
    POOL_ASSERT(pool->caller_taken > 0);
    slab->caller = 0;
    pool->caller_taken--;
    update_pending(pool);
    vsr_io_pool_release(pool, id);
    return true;
}

uint32_t vsr_io_pool_recv_begin(struct vsr_io_pool *pool, uint16_t buffer_id,
                                uint32_t bytes)
{
    struct vsr_io_slab_entry *slab = entry(pool, buffer_id);
    uint32_t offset = slab->consumed;

    POOL_ASSERT(slab->state == VSR_IO_SLAB_KERNEL);
    POOL_ASSERT(bytes <= pool->slab_bytes - offset);
    POOL_ASSERT(slab->refs < UINT32_MAX);
    slab->consumed = offset + bytes;
    slab->refs++;
    return offset;
}

/* The kernel no longer owns the slab: it is FREE when nobody holds it,
 * HELD otherwise. */
static void leave_ring(struct vsr_io_pool *pool, uint32_t id)
{
    struct vsr_io_slab_entry *slab = entry(pool, id);

    POOL_ASSERT(slab->state == VSR_IO_SLAB_KERNEL);
    POOL_ASSERT(pool->kernel_count > 0);
    pool->kernel_count--;
    slab->consumed = 0;
    if (slab->refs == 0) {
        free_push(pool, id);
    } else {
        slab->state = VSR_IO_SLAB_HELD;
    }
}

void vsr_io_pool_recv_end(struct vsr_io_pool *pool, uint16_t buffer_id)
{
    leave_ring(pool, buffer_id);
}

uint32_t vsr_io_pool_provide(struct vsr_io_pool *pool,
                             struct vsr_io_buffer *buffers, uint32_t capacity)
{
    uint32_t count = 0;

    if (!pool->ring_registered) {
        return 0;
    }
    while (count < capacity && pool->free_count > provision_floor(pool)) {
        uint32_t id = free_pop(pool);
        struct vsr_io_slab_entry *slab = entry(pool, id);

        POOL_ASSERT(pool->kernel_count < pool->ring_entries);
        slab->state = VSR_IO_SLAB_KERNEL;
        slab->consumed = 0;
        pool->kernel_count++;
        buffers[count].base = vsr_io_pool_slab(pool, id);
        buffers[count].length = pool->slab_bytes;
        buffers[count].id = (uint16_t)id;
        buffers[count].reserved = 0;
        count++;
    }
    return count;
}

void vsr_io_pool_starved(struct vsr_io_pool *pool)
{
    pool->starved = true;
}

bool vsr_io_pool_was_starved(struct vsr_io_pool *pool)
{
    if (!pool->starved || pool->kernel_count == 0) {
        return false;
    }
    pool->starved = false;
    return true;
}

void vsr_io_pool_ring_lost(struct vsr_io_pool *pool)
{
    for (uint32_t id = 0; id < pool->slabs && pool->kernel_count > 0; ++id) {
        if (pool->entries[id].state == VSR_IO_SLAB_KERNEL) {
            leave_ring(pool, id);
        }
    }
    POOL_ASSERT(pool->kernel_count == 0);
    pool->ring_registered = false;
}
