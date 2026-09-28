#ifndef VSR_IO_POOL_H
#define VSR_IO_POOL_H

#include "vsr-io.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/*
 * Payload pool: fixed-size slabs of the registered payload region, their
 * reference counts and the provided-buffer ring bookkeeping
 * (docs/io-implementation.md, "Pool"). A pure planner: it decides which
 * slabs to hand to the kernel and records what completions report; the
 * engine turns its decisions into executor calls.
 *
 * Slab states:
 *   FREE    on the free list, referenced by nobody
 *   KERNEL  provided to the buffer ring; the kernel owns it until a RECV
 *           completion with BUFFER_MORE clear names it, or the ring is torn
 *           down
 *   HELD    referenced by refs > 0 holders: message leases, LOAD leases,
 *           in-flight send vectors, stream DATA ops, cold-load reads, the
 *           caller (vsr_io_slab_acquire), link send slabs, capture staging
 * A KERNEL slab with incremental consumption also carries refs for the
 * frames already delivered from it; it becomes FREE only when the kernel
 * has left it AND refs are zero. A slab returns to the ring only from FREE,
 * and only while the free count exceeds the reserve, so that cold loads,
 * reassembly and staging can always obtain one eventually.
 *
 * Ids are the provided-buffer ids (0..slabs-1) and double as slab indexes;
 * the pool region is registered as ONE region with the executor, so a
 * FIXED_BUFFER record for any slab names region_index and addresses the
 * bytes directly.
 */

enum vsr_io_slab_state {
    VSR_IO_SLAB_FREE,
    VSR_IO_SLAB_KERNEL,
    VSR_IO_SLAB_HELD
};

struct vsr_io_slab_entry {
    uint32_t refs;
    uint32_t consumed; /* KERNEL: bytes delivered so far by RECV CQEs. */
    uint16_t state;    /* enum vsr_io_slab_state */
    uint16_t caller;   /* 1 while taken by vsr_io_slab_acquire. */
    uint32_t next;     /* Free-list link; VSR_IO_INDEX_NONE ends it. */
};

struct vsr_io_pool {
    unsigned char *base; /* Payload region, page aligned. */
    size_t size;
    uint32_t slab_bytes;
    uint32_t slabs;
    uint32_t free_head;
    uint32_t free_count;
    uint32_t kernel_count; /* Slabs currently in the ring. */
    uint32_t reserve;      /* Free slabs never provided. */
    uint32_t pending;      /* FREE slabs the next prepare provides. */
    uint32_t region_index; /* Executor buffer region of the pool. */
    uint16_t group;        /* Buffer group of the ring. */
    uint16_t ring_entries; /* Power of two >= slabs. */
    bool ring_registered;
    struct vsr_io_slab_entry *entries; /* [slabs] */
    void *ring_memory;                 /* Provided-ring memory, page aligned:
                                           16 bytes per entry. */
};

#define VSR_IO_INDEX_NONE UINT32_MAX

/* Bookkeeping bytes (entries array plus ring memory) for the limits. The
 * ring memory needs page alignment; the engine places it first. */
int vsr_io_pool_size(const struct vsr_io_limits *limits, size_t page_bytes,
                     size_t *bytes, size_t *alignment);
/* base/size is the payload region; memory the bookkeeping region. reserve
 * slabs are never provided. Every slab starts FREE. */
void vsr_io_pool_init(struct vsr_io_pool *pool, void *base, size_t size,
                      const struct vsr_io_limits *limits, uint32_t reserve,
                      uint32_t region_index, uint16_t group, void *memory,
                      size_t memory_size);

static inline unsigned char *vsr_io_pool_slab(const struct vsr_io_pool *pool,
                                              uint32_t id)
{
    return pool->base + (size_t)id * pool->slab_bytes;
}

/* Slab index of a pointer inside the pool, or INDEX_NONE. */
uint32_t vsr_io_pool_locate(const struct vsr_io_pool *pool, const void *ptr);
static inline bool vsr_io_pool_contains(const struct vsr_io_pool *pool,
                                        const void *ptr, size_t length)
{
    const unsigned char *p = ptr;
    return p >= pool->base && length <= pool->size &&
           p <= pool->base + pool->size - length;
}

/* Takes a FREE slab as HELD with one reference; INDEX_NONE when none is
 * free. `caller` marks a slab taken through vsr_io_slab_acquire. */
uint32_t vsr_io_pool_acquire(struct vsr_io_pool *pool, bool caller);
void vsr_io_pool_retain(struct vsr_io_pool *pool, uint32_t id);
/* Drops one reference; a slab reaching zero outside the ring becomes FREE
 * and is queued for provision. */
void vsr_io_pool_release(struct vsr_io_pool *pool, uint32_t id);

/*
 * Receive bookkeeping. A RECV completion with BUFFER set names buffer_id
 * and delivered `bytes` at the slab's current consumed offset; recv_begin
 * returns that offset, advances it, and takes one reference for the frames
 * the link will carve from those bytes (the link releases it when it has
 * leased or discarded every byte). recv_end records that the kernel left
 * the slab (BUFFER_MORE clear); the slab returns to FREE once refs drop.
 */
uint32_t vsr_io_pool_recv_begin(struct vsr_io_pool *pool, uint16_t buffer_id,
                                uint32_t bytes);
void vsr_io_pool_recv_end(struct vsr_io_pool *pool, uint16_t buffer_id);

/*
 * Provision planning for vsr_io_prepare: fills up to capacity buffers to
 * hand to the executor's provide(), moving them FREE -> KERNEL, respecting
 * the reserve. `starved` reports that a RECV completed with -ENOBUFS since
 * the last call, so the link module re-arms receives after providing.
 */
uint32_t vsr_io_pool_provide(struct vsr_io_pool *pool,
                             struct vsr_io_buffer *buffers, uint32_t capacity);
void vsr_io_pool_starved(struct vsr_io_pool *pool);
bool vsr_io_pool_was_starved(struct vsr_io_pool *pool); /* Clears it. */
/* Ring torn down (close or crash): every KERNEL slab is back to the pool. */
void vsr_io_pool_ring_lost(struct vsr_io_pool *pool);

#endif /* VSR_IO_POOL_H */
