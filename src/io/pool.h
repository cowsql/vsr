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
 * has left it AND refs are zero.
 *
 * Shares (decisions 54 and 127): the pool is split between the ring, the
 * caller (caller_slabs) and the engine's RESERVE, the slabs its own users
 * acquire (link send slabs, reassembly, stream chunk reads, cold loads,
 * recovery reads, snapshot staging). The pool counts the slabs internal
 * users acquired and still hold (internal_taken; a slab handed over to a
 * lease stops counting, see handoff), and a slab returns to the ring only
 * from FREE, and only while the free count exceeds the provision floor:
 * the part of the reserve internal users do not hold plus the part of the
 * caller's share the caller does not hold. The kernel returns a provided
 * slab only once it has filled it, so a share left to the ring would never
 * come back on an idle engine.
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
    uint8_t state;     /* enum vsr_io_slab_state */
    uint8_t internal;  /* 1 while acquired by an internal user and not
                          handed over to a lease: counts in internal_taken
                          until it is FREE again. */
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
    uint32_t kernel_count;   /* Slabs currently in the ring. */
    uint32_t reserve;        /* The engine's share: kept FREE while its
                              internal users do not hold it. */
    uint32_t internal_taken; /* Slabs internal users acquired and hold. */
    uint32_t caller_slabs;   /* The caller's share (vsr_io_limits). */
    uint32_t caller_taken;   /* Slabs the caller holds, <= caller_slabs. */
    uint32_t pending;        /* FREE slabs above the provision floor: what the
                              next prepare provides. */
    uint32_t region_index;   /* Executor buffer region of the pool. */
    uint16_t group;          /* Buffer group of the ring. */
    uint16_t ring_entries;   /* Power of two >= slabs. */
    bool ring_registered;    /* Set by the engine once buffer_ring succeeded,
                              cleared by ring_lost; provide hands out
                              nothing while clear. */
    bool starved;            /* A RECV ended with -ENOBUFS that was_starved
                              has not reported yet. */
    struct vsr_io_slab_entry *entries; /* [slabs] */
    void *ring_memory;                 /* Provided-ring memory, page aligned:
                                           16 bytes per entry. */
};

#define VSR_IO_INDEX_NONE UINT32_MAX

/* Bookkeeping bytes (entries array plus ring memory) for the limits. The
 * ring memory needs page alignment; the engine places it first. EINVAL for
 * zero slabs, a page size that is not a power of two, or slab_bytes not a
 * positive multiple of it; ELIMIT beyond 32768 slabs (16-bit ids, a
 * power-of-two ring) or on overflow. */
int vsr_io_pool_size(const struct vsr_io_limits *limits, size_t page_bytes,
                     size_t *bytes, size_t *alignment);
/* base/size is the payload region; memory the bookkeeping region. The
 * reserve (while internal users do not hold it) and the caller's share of
 * limits->caller_slabs (while the caller does not hold it) are never
 * provided; reserve + caller_slabs < slabs. Every slab starts FREE. */
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
/* Compared as integers: ptr is usually outside the pool (tail buffers, the
 * core arena, caller memory), where relational pointer comparison is
 * undefined. */
static inline bool vsr_io_pool_contains(const struct vsr_io_pool *pool,
                                        const void *ptr, size_t length)
{
    uintptr_t p = (uintptr_t)ptr;
    uintptr_t base = (uintptr_t)pool->base;

    return p >= base && length <= pool->size && p - base <= pool->size - length;
}

/* Takes a FREE slab as HELD with one reference; INDEX_NONE when none is
 * free. `caller` marks a slab taken through vsr_io_slab_acquire, which never
 * takes the part of the reserve internal users do not hold nor more than
 * the share: INDEX_NONE (ELIMIT) once caller_taken == caller_slabs or
 * free_count <= reserve - internal_taken (0 once internal users hold the
 * whole reserve). Internal users (link send slabs, reassembly, stream chunk
 * reads, cold loads, staging) may take every FREE slab and count in
 * internal_taken until the slab is FREE again or handed over. */
uint32_t vsr_io_pool_acquire(struct vsr_io_pool *pool, bool caller);
/* An internally acquired slab now belongs to a lease (a cold LOAD result, a
 * reassembled MESSAGE): it stops counting in internal_taken, so the reserve
 * is again available to the internal users it is sized for, while the
 * lease keeps the slab HELD like a received one. No-op for any other
 * slab. */
void vsr_io_pool_handoff(struct vsr_io_pool *pool, uint32_t id);
/* The provision floor: FREE slabs provision leaves, the reserve's part
 * internal users do not hold plus the caller's untaken share. */
uint32_t vsr_io_pool_floor(const struct vsr_io_pool *pool);
void vsr_io_pool_retain(struct vsr_io_pool *pool, uint32_t id);
/* Drops one reference; a slab reaching zero outside the ring becomes FREE
 * and is queued for provision. Never the caller's own reference. */
void vsr_io_pool_release(struct vsr_io_pool *pool, uint32_t id);
/* The caller's release (vsr_io_slab_release): false, changing nothing,
 * unless id names a slab the caller holds; else clears `caller`, returns
 * the share, and drops the caller's reference. Holders that retained the
 * slab (a send in flight) keep it HELD until they release it. */
bool vsr_io_pool_caller_release(struct vsr_io_pool *pool, uint32_t id);

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
 * hand to the executor's provide(), moving them FREE -> KERNEL, never below
 * the provision floor and nothing while the ring is not registered.
 * `starved` records that a RECV completed with -ENOBUFS, so the link module
 * re-arms receives once the ring holds a buffer again.
 */
uint32_t vsr_io_pool_provide(struct vsr_io_pool *pool,
                             struct vsr_io_buffer *buffers, uint32_t capacity);
void vsr_io_pool_starved(struct vsr_io_pool *pool);
/* True once per starvation, as soon as the ring holds a buffer again; it
 * clears the flag then, so receives are not re-armed into an empty ring. */
bool vsr_io_pool_was_starved(struct vsr_io_pool *pool);
/* Ring torn down (close or crash): every KERNEL slab is back to the pool.
 * Starvation is kept: a receive that ended with -ENOBUFS is still to be
 * re-armed if a ring is registered and provided again. */
void vsr_io_pool_ring_lost(struct vsr_io_pool *pool);

#endif /* VSR_IO_POOL_H */
