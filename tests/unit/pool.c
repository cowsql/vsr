#include "config.h"

#include "io/pool.h"
#include "lib/check.h"
#include "lib/random.h"

#include <inttypes.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PAGE 4096u
#define MAX_SLABS 16u
#define MAX_HOLDERS 4096u

static _Alignas(4096) unsigned char payload[MAX_SLABS * PAGE];
static _Alignas(4096) unsigned char bookkeeping[2 * PAGE];

static unsigned char pattern(size_t i)
{
    return (unsigned char)(i * 31u + 7u);
}

static void fill_payload(void)
{
    for (size_t i = 0; i < sizeof(payload); ++i) {
        payload[i] = pattern(i);
    }
}

/* The pool never touches slab bytes: every byte keeps its pattern. */
static void check_payload(void)
{
    for (size_t i = 0; i < sizeof(payload); ++i) {
        CHECK(payload[i] == pattern(i));
    }
}

static struct vsr_io_limits limits_for(uint32_t slabs, uint32_t slab_bytes)
{
    struct vsr_io_limits limits;

    memset(&limits, 0, sizeof(limits));
    limits.slabs = slabs;
    limits.slab_bytes = slab_bytes;
    return limits;
}

static void setup_share(struct vsr_io_pool *pool, uint32_t slabs,
                        uint32_t reserve, uint32_t caller_slabs)
{
    struct vsr_io_limits limits = limits_for(slabs, PAGE);
    size_t bytes = 0;
    size_t alignment = 0;

    limits.caller_slabs = caller_slabs;
    CHECK(vsr_io_pool_size(&limits, PAGE, &bytes, &alignment) == VSR_OK);
    CHECK(bytes <= sizeof(bookkeeping));
    CHECK(alignment == PAGE);
    fill_payload();
    vsr_io_pool_init(pool, payload, sizeof(payload), &limits, reserve, 3, 7,
                     bookkeeping, bytes);
    CHECK(pool->caller_slabs == caller_slabs && pool->caller_taken == 0);
}

/* No caller share: the caller cannot acquire. */
static void setup(struct vsr_io_pool *pool, uint32_t slabs, uint32_t reserve)
{
    setup_share(pool, slabs, reserve, 0);
}

/* Recounts the pool from its entries: states agree with the counters, the
 * free list holds exactly the FREE slabs, the caller's holds are HELD and
 * counted, and every state is consistent. */
static void check_pool(const struct vsr_io_pool *pool)
{
    uint32_t free = 0;
    uint32_t kernel = 0;
    uint32_t held = 0;
    uint32_t listed = 0;
    uint32_t lent = 0;
    uint32_t floor;
    bool seen[MAX_SLABS];

    memset(seen, 0, sizeof(seen));
    CHECK(pool->slabs <= MAX_SLABS);
    for (uint32_t id = 0; id < pool->slabs; ++id) {
        const struct vsr_io_slab_entry *slab = &pool->entries[id];

        switch (slab->state) {
        case VSR_IO_SLAB_FREE:
            CHECK(slab->refs == 0);
            CHECK(slab->consumed == 0);
            CHECK(slab->caller == 0);
            free++;
            break;
        case VSR_IO_SLAB_KERNEL:
            CHECK(slab->consumed <= pool->slab_bytes);
            CHECK(slab->caller == 0);
            kernel++;
            break;
        case VSR_IO_SLAB_HELD:
            CHECK(slab->refs > 0);
            CHECK(slab->consumed == 0);
            CHECK(slab->caller <= 1);
            lent += slab->caller;
            held++;
            break;
        default:
            CHECK(false);
        }
    }
    CHECK(free == pool->free_count);
    CHECK(kernel == pool->kernel_count);
    CHECK(free + kernel + held == pool->slabs);
    CHECK(kernel <= pool->ring_entries);
    for (uint32_t id = pool->free_head; id != VSR_IO_INDEX_NONE;
         id = pool->entries[id].next) {
        CHECK(id < pool->slabs);
        CHECK(!seen[id]);
        CHECK(pool->entries[id].state == VSR_IO_SLAB_FREE);
        seen[id] = true;
        listed++;
    }
    CHECK(listed == pool->free_count);
    CHECK(lent == pool->caller_taken);
    CHECK(lent <= pool->caller_slabs);
    floor = pool->reserve + pool->caller_slabs - lent;
    CHECK(pool->pending == (free > floor ? free - floor : 0));
}

static void test_layout(void)
{
    struct vsr_io_pool pool;
    struct vsr_io_limits limits;
    size_t bytes = 0;
    size_t alignment = 0;
    const uint32_t counts[] = {1, 2, 3, 5, 8, 9, 16};

    for (size_t i = 0; i < sizeof(counts) / sizeof(counts[0]); ++i) {
        uint32_t slabs = counts[i];
        uint32_t entries;

        limits = limits_for(slabs, PAGE);
        CHECK(vsr_io_pool_size(&limits, PAGE, &bytes, &alignment) == VSR_OK);
        CHECK(alignment == PAGE);
        vsr_io_pool_init(&pool, payload, sizeof(payload), &limits, 0, 1, 2,
                         bookkeeping, bytes);
        check_pool(&pool);
        entries = pool.ring_entries;
        CHECK(entries >= slabs && entries < 2 * slabs + 1);
        CHECK((entries & (entries - 1)) == 0);
        CHECK(pool.ring_memory == bookkeeping);
        CHECK((unsigned char *)pool.entries >=
              bookkeeping + (size_t)entries * 16u);
        CHECK((unsigned char *)(pool.entries + slabs) <= bookkeeping + bytes);
        CHECK(pool.slabs == slabs && pool.free_count == slabs);
        CHECK(pool.size == (size_t)slabs * PAGE);
        CHECK(pool.region_index == 1 && pool.group == 2);
        CHECK(!pool.ring_registered && !pool.starved);
    }

    /* The largest ring: 32768 16-bit ids. */
    limits = limits_for(32768, PAGE);
    CHECK(vsr_io_pool_size(&limits, PAGE, &bytes, &alignment) == VSR_OK);
    CHECK(bytes >= (size_t)32768 * (16 + sizeof(struct vsr_io_slab_entry)));
    limits = limits_for(2, 2 * PAGE);
    CHECK(vsr_io_pool_size(&limits, PAGE, &bytes, &alignment) == VSR_OK);

    limits = limits_for(32769, PAGE);
    CHECK(vsr_io_pool_size(&limits, PAGE, &bytes, &alignment) == VSR_ELIMIT);
    limits = limits_for(UINT32_MAX, PAGE);
    CHECK(vsr_io_pool_size(&limits, PAGE, &bytes, &alignment) == VSR_ELIMIT);

    limits = limits_for(0, PAGE);
    CHECK(vsr_io_pool_size(&limits, PAGE, &bytes, &alignment) == VSR_EINVAL);
    limits = limits_for(4, 0);
    CHECK(vsr_io_pool_size(&limits, PAGE, &bytes, &alignment) == VSR_EINVAL);
    limits = limits_for(4, PAGE + 512);
    CHECK(vsr_io_pool_size(&limits, PAGE, &bytes, &alignment) == VSR_EINVAL);
    limits = limits_for(4, PAGE);
    CHECK(vsr_io_pool_size(&limits, 0, &bytes, &alignment) == VSR_EINVAL);
    CHECK(vsr_io_pool_size(&limits, 3000, &bytes, &alignment) == VSR_EINVAL);
    CHECK(vsr_io_pool_size(NULL, PAGE, &bytes, &alignment) == VSR_EINVAL);
    CHECK(vsr_io_pool_size(&limits, PAGE, NULL, &alignment) == VSR_EINVAL);
    CHECK(vsr_io_pool_size(&limits, PAGE, &bytes, NULL) == VSR_EINVAL);
}

static void test_locate_contains(void)
{
    struct vsr_io_pool pool;
    unsigned char *base = payload;
    const size_t page = PAGE;

    setup(&pool, 4, 0);
    CHECK(vsr_io_pool_slab(&pool, 0) == base);
    CHECK(vsr_io_pool_slab(&pool, 3) == base + 3 * page);
    CHECK(vsr_io_pool_locate(&pool, base) == 0);
    CHECK(vsr_io_pool_locate(&pool, base + page - 1) == 0);
    CHECK(vsr_io_pool_locate(&pool, base + page) == 1);
    CHECK(vsr_io_pool_locate(&pool, base + 4 * page - 1) == 3);
    CHECK(vsr_io_pool_locate(&pool, base + 4 * page) == VSR_IO_INDEX_NONE);
    CHECK(vsr_io_pool_locate(&pool, bookkeeping) == VSR_IO_INDEX_NONE);
    CHECK(vsr_io_pool_contains(&pool, base, 4 * page));
    CHECK(vsr_io_pool_contains(&pool, base + 2 * page + 5, 100));
    CHECK(vsr_io_pool_contains(&pool, base + 4 * page, 0));
    CHECK(!vsr_io_pool_contains(&pool, base + 1, 4 * page));
    CHECK(!vsr_io_pool_contains(&pool, base + 2 * page, 2 * page + 1));
    CHECK(!vsr_io_pool_contains(&pool, base, 4 * page + 1));
    CHECK(!vsr_io_pool_contains(&pool, base + 4 * page, 1));
    CHECK(!vsr_io_pool_contains(&pool, bookkeeping, 1));
    check_payload();
}

/* Provision hands out FREE slabs as KERNEL, never the reserve, and nothing
 * before the ring is registered. */
static void test_provide_reserve(void)
{
    struct vsr_io_pool pool;
    struct vsr_io_buffer buffers[MAX_SLABS];
    bool seen[MAX_SLABS];

    memset(seen, 0, sizeof(seen));
    setup(&pool, 8, 3);
    CHECK(pool.pending == 5);
    CHECK(vsr_io_pool_provide(&pool, buffers, MAX_SLABS) == 0);
    CHECK(pool.free_count == 8 && pool.kernel_count == 0);
    pool.ring_registered = true;

    CHECK(vsr_io_pool_provide(&pool, buffers, 0) == 0);
    CHECK(vsr_io_pool_provide(&pool, buffers, 2) == 2);
    check_pool(&pool);
    CHECK(pool.kernel_count == 2 && pool.free_count == 6 && pool.pending == 3);
    CHECK(vsr_io_pool_provide(&pool, buffers + 2, MAX_SLABS) == 3);
    check_pool(&pool);
    for (uint32_t i = 0; i < 5; ++i) {
        uint16_t id = buffers[i].id;

        CHECK(id < 8 && !seen[id]);
        seen[id] = true;
        CHECK(buffers[i].base == vsr_io_pool_slab(&pool, id));
        CHECK(buffers[i].length == PAGE);
        CHECK(pool.entries[id].state == VSR_IO_SLAB_KERNEL);
        CHECK(pool.entries[id].consumed == 0 && pool.entries[id].refs == 0);
    }
    CHECK(pool.free_count == 3 && pool.pending == 0);
    CHECK(vsr_io_pool_provide(&pool, buffers, MAX_SLABS) == 0);
    check_pool(&pool);
    check_payload();
}

/* Many frames per slab: each delivery lands after the previous ones, holds
 * its own reference, and the slab stays in the ring until the delivery with
 * BUFFER_MORE clear. */
static void test_incremental(void)
{
    struct vsr_io_pool pool;
    struct vsr_io_buffer buffer;
    const uint32_t sizes[] = {100, 24, 1000, 72, 150};
    uint32_t expected = 0;
    uint16_t id;

    setup(&pool, 4, 1);
    pool.ring_registered = true;
    CHECK(vsr_io_pool_provide(&pool, &buffer, 1) == 1);
    id = buffer.id;
    for (uint32_t i = 0; i < sizeof(sizes) / sizeof(sizes[0]); ++i) {
        CHECK(vsr_io_pool_recv_begin(&pool, id, sizes[i]) == expected);
        expected += sizes[i];
        CHECK(pool.entries[id].consumed == expected);
        CHECK(pool.entries[id].state == VSR_IO_SLAB_KERNEL);
        CHECK(pool.entries[id].refs == i + 1);
        check_pool(&pool);
    }
    /* The last delivery carried three frames: three leases, and the
     * delivery reference goes once they are carved. */
    for (int i = 0; i < 3; ++i) {
        vsr_io_pool_retain(&pool, id);
    }
    vsr_io_pool_release(&pool, id);
    /* Every earlier delivery was consumed or discarded: the slab stays
     * KERNEL with only the three leases. */
    for (int i = 0; i < 4; ++i) {
        vsr_io_pool_release(&pool, id);
    }
    CHECK(pool.entries[id].state == VSR_IO_SLAB_KERNEL);
    CHECK(pool.entries[id].refs == 3);
    check_pool(&pool);

    /* The final delivery fills the slab and the kernel leaves it. */
    CHECK(vsr_io_pool_recv_begin(&pool, id, PAGE - expected) == expected);
    vsr_io_pool_recv_end(&pool, id);
    CHECK(pool.entries[id].state == VSR_IO_SLAB_HELD);
    CHECK(pool.entries[id].refs == 4);
    CHECK(pool.kernel_count == 0);
    check_pool(&pool);
    for (int i = 0; i < 3; ++i) {
        vsr_io_pool_release(&pool, id);
        CHECK(pool.entries[id].state == VSR_IO_SLAB_HELD);
    }
    vsr_io_pool_release(&pool, id);
    CHECK(pool.entries[id].state == VSR_IO_SLAB_FREE);
    CHECK(pool.free_count == 4 && pool.pending == 3);
    check_pool(&pool);

    /* Provided again, it is consumed from its start. */
    CHECK(vsr_io_pool_provide(&pool, &buffer, 1) == 1);
    CHECK(vsr_io_pool_recv_begin(&pool, buffer.id, 10) == 0);
    vsr_io_pool_release(&pool, buffer.id);
    check_pool(&pool);
    check_payload();
}

/* A slab whose references all dropped in the ring is FREE as soon as the
 * kernel leaves it; one with a lease is HELD until the lease goes. */
static void test_kernel_leaves(void)
{
    struct vsr_io_pool pool;
    struct vsr_io_buffer buffers[2];
    uint16_t a;
    uint16_t b;

    setup(&pool, 3, 1);
    pool.ring_registered = true;
    CHECK(vsr_io_pool_provide(&pool, buffers, 2) == 2);
    a = buffers[0].id;
    b = buffers[1].id;

    CHECK(vsr_io_pool_recv_begin(&pool, a, 512) == 0);
    vsr_io_pool_release(&pool, a); /* Discarded. */
    CHECK(vsr_io_pool_recv_begin(&pool, a, PAGE - 512) == 512);
    vsr_io_pool_release(&pool, a);
    CHECK(pool.entries[a].state == VSR_IO_SLAB_KERNEL);
    vsr_io_pool_recv_end(&pool, a);
    CHECK(pool.entries[a].state == VSR_IO_SLAB_FREE);
    check_pool(&pool);

    /* A ring without incremental consumption: one delivery per buffer. */
    CHECK(vsr_io_pool_recv_begin(&pool, b, 300) == 0);
    vsr_io_pool_retain(&pool, b); /* MESSAGE lease. */
    vsr_io_pool_recv_end(&pool, b);
    CHECK(pool.entries[b].state == VSR_IO_SLAB_HELD);
    CHECK(pool.entries[b].refs == 2);
    CHECK(pool.entries[b].consumed == 0);
    vsr_io_pool_release(&pool, b);
    CHECK(pool.entries[b].state == VSR_IO_SLAB_HELD);
    check_pool(&pool);
    vsr_io_pool_release(&pool, b);
    CHECK(pool.entries[b].state == VSR_IO_SLAB_FREE);
    CHECK(pool.free_count == 3);
    check_pool(&pool);
    check_payload();
}

/* Callers never take the reserve; internal users may. A KERNEL slab is
 * never acquired. The caller's share (4) exceeds what the reserve lets it
 * take, so the reserve is what stops it here. */
static void test_acquire_reserve(void)
{
    struct vsr_io_pool pool;
    struct vsr_io_buffer buffers[MAX_SLABS];
    uint32_t taken[5];
    uint32_t id;

    setup_share(&pool, 5, 2, 4);
    for (int i = 0; i < 3; ++i) {
        taken[i] = vsr_io_pool_acquire(&pool, true);
        CHECK(taken[i] != VSR_IO_INDEX_NONE);
        CHECK(pool.entries[taken[i]].state == VSR_IO_SLAB_HELD);
        CHECK(pool.entries[taken[i]].refs == 1);
        CHECK(pool.entries[taken[i]].caller == 1);
        check_pool(&pool);
    }
    CHECK(pool.free_count == 2 && pool.caller_taken == 3);
    CHECK(vsr_io_pool_acquire(&pool, true) == VSR_IO_INDEX_NONE);
    for (int i = 3; i < 5; ++i) {
        taken[i] = vsr_io_pool_acquire(&pool, false);
        CHECK(taken[i] != VSR_IO_INDEX_NONE);
        CHECK(pool.entries[taken[i]].caller == 0);
    }
    CHECK(vsr_io_pool_acquire(&pool, false) == VSR_IO_INDEX_NONE);
    CHECK(vsr_io_pool_acquire(&pool, true) == VSR_IO_INDEX_NONE);
    check_pool(&pool);
    /* A send in flight retains the caller's slab past its release. */
    vsr_io_pool_retain(&pool, taken[0]);
    CHECK(vsr_io_pool_caller_release(&pool, taken[0]));
    CHECK(pool.entries[taken[0]].state == VSR_IO_SLAB_HELD);
    CHECK(pool.entries[taken[0]].caller == 0 && pool.caller_taken == 2);
    check_pool(&pool);
    vsr_io_pool_release(&pool, taken[0]);
    CHECK(pool.entries[taken[0]].state == VSR_IO_SLAB_FREE);
    for (int i = 1; i < 3; ++i) {
        CHECK(vsr_io_pool_caller_release(&pool, taken[i]));
    }
    for (int i = 3; i < 5; ++i) {
        vsr_io_pool_release(&pool, taken[i]);
    }
    CHECK(pool.free_count == 5 && pool.caller_taken == 0);
    check_pool(&pool);

    /* Everything above the reserve in the ring (no caller share, so that
     * provision may go down to the reserve): only internal users get the
     * FREE reserve, never a KERNEL slab. */
    setup(&pool, 5, 2);
    pool.ring_registered = true;
    CHECK(vsr_io_pool_provide(&pool, buffers, MAX_SLABS) == 3);
    CHECK(vsr_io_pool_acquire(&pool, true) == VSR_IO_INDEX_NONE);
    for (int i = 0; i < 2; ++i) {
        taken[i] = vsr_io_pool_acquire(&pool, false);
        CHECK(taken[i] != VSR_IO_INDEX_NONE);
        CHECK(pool.entries[taken[i]].state == VSR_IO_SLAB_HELD);
        for (int k = 0; k < 3; ++k) {
            CHECK(taken[i] != buffers[k].id);
        }
    }
    CHECK(vsr_io_pool_acquire(&pool, false) == VSR_IO_INDEX_NONE);
    /* The kernel returns an unreferenced slab: FREE, but within the
     * reserve, so only an internal user takes it. */
    vsr_io_pool_recv_end(&pool, buffers[1].id);
    check_pool(&pool);
    CHECK(vsr_io_pool_acquire(&pool, true) == VSR_IO_INDEX_NONE);
    id = vsr_io_pool_acquire(&pool, false);
    CHECK(id == buffers[1].id);
    check_pool(&pool);
    check_payload();
}

static void test_starved(void)
{
    struct vsr_io_pool pool;
    struct vsr_io_buffer buffers[MAX_SLABS];
    uint32_t held[3];

    setup(&pool, 3, 1);
    pool.ring_registered = true;
    CHECK(!vsr_io_pool_was_starved(&pool));

    /* Every slab held: the ring is empty, the flag waits for a buffer. */
    for (int i = 0; i < 3; ++i) {
        held[i] = vsr_io_pool_acquire(&pool, false);
    }
    vsr_io_pool_starved(&pool);
    CHECK(vsr_io_pool_provide(&pool, buffers, MAX_SLABS) == 0);
    CHECK(!vsr_io_pool_was_starved(&pool));
    CHECK(pool.starved);
    for (int i = 0; i < 3; ++i) {
        vsr_io_pool_release(&pool, held[i]);
    }
    CHECK(!vsr_io_pool_was_starved(&pool));
    CHECK(vsr_io_pool_provide(&pool, buffers, MAX_SLABS) == 2);
    CHECK(vsr_io_pool_was_starved(&pool));
    CHECK(!pool.starved);
    CHECK(!vsr_io_pool_was_starved(&pool));

    /* Starved while the ring still holds a buffer (a receive raced the
     * provision): reported at once. */
    vsr_io_pool_starved(&pool);
    vsr_io_pool_starved(&pool);
    CHECK(vsr_io_pool_was_starved(&pool));
    CHECK(!vsr_io_pool_was_starved(&pool));
    check_pool(&pool);
    check_payload();
}

static void test_ring_lost(void)
{
    struct vsr_io_pool pool;
    struct vsr_io_buffer buffers[MAX_SLABS];
    uint16_t a;

    setup(&pool, 6, 1);
    pool.ring_registered = true;
    CHECK(vsr_io_pool_provide(&pool, buffers, MAX_SLABS) == 5);
    a = buffers[0].id;
    CHECK(vsr_io_pool_recv_begin(&pool, a, 64) == 0);
    vsr_io_pool_starved(&pool);
    vsr_io_pool_ring_lost(&pool);
    check_pool(&pool);
    CHECK(pool.kernel_count == 0 && pool.free_count == 5);
    CHECK(pool.entries[a].state == VSR_IO_SLAB_HELD);
    CHECK(pool.entries[a].refs == 1 && pool.entries[a].consumed == 0);
    /* Starvation outlives the ring, but is not reported without one. */
    CHECK(!pool.ring_registered && pool.starved);
    CHECK(!vsr_io_pool_was_starved(&pool));
    CHECK(vsr_io_pool_provide(&pool, buffers, MAX_SLABS) == 0);
    vsr_io_pool_release(&pool, a);
    CHECK(pool.free_count == 6);
    check_pool(&pool);

    /* A new ring starts from scratch; the starved receive is re-armed. */
    pool.ring_registered = true;
    CHECK(vsr_io_pool_provide(&pool, buffers, MAX_SLABS) == 5);
    CHECK(vsr_io_pool_was_starved(&pool));
    CHECK(vsr_io_pool_recv_begin(&pool, buffers[4].id, 8) == 0);
    vsr_io_pool_release(&pool, buffers[4].id);
    vsr_io_pool_ring_lost(&pool);
    CHECK(pool.free_count == 6);
    check_pool(&pool);
    check_payload();
}

/*
 * Randomized holder model: provisions, incremental deliveries with frames
 * leased from them, retains and releases by random holders, acquires by
 * callers and internal users, caller releases (valid or not) of slabs that
 * sends may still retain, starvation and ring loss, all interleaved. The
 * model predicts every state, reference count, consumed offset and caller
 * flag independently of the pool; the invariant is checked after every
 * step, and at the end every reference taken has been dropped and every
 * slab is FREE.
 */
struct model {
    uint32_t slabs;
    uint32_t reserve;
    uint32_t caller_slabs;
    uint32_t lent[MAX_SLABS]; /* Slabs the caller holds, one ref each. */
    uint32_t lent_count;
    uint32_t refs[MAX_SLABS];
    uint32_t consumed[MAX_SLABS];
    uint16_t state[MAX_SLABS];
    uint16_t ring[MAX_SLABS]; /* Provided ids, in the kernel's order. */
    uint32_t ring_head;
    uint32_t ring_count;
    uint32_t holders[MAX_HOLDERS]; /* One slab id per reference. */
    uint32_t holder_count;
    bool registered;
    bool starved;
    uint64_t taken;
    uint64_t dropped;
};

static uint32_t model_count(const struct model *m, uint16_t state)
{
    uint32_t count = 0;

    for (uint32_t id = 0; id < m->slabs; ++id) {
        count += m->state[id] == state ? 1u : 0u;
    }
    return count;
}

static bool model_lent(const struct model *m, uint32_t id)
{
    for (uint32_t i = 0; i < m->lent_count; ++i) {
        if (m->lent[i] == id) {
            return true;
        }
    }
    return false;
}

static void model_check(const struct model *m, const struct vsr_io_pool *pool)
{
    check_pool(pool);
    for (uint32_t id = 0; id < m->slabs; ++id) {
        CHECK(pool->entries[id].state == m->state[id]);
        CHECK(pool->entries[id].refs == m->refs[id]);
        CHECK(pool->entries[id].consumed == m->consumed[id]);
        CHECK(pool->entries[id].caller == (model_lent(m, id) ? 1 : 0));
    }
    CHECK(pool->caller_slabs == m->caller_slabs);
    CHECK(pool->caller_taken == m->lent_count);
    CHECK(pool->ring_registered == m->registered);
    CHECK(pool->starved == m->starved);
    CHECK(m->taken - m->dropped == m->holder_count + m->lent_count);
}

static void model_take(struct model *m, uint32_t id)
{
    CHECK(m->holder_count < MAX_HOLDERS);
    m->holders[m->holder_count++] = id;
    m->refs[id]++;
    m->taken++;
}

static void model_unref(struct model *m, uint32_t id)
{
    m->refs[id]--;
    m->dropped++;
    if (m->refs[id] == 0 && m->state[id] == VSR_IO_SLAB_HELD) {
        m->state[id] = VSR_IO_SLAB_FREE;
    }
}

static void model_drop(struct model *m, struct vsr_io_pool *pool,
                       uint32_t holder)
{
    uint32_t id = m->holders[holder];

    m->holders[holder] = m->holders[--m->holder_count];
    vsr_io_pool_release(pool, id);
    model_unref(m, id);
}

/* The caller's release of its i-th slab. */
static void model_give_back(struct model *m, struct vsr_io_pool *pool,
                            uint32_t i)
{
    uint32_t id;

    CHECK(i < m->lent_count);
    id = m->lent[i];
    m->lent[i] = m->lent[--m->lent_count];
    CHECK(vsr_io_pool_caller_release(pool, id));
    model_unref(m, id);
}

static void model_leave(struct model *m, uint32_t id)
{
    m->consumed[id] = 0;
    m->state[id] = m->refs[id] > 0 ? VSR_IO_SLAB_HELD : VSR_IO_SLAB_FREE;
}

static void step_provide(struct model *m, struct vsr_io_pool *pool,
                         struct test_random *random)
{
    struct vsr_io_buffer buffers[MAX_SLABS];
    uint32_t capacity = test_random_bounded(random, 5);
    uint32_t free = model_count(m, VSR_IO_SLAB_FREE);
    uint32_t floor = m->reserve + m->caller_slabs - m->lent_count;
    uint32_t spare = free > floor ? free - floor : 0;
    uint32_t expected =
        m->registered ? (capacity < spare ? capacity : spare) : 0;
    uint32_t count = vsr_io_pool_provide(pool, buffers, capacity);

    CHECK(count == expected);
    for (uint32_t i = 0; i < count; ++i) {
        uint16_t id = buffers[i].id;

        CHECK(id < m->slabs && m->state[id] == VSR_IO_SLAB_FREE);
        CHECK(buffers[i].base == payload + (size_t)id * PAGE);
        CHECK(buffers[i].length == PAGE);
        m->state[id] = VSR_IO_SLAB_KERNEL;
        m->ring[(m->ring_head + m->ring_count++) % MAX_SLABS] = id;
    }
}

static void step_deliver(struct model *m, struct vsr_io_pool *pool,
                         struct test_random *random)
{
    uint16_t id;
    uint32_t remaining;
    uint32_t bytes;
    uint32_t frames;
    bool more;

    if (m->ring_count == 0) {
        /* A multishot RECV found the ring empty, or it was unregistered
         * under it (decision 58): -ENOBUFS. */
        vsr_io_pool_starved(pool);
        m->starved = true;
        return;
    }
    id = m->ring[m->ring_head];
    remaining = PAGE - m->consumed[id];
    bytes = 1 + test_random_bounded(random, remaining);
    more = bytes < remaining && test_random_bounded(random, 4) != 0;
    CHECK(vsr_io_pool_recv_begin(pool, id, bytes) == m->consumed[id]);
    m->consumed[id] += bytes;
    model_take(m, id);
    /* The link leases up to three frames from the delivery, then drops
     * the delivery's reference or keeps it for a partial frame. */
    frames = test_random_bounded(random, 4);
    for (uint32_t i = 0; i < frames; ++i) {
        vsr_io_pool_retain(pool, id);
        model_take(m, id);
    }
    if (test_random_bounded(random, 2) == 0) {
        model_drop(m, pool, m->holder_count - 1 - frames);
    }
    if (!more) {
        vsr_io_pool_recv_end(pool, id);
        model_leave(m, id);
        m->ring_head = (m->ring_head + 1) % MAX_SLABS;
        m->ring_count--;
    }
}

static void step_acquire(struct model *m, struct vsr_io_pool *pool,
                         struct test_random *random)
{
    bool caller = test_random_bounded(random, 2) == 0;
    uint32_t free = model_count(m, VSR_IO_SLAB_FREE);
    bool expected = caller
                        ? m->lent_count < m->caller_slabs && free > m->reserve
                        : free > 0;
    uint32_t id = vsr_io_pool_acquire(pool, caller);

    if (!expected) {
        CHECK(id == VSR_IO_INDEX_NONE);
        return;
    }
    CHECK(id < m->slabs && m->state[id] == VSR_IO_SLAB_FREE);
    CHECK(pool->entries[id].caller == (caller ? 1 : 0));
    m->state[id] = VSR_IO_SLAB_HELD;
    if (caller) {
        m->lent[m->lent_count++] = id;
        m->refs[id]++;
        m->taken++;
    } else {
        model_take(m, id);
    }
}

/* The caller gives a slab back, or names one it does not hold (any state,
 * or past the pool), which changes nothing. */
static void step_caller_release(struct model *m, struct vsr_io_pool *pool,
                                struct test_random *random)
{
    uint32_t id;

    if (m->lent_count > 0 && test_random_bounded(random, 4) != 0) {
        model_give_back(m, pool, test_random_bounded(random, m->lent_count));
        return;
    }
    id = test_random_bounded(random, m->slabs + 2);
    if (!model_lent(m, id)) {
        CHECK(!vsr_io_pool_caller_release(pool, id));
    }
}

static void step_ring(struct model *m, struct vsr_io_pool *pool,
                      struct test_random *random)
{
    uint32_t choice = test_random_bounded(random, 16);

    if (choice == 0) {
        /* Starvation outlives the ring. */
        vsr_io_pool_ring_lost(pool);
        for (uint32_t id = 0; id < m->slabs; ++id) {
            if (m->state[id] == VSR_IO_SLAB_KERNEL) {
                model_leave(m, id);
            }
        }
        m->ring_count = 0;
        m->registered = false;
    } else if (choice == 1) {
        pool->ring_registered = true;
        m->registered = true;
    } else {
        bool expected = m->starved && m->ring_count > 0;

        CHECK(vsr_io_pool_was_starved(pool) == expected);
        if (expected) {
            m->starved = false;
        }
    }
}

static void run_model(struct test_random *random, uint32_t steps)
{
    struct vsr_io_pool pool;
    struct model m;

    memset(&m, 0, sizeof(m));
    m.slabs = 1 + test_random_bounded(random, MAX_SLABS);
    m.reserve = test_random_bounded(random, m.slabs);
    /* reserve + caller_slabs < slabs, as the minimum-slabs rule ensures. */
    m.caller_slabs = test_random_bounded(random, m.slabs - m.reserve);
    m.registered = true;
    setup_share(&pool, m.slabs, m.reserve, m.caller_slabs);
    pool.ring_registered = true;
    model_check(&m, &pool);
    for (uint32_t step = 0; step < steps; ++step) {
        switch (test_random_bounded(random, 10)) {
        case 0:
        case 1:
            step_provide(&m, &pool, random);
            break;
        case 2:
        case 3:
            step_deliver(&m, &pool, random);
            break;
        case 4:
            /* A new holder of a held slab: a lease, or a send of bytes in
             * a slab the caller holds. */
            if (m.holder_count + m.lent_count > 0 &&
                m.holder_count < MAX_HOLDERS) {
                uint32_t pick =
                    test_random_bounded(random, m.holder_count + m.lent_count);
                uint32_t id = pick < m.holder_count
                                  ? m.holders[pick]
                                  : m.lent[pick - m.holder_count];

                vsr_io_pool_retain(&pool, id);
                model_take(&m, id);
            }
            break;
        case 5:
        case 6:
            if (m.holder_count > 0) {
                model_drop(&m, &pool,
                           test_random_bounded(random, m.holder_count));
            }
            break;
        case 7:
            step_acquire(&m, &pool, random);
            break;
        case 8:
            step_caller_release(&m, &pool, random);
            break;
        default:
            step_ring(&m, &pool, random);
            break;
        }
        model_check(&m, &pool);
    }
    while (m.holder_count + m.lent_count > 0) {
        uint32_t pick =
            test_random_bounded(random, m.holder_count + m.lent_count);

        if (pick < m.holder_count) {
            model_drop(&m, &pool, pick);
        } else {
            model_give_back(&m, &pool, pick - m.holder_count);
        }
        model_check(&m, &pool);
    }
    vsr_io_pool_ring_lost(&pool);
    CHECK(m.taken == m.dropped);
    CHECK(pool.free_count == m.slabs && pool.kernel_count == 0);
    CHECK(pool.caller_taken == 0);
    for (uint32_t id = 0; id < m.slabs; ++id) {
        CHECK(pool.entries[id].state == VSR_IO_SLAB_FREE);
        CHECK(pool.entries[id].refs == 0);
    }
    check_pool(&pool);
    check_payload();
}

/*
 * Decision 54: provision leaves the part of the caller's share it does not
 * hold FREE, so with the ring as full as provision makes it the caller
 * still obtains exactly caller_slabs slabs; what the caller gives back
 * restores its share and never reaches the ring.
 */
static void test_caller_share(void)
{
    struct vsr_io_pool pool;
    struct vsr_io_buffer buffers[MAX_SLABS];
    uint32_t internal[4];
    uint32_t a;
    uint32_t b;
    uint32_t c;
    uint32_t d;
    uint16_t k;

    setup_share(&pool, 8, 2, 2);
    CHECK(pool.pending == 4);
    pool.ring_registered = true;
    CHECK(vsr_io_pool_provide(&pool, buffers, MAX_SLABS) == 4);
    CHECK(pool.free_count == 4 && pool.kernel_count == 4 && pool.pending == 0);
    check_pool(&pool);

    /* Exactly the share. */
    a = vsr_io_pool_acquire(&pool, true);
    b = vsr_io_pool_acquire(&pool, true);
    CHECK(a != VSR_IO_INDEX_NONE && b != VSR_IO_INDEX_NONE && a != b);
    for (int i = 0; i < 4; ++i) {
        CHECK(a != buffers[i].id && b != buffers[i].id);
    }
    CHECK(pool.entries[a].caller == 1 && pool.entries[b].caller == 1);
    CHECK(pool.caller_taken == 2 && pool.free_count == 2);
    CHECK(vsr_io_pool_acquire(&pool, true) == VSR_IO_INDEX_NONE);
    CHECK(vsr_io_pool_provide(&pool, buffers + 4, MAX_SLABS) == 0);
    check_pool(&pool);

    /* The kernel returns a slab: FREE above the reserve, yet the share is
     * used up, so the caller gets nothing and the ring gets it back. */
    k = buffers[0].id;
    CHECK(vsr_io_pool_recv_begin(&pool, k, PAGE) == 0);
    vsr_io_pool_release(&pool, k);
    vsr_io_pool_recv_end(&pool, k);
    CHECK(pool.free_count == 3 && pool.pending == 1);
    CHECK(vsr_io_pool_acquire(&pool, true) == VSR_IO_INDEX_NONE);
    check_pool(&pool);

    /* Giving one back restores the share, not the ring's part: pending is
     * still the one slab the kernel returned. */
    CHECK(vsr_io_pool_caller_release(&pool, a));
    CHECK(pool.entries[a].state == VSR_IO_SLAB_FREE);
    CHECK(pool.caller_taken == 1 && pool.free_count == 4 && pool.pending == 1);
    check_pool(&pool);
    CHECK(vsr_io_pool_provide(&pool, buffers + 4, MAX_SLABS) == 1);
    CHECK(buffers[4].id == k || buffers[4].id == a);
    CHECK(pool.free_count == 3 && pool.pending == 0);
    c = vsr_io_pool_acquire(&pool, true);
    CHECK(c != VSR_IO_INDEX_NONE && c != buffers[4].id);
    CHECK(pool.caller_taken == 2 && pool.free_count == 2);
    CHECK(vsr_io_pool_acquire(&pool, true) == VSR_IO_INDEX_NONE);
    check_pool(&pool);

    /* Both back: the ring keeps what it has and gets nothing more. */
    CHECK(vsr_io_pool_caller_release(&pool, b));
    CHECK(vsr_io_pool_caller_release(&pool, c));
    CHECK(pool.caller_taken == 0 && pool.free_count == 4 && pool.pending == 0);
    CHECK(vsr_io_pool_provide(&pool, buffers, MAX_SLABS) == 0);
    CHECK(pool.kernel_count == 4);
    check_pool(&pool);

    /* Internal users may take the share and the reserve alike; the caller,
     * holding none, then gets nothing until the FREE count is above the
     * reserve again. */
    for (int i = 0; i < 4; ++i) {
        internal[i] = vsr_io_pool_acquire(&pool, false);
        CHECK(internal[i] != VSR_IO_INDEX_NONE);
        CHECK(pool.entries[internal[i]].caller == 0);
    }
    CHECK(vsr_io_pool_acquire(&pool, false) == VSR_IO_INDEX_NONE);
    CHECK(vsr_io_pool_acquire(&pool, true) == VSR_IO_INDEX_NONE);
    vsr_io_pool_release(&pool, internal[0]);
    vsr_io_pool_release(&pool, internal[1]);
    CHECK(pool.free_count == 2 && pool.pending == 0);
    CHECK(vsr_io_pool_acquire(&pool, true) == VSR_IO_INDEX_NONE);
    vsr_io_pool_release(&pool, internal[2]);
    d = vsr_io_pool_acquire(&pool, true);
    CHECK(d != VSR_IO_INDEX_NONE && pool.free_count == 2);
    vsr_io_pool_release(&pool, internal[3]);
    CHECK(pool.free_count == 3 && pool.pending == 0);
    CHECK(vsr_io_pool_provide(&pool, buffers, MAX_SLABS) == 0);
    CHECK(vsr_io_pool_caller_release(&pool, d));
    CHECK(pool.free_count == 4 && pool.pending == 0);
    check_pool(&pool);

    /* No share: the caller never acquires, and provision goes down to the
     * reserve. */
    setup_share(&pool, 4, 1, 0);
    CHECK(vsr_io_pool_acquire(&pool, true) == VSR_IO_INDEX_NONE);
    CHECK(pool.free_count == 4 && pool.pending == 3);
    pool.ring_registered = true;
    CHECK(vsr_io_pool_provide(&pool, buffers, MAX_SLABS) == 3);
    check_pool(&pool);
    check_payload();
}

/*
 * The caller's release takes only a slab the caller holds; anything else
 * changes nothing. A send retaining the caller's bytes keeps the slab HELD
 * past the release, but the share returns at once, and provision still
 * leaves it FREE.
 */
static void test_caller_release(void)
{
    struct vsr_io_pool pool;
    struct vsr_io_buffer buffers[MAX_SLABS];
    struct vsr_io_slab_entry before[MAX_SLABS];
    uint32_t internal;
    uint32_t free_id = VSR_IO_INDEX_NONE;
    uint32_t a;
    uint32_t a2;
    uint16_t k;

    setup_share(&pool, 6, 1, 3);
    pool.ring_registered = true;
    CHECK(vsr_io_pool_provide(&pool, buffers, MAX_SLABS) == 2);
    k = buffers[0].id;
    CHECK(vsr_io_pool_recv_begin(&pool, k, 100) == 0); /* KERNEL, refs 1. */
    internal = vsr_io_pool_acquire(&pool, false);
    a = vsr_io_pool_acquire(&pool, true);
    CHECK(internal != VSR_IO_INDEX_NONE && a != VSR_IO_INDEX_NONE);
    for (uint32_t id = 0; id < pool.slabs; ++id) {
        if (pool.entries[id].state == VSR_IO_SLAB_FREE) {
            free_id = id;
        }
    }
    CHECK(free_id != VSR_IO_INDEX_NONE);
    memcpy(before, pool.entries, sizeof(before[0]) * pool.slabs);
    CHECK(!vsr_io_pool_caller_release(&pool, k));
    CHECK(!vsr_io_pool_caller_release(&pool, internal));
    CHECK(!vsr_io_pool_caller_release(&pool, free_id));
    CHECK(!vsr_io_pool_caller_release(&pool, 6));
    CHECK(!vsr_io_pool_caller_release(&pool, VSR_IO_INDEX_NONE));
    CHECK(memcmp(before, pool.entries, sizeof(before[0]) * pool.slabs) == 0);
    CHECK(pool.caller_taken == 1 && pool.free_count == 2);
    check_pool(&pool);

    /* A send of the caller's bytes is in flight when the caller releases. */
    vsr_io_pool_retain(&pool, a);
    CHECK(vsr_io_pool_caller_release(&pool, a));
    CHECK(pool.entries[a].state == VSR_IO_SLAB_HELD);
    CHECK(pool.entries[a].refs == 1 && pool.entries[a].caller == 0);
    CHECK(pool.caller_taken == 0);
    CHECK(!vsr_io_pool_caller_release(&pool, a));
    check_pool(&pool);
    /* The share is back although the slab is not FREE: the caller may take
     * another, down to the reserve, and the ring gets nothing. */
    CHECK(vsr_io_pool_provide(&pool, buffers + 2, MAX_SLABS) == 0);
    a2 = vsr_io_pool_acquire(&pool, true);
    CHECK(a2 != VSR_IO_INDEX_NONE && a2 != a);
    CHECK(pool.free_count == 1);
    CHECK(vsr_io_pool_acquire(&pool, true) == VSR_IO_INDEX_NONE);
    check_pool(&pool);
    /* The send's NOTIF: FREE, but within the floor of 1 + 3 - 1. */
    vsr_io_pool_release(&pool, a);
    CHECK(pool.entries[a].state == VSR_IO_SLAB_FREE);
    CHECK(pool.free_count == 2 && pool.pending == 0);
    CHECK(vsr_io_pool_provide(&pool, buffers + 2, MAX_SLABS) == 0);
    check_pool(&pool);

    CHECK(vsr_io_pool_caller_release(&pool, a2));
    vsr_io_pool_release(&pool, internal);
    vsr_io_pool_release(&pool, k);
    vsr_io_pool_ring_lost(&pool);
    CHECK(pool.free_count == 6 && pool.caller_taken == 0);
    check_pool(&pool);
    check_payload();
}

/* A receive that ended with -ENOBUFS before the ring was lost is re-armed
 * once a new ring holds a buffer (the flag used to be cleared by the
 * loss, leaving that receive unarmed for good). */
static void test_starved_across_ring_loss(void)
{
    struct vsr_io_pool pool;
    struct vsr_io_buffer buffers[MAX_SLABS];
    uint32_t held[4];

    setup(&pool, 4, 1);
    pool.ring_registered = true;
    for (int i = 0; i < 4; ++i) {
        held[i] = vsr_io_pool_acquire(&pool, false);
        CHECK(held[i] != VSR_IO_INDEX_NONE);
    }
    vsr_io_pool_starved(&pool);
    vsr_io_pool_ring_lost(&pool);
    CHECK(!vsr_io_pool_was_starved(&pool));
    for (int i = 0; i < 4; ++i) {
        vsr_io_pool_release(&pool, held[i]);
    }
    CHECK(vsr_io_pool_provide(&pool, buffers, MAX_SLABS) == 0);
    CHECK(!vsr_io_pool_was_starved(&pool));
    pool.ring_registered = true;
    CHECK(vsr_io_pool_provide(&pool, buffers, MAX_SLABS) == 3);
    CHECK(vsr_io_pool_was_starved(&pool));
    CHECK(!vsr_io_pool_was_starved(&pool));
    check_pool(&pool);
    check_payload();
}

/* The largest pool: 32768 slabs, every 16-bit id provided once and mapped
 * back to its bytes, a ring of 32768 entries. Slabs of 16 bytes over a
 * 16-byte "page" keep the payload small; the pool never reads it. */
static _Alignas(4096) unsigned char big_payload[32768u * 16u];
static _Alignas(4096) unsigned char big_bookkeeping[32768u * 32u];
static struct vsr_io_buffer big_buffers[32768];
static bool big_seen[32768];

static void test_largest_ring(void)
{
    struct vsr_io_pool pool;
    struct vsr_io_limits limits = limits_for(32768, 16);
    size_t bytes = 0;
    size_t alignment = 0;
    unsigned char local = 0;

    CHECK(vsr_io_pool_size(&limits, 16, &bytes, &alignment) == VSR_OK);
    CHECK(bytes == (size_t)32768 * (16 + sizeof(struct vsr_io_slab_entry)));
    CHECK(bytes <= sizeof(big_bookkeeping) && alignment == 16);
    vsr_io_pool_init(&pool, big_payload, sizeof(big_payload), &limits, 0, 0, 0,
                     big_bookkeeping, bytes);
    CHECK(pool.ring_entries == 32768);
    pool.ring_registered = true;
    CHECK(vsr_io_pool_provide(&pool, big_buffers, 32768) == 32768);
    for (uint32_t i = 0; i < 32768; ++i) {
        uint16_t id = big_buffers[i].id;

        CHECK(!big_seen[id]);
        big_seen[id] = true;
        CHECK(big_buffers[i].base == big_payload + (size_t)id * 16);
        CHECK(vsr_io_pool_locate(&pool, big_buffers[i].base) == id);
    }
    CHECK(pool.kernel_count == 32768 && pool.free_count == 0);
    CHECK(vsr_io_pool_recv_begin(&pool, 32767, 10) == 0);
    CHECK(vsr_io_pool_recv_begin(&pool, 32767, 6) == 10);
    vsr_io_pool_recv_end(&pool, 32767);
    CHECK(pool.entries[32767].state == VSR_IO_SLAB_HELD);
    CHECK(vsr_io_pool_locate(&pool, big_payload + sizeof(big_payload) - 1) ==
          32767);
    CHECK(vsr_io_pool_locate(&pool, big_payload + sizeof(big_payload)) ==
          VSR_IO_INDEX_NONE);
    CHECK(!vsr_io_pool_contains(&pool, &local, 1));
    CHECK(!vsr_io_pool_contains(&pool, big_bookkeeping, 1));
    vsr_io_pool_release(&pool, 32767);
    vsr_io_pool_release(&pool, 32767);
    CHECK(pool.free_count == 1);
    vsr_io_pool_ring_lost(&pool);
    CHECK(pool.free_count == 32768 && pool.kernel_count == 0);

    /* A page smaller than an entry's alignment: the entries' alignment. */
    limits = limits_for(4, 1);
    CHECK(vsr_io_pool_size(&limits, 1, &bytes, &alignment) == VSR_OK);
    CHECK(alignment == _Alignof(struct vsr_io_slab_entry));
}

int main(int argc, char **argv)
{
    uint64_t seed = 1;
    struct test_random random;

    if (argc > 1) {
        seed = strtoull(argv[1], NULL, 0);
    }
    printf("pool seed %" PRIu64 "\n", seed);
    test_layout();
    test_locate_contains();
    test_provide_reserve();
    test_incremental();
    test_kernel_leaves();
    test_acquire_reserve();
    test_starved();
    test_ring_lost();
    test_caller_share();
    test_caller_release();
    test_starved_across_ring_loss();
    test_largest_ring();
    test_random_seed(&random, seed, 1);
    for (uint32_t round = 0; round < 200; ++round) {
        run_model(&random, 1000);
    }
    return 0;
}
