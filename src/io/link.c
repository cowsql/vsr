#include "config.h"

#include "io/link.h"

#include "checked.h"
#include "io/pool.h"

#include <stdalign.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

/*
 * Table layout of struct vsr_io_links, in one region: the node table, the
 * authorization table, the link table, the per-node send queues
 * (link_queue entries per node, one ring each) and the per-link vector
 * arrays (VSR_IO_SEND_VECTORS per link). Every table starts at its type's
 * alignment; the region's alignment is the largest of them.
 */

#define LINK_NONE VSR_IO_INDEX_NONE

/* Invariant checks in debug builds; a violation traps (see pool.c). */
#ifdef NDEBUG
#define LINKS_ASSERT(condition) ((void)sizeof(condition))
#else
#define LINKS_ASSERT(condition) ((condition) ? (void)0 : __builtin_trap())
#endif

struct links_plan {
    size_t nodes;
    size_t authorizations;
    size_t links;
    size_t queue;
    size_t vecs;
    size_t total;
    size_t alignment;
};

static size_t align_max(size_t a, size_t b)
{
    return a > b ? a : b;
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

static bool links_plan(const struct vsr_io_limits *limits,
                       struct links_plan *plan)
{
    size_t offset = 0;
    size_t bytes;
    size_t count;

    memset(plan, 0, sizeof(*plan));
    plan->alignment = alignof(struct vsr_io_node);
    plan->alignment =
        align_max(plan->alignment, alignof(struct vsr_io_authorization));
    plan->alignment = align_max(plan->alignment, alignof(struct vsr_io_link));
    plan->alignment =
        align_max(plan->alignment, alignof(struct vsr_io_queued_send));
    plan->alignment = align_max(plan->alignment, alignof(struct vsr_io_vec));
    if (!vsr_size_mul(limits->nodes, sizeof(struct vsr_io_node), &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_node), &plan->nodes) ||
        !vsr_size_mul(limits->authorizations,
                      sizeof(struct vsr_io_authorization), &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_authorization),
               &plan->authorizations) ||
        !vsr_size_mul(limits->links, sizeof(struct vsr_io_link), &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_link), &plan->links) ||
        !vsr_size_mul(limits->nodes, limits->link_queue, &count) ||
        !vsr_size_mul(count, sizeof(struct vsr_io_queued_send), &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_queued_send),
               &plan->queue) ||
        !vsr_size_mul(limits->links, VSR_IO_SEND_VECTORS, &count) ||
        !vsr_size_mul(count, sizeof(struct vsr_io_vec), &bytes) ||
        !place(&offset, bytes, alignof(struct vsr_io_vec), &plan->vecs)) {
        return false;
    }
    plan->total = offset;
    return true;
}

int vsr_io_links_size(const struct vsr_io_limits *limits, size_t *bytes,
                      size_t *alignment)
{
    struct links_plan plan;

    if (limits == NULL || bytes == NULL || alignment == NULL) {
        return VSR_EINVAL;
    }
    if (!links_plan(limits, &plan)) {
        return VSR_ELIMIT;
    }
    *bytes = plan.total;
    *alignment = plan.alignment;
    return VSR_OK;
}

static void node_reset(struct vsr_io_node *node)
{
    memset(node, 0, sizeof(*node));
    node->carrier = LINK_NONE;
    node->next_dial_ns = VSR_NO_DEADLINE;
}

static void link_reset(struct vsr_io_link *link, struct vsr_io_vec *vecs)
{
    memset(link, 0, sizeof(*link));
    link->state = VSR_IO_LINK_FREE;
    link->node_index = LINK_NONE;
    link->node = VSR_IO_NO_NODE;
    link->fd = -1;
    link->stream = LINK_NONE;
    link->recv_slot = LINK_NONE;
    link->partial_slab = LINK_NONE;
    link->reassembly_slab = LINK_NONE;
    link->send_slab = LINK_NONE;
    for (uint32_t i = 0; i < VSR_IO_LINK_SENDS; ++i) {
        link->sends[i].slot = LINK_NONE;
    }
    link->encoding = LINK_NONE;
    link->deadline = LINK_NONE; /* Bound by the engine after init. */
    link->shutdown_slot = LINK_NONE;
    link->vecs = vecs;
}

void vsr_io_links_init(struct vsr_io_links *links, void *memory, size_t size,
                       const struct vsr_io_limits *limits, uint64_t frame_limit)
{
    struct links_plan plan;
    unsigned char *base = memory;
    bool sized = links_plan(limits, &plan);

    LINKS_ASSERT(sized && plan.total <= size);
    (void)sized;
    (void)size;
    memset(links, 0, sizeof(*links));
    memset(base, 0, plan.total);
    links->nodes = (struct vsr_io_node *)(void *)(base + plan.nodes);
    links->authorizations =
        (struct vsr_io_authorization *)(void *)(base + plan.authorizations);
    links->links = (struct vsr_io_link *)(void *)(base + plan.links);
    links->queue = (struct vsr_io_queued_send *)(void *)(base + plan.queue);
    links->vecs = (struct vsr_io_vec *)(void *)(base + plan.vecs);
    links->nodes_count = limits->nodes;
    links->authorizations_count = limits->authorizations;
    links->links_count = limits->links;
    links->link_queue = limits->link_queue;
    links->established = 0;
    links->pending = 0;
    links->listeners = 0;
    links->frame_limit = frame_limit;
    for (uint32_t i = 0; i < links->nodes_count; ++i) {
        node_reset(&links->nodes[i]);
    }
    for (uint32_t i = 0; i < links->links_count; ++i) {
        link_reset(&links->links[i],
                   links->vecs + (size_t)i * VSR_IO_SEND_VECTORS);
    }
}
