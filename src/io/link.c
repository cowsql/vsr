#include "config.h"

#include "io/link.h"

#include "checked.h"
#include "io/codec.h"
#include "io/crc32c.h"
#include "io/deadline.h"
#include "io/engine.h"
#include "io/pool.h"
#include "io/slots.h"
#include "io/stream.h"

#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdalign.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <sys/socket.h>

/*
 * Table layout of struct vsr_io_links, in one region: the node table, the
 * authorization table, the link table, the per-node send queues
 * (link_queue entries per node, one ring each) and the per-link vector
 * arrays (VSR_IO_SEND_VECTORS per link). Every table starts at its type's
 * alignment; the region's alignment is the largest of them.
 *
 * Then the planner (docs/io-implementation.md, "Links"): the node and
 * authorization tables behind the public calls, dialing with backoff and
 * LINK_WANTED, accepting, the TRUSTED and EXTERNAL handshakes, carrier
 * election, the handshake and idle deadlines, and the close paths. Every
 * time-based decision is taken at poll time or from io->now, the last poll
 * time, since completions carry no clock. The receive path carves the
 * preamble and whole frames in place; a frame that straddles slabs is
 * reassembled (phase 2, marked below), MESSAGE frames are delivered under
 * the sender's authorization (phase 2) and sends beyond the handshake's
 * control bytes are coalesced from the node queues (phase 3).
 */

#define LINK_NONE VSR_IO_INDEX_NONE

/* Invariant checks in debug builds; a violation traps (see pool.c). */
#ifdef NDEBUG
#define LINKS_ASSERT(condition) ((void)sizeof(condition))
#else
#define LINKS_ASSERT(condition) ((condition) ? (void)0 : __builtin_trap())
#endif

/* Sub-record tags of a SHUTDOWN-kind slot. */
enum teardown_owner { TEARDOWN_LINK, TEARDOWN_LISTENER, TEARDOWN_ORPHAN };

/* Send entry states. */
enum send_state { SEND_FREE, SEND_INFLIGHT, SEND_NOTIF };

/* Longest chain one prepare step needs: the listener's SOCKET, BIND, LISTEN
 * and ACCEPT (a link teardown is at most SHUTDOWN, CANCEL and CLOSE). */
#define LINK_CHAIN_MAX 4u
#define LINK_HELLO_BYTES                                                       \
    (VSR_IO_FRAME_HEADER_BYTES + (uint32_t)sizeof(struct vsr_io_wire_hello))

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

static void link_reset(struct vsr_io_link *link, struct vsr_io_vec *vecs,
                       uint32_t deadline)
{
    memset(link, 0, sizeof(*link));
    link->state = VSR_IO_LINK_FREE;
    link->node_index = LINK_NONE;
    link->node = VSR_IO_NO_NODE;
    link->fd = -1;
    link->raw_fd = -1;
    link->stream = LINK_NONE;
    link->connect_slot = LINK_NONE;
    link->recv_slot = LINK_NONE;
    link->partial_slab = LINK_NONE;
    link->reassembly_slab = LINK_NONE;
    link->send_slab = LINK_NONE;
    for (uint32_t i = 0; i < VSR_IO_LINK_SENDS; ++i) {
        link->sends[i].slot = LINK_NONE;
    }
    link->encoding = LINK_NONE;
    link->deadline = deadline; /* Bound by the engine after init. */
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
                   links->vecs + (size_t)i * VSR_IO_SEND_VECTORS, LINK_NONE);
    }
    for (uint32_t i = 0; i < VSR_IO_LISTENERS_MAX; ++i) {
        links->listener_table[i].state = VSR_IO_LISTENER_FREE;
        links->listener_table[i].file_slot = LINK_NONE;
        links->listener_table[i].slot = LINK_NONE;
        links->listener_table[i].cancel = LINK_NONE;
    }
    for (uint32_t i = 0; i < VSR_IO_LINK_ORPHANS; ++i) {
        links->orphans[i] = -1;
    }
}

/* -------------------------------------------------------------------------
 * Nodes and authorizations
 * ---------------------------------------------------------------------- */

static bool id_zero(struct vsr_id id)
{
    return id.hi == 0 && id.lo == 0;
}

static bool id_equal(struct vsr_id a, struct vsr_id b)
{
    return a.hi == b.hi && a.lo == b.lo;
}

uint32_t vsr_io_links_node_index(const struct vsr_io_links *links,
                                 uint64_t node)
{
    if (node == VSR_IO_NO_NODE) {
        return LINK_NONE;
    }
    for (uint32_t i = 0; i < links->nodes_count; ++i) {
        if (links->nodes[i].id == node) {
            return i;
        }
    }
    return LINK_NONE;
}

uint64_t vsr_io_links_lookup(const struct vsr_io_links *links,
                             struct vsr_id cluster, uint64_t replica)
{
    for (uint32_t i = 0; i < links->authorizations_count; ++i) {
        const struct vsr_io_authorization *entry = &links->authorizations[i];

        if (id_equal(entry->cluster, cluster) && entry->replica == replica) {
            return entry->node;
        }
    }
    return VSR_IO_NO_NODE;
}

/* True while some (cluster, replica) names the node. */
static bool node_authorized(const struct vsr_io_links *links, uint64_t node)
{
    for (uint32_t i = 0; i < links->authorizations_count; ++i) {
        if (links->authorizations[i].node == node) {
            return true;
        }
    }
    return false;
}

static bool address_valid(const struct vsr_io_address *address)
{
    return address->length > 0 &&
           address->length <= sizeof(address->sockaddr) &&
           address->reserved == 0;
}

static bool address_equal(const struct vsr_io_address *a,
                          const struct vsr_io_address *b)
{
    return a->length == b->length &&
           memcmp(&a->sockaddr, &b->sockaddr, a->length) == 0;
}

/* Dial backoff after `attempts` consecutive failures: the initial delay,
 * doubling to 16x (VSR_IO_BACKOFF_MAX_SHIFT). */
static uint64_t backoff_ns(const struct vsr_io *io, uint32_t attempts)
{
    uint64_t delay = io->options.connect_backoff_ns;
    uint32_t shift = attempts > 0 ? attempts - 1 : 0;

    if (shift > VSR_IO_BACKOFF_MAX_SHIFT) {
        shift = VSR_IO_BACKOFF_MAX_SHIFT;
    }
    if (delay > (VSR_NO_DEADLINE - 1) >> shift) {
        return (VSR_NO_DEADLINE - 1) >> shift << shift;
    }
    return delay << shift;
}

static uint32_t dial_handle(const struct vsr_io *io, uint32_t node_index)
{
    /* DIAL handles follow the LINK handles (decisions 60 and 67). */
    return io->links.links_count + node_index;
}

static uint32_t node_peer_pending(const struct vsr_io *io, uint32_t node_index);

/* A node that needs a peer link and has none in progress is dialed now, or
 * once its backoff expires, by the next poll. Stream links count for
 * nothing here: they never carry the node's sends. */
static void node_want_dial(struct vsr_io *io, uint32_t index)
{
    struct vsr_io_node *node = &io->links.nodes[index];

    if (io->links.closing || !node->wanted || node->dialing ||
        node->carrier != LINK_NONE || node->due ||
        node->id == io->options.node || node_peer_pending(io, index) > 0) {
        return;
    }
    if (node->next_dial_ns == VSR_NO_DEADLINE ||
        node->next_dial_ns <= io->now) {
        node->due = true;
        io->links.dials_due++;
        return;
    }
    vsr_io_deadlines_arm(&io->deadlines, dial_handle(io, index),
                         node->next_dial_ns);
}

/* A replica authorizing the node, or a SEND toward it, wants a link when
 * no peer link is established. */
static void node_want(struct vsr_io *io, uint32_t index)
{
    struct vsr_io_node *node = &io->links.nodes[index];

    if (node->carrier == LINK_NONE) {
        node->wanted = true;
    }
    node_want_dial(io, index);
}

/* One more failed dial or unanswered LINK_WANTED: the next attempt waits
 * out the backoff. */
static void node_backoff(struct vsr_io *io, uint32_t index, int32_t error)
{
    struct vsr_io_node *node = &io->links.nodes[index];

    if (node->attempts < UINT32_MAX) {
        node->attempts++;
    }
    node->next_dial_ns = io->now + backoff_ns(io, node->attempts);
    if (error != 0) {
        node->last_error = error;
    }
    node->dialing = false;
}

static void link_close(struct vsr_io *io, uint32_t index, int32_t error);
static bool link_send_slab(struct vsr_io *io, struct vsr_io_link *link);
static void node_queue_drop(struct vsr_io *io, uint32_t node_index, bool all);
static void node_queue_ready(struct vsr_io *io, uint32_t node_index);
static void node_queue_review(struct vsr_io *io, uint32_t node_index,
                              uint32_t old_carrier);

/* Closes every link of the node; the carrier's loss retries its queued
 * sends (node_queue_review). */
static void node_close_links(struct vsr_io *io, uint32_t index, int32_t error)
{
    for (uint32_t i = 0; i < io->links.links_count; ++i) {
        if (io->links.links[i].node_index == index) {
            link_close(io, i, error);
        }
    }
}

int vsr_io_links_node_set(struct vsr_io *io, uint64_t node,
                          const struct vsr_io_address *address)
{
    struct vsr_io_links *links = &io->links;
    uint32_t index = vsr_io_links_node_index(links, node);
    struct vsr_io_node *entry;

    if (node == VSR_IO_NO_NODE ||
        (address != NULL && !address_valid(address))) {
        return VSR_EINVAL;
    }
    if (index == LINK_NONE) {
        for (index = 0; index < links->nodes_count; ++index) {
            if (links->nodes[index].id == 0) {
                break;
            }
        }
        if (index == links->nodes_count) {
            return VSR_ELIMIT;
        }
        entry = &links->nodes[index];
        node_reset(entry);
        entry->id = node;
    } else {
        entry = &links->nodes[index];
        if (entry->has_address == (address != NULL) &&
            (address == NULL || address_equal(&entry->address, address))) {
            return VSR_OK;
        }
        /* A changed address closes the links and restarts the schedule. */
        node_close_links(io, index, -ECONNABORTED);
        entry->attempts = 0;
        entry->next_dial_ns = VSR_NO_DEADLINE;
        entry->last_error = 0;
        vsr_io_deadlines_arm(&io->deadlines, dial_handle(io, index),
                             VSR_NO_DEADLINE);
        if (entry->due) {
            entry->due = false;
            links->dials_due--;
        }
        entry->dialing = false;
    }
    memset(&entry->address, 0, sizeof(entry->address));
    entry->has_address = address != NULL;
    if (address != NULL) {
        entry->address.length = address->length;
        memcpy(&entry->address.sockaddr, &address->sockaddr, address->length);
    }
    if (node_authorized(links, node)) {
        node_want(io, index);
    }
    return VSR_OK;
}

int vsr_io_links_node_clear(struct vsr_io *io, uint64_t node)
{
    struct vsr_io_links *links = &io->links;
    uint32_t index = vsr_io_links_node_index(links, node);
    struct vsr_io_node *entry;

    if (index == LINK_NONE) {
        return VSR_EINVAL;
    }
    entry = &links->nodes[index];
    node_close_links(io, index, -ECONNABORTED);
    node_queue_drop(io, index, true); /* Nothing waits for an unknown node. */
    /* A cleared node is unknown, so nothing may act as it (decision 73). */
    for (uint32_t i = 0; i < links->authorizations_count; ++i) {
        if (links->authorizations[i].node == node) {
            memset(&links->authorizations[i], 0,
                   sizeof(links->authorizations[i]));
        }
    }
    vsr_io_deadlines_arm(&io->deadlines, dial_handle(io, index),
                         VSR_NO_DEADLINE);
    if (entry->due) {
        links->dials_due--;
    }
    node_reset(entry);
    return VSR_OK;
}

int vsr_io_links_authorize(struct vsr_io *io, struct vsr_id cluster,
                           uint64_t replica, uint64_t node)
{
    struct vsr_io_links *links = &io->links;
    struct vsr_io_authorization *entry = NULL;
    struct vsr_io_authorization *free = NULL;
    uint64_t previous;
    uint32_t index;

    if (id_zero(cluster)) {
        return VSR_EINVAL;
    }
    for (uint32_t i = 0; i < links->authorizations_count; ++i) {
        struct vsr_io_authorization *candidate = &links->authorizations[i];

        if (id_zero(candidate->cluster)) {
            if (free == NULL) {
                free = candidate;
            }
        } else if (id_equal(candidate->cluster, cluster) &&
                   candidate->replica == replica) {
            entry = candidate;
        }
    }
    if (node == VSR_IO_NO_NODE) {
        /* Revoke: idempotent; the node's links close once nothing names
         * it any more (decision 73). */
        if (entry == NULL) {
            return VSR_OK;
        }
        previous = entry->node;
        memset(entry, 0, sizeof(*entry));
        index = vsr_io_links_node_index(links, previous);
        if (index != LINK_NONE && !node_authorized(links, previous)) {
            node_close_links(io, index, -ECONNABORTED);
            links->nodes[index].wanted = false;
        }
        return VSR_OK;
    }
    index = vsr_io_links_node_index(links, node);
    if (index == LINK_NONE) {
        return VSR_EINVAL; /* A node must be known to be authorized. */
    }
    if (entry != NULL && entry->node == node) {
        node_want(io, index); /* Re-authorizing still wants a link. */
        return VSR_OK;
    }
    if (entry == NULL) {
        if (free == NULL) {
            return VSR_ELIMIT;
        }
        entry = free;
        previous = VSR_IO_NO_NODE;
    } else {
        previous = entry->node;
    }
    entry->cluster = cluster;
    entry->replica = replica;
    entry->node = node;
    if (previous != VSR_IO_NO_NODE) {
        uint32_t old = vsr_io_links_node_index(links, previous);

        if (old != LINK_NONE && !node_authorized(links, previous)) {
            node_close_links(io, old, -ECONNABORTED);
            links->nodes[old].wanted = false;
        }
    }
    /* A replica authorizing a node dials it. */
    node_want(io, index);
    return VSR_OK;
}

int vsr_io_links_node_status(const struct vsr_io *io, uint64_t node,
                             struct vsr_io_node_status *status)
{
    const struct vsr_io_links *links = &io->links;
    uint32_t index = vsr_io_links_node_index(links, node);
    const struct vsr_io_node *entry;

    if (status == NULL) {
        return VSR_EINVAL;
    }
    memset(status, 0, sizeof(*status));
    if (index == LINK_NONE) {
        return VSR_EINVAL;
    }
    entry = &links->nodes[index];
    if (entry->established > 0) {
        status->state = VSR_IO_NODE_LINKED;
    } else if (entry->pending > 0 || entry->dialing) {
        status->state = VSR_IO_NODE_PENDING;
    } else {
        status->state = VSR_IO_NODE_UNLINKED;
    }
    status->links = entry->established;
    status->linked_since_ns = entry->linked_since_ns;
    status->last_received_ns = entry->last_received_ns;
    status->next_dial_ns = status->state == VSR_IO_NODE_UNLINKED
                               ? entry->next_dial_ns
                               : VSR_NO_DEADLINE;
    status->last_error = entry->last_error;
    return VSR_OK;
}

/* -------------------------------------------------------------------------
 * Links: allocation, election, closing
 * ---------------------------------------------------------------------- */

static uint32_t link_index(const struct vsr_io *io,
                           const struct vsr_io_link *link)
{
    return (uint32_t)(link - io->links.links);
}

static bool link_pending_state(uint32_t state)
{
    return state == VSR_IO_LINK_CONNECTING || state == VSR_IO_LINK_EXTERNAL ||
           state == VSR_IO_LINK_HELLO;
}

/* Pending peer links (CONNECTING, EXTERNAL, HELLO) known to be the node's:
 * its outbound dials, and an EXTERNAL inbound link once identified. */
static uint32_t node_peer_pending(const struct vsr_io *io, uint32_t node_index)
{
    const struct vsr_io_links *links = &io->links;
    uint32_t count = 0;

    for (uint32_t i = 0; i < links->links_count; ++i) {
        const struct vsr_io_link *link = &links->links[i];

        if (link->node_index == node_index &&
            link->purpose == VSR_IO_PURPOSE_PEER &&
            link_pending_state(link->state)) {
            count++;
        }
    }
    return count;
}

/* Takes a FREE link for a node (or LINK_NONE when unidentified yet);
 * LINK_NONE when the table is full. Pending and established counts follow
 * the state transitions below. */
static uint32_t link_alloc(struct vsr_io *io, uint32_t purpose,
                           uint32_t direction, uint32_t node_index,
                           uint64_t node)
{
    struct vsr_io_links *links = &io->links;

    for (uint32_t i = 0; i < links->links_count; ++i) {
        struct vsr_io_link *link = &links->links[i];

        if (link->state != VSR_IO_LINK_FREE) {
            continue;
        }
        link_reset(link, link->vecs, link->deadline);
        link->purpose = purpose;
        link->direction = direction;
        link->node_index = node_index;
        link->node = node;
        vsr_io_engine_random(io, &link->nonce, sizeof(link->nonce));
        return i;
    }
    return LINK_NONE;
}

/* Binds a link to the node it turned out to belong to (inbound links
 * learn it from the handshake), keeping the node's counts right. */
static void link_identify(struct vsr_io *io, struct vsr_io_link *link,
                          uint32_t node_index)
{
    LINKS_ASSERT(link->node_index == LINK_NONE);
    link->node_index = node_index;
    link->node = io->links.nodes[node_index].id;
    if (link_pending_state(link->state)) {
        io->links.nodes[node_index].pending++;
    }
}

/* Moves a link into a pending state (CONNECTING, EXTERNAL or HELLO),
 * counting it for its node and the stats. */
static void link_enter_pending(struct vsr_io *io, struct vsr_io_link *link,
                               uint32_t state)
{
    LINKS_ASSERT(link_pending_state(state) && link->state == VSR_IO_LINK_FREE);
    link->state = state;
    io->links.pending++;
    if (link->node_index != LINK_NONE) {
        io->links.nodes[link->node_index].pending++;
    }
}

/* Carrier election (decision 41): among the established peer links of the
 * node, the one dialed by the lower node identity wins, oldest first; with
 * none in that direction, the oldest of any direction. */
static void node_elect(struct vsr_io *io, uint32_t node_index)
{
    struct vsr_io_links *links = &io->links;
    struct vsr_io_node *node = &links->nodes[node_index];
    uint32_t preferred =
        io->options.node < node->id ? VSR_IO_OUTBOUND : VSR_IO_INBOUND;
    uint32_t best = LINK_NONE;
    bool best_preferred = false;

    for (uint32_t i = 0; i < links->links_count; ++i) {
        const struct vsr_io_link *link = &links->links[i];
        bool is_preferred;

        if (link->state != VSR_IO_LINK_ESTABLISHED ||
            link->node_index != node_index ||
            link->purpose != VSR_IO_PURPOSE_PEER) {
            continue;
        }
        is_preferred = link->direction == preferred;
        if (best == LINK_NONE || (is_preferred && !best_preferred) ||
            (is_preferred == best_preferred &&
             link->established_ns < links->links[best].established_ns)) {
            best = i;
            best_preferred = is_preferred;
        }
    }
    node->carrier = best;
}

/* The idle timer is lazy: armed at establishment and, when it pops while
 * the link was active meanwhile, re-armed from the last activity, so a
 * delivery costs no heap operation. */
static void link_arm_idle(struct vsr_io *io, struct vsr_io_link *link)
{
    uint64_t idle = io->options.idle_timeout_ns;

    vsr_io_deadlines_arm(&io->deadlines, link->deadline,
                         idle == 0 ? VSR_NO_DEADLINE
                                   : link->last_active_ns + idle);
}

/* The handshake succeeded (or was done by the caller): the link carries
 * traffic; a peer link resets the node's dial schedule and ends its want. */
static void link_establish(struct vsr_io *io, struct vsr_io_link *link)
{
    struct vsr_io_node *node;

    LINKS_ASSERT(link_pending_state(link->state) &&
                 link->node_index != LINK_NONE);
    node = &io->links.nodes[link->node_index];
    io->links.pending--;
    node->pending--;
    link->state = VSR_IO_LINK_ESTABLISHED;
    link->established_ns = io->now;
    link->last_active_ns = io->now;
    link->stage = VSR_IO_STAGE_NONE;
    io->links.established++;
    if (node->established == 0) {
        node->linked_since_ns = io->now;
    }
    node->established++;
    if (link->purpose == VSR_IO_PURPOSE_PEER) {
        uint32_t old = node->carrier;

        node->attempts = 0;
        node->next_dial_ns = VSR_NO_DEADLINE;
        node->last_error = 0;
        node->dialing = false;
        vsr_io_deadlines_arm(&io->deadlines, dial_handle(io, link->node_index),
                             VSR_NO_DEADLINE);
        if (node->due) {
            node->due = false;
            io->links.dials_due--;
        }
        node->wanted = false;
        node_elect(io, link->node_index);
        node_queue_review(io, link->node_index, old);
    }
    link_arm_idle(io, link);
    /* Best effort: a send without a slab retries the pool at prepare. */
    (void)link_send_slab(io, link);
    if (link->purpose == VSR_IO_PURPOSE_STREAM && link->stream != LINK_NONE) {
        vsr_io_streams_link_up(io, link->stream);
    }
}

static void link_set_retry(struct vsr_io *io, struct vsr_io_link *link,
                           bool retry)
{
    if (link->retry == retry) {
        return;
    }
    link->retry = retry;
    if (retry) {
        io->links.retries_due++;
    } else {
        io->links.retries_due--;
    }
}

/* Drops the receive-side slab references a link holds: the partial run
 * (whose reference is the reassembly slab's own once the run lives there)
 * and every held run. */
static void link_release_partial(struct vsr_io *io, struct vsr_io_link *link)
{
    if (link->partial_slab != LINK_NONE) {
        vsr_io_pool_release(&io->pool, link->partial_slab);
        if (link->reassembly_slab == link->partial_slab) {
            link->reassembly_slab = LINK_NONE;
        }
        link->partial_slab = LINK_NONE;
    }
    link->partial_length = 0;
    if (link->reassembly_slab != LINK_NONE) {
        vsr_io_pool_release(&io->pool, link->reassembly_slab);
        link->reassembly_slab = LINK_NONE;
    }
    for (uint32_t i = 0; i < link->held_count; ++i) {
        vsr_io_pool_release(&io->pool, link->held[i].slab);
    }
    link->held_count = 0;
    link_set_retry(io, link, false);
}

/* A CLOSING link is FREE once its teardown records went out and every
 * slot it owns completed (the receive terminated, every NOTIF arrived). */
static void link_try_free(struct vsr_io *io, struct vsr_io_link *link)
{
    if (link->state != VSR_IO_LINK_CLOSING || !link->torn_down ||
        link->recv_slot != LINK_NONE || link->shutdown_slot != LINK_NONE ||
        link->connect_slot != LINK_NONE) {
        return;
    }
    for (uint32_t i = 0; i < VSR_IO_LINK_SENDS; ++i) {
        if (link->sends[i].slot != LINK_NONE) {
            return;
        }
    }
    if (link->send_slab != LINK_NONE) {
        vsr_io_pool_release(&io->pool, link->send_slab);
    }
    if (link->fd >= 0) {
        vsr_io_engine_slot_free(io, (uint32_t)link->fd);
    }
    link_reset(link, link->vecs, link->deadline);
}

/* Leaves the current state for CLOSING: counts, election and the node's
 * schedule are updated now; the descriptor is closed by prepare (once the
 * caller returned it, for a HANDSHAKE op in flight) and the entry is freed
 * when every completion is in. error 0 is an orderly close. */
static void link_close(struct vsr_io *io, uint32_t index, int32_t error)
{
    struct vsr_io_link *link = &io->links.links[index];
    struct vsr_io_node *node = NULL;
    uint32_t state = link->state;

    if (state == VSR_IO_LINK_FREE || state == VSR_IO_LINK_CLOSING) {
        return;
    }
    if (link->node_index != LINK_NONE) {
        node = &io->links.nodes[link->node_index];
        if (error != 0) {
            node->last_error = error;
        }
    }
    if (link_pending_state(state)) {
        io->links.pending--;
        if (node != NULL) {
            node->pending--;
        }
        /* A dial that never reached ESTABLISHED, whichever step failed,
         * waits out the backoff before the next; an accepted link's
         * failure is the dialer's to schedule, a stream dial's is the
         * stream's. */
        if (node != NULL && link->direction == VSR_IO_OUTBOUND &&
            link->purpose == VSR_IO_PURPOSE_PEER) {
            node_backoff(io, link->node_index, error);
        }
    } else {
        LINKS_ASSERT(state == VSR_IO_LINK_ESTABLISHED && node != NULL);
        io->links.established--;
        node->established--;
        if (node->established == 0) {
            node->linked_since_ns = 0;
        }
    }
    if (state == VSR_IO_LINK_EXTERNAL &&
        link->stage == VSR_IO_STAGE_HANDSHAKE && link->handshake_op == 0) {
        io->links.handshakes_due--; /* Its op never found ring room. */
    }
    link->state = VSR_IO_LINK_CLOSING;
    link->error = error;
    link->stage = VSR_IO_STAGE_NONE;
    vsr_io_deadlines_arm(&io->deadlines, link->deadline, VSR_NO_DEADLINE);
    link_release_partial(io, link);
    if (node != NULL && link->purpose == VSR_IO_PURPOSE_PEER) {
        uint32_t old = node->carrier;

        if (state == VSR_IO_LINK_ESTABLISHED) {
            node_elect(io, link->node_index);
        }
        node_queue_review(io, link->node_index, old);
        node_want_dial(io, link->node_index);
    } else if (link->purpose == VSR_IO_PURPOSE_STREAM &&
               link->stream != LINK_NONE) {
        vsr_io_streams_link_lost(io, link->stream, error);
    }
    /* The build is dropped; a send in flight completes on its own. */
    if (link->inflight == 0) {
        link->vec_count = 0;
        link->build_bytes = 0;
    }
    link->encoding = LINK_NONE;
    link->encoder.message = NULL;
    /* Nothing to close and nothing that could still produce a descriptor
     * (a SOCKET in flight): gone at once. */
    if (link->fd < 0 && link->raw_fd < 0 && link->handshake_op == 0 &&
        link->connect_slot == LINK_NONE) {
        link->torn_down = true;
        link_try_free(io, link);
    }
}

void vsr_io_links_close(struct vsr_io *io, uint32_t link, int32_t error)
{
    if (link < io->links.links_count) {
        link_close(io, link, error);
    }
}

/* -------------------------------------------------------------------------
 * Send side: the send-slab ring, the build and the send records
 *
 * Every byte a link sends belongs to one stream that `stream_offset`
 * counts. The BUILD is the send under construction: `vec_count` vectors
 * in `vecs` covering the `build_bytes` bytes below `stream_offset`;
 * control bytes, encoded messages and stream frames all append to it.
 * Header bytes (frame headers, control bytes, inline copies, padding) are
 * written into the send slab used as a ring addressed by unwrapped
 * counters: `header_tail` is the write position, `header_sent` where the
 * build's header bytes start and `header_head` the floor below which every
 * byte is reusable (the oldest live send's `header_begin`). A record goes
 * out from the build when nothing is in flight and a send entry is free;
 * a short result leaves the unsent tail of its vectors in place as the
 * next build, so stream offsets never rewind. A plain send's completion,
 * or a zero-copy send's NOTIF, releases its entry, recomputes the floor
 * and `notified_offset` (the oldest live send's `begin`, so NOTIFs may
 * arrive in any order) and completes the queued messages the carrier has
 * sent up to there.
 * ---------------------------------------------------------------------- */

/* A writer area below this many bytes at the slab's end is skipped when
 * the front holds more: a frame header and one inline copy fit. */
#define LINK_RING_MIN (VSR_IO_FRAME_HEADER_BYTES + VSR_IO_INLINE_BYTES)

static unsigned char *link_ring_base(const struct vsr_io *io,
                                     const struct vsr_io_link *link,
                                     uint64_t position)
{
    return vsr_io_pool_slab(&io->pool, link->send_slab) +
           (uint32_t)(position % io->pool.slab_bytes);
}

static uint64_t link_ring_free(const struct vsr_io *io,
                               const struct vsr_io_link *link)
{
    LINKS_ASSERT(link->header_tail - link->header_head <= io->pool.slab_bytes);
    return io->pool.slab_bytes - (link->header_tail - link->header_head);
}

/* The contiguous area at the write position: its base and capacity (0
 * when the ring is full). The tail skips to the slab's start when the
 * space before its end is small and the front holds more. */
static uint32_t link_ring_area(struct vsr_io *io, struct vsr_io_link *link,
                               unsigned char **base)
{
    uint64_t free = link_ring_free(io, link);
    uint32_t to_end = io->pool.slab_bytes -
                      (uint32_t)(link->header_tail % io->pool.slab_bytes);

    if (to_end < LINK_RING_MIN && free > to_end) {
        link->header_tail += to_end;
        free -= to_end;
        to_end = io->pool.slab_bytes;
    }
    *base = link_ring_base(io, link, link->header_tail);
    return free < to_end ? (uint32_t)free : to_end;
}

/* Takes the send slab on first use; false while the pool has none. */
static bool link_send_slab(struct vsr_io *io, struct vsr_io_link *link)
{
    if (link->send_slab == LINK_NONE) {
        link->send_slab = vsr_io_pool_acquire(&io->pool, false);
        if (link->send_slab == LINK_NONE) {
            return false;
        }
        link->header_head = 0;
        link->header_tail = 0;
        link->header_sent = 0;
    }
    return true;
}

/* Merges the build's vector at `index` into the one before it when the
 * two are contiguous in memory (a frame's padding and the next frame's
 * header, say), shifting the later ones down. */
static void link_build_merge(struct vsr_io_link *link, uint32_t index)
{
    struct vsr_io_vec *previous;
    struct vsr_io_vec *vec;

    if (index == 0 || index >= link->vec_count) {
        return;
    }
    previous = &link->vecs[index - 1];
    vec = &link->vecs[index];
    if ((const unsigned char *)previous->base + previous->length != vec->base) {
        return;
    }
    previous->length += vec->length;
    memmove(vec, vec + 1, (link->vec_count - index - 1) * sizeof(*vec));
    link->vec_count--;
}

/* Appends one vector to the build; the caller checked the room. */
static void link_build_append(struct vsr_io_link *link, const void *base,
                              size_t length)
{
    LINKS_ASSERT(link->inflight == 0 && link->vec_count < VSR_IO_SEND_VECTORS);
    /* vsr_io_vec is the executor's descriptor and is not const-qualified;
     * a send never writes through it. */
    /* NOLINTNEXTLINE(performance-no-int-to-ptr) */
    link->vecs[link->vec_count].base = (void *)(uintptr_t)base;
    link->vecs[link->vec_count].length = length;
    link->vec_count++;
    link->build_bytes += length;
    link->stream_offset += length;
    link_build_merge(link, link->vec_count - 1);
}

/* Writes control bytes into the ring as one build vector; false when the
 * send slab is missing, the ring has no contiguous room, no vector is
 * free or a send is in flight (the vectors are its), in which case the
 * caller retries at the next prepare. */
static bool link_write_control(struct vsr_io *io, struct vsr_io_link *link,
                               const unsigned char *bytes, uint32_t length)
{
    unsigned char *base;

    if (link->inflight != 0 || link->vec_count == VSR_IO_SEND_VECTORS ||
        !link_send_slab(io, link) || link_ring_area(io, link, &base) < length) {
        return false;
    }
    memcpy(base, bytes, length);
    link->header_tail += length;
    link_build_append(link, base, length);
    return true;
}

static void put_preamble(unsigned char *out)
{
    vsr_io_put_u64(out, VSR_IO_WIRE_MAGIC);
}

/* The dialer's preamble and, in TRUSTED mode, the HELLO frame of either
 * side, written once the send slab is available. */
static bool link_write_hello(struct vsr_io *io, struct vsr_io_link *link,
                             bool preamble, bool hello)
{
    unsigned char bytes[VSR_IO_PREAMBLE_BYTES + LINK_HELLO_BYTES];
    uint32_t length = 0;

    if (preamble) {
        put_preamble(bytes);
        length += VSR_IO_PREAMBLE_BYTES;
    }
    if (hello) {
        unsigned char *body = bytes + length + VSR_IO_FRAME_HEADER_BYTES;

        vsr_io_codec_put_hello(body, io->options.handshake, link->purpose,
                               io->options.node, link->nonce);
        vsr_io_codec_put_frame(
            bytes + length, VSR_IO_FRAME_HELLO,
            sizeof(struct vsr_io_wire_hello),
            vsr_io_crc32c(0, body, sizeof(struct vsr_io_wire_hello)));
        length += LINK_HELLO_BYTES;
    }
    return link_write_control(io, link, bytes, length);
}

/* The record's descriptor: the slot once installed, else the raw one. */
static void link_sqe_fd(const struct vsr_io_link *link, struct vsr_io_sqe *sqe)
{
    if (link->fd >= 0) {
        sqe->fd = link->fd;
        sqe->flags |= VSR_IO_SQE_FIXED_FILE;
    } else {
        sqe->fd = link->raw_fd;
    }
}

/* Classifies a send by the flag rule (decision 38) and fills the record. */
static void link_send_flags(const struct vsr_io *io, struct vsr_io_send *send,
                            const struct vsr_io_vec *vecs, uint32_t count,
                            struct vsr_io_sqe *sqe)
{
    uint64_t bytes = 0;

    send->fixed = true;
    for (uint32_t i = 0; i < count; ++i) {
        bytes += vecs[i].length;
        if (!vsr_io_pool_contains(&io->pool, vecs[i].base, vecs[i].length)) {
            send->fixed = false;
        }
    }
    send->zero_copy = send->fixed || bytes >= io->options.zero_copy_bytes;
    sqe->op_flags = VSR_IO_SEND_VECTORED;
    if (send->zero_copy) {
        sqe->op_flags |= VSR_IO_SEND_ZERO_COPY;
    }
    if (send->fixed) {
        sqe->flags |= VSR_IO_SQE_FIXED_BUFFER;
        sqe->buffer_index = (uint16_t)io->pool.region_index;
    }
}

/* Position `position` of a node's send queue (0 is the oldest). */
static struct vsr_io_queued_send *node_queue_at(struct vsr_io_links *links,
                                                uint32_t node_index,
                                                uint32_t position)
{
    const struct vsr_io_node *node = &links->nodes[node_index];
    uint32_t slot = (node->queue_head + position) % links->link_queue;

    return &links->queue[(size_t)node_index * links->link_queue + slot];
}

/* The queue position of the first message not yet handed to the carrier's
 * stream (the started ones are a prefix), or the count when none. */
static uint32_t node_queue_unstarted(struct vsr_io_links *links,
                                     uint32_t node_index)
{
    const struct vsr_io_node *node = &links->nodes[node_index];
    uint32_t position = 0;

    while (position < node->queue_count &&
           node_queue_at(links, node_index, position)->end != 0) {
        position++;
    }
    return position;
}

/* Completes a queued SEND op to its replica. */
static void queued_complete(struct vsr_io *io,
                            const struct vsr_io_queued_send *queued,
                            int32_t status)
{
    struct vsr_io_completion completion = {queued->op, status, LINK_NONE, NULL};

    vsr_io_engine_complete_core(&io->replicas[queued->replica], &completion);
    if (status == VSR_IO_OK) {
        io->stats.messages_sent++;
    } else if (status == VSR_IO_RETRY) {
        io->stats.messages_retried++;
    }
}

/* Removes the message at `position`, completing it with `status`; the
 * later ones move up, and the carrier's encoder follows the message it is
 * on (never the removed one). */
static void node_queue_remove(struct vsr_io *io, uint32_t node_index,
                              uint32_t position, int32_t status)
{
    struct vsr_io_links *links = &io->links;
    struct vsr_io_node *node = &links->nodes[node_index];
    uint32_t base = node_index * links->link_queue;

    LINKS_ASSERT(position < node->queue_count);
    queued_complete(io, node_queue_at(links, node_index, position), status);
    if (node->carrier != LINK_NONE &&
        links->links[node->carrier].encoding != LINK_NONE) {
        struct vsr_io_link *carrier = &links->links[node->carrier];
        uint32_t at =
            (carrier->encoding - base + links->link_queue - node->queue_head) %
            links->link_queue;

        LINKS_ASSERT(at != position && at < node->queue_count);
        if (at > position) {
            carrier->encoding =
                base + (node->queue_head + at - 1) % links->link_queue;
        }
    }
    for (uint32_t i = position; i + 1 < node->queue_count; ++i) {
        *node_queue_at(links, node_index, i) =
            *node_queue_at(links, node_index, i + 1);
    }
    node->queue_count--;
}

/* The status of a message that will not be delivered: RETRY, or CANCELLED
 * once the engine is closing. */
static int32_t links_fail_status(const struct vsr_io_links *links)
{
    return links->closing != 0 ? VSR_IO_CANCELLED : VSR_IO_RETRY;
}

/* Sends of a link still in flight or awaiting their NOTIF. */
static uint32_t link_live_sends(const struct vsr_io_link *link)
{
    uint32_t count = 0;

    for (uint32_t i = 0; i < VSR_IO_LINK_SENDS; ++i) {
        count += link->sends[i].state != SEND_FREE ? 1 : 0;
    }
    return count;
}

/* Completes the messages nobody reads any more: a retiring one once its
 * link's notified offset passed it or that link has no live send (RETRY),
 * the carrier's once its notified offset passed it (OK). They form the
 * queue's prefix, in that order; the walk stops at the first unstarted
 * message. */
static void node_queue_ready(struct vsr_io *io, uint32_t node_index)
{
    struct vsr_io_links *links = &io->links;
    struct vsr_io_node *node = &links->nodes[node_index];
    uint32_t position = 0;

    while (position < node->queue_count) {
        const struct vsr_io_queued_send *queued =
            node_queue_at(links, node_index, position);
        const struct vsr_io_link *link;

        if (queued->end == 0) {
            return;
        }
        if (queued->retiring != 0) {
            link = &links->links[queued->retiring - 1];
            if (queued->end <= link->notified_offset ||
                link_live_sends(link) == 0) {
                node_queue_remove(io, node_index, position,
                                  links_fail_status(links));
                continue;
            }
        } else if (node->carrier != LINK_NONE) {
            link = &links->links[node->carrier];
            if (queued->end <= link->notified_offset) {
                node_queue_remove(io, node_index, position, VSR_IO_OK);
                continue;
            }
        }
        position++;
    }
}

/* Drops the messages not yet on any wire (the queue's tail), or every
 * message, retiring ones included, each completing with the failure
 * status; the latter only when the node or the engine goes away, when a
 * zero-copy send of a closing link may still read the bytes for the
 * NOTIF's short while. */
static void node_queue_drop(struct vsr_io *io, uint32_t node_index, bool all)
{
    struct vsr_io_links *links = &io->links;
    struct vsr_io_node *node = &links->nodes[node_index];
    int32_t status = links_fail_status(links);

    while (
        node->queue_count > 0 &&
        (all ||
         node_queue_at(links, node_index, node->queue_count - 1)->end == 0)) {
        node_queue_remove(io, node_index, node->queue_count - 1, status);
    }
}

/* After an election, or a dial's failure: the messages the previous
 * carrier had put on its stream retire (RETRY once its sends no longer
 * read them), a demoted link with a frame half sent or a build pending is
 * closed rather than left to corrupt its stream, and an unlinked node's
 * waiting messages are retried at once. */
static void node_queue_review(struct vsr_io *io, uint32_t node_index,
                              uint32_t old_carrier)
{
    struct vsr_io_links *links = &io->links;
    struct vsr_io_node *node = &links->nodes[node_index];

    if (old_carrier != LINK_NONE && old_carrier != node->carrier) {
        struct vsr_io_link *old = &links->links[old_carrier];
        bool torn = old->encoding != LINK_NONE || old->vec_count > 0;

        for (uint32_t i = 0; i < node->queue_count; ++i) {
            struct vsr_io_queued_send *queued =
                node_queue_at(links, node_index, i);

            if (queued->end == 0) {
                break;
            }
            if (queued->retiring == 0) {
                queued->retiring = old_carrier + 1;
            }
        }
        old->encoding = LINK_NONE;
        old->encoder.message = NULL;
        if (torn && old->state == VSR_IO_LINK_ESTABLISHED) {
            link_close(io, old_carrier, -ECANCELED);
        }
    }
    if (node->carrier == LINK_NONE && node_peer_pending(io, node_index) == 0) {
        node_queue_drop(io, node_index, false);
    }
    node_queue_ready(io, node_index);
}

/* Encodes queued messages into the build, in order, while the coalesce
 * budget, the vectors and the ring allow; a message continues in the next
 * build (the encoder keeps its position). Only the node's carrier builds. */
static void link_build_messages(struct vsr_io *io, struct vsr_io_link *link)
{
    struct vsr_io_links *links = &io->links;
    uint64_t budget = io->options.send_coalesce_bytes;

    while (link->build_bytes < budget &&
           link->vec_count < VSR_IO_SEND_VECTORS) {
        struct vsr_io_writer writer;
        uint64_t before;
        uint64_t emitted;
        uint32_t count = 0;
        bool done = false;
        int rc;

        if (link->encoding == LINK_NONE) {
            uint32_t position = node_queue_unstarted(links, link->node_index);
            struct vsr_io_queued_send *queued;

            if (position == links->nodes[link->node_index].queue_count ||
                !link_send_slab(io, link)) {
                return;
            }
            queued = node_queue_at(links, link->node_index, position);
            vsr_io_encoder_begin(&link->encoder, queued->message,
                                 queued->length, queued->crc);
            queued->end = link->stream_offset + VSR_IO_FRAME_HEADER_BYTES +
                          queued->length;
            link->encoding = (uint32_t)(queued - links->queue);
        }
        writer.capacity = link_ring_area(io, link, &writer.base);
        writer.used = 0;
        before = link->encoder.offset;
        rc = vsr_io_encoder_emit(&link->encoder, &writer,
                                 link->vecs + link->vec_count,
                                 VSR_IO_SEND_VECTORS - link->vec_count, &count,
                                 budget - link->build_bytes, &done);
        emitted = link->encoder.offset - before;
        link->header_tail += writer.used;
        link->vec_count += count;
        link->build_bytes += emitted;
        link->stream_offset += emitted;
        if (count > 0) {
            link_build_merge(link, link->vec_count - count);
        }
        if (rc != VSR_OK) {
            /* The graph changed since its digest: the core broke the pin
             * rule of vsr.h. The frame cannot be completed; the link
             * closes and the message completes RETRY with the prefix. */
            LINKS_ASSERT(false);
            link_close(io, link_index(io, link), -EBADMSG);
            return;
        }
        if (done) {
            link->encoding = LINK_NONE;
        } else if (emitted == 0) {
            return; /* No room: the ring, the vectors or the budget. */
        }
    }
}

/* One send from the build when nothing is in flight and a send entry is
 * free; the carrier of a node first extends the build with queued
 * messages. True when a record was emitted. */
static bool link_prepare_send(struct vsr_io *io, struct vsr_io_link *link,
                              struct vsr_io_sqe *sqe)
{
    struct vsr_io_send *send = NULL;
    uint32_t entry = 0;
    uint32_t slot;

    if (link->inflight != 0) {
        return false;
    }
    for (entry = 0; entry < VSR_IO_LINK_SENDS; ++entry) {
        if (link->sends[entry].state == SEND_FREE) {
            send = &link->sends[entry];
            break;
        }
    }
    if (send == NULL) {
        return false;
    }
    if (link->state == VSR_IO_LINK_ESTABLISHED &&
        link->purpose == VSR_IO_PURPOSE_PEER &&
        io->links.nodes[link->node_index].carrier == link_index(io, link)) {
        link_build_messages(io, link);
        if (link->state != VSR_IO_LINK_ESTABLISHED) {
            return false;
        }
    }
    if (link->vec_count == 0) {
        return false;
    }
    LINKS_ASSERT(link->build_bytes > 0 && link->send_slab != LINK_NONE);
    sqe->opcode = VSR_IO_SQE_SEND;
    link_sqe_fd(link, sqe);
    link_send_flags(io, send, link->vecs, link->vec_count, sqe);
    slot = vsr_io_slots_alloc(&io->slots, VSR_IO_SLOT_SEND,
                              send->zero_copy ? 2 : 1, link_index(io, link),
                              entry, 0);
    if (slot == LINK_NONE) {
        sqe->flags = 0;
        return false;
    }
    send->slot = slot;
    send->state = SEND_INFLIGHT;
    send->header_begin = link->header_sent;
    send->header_end = link->header_tail;
    send->begin = link->stream_offset - link->build_bytes;
    send->end = link->stream_offset;
    link->header_sent = link->header_tail;
    link->build_bytes = 0;
    link->inflight = 1;
    sqe->user_data = vsr_io_slots_user_data(&io->slots, slot);
    sqe->addr = link->vecs;
    sqe->length = link->vec_count;
    io->stats.sends++;
    if (send->zero_copy) {
        io->stats.sends_zero_copy++;
    }
    return true;
}

/* The ring floor and the notified offset follow the oldest live send: the
 * slab bytes below every live send's range are reusable, and the stream
 * bytes below every live send's begin are no longer read. */
static void link_send_floor(struct vsr_io_link *link)
{
    uint64_t floor = link->header_sent;
    uint64_t notified = link->sent_offset;

    for (uint32_t i = 0; i < VSR_IO_LINK_SENDS; ++i) {
        const struct vsr_io_send *send = &link->sends[i];

        if (send->state == SEND_FREE) {
            continue;
        }
        if (send->header_begin < floor) {
            floor = send->header_begin;
        }
        if (send->begin < notified) {
            notified = send->begin;
        }
    }
    link->header_head = floor;
    link->notified_offset = notified;
}

/* Send progress reaches its consumers: a peer link's node completes the
 * queued messages nobody reads any more (a closing link's NOTIFs release
 * retiring ones), a stream link tells its stream. */
static void link_send_progress(struct vsr_io *io, struct vsr_io_link *link)
{
    if (link->node_index == LINK_NONE) {
        return;
    }
    if (link->purpose == VSR_IO_PURPOSE_PEER) {
        node_queue_ready(io, link->node_index);
    } else if (link->stream != LINK_NONE &&
               link->state == VSR_IO_LINK_ESTABLISHED) {
        vsr_io_streams_sent(io, link->stream, link->notified_offset);
    }
}

/* The entry's bytes are no longer read; its slot is consumed by the
 * dispatcher, which clears `slot` once the table freed it. */
static void link_send_release(struct vsr_io *io, struct vsr_io_link *link,
                              struct vsr_io_send *send)
{
    send->state = SEND_FREE;
    link_send_floor(link);
    link_send_progress(io, link);
}

/* A short result: the first `sent` bytes leave the vectors, the rest stay
 * as the build. */
static void link_send_remainder(struct vsr_io_link *link, uint64_t sent)
{
    uint32_t first = 0;

    while (first < link->vec_count && sent >= link->vecs[first].length) {
        sent -= link->vecs[first].length;
        first++;
    }
    LINKS_ASSERT(first < link->vec_count);
    if (sent > 0) {
        link->vecs[first].base = (unsigned char *)link->vecs[first].base + sent;
        link->vecs[first].length -= (size_t)sent;
    }
    memmove(link->vecs, link->vecs + first,
            (link->vec_count - first) * sizeof(link->vecs[0]));
    link->vec_count -= first;
}

/* The result completion of a send: a failure closes the link; a short
 * result hands the rest back to the build; the NOTIF (or a plain send's
 * completion) releases the entry. */
static void link_send_complete(struct vsr_io *io, struct vsr_io_link *link,
                               uint32_t entry, const struct vsr_io_cqe *cqe)
{
    struct vsr_io_send *send = &link->sends[entry];
    uint64_t total = send->end - send->begin;
    uint64_t sent;

    if ((cqe->flags & VSR_IO_CQE_NOTIF) != 0) {
        LINKS_ASSERT(send->state == SEND_NOTIF);
        link_send_release(io, link, send);
        return;
    }
    LINKS_ASSERT(send->state == SEND_INFLIGHT && link->inflight != 0);
    link->inflight = 0;
    if (cqe->result < 0) {
        /* A rejected zero-copy send completes once, without MORE
         * (decision 62); an accepted one still gets its NOTIF. The entry
         * is released like at a NOTIF: the link may be closing already
         * (a demotion, a caller's close), when nothing else would
         * complete the retiring messages its sends covered. */
        link->vec_count = 0;
        if (send->zero_copy && (cqe->flags & VSR_IO_CQE_MORE) != 0) {
            send->state = SEND_NOTIF;
        } else {
            link_send_release(io, link, send);
        }
        link_close(io, link_index(io, link), cqe->result);
        return;
    }
    sent = (uint64_t)cqe->result < total ? (uint64_t)cqe->result : total;
    if (sent < total) {
        /* Short: the unsent tail of the vectors is the next build, its
         * header bytes reserved from the send's own until it goes out. */
        link_send_remainder(link, sent);
        send->end = send->begin + sent;
        link->build_bytes = total - sent;
        link->header_sent = send->header_begin;
    } else {
        link->vec_count = 0;
    }
    link->sent_offset = send->end;
    link->last_active_ns = io->now;
    io->stats.bytes_sent += sent;
    if (send->zero_copy) {
        send->state = SEND_NOTIF;
        link_send_floor(link);
        link_send_progress(io, link);
    } else {
        link_send_release(io, link, send);
    }
}

/* -------------------------------------------------------------------------
 * Handshake
 * ---------------------------------------------------------------------- */

static void link_arm_handshake(struct vsr_io *io, struct vsr_io_link *link)
{
    vsr_io_deadlines_arm(&io->deadlines, link->deadline,
                         io->now + io->options.handshake_timeout_ns);
}

/* Takes the raw descriptor over into an engine file slot; false (and the
 * link closing) when no slot is free or the executor refuses. */
static bool link_install(struct vsr_io *io, struct vsr_io_link *link)
{
    uint32_t slot = vsr_io_engine_slot_alloc(io);
    int rc;

    LINKS_ASSERT(link->raw_fd >= 0 && link->fd < 0);
    if (slot == LINK_NONE) {
        link_close(io, link_index(io, link), -ENFILE);
        return false;
    }
    rc = vsr_io_engine_install(io, slot, link->raw_fd);
    if (rc < 0) {
        vsr_io_engine_slot_free(io, slot);
        link_close(io, link_index(io, link), rc);
        return false;
    }
    link->fd = (int32_t)slot;
    link->raw_fd = -1;
    return true;
}

/* A connected socket (dialed, accepted or adopted) enters the configured
 * handshake: TRUSTED installs it and exchanges HELLOs on the slot; EXTERNAL
 * exchanges the preamble on the raw descriptor, then hands it to the
 * caller. The link is FREE or CONNECTING on entry. */
static void link_start_handshake(struct vsr_io *io, struct vsr_io_link *link)
{
    bool dialer = link->direction == VSR_IO_OUTBOUND;

    if (link->state == VSR_IO_LINK_CONNECTING) {
        /* From CONNECTING to the handshake without leaving pending. */
        link->state = VSR_IO_LINK_FREE;
        io->links.pending--;
        if (link->node_index != LINK_NONE) {
            io->links.nodes[link->node_index].pending--;
        }
    }
    if (io->options.handshake == VSR_IO_HANDSHAKE_TRUSTED) {
        link_enter_pending(io, link, VSR_IO_LINK_HELLO);
        link->preamble_seen = dialer ? VSR_IO_PREAMBLE_BYTES : 0;
        link_arm_handshake(io, link);
        (void)link_install(io, link);
        return;
    }
    link_enter_pending(io, link, VSR_IO_LINK_EXTERNAL);
    link->stage =
        dialer ? VSR_IO_STAGE_PREAMBLE_SEND : VSR_IO_STAGE_PREAMBLE_RECV;
    link->preamble_seen = 0;
    link_arm_handshake(io, link);
}

/* Emits the HANDSHAKE op for an EXTERNAL link whose preamble went out or
 * came in; the ring may be full, in which case poll retries. */
static void link_forward_handshake(struct vsr_io *io, struct vsr_io_link *link)
{
    struct vsr_io_forwarded *entry;

    LINKS_ASSERT(link->state == VSR_IO_LINK_EXTERNAL &&
                 link->stage == VSR_IO_STAGE_HANDSHAKE &&
                 link->handshake_op == 0);
    entry = vsr_io_forward(io, NULL, VSR_IO_OP_HANDSHAKE);
    if (entry == NULL) {
        return;
    }
    io->links.handshake_ops++;
    link->handshake_op = io->links.handshake_ops;
    io->links.handshakes_due--;
    entry->op.op.id = link->handshake_op;
    entry->op.op.data = &entry->rail.handshake;
    entry->rail.handshake.fd = link->raw_fd;
    entry->rail.handshake.direction = link->direction;
    entry->rail.handshake.node = link->node;
    entry->rail.handshake.peer = link->peer;
}

static void link_want_handshake(struct vsr_io *io, struct vsr_io_link *link)
{
    link->stage = VSR_IO_STAGE_HANDSHAKE;
    io->links.handshakes_due++;
    link_forward_handshake(io, link);
}

/* The peer's HELLO: mode and purpose must match, the node must be the
 * dialed one (outbound) or a known one (inbound). The acceptor answers with
 * its own HELLO and both sides are established. */
static void link_hello(struct vsr_io *io, struct vsr_io_link *link,
                       const struct vsr_io_wire_hello *hello)
{
    uint32_t index = link_index(io, link);
    uint32_t node_index;

    if (link->state != VSR_IO_LINK_HELLO || link->hello_seen ||
        hello->handshake != io->options.handshake ||
        hello->node == VSR_IO_NO_NODE) {
        io->stats.frames_rejected++;
        link_close(io, index, -EPROTO);
        return;
    }
    if (link->direction == VSR_IO_OUTBOUND) {
        if (hello->purpose != link->purpose || hello->node != link->node) {
            io->stats.frames_rejected++;
            link_close(io, index, -EPROTO);
            return;
        }
        link->hello_seen = true;
        io->links.nodes[link->node_index].last_received_ns = io->now;
        link_establish(io, link);
        return;
    }
    node_index = vsr_io_links_node_index(&io->links, hello->node);
    if (node_index == LINK_NONE || hello->node == io->options.node) {
        /* Unknown nodes cannot be authorized for anything (decision 73),
         * and nothing may claim to be this engine: it never dials itself,
         * so such a link could only inject its own replicas' frames. */
        io->stats.frames_rejected++;
        link_close(io, index, -EPROTO);
        return;
    }
    link->purpose = hello->purpose;
    link->hello_seen = true;
    link_identify(io, link, node_index);
    io->links.nodes[node_index].last_received_ns = io->now;
    link_establish(io, link);
}

int vsr_io_links_handshake_done(struct vsr_io *io, uint64_t op, int32_t status,
                                const struct vsr_io_handshake_done *done)
{
    struct vsr_io_links *links = &io->links;
    struct vsr_io_link *link = NULL;
    uint32_t index;

    if (op == 0) {
        return VSR_EINVAL;
    }
    for (index = 0; index < links->links_count; ++index) {
        if (links->links[index].handshake_op == op) {
            link = &links->links[index];
            break;
        }
    }
    if (link == NULL) {
        return VSR_EINVAL;
    }
    /* The descriptor is the engine's again, whatever the outcome. */
    link->handshake_op = 0;
    if (link->state == VSR_IO_LINK_CLOSING) {
        return VSR_OK;
    }
    LINKS_ASSERT(link->state == VSR_IO_LINK_EXTERNAL);
    if (status != VSR_IO_OK || done == NULL || done->node == VSR_IO_NO_NODE) {
        link_close(io, index, status == VSR_IO_OK ? VSR_EINVAL : -EACCES);
        return VSR_OK;
    }
    if (link->direction == VSR_IO_OUTBOUND) {
        if (done->node != link->node) {
            link_close(io, index, -EACCES);
            return VSR_OK;
        }
    } else {
        uint32_t node_index = vsr_io_links_node_index(links, done->node);

        /* Unknown, or this engine's own identity (a reflected handshake,
         * say): refused like the HELLO's claim. */
        if (node_index == LINK_NONE || done->node == io->options.node) {
            link_close(io, index, -EACCES);
            return VSR_OK;
        }
        link_identify(io, link, node_index);
    }
    /* The caller's protocol replaced the HELLO exchange. */
    link->hello_sent = true;
    link->hello_seen = true;
    link->preamble_seen = VSR_IO_PREAMBLE_BYTES;
    if (!link_install(io, link)) {
        return VSR_OK;
    }
    link_establish(io, link);
    return VSR_OK;
}

/* -------------------------------------------------------------------------
 * Receive path: preamble and frames
 * ---------------------------------------------------------------------- */

/* The envelope's cluster and sender, read without decoding the body. */
static bool link_envelope(const struct vsr_io_cursor *body,
                          struct vsr_id *cluster, uint64_t *from)
{
    struct vsr_io_cursor cursor = *body;
    uint64_t skip;

    return vsr_io_cursor_u64(&cursor, &cluster->hi) &&
           vsr_io_cursor_u64(&cursor, &cluster->lo) &&
           vsr_io_cursor_u64(&cursor, &skip) &&
           vsr_io_cursor_u64(&cursor, &skip) &&
           vsr_io_cursor_u64(&cursor, from);
}

/*
 * A MESSAGE on an established peer link (decision 67): the sender named
 * by the envelope must be the replica the link's node is authorized for
 * in the envelope's cluster, and the cluster must be one of this engine's
 * replicas; either failure, like a body too short for its envelope, drops
 * the frame and counts it, since the stream itself is intact. False when
 * the replica has no free region: the bytes stay in place for a retry.
 */
static bool link_message(struct vsr_io *io, struct vsr_io_link *link,
                         const struct vsr_io_cursor *body)
{
    struct vsr_id cluster;
    uint64_t from = 0;
    struct vsr_io_replica *replica;

    if (!link_envelope(body, &cluster, &from)) {
        io->stats.frames_rejected++;
        return true;
    }
    replica = vsr_io_engine_replica(io, cluster);
    if (replica == NULL ||
        vsr_io_links_lookup(&io->links, cluster, from) != link->node) {
        io->stats.frames_rejected++;
        return true;
    }
    if (!vsr_io_engine_deliver(io, replica, body, link->partial_slab)) {
        return false;
    }
    io->links.nodes[link->node_index].last_received_ns = io->now;
    return true;
}

/* Frame dispatch: HELLO drives the handshake, MESSAGE is delivered to its
 * replica, the stream kinds go to the stream module. False when the frame
 * could not be consumed yet (a MESSAGE without a region, a stream without
 * a unit); the caller keeps its bytes and retries at poll. */
static bool link_frame(struct vsr_io *io, struct vsr_io_link *link,
                       const struct vsr_io_wire_frame *frame,
                       const struct vsr_io_cursor *body)
{
    struct vsr_io_cursor cursor = *body;
    struct vsr_io_wire_hello hello;

    switch (frame->kind) {
    case VSR_IO_FRAME_HELLO:
        if (vsr_io_codec_get_hello(&cursor, &hello) != VSR_OK) {
            io->stats.frames_rejected++;
            link_close(io, link_index(io, link), -EBADMSG);
            return true;
        }
        link_hello(io, link, &hello);
        return true;
    case VSR_IO_FRAME_MESSAGE:
        if (link->state != VSR_IO_LINK_ESTABLISHED ||
            link->purpose != VSR_IO_PURPOSE_PEER) {
            io->stats.frames_rejected++;
            link_close(io, link_index(io, link), -EPROTO);
            return true;
        }
        return link_message(io, link, body);
    default:
        if (link->state != VSR_IO_LINK_ESTABLISHED ||
            link->purpose != VSR_IO_PURPOSE_STREAM ||
            frame->kind < VSR_IO_FRAME_STREAM_REQUEST ||
            frame->kind > VSR_IO_FRAME_STREAM_END) {
            io->stats.frames_rejected++;
            link_close(io, link_index(io, link), -EPROTO);
            return true;
        }
        if (!vsr_io_streams_frame(io, link_index(io, link), frame->kind, body,
                                  link->partial_slab)) {
            return false;
        }
        if (link->node_index != LINK_NONE) {
            io->links.nodes[link->node_index].last_received_ns = io->now;
        }
        return true;
    }
}

enum carve_result {
    CARVE_EMPTY,      /* Every byte of the partial run was consumed. */
    CARVE_INCOMPLETE, /* A frame needs more bytes than the run holds. */
    CARVE_BLOCKED,    /* A whole frame could not be delivered yet. */
    CARVE_CLOSED      /* The link was closed; nothing is held. */
};

/*
 * Carves the bytes held in the link's partial run (one slab, contiguous):
 * the preamble while one is expected, then whole frames, each decoded only
 * when its header and body are both present. Bytes of an incomplete frame
 * stay in place with the slab referenced; the next delivery continues them
 * when it lands right after them in the same slab, or is copied behind
 * them in a reassembly slab otherwise (link_drain).
 */
static enum carve_result link_carve(struct vsr_io *io, struct vsr_io_link *link)
{
    uint32_t index = link_index(io, link);
    unsigned char preamble[VSR_IO_PREAMBLE_BYTES];

    put_preamble(preamble);
    while (link->partial_length > 0) {
        const unsigned char *bytes =
            vsr_io_pool_slab(&io->pool, link->partial_slab) +
            link->partial_offset;
        struct vsr_io_cursor cursor;
        struct vsr_io_wire_frame frame;
        uint32_t crc = 0;
        uint32_t consumed;

        if (link->preamble_seen < VSR_IO_PREAMBLE_BYTES) {
            if (bytes[0] != preamble[link->preamble_seen]) {
                io->stats.frames_rejected++;
                link_close(io, index, -EPROTO);
                return CARVE_CLOSED;
            }
            link->preamble_seen++;
            consumed = 1;
        } else {
            if (link->partial_length < VSR_IO_FRAME_HEADER_BYTES) {
                return CARVE_INCOMPLETE;
            }
            vsr_io_cursor_init_one(&cursor, bytes, link->partial_length);
            if (vsr_io_codec_get_frame(&cursor,
                                       (uint32_t)(io->links.frame_limit -
                                                  VSR_IO_FRAME_HEADER_BYTES),
                                       &frame) != VSR_OK) {
                io->stats.frames_rejected++;
                link_close(io, index, -EBADMSG);
                return CARVE_CLOSED;
            }
            if (vsr_io_cursor_remaining(&cursor) < frame.length) {
                return CARVE_INCOMPLETE; /* The body is still arriving. */
            }
            vsr_io_cursor_init_one(&cursor, bytes + VSR_IO_FRAME_HEADER_BYTES,
                                   frame.length);
            if (!vsr_io_cursor_crc(&cursor, frame.length, &crc) ||
                crc != frame.body_crc) {
                io->stats.frames_rejected++;
                link_close(io, index, -EBADMSG);
                return CARVE_CLOSED;
            }
            if (!link_frame(io, link, &frame, &cursor)) {
                return CARVE_BLOCKED;
            }
            consumed = VSR_IO_FRAME_HEADER_BYTES + frame.length;
        }
        if (link->state == VSR_IO_LINK_CLOSING) {
            return CARVE_CLOSED; /* link_close released the partial. */
        }
        link->partial_offset += consumed;
        link->partial_length -= consumed;
    }
    if (link->partial_slab != LINK_NONE) {
        vsr_io_pool_release(&io->pool, link->partial_slab);
        if (link->reassembly_slab == link->partial_slab) {
            link->reassembly_slab = LINK_NONE;
        }
        link->partial_slab = LINK_NONE;
    }
    return CARVE_EMPTY;
}

/* Bytes the partial run's frame still lacks: those of its header first,
 * then of the body the header announces. */
static uint32_t link_frame_missing(const struct vsr_io *io,
                                   const struct vsr_io_link *link)
{
    const unsigned char *bytes =
        vsr_io_pool_slab(&io->pool, link->partial_slab) + link->partial_offset;
    struct vsr_io_cursor cursor;
    struct vsr_io_wire_frame frame;
    uint32_t whole;

    if (link->partial_length < VSR_IO_FRAME_HEADER_BYTES) {
        return VSR_IO_FRAME_HEADER_BYTES - link->partial_length;
    }
    vsr_io_cursor_init_one(&cursor, bytes, link->partial_length);
    if (vsr_io_codec_get_frame(
            &cursor,
            (uint32_t)(io->links.frame_limit - VSR_IO_FRAME_HEADER_BYTES),
            &frame) != VSR_OK) {
        return 0; /* Carving rejects it. */
    }
    whole = VSR_IO_FRAME_HEADER_BYTES + frame.length;
    return whole > link->partial_length ? whole - link->partial_length : 0;
}

/*
 * Continues the partial run's incomplete frame with the first held run:
 * the partial bytes move into a reassembly slab acquired for the frame
 * (once), then the frame's missing bytes are copied behind them from the
 * held run, header first so that its length is known, never past the
 * frame's end, so the bytes that follow stay in place for zero-copy
 * carving. A frame fits one slab (frame_limit <= slab_bytes) and the run
 * starts at offset 0 of a fresh slab, so the copy always fits. False when
 * no slab is free: the runs stay held and poll retries.
 */
static bool link_reassemble(struct vsr_io *io, struct vsr_io_link *link)
{
    struct vsr_io_run *run = &link->held[0];
    unsigned char *target;
    uint32_t missing;

    LINKS_ASSERT(link->partial_length > 0 && link->held_count > 0);
    if (link->reassembly_slab == LINK_NONE) {
        uint32_t slab = vsr_io_pool_acquire(&io->pool, false);

        if (slab == LINK_NONE) {
            return false;
        }
        memcpy(vsr_io_pool_slab(&io->pool, slab),
               vsr_io_pool_slab(&io->pool, link->partial_slab) +
                   link->partial_offset,
               link->partial_length);
        vsr_io_pool_release(&io->pool, link->partial_slab);
        link->reassembly_slab = slab;
        link->partial_slab = slab;
        link->partial_offset = 0;
        io->links.reassembled++;
    }
    LINKS_ASSERT(link->partial_slab == link->reassembly_slab &&
                 link->partial_offset == 0);
    missing = link_frame_missing(io, link);
    if (missing > run->length) {
        missing = run->length;
    }
    LINKS_ASSERT(link->partial_length + missing <= io->pool.slab_bytes);
    target =
        vsr_io_pool_slab(&io->pool, link->partial_slab) + link->partial_length;
    memcpy(target, vsr_io_pool_slab(&io->pool, run->slab) + run->offset,
           missing);
    link->partial_length += missing;
    run->offset += missing;
    run->length -= missing;
    if (run->length == 0) {
        vsr_io_pool_release(&io->pool, run->slab);
        link->held_count--;
        memmove(&link->held[0], &link->held[1],
                link->held_count * sizeof(link->held[0]));
    }
    return true;
}

/*
 * Carves everything the link holds: the partial run, then each held run in
 * turn once the partial is consumed, reassembling a frame that straddles
 * runs. Stops, with `retry` set for poll, when a frame cannot be delivered
 * or reassembled yet.
 */
static void link_drain(struct vsr_io *io, struct vsr_io_link *link)
{
    for (;;) {
        enum carve_result result = link_carve(io, link);

        if (result == CARVE_CLOSED) {
            return;
        }
        if (result == CARVE_EMPTY) {
            if (link->held_count == 0) {
                link_set_retry(io, link, false);
                return;
            }
            link->partial_slab = link->held[0].slab;
            link->partial_offset = link->held[0].offset;
            link->partial_length = link->held[0].length;
            link->held_count--;
            memmove(&link->held[0], &link->held[1],
                    link->held_count * sizeof(link->held[0]));
            continue;
        }
        if (result == CARVE_BLOCKED) {
            link_set_retry(io, link, true);
            return;
        }
        LINKS_ASSERT(result == CARVE_INCOMPLETE);
        if (link->held_count == 0) {
            link_set_retry(io, link, false);
            return; /* The rest is still to arrive. */
        }
        if (!link_reassemble(io, link)) {
            link_set_retry(io, link, true);
            return;
        }
    }
}

/* A multishot RECV delivery of `bytes` at the slab's consumed offset. */
static void link_received(struct vsr_io *io, struct vsr_io_link *link,
                          uint16_t slab, uint32_t bytes)
{
    uint32_t offset = vsr_io_pool_recv_begin(&io->pool, slab, bytes);
    struct vsr_io_run *last =
        link->held_count > 0 ? &link->held[link->held_count - 1] : NULL;

    io->stats.bytes_received += bytes;
    link->last_active_ns = io->now;
    if (link->partial_length == 0) {
        LINKS_ASSERT(link->partial_slab == LINK_NONE && link->held_count == 0);
        link->partial_slab = slab;
        link->partial_offset = offset;
        link->partial_length = bytes;
    } else if (last == NULL && link->partial_slab == slab &&
               link->partial_offset + link->partial_length == offset) {
        /* Continues the partial run in the same slab: one reference keeps
         * the slab, the delivery's own is dropped. */
        link->partial_length += bytes;
        vsr_io_pool_release(&io->pool, slab);
    } else if (last != NULL && last->slab == slab &&
               last->offset + last->length == offset) {
        last->length += bytes;
        vsr_io_pool_release(&io->pool, slab);
    } else if (link->held_count < VSR_IO_LINK_HELD) {
        last = &link->held[link->held_count++];
        last->slab = slab;
        last->offset = offset;
        last->length = bytes;
    } else {
        /* Every held run is a slab the pool could not replace: the stream
         * cannot be kept intact without unbounded memory. */
        vsr_io_pool_release(&io->pool, slab);
        link_close(io, link_index(io, link), -ENOBUFS);
        return;
    }
    link_drain(io, link);
}

/* -------------------------------------------------------------------------
 * Dialing and accepting
 * ---------------------------------------------------------------------- */

/* Starts a dial: a CONNECTING link whose SOCKET goes out at prepare. */
static void node_dial(struct vsr_io *io, uint32_t node_index)
{
    struct vsr_io_node *node = &io->links.nodes[node_index];
    uint32_t index = link_alloc(io, VSR_IO_PURPOSE_PEER, VSR_IO_OUTBOUND,
                                node_index, node->id);
    struct vsr_io_link *link;

    if (index == LINK_NONE) {
        /* No link entry: try again after a backoff. */
        node_backoff(io, node_index, -ENFILE);
        node_want_dial(io, node_index);
        return;
    }
    link = &io->links.links[index];
    link->peer = node->address;
    link->stage = VSR_IO_STAGE_SOCKET;
    node->dialing = true;
    link_enter_pending(io, link, VSR_IO_LINK_CONNECTING);
}

/* A LINK_WANTED op for a caller-dialed node, counted as an attempt. */
static void node_link_wanted(struct vsr_io *io, uint32_t node_index)
{
    struct vsr_io_node *node = &io->links.nodes[node_index];
    struct vsr_io_forwarded *entry =
        vsr_io_forward(io, NULL, VSR_IO_OP_LINK_WANTED);

    if (entry == NULL) {
        /* Ring full: stays due for the next poll. */
        node->due = true;
        io->links.dials_due++;
        return;
    }
    node_backoff(io, node_index, 0);
    node->dialing = true;
    entry->op.op.id = 0;
    entry->op.op.data = &entry->rail.wanted;
    entry->rail.wanted.node = node->id;
    entry->rail.wanted.attempt = node->attempts;
    vsr_io_deadlines_arm(&io->deadlines, dial_handle(io, node_index),
                         node->next_dial_ns);
}

/* An accepted raw descriptor becomes an inbound link in the handshake, or
 * is closed when no link is free or the engine is closing. */
static void listener_accepted(struct vsr_io *io, int fd)
{
    struct vsr_io_links *links = &io->links;

    if (!links->closing) {
        uint32_t index = link_alloc(io, VSR_IO_PURPOSE_PEER, VSR_IO_INBOUND,
                                    LINK_NONE, VSR_IO_NO_NODE);

        if (index != LINK_NONE) {
            links->links[index].raw_fd = fd;
            link_start_handshake(io, &links->links[index]);
            return;
        }
    }
    for (uint32_t i = 0; i < VSR_IO_LINK_ORPHANS; ++i) {
        if (links->orphans[i] < 0) {
            links->orphans[i] = fd;
            links->orphans_count++;
            return;
        }
    }
    /* No room to remember it either: the descriptor leaks. A listener
     * delivers at most a batch of accepts between two prepares, and the
     * link table is sized for the peers, so this needs a flood. */
}

int vsr_io_links_adopt(struct vsr_io *io, int fd, uint64_t node, uint32_t flags)
{
    struct vsr_io_links *links = &io->links;
    bool handshake = (flags & VSR_IO_ADOPT_HANDSHAKE) != 0;
    bool outbound = (flags & VSR_IO_ADOPT_OUTBOUND) != 0;
    uint32_t node_index = vsr_io_links_node_index(links, node);
    uint32_t index;
    struct vsr_io_link *link;

    if (fd < 0 ||
        (flags & ~(uint32_t)(VSR_IO_ADOPT_HANDSHAKE | VSR_IO_ADOPT_OUTBOUND)) !=
            0 ||
        (outbound && !handshake) || links->closing) {
        return VSR_EINVAL;
    }
    if (handshake && !outbound) {
        if (node != VSR_IO_NO_NODE) {
            return VSR_EINVAL;
        }
    } else if (node_index == LINK_NONE || node == io->options.node) {
        return VSR_EINVAL; /* Unknown, or the engine itself. */
    }
    index =
        link_alloc(io, VSR_IO_PURPOSE_PEER,
                   !handshake || outbound ? VSR_IO_OUTBOUND : VSR_IO_INBOUND,
                   node_index, node);
    if (index == LINK_NONE) {
        return VSR_ELIMIT;
    }
    link = &links->links[index];
    link->raw_fd = fd;
    if (handshake) {
        link_start_handshake(io, link);
        return VSR_OK;
    }
    /* Authenticated by the caller: established at once. */
    link_enter_pending(io, link, VSR_IO_LINK_HELLO);
    if (!link_install(io, link)) {
        return VSR_OK;
    }
    link->preamble_seen = VSR_IO_PREAMBLE_BYTES;
    link->hello_seen = true;
    link->hello_sent = true;
    link_establish(io, link);
    return VSR_OK;
}

/* -------------------------------------------------------------------------
 * Sends from the core and streams
 * ---------------------------------------------------------------------- */

int vsr_io_links_send(struct vsr_io *io, uint32_t replica, uint64_t op,
                      const struct vsr_message *message, uint64_t member)
{
    struct vsr_io_links *links = &io->links;
    struct vsr_io_replica *entry;
    struct vsr_io_node *node;
    struct vsr_io_queued_send *queued;
    uint64_t node_id;
    uint32_t node_index;
    uint32_t length = 0;
    uint32_t crc = 0;
    int rc;

    if (replica >= io->options.limits.replicas || message == NULL ||
        io->replicas[replica].state == VSR_IO_REPLICA_FREE || links->closing) {
        io->stats.messages_retried++;
        return VSR_IO_RETRY;
    }
    entry = &io->replicas[replica];
    node_id = vsr_io_links_lookup(links, entry->options.cluster, member);
    node_index = vsr_io_links_node_index(links, node_id);
    if (node_index == LINK_NONE || node_id == io->options.node) {
        /* Unknown, unauthorized or own destination. */
        io->stats.messages_retried++;
        return VSR_IO_RETRY;
    }
    node = &links->nodes[node_index];
    node_want(io, node_index); /* A SEND wants a link to its node. */
    rc = vsr_io_codec_message_digest(message, &entry->options.limits, &length,
                                     &crc);
    if (rc != VSR_OK) {
        /* Malformed or beyond the limits: the core retries its protocol
         * work on any SEND failure, so RETRY says it all. */
        io->stats.messages_retried++;
        return VSR_IO_RETRY;
    }
    if (node->queue_count == links->link_queue) {
        /* Full: the oldest message not on the wire yields, or this one. */
        uint32_t position = node_queue_unstarted(links, node_index);

        if (position == node->queue_count) {
            io->stats.messages_retried++;
            return VSR_IO_RETRY;
        }
        node_queue_remove(io, node_index, position, VSR_IO_RETRY);
    }
    queued = node_queue_at(links, node_index, node->queue_count);
    queued->op = op;
    queued->replica = replica;
    queued->retiring = 0;
    queued->message = message;
    queued->length = length;
    queued->crc = crc;
    queued->end = 0;
    node->queue_count++;
    return VSR_OK;
}

int vsr_io_links_open_stream(struct vsr_io *io, uint64_t node, uint32_t stream,
                             uint32_t *link)
{
    struct vsr_io_links *links = &io->links;
    uint32_t node_index = vsr_io_links_node_index(links, node);
    struct vsr_io_link *entry;
    uint32_t index;

    if (link == NULL) {
        return VSR_EINVAL;
    }
    *link = LINK_NONE;
    if (node_index == LINK_NONE || !links->nodes[node_index].has_address ||
        node == io->options.node || links->closing) {
        return VSR_EINVAL;
    }
    index = link_alloc(io, VSR_IO_PURPOSE_STREAM, VSR_IO_OUTBOUND, node_index,
                       node);
    if (index == LINK_NONE) {
        return VSR_ELIMIT;
    }
    entry = &links->links[index];
    entry->stream = stream;
    entry->peer = links->nodes[node_index].address;
    entry->stage = VSR_IO_STAGE_SOCKET;
    link_enter_pending(io, entry, VSR_IO_LINK_CONNECTING);
    *link = index;
    return VSR_OK;
}

int vsr_io_links_send_frame(struct vsr_io *io, uint32_t link, uint16_t kind,
                            const unsigned char *header, size_t header_bytes,
                            const struct vsr_io_vec *payload, uint32_t count,
                            uint32_t body_crc, uint64_t *end)
{
    static const unsigned char zeros[8];
    struct vsr_io_link *entry;
    unsigned char *base;
    uint64_t total = header_bytes;
    uint32_t vectors = 1; /* The header run; payload runs and a pad. */
    uint32_t pad;
    uint32_t need;
    bool entry_free = false;

    if (end == NULL) {
        return VSR_EINVAL;
    }
    *end = 0;
    if (link >= io->links.links_count || (header == NULL && header_bytes > 0) ||
        (payload == NULL && count > 0) ||
        (kind != VSR_IO_FRAME_STREAM_REQUEST &&
         kind != VSR_IO_FRAME_STREAM_CHUNK &&
         kind != VSR_IO_FRAME_STREAM_END)) {
        return VSR_EINVAL;
    }
    entry = &io->links.links[link];
    if (entry->state != VSR_IO_LINK_ESTABLISHED ||
        entry->purpose != VSR_IO_PURPOSE_STREAM) {
        return VSR_EINVAL;
    }
    for (uint32_t i = 0; i < count; ++i) {
        total += payload[i].length;
        if (payload[i].length > 0) {
            vectors++;
        }
    }
    if (total > io->links.frame_limit - VSR_IO_FRAME_HEADER_BYTES) {
        return VSR_ELIMIT;
    }
    pad = (uint32_t)((8 - total % 8) % 8);
    if (pad > 0 && vectors > 1) {
        vectors++; /* The pad follows the payload, from the ring. */
    }
    if (total + pad > io->links.frame_limit - VSR_IO_FRAME_HEADER_BYTES) {
        return VSR_ELIMIT;
    }
    for (uint32_t i = 0; i < VSR_IO_LINK_SENDS; ++i) {
        entry_free = entry_free || entry->sends[i].state == SEND_FREE;
    }
    if (entry->inflight != 0 || !entry_free ||
        (entry->build_bytes > 0 &&
         entry->build_bytes + VSR_IO_FRAME_HEADER_BYTES + total + pad >
             io->options.send_coalesce_bytes) ||
        entry->vec_count + vectors > VSR_IO_SEND_VECTORS ||
        !link_send_slab(io, entry)) {
        return VSR_EBUSY;
    }
    need = VSR_IO_FRAME_HEADER_BYTES + (uint32_t)header_bytes + pad;
    if (link_ring_area(io, entry, &base) < need) {
        return VSR_EBUSY;
    }
    if (pad > 0) {
        body_crc = vsr_io_crc32c(body_crc, zeros, pad);
    }
    vsr_io_codec_put_frame(base, kind, (uint32_t)(total + pad), body_crc);
    memcpy(base + VSR_IO_FRAME_HEADER_BYTES, header, header_bytes);
    memset(base + VSR_IO_FRAME_HEADER_BYTES + header_bytes, 0, pad);
    entry->header_tail += need;
    if (vectors == 1) {
        link_build_append(entry, base, need);
    } else {
        link_build_append(entry, base,
                          VSR_IO_FRAME_HEADER_BYTES + header_bytes);
        for (uint32_t i = 0; i < count; ++i) {
            if (payload[i].length > 0) {
                link_build_append(entry, payload[i].base, payload[i].length);
            }
        }
        if (pad > 0) {
            link_build_append(
                entry, base + VSR_IO_FRAME_HEADER_BYTES + header_bytes, pad);
        }
    }
    *end = entry->stream_offset;
    return VSR_OK;
}

/* -------------------------------------------------------------------------
 * Poll and deadlines
 * ---------------------------------------------------------------------- */

void vsr_io_links_poll(struct vsr_io *io, uint64_t now)
{
    struct vsr_io_links *links = &io->links;

    io->now = now;
    if (links->dials_due > 0) {
        for (uint32_t i = 0; i < links->nodes_count && links->dials_due > 0;
             ++i) {
            struct vsr_io_node *node = &links->nodes[i];

            if (!node->due) {
                continue;
            }
            node->due = false;
            links->dials_due--;
            if (links->closing || !node->wanted || node->carrier != LINK_NONE ||
                node->dialing || node_peer_pending(io, i) > 0) {
                continue;
            }
            if (node->has_address) {
                node_dial(io, i);
            } else {
                node_link_wanted(io, i);
            }
        }
    }
    if (links->handshakes_due > 0) {
        for (uint32_t i = 0;
             i < links->links_count && links->handshakes_due > 0; ++i) {
            struct vsr_io_link *link = &links->links[i];

            if (link->state == VSR_IO_LINK_EXTERNAL &&
                link->stage == VSR_IO_STAGE_HANDSHAKE &&
                link->handshake_op == 0) {
                link_forward_handshake(io, link);
            }
        }
    }
    if (links->retries_due > 0) {
        /* Held bytes whose carving stopped short: a region or a slab may
         * have been freed since. */
        for (uint32_t i = 0; i < links->links_count; ++i) {
            struct vsr_io_link *link = &links->links[i];

            if (link->retry) {
                link_drain(io, link);
            }
        }
    }
}

void vsr_io_links_deadline(struct vsr_io *io, uint16_t kind, uint32_t index,
                           uint64_t now)
{
    struct vsr_io_links *links = &io->links;

    io->now = now;
    if (kind == VSR_IO_DEADLINE_DIAL) {
        struct vsr_io_node *node;

        if (index >= links->nodes_count) {
            return;
        }
        node = &links->nodes[index];
        if (node_peer_pending(io, index) == 0) {
            node->dialing = false; /* A LINK_WANTED's wait is over. */
            if (!node->has_address && node->carrier == LINK_NONE) {
                /* Unanswered: the waiting messages retry; those a lost
                 * link's kernel may still read wait for its NOTIFs. */
                node_queue_drop(io, index, false);
            }
        }
        node_want_dial(io, index);
        return;
    }
    if (kind == VSR_IO_DEADLINE_LINK && index < links->links_count) {
        struct vsr_io_link *link = &links->links[index];

        if (link_pending_state(link->state)) {
            link_close(io, index, -ETIMEDOUT);
        } else if (link->state == VSR_IO_LINK_ESTABLISHED) {
            uint64_t idle = io->options.idle_timeout_ns;

            /* A link bound to a stream lives as long as its stream. */
            if (idle == 0 || (link->purpose == VSR_IO_PURPOSE_STREAM &&
                              link->stream != LINK_NONE)) {
                return;
            }
            if (now - link->last_active_ns < idle) {
                link_arm_idle(io, link);
                return;
            }
            /* The carrier stays while sends are queued for the node. */
            if (link->node_index != LINK_NONE &&
                links->nodes[link->node_index].carrier == index &&
                links->nodes[link->node_index].queue_count > 0) {
                link->last_active_ns = now;
                link_arm_idle(io, link);
                return;
            }
            link_close(io, index, 0);
        }
    }
}

/* -------------------------------------------------------------------------
 * Prepare
 * ---------------------------------------------------------------------- */

struct batch {
    struct vsr_io_sqe *sqes;
    uint32_t capacity;
    uint32_t count;
};

static struct vsr_io_sqe *batch_next(struct batch *batch)
{
    struct vsr_io_sqe *sqe;

    if (batch->count == batch->capacity) {
        return NULL;
    }
    sqe = &batch->sqes[batch->count];
    memset(sqe, 0, sizeof(*sqe));
    return sqe;
}

static bool batch_room(const struct batch *batch, uint32_t records)
{
    return batch->capacity - batch->count >= records;
}

/* The listener chain: SOCKET (DIRECT into its file slot), BIND and LISTEN
 * skip their success completions; the multishot ACCEPT owns the slot. */
static bool listener_prepare_setup(struct vsr_io *io, uint32_t index,
                                   struct batch *batch)
{
    struct vsr_io_listener *listener = &io->links.listener_table[index];
    const struct vsr_io_address *address = &io->options.listen[index];
    const struct sockaddr *sockaddr =
        (const struct sockaddr *)(const void *)&address->sockaddr;
    struct vsr_io_sqe *sqe;
    uint64_t user_data;

    if (!batch_room(batch, LINK_CHAIN_MAX)) {
        return false;
    }
    if (listener->file_slot == LINK_NONE) {
        listener->file_slot = vsr_io_engine_slot_alloc(io);
        if (listener->file_slot == LINK_NONE) {
            io->stats.failure = -ENFILE;
            listener->state = VSR_IO_LISTENER_FREE;
            io->links.listeners--;
            return true;
        }
    }
    listener->slot =
        vsr_io_slots_alloc(&io->slots, VSR_IO_SLOT_LISTEN, 1, index, 0, 0);
    if (listener->slot == LINK_NONE) {
        return false;
    }
    user_data = vsr_io_slots_user_data(&io->slots, listener->slot);
    sqe = batch_next(batch);
    sqe->opcode = VSR_IO_SQE_SOCKET;
    sqe->flags = VSR_IO_SQE_LINK | VSR_IO_SQE_SKIP_SUCCESS | VSR_IO_SQE_DIRECT;
    sqe->user_data = user_data;
    sqe->length = sockaddr->sa_family;
    sqe->op_flags = SOCK_STREAM;
    sqe->fd2 = (int32_t)listener->file_slot;
    batch->count++;
    sqe = batch_next(batch);
    sqe->opcode = VSR_IO_SQE_BIND;
    sqe->flags =
        VSR_IO_SQE_LINK | VSR_IO_SQE_SKIP_SUCCESS | VSR_IO_SQE_FIXED_FILE;
    sqe->fd = (int32_t)listener->file_slot;
    sqe->user_data = user_data;
    sqe->addr = &address->sockaddr;
    sqe->length = address->length;
    batch->count++;
    sqe = batch_next(batch);
    sqe->opcode = VSR_IO_SQE_LISTEN;
    sqe->flags =
        VSR_IO_SQE_LINK | VSR_IO_SQE_SKIP_SUCCESS | VSR_IO_SQE_FIXED_FILE;
    sqe->fd = (int32_t)listener->file_slot;
    sqe->user_data = user_data;
    sqe->length = VSR_IO_LISTEN_BACKLOG;
    batch->count++;
    sqe = batch_next(batch);
    sqe->opcode = VSR_IO_SQE_ACCEPT;
    sqe->flags = VSR_IO_SQE_FIXED_FILE;
    sqe->fd = (int32_t)listener->file_slot;
    sqe->user_data = user_data;
    sqe->op_flags = VSR_IO_ACCEPT_MULTISHOT;
    batch->count++;
    listener->state = VSR_IO_LISTENER_ACTIVE;
    return true;
}

static bool listener_prepare_rearm(struct vsr_io *io, uint32_t index,
                                   struct batch *batch)
{
    struct vsr_io_listener *listener = &io->links.listener_table[index];
    struct vsr_io_sqe *sqe = batch_next(batch);

    if (sqe == NULL) {
        return false;
    }
    listener->slot =
        vsr_io_slots_alloc(&io->slots, VSR_IO_SLOT_LISTEN, 1, index, 0, 0);
    if (listener->slot == LINK_NONE) {
        return false;
    }
    sqe->opcode = VSR_IO_SQE_ACCEPT;
    sqe->flags = VSR_IO_SQE_FIXED_FILE;
    sqe->fd = (int32_t)listener->file_slot;
    sqe->user_data = vsr_io_slots_user_data(&io->slots, listener->slot);
    sqe->op_flags = VSR_IO_ACCEPT_MULTISHOT;
    batch->count++;
    listener->state = VSR_IO_LISTENER_ACTIVE;
    return true;
}

/* Shutdown of a listener: the ACCEPT is cancelled and the socket closed;
 * both completions share one SHUTDOWN slot. */
static bool listener_prepare_cancel(struct vsr_io *io, uint32_t index,
                                    struct batch *batch)
{
    struct vsr_io_listener *listener = &io->links.listener_table[index];
    struct vsr_io_sqe *sqe;
    uint8_t records = listener->slot != LINK_NONE ? 2 : 1;
    uint64_t user_data;

    if (!batch_room(batch, records)) {
        return false;
    }
    listener->cancel = vsr_io_slots_alloc(&io->slots, VSR_IO_SLOT_SHUTDOWN,
                                          records, index, TEARDOWN_LISTENER, 0);
    if (listener->cancel == LINK_NONE) {
        return false;
    }
    user_data = vsr_io_slots_user_data(&io->slots, listener->cancel);
    if (listener->slot != LINK_NONE) {
        sqe = batch_next(batch);
        sqe->opcode = VSR_IO_SQE_CANCEL;
        sqe->user_data = user_data;
        sqe->offset = vsr_io_slots_user_data(&io->slots, listener->slot);
        batch->count++;
    }
    sqe = batch_next(batch);
    sqe->opcode = VSR_IO_SQE_CLOSE;
    sqe->flags = VSR_IO_SQE_FIXED_FILE;
    sqe->fd = (int32_t)listener->file_slot;
    sqe->user_data = user_data;
    batch->count++;
    listener->state = VSR_IO_LISTENER_CLOSING;
    return true;
}

static void listener_try_free(struct vsr_io *io, uint32_t index)
{
    struct vsr_io_listener *listener = &io->links.listener_table[index];

    if (listener->state != VSR_IO_LISTENER_CLOSING ||
        listener->slot != LINK_NONE || listener->cancel != LINK_NONE) {
        return;
    }
    vsr_io_engine_slot_free(io, listener->file_slot);
    listener->file_slot = LINK_NONE;
    listener->state = VSR_IO_LISTENER_FREE;
    io->links.listeners--;
}

static bool listeners_prepare(struct vsr_io *io, struct batch *batch)
{
    for (uint32_t i = 0; i < io->options.listen_count; ++i) {
        struct vsr_io_listener *listener = &io->links.listener_table[i];
        bool ok = true;

        switch (listener->state) {
        case VSR_IO_LISTENER_SETUP:
            ok = listener_prepare_setup(io, i, batch);
            break;
        case VSR_IO_LISTENER_REARM:
            ok = listener_prepare_rearm(io, i, batch);
            break;
        case VSR_IO_LISTENER_CANCEL:
            ok = listener_prepare_cancel(io, i, batch);
            break;
        default:
            break;
        }
        if (!ok) {
            return false;
        }
    }
    return true;
}

/* Orphaned accepted descriptors (no link entry was free) are closed. */
static bool orphans_prepare(struct vsr_io *io, struct batch *batch)
{
    struct vsr_io_links *links = &io->links;

    for (uint32_t i = 0; i < VSR_IO_LINK_ORPHANS && links->orphans_count > 0;
         ++i) {
        struct vsr_io_sqe *sqe;
        uint32_t slot;

        if (links->orphans[i] < 0) {
            continue;
        }
        sqe = batch_next(batch);
        if (sqe == NULL) {
            return false;
        }
        slot = vsr_io_slots_alloc(&io->slots, VSR_IO_SLOT_SHUTDOWN, 1, i,
                                  TEARDOWN_ORPHAN, 0);
        if (slot == LINK_NONE) {
            return false;
        }
        sqe->opcode = VSR_IO_SQE_CLOSE;
        sqe->fd = links->orphans[i];
        sqe->user_data = vsr_io_slots_user_data(&io->slots, slot);
        batch->count++;
        links->orphans[i] = -1;
        links->orphans_count--;
    }
    return true;
}

/* The dial's records, one per prepare: SOCKET, then CONNECT on the raw
 * descriptor it produced. */
static bool link_prepare_dial(struct vsr_io *io, struct vsr_io_link *link,
                              struct batch *batch)
{
    const struct sockaddr *sockaddr =
        (const struct sockaddr *)(const void *)&link->peer.sockaddr;
    struct vsr_io_sqe *sqe;

    if (link->connect_slot != LINK_NONE ||
        (link->stage != VSR_IO_STAGE_SOCKET &&
         link->stage != VSR_IO_STAGE_CONNECT)) {
        return true;
    }
    sqe = batch_next(batch);
    if (sqe == NULL) {
        return false;
    }
    link->connect_slot =
        vsr_io_slots_alloc(&io->slots, VSR_IO_SLOT_CONNECT, 1,
                           link_index(io, link), link->stage, 0);
    if (link->connect_slot == LINK_NONE) {
        return false;
    }
    sqe->user_data = vsr_io_slots_user_data(&io->slots, link->connect_slot);
    if (link->stage == VSR_IO_STAGE_SOCKET) {
        sqe->opcode = VSR_IO_SQE_SOCKET;
        sqe->length = sockaddr->sa_family;
        sqe->op_flags = SOCK_STREAM;
    } else {
        sqe->opcode = VSR_IO_SQE_CONNECT;
        sqe->fd = link->raw_fd;
        sqe->addr = &link->peer.sockaddr;
        sqe->length = link->peer.length;
    }
    batch->count++;
    return true;
}

/* The EXTERNAL acceptor's exact 8-byte preamble read on the raw
 * descriptor, re-issued until the bytes are in. */
static bool link_prepare_preamble_recv(struct vsr_io *io,
                                       struct vsr_io_link *link,
                                       struct batch *batch)
{
    struct vsr_io_sqe *sqe;

    if (link->connect_slot != LINK_NONE) {
        return true;
    }
    sqe = batch_next(batch);
    if (sqe == NULL) {
        return false;
    }
    link->connect_slot =
        vsr_io_slots_alloc(&io->slots, VSR_IO_SLOT_CONNECT, 1,
                           link_index(io, link), link->stage, 0);
    if (link->connect_slot == LINK_NONE) {
        return false;
    }
    sqe->opcode = VSR_IO_SQE_RECV;
    sqe->fd = link->raw_fd;
    sqe->user_data = vsr_io_slots_user_data(&io->slots, link->connect_slot);
    sqe->addr = link->preamble + link->preamble_seen;
    sqe->length = VSR_IO_PREAMBLE_BYTES - link->preamble_seen;
    batch->count++;
    return true;
}

/* The multishot receive of an installed link, armed once and after every
 * termination while the link is open; after -ENOBUFS only once the pool
 * reported a buffer again. */
static const int link_nodelay_on = 1;

/* TCP_NODELAY on a taken-over socket (options.nodelay): one record on the
 * connect slot, idle by then, whose result is ignored, so a socket without
 * the option is simply left as it is. Known AF_UNIX peers skip it. */
static bool link_prepare_nodelay(struct vsr_io *io, struct vsr_io_link *link,
                                 struct batch *batch)
{
    struct vsr_io_sqe *sqe;

    if (link->nodelay_set || link->fd < 0 || link->connect_slot != LINK_NONE) {
        return true;
    }
    if (io->options.nodelay == 0 || link->peer.sockaddr.ss_family == AF_UNIX) {
        link->nodelay_set = true;
        return true;
    }
    sqe = batch_next(batch);
    if (sqe == NULL) {
        return false;
    }
    link->connect_slot =
        vsr_io_slots_alloc(&io->slots, VSR_IO_SLOT_CONNECT, 1,
                           link_index(io, link), VSR_IO_STAGE_NODELAY, 0);
    if (link->connect_slot == LINK_NONE) {
        return false;
    }
    sqe->opcode = VSR_IO_SQE_SETSOCKOPT;
    sqe->flags = VSR_IO_SQE_FIXED_FILE;
    sqe->fd = link->fd;
    sqe->op_flags = ((uint32_t)IPPROTO_TCP << 16) | TCP_NODELAY;
    sqe->addr = &link_nodelay_on;
    sqe->length = sizeof(link_nodelay_on);
    sqe->user_data = vsr_io_slots_user_data(&io->slots, link->connect_slot);
    batch->count++;
    link->nodelay_set = true;
    return true;
}

static bool link_prepare_recv(struct vsr_io *io, struct vsr_io_link *link,
                              struct batch *batch)
{
    struct vsr_io_sqe *sqe;

    if (link->recv_slot != LINK_NONE || link->fd < 0 || link->recv_starved ||
        (link->state != VSR_IO_LINK_HELLO &&
         link->state != VSR_IO_LINK_ESTABLISHED)) {
        return true;
    }
    sqe = batch_next(batch);
    if (sqe == NULL) {
        return false;
    }
    link->recv_slot = vsr_io_slots_alloc(&io->slots, VSR_IO_SLOT_RECV, 1,
                                         link_index(io, link), 0, 0);
    if (link->recv_slot == LINK_NONE) {
        return false;
    }
    sqe->opcode = VSR_IO_SQE_RECV;
    sqe->flags = VSR_IO_SQE_FIXED_FILE | VSR_IO_SQE_BUFFER_SELECT;
    sqe->fd = link->fd;
    sqe->buffer_group = io->pool.group;
    sqe->op_flags = VSR_IO_RECV_MULTISHOT;
    sqe->user_data = vsr_io_slots_user_data(&io->slots, link->recv_slot);
    batch->count++;
    return true;
}

/* Teardown of a CLOSING link once the caller returned its descriptor:
 * SHUTDOWN, CANCEL of the receive and CLOSE of the slot, or a plain CLOSE
 * of a raw descriptor, sharing one slot. A CONNECT or the EXTERNAL
 * preamble receive still in flight may never complete on its own (a black
 * hole, a silent peer) and is cancelled first; a SOCKET still owes the
 * descriptor and a SETSOCKOPT completes at once, so those are waited for. */
static bool link_prepare_teardown(struct vsr_io *io, struct vsr_io_link *link,
                                  struct batch *batch)
{
    struct vsr_io_sqe *sqe;
    uint8_t records = 0;
    uint64_t user_data;
    bool cancel_dial = false;

    if (link->torn_down || link->handshake_op != 0) {
        return true;
    }
    if (link->connect_slot != LINK_NONE) {
        uint32_t stage = io->slots.slots[link->connect_slot].sub;

        if (stage != VSR_IO_STAGE_CONNECT &&
            stage != VSR_IO_STAGE_PREAMBLE_RECV) {
            return true;
        }
        cancel_dial = true;
    }
    if (link->fd < 0 && link->raw_fd < 0) {
        LINKS_ASSERT(!cancel_dial);
        link->torn_down = true;
        link_try_free(io, link);
        return true;
    }
    if (link->fd >= 0) {
        records = 2 + (link->recv_slot != LINK_NONE ? 1 : 0);
    } else {
        records = 1 + (cancel_dial ? 1 : 0);
    }
    if (!batch_room(batch, records)) {
        return false;
    }
    link->shutdown_slot =
        vsr_io_slots_alloc(&io->slots, VSR_IO_SLOT_SHUTDOWN, records,
                           link_index(io, link), TEARDOWN_LINK, 0);
    if (link->shutdown_slot == LINK_NONE) {
        return false;
    }
    user_data = vsr_io_slots_user_data(&io->slots, link->shutdown_slot);
    if (link->fd >= 0) {
        sqe = batch_next(batch);
        sqe->opcode = VSR_IO_SQE_SHUTDOWN;
        sqe->flags = VSR_IO_SQE_FIXED_FILE;
        sqe->fd = link->fd;
        sqe->length = SHUT_RDWR;
        sqe->user_data = user_data;
        batch->count++;
        if (link->recv_slot != LINK_NONE) {
            sqe = batch_next(batch);
            sqe->opcode = VSR_IO_SQE_CANCEL;
            sqe->offset = vsr_io_slots_user_data(&io->slots, link->recv_slot);
            sqe->user_data = user_data;
            batch->count++;
        }
        sqe = batch_next(batch);
        sqe->opcode = VSR_IO_SQE_CLOSE;
        sqe->flags = VSR_IO_SQE_FIXED_FILE;
        sqe->fd = link->fd;
        sqe->user_data = user_data;
        batch->count++;
    } else {
        if (cancel_dial) {
            sqe = batch_next(batch);
            sqe->opcode = VSR_IO_SQE_CANCEL;
            sqe->offset =
                vsr_io_slots_user_data(&io->slots, link->connect_slot);
            sqe->user_data = user_data;
            batch->count++;
        }
        sqe = batch_next(batch);
        sqe->opcode = VSR_IO_SQE_CLOSE;
        sqe->fd = link->raw_fd;
        sqe->user_data = user_data;
        batch->count++;
    }
    link->torn_down = true;
    return true;
}

static bool link_prepare(struct vsr_io *io, struct vsr_io_link *link,
                         struct batch *batch)
{
    struct vsr_io_sqe *sqe;

    switch (link->state) {
    case VSR_IO_LINK_CONNECTING:
        return link_prepare_dial(io, link, batch);
    case VSR_IO_LINK_EXTERNAL:
        if (link->stage == VSR_IO_STAGE_PREAMBLE_RECV) {
            return link_prepare_preamble_recv(io, link, batch);
        }
        if (link->stage == VSR_IO_STAGE_PREAMBLE_SEND && !link->hello_sent) {
            link->hello_sent = link_write_hello(io, link, true, false);
        }
        break;
    case VSR_IO_LINK_HELLO:
        /* The dialer's preamble and HELLO; the acceptor's HELLO once the
         * dialer's was accepted (hello_seen). */
        if (!link->hello_sent) {
            if (link->direction == VSR_IO_OUTBOUND) {
                link->hello_sent = link_write_hello(io, link, true, true);
            }
        }
        if (!link_prepare_nodelay(io, link, batch) ||
            !link_prepare_recv(io, link, batch)) {
            return false;
        }
        break;
    case VSR_IO_LINK_ESTABLISHED:
        if (!link->hello_sent) {
            link->hello_sent = link_write_hello(io, link, false, true);
        }
        if (!link_prepare_nodelay(io, link, batch) ||
            !link_prepare_recv(io, link, batch)) {
            return false;
        }
        break;
    case VSR_IO_LINK_CLOSING:
        return link_prepare_teardown(io, link, batch);
    default:
        return true;
    }
    sqe = batch_next(batch);
    if (sqe == NULL) {
        return false;
    }
    if (link_prepare_send(io, link, sqe)) {
        batch->count++;
    }
    return true;
}

void vsr_io_links_prepare(struct vsr_io *io, struct vsr_io_sqe *sqes,
                          uint32_t capacity, uint32_t *count)
{
    struct vsr_io_links *links = &io->links;
    struct batch batch = {sqes, capacity, 0};

    *count = 0;
    if (!links->listen_started && !links->closing) {
        /* Listener setup waits for the first prepare (decision 45). */
        links->listen_started = 1;
        for (uint32_t i = 0; i < io->options.listen_count; ++i) {
            links->listener_table[i].state = VSR_IO_LISTENER_SETUP;
        }
        links->listeners = io->options.listen_count;
    }
    if (vsr_io_pool_was_starved(&io->pool)) {
        for (uint32_t i = 0; i < links->links_count; ++i) {
            links->links[i].recv_starved = false;
        }
    }
    if (!listeners_prepare(io, &batch) || !orphans_prepare(io, &batch)) {
        *count = batch.count;
        return;
    }
    for (uint32_t i = 0; i < links->links_count; ++i) {
        if (!link_prepare(io, &links->links[i], &batch)) {
            break;
        }
    }
    *count = batch.count;
}

/* -------------------------------------------------------------------------
 * Completions
 * ---------------------------------------------------------------------- */

static void listener_complete(struct vsr_io *io, uint32_t index,
                              const struct vsr_io_cqe *cqe)
{
    struct vsr_io_listener *listener = &io->links.listener_table[index];
    bool more = (cqe->flags & VSR_IO_CQE_MORE) != 0;

    if (cqe->result >= 0) {
        listener_accepted(io, cqe->result);
    }
    if (more) {
        return;
    }
    /* The ACCEPT terminated: cancelled at shutdown, failed in setup (fatal,
     * decision 45), or ended by the ring; the last is re-armed. */
    listener->slot = LINK_NONE;
    if (listener->state == VSR_IO_LISTENER_CLOSING ||
        listener->state == VSR_IO_LISTENER_CANCEL || io->links.closing) {
        if (listener->state == VSR_IO_LISTENER_CANCEL) {
            /* Gone before the CANCEL went out: only the CLOSE remains. */
            return;
        }
        listener_try_free(io, index);
        return;
    }
    if (cqe->result < 0 && cqe->result != -ECANCELED &&
        cqe->result != -EMFILE && cqe->result != -ENFILE &&
        cqe->result != -ECONNABORTED) {
        io->stats.failure = cqe->result;
        listener->state = VSR_IO_LISTENER_CANCEL;
        return;
    }
    listener->state = VSR_IO_LISTENER_REARM;
}

/* SOCKET, CONNECT and the EXTERNAL preamble receive of a link. */
static void link_connect_complete(struct vsr_io *io, struct vsr_io_link *link,
                                  uint32_t stage, const struct vsr_io_cqe *cqe)
{
    uint32_t index = link_index(io, link);

    link->connect_slot = LINK_NONE;
    if (stage == VSR_IO_STAGE_NODELAY) {
        return; /* Best effort: the result is not even looked at. */
    }
    if (link->state == VSR_IO_LINK_CLOSING) {
        if (stage == VSR_IO_STAGE_SOCKET && cqe->result >= 0) {
            link->raw_fd = cqe->result; /* Closed by the teardown. */
        }
        return;
    }
    switch (stage) {
    case VSR_IO_STAGE_SOCKET:
        if (cqe->result < 0) {
            link_close(io, index, cqe->result);
            return;
        }
        link->raw_fd = cqe->result;
        link->stage = VSR_IO_STAGE_CONNECT;
        return;
    case VSR_IO_STAGE_CONNECT:
        if (cqe->result < 0) {
            link_close(io, index, cqe->result);
            return;
        }
        link_start_handshake(io, link);
        return;
    case VSR_IO_STAGE_PREAMBLE_RECV:
        if (cqe->result <= 0) {
            link_close(io, index, cqe->result == 0 ? -EPIPE : cqe->result);
            return;
        }
        link->preamble_seen += (uint32_t)cqe->result;
        LINKS_ASSERT(link->preamble_seen <= VSR_IO_PREAMBLE_BYTES);
        io->stats.bytes_received += (uint64_t)cqe->result;
        if (link->preamble_seen < VSR_IO_PREAMBLE_BYTES) {
            return; /* Re-issued by prepare for the rest. */
        }
        if (vsr_io_get_u64(link->preamble) != VSR_IO_WIRE_MAGIC) {
            io->stats.frames_rejected++;
            link_close(io, index, -EPROTO);
            return;
        }
        link_want_handshake(io, link);
        return;
    default:
        LINKS_ASSERT(false);
        return;
    }
}

static void link_recv_complete(struct vsr_io *io, struct vsr_io_link *link,
                               const struct vsr_io_cqe *cqe)
{
    uint32_t index = link_index(io, link);
    bool more = (cqe->flags & VSR_IO_CQE_MORE) != 0;

    if (cqe->result > 0 && (cqe->flags & VSR_IO_CQE_BUFFER) != 0) {
        if (link->state == VSR_IO_LINK_CLOSING) {
            /* Bytes for a link on its way out: the slab reference is
             * taken and dropped. */
            vsr_io_pool_recv_begin(&io->pool, cqe->buffer_id,
                                   (uint32_t)cqe->result);
            vsr_io_pool_release(&io->pool, cqe->buffer_id);
        } else {
            link_received(io, link, cqe->buffer_id, (uint32_t)cqe->result);
        }
        if ((cqe->flags & VSR_IO_CQE_BUFFER_MORE) == 0) {
            vsr_io_pool_recv_end(&io->pool, cqe->buffer_id);
        }
    }
    if (more) {
        return;
    }
    link->recv_slot = LINK_NONE;
    if (link->state == VSR_IO_LINK_CLOSING) {
        return;
    }
    if (cqe->result == -ENOBUFS) {
        vsr_io_pool_starved(&io->pool);
        link->recv_starved = true;
        return;
    }
    if (cqe->result == 0) {
        link_close(io, index, -EPIPE); /* End of stream. */
        return;
    }
    if (cqe->result < 0) {
        link_close(io, index, cqe->result);
    }
    /* A positive result without MORE: the ring ended the multishot at a
     * full completion queue (decision 68); prepare re-arms it. */
}

void vsr_io_links_complete(struct vsr_io *io, uint32_t slot,
                           const struct vsr_io_cqe *cqe)
{
    struct vsr_io_slot *entry = &io->slots.slots[slot];
    struct vsr_io_links *links = &io->links;
    struct vsr_io_link *link = NULL;
    bool more = (cqe->flags & VSR_IO_CQE_MORE) != 0;
    uint32_t owner = entry->owner;
    uint32_t sub = entry->sub;
    uint8_t kind = entry->kind;

    if (kind == VSR_IO_SLOT_LISTEN) {
        vsr_io_slots_consumed(&io->slots, slot, more);
        listener_complete(io, owner, cqe);
        return;
    }
    if (kind == VSR_IO_SLOT_SHUTDOWN && sub != TEARDOWN_LINK) {
        vsr_io_slots_consumed(&io->slots, slot, false);
        if (sub == TEARDOWN_LISTENER && entry->kind == VSR_IO_SLOT_FREE) {
            links->listener_table[owner].cancel = LINK_NONE;
            listener_try_free(io, owner);
        }
        return;
    }
    LINKS_ASSERT(owner < links->links_count);
    link = &links->links[owner];
    switch (kind) {
    case VSR_IO_SLOT_CONNECT:
        vsr_io_slots_consumed(&io->slots, slot, false);
        link_connect_complete(io, link, sub, cqe);
        break;
    case VSR_IO_SLOT_RECV:
        vsr_io_slots_consumed(&io->slots, slot, more);
        link_recv_complete(io, link, cqe);
        break;
    case VSR_IO_SLOT_SEND: {
        /* A zero-copy send refused before the kernel took it completes
         * once without MORE: no NOTIF follows and the slot is freed
         * outright (decision 62, slots.h). */
        bool refused = cqe->result < 0 && link->sends[sub].zero_copy && !more &&
                       (cqe->flags & VSR_IO_CQE_NOTIF) == 0;

        link_send_complete(io, link, sub, cqe);
        if (refused) {
            vsr_io_slots_free(&io->slots, slot);
        } else {
            vsr_io_slots_consumed(&io->slots, slot, more);
        }
        if (entry->kind == VSR_IO_SLOT_FREE) {
            link->sends[sub].slot = LINK_NONE;
        }
        if (link->state == VSR_IO_LINK_EXTERNAL &&
            link->stage == VSR_IO_STAGE_PREAMBLE_SEND &&
            link->sent_offset >= VSR_IO_PREAMBLE_BYTES) {
            link_want_handshake(io, link);
        }
        break;
    }
    case VSR_IO_SLOT_SHUTDOWN:
        vsr_io_slots_consumed(&io->slots, slot, false);
        if (entry->kind == VSR_IO_SLOT_FREE) {
            link->shutdown_slot = LINK_NONE;
        }
        break;
    default:
        LINKS_ASSERT(false);
        return;
    }
    link_try_free(io, link);
}

/* -------------------------------------------------------------------------
 * Shutdown
 * ---------------------------------------------------------------------- */

void vsr_io_links_shutdown(struct vsr_io *io)
{
    struct vsr_io_links *links = &io->links;

    links->closing = 1;
    for (uint32_t i = 0; i < io->options.listen_count; ++i) {
        struct vsr_io_listener *listener = &links->listener_table[i];

        switch (listener->state) {
        case VSR_IO_LISTENER_SETUP:
            listener->state = VSR_IO_LISTENER_FREE;
            links->listeners--;
            break;
        case VSR_IO_LISTENER_ACTIVE:
        case VSR_IO_LISTENER_REARM:
            listener->state = VSR_IO_LISTENER_CANCEL;
            break;
        default:
            break;
        }
    }
    for (uint32_t i = 0; i < links->links_count; ++i) {
        link_close(io, i, -ECANCELED);
    }
    for (uint32_t i = 0; i < links->nodes_count; ++i) {
        struct vsr_io_node *node = &links->nodes[i];

        node_queue_drop(io, i, true); /* CANCELLED: closing is set. */
        if (node->due) {
            node->due = false;
            links->dials_due--;
        }
        vsr_io_deadlines_arm(&io->deadlines, dial_handle(io, i),
                             VSR_NO_DEADLINE);
    }
}
