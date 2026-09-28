#include "config.h"

#define _GNU_SOURCE 1

#include "sim/sim.h"

#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/un.h>

/* ------------------------------------------------------------------------
 * Tables
 * --------------------------------------------------------------------- */

static struct vsr_sim_connection *connection_at(const struct vsr_sim *sim,
                                                uint32_t index)
{
    if (index >= sim->connections_count ||
        !sim->connections[index]->used) {
        return NULL;
    }
    return sim->connections[index];
}

static uint32_t connection_new(struct vsr_sim *sim)
{
    uint32_t index = 0;
    struct vsr_sim_connection *connection;
    uint32_t generation;

    while (index < sim->connections_count &&
           sim->connections[index]->used) {
        ++index;
    }
    if (index == sim->connections_count) {
        sim->connections = vsr_sim_grow(sim->connections, (size_t)index + 1,
                                        sizeof(*sim->connections));
        sim->connections[index] =
            vsr_sim_alloc(sizeof(struct vsr_sim_connection));
        ++sim->connections_count;
    }
    connection = sim->connections[index];
    generation = connection->generation + 1;
    memset(connection, 0, sizeof(*connection));
    connection->used = 1;
    connection->generation = generation;
    connection->listener = VSR_SIM_NONE;
    connection->ends[0].object = VSR_SIM_NONE;
    connection->ends[1].object = VSR_SIM_NONE;
    return index;
}

static void free_queue(struct vsr_sim_end *end)
{
    struct vsr_sim_segment *segment = end->head;

    while (segment != NULL) {
        struct vsr_sim_segment *next = segment->next;

        free(segment->bytes);
        free(segment);
        segment = next;
    }
    end->head = NULL;
    end->tail = NULL;
    end->queued = 0;
}

static void connection_maybe_free(struct vsr_sim *sim, uint32_t index)
{
    struct vsr_sim_connection *connection = connection_at(sim, index);

    if (connection == NULL || !connection->ends[0].closed ||
        !connection->ends[1].closed) {
        return;
    }
    free_queue(&connection->ends[0]);
    free_queue(&connection->ends[1]);
    connection->used = 0;
    ++connection->generation;
}

static struct vsr_sim_listener *listener_at(const struct vsr_sim *sim,
                                            uint32_t index)
{
    if (index >= sim->listeners_count || !sim->listeners[index]->used) {
        return NULL;
    }
    return sim->listeners[index];
}

static bool key_equal(const struct vsr_sim_key *a, const struct vsr_sim_key *b)
{
    return a->family == b->family && a->port == b->port &&
           a->length == b->length && memcmp(a->path, b->path, a->length) == 0;
}

static uint32_t listener_find(const struct vsr_sim *sim, uint32_t node,
                              const struct vsr_sim_key *key)
{
    for (uint32_t i = 0; i < sim->listeners_count; ++i) {
        const struct vsr_sim_listener *listener = sim->listeners[i];

        if (listener->used && listener->node == node &&
            key_equal(&listener->key, key)) {
            return i;
        }
    }
    return VSR_SIM_NONE;
}

static uint32_t listener_new(struct vsr_sim *sim)
{
    uint32_t index = 0;

    while (index < sim->listeners_count && sim->listeners[index]->used) {
        ++index;
    }
    if (index == sim->listeners_count) {
        sim->listeners = vsr_sim_grow(sim->listeners, (size_t)index + 1,
                                      sizeof(*sim->listeners));
        sim->listeners[index] = vsr_sim_alloc(sizeof(struct vsr_sim_listener));
        ++sim->listeners_count;
    }
    free(sim->listeners[index]->pending);
    memset(sim->listeners[index], 0, sizeof(struct vsr_sim_listener));
    sim->listeners[index]->used = 1;
    return index;
}

static void listener_push(struct vsr_sim_listener *listener,
                          uint32_t connection)
{
    if (listener->pending_count == listener->pending_capacity) {
        listener->pending_capacity = listener->pending_capacity == 0
                                         ? 8
                                         : listener->pending_capacity * 2;
        listener->pending =
            vsr_sim_grow(listener->pending, listener->pending_capacity,
                         sizeof(*listener->pending));
    }
    listener->pending[listener->pending_count++] = connection;
}

static void listener_remove(struct vsr_sim_listener *listener,
                            uint32_t connection)
{
    for (uint32_t i = 0; i < listener->pending_count; ++i) {
        if (listener->pending[i] == connection) {
            memmove(&listener->pending[i], &listener->pending[i + 1],
                    sizeof(*listener->pending) *
                        (listener->pending_count - i - 1));
            --listener->pending_count;
            return;
        }
    }
}

static struct vsr_sim_node *node_at(struct vsr_sim *sim, uint32_t node)
{
    return &sim->nodes[node];
}

/* ------------------------------------------------------------------------
 * Resets and stalls
 * --------------------------------------------------------------------- */

static void reset_connection(struct vsr_sim *sim, uint32_t index)
{
    struct vsr_sim_connection *connection = connection_at(sim, index);

    if (connection == NULL) {
        return;
    }
    if (connection->listener != VSR_SIM_NONE) {
        struct vsr_sim_listener *listener =
            listener_at(sim, connection->listener);

        if (listener != NULL) {
            listener_remove(listener, index);
        }
        connection->listener = VSR_SIM_NONE;
        connection->ends[1].closed = 1;
    }
    if (connection->ends[0].reset && connection->ends[1].reset) {
        connection_maybe_free(sim, index);
        return;
    }
    for (uint32_t i = 0; i < 2; ++i) {
        free_queue(&connection->ends[i]);
        connection->ends[i].reset = 1;
    }
    for (uint32_t i = 0; i < 2; ++i) {
        const struct vsr_sim_end *end = &connection->ends[i];

        if (end->object != VSR_SIM_NONE && node_at(sim, end->node)->alive) {
            vsr_sim_emit(sim, VSR_SIM_TRACE_RESET, end->node,
                         connection->ends[1 - i].node, 0);
        }
    }
    for (uint32_t i = 0; i < 2; ++i) {
        uint32_t node = connection->ends[i].node;
        uint32_t object = connection->ends[i].object;

        if (object != VSR_SIM_NONE && node_at(sim, node)->alive) {
            vsr_sim_net_serve(node_at(sim, node), object);
        }
    }
    connection_maybe_free(sim, index);
}

static void stall(struct vsr_sim *sim, uint32_t index)
{
    struct vsr_sim_connection *connection = connection_at(sim, index);

    if (connection == NULL || connection->stalled) {
        return;
    }
    connection->stalled = 1;
    vsr_sim_schedule(
        sim, vsr_sim_add(sim->now_ns, sim->faults.network.stall_reset_ns),
        VSR_SIM_EVENT_STALL, VSR_SIM_NO_NODE, index, 0,
        connection->generation);
}

bool vsr_sim_net_stall(struct vsr_sim *sim, uint32_t index,
                       uint32_t generation)
{
    struct vsr_sim_connection *connection = connection_at(sim, index);

    if (connection == NULL || connection->generation != generation) {
        return false;
    }
    if (connection->ends[0].reset && connection->ends[1].reset) {
        return false;
    }
    reset_connection(sim, index);
    return true;
}

/* ------------------------------------------------------------------------
 * Transmission
 * --------------------------------------------------------------------- */

static void append(struct vsr_sim_end *end, struct vsr_sim_segment *segment)
{
    segment->next = NULL;
    if (end->tail == NULL) {
        end->head = segment;
    } else {
        end->tail->next = segment;
    }
    end->tail = segment;
    end->queued += segment->length;
}

static struct vsr_sim_segment *segment_new(unsigned char *bytes,
                                           uint32_t length, uint64_t due,
                                           bool fin)
{
    struct vsr_sim_segment *segment = vsr_sim_alloc(sizeof(*segment));

    segment->bytes = bytes;
    segment->length = length;
    segment->due_ns = due;
    segment->fin = fin;
    return segment;
}

/* Queues bytes (or a FIN) from end `from` toward the other end, applying
 * the network faults in a fixed order: partition or stall, drop, reset,
 * corrupt, delay, split. Takes ownership of bytes. Returns the arrival
 * time, VSR_SIM_NEVER when lost; *reset asks the caller to reset the
 * connection once it has finished with its own record. */
static uint64_t transmit(struct vsr_sim *sim, uint32_t index, uint32_t from,
                         unsigned char *bytes, uint32_t length, bool fin,
                         bool *reset)
{
    struct vsr_sim_connection *connection = connection_at(sim, index);
    struct vsr_sim_end *source = &connection->ends[from];
    struct vsr_sim_end *target = &connection->ends[1 - from];
    const struct vsr_sim_network_faults *faults = &sim->faults.network;
    bool cross = source->node != target->node;
    bool blocked = connection->stalled != 0;
    bool corrupted = false;
    uint64_t due;

    *reset = false;
    if (!blocked && cross &&
        vsr_sim_partitioned(sim, source->node, target->node)) {
        blocked = true;
    }
    if (!blocked && cross && !fin && vsr_sim_chance(sim, faults->drop_ppm)) {
        blocked = true;
    }
    if (blocked) {
        if (!fin) {
            vsr_sim_emit(sim, VSR_SIM_TRACE_DROP, source->node, target->node,
                         length);
        }
        stall(sim, index);
        append(target, segment_new(bytes, length, VSR_SIM_NEVER, fin));
        return VSR_SIM_NEVER;
    }
    if (cross && !fin && vsr_sim_chance(sim, faults->reset_ppm)) {
        free(bytes);
        *reset = true;
        return sim->now_ns;
    }
    if (cross && !fin && length > 0 &&
        vsr_sim_chance(sim, faults->corrupt_ppm)) {
        uint64_t flips = 1 + vsr_sim_pcg_below(&sim->random, 3);

        corrupted = true;
        for (uint64_t i = 0; i < flips; ++i) {
            uint64_t at = vsr_sim_pcg_below(&sim->random, length);
            uint64_t bit = vsr_sim_pcg_below(&sim->random, 8);

            bytes[at] = (unsigned char)(bytes[at] ^ (1u << bit));
        }
    }
    due = vsr_sim_add(sim->now_ns,
                      cross ? vsr_sim_range(sim, faults->delay_min_ns,
                                            faults->delay_max_ns)
                            : 0);
    if (due < target->last_due_ns) {
        due = target->last_due_ns;
    }
    target->last_due_ns = due;
    if (cross && !fin && length > 1 &&
        vsr_sim_chance(sim, faults->split_ppm)) {
        uint64_t pieces = 2 + vsr_sim_pcg_below(&sim->random, 3);
        uint32_t offset = 0;

        if (pieces > length) {
            pieces = length;
        }
        for (uint64_t i = 0; i < pieces; ++i) {
            uint32_t left = length - offset;
            uint32_t size = left;
            unsigned char *copy;
            struct vsr_sim_segment *segment;

            if (i + 1 < pieces) {
                uint32_t reserve = (uint32_t)(pieces - i - 1);

                size = 1 + (uint32_t)vsr_sim_pcg_below(&sim->random,
                                                       left - reserve);
            }
            copy = vsr_sim_alloc(size);
            memcpy(copy, bytes + offset, size);
            segment = segment_new(copy, size, due, false);
            segment->boundary = i + 1 < pieces;
            segment->corrupted = corrupted && i == 0;
            append(target, segment);
            offset += size;
        }
        free(bytes);
    } else {
        struct vsr_sim_segment *segment =
            segment_new(bytes, length, due, fin);

        segment->corrupted = corrupted;
        append(target, segment);
    }
    /* Corruption is reported at arrival, on the first piece. */
    vsr_sim_schedule(sim, due, VSR_SIM_EVENT_SEGMENT, VSR_SIM_NO_NODE, index,
                     1 - from, connection->generation);
    return due;
}

bool vsr_sim_net_segment(struct vsr_sim *sim, uint32_t index, uint32_t end,
                         uint32_t generation)
{
    struct vsr_sim_connection *connection = connection_at(sim, index);
    struct vsr_sim_end *target;
    uint32_t source;
    bool any = false;
    bool data = false;

    if (connection == NULL || connection->generation != generation ||
        end > 1) {
        return false;
    }
    target = &connection->ends[end];
    source = connection->ends[1 - end].node;
    for (struct vsr_sim_segment *segment = target->head; segment != NULL;
         segment = segment->next) {
        if (segment->arrived) {
            continue;
        }
        if (segment->due_ns > sim->now_ns) {
            break;
        }
        segment->arrived = 1;
        any = true;
        if (!segment->fin) {
            data = true;
            vsr_sim_emit(sim,
                         segment->corrupted ? VSR_SIM_TRACE_CORRUPT
                                            : VSR_SIM_TRACE_DELIVER,
                         target->node, source, segment->length);
        }
    }
    if (!any) {
        return false;
    }
    if (target->closed) {
        /* Data reaching a closed socket answers with a reset. */
        if (data) {
            reset_connection(sim, index);
        }
        return true;
    }
    if (target->object != VSR_SIM_NONE) {
        vsr_sim_net_serve(node_at(sim, target->node), target->object);
    }
    return true;
}

/* ------------------------------------------------------------------------
 * Addresses
 * --------------------------------------------------------------------- */

static bool key_in_use(const struct vsr_sim_node *node,
                       const struct vsr_sim_key *key)
{
    for (uint32_t i = 0; i < node->objects_count; ++i) {
        const struct vsr_sim_object *object = node->objects[i];

        if (object->kind == VSR_SIM_OBJECT_SOCKET && object->bound &&
            key_equal(&object->key, key)) {
            return true;
        }
    }
    return false;
}

static void ephemeral(struct vsr_sim_node *node, struct vsr_sim_key *key)
{
    memset(key, 0, sizeof(*key));
    key->family = AF_INET;
    for (;;) {
        key->port = node->next_port;
        node->next_port =
            node->next_port >= 65535u ? VSR_SIM_EPHEMERAL_PORT
                                      : node->next_port + 1;
        if (!key_in_use(node, key)) {
            return;
        }
    }
}

/* Parses a raw socket address into a key and the node it names
 * (VSR_SIM_NONE: no such host). */
static int parse_address(const struct vsr_sim_node *node,
                         const struct vsr_sim_op *op, uint32_t domain,
                         bool bind, struct vsr_sim_key *key, uint32_t *peer)
{
    const struct vsr_sim *sim = node->world;
    sa_family_t family;

    memset(key, 0, sizeof(*key));
    *peer = node->index;
    if (op->raw == NULL || op->raw_length < sizeof(family)) {
        return -EINVAL;
    }
    memcpy(&family, op->raw, sizeof(family));
    if (family != AF_INET && family != AF_UNIX) {
        return -EAFNOSUPPORT;
    }
    if (family != domain) {
        return -EAFNOSUPPORT;
    }
    if (family == AF_INET) {
        struct sockaddr_in in;
        uint32_t ip;

        if (op->raw_length < sizeof(in)) {
            return -EINVAL;
        }
        memcpy(&in, op->raw, sizeof(in));
        ip = ntohl(in.sin_addr.s_addr);
        key->family = AF_INET;
        key->port = ntohs(in.sin_port);
        if (ip == INADDR_ANY || (ip >> 24) == 127u) {
            *peer = node->index;
        } else if (ip >= UINT32_C(0x0a000001) &&
                   ip - UINT32_C(0x0a000001) < sim->nodes_count) {
            *peer = ip - UINT32_C(0x0a000001);
        } else {
            *peer = VSR_SIM_NONE;
        }
        if (bind && *peer != node->index) {
            return -EADDRNOTAVAIL;
        }
        return 0;
    }
    {
        size_t offset = offsetof(struct sockaddr_un, sun_path);
        const unsigned char *name = op->raw + offset;
        size_t length;

        if (op->raw_length <= offset) {
            return -EINVAL;
        }
        length = op->raw_length - offset;
        if (name[0] != 0) {
            length = strnlen((const char *)name, length);
        }
        if (length == 0 || length > sizeof(key->path)) {
            return -EINVAL;
        }
        key->family = AF_UNIX;
        key->length = (uint32_t)length;
        memcpy(key->path, name, length);
        return 0;
    }
}

/* ------------------------------------------------------------------------
 * Serving parked records
 * --------------------------------------------------------------------- */

static size_t take(struct vsr_sim_end *end, unsigned char *destination,
                   size_t space, bool peek)
{
    struct vsr_sim_segment *segment = end->head;
    size_t taken = 0;

    while (segment != NULL && segment->arrived && !segment->fin &&
           taken < space) {
        size_t available = segment->length - segment->offset;
        size_t size = available < space - taken ? available : space - taken;
        bool whole = size == available;
        bool boundary = segment->boundary != 0;
        struct vsr_sim_segment *next = segment->next;

        memcpy(destination + taken, segment->bytes + segment->offset, size);
        taken += size;
        if (!peek) {
            segment->offset += (uint32_t)size;
            end->queued -= size;
            if (whole) {
                end->head = next;
                if (end->tail == segment) {
                    end->tail = NULL;
                }
                free(segment->bytes);
                free(segment);
            }
        }
        if (!whole || boundary) {
            break;
        }
        segment = next;
    }
    return taken;
}

static void serve_accept(struct vsr_sim_node *node, uint32_t index)
{
    struct vsr_sim *sim = node->world;

    for (;;) {
        struct vsr_sim_object *object = vsr_sim_object(node, index);
        struct vsr_sim_listener *listener = listener_at(sim, object->listener);
        uint32_t op = vsr_sim_exec_waiter(node, index, VSR_IO_SQE_ACCEPT);
        const struct vsr_io_sqe *sqe;
        struct vsr_sim_connection *connection;
        uint32_t accepted;
        uint32_t domain = object->domain;
        uint32_t pending;
        int error;
        int result;

        if (op == VSR_SIM_NONE || listener == NULL ||
            listener->pending_count == 0) {
            return;
        }
        sqe = &vsr_sim_op(node, op)->sqe;
        error = vsr_sim_exec_install(node, sqe, 0, true);
        if (error != 0) {
            /* The connection stays queued for the next accept. */
            vsr_sim_exec_complete(node, op, error, 0, 0);
            continue;
        }
        pending = listener->pending[0];
        listener_remove(listener, pending);
        connection = connection_at(sim, pending);
        connection->listener = VSR_SIM_NONE;
        accepted = vsr_sim_object_new(node, VSR_SIM_OBJECT_SOCKET);
        {
            struct vsr_sim_object *socket = vsr_sim_object(node, accepted);

            socket->domain = domain;
            socket->state = VSR_SIM_SOCKET_CONNECTED;
            socket->connection = pending;
            socket->end = 1;
        }
        connection->ends[1].object = accepted;
        connection->ends[1].attached = 1;
        result = vsr_sim_exec_install(node, sqe, accepted, false);
        if (vsr_sim_op(node, op)->multishot) {
            vsr_sim_exec_post(node, op, result, 0, 0);
        } else {
            vsr_sim_exec_complete(node, op, result, 0, 0);
        }
    }
}

static void serve_peer(struct vsr_sim *sim,
                       const struct vsr_sim_connection *connection,
                       uint32_t end)
{
    const struct vsr_sim_end *peer = &connection->ends[1 - end];

    if (peer->object != VSR_SIM_NONE && node_at(sim, peer->node)->alive) {
        vsr_sim_net_serve(node_at(sim, peer->node), peer->object);
    }
}

static void serve_recv(struct vsr_sim_node *node, uint32_t index)
{
    struct vsr_sim *sim = node->world;

    for (;;) {
        struct vsr_sim_object *object = vsr_sim_object(node, index);
        struct vsr_sim_connection *connection =
            connection_at(sim, object->connection);
        uint32_t op = vsr_sim_exec_waiter(node, index, VSR_IO_SQE_RECV);
        struct vsr_sim_op *record;
        struct vsr_sim_end *end;
        bool peek;
        size_t taken;
        uint16_t flags = 0;
        uint16_t id = 0;
        uint32_t connection_index;
        uint32_t end_index;

        if (op == VSR_SIM_NONE || connection == NULL) {
            return;
        }
        record = vsr_sim_op(node, op);
        end = &connection->ends[object->end];
        peek = (record->sqe.op_flags & VSR_IO_RECV_PEEK) != 0;
        if (end->reset) {
            vsr_sim_exec_complete(node, op, -ECONNRESET, 0, 0);
            continue;
        }
        if (object->shut_rd) {
            vsr_sim_exec_complete(node, op, 0, 0, 0);
            continue;
        }
        if (end->head == NULL || !end->head->arrived) {
            return;
        }
        if (end->head->fin) {
            vsr_sim_exec_complete(node, op, 0, 0, 0);
            continue;
        }
        if ((record->sqe.flags & VSR_IO_SQE_BUFFER_SELECT) != 0) {
            struct vsr_sim_ring *ring =
                vsr_sim_exec_ring(node, record->sqe.buffer_group);
            struct vsr_sim_provided *buffer;
            size_t offset;
            size_t space;

            if (ring == NULL || ring->count == 0) {
                vsr_sim_exec_complete(node, op, -ENOBUFS, 0, 0);
                continue;
            }
            vsr_sim_ring_verify(node, ring);
            buffer = &ring->slots[ring->head];
            offset = (ring->flags & VSR_IO_BUFFER_RING_INCREMENTAL) != 0
                         ? buffer->consumed
                         : 0;
            space = buffer->buffer.length - offset;
            if (record->sqe.length != 0 && record->sqe.length < space) {
                space = record->sqe.length;
            }
            taken = take(end, (unsigned char *)buffer->buffer.base + offset,
                         space, peek);
            id = buffer->buffer.id;
            flags = (uint16_t)(VSR_IO_CQE_BUFFER |
                               vsr_sim_ring_consume(node, ring,
                                                    (uint32_t)taken));
            if (!record->multishot) {
                record->requested = space;
            }
        } else {
            if (record->sqe.length == 0) {
                vsr_sim_exec_complete(node, op, 0, 0, 0);
                continue;
            }
            taken = take(end, (unsigned char *)(uintptr_t)record->sqe.addr,
                         record->sqe.length, peek);
        }
        connection_index = object->connection;
        end_index = object->end;
        if (record->multishot) {
            vsr_sim_exec_post(node, op, (int32_t)taken, flags, id);
        } else {
            vsr_sim_exec_complete(node, op, (int32_t)taken, flags, id);
        }
        /* Space freed: the peer's parked sends may proceed. */
        connection = connection_at(sim, connection_index);
        if (!peek && connection != NULL) {
            serve_peer(sim, connection, end_index);
        }
    }
}

static unsigned char *gather(const struct vsr_sim_op *op, uint32_t size)
{
    unsigned char *bytes = vsr_sim_alloc(size);
    uint32_t at = 0;

    if ((op->sqe.op_flags & VSR_IO_SEND_VECTORED) == 0) {
        memcpy(bytes, op->sqe.addr, size);
        return bytes;
    }
    for (uint32_t i = 0; i < op->vec_count && at < size; ++i) {
        size_t take_bytes = op->vecs[i].length;

        if (take_bytes > size - at) {
            take_bytes = size - at;
        }
        if (take_bytes > 0) {
            memcpy(bytes + at, op->vecs[i].base, take_bytes);
        }
        at += (uint32_t)take_bytes;
    }
    return bytes;
}

static void serve_send(struct vsr_sim_node *node, uint32_t index)
{
    struct vsr_sim *sim = node->world;

    for (;;) {
        struct vsr_sim_object *object = vsr_sim_object(node, index);
        uint32_t connection_index = object->connection;
        struct vsr_sim_connection *connection =
            connection_at(sim, connection_index);
        uint32_t op = vsr_sim_exec_waiter(node, index, VSR_IO_SQE_SEND);
        struct vsr_sim_op *record;
        struct vsr_sim_end *end;
        struct vsr_sim_end *peer;
        uint64_t space;
        uint64_t size;
        uint64_t due;
        bool reset;

        if (op == VSR_SIM_NONE || connection == NULL) {
            return;
        }
        record = vsr_sim_op(node, op);
        end = &connection->ends[object->end];
        peer = &connection->ends[1 - object->end];
        if (end->reset) {
            vsr_sim_exec_complete(node, op, -ECONNRESET, 0, 0);
            continue;
        }
        if (object->shut_wr) {
            vsr_sim_exec_complete(node, op, -EPIPE, 0, 0);
            continue;
        }
        size = record->requested;
        if (size == 0) {
            vsr_sim_exec_complete(node, op, 0, 0, 0);
            continue;
        }
        space = peer->queued >= VSR_SIM_SOCKET_BUFFER
                    ? 0
                    : VSR_SIM_SOCKET_BUFFER - peer->queued;
        if (space == 0) {
            return;
        }
        if (size > space) {
            size = space;
        }
        due = transmit(sim, connection_index, object->end,
                       gather(record, (uint32_t)size), (uint32_t)size, false,
                       &reset);
        record->due_ns = due == VSR_SIM_NEVER ? sim->now_ns : due;
        vsr_sim_exec_complete(node, op, (int32_t)size, 0, 0);
        if (reset) {
            reset_connection(sim, connection_index);
        }
    }
}

void vsr_sim_net_serve(struct vsr_sim_node *node, uint32_t index)
{
    const struct vsr_sim_object *object = vsr_sim_object(node, index);

    if (!node->alive || object == NULL ||
        object->kind != VSR_SIM_OBJECT_SOCKET) {
        return;
    }
    if (object->state == VSR_SIM_SOCKET_LISTENING) {
        serve_accept(node, index);
    } else if (object->state == VSR_SIM_SOCKET_CONNECTED) {
        serve_recv(node, index);
        object = vsr_sim_object(node, index);
        if (object->kind == VSR_SIM_OBJECT_SOCKET &&
            object->state == VSR_SIM_SOCKET_CONNECTED) {
            serve_send(node, index);
        }
    }
}

/* ------------------------------------------------------------------------
 * Record starts
 * --------------------------------------------------------------------- */

static void park(struct vsr_sim_node *node, uint32_t op, uint32_t object)
{
    vsr_sim_op(node, op)->object = object;
    vsr_sim_exec_arm(node, op, node->world->now_ns, VSR_SIM_ACTION_SOCKET);
    vsr_sim_net_serve(node, object);
}

static void start_socket(struct vsr_sim_node *node, uint32_t op)
{
    const struct vsr_io_sqe *sqe = &vsr_sim_op(node, op)->sqe;
    uint32_t domain = sqe->length;
    uint32_t type = sqe->op_flags;
    uint32_t object;
    int error;

    if (domain != AF_INET && domain != AF_UNIX) {
        vsr_sim_exec_complete(node, op, -EAFNOSUPPORT, 0, 0);
        return;
    }
    if ((type & ~(uint32_t)(0xf | SOCK_NONBLOCK | SOCK_CLOEXEC)) != 0) {
        vsr_sim_exec_complete(node, op, -EINVAL, 0, 0);
        return;
    }
    if ((type & 0xfu) != SOCK_STREAM) {
        vsr_sim_exec_complete(node, op, -ESOCKTNOSUPPORT, 0, 0);
        return;
    }
    if (sqe->offset != 0 &&
        !(domain == AF_INET && sqe->offset == IPPROTO_TCP)) {
        vsr_sim_exec_complete(node, op, -EPROTONOSUPPORT, 0, 0);
        return;
    }
    error = vsr_sim_exec_install(node, sqe, 0, true);
    if (error != 0) {
        vsr_sim_exec_complete(node, op, error, 0, 0);
        return;
    }
    object = vsr_sim_object_new(node, VSR_SIM_OBJECT_SOCKET);
    vsr_sim_object(node, object)->domain = domain;
    vsr_sim_exec_complete(node, op,
                          vsr_sim_exec_install(node, sqe, object, false), 0,
                          0);
}

static void start_bind(struct vsr_sim_node *node, uint32_t op,
                       uint32_t index)
{
    struct vsr_sim_object *object = vsr_sim_object(node, index);
    struct vsr_sim_key key;
    uint32_t peer;
    int error;

    if (object->bound || object->state != VSR_SIM_SOCKET_NEW) {
        vsr_sim_exec_complete(node, op, -EINVAL, 0, 0);
        return;
    }
    error = parse_address(node, vsr_sim_op(node, op), object->domain, true,
                          &key, &peer);
    if (error == 0 && key.family == AF_INET && key.port == 0) {
        ephemeral(node, &key);
    } else if (error == 0 && key_in_use(node, &key)) {
        error = -EADDRINUSE;
    }
    if (error == 0) {
        object->key = key;
        object->bound = 1;
        object->state = VSR_SIM_SOCKET_BOUND;
    }
    vsr_sim_exec_complete(node, op, error, 0, 0);
}

static void start_listen(struct vsr_sim_node *node, uint32_t op,
                         uint32_t index)
{
    struct vsr_sim *sim = node->world;
    struct vsr_sim_object *object = vsr_sim_object(node, index);
    struct vsr_sim_listener *listener;
    uint32_t backlog = vsr_sim_op(node, op)->sqe.length;
    uint32_t created;

    if (object->state == VSR_SIM_SOCKET_LISTENING) {
        listener_at(sim, object->listener)->backlog = backlog;
        vsr_sim_exec_complete(node, op, 0, 0, 0);
        return;
    }
    if (object->state != VSR_SIM_SOCKET_NEW &&
        object->state != VSR_SIM_SOCKET_BOUND) {
        vsr_sim_exec_complete(node, op, -EINVAL, 0, 0);
        return;
    }
    if (!object->bound) {
        if (object->domain != AF_INET) {
            vsr_sim_exec_complete(node, op, -EINVAL, 0, 0);
            return;
        }
        ephemeral(node, &object->key);
        object->bound = 1;
    }
    created = listener_new(sim);
    listener = sim->listeners[created];
    listener->node = node->index;
    listener->object = index;
    listener->backlog = backlog;
    listener->key = object->key;
    object->listener = created;
    object->state = VSR_SIM_SOCKET_LISTENING;
    vsr_sim_exec_complete(node, op, 0, 0, 0);
}

static void restore_unconnected(struct vsr_sim_object *object)
{
    object->state =
        object->bound ? VSR_SIM_SOCKET_BOUND : VSR_SIM_SOCKET_NEW;
}

static void start_connect(struct vsr_sim_node *node, uint32_t op,
                          uint32_t index)
{
    struct vsr_sim *sim = node->world;
    struct vsr_sim_object *object = vsr_sim_object(node, index);
    struct vsr_sim_op *record = vsr_sim_op(node, op);
    uint32_t peer;
    int error;

    if (object->state == VSR_SIM_SOCKET_CONNECTED) {
        vsr_sim_exec_complete(node, op, -EISCONN, 0, 0);
        return;
    }
    if (object->state == VSR_SIM_SOCKET_CONNECTING) {
        vsr_sim_exec_complete(node, op, -EALREADY, 0, 0);
        return;
    }
    if (object->state == VSR_SIM_SOCKET_LISTENING) {
        vsr_sim_exec_complete(node, op, -EINVAL, 0, 0);
        return;
    }
    error = parse_address(node, record, object->domain, false, &record->key,
                          &peer);
    if (error != 0) {
        vsr_sim_exec_complete(node, op, error, 0, 0);
        return;
    }
    record->peer = peer;
    record->object = index;
    if (peer == VSR_SIM_NONE || !sim->nodes[peer].alive ||
        vsr_sim_partitioned(sim, node->index, peer)) {
        object->state = VSR_SIM_SOCKET_CONNECTING;
        vsr_sim_exec_arm(
            node, op,
            vsr_sim_add(sim->now_ns, sim->faults.network.connect_timeout_ns),
            VSR_SIM_ACTION_UNREACHABLE);
        return;
    }
    if (listener_find(sim, peer, &record->key) == VSR_SIM_NONE) {
        vsr_sim_exec_complete(node, op, -ECONNREFUSED, 0, 0);
        return;
    }
    object->state = VSR_SIM_SOCKET_CONNECTING;
    vsr_sim_exec_arm(
        node, op,
        vsr_sim_add(sim->now_ns,
                    peer != node->index
                        ? vsr_sim_range(sim, sim->faults.network.delay_min_ns,
                                        sim->faults.network.delay_max_ns)
                        : 0),
        VSR_SIM_ACTION_CONNECT);
}

void vsr_sim_net_action(struct vsr_sim_node *node, uint32_t op)
{
    struct vsr_sim *sim = node->world;
    struct vsr_sim_op *record = vsr_sim_op(node, op);
    uint32_t index = record->object;
    struct vsr_sim_object *object = vsr_sim_object(node, index);
    uint32_t peer = record->peer;
    uint32_t listener_index;
    uint32_t created;
    struct vsr_sim_connection *connection;
    struct vsr_sim_listener *listener;

    if (record->action == VSR_SIM_ACTION_UNREACHABLE) {
        restore_unconnected(object);
        vsr_sim_exec_complete(node, op, -EHOSTUNREACH, 0, 0);
        return;
    }
    if (!sim->nodes[peer].alive ||
        vsr_sim_partitioned(sim, node->index, peer)) {
        vsr_sim_exec_arm(
            node, op,
            vsr_sim_add(record->started_ns,
                        sim->faults.network.connect_timeout_ns),
            VSR_SIM_ACTION_UNREACHABLE);
        return;
    }
    listener_index = listener_find(sim, peer, &record->key);
    if (listener_index == VSR_SIM_NONE) {
        restore_unconnected(object);
        vsr_sim_exec_complete(node, op, -ECONNREFUSED, 0, 0);
        return;
    }
    created = connection_new(sim);
    connection = sim->connections[created];
    connection->ends[0].node = node->index;
    connection->ends[0].object = index;
    connection->ends[0].attached = 1;
    connection->ends[1].node = peer;
    connection->listener = listener_index;
    listener = sim->listeners[listener_index];
    listener_push(listener, created);
    object->state = VSR_SIM_SOCKET_CONNECTED;
    object->connection = created;
    object->end = 0;
    vsr_sim_exec_complete(node, op, 0, 0, 0);
    listener = listener_at(sim, listener_index);
    if (listener != NULL) {
        vsr_sim_net_serve(node_at(sim, peer), listener->object);
    }
}

void vsr_sim_net_cancel(struct vsr_sim_node *node, uint32_t op)
{
    struct vsr_sim_op *record = vsr_sim_op(node, op);
    struct vsr_sim_object *object;

    if (record->sqe.opcode != VSR_IO_SQE_CONNECT) {
        return;
    }
    object = vsr_sim_object(node, record->object);
    if (object != NULL && object->state == VSR_SIM_SOCKET_CONNECTING) {
        restore_unconnected(object);
    }
}

static int check_memory(struct vsr_sim_node *node, const struct vsr_sim_op *op)
{
    const struct vsr_io_sqe *sqe = &op->sqe;

    if ((sqe->flags & VSR_IO_SQE_FIXED_BUFFER) != 0) {
        if (op->vecs != NULL) {
            for (uint32_t i = 0; i < op->vec_count; ++i) {
                int error = vsr_sim_exec_region(node, sqe, op->vecs[i].base,
                                                op->vecs[i].length);

                if (error != 0) {
                    return error;
                }
            }
            return 0;
        }
        return vsr_sim_exec_region(node, sqe, sqe->addr, sqe->length);
    }
    if (op->vecs != NULL) {
        for (uint32_t i = 0; i < op->vec_count; ++i) {
            if (op->vecs[i].base == NULL && op->vecs[i].length > 0) {
                return -EFAULT;
            }
        }
        return 0;
    }
    return sqe->addr == NULL && sqe->length > 0 ? -EFAULT : 0;
}

static void start_recv(struct vsr_sim_node *node, uint32_t op,
                       uint32_t index)
{
    const struct vsr_sim_object *object = vsr_sim_object(node, index);
    const struct vsr_sim_op *record = vsr_sim_op(node, op);
    const struct vsr_io_sqe *sqe = &record->sqe;
    bool select = (sqe->flags & VSR_IO_SQE_BUFFER_SELECT) != 0;
    int error = 0;

    if ((sqe->op_flags & ~(uint32_t)(VSR_IO_RECV_MULTISHOT |
                                     VSR_IO_RECV_PEEK)) != 0 ||
        (record->multishot && !select) ||
        (record->multishot &&
         (sqe->op_flags & VSR_IO_RECV_PEEK) != 0) ||
        (select && (sqe->flags & VSR_IO_SQE_FIXED_BUFFER) != 0)) {
        error = -EINVAL;
    } else if (object->state != VSR_SIM_SOCKET_CONNECTED) {
        error = -ENOTCONN;
    } else if (select) {
        const struct vsr_sim_ring *ring =
            vsr_sim_exec_ring(node, sqe->buffer_group);

        /* Buffer selection happens at every attempt, the first included. */
        if (ring == NULL || ring->count == 0) {
            error = -ENOBUFS;
        }
    } else {
        error = check_memory(node, record);
    }
    if (error != 0) {
        vsr_sim_exec_complete(node, op, error, 0, 0);
        return;
    }
    park(node, op, index);
}

static void start_send(struct vsr_sim_node *node, uint32_t op,
                       uint32_t index)
{
    const struct vsr_sim_object *object = vsr_sim_object(node, index);
    const struct vsr_sim_op *record = vsr_sim_op(node, op);
    int error = 0;

    if ((record->sqe.op_flags & ~(uint32_t)(VSR_IO_SEND_ZERO_COPY |
                                            VSR_IO_SEND_VECTORED)) != 0 ||
        (record->sqe.flags & VSR_IO_SQE_BUFFER_SELECT) != 0) {
        error = -EINVAL;
    } else if (object->state != VSR_SIM_SOCKET_CONNECTED) {
        error = -ENOTCONN;
    } else {
        error = check_memory(node, record);
    }
    if (error == 0 && record->requested > INT32_MAX) {
        error = -EINVAL;
    }
    if (error != 0) {
        vsr_sim_exec_complete(node, op, error, 0, 0);
        return;
    }
    park(node, op, index);
}

static void send_fin(struct vsr_sim *sim, uint32_t connection_index,
                     uint32_t end)
{
    struct vsr_sim_connection *connection =
        connection_at(sim, connection_index);
    bool reset;

    if (connection == NULL || connection->ends[end].reset ||
        connection->ends[end].fin_sent) {
        return;
    }
    connection->ends[end].fin_sent = 1;
    (void)transmit(sim, connection_index, end, NULL, 0, true, &reset);
}

static void start_shutdown(struct vsr_sim_node *node, uint32_t op,
                           uint32_t index)
{
    struct vsr_sim_object *object = vsr_sim_object(node, index);
    uint32_t how = vsr_sim_op(node, op)->sqe.length;

    if (how != SHUT_RD && how != SHUT_WR && how != SHUT_RDWR) {
        vsr_sim_exec_complete(node, op, -EINVAL, 0, 0);
        return;
    }
    if (object->state != VSR_SIM_SOCKET_CONNECTED) {
        vsr_sim_exec_complete(node, op, -ENOTCONN, 0, 0);
        return;
    }
    if (how != SHUT_RD && !object->shut_wr) {
        object->shut_wr = 1;
        send_fin(node->world, object->connection, object->end);
    }
    if (how != SHUT_WR) {
        object->shut_rd = 1;
    }
    vsr_sim_exec_complete(node, op, 0, 0, 0);
    vsr_sim_net_serve(node, index);
}

static void start_sockopt(struct vsr_sim_node *node, uint32_t op,
                          uint32_t index)
{
    struct vsr_sim_object *object = vsr_sim_object(node, index);
    const struct vsr_sim_op *record = vsr_sim_op(node, op);
    uint32_t level = record->sqe.op_flags >> 16;
    uint32_t name = record->sqe.op_flags & 0xffffu;
    uint32_t *value;
    int integer;

    if (level == SOL_SOCKET && name == SO_KEEPALIVE) {
        value = &object->keepalive;
    } else if (level == IPPROTO_TCP && name == TCP_NODELAY) {
        if (object->domain != AF_INET) {
            vsr_sim_exec_complete(node, op, -EOPNOTSUPP, 0, 0);
            return;
        }
        value = &object->nodelay;
    } else {
        vsr_sim_exec_complete(node, op, -ENOPROTOOPT, 0, 0);
        return;
    }
    if (record->sqe.opcode == VSR_IO_SQE_SETSOCKOPT) {
        if (record->option_length < sizeof(integer)) {
            vsr_sim_exec_complete(node, op, -EINVAL, 0, 0);
            return;
        }
        memcpy(&integer, record->option, sizeof(integer));
        *value = integer != 0;
        vsr_sim_exec_complete(node, op, 0, 0, 0);
        return;
    }
    if (record->sqe.addr == NULL) {
        vsr_sim_exec_complete(node, op, -EFAULT, 0, 0);
        return;
    }
    if (record->sqe.length < sizeof(integer)) {
        vsr_sim_exec_complete(node, op, -EINVAL, 0, 0);
        return;
    }
    integer = *value != 0;
    memcpy((void *)(uintptr_t)record->sqe.addr, &integer, sizeof(integer));
    vsr_sim_exec_complete(node, op, (int32_t)sizeof(integer), 0, 0);
}

void vsr_sim_net_start(struct vsr_sim_node *node, uint32_t op)
{
    const struct vsr_io_sqe *sqe = &vsr_sim_op(node, op)->sqe;
    uint32_t index;
    int error;

    if (sqe->opcode == VSR_IO_SQE_SOCKET) {
        start_socket(node, op);
        return;
    }
    error = vsr_sim_exec_resolve(
        node, sqe->fd, (sqe->flags & VSR_IO_SQE_FIXED_FILE) != 0, &index);
    if (error == 0 &&
        vsr_sim_object(node, index)->kind != VSR_SIM_OBJECT_SOCKET) {
        error = -ENOTSOCK;
    }
    if (error != 0) {
        vsr_sim_exec_complete(node, op, error, 0, 0);
        return;
    }
    switch (sqe->opcode) {
    case VSR_IO_SQE_BIND:
        start_bind(node, op, index);
        break;
    case VSR_IO_SQE_LISTEN:
        start_listen(node, op, index);
        break;
    case VSR_IO_SQE_CONNECT:
        start_connect(node, op, index);
        break;
    case VSR_IO_SQE_ACCEPT: {
        const struct vsr_sim_op *record = vsr_sim_op(node, op);

        if (vsr_sim_object(node, index)->state !=
                VSR_SIM_SOCKET_LISTENING ||
            (record->sqe.op_flags & ~(uint32_t)VSR_IO_ACCEPT_MULTISHOT) !=
                0 ||
            (record->multishot &&
             (record->sqe.flags & VSR_IO_SQE_DIRECT) != 0 &&
             record->sqe.fd2 != VSR_IO_SLOT_ALLOC)) {
            vsr_sim_exec_complete(node, op, -EINVAL, 0, 0);
            break;
        }
        park(node, op, index);
        break;
    }
    case VSR_IO_SQE_RECV:
        start_recv(node, op, index);
        break;
    case VSR_IO_SQE_SEND:
        start_send(node, op, index);
        break;
    case VSR_IO_SQE_SHUTDOWN:
        start_shutdown(node, op, index);
        break;
    case VSR_IO_SQE_SETSOCKOPT:
    case VSR_IO_SQE_GETSOCKOPT:
        start_sockopt(node, op, index);
        break;
    default:
        vsr_sim_exec_complete(node, op, -EINVAL, 0, 0);
        break;
    }
}

/* ------------------------------------------------------------------------
 * Close, crash, partitions
 * --------------------------------------------------------------------- */

static uint32_t any_waiter(const struct vsr_sim_node *node, uint32_t object)
{
    uint32_t found = VSR_SIM_NONE;
    uint64_t sequence = UINT64_MAX;

    for (uint32_t i = 0; i < node->ops_count; ++i) {
        const struct vsr_sim_op *op = node->ops[i];

        if (op->state == VSR_SIM_OP_WAITING && op->object == object &&
            !op->holds &&
            (op->action == VSR_SIM_ACTION_SOCKET ||
             op->action == VSR_SIM_ACTION_CONNECT ||
             op->action == VSR_SIM_ACTION_UNREACHABLE) &&
            op->sequence < sequence) {
            found = i;
            sequence = op->sequence;
        }
    }
    return found;
}

static void drop_listener(struct vsr_sim *sim, uint32_t index)
{
    struct vsr_sim_listener *listener = listener_at(sim, index);

    if (listener == NULL) {
        return;
    }
    while (listener->pending_count > 0) {
        reset_connection(sim, listener->pending[0]);
    }
    listener->used = 0;
}

static void detach(struct vsr_sim *sim, uint32_t connection_index,
                   uint32_t end_index, bool graceful)
{
    struct vsr_sim_connection *connection =
        connection_at(sim, connection_index);
    struct vsr_sim_end *end;

    if (connection == NULL) {
        return;
    }
    end = &connection->ends[end_index];
    if (graceful) {
        send_fin(sim, connection_index, end_index);
        connection = connection_at(sim, connection_index);
        if (connection == NULL) {
            return;
        }
        end = &connection->ends[end_index];
    }
    free_queue(end);
    end->object = VSR_SIM_NONE;
    end->closed = 1;
    connection_maybe_free(sim, connection_index);
}

void vsr_sim_net_close(struct vsr_sim_node *node, uint32_t index)
{
    struct vsr_sim *sim = node->world;
    struct vsr_sim_object *object;

    for (;;) {
        uint32_t op = any_waiter(node, index);

        if (op == VSR_SIM_NONE) {
            break;
        }
        vsr_sim_exec_complete(node, op, -ECANCELED, 0, 0);
    }
    object = vsr_sim_object(node, index);
    if (object->state == VSR_SIM_SOCKET_LISTENING) {
        drop_listener(sim, object->listener);
    } else if (object->state == VSR_SIM_SOCKET_CONNECTED) {
        detach(sim, object->connection, object->end, true);
    }
    object->bound = 0;
}

void vsr_sim_net_crash(struct vsr_sim_node *node)
{
    struct vsr_sim *sim = node->world;

    for (uint32_t i = 0; i < node->objects_count; ++i) {
        struct vsr_sim_object *object = node->objects[i];

        if (object->kind != VSR_SIM_OBJECT_SOCKET) {
            continue;
        }
        if (object->state == VSR_SIM_SOCKET_LISTENING) {
            drop_listener(sim, object->listener);
        } else if (object->state == VSR_SIM_SOCKET_CONNECTED) {
            reset_connection(sim, object->connection);
            detach(sim, object->connection, object->end, false);
        }
    }
}

static bool between(const struct vsr_sim_connection *connection, uint32_t a,
                    uint32_t b)
{
    return (connection->ends[0].node == a && connection->ends[1].node == b) ||
           (connection->ends[0].node == b && connection->ends[1].node == a);
}

void vsr_sim_net_partition(struct vsr_sim *sim, uint32_t a, uint32_t b)
{
    for (uint32_t i = 0; i < sim->connections_count; ++i) {
        struct vsr_sim_connection *connection = connection_at(sim, i);

        if (connection == NULL || !between(connection, a, b) ||
            (connection->ends[0].reset && connection->ends[1].reset)) {
            continue;
        }
        for (uint32_t end = 0; end < 2; ++end) {
            struct vsr_sim_end *target = &connection->ends[end];

            for (struct vsr_sim_segment *segment = target->head;
                 segment != NULL; segment = segment->next) {
                if (segment->arrived || segment->due_ns == VSR_SIM_NEVER) {
                    continue;
                }
                segment->due_ns = VSR_SIM_NEVER;
                if (!segment->fin) {
                    vsr_sim_emit(sim, VSR_SIM_TRACE_DROP,
                                 connection->ends[1 - end].node, target->node,
                                 segment->length);
                }
            }
        }
        stall(sim, i);
    }
}

void vsr_sim_net_reset_between(struct vsr_sim *sim, uint32_t a, uint32_t b)
{
    for (uint32_t i = 0; i < sim->connections_count; ++i) {
        struct vsr_sim_connection *connection = connection_at(sim, i);

        if (connection == NULL || !between(connection, a, b) ||
            (connection->ends[0].reset && connection->ends[1].reset)) {
            continue;
        }
        reset_connection(sim, i);
    }
}

void vsr_sim_net_free(struct vsr_sim *sim)
{
    for (uint32_t i = 0; i < sim->connections_count; ++i) {
        free_queue(&sim->connections[i]->ends[0]);
        free_queue(&sim->connections[i]->ends[1]);
        free(sim->connections[i]);
    }
    free(sim->connections);
    for (uint32_t i = 0; i < sim->listeners_count; ++i) {
        free(sim->listeners[i]->pending);
        free(sim->listeners[i]);
    }
    free(sim->listeners);
}
