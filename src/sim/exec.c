#include "config.h"

#define _GNU_SOURCE 1

#include "sim/sim.h"

#include <errno.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------------
 * Handles
 * --------------------------------------------------------------------- */

static struct vsr_sim_node *handle_node(void *ctx)
{
    struct vsr_sim_handle *handle = ctx;

    if (handle == NULL || handle->node == NULL) {
        vsr_sim_fatal("executor call through a null handle", VSR_SIM_NO_NODE,
                      0);
    }
    if (!handle->node->alive ||
        handle->incarnation != handle->node->incarnation) {
        vsr_sim_fatal("executor handle used after its node crashed",
                      handle->node->index, 0);
    }
    return handle->node;
}

/* ------------------------------------------------------------------------
 * Objects and descriptors
 * --------------------------------------------------------------------- */

struct vsr_sim_object *vsr_sim_object(const struct vsr_sim_node *node,
                                      uint32_t object)
{
    if (object >= node->objects_count) {
        return NULL;
    }
    return node->objects[object];
}

uint32_t vsr_sim_object_new(struct vsr_sim_node *node, uint32_t kind)
{
    uint32_t index = 0;
    struct vsr_sim_object *object;

    while (index < node->objects_count &&
           node->objects[index]->kind != VSR_SIM_OBJECT_FREE) {
        ++index;
    }
    if (index == node->objects_count) {
        node->objects = vsr_sim_grow_table(node->objects, (size_t)index + 1);
        node->objects[index] = vsr_sim_alloc(sizeof(struct vsr_sim_object));
        ++node->objects_count;
    }
    object = node->objects[index];
    memset(object, 0, sizeof(*object));
    object->kind = kind;
    object->index = index;
    object->inode = VSR_SIM_NONE;
    object->listener = VSR_SIM_NONE;
    object->connection = VSR_SIM_NONE;
    return index;
}

void vsr_sim_object_release(struct vsr_sim_node *node, uint32_t index)
{
    struct vsr_sim_object *object = vsr_sim_object(node, index);

    if (object == NULL || object->kind == VSR_SIM_OBJECT_FREE ||
        object->refs == 0) {
        vsr_sim_fatal("object reference underflow", node->index, index);
    }
    if (--object->refs > 0) {
        return;
    }
    switch (object->kind) {
    case VSR_SIM_OBJECT_SOCKET:
        vsr_sim_net_close(node, index);
        break;
    case VSR_SIM_OBJECT_FILE:
    case VSR_SIM_OBJECT_DIR:
        vsr_sim_disk_close(node, index);
        break;
    default:
        break;
    }
    node->objects[index]->kind = VSR_SIM_OBJECT_FREE;
}

int vsr_sim_exec_resolve(struct vsr_sim_node *node, int32_t fd, bool fixed,
                         uint32_t *object)
{
    int32_t value;

    if (fixed) {
        if (fd < 0 || (uint32_t)fd >= node->slots_count) {
            return -EBADF;
        }
        value = node->slots[fd];
    } else {
        if (fd < 0 || (uint32_t)fd >= node->fds_count) {
            return -EBADF;
        }
        value = node->fds[fd];
    }
    if (value < 0) {
        return -EBADF;
    }
    *object = (uint32_t)value;
    return 0;
}

int vsr_sim_exec_install(struct vsr_sim_node *node,
                         const struct vsr_io_sqe *sqe, uint32_t object,
                         bool check)
{
    struct vsr_sim_object *target = vsr_sim_object(node, object);

    if ((sqe->flags & VSR_IO_SQE_DIRECT) != 0) {
        uint32_t slot = 0;

        if (node->slots_count == 0) {
            return -ENXIO;
        }
        if (sqe->fd2 == VSR_IO_SLOT_ALLOC) {
            while (slot < node->slots_count && node->slots[slot] >= 0) {
                ++slot;
            }
            if (slot == node->slots_count) {
                return -ENFILE;
            }
        } else if (sqe->fd2 < 0 || (uint32_t)sqe->fd2 >= node->slots_count) {
            return -EINVAL;
        } else {
            slot = (uint32_t)sqe->fd2;
        }
        if (check) {
            return 0;
        }
        if (node->slots[slot] >= 0) {
            uint32_t old = (uint32_t)node->slots[slot];

            node->slots[slot] = -1;
            vsr_sim_object_release(node, old);
        }
        node->slots[slot] = (int32_t)object;
        ++target->refs;
        return (int)slot;
    }
    {
        uint32_t fd = VSR_SIM_FIRST_FD;

        while (fd < node->fds_count && node->fds[fd] >= 0) {
            ++fd;
        }
        if (check) {
            return 0;
        }
        if (fd >= node->fds_count) {
            uint32_t grown = fd + 16;

            node->fds = vsr_sim_grow(node->fds, grown, sizeof(*node->fds));
            for (uint32_t i = node->fds_count; i < grown; ++i) {
                node->fds[i] = -1;
            }
            node->fds_count = grown;
        }
        node->fds[fd] = (int32_t)object;
        ++target->refs;
        return (int)fd;
    }
}

int vsr_sim_exec_region(const struct vsr_sim_node *node,
                        const struct vsr_io_sqe *sqe, const void *base,
                        size_t length)
{
    const struct vsr_io_region *region;
    uintptr_t first;
    uintptr_t at;

    if (sqe->buffer_index >= node->regions_count) {
        return -EFAULT;
    }
    region = &node->regions[sqe->buffer_index];
    if (region->base == NULL || base == NULL) {
        return -EFAULT;
    }
    first = (uintptr_t)region->base;
    at = (uintptr_t)base;
    if (at < first || at - first > region->size ||
        length > region->size - (at - first)) {
        return -EFAULT;
    }
    return 0;
}

/* ------------------------------------------------------------------------
 * Ownership checksums
 * --------------------------------------------------------------------- */

uint64_t vsr_sim_checksum(const void *bytes, size_t size, uint64_t seed)
{
    const uint64_t prime = UINT64_C(4294967291);
    const unsigned char *at = bytes;
    uint64_t a = (seed & UINT32_MAX) % prime;
    uint64_t b = (seed >> 32) % prime;

    while (size > 0) {
        size_t chunk = size < 4096 ? size : 4096;

        for (size_t i = 0; i < chunk; ++i) {
            a += at[i];
            b += a;
        }
        a %= prime;
        b %= prime;
        at += chunk;
        size -= chunk;
    }
    return (b << 32) | a;
}

/* The bytes a record reads from caller memory: WRITE, WRITEV and SEND. */
static bool op_source_checksum(const struct vsr_sim_op *op, uint64_t *sum)
{
    const struct vsr_io_sqe *sqe = &op->sqe;
    uint64_t value = 1;

    switch (sqe->opcode) {
    case VSR_IO_SQE_WRITE:
        break;
    case VSR_IO_SQE_WRITEV:
        if (op->vecs == NULL) {
            return false;
        }
        for (uint32_t i = 0; i < op->vec_count; ++i) {
            if (op->vecs[i].base == NULL && op->vecs[i].length > 0) {
                return false;
            }
            value =
                vsr_sim_checksum(op->vecs[i].base, op->vecs[i].length, value);
        }
        *sum = value;
        return true;
    case VSR_IO_SQE_SEND:
        if ((sqe->op_flags & VSR_IO_SEND_VECTORED) != 0) {
            if (op->vecs == NULL) {
                return false;
            }
            for (uint32_t i = 0; i < op->vec_count; ++i) {
                if (op->vecs[i].base == NULL && op->vecs[i].length > 0) {
                    return false;
                }
                value = vsr_sim_checksum(op->vecs[i].base, op->vecs[i].length,
                                         value);
            }
            *sum = value;
            return true;
        }
        break;
    default:
        return false;
    }
    if (sqe->addr == NULL) {
        return false;
    }
    *sum = vsr_sim_checksum(sqe->addr, sqe->length, value);
    return true;
}

static void verify_op(const struct vsr_sim_node *node,
                      const struct vsr_sim_op *op)
{
    uint64_t sum;

    if (!op->checked) {
        return;
    }
    if (!op_source_checksum(op, &sum) || sum != op->checksum) {
        vsr_sim_fatal("bytes of a submitted record changed before the "
                      "executor released them (ownership violation)",
                      node->index, op->sqe.user_data);
    }
}

static bool overlaps(const void *a, size_t a_length, const void *b,
                     size_t b_length)
{
    uintptr_t a0 = (uintptr_t)a;
    uintptr_t b0 = (uintptr_t)b;

    return a_length > 0 && b_length > 0 && a0 < b0 + b_length &&
           b0 < a0 + a_length;
}

/* Whether [base, base + length) meets bytes a record still reads: the
 * source of an in-flight WRITE, WRITEV or SEND, including a zero-copy send
 * whose NOTIF has not been posted. */
static bool op_source_overlaps(const struct vsr_sim_op *op, const void *base,
                               size_t length)
{
    if (op->state == VSR_SIM_OP_FREE || !op->checked) {
        return false;
    }
    if (op->vecs != NULL) {
        for (uint32_t i = 0; i < op->vec_count; ++i) {
            if (overlaps(base, length, op->vecs[i].base, op->vecs[i].length)) {
                return true;
            }
        }
        return false;
    }
    return overlaps(base, length, op->sqe.addr, op->sqe.length);
}

/* ------------------------------------------------------------------------
 * Operation table and completion queue
 * --------------------------------------------------------------------- */

struct vsr_sim_op *vsr_sim_op(const struct vsr_sim_node *node, uint32_t op)
{
    if (op >= node->ops_count) {
        return NULL;
    }
    return node->ops[op];
}

static void op_release_memory(struct vsr_sim_op *op)
{
    free(op->vecs);
    free(op->path);
    free(op->path2);
    free(op->raw);
    op->raw = NULL;
    op->raw_length = 0;
    for (uint64_t i = 0; i < op->capture_count; ++i) {
        free(op->captures[i].bytes);
    }
    free(op->captures);
    op->vecs = NULL;
    op->path = NULL;
    op->path2 = NULL;
    op->captures = NULL;
    op->capture_count = 0;
    op->vec_count = 0;
}

static uint32_t op_alloc(struct vsr_sim_node *node)
{
    uint32_t index = 0;
    struct vsr_sim_op *op;
    uint32_t generation;

    while (index < node->ops_count &&
           node->ops[index]->state != VSR_SIM_OP_FREE) {
        ++index;
    }
    if (index == node->ops_count) {
        node->ops = vsr_sim_grow_table(node->ops, (size_t)index + 1);
        node->ops[index] = vsr_sim_alloc(sizeof(struct vsr_sim_op));
        ++node->ops_count;
    }
    op = node->ops[index];
    generation = op->generation + 1;
    memset(op, 0, sizeof(*op));
    op->generation = generation;
    op->link_next = VSR_SIM_NONE;
    op->object = VSR_SIM_NONE;
    op->region = VSR_SIM_NONE;
    op->peer = VSR_SIM_NONE;
    op->requested = UINT64_MAX;
    ++node->inflight;
    return index;
}

static void op_free(struct vsr_sim_node *node, uint32_t index)
{
    struct vsr_sim_op *op = node->ops[index];

    op_release_memory(op);
    op->state = VSR_SIM_OP_FREE;
    op->action = VSR_SIM_ACTION_NONE;
    ++op->generation;
    --node->inflight;
}

static void post(struct vsr_sim_node *node, uint64_t user_data, int32_t result,
                 uint16_t flags, uint16_t buffer_id)
{
    struct vsr_sim *sim = node->world;
    struct vsr_io_cqe *cqe;
    struct vsr_sim_trace_event event;

    if (node->cqes_count == node->cqes_capacity) {
        uint32_t capacity =
            node->cqes_capacity == 0 ? 64 : node->cqes_capacity * 2;
        struct vsr_io_cqe *grown =
            vsr_sim_alloc(sizeof(*grown) * (size_t)capacity);

        for (uint32_t i = 0; i < node->cqes_count; ++i) {
            grown[i] = node->cqes[(node->cqes_head + i) % node->cqes_capacity];
        }
        free(node->cqes);
        node->cqes = grown;
        node->cqes_head = 0;
        node->cqes_capacity = capacity;
    }
    cqe =
        &node->cqes[(node->cqes_head + node->cqes_count) % node->cqes_capacity];
    cqe->user_data = user_data;
    cqe->result = result;
    cqe->flags = flags;
    cqe->buffer_id = buffer_id;
    ++node->cqes_count;
    if (sim->trace.event != NULL) {
        memset(&event, 0, sizeof(event));
        event.kind = VSR_SIM_TRACE_COMPLETE;
        event.node = node->index;
        event.peer = VSR_SIM_NO_NODE;
        event.user_data = user_data;
        event.result = result;
        event.bytes = flags;
        event.now_ns = sim->now_ns;
        vsr_sim_emit_event(sim, &event);
    }
}

void vsr_sim_exec_post(struct vsr_sim_node *node, uint32_t index,
                       int32_t result, uint16_t flags, uint16_t buffer_id)
{
    post(node, node->ops[index]->sqe.user_data, result,
         (uint16_t)(flags | VSR_IO_CQE_MORE), buffer_id);
}

void vsr_sim_exec_arm(struct vsr_sim_node *node, uint32_t index,
                      uint64_t due_ns, uint32_t action)
{
    struct vsr_sim *sim = node->world;
    struct vsr_sim_op *op = node->ops[index];

    if (op->state != VSR_SIM_OP_NOTIF) {
        op->state = VSR_SIM_OP_WAITING;
    }
    op->action = action;
    op->due_ns = due_ns < sim->now_ns ? sim->now_ns : due_ns;
    if (action != VSR_SIM_ACTION_SOCKET) {
        vsr_sim_schedule(sim, op->due_ns, VSR_SIM_EVENT_OP, node->index, index,
                         0, op->generation);
    }
}

static bool link_success(const struct vsr_sim_op *op, int32_t result)
{
    if (result < 0) {
        return false;
    }
    return op->requested == UINT64_MAX || (uint64_t)result == op->requested;
}

static void start(struct vsr_sim_node *node, uint32_t index);

void vsr_sim_exec_complete(struct vsr_sim_node *node, uint32_t index,
                           int32_t result, uint16_t flags, uint16_t buffer_id)
{
    struct vsr_sim_op *op = node->ops[index];
    uint32_t next = op->link_next;
    bool success = link_success(op, result);
    bool zero_copy = op->zero_copy != 0;

    if (op->state == VSR_SIM_OP_FREE || op->state == VSR_SIM_OP_NOTIF) {
        vsr_sim_fatal("completion of a finished record", node->index,
                      op->sqe.user_data);
    }
    if (!zero_copy) {
        verify_op(node, op);
    }
    if ((op->sqe.flags & VSR_IO_SQE_SKIP_SUCCESS) == 0 || result < 0) {
        post(node, op->sqe.user_data, result,
             (uint16_t)(flags | (zero_copy ? VSR_IO_CQE_MORE : 0)), buffer_id);
    }
    op->link_next = VSR_SIM_NONE;
    if (op->holds) {
        uint32_t object = op->object;

        op->holds = 0;
        op->object = VSR_SIM_NONE;
        vsr_sim_object_release(node, object);
        op = node->ops[index];
    }
    op->object = VSR_SIM_NONE;
    if (zero_copy) {
        uint64_t due = op->due_ns;

        op->state = VSR_SIM_OP_NOTIF;
        ++op->generation;
        vsr_sim_exec_arm(node, index, due, VSR_SIM_ACTION_NOTIF);
    } else {
        op_free(node, index);
    }
    if (next != VSR_SIM_NONE) {
        if (success) {
            node->ops[next]->state = VSR_SIM_OP_PENDING;
            start(node, next);
        } else {
            vsr_sim_exec_complete(node, next, -ECANCELED, 0, 0);
        }
    }
}

uint32_t vsr_sim_exec_waiter(const struct vsr_sim_node *node, uint32_t object,
                             uint8_t opcode)
{
    uint32_t found = VSR_SIM_NONE;
    uint64_t sequence = UINT64_MAX;

    for (uint32_t i = 0; i < node->ops_count; ++i) {
        const struct vsr_sim_op *op = node->ops[i];

        if (op->state == VSR_SIM_OP_WAITING &&
            op->action == VSR_SIM_ACTION_SOCKET && op->object == object &&
            op->sqe.opcode == opcode && op->sequence < sequence) {
            found = i;
            sequence = op->sequence;
        }
    }
    return found;
}

void vsr_sim_exec_hold(struct vsr_sim_node *node, uint32_t index,
                       uint32_t object)
{
    struct vsr_sim_op *op = node->ops[index];
    struct vsr_sim_object *target = vsr_sim_object(node, object);

    if (op->holds || target == NULL) {
        vsr_sim_fatal("record holds two objects or none", node->index,
                      op->sqe.user_data);
    }
    op->object = object;
    op->holds = 1;
    ++target->refs;
}

/* ------------------------------------------------------------------------
 * Timers, TIMEOUT_UPDATE and CANCEL
 * --------------------------------------------------------------------- */

static uint64_t timer_due(struct vsr_sim_node *node, uint64_t offset,
                          bool absolute)
{
    struct vsr_sim *sim = node->world;
    uint64_t deadline =
        absolute ? offset : vsr_sim_add(vsr_sim_node_now(node), offset);
    uint64_t due =
        deadline > node->clock_offset_ns ? deadline - node->clock_offset_ns : 0;

    if (due < sim->now_ns) {
        due = sim->now_ns;
    }
    return vsr_sim_add(due,
                       vsr_sim_range(sim, 0, sim->faults.clock.jitter_max_ns));
}

static void start_timeout(struct vsr_sim_node *node, uint32_t index)
{
    struct vsr_sim_op *op = node->ops[index];
    uint64_t due = timer_due(node, op->sqe.offset,
                             (op->sqe.op_flags & VSR_IO_TIMEOUT_ABSOLUTE) != 0);

    vsr_sim_exec_arm(node, index, due, VSR_SIM_ACTION_TIMEOUT);
}

static void start_timeout_update(struct vsr_sim_node *node, uint32_t index)
{
    struct vsr_sim_op *op = node->ops[index];
    uint32_t found = VSR_SIM_NONE;
    uint64_t sequence = UINT64_MAX;
    uint64_t due;

    for (uint32_t i = 0; i < node->ops_count; ++i) {
        const struct vsr_sim_op *other = node->ops[i];

        if (other->state == VSR_SIM_OP_WAITING &&
            other->action == VSR_SIM_ACTION_TIMEOUT &&
            other->sqe.user_data == op->target && other->sequence < sequence) {
            found = i;
            sequence = other->sequence;
        }
    }
    if (found == VSR_SIM_NONE) {
        vsr_sim_exec_complete(node, index, -ENOENT, 0, 0);
        return;
    }
    due = timer_due(node, op->sqe.offset,
                    (op->sqe.op_flags & VSR_IO_TIMEOUT_ABSOLUTE) != 0);
    ++node->ops[found]->generation;
    vsr_sim_exec_arm(node, found, due, VSR_SIM_ACTION_TIMEOUT);
    vsr_sim_exec_complete(node, index, 0, 0, 0);
}

/* BY_FD matches the object the descriptor names now, as io_uring compares
 * the file a request holds with the one the cancel's fd resolves to; a
 * record parked on an object whose descriptor was closed and reused is
 * not on the new descriptor. by_object is that object, or NONE to match
 * on user_data. */
static bool cancel_matches(const struct vsr_sim_op *cancel,
                           const struct vsr_sim_op *op, uint32_t by_object)
{
    if (op == cancel || op->state != VSR_SIM_OP_WAITING) {
        return false;
    }
    if ((cancel->sqe.op_flags & VSR_IO_CANCEL_BY_FD) != 0) {
        return op->object == by_object;
    }
    return op->sqe.user_data == cancel->sqe.offset;
}

/* Returns 1 when cancelled, 0 when already completing (disk work). */
static int cancel_one(struct vsr_sim_node *node, uint32_t index)
{
    struct vsr_sim_op *op = node->ops[index];

    if (op->action == VSR_SIM_ACTION_DISK) {
        return 0;
    }
    if (op->action == VSR_SIM_ACTION_SOCKET ||
        op->action == VSR_SIM_ACTION_CONNECT ||
        op->action == VSR_SIM_ACTION_UNREACHABLE) {
        vsr_sim_net_cancel(node, index);
    }
    vsr_sim_exec_complete(node, index, -ECANCELED, 0, 0);
    return 1;
}

static void start_cancel(struct vsr_sim_node *node, uint32_t index)
{
    const struct vsr_sim_op *cancel = node->ops[index];
    bool all = (cancel->sqe.op_flags & VSR_IO_CANCEL_ALL) != 0;
    uint32_t by_object = VSR_SIM_NONE;
    uint32_t *matches;
    uint32_t count = 0;
    int32_t cancelled = 0;
    int32_t busy = 0;

    if ((cancel->sqe.op_flags &
         ~(uint32_t)(VSR_IO_CANCEL_BY_FD | VSR_IO_CANCEL_ALL)) != 0) {
        vsr_sim_exec_complete(node, index, -EINVAL, 0, 0);
        return;
    }
    if ((cancel->sqe.op_flags & VSR_IO_CANCEL_BY_FD) != 0) {
        int error = vsr_sim_exec_resolve(
            node, cancel->sqe.fd,
            (cancel->sqe.flags & VSR_IO_SQE_FIXED_FILE) != 0, &by_object);

        if (error != 0) {
            vsr_sim_exec_complete(node, index, error, 0, 0);
            return;
        }
    }
    matches = vsr_sim_alloc(sizeof(*matches) * ((size_t)node->ops_count + 1));
    for (uint32_t i = 0; i < node->ops_count; ++i) {
        if (cancel_matches(cancel, node->ops[i], by_object)) {
            uint32_t at = count++;

            /* Insertion in submission order. */
            while (at > 0 && node->ops[matches[at - 1]]->sequence >
                                 node->ops[i]->sequence) {
                matches[at] = matches[at - 1];
                --at;
            }
            matches[at] = i;
        }
    }
    for (uint32_t i = 0; i < count; ++i) {
        const struct vsr_sim_op *op = node->ops[matches[i]];

        /* An earlier cancellation may have ended a chain member. */
        if (op->state != VSR_SIM_OP_WAITING) {
            continue;
        }
        if (cancel_one(node, matches[i]) != 0) {
            ++cancelled;
        } else {
            ++busy;
        }
        if (!all) {
            break;
        }
    }
    free(matches);
    if (cancelled == 0 && busy == 0) {
        vsr_sim_exec_complete(node, index, -ENOENT, 0, 0);
    } else if (cancelled == 0) {
        vsr_sim_exec_complete(node, index, -EALREADY, 0, 0);
    } else {
        vsr_sim_exec_complete(node, index, all ? cancelled : 0, 0, 0);
    }
}

/* ------------------------------------------------------------------------
 * CLOSE
 * --------------------------------------------------------------------- */

static void start_close(struct vsr_sim_node *node, uint32_t index)
{
    const struct vsr_io_sqe *sqe = &node->ops[index]->sqe;
    uint32_t object;
    int error;

    if ((sqe->flags & VSR_IO_SQE_FIXED_FILE) != 0) {
        error = vsr_sim_exec_resolve(node, sqe->fd, true, &object);
        if (error == 0) {
            node->slots[sqe->fd] = -1;
        }
    } else {
        error = vsr_sim_exec_resolve(node, sqe->fd, false, &object);
        if (error == 0) {
            node->fds[sqe->fd] = -1;
        }
    }
    if (error == 0) {
        vsr_sim_object_release(node, object);
    }
    vsr_sim_exec_complete(node, index, error, 0, 0);
}

/* ------------------------------------------------------------------------
 * Submission
 * --------------------------------------------------------------------- */

static char *copy_path(const void *addr, int32_t *error)
{
    size_t length;
    char *copy;

    if (addr == NULL) {
        *error = -EFAULT;
        return NULL;
    }
    length = strnlen(addr, VSR_SIM_PATH_BYTES);
    if (length == VSR_SIM_PATH_BYTES) {
        *error = -ENAMETOOLONG;
        return NULL;
    }
    copy = vsr_sim_alloc(length + 1);
    memcpy(copy, addr, length);
    copy[length] = '\0';
    return copy;
}

static void copy_vecs(struct vsr_sim_op *op)
{
    const struct vsr_io_sqe *sqe = &op->sqe;
    uint64_t total = 0;

    if (sqe->length > VSR_SIM_MAX_VECS) {
        op->error = -EINVAL;
        return;
    }
    if (sqe->addr == NULL && sqe->length > 0) {
        op->error = -EFAULT;
        return;
    }
    op->vec_count = sqe->length;
    op->vecs = vsr_sim_alloc(sizeof(*op->vecs) * ((size_t)sqe->length + 1));
    if (sqe->length > 0) {
        memcpy(op->vecs, sqe->addr, sizeof(*op->vecs) * sqe->length);
    }
    for (uint32_t i = 0; i < op->vec_count; ++i) {
        total += op->vecs[i].length;
    }
    op->requested = total;
}

/* What the kernel does at prep: copy what the record points at, other than
 * data buffers, and checksum the data it will read. */
static void prepare(struct vsr_sim_node *node, struct vsr_sim_op *op)
{
    const struct vsr_io_sqe *sqe = &op->sqe;

    switch (sqe->opcode) {
    case VSR_IO_SQE_READ:
    case VSR_IO_SQE_WRITE:
        op->requested = sqe->length;
        break;
    case VSR_IO_SQE_READV:
    case VSR_IO_SQE_WRITEV:
        copy_vecs(op);
        break;
    case VSR_IO_SQE_SEND:
        op->zero_copy = (sqe->op_flags & VSR_IO_SEND_ZERO_COPY) != 0;
        if ((sqe->op_flags & VSR_IO_SEND_VECTORED) != 0) {
            copy_vecs(op);
        } else {
            op->requested = sqe->length;
        }
        break;
    case VSR_IO_SQE_RECV:
        op->multishot = (sqe->op_flags & VSR_IO_RECV_MULTISHOT) != 0;
        if ((sqe->flags & VSR_IO_SQE_BUFFER_SELECT) == 0 && !op->multishot) {
            op->requested = sqe->length;
        }
        break;
    case VSR_IO_SQE_ACCEPT:
        op->multishot = (sqe->op_flags & VSR_IO_ACCEPT_MULTISHOT) != 0;
        break;
    case VSR_IO_SQE_OPENAT:
    case VSR_IO_SQE_UNLINKAT:
    case VSR_IO_SQE_MKDIRAT:
    case VSR_IO_SQE_STATX:
        op->path = copy_path(sqe->addr, &op->error);
        break;
    case VSR_IO_SQE_RENAMEAT:
        op->path = copy_path(sqe->addr, &op->error);
        if (op->error == 0) {
            op->path2 = copy_path(sqe->addr2, &op->error);
        }
        break;
    case VSR_IO_SQE_CONNECT:
    case VSR_IO_SQE_BIND:
        if (sqe->addr == NULL) {
            op->error = -EFAULT;
        } else if (sqe->length > sizeof(struct sockaddr_storage)) {
            op->error = -EINVAL;
        } else {
            op->raw = vsr_sim_alloc(sizeof(struct sockaddr_storage));
            memcpy(op->raw, sqe->addr, sqe->length);
            op->raw_length = sqe->length;
        }
        break;
    case VSR_IO_SQE_SETSOCKOPT:
        op->option_length = sqe->length;
        if (sqe->addr == NULL && sqe->length > 0) {
            op->error = -EFAULT;
        } else if (sqe->length > 0) {
            memcpy(op->option, sqe->addr,
                   sqe->length < sizeof(op->option) ? sqe->length
                                                    : sizeof(op->option));
        }
        break;
    case VSR_IO_SQE_TIMEOUT_UPDATE:
        if (sqe->addr2 == NULL) {
            op->error = -EFAULT;
        } else {
            memcpy(&op->target, sqe->addr2, sizeof(op->target));
        }
        break;
    default:
        break;
    }
    if ((sqe->flags & VSR_IO_SQE_FIXED_BUFFER) != 0) {
        op->region = sqe->buffer_index;
    }
    if (op->error == 0) {
        bool readable = true;

        /* A FIXED_BUFFER record outside its region fails at start; its
         * bytes may not be addressable, so they are not read here. */
        if ((sqe->flags & VSR_IO_SQE_FIXED_BUFFER) != 0) {
            if (op->vecs != NULL) {
                for (uint32_t i = 0; i < op->vec_count; ++i) {
                    if (vsr_sim_exec_region(node, sqe, op->vecs[i].base,
                                            op->vecs[i].length) != 0) {
                        readable = false;
                    }
                }
            } else if (vsr_sim_exec_region(node, sqe, sqe->addr, sqe->length) !=
                       0) {
                readable = false;
            }
        }
        if (readable && op_source_checksum(op, &op->checksum)) {
            op->checked = 1;
        }
    }
}

static bool known_flags(const struct vsr_io_sqe *sqe)
{
    const uint32_t flags = VSR_IO_SQE_LINK | VSR_IO_SQE_FIXED_FILE |
                           VSR_IO_SQE_FIXED_BUFFER | VSR_IO_SQE_BUFFER_SELECT |
                           VSR_IO_SQE_SKIP_SUCCESS | VSR_IO_SQE_DIRECT;

    return (sqe->flags & ~flags) == 0;
}

static void start(struct vsr_sim_node *node, uint32_t index)
{
    struct vsr_sim_op *op = node->ops[index];
    const struct vsr_io_sqe *sqe = &op->sqe;

    op->started_ns = node->world->now_ns;
    if (op->zero_copy) {
        op->due_ns = node->world->now_ns;
    }
    if (op->error != 0) {
        vsr_sim_exec_complete(node, index, op->error, 0, 0);
        return;
    }
    if (!known_flags(sqe) ||
        ((sqe->flags & VSR_IO_SQE_SKIP_SUCCESS) != 0 &&
         (op->multishot || op->zero_copy)) ||
        ((sqe->flags & VSR_IO_SQE_LINK) != 0 && op->multishot)) {
        vsr_sim_exec_complete(node, index, -EINVAL, 0, 0);
        return;
    }
    switch ((enum vsr_io_sqe_opcode)sqe->opcode) {
    case VSR_IO_SQE_NOP:
        vsr_sim_exec_complete(node, index, 0, 0, 0);
        return;
    case VSR_IO_SQE_READ:
    case VSR_IO_SQE_WRITE:
    case VSR_IO_SQE_READV:
    case VSR_IO_SQE_WRITEV:
    case VSR_IO_SQE_FSYNC:
    case VSR_IO_SQE_FALLOCATE:
    case VSR_IO_SQE_OPENAT:
    case VSR_IO_SQE_RENAMEAT:
    case VSR_IO_SQE_UNLINKAT:
    case VSR_IO_SQE_MKDIRAT:
    case VSR_IO_SQE_STATX:
        vsr_sim_disk_start(node, index);
        return;
    case VSR_IO_SQE_CLOSE:
        start_close(node, index);
        return;
    case VSR_IO_SQE_SOCKET:
    case VSR_IO_SQE_CONNECT:
    case VSR_IO_SQE_BIND:
    case VSR_IO_SQE_LISTEN:
    case VSR_IO_SQE_ACCEPT:
    case VSR_IO_SQE_RECV:
    case VSR_IO_SQE_SEND:
    case VSR_IO_SQE_SHUTDOWN:
    case VSR_IO_SQE_SETSOCKOPT:
    case VSR_IO_SQE_GETSOCKOPT:
        vsr_sim_net_start(node, index);
        return;
    case VSR_IO_SQE_TIMEOUT:
        start_timeout(node, index);
        return;
    case VSR_IO_SQE_TIMEOUT_UPDATE:
        start_timeout_update(node, index);
        return;
    case VSR_IO_SQE_CANCEL:
        start_cancel(node, index);
        return;
    default:
        vsr_sim_exec_complete(node, index, -EINVAL, 0, 0);
        return;
    }
}

void vsr_sim_exec_event(struct vsr_sim_node *node, uint32_t index)
{
    struct vsr_sim_op *op = node->ops[index];

    switch (op->action) {
    case VSR_SIM_ACTION_DISK:
        op->action = VSR_SIM_ACTION_NONE;
        vsr_sim_disk_finish(node, index);
        break;
    case VSR_SIM_ACTION_TIMEOUT:
        vsr_sim_exec_complete(node, index, -ETIME, 0, 0);
        break;
    case VSR_SIM_ACTION_CONNECT:
    case VSR_SIM_ACTION_UNREACHABLE:
        vsr_sim_net_action(node, index);
        break;
    case VSR_SIM_ACTION_NOTIF:
        verify_op(node, op);
        post(node, op->sqe.user_data, 0, VSR_IO_CQE_NOTIF, 0);
        op_free(node, index);
        break;
    default:
        break;
    }
}

static int submit_and_wait(void *ctx, const struct vsr_io_sqe *sqes,
                           uint32_t count, uint32_t want, uint64_t min_wait_ns,
                           uint64_t deadline_ns)
{
    struct vsr_sim_node *node = handle_node(ctx);
    struct vsr_sim *sim = node->world;
    uint32_t *indices;
    uint32_t previous = VSR_SIM_NONE;

    if (count > 0 && sqes == NULL) {
        return -EFAULT;
    }
    indices = vsr_sim_alloc(sizeof(*indices) * ((size_t)count + 1));
    for (uint32_t i = 0; i < count; ++i) {
        uint32_t index = op_alloc(node);
        struct vsr_sim_op *op = node->ops[index];

        op->sqe = sqes[i];
        op->sequence = ++sim->sequence;
        op->state = VSR_SIM_OP_PENDING;
        if (previous != VSR_SIM_NONE &&
            (node->ops[previous]->sqe.flags & VSR_IO_SQE_LINK) != 0) {
            node->ops[previous]->link_next = index;
            op->state = VSR_SIM_OP_LINKED;
        }
        if (sim->trace.event != NULL) {
            struct vsr_sim_trace_event event;

            memset(&event, 0, sizeof(event));
            event.kind = VSR_SIM_TRACE_SUBMIT;
            event.node = node->index;
            event.peer = VSR_SIM_NO_NODE;
            event.opcode = op->sqe.opcode;
            event.user_data = op->sqe.user_data;
            event.now_ns = sim->now_ns;
            vsr_sim_emit_event(sim, &event);
        }
        prepare(node, op);
        indices[i] = index;
        previous = index;
    }
    for (uint32_t i = 0; i < count; ++i) {
        /* Chain members start when their predecessor completes; a head
         * is still PENDING here unless an earlier record ended it. */
        if (node->ops[indices[i]]->state == VSR_SIM_OP_PENDING) {
            start(node, indices[i]);
        }
    }
    free(indices);
    node->waited = 1;
    node->want = want;
    node->min_wait_ns = min_wait_ns;
    node->deadline_ns = deadline_ns;
    node->wait_started_ns = vsr_sim_node_now(node);
    return 0;
}

static uint32_t reap(void *ctx, struct vsr_io_cqe *cqes, uint32_t capacity)
{
    struct vsr_sim_node *node = handle_node(ctx);
    uint32_t count = 0;

    node->wake_pending = 0;
    if (cqes == NULL) {
        return 0;
    }
    while (count < capacity && node->cqes_count > 0) {
        cqes[count++] = node->cqes[node->cqes_head];
        node->cqes_head = (node->cqes_head + 1) % node->cqes_capacity;
        --node->cqes_count;
    }
    return count;
}

static uint64_t now(void *ctx)
{
    return vsr_sim_node_now(handle_node(ctx));
}

static void random_bytes(void *ctx, void *bytes, size_t size)
{
    struct vsr_sim_node *node = handle_node(ctx);
    unsigned char *at = bytes;

    for (size_t i = 0; i < size; i += 4) {
        uint32_t value = vsr_sim_pcg_next(&node->world->random);
        size_t take = size - i < 4 ? size - i : 4;

        for (size_t j = 0; j < take; ++j) {
            at[i + j] = (unsigned char)(value >> (8 * j));
        }
    }
}

static void wake(void *ctx)
{
    handle_node(ctx)->wake_pending = 1;
}

bool vsr_sim_exec_ready(const struct vsr_sim_node *node)
{
    uint64_t clock = vsr_sim_node_now(node);

    if (!node->waited || node->wake_pending || node->cqes_count >= node->want) {
        return true;
    }
    if (node->deadline_ns != VSR_NO_DEADLINE && clock >= node->deadline_ns) {
        return true;
    }
    return node->min_wait_ns > 0 && node->cqes_count > 0 &&
           clock >= vsr_sim_add(node->wait_started_ns, node->min_wait_ns);
}

uint64_t vsr_sim_exec_wake_at(const struct vsr_sim_node *node)
{
    uint64_t at = VSR_SIM_NEVER;
    uint64_t candidate;

    if (node->deadline_ns != VSR_NO_DEADLINE) {
        candidate = node->deadline_ns > node->clock_offset_ns
                        ? node->deadline_ns - node->clock_offset_ns
                        : 0;
        at = candidate < at ? candidate : at;
    }
    if (node->min_wait_ns > 0 && node->cqes_count > 0) {
        uint64_t end = vsr_sim_add(node->wait_started_ns, node->min_wait_ns);

        candidate =
            end > node->clock_offset_ns ? end - node->clock_offset_ns : 0;
        at = candidate < at ? candidate : at;
    }
    if (at != VSR_SIM_NEVER && at <= node->world->now_ns) {
        at = node->world->now_ns;
    }
    return at;
}

/* ------------------------------------------------------------------------
 * Registration
 * --------------------------------------------------------------------- */

static int register_files(void *ctx, uint32_t slots)
{
    struct vsr_sim_node *node = handle_node(ctx);
    uint32_t limit = node->world->options.file_slots;

    if (node->slots_count != 0) {
        return -EBUSY;
    }
    if (slots == 0 || slots > VSR_SIM_MAX_DIRECT ||
        (limit != 0 && slots > limit)) {
        return -EINVAL;
    }
    node->slots = vsr_sim_alloc(sizeof(*node->slots) * slots);
    for (uint32_t i = 0; i < slots; ++i) {
        node->slots[i] = -1;
    }
    node->slots_count = slots;
    return 0;
}

static int update_file(void *ctx, uint32_t slot, int fd)
{
    struct vsr_sim_node *node = handle_node(ctx);
    uint32_t object;
    int error;

    /* No table yet, or a slot past it: -EINVAL (decision 58). */
    if (slot >= node->slots_count) {
        return -EINVAL;
    }
    if (fd < 0) {
        if (node->slots[slot] >= 0) {
            uint32_t old = (uint32_t)node->slots[slot];

            node->slots[slot] = -1;
            vsr_sim_object_release(node, old);
        }
        return 0;
    }
    error = vsr_sim_exec_resolve(node, fd, false, &object);
    if (error != 0) {
        return error;
    }
    /* The executor takes the descriptor over: the reference moves. */
    node->fds[fd] = -1;
    if (node->slots[slot] >= 0) {
        uint32_t old = (uint32_t)node->slots[slot];

        node->slots[slot] = -1;
        vsr_sim_object_release(node, old);
    }
    node->slots[slot] = (int32_t)object;
    return 0;
}

static int register_buffers(void *ctx, uint32_t regions)
{
    struct vsr_sim_node *node = handle_node(ctx);
    uint32_t limit = node->world->options.buffer_regions;

    if (node->regions_count != 0) {
        return -EBUSY;
    }
    if (regions == 0 || regions > VSR_SIM_MAX_DIRECT ||
        (limit != 0 && regions > limit)) {
        return -EINVAL;
    }
    node->regions = vsr_sim_alloc(sizeof(*node->regions) * regions);
    node->regions_count = regions;
    return 0;
}

static int update_buffer(void *ctx, uint32_t index,
                         const struct vsr_io_region *region)
{
    struct vsr_sim_node *node = handle_node(ctx);

    /* No table yet, or an index past it: -EINVAL (decision 58). */
    if (index >= node->regions_count) {
        return -EINVAL;
    }
    for (uint32_t i = 0; i < node->ops_count; ++i) {
        if (node->ops[i]->state != VSR_SIM_OP_FREE &&
            node->ops[i]->region == index) {
            return -EBUSY;
        }
    }
    if (region == NULL) {
        memset(&node->regions[index], 0, sizeof(node->regions[index]));
        return 0;
    }
    if (region->base == NULL || region->size == 0) {
        return -EINVAL;
    }
    node->regions[index] = *region;
    return 0;
}

struct vsr_sim_ring *vsr_sim_exec_ring(struct vsr_sim_node *node,
                                       uint32_t group)
{
    for (uint32_t i = 0; i < node->rings_count; ++i) {
        if (node->rings[i].used && node->rings[i].group == group) {
            return &node->rings[i];
        }
    }
    return NULL;
}

static void free_ring(struct vsr_sim_ring *ring)
{
    free(ring->slots);
    memset(ring, 0, sizeof(*ring));
}

static void verify_ring(const struct vsr_sim_node *node,
                        const struct vsr_sim_ring *ring);

static int buffer_ring(void *ctx, uint16_t group, uint32_t entries,
                       uint32_t flags, const struct vsr_io_region *memory)
{
    struct vsr_sim_node *node = handle_node(ctx);
    struct vsr_sim_ring *ring = vsr_sim_exec_ring(node, group);
    uint32_t index = 0;

    if (entries == 0) {
        if (ring == NULL) {
            return -ENOENT;
        }
        /* Receives pending on the group stay parked and terminate with
         * -ENOBUFS at their next delivery (decision 58). The buffers go
         * back to the caller now, so this is the last chance to see that
         * nobody touched them. */
        verify_ring(node, ring);
        free_ring(ring);
        return 0;
    }
    if ((entries & (entries - 1)) != 0 || entries > VSR_SIM_MAX_RING ||
        (flags & ~(uint32_t)VSR_IO_BUFFER_RING_INCREMENTAL) != 0) {
        return -EINVAL;
    }
    if (memory == NULL || memory->base == NULL ||
        memory->size < (size_t)entries * 16u ||
        ((uintptr_t)memory->base & 4095u) != 0) {
        return -EINVAL;
    }
    if (ring != NULL) {
        return -EEXIST;
    }
    while (index < node->rings_count && node->rings[index].used) {
        ++index;
    }
    if (index == node->rings_count) {
        node->rings =
            vsr_sim_grow(node->rings, (size_t)index + 1, sizeof(*node->rings));
        ++node->rings_count;
    }
    ring = &node->rings[index];
    memset(ring, 0, sizeof(*ring));
    ring->used = 1;
    ring->group = group;
    ring->entries = entries;
    ring->flags = flags;
    ring->slots = vsr_sim_alloc(sizeof(*ring->slots) * entries);
    return 0;
}

static uint64_t remaining_checksum(const struct vsr_sim_provided *provided)
{
    const unsigned char *base = provided->buffer.base;

    return vsr_sim_checksum(base + provided->consumed,
                            provided->buffer.length - provided->consumed, 7);
}

static const struct vsr_sim_provided *ring_slot(const struct vsr_sim_ring *ring,
                                                uint32_t position)
{
    return &ring->slots[(ring->head + position) % ring->entries];
}

/* The bytes of a provided buffer the kernel still owns: the whole buffer,
 * or what an INCREMENTAL ring has not yet delivered from it. */
static bool provided_overlaps(const struct vsr_sim_provided *held,
                              const void *base, size_t length)
{
    const unsigned char *held_base = held->buffer.base;

    return overlaps(base, length, held_base + held->consumed,
                    held->buffer.length - held->consumed);
}

static void verify_provided(const struct vsr_sim_node *node,
                            const struct vsr_sim_provided *provided)
{
    if (remaining_checksum(provided) != provided->checksum) {
        vsr_sim_fatal("a provided buffer changed while the kernel owned it "
                      "(ownership violation)",
                      node->index, provided->buffer.id);
    }
}

static void verify_ring(const struct vsr_sim_node *node,
                        const struct vsr_sim_ring *ring)
{
    for (uint32_t i = 0; i < ring->count; ++i) {
        verify_provided(node, ring_slot(ring, i));
    }
}

static int provide(void *ctx, uint16_t group,
                   const struct vsr_io_buffer *buffers, uint32_t count)
{
    struct vsr_sim_node *node = handle_node(ctx);
    struct vsr_sim_ring *ring = vsr_sim_exec_ring(node, group);

    if (ring == NULL) {
        return -ENOENT;
    }
    if (count > 0 && buffers == NULL) {
        return -EFAULT;
    }
    if (count > ring->entries - ring->count) {
        return -ENOSPC;
    }
    for (uint32_t i = 0; i < count; ++i) {
        if (buffers[i].base == NULL || buffers[i].length == 0) {
            return -EINVAL;
        }
    }
    /* A buffer handed to the kernel must not be one it already owns
     * through any ring, nor one a submitted record still reads. */
    for (uint32_t i = 0; i < count; ++i) {
        const void *base = buffers[i].base;
        size_t length = buffers[i].length;

        for (uint32_t r = 0; r < node->rings_count; ++r) {
            const struct vsr_sim_ring *other = &node->rings[r];

            if (!other->used) {
                continue;
            }
            for (uint32_t j = 0; j < other->count; ++j) {
                if (provided_overlaps(ring_slot(other, j), base, length)) {
                    vsr_sim_fatal("provided buffer overlaps one the kernel "
                                  "still owns (ownership violation)",
                                  node->index, buffers[i].id);
                }
            }
        }
        for (uint32_t j = 0; j < node->ops_count; ++j) {
            if (op_source_overlaps(node->ops[j], base, length)) {
                vsr_sim_fatal("provided buffer overlaps the source of a "
                              "record in flight (ownership violation)",
                              node->index, node->ops[j]->sqe.user_data);
            }
        }
        for (uint32_t j = 0; j < i; ++j) {
            if (overlaps(base, length, buffers[j].base, buffers[j].length)) {
                vsr_sim_fatal("provided buffers overlap each other",
                              node->index, buffers[i].id);
            }
        }
    }
    for (uint32_t i = 0; i < count; ++i) {
        struct vsr_sim_provided *slot =
            &ring->slots[(ring->head + ring->count) % ring->entries];

        memset(slot, 0, sizeof(*slot));
        slot->buffer = buffers[i];
        slot->checksum = remaining_checksum(slot);
        ++ring->count;
    }
    return 0;
}

void vsr_sim_ring_verify(const struct vsr_sim_node *node,
                         const struct vsr_sim_ring *ring)
{
    verify_provided(node, &ring->slots[ring->head]);
}

uint16_t vsr_sim_ring_consume(struct vsr_sim_node *node,
                              struct vsr_sim_ring *ring, uint32_t used)
{
    struct vsr_sim_provided *head = &ring->slots[ring->head];

    (void)node;
    if ((ring->flags & VSR_IO_BUFFER_RING_INCREMENTAL) != 0) {
        head->consumed += used;
        if (head->consumed < head->buffer.length) {
            head->checksum = remaining_checksum(head);
            return VSR_IO_CQE_BUFFER_MORE;
        }
    }
    ring->head = (ring->head + 1) % ring->entries;
    --ring->count;
    return 0;
}

/* ------------------------------------------------------------------------
 * Crash and teardown
 * --------------------------------------------------------------------- */

static void drop_state(struct vsr_sim_node *node)
{
    for (uint32_t i = 0; i < node->ops_count; ++i) {
        op_release_memory(node->ops[i]);
        free(node->ops[i]);
    }
    free(node->ops);
    node->ops = NULL;
    node->ops_count = 0;
    node->inflight = 0;
    free(node->cqes);
    node->cqes = NULL;
    node->cqes_head = 0;
    node->cqes_count = 0;
    node->cqes_capacity = 0;
    for (uint32_t i = 0; i < node->objects_count; ++i) {
        free(node->objects[i]);
    }
    free(node->objects);
    node->objects = NULL;
    node->objects_count = 0;
    free(node->fds);
    node->fds = NULL;
    node->fds_count = 0;
    free(node->slots);
    node->slots = NULL;
    node->slots_count = 0;
    free(node->regions);
    node->regions = NULL;
    node->regions_count = 0;
    for (uint32_t i = 0; i < node->rings_count; ++i) {
        free_ring(&node->rings[i]);
    }
    free(node->rings);
    node->rings = NULL;
    node->rings_count = 0;
}

void vsr_sim_exec_crash(struct vsr_sim_node *node)
{
    /* Up to this instant the kernel could still read every in-flight
     * source and write every provided buffer; a change before the crash is
     * a violation whether or not the crash then hides it. */
    for (uint32_t i = 0; i < node->ops_count; ++i) {
        if (node->ops[i]->state != VSR_SIM_OP_FREE) {
            verify_op(node, node->ops[i]);
        }
    }
    for (uint32_t i = 0; i < node->rings_count; ++i) {
        if (node->rings[i].used) {
            verify_ring(node, &node->rings[i]);
        }
    }
    drop_state(node);
    node->waited = 0;
    node->wake_pending = 0;
}

void vsr_sim_exec_free(struct vsr_sim_node *node)
{
    drop_state(node);
}

const struct vsr_io_executor_ops vsr_sim_executor_ops = {
    .now = now,
    .random = random_bytes,
    .submit_and_wait = submit_and_wait,
    .reap = reap,
    .register_files = register_files,
    .update_file = update_file,
    .register_buffers = register_buffers,
    .update_buffer = update_buffer,
    .buffer_ring = buffer_ring,
    .provide = provide,
    .wake = wake,
};
