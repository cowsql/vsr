#include "config.h"

#include "checked.h"
#include "extension.h"
#include "lib/check.h"
#include "protocol.h"
#include "reads.h"

#include <stdlib.h>
#include <string.h>

/* Isolate read-round logic from transport/storage scheduling. Runtime tests
 * separately exercise real operation pins, reservations and graph lifetimes. */
struct emitted {
    struct vsr_operation operation;
    struct vsr_message message;
    struct vsr_nonce nonce;
    struct vsr_reply reply;
    struct vsr_read_fence fence;
};

struct fixture {
    struct vsr core;
    struct vsr_protocol protocol;
    struct vsr_extension extension;
    struct vsr_member members[5];
    struct emitted output[64];
    size_t count;
    uint32_t noops;
    void *memory;
};

bool vsr_protocol_emit(struct vsr *v, uint32_t type, uint64_t arg, uint64_t tag,
                       const void *data, const uint32_t *leases, uint32_t count,
                       uint64_t completion_bytes)
{
    struct fixture *f = (void *)v;
    CHECK(f->count < 64 && leases == NULL && count == 0 &&
          completion_bytes == 0);
    struct emitted *out = &f->output[f->count++];
    memset(out, 0, sizeof(*out));
    out->operation.tag = tag;
    out->operation.output = (struct vsr_op){type, 0, f->count, NULL, arg};
    if (type == VSR_OP_SEND) {
        out->message = *(const struct vsr_message *)data;
        out->nonce = *(const struct vsr_nonce *)out->message.body;
        out->message.body = &out->nonce;
        out->operation.output.data = &out->message;
    } else if (type == VSR_OP_REPLY) {
        out->reply = *(const struct vsr_reply *)data;
        out->operation.output.data = &out->reply;
    } else {
        CHECK(type == VSR_OP_READ_READY);
        out->fence = *(const struct vsr_read_fence *)data;
        out->operation.output.data = &out->fence;
    }
    return true;
}

bool vsr_protocol_ready(const struct vsr *v)
{
    const struct vsr_protocol *p = vsr_protocol_const(v);
    return v->status.state == VSR_STATE_NORMAL && !p->hard_dirty &&
           p->safe_sequence >= p->hard_sequence;
}

bool vsr_protocol_noop(struct vsr *v)
{
    struct fixture *f = (void *)v;
    ++f->noops;
    ++f->protocol.log_end;
    return true;
}

bool vsr_protocol_nonce(struct vsr *v, struct vsr_nonce *nonce)
{
    struct vsr_protocol *p = vsr_protocol(v);
    ++p->nonce_counter;
    *nonce = (struct vsr_nonce){v->options.incarnation, p->nonce_counter};
    return true;
}

void vsr_fail(struct vsr *v, uint32_t code,
              const struct vsr_operation *operation, int32_t status)
{
    (void)operation;
    (void)status;
    v->status.failure.code = code;
    v->status.state = VSR_STATE_FAILED;
}

uint64_t vsr_after(struct vsr *v, uint64_t delay)
{
    CHECK(delay < UINT64_MAX - v->now);
    return v->time_set ? v->now + delay : VSR_NO_DEADLINE;
}

static void initialize(struct fixture *f, uint32_t count, uint32_t faults,
                       uint32_t self, bool established)
{
    memset(f, 0, sizeof(*f));
    f->core.protocol = &f->protocol;
    f->protocol.extension = &f->extension;
    f->core.options.cluster = (struct vsr_id){1, 2};
    f->core.options.incarnation = (struct vsr_id){3, 4};
    f->core.options.replica = (uint64_t)self + 1;
    f->core.options.limits.members = 5;
    f->core.options.limits.pending_reads = 8;
    f->core.options.retry_ns = 10;
    f->core.time_set = true;
    f->core.status.state = VSR_STATE_NORMAL;
    f->core.status.role = VSR_MEMBER_FULL;
    f->core.status.primary = 1;
    f->core.status.committed = 5;
    f->core.status.applied = 5;
    f->protocol.self = self;
    f->protocol.log_begin = 1;
    f->protocol.log_end = 6;
    f->protocol.stable_end = 6;
    f->protocol.stable_commit = 5;
    f->protocol.current = (struct vsr_membership){0, f->members, count, faults};
    f->protocol.epoch =
        (struct vsr_epoch){&f->protocol.current, NULL, 0, VSR_EPOCH_STEADY, 0};
    for (uint32_t i = 0; i < count; ++i) {
        f->members[i] =
            (struct vsr_member){(uint64_t)i + 1, VSR_MEMBER_FULL, 0};
    }
    size_t size;
    size_t alignment;
    CHECK(vsr_reads_size(&f->core.options, &size, &alignment) == VSR_OK);
    f->memory = aligned_alloc(alignment, size);
    CHECK(f->memory != NULL);
    vsr_reads_init(&f->core, f->memory, size);
    vsr_reads_normal(&f->core);
    if (established) {
        f->protocol.log_end = 7;
        f->protocol.stable_end = 7;
        f->protocol.stable_commit = 6;
        f->core.status.committed = 6;
        f->core.status.applied = 6;
    }
}

static void destroy(struct fixture *f)
{
    vsr_reads_stop(&f->core);
    free(f->memory);
}

static void admit(struct fixture *f, uint64_t cookie, uint32_t consistency,
                  uint64_t minimum, uint64_t deadline)
{
    const struct vsr_read_barrier barrier = {minimum, deadline, consistency, 0};
    const struct vsr_event event = {VSR_EVENT_READ, 0, cookie, &barrier, 1};
    bool handled = false;
    CHECK(vsr_reads_event(&f->core, &event, VSR_INDEX_NONE, &handled) ==
          VSR_OK);
    CHECK(handled);
}

static void drain(struct fixture *f)
{
    for (uint32_t i = 0; i < 100; ++i) {
        if (!vsr_reads_poll(&f->core)) {
            return;
        }
    }
    CHECK(false);
}

static size_t effects(const struct fixture *f, uint32_t type)
{
    size_t count = 0;
    for (size_t i = 0; i < f->count; ++i) {
        count += f->output[i].operation.output.type == type ? 1u : 0u;
    }
    return count;
}

static struct emitted *last(struct fixture *f, uint32_t type)
{
    for (size_t i = f->count; i > 0; --i) {
        if (f->output[i - 1].operation.output.type == type) {
            return &f->output[i - 1];
        }
    }
    CHECK(false);
    return NULL;
}

static void ack(struct fixture *f, uint64_t peer, const struct vsr_nonce *nonce,
                uint64_t floor)
{
    const struct vsr_message message = {f->core.options.cluster,
                                        f->core.status.epoch,
                                        f->core.status.view,
                                        peer,
                                        VSR_MSG_READ_ACK,
                                        0,
                                        floor,
                                        nonce};
    const struct vsr_event event = {VSR_EVENT_MESSAGE, 0, 0, &message, 1};
    bool handled = false;
    CHECK(vsr_reads_event(&f->core, &event, VSR_INDEX_NONE, &handled) ==
          VSR_OK);
    CHECK(handled);
}

static void finish(struct fixture *f, struct emitted *out, int status)
{
    const struct vsr_event event = {VSR_EVENT_COMPLETE, status,
                                    out->operation.output.id, NULL, 0};
    vsr_reads_complete(&f->core, &out->operation, &event, VSR_INDEX_NONE);
}

static void quorum_and_fence(void)
{
    struct fixture f;
    initialize(&f, 5, 2, 0, true);
    admit(&f, 10, VSR_READ_LINEARIZABLE, 7, 100);
    drain(&f);
    CHECK(effects(&f, VSR_OP_SEND) == 4 && f.noops == 0);
    struct vsr_nonce nonce = last(&f, VSR_OP_SEND)->nonce;
    struct vsr_nonce wrong = nonce;
    ++wrong.counter;
    ack(&f, 2, &nonce, 6);
    ack(&f, 2, &nonce, 6);
    ack(&f, 99, &nonce, 6);
    ack(&f, 3, &nonce, 5);
    ack(&f, 3, &wrong, 6);
    drain(&f);
    CHECK(effects(&f, VSR_OP_READ_READY) == 0);
    ack(&f, 3, &nonce, 6);
    drain(&f);
    CHECK(effects(&f, VSR_OP_READ_READY) == 0); /* min_op still unmet */
    f.core.status.applied = 7;
    drain(&f);
    CHECK(effects(&f, VSR_OP_READ_READY) == 1);
    struct emitted *fence = last(&f, VSR_OP_READ_READY);
    CHECK(fence->fence.cookie == 10 && fence->fence.applied == 7);
    CHECK(f.protocol.application_busy);
    f.core.status.state = VSR_STATE_VIEW_CHANGE;
    f.core.status.view = 1;
    f.core.now = 101;
    drain(&f);
    CHECK(f.protocol.application_busy && effects(&f, VSR_OP_REPLY) == 0);
    finish(&f, fence, VSR_IO_CANCELLED);
    CHECK(!f.protocol.application_busy);
    destroy(&f);
}

static void fresh_cohorts(void)
{
    struct fixture f;
    initialize(&f, 3, 1, 0, true);
    admit(&f, 1, VSR_READ_LINEARIZABLE, 0, VSR_NO_DEADLINE);
    CHECK(vsr_reads_poll(&f.core)); /* start cohort */
    CHECK(vsr_reads_poll(&f.core)); /* first probe */
    struct vsr_nonce first = last(&f, VSR_OP_SEND)->nonce;
    admit(&f, 2, VSR_READ_LINEARIZABLE, 0, VSR_NO_DEADLINE);
    ack(&f, 2, &first, 6);
    drain(&f);
    CHECK(effects(&f, VSR_OP_READ_READY) == 1);
    struct emitted *ready = last(&f, VSR_OP_READ_READY);
    CHECK(ready->fence.cookie == 1);
    struct vsr_nonce second = last(&f, VSR_OP_SEND)->nonce;
    CHECK(!vsr_nonce_equal(first, second));
    finish(&f, ready, VSR_IO_OK);
    ack(&f, 2, &first, 6);
    drain(&f);
    CHECK(effects(&f, VSR_OP_READ_READY) == 1);
    ack(&f, 2, &second, 6);
    drain(&f);
    CHECK(effects(&f, VSR_OP_READ_READY) == 2);
    CHECK(last(&f, VSR_OP_READ_READY)->fence.cookie == 2);
    finish(&f, last(&f, VSR_OP_READ_READY), VSR_IO_OK);
    destroy(&f);
}

static void noops_causal_and_deadlines(void)
{
    struct fixture f;
    initialize(&f, 1, 0, 0, false);
    admit(&f, 1, VSR_READ_LINEARIZABLE, 0, VSR_NO_DEADLINE);
    drain(&f);
    CHECK(f.noops == 1 && f.count == 0);
    drain(&f);
    CHECK(f.noops == 1);
    f.protocol.stable_commit = 6;
    f.core.status.committed = 6;
    f.core.status.applied = 6;
    drain(&f);
    CHECK(effects(&f, VSR_OP_READ_READY) == 1);
    CHECK(effects(&f, VSR_OP_SEND) == 0);
    destroy(&f);

    initialize(&f, 3, 1, 1, true);
    admit(&f, 1, VSR_READ_CAUSAL, 8, VSR_NO_DEADLINE);
    drain(&f);
    CHECK(f.count == 0);
    f.core.status.applied = 8;
    drain(&f);
    CHECK(effects(&f, VSR_OP_READ_READY) == 1 && f.noops == 0);
    finish(&f, last(&f, VSR_OP_READ_READY), VSR_IO_OK);
    admit(&f, 2, VSR_READ_LINEARIZABLE, 0, VSR_NO_DEADLINE);
    drain(&f);
    CHECK(last(&f, VSR_OP_REPLY)->reply.status == VSR_REPLY_NOT_PRIMARY);
    f.core.time_set = false;
    admit(&f, 3, VSR_READ_CAUSAL, 9, 0);
    drain(&f);
    CHECK(effects(&f, VSR_OP_REPLY) == 1);
    f.core.time_set = true;
    drain(&f);
    CHECK(effects(&f, VSR_OP_REPLY) == 2);
    CHECK(last(&f, VSR_OP_REPLY)->reply.status == VSR_REPLY_TIMEOUT);
    destroy(&f);
}

static void retry_and_view_change(void)
{
    struct fixture f;
    initialize(&f, 3, 1, 0, true);
    admit(&f, 1, VSR_READ_LINEARIZABLE, 0, VSR_NO_DEADLINE);
    drain(&f);
    CHECK(effects(&f, VSR_OP_SEND) == 2);
    finish(&f, last(&f, VSR_OP_SEND), VSR_IO_FAILED);
    drain(&f);
    CHECK(effects(&f, VSR_OP_SEND) == 2);
    CHECK(vsr_reads_deadline(&f.core) == 10);
    f.core.now = 10;
    drain(&f);
    CHECK(effects(&f, VSR_OP_SEND) == 4);
    f.core.status.view = 1;
    f.core.status.state = VSR_STATE_VIEW_CHANGE;
    drain(&f);
    CHECK(effects(&f, VSR_OP_REPLY) == 1);
    CHECK(effects(&f, VSR_OP_READ_READY) == 0);
    CHECK(vsr_reads_deadline(&f.core) == VSR_NO_DEADLINE);
    destroy(&f);
}

int main(void)
{
    quorum_and_fence();
    fresh_cohorts();
    noops_causal_and_deadlines();
    retry_and_view_change();
    return 0;
}
