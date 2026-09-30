#include "config.h"

#include "lib/pure_executor.h"

#include "lib/check.h"

#include <stdio.h>
#include <stdlib.h>

static void note(struct pure_executor *pure, const char *operation)
{
    pure->calls++;
    if (pure->armed == 0) {
        return;
    }
    pure->violations++;
    pure->last = operation;
    if (pure->abort_on_violation) {
        fprintf(stderr,
                "purity: the executor's %s was called from an engine "
                "primitive\n",
                operation);
        abort();
    }
}

static uint64_t pure_now(void *ctx)
{
    struct pure_executor *pure = ctx;

    note(pure, "now");
    return pure->inner.ops->now(pure->inner.ctx);
}

static void pure_random(void *ctx, void *bytes, size_t size)
{
    struct pure_executor *pure = ctx;

    note(pure, "random");
    pure->inner.ops->random(pure->inner.ctx, bytes, size);
}

static int pure_submit(void *ctx, const struct vsr_io_sqe *sqes, uint32_t count,
                       uint32_t want, uint64_t min_wait_ns,
                       uint64_t deadline_ns)
{
    struct pure_executor *pure = ctx;

    note(pure, "submit_and_wait");
    return pure->inner.ops->submit_and_wait(pure->inner.ctx, sqes, count, want,
                                            min_wait_ns, deadline_ns);
}

static uint32_t pure_reap(void *ctx, struct vsr_io_cqe *cqes, uint32_t capacity)
{
    struct pure_executor *pure = ctx;

    note(pure, "reap");
    return pure->inner.ops->reap(pure->inner.ctx, cqes, capacity);
}

static int pure_register_files(void *ctx, uint32_t slots)
{
    struct pure_executor *pure = ctx;

    note(pure, "register_files");
    return pure->inner.ops->register_files(pure->inner.ctx, slots);
}

static int pure_update_file(void *ctx, uint32_t slot, int fd)
{
    struct pure_executor *pure = ctx;

    note(pure, "update_file");
    return pure->inner.ops->update_file(pure->inner.ctx, slot, fd);
}

static int pure_register_buffers(void *ctx, uint32_t regions)
{
    struct pure_executor *pure = ctx;

    note(pure, "register_buffers");
    return pure->inner.ops->register_buffers(pure->inner.ctx, regions);
}

static int pure_update_buffer(void *ctx, uint32_t index,
                              const struct vsr_io_region *region)
{
    struct pure_executor *pure = ctx;

    note(pure, "update_buffer");
    return pure->inner.ops->update_buffer(pure->inner.ctx, index, region);
}

static int pure_buffer_ring(void *ctx, uint16_t group, uint32_t entries,
                            uint32_t flags, const struct vsr_io_region *memory)
{
    struct pure_executor *pure = ctx;

    note(pure, "buffer_ring");
    return pure->inner.ops->buffer_ring(pure->inner.ctx, group, entries, flags,
                                        memory);
}

static int pure_provide(void *ctx, uint16_t group,
                        const struct vsr_io_buffer *buffers, uint32_t count)
{
    struct pure_executor *pure = ctx;

    note(pure, "provide");
    return pure->inner.ops->provide(pure->inner.ctx, group, buffers, count);
}

static void pure_wake(void *ctx)
{
    struct pure_executor *pure = ctx;

    note(pure, "wake");
    pure->inner.ops->wake(pure->inner.ctx);
}

static const struct vsr_io_executor_ops pure_ops = {pure_now,
                                                    pure_random,
                                                    pure_submit,
                                                    pure_reap,
                                                    pure_register_files,
                                                    pure_update_file,
                                                    pure_register_buffers,
                                                    pure_update_buffer,
                                                    pure_buffer_ring,
                                                    pure_provide,
                                                    pure_wake};

struct vsr_io_executor pure_executor_init(struct pure_executor *pure,
                                          struct vsr_io_executor inner,
                                          bool abort_on_violation)
{
    struct vsr_io_executor out;

    CHECK(pure != NULL && inner.ops != NULL);
    pure->inner = inner;
    pure->armed = 0;
    pure->abort_on_violation = abort_on_violation;
    pure->calls = 0;
    pure->violations = 0;
    pure->last = NULL;
    out.ops = &pure_ops;
    out.ctx = pure;
    return out;
}

void pure_executor_arm(struct pure_executor *pure)
{
    pure->armed++;
}

void pure_executor_disarm(struct pure_executor *pure)
{
    CHECK(pure->armed > 0);
    pure->armed--;
}
