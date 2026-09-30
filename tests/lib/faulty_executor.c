#include "config.h"

#include "lib/faulty_executor.h"

#include <errno.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define PPM UINT32_C(1000000)
#define STREAM UINT64_C(0xFA17)
#define PULL 64u

enum record_flags {
    RECORD_VALID = 1u << 0,
    RECORD_ZERO_COPY = 1u << 1,
    RECORD_CHAINED = 1u << 2 /* Has LINK or follows a LINK record. */
};

static bool chance(struct faulty_executor *faulty, uint32_t ppm)
{
    if (ppm == 0) {
        return false;
    }
    if (ppm >= PPM) {
        return true;
    }
    return test_random_bounded(&faulty->random, PPM) < ppm;
}

/* The largest power of two dividing a request's offset and length, as a
 * shift: a count in whole units of it keeps the request's alignment, so
 * the rest of a short O_DIRECT or whole-block transfer stays aligned. */
static uint8_t align_shift(uint64_t offset, uint32_t length)
{
    uint64_t bits = offset | length;
    uint8_t shift = 0;

    while (shift < 31u && (bits & 1u) == 0) {
        bits >>= 1;
        ++shift;
    }
    return shift;
}

/* Uniform in [1, value - 1]; value must be > 1. */
static uint32_t shorter(struct faulty_executor *faulty, uint32_t value)
{
    return 1u + test_random_bounded(&faulty->random, value - 1u);
}

static void remember(struct faulty_executor *faulty,
                     const struct vsr_io_sqe *sqe, bool chained)
{
    struct faulty_executor_record *record =
        &faulty->records[faulty->record_next];
    uint8_t flags = RECORD_VALID;

    if (sqe->opcode == VSR_IO_SQE_SEND &&
        (sqe->op_flags & VSR_IO_SEND_ZERO_COPY) != 0) {
        flags |= RECORD_ZERO_COPY;
    }
    if (chained) {
        flags |= RECORD_CHAINED;
    }
    memset(record, 0, sizeof(*record));
    record->user_data = sqe->user_data;
    record->opcode = sqe->opcode;
    record->flags = flags;
    record->align_shift = align_shift(sqe->offset, sqe->length);
    faulty->record_next = (faulty->record_next + 1u) % FAULTY_EXECUTOR_RECORDS;
}

/* The newest valid record with this user_data, or NULL. */
static struct faulty_executor_record *lookup(struct faulty_executor *faulty,
                                             uint64_t user_data)
{
    uint32_t at = faulty->record_next;

    for (uint32_t i = 0; i < FAULTY_EXECUTOR_RECORDS; ++i) {
        struct faulty_executor_record *record;

        at = (at + FAULTY_EXECUTOR_RECORDS - 1u) % FAULTY_EXECUTOR_RECORDS;
        record = &faulty->records[at];
        if ((record->flags & RECORD_VALID) != 0 &&
            record->user_data == user_data) {
            return record;
        }
    }
    return NULL;
}

/* Request-side shortening applies to stream records whose length is a
 * byte count and whose result the inner executor then reports honestly. */
static bool shortenable(const struct vsr_io_sqe *sqe)
{
    if (sqe->length <= 1u) {
        return false;
    }
    if (sqe->opcode == VSR_IO_SQE_SEND) {
        return (sqe->op_flags &
                (VSR_IO_SEND_ZERO_COPY | VSR_IO_SEND_VECTORED)) == 0;
    }
    if (sqe->opcode == VSR_IO_SQE_RECV) {
        return (sqe->flags & VSR_IO_SQE_BUFFER_SELECT) == 0 &&
               (sqe->op_flags & VSR_IO_RECV_MULTISHOT) == 0;
    }
    return false;
}

/* A FILES_UPDATE failed this way may have updated its slots all the same,
 * as a failed write may have written; a PROVIDE never completes when it
 * succeeds, so it is never faulted. */
static bool eio_eligible(uint8_t opcode)
{
    return opcode == VSR_IO_SQE_READ || opcode == VSR_IO_SQE_WRITE ||
           opcode == VSR_IO_SQE_READV || opcode == VSR_IO_SQE_WRITEV ||
           opcode == VSR_IO_SQE_FSYNC || opcode == VSR_IO_SQE_FILES_UPDATE;
}

/* Completions whose relative order the caller relies on: those of one
 * user_data; those naming one provided buffer, since where an incremental
 * buffer's bytes lie follows from the order of its completions across
 * every receive on the ring; and those of LINK chains, whose members
 * complete in chain order (decision 62). Ids of different groups, and
 * members of different chains, are ordered too, which is merely stricter:
 * chained completions are never delayed on their own. */
static bool ordered(const struct faulty_executor_held *earlier,
                    const struct faulty_executor_held *later)
{
    const struct vsr_io_cqe *a = &earlier->cqe;
    const struct vsr_io_cqe *b = &later->cqe;

    return a->user_data == b->user_data ||
           ((a->flags & b->flags & VSR_IO_CQE_BUFFER) != 0 &&
            a->buffer_id == b->buffer_id) ||
           (earlier->chained != 0 && later->chained != 0);
}

/* Whether the newest held entry is ordered after an earlier one. */
static bool is_behind(const struct faulty_executor *faulty)
{
    const struct faulty_executor_held *newest =
        &faulty->held[faulty->held_count - 1u];

    for (uint32_t i = 0; i + 1u < faulty->held_count; ++i) {
        if (ordered(&faulty->held[i], newest)) {
            return true;
        }
    }
    return false;
}

/* Decides one completion pulled from the inner executor, in a fixed order:
 * -EIO, then shortening, then delay. */
static void admit(struct faulty_executor *faulty, const struct vsr_io_cqe *cqe)
{
    const struct faulty_executor_options *options = &faulty->options;
    struct faulty_executor_held *held;
    struct faulty_executor_record *record;
    bool chained = false;
    bool behind;

    if (cqe->user_data == FAULTY_EXECUTOR_USER_DATA) {
        if (cqe->result == 0) {
            ++faulty->stats.cancelled;
        }
        return;
    }
    held = &faulty->held[faulty->held_count++];
    memset(held, 0, sizeof(*held));
    held->cqe = *cqe;
    record = lookup(faulty, cqe->user_data);
    if (record != NULL) {
        chained = (record->flags & RECORD_CHAINED) != 0;
    }
    held->chained = chained ? 1u : 0u;
    behind = is_behind(faulty);
    if (record != NULL && !chained) {
        uint16_t special = VSR_IO_CQE_BUFFER | VSR_IO_CQE_BUFFER_MORE |
                           VSR_IO_CQE_MORE | VSR_IO_CQE_NOTIF;
        uint32_t units;

        if (eio_eligible(record->opcode) && held->cqe.result >= 0 &&
            chance(faulty, options->eio_ppm)) {
            held->cqe.result = -EIO;
            ++faulty->stats.eio;
        }
        /* In whole units of the request's alignment: a file's rest is
         * issued again at its offset, which O_DIRECT (and the store's
         * whole-block writes) need aligned. */
        units = held->cqe.result > 0
                    ? (uint32_t)held->cqe.result >> record->align_shift
                    : 0;
        if ((record->opcode == VSR_IO_SQE_READ ||
             record->opcode == VSR_IO_SQE_WRITE) &&
            units > 1 && (held->cqe.flags & special) == 0 &&
            (record->flags & RECORD_ZERO_COPY) == 0 &&
            chance(faulty, options->short_ppm)) {
            held->cqe.result =
                (int32_t)(shorter(faulty, units) << record->align_shift);
            ++faulty->stats.shortened;
        }
    }
    /* A completion behind an earlier one it is ordered after waits for it
     * anyway and is not held on its own account. */
    if (!chained && !behind && chance(faulty, options->delay_ppm)) {
        uint32_t most =
            options->delay_reaps_max > 0 ? options->delay_reaps_max : 1u;

        held->reaps = 1u + test_random_bounded(&faulty->random, most);
        ++faulty->stats.delayed;
    }
    if (record != NULL && (cqe->flags & VSR_IO_CQE_MORE) == 0) {
        record->flags = 0; /* Final completion: forget the record. */
    }
}

/* The wait the inner executor must do for the caller's, given what the
 * wrapper holds. Completions due now count toward `want` and, being
 * available, end a batching window as the inner's own would. A delayed one
 * becomes due only through reaps, so while one is held the wait does not
 * block: the caller then sees an early return, as after a wake. */
static void held_wait(const struct faulty_executor *faulty, uint32_t *want,
                      uint64_t *min_wait_ns, uint64_t *deadline_ns)
{
    uint32_t due = faulty->held_count;

    if (due == 0) {
        return;
    }
    for (uint32_t i = 0; i < faulty->held_count; ++i) {
        if (faulty->held[i].reaps > 0) {
            due = 0;
            break;
        }
    }
    if (due == 0 || due >= *want) {
        *want = 0;
        *min_wait_ns = 0;
        *deadline_ns = 0;
        return;
    }
    *want -= due;
    if (*min_wait_ns > 0) {
        uint64_t now = faulty->inner.ops->now(faulty->inner.ctx);
        uint64_t end = *min_wait_ns > VSR_NO_DEADLINE - now
                           ? VSR_NO_DEADLINE
                           : now + *min_wait_ns;

        if (end < *deadline_ns) {
            *deadline_ns = end;
        }
    }
}

static int faulty_submit_and_wait(void *ctx, const struct vsr_io_sqe *sqes,
                                  uint32_t count, uint32_t want,
                                  uint64_t min_wait_ns, uint64_t deadline_ns)
{
    struct faulty_executor *faulty = ctx;
    const struct vsr_io_sqe *forward = sqes;
    bool copy = count <= FAULTY_EXECUTOR_BATCH;
    bool tail_linked = false;
    bool follows_link = false;
    uint32_t total = count;
    uint32_t shortened = 0;
    int rc;

    if (count > 0 && sqes == NULL) {
        /* Refused by the inner executor, as it would be bare. */
        return faulty->inner.ops->submit_and_wait(
            faulty->inner.ctx, sqes, count, want, min_wait_ns, deadline_ns);
    }
    for (uint32_t i = 0; i < count; ++i) {
        if (sqes[i].user_data == FAULTY_EXECUTOR_USER_DATA) {
            return -EINVAL;
        }
    }
    if (count > 0) {
        tail_linked = (sqes[count - 1u].flags & VSR_IO_SQE_LINK) != 0;
        if (copy) {
            memcpy(faulty->batch, sqes, sizeof(*sqes) * count);
            forward = faulty->batch;
        }
    }
    for (uint32_t i = 0; i < count; ++i) {
        const struct vsr_io_sqe *sqe = &sqes[i];
        bool chained = follows_link || (sqe->flags & VSR_IO_SQE_LINK) != 0;

        follows_link = (sqe->flags & VSR_IO_SQE_LINK) != 0;
        remember(faulty, sqe, chained);
        if (!copy || chained) {
            continue;
        }
        /* An appended record would join a chain left open at the tail. */
        if (sqe->opcode == VSR_IO_SQE_RECV && !tail_linked &&
            chance(faulty, faulty->options.cancel_recv_ppm) &&
            total < FAULTY_EXECUTOR_BATCH) {
            struct vsr_io_sqe *cancel = &faulty->batch[total++];

            memset(cancel, 0, sizeof(*cancel));
            cancel->opcode = VSR_IO_SQE_CANCEL;
            cancel->fd = -1;
            cancel->user_data = FAULTY_EXECUTOR_USER_DATA;
            cancel->offset = sqe->user_data;
        }
        if (shortenable(sqe) && chance(faulty, faulty->options.short_ppm)) {
            faulty->batch[i].length = shorter(faulty, sqe->length);
            ++shortened;
        }
    }
    held_wait(faulty, &want, &min_wait_ns, &deadline_ns);
    rc = faulty->inner.ops->submit_and_wait(faulty->inner.ctx, forward, total,
                                            want, min_wait_ns, deadline_ns);
    if (rc == 0) {
        faulty->stats.shortened += shortened;
    }
    return rc;
}

static uint32_t faulty_reap(void *ctx, struct vsr_io_cqe *cqes,
                            uint32_t capacity)
{
    struct faulty_executor *faulty = ctx;
    struct vsr_io_cqe pulled[PULL];
    uint32_t delivered = 0;
    uint32_t kept = 0;

    for (uint32_t i = 0; i < faulty->held_count; ++i) {
        if (faulty->held[i].reaps > 0) {
            --faulty->held[i].reaps;
        }
    }
    while (faulty->held_count < FAULTY_EXECUTOR_HELD) {
        uint32_t room = FAULTY_EXECUTOR_HELD - faulty->held_count;
        uint32_t ask = room < PULL ? room : PULL;
        uint32_t got = faulty->inner.ops->reap(faulty->inner.ctx, pulled, ask);

        for (uint32_t i = 0; i < got; ++i) {
            admit(faulty, &pulled[i]);
        }
        if (got < ask) {
            break;
        }
    }
    /* Deliver in arrival order what is due and not behind an undelivered
     * completion it is ordered after. */
    for (uint32_t i = 0; i < faulty->held_count; ++i) {
        struct faulty_executor_held entry = faulty->held[i];
        bool keep = entry.reaps > 0 || delivered == capacity;

        for (uint32_t j = 0; !keep && j < kept; ++j) {
            keep = ordered(&faulty->held[j], &entry);
        }
        if (keep) {
            faulty->held[kept++] = entry;
        } else {
            cqes[delivered++] = entry.cqe;
        }
    }
    faulty->held_count = kept;
    return delivered;
}

static uint64_t faulty_now(void *ctx)
{
    struct faulty_executor *faulty = ctx;

    return faulty->inner.ops->now(faulty->inner.ctx);
}

static void faulty_random(void *ctx, void *bytes, size_t size)
{
    struct faulty_executor *faulty = ctx;

    faulty->inner.ops->random(faulty->inner.ctx, bytes, size);
}

static int faulty_register_files(void *ctx, uint32_t slots)
{
    struct faulty_executor *faulty = ctx;

    return faulty->inner.ops->register_files(faulty->inner.ctx, slots);
}

static int faulty_update_file(void *ctx, uint32_t slot, int fd)
{
    struct faulty_executor *faulty = ctx;

    return faulty->inner.ops->update_file(faulty->inner.ctx, slot, fd);
}

static int faulty_register_buffers(void *ctx, uint32_t regions)
{
    struct faulty_executor *faulty = ctx;

    return faulty->inner.ops->register_buffers(faulty->inner.ctx, regions);
}

static int faulty_update_buffer(void *ctx, uint32_t index,
                                const struct vsr_io_region *region)
{
    struct faulty_executor *faulty = ctx;

    return faulty->inner.ops->update_buffer(faulty->inner.ctx, index, region);
}

static int faulty_buffer_ring(void *ctx, uint16_t group, uint32_t entries,
                              uint32_t flags,
                              const struct vsr_io_region *memory)
{
    struct faulty_executor *faulty = ctx;

    return faulty->inner.ops->buffer_ring(faulty->inner.ctx, group, entries,
                                          flags, memory);
}

static int faulty_provide(void *ctx, uint16_t group,
                          const struct vsr_io_buffer *buffers, uint32_t count)
{
    struct faulty_executor *faulty = ctx;

    return faulty->inner.ops->provide(faulty->inner.ctx, group, buffers, count);
}

static void faulty_wake(void *ctx)
{
    struct faulty_executor *faulty = ctx;

    faulty->inner.ops->wake(faulty->inner.ctx);
}

static const struct vsr_io_executor_ops faulty_ops = {
    .now = faulty_now,
    .random = faulty_random,
    .submit_and_wait = faulty_submit_and_wait,
    .reap = faulty_reap,
    .register_files = faulty_register_files,
    .update_file = faulty_update_file,
    .register_buffers = faulty_register_buffers,
    .update_buffer = faulty_update_buffer,
    .buffer_ring = faulty_buffer_ring,
    .provide = faulty_provide,
    .wake = faulty_wake,
};

void faulty_executor_init(struct faulty_executor *faulty,
                          const struct vsr_io_executor *inner,
                          const struct faulty_executor_options *options)
{
    memset(faulty, 0, sizeof(*faulty));
    faulty->inner = *inner;
    faulty->options = *options;
    test_random_seed(&faulty->random, options->seed, STREAM);
}

struct vsr_io_executor faulty_executor_handle(struct faulty_executor *faulty)
{
    struct vsr_io_executor executor;

    executor.ops = &faulty_ops;
    executor.ctx = faulty;
    return executor;
}

void faulty_executor_set_options(struct faulty_executor *faulty,
                                 const struct faulty_executor_options *options)
{
    uint64_t seed = faulty->options.seed;

    faulty->options = *options;
    faulty->options.seed = seed;
}

void faulty_executor_stats(const struct faulty_executor *faulty,
                           struct faulty_executor_stats *stats)
{
    *stats = faulty->stats;
}
