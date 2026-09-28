/* liburing's header needs the GNU declarations (sigset_t, AT_FDCWD, statx,
 * idtype_t); the feature macro must precede every system header. */
#define _GNU_SOURCE
#include "config.h"

#include "io/uring.h"

#include "checked.h"

#include <errno.h>
#include <fcntl.h>
#include <liburing.h>
#include <limits.h>
#include <poll.h>
#include <stdalign.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/random.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <time.h>
#include <unistd.h>

/* Records hand vectors to the kernel as they are. */
_Static_assert(sizeof(struct vsr_io_vec) == sizeof(struct iovec),
               "vsr_io_vec must match struct iovec");
_Static_assert(offsetof(struct vsr_io_vec, base) ==
                   offsetof(struct iovec, iov_base),
               "vsr_io_vec.base must match iov_base");
_Static_assert(offsetof(struct vsr_io_vec, length) ==
                   offsetof(struct iovec, iov_len),
               "vsr_io_vec.length must match iov_len");
_Static_assert(sizeof(struct vsr_io_uring_timespec) ==
                   sizeof(struct __kernel_timespec),
               "timespec table must match struct __kernel_timespec");
_Static_assert(offsetof(struct vsr_io_uring_timespec, tv_nsec) ==
                   offsetof(struct __kernel_timespec, tv_nsec),
               "timespec table must match struct __kernel_timespec");
_Static_assert(sizeof(struct io_uring_sqe) == 64, "64-byte SQEs");

#define NS_PER_SEC UINT64_C(1000000000)
#define MAX_SQ_ENTRIES 32768u         /* IORING_MAX_ENTRIES */
#define MAX_CQ_ENTRIES 65536u         /* IORING_MAX_CQ_ENTRIES */
#define MAX_FILE_SLOTS (1u << 20)     /* IORING_MAX_FIXED_FILES */
#define MAX_BUFFER_REGIONS (1u << 14) /* IORING_MAX_REG_BUFFERS */
#define MAX_RING_ENTRIES 32768u       /* Provided-buffer ring entries. */
#define MAX_VECTORS 1024u             /* UIO_MAXIOV */

enum {
    RECORD_FLAGS = VSR_IO_SQE_LINK | VSR_IO_SQE_FIXED_FILE |
                   VSR_IO_SQE_FIXED_BUFFER | VSR_IO_SQE_BUFFER_SELECT |
                   VSR_IO_SQE_SKIP_SUCCESS | VSR_IO_SQE_DIRECT
};

/* Every feature the executor relies on without probing (design section 2);
 * init refuses a kernel lacking one. */
#define REQUIRED_FEATURES                                                      \
    (IORING_FEAT_NODROP | IORING_FEAT_EXT_ARG | IORING_FEAT_REG_REG_RING |     \
     IORING_FEAT_MIN_TIMEOUT)

static void *mutable(const void *pointer)
{
    return (void *)(uintptr_t)pointer;
}

static uint64_t monotonic_ns(void)
{
    struct timespec ts;
    uint64_t ns;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    ns = (uint64_t)ts.tv_sec * NS_PER_SEC + (uint64_t)ts.tv_nsec;
    return ns < VSR_NO_DEADLINE ? ns : VSR_NO_DEADLINE - 1;
}

static void to_timespec(uint64_t ns, struct vsr_io_uring_timespec *ts)
{
    ts->tv_sec = (int64_t)(ns / NS_PER_SEC);
    ts->tv_nsec = (int64_t)(ns % NS_PER_SEC);
}

static uint32_t round_pow2(uint32_t value)
{
    uint32_t result = 1;

    while (result < value) {
        result <<= 1;
    }
    return result;
}

static bool is_pow2(uint32_t value)
{
    return value != 0 && (value & (value - 1)) == 0;
}

/* ------------------------------------------------------------------------
 * Layout
 * --------------------------------------------------------------------- */

struct uring_plan {
    size_t ring;
    size_t buffers;
    size_t groups;
    size_t directs;
    size_t timespecs;
    size_t memory;
    size_t memory_size;
    size_t total;
    size_t page;
    uint32_t sq_ring;
    unsigned setup;
};

static size_t page_size(void)
{
    long page = sysconf(_SC_PAGESIZE);

    if (page <= 0 || ((unsigned long)page & ((unsigned long)page - 1)) != 0) {
        return 4096;
    }
    return (size_t)page;
}

static bool align_up(size_t value, size_t alignment, size_t *out)
{
    size_t sum;

    if (!vsr_size_add(value, alignment - 1, &sum)) {
        return false;
    }
    *out = sum & ~(alignment - 1);
    return true;
}

/* Reserves count * size bytes aligned to `alignment` at *offset. */
static bool reserve(size_t *offset, size_t count, size_t size, size_t alignment,
                    size_t *at)
{
    size_t bytes;

    if (!align_up(*offset, alignment, at) ||
        !vsr_size_mul(count, size, &bytes) ||
        !vsr_size_add(*at, bytes, offset)) {
        return false;
    }
    return true;
}

static unsigned setup_flags(const struct vsr_io_uring_options *options)
{
    /* SQPOLL excludes DEFER_TASKRUN and COOP_TASKRUN (-EINVAL at setup):
     * the SQ thread runs the task work. */
    unsigned flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_SUBMIT_ALL |
                     IORING_SETUP_NO_MMAP | IORING_SETUP_NO_SQARRAY |
                     IORING_SETUP_CQSIZE;

    if (options->sqpoll_idle_ms > 0) {
        flags |= IORING_SETUP_SQPOLL;
        if (options->sqpoll_cpu != UINT32_MAX) {
            flags |= IORING_SETUP_SQ_AFF;
        }
    } else {
        flags |= IORING_SETUP_DEFER_TASKRUN | IORING_SETUP_COOP_TASKRUN;
    }
    return flags;
}

static int plan(const struct vsr_io_uring_options *options,
                struct uring_plan *plan)
{
    struct io_uring_params params;
    ssize_t memory;
    size_t offset;

    if (options == NULL || options->reserved != 0 || options->sq_entries == 0 ||
        options->cq_entries < 2 * (uint64_t)options->sq_entries) {
        return VSR_EINVAL;
    }
    if (options->sq_entries > MAX_SQ_ENTRIES ||
        options->cq_entries > MAX_CQ_ENTRIES ||
        options->file_slots > MAX_FILE_SLOTS ||
        options->buffer_regions > MAX_BUFFER_REGIONS) {
        return VSR_ELIMIT;
    }
    memset(plan, 0, sizeof(*plan));
    plan->page = page_size();
    plan->setup = setup_flags(options);
    plan->sq_ring = round_pow2(options->sq_entries);

    memset(&params, 0, sizeof(params));
    params.flags = plan->setup;
    params.cq_entries = options->cq_entries;
    memory = io_uring_memory_size_params(options->sq_entries, &params);
    if (memory <= 0) {
        return VSR_EINVAL;
    }

    offset = sizeof(struct vsr_io_uring);
    if (!reserve(&offset, 1, sizeof(struct io_uring), alignof(struct io_uring),
                 &plan->ring) ||
        !reserve(&offset, options->buffer_regions,
                 sizeof(struct vsr_io_uring_region),
                 alignof(struct vsr_io_uring_region), &plan->buffers) ||
        !reserve(&offset, VSR_IO_URING_GROUPS,
                 sizeof(struct vsr_io_uring_group),
                 alignof(struct vsr_io_uring_group), &plan->groups) ||
        !reserve(&offset, options->cq_entries,
                 sizeof(struct vsr_io_uring_direct),
                 alignof(struct vsr_io_uring_direct), &plan->directs) ||
        !reserve(&offset, plan->sq_ring, sizeof(struct vsr_io_uring_timespec),
                 alignof(struct vsr_io_uring_timespec), &plan->timespecs) ||
        !reserve(&offset, 1, (size_t)memory, plan->page, &plan->memory)) {
        return VSR_ELIMIT;
    }
    plan->memory_size = (size_t)memory;
    plan->total = offset;
    return VSR_OK;
}

int vsr_io_uring_layout(const struct vsr_io_uring_options *options,
                        struct vsr_io_need *need)
{
    struct uring_plan layout;
    int rc;

    if (need == NULL) {
        return VSR_EINVAL;
    }
    rc = plan(options, &layout);
    if (rc != VSR_OK) {
        return rc;
    }
    need->size = layout.total;
    need->alignment = layout.page;
    return VSR_OK;
}

/* ------------------------------------------------------------------------
 * Record -> SQE
 * --------------------------------------------------------------------- */

static bool region_contains(const struct vsr_io_uring *uring, uint16_t index,
                            const void *addr, size_t length)
{
    const struct vsr_io_uring_region *region;
    uintptr_t at = (uintptr_t)addr;
    uintptr_t base;

    if (index >= uring->buffers_registered || uring->buffers == NULL) {
        return false;
    }
    region = &uring->buffers[index];
    if (region->base == NULL) {
        return false;
    }
    base = (uintptr_t)region->base;
    if (at < base || at - base > region->size) {
        return false;
    }
    return length <= region->size - (size_t)(at - base);
}

static int check_region(const struct vsr_io_uring *uring,
                        const struct vsr_io_sqe *record)
{
    return region_contains(uring, record->buffer_index, record->addr,
                           record->length)
               ? 0
               : -EFAULT;
}

static int check_vectors(const struct vsr_io_uring *uring,
                         const struct vsr_io_sqe *record)
{
    const struct vsr_io_vec *vecs = record->addr;

    if (record->length > MAX_VECTORS) {
        return -EINVAL;
    }
    if (record->length > 0 && vecs == NULL) {
        return -EFAULT;
    }
    for (uint32_t i = 0; i < record->length; ++i) {
        if (!region_contains(uring, record->buffer_index, vecs[i].base,
                             vecs[i].length)) {
            return -EFAULT;
        }
    }
    return 0;
}

static struct vsr_io_uring_timespec *
timespec_for(struct vsr_io_uring *uring, const struct io_uring_sqe *sqe)
{
    if (uring->timespecs == NULL || uring->timespecs_count == 0) {
        return NULL;
    }
    if (uring->ring != NULL && uring->ring->sq.sqes != NULL) {
        uintptr_t base = (uintptr_t)uring->ring->sq.sqes;
        uintptr_t at = (uintptr_t)sqe;

        if (at >= base &&
            at - base < (uintptr_t)uring->timespecs_count * sizeof(*sqe)) {
            return &uring->timespecs[(at - base) / sizeof(*sqe)];
        }
    }
    return &uring->timespecs[0];
}

static int remember_direct(struct vsr_io_uring *uring, uint64_t user_data,
                           int32_t slot)
{
    if (uring->directs == NULL ||
        uring->directs_count >= uring->directs_capacity) {
        return -EAGAIN;
    }
    for (uint32_t i = 0; i < uring->directs_capacity; ++i) {
        if (uring->directs[i].slot < 0) {
            uring->directs[i].user_data = user_data;
            uring->directs[i].slot = slot;
            uring->directs_count++;
            return 0;
        }
    }
    return -EAGAIN;
}

/* The slot argument of a *_direct prep, or -EINVAL. */
static int direct_slot(const struct vsr_io_sqe *record, unsigned *slot)
{
    if (record->fd2 == VSR_IO_SLOT_ALLOC) {
        *slot = IORING_FILE_INDEX_ALLOC;
        return 0;
    }
    if (record->fd2 < 0) {
        return -EINVAL;
    }
    *slot = (unsigned)record->fd2;
    return 0;
}

static bool uses_fixed_buffer(uint8_t opcode)
{
    return opcode == VSR_IO_SQE_READ || opcode == VSR_IO_SQE_WRITE ||
           opcode == VSR_IO_SQE_READV || opcode == VSR_IO_SQE_WRITEV ||
           opcode == VSR_IO_SQE_RECV || opcode == VSR_IO_SQE_SEND;
}

static bool uses_direct(uint8_t opcode)
{
    return opcode == VSR_IO_SQE_OPENAT || opcode == VSR_IO_SQE_SOCKET ||
           opcode == VSR_IO_SQE_ACCEPT;
}

static int translate_recv(const struct vsr_io_uring *uring,
                          const struct vsr_io_sqe *record,
                          struct io_uring_sqe *sqe, unsigned *sqe_flags)
{
    uint32_t known = VSR_IO_RECV_MULTISHOT | VSR_IO_RECV_PEEK;
    bool multishot = (record->op_flags & VSR_IO_RECV_MULTISHOT) != 0;
    bool select = (record->flags & VSR_IO_SQE_BUFFER_SELECT) != 0;
    int msg_flags = 0;
    int rc;

    if ((record->op_flags & ~known) != 0) {
        return -EINVAL;
    }
    if (record->op_flags & VSR_IO_RECV_PEEK) {
        msg_flags |= MSG_PEEK;
    }
    if (multishot) {
        /* The kernel's multishot receive always selects buffers. */
        if (!select || (record->flags & VSR_IO_SQE_SKIP_SUCCESS)) {
            return -EINVAL;
        }
        io_uring_prep_recv_multishot(sqe, record->fd, NULL, record->length,
                                     msg_flags);
    } else if (select) {
        io_uring_prep_recv(sqe, record->fd, NULL, record->length, msg_flags);
    } else {
        if (record->flags & VSR_IO_SQE_FIXED_BUFFER) {
            rc = check_region(uring, record);
            if (rc != 0) {
                return rc;
            }
        }
        if (record->flags & VSR_IO_SQE_LINK) {
            msg_flags |= MSG_WAITALL;
        }
        io_uring_prep_recv(sqe, record->fd, mutable(record->addr),
                           record->length, msg_flags);
    }
    if (select) {
        *sqe_flags |= IOSQE_BUFFER_SELECT;
        sqe->buf_group = record->buffer_group;
    }
    return 0;
}

static int translate_send(const struct vsr_io_uring *uring,
                          const struct vsr_io_sqe *record,
                          struct io_uring_sqe *sqe)
{
    uint32_t known = VSR_IO_SEND_ZERO_COPY | VSR_IO_SEND_VECTORED;
    bool zero_copy = (record->op_flags & VSR_IO_SEND_ZERO_COPY) != 0;
    bool vectored = (record->op_flags & VSR_IO_SEND_VECTORED) != 0;
    bool fixed = (record->flags & VSR_IO_SQE_FIXED_BUFFER) != 0;
    int msg_flags = MSG_NOSIGNAL;
    unsigned zc_flags = 0;
    int rc;

    if ((record->op_flags & ~known) != 0) {
        return -EINVAL;
    }
    if (vectored && record->length > MAX_VECTORS) {
        return -EINVAL;
    }
    if (record->flags & VSR_IO_SQE_LINK) {
        msg_flags |= MSG_WAITALL;
    }
    if (zero_copy) {
        if (record->flags & VSR_IO_SQE_SKIP_SUCCESS) {
            return -EINVAL;
        }
        /* The range check is the kernel's here: its -EFAULT comes with the
         * NOTIF completion the contract promises after a failed send. */
        if (fixed) {
            zc_flags |= IORING_RECVSEND_FIXED_BUF;
        }
        if (vectored) {
            zc_flags |= IORING_SEND_VECTORIZED;
        }
        io_uring_prep_send_zc(sqe, record->fd, record->addr, record->length,
                              msg_flags, zc_flags);
        if (fixed) {
            sqe->buf_index = record->buffer_index;
        }
        return 0;
    }
    if (fixed) {
        rc = vectored ? check_vectors(uring, record)
                      : check_region(uring, record);
        if (rc != 0) {
            return rc;
        }
    }
    io_uring_prep_send(sqe, record->fd, record->addr, record->length,
                       msg_flags);
    if (vectored) {
        sqe->ioprio |= IORING_SEND_VECTORIZED;
    }
    return 0;
}

static int translate_timeout(struct vsr_io_uring *uring,
                             const struct vsr_io_sqe *record,
                             struct io_uring_sqe *sqe)
{
    struct vsr_io_uring_timespec *ts = timespec_for(uring, sqe);
    unsigned flags = 0;

    if ((record->op_flags & ~(uint32_t)VSR_IO_TIMEOUT_ABSOLUTE) != 0 ||
        ts == NULL) {
        return -EINVAL;
    }
    if (record->op_flags & VSR_IO_TIMEOUT_ABSOLUTE) {
        flags |= IORING_TIMEOUT_ABS; /* CLOCK_MONOTONIC, the executor's. */
    }
    to_timespec(record->offset, ts);
    if (record->opcode == VSR_IO_SQE_TIMEOUT) {
        io_uring_prep_timeout(sqe, (const struct __kernel_timespec *)ts, 0,
                              flags);
    } else {
        const uint64_t *target = record->addr2;

        if (target == NULL) {
            return -EINVAL;
        }
        io_uring_prep_timeout_update(sqe, (const struct __kernel_timespec *)ts,
                                     *target, flags);
    }
    return 0;
}

static int translate_cancel(const struct vsr_io_sqe *record,
                            struct io_uring_sqe *sqe)
{
    uint32_t known = VSR_IO_CANCEL_BY_FD | VSR_IO_CANCEL_ALL;
    unsigned flags = 0;

    if ((record->op_flags & ~known) != 0) {
        return -EINVAL;
    }
    if (record->op_flags & VSR_IO_CANCEL_ALL) {
        flags |= IORING_ASYNC_CANCEL_ALL;
    }
    if (record->op_flags & VSR_IO_CANCEL_BY_FD) {
        flags |= IORING_ASYNC_CANCEL_FD;
        if (record->flags & VSR_IO_SQE_FIXED_FILE) {
            flags |= IORING_ASYNC_CANCEL_FD_FIXED;
        }
        io_uring_prep_cancel_fd(sqe, record->fd, flags);
    } else {
        io_uring_prep_cancel64(sqe, record->offset, (int)flags);
    }
    return 0;
}

int vsr_io_uring_translate(struct vsr_io_uring *uring,
                           const struct vsr_io_sqe *record, void *memory)
{
    struct io_uring_sqe *sqe = memory;
    uint8_t flags = record->flags;
    bool fixed = (flags & VSR_IO_SQE_FIXED_BUFFER) != 0;
    bool direct = (flags & VSR_IO_SQE_DIRECT) != 0;
    unsigned sqe_flags = 0;
    unsigned slot = 0;
    bool remember = false;
    int rc = 0;

    memset(sqe, 0, sizeof(*sqe));
    if ((flags & ~(unsigned)RECORD_FLAGS) != 0 ||
        ((flags & VSR_IO_SQE_BUFFER_SELECT) &&
         record->opcode != VSR_IO_SQE_RECV) ||
        (fixed && !uses_fixed_buffer(record->opcode)) ||
        (fixed && (flags & VSR_IO_SQE_BUFFER_SELECT)) ||
        (direct && !uses_direct(record->opcode))) {
        return -EINVAL;
    }
    if (direct) {
        rc = direct_slot(record, &slot);
        if (rc != 0) {
            return rc;
        }
        remember = slot != IORING_FILE_INDEX_ALLOC &&
                   !(flags & VSR_IO_SQE_SKIP_SUCCESS);
    }
    if (flags & VSR_IO_SQE_LINK) {
        sqe_flags |= IOSQE_IO_LINK;
    }
    if (flags & VSR_IO_SQE_SKIP_SUCCESS) {
        sqe_flags |= IOSQE_CQE_SKIP_SUCCESS;
    }
    if ((flags & VSR_IO_SQE_FIXED_FILE) && record->opcode != VSR_IO_SQE_CLOSE &&
        record->opcode != VSR_IO_SQE_CANCEL) {
        sqe_flags |= IOSQE_FIXED_FILE;
    }

    switch ((enum vsr_io_sqe_opcode)record->opcode) {
    case VSR_IO_SQE_NOP:
        io_uring_prep_nop(sqe);
        break;
    case VSR_IO_SQE_READ:
        if (fixed) {
            rc = check_region(uring, record);
            if (rc == 0) {
                io_uring_prep_read_fixed(sqe, record->fd, mutable(record->addr),
                                         record->length, record->offset,
                                         record->buffer_index);
            }
        } else {
            io_uring_prep_read(sqe, record->fd, mutable(record->addr),
                               record->length, record->offset);
        }
        break;
    case VSR_IO_SQE_WRITE:
        if (fixed) {
            rc = check_region(uring, record);
            if (rc == 0) {
                io_uring_prep_write_fixed(sqe, record->fd, record->addr,
                                          record->length, record->offset,
                                          record->buffer_index);
            }
        } else {
            io_uring_prep_write(sqe, record->fd, record->addr, record->length,
                                record->offset);
        }
        break;
    case VSR_IO_SQE_READV:
        if (record->length > MAX_VECTORS) {
            rc = -EINVAL;
        } else if (fixed) {
            rc = check_vectors(uring, record);
            if (rc == 0) {
                io_uring_prep_readv_fixed(sqe, record->fd, record->addr,
                                          record->length, record->offset, 0,
                                          record->buffer_index);
            }
        } else {
            io_uring_prep_readv(sqe, record->fd, record->addr, record->length,
                                record->offset);
        }
        break;
    case VSR_IO_SQE_WRITEV:
        if (record->length > MAX_VECTORS) {
            rc = -EINVAL;
        } else if (fixed) {
            rc = check_vectors(uring, record);
            if (rc == 0) {
                io_uring_prep_writev_fixed(sqe, record->fd, record->addr,
                                           record->length, record->offset, 0,
                                           record->buffer_index);
            }
        } else {
            io_uring_prep_writev(sqe, record->fd, record->addr, record->length,
                                 record->offset);
        }
        break;
    case VSR_IO_SQE_FSYNC:
        if ((record->op_flags & ~(uint32_t)VSR_IO_FSYNC_DATASYNC) != 0) {
            rc = -EINVAL;
        } else {
            io_uring_prep_fsync(sqe, record->fd,
                                (record->op_flags & VSR_IO_FSYNC_DATASYNC)
                                    ? IORING_FSYNC_DATASYNC
                                    : 0);
        }
        break;
    case VSR_IO_SQE_FALLOCATE:
        io_uring_prep_fallocate(sqe, record->fd, (int)record->op_flags,
                                record->offset, record->length);
        break;
    case VSR_IO_SQE_OPENAT:
        if (direct) {
            io_uring_prep_openat_direct(sqe, record->fd, record->addr,
                                        (int)record->op_flags,
                                        (mode_t)record->length, slot);
        } else {
            io_uring_prep_openat(sqe, record->fd, record->addr,
                                 (int)record->op_flags, (mode_t)record->length);
        }
        break;
    case VSR_IO_SQE_CLOSE:
        if (flags & VSR_IO_SQE_FIXED_FILE) {
            if (record->fd < 0) {
                rc = -EBADF;
            } else {
                io_uring_prep_close_direct(sqe, (unsigned)record->fd);
            }
        } else {
            io_uring_prep_close(sqe, record->fd);
        }
        break;
    case VSR_IO_SQE_RENAMEAT:
        io_uring_prep_renameat(sqe, record->fd, record->addr, record->fd,
                               record->addr2, record->op_flags);
        break;
    case VSR_IO_SQE_UNLINKAT:
        io_uring_prep_unlinkat(sqe, record->fd, record->addr,
                               (int)record->op_flags);
        break;
    case VSR_IO_SQE_MKDIRAT:
        io_uring_prep_mkdirat(sqe, record->fd, record->addr,
                              (mode_t)record->length);
        break;
    case VSR_IO_SQE_STATX:
        io_uring_prep_statx(sqe, record->fd, record->addr,
                            (int)record->op_flags, record->length,
                            mutable(record->addr2));
        break;
    case VSR_IO_SQE_SOCKET:
        if (record->offset > INT_MAX || record->length > INT_MAX) {
            rc = -EINVAL;
        } else if (direct && slot == IORING_FILE_INDEX_ALLOC) {
            io_uring_prep_socket_direct_alloc(sqe, (int)record->length,
                                              (int)record->op_flags,
                                              (int)record->offset, 0);
        } else if (direct) {
            io_uring_prep_socket_direct(sqe, (int)record->length,
                                        (int)record->op_flags,
                                        (int)record->offset, slot, 0);
        } else {
            io_uring_prep_socket(sqe, (int)record->length,
                                 (int)record->op_flags, (int)record->offset, 0);
        }
        break;
    case VSR_IO_SQE_CONNECT:
        io_uring_prep_connect(sqe, record->fd, record->addr, record->length);
        break;
    case VSR_IO_SQE_BIND:
        io_uring_prep_bind(sqe, record->fd, record->addr, record->length);
        break;
    case VSR_IO_SQE_LISTEN:
        if (record->length > INT_MAX) {
            rc = -EINVAL;
        } else {
            io_uring_prep_listen(sqe, record->fd, (int)record->length);
        }
        break;
    case VSR_IO_SQE_ACCEPT:
        if ((record->op_flags & ~(uint32_t)VSR_IO_ACCEPT_MULTISHOT) != 0) {
            rc = -EINVAL;
        } else if (record->op_flags & VSR_IO_ACCEPT_MULTISHOT) {
            /* The kernel's multishot accept allocates slots only. */
            if ((flags & VSR_IO_SQE_SKIP_SUCCESS) ||
                (direct && slot != IORING_FILE_INDEX_ALLOC)) {
                rc = -EINVAL;
            } else if (direct) {
                io_uring_prep_multishot_accept_direct(sqe, record->fd, NULL,
                                                      NULL, 0);
            } else {
                io_uring_prep_multishot_accept(sqe, record->fd, NULL, NULL,
                                               SOCK_CLOEXEC);
            }
        } else if (direct) {
            io_uring_prep_accept_direct(sqe, record->fd, NULL, NULL, 0, slot);
        } else {
            io_uring_prep_accept(sqe, record->fd, NULL, NULL, SOCK_CLOEXEC);
        }
        break;
    case VSR_IO_SQE_RECV:
        rc = translate_recv(uring, record, sqe, &sqe_flags);
        break;
    case VSR_IO_SQE_SEND:
        rc = translate_send(uring, record, sqe);
        break;
    case VSR_IO_SQE_SHUTDOWN:
        if (record->length > INT_MAX) {
            rc = -EINVAL;
        } else {
            io_uring_prep_shutdown(sqe, record->fd, (int)record->length);
        }
        break;
    case VSR_IO_SQE_SETSOCKOPT:
    case VSR_IO_SQE_GETSOCKOPT:
        if (record->length > INT_MAX) {
            rc = -EINVAL;
        } else {
            io_uring_prep_cmd_sock(sqe,
                                   record->opcode == VSR_IO_SQE_SETSOCKOPT
                                       ? SOCKET_URING_OP_SETSOCKOPT
                                       : SOCKET_URING_OP_GETSOCKOPT,
                                   record->fd, (int)(record->op_flags >> 16),
                                   (int)(record->op_flags & 0xffffu),
                                   mutable(record->addr), (int)record->length);
        }
        break;
    case VSR_IO_SQE_TIMEOUT:
    case VSR_IO_SQE_TIMEOUT_UPDATE:
        rc = translate_timeout(uring, record, sqe);
        break;
    case VSR_IO_SQE_CANCEL:
        rc = translate_cancel(record, sqe);
        break;
    default:
        rc = -EINVAL;
        break;
    }
    if (rc != 0) {
        memset(sqe, 0, sizeof(*sqe));
        return rc;
    }
    if (remember) {
        rc = remember_direct(uring, record->user_data, (int32_t)slot);
        if (rc != 0) {
            memset(sqe, 0, sizeof(*sqe));
            return rc;
        }
    }
    sqe->flags = (uint8_t)(sqe->flags | sqe_flags);
    sqe->user_data = record->user_data;
    return 0;
}

/* A record the translation rejected: a NOP completing with `error`, in the
 * record's place in its chain. */
static void inject_failure(struct io_uring_sqe *sqe,
                           const struct vsr_io_sqe *record, int error,
                           bool link)
{
    memset(sqe, 0, sizeof(*sqe));
    io_uring_prep_nop(sqe);
    sqe->nop_flags = IORING_NOP_INJECT_RESULT;
    sqe->len = (uint32_t)error;
    sqe->user_data = record->user_data;
    if (link) {
        sqe->flags = IOSQE_IO_LINK;
    }
}

/* ------------------------------------------------------------------------
 * CQE -> record
 * --------------------------------------------------------------------- */

void vsr_io_uring_reap_one(const void *memory, struct vsr_io_cqe *record)
{
    const struct io_uring_cqe *cqe = memory;
    uint16_t flags = 0;

    record->user_data = cqe->user_data;
    record->result = cqe->res;
    record->buffer_id = 0;
    if (cqe->flags & IORING_CQE_F_MORE) {
        flags |= VSR_IO_CQE_MORE;
    }
    if (cqe->flags & IORING_CQE_F_BUFFER) {
        flags |= VSR_IO_CQE_BUFFER;
        record->buffer_id = (uint16_t)(cqe->flags >> IORING_CQE_BUFFER_SHIFT);
    }
    if (cqe->flags & IORING_CQE_F_BUF_MORE) {
        flags |= VSR_IO_CQE_BUFFER_MORE;
    }
    if (cqe->flags & IORING_CQE_F_NOTIF) {
        flags |= VSR_IO_CQE_NOTIF;
    }
    record->flags = flags;
}

static void resolve_direct(struct vsr_io_uring *uring,
                           struct vsr_io_cqe *record)
{
    for (uint32_t i = 0; i < uring->directs_capacity; ++i) {
        struct vsr_io_uring_direct *direct = &uring->directs[i];

        if (direct->slot >= 0 && direct->user_data == record->user_data) {
            if (record->result == 0) {
                record->result = direct->slot;
            }
            direct->slot = -1;
            uring->directs_count--;
            return;
        }
    }
}

static void consume_wake(struct vsr_io_uring *uring,
                         const struct io_uring_cqe *cqe)
{
    uint64_t value;
    ssize_t n;

    /* Reset the counter; EAGAIN when another completion already did. */
    n = read(uring->wake_fd, &value, sizeof(value));
    (void)n;
    uring->wake_reads++;
    if (!(cqe->flags & IORING_CQE_F_MORE)) {
        uring->wake_armed = 0; /* Re-armed by the next submit_and_wait. */
    }
}

static uint32_t uring_reap(void *ctx, struct vsr_io_cqe *cqes,
                           uint32_t capacity)
{
    struct vsr_io_uring *uring = ctx;
    struct io_uring_cqe *cqe;
    unsigned head;
    unsigned seen = 0;
    uint32_t n = 0;

    io_uring_for_each_cqe(uring->ring, head, cqe)
    {
        if (cqe->user_data == VSR_IO_URING_WAKE_USER_DATA) {
            consume_wake(uring, cqe);
            seen++;
            continue;
        }
        if (n == capacity) {
            break;
        }
        vsr_io_uring_reap_one(cqe, &cqes[n]);
        if (uring->directs_count > 0) {
            resolve_direct(uring, &cqes[n]);
        }
        n++;
        seen++;
    }
    io_uring_cq_advance(uring->ring, seen);
    return n;
}

/* ------------------------------------------------------------------------
 * Submission and waiting
 * --------------------------------------------------------------------- */

static int fail(struct vsr_io_uring *uring, int error)
{
    if (uring->failure == 0) {
        uring->failure = error;
    }
    return uring->failure;
}

/* Makes room for `count` SQEs, submitting what the SQ holds. */
static int ensure_space(struct vsr_io_uring *uring, unsigned count)
{
    while (io_uring_sq_space_left(uring->ring) < count) {
        int rc = io_uring_submit(uring->ring);

        if (rc == -EINTR) {
            continue;
        }
        if (rc < 0) {
            return rc;
        }
        if (io_uring_sq_space_left(uring->ring) >= count) {
            break;
        }
        if (!(uring->ring->flags & IORING_SETUP_SQPOLL)) {
            return -EBUSY;
        }
        rc = io_uring_sqring_wait(uring->ring);
        if (rc < 0 && rc != -EINTR) {
            return rc;
        }
    }
    return 0;
}

static int arm_wake(struct vsr_io_uring *uring)
{
    struct io_uring_sqe *sqe;
    int rc = ensure_space(uring, 1);

    if (rc != 0) {
        return rc;
    }
    sqe = io_uring_get_sqe(uring->ring);
    io_uring_prep_poll_multishot(sqe, uring->wake_fd, POLLIN);
    sqe->user_data = VSR_IO_URING_WAKE_USER_DATA;
    uring->wake_armed = 1;
    return 0;
}

/* Records up to the first without LINK: one chain, placed in one submit. */
static uint32_t chain_length(const struct vsr_io_sqe *sqes, uint32_t count)
{
    uint32_t n = 1;

    while (n < count && (sqes[n - 1].flags & VSR_IO_SQE_LINK)) {
        n++;
    }
    return n;
}

static int submit_records(struct vsr_io_uring *uring,
                          const struct vsr_io_sqe *sqes, uint32_t count)
{
    unsigned entries = uring->ring->sq.ring_entries;
    uint32_t i = 0;

    while (i < count) {
        uint32_t chain = chain_length(sqes + i, count - i);
        /* A chain longer than the SQ cannot be submitted as one: every
         * record of it fails alone. */
        bool too_long = chain > entries;
        int rc;

        for (uint32_t j = 0; j < chain; ++j) {
            const struct vsr_io_sqe *record = &sqes[i + j];
            struct io_uring_sqe *sqe;
            bool last = i + j + 1 == count;

            if (j == 0 || too_long) {
                rc = ensure_space(uring, too_long ? 1 : chain);
                if (rc != 0) {
                    return rc;
                }
            }
            sqe = io_uring_get_sqe(uring->ring);
            if (too_long) {
                inject_failure(sqe, record, -EINVAL, false);
                continue;
            }
            rc = vsr_io_uring_translate(uring, record, sqe);
            if (rc != 0) {
                inject_failure(sqe, record, rc,
                               (record->flags & VSR_IO_SQE_LINK) != 0);
            }
            if (last) {
                /* A chain never spans batches. */
                sqe->flags = (uint8_t)(sqe->flags & ~IOSQE_IO_LINK);
            }
        }
        uring->submitted += chain;
        i += chain;
    }
    return 0;
}

/* True when a wake completion sits in the CQ; *scan skips what was seen. */
static bool wake_pending(const struct vsr_io_uring *uring, unsigned *scan)
{
    const struct io_uring *ring = uring->ring;
    unsigned head = *ring->cq.khead;
    unsigned ready = io_uring_cq_ready(ring);
    unsigned tail = head + ready;
    unsigned at = *scan;

    if (at - head > ready) {
        at = head;
    }
    for (; at != tail; ++at) {
        if (ring->cq.cqes[at & ring->cq.ring_mask].user_data ==
            VSR_IO_URING_WAKE_USER_DATA) {
            *scan = at;
            return true;
        }
    }
    *scan = tail;
    return false;
}

static int uring_submit_and_wait(void *ctx, const struct vsr_io_sqe *sqes,
                                 uint32_t count, uint32_t want,
                                 uint64_t min_wait_ns, uint64_t deadline_ns)
{
    struct vsr_io_uring *uring = ctx;
    struct io_uring *ring = uring->ring;
    uint64_t start = monotonic_ns();
    uint64_t window_end;
    unsigned scan = *ring->cq.khead;
    int rc;

    if (uring->failure != 0) {
        return uring->failure;
    }
    if ((count > 0 && sqes == NULL) || want > ring->cq.ring_entries) {
        return -EINVAL;
    }
    for (uint32_t i = 0; i < count; ++i) {
        if (sqes[i].user_data == VSR_IO_URING_WAKE_USER_DATA) {
            return -EINVAL;
        }
    }
    if (!uring->wake_armed) {
        rc = arm_wake(uring);
        if (rc != 0) {
            return fail(uring, rc);
        }
    }
    rc = submit_records(uring, sqes, count);
    if (rc != 0) {
        return fail(uring, rc);
    }
    /* Submit the rest and run deferred task work without waiting, so that
     * completions already due are in the CQ before deciding to wait. */
    do {
        rc = io_uring_submit_and_get_events(ring);
    } while (rc == -EINTR);
    if (rc < 0 && rc != -EAGAIN && rc != -EBUSY) {
        return fail(uring, rc);
    }

    window_end = min_wait_ns > VSR_NO_DEADLINE - start ? VSR_NO_DEADLINE
                                                       : start + min_wait_ns;
    for (;;) {
        struct __kernel_timespec ts;
        struct __kernel_timespec *timeout = NULL;
        struct io_uring_cqe *cqe;
        unsigned available;
        unsigned wait_nr;
        unsigned min_us = 0;
        uint64_t now;
        uint64_t limit;

        if (wake_pending(uring, &scan)) {
            return 0;
        }
        available = io_uring_cq_ready(ring);
        if (available >= want) {
            return 0;
        }
        now = monotonic_ns();
        if (deadline_ns != VSR_NO_DEADLINE && now >= deadline_ns) {
            return 0;
        }
        if (min_wait_ns > 0 && available >= 1 && now >= window_end) {
            return 0;
        }
        limit = deadline_ns;
        if (min_wait_ns > 0 && now < window_end) {
            /* Batching window: the kernel waits for `want`, returning at
             * the window's end once one completion exists. A wake inside
             * the window returns at its end at the latest. */
            wait_nr = want;
            if (deadline_ns > window_end && deadline_ns != VSR_NO_DEADLINE) {
                uint64_t us = (window_end - now + 999) / 1000;

                min_us = us > UINT_MAX ? UINT_MAX : (unsigned)us;
            } else if (deadline_ns == VSR_NO_DEADLINE) {
                /* Without a timeout the kernel's minimum would act as
                 * one; wait for the window explicitly instead. */
                limit = window_end;
            }
        } else {
            /* One more completion at a time, so that a wake (a single
             * completion) always ends the wait. */
            wait_nr = available + 1;
        }
        if (limit != VSR_NO_DEADLINE) {
            struct vsr_io_uring_timespec relative;

            to_timespec(limit - now, &relative);
            ts.tv_sec = relative.tv_sec;
            ts.tv_nsec = relative.tv_nsec;
            timeout = &ts;
        }
        rc = io_uring_submit_and_wait_min_timeout(ring, &cqe, wait_nr, timeout,
                                                  min_us, NULL);
        if (rc == -EAGAIN || rc == -EBUSY) {
            return 0; /* Completions must be reaped first. */
        }
        if (rc < 0 && rc != -ETIME && rc != -EINTR) {
            return fail(uring, rc);
        }
    }
}

/* ------------------------------------------------------------------------
 * Registration
 * --------------------------------------------------------------------- */

static int uring_register_files(void *ctx, uint32_t slots)
{
    struct vsr_io_uring *uring = ctx;
    int rc;

    if (uring->files_registered > 0) {
        return -EBUSY;
    }
    if (slots == 0 || slots > uring->options.file_slots) {
        return -EINVAL;
    }
    rc = io_uring_register_files_sparse(uring->ring, slots);
    if (rc < 0) {
        return rc;
    }
    uring->files_registered = slots;
    return 0;
}

static int uring_update_file(void *ctx, uint32_t slot, int fd)
{
    struct vsr_io_uring *uring = ctx;
    int file = fd < 0 ? -1 : fd;
    int rc;

    if (slot >= uring->files_registered) {
        return -EINVAL;
    }
    rc = io_uring_register_files_update(uring->ring, slot, &file, 1);
    if (rc < 0) {
        return rc;
    }
    if (rc != 1) {
        return -EINVAL;
    }
    /* The table holds its own reference: the executor owns the
     * descriptor from here, and closing the slot closes the file. */
    if (file >= 0) {
        (void)close(file);
    }
    return 0;
}

static int uring_register_buffers(void *ctx, uint32_t regions)
{
    struct vsr_io_uring *uring = ctx;
    int rc;

    if (uring->buffers_registered > 0) {
        return -EBUSY;
    }
    if (regions == 0 || regions > uring->options.buffer_regions) {
        return -EINVAL;
    }
    rc = io_uring_register_buffers_sparse(uring->ring, regions);
    if (rc < 0) {
        return rc;
    }
    memset(uring->buffers, 0, sizeof(*uring->buffers) * regions);
    uring->buffers_registered = regions;
    return 0;
}

static int uring_update_buffer(void *ctx, uint32_t index,
                               const struct vsr_io_region *region)
{
    struct vsr_io_uring *uring = ctx;
    struct iovec iov = {0};
    __u64 tag = 0;
    int rc;

    if (index >= uring->buffers_registered) {
        return -EINVAL;
    }
    if (region != NULL) {
        iov.iov_base = region->base;
        iov.iov_len = region->size;
    }
    rc =
        io_uring_register_buffers_update_tag(uring->ring, index, &iov, &tag, 1);
    if (rc < 0) {
        return rc;
    }
    uring->buffers[index].base = iov.iov_base;
    uring->buffers[index].size = iov.iov_len;
    return 0;
}

static struct vsr_io_uring_group *find_group(struct vsr_io_uring *uring,
                                             uint16_t group)
{
    for (uint32_t i = 0; i < VSR_IO_URING_GROUPS; ++i) {
        if (uring->groups[i].registered && uring->groups[i].group == group) {
            return &uring->groups[i];
        }
    }
    return NULL;
}

static int uring_buffer_ring(void *ctx, uint16_t group, uint32_t entries,
                             uint32_t flags, const struct vsr_io_region *memory)
{
    struct vsr_io_uring *uring = ctx;
    struct vsr_io_uring_group *entry = find_group(uring, group);
    struct io_uring_buf_reg reg;
    size_t page = page_size();
    int rc;

    if (entries == 0) {
        if (entry == NULL) {
            return -ENOENT;
        }
        rc = io_uring_unregister_buf_ring(uring->ring, group);
        if (rc < 0) {
            return rc;
        }
        memset(entry, 0, sizeof(*entry));
        uring->groups_count--;
        return 0;
    }
    if ((flags & ~(uint32_t)VSR_IO_BUFFER_RING_INCREMENTAL) != 0 ||
        !is_pow2(entries) || entries > MAX_RING_ENTRIES || memory == NULL ||
        memory->base == NULL || ((uintptr_t)memory->base & (page - 1)) != 0 ||
        memory->size < (size_t)entries * sizeof(struct io_uring_buf)) {
        return -EINVAL;
    }
    if (entry != NULL) {
        return -EEXIST;
    }
    for (uint32_t i = 0; i < VSR_IO_URING_GROUPS && entry == NULL; ++i) {
        if (!uring->groups[i].registered) {
            entry = &uring->groups[i];
        }
    }
    if (entry == NULL) {
        return -ENOSPC;
    }
    memset(memory->base, 0, (size_t)entries * sizeof(struct io_uring_buf));
    io_uring_buf_ring_init(memory->base);
    memset(&reg, 0, sizeof(reg));
    reg.ring_addr = (uint64_t)(uintptr_t)memory->base;
    reg.ring_entries = entries;
    reg.bgid = group;
    if (flags & VSR_IO_BUFFER_RING_INCREMENTAL) {
        reg.flags = IOU_PBUF_RING_INC;
    }
    rc = io_uring_register_buf_ring(uring->ring, &reg, 0);
    if (rc < 0) {
        return rc;
    }
    entry->ring_memory = memory->base;
    entry->entries = entries;
    entry->flags = flags;
    entry->tail = 0;
    entry->head = 0;
    entry->group = group;
    entry->registered = 1;
    uring->groups_count++;
    return 0;
}

static int uring_provide(void *ctx, uint16_t group,
                         const struct vsr_io_buffer *buffers, uint32_t count)
{
    struct vsr_io_uring *uring = ctx;
    struct vsr_io_uring_group *entry = find_group(uring, group);
    struct io_uring_buf_ring *ring;
    struct io_uring_buf *slots;
    uint32_t mask;
    uint32_t held;

    if (entry == NULL) {
        return -ENOENT;
    }
    if (count == 0) {
        return 0;
    }
    if (buffers == NULL) {
        return -EINVAL;
    }
    held = (uint16_t)(entry->tail - entry->head);
    if (held + count > entry->entries) {
        /* Ask the kernel how far it has consumed before refusing. */
        uint16_t head;
        int rc = io_uring_buf_ring_head(uring->ring, group, &head);

        if (rc < 0) {
            return rc;
        }
        entry->head = head;
        held = (uint16_t)(entry->tail - entry->head);
        if (held + count > entry->entries) {
            return -ENOSPC;
        }
    }
    /* Entries are written through a pointer rather than
     * io_uring_buf_ring_add, whose bufs[0] member the bounds sanitizer
     * (with -fstrict-flex-arrays=3) rejects; entry 0's resv field is the
     * ring tail and stays untouched. */
    ring = entry->ring_memory;
    slots = entry->ring_memory;
    mask = entry->entries - 1;
    for (uint32_t i = 0; i < count; ++i) {
        struct io_uring_buf *slot = slots + ((entry->tail + i) & mask);

        slot->addr = (uint64_t)(uintptr_t)buffers[i].base;
        slot->len = buffers[i].length;
        slot->bid = buffers[i].id;
    }
    io_uring_buf_ring_advance(ring, (int)count);
    entry->tail = (uint16_t)(entry->tail + count);
    return 0;
}

/* ------------------------------------------------------------------------
 * Clock, entropy, wake
 * --------------------------------------------------------------------- */

static uint64_t uring_now(void *ctx)
{
    (void)ctx;
    return monotonic_ns();
}

static void uring_random(void *ctx, void *bytes, size_t size)
{
    unsigned char *at = bytes;

    (void)ctx;
    while (size > 0) {
        ssize_t n = getrandom(at, size, 0);

        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            abort(); /* No entropy source: nothing sound to return. */
        }
        at += n;
        size -= (size_t)n;
    }
}

static void uring_wake(void *ctx)
{
    const struct vsr_io_uring *uring = ctx;
    uint64_t one = 1;
    ssize_t n;

    do {
        n = write(uring->wake_fd, &one, sizeof(one));
    } while (n < 0 && errno == EINTR);
    /* EAGAIN: the counter is saturated, a wake is already pending. */
}

const struct vsr_io_executor_ops vsr_io_uring_ops = {
    .now = uring_now,
    .random = uring_random,
    .submit_and_wait = uring_submit_and_wait,
    .reap = uring_reap,
    .register_files = uring_register_files,
    .update_file = uring_update_file,
    .register_buffers = uring_register_buffers,
    .update_buffer = uring_update_buffer,
    .buffer_ring = uring_buffer_ring,
    .provide = uring_provide,
    .wake = uring_wake,
};

/* ------------------------------------------------------------------------
 * Lifecycle
 * --------------------------------------------------------------------- */

int vsr_io_uring_init(void *memory, size_t size,
                      const struct vsr_io_uring_options *options,
                      struct vsr_io_executor *out)
{
    struct uring_plan layout;
    struct vsr_io_uring *uring = memory;
    struct io_uring_params params;
    unsigned char *base = memory;
    int rc;

    if (memory == NULL || options == NULL || out == NULL ||
        plan(options, &layout) != VSR_OK ||
        ((uintptr_t)memory & (layout.page - 1)) != 0 || size < layout.total) {
        return -EINVAL;
    }
    memset(memory, 0, layout.total);
    uring->options = *options;
    uring->ring = (void *)(base + layout.ring);
    uring->ring_memory = base + layout.memory;
    uring->ring_memory_size = layout.memory_size;
    uring->wake_fd = -1;
    uring->buffers = (void *)(base + layout.buffers);
    uring->groups = (void *)(base + layout.groups);
    uring->directs = (void *)(base + layout.directs);
    uring->directs_capacity = options->cq_entries;
    for (uint32_t i = 0; i < uring->directs_capacity; ++i) {
        uring->directs[i].slot = -1;
    }
    uring->timespecs = (void *)(base + layout.timespecs);
    uring->timespecs_count = layout.sq_ring;

    memset(&params, 0, sizeof(params));
    params.flags = layout.setup;
    params.cq_entries = options->cq_entries;
    if (layout.setup & IORING_SETUP_SQPOLL) {
        params.sq_thread_idle = options->sqpoll_idle_ms;
        params.sq_thread_cpu = options->sqpoll_cpu;
    }
    rc = io_uring_queue_init_mem(options->sq_entries, uring->ring, &params,
                                 uring->ring_memory, uring->ring_memory_size);
    if (rc < 0) {
        return rc;
    }
    if ((params.features & REQUIRED_FEATURES) != REQUIRED_FEATURES) {
        rc = -EOPNOTSUPP;
        goto fail;
    }
    if (uring->ring->sq.ring_entries != layout.sq_ring) {
        rc = -EINVAL;
        goto fail;
    }
    rc = io_uring_register_ring_fd(uring->ring);
    if (rc < 0) {
        goto fail;
    }
    if (options->napi_busy_poll_us > 0) {
        struct io_uring_napi napi;

        memset(&napi, 0, sizeof(napi));
        napi.busy_poll_to = options->napi_busy_poll_us;
        rc = io_uring_register_napi(uring->ring, &napi);
        if (rc < 0) {
            goto fail;
        }
    }
    uring->wake_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (uring->wake_fd < 0) {
        rc = -errno;
        goto fail;
    }
    rc = arm_wake(uring);
    if (rc == 0) {
        rc = io_uring_submit(uring->ring);
    }
    if (rc < 0) {
        goto fail;
    }
    out->ops = &vsr_io_uring_ops;
    out->ctx = uring;
    return 0;

fail:
    io_uring_queue_exit(uring->ring);
    if (uring->wake_fd >= 0) {
        (void)close(uring->wake_fd);
        uring->wake_fd = -1;
    }
    return rc;
}

void vsr_io_uring_deinit(struct vsr_io_executor *executor)
{
    struct vsr_io_uring *uring;

    if (executor == NULL || executor->ctx == NULL) {
        return;
    }
    uring = executor->ctx;
    /* Closing the ring releases every registration and cancels every
     * pending request; the region stays the caller's. */
    io_uring_queue_exit(uring->ring);
    if (uring->wake_fd >= 0) {
        (void)close(uring->wake_fd);
        uring->wake_fd = -1;
    }
    executor->ops = NULL;
    executor->ctx = NULL;
}

int vsr_io_uring_fd(const struct vsr_io_executor *executor)
{
    const struct vsr_io_uring *uring;

    if (executor == NULL || executor->ctx == NULL) {
        return -EBADF;
    }
    uring = executor->ctx;
    return uring->ring->ring_fd;
}
