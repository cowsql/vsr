/* The GNU declarations (statx, AT_FDCWD, syscall) precede every system
 * header. */
#define _GNU_SOURCE
#include "config.h"

#include "io/uring.h"

#include "checked.h"

#include <endian.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <stdalign.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/random.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/syscall.h>
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
_Static_assert(sizeof(struct io_uring_sqe) == 64, "64-byte SQEs");
_Static_assert(sizeof(struct io_uring_cqe) == 16, "16-byte CQEs");
_Static_assert(sizeof(struct io_uring_buf) == 16, "16-byte buffer entries");
_Static_assert(sizeof(_Atomic uint32_t) == sizeof(uint32_t) &&
                   sizeof(_Atomic uint16_t) == sizeof(uint16_t),
               "ring words are addressed as atomics in place");

#define NS_PER_SEC UINT64_C(1000000000)
#define MAX_SQ_ENTRIES 32768u         /* IORING_MAX_ENTRIES */
#define MAX_CQ_ENTRIES 65536u         /* IORING_MAX_CQ_ENTRIES */
#define MAX_FILE_SLOTS (1u << 20)     /* IORING_MAX_FIXED_FILES */
#define MAX_BUFFER_REGIONS (1u << 14) /* IORING_MAX_REG_BUFFERS */
#define MAX_RING_ENTRIES 32768u       /* Provided-buffer ring entries. */
#define MAX_VECTORS 1024u             /* UIO_MAXIOV */
/* deinit's drain: each cancel round waits this long for requests already
 * completing, and the whole drain gives up after the limit. */
#define DRAIN_STEP_NS UINT64_C(10000000)    /* 10 ms */
#define DRAIN_LIMIT_NS UINT64_C(1000000000) /* 1 s */
/* A register call of this kind takes the argument's size as nr_args. */
#define REGISTER_SIZED(type) ((unsigned)sizeof(type))

enum {
    RECORD_FLAGS = VSR_IO_SQE_LINK | VSR_IO_SQE_FIXED_FILE |
                   VSR_IO_SQE_FIXED_BUFFER | VSR_IO_SQE_BUFFER_SELECT |
                   VSR_IO_SQE_SKIP_SUCCESS | VSR_IO_SQE_DIRECT
};

/* Every feature the executor relies on (design section 2); init refuses a
 * kernel lacking one with -ENOSYS. SINGLE_MMAP: one mapping holds both
 * rings; NODROP: the CQ overflows into a list instead of dropping; EXT_ARG
 * and MIN_TIMEOUT: the wait argument with a timeout and a minimum wait;
 * REG_REG_RING: register calls through the registered ring; RSRC_TAGS: the
 * tagged table update calls; CQE_SKIP: SKIP_SUCCESS; LINKED_FILE: a DIRECT
 * slot opened earlier in a chain. */
#define REQUIRED_FEATURES                                                      \
    (IORING_FEAT_SINGLE_MMAP | IORING_FEAT_NODROP | IORING_FEAT_EXT_ARG |      \
     IORING_FEAT_REG_REG_RING | IORING_FEAT_MIN_TIMEOUT |                      \
     IORING_FEAT_RSRC_TAGS | IORING_FEAT_CQE_SKIP | IORING_FEAT_LINKED_FILE)

/* Every io_uring opcode the translation table and the wake poll emit; init
 * refuses a kernel whose probe lacks one with -ENOSYS. */
static const uint8_t required_opcodes[] = {
    IORING_OP_NOP,          IORING_OP_READ,        IORING_OP_WRITE,
    IORING_OP_READ_FIXED,   IORING_OP_WRITE_FIXED, IORING_OP_READV,
    IORING_OP_WRITEV,       IORING_OP_READV_FIXED, IORING_OP_WRITEV_FIXED,
    IORING_OP_FSYNC,        IORING_OP_FALLOCATE,   IORING_OP_OPENAT,
    IORING_OP_CLOSE,        IORING_OP_RENAMEAT,    IORING_OP_UNLINKAT,
    IORING_OP_MKDIRAT,      IORING_OP_STATX,       IORING_OP_SOCKET,
    IORING_OP_CONNECT,      IORING_OP_BIND,        IORING_OP_LISTEN,
    IORING_OP_ACCEPT,       IORING_OP_RECV,        IORING_OP_SEND,
    IORING_OP_SEND_ZC,      IORING_OP_SENDMSG_ZC,  IORING_OP_SHUTDOWN,
    IORING_OP_URING_CMD,    IORING_OP_TIMEOUT,     IORING_OP_TIMEOUT_REMOVE,
    IORING_OP_ASYNC_CANCEL, IORING_OP_POLL_ADD};

/* Entry 0 of a provided-buffer ring doubles as its header: the kernel's
 * struct io_uring_buf_ring overlays the tail on that entry's resv field.
 * This is the same overlay with the tail as an atomic, stored with release
 * ordering so the entries written before it are visible to the kernel. */
struct buf_ring_header {
    uint64_t reserved1;
    uint32_t reserved2;
    uint16_t reserved3;
    _Atomic uint16_t tail;
};
_Static_assert(sizeof(struct buf_ring_header) == sizeof(struct io_uring_buf),
               "buffer ring header must overlay entry 0");
_Static_assert(offsetof(struct buf_ring_header, tail) ==
                   offsetof(struct io_uring_buf_ring, tail),
               "buffer ring tail must overlay the kernel's");

/* The msghdr's iovec pointer is not const, though the vectors are never
 * written; the conversion reads the pointer back through a union rather
 * than a cast that drops the qualifier. */
static struct iovec *vectors_of(const void *pointer)
{
    union {
        const void *in;
        struct iovec *out;
    } convert;

    convert.in = pointer;
    return convert.out;
}

static uint64_t as_u64(const void *pointer)
{
    return (uint64_t)(uintptr_t)pointer;
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

static void to_timespec(uint64_t ns, struct __kernel_timespec *ts)
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
 * Syscalls
 * --------------------------------------------------------------------- */

static int ring_setup(uint32_t entries, struct io_uring_params *params)
{
    long rc = syscall(__NR_io_uring_setup, (unsigned long)entries, params);

    return rc < 0 ? -errno : (int)rc;
}

/* io_uring_enter through the registered ring index once there is one. */
static int ring_enter(const struct vsr_io_uring_ring *ring, uint32_t to_submit,
                      uint32_t min_complete, uint32_t flags, const void *arg,
                      size_t arg_size)
{
    long rc = syscall(__NR_io_uring_enter, (unsigned long)ring->enter_fd,
                      (unsigned long)to_submit, (unsigned long)min_complete,
                      (unsigned long)(flags | ring->enter_flags), arg,
                      (unsigned long)arg_size);

    return rc < 0 ? -errno : (int)rc;
}

/* io_uring_register, through the registered ring index once there is one
 * (IORING_FEAT_REG_REG_RING, required). */
static int ring_register(const struct vsr_io_uring_ring *ring, unsigned opcode,
                         const void *arg, unsigned nr_args)
{
    long rc;

    if (ring->enter_flags & IORING_ENTER_REGISTERED_RING) {
        opcode |= (unsigned)IORING_REGISTER_USE_REGISTERED_RING;
    }
    rc = syscall(__NR_io_uring_register, (unsigned long)ring->enter_fd,
                 (unsigned long)opcode, arg, (unsigned long)nr_args);
    return rc < 0 ? -errno : (int)rc;
}

/* ------------------------------------------------------------------------
 * Layout
 * --------------------------------------------------------------------- */

struct uring_plan {
    size_t buffers;
    size_t groups;
    size_t directs;
    size_t timespecs;
    size_t msghdrs;
    size_t total;
    size_t page;
    uint32_t sq_ring;
    uint32_t cq_ring;
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
    /* SQPOLL excludes DEFER_TASKRUN, COOP_TASKRUN and TASKRUN_FLAG (-EINVAL
     * at setup): the SQ thread runs the task work. */
    unsigned flags = IORING_SETUP_SINGLE_ISSUER | IORING_SETUP_SUBMIT_ALL |
                     IORING_SETUP_NO_SQARRAY | IORING_SETUP_CQSIZE;

    if (options->sqpoll_idle_ms > 0) {
        flags |= IORING_SETUP_SQPOLL;
        if (options->sqpoll_cpu != UINT32_MAX) {
            flags |= IORING_SETUP_SQ_AFF;
        }
    } else {
        /* TASKRUN_FLAG: the kernel raises IORING_SQ_TASKRUN when deferred
         * task work waits for an enter, which reap() then issues. */
        flags |= IORING_SETUP_DEFER_TASKRUN | IORING_SETUP_COOP_TASKRUN |
                 IORING_SETUP_TASKRUN_FLAG;
    }
    return flags;
}

static int plan(const struct vsr_io_uring_options *options,
                struct uring_plan *plan)
{
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
    plan->cq_ring = round_pow2(options->cq_entries);

    offset = sizeof(struct vsr_io_uring);
    if (!reserve(&offset, options->buffer_regions,
                 sizeof(struct vsr_io_uring_region),
                 alignof(struct vsr_io_uring_region), &plan->buffers) ||
        !reserve(&offset, VSR_IO_URING_GROUPS,
                 sizeof(struct vsr_io_uring_group),
                 alignof(struct vsr_io_uring_group), &plan->groups) ||
        !reserve(&offset, options->cq_entries,
                 sizeof(struct vsr_io_uring_direct),
                 alignof(struct vsr_io_uring_direct), &plan->directs) ||
        !reserve(&offset, plan->sq_ring, sizeof(struct __kernel_timespec),
                 alignof(struct __kernel_timespec), &plan->timespecs) ||
        !reserve(&offset, plan->sq_ring, sizeof(struct msghdr),
                 alignof(struct msghdr), &plan->msghdrs)) {
        return VSR_ELIMIT;
    }
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

/* The ring index of `sqe`, or 0 when it lies outside the SQE array (the
 * unit test translates into its own memory). */
static uint32_t slot_of(const struct vsr_io_uring *uring,
                        const struct io_uring_sqe *sqe)
{
    if (uring->ring.sqes != NULL) {
        uintptr_t base = (uintptr_t)uring->ring.sqes;
        uintptr_t at = (uintptr_t)sqe;

        if (at >= base &&
            at - base < (uintptr_t)uring->table_entries * sizeof(*sqe)) {
            return (uint32_t)((at - base) / sizeof(*sqe));
        }
    }
    return 0;
}

static struct __kernel_timespec *timespec_for(struct vsr_io_uring *uring,
                                              const struct io_uring_sqe *sqe)
{
    if (uring->timespecs == NULL || uring->table_entries == 0) {
        return NULL;
    }
    return &uring->timespecs[slot_of(uring, sqe)];
}

static struct msghdr *msghdr_for(struct vsr_io_uring *uring,
                                 const struct io_uring_sqe *sqe)
{
    if (uring->msghdrs == NULL || uring->table_entries == 0) {
        return NULL;
    }
    return &uring->msghdrs[slot_of(uring, sqe)];
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

/* The target slot of a DIRECT record, or -EINVAL. */
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

/* The common SQE fields; the SQE is zero before. */
static void fill(struct io_uring_sqe *sqe, uint8_t opcode, int32_t fd,
                 const void *addr, uint32_t len, uint64_t off)
{
    sqe->opcode = opcode;
    sqe->fd = fd;
    sqe->off = off;
    sqe->addr = as_u64(addr);
    sqe->len = len;
}

/* The direct-descriptor target: the kernel takes the slot plus one, with
 * 0 meaning none, so IORING_FILE_INDEX_ALLOC stands for itself. */
static void fill_direct(struct io_uring_sqe *sqe, unsigned slot)
{
    sqe->file_index = slot == IORING_FILE_INDEX_ALLOC ? slot : slot + 1;
}

static int translate_recv(const struct vsr_io_uring *uring,
                          const struct vsr_io_sqe *record,
                          struct io_uring_sqe *sqe, unsigned *sqe_flags)
{
    uint32_t known = VSR_IO_RECV_MULTISHOT | VSR_IO_RECV_PEEK;
    bool multishot = (record->op_flags & VSR_IO_RECV_MULTISHOT) != 0;
    bool select = (record->flags & VSR_IO_SQE_BUFFER_SELECT) != 0;
    uint32_t msg_flags = 0;
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
        fill(sqe, IORING_OP_RECV, record->fd, NULL, record->length, 0);
        sqe->ioprio |= IORING_RECV_MULTISHOT;
    } else if (select) {
        fill(sqe, IORING_OP_RECV, record->fd, NULL, record->length, 0);
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
        fill(sqe, IORING_OP_RECV, record->fd, record->addr, record->length, 0);
    }
    sqe->msg_flags = msg_flags;
    if (select) {
        /* POLL_FIRST: the buffer is selected at a delivery, so an empty
         * ring is -ENOBUFS then and never at arming (decision 65). */
        *sqe_flags |= IOSQE_BUFFER_SELECT;
        sqe->buf_group = record->buffer_group;
        sqe->ioprio |= IORING_RECVSEND_POLL_FIRST;
    }
    return 0;
}

static int translate_send(struct vsr_io_uring *uring,
                          const struct vsr_io_sqe *record,
                          struct io_uring_sqe *sqe)
{
    uint32_t known = VSR_IO_SEND_ZERO_COPY | VSR_IO_SEND_VECTORED;
    bool zero_copy = (record->op_flags & VSR_IO_SEND_ZERO_COPY) != 0;
    bool vectored = (record->op_flags & VSR_IO_SEND_VECTORED) != 0;
    bool fixed = (record->flags & VSR_IO_SQE_FIXED_BUFFER) != 0;
    uint32_t msg_flags = MSG_NOSIGNAL;

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
        if (fixed && vectored) {
            /* SENDMSG_ZC over a registered region: the msghdr names the
             * vectors and lives in the per-slot table (decision 53). */
            struct msghdr *msg = msghdr_for(uring, sqe);

            if (msg == NULL) {
                return -EINVAL;
            }
            memset(msg, 0, sizeof(*msg));
            msg->msg_iov = vectors_of(record->addr);
            msg->msg_iovlen = record->length;
            fill(sqe, IORING_OP_SENDMSG_ZC, record->fd, msg, 1, 0);
            sqe->ioprio |= IORING_RECVSEND_FIXED_BUF;
            sqe->buf_index = record->buffer_index;
        } else {
            fill(sqe, IORING_OP_SEND_ZC, record->fd, record->addr,
                 record->length, 0);
            if (fixed) {
                sqe->ioprio |= IORING_RECVSEND_FIXED_BUF;
                sqe->buf_index = record->buffer_index;
            }
            if (vectored) {
                sqe->ioprio |= IORING_SEND_VECTORIZED;
            }
        }
        sqe->msg_flags = msg_flags;
        return 0;
    }
    if (fixed) {
        int rc = vectored ? check_vectors(uring, record)
                          : check_region(uring, record);

        if (rc != 0) {
            return rc;
        }
    }
    fill(sqe, IORING_OP_SEND, record->fd, record->addr, record->length, 0);
    sqe->msg_flags = msg_flags;
    if (vectored) {
        sqe->ioprio |= IORING_SEND_VECTORIZED;
    }
    return 0;
}

static int translate_timeout(struct vsr_io_uring *uring,
                             const struct vsr_io_sqe *record,
                             struct io_uring_sqe *sqe)
{
    struct __kernel_timespec *ts = timespec_for(uring, sqe);
    uint32_t flags = 0;

    if ((record->op_flags & ~(uint32_t)VSR_IO_TIMEOUT_ABSOLUTE) != 0 ||
        ts == NULL) {
        return -EINVAL;
    }
    if (record->op_flags & VSR_IO_TIMEOUT_ABSOLUTE) {
        flags |= IORING_TIMEOUT_ABS; /* CLOCK_MONOTONIC, the executor's. */
    }
    to_timespec(record->offset, ts);
    if (record->opcode == VSR_IO_SQE_TIMEOUT) {
        /* len is the completion count (1: none), off the count field. */
        fill(sqe, IORING_OP_TIMEOUT, -1, ts, 1, 0);
    } else {
        const uint64_t *target = record->addr2;

        if (target == NULL) {
            return -EINVAL;
        }
        /* addr is the target's user_data, addr2 the new timespec. */
        fill(sqe, IORING_OP_TIMEOUT_REMOVE, -1, NULL, 0, as_u64(ts));
        sqe->addr = *target;
        flags |= IORING_TIMEOUT_UPDATE;
    }
    sqe->timeout_flags = flags;
    return 0;
}

static int translate_cancel(const struct vsr_io_sqe *record,
                            struct io_uring_sqe *sqe)
{
    uint32_t known = VSR_IO_CANCEL_BY_FD | VSR_IO_CANCEL_ALL;
    uint32_t flags = 0;

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
        fill(sqe, IORING_OP_ASYNC_CANCEL, record->fd, NULL, 0, 0);
    } else {
        fill(sqe, IORING_OP_ASYNC_CANCEL, -1, NULL, 0, 0);
        sqe->addr = record->offset; /* The target's user_data. */
    }
    sqe->cancel_flags = flags;
    return 0;
}

static int translate_socket(const struct vsr_io_sqe *record,
                            struct io_uring_sqe *sqe, bool direct,
                            unsigned slot)
{
    if (record->offset > INT_MAX || record->length > INT_MAX) {
        return -EINVAL;
    }
    /* fd is the domain, off the type, len the protocol. */
    fill(sqe, IORING_OP_SOCKET, (int32_t)record->length, NULL,
         (uint32_t)record->offset, record->op_flags);
    if (direct) {
        fill_direct(sqe, slot);
    }
    return 0;
}

static int translate_accept(const struct vsr_io_sqe *record,
                            struct io_uring_sqe *sqe, bool direct,
                            unsigned slot)
{
    bool multishot = (record->op_flags & VSR_IO_ACCEPT_MULTISHOT) != 0;

    if ((record->op_flags & ~(uint32_t)VSR_IO_ACCEPT_MULTISHOT) != 0) {
        return -EINVAL;
    }
    /* The kernel's multishot accept allocates slots only. */
    if (multishot && ((record->flags & VSR_IO_SQE_SKIP_SUCCESS) ||
                      (direct && slot != IORING_FILE_INDEX_ALLOC))) {
        return -EINVAL;
    }
    fill(sqe, IORING_OP_ACCEPT, record->fd, NULL, 0, 0);
    /* A direct descriptor is close-on-exec by nature. */
    sqe->accept_flags = direct ? 0 : SOCK_CLOEXEC;
    if (multishot) {
        sqe->ioprio |= IORING_ACCEPT_MULTISHOT;
    }
    if (direct) {
        fill_direct(sqe, slot);
    }
    return 0;
}

static int translate_sockopt(const struct vsr_io_sqe *record,
                             struct io_uring_sqe *sqe)
{
    if (record->length > INT_MAX) {
        return -EINVAL;
    }
    /* The kernel's socket command serves GETSOCKOPT at SOL_SOCKET only;
     * the contract makes every other level -EOPNOTSUPP on both executors
     * (decision 65), so it is refused here rather than by the kernel. */
    if (record->opcode == VSR_IO_SQE_GETSOCKOPT &&
        (record->op_flags >> 16) != SOL_SOCKET) {
        return -EOPNOTSUPP;
    }
    sqe->opcode = IORING_OP_URING_CMD;
    sqe->fd = record->fd;
    sqe->cmd_op = record->opcode == VSR_IO_SQE_SETSOCKOPT
                      ? SOCKET_URING_OP_SETSOCKOPT
                      : SOCKET_URING_OP_GETSOCKOPT;
    sqe->level = record->op_flags >> 16;
    sqe->optname = record->op_flags & 0xffffu;
    sqe->optval = as_u64(record->addr);
    sqe->optlen = record->length;
    return 0;
}

int vsr_io_uring_translate(struct vsr_io_uring *uring,
                           const struct vsr_io_sqe *record,
                           struct io_uring_sqe *sqe)
{
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
        fill(sqe, IORING_OP_NOP, -1, NULL, 0, 0);
        break;
    case VSR_IO_SQE_READ:
    case VSR_IO_SQE_WRITE: {
        bool read = record->opcode == VSR_IO_SQE_READ;

        if (fixed) {
            rc = check_region(uring, record);
            if (rc == 0) {
                fill(sqe, read ? IORING_OP_READ_FIXED : IORING_OP_WRITE_FIXED,
                     record->fd, record->addr, record->length, record->offset);
                sqe->buf_index = record->buffer_index;
            }
        } else {
            fill(sqe, read ? IORING_OP_READ : IORING_OP_WRITE, record->fd,
                 record->addr, record->length, record->offset);
        }
        break;
    }
    case VSR_IO_SQE_READV:
    case VSR_IO_SQE_WRITEV: {
        bool read = record->opcode == VSR_IO_SQE_READV;

        if (record->length > MAX_VECTORS) {
            rc = -EINVAL;
        } else if (fixed) {
            rc = check_vectors(uring, record);
            if (rc == 0) {
                fill(sqe, read ? IORING_OP_READV_FIXED : IORING_OP_WRITEV_FIXED,
                     record->fd, record->addr, record->length, record->offset);
                sqe->buf_index = record->buffer_index;
            }
        } else {
            fill(sqe, read ? IORING_OP_READV : IORING_OP_WRITEV, record->fd,
                 record->addr, record->length, record->offset);
        }
        break;
    }
    case VSR_IO_SQE_FSYNC:
        if ((record->op_flags & ~(uint32_t)VSR_IO_FSYNC_DATASYNC) != 0) {
            rc = -EINVAL;
        } else {
            fill(sqe, IORING_OP_FSYNC, record->fd, NULL, 0, 0);
            sqe->fsync_flags = (record->op_flags & VSR_IO_FSYNC_DATASYNC)
                                   ? IORING_FSYNC_DATASYNC
                                   : 0;
        }
        break;
    case VSR_IO_SQE_FALLOCATE:
        /* len is the mode, addr the length. */
        fill(sqe, IORING_OP_FALLOCATE, record->fd, NULL, record->op_flags,
             record->offset);
        sqe->addr = record->length;
        break;
    case VSR_IO_SQE_OPENAT:
        /* len is the mode. */
        fill(sqe, IORING_OP_OPENAT, record->fd, record->addr, record->length,
             0);
        sqe->open_flags = record->op_flags;
        if (direct) {
            /* A direct descriptor is close-on-exec by nature: the kernel
             * refuses O_CLOEXEC with a slot (-EINVAL), as SOCK_CLOEXEC on
             * a direct accept. */
            sqe->open_flags &= ~(uint32_t)O_CLOEXEC;
            fill_direct(sqe, slot);
        }
        break;
    case VSR_IO_SQE_CLOSE:
        if (flags & VSR_IO_SQE_FIXED_FILE) {
            if (record->fd < 0) {
                rc = -EBADF;
            } else {
                fill(sqe, IORING_OP_CLOSE, 0, NULL, 0, 0);
                fill_direct(sqe, (unsigned)record->fd);
            }
        } else {
            fill(sqe, IORING_OP_CLOSE, record->fd, NULL, 0, 0);
        }
        break;
    case VSR_IO_SQE_RENAMEAT:
        /* len is the new directory descriptor, off the new path. */
        fill(sqe, IORING_OP_RENAMEAT, record->fd, record->addr,
             (uint32_t)record->fd, as_u64(record->addr2));
        sqe->rename_flags = record->op_flags;
        break;
    case VSR_IO_SQE_UNLINKAT:
        fill(sqe, IORING_OP_UNLINKAT, record->fd, record->addr, 0, 0);
        sqe->unlink_flags = record->op_flags;
        break;
    case VSR_IO_SQE_MKDIRAT:
        fill(sqe, IORING_OP_MKDIRAT, record->fd, record->addr, record->length,
             0);
        break;
    case VSR_IO_SQE_STATX:
        /* len is the mask, off the statx buffer. */
        fill(sqe, IORING_OP_STATX, record->fd, record->addr, record->length,
             as_u64(record->addr2));
        sqe->statx_flags = record->op_flags;
        break;
    case VSR_IO_SQE_SOCKET:
        rc = translate_socket(record, sqe, direct, slot);
        break;
    case VSR_IO_SQE_CONNECT:
    case VSR_IO_SQE_BIND:
        /* off is the address length. */
        fill(sqe,
             record->opcode == VSR_IO_SQE_CONNECT ? IORING_OP_CONNECT
                                                  : IORING_OP_BIND,
             record->fd, record->addr, 0, record->length);
        break;
    case VSR_IO_SQE_LISTEN:
        if (record->length > INT_MAX) {
            rc = -EINVAL;
        } else {
            fill(sqe, IORING_OP_LISTEN, record->fd, NULL, record->length, 0);
        }
        break;
    case VSR_IO_SQE_ACCEPT:
        rc = translate_accept(record, sqe, direct, slot);
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
            fill(sqe, IORING_OP_SHUTDOWN, record->fd, NULL, record->length, 0);
        }
        break;
    case VSR_IO_SQE_SETSOCKOPT:
    case VSR_IO_SQE_GETSOCKOPT:
        rc = translate_sockopt(record, sqe);
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
    fill(sqe, IORING_OP_NOP, -1, NULL, (uint32_t)error, 0);
    sqe->nop_flags = IORING_NOP_INJECT_RESULT;
    sqe->user_data = record->user_data;
    if (link) {
        sqe->flags = IOSQE_IO_LINK;
    }
}

/* ------------------------------------------------------------------------
 * CQE -> record
 * --------------------------------------------------------------------- */

void vsr_io_uring_reap_one(const struct io_uring_cqe *cqe,
                           struct vsr_io_cqe *record)
{
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

/* ------------------------------------------------------------------------
 * The ring: SQ and CQ bookkeeping over the shared memory
 * --------------------------------------------------------------------- */

/* Publishes the filled SQEs to the kernel and returns how many it has not
 * consumed yet. The release store orders the SQE writes before the tail
 * the kernel (an SQPOLL thread, or the enter that follows) reads. */
static uint32_t ring_flush_sq(struct vsr_io_uring_ring *ring)
{
    atomic_store_explicit(ring->sq_tail, ring->sq_local_tail,
                          memory_order_release);
    return ring->sq_local_tail -
           atomic_load_explicit(ring->sq_head, memory_order_relaxed);
}

/* Free SQ slots. The acquire load pairs with the kernel's release of the
 * head after it read the SQEs, so a slot is never overwritten early. */
static uint32_t ring_sq_space(const struct vsr_io_uring_ring *ring)
{
    uint32_t head = atomic_load_explicit(ring->sq_head, memory_order_acquire);

    return ring->sq_entries - (ring->sq_local_tail - head);
}

/* The next free SQE; the caller ensured the space. */
static struct io_uring_sqe *ring_get_sqe(struct vsr_io_uring_ring *ring)
{
    struct io_uring_sqe *sqe = &ring->sqes[ring->sq_local_tail & ring->sq_mask];

    ring->sq_local_tail++;
    return sqe;
}

/* Kernel-side conditions that need an enter: a full CQ whose overflow list
 * waits to be flushed, or deferred task work (TASKRUN_FLAG). */
static bool ring_cq_needs_enter(const struct vsr_io_uring_ring *ring)
{
    return (atomic_load_explicit(ring->sq_flags, memory_order_relaxed) &
            (IORING_SQ_CQ_OVERFLOW | IORING_SQ_TASKRUN)) != 0;
}

/* Under SQPOLL, whether the SQ thread sleeps and needs SQ_WAKEUP; the full
 * fence orders the tail store before the flags load, so a thread that goes
 * to sleep after checking the tail is not missed. */
static bool ring_sqpoll_needs_wakeup(const struct vsr_io_uring_ring *ring)
{
    atomic_thread_fence(memory_order_seq_cst);
    return (atomic_load_explicit(ring->sq_flags, memory_order_relaxed) &
            IORING_SQ_NEED_WAKEUP) != 0;
}

/* Hands the filled SQEs to the kernel: an enter with to_submit, except
 * under SQPOLL where the SQ thread takes them and only a sleeping thread
 * is woken (SQ_WAKEUP). GETEVENTS is added on request and whenever the SQ
 * flags ask for it, so completions due are posted. Returns the enter
 * result (SQEs consumed) or a negative errno; EINTR is retried. */
static int ring_submit(struct vsr_io_uring_ring *ring, bool get_events)
{
    uint32_t to_submit = ring_flush_sq(ring);
    uint32_t flags = 0;
    bool enter = to_submit > 0;

    if (ring->setup & IORING_SETUP_SQPOLL) {
        if (ring_sqpoll_needs_wakeup(ring)) {
            flags |= IORING_ENTER_SQ_WAKEUP;
        } else {
            enter = false;
        }
    }
    if (get_events || ring_cq_needs_enter(ring)) {
        flags |= IORING_ENTER_GETEVENTS;
        enter = true;
    }
    if (!enter) {
        return (int)to_submit;
    }
    for (;;) {
        int rc = ring_enter(ring, to_submit, 0, flags, NULL, 0);

        if (rc != -EINTR) {
            return rc;
        }
    }
}

/* Waits for `wait_nr` completions, at most `ts` (relative; NULL: forever)
 * and, with `min_wait_us`, returns at that mark once any completion has
 * arrived (IORING_FEAT_MIN_TIMEOUT): the kernel waits the minimum for the
 * full count and, if that yields nothing, continues for one completion up
 * to the timeout. Pending SQEs are flushed by the same enter. */
static int ring_wait(struct vsr_io_uring_ring *ring, uint32_t wait_nr,
                     const struct __kernel_timespec *ts, uint32_t min_wait_us)
{
    struct io_uring_getevents_arg arg;
    uint32_t to_submit = ring_flush_sq(ring);
    uint32_t flags = IORING_ENTER_GETEVENTS | IORING_ENTER_EXT_ARG;

    memset(&arg, 0, sizeof(arg));
    arg.min_wait_usec = min_wait_us;
    arg.ts = as_u64(ts);
    if ((ring->setup & IORING_SETUP_SQPOLL) && ring_sqpoll_needs_wakeup(ring)) {
        flags |= IORING_ENTER_SQ_WAKEUP;
    }
    return ring_enter(ring, to_submit, wait_nr, flags, &arg, sizeof(arg));
}

/* Completions posted and not consumed: [head, tail). The acquire load of
 * the tail pairs with the kernel's release after writing the CQEs. */
static uint32_t ring_cq_ready(const struct vsr_io_uring_ring *ring,
                              uint32_t *head)
{
    *head = atomic_load_explicit(ring->cq_head, memory_order_relaxed);
    return atomic_load_explicit(ring->cq_tail, memory_order_acquire) - *head;
}

/* Returns `seen` CQEs to the kernel; the release store orders the reads
 * of the entries before the head that lets the kernel reuse them. */
static void ring_cq_advance(struct vsr_io_uring_ring *ring, uint32_t head,
                            uint32_t seen)
{
    if (seen > 0) {
        atomic_store_explicit(ring->cq_head, head + seen, memory_order_release);
    }
}

static uint32_t uring_reap(void *ctx, struct vsr_io_cqe *cqes,
                           uint32_t capacity)
{
    struct vsr_io_uring *uring = ctx;
    struct vsr_io_uring_ring *ring = &uring->ring;
    uint32_t head;
    uint32_t ready;
    uint32_t seen = 0;
    uint32_t n = 0;

    /* Deferred task work or an overflowed CQ: an enter with GETEVENTS and
     * no minimum posts what is due without blocking. */
    if (uring->failure == 0 && ring_cq_needs_enter(ring)) {
        int rc;

        do {
            rc = ring_enter(ring, 0, 0, IORING_ENTER_GETEVENTS, NULL, 0);
        } while (rc == -EINTR);
        if (rc < 0 && rc != -EAGAIN && rc != -EBUSY && uring->failure == 0) {
            uring->failure = rc;
        }
    }
    ready = ring_cq_ready(ring, &head);
    for (uint32_t at = head; at != head + ready; ++at) {
        const struct io_uring_cqe *cqe = &ring->cqes[at & ring->cq_mask];

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
    ring_cq_advance(ring, head, seen);
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

/* Makes room for `count` SQEs, submitting what the SQ holds; under SQPOLL
 * a full SQ waits (SQ_WAIT) for the SQ thread to consume entries. */
static int ensure_space(struct vsr_io_uring *uring, uint32_t count)
{
    struct vsr_io_uring_ring *ring = &uring->ring;

    while (ring_sq_space(ring) < count) {
        int rc = ring_submit(ring, false);

        if (rc < 0) {
            return rc;
        }
        if (ring_sq_space(ring) >= count) {
            break;
        }
        if (!(ring->setup & IORING_SETUP_SQPOLL)) {
            return -EBUSY;
        }
        rc = ring_enter(ring, 0, 0, IORING_ENTER_SQ_WAIT, NULL, 0);
        if (rc < 0 && rc != -EINTR) {
            return rc;
        }
    }
    return 0;
}

static int arm_wake(struct vsr_io_uring *uring)
{
    struct io_uring_sqe *sqe;
    uint32_t events = POLLIN;
    int rc = ensure_space(uring, 1);

    if (rc != 0) {
        return rc;
    }
    sqe = ring_get_sqe(&uring->ring);
    memset(sqe, 0, sizeof(*sqe));
    /* A multishot poll: len carries the poll flags, poll32_events the mask
     * as a 32-bit word (its halves swapped on big-endian hosts). */
    fill(sqe, IORING_OP_POLL_ADD, uring->wake_fd, NULL, IORING_POLL_ADD_MULTI,
         0);
#if __BYTE_ORDER == __BIG_ENDIAN
    events = (events << 16) | (events >> 16);
#endif
    sqe->poll32_events = events;
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
    uint32_t entries = uring->ring.sq_entries;
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
            sqe = ring_get_sqe(&uring->ring);
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
static bool wake_pending(const struct vsr_io_uring *uring, uint32_t *scan)
{
    const struct vsr_io_uring_ring *ring = &uring->ring;
    uint32_t head;
    uint32_t ready = ring_cq_ready(ring, &head);
    uint32_t tail = head + ready;
    uint32_t at = *scan;

    if (at - head > ready) {
        at = head;
    }
    for (; at != tail; ++at) {
        if (ring->cqes[at & ring->cq_mask].user_data ==
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
    struct vsr_io_uring_ring *ring = &uring->ring;
    uint64_t start = monotonic_ns();
    uint64_t window_end;
    uint32_t scan;
    int rc;

    if (uring->failure != 0) {
        return uring->failure;
    }
    if ((count > 0 && sqes == NULL) || want > ring->cq_entries) {
        return -EINVAL;
    }
    for (uint32_t i = 0; i < count; ++i) {
        if (sqes[i].user_data == VSR_IO_URING_WAKE_USER_DATA) {
            return -EINVAL;
        }
    }
    scan = atomic_load_explicit(ring->cq_head, memory_order_relaxed);
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
    rc = ring_submit(ring, true);
    if (rc < 0 && rc != -EAGAIN && rc != -EBUSY) {
        return fail(uring, rc);
    }

    window_end = min_wait_ns > VSR_NO_DEADLINE - start ? VSR_NO_DEADLINE
                                                       : start + min_wait_ns;
    for (;;) {
        struct __kernel_timespec ts;
        const struct __kernel_timespec *timeout = NULL;
        uint32_t head;
        uint32_t available;
        uint32_t wait_nr;
        uint32_t min_us = 0;
        uint64_t now;
        uint64_t limit;

        if (wake_pending(uring, &scan)) {
            return 0;
        }
        available = ring_cq_ready(ring, &head);
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

                min_us = us > UINT32_MAX ? UINT32_MAX : (uint32_t)us;
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
            to_timespec(limit - now, &ts);
            timeout = &ts;
        }
        rc = ring_wait(ring, wait_nr, timeout, min_us);
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

/* The kernel refuses a file table larger than RLIMIT_NOFILE with -EMFILE;
 * the soft limit is raised by the table size, within the hard limit. */
static int raise_nofile(uint32_t slots)
{
    struct rlimit limit;

    if (getrlimit(RLIMIT_NOFILE, &limit) != 0) {
        return -errno;
    }
    if (limit.rlim_cur >= slots) {
        return 0;
    }
    limit.rlim_cur = limit.rlim_max - limit.rlim_cur > slots
                         ? limit.rlim_cur + slots
                         : limit.rlim_max;
    return setrlimit(RLIMIT_NOFILE, &limit) == 0 ? 0 : -errno;
}

static int uring_register_files(void *ctx, uint32_t slots)
{
    struct vsr_io_uring *uring = ctx;
    struct io_uring_rsrc_register reg;
    int rc;

    if (uring->files_registered > 0) {
        return -EBUSY;
    }
    if (slots == 0 || slots > uring->options.file_slots) {
        return -EINVAL;
    }
    memset(&reg, 0, sizeof(reg));
    reg.nr = slots;
    reg.flags = IORING_RSRC_REGISTER_SPARSE;
    rc = ring_register(&uring->ring, IORING_REGISTER_FILES2, &reg,
                       REGISTER_SIZED(reg));
    if (rc == -EMFILE && raise_nofile(slots) == 0) {
        rc = ring_register(&uring->ring, IORING_REGISTER_FILES2, &reg,
                           REGISTER_SIZED(reg));
    }
    if (rc < 0) {
        return rc;
    }
    uring->files_registered = slots;
    return 0;
}

static int uring_update_file(void *ctx, uint32_t slot, int fd)
{
    struct vsr_io_uring *uring = ctx;
    struct io_uring_rsrc_update up;
    int file = fd < 0 ? -1 : fd;
    int rc;

    if (slot >= uring->files_registered) {
        return -EINVAL;
    }
    memset(&up, 0, sizeof(up));
    up.offset = slot;
    up.data = as_u64(&file);
    /* nr_args is the number of descriptors; the result the number
     * installed. */
    rc = ring_register(&uring->ring, IORING_REGISTER_FILES_UPDATE, &up, 1);
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
    struct io_uring_rsrc_register reg;
    int rc;

    if (uring->buffers_registered > 0) {
        return -EBUSY;
    }
    if (regions == 0 || regions > uring->options.buffer_regions) {
        return -EINVAL;
    }
    memset(&reg, 0, sizeof(reg));
    reg.nr = regions;
    reg.flags = IORING_RSRC_REGISTER_SPARSE;
    rc = ring_register(&uring->ring, IORING_REGISTER_BUFFERS2, &reg,
                       REGISTER_SIZED(reg));
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
    struct io_uring_rsrc_update2 up;
    struct iovec iov = {0};
    uint64_t tag = 0;
    int rc;

    if (index >= uring->buffers_registered) {
        return -EINVAL;
    }
    if (region != NULL) {
        iov.iov_base = region->base;
        iov.iov_len = region->size;
    }
    /* An empty iovec clears the entry; the tag stays 0 (no notification
     * when the kernel drops its reference). */
    memset(&up, 0, sizeof(up));
    up.offset = index;
    up.data = as_u64(&iov);
    up.tags = as_u64(&tag);
    up.nr = 1;
    rc = ring_register(&uring->ring, IORING_REGISTER_BUFFERS_UPDATE, &up,
                       REGISTER_SIZED(up));
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

    memset(&reg, 0, sizeof(reg));
    reg.bgid = group;
    if (entries == 0) {
        if (entry == NULL) {
            return -ENOENT;
        }
        rc = ring_register(&uring->ring, IORING_UNREGISTER_PBUF_RING, &reg, 1);
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
    /* Zeroing the ring clears its tail, the kernel's starting point. */
    memset(memory->base, 0, (size_t)entries * sizeof(struct io_uring_buf));
    reg.ring_addr = as_u64(memory->base);
    reg.ring_entries = entries;
    if (flags & VSR_IO_BUFFER_RING_INCREMENTAL) {
        reg.flags = IOU_PBUF_RING_INC;
    }
    rc = ring_register(&uring->ring, IORING_REGISTER_PBUF_RING, &reg, 1);
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

/* The kernel's head of a buffer ring: how far it has consumed. */
static int buffer_ring_head(struct vsr_io_uring *uring, uint16_t group,
                            uint32_t *head)
{
    struct io_uring_buf_status status;
    int rc;

    memset(&status, 0, sizeof(status));
    status.buf_group = group;
    rc = ring_register(&uring->ring, IORING_REGISTER_PBUF_STATUS, &status, 1);
    if (rc < 0) {
        return rc;
    }
    *head = status.head;
    return 0;
}

static int uring_provide(void *ctx, uint16_t group,
                         const struct vsr_io_buffer *buffers, uint32_t count)
{
    struct vsr_io_uring *uring = ctx;
    struct vsr_io_uring_group *entry = find_group(uring, group);
    struct buf_ring_header *header;
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
        uint32_t head;
        int rc = buffer_ring_head(uring, group, &head);

        if (rc < 0) {
            return rc;
        }
        entry->head = (uint16_t)head;
        held = (uint16_t)(entry->tail - entry->head);
        if (held + count > entry->entries) {
            return -ENOSPC;
        }
    }
    /* Entries are written through a pointer to the entry array rather
     * than the kernel's bufs[0] member, which the bounds sanitizer (with
     * -fstrict-flex-arrays=3) rejects; entry 0's resv field is the ring
     * tail and stays untouched until the release store below. */
    header = entry->ring_memory;
    slots = entry->ring_memory;
    mask = entry->entries - 1;
    for (uint32_t i = 0; i < count; ++i) {
        struct io_uring_buf *slot = slots + ((entry->tail + i) & mask);

        slot->addr = as_u64(buffers[i].base);
        slot->len = buffers[i].length;
        slot->bid = buffers[i].id;
    }
    entry->tail = (uint16_t)(entry->tail + count);
    atomic_store_explicit(&header->tail, (uint16_t)entry->tail,
                          memory_order_release);
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

static void *map_pages(int fd, size_t size, uint64_t offset)
{
    void *at = mmap(NULL, size, PROT_READ | PROT_WRITE,
                    MAP_SHARED | MAP_POPULATE, fd, (off_t)offset);

    return at == MAP_FAILED ? NULL : at;
}

static void unmap_ring(struct vsr_io_uring_ring *ring)
{
    if (ring->rings != NULL) {
        (void)munmap(ring->rings, ring->rings_size);
        ring->rings = NULL;
    }
    if (ring->sqes != NULL) {
        (void)munmap(ring->sqes, ring->sqes_size);
        ring->sqes = NULL;
    }
    ring->cqes = NULL;
}

/* Maps the rings and the SQE array the kernel allocated (one mapping for
 * both rings, IORING_FEAT_SINGLE_MMAP) and resolves the ring words from
 * the offsets setup returned. */
static int map_ring(struct vsr_io_uring *uring, const struct io_uring_params *p,
                    const struct uring_plan *layout)
{
    struct vsr_io_uring_ring *ring = &uring->ring;
    unsigned char *rings;
    size_t rings_size;

    if (p->sq_entries != layout->sq_ring || p->cq_entries != layout->cq_ring ||
        !vsr_size_add(p->cq_off.cqes,
                      (size_t)p->cq_entries * sizeof(struct io_uring_cqe),
                      &rings_size) ||
        p->sq_off.head > rings_size - sizeof(uint32_t) ||
        p->sq_off.tail > rings_size - sizeof(uint32_t) ||
        p->sq_off.flags > rings_size - sizeof(uint32_t) ||
        p->cq_off.head > rings_size - sizeof(uint32_t) ||
        p->cq_off.tail > rings_size - sizeof(uint32_t)) {
        return -EINVAL;
    }
    ring->rings_size = rings_size;
    ring->rings = map_pages(ring->fd, rings_size, IORING_OFF_SQ_RING);
    if (ring->rings == NULL) {
        return -errno;
    }
    ring->sqes_size = (size_t)p->sq_entries * sizeof(struct io_uring_sqe);
    ring->sqes = map_pages(ring->fd, ring->sqes_size, IORING_OFF_SQES);
    if (ring->sqes == NULL) {
        int rc = -errno;

        unmap_ring(ring);
        return rc;
    }
    rings = ring->rings;
    ring->setup = p->flags;
    ring->features = p->features;
    ring->sq_entries = p->sq_entries;
    ring->sq_mask = p->sq_entries - 1;
    ring->sq_local_tail = 0;
    ring->sq_head = (void *)(rings + p->sq_off.head);
    ring->sq_tail = (void *)(rings + p->sq_off.tail);
    ring->sq_flags = (void *)(rings + p->sq_off.flags);
    ring->cq_entries = p->cq_entries;
    ring->cq_mask = p->cq_entries - 1;
    ring->cq_head = (void *)(rings + p->cq_off.head);
    ring->cq_tail = (void *)(rings + p->cq_off.tail);
    ring->cqes = (void *)(rings + p->cq_off.cqes);
    return 0;
}

/* Registers the ring descriptor: enter and register take its index from
 * here on. */
static int register_ring_fd(struct vsr_io_uring_ring *ring)
{
    struct io_uring_rsrc_update up;
    int rc;

    memset(&up, 0, sizeof(up));
    up.offset = UINT32_MAX; /* Any free index. */
    up.data = (uint64_t)(unsigned)ring->fd;
    rc = ring_register(ring, IORING_REGISTER_RING_FDS, &up, 1);
    if (rc < 0) {
        return rc;
    }
    if (rc != 1 || up.offset > INT_MAX) {
        return -EINVAL;
    }
    ring->enter_fd = (int)up.offset;
    ring->enter_flags = IORING_ENTER_REGISTERED_RING;
    return 0;
}

static void unregister_ring_fd(struct vsr_io_uring_ring *ring)
{
    struct io_uring_rsrc_update up;

    if (!(ring->enter_flags & IORING_ENTER_REGISTERED_RING)) {
        return;
    }
    memset(&up, 0, sizeof(up));
    up.offset = (uint32_t)ring->enter_fd;
    /* Best effort: closing the ring releases the index at exit anyway. */
    (void)ring_register(ring, IORING_UNREGISTER_RING_FDS, &up, 1);
    ring->enter_fd = ring->fd;
    ring->enter_flags = 0;
}

/* The one-time check of decision 53: every opcode the translation table
 * emits must be supported, else the kernel is too old. */
static int probe_opcodes(const struct vsr_io_uring_ring *ring)
{
    _Alignas(struct io_uring_probe) unsigned char
        memory[sizeof(struct io_uring_probe) +
               IORING_OP_LAST * sizeof(struct io_uring_probe_op)];
    const struct io_uring_probe *probe = (const void *)memory;
    const struct io_uring_probe_op *ops =
        (const void *)(memory + offsetof(struct io_uring_probe, ops));
    int rc;

    memset(memory, 0, sizeof(memory));
    rc = ring_register(ring, IORING_REGISTER_PROBE, memory, IORING_OP_LAST);
    if (rc < 0) {
        return rc;
    }
    for (size_t i = 0; i < sizeof(required_opcodes); ++i) {
        uint8_t op = required_opcodes[i];

        if (op >= probe->ops_len || ops[op].op != op ||
            !(ops[op].flags & IO_URING_OP_SUPPORTED)) {
            return -ENOSYS;
        }
    }
    return 0;
}

/* Discards every posted completion, noting the wake poll's end. */
static void discard_completions(struct vsr_io_uring *uring)
{
    struct vsr_io_uring_ring *ring = &uring->ring;
    uint32_t head;
    uint32_t ready = ring_cq_ready(ring, &head);

    for (uint32_t at = head; at != head + ready; ++at) {
        const struct io_uring_cqe *cqe = &ring->cqes[at & ring->cq_mask];

        if (cqe->user_data == VSR_IO_URING_WAKE_USER_DATA &&
            !(cqe->flags & IORING_CQE_F_MORE)) {
            uring->wake_armed = 0;
        }
    }
    ring_cq_advance(ring, head, ready);
}

/* Cancels every request still in flight (IORING_REGISTER_SYNC_CANCEL with
 * ANY | ALL, which also waits, per round, for requests already completing)
 * and collects the completions, so that as little as possible is pending
 * when the ring is closed: the kernel finishes a closed ring
 * asynchronously, and a request still in flight then completes into the
 * caller's buffers after deinit has returned. What the cancel cannot reach
 * (an operation in progress in a kernel worker, a zero-copy notification
 * whose bytes the network still holds) still completes during that
 * teardown; the contract asks the caller to see those records complete
 * before deinit. */
static void drain(struct vsr_io_uring *uring)
{
    struct vsr_io_uring_ring *ring = &uring->ring;
    uint64_t limit = monotonic_ns() + DRAIN_LIMIT_NS;
    int rc;

    for (;;) {
        struct io_uring_sync_cancel_reg cancel;
        int found;

        memset(&cancel, 0, sizeof(cancel));
        cancel.flags = IORING_ASYNC_CANCEL_ANY | IORING_ASYNC_CANCEL_ALL;
        cancel.fd = -1;
        to_timespec(DRAIN_STEP_NS, &cancel.timeout);
        found = ring_register(ring, IORING_REGISTER_SYNC_CANCEL, &cancel, 1);
        /* Task work posts the completions of what was cancelled. */
        do {
            rc = ring_enter(ring, 0, 0, IORING_ENTER_GETEVENTS, NULL, 0);
        } while (rc == -EINTR);
        discard_completions(uring);
        /* > 0: cancelled now, there may be more; -ETIME and -EALREADY:
         * requests were still completing; anything else: nothing left,
         * or a cancel the kernel refuses. */
        if (found <= 0 && found != -ETIME && found != -EALREADY) {
            break;
        }
        if (monotonic_ns() >= limit) {
            break;
        }
    }
    if (ring->setup & IORING_SETUP_SQPOLL) {
        /* The SQ thread posts the completions: give them a moment. */
        struct __kernel_timespec ts;

        to_timespec(DRAIN_STEP_NS, &ts);
        (void)ring_wait(ring, 1, &ts, 0);
        discard_completions(uring);
    }
}

static void close_ring(struct vsr_io_uring *uring)
{
    /* The rings are mapped after setup succeeded (init may fail between
     * the two); only a mapped ring has anything to drain. */
    if (uring->ring.fd >= 0 && uring->ring.cqes != NULL) {
        drain(uring);
    }
    unregister_ring_fd(&uring->ring);
    /* The mappings go first: the kernel's teardown still writes the ring
     * words (it commits the CQ tail and clears SQ flags as it cancels),
     * into pages it owns and this side no longer sees. */
    unmap_ring(&uring->ring);
    if (uring->ring.fd >= 0) {
        /* Closing the ring releases every registration and cancels every
         * pending request; the region stays the caller's. */
        (void)close(uring->ring.fd);
        uring->ring.fd = -1;
        uring->ring.enter_fd = -1;
    }
    if (uring->wake_fd >= 0) {
        (void)close(uring->wake_fd);
        uring->wake_fd = -1;
    }
}

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
    uring->ring.fd = -1;
    uring->ring.enter_fd = -1;
    uring->wake_fd = -1;
    uring->buffers = (void *)(base + layout.buffers);
    uring->groups = (void *)(base + layout.groups);
    uring->directs = (void *)(base + layout.directs);
    uring->directs_capacity = options->cq_entries;
    for (uint32_t i = 0; i < uring->directs_capacity; ++i) {
        uring->directs[i].slot = -1;
    }
    uring->timespecs = (void *)(base + layout.timespecs);
    uring->msghdrs = (void *)(base + layout.msghdrs);
    uring->table_entries = layout.sq_ring;

    memset(&params, 0, sizeof(params));
    params.flags = layout.setup;
    params.cq_entries = options->cq_entries;
    if (layout.setup & IORING_SETUP_SQPOLL) {
        params.sq_thread_idle = options->sqpoll_idle_ms;
        params.sq_thread_cpu = options->sqpoll_cpu;
    }
    rc = ring_setup(options->sq_entries, &params);
    if (rc < 0) {
        return rc;
    }
    uring->ring.fd = rc;
    uring->ring.enter_fd = rc;
    if ((params.features & REQUIRED_FEATURES) != REQUIRED_FEATURES) {
        rc = -ENOSYS;
        goto fail;
    }
    rc = map_ring(uring, &params, &layout);
    if (rc < 0) {
        goto fail;
    }
    rc = register_ring_fd(&uring->ring);
    if (rc < 0) {
        goto fail;
    }
    rc = probe_opcodes(&uring->ring);
    if (rc < 0) {
        goto fail;
    }
    if (options->napi_busy_poll_us > 0) {
        struct io_uring_napi napi;

        memset(&napi, 0, sizeof(napi));
        napi.busy_poll_to = options->napi_busy_poll_us;
        rc = ring_register(&uring->ring, IORING_REGISTER_NAPI, &napi, 1);
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
        rc = ring_submit(&uring->ring, false);
    }
    if (rc < 0) {
        goto fail;
    }
    out->ops = &vsr_io_uring_ops;
    out->ctx = uring;
    return 0;

fail:
    close_ring(uring);
    return rc;
}

void vsr_io_uring_deinit(struct vsr_io_executor *executor)
{
    struct vsr_io_uring *uring;

    if (executor == NULL || executor->ctx == NULL) {
        return;
    }
    uring = executor->ctx;
    close_ring(uring);
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
    return uring->ring.fd;
}
