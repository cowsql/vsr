#ifndef VSR_IO_URING_H
#define VSR_IO_URING_H

#include "vsr-io.h"

/* The kernel's io_uring UAPI, vendored under src/io/uapi (its README says
 * where it comes from). It uses two extensions -Wpedantic reports:
 * zero-length arrays and an enumerator above INT_MAX. */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wpedantic"
#include "io/uapi/io_uring.h"
#pragma GCC diagnostic pop

#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/socket.h>

/*
 * io_uring executor (docs/io-implementation.md, "Executor: io_uring"): the
 * production implementation of struct vsr_io_executor_ops over one ring,
 * and one of the two modules that touch the kernel. Every operation's
 * semantics are the "Executor contract" of the implementation document;
 * the conformance suite runs the same tests over this executor and over
 * the simulation.
 *
 * The ring is driven through the raw io_uring_setup, io_uring_enter and
 * io_uring_register syscalls (decision 52): no liburing, the UAPI header
 * above is the whole interface. It is created with SINGLE_ISSUER |
 * DEFER_TASKRUN | COOP_TASKRUN | TASKRUN_FLAG | SUBMIT_ALL | NO_SQARRAY |
 * CQSIZE and the options' SQ/CQ sizes; SQPOLL replaces the three task-work
 * flags, which the kernel refuses to combine with it. The rings and the
 * SQE array are the kernel's pages, mapped from the ring descriptor
 * (IORING_OFF_SQ_RING for both rings, IORING_OFF_SQES), not memory of the
 * caller's region (NO_MMAP): the kernel finishes a closed ring
 * asynchronously and writes its ring words (the CQ tail it commits, the
 * SQ flags it clears) after close, so caller memory holding them would be
 * written after deinit returned it. REGISTERED_FD_ONLY is not used because it
 * leaves no descriptor for vsr_io_uring_fd; the ring descriptor is
 * registered after setup instead, and every enter and register call takes
 * the registered-ring path. Init then probes the opcode table
 * (IORING_REGISTER_PROBE) and the feature bits once and refuses an older
 * kernel with -ENOSYS (decision 53); nothing is probed after that. The
 * sparse file and buffer tables are registered by register_files() and
 * register_buffers() (once each, as the contract states), at most
 * options.file_slots and options.buffer_regions entries; buffer rings are
 * registered on buffer_ring() with the caller's page-aligned memory and
 * IOU_PBUF_RING_INC when INCREMENTAL is requested. wake() writes an eventfd
 * that a multishot poll on the ring watches; its completions are consumed
 * internally and never reach reap(). Time is CLOCK_MONOTONIC; random is
 * getrandom(2). deinit cancels every request still in flight
 * (IORING_REGISTER_SYNC_CANCEL) and discards the completions before it
 * unmaps and closes the ring, so that as little as possible completes
 * into the caller's buffers during the kernel's asynchronous teardown.
 *
 * Ring words: the head, tail, flags and array pointers below are computed
 * from the offsets setup returns. Words the kernel writes (SQ head, CQ
 * tail, SQ flags) are read with acquire loads and words this side writes
 * (SQ tail, CQ head, the buffer-ring tail) are published with release
 * stores, the pairing io_uring's shared-memory protocol requires; without
 * NO_SQARRAY there would be an index array, with it the SQE of ring index
 * i is sqes[i & mask].
 *
 * Record translation is a switch from enum vsr_io_sqe_opcode to the SQE
 * fields of the io_uring opcode, with flags mapped 1:1 (LINK ->
 * IOSQE_IO_LINK, FIXED_FILE -> IOSQE_FIXED_FILE, BUFFER_SELECT ->
 * IOSQE_BUFFER_SELECT, SKIP_SUCCESS -> IOSQE_CQE_SKIP_SUCCESS, DIRECT ->
 * file_index of fd2 + 1 or IORING_FILE_INDEX_ALLOC, FIXED_BUFFER -> the
 * *_FIXED opcode or IORING_RECVSEND_FIXED_BUF with buffer_index).
 * Completion flags map back the same way (IORING_CQE_F_MORE, F_BUFFER with
 * the buffer id in the upper flag bits, F_BUF_MORE, F_NOTIF). A record the
 * translation rejects becomes a NOP with IORING_NOP_INJECT_RESULT carrying
 * the negative errno and the record's user_data and LINK flag, so the
 * failure is an ordinary completion in submission order and fails its
 * chain.
 *
 * FIXED_BUFFER validation: the executor keeps every region it was given
 * and rejects, with -EFAULT in the completion, a record whose addr/length
 * (or any vector) is not inside region buffer_index; the kernel would
 * reject it too, but the check makes the failure deterministic and the
 * same as the simulation's. Zero-copy sends are the exception: their
 * range check is left to the kernel (the same rule, -EFAULT) because only
 * a kernel failure is followed by the NOTIF completion the contract
 * promises. Plain RECV and SEND have no fixed-buffer form in the kernel:
 * after validation the flag is dropped and the memory used directly.
 *
 * Zero-copy sends: a plain one is SEND_ZC, with IORING_RECVSEND_FIXED_BUF
 * for FIXED_BUFFER and IORING_SEND_VECTORIZED for VECTORED; the vectored
 * fixed-buffer one is SENDMSG_ZC with IORING_RECVSEND_FIXED_BUF and a
 * struct msghdr whose msg_iov is the record's vector array (decision 53:
 * the kernel accepts that combination on SEND_ZC only from 7.x, on
 * SENDMSG_ZC since 6.15). The msghdr is read when the kernel consumes the
 * SQE, so it lives in a table indexed like the SQ ring, exactly as the
 * timespec of TIMEOUT and TIMEOUT_UPDATE does.
 *
 * Kernel details the contract needs and the SQE fields do not give: a
 * DIRECT record naming its slot completes with 0 in the kernel, so the
 * executor remembers (user_data, slot) until the completion and reports
 * the slot; SEND always carries MSG_NOSIGNAL, and SEND and non-selecting
 * RECV carry MSG_WAITALL when LINK is set, so that a short transfer fails
 * the chain as the contract's LINK rule says; a BUFFER_SELECT receive
 * carries IORING_RECVSEND_POLL_FIRST, so the kernel selects its buffer at
 * a delivery and an empty ring is -ENOBUFS then, never at arming
 * (decision 65); GETSOCKOPT at a level other than SOL_SOCKET is refused
 * at translation with -EOPNOTSUPP, the only level the kernel's socket
 * command serves and the contract's rule for both executors (decision
 * 65). A zero-copy record the translation rejects completes exactly once
 * with the error and MORE clear, since the failure NOP has no
 * notification (decision 62).
 *
 * Contract values the executor decides itself, before any kernel call: a
 * record carrying the reserved user_data (below) makes submit_and_wait
 * return -EINVAL before any SQE is written; a second register_files or
 * register_buffers is -EBUSY; buffer_ring on a registered group -EEXIST;
 * unregistering or providing to an unknown group -ENOENT; update_file and
 * update_buffer with a slot or index outside the table, or before the
 * table is registered, -EINVAL. update_buffer of a region a pending
 * record uses is a caller error the ring cannot detect: it returns 0 and
 * the kernel keeps the old registration alive until those records
 * complete (decision 65).
 */

/* One io_uring instance as the kernel shares it. The pointers address the
 * two mappings; see the memory-ordering note above. */
struct vsr_io_uring_ring {
    int fd;               /* The ring descriptor, or -1. */
    int enter_fd;         /* Registered index of fd for enter/register. */
    uint32_t enter_flags; /* IORING_ENTER_REGISTERED_RING once registered. */
    uint32_t setup;       /* IORING_SETUP_* the ring was created with. */
    uint32_t features;    /* IORING_FEAT_* the kernel reported. */
    uint32_t sq_entries;  /* Power of two. */
    uint32_t sq_mask;
    uint32_t sq_local_tail;     /* SQEs filled; published by a flush. */
    _Atomic uint32_t *sq_head;  /* Kernel: SQEs consumed. */
    _Atomic uint32_t *sq_tail;  /* Ours: SQEs published. */
    _Atomic uint32_t *sq_flags; /* Kernel: NEED_WAKEUP, CQ_OVERFLOW, TASKRUN. */
    struct io_uring_sqe *sqes;  /* The SQE mapping. */
    size_t sqes_size;
    uint32_t cq_entries; /* Power of two. */
    uint32_t cq_mask;
    _Atomic uint32_t *cq_head; /* Ours: CQEs consumed. */
    _Atomic uint32_t *cq_tail; /* Kernel: CQEs posted. */
    struct io_uring_cqe *cqes;
    void *rings; /* The mapping of both rings. */
    size_t rings_size;
};

struct vsr_io_uring_region {
    void *base;
    size_t size;
};

struct vsr_io_uring_group {
    void *ring_memory;
    uint32_t entries; /* Power of two. */
    uint32_t flags;
    uint32_t tail; /* Ring tail (16-bit kernel index, kept mod 65536). */
    uint32_t head; /* Kernel head as last read (16-bit). */
    uint16_t group;
    uint16_t registered;
};

/* A pending DIRECT record naming its slot: its completion reports it. */
struct vsr_io_uring_direct {
    uint64_t user_data;
    int32_t slot; /* -1: entry free. */
    uint32_t reserved;
};

struct vsr_io_uring {
    struct vsr_io_uring_options options;
    struct vsr_io_uring_ring ring;
    int wake_fd;         /* eventfd written by wake(). */
    uint64_t wake_reads; /* Wake completions consumed. */
    uint32_t wake_armed; /* Multishot poll on wake_fd outstanding. */
    uint32_t submitted;  /* Records submitted since init (stats). */
    struct vsr_io_uring_region *buffers; /* [options.buffer_regions] */
    struct vsr_io_uring_group *groups;   /* Buffer rings by group id, up to
                                            VSR_IO_URING_GROUPS. */
    uint32_t groups_count;
    int32_t failure; /* First fatal ring error; every call fails after. */
    uint32_t files_registered;   /* Registered file table size; 0 none. */
    uint32_t buffers_registered; /* Registered buffer table size; 0 none. */
    struct vsr_io_uring_direct *directs; /* [directs_capacity] */
    uint32_t directs_capacity;           /* options.cq_entries */
    uint32_t directs_count;
    /* Per-SQE-slot tables the kernel reads when it consumes the SQE, both
     * [table_entries], indexed like the SQ ring: the timespec of a TIMEOUT
     * or TIMEOUT_UPDATE, the msghdr of a vectored fixed-buffer SENDMSG_ZC. */
    struct __kernel_timespec *timespecs;
    struct msghdr *msghdrs;
    uint32_t table_entries; /* SQ ring entries (a power of two). */
    uint32_t reserved;
};

#define VSR_IO_URING_GROUPS 8u
/* user_data reserved for the wake poll; never returned by reap. A record
 * carrying it makes submit_and_wait fail with -EINVAL, submitting none. */
#define VSR_IO_URING_WAKE_USER_DATA UINT64_MAX

/* The ops table installed by vsr_io_uring_init. */
extern const struct vsr_io_executor_ops vsr_io_uring_ops;

/* Translation of one record into the SQE at `sqe`. Returns 0 or a negative
 * errno the executor turns into an immediate failure completion. Reads the
 * registered regions, and may write uring->timespecs or uring->msghdrs (the
 * entry of the SQE's ring index; entry 0 when `sqe` is outside the ring, as
 * in the unit test) and uring->directs. */
int vsr_io_uring_translate(struct vsr_io_uring *uring,
                           const struct vsr_io_sqe *record,
                           struct io_uring_sqe *sqe);
/* Translation of one io_uring CQE into a record. */
void vsr_io_uring_reap_one(const struct io_uring_cqe *cqe,
                           struct vsr_io_cqe *record);

#endif /* VSR_IO_URING_H */
