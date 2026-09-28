#ifndef VSR_IO_URING_H
#define VSR_IO_URING_H

#include "vsr-io.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/*
 * io_uring executor (docs/io-implementation.md, "Executor: io_uring"): the
 * production implementation of struct vsr_io_executor_ops over one ring,
 * and one of the two modules that touch the kernel. Every operation's
 * semantics are the "Executor contract" of the implementation document;
 * the conformance suite runs the same tests over this executor and over
 * the simulation.
 *
 * The ring is created with SINGLE_ISSUER | DEFER_TASKRUN | COOP_TASKRUN |
 * SUBMIT_ALL, user memory (NO_MMAP) from the caller's region, and the
 * options' SQ/CQ sizes; SQPOLL replaces DEFER_TASKRUN and COOP_TASKRUN,
 * which the kernel refuses to combine with it. REGISTERED_FD_ONLY is not
 * used because it leaves no descriptor for vsr_io_uring_fd; the ring
 * descriptor is registered after setup instead, so io_uring_enter still
 * takes the registered-ring path. The sparse file and buffer tables are
 * registered by register_files() and register_buffers() (once each, as
 * the contract states), at most options.file_slots and
 * options.buffer_regions entries; buffer rings are registered on
 * buffer_ring() with the caller's page-aligned memory and IOU_PBUF_RING_INC
 * when INCREMENTAL is requested. wake() writes an eventfd that a multishot
 * poll on the ring watches; its completions are consumed internally and
 * never reach reap(). Time is CLOCK_MONOTONIC; random is getrandom(2).
 *
 * Record translation is a switch from enum vsr_io_sqe_opcode to the
 * io_uring prep function, with flags mapped 1:1 (LINK -> IOSQE_IO_LINK,
 * FIXED_FILE -> IOSQE_FIXED_FILE, BUFFER_SELECT -> IOSQE_BUFFER_SELECT,
 * SKIP_SUCCESS -> IOSQE_CQE_SKIP_SUCCESS, DIRECT -> the *_direct prep with
 * fd2 as the slot or IORING_FILE_INDEX_ALLOC, FIXED_BUFFER -> the *_fixed
 * prep or IORING_RECVSEND_FIXED_BUF with buffer_index). Completion flags
 * map back the same way (IORING_CQE_F_MORE, F_BUFFER with the buffer id in
 * the upper flag bits, F_BUF_MORE, F_NOTIF). A record the translation
 * rejects becomes a NOP with IORING_NOP_INJECT_RESULT carrying the negative
 * errno and the record's user_data and LINK flag, so the failure is an
 * ordinary completion in submission order and fails its chain.
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
 * Kernel details the contract needs and the prep functions do not give:
 * a DIRECT record naming its slot completes with 0 in the kernel, so the
 * executor remembers (user_data, slot) until the completion and reports
 * the slot; TIMEOUT and TIMEOUT_UPDATE take a timespec the kernel reads
 * when it consumes the SQE, kept in a table indexed like the SQ; SEND
 * always carries MSG_NOSIGNAL, and SEND and non-selecting RECV carry
 * MSG_WAITALL when LINK is set, so that a short transfer fails the chain as
 * the contract's LINK rule says.
 */

struct io_uring; /* liburing; the ring lives inside the executor memory. */

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

/* Layout of struct __kernel_timespec, without the kernel headers. */
struct vsr_io_uring_timespec {
    int64_t tv_sec;
    int64_t tv_nsec;
};

struct vsr_io_uring {
    struct vsr_io_uring_options options;
    struct io_uring *ring; /* Placed in the region after this struct. */
    void *ring_memory;     /* SQ/CQ/SQE arrays (NO_MMAP), page aligned. */
    size_t ring_memory_size;
    int wake_fd;         /* eventfd written by wake(). */
    uint64_t wake_reads; /* Wake completions consumed. */
    uint32_t wake_armed; /* Multishot poll on wake_fd outstanding. */
    uint32_t submitted;  /* Records submitted since init (stats). */
    uint32_t reserved;
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
    struct vsr_io_uring_timespec *timespecs; /* [timespecs_count], indexed
                                                like the SQ ring. */
    uint32_t timespecs_count; /* SQ ring entries (a power of two). */
    uint32_t reserved2;
};

#define VSR_IO_URING_GROUPS 8u
/* user_data reserved for the wake poll; never returned by reap. A record
 * carrying it makes submit_and_wait fail with -EINVAL, submitting none. */
#define VSR_IO_URING_WAKE_USER_DATA UINT64_MAX

/* The ops table installed by vsr_io_uring_init. */
extern const struct vsr_io_executor_ops vsr_io_uring_ops;

/* Translation of one record into the SQE at `sqe` (a struct io_uring_sqe;
 * void here to keep liburing out of the header). Returns 0 or a negative
 * errno the executor turns into an immediate failure completion. Reads the
 * registered regions, and may write uring->timespecs (the entry of the
 * SQE's ring index; entry 0 when `sqe` is outside the ring, as in the unit
 * test) and uring->directs. */
int vsr_io_uring_translate(struct vsr_io_uring *uring,
                           const struct vsr_io_sqe *record, void *sqe);
/* Translation of one io_uring CQE into a record. */
void vsr_io_uring_reap_one(const void *cqe, struct vsr_io_cqe *record);

#endif /* VSR_IO_URING_H */
