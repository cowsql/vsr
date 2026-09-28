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
 * The ring is created with SINGLE_ISSUER | DEFER_TASKRUN, user memory
 * (NO_MMAP) from the caller's region, a registered ring descriptor and the
 * options' SQ/CQ sizes. Sparse file and buffer tables of file_slots and
 * buffer_regions entries are registered at init; buffer rings are
 * registered on buffer_ring() with the caller's page-aligned memory and
 * IOU_PBUF_RING_INC when INCREMENTAL is requested. wake() writes an eventfd
 * that a multishot poll on the ring watches; its completions are consumed
 * internally and never reach reap(). Time is CLOCK_MONOTONIC; random is
 * getrandom(2).
 *
 * Record translation is a table from enum vsr_io_sqe_opcode to the
 * io_uring prep function, with flags mapped 1:1 (LINK -> IOSQE_IO_LINK,
 * FIXED_FILE -> IOSQE_FIXED_FILE, BUFFER_SELECT -> IOSQE_BUFFER_SELECT,
 * SKIP_SUCCESS -> IOSQE_CQE_SKIP_SUCCESS, DIRECT -> the *_direct prep with
 * fd2 as the slot or IORING_FILE_INDEX_ALLOC, FIXED_BUFFER -> the *_fixed
 * prep or IORING_RECVSEND_FIXED_BUF with buffer_index). Completion flags
 * map back the same way (IORING_CQE_F_MORE, F_BUFFER with the buffer id in
 * the upper flag bits, F_BUF_MORE, F_NOTIF). A CQ overflow is drained by
 * submit_and_wait before submitting; the CQ is sized so that it never
 * overflows under the engine's slot count.
 *
 * FIXED_BUFFER validation: the executor keeps every region it was given
 * and rejects, with -EFAULT in the completion, a record whose addr/length
 * (or any vector) is not inside region buffer_index; the kernel would
 * reject it too, but the check makes the failure deterministic and the
 * same as the simulation's.
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
    uint32_t tail; /* Provided entries so far (mod entries). */
    uint16_t group;
    uint16_t registered;
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
};

#define VSR_IO_URING_GROUPS 8u
/* user_data reserved for the wake poll; never returned by reap. */
#define VSR_IO_URING_WAKE_USER_DATA UINT64_MAX

/* The ops table installed by vsr_io_uring_init. */
extern const struct vsr_io_executor_ops vsr_io_uring_ops;

/* Translation of one record into the SQE at `sqe` (a struct io_uring_sqe;
 * void here to keep liburing out of the header). Returns 0 or a negative
 * errno the executor turns into an immediate failure completion. */
int vsr_io_uring_translate(struct vsr_io_uring *uring,
                           const struct vsr_io_sqe *record, void *sqe);
/* Translation of one io_uring CQE into a record. */
void vsr_io_uring_reap_one(const void *cqe, struct vsr_io_cqe *record);

#endif /* VSR_IO_URING_H */
