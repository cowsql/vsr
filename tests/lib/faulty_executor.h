#ifndef VSR_TEST_FAULTY_EXECUTOR_H
#define VSR_TEST_FAULTY_EXECUTOR_H

#include "lib/random.h"
#include "vsr-io.h"

#include <stdint.h>

/*
 * Seeded fault-injecting wrapper executor (docs/io-implementation.md,
 * section 9). It wraps any vsr_io_executor, forwards every record to it and,
 * from one seeded generator, injects faults the executor contract permits:
 *
 * - eio_ppm: a READ, WRITE, READV, WRITEV, FSYNC or FILES_UPDATE
 *   completion with a non-negative result completes -EIO instead (a
 *   PROVIDE record, which completes only when it fails, passes through
 *   untouched). The operation itself ran: a FILES_UPDATE's slots hold
 *   their files, a WRITE's bytes are in the file (durable only as far as
 *   a later flush makes them), a READ's buffer holds the file's bytes, an
 *   FSYNC flushed.
 *   As with a real device, a caller may assume nothing about what a
 *   failed write left in the file: the bytes may be there, so recovery
 *   may find the record intact, and nothing the failed FSYNC covered may
 *   be taken as durable.
 * - short_ppm: a positive READ or WRITE result becomes a smaller positive
 *   count in whole units of the largest power of two dividing the
 *   request's offset and length, so the rest stays as aligned as the
 *   request was (an O_DIRECT or whole-block transfer is shortened by whole
 *   blocks, and one of a single unit is never shortened); the bytes past
 *   the count were transferred all the same. A plain SEND (not zero-copy,
 *   not vectored) or a plain RECV (no BUFFER_SELECT, not multishot) of
 *   length > 1 is submitted with a shorter length, so the inner executor
 *   returns a genuinely short count. Stream records are shortened at
 *   submission because rewriting their results would lose received bytes
 *   or resend sent ones; file records are shortened at completion because
 *   the caller repeats the rest at an explicit offset. Streams are assumed
 *   to be stream sockets (a shortened datagram would be truncated), and a
 *   shortened record with SKIP_SUCCESS completes silently, as a short one
 *   of the kernel's would: callers must not combine SKIP_SUCCESS with
 *   SEND or RECV.
 * - delay_ppm: a completion is held for 1..delay_reaps_max reaps.
 * - cancel_recv_ppm: a RECV record (multishot or not) is followed in the
 *   same inner batch by a CANCEL record targeting its user_data, so it
 *   completes -ECANCELED unless it completes first; its queued bytes stay
 *   queued. A caller's own CANCEL of it may then find nothing (-ENOENT).
 *
 * It never alters bytes, user_data or completion flags, and never reorders
 * the completions the caller relies on the order of: those of one user_data
 * (the NOTIF of a zero-copy send stays behind its result, a multishot
 * record's terminal completion stays last), those naming one provided
 * buffer, whose position in an incremental buffer follows from their order
 * across every receive on the ring, and those of LINK chains. It never
 * faults or delays records of a LINK chain, whose success rule a rewritten
 * or shortened result would break (a chained record that outlives the map
 * below may be delayed). Result faults need the record's opcode, which a
 * map filled at submission provides; a completion whose record the map no
 * longer holds is only ever delayed. With every rate zero the wrapper is
 * transparent (the conformance suite runs through it over both executors)
 * but for the value it reserves.
 * Completions it holds that are due count toward submit_and_wait's `want`
 * and end a batching window as the inner's own would; while it holds a
 * delayed one, submit_and_wait does not block (the caller sees an early
 * return, as after a wake), so the caller keeps reaping until it is
 * delivered, at most delay_reaps_max reaps later.
 *
 * user_data FAULTY_EXECUTOR_USER_DATA is reserved for the injected CANCEL
 * records, whose completions the wrapper consumes; submit_and_wait refuses
 * a caller record carrying it with -EINVAL, a narrowing of the contract,
 * which reserves UINT64_MAX alone. user_data of records in flight should
 * be unique, as the engine's are. All state is inside the struct (fixed
 * capacities below, no allocation); it is large, so place it in static or
 * heap storage. There is no deinit: completions still held when the inner
 * executor goes are dropped with it, and faulty_executor_init starts over
 * on a new one. Determinism: the generator is seeded once at init and
 * drawn in a fixed order, per submitted record then per completion, so a
 * seed repeats over a deterministic inner executor.
 */

#define FAULTY_EXECUTOR_USER_DATA (UINT64_MAX - 1)
/* Largest batch that can carry injected records; a larger batch is
 * forwarded as is, with no submission-side faults. */
#define FAULTY_EXECUTOR_BATCH 256u
/* Completions the wrapper can hold; beyond, the rest stay in the inner. */
#define FAULTY_EXECUTOR_HELD 1024u
/* Ring of the most recently submitted records (user_data to opcode). */
#define FAULTY_EXECUTOR_RECORDS 4096u

struct faulty_executor_options {
    uint64_t seed; /* Used by init only. */
    uint32_t eio_ppm;
    uint32_t short_ppm;
    uint32_t delay_ppm;
    uint32_t delay_reaps_max; /* 0 counts as 1. */
    uint32_t cancel_recv_ppm;
};

struct faulty_executor_stats {
    uint64_t eio;       /* Results rewritten to -EIO. */
    uint64_t shortened; /* Records or results shortened. */
    uint64_t delayed;   /* Completions held. */
    uint64_t cancelled; /* Receives an injected CANCEL cancelled. */
};

struct faulty_executor_record {
    uint64_t user_data;
    uint8_t opcode;
    uint8_t flags;       /* Private. */
    uint8_t align_shift; /* READ, WRITE: log2 of the request's alignment. */
    uint8_t reserved8;
    uint32_t reserved;
};

struct faulty_executor_held {
    struct vsr_io_cqe cqe;
    uint32_t reaps;   /* Reaps left before it may be delivered. */
    uint32_t chained; /* Of a LINK chain's record. */
};

struct faulty_executor {
    struct vsr_io_executor inner;
    struct faulty_executor_options options;
    struct faulty_executor_stats stats;
    struct test_random random;
    uint32_t held_count;
    uint32_t record_next;
    struct faulty_executor_held held[FAULTY_EXECUTOR_HELD];
    struct faulty_executor_record records[FAULTY_EXECUTOR_RECORDS];
    struct vsr_io_sqe batch[FAULTY_EXECUTOR_BATCH];
};

void faulty_executor_init(struct faulty_executor *faulty,
                          const struct vsr_io_executor *inner,
                          const struct faulty_executor_options *options);
/* The wrapper as an executor; valid while `faulty` is. */
struct vsr_io_executor faulty_executor_handle(struct faulty_executor *faulty);
/* Replaces the rates from the next record or completion on; the seed is
 * ignored (the generator is not reseeded). */
void faulty_executor_set_options(struct faulty_executor *faulty,
                                 const struct faulty_executor_options *options);
void faulty_executor_stats(const struct faulty_executor *faulty,
                           struct faulty_executor_stats *stats);

#endif /* VSR_TEST_FAULTY_EXECUTOR_H */
