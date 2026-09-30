#ifndef VSR_TEST_PURE_EXECUTOR_H
#define VSR_TEST_PURE_EXECUTOR_H

#include "vsr-io.h"

#include <stdbool.h>
#include <stdint.h>

/*
 * Purity guard (docs/io-design.md decision E7): an executor wrapper that
 * forwards every call to an inner executor and counts the calls made while
 * it is armed. A harness arms it around each call to the engine's four
 * primitives (vsr_io_complete, vsr_io_poll, vsr_io_submit, vsr_io_prepare)
 * and, where it can, around other calls that must not reach the executor;
 * any executor call then is a violation: counted, remembered by name and,
 * with abort_on_violation, fatal with a diagnostic naming the call.
 * Executor calls are allowed only in vsr_io_init, vsr_io_deinit,
 * vsr_io_attach and vsr_io_detach (setup and teardown), vsr_io_wake and
 * the loop itself (vsr_io_run, or the caller's own loop), which the
 * harness leaves unarmed. Arming nests. The wrapper holds no other state
 * and can wrap any executor, the simulation's or a ring.
 */

struct pure_executor {
    struct vsr_io_executor inner;
    uint32_t armed;          /* Depth of the arms in force. */
    bool abort_on_violation; /* Abort at the first violation. */
    uint64_t calls;          /* Every call forwarded. */
    uint64_t violations;     /* Calls made while armed. */
    const char *last;        /* The operation of the last violation. */
};

/* Wraps inner; the returned executor goes to the engine. */
struct vsr_io_executor pure_executor_init(struct pure_executor *pure,
                                          struct vsr_io_executor inner,
                                          bool abort_on_violation);
void pure_executor_arm(struct pure_executor *pure);
void pure_executor_disarm(struct pure_executor *pure);

#endif /* VSR_TEST_PURE_EXECUTOR_H */
