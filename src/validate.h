#ifndef VSR_VALIDATE_H
#define VSR_VALIDATE_H

#include "vsr.h"

/* A fresh context is used for each input graph. The limits are already valid.
 * arena may be NULL only when arena_size is zero. Graphs may share descriptors
 * and bytes; each blob occurrence contributes its full logical payload size.
 * No function reads payload bytes or retains input pointers. payload_bytes is
 * valid on success; callers discard it after failure. Accessibility and graph
 * acyclicity remain caller preconditions, as specified by the public API. */
struct vsr_validation {
    const struct vsr_limits *limits;
    const void *arena;
    size_t arena_size;
    uint64_t payload_bytes;
    /* Successful, structurally valid completions can contradict their issued
     * operation or the storage model. Consume them and latch this failure before
     * invoking protocol completion handling. Zero means no such inconsistency. */
    uint32_t failure_code;
};

int vsr_validate_options(const struct vsr_options *options);
int vsr_validate_membership(struct vsr_validation *validation,
                            const struct vsr_membership *membership);
int vsr_validate_epoch(struct vsr_validation *validation,
                       const struct vsr_epoch *epoch);
int vsr_validate_blob(struct vsr_validation *validation,
                      const struct vsr_blob *blob, uint64_t limit);
int vsr_validate_request(struct vsr_validation *validation,
                         const struct vsr_request *request);
int vsr_validate_entry(struct vsr_validation *validation,
                       const struct vsr_entry *entry);
int vsr_validate_entries(struct vsr_validation *validation,
                         const struct vsr_entries *entries);
int vsr_validate_checkpoint(struct vsr_validation *validation,
                            const struct vsr_checkpoint *checkpoint);
int vsr_validate_message(struct vsr_validation *validation,
                         const struct vsr_message *message);
int vsr_validate_recovered(struct vsr_validation *validation,
                           const struct vsr_recovered *recovered);

/* completion is the matching outstanding operation for COMPLETE, NULL for all
 * other events. IDs, lease uniqueness, time ordering, and protocol applicability
 * are checked by the runtime. A complete walk checks all reachable typed data,
 * alignment, arena exclusion, reserved fields, shape and configured limits.
 * LOAD revision/client/range mismatch, APPLY cardinality/control-result mismatch,
 * and snapshot identity/boundary mismatch set failure_code and return OK after
 * validating the entire graph. Returned counts/bytes above an issued maximum,
 * invalid numeric domains, reserved fields or pointers remain API errors. */
int vsr_validate_event(struct vsr_validation *validation,
                       const struct vsr_event *event,
                       const struct vsr_op *completion);

#endif /* VSR_VALIDATE_H */
