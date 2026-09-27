#include "config.h"

#include "lib/check.h"
#include "vsr.h"

#include <stdlib.h>
#include <string.h>

/* include/vsr.h, vsr_init: "Returns OK, EINVAL, or ELIMIT; *out=NULL on error
 * when out is valid" and "memory must not overlap options, seed metadata, or
 * out". When the options record lies inside the arena, out is a valid,
 * separate pointer, so the EINVAL return must also clear it. The core rejects
 * the overlap before it writes through out (src/core.c, vsr_init), leaving
 * the caller's stale value in place. Expected to fail until that is fixed. */
int main(void)
{
    const struct vsr_member member = {1, VSR_MEMBER_FULL, 0};
    const struct vsr_membership seed = {0, &member, 1, 0};
    const struct vsr_options options = {
        .cluster = {7, 7},
        .incarnation = {1, 1},
        .replica = 1,
        .seed = &seed,
        .limits =
            {
                .members = 4,
                .operations = 16,
                .input_leases = 32,
                .pending_requests = 8,
                .pending_reads = 4,
                .transfers = 2,
                .log_cache_entries = 16,
                .client_cache_entries = 8,
                .batch_entries = 4,
                .spans_per_blob = 4,
                .work_per_step = 64,
                .command_bytes = 64,
                .result_bytes = 32,
                .manifest_bytes = 64,
                .message_bytes = 256,
                .pinned_payload_bytes = 4096,
            },
        .heartbeat_ns = 100,
        .view_timeout_ns = 1000,
        .retry_ns = 50,
        .transfer_timeout_ns = 5000,
        .start_mode = VSR_START_NEW,
        .durability = VSR_DURABLE,
    };
    struct vsr_layout layout;
    CHECK(vsr_layout(&options, &layout) == VSR_OK);
    unsigned char *memory = aligned_alloc(layout.alignment, layout.size);
    CHECK(memory != NULL);
    struct vsr_options *inside = (struct vsr_options *)(void *)memory;
    *inside = options;
    struct vsr *out = (struct vsr *)(void *)memory;
    CHECK(vsr_init(memory, layout.size, inside, &out) == VSR_EINVAL);
    CHECK(out == NULL);
    free(memory);
    return 0;
}
