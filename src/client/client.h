#ifndef VSR_CLIENT_INTERNAL_H
#define VSR_CLIENT_INTERNAL_H

#include "vsr-client.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/*
 * Client bookkeeping internals (docs/io-implementation.md, "Client"). The
 * public contract is vsr-client.h; this header fixes the arena layout and
 * the export image. One module, src/client/client.c, implements both.
 *
 * Arena: struct vsr_client, then lanes[options.lanes], then a membership
 * copy with members[options.members] (and a second one for a membership
 * being adopted before it replaces the current one). Nothing else.
 */

struct vsr_client_lane {
    struct vsr_id incarnation;
    uint64_t next_number;
    uint64_t pending_number; /* 0 when no request is pending. */
    uint64_t deadline_ns;    /* Attempt or backoff deadline. */
    uint64_t replica;        /* Target of the current attempt. */
    uint64_t routing_epoch;  /* Epoch used by the current attempt. */
    uint32_t state;          /* enum vsr_client_lane_state */
    uint32_t type;           /* enum vsr_request_type of the pending one. */
    uint32_t attempts;
    uint32_t busy_streak;
    uint32_t rotation; /* Next member index to try without a primary. */
    uint32_t reserved;
    const void *body; /* Caller-owned pending body; NULL when DETACHED. */
};

struct vsr_client {
    struct vsr_client_options options;
    struct vsr_client_lane *lanes;
    struct vsr_membership membership; /* Current topology; count 0 none. */
    struct vsr_member *members;       /* [options.members] */
    struct vsr_membership adopting;   /* Scratch for a reply's membership. */
    struct vsr_member *adopting_members;
    uint64_t epoch;   /* Routing epoch for new requests. */
    uint64_t primary; /* VSR_NO_REPLICA when unknown. */
    uint64_t min_op;
    uint32_t open;          /* Open lanes. */
    uint32_t busy;          /* Lanes not IDLE. */
    uint32_t causal_cursor; /* Round-robin index for CAUSAL reads. */
    uint32_t reserved;
};

/*
 * Export image (vsr_client_export), little-endian, versioned:
 *   vsr_client_image_header
 *   lanes[header.lanes] of vsr_client_image_lane, open lanes only
 *   members[header.members] of {u64 id; u32 role; u32 reserved}
 * followed by a trailing uint32_t CRC32C over everything before it. Import
 * rejects a bad magic, version, CRC or counts above the client's
 * capacities.
 */
#define VSR_CLIENT_IMAGE_MAGIC UINT32_C(0x31494C43) /* "CLI1" */

struct vsr_client_image_header {
    uint32_t magic;
    uint32_t version; /* VSR_CLIENT_STATE_VERSION */
    uint32_t lanes;
    uint32_t members;
    uint64_t epoch;
    uint64_t primary;
    uint64_t min_op;
    uint64_t membership_epoch;
    uint32_t faults;
    uint32_t reserved;
};

struct vsr_client_image_lane {
    uint64_t incarnation_hi;
    uint64_t incarnation_lo;
    uint64_t next_number;
    uint64_t pending_number;
    uint32_t type;
    uint32_t reserved;
};

#endif /* VSR_CLIENT_INTERNAL_H */
