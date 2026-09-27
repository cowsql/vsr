#ifndef VSR_OBJECTS_H
#define VSR_OBJECTS_H

#include "vsr.h"

#include <stdbool.h>
#include <stdint.h>

/* Logical comparisons of already validated objects. Padding, pointer values,
 * span boundaries, and routing epochs are never request identity. */
static inline bool vsr_id_equal(struct vsr_id a, struct vsr_id b)
{
    return a.hi == b.hi && a.lo == b.lo;
}

static inline bool vsr_id_present(struct vsr_id id)
{
    return id.hi != 0 || id.lo != 0;
}

static inline bool vsr_request_id_equal(struct vsr_request_id a,
                                        struct vsr_request_id b)
{
    return vsr_id_equal(a.client, b.client) && a.number == b.number;
}

static inline bool vsr_nonce_equal(struct vsr_nonce a, struct vsr_nonce b)
{
    return vsr_id_equal(a.incarnation, b.incarnation) && a.counter == b.counter;
}

static inline bool vsr_revision_equal(struct vsr_revision a,
                                      struct vsr_revision b)
{
    return vsr_id_equal(a.incarnation, b.incarnation) &&
           a.sequence == b.sequence;
}

bool vsr_blob_equal(const struct vsr_blob *a, const struct vsr_blob *b);
bool vsr_membership_equal(const struct vsr_membership *a,
                          const struct vsr_membership *b);
bool vsr_epoch_equal(const struct vsr_epoch *a, const struct vsr_epoch *b);
bool vsr_request_equal(const struct vsr_request *a,
                       const struct vsr_request *b);
bool vsr_entry_equal(const struct vsr_entry *a, const struct vsr_entry *b);
bool vsr_value_equal(const struct vsr_value *a, const struct vsr_value *b);

/* UINT32_MAX denotes absence; IDs are searched in canonical sorted order. */
uint32_t vsr_member_index(const struct vsr_membership *membership, uint64_t id);
uint64_t vsr_primary(const struct vsr_membership *membership, uint64_t view);

#endif
