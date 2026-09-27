#include "config.h"

#include "objects.h"

#include <stddef.h>
#include <string.h>

bool vsr_blob_equal(const struct vsr_blob *a, const struct vsr_blob *b)
{
    uint32_t ai = 0;
    uint32_t bi = 0;
    size_t ao = 0;
    size_t bo = 0;

    if (a->size != b->size) {
        return false;
    }
    while (ai < a->count && bi < b->count) {
        const struct vsr_span *as = &a->spans[ai];
        const struct vsr_span *bs = &b->spans[bi];
        size_t ar = as->size - ao;
        size_t br = bs->size - bo;
        size_t n = ar < br ? ar : br;
        const unsigned char *ap = as->data;
        const unsigned char *bp = bs->data;

        if (memcmp(ap + ao, bp + bo, n) != 0) {
            return false;
        }
        ao += n;
        bo += n;
        if (ao == as->size) {
            ++ai;
            ao = 0;
        }
        if (bo == bs->size) {
            ++bi;
            bo = 0;
        }
    }
    return ai == a->count && bi == b->count;
}

bool vsr_membership_equal(const struct vsr_membership *a,
                          const struct vsr_membership *b)
{
    uint32_t i;
    if (a == NULL || b == NULL) {
        return a == b;
    }
    if (a->epoch != b->epoch || a->count != b->count ||
        a->faults != b->faults) {
        return false;
    }
    for (i = 0; i < a->count; ++i) {
        if (a->members[i].id != b->members[i].id ||
            a->members[i].role != b->members[i].role) {
            return false;
        }
    }
    return true;
}

bool vsr_epoch_equal(const struct vsr_epoch *a, const struct vsr_epoch *b)
{
    if (a == NULL || b == NULL) {
        return a == b;
    }
    return a->boundary == b->boundary && a->phase == b->phase &&
           vsr_membership_equal(a->current, b->current) &&
           vsr_membership_equal(a->previous, b->previous);
}

static bool request_body_equal(uint32_t type, const void *a, const void *b)
{
    switch (type) {
    case VSR_REQUEST_COMMAND:
        return vsr_blob_equal(a, b);
    case VSR_REQUEST_RECONFIGURE:
        return vsr_membership_equal(a, b);
    case VSR_REQUEST_CHECK_EPOCH: {
        const struct vsr_check_epoch *ca = a;
        const struct vsr_check_epoch *cb = b;
        return ca->epoch == cb->epoch;
    }
    case VSR_REQUEST_NOOP:
        return true;
    default:
        return false;
    }
}

bool vsr_request_equal(const struct vsr_request *a, const struct vsr_request *b)
{
    return a->type == b->type && vsr_request_id_equal(a->id, b->id) &&
           request_body_equal(a->type, a->body, b->body);
}

bool vsr_entry_equal(const struct vsr_entry *a, const struct vsr_entry *b)
{
    return a->op == b->op && a->epoch == b->epoch && a->view == b->view &&
           a->type == b->type && vsr_request_id_equal(a->request, b->request) &&
           request_body_equal(a->type, a->body, b->body);
}

bool vsr_value_equal(const struct vsr_value *a, const struct vsr_value *b)
{
    return a->code == b->code && vsr_blob_equal(&a->data, &b->data);
}

uint32_t vsr_member_index(const struct vsr_membership *membership, uint64_t id)
{
    uint32_t lo = 0;
    uint32_t hi;
    if (membership == NULL || id == VSR_NO_REPLICA) {
        return UINT32_MAX;
    }
    hi = membership->count;
    while (lo < hi) {
        uint32_t mid = lo + (hi - lo) / 2;
        uint64_t found = membership->members[mid].id;
        if (found == id) {
            return mid;
        }
        if (found < id) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return UINT32_MAX;
}

uint64_t vsr_primary(const struct vsr_membership *membership, uint64_t view)
{
    uint32_t count = 0;
    uint32_t i;
    uint64_t position;

    if (membership == NULL) {
        return VSR_NO_REPLICA;
    }
    for (i = 0; i < membership->count; ++i) {
        if (membership->members[i].role == VSR_MEMBER_FULL) {
            ++count;
        }
    }
    if (count == 0) {
        return VSR_NO_REPLICA;
    }
    position = view % count;
    for (i = 0; i < membership->count; ++i) {
        if (membership->members[i].role == VSR_MEMBER_FULL) {
            if (position == 0) {
                return membership->members[i].id;
            }
            --position;
        }
    }
    return VSR_NO_REPLICA;
}
