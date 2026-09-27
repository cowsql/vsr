#include "config.h"

#include "lib/check.h"
#include "objects.h"

#include <stddef.h>
#include <stdint.h>

int main(void)
{
    const struct vsr_span a_spans[] = {{"ab", 2}, {"cdef", 4}};
    const struct vsr_span b_spans[] = {{"abc", 3}, {"d", 1}, {"ef", 2}};
    const struct vsr_span c_spans[] = {{"abcdeg", 6}};
    const struct vsr_blob a = {a_spans, 6, 2, 0};
    const struct vsr_blob b = {b_spans, 6, 3, 0};
    const struct vsr_blob c = {c_spans, 6, 1, 0};
    const struct vsr_blob empty = {NULL, 0, 0, 0};
    const struct vsr_member members[] = {{2, VSR_MEMBER_FULL, 0},
                                         {4, VSR_MEMBER_WITNESS, 0},
                                         {9, VSR_MEMBER_FULL, 0}};
    const struct vsr_membership membership = {0, members, 3, 1};
    struct vsr_membership changed = membership;
    struct vsr_request left = {{{1, 2}, 3}, 0, VSR_REQUEST_COMMAND, 0, &a};
    struct vsr_request right = left;
    struct vsr_check_epoch target_a = {1};
    struct vsr_check_epoch target_b = {2};

    CHECK(vsr_blob_equal(&a, &b));
    CHECK(!vsr_blob_equal(&a, &c));
    CHECK(!vsr_blob_equal(&a, &empty));
    CHECK(vsr_blob_equal(&empty, &empty));
    right.epoch = 10;
    right.body = &b;
    CHECK(vsr_request_equal(&left, &right));
    right.id.number = 4;
    CHECK(!vsr_request_equal(&left, &right));
    right.id = left.id;
    left.type = VSR_REQUEST_CHECK_EPOCH;
    right.type = VSR_REQUEST_CHECK_EPOCH;
    left.body = &target_a;
    right.body = &target_b;
    CHECK(!vsr_request_equal(&left, &right));
    target_b.epoch = 1;
    CHECK(vsr_request_equal(&left, &right));

    CHECK(vsr_membership_equal(&membership, &changed));
    changed.epoch = 1;
    CHECK(!vsr_membership_equal(&membership, &changed));
    CHECK(vsr_member_index(&membership, 2) == 0);
    CHECK(vsr_member_index(&membership, 4) == 1);
    CHECK(vsr_member_index(&membership, 9) == 2);
    CHECK(vsr_member_index(&membership, 3) == UINT32_MAX);
    CHECK(vsr_member_index(&membership, 10) == UINT32_MAX);
    CHECK(vsr_primary(&membership, 0) == 2);
    CHECK(vsr_primary(&membership, 1) == 9);
    CHECK(vsr_primary(&membership, 2) == 2);
    CHECK(vsr_primary(&membership, UINT64_MAX - 1) == 2);
    return 0;
}
