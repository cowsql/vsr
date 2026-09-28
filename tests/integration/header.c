/* Compile-only API/layout checks; no library implementation is linked. */
#include "vsr.h"

/* Verify that the public header can be included more than once. */
#include "vsr.h"

/* The I/O layer's public contracts and every private header of its modules
 * compile under the same strict flags; the headers are the interface
 * specification of docs/io-implementation.md. */
#include "vsr-client.h"
#include "vsr-io.h"
#include "vsr-sim.h"

#include "client/client.h"
#include "io/codec.h"
#include "io/crc32c.h"
#include "io/cursor.h"
#include "io/deadline.h"
#include "io/engine.h"
#include "io/link.h"
#include "io/pool.h"
#include "io/slots.h"
#include "io/snapshot.h"
#include "io/store.h"
#include "io/stream.h"
#include "io/uring.h"
#include "io/wire.h"
#include "sim/sim.h"

#define CHECK_LAYOUT(test, name) _Static_assert((test), #name)

#if UINTPTR_MAX == UINT64_MAX
CHECK_LAYOUT(sizeof(struct vsr_event) == 32, event_is_32_bytes);
CHECK_LAYOUT(sizeof(struct vsr_op) == 32, operation_is_32_bytes);
CHECK_LAYOUT(sizeof(struct vsr_update) == 32, update_is_32_bytes);
CHECK_LAYOUT(sizeof(struct vsr_message) == 64, message_is_64_bytes);
CHECK_LAYOUT(sizeof(struct vsr_entry) == 64, entry_is_64_bytes);
CHECK_LAYOUT(sizeof(struct vsr_member) == 16, member_is_16_bytes);
CHECK_LAYOUT(sizeof(struct vsr_client_record) == 64, client_record_is_64_bytes);
CHECK_LAYOUT(sizeof(struct vsr_read_barrier) == 24, read_barrier_is_24_bytes);
CHECK_LAYOUT(sizeof(struct vsr_failure) == 24, failure_is_24_bytes);
CHECK_LAYOUT(sizeof(struct vsr_check_epoch) == 8, epoch_target_is_8_bytes);
CHECK_LAYOUT(offsetof(struct vsr_event, data) == 16, event_body_offset);
CHECK_LAYOUT(offsetof(struct vsr_op, data) == 16, operation_body_offset);
CHECK_LAYOUT(offsetof(struct vsr_message, body) == 56, message_body_offset);
CHECK_LAYOUT(offsetof(struct vsr_entry, body) == 56, entry_body_offset);
/* vsr-io.h documents these record and wrapper sizes. */
CHECK_LAYOUT(sizeof(struct vsr_io_sqe) == 64, sqe_is_64_bytes);
CHECK_LAYOUT(sizeof(struct vsr_io_cqe) == 16, cqe_is_16_bytes);
CHECK_LAYOUT(sizeof(struct vsr_io_op) == 48, io_op_is_48_bytes);
CHECK_LAYOUT(sizeof(struct vsr_io_event) == 48, io_event_is_48_bytes);
CHECK_LAYOUT(sizeof(struct vsr_io_vec) == 16, vec_is_16_bytes);
CHECK_LAYOUT(sizeof(struct vsr_io_buffer) == 16, buffer_is_16_bytes);
#endif

/* Exercise both step signatures with caller-owned contiguous queue storage. */
int vsr_header_compile_check(struct vsr *v, const struct vsr_event *completion);
void vsr_header_lifecycle_signatures(void);
int vsr_header_payload_check(const struct vsr *v,
                             const struct vsr_membership *membership,
                             uint64_t operation_id, uint64_t lease);

int vsr_header_compile_check(struct vsr *v, const struct vsr_event *completion)
{
    struct vsr_op operations[8];
    struct vsr_update update = {operations, 8, 0, 0, 0, VSR_NO_DEADLINE};
    struct vsr_event events[2] = {{VSR_EVENT_TIME, 0, UINT64_C(1000), NULL, 0},
                                  {VSR_EVENT_TIME, 0, UINT64_C(1000), NULL, 0}};
    struct vsr_status status;
    int result;

    if (completion != NULL)
        events[1] = *completion;
    result = vsr_step_many(v, events, 2, &update);
    vsr_get_status(v, &status);
    /* A real driver dispatches update.ops before draining or reusing them. */
    if (update.count == 0 && update.consumed == 2 && result == VSR_OK)
        result = vsr_step(v, NULL, &update);
    return result;
}

/* Check less frequent lifecycle signatures without constructing fake state. */
void vsr_header_lifecycle_signatures(void)
{
    int (*layout_fn)(const struct vsr_options *, struct vsr_layout *) =
        vsr_layout;
    int (*init_fn)(void *, size_t, const struct vsr_options *, struct vsr **) =
        vsr_init;
    int (*deinit_fn)(struct vsr *) = vsr_deinit;
    (void)layout_fn;
    (void)init_fn;
    (void)deinit_fn;
}

/* Exercise cold-path payloads as a C adapter would construct them. */
int vsr_header_payload_check(const struct vsr *v,
                             const struct vsr_membership *membership,
                             uint64_t operation_id, uint64_t lease)
{
    const struct vsr_epoch epoch = {membership, NULL, 0, VSR_EPOCH_STEADY, 0};
    const struct vsr_store_identity identity = {
        {UINT64_C(1), UINT64_C(2)}, UINT64_C(7), VSR_DURABLE, 0};
    const struct vsr_hard_state hard = {
        0, 0, 0, &epoch, VSR_HARD_NORMAL, VSR_MEMBER_FULL};
    const struct vsr_recovered recovered = {
        identity, UINT64_C(1), UINT64_C(1), UINT64_C(1), hard, NULL};
    const struct vsr_loaded loaded = {&recovered, UINT64_C(1), 0, 1, 0};
    const struct vsr_event completion = {VSR_EVENT_COMPLETE, VSR_IO_OK,
                                         operation_id, &loaded, lease};
    const struct vsr_read_barrier read = {0, VSR_NO_DEADLINE,
                                          VSR_READ_LINEARIZABLE, 0};
    const struct vsr_change changes[] = {{VSR_STORE_IDENTITY, 1, 0, &identity},
                                         {VSR_STORE_HARD_STATE, 1, 0, &hard}};
    const struct vsr_store transaction = {UINT64_C(1), changes, 2, 0};
    const struct vsr_log_state offer = {
        {{UINT64_C(3), UINT64_C(4)}, UINT64_C(1)},
        0,
        0,
        0,
        UINT64_C(1),
        UINT64_C(1),
        &epoch,
        {NULL, 0, 0},
        NULL};
    const struct vsr_state_chunk chunk = {
        {{UINT64_C(5), UINT64_C(6)}, UINT64_C(1)}, offer, 0, 0};
    const struct vsr_message message = {
        {UINT64_C(1), UINT64_C(2)}, 0, 0, UINT64_C(7),
        VSR_MSG_NEW_STATE,          0, 0, &chunk};
    /* Routing can advance without changing the logged handoff target. */
    const struct vsr_check_epoch target = {UINT64_C(1)};
    const struct vsr_request check_epoch = {
        {{UINT64_C(9), UINT64_C(10)}, UINT64_C(2)},
        UINT64_C(2),
        VSR_REQUEST_CHECK_EPOCH,
        0,
        &target};
    const struct vsr_entry epoch_entry = {
        UINT64_C(12),
        UINT64_C(2),
        0,
        {{UINT64_C(9), UINT64_C(10)}, UINT64_C(2)},
        VSR_REQUEST_CHECK_EPOCH,
        0,
        &target};
    struct vsr_status status;
    (void)check_epoch;
    (void)epoch_entry;
    (void)completion;
    (void)read;
    (void)transaction;
    (void)message;
    /* These stack payloads are compile-only; submitting them would require
     * keeping them alive until their leases are explicitly released. */
    vsr_get_status(v, &status);
    return status.failure.code == VSR_FAILURE_NONE;
}
