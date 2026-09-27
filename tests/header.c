/* Compile-only API/layout checks; no library implementation is linked. */
#include "vsr.h"
#include "vsr.h"

#if defined(__cplusplus)
#define CHECK_LAYOUT(test, name) static_assert((test), #name)
#elif defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
#define CHECK_LAYOUT(test, name) _Static_assert((test), #name)
#else
#define CHECK_LAYOUT(test, name) typedef char name[(test) ? 1 : -1]
#endif

#if UINTPTR_MAX == UINT64_MAX
CHECK_LAYOUT(sizeof(struct vsr_event) == 32, event_is_32_bytes);
CHECK_LAYOUT(sizeof(struct vsr_op) == 32, operation_is_32_bytes);
CHECK_LAYOUT(sizeof(struct vsr_update) == 32, update_is_32_bytes);
CHECK_LAYOUT(sizeof(struct vsr_message) == 64, message_is_64_bytes);
CHECK_LAYOUT(sizeof(struct vsr_entry) == 64, entry_is_64_bytes);
CHECK_LAYOUT(sizeof(struct vsr_member) == 16, member_is_16_bytes);
CHECK_LAYOUT(sizeof(struct vsr_client_record) == 64, client_record_is_64_bytes);
CHECK_LAYOUT(offsetof(struct vsr_event, data) == 16, event_body_offset);
CHECK_LAYOUT(offsetof(struct vsr_op, data) == 16, operation_body_offset);
CHECK_LAYOUT(offsetof(struct vsr_message, body) == 56, message_body_offset);
CHECK_LAYOUT(offsetof(struct vsr_entry, body) == 56, entry_body_offset);
#endif

/* Exercise both step signatures with caller-owned contiguous queue storage. */
int vsr_header_compile_check(struct vsr *v, const struct vsr_event *completion)
{
    struct vsr_op operations[8];
    struct vsr_update update = { operations, 8, 0, 0, 0, VSR_NO_DEADLINE };
    struct vsr_event events[2] = {
        { VSR_EVENT_TIME, 0, UINT64_C(1000), NULL, 0 },
        { VSR_EVENT_TIME, 0, UINT64_C(1000), NULL, 0 }
    };
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
    int (*layout_fn)(const struct vsr_options *, struct vsr_layout *) = vsr_layout;
    int (*init_fn)(void *, size_t, const struct vsr_options *, struct vsr **) = vsr_init;
    int (*deinit_fn)(struct vsr *) = vsr_deinit;
    (void)layout_fn;
    (void)init_fn;
    (void)deinit_fn;
}
