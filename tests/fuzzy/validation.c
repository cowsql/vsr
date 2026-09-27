#include "config.h"

#include "validate.h"

#include <stddef.h>
#include <stdint.h>

struct input {
    const uint8_t *bytes;
    size_t size;
    size_t offset;
};

static uint32_t next(struct input *input)
{
    if (input->offset == input->size) {
        return 0;
    }
    return input->bytes[input->offset++];
}

/* Fuzz logical graph contents, never manufacture inaccessible C pointers.
 * Counts larger than the backing arrays also exceed configured limits, so a
 * correct validator rejects them before traversing those arrays. */
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);
int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    static const uint8_t payload[64] = {0};
    const struct vsr_limits limits = {5, 8, 16, 8,   8,   8,   8,    8,
                                      4, 4, 64, 256, 256, 256, 1024, 4096};
    struct input input = {data, size, 0};
    struct vsr_span spans[4];
    struct vsr_blob blob = {0};
    struct vsr_member members[5];
    struct vsr_membership current = {0};
    struct vsr_membership previous = {0};
    struct vsr_epoch epoch = {0};
    struct vsr_check_epoch check = {next(&input)};
    struct vsr_entry entries[4] = {0};
    struct vsr_entries batch = {entries, next(&input) % 6, 0};
    struct vsr_request request = {0};
    struct vsr_checkpoint checkpoint = {0};
    struct vsr_log_state state = {0};
    struct vsr_prepare prepare = {0};
    struct vsr_recovery recovery = {0};
    struct vsr_fetch fetch = {0};
    struct vsr_state_chunk chunk = {0};
    struct vsr_message message = {0};
    struct vsr_nonce nonce = {{next(&input), next(&input)}, next(&input)};

    for (uint32_t i = 0; i < 4; ++i) {
        spans[i].data = next(&input) % 8 == 0 ? NULL : payload;
        spans[i].size = next(&input) % 65;
    }
    blob.count = next(&input) % 6;
    blob.spans = next(&input) % 8 == 0 ? NULL : spans;
    for (uint32_t i = 0; i < blob.count && i < 4; ++i) {
        blob.size += spans[i].size;
    }
    if (next(&input) % 4 == 0) {
        blob.size = next(&input);
    }
    blob.reserved = next(&input) % 8 == 0 ? 1u : 0u;
    for (uint32_t i = 0; i < 5; ++i) {
        members[i] =
            (struct vsr_member){next(&input) % 16, next(&input) % 4, 0};
    }
    current.epoch = next(&input) % 4;
    current.count = next(&input) % 7;
    current.faults = next(&input) % 4;
    current.members = next(&input) % 8 == 0 ? NULL : members;
    previous = current;
    previous.epoch = current.epoch == 0 ? 0 : current.epoch - 1;
    epoch.current = &current;
    epoch.previous = next(&input) % 2 == 0 ? NULL : &previous;
    epoch.boundary = next(&input) % 16;
    epoch.phase = next(&input) % 5;
    for (uint32_t i = 0; i < 4; ++i) {
        entries[i].op = next(&input) % 32;
        entries[i].epoch = next(&input) % 4;
        entries[i].view = next(&input) % 8;
        entries[i].request.client = (struct vsr_id){next(&input), next(&input)};
        entries[i].request.number = next(&input) % 8;
        entries[i].type = next(&input) % 6;
        switch (entries[i].type) {
        case VSR_REQUEST_COMMAND:
            entries[i].body = &blob;
            break;
        case VSR_REQUEST_RECONFIGURE:
            entries[i].body = &current;
            break;
        case VSR_REQUEST_CHECK_EPOCH:
            entries[i].body = &check;
            break;
        default:
            entries[i].body = NULL;
            break;
        }
    }
    if (batch.count == 0 || next(&input) % 8 == 0) {
        batch.entries = NULL;
    }
    request = (struct vsr_request){entries[0].request, entries[0].epoch,
                                   entries[0].type, 0, entries[0].body};
    checkpoint = (struct vsr_checkpoint){{next(&input), next(&input)},
                                         next(&input) % 32,
                                         next(&input) % 8,
                                         &epoch,
                                         blob};
    state.revision =
        (struct vsr_revision){{next(&input), next(&input)}, next(&input)};
    state.view = next(&input) % 8;
    state.last_normal_view = next(&input) % 8;
    state.committed = next(&input) % 32;
    state.log_begin = next(&input) % 32;
    state.log_end = next(&input) % 32;
    state.epoch = &epoch;
    state.entries = batch;
    state.checkpoint = next(&input) % 2 == 0 ? NULL : &checkpoint;
    prepare = (struct vsr_prepare){batch, next(&input) % 32};
    recovery =
        (struct vsr_recovery){nonce, next(&input) % 2 == 0 ? NULL : &state};
    fetch = (struct vsr_fetch){nonce,
                               state.revision,
                               next(&input) % 32,
                               next(&input) % 32,
                               next(&input) * 8u,
                               next(&input) % 6,
                               0};
    chunk = (struct vsr_state_chunk){nonce, state, next(&input) % 32,
                                     next(&input) % 32};
    message = (struct vsr_message){{next(&input), next(&input)},
                                   next(&input) % 4,
                                   next(&input) % 8,
                                   next(&input) % 16,
                                   next(&input) % 23,
                                   0,
                                   next(&input) % 32,
                                   NULL};
    switch (message.type) {
    case VSR_MSG_PREPARE:
        message.body = &prepare;
        break;
    case VSR_MSG_DO_VIEW_CHANGE:
    case VSR_MSG_START_VIEW:
        message.body = &state;
        break;
    case VSR_MSG_RECOVERY:
    case VSR_MSG_RECOVERY_RESPONSE:
        message.body = &recovery;
        break;
    case VSR_MSG_GET_STATE:
    case VSR_MSG_GET_LOG:
        message.body = &fetch;
        break;
    case VSR_MSG_NEW_STATE:
    case VSR_MSG_LOG:
    case VSR_MSG_STATE_UNAVAILABLE:
        message.body = &chunk;
        break;
    case VSR_MSG_START_EPOCH:
    case VSR_MSG_NEW_EPOCH:
        message.body = &epoch;
        break;
    case VSR_MSG_CHECKPOINT:
        message.body = &checkpoint;
        break;
    case VSR_MSG_READ_PROBE:
    case VSR_MSG_READ_ACK:
        message.body = &nonce;
        break;
    default:
        break;
    }
    {
        struct vsr_validation validation = {.limits = &limits};
        (void)vsr_validate_blob(&validation, &blob, limits.command_bytes);
        validation = (struct vsr_validation){.limits = &limits};
        (void)vsr_validate_request(&validation, &request);
        validation = (struct vsr_validation){.limits = &limits};
        (void)vsr_validate_entries(&validation, &batch);
        validation = (struct vsr_validation){.limits = &limits};
        (void)vsr_validate_message(&validation, &message);
    }
    return 0;
}
