#include "config.h"

#include "internal.h"

#include <stdalign.h>
#include <string.h>

/* Only descriptor memory is copied. Lease pins protect span payloads. */
static void *copy_array(struct vsr_operation *operation, const void *source,
                        size_t count, size_t size, size_t alignment)
{
    if (count == 0) {
        return NULL;
    }
    if (size != 0 && count > SIZE_MAX / size) {
        return NULL;
    }
    void *target = vsr_operation_alloc(operation, count * size, alignment);
    if (target != NULL) {
        memcpy(target, source, count * size);
    }
    return target;
}

#define COPY_ONE(operation, source, type)                                      \
    copy_array((operation), (source), 1, sizeof(type), alignof(type))

static bool copy_blob(struct vsr_operation *operation, struct vsr_blob *blob)
{
    if (blob->count == 0) {
        blob->spans = NULL;
        return true;
    }
    blob->spans = copy_array(operation, blob->spans, blob->count,
                             sizeof(*blob->spans), alignof(struct vsr_span));
    return blob->spans != NULL;
}

static struct vsr_membership *
copy_membership(struct vsr_operation *operation,
                const struct vsr_membership *source)
{
    if (source == NULL) {
        return NULL;
    }
    struct vsr_membership *target =
        COPY_ONE(operation, source, struct vsr_membership);
    if (target == NULL) {
        return NULL;
    }
    target->members =
        copy_array(operation, source->members, source->count,
                   sizeof(*source->members), alignof(struct vsr_member));
    return target->members == NULL ? NULL : target;
}

static struct vsr_epoch *copy_epoch(struct vsr_operation *operation,
                                    const struct vsr_epoch *source)
{
    if (source == NULL) {
        return NULL;
    }
    struct vsr_epoch *target = COPY_ONE(operation, source, struct vsr_epoch);
    if (target == NULL) {
        return NULL;
    }
    target->current = copy_membership(operation, source->current);
    target->previous = copy_membership(operation, source->previous);
    if (target->current == NULL ||
        (source->previous != NULL && target->previous == NULL)) {
        return NULL;
    }
    return target;
}

static struct vsr_checkpoint *
copy_checkpoint(struct vsr_operation *operation,
                const struct vsr_checkpoint *source)
{
    if (source == NULL) {
        return NULL;
    }
    struct vsr_checkpoint *target =
        COPY_ONE(operation, source, struct vsr_checkpoint);
    if (target == NULL) {
        return NULL;
    }
    target->epoch = copy_epoch(operation, source->epoch);
    if (target->epoch == NULL || !copy_blob(operation, &target->manifest)) {
        return NULL;
    }
    return target;
}

static bool copy_entry_body(struct vsr_operation *operation,
                            struct vsr_entry *entry)
{
    switch (entry->type) {
    case VSR_REQUEST_COMMAND: {
        struct vsr_blob *body =
            COPY_ONE(operation, entry->body, struct vsr_blob);
        if (body == NULL || !copy_blob(operation, body)) {
            return false;
        }
        entry->body = body;
        return true;
    }
    case VSR_REQUEST_RECONFIGURE:
        entry->body = copy_membership(operation, entry->body);
        return entry->body != NULL;
    case VSR_REQUEST_CHECK_EPOCH:
        entry->body = COPY_ONE(operation, entry->body, struct vsr_check_epoch);
        return entry->body != NULL;
    case VSR_REQUEST_NOOP:
        entry->body = NULL;
        return true;
    default:
        return false;
    }
}

static bool copy_entries(struct vsr_operation *operation,
                         struct vsr_entries *batch)
{
    if (batch->count == 0) {
        batch->entries = NULL;
        return true;
    }
    struct vsr_entry *entries =
        copy_array(operation, batch->entries, batch->count, sizeof(*entries),
                   alignof(struct vsr_entry));
    if (entries == NULL) {
        return false;
    }
    batch->entries = entries;
    for (uint32_t i = 0; i < batch->count; i++) {
        if (!copy_entry_body(operation, &entries[i])) {
            return false;
        }
    }
    return true;
}

static bool copy_log_state(struct vsr_operation *operation,
                           struct vsr_log_state *state)
{
    const struct vsr_checkpoint *checkpoint = state->checkpoint;
    state->epoch = copy_epoch(operation, state->epoch);
    state->checkpoint = copy_checkpoint(operation, checkpoint);
    return state->epoch != NULL &&
           (checkpoint == NULL || state->checkpoint != NULL) &&
           copy_entries(operation, &state->entries);
}

static bool copy_message(struct vsr_operation *operation,
                         struct vsr_message *message)
{
    switch (message->type) {
    case VSR_MSG_PREPARE: {
        struct vsr_prepare *body =
            COPY_ONE(operation, message->body, struct vsr_prepare);
        if (body == NULL || !copy_entries(operation, &body->batch)) {
            return false;
        }
        message->body = body;
        return true;
    }
    case VSR_MSG_DO_VIEW_CHANGE:
    case VSR_MSG_START_VIEW: {
        struct vsr_log_state *body =
            COPY_ONE(operation, message->body, struct vsr_log_state);
        if (body == NULL || !copy_log_state(operation, body)) {
            return false;
        }
        message->body = body;
        return true;
    }
    case VSR_MSG_RECOVERY:
    case VSR_MSG_RECOVERY_RESPONSE: {
        struct vsr_recovery *body =
            COPY_ONE(operation, message->body, struct vsr_recovery);
        if (body == NULL) {
            return false;
        }
        if (body->state != NULL) {
            struct vsr_log_state *state =
                COPY_ONE(operation, body->state, struct vsr_log_state);
            if (state == NULL || !copy_log_state(operation, state)) {
                return false;
            }
            body->state = state;
        }
        message->body = body;
        return true;
    }
    case VSR_MSG_GET_STATE:
    case VSR_MSG_GET_LOG:
        message->body = COPY_ONE(operation, message->body, struct vsr_fetch);
        return message->body != NULL;
    case VSR_MSG_NEW_STATE:
    case VSR_MSG_LOG:
    case VSR_MSG_STATE_UNAVAILABLE: {
        struct vsr_state_chunk *body =
            COPY_ONE(operation, message->body, struct vsr_state_chunk);
        if (body == NULL || !copy_log_state(operation, &body->state)) {
            return false;
        }
        message->body = body;
        return true;
    }
    case VSR_MSG_START_EPOCH:
    case VSR_MSG_NEW_EPOCH:
        message->body = copy_epoch(operation, message->body);
        return message->body != NULL;
    case VSR_MSG_CHECKPOINT:
        message->body = copy_checkpoint(operation, message->body);
        return message->body != NULL;
    case VSR_MSG_READ_PROBE:
    case VSR_MSG_READ_ACK:
        message->body = COPY_ONE(operation, message->body, struct vsr_nonce);
        return message->body != NULL;
    case VSR_MSG_PREPARE_OK:
    case VSR_MSG_COMMIT:
    case VSR_MSG_START_VIEW_CHANGE:
    case VSR_MSG_EPOCH_STARTED:
        message->body = NULL;
        return true;
    default:
        return false;
    }
}

static bool copy_change(struct vsr_operation *operation,
                        struct vsr_change *change)
{
    switch (change->type) {
    case VSR_STORE_APPEND: {
        struct vsr_entries batch = {change->data, change->count, 0};
        if (!copy_entries(operation, &batch)) {
            return false;
        }
        change->data = batch.entries;
        return true;
    }
    case VSR_STORE_CLIENTS: {
        struct vsr_client_record *clients =
            copy_array(operation, change->data, change->count, sizeof(*clients),
                       alignof(struct vsr_client_record));
        if (clients == NULL) {
            return false;
        }
        for (uint32_t i = 0; i < change->count; i++) {
            if (!copy_blob(operation, &clients[i].result.data)) {
                return false;
            }
        }
        change->data = clients;
        return true;
    }
    case VSR_STORE_HARD_STATE: {
        struct vsr_hard_state *hard =
            COPY_ONE(operation, change->data, struct vsr_hard_state);
        if (hard == NULL) {
            return false;
        }
        hard->epoch = copy_epoch(operation, hard->epoch);
        change->data = hard;
        return hard->epoch != NULL;
    }
    case VSR_STORE_PUBLISH_CHECKPOINT:
    case VSR_STORE_RESTORE_CHECKPOINT:
        change->data = copy_checkpoint(operation, change->data);
        return change->data != NULL;
    case VSR_STORE_IDENTITY:
        change->data =
            COPY_ONE(operation, change->data, struct vsr_store_identity);
        return change->data != NULL;
    case VSR_STORE_TRUNCATE:
    case VSR_STORE_TRIM:
        change->data = NULL;
        return true;
    default:
        return false;
    }
}

static const void *copy_operation(struct vsr_operation *operation,
                                  const void *data)
{
    switch (operation->output.type) {
    case VSR_OP_SEND: {
        struct vsr_message *message =
            COPY_ONE(operation, data, struct vsr_message);
        return message != NULL && copy_message(operation, message) ? message
                                                                   : NULL;
    }
    case VSR_OP_REPLY: {
        struct vsr_reply *reply = COPY_ONE(operation, data, struct vsr_reply);
        if (reply == NULL) {
            return NULL;
        }
        const struct vsr_membership *membership = reply->membership;
        reply->membership = copy_membership(operation, membership);
        return (membership == NULL || reply->membership != NULL) &&
                       copy_blob(operation, &reply->result.data)
                   ? reply
                   : NULL;
    }
    case VSR_OP_LOAD:
        return COPY_ONE(operation, data, struct vsr_store_read);
    case VSR_OP_STORE: {
        struct vsr_store *store = COPY_ONE(operation, data, struct vsr_store);
        if (store == NULL) {
            return NULL;
        }
        struct vsr_change *changes =
            copy_array(operation, store->changes, store->count,
                       sizeof(*changes), alignof(struct vsr_change));
        if (changes == NULL) {
            return NULL;
        }
        store->changes = changes;
        uint32_t seen = 0;
        for (uint32_t i = 0; i < store->count; i++) {
            uint32_t type = changes[i].type;
            if (type > VSR_STORE_IDENTITY) {
                return NULL;
            }
            /* Match the arena planner's bounded internal transaction shape.
             * A publication/restoration shares one checkpoint descriptor slot. */
            uint32_t key = type == VSR_STORE_RESTORE_CHECKPOINT
                               ? VSR_STORE_PUBLISH_CHECKPOINT
                               : type;
            uint32_t bit = UINT32_C(1) << key;
            if ((seen & bit) != 0) {
                return NULL;
            }
            seen |= bit;
            if (!copy_change(operation, &changes[i])) {
                return NULL;
            }
        }
        return store;
    }
    case VSR_OP_APPLY: {
        struct vsr_apply *apply = COPY_ONE(operation, data, struct vsr_apply);
        return apply != NULL && copy_entries(operation, &apply->batch) ? apply
                                                                       : NULL;
    }
    case VSR_OP_READ_READY:
        return COPY_ONE(operation, data, struct vsr_read_fence);
    case VSR_OP_SNAPSHOT_CAPTURE:
    case VSR_OP_SNAPSHOT_FETCH:
    case VSR_OP_SNAPSHOT_SYNC:
    case VSR_OP_SNAPSHOT_INSTALL:
    case VSR_OP_SNAPSHOT_DROP: {
        struct vsr_snapshot_task *task =
            COPY_ONE(operation, data, struct vsr_snapshot_task);
        if (task == NULL) {
            return NULL;
        }
        const struct vsr_checkpoint *checkpoint = task->checkpoint;
        const struct vsr_checkpoint *basis = task->basis;
        task->checkpoint = copy_checkpoint(operation, checkpoint);
        task->basis = copy_checkpoint(operation, basis);
        return (checkpoint == NULL || task->checkpoint != NULL) &&
                       (basis == NULL || task->basis != NULL)
                   ? task
                   : NULL;
    }
    case VSR_OP_SYNC:
    case VSR_OP_RECLAIM:
    case VSR_OP_RELEASE:
    default:
        return NULL;
    }
}

int vsr_operation_copy(struct vsr_operation *operation, const void *data)
{
    if (operation == NULL || operation->state != VSR_SLOT_BUILDING) {
        return VSR_EINVAL;
    }
    if (data == NULL) {
        if (operation->output.type != VSR_OP_SYNC &&
            operation->output.type != VSR_OP_RECLAIM) {
            return VSR_EINVAL;
        }
        operation->output.data = NULL;
        return VSR_OK;
    }
    size_t used = operation->used;
    const void *copy = copy_operation(operation, data);
    if (copy == NULL) {
        operation->used = used;
        return VSR_ELIMIT;
    }
    operation->output.data = copy;
    return VSR_OK;
}
