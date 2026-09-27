#include "config.h"

#include "validate.h"

#include <stdalign.h>
#include <stdbool.h>
#include <stdint.h>

#define TRY(expression)                                                        \
    do {                                                                       \
        int validation_result = (expression);                                  \
        if (validation_result != VSR_OK) {                                     \
            return validation_result;                                          \
        }                                                                      \
    } while (0)

static bool id_present(struct vsr_id id)
{
    return id.hi != 0 || id.lo != 0;
}

static bool id_equal(struct vsr_id a, struct vsr_id b)
{
    return a.hi == b.hi && a.lo == b.lo;
}

static bool counter(uint64_t value)
{
    return value < UINT64_MAX;
}

/* Check the address range before dereferencing. This cannot establish pointer
 * accessibility, but rejects misalignment, integer wrap and arena aliases. */
static int object(const struct vsr_validation *v, const void *pointer,
                  size_t size, size_t alignment)
{
    uintptr_t begin = (uintptr_t)pointer;
    uintptr_t arena = (uintptr_t)v->arena;

    if (pointer == NULL || begin % alignment != 0 ||
        size > UINTPTR_MAX - begin) {
        return VSR_EINVAL;
    }
    if (v->arena_size != 0) {
        if (v->arena == NULL || v->arena_size > UINTPTR_MAX - arena ||
            (begin < arena + v->arena_size && arena < begin + size)) {
            return VSR_EINVAL;
        }
    }
    return VSR_OK;
}

#define OBJECT(v, p, type) object((v), (p), sizeof(type), alignof(type))

static int array(const struct vsr_validation *v, const void *pointer,
                 uint32_t count, size_t size, size_t alignment)
{
    if (count == 0) {
        return pointer == NULL ? VSR_OK : VSR_EINVAL;
    }
    if ((uint64_t)count > SIZE_MAX / size) {
        return VSR_ELIMIT;
    }
    return object(v, pointer, (size_t)count * size, alignment);
}

static int add_payload(struct vsr_validation *v, uint64_t size)
{
    if (size > UINT64_MAX - v->payload_bytes) {
        return VSR_ELIMIT;
    }
    v->payload_bytes += size;
    return VSR_OK;
}

static int inconsistent(struct vsr_validation *v, uint32_t failure)
{
    if (failure == VSR_FAILURE_NONE) {
        return VSR_EINVAL;
    }
    if (v->failure_code == VSR_FAILURE_NONE) {
        v->failure_code = failure;
    }
    return VSR_OK;
}

int vsr_validate_membership(struct vsr_validation *v,
                            const struct vsr_membership *membership)
{
    uint32_t full = 0;
    uint64_t previous = 0;

    TRY(OBJECT(v, membership, struct vsr_membership));
    if (!counter(membership->epoch) || membership->count == 0 ||
        (uint64_t)membership->faults * 2 + 1 > membership->count) {
        return VSR_EINVAL;
    }
    if (membership->count > v->limits->members) {
        return VSR_ELIMIT;
    }
    TRY(array(v, membership->members, membership->count,
              sizeof(*membership->members), alignof(struct vsr_member)));
    for (uint32_t i = 0; i < membership->count; ++i) {
        const struct vsr_member *member = &membership->members[i];
        if (member->id <= previous || member->reserved != 0 ||
            (member->role != VSR_MEMBER_FULL &&
             member->role != VSR_MEMBER_WITNESS)) {
            return VSR_EINVAL;
        }
        previous = member->id;
        full += (uint32_t)(member->role == VSR_MEMBER_FULL);
    }
    return full > membership->faults ? VSR_OK : VSR_EINVAL;
}

int vsr_validate_epoch(struct vsr_validation *v, const struct vsr_epoch *epoch)
{
    TRY(OBJECT(v, epoch, struct vsr_epoch));
    if (epoch->reserved != 0 || epoch->phase > VSR_EPOCH_INSTALLED ||
        !counter(epoch->boundary)) {
        return VSR_EINVAL;
    }
    TRY(vsr_validate_membership(v, epoch->current));
    if (epoch->current->epoch == 0) {
        return epoch->previous == NULL && epoch->boundary == 0 &&
                       epoch->phase == VSR_EPOCH_STEADY
                   ? VSR_OK
                   : VSR_EINVAL;
    }
    if (epoch->boundary == 0) {
        return VSR_EINVAL;
    }
    TRY(vsr_validate_membership(v, epoch->previous));
    return epoch->previous->epoch == epoch->current->epoch - 1 ? VSR_OK
                                                               : VSR_EINVAL;
}

int vsr_validate_blob(struct vsr_validation *v, const struct vsr_blob *blob,
                      uint64_t limit)
{
    uint64_t size = 0;

    TRY(OBJECT(v, blob, struct vsr_blob));
    if (blob->reserved != 0 || (blob->count == 0) != (blob->size == 0)) {
        return VSR_EINVAL;
    }
    if (blob->count > v->limits->spans_per_blob || blob->size > limit) {
        return VSR_ELIMIT;
    }
    TRY(array(v, blob->spans, blob->count, sizeof(*blob->spans),
              alignof(struct vsr_span)));
    for (uint32_t i = 0; i < blob->count; ++i) {
        const struct vsr_span *span = &blob->spans[i];
        if (span->size == 0) {
            return VSR_EINVAL;
        }
        if (span->size > UINT64_MAX - size) {
            return VSR_ELIMIT;
        }
        TRY(object(v, span->data, span->size, 1));
        size += (uint64_t)span->size;
    }
    if (size != blob->size) {
        return VSR_EINVAL;
    }
    return add_payload(v, size);
}

static int request_id(struct vsr_request_id id)
{
    return id_present(id.client) && id.number != 0 && counter(id.number)
               ? VSR_OK
               : VSR_EINVAL;
}

/* Routing preconditions on client requests produce protocol replies. A logged
 * entry, however, must bind its control body to the entry's actual epoch. */
static int request_body(struct vsr_validation *v, struct vsr_request_id id,
                        uint32_t type, const void *body, uint64_t epoch,
                        bool logged)
{
    if (type == VSR_REQUEST_NOOP) {
        return logged && !id_present(id.client) && id.number == 0 &&
                       body == NULL
                   ? VSR_OK
                   : VSR_EINVAL;
    }
    TRY(request_id(id));
    switch (type) {
    case VSR_REQUEST_COMMAND:
        return vsr_validate_blob(v, body, v->limits->command_bytes);
    case VSR_REQUEST_RECONFIGURE: {
        const struct vsr_membership *membership = body;
        TRY(vsr_validate_membership(v, membership));
        if (logged && membership->epoch != epoch + 1) {
            return VSR_EINVAL;
        }
        return VSR_OK;
    }
    case VSR_REQUEST_CHECK_EPOCH: {
        const struct vsr_check_epoch *check = body;
        TRY(OBJECT(v, check, struct vsr_check_epoch));
        return counter(check->epoch) && (!logged || check->epoch <= epoch)
                   ? VSR_OK
                   : VSR_EINVAL;
    }
    default:
        return VSR_EINVAL;
    }
}

int vsr_validate_request(struct vsr_validation *v,
                         const struct vsr_request *request)
{
    TRY(OBJECT(v, request, struct vsr_request));
    if (request->reserved != 0 || !counter(request->epoch)) {
        return VSR_EINVAL;
    }
    return request_body(v, request->id, request->type, request->body,
                        request->epoch, false);
}

int vsr_validate_entry(struct vsr_validation *v, const struct vsr_entry *entry)
{
    TRY(OBJECT(v, entry, struct vsr_entry));
    if (entry->reserved != 0 || entry->op == 0 || !counter(entry->op) ||
        !counter(entry->epoch) || !counter(entry->view)) {
        return VSR_EINVAL;
    }
    return request_body(v, entry->request, entry->type, entry->body,
                        entry->epoch, true);
}

int vsr_validate_entries(struct vsr_validation *v,
                         const struct vsr_entries *entries)
{
    TRY(OBJECT(v, entries, struct vsr_entries));
    if (entries->reserved != 0) {
        return VSR_EINVAL;
    }
    if (entries->count > v->limits->batch_entries) {
        return VSR_ELIMIT;
    }
    TRY(array(v, entries->entries, entries->count, sizeof(*entries->entries),
              alignof(struct vsr_entry)));
    for (uint32_t i = 0; i < entries->count; ++i) {
        const struct vsr_entry *entry = &entries->entries[i];
        TRY(vsr_validate_entry(v, entry));
        if (i != 0 && entry->op != entries->entries[i - 1].op + 1) {
            return VSR_EINVAL;
        }
    }
    return VSR_OK;
}

static int checkpoint_shape(struct vsr_validation *v,
                            const struct vsr_checkpoint *checkpoint,
                            uint32_t failure)
{
    TRY(OBJECT(v, checkpoint, struct vsr_checkpoint));
    if (!id_present(checkpoint->id) || !counter(checkpoint->op) ||
        !counter(checkpoint->view)) {
        return VSR_EINVAL;
    }
    TRY(vsr_validate_epoch(v, checkpoint->epoch));
    if (checkpoint->epoch->boundary > checkpoint->op ||
        (checkpoint->op == 0 && checkpoint->view != 0)) {
        TRY(inconsistent(v, failure));
    }
    return vsr_validate_blob(v, &checkpoint->manifest,
                             v->limits->manifest_bytes);
}

int vsr_validate_checkpoint(struct vsr_validation *v,
                            const struct vsr_checkpoint *value)
{
    return checkpoint_shape(v, value, VSR_FAILURE_NONE);
}

static int nonce(struct vsr_nonce value)
{
    return id_present(value.incarnation) && value.counter != 0 &&
                   counter(value.counter)
               ? VSR_OK
               : VSR_EINVAL;
}

static bool revision_present(struct vsr_revision value)
{
    return id_present(value.incarnation) && value.sequence != 0 &&
           counter(value.sequence);
}

static bool revision_empty(struct vsr_revision value)
{
    return !id_present(value.incarnation) && value.sequence == 0;
}

static int log_bounds(struct vsr_validation *v, uint64_t first, uint64_t end,
                      uint64_t committed, const struct vsr_checkpoint *anchor,
                      uint32_t failure)
{
    if (first == 0 || !counter(first) || end == 0 || !counter(committed)) {
        return VSR_EINVAL;
    }
    if (first > end || committed >= end || first > committed + 1) {
        TRY(inconsistent(v, failure));
    }
    if (anchor == NULL) {
        return first == 1 ? VSR_OK : inconsistent(v, failure);
    }
    TRY(checkpoint_shape(v, anchor, failure));
    if (anchor->op > committed || anchor->op + 1 < first ||
        anchor->op + 1 > end) {
        TRY(inconsistent(v, failure));
    }
    return VSR_OK;
}

static int log_state(struct vsr_validation *v,
                     const struct vsr_log_state *state)
{
    TRY(OBJECT(v, state, struct vsr_log_state));
    if (!revision_present(state->revision) || !counter(state->view) ||
        state->last_normal_view > state->view) {
        return VSR_EINVAL;
    }
    TRY(vsr_validate_epoch(v, state->epoch));
    if (state->epoch->boundary > state->committed) {
        return VSR_EINVAL;
    }
    TRY(log_bounds(v, state->log_begin, state->log_end, state->committed,
                   state->checkpoint, VSR_FAILURE_NONE));
    if (state->checkpoint != NULL && state->checkpoint->epoch->current->epoch >
                                         state->epoch->current->epoch) {
        return VSR_EINVAL;
    }
    TRY(vsr_validate_entries(v, &state->entries));
    if (state->entries.count != 0 &&
        (state->entries.entries[0].op < state->log_begin ||
         state->entries.entries[state->entries.count - 1].op >=
             state->log_end)) {
        return VSR_EINVAL;
    }
    return VSR_OK;
}

static int fetch(struct vsr_validation *v, const struct vsr_fetch *request,
                 bool discovery_allowed)
{
    TRY(OBJECT(v, request, struct vsr_fetch));
    TRY(nonce(request->nonce));
    if (request->reserved != 0 || request->max_entries == 0 ||
        request->max_bytes == 0) {
        return VSR_EINVAL;
    }
    if (request->max_entries > v->limits->batch_entries ||
        request->max_bytes > v->limits->message_bytes ||
        request->max_bytes <
            v->limits->command_bytes + v->limits->manifest_bytes) {
        return VSR_ELIMIT;
    }
    if (revision_empty(request->revision)) {
        return discovery_allowed && request->first == 0 && request->end == 0
                   ? VSR_OK
                   : VSR_EINVAL;
    }
    return revision_present(request->revision) && request->first != 0 &&
                   counter(request->first) && request->first <= request->end
               ? VSR_OK
               : VSR_EINVAL;
}

int vsr_validate_message(struct vsr_validation *v,
                         const struct vsr_message *message)
{
    uint64_t before = v->payload_bytes;

    TRY(OBJECT(v, message, struct vsr_message));
    if (!id_present(message->cluster) || message->from == 0 ||
        !counter(message->epoch) || !counter(message->view) ||
        !counter(message->number) || message->flags != 0) {
        return VSR_EINVAL;
    }
    switch (message->type) {
    case VSR_MSG_PREPARE: {
        const struct vsr_prepare *prepare = message->body;
        TRY(OBJECT(v, prepare, struct vsr_prepare));
        TRY(vsr_validate_entries(v, &prepare->batch));
        if (prepare->batch.count == 0 ||
            prepare->batch.entries[prepare->batch.count - 1].op !=
                message->number ||
            prepare->committed > message->number) {
            return VSR_EINVAL;
        }
        break;
    }
    case VSR_MSG_START_VIEW_CHANGE:
    case VSR_MSG_PREPARE_OK:
    case VSR_MSG_COMMIT:
    case VSR_MSG_EPOCH_STARTED:
        if (message->body != NULL ||
            (message->type == VSR_MSG_START_VIEW_CHANGE &&
             message->number != 0) ||
            (message->type == VSR_MSG_EPOCH_STARTED && message->number == 0)) {
            return VSR_EINVAL;
        }
        break;
    case VSR_MSG_DO_VIEW_CHANGE:
    case VSR_MSG_START_VIEW: {
        const struct vsr_log_state *state = message->body;
        TRY(log_state(v, state));
        if (message->number != state->log_end - 1) {
            return VSR_EINVAL;
        }
        break;
    }
    case VSR_MSG_RECOVERY:
    case VSR_MSG_RECOVERY_RESPONSE: {
        const struct vsr_recovery *recovery = message->body;
        TRY(OBJECT(v, recovery, struct vsr_recovery));
        TRY(nonce(recovery->nonce));
        if (message->type == VSR_MSG_RECOVERY) {
            if (recovery->state != NULL || message->number != 0) {
                return VSR_EINVAL;
            }
        } else if (recovery->state == NULL) {
            if (message->number != 0) {
                return VSR_EINVAL;
            }
        } else {
            TRY(log_state(v, recovery->state));
            if (message->number != recovery->state->log_end - 1) {
                return VSR_EINVAL;
            }
        }
        break;
    }
    case VSR_MSG_GET_STATE:
    case VSR_MSG_GET_LOG:
        if (message->number != 0) {
            return VSR_EINVAL;
        }
        TRY(fetch(v, message->body, message->type == VSR_MSG_GET_STATE));
        break;
    case VSR_MSG_NEW_STATE:
    case VSR_MSG_LOG:
    case VSR_MSG_STATE_UNAVAILABLE: {
        const struct vsr_state_chunk *chunk = message->body;
        TRY(OBJECT(v, chunk, struct vsr_state_chunk));
        TRY(nonce(chunk->nonce));
        TRY(log_state(v, &chunk->state));
        if (message->number != chunk->state.log_end - 1 ||
            !counter(chunk->first) || chunk->first > chunk->next) {
            return VSR_EINVAL;
        }
        if (message->type == VSR_MSG_STATE_UNAVAILABLE || chunk->first == 0) {
            if (chunk->first != chunk->next ||
                chunk->state.entries.count != 0 ||
                (chunk->first == 0 && message->type == VSR_MSG_LOG)) {
                return VSR_EINVAL;
            }
        } else if (chunk->first < chunk->state.log_begin ||
                   chunk->next > chunk->state.log_end ||
                   chunk->next - chunk->first != chunk->state.entries.count ||
                   (chunk->state.entries.count != 0 &&
                    chunk->state.entries.entries[0].op != chunk->first)) {
            return VSR_EINVAL;
        }
        break;
    }
    case VSR_MSG_START_EPOCH:
    case VSR_MSG_NEW_EPOCH: {
        const struct vsr_epoch *epoch = message->body;
        TRY(vsr_validate_epoch(v, epoch));
        if (message->number != epoch->boundary) {
            return VSR_EINVAL;
        }
        break;
    }
    case VSR_MSG_CHECKPOINT: {
        const struct vsr_checkpoint *checkpoint = message->body;
        TRY(vsr_validate_checkpoint(v, checkpoint));
        if (message->number != checkpoint->op) {
            return VSR_EINVAL;
        }
        break;
    }
    case VSR_MSG_READ_PROBE:
    case VSR_MSG_READ_ACK: {
        const struct vsr_nonce *read = message->body;
        TRY(OBJECT(v, read, struct vsr_nonce));
        TRY(nonce(*read));
        break;
    }
    default:
        return VSR_EINVAL;
    }
    return v->payload_bytes - before <= v->limits->message_bytes ? VSR_OK
                                                                 : VSR_ELIMIT;
}

int vsr_validate_recovered(struct vsr_validation *v,
                           const struct vsr_recovered *recovered)
{
    const struct vsr_hard_state *hard;

    TRY(OBJECT(v, recovered, struct vsr_recovered));
    if (!id_present(recovered->identity.cluster) ||
        recovered->identity.replica == 0 ||
        recovered->identity.durability > VSR_REPLICATED ||
        recovered->identity.reserved != 0 || recovered->sequence == 0 ||
        !counter(recovered->sequence)) {
        return VSR_EINVAL;
    }
    hard = &recovered->hard;
    if (!counter(hard->view) || !counter(hard->last_normal_view) ||
        hard->state > VSR_HARD_RETIRED ||
        (hard->role != VSR_MEMBER_FULL && hard->role != VSR_MEMBER_WITNESS)) {
        return VSR_EINVAL;
    }
    if (hard->last_normal_view > hard->view ||
        (hard->state == VSR_HARD_NORMAL &&
         hard->last_normal_view != hard->view)) {
        TRY(inconsistent(v, VSR_FAILURE_STORAGE));
    }
    TRY(vsr_validate_epoch(v, hard->epoch));
    bool transferring = hard->epoch->phase == VSR_EPOCH_TRANSFERRING &&
                        (hard->state == VSR_HARD_RECOVERING ||
                         hard->state == VSR_HARD_TRANSITIONING);
    if (hard->epoch->boundary > hard->committed && !transferring) {
        TRY(inconsistent(v, VSR_FAILURE_STORAGE));
    }
    TRY(log_bounds(v, recovered->log_begin, recovered->log_end, hard->committed,
                   recovered->checkpoint, VSR_FAILURE_STORAGE));
    if (recovered->checkpoint != NULL &&
        recovered->checkpoint->epoch->current->epoch >
            hard->epoch->current->epoch) {
        TRY(inconsistent(v, VSR_FAILURE_STORAGE));
    }
    return VSR_OK;
}

static int value(struct vsr_validation *v, const struct vsr_value *result)
{
    TRY(OBJECT(v, result, struct vsr_value));
    if (result->reserved != 0) {
        return VSR_EINVAL;
    }
    return vsr_validate_blob(v, &result->data, v->limits->result_bytes);
}

static int client_record(struct vsr_validation *v,
                         const struct vsr_client_record *record)
{
    TRY(OBJECT(v, record, struct vsr_client_record));
    TRY(request_id(record->request));
    if (record->op == 0 || !counter(record->op)) {
        return VSR_EINVAL;
    }
    return value(v, &record->result);
}

static int loaded(struct vsr_validation *v, const struct vsr_loaded *result,
                  const struct vsr_store_read *read)
{
    uint64_t before = v->payload_bytes;

    TRY(OBJECT(v, result, struct vsr_loaded));
    if (result->reserved != 0 ||
        (result->count == 0) != (result->items == NULL) ||
        !counter(result->sequence)) {
        return VSR_EINVAL;
    }
    if (result->count > read->max_count) {
        return VSR_ELIMIT;
    }
    if ((read->type != VSR_LOAD_RECOVERY &&
         result->sequence != read->sequence) ||
        (read->type != VSR_LOAD_LOG && result->next != 0)) {
        TRY(inconsistent(v, VSR_FAILURE_STORAGE));
    }
    switch (read->type) {
    case VSR_LOAD_RECOVERY: {
        const struct vsr_recovered *recovered = result->items;
        if (result->count != 1) {
            TRY(inconsistent(v, VSR_FAILURE_STORAGE));
            break;
        }
        TRY(vsr_validate_recovered(v, recovered));
        if (result->sequence != recovered->sequence) {
            TRY(inconsistent(v, VSR_FAILURE_STORAGE));
        }
        break;
    }
    case VSR_LOAD_LOG: {
        const struct vsr_entry *entries = result->items;
        if (result->count > v->limits->batch_entries) {
            return VSR_ELIMIT;
        }
        TRY(array(v, entries, result->count, sizeof(*entries),
                  alignof(struct vsr_entry)));
        if (result->next < read->first || result->next > read->end ||
            result->next - read->first != result->count ||
            (result->count == 0 && read->first != read->end)) {
            TRY(inconsistent(v, VSR_FAILURE_STORAGE));
        }
        for (uint32_t i = 0; i < result->count; ++i) {
            TRY(vsr_validate_entry(v, &entries[i]));
            if (i > UINT64_MAX - read->first ||
                entries[i].op != read->first + i) {
                TRY(inconsistent(v, VSR_FAILURE_STORAGE));
            }
        }
        break;
    }
    case VSR_LOAD_CLIENT:
        if (result->count > 1) {
            return VSR_EINVAL;
        }
        if (result->count != 0) {
            const struct vsr_client_record *record = result->items;
            TRY(client_record(v, record));
            if (!id_equal(record->request.client, read->client)) {
                TRY(inconsistent(v, VSR_FAILURE_STORAGE));
            }
        }
        break;
    case VSR_LOAD_REQUEST:
        if (result->count > 1) {
            return VSR_EINVAL;
        }
        if (result->count != 0) {
            const struct vsr_entry *entry = result->items;
            TRY(vsr_validate_entry(v, entry));
            if (!id_equal(entry->request.client, read->client)) {
                TRY(inconsistent(v, VSR_FAILURE_STORAGE));
            }
        }
        break;
    default:
        return VSR_EINVAL;
    }
    return v->payload_bytes - before <= read->max_bytes ? VSR_OK : VSR_ELIMIT;
}

static bool membership_equal(const struct vsr_membership *a,
                             const struct vsr_membership *b)
{
    if (a->epoch != b->epoch || a->count != b->count ||
        a->faults != b->faults) {
        return false;
    }
    for (uint32_t i = 0; i < a->count; ++i) {
        if (a->members[i].id != b->members[i].id ||
            a->members[i].role != b->members[i].role) {
            return false;
        }
    }
    return true;
}

static bool epoch_equal(const struct vsr_epoch *a, const struct vsr_epoch *b)
{
    return a != NULL && b != NULL && a->boundary == b->boundary &&
           a->phase == b->phase && membership_equal(a->current, b->current) &&
           ((a->previous == NULL && b->previous == NULL) ||
            (a->previous != NULL && b->previous != NULL &&
             membership_equal(a->previous, b->previous)));
}

static int complete(struct vsr_validation *v, const struct vsr_event *event,
                    const struct vsr_op *operation)
{
    if (operation == NULL || event->id != operation->id || event->id == 0 ||
        event->status < VSR_IO_OK || event->status > VSR_IO_CANCELLED ||
        operation->type >= VSR_OP_RELEASE) {
        return VSR_EINVAL;
    }
    if (event->status != VSR_IO_OK) {
        return event->data == NULL ? VSR_OK : VSR_EINVAL;
    }
    switch (operation->type) {
    case VSR_OP_LOAD:
        return operation->data != NULL ? loaded(v, event->data, operation->data)
                                       : VSR_EINVAL;
    case VSR_OP_APPLY: {
        const struct vsr_applied *result = event->data;
        const struct vsr_apply *apply = operation->data;
        TRY(OBJECT(v, result, struct vsr_applied));
        if (apply == NULL || result->reserved != 0) {
            return VSR_EINVAL;
        }
        if (result->count > v->limits->batch_entries) {
            return VSR_ELIMIT;
        }
        if (result->count != apply->batch.count) {
            TRY(inconsistent(v, VSR_FAILURE_APPLICATION));
        }
        TRY(array(v, result->results, result->count, sizeof(*result->results),
                  alignof(struct vsr_value)));
        for (uint32_t i = 0; i < result->count; ++i) {
            TRY(value(v, &result->results[i]));
            if (i < apply->batch.count &&
                apply->batch.entries[i].type != VSR_REQUEST_COMMAND &&
                (result->results[i].code != 0 ||
                 result->results[i].data.size != 0)) {
                TRY(inconsistent(v, VSR_FAILURE_APPLICATION));
            }
        }
        return VSR_OK;
    }
    case VSR_OP_SNAPSHOT_CAPTURE:
    case VSR_OP_SNAPSHOT_FETCH: {
        const struct vsr_snapshot_task *task = operation->data;
        const struct vsr_checkpoint *checkpoint = event->data;
        TRY(checkpoint_shape(v, checkpoint, VSR_FAILURE_SNAPSHOT));
        if (task == NULL || task->checkpoint == NULL ||
            checkpoint->op != task->op ||
            checkpoint->op != task->checkpoint->op ||
            checkpoint->view != task->checkpoint->view ||
            !epoch_equal(checkpoint->epoch, task->checkpoint->epoch) ||
            (operation->type == VSR_OP_SNAPSHOT_FETCH &&
             !id_equal(checkpoint->id, task->checkpoint->id))) {
            TRY(inconsistent(v, VSR_FAILURE_SNAPSHOT));
        }
        return VSR_OK;
    }
    default:
        return event->data == NULL ? VSR_OK : VSR_EINVAL;
    }
}

int vsr_validate_event(struct vsr_validation *v, const struct vsr_event *event,
                       const struct vsr_op *completion)
{
    TRY(OBJECT(v, event, struct vsr_event));
    if ((event->data == NULL) != (event->lease == 0) ||
        (event->type != VSR_EVENT_COMPLETE && event->status != VSR_IO_OK)) {
        return VSR_EINVAL;
    }
    switch (event->type) {
    case VSR_EVENT_TIME:
        return counter(event->id) && event->data == NULL ? VSR_OK : VSR_EINVAL;
    case VSR_EVENT_MESSAGE:
        return event->id == 0 ? vsr_validate_message(v, event->data)
                              : VSR_EINVAL;
    case VSR_EVENT_REQUEST:
        return event->id != 0 ? vsr_validate_request(v, event->data)
                              : VSR_EINVAL;
    case VSR_EVENT_CLIENT_QUERY: {
        const struct vsr_id *client = event->data;
        TRY(OBJECT(v, client, struct vsr_id));
        return event->id != 0 && id_present(*client) ? VSR_OK : VSR_EINVAL;
    }
    case VSR_EVENT_READ: {
        const struct vsr_read_barrier *read = event->data;
        TRY(OBJECT(v, read, struct vsr_read_barrier));
        return event->id != 0 && counter(read->min_op) && read->reserved == 0 &&
                       read->consistency <= VSR_READ_CAUSAL
                   ? VSR_OK
                   : VSR_EINVAL;
    }
    case VSR_EVENT_COMPLETE:
        return complete(v, event, completion);
    case VSR_EVENT_CHECKPOINT:
    case VSR_EVENT_STOP:
        return event->id == 0 && event->data == NULL ? VSR_OK : VSR_EINVAL;
    default:
        return VSR_EINVAL;
    }
}

int vsr_validate_options(const struct vsr_options *options)
{
    struct vsr_validation v = {0};
    const struct vsr_limits *limits;
    bool member = false;
    uint64_t minimum;

    TRY(OBJECT(&v, options, struct vsr_options));
    limits = &options->limits;
    v.limits = limits;
    if (!id_present(options->cluster) || !id_present(options->incarnation) ||
        options->replica == 0 || options->reserved != 0 ||
        options->start_mode > VSR_START_JOIN ||
        options->durability > VSR_REPLICATED || options->heartbeat_ns == 0 ||
        !counter(options->heartbeat_ns) ||
        options->view_timeout_ns <= options->heartbeat_ns ||
        !counter(options->view_timeout_ns) || options->retry_ns == 0 ||
        !counter(options->retry_ns) || options->transfer_timeout_ns == 0 ||
        !counter(options->transfer_timeout_ns) ||
        !counter(options->batch_delay_ns) ||
        !counter(options->checkpoint_interval) ||
        (options->cache_line_bytes != 0 &&
         (options->cache_line_bytes & (options->cache_line_bytes - 1)) != 0)) {
        return VSR_EINVAL;
    }
    if (limits->members == 0 || limits->operations == 0 ||
        limits->input_leases == 0 || limits->pending_requests == 0 ||
        limits->pending_reads == 0 || limits->transfers == 0 ||
        limits->log_cache_entries == 0 || limits->client_cache_entries == 0 ||
        limits->batch_entries == 0 || limits->spans_per_blob == 0 ||
        limits->work_per_step == 0 || limits->command_bytes == 0 ||
        limits->result_bytes == 0 || limits->manifest_bytes == 0 ||
        limits->message_bytes == 0 || limits->pinned_payload_bytes == 0) {
        return VSR_EINVAL;
    }
    if (limits->batch_entries > limits->log_cache_entries ||
        limits->command_bytes > UINT64_MAX - limits->manifest_bytes ||
        limits->result_bytes > UINT64_MAX / limits->batch_entries) {
        return VSR_ELIMIT;
    }
    minimum = limits->command_bytes + limits->manifest_bytes;
    if (limits->message_bytes < minimum ||
        limits->result_bytes > UINT64_MAX - minimum ||
        limits->pinned_payload_bytes < minimum + limits->result_bytes) {
        return VSR_ELIMIT;
    }
    TRY(vsr_validate_membership(&v, options->seed));
    for (uint32_t i = 0; i < options->seed->count; ++i) {
        if (options->seed->members[i].id == options->replica) {
            member = true;
            break;
        }
    }
    if (options->start_mode == VSR_START_NEW &&
        (!member || options->seed->epoch != 0)) {
        return VSR_EINVAL;
    }
    if (options->start_mode == VSR_START_JOIN) {
        if (member || (options->join_role != VSR_MEMBER_FULL &&
                       options->join_role != VSR_MEMBER_WITNESS)) {
            return VSR_EINVAL;
        }
    } else if (options->join_role != VSR_MEMBER_NONE) {
        return VSR_EINVAL;
    }
    return VSR_OK;
}
