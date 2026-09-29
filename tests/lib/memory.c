#include "config.h"

#include "lib/memory.h"

#include "lib/check.h"

#include <stdlib.h>
#include <string.h>

struct mem_block {
    struct mem_block *next;
    void *data;
    size_t size;
};

struct mem_watch {
    struct mem_watch *next;
    const void *source;
    size_t size;
    unsigned char saved[];
};

struct mem_graph {
    struct mem_block *blocks;
    struct mem_watch *watches;
    bool watch_sources;
};

static void watch(struct mem_graph *graph, const void *source, size_t size)
{
    struct mem_watch *item;
    if (size == 0)
        return;
    CHECK(source != NULL && size <= SIZE_MAX - sizeof(*item));
    item = malloc(sizeof(*item) + size);
    CHECK(item != NULL);
    item->source = source;
    item->size = size;
    memcpy(item->saved, source, size);
    item->next = graph->watches;
    graph->watches = item;
}

static void watch_source(struct mem_graph *graph, const void *source,
                         size_t count, size_t size)
{
    if (graph->watch_sources) {
        CHECK(size != 0 && count <= SIZE_MAX / size);
        watch(graph, source, count * size);
    }
}

void mem_graph_watch_sources(struct mem_graph *graph)
{
    graph->watch_sources = true;
}

void mem_graph_freeze(struct mem_graph *graph)
{
    CHECK(graph->watches == NULL);
    for (const struct mem_block *block = graph->blocks; block != NULL;
         block = block->next)
        watch(graph, block->data, block->size);
}

bool mem_graph_unchanged(const struct mem_graph *graph)
{
    for (const struct mem_watch *item = graph->watches; item != NULL;
         item = item->next) {
        if (memcmp(item->source, item->saved, item->size) != 0)
            return false;
    }
    return true;
}

struct mem_graph *mem_graph_create(void)
{
    struct mem_graph *graph = calloc(1, sizeof(*graph));
    CHECK(graph != NULL);
    return graph;
}

void mem_graph_destroy(struct mem_graph *graph)
{
    if (graph == NULL)
        return;
    while (graph->watches != NULL) {
        struct mem_watch *item = graph->watches;
        graph->watches = item->next;
        free(item);
    }
    while (graph->blocks != NULL) {
        struct mem_block *block = graph->blocks;
        graph->blocks = block->next;
        free(block->data);
        free(block);
    }
    free(graph);
}

void *mem_graph_alloc(struct mem_graph *graph, size_t count, size_t size)
{
    struct mem_block *block;
    CHECK(size != 0 && count <= SIZE_MAX / size);
    if (count == 0)
        return NULL;
    block = malloc(sizeof(*block));
    CHECK(block != NULL);
    block->data = calloc(count, size);
    block->size = count * size;
    CHECK(block->data != NULL);
    block->next = graph->blocks;
    graph->blocks = block;
    return block->data;
}

static void *copy(struct mem_graph *graph, const void *object, size_t size)
{
    if (size == 0)
        return NULL;
    void *result = mem_graph_alloc(graph, 1, size);
    watch_source(graph, object, 1, size);
    memcpy(result, object, size);
    return result;
}

static bool same_id(struct vsr_id a, struct vsr_id b)
{
    return a.hi == b.hi && a.lo == b.lo;
}

static void clone_blob(struct mem_graph *graph, struct vsr_blob *blob)
{
    if (blob->count == 0) {
        blob->spans = NULL;
        return;
    }
    const struct vsr_span *source = blob->spans;
    struct vsr_span *spans =
        mem_graph_alloc(graph, blob->count, sizeof(*spans));
    CHECK(source != NULL && spans != NULL);
    watch_source(graph, source, blob->count, sizeof(*source));
    for (uint32_t i = 0; i < blob->count; i++) {
        spans[i] = source[i];
        spans[i].data = copy(graph, source[i].data, source[i].size);
    }
    blob->spans = spans;
}

static void *clone_body(struct mem_graph *graph, uint32_t type,
                        const void *body)
{
    switch (type) {
    case VSR_REQUEST_COMMAND:
        return mem_clone(graph, MEM_BLOB, body);
    case VSR_REQUEST_RECONFIGURE:
        return mem_clone(graph, MEM_MEMBERSHIP, body);
    case VSR_REQUEST_CHECK_EPOCH:
        return copy(graph, body, sizeof(struct vsr_check_epoch));
    case VSR_REQUEST_NOOP:
        CHECK(body == NULL);
        return NULL;
    default:
        CHECK(false);
        return NULL;
    }
}

static struct vsr_entry *clone_entries(struct mem_graph *graph,
                                       const struct vsr_entry *source,
                                       uint32_t count)
{
    struct vsr_entry *entries = mem_graph_alloc(graph, count, sizeof(*entries));
    CHECK(count == 0 || (entries != NULL && source != NULL));
    watch_source(graph, source, count, sizeof(*source));
    for (uint32_t i = 0; i < count; i++) {
        entries[i] = source[i];
        entries[i].body = clone_body(graph, source[i].type, source[i].body);
    }
    return entries;
}

static struct vsr_client_record *
clone_clients(struct mem_graph *graph, const struct vsr_client_record *source,
              uint32_t count)
{
    struct vsr_client_record *clients =
        mem_graph_alloc(graph, count, sizeof(*clients));
    CHECK(count == 0 || (clients != NULL && source != NULL));
    watch_source(graph, source, count, sizeof(*source));
    for (uint32_t i = 0; i < count; i++) {
        clients[i] = source[i];
        clone_blob(graph, &clients[i].result.data);
    }
    return clients;
}

static void clone_log_state(struct mem_graph *graph,
                            struct vsr_log_state *state)
{
    state->epoch = mem_clone(graph, MEM_EPOCH, state->epoch);
    state->checkpoint = mem_clone(graph, MEM_CHECKPOINT, state->checkpoint);
    state->entries.entries =
        clone_entries(graph, state->entries.entries, state->entries.count);
}

static void *clone_message_body(struct mem_graph *graph,
                                const struct vsr_message *message)
{
    switch (message->type) {
    case VSR_MSG_PREPARE: {
        struct vsr_prepare *prepare =
            copy(graph, message->body, sizeof(*prepare));
        prepare->batch.entries =
            clone_entries(graph, prepare->batch.entries, prepare->batch.count);
        return prepare;
    }
    case VSR_MSG_DO_VIEW_CHANGE:
    case VSR_MSG_START_VIEW: {
        struct vsr_log_state *state =
            copy(graph, message->body, sizeof(*state));
        clone_log_state(graph, state);
        return state;
    }
    case VSR_MSG_RECOVERY:
    case VSR_MSG_RECOVERY_RESPONSE: {
        struct vsr_recovery *recovery =
            copy(graph, message->body, sizeof(*recovery));
        if (recovery->state != NULL) {
            struct vsr_log_state *state =
                copy(graph, recovery->state, sizeof(*state));
            clone_log_state(graph, state);
            recovery->state = state;
        }
        return recovery;
    }
    case VSR_MSG_GET_STATE:
    case VSR_MSG_GET_LOG:
        return copy(graph, message->body, sizeof(struct vsr_fetch));
    case VSR_MSG_NEW_STATE:
    case VSR_MSG_LOG:
    case VSR_MSG_STATE_UNAVAILABLE: {
        struct vsr_state_chunk *chunk =
            copy(graph, message->body, sizeof(*chunk));
        clone_log_state(graph, &chunk->state);
        return chunk;
    }
    case VSR_MSG_START_EPOCH:
    case VSR_MSG_NEW_EPOCH:
        return mem_clone(graph, MEM_EPOCH, message->body);
    case VSR_MSG_CHECKPOINT:
        return mem_clone(graph, MEM_CHECKPOINT, message->body);
    case VSR_MSG_READ_PROBE:
    case VSR_MSG_READ_ACK:
        return copy(graph, message->body, sizeof(struct vsr_nonce));
    case VSR_MSG_PREPARE_OK:
    case VSR_MSG_COMMIT:
    case VSR_MSG_START_VIEW_CHANGE:
    case VSR_MSG_EPOCH_STARTED:
        CHECK(message->body == NULL);
        return NULL;
    default:
        CHECK(false);
        return NULL;
    }
}

void *mem_clone(struct mem_graph *graph, enum mem_kind kind, const void *object)
{
    if (object == NULL)
        return NULL;
    switch (kind) {
    case MEM_ID:
        return copy(graph, object, sizeof(struct vsr_id));
    case MEM_BLOB: {
        struct vsr_blob *blob = copy(graph, object, sizeof(*blob));
        clone_blob(graph, blob);
        return blob;
    }
    case MEM_MEMBERSHIP: {
        struct vsr_membership *membership =
            copy(graph, object, sizeof(*membership));
        struct vsr_member *members =
            mem_graph_alloc(graph, membership->count, sizeof(*members));
        watch_source(graph, membership->members, membership->count,
                     sizeof(*members));
        if (membership->count != 0)
            memcpy(members, membership->members,
                   membership->count * sizeof(*members));
        membership->members = members;
        return membership;
    }
    case MEM_EPOCH: {
        struct vsr_epoch *epoch = copy(graph, object, sizeof(*epoch));
        epoch->current = mem_clone(graph, MEM_MEMBERSHIP, epoch->current);
        epoch->previous = mem_clone(graph, MEM_MEMBERSHIP, epoch->previous);
        return epoch;
    }
    case MEM_REQUEST: {
        struct vsr_request *request = copy(graph, object, sizeof(*request));
        request->body = clone_body(graph, request->type, request->body);
        return request;
    }
    case MEM_ENTRY:
        return clone_entries(graph, object, 1);
    case MEM_CLIENT:
        return clone_clients(graph, object, 1);
    case MEM_REPLY: {
        struct vsr_reply *reply = copy(graph, object, sizeof(*reply));
        reply->membership = mem_clone(graph, MEM_MEMBERSHIP, reply->membership);
        clone_blob(graph, &reply->result.data);
        return reply;
    }
    case MEM_CHECKPOINT: {
        struct vsr_checkpoint *checkpoint =
            copy(graph, object, sizeof(*checkpoint));
        checkpoint->epoch = mem_clone(graph, MEM_EPOCH, checkpoint->epoch);
        clone_blob(graph, &checkpoint->manifest);
        return checkpoint;
    }
    case MEM_MESSAGE: {
        struct vsr_message *message = copy(graph, object, sizeof(*message));
        message->body = clone_message_body(graph, message);
        return message;
    }
    case MEM_HARD_STATE: {
        struct vsr_hard_state *hard = copy(graph, object, sizeof(*hard));
        hard->epoch = mem_clone(graph, MEM_EPOCH, hard->epoch);
        return hard;
    }
    case MEM_IDENTITY:
        return copy(graph, object, sizeof(struct vsr_store_identity));
    case MEM_RECOVERED: {
        struct vsr_recovered *recovered =
            copy(graph, object, sizeof(*recovered));
        recovered->hard.epoch =
            mem_clone(graph, MEM_EPOCH, recovered->hard.epoch);
        recovered->checkpoint =
            mem_clone(graph, MEM_CHECKPOINT, recovered->checkpoint);
        return recovered;
    }
    case MEM_STORE_READ:
        return copy(graph, object, sizeof(struct vsr_store_read));
    case MEM_STORE: {
        struct vsr_store *transaction =
            copy(graph, object, sizeof(*transaction));
        const struct vsr_change *source = transaction->changes;
        struct vsr_change *changes =
            mem_graph_alloc(graph, transaction->count, sizeof(*changes));
        watch_source(graph, source, transaction->count, sizeof(*source));
        for (uint32_t i = 0; i < transaction->count; i++) {
            changes[i] = source[i];
            switch (source[i].type) {
            case VSR_STORE_APPEND:
                changes[i].data =
                    clone_entries(graph, source[i].data, source[i].count);
                break;
            case VSR_STORE_CLIENTS:
                changes[i].data =
                    clone_clients(graph, source[i].data, source[i].count);
                break;
            case VSR_STORE_HARD_STATE:
                changes[i].data =
                    mem_clone(graph, MEM_HARD_STATE, source[i].data);
                break;
            case VSR_STORE_PUBLISH_CHECKPOINT:
            case VSR_STORE_RESTORE_CHECKPOINT:
                changes[i].data =
                    mem_clone(graph, MEM_CHECKPOINT, source[i].data);
                break;
            case VSR_STORE_IDENTITY:
                changes[i].data =
                    mem_clone(graph, MEM_IDENTITY, source[i].data);
                break;
            case VSR_STORE_TRIM:
            case VSR_STORE_TRUNCATE:
                CHECK(source[i].data == NULL);
                break;
            default:
                CHECK(false);
            }
        }
        transaction->changes = changes;
        return transaction;
    }
    case MEM_APPLY: {
        struct vsr_apply *apply = copy(graph, object, sizeof(*apply));
        apply->batch.entries =
            clone_entries(graph, apply->batch.entries, apply->batch.count);
        return apply;
    }
    case MEM_APPLIED: {
        struct vsr_applied *applied = copy(graph, object, sizeof(*applied));
        const struct vsr_value *source = applied->results;
        struct vsr_value *results =
            mem_graph_alloc(graph, applied->count, sizeof(*results));
        watch_source(graph, source, applied->count, sizeof(*source));
        for (uint32_t i = 0; i < applied->count; i++) {
            results[i] = source[i];
            clone_blob(graph, &results[i].data);
        }
        applied->results = results;
        return applied;
    }
    case MEM_READ_BARRIER:
        return copy(graph, object, sizeof(struct vsr_read_barrier));
    case MEM_READ_FENCE:
        return copy(graph, object, sizeof(struct vsr_read_fence));
    case MEM_SNAPSHOT_TASK: {
        struct vsr_snapshot_task *task = copy(graph, object, sizeof(*task));
        task->checkpoint = mem_clone(graph, MEM_CHECKPOINT, task->checkpoint);
        task->basis = mem_clone(graph, MEM_CHECKPOINT, task->basis);
        return task;
    }
    }
    CHECK(false);
    return NULL;
}

struct vsr_loaded *mem_clone_loaded(struct mem_graph *graph, uint32_t load_type,
                                    const struct vsr_loaded *loaded)
{
    struct vsr_loaded *result = copy(graph, loaded, sizeof(*result));
    switch (load_type) {
    case VSR_LOAD_RECOVERY:
        result->items = mem_clone(graph, MEM_RECOVERED, loaded->items);
        break;
    case VSR_LOAD_LOG:
    case VSR_LOAD_REQUEST:
        result->items = clone_entries(graph, loaded->items, loaded->count);
        break;
    case VSR_LOAD_CLIENT:
        result->items = clone_clients(graph, loaded->items, loaded->count);
        break;
    default:
        CHECK(false);
    }
    return result;
}

struct vsr_op mem_clone_op(struct mem_graph *graph, const struct vsr_op *op)
{
    struct vsr_op result = *op;
    switch (op->type) {
    case VSR_OP_SEND:
        result.data = mem_clone(graph, MEM_MESSAGE, op->data);
        break;
    case VSR_OP_REPLY:
        result.data = mem_clone(graph, MEM_REPLY, op->data);
        break;
    case VSR_OP_LOAD:
        result.data = mem_clone(graph, MEM_STORE_READ, op->data);
        break;
    case VSR_OP_STORE:
        result.data = mem_clone(graph, MEM_STORE, op->data);
        break;
    case VSR_OP_APPLY:
        result.data = mem_clone(graph, MEM_APPLY, op->data);
        break;
    case VSR_OP_READ_READY:
        result.data = mem_clone(graph, MEM_READ_FENCE, op->data);
        break;
    case VSR_OP_SNAPSHOT_CAPTURE:
    case VSR_OP_SNAPSHOT_FETCH:
    case VSR_OP_SNAPSHOT_SYNC:
    case VSR_OP_SNAPSHOT_INSTALL:
    case VSR_OP_SNAPSHOT_DROP:
        result.data = mem_clone(graph, MEM_SNAPSHOT_TASK, op->data);
        break;
    case VSR_OP_SYNC:
    case VSR_OP_RECLAIM:
    case VSR_OP_RELEASE:
        CHECK(op->data == NULL);
        break;
    default:
        CHECK(false);
    }
    return result;
}

bool mem_blob_equal(const struct vsr_blob *a, const struct vsr_blob *b)
{
    uint32_t ai = 0, bi = 0;
    size_t ao = 0, bo = 0;
    if (a->size != b->size)
        return false;
    while (ai < a->count && bi < b->count) {
        size_t n = a->spans[ai].size - ao;
        const size_t bn = b->spans[bi].size - bo;
        if (n > bn)
            n = bn;
        if (memcmp((const unsigned char *)a->spans[ai].data + ao,
                   (const unsigned char *)b->spans[bi].data + bo, n) != 0)
            return false;
        ao += n;
        bo += n;
        if (ao == a->spans[ai].size) {
            ai++;
            ao = 0;
        }
        if (bo == b->spans[bi].size) {
            bi++;
            bo = 0;
        }
    }
    return ai == a->count && bi == b->count;
}

static bool membership_equal(const struct vsr_membership *a,
                             const struct vsr_membership *b)
{
    if (a->epoch != b->epoch || a->count != b->count || a->faults != b->faults)
        return false;
    for (uint32_t i = 0; i < a->count; i++) {
        if (a->members[i].id != b->members[i].id ||
            a->members[i].role != b->members[i].role)
            return false;
    }
    return true;
}

bool mem_entry_equal(const struct vsr_entry *a, const struct vsr_entry *b)
{
    if (a->op != b->op || a->epoch != b->epoch || a->view != b->view ||
        !same_id(a->request.client, b->request.client) ||
        a->request.number != b->request.number || a->type != b->type)
        return false;
    switch (a->type) {
    case VSR_REQUEST_COMMAND:
        return mem_blob_equal(a->body, b->body);
    case VSR_REQUEST_RECONFIGURE:
        return membership_equal(a->body, b->body);
    case VSR_REQUEST_CHECK_EPOCH:
        return ((const struct vsr_check_epoch *)a->body)->epoch ==
               ((const struct vsr_check_epoch *)b->body)->epoch;
    case VSR_REQUEST_NOOP:
        return a->body == NULL && b->body == NULL;
    default:
        return false;
    }
}

struct mem_revision {
    struct mem_graph *graph;
    struct vsr_recovered recovered;
    struct vsr_entry *entries;
    uint32_t entry_count;
    struct vsr_client_record *clients;
    uint32_t client_count;
};

struct mem_pending {
    struct mem_graph *graph;
    const struct vsr_store *transaction;
};

struct mem_checkpoint {
    struct mem_graph *graph;
    const struct vsr_checkpoint *checkpoint;
    const struct vsr_client_record *clients;
    uint32_t client_count;
};

struct mem_store {
    struct mem_revision **revisions;
    struct mem_pending *pending;
    /* Logical shape of an issued but not yet readable transaction, built on
     * its predecessor so a malformed one is attributed at submission. */
    struct mem_revision **projected;
    size_t capacity;
    uint64_t readable;
    uint64_t durable;
    struct mem_checkpoint *checkpoints;
    size_t checkpoint_count;
};

struct mem_store *mem_store_create(void)
{
    struct mem_store *store = calloc(1, sizeof(*store));
    CHECK(store != NULL);
    return store;
}

static void revision_destroy(struct mem_revision *revision)
{
    if (revision != NULL) {
        mem_graph_destroy(revision->graph);
        free(revision);
    }
}

void mem_store_destroy(struct mem_store *store)
{
    if (store == NULL)
        return;
    for (size_t i = 0; i < store->capacity; i++) {
        revision_destroy(store->revisions[i]);
        revision_destroy(store->projected[i]);
        mem_graph_destroy(store->pending[i].graph);
    }
    for (size_t i = 0; i < store->checkpoint_count; i++)
        mem_graph_destroy(store->checkpoints[i].graph);
    free(store->checkpoints);
    free(store->pending);
    free(store->projected);
    free(store->revisions);
    free(store);
}

static struct mem_revision *revision_get(const struct mem_store *store,
                                         uint64_t sequence)
{
    if (sequence == 0 || sequence >= store->capacity)
        return NULL;
    return store->revisions[(size_t)sequence];
}

static void store_grow(struct mem_store *store, uint64_t sequence)
{
    size_t capacity = store->capacity == 0 ? 16 : store->capacity;
    struct mem_revision **revisions;
    struct mem_revision **projected;
    struct mem_pending *pending;
    CHECK(sequence < SIZE_MAX / sizeof(*pending));
    while (capacity <= sequence) {
        CHECK(capacity <= SIZE_MAX / 2);
        capacity *= 2;
    }
    if (capacity == store->capacity)
        return;
    revisions = calloc(capacity, sizeof(struct mem_revision *));
    projected = calloc(capacity, sizeof(struct mem_revision *));
    pending = calloc(capacity, sizeof(*pending));
    CHECK(revisions != NULL && projected != NULL && pending != NULL);
    if (store->capacity != 0) {
        memcpy(revisions, store->revisions,
               store->capacity * sizeof(struct mem_revision *));
        memcpy(projected, store->projected,
               store->capacity * sizeof(struct mem_revision *));
        memcpy(pending, store->pending, store->capacity * sizeof(*pending));
    }
    free(store->revisions);
    free(store->projected);
    free(store->pending);
    store->revisions = revisions;
    store->projected = projected;
    store->pending = pending;
    store->capacity = capacity;
}

static struct mem_revision *revision_clone(const struct mem_revision *source)
{
    struct mem_revision *revision = calloc(1, sizeof(*revision));
    CHECK(revision != NULL);
    revision->graph = mem_graph_create();
    if (source == NULL) {
        revision->recovered.log_begin = 1;
        revision->recovered.log_end = 1;
    } else {
        const struct vsr_recovered *recovered =
            mem_clone(revision->graph, MEM_RECOVERED, &source->recovered);
        revision->recovered = *recovered;
        revision->entry_count = source->entry_count;
        revision->client_count = source->client_count;
        revision->entries = clone_entries(revision->graph, source->entries,
                                          source->entry_count);
        revision->clients = clone_clients(revision->graph, source->clients,
                                          source->client_count);
    }
    return revision;
}

static const struct mem_checkpoint *
checkpoint_get(const struct mem_store *store, struct vsr_id id)
{
    for (size_t i = 0; i < store->checkpoint_count; i++) {
        if (same_id(store->checkpoints[i].checkpoint->id, id))
            return &store->checkpoints[i];
    }
    return NULL;
}

static int update_clients(struct mem_revision *revision,
                          const struct vsr_client_record *clients,
                          uint32_t count)
{
    struct vsr_client_record *records;
    CHECK(count <= UINT32_MAX - revision->client_count);
    records = mem_graph_alloc(revision->graph, revision->client_count + count,
                              sizeof(*records));
    if (revision->client_count != 0)
        memcpy(records, revision->clients,
               revision->client_count * sizeof(*records));
    revision->clients = records;
    for (uint32_t i = 0; i < count; i++) {
        uint32_t index = 0;
        while (
            index < revision->client_count &&
            !same_id(records[index].request.client, clients[i].request.client))
            index++;
        if (index < revision->client_count) {
            if (records[index].request.number > clients[i].request.number)
                continue;
            if (records[index].request.number == clients[i].request.number) {
                if (records[index].op != clients[i].op ||
                    records[index].result.code != clients[i].result.code ||
                    !mem_blob_equal(&records[index].result.data,
                                    &clients[i].result.data))
                    return VSR_IO_CORRUPT;
                continue;
            }
        }
        records[index] = clients[i];
        clone_blob(revision->graph, &records[index].result.data);
        if (index == revision->client_count)
            revision->client_count++;
    }
    return VSR_IO_OK;
}

static int apply_change(const struct mem_store *store,
                        struct mem_revision *revision,
                        const struct vsr_change *change, uint64_t sequence,
                        uint32_t final_role)
{
    struct vsr_recovered *recovered = &revision->recovered;
    switch (change->type) {
    case VSR_STORE_IDENTITY:
        if (sequence != 1 || recovered->identity.replica != 0 ||
            change->count != 1)
            return VSR_IO_CORRUPT;
        recovered->identity = *(const struct vsr_store_identity *)change->data;
        return VSR_IO_OK;
    case VSR_STORE_APPEND: {
        const struct vsr_entry *entries = change->data;
        struct vsr_entry *combined;
        if (change->first != recovered->log_end ||
            change->count > UINT32_MAX - revision->entry_count ||
            change->count > UINT64_MAX - change->first)
            return VSR_IO_CORRUPT;
        for (uint32_t i = 0; i < change->count; i++) {
            if (entries[i].op != change->first + i)
                return VSR_IO_CORRUPT;
        }
        combined = mem_graph_alloc(revision->graph,
                                   revision->entry_count + change->count,
                                   sizeof(*combined));
        if (revision->entry_count != 0)
            memcpy(combined, revision->entries,
                   revision->entry_count * sizeof(*combined));
        for (uint32_t i = 0; i < change->count; i++) {
            combined[revision->entry_count + i] = entries[i];
            combined[revision->entry_count + i].body =
                clone_body(revision->graph, entries[i].type, entries[i].body);
        }
        revision->entries = combined;
        revision->entry_count += change->count;
        recovered->log_end += change->count;
        return VSR_IO_OK;
    }
    case VSR_STORE_TRUNCATE:
        if (change->first <= recovered->hard.committed ||
            change->first < recovered->log_begin ||
            change->first > recovered->log_end)
            return VSR_IO_CORRUPT;
        revision->entry_count =
            (uint32_t)(change->first - recovered->log_begin);
        recovered->log_end = change->first;
        return VSR_IO_OK;
    case VSR_STORE_CLIENTS:
        return update_clients(revision, change->data, change->count);
    case VSR_STORE_HARD_STATE: {
        const struct vsr_hard_state *hard = change->data;
        if (change->count != 1 || hard->committed < recovered->hard.committed ||
            hard->last_normal_view > hard->view || hard->epoch == NULL)
            return VSR_IO_CORRUPT;
        if (recovered->hard.epoch != NULL) {
            const struct vsr_epoch *old = recovered->hard.epoch;
            if (hard->epoch->current->epoch < old->current->epoch)
                return VSR_IO_CORRUPT;
            if (hard->epoch->current->epoch == old->current->epoch &&
                (hard->view < recovered->hard.view ||
                 !membership_equal(old->current, hard->epoch->current) ||
                 old->boundary != hard->epoch->boundary ||
                 ((old->previous == NULL) != (hard->epoch->previous == NULL)) ||
                 (old->previous != NULL &&
                  !membership_equal(old->previous, hard->epoch->previous))))
                return VSR_IO_CORRUPT;
        }
        recovered->hard = *hard;
        recovered->hard.epoch =
            mem_clone(revision->graph, MEM_EPOCH, hard->epoch);
        return VSR_IO_OK;
    }
    case VSR_STORE_PUBLISH_CHECKPOINT: {
        const struct vsr_checkpoint *checkpoint = change->data;
        if (change->count != 1 || checkpoint->op > recovered->hard.committed ||
            (recovered->checkpoint != NULL &&
             checkpoint->op < recovered->checkpoint->op))
            return VSR_IO_CORRUPT;
        recovered->checkpoint =
            mem_clone(revision->graph, MEM_CHECKPOINT, checkpoint);
        return VSR_IO_OK;
    }
    case VSR_STORE_RESTORE_CHECKPOINT: {
        const struct vsr_checkpoint *checkpoint = change->data;
        const struct mem_checkpoint *base =
            checkpoint_get(store, checkpoint->id);
        uint32_t drop = 0;
        if ((base == NULL && final_role != VSR_MEMBER_WITNESS) ||
            change->count != 1 || checkpoint->op == UINT64_MAX)
            return VSR_IO_CORRUPT;
        while (drop < revision->entry_count &&
               revision->entries[drop].op <= checkpoint->op)
            drop++;
        if (drop != 0) {
            revision->entries += drop;
            revision->entry_count -= drop;
        }
        recovered->log_begin = checkpoint->op + 1;
        if (recovered->log_end < recovered->log_begin)
            recovered->log_end = recovered->log_begin;
        revision->clients = base == NULL
                                ? NULL
                                : clone_clients(revision->graph, base->clients,
                                                base->client_count);
        revision->client_count = base == NULL ? 0 : base->client_count;
        recovered->checkpoint =
            mem_clone(revision->graph, MEM_CHECKPOINT, checkpoint);
        return VSR_IO_OK;
    }
    case VSR_STORE_TRIM: {
        uint64_t remove;
        if (recovered->checkpoint == NULL ||
            change->first < recovered->log_begin ||
            change->first > recovered->log_end ||
            change->first > recovered->checkpoint->op + 1)
            return VSR_IO_CORRUPT;
        remove = change->first - recovered->log_begin;
        if (remove != 0)
            revision->entries += (size_t)remove;
        revision->entry_count -= (uint32_t)remove;
        recovered->log_begin = change->first;
        return VSR_IO_OK;
    }
    default:
        return VSR_IO_CORRUPT;
    }
}

/* Builds the immutable revision that transaction `sequence` produces on top
 * of `base`, or reports the first logical violation and leaves no revision. */
static int revision_build(const struct mem_store *store,
                          const struct mem_revision *base,
                          const struct vsr_store *transaction,
                          uint64_t sequence, struct mem_revision **out)
{
    struct mem_revision *revision = revision_clone(base);
    uint32_t final_role = revision->recovered.hard.role;
    *out = NULL;
    for (uint32_t i = 0; i < transaction->count; i++) {
        const struct vsr_change *change = &transaction->changes[i];
        if (change->type == VSR_STORE_HARD_STATE)
            final_role = ((const struct vsr_hard_state *)change->data)->role;
    }
    for (uint32_t i = 0; i < transaction->count; i++) {
        const int result = apply_change(
            store, revision, &transaction->changes[i], sequence, final_role);
        if (result != VSR_IO_OK) {
            revision_destroy(revision);
            return result;
        }
    }
    if (revision->recovered.identity.replica == 0 ||
        revision->recovered.hard.epoch == NULL ||
        revision->recovered.hard.committed >= revision->recovered.log_end ||
        revision->recovered.log_begin >
            revision->recovered.hard.committed + 1 ||
        (revision->recovered.log_begin > 1 &&
         revision->recovered.checkpoint == NULL) ||
        (revision->recovered.checkpoint != NULL &&
         (revision->recovered.checkpoint->op >
              revision->recovered.hard.committed ||
          revision->recovered.checkpoint->op + 1 <
              revision->recovered.log_begin ||
          revision->recovered.checkpoint->op >= revision->recovered.log_end)) ||
        revision->recovered.log_end - revision->recovered.log_begin !=
            revision->entry_count) {
        revision_destroy(revision);
        return VSR_IO_CORRUPT;
    }
    revision->recovered.sequence = sequence;
    *out = revision;
    return VSR_IO_OK;
}

int mem_store_validate(struct mem_store *store,
                       const struct vsr_store *transaction)
{
    const struct mem_revision *base;
    struct mem_revision *projected;
    int result;
    if (transaction->sequence == 0 || transaction->sequence == UINT64_MAX ||
        transaction->sequence <= store->readable || transaction->count == 0 ||
        transaction->count > VSR_MAX_STORE_CHANGES)
        return VSR_IO_CORRUPT;
    store_grow(store, transaction->sequence);
    if (store->projected[(size_t)transaction->sequence] != NULL)
        return VSR_IO_CORRUPT;
    if (transaction->sequence == store->readable + 1)
        base = revision_get(store, store->readable);
    else
        base = store->projected[(size_t)transaction->sequence - 1];
    if (base == NULL && transaction->sequence != 1)
        return VSR_IO_CORRUPT;
    result = revision_build(store, base, transaction, transaction->sequence,
                            &projected);
    if (result == VSR_IO_OK)
        store->projected[(size_t)transaction->sequence] = projected;
    return result;
}

int mem_store_submit(struct mem_store *store,
                     const struct vsr_store *transaction)
{
    struct mem_pending *pending;
    if (transaction->sequence == 0 || transaction->sequence == UINT64_MAX ||
        transaction->sequence <= store->readable || transaction->count == 0 ||
        transaction->count > VSR_MAX_STORE_CHANGES)
        return VSR_IO_CORRUPT;
    store_grow(store, transaction->sequence);
    pending = &store->pending[(size_t)transaction->sequence];
    if (pending->graph != NULL)
        return VSR_IO_CORRUPT;
    pending->graph = mem_graph_create();
    pending->transaction = mem_clone(pending->graph, MEM_STORE, transaction);
    while (store->readable + 1 < store->capacity) {
        const uint64_t next = store->readable + 1;
        struct mem_revision *revision;
        int result;
        pending = &store->pending[(size_t)next];
        if (pending->graph == NULL)
            break;
        result = revision_build(store, revision_get(store, store->readable),
                                pending->transaction, next, &revision);
        mem_graph_destroy(pending->graph);
        *pending = (struct mem_pending){0};
        if (result != VSR_IO_OK)
            return result;
        /* The projection built at submission is now superseded by the fact. */
        revision_destroy(store->projected[(size_t)next]);
        store->projected[(size_t)next] = NULL;
        store->revisions[(size_t)next] = revision;
        store->readable = next;
    }
    return VSR_IO_OK;
}

int mem_store_sync(struct mem_store *store, uint64_t sequence)
{
    if (sequence > store->readable)
        return VSR_IO_RETRY;
    if (sequence > store->durable)
        store->durable = sequence;
    return VSR_IO_OK;
}

void mem_store_crash(struct mem_store *store)
{
    for (size_t i = 0; i < store->capacity; i++) {
        if (i > store->durable) {
            revision_destroy(store->revisions[i]);
            store->revisions[i] = NULL;
        }
        revision_destroy(store->projected[i]);
        store->projected[i] = NULL;
        mem_graph_destroy(store->pending[i].graph);
        store->pending[i] = (struct mem_pending){0};
    }
    store->readable = store->durable;
}

void mem_store_reclaim(struct mem_store *store, uint64_t oldest)
{
    for (size_t i = 1; i < store->capacity && i < oldest; i++) {
        if (i != store->readable && i != store->durable) {
            revision_destroy(store->revisions[i]);
            store->revisions[i] = NULL;
        }
    }
}

uint64_t mem_store_readable(const struct mem_store *store)
{
    return store->readable;
}

uint64_t mem_store_durable(const struct mem_store *store)
{
    return store->durable;
}

const struct vsr_recovered *mem_store_recovered(const struct mem_store *store,
                                                uint64_t sequence)
{
    const struct mem_revision *revision = revision_get(store, sequence);
    return revision == NULL ? NULL : &revision->recovered;
}

const struct vsr_entry *mem_store_entry(const struct mem_store *store,
                                        uint64_t sequence, uint64_t op)
{
    const struct mem_revision *revision = revision_get(store, sequence);
    if (revision == NULL || op < revision->recovered.log_begin ||
        op >= revision->recovered.log_end)
        return NULL;
    return &revision->entries[(size_t)(op - revision->recovered.log_begin)];
}

const struct vsr_client_record *mem_store_client(const struct mem_store *store,
                                                 uint64_t sequence,
                                                 struct vsr_id client)
{
    const struct mem_revision *revision = revision_get(store, sequence);
    if (revision != NULL) {
        for (uint32_t i = 0; i < revision->client_count; i++) {
            if (same_id(revision->clients[i].request.client, client))
                return &revision->clients[i];
        }
    }
    return NULL;
}

static uint64_t entry_bytes(const struct vsr_entry *entry)
{
    if (entry->type == VSR_REQUEST_COMMAND)
        return ((const struct vsr_blob *)entry->body)->size;
    return 0;
}

int mem_store_load(const struct mem_store *store,
                   const struct vsr_store_read *read, struct mem_graph *graph,
                   struct vsr_loaded **loaded)
{
    const struct mem_revision *revision;
    struct vsr_loaded result = {0};
    *loaded = NULL;
    if (read->type == VSR_LOAD_RECOVERY) {
        revision = revision_get(store, store->readable);
        if (revision == NULL)
            return VSR_IO_NOT_FOUND;
        result.items = &revision->recovered;
        result.sequence = store->readable;
        result.count = 1;
    } else {
        revision = revision_get(store, read->sequence);
        if (revision == NULL)
            return VSR_IO_NOT_FOUND;
        result.sequence = read->sequence;
        switch (read->type) {
        case VSR_LOAD_LOG: {
            uint64_t bytes = 0;
            if (read->first < revision->recovered.log_begin ||
                read->end > revision->recovered.log_end ||
                read->first > read->end)
                return VSR_IO_NOT_FOUND;
            if (read->first == read->end) {
                result.next = read->end;
                break;
            }
            result.items = &revision->entries[(
                size_t)(read->first - revision->recovered.log_begin)];
            result.next = read->first;
            while (result.next < read->end && result.count < read->max_count) {
                const struct vsr_entry *entry =
                    mem_store_entry(store, read->sequence, result.next);
                CHECK(entry != NULL);
                const uint64_t size = entry_bytes(entry);
                if (size > read->max_bytes - bytes)
                    break;
                bytes += size;
                result.count++;
                result.next++;
            }
            if (result.count == 0)
                return VSR_IO_FAILED;
            break;
        }
        case VSR_LOAD_CLIENT: {
            const struct vsr_client_record *client =
                mem_store_client(store, read->sequence, read->client);
            if (client != NULL) {
                if (client->result.data.size > read->max_bytes)
                    return VSR_IO_FAILED;
                result.items = client;
                result.count = 1;
            }
            break;
        }
        case VSR_LOAD_REQUEST:
            for (uint32_t i = 0; i < revision->entry_count; i++) {
                const struct vsr_entry *entry = &revision->entries[i];
                if (same_id(entry->request.client, read->client) &&
                    (result.items == NULL ||
                     entry->request.number >
                         ((const struct vsr_entry *)result.items)
                             ->request.number)) {
                    result.items = entry;
                    result.count = 1;
                }
            }
            if (result.items != NULL &&
                entry_bytes(result.items) > read->max_bytes)
                return VSR_IO_FAILED;
            break;
        case VSR_LOAD_RECOVERY:
        default:
            return VSR_IO_CORRUPT;
        }
    }
    *loaded = mem_clone_loaded(graph, read->type, &result);
    return VSR_IO_OK;
}

void mem_store_checkpoint(struct mem_store *store,
                          const struct vsr_checkpoint *checkpoint,
                          const struct vsr_client_record *clients,
                          uint32_t count)
{
    struct mem_checkpoint *checkpoints;
    struct mem_checkpoint *saved;
    const struct mem_checkpoint *existing =
        checkpoint_get(store, checkpoint->id);
    for (uint32_t i = 0; i < count; i++)
        CHECK(clients[i].op <= checkpoint->op);
    if (existing != NULL) {
        CHECK(existing->checkpoint->op == checkpoint->op);
        return;
    }
    CHECK(store->checkpoint_count < SIZE_MAX / sizeof(*checkpoints) - 1);
    checkpoints = realloc(store->checkpoints,
                          (store->checkpoint_count + 1) * sizeof(*checkpoints));
    CHECK(checkpoints != NULL);
    store->checkpoints = checkpoints;
    saved = &store->checkpoints[store->checkpoint_count++];
    saved->graph = mem_graph_create();
    saved->checkpoint = mem_clone(saved->graph, MEM_CHECKPOINT, checkpoint);
    saved->clients = clone_clients(saved->graph, clients, count);
    saved->client_count = count;
}

void mem_store_checkpoint_from_revision(struct mem_store *store,
                                        const struct vsr_checkpoint *checkpoint,
                                        uint64_t sequence)
{
    const struct mem_revision *revision = revision_get(store, sequence);
    CHECK(revision != NULL || (sequence == 0 && checkpoint->op == 0));
    mem_store_checkpoint(store, checkpoint,
                         revision == NULL ? NULL : revision->clients,
                         revision == NULL ? 0 : revision->client_count);
}

bool mem_store_checkpoint_copy(struct mem_store *destination,
                               const struct mem_store *source,
                               const struct vsr_checkpoint *checkpoint)
{
    const struct mem_checkpoint *base = checkpoint_get(source, checkpoint->id);
    if (base == NULL)
        return false;
    mem_store_checkpoint(destination, checkpoint, base->clients,
                         base->client_count);
    return true;
}
