#include "config.h"

#include "lib/check.h"
#include "lib/memory.h"

#include <string.h>

static const struct vsr_member members[] = {{1, VSR_MEMBER_FULL, 0}};
static const struct vsr_membership membership = {0, members, 1, 0};
static const struct vsr_epoch epoch = {&membership, NULL, 0, VSR_EPOCH_STEADY,
                                       0};
static const struct vsr_id client_id = {10, 20};

static struct vsr_hard_state hard_state(uint64_t committed)
{
    return (struct vsr_hard_state){
        0, 0, committed, &epoch, VSR_HARD_NORMAL, VSR_MEMBER_FULL};
}

static void initialize(struct mem_store *store)
{
    const struct vsr_store_identity identity = {{1, 2}, 1, VSR_DURABLE, 0};
    const struct vsr_hard_state hard = hard_state(0);
    const struct vsr_change changes[] = {{VSR_STORE_IDENTITY, 1, 0, &identity},
                                         {VSR_STORE_HARD_STATE, 1, 0, &hard}};
    const struct vsr_store transaction = {1, changes, 2, 0};
    CHECK(mem_store_submit(store, &transaction) == VSR_IO_OK);
}

static void test_graph_copy(void)
{
    struct mem_graph *graph = mem_graph_create();
    unsigned char bytes[] = {1, 2, 3, 4};
    const struct vsr_span spans[] = {{bytes, 1}, {bytes + 1, 3}};
    const struct vsr_blob blob = {spans, 4, 2, 0};
    struct vsr_entry entry = {1, 0,    0, {{10, 20}, 1}, VSR_REQUEST_COMMAND,
                              0, &blob};
    const struct vsr_checkpoint checkpoint = {{70, 1}, 0, 0, &epoch, blob};
    const struct vsr_log_state state = {
        {{80, 1}, 2}, 0, 0, 0, 1, 2, &epoch, {&entry, 1, 0}, &checkpoint};
    const struct vsr_recovery recovery = {{{90, 1}, 1}, &state};
    const struct vsr_message message = {
        {1, 2}, 0, 0, 1, VSR_MSG_RECOVERY_RESPONSE, 0, 1, &recovery};
    const struct vsr_message *copied = mem_clone(graph, MEM_MESSAGE, &message);
    const struct vsr_recovery *copied_recovery = copied->body;
    const struct vsr_log_state *copied_state = copied_recovery->state;
    const struct vsr_entry *copied_entry = copied_state->entries.entries;
    const struct vsr_blob *copied_blob = copied_entry->body;
    CHECK(copied != &message && copied_recovery != &recovery);
    CHECK(copied_state != &state && copied_entry != &entry);
    CHECK(copied_state->epoch != &epoch);
    CHECK(copied_state->epoch->current->members != members);
    CHECK(copied_state->checkpoint != &checkpoint);
    CHECK(mem_entry_equal(copied_entry, &entry));
    CHECK(copied_blob->spans[0].data != spans[0].data);
    bytes[0] = 99;
    entry.op = 9;
    CHECK(copied_entry->op == 1);
    CHECK(*(const unsigned char *)copied_blob->spans[0].data == 1);
    CHECK(*(const unsigned char *)copied_state->checkpoint->manifest.spans[0]
               .data == 1);
    mem_graph_destroy(graph);
}

static void test_blob_segmentation(void)
{
    const char text[] = "deterministic";
    const struct vsr_span a_spans[] = {{text, 13}};
    const struct vsr_span b_spans[] = {{text, 2}, {text + 2, 4}, {text + 6, 7}};
    const struct vsr_span bad_spans[] = {{"Deterministic", 13}};
    const struct vsr_blob a = {a_spans, 13, 1, 0};
    const struct vsr_blob b = {b_spans, 13, 3, 0};
    const struct vsr_blob bad = {bad_spans, 13, 1, 0};
    const struct vsr_blob empty = {0};
    CHECK(mem_blob_equal(&a, &b));
    CHECK(!mem_blob_equal(&a, &bad));
    CHECK(!mem_blob_equal(&a, &empty));
    CHECK(mem_blob_equal(&empty, &empty));
}

static void test_transaction_frontiers(void)
{
    struct mem_store *store = mem_store_create();
    const struct vsr_blob command = {0};
    const struct vsr_entry entries[] = {
        {1, 0, 0, {{10, 20}, 1}, VSR_REQUEST_COMMAND, 0, &command},
        {2, 0, 0, {{10, 20}, 2}, VSR_REQUEST_COMMAND, 0, &command}};
    const struct vsr_change append = {VSR_STORE_APPEND, 2, 1, entries};
    const struct vsr_store second = {2, &append, 1, 0};
    const struct vsr_hard_state hard = hard_state(1);
    const struct vsr_change commit = {VSR_STORE_HARD_STATE, 1, 0, &hard};
    const struct vsr_store third = {3, &commit, 1, 0};
    struct mem_graph *graph = mem_graph_create();
    struct vsr_loaded *loaded = NULL;
    const struct vsr_store_read recovery = {
        0, 0, 0, {0, 0}, 0, VSR_LOAD_RECOVERY, 1};
    CHECK(mem_store_load(store, &recovery, graph, &loaded) == VSR_IO_NOT_FOUND);
    initialize(store);
    CHECK(mem_store_durable(store) == 0);
    CHECK(mem_store_submit(store, &third) == VSR_IO_OK);
    CHECK(mem_store_readable(store) == 1);
    CHECK(mem_store_sync(store, 3) == VSR_IO_RETRY);
    CHECK(mem_store_submit(store, &second) == VSR_IO_OK);
    CHECK(mem_store_readable(store) == 3);
    CHECK(mem_store_recovered(store, 1)->log_end == 1);
    CHECK(mem_store_recovered(store, 2)->hard.committed == 0);
    CHECK(mem_store_recovered(store, 3)->hard.committed == 1);
    CHECK(mem_store_sync(store, 2) == VSR_IO_OK);
    CHECK(mem_store_durable(store) == 2);
    mem_store_crash(store);
    CHECK(mem_store_readable(store) == 2 && mem_store_durable(store) == 2);
    CHECK(mem_store_recovered(store, 3) == NULL);
    CHECK(mem_store_load(store, &recovery, graph, &loaded) == VSR_IO_OK);
    CHECK(loaded->sequence == 2 && loaded->count == 1);
    CHECK(((const struct vsr_recovered *)loaded->items)->hard.committed == 0);
    CHECK(mem_store_submit(store, &third) == VSR_IO_OK);
    CHECK(mem_store_sync(store, 3) == VSR_IO_OK);
    mem_store_reclaim(store, 3);
    CHECK(mem_store_recovered(store, 1) == NULL);
    CHECK(mem_store_recovered(store, 3) != NULL);
    /* A loaded graph is independent of both crash truncation and reclaim. */
    CHECK(((const struct vsr_recovered *)loaded->items)->sequence == 2);
    mem_graph_destroy(graph);
    mem_store_destroy(store);
}

static void test_atomicity_and_indexes(void)
{
    struct mem_store *store = mem_store_create();
    struct mem_graph *graph = mem_graph_create();
    const struct vsr_span span = {"abc", 3};
    const struct vsr_blob command = {&span, 3, 1, 0};
    const struct vsr_entry entries[] = {
        {1, 0, 0, {{10, 20}, 1}, VSR_REQUEST_COMMAND, 0, &command},
        {2, 0, 0, {{10, 20}, 2}, VSR_REQUEST_COMMAND, 0, &command},
        {3, 0, 0, {{10, 20}, 3}, VSR_REQUEST_COMMAND, 0, &command}};
    const struct vsr_hard_state hard = hard_state(1);
    const struct vsr_change changes[] = {{VSR_STORE_APPEND, 3, 1, entries},
                                         {VSR_STORE_HARD_STATE, 1, 0, &hard}};
    const struct vsr_store second = {2, changes, 2, 0};
    struct vsr_loaded *loaded;
    struct vsr_store_read read = {2, 1, 4, {0, 0}, 4, VSR_LOAD_LOG, 3};
    initialize(store);
    CHECK(mem_store_submit(store, &second) == VSR_IO_OK);
    CHECK(mem_store_load(store, &read, graph, &loaded) == VSR_IO_OK);
    CHECK(loaded->count == 1 && loaded->next == 2);
    CHECK(mem_entry_equal(loaded->items, &entries[0]));
    /* Empty ranges are successful complete scans, including log_end. */
    read.first = 4;
    read.end = 4;
    CHECK(mem_store_load(store, &read, graph, &loaded) == VSR_IO_OK);
    CHECK(loaded->count == 0 && loaded->next == 4 && loaded->items == NULL);
    read = (struct vsr_store_read){2, 0, 0, client_id, 8, VSR_LOAD_REQUEST, 1};
    CHECK(mem_store_load(store, &read, graph, &loaded) == VSR_IO_OK);
    CHECK(((const struct vsr_entry *)loaded->items)->request.number == 3);
    {
        const struct vsr_change bad_changes[] = {
            {VSR_STORE_TRUNCATE, 0, 3, NULL}, {VSR_STORE_TRUNCATE, 0, 1, NULL}};
        const struct vsr_store bad = {3, bad_changes, 2, 0};
        CHECK(mem_store_submit(store, &bad) == VSR_IO_CORRUPT);
        CHECK(mem_store_readable(store) == 2);
        CHECK(mem_store_entry(store, 2, 3) != NULL);
    }
    {
        const struct vsr_change truncate = {VSR_STORE_TRUNCATE, 0, 3, NULL};
        const struct vsr_store third = {3, &truncate, 1, 0};
        CHECK(mem_store_submit(store, &third) == VSR_IO_OK);
        read.sequence = 3;
        CHECK(mem_store_load(store, &read, graph, &loaded) == VSR_IO_OK);
        CHECK(((const struct vsr_entry *)loaded->items)->request.number == 2);
        CHECK(mem_store_entry(store, 2, 3) != NULL);
        CHECK(mem_store_entry(store, 3, 3) == NULL);
    }
    {
        const struct vsr_client_record records[] = {
            {{{10, 20}, 2}, 2, {{0}, 42, 0}}, {{{10, 20}, 1}, 1, {{0}, 17, 0}}};
        const struct vsr_change clients = {VSR_STORE_CLIENTS, 2, 0, records};
        const struct vsr_store fourth = {4, &clients, 1, 0};
        CHECK(mem_store_submit(store, &fourth) == VSR_IO_OK);
        CHECK(mem_store_client(store, 4, client_id)->request.number == 2);
        CHECK(mem_store_client(store, 4, client_id)->result.code == 42);
        CHECK(mem_store_client(store, 3, client_id) == NULL);
        read =
            (struct vsr_store_read){4, 0, 0, {50, 60}, 8, VSR_LOAD_CLIENT, 1};
        CHECK(mem_store_load(store, &read, graph, &loaded) == VSR_IO_OK);
        CHECK(loaded->count == 0 && loaded->items == NULL);
    }
    mem_store_destroy(store);
    /* Last copied result remains valid after destruction of the whole store. */
    CHECK(loaded->sequence == 4);
    mem_graph_destroy(graph);
}

static void test_checkpoint_restore_suffix(void)
{
    struct mem_store *store = mem_store_create();
    const struct vsr_blob command = {0};
    const struct vsr_entry entries[] = {
        {1, 0, 0, {{10, 20}, 1}, VSR_REQUEST_COMMAND, 0, &command},
        {2, 0, 0, {{10, 20}, 2}, VSR_REQUEST_COMMAND, 0, &command},
        {3, 0, 0, {{10, 20}, 3}, VSR_REQUEST_COMMAND, 0, &command},
        {4, 0, 0, {{10, 20}, 4}, VSR_REQUEST_COMMAND, 0, &command},
        {5, 0, 0, {{10, 20}, 5}, VSR_REQUEST_COMMAND, 0, &command}};
    const struct vsr_hard_state hard = hard_state(4);
    const struct vsr_change changes[] = {{VSR_STORE_APPEND, 5, 1, entries},
                                         {VSR_STORE_HARD_STATE, 1, 0, &hard}};
    const struct vsr_store second = {2, changes, 2, 0};
    const struct vsr_checkpoint checkpoint = {{70, 80}, 2, 0, &epoch, {0}};
    const struct vsr_client_record client = {{{10, 20}, 2}, 2, {{0}, 2, 0}};
    initialize(store);
    CHECK(mem_store_submit(store, &second) == VSR_IO_OK);
    mem_store_checkpoint(store, &checkpoint, &client, 1);
    {
        const struct vsr_change restore[] = {
            {VSR_STORE_RESTORE_CHECKPOINT, 1, 0, &checkpoint},
            {VSR_STORE_HARD_STATE, 1, 0, &hard}};
        const struct vsr_store third = {3, restore, 2, 0};
        CHECK(mem_store_submit(store, &third) == VSR_IO_OK);
        CHECK(mem_store_recovered(store, 3)->log_begin == 3);
        CHECK(mem_store_recovered(store, 3)->log_end == 6);
        CHECK(mem_store_recovered(store, 3)->hard.committed == 4);
        CHECK(mem_store_entry(store, 3, 2) == NULL);
        CHECK(mem_entry_equal(mem_store_entry(store, 3, 3), &entries[2]));
        CHECK(mem_entry_equal(mem_store_entry(store, 3, 5), &entries[4]));
        CHECK(mem_store_client(store, 3, client_id)->request.number == 2);
        CHECK(mem_store_entry(store, 2, 1) != NULL);
    }
    CHECK(mem_store_sync(store, 3) == VSR_IO_OK);
    mem_store_crash(store);
    CHECK(mem_store_recovered(store, 3)->checkpoint->op == 2);
    CHECK(mem_store_entry(store, 3, 4)->op == 4);
    mem_store_destroy(store);
}

static void test_storage_rejects_regressions(void)
{
    struct mem_store *store = mem_store_create();
    struct vsr_hard_state hard = hard_state(0);
    struct vsr_change change = {VSR_STORE_HARD_STATE, 1, 0, &hard};
    struct vsr_store transaction = {2, &change, 1, 0};
    initialize(store);
    hard.view = 2;
    hard.last_normal_view = 2;
    CHECK(mem_store_submit(store, &transaction) == VSR_IO_OK);
    transaction.sequence = 3;
    hard.view = 1;
    hard.last_normal_view = 1;
    CHECK(mem_store_submit(store, &transaction) == VSR_IO_CORRUPT);
    CHECK(mem_store_recovered(store, 2)->hard.view == 2);
    {
        const struct vsr_member replacement_member = {9, VSR_MEMBER_FULL, 0};
        const struct vsr_membership replacement = {0, &replacement_member, 1,
                                                   0};
        const struct vsr_epoch replacement_epoch = {&replacement, NULL, 0,
                                                    VSR_EPOCH_STEADY, 0};
        hard = hard_state(0);
        hard.view = 2;
        hard.last_normal_view = 2;
        hard.epoch = &replacement_epoch;
        CHECK(mem_store_submit(store, &transaction) == VSR_IO_CORRUPT);
    }
    mem_store_destroy(store);
}

static void test_conflicting_client_result(void)
{
    struct mem_store *store = mem_store_create();
    const struct vsr_blob command = {0};
    const struct vsr_entry entry = {
        1, 0, 0, {{10, 20}, 1}, VSR_REQUEST_COMMAND, 0, &command};
    const struct vsr_hard_state hard = hard_state(1);
    struct vsr_client_record client = {{{10, 20}, 1}, 1, {{0}, 42, 0}};
    const struct vsr_change initial[] = {{VSR_STORE_APPEND, 1, 1, &entry},
                                         {VSR_STORE_HARD_STATE, 1, 0, &hard},
                                         {VSR_STORE_CLIENTS, 1, 0, &client}};
    const struct vsr_store second = {2, initial, 3, 0};
    const struct vsr_change clients = {VSR_STORE_CLIENTS, 1, 0, &client};
    const struct vsr_store third = {3, &clients, 1, 0};
    initialize(store);
    CHECK(mem_store_submit(store, &second) == VSR_IO_OK);
    client.result.code = 43;
    CHECK(mem_store_submit(store, &third) == VSR_IO_CORRUPT);
    CHECK(mem_store_readable(store) == 2);
    CHECK(mem_store_client(store, 2, client_id)->result.code == 42);
    client.result.code = 42;
    CHECK(mem_store_submit(store, &third) == VSR_IO_OK);
    mem_store_destroy(store);
}

static void test_witness_remote_restore(void)
{
    struct mem_store *store = mem_store_create();
    const struct vsr_blob command = {0};
    const struct vsr_entry entries[] = {
        {1, 0, 0, {{10, 20}, 1}, VSR_REQUEST_COMMAND, 0, &command},
        {2, 0, 0, {{10, 20}, 2}, VSR_REQUEST_COMMAND, 0, &command}};
    struct vsr_hard_state hard = hard_state(2);
    const struct vsr_change append[] = {{VSR_STORE_APPEND, 2, 1, entries},
                                        {VSR_STORE_HARD_STATE, 1, 0, &hard}};
    const struct vsr_store second = {2, append, 2, 0};
    const struct vsr_checkpoint remote = {{99, 1}, 1, 0, &epoch, {0}};
    const struct vsr_change changes[] = {
        {VSR_STORE_RESTORE_CHECKPOINT, 1, 0, &remote},
        {VSR_STORE_HARD_STATE, 1, 0, &hard}};
    const struct vsr_store third = {3, changes, 2, 0};
    initialize(store);
    CHECK(mem_store_submit(store, &second) == VSR_IO_OK);
    CHECK(mem_store_submit(store, &third) == VSR_IO_CORRUPT);
    hard.role = VSR_MEMBER_WITNESS;
    CHECK(mem_store_submit(store, &third) == VSR_IO_OK);
    CHECK(mem_store_recovered(store, 3)->hard.role == VSR_MEMBER_WITNESS);
    CHECK(mem_store_recovered(store, 3)->log_begin == 2);
    CHECK(mem_entry_equal(mem_store_entry(store, 3, 2), &entries[1]));
    mem_store_destroy(store);
}

int main(void)
{
    test_graph_copy();
    test_blob_segmentation();
    test_transaction_frontiers();
    test_atomicity_and_indexes();
    test_checkpoint_restore_suffix();
    test_storage_rejects_regressions();
    test_conflicting_client_result();
    test_witness_remote_restore();
    return 0;
}
