#include "config.h"

#include "lib/check.h"
#include "lib/memory.h"

static void borrowed_message_graph(void)
{
    unsigned char bytes[] = {1, 2, 3};
    unsigned char same_bytes[] = {1, 2, 3};
    struct vsr_span span = {bytes, 3};
    struct vsr_blob body = {&span, 3, 1, 0};
    struct vsr_entry entry = {1, 0,    0, {{10, 20}, 1}, VSR_REQUEST_COMMAND,
                              0, &body};
    struct vsr_prepare prepare = {{&entry, 1, 0}, 0};
    struct vsr_message message = {{1, 1},          0, 0, 1,
                                  VSR_MSG_PREPARE, 0, 1, &prepare};
    const struct vsr_op operation = {VSR_OP_SEND, 0, 1, &message, 2};
    struct mem_graph *graph = mem_graph_create();
    mem_graph_watch_sources(graph);
    const struct vsr_op copied = mem_clone_op(graph, &operation);
    CHECK(copied.data != operation.data);
    CHECK(mem_graph_unchanged(graph));
    bytes[2] = 4;
    CHECK(!mem_graph_unchanged(graph));
    bytes[2] = 3;
    CHECK(mem_graph_unchanged(graph));
    /* Identical logical bytes do not permit mutating a pinned span pointer. */
    span.data = same_bytes;
    CHECK(!mem_graph_unchanged(graph));
    span.data = bytes;
    entry.view = 1;
    CHECK(!mem_graph_unchanged(graph));
    entry.view = 0;
    prepare.committed = 1;
    CHECK(!mem_graph_unchanged(graph));
    prepare.committed = 0;
    CHECK(mem_graph_unchanged(graph));
    mem_graph_destroy(graph);
}

static void borrowed_store_arrays(void)
{
    struct vsr_member member = {1, VSR_MEMBER_FULL, 0};
    struct vsr_membership membership = {0, &member, 1, 0};
    struct vsr_epoch epoch = {&membership, NULL, 0, VSR_EPOCH_STEADY, 0};
    struct vsr_hard_state hard = {
        0, 0, 0, &epoch, VSR_HARD_NORMAL, VSR_MEMBER_FULL};
    struct vsr_change change = {VSR_STORE_HARD_STATE, 1, 0, &hard};
    struct vsr_store store = {1, &change, 1, 0};
    const struct vsr_op operation = {VSR_OP_STORE, 0, 1, &store, 0};
    struct mem_graph *graph = mem_graph_create();
    mem_graph_watch_sources(graph);
    (void)mem_clone_op(graph, &operation);
    CHECK(mem_graph_unchanged(graph));
    member.id = 2;
    CHECK(!mem_graph_unchanged(graph));
    member.id = 1;
    change.first = 1;
    CHECK(!mem_graph_unchanged(graph));
    change.first = 0;
    hard.view = 1;
    CHECK(!mem_graph_unchanged(graph));
    hard.view = 0;
    CHECK(mem_graph_unchanged(graph));
    mem_graph_destroy(graph);
}

static void frozen_input_graph(void)
{
    struct mem_graph *graph = mem_graph_create();
    struct vsr_request *request = mem_graph_alloc(graph, 1, sizeof(*request));
    struct vsr_blob *blob = mem_graph_alloc(graph, 1, sizeof(*blob));
    struct vsr_span *span = mem_graph_alloc(graph, 1, sizeof(*span));
    unsigned char *bytes = mem_graph_alloc(graph, 3, sizeof(*bytes));
    bytes[0] = 1;
    bytes[1] = 2;
    bytes[2] = 3;
    *span = (struct vsr_span){bytes, 3};
    *blob = (struct vsr_blob){span, 3, 1, 0};
    *request =
        (struct vsr_request){{{10, 20}, 1}, 0, VSR_REQUEST_COMMAND, 0, blob};
    mem_graph_freeze(graph);
    CHECK(mem_graph_unchanged(graph));
    bytes[1] = 9;
    CHECK(!mem_graph_unchanged(graph));
    bytes[1] = 2;
    span->size = 2;
    CHECK(!mem_graph_unchanged(graph));
    span->size = 3;
    request->epoch = 1;
    CHECK(!mem_graph_unchanged(graph));
    request->epoch = 0;
    CHECK(mem_graph_unchanged(graph));
    mem_graph_destroy(graph);
}

int main(void)
{
    borrowed_message_graph();
    borrowed_store_arrays();
    frozen_input_graph();
    return 0;
}
