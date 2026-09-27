#include "config.h"
#include "lib/check.h"
#include "lib/memory_cluster.h"

static bool trim_store(const struct vsr_op *operation)
{
    if (operation->type != VSR_OP_STORE)
        return false;
    const struct vsr_store *store = operation->data;
    for (uint32_t i = 0; i < store->count; i++)
        if (store->changes[i].type == VSR_STORE_TRIM)
            return true;
    return false;
}

/* Completion and the next input can precede internal metadata reconciliation.
 * Every advertised range must still belong to its exact indexed revision. */
static void offer_after_trim_completion(void)
{
    struct vsr_member member = {1, VSR_MEMBER_FULL, 0};
    struct vsr_membership group = {0, &member, 1, 0};
    struct vsr_options options = mem_options(1, &group);
    options.limits.work_per_step = 1;
    struct mem_cluster *cluster = mem_cluster_create();
    struct mem_node *node = mem_cluster_add(cluster, &options);
    while (mem_node_time(node, 0).consumed == 0)
        (void)mem_node_event(node, NULL);
    CHECK(mem_cluster_run(cluster, 100000) < 100000);
    const struct vsr_blob body = {0};
    const struct vsr_request request = {
        {{2, 3}, 1}, 0, VSR_REQUEST_COMMAND, 0, &body};
    const struct vsr_event command = {VSR_EVENT_REQUEST, VSR_IO_OK, 1, &request,
                                      1};
    CHECK(mem_node_event(node, &command).consumed == 1);
    CHECK(mem_cluster_run(cluster, 100000) < 100000);
    const struct vsr_event checkpoint = {VSR_EVENT_CHECKPOINT, VSR_IO_OK, 0,
                                         NULL, 0};
    CHECK(mem_node_event(node, &checkpoint).consumed == 1);
    uint64_t trim_sequence = 0;
    bool synchronized = false;
    for (uint32_t turn = 0; turn < 100000 && !synchronized; turn++) {
        (void)mem_node_event(node, NULL);
        for (size_t i = 0; i < mem_node_effects(node); i++) {
            const struct vsr_op *operation = mem_node_effect(node, i);
            if (trim_store(operation))
                trim_sequence =
                    ((const struct vsr_store *)operation->data)->sequence;
            bool trim_sync = trim_sequence != 0 &&
                             operation->type == VSR_OP_SYNC &&
                             operation->arg >= trim_sequence;
            if (mem_node_complete(node, i, VSR_IO_OK)) {
                synchronized = trim_sync;
                break;
            }
        }
    }
    CHECK(synchronized);
    const struct vsr_fetch fetch = {.nonce = {{99, 1}, 1},
                                    .max_bytes = options.limits.command_bytes +
                                                 options.limits.manifest_bytes,
                                    .max_entries = 1};
    const struct vsr_message message = {options.cluster,   0, 0, 99,
                                        VSR_MSG_GET_STATE, 0, 0, &fetch};
    const struct vsr_event discover = {VSR_EVENT_MESSAGE, VSR_IO_OK, 0,
                                       &message, 1};
    bool admitted = false;
    bool verified = false;
    for (uint32_t turn = 0; turn < 100000 && !verified; turn++) {
        if (!admitted) {
            admitted = mem_node_event(node, &discover).consumed == 1;
            if (!admitted)
                (void)mem_node_event(node, NULL);
        } else
            (void)mem_node_event(node, NULL);
        for (size_t i = 0; i < mem_node_effects(node); i++) {
            const struct vsr_op *operation = mem_node_effect(node, i);
            if (operation->type == VSR_OP_SEND) {
                const struct vsr_message *reply = operation->data;
                if (reply->type == VSR_MSG_NEW_STATE) {
                    const struct vsr_state_chunk *chunk = reply->body;
                    const struct vsr_recovered *stored = mem_store_recovered(
                        mem_node_store(node), chunk->state.revision.sequence);
                    CHECK(stored != NULL);
                    CHECK(stored->log_begin == chunk->state.log_begin);
                    CHECK(stored->log_end == chunk->state.log_end);
                    verified = true;
                    break;
                }
            }
            if (mem_node_complete(node, i, VSR_IO_OK))
                break;
        }
    }
    CHECK(admitted && verified);
    mem_cluster_destroy(cluster);
}

int main(void)
{
    offer_after_trim_completion();
    return 0;
}
